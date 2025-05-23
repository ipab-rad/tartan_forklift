#!/usr/bin/python3
"""Module to upload data to EIDF or Segments.ai S3."""

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path

from labelling_preproc.common.s3_client import EIDFfS3Client, SegmentS3Client
from labelling_preproc.common.utils import (
    directory_exists,
    file_exists,
    get_env_var,
    metadata_is_valid,
    show_progress_bar,
)

import yaml


class AssetUploader:
    """
    Class to upload data to EIDF or Segments.ai S3.

    This class uploads files found in a directory to S3 based on the
    `export_metadata.yaml` file describing assets information.
    """

    def __init__(
        self, dataset_name: str, data_directory: Path, s3_client_name='eidf'
    ) -> None:
        """
        Initialise the uploader with dataset name, directory, and S3 client.

        Args:
            dataset_name: The name of the dataset (may include org prefix).
            data_directory: Directory containing exported data and metadata.
            s3_client_name: Name of the S3 organisation to
                            use ('eidf' or 'segmentsai').
        """
        self.s3 = self.get_s3_client(s3_client_name)
        directory_exists(data_directory)
        self.dataset_name = dataset_name.split('/')[-1]
        self.local_data_directory = data_directory

        self.metadata_file = self.local_data_directory / 'export_metadata.yaml'
        file_exists(self.metadata_file)

        with open(self.metadata_file) as file:
            self.export_metadata_yaml = yaml.safe_load(file)

        metadata_is_valid(self.export_metadata_yaml)

    def get_s3_client(self, s3_client_name: str):
        """
        Create an S3 client based on the selected organisation.

        Args:
            s3_client_name: The organisation name ('segmentsai' or 'eidf').

        Returns:
            An instance of a subclass of S3Client.

        Raises:
            ValueError: If the client name is not recognised.
        """
        if s3_client_name.lower() == 'eidf':
            project_name = get_env_var('EIDF_PROJECT_NAME')
            bucket_name = get_env_var('AWS_BUCKET_NAME')
            endpoint_url = get_env_var('AWS_ENDPOINT_URL')
            print('S3 client --> EIDF')
            return EIDFfS3Client(project_name, bucket_name, endpoint_url)

        elif s3_client_name.lower() == 'segmentsai':
            api_key = get_env_var('SEGMENTS_API_KEY')
            print('S3 client --> SegmentsAI')
            return SegmentS3Client(api_key)

        else:
            raise ValueError(
                f'Unknown S3 client name: {s3_client_name} '
                'Valid names: ["eidf", "segmentsai"]'
            )

    def upload_file(self, local_file_path: Path, label: str):
        """
        Upload a single file to S3.

        Args:
            local_file_path: Path to the file to be uploaded.
            label: Label to use as the S3 key.

        Returns:
            A TartanAsset instance if successful, otherwise None.
        """
        if not local_file_path.is_file():
            print(f'WARN: File not found: {local_file_path}', file=sys.stderr)
            return None

        with local_file_path.open('rb') as f:
            asset = self.s3.upload_file(f, label)
            return asset

    def get_s3_key_from_path(self, file_path: Path, dataset_name: str) -> str:
        """
        Convert a local file path into an S3 key rooted at the dataset name.

        An S3 key is a path-like string used to identify files in Amazon S3.
        This function ensures the dataset name is the top-level directory in
        the resulting key.

        If the file path already includes a folder that starts with the dataset
        name, the key will start from that point. Otherwise, the dataset name
        will be prepended to the full path.

        Args:
            file_path: The path to the file.
            dataset_name: The dataset name to use as the key root.

        Returns:
            A forward-slash S3 key string starting at the dataset name.
        """
        for index, part in enumerate(file_path.parts):
            if part.startswith(dataset_name):
                return Path(*file_path.parts[index:]).as_posix()

        return f'{dataset_name}/{file_path.as_posix()}'

    def run(self):
        """
        Start the uploading process based on metadata.

        Iterates through all time-synchronised groups, uploads associated
        lidar and camera assets, and records the resulting S3 URLs and UUIDs
        into a new metadata file.
        """
        urls_list = {'assets_ids': {}}

        file_dict = {
            'local_file': '',
            'label': '',
            'uuid': '',
            's3_url': '',
        }

        upload_metadata_file = (
            self.local_data_directory / 'upload_metadata.json'
        )

        if upload_metadata_file.exists():
            raise ValueError(
                ' "upload_metadata.json" already exists. '
                'This script assumes the data has not been uploaded yet. '
                'Delete the file if you want to proceed.'
            )

        total_goups = len(
            self.export_metadata_yaml.get('time_sync_groups', [])
        )
        progress = 1

        for sync_group in self.export_metadata_yaml.get(
            'time_sync_groups', []
        ):
            show_progress_bar('Uploading', progress, total_goups)

            lidar_dict = deepcopy(file_dict)
            lidar_dict['local_file'] = sync_group['lidar']['file']

            lidar_file = Path(sync_group['lidar']['file'])
            lidar_file_path = self.local_data_directory / lidar_file

            lidar_s3_key = self.get_s3_key_from_path(
                lidar_file_path, self.dataset_name
            )
            lidar_dict['label'] = lidar_s3_key

            lidar_asset = self.upload_file(lidar_file_path, lidar_s3_key)

            if lidar_asset is not None:
                lidar_dict['uuid'] = lidar_asset.uuid
                lidar_dict['s3_url'] = lidar_asset.url
            else:
                lidar_dict['uuid'] = 'UPLOAD_FAIL'
                lidar_dict['s3_url'] = 'UPLOAD_FAIL'

            urls_list['assets_ids'][
                str(sync_group['lidar']['global_id'])
            ] = lidar_dict

            for cam in sync_group.get('cameras', []):
                cam_dict = deepcopy(file_dict)
                cam_dict['local_file'] = cam['file']

                cam_file_path = self.local_data_directory / cam['file']

                cam_s3_key = self.get_s3_key_from_path(
                    cam_file_path, self.dataset_name
                )
                cam_dict['label'] = cam_s3_key

                cam_asset = self.upload_file(cam_file_path, cam_s3_key)

                if cam_asset is not None:
                    cam_dict['uuid'] = cam_asset.uuid
                    cam_dict['s3_url'] = cam_asset.url
                else:
                    cam_dict['uuid'] = 'UPLOAD_FAIL'
                    cam_dict['s3_url'] = 'UPLOAD_FAIL'

                urls_list['assets_ids'][str(cam['global_id'])] = cam_dict

            progress += 1

        with upload_metadata_file.open('w') as outfile:
            json.dump(urls_list, outfile, indent=4)

        print(f'\n\n🚀 Uploading metadata saved as: {upload_metadata_file}\n')


def main():
    """
    Entry point for the script.

    Parses command-line arguments and runs the AssetUploader.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'dataset_name',
        type=str,
        help='Dataset name including Segments.ai organisation account name',
    )
    parser.add_argument(
        'data_directory',
        type=str,
        help='The directory containing the exported data to upload',
    )
    parser.add_argument(
        's3_org',
        type=str,
        default='eidf',
        choices=['eidf', 'segmentsai'],
        nargs='?',
        help=(
            'Whether to upload to EIDF or Segments.ai AWS S3 '
            '(Optional, default: eidf)'
        ),
    )

    args = parser.parse_args()
    dataset_name = args.dataset_name
    data_directory = Path(args.data_directory)
    s3_org = args.s3_org

    try:
        uploader = AssetUploader(dataset_name, data_directory, s3_org)
        uploader.run()
    except Exception as e:  # noqa: BLE001
        print(f'{e}')


if __name__ == '__main__':
    main()
