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

    def __init__(self, s3_client_name='eidf') -> None:
        """
        Initialise the uploader.

        Args:
            s3_client_name: Name of the S3 organisation to
                            use ('eidf' or 'segmentsai').
        """
        self.s3 = self.get_s3_client(s3_client_name)

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

    def get_s3_key_from_path(
        self, export_sub_directory: Path, file_path: Path
    ) -> str:
        """
        Make an S3 key by removing the export root’s parent from the file path.

        Assumes output from the `tartan_rosbag_exporter bag_export` ROS 2 node,
        which produces a directory structure like:
            <base_directory>/
                └── <export_root>/
                        └── <export_sub_directory>/
                                └── <file>

        The S3 key is computed relative to <base_directory>, so the entire
        file path under <export_root> is preserved.

        Args:
            export_sub_directory: Absolute path to the export sub-directory.
            file_path: Absolute path to the file to be uploaded.
        """
        export_root_parent = export_sub_directory.parent.parent
        return str(file_path.relative_to(export_root_parent))

    def load_export_metadata(self, export_sub_directory: Path) -> dict:
        """
        Load the export metadata from the specified directory.

        Args:
            export_sub_directory: The absolute path of the
                                  subdirectory containing exported data.
        """
        directory_exists(export_sub_directory)
        metadata_file = export_sub_directory / 'export_metadata.yaml'
        file_exists(metadata_file)
        with open(metadata_file) as file:
            export_metadata = yaml.safe_load(file)

        metadata_is_valid(export_metadata)

        return export_metadata

    def run(self, export_sub_directory: Path):
        """
        Start the uploading process based on metadata.

        Iterates through all time-synchronised groups, uploads associated
        lidar and camera assets, and records the resulting S3 URLs and UUIDs
        into a new metadata file.

        Args:
            export_sub_directory: The absolute path of the
                                  sub-directory containing exported data.
        """
        export_metadata = self.load_export_metadata(export_sub_directory)

        urls_list = {'assets_ids': {}}

        file_dict = {
            'local_file': '',
            'label': '',
            'uuid': '',
            's3_url': '',
        }

        upload_metadata_file = export_sub_directory / 'upload_metadata.json'

        if upload_metadata_file.exists():
            raise ValueError(
                ' "upload_metadata.json" already exists. '
                'This script assumes the data has not been uploaded yet. '
                'Delete the file if you want to proceed.'
            )

        total_groups = len(export_metadata.get('time_sync_groups', []))
        progress = 1

        for sync_group in export_metadata.get('time_sync_groups', []):
            show_progress_bar('Uploading', progress, total_groups)

            lidar_dict = deepcopy(file_dict)
            lidar_dict['local_file'] = sync_group['lidar']['file']

            lidar_file = Path(sync_group['lidar']['file'])
            lidar_file_path = export_sub_directory / lidar_file

            lidar_s3_key = self.get_s3_key_from_path(
                export_sub_directory, lidar_file_path
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

                cam_file_path = export_sub_directory / cam['file']

                cam_s3_key = self.get_s3_key_from_path(
                    export_sub_directory, cam_file_path
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
        'export_sub_directory',
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
    export_sub_directory = Path(args.export_sub_directory).resolve()
    s3_org = args.s3_org

    try:
        uploader = AssetUploader(s3_org)
        uploader.run(export_sub_directory)
    except Exception as e:  # noqa: BLE001
        print(f'{e}')


if __name__ == '__main__':
    main()
