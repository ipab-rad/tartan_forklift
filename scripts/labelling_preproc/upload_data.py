#!/usr/bin/python3

import argparse
import sys
import json
import yaml
from pathlib import Path
from copy import deepcopy

from labelling_preproc.common.s3_client import SegmentS3Client, EIDFfS3Client

from labelling_preproc.common.utils import (
    directory_exists,
    file_exists,
    metadata_is_valid,
    get_env_var,
    show_progress_bar,
)


class AssetUploader:

    def __init__(
        self, dataset_name: str, data_directory: Path, s3_client_name='eidf'
    ) -> None:
        # Initialise s3 client
        self.s3 = self.get_s3_client(s3_client_name)

        directory_exists(data_directory)

        # Skip first part of dataset string to remove the non-informative organisation name
        self.dataset_name = dataset_name.split('/')[-1]

        self.local_data_directory = data_directory

        # Load export_metadata.yaml
        self.metadata_file = self.local_data_directory / 'export_metadata.yaml'

        file_exists(self.metadata_file)

        with open(self.metadata_file) as file:
            self.export_metadata_yaml = yaml.safe_load(file)

        # Ensure metadata is valid
        metadata_is_valid(self.export_metadata_yaml)

    def get_s3_client(self, s3_client_name: str):
        """
        Create an S3 client based on the provided organisation name

        :param s3_client_name: The organisation name ("segmentsai" or "eidf").
        :return: An instance of an S3Client.
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
                'Valid names: [\'eidf\',\'segmentsai\']'
            )

    # Function to upload files
    def upload_file(self, local_file_path: Path, label: str):
        """
        Uploads file to Segments.ai and returns asset info.

        :param: local_file_path: File to be uploaded
        :param: label: Desired label for the file

        """
        if not local_file_path.is_file():
            print(f'WARN: File not found: {local_file_path}', file=sys.stderr)
            return None

        with local_file_path.open('rb') as f:
            asset = self.s3.upload_file(f, label)
            return asset

    def get_s3_key_from_path(self, file_path: Path, dataset_name: str) -> str:
        """
        Generate an S3 key from a local file path, ensuring it starts with the dataset name.

        If the file path already contains a directory that starts with the dataset name,
        the S3 key will start from that point onward. Otherwise, the dataset name will
        be prepended to the entire path.

        Args:
            file_path (Path): The full local path to the file.
            dataset_name (str): The dataset name to use as the S3 key prefix.

        Returns:
            str: The S3-compatible key (with forward slashes), rooted at the dataset name.
        """
        # Look for the first path segment that starts with the dataset name
        for index, part in enumerate(file_path.parts):
            if part.startswith(dataset_name):
                # Build the S3 key from the matched part onward
                return Path(*file_path.parts[index:]).as_posix()

        # If not found, prepend the dataset name to the full path
        return f"{dataset_name}/{file_path.as_posix()}"

    def run(self):
        """
        Start the uploading process based on a metadata file.
        """
        urls_list = {'assets_ids': {}}

        # Default file metadata structure
        file_dict = {
            'local_file': '',
            'label': '',
            'uuid': '',
            's3_url': '',
        }

        # Ensure we don't easily overwrite a previous generated file
        upload_metadata_file = (
            self.local_data_directory / 'upload_metadata.json'
        )

        if upload_metadata_file.exists():
            raise ValueError(
                ' \'upload_metadata.json\' already exists. '
                'This script assumes the data has not been uploaded yet. '
                'Delete the file if you want to proceed.'
            )

        rosbag_name = Path(self.export_metadata_yaml['rosbags'][0]).stem
        total_goups = len(
            self.export_metadata_yaml.get('time_sync_groups', [])
        )
        progress = 1

        # Iterate through all time-sync groups
        for sync_group in self.export_metadata_yaml.get(
            'time_sync_groups', []
        ):

            show_progress_bar('Uploading', progress, total_goups)

            # Process Lidar files meta
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

            # Process Camera files meta
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

                urls_list['assets_ids'][str(cam["global_id"])] = cam_dict

            progress += 1

        # Save metadata as JSON
        with upload_metadata_file.open('w') as outfile:
            json.dump(urls_list, outfile, indent=4)

        print(
            f'\n\n🚀 Uploading metadata saved as: {upload_metadata_file}\n'
        )


def main():
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
        help='Whether to upload to EIDF or Segments.ai AWS S3 (Optional, default: eidf)',
    )

    args = parser.parse_args()
    dataset_name = args.dataset_name
    data_directory = Path(args.data_directory)
    s3_org = args.s3_org

    try:
        uploader = AssetUploader(dataset_name, data_directory, s3_org)
        uploader.run()

    except Exception as e:
        print(f"{e}")


if __name__ == "__main__":
    main()
