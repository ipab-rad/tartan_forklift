"""Segments.ai dataset creator module."""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import List, Optional

import colorlog

from labelling_preproc.add_segmentsai_sample import SegmentsSampleCreator
from labelling_preproc.common.response import PreprocessingError
from labelling_preproc.common.segments_client_wrapper import (
    SegmentsClientWrapper,
)
from labelling_preproc.common.utils import file_exists, get_env_var
from labelling_preproc.generate_ego_trajectory import EgoTrajectoryGenerator
from labelling_preproc.upload_data import AssetUploader


import yaml


class DatasetCreator:
    """
    Provide an interface to create a Segments.ai dataset.

    This class uses the Segments.ai Python client to create a new dataset
    based on:
        - A directory containing the exported data from a ROS bag recording
        - A directory containing the ROS bag recording
        - A JSON file with the dataset attributes
    The name of the exported data directory is used as the dataset name to
    keep the association with the recording.
    """

    def __init__(
        self,
        dataset_attributes_file: Path,
        s3_organisation,
        logger: logging.Logger,
    ):
        """
        Initialise the DatasetCreator.

        Args:
            dataset_attributes_file: Absolute path to the dataset attributes
                                     JSON file.
            s3_organisation: Name of the AWS S3 organisation to target
            logger: Logger object for level-based logging
        """
        self.logger = logger
        # Unique organisation name where Segments.ai datasets will be created
        self.ORGANISATION_NAME = 'UniversityofEdinburgh'

        file_exists(dataset_attributes_file)
        with open(dataset_attributes_file, encoding='utf-8') as f:
            self.dataset_attributes = json.load(f)

        api_key = get_env_var('SEGMENTS_API_KEY')
        self.client = SegmentsClientWrapper(api_key)

        self.trajectory_generator = EgoTrajectoryGenerator()
        self.asset_uploader = AssetUploader(s3_organisation)
        self.segments_sample_creator = SegmentsSampleCreator(self.client)

        self.RETRY_INTERVAL_SEC = 10

    def get_rosbag_file_name(self, sequence_export: Path) -> str:
        """
        Obtain the ROS bag file name from the export directory metadata.

        Args:
            sequence_export: Path to the directory containing the exported data
        """
        metadata_file = sequence_export / 'export_metadata.yaml'

        with open(metadata_file) as file:
            metadata = yaml.safe_load(file)
        rosbags_list = metadata.get('rosbags')

        # Get the first and only rosbag file name from the list
        return rosbags_list[0]

    def sort_sub_directories(self, export_directory: Path) -> List[Path]:
        """
        Return a list of subdirectories sorted by their numeric suffix.

        Assumes each subdirectory name ends with '_N' where N is an integer.

        Args:
            export_directory (Path): The parent directory containing
                                     subdirectories to sort.
        Returns:
            List[Path]: Sorted list of subdirectory paths.
        """

        def extract_suffix_num(path: Path) -> int:
            """
            Extract the numeric suffix from a path's name.

            Assuming the format '<path_name>_N'. If the format is invalid,
            return a high number to sort it at the end.
            """
            TEN_THOUSAND = 10000
            try:
                return int(path.name.rsplit('_', 1)[-1])
            except (IndexError, ValueError):
                return TEN_THOUSAND

        subdirs = []
        for item in export_directory.iterdir():
            if item.is_dir():
                subdirs.append(item)

        return sorted(subdirs, key=extract_suffix_num)

    def add_dataset(
        self, dataset_name: Path, recording_directory: Path
    ) -> Optional[str]:
        """
        Add a new dataset to Segments.ai.

        If the creation fails and the error is recoverable,
        the function will keep trying every self.RETRY_INTERVAL_SEC seconds.
        If an unrecoverable error happens, the dataset of this ROSbag recording
        will be skipped and logged.

        Args:
            dataset_name: Name of the dataset to be created
            recording_directory: Directory containing the ROS bag
                                        recording
        Returns:
            A string with the dataset name if the creation was
             successful, none otherwise.
        """
        recording_name = recording_directory.name
        task_type = 'multisensor-sequence'

        readme_str = (
            f'# {dataset_name}\n'
            f'Created from `{recording_name}` rosbag recording.\n\n'
            'Each sequence defined in this dataset corresponds to a '
            'ROS bag of the recording:\n\n'
            f'\t`sequence_<N> -> {recording_name}_<N>.mcap`\n'
        )

        while True:
            response = self.client.add_dataset(
                dataset_name,
                task_type,
                self.dataset_attributes,
                readme_str,
                self.ORGANISATION_NAME,
            )

            if response.ok:
                # Obtain dataset full name from the response's metadata
                return response.metadata.full_name
            elif response.error in {
                PreprocessingError.SegmentsAPILimitError,
                PreprocessingError.SegmentsNetworkError,
                PreprocessingError.SegmentsTimeoutError,
            }:
                # We can recover from these errors, so we wait for a few
                # seconds before continuing with the loop
                self.logger.warning(
                    f'[DatasetCreator] Error {response.error.value} happened '
                    f'when creating a "{dataset_name}" dataset'
                    f'\nRetrying in {self.RETRY_INTERVAL_SEC} seconds...'
                    f'\nMore details:\n{response.error_message}'
                )
                time.sleep(self.RETRY_INTERVAL_SEC)
            else:
                # Other errors are not recoverable for this new dataset
                # so we log the error and skip this ROS bag recording
                self.logger.warning(
                    f'[DatasetCreator] An unrecoverable error '
                    f'{response.error.value} happened '
                    f'when creating "{dataset_name}" dataset, '
                    f'skipping {recording_name} recording.\n'
                    f'More details: {response.error_message}'
                )
                return None

    def create_sample(
        self, dataset_full_name: str, export_sub_directory: Path
    ) -> bool:
        """
        Create a Segments.ai sample for the given export sub directory.

        If the creation fails and the error is recoverable,
        the function will keep trying every self.RETRY_INTERVAL_SEC seconds.
        If an unrecoverable error happens, the sample of this export
        sub-directory will be skipped and logged.

        Args:
            dataset_full_name: Full name of the Segments.ai dataset
            export_sub_directory: Path to the directory containing the exported
                                  data for the sample
        Returns:
            bool: True if the sample was created successfully, False otherwise.
        """
        while True:
            sequence_name = export_sub_directory.name
            response = self.segments_sample_creator.add(
                dataset_full_name, sequence_name, export_sub_directory
            )
            if response.ok:
                self.logger.debug(
                    f'[DatasetCreator] Sample "{sequence_name}" added'
                )
                return True
            elif response.error in {
                PreprocessingError.SegmentsAPILimitError,
                PreprocessingError.SegmentsNetworkError,
                PreprocessingError.SegmentsTimeoutError,
            }:
                # We can recover from these errors, so we wait for a few
                # seconds before continuing with the loop
                self.logger.warning(
                    f'[DatasetCreator] Error {response.error.value} happened '
                    f'when adding sample "{sequence_name}"'
                    f'\nRetrying in {self.RETRY_INTERVAL_SEC} seconds...'
                    f'\nMore details:\n{response.error_message}'
                )
                time.sleep(self.RETRY_INTERVAL_SEC)
            else:
                # Other errors are not recoverable for this sample
                # so we log the error and skip this sample
                self.logger.warning(
                    '[DatasetCreator] An unrecoverable error '
                    f'{response.error.value} happened when '
                    f'adding sample "{sequence_name}", '
                    f'skipping it.\n'
                    f'More details:\n{response.error_message}'
                )
                return False

    def create(self, export_directory: Path, recording_directory: Path) -> str:
        """
        Create a Segments.ai dataset from ROS bag data.

        Args:
            recording_directory: Directory containing the recording
                                 ROS bag files
            export_directory: Directory containing the recording exported data
        """
        # Use rosbag export directory name as dataset name
        dataset_name = export_directory.name

        # Create a Segments.ai dataset
        dataset_full_name = self.add_dataset(dataset_name, recording_directory)

        if not dataset_full_name:
            # If we couldn't create a dataset, it may be due to an
            # unrecoverable error, thus, early return
            return None

        self.logger.debug(
            f'[DatasetCreator] New dataset added: {dataset_full_name}'
        )

        export_sub_directory_list = self.sort_sub_directories(export_directory)

        # Iterate through sub-directories in the export directory
        # and create a Segments.ai sample for each
        for export_sub_directory in export_sub_directory_list:
            self.logger.debug(
                '[DatasetCreator] Processing directory: '
                f'{export_sub_directory}'
            )

            # Generate ego trajectory from export directory's ROS bag
            rosbag_file_name = self.get_rosbag_file_name(export_sub_directory)

            rosbag_file = recording_directory / rosbag_file_name
            self.trajectory_generator.run_mola_lidar_odometry(
                str(rosbag_file), str(export_sub_directory)
            )

            # Upload the directory to the cloud
            self.asset_uploader.run(export_sub_directory.resolve())

            # Create Segments.ai sample
            self.create_sample(dataset_full_name, export_sub_directory)

        return dataset_full_name


def main():
    """
    Entry point for the script.

    Parse command line arguments and run DatasetCreator.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--export_directory',
        type=str,
        required=True,
        help='Absolute path to the directory where the rosbag recording \
            was exported to.',
    )

    parser.add_argument(
        '--recording_directory',
        type=str,
        required=True,
        help='Absolute path to the directory containing the rosbag recording.',
    )

    parser.add_argument(
        '--dataset_attributes_file',
        type=str,
        required=True,
        help='Absolute path to the dataset attributes file.',
    )

    export_directory = Path(parser.parse_args().export_directory)
    recording_directory = Path(parser.parse_args().recording_directory)
    dataset_attributes_file = Path(parser.parse_args().dataset_attributes_file)

    # Define basic logger
    logger = logging.getLogger('DatasetCreator')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(asctime)s %(log_color)s%(levelname)s%(reset)s: %(message)s'
        )
    )
    logger.addHandler(handler)

    dataset_creator = DatasetCreator(
        dataset_attributes_file=dataset_attributes_file,
        s3_organisation='eidf',
        logger=logger,
    )

    dataset_creator.create(export_directory, recording_directory)


if __name__ == '__main__':
    main()
