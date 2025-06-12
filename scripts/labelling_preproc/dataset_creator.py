"""Segments.ai dataset creator module."""

import argparse
import json
import logging
from pathlib import Path


from labelling_preproc.add_segmentsai_sample import SegmentsSampleCreator
from labelling_preproc.common.utils import file_exists, get_env_var
from labelling_preproc.generate_ego_trajectory import EgoTrajectoryGenerator
from labelling_preproc.upload_data import AssetUploader

from segments import SegmentsClient
from segments.typing import Category

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
            debug_mode: Enable debug logs.
        """
        self.logger = logger
        # Unique organisation name where Segments.ai datasets will be created
        self.ORGANISATION_NAME = 'UniversityofEdinburgh'

        file_exists(dataset_attributes_file)
        with open(dataset_attributes_file, encoding='utf-8') as f:
            self.dataset_attributes = json.load(f)

        api_key = get_env_var('SEGMENTS_API_KEY')
        self.client = SegmentsClient(api_key)

        self.trajectory_generator = EgoTrajectoryGenerator()
        self.asset_uploader = AssetUploader(s3_organisation)
        self.segments_sample_creator = SegmentsSampleCreator()

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

    def add_dataset(
        self, dataset_name: Path, recording_directory: Path
    ) -> str:
        """
        Add a new dataset to Segments.ai.

        Args:
            dataset_name: Name of the dataset to be created
            recording_directory: Directory containing the ROS bag
                                        recording
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

        dataset = self.client.add_dataset(
            name=dataset_name,
            task_type=task_type,
            task_attributes=self.dataset_attributes,
            category=Category.STREET_SCENERY,
            readme=readme_str,
            enable_3d_cuboid_rotation=True,
            organization=self.ORGANISATION_NAME,
        )

        return dataset.full_name

    def create(self, export_directory: Path, recording_directory: Path) -> str:
        """
        Create a Segments.ai dataset from ROS bag data.

        Args:
            recording_directory: Directory containing the recording
                                 ROS bag files
            export_directory: Directory containing the recording expoted data
        """
        # Use rosbag export directory name as dataset name
        dataset_name = export_directory.name

        # Create a Segments.ai dataset
        dataset_full_name = self.add_dataset(dataset_name, recording_directory)

        self.logger.debug(
            f'[DatasetCreator] New dataset added: {dataset_full_name}'
        )

        # Iterate through subdirectories in the export directory
        # and create a Segments.ai sample for each
        for export_sub_directory in export_directory.iterdir():
            if export_sub_directory.is_dir():
                self.logger.debug(
                    '[DatasetCreator] Processing directory: '
                    f'{export_sub_directory}'
                )

                # Generate ego trajectory from export's ROS bag
                rosbag_file_name = self.get_rosbag_file_name(
                    export_sub_directory
                )

                rosbag_file = recording_directory / rosbag_file_name
                self.trajectory_generator.run_mola_lidar_odometry(
                    str(rosbag_file), str(export_sub_directory)
                )

                # Upload the directory to the cloud
                self.asset_uploader.run(export_sub_directory.resolve())

                # Create a multi-sensor sequence in Segments.ai
                sequence_name = export_sub_directory.name
                self.segments_sample_creator.add(
                    dataset_full_name, sequence_name, export_sub_directory
                )

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
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    )
    logger.addHandler(handler)

    dataset_creator = DatasetCreator(
        dataset_attributes_file=dataset_attributes_file,
        logger=logger,
        debug_mode=False,
    )

    dataset_creator.create(export_directory, recording_directory)


if __name__ == '__main__':
    main()
