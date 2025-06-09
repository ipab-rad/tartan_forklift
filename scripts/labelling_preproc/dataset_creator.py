"""Segments.ai dataset creator module."""

import argparse
import json
import logging
from pathlib import Path

from labelling_preproc.common.utils import file_exists, get_env_var

from segments import SegmentsClient
from segments.typing import Category


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

    def __init__(self, dataset_attributes_file: Path, logger: logging.Logger):
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

    def create(
        self, export_directory: Path, recording_directory: Path
    ) -> None:
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

        self.logger.info(f'Created dataset: {dataset_full_name}')

        # Iterate through subdirectories in the export directory
        for data_directory in export_directory.iterdir():
            if data_directory.is_dir():
                # TODO: Add a Segments.ai sample for each directory (#46)
                pass


def main():
    """
    Entry point for the script.

    Parse command line arguments and run DatasetCreator.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--export_directory',
        type=str,
        help='Absolute path to the directory where the rosbag recording \
            was exported to.',
    )

    parser.add_argument(
        '--recording_directory',
        type=str,
        help='Absolute path to the directory containing the rosbag recording.',
    )

    parser.add_argument(
        '--dataset_attributes_file',
        type=str,
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
