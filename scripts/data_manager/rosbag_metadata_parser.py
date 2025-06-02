"""Parse metadata from ROS bag recordings."""

from pathlib import Path

import yaml


class RosbagMetadataParser:
    """
    Parse the metadata file from a ROS bag recording.

    This class reads a `metadata.yaml` file located in a ROS bag recording
    directory and extracts the list of expected `.mcap` files.
    """

    def get_expected_rosbag_files(
        self, metadata_file_path: Path
    ) -> list[Path]:
        """
        Get expected ROS bag files from the recording metadata.

        Args:
            metadata_file_path: Path to the metadata file.

        Returns:
            List of expected ROS bag file paths.
        """
        with open(metadata_file_path) as file:
            metadata = yaml.safe_load(file)
            recording_info = metadata.get('rosbag2_bagfile_information')
            rosbag_file_list = recording_info.get('relative_file_paths')
            parent_directory = metadata_file_path.parent
            rosbag_files = []
            for rosbag_file in rosbag_file_list:
                rosbag_file_path = parent_directory / rosbag_file
                rosbag_files.append(rosbag_file_path)

            return rosbag_files
