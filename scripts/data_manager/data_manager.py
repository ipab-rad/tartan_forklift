"""Module to detect new ROS bag data and manage its processing."""

import argparse
import logging
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import colorlog

from data_manager.new_rosbag_watchdog import NewRosbagWatchdog

from watchdog.observers import Observer


class DataManager:
    """
    Monitor a directory and detect new ROS bag recordings.

    This class uses the Watchdog library to observe a target directory and
    to detect when a new ROS bag recording is available.
    A recording is defined by a `metadata.yaml` file and one or more ROS bags.

    It enters a polling loop and checks for new recordings using the
    `NewRosbagWatchdog`. When a new recording is detected, the path is
    extracted for further processing.

    The path to monitor is passed via command-line argument.
    """

    def __init__(
        self,
        output_directory: str,
        exporter_config_file: str,
        debug_mode: bool,
    ) -> None:
        """Initialise the DataManager and configure logging."""
        self.logger = self.setup_logging(debug_mode=debug_mode)
        # Polling interval in seconds to check for new recordings.
        self.POLLING_INTERVAL_SEC = 1
        self.output_directory = output_directory
        self.exporter_config_file = exporter_config_file

    def setup_logging(self, debug_mode: bool) -> logging.Logger:
        """
        Configure logging with colour support and rotating file log.

        Args:
            debug_mode: Whether to show debug logs in the console.

        Returns:
            A configured logger instance.
        """
        timestamp = datetime.now().strftime('%Y_%m_%d-%H_%M_%S')
        log_filename = f'{timestamp}_data_manager.log'

        logger = logging.getLogger('data_manager')
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)

        color_formatter = colorlog.ColoredFormatter(
            '%(asctime)s %(log_color)s%(levelname)s%(reset)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        console_handler.setFormatter(color_formatter)

        file_formatter = logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        file_handler.setFormatter(file_formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def export_rosbag_recording(
        self, rosbag_directory: Path
    ) -> Optional[Path]:
        """
        Export data from a ROS bag recording using the `bag_exporter` ROS node.

        Call the `ros2_bag_exporter bag_exporter` node with the given
        rosbag directory, `self.output directory`, and
        `self.exporter_config_file`.Parse the node stdout + stderr to
        locate and return the path where data was exported, or
        None if extraction fails.

        Args:
            rosbag_directory: Path to the recording

        Returns:
            The directory where the data was exported to, or None if
            extraction failed.
        """
        bag_exporter_cmd = [
            'ros2',
            'run',
            'ros2_bag_exporter',
            'bag_exporter',
            '--ros-args',
            '-p',
            f'rosbags_directory:={rosbag_directory}',
            '-p',
            f'output_directory:={self.output_directory}',
            '-p',
            f'config_file:={self.exporter_config_file}',
        ]

        # Avoid ANSI color codes in ROS logs
        env = os.environ.copy()
        env['RCUTILS_COLORIZED_OUTPUT'] = '0'

        try:
            # Run command and capture all output
            output = subprocess.run(
                bag_exporter_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=True,
                env=env,
            )
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f'❌ Failed to extract {rosbag_directory}'
                f'Error output: {e.stdout}'
            )
            return None

        # Find the export directory line
        for line in output.stdout.splitlines():
            if 'Data exported in:' in line:
                # Extract the export path from the log line
                path_str = line.rsplit(':', 1)[-1].strip()
                return Path(path_str)

        return None

    def run(self, rosbags_directory: str) -> None:
        """
        Start monitoring the given directory for new recordings.

        This sets up the watchdog observer and polls for new ROS bag
        recordings. When a new recording is detected, it extracts the path
        for further processing.

        Args:
            rosbags_directory: Directory to monitor for new data.
        """
        self.logger.info(
            f'Starting DataManager to monitor directory: {rosbags_directory}'
        )

        new_rosbag_watchdog = NewRosbagWatchdog(self.logger)

        observer = Observer()
        observer.schedule(
            new_rosbag_watchdog, rosbags_directory, recursive=True
        )

        observer.start()

        try:
            while True:
                new_recording_path = (
                    new_rosbag_watchdog.are_there_more_recordings()
                )
                if new_recording_path:
                    self.logger.info(
                        f'New recording found: {new_recording_path},'
                        f' extracting data...'
                    )

                    # Export data from the new recording
                    export_directory = self.export_rosbag_recording(
                        new_recording_path
                    )

                    self.logger.info(
                        f'Extraction completed --> {export_directory}'
                    )

                time.sleep(self.POLLING_INTERVAL_SEC)
        except KeyboardInterrupt:
            self.logger.info('DataManager interrupted by user.')
            observer.stop()
        observer.join()


def main() -> None:
    """
    Entry point for the script.

    Parses command-line arguments and starts the DataManager.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--rosbags_directory',
        type=str,
        help='Path to the directory to monitor for new rosbags.',
    )

    parser.add_argument(
        '--output_directory',
        type=str,
        help='Parent directory to save exported data.',
    )

    parser.add_argument(
        '--export_config_file',
        type=str,
        help='Configuration file for the bag exporter',
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging.',
    )

    args = parser.parse_args()
    rosbags_directory = args.rosbags_directory
    output_directory = args.output_directory
    exporter_config_file = args.export_config_file
    debug_mode = args.debug

    data_manager = DataManager(
        output_directory, exporter_config_file, debug_mode
    )

    data_manager.run(rosbags_directory)


if __name__ == '__main__':
    main()
