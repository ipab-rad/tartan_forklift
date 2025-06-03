"""Module to detect new ROS bag data and manage its processing."""

import argparse
import logging
import time
from datetime import datetime

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

    def __init__(self, debug_mode: bool) -> None:
        """Initialise the DataManager and configure logging."""
        self.logger = self.setup_logging(debug_mode=debug_mode)
        # Polling interval in seconds to check for new recordings.
        self.POLLING_INTERVAL_SEC = 1

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
                        f'New recording found: {new_recording_path}'
                    )

                    # TODO: Process the new recording

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
        'rosbags_directory',
        type=str,
        help='Path to the directory to monitor for new data.',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging.',
    )

    args = parser.parse_args()
    rosbags_directory = args.rosbags_directory
    debug_mode = args.debug
    data_manager = DataManager(debug_mode)

    data_manager.run(rosbags_directory)


if __name__ == '__main__':
    main()
