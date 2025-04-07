"""
This module provides a CLI interface for uploading rosbags to a cloud server.

It implements the RosbagUploader class, which handles the compression
and uploading process, and the main function that parses
command-line arguments.
"""

import argparse
import logging
import time
from datetime import datetime

import colorlog

from upload_rosbags.modules.config_parser import ConfigParser


class RosbagUploader:
    """
    A class for compressing and uploading ROS bag files to a cloud server.

    It expects a YAML parameters file with and runtime arguments to:
     - Stablish a connection to the cloud server
     - Compress the bag files using `mcap` CLI
     - Upload the compressed files to the cloud server using SSH and `lftp`
       tool

    """

    def __init__(self, config_path):
        """Initialise the uploader attributes."""
        self.params = ConfigParser().load_config(config_path)

        self.logger = self.setup_logging(debug_mode=False)

    def setup_logging(self, debug_mode):
        """Configure logging with color support."""
        # Timestamp for the log file name
        timestamp = datetime.now().strftime('%Y_%m_%d-%H_%M_%S')
        log_filename = f'{timestamp}_rosbag_upload.log'

        # Create a logger
        logger = logging.getLogger('upload_rosbags')
        logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        logger.propagate = False

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        # Create a file handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)

        # Create a colored formatter for the console handler
        color_formatter = colorlog.ColoredFormatter(
            '%(asctime)s %(log_color)s%(levelname)s%(reset)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        console_handler.setFormatter(color_formatter)

        # Create a regular formatter for the file handler
        file_formatter = logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        file_handler.setFormatter(file_formatter)

        # Add the handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def run(self):
        """Start the uploading process."""
        self.logger.info('Starting the upload process...')

        counter = 0
        try:
            while True:
                self.logger.info(f'Uploading bag file {counter}...')
                time.sleep(1)  # Simulate the upload process
                counter += 1
        except KeyboardInterrupt:
            self.logger.info('Upload process interrupted by user.')


def main():
    """Parse command-line arguments and run the uploader."""
    parser = argparse.ArgumentParser(description='Rosbag Uploader')
    parser.add_argument(
        '--config', required=True, help='Path to the YAML configuration file'
    )
    args = parser.parse_args()

    uploader = RosbagUploader(args.config)
    uploader.run()


if __name__ == '__main__':
    main()
