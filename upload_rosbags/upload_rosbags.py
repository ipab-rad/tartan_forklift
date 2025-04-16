"""
This module provides a CLI interface for uploading rosbags to a cloud server.

It implements the RosbagUploader class, which handles the compression
and uploading process, and the main function that parses
command-line arguments.
"""

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path
from typing import List

import colorlog


from upload_rosbags.modules.config_parser import ConfigParser
from upload_rosbags.modules.ssh_client import SSHClient


@dataclass
class Rosbag:
    """A class representing a ROS bag file information."""

    absolute_path: Path
    size_bytes: int


class RosbagUploader:
    """
    A class for compressing and uploading ROS bag files to a cloud server.

    It expects a YAML parameters file with and runtime arguments to:
     - Stablish a connection to the cloud server
     - Compress the bag files using `mcap` CLI
     - Upload the compressed files to the cloud server using SSH and `lftp`
       tool

    """

    def __init__(self, config_path, lftp_password, debug) -> None:
        """Initialise the uploader attributes."""
        self.params = ConfigParser().load_config(config_path)

        self.lftp_password = lftp_password

        self.logger = self.setup_logging(debug_mode=debug)

        self.ssh_client = SSHClient(self.params, self.logger)

    def setup_logging(self, debug_mode) -> logging.Logger:
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

    def get_rosbag_directories(self, base_directory) -> List[str]:
        """List directories on the local machine containing .mcap files."""
        base_path = Path(base_directory)
        # Use a set to avoid duplicates
        directories = {
            file.parent.resolve() for file in base_path.rglob('*.mcap')
        }
        sorted_dirs = sorted(str(d) for d in directories)

        return sorted_dirs

    def get_rosbags_from_directory(self, directory: str) -> List[Rosbag]:
        """
        Find all .mcap files in a directory.

        Return a list of Rosbag objects containing the absolute path and
        size in bytes. The list is sorted based on the rosbag suffix number
        """
        base_path = Path(directory)

        # Helper lambda function to sort the files
        def order_based_on_sufix(path: Path):
            stem = path.stem
            suffix_str = stem.rsplit('_', 1)[-1]
            return int(suffix_str) if suffix_str.isdigit() else -1

        mcap_files = sorted(
            (file for file in base_path.rglob('*.mcap') if file.is_file()),
            key=order_based_on_sufix,
        )
        return [
            Rosbag(
                absolute_path=file.resolve(), size_bytes=file.stat().st_size
            )
            for file in mcap_files
        ]

    def upload_file(self, file_abs_path) -> None:
        """Upload a single bag file to a remote server using ssh and lftp."""
        # Create the destination path
        file_abs_path_obj = Path(file_abs_path)
        local_base = Path(self.params.local_rosbags_directory)
        cloud_base = Path(self.params.cloud_upload_directory)

        # Compute relative path and construct remote destination
        relative_file_path = file_abs_path_obj.relative_to(local_base)
        remote_destination = cloud_base / relative_file_path

        # Make sure the parent directory exists
        mkdir_cmd = f'mkdir -p {str(remote_destination.parent)}'
        self.ssh_client.send_command(mkdir_cmd)

        self.logger.debug(
            f'Uploading {file_abs_path} to {str(remote_destination)}'
        )

        # Upload the file using lftp and the ssh client
        lftp_cmd = (
            f'lftp -u "{self.params.local_host_user},'
            f'{self.lftp_password}" {self.params.local_hostname} '
            f'-e "pget -n 4 \"{file_abs_path}\" '
            f'-o \"{str(remote_destination)}\"; bye"'
        )

        self.logger.debug(f'ftp cmd: "{lftp_cmd}"')

        stdout, stderr = self.ssh_client.send_command(lftp_cmd)

    def process_rosbags_in_directory(
        self,
        rosbag_directory,
        rosbags_list: List[Rosbag],
        uploaded_rosbags: int,
        total_rosbags: int,
    ) -> tuple[int, float]:
        """Compress and upload rosbags in a directory."""
        # TODO: Start compression logic here. For now, create a
        # queue to simulate the future compression queueing process
        compressed_rosbags_queue = Queue()
        for rosbag in rosbags_list:
            compressed_rosbags_queue.put(rosbag)

        current_rosbag_count = uploaded_rosbags
        total_uploading_time = 0

        # Upload metadata file if it exists
        metadata_file = Path(rosbag_directory) / 'metadata.yaml'
        if metadata_file.exists():
            self.logger.info(
                f'Uploading metadata file: {metadata_file.absolute()}'
            )
            self.upload_file(str(metadata_file.absolute()))

        # Upload until the queue is empty
        while compressed_rosbags_queue.qsize() > 0:
            rosbag = compressed_rosbags_queue.get()
            self.logger.info(
                f'Uploading [{current_rosbag_count + 1}/{total_rosbags}]: '
                f'{rosbag.absolute_path.name} ({rosbag.size_bytes} bytes)'
            )

            file_path = str(rosbag.absolute_path)
            start_time = time.time()
            self.upload_file(file_path)
            elapsed_time = time.time() - start_time

            total_uploading_time += elapsed_time
            current_rosbag_count += 1

            time_str = self.compute_time_string(int(elapsed_time))
            throughput = self.compute_throughput(
                rosbag.size_bytes, elapsed_time
            )
            self.logger.info(
                f'Upload completed in ' f'{time_str} at {throughput:.2f} Gbps'
            )

        return current_rosbag_count, total_uploading_time

    def run(self):
        """Start the uploading process."""
        self.logger.info('Starting the upload process...')

        rosbags_directories = self.get_rosbag_directories(
            self.params.local_rosbags_directory
        )
        self.logger.info(
            f'{len(rosbags_directories)} rosbags directories found:'
            f'\n\t' + '\n\t'.join(rosbags_directories)
        )

        total_rosbags = 0
        rosbags_directories_dict = {}
        # Get the list of rosbags in each directory
        for rosbag_directory in rosbags_directories:
            rosbags_list = self.get_rosbags_from_directory(rosbag_directory)
            rosbags_directories_dict[rosbag_directory] = rosbags_list
            total_rosbags += len(rosbags_list)

        # To track progress
        uploaded_rosbags = 0
        global_upload_time = 0
        # Process each directory
        try:
            for directory, rosbags_list in rosbags_directories_dict.items():
                print('')
                self.logger.info(f'Processing directory: {directory}')
                total_uploads, total_upload_time = (
                    self.process_rosbags_in_directory(
                        directory,
                        rosbags_list,
                        uploaded_rosbags,
                        total_rosbags,
                    )
                )
                # Update the global count of uploaded rosbags
                # so it is carried over to the next for iteration
                uploaded_rosbags = total_uploads
                global_upload_time += total_upload_time

            print('')
            self.logger.info(
                f'{uploaded_rosbags} rosbags were uploaded in '
                f'{self.compute_time_string(int(global_upload_time))}'
            )

        except KeyboardInterrupt:
            self.logger.info('Upload process interrupted by user.')

    @staticmethod
    def compute_time_string(total_seconds: int) -> str:
        """Convert total seconds into a human-readable time string."""
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return (
            f'{hours} hours {minutes} minutes {seconds} seconds'
            if hours
            else (
                f'{minutes} minutes {seconds} seconds'
                if minutes
                else f'{seconds} seconds'
            )
        )

    @staticmethod
    def compute_throughput(file_size: int, elapsed_time: float) -> float:
        """Compute the throughput in Gigabit/sec."""
        file_size_gigabits = (file_size * 8) / (1024**3)
        return file_size_gigabits / elapsed_time


def main():
    """Parse command-line arguments and run the uploader."""
    parser = argparse.ArgumentParser(description='Rosbag Uploader')
    parser.add_argument(
        '--config', required=True, help='Path to the YAML configuration file'
    )
    parser.add_argument(
        '--lftp-password', required=True, help='Password for lftp server'
    )
    parser.add_argument(
        '--debug',
        required=False,
        help='Wether to run in debug mode',
        action='store_true',
    )
    args = parser.parse_args()

    uploader = RosbagUploader(args.config, args.lftp_password, args.debug)
    uploader.run()


if __name__ == '__main__':
    main()
