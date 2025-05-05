"""
This module provides a CLI interface for compressing rosbags (.mcap).

It implements the RosbagCompressor class, which is responsible for compressing
rosbags using a CiompressionManager.
"""

import argparse
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List

import colorlog

from upload_rosbags.modules.compression_manager import CompressionManager
from upload_rosbags.modules.data_types import Parameters, Rosbag


class RosbagCompressor:
    """
    Compress rosbags (.mcap) using CompressionManager.

    This class manages the following workflow:
    - Find directories containing .mcap files.
    - For each directory, find all .mcap files.
    - Copy the metadata.yaml file to the destination directory
    - Feed the rosbag list to the CompressionManager.
    - Wait for the compression to finish.
    - Log statistics.
    - Handle keyboard interrupts.
    """

    def __init__(self, rosbags_directory: str, destination_directory: str):
        """Initialise the RosbagCompressor."""
        self.rosbags_directory = Path(rosbags_directory)
        self.destination_directory = Path(destination_directory)

        self.logger = self.setup_logging(debug_mode=True)

        self.params = Parameters(
            local_host_user='',
            local_hostname='',
            local_rosbags_directory='',
            cloud_user='',
            cloud_hostname='',
            cloud_ssh_alias='',
            cloud_upload_directory='',
            mcap_bin_path='/home/eidf150/eidf150/hectorc/mcap',
            mcap_compression_chunk_size=62914560,
            compression_parallel_workers=6,
            compression_queue_max_size=9,
        )

    def setup_logging(self, debug_mode) -> logging.Logger:
        """Configure logging with color support."""
        # Timestamp for the log file name
        timestamp = datetime.now().strftime('%Y_%m_%d-%H_%M_%S')
        log_filename = f'{timestamp}_rosbag_compressor.log'

        # Create a logger
        logger = logging.getLogger('compress_rosbags')
        logger.setLevel(logging.DEBUG)

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

    def run(self):
        """Start the compression process."""
        # Get the list of directories containing .mcap files
        directories = self.get_rosbag_directories(self.rosbags_directory)
        self.logger.info(
            f'Found {len(directories)} directories containing .mcap files.'
        )

        total_rosbags = 0
        tota_compression_time = 0
        for directory in directories:
            # Get the list of .mcap files in the directory
            rosbags = self.get_rosbags_from_directory(directory)
            self.logger.info(
                f'Processing directory: {directory} ({len(rosbags)} rosbags)'
            )
            destination_path = (
                Path(self.destination_directory) / Path(directory).name
            )

            # Make sure the path exists
            destination_path.mkdir(parents=True, exist_ok=True)

            metadata_file = Path(directory) / 'metadata.yaml'
            if metadata_file.exists():
                self.logger.info('Copying metadata file')
                shutil.copy2(metadata_file, destination_path)

            # Create a CompressionManager instance
            compressor = CompressionManager(
                rosbag_directory=directory,
                rosbags_list=rosbags,
                temp_directory=str(destination_path),
                params=self.params,
                logger=self.logger,
            )

            # Compress the rosbags
            compressor.start_compression()

            try:
                start = time.time()
                # Wait for the compression to finish
                while True:
                    comp_rosbag = compressor.get_compressed_bag()
                    if comp_rosbag is None:
                        # No moree rosbags to compress
                        break

                duration = time.time() - start
                self.logger.info(
                    f'{len(rosbags)} compressed in '
                    f'{self.compute_time_string(int(duration))}'
                )

                total_rosbags += len(rosbags)
                tota_compression_time += duration

            except KeyboardInterrupt:
                self.logger.info('Compression interrupted by user.')
                compressor.stop()
                break
        print('')
        self.logger.info(
            f'Finished compressing {total_rosbags} rosbags in '
            f'{self.compute_time_string(int(tota_compression_time))} \n'
        )

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


def main():
    """Parse command-line arguments and run the compressor."""
    parser = argparse.ArgumentParser(description='Compress ROS bags.')
    parser.add_argument(
        'rosbags_directory',
        type=str,
        help='Directory containing the .mcap files to compress.',
    )
    parser.add_argument(
        'destination_directory',
        type=str,
        help='Directory to save the compressed rosbag directory.',
    )
    args = parser.parse_args()
    rosbags_directory = args.rosbags_directory
    destination_directory = args.destination_directory

    compressor = RosbagCompressor(rosbags_directory, destination_directory)
    compressor.run()


if __name__ == '__main__':
    main()
