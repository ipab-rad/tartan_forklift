"""Module to backup ROS bag files to S3 using boto3."""

import argparse
import logging
import queue
import time
from logging import Logger
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Optional

import colorlog

from labelling_preproc.common.s3_client import EIDFfS3Client, TartanAsset
from labelling_preproc.common.utils import get_env_var


class S3RosbagBackupAgent:
    """
    Handles uploading ROS bag recordings to S3 using a background process.

    This class manages a queue of recording directories and uploads them
    to an S3 bucket asynchronously. Use the `enqueue()` method to add
    new recordings for backup. Useful when handling multiple uploads
    without blocking.
    """

    def __init__(self, logger: Logger, dryrun: bool = False) -> None:
        """Initialise the class."""
        self.logger = logger
        self.dryrun = dryrun

        # Configure AWS S3
        aws_endpoint_url = get_env_var('AWS_ENDPOINT_URL')
        backup_bucket_name = get_env_var('AWS_ROSBAG_BACKUP_BUCKET_NAME')
        eidf_project_name = get_env_var('EIDF_PROJECT_NAME')
        self.s3_client = EIDFfS3Client(
            eidf_project_name, backup_bucket_name, aws_endpoint_url
        )

        self.to_backup_queue = Queue()

        self.uploading_process = None

        self.QUEUE_TIMEOUT_SEC = 1

        self.logger.info(
            'S3 backup agent initialised ->'
            f'S3 bucket: "{backup_bucket_name}" | '
            f'S3 endpoint: "{aws_endpoint_url}"'
        )

    def _get_rosbag_files(self, recording_path: Path) -> list[Path]:
        """
        Get a sorted list of ROS bag files in the recording path.

        Args:
            recording_path (Path): Path to the directory containing
                                   ROS bag files.
        """
        rosbag_list = recording_path.glob('*.mcap')

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

        return sorted(rosbag_list, key=extract_suffix_num)

    def _upload_file(
        self, filepath: Path, s3_key: str
    ) -> Optional[TartanAsset]:
        """
        Upload a single file to AWS S3.

        Args:
            filepath: Path to the file to be uploaded.
            s3_key: Desired key for the uploaded file in S3

        Returns:
            A TartanAsset instance if successful, otherwise None.
        """
        if self.dryrun:
            return TartanAsset(
                url=f's3://{self.s3_client.bucket_name}/{s3_key}',
                uuid='dryrun-uuid',
            )
        else:
            with filepath.open('rb') as f:
                asset_meta = self.s3_client.upload_file(f, s3_key)
                return asset_meta

    def _compute_upload_rate_mbps(
        self, file: Path, elapsed_time: float
    ) -> float:
        """
        Compute the upload rate in mega bits per second (Mbps).

        Args:
            file (Path): The file that was uploaded.
            elapsed_time (float): Time taken to upload the file in seconds.

        Returns:
            float: Upload rate in Mbps.
        """
        file_size_megabits = file.stat().st_size * 8 / (1000000)
        if elapsed_time > 0:
            upload_rate_mbps = file_size_megabits / elapsed_time
            return upload_rate_mbps
        return 0.0

    def _upload_recording(self, recording_path: Path) -> None:
        """
        Upload a ROS bag recording to S3.

        This method will find all ROS bag files in `recording_path`
        and upload them to S3. It will also upload a metadata file.

        Args:
            recording_path (Path): Path to the ROS bag recording directory.
        """
        rosbag_list = self._get_rosbag_files(recording_path)

        # Append metadata file
        metadata_file = recording_path / 'metadata.yaml'
        rosbag_list.append(metadata_file)

        total_files_count = len(rosbag_list)
        upload_counter = 1

        for rosbag in rosbag_list:
            # Keep the folder structure in the S3 bucket by using the path
            # relative to the recording's parent directory. This avoids
            # including the full local path in the S3 key
            rosbag_s3_key = str(rosbag.relative_to(recording_path.parent))

            self.logger.debug(
                f'[BackupAgent] Uploading {recording_path.name} '
                f' [{upload_counter}/{total_files_count}] ...'
            )
            now = time.time()
            asset_meta = self._upload_file(rosbag, rosbag_s3_key)
            elapsed_time = time.time() - now
            if asset_meta:
                rate_mbps = self._compute_upload_rate_mbps(
                    rosbag, elapsed_time
                )
                self.logger.debug(
                    f'[BackupAgent] {recording_path.name} '
                    f'[{upload_counter}/{total_files_count}] uploaded in '
                    f'{elapsed_time:.2f} s at {rate_mbps:.2f} Mbps'
                )
                upload_counter += 1

    def _backup_routine(self):
        """
        Routine to handle the backup process in a separate process.

        This method will continuously check the queue for new ROS bag
        recordings to back up. It will block until uploading is complete.
        It will exit when there are no more recordings to back up.
        """
        while True:
            try:
                rosbag_recording = self.to_backup_queue.get(
                    timeout=self.QUEUE_TIMEOUT_SEC
                )
                # Block until the upload is complete
                self._upload_recording(rosbag_recording)
            except queue.Empty:
                self.logger.debug(
                    '[BackupAgent]: No more ROS bag recordings'
                    'to back up. Pausing backup routine'
                )
                break

    def _start_uploading_process(self):
        self.uploading_process = Process(target=self._backup_routine)
        self.uploading_process.start()

    def enqueue(self, recording_path: Path):
        """
        Enqueue a ROS bag recording for backup.

        Args:
            recording_path (Path): Path to the ROS bag recording directory.
        """
        self.to_backup_queue.put(recording_path)

        # Re-start the uploading process routine if needed
        if not self.uploading_process or not self.uploading_process.is_alive():
            self._start_uploading_process()


def main():
    """
    Entry point for the script.

    Parses command-line arguments and starts the S3RosbagBackupAgent.
    """
    parser = argparse.ArgumentParser(
        description='Backup ROS bag recording to S3 bucket.'
    )

    parser.add_argument(
        '--recordings_list',
        nargs='+',  # Accepts one or more arguments as a list
        type=str,
        help='List of ROS bag recording directories to back up.',
    )
    parser.add_argument(
        '--debug', action='store_true', help='Enable debug logging.'
    )
    parser.add_argument(
        '--dryrun',
        action='store_true',
        help=(
            'List files that would be uploaded without '
            'actually uploading them.'
        ),
    )

    args = parser.parse_args()

    # Define basic logger
    logger = logging.getLogger('BackupAgent')
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(asctime)s %(log_color)s%(levelname)s%(reset)s: %(message)s'
        )
    )
    logger.addHandler(handler)

    recordings_list = args.recordings_list
    if not recordings_list:
        logger.error('No recordings provided. Exiting.')
        return

    backup_agent = S3RosbagBackupAgent(logger, args.dryrun)

    for rosbag_recording_dir in recordings_list:
        rosbag_recording_path = Path(rosbag_recording_dir)
        if not rosbag_recording_path.exists():
            logger.error(f'Recording {rosbag_recording_path} does not exist.')
            continue

        if not rosbag_recording_path.is_dir():
            logger.error(
                f'Recording {rosbag_recording_path} is not a directory.'
            )
            continue

        logger.info(f'Enqueuing recording: {rosbag_recording_path}')
        backup_agent.enqueue(rosbag_recording_path)


if __name__ == '__main__':
    main()
