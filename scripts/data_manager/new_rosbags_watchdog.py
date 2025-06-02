"""Watchdog module to monitor and handle new ROS bag recordings."""

import copy
import queue
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from data_manager.rosbag_metadata_parser import RosbagMetadataParser

from watchdog.events import FileSystemEvent, FileSystemEventHandler


@dataclass
class RosbagRecordingMeta:
    """Metadata associated with a ROS bag recording directory."""

    absolute_path: Path = None
    expected_rosbags: list[Path] = None
    received_rosbags: list[Path] = None


class NewRosbagsWatchdog(FileSystemEventHandler):
    """
    Watchdog handler for new ROS bag recordings.

    This class monitors a directory for the arrival of new ROS bag files
    and their associated `metadata.yaml`. Each ROS bag recording directory
    should include one metadata file and at lest one ROS bag file.

    Rather than relying on file creation or modification events, this class
    responds to the `on_closed` event. This guarantees that a file is fully
    written to disk before it is processed, this is necessary when dealing
    with large ROS bag files that may be copied or streamed over time.

    Once all the expected ROS bag files listed in the metadata are received,
    the class enqueues the full directory path for downstream processing.
    """

    def __init__(self, logger) -> None:
        """Initialise the handler."""
        self.logger = logger
        self.meta_parser = RosbagMetadataParser()
        self.rosbags_directories = {}
        self.rosbag_recording_queue = queue.Queue()
        super().__init__()

    def _is_rosbag_metadata(self, path: str) -> bool:
        """
        Check if the given path is a ROS bag metadata file.

        Args:
            path: Path string to check.

        Returns:
            True if it's a metadata.yaml file, otherwise False.
        """
        return path.endswith('metadata.yaml')

    def _is_rosbag_file(self, path: str) -> bool:
        """
        Check if the given path is a ROS bag `.mcap` data file.

        Args:
            path: Path string to check.

        Returns:
            True if it's an `.mcap` file, otherwise False.
        """
        return path.endswith('.mcap')

    def _all_rosbags_received(
        self, rosbag_recording_meta: RosbagRecordingMeta
    ) -> bool:
        """
        Determine if all expected ROS bag files for a recording have arrived.

        Args:
            rosbag_recording_meta: Object tracking expected and received files.

        Returns:
            True if the set of received files matches the expected files.
        """
        expected_set = set(rosbag_recording_meta.expected_rosbags)
        received_set = set(rosbag_recording_meta.received_rosbags)
        return expected_set == received_set

    def _handle_metadata_file(self, metadata_file: Path) -> None:
        """
        Parse and store expected `.mcap` files from the metadata file.

        Args:
            metadata_file: Path to the metadata.yaml file.
        """
        rosbag_directory = str(metadata_file.parent)
        expected_rosbags = self.meta_parser.get_expected_rosbag_files(
            metadata_file
        )

        meta = self.rosbags_directories.get(rosbag_directory)
        if meta is None:
            # First time seeing this directory, no files yet
            self.logger.debug(
                '[NewRosbagsWatchdog] New rosbag directory detected: '
                f'{rosbag_directory}'
            )
            self.rosbags_directories[rosbag_directory] = RosbagRecordingMeta(
                absolute_path=metadata_file.parent,
                expected_rosbags=copy.deepcopy(expected_rosbags),
                received_rosbags=[],
            )
        else:
            meta.expected_rosbags = copy.deepcopy(expected_rosbags)

    def _handle_rosbag_file(self, rosbag_file: Path) -> None:
        """
        Track a new ROS bag file and check for recording completion.

        Args:
            rosbag_file: Path to the received ROS bag file.
        """
        rosbag_directory = str(rosbag_file.parent)
        meta = self.rosbags_directories.get(rosbag_directory)

        if meta is None:
            # First time seeing this directory, no metadata yet
            self.logger.debug(
                '[NewRosbagsWatchdog] New rosbag directory detected: '
                f'{rosbag_directory}'
            )
            self.rosbags_directories[rosbag_directory] = RosbagRecordingMeta(
                absolute_path=None,
                expected_rosbags=None,
                received_rosbags=[rosbag_file],
            )
            return

        meta.received_rosbags.append(rosbag_file)

        if meta.absolute_path is None:
            # Metadata hasn't been received yet
            return

        if self._all_rosbags_received(meta):
            self.logger.debug(
                '[NewRosbagsWatchdog] All ROS bags received for: '
                f'{rosbag_directory}'
            )
            self.rosbag_recording_queue.put(rosbag_directory)
            self.rosbags_directories.pop(rosbag_directory)

    def on_closed(self, event: FileSystemEvent) -> None:
        """
        Handle the event triggered when a file is closed.

        File creation can trigger multiple events in watchdog: created,
        modified, and finally closed. This method uses `on_closed` to
        ensure the file is fully written to disk before processing.

        This is important for large ROS bag files that may
        still be copying or growing when the file is first created.

        Args:
            event: A watchdog file system event.
        """
        path = event.src_path

        if self._is_rosbag_metadata(path):
            self._handle_metadata_file(Path(path))
        elif self._is_rosbag_file(path):
            self._handle_rosbag_file(Path(path))

    def are_there_more_recordings(self) -> Optional[str]:
        """
        Get a ROS bag recording directory if available.

        Returns:
            The path to the directory if available, or None if the queue
            is empty.
        """
        try:
            return self.rosbag_recording_queue.get_nowait()
        except queue.Empty:
            return None
