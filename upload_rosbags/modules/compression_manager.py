"""This module contains the implementation to compress .mcap rosbags."""

import subprocess
import threading
import time
from logging import Logger
from multiprocessing import Queue
from pathlib import Path
from queue import Empty
from typing import List, Optional

from upload_rosbags.modules.data_types import Parameters, Rosbag


class CompressionManager:
    """
    Implements the compression of .mcap rosbags using the mcap CLI.

    It is designed to spawn multiple threads to compress rosbags in parallel.
    """

    def __init__(
        self,
        rosbag_directory: str,
        rosbags_list: List[Rosbag],
        temp_directory: str,
        params: Parameters,
        logger: Logger,
    ) -> None:
        """Initialise the CompressionManager."""
        self.rosbag_directory = rosbag_directory
        self.rosbags_list = rosbags_list
        self.temp_directory = Path(temp_directory)
        self.params = params
        self.logger = logger

        self.compressed_rosbags_queue = Queue()
        self.threads: List[threading.Thread] = []
        self.stop_event = threading.Event()

        self.pending_rosbags_indices = set(range(len(rosbags_list)))
        self.pending_lock = threading.Lock()

    def compress_rosbag(self, rosbag: Rosbag, thread_id) -> Rosbag:
        """Compress a rosbag file using mcap CLI."""
        # Define temporary path for the compressed rosbag
        compressed_rosbag_path = (
            self.temp_directory / rosbag.absolute_path.name
        )

        compress_cmd = [
            self.params.mcap_bin_path,
            'compress',
            str(rosbag.absolute_path),
            '-o',
            str(compressed_rosbag_path),
            '--chunk-size',
            str(self.params.mcap_compression_chunk_size),
        ]

        self.logger.debug(
            f'Compression thread [{thread_id}]: '
            f'Compressing {rosbag.absolute_path.name}...'
        )

        start = time.time()
        try:
            subprocess.run(compress_cmd, check=True)
        except subprocess.CalledProcessError as e:
            # TODO:  Handle error appropriately
            self.logger.warning(f'Compression failed: {e}')

        duration = time.time() - start
        file_size = 0
        if compressed_rosbag_path.is_file():
            file_size = compressed_rosbag_path.stat().st_size

        self.logger.debug(
            f'Compression thread [{thread_id}]: '
            f'finished in {duration:.2f} seconds '
        )

        return Rosbag(
            absolute_path=compressed_rosbag_path, size_bytes=file_size
        )

    def compression_worker(self, thread_id: int) -> None:
        """
        Thread function to compress rosbags.

        Maintains a loop that checks for new rosbags to compress
        and compresses them until an stop event is set without going beyond
        the maximum size of the queue.
        """
        while not self.stop_event.is_set():
            # Do not go beyond the maximum size of the queue
            if (
                self.compressed_rosbags_queue.qsize()
                >= self.params.compression_queue_max_size
            ):
                time.sleep(0.5)
                continue

            # Get the next rosbag to compress
            with self.pending_lock:
                if not self.pending_rosbags_indices:
                    break
                idx = self.pending_rosbags_indices.pop()

            rosbag = self.rosbags_list[idx]

            compressed_rosbag = self.compress_rosbag(rosbag, thread_id)

            self.compressed_rosbags_queue.put(compressed_rosbag)

    def start_compression(self) -> None:
        """
        Start the compression routine in multiple threads.

        This function does not block. It starts the threads and
        returns immediately.
        """
        for thread_id in range(self.params.compression_parallel_workers):
            t = threading.Thread(
                target=self.compression_worker, args=(thread_id,), daemon=True
            )
            t.start()
            self.threads.append(t)

    def is_running(self) -> bool:
        """Check if the compression process is still active."""
        with self.pending_lock:
            return bool(self.pending_rosbags_indices) or any(
                t.is_alive() for t in self.threads
            )

    def stop(self) -> None:
        """Halt the compression process."""
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=1)
        self.threads.clear()

    def get_compressed_bag(self) -> Optional[Rosbag]:
        """
        Return a compressed rosbag from the queue.

        This function blocks until a compressed rosbag is available.
        If there are no more compressed rosbags and the compression
        process has finished, it returns None. This guarantees that
        no more rosbags will be added to the queue.
        """
        while True:
            try:
                return self.compressed_rosbags_queue.get(timeout=0.5)
            except Empty:
                # Check if the compression manager is still running
                if not self.is_running():
                    # Nothing left to compress, and the queue won't be
                    # filled again
                    return None

                # Compression is still in progress, wait a bit and try again
                time.sleep(0.5)
