"""Module to define dataclass types."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Rosbag:
    """A class representing a ROS bag file information."""

    absolute_path: Path
    size_bytes: int


@dataclass
class Parameters:
    """A class representing the parameters for the upload process."""

    local_host_user: str
    local_hostname: str
    local_rosbags_directory: str
    cloud_user: str
    cloud_hostname: str
    cloud_ssh_alias: str
    cloud_upload_directory: str
    mcap_bin_path: str
    mcap_compression_chunk_size: int
    compression_parallel_workers: int
    compression_queue_max_size: int
