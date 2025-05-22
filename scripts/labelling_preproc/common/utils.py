import sys
import os
from pathlib import Path
from typing import Dict, Any


def directory_exists(directory: Path):
    """
    Verify if a directory exists

    :param directory: The directory to check
    :type directory: Path
    :raises ValueError: If the directory does not exist

    """
    if not directory.exists():
        raise FileNotFoundError(f' Directory \'{directory}\' does not exist')


def file_exists(file: Path):
    """
    Verify if a file exists

    :param file: The file to check
    :type file: Path
    :raises FileNotFoundError: If the file does not exist

    """
    if not file.is_file():
        raise FileNotFoundError(f' File \'{file}\' not found.')


def metadata_is_valid(metadata: Dict[str, Any]) -> bool:
    """
    Verify export metadata YAML object

    :param metadata: YAML dict objeect
    :type metadata: Dict[str, Any]
    :raises ValueError: If validation fails

    """

    def is_key_valid(key: str):
        if key not in metadata or not metadata[key]:
            raise ValueError(
                f' \'{key}\' key missing or empty in metadata file.'
            )

    # Note: If metadata format changes, these key names may not be valid anymore
    is_key_valid('rosbags')
    is_key_valid('time_sync_groups')


def get_env_var(env_var_key: str) -> str:
    """
    Retrieve the value of an environmental variable.

    :param env_var_key: The name of the environmental variable.
    :type env_var_key: str
    :returns: The value of the environmental variable.
    :rtype: str
    :raises ValueError: If the variable is not set.
    """
    env_value = os.getenv(env_var_key)
    if not env_value:
        raise ValueError(
            f' \'{env_var_key}\' environment variable was not found.'
        )
    return env_value


def show_progress_bar(log_name, progress, total, bar_length=50):
    """
    Displays or a console progress bar.

    :param progress: Current progress (int)
    :param total: Total value corresponding to 100% (int)
    :param bar_length: Character length of the progress bar (int)
    """
    # Calculate progress as a fraction and percentage
    fraction = progress / total
    percent = int(fraction * 100)

    # Create the bar string with '=' for completed part and '-' for remaining part
    completed_length = int(round(bar_length * fraction))
    bar = '=' * completed_length + '-' * (bar_length - completed_length)

    # Print the progress bar with carriage return to overwrite the line
    sys.stdout.write(f'\r📤 {log_name}: |{bar}| {percent} %')
    sys.stdout.flush()
