"""Module to define common functions."""

import os
import sys
from pathlib import Path
from typing import Any, Dict


def directory_exists(directory: Path):
    """
    Verify if a directory exists.

    Args:
        directory: The directory to check.

    Raises:
        FileNotFoundError: If the directory does not exist.
    """
    if not directory.exists():
        raise FileNotFoundError(f' Directory "{directory}" does not exist')


def file_exists(file: Path):
    """
    Verify if a file exists.

    Args:
        file: The file to check.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not file.is_file():
        raise FileNotFoundError(f' File "{file}" not found.')


def metadata_is_valid(metadata: Dict[str, Any]) -> bool:
    """
    Verify export metadata YAML object.

    Args:
        metadata: Dictionary parsed from YAML metadata file.

    Raises:
        ValueError: If required keys are missing or empty.
    """

    def is_key_valid(key: str):
        if key not in metadata or not metadata[key]:
            raise ValueError(f'"{key}" key missing or empty in metadata file.')

    # Note: If metadata format changes, these key names may not be
    #       valid anymore
    is_key_valid('rosbags')
    is_key_valid('time_sync_groups')


def get_env_var(env_var_key: str) -> str:
    """
    Retrieve the value of an environment variable.

    Args:
        env_var_key: The name of the environment variable.

    Returns:
        The value of the environment variable.

    Raises:
        ValueError: If the variable is not set.
    """
    env_value = os.getenv(env_var_key)
    if not env_value:
        raise ValueError(
            f'"{env_var_key}" environment variable' ' was not found.'
        )
    return env_value


def show_progress_bar(log_name, progress, total, bar_length=50):
    """
    Display a console progress bar.

    Args:
        log_name: Name to show in the progress log.
        progress: Current progress value.
        total: Total value corresponding to 100%.
        bar_length: Character length of the progress bar.
    """
    # Calculate progress as a fraction and percentage
    fraction = progress / total
    percent = int(fraction * 100)

    # Create the bar string with '=' for completed part and '-' for
    #  remaining part
    completed_length = int(round(bar_length * fraction))
    bar = '=' * completed_length + '-' * (bar_length - completed_length)

    # Print the progress bar with carriage return to overwrite the line
    sys.stdout.write(f'\r📤 {log_name}: |{bar}| {percent} %')
    sys.stdout.flush()
