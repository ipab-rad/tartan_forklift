"""Metadata generator module."""

import argparse
import hashlib
import json
import os
from datetime import datetime

from mcap.reader import make_reader

DEFAULT_PATH = '/recorded_datasets/edinburgh'


def read_mcap_file(mcap_file_path):
    """
    Read an MCAP file and extract metadata.

    Parameters:
    mcap_file_path (str): The path to the MCAP file.

    Returns:
    dict: A dictionary containing information.
    """
    topic_message_counts = {}
    topic_message_types = {}
    start_time = None
    end_time = None

    with open(mcap_file_path, 'rb') as f:
        reader = make_reader(f)
        for schema, channel, message in reader.iter_messages():
            if start_time is None:
                start_time = message.log_time
            end_time = message.log_time

            topic = channel.topic
            if topic not in topic_message_counts:
                topic_message_counts[topic] = 0
                topic_message_types[topic] = (
                    schema.name
                )  # Store the message type
            topic_message_counts[topic] += 1

    duration = end_time - start_time
    duration_seconds = duration / 1e9  # Convert nanoseconds to seconds

    result = {
        'duration': f'{duration_seconds: .0f}s',
        'topics': topic_message_counts,
        'types': topic_message_types,
    }

    return result


def get_file_size(file_path):
    """
    Get the size of a file.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    int: The size of the file in bytes.
    """
    return os.path.getsize(file_path)


def get_file_hash(file_path):
    """
    Get the hash of a file.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    str: The MD5 hash of the file.
    """
    hash_func = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def generate_metadata(file_path, root_dir):
    """
    Generate metadata for an MCAP file.

    Parameters:
    file_path (str): The path to the MCAP file.
    root_dir (str): The root directory for relative path calculation.

    Returns:
    tuple: A tuple containing the relative path and the metadata dictionary.
    """
    relative_path = os.path.relpath(file_path, root_dir)
    mcap_info = read_mcap_file(file_path)
    metadata = {
        'name': os.path.basename(file_path),
        'resource:identifier': os.path.splitext(os.path.basename(file_path))[
            0
        ],
        'resource:description': 'Rosbag MCAP log file',
        'resource:format': 'MCAP',
        'resource:licence': 'cc-by-4.0',
        'resource:size': get_file_size(file_path),
        'resource:hash': get_file_hash(file_path),
        'resource:issued': datetime.now().strftime('%Y-%m-%d'),
        'resource:modified': datetime.fromtimestamp(
            os.path.getmtime(file_path)
        ).strftime('%Y-%m-%d'),
        'duration': mcap_info['duration'],
        'topics': mcap_info['topics'],
        'types': mcap_info['types'],  # Include the message types
    }
    return relative_path, metadata


def create_resources_json(directory):
    """
    Create a JSON file containing metadata for all MCAP files in a directory.

    Parameters:
    directory (str): The directory to search for MCAP files.
    """
    resources = {
        'name': 'dataset',
        'resource:identifier': 'Autonomous driving dataset',
        'resource:description': 'description of the dataset',
        'resource:licence': 'cc-by-4.0',
        'resource:format': 'MCAP',
    }

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.mcap'):
                file_path = os.path.join(root, file)
                relative_path, metadata = generate_metadata(
                    file_path, directory
                )
                resources[relative_path] = metadata

    with open(os.path.join(directory, 'resources.json'), 'w') as json_file:
        json.dump(resources, json_file, indent=4)


def main():
    """Parse arguments and generate metadata."""
    parser = argparse.ArgumentParser(
        description='Generate metadata for Rosbag MCAP files.'
    )
    parser.add_argument(
        '-p',
        type=str,
        default=DEFAULT_PATH,
        help='Path to the directory containing MCAP files',
    )

    args = parser.parse_args()
    create_resources_json(args.p)


if __name__ == '__main__':
    main()
