#!/usr/bin/env python3

"""
Utility script for merging ROS2 bag (.mcap) files.

The script expects a YAML file containing the location of the
rosbags and the topics to merge
"""

import argparse
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import yaml


def sort_by_numeric_suffix(files):
    """
    Sorts a list of files by numeric suffix extracted from their filenames.

    Args:
        files (list): List of file paths.

    Returns:
        list: Sorted list of file paths based on numeric suffix.
    """

    def extract_number(file):
        match = re.search(r'_(\d+)\.mcap$', file)
        return (
            int(match.group(1)) if match else float('inf')
        )  # Non-matching files go to the end

    return sorted(files, key=extract_number)


def create_temp_yaml(input_rosbag_dir, yaml_file):
    """
    Modify 'uri' in a YAML file and save to a temporary file.

    Args:
        input_rosbag_dir (str): Directory to prepend to 'uri' values.
        yaml_file (str): Path to the original YAML file.

    Returns:
        list: [bool, str] - Success status and path to the
                            temporary file or an
                            empty string on error.
    """
    with open(yaml_file) as file:
        data = yaml.safe_load(file)

    # Modify the 'uri' parameter
    if 'output_bags' in data:
        for entry in data['output_bags']:
            if 'uri' in entry:
                entry['uri'] = os.path.join(input_rosbag_dir, entry['uri'])

    # Create a temporary file in the current directory
    tmp_yaml_path = os.path.join(
        input_rosbag_dir, 'ros2_convert_temp_params.yaml'
    )

    try:
        with open(tmp_yaml_path, 'w') as temp_file:
            yaml.dump(data, temp_file, default_flow_style=False)

    except FileNotFoundError as e:
        print(f'Error: File not found - {e}')
        return [False, '']
    except yaml.YAMLError as e:
        print(f'Error: Failed to parse YAML - {e}')
        return [False, '']

    return [True, tmp_yaml_path]


def merge_rosbags(input_dir: str, yaml_file_path: str, range_str: str = None):
    """
    Automates the merging of ROS2 bag files using the ros2 bag convert command.

    Args:
        input_dir (str): Path to the directory containing .mcap files.
        yaml_file_path (str): Path to the YAML configuration file
    """
    # Load the YAML file
    try:
        with open(yaml_file_path) as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)
    except (OSError, yaml.YAMLError) as e:
        raise ValueError(
            f'Failed to load YAML file: {yaml_file_path}. Error: {e}'
        )

    output_bag_name = yaml_content.get('output_bags', [{}])[0].get('uri')

    if not output_bag_name:
        raise ValueError(
            'No \"uri\" value found in \'output_bags\' of YAML file: '
            f'{yaml_file_path}'
        )

    # Find all .mcap files in the given directory in a non-recursive way
    mcap_files = [str(file) for file in Path(input_dir).glob('*.mcap')]
    mcap_files = sort_by_numeric_suffix(mcap_files)

    if not mcap_files:
        print(f'No .mcap files found in directory: {input_dir}')
        return

    if range_str:
        try:
            start, end = map(int, range_str.split(':'))
            mcap_files = mcap_files[
                start : end + 1  # noqa: E203
            ]  # Slice the list based on the range
        except (ValueError, IndexError):
            print(
                f'Invalid range: {range_str}. Expected format is \"start:end\"'
            )
            return

    if not mcap_files:
        print(f'No files in the specified range: {range_str}')
        return

    ok_status, temp_param_yaml = create_temp_yaml(input_dir, yaml_file_path)

    # Construct the ros2 bag convert command
    command = ['ros2', 'bag', 'convert']

    print(f'Rosbags to be merged and filtered based on {yaml_file_path}')
    for mcap_file in mcap_files:
        print(mcap_file)
        command.extend(['--input', mcap_file])

    # Add the YAML file location
    command.extend(['mcap', '--output-options', temp_param_yaml])

    if not ok_status:
        print('There was an error creating a temp yaml, aborting...')
        return

    # Execute the command
    try:
        start_time = time.time()

        subprocess.run(command, check=True)

        end_time = time.time()

        elapsed_time = end_time - start_time
        print(
            f'{len(mcap_files)} rosbags mereged and filtered in '
            f'{elapsed_time:.2f} seconds'
        )

        range_subfix = ''
        if range_str:
            range_subfix = f'_{start}-{end}'

        # Obtain rosbag base name from one of the .mcap files
        base_name = mcap_files[0].rsplit('/', -1)[-1]
        base_name = base_name.rsplit('_', 1)[0]

        # print(f'Bag base name: {base_name}')
        new_bag_file_name = (
            base_name + '_' + output_bag_name + range_subfix + '.mcap'
        )

        new_bag_file_path = os.path.join(
            input_dir, output_bag_name, new_bag_file_name
        )

        # Rename rosbag for consistency
        output_bag_file_path = os.path.join(
            input_dir, output_bag_name, output_bag_name + '_0.mcap'
        )

        shutil.move(output_bag_file_path, new_bag_file_path)

        print(f'Saved as: {new_bag_file_path}')
    except subprocess.CalledProcessError as e:
        print(f'Error during conversion: {e}')

    # Ensure the temp yaml file is deleted afterwards
    if os.path.exists(temp_param_yaml):
        os.remove(temp_param_yaml)


def main():
    """Parse command-line arguments and initiate the merging process."""
    parser = argparse.ArgumentParser(
        description='Convert ROS2 bags in a directory.'
    )
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Path to the directory containing .mcap files',
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to the YAML configuration file for --output-options',
    )
    parser.add_argument(
        '--range',
        type=str,
        help='Range of indices to process in the format \"start:end\"',
    )

    # Parse arguments
    args = parser.parse_args()

    # Call the conversion function with parsed arguments
    merge_rosbags(args.input, args.config, args.range)


if __name__ == '__main__':
    main()
