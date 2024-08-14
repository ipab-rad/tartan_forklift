"""This script automates the upload of rosbags from vehicle to cloud host."""

import argparse
import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

import yaml


# Setup logging
logging.basicConfig(
    filename='upload_vehicle_data.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
)


def run_ssh_command(remote_user, remote_ip, command):
    """Run a command on the remote machine using SSH."""
    ssh_command = f"ssh {remote_user}@{remote_ip} '{command}'"
    try:
        subprocess.run(ssh_command, shell=True, check=True)
        logging.info(f'Ran SSH command: {command}')
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to run SSH command: {command}: {e}')
        raise


def create_remote_temp_directory(remote_user, remote_ip, remote_directory):
    """Create a directory on the remote machine."""
    command = f'mkdir -p {remote_directory}/temp'
    run_ssh_command(remote_user, remote_ip, command)


def delete_remote_temp_directory(remote_user, remote_ip, remote_directory):
    """Delete a directory on the remote machine."""
    command = f'rm -rf {remote_directory}/temp'
    run_ssh_command(remote_user, remote_ip, command)


def delete_remote_directory_contents(remote_user, remote_ip, remote_directory):
    """Delete the contents of a directory on the remote machine."""
    command = f'rm -rf {remote_directory}/*'
    run_ssh_command(remote_user, remote_ip, command)


def get_remote_home_directory(remote_user, remote_ip):
    """Get the home directory of the remote user."""
    home_dir_cmd = f'ssh {remote_user}@{remote_ip} "eval echo ~$USER"'
    try:
        result = subprocess.run(
            home_dir_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        remote_home_directory = result.stdout.strip()
        logging.info(
            f'Remote home directory for {remote_user} '
            f'is {remote_home_directory}'
        )
        return remote_home_directory
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to get remote home directory: {e}')
        raise


def compress_and_transfer_rosbag(
    remote_user,
    remote_ip,
    rosbag_path,
    remote_directory,
    cloud_upload_directory,
    mcap_path,
    max_upload_attempts,
):
    """Compress rosbag on remote machine and transfer it to cloud host."""
    remote_temp_directory = f'{remote_directory}/temp'
    relative_bag_path = os.path.relpath(rosbag_path, start=remote_directory)
    print(f'relative_bag_path: {relative_bag_path}')
    # Check available disk space before compression
    if not check_disk_space(
        remote_user, remote_ip, remote_temp_directory, rosbag_path
    ):
        logging.error(f'Insufficient disk space for compressing {rosbag_path}')
        return False

    # Compress the rosbag on the remote machine
    remote_compressed_path = os.path.join(
        remote_temp_directory, os.path.basename(rosbag_path)
    )
    compress_cmd = (
        f'{mcap_path} compress {rosbag_path} -o {remote_compressed_path}'
    )
    try:
        run_ssh_command(remote_user, remote_ip, compress_cmd)
        logging.info(
            f'Compressed rosbag {rosbag_path} to'
            f'{remote_compressed_path} on the remote machine.'
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f'Failed to compress rosbag {rosbag_path} on remote machine: {e}'
        )
        return False

    # Transfer the compressed file to the cloud host
    rsync_cmd = [
        'rsync',
        '-av',
        '--checksum',
        '--progress',
        '--stats',
        f'{remote_user}@{remote_ip}:{remote_compressed_path}',
        os.path.join(cloud_upload_directory, relative_bag_path),
    ]

    print(
        f'cloud_upload_directory:'
        f'{os.path.join(cloud_upload_directory, relative_bag_path),}'
    )
    success = False
    attempts = 0
    while attempts < max_upload_attempts:
        try:
            subprocess.run(rsync_cmd, check=True)
            logging.info(
                f'Transferred compressed rosbag {remote_compressed_path} '
                f'to the cloud host at {cloud_upload_directory}.'
            )
            success = True
            break
        except subprocess.CalledProcessError as e:
            attempts += 1
            logging.error(
                f'Failed to transfer compressed rosbag'
                f'{remote_compressed_path} '
                f'from remote machine: {e}. '
                f'Attempt {attempts} of {max_upload_attempts}. Retrying...'
            )

    if not success:
        logging.error(
            f'All {max_upload_attempts} attempts to'
            f' transfer compressed rosbag '
            f'{remote_compressed_path} from remote machine have failed.'
        )
        return False

    # Remove the compressed file from the remote machine
    remove_remote_file_cmd = f'rm {remote_compressed_path}'
    try:
        run_ssh_command(remote_user, remote_ip, remove_remote_file_cmd)
        logging.info(
            f'Removed compressed rosbag {remote_compressed_path}'
            f' from the remote machine.'
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f'Failed to remove compressed rosbag'
            f'{remote_compressed_path} from remote machine: {e}'
        )
        return False

    return True


def get_remote_rosbags_list(remote_user, remote_ip, remote_directory):
    """Get a list of all rosbags on the remote machine."""
    list_cmd = f"find {remote_directory} -name '*.mcap'"
    try:
        result = subprocess.run(
            f'ssh {remote_user}@{remote_ip} ' f"'{list_cmd}'",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        rosbag_list = result.stdout.splitlines()
        logging.info(
            f'Found {len(rosbag_list)} rosbags on the remote machine.'
        )
        return rosbag_list
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to list rosbags on remote machine: {e}')
        raise


def list_remote_directories(
    remote_user, remote_ip, base_remote_directory, depth
):
    """List directories on the remote machine up to a given depth."""
    list_cmd = (
        f'ssh {remote_user}@{remote_ip} '
        f'"find {base_remote_directory} -maxdepth {depth} -mindepth 1 -type d"'
    )
    try:
        result = subprocess.run(
            list_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        subdirectories = result.stdout.splitlines()
        logging.info(
            f'Listed directories up to depth {depth} in '
            f'{base_remote_directory}'
        )
        return subdirectories
    except subprocess.CalledProcessError as e:
        logging.error(
            f'Failed to list subdirectories in {base_remote_directory}: {e}'
        )
        return []


def get_remote_file_size(remote_user, remote_ip, file_path):
    """Get the size of a remote file."""
    size_cmd = f'stat -c%s {file_path}'
    try:
        result = subprocess.run(
            f'ssh {remote_user}@{remote_ip} ' f"'{size_cmd}'",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        file_size = int(result.stdout.strip())
        logging.info(f'Size of file {file_path} is {file_size} bytes.')
        return file_size / (1024**3)  # Convert to GB
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to get size of remote file {file_path}: {e}')
        raise


def get_estimated_compression_time(file_sizes):
    """Estimate compression time in hours based on file sizes."""
    compression_speed_mbps = 120  # Compression speed in MB/s for zstd level 2
    total_compression_time = sum(
        size / compression_speed_mbps / 3600 for size in file_sizes
    )  # Total compression time in hours
    return total_compression_time


def get_estimated_upload_time(total_size_gb, bandwidth_mbps, file_sizes):
    """Estimate upload time in hours, including compression time."""
    compression_time = get_estimated_compression_time(file_sizes)
    bandwidth_mbs = bandwidth_mbps / 8  # Convert Mbps to MB/s
    total_size_mb = total_size_gb * 1024  # Convert GB to MB
    upload_time = total_size_mb / bandwidth_mbs / 3600  # Upload time in hours
    return upload_time + compression_time  # Total time including compression


def measure_bandwidth(remote_ip, remote_user):
    """Measure bandwidth between cloud host and remote machine."""
    try:
        # Start iperf3 server on the remote machine
        server_cmd = f'ssh {remote_user}@{remote_ip} ' f"'iperf3 -s -D'"
        subprocess.run(server_cmd, shell=True, check=True)
        logging.info(f'Started iperf3 server on {remote_ip}')

        # Run iperf3 client on the cloud host
        result = subprocess.run(
            ['iperf3', '-c', remote_ip, '-f', 'm'],
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.split('\n'):
            if 'receiver' in line:
                bandwidth_mbps = float(line.split()[-3])
                return bandwidth_mbps
    except subprocess.CalledProcessError as e:
        logging.error(f'iperf3 error: {e}')
    finally:
        # Stop iperf3 server on the remote machine
        stop_server_cmd = f'ssh {remote_user}@{remote_ip} ' f"'pkill iperf3'"
        subprocess.run(stop_server_cmd, shell=True)
        logging.info(f'Stopped iperf3 server on {remote_ip}')
    return None


def check_disk_space(
    remote_user, remote_ip, directory, rosbag_path, retries=3, delay=5
):
    """Check if there's enough disk space on the remote machine."""
    for attempt in range(retries):
        try:
            # Get available disk space on the remote machine
            disk_usage_cmd = (
                f'ssh {remote_user}@{remote_ip} '
                f"\"stat -f --format='%a * %S' {directory} | bc\""
            )
            result = subprocess.run(
                disk_usage_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            available_space = int(result.stdout.strip())
            logging.info(f'Available space: {available_space} bytes.')

            # Get the file size of the rosbag on the remote machine
            file_size_cmd = (
                f"ssh {remote_user}@{remote_ip} 'stat -c%s {rosbag_path}'"
            )
            result = subprocess.run(
                file_size_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            file_size = int(result.stdout.strip())
            logging.info(f'File size: {file_size} bytes.')

            # Check if there is enough space
            if file_size <= available_space:
                return True
            else:
                logging.warning(
                    f'Insufficient disk space for {rosbag_path}. '
                    f'Attempting to free up space.'
                )
                # If no space, delete the oldest mcap file
                delete_oldest_mcap(remote_user, remote_ip, directory)

        except subprocess.CalledProcessError as e:
            logging.error(
                f'Attempt {attempt + 1} - '
                f'Failed to check disk space on remote machine: {e}'
            )

        if attempt < retries - 1:
            logging.info(f'Retrying in {delay} seconds...')
            time.sleep(delay)

    logging.error('Failed to check disk space after multiple attempts.')
    return False


def delete_oldest_mcap(remote_user, remote_ip, directory):
    """Delete the oldest mcap file in the directory."""
    list_cmd = (
        f"find {directory} -name '*.mcap' -type f -printf '%T+ %p\n' | sort | "
        "head -n 1 | cut -d' ' -f2-"
    )
    try:
        result = subprocess.run(
            f'ssh {remote_user}@{remote_ip} "{list_cmd}"',
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        oldest_file = result.stdout.strip()
        if oldest_file:
            delete_cmd = f'rm {oldest_file}'
            run_ssh_command(remote_user, remote_ip, delete_cmd)
            logging.info(f'Deleted oldest mcap file: {oldest_file}')
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to delete oldest mcap file: {e}')
        raise


def delete_remote_file(remote_user, remote_ip, file_path):
    """Delete a specific file on the remote machine."""
    command = f'rm {file_path}'
    run_ssh_command(remote_user, remote_ip, command)


def find_metadata_file(remote_user, remote_ip, remote_directory):
    """Find the metadata.yaml file on the remote machine."""
    find_cmd = f"find {remote_directory} -name 'metadata.yaml'"
    try:
        result = subprocess.run(
            f'ssh {remote_user}@{remote_ip} ' f"'{find_cmd}'",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        metadata_files = result.stdout.splitlines()
        if metadata_files:
            logging.info(f'Found metadata.yaml file at {metadata_files[0]}.')
            return metadata_files[0]
        else:
            logging.error('metadata.yaml file not found.')
            return None
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to find metadata.yaml file: {e}')
        raise


def copy_metadata_file(
    remote_user,
    remote_ip,
    metadata_path,
    cloud_upload_directory,
    remote_directory,
):
    """Copy metadata.yaml file from remote machine to cloud host."""
    relative_metadata_path = os.path.relpath(
        metadata_path, start=remote_directory
    )
    print(f'metadata_path: {metadata_path}')
    print(f'relative_metadata_path: {relative_metadata_path}')
    rsync_cmd = [
        'rsync',
        '-av',
        '--checksum',
        '--progress',
        '--stats',
        f'{remote_user}@{remote_ip}:{metadata_path}',
        os.path.join(cloud_upload_directory, relative_metadata_path),
    ]
    try:
        subprocess.run(rsync_cmd, check=True)
        logging.info(f'Copied metadata.yaml to {cloud_upload_directory}.')
        print(
            f'Copied metadata.yaml to '
            f' {os.path.join(cloud_upload_directory, relative_metadata_path)}.'
        )
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to copy metadata.yaml: {e}')
        return False


def read_metadata(metadata_path):
    """Read the metadata.yaml file and return its contents."""
    with open(metadata_path) as file:
        metadata = yaml.safe_load(file)
    return metadata


def check_and_create_local_directory(directory_path):
    """Check if a directory exists on the local machine and create it."""
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            logging.info(f'Created local directory: {directory_path}')
        except OSError as e:
            logging.error(f'Failed to create directory {directory_path}: {e}')
            raise


def process_directory(
    remote_user,
    remote_ip,
    remote_directory,
    cloud_upload_directory,
    config,
    base_remote_directory,
):
    """Process each directory."""
    # Create the remote temporary directory
    create_remote_temp_directory(remote_user, remote_ip, remote_directory)

    try:
        # Find and copy the metadata.yaml file
        metadata_path = find_metadata_file(
            remote_user, remote_ip, remote_directory
        )
        if metadata_path is None:
            print(
                'metadata.yaml file not found in {remote_directory}. Skipping.'
            )
        relative_metadata_path = os.path.relpath(
            metadata_path, start=base_remote_directory
        )

        local_metadata_path = os.path.join(
            cloud_upload_directory, relative_metadata_path
        )

        check_and_create_local_directory(os.path.dirname(local_metadata_path))

        if not copy_metadata_file(
            remote_user,
            remote_ip,
            metadata_path,
            cloud_upload_directory,
            base_remote_directory,
        ):
            print(
                'Failed to copy metadata.yaml from '
                '{remote_directory}. '
                'Skipping.'
            )

        # Read the metadata.yaml file

        metadata = read_metadata(local_metadata_path)
        expected_bags = metadata.get('rosbag2_bagfile_information', {}).get(
            'relative_file_paths', None
        )
        print(f'Expected bags: {expected_bags}')
        print(f'len(expected_bags): {len(expected_bags)}')

        # Get the list of rosbags from the remote machine
        rosbag_list = get_remote_rosbags_list(
            remote_user, remote_ip, remote_directory
        )

        if len(rosbag_list) != len(expected_bags):
            print(
                'The number of rosbags does not match the metadata. Skipping.'
            )
            logging.error('The number of rosbags does not match the metadata.')
            return

        bandwidth_mbps = measure_bandwidth(remote_ip, remote_user)
        if bandwidth_mbps is None:
            print('Could not measure bandwidth. Skipping.')
            return

        rosbag_sizes = [
            get_remote_file_size(remote_user, remote_ip, rosbag)
            for rosbag in rosbag_list
        ]
        total_size_gb = sum(rosbag_sizes)
        estimated_time = get_estimated_upload_time(
            total_size_gb, bandwidth_mbps, rosbag_sizes
        )

        if estimated_time < 1:
            estimated_time_str = f'{estimated_time * 60:.2f} minutes'
        else:
            estimated_time_str = f'{estimated_time:.2f} hours'

        print(
            f'Found {len(rosbag_list)} files to upload with total size '
            f'{total_size_gb:.2f} GB.'
        )
        print(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')
        print(
            f'Estimated total time (including compression) is at least: '
            f'{estimated_time_str}.'
        )

        logging.info(
            f'Starting upload of {len(rosbag_list)} files with total size '
            f'{total_size_gb:.2f} GB from {remote_directory}.'
        )
        logging.info(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')

        successfully_uploaded_files = []

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(
            max_workers=config['parallel_processes']
        ) as executor:
            futures = [
                executor.submit(
                    compress_and_transfer_rosbag,
                    remote_user,
                    remote_ip,
                    rosbag,
                    remote_directory,
                    cloud_upload_directory,
                    config['mcap_path'],
                    config['upload_attempts'],
                )
                for rosbag in rosbag_list
            ]

            for future in futures:
                result = future.result()
                if result:
                    successfully_uploaded_files.append(result)

        if config['clean_up']:
            # Only delete rosbag files that were successfully uploaded
            for rosbag in successfully_uploaded_files:
                delete_remote_file(remote_user, remote_ip, rosbag)

    finally:
        # Delete the remote temporary directory
        delete_remote_temp_directory(remote_user, remote_ip, remote_directory)


def main(config, debug):
    """Automate the upload of rosbags."""
    remote_user = config['remote_user']
    remote_ip = config['remote_ip']
    base_remote_directory = config['remote_directory']
    cloud_upload_directory = config['cloud_upload_directory']
    directory_depth = config.get(
        'directory_depth', 1
    )  # Default to 1 for flat structure

    # Get all subdirectories in the base remote directory
    subdirectories = list_remote_directories(
        remote_user, remote_ip, base_remote_directory, directory_depth
    )

    total_rosbags = 0
    total_size_gb = 0.0
    total_estimated_time = 0.0
    bandwidth_mbps = measure_bandwidth(remote_ip, remote_user)

    if bandwidth_mbps is None:
        print('Could not measure bandwidth. Exiting.')
        return

    # Compute total estimated time for all subdirectories
    for subdirectory in subdirectories:
        # Get the list of rosbags from the remote machine
        rosbag_list = get_remote_rosbags_list(
            remote_user, remote_ip, subdirectory
        )

        # Get the size of each rosbag
        rosbag_sizes = [
            get_remote_file_size(remote_user, remote_ip, rosbag)
            for rosbag in rosbag_list
        ]
        total_rosbags += len(rosbag_list)
        total_size_gb += sum(rosbag_sizes)
        total_estimated_time += get_estimated_upload_time(
            sum(rosbag_sizes), bandwidth_mbps, rosbag_sizes
        )

    if total_estimated_time < 1:
        estimated_time_str = f'{total_estimated_time * 60:.2f} minutes'
    else:
        estimated_time_str = f'{total_estimated_time:.2f} hours'

    if debug:
        print(
            f'Found {total_rosbags} files to upload with total size '
            f'{total_size_gb:.2f} GB.'
        )
        print(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')
        print(
            f'Estimated total time (including compression) for '
            f'all subdirectories is at least: '
            f'{estimated_time_str}'
        )
        for bag_path in subdirectories:
            print(f'Subdirectories to be processed: {bag_path}')
        print()

    confirm = input('Do you want to proceed to upload? (yes/no): ')

    if confirm.lower() != 'yes':
        print('Upload aborted.')
        return

    logging.info(
        f'Starting upload of {total_rosbags} files with total size '
        f'{total_size_gb:.2f} GB.'
    )
    logging.info(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')

    # Process each subdirectory
    for subdirectory in subdirectories:
        process_directory(
            remote_user,
            remote_ip,
            subdirectory,
            cloud_upload_directory,
            config,
            base_remote_directory,
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Automate the compression and upload of rosbags.'
    )
    parser.add_argument(
        '-c',
        '--config',
        default='vehicle_data_params.yaml',
        help='Path to the YAML configuration file',
    )
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Enable debugging prints',
    )
    args = parser.parse_args()

    with open(args.config) as file:
        config = yaml.safe_load(file)

    main(config, args.debug)
