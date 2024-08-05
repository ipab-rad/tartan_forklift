"""This script automates the upload of rosbags from vehicle to cloud host."""

import argparse
import logging
import os
import subprocess
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


def create_remote_directory(remote_user, remote_ip, remote_directory):
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
        '-avz',
        '--checksum',
        f'{remote_user}@{remote_ip}:{remote_compressed_path}',
        cloud_upload_directory,
    ]
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
    compression_speed_mbps = 400  # Compression speed in MB/s for zstd level 2
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


def check_disk_space(remote_user, remote_ip, directory, rosbag_path):
    """Check if there's enough disk space on the remote machine."""
    # Get available disk space on the remote machine
    disk_usage_cmd = (
        f'ssh {remote_user}@{remote_ip} '
        f"\"stat -f --format='%a * %S' {directory} | bc\""
    )
    try:
        result = subprocess.run(
            disk_usage_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        available_space = int(result.stdout.strip())

    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to get disk space on remote machine: {e}')
        raise

    # Get the file size of the rosbag on the remote machine
    file_size_cmd = f"ssh {remote_user}@{remote_ip} 'stat -c%s {rosbag_path}'"
    try:
        result = subprocess.run(
            file_size_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        file_size = int(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to get file size on remote machine: {e}')
        raise

    # Check if there is enough space
    if file_size > available_space:
        # If no space, delete the oldest mcap file
        delete_oldest_mcap(remote_user, remote_ip, directory)
        try:
            result = subprocess.run(
                disk_usage_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            available_space = int(result.stdout.strip())  # Space in bytes

        except subprocess.CalledProcessError as e:
            logging.error(
                f'Failed to get disk space on remote machine '
                f'after deletion: {e}'
            )
            raise

    return file_size <= available_space


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
    remote_user, remote_ip, metadata_path, cloud_upload_directory
):
    """Copy metadata.yaml file from remote machine to cloud host."""
    rsync_cmd = [
        'rsync',
        '-avz',
        '--checksum',
        f'{remote_user}@{remote_ip}:{metadata_path}',
        cloud_upload_directory,
    ]
    try:
        subprocess.run(rsync_cmd, check=True)
        logging.info(f'Copied metadata.yaml to {cloud_upload_directory}.')
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f'Failed to copy metadata.yaml: {e}')
        return False


def read_metadata(metadata_path):
    """Read the metadata.yaml file and return its contents."""
    with open(metadata_path) as file:
        metadata = yaml.safe_load(file)
    return metadata


def main(config):
    """Automate the upload of rosbags."""
    remote_user = config['remote_user']
    remote_ip = config['remote_ip']
    remote_directory = config['remote_directory']
    cloud_upload_directory = config['cloud_upload_directory']
    clean_up = config['clean_up']
    max_upload_attempts = config.get(
        'upload_attempts', 3
    )  # Default to 3 if not specified
    mcap_path = config.get(
        'mcap_path', 'mcap'
    )  # Default to 'mcap' if not specified
    parallel_processes = config.get(
        'parallel_processes', 1
    )  # Default to 1 if not specified

    # Create the remote temporary directory
    create_remote_directory(remote_user, remote_ip, remote_directory)

    try:
        # Find and copy the metadata.yaml file
        metadata_path = find_metadata_file(
            remote_user, remote_ip, remote_directory
        )
        if metadata_path is None:
            print('metadata.yaml file not found. Exiting.')
            return
        if not copy_metadata_file(
            remote_user, remote_ip, metadata_path, cloud_upload_directory
        ):
            print('Failed to copy metadata.yaml. Exiting.')
            return

        # Read the metadata.yaml file
        local_metadata_path = os.path.join(
            cloud_upload_directory, 'metadata.yaml'
        )
        metadata = read_metadata(local_metadata_path)
        expected_bags = metadata.get('expected_bags', [])

        # Get the list of rosbags from the remote machine
        rosbag_list = get_remote_rosbags_list(
            remote_user, remote_ip, remote_directory
        )

        if len(rosbag_list) != len(expected_bags):
            print(
                'The number of rosbags does not match the metadata. Exiting.'
            )
            logging.error('The number of rosbags does not match the metadata.')
            return

        bandwidth_mbps = measure_bandwidth(remote_ip, remote_user)
        if bandwidth_mbps is None:
            print('Could not measure bandwidth. Exiting.')
            return

        rosbag_sizes = [
            get_remote_file_size(remote_user, remote_ip, rosbag)
            for rosbag in rosbag_list
        ]
        total_size_gb = sum(rosbag_sizes)
        estimated_time = get_estimated_upload_time(
            total_size_gb, bandwidth_mbps, rosbag_sizes
        )

        print(
            f'Found {len(rosbag_list)} files to upload with total size '
            f'{total_size_gb:.2f} GB.'
        )
        print(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')
        print(
            f'Estimated total time (including compression) is at least: '
            f'{estimated_time:.2f} hours.'
        )
        confirm = input('Do you want to proceed to upload? (yes/no): ')

        if confirm.lower() != 'yes':
            print('Upload aborted.')
            return

        logging.info(
            f'Starting upload of {len(rosbag_list)} files with total size '
            f'{total_size_gb:.2f} GB.'
        )
        logging.info(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')

        successfully_uploaded_files = []

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=parallel_processes) as executor:
            futures = [
                executor.submit(
                    compress_and_transfer_rosbag,
                    remote_user,
                    remote_ip,
                    rosbag,
                    remote_directory,
                    cloud_upload_directory,
                    mcap_path,
                    max_upload_attempts,
                )
                for rosbag in rosbag_list
            ]

            for future in futures:
                result = future.result()
                if result:
                    successfully_uploaded_files.append(result)

        if clean_up:
            # Only delete rosbag files that were successfully uploaded
            for rosbag in successfully_uploaded_files:
                delete_remote_file(remote_user, remote_ip, rosbag)

    finally:
        # Delete the remote temporary directory
        delete_remote_temp_directory(remote_user, remote_ip, remote_directory)


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
    args = parser.parse_args()

    with open(args.config) as file:
        config = yaml.safe_load(file)

    main(config)
