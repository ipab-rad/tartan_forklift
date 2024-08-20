"""This script automates the upload of rosbags from vehicle to cloud host."""

import argparse
import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import colorlog

import yaml


def setup_logging(debug_mode):
    """Configure logging with color support."""
    # Timestamp for the log file name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'upload_vehicle_data_{timestamp}.log'

    # Create a logger
    logger = logging.getLogger('rosbag_upload')
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    logger.propagate = False

    # Create a console handler (StreamHandler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    # Create a file handler (FileHandler)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    # Create a colored formatter for the console handler
    color_formatter = colorlog.ColoredFormatter(
        '%(asctime)s - %(log_color)s%(levelname)s%(reset)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    console_handler.setFormatter(color_formatter)

    # Create a regular formatter for the file handler
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    file_handler.setFormatter(file_formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def run_ssh_command(logger, remote_user, remote_ip, command):
    """Run a command on the remote machine using SSH."""
    ssh_command = f"ssh {remote_user}@{remote_ip} '{command}'"
    try:
        subprocess.run(ssh_command, shell=True, check=True)
        logger.debug(f'Ran SSH command: {command}')
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to run SSH command: {command}: {e}')
        raise


def create_remote_temp_directory(
    logger, remote_user, remote_ip, remote_directory
):
    """Create a directory on the remote machine."""
    command = f'mkdir -p {remote_directory}/temp'
    run_ssh_command(logger, remote_user, remote_ip, command)
    logger.debug(
        f'Created remote temporary directory: {remote_directory}/temp'
    )


def delete_remote_temp_directory(
    logger, remote_user, remote_ip, remote_directory
):
    """Delete a directory on the remote machine."""
    command = f'rm -rf {remote_directory}/temp'
    run_ssh_command(logger, remote_user, remote_ip, command)
    logger.debug(
        f'Deleted remote temporary directory: {remote_directory}/temp'
    )


def delete_remote_directory_contents(
    logger, remote_user, remote_ip, remote_directory
):
    """Delete the contents of a directory on the remote machine."""
    command = f'rm -rf {remote_directory}/*'
    run_ssh_command(logger, remote_user, remote_ip, command)
    logger.debug(f'Deleted contents of remote directory: {remote_directory}')


def get_remote_home_directory(logger, remote_user, remote_ip):
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
        logger.info(
            f'Remote home directory for {remote_user} '
            f'is {remote_home_directory}'
        )
        return remote_home_directory
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to get remote home directory: {e}')
        raise


def compress_and_transfer_rosbag(
    logger,
    remote_user,
    remote_ip,
    rosbag_path,
    remote_directory,
    cloud_upload_directory,
    mcap_path,
    max_upload_attempts,
    base_remote_directory,
    current_rosbag_number,
    total_rosbags,
    global_rosbag_counter,
):
    """Compress rosbag on remote machine and transfer it to cloud host."""
    remote_temp_directory = f'{remote_directory}/temp'
    relative_bag_path = os.path.relpath(
        rosbag_path, start=base_remote_directory
    )
    # Check available disk space before compression

    if not check_disk_space(
        logger, remote_user, remote_ip, remote_temp_directory, rosbag_path
    ):
        logger.error(f'Insufficient disk space for compressing {rosbag_path}')
        return False

    logger.info(
        f'Enough space found on the remote machine. Start '
        f'compressing: \n{rosbag_path}'
    )

    remote_compressed_path = os.path.join(
        remote_temp_directory, os.path.basename(rosbag_path)
    )
    compress_cmd = (
        f'{mcap_path} compress {rosbag_path} -o {remote_compressed_path}'
    )
    try:
        start_time = time.time()
        run_ssh_command(logger, remote_user, remote_ip, compress_cmd)
        duration = time.time() - start_time

        logger.info(f'Rosbag compressed in  {duration:.2f} seconds')

        logger.debug(
            f'Compressed version temporarily stored in '
            f'{remote_compressed_path}'
        )
    except subprocess.CalledProcessError as e:
        logger.error(
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

    success = False
    attempts = 0
    while attempts < max_upload_attempts:
        try:
            logger.info(
                f'Uploading rosbag {global_rosbag_counter}/{total_rosbags} ...'
            )
            start_time = time.time()
            subprocess.run(rsync_cmd, check=True)
            duration = time.time() - start_time

            logger.info(f'Rosbag uploaded in {duration:.2f} seconds')

            logger.debug(
                f'Compressed rosbag {remote_compressed_path} '
                f'uploaded to {cloud_upload_directory}.'
            )
            success = True
            break
        except subprocess.CalledProcessError as e:
            attempts += 1
            logger.warning(
                f'Failed to transfer compressed rosbag'
                f'{remote_compressed_path} '
                f'from remote machine: {e}. '
                f'Attempt {attempts} of {max_upload_attempts}. Retrying...'
            )

    if not success:
        logger.error(
            f'All {max_upload_attempts} attempts to'
            f' transfer compressed rosbag '
            f'{remote_compressed_path} from remote machine have failed.'
        )
        return False

    # Remove the compressed file from the remote machine
    remove_remote_file_cmd = f'rm {remote_compressed_path}'
    try:
        run_ssh_command(logger, remote_user, remote_ip, remove_remote_file_cmd)
        logger.debug(
            f'Removed compressed rosbag {remote_compressed_path}'
            f' from the remote machine.'
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            f'Failed to remove compressed rosbag'
            f'{remote_compressed_path} from remote machine: {e}'
        )
        return False

    return True


def get_remote_rosbags_list(logger, remote_user, remote_ip, remote_directory):
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
        logger.info(f'Found {len(rosbag_list)} rosbags in the subdirectory.')
        return rosbag_list
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to list rosbags on remote machine: {e}')
        raise


def list_remote_directories(
    logger, remote_user, remote_ip, base_remote_directory
):
    """List directories on the remote machine containing .mcap files."""
    list_cmd = (
        f'ssh {remote_user}@{remote_ip} '
        f'"find {base_remote_directory} -type f -name \\"*.mcap\\" '
        f'-printf \'%h\\n\' | sort -u"'
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
        logger.info(
            f'Listed directories containing .mcap '
            f'files in {base_remote_directory}'
        )
        return subdirectories
    except subprocess.CalledProcessError as e:
        logger.error(
            f'Failed to list directories in {base_remote_directory}: {e}'
        )
        return []


def get_remote_file_size(logger, remote_user, remote_ip, file_path):
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
        logger.info(f'Size of file {file_path} is {file_size} bytes.')
        return file_size  # Return size in bytes for further calculations
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to get size of remote file {file_path}: {e}')
        raise


def get_estimated_compression_time(file_sizes):
    """Estimate compression time in hours based on file sizes."""
    compression_speed_mbps = 120  # Compression speed in MB/s for zstd level 2
    total_compression_time = sum(
        (size / (1024**2)) / compression_speed_mbps for size in file_sizes
    )  # Total compression time in seconds
    total_compression_time_hours = (
        total_compression_time / 3600
    )  # Convert to hours
    return total_compression_time_hours


def get_estimated_upload_time(total_size_bytes, bandwidth_mbps, file_sizes):
    """Estimate upload time in hours, including compression time."""
    compression_time = get_estimated_compression_time(file_sizes)
    bandwidth_mbs = bandwidth_mbps / 8  # Convert Mbps to MB/s
    total_size_mb = total_size_bytes / (1024**2)  # Convert bytes to MB
    upload_time = total_size_mb / bandwidth_mbs  # Upload time in seconds
    return (
        upload_time / 3600
    ) + compression_time  # Total time in hours including compression


def measure_bandwidth(logger, remote_ip, remote_user):
    """Measure bandwidth between cloud host and remote machine."""
    logger.info(f'Starting bandwidth measurement for {remote_ip}...')
    try:
        # Start iperf3 server on the remote machine
        server_cmd = f'ssh {remote_user}@{remote_ip} ' f"'iperf3 -s -D'"
        subprocess.run(server_cmd, shell=True, check=True)
        logger.debug(f'Started iperf3 server on {remote_ip}')

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
                logger.info(f'Measured bandwidth: {bandwidth_mbps:.2f} Mbps')
                return bandwidth_mbps
    except subprocess.CalledProcessError as e:
        logger.error(f'iperf3 error: {e}')
    finally:
        # Stop iperf3 server on the remote machine
        stop_server_cmd = f'ssh {remote_user}@{remote_ip} ' f"'pkill iperf3'"
        subprocess.run(stop_server_cmd, shell=True)
        logger.debug(f'Stopped iperf3 server on {remote_ip}')
    logger.warning(f'Failed to measure bandwidth for {remote_ip}.')
    return None


def check_disk_space(
    logger, remote_user, remote_ip, directory, rosbag_path, retries=3, delay=5
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
            logger.debug(f'Available space: {available_space} bytes.')

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
            logger.debug(f'File size: {file_size} bytes.')

            # Check if there is enough space
            if file_size <= available_space:
                return True
            else:
                logger.warning(
                    f'Insufficient disk space for {rosbag_path}. '
                    f'Attempting to free up space.'
                )
                # If no space, delete the oldest mcap file
                delete_oldest_mcap(logger, remote_user, remote_ip, directory)

        except subprocess.CalledProcessError as e:
            logger.error(
                f'Attempt {attempt + 1} - '
                f'Failed to check disk space on remote machine: {e}'
            )

        if attempt < retries - 1:
            logger.info(f'Retrying in {delay} seconds...')
            time.sleep(delay)

    logger.error('Failed to check disk space after multiple attempts.')
    return False


def delete_oldest_mcap(logger, remote_user, remote_ip, directory):
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
            run_ssh_command(logger, remote_user, remote_ip, delete_cmd)
            logger.info(f'Deleted oldest mcap file: {oldest_file}')
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to delete oldest mcap file: {e}')
        raise


def delete_remote_file(logger, remote_user, remote_ip, file_path):
    """Delete a specific file on the remote machine."""
    command = f'rm {file_path}'
    run_ssh_command(logger, remote_user, remote_ip, command)
    logger.debug(f'Deleted file: {file_path}')


def find_metadata_file(logger, remote_user, remote_ip, remote_directory):
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
            logger.info(f'Found metadata.yaml file at {metadata_files[0]}.')
            return metadata_files[0]
        else:
            logger.error('metadata.yaml file not found.')
            return None
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to find metadata.yaml file: {e}')
        raise


def copy_metadata_file(
    logger,
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
        logger.info(f'Uploading {relative_metadata_path}.')
        subprocess.run(rsync_cmd, check=True)
        logger.debug(f'Copied metadata.yaml to {cloud_upload_directory}.')
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to copy metadata.yaml: {e}')
        return False


def read_metadata(logger, metadata_path):
    """Read the metadata.yaml file and return its contents."""
    with open(metadata_path) as file:
        metadata = yaml.safe_load(file)
    logger.debug(f'Read metadata from {metadata_path}.')
    return metadata


def check_and_create_local_directory(logger, directory_path):
    """Check if a directory exists on the local machine and create it."""
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            logger.debug(f'Created local directory: {directory_path}')
        except OSError as e:
            logger.error(f'Failed to create directory {directory_path}: {e}')
            raise


def process_directory(
    logger,
    remote_user,
    remote_ip,
    remote_directory,
    cloud_upload_directory,
    config,
    base_remote_directory,
    bandwidth_mbps,
    file_sizes_dict,
    files_dict,
    global_rosbag_counter,
    total_rosbags,
):
    """Process each directory."""
    logger.info('')
    logger.info(f'Processing: {remote_directory}')
    # Create the remote temporary directory
    create_remote_temp_directory(
        logger, remote_user, remote_ip, remote_directory
    )
    successfully_uploaded_files = []
    total_files = 0

    try:
        # Find and copy the metadata.yaml file
        metadata_path = find_metadata_file(
            logger, remote_user, remote_ip, remote_directory
        )
        if metadata_path is None:
            logger.warning(
                f'metadata.yaml file not found in {remote_directory}. '
                f'Skipping.'
            )
        relative_metadata_path = os.path.relpath(
            metadata_path, start=base_remote_directory
        )

        local_metadata_path = os.path.join(
            cloud_upload_directory, relative_metadata_path
        )

        check_and_create_local_directory(
            logger, os.path.dirname(local_metadata_path)
        )

        if not copy_metadata_file(
            logger,
            remote_user,
            remote_ip,
            metadata_path,
            cloud_upload_directory,
            base_remote_directory,
        ):
            logger.error(
                f'Failed to copy metadata.yaml from '
                f'{remote_directory}. Skipping file.'
            )

        # Read the metadata.yaml file

        metadata = read_metadata(logger, local_metadata_path)
        expected_bags = metadata.get('rosbag2_bagfile_information', {}).get(
            'relative_file_paths', None
        )

        # Retrieve the list of rosbags contained in the remote subdirectory
        rosbag_list = files_dict.get(remote_directory)
        if rosbag_list is None:
            rosbag_list = get_remote_rosbags_list(
                logger, remote_user, remote_ip, remote_directory
            )
            files_dict[remote_directory] = rosbag_list

        if len(rosbag_list) != len(expected_bags):
            logger.error(
                'The number of rosbags does not match the metadata. Skipping.'
            )
            return {
                'uploaded_files': len(successfully_uploaded_files),
                'total_files': total_files,
                'global_rosbag_counter': global_rosbag_counter,
            }

        rosbag_sizes = file_sizes_dict[remote_directory]
        total_size_bytes = sum(rosbag_sizes)
        total_files = len(rosbag_list)
        estimated_time = get_estimated_upload_time(
            total_size_bytes, bandwidth_mbps, rosbag_sizes
        )

        # Convert estimated_time (in hours) to seconds
        total_seconds = int(estimated_time * 3600)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if hours > 0:
            estimated_time_str = (
                f'{hours} hours {minutes} minutes {seconds} seconds'
            )
        elif minutes > 0:
            estimated_time_str = f'{minutes} minutes {seconds} seconds'
        else:
            estimated_time_str = f'{seconds} seconds'

        logger.info(
            f'Found {len(rosbag_list)} files to upload with total size '
            f'{total_size_bytes / (1024**3):.2f} GB from {remote_directory}.'
        )
        logger.info(
            f'Estimated rosbags upload time (including compression) '
            f'is at least: {estimated_time_str}.'
        )

        successfully_uploaded_files = []

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(
            max_workers=config['parallel_processes']
        ) as executor:
            futures = [
                executor.submit(
                    compress_and_transfer_rosbag,
                    logger,
                    remote_user,
                    remote_ip,
                    rosbag,
                    remote_directory,
                    cloud_upload_directory,
                    config['mcap_path'],
                    config['upload_attempts'],
                    base_remote_directory,
                    current_rosbag_number=i + 1,
                    global_rosbag_counter=global_rosbag_counter + i + 1,
                    total_rosbags=total_rosbags,
                )
                for i, rosbag in enumerate(rosbag_list)
            ]

            for future in futures:
                result = future.result()
                if result:
                    successfully_uploaded_files.append(result)
        global_rosbag_counter += len(successfully_uploaded_files)

        if config['clean_up']:
            # Only delete rosbag files that were successfully uploaded
            for rosbag in successfully_uploaded_files:
                delete_remote_file(logger, remote_user, remote_ip, rosbag)

    finally:
        # Delete the remote temporary directory
        delete_remote_temp_directory(
            logger, remote_user, remote_ip, remote_directory
        )
    return {
        'uploaded_files': len(successfully_uploaded_files),
        'total_files': total_files,
        'global_rosbag_counter': global_rosbag_counter,
    }


def main(config, debug):
    """Automate the upload of rosbags."""
    logger = setup_logging(debug)
    remote_user = config['remote_user']
    remote_ip = config['remote_ip']
    base_remote_directory = config['remote_directory']
    cloud_upload_directory = config['cloud_upload_directory']
    logger.info('Starting rosbag upload process.')

    # Measure bandwidth once at the start
    bandwidth_mbps = measure_bandwidth(logger, remote_ip, remote_user)
    if bandwidth_mbps is None:
        logger.error('Could not measure bandwidth. Exiting.')
        return
    #  Retrieve all subdirectories containing rosbags in the remotes directory
    subdirectories = list_remote_directories(
        logger, remote_user, remote_ip, base_remote_directory
    )
    logger.info(f'Rosbags subdirectories found: {len(subdirectories)}')
    total_rosbags = 0
    total_size_bytes = 0.0
    total_estimated_time = 0.0
    file_sizes_dict = {}
    files_dict = {}

    # Compute total estimated time for all subdirectories
    for subdirectory in subdirectories:
        # Retrieve the list of rosbags contained in the remote subdirectory
        rosbag_list = get_remote_rosbags_list(
            logger, remote_user, remote_ip, subdirectory
        )
        files_dict[subdirectory] = rosbag_list

        # Get the size of each rosbag
        rosbag_sizes = [
            get_remote_file_size(logger, remote_user, remote_ip, rosbag)
            for rosbag in rosbag_list
        ]
        file_sizes_dict[subdirectory] = rosbag_sizes
        total_rosbags += len(rosbag_list)
        total_size_bytes += sum(rosbag_sizes)
        total_estimated_time += get_estimated_upload_time(
            sum(rosbag_sizes), bandwidth_mbps, rosbag_sizes
        )

    total_estimated_seconds = int(total_estimated_time * 3600)
    hours = total_estimated_seconds // 3600
    minutes = (total_estimated_seconds % 3600) // 60
    seconds = total_estimated_seconds % 60

    if hours > 0:
        estimated_time_str = (
            f'{hours} hours {minutes} minutes {seconds} seconds'
        )
    elif minutes > 0:
        estimated_time_str = f'{minutes} minutes {seconds} seconds'
    else:
        estimated_time_str = f'{seconds} seconds'

    logger.info(
        f'Found {total_rosbags} rosbags (mcap) files to upload '
        f'with total size {total_size_bytes / (1024**3):.2f} GB.'
    )
    logger.info(
        f'Estimated total time (including compression) for '
        f'all subdirectories is: {estimated_time_str}.'
    )
    confirm = input('Do you want to proceed to upload? (yes/no): ')

    if confirm.lower() != 'yes':
        logger.info('Upload aborted by user.')
        return

    logger.info('User confirmed upload. Beginning processing of directories.')

    total_uploaded_files = (
        0  # Initialize counter for successfully uploaded files
    )
    total_files = 0  # Initialize counter for total files
    global_rosbag_counter = 0

    # Process each subdirectory
    for subdirectory in subdirectories:
        result = process_directory(
            logger,
            remote_user,
            remote_ip,
            subdirectory,
            cloud_upload_directory,
            config,
            base_remote_directory,
            bandwidth_mbps,
            file_sizes_dict,
            files_dict,
            global_rosbag_counter,
            total_rosbags,
        )
        total_uploaded_files += result.get(
            'uploaded_files', 0
        )  # Update the count of uploaded files
        total_files += result.get(
            'total_files', 0
        )  # Update the total file count

        global_rosbag_counter = result.get(
            'global_rosbag_counter', global_rosbag_counter
        )  # Update the global rosbag counter

    # Final log statement after processing all subdirectories
    logger.info(
        f'Uploading finished. {total_uploaded_files}/{total_files} '
        f'files were successfully uploaded.'
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
