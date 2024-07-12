import subprocess
import logging
import os
from pathlib import Path
import argparse

# Setup logging
logging.basicConfig(
    filename='upload_vehicle_data.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
)


def run_ssh_command(remote_user, remote_ip, password, command):
    """Run a command on the remote machine using SSH"""
    ssh_command = (
        f"sshpass -p {password} ssh {remote_user}@{remote_ip} '{command}'"
    )
    try:
        subprocess.run(ssh_command, shell=True, check=True)
        logging.info(f"Ran SSH command: {command}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to run SSH command: {command}: {e}")
        raise


def create_remote_directory(
    remote_user, remote_ip, remote_temp_directory, password
):
    """Create a directory on the remote machine"""
    command = f"mkdir -p {remote_temp_directory}"
    run_ssh_command(remote_user, remote_ip, password, command)


def delete_remote_directory(
    remote_user, remote_ip, remote_temp_directory, password
):
    """Delete a directory on the remote machine"""
    command = f"rm -rf {remote_temp_directory}"
    run_ssh_command(remote_user, remote_ip, password, command)


def compress_and_transfer_rosbag(
    remote_user,
    remote_ip,
    rosbag_path,
    remote_temp_directory,
    vdi_upload_directory,
    password,
):
    """Compress a single rosbag on the remote machine and transfer it to the VDI machine"""
    # Compress the rosbag on the remote machine
    remote_compressed_path = (
        os.path.join(remote_temp_directory, os.path.basename(rosbag_path))
        + ".zst"
    )
    compress_cmd = f"mcap compress -o {rosbag_path} {remote_compressed_path}"
    try:
        run_ssh_command(remote_user, remote_ip, password, compress_cmd)
        logging.info(
            f"Compressed rosbag {rosbag_path} to {remote_compressed_path} on the remote machine."
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Failed to compress rosbag {rosbag_path} on remote machine: {e}"
        )
        return False

    # Transfer the compressed file to the VDI machine
    rsync_cmd = [
        "sshpass",
        "-p",
        password,
        "rsync",
        "-avz",
        f"{remote_user}@{remote_ip}:{remote_compressed_path}",
        vdi_upload_directory,
    ]
    try:
        subprocess.run(rsync_cmd, check=True)
        logging.info(
            f"Transferred compressed rosbag {remote_compressed_path} to the VDI machine at {vdi_upload_directory}."
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Failed to transfer compressed rosbag {remote_compressed_path} from remote machine: {e}"
        )
        return False

    # Remove the compressed file from the remote machine
    remove_remote_file_cmd = f"rm {remote_compressed_path}"
    try:
        run_ssh_command(
            remote_user, remote_ip, password, remove_remote_file_cmd
        )
        logging.info(
            f'Removed compressed rosbag {remote_compressed_path} from the remote machine.'
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Failed to remove compressed rosbag {remote_compressed_path} from remote machine: {e}"
        )
        return False

    return True


def get_remote_rosbags_list(
    remote_user, remote_ip, remote_directory, password
):
    """Get a list of all rosbags on the remote machine"""
    list_cmd = f"find {remote_directory} -name '*.mcap'"
    try:
        result = subprocess.run(
            f"sshpass -p {password} ssh {remote_user}@{remote_ip} '{list_cmd}'",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        rosbag_list = result.stdout.splitlines()
        logging.info(
            f"Found {len(rosbag_list)} rosbags on the remote machine."
        )
        return rosbag_list
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list rosbags on remote machine: {e}")
        raise


def get_remote_file_size(remote_user, remote_ip, file_path, password):
    """Get the size of a remote file"""
    size_cmd = f"stat -c%s {file_path}"
    try:
        result = subprocess.run(
            f"sshpass -p {password} ssh {remote_user}@{remote_ip} '{size_cmd}'",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        file_size = int(result.stdout.strip())
        logging.info(f"Size of file {file_path} is {file_size} bytes.")
        return file_size / (1024**3)  # Convert to GB
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get size of remote file {file_path}: {e}")
        raise


def get_estimated_upload_time(total_size_gb, bandwidth_mbps):
    """Estimate upload time in hours"""
    bandwidth_mbs = bandwidth_mbps / 8  # Convert Mbps to MB/s
    total_size_mb = total_size_gb * 1024  # Convert GB to MB
    return total_size_mb / bandwidth_mbs / 3600  # Return time in hours


def measure_bandwidth(remote_ip, remote_user, password):
    """Measure the actual bandwidth using iperf3 between the VDI machine and the remote machine"""
    try:
        # Start iperf3 server on the remote machine
        server_cmd = f"sshpass -p {password} ssh {remote_user}@{remote_ip} 'iperf3 -s -D'"
        subprocess.run(server_cmd, shell=True, check=True)
        logging.info(f"Started iperf3 server on {remote_ip}")

        # Run iperf3 client on the VDI machine
        result = subprocess.run(
            ["iperf3", "-c", remote_ip, "-f", "m"],
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.split('\n'):
            if "receiver" in line:
                bandwidth_mbps = float(line.split()[-2])
                return bandwidth_mbps
    except subprocess.CalledProcessError as e:
        logging.error(f"iperf3 error: {e}")
    finally:
        # Stop iperf3 server on the remote machine
        stop_server_cmd = f"sshpass -p {password} ssh {remote_user}@{remote_ip} 'pkill iperf3'"
        subprocess.run(stop_server_cmd, shell=True)
        logging.info(f"Stopped iperf3 server on {remote_ip}")
    return None


def main(
    remote_user,
    remote_ip,
    remote_directory,
    remote_temp_directory,
    vdi_upload_directory,
    iperf_server_ip,
    password,
):
    # Create the remote temporary directory
    create_remote_directory(
        remote_user, remote_ip, remote_temp_directory, password
    )

    try:
        # Get the list of rosbags from the remote machine
        rosbag_list = get_remote_rosbags_list(
            remote_user, remote_ip, remote_directory, password
        )

        bandwidth_mbps = measure_bandwidth(remote_ip)
        if bandwidth_mbps is None:
            print("Could not measure bandwidth. Exiting.")
            return

        total_size_gb = sum(
            get_remote_file_size(remote_user, remote_ip, rosbag, password)
            for rosbag in rosbag_list
        )
        estimated_time = get_estimated_upload_time(
            total_size_gb, bandwidth_mbps
        )

        print(
            f"Found {len(rosbag_list)} files to upload with total size {total_size_gb:.2f} GB."
        )
        print(f"Measured bandwidth: {bandwidth_mbps:.2f} Mbps")
        print(f"Estimated upload time: {estimated_time:.2f} hours.")
        confirm = input("Do you want to proceed to upload? (yes/no): ")

        if confirm.lower() != 'yes':
            print("Upload aborted.")
            return

        logging.info(
            f"Starting upload of {len(rosbag_list)} files with total size {total_size_gb:.2f} GB."
        )
        logging.info(f"Measured bandwidth: {bandwidth_mbps:.2f} Mbps")

        for rosbag in rosbag_list:
            success = compress_and_transfer_rosbag(
                remote_user,
                remote_ip,
                rosbag,
                remote_temp_directory,
                vdi_upload_directory,
                password,
            )
            if not success:
                logging.error(
                    f"Failed to process {rosbag}. Continuing with next file."
                )
            else:
                logging.info(f"Successfully processed {rosbag}.")

    finally:
        # Delete the remote temporary directory
        delete_remote_directory(
            remote_user, remote_ip, remote_temp_directory, password
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automate the compression and upload of rosbags."
    )
    parser.add_argument("remote_user", help="Username for the remote machine")
    parser.add_argument(
        "password", help="Password for SSH and rsync operations"
    )
    parser.add_argument(
        "-remote_temp_directory",
        default="/mnt/mydrive/rosbags/temp",
        help="Remote temporary directory for storing compressed files",
    )
    parser.add_argument(
        "-iperf_server_ip",
        default="129.215.117.104",
        help="IP address of the iperf3 server for bandwidth measurement",
    )
    parser.add_argument(
        "-remote_ip",
        default="129.215.117.104",
        help="IP address of the remote machine (default: 129.215.117.104)",
    )
    parser.add_argument(
        "-remote_directory",
        default="/mnt/mydrive/rosbags",
        help="Directory on the remote machine containing rosbags (default: /mnt/mydrive/rosbags)",
    )
    parser.add_argument(
        "-vdi_upload_directory",
        default="/mnt/vdb/data",
        help="VDI directory for uploading compressed files (default: /mnt/vdb/data)",
    )

    args = parser.parse_args()

    main(
        args.remote_user,
        args.remote_ip,
        args.remote_directory,
        args.remote_temp_directory,
        args.vdi_upload_directory,
        args.iperf_server_ip,
        args.password,
    )
