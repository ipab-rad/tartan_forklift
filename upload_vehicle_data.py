import subprocess
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil

# Setup logging
logging.basicConfig(filename='upload_vehicle_data.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s: %(message)s')

def get_rosbags(directory):
    """List all rosbags in the directory"""
    return list(Path(directory).rglob('*.mcap'))

def get_file_size(file_path):
    """Get size of a file in GB"""
    return os.path.getsize(file_path) / (1024 ** 3)

def compress_rosbag(file_path):
    """Compress a rosbag using mcap CLI with zstd level 2"""
    compressed_file_path = f"{file_path}.zst"
    cmd = ["mcap", "compress", "-o", compressed_file_path, file_path]
    subprocess.run(cmd, check=True)
    return compressed_file_path

def upload_file(file_path, remote_path):
    """Upload a file using rsync and verify its integrity"""
    rsync_cmd = ["rsync", "-avz", "--checksum", file_path, remote_path]
    subprocess.run(rsync_cmd, check=True)

def remove_file(file_path):
    """Remove a file"""
    os.remove(file_path)

def get_estimated_upload_time(total_size_gb, bandwidth_mbps):
    """Estimate upload time in hours"""
    bandwidth_mbs = bandwidth_mbps / 8  # Convert Mbps to MB/s
    total_size_mb = total_size_gb * 1024  # Convert GB to MB
    return total_size_mb / bandwidth_mbs / 3600  # Return time in hours

def measure_bandwidth(server_ip):
    """Measure the actual bandwidth using iperf3"""
    try:
        result = subprocess.run(["iperf3", "-c", server_ip, "-f", "m"], capture_output=True, text=True, check=True)
        for line in result.stdout.split('\n'):
            if "receiver" in line:
                bandwidth_mbps = float(line.split()[-2])
                return bandwidth_mbps
    except subprocess.CalledProcessError as e:
        logging.error(f"iperf3 error: {e}")
        return None

def main(vehicle_directory, remote_directory, iperf_server_ip, max_parallel_compressions=4):
    rosbags = get_rosbags(vehicle_directory)
    total_size_gb = sum(get_file_size(f) for f in rosbags)
    
    bandwidth_mbps = measure_bandwidth(iperf_server_ip)
    if bandwidth_mbps is None:
        print("Could not measure bandwidth. Exiting.")
        return

    estimated_time = get_estimated_upload_time(total_size_gb, bandwidth_mbps)

    print(f"Found {len(rosbags)} files to upload with total size {total_size_gb:.2f} GB.")
    print(f"Measured bandwidth: {bandwidth_mbps:.2f} Mbps")
    print(f"Estimated upload time: {estimated_time:.2f} hours.")
    confirm = input("Do you want to proceed to upload? (yes/no): ")

    if confirm.lower() != 'yes':
        print("Upload aborted.")
        return

    logging.info(f"Starting upload of {len(rosbags)} files with total size {total_size_gb:.2f} GB.")
    logging.info(f"Measured bandwidth: {bandwidth_mbps:.2f} Mbps")

    with ThreadPoolExecutor(max_workers=max_parallel_compressions) as executor:
        for rosbag in rosbags:
            compressed_file = executor.submit(compress_rosbag, rosbag)
            try:
                compressed_file_path = compressed_file.result()
                upload_file(compressed_file_path, remote_directory)
                remove_file(compressed_file_path)
                remove_file(rosbag)
                logging.info(f"Successfully uploaded and removed {rosbag}.")
            except Exception as e:
                logging.error(f"Error processing {rosbag}: {e}")

if __name__ == "__main__":
    vehicle_directory = "/path/to/vehicle/rosbags"
    remote_directory = "user@remote:/path/to/cloud/storage"
    iperf_server_ip = "remote.server.ip"  # Update with the IP of the server
    main(vehicle_directory, remote_directory, iperf_server_ip)
