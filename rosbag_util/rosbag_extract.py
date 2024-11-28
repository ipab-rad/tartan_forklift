import os
import argparse
import subprocess
import re
import time
import yaml
import shutil
from pathlib import Path


def sort_by_numeric_suffix(files):
    def extract_number(file):
        match = re.search(r"_(\d+)\.mcap$", file)
        return int(match.group(1)) if match else float('inf')  # Non-matching files go to the end
    return sorted(files, key=extract_number)

def convert_rosbags(input_dir: str, yaml_file_path: str, range_str: str = None):
    """
    Automates the conversion of ROS2 bag files using the ros2 bag convert command.

    Args:
        input_dir (str): Path to the directory containing .mcap files.
        yaml_file_path (str): Path to the YAML configuration file for --output-options.
    """
    
    # Load the YAML file
    try:
        with open(yaml_file_path, "r") as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)
    except Exception as e:
        raise ValueError(f"Failed to load YAML file: {yaml_file_path}. Error: {e}")
    
    output_bag_name = yaml_content.get("output_bags", [{}])[0].get("uri")

    if not output_bag_name:
        raise ValueError(f"No 'uri' value found in 'output_bags' of YAML file: {yaml_file_path}")

    # Find all .mcap files in the given directory in a non-recursive way   
    mcap_files = [str(file) for file in Path(input_dir).glob("*.mcap")]
    mcap_files = sort_by_numeric_suffix(mcap_files)

    if not mcap_files:
        print(f"No .mcap files found in directory: {input_dir}")
        return
    
    if range_str:
        try:
            start, end = map(int, range_str.split(":"))
            mcap_files = mcap_files[start:end + 1]  # Slice the list based on the range
        except (ValueError, IndexError):
            print(f"Invalid range: {range_str}. Expected format is 'start:end'.")
            return

    if not mcap_files:
        print(f"No files in the specified range: {range_str}")
        return
    
    # Construct the ros2 bag convert command
    command = ["ros2", "bag", "convert"]

    print(f'Rosbags to be merged and filtered based on {yaml_file_path}')
    for mcap_file in mcap_files:
        print(mcap_file)
        command.extend(["--input", mcap_file])
        
    # Add the storage_id and output-options
    command.extend(["mcap", "--output-options", yaml_file_path])

    # Execute the command
    try:
        start_time = time.time()
        
        subprocess.run(command, check=True)
        
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        print(f'{len(mcap_files)} rosbags mereged and filtered in {elapsed_time:.2f} seconds')
        
        # Rename rosbag for consistency
        output_bag_file_path = os.path.join(input_dir, output_bag_name, output_bag_name +'_0.mcap' )
        
        range_subfix = ""
        if range_str:
          range_subfix = f'_{start}-{end}'
        base_name = mcap_files[0].rsplit("_", 1)[0]
        new_bag_file_name =  base_name + '_' + output_bag_name + range_subfix +  '.mcap'
        new_bag_file_path = os.path.join(input_dir, output_bag_name, new_bag_file_name)
        
        # Rename generated bag
        shutil.move(output_bag_file_path, new_bag_file_path)
        
        print(f'Saved as: {new_bag_file_path}')
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e}")
        
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Convert ROS2 bags in a directory.")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the directory containing .mcap files.",
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the YAML configuration file for --output-options.",
    )
    parser.add_argument(
        "--range",
        type=str,
        help="Range of indices to process in the format 'start:end'. Defaults to all files.",
    )

    # Parse arguments
    args = parser.parse_args()

    # Call the conversion function with parsed arguments
    convert_rosbags(args.input, args.config, args.range)

if __name__ == "__main__":
    main()