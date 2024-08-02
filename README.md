# tartan_dataset_management
Collection of tools to manage ROSbag recordings data from AV vehicle

## Metadata Generator Usage

This script generates metadata for ROSbag MCAP files. The metadata is compiled into a `resources.json` file that complies with the EIDF requirements

### Features
- Reads MCAP files and extracts metadata such as:
  - Duration of the log
  - Topics and message counts
  - File size
  - File hash (MD5)
- Generates a JSON file (`resources.json`) with metadata for all MCAP files in a given directory.
- Metadata includes:
  - File name
  - Identifier
  - Description
  - Format
  - License
  - Size
  - Hash
  - Issued date
  - Modified date
  - Duration
  - Topics and message counts

### Usage

#### 1. Setup

Ensure all dependencies are installed. You can use the following command to install required packages:

```bash
pip install mcap
```

#### 2. Running the Script

To generate the metadata JSON file, follow these steps:

- Place all your MCAP files in a directory.
- The default directory is `/recorded_datasets/edinburgh`
- Run the script:

  ```bash
  python metadata_generator.py
  ```

If you want to generate metadata for files in a specified path, run the script:

```bash
python metadata_generator.py -p path/to/file
```

#### 3. Output

The script will generate a `resources.json` file in the specified directory. This JSON file will contain metadata for each MCAP file in the directory.

## Upload_vehicle_data usage
This script automates the upload of rosbags from a vehicle to a cloud infrastructure, specifically within the EIDF (Edinburgh International Data Facility) cloud instance. The primary purpose of this script is to streamline the process of data collection and transfer, ensuring efficient handling and storage of large datasets generated by vehicle sensors.

- EIDF Cloud Instance: The EIDF cloud provides robust and scalable infrastructure for data storage and computation, which is crucial for handling the large volumes of data generated by autonomous vehicles. By utilizing the EIDF cloud, the script ensures that data is securely stored and readily available for further analysis and processing.

- Data Collection: The rosbags collected contain crucial data from various sensors on the vehicle, including LiDAR, cameras, and GPS. This data is essential for research and development in autonomous driving, enabling tasks such as sensor fusion, environment mapping, and behavior prediction.

- Automation and Efficiency: Manually uploading large datasets can be time-consuming and prone to errors. This script automates the entire process, from compressing the rosbags to transferring them securely to the remote host machine. It also includes functionality to manage disk space efficiently on the remote machine.

### 1. Dependencies


  ##### 1. Remote host machine
- iperf3: A tool for measuring bandwidth.
```bash
sudo apt-get install iperf3
```
- set SSH keys authentication: This should be done before using this script.
   - create an ssh-key:
   ```bash
   ssh-keygen -t rsa -b 2048
   ```
   - share a SSH Key
   ```bash
   ssh-copy-id username@hostname_ip
   ```

#### 2. Remote machine
- iperf3: A tool for measuring bandwidth.
```bash
sudo apt-get install iperf3
```
- MCAP cli: A tool for compressing rosbags
```bash
wget -O $HOME/mcap https://github.com/foxglove/mcap/releases/download/releases%2Fmcap-cli%2Fv0.0.47/mcap-linux-amd64 && chmod +x $HOME/mcap
```
### 2. Usage
To run the script, use the following command:
  ```bash
  python3 upload_vehicle_data.py -config <path_to_yaml_config>
  ```
  or
  ```bash
  python3 upload_vehicle_data.py -c <path_to_yaml_config>
  ```
Replace <path_to_yaml_config> with the path to your YAML configuration file that contains the necessary parameters for the script.

### 3. YAML Parameters
- `remote_user` (str): Username for the remote machine.
- `remote_ip` (str): IP address of the remote machine (vehicle PC) (default: 129.215.117.104).
- `remote_directory` (str): Directory on the remote machine containing rosbags (default: /mnt/mydrive/rosbags).
- `cloud_upload_directory` (str): Remote host directory for uploading compressed files (default: /mnt/vdb/data).
- `clean_up ` (bool): Whether or not to delete all rosbags from the vehicle machine after uploading
- `upload_attempts` (int): The number of attempts the script should make to upload each rosbag file to the cloud host. If not specified, the default value is `3`.
- `mcap_path` (str): The binary path for the mcap cli. Can be found using `which mcap` if mcap is correctly installed.
- `parallel_processes`: The number of parallel processes to use for compression and upload.
YAML file example:
```bash
remote_user: "user namee"
remote_temp_directory: "/mnt/mydrive/rosbags/temp"
remote_ip: "129.215.117.104"
remote_directory: "/mnt/mydrive/rosbags"
cloud_upload_directory: "/mnt/vdb/data"
clean_up: false
upload_attempts: 3
mcap_path: "Home/mcap"
parallel_processes: 1

```


### 4. Script Workflow:
- The script measures the available bandwidth using iperf3.
- Create the remote temporary directory.
- Lists all .mcap files in the specified directory.
- Displays the total number of files, their combined size, and the estimated upload time based on the measured bandwidth.
- Prompts the user to confirm the upload.
- Compresses each .mcap file using mcap CLI with zstd level 2.
- Uploads each compressed file to the remote server using rsync.
- Verifies the integrity of the uploaded file.
- Removes the original and compressed files from the vehicle after successful upload and verification.
- Logs the entire process, including any errors.

### 5. Logging
The script logs its activity to upload_vehicle_data.log. This log file contains:

  - Information about the files processed.
  - Bandwidth measurements.
  - Any errors encountered during the process.
