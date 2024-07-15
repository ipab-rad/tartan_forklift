# tartan_dataset_management
Collection of tools to manage ROSbag recordings data from AV vehicle

## Metadata Generator Usage

This script generates metadata for ROSbag MCAP files. The metadata is compiled into a `resources.json` file that complies with ...

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


### 1. Script Workflow:

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

### 2. Script Parameters
- `remote_user` (str): Username for the remote machine.
- `password (str)`: Password for SSH and rsync operations.
- `remote_temp_directory` (str): Remote temporary directory for storing compressed files (default: /mnt/mydrive/rosbags/temp).
- `iperf_server_ip` (str): IP address of the iperf3 server for bandwidth measurement (default: 129.215.117.104).
- `remote_ip` (str): IP address of the remote machine (vehicle PC) (default: 129.215.117.104).
- `remote_directory` (str): Directory on the remote machine containing rosbags (default: /mnt/mydrive/rosbags).
- `vdi_upload_directory` (str): VDI directory for uploading compressed files (default: /mnt/vdb/data).

### 3. Logging
The script logs its activity to upload_vehicle_data.log. This log file contains:

  - Information about the files processed.
  - Bandwidth measurements.
  - Any errors encountered during the process.

### 4. Usage
  ```bash
  upload_vehicle_data.py remote_user password
  ```

### 5. Dependencies


  ##### 1. VDI machine
- sshpass: A non-interactive ssh password authentication tool.
```bash
sudo apt-get install sshpass
```
- iperf3: A tool for measuring bandwidth.
```bash
sudo apt-get install iperf3
```
#### 2. Remote machine
- iperf3: A tool for measuring bandwidth.
```bash
sudo apt-get install iperf3
```
- MCAP cli: A tool for compressing rosbags

  https://mcap.dev/guides/cli
