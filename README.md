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

#### 1.Configuration

Update the script with the correct paths and IP addresses:

  -vehicle_directory: The directory on the vehicle where the rosbags are stored.
  -remote_directory: The remote directory where the rosbags will be uploaded.
  -iperf_server_ip: The IP address of the iperf3 server.

#### 2.Script Workflow:

-The script measures the available bandwidth using iperf3.
-Lists all .mcap files in the specified directory.
-Displays the total number of files, their combined size, and the estimated upload time based on the measured bandwidth.
-Prompts the user to confirm the upload.
-Compresses each .mcap file using mcap CLI with zstd level 2.
-Uploads each compressed file to the remote server using rsync.
-Verifies the integrity of the uploaded file.
-Removes the original and compressed files from the vehicle after successful upload and verification.
-Logs the entire process, including any errors.

#### 3.Script Parameters
-vehicle_directory: Directory containing the rosbags on the vehicle.
-remote_directory: Remote server directory for the uploaded files.
-iperf_server_ip: IP address of the iperf3 server.
-max_parallel_compressions: Maximum number of parallel compressions (default is 4).

#### 4.Logging 
The script logs its activity to upload_vehicle_data.log. This log file contains:

  -Information about the files processed.
  -Bandwidth measurements.
  -Any errors encountered during the process.

#### 5.usage
  ```bash
  python upload_vehicle_data.py
  ```



