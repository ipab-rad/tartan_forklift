# Tartan Forklift

Collection of tools to manage ROSbag recordings data from AV vehicle.

## Labelling preproc

This package contains different modules to read and parse exported ROS sensor data to create and prepare a dataset sample for the [Segments.ai](https://segments.ai/) platform for labelling.

Currently, the modules assume the following:

  1. A ROS bag was exported using [ipab-rad/tartan_rosbag_exporter](https://github.com/ipab-rad/tartan_rosbag_exporter).
  2. The user is familiar with Segments.ai platform and its sample formats, and has created a dataset with [multi-sensor sequence](https://docs.segments.ai/reference/sample-types#multi-sensor-sequence) support.
  3. The user has access to both EIDF S3 and Segments.ai.

### Usage guide

To use the `labelling_preproc`'s modules to upload and add a **multi-sensor sequence** to segments.ai, you will need access key tokens.

Create a file named `dataset_keys.env` inside a `keys` directory in the parent directory of this repository:

```bash
mkdir -p keys && touch keys/dataset_keys.env
```

Add the following environment variables to `dataset_keys.env`:

```bash
# EIDF AWS S3
AWS_ACCESS_KEY_ID=my_access_key_id
AWS_SECRET_ACCESS_KEY=my_secret_access_key
AWS_ENDPOINT_URL=my_s3_organisation_url
BUCKET_NAME=my_bucket_name
EIDF_PROJECT_NAME=my_projectxyz

# Segments.ai key
SEGMENTS_API_KEY=my_segment_ai_api_key
```

The `dev.sh` script will attempt to locate the `dataset_keys.env` file. If the file is missing or incorrectly named, the script will throw an error. File and path names are case-sensitive.

For access credentials, please contact [Hector Cruz](@hect95) or [Alejandro Bordallo](@GreatAlexander).

#### Build and run the Docker container

To build and run the Docker container interactively, use:

```bash
./dev.sh -l -p <rosbags_directory> -o <exported_data_directory>
```

where:

- `<rosbags_directory>`: Parent directory containing your ROS bags recordings
- `<exported_data_directory>`: Parent directory where the data is/will be exported.

The input directories will be mounted in `/opt/ros_ws/rosbags` and `/opt/ros_ws/exported_data` in the container respectively.

After running the Docker container, install the Python modules:

```bash
pip install -e ./scripts
```

#### Export your ROS bags

As mentioned above, the `labelling_preproc` modules expect exported data before creating a sample. You can export your rosbags with the following command:

```bash
cd /opt/ros_ws

ros2 run ros2_bag_exporter bag_exporter --ros-args \
  -p rosbags_directory:=./rosbags/<my_recording_directory> \
  -p output_directory:=./exported_data \
  -p config_file:=./config/av_sensor_export_config.yaml
```

The [config/av_sensor_export_config.yaml](./config/av_sensor_export_config.yaml) is the default config file that tells `ros2_bag_exporter` which sensor topics and data formats to use.

The exporter will create a directory inside `exported_data/`. This directory will contain:

- Extracted point clouds (`.pcd`)
- Images (`.jpg`)
- `export_metadata.yaml`

We'll refer to this directory as `<data_directory>`.

#### Add a multi-sensor sequence sample

Create a new dataset on the Segments.ai platform if you haven't already. For consistency, name the dataset exactly the same as your exported `<data_directory>` directory. On Segments.ai, datasets follow the format `organisation_name/dataset_name`. Therefore, your full `dataset_name` should be `UniversityOfEdinburgh/<data_directory>_name`, where `UniversityOfEdinburgh` is the organisation name currently in use. This naming convention helps keep your exported data and Segments.ai datasets aligned.

1. **Extract the Ego Trajectory from the ROSbag**

    ```bash
    generate_ego_trajectory <my_path_to_rosbag.mcap> <data_directory>
    ```

    A `.tum` file with the same name as your rosbag should appear in `<data_directory>`.

2. **Upload Data to S3**

    To upload the extracted data to either EIDF or Segments.ai AWS S3, run:

    ```bash
    upload_data <dataset_name> <data_directory> eidf
    # Or
    upload_data <dataset_name> <data_directory> segments
    ```

    If no S3 organisation is specified, `eidf` is used by default.

    After the upload, you should see an `upload_metadata.json` file inside `<data_directory>`.

3. **Add a multi-sensor sample to Segments.ai**

    Run the script:

    ```bash
    add_segmentsai_sample <dataset_name> <sequence_name> <data_directory>
    ```

    where:

    - `<dataset_name>`: The full dataset name on Segments.ai
    - `<sequence_name>`: Desired sequence name for the multi-sensor sample
        - Ensure the sequence name is unique within your dataset; otherwise, the sample will not be uploaded
    - `<data_directory>`: Directory with the extracted rosbags and metadata files

If successful, you will see your new sequence listed in the _Samples_ tab on your dataset page.

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

## Upload rosbags

This script automates the process of uploading rosbags from the IPAB-RAD autonomous vehicle server to a cloud instance within the [EIDF](https://edinburgh-international-data-facility.ed.ac.uk/) (Edinburgh International Data Facility) infrastructure. It streamlines data collection and transfer by first compressing the rosbags using the [MCAP CLI](https://mcap.dev/guides/cli), and then uploading the compressed files. This ensures efficient handling and storage of large datasets generated by vehicle sensors.

### 1. Dependencies

#### Vehicle machine (host)

- Install Python dependencies:

  ```bash
  pip install colorlog paramiko paramiko_jump
  ```

- Install MCAP CLI `v0.0.47` for rosbag compression:

  ```bash
  wget -O $HOME/mcap https://github.com/foxglove/mcap/releases/download/releases%2Fmcap-cli%2Fv0.0.47/mcap-linux-amd64
  chmod +x $HOME/mcap
  ```

- Set up an FTP server by following this [guide](https://documentation.ubuntu.com/server/how-to/networking/ftp/index.html)

#### EIDF (cloud)

- Install `lftp`:
  ```bash
  sudo apt update && sudo apt install lftp
  ```

### 2. Usage

To execute the script from the host machine, run:

```bash
python3 -m upload_rosbags.upload_rosbags \
  --config ./upload_rosbags/upload_config.yaml \
  --lftp-password <host_user_password> \

# Add --debug flag to set DEBUG log level
```

### 3. YAML Parameters

The configuration file accepts the following parameters:

- `local_host_user` (str): Username for the host machine.
- `local_hostname` (str): IP address or hostname of the host machine (interface connected to the internet).
- `local_rosbags_directory` (str): Path to the directory on the host machine containing the rosbags.
- `cloud_ssh_alias` (str): SSH alias for the cloud server defined in `~/.ssh/config`. If unset, `cloud_user` and `cloud_hostname` must be provided.
- `cloud_user` (str): Username for the cloud target machine. Ignored if `cloud_ssh_alias` is defined and valid.
- `cloud_hostname` (str): Hostname or IP of the cloud target machine. Ignored if `cloud_ssh_alias` is defined and valid.
- `cloud_upload_directory` (str): Destination directory on the cloud server for uploading compressed files.
- `mcap_bin_path` (str): Full path to the `mcap` CLI binary.
- `mcap_compression_chunk_size` (int): Chunk size in bytes used during MCAP compression.
- `compression_parallel_workers` (int): Number of parallel worker threads for compression.
- `compression_queue_max_size` (int): Maximum number of compressed rosbags allowed in the queue at any time.

See [upload\_config.yaml](./upload_rosbags/upload_config.yaml) for a sample configuration.

### 4. Logging

The script logs its actions to a file named `<timestamp>_rosbag_upload.log`.



## ROS2 Bag Merging Script

This script automates the merging of ROS2 bag files using the `ros2 bag convert` command.

The merging is based on a `.yaml` file that complies with [rosbag2](https://github.com/ros2/rosbag2?tab=readme-ov-file#converting-bags) guidelines.

### Prerequisites

Before running the Python script, ensure that the Docker container is set up by running the `dev.sh` script:

```bash
./dev.sh
```

**Note**: `dev.sh` will automatically mount the `/mnt/vdb/data` directory as the `rosbags` directory in the container. This is the current default SSD location on the EIDF cloud machine. Use the `-p` flag to specify a different directory.

### Usage

Once the Docker container is running, you can execute the Python script with the following command:

```bash
merge_rosbags.py --input <input_directory> --config <config_file.yaml> --range <start:end>
```

#### Arguments:
- `--input <input_directory>`: The directory containing `.mcap` files to be merged.
- `--config <config_file.yaml>`: Path to the YAML configuration file used for the merge.
  - Please refer to the `rosbag_util` directory and `rosbag2` [Converting bags](https://github.com/ros2/rosbag2?tab=readme-ov-file#converting-bags) `README` for further reference.
- `--range <start:end>` (optional): Range of rosbag split indices to process, in the format `start:end`.

Example:

If we have the following directory structure:

```bash
└── rosbags
      ├── 2024_10_24-14_15_33_haymarket_1
      │   ├── 2024_10_24-14_15_33_haymarket_1_0.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_1.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_2.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_3.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_4.mcap
      ...
```

We can run the script as:

```bash
merge_rosbags.py --input ./rosbags/2024_10_24-14_15_33_haymarket_1 --range 0:2 --config ./rosbag_util/mapping_merge_params.yaml

# Or omit the range to merge all available rosbags
merge_rosbags.py --input ./rosbags/2024_10_24-14_15_33_haymarket_1 --config ./rosbag_util/mapping_merge_params.yaml
```

The resulting merged `.mcap` file will be saved within the specified directory under a sub-directory named the same as the value of the `uri` parameter defined in the YAML file. The file name will be in the format `<current_rosbag_name>_<uri>_<from-to>`. If no range is specified, the name will be `<current_rosbag_name>_<suffix>`.

```bash
└── rosbags
      ├── 2024_10_24-14_15_33_haymarket_1
      │   ├── 2024_10_24-14_15_33_haymarket_1_0.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_1.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_2.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_3.mcap
      │   ├── 2024_10_24-14_15_33_haymarket_1_4.mcap
      │   └── mapping
              ├── 2024_10_24-14_15_33_haymarket_mapping_0-2.mcap
              └── 2024_10_24-14_15_33_haymarket_mapping.mcap # Larger file
      ...
```
