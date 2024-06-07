# tartan_dataset_management
Collection of tools to manage ROSbag recordings data from AV vehicle

## Metadata Generator Usage

This script is used to generate metadata for ROSbag MCAP files. The metadata is compiled into a `resources.json` file that complies with the [EIDF requirements](https://github.com/marioa/eidf-docs/blob/data_ingest/docs/dataingest/ResourceMetaDataUserDoc.md)..

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

### Prerequisites

Ensure all dependencies are installed. You can use the following command to install required packages:

```bash
pip install mcap
```

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
