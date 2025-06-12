#!/bin/bash
# ----------------------------------------------------------------
# Build docker dev stage and add local code for live development
# ----------------------------------------------------------------

CYCLONE_VOL=""

# Default cyclone_dds.xml path
CYCLONE_DIR=~/cyclone_dds.xml
# Default in-vehicle rosbags directory
ROSBAGS_DIR=/mnt/vdb/data
# Default in-cloud exports directory
EXPORTS_OUTPUT_DIR=/mnt/vdb/exported_data

# Function to print usage
usage() {
    echo "
Usage: dev.sh [-l|--local] [--path | -p ] [--output | -o ] [--headless] [--help | -h]

Options:
    -l | --local    Use default local cyclone_dds.xml config
                    Optionally point to absolute -l /path/to/cyclone_dds.xml
    -p | --path   ROSBAGS_DIR_PATH
                    Specify path to store recorded rosbags
    -o | --output EXPORTS_OUTPUT_DIR
                    Specify path where exported data is stored
    -h | --help     Display this help message and exit.
    "
    exit 1
}

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -l|--local)
            if [[ -n "$2" && "$2" != -* ]]; then
                CYCLONE_DIR="$2"
                shift
            fi
            CYCLONE_VOL="-v $CYCLONE_DIR:/opt/ros_ws/cyclone_dds.xml"
            ;;
        -p|--path)
            if [[ -n "$2" && "$2" != -* ]]; then
                ROSBAGS_DIR="$2"
                shift
            else
                echo "Error: Argument for $1 is missing."
                usage
            fi
            ;;
        -o|--output)
            if [[ -n "$2" && "$2" != -* ]]; then
                EXPORTS_OUTPUT_DIR="$2"
                shift
            else
                echo "Error: Argument for $1 is missing."
                usage
            fi
            ;;
        -h|--help) usage ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
    shift
done


# Verify CYCLONE_DIR exists
if [ -n "$CYCLONE_VOL" ]; then
    if [ ! -f "$CYCLONE_DIR" ]; then
        echo "$CYCLONE_DIR does not exist! Please provide a valid path to cyclone_dds.xml"
        exit 1
    fi
fi

# Verify ROSBAGS_DIR exists
if [ ! -d "$ROSBAGS_DIR" ]; then
    echo "$ROSBAGS_DIR does not exist! Please provide a valid path to store rosbags"
    exit 1
fi

# Verify EXPORTS_OUTPUT_DIR exists
if [ ! -d "$EXPORTS_OUTPUT_DIR" ]; then
    echo "$EXPORTS_OUTPUT_DIR does not exist! Please provide a valid path where exported data is stored"
    exit 1
fi

# Build docker image up to dev stage
docker build \
    --build-arg USER_ID=$(id -u) \
    --build-arg GROUP_ID=$(id -g) \
    --build-arg USERNAME=tartan_forklift \
    -t tartan_forklift:latest-dev \
    -f Dockerfile --target dev .

# Get the absolute path of the script
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

KEYS_FILE=$SCRIPT_DIR/keys/dataset_keys.env
# Check this file exist, exit otherwise
if [ ! -f "$KEYS_FILE" ]; then
    echo "$KEYS_FILE does not exist! Docker container will not run"
    exit 1
fi

# Run docker image with local code volumes for development
docker run -it --rm --net host \
    --user "$(id -u):$(id -g)" \
    $CYCLONE_VOL \
    -v $KEYS_FILE:/keys/dataset_keys.env \
    -v $EXPORTS_OUTPUT_DIR:/opt/ros_ws/exported_data \
    -v $ROSBAGS_DIR:/opt/ros_ws/rosbags \
    -v $SCRIPT_DIR/config:/opt/ros_ws/config \
    -v $SCRIPT_DIR/rosbag_util:/opt/ros_ws/rosbag_util \
    -v $SCRIPT_DIR/scripts:/opt/ros_ws/scripts \
    -v /etc/localtime:/etc/localtime:ro \
    tartan_forklift:latest-dev
