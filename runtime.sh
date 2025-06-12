#!/bin/bash
# ----------------------------------------------------------------
# Build docker runtime stage and run it with the provided options
# ----------------------------------------------------------------

CYCLONE_VOL=""
BASH_CMD=()

# Default cyclone_dds.xml path
CYCLONE_DIR=~/cyclone_dds.xml
# Default rosbags directory
ROSBAGS_DIR=/mnt/vdb/data
# Default export directory
EXPORTS_OUTPUT_DIR=/mnt/vdb/exported_data

# Function to print usage
usage() {
    echo "
Usage: runtime.sh [OPTIONS] [COMMAND]

Options:
  -l, --local [PATH]      Use local cyclone_dds.xml configuration file in $HOME.
                          Optionally provide an absolute path with -l /path/to/cyclone_dds.xml.
  -p, --path PATH         Path to the directory where recorded rosbags are stored.
                            Default: $ROSBAGS_DIR

  -o, --output PATH       Path to the directory where exported data will be saved.
                            Default: $EXPORTS_OUTPUT_DIR
  -h, --help              Display this help message and exit.

Arguments:
  COMMAND                 Optional command to run inside the Docker container.
                          Defaults to an interactive Bash shell if not specified.

Examples:
  runtime.sh                        # Run container with default paths and start bash
  runtime.sh -p /data/rosbags ls    # Run 'ls' inside container with custom rosbag path
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
            # Save all remaining args as the command
            BASH_CMD=("$@")
            break
            ;;
    esac
    shift
done

# If no command was given, default to bash
if [[ ${#BASH_CMD[@]} -eq 0 ]]; then
    BASH_CMD=("bash")
fi

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
    -t tartan_forklift:latest \
    -f Dockerfile --target runtime .

# Get the absolute path of the script
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

KEYS_FILE=$SCRIPT_DIR/keys/dataset_keys.env
# Check this file exist, exit otherwise
if [ ! -f "$KEYS_FILE" ]; then
    echo "$KEYS_FILE does not exist! Docker container will not run"
    exit 1
fi

# Run docker image
docker run -it --rm --net host \
    --user "$(id -u):$(id -g)" \
    $CYCLONE_VOL \
    -v $KEYS_FILE:/keys/dataset_keys.env \
    -v $EXPORTS_OUTPUT_DIR:/opt/ros_ws/exported_data \
    -v $ROSBAGS_DIR:/opt/ros_ws/rosbags \
    -v /etc/localtime:/etc/localtime:ro \
    tartan_forklift:latest "${BASH_CMD[@]}"
