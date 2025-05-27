#!/bin/bash
set -e

# Setup ROS2 env
source "/opt/ros/$ROS_DISTRO/setup.bash"
source "/opt/ros_ws/install/setup.bash"

# Load Dataset keys if file exists
if [ -f /keys/dataset_keys.env ]; then
    set -o allexport
    source /keys/dataset_keys.env
    set +o allexport
else
    echo "Warning: /keys/dataset_keys.env not found. Skipping dataset key loading."
fi

# Important for access python scripts
export PYTHONPATH="/usr/local/lib/python3.10/site-packages:$PYTHONPATH"
export PATH="/usr/local/lib/python3.10/site-packages/bin:$PATH"
export PATH="$HOME/.local/bin:$PATH"

# If no command is provided, start an interactive bash session
if [ $# -eq 0 ]; then
    exec bash
else
    exec "$@"
fi
