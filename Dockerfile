FROM ros:humble-ros-base-jammy AS base

# Install key dependencies
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y --quiet --no-install-recommends install \
        ros-"$ROS_DISTRO"-can-msgs \
        ros-"$ROS_DISTRO"-dataspeed-ulc-msgs \
        ros-"$ROS_DISTRO"-dbw-ford-msgs \
        ros-"$ROS_DISTRO"-ffmpeg-image-transport \
        ros-"$ROS_DISTRO"-flir-camera-msgs \
        ros-"$ROS_DISTRO"-gps-msgs \
        ros-"$ROS_DISTRO"-image-transport \
        ros-"$ROS_DISTRO"-image-transport-plugins \
        ros-"$ROS_DISTRO"-mcap-vendor \
        ros-"$ROS_DISTRO"-microstrain-inertial-msgs \
        ros-"$ROS_DISTRO"-nmea-msgs \
        ros-"$ROS_DISTRO"-novatel-gps-msgs \
        ros-"$ROS_DISTRO"-ouster-msgs \
        ros-"$ROS_DISTRO"-radar-msgs \
        ros-"$ROS_DISTRO"-rmw-cyclonedds-cpp \
        ros-"$ROS_DISTRO"-rosbag2-storage-mcap \
        ros-"$ROS_DISTRO"-velodyne-msgs \
        ros-"$ROS_DISTRO"-geographic-msgs \
        ros-"$ROS_DISTRO"-autoware-*-msgs \
        python3-pip \
        python3-vcstool \
    && pip install --no-cache-dir mcap \
    && rm -rf /var/lib/apt/lists/*

# Setup ROS workspace folder
ENV ROS_WS=/opt/ros_ws
WORKDIR $ROS_WS

# Set cyclone DDS ROS RMW
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

COPY ./cyclone_dds.xml $ROS_WS/

# Configure Cyclone cfg file
ENV CYCLONEDDS_URI=file://${ROS_WS}/cyclone_dds.xml

# Enable ROS log colorised output
ENV RCUTILS_COLORIZED_OUTPUT=1

# Copy tools scripts and config
COPY rosbag_util $ROS_WS/rosbag_util

# Come back to ros_ws
WORKDIR $ROS_WS

# Create username
ARG USER_ID
ARG GROUP_ID
ARG USERNAME=tartan_forklift

RUN groupadd -g $GROUP_ID $USERNAME && \
    useradd -u $USER_ID -g $GROUP_ID -m -l $USERNAME && \
    usermod -aG sudo $USERNAME && \
    echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# -----------------------------------------------------------------------

FROM base AS prebuilt

# Nothing to build from source

# -----------------------------------------------------------------------

FROM prebuilt AS dev

# Install basic dev tools (And clean apt cache afterwards)
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y --quiet --no-install-recommends install \
        # Command-line editor
        nano \
        # Ping network tools
        inetutils-ping \
        # Bash auto-completion for convenience
        bash-completion \
        # ROS Rqt graph \
        ros-"$ROS_DISTRO"-rqt-graph \
    && rm -rf /var/lib/apt/lists/*

# Add colcon build alias for convenience
RUN echo 'alias colcon_build="colcon build --symlink-install \
    --cmake-args -DCMAKE_BUILD_TYPE=Release && \
    source install/setup.bash"' >> /etc/bash.bashrc

# Enter bash for clvelopment
CMD ["bash"]

# -----------------------------------------------------------------------

FROM base AS runtime

# Do nothing here
CMD ["bash"]
