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
        ros-"$ROS_DISTRO"-mola \
        ros-"$ROS_DISTRO"-mola-state-estimation \
        ros-"$ROS_DISTRO"-mola-lidar-odometry \
        python3-pip \
        python3-vcstool \
    && pip install --no-cache-dir mcap pandas colorama \
       segments-ai awscli boto3 scipy \
    && pip install --no-cache-dir --upgrade setuptools pip \
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
COPY scripts     $ROS_WS/scripts
COPY config     $ROS_WS/config

# Come back to ros_ws
WORKDIR $ROS_WS

# Add scripts to PATH and source ros
RUN echo "source /opt/ros/$ROS_DISTRO/setup.bash" >> /etc/bash.bashrc

# Create username
ARG USER_ID
ARG GROUP_ID
ARG USERNAME=tartan_forklift

RUN groupadd -g $GROUP_ID $USERNAME && \
    useradd -u $USER_ID -g $GROUP_ID -m -l $USERNAME && \
    usermod -aG sudo $USERNAME && \
    echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Setup ros2_bag_exporter
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y --quiet --no-install-recommends install \
       libopencv-dev \
       libpcl-dev \
       libyaml-cpp-dev \
       ros-"$ROS_DISTRO"-ament-index-cpp  \
       ros-"$ROS_DISTRO"-cv-bridge \
       ros-"$ROS_DISTRO"-pcl-conversions \
       ros-"$ROS_DISTRO"-pcl-ros \
       ros-"$ROS_DISTRO"-rclcpp \
       ros-"$ROS_DISTRO"-rosbag2-cpp \
       ros-"$ROS_DISTRO"-rosbag2-storage \
       ros-"$ROS_DISTRO"-sensor-msgs \
    && pip install --no-cache-dir mcap colorama \
    && rm -rf /var/lib/apt/lists/*

ENV EXPORTER=$ROS_WS/src/tartan_rosbag_exporter
RUN git clone -b v1.0.0 https://github.com/ipab-rad/tartan_rosbag_exporter.git $EXPORTER \
    && . /opt/ros/"$ROS_DISTRO"/setup.sh \
    && colcon build --cmake-args -DCMAKE_BUILD_TYPE=Release \
    && rm -rf $ROS_WS/build $EXPORTER

# Give read/write permissions to the user on the ROS_WS directory
RUN chown -R $USERNAME:$USERNAME $ROS_WS && \
    chmod -R 775 $ROS_WS

COPY entrypoint.sh /entrypoint.sh

# -----------------------------------------------------------------------

FROM base AS prebuilt

WORKDIR $ROS_WS

# Install Python packages into system-wide location
RUN pip install --no-cache-dir \
    --target=/usr/local/lib/python3.10/site-packages ./scripts

# -----------------------------------------------------------------------

FROM base AS dev

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

ENTRYPOINT ["/entrypoint.sh"]

# -----------------------------------------------------------------------

FROM base AS runtime

# Copy artifacts/binaries from prebuilt
COPY --from=prebuilt /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

ENTRYPOINT ["/entrypoint.sh"]
