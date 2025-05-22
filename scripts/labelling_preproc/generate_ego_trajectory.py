#!/usr/bin/python3

import subprocess
import sys
from pathlib import Path

from labelling_preproc.common.utils import file_exists, directory_exists


class EgoTrajectoryGenerator:

    def __init__(self):
        pass

    def run_mola_lidar_odometry(self, rosbag_path, output_dir):
        """Run mola-lidar-odometry-cli with the given ROS bag file and output directory."""

        rosbag_path = Path(rosbag_path)
        output_dir = Path(output_dir)

        # Validate input file
        file_exists(rosbag_path)

        # Validate output directory
        directory_exists(output_dir)

        # Extract rosbag filename without extension
        rosbag_name = rosbag_path.stem

        # Define output file paths
        output_tum_path = output_dir / f"{rosbag_name}_trajectory.tum"
        output_map_path = output_dir / f"{rosbag_name}_map.simplemap"

        # Use default config file
        mola_config_path = "$(ros2 pkg prefix mola_lidar_odometry)/share/mola_lidar_odometry/pipelines/lidar3d-default.yaml"

        # Construct the command
        cmd = (
            f"export MOLA_USE_FIXED_LIDAR_POSE=true && "  # To avoid using lidar -> base_link TF
            f"export MOLA_MIN_XYZ_BETWEEN_MAP_UPDATES=0.0001 && "  # Ensure we get a pose for every lidar frame
            f"mola-lidar-odometry-cli "
            f"-c {mola_config_path} "
            f"--input-rosbag2 {rosbag_path} "
            f"--lidar-sensor-label /sensor/lidar/top/points "
            f"--output-tum-path {output_tum_path} "
            f"--output-simplemap {output_map_path}"
        )

        # Run the command
        try:
            print(f'⏳ Running ...\n')
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                text=True,
                capture_output=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f'💾 Trajectory filed saved as: {output_tum_path}\n')

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'❌ MOLA lidar-odometry cmd failed: {e}') from e


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: generate_ego_trajectory <path_to_rosbag.mcap> <output_directory>",
            file=sys.stderr,
        )
        sys.exit(1)

    rosbag_path = sys.argv[1]
    output_dir = sys.argv[2]

    try:
        ego_trajectory_generator = EgoTrajectoryGenerator()

        ego_trajectory_generator.run_mola_lidar_odometry(
            rosbag_path, output_dir
        )

    except Exception as e:
        print(f'Error: {e}')


if __name__ == "__main__":
    main()
