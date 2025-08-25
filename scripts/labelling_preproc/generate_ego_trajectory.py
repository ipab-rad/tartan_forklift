#!/usr/bin/python3
"""A module to generate an ego trajectory from a rosbag file."""

import subprocess
import sys
from pathlib import Path

from labelling_preproc.common.utils import directory_exists, file_exists


class EgoTrajectoryGenerator:
    """
    A wrapper for the MOLA lidar-odometry CLI.

    Refer to the Modular Optimisation for Localisation and Mapping (MOLA)
    documentation for more details:
    https://docs.mola-slam.org/latest/building-maps.html
    """

    def run_mola_lidar_odometry(
        self, rosbag_path: str, output_dir: str
    ) -> None:
        """
        Run the mola-lidar-odometry CLI on a given rosbag file.

        The generated files will be saved in the given output directory,
        using the same name as the rosbag file.

        Args:
            rosbag_path: Path to the input rosbag file (.mcap).
            output_dir: Directory where the output files should be saved.

        Raises:
            RuntimeError: If the MOLA command fails during execution.
        """
        rosbag_path = Path(rosbag_path)
        output_dir = Path(output_dir)

        # Ensure the input rosbag file exists
        file_exists(rosbag_path)

        # Ensure the output directory exists
        directory_exists(output_dir)

        # Extract the rosbag filename without extension
        rosbag_name = rosbag_path.stem

        # Define the paths for the output files
        output_tum_path = output_dir / f'{rosbag_name}_trajectory.tum'
        output_map_path = output_dir / f'{rosbag_name}_map.simplemap'

        # Use the default MOLA config file
        mola_config_path = (
            '$(ros2 pkg prefix mola_lidar_odometry)/share/'
            'mola_lidar_odometry/pipelines/lidar3d-default.yaml'
        )

        # Construct the CLI command
        cmd = (
            # Avoid relying on the lidar -> base_link transform
            'export MOLA_USE_FIXED_LIDAR_POSE=true && '
            'export MOLA_IMU_NAME=/sensor/imu/front/data && '
            # Ensure a pose is generated for every lidar frame
            'export MOLA_MIN_XYZ_BETWEEN_MAP_UPDATES=0.0001 && '
            'export MOLA_MIN_ROT_BETWEEN_MAP_UPDATES=0.0001 && '
            'export MOLA_MINIMUM_ICP_QUALITY=0.1 && '
            'export MOLA_MAP_CLOUD_DECIMATION=0.005 && '
            'mola-lidar-odometry-cli '
            f'-c {mola_config_path} '
            f'--input-rosbag2 {rosbag_path} '
            '--lidar-sensor-label /sensor/lidar/top/points '
            f'--output-tum-path {output_tum_path} '
            f'--output-simplemap {output_map_path}'
        )

        # Run the command
        try:
            print('⏳ Running ...\n')
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                text=True,
                capture_output=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f'💾 Trajectory file saved as: {output_tum_path}\n')

        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f'❌ MOLA lidar-odometry cmd failed: {e}'
            ) from e


def main():
    """
    Entry point for the script.

    Parses command-line arguments and runs the EgoTrajectoryGenerator.
    """
    if len(sys.argv) != 3:
        print(
            'Usage: '
            'generate_ego_trajectory <path_to_rosbag.mcap> <output_directory>',
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

    except Exception as e:  # noqa: BLE001
        print(f'Error: {e}')


if __name__ == '__main__':
    main()
