#!/usr/bin/python3
"""Sensor frame creator module to create Segments.ai samples and keyframes."""

import copy
from dataclasses import dataclass
from pathlib import Path

from labelling_preproc.common.camera_calibration_parser import (
    CameraCalibrationData,
    CameraCalibrationParser,
)
from labelling_preproc.common.ego_setup import EgoPoses
from labelling_preproc.common.sample_formats import (
    camera_grid_positions,
    camera_image_struct,
    image_struct,
    pcd_struct,
)
from labelling_preproc.common.transform_tree import Transform, TransformTree
from labelling_preproc.common.utils import file_exists


@dataclass
class CameraData:
    """Holds camera calibration and extrinsic transform data."""

    calibration_data: CameraCalibrationData
    extrinsics: Transform


class SensorFrameCreator:
    """
    Class to create data points (keyframes) for the Segments.ai sample format.

    This class is used to create different main and sub sample formats based
    on the sensor data and metadata from a directory.

    For more information on the sample types/format used here please refer to:
    https://docs.segments.ai/reference/sample-types
    """

    def __init__(self, data_directory: Path, cameras_info: list):
        """
        Initialise the class with the data directory and camera info.

        Args:
            data_directory: Path to the directory containing calibration and
                            metadata files.
            cameras_info: List of dictionaries containing camera metadata.
        """
        self.data_directory = data_directory
        self.GROUND_Z_OFFSET_BELOW_LIDAR_M = -1.78

        transforms_file = data_directory / 'extrinsics/transforms.yaml'
        file_exists(transforms_file)
        self.transform_tree = TransformTree(str(transforms_file))
        self.camera_calibration_parser = CameraCalibrationParser()
        self.cameras_data = {}

        self.LIDAR_FRAME_ID = 'lidar_top'

        self.get_cameras_calibration(cameras_info)

    def get_cameras_calibration(self, cameras_info: list):
        """
        Get camera calibration data and extrinsics.

        Uses the camera list to read calibration files and extract extrinsics
        relative to LIDAR_FRAME_ID.

        Args:
            cameras_info: List of dictionaries containing camera metadata.
        """
        for camera in cameras_info:
            camera_name = camera['name']
            calibration_file = (
                self.data_directory
                / 'camera'
                / camera_name
                / 'camera_calibration.yaml'
            )
            calibration_data = (
                self.camera_calibration_parser.get_camera_calibration(
                    str(calibration_file)
                )
            )
            transform = self.transform_tree.get_transform(
                self.LIDAR_FRAME_ID, calibration_data.frame_id
            )
            self.cameras_data[camera_name] = CameraData(
                calibration_data=calibration_data, extrinsics=transform
            )

    def create_3dpointcloud_frame(
        self,
        idx: int,
        sync_key_frame: dict,
        assets_meta: dict,
        ego_poses: EgoPoses,
    ):
        """
        Create a 3D point cloud frame based on synchronised sensor data.

        Args:
            idx: Index of the frame in the sequence.
            sync_key_frame: Dictionary containing synchronised frame metadata.
            assets_meta: Dictionary mapping asset IDs to their metadata.
            ego_poses: An EgoPoses object providing pose data.

        Returns:
            A dictionary representing a Segments.ai 3D point cloud sample.
        """
        pointcloud_frame = pcd_struct
        lidar_asset_id = str(sync_key_frame['lidar']['global_id'])
        pointcloud_frame['pcd']['url'] = assets_meta[lidar_asset_id]['s3_url']

        total_nanosec = (
            sync_key_frame['stamp']['sec'] * (10**9)
            + sync_key_frame['stamp']['nanosec']
        )
        pointcloud_frame['timestamp'] = str(total_nanosec)
        pointcloud_frame['name'] = 'frame_' + str(idx)
        pointcloud_frame['ego_pose'] = ego_poses.getEgoPose(idx)
        pointcloud_frame['images'] = self.get_images(
            sync_key_frame, assets_meta
        )
        pointcloud_frame['default_z'] = self.GROUND_Z_OFFSET_BELOW_LIDAR_M

        return pointcloud_frame

    def create_image_frame(self, idx, cam_meta, assets_meta):
        """
        Create an image sample format for a single camera.

        Args:
            idx: Index of the frame.
            cam_meta: Metadata dictionary for the camera.
            assets_meta: Dictionary mapping asset IDs to their metadata.

        Returns:
            A dictionary representing a Segments.ai image sample.
        """
        image_frame = image_struct
        img_asset_id = str(cam_meta['global_id'])
        url = assets_meta[img_asset_id]['s3_url']
        image_frame['image']['url'] = url
        image_frame['name'] = 'frame_' + str(idx)

        return image_frame

    def get_images(self, sync_key_frame, assets_meta):
        """
        Create a list of camera image sample formats.

        Args:
            sync_key_frame: Dictionary containing synchronised frame metadata.
            assets_meta: Dictionary mapping asset IDs to their S3 metadata.

        Returns:
            A list of dictionaries, each representing a camera image sample.
        """
        images = []
        for cam in sync_key_frame['cameras']:
            name = cam['name']
            global_id = cam['global_id']
            camera_image = camera_image_struct
            camera_image['url'] = assets_meta[str(global_id)]['s3_url']
            camera_image['row'] = camera_grid_positions[name]['row']
            camera_image['col'] = camera_grid_positions[name]['col']

            intrinsics = self.cameras_data[name].calibration_data.intrinsics
            camera_image['intrinsics']['intrinsic_matrix'] = [
                [intrinsics.fx, 0.0, intrinsics.cx],
                [0.0, intrinsics.fy, intrinsics.cy],
                [0.0, 0.0, 1.0],
            ]

            tf = self.cameras_data[name].extrinsics
            camera_image['extrinsics']['translation'] = {
                'x': tf.x,
                'y': tf.y,
                'z': tf.z,
            }
            camera_image['extrinsics']['rotation'] = {
                'qx': tf.qx,
                'qy': tf.qy,
                'qz': tf.qz,
                'qw': tf.qw,
            }

            distortion = self.cameras_data[name].calibration_data.distortion
            camera_image['distortion']['model'] = distortion.model
            camera_image['distortion']['coefficients'] = {
                'k1': distortion.k1,
                'k2': distortion.k2,
                'k3': distortion.k3,
                'p1': distortion.p1,
                'p2': distortion.p2,
            }

            camera_image['camera_convention'] = 'OpenCV'
            camera_image['name'] = 'camera_' + name

            images.append(copy.deepcopy(camera_image))

        return images
