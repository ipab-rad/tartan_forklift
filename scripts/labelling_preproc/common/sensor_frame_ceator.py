#!/usr/bin/python3
import copy

from dataclasses import dataclass
from pathlib import Path


from labelling_preproc.common.camera_calibration_parser import (
    CameraCalibrationParser,
    CameraCalibrationData,
)
from labelling_preproc.common.transform_tree import TransformTree, Transform

from labelling_preproc.common.sample_formats import (
    image_struct,
    pcd_struct,
    camera_image_struct,
    camera_grid_positions,
)

from labelling_preproc.common.utils import file_exists


@dataclass
class CameraData:
    """
    Holds camera calibration and extrinsic transform data.
    """

    calibration_data: CameraCalibrationData
    extrinsics: Transform


class SensorFrameCreator:

    def __init__(self, data_directory: Path, cameras_info: list):
        self.data_directory = data_directory
        # Based on vehicle's TF tree
        self.GROUND_Z_OFFSET_BELOW_LIDAR_M = -1.78

        # Load the transform tree from the transforms.yaml file
        transforms_file = data_directory / 'extrinsics/transforms.yaml'
        file_exists(transforms_file)
        self.transform_tree = TransformTree(str(transforms_file))
        self.camera_calibration_parser = CameraCalibrationParser()
        self.cameras_data = {}

        self.LIDAR_FRAME_ID = 'lidar_ouster_top'

        self.get_cameras_calibration(cameras_info)

    def get_cameras_calibration(self, cameras_info: list):

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
        self, idx, sync_key_frame, assets_meta, ego_poses
    ):
        # Initialise frame with the template struct
        pointcloud_frame = pcd_struct

        # Set LIDAR url
        lidar_asset_id = str(sync_key_frame['lidar']['global_id'])
        pointcloud_frame['pcd']['url'] = assets_meta[lidar_asset_id]['s3_url']

        # Set frame timestamp
        total_nanosec = (
            sync_key_frame['stamp']['sec'] * (10**9)
            + sync_key_frame['stamp']['nanosec']
        )
        pointcloud_frame['timestamp'] = str(total_nanosec)

        # Set frame name based on index
        pointcloud_frame['name'] = 'frame_' + str(idx)

        # Get and set ego pose based on index
        pointcloud_frame['ego_pose'] = ego_poses.getEgoPose(idx)

        # Get and Set images based on metadata
        pointcloud_frame['images'] = self.get_images(
            sync_key_frame, assets_meta
        )

        # Set ground height offset relative to the lidar
        pointcloud_frame['default_z'] = self.GROUND_Z_OFFSET_BELOW_LIDAR_M

        return pointcloud_frame

    def create_image_frame(self, idx, cam_meta, assets_meta):
        image_frame = image_struct
        # Get url based on camera's image global id
        img_asset_id = str(cam_meta['global_id'])
        url = assets_meta[img_asset_id]['s3_url']
        image_frame['image']['url'] = url
        image_frame['name'] = 'frame_' + str(idx)

        return image_frame

    def get_images(self, sync_key_frame, assets_meta):
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
            camera_image['name'] = "camera_" + name

            images.append(copy.deepcopy(camera_image))

        return images
