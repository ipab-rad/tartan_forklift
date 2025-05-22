#!/usr/bin/python3

import sys
import copy
import yaml
import json
from pathlib import Path

from labelling_preproc.common.ego_setup import EgoPoses
from labelling_preproc.common.sample_formats import (
    camera_ids_list,
    sensor_sequence_struct,
)
from labelling_preproc.common.sensor_frame_ceator import SensorFrameCreator
from labelling_preproc.common.s3_client import SegmentS3Client
from labelling_preproc.common.utils import (
    get_env_var,
    file_exists,
    directory_exists,
)


class SegmentsSampleCreator:
    def __init__(self):

        # Get Segment API key from env variable
        api_key = get_env_var('SEGMENTS_API_KEY')

        # Initialise Segments.ai client
        self.client = SegmentS3Client(api_key)

    def add(
        self, dataset_name: str, sequence_name: str, local_data_directory: Path
    ):

        # Verify the dataset exists
        self.client.verify_dataset(dataset_name)

        # Verify provided data directory
        directory_exists(local_data_directory)

        # Load export_metadata.yaml
        export_metadata_file = local_data_directory / 'export_metadata.yaml'
        file_exists(export_metadata_file)

        with open(export_metadata_file) as yaml_file:
            export_metadata_yaml = yaml.safe_load(yaml_file)

        # Load upload_metadata.yaml
        upload_metadata_file = local_data_directory / 'upload_metadata.json'
        file_exists(upload_metadata_file)

        with open(upload_metadata_file) as json_file:
            upload_metadata_json = json.load(json_file)
            assets_meta = upload_metadata_json['assets_ids']

        # Search for a .tum file
        tum_files = list(local_data_directory.glob('*.tum'))
        if not tum_files:
            raise ValueError(f'Trajectory file (.tum ) not found.')

        # Initialise ego_poses based on .tum file
        ego_poses = EgoPoses(tum_files[0])

        sync_key_frames = export_metadata_yaml.get('time_sync_groups', [])

        # Verify that the number of trajectory poses matches the number of key frames
        [ok, msg] = ego_poses.validatePoseCount(len(sync_key_frames))

        if not ok:
            raise ValueError(
                f'The number of poses is not equal to the number of key frames.\n'
                f'{msg}\n'
            )

        # Initialise sensors' frames lists
        pointcloud_frames = []
        cameras_frames = {}
        for cam_id in camera_ids_list:
            cameras_frames[cam_id] = []

        # Use the first sync frame to get information about the cameras
        cameras_info = sync_key_frames[0]['cameras']
        self.frame_creator = SensorFrameCreator(
            local_data_directory, cameras_info
        )

        print('Creating sensor sequences samples...')
        # Iterate over synchronised key frames
        for idx, sync_key_frame in enumerate(sync_key_frames):
            # Create pointcloud frame
            pointcloud_frame = self.frame_creator.create_3dpointcloud_frame(
                idx, sync_key_frame, assets_meta, ego_poses
            )
            pointcloud_frames.append(copy.deepcopy(pointcloud_frame))

            # Create an image frame per camera
            for cam_meta in sync_key_frame['cameras']:
                image_frame = self.frame_creator.create_image_frame(
                    idx, cam_meta, assets_meta
                )
                cameras_frames[cam_meta['name']].append(
                    copy.deepcopy(image_frame)
                )

        # Create mulit-sensor sequence
        multi_sensor_sequence = {'sensors': []}

        lidar_sequence = sensor_sequence_struct
        lidar_sequence['name'] = 'lidar top'
        lidar_sequence['task_type'] = 'pointcloud-cuboid-sequence'
        lidar_sequence['attributes'] = {'frames': pointcloud_frames}
        multi_sensor_sequence['sensors'].append(copy.deepcopy(lidar_sequence))

        for cam_name, image_frames in cameras_frames.items():
            camera_sequence = sensor_sequence_struct
            camera_sequence['name'] = 'camera_' + cam_name
            camera_sequence['task_type'] = 'image-vector-sequence'
            camera_sequence['attributes'] = {'frames': image_frames}
            multi_sensor_sequence['sensors'].append(
                copy.deepcopy(camera_sequence)
            )

        # Save multi-sensor sequence as JSON
        multi_sensor_sequence_file = (
            local_data_directory / 'multi_sensor_sequence.json'
        )

        with multi_sensor_sequence_file.open('w') as outfile:
            json.dump(multi_sensor_sequence, outfile, indent=4)

        # Upload sequence sample
        print('Uploading sample ...')

        self.client.add_sample(
            dataset_name, sequence_name, multi_sensor_sequence
        )

        print('Done ✅')


def main():
    # Ensure command-line argument is provided
    if len(sys.argv) < 4:
        print(
            'ERROR: Please provide the required arguments\n'
            'add_segmentsai_sample <dataset_name> <sequence_name> <data_directory>',
            file=sys.stderr,
        )
        sys.exit(1)

    # Mandatory arguments
    dataset_name = sys.argv[1]
    sequence_name = sys.argv[2]
    data_directory = Path(sys.argv[3])

    sample_creator = SegmentsSampleCreator()
    sample_creator.add(dataset_name, sequence_name, data_directory)


if __name__ == '__main__':
    main()
