#!/usr/bin/python3
"""Module to create/upload a multi-sensor sequence to Segments.ai."""
import copy
import json
import sys
from pathlib import Path

from labelling_preproc.common.ego_setup import EgoPoses
from labelling_preproc.common.response import (
    PreprocessingError,
    PreprocessingResponse,
)
from labelling_preproc.common.sample_formats import (
    camera_ids_list,
    sensor_sequence_struct,
)
from labelling_preproc.common.segments_client_wrapper import (
    SegmentsClientWrapper,
)
from labelling_preproc.common.sensor_frame_creator import SensorFrameCreator
from labelling_preproc.common.utils import (
    directory_exists,
    file_exists,
    get_env_var,
)

import yaml


class SegmentsSampleCreator:
    """
    Provide an interface to create/upload a multi-sensor sequence.

    It mainly parses the exported data containing in the host directory
    and creates a multi-sensor sequence in a pre-existing Segments.ai dataset.
    This class uses the SegmentS3Client to interact with the Segments.ai API
    to upload a multi-sensor sequence.

    For more information about muli-sensor sequences, refer to:
    https://docs.segments.ai/reference/sample-types#multi-sensor-sequence
    """

    def __init__(self, segments_client: SegmentsClientWrapper):
        """Initialise class."""
        self.client = segments_client

    def add(
        self, dataset_name: str, sequence_name: str, local_data_directory: Path
    ) -> PreprocessingResponse:
        """
        Create and upload a multi-sensor sequence to a pre-existing dataset.

        This method read ROS exported data stored in `local_data_directory`,
        parses the metadata files using other sub-modules, and creates
        a multi-sensor sequence in the specified Segments.ai dataset with
        a defined sequence name.
        The method expects the following files to be present in the
        `local_data_directory`:
            - `export_metadata.yaml`: Contains metadata about the export.
            - `upload_metadata.json`: Contains information on where the data
                                      is stored in S3.
            - A `.tum` file: Contains the vehicle trajectory poses.

        Args:
            dataset_name : Name of the pre-existing dataset in Segments.ai
            sequence_name : Desired name for the sequence sample
            local_data_directory : Path to the data directory

        Raises:
            FileNotFoundError: If the provided directory or required files
                               do not exist.
            ValueError: If a .tum files is not found or if the number of
                        trajectory poses does not match the number of frames.

        Returns:
            PreprocessingResponse: An object indicating success or failure and
                                   their corresponding error messages

        """
        # Verify the dataset exists
        response = self.client.verify_dataset(dataset_name)
        if not response.ok:
            return response

        # Verify provided data directory
        # TODO: Handle possible exceptions for this function (#61)
        directory_exists(local_data_directory)

        # Load export_metadata.yaml
        export_metadata_file = local_data_directory / 'export_metadata.yaml'
        # TODO: Handle possible exceptions for this function (#61)
        file_exists(export_metadata_file)

        with open(export_metadata_file) as yaml_file:
            export_metadata_yaml = yaml.safe_load(yaml_file)

        # Load upload_metadata.yaml
        upload_metadata_file = local_data_directory / 'upload_metadata.json'
        # TODO: Handle possible exceptions for this function (#61)
        file_exists(upload_metadata_file)

        with open(upload_metadata_file) as json_file:
            upload_metadata_json = json.load(json_file)
            assets_meta = upload_metadata_json['assets_ids']

        # Search for a .tum file
        tum_files = list(local_data_directory.glob('*.tum'))
        if not tum_files:
            raise ValueError('Trajectory file (.tum ) not found.')

        # Initialise ego_poses based on .tum file
        ego_poses = EgoPoses(tum_files[0])

        sync_key_frames = export_metadata_yaml.get('time_sync_groups', [])

        # Verify that the number of trajectory poses matches the
        #   number of key frames
        [ok, msg] = ego_poses.validatePoseCount(len(sync_key_frames))

        if not ok:
            msg = (
                'The number of poses is not equal to the number of key frames'
                f'\n{msg}\n'
            )
            response = PreprocessingResponse(
                ok=False,
                error=PreprocessingError.InvalidPoseCountError,
                error_message=msg,
            )
            return response
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

        # TODO: Use logger (#58)
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

        # TODO: Use logger (#58)
        # Upload sequence sample
        print('Uploading sample ...')

        response = self.client.add_sample(
            dataset_name, sequence_name, multi_sensor_sequence
        )

        # TODO: Use logger (#58)
        if response.ok:
            print('Done ✅')

        return response


def main():
    """
    Entry point for the script.

    Parses command-line arguments and runs the SegmentsSampleCreator.
    """
    # Ensure command-line argument is provided
    if len(sys.argv) < 4:
        print(
            'ERROR: Please provide the required arguments\n'
            'add_segmentsai_sample '
            '<dataset_name> <sequence_name> <data_directory>',
            file=sys.stderr,
        )
        sys.exit(1)

    # Mandatory arguments
    dataset_name = sys.argv[1]
    sequence_name = sys.argv[2]
    data_directory = Path(sys.argv[3])

    # Get Segment API key from env variable
    api_key = get_env_var('SEGMENTS_API_KEY')
    segments_client_wrapper = SegmentsClientWrapper(api_key)

    sample_creator = SegmentsSampleCreator(segments_client_wrapper)
    response = sample_creator.add(dataset_name, sequence_name, data_directory)

    if not response.ok:
        print(
            f'An {str(response.error)} happened when adding '
            f'{sequence_name} sample. '
            f'Erro details: \n{response.error_message}'
        )


if __name__ == '__main__':
    main()
