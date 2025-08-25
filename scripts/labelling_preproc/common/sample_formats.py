"""
Module to define Segments.ai sample formats.

For more information the sample types/structs used here please refer to:
    https://docs.segments.ai/reference/sample-types
"""

# Sensor sequence dictionary structure
sensor_sequence_struct = {'name': '', 'task_type': '', 'attributes': {}}

# Cameras' id list
# Do not modify unless you know what you are doing!
camera_ids_list = [
    'fsp_l',
    'fsp_r',
    'rsp_l',
    'rsp_r',
    'lspf_r',
    'lspr_l',
    'rspf_l',
    'rspr_r',
]

# Cameras' position in Segments.ai grid
camera_grid_positions = {
    'fsp_l': {'row': 0, 'col': 1},
    'fsp_r': {'row': 0, 'col': 2},
    'rsp_l': {'row': 1, 'col': 1},
    'rsp_r': {'row': 1, 'col': 2},
    'lspf_r': {'row': 0, 'col': 0},
    'lspr_l': {'row': 1, 'col': 0},
    'rspf_l': {'row': 0, 'col': 3},
    'rspr_r': {'row': 1, 'col': 3},
}

# Image sample dictionary structure
image_struct = {'image': {'url': ''}, 'name': ''}

# Pointcloud sample dictionary structure
pcd_struct = {
    'pcd': {
        'url': 'pcd_url',
        'type': 'pcd',
    },
    'images': [],
    'ego_pose': {
        'position': {
            'x': 0,
            'y': 0,
            'z': 0,
        },
        'heading': {
            'qx': 0,
            'qy': 0,
            'qz': 0,
            'qw': 1,
        },
    },
    'default_z': 0,
    'name': 'test',
    'timestamp': 0,
}

camera_image_struct = {
    'url': 'image_url',
    'row': 0,  # Row when displaying multiple camera images
    'col': 0,  # Col when displaying multiple camera images
    'intrinsics': {
        'intrinsic_matrix': [
            [0, 0, 0],
            [0, 0, 0],
            [0, 0, 0],
        ]
    },
    'extrinsics': {
        'translation': {'x': 0, 'y': 0, 'z': 0},
        'rotation': {'qx': 0, 'qy': 0, 'qz': 0, 'qw': 0},
    },
    'distortion': {
        'model': 'brown-conrady',
        'coefficients': {
            'k1': 0,
            'k2': 0,
            'k3': 0,
            'p1': 0,
            'p2': 0,
        },
    },
    'camera_convention': 'OpenCV',
    'name': 'camera_fsp_l',
}
