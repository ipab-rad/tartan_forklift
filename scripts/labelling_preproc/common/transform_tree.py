"""
Module to parse a YAML transform tree and compute transforms.

Implements a TransformTree class that replicates the behaviour
of the ROS 2 TF tree.
"""

from collections import defaultdict

import numpy as np

from scipy.spatial.transform import Rotation as R

import yaml


class Transform:
    """Represents a rigid homogeneous transformation matrix."""

    def __init__(self, matrix: np.ndarray):
        """
        Initialise the Transform object with a 4x4 matrix.

        Args:
            matrix: 4x4 numpy array representing the transformation.
        """
        self._matrix = matrix
        self.x, self.y, self.z = matrix[:3, 3]

        rot = R.from_matrix(matrix[:3, :3])
        self.qx, self.qy, self.qz, self.qw = rot.as_quat()

    def matrix(self) -> np.ndarray:
        """
        Return the 4x4 homogeneous transformation matrix.

        Returns:
            A numpy 4x4 array.
        """
        return self._matrix


class TransformTree:
    """
    Parses a YAML transform tree and resolves frame-to-frame transforms.

    This class replicates ROS 2 TF tree behaviour for querying transforms
    between any two connected frames in a static transform graph.
    """

    def __init__(self, yaml_file_path: str) -> None:
        """
        Load and parse the YAML file containing transforms.

        Args:
            yaml_file_path: Path to the YAML file.
        """
        with open(yaml_file_path) as f:
            data = yaml.safe_load(f)

        self.transforms = {}
        self.children = defaultdict(list)
        self.parents = {}

        for t in data['transforms']:
            parent = t['parent_frame']
            child = t['child_frame']
            tf = t['transform']
            mat = self._transform_to_matrix(tf)

            self.transforms[(parent, child)] = mat
            self.transforms[(child, parent)] = np.linalg.inv(mat)

            self.children[parent].append(child)
            self.parents[child] = parent

    def _transform_to_matrix(self, tf: dict) -> np.ndarray:
        """
        Convert a translation + quaternion dictionary into a 4x4 matrix.

        Args:
            tf: Dictionary with keys x, y, z, qx, qy, qz, qw.

        Returns:
            A 4x4 numpy array representing the transform.
        """
        trans = np.array([tf['x'], tf['y'], tf['z']])
        quat = np.array([tf['qx'], tf['qy'], tf['qz'], tf['qw']])
        rot = R.from_quat(quat).as_matrix()
        mat = np.eye(4)
        mat[:3, :3] = rot
        mat[:3, 3] = trans
        return mat

    def _path_to_root(self, frame: str) -> list[str]:
        """
        Return the path from the root to the given frame.

        Args:
            frame: Frame name to trace upwards.

        Returns:
            A list of frame names from the root to the input frame.

        Raises:
            KeyError: If the frame is not found in the tree.
        """
        if frame not in self.parents and frame not in self.children:
            raise KeyError(f"Frame '{frame}' is not in the tree.")

        path = [frame]
        current = frame
        while current in self.parents:
            current = self.parents[current]
            path.append(current)
        return list(reversed(path))

    def _find_common_ancestor(
        self, path_a: list[str], path_b: list[str]
    ) -> tuple[str, int, int]:
        """
        Find the deepest shared ancestor between two transformation paths.

        Args:
            path_a: List of frames from root to target_frame.
            path_b: List of frames from root to source_frame.

        Returns:
            A tuple (common_frame, index_in_a, index_in_b).

        Raises:
            RuntimeError: If there is no shared ancestor.
        """
        min_len = min(len(path_a), len(path_b))
        last_common = None
        for i in range(min_len):
            if path_a[i] != path_b[i]:
                break
            last_common = (path_a[i], i, i)

        if last_common is None:
            raise RuntimeError('No common ancestor found.')

        return last_common

    def get_transform(self, target_frame: str, source_frame: str) -> Transform:
        """
        Compute the transform from source_frame to target_frame.

        Args:
            target_frame: Name of the target frame.
            source_frame: Name of the source frame.

        Returns:
            A Transform object representing the transformation.

        Raises:
            KeyError: If the frames are not connected or unknown.
        """
        if target_frame == source_frame:
            return Transform(np.eye(4))

        path_a = self._path_to_root(target_frame)
        path_b = self._path_to_root(source_frame)

        common_frame, index_a, index_b = self._find_common_ancestor(
            path_a, path_b
        )

        tf_common_to_a = np.eye(4)
        for i in range(index_a, len(path_a) - 1):
            parent = path_a[i + 1]
            child = path_a[i]
            tf_common_to_a = self.transforms[(parent, child)] @ tf_common_to_a

        tf_b_to_common = np.eye(4)
        for i in range(len(path_b) - 1, index_b, -1):
            parent = path_b[i - 1]
            child = path_b[i]
            tf_b_to_common = self.transforms[(parent, child)] @ tf_b_to_common

        return Transform(tf_common_to_a @ tf_b_to_common)
