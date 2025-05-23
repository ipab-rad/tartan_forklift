#!/usr/bin/python3
"""Ego setup module to read and process ego poses from a .tum file."""

import warnings  # To ignore FutureWarning from pandas

import pandas as pd

warnings.simplefilter(action='ignore', category=FutureWarning)


class EgoPoses:
    """Class to parse ego poses from a .tum file."""

    def __init__(self, tum_file_path: str) -> None:
        """
        Initialise the class with the path to the .tum file.

        Args:
            tum_file_path: Path to the .tum trajectory file.
        """
        self.tum_file_path = tum_file_path
        self.columns = ['timestamp', 'x', 'y', 'z', 'qx', 'qy', 'qz', 'qw']

        self.df = pd.read_csv(
            self.tum_file_path,
            delim_whitespace=True,
            header=None,
            names=self.columns,
        )

        self.ego_x = self.df['x'].tolist()
        self.ego_y = self.df['y'].tolist()
        self.ego_z = self.df['z'].tolist()
        self.ego_qx = self.df['qx'].tolist()
        self.ego_qy = self.df['qy'].tolist()
        self.ego_qz = self.df['qz'].tolist()
        self.ego_qw = self.df['qw'].tolist()

    def getEgoPose(self, i: int) -> dict:
        """
        Return a dictionary with the ego pose for the given index.

        Args:
            i: Index of the desired pose.

        Returns:
            A dictionary with keys 'position' and 'heading', each containing
            the respective coordinates or quaternion values.
        """
        ego_pose = {
            'position': {
                'x': self.ego_x[i],
                'y': self.ego_y[i],
                'z': self.ego_z[i],
            },
            'heading': {
                'qx': self.ego_qx[i],
                'qy': self.ego_qy[i],
                'qz': self.ego_qz[i],
                'qw': self.ego_qw[i],
            },
        }
        return ego_pose

    def validatePoseCount(self, key_frames_n: int) -> list[bool, str]:
        """
        Validate the number of poses against the number of key frames.

        Args:
            key_frames_n: Expected number of key frames.

        Returns:
            A list containing a boolean and a message:
            - True and 'Ok' if the counts match.
            - False and an error message if they do not.
        """
        if len(self.ego_x) != key_frames_n:
            msg = (
                f'Trajectory poses = {len(self.ego_x)}'
                f' | Key frames = {key_frames_n}'
            )
            return [False, msg]
        else:
            return [True, 'Ok']
