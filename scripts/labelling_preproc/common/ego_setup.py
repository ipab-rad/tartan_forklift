#!/usr/bin/python3
import warnings  # To ignore FutureWarning from pandas

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd


class EgoPoses:
    def __init__(self, tum_file_path):
        self.tum_file_path = tum_file_path
        # Define columns as given
        self.columns = ['timestamp', 'x', 'y', 'z', 'qx', 'qy', 'qz', 'qw']

        # Read the trajectory file using pandas
        self.df = pd.read_csv(
            self.tum_file_path,
            delim_whitespace=True,
            header=None,
            names=self.columns,
        )

        # Store the data into separate lists
        self.ego_x = self.df['x'].tolist()
        self.ego_y = self.df['y'].tolist()
        self.ego_z = self.df['z'].tolist()
        self.ego_qx = self.df['qx'].tolist()
        self.ego_qy = self.df['qy'].tolist()
        self.ego_qz = self.df['qz'].tolist()
        self.ego_qw = self.df['qw'].tolist()

    def getEgoPose(self, i):
        """Return a dictionary with the ego pose for the given index."""
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

    def validatePoseCount(self, key_frames_n):
        if len(self.ego_x) != key_frames_n:
            msg = f'Trajectory poses = {len(self.ego_x)} | Key frames = {key_frames_n}'
            return [False, msg]
        else:
            return [True, 'Ok']
