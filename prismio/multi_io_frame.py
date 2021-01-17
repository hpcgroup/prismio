# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

"""
The prismio.multi_io_frame module aims to provide functionalities of analysing
and comparing multiple runs. 

"""


import sys
import numpy as np
import pandas as pd
from io_frame import IOFrame


class MultiIOFrame():
    io_frames = []
    
    def __init__(self, io_frames):
        """
        Args:
            io_frames (list of io_frame objects): io_frames for all runs the user wants to compare.

        Return:
            None.

        """
        self.io_frames = io_frames
        self.set_log_dirs()
        self.num_processes = io_frames[0].num_processes

    def set_log_dirs(self):
        """
        Put log_dirs of all io_frames to a list.

        Args:
            None.

        Return:
            None.

        """
        self.log_dirs = []
        for io_frame in self.io_frames:
            self.log_dirs.append(io_frame.log_dir)
