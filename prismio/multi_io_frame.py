# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

"""
The prismio.multi_io_frame module aims to provide functionalities of analysing
and comparing multiple runs. 

"""


import sys
import glob
import numpy as np
import pandas as pd
from io_frame import IOFrame


class MultiIOFrame():
    io_frames = []
    
    def __init__(self, directories):
        """
        Args:
            directories (list or str): a list of tracing directories or a root directory that contains tracing directories.

        Return:
            None.

        """
        if type(directories) is not list and type(directories) is not str:
            sys.stderr.write("error: please pass in a root directory or a list of tracing directories\n")
            return
        
        if type(directories) is str:
            directories = glob.glob(directories + "/*")

        self.io_frames = {}
        for directory in directories:
            self.io_frames[directory] = IOFrame.from_recorder(directory)
