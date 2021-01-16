# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys
import numpy as np
import pandas as pd
from io_frame import IOFrame


class MultiIOFrame():
    io_frames = []
    
    def __init__(self, io_frames):
        self.io_frames = io_frames
        self.set_log_dirs()
        self.np = io_frames[0].np

    def set_log_dirs(self):
        self.log_dirs = []
        for rdf in self.io_frames:
            self.log_dirs.append(rdf.log_dir)

    def get_log_dirs(self):
        return self.log_dirs
