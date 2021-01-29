# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

"""
The prismio.reader.recorder_reader module provides functions for processing tracing data
from Recorder, for example, sorting records, finding the file name each record operates.
With data processing, it organize the data to a dataframe, and create the IOFrame for
recorder tracing files.

"""


import sys
import os
from csv import writer
import numpy as np
import pandas as pd
from prismio.io_frame import IOFrame
import recorder_viz
import time

class RecorderReader:
    """
    The reader class for recorder data. It can read in recorder trace files, 
    preprocess the data, and create a corresponding IOFrame.
    """
    def __init__(self, log_dir):
        """
        Use the Recorder creader_wrapper to read in tracing data.

        Args:
            log_dir (string): path to the trace files directory of Recorder the user wants to analyze.

        Return:
            None.

        """
        self.log_dir = log_dir
        self.reader = recorder_viz.RecorderReader(log_dir)
    
    def find_file_names(self, dataframe):
        """
        Figure out the file name each record accesses. Assign that to each record.
        So after this function, all records should have the correct file name.

        Args:
            dataframe (DataFrame): dataframe sorted by rank then tstart.

        Return:
            None.

        """
        fd_to_file_names = [{0: "stdin", 1: "stdout", 2: "stderr"}] * self.reader.GM.total_ranks
        filenames = []
        
        for index, row in dataframe.iterrows():
            rank = row['rank']
            function_args = row['args']
            fd_to_file_name = fd_to_file_names[rank]
            func_name = row['name']
            if 'fdopen' in func_name:
                fd = row['return_value']
                old_fd = int(function_args[0])
                if old_fd not in fd_to_file_name: 
                    filenames.append('__unknown__')
                else:
                    file_name = fd_to_file_name[old_fd]
                    fd_to_file_name[fd] = file_name
                    filenames.append(file_name)
            elif 'fopen' in func_name or 'open' in func_name:
                fd = row['return_value']
                file_name = function_args[0]
                fd_to_file_name[fd] = file_name
                filenames.append(file_name)
            elif 'fwrite' in func_name or 'fread' in func_name:
                fd = int(function_args[3])
                if fd not in fd_to_file_name:
                    filenames.append('__unknown__')
                else: 
                    file_name = fd_to_file_name[fd]
                    filenames.append(file_name)
            elif 'seek' in func_name or 'close' in func_name or 'sync' in func_name or 'writev' in func_name or 'readv' in func_name or 'pwrite' in func_name or 'pread' in func_name or 'write' in func_name or 'read' in func_name or 'fprintf' in func_name:
                fd = int(function_args[0])
                if fd not in fd_to_file_name:
                    filenames.append('__unknown__')
                else: 
                    file_name = fd_to_file_name[fd]
                    filenames.append(file_name)
            else:
                filenames.append(None)
        return filenames
        
    def read(self):
        """
        Call sort_records and then find_file_names. After it has all information needed,
        it creates the dataframe row by row. Then create an IOFrame with this dataframe. 

        Args:
            None.

        Return:
            An IOFrame created by trace files of recorder specified by the log_dir of this RecorderReader.

        """
        dic = {
            'rank': [], 
            'fid': [], 
            'name': [], 
            'tstart': [],
            'tend': [],
            'time': [], 
            'arg_counts': [],
            'args': [],
            'return_value': []
        }
        for rank in range(self.reader.GM.total_ranks):
            for record_index in range(self.reader.LMs[rank].total_records):
                record = self.reader.records[rank][record_index]
                dic['rank'].append(rank)
                dic['fid'].append(record.func_id)
                dic['name'].append(self.reader.funcs[record.func_id])
                dic['tstart'].append(record.tstart)
                dic['tend'].append(record.tend)
                dic['time'].append(record.tend - record.tstart)
                dic['arg_counts'].append(record.arg_count)
                dic['args'].append(record.args_to_strs())
                dic['return_value'].append(record.res)

        dataframe = pd.DataFrame.from_dict(dic)
        dataframe = dataframe.sort_values(['rank', 'tstart'])
        filenames = self.find_file_names(dataframe)
        dataframe['file'] = filenames

        return IOFrame(dataframe)
