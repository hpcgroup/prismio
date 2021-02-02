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
        self.reader = recorder_viz.RecorderReader(log_dir)    
        
    def read(self):
        """
        Call sort_records and then find_filenames. After it has all information needed,
        it creates the dataframe row by row. Then create an IOFrame with this dataframe. 

        Args:
            None.

        Return:
            An IOFrame created by trace files of recorder specified by the log_dir of this RecorderReader.

        """
        all_records = []
        for rank in range(self.reader.GM.total_ranks):
            per_rank_records = []
            for record_index in range(self.reader.LMs[rank].total_records):
                per_rank_records.append(self.reader.records[rank][record_index])
            per_rank_records = sorted(per_rank_records, key=lambda x: x.tstart)
            all_records.append(per_rank_records)

        records_as_dict = {
            'rank': [], 
            'fid': [], 
            'name': [], 
            'tstart': [],
            'tend': [],
            'time': [], 
            'arg_count': [],
            'args': [],
            'return_value': [],
            'file': []
        }

        fd_to_filenames = [{0: "stdin", 1: "stdout", 2: "stderr"}] * self.reader.GM.total_ranks
        
        for rank in range(self.reader.GM.total_ranks):
            for record in all_records[rank]:
                fd_to_filename = fd_to_filenames[rank]
                function_args = record.args_to_strs()
                func_name = self.reader.funcs[record.func_id]
                if 'fdopen' in func_name:
                    fd = record.res
                    old_fd = int(function_args[0])
                    if old_fd not in fd_to_filename: 
                        filename = '__unknown__'
                    else:
                        filename = fd_to_filename[old_fd]
                        fd_to_filename[fd] = filename
                elif 'fopen' in func_name or 'open' in func_name:
                    fd = record.res
                    filename = function_args[0]
                    fd_to_filename[fd] = filename
                elif 'fwrite' in func_name or 'fread' in func_name:
                    fd = int(function_args[3])
                    if fd not in fd_to_filename:
                        filename = '__unknown__'
                    else: 
                        filename = fd_to_filename[fd]
                elif 'seek' in func_name or 'close' in func_name or 'sync' in func_name or 'writev' in func_name or 'readv' in func_name or 'pwrite' in func_name or 'pread' in func_name or 'write' in func_name or 'read' in func_name or 'fprintf' in func_name:
                    fd = int(function_args[0])
                    if fd not in fd_to_filename:
                        filename = '__unknown__'
                    else: 
                        filename = fd_to_filename[fd]
                else:
                    filename = None    

                records_as_dict['rank'].append(rank)
                records_as_dict['fid'].append(record.func_id)
                records_as_dict['name'].append(func_name)
                records_as_dict['tstart'].append(record.tstart)
                records_as_dict['tend'].append(record.tend)
                records_as_dict['time'].append(record.tend - record.tstart)
                records_as_dict['arg_count'].append(record.arg_count)
                records_as_dict['args'].append(function_args)
                records_as_dict['return_value'].append(record.res)
                records_as_dict['file'].append(filename) 

        dataframe = pd.DataFrame.from_dict(records_as_dict)

        return IOFrame(dataframe)
