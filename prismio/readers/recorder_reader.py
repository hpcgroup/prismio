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
import numpy as np
import pandas as pd
from prismio.io_frame import IOFrame

from pathlib import Path
sys.path.insert(1, str(Path(__file__).parent.parent.parent.absolute()) + '/external/tools/reporter')
sys.path.insert(1, str(Path(__file__).parent.parent.absolute()))
import creader_wrapper


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
            A list containing sorted records.

        """
        self.log_dir = log_dir
        self.reader = creader_wrapper.RecorderReader(log_dir)
    
    def sort_records(self):
        """
        Assign the rank to each record. Sort all records in ascending start time order.

        Args:
            None.

        Return:
            A list containing sorted records.

        """
        records = []
        for rank in range(self.reader.GM.total_ranks):
            for i in range(self.reader.LMs[rank].total_records):
                record = self.reader.records[rank][i]
                record.rank = rank
                records.append( record )
        records = sorted(records, key=lambda x: x.tstart)
        return records

    def find_file_names(self, records, num_processes, func_id_to_name):
        """
        Figure out the file name each record accesses. Assign that to each record.
        So after this function, all records should have the correct file name.

        Args:
            records (list of records): sorted list of records of a run.
            num_processes: (integer): the number of processes of the run.
            func_id_to_name (list): map from function id to function name. Dependent on Recorder.

        Return:
            None.

        """
        fd_to_file_names = [{0: "stdin", 1: "stdout", 2: "stderr"}] * num_processes
        
        for i, record in enumerate(records):
            rank = record.rank
            argv = record.args_to_strs()
            fd_to_file_name = fd_to_file_names[rank]
            func_name = func_id_to_name[record.func_id]
            if 'fdopen' in func_name:
                fd = record.res
                old_fd = int(argv[0])
                if old_fd not in fd_to_file_name:
                    record.file_name = '__unknown__'
                else:
                    file_name = fd_to_file_name[old_fd]
                    fd_to_file_name[fd] = file_name
                    record.file_name = file_name
            elif 'fopen' in func_name or 'open' in func_name:
                fd = record.res
                file_name = argv[0]
                fd_to_file_name[fd] = file_name
                record.file_name = file_name
            elif 'fwrite' in func_name or 'fread' in func_name:
                fd = int(argv[3])
                if fd not in fd_to_file_name:
                    record.file_name = '__unknown__'
                else: 
                    file_name = fd_to_file_name[fd]
                    record.file_name = file_name
            elif 'seek' in func_name or 'close' in func_name or 'sync' in func_name or 'writev' in func_name or 'readv' in func_name or 'pwrite' in func_name or 'pread' in func_name or 'write' in func_name or 'read' in func_name or 'fprintf' in func_name:
                fd = int(argv[0])
                if fd not in fd_to_file_name:
                    record.file_name = '__unknown__'
                else: 
                    file_name = fd_to_file_name[fd]
                    record.file_name = file_name
            else:
                record.file_name = None
        
    def read(self):
        """
        Call sort_records and then find_file_names. After it has all information needed,
        it creates the dataframe row by row. Then create an IOFrame with this dataframe. 

        Args:
            None.

        Return:
            An IOFrame created by trace files of recorder specified by the log_dir of this RecorderReader.

        """
        df = pd.DataFrame(data=[], columns = ['rank', 'func_id', 'func_name', 'tstart', 'tend', 'telapsed', 'argc', 'argv', 'file_name', 'res'])
        num_processes = self.reader.GM.total_ranks
        records = self.sort_records()
        self.find_file_names(records, num_processes, self.reader.funcs)
        for index, record in enumerate(records):
            rank = record.rank
            func_id = record.func_id
            func_name = self.reader.funcs[func_id]
            tstart = record.tstart
            tend = record.tend
            telapsed = tend - tstart
            argc = record.arg_count
            argv = record.args_to_strs()
            file_name = record.file_name
            res = record.res
            df.loc[index] = [rank, func_id, func_name, tstart, tend, telapsed, argc, argv, file_name, res]    
        
        return IOFrame(df, self.log_dir, num_processes)
