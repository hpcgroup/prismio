# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys
import numpy as np
import pandas as pd
from io_frame import IOFrame

from pathlib import Path
sys.path.insert(1, str(Path(__file__).parent.parent.parent.absolute()) + '/external/tools/reporter')
sys.path.insert(1, str(Path(__file__).parent.parent.absolute()))
import creader_wrapper


class RecorderReader:
    def ignore_funcs(self, func):
        ignore = ["MPI", "H5", "writev"]
        for f in ignore:
            if f in func:
                return True
        return False

    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.reader = creader_wrapper.RecorderReader(log_dir)
    
    def sort_records(self):
        records = []
        for rank in range(self.reader.GM.total_ranks):
            for record in self.reader.records[rank]:
                record.rank = rank
                records.append( record )
        records = sorted(records, key=lambda x: x.tstart)
        return records

    def get_fd_to_file_name(self, records, np, func_id_to_name):
        fd_to_file_names = [{0: "stdin", 1: "stdout", 2: "stderr"}] * np
        
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
        return fd_to_file_names
        
    def read(self):
        df = pd.DataFrame(data=[], columns = ['rank', 'func_id', 'func_name', 'tstart', 'tend', 'telapsed', 'argc', 'argv', 'file_name', 'res'])
        np = self.reader.GM.total_ranks
        records = self.sort_records()
        fd_to_file_name = self.get_fd_to_file_name(records, np, self.reader.funcs)
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
        
        return IOFrame(df, self.log_dir, np, fd_to_file_name)
