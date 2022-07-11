# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# Using third party library modules and code from Recorder https://github.com/uiuc-hpc/Recorder 
# Copyright (c) 2020 University of Illinois Board of Trustees. All Rights Reserved.
# Developed by: Chen Wang, chenw5@illinois.edu 

"""
The prismio.reader.recorder_reader module provides functions for processing tracing data
from Recorder, for example, sorting records, finding the file name each record operates.
With data processing, it organize the data to a dataframe, and create the IOFrame for
recorder tracing files.

"""


from ftplib import all_errors
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

        Posix_IO_functions = [
            "creat",        "creat64",      "open",         "open64",   "close",
            "write",        "read",         "lseek",        "lseek64",  "pread",
            "pread64",      "pwrite",       "pwrite64",     "readv",    "writev",
            "mmap",         "mmap64",       "fopen",        "fopen64",  "fclose",
            "fwrite",       "fread",        "ftell",        "fseek",    "fsync",
            "fdatasync",    "__xstat",      "__xstat64",    "__lxstat", "__lxstat64",
            "__fxstat",     "__fxstat64",   "getcwd",       "mkdir",    "rmdir",
            "chdir",        "link",         "linkat",       "unlink",   "symlink",
            "symlinkat",    "readlink",     "readlinkat",   "rename",   "chmod",
            "chown",        "lchown",       "utime",        "opendir",  "readdir",
            "closedir",     "rewinddir",    "mknod",        "mknodat",  "fcntl",
            "dup",          "dup2",         "pipe",         "mkfifo",   "umask",
            "fdopen",       "fileno",       "access",       "faccessat","tmpfile",
            "remove",       "truncate",     "ftruncate",    "vfprintf", "msync",
            "fseeko",       "ftello",
        ]

        MPI_functions = [
            "MPI_Comm_size",              "MPI_Comm_rank",                 "MPI_Get_processor_name",
            "MPI_Comm_set_errhandler",    "MPI_Barrier",                   "MPI_Bcast",
            "MPI_Ibcast",                 "MPI_Gather",                    "MPI_Scatter",
            "MPI_Gatherv",                "MPI_Scatterv",                  "MPI_Allgather",  
            "MPI_Allgatherv",             "MPI_Alltoall",                  "MPI_Reduce", 
            "MPI_Allreduce",              "MPI_Reduce_scatter",            "MPI_Waitall",  
            "MPI_Scan",                   "MPI_Type_create_darray",        "MPI_Type_commit",
            "MPI_File_open",              "MPI_File_close",                "MPI_File_sync",
            "MPI_File_set_size",          "MPI_File_set_view",             "MPI_File_read",
            "MPI_File_read_at",           "MPI_File_read_at_all",          "MPI_File_read_all",
            "MPI_File_read_shared",       "MPI_File_read_ordered",         "MPI_File_read_at_all_begin",
            "MPI_File_read_all_begin",    "MPI_File_read_ordered_begin",   "MPI_File_iread_at",
            "MPI_File_iread",             "MPI_File_iread_shared",         "MPI_File_write",
            "MPI_File_write_at",          "MPI_File_write_at_all",         "MPI_File_write_all",
            "MPI_File_write_shared",      "MPI_File_write_ordered",        "MPI_File_write_at_all_begin",
            "MPI_File_write_all_begin",   "MPI_File_write_ordered_begin",  "MPI_File_iwrite_at",
            "MPI_File_iwrite",            "MPI_File_iwrite_shared",        "MPI_Finalized",
            "MPI_Cart_rank",              "MPI_Cart_create",               "MPI_Cart_get",
            "MPI_Cart_shift",             "MPI_Wait",                      "MPI_Send",
            "MPI_Recv",                   "MPI_Sendrecv",                  "MPI_Isend",
            "MPI_Irecv",                  "MPI_Waitsome",                  "MPI_Waitany",
            "MPI_Ssend",                  "MPI_Comm_split",                "MPI_Comm_create",
            "MPI_Comm_dup",               "MPI_File_seek",                 "MPI_File_seek_shared",
            "MPI_File_get_size",          "MPI_Comm_free",                 "MPI_Cart_sub",
            "MPI_Test",                   "MPI_Testall",                   "MPI_Testsome",
            "MPI_Testany",                "MPI_Ireduce",                   "MPI_Igather",
            "MPI_Iscatter",               "MPI_Ialltoall",                 "MPI_Comm_split_type",             
            "MPI_Init",                   "MPI_Init_thread",               "MPI_Finalize",   
        ]

        MPI_IO_functions = [
            "MPI_File_open",              "MPI_File_close",                "MPI_File_sync",
            "MPI_File_set_size",          "MPI_File_set_view",             "MPI_File_read",
            "MPI_File_read_at",           "MPI_File_read_at_all",          "MPI_File_read_all",
            "MPI_File_read_shared",       "MPI_File_read_ordered",         "MPI_File_read_at_all_begin",
            "MPI_File_read_all_begin",    "MPI_File_read_ordered_begin",   "MPI_File_iread_at",
            "MPI_File_iread",             "MPI_File_iread_shared",         "MPI_File_write",
            "MPI_File_write_at",          "MPI_File_write_at_all",         "MPI_File_write_all",
            "MPI_File_write_shared",      "MPI_File_write_ordered",        "MPI_File_write_at_all_begin",
            "MPI_File_write_all_begin",   "MPI_File_write_ordered_begin",  "MPI_File_iwrite_at",
            "MPI_File_iwrite",            "MPI_File_iwrite_shared",        "MPI_File_seek",                 
            "MPI_File_seek_shared",       "MPI_File_get_size",
        ]

        MPI_communication_functions = [
            "MPI_Comm_size",              "MPI_Comm_rank",                 "MPI_Get_processor_name",
            "MPI_Comm_set_errhandler",    "MPI_Barrier",                   "MPI_Bcast",
            "MPI_Ibcast",                 "MPI_Gather",                    "MPI_Scatter",
            "MPI_Gatherv",                "MPI_Scatterv",                  "MPI_Allgather",  
            "MPI_Allgatherv",             "MPI_Alltoall",                  "MPI_Reduce", 
            "MPI_Allreduce",              "MPI_Reduce_scatter",            "MPI_Waitall",  
            "MPI_Scan",                   "MPI_Type_create_darray",        "MPI_Type_commit",
            "MPI_Finalized",
            "MPI_Cart_rank",              "MPI_Cart_create",               "MPI_Cart_get",
            "MPI_Cart_shift",             "MPI_Wait",                      "MPI_Send",
            "MPI_Recv",                   "MPI_Sendrecv",                  "MPI_Isend",
            "MPI_Irecv",                  "MPI_Waitsome",                  "MPI_Waitany",
            "MPI_Ssend",                  "MPI_Comm_split",                "MPI_Comm_create",
            "MPI_Comm_dup",               "MPI_Comm_free",                 "MPI_Cart_sub",
            "MPI_Test",                   "MPI_Testall",                   "MPI_Testsome",
            "MPI_Testany",                "MPI_Ireduce",                   "MPI_Igather",
            "MPI_Iscatter",               "MPI_Ialltoall",                 "MPI_Comm_split_type",             
            "MPI_Init",                   "MPI_Init_thread",               "MPI_Finalize",   
        ]

        HDF5_IO_functions = [
            "H5Fcreate",            "H5Fopen",              "H5Fclose",     "H5Fflush", 
            "H5Gclose",             "H5Gcreate1",           "H5Gcreate2",   
            "H5Gget_objinfo",       "H5Giterate",           "H5Gopen1",
            "H5Gopen2",             "H5Dclose",             "H5Dcreate1",
            "H5Dcreate2",           "H5Dget_create_plist",  "H5Dget_space", 
            "H5Dget_type",          "H5Dopen1",             "H5Dopen2",
            "H5Dread",              "H5Dwrite",             "H5Dset_extent",
            "H5Sclose",             "H5Sget_simple_extent_npoints",                                       
            "H5Screate",            "H5Screate_simple",     "H5Sget_select_npoints",
            "H5Sselect_elements",   "H5Sget_simple_extent_dims",
            "H5Sselect_hyperslab",  "H5Sselect_none",       "H5Tclose",     
            "H5Tcopy",              "H5Tget_class",         "H5Tget_size",
            "H5Tset_size",          "H5Tcreate",            "H5Tinsert",
            "H5Aclose",             "H5Acreate1",           "H5Acreate2",   
            "H5Aget_name",          "H5Aget_num_attrs",     "H5Aget_space",
            "H5Aget_type",          "H5Aopen",              "H5Aopen_idx",
            "H5Aopen_name",         "H5Aread",              "H5Awrite",
            "H5Pclose",             "H5Pcreate",            "H5Pget_chunk", 
            "H5Pget_mdc_config",    "H5Pset_alignment",     "H5Pset_chunk",
            "H5Pset_dxpl_mpio",     "H5Pset_fapl_core",     "H5Pset_fapl_mpio",
            "H5Pset_fapl_mpiposix", "H5Pset_istore_k",      "H5Pset_mdc_config",
            "H5Lexists",           "H5Lget_val",            "H5Pset_meta_block_size",
            "H5Literate",           "H5Oclose",             "H5Oget_info",  
            "H5Oget_info_by_name",  "H5Oopen",
            "H5Pset_coll_metadata_write",                   "H5Pget_coll_metadata_write",   
            "H5Pset_all_coll_metadata_ops",                 "H5Pget_all_coll_metadata_ops"
        ]



        metadata_as_dict = {
            'rank': [], 
            'start_timestamp': [], 
            'end_timestamp': [], 
            'time': [], 
            'file_count': [],
            'total_records': []
        }

        for rank in range(len(self.reader.LMs)):
            metadata_as_dict['rank'].append(rank)
            metadata_as_dict['start_timestamp'].append(self.reader.LMs[rank].start_timestamp)
            metadata_as_dict['end_timestamp'].append(self.reader.LMs[rank].end_timestamp)
            metadata_as_dict['time'].append(self.reader.LMs[rank].end_timestamp - self.reader.LMs[rank].start_timestamp)
            metadata_as_dict['file_count'].append(self.reader.LMs[rank].num_files)
            metadata_as_dict['total_records'].append(self.reader.LMs[rank].total_records)

        metadata = pd.DataFrame.from_dict(metadata_as_dict)

        all_records = []
        for rank in range(self.reader.GM.total_ranks):
            for record_index in range(self.reader.LMs[rank].total_records):
                record = self.reader.records[rank][record_index]
                record.rank = rank
                all_records.append(record)
        all_records = sorted(all_records, key=lambda x: x.tstart)

        records_as_dict = {
            'rank': [], 
            'function_id': [], 
            'function_name': [], 
            'tstart': [],
            'tend': [],
            'time': [], 
            'arg_count': [],
            'args': [],
            'return_value': [],
            'file_name': [],
            'io_volume': [],
            'offset': [],
            'function_type': [],
            'I/O_type': [],
            'I/O_interface': [],
            'error': [],
        }

        fd_to_filenames = [{0: "stdin", 1: "stdout", 2: "stderr"} for _ in range(self.reader.GM.total_ranks)]
        fd_offsets = [{0: 0, 1: 0, 2: 0} for _ in range(self.reader.GM.total_ranks)]
        end_of_files = {"stdin": 0, "stdout": 0, "stderr": 0}
        
        for record in all_records:
            rank = record.rank
            fd_to_filename = fd_to_filenames[rank]
            fd_offset = fd_offsets[rank]
            func_name = self.reader.funcs[record.func_id]
            '''
                File "/Users/henryxu/Desktop/Research/prismio/prismio/readers/recorder_reader.py", line 216, in read
                function_args = record.args_to_strs()
                File "/Users/henryxu/miniconda3/lib/python3.9/site-packages/recorder_viz/creader_wrapper.py", line 57, in args_to_strs
                arg_strs[i] = self.args[i].decode('utf-8')
            AttributeError: 'NoneType' object has no attribute 'decode'
            '''

            filename = None
            io_size = None
            offset = None
            error = None

            try:
                function_args = record.args_to_strs()
            except UnicodeDecodeError:
                print(func_name)
                sys.stdout.flush()
                print(record)
                sys.stdout.flush()
                print('UnicodeDecodeError from function arguments')
                sys.stdout.flush()
                function_args = ['None', 'None', 'None', 'None', 'None', 'None']
                error = 1
            except AttributeError:
                print(func_name)
                sys.stdout.flush()
                print(record)
                sys.stdout.flush()
                print('AttributeError from function arguments')
                sys.stdout.flush()
                function_args = ['None', 'None', 'None', 'None', 'None', 'None']
                error = 1
                
            if error == 1:
                pass
            elif 'MPI' in func_name or 'H5' in func_name:
                pass
            elif 'fdopen' in func_name:
                fd = record.res
                old_fd = int(function_args[0])
                if old_fd not in fd_to_filename:
                    error = "Error: fdopen a non-existing file descriptor (no previous open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                else:
                    filename = fd_to_filename[old_fd]
                    fd_to_filename[fd] = filename
                    fd_offset[fd] = 0

            elif 'fopen' in func_name:
                fd = record.res
                filename = function_args[0]
                fd_to_filename[fd] = filename
                if filename not in end_of_files:
                    end_of_files[filename] = 0
                
                fd_offset[fd] = 0
                openMode = function_args[1]
                if 'a' in openMode:
                    fd_offset[fd] = end_of_files[filename]

            elif 'open' in func_name:
                fd = record.res
                filename = function_args[0]
                fd_to_filename[fd] = filename
                if filename not in end_of_files:
                    end_of_files[filename] = 0

                fd_offset[fd] = 0
                
                openMode = int(function_args[1])
                if openMode == 2:
                    fd_offset[fd] = end_of_files[filename]

            elif 'seek' in func_name:
                fd, offset, whence = int(function_args[0]), int(function_args[1]), int(function_args[2])
                
                if fd not in fd_to_filename:
                    error = "Error: seek a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                    offset = -1
                else: 
                    filename = fd_to_filename[fd]
                    if whence == 0:     # SEEK_SET
                        fd_offset[fd] = offset
                    elif whence == 1:   # SEEK_CUR
                        if fd_offset[fd] + offset > end_of_files[filename]:
                            error = "Warning: seek beyond end of file"
                            print(error)
                            fd_offset[fd] += offset
                        elif fd_offset[fd] + offset < 0:
                            error = "Error: seek beyond start of file"
                            print(error)
                        else:
                            fd_offset[fd] += offset
                    elif whence == 2:   # SEEK_END'
                        if offset > 0:
                            error = "Warning: seek beyond end of file"
                            print(error)
                            fd_offset[fd] = end_of_files[filename] + offset
                        elif end_of_files[filename] + offset < 0:
                            error = "Error: seek beyond start of file"
                            print(error)
                        else:
                            fd_offset[fd] = end_of_files[filename] + offset

            elif 'close' in func_name:
                fd = int(function_args[0])
                if fd not in fd_to_filename:
                    filename = '__unknown__'
                    error = "Error: close a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                else: 
                    filename = fd_to_filename[fd]
                    del fd_to_filename[fd]
                    del fd_offset[fd]
            
            elif 'sync' in func_name:
                fd = int(function_args[0])
                if fd not in fd_to_filename:
                    filename = '__unknown__'
                    error = "Error: sync a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                else: 
                    filename = fd_to_filename[fd]

            elif 'fwrite' in func_name or 'fread' in func_name:
                io_size = int(function_args[1]) * int(function_args[2])
                fd = int(function_args[3])
                
                if fd not in fd_to_filename:
                    error = "Error: write or read a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                    offset = -1
                else: 
                    filename = fd_to_filename[fd]
                    offset = fd_offset[fd]
                    fd_offset[fd] += io_size
                    end_of_files[filename] = max(end_of_files[filename], fd_offset[fd])

            elif 'writev' in func_name or 'readv' in func_name:
                fd, io_size = int(function_args[0]), int(function_args[1])

                if fd not in fd_to_filename:
                    error = "Error: write or read a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                    offset = -1
                else: 
                    filename = fd_to_filename[fd]
                    offset = fd_offset[fd]
                    fd_offset[fd] += io_size
                    end_of_files[filename] = max(end_of_files[filename], fd_offset[fd])

            elif 'pwrite' in func_name or 'pread' in func_name: # does not change offset
                fd, io_size, offset = int(function_args[0]), int(function_args[2]), int(function_args[3])
                
                if fd not in fd_to_filename:
                    error = "Error: write or read a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                else: 
                    filename = fd_to_filename[fd]
                    
                    if offset > end_of_files[filename]:
                        error = "Warning: pwrite or pread beyond end of file"
                        print(error)
                    elif offset < 0:
                        error = "Error: pwrite or pread beyond start of file"
                        print(error)
                    end_of_files[filename] = max(end_of_files[filename], offset + io_size)
            
            elif 'write' in func_name or 'read' in func_name:
                fd, io_size = int(function_args[0]), int(function_args[2])

                if fd not in fd_to_filename:
                    error = "Error: write or read a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                    offset = -1
                else: 
                    filename = fd_to_filename[fd]
                    offset = fd_offset[fd]
                    fd_offset[fd] += io_size
                    end_of_files[filename] = max(end_of_files[filename], fd_offset[fd])

            elif 'fprintf' in func_name:
                fd, io_size = int(function_args[0]), int(function_args[1])

                if fd not in fd_to_filename:
                    error = "Error: fprintf a non-existing file descriptor (no open returns this file descriptor or already closed)"
                    print(error)
                    filename = '__unknown__'
                    offset = -1
                else: 
                    filename = fd_to_filename[fd]
                    offset = fd_offset[fd]
                    fd_offset[fd] += io_size
                    end_of_files[filename] = max(end_of_files[filename], fd_offset[fd])

            records_as_dict['rank'].append(rank)
            records_as_dict['function_id'].append(record.func_id)
            records_as_dict['function_name'].append(func_name)
            records_as_dict['tstart'].append(record.tstart)
            records_as_dict['tend'].append(record.tend)
            records_as_dict['time'].append(record.tend - record.tstart)
            records_as_dict['arg_count'].append(record.arg_count)
            records_as_dict['args'].append(function_args)
            records_as_dict['return_value'].append(record.res)
            records_as_dict['file_name'].append(filename) 
            records_as_dict['io_volume'].append(io_size)
            records_as_dict['error'].append(error)
            records_as_dict['offset'].append(offset)

            if func_name in Posix_IO_functions or func_name in MPI_IO_functions or func_name in HDF5_IO_functions:
                records_as_dict['function_type'].append('I/O')
            elif func_name in MPI_communication_functions:
                records_as_dict['function_type'].append('communication')
            else:
                records_as_dict['function_type'].append('compute')

            if 'write' in func_name:
                records_as_dict['I/O_type'].append('write')
            elif 'read' in func_name:
                records_as_dict['I/O_type'].append('read')
            elif func_name in Posix_IO_functions or func_name in MPI_IO_functions or func_name in HDF5_IO_functions:
                records_as_dict['I/O_type'].append('meta')
            else:
                records_as_dict['I/O_type'].append('not I/O')

            if func_name in Posix_IO_functions :
                records_as_dict['I/O_interface'].append('POSIX')
            elif func_name in MPI_IO_functions:
                records_as_dict['I/O_interface'].append('MPIIO')
            elif func_name in HDF5_IO_functions:
                records_as_dict['I/O_interface'].append('HDF5')
            else:
                records_as_dict['I/O_interface'].append('not I/O')

        dataframe = pd.DataFrame.from_dict(records_as_dict)

        return IOFrame(dataframe, metadata)
