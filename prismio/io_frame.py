# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

"""
The prismio.io_frame module provides the IOFrame class for structured data
structure and flexible api of tracing/profiling data generated by Recorder
or Darshan
"""


import sys
import os
import numpy as np
import pandas as pd

class IOFrame:
    """
    Main class of the prism application. It holds I/O performance data 
    generated by I/O tools. It reorganizes the data into a Pandas.DataFrame,
    which contains useful information such as the start time of functions, 
    the files functions access to, etc. It also provides flexible api 
    functions for user to do analysis.
    """
    def __init__(self, dataframe):
        """
        Args:
            dataframe (DataFrame): the dataframe this IOFrame should have.

        Return:
            None.

        """
        self.dataframe = dataframe

    @staticmethod
    def from_recorder(log_dir):
        """
        Read trace files from recorder and create the corresponding
        IOFrame object.

        Args:
            log_dir (str): path to the trace files directory of Recorder the user wants to analyze.

        Return:
            A IOFrame object corresponding to this trace files directory.

        """
        from prismio.readers.recorder_reader import RecorderReader
        return RecorderReader(log_dir).read()

    def filter(self, my_lambda): 
        """
        Create a new IOFrame based on the filter function the user provides.

        Args:
            my_lambda (function): filtering function. For example, np.sum, np.sort.

        Return:
            A new IOFrame object with a new filtered dataframe.

        """
        dataframe = self.dataframe[self.dataframe.apply(my_lambda, axis = 1)]
        dataframe = dataframe.reset_index()
        dataframe = dataframe.drop('index', axis=1)
        return IOFrame(dataframe)

    def groupby_aggregate(self, groupby_columns, agg_dict=None, drop=False):
        """
        Return a dataframe after groupby and aggregate operations on the dataframe of this IOFrame.

        Args:
            groupby_columns (list of strings): the column names the user wants to groupby.
            agg_dict (dictionary): aggregation functions for some columns

        Return:
            A dataframe after groupby and aggregate operations on the dataframe of this IOFrame.

        """
        # default aggregation functions for all columns
        default_agg_dict = {
            'rank': lambda x: x.iloc[0],
            'function_id': lambda x: x.iloc[0],
            'function_name': lambda x: x.iloc[0],
            'tstart': np.min,
            'tend': np.max,
            'time': np.sum,
            'arg_count': lambda x: x.iloc[0],
            'args': lambda x: x.iloc[0],
            'return_value': lambda x: x.iloc[0],
            'file_name': lambda x: x.iloc[0],
            'io_volume': np.sum
        }

        groupby_obj = self.dataframe.groupby(groupby_columns)

        # if agg_dic is None, use the default agg_dict
        if agg_dict is None:
            agg_dataframe = groupby_obj.agg(default_agg_dict)
            return agg_dataframe
        
        # if not, make sure columns contain keys in agg_dict
        for key in agg_dict:
            if key not in self.dataframe.columns:
                raise KeyError("Specified column does not exist in the dataframe!")
        
        # if drop other columns, directly apply agg_dict
        if drop:
            agg_dataframe = groupby_obj.agg(agg_dict)
        # else replace functions in default agg_dict with user specified ones,
        # then apply default agg_dict
        else:
            for key in agg_dict:
                default_agg_dict[key] = agg_dict[key]
            agg_dataframe = groupby_obj.agg(default_agg_dict)
        
        return agg_dataframe
    
    def file_count(self, rank=None, agg_function=None):
        """
        Depending on input arguments, return the number of files for ranks selected by the
        user in the form of a DataFrame. It contains the number of files touched (read or written) 
        by these ranks. If agg_function is specified, then it will apply the function to 
        the result.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            If rank == None and agg_function == None, it returns a Pandas DataFrame 
            that contains the number of files for all ranks.
            If rank != None and agg_function == None, it returns a Pandas DataFrame 
            that contains the number of files for the listed ranks.
            If rank == None and agg_function != None, it returns a number from applying
            the function on the dataframe. For example, if agg_function = np.mean, it
            returns the average number of files of all ranks.
            If rank != None and agg_function != None, it returns a number from applying
            the function on the dataframe for listed ranks. For example, if rank =
            [1,3,5], agg_function = np.mean, it returns the average number of files 
            of all rank 1, 3, 5.

        """

        # save the original dataframe because we are going to filter out rows
        original_dataframe = self.dataframe
        # filter out not specified ranks
        if rank is not None:
            # This will copy the dataframe instead of overwriting it
            self.dataframe = self.dataframe[self.dataframe['rank'].isin(rank)]
        
        # groupby rank, then count the number of unique file names
        dataframe = self.groupby_aggregate(['rank'], {'file_name': 'nunique'}, drop=True)
        dataframe = dataframe.rename(columns={'file_name': 'num_files'})
        self.dataframe = original_dataframe
        
        if agg_function == None:
            return dataframe
        # apply agg_function if it's not None
        else:
            return agg_function(dataframe)

    def file_access_count(self, rank=None, agg_function=None):
        """
        Depending on input arguments, return the number of accesses of each file in each rank
        selected by the user in the form of a DataFrame. If agg_function is specified, then it 
        will apply the function to the result.
        The function first group the dataframe by file name. Then it goes through each group. 
        And for each group, the function groups it by rank and aggregates to find the count for
        each rank. Then it filters ranks the user wants, and put result to a dictionary. After
        going through all file name groups. It use the dictionary to create a dataframe. If
        agg_function is specified, it applies the function on the dataframe.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            If rank == None and agg_function == None, it returns a Pandas DataFrame in which
            the columns are file names, rows are ranks, and the values are the number of accesses
            for that file and rank.
            If rank != None and agg_function == None, it returns a similar Pandas DataFrame but
            with only user specified ranks (rows).
            If rank == None and agg_function != None, it returns a Pandas Series that has the number
            from applying the function on each file name across all ranks. For example, if agg_function = 
            np.mean, it returns a series containing the average number of accesses for this file across 
            all ranks.
            If rank != None and agg_function != None, it returns a Pandas Series that has the number
            from applying the function on each file name across selected ranks. For example, if agg_function = 
            np.mean, rank = [1, 3, 5], it returns a series containing the average number of accesses for 
            this file across rank 1, 3, 5.

        """

        # save the original dataframe because we are going to filter out rows
        original_dataframe = self.dataframe
        # filter out not specified ranks
        if rank is not None:
            # This will copy the dataframe instead of overwriting it
            self.dataframe = self.dataframe[self.dataframe['rank'].isin(rank)]
        
        # groupby file name and rank, then count the number of each file name
        dataframe = self.groupby_aggregate(['file_name', 'rank'], {'file_name': 'count'}, drop=True)
        dataframe = dataframe.rename(columns={'file_name': 'access_count'})
        self.dataframe = original_dataframe
        
        if agg_function is None:
            return dataframe
        # group by file names and apply agg_function over ranks if it's not None
        else:
            dataframe = dataframe.groupby(level=[0]).agg({'access_count': agg_function})
            return dataframe
            
    def function_count(self, rank=None, agg_function=None):
        """
        Identical to the previous one. Only instead of groupby file, it groupby function.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            Identical structure to the previous one, except the value here is the number of function
            calls for a function in selected ranks. Or avg/min/max accross selected ranks depending 
            on the agg_function

        """
        
        # save the original dataframe because we are going to filter out rows
        original_dataframe = self.dataframe
        # filter out not specified ranks
        if rank is not None:
            # This will copy the dataframe instead of overwriting it
            self.dataframe = self.dataframe[self.dataframe['rank'].isin(rank)]
        
        # groupby function name and rank, then count the number of each function name
        dataframe = self.groupby_aggregate(['function_name', 'rank'], {'function_name': 'count'}, drop=True)
        self.dataframe = original_dataframe
        dataframe = dataframe.rename(columns={'function_name': 'num_calls'})
        
        # group by function name and apply agg_function over ranks if it's not None
        if agg_function is None:
            return dataframe
        else:
            dataframe = dataframe.groupby(level=[0]).agg({'num_calls': agg_function})
            return dataframe

    def function_time(self, rank=None, agg_function=None):
        """
        Identical to the previous one. Only instead of aggregating by count, it 
        aggregating by sum of the time.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            Identical structure to the previous one, except the value here is the total time of function
            in selected ranks. Or avg/min/max accross selected ranks depending on the agg_function

        """
        
        # save the original dataframe because we are going to filter out rows
        original_dataframe = self.dataframe
        # filter out not specified ranks
        if rank is not None:
            # This will copy the dataframe instead of overwriting it
            self.dataframe = self.dataframe[self.dataframe['rank'].isin(rank)]
        
        # groupby function name and rank, then sum the runtime 
        dataframe = self.groupby_aggregate(['function_name', 'rank'], {'time': 'sum'}, drop=True)
        dataframe = dataframe.rename(columns={'time': 'cumulative_time'})
        self.dataframe = original_dataframe
        
        if agg_function is None:
            return dataframe
        # group by function name and apply agg_function over ranks if it's not None
        else:
            dataframe = dataframe.groupby(level=[0]).agg({'cumulative_time': agg_function})
            return dataframe


    def function_count_by_library(self, rank=None, agg_function=None):
        """
        Count the number of function calls from mpi, hdf5 and posix. Same implementation to previous
        ones. But it first check the library for each function call, and then groupby the library.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            Identical structure to the previous one, except the value here is the number of function
            calls of a library in selected ranks. Or avg/min/max accross selected ranks depending on 
            the agg_function

        """
        
        # helper function to check library for a given function
        def check_library(function):
            if 'H5' in function:
                return 'hdf5'
            elif 'MPI' in function:
                return 'mpi'
            else:
                return 'posix'

        # save the original dataframe because we are going to filter out rows
        original_dataframe = self.dataframe
        # filter out not specified ranks
        if rank is not None:
            # This will copy the dataframe instead of overwriting it
            self.dataframe = self.dataframe[self.dataframe['rank'].isin(rank)]
        
        # check library for each row and put result into a new column 
        pd.options.mode.chained_assignment = None
        self.dataframe['library'] = self.dataframe['function_name'].apply(lambda function: check_library(function))
        pd.options.mode.chained_assignment = 'warn'

        # groupby library name and rank, then count the number of functions in each library
        dataframe = self.groupby_aggregate(['library', 'rank'], {'library': 'count'}, drop=True)
        dataframe = dataframe.rename(columns={'library': 'func_count_of_lib'})
        self.dataframe = original_dataframe

        # filter out not specified ranks
        # group by library name and apply agg_function over ranks if it's not None
        if agg_function is None:
            return dataframe
        else:
            dataframe = dataframe.groupby(level=[0]).agg({'func_count_of_lib': agg_function})
            return dataframe
