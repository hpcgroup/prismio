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
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
# import tkinter

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

    def groupby_aggregate(self, groupby_columns, agg_dict=None):
        """
        Return a dataframe after groupby and aggregate operations on the dataframe of this IOFrame.

        Args:
            groupby_columns (list of strings): the column names the user want to groupby.
            agg_function (function or string): the function used for aggregation. For example, 'sum', 'count', etc.
            agg_columns (list of strings): the columns the user want to aggregate, i.e., show up in the result.

        Return:
            A dataframe after groupby and aggregate operations on the dataframe of this IOFrame.

        """
        default_agg_dict = {
            'rank': 'nunique',
            'fid': 'count',
            'function': 'count',
            'tstart': np.min,
            'tend': np.max,
            'time': np.sum,
            'arg_count': lambda x: x.iloc[0],
            'args': lambda x: x.iloc[0],
            'return_value': lambda x: x.iloc[0],
            'file': 'nunique',
            'io_size': np.sum
        }
        groupby_obj = self.dataframe.groupby(groupby_columns)
        if (agg_dict == None):
            agg_dict = default_agg_dict
        agg_dataframe = groupby_obj.agg(agg_dict)
        return IOFrame(agg_dataframe)
    
    def file_count(self, rank=None, agg_function=np.mean):
        """
        Depending on input arguments, return the number of files for each rank selected by the
        user in the form of a Pandas Series or DataFrame. If agg_function is specified, then it 
        will apply the function to the result.

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
        file_count = self.groupby_aggregate(['rank'], 'nunique')['file']
        if rank != None:
            file_count = file_count.loc[rank]
        if agg_function == None:
            return file_count
        else:
            return agg_function(file_count)

    def file_access_count(self, rank=None, agg_function=np.mean):
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
        groupby_obj = self.dataframe.groupby(['file'])
        file_access_count = {}
        for key, group in groupby_obj:
            file_access_count_per_rank = group.groupby(['rank'])['fid'].count()
            if rank != None:
                file_access_count_per_rank = file_access_count_per_rank.filter(items=rank)
            file_access_count[key] = file_access_count_per_rank
        file_access_count = pd.DataFrame(file_access_count)
        if agg_function == None:
            return file_access_count
        else:
            return file_access_count.apply(agg_function)
            
    def function_count(self, rank=None, agg_function=np.mean):
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
        groupby_obj = self.dataframe.groupby(['function'])
        function_count = {}
        for key, group in groupby_obj:
            function_count_per_rank = group.groupby(['rank'])['fid'].count()
            if rank != None:
                function_count_per_rank = function_count_per_rank.filter(items=rank)
            function_count[key] = function_count_per_rank
        function_count = pd.DataFrame(function_count)
        if agg_function == None:
            return function_count
        else:
            return function_count.apply(agg_function)

    def function_time(self, rank=None, agg_function=np.mean):
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
        groupby_obj = self.dataframe.groupby(['function'])
        function_time = {}
        for key, group in groupby_obj:
            function_time_per_rank = group.groupby(['rank'])['time'].sum()
            if rank != None:
                function_time_per_rank = function_time_per_rank.filter(items=rank)
            function_time[key] = function_time_per_rank
        function_time = pd.DataFrame(function_time)
        if agg_function == None:
            return function_time
        else:
            return function_time.apply(agg_function)

    def function_calls_by_library(self, rank=None, agg_function=np.mean):
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
        def check_library(function):
            if 'H5' in function:
                return 'hdf5'
            elif 'MPI' in function:
                return 'mpi'
            else:
                return 'posix'

        self.dataframe['layer'] = self.dataframe['function'].apply(lambda function: check_library(function))
        groupby_obj = self.dataframe.groupby(['layer'])

        function_calls_by_library = {}
        for key, group in groupby_obj:
            function_calls_by_library_per_rank = group.groupby(['rank'])['function'].count()
            if rank != None:
                function_calls_by_library_per_rank = function_calls_by_library_per_rank.filter(items=rank)
            function_calls_by_library[key] = function_calls_by_library_per_rank
        function_calls_by_library = pd.DataFrame(function_calls_by_library)
        if agg_function == None:
            return function_calls_by_library
        else:
            return function_calls_by_library.apply(agg_function)

    def plot_trace(self, functions=None):
        """
        Directly plot the trace of selected functions for all ranks.
        First filter the selected functions and groupby functions. It then goes through each group
        and construct x and y from tstart, tend and rank. The reason why adding those NaN is by doing
        so matplotlib is able to plot segmented lines for each function call. Otherwise all lines will
        be connected.

        Args:
            functions (list of strings): functions the user want to see in the trace.

        Return:
            None

        """
        dataframe = self.dataframe
        if functions != None:
            dataframe = dataframe[dataframe['function'].isin(functions)]
        groupby_obj = dataframe.groupby(['function'])
        for key, group in groupby_obj:
            x = [np.NaN] * 3 * len(group)
            x[::3] = group['tstart']
            x[1::3] = group['tend']
            y = [np.NaN] * 3 * len(group)
            y[::3] = group['rank']
            y[1::3] = group['rank']
            plt.plot(x, y, label=key)
        plt.show()
        plt.clf()

    def plot_io_sizes(self):
        """
        Directly plot the number of io accesses with different sizes.

        Args:
            None.

        Return:
            None

        """
        io_sizes = self.groupby_aggregate(['io_size'], 'count')['function']
        ax = io_sizes.plot.bar()
        Y = io_sizes.values
        for i, y in enumerate(Y):
            ax.text(i, y + 1, y, ha='center', va='bottom')
        plt.savefig("io_sizes.png")
