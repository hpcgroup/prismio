# Copyright 2020-2021 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

"""
The prismio.io_frame module provides the IOFrame class for structured data
structure and flexible api of tracing/profiling data generated by Recorder
or Darshan
"""

import dataclasses
import sys
import os

from pandas.core.frame import DataFrame
from typing import Callable, List, Dict, Optional
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class IOFrame:
    """
    Main class of the prism application. It holds I/O performance data
    generated by I/O tools. It reorganizes the data into a Pandas.DataFrame,
    which contains useful information such as the start time of functions, the
    files functions access to, etc. It also provides flexible api functions for
    user to do analysis.
    """

    # the dataframe this IOFrame should have.
    dataframe: DataFrame
    # the dataframe containing metadata info, such as total runtime of each rank
    metadata: DataFrame

    @staticmethod
    def from_recorder(log_dir: str):
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
        dataframe = self.dataframe[self.dataframe.apply(my_lambda, axis=1)]
        dataframe = dataframe.reset_index()
        dataframe = dataframe.drop("index", axis=1)
        print("Warning: filtering dataframe may cause inconsistency in metadata!")

        return IOFrame(dataframe, self.metadata)

    def groupby_aggregate(
        self,
        groupby_columns: List[str],
        rank: Optional[list] = None,
        agg_dict: Optional[dict] = None,
        filter_lambda: Optional[Callable[..., bool]] = None,
        drop: Optional[bool] = False,
        dropna: Optional[bool] = False
    ):
        """
        Return a dataframe after groupby and aggregate operations on the
        dataframe of this IOFrame.

        Args:
            groupby_columns (list of strings): the column names the user wants to groupby.
            rank (list): Ranks the user wants to keep. Other ranks will be filtered out in the result.
                If it is None, then keep all ranks.
            agg_dict (dictionary): aggregation functions for some columns
            filter_lambda (function): function used to filter rows before groupby
            drop: If true, drop columns not specified in agg_dict. Otherwise keep all columns in the result.
            dropna: used by groupby, decide whether to include NaN as a group.

        Return:
            A dataframe after groupby and aggregate operations on the dataframe of this IOFrame.

        """
        # default aggregation functions for all columns
        default_agg_dict = {
            "rank": lambda x: x.iloc[0],
            "function_id": lambda x: x.iloc[0],
            "function_name": lambda x: x.iloc[0],
            "tstart": np.min,
            "tend": np.max,
            "time": np.sum,
            "arg_count": lambda x: x.iloc[0],
            "args": lambda x: x.iloc[0],
            "return_value": lambda x: x.iloc[0],
            "file_name": lambda x: x.iloc[0],
            "io_volume": np.sum,
        }

        # Filter out not specified ranks. Make a deep copy and groupby_agg on it,
        # so self.dataframe is not changed.
        dataframe = self.dataframe
        if rank is not None:
            dataframe = self.dataframe[self.dataframe["rank"].isin(rank)]
            # dataframe = dataframe.copy(deep=True)
        if filter_lambda is not None:
            dataframe = dataframe[self.dataframe.apply(filter_lambda, axis=1)]
            dataframe = dataframe.reset_index()
            dataframe = dataframe.drop("index", axis=1)

        groupby_obj = dataframe.groupby(groupby_columns, dropna=dropna)

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

    def file_count(
        self, rank: Optional[list] = None, agg_function: Optional[Callable] = None
    ):
        """
        Depending on input arguments, return the number of files for ranks
        selected by the user in the form of a DataFrame. It contains the number
        of files touched (read or written) by these ranks. If agg_function is
        specified, then it will apply the function to the result.

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

        # result=self.metadata["num_files"]
        # if rank is not None:
        #     result = result.filter(rank, axis=0)
        # if agg_function is None:
        #     return result
        # else:
        #     return agg_function(result)

        # groupby rank, then count the number of unique file names
        dataframe = self.groupby_aggregate(
            ["rank"], rank=rank, agg_dict={"file_name": "nunique"}, drop=True
        )
        dataframe = dataframe.rename(columns={"file_name": "file_count"})
        
        if agg_function == None:
            return dataframe
        # apply agg_function if it's not None
        else:
            return agg_function(dataframe)

    def file_access_count(self, rank: Optional[list] = None, agg_function: Optional[Callable] = None):
        """
        Depending on input arguments, return the number of accesses of each
        file in each rank selected by the user in the form of a DataFrame. If
        agg_function is specified, then it will apply the function to the
        result. The function first group the dataframe by file name. Then it
        goes through each group. And for each group, the function groups it by
        rank and aggregates to find the count for each rank. Then it filters
        ranks the user wants, and put result to a dictionary. After going
        through all file name groups. It use the dictionary to create a
        dataframe. If agg_function is specified, it applies the function on the
        dataframe.

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
        
        # groupby file name and rank, then count the number of each file name
        dataframe = self.groupby_aggregate(
            ["file_name", "rank"], rank=rank, agg_dict={"file_name": "count"}, drop=True
        )
        dataframe = dataframe.rename(columns={"file_name": "file_access_count"})
        
        if agg_function is None:
            return dataframe
        # group by file names and apply agg_function over ranks if it's not None
        else:
            dataframe = dataframe.groupby(level=[0]).agg(
                {"file_access_count": agg_function}
            )
            return dataframe
            
    def function_count(
        self, rank: Optional[list] = None, agg_function: Optional[Callable] = None
    ):
        """
        Identical to the previous one. Only instead of groupby file, it groupby
        function.

        Args:
            rank (None or a list): user selected ranks to get file count.
            agg_function: (function): aggregation function applying on the result.

        Return:
            Identical structure to the previous one, except the value here is the number of function
            calls for a function in selected ranks. Or avg/min/max accross selected ranks depending 
            on the agg_function

        """
        
        # groupby function name and rank, then count the number of each function name
        dataframe = self.groupby_aggregate(
            ["function_name", "rank"],
            rank=rank, agg_dict={"function_name": "count"},
            drop=True
        )
        dataframe = dataframe.rename(columns={"function_name": "function_count"})
        
        # group by function name and apply agg_function over ranks if it's not None
        if agg_function is None:
            return dataframe
        else:
            dataframe = dataframe.groupby(level=[0]).agg(
                {"function_count": agg_function}
            )
            return dataframe

    def function_time(
        self, rank: Optional[list] = None, agg_function: Optional[Callable] = None
    ):
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
       
        # groupby function name and rank, then sum the runtime 
        dataframe = self.groupby_aggregate(
            ["function_name", "rank"], rank=rank, agg_dict={"time": "sum"}, drop=True
        )
        
        if agg_function is None:
            return dataframe
        # group by function name and apply agg_function over ranks if it's not None
        else:
            dataframe = dataframe.groupby(level=[0]).agg({"time": agg_function})
            return dataframe


    def function_count_by_library(
        self, rank: Optional[list] = None, agg_function: Optional[Callable] = None
    ):
        """
        Count the number of function calls from mpi, hdf5 and posix. Same
        implementation to previous ones. But it first check the library for
        each function call, and then groupby the library.

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
            if "H5" in function:
                return "hdf5"
            elif "MPI" in function:
                return "mpi"
            else:
                return "posix"

        # check library for each row and put result into a new column
        self.dataframe["library"] = self.dataframe["function_name"].apply(
            lambda function: check_library(function)
        )
        
        # groupby library name and rank, then count the number of functions in each library
        dataframe = self.groupby_aggregate(
            ["library", "rank"], rank=rank, agg_dict={"library": "count"}, drop=True
        )
        
        # drop the new column to maintain the original dataframe
        self.dataframe.drop(["library"], axis=1)
        
        dataframe = dataframe.rename(columns={"library": "library_call_count"})
        
        # group by library name and apply agg_function over ranks if it's not None
        if agg_function is None:
            return dataframe
        else:
            dataframe = dataframe.groupby(level=[0]).agg(
                {"library_call_count": agg_function}
            )
            return dataframe


    def io_volume(self, by_rank: Optional[bool]=False, by_file: Optional[bool]=False):
        """
        Compute I/O volumes at different granularities. By default it returns
        the io volume of the whole run.  If by_rank is True, return a dataframe
        where each row corresponds to a rank and has the io volumn for it. If
        by_file is True, return a dataframe where each row corresponds to a
        file and has its io volume. If both are True, return a multi-index
        dataframe where each row corresponds to a file accessed by a rank and
        its io_volume

        Args:
            by_rank (bool): Show io volumn of each rank if true.
            by_file (bool): Show io volumn of each file if true.

        Return:
            A dataframe or a number depending on the granularity.

        """

        groupby_columns = []
        if by_rank:
            groupby_columns.append("rank")
        if by_file:
            groupby_columns.append("file_name")
        if not groupby_columns:
            return self.dataframe["io_volume"].sum()
        dataframe = self.groupby_aggregate(
            groupby_columns, rank = None, agg_dict={"io_volume": np.sum}, drop=True
        )

        return dataframe

    def percentage(self, function_type: str="io", by_rank: Optional[bool]=False, by_file: Optional[bool]=False):
        """
        Compute the percentage of time spent in a type of functions. By
        default it returns the percentage of io time vs the whole run. If
        by_rank is True, return a dataframe where each row corresponds to a
        rank and has the time spent in function_type vs time spent in that
        rank. If by_file is True, return a dataframe where each row corresponds
        to a file and has its time spent in function_type vs the total runtime
        (only meaning for if function_type is io) If both are Ture, return a
        multi-index dataframe where each row corresponds to a file accessed by
        a rank and its time spent in function_type vs time spent in that rank.
        (only meaning for if function_type is io) 

        Args:
            function_type (str): the function type 
            by_rank (bool): Show io volumn of each rank if true.
            by_file (bool): Show io volumn of each file if true.

        Return:
            A dataframe or a number depending on the granularity.

        """

        if function_type == "io":
            function_type = "write,read,other_io"
        if by_rank and not by_file:
            dataframe = self.groupby_aggregate(["rank"], rank = None, agg_dict={"time": np.sum}, filter_lambda=lambda x: x["function_type"] in function_type, drop=True)
            dataframe = dataframe.join(self.metadata["time"], lsuffix="_io", rsuffix="_total")
            dataframe["percentage"] = dataframe["time_io"] / dataframe["time_total"]
            return dataframe
        
        if by_file and not by_rank:
            total_runtime = self.metadata["end_timestamp"].max() - self.metadata["start_timestamp"].min()
            dataframe = self.groupby_aggregate(["file_name"], rank = None, agg_dict={"time": np.sum}, filter_lambda=lambda x: x["function_type"] in function_type, drop=True)
            dataframe["percentage"] = dataframe["time"] / total_runtime
            return dataframe
        
        if by_file and by_rank:
            dataframe = self.groupby_aggregate(["rank", "file_name"], rank = None, agg_dict={"time": np.sum}, filter_lambda=lambda x: x["function_type"] in function_type, drop=True)
            dataframe = dataframe.reset_index()
            dataframe = dataframe.merge(self.metadata[["rank", "time"]], on=["rank"], suffixes=("_io_this_rank", "_total_this_rank"))
            dataframe["percentage"] = dataframe["time_io_this_rank"] / dataframe["time_total_this_rank"]
            dataframe = dataframe.set_index(["rank", "file_name"])
            return dataframe
        
        total_runtime = self.metadata["time"].sum()    
        time = self.dataframe[self.dataframe.apply(lambda x: x["function_type"] in function_type, axis=1)]["time"].sum()
        return time / total_runtime
        
    def file_info(self):
        """
        Organize file information (num of access, io_volume, time spent) to a dataframe
        
        Args:

        Return:
            A multi-index dataframe containing information of a file operated by a rank for all files and ranks.

        """
        dataframe = self.groupby_aggregate(["file_name","rank","function_type"], agg_dict={"file_name": "count", "io_volume": np.sum, "time": np.sum}, drop=True, dropna=True)
        dataframe = dataframe.rename(columns={"file_name": "file_access_count"})
        return dataframe


    def shared_files(self):
        """
        Organize shared file information to a dataframe. Besides num of access, io_volume, time spent, include number
        of ranks that share the file
        
        Args:

        Return:
            A multi-index dataframe containing information of a file shared by some ranks for all files.

        """
        dataframe = self.groupby_aggregate(["file_name", "function_type"], agg_dict={"rank": "nunique", "file_name": "count", "io_volume": np.sum, "time": np.sum}, drop=True, dropna=True)
        dataframe = dataframe.rename(columns={"file_name": "file_access_count"})
        dataframe = dataframe.rename(columns={"rank": "num_ranks"})
        return dataframe

    def rank_involved_IO(self):
        """
        Organize rank information to a dataframe.
        
        Args:

        Return:
            A multi-index dataframe containing information of a rank.

        """
        dataframe = self.groupby_aggregate(["rank","file_name","function_type"], agg_dict={"file_name": "count", "io_volume": np.sum, "time": np.sum}, drop=True, dropna=False)
        dataframe = dataframe.rename(columns={"file_name": "file_access_count"})
        return dataframe
    
