import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(1, str(Path(__file__).parent.parent.absolute()) + '/external/tools/reporter')
import creader_wrapper

class IOFrame:
    def __init__(self, df, log_dir, np, fd_to_file_name):
        self.df = df
        self.log_dir = log_dir
        self.np = np
        self.fd_to_file_name = fd_to_file_name

    @staticmethod
    def from_recorder(log_dir):
        from readers.recorder_reader import RecorderReader
        return RecorderReader(log_dir).read()

    def filter(self, my_lambda): 
        df = self.df[self.df.apply(my_lambda, axis = 1)]
        df = df.reset_index()
        df = df.drop('index', axis=1)
        return IOFrame(df, self.log_dir, self.np, self.fd_to_file_name)

    def groupby_aggregate(self, groupby_columns, agg_function):
        groupby_obj = self.df.groupby(groupby_columns)
        agg_df = groupby_obj.agg(agg_function)
        return agg_df
    