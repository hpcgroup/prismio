import sys
import numpy as np
import pandas as pd
from io_frame import IOFrame


class MultiIOFrame():
    recorder_dfs = []
    
    def __init__(self, recorder_dfs):
        self.recorder_dfs = recorder_dfs
        self.set_log_dirs()
        self.np = recorder_dfs[0].np
        self.func_id_to_name = recorder_dfs[0].func_id_to_name

    def set_log_dirs(self):
        self.log_dirs = []
        for rdf in self.recorder_dfs:
            self.log_dirs.append(rdf.log_dir)

    def get_log_dirs(self):
        return self.log_dirs

    def get_record_count_aggregated_by_logs(self):
        columns = range(self.np)
        df = pd.DataFrame(data=[], columns=columns)
        for i, log_dir in enumerate(self.log_dirs):
            df.loc[log_dir] = self.recorder_dfs[i].get_record_count_each_rank()
        return df

    def get_record_count_aggregated_by_ranks(self):
        return self.get_record_count_aggregated_by_logs().transpose()

    def get_average_record_count_each_rank(self):
        return list(self.get_record_count_aggregated_by_logs().mean())

    def get_file_count_aggregated_by_logs(self):
        columns = range(self.np)
        df = pd.DataFrame(data=[], columns=columns)
        for i, log_dir in enumerate(self.log_dirs):
            df.loc[log_dir] = self.recorder_dfs[i].get_total_file_count_each_rank()
        return df

    def get_file_count_aggregated_by_ranks(self):
        return self.get_file_count_aggregated_by_logs().transpose()

    def get_function_count_aggregated_by_logs(self):
        columns = self.func_id_to_name
        df = pd.DataFrame(data=[], columns=columns)
        for i, log_dir in enumerate(self.log_dirs):
            df.loc[log_dir] = self.recorder_dfs[i].get_function_count()
        return df

    def get_function_count_aggregated_by_functions(self):
        return self.get_function_count_aggregated_by_logs().transpose()

    def get_nonzero_function_count_aggregated_by_functions(self):
        df = self.get_function_count_aggregated_by_functions()
        return df[df.sum(axis=1)>0]

    def get_nonzero_function_count_aggregated_by_logs(self):
        return self.get_nonzero_function_count_aggregated_by_functions().transpose()

    def get_function_time_aggregated_by_logs(self):
        columns = self.func_id_to_name
        df = pd.DataFrame(data=[], columns=columns)
        for i, log_dir in enumerate(self.log_dirs):
            df.loc[log_dir] = self.recorder_dfs[i].get_function_time()
        return df

    def get_function_time_aggregated_by_functions(self):
        return self.get_function_time_aggregated_by_logs().transpose()

    def get_nonzero_function_time_aggregated_by_functions(self):
        df = self.get_function_time_aggregated_by_functions()
        return df[df.sum(axis=1)>0]

    def get_nonzero_function_time_aggregated_by_logs(self):
        return self.get_nonzero_function_time_aggregated_by_functions().transpose()
