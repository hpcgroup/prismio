import sys
import numpy as np
import pandas as pd
from creader_wrapper import RecorderReader

class IOFrame:
    def __init__(self, df, log_dir, np, fd_to_file_name, func_id_to_name):
        self.df = df
        self.log_dir = log_dir
        self.np = np
        self.fd_to_file_name = fd_to_file_name
        self.func_id_to_name = func_id_to_name

    @staticmethod
    def from_recorder(log_dir):
        from recorder_reader import RecorderDataReader
        return RecorderDataReader(log_dir).read()

    def filter(self, my_lambda): 
        df = self.df[self.df.apply(my_lambda, axis = 1)]
        df = df.reset_index()
        df = df.drop('index', axis=1)
        return IOFrame(df, self.log_dir, self.np, self.fd_to_file_name, self.func_id_to_name)
    
    # only meaningful for column in 'rank', 'func_id', 'func_name', 'file_name'
    def get_aggregated_count_by_column(self, column):
        return dict(self.df.groupby([column]).count().iloc[:,0])

    # only meaningful for column in 'func_id', 'func_name', 'file_name'
    def get_aggregated_count_by_column_each_rank(self, column):
        counts = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            counts.append(dict(df.groupby([column]).count().iloc[:,0]))
        return counts

    def get_data_frame(self):
        return self.df
    
    def get_record_count(self):
        return self.df.shape[0]

    def get_record_count_each_rank(self):
        return list(self.df.groupby(['rank']).count().iloc[:,0])
    
    def get_nonzero_function_count(self):
        return dict(self.df.groupby(['func_name']).count().iloc[:,0])

    def get_function_count(self):
        count = [0] * 222
        nonzero_function_count = self.get_nonzero_function_count()
        for func_id in nonzero_function_count:
            count[func_id] += nonzero_function_count[func_id]
        return count

    def get_nonzero_function_count_each_rank(self):
        counts = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            counts.append(dict(df.groupby(['func_name']).count().iloc[:,0]))
        return counts

    def get_function_count_each_rank(self):
        counts = []
        nonzero_counts = self.get_nonzero_function_count_each_rank()
        for nonzero_count in nonzero_counts:
            count = [0] * 222
            for func_id in nonzero_count:
                count[func_id] += nonzero_count[func_id]
            counts.append(count)
        return counts

    def get_file_access_count(self):
        dic = dict(self.df.groupby(['file_name']).count().iloc[:,0])
        if ('__unknown__' in dic):
            dic.pop('__unknown__')
        return dic

    def get_total_file_access_count(self):
        return sum(self.get_file_access_count().values())

    def get_file_access_count_each_rank(self):
        counts = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            dic = dict(df.groupby(['file_name']).count().iloc[:,0])
            if ('__unknown__' in dic):
                dic.pop('__unknown__')
            counts.append(dic)
        return counts

    def get_total_file_access_count_each_rank(self):
        counts = []
        for dic in self.get_file_access_count_each_rank():
            counts.append(sum(dic.values()))
        return counts

    def get_files(self):
        return list(self.df.file_name.unique())
    
    def get_files_each_ranks(self):
        files = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            files.append(list(df.file_name.unique()))
        return files

    def get_total_file_count(self):
        files = self.df.file_name.unique()
        n = len(files)
        if None in files:
            n = n - 1
        if '__unknown__' in files:
            n = n - 1
        if 'stdin' in files:
            n = n - 1
        if 'stdout' in files:
            n = n - 1
        if 'stderr' in files:
            n = n - 1
        return n

    def get_total_file_count_each_rank(self):
        count = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            files = df.file_name.unique()
            n = len(files)
            if None in files:
                n = n - 1
            if '__unknown__' in files:
                n = n - 1
            if 'stdin' in files:
                n = n - 1
            if 'stdout' in files:
                n = n - 1
            if 'stderr' in files:
                n = n - 1
            count.append(n)
        return count

    def get_total_function_time(self):
        return self.df['telapsed'].sum()

    def get_nonzero_function_time(self):
        return dict(self.df.groupby(['func_name'])['telapsed'].sum())

    def get_function_time(self):
        time = [0] * 222
        nonzero_function_time = self.get_nonzero_function_time()
        for func_id in nonzero_function_time:
            time[func_id] += nonzero_function_time[func_id]
        return time
    
    def get_total_function_time_each_rank(self):
        return list(self.df.groupby(['rank'])['telapsed'].sum())

    def get_nonzero_function_time_each_rank(self):
        times = []
        for rank in range(self.np):
            df = self.df[self.df['rank'] == rank]
            times.append(dict(df.groupby(['func_name'])['telapsed'].sum()))
        return times
    
    def get_function_time_each_rank(self):
        times = []
        nonzero_times = self.get_nonzero_function_time_each_rank()
        for nonzero_time in nonzero_times:
            time = [0] * 222
            for func_id in nonzero_time:
                time[func_id] += nonzero_time[func_id]
            times.append(time)
        return times

    def get_function_layers(self):
        layers = {'hdf5':0, 'mpi':0, 'posix':0 }
        dic = dict(self.df.groupby(['func_name'])['func_name'].count())
        for key in dic:
            if "H5" in key:
                layers['hdf5'] += dic[key]
            elif "MPI" in key:
                layers['mpi'] += dic[key]
            else:
                layers['posix'] += dic[key]
        return layers
    
    def get_overall_read_activities(self):
        read = {}
        for i in range(self.np):
            read[i] = []
        for i in range(self.df.shape[0]):
            row = self.df.iloc[i]
            func_name = row['func_name']
            if "MPI" in func_name or "H5" in func_name: 
                continue
            if "read" in func_name:
                rank = row['rank']
                read[rank].append(row['tstart'])
                read[rank].append(row['tend'])
        return read

    def get_overall_write_activities(self):
        write = {}
        for i in range(self.np):
            write[i] = []
        for i in range(self.df.shape[0]):
            row = self.df.iloc[i]
            func_name = row['func_name']
            if "MPI" in func_name or "H5" in func_name: 
                continue
            if "write" in func_name or "fprintf" in func_name:
                rank = row['rank']
                write[rank].append(row['tstart'])
                write[rank].append(row['tend'])
        return write
