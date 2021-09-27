import sys, math, time, os
import numpy as np
import pandas as pd

sys.path.insert(1, os.getcwd())
sys.path.insert(1, os.getcwd() + "/..")
sys.path.insert(1, os.getcwd() + "/../..")

# print(sys.path)

from prismio.io_frame import IOFrame
from prismio.readers.recorder_reader import RecorderReader
from prismio.multi_io_frame import MultiIOFrame

import recorder_viz

def test_recorder_reader():
    simple_test = IOFrame.from_recorder("./data/recorder-simple")
    dataframe = simple_test.dataframe
    assert dataframe.shape[0] == 12
    assert dataframe.shape[1] == 11
    columns = ['rank', 'function_id', 'function_name', 'tstart', 'tend', 'time', 'arg_count',
       'args', 'return_value', 'file_name', 'io_volume']
    for i, attribute in enumerate(columns):
        assert dataframe.columns[i] == attribute
    
    
def test_filter():
    simple_test = IOFrame.from_recorder("./data/recorder-simple")
    filtered_io_frame = simple_test.filter(lambda x: x['time'] > 0.00001)
    dataframe = filtered_io_frame.dataframe
    assert len(dataframe) == 5
    assert dataframe['time'].min() > 0.00001
    filtered_io_frame = simple_test.filter(lambda x: 'write' in x['function_name'])
    dataframe = filtered_io_frame.dataframe
    assert len(dataframe) == 2

def test_groupby_aggregate():
    simple_test = IOFrame.from_recorder("./data/recorder-simple")
    grouped_io_frame = simple_test.groupby_aggregate(['rank'], agg_dict={'time': 'sum', 'arg_count': 'mean'})
    dataframe = grouped_io_frame
    assert dataframe['time'][0] == 0.00035899999999999994
    assert dataframe['arg_count'][0] == 3.3333333333333335
    assert dataframe['time'][1] == 0.0016940000000000002
    assert dataframe['arg_count'][1] == 3.5

    grouped_io_frame = simple_test.groupby_aggregate(['rank', 'function_name'], agg_dict={'time': 'sum'})
    dataframe = grouped_io_frame
    assert dataframe.loc[0].loc['fwrite']['time'] == 7.000000000000062e-06
    assert dataframe.loc[1].loc['MPI_Recv']['time'] == 0.00114

def test_file_count():
    ior_io_frame = IOFrame.from_recorder("./data/recorder-ior")
    file_count = ior_io_frame.file_count(agg_function=np.mean)
    assert len(file_count) == 1
    assert file_count[0] == 3.0
    file_count = ior_io_frame.file_count()
    assert len(file_count) == 2
    assert file_count.loc[0]['num_files'] == 3
    assert file_count.loc[1]['num_files'] == 3
    file_count = ior_io_frame.file_count(rank=[0])
    assert len(file_count) == 1
    assert file_count.loc[0]['num_files'] == 3

def test_file_access_count():
    ior_io_frame = IOFrame.from_recorder("./data/recorder-ior")
    file_access_count = ior_io_frame.file_access_count(agg_function=np.mean)
    assert len(file_access_count) == 4
    assert file_access_count.loc['/dev/shm/job2443846470-34168-OMPI_COLL_IBM-0-collshm-comm4-master0']['access_count'] == 4
    assert file_access_count.loc['stdout']['access_count'] == 122
    file_access_count = ior_io_frame.file_access_count(rank=[0], agg_function=np.mean)
    assert len(file_access_count) == 3
    assert file_access_count.loc['/g/g92/xu23/research/performance-analysis/ior/testFile']['access_count'] == 12
    assert file_access_count.loc['stdout']['access_count'] == 122
    file_access_count = ior_io_frame.file_access_count()
    assert len(file_access_count) == 6
    assert file_access_count.loc['/dev/shm/job2443846470-34168-OMPI_COLL_IBM-0-collshm-comm4-master0'].loc[0]['access_count'] == 6
    file_access_count = ior_io_frame.file_access_count(agg_function='sum')
    assert file_access_count.loc['/g/g92/xu23/research/performance-analysis/ior/testFile']['access_count'] == 24

def test_function_count():
    ior_io_frame = IOFrame.from_recorder("./data/recorder-ior")
    function_count = ior_io_frame.function_count(agg_function=np.mean)
    assert len(function_count) == 22
    assert function_count.loc['MPI_Allreduce']['num_calls'] == 8.0
    assert function_count.loc['write']['num_calls'] == 2.0
    function_count = ior_io_frame.function_count(rank=[1], agg_function=np.mean)
    assert len(function_count) == 19
    assert function_count.loc['MPI_Reduce']['num_calls'] == 14
    function_count = ior_io_frame.function_count()
    assert len(function_count) == 41
    assert function_count.loc['MPI_Allreduce', 0]['num_calls'] == 8
    assert function_count.loc['access', 0]['num_calls'] == 1
    function_count = ior_io_frame.function_count(agg_function='sum')
    assert function_count.loc['__xstat']['num_calls'] == 5
    assert function_count.loc['vfprintf']['num_calls'] == 116

def test_function_time():
    ior_io_frame = IOFrame.from_recorder("./data/recorder-ior")
    function_time = ior_io_frame.function_time(agg_function=np.mean)
    assert len(function_time) == 22
    assert function_time.loc['MPI_Allreduce']['cumulative_time'] == 0.00024650000000000236
    assert function_time.loc['write']['cumulative_time'] == 2.2499999999999517e-05
    function_time = ior_io_frame.function_time(rank=[1], agg_function=np.mean)
    assert len(function_time) == 19
    assert function_time.loc['MPI_Reduce']['cumulative_time'] == 1.3999999999997521e-05
    function_time = ior_io_frame.function_time()
    assert len(function_time) == 41
    assert function_time.loc['MPI_Allreduce', 0]['cumulative_time'] == 8.200000000000264e-05
    assert function_time.loc['access', 0]['cumulative_time'] == 5.000000000000664e-06
    function_time = ior_io_frame.function_time(agg_function='max')
    assert function_time.loc['__xstat']['cumulative_time'] == 0.00014500000000000103
    assert function_time.loc['vfprintf']['cumulative_time'] == 7.199999999999741e-05

def test_function_calls_by_library():
    ior_io_frame = IOFrame.from_recorder("./data/recorder-ior")
    layer = ior_io_frame.function_count_by_library(agg_function=np.mean)
    assert len(layer) == 2
    assert layer.loc['mpi']['func_count_of_lib'] == 43.5
    assert layer.loc['posix']['func_count_of_lib'] == 91
    layer = ior_io_frame.function_count_by_library(rank=[0], agg_function=np.mean)
    assert layer.loc['mpi']['func_count_of_lib'] == 44
    assert layer.loc['posix']['func_count_of_lib'] == 153
    layer = ior_io_frame.function_count_by_library()
    assert len(layer) == 4
    assert layer.loc['mpi', 0]['func_count_of_lib'] == 44
    assert layer.loc['posix', 1]['func_count_of_lib'] == 29
    layer = ior_io_frame.function_count_by_library(rank=[1], agg_function=np.sum)
    assert layer.loc['mpi']['func_count_of_lib'] == 43
    assert layer.loc['posix']['func_count_of_lib'] == 29
