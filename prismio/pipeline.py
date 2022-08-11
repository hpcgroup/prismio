from asyncore import readwrite
import sys
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
sys.path.insert(1, os.path.join(sys.path[0], '/Users/henryxu/Desktop/Research/prismio'))
from prismio.io_frame import IOFrame
import seaborn as sns
import pickle
import time

def pipeline(trace_path, target='system', system=None):
    features = [
        'transferSize(-t)', 
        'read-after-read', 
        'read-after-write', 
        'read-renamed', 
        'fsyncPerWrite(-Y)', 
        'fsync(-e)', 
        'collective(-c)',
        'useFileView(-V)', 
        'write-after-write',
        'write-after-read',
        'preallocate(-p)', 
        'uniqueDir(-u)', 
        'MPIIO',
        'write-scratch',
        'POSIX',
    ]

    pipeline_start = time.perf_counter()

    X = pd.DataFrame(columns = features)

    read_start = time.perf_counter()
    io_frame = IOFrame.from_recorder(trace_path, include_io_to_system_file=False)
    read_end = time.perf_counter()
    print()
    print("Time of reading Recorder trace: " + str(read_end - read_start))
    print()
    
    extract_start = time.perf_counter()
    sharedDirectories = io_frame.shared_directories()

    groups = io_frame.dataframe.groupby(['file_name'])
    for file, group in groups:
        io_frame_file = IOFrame(group, None)
        x = feature_extraction(io_frame_file)
        directory = file.rsplit('/', 1)[0]
        if sharedDirectories.loc[directory]['num_ranks'] > 1:
            x.insert(11, 0)
        else:
            x.insert(11, 1)
        print(file)
        print(x)
        X.loc[file] = x
    
    extract_end = time.perf_counter()
    print()
    print("Time of feature extraction: " + str(extract_end - extract_start))
    print()

    predict_start = time.perf_counter()
    if target == 'system':
        y = predictSystem(X)
        X['predicted_system'] = np.where(y == 0, 'bb', 'gpfs')
    elif target == 'BW':
        y = predictBW(X, system)
        X['predictedBW'] = y
    # print(X)
    predict_end = time.perf_counter()
    print()
    print("Time of loading model and prediction: " + str(predict_end - predict_start))
    print()

    pipeline_end = time.perf_counter()
    print()
    print("Time of the whole pipeline: " + str(pipeline_end - pipeline_start))
    print()

    return X
    
def feature_extraction(io_frame):

    transferSize = io_frame.getTransferSize()
    readWritePattern = io_frame.getReadWritePattern()
    fsync = io_frame.isFsync()
    fsyncPerWrite = io_frame.isFsyncPerWrite()
    useFileView = io_frame.isUseFileView()
    collective = io_frame.isCollective()
    interface = io_frame.getAPI()

    x = [
        transferSize,
        readWritePattern['RAR'],
        readWritePattern['RAW'],
        readWritePattern['read-unseen-file'],
        fsyncPerWrite,
        fsync,
        collective,
        useFileView,
        readWritePattern['WAW'],
        readWritePattern['WAR'],
        0,
        interface['MPIIO'],
        readWritePattern['write-from-scratch'],
        interface['POSIX'],
    ]
    return x

def predictSystem(X):
    model_file = "/Users/henryxu/Desktop/io-project/jupyter-notebook/model/pickles/DecisionTreeClassifier-Original data-15features-iter1"
    model = pickle.load(open(model_file, 'rb'))
    return model.predict(X)

def predictBW(X, system):
    model_file = "/Users/henryxu/Desktop/io-project/jupyter-notebook/model/pickles/6D-LinearRegression-MinMaxScaler-iter9"
    model = pickle.load(open(model_file, 'rb'))
    X['lassen-gpfs'] = np.where(system == 'lassen-gpfs', 1, 0)
    X['lassen-bb'] = np.where(system == 'lassen-bb', 1, 0)
    for i in range(2,7):
        X["transferSize(-t)^" + str(i)] = X["transferSize(-t)"] ** i

    # print(X.columns)
    # from sklearn.preprocessing import MinMaxScaler
    # scaler = MinMaxScaler()
    # X = scaler.fit_transform(X)
    X["transferSize(-t)"] = X["transferSize(-t)"] / 1000000000
    for i in range(2, 7):
        X["transferSize(-t)^" + str(i)] = X["transferSize(-t)^" + str(i)] / (1000000000 ** i)

    # print(X)

    return model.predict(X)
