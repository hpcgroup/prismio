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

def pipeline(trace_path):
    io_frame = IOFrame.from_recorder(trace_path, include_io_to_system_file=False)
    x = feature_extraction(io_frame)
    y = prediction(x)
    print(y)
    
def feature_extraction(io_frame):
    features = [
        'transferSize(-t)', 
        'read-after-read', 
        'read-after-write', 
        'read-renamed', 
        'fsync(-e)', 
        'fsyncPerWrite(-Y)', 
        'useFileView(-V)', 
        'collective(-c)',
        'write-after-write',
        'write-after-read',
        'preallocate(-p)', 
        'uniqueDir(-u)', 
        'POSIX',
        'write-scratch',
        'MPIIO',
    ]

    transferSize = io_frame.getTransferSize()
    readWritePattern = io_frame.getReadWritePattern()
    fsync = io_frame.isFsync()
    fsyncPerWrite = io_frame.isFsyncPerWrite()
    useFileView = io_frame.isUseFileView()
    collective = io_frame.isCollective()
    uniqueDir = io_frame.isUniqueDir()
    interface = io_frame.getAPI()

    x = pd.DataFrame(columns = features)
    x.loc[0] = [
        transferSize,
        readWritePattern['RAR'],
        readWritePattern['RAW'],
        readWritePattern['read-unseen-file'],
        fsync,
        fsyncPerWrite,
        useFileView,
        collective,
        readWritePattern['WAW'],
        readWritePattern['WAR'],
        0,
        uniqueDir,
        interface['POSIX'],
        readWritePattern['write-from-scratch'],
        interface['MPIIO'],
    ]
    return x

def prediction(x):
    model_file = "/Users/henryxu/Desktop/io-project/jupyter-notebook/model/pickles/DecisionTreeClassifier-Original data-15features-iter1"
    model = pickle.load(open(model_file, 'rb'))
    return model.predict(x)
