import sys, math, time
import numpy as np
import pandas as pd
from math import pi
from bokeh.plotting import figure, output_file, show
from bokeh.embed import components
from bokeh.models import FixedTicker, ColumnDataSource, LabelSet
from prettytable import PrettyTable

from creader_wrapper import RecorderReader
from recorder_df import RecorderDataFrame
from recorder_reader import RecorderDataReader
from multiple_rdf_handler import MultiRecorderDFHandler
from html_writer import HTMLWriter
from build_offset_intervals import ignore_files
from build_offset_intervals import build_offset_intervals

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

log_dir1 = "./log_dirs/recorder-logs-1"
log_dir2 = './log_dirs/recorder-logs-8-hacc-8'
log_dir3 = './log_dirs/recorder-logs-8-hacc-16'
rdf1 = RecorderDataFrame.from_recorder(log_dir1)
rdf2 = RecorderDataFrame.from_recorder(log_dir2)
rdf3 = RecorderDataFrame.from_recorder(log_dir3)

rdfs=[rdf1,rdf2, rdf3]
m=MultiRecorderDFHandler(rdfs)
