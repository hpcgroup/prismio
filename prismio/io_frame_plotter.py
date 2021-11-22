import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib as mpl
from prismio.io_frame import IOFrame
from matplotlib.pyplot import cm

class IOFramePlotter:
    def __init__(self, io_frame, save_dir='/Users/henryxu/Desktop') -> None:
        self.save_dir = save_dir
        self.io_frame = io_frame

    def plot_function_count(self, stacked=False, filename='function_count'):
        df = self.io_frame.function_count()
        functions = df.index.get_level_values(level=0)
        ranks = df.index.get_level_values(level=1).unique()
        num_functions = len(functions)
        num_ranks = len(ranks)
        colors = cm.rainbow(np.linspace(0, 1, num_functions))
        
        if stacked:
            bottom = np.zeros(num_ranks)
            for function, color in zip(functions, colors):
                y = df.loc[function]['function_count']
                y = y.reindex(list(range(num_ranks)), fill_value = 0)
                plt.bar(ranks, y, bottom=bottom, label=function, color=color)
                bottom += y

            plt.xticks(np.arange(num_ranks), ranks, rotation=0)
            plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
            plt.ylabel('count')
            plt.xlabel('rank')
            plt.title("function count")
            plt.rcParams['figure.figsize'] = (30.0, 16.0)
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
        else:
            total_width = 0.8
            width = total_width / num_functions

            for i, function in enumerate(functions):
                y = df.loc[function]['function_count']
                y = y.reindex(list(range(num_ranks)), fill_value = 0)
                plt.bar(ranks - total_width / 2 + (i - 0.5) * width, y, width=width, label=function, color=colors[i])

            plt.xticks(np.arange(num_ranks), ranks, rotation=0)
            plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
            plt.ylabel('function_count')
            plt.xlabel('rank')
            plt.title("function time")
            plt.rcParams['figure.figsize'] = (30.0, 16.0)
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')


    def plot_function_time(self, stacked=False, filename='function_time'):
        df = self.io_frame.function_time()
        functions = df.index.get_level_values(level=0)
        ranks = df.index.get_level_values(level=1).unique()
        num_functions = len(functions)
        num_ranks = len(ranks)
        colors = cm.rainbow(np.linspace(0, 1, num_functions))

        if stacked:
            bottom = np.zeros(num_ranks)
            for function, color in zip(functions, colors):
                y = df.loc[function]['time']
                y = y.reindex(list(range(num_ranks)), fill_value = 0)
                plt.bar(ranks, y, bottom=bottom, label=function, color=color)
                bottom += y

            plt.xticks(np.arange(num_ranks), ranks, rotation=0)
            plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
            plt.ylabel('time')
            plt.xlabel('rank')
            plt.title("function time")
            plt.rcParams['figure.figsize'] = (30.0, 16.0)
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')

        else:
            total_width = 0.8
            width = total_width / num_functions

            for i, function in enumerate(functions):
                y = df.loc[function]['time']
                y = y.reindex(list(range(num_ranks)), fill_value = 0)
                plt.bar(ranks - total_width / 2 + (i - 0.5) * width, y, width=width, label=function, color=colors[i])

            plt.xticks(np.arange(num_ranks), ranks, rotation=0)
            plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
            plt.ylabel('time')
            plt.xlabel('rank')
            plt.title("function time")
            plt.rcParams['figure.figsize'] = (30.0, 16.0)
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
