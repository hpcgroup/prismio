import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
from prismio.io_frame import IOFrame


class IOFramePlotter:
    def __init__(self, io_frame, save_dir='/Users/henryxu/Desktop') -> None:
        self.save_dir = save_dir
        self.io_frame = io_frame

    def plot_function_count(self, aggregate=True, function_major=False, stacked=False, sort=True, ascending=False, save=False, filename='function_count'):
        if aggregate:
            df = self.io_frame.function_count(agg_function=['min', 'mean', 'max'])
            df.columns = df.columns.droplevel()
            if sort:
                df.sort_values('mean', ascending=ascending, inplace=True)
            df.reset_index(inplace=True)
            df['ymin'] = df['mean'] - df['min']
            df['ymax'] = df['max'] - df['mean']
            yerr = df[['ymin', 'ymax']].T.to_numpy()
            plt.rcParams['errorbar.capsize'] = 10
            sns.barplot(x='function_name', y='mean', data=df, yerr=yerr, ec='black')
            plt.xticks(rotation = 45)
            plt.ylabel('count')
            plt.xlabel('rank')
            plt.title("function count")
            if save:
                plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
            else:
                plt.show()
            plt.clf()
            return
        
        df = self.io_frame.function_count()

        if stacked:
            if function_major:
                df = df.swaplevel()
            df = df.unstack(fill_value=0)
            df.columns = df.columns.droplevel()
            for rank in df.columns.to_list():
                df[rank] = df[rank] / df[rank].sum()
            df=df.T
            df.plot(kind='bar', stacked=True, ec='black')
        else:
            df.reset_index(inplace=True)
            if sort:
                df.sort_values('function_count', ascending=ascending, inplace=True)
            if function_major:
                sns.barplot(x='function_name', y='function_count', hue='rank', data=df, ec='black')
            else:
                sns.barplot(x='rank', y='function_count', hue='function_name', data=df, ec='black')
        
        if function_major:
            plt.xticks(rotation = 45)
        else:
            plt.xticks(rotation = 0)
        plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
        plt.ylabel('count')
        plt.xlabel('rank')
        plt.title("function count")
        if save:
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
        else:
            plt.show()
        plt.clf()

    def plot_function_time(self, aggregate=True, function_major=False, stacked=False, sort=True, ascending=False, save=False, filename='function_time'):
        if aggregate:
            df = self.io_frame.function_time(agg_function=['min', 'mean', 'max'])
            df.columns = df.columns.droplevel()
            df.reset_index(inplace=True)
            if sort:
                df.sort_values('mean', ascending=ascending, inplace=True)
            df['ymin'] = df['mean'] - df['min']
            df['ymax'] = df['max'] - df['mean']
            yerr = df[['ymin', 'ymax']].T.to_numpy()
            plt.rcParams['errorbar.capsize'] = 10
            sns.barplot(x='function_name', y='mean', data=df, yerr=yerr, ec='black')
            plt.xticks(rotation = 45)
            plt.ylabel('time')
            plt.xlabel('rank')
            plt.title("function time")
            if save:
                plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
            else:
                plt.show()
            plt.clf()
            return
        
        df = self.io_frame.function_time()

        if stacked:
            if function_major:
                df = df.swaplevel()
            df = df.unstack(fill_value=0)
            df.columns = df.columns.droplevel()
            for rank in df.columns.to_list():
                df[rank] = df[rank] / df[rank].sum()
            df=df.T
            df.plot(kind='bar', stacked=True, ec='black')
        else:
            df.reset_index(inplace=True)
            if sort:
                df.sort_values('time', ascending=ascending, inplace=True)
            if function_major:
                sns.barplot(x='function_name', y='time', hue='rank', data=df, ec='black')
            else:
                sns.barplot(x='rank', y='time', hue='function_name', data=df, ec='black')
        
        if function_major:
            plt.xticks(rotation = 45)
        else:
            plt.xticks(rotation = 0)
        plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
        plt.ylabel('time')
        plt.xlabel('rank')
        plt.title("function time")
        if save:
            plt.savefig(self.save_dir + '/' + filename, bbox_inches='tight')
        else:
            plt.show()
        plt.clf()
