import matplotlib.pyplot as plt
import seaborn as sns
import os


BBOX_X, BBOX_Y = 1.05, 1.0
LEGEND_LOC = 'upper left'
SAVE_DIR = os.path.abspath('')

class IOFramePlotterRevised:
    def __init__(self, io_frame):
        self.io_frame = io_frame

    def assign_labels(self, title, xlabel, ylabel, rotation):
        plt.title(title)
        plt.xticks(rotation=rotation)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(bbox_to_anchor=(BBOX_X, BBOX_Y), loc=LEGEND_LOC)

    def save_plot(self, file_name=None):
        if file_name:
            plt.savefig(SAVE_DIR + '/' + file_name, bbox_inches = 'tight')
        else:
            plt.show()

    def output_plot(self, title, xlabel, ylabel, rotation, file_name=None):
        self.assign_labels(title, xlabel, ylabel, rotation)
        self.save_plot(file_name)
        plt.clf()

    def plot_stacked_function_count(self, function_major=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Count', 'Rank', 'Count', 0

        df = self.io_frame.function_count()

        if function_major:
            df = df.swaplevel()
            rotation = 90

        df = df.unstack(fill_value=0)
        df.columns = df.columns.droplevel()

        for rank in df.columns.to_list():
            df[rank] = df[rank] / df[rank].sum()

        df = df.T
        df.plot(kind='bar', stacked=True, ec='black')
        
        self.output_plot(title, xlabel, ylabel, rotation, file_name)
    
    def plot_aggregate_function_count(self, sort=True, ascending=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Count', 'Rank', 'Count', 90
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

        self.output_plot(title, xlabel, ylabel, rotation, file_name)

    def plot_function_count(self, function_major=False, sort=True, ascending=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Count', 'Rank', 'Count', 0

        df = self.io_frame.function_count()
        df.reset_index(inplace=True)
        if sort:
            df.sort_values('function_count', ascending=ascending, inplace=True)
        
        if function_major:
            sns.barplot(x='function_name', y='function_count', hue='rank', data=df, ec='black')
            rotation = 90
        else:
            sns.barplot(x='rank', y='function_count', hue='function_name', data=df, ec='black')

        self.output_plot(title, xlabel, ylabel, rotation, file_name)

    def plot_function_time(self, function_major=False, sort=True, ascending=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Time', 'Rank', 'Time', 0
        df = self.io_frame.function_time()

        df.reset_index(inplace=True)

        if sort:
            df.sort_values('time', ascending=ascending, inplace=True)

        if function_major:
            sns.barplot(x='function_name', y='time', hue='rank', data=df, ec='black')
            rotation = 90
        else:
            sns.barplot(x='rank', y='time', hue='function_name', data=df, ec='black')

        self.output_plot(title, xlabel, ylabel, rotation, file_name)

    def plot_stacked_function_time(self, function_major=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Time', 'Rank', 'Time', 0
        df = self.io_frame.function_time()

        if function_major:
            df = df.swaplevel()

        df = df.unstack(fill_value=0)
        df.columns = df.columns.droplevel()

        for rank in df.columns.to_list():
            df[rank] = df[rank] / df[rank].sum()
    
        df=df.T
        df.plot(kind='bar', stacked=True, ec='black')

        self.output_plot(title, xlabel, ylabel, rotation, file_name)

    def plot_aggregate_function_time(self, sort=True, ascending=False, file_name=None):
        title, xlabel, ylabel, rotation = 'Function Time', 'Rank', 'Time', 90
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

        self.output_plot(title, xlabel, ylabel, rotation, file_name)
        