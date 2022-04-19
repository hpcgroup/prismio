import matplotlib.pyplot as plt
import seaborn as sns
import os

BBOX_X, BBOX_Y = 1.05, 1.0
LEGEND_LOC = 'upper left'
SAVE_DIR = os.path.abspath('')

def assign_labels(title, xlabel, ylabel, legend_title, rotation):
    plt.title(title)
    plt.xticks(rotation=rotation)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if legend_title != "":
        plt.legend(bbox_to_anchor=(BBOX_X, BBOX_Y), loc=LEGEND_LOC, title=legend_title)

def save_plot(file_name=None):
    if file_name:
        plt.savefig(SAVE_DIR + '/' + file_name, bbox_inches= 'tight')
    else:
        plt.show()

def output_plot(title="", xlabel="", ylabel="", legend_title="", rotation=0, file_name=None):
    assign_labels(title, xlabel, ylabel, legend_title, rotation)
    save_plot(file_name)
    plt.clf()

def get_plot_labels(dataframe):
    df = dataframe.reset_index()
    column_names = df.columns
    
    labels = column_names[0], column_names[1], column_names[2]
    return labels

def plot(dataframe, function_major=False, sort=True, ascending=False, file_name=None):
    xlabel, legend_title, ylabel = get_plot_labels(dataframe)

    df = dataframe.reset_index()
    rotation = 0

    if sort:
        df.sort_values(ylabel, ascending=ascending, inplace=True)

    if function_major:
        sns.barplot(x=xlabel, y=ylabel, 
                    hue=legend_title, data=df, ec='black')
        rotation = 90
    else:
        xlabel, legend_title = legend_title, xlabel
        sns.barplot(x=xlabel, y=ylabel,
                    hue=legend_title, data=df, ec='black')

    labels = xlabel, ylabel, legend_title
    xlabel, ylabel, legend_title = tuple([x.title().replace("_", " ") for x in labels])
    title = xlabel + " vs " + ylabel

    output_plot(title, xlabel, ylabel, legend_title, rotation, file_name)

def plot_stacked(dataframe, function_major=False, file_name=None):
    xlabel, legend_title, ylabel = get_plot_labels(dataframe)
    df, rotation = dataframe, 0

    if function_major:
        df = df.swaplevel()
        rotation = 90
    else:
        xlabel, legend_title = legend_title, xlabel

    df = df.unstack(fill_value=0)
    df.columns = df.columns.droplevel()

    for rank in df.columns.to_list():
        df[rank] = df[rank] / df[rank].sum()

    df = df.T
    df.plot(kind='bar', stacked=True, ec='black')

    labels = xlabel, ylabel, legend_title
    xlabel, ylabel, legend_title = tuple([x.title().replace("_", " ") for x in labels])
    title = xlabel + " vs " + ylabel 

    output_plot(title, xlabel, ylabel, legend_title, rotation=rotation)

def plot_yerr(dataframe, sort=True, ascending=False, file_name=None):
    df = dataframe
    title = "Yerr of " + df.columns[0][0].title().replace("_", " ")

    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)


    if sort:
        df.sort_values('mean', ascending=ascending, inplace=True)

    df['ymin'] = df['mean'] - df['min']
    df['ymax'] = df['max'] - df['min']
    yerr = df[['ymin', 'ymax']].T.to_numpy()

    plt.rcParams['errorbar.capsize'] = 10
    sns.barplot(x=df.columns[0], y='min', data=df, yerr=yerr, ec='black')

    output_plot(title, rotation=90, file_name=file_name)

def plot_aggregate(dataframe, sort=True, ascending=False, file_name=None):
    df = dataframe
    title = df.columns[0][0].title().replace("_", " ")

    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)

    xlabel, ylabel = df.columns[0], df.columns[1]

    title = "Aggregate " + ylabel.title() + " of " + title

    if sort:
        df.sort_values(ylabel, ascending=ascending, inplace=True)

    sns.barplot(x=xlabel, y=ylabel, data=df, ec='black')
    output_plot(title, xlabel, ylabel, rotation=90)


