from download import DataDownloader

import argparse
from matplotlib import gridspec
import matplotlib.pyplot as plt
import numpy as np

import os.path


def process_data(data_source):
    """
    Fnc separates date and region from whole data set and for each sets count
    :return: 2D array of counts for each year and region
    """
    date_coll = np.array([row[0].year for row in data_source[1]])
    reg_coll = np.array([row[-1] for row in data_source[1]])

    df = np.array([date_coll, reg_coll])

    colls = np.unique(df[0])  # years
    rows = np.unique(df[1])  # regs

    return colls, rows, np.array([[np.logical_and(df[0] == c, df[1] == r).sum() for c in colls] for r in rows])


def annotate_bars(rects, ax, df):
    """
    Fnc annotates created bar plot by oder of bars
    :param rects: rectangles in plot
    :param ax: current axes
    :param df: data set
    """
    arg_sort = np.argsort(df)
    indexes = [0] * len(df)

    for i in range(0, len(df)):
        indexes[arg_sort[i]] = i

    for i in range(0, len(rects)):
        height = rects[i].get_height()
        ax.annotate('{}'.format(indexes[i] + 1),
                    xy=(rects[i].get_x() + rects[i].get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


def get_fig(years, regs, df):
    """
    Fnc creates figure with 6 subplots for each year
    :param df: data set
    :return: created figure
    """
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.suptitle("Přehled nehodovosti v krajích za rok 2016-2020", fontsize=16, fontweight='bold')

    spec = gridspec.GridSpec(ncols=2, nrows=3, figure=fig, hspace=0.4)
    axes = list()

    for i in range(0, 3):
        for j in range(0, 2):
            if i == 2 and j == 1: break
            axes.append(fig.add_subplot(spec[i, j]))

    for i in range(0, len(axes)):
        ax = axes[i]
        rects = ax.bar(regs, df[:, i], color="C1", align='center')

        annotate_bars(rects, ax, df[:, i])

        ax.set_title(years[i], fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', labelrotation=45)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_axisbelow(True)
        ax.grid(axis="y", linewidth=1, linestyle=":")
        ax.margins(x=0.02)
        ax.set_ylim(top=df.max() * 1.1)

    return fig


def plot_stat(data_source, fig_location=None, show_figure=False):
    """
    Fnc plots number of accidents for each year and region
    :param data_source: all data
    :param fig_location: where to store figure
    :param show_figure: show figure
    """
    colls_year, rows_reg, df = process_data(data_source)
    fig = get_fig(colls_year, rows_reg, df)

    if show_figure:
        fig.show()

    if fig_location is not None:
        fig.savefig(os.path.join(fig_location, 'stat.svg'))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fig_location', dest='fg_loc', type=str)
    parser.add_argument('--show_figure ', action='store_true', dest='show')

    args = parser.parse_args()
    return args.show, args.fg_loc


if __name__ == '__main__':
    show, fig_loc = parse_args()
    d = DataDownloader()
    data_source = d.get_list()
    plot_stat(data_source, fig_loc, show)
