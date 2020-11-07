from download import DataDownloader
import argparse
import pandas as pd


def plot_stat(data_source, fig_location=None, show_figure=False):
    df = pd.DataFrame(data_source[1], columns=data_source[0])
    print(df)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fig_location', dest='fg_loc', type=str)
    parser.add_argument('--show_figure ', action='store_true', dest='show')

    args = parser.parse_args()
    return args.show, args.fg_loc


if __name__ == '__main__':
    show, fig_loc = parse_args()
    d = DataDownloader()
    data_source = d.get_list(['PHA', 'JHM'])

    plot_stat(data_source, fig_loc, show)
