# -*- encoding: utf-8 -*-

import numpy as np
import matplotlib.pyplot as plt

def cdf(data):
    x, counts = np.unique(data, return_counts=True)
    cusum = np.cumsum(counts)
    y = cusum / cusum[-1]
    return x, y

def set_fig_config(fig_size: tuple = (8, 5), tick_size: int = 15, label_size: int = 16, legend_size: int = 16, title_size: int = 20, line_width: int = 4):
    parameters = {'figure.figsize': fig_size, 'xtick.labelsize': tick_size, 'ytick.labelsize': tick_size, 'legend.fontsize': legend_size, 'axes.labelsize': label_size, 'axes.titlesize': title_size, 'lines.linewidth': line_width}
    plt.rcParams.update(parameters)
