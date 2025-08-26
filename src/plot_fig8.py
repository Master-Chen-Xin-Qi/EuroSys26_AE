# -*- encoding: utf-8 -*-

from utils import set_fig_config
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib


matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

colors = ['#011e90', '#de1f00', '#fcbf50', '#7d4e4e', '#b30086', 'black', '#15D776', '#6B8432']

def plot_new_wr_segment():
    set_fig_config(fig_size=(6, 2.6))
    LABEL_SIZE = 20
    ratio = np.load('../data/fig8/ratio.npy')
    plt.figure()
    x, counts = np.unique(ratio, return_counts=True)
    cusum = np.cumsum(counts)
    y = cusum / cusum[-1]
    if x[0] < 0:
        plt.plot(x, y, color='black', drawstyle='steps-post', clip_on=False)
    else:
        plt.plot([0, x[0]], [0, y[0]], color='#011e90', drawstyle='steps-post')
        plt.plot(x, y, color='black', drawstyle='steps-post')
    plt.ylim(0, 1.0)
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0], fontsize=LABEL_SIZE)
    plt.xticks([-1, -1/3, 0, 1/3, 1], ['-1', '-1/3', '0', '1/3', '1'], fontsize=LABEL_SIZE)
    x1_idx = np.abs(x-(-1/3)).argmin()
    x2_idx = np.abs(x-(1/3)).argmin()
    print(y[x1_idx])
    print(y[x2_idx])
    plt.vlines(-1/3, 0, y[x1_idx], colors='black', linewidth=4)
    plt.vlines(1/3, 0, y[x2_idx], colors='black', linewidth=4)

    plt.fill_between(x[:x1_idx], 0, y[:x1_idx], color=colors[1])
    plt.fill_between(x[x1_idx:x2_idx], 0, y[x1_idx:x2_idx], color=colors[3])
    plt.fill_between(x[x2_idx:], 0, y[x2_idx:], color=colors[0])
    plt.text(0.6, 0.65, 'W dominant', fontsize=20, horizontalalignment='center', color=colors[0])
    plt.text(0, 0.23, 'W/R\nbalanced', fontsize=20, horizontalalignment='center', color=colors[3])
    plt.text(-0.645, 0.13, 'R dominant', fontsize=20, horizontalalignment='center', color=colors[1])
    plt.xlim(-1, 1)
    plt.xlabel(r"$\frac{W-R}{W+R}$", labelpad=-2, fontsize=LABEL_SIZE)
    plt.ylabel("CDF", fontsize=LABEL_SIZE)
    save_path = "../pic/fig8_rw_cdf.pdf"
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f'Save {os.path.abspath(save_path)}')

def main():
    plot_new_wr_segment()


if __name__ == "__main__":
    main()