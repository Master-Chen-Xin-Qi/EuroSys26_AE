# -*- encoding: utf-8 -*-

import os
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
from utils import cdf

line_width = 4
LEGEND_SIZE = 15
LABEL_SIZE = 18
colors = ['#011e90', '#de1f00', '#fcbf50', '#7d4e4e', '#b30086', 'black'] 
plt.rcParams['xtick.labelsize'] = LABEL_SIZE
plt.rcParams['ytick.labelsize'] = LABEL_SIZE
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def plot_fig4(clusters, map_base_path, save_path):
    plt.figure(figsize=(6, 2))
    for i, cluster in enumerate(clusters):
        map_path = map_base_path + cluster + '.npy'
        user_device = np.load(map_path, allow_pickle=True)
        x, y = cdf(user_device)
        plt.plot([0, x[0]], [0, y[0]], color=colors[i], linewidth=line_width, drawstyle='steps-post', clip_on=True, label=f'cluster{i+1}')
        plt.plot(x, y, color=colors[i], linewidth=line_width, drawstyle='steps-post', clip_on=True,label=f'cluster{i+1}')
        print(f'Finish {cluster}')
    plt.ylim(0.8, 1)
    plt.yticks([0.8, 0.9, 1.0])
    plt.xlim(0, 300)
    plt.xlabel('Number of VDs for a user', fontsize=LABEL_SIZE)
    plt.ylabel('CDF', fontsize=LABEL_SIZE)
    ax = plt.gca()
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ax.legend(unique.values(), unique.keys(), loc='lower right', fontsize=LEGEND_SIZE, handlelength=1.0, bbox_to_anchor=(1.007, -0.04), labelspacing=0.4, ncols=2, columnspacing=0.8)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f'Finish {os.path.abspath(save_path)}')


if __name__ == '__main__':
    map_base_path = '../data/fig4/'
    clusters = ['cluster1_vd', 'cluster2_vd', 'cluster3_vd', 'cluster4_vd', 'cluster5_vd']
    save_path = '../pic/fig4_user_vd.pdf'
    plot_fig4(clusters, map_base_path, save_path)