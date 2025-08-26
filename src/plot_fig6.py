# -*- encoding: utf-8 -*-

import os
import json
import matplotlib.pyplot as plt
import matplotlib
from utils import cdf

line_width = 4
LEGEND_SIZE = 18
LABEL_SIZE = 22
colors = ['#011e90', '#de1f00', '#fcbf50', '#7d4e4e', '#b30086', 'black']
plt.rcParams['xtick.labelsize'] = LABEL_SIZE
plt.rcParams['ytick.labelsize'] = LABEL_SIZE
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42


def plot_life_cdf(life_path, save_path):
    clusters = os.listdir(life_path)
    plt.figure(figsize=(6, 4))
    total_1hour = 0
    total = 0
    for i, cluster in enumerate(sorted(clusters)):
        with open(life_path+cluster, 'r') as f:
            life_map = json.load(f)
        life_list = list(life_map.values())  
        life_list = [i / 3600 for i in life_list]
        total_1hour += len([i for i in life_list if i < 1])
        total += len(life_list)
        x, y = cdf(life_list)
        plt.plot(x, y, color=colors[i], linewidth=line_width, drawstyle='steps-post', label=f'cluster{i+1}')
        plt.ylim(0, 1)
        plt.xscale('log')
        plt.xticks([0.01, 0.1, 1, 10, 100, 1000])
    ax = plt.gca()
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ax.legend(unique.values(), unique.keys(), fontsize=LEGEND_SIZE, loc='lower right', ncols=1, bbox_to_anchor=(1.007, 0.0), handlelength=1.0, labelspacing=0.4, columnspacing=0.8)  
    plt.xlim(0.005, 1000)
    plt.ylabel('CDF', fontsize=LABEL_SIZE)
    plt.xlabel('Life Time (hours)', fontsize=LABEL_SIZE)
    plt.savefig(save_path+'fig6_life_cdf.pdf', dpi=300, bbox_inches='tight')
    print(f'Save {save_path}fig6_life_cdf.pdf successfully!')


if __name__ == '__main__':
    life_path = '../data/fig6/'
    cdf_save_path = '../pic/'
    plot_life_cdf(life_path, cdf_save_path)