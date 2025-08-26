# -*- encoding: utf-8 -*-

import os
import json
import matplotlib.pyplot as plt
from utils import cdf


def plot_e_cdf(life_path, save_path):
    line_width = 4
    LABEL_SIZE = 18
    colors = ['#011e90', '#de1f00', '#fcbf50', '#7d4e4e', '#b30086', 'black']
    plt.rcParams['xtick.labelsize'] = LABEL_SIZE
    plt.rcParams['ytick.labelsize'] = LABEL_SIZE
    plt.figure(figsize=(6.5, 2.5))
    with open(life_path+'life.json', 'r') as f:
        life_map = json.load(f)
    life_list = list(life_map.values())  
    life_list = [i / 3600 for i in life_list]
    life_list = [i for i in life_list if i < 1000]
    x, y = cdf(life_list)
    plt.plot(x, y, color=colors[0], linewidth=line_width, drawstyle='steps-post', clip_on=False)
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1])
    plt.ylim(0, 1)
    plt.plot([x[785], x[785]], [0, 0.2], 'g--', linewidth=line_width)
    plt.plot([0, x[785]], [0.2, 0.2], 'g--', linewidth=line_width)
    plt.text(0.02, 0.25, 'a', fontsize=LABEL_SIZE, color='g')
    plt.plot([x[3666], x[3666]], [0, 0.8], 'r--', linewidth=line_width)
    plt.plot([0, x[3666]], [0.8, 0.8], 'r--', linewidth=line_width)
    plt.text(0.02, 0.85, 'b', fontsize=LABEL_SIZE, color='r')
    plt.ylim(-0.005, 1.005)
    plt.xlabel('X = Lifespan (hour)', fontsize=LABEL_SIZE)
    plt.ylabel('CDF', fontsize=LABEL_SIZE)
    plt.xscale('log')
    plt.xticks([0.01, 0.1, 1, 10, 100, 1000], ['$10^{-2}$', '$10^{-1}$', '$10^0$', '$10^1$', '$10^2$', '$10^3$'])
    fig_path = os.path.join(save_path, 'fig12_lifespan_cdf.pdf')
    plt.savefig(fig_path, dpi=300, bbox_inches='tight')
    print(f'Save {os.path.abspath(fig_path)} successfully!')


if __name__ == '__main__':
    life_path = '../data/fig12/'
    cdf_save_path = '../pic/'
    plot_e_cdf(life_path, cdf_save_path)
