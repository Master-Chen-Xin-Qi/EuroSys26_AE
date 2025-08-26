# -*- encoding: utf-8 -*-

import matplotlib.pyplot as plt
import os
import numpy as np
import seaborn as sns
from utils import set_fig_config

colors = ['#011e90', '#de1f00', '#fcbf50', '#7d4e4e', '#b30086', 'black', '#15D776', '#6B8432']
all_clusters = ['cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5']

def plot_errors(base_folder, save_folder):
    all_files = os.listdir(base_folder)
    all_files = sorted(all_files, key=lambda x: os.path.basename(x))
    set_fig_config(fig_size=(6, 3))
    LABEL_SIZE = 20
    plt.figure()
    clusters = []
    for i, file in enumerate(all_files):
        cluster = file.split('_')[0]
        clusters.append(cluster)
        cluster_index = all_clusters.index(cluster)
        data = np.load(base_folder + file)
        data = data[~np.isnan(data)]
        if cluster_index == 0:
            cluster_index = -1
        sns.boxplot(x=i, y=data, color=colors[cluster_index], linewidth=2, flierprops={'markerfacecolor': 'black', 'markeredgecolor': 'black'}, boxprops=dict(edgecolor='black'), medianprops={'color': 'black'}, whiskerprops={'color': 'black'}, capprops={'color': 'black'})
    plt.ylim(0, 100)
    new_clusters = ["C1", "C2", "C3", "C4", "C5"]
    plt.xticks([i for i in range(len(clusters))], new_clusters, fontsize=LABEL_SIZE)
    plt.yticks([0, 20, 40, 60, 80, 100], fontsize=LABEL_SIZE)
    plt.ylabel('RMSE', fontsize=LABEL_SIZE, labelpad=-8)
    save_path = save_folder + 'fig10_rmse.pdf'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f'Save {os.path.abspath(save_path)}')


def main():
    base_folder = '../data/fig10/'
    save_folder = '../pic/'
    plot_errors(base_folder, save_folder)


if __name__ == '__main__':
    main()