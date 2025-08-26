# -*- encoding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

FIG_SIZE = (6, 5)
LINE_WIDTH = 5
FONT_SIZE = 22
MARKER_SIZE = 12
LABEL_SIZE = 20
LEGEND_SIZE = 18
parameters = {'xtick.labelsize': LABEL_SIZE, 'ytick.labelsize': LABEL_SIZE}
plt.rcParams.update(parameters)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42


def plot_traffic_single(file):
    fig_folder = '../pic/'
    df = pd.read_csv(file)
    w_traffic = df[df['type'] == 'W']['traffic(MB)']
    r_traffic = df[df['type'] == 'R']['traffic(MB)']
    volume_id = file.split('/')[-1].split('.')[-2]
    x = [i for i in range(len(w_traffic))]
    fig = plt.figure(figsize=FIG_SIZE)
    plt.subplot(2, 1, 1)
    plt.plot(x, w_traffic, label='Write', linewidth=LINE_WIDTH, color='r')
    plt.ylabel('Traffic (MB/s)', fontsize=FONT_SIZE)
    plt.xlim(0, 1800)
    plt.xticks([0, 300, 600, 900, 1200, 1500, 1800], ['0', '5', '10', '15', '20', '25', '30'])
    plt.subplot(2, 1, 2)
    plt.plot(x, r_traffic, label='Read', linewidth=LINE_WIDTH, color='blue')
    plt.xticks([0, 300, 600, 900, 1200, 1500, 1800], ['0', '5', '10', '15', '20', '25', '30'])
    plt.xlim(0, 1800)
    plt.xlabel('Time (min)', fontsize=FONT_SIZE)
    plt.ylabel('Traffic (MB/s)', fontsize=FONT_SIZE)
    fig.legend(loc='upper center', bbox_to_anchor=(0.58, 1.08), ncol=2, fontsize=FONT_SIZE, frameon=False, handlelength=1)
    if not os.path.exists(fig_folder):
        os.makedirs(fig_folder)
    save_name = f'{fig_folder}/fig3_{volume_id}.pdf'
    plt.tight_layout()
    plt.savefig(save_name, bbox_inches='tight', dpi=300)
    print(f'Figure saved in {os.path.abspath(save_name)}')


def plot_resonant_traffic():
    file_path1 = "../data/fig3/vd1.csv"
    file_path2 = "../data/fig3/vd2.csv"
    plot_traffic_single(file_path1)
    plot_traffic_single(file_path2)


if __name__ == "__main__":
    plot_resonant_traffic()