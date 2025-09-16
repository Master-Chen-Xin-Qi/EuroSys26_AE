# -*- encoding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

diff_colors = ['#7d4e4e', '#de1f00', '#15D776', '#7B5D03', '#b30086', '#fcbf50', '#011e90', '#837FA3']

FIG_SIZE = (10, 5)
LINE_WIDTH = 5
FONT_SIZE = 24
MARKER_SIZE = 12
LABEL_SIZE = 20
LEGEND_SIZE = 20
parameters = {'xtick.labelsize': LABEL_SIZE, 'ytick.labelsize': LABEL_SIZE}
plt.rcParams.update(parameters)

def plot_imbalance_bs(plot_data, save_path):
    plt.figure(figsize=FIG_SIZE)
    LINE_WIDTH = 3
    MARKER_SIZE = 10
    [[r_throughput, r_50, r_99, r_999], [w_throughput, w_50, w_99, w_999]] = plot_data
    data_num = 17
    r_throughput = r_throughput[:data_num]
    r_50 = r_50[:data_num]
    r_99 = r_99[:data_num]
    r_999 = r_999[:data_num]
    w_throughput = w_throughput[:data_num]
    w_50 = w_50[:data_num]
    w_99 = w_99[:data_num]
    w_999 = w_999[:data_num]
    x = np.arange(0, 255, 15)
    plt.yticks([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    xlim_l = -10
    xlim_r = 250
    plt.xticks([0, 30, 60, 90, 120, 150, 180, 210, 240])
    plt.xlim(xlim_l, xlim_r)
    plt.ylim(0, 10)
    plt.hlines(1, xlim_l, xlim_r, linestyles='dashed', linewidth=3, color='black')
    plt.plot(x, r_throughput, color=diff_colors[4], label='read traffic', marker='x', clip_on=False, markersize=MARKER_SIZE)
    plt.plot(x, w_throughput, color=diff_colors[5], label='write traffic', marker='*', clip_on=False, markersize=MARKER_SIZE)
    plt.plot(x, r_50, color=diff_colors[0], label='read P50 lat.', marker='v', clip_on=False, markersize=MARKER_SIZE)
    plt.plot(x, w_50, color=diff_colors[1], label='write P50 lat.', marker='^', clip_on=False, markersize=MARKER_SIZE)
    plt.plot(x, r_99, color=diff_colors[2], label='read P99 lat.', marker='o', clip_on=False, markersize=MARKER_SIZE)
    plt.plot(x, w_99, color=diff_colors[3], label='write P99 lat.', marker='s', clip_on=False, markersize=MARKER_SIZE)
    legend = plt.legend(loc='upper center', ncol=3, columnspacing=1.2, labelspacing=0.5, handlelength=1.5, bbox_to_anchor=(0.5, 1.35), frameon=False, fontsize=LEGEND_SIZE) 
    for handle in legend.legend_handles:
        handle.set_linewidth(LINE_WIDTH)
        handle.set_markersize(MARKER_SIZE)
    plt.xlabel('Time (s)', fontsize=FONT_SIZE)
    plt.ylabel('Ratio to Avg.', fontsize=FONT_SIZE)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f'Save to {os.path.abspath(save_path)}:1')


def main():
    data_path = '../data/fig9/skew_data.csv'
    save_path = '../pic/fig9_imbalance_bs.pdf'

    df = pd.read_csv(data_path).reset_index(drop=True)
    df = df.sort_values(by='time')
    r_p50 = df[df['label'] == 'read_p50_ratio']['value'].values
    r_p99 = df[df['label'] == 'read_p99_ratio']['value'].values
    r_p999 = df[df['label'] == 'read_p999_ratio']['value'].values
    w_p50 = df[df['label'] == 'write_p50_ratio']['value'].values
    w_p99 = df[df['label'] == 'write_p99_ratio']['value'].values
    w_p999 = df[df['label'] == 'write_p999_ratio']['value'].values
    r_traffic = df[df['label'] == 'read_tpt_ratio']['value'].values
    w_traffic = df[df['label'] == 'write_tpt_ratio']['value'].values
    plot_data = [[r_traffic, r_p50, r_p99, r_p999], [w_traffic, w_p50, w_p99, w_p999]]
    plot_imbalance_bs(plot_data, save_path)

if __name__ == "__main__":
    main()