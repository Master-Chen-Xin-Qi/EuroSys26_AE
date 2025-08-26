# -*- encoding: utf-8 -*-

import matplotlib.pyplot as plt
import os

FIG_SIZE = (8, 5.2)
LINE_WIDTH = 5
FONT_SIZE = 26
MARKER_SIZE = 12
LABEL_SIZE = 24
LEGEND_SIZE = 24
parameters = {'xtick.labelsize': LABEL_SIZE, 'ytick.labelsize': LABEL_SIZE}
plt.rcParams.update(parameters)
markers = ['D', '^', 'v', 'p', 'P', '*', 'X', 'd']
p99_res = [16.413, 14.983, 13.821, 13.337, 13.135, 13.348, 12.797, 12.5, 12.8, 12.8, 12.864, 13.189, 12.1, 11.8, 11.4, 10.358, 10.493, 13.253, 14.1, 14.5, 14.8, 14.842]
p999_res = [30.623, 28.946, 27.734, 27.371, 26.841, 27.114, 26.435, 26.1, 26.1, 26.5, 26.565, 26.922, 25.9, 25.5, 24.92, 23.672, 23.69, 27.309, 27.6, 27.9, 28.24, 29.235]
cov_res = [0.182, 0.1843, 0.1784, 0.1791, 0.1669, 0.1611, 0.1517, 0.153, 0.152, 0.152, 0.151, 0.1628, 0.16, 0.155, 0.157, 0.1552, 0.1563, 0.161, 0.158, 0.155, 0.1528, 0.1542]
sched_t = [94, 208, 313, 371, 396, 612, 727, 840, 943, 1062, 1219, 1330, 1406, 1536, 1686, 1880, 1914, 1960, 2096, 2233, 2451, 2603]

diff_colors = ['#7d4e4e', '#de1f00', '#15D776', '#7B5D03', '#b30086', '#fcbf50', '#011e90', '#837FA3']

def plot_paper_u_cov():
    plt.figure(figsize=FIG_SIZE)

    fig, ax1 = plt.subplots(figsize=FIG_SIZE)

    ax1.plot(sched_t, p99_res, linewidth=LINE_WIDTH, color='#011e90', marker=markers[1], markersize=MARKER_SIZE, label='P99')
    ax1.plot(sched_t, p999_res, linewidth=LINE_WIDTH, color=diff_colors[2], marker=markers[2], markersize=MARKER_SIZE, label='P999')
    x_delta = 2008
    y1_delta = 13.653
    y2_delta = 27.339
    ax1.text(x_delta, y1_delta, '★', fontsize=FONT_SIZE+6, color=diff_colors[1])
    ax1.text(x_delta, y2_delta, '★', fontsize=FONT_SIZE+6, color=diff_colors[1])
    ax1.set_xlim(0, 2800)
    ax1.set_ylabel('4KiB write latency (ms)', fontsize=FONT_SIZE)
    ax1.set_xticks([0, 700, 1400, 2100, 2800])
    ax1.set_xlabel('Number of schedules', fontsize=FONT_SIZE)

    ax2 = ax1.twinx()
    sort_sched_t = sorted(sched_t)
    ax2.plot(sort_sched_t, cov_res, linewidth=LINE_WIDTH, color=diff_colors[3], marker=markers[3], markersize=MARKER_SIZE, label='CoV')

    ax2.set_ylim(0.14, 0.19)
    ax2.set_ylabel('Cov', fontsize=FONT_SIZE)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    handles = handles1 + handles2
    labels = labels1 + labels2
    ax1.legend(handles, labels, fontsize=LEGEND_SIZE, loc='center', ncols=3, bbox_to_anchor=(0.5, 1.03), handlelength=1.5, columnspacing=1, frameon=False)
    fig.tight_layout()
    save_path = '../pic/fig7_U_cov_exp.pdf'
    plt.savefig(save_path)
    abs_path = os.path.abspath(save_path)
    print(f'Save to {abs_path}')


if __name__ == "__main__":
    plot_paper_u_cov()
