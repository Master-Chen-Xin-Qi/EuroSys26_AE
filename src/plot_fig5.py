# -*- encoding: utf-8 -*-

import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import json
import os
import re

FIG_SIZE = (12, 4)
LINE_WIDTH = 4
MARKER_SIZE = 12
LABEL_SIZE = 18
LEGEND_SIZE = 18
parameters = {'xtick.labelsize': LABEL_SIZE, 'ytick.labelsize': LABEL_SIZE}
plt.rcParams.update(parameters)
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

fig = plt.figure(figsize=FIG_SIZE)
gs = fig.add_gridspec(
    nrows=2,  
    ncols=4,  
    width_ratios=[1.9, 0.35, 1, 1],  
    height_ratios=[1, 1],           
    hspace=0.25,                   
    wspace=0.05                    
)

machines = ["machine1", "machine2", "machine3", "machine4"]
res_folder = '../data/fig5'
latency = []
for machine in machines:
    with open(os.path.join(res_folder, f'{machine}.txt'), 'r') as f:
        lat = f.readlines()
    lat = np.array([float(i.strip()) for i in lat])
    latency.append(lat)
th_file = os.path.join(res_folder, 'BS_ThroughputAll.json')
with open(th_file, 'r') as f:
    th_data = json.load(f)
    data = th_data[0]['data'][0]
    numbers = re.findall(r'\d+\.\d+|\d+', data)
    th = [float(num) if '.' in num else int(num) for num in numbers]
    th = np.array(th) / 1024 / 1024

ax_main = fig.add_subplot(gs[:, 0])  
ax1 = fig.add_subplot(gs[0, 2])      
ax2 = fig.add_subplot(gs[0, 3])      
ax3 = fig.add_subplot(gs[1, 2])      
ax4 = fig.add_subplot(gs[1, 3])      

x = [i for i in range(0, 15*80, 15)]
reson_start_ts = 10 * 60 + 45

ax_main.plot(x, th, color='#1f77b4', linewidth=LINE_WIDTH)
ax_main.axvline(x=reson_start_ts, color='red', linestyle='--', linewidth=LINE_WIDTH)
ax_main.text(x=0.275, y=0.2, s="Resonance occurs", color="red", fontsize=15.7, horizontalalignment='center',
verticalalignment='center', transform=ax_main.transAxes)
ax_main.arrow(90, 140, 480, 0, head_width=40, head_length=50, fc='red', ec='red', lw=3)
ax_main.set_ylabel("Write traffic (MB/s)", fontsize=LABEL_SIZE)
ax_main.set_xlabel("Time (s)", fontsize=LABEL_SIZE)
ax_main.set_xticks([i for i in range(0, 1400, 200)])
ax_main.set_xlim(0, 1200)
ax_main.set_ylim(0, 1000)
ax_main.grid(True, linestyle=':', alpha=0.6)

sub_axes = [ax1, ax2, ax3, ax4]
colors = ['#ff7f0e', '#2ca02c', '#8B4500', '#9467bd']
for i, ax in enumerate(sub_axes):
    color = colors[i]
    ax.plot(x, latency[i] / 1000, color=color)
    ax.axvline(x=reson_start_ts, color='red', linestyle='--', linewidth=LINE_WIDTH)
    ax.set_title(f"#Blockserver{i+1}", fontsize=LABEL_SIZE)
    ax.grid(True, linestyle=':', alpha=0.4)
    ax.set_xlabel("Time (s)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Lat. (ms)", fontsize=LABEL_SIZE)
    ax.set_xticks([0, 600])
    ax.set_xlim(0, 1200)
    ax.set_ylim(0, 120)
    if i not in [2, 3]:
        ax.set_xticklabels([])
        ax.set_xlabel('')
    if i not in [0, 2]:
        ax.set_yticklabels([])
        ax.set_ylabel('')

save_path = '../pic/fig5_reson_case.pdf'
plt.tight_layout()
plt.savefig(save_path, bbox_inches='tight', dpi=300)
print(f"Save to {os.path.abspath(save_path)}:1")