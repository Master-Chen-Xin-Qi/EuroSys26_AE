# -*- encoding: utf-8 -*-

import logging
import subprocess
import os
import time
import json
from enum import Enum
from typing import Dict, List, Tuple
import networkx as nx
from tqdm import tqdm
import re
from concurrent.futures import as_completed
import numpy as np

# scheduling priority of segment
class Priority(Enum):
    SEGMENT_TRANSITION_PRIORITY_LOW = 1
    SEGMENT_TRANSITION_PRIORITY_INSTANT = 10
    SEGMENT_TRANSITION_PRIORITY_HIGH_INSTANT = 20

# scheduling reason of segment
class Reason(Enum):
    PLAN_REASON_INVALID = 0
    PLAN_CREATE_DEVICE = 1
    PLAN_REPORT_ERROR = 2
    PLAN_SEGMENT_SNIFF = 3
    PLAN_SERVER_CRASH = 4
    PLAN_ERROR_DETECT = 5
    PLAN_REBALANCE = 6
    PLAN_FLOW_REBALANCE = 7
    PLAN_OPS = 8
    PLAN_OTHER_REASON = 9
    PLAN_REPORT_ERROR_ABNORMAL_SERVER = 10
    PLAN_RELOAD_DEVICE = 11
    PLAN_THREADFLOW_REBALANCE = 12
    PLAN_ZONE_AFFINITY = 13
    PLAN_OBSERVATION_SCHEDULE = 14
    PLAN_PM_VOLUME_ANTI_AFFINITY = 15

class AdamOptimizer:
    def __init__(self, alpha=0.1, beta1=0.9, beta2=0.999, epsilon=1e-8):
        self.alpha = alpha  
        self.beta1 = beta1
        self.beta2 = beta2
        self.epsilon = epsilon
        self.m = 0
        self.v = 0
        self.t = 0
    
    def update(self, gradient):
        self.t += 1
        self.m = self.beta1 * self.m + (1 - self.beta1) * gradient
        self.v = self.beta2 * self.v + (1 - self.beta2) * (gradient**2)
        m_hat = self.m / (1 - self.beta1**self.t)
        v_hat = self.v / (1 - self.beta2**self.t)
        delta = -self.alpha * m_hat / (np.sqrt(v_hat) + self.epsilon)
        return delta

class FullPathFormatter(logging.Formatter):
    def format(self, record):
        record.pathname = os.path.abspath(record.pathname)
        return super().format(record)

def configure_logging(log_file, base_level="debug"):
    """
        base_level: 'debug' or 'info', only logs with level greater than or equal to base_level will be recorded
    """

    if base_level == "debug":
        level = logging.DEBUG
    elif base_level == "info":
        level = logging.INFO
    else:
        raise ValueError("log_level must be 'debug', 'info'")
    # create file logger
    f_logger = logging.getLogger('file_logger')
    f_logger.setLevel(level)

    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(level)
    file_handler.setFormatter(FullPathFormatter('[%(asctime)s] [%(pathname)s:%(lineno)d] [%(levelname)s] %(message)s'))
    f_logger.addHandler(file_handler)

    # create console logger
    cf_logger = logging.getLogger('console_file_logger')
    cf_logger.setLevel(level)

    # create console handler, set level to INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(FullPathFormatter('[%(asctime)s] [%(pathname)s:%(lineno)d] [%(levelname)s] %(message)s'))
    cf_logger.addHandler(console_handler)
    cf_logger.addHandler(file_handler)

    return f_logger, cf_logger

def run_cmd(strCmd, bPrintByDebug=True):
    if (bPrintByDebug):
        print(strCmd)
    nRet, strOutput = subprocess.getstatusoutput(strCmd)
    #python bug
    nRet = nRet >> 8
    if (0 != nRet and bPrintByDebug):
        print("run error, ret: %s" % (nRet))

    return nRet, strOutput

def run_cmd_with_exit(strCmd, bPrintByDebug=False):
    nRet, strOutput = run_cmd(strCmd, bPrintByDebug)
    if (0 != nRet):
        exit(nRet)
    return nRet, strOutput
    
def send_choose_rpc(choose_res, curl_proc_executor, rpc_method, f_logger):
    futures = []
    for device_id, segment_index, target_bs in choose_res:
        futures.append(curl_proc_executor.submit(schedule_segment, device_id, segment_index, target_bs, rpc_method, f_logger))
    for future in as_completed(futures):
        future.result()

def schedule_segment(choose_dev, choose_seg, target_bs, rpc_method, f_logger):

    current_time = int(time.time() * 1_000_000)
    plan = {
        "device_id": choose_dev,
        "segment_index": choose_seg,
        "blockserver": target_bs,
        "priority": Priority.SEGMENT_TRANSITION_PRIORITY_HIGH_INSTANT.name,
        "reason": Reason.PLAN_OTHER_REASON.name,
        "reload": True,
        "plan_generated_time": current_time
    }
    seg_json = {"plans": [plan]}
    rpc_call_cmd = f"""curl --header "Content-Type: application/json" --request POST --data '{json.dumps(seg_json)}' {rpc_method}"""
    # if f_logger.isEnabledFor(logging.DEBUG):
    #     f_logger.debug(f'RPC call: {rpc_call_cmd}')
    _ret, output = run_cmd_with_exit(rpc_call_cmd)
    if f_logger.isEnabledFor(logging.DEBUG):
        json_matches = re.findall(r'\{.*?\}', output, re.DOTALL)
        if not json_matches:
            f_logger.error(f'No json response in output: {output}')
        else:
            [f_logger.debug(json_str) for json_str in json_matches]

def generate_resonate_list(w_traffic, r_traffic, user_volume_map, check_len, corr_thresh, volume_limit=2):
    resonate = {"w_pos": [], "w_pos_avg": [], "w_pos_matrix": [], "r_pos": [], "r_pos_avg": [], "r_pos_matrix": [], "w_neg": [], "w_neg_avg": [], "w_neg_matrix": [], "r_neg": [], "r_neg_avg": [], "r_neg_matrix": []}
    for user, volume_list in user_volume_map.items():
        if len(volume_list) < volume_limit:
            continue
        volume_list = user_volume_map[user]
        w_vol_traffic, r_vol_traffic = {}, {}
        for volume in volume_list:
            w_vol_traffic[volume] = w_traffic[str(volume)]
            r_vol_traffic[volume] = r_traffic[str(volume)]
        w_res, r_res = extract_resonate(user, w_vol_traffic, r_vol_traffic, check_len, corr_thresh)
        update_resonate(resonate, w_res, r_res)
    place_segment(resonate)
    return resonate
    
def update_resonate(resonate, w_res, r_res):
    w_keys = ['w_pos', 'w_neg', 'w_pos_avg', 'w_neg_avg', 'w_pos_matrix', 'w_neg_matrix']
    r_keys = ['r_pos', 'r_neg', 'r_pos_avg', 'r_neg_avg', 'r_pos_matrix', 'r_neg_matrix']
    for key, res in zip(w_keys, w_res):
        resonate[key].extend(res)
    for key, res in zip(r_keys, r_res):
        resonate[key].extend(res)

def extract_resonate(user, w_vol_traffic: Dict, r_vol_traffic: Dict, check_len, corr_thresh) -> Tuple[List, List]:
    w_pos, w_neg, w_pos_avg, w_neg_avg, r_pos, r_neg, r_pos_avg, r_neg_avg = [], [], [], [], [], [], [], []
    w_pos, w_neg, w_pos_avg, w_neg_avg, w_pos_matrix, w_neg_matrix = judge_vol_resonate(w_vol_traffic, check_len, user, 'w', corr_thresh)
    r_pos, r_neg, r_pos_avg, r_neg_avg, r_pos_matrix, r_neg_matrix = judge_vol_resonate(r_vol_traffic, check_len, user, 'r', corr_thresh)
    return [w_pos, w_neg, w_pos_avg, w_neg_avg, w_pos_matrix, w_neg_matrix], [r_pos, r_neg, r_pos_avg, r_neg_avg, r_pos_matrix, r_neg_matrix]


def judge_vol_resonate(vol_traffic: Dict, check_len: int, user: str, type: str, corr_thresh, avg_thresh=0.02):

    assert type in ['w', 'r']
    vol_traffic = {vol: traffic[1:check_len+1] for vol, traffic in vol_traffic.items() if np.mean(traffic[1:check_len+1]) > avg_thresh}
    if len(vol_traffic) <= 1:
        return [], [], [], [], [], []
    res_matrix = pearson_correlation(list(vol_traffic.values()))
    pos_list, neg_list = graph_method(res_matrix, corr_thresh)
    if len(pos_list) > 0:
        vol_num = sum(len(pos_l) for pos_l in pos_list)
        print(f'Find {len(pos_list)} pos {type}_reson pairs for ali_uid {user}, total {vol_num} vols!')
    if len(neg_list) > 0:
        vol_num = sum(len(neg_l) for neg_l in neg_list)
        print(f'Find {len(neg_list)} neg {type}_reson pairs for ali_uid {user}, total {vol_num} vols!')
    pos_vol_list = trans_index_volume(pos_list, list(vol_traffic.keys()))
    neg_vol_list = trans_index_volume(neg_list, list(vol_traffic.keys()))
    avg_pos_traffic = compute_avg_traffic(vol_traffic, pos_vol_list, check_len)
    avg_neg_traffic = compute_avg_traffic(vol_traffic, neg_vol_list, check_len)
    avg_pos_traffic, pos_vol_list, pos_list = sort_by_avg(avg_pos_traffic, pos_vol_list, pos_list)
    avg_neg_traffic, neg_vol_list, neg_list = sort_by_avg(avg_neg_traffic, neg_vol_list, neg_list)
    pos_matrix = extract_matrix(res_matrix, pos_list)
    neg_matrix = extract_matrix(res_matrix, neg_list)
    return pos_vol_list, neg_vol_list, avg_pos_traffic, avg_neg_traffic, pos_matrix, neg_matrix

def trans_index_volume(resonate_list, volumes):
    volume_list = [[volumes[i] for i in group] for group in resonate_list]
    return volume_list

def compute_avg_traffic(vol_traffic, reson_list, check_len):
    all_avg = []
    for group in reson_list:
        avg = [np.mean(vol_traffic[vol][1:check_len+1]) for vol in group]
        all_avg.append(avg)
    return all_avg

def sort_by_avg(avg_traffic, vol_list, index_list):

    if len(avg_traffic) == 0:
        return [], [], []
    sorted_avg_traffic, sorted_vol_list, sorted_index_list = [], [], []
    for i in range(len(avg_traffic)):
        zipped_lists = zip(avg_traffic[i], vol_list[i], index_list[i])
        sorted_lists = sorted(zipped_lists, key=lambda x: x[0], reverse=True)
        sorted_avg, sorted_vol, sorted_index = zip(*sorted_lists)
        sorted_avg_traffic.append(list(sorted_avg))
        sorted_vol_list.append(list(sorted_vol))
        sorted_index_list.append(list(sorted_index))
    return sorted_avg_traffic, sorted_vol_list, sorted_index_list

def extract_matrix(matrix, index_list):
    all_mat = []
    for index in index_list:
        trunc_mat = trunc_matrix(matrix, index)
        all_mat.append(trunc_mat)
    return all_mat

def trunc_matrix(matrix, index):
    n = len(index)
    trunc_mat = np.zeros((n, n))
    for i in range(n):
        for j in range(i, n):
            trunc_mat[i][j] = matrix[index[i], index[j]]
            trunc_mat[j][i] = matrix[index[j], index[i]]
    return trunc_mat


def pearson_correlation(time_series):
    res_matrix = np.corrcoef(time_series, dtype='float32')
    return res_matrix


def graph_method(matrix, pos_threshold=0.5) -> Tuple[List[List[int]], List[List[int]]]:

    neg_threhold = -pos_threshold
    pos_graph = nx.Graph()
    neg_graph = nx.Graph()
    n = matrix.shape[0]
    indices = list(range(n))
    pos_graph.add_nodes_from(indices)
    neg_graph.add_nodes_from(indices)

    for i in tqdm(range(n), desc='Building graph'):
        row = matrix[i, i+1:]
        mask1 = row > pos_threshold
        mask2 = row < neg_threhold
        idx1 = np.where(mask1)[0] + i + 1
        idx2 = np.where(mask2)[0] + i + 1
        pos_graph.add_edges_from((i, j) for j in idx1)
        neg_graph.add_edges_from((i, j) for j in idx2)
    pos_reson = max_clique(pos_graph)
    neg_reson = max_clique(neg_graph)
    return pos_reson, neg_reson
                
def max_clique(graph) -> List[List[int]]:

    connected_components = list(nx.connected_components(graph))
    reson_group = []
    for component in connected_components:
        if len(component) <= 1:
            continue
        subgraph = graph.subgraph(component)
        max_clique = nx.max_weight_clique(subgraph, weight=None)  # type: ignore
        reson_group.append(max_clique[0])
    return reson_group

def avg_similar(matrix, cliques):

    sum_similar = []
    for i in range(len(cliques)):
        for j in range(i+1, len(cliques)):
            sum_similar.append(matrix[cliques[i], cliques[j]])
    return sum(sum_similar) / len(sum_similar)

