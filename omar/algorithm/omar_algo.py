# -*- encoding: utf-8 -*-

import numpy as np
import logging
from utils.util import send_choose_rpc
from utils.config import MB, MIN_THRESHOLD, MAX_THRESHOLD, LESS_BALANCE_RATIO, MAX_W_SKEW, MAX_R_SKEW, MAX_BORROW_TOKENS


def get_nested_attr(obj, attr):
    attributes = attr.split('.')
    for attribute in attributes:
        obj = getattr(obj, attribute)
    return obj

def check_r_w_traffic(cpp_res, cf_logger):

    all_bs_traffic = cpp_res.bs_flow
    all_bs = list(all_bs_traffic.keys())
    bs_urgent_w, bs_urgent_r = [], []
    for v in all_bs_traffic.values():
        bs_urgent_w.append(v.mTrafficSum.write_urgent_sum)
        bs_urgent_r.append(v.mTrafficSum.read_urgent_sum)
    bs_urgent_w, bs_urgent_r = np.array(bs_urgent_w), np.array(bs_urgent_r)
    tmp_max_urgent_w = max(bs_urgent_w)
    tmp_min_urgent_w = min(bs_urgent_w)
    mean_bs_urgent_w = np.mean(bs_urgent_w)
    tmp_max_urgent_r = max(bs_urgent_r)
    tmp_min_urgent_r = min(bs_urgent_r)
    mean_bs_urgent_r = np.mean(bs_urgent_r)

    w_less_flag, r_less_flag = 0, 0
    if tmp_max_urgent_w < MAX_THRESHOLD:
        w_less_flag = 1
        cf_logger.info(f'Max urgent w: {tmp_max_urgent_w / MB:.2f}, less schedule!')
    if tmp_max_urgent_r < MAX_THRESHOLD:
        r_less_flag = 1
        cf_logger.info(f'Max urgent r: {tmp_max_urgent_r / MB:.2f}, less schedule!')
    return all_bs, [bs_urgent_w, tmp_max_urgent_w, tmp_min_urgent_w, mean_bs_urgent_w, w_less_flag], [bs_urgent_r, tmp_max_urgent_r, tmp_min_urgent_r, mean_bs_urgent_r, r_less_flag]

def omar_schedule(cpp_res, args, rpc_method, cf_logger, f_logger, proc_executor, remain_tokens):
    
    all_bs, [bs_urgent_w, tmp_max_urgent_w, tmp_min_urgent_w, mean_bs_urgent_w, w_less_flag], [bs_urgent_r, tmp_max_urgent_r, tmp_min_urgent_r, mean_bs_urgent_r, r_less_flag] = check_r_w_traffic(cpp_res, cf_logger)
    w_max_skew = tmp_max_urgent_w / mean_bs_urgent_w
    w_min_skew = tmp_min_urgent_w / mean_bs_urgent_w
    r_max_skew = tmp_max_urgent_r / mean_bs_urgent_r
    r_min_skew = tmp_min_urgent_r / mean_bs_urgent_r
    f_logger.debug(f'bs: {all_bs}, bs_urgent_w: {(bs_urgent_w / MB).astype(int)}, bs_urgent_r: {(bs_urgent_r / MB).astype(int)}')
    cf_logger.info(f'w_max_skew: {w_max_skew:.2f}, w_min_skew: {w_min_skew:.2f}, r_max_skew: {r_max_skew:.2f}, r_min_skew: {r_min_skew:.2f}')
    if tmp_max_urgent_w < MIN_THRESHOLD and tmp_max_urgent_r < MIN_THRESHOLD:
        cf_logger.debug(f'No need to schedule! max_urgent_w and r < {MIN_THRESHOLD}')

    if w_less_flag:
        w_max_ratio = 1+LESS_BALANCE_RATIO
        w_min_ratio = 1-LESS_BALANCE_RATIO
    else:
        w_max_ratio = 1+args.ratio
        w_min_ratio = 1-args.ratio
    if r_less_flag:
        r_max_ratio = 1+LESS_BALANCE_RATIO
        r_min_ratio = 1-LESS_BALANCE_RATIO
    else:
        r_max_ratio = 1+args.ratio
        r_min_ratio = 1-args.ratio
    if w_max_skew <= w_max_ratio and w_min_skew >= w_min_ratio and r_max_skew <= r_max_ratio and r_min_skew >= r_min_ratio:
        cf_logger.debug(f'No need to schedule! w_max_skew < {w_max_ratio}, w_min_skew > {w_min_ratio}, r_max_skew < {r_max_ratio}, r_min_skew > {r_min_ratio}')
        return 0
    if remain_tokens <=0:
        if (w_max_skew <= 1+MAX_W_SKEW and w_min_skew >= 1-MAX_W_SKEW and r_max_skew <= 1+MAX_R_SKEW and r_min_skew >= 1-MAX_R_SKEW):
            cf_logger.debug(f'No token to schedule! But w_max_skew < {1+MAX_W_SKEW}, w_min_skew > {1-MAX_W_SKEW}, r_max_skew < {1+MAX_R_SKEW}, r_min_skew > {1-MAX_R_SKEW}')
            return 0
        else:
            cf_logger.debug(f'Remain tokens: {remain_tokens}, but still need to schedule since skewness')

    schedule_time = 0
    choose_res = []
    choose_read = []
    r_index_map = {bs: 0 for bs in all_bs}
    w_index_map = {bs: 0 for bs in all_bs}
    r_cannot_sched_bs = set()
    w_cannot_sched_bs = set()

    def perform_transfer(io_type):
        nonlocal schedule_time, remain_tokens
        if io_type == 'w':
            bs_urgent_traffic = bs_urgent_w
            mean_bs_urgent_traffic = mean_bs_urgent_w
            cannot_sched_bs = w_cannot_sched_bs
            index_map = w_index_map
            dw = w_max_ratio - 1
        else:
            bs_urgent_traffic = bs_urgent_r
            mean_bs_urgent_traffic = mean_bs_urgent_r
            cannot_sched_bs = r_cannot_sched_bs
            index_map = r_index_map
            dw = r_max_ratio - 1
        assert dw > 0
        delta = dw * mean_bs_urgent_traffic
        max_bs_index = np.argmax(bs_urgent_traffic)
        min_bs_index = np.argmin(bs_urgent_traffic)
        source_bs = all_bs[max_bs_index]

        index = 1
        while source_bs in cannot_sched_bs:
            source_bs = all_bs[np.argsort(bs_urgent_traffic)[-index]]
            index += 1
        target_bs = all_bs[min_bs_index]

        if io_type == 'w':
            source_items = cpp_res.sort_write_seg[source_bs]
        else:
            source_items = cpp_res.sort_read_seg[source_bs]
        dev_attr = 'segment_id.device_id'
        seg_attr = 'segment_id.segment_index'
        if index_map[source_bs] >= len(source_items):
            cannot_sched_bs.add(source_bs)
            f_logger.debug(f'{io_type}: Cannot schedule source bs: {source_bs} anymore')
            return -1
        for i in range(index_map[source_bs], len(source_items)):
            if index_map[source_bs] >= len(source_items):
                cannot_sched_bs.add(source_bs)
                f_logger.debug(f'{io_type}: Cannot schedule source bs: {source_bs} anymore')
                return -1
            if io_type == 'w':
                urgent_traffic = source_items[i].traffic.write_urgent_sum
                urgent_std = source_items[i].traffic_std.write_urgent_std
            else:
                urgent_traffic = source_items[i].traffic.read_urgent_sum    
                urgent_std = source_items[i].traffic_std.read_urgent_std  
            if urgent_traffic <= MB:
                index_map[source_bs] = len(source_items)
                cannot_sched_bs.add(source_bs)
                f_logger.debug(f'{io_type}: Cannot schedule source bs: {source_bs} anymore')
                return -1          
            dev_id = get_nested_attr(source_items[i], dev_attr)
            seg_id = get_nested_attr(source_items[i], seg_attr)
            if io_type == 'w' and [dev_id, seg_id] in choose_read:
                index_map[source_bs] = i+1
                f_logger.debug(f'{io_type}: Skip device: {dev_id}, segment_id: {seg_id} since already scheduled in read')
                continue
            diff = bs_urgent_traffic[max_bs_index] - mean_bs_urgent_traffic - urgent_traffic
            if diff > -delta:
                choose_res.append([dev_id, seg_id, target_bs])
                if io_type == 'r':
                    choose_read.append([dev_id, seg_id])
                bs_urgent_traffic[max_bs_index] -= urgent_traffic
                bs_urgent_traffic[min_bs_index] += urgent_traffic
                if io_type == 'r':
                    bs_urgent_w[max_bs_index] -= source_items[i].traffic.write_urgent_sum
                    bs_urgent_w[min_bs_index] += source_items[i].traffic.write_urgent_sum
                else:
                    bs_urgent_r[max_bs_index] -= source_items[i].traffic.read_urgent_sum
                    bs_urgent_r[min_bs_index] += source_items[i].traffic.read_urgent_sum
                f_logger.debug(f'{io_type}: Choose device: {dev_id}, segment_id: {seg_id}, source bs: {source_bs}, target bs: {target_bs}, urgent traffic: {urgent_traffic}, urgent std: {urgent_std}, i: {i}, index: {index_map[source_bs]}, after schedule urgent traffic: {(bs_urgent_traffic / MB).astype(int)}')
                schedule_time += 1
                remain_tokens -= 1
                index_map[source_bs] = i+1
                break
        return schedule_time
    
    if tmp_max_urgent_r > MIN_THRESHOLD:
        while r_max_skew > r_max_ratio or r_min_skew < r_min_ratio:
            if remain_tokens <= -MAX_BORROW_TOKENS:
                f_logger.debug('r: No token to schedule, break!')
                break
            if len(r_cannot_sched_bs) >= len(all_bs)-1:
                break
            if perform_transfer('r') == -1:
                if r_max_skew > r_max_ratio and r_min_skew >= r_min_ratio:
                    break
            if remain_tokens <= 0:
                r_max_ratio = 1+MAX_R_SKEW
                r_min_ratio = 1-MAX_R_SKEW
                w_max_ratio = 1+MAX_W_SKEW
                w_min_ratio = 1-MAX_W_SKEW
            r_max_skew = max(bs_urgent_r) / mean_bs_urgent_r
            r_min_skew = min(bs_urgent_r) / mean_bs_urgent_r
    else:
        f_logger.debug(f'No need to schedule read since max_r < {MIN_THRESHOLD}')

    if tmp_max_urgent_w > MIN_THRESHOLD:
        while w_max_skew > w_max_ratio or w_min_skew < w_min_ratio:
            if remain_tokens <= -MAX_BORROW_TOKENS:
                f_logger.debug('w: No token to schedule, break!')
                break
            if len(w_cannot_sched_bs) >= len(all_bs)-1:
                break
            if perform_transfer('w') == -1:
                if w_max_skew > w_max_ratio and w_min_skew >= w_min_ratio:
                    break
            if remain_tokens <= 0:
                w_max_ratio = 1+MAX_W_SKEW
                w_min_ratio = 1-MAX_W_SKEW
            w_max_skew = max(bs_urgent_w) / mean_bs_urgent_w
            w_min_skew = min(bs_urgent_w) / mean_bs_urgent_w
    else:
        f_logger.debug(f'No need to schedule read since max_w < {MIN_THRESHOLD}')
    
    if f_logger.isEnabledFor(logging.DEBUG):
        f_logger.debug(f'After schedule, w_max_skew: {w_max_skew:.2f}, w_min_skew: {w_min_skew:.2f}, r_max_skew: {r_max_skew:.2f}, r_min_skew: {r_min_skew:.2f}')
    send_choose_rpc(choose_res, proc_executor, rpc_method, f_logger)
    return schedule_time
    
