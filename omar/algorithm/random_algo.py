# -*- encoding: utf-8 -*-

import numpy as np
import random
from utils.util import send_choose_rpc

def random_schedule(cpp_res, args, rpc_method, cf_logger, f_logger, proc_executor):

    RANDOM_MAX_THRESHOLD = 800 * 1024 * 1024
    all_bs_w = cpp_res.bs_flow
    bs_urgent_w = [v.mTrafficSum.write_urgent_sum for v in all_bs_w.values()]
    if max(bs_urgent_w) == 0:
        cf_logger.info('No traffic on any blockserver, no need to schedule!')
        return 0
    all_bs = list(all_bs_w.keys())
    max_bs_urgent_w = max(bs_urgent_w)
    mean_bs_urgent_w = np.mean(bs_urgent_w)
    skew = max_bs_urgent_w / mean_bs_urgent_w
    f_logger.debug(f'bs_urgent_w: {bs_urgent_w}')
    f_logger.debug(f'skew: {skew:.2f}, max_bs: {max_bs_urgent_w}, mean_bs: {mean_bs_urgent_w}')
    if max_bs_urgent_w < RANDOM_MAX_THRESHOLD or skew <= 1.2:
        cf_logger.debug(f'Max Skew: {skew}, max urgent w: {max_bs_urgent_w}, no need to schedule!')
        return 0
    seg_index = 0
    schedule_time = 0
    choose_res = []
    diff = max_bs_urgent_w - mean_bs_urgent_w
    source_bs = all_bs[np.argmax(bs_urgent_w)]
    while abs(diff) > 0.05 * mean_bs_urgent_w:
        remain_bs = [bs for bs in all_bs if bs != source_bs]
        source_segs = cpp_res.sort_bs_seg[source_bs]
        if seg_index >= len(source_segs):
            break
        for i in range(seg_index, len(source_segs)):
            target_bs = random.choice(remain_bs)
            if (diff - source_segs[i].traffic.write_urgent_sum) > -0.05 * mean_bs_urgent_w:
                choose_res.append([source_segs[i].segment_id.device_id, source_segs[i].segment_id.segment_index, target_bs])
                seg_index += 1
                diff -= source_segs[i].traffic.write_urgent_sum
                schedule_time += 1
            if abs(diff) < 0.05 * mean_bs_urgent_w:
                break
    send_choose_rpc(choose_res, proc_executor, rpc_method, f_logger)
    return schedule_time
