# -*- encoding: utf-8 -*-

from cpp_code.read_and_merge import merge_bs_segment, bs_stat, merge_bs_rw_segment
from apscheduler.schedulers.background import BackgroundScheduler
from utils.util import generate_resonate_list, run_cmd_with_exit, configure_logging
from utils.config import bs_file, BS_QUEUE_LEN, Q_TIME, RESON_TIME, W_RATE, R_RATE, FIRST_ADJUST, PCC_THRESHOLD, CHECK_LEN, MAX_BASE_FREQ
from utils.token_optimizer import TokenSpeedOptimizer
from algorithm.random_algo import random_schedule
from algorithm.omar_algo import omar_schedule
import logging
import argparse
import datetime
import time
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from collections import namedtuple, deque
import pwd
import grp

admin_uid = pwd.getpwnam('admin').pw_uid
admin_gid = grp.getgrnam('admin').gr_gid
bs_all = []
bs_num = 0
with open(bs_file, 'r') as f:
    for i in f.readlines():
        bs_all.append(i.strip())
        bs_num += 1
bs_all.sort()

f_logger, cf_logger = logging.getLogger('file_logger'), logging.getLogger('console_file_logger')
schedule_times = 0
proc_executor = ProcessPoolExecutor()
base_scheduler = BackgroundScheduler()

bs_queue = {}

seg_record = {}     
seg_lat = {} 
queue_len = 0 
update_freq_flag = False
SegLat = namedtuple('SegLat', ['r_lat', 'w_lat'])
avg_r_lat, avg_w_lat, all_sched_freq = [], [], []
base_sched_freq = Q_TIME * 2 
token_speed = Q_TIME / base_sched_freq 
sched_in_window = 0
remain_token = 0
token_optimizer = TokenSpeedOptimizer(initial_token_speed=token_speed)

# key: vd_id, value: traffic
w_traffic = {} 
r_traffic = {}
# key: user_id, value: [volume_id, ...]
user_volume_map = {}

def segment_lat_collect():
    global seg_lat, update_freq_flag, avg_w_lat, avg_r_lat
    res = merge_bs_segment()
    for bs, segs in res.sort_bs_seg.items():
        for seg in segs:
            seg_id = f'{seg.segment_id.device_id}-{seg.segment_id.segment_index}'
            if seg_id not in seg_lat:
                seg_lat[seg_id] = deque(maxlen=queue_len)
            seg_lat[seg_id].append(SegLat(r_lat=seg.latency.read_urgent_sum, w_lat=seg.latency.write_urgent_sum))
            if len(seg_lat[seg_id]) == queue_len:
                update_freq_flag = True
    if update_freq_flag:
        global avg_r_lat, avg_w_lat
        total_r_lat, total_w_lat, count = 0, 0, 0
        for latencies in seg_lat.values():
            for lat in latencies:
                total_r_lat += lat.r_lat  
                total_w_lat += lat.w_lat
                count += 1
        r_lat = total_r_lat / count if count else 0 
        w_lat = total_w_lat / count if count else 0
        avg_r_lat.append(r_lat)
        avg_w_lat.append(w_lat)
        seg_lat = {}
        update_freq_flag = False
        if len(all_sched_freq) == 0:
            all_sched_freq.append(schedule_times)
        else:
            sched_t = schedule_times - sum(all_sched_freq)
            assert sched_t >= 0, 'schedule times should be greater than 0'
            all_sched_freq.append(sched_t)
        cf_logger.debug(f'Add frequency to all_sched_freq! all_sched_freq: {all_sched_freq}')

def adjust_sched_freq():
    global base_sched_freq, token_speed, sched_in_window, remain_token, token_optimizer
    if len(avg_r_lat) < 2:
        cf_logger.info(f'Not enough latency data, simple adjust. Last freq: {base_sched_freq}, new freq: {base_sched_freq * FIRST_ADJUST}, token_speed: {Q_TIME / (base_sched_freq * FIRST_ADJUST)}')
        base_sched_freq = max(MAX_BASE_FREQ, base_sched_freq * FIRST_ADJUST)
    else:
        current_r_lat = avg_r_lat[-1]
        current_w_lat = avg_w_lat[-1]
        current_freq = all_sched_freq[-1] if all_sched_freq else base_sched_freq
        
        new_token_speed = token_optimizer.update(
            current_r_lat=current_r_lat,
            current_w_lat=current_w_lat,
            current_freq=current_freq,
            w_rate=W_RATE,
            r_rate=R_RATE
        )
        
        base_sched_freq = max(MAX_BASE_FREQ, int(Q_TIME / new_token_speed))
        token_speed = new_token_speed
        
        stats = token_optimizer.get_optimization_stats()
        cf_logger.info(f'Adam optimization - Step: {stats["optimization_step"]}, '
                      f'Best metric: {stats["best_metric"]:.2f}, '
                      f'Best token_speed: {stats["best_token_speed"]:.2f}, '
                      f'Current token_speed: {stats["current_token_speed"]:.2f}, '
                      f'New base_sched_freq: {base_sched_freq}')
        
        cf_logger.debug(f'Current r_lat: {int(current_r_lat)}, w_lat: {int(current_w_lat)}, '
                       f'freq: {current_freq}, all_sched_freq: {all_sched_freq}')
    
    sched_in_window = 0
    remain_token = 0
    update_job_interval('gen_token', token_speed)

def rpc_method():
    # This is your rpc method to send the scheduling decision to the blockmaster. It can be a http interface like 'http://0.0.0.0:1000/rpc/BM/ScheduleSegment'
    pass


def period_base(args, merge_func, sort_flag, schedule_func, rpc_method):
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S") + f".{int(now.microsecond / 10000):02d}"
    cf_logger.info(f'Scheduling at {formatted_time}...')

    if not merge_func:
        res = None
    elif isinstance(sort_flag, list) or isinstance(sort_flag, tuple):
        res = merge_func(*sort_flag)
    else:
        res = merge_func(sort_flag)
    global schedule_times, sched_in_window, remain_token
    if schedule_func is None:
        schedule_time = 0
    elif 'omar' in schedule_func.__name__:
        schedule_time = schedule_func(res, args, rpc_method, cf_logger, f_logger, proc_executor, remain_token)
    else:
        schedule_time = schedule_func(res, args, rpc_method, cf_logger, f_logger, proc_executor)
    schedule_times += schedule_time
    sched_in_window += schedule_time
    remain_token -= schedule_time
    cf_logger.info(f'Schedule {schedule_time} times, total schedule times: {schedule_times}!')

def scheduler(args, delta=10):
    """Start the scheduler"""
    
    start_time = datetime.datetime.now()
    finish_time = start_time + datetime.timedelta(seconds=args.t_len+delta)
    cf_logger.info(f'Start scheduling at {start_time:%Y-%m-%d %H:%M:%S}, will end at: {finish_time:%Y-%m-%d %H:%M:%S}')

    schedule_functions = {
        'random': random_schedule,
        'omar': omar_schedule,
    }

    if args.algo not in schedule_functions:
        raise ValueError(f'No such schedule function: {args.algo}')

    # sort_flag: 0-write traffic; 1-write traffic and standard deviation (for var_s_rw, it is read and write traffic and standard deviation); 2-write traffic, iops, latency weighted sum; 3-read and write together, traffic sum, standard deviation sum; 4-write latency; 5-write latency divided by iops, 6-write traffic and standard deviation (short-term and long-term); 7-write traffic, standard deviation, iops, latency calculate score; 8-write traffic, iops, latency calculate score; 9-read sort, choose read traffic large and write traffic small
    
    if 'omar' in args.algo:
        merge_func = merge_bs_rw_segment
    elif 'random' in args.algo:
        merge_func = merge_bs_segment
    else:
        raise ValueError(f'No such merge function: {args.algo}')
    
    sort_flag = 0
    if 'omar' in args.algo:
        sort_flag = [9, 7]
    assert (sort_flag !=0 if 'var' in args.algo else True), 'Standard deviation is required for var algorithms'
    cf_logger.info(f'Sort flag: {sort_flag}')
    schedule_func = schedule_functions[args.algo]

    def job():
        period_base(args, merge_func, sort_flag, schedule_func)
    
    if args.debug:
        job()
    else:
        global base_scheduler
        base_scheduler.add_job(job, 'interval', seconds=args.interval)
        if 'omar' in args.algo:
            base_scheduler.add_job(generate_resonate_list, 'interval', seconds=RESON_TIME, args=[w_traffic, r_traffic, user_volume_map, CHECK_LEN, PCC_THRESHOLD])
            base_scheduler.add_job(segment_lat_collect, 'interval', seconds=args.interval*2)
            base_scheduler.add_job(adjust_sched_freq, 'interval', seconds=Q_TIME)
            base_scheduler.add_job(gen_sched_token, 'interval', seconds=token_speed, id='gen_token')
        base_scheduler.start()
        time.sleep(args.t_len+delta)
        base_scheduler.shutdown()

    cf_logger.info(f'Schedule finished! Start at {start_time:%Y-%m-%d %H:%M:%S}, end at: {finish_time:%Y-%m-%d %H:%M:%S}')
    cf_logger.info(f'Schedule times in each window: {all_sched_freq}')
    print(f"Total segment schedule times: {schedule_times}")

def gen_sched_token():
    global remain_token
    remain_token += 1
    f_logger.debug(f'Generating a token, remain_token: {remain_token}')

def update_job_interval(job_id, new_interval):
    global base_scheduler
    base_scheduler.reschedule_job(job_id, trigger='interval', seconds=new_interval)

def main():
    parser = argparse.ArgumentParser(description='Start the scheduler')
    parser.add_argument('--t_len', type=int, default=30*60, help='The test time (s)')
    parser.add_argument('--interval', type=int, default=3, help='The interval of scheduling')
    parser.add_argument('--map', type=str, default=None, help='The map file of segments')
    parser.add_argument('--algo', type=str, required=True, help='The scheduling algorithm')
    parser.add_argument('--log_level', '-ll', type=str, default='debug', help='The log level')
    parser.add_argument('--debug', '-d', action='store_true', help='Whether to debug')
    parser.add_argument('--start_time', '-st', type=str, default=None, help='The start time of scheduling, generated LOG name')
    parser.add_argument('--bs_qlen', '-bsl', type=int, default=BS_QUEUE_LEN, help='The length of bs_queue')
    args = parser.parse_args()

    global queue_len
    queue_len = Q_TIME // (args.interval * 2)

    if args.start_time:
        current_time = args.start_time.replace(' ', '_')
    else:
        current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    global f_logger 
    global cf_logger
    logger_fld = './log/mysched_log/' + args.algo
    if not os.path.exists(logger_fld):
        os.makedirs(logger_fld)
    os.chown(logger_fld, admin_uid, admin_gid)
    if not args.debug:
        logger_file = os.path.join(logger_fld, f'{current_time}.LOG')
    else:
        logger_file = os.path.join(logger_fld, 'FOR_DEBUG.LOG')
    print(f'Logging to {os.path.abspath(logger_file)}:1')
    f_logger, cf_logger = configure_logging(logger_file, base_level=args.log_level)
    f_logger.info(f'Running script with arguments: {args}')

    if args.map:
        cf_logger.info('Reloading all segments according to the map file......')
        reload_cmd = f"cd iorecord_replay && python segment_load.py --load {args.map}"
        ret_, output = run_cmd_with_exit(reload_cmd)
        f_logger.info(output)

    scheduler(args)
    os.chown(logger_file, admin_uid, admin_gid)

if __name__ == '__main__':
    if os.geteuid() != 0:
        print("Re-running the script with sudo...")
        os.execvp('sudo', ['sudo', 'python3'] + sys.argv)
    else:
        main()
