# -*- encoding: utf-8 -*-

bs_file = '/path/to/bs_list'  # This is the file that contains all ips of block servers
MB = 1024 * 1024
LT_MAX_THRESHOLD = 500 * MB 
MAX_THRESHOLD = 800 * MB
MIN_THRESHOLD = 300 * MB
LESS_BALANCE_RATIO = 0.2
MAX_ALL_THRESHOLD = 1200 * MB
BS_INTERVAL = 2
BS_QUEUE_LEN = 10
SEG_INTERVAL = 50

Q_TIME = 60 * 5
FIRST_ADJUST = 0.5
MAX_BASE_FREQ = 10
W_RATE = 0.8
R_RATE = 0.2
MAX_W_SKEW = 0.35 
MAX_R_SKEW = 0.4
MAX_BORROW_TOKENS = 8 
PCC_THRESHOLD = 0.7
CHECK_LEN = 12 * 60
MB = 1024 * 1024
MIN_THRESHOLD = 300 * MB
MAX_THRESHOLD = 800 * MB
LESS_BALANCE_RATIO = 0.2
MAX_W_SKEW = 0.35
MAX_R_SKEW = 0.4
MAX_BORROW_TOKENS = 8


OPTIMIZER_CONFIG = {
    'learning_rate': 0.01,
    'beta1': 0.9,
    'beta2': 0.999,
    'eps': 1e-8,
    'weight_decay': 0.0,
    
    'min_token_speed': 10.0,
    'max_token_speed': 300.0,
    
    'history_window': 10,
    
    'performance_weight': 2.0,
    'stability_weight': 0.5,
    'speed_weight': 1.0,
    'trend_weight': 0.3,
    'lr_scheduler': {
        'type': 'step',  # 'step', 'exponential', 'cosine'
        'step_size': 50,
        'gamma': 0.9,
        'min_lr': 0.001
    },
    
    'early_stopping': {
        'patience': 20,
        'min_delta': 0.001
    }
}


OPTIMIZER_SAVE_PATH = './checkpoints/token_optimizer.pth' 

RESON_TIME = 60 * 60