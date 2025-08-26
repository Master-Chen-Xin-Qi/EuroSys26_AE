# -*- encoding: utf-8 -*-

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from typing import List, Tuple
import logging
import os
from utils.config import OPTIMIZER_CONFIG

class TokenSpeedOptimizer:
    def __init__(self, 
                 initial_token_speed: float = 60.0,
                 learning_rate: float = None,
                 beta1: float = None,
                 beta2: float = None,
                 eps: float = None,
                 weight_decay: float = None,
                 min_token_speed: float = None,
                 max_token_speed: float = None,
                 history_window: int = None):
        config = OPTIMIZER_CONFIG
        self.min_token_speed = min_token_speed or config['min_token_speed']
        self.max_token_speed = max_token_speed or config['max_token_speed']
        self.history_window = history_window or config['history_window']
        
        self.performance_weight = config['performance_weight']
        self.stability_weight = config['stability_weight']
        self.speed_weight = config['speed_weight']
        self.trend_weight = config['trend_weight']
        
        self.token_speed_param = nn.Parameter(torch.tensor(initial_token_speed, dtype=torch.float32))
        
        self.optimizer = optim.Adam(
            [self.token_speed_param], 
            lr=learning_rate or config['learning_rate'],
            betas=(beta1 or config['beta1'], beta2 or config['beta2']),
            eps=eps or config['eps'],
            weight_decay=weight_decay or config['weight_decay']
        )
        
        self.latency_history: List[Tuple[float, float]] = []  # (read_lat, write_lat)
        self.frequency_history: List[int] = []
        self.metric_history: List[float] = []
        
        self.logger = logging.getLogger('token_optimizer')
        
        self.optimization_step = 0
        self.best_metric = float('inf')
        self.best_token_speed = initial_token_speed
        
    def compute_loss(self, 
                    current_metric: float,
                    previous_metric: float,
                    current_freq: int,
                    previous_freq: int,
                    w_rate: float = 0.8,
                    r_rate: float = 0.2) -> torch.Tensor:
        performance_loss = torch.relu(current_metric - previous_metric)
        
        freq_change = torch.abs(torch.tensor(current_freq - previous_freq, dtype=torch.float32))
        stability_loss = torch.exp(-freq_change / 10.0)
        
        token_speed = self.token_speed_param
        speed_loss = torch.relu(token_speed - self.max_token_speed) + torch.relu(self.min_token_speed - token_speed)
        
        trend_loss = self._compute_trend_loss(current_metric)
        
        total_loss = (performance_loss * self.performance_weight + 
                     stability_loss * self.stability_weight + 
                     speed_loss * self.speed_weight + 
                     trend_loss * self.trend_weight)
        
        return total_loss
    
    def _compute_trend_loss(self, current_metric: float) -> torch.Tensor:
        if len(self.metric_history) < 3:
            return torch.tensor(0.0)
        
        recent_metrics = self.metric_history[-3:]
        if len(recent_metrics) >= 2:
            trend = recent_metrics[-1] - recent_metrics[-2]
            
            if trend > 0:
                return torch.tensor(trend * 0.1)
        
        return torch.tensor(0.0)
    
    def update(self, 
               current_r_lat: float,
               current_w_lat: float,
               current_freq: int,
               w_rate: float = 0.8,
               r_rate: float = 0.2) -> float:
        current_metric = w_rate * current_w_lat + r_rate * current_r_lat
        
        self.latency_history.append((current_r_lat, current_w_lat))
        self.frequency_history.append(current_freq)
        self.metric_history.append(current_metric)
        
        if len(self.latency_history) > self.history_window:
            self.latency_history.pop(0)
            self.frequency_history.pop(0)
            self.metric_history.pop(0)
        
        if len(self.metric_history) >= 2:
            previous_metric = self.metric_history[-2]
            previous_freq = self.frequency_history[-2]
            
            loss = self.compute_loss(
                current_metric, previous_metric,
                current_freq, previous_freq,
                w_rate, r_rate
            )
            
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            
            if current_metric < self.best_metric:
                self.best_metric = current_metric
                self.best_token_speed = self.token_speed_param.item()
            
            self.optimization_step += 1
            
            self.logger.debug(f'Optimization step {self.optimization_step}: '
                            f'loss={loss.item():.4f}, '
                            f'token_speed={self.token_speed_param.item():.2f}, '
                            f'current_metric={current_metric:.2f}')
        
        new_token_speed = self.token_speed_param.item()
        
        new_token_speed = np.clip(new_token_speed, self.min_token_speed, self.max_token_speed)
        
        return new_token_speed
    
    def get_best_token_speed(self) -> float:
        return self.best_token_speed
    
    def reset_optimizer(self):
        self.optimizer = optim.Adam([self.token_speed_param], lr=self.optimizer.param_groups[0]['lr'])
        self.optimization_step = 0
        self.logger.info("Token optimizer reset")
    
    def set_learning_rate(self, lr: float):
        for param_group in self.optimizer.param_groups:
            param_group['lr'] = lr
        self.logger.info(f"Learning rate set to {lr}")
    
    def get_optimization_stats(self) -> dict:
        return {
            'optimization_step': self.optimization_step,
            'best_metric': self.best_metric,
            'best_token_speed': self.best_token_speed,
            'current_token_speed': self.token_speed_param.item(),
            'history_size': len(self.metric_history)
        }
    
    def save_checkpoint(self, filepath: str = None):
        if filepath is None:
            from utils.config import OPTIMIZER_SAVE_PATH
            filepath = OPTIMIZER_SAVE_PATH
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        checkpoint = {
            'token_speed_param': self.token_speed_param.data,
            'optimizer_state': self.optimizer.state_dict(),
            'optimization_step': self.optimization_step,
            'best_metric': self.best_metric,
            'best_token_speed': self.best_token_speed,
            'metric_history': self.metric_history,
            'latency_history': self.latency_history,
            'frequency_history': self.frequency_history
        }
        
        torch.save(checkpoint, filepath)
        self.logger.info(f"Optimizer checkpoint saved to {filepath}")
    
    def load_checkpoint(self, filepath: str = None):
        if filepath is None:
            from utils.config import OPTIMIZER_SAVE_PATH
            filepath = OPTIMIZER_SAVE_PATH
        
        if not os.path.exists(filepath):
            self.logger.warning(f"Checkpoint file {filepath} not found, starting fresh")
            return
        
        checkpoint = torch.load(filepath)
        
        self.token_speed_param.data = checkpoint['token_speed_param']
        self.optimizer.load_state_dict(checkpoint['optimizer_state'])
        self.optimization_step = checkpoint['optimization_step']
        self.best_metric = checkpoint['best_metric']
        self.best_token_speed = checkpoint['best_token_speed']
        self.metric_history = checkpoint['metric_history']
        self.latency_history = checkpoint['latency_history']
        self.frequency_history = checkpoint['frequency_history']
        
        self.logger.info(f"Optimizer checkpoint loaded from {filepath}")
    
    def adaptive_learning_rate(self, performance_improvement: float):
        if performance_improvement > 0.1:
            self.set_learning_rate(self.optimizer.param_groups[0]['lr'] * 1.1)
        elif performance_improvement < -0.1:
            self.set_learning_rate(self.optimizer.param_groups[0]['lr'] * 0.9) 
