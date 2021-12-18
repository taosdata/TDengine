# -*- coding: utf-8 -*-
import yaml
import os
import time
from loguru import logger
current_time = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time()))
current_dir = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(current_dir, '../config/perf_test.yaml')
f = open(config_file)
config = yaml.load(f, Loader=yaml.FullLoader)
log_file = os.path.join(current_dir, f'../log/performance_{current_time}.log')
logger.add(log_file)
logger.info(f'init env success, log will be export to {log_file}')
