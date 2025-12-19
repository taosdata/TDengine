# gunicorn_config.py
import multiprocessing

# list address and port
bind = '0.0.0.0:6035'

# Number of worker processes (typically recommended 2 * CPU cores + 1)
workers = 2

# Specify worker type, using default sync worker here
# For IO-intensive applications, consider eventlet or gevent
worker_class = 'sync'

# Number of threads per process (recommended for model deployment)
threads = max(multiprocessing.cpu_count() / 4 + 1, 2)

# Maximum number of requests, worker will restart after reaching limit, helps release memory
max_requests = 1000

# Random jitter added to max_requests to avoid all workers restarting simultaneously
max_requests_jitter = 50

# Timeout settings
timeout = 1200

# keep-alive time
keepalive = 1200

# Log Setting
accesslog = '/var/log/taos/taosanode/access.log'
errorlog = '/var/log/taos/taosanode/error.log'

# log level: debug, info, warning, error, critical
loglevel = 'debug'

# Set process name
proc_name = 'tdgpt_taosanode_app'

# Preload application before forking worker processes. This can improve startup time and save memory
preload_app = True

# [taosanode]
# default app log file
app_log = '/var/log/taos/taosanode/taosanode.app.log'

# model storage directory
model_dir = '/usr/local/taos/taosanode/model/'

# default log level
log_level = 'DEBUG'

# draw the query results
draw_result = False

# moe default service host
tdtsfm_1 = 'http://127.0.0.1:6036/tdtsfm'
timemoe_fc = 'http://127.0.0.1:6037/ds_predict'
