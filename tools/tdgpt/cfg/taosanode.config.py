# gunicorn_config.py, not valid for windows
import multiprocessing
import platform
import os as _os

on_windows = platform.system().lower() == "windows"
on_github_actions = bool(_os.environ.get("GITHUB_ACTIONS"))

# Auto-detect install directory from this config file's location:
# cfg/taosanode.config.py → parent dir is <install_dir>
_cfg_dir = _os.path.dirname(_os.path.abspath(__file__))
_install_dir = _os.path.dirname(_cfg_dir)

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

# Log Setting, only valid on Linux
accesslog = '/var/log/taos/taosanode/access.log'
errorlog = '/var/log/taos/taosanode/error.log'

# only valid on the Windows system.
waitresslog = _os.path.join(_install_dir, 'log', 'taosanode-service.log').replace('\\', '/') if (on_windows or on_github_actions) else '/var/log/taos/taosanode/waitress.log'

# log level: debug, info, warning, error, critical
loglevel = 'debug'

# Set process name
proc_name = 'tdgpt_taosanode_app'

# set the pid file
pidfile = _os.path.join(_install_dir, 'taosanode.pid').replace('\\', '/') if (on_windows or on_github_actions) else '/usr/local/taos/taosanode/taosanode.pid'

# virtual environment directory
virtualenv = _os.path.join(_install_dir, 'venvs', 'venv').replace('\\', '/') if (on_windows or on_github_actions) else '/usr/local/taos/taosanode/venv'

# set the taosanoded basic python library directory
pythonpath = (_os.path.join(_install_dir, 'lib', 'taosanalytics') + '/').replace('\\', '/') if (on_windows or on_github_actions) else '/usr/local/taos/taosanode/lib/taosanalytics/'

# wsgi app name
wsgi_app = 'app:app'

# Preload application before forking worker processes. This can improve startup time and save memory
preload_app = True

# [taosanode]
# The following configuration parameters are valid on both Windows and Linux system.
# default app log file
app_log = _os.path.join(_install_dir, 'log', 'taosanode.app.log').replace('\\', '/') if (on_windows or on_github_actions) else '/var/log/taos/taosanode/taosanode.app.log'

# model storage directory
model_dir = (_os.path.join(_install_dir, 'model') + '/').replace('\\', '/') if (on_windows or on_github_actions) else '/usr/local/taos/taosanode/model/'

# default log level
log_level = 'DEBUG'

# draw the query results
draw_result = False

# Model configuration - defines all available models
# Required models: tdtsfm, timemoe (must exist, error if missing)
# Optional models: moirai, chronos, timesfm, moment (skip if not found)
# Port and endpoint are the single source of truth for service URLs;
# conf.py auto-derives http://127.0.0.1:<port><endpoint> for each model.
models = {
    "tdtsfm": {
        "script": "tdtsfm-server.py",
        "default_model": None,
        "port": 6061,
        "endpoint": "/tdtsfm",
        "algo_name": "tdtsfm_1",
        "required": True,  # Must exist
    },
    "timemoe": {
        "script": "timemoe-server.py",
        "default_model": "Maple728/TimeMoE-200M",
        "port": 6062,
        "endpoint": "/ds_predict",
        "algo_name": "timemoe_fc",
        "required": True,  # Must exist
    },
    "moirai": {
        "script": "moirai-server.py",
        "default_model": "Salesforce/moirai-moe-1.0-R-small",
        "port": 6064,
        "endpoint": "/ds_predict",
        "required": False,  # Optional
    },
    "chronos": {
        "script": "chronos-server.py",
        "default_model": "amazon/chronos-bolt-base",
        "port": 6063,
        "endpoint": "/ds_predict",
        "required": False,  # Optional
    },
    "timesfm": {
        "script": "timesfm-server.py",
        "default_model": "google/timesfm-2.0-500m-pytorch",
        "port": 6065,
        "endpoint": "/ds_predict",
        "required": False,  # Optional
    },
    "moment": {
        "script": "moment-server.py",
        "default_model": "AutonLab/MOMENT-1-base",
        "port": 6066,
        "endpoint": "/imputation",
        "required": False,  # Optional
    },
}

timesfm_venv = _os.path.join(_install_dir, 'venvs', 'timesfm_venv').replace('\\', '/') if (on_windows or on_github_actions) else '/var/lib/taos/taosanode/timesfm_venv'
moirai_venv = _os.path.join(_install_dir, 'venvs', 'moirai_venv').replace('\\', '/') if (on_windows or on_github_actions) else '/var/lib/taos/taosanode/moirai_venv'
chronos_venv = _os.path.join(_install_dir, 'venvs', 'chronos_venv').replace('\\', '/') if (on_windows or on_github_actions) else '/var/lib/taos/taosanode/chronos_venv'
momentfm_venv = _os.path.join(_install_dir, 'venvs', 'momentfm_venv').replace('\\', '/') if (on_windows or on_github_actions) else '/var/lib/taos/taosanode/momentfm_venv'

# Windows下waitress服务器配置（仅在Windows系统上使用）
waitress_config = {
    'threads': 4,                    # 工作线程数
    'channel_timeout': 1200,         # 通道超时（秒）
    'connection_limit': 1000,        # 最大连接数
    'cleanup_interval': 30,          # 清理间隔（秒）
    'log_socket_errors': True        # 记录socket错误
}
