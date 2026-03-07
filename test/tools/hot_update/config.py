#!/usr/bin/env python3
# filepath: hot_update/config.py
#
# Global configuration for rolling upgrade test

import os

# ==================== Cluster ====================
FQDN          = ""                              # empty → auto socket.gethostname()
BASE_PATH     = os.path.expanduser("~/td_rolling_upgrade")
DNODE_COUNT   = 3
MNODE_COUNT   = 2
SNODE_COUNT   = 0
BASE_PORT     = 6030                            # dnode1 port; dnode2 = 6130, dnode3 = 6230

# ==================== Database / Schema ====================
DB_NAME       = "test_db"
REPLICA       = 3
STABLE_NAME   = "meters"
SUBTABLE_COUNT         = 1000
INIT_ROWS_PER_SUBTABLE = 100_000               # 确保 WAL / data / stt 均有文件
WRITE_BATCH_SIZE       = 500                   # 每次 INSERT 批量行数
WRITE_THREADS          = 20                    # 并发写入线程数（初始化阶段）

# ==================== Topic ====================
TOPIC_NAME    = "test_topic"

# ==================== Runtime thresholds ====================
MAX_WRITE_LATENCY_S  = 2.0     # 写入延时上限（秒）
MAX_QUERY_LATENCY_S  = 2.0     # 查询延时上限（秒）
MAX_SUBSCRIBE_GAP_S  = 2.0     # 订阅消息间隔上限（秒）
VERIFY_DURATION_S    = 60      # 升级后观察窗口（秒）

# ==================== Retry ====================
RETRY_INTERVAL_S = 1
RETRY_TIMEOUT_S  = 300         # 5 分钟

# ==================== Upgrade ====================
NODE_READY_TIMEOUT_S  = 120    # 等待单个节点 ready 的最大时间
GRACEFUL_STOP_TIMEOUT = 30     # taosd SIGTERM 后等待退出的最大时间
