#!/usr/bin/env python3
# filepath: hot_update/config.py
#
# Global configuration for rolling upgrade test

import os

# ==================== Cluster ====================
FQDN          = ""                              # empty → auto socket.gethostname()
BASE_PATH     = os.path.expanduser("~/td_rolling_upgrade")
DNODE_COUNT   = 3
MNODE_COUNT   = 3
SNODE_COUNT   = 0
BASE_PORT     = 6030                            # dnode1 port; dnode2 = 6130, dnode3 = 6230

# ==================== Database / Schema ====================
DB_NAME       = "test_db"
VGROUPS       = 2
REPLICA       = 3
STABLE_NAME   = "meters"
SUBTABLE_COUNT         = 100
INIT_ROWS_PER_SUBTABLE = 10_000                # 确保 WAL / data / stt 均有文件
WRITE_BATCH_SIZE       = 5000                  # 每次 INSERT 批量行数
WRITE_THREADS          = 20                    # 并发写入线程数（初始化阶段）

# ==================== Topic ====================
TOPIC_NAME    = "test_topic"

# ==================== Test user ====================
# test_user is created in Phase 2 on the base version with limited privileges:
#   - SELECT on TEST_USER_READ_STABLE  (read-only stable)
#   - INSERT on TEST_USER_WRITE_STABLE (write-only stable)
#   - no access to TEST_USER_HIDDEN_STABLE
# After upgrade we verify the exact same privilege set is preserved.
TEST_USER_NAME          = "test_user"
TEST_USER_PASS          = "Test@1234"
TEST_USER_READ_STABLE   = "meters_ro"      # test_user has SELECT only
TEST_USER_WRITE_STABLE  = "meters_wo"      # test_user has INSERT only
TEST_USER_HIDDEN_STABLE = "meters_hidden"  # test_user has NO access

# ==================== Tag indexes ====================
# An explicit tag index created on the BASE version and verified after upgrade.
# NOTE: the first tag column (groupid) automatically gets a system-generated index,
# so we only create an explicit index on the second tag column (location).
TAG_INDEX_LOCATION = "idx_compat_location"  # on meters.location (BINARY)

# ==================== RSMA ====================
# RSMA created on the BASE version; verified unchanged (by DDL) after upgrade.
RSMA_NAME = "rsma_compat_meters"  # RSMA on meters stable (min voltage, avg current, last phase)

# ==================== TSMA ====================
# TSMA created on the BASE version (>= 3.3.6.0); verified unchanged after upgrade.
TSMA_NAME = "tsma_compat_meters"  # TSMA on meters stable (sum/avg/min/max/count/last)

# ==================== Stream (全新流) ====================
# New-style stream created on the BASE version (>= 3.3.7.0); verified running after upgrade.
STREAM_NAME      = "stream_compat_meters"  # PERIOD stream aggregating from meters stable
STREAM_OUT_TABLE = "stream_compat_out"     # output table for stream results

# ==================== Runtime thresholds ====================
MAX_WRITE_LATENCY_S  = 2.0     # 写入延时上限（秒）
MAX_QUERY_LATENCY_S  = 2.0     # 查询延时上限（秒）
MAX_SUBSCRIBE_GAP_S  = 2.0     # 订阅消息间隔上限（秒）
VERIFY_DURATION_S    = 30      # 升级后观察窗口（秒）

# ==================== Retry ====================
RETRY_INTERVAL_S        = 1    # 每次重试间隔（秒）
MAX_CONSECUTIVE_RETRIES = 30   # 连续失败超过此次数 → 计为 1 次真正失败
RETRY_MAX_DURATION_S    = 150  # 单次 SQL 重试超过此秒数 → 也计为 1 次真正失败
SUBSCRIBE_NO_DATA_TIMEOUT_S = 180  # 订阅：超过此秒数未收到任何数据判定不通过
SUBSCRIBE_DRAIN_WAIT_S      = 60   # Phase 5 结束前等待订阅追上写入的最大时间（秒）

# ==================== Upgrade ====================
NODE_INSTALL_SLEEP_S  = 20    # STOP 后模拟安装新软件包的等待时间（秒）
NODE_READY_TIMEOUT_S  = 180   # 等待单个节点 ready 的最大时间
GRACEFUL_STOP_TIMEOUT = 30    # taosd SIGTERM 后等待退出的最大时间
