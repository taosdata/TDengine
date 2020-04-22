/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "tlog.h"
#include "monitor.h"
#include "dnode.h"
#include "tsclient.h"
#include "taosdef.h"
#include "tsystem.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "monitorSystem.h"

#define monitorError(...)                    \
  if (monitorDebugFlag & DEBUG_ERROR) {      \
    taosPrintLog("ERROR MON ", 255, __VA_ARGS__); \
  }
#define monitorWarn(...)                                  \
  if (monitorDebugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorTrace(...)                           \
  if (monitorDebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorPrint(...) \
  { taosPrintLog("MON ", 255, __VA_ARGS__); }

#define monitorLError(...) monitorError(__VA_ARGS__)
#define monitorLWarn(...) monitorWarn(__VA_ARGS__)
#define monitorLPrint(...) monitorPrint(__VA_ARGS__)

#define SQL_LENGTH     1024
#define LOG_LEN_STR    80
#define IP_LEN_STR     15
#define CHECK_INTERVAL 1000

typedef enum {
  MONITOR_CMD_CREATE_DB,
  MONITOR_CMD_CREATE_TB_LOG,
  MONITOR_CMD_CREATE_MT_DN,
  MONITOR_CMD_CREATE_MT_ACCT,
  MONITOR_CMD_CREATE_TB_DN,
  MONITOR_CMD_CREATE_TB_ACCT_ROOT,
  MONITOR_CMD_CREATE_TB_SLOWQUERY,
  MONITOR_CMD_MAX
} MonitorCommand;

typedef enum {
  MONITOR_STATE_UN_INIT,
  MONITOR_STATE_INITIALIZING,
  MONITOR_STATE_INITIALIZED,
  MONITOR_STATE_STOPPED
} MonitorState;

typedef struct {
  void * conn;
  void * timer;
  char   privateIpStr[TSDB_IPv4ADDR_LEN];
  int8_t cmdIndex;
  int8_t state;
  char   sql[SQL_LENGTH];
  void * initTimer;
  void * diskTimer;
} MonitorConn;

MonitorConn *monitor = NULL;

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos);
void monitorInitConn(void *para, void *unused);
void monitorInitConnCb(void *param, TAOS_RES *result, int code);
void monitorInitDatabase();
void monitorInitDatabaseCb(void *param, TAOS_RES *result, int code);
void monitorStartTimer();
void monitorSaveSystemInfo();
void monitorSaveLog(int level, const char *const format, ...);
void monitorSaveAcctLog(char *acctId, int64_t currentPointsPerSecond, int64_t maxPointsPerSecond,
                        int64_t totalTimeSeries, int64_t maxTimeSeries, int64_t totalStorage, int64_t maxStorage,
                        int64_t totalQueryTime, int64_t maxQueryTime, int64_t totalInbound, int64_t maxInbound,
                        int64_t totalOutbound, int64_t maxOutbound, int64_t totalDbs, int64_t maxDbs,
                        int64_t totalUsers, int64_t maxUsers, int64_t totalStreams, int64_t maxStreams,
                        int64_t totalConns, int64_t maxConns, int8_t accessState);
void (*mnodeCountRequestFp)(SDnodeStatisInfo *info) = NULL;
void monitorExecuteSQL(char *sql);

void monitorCheckDiskUsage(void *para, void *unused) {
  taosGetDisk();
  taosTmrReset(monitorCheckDiskUsage, CHECK_INTERVAL, NULL, tscTmr, &monitor->diskTimer);
}

int monitorInitSystem() {
  monitor = (MonitorConn *)malloc(sizeof(MonitorConn));
  memset(monitor, 0, sizeof(MonitorConn));
  taosTmrReset(monitorCheckDiskUsage, CHECK_INTERVAL, NULL, tscTmr, &monitor->diskTimer);
  return 0;
}

int monitorStartSystem() {
  if (monitor == NULL) {
    monitorInitSystem();
  }
  taosTmrReset(monitorInitConn, 10, NULL, tscTmr, &monitor->initTimer);
  return 0;
}

void monitorStartSystemRetry() {
  if (monitor->initTimer != NULL) {
    taosTmrReset(monitorInitConn, 3000, NULL, tscTmr, &monitor->initTimer);
  }
}

void monitorInitConn(void *para, void *unused) {
  monitorPrint("starting to initialize monitor service ..");
  monitor->state = MONITOR_STATE_INITIALIZING;

  if (monitor->privateIpStr[0] == 0) {
    strcpy(monitor->privateIpStr, tsPrivateIp);
    for (int i = 0; i < TSDB_IPv4ADDR_LEN; ++i) {
      if (monitor->privateIpStr[i] == '.') {
        monitor->privateIpStr[i] = '_';
      }
    }
  }

  if (monitor->conn == NULL) {
    taos_connect_a(NULL, "monitor", tsInternalPass, "", 0, monitorInitConnCb, monitor, &(monitor->conn));
  } else {
    monitorInitDatabase();
  }
}

void monitorInitConnCb(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    monitorError("monitor:%p, connect to database failed, code:%d", monitor->conn, code);
    taos_close(monitor->conn);
    monitor->conn = NULL;
    monitor->state = MONITOR_STATE_UN_INIT;
    monitorStartSystemRetry();
    return;
  }

  monitorTrace("monitor:%p, connect to database success, code:%d", monitor->conn, code);
  monitorInitDatabase();
}

void dnodeBuildMonitorSql(char *sql, int cmd) {
  memset(sql, 0, SQL_LENGTH);

  if (cmd == MONITOR_CMD_CREATE_DB) {
    snprintf(sql, SQL_LENGTH,
             "create database if not exists %s replica 1 days 10 keep 30 rows 1024 cache 2048 "
             "ablocks 2 tblocks 32 tables 32 precision 'us'",
             tsMonitorDbName);
  } else if (cmd == MONITOR_CMD_CREATE_MT_DN) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.dn(ts timestamp"
             ", cpu_taosd float, cpu_system float, cpu_cores int"
             ", mem_taosd float, mem_system float, mem_total int"
             ", disk_used float, disk_total int"
             ", band_speed float"
             ", io_read float, io_write float"
             ", req_http int, req_select int, req_insert int"
             ") tags (ipaddr binary(%d))",
             tsMonitorDbName, IP_LEN_STR + 1);
  } else if (cmd == MONITOR_CMD_CREATE_TB_DN) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.dn_%s using %s.dn tags('%s')", tsMonitorDbName,
             monitor->privateIpStr, tsMonitorDbName, tsPrivateIp);
  } else if (cmd == MONITOR_CMD_CREATE_MT_ACCT) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.acct(ts timestamp "
             ", currentPointsPerSecond bigint, maxPointsPerSecond bigint"
             ", totalTimeSeries bigint, maxTimeSeries bigint"
             ", totalStorage bigint, maxStorage bigint"
             ", totalQueryTime bigint, maxQueryTime bigint"
             ", totalInbound bigint, maxInbound bigint"
             ", totalOutbound bigint, maxOutbound bigint"
             ", totalDbs smallint, maxDbs smallint"
             ", totalUsers smallint, maxUsers smallint"
             ", totalStreams smallint, maxStreams smallint"
             ", totalConns smallint, maxConns smallint"
             ", accessState smallint"
             ") tags (acctId binary(%d))",
             tsMonitorDbName, TSDB_USER_LEN + 1);
  } else if (cmd == MONITOR_CMD_CREATE_TB_ACCT_ROOT) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.acct_%s using %s.acct tags('%s')", tsMonitorDbName, "root",
             tsMonitorDbName, "root");
  } else if (cmd == MONITOR_CMD_CREATE_TB_SLOWQUERY) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.slowquery(ts timestamp, username "
             "binary(%d), created_time timestamp, time bigint, sql binary(%d))",
             tsMonitorDbName, TSDB_TABLE_ID_LEN, TSDB_SHOW_SQL_LEN);
  } else if (cmd == MONITOR_CMD_CREATE_TB_LOG) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.log(ts timestamp, level tinyint, "
             "content binary(%d), ipaddr binary(%d))",
             tsMonitorDbName, LOG_LEN_STR, IP_LEN_STR);
  }

  sql[SQL_LENGTH] = 0;
}

void monitorInitDatabase() {
  if (monitor->cmdIndex < MONITOR_CMD_MAX) {
    dnodeBuildMonitorSql(monitor->sql, monitor->cmdIndex);
    taos_query_a(monitor->conn, monitor->sql, monitorInitDatabaseCb, NULL);
  } else {
    monitor->state = MONITOR_STATE_INITIALIZED;
    monitorPrint("monitor service init success");

    monitorStartTimer();
  }
}

void monitorInitDatabaseCb(void *param, TAOS_RES *result, int code) {
  if (-code == TSDB_CODE_TABLE_ALREADY_EXIST || -code == TSDB_CODE_DB_ALREADY_EXIST || code >= 0) {
    monitorTrace("monitor:%p, sql success, code:%d, %s", monitor->conn, code, monitor->sql);
    if (monitor->cmdIndex == MONITOR_CMD_CREATE_TB_LOG) {
      monitorLPrint("dnode:%s is started", tsPrivateIp);
    }
    monitor->cmdIndex++;
    monitorInitDatabase();
  } else {
    monitorError("monitor:%p, sql failed, code:%d, %s", monitor->conn, code, monitor->sql);
    monitor->state = MONITOR_STATE_UN_INIT;
    monitorStartSystemRetry();
  }
}

void monitorStopSystem() {
  if (monitor == NULL) {
    return;
  }

  monitorLPrint("dnode:%s monitor module is stopped", tsPrivateIp);
  monitor->state = MONITOR_STATE_STOPPED;
  // taosLogFp = NULL;
  if (monitor->initTimer != NULL) {
    taosTmrStopA(&(monitor->initTimer));
  }
  if (monitor->timer != NULL) {
    taosTmrStopA(&(monitor->timer));
  }
}

void monitorCleanUpSystem() {
  monitorPrint("monitor service cleanup");
  monitorStopSystem();
}

void monitorStartTimer() {
  taosTmrReset(monitorSaveSystemInfo, tsMonitorInterval * 1000, NULL, tscTmr, &monitor->timer);
}

void dnodeMontiorInsertAcctCallback(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    monitorError("monitor:%p, save account info failed, code:%d", monitor->conn, code);
  } else if (code == 0) {
    monitorError("monitor:%p, save account info failed, affect rows:%d", monitor->conn, code);
  } else {
    monitorTrace("monitor:%p, save account info success, code:%d", monitor->conn, code);
  }
}

void dnodeMontiorInsertSysCallback(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    monitorError("monitor:%p, save system info failed, code:%d %s", monitor->conn, code, monitor->sql);
  } else if (code == 0) {
    monitorError("monitor:%p, save system info failed, affect rows:%d %s", monitor->conn, code, monitor->sql);
  } else {
    monitorTrace("monitor:%p, save system info success, code:%d %s", monitor->conn, code, monitor->sql);
  }
}

void dnodeMontiorInsertLogCallback(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    monitorError("monitor:%p, save log failed, code:%d", monitor->conn, code);
  } else if (code == 0) {
    monitorError("monitor:%p, save log failed, affect rows:%d", monitor->conn, code);
  } else {
    monitorTrace("monitor:%p, save log info success, code:%d", monitor->conn, code);
  }
}

// unit is MB
int monitorBuildMemorySql(char *sql) {
  float sysMemoryUsedMB = 0;
  bool  suc = taosGetSysMemory(&sysMemoryUsedMB);
  if (!suc) {
    monitorError("monitor:%p, get sys memory info failed.", monitor->conn);
  }

  float procMemoryUsedMB = 0;
  suc = taosGetProcMemory(&procMemoryUsedMB);
  if (!suc) {
    monitorError("monitor:%p, get proc memory info failed.", monitor->conn);
  }

  return sprintf(sql, ", %f, %f, %d", procMemoryUsedMB, sysMemoryUsedMB, tsTotalMemoryMB);
}

// unit is %
int monitorBuildCpuSql(char *sql) {
  float sysCpuUsage = 0, procCpuUsage = 0;
  bool  suc = taosGetCpuUsage(&sysCpuUsage, &procCpuUsage);
  if (!suc) {
    monitorError("monitor:%p, get cpu usage failed.", monitor->conn);
  }

  if (sysCpuUsage <= procCpuUsage) {
    sysCpuUsage = procCpuUsage + (float)0.1;
  }

  return sprintf(sql, ", %f, %f, %d", procCpuUsage, sysCpuUsage, tsNumOfCores);
}

// unit is GB
int monitorBuildDiskSql(char *sql) {
  return sprintf(sql, ", %f, %d", (tsTotalDataDirGB - tsAvailDataDirGB), (int32_t)tsTotalDataDirGB);
}

// unit is Kb
int monitorBuildBandSql(char *sql) {
  float bandSpeedKb = 0;
  bool  suc = taosGetBandSpeed(&bandSpeedKb);
  if (!suc) {
    monitorError("monitor:%p, get bandwidth speed failed.", monitor->conn);
  }

  return sprintf(sql, ", %f", bandSpeedKb);
}

int monitorBuildReqSql(char *sql) {
  SDnodeStatisInfo info;
  info.httpReqNum = info.submitReqNum = info.queryReqNum = 0;
  (*mnodeCountRequestFp)(&info);

  return sprintf(sql, ", %d, %d, %d)", info.httpReqNum, info.queryReqNum, info.submitReqNum);
}

int monitorBuildIoSql(char *sql) {
  float readKB = 0, writeKB = 0;
  bool  suc = taosGetProcIO(&readKB, &writeKB);
  if (!suc) {
    monitorError("monitor:%p, get io info failed.", monitor->conn);
  }

  return sprintf(sql, ", %f, %f", readKB, writeKB);
}

void monitorSaveSystemInfo() {
  if (monitor->state != MONITOR_STATE_INITIALIZED) {
    return;
  }

  if (mnodeCountRequestFp == NULL) {
    return;
  }

  int64_t ts = taosGetTimestampUs();
  char *  sql = monitor->sql;
  int pos = snprintf(sql, SQL_LENGTH, "insert into %s.dn_%s values(%" PRId64, tsMonitorDbName, monitor->privateIpStr, ts);

  pos += monitorBuildCpuSql(sql + pos);
  pos += monitorBuildMemorySql(sql + pos);
  pos += monitorBuildDiskSql(sql + pos);
  pos += monitorBuildBandSql(sql + pos);
  pos += monitorBuildIoSql(sql + pos);
  pos += monitorBuildReqSql(sql + pos);

  monitorTrace("monitor:%p, save system info, sql:%s", monitor->conn, sql);
  taos_query_a(monitor->conn, sql, dnodeMontiorInsertSysCallback, "log");

  if (monitor->timer != NULL && monitor->state != MONITOR_STATE_STOPPED) {
    monitorStartTimer();
  }
}

void monitorSaveAcctLog(char *acctId, int64_t currentPointsPerSecond, int64_t maxPointsPerSecond,
                        int64_t totalTimeSeries, int64_t maxTimeSeries, int64_t totalStorage, int64_t maxStorage,
                        int64_t totalQueryTime, int64_t maxQueryTime, int64_t totalInbound, int64_t maxInbound,
                        int64_t totalOutbound, int64_t maxOutbound, int64_t totalDbs, int64_t maxDbs,
                        int64_t totalUsers, int64_t maxUsers, int64_t totalStreams, int64_t maxStreams,
                        int64_t totalConns, int64_t maxConns, int8_t accessState) {
  if (monitor == NULL) return;
  if (monitor->state != MONITOR_STATE_INITIALIZED) return;

  char sql[1024] = {0};
  sprintf(sql,
          "insert into %s.acct_%s using %s.acct tags('%s') values(now"
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
	      ", %" PRId64 ", %" PRId64
          ", %d)",
          tsMonitorDbName, acctId, tsMonitorDbName, acctId,
          currentPointsPerSecond, maxPointsPerSecond,
          totalTimeSeries, maxTimeSeries,
          totalStorage, maxStorage,
          totalQueryTime, maxQueryTime,
          totalInbound, maxInbound,
          totalOutbound, maxOutbound,
          totalDbs, maxDbs,
          totalUsers, maxUsers,
          totalStreams, maxStreams,
          totalConns, maxConns,
          accessState);

  monitorTrace("monitor:%p, save account info, sql %s", monitor->conn, sql);
  taos_query_a(monitor->conn, sql, dnodeMontiorInsertAcctCallback, "account");
}

void monitorSaveLog(int level, const char *const format, ...) {
  va_list argpointer;
  char    sql[SQL_LENGTH] = {0};
  int     max_length = SQL_LENGTH - 30;

  if (monitor->state != MONITOR_STATE_INITIALIZED) {
    return;
  }

  int len = snprintf(sql, (size_t)max_length, "import into %s.log values(%" PRId64 ", %d,'", tsMonitorDbName,
                     taosGetTimestampUs(), level);

  va_start(argpointer, format);
  len += vsnprintf(sql + len, (size_t)(max_length - len), format, argpointer);
  va_end(argpointer);
  if (len > max_length) len = max_length;

  len += sprintf(sql + len, "', '%s')", tsPrivateIp);
  sql[len++] = 0;

  monitorTrace("monitor:%p, save log, sql: %s", monitor->conn, sql);
  taos_query_a(monitor->conn, sql, dnodeMontiorInsertLogCallback, "log");
}

void monitorExecuteSQL(char *sql) {
  monitorTrace("monitor:%p, execute sql: %s", monitor->conn, sql);
  taos_query_a(monitor->conn, sql, NULL, NULL);
}
