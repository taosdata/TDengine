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
#include "taosdef.h"
#include "taoserror.h"
#include "tlog.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "tsystem.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "dnode.h"
#include "monitor.h"

#define monitorError(...)                         \
  if (monitorDebugFlag & DEBUG_ERROR) {           \
    taosPrintLog("ERROR MON ", 255, __VA_ARGS__); \
  }
#define monitorWarn(...)                                       \
  if (monitorDebugFlag & DEBUG_WARN) {                         \
    taosPrintLog("WARN  MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorTrace(...)                                \
  if (monitorDebugFlag & DEBUG_TRACE) {                  \
    taosPrintLog("MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorPrint(...) \
  { taosPrintLog("MON ", 255, __VA_ARGS__); }

#define SQL_LENGTH     1024
#define LOG_LEN_STR    100
#define IP_LEN_STR     18
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
} EMonitorCommand;

typedef enum {
  MONITOR_STATE_UN_INIT,
  MONITOR_STATE_INITIALIZING,
  MONITOR_STATE_INITIALIZED,
  MONITOR_STATE_STOPPED
} EMonitorState;

typedef struct {
  void * conn;
  void * timer;
  char   ep[TSDB_FQDN_LEN];
  int8_t cmdIndex;
  int8_t state;
  char   sql[SQL_LENGTH];
  void * initTimer;
  void * diskTimer;
} SMonitorConn;

static SMonitorConn tsMonitorConn;
static void monitorInitConn(void *para, void *unused);
static void monitorInitConnCb(void *param, TAOS_RES *result, int32_t code);
static void monitorInitDatabase();
static void monitorInitDatabaseCb(void *param, TAOS_RES *result, int32_t code);
static void monitorStartTimer();
static void monitorSaveSystemInfo();

static void monitorCheckDiskUsage(void *para, void *unused) {
  taosGetDisk();
  taosTmrReset(monitorCheckDiskUsage, CHECK_INTERVAL, NULL, tscTmr, &tsMonitorConn.diskTimer);
}

int32_t monitorInitSystem() {
  taos_init();
  taosTmrReset(monitorCheckDiskUsage, CHECK_INTERVAL, NULL, tscTmr, &tsMonitorConn.diskTimer);
  return 0;
}

int32_t monitorStartSystem() {
  monitorPrint("start monitor module");
  monitorInitSystem();
  taosTmrReset(monitorInitConn, 10, NULL, tscTmr, &tsMonitorConn.initTimer);
  return 0;
}

static void monitorStartSystemRetry() {
  if (tsMonitorConn.initTimer != NULL) {
    taosTmrReset(monitorInitConn, 3000, NULL, tscTmr, &tsMonitorConn.initTimer);
  }
}

static void monitorInitConn(void *para, void *unused) {
  monitorPrint("starting to initialize monitor service ..");
  tsMonitorConn.state = MONITOR_STATE_INITIALIZING;

  if (tsMonitorConn.ep[0] == 0) 
    strcpy(tsMonitorConn.ep, tsLocalEp);

  int len = strlen(tsMonitorConn.ep);
  for (int i = 0; i < len; ++i) {
    if (tsMonitorConn.ep[i] == ':' || tsMonitorConn.ep[i] == '-') {
      tsMonitorConn.ep[i] = '_';
    }
  }

  if (tsMonitorConn.conn == NULL) {
    taos_connect_a(NULL, "monitor", tsInternalPass, "", 0, monitorInitConnCb, &tsMonitorConn, &(tsMonitorConn.conn));
  } else {
    monitorInitDatabase();
  }
}

static void monitorInitConnCb(void *param, TAOS_RES *result, int32_t code) {
  if (code < 0) {
    monitorError("monitor:%p, connect to database failed, reason:%s", tsMonitorConn.conn, tstrerror(code));
    taos_close(tsMonitorConn.conn);
    tsMonitorConn.conn = NULL;
    tsMonitorConn.state = MONITOR_STATE_UN_INIT;
    monitorStartSystemRetry();
    return;
  }

  monitorTrace("monitor:%p, connect to database success, reason:%s", tsMonitorConn.conn, tstrerror(code));
  monitorInitDatabase();
}

static void dnodeBuildMonitorSql(char *sql, int32_t cmd) {
  memset(sql, 0, SQL_LENGTH);

  if (cmd == MONITOR_CMD_CREATE_DB) {
    snprintf(sql, SQL_LENGTH,
             "create database if not exists %s replica 1 days 10 keep 30 cache 1 "
             "blocks 2 maxtables 16 precision 'us'",
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
             tsMonitorDbName, TSDB_FQDN_LEN + 1);
  } else if (cmd == MONITOR_CMD_CREATE_TB_DN) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.dn_%s using %s.dn tags('%s')", tsMonitorDbName,
             tsMonitorConn.ep, tsMonitorDbName, tsLocalEp);
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

static void monitorInitDatabase() {
  if (tsMonitorConn.cmdIndex < MONITOR_CMD_MAX) {
    dnodeBuildMonitorSql(tsMonitorConn.sql, tsMonitorConn.cmdIndex);
    taos_query_a(tsMonitorConn.conn, tsMonitorConn.sql, monitorInitDatabaseCb, NULL);
  } else {
    tsMonitorConn.state = MONITOR_STATE_INITIALIZED;
    monitorPrint("monitor service init success");

    monitorStartTimer();
  }
}

static void monitorInitDatabaseCb(void *param, TAOS_RES *result, int32_t code) {
  if (-code == TSDB_CODE_TABLE_ALREADY_EXIST || -code == TSDB_CODE_DB_ALREADY_EXIST || code >= 0) {
    monitorTrace("monitor:%p, sql success, reason:%d, %s", tsMonitorConn.conn, tstrerror(code), tsMonitorConn.sql);
    if (tsMonitorConn.cmdIndex == MONITOR_CMD_CREATE_TB_LOG) {
      monitorPrint("dnode:%s is started", tsLocalEp);
    }
    tsMonitorConn.cmdIndex++;
    monitorInitDatabase();
  } else {
    monitorError("monitor:%p, sql failed, reason:%s, %s", tsMonitorConn.conn, tstrerror(code), tsMonitorConn.sql);
    tsMonitorConn.state = MONITOR_STATE_UN_INIT;
    monitorStartSystemRetry();
  }
}

void monitorStopSystem() {
  monitorPrint("monitor module is stopped");
  tsMonitorConn.state = MONITOR_STATE_STOPPED;
  if (tsMonitorConn.initTimer != NULL) {
    taosTmrStopA(&(tsMonitorConn.initTimer));
  }
  if (tsMonitorConn.timer != NULL) {
    taosTmrStopA(&(tsMonitorConn.timer));
  }
}

void monitorCleanUpSystem() {
  monitorStopSystem();
  monitorPrint("monitor module cleanup");
}

static void monitorStartTimer() {
  taosTmrReset(monitorSaveSystemInfo, tsMonitorInterval * 1000, NULL, tscTmr, &tsMonitorConn.timer);
}

static void dnodeMontiorInsertAcctCallback(void *param, TAOS_RES *result, int32_t code) {
  if (code < 0) {
    monitorError("monitor:%p, save account info failed, code:%s", tsMonitorConn.conn, tstrerror(code));
  } else if (code == 0) {
    monitorError("monitor:%p, save account info failed, affect rows:%d", tsMonitorConn.conn, code);
  } else {
    monitorTrace("monitor:%p, save account info success, code:%s", tsMonitorConn.conn, tstrerror(code));
  }
}

static void dnodeMontiorInsertSysCallback(void *param, TAOS_RES *result, int32_t code) {
  if (code < 0) {
    monitorError("monitor:%p, save system info failed, code:%s %s", tsMonitorConn.conn, tstrerror(code), tsMonitorConn.sql);
  } else if (code == 0) {
    monitorError("monitor:%p, save system info failed, affect rows:%d %s", tsMonitorConn.conn, code, tsMonitorConn.sql);
  } else {
    monitorTrace("monitor:%p, save system info success, code:%s %s", tsMonitorConn.conn, tstrerror(code), tsMonitorConn.sql);
  }
}

static void dnodeMontiorInsertLogCallback(void *param, TAOS_RES *result, int32_t code) {
  if (code < 0) {
    monitorError("monitor:%p, save log failed, code:%s", tsMonitorConn.conn, tstrerror(code));
  } else if (code == 0) {
    monitorError("monitor:%p, save log failed, affect rows:%d", tsMonitorConn.conn, code);
  } else {
    monitorTrace("monitor:%p, save log info success, code:%s", tsMonitorConn.conn, tstrerror(code));
  }
}

// unit is MB
static int32_t monitorBuildMemorySql(char *sql) {
  float sysMemoryUsedMB = 0;
  bool  suc = taosGetSysMemory(&sysMemoryUsedMB);
  if (!suc) {
    monitorError("monitor:%p, get sys memory info failed.", tsMonitorConn.conn);
  }

  float procMemoryUsedMB = 0;
  suc = taosGetProcMemory(&procMemoryUsedMB);
  if (!suc) {
    monitorError("monitor:%p, get proc memory info failed.", tsMonitorConn.conn);
  }

  return sprintf(sql, ", %f, %f, %d", procMemoryUsedMB, sysMemoryUsedMB, tsTotalMemoryMB);
}

// unit is %
static int32_t monitorBuildCpuSql(char *sql) {
  float sysCpuUsage = 0, procCpuUsage = 0;
  bool  suc = taosGetCpuUsage(&sysCpuUsage, &procCpuUsage);
  if (!suc) {
    monitorError("monitor:%p, get cpu usage failed.", tsMonitorConn.conn);
  }

  if (sysCpuUsage <= procCpuUsage) {
    sysCpuUsage = procCpuUsage + (float)0.1;
  }

  return sprintf(sql, ", %f, %f, %d", procCpuUsage, sysCpuUsage, tsNumOfCores);
}

// unit is GB
static int32_t monitorBuildDiskSql(char *sql) {
  return sprintf(sql, ", %f, %d", (tsTotalDataDirGB - tsAvailDataDirGB), (int32_t)tsTotalDataDirGB);
}

// unit is Kb
static int32_t monitorBuildBandSql(char *sql) {
  float bandSpeedKb = 0;
  bool  suc = taosGetBandSpeed(&bandSpeedKb);
  if (!suc) {
    monitorError("monitor:%p, get bandwidth speed failed.", tsMonitorConn.conn);
  }

  return sprintf(sql, ", %f", bandSpeedKb);
}

static int32_t monitorBuildReqSql(char *sql) {
  SDnodeStatisInfo info = dnodeGetStatisInfo(); 
  return sprintf(sql, ", %d, %d, %d)", info.httpReqNum, info.queryReqNum, info.submitReqNum);
}

static int32_t monitorBuildIoSql(char *sql) {
  float readKB = 0, writeKB = 0;
  bool  suc = taosGetProcIO(&readKB, &writeKB);
  if (!suc) {
    monitorError("monitor:%p, get io info failed.", tsMonitorConn.conn);
  }

  return sprintf(sql, ", %f, %f", readKB, writeKB);
}

static void monitorSaveSystemInfo() {
  if (tsMonitorConn.state != MONITOR_STATE_INITIALIZED) {
    monitorStartTimer();
    return;
  }

  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitorConn.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.dn_%s values(%" PRId64, tsMonitorDbName, tsMonitorConn.ep, ts);

  pos += monitorBuildCpuSql(sql + pos);
  pos += monitorBuildMemorySql(sql + pos);
  pos += monitorBuildDiskSql(sql + pos);
  pos += monitorBuildBandSql(sql + pos);
  pos += monitorBuildIoSql(sql + pos);
  pos += monitorBuildReqSql(sql + pos);

  monitorTrace("monitor:%p, save system info, sql:%s", tsMonitorConn.conn, sql);
  taos_query_a(tsMonitorConn.conn, sql, dnodeMontiorInsertSysCallback, "log");

  if (tsMonitorConn.timer != NULL && tsMonitorConn.state != MONITOR_STATE_STOPPED) {
    monitorStartTimer();
  }
}

void monitorSaveAcctLog(SAcctMonitorObj *pMon) {
  if (tsMonitorConn.state != MONITOR_STATE_INITIALIZED) return;

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
          tsMonitorDbName, pMon->acctId, tsMonitorDbName, pMon->acctId,
          pMon->currentPointsPerSecond, pMon->maxPointsPerSecond,
          pMon->totalTimeSeries, pMon->maxTimeSeries,
          pMon->totalStorage, pMon->maxStorage,
          pMon->totalQueryTime, pMon->maxQueryTime,
          pMon->totalInbound, pMon->maxInbound,
          pMon->totalOutbound, pMon->maxOutbound,
          pMon->totalDbs, pMon->maxDbs,
          pMon->totalUsers, pMon->maxUsers,
          pMon->totalStreams, pMon->maxStreams,
          pMon->totalConns, pMon->maxConns,
          pMon->accessState);

  monitorTrace("monitor:%p, save account info, sql %s", tsMonitorConn.conn, sql);
  taos_query_a(tsMonitorConn.conn, sql, dnodeMontiorInsertAcctCallback, "account");
}

void monitorSaveLog(int32_t level, const char *const format, ...) {
  if (tsMonitorConn.state != MONITOR_STATE_INITIALIZED) return;

  va_list argpointer;
  char    sql[SQL_LENGTH] = {0};
  int32_t max_length = SQL_LENGTH - 30;

  if (tsMonitorConn.state != MONITOR_STATE_INITIALIZED) return;

  int32_t len = snprintf(sql, (size_t)max_length, "insert into %s.log values(%" PRId64 ", %d,'", tsMonitorDbName,
                         taosGetTimestampUs(), level);

  va_start(argpointer, format);
  len += vsnprintf(sql + len, (size_t)(max_length - len), format, argpointer);
  va_end(argpointer);
  if (len > max_length) len = max_length;

  len += sprintf(sql + len, "', '%s')", tsLocalEp);
  sql[len++] = 0;

  monitorTrace("monitor:%p, save log, sql: %s", tsMonitorConn.conn, sql);
  taos_query_a(tsMonitorConn.conn, sql, dnodeMontiorInsertLogCallback, "log");
}

void monitorExecuteSQL(char *sql) {
  if (tsMonitorConn.state != MONITOR_STATE_INITIALIZED) return;

  monitorTrace("monitor:%p, execute sql: %s", tsMonitorConn.conn, sql);
  
  // bug while insert binary
  // taos_query_a(tsMonitorConn.conn, sql, NULL, NULL);
}
