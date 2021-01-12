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
#include "ttimer.h"
#include "tutil.h"
#include "tsystem.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "dnode.h"
#include "monitor.h"
#include "taoserror.h"

#define monFatal(...) { if (monDebugFlag & DEBUG_FATAL) { taosPrintLog("MON FATAL ", 255, __VA_ARGS__); }}
#define monError(...) { if (monDebugFlag & DEBUG_ERROR) { taosPrintLog("MON ERROR ", 255, __VA_ARGS__); }}
#define monWarn(...)  { if (monDebugFlag & DEBUG_WARN)  { taosPrintLog("MON WARN ", 255, __VA_ARGS__); }}
#define monInfo(...)  { if (monDebugFlag & DEBUG_INFO)  { taosPrintLog("MON ", 255, __VA_ARGS__); }}
#define monDebug(...) { if (monDebugFlag & DEBUG_DEBUG) { taosPrintLog("MON ", monDebugFlag, __VA_ARGS__); }}
#define monTrace(...) { if (monDebugFlag & DEBUG_TRACE) { taosPrintLog("MON ", monDebugFlag, __VA_ARGS__); }}

#define SQL_LENGTH     1030
#define LOG_LEN_STR    100
#define IP_LEN_STR     TSDB_EP_LEN
#define CHECK_INTERVAL 1000

typedef enum {
  MON_CMD_CREATE_DB,
  MON_CMD_CREATE_TB_LOG,
  MON_CMD_CREATE_MT_DN,
  MON_CMD_CREATE_MT_ACCT,
  MON_CMD_CREATE_TB_DN,
  MON_CMD_CREATE_TB_ACCT_ROOT,
  MON_CMD_CREATE_TB_SLOWQUERY,
  MON_CMD_MAX
} EMonCmd;

typedef enum {
  MON_STATE_NOT_INIT,
  MON_STATE_INITED
} EMonState;

typedef struct {
  pthread_t thread;
  void *    conn;
  char      ep[TSDB_EP_LEN];
  int8_t    cmdIndex;
  int8_t    state;
  int8_t    start;   // enable/disable by mnode
  int8_t    quiting; // taosd is quiting 
  char      sql[SQL_LENGTH + 1];
} SMonConn;

static SMonConn tsMonitor = {0};
static void  monSaveSystemInfo();
static void *monThreadFunc(void *param);
static void  monBuildMonitorSql(char *sql, int32_t cmd);
extern int32_t (*monStartSystemFp)();
extern void    (*monStopSystemFp)();
extern void    (*monExecuteSQLFp)(char *sql);

int32_t monInitSystem() {
  if (tsMonitor.ep[0] == 0) {
    strcpy(tsMonitor.ep, tsLocalEp);
  }

  int32_t len = (int32_t)strlen(tsMonitor.ep);
  for (int32_t i = 0; i < len; ++i) {
    if (tsMonitor.ep[i] == ':' || tsMonitor.ep[i] == '-' || tsMonitor.ep[i] == '.') {
      tsMonitor.ep[i] = '_';
    }
  }

  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&tsMonitor.thread, &thAttr, monThreadFunc, NULL)) {
    monError("failed to create thread to for monitor module, reason:%s", strerror(errno));
    return -1;
  }

  pthread_attr_destroy(&thAttr);
  monDebug("monitor thread is launched");

  monStartSystemFp = monStartSystem;
  monStopSystemFp = monStopSystem;
  return 0;
}

int32_t monStartSystem() {
  taos_init();
  tsMonitor.start = 1;
  monExecuteSQLFp = monExecuteSQL;
  monInfo("monitor module start");
  return 0;
}

static void *monThreadFunc(void *param) {
  monDebug("starting to initialize monitor module ...");

  while (1) {
    static int32_t accessTimes = 0;
    accessTimes++;
    taosMsleep(1000);

    if (tsMonitor.quiting) {
      tsMonitor.state = MON_STATE_NOT_INIT;
      monInfo("monitor thread will quit, for taosd is quiting");
      break;
    } else {
      taosGetDisk();
    }

    if (tsMonitor.start == 0) {
      continue;
    }
    
    if (dnodeGetDnodeId() <= 0) {
      monDebug("dnode not initialized, waiting for 3000 ms to start monitor module");
      continue;
    }

    if (tsMonitor.conn == NULL) {
      tsMonitor.state = MON_STATE_NOT_INIT;
      tsMonitor.conn = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
      if (tsMonitor.conn == NULL) {
        monError("failed to connect to database, reason:%s", tstrerror(terrno));
        continue;
      } else {
        monDebug("connect to database success");
      }
    }

    if (tsMonitor.state == MON_STATE_NOT_INIT) {
      int32_t code = 0;

      for (; tsMonitor.cmdIndex < MON_CMD_MAX; ++tsMonitor.cmdIndex) {
        monBuildMonitorSql(tsMonitor.sql, tsMonitor.cmdIndex);
        void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
        code = taos_errno(res);
        taos_free_result(res);

        if (code != 0) {
          monError("failed to exec sql:%s, reason:%s", tsMonitor.sql, tstrerror(code));
          break;
        } else {
          monDebug("successfully to exec sql:%s", tsMonitor.sql);
        }
      }

      if (tsMonitor.start && code == 0) {
        tsMonitor.state = MON_STATE_INITED;
      }
    }

    if (tsMonitor.state == MON_STATE_INITED) {
      if (accessTimes % tsMonitorInterval == 0) {
        monSaveSystemInfo();
      }
    }
  }

  monInfo("monitor thread is stopped");
  return NULL;
}

static void monBuildMonitorSql(char *sql, int32_t cmd) {
  memset(sql, 0, SQL_LENGTH);

  if (cmd == MON_CMD_CREATE_DB) {
    snprintf(sql, SQL_LENGTH,
             "create database if not exists %s replica 1 days 10 keep 30 cache %d "
             "blocks %d precision 'us'",
             tsMonitorDbName, TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MIN_TOTAL_BLOCKS);
  } else if (cmd == MON_CMD_CREATE_MT_DN) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.dn(ts timestamp"
             ", cpu_taosd float, cpu_system float, cpu_cores int"
             ", mem_taosd float, mem_system float, mem_total int"
             ", disk_used float, disk_total int"
             ", band_speed float"
             ", io_read float, io_write float"
             ", req_http int, req_select int, req_insert int"
             ") tags (dnodeid int, fqdn binary(%d))",
             tsMonitorDbName, TSDB_FQDN_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_DN) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.dn%d using %s.dn tags(%d, '%s')", tsMonitorDbName,
             dnodeGetDnodeId(), tsMonitorDbName, dnodeGetDnodeId(), tsLocalEp);
  } else if (cmd == MON_CMD_CREATE_MT_ACCT) {
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
             tsMonitorDbName, TSDB_USER_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_ACCT_ROOT) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.acct_%s using %s.acct tags('%s')", tsMonitorDbName, TSDB_DEFAULT_USER,
             tsMonitorDbName, TSDB_DEFAULT_USER);
  } else if (cmd == MON_CMD_CREATE_TB_SLOWQUERY) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.slowquery(ts timestamp, username "
             "binary(%d), created_time timestamp, time bigint, sql binary(%d))",
             tsMonitorDbName, TSDB_TABLE_FNAME_LEN - 1, TSDB_SLOW_QUERY_SQL_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_LOG) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.log(ts timestamp, level tinyint, "
             "content binary(%d), ipaddr binary(%d))",
             tsMonitorDbName, LOG_LEN_STR, IP_LEN_STR);
  }

  sql[SQL_LENGTH] = 0;
}

void monStopSystem() {
  tsMonitor.start = 0;
  tsMonitor.state = MON_STATE_NOT_INIT;
  monExecuteSQLFp = NULL;
  monInfo("monitor module stopped");
}

void monCleanupSystem() {
  tsMonitor.quiting = 1;
  monStopSystem();
  pthread_join(tsMonitor.thread, NULL);
  if (tsMonitor.conn != NULL) {
    taos_close(tsMonitor.conn);
    tsMonitor.conn = NULL;
  }
  monInfo("monitor module is cleaned up");
}

// unit is MB
static int32_t monBuildMemorySql(char *sql) {
  float sysMemoryUsedMB = 0;
  bool  suc = taosGetSysMemory(&sysMemoryUsedMB);
  if (!suc) {
    monDebug("failed to get sys memory info");
  }

  float procMemoryUsedMB = 0;
  suc = taosGetProcMemory(&procMemoryUsedMB);
  if (!suc) {
    monDebug("failed to get proc memory info");
  }

  return sprintf(sql, ", %f, %f, %d", procMemoryUsedMB, sysMemoryUsedMB, tsTotalMemoryMB);
}

// unit is %
static int32_t monBuildCpuSql(char *sql) {
  float sysCpuUsage = 0, procCpuUsage = 0;
  bool  suc = taosGetCpuUsage(&sysCpuUsage, &procCpuUsage);
  if (!suc) {
    monDebug("failed to get cpu usage");
  }

  if (sysCpuUsage <= procCpuUsage) {
    sysCpuUsage = procCpuUsage + 0.1f;
  }

  return sprintf(sql, ", %f, %f, %d", procCpuUsage, sysCpuUsage, tsNumOfCores);
}

// unit is GB
static int32_t monBuildDiskSql(char *sql) {
  return sprintf(sql, ", %f, %d", (tsTotalDataDirGB - tsAvailDataDirGB), (int32_t)tsTotalDataDirGB);
}

// unit is Kb
static int32_t monBuildBandSql(char *sql) {
  float bandSpeedKb = 0;
  bool  suc = taosGetBandSpeed(&bandSpeedKb);
  if (!suc) {
    monDebug("failed to get bandwidth speed");
  }

  return sprintf(sql, ", %f", bandSpeedKb);
}

static int32_t monBuildReqSql(char *sql) {
  SStatisInfo info = dnodeGetStatisInfo();
  return sprintf(sql, ", %d, %d, %d)", info.httpReqNum, info.queryReqNum, info.submitReqNum);
}

static int32_t monBuildIoSql(char *sql) {
  float readKB = 0, writeKB = 0;
  bool  suc = taosGetProcIO(&readKB, &writeKB);
  if (!suc) {
    monDebug("failed to get io info");
  }

  return sprintf(sql, ", %f, %f", readKB, writeKB);
}

static void monSaveSystemInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.dn%d values(%" PRId64, tsMonitorDbName, dnodeGetDnodeId(), ts);

  pos += monBuildCpuSql(sql + pos);
  pos += monBuildMemorySql(sql + pos);
  pos += monBuildDiskSql(sql + pos);
  pos += monBuildBandSql(sql + pos);
  pos += monBuildIoSql(sql + pos);
  pos += monBuildReqSql(sql + pos);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int32_t code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save system info, reason:%s, sql:%s", tstrerror(code), tsMonitor.sql);
  } else {
    monDebug("successfully to save system info, sql:%s", tsMonitor.sql);
  }
}

static void monExecSqlCb(void *param, TAOS_RES *result, int32_t code) {
  int32_t c = taos_errno(result);
  if (c != TSDB_CODE_SUCCESS) {
    monError("save %s failed, reason:%s", (char *)param, tstrerror(c));
  } else {
    int32_t rows = taos_affected_rows(result);
    monDebug("save %s succ, rows:%d", (char *)param, rows);
  }

  taos_free_result(result);
}

void monSaveAcctLog(SAcctMonitorObj *pMon) {
  if (tsMonitor.state != MON_STATE_INITED) return;

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

  monDebug("save account info, sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "account info");
}

void monSaveLog(int32_t level, const char *const format, ...) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  va_list argpointer;
  char    sql[SQL_LENGTH] = {0};
  int32_t max_length = SQL_LENGTH - 30;
  int32_t len = snprintf(sql, (size_t)max_length, "insert into %s.log values(%" PRId64 ", %d,'", tsMonitorDbName,
                         taosGetTimestampUs(), level);

  va_start(argpointer, format);
  len += vsnprintf(sql + len, (size_t)(max_length - len), format, argpointer);
  va_end(argpointer);
  if (len > max_length) len = max_length;

  len += sprintf(sql + len, "', '%s')", tsLocalEp);
  sql[len++] = 0;

  monDebug("save log, sql: %s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "log");
}

void monExecuteSQL(char *sql) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  monDebug("execute sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "sql");
}
