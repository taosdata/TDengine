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
#include "tdisk.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "dnode.h"
#include "monitor.h"
#include "taoserror.h"

#define mnFatal(...) { if (monitorDebugFlag & DEBUG_FATAL) { taosPrintLog("MON FATAL ", 255, __VA_ARGS__); }}
#define mnError(...) { if (monitorDebugFlag & DEBUG_ERROR) { taosPrintLog("MON ERROR ", 255, __VA_ARGS__); }}
#define mnWarn(...)  { if (monitorDebugFlag & DEBUG_WARN)  { taosPrintLog("MON WARN ", 255, __VA_ARGS__); }}
#define mnInfo(...)  { if (monitorDebugFlag & DEBUG_INFO)  { taosPrintLog("MON ", 255, __VA_ARGS__); }}
#define mnDebug(...) { if (monitorDebugFlag & DEBUG_DEBUG) { taosPrintLog("MON ", monitorDebugFlag, __VA_ARGS__); }}
#define mnTrace(...) { if (monitorDebugFlag & DEBUG_TRACE) { taosPrintLog("MON ", monitorDebugFlag, __VA_ARGS__); }}

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
} EMonitorCommand;

typedef enum {
  MON_STATE_NOT_INIT,
  MON_STATE_INITED
} EMonitorState;

typedef struct {
  pthread_t thread;
  void *    conn;
  char      ep[TSDB_EP_LEN];
  int8_t    cmdIndex;
  int8_t    state;
  int8_t    start;   // enable/disable by mnode
  int8_t    quiting; // taosd is quiting 
  char      sql[SQL_LENGTH + 1];
} SMonitorConn;

static SMonitorConn tsMonitor = {0};
static void  monitorSaveSystemInfo();
static void *monitorThreadFunc(void *param);
static void  monitorBuildMonitorSql(char *sql, int32_t cmd);
extern int32_t (*monitorStartSystemFp)();
extern void    (*monitorStopSystemFp)();
extern void    (*monitorExecuteSQLFp)(char *sql);

int32_t monitorInitSystem() {
  if (tsMonitor.ep[0] == 0) {
    strcpy(tsMonitor.ep, tsLocalEp);
  }

  int len = strlen(tsMonitor.ep);
  for (int i = 0; i < len; ++i) {
    if (tsMonitor.ep[i] == ':' || tsMonitor.ep[i] == '-' || tsMonitor.ep[i] == '.') {
      tsMonitor.ep[i] = '_';
    }
  }

  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&tsMonitor.thread, &thAttr, monitorThreadFunc, NULL)) {
    mnError("failed to create thread to for monitor module, reason:%s", strerror(errno));
    return -1;
  }

  pthread_attr_destroy(&thAttr);
  mnDebug("monitor thread is launched");

  monitorStartSystemFp = monitorStartSystem;
  monitorStopSystemFp = monitorStopSystem;
  return 0;
}

int32_t monitorStartSystem() {
  taos_init();
  tsMonitor.start = 1;
  monitorExecuteSQLFp = monitorExecuteSQL;
  mnInfo("monitor module start");
  return 0;
}

static void *monitorThreadFunc(void *param) {
  mnDebug("starting to initialize monitor module ...");

  while (1) {
    static int32_t accessTimes = 0;
    accessTimes++;
    taosMsleep(1000);

    if (tsMonitor.quiting) {
      tsMonitor.state = MON_STATE_NOT_INIT;
      mnInfo("monitor thread will quit, for taosd is quiting");
      break;
    } else {
      taosGetDisk();
    }

    if (tsMonitor.start == 0) {
      continue;
    }
    
    if (dnodeGetDnodeId() <= 0) {
      mnDebug("dnode not initialized, waiting for 3000 ms to start monitor module");
      continue;
    }

    if (tsMonitor.conn == NULL) {
      tsMonitor.state = MON_STATE_NOT_INIT;
      tsMonitor.conn = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
      if (tsMonitor.conn == NULL) {
        mnError("failed to connect to database, reason:%s", tstrerror(terrno));
        continue;
      } else {
        mnDebug("connect to database success");
      }
    }

    if (tsMonitor.state == MON_STATE_NOT_INIT) {
      for (; tsMonitor.cmdIndex < MON_CMD_MAX; ++tsMonitor.cmdIndex) {
        monitorBuildMonitorSql(tsMonitor.sql, tsMonitor.cmdIndex);
        void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
        int   code = taos_errno(res);
        taos_free_result(res);

        if (code != 0) {
          mnError("failed to exec sql:%s, reason:%s", tsMonitor.sql, tstrerror(code));
          break;
        } else {
          mnDebug("successfully to exec sql:%s", tsMonitor.sql);
        }
      }

      if (tsMonitor.start) {
        tsMonitor.state = MON_STATE_INITED;
      }
    }

    if (tsMonitor.state == MON_STATE_INITED) {
      if (accessTimes % tsMonitorInterval == 0) {
        monitorSaveSystemInfo();
      }
    }
  }

  mnInfo("monitor thread is stopped");
  return NULL;
}

static void monitorBuildMonitorSql(char *sql, int32_t cmd) {
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

void monitorStopSystem() {
  tsMonitor.start = 0;
  tsMonitor.state = MON_STATE_NOT_INIT;
  monitorExecuteSQLFp = NULL;
  mnInfo("monitor module stopped");
}

void monitorCleanUpSystem() {
  tsMonitor.quiting = 1;
  monitorStopSystem();
  pthread_join(tsMonitor.thread, NULL);
  if (tsMonitor.conn != NULL) {
    taos_close(tsMonitor.conn);
    tsMonitor.conn = NULL;
  }
  mnInfo("monitor module is cleaned up");
}

// unit is MB
static int32_t monitorBuildMemorySql(char *sql) {
  float sysMemoryUsedMB = 0;
  bool  suc = taosGetSysMemory(&sysMemoryUsedMB);
  if (!suc) {
    mnDebug("failed to get sys memory info");
  }

  float procMemoryUsedMB = 0;
  suc = taosGetProcMemory(&procMemoryUsedMB);
  if (!suc) {
    mnDebug("failed to get proc memory info");
  }

  return sprintf(sql, ", %f, %f, %d", procMemoryUsedMB, sysMemoryUsedMB, tsTotalMemoryMB);
}

// unit is %
static int32_t monitorBuildCpuSql(char *sql) {
  float sysCpuUsage = 0, procCpuUsage = 0;
  bool  suc = taosGetCpuUsage(&sysCpuUsage, &procCpuUsage);
  if (!suc) {
    mnDebug("failed to get cpu usage");
  }

  if (sysCpuUsage <= procCpuUsage) {
    sysCpuUsage = procCpuUsage + 0.1f;
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
    mnDebug("failed to get bandwidth speed");
  }

  return sprintf(sql, ", %f", bandSpeedKb);
}

static int32_t monitorBuildReqSql(char *sql) {
  SStatisInfo info = dnodeGetStatisInfo();
  return sprintf(sql, ", %d, %d, %d)", info.httpReqNum, info.queryReqNum, info.submitReqNum);
}

static int32_t monitorBuildIoSql(char *sql) {
  float readKB = 0, writeKB = 0;
  bool  suc = taosGetProcIO(&readKB, &writeKB);
  if (!suc) {
    mnDebug("failed to get io info");
  }

  return sprintf(sql, ", %f, %f", readKB, writeKB);
}

static void monitorSaveSystemInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.dn%d values(%" PRId64, tsMonitorDbName, dnodeGetDnodeId(), ts);

  pos += monitorBuildCpuSql(sql + pos);
  pos += monitorBuildMemorySql(sql + pos);
  pos += monitorBuildDiskSql(sql + pos);
  pos += monitorBuildBandSql(sql + pos);
  pos += monitorBuildIoSql(sql + pos);
  pos += monitorBuildReqSql(sql + pos);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int   code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    mnError("failed to save system info, reason:%s, sql:%s", tstrerror(code), tsMonitor.sql);
  } else {
    mnDebug("successfully to save system info, sql:%s", tsMonitor.sql);
  }
}

static void montiorExecSqlCb(void *param, TAOS_RES *result, int32_t code) {
  int32_t c = taos_errno(result);
  if (c != TSDB_CODE_SUCCESS) {
    mnError("save %s failed, reason:%s", (char *)param, tstrerror(c));
  } else {
    int32_t rows = taos_affected_rows(result);
    mnDebug("save %s succ, rows:%d", (char *)param, rows);
  }

  taos_free_result(result);
}

void monitorSaveAcctLog(SAcctMonitorObj *pMon) {
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

  mnDebug("save account info, sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, montiorExecSqlCb, "account info");
}

void monitorSaveLog(int32_t level, const char *const format, ...) {
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

  mnDebug("save log, sql: %s", sql);
  taos_query_a(tsMonitor.conn, sql, montiorExecSqlCb, "log");
}

void monitorExecuteSQL(char *sql) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  mnDebug("execute sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, montiorExecSqlCb, "sql");
}
