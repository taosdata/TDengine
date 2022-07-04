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
#include "tfs.h"
#include "tlog.h"
#include "ttimer.h"
#include "tutil.h"
#include "tsclient.h"
#include "dnode.h"
#include "vnode.h"
#include "monitor.h"
#include "taoserror.h"

#define monFatal(...) { if (monDebugFlag & DEBUG_FATAL) { taosPrintLog("MON FATAL ", 255, __VA_ARGS__); }}
#define monError(...) { if (monDebugFlag & DEBUG_ERROR) { taosPrintLog("MON ERROR ", 255, __VA_ARGS__); }}
#define monWarn(...)  { if (monDebugFlag & DEBUG_WARN)  { taosPrintLog("MON WARN ", 255, __VA_ARGS__); }}
#define monInfo(...)  { if (monDebugFlag & DEBUG_INFO)  { taosPrintLog("MON ", 255, __VA_ARGS__); }}
#define monDebug(...) { if (monDebugFlag & DEBUG_DEBUG) { taosPrintLog("MON ", monDebugFlag, __VA_ARGS__); }}
#define monTrace(...) { if (monDebugFlag & DEBUG_TRACE) { taosPrintLog("MON ", monDebugFlag, __VA_ARGS__); }}

#define SQL_LENGTH          4096
#define LOG_LEN_STR         512
#define IP_LEN_STR          TSDB_EP_LEN
#define VGROUP_STATUS_LEN   512
#define DNODE_INFO_LEN      128
#define QUERY_ID_LEN        24
#define CHECK_INTERVAL      1000

#define SQL_STR_FMT "\"%s\""

static SMonHttpStatus monHttpStatusTable[] = {
  {"HTTP_CODE_CONTINUE",                100},
  {"HTTP_CODE_SWITCHING_PROTOCOL",      101},
  {"HTTP_CODE_PROCESSING",              102},
  {"HTTP_CODE_EARLY_HINTS",             103},
  {"HTTP_CODE_OK",                      200},
  {"HTTP_CODE_CREATED",                 201},
  {"HTTP_CODE_ACCEPTED",                202},
  {"HTTP_CODE_NON_AUTHORITATIVE_INFO",  203},
  {"HTTP_CODE_NO_CONTENT",              204},
  {"HTTP_CODE_RESET_CONTENT",           205},
  {"HTTP_CODE_PARTIAL_CONTENT",         206},
  {"HTTP_CODE_MULTI_STATUS",            207},
  {"HTTP_CODE_ALREADY_REPORTED",        208},
  {"HTTP_CODE_IM_USED",                 226},
  {"HTTP_CODE_MULTIPLE_CHOICE",         300},
  {"HTTP_CODE_MOVED_PERMANENTLY",       301},
  {"HTTP_CODE_FOUND",                   302},
  {"HTTP_CODE_SEE_OTHER",               303},
  {"HTTP_CODE_NOT_MODIFIED",            304},
  {"HTTP_CODE_USE_PROXY",               305},
  {"HTTP_CODE_UNUSED",                  306},
  {"HTTP_CODE_TEMPORARY_REDIRECT",      307},
  {"HTTP_CODE_PERMANENT_REDIRECT",      308},
  {"HTTP_CODE_BAD_REQUEST",             400},
  {"HTTP_CODE_UNAUTHORIZED",            401},
  {"HTTP_CODE_PAYMENT_REQUIRED",        402},
  {"HTTP_CODE_FORBIDDEN",               403},
  {"HTTP_CODE_NOT_FOUND",               404},
  {"HTTP_CODE_METHOD_NOT_ALLOWED",      405},
  {"HTTP_CODE_NOT_ACCEPTABLE",          406},
  {"HTTP_CODE_PROXY_AUTH_REQUIRED",     407},
  {"HTTP_CODE_REQUEST_TIMEOUT",         408},
  {"HTTP_CODE_CONFLICT",                409},
  {"HTTP_CODE_GONE",                    410},
  {"HTTP_CODE_LENGTH_REQUIRED",         411},
  {"HTTP_CODE_PRECONDITION_FAILED",     412},
  {"HTTP_CODE_PAYLOAD_TOO_LARGE",       413},
  {"HTTP_CODE_URI_TOO_LARGE",           414},
  {"HTTP_CODE_UNSUPPORTED_MEDIA_TYPE",  415},
  {"HTTP_CODE_RANGE_NOT_SATISFIABLE",   416},
  {"HTTP_CODE_EXPECTATION_FAILED",      417},
  {"HTTP_CODE_IM_A_TEAPOT",             418},
  {"HTTP_CODE_MISDIRECTED_REQUEST",     421},
  {"HTTP_CODE_UNPROCESSABLE_ENTITY",    422},
  {"HTTP_CODE_LOCKED",                  423},
  {"HTTP_CODE_FAILED_DEPENDENCY",       424},
  {"HTTP_CODE_TOO_EARLY",               425},
  {"HTTP_CODE_UPGRADE_REQUIRED",        426},
  {"HTTP_CODE_PRECONDITION_REQUIRED",   428},
  {"HTTP_CODE_TOO_MANY_REQUESTS",       429},
  {"HTTP_CODE_REQ_HDR_FIELDS_TOO_LARGE",431},
  {"HTTP_CODE_UNAVAIL_4_LEGAL_REASONS", 451},
  {"HTTP_CODE_INTERNAL_SERVER_ERROR",   500},
  {"HTTP_CODE_NOT_IMPLEMENTED",         501},
  {"HTTP_CODE_BAD_GATEWAY",             502},
  {"HTTP_CODE_SERVICE_UNAVAILABLE",     503},
  {"HTTP_CODE_GATEWAY_TIMEOUT",         504},
  {"HTTP_CODE_HTTP_VER_NOT_SUPPORTED",  505},
  {"HTTP_CODE_VARIANT_ALSO_NEGOTIATES", 506},
  {"HTTP_CODE_INSUFFICIENT_STORAGE",    507},
  {"HTTP_CODE_LOOP_DETECTED",           508},
  {"HTTP_CODE_NOT_EXTENDED",            510},
  {"HTTP_CODE_NETWORK_AUTH_REQUIRED",   511}
};

typedef enum {
  MON_CMD_CREATE_DB,
  MON_CMD_CREATE_TB_LOG,
  MON_CMD_CREATE_MT_DN,
  MON_CMD_CREATE_MT_ACCT,
  MON_CMD_CREATE_TB_DN,
  MON_CMD_CREATE_TB_ACCT_ROOT,
  MON_CMD_CREATE_TB_SLOWQUERY,
  //followings are extension for taoskeeper
  MON_CMD_CREATE_TB_CLUSTER,
  MON_CMD_CREATE_MT_DNODES,
  MON_CMD_CREATE_TB_DNODE,
  MON_CMD_CREATE_MT_DISKS,
  MON_CMD_CREATE_TB_DISKS,
  MON_CMD_CREATE_MT_VGROUPS,
  MON_CMD_CREATE_MT_LOGS,
  MON_CMD_CREATE_TB_DNODE_LOG,
  MON_CMD_CREATE_TB_GRANTS,
  MON_CMD_CREATE_MT_RESTFUL,
  MON_CMD_CREATE_TB_RESTFUL,
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

typedef struct {
  SDnodeStatisInfo dInfo;
  SVnodeStatisInfo vInfo;
  float io_read;
  float io_write;
  float io_read_disk;
  float io_write_disk;
  int32_t monQueryReqCnt;
  int32_t monSubmitReqCnt;
} SMonStat;

static void *monHttpStatusHashTable;

static SMonConn tsMonitor = {0};
static SMonStat tsMonStat = {{0}};
static int32_t  monQueryReqNum = 0, monSubmitReqNum = 0;
static bool     monHasMnodeMaster = false;

static void  monSaveSystemInfo();
static void  monSaveClusterInfo();
static void  monSaveDnodesInfo();
static void  monSaveVgroupsInfo();
static void  monSaveDisksInfo();
static void  monSaveGrantsInfo();
static void  monSaveHttpReqInfo();
static void  monGetSysStats();
static void *monThreadFunc(void *param);
static void  monBuildMonitorSql(char *sql, int32_t cmd);
static void monInitHttpStatusHashTable();
static void monCleanupHttpStatusHashTable();

extern int32_t (*monStartSystemFp)();
extern void    (*monStopSystemFp)();
extern void    (*monExecuteSQLFp)(char *sql);
extern char *  strptime(const char *buf, const char *fmt, struct tm *tm); //make the compilation pass

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
  monInitHttpStatusHashTable();

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
  if (taos_init()) {
    return -1;
  }
  tsMonitor.start = 1;
  monExecuteSQLFp = monExecuteSQL;
  monInfo("monitor module start");
  return 0;
}

static void monInitHttpStatusHashTable() {
  int32_t numOfEntries = tListLen(monHttpStatusTable);
  monHttpStatusHashTable = taosHashInit(numOfEntries, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  for (int32_t i = 0; i < numOfEntries; ++i) {
    monHttpStatusTable[i].index = i;
    SMonHttpStatus* pEntry = &monHttpStatusTable[i];
    taosHashPut(monHttpStatusHashTable, &monHttpStatusTable[i].code, sizeof(int32_t),
                                        &pEntry, POINTER_BYTES);
  }
}

static void monCleanupHttpStatusHashTable() {
  void* m = monHttpStatusHashTable;
  if (m != NULL && atomic_val_compare_exchange_ptr(&monHttpStatusHashTable, m, 0) == m) {
    taosHashCleanup(m);
  }
}

SMonHttpStatus *monGetHttpStatusHashTableEntry(int32_t code) {
  if (monHttpStatusHashTable == NULL) {
    return NULL;
  }
  return (SMonHttpStatus*)taosHashGet(monHttpStatusHashTable, &code, sizeof(int32_t));
}

static void *monThreadFunc(void *param) {
  monDebug("starting to initialize monitor module ...");
  setThreadName("monitor");

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
        monGetSysStats();
        //monSaveDnodesInfo has to be the first, as it calculates
        //stats using monSubmitReqNum before any insertion from monitor
        monSaveDnodesInfo();
        if (monHasMnodeMaster) {
          //only mnode master will write cluster info
          monSaveClusterInfo();
        }
        monSaveVgroupsInfo();
        monSaveDisksInfo();
        monSaveGrantsInfo();
        monSaveHttpReqInfo();
        monSaveSystemInfo();
      }
    }
  }

  monInfo("monitor thread is stopped");
  return NULL;
}

static void monBuildMonitorSql(char *sql, int32_t cmd) {
  memset(sql, 0, SQL_LENGTH);

#ifdef _STORAGE
  char *keepValue = "30,30,30";
#else
  char *keepValue = "30";
#endif

  if (cmd == MON_CMD_CREATE_DB) {
    snprintf(sql, SQL_LENGTH,
             "create database if not exists %s replica %d days 10 keep %s cache %d "
             "blocks %d precision 'us'",
             tsMonitorDbName, tsMonitorReplica, keepValue, TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MIN_TOTAL_BLOCKS);
  } else if (cmd == MON_CMD_CREATE_MT_DN) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.dn(ts timestamp"
             ", cpu_taosd float, cpu_system float, cpu_cores int"
             ", mem_taosd float, mem_system float, mem_total int"
             ", disk_used float, disk_total int"
             ", band_speed float"
             ", io_read float, io_write float"
             ", req_http bigint, req_select bigint, req_insert bigint"
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
  } else if (cmd == MON_CMD_CREATE_TB_CLUSTER) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.cluster_info(ts timestamp"
             ", first_ep binary(%d), version binary(%d)"
             ", master_uptime float, monitor_interval int"
             ", dnodes_total int, dnodes_alive int"
             ", mnodes_total int, mnodes_alive int"
             ", vgroups_total int, vgroups_alive int"
             ", vnodes_total int, vnodes_alive int"
             ", connections_total int)",
             tsMonitorDbName, TSDB_EP_LEN, TSDB_VERSION_LEN);
  } else if (cmd == MON_CMD_CREATE_MT_DNODES) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.dnodes_info(ts timestamp"
             ", uptime float"
             ", cpu_engine float, cpu_system float, cpu_cores int"
             ", mem_engine float, mem_system float, mem_total float"
             ", disk_engine float, disk_used float, disk_total float"
             ", net_in float, net_out float"
             ", io_read float, io_write float"
             ", io_read_disk float, io_write_disk float"
             ", req_http bigint, req_http_rate float"
             ", req_select bigint, req_select_rate float"
             ", req_insert bigint, req_insert_success bigint, req_insert_rate float"
             ", req_insert_batch bigint, req_insert_batch_success bigint, req_insert_batch_rate float"
             ", errors bigint"
             ", vnodes_num int"
             ", masters int"
             ", has_mnode bool"
             ") tags (dnode_id int, dnode_ep binary(%d))",
             tsMonitorDbName, TSDB_EP_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_DNODE) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.dnode_%d using %s.dnodes_info tags(%d, '%s')", tsMonitorDbName,
             dnodeGetDnodeId(), tsMonitorDbName, dnodeGetDnodeId(), tsLocalEp);
  } else if (cmd == MON_CMD_CREATE_MT_DISKS) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.disks_info(ts timestamp"
             ", datadir_l0_used float, datadir_l0_total float"
             ", datadir_l1_used float, datadir_l1_total float"
             ", datadir_l2_used float, datadir_l2_total float"
             ") tags (dnode_id int, dnode_ep binary(%d))",
             tsMonitorDbName, TSDB_EP_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_DISKS) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.disks_%d using %s.disks_info tags(%d, '%s')", tsMonitorDbName,
             dnodeGetDnodeId(), tsMonitorDbName, dnodeGetDnodeId(), tsLocalEp);
  } else if (cmd == MON_CMD_CREATE_MT_VGROUPS) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.vgroups_info(ts timestamp"
             ", database_name binary(%d)"
             ", tables_num int, status binary(%d)"
             ", online_vnodes tinyint"
             ", dnode_ids binary(%d), dnode_roles binary(%d)"
             ") tags (vgroup_id int)",
             tsMonitorDbName, TSDB_DB_NAME_LEN, VGROUP_STATUS_LEN,
             DNODE_INFO_LEN, DNODE_INFO_LEN);
  } else if (cmd == MON_CMD_CREATE_MT_LOGS) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.logs(ts timestamp, level tinyint, "
             "content binary(%d)) tags (dnode_id int, dnode_ep binary(%d))",
             tsMonitorDbName, LOG_LEN_STR, TSDB_EP_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_DNODE_LOG) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.dnode_%d_log using %s.logs tags(%d, '%s')", tsMonitorDbName,
             dnodeGetDnodeId(), tsMonitorDbName, dnodeGetDnodeId(), tsLocalEp);
  } else if (cmd == MON_CMD_CREATE_TB_GRANTS) {
    snprintf(sql, SQL_LENGTH,
             "create table if not exists %s.grants_info(ts timestamp"
             ", expire_time int, timeseries_used int, timeseries_total int)",
             tsMonitorDbName);
  } else if (cmd == MON_CMD_CREATE_MT_RESTFUL) {
    int usedLen = 0, len = 0;
    int pos = snprintf(sql, SQL_LENGTH,
                       "create table if not exists %s.restful_info(ts timestamp", tsMonitorDbName);
    usedLen += pos;
    for (int i = 0; i < tListLen(monHttpStatusTable); ++i) {
      len = snprintf(sql + pos, SQL_LENGTH - usedLen, ", %s_%d int",
                                monHttpStatusTable[i].name,
                                monHttpStatusTable[i].code);
      usedLen += len;
      pos += len;
    }
    snprintf(sql + pos, SQL_LENGTH - usedLen,
             ") tags (dnode_id int, dnode_ep binary(%d))",
             TSDB_EP_LEN);
  } else if (cmd == MON_CMD_CREATE_TB_RESTFUL) {
    snprintf(sql, SQL_LENGTH, "create table if not exists %s.restful_%d using %s.restful_info tags(%d, '%s')", tsMonitorDbName,
             dnodeGetDnodeId(), tsMonitorDbName, dnodeGetDnodeId(), tsLocalEp);
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
  if (taosCheckPthreadValid(tsMonitor.thread)) {
    pthread_join(tsMonitor.thread, NULL);
  }

  if (tsMonitor.conn != NULL) {
    taos_close(tsMonitor.conn);
    tsMonitor.conn = NULL;
  }
  monCleanupHttpStatusHashTable();
  monInfo("monitor module is cleaned up");
}

static void monGetSysStats() {
  memset(&tsMonStat, 0, sizeof(SMonStat));
  bool suc = taosGetProcIO(&tsMonStat.io_read, &tsMonStat.io_write,
                           &tsMonStat.io_read_disk, &tsMonStat.io_write_disk);
  if (!suc) {
    monDebug("failed to get io info");
  }

  tsMonStat.dInfo = dnodeGetStatisInfo();
  tsMonStat.vInfo = vnodeGetStatisInfo();

  tsMonStat.monQueryReqCnt = monFetchQueryReqCnt();
  tsMonStat.monSubmitReqCnt = monFetchSubmitReqCnt();
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

  return snprintf(sql, SQL_LENGTH, ", %f, %f, %d", procMemoryUsedMB, sysMemoryUsedMB, tsTotalMemoryMB);
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

  return snprintf(sql, SQL_LENGTH, ", %f, %f, %d", procCpuUsage, sysCpuUsage, tsNumOfCores);
}

// unit is GB
static int32_t monBuildDiskSql(char *sql) {
  return snprintf(sql, SQL_LENGTH, ", %f, %d", tsUsedDataDirGB, (int32_t)tsTotalDataDirGB);
}

// unit is Kb
static int32_t monBuildBandSql(char *sql) {
  float bandSpeedKb = 0;
  bool  suc = taosGetBandSpeed(&bandSpeedKb);
  if (!suc) {
    monDebug("failed to get bandwidth speed");
  }

  return snprintf(sql, SQL_LENGTH, ", %f", bandSpeedKb);
}

static int32_t monBuildReqSql(char *sql) {
  SDnodeStatisInfo info = tsMonStat.dInfo;
  return snprintf(sql, SQL_LENGTH, ", %"PRId64", %"PRId64", %"PRId64")", info.httpReqNum, info.queryReqNum, info.submitReqNum);
}

static int32_t monBuildIoSql(char *sql) {
  float readKB = 0, writeKB = 0;
  readKB = tsMonStat.io_read;
  writeKB = tsMonStat.io_write;

  return snprintf(sql, SQL_LENGTH, ", %f, %f", readKB, writeKB);
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
    monIncSubmitReqCnt();
    monDebug("successfully to save system info, sql:%s", tsMonitor.sql);
  }
}

static int32_t monGetRowElemCharLen(TAOS_FIELD field, char *rowElem) {
  if (field.type != TSDB_DATA_TYPE_BINARY && field.type != TSDB_DATA_TYPE_NCHAR) {
    return -1;
  }

  int32_t charLen = varDataLen(rowElem - VARSTR_HEADER_SIZE);
  if (field.type == TSDB_DATA_TYPE_BINARY) {
    assert(charLen <= field.bytes && charLen >= 0);
  } else {
    assert(charLen <= field.bytes * TSDB_NCHAR_SIZE && charLen >= 0);
  }

  return charLen;
}

static int32_t monBuildFirstEpSql(char *sql) {
  return snprintf(sql, SQL_LENGTH, ", "SQL_STR_FMT, tsFirst);
}

static int32_t monBuildVersionSql(char *sql) {
  return snprintf(sql, SQL_LENGTH, ", "SQL_STR_FMT, version);
}

static int32_t monBuildMasterUptimeSql(char *sql) {
  int64_t masterUptime = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show mnodes");

  TAOS_ROW row;
  int32_t  num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "role") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "leader", charLen) == 0) {
          if (strcmp(fields[i + 1].name, "role_time") == 0) {
            int64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);
            //master uptime in seconds
            masterUptime = (now - *(int64_t *)row[i + 1]) / 1000;
          }
        }
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %" PRId64, masterUptime);
}

static int32_t monBuildMonIntervalSql(char *sql) {
  return snprintf(sql, SQL_LENGTH, ", %d", tsMonitorInterval);
}

static int32_t monBuildDnodesTotalSql(char *sql) {
  int32_t totalDnodes = 0, totalDnodesAlive = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show dnodes");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show dnodes, reason:%s", tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    totalDnodes++;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "status") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "ready", charLen) == 0)  {
          totalDnodesAlive++;
        }
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %d, %d", totalDnodes, totalDnodesAlive);
}

static int32_t monBuildMnodesTotalSql(char *sql) {
  int32_t totalMnodes = 0, totalMnodesAlive= 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show mnodes");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show mnodes, reason:%s", tstrerror(code));
  }

  TAOS_ROW row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    totalMnodes++;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "role") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "leader", charLen) == 0 ||
            strncmp((char *)row[i], "follower", charLen) == 0)  {
          totalMnodesAlive += 1;
        }
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %d, %d", totalMnodes, totalMnodesAlive);
}


static int32_t monGetVgroupsTotalStats(char *dbName, int32_t *totalVgroups,
                                           int32_t *totalVgroupsAlive) {
  char subsql[TSDB_DB_NAME_LEN + 14];
  memset(subsql, 0, sizeof(subsql));
  snprintf(subsql, TSDB_DB_NAME_LEN + 13, "show %s.vgroups", dbName);
  TAOS_RES *result = taos_query(tsMonitor.conn, subsql);
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show %s.vgroups, reason:%s", dbName, tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    *totalVgroups += 1;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "status") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "ready", charLen) == 0)  {
          *totalVgroupsAlive += 1;
        }
      }
    }
  }
  taos_free_result(result);

  return 0;
}

static int32_t monBuildVgroupsTotalSql(char *sql) {
  int32_t totalVgroups = 0, totalVgroupsAlive = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show databases");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show databases, reason:%s", tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    for (int i = 0; i < num_fields; ++i) {
      //database name
      if (strcmp(fields[i].name, "name") == 0) {
        monGetVgroupsTotalStats((char *)row[i], &totalVgroups, &totalVgroupsAlive);
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %d, %d", totalVgroups, totalVgroupsAlive);
}

static int32_t monGetVnodesTotalStats(char *ep, int32_t *totalVnodes,
                                           int32_t *totalVnodesAlive) {
  char subsql[TSDB_EP_LEN + 15];
  memset(subsql, 0, sizeof(subsql));
  snprintf(subsql, TSDB_EP_LEN, "show vnodes "SQL_STR_FMT, ep);
  TAOS_RES *result = taos_query(tsMonitor.conn, subsql);
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show vnodes "SQL_STR_FMT", reason:%s", ep, tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    *totalVnodes += 1;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "status") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "leader", charLen) == 0 ||
            strncmp((char *)row[i], "follower", charLen) == 0)  {
          *totalVnodesAlive += 1;
        }
      }
    }
  }
  taos_free_result(result);

  return 0;
}

static int32_t monBuildVnodesTotalSql(char *sql) {
  int32_t totalVnodes = 0, totalVnodesAlive = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show dnodes");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show dnodes, reason:%s", tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    for (int i = 0; i < num_fields; ++i) {
      //database name
      if (strcmp(fields[i].name, "end_point") == 0) {
        monGetVnodesTotalStats((char *)row[i], &totalVnodes, &totalVnodesAlive);
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %d, %d", totalVnodes, totalVnodesAlive);
}

static int32_t monBuildConnsTotalSql(char *sql) {
  int32_t totalConns = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show connections");
  TAOS_ROW    row;

  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show connections, reason:%s", tstrerror(code));
  }

  while ((row = taos_fetch_row(result))) {
    totalConns++;
  }

  taos_free_result(result);
  return snprintf(sql, SQL_LENGTH, ", %d)", totalConns);
}

static int32_t monBuildDnodeUptimeSql(char *sql) {
  int64_t dnodeUptime = 0;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show dnodes");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show dnodes, reason:%s", tstrerror(code));
  }

  TAOS_ROW row;
  int32_t  num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  bool is_self_ep = false;
  while ((row = taos_fetch_row(result))) {
    if (is_self_ep) {
      break;
    }
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "end_point") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], tsLocalEp, charLen) == 0) {
          is_self_ep = true;
        }
      }
      if (strcmp(fields[i].name, "create_time") == 0) {
        if (is_self_ep) {
          int64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);
          //dnodes uptime in seconds
          dnodeUptime = (now - *(int64_t *)row[i]) / 1000;
        }
      }
    }
  }

  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %" PRId64, dnodeUptime);
}

static int32_t monBuildDnodeIoSql(char *sql) {
  float rcharKB = 0, wcharKB = 0;
  float rbyteKB = 0, wbyteKB = 0;
  rcharKB = tsMonStat.io_read;
  wcharKB = tsMonStat.io_write;
  rbyteKB = tsMonStat.io_read_disk;
  wbyteKB = tsMonStat.io_write_disk;

  return snprintf(sql, SQL_LENGTH, ", %f, %f, %f, %f", rcharKB / 1024, wcharKB / 1024,
                                                       rbyteKB / 1024, wbyteKB / 1024);
}

static int32_t monBuildNetworkIOSql(char *sql) {
  float netInKb = 0, netOutKb = 0;
  bool  suc = taosGetNetworkIO(&netInKb, &netOutKb);
  if (!suc) {
    monDebug("failed to get network I/O info");
  }

  return snprintf(sql, SQL_LENGTH,  ", %f, %f", netInKb / 1024,
                                                netOutKb / 1024);
}

static int32_t monBuildDnodeReqSql(char *sql) {
  int64_t queryReqNum = tsMonStat.dInfo.queryReqNum - tsMonStat.monQueryReqCnt;
  int64_t submitReqNum = tsMonStat.dInfo.submitReqNum;
  int64_t submitRowNum = tsMonStat.vInfo.submitRowNum;
  int64_t submitReqSucNum = tsMonStat.vInfo.submitReqSucNum;
  int64_t submitRowSucNum = tsMonStat.vInfo.submitRowSucNum;

  float interval = (float)(tsMonitorInterval * 1.0);
  float httpReqRate = tsMonStat.dInfo.httpReqNum / interval;
  float queryReqRate = queryReqNum / interval;
  float submitReqRate = submitReqNum / interval;
  float submitRowRate = submitRowNum / interval;

  return snprintf(sql, SQL_LENGTH, ", %"PRId64", %f, %"PRId64", %f, %"PRId64", %"PRId64", %f, %"PRId64", %"PRId64", %f",
                                                                               tsMonStat.dInfo.httpReqNum, httpReqRate,
                                                                               queryReqNum, queryReqRate,
                                                                               submitRowNum, submitRowSucNum, submitRowRate,
                                                                               submitReqNum, submitReqSucNum, submitReqRate);
}

static int32_t monBuildDnodeErrorsSql(char *sql) {
  int64_t dnode_err = dnodeGetDnodeError();
  return snprintf(sql, SQL_LENGTH, ", %"PRId64, dnode_err);
}

static int32_t monBuildDnodeVnodesSql(char *sql) {
  int32_t vnodeNum = 0, masterNum = 0;
  char sqlStr[TSDB_EP_LEN + 15];
  memset(sqlStr, 0, sizeof(sqlStr));
  snprintf(sqlStr, TSDB_EP_LEN + 14, "show vnodes "SQL_STR_FMT, tsLocalEp);
  TAOS_RES *result = taos_query(tsMonitor.conn, sqlStr);
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show vnodes "SQL_STR_FMT", reason:%s", tsLocalEp, tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    vnodeNum += 1;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "status") == 0) {
        int32_t charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "leader", charLen) == 0)  {
          masterNum += 1;
        }
      }
    }
  }
  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %d, %d", vnodeNum, masterNum);
}

static int32_t monBuildDnodeMnodeSql(char *sql) {
  bool has_mnode = false, has_mnode_row;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show mnodes");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show mnodes, reason:%s", tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  int32_t charLen;
  while ((row = taos_fetch_row(result))) {
    has_mnode_row = false;
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "end_point") == 0) {
        charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], tsLocalEp, charLen) == 0)  {
          has_mnode = true;
          has_mnode_row = true;
        }
      } else if (strcmp(fields[i].name, "role") == 0) {
        charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (strncmp((char *)row[i], "leader", charLen) == 0)  {
          if (has_mnode_row) {
            monHasMnodeMaster = true;
          }
        }
      }
    }
  }
  taos_free_result(result);

  return snprintf(sql, SQL_LENGTH, ", %s)", has_mnode ? "true" : "false");
}

static int32_t monBuildDnodeDiskSql(char *sql) {
  float taosdDataDirGB = 0;
  return snprintf(sql, SQL_LENGTH, ", %f, %f, %f", taosdDataDirGB, tsUsedDataDirGB, tsTotalDataDirGB);
}

static int32_t monBuildDiskTierSql(char *sql) {
  const int8_t numTiers = 3;
  const double unit = 1024 * 1024 * 1024;
  SFSMeta      fsMeta;
  STierMeta* tierMetas = calloc(numTiers, sizeof(STierMeta));
  tfsUpdateInfo(&fsMeta, tierMetas, numTiers);
  int32_t pos = 0;

  for (int i = 0; i < numTiers; ++i) {
    pos += snprintf(sql + pos, SQL_LENGTH, ", %f, %f", (float)(tierMetas[i].used / unit), (float)(tierMetas[i].size / unit));
  }
  pos += snprintf(sql + pos, SQL_LENGTH, ")");

  free(tierMetas);

  return pos;
}

static void monSaveClusterInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.cluster_info values(%" PRId64, tsMonitorDbName, ts);

  pos += monBuildFirstEpSql(sql + pos);
  pos += monBuildVersionSql(sql + pos);
  pos += monBuildMasterUptimeSql(sql + pos);
  pos += monBuildMonIntervalSql(sql + pos);
  pos += monBuildDnodesTotalSql(sql + pos);
  pos += monBuildMnodesTotalSql(sql + pos);
  pos += monBuildVgroupsTotalSql(sql + pos);
  pos += monBuildVnodesTotalSql(sql + pos);
  pos += monBuildConnsTotalSql(sql + pos);

  monDebug("save cluster, sql:%s", sql);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int32_t code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save cluster info, reason:%s, sql:%s", tstrerror(code), tsMonitor.sql);
  } else {
    monIncSubmitReqCnt();
    monDebug("successfully to save cluster info, sql:%s", tsMonitor.sql);
  }
}

static void monSaveDnodesInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int64_t intervalUs = tsMonitorInterval * 1000000;
  ts = ts / intervalUs * intervalUs; //To align timestamp to integer multiples of monitor interval
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.dnode_%d values(%" PRId64, tsMonitorDbName, dnodeGetDnodeId(), ts);

  pos += monBuildDnodeUptimeSql(sql + pos);
  pos += monBuildCpuSql(sql + pos);
  pos += monBuildMemorySql(sql + pos);
  pos += monBuildDnodeDiskSql(sql + pos);
  pos += monBuildNetworkIOSql(sql + pos);
  pos += monBuildDnodeIoSql(sql + pos);
  pos += monBuildDnodeReqSql(sql + pos);
  pos += monBuildDnodeErrorsSql(sql + pos);
  pos += monBuildDnodeVnodesSql(sql + pos);
  pos += monBuildDnodeMnodeSql(sql + pos);

  monDebug("save dnodes, sql:%s", sql);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int32_t code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save dnode_%d info, reason:%s, sql:%s", dnodeGetDnodeId(), tstrerror(code), tsMonitor.sql);
  } else {
    monIncSubmitReqCnt();
    monDebug("successfully to save dnode_%d info, sql:%s", dnodeGetDnodeId(), tsMonitor.sql);
  }
}


static int32_t checkCreateVgroupTable(int32_t vgId) {
  char subsql[256];
  int32_t code = TSDB_CODE_SUCCESS;

  memset(subsql, 0, sizeof(subsql));
  snprintf(subsql, sizeof(subsql), "create table if not exists %s.vgroup_%d using %s.vgroups_info tags(%d)",
            tsMonitorDbName, vgId, tsMonitorDbName, vgId);

  TAOS_RES *result = taos_query(tsMonitor.conn, subsql);
  code = taos_errno(result);
  taos_free_result(result);

  return code;
}

static uint32_t monBuildVgroupsInfoSql(char *sql, char *dbName) {
  char v_dnode_ids[256] = {0}, v_dnode_status[1024] = {0};
  int64_t ts = taosGetTimestampUs();

  memset(sql, 0, SQL_LENGTH + 1);
  snprintf(sql, SQL_LENGTH, "show %s.vgroups", dbName);
  TAOS_RES *result = taos_query(tsMonitor.conn, sql);
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show %s.vgroups, reason:%s", dbName, tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  int32_t charLen;
  while ((row = taos_fetch_row(result))) {
    int32_t vgId;
    int32_t pos = 0;

    for (int i = 0; i < num_fields; ++i) {
      const char *v_dnode_str = strchr(fields[i].name, '_');
      if (strcmp(fields[i].name, "vgId") == 0) {
        vgId = *(int32_t *)row[i];
        if (checkCreateVgroupTable(vgId) == TSDB_CODE_SUCCESS) {
          memset(sql, 0, SQL_LENGTH + 1);
          pos += snprintf(sql, SQL_LENGTH, "insert into %s.vgroup_%d values(%" PRId64 ", "SQL_STR_FMT,
                   tsMonitorDbName, vgId, ts, dbName);
        } else {
          goto DONE;
        }
      } else if (strcmp(fields[i].name, "tables") == 0) {
        pos += snprintf(sql + pos, SQL_LENGTH, ", %d", *(int32_t *)row[i]);
      } else if (strcmp(fields[i].name, "status") == 0) {
        charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (charLen < 0) {
          monError("failed to save vgroup_%d info, reason: invalid row %s len, sql:%s", vgId, (char *)row[i], tsMonitor.sql);
          goto DONE;
        }
        char tmpBuf[10] = {0};
        memcpy(tmpBuf, row[i], charLen);
        pos += snprintf(sql + pos, strlen(SQL_STR_FMT) + charLen + 1, ", "SQL_STR_FMT, tmpBuf);
      } else if (strcmp(fields[i].name, "onlines") == 0) {
        pos += snprintf(sql + pos, SQL_LENGTH, ", %d", *(int32_t *)row[i]);
      } else if (v_dnode_str && strcmp(v_dnode_str, "_dnode") == 0) {
        snprintf(v_dnode_ids, sizeof(v_dnode_ids), "%d;", *(int16_t *)row[i]);
      } else if (v_dnode_str && strcmp(v_dnode_str, "_status") == 0) {
        charLen = monGetRowElemCharLen(fields[i], (char *)row[i]);
        if (charLen < 0) {
          monError("failed to save vgroup_%d info, reason: invalid row %s len, sql:%s", vgId, (char *)row[i], tsMonitor.sql);
          goto DONE;
        }
        snprintf(v_dnode_status, charLen + 1, "%s;", (char *)row[i]);
      } else if (strcmp(fields[i].name, "compacting") == 0) {
        //flush dnode_ids and dnode_role in to sql
        pos += snprintf(sql + pos, SQL_LENGTH, ", "SQL_STR_FMT", "SQL_STR_FMT")", v_dnode_ids, v_dnode_status);
      }
    }
    monDebug("save vgroups, sql:%s", sql);
    TAOS_RES *res = taos_query(tsMonitor.conn, sql);
    code = taos_errno(res);
    taos_free_result(res);
    if (code != 0) {
      monError("failed to save vgroup_%d info, reason:%s, sql:%s", vgId, tstrerror(code), tsMonitor.sql);
    } else {
      monIncSubmitReqCnt();
      monDebug("successfully to save vgroup_%d info, sql:%s", vgId, tsMonitor.sql);
    }
  }

DONE:
  taos_free_result(result);
  return TSDB_CODE_SUCCESS;
}

static void monSaveVgroupsInfo() {
  char *  sql = tsMonitor.sql;
  TAOS_RES *result = taos_query(tsMonitor.conn, "show databases");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    monError("failed to execute cmd: show databases, reason:%s", tstrerror(code));
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    for (int i = 0; i < num_fields; ++i) {
      //database name
      if (strcmp(fields[i].name, "name") == 0) {
        monBuildVgroupsInfoSql(sql, (char *)row[i]);
      }
    }
  }

  taos_free_result(result);
}

static void monSaveDisksInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.disks_%d values(%" PRId64, tsMonitorDbName, dnodeGetDnodeId(), ts);

  monBuildDiskTierSql(sql + pos);

  monDebug("save disk, sql:%s", sql);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int32_t code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save disks_%d info, reason:%s, sql:%s", dnodeGetDnodeId(), tstrerror(code), tsMonitor.sql);
  } else {
    monIncSubmitReqCnt();
    monDebug("successfully to save disks_%d info, sql:%s", dnodeGetDnodeId(), tsMonitor.sql);
  }
}

static void monSaveGrantsInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.grants_info values(%" PRId64, tsMonitorDbName, ts);

  TAOS_RES *result = taos_query(tsMonitor.conn, "show grants");
  int32_t code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    taos_free_result(result);
    return;
  }

  TAOS_ROW    row;
  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result))) {
    for (int i = 0; i < num_fields; ++i) {
      if (strcmp(fields[i].name, "expire time") == 0) {
        char *expStr = (char *)row[i];
        if (expStr[0] == 'u') {
          pos += snprintf(sql + pos, SQL_LENGTH, ", NULL");
        } else {
          struct tm expTime = {0};
          strptime((char *)row[i], "%Y-%m-%d %H:%M:%S", &expTime);
          int32_t expTimeSec = (int32_t)mktime(&expTime);
          pos += snprintf(sql + pos, SQL_LENGTH, ", %d", expTimeSec - taosGetTimestampSec());
        }
      } else if (strcmp(fields[i].name, "timeseries") == 0) {
        char *timeseries = (char *)row[i];
        if (timeseries[0] == 'u') {
          pos += snprintf(sql + pos, SQL_LENGTH, ", NULL, NULL)");
        } else {
          int32_t timeseries_used = strtol(timeseries, NULL, 10);
          timeseries = strchr(timeseries, '/');
          int32_t timeseries_total = strtol(timeseries + 1, NULL, 10);
          pos += snprintf(sql + pos, SQL_LENGTH, ", %d, %d)", timeseries_used, timeseries_total);
        }
      }
    }
  }

  monDebug("save grants, sql:%s", sql);
  taos_free_result(result);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save grants info, reason:%s, sql:%s", tstrerror(code), tsMonitor.sql);
  } else {
    monIncSubmitReqCnt();
    monDebug("successfully to save grants info, sql:%s", tsMonitor.sql);
  }

}

static void monSaveHttpReqInfo() {
  int64_t ts = taosGetTimestampUs();
  char *  sql = tsMonitor.sql;
  int32_t pos = snprintf(sql, SQL_LENGTH, "insert into %s.restful_%d values(%" PRId64, tsMonitorDbName, dnodeGetDnodeId(), ts);

  for (int32_t i = 0; i < tListLen(monHttpStatusTable); ++i) {
    int32_t status = dnodeGetHttpStatusInfo(i);
    pos += snprintf(sql + pos, SQL_LENGTH, ", %d", status);
  }
  pos += snprintf(sql + pos, SQL_LENGTH, ")");
  dnodeClearHttpStatusInfo();

  monDebug("save http req, sql:%s", sql);

  void *res = taos_query(tsMonitor.conn, tsMonitor.sql);
  int32_t code = taos_errno(res);
  taos_free_result(res);

  if (code != 0) {
    monError("failed to save restful_%d info, reason:%s, sql:%s", dnodeGetDnodeId(), tstrerror(code), tsMonitor.sql);
  } else {
    monIncSubmitReqCnt();
    monDebug("successfully to save restful_%d info, sql:%s", dnodeGetDnodeId(), tsMonitor.sql);
  }
}

static void monExecSqlCb(void *param, TAOS_RES *result, int32_t code) {
  int32_t c = taos_errno(result);
  if (c != TSDB_CODE_SUCCESS) {
    monError("save %s failed, reason:%s", (char *)param, tstrerror(c));
  } else {
    monIncSubmitReqCnt();
    int32_t rows = taos_affected_rows(result);
    monDebug("save %s succ, rows:%d", (char *)param, rows);
  }

  taos_free_result(result);
}

void monSaveAcctLog(SAcctMonitorObj *pMon) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  char sql[1024] = {0};
  snprintf(sql, 1023,
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

  len += snprintf(sql + len, SQL_LENGTH, "', '%s')", tsLocalEp);
  sql[len++] = 0;

  monDebug("save log, sql: %s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "log");
}

void monSaveDnodeLog(int32_t level, const char *const format, ...) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  va_list argpointer;
  char    sql[SQL_LENGTH] = {0};
  int32_t max_length = SQL_LENGTH - 30;
  int32_t len = snprintf(sql, (size_t)max_length, "insert into %s.dnode_%d_log values(%" PRId64 ", %d,'", tsMonitorDbName,
                         dnodeGetDnodeId(), taosGetTimestampUs(), level);

  va_start(argpointer, format);
  len += vsnprintf(sql + len, (size_t)(max_length - len), format, argpointer);
  va_end(argpointer);
  if (len > max_length) len = max_length;

  len += snprintf(sql + len, SQL_LENGTH, "')");
  sql[len++] = 0;

  monDebug("save dnode log, sql: %s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "log");
}

void monExecuteSQL(char *sql) {
  if (tsMonitor.state != MON_STATE_INITED) return;

  monDebug("execute sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, monExecSqlCb, "sql");
}

void monExecuteSQLWithResultCallback(char *sql, MonExecuteSQLCbFP callback, void* param) {
  if (tsMonitor.conn == NULL) {
    callback(param, NULL, TSDB_CODE_MON_CONNECTION_INVALID);
    return;
  }

  monDebug("execute sql:%s", sql);
  taos_query_a(tsMonitor.conn, sql, callback, param);
}

void monIncQueryReqCnt() {
  atomic_fetch_add_32(&monQueryReqNum, 1);
}

void monIncSubmitReqCnt() {
  atomic_fetch_add_32(&monSubmitReqNum, 1);
}

int32_t monFetchQueryReqCnt() {
  return atomic_exchange_32(&monQueryReqNum, 0);
}

int32_t monFetchSubmitReqCnt() {
  return atomic_exchange_32(&monSubmitReqNum, 0);
}
