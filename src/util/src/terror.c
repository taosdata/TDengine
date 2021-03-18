/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define TAOS_ERROR_C
 
typedef struct {
    int32_t val;
    const char* str;
} STaosError;

#include "os.h"
#include "taoserror.h"

static threadlocal int32_t tsErrno;
int32_t* taosGetErrno() {
  return &tsErrno;
}

#ifdef TAOS_ERROR_C
#define TAOS_DEFINE_ERROR(name, msg) {.val = (name), .str=(msg)},
#else
#define TAOS_DEFINE_ERROR(name, mod, code, msg) static const int32_t name = TAOS_DEF_ERROR_CODE(mod, code);
#endif

#define TAOS_SYSTEM_ERROR(code)             (0x80ff0000 | (code))
#define TAOS_SUCCEEDED(err)                 ((err) >= 0)
#define TAOS_FAILED(err)                    ((err) < 0)

#ifdef TAOS_ERROR_C
STaosError errors[] = {
    {.val = 0, .str = "success"},
#endif

// rpc
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ACTION_IN_PROGRESS,       "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_REQUIRED,            "Authentication required")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_FAILURE,             "Authentication failure")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_REDIRECT,                 "Redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NOT_READY,                "System not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ALREADY_PROCESSED,        "Message already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED, "Last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MISMATCHED_LINK_ID,       "Mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_TOO_SLOW,                 "Processing of request timed out")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MAX_SESSIONS,             "Number of sessions reached limit")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NETWORK_UNAVAIL,          "Unable to establish connection")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_APP_ERROR,                "Unexpected generic error in RPC")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_UNEXPECTED_RESPONSE,      "Unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_VALUE,            "Invalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TRAN_ID,          "Invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_SESSION_ID,       "Invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_MSG_TYPE,         "Invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_RESPONSE_TYPE,    "Invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TIME_STAMP,       "Client and server's time is not synchronized")
TAOS_DEFINE_ERROR(TSDB_CODE_APP_NOT_READY,                "Database not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_FQDN_ERROR,               "Unable to resolve FQDN")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_VERSION,          "Invalid app version")

//common & util
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OPS_NOT_SUPPORT,          "Operation not supported")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_MEMORY_CORRUPTED,         "Memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OUT_OF_MEMORY,            "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_INVALID_CFG_MSG,          "Invalid config message")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_FILE_CORRUPTED,           "Data file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_NO_MEMORY,                "Ref out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_FULL,                     "too many Ref Objs")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ID_REMOVED,               "Ref ID is removed")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_INVALID_ID,               "Invalid Ref ID")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ALREADY_EXIST,            "Ref is already there")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_NOT_EXIST,                "Ref is not there")

//client
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_SQL,              "Invalid SQL statement")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_QHANDLE,          "Invalid qhandle")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TIME_STAMP,       "Invalid combination of client/service time")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VALUE,            "Invalid value in client")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VERSION,          "Invalid client version")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_IE,               "Invalid client ie")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_FQDN,             "Invalid host name")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_USER_LENGTH,      "Invalid user name")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_PASS_LENGTH,      "Invalid password")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_DB_LENGTH,        "Database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH,  "Table name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_CONNECTION,       "Invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_OUT_OF_MEMORY,            "System out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_DISKSPACE,             "System out of disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CACHE_ERASED,       "Query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CANCELLED,          "Query terminated")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_SORTED_RES_TOO_MANY,      "Result set too large to be sorted")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_APP_ERROR,                "Application error")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_ACTION_IN_PROGRESS,       "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DISCONNECTED,             "Disconnected from service")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_WRITE_AUTH,            "No write permission")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_CONN_KILLED,              "Connection killed")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_SQL_SYNTAX_ERROR,         "Syntax error in SQL")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DB_NOT_SELECTED,          "Database not specified or available")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TABLE_NAME,       "Table does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_EXCEED_SQL_LIMIT,         "SQL statement too long, check maxSQLLength config")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_FILE_EMPTY,               "File is empty")

// mnode
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MSG_NOT_PROCESSED,        "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_IN_PROGRESS,       "Message is progressing")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_NEED_REPROCESSED,  "Message need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_RIGHTS,                "Insufficient privilege for operation")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_APP_ERROR,                "Unexpected generic error in mnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONNECTION,       "Invalid message connection")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_VERSION,      "Incompatible protocol version")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_LEN,          "Invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_TYPE,         "Invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_SHELL_CONNS,     "Too many connections")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_OUT_OF_MEMORY,            "Out of memory in mnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_SHOWOBJ,          "Data expired")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_QUERY_ID,         "Invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STREAM_ID,        "Invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONN_ID,          "Invalid connection id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MNODE_IS_RUNNING,         "mnode is alreay running")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_CONFIG_SYNC,    "failed to config sync")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_START_SYNC,     "failed to start sync")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_CREATE_DIR,     "failed to create mnode dir")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_INIT_STEP,      "failed to init components")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE,    "Object already there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_ERROR,                "Unexpected generic error in sdb")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE,   "Invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_OBJ_NOT_THERE,        "Object not there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVAID_META_ROW,      "Invalid meta row")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVAID_KEY_TYPE,      "Invalid key type")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ALREADY_EXIST,      "DNode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_EXIST,          "DNode does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_EXIST,         "VGroup does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_REMOVE_MASTER,         "Master DNode cannot be removed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_ENOUGH_DNODES,         "Out of DNodes")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT, "Cluster cfg inconsistent")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION, "Invalid dnode cfg option")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_BALANCE_ENABLED,          "Balance already enabled")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_IN_DNODE,      "Vgroup not in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE,  "Vgroup already in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_FREE,           "Dnode not avaliable")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CLUSTER_ID,       "Cluster id not match")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_READY,                "Cluster not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED,  "Dnode Id not configured")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED,  "Dnode Ep not configured")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_ALREADY_EXIST,       "Account already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT,             "Invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT_OPTION,      "Invalid account options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_EXPIRED,             "Account authorization has expired")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_USER_ALREADY_EXIST,       "User already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER,             "Invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER_FORMAT,      "Invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_PASS_FORMAT,      "Invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_USER_FROM_CONN,        "Can not get user from conn")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_USERS,           "Too many users")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_TABLE_ALREADY_EXIST,      "Table already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_ID,         "Table name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_NAME,       "Table does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_TYPE,       "Invalid table type in tsdb")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TAGS,            "Too many tags")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TIMESERIES,      "Too many time series")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_SUPER_TABLE,          "Not super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_MND_COL_NAME_TOO_LONG,        "Tag name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_ALREAY_EXIST,         "Tag already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_NOT_EXIST,            "Tag does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_ALREAY_EXIST,       "Field already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_NOT_EXIST,          "Field does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STABLE_NAME,      "Super table does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CREATE_TABLE_MSG, "Invalid create table message")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_NAME,        "Invalid func name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_LEN,         "Invalid func length")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_CODE,        "Invalid func code")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FUNC_ALREADY_EXIST,       "Func already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC,             "Invalid func")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_NOT_SELECTED,          "Database not specified or available")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_ALREADY_EXIST,         "Database already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION,        "Invalid database options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB,               "Invalid database name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MONITOR_DB_FORBIDDEN,     "Cannot delete monitor database")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_DATABASES,       "Too many databases for account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_IN_DROPPING,           "Database not available")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_READY,         "Database unsynced")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION_DAYS,   "Invalid database option: days out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION_KEEP,   "Invalid database option: keep >= keep1 >= keep0 >= days")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TOPIC,            "Invalid topic name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TOPIC_OPTION,     "Invalid topic option")

// dnode
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MSG_NOT_PROCESSED,        "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_OUT_OF_MEMORY,            "Dnode out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_NO_WRITE_ACCESS,          "No permission for disk files in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_INVALID_MSG_LEN,          "Invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_ACTION_IN_PROGRESS,       "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_TOO_MANY_VNODES,          "Too many vnode directories")

// vnode
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_IN_PROGRESS,       "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_MSG_NOT_PROCESSED,        "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_NEED_REPROCESSED,  "Action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_VGROUP_ID,        "Invalid Vgroup ID")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INIT_FAILED,              "Vnode initialization failed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISKSPACE,             "System out of disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISK_PERMISSIONS,      "No write permission for disk files")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR,      "Missing data file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_OUT_OF_MEMORY,            "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_APP_ERROR,                "Unexpected generic error in vnode")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_VRESION_FILE,     "Invalid version file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_FULL,                  "Database memory is full for commit failed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_FLOWCTRL,              "Database memory is full for waiting commit")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_DROPPING,              "Database is dropping")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_BALANCING,             "Database is balancing")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NOT_SYNCED,               "Database suspended")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_WRITE_AUTH,            "Database write operation denied")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_SYNCING,               "Database is syncing")

// tsdb
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_ID,         "Invalid table ID")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_TYPE,       "Invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION,    "Invalid table schema version")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_ALREADY_EXIST,      "Table already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_CONFIG,           "Invalid configuration")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INIT_FAILED,              "Tsdb init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISKSPACE,             "No diskspace for tsdb")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISK_PERMISSIONS,      "No permission for disk files")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_FILE_CORRUPTED,           "Data file(s) corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_OUT_OF_MEMORY,            "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE,      "Tag too old")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE,   "Timestamp data out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP,      "Submit message is messed up")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_ACTION,           "Invalid operation")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_CREATE_TB_MSG,    "Invalid creation of table")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_TABLE_DATA_IN_MEM,     "No table data in memory skiplist")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_FILE_ALREADY_EXISTS,      "File already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_RECONFIGURE,        "Need to reconfigure table")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO,    "Invalid information to create table")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_AVAIL_DISK,            "No available disk")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_MESSED_MSG,               "TSDB messed message")

// query
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_QHANDLE,          "Invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_MSG,              "Invalid message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NO_DISKSPACE,             "No diskspace for query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_OUT_OF_MEMORY,            "System out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_APP_ERROR,                "Unexpected generic error in query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_DUP_JOIN_KEY,             "Duplicated join key")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_EXCEED_TAGS_LIMIT,        "Tag conditon too many")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NOT_READY,                "Query not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_HAS_RSP,                  "Query should response")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_IN_EXEC,                  "Multiple retrieval of this query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW,      "Too many time window in query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NOT_ENOUGH_BUFFER,        "Query buffer limit has reached")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INCONSISTAN,              "File inconsistance in replica")


// grant
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,                "License expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,          "DNode creation limited by licence")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,           "Account creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,     "Table creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,             "DB creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,           "User creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,           "Conn creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,         "Stream creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,          "Write speed limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,        "Storage capacity limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,      "Query time limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,            "CPU cores limited by license")

// sync
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_CONFIG,           "Invalid Sync Configuration")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_NOT_ENABLED,              "Sync module not enabled")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_VERSION,          "Invalid Sync version")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_CONFIRM_EXPIRED,          "Sync confirm expired")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_TOO_MANY_FWDINFO,         "Too many sync fwd infos")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_MISMATCHED_PROTOCOL,      "Mismatched protocol")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_MISMATCHED_CLUSTERID,     "Mismatched clusterId")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_MISMATCHED_SIGNATURE,     "Mismatched signature")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_CHECKSUM,         "Invalid msg checksum")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_MSGLEN,           "Invalid msg length")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_MSGTYPE,          "Invalid msg type")

// wal
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_APP_ERROR,                "Unexpected generic error in wal")
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_FILE_CORRUPTED,           "WAL file is corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_SIZE_LIMIT,               "WAL size exceeds limit")

// http
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_SERVER_OFFLINE,          "http server is not onlin")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_UNSUPPORT_URL,           "url is not support")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_URL,            "invalid url format")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_ENOUGH_MEMORY,        "no enough memory")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_REQUSET_TOO_BIG,         "request size is too big")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_AUTH_INFO,            "no auth info input")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_MSG_INPUT,            "request is empty")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_SQL_INPUT,            "no sql input")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_EXEC_USEDB,           "no need to execute use db cmd")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_SESSION_FULL,            "session list was full")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GEN_TAOSD_TOKEN_ERR,     "generate taosd token error")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_MULTI_REQUEST,   "size of multi request is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_CREATE_GZIP_FAILED,      "failed to create gzip")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_FINISH_GZIP_FAILED,      "failed to finish gzip")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_LOGIN_FAILED,            "failed to login")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_VERSION,         "invalid http version")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_CONTENT_LENGTH,  "invalid content length")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_AUTH_TYPE,       "invalid type of Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_AUTH_FORMAT,     "invalid format of Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_BASIC_AUTH,      "invalid basic Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_TAOSD_AUTH,      "invalid taosd Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_METHOD_FAILED,     "failed to parse method")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_TARGET_FAILED,     "failed to parse target")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_VERSION_FAILED,    "failed to parse http version")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_SP_FAILED,         "failed to parse sp")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_STATUS_FAILED,     "failed to parse status")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_PHRASE_FAILED,     "failed to parse phrase")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CRLF_FAILED,       "failed to parse crlf")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_FAILED,     "failed to parse header")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED, "failed to parse header key")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED, "failed to parse header val")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED, "failed to parse chunk size")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CHUNK_FAILED,      "failed to parse chunk")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_END_FAILED,        "failed to parse end section")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_INVALID_STATE,     "invalid parse state")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_ERROR_STATE,       "failed to parse error section")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_QUERY_NULL,           "query size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_QUERY_SIZE,           "query size can not more than 100")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_REQ_PARSE_ERROR,      "parse grafana json error")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_DB_NOT_INPUT,         "database name can not be null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_DB_TOO_LONG,          "database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_INVALID_JSON,         "invalid telegraf json fromat")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRICS_NULL,         "metrics size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRICS_SIZE,         "metrics size can not more than 1K")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NULL,          "metric name not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_TYPE,          "metric name type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NAME_NULL,     "metric name length is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NAME_LONG,     "metric name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_NULL,       "timestamp not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_TYPE,       "timestamp type should be integer")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_VAL_NULL,   "timestamp value smaller than 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_NULL,            "tags not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_SIZE_0,          "tags size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_SIZE_LONG,       "tags size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NULL,             "tag is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NAME_NULL,        "tag name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NAME_SIZE,        "tag name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_VALUE_TYPE,       "tag value type should be number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_VALUE_NULL,       "tag value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TABLE_NULL,           "table is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TABLE_SIZE,           "table name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_NULL,          "fields not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_SIZE_0,        "fields size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_SIZE_LONG,     "fields size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NULL,           "field is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NAME_NULL,      "field name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NAME_SIZE,      "field name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_VALUE_TYPE,     "field value type should be number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_VALUE_NULL,     "field value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_HOST_NOT_STRING,      "host type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_STABLE_NOT_EXIST,     "stable not exist")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_DB_NOT_INPUT,         "database name can not be null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_DB_TOO_LONG,          "database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_INVALID_JSON,         "invalid opentsdb json fromat")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRICS_NULL,         "metrics size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRICS_SIZE,         "metrics size can not more than 10K")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NULL,          "metric name not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_TYPE,          "metric name type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NAME_NULL,     "metric name length is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NAME_LONG,     "metric name length can not more than 22")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_NULL,       "timestamp not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_TYPE,       "timestamp type should be integer")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_VAL_NULL,   "timestamp value smaller than 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_NULL,            "tags not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_SIZE_0,          "tags size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_SIZE_LONG,       "tags size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NULL,             "tag is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NAME_NULL,        "tag name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NAME_SIZE,        "tag name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_TYPE,       "tag value type should be boolean, number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_NULL,       "tag value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_TOO_LONG,   "tag value can not more than 64")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_VALUE_NULL,           "value not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_VALUE_TYPE,           "value type should be boolean, number or string")

// odbc
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_OOM,                     "out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_CHAR_NOT_NUM,       "convertion not a valid literal input")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_UNDEF,              "convertion undefined")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_TRUNC_FRAC,         "convertion fractional truncated")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_TRUNC,              "convertion truncated")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_NOT_SUPPORT,        "convertion not supported")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_OOR,                "convertion numeric value out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_OUT_OF_RANGE,            "out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NOT_SUPPORT,             "not supported yet")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_INVALID_HANDLE,          "invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NO_RESULT,               "no result set")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NO_FIELDS,               "no fields returned")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_INVALID_CURSOR,          "invalid cursor")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_STATEMENT_NOT_READY,     "statement not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONNECTION_BUSY,         "connection still busy")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_BAD_CONNSTR,             "bad connection string")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_BAD_ARG,                 "bad argument")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_NOT_VALID_TS,       "not a valid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_TOO_LARGE,      "src too large")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_BAD_SEQ,        "src bad sequence")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_INCOMPLETE,     "src incomplete")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_GENERAL,        "src general")

// tfs
TAOS_DEFINE_ERROR(TSDB_CODE_FS_OUT_OF_MEMORY,             "tfs out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_INVLD_CFG,                 "tfs invalid mount config")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_TOO_MANY_MOUNT,            "tfs too many mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_DUP_PRIMARY,               "tfs duplicate primary mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_PRIMARY_DISK,           "tfs no primary mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_MOUNT_AT_TIER,          "tfs no mount at tier")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_FILE_ALREADY_EXISTS,       "tfs file already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_INVLD_LEVEL,               "tfs invalid level")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_VALID_DISK,             "tfs no valid disk")

#ifdef TAOS_ERROR_C
};
#endif

static int tsCompareTaosError(const void* a, const void* b) {
  const STaosError* x = (const STaosError*)a;
  const STaosError* y = (const STaosError*)b;
  if (x->val < y->val) {
    return -1;
  }
  if (x->val > y->val) {
    return 1;
  }
  return 0;
}

static pthread_once_t tsErrorInit = PTHREAD_ONCE_INIT;
static void tsSortError(void) {
  qsort(errors, sizeof(errors)/sizeof(errors[0]), sizeof(errors[0]), tsCompareTaosError);
}


const char* tstrerror(int32_t err) {
  pthread_once(&tsErrorInit, tsSortError);

  // this is a system errno
  if ((err & 0x00ff0000) == 0x00ff0000) {
    return strerror(err & 0x0000ffff);
  }

  size_t s = 0, e = sizeof(errors)/sizeof(errors[0]);
  while (s < e) {
    size_t mid = (s + e) / 2;
    int32_t val = errors[mid].val;
    if (err > val) {
      s = mid + 1;
    } else if (err < val) {
      e = mid;
    } else if (err == val) {
      return errors[mid].str;
    } else {
      break;
    }
  }

  return "";
}
