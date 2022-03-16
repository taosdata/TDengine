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

// clang-format off

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"

#define TAOS_ERROR_C

typedef struct {
  int32_t     val;
  const char* str;
} STaosError;

static threadlocal int32_t tsErrno;

int32_t* taosGetErrno() { return &tsErrno; }

#ifdef TAOS_ERROR_C
#define TAOS_DEFINE_ERROR(name, msg) {.val = (name), .str = (msg)},
#else
#define TAOS_DEFINE_ERROR(name, mod, code, msg) static const int32_t name = TAOS_DEF_ERROR_CODE(mod, code);
#endif

#define TAOS_SYSTEM_ERROR(code) (0x80ff0000 | (code))
#define TAOS_SUCCEEDED(err)     ((err) >= 0)
#define TAOS_FAILED(err)        ((err) < 0)

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
TAOS_DEFINE_ERROR(TSDB_CODE_COMPRESS_ERROR,               "Failed to compress msg")

//common & util
TAOS_DEFINE_ERROR(TSDB_CODE_OPS_NOT_SUPPORT,              "Operation not supported")
TAOS_DEFINE_ERROR(TSDB_CODE_OUT_OF_MEMORY,                "Out of Memory")
TAOS_DEFINE_ERROR(TSDB_CODE_OUT_OF_RANGE,                 "Out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PTR,                  "Invalid pointer")
TAOS_DEFINE_ERROR(TSDB_CODE_MEMORY_CORRUPTED,             "Memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_FILE_CORRUPTED,               "Data file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_CHECKSUM_ERROR,               "Checksum error")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG,                  "Invalid message")
TAOS_DEFINE_ERROR(TSDB_CODE_MSG_NOT_PROCESSED,            "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PARA,                 "Invalid parameters")
TAOS_DEFINE_ERROR(TSDB_CODE_REPEAT_INIT,                  "Repeat initialization")
TAOS_DEFINE_ERROR(TSDB_CODE_CFG_NOT_FOUND,                "Config not found")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CFG,                  "Invalid config option")

TAOS_DEFINE_ERROR(TSDB_CODE_REF_NO_MEMORY,                "Ref out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_FULL,                     "too many Ref Objs")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ID_REMOVED,               "Ref ID is removed")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_INVALID_ID,               "Invalid Ref ID")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ALREADY_EXIST,            "Ref is already there")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_NOT_EXIST,                "Ref is not there")

TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VERSION_NUMBER,       "Invalid version number")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VERSION_STRING,       "Invalid version string")
TAOS_DEFINE_ERROR(TSDB_CODE_VERSION_NOT_COMPATIBLE,       "Version not compatible")

//client
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_OPERATION,        "Invalid operation")
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
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_LINE_SYNTAX_ERROR,        "Syntax error in Line")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_META_CACHED,           "No table meta cached")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DUP_COL_NAMES,            "duplicated column names")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TAG_LENGTH,       "Invalid tag length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_COLUMN_LENGTH,    "Invalid column length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DUP_TAG_NAMES,            "duplicated tag names")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_JSON,             "Invalid JSON format")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_JSON_TYPE,        "Invalid JSON data type")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_VALUE_OUT_OF_RANGE,       "Value out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_INPUT,            "Invalid tsc input")

// mnode-common
TAOS_DEFINE_ERROR(TSDB_CODE_MND_APP_ERROR,                "Mnode internal error")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_READY,                "Cluster not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MSG_NOT_PROCESSED,        "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_IN_PROGRESS,       "Message is progressing")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_NEED_REPROCESSED,  "Message need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_RIGHTS,                "Insufficient privilege for operation")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_OPTIONS,          "Invalid mnode options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONNECTION,       "Invalid message connection")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_VERSION,      "Incompatible protocol version")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_LEN,          "Invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_TYPE,         "Invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_SHELL_CONNS,     "Too many connections")

// mnode-show
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_SHOWOBJ,          "Data expired")

// mnode-profile
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_QUERY_ID,         "Invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STREAM_ID,        "Invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONN_ID,          "Invalid connection id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MNODE_IS_RUNNING,         "mnode is alreay running")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_CONFIG_SYNC,    "failed to config sync")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_START_SYNC,     "failed to start sync")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_CREATE_DIR,     "failed to create mnode dir")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FAILED_TO_INIT_STEP,      "failed to init components")

// mnode-sdb
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_APP_ERROR,                "Unexpected generic error in sdb")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_OBJ_ALREADY_THERE,        "Object already there")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_OBJ_NOT_THERE,            "Object not there")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_OBJ_CREATING,             "Object is creating")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_OBJ_DROPPING,             "Object is dropping")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_TABLE_TYPE,       "Invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_KEY_TYPE,         "Invalid key type")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_ACTION_TYPE,      "Invalid action type")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_STATUS_TYPE,      "Invalid status type")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_DATA_VER,         "Invalid raw data version")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_DATA_LEN,         "Invalid raw data len")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_DATA_CONTENT,     "Invalid raw data content")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_INVALID_WAl_VER,          "Invalid wal version")

// mnode-dnode
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ALREADY_EXIST,      "Dnode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_EXIST,          "Dnode does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_DNODES,          "Too many dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_ENOUGH_DNODES,         "Out of dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CLUSTER_CFG,      "Cluster cfg inconsistent")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CLUSTER_ID,       "Cluster id not match")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DNODE_CFG,        "Invalid dnode cfg")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DNODE_EP,         "Invalid dnode end point")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DNODE_ID,         "Invalid dnode id")

// mnode-node
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MNODE_ALREADY_EXIST,      "Mnode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MNODE_NOT_EXIST,          "Mnode not there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_QNODE_ALREADY_EXIST,      "Qnode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_QNODE_NOT_EXIST,          "Qnode not there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SNODE_ALREADY_EXIST,      "Snode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SNODE_NOT_EXIST,          "Snode not there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_BNODE_ALREADY_EXIST,      "Bnode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_BNODE_NOT_EXIST,          "Bnode not there")

// mnode-acct
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_ALREADY_EXIST,       "Account already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_NOT_EXIST,           "Invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_ACCTS,           "Too many accounts")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT_OPTION,      "Invalid account options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_EXPIRED,             "Account authorization has expired")

// mnode-user
TAOS_DEFINE_ERROR(TSDB_CODE_MND_USER_ALREADY_EXIST,       "User already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_USER_NOT_EXIST,           "Invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_USERS,           "Too many users")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER_FORMAT,      "Invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_PASS_FORMAT,      "Invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_USER_FROM_CONN,        "Can not get user from conn")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ALTER_OPER,       "Invalid alter operation")

// mnode-db
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_ALREADY_EXIST,         "Database already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_NOT_EXIST,             "Database not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_DATABASES,       "Too many databases for account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_NOT_SELECTED,          "Database not specified or available")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB,               "Invalid database name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION,        "Invalid database options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_ACCT,          "Invalid database account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_OPTION_UNCHANGED,      "Database options not changed")

// mnode-vgroup
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE,  "Vgroup already in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_IN_DNODE,      "Vgroup not in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_EXIST,         "VGroup does not exist")

// mnode-stable
TAOS_DEFINE_ERROR(TSDB_CODE_MND_STB_ALREADY_EXIST,        "Stable already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_STB_NOT_EXIST,            "Stable not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NAME_CONFLICT_WITH_TOPIC, "Stable confilct with topic")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_STBS,            "Too many stables")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STB,              "Invalid stable name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STB_OPTION,       "Invalid stable options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STB_ALTER_OPTION, "Invalid stable alter options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_STB_OPTION_UNCHNAGED,     "Stable option unchanged")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ROW_BYTES,        "Invalid row bytes")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TAGS,            "Too many tags")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_ALREADY_EXIST,        "Tag already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_NOT_EXIST,            "Tag does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_COLUMNS,         "Too many columns")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_COLUMN_ALREADY_EXIST,     "Column already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_COLUMN_NOT_EXIST,         "Column does not exist")

// mnode-infoSchema
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_INFOS_TBL,        "Invalid information schema table name")


// mnode-func
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FUNC_ALREADY_EXIST,       "Func already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FUNC_NOT_EXIST,           "Func not exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC,             "Invalid func")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_NAME,        "Invalid func name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_COMMENT,     "Invalid func comment")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_CODE,        "Invalid func code")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_BUFSIZE,     "Invalid func bufSize")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_FUNC_RETRIEVE,    "Invalid func retrieve msg")

// mnode-trans
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TRANS_ALREADY_EXIST,      "Transaction already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TRANS_NOT_EXIST,          "Transaction not exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TRANS_INVALID_STAGE,      "Invalid stage to kill")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TRANS_CANT_PARALLEL,      "Invalid stage to kill")

// mnode-topic
TAOS_DEFINE_ERROR(TSDB_CODE_MND_UNSUPPORTED_TOPIC,        "Topic with aggregation is unsupported")

// dnode
TAOS_DEFINE_ERROR(TSDB_CODE_DND_ACTION_IN_PROGRESS,       "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_OFFLINE,                  "Dnode is offline")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_INVALID_MSG_LEN,          "Invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_DNODE_READ_FILE_ERROR,    "Read dnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_DNODE_WRITE_FILE_ERROR,   "Write dnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED,   "Mnode already deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MNODE_NOT_DEPLOYED,       "Mnode not deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MNODE_INVALID_OPTION,     "Mnode option invalid")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MNODE_READ_FILE_ERROR,    "Read mnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MNODE_WRITE_FILE_ERROR,   "Write mnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_QNODE_ALREADY_DEPLOYED,   "Qnode already deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_QNODE_NOT_DEPLOYED,       "Qnode not deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_QNODE_INVALID_OPTION,     "Qnode option invalid")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_QNODE_READ_FILE_ERROR,    "Read qnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR,   "Write qnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_SNODE_ALREADY_DEPLOYED,   "Snode already deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_SNODE_NOT_DEPLOYED,       "Snode not deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_SNODE_INVALID_OPTION,     "Snode option invalid")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_SNODE_READ_FILE_ERROR,    "Read snode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_SNODE_WRITE_FILE_ERROR,   "Write snode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED,   "Bnode already deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_BNODE_NOT_DEPLOYED,       "Bnode not deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_BNODE_INVALID_OPTION,     "Bnode option invalid")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_BNODE_READ_FILE_ERROR,    "Read bnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR,   "Write bnode.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED,   "Vnode already deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_NOT_DEPLOYED,       "Vnode not deployed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_INVALID_OPTION,     "Vnode option invalid")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_READ_FILE_ERROR,    "Read vnodes.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_WRITE_FILE_ERROR,   "Write vnodes.json error")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_VNODE_TOO_MANY_VNODES,    "Too many vnodes")

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
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_CFG_FILE,         "Invalid config file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_TERM_FILE,        "Invalid term file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_FLOWCTRL,              "Database memory is full")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_DROPPING,              "Database is dropping")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_UPDATING,              "Database is updating")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_CLOSING,               "Database is closing")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NOT_SYNCED,               "Database suspended")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_WRITE_AUTH,            "Database write operation denied")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_SYNCING,               "Database is syncing")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_TSDB_STATE,       "Invalid tsdb state")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_TB_NOT_EXIST,             "Table not exists")

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
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_IVLD_TAG_VAL,             "TSDB invalid tag value")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_CACHE_LAST_ROW,        "TSDB no cache last row data")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_SMA_INDEX_IN_META,     "No sma index in meta")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TDB_ENV_OPEN_ERROR,       "TDB env open error")

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
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_TIME_CONDITION,   "One valid time range condition expected")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_SYS_ERROR,                "System error")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_INPUT,            "invalid input")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_SCH_NOT_EXIST,            "Scheduler not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_NOT_EXIST,           "Task not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_ALREADY_EXIST,       "Task already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST,       "Task context not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_CANCELLED,           "Task cancelled")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_DROPPED,             "Task dropped")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_CANCELLING,          "Task cancelling")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_DROPPING,            "Task dropping")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_DUPLICATTED_OPERATION,    "Duplicatted operation")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_MSG_ERROR,           "Task message error")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_JOB_FREED,                "Job already freed")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TASK_STATUS_ERROR,        "Task status error")

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
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_INVALID_VER,              "WAL use invalid version")

// tfs
TAOS_DEFINE_ERROR(TSDB_CODE_FS_APP_ERROR,                "tfs out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_INVLD_CFG,                 "tfs invalid mount config")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_TOO_MANY_MOUNT,            "tfs too many mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_DUP_PRIMARY,               "tfs duplicate primary mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_PRIMARY_DISK,           "tfs no primary mount")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_MOUNT_AT_TIER,          "tfs no mount at tier")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_FILE_ALREADY_EXISTS,       "tfs file already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_INVLD_LEVEL,               "tfs invalid level")
TAOS_DEFINE_ERROR(TSDB_CODE_FS_NO_VALID_DISK,             "tfs no valid disk")

// catalog
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_INTERNAL_ERROR,           "catalog internal error")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_INVALID_INPUT,            "invalid catalog input parameters")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_NOT_READY,                "catalog is not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_MEM_ERROR,                "catalog memory error")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_SYS_ERROR,                "catalog system error")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_DB_DROPPED,               "Database is dropped")
TAOS_DEFINE_ERROR(TSDB_CODE_CTG_OUT_OF_SERVICE,           "catalog is out of service")

//scheduler
TAOS_DEFINE_ERROR(TSDB_CODE_SCH_STATUS_ERROR,             "scheduler status error")
TAOS_DEFINE_ERROR(TSDB_CODE_SCH_INTERNAL_ERROR,           "scheduler internal error")

#ifdef TAOS_ERROR_C
};
#endif

static int32_t taosCompareTaosError(const void* a, const void* b) {
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
  qsort(errors, sizeof(errors) / sizeof(errors[0]), sizeof(errors[0]), taosCompareTaosError);
}

const char* tstrerror(int32_t err) {
  pthread_once(&tsErrorInit, tsSortError);

  // this is a system errno
  if ((err & 0x00ff0000) == 0x00ff0000) {
    return strerror(err & 0x0000ffff);
  }

  int32_t s = 0;
  int32_t e = sizeof(errors) / sizeof(errors[0]);

  while (s < e) {
    int32_t mid = (s + e) / 2;
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

const char* terrstr() { return tstrerror(terrno); }
