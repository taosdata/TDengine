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

#ifndef TDENGINE_TAOSERROR_H
#define TDENGINE_TAOSERROR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#ifdef TAOS_ERROR_C
#define TAOS_DEFINE_ERROR(name, mod, code, msg) {.val = (int32_t)((0x80000000 | ((mod)<<16) | (code))), .str=(msg)},
#else
#define TAOS_DEFINE_ERROR(name, mod, code, msg) static const int32_t name = (int32_t)((0x80000000 | ((mod)<<16) | (code)));
#endif
 
#define TAOS_SYSTEM_ERROR(code)             (0x80ff0000 | (code))
#define TAOS_SUCCEEDED(err)                 ((err) >= 0)
#define TAOS_FAILED(err)                    ((err) < 0)

const char* tstrerror(int32_t err);

int32_t* taosGetErrno();
#define terrno                              (*taosGetErrno())
 
#define TSDB_CODE_SUCCESS                   0

#ifdef TAOS_ERROR_C
static STaosError errors[] = {
    {.val = 0, .str = "success"},
#endif

// rpc
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ACTION_IN_PROGRESS,       0, 0x0001, "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_REQUIRED,            0, 0x0002, "Authentication required")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_FAILURE,             0, 0x0003, "Authentication failure")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_REDIRECT,                 0, 0x0004, "Redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NOT_READY,                0, 0x0005, "System not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ALREADY_PROCESSED,        0, 0x0006, "Message already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED,0, 0x0007, "Last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MISMATCHED_LINK_ID,       0, 0x0008, "Mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_TOO_SLOW,                 0, 0x0009, "Processing of request timed out")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MAX_SESSIONS,             0, 0x000A, "Number of sessions reached limit")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NETWORK_UNAVAIL,          0, 0x000B, "Unable to establish connection")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_APP_ERROR,                0, 0x000C, "Unexpected generic error in RPC")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_UNEXPECTED_RESPONSE,      0, 0x000D, "Unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_VALUE,            0, 0x000E, "Invalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TRAN_ID,          0, 0x000F, "Invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_SESSION_ID,       0, 0x0010, "Invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_MSG_TYPE,         0, 0x0011, "Invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_RESPONSE_TYPE,    0, 0x0012, "Invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TIME_STAMP,       0, 0x0013, "Invalid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_APP_NOT_READY,                0, 0x0014, "Database not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_FQDN_ERROR,               0, 0x0015, "Unable to resolve FQDN")

//common & util
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OPS_NOT_SUPPORT,          0, 0x0100, "Operation not supported")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_MEMORY_CORRUPTED,         0, 0x0101, "Memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OUT_OF_MEMORY,            0, 0x0102, "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_INVALID_CFG_MSG,          0, 0x0103, "Invalid config message")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_FILE_CORRUPTED,           0, 0x0104, "Data file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_NO_MEMORY,                0, 0x0105, "Ref out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_FULL,                     0, 0x0106, "too many Ref Objs")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ID_REMOVED,               0, 0x0107, "Ref ID is removed")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_INVALID_ID,               0, 0x0108, "Invalid Ref ID")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_ALREADY_EXIST,            0, 0x0109, "Ref is already there")
TAOS_DEFINE_ERROR(TSDB_CODE_REF_NOT_EXIST,                0, 0x010A, "Ref is not there")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_INVALID_DISK_TIER,        0, 0x010B, "Invalid disk tier setting")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_TOO_MANY_DISKS,           0, 0x010C, "Too many disks in one tier")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_DISK_ALREADY_EXISTS,      0, 0x010D, "Disk already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_DISK_NOT_DIRECTORY,       0, 0x010E, "Disk is not a directory")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_NO_DISK_SPACE,            0, 0x010F, "Dnode no disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_DUPLICATE_PRIMARY_DISK,   0, 0x0110, "Duplicate primary disk")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_LACK_PRIMARY_DISK,        0, 0x0111, "Lack primary disk")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_NO_DISK_AT_TIER,          0, 0x0112, "No disk at tier")

//client
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_SQL,              0, 0x0200, "Invalid SQL statement")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_QHANDLE,          0, 0x0201, "Invalid qhandle")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TIME_STAMP,       0, 0x0202, "Invalid combination of client/service time")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VALUE,            0, 0x0203, "Invalid value in client")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VERSION,          0, 0x0204, "Invalid client version")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_IE,               0, 0x0205, "Invalid client ie")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_FQDN,             0, 0x0206, "Invalid host name")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_USER_LENGTH,      0, 0x0207, "Invalid user name")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_PASS_LENGTH,      0, 0x0208, "Invalid password")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_DB_LENGTH,        0, 0x0209, "Database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH,  0, 0x020A, "Table name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_CONNECTION,       0, 0x020B, "Invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_OUT_OF_MEMORY,            0, 0x020C, "System out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_DISKSPACE,             0, 0x020D, "System out of disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CACHE_ERASED,       0, 0x020E, "Query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CANCELLED,          0, 0x020F, "Query terminated")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_SORTED_RES_TOO_MANY,      0, 0x0210, "Result set too large to be sorted")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_APP_ERROR,                0, 0x0211, "Application error")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_ACTION_IN_PROGRESS,       0, 0x0212, "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DISCONNECTED,             0, 0x0213, "Disconnected from service")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_WRITE_AUTH,            0, 0x0214, "No write permission")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_CONN_KILLED,              0, 0x0215, "Connection killed")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_SQL_SYNTAX_ERROR,         0, 0x0216, "Syntax error in SQL")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DB_NOT_SELECTED,          0, 0x0217, "Database not specified or available")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TABLE_NAME,       0, 0x0218, "Table does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_EXCEED_SQL_LIMIT,         0, 0x0219, "SQL statement too long, check maxSQLLength config")

// mnode
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MSG_NOT_PROCESSED,        0, 0x0300, "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_IN_PROGRESS,       0, 0x0301, "Message is progressing")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_NEED_REPROCESSED,  0, 0x0302, "Messag need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_RIGHTS,                0, 0x0303, "Insufficient privilege for operation")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_APP_ERROR,                0, 0x0304, "Unexpected generic error in mnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONNECTION,       0, 0x0305, "Invalid message connection")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_VERSION,      0, 0x0306, "Incompatible protocol version")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_LEN,          0, 0x0307, "Invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_TYPE,         0, 0x0308, "Invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_SHELL_CONNS,     0, 0x0309, "Too many connections")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_OUT_OF_MEMORY,            0, 0x030A, "Out of memory in mnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_SHOWOBJ,          0, 0x030B, "Data expired")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_QUERY_ID,         0, 0x030C, "Invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STREAM_ID,        0, 0x030D, "Invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONN_ID,          0, 0x030E, "Invalid connection id")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE,    0, 0x0320, "Object already there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_ERROR,                0, 0x0321, "Unexpected generic error in sdb")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE,   0, 0x0322, "Invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_OBJ_NOT_THERE,        0, 0x0323, "Object not there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVAID_META_ROW,      0, 0x0324, "Invalid meta row")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_INVAID_KEY_TYPE,      0, 0x0325, "Invalid key type")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ALREADY_EXIST,      0, 0x0330, "DNode already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_EXIST,          0, 0x0331, "DNode does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_EXIST,         0, 0x0332, "VGroup does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_REMOVE_MASTER,         0, 0x0333, "Master DNode cannot be removed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_ENOUGH_DNODES,         0, 0x0334, "Out of DNodes")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT, 0, 0x0335, "Cluster cfg inconsistent")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION, 0, 0x0336, "Invalid dnode cfg option")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_BALANCE_ENABLED,          0, 0x0337, "Balance already enabled")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_IN_DNODE,      0, 0x0338, "Vgroup not in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE,  0, 0x0339, "Vgroup already in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_FREE,           0, 0x033A, "Dnode not avaliable")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CLUSTER_ID,       0, 0x033B, "Cluster id not match")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_READY,                0, 0x033C, "Cluster not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED,  0, 0x033D, "Dnode Id not configured")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED,  0, 0x033E, "Dnode Ep not configured")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_ALREADY_EXIST,       0, 0x0340, "Account already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT,             0, 0x0341, "Invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT_OPTION,      0, 0x0342, "Invalid account options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_EXPIRED,             0, 0x0343, "Account authorization has expired")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_USER_ALREADY_EXIST,       0, 0x0350, "User already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER,             0, 0x0351, "Invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER_FORMAT,      0, 0x0352, "Invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_PASS_FORMAT,      0, 0x0353, "Invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_USER_FROM_CONN,        0, 0x0354, "Can not get user from conn")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_USERS,           0, 0x0355, "Too many users")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_TABLE_ALREADY_EXIST,      0, 0x0360, "Table already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_ID,         0, 0x0361, "Table name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_NAME,       0, 0x0362, "Table does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_TYPE,       0, 0x0363, "Invalid table type in tsdb")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TAGS,            0, 0x0364, "Too many tags")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TIMESERIES,      0, 0x0366, "Too many time series")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_SUPER_TABLE,          0, 0x0367, "Not super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_MND_COL_NAME_TOO_LONG,        0, 0x0368, "Tag name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_ALREAY_EXIST,         0, 0x0369, "Tag already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_NOT_EXIST,            0, 0x036A, "Tag does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_ALREAY_EXIST,       0, 0x036B, "Field already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_NOT_EXIST,          0, 0x036C, "Field does not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STABLE_NAME,      0, 0x036D, "Super table does not exist")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_NOT_SELECTED,          0, 0x0380, "Database not specified or available")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_ALREADY_EXIST,         0, 0x0381, "Database already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION,        0, 0x0382, "Invalid database options")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB,               0, 0x0383, "Invalid database name")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MONITOR_DB_FORBIDDEN,     0, 0x0384, "Cannot delete monitor database")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_DATABASES,       0, 0x0385, "Too many databases for account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_IN_DROPPING,           0, 0x0386, "Database not available")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_READY,         0, 0x0387, "Database unsynced")

// dnode
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MSG_NOT_PROCESSED,        0, 0x0400, "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_OUT_OF_MEMORY,            0, 0x0401, "Dnode out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_NO_WRITE_ACCESS,          0, 0x0402, "No permission for disk files in dnode")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_INVALID_MSG_LEN,          0, 0x0403, "Invalid message length")

// vnode 
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_IN_PROGRESS,       0, 0x0500, "Action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_MSG_NOT_PROCESSED,        0, 0x0501, "Message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_NEED_REPROCESSED,  0, 0x0502, "Action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_VGROUP_ID,        0, 0x0503, "Invalid Vgroup ID")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INIT_FAILED,              0, 0x0504, "Vnode initialization failed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISKSPACE,             0, 0x0505, "System out of disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISK_PERMISSIONS,      0, 0x0506, "No write permission for disk files")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR,      0, 0x0507, "Missing data file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_OUT_OF_MEMORY,            0, 0x0508, "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_APP_ERROR,                0, 0x0509, "Unexpected generic error in vnode")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_VRESION_FILE,     0, 0x050A, "Invalid version file")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_IS_FULL,                  0, 0x050B, "Vnode memory is full because commit failed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NOT_SYNCED,               0, 0x0511, "Database suspended")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_WRITE_AUTH,            0, 0x0512, "Write operation denied")

// tsdb
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_ID,         0, 0x0600, "Invalid table ID")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_TYPE,       0, 0x0601, "Invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION,    0, 0x0602, "Invalid table schema version")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_ALREADY_EXIST,      0, 0x0603, "Table already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_CONFIG,           0, 0x0604, "Invalid configuration")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INIT_FAILED,              0, 0x0605, "Tsdb init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISKSPACE,             0, 0x0606, "No diskspace for tsdb")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISK_PERMISSIONS,      0, 0x0607, "No permission for disk files")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_FILE_CORRUPTED,           0, 0x0608, "Data file(s) corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_OUT_OF_MEMORY,            0, 0x0609, "Out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE,      0, 0x060A, "Tag too old")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE,   0, 0x060B, "Timestamp data out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP,      0, 0x060C, "Submit message is messed up")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_ACTION,           0, 0x060D, "Invalid operation")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_CREATE_TB_MSG,    0, 0x060E, "Invalid creation of table")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_TABLE_DATA_IN_MEM,     0, 0x060F, "No table data in memory skiplist")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_FILE_ALREADY_EXISTS,      0, 0x0610, "File already exists")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_RECONFIGURE,        0, 0x0611, "Need to reconfigure table")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO,    0, 0x0612, "Invalid information to create table")

// query
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_QHANDLE,          0, 0x0700, "Invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_MSG,              0, 0x0701, "Invalid message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NO_DISKSPACE,             0, 0x0702, "No diskspace for query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_OUT_OF_MEMORY,            0, 0x0703, "System out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_APP_ERROR,                0, 0x0704, "Unexpected generic error in query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_DUP_JOIN_KEY,             0, 0x0705, "Duplicated join key")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_EXCEED_TAGS_LIMIT,        0, 0x0706, "Tag conditon too many")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NOT_READY,                0, 0x0707, "Query not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_HAS_RSP,                  0, 0x0708, "Query should response")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_IN_EXEC,                  0, 0x0709, "Multiple retrieval of this query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW,      0, 0x070A, "Too many time window in query")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NOT_ENOUGH_BUFFER,        0, 0x070B, "Query buffer limit has reached")

// grant
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,                0, 0x0800, "License expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,          0, 0x0801, "DNode creation limited by licence")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,           0, 0x0802, "Account creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,     0, 0x0803, "Table creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,             0, 0x0804, "DB creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,           0, 0x0805, "User creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,           0, 0x0806, "Conn creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,         0, 0x0807, "Stream creation limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,          0, 0x0808, "Write speed limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,        0, 0x0809, "Storage capacity limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,      0, 0x080A, "Query time limited by license")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,            0, 0x080B, "CPU cores limited by license")

// sync
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_CONFIG,           0, 0x0900, "Invalid Sync Configuration")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_NOT_ENABLED,              0, 0x0901, "Sync module not enabled")
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_VERSION,          0, 0x0902, "Invalid Sync version")

// wal
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_APP_ERROR,                0, 0x1000, "Unexpected generic error in wal")
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_FILE_CORRUPTED,           0, 0x1001, "WAL file is corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_SIZE_LIMIT,               0, 0x1002, "WAL size exceeds limit")

// http
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_SERVER_OFFLINE,          0, 0x1100, "http server is not onlin")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_UNSUPPORT_URL,           0, 0x1101, "url is not support")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVLALID_URL,            0, 0x1102, "invalid url format")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_ENOUGH_MEMORY,        0, 0x1103, "no enough memory")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_REQUSET_TOO_BIG,         0, 0x1104, "request size is too big")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_AUTH_INFO,            0, 0x1105, "no auth info input")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_MSG_INPUT,            0, 0x1106, "request is empty")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_SQL_INPUT,            0, 0x1107, "no sql input")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_NO_EXEC_USEDB,           0, 0x1108, "no need to execute use db cmd")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_SESSION_FULL,            0, 0x1109, "session list was full")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GEN_TAOSD_TOKEN_ERR,     0, 0x110A, "generate taosd token error")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_MULTI_REQUEST,   0, 0x110B, "size of multi request is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_CREATE_GZIP_FAILED,      0, 0x110C, "failed to create gzip")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_FINISH_GZIP_FAILED,      0, 0x110D, "failed to finish gzip")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_LOGIN_FAILED,            0, 0x110E, "failed to login")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_VERSION,         0, 0x1120, "invalid http version")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_CONTENT_LENGTH,  0, 0x1121, "invalid content length")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_AUTH_TYPE,       0, 0x1122, "invalid type of Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_AUTH_FORMAT,     0, 0x1123, "invalid format of Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_BASIC_AUTH,      0, 0x1124, "invalid basic Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_INVALID_TAOSD_AUTH,      0, 0x1125, "invalid taosd Authorization")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_METHOD_FAILED,     0, 0x1126, "failed to parse method")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_TARGET_FAILED,     0, 0x1127, "failed to parse target")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_VERSION_FAILED,    0, 0x1128, "failed to parse http version")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_SP_FAILED,         0, 0x1129, "failed to parse sp")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_STATUS_FAILED,     0, 0x112A, "failed to parse status")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_PHRASE_FAILED,     0, 0x112B, "failed to parse phrase")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CRLF_FAILED,       0, 0x112C, "failed to parse crlf")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_FAILED,     0, 0x112D, "failed to parse header")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED, 0, 0x112E, "failed to parse header key")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED, 0, 0x112F, "failed to parse header val")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED, 0, 0x1130, "failed to parse chunk size")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_CHUNK_FAILED,      0, 0x1131, "failed to parse chunk")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_END_FAILED,        0, 0x1132, "failed to parse end section")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_INVALID_STATE,     0, 0x1134, "invalid parse state")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_PARSE_ERROR_STATE,       0, 0x1135, "failed to parse error section")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_QUERY_NULL,           0, 0x1150, "query size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_QUERY_SIZE,           0, 0x1151, "query size can not more than 100")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_GC_REQ_PARSE_ERROR,      0, 0x1152, "parse grafana json error")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_DB_NOT_INPUT,         0, 0x1160, "database name can not be null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_DB_TOO_LONG,          0, 0x1161, "database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_INVALID_JSON,         0, 0x1162, "invalid telegraf json fromat")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRICS_NULL,         0, 0x1163, "metrics size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRICS_SIZE,         0, 0x1164, "metrics size can not more than 1K")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NULL,          0, 0x1165, "metric name not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_TYPE,          0, 0x1166, "metric name type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NAME_NULL,     0, 0x1167, "metric name length is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_METRIC_NAME_LONG,     0, 0x1168, "metric name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_NULL,       0, 0x1169, "timestamp not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_TYPE,       0, 0x116A, "timestamp type should be integer")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TIMESTAMP_VAL_NULL,   0, 0x116B, "timestamp value smaller than 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_NULL,            0, 0x116C, "tags not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_SIZE_0,          0, 0x116D, "tags size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAGS_SIZE_LONG,       0, 0x116E, "tags size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NULL,             0, 0x116F, "tag is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NAME_NULL,        0, 0x1170, "tag name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_NAME_SIZE,        0, 0x1171, "tag name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_VALUE_TYPE,       0, 0x1172, "tag value type should be number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TAG_VALUE_NULL,       0, 0x1173, "tag value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TABLE_NULL,           0, 0x1174, "table is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_TABLE_SIZE,           0, 0x1175, "table name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_NULL,          0, 0x1176, "fields not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_SIZE_0,        0, 0x1177, "fields size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELDS_SIZE_LONG,     0, 0x1178, "fields size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NULL,           0, 0x1179, "field is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NAME_NULL,      0, 0x117A, "field name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_NAME_SIZE,      0, 0x117B, "field name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_VALUE_TYPE,     0, 0x117C, "field value type should be number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_FIELD_VALUE_NULL,     0, 0x117D, "field value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_HOST_NOT_STRING,      0, 0x117E, "host type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_TG_STABLE_NOT_EXIST,     0, 0x117F, "stable not exist")

TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_DB_NOT_INPUT,         0, 0x1190, "database name can not be null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_DB_TOO_LONG,          0, 0x1191, "database name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_INVALID_JSON,         0, 0x1192, "invalid opentsdb json fromat")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRICS_NULL,         0, 0x1193, "metrics size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRICS_SIZE,         0, 0x1194, "metrics size can not more than 10K")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NULL,          0, 0x1195, "metric name not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_TYPE,          0, 0x1196, "metric name type should be string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NAME_NULL,     0, 0x1197, "metric name length is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_METRIC_NAME_LONG,     0, 0x1198, "metric name length can not more than 22")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_NULL,       0, 0x1199, "timestamp not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_TYPE,       0, 0x119A, "timestamp type should be integer")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TIMESTAMP_VAL_NULL,   0, 0x119B, "timestamp value smaller than 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_NULL,            0, 0x119C, "tags not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_SIZE_0,          0, 0x119D, "tags size is 0")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAGS_SIZE_LONG,       0, 0x119E, "tags size too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NULL,             0, 0x119F, "tag is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NAME_NULL,        0, 0x11A0, "tag name is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_NAME_SIZE,        0, 0x11A1, "tag name length too long")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_TYPE,       0, 0x11A2, "tag value type should be boolean, number or string")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_NULL,       0, 0x11A3, "tag value is null")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_TAG_VALUE_TOO_LONG,   0, 0x11A4, "tag value can not more than 64")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_VALUE_NULL,           0, 0x11A5, "value not find")
TAOS_DEFINE_ERROR(TSDB_CODE_HTTP_OP_VALUE_TYPE,           0, 0x11A6, "value type should be boolean, number or string")


TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_OOM,                     0, 0x2100, "out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_CHAR_NOT_NUM,       0, 0x2101, "convertion not a valid literal input")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_UNDEF,              0, 0x2102, "convertion undefined")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_TRUNC_FRAC,         0, 0x2103, "convertion fractional truncated")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_TRUNC,              0, 0x2104, "convertion truncated")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_NOT_SUPPORT,        0, 0x2105, "convertion not supported")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_OOR,                0, 0x2106, "convertion numeric value out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_OUT_OF_RANGE,            0, 0x2107, "out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NOT_SUPPORT,             0, 0x2108, "not supported yet")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_INVALID_HANDLE,          0, 0x2109, "invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NO_RESULT,               0, 0x210a, "no result set")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_NO_FIELDS,               0, 0x210b, "no fields returned")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_INVALID_CURSOR,          0, 0x210c, "invalid cursor")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_STATEMENT_NOT_READY,     0, 0x210d, "statement not ready")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONNECTION_BUSY,         0, 0x210e, "connection still busy")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_BAD_CONNSTR,             0, 0x210f, "bad connection string")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_BAD_ARG,                 0, 0x2110, "bad argument")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_NOT_VALID_TS,       0, 0x2111, "not a valid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_TOO_LARGE,      0, 0x2112, "src too large")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_BAD_SEQ,        0, 0x2113, "src bad sequence")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_INCOMPLETE,     0, 0x2114, "src incomplete")
TAOS_DEFINE_ERROR(TSDB_CODE_ODBC_CONV_SRC_GENERAL,        0, 0x2115, "src general")


#ifdef TAOS_ERROR_C
};
#endif


#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
