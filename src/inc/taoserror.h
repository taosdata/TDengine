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
#define TAOS_DEFINE_ERROR(name, mod, code, msg) {.val = (0x80000000 | ((mod)<<16) | (code)), .str=(msg)},
#else
#define TAOS_DEFINE_ERROR(name, mod, code, msg) static const int32_t name = (0x80000000 | ((mod)<<16) | (code));
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
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ACTION_IN_PROGRESS,       0, 0x0001, "action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_REQUIRED,            0, 0x0002, "auth required")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_AUTH_FAILURE,             0, 0x0003, "auth failure")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_REDIRECT,                 0, 0x0004, "redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NOT_READY,                0, 0x0005, "not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_ALREADY_PROCESSED,        0, 0x0006, "message already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED,0, 0x0007, "last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MISMATCHED_LINK_ID,       0, 0x0008, "mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_TOO_SLOW,                 0, 0x0009, "too slow")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_MAX_SESSIONS,             0, 0x000A, "max sessions")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_NETWORK_UNAVAIL,          0, 0x000B, "network unavailable")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_APP_ERROR,                0, 0x000C, "rpc app error")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_UNEXPECTED_RESPONSE,      0, 0x000D, "unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_VALUE,            0, 0x000E, "invalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TRAN_ID,          0, 0x000F, "invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_SESSION_ID,       0, 0x0010, "invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_MSG_TYPE,         0, 0x0011, "invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_RESPONSE_TYPE,    0, 0x0012, "invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_RPC_INVALID_TIME_STAMP,       0, 0x0013, "invalid timestamp")

//common & util
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OPS_NOT_SUPPORT,          0, 0x0100, "operations not support")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_MEMORY_CORRUPTED,         0, 0x0101, "memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_OUT_OF_MEMORY,            0, 0x0102, "out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_COM_INVALID_CFG_MSG,          0, 0x0103, "invalid config message")

//client
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_SQL,              0, 0x0200, "invalid sql")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_QHANDLE,          0, 0x0201, "client invalid qhandle")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TIME_STAMP,       0, 0x0202, "client time/server time can not be mixed up")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VALUE,            0, 0x0203, "clientinvalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_VERSION,          0, 0x0204, "client invalid version")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_IE,               0, 0x0205, "client invalid ie")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_FQDN,             0, 0x0206, "client invalid fqdn")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_USER_LENGTH,      0, 0x0207, "client invalid username length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_PASS_LENGTH,      0, 0x0208, "client invalid password length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_DB_LENGTH,        0, 0x0209, "client invalid database length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH,  0, 0x020A, "client invalid table length")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_INVALID_CONNECTION,       0, 0x020B, "client invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_OUT_OF_MEMORY,            0, 0x020C, "client out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_DISKSPACE,             0, 0x020D, "client no disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CACHE_ERASED,       0, 0x020E, "client query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_QUERY_CANCELLED,          0, 0x020F, "client query cancelled")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_SORTED_RES_TOO_MANY,      0, 0x0210, "client sorted res too many")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_APP_ERROR,                0, 0x0211, "client app error")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_ACTION_IN_PROGRESS,       0, 0x0212, "client action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_DISCONNECTED,             0, 0x0213, "client disconnected")
TAOS_DEFINE_ERROR(TSDB_CODE_TSC_NO_WRITE_AUTH,            0, 0x0214, "client no write auth")

// mnode
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MSG_NOT_PROCESSED,        0, 0x0300, "mnode message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_IN_PROGRESS,       0, 0x0301, "mnode action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACTION_NEED_REPROCESSED,  0, 0x0302, "mnode action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_RIGHTS,                0, 0x0303, "mnode no rights")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_APP_ERROR,                0, 0x0304, "mnode app error")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONNECTION,       0, 0x0305, "mnode invalid message connection")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_VERSION,      0, 0x0306, "mnode invalid message version")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_LEN,          0, 0x0307, "mnode invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_MSG_TYPE,         0, 0x0308, "mnode invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_SHELL_CONNS,     0, 0x0309, "mnode too many shell conns")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_OUT_OF_MEMORY,            0, 0x030A, "mnode out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_SHOWOBJ,          0, 0x030B, "mnode invalid show handle")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_QUERY_ID,         0, 0x030C, "mnode invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_STREAM_ID,        0, 0x030D, "mnode invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_CONN_ID,          0, 0x030E, "mnode invalid connection")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE,    0, 0x0320, "mnode object already there")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_SDB_ERROR,                0, 0x0321, "mnode sdb error")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_ALREADY_EXIST,      0, 0x0330, "mnode dnode already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DNODE_NOT_EXIST,          0, 0x0331, "mnode dnode not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_VGROUP_NOT_EXIST,         0, 0x0332, "mnode vgroup not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_REMOVE_MASTER,         0, 0x0333, "mnode cant not remove master")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_ENOUGH_DNODES,         0, 0x0334, "mnode no enough dnodes")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_ACCT_ALREADY_EXIST,       0, 0x0340, "mnode accounts already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT,             0, 0x0341, "mnode invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT_PARA,        0, 0x0342, "mnode invalid account parameter")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_ACCT_OPTION,      0, 0x0343, "mnode invalid acct option")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_ACCTS,           0, 0x0344, "mnode too many accounts")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_USER_ALREADY_EXIST,       0, 0x0350, "mnode user already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER,             0, 0x0351, "mnode invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_USER_FORMAT,      0, 0x0352, "mnode invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_PASS_FORMAT,      0, 0x0353, "mnode invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NO_USER_FROM_CONN,        0, 0x0354, "mnode can not get user from conn")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_USERS,           0, 0x0355, "mnode too many users")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_TABLE_ALREADY_EXIST,      0, 0x0360, "mnode table already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_ID,         0, 0x0361, "mnode invalid table id")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_TABLE_TYPE,       0, 0x0362, "mnode invalid table type")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TAGS,            0, 0x0363, "mnode too many tags")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TABLES,          0, 0x0364, "mnode too many tables")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_TIMESERIES,      0, 0x0365, "mnode not enough time series")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_NOT_SUPER_TABLE,          0, 0x0366, "mnode no super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_MND_COL_NAME_TOO_LONG,        0, 0x0367, "mnode column name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_ALREAY_EXIST,         0, 0x0368, "mnode tag already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TAG_NOT_EXIST,            0, 0x0369, "mnode tag not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_ALREAY_EXIST,       0, 0x036A, "mnode field already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_FIELD_NOT_EXIST,          0, 0x036B, "mnode field not exist")

TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_NOT_SELECTED,          0, 0x0380, "mnode db not selected")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_DB_ALREADY_EXIST,         0, 0x0381, "mnode database aleady exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB_OPTION,        0, 0x0382, "mnode invalid db option")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_INVALID_DB,               0, 0x0383, "mnode invalid database")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_MONITOR_DB_FORBIDDEN,     0, 0x0384, "mnode monitor db forbidden")
TAOS_DEFINE_ERROR(TSDB_CODE_MND_TOO_MANY_DATABASES,       0, 0x0385, "mnode too many databases")

// dnode
TAOS_DEFINE_ERROR(TSDB_CODE_DND_MSG_NOT_PROCESSED,        0, 0x0400, "dnode message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_OUT_OF_MEMORY,            0, 0x0401, "dnode out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_NO_WRITE_ACCESS,          0, 0x0402, "dnode no disk write access")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_INVALID_MSG_LEN,          0, 0x0403, "dnode invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_DND_INVALID_FILE_FORMAT,      0, 0x0404, "dnode invalid file format")

// vnode 
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_IN_PROGRESS,       0, 0x0500, "vnode action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_MSG_NOT_PROCESSED,        0, 0x0501, "vnode message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_ACTION_NEED_REPROCESSED,  0, 0x0502, "vnode action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INVALID_VGROUP_ID,        0, 0x0503, "vnode invalid vgroup id")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_INIT_FAILED,              0, 0x0504, "vnode init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISKSPACE,             0, 0x0505, "vnode no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_DISK_PERMISSIONS,      0, 0x0506, "vnode no disk permissions")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR,      0, 0x0507, "vnode no such file or directory")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_OUT_OF_MEMORY,            0, 0x0508, "vnode out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_VND_APP_ERROR,                0, 0x0509, "vnode app error")

// tsdb
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_ID,         0, 0x0600, "tsdb invalid table id")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_TABLE_TYPE,       0, 0x0601, "tsdb invalid table schema version")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_SCHEMA_VERSION,     0, 0x0602, "tsdb invalid table schema version")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TABLE_ALREADY_EXIST,      0, 0x0603, "tsdb table already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INVALID_CONFIG,           0, 0x0604, "tsdb invalid configuration")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_INIT_FAILED,              0, 0x0605, "tsdb init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISKSPACE,             0, 0x0606, "tsdb no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_NO_DISK_PERMISSIONS,      0, 0x0607, "tsdb no disk permissions")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_FILE_CORRUPTED,           0, 0x0608, "tsdb file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_OUT_OF_MEMORY,            0, 0x0609, "tsdb out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE,      0, 0x060A, "tsdb tag version is out of date")
TAOS_DEFINE_ERROR(TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE,   0, 0x060B, "tsdb timestamp is out of range")

// query
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_QHANDLE,          0, 0x0700, "query invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_INVALID_MSG,              0, 0x0701, "query invalid message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_NO_DISKSPACE,             0, 0x0702, "query no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_OUT_OF_MEMORY,            0, 0x0703, "query out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_QRY_APP_ERROR,                0, 0x0704, "query app error")

// grant
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,                0, 0x0800, "grant expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,          0, 0x0801, "grant dnode limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,           0, 0x0802, "grant account limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,     0, 0x0803, "grant timeseries limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,             0, 0x0804, "grant db limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,           0, 0x0805, "grant user limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,           0, 0x0806, "grant conn limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,         0, 0x0807, "grant stream limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,          0, 0x0808, "grant speed limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,        0, 0x0809, "grant storage limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,      0, 0x080A, "grant query time limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,            0, 0x080B, "grant cpu limited")

// sync
TAOS_DEFINE_ERROR(TSDB_CODE_SYN_INVALID_CONFIG,           0, 0x0900, "sync invalid configuration")

// wal
TAOS_DEFINE_ERROR(TSDB_CODE_WAL_APP_ERROR,                0, 0x1000, "wal app error")

#ifdef TAOS_ERROR_C
};
#endif


#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
