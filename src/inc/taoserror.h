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
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_IN_PROGRESS,         0, 0x0001, "action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_NEED_REPROCESSED,    0, 0x0003, "action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MSG_NOT_PROCESSED,          0, 0x0004, "message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_PROCESSED,          0, 0x0005, "message already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_REDIRECT,                   0, 0x0006, "redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_LAST_SESSION_NOT_FINISHED,  0, 0x0007, "last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_MAX_SESSIONS,               0, 0x0008, "max sessions")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SESSION_ID,         0, 0x0009, "invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TRAN_ID,            0, 0x000A, "invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_TYPE,           0, 0x000B, "invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_LEN,            0, 0x000C, "invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_CONTENT,        0, 0x000D, "invalid message content")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_VERSION,        0, 0x000E, "invalid message version")
TAOS_DEFINE_ERROR(TSDB_CODE_UNEXPECTED_RESPONSE,        0, 0x000F, "unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_RESPONSE_TYPE,      0, 0x0010, "invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_MISMATCHED_METER_ID,        0, 0x0011, "mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_DISCONNECTED,               0, 0x0012, "disconnected")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_READY,                  0, 0x0013, "not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_SLOW,                   0, 0x0014, "too slow")
TAOS_DEFINE_ERROR(TSDB_CODE_OTHERS,                     0, 0x0015, "others")
TAOS_DEFINE_ERROR(TSDB_CODE_APP_ERROR,                  0, 0x0016, "app error")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_THERE,              0, 0x0017, "already there")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RESOURCE,                0, 0x0018, "no resource")
TAOS_DEFINE_ERROR(TSDB_CODE_OPS_NOT_SUPPORT,            0, 0x0019, "operations not support")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_OPTION,             0, 0x001A, "invalid option")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_CONFIGURED,             0, 0x001B, "not configured")
TAOS_DEFINE_ERROR(TSDB_CODE_NETWORK_UNAVAIL,            0, 0x001C, "network unavailable")
TAOS_DEFINE_ERROR(TSDB_CODE_AUTH_REQUIRED,              0, 0x001D, "auth required")

// db
TAOS_DEFINE_ERROR(TSDB_CODE_DB_NOT_SELECTED,            0, 0x0100, "db not selected")
TAOS_DEFINE_ERROR(TSDB_CODE_DB_ALREADY_EXIST,           0, 0x0101, "database aleady exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_DB,                 0, 0x0102, "invalid database")
TAOS_DEFINE_ERROR(TSDB_CODE_MONITOR_DB_FORBIDDEN,       0, 0x0103, "monitor db forbidden")

// user
TAOS_DEFINE_ERROR(TSDB_CODE_USER_ALREADY_EXIST,         0, 0x0180, "user already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_USER,               0, 0x0181, "invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PASS,               0, 0x0182, "invalid password")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_USER_FORMAT,        0, 0x0183, "invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PASS_FORMAT,        0, 0x0184, "invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_USER_FROM_CONN,          0, 0x0185, "can not get user from conn")

// table
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ALREADY_EXIST,        0, 0x0200, "table already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_ID,           0, 0x0201, "invalid table id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_TYPE,         0, 0x0202, "invalid table typee")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE,              0, 0x0203, "invalid table name")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_SUPER_TABLE,            0, 0x0204, "no super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_TABLE,           0, 0x0205, "not active table")
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ID_MISMATCH,          0, 0x0206, "table id mismatch")
TAOS_DEFINE_ERROR(TSDB_CODE_TAG_ALREAY_EXIST,           0, 0x0207, "tag already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_TAG_NOT_EXIST,              0, 0x0208, "tag not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_FIELD_ALREAY_EXIST,         0, 0x0209, "field already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_FIELD_NOT_EXIST,            0, 0x020A, "field not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_COL_NAME_TOO_LONG,          0, 0x020B, "column name too long")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TAGS,              0, 0x020C, "too many tags")


// dnode & mnode
TAOS_DEFINE_ERROR(TSDB_CODE_NO_ENOUGH_DNODES,           0, 0x0280, "no enough dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_ALREADY_EXIST,        0, 0x0281, "dnode already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_NOT_EXIST,            0, 0x0282, "dnode not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_MASTER,                  0, 0x0283, "no master")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_REMOVE_MASTER,           0, 0x0284, "no remove master")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_ID,           0, 0x0285, "invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_STREAM_ID,          0, 0x0286, "invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CONNECTION,         0, 0x0287, "invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_ERROR,                  0, 0x0288, "sdb error")
TAOS_DEFINE_ERROR(TSDB_CODE_TIMESTAMP_OUT_OF_RANGE,     0, 0x0289, "timestamp is out of range")

// acct
TAOS_DEFINE_ERROR(TSDB_CODE_ACCT_ALREADY_EXIST,         0, 0x0300, "accounts already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT,               0, 0x0301, "invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT_PARAMETER,     0, 0x0302, "invalid account parameter")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_ACCTS,             0, 0x0303, "too many accounts")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_USERS,             0, 0x0304, "too many users")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TABLES,            0, 0x0305, "too many tables")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_DATABASES,         0, 0x0306, "too many databases")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TIME_SERIES,       0, 0x0307, "not enough time series")

// grant
TAOS_DEFINE_ERROR(TSDB_CODE_AUTH_FAILURE,               0, 0x0380, "auth failure")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RIGHTS,                  0, 0x0381, "no rights")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_WRITE_ACCESS,            0, 0x0382, "no write access")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_READ_ACCESS,             0, 0x0383, "no read access")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,              0, 0x0384, "grant expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,        0, 0x0385, "grant dnode limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,         0, 0x0386, "grant account limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,   0, 0x0387, "grant timeseries limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,           0, 0x0388, "grant db limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,         0, 0x0389, "grant user limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,         0, 0x038A, "grant conn limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,       0, 0x038B, "grant stream limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,        0, 0x038C, "grant speed limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,      0, 0x038D, "grant storage limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,    0, 0x038E, "grant query time limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,          0, 0x038F, "grant cpu limited")

// server
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VGROUP_ID,          0, 0x0400, "invalid vgroup id")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_VNODE,           0, 0x0401, "not active vnode")
TAOS_DEFINE_ERROR(TSDB_CODE_VG_INIT_FAILED,             0, 0x0402, "vgroup init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_NO_DISKSPACE,          0, 0x0403, "server no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_OUT_OF_MEMORY,         0, 0x0404, "server out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_DISK_PERMISSIONS,        0, 0x0405, "no disk permissions")
TAOS_DEFINE_ERROR(TSDB_CODE_FILE_CORRUPTED,             0, 0x0406, "file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_MEMORY_CORRUPTED,           0, 0x0407, "memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_SUCH_FILE_OR_DIR,       0, 0x0408, "no such file or directory")

// client
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CLIENT_VERSION,     0, 0x0481, "invalid client version")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_OUT_OF_MEMORY,          0, 0x0482, "client out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_NO_DISKSPACE,           0, 0x0483, "client no disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TIME_STAMP,         0, 0x0484, "invalid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SQL,                0, 0x0485, "invalid sql")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CACHE_ERASED,         0, 0x0486, "query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_MSG,          0, 0x0487, "invalid query message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_SORTED_RES_TOO_MANY,        0, 0x0488, "sorted res too many")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QHANDLE,            0, 0x0489, "invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CANCELLED,            0, 0x048A, "query cancelled")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_IE,                 0, 0x048B, "invalid ie")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VALUE,              0, 0x048C, "invalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_FQDN,               0, 0x048D, "invalid FQDN")

// others
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_FILE_FORMAT,        0, 0x0500, "invalid file format")

// TSDB
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CONFIG,             0, 0x0580, "invalid TSDB configuration")


#ifdef TAOS_ERROR_C
};
#endif


#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
