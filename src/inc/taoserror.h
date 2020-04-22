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
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_IN_PROGRESS,         0, 1, "action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_NEED_REPROCESSED,    0, 3, "action need to be reprocessed")
TAOS_DEFINE_ERROR(TSDB_CODE_MSG_NOT_PROCESSED,          0, 4, "message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_PROCESSED,          0, 5, "message already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_REDIRECT,                   0, 6, "redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_LAST_SESSION_NOT_FINISHED,  0, 7, "last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_MAX_SESSIONS,               0, 8, "max sessions")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SESSION_ID,         0, 9, "invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TRAN_ID,            0, 10, "invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_TYPE,           0, 11, "invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_LEN,            0, 12, "invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_CONTENT,        0, 13, "invalid message content")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_VERSION,        0, 14, "invalid message version")
TAOS_DEFINE_ERROR(TSDB_CODE_UNEXPECTED_RESPONSE,        0, 15, "unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_RESPONSE_TYPE,      0, 16, "invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_MISMATCHED_METER_ID,        0, 17, "mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_DISCONNECTED,               0, 18, "disconnected")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_READY,                  0, 19, "not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_SLOW,                   0, 20, "too slow")
TAOS_DEFINE_ERROR(TSDB_CODE_OTHERS,                     0, 21, "others")
TAOS_DEFINE_ERROR(TSDB_CODE_APP_ERROR,                  0, 22, "app error")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_THERE,              0, 23, "already there")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RESOURCE,                0, 14, "no resource")
TAOS_DEFINE_ERROR(TSDB_CODE_OPS_NOT_SUPPORT,            0, 25, "operations not support")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_OPTION,             0, 26, "invalid option")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_CONFIGURED,             0, 27, "not configured")
TAOS_DEFINE_ERROR(TSDB_CODE_NODE_OFFLINE,               0, 28, "node offline")
TAOS_DEFINE_ERROR(TSDB_CODE_NETWORK_UNAVAIL,            0, 29, "network unavailable")

// db
TAOS_DEFINE_ERROR(TSDB_CODE_DB_NOT_SELECTED,            0, 100, "db not selected")
TAOS_DEFINE_ERROR(TSDB_CODE_DB_ALREADY_EXIST,           0, 101, "database aleady exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_DB,                 0, 102, "invalid database")
TAOS_DEFINE_ERROR(TSDB_CODE_MONITOR_DB_FORBIDDEN,       0, 103, "monitor db forbidden")

// user
TAOS_DEFINE_ERROR(TSDB_CODE_USER_ALREADY_EXIST,         0, 150, "user already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_USER,               0, 151, "invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PASS,               0, 152, "invalid password")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_USER_FORMAT,        0, 153, "invalid user format")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PASS_FORMAT,        0, 154, "invalid password format")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_USER_FROM_CONN,          0, 155, "can not get user from conn")

// table
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ALREADY_EXIST,        0, 200, "table already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_ID,           0, 201, "invalid table id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_TYPE,         0, 202, "invalid table typee")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE,              0, 203, "invalid table name")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_SUPER_TABLE,            0, 204, "no super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_TABLE,           0, 205, "not active table")
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ID_MISMATCH,          0, 206, "table id mismatch")

// dnode & mnode
TAOS_DEFINE_ERROR(TSDB_CODE_NO_ENOUGH_DNODES,           0, 300, "no enough dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_ALREADY_EXIST,        0, 301, "dnode already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_NOT_EXIST,            0, 302, "dnode not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_MASTER,                  0, 303, "no master")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_REMOVE_MASTER,           0, 304, "no remove master")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_ID,           0, 305, "invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_STREAM_ID,          0, 306, "invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CONNECTION,         0, 307, "invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_ERROR,                  0, 308, "sdb error")

// acct
TAOS_DEFINE_ERROR(TSDB_CODE_ACCT_ALREADY_EXIST,         0, 400, "accounts already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT,               0, 401, "invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT_PARAMETER,     0, 402, "invalid account parameter")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_ACCTS,             0, 403, "too many accounts")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_USERS,             0, 404, "too many users")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TABLES,            0, 405, "too many tables")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_DATABASES,         0, 406, "too many databases")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TIME_SERIES,       0, 407, "not enough time series")

// grant
TAOS_DEFINE_ERROR(TSDB_CODE_AUTH_FAILURE,               0, 400, "auth failure")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RIGHTS,                  0, 401, "no rights")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_WRITE_ACCESS,            0, 402, "no write access")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_READ_ACCESS,             0, 403, "no read access")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,              0, 404, "grant expired")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,        0, 405, "grant dnode limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,         0, 406, "grant account limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,   0, 407, "grant timeseries limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,           0, 408, "grant db limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,         0, 409, "grant user limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,         0, 410, "grant conn limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,       0, 411, "grant stream limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,        0, 412, "grant speed limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,      0, 413, "grant storage limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,    0, 414, "grant query time limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,          0, 415, "grant cpu limited")

// server
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VGROUP_ID,          0, 500, "invalid vgroup id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VNODE_ID,           0, 501, "invalid vnode id")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_VNODE,           0, 502, "not active vnode")
TAOS_DEFINE_ERROR(TSDB_CODE_VG_INIT_FAILED,             0, 503, "vg init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_NO_DISKSPACE,          0, 504, "server no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_OUT_OF_MEMORY,         0, 505, "server out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_DISK_PERMISSIONS,        0, 506, "no disk permissions")
TAOS_DEFINE_ERROR(TSDB_CODE_FILE_CORRUPTED,             0, 507, "file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_MEMORY_CORRUPTED,           0, 508, "memory corrupted")

// client
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CLIENT_VERSION,     0, 601, "invalid client version")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_OUT_OF_MEMORY,          0, 602, "client out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_NO_DISKSPACE,           0, 603, "client no disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TIME_STAMP,         0, 604, "invalid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SQL,                0, 605, "invalid sql")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CACHE_ERASED,         0, 606, "query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_MSG,          0, 607, "invalid query message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_SORTED_RES_TOO_MANY,        0, 608, "sorted res too many")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QHANDLE,            0, 609, "invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CANCELLED,            0, 610, "query cancelled")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_IE,                 0, 611, "invalid ie")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VALUE,              0, 612, "invalid value")

// others
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_FILE_FORMAT,        0, 700, "invalid file format")


#ifdef TAOS_ERROR_C
};
#endif

#define TSDB_CODE_MAX_ERROR_CODE             120

#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
