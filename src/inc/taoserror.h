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

TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_IN_PROGRESS,         0, 1, "action in progress")
TAOS_DEFINE_ERROR(TSDB_CODE_MSG_NOT_PROCESSED,          0, 4, "message not processed")
TAOS_DEFINE_ERROR(TSDB_CODE_LAST_SESSION_NOT_FINISHED,  0, 5, "last session not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SESSION_ID,         0, 6, "invalid session id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TRAN_ID,            0, 7, "invalid transaction id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_TYPE,           0, 8, "invalid message type")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_PROCESSED,          0, 9, "already processed")
TAOS_DEFINE_ERROR(TSDB_CODE_AUTH_FAILURE,               0, 10, "auth failure")
TAOS_DEFINE_ERROR(TSDB_CODE_WRONG_MSG_SIZE,             0, 11, "wrong message size")
TAOS_DEFINE_ERROR(TSDB_CODE_UNEXPECTED_RESPONSE,        0, 12, "unexpected response")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_RESPONSE_TYPE,      0, 13, "invalid response type")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RESOURCE,                0, 14, "no resource")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TIME_STAMP,         0, 15, "invalid timestamp")
TAOS_DEFINE_ERROR(TSDB_CODE_MISMATCHED_METER_ID,        0, 16, "mismatched meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_TRANS_NOT_FINISHED,  0, 17, "action transaction not finished")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_NOT_ONLINE,          0, 18, "action not online")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_SEND_FAILD,          0, 19, "action send failed")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_SESSION,         0, 20, "not active session")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VNODE_ID,           0, 21, "invalid vnode id")
TAOS_DEFINE_ERROR(TSDB_CODE_APP_ERROR,                  0, 22, "app error")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_IE,                 0, 23, "invalid ie")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VALUE,              0, 24, "invalid value")
TAOS_DEFINE_ERROR(TSDB_CODE_REDIRECT,                   0, 25, "redirect")
TAOS_DEFINE_ERROR(TSDB_CODE_ALREADY_THERE,              0, 26, "already there")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_METER_ID,           0, 27, "invalid meter id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SQL,                0, 28, "invalid sql")
TAOS_DEFINE_ERROR(TSDB_CODE_NETWORK_UNAVAIL,            0, 29, "network unavailable")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_LEN,            0, 30, "invalid message length")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_DB,                 0, 31, "invalid database")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE,              0, 32, "invalid table")
TAOS_DEFINE_ERROR(TSDB_CODE_DB_ALREADY_EXIST,           0, 33, "database aleady exist")
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ALREADY_EXIST,        0, 34, "table already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_USER,               0, 35, "invalid user")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT,               0, 36, "invalid account")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_PASS,               0, 37, "invalid password")
TAOS_DEFINE_ERROR(TSDB_CODE_DB_NOT_SELECTED,            0, 38, "db not selected")
TAOS_DEFINE_ERROR(TSDB_CODE_MEMORY_CORRUPTED,           0, 39, "memory corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_USER_ALREADY_EXIST,         0, 40, "user already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_RIGHTS,                  0, 41, "no rights")
TAOS_DEFINE_ERROR(TSDB_CODE_DISCONNECTED,               0, 42, "disconnected")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_MASTER,                  0, 43, "no master")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_CONFIGURED,             0, 44, "not configured")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_OPTION,             0, 45, "invalid option")
TAOS_DEFINE_ERROR(TSDB_CODE_NODE_OFFLINE,               0, 46, "node offline")
TAOS_DEFINE_ERROR(TSDB_CODE_SYNC_REQUIRED,              0, 47, "sync required")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_ENOUGH_DNODES,           0, 48, "no enough dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_UNSYNCED,                   0, 49, "unsyned")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_SLOW,                   0, 50, "too slow")
TAOS_DEFINE_ERROR(TSDB_CODE_OTHERS,                     0, 51, "others")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_REMOVE_MASTER,           0, 52, "no remove master")
TAOS_DEFINE_ERROR(TSDB_CODE_WRONG_SCHEMA,               0, 53, "wrong schema")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_VNODE,           0, 54, "not active vnode")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_USERS,             0, 55, "too many users")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_DATABASES,         0, 56, "too many databases")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_TABLES,            0, 57, "too many tables")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_DNODES,            0, 58, "too many dnodes")
TAOS_DEFINE_ERROR(TSDB_CODE_TOO_MANY_ACCTS,             0, 59, "too many accounts")
TAOS_DEFINE_ERROR(TSDB_CODE_ACCT_ALREADY_EXIST,         0, 60, "accounts already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_ALREADY_EXIST,        0, 61, "dnode already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_SDB_ERROR,                  0, 62, "sdb error")
TAOS_DEFINE_ERROR(TSDB_CODE_METRICMETA_EXPIRED,         0, 63, "metricmeta expired")    // local cached metric-meta expired causes error in metric query
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_READY,                  0, 64, "not ready")    // peer is not ready to process data
TAOS_DEFINE_ERROR(TSDB_CODE_MAX_SESSIONS,               0, 65, "max sessions")    // too many sessions
TAOS_DEFINE_ERROR(TSDB_CODE_MAX_CONNECTIONS,            0, 66, "max connections")    // too many connections
TAOS_DEFINE_ERROR(TSDB_CODE_SESSION_ALREADY_EXIST,      0, 67, "session already exist")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_QSUMMARY,                0, 68, "no qsummery")
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_OUT_OF_MEMORY,         0, 69, "server out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QHANDLE,            0, 70, "invalid handle")
TAOS_DEFINE_ERROR(TSDB_CODE_RELATED_TABLES_EXIST,       0, 71, "related tables exist")
TAOS_DEFINE_ERROR(TSDB_CODE_MONITOR_DB_FORBIDDEN,       0, 72, "monitor db forbidden")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_DISK_PERMISSIONS,        0, 73, "no disk permissions")
TAOS_DEFINE_ERROR(TSDB_CODE_VG_INIT_FAILED,             0, 74, "vg init failed")
TAOS_DEFINE_ERROR(TSDB_CODE_DATA_ALREADY_IMPORTED,      0, 75, "data already imported")
TAOS_DEFINE_ERROR(TSDB_CODE_OPS_NOT_SUPPORT,            0, 76, "operations not support")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_ID,           0, 77, "invalid query id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_STREAM_ID,          0, 78, "invalid stream id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CONNECTION,         0, 79, "invalid connection")
TAOS_DEFINE_ERROR(TSDB_CODE_ACTION_NOT_BALANCED,        0, 80, "action not balanced")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_OUT_OF_MEMORY,          0, 81, "client out of memory")
TAOS_DEFINE_ERROR(TSDB_CODE_DATA_OVERFLOW,              0, 82, "data overflow")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CANCELLED,            0, 83, "query cancelled")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_TIMESERIES_LIMITED,   0, 84, "grant timeseries limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_EXPIRED,              0, 85, "grant expired")
TAOS_DEFINE_ERROR(TSDB_CODE_CLI_NO_DISKSPACE,           0, 86, "client no disk space")
TAOS_DEFINE_ERROR(TSDB_CODE_FILE_CORRUPTED,             0, 87, "file corrupted")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_CLIENT_VERSION,     0, 88, "invalid client version")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_ACCT_PARAMETER,     0, 89, "invalid account parameter")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ENOUGH_TIME_SERIES,     0, 90, "not enough time series")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_WRITE_ACCESS,            0, 91, "no write access")
TAOS_DEFINE_ERROR(TSDB_CODE_NO_READ_ACCESS,             0, 92, "no read access")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DB_LIMITED,           0, 93, "grant db limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_USER_LIMITED,         0, 94, "grant user limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CONN_LIMITED,         0, 95, "grant conn limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STREAM_LIMITED,       0, 96, "grant stream limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_SPEED_LIMITED,        0, 97, "grant speed limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_STORAGE_LIMITED,      0, 98, "grant storage limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_QUERYTIME_LIMITED,    0, 99, "grant query time limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_ACCT_LIMITED,         0, 100, "grant account limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_DNODE_LIMITED,        0, 101, "grant dnode limited")
TAOS_DEFINE_ERROR(TSDB_CODE_GRANT_CPU_LIMITED,          0, 102, "grant cpu limited")
TAOS_DEFINE_ERROR(TSDB_CODE_SESSION_NOT_READY,          0, 103, "session not ready")        // table NOT in ready state
TAOS_DEFINE_ERROR(TSDB_CODE_BATCH_SIZE_TOO_BIG,         0, 104, "batch size too big")
TAOS_DEFINE_ERROR(TSDB_CODE_TIMESTAMP_OUT_OF_RANGE,     0, 105, "timestamp out of range")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_QUERY_MSG,          0, 106, "invalid query message")    // failed to validate the sql expression msg by vnode
TAOS_DEFINE_ERROR(TSDB_CODE_SORTED_RES_TOO_MANY,        0, 107, "sorted res too many")      // too many result for ordered super table projection query
TAOS_DEFINE_ERROR(TSDB_CODE_FILE_BLOCK_TS_DISORDERED,   0, 108, "file block ts disordered") // time stamp in file block is disordered
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_COMMIT_LOG,         0, 109, "invalid commit log")       // commit log init failed
TAOS_DEFINE_ERROR(TSDB_CODE_SERV_NO_DISKSPACE,          0, 110, "server no diskspace")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_SUPER_TABLE,            0, 111, "no super table")           // operation only available for super table
TAOS_DEFINE_ERROR(TSDB_CODE_DUPLICATE_TAGS,             0, 112, "duplicate tags")           // tags value for join not unique
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_SUBMIT_MSG,         0, 113, "invalid submit message")
TAOS_DEFINE_ERROR(TSDB_CODE_NOT_ACTIVE_TABLE,           0, 114, "not active table")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_ID,           0, 115, "invalid table id")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VNODE_STATUS,       0, 116, "invalid vnode status")
TAOS_DEFINE_ERROR(TSDB_CODE_FAILED_TO_LOCK_RESOURCES,   0, 117, "failed to lock resources")
TAOS_DEFINE_ERROR(TSDB_CODE_TABLE_ID_MISMATCH,          0, 118, "table id mismatch")
TAOS_DEFINE_ERROR(TSDB_CODE_QUERY_CACHE_ERASED,         0, 119, "query cache erased")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG,                0, 120, "invalid message")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_TABLE_TYPE,         0, 121, "invalid table typee")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_MSG_VERSION,        0, 122, "invalid version of message")
TAOS_DEFINE_ERROR(TSDB_CODE_DNODE_NOT_EXIST,            0, 123, "dnode not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_INVALID_VGROUP_ID,          0, 124, "invalid vgroup id")

#ifdef TAOS_ERROR_C
};
#endif

#define TSDB_CODE_MAX_ERROR_CODE             120

#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
