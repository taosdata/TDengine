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

#define TSDB_CODE_SUCCESS                    0
#define TSDB_CODE_ACTION_IN_PROGRESS         1

#define TSDB_CODE_LAST_SESSION_NOT_FINISHED  5
#define TSDB_CODE_INVALID_SESSION_ID         6
#define TSDB_CODE_INVALID_TRAN_ID            7
#define TSDB_CODE_INVALID_MSG_TYPE           8
#define TSDB_CODE_ALREADY_PROCESSED          9
#define TSDB_CODE_AUTH_FAILURE               10
#define TSDB_CODE_WRONG_MSG_SIZE             11
#define TSDB_CODE_UNEXPECTED_RESPONSE        12
#define TSDB_CODE_INVALID_RESPONSE_TYPE      13
#define TSDB_CODE_NO_RESOURCE                14
#define TSDB_CODE_INVALID_TIME_STAMP         15
#define TSDB_CODE_MISMATCHED_METER_ID        16
#define TSDB_CODE_ACTION_TRANS_NOT_FINISHED  17
#define TSDB_CODE_ACTION_NOT_ONLINE          18
#define TSDB_CODE_ACTION_SEND_FAILD          19
#define TSDB_CODE_NOT_ACTIVE_SESSION         20
#define TSDB_CODE_INVALID_VNODE_ID           21
#define TSDB_CODE_APP_ERROR                  22
#define TSDB_CODE_INVALID_IE                 23
#define TSDB_CODE_INVALID_VALUE              24
#define TSDB_CODE_REDIRECT                   25
#define TSDB_CODE_ALREADY_THERE              26
#define TSDB_CODE_INVALID_METER_ID           27
#define TSDB_CODE_INVALID_SQL                28
#define TSDB_CODE_NETWORK_UNAVAIL            29
#define TSDB_CODE_INVALID_MSG_LEN            30
#define TSDB_CODE_INVALID_DB                 31
#define TSDB_CODE_INVALID_TABLE              32
#define TSDB_CODE_DB_ALREADY_EXIST           33
#define TSDB_CODE_TABLE_ALREADY_EXIST        34
#define TSDB_CODE_INVALID_USER               35
#define TSDB_CODE_INVALID_ACCT               36
#define TSDB_CODE_INVALID_PASS               37
#define TSDB_CODE_DB_NOT_SELECTED            38
#define TSDB_CODE_MEMORY_CORRUPTED           39
#define TSDB_CODE_USER_ALREADY_EXIST         40
#define TSDB_CODE_NO_RIGHTS                  41
#define TSDB_CODE_DISCONNECTED               42
#define TSDB_CODE_NO_MASTER                  43
#define TSDB_CODE_NOT_CONFIGURED             44
#define TSDB_CODE_INVALID_OPTION             45
#define TSDB_CODE_NODE_OFFLINE               46
#define TSDB_CODE_SYNC_REQUIRED              47
#define TSDB_CODE_NO_ENOUGH_DNODES           48
#define TSDB_CODE_UNSYNCED                   49
#define TSDB_CODE_TOO_SLOW                   50
#define TSDB_CODE_OTHERS                     51
#define TSDB_CODE_NO_REMOVE_MASTER           52
#define TSDB_CODE_WRONG_SCHEMA               53
#define TSDB_CODE_NOT_ACTIVE_VNODE           54
#define TSDB_CODE_TOO_MANY_USERS             55
#define TSDB_CODE_TOO_MANY_DATABSES          56
#define TSDB_CODE_TOO_MANY_TABLES            57
#define TSDB_CODE_TOO_MANY_DNODES            58
#define TSDB_CODE_TOO_MANY_ACCTS             59
#define TSDB_CODE_ACCT_ALREADY_EXIST         60
#define TSDB_CODE_DNODE_ALREADY_EXIST        61
#define TSDB_CODE_SDB_ERROR                  62
#define TSDB_CODE_METRICMETA_EXPIRED         63    // local cached metric-meta expired causes error in metric query
#define TSDB_CODE_NOT_READY                  64    // peer is not ready to process data
#define TSDB_CODE_MAX_SESSIONS               65    // too many sessions
#define TSDB_CODE_MAX_CONNECTIONS            66    // too many connections
#define TSDB_CODE_SESSION_ALREADY_EXIST      67
#define TSDB_CODE_NO_QSUMMARY                68
#define TSDB_CODE_SERV_OUT_OF_MEMORY         69
#define TSDB_CODE_INVALID_QHANDLE            70
#define TSDB_CODE_RELATED_TABLES_EXIST       71
#define TSDB_CODE_MONITOR_DB_FORBEIDDEN      72
#define TSDB_CODE_VG_COMMITLOG_INIT_FAILED   73
#define TSDB_CODE_VG_INIT_FAILED             74
#define TSDB_CODE_DATA_ALREADY_IMPORTED      75
#define TSDB_CODE_OPS_NOT_SUPPORT            76
#define TSDB_CODE_INVALID_QUERY_ID           77
#define TSDB_CODE_INVALID_STREAM_ID          78
#define TSDB_CODE_INVALID_CONNECTION         79
#define TSDB_CODE_ACTION_NOT_BALANCED        80
#define TSDB_CODE_CLI_OUT_OF_MEMORY          81
#define TSDB_CODE_DATA_OVERFLOW              82
#define TSDB_CODE_QUERY_CANCELLED            83
#define TSDB_CODE_GRANT_TIMESERIES_LIMITED   84
#define TSDB_CODE_GRANT_EXPIRED              85
#define TSDB_CODE_CLI_NO_DISKSPACE           86
#define TSDB_CODE_FILE_CORRUPTED             87
#define TSDB_CODE_INVALID_CLIENT_VERSION     88
#define TSDB_CODE_INVALID_ACCT_PARAMETER     89
#define TSDB_CODE_NOT_ENOUGH_TIME_SERIES     90
#define TSDB_CODE_NO_WRITE_ACCESS            91
#define TSDB_CODE_NO_READ_ACCESS             92
#define TSDB_CODE_GRANT_DB_LIMITED           93
#define TSDB_CODE_GRANT_USER_LIMITED         94
#define TSDB_CODE_GRANT_CONN_LIMITED         95
#define TSDB_CODE_GRANT_STREAM_LIMITED       96
#define TSDB_CODE_GRANT_SPEED_LIMITED        97
#define TSDB_CODE_GRANT_STORAGE_LIMITED      98
#define TSDB_CODE_GRANT_QUERYTIME_LIMITED    99
#define TSDB_CODE_GRANT_ACCT_LIMITED         100
#define TSDB_CODE_GRANT_DNODE_LIMITED        101
#define TSDB_CODE_GRANT_CPU_LIMITED          102
#define TSDB_CODE_SESSION_NOT_READY          103      // table NOT in ready state
#define TSDB_CODE_BATCH_SIZE_TOO_BIG         104
#define TSDB_CODE_TIMESTAMP_OUT_OF_RANGE     105
#define TSDB_CODE_INVALID_QUERY_MSG          106      // failed to validate the sql expression msg by vnode
#define TSDB_CODE_CACHE_BLOCK_TS_DISORDERED  107      // time stamp in cache block is disordered
#define TSDB_CODE_FILE_BLOCK_TS_DISORDERED   108      // time stamp in file block is disordered
#define TSDB_CODE_INVALID_COMMIT_LOG         109      // commit log init failed
#define TSDB_CODE_SERVER_NO_SPACE            110
#define TSDB_CODE_NOT_SUPER_TABLE            111      // operation only available for super table
#define TSDB_CODE_DUPLICATE_TAGS             112      // tags value for join not unique
#define TSDB_CODE_INVALID_SUBMIT_MSG         113
#define TSDB_CODE_NOT_ACTIVE_TABLE           114
#define TSDB_CODE_INVALID_TABLE_ID           115

#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
