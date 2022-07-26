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

#define TAOS_DEF_ERROR_CODE(mod, code) ((int32_t)((0x80000000 | ((mod)<<16) | (code))))

#define TAOS_SYSTEM_ERROR(code)             (0x80ff0000 | (code))
#define TAOS_SUCCEEDED(err)                 ((err) >= 0)
#define TAOS_FAILED(err)                    ((err) < 0)

const char* tstrerror(int32_t err);

int32_t* taosGetErrno();
#define terrno                              (*taosGetErrno())

#define TSDB_CODE_SUCCESS                   0
#define TSDB_CODE_FAILED                    -1   // unknown or needn't tell detail error

// rpc
#define TSDB_CODE_RPC_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0001)  //"Action in progress"
#define TSDB_CODE_RPC_AUTH_REQUIRED             TAOS_DEF_ERROR_CODE(0, 0x0002)  //"Authentication required"
#define TSDB_CODE_RPC_AUTH_FAILURE              TAOS_DEF_ERROR_CODE(0, 0x0003)  //"Authentication failure"
#define TSDB_CODE_RPC_REDIRECT                  TAOS_DEF_ERROR_CODE(0, 0x0004)  //"Redirect"
#define TSDB_CODE_RPC_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0005)  //"System not ready"    // peer is not ready to process data
#define TSDB_CODE_RPC_ALREADY_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0006)  //"Message already processed"
#define TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED TAOS_DEF_ERROR_CODE(0, 0x0007)  //"Last session not finished"
#define TSDB_CODE_RPC_MISMATCHED_LINK_ID        TAOS_DEF_ERROR_CODE(0, 0x0008)  //"Mismatched meter id"
#define TSDB_CODE_RPC_TOO_SLOW                  TAOS_DEF_ERROR_CODE(0, 0x0009)  //"Processing of request timed out"
#define TSDB_CODE_RPC_MAX_SESSIONS              TAOS_DEF_ERROR_CODE(0, 0x000A)  //"Number of sessions reached limit"    // too many sessions
#define TSDB_CODE_RPC_NETWORK_UNAVAIL           TAOS_DEF_ERROR_CODE(0, 0x000B)  //"Unable to establish connection"
#define TSDB_CODE_RPC_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x000C)  //"Unexpected generic error in RPC"
#define TSDB_CODE_RPC_UNEXPECTED_RESPONSE       TAOS_DEF_ERROR_CODE(0, 0x000D)  //"Unexpected response"
#define TSDB_CODE_RPC_INVALID_VALUE             TAOS_DEF_ERROR_CODE(0, 0x000E)  //"Invalid value"
#define TSDB_CODE_RPC_INVALID_TRAN_ID           TAOS_DEF_ERROR_CODE(0, 0x000F)  //"Invalid transaction id"
#define TSDB_CODE_RPC_INVALID_SESSION_ID        TAOS_DEF_ERROR_CODE(0, 0x0010)  //"Invalid session id"
#define TSDB_CODE_RPC_INVALID_MSG_TYPE          TAOS_DEF_ERROR_CODE(0, 0x0011)  //"Invalid message type"
#define TSDB_CODE_RPC_INVALID_RESPONSE_TYPE     TAOS_DEF_ERROR_CODE(0, 0x0012)  //"Invalid response type"
#define TSDB_CODE_RPC_INVALID_TIME_STAMP        TAOS_DEF_ERROR_CODE(0, 0x0013)  //"Client and server's time is not synchronized"
#define TSDB_CODE_APP_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0014)  //"Database not ready"
#define TSDB_CODE_RPC_FQDN_ERROR                TAOS_DEF_ERROR_CODE(0, 0x0015)  //"Unable to resolve FQDN"
#define TSDB_CODE_RPC_INVALID_VERSION           TAOS_DEF_ERROR_CODE(0, 0x0016)  //"Invalid app version"
#define TSDB_CODE_RPC_SHORTCUT                  TAOS_DEF_ERROR_CODE(0, 0x0017)  //"Shortcut"

//common & util
#define TSDB_CODE_COM_OPS_NOT_SUPPORT           TAOS_DEF_ERROR_CODE(0, 0x0100)  //"Operation not supported"
#define TSDB_CODE_COM_MEMORY_CORRUPTED          TAOS_DEF_ERROR_CODE(0, 0x0101)  //"Memory corrupted"
#define TSDB_CODE_COM_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0102)  //"Out of memory"
#define TSDB_CODE_COM_INVALID_CFG_MSG           TAOS_DEF_ERROR_CODE(0, 0x0103)  //"Invalid config message"
#define TSDB_CODE_COM_FILE_CORRUPTED            TAOS_DEF_ERROR_CODE(0, 0x0104)  //"Data file corrupted"
#define TSDB_CODE_REF_NO_MEMORY                 TAOS_DEF_ERROR_CODE(0, 0x0105)  //"Ref out of memory"
#define TSDB_CODE_REF_FULL                      TAOS_DEF_ERROR_CODE(0, 0x0106)  //"too many Ref Objs"
#define TSDB_CODE_REF_ID_REMOVED                TAOS_DEF_ERROR_CODE(0, 0x0107)  //"Ref ID is removed"
#define TSDB_CODE_REF_INVALID_ID                TAOS_DEF_ERROR_CODE(0, 0x0108)  //"Invalid Ref ID"
#define TSDB_CODE_REF_ALREADY_EXIST             TAOS_DEF_ERROR_CODE(0, 0x0109)  //"Ref is already there"
#define TSDB_CODE_REF_NOT_EXIST                 TAOS_DEF_ERROR_CODE(0, 0x010A)  //"Ref is not there"

//client
#define TSDB_CODE_TSC_INVALID_OPERATION         TAOS_DEF_ERROR_CODE(0, 0x0200)  //"Invalid Operation")
#define TSDB_CODE_TSC_INVALID_QHANDLE           TAOS_DEF_ERROR_CODE(0, 0x0201)  //"Invalid qhandle")
#define TSDB_CODE_TSC_INVALID_TIME_STAMP        TAOS_DEF_ERROR_CODE(0, 0x0202)  //"Invalid combination of client/service time")
#define TSDB_CODE_TSC_INVALID_VALUE             TAOS_DEF_ERROR_CODE(0, 0x0203)  //"Invalid value in client")
#define TSDB_CODE_TSC_INVALID_VERSION           TAOS_DEF_ERROR_CODE(0, 0x0204)  //"Invalid client version")
#define TSDB_CODE_TSC_INVALID_IE                TAOS_DEF_ERROR_CODE(0, 0x0205)  //"Invalid client ie")
#define TSDB_CODE_TSC_INVALID_FQDN              TAOS_DEF_ERROR_CODE(0, 0x0206)  //"Invalid host name")
#define TSDB_CODE_TSC_INVALID_USER_LENGTH       TAOS_DEF_ERROR_CODE(0, 0x0207)  //"Invalid user name")
#define TSDB_CODE_TSC_INVALID_PASS_LENGTH       TAOS_DEF_ERROR_CODE(0, 0x0208)  //"Invalid password")
#define TSDB_CODE_TSC_INVALID_DB_LENGTH         TAOS_DEF_ERROR_CODE(0, 0x0209)  //"Database name too long")
#define TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH   TAOS_DEF_ERROR_CODE(0, 0x020A)  //"Table name too long")
#define TSDB_CODE_TSC_INVALID_CONNECTION        TAOS_DEF_ERROR_CODE(0, 0x020B)  //"Invalid connection")
#define TSDB_CODE_TSC_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x020C)  //"System out of memory")
#define TSDB_CODE_TSC_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x020D)  //"System out of disk space")
#define TSDB_CODE_TSC_QUERY_CACHE_ERASED        TAOS_DEF_ERROR_CODE(0, 0x020E)  //"Query cache erased")
#define TSDB_CODE_TSC_QUERY_CANCELLED           TAOS_DEF_ERROR_CODE(0, 0x020F)  //"Query terminated")
#define TSDB_CODE_TSC_SORTED_RES_TOO_MANY       TAOS_DEF_ERROR_CODE(0, 0x0210)  //"Result set too large to be sorted")      // too many result for ordered super table projection query
#define TSDB_CODE_TSC_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0211)  //"Application error")
#define TSDB_CODE_TSC_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0212)  //"Action in progress")
#define TSDB_CODE_TSC_DISCONNECTED              TAOS_DEF_ERROR_CODE(0, 0x0213)  //"Disconnected from service")
#define TSDB_CODE_TSC_NO_WRITE_AUTH             TAOS_DEF_ERROR_CODE(0, 0x0214)  //"No write permission")
#define TSDB_CODE_TSC_CONN_KILLED               TAOS_DEF_ERROR_CODE(0, 0x0215)  //"Connection killed")
#define TSDB_CODE_TSC_SQL_SYNTAX_ERROR          TAOS_DEF_ERROR_CODE(0, 0x0216)  //"Syntax error in SQL")
#define TSDB_CODE_TSC_DB_NOT_SELECTED           TAOS_DEF_ERROR_CODE(0, 0x0217)  //"Database not specified or available")
#define TSDB_CODE_TSC_INVALID_TABLE_NAME        TAOS_DEF_ERROR_CODE(0, 0x0218)  //"Table does not exist")
#define TSDB_CODE_TSC_EXCEED_SQL_LIMIT          TAOS_DEF_ERROR_CODE(0, 0x0219)  //"SQL statement too long check maxSQLLength config")
#define TSDB_CODE_TSC_FILE_EMPTY                TAOS_DEF_ERROR_CODE(0, 0x021A)  //"File is empty")
#define TSDB_CODE_TSC_LINE_SYNTAX_ERROR         TAOS_DEF_ERROR_CODE(0, 0x021B)  //"Syntax error in Line")
#define TSDB_CODE_TSC_NO_META_CACHED            TAOS_DEF_ERROR_CODE(0, 0x021C)  //"No table meta cached")
#define TSDB_CODE_TSC_DUP_COL_NAMES             TAOS_DEF_ERROR_CODE(0, 0x021D)  //"duplicated column names")
#define TSDB_CODE_TSC_INVALID_TAG_LENGTH        TAOS_DEF_ERROR_CODE(0, 0x021E)  //"Invalid tag length")
#define TSDB_CODE_TSC_INVALID_COLUMN_LENGTH     TAOS_DEF_ERROR_CODE(0, 0x021F)  //"Invalid column length")
#define TSDB_CODE_TSC_DUP_TAG_NAMES             TAOS_DEF_ERROR_CODE(0, 0x0220)  //"duplicated tag names")
#define TSDB_CODE_TSC_INVALID_JSON              TAOS_DEF_ERROR_CODE(0, 0x0221)  //"Invalid JSON format")
#define TSDB_CODE_TSC_INVALID_JSON_TYPE         TAOS_DEF_ERROR_CODE(0, 0x0222)  //"Invalid JSON data type")
#define TSDB_CODE_TSC_INVALID_JSON_CONFIG       TAOS_DEF_ERROR_CODE(0, 0x0223)  //"Invalid JSON configuration")
#define TSDB_CODE_TSC_VALUE_OUT_OF_RANGE        TAOS_DEF_ERROR_CODE(0, 0x0224)  //"Value out of range")
#define TSDB_CODE_TSC_INVALID_PROTOCOL_TYPE     TAOS_DEF_ERROR_CODE(0, 0x0225)  //"Invalid line protocol type")
#define TSDB_CODE_TSC_INVALID_PRECISION_TYPE    TAOS_DEF_ERROR_CODE(0, 0x0226)  //"Invalid timestamp precision type")
#define TSDB_CODE_TSC_RES_TOO_MANY              TAOS_DEF_ERROR_CODE(0, 0x0227)  //"Result set too large to be output")
#define TSDB_CODE_TSC_INVALID_SCHEMA_VERSION    TAOS_DEF_ERROR_CODE(0, 0x0228)  //"invalid table schema version")

// mnode
#define TSDB_CODE_MND_MSG_NOT_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0300)  //"Message not processed"
#define TSDB_CODE_MND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0301)  //"Message is progressing"
#define TSDB_CODE_MND_ACTION_NEED_REPROCESSED   TAOS_DEF_ERROR_CODE(0, 0x0302)  //"Messag need to be reprocessed"
#define TSDB_CODE_MND_NO_RIGHTS                 TAOS_DEF_ERROR_CODE(0, 0x0303)  //"Insufficient privilege for operation"
#define TSDB_CODE_MND_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0304)  //"Unexpected generic error in mnode"
#define TSDB_CODE_MND_INVALID_CONNECTION        TAOS_DEF_ERROR_CODE(0, 0x0305)  //"Invalid message connection"
#define TSDB_CODE_MND_INVALID_MSG_VERSION       TAOS_DEF_ERROR_CODE(0, 0x0306)  //"Incompatible protocol version"
#define TSDB_CODE_MND_INVALID_MSG_LEN           TAOS_DEF_ERROR_CODE(0, 0x0307)  //"Invalid message length"
#define TSDB_CODE_MND_INVALID_MSG_TYPE          TAOS_DEF_ERROR_CODE(0, 0x0308)  //"Invalid message type"
#define TSDB_CODE_MND_TOO_MANY_SHELL_CONNS      TAOS_DEF_ERROR_CODE(0, 0x0309)  //"Too many connections"
#define TSDB_CODE_MND_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x030A)  //"Out of memory in mnode"
#define TSDB_CODE_MND_INVALID_SHOWOBJ           TAOS_DEF_ERROR_CODE(0, 0x030B)  //"Data expired"
#define TSDB_CODE_MND_INVALID_QUERY_ID          TAOS_DEF_ERROR_CODE(0, 0x030C)  //"Invalid query id"
#define TSDB_CODE_MND_INVALID_STREAM_ID         TAOS_DEF_ERROR_CODE(0, 0x030D)  //"Invalid stream id"
#define TSDB_CODE_MND_INVALID_CONN_ID           TAOS_DEF_ERROR_CODE(0, 0x030E)  //"Invalid connection id"
#define TSDB_CODE_MND_MNODE_IS_RUNNING          TAOS_DEF_ERROR_CODE(0, 0x0310)  //"mnode is already running"
#define TSDB_CODE_MND_FAILED_TO_CONFIG_SYNC     TAOS_DEF_ERROR_CODE(0, 0x0311)  //"failed to config sync"
#define TSDB_CODE_MND_FAILED_TO_START_SYNC      TAOS_DEF_ERROR_CODE(0, 0x0312)  //"failed to start sync"
#define TSDB_CODE_MND_FAILED_TO_CREATE_DIR      TAOS_DEF_ERROR_CODE(0, 0x0313)  //"failed to create mnode dir"
#define TSDB_CODE_MND_FAILED_TO_INIT_STEP       TAOS_DEF_ERROR_CODE(0, 0x0314)  //"failed to init components"

#define TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE     TAOS_DEF_ERROR_CODE(0, 0x0320)  //"Object already there"
#define TSDB_CODE_MND_SDB_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0321)  //"Unexpected generic error in sdb"
#define TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE    TAOS_DEF_ERROR_CODE(0, 0x0322)  //"Invalid table type"
#define TSDB_CODE_MND_SDB_OBJ_NOT_THERE         TAOS_DEF_ERROR_CODE(0, 0x0323)  //"Object not there"
#define TSDB_CODE_MND_SDB_INVAID_META_ROW       TAOS_DEF_ERROR_CODE(0, 0x0324)  //"Invalid meta row"
#define TSDB_CODE_MND_SDB_INVAID_KEY_TYPE       TAOS_DEF_ERROR_CODE(0, 0x0325)  //"Invalid key type"

#define TSDB_CODE_MND_DNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0330)  //"DNode already exists"
#define TSDB_CODE_MND_DNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0331)  //"DNode does not exist"
#define TSDB_CODE_MND_VGROUP_NOT_EXIST          TAOS_DEF_ERROR_CODE(0, 0x0332)  //"VGroup does not exist"
#define TSDB_CODE_MND_NO_REMOVE_MASTER          TAOS_DEF_ERROR_CODE(0, 0x0333)  //"Master DNode cannot be removed"
#define TSDB_CODE_MND_NO_ENOUGH_DNODES          TAOS_DEF_ERROR_CODE(0, 0x0334)  //"Out of DNodes"
#define TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT  TAOS_DEF_ERROR_CODE(0, 0x0335)  //"Cluster cfg inconsistent"
#define TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION  TAOS_DEF_ERROR_CODE(0, 0x0336)  //"Invalid dnode cfg option"
#define TSDB_CODE_MND_BALANCE_ENABLED           TAOS_DEF_ERROR_CODE(0, 0x0337)  //"Balance already enabled"
#define TSDB_CODE_MND_VGROUP_NOT_IN_DNODE       TAOS_DEF_ERROR_CODE(0, 0x0338)  //"Vgroup not in dnode"
#define TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE   TAOS_DEF_ERROR_CODE(0, 0x0339)  //"Vgroup already in dnode"
#define TSDB_CODE_MND_DNODE_NOT_FREE            TAOS_DEF_ERROR_CODE(0, 0x033A)  //"Dnode not avaliable"
#define TSDB_CODE_MND_INVALID_CLUSTER_ID        TAOS_DEF_ERROR_CODE(0, 0x033B)  //"Cluster id not match"
#define TSDB_CODE_MND_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x033C)  //"Cluster not ready"
#define TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED   TAOS_DEF_ERROR_CODE(0, 0x033D)  //"Dnode Id not configured"
#define TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED   TAOS_DEF_ERROR_CODE(0, 0x033E)  //"Dnode Ep not configured"

#define TSDB_CODE_MND_ACCT_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0340)  //"Account already exists"
#define TSDB_CODE_MND_INVALID_ACCT              TAOS_DEF_ERROR_CODE(0, 0x0341)  //"Invalid account"
#define TSDB_CODE_MND_INVALID_ACCT_OPTION       TAOS_DEF_ERROR_CODE(0, 0x0342)  //"Invalid account options"
#define TSDB_CODE_MND_ACCT_EXPIRED              TAOS_DEF_ERROR_CODE(0, 0x0343)  //"Account authorization has expired"

#define TSDB_CODE_MND_USER_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0350)  //"User already exists"
#define TSDB_CODE_MND_INVALID_USER              TAOS_DEF_ERROR_CODE(0, 0x0351)  //"Invalid user"
#define TSDB_CODE_MND_INVALID_USER_FORMAT       TAOS_DEF_ERROR_CODE(0, 0x0352)  //"Invalid user format"
#define TSDB_CODE_MND_INVALID_PASS_FORMAT       TAOS_DEF_ERROR_CODE(0, 0x0353)  //"Invalid password format"
#define TSDB_CODE_MND_NO_USER_FROM_CONN         TAOS_DEF_ERROR_CODE(0, 0x0354)  //"Can not get user from conn"
#define TSDB_CODE_MND_TOO_MANY_USERS            TAOS_DEF_ERROR_CODE(0, 0x0355)  //"Too many users"

#define TSDB_CODE_MND_TABLE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0360)  //"Table already exists"
#define TSDB_CODE_MND_INVALID_TABLE_ID          TAOS_DEF_ERROR_CODE(0, 0x0361)  //"Table name too long"
#define TSDB_CODE_MND_INVALID_TABLE_NAME        TAOS_DEF_ERROR_CODE(0, 0x0362)  //"Table does not exist"
#define TSDB_CODE_MND_INVALID_TABLE_TYPE        TAOS_DEF_ERROR_CODE(0, 0x0363)  //"Invalid table type in tsdb"
#define TSDB_CODE_MND_TOO_MANY_TAGS             TAOS_DEF_ERROR_CODE(0, 0x0364)  //"Too many tags"
#define TSDB_CODE_MND_TOO_MANY_COLUMNS          TAOS_DEF_ERROR_CODE(0, 0x0365)  //"Too many columns"
#define TSDB_CODE_MND_TOO_MANY_TIMESERIES       TAOS_DEF_ERROR_CODE(0, 0x0366)  //"Too many time series"
#define TSDB_CODE_MND_NOT_SUPER_TABLE           TAOS_DEF_ERROR_CODE(0, 0x0367)  //"Not super table"           // operation only available for super table
#define TSDB_CODE_MND_COL_NAME_TOO_LONG         TAOS_DEF_ERROR_CODE(0, 0x0368)  //"Tag name too long"
#define TSDB_CODE_MND_TAG_ALREAY_EXIST          TAOS_DEF_ERROR_CODE(0, 0x0369)  //"Tag already exists"
#define TSDB_CODE_MND_TAG_NOT_EXIST             TAOS_DEF_ERROR_CODE(0, 0x036A)  //"Tag does not exist"
#define TSDB_CODE_MND_FIELD_ALREAY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x036B)  //"Field already exists"
#define TSDB_CODE_MND_FIELD_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x036C)  //"Field does not exist"
#define TSDB_CODE_MND_INVALID_STABLE_NAME       TAOS_DEF_ERROR_CODE(0, 0x036D)  //"Super table does not exist"
#define TSDB_CODE_MND_INVALID_CREATE_TABLE_MSG  TAOS_DEF_ERROR_CODE(0, 0x036E)  //"Invalid create table message"
#define TSDB_CODE_MND_EXCEED_MAX_ROW_BYTES      TAOS_DEF_ERROR_CODE(0, 0x036F)  //"Exceed max row bytes"

#define TSDB_CODE_MND_INVALID_FUNC_NAME         TAOS_DEF_ERROR_CODE(0, 0x0370)  //"Invalid func name"
#define TSDB_CODE_MND_INVALID_FUNC_LEN          TAOS_DEF_ERROR_CODE(0, 0x0371)  //"Invalid func length"
#define TSDB_CODE_MND_INVALID_FUNC_CODE         TAOS_DEF_ERROR_CODE(0, 0x0372)  //"Invalid func code"
#define TSDB_CODE_MND_FUNC_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0373)  //"Func already exists"
#define TSDB_CODE_MND_INVALID_FUNC              TAOS_DEF_ERROR_CODE(0, 0x0374)  //"Invalid func"
#define TSDB_CODE_MND_INVALID_FUNC_BUFSIZE      TAOS_DEF_ERROR_CODE(0, 0x0375)  //"Invalid func bufSize"

#define TSDB_CODE_MND_INVALID_TAG_LENGTH        TAOS_DEF_ERROR_CODE(0, 0x0376)  //"invalid tag length"
#define TSDB_CODE_MND_INVALID_COLUMN_LENGTH     TAOS_DEF_ERROR_CODE(0, 0x0377)   //"invalid column length"

#define TSDB_CODE_MND_DB_NOT_SELECTED           TAOS_DEF_ERROR_CODE(0, 0x0380)  //"Database not specified or available"
#define TSDB_CODE_MND_DB_ALREADY_EXIST          TAOS_DEF_ERROR_CODE(0, 0x0381)  //"Database already exists"
#define TSDB_CODE_MND_INVALID_DB_OPTION         TAOS_DEF_ERROR_CODE(0, 0x0382)  //"Invalid database options"
#define TSDB_CODE_MND_INVALID_DB                TAOS_DEF_ERROR_CODE(0, 0x0383)  //"Invalid database name"
#define TSDB_CODE_MND_MONITOR_DB_FORBIDDEN      TAOS_DEF_ERROR_CODE(0, 0x0384)  //"Cannot delete monitor database"
#define TSDB_CODE_MND_TOO_MANY_DATABASES        TAOS_DEF_ERROR_CODE(0, 0x0385)  //"Too many databases for account"
#define TSDB_CODE_MND_DB_IN_DROPPING            TAOS_DEF_ERROR_CODE(0, 0x0386)  //"Database not available"
#define TSDB_CODE_MND_VGROUP_NOT_READY          TAOS_DEF_ERROR_CODE(0, 0x0387)  //"Database unsynced"

#define TSDB_CODE_MND_INVALID_DB_OPTION_DAYS    TAOS_DEF_ERROR_CODE(0, 0x0390)  //"Invalid database option: days out of range"
#define TSDB_CODE_MND_INVALID_DB_OPTION_KEEP    TAOS_DEF_ERROR_CODE(0, 0x0391)  //"Invalid database option: keep >= keep1 >= keep0 >= days"

#define TSDB_CODE_MND_INVALID_TOPIC             TAOS_DEF_ERROR_CODE(0, 0x0392)  //"Invalid topic name)
#define TSDB_CODE_MND_INVALID_TOPIC_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0393)  //"Invalid topic option)
#define TSDB_CODE_MND_INVALID_TOPIC_PARTITONS   TAOS_DEF_ERROR_CODE(0, 0x0394)  //"Invalid topic partitons num, valid range: [1, 1000])
#define TSDB_CODE_MND_TOPIC_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0395)  //"Topic already exists)

// dnode
#define TSDB_CODE_DND_MSG_NOT_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0400)  //"Message not processed"
#define TSDB_CODE_DND_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0401)  //"Dnode out of memory"
#define TSDB_CODE_DND_NO_WRITE_ACCESS           TAOS_DEF_ERROR_CODE(0, 0x0402)  //"No permission for disk files in dnode"
#define TSDB_CODE_DND_INVALID_MSG_LEN           TAOS_DEF_ERROR_CODE(0, 0x0403)  //"Invalid message length"
#define TSDB_CODE_DND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0404)  //"Action in progress"
#define TSDB_CODE_DND_TOO_MANY_VNODES           TAOS_DEF_ERROR_CODE(0, 0x0405)  //"Too many vnode directories"
#define TSDB_CODE_DND_EXITING                   TAOS_DEF_ERROR_CODE(0, 0x0406)  //"Dnode is exiting"
#define TSDB_CODE_DND_VNODE_OPEN_FAILED         TAOS_DEF_ERROR_CODE(0, 0x0407)  //"Vnode open failed"

// vnode
#define TSDB_CODE_VND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0500)  //"Action in progress"
#define TSDB_CODE_VND_MSG_NOT_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0501)  //"Message not processed"
#define TSDB_CODE_VND_ACTION_NEED_REPROCESSED   TAOS_DEF_ERROR_CODE(0, 0x0502)  //"Action need to be reprocessed"
#define TSDB_CODE_VND_INVALID_VGROUP_ID         TAOS_DEF_ERROR_CODE(0, 0x0503)  //"Invalid Vgroup ID"
#define TSDB_CODE_VND_INIT_FAILED               TAOS_DEF_ERROR_CODE(0, 0x0504)  //"Vnode initialization failed"
#define TSDB_CODE_VND_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x0505)  //"System out of disk space"
#define TSDB_CODE_VND_NO_DISK_PERMISSIONS       TAOS_DEF_ERROR_CODE(0, 0x0506)  //"No write permission for disk files"
#define TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR       TAOS_DEF_ERROR_CODE(0, 0x0507)  //"Missing data file"
#define TSDB_CODE_VND_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0508)  //"Out of memory"
#define TSDB_CODE_VND_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0509)  //"Unexpected generic error in vnode"
#define TSDB_CODE_VND_INVALID_VRESION_FILE      TAOS_DEF_ERROR_CODE(0, 0x050A)  //"Invalid version file"
#define TSDB_CODE_VND_IS_FULL                   TAOS_DEF_ERROR_CODE(0, 0x050B)  //"Database memory is full for commit failed"
#define TSDB_CODE_VND_IS_FLOWCTRL               TAOS_DEF_ERROR_CODE(0, 0x050C)  //"Database memory is full for waiting commit"
#define TSDB_CODE_VND_IS_DROPPING               TAOS_DEF_ERROR_CODE(0, 0x050D)  //"Database is dropping"
#define TSDB_CODE_VND_IS_BALANCING              TAOS_DEF_ERROR_CODE(0, 0x050E)  //"Database is balancing"
#define TSDB_CODE_VND_IS_CLOSING                TAOS_DEF_ERROR_CODE(0, 0x0510)  //"Database is closing"
#define TSDB_CODE_VND_NOT_SYNCED                TAOS_DEF_ERROR_CODE(0, 0x0511)  //"Database suspended"
#define TSDB_CODE_VND_NO_WRITE_AUTH             TAOS_DEF_ERROR_CODE(0, 0x0512)  //"Database write operation denied"
#define TSDB_CODE_VND_IS_SYNCING                TAOS_DEF_ERROR_CODE(0, 0x0513)  //"Database is syncing"
#define TSDB_CODE_VND_INVALID_TSDB_STATE        TAOS_DEF_ERROR_CODE(0, 0x0514)  //"Invalid tsdb state"
#define TSDB_CODE_WAIT_THREAD_TOO_MANY          TAOS_DEF_ERROR_CODE(0, 0x0515) //"Wait threads too many"

// tsdb
#define TSDB_CODE_TDB_INVALID_TABLE_ID          TAOS_DEF_ERROR_CODE(0, 0x0600)  //"Invalid table ID")
#define TSDB_CODE_TDB_INVALID_TABLE_TYPE        TAOS_DEF_ERROR_CODE(0, 0x0601)  //"Invalid table type")
#define TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION     TAOS_DEF_ERROR_CODE(0, 0x0602)  //"Invalid table schema version")
#define TSDB_CODE_TDB_TABLE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0603)  //"Table already exists")
#define TSDB_CODE_TDB_INVALID_CONFIG            TAOS_DEF_ERROR_CODE(0, 0x0604)  //"Invalid configuration")
#define TSDB_CODE_TDB_INIT_FAILED               TAOS_DEF_ERROR_CODE(0, 0x0605)  //"Tsdb init failed")
#define TSDB_CODE_TDB_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x0606)  //"No diskspace for tsdb")
#define TSDB_CODE_TDB_NO_DISK_PERMISSIONS       TAOS_DEF_ERROR_CODE(0, 0x0607)  //"No permission for disk files")
#define TSDB_CODE_TDB_FILE_CORRUPTED            TAOS_DEF_ERROR_CODE(0, 0x0608)  //"Data file(s) corrupted")
#define TSDB_CODE_TDB_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0609)  //"Out of memory")
#define TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE       TAOS_DEF_ERROR_CODE(0, 0x060A)  //"Tag too old")
#define TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE    TAOS_DEF_ERROR_CODE(0, 0x060B)  //"Timestamp data out of range")
#define TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP       TAOS_DEF_ERROR_CODE(0, 0x060C)  //"Submit message is messed up")
#define TSDB_CODE_TDB_INVALID_ACTION            TAOS_DEF_ERROR_CODE(0, 0x060D)  //"Invalid operation")
#define TSDB_CODE_TDB_INVALID_CREATE_TB_MSG     TAOS_DEF_ERROR_CODE(0, 0x060E)  //"Invalid creation of table")
#define TSDB_CODE_TDB_NO_TABLE_DATA_IN_MEM      TAOS_DEF_ERROR_CODE(0, 0x060F)  //"No table data in memory skiplist")
#define TSDB_CODE_TDB_FILE_ALREADY_EXISTS       TAOS_DEF_ERROR_CODE(0, 0x0610)  //"File already exists")
#define TSDB_CODE_TDB_TABLE_RECONFIGURE         TAOS_DEF_ERROR_CODE(0, 0x0611)  //"Need to reconfigure table")
#define TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO     TAOS_DEF_ERROR_CODE(0, 0x0612)  //"Invalid information to create table")
#define TSDB_CODE_TDB_NO_AVAIL_DISK             TAOS_DEF_ERROR_CODE(0, 0x0613)  //"No available disk")
#define TSDB_CODE_TDB_MESSED_MSG                TAOS_DEF_ERROR_CODE(0, 0x0614)  //"TSDB messed message")
#define TSDB_CODE_TDB_IVLD_TAG_VAL              TAOS_DEF_ERROR_CODE(0, 0x0615)  //"TSDB invalid tag value")
#define TSDB_CODE_TDB_NO_CACHE_LAST_ROW         TAOS_DEF_ERROR_CODE(0, 0x0616)  //"TSDB no cache last row data")
#define TSDB_CODE_TDB_INCOMPLETE_DFILESET       TAOS_DEF_ERROR_CODE(0, 0x0617)  //"TSDB incomplete DFileSet")

// query
#define TSDB_CODE_QRY_INVALID_QHANDLE           TAOS_DEF_ERROR_CODE(0, 0x0700)  //"Invalid handle")
#define TSDB_CODE_QRY_INVALID_MSG               TAOS_DEF_ERROR_CODE(0, 0x0701)  //"Invalid message")    // failed to validate the sql expression msg by vnode
#define TSDB_CODE_QRY_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x0702)  //"No diskspace for query")
#define TSDB_CODE_QRY_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0703)  //"System out of memory")
#define TSDB_CODE_QRY_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0704)  //"Unexpected generic error in query")
#define TSDB_CODE_QRY_DUP_JOIN_KEY              TAOS_DEF_ERROR_CODE(0, 0x0705)  //"Duplicated join key")
#define TSDB_CODE_QRY_EXCEED_TAGS_LIMIT         TAOS_DEF_ERROR_CODE(0, 0x0706)  //"Tag condition too many")
#define TSDB_CODE_QRY_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0707)  //"Query not ready")
#define TSDB_CODE_QRY_HAS_RSP                   TAOS_DEF_ERROR_CODE(0, 0x0708)  //"Query should response")
#define TSDB_CODE_QRY_IN_EXEC                   TAOS_DEF_ERROR_CODE(0, 0x0709)  //"Multiple retrieval of this query")
#define TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW       TAOS_DEF_ERROR_CODE(0, 0x070A)  //"Too many time window in query")
#define TSDB_CODE_QRY_NOT_ENOUGH_BUFFER         TAOS_DEF_ERROR_CODE(0, 0x070B)  //"Query buffer limit has reached")
#define TSDB_CODE_QRY_INCONSISTAN               TAOS_DEF_ERROR_CODE(0, 0x070C)  //"File inconsistency in replica")
#define TSDB_CODE_QRY_SYS_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x070D)  //"System error")
#define TSDB_CODE_QRY_INVALID_TIME_CONDITION    TAOS_DEF_ERROR_CODE(0, 0x070E)  //"invalid time condition")
#define TSDB_CODE_QRY_INVALID_SCHEMA_VERSION    TAOS_DEF_ERROR_CODE(0, 0x0710)  //"invalid schema version")
#define TSDB_CODE_QRY_RESULT_TOO_LARGE          TAOS_DEF_ERROR_CODE(0, 0x0711)  //"result num is too large")

// grant
#define TSDB_CODE_GRANT_EXPIRED                 TAOS_DEF_ERROR_CODE(0, 0x0800)  //"License expired"
#define TSDB_CODE_GRANT_DNODE_LIMITED           TAOS_DEF_ERROR_CODE(0, 0x0801)  //"DNode creation limited by licence"
#define TSDB_CODE_GRANT_ACCT_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0802)  //"Account creation limited by license"
#define TSDB_CODE_GRANT_TIMESERIES_LIMITED      TAOS_DEF_ERROR_CODE(0, 0x0803)  //"Table creation limited by license"
#define TSDB_CODE_GRANT_DB_LIMITED              TAOS_DEF_ERROR_CODE(0, 0x0804)  //"DB creation limited by license"
#define TSDB_CODE_GRANT_USER_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0805)  //"User creation limited by license"
#define TSDB_CODE_GRANT_CONN_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0806)  //"Conn creation limited by license"
#define TSDB_CODE_GRANT_STREAM_LIMITED          TAOS_DEF_ERROR_CODE(0, 0x0807)  //"Stream creation limited by license"
#define TSDB_CODE_GRANT_SPEED_LIMITED           TAOS_DEF_ERROR_CODE(0, 0x0808)  //"Write speed limited by license"
#define TSDB_CODE_GRANT_STORAGE_LIMITED         TAOS_DEF_ERROR_CODE(0, 0x0809)  //"Storage capacity limited by license"
#define TSDB_CODE_GRANT_QUERYTIME_LIMITED       TAOS_DEF_ERROR_CODE(0, 0x080A)  //"Query time limited by license"
#define TSDB_CODE_GRANT_CPU_LIMITED             TAOS_DEF_ERROR_CODE(0, 0x080B)  //"CPU cores limited by license"

// sync
#define TSDB_CODE_SYN_INVALID_CONFIG            TAOS_DEF_ERROR_CODE(0, 0x0900)  //"Invalid Sync Configuration"
#define TSDB_CODE_SYN_NOT_ENABLED               TAOS_DEF_ERROR_CODE(0, 0x0901)  //"Sync module not enabled"
#define TSDB_CODE_SYN_INVALID_VERSION           TAOS_DEF_ERROR_CODE(0, 0x0902)  //"Invalid Sync version"
#define TSDB_CODE_SYN_CONFIRM_EXPIRED           TAOS_DEF_ERROR_CODE(0, 0x0903)  //"Sync confirm expired"
#define TSDB_CODE_SYN_TOO_MANY_FWDINFO          TAOS_DEF_ERROR_CODE(0, 0x0904)  //"Too many sync fwd infos"
#define TSDB_CODE_SYN_MISMATCHED_PROTOCOL       TAOS_DEF_ERROR_CODE(0, 0x0905)  //"Mismatched protocol"
#define TSDB_CODE_SYN_MISMATCHED_CLUSTERID      TAOS_DEF_ERROR_CODE(0, 0x0906)  //"Mismatched clusterId"
#define TSDB_CODE_SYN_MISMATCHED_SIGNATURE      TAOS_DEF_ERROR_CODE(0, 0x0907)  //"Mismatched signature"
#define TSDB_CODE_SYN_INVALID_CHECKSUM          TAOS_DEF_ERROR_CODE(0, 0x0908)  //"Invalid msg checksum"
#define TSDB_CODE_SYN_INVALID_MSGLEN            TAOS_DEF_ERROR_CODE(0, 0x0909)  //"Invalid msg length"
#define TSDB_CODE_SYN_INVALID_MSGTYPE           TAOS_DEF_ERROR_CODE(0, 0x090A)  //"Invalid msg type"

// wal
#define TSDB_CODE_WAL_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x1000)  //"Unexpected generic error in wal"
#define TSDB_CODE_WAL_FILE_CORRUPTED            TAOS_DEF_ERROR_CODE(0, 0x1001)  //"WAL file is corrupted"
#define TSDB_CODE_WAL_SIZE_LIMIT                TAOS_DEF_ERROR_CODE(0, 0x1002)  //"WAL size exceeds limit"

// http
#define TSDB_CODE_HTTP_SERVER_OFFLINE           TAOS_DEF_ERROR_CODE(0, 0x1100)  //"http server is not online"
#define TSDB_CODE_HTTP_UNSUPPORT_URL            TAOS_DEF_ERROR_CODE(0, 0x1101)  //"url is not support"
#define TSDB_CODE_HTTP_INVALID_URL              TAOS_DEF_ERROR_CODE(0, 0x1102)  //invalid url format"
#define TSDB_CODE_HTTP_NO_ENOUGH_MEMORY         TAOS_DEF_ERROR_CODE(0, 0x1103)  //"no enough memory"
#define TSDB_CODE_HTTP_REQUSET_TOO_BIG          TAOS_DEF_ERROR_CODE(0, 0x1104)  //"request size is too big"
#define TSDB_CODE_HTTP_NO_AUTH_INFO             TAOS_DEF_ERROR_CODE(0, 0x1105)  //"no auth info input"
#define TSDB_CODE_HTTP_NO_MSG_INPUT             TAOS_DEF_ERROR_CODE(0, 0x1106)  //"request is empty"
#define TSDB_CODE_HTTP_NO_SQL_INPUT             TAOS_DEF_ERROR_CODE(0, 0x1107)  //"no sql input"
#define TSDB_CODE_HTTP_NO_EXEC_USEDB            TAOS_DEF_ERROR_CODE(0, 0x1108)  //"no need to execute use db cmd"
#define TSDB_CODE_HTTP_SESSION_FULL             TAOS_DEF_ERROR_CODE(0, 0x1109)  //"session list was full"
#define TSDB_CODE_HTTP_GEN_TAOSD_TOKEN_ERR      TAOS_DEF_ERROR_CODE(0, 0x110A)  //"generate taosd token error"
#define TSDB_CODE_HTTP_INVALID_MULTI_REQUEST    TAOS_DEF_ERROR_CODE(0, 0x110B)  //"size of multi request is 0"
#define TSDB_CODE_HTTP_CREATE_GZIP_FAILED       TAOS_DEF_ERROR_CODE(0, 0x110C)  //"failed to create gzip"
#define TSDB_CODE_HTTP_FINISH_GZIP_FAILED       TAOS_DEF_ERROR_CODE(0, 0x110D)  //"failed to finish gzip"
#define TSDB_CODE_HTTP_LOGIN_FAILED             TAOS_DEF_ERROR_CODE(0, 0x110E)  //"failed to login"

#define TSDB_CODE_HTTP_INVALID_VERSION          TAOS_DEF_ERROR_CODE(0, 0x1120)  //"invalid http version"
#define TSDB_CODE_HTTP_INVALID_CONTENT_LENGTH   TAOS_DEF_ERROR_CODE(0, 0x1121)  //"invalid content length"
#define TSDB_CODE_HTTP_INVALID_AUTH_TYPE        TAOS_DEF_ERROR_CODE(0, 0x1122)  //"invalid type of Authorization"
#define TSDB_CODE_HTTP_INVALID_AUTH_FORMAT      TAOS_DEF_ERROR_CODE(0, 0x1123)  //"invalid format of Authorization"
#define TSDB_CODE_HTTP_INVALID_BASIC_AUTH       TAOS_DEF_ERROR_CODE(0, 0x1124)  //"invalid basic Authorization"
#define TSDB_CODE_HTTP_INVALID_TAOSD_AUTH       TAOS_DEF_ERROR_CODE(0, 0x1125)  //"invalid taosd Authorization"
#define TSDB_CODE_HTTP_PARSE_METHOD_FAILED      TAOS_DEF_ERROR_CODE(0, 0x1126)  //"failed to parse method"
#define TSDB_CODE_HTTP_PARSE_TARGET_FAILED      TAOS_DEF_ERROR_CODE(0, 0x1127)  //"failed to parse target"
#define TSDB_CODE_HTTP_PARSE_VERSION_FAILED     TAOS_DEF_ERROR_CODE(0, 0x1128)  //"failed to parse http version"
#define TSDB_CODE_HTTP_PARSE_SP_FAILED          TAOS_DEF_ERROR_CODE(0, 0x1129)  //"failed to parse sp"
#define TSDB_CODE_HTTP_PARSE_STATUS_FAILED      TAOS_DEF_ERROR_CODE(0, 0x112A)  //"failed to parse status"
#define TSDB_CODE_HTTP_PARSE_PHRASE_FAILED      TAOS_DEF_ERROR_CODE(0, 0x112B)  //"failed to parse phrase"
#define TSDB_CODE_HTTP_PARSE_CRLF_FAILED        TAOS_DEF_ERROR_CODE(0, 0x112C)  //"failed to parse crlf"
#define TSDB_CODE_HTTP_PARSE_HEADER_FAILED      TAOS_DEF_ERROR_CODE(0, 0x112D)  //"failed to parse header"
#define TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED  TAOS_DEF_ERROR_CODE(0, 0x112E)  //"failed to parse header key"
#define TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED  TAOS_DEF_ERROR_CODE(0, 0x112F)  //"failed to parse header val"
#define TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED  TAOS_DEF_ERROR_CODE(0, 0x1130)  //"failed to parse chunk size"
#define TSDB_CODE_HTTP_PARSE_CHUNK_FAILED       TAOS_DEF_ERROR_CODE(0, 0x1131)  //"failed to parse chunk"
#define TSDB_CODE_HTTP_PARSE_END_FAILED         TAOS_DEF_ERROR_CODE(0, 0x1132)  //"failed to parse end section"
#define TSDB_CODE_HTTP_PARSE_INVALID_STATE      TAOS_DEF_ERROR_CODE(0, 0x1134)  //"invalid parse state"
#define TSDB_CODE_HTTP_PARSE_ERROR_STATE        TAOS_DEF_ERROR_CODE(0, 0x1135)  //"failed to parse error section"

#define TSDB_CODE_HTTP_GC_QUERY_NULL            TAOS_DEF_ERROR_CODE(0, 0x1150)  //"query size is 0"
#define TSDB_CODE_HTTP_GC_QUERY_SIZE            TAOS_DEF_ERROR_CODE(0, 0x1151)  //"query size can not more than 100"
#define TSDB_CODE_HTTP_GC_REQ_PARSE_ERROR       TAOS_DEF_ERROR_CODE(0, 0x1152)  //"parse grafana json error"

#define TSDB_CODE_HTTP_TG_DB_NOT_INPUT          TAOS_DEF_ERROR_CODE(0, 0x1160)  //"database name can not be null"
#define TSDB_CODE_HTTP_TG_DB_TOO_LONG           TAOS_DEF_ERROR_CODE(0, 0x1161)  //"database name too long"
#define TSDB_CODE_HTTP_TG_INVALID_JSON          TAOS_DEF_ERROR_CODE(0, 0x1162)  //"invalid telegraf json fromat"
#define TSDB_CODE_HTTP_TG_METRICS_NULL          TAOS_DEF_ERROR_CODE(0, 0x1163)  //"metrics size is 0"
#define TSDB_CODE_HTTP_TG_METRICS_SIZE          TAOS_DEF_ERROR_CODE(0, 0x1164)  //"metrics size can not more than 1K"
#define TSDB_CODE_HTTP_TG_METRIC_NULL           TAOS_DEF_ERROR_CODE(0, 0x1165)  //"metric name not find"
#define TSDB_CODE_HTTP_TG_METRIC_TYPE           TAOS_DEF_ERROR_CODE(0, 0x1166)  //"metric name type should be string"
#define TSDB_CODE_HTTP_TG_METRIC_NAME_NULL      TAOS_DEF_ERROR_CODE(0, 0x1167)  //"metric name length is 0"
#define TSDB_CODE_HTTP_TG_METRIC_NAME_LONG      TAOS_DEF_ERROR_CODE(0, 0x1168)  //"metric name length too long"
#define TSDB_CODE_HTTP_TG_TIMESTAMP_NULL        TAOS_DEF_ERROR_CODE(0, 0x1169)  //"timestamp not find"
#define TSDB_CODE_HTTP_TG_TIMESTAMP_TYPE        TAOS_DEF_ERROR_CODE(0, 0x116A)  //"timestamp type should be integer"
#define TSDB_CODE_HTTP_TG_TIMESTAMP_VAL_NULL    TAOS_DEF_ERROR_CODE(0, 0x116B)  //"timestamp value smaller than 0"
#define TSDB_CODE_HTTP_TG_TAGS_NULL             TAOS_DEF_ERROR_CODE(0, 0x116C)  //"tags not find"
#define TSDB_CODE_HTTP_TG_TAGS_SIZE_0           TAOS_DEF_ERROR_CODE(0, 0x116D)  //"tags size is 0"
#define TSDB_CODE_HTTP_TG_TAGS_SIZE_LONG        TAOS_DEF_ERROR_CODE(0, 0x116E)  //"tags size too long"
#define TSDB_CODE_HTTP_TG_TAG_NULL              TAOS_DEF_ERROR_CODE(0, 0x116F)  //"tag is null"
#define TSDB_CODE_HTTP_TG_TAG_NAME_NULL         TAOS_DEF_ERROR_CODE(0, 0x1170)  //"tag name is null"
#define TSDB_CODE_HTTP_TG_TAG_NAME_SIZE         TAOS_DEF_ERROR_CODE(0, 0x1171)  //"tag name length too long"
#define TSDB_CODE_HTTP_TG_TAG_VALUE_TYPE        TAOS_DEF_ERROR_CODE(0, 0x1172)  //"tag value type should be number or string"
#define TSDB_CODE_HTTP_TG_TAG_VALUE_NULL        TAOS_DEF_ERROR_CODE(0, 0x1173)  //"tag value is null"
#define TSDB_CODE_HTTP_TG_TABLE_NULL            TAOS_DEF_ERROR_CODE(0, 0x1174)  //"table is null"
#define TSDB_CODE_HTTP_TG_TABLE_SIZE            TAOS_DEF_ERROR_CODE(0, 0x1175)  //"table name length too long"
#define TSDB_CODE_HTTP_TG_FIELDS_NULL           TAOS_DEF_ERROR_CODE(0, 0x1176)  //"fields not find"
#define TSDB_CODE_HTTP_TG_FIELDS_SIZE_0         TAOS_DEF_ERROR_CODE(0, 0x1177)  //"fields size is 0"
#define TSDB_CODE_HTTP_TG_FIELDS_SIZE_LONG      TAOS_DEF_ERROR_CODE(0, 0x1178)  //"fields size too long"
#define TSDB_CODE_HTTP_TG_FIELD_NULL            TAOS_DEF_ERROR_CODE(0, 0x1179)  //"field is null"
#define TSDB_CODE_HTTP_TG_FIELD_NAME_NULL       TAOS_DEF_ERROR_CODE(0, 0x117A)  //"field name is null"
#define TSDB_CODE_HTTP_TG_FIELD_NAME_SIZE       TAOS_DEF_ERROR_CODE(0, 0x117B)  //"field name length too long"
#define TSDB_CODE_HTTP_TG_FIELD_VALUE_TYPE      TAOS_DEF_ERROR_CODE(0, 0x117C)  //"field value type should be number or string"
#define TSDB_CODE_HTTP_TG_FIELD_VALUE_NULL      TAOS_DEF_ERROR_CODE(0, 0x117D)  //"field value is null"
#define TSDB_CODE_HTTP_TG_HOST_NOT_STRING       TAOS_DEF_ERROR_CODE(0, 0x117E)  //"host type should be string"
#define TSDB_CODE_HTTP_TG_STABLE_NOT_EXIST      TAOS_DEF_ERROR_CODE(0, 0x117F)  //"stable not exist"

#define TSDB_CODE_HTTP_OP_DB_NOT_INPUT          TAOS_DEF_ERROR_CODE(0, 0x1190)  //"database name can not be null"
#define TSDB_CODE_HTTP_OP_DB_TOO_LONG           TAOS_DEF_ERROR_CODE(0, 0x1191)  //"database name too long"
#define TSDB_CODE_HTTP_OP_INVALID_JSON          TAOS_DEF_ERROR_CODE(0, 0x1192)  //"invalid opentsdb json fromat"
#define TSDB_CODE_HTTP_OP_METRICS_NULL          TAOS_DEF_ERROR_CODE(0, 0x1193)  //"metrics size is 0"
#define TSDB_CODE_HTTP_OP_METRICS_SIZE          TAOS_DEF_ERROR_CODE(0, 0x1194)  //"metrics size can not more than 10K"
#define TSDB_CODE_HTTP_OP_METRIC_NULL           TAOS_DEF_ERROR_CODE(0, 0x1195)  //"metric name not find"
#define TSDB_CODE_HTTP_OP_METRIC_TYPE           TAOS_DEF_ERROR_CODE(0, 0x1196)  //"metric name type should be string"
#define TSDB_CODE_HTTP_OP_METRIC_NAME_NULL      TAOS_DEF_ERROR_CODE(0, 0x1197)  //"metric name length is 0"
#define TSDB_CODE_HTTP_OP_METRIC_NAME_LONG      TAOS_DEF_ERROR_CODE(0, 0x1198)  //"metric name length can not more than 22"
#define TSDB_CODE_HTTP_OP_TIMESTAMP_NULL        TAOS_DEF_ERROR_CODE(0, 0x1199)  //"timestamp not find"
#define TSDB_CODE_HTTP_OP_TIMESTAMP_TYPE        TAOS_DEF_ERROR_CODE(0, 0x119A)  //"timestamp type should be integer"
#define TSDB_CODE_HTTP_OP_TIMESTAMP_VAL_NULL    TAOS_DEF_ERROR_CODE(0, 0x119B)  //"timestamp value smaller than 0"
#define TSDB_CODE_HTTP_OP_TAGS_NULL             TAOS_DEF_ERROR_CODE(0, 0x119C)  //"tags not find"
#define TSDB_CODE_HTTP_OP_TAGS_SIZE_0           TAOS_DEF_ERROR_CODE(0, 0x119D)  //"tags size is 0"
#define TSDB_CODE_HTTP_OP_TAGS_SIZE_LONG        TAOS_DEF_ERROR_CODE(0, 0x119E)  //"tags size too long"
#define TSDB_CODE_HTTP_OP_TAG_NULL              TAOS_DEF_ERROR_CODE(0, 0x119F)  //"tag is null"
#define TSDB_CODE_HTTP_OP_TAG_NAME_NULL         TAOS_DEF_ERROR_CODE(0, 0x11A0)  //"tag name is null"
#define TSDB_CODE_HTTP_OP_TAG_NAME_SIZE         TAOS_DEF_ERROR_CODE(0, 0x11A1)  //"tag name length too long"
#define TSDB_CODE_HTTP_OP_TAG_VALUE_TYPE        TAOS_DEF_ERROR_CODE(0, 0x11A2)  //"tag value type should be boolean number or string"
#define TSDB_CODE_HTTP_OP_TAG_VALUE_NULL        TAOS_DEF_ERROR_CODE(0, 0x11A3)  //"tag value is null"
#define TSDB_CODE_HTTP_OP_TAG_VALUE_TOO_LONG    TAOS_DEF_ERROR_CODE(0, 0x11A4)  //"tag value can not more than 64"
#define TSDB_CODE_HTTP_OP_VALUE_NULL            TAOS_DEF_ERROR_CODE(0, 0x11A5)  //"value not find"
#define TSDB_CODE_HTTP_OP_VALUE_TYPE            TAOS_DEF_ERROR_CODE(0, 0x11A6)  //"value type should be boolean number or string"

#define TSDB_CODE_HTTP_REQUEST_JSON_ERROR       TAOS_DEF_ERROR_CODE(0, 0x1F00)  //"http request json error"

// odbc
#define TSDB_CODE_ODBC_OOM                      TAOS_DEF_ERROR_CODE(0, 0x2100)  //"out of memory"
#define TSDB_CODE_ODBC_CONV_CHAR_NOT_NUM        TAOS_DEF_ERROR_CODE(0, 0x2101)  //"convertion not a valid literal input"
#define TSDB_CODE_ODBC_CONV_UNDEF               TAOS_DEF_ERROR_CODE(0, 0x2102)  //"convertion undefined"
#define TSDB_CODE_ODBC_CONV_TRUNC_FRAC          TAOS_DEF_ERROR_CODE(0, 0x2103)  //"convertion fractional truncated"
#define TSDB_CODE_ODBC_CONV_TRUNC               TAOS_DEF_ERROR_CODE(0, 0x2104)  //"convertion truncated"
#define TSDB_CODE_ODBC_CONV_NOT_SUPPORT         TAOS_DEF_ERROR_CODE(0, 0x2105)  //"convertion not supported"
#define TSDB_CODE_ODBC_CONV_OOR                 TAOS_DEF_ERROR_CODE(0, 0x2106)  //"convertion numeric value out of range"
#define TSDB_CODE_ODBC_OUT_OF_RANGE             TAOS_DEF_ERROR_CODE(0, 0x2107)  //"out of range"
#define TSDB_CODE_ODBC_NOT_SUPPORT              TAOS_DEF_ERROR_CODE(0, 0x2108)  //"not supported yet"
#define TSDB_CODE_ODBC_INVALID_HANDLE           TAOS_DEF_ERROR_CODE(0, 0x2109)  //"invalid handle"
#define TSDB_CODE_ODBC_NO_RESULT                TAOS_DEF_ERROR_CODE(0, 0x210a)  //"no result set"
#define TSDB_CODE_ODBC_NO_FIELDS                TAOS_DEF_ERROR_CODE(0, 0x210b)  //"no fields returned"
#define TSDB_CODE_ODBC_INVALID_CURSOR           TAOS_DEF_ERROR_CODE(0, 0x210c)  //"invalid cursor"
#define TSDB_CODE_ODBC_STATEMENT_NOT_READY      TAOS_DEF_ERROR_CODE(0, 0x210d)  //"statement not ready"
#define TSDB_CODE_ODBC_CONNECTION_BUSY          TAOS_DEF_ERROR_CODE(0, 0x210e)  //"connection still busy"
#define TSDB_CODE_ODBC_BAD_CONNSTR              TAOS_DEF_ERROR_CODE(0, 0x210f)  //"bad connection string"
#define TSDB_CODE_ODBC_BAD_ARG                  TAOS_DEF_ERROR_CODE(0, 0x2110)  //"bad argument"
#define TSDB_CODE_ODBC_CONV_NOT_VALID_TS        TAOS_DEF_ERROR_CODE(0, 0x2111)  //"not a valid timestamp"
#define TSDB_CODE_ODBC_CONV_SRC_TOO_LARGE       TAOS_DEF_ERROR_CODE(0, 0x2112)  //"src too large"
#define TSDB_CODE_ODBC_CONV_SRC_BAD_SEQ         TAOS_DEF_ERROR_CODE(0, 0x2113)  //"src bad sequence"
#define TSDB_CODE_ODBC_CONV_SRC_INCOMPLETE      TAOS_DEF_ERROR_CODE(0, 0x2114)  //"src incomplete"
#define TSDB_CODE_ODBC_CONV_SRC_GENERAL         TAOS_DEF_ERROR_CODE(0, 0x2115)  //"src general"

// tfs
#define TSDB_CODE_FS_OUT_OF_MEMORY              TAOS_DEF_ERROR_CODE(0, 0x2200)  //"tfs out of memory"
#define TSDB_CODE_FS_INVLD_CFG                  TAOS_DEF_ERROR_CODE(0, 0x2201)  //"tfs invalid mount config"
#define TSDB_CODE_FS_TOO_MANY_MOUNT             TAOS_DEF_ERROR_CODE(0, 0x2202)  //"tfs too many mount"
#define TSDB_CODE_FS_DUP_PRIMARY                TAOS_DEF_ERROR_CODE(0, 0x2203)  //"tfs duplicate primary mount"
#define TSDB_CODE_FS_NO_PRIMARY_DISK            TAOS_DEF_ERROR_CODE(0, 0x2204)  //"tfs no primary mount"
#define TSDB_CODE_FS_NO_MOUNT_AT_TIER           TAOS_DEF_ERROR_CODE(0, 0x2205)  //"tfs no mount at tier"
#define TSDB_CODE_FS_FILE_ALREADY_EXISTS        TAOS_DEF_ERROR_CODE(0, 0x2206)  //"tfs file already exists"
#define TSDB_CODE_FS_INVLD_LEVEL                TAOS_DEF_ERROR_CODE(0, 0x2207)  //"tfs invalid level"
#define TSDB_CODE_FS_NO_VALID_DISK              TAOS_DEF_ERROR_CODE(0, 0x2208)  //"tfs no valid disk"

// monitor
#define TSDB_CODE_MON_CONNECTION_INVALID        TAOS_DEF_ERROR_CODE(0, 0x2300)  //"monitor invalid monitor db connection"

#ifdef __cplusplus
}
#endif

#endif //TDENGINE_TAOSERROR_H
