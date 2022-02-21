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

#ifndef _TD_COMMON_TAOS_ERROR_H_
#define _TD_COMMON_TAOS_ERROR_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_DEF_ERROR_CODE(mod, code) ((int32_t)((0x80000000 | ((mod)<<16) | (code))))
 
#define TAOS_SYSTEM_ERROR(code)             (0x80ff0000 | (code))
#define TAOS_SUCCEEDED(err)                 ((err) >= 0)
#define TAOS_FAILED(err)                    ((err) < 0)

const char* tstrerror(int32_t err);
const char* terrstr();

int32_t* taosGetErrno();
#define terrno                              (*taosGetErrno())
 
#define TSDB_CODE_SUCCESS                   0
#define TSDB_CODE_FAILED                    -1   // unknown or needn't tell detail error

// rpc
#define TSDB_CODE_RPC_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0001)  //"Action in progress")
#define TSDB_CODE_RPC_AUTH_REQUIRED             TAOS_DEF_ERROR_CODE(0, 0x0002)  //"Authentication required")
#define TSDB_CODE_RPC_AUTH_FAILURE              TAOS_DEF_ERROR_CODE(0, 0x0003)  //"Authentication failure")
#define TSDB_CODE_RPC_REDIRECT                  TAOS_DEF_ERROR_CODE(0, 0x0004)  //"Redirect")
#define TSDB_CODE_RPC_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0005)  //"System not ready")    // peer is not ready to process data
#define TSDB_CODE_RPC_ALREADY_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0006)  //"Message already processed")
#define TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED TAOS_DEF_ERROR_CODE(0, 0x0007)  //"Last session not finished")
#define TSDB_CODE_RPC_MISMATCHED_LINK_ID        TAOS_DEF_ERROR_CODE(0, 0x0008)  //"Mismatched meter id")
#define TSDB_CODE_RPC_TOO_SLOW                  TAOS_DEF_ERROR_CODE(0, 0x0009)  //"Processing of request timed out")
#define TSDB_CODE_RPC_MAX_SESSIONS              TAOS_DEF_ERROR_CODE(0, 0x000A)  //"Number of sessions reached limit")    // too many sessions
#define TSDB_CODE_RPC_NETWORK_UNAVAIL           TAOS_DEF_ERROR_CODE(0, 0x000B)  //"Unable to establish connection")
#define TSDB_CODE_RPC_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x000C)  //"Unexpected generic error in RPC")
#define TSDB_CODE_RPC_UNEXPECTED_RESPONSE       TAOS_DEF_ERROR_CODE(0, 0x000D)  //"Unexpected response")
#define TSDB_CODE_RPC_INVALID_VALUE             TAOS_DEF_ERROR_CODE(0, 0x000E)  //"Invalid value")
#define TSDB_CODE_RPC_INVALID_TRAN_ID           TAOS_DEF_ERROR_CODE(0, 0x000F)  //"Invalid transaction id")
#define TSDB_CODE_RPC_INVALID_SESSION_ID        TAOS_DEF_ERROR_CODE(0, 0x0010)  //"Invalid session id")
#define TSDB_CODE_RPC_INVALID_MSG_TYPE          TAOS_DEF_ERROR_CODE(0, 0x0011)  //"Invalid message type")
#define TSDB_CODE_RPC_INVALID_RESPONSE_TYPE     TAOS_DEF_ERROR_CODE(0, 0x0012)  //"Invalid response type")
#define TSDB_CODE_RPC_INVALID_TIME_STAMP        TAOS_DEF_ERROR_CODE(0, 0x0013)  //"Client and server's time is not synchronized")
#define TSDB_CODE_APP_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0014)  //"Database not ready")
#define TSDB_CODE_RPC_FQDN_ERROR                TAOS_DEF_ERROR_CODE(0, 0x0015)  //"Unable to resolve FQDN")
#define TSDB_CODE_RPC_INVALID_VERSION           TAOS_DEF_ERROR_CODE(0, 0x0016)  //"Invalid app version")

//common & util
#define TSDB_CODE_OPS_NOT_SUPPORT               TAOS_DEF_ERROR_CODE(0, 0x0100)
#define TSDB_CODE_OUT_OF_MEMORY                 TAOS_DEF_ERROR_CODE(0, 0x0101)
#define TSDB_CODE_OUT_OF_RANGE                  TAOS_DEF_ERROR_CODE(0, 0x0102)
#define TSDB_CODE_INVALID_PTR                   TAOS_DEF_ERROR_CODE(0, 0x0103)
#define TSDB_CODE_MEMORY_CORRUPTED              TAOS_DEF_ERROR_CODE(0, 0x0104)
#define TSDB_CODE_FILE_CORRUPTED                TAOS_DEF_ERROR_CODE(0, 0x0106)
#define TSDB_CODE_CHECKSUM_ERROR                TAOS_DEF_ERROR_CODE(0, 0x0107)
#define TSDB_CODE_INVALID_MSG                   TAOS_DEF_ERROR_CODE(0, 0x0108)
#define TSDB_CODE_MSG_NOT_PROCESSED             TAOS_DEF_ERROR_CODE(0, 0x0109)
#define TSDB_CODE_INVALID_PARA                  TAOS_DEF_ERROR_CODE(0, 0x010A)
#define TSDB_CODE_REPEAT_INIT                   TAOS_DEF_ERROR_CODE(0, 0x010B)

#define TSDB_CODE_REF_NO_MEMORY                 TAOS_DEF_ERROR_CODE(0, 0x0110)
#define TSDB_CODE_REF_FULL                      TAOS_DEF_ERROR_CODE(0, 0x0111)
#define TSDB_CODE_REF_ID_REMOVED                TAOS_DEF_ERROR_CODE(0, 0x0112)
#define TSDB_CODE_REF_INVALID_ID                TAOS_DEF_ERROR_CODE(0, 0x0113)
#define TSDB_CODE_REF_ALREADY_EXIST             TAOS_DEF_ERROR_CODE(0, 0x0114)
#define TSDB_CODE_REF_NOT_EXIST                 TAOS_DEF_ERROR_CODE(0, 0x0115)

#define TSDB_CODE_INVALID_VERSION_NUMBER        TAOS_DEF_ERROR_CODE(0, 0x0120)
#define TSDB_CODE_INVALID_VERSION_STRING        TAOS_DEF_ERROR_CODE(0, 0x0121)
#define TSDB_CODE_VERSION_NOT_COMPATIBLE        TAOS_DEF_ERROR_CODE(0, 0x0122)

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
#define TSDB_CODE_TSC_VALUE_OUT_OF_RANGE        TAOS_DEF_ERROR_CODE(0, 0x0223)  //"Value out of range")
#define TSDB_CODE_TSC_INVALID_INPUT             TAOS_DEF_ERROR_CODE(0, 0X0224)  //"Invalid tsc input")

// mnode-common
#define TSDB_CODE_MND_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0300)
#define TSDB_CODE_MND_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0301)
#define TSDB_CODE_MND_MSG_NOT_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0302)
#define TSDB_CODE_MND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0303)
#define TSDB_CODE_MND_ACTION_NEED_REPROCESSED   TAOS_DEF_ERROR_CODE(0, 0x0304)
#define TSDB_CODE_MND_NO_RIGHTS                 TAOS_DEF_ERROR_CODE(0, 0x0305)
#define TSDB_CODE_MND_INVALID_OPTIONS           TAOS_DEF_ERROR_CODE(0, 0x0306)
#define TSDB_CODE_MND_INVALID_CONNECTION        TAOS_DEF_ERROR_CODE(0, 0x0307)
#define TSDB_CODE_MND_INVALID_MSG_VERSION       TAOS_DEF_ERROR_CODE(0, 0x0308)
#define TSDB_CODE_MND_INVALID_MSG_LEN           TAOS_DEF_ERROR_CODE(0, 0x0309)
#define TSDB_CODE_MND_INVALID_MSG_TYPE          TAOS_DEF_ERROR_CODE(0, 0x030A)
#define TSDB_CODE_MND_TOO_MANY_SHELL_CONNS      TAOS_DEF_ERROR_CODE(0, 0x030B)

// mnode-show
#define TSDB_CODE_MND_INVALID_SHOWOBJ           TAOS_DEF_ERROR_CODE(0, 0x0310)

// mnode-profile
#define TSDB_CODE_MND_INVALID_QUERY_ID          TAOS_DEF_ERROR_CODE(0, 0x0320)
#define TSDB_CODE_MND_INVALID_STREAM_ID         TAOS_DEF_ERROR_CODE(0, 0x0321)
#define TSDB_CODE_MND_INVALID_CONN_ID           TAOS_DEF_ERROR_CODE(0, 0x0322)
#define TSDB_CODE_MND_MNODE_IS_RUNNING          TAOS_DEF_ERROR_CODE(0, 0x0323)
#define TSDB_CODE_MND_FAILED_TO_CONFIG_SYNC     TAOS_DEF_ERROR_CODE(0, 0x0324)
#define TSDB_CODE_MND_FAILED_TO_START_SYNC      TAOS_DEF_ERROR_CODE(0, 0x0325)
#define TSDB_CODE_MND_FAILED_TO_CREATE_DIR      TAOS_DEF_ERROR_CODE(0, 0x0326)
#define TSDB_CODE_MND_FAILED_TO_INIT_STEP       TAOS_DEF_ERROR_CODE(0, 0x0327)

// mnode-sdb
#define TSDB_CODE_SDB_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0330)
#define TSDB_CODE_SDB_OBJ_ALREADY_THERE         TAOS_DEF_ERROR_CODE(0, 0x0331)
#define TSDB_CODE_SDB_OBJ_NOT_THERE             TAOS_DEF_ERROR_CODE(0, 0x0332)
#define TSDB_CODE_SDB_OBJ_CREATING              TAOS_DEF_ERROR_CODE(0, 0x0333)
#define TSDB_CODE_SDB_OBJ_DROPPING              TAOS_DEF_ERROR_CODE(0, 0x0334)
#define TSDB_CODE_SDB_INVALID_TABLE_TYPE        TAOS_DEF_ERROR_CODE(0, 0x0335)
#define TSDB_CODE_SDB_INVALID_KEY_TYPE          TAOS_DEF_ERROR_CODE(0, 0x0336)
#define TSDB_CODE_SDB_INVALID_ACTION_TYPE       TAOS_DEF_ERROR_CODE(0, 0x0337)
#define TSDB_CODE_SDB_INVALID_STATUS_TYPE       TAOS_DEF_ERROR_CODE(0, 0x0338)
#define TSDB_CODE_SDB_INVALID_DATA_VER          TAOS_DEF_ERROR_CODE(0, 0x0339)
#define TSDB_CODE_SDB_INVALID_DATA_LEN          TAOS_DEF_ERROR_CODE(0, 0x033A)
#define TSDB_CODE_SDB_INVALID_DATA_CONTENT      TAOS_DEF_ERROR_CODE(0, 0x033B)
#define TSDB_CODE_SDB_INVALID_WAl_VER           TAOS_DEF_ERROR_CODE(0, 0x033C)

// mnode-dnode
#define TSDB_CODE_MND_DNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0340)
#define TSDB_CODE_MND_DNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0341)
#define TSDB_CODE_MND_TOO_MANY_DNODES           TAOS_DEF_ERROR_CODE(0, 0x0342)
#define TSDB_CODE_MND_NO_ENOUGH_DNODES          TAOS_DEF_ERROR_CODE(0, 0x0343)
#define TSDB_CODE_MND_INVALID_CLUSTER_CFG       TAOS_DEF_ERROR_CODE(0, 0x0344)
#define TSDB_CODE_MND_INVALID_CLUSTER_ID        TAOS_DEF_ERROR_CODE(0, 0x0345)
#define TSDB_CODE_MND_INVALID_DNODE_CFG         TAOS_DEF_ERROR_CODE(0, 0x0346)
#define TSDB_CODE_MND_INVALID_DNODE_EP          TAOS_DEF_ERROR_CODE(0, 0x0347)
#define TSDB_CODE_MND_INVALID_DNODE_ID          TAOS_DEF_ERROR_CODE(0, 0x0348)

// mnode-node
#define TSDB_CODE_MND_MNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0350)
#define TSDB_CODE_MND_MNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0351)
#define TSDB_CODE_MND_QNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0352)
#define TSDB_CODE_MND_QNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0353)
#define TSDB_CODE_MND_SNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0354)
#define TSDB_CODE_MND_SNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0355)
#define TSDB_CODE_MND_BNODE_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x0356)
#define TSDB_CODE_MND_BNODE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x0357)

// mnode-acct
#define TSDB_CODE_MND_ACCT_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0360)
#define TSDB_CODE_MND_ACCT_NOT_EXIST            TAOS_DEF_ERROR_CODE(0, 0x0361)
#define TSDB_CODE_MND_TOO_MANY_ACCTS            TAOS_DEF_ERROR_CODE(0, 0x0362)
#define TSDB_CODE_MND_INVALID_ACCT_OPTION       TAOS_DEF_ERROR_CODE(0, 0x0363)
#define TSDB_CODE_MND_ACCT_EXPIRED              TAOS_DEF_ERROR_CODE(0, 0x0364)

// mnode-user
#define TSDB_CODE_MND_USER_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0370)
#define TSDB_CODE_MND_USER_NOT_EXIST            TAOS_DEF_ERROR_CODE(0, 0x0371)
#define TSDB_CODE_MND_TOO_MANY_USERS            TAOS_DEF_ERROR_CODE(0, 0x0372)
#define TSDB_CODE_MND_INVALID_USER_FORMAT       TAOS_DEF_ERROR_CODE(0, 0x0373)
#define TSDB_CODE_MND_INVALID_PASS_FORMAT       TAOS_DEF_ERROR_CODE(0, 0x0374)
#define TSDB_CODE_MND_NO_USER_FROM_CONN         TAOS_DEF_ERROR_CODE(0, 0x0375)
#define TSDB_CODE_MND_INVALID_ALTER_OPER        TAOS_DEF_ERROR_CODE(0, 0x0376)

// mnode-db
#define TSDB_CODE_MND_DB_ALREADY_EXIST          TAOS_DEF_ERROR_CODE(0, 0x0380)
#define TSDB_CODE_MND_DB_NOT_EXIST              TAOS_DEF_ERROR_CODE(0, 0x0381) 
#define TSDB_CODE_MND_TOO_MANY_DATABASES        TAOS_DEF_ERROR_CODE(0, 0x0382)
#define TSDB_CODE_MND_DB_NOT_SELECTED           TAOS_DEF_ERROR_CODE(0, 0x0383)
#define TSDB_CODE_MND_INVALID_DB                TAOS_DEF_ERROR_CODE(0, 0x0384)
#define TSDB_CODE_MND_INVALID_DB_OPTION         TAOS_DEF_ERROR_CODE(0, 0x0385)
#define TSDB_CODE_MND_INVALID_DB_ACCT           TAOS_DEF_ERROR_CODE(0, 0x0386)
#define TSDB_CODE_MND_DB_OPTION_UNCHANGED       TAOS_DEF_ERROR_CODE(0, 0x0387)

// mnode-vgroup
#define TSDB_CODE_MND_VGROUP_NOT_EXIST          TAOS_DEF_ERROR_CODE(0, 0x0390)
#define TSDB_CODE_MND_VGROUP_NOT_IN_DNODE       TAOS_DEF_ERROR_CODE(0, 0x0391)
#define TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE   TAOS_DEF_ERROR_CODE(0, 0x0392)

// mnode-stable
#define TSDB_CODE_MND_STB_ALREADY_EXIST         TAOS_DEF_ERROR_CODE(0, 0x03A0)
#define TSDB_CODE_MND_STB_NOT_EXIST             TAOS_DEF_ERROR_CODE(0, 0x03A1)
#define TSDB_CODE_MND_NAME_CONFLICT_WITH_TOPIC  TAOS_DEF_ERROR_CODE(0, 0x03A2)
#define TSDB_CODE_MND_TOO_MANY_STBS             TAOS_DEF_ERROR_CODE(0, 0x03A3)
#define TSDB_CODE_MND_INVALID_STB               TAOS_DEF_ERROR_CODE(0, 0x03A4)
#define TSDB_CODE_MND_INVALID_STB_OPTION        TAOS_DEF_ERROR_CODE(0, 0x03A5)
#define TSDB_CODE_MND_INVALID_STB_ALTER_OPTION  TAOS_DEF_ERROR_CODE(0, 0x03A6)
#define TSDB_CODE_MND_STB_OPTION_UNCHNAGED      TAOS_DEF_ERROR_CODE(0, 0x03A7)
#define TSDB_CODE_MND_INVALID_ROW_BYTES         TAOS_DEF_ERROR_CODE(0, 0x03A8)
#define TSDB_CODE_MND_TOO_MANY_TAGS             TAOS_DEF_ERROR_CODE(0, 0x03A9)
#define TSDB_CODE_MND_TAG_ALREADY_EXIST         TAOS_DEF_ERROR_CODE(0, 0x03AA)
#define TSDB_CODE_MND_TAG_NOT_EXIST             TAOS_DEF_ERROR_CODE(0, 0x03AB)
#define TSDB_CODE_MND_TOO_MANY_COLUMNS          TAOS_DEF_ERROR_CODE(0, 0x03AC)
#define TSDB_CODE_MND_COLUMN_ALREADY_EXIST      TAOS_DEF_ERROR_CODE(0, 0x03AD)
#define TSDB_CODE_MND_COLUMN_NOT_EXIST          TAOS_DEF_ERROR_CODE(0, 0x03AE)

// mnode-func
#define TSDB_CODE_MND_FUNC_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x03C0)
#define TSDB_CODE_MND_FUNC_NOT_EXIST            TAOS_DEF_ERROR_CODE(0, 0x03C1)
#define TSDB_CODE_MND_INVALID_FUNC              TAOS_DEF_ERROR_CODE(0, 0x03C2)
#define TSDB_CODE_MND_INVALID_FUNC_NAME         TAOS_DEF_ERROR_CODE(0, 0x03C3)
#define TSDB_CODE_MND_INVALID_FUNC_COMMENT      TAOS_DEF_ERROR_CODE(0, 0x03C4)
#define TSDB_CODE_MND_INVALID_FUNC_CODE         TAOS_DEF_ERROR_CODE(0, 0x03C5)
#define TSDB_CODE_MND_INVALID_FUNC_BUFSIZE      TAOS_DEF_ERROR_CODE(0, 0x03C6)
#define TSDB_CODE_MND_INVALID_FUNC_RETRIEVE     TAOS_DEF_ERROR_CODE(0, 0x03C7)

// mnode-trans
#define TSDB_CODE_MND_TRANS_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x03D0)
#define TSDB_CODE_MND_TRANS_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x03D1)
#define TSDB_CODE_MND_TRANS_INVALID_STAGE       TAOS_DEF_ERROR_CODE(0, 0x03D2)
#define TSDB_CODE_MND_TRANS_CANT_PARALLEL       TAOS_DEF_ERROR_CODE(0, 0x03D4)

// mnode-mq
#define TSDB_CODE_MND_TOPIC_ALREADY_EXIST       TAOS_DEF_ERROR_CODE(0, 0x03E0)
#define TSDB_CODE_MND_TOPIC_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x03E1)
#define TSDB_CODE_MND_TOO_MANY_TOPICS           TAOS_DEF_ERROR_CODE(0, 0x03E2)
#define TSDB_CODE_MND_INVALID_TOPIC             TAOS_DEF_ERROR_CODE(0, 0x03E3)
#define TSDB_CODE_MND_INVALID_TOPIC_OPTION      TAOS_DEF_ERROR_CODE(0, 0x03E4)
#define TSDB_CODE_MND_TOPIC_OPTION_UNCHNAGED    TAOS_DEF_ERROR_CODE(0, 0x03E5)
#define TSDB_CODE_MND_NAME_CONFLICT_WITH_STB    TAOS_DEF_ERROR_CODE(0, 0x03E6)
#define TSDB_CODE_MND_CONSUMER_NOT_EXIST        TAOS_DEF_ERROR_CODE(0, 0x03E7)
#define TSDB_CODE_MND_UNSUPPORTED_TOPIC         TAOS_DEF_ERROR_CODE(0, 0x03E8)
#define TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST       TAOS_DEF_ERROR_CODE(0, 0x03E9)
#define TSDB_CODE_MND_MQ_PLACEHOLDER            TAOS_DEF_ERROR_CODE(0, 0x03F0)

// dnode
#define TSDB_CODE_DND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0400)
#define TSDB_CODE_DND_OFFLINE                   TAOS_DEF_ERROR_CODE(0, 0x0401)
#define TSDB_CODE_DND_INVALID_MSG_LEN           TAOS_DEF_ERROR_CODE(0, 0x0402)
#define TSDB_CODE_DND_DNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0410)
#define TSDB_CODE_DND_DNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0411)
#define TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED    TAOS_DEF_ERROR_CODE(0, 0x0420)
#define TSDB_CODE_DND_MNODE_NOT_DEPLOYED        TAOS_DEF_ERROR_CODE(0, 0x0421)
#define TSDB_CODE_DND_MNODE_INVALID_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0422)
#define TSDB_CODE_DND_MNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0423)
#define TSDB_CODE_DND_MNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0424)
#define TSDB_CODE_DND_QNODE_ALREADY_DEPLOYED    TAOS_DEF_ERROR_CODE(0, 0x0430)
#define TSDB_CODE_DND_QNODE_NOT_DEPLOYED        TAOS_DEF_ERROR_CODE(0, 0x0431)
#define TSDB_CODE_DND_QNODE_INVALID_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0432)
#define TSDB_CODE_DND_QNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0433)
#define TSDB_CODE_DND_QNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0434)
#define TSDB_CODE_DND_SNODE_ALREADY_DEPLOYED    TAOS_DEF_ERROR_CODE(0, 0x0440)
#define TSDB_CODE_DND_SNODE_NOT_DEPLOYED        TAOS_DEF_ERROR_CODE(0, 0x0441)
#define TSDB_CODE_DND_SNODE_INVALID_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0442)
#define TSDB_CODE_DND_SNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0443)
#define TSDB_CODE_DND_SNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0444)
#define TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED    TAOS_DEF_ERROR_CODE(0, 0x0450)
#define TSDB_CODE_DND_BNODE_NOT_DEPLOYED        TAOS_DEF_ERROR_CODE(0, 0x0451)
#define TSDB_CODE_DND_BNODE_INVALID_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0452)
#define TSDB_CODE_DND_BNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0453)
#define TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0454)
#define TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED    TAOS_DEF_ERROR_CODE(0, 0x0460)
#define TSDB_CODE_DND_VNODE_NOT_DEPLOYED        TAOS_DEF_ERROR_CODE(0, 0x0461)
#define TSDB_CODE_DND_VNODE_INVALID_OPTION      TAOS_DEF_ERROR_CODE(0, 0x0462)
#define TSDB_CODE_DND_VNODE_READ_FILE_ERROR     TAOS_DEF_ERROR_CODE(0, 0x0463)
#define TSDB_CODE_DND_VNODE_WRITE_FILE_ERROR    TAOS_DEF_ERROR_CODE(0, 0x0464)
#define TSDB_CODE_DND_VNODE_TOO_MANY_VNODES     TAOS_DEF_ERROR_CODE(0, 0x0465)

// vnode
#define TSDB_CODE_VND_ACTION_IN_PROGRESS        TAOS_DEF_ERROR_CODE(0, 0x0500)  //"Action in progress")
#define TSDB_CODE_VND_MSG_NOT_PROCESSED         TAOS_DEF_ERROR_CODE(0, 0x0501)  //"Message not processed")
#define TSDB_CODE_VND_ACTION_NEED_REPROCESSED   TAOS_DEF_ERROR_CODE(0, 0x0502)  //"Action need to be reprocessed")
#define TSDB_CODE_VND_INVALID_VGROUP_ID         TAOS_DEF_ERROR_CODE(0, 0x0503)  //"Invalid Vgroup ID")
#define TSDB_CODE_VND_INIT_FAILED               TAOS_DEF_ERROR_CODE(0, 0x0504)  //"Vnode initialization failed")
#define TSDB_CODE_VND_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x0505)  //"System out of disk space")
#define TSDB_CODE_VND_NO_DISK_PERMISSIONS       TAOS_DEF_ERROR_CODE(0, 0x0506)  //"No write permission for disk files")
#define TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR       TAOS_DEF_ERROR_CODE(0, 0x0507)  //"Missing data file")
#define TSDB_CODE_VND_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0508)  //"Out of memory")
#define TSDB_CODE_VND_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0509)  //"Unexpected generic error in vnode")
#define TSDB_CODE_VND_INVALID_CFG_FILE          TAOS_DEF_ERROR_CODE(0, 0x050A)  //"Invalid config file)
#define TSDB_CODE_VND_INVALID_TERM_FILE         TAOS_DEF_ERROR_CODE(0, 0x050B)  //"Invalid term file")
#define TSDB_CODE_VND_IS_FLOWCTRL               TAOS_DEF_ERROR_CODE(0, 0x050C)  //"Database memory is full")
#define TSDB_CODE_VND_IS_DROPPING               TAOS_DEF_ERROR_CODE(0, 0x050D)  //"Database is dropping")
#define TSDB_CODE_VND_IS_UPDATING               TAOS_DEF_ERROR_CODE(0, 0x050E)  //"Database is updating")
#define TSDB_CODE_VND_IS_CLOSING                TAOS_DEF_ERROR_CODE(0, 0x0510)  //"Database is closing")
#define TSDB_CODE_VND_NOT_SYNCED                TAOS_DEF_ERROR_CODE(0, 0x0511)  //"Database suspended")
#define TSDB_CODE_VND_NO_WRITE_AUTH             TAOS_DEF_ERROR_CODE(0, 0x0512)  //"Database write operation denied")
#define TSDB_CODE_VND_IS_SYNCING                TAOS_DEF_ERROR_CODE(0, 0x0513)  //"Database is syncing")
#define TSDB_CODE_VND_INVALID_TSDB_STATE        TAOS_DEF_ERROR_CODE(0, 0x0514)  //"Invalid tsdb state")
#define TSDB_CODE_VND_TB_NOT_EXIST              TAOS_DEF_ERROR_CODE(0, 0x0515)  // "Table not exists")

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

// query
#define TSDB_CODE_QRY_INVALID_QHANDLE           TAOS_DEF_ERROR_CODE(0, 0x0700)  //"Invalid handle")
#define TSDB_CODE_QRY_INVALID_MSG               TAOS_DEF_ERROR_CODE(0, 0x0701)  //"Invalid message")    // failed to validate the sql expression msg by vnode
#define TSDB_CODE_QRY_NO_DISKSPACE              TAOS_DEF_ERROR_CODE(0, 0x0702)  //"No diskspace for query")
#define TSDB_CODE_QRY_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x0703)  //"System out of memory")
#define TSDB_CODE_QRY_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x0704)  //"Unexpected generic error in query")
#define TSDB_CODE_QRY_DUP_JOIN_KEY              TAOS_DEF_ERROR_CODE(0, 0x0705)  //"Duplicated join key")
#define TSDB_CODE_QRY_EXCEED_TAGS_LIMIT         TAOS_DEF_ERROR_CODE(0, 0x0706)  //"Tag conditon too many")
#define TSDB_CODE_QRY_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x0707)  //"Query not ready")
#define TSDB_CODE_QRY_HAS_RSP                   TAOS_DEF_ERROR_CODE(0, 0x0708)  //"Query should response")
#define TSDB_CODE_QRY_IN_EXEC                   TAOS_DEF_ERROR_CODE(0, 0x0709)  //"Multiple retrieval of this query")
#define TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW       TAOS_DEF_ERROR_CODE(0, 0x070A)  //"Too many time window in query")
#define TSDB_CODE_QRY_NOT_ENOUGH_BUFFER         TAOS_DEF_ERROR_CODE(0, 0x070B)  //"Query buffer limit has reached")
#define TSDB_CODE_QRY_INCONSISTAN               TAOS_DEF_ERROR_CODE(0, 0x070C)  //"File inconsistency in replica")
#define TSDB_CODE_QRY_INVALID_TIME_CONDITION    TAOS_DEF_ERROR_CODE(0, 0x070D)  //"invalid time condition")
#define TSDB_CODE_QRY_SYS_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x070E)  //"System error")
#define TSDB_CODE_QRY_INVALID_INPUT             TAOS_DEF_ERROR_CODE(0, 0x070F)  //"invalid input")
#define TSDB_CODE_QRY_SCH_NOT_EXIST             TAOS_DEF_ERROR_CODE(0, 0x0710)  //"Scheduler not exist")
#define TSDB_CODE_QRY_TASK_NOT_EXIST            TAOS_DEF_ERROR_CODE(0, 0x0711)  //"Task not exist")
#define TSDB_CODE_QRY_TASK_ALREADY_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0712)  //"Task already exist")
#define TSDB_CODE_QRY_TASK_CTX_NOT_EXIST        TAOS_DEF_ERROR_CODE(0, 0x0713)  //"Task context not exist")
#define TSDB_CODE_QRY_TASK_CANCELLED            TAOS_DEF_ERROR_CODE(0, 0x0714)  //"Task cancelled")
#define TSDB_CODE_QRY_TASK_DROPPED              TAOS_DEF_ERROR_CODE(0, 0x0715)  //"Task dropped")
#define TSDB_CODE_QRY_TASK_CANCELLING           TAOS_DEF_ERROR_CODE(0, 0x0716)  //"Task cancelling")
#define TSDB_CODE_QRY_TASK_DROPPING             TAOS_DEF_ERROR_CODE(0, 0x0717)  //"Task dropping")
#define TSDB_CODE_QRY_DUPLICATTED_OPERATION     TAOS_DEF_ERROR_CODE(0, 0x0718)  //"Duplicatted operation")
#define TSDB_CODE_QRY_TASK_MSG_ERROR            TAOS_DEF_ERROR_CODE(0, 0x0719)  //"Task message error")
#define TSDB_CODE_QRY_JOB_FREED                 TAOS_DEF_ERROR_CODE(0, 0x071A)  //"Job freed")
#define TSDB_CODE_QRY_TASK_STATUS_ERROR         TAOS_DEF_ERROR_CODE(0, 0x071B)  //"Task status error")

// grant
#define TSDB_CODE_GRANT_EXPIRED                 TAOS_DEF_ERROR_CODE(0, 0x0800)  //"License expired")
#define TSDB_CODE_GRANT_DNODE_LIMITED           TAOS_DEF_ERROR_CODE(0, 0x0801)  //"DNode creation limited by licence")
#define TSDB_CODE_GRANT_ACCT_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0802)  //"Account creation limited by license")
#define TSDB_CODE_GRANT_TIMESERIES_LIMITED      TAOS_DEF_ERROR_CODE(0, 0x0803)  //"Table creation limited by license")
#define TSDB_CODE_GRANT_DB_LIMITED              TAOS_DEF_ERROR_CODE(0, 0x0804)  //"DB creation limited by license")
#define TSDB_CODE_GRANT_USER_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0805)  //"User creation limited by license")
#define TSDB_CODE_GRANT_CONN_LIMITED            TAOS_DEF_ERROR_CODE(0, 0x0806)  //"Conn creation limited by license")
#define TSDB_CODE_GRANT_STREAM_LIMITED          TAOS_DEF_ERROR_CODE(0, 0x0807)  //"Stream creation limited by license")
#define TSDB_CODE_GRANT_SPEED_LIMITED           TAOS_DEF_ERROR_CODE(0, 0x0808)  //"Write speed limited by license")
#define TSDB_CODE_GRANT_STORAGE_LIMITED         TAOS_DEF_ERROR_CODE(0, 0x0809)  //"Storage capacity limited by license")
#define TSDB_CODE_GRANT_QUERYTIME_LIMITED       TAOS_DEF_ERROR_CODE(0, 0x080A)  //"Query time limited by license")
#define TSDB_CODE_GRANT_CPU_LIMITED             TAOS_DEF_ERROR_CODE(0, 0x080B)  //"CPU cores limited by license")

// sync
#define TSDB_CODE_SYN_INVALID_CONFIG            TAOS_DEF_ERROR_CODE(0, 0x0900)  //"Invalid Sync Configuration")
#define TSDB_CODE_SYN_NOT_ENABLED               TAOS_DEF_ERROR_CODE(0, 0x0901)  //"Sync module not enabled")
#define TSDB_CODE_SYN_INVALID_VERSION           TAOS_DEF_ERROR_CODE(0, 0x0902)  //"Invalid Sync version")
#define TSDB_CODE_SYN_CONFIRM_EXPIRED           TAOS_DEF_ERROR_CODE(0, 0x0903)  //"Sync confirm expired")
#define TSDB_CODE_SYN_TOO_MANY_FWDINFO          TAOS_DEF_ERROR_CODE(0, 0x0904)  //"Too many sync fwd infos")
#define TSDB_CODE_SYN_MISMATCHED_PROTOCOL       TAOS_DEF_ERROR_CODE(0, 0x0905)  //"Mismatched protocol")
#define TSDB_CODE_SYN_MISMATCHED_CLUSTERID      TAOS_DEF_ERROR_CODE(0, 0x0906)  //"Mismatched clusterId")
#define TSDB_CODE_SYN_MISMATCHED_SIGNATURE      TAOS_DEF_ERROR_CODE(0, 0x0907)  //"Mismatched signature")
#define TSDB_CODE_SYN_INVALID_CHECKSUM          TAOS_DEF_ERROR_CODE(0, 0x0908)  //"Invalid msg checksum")
#define TSDB_CODE_SYN_INVALID_MSGLEN            TAOS_DEF_ERROR_CODE(0, 0x0909)  //"Invalid msg length")
#define TSDB_CODE_SYN_INVALID_MSGTYPE           TAOS_DEF_ERROR_CODE(0, 0x090A)  //"Invalid msg type")

// tq
#define TSDB_CODE_TQ_INVALID_CONFIG             TAOS_DEF_ERROR_CODE(0, 0x0A00)  //"Invalid configuration")
#define TSDB_CODE_TQ_INIT_FAILED                TAOS_DEF_ERROR_CODE(0, 0x0A01)  //"Tq init failed")
#define TSDB_CODE_TQ_NO_DISKSPACE               TAOS_DEF_ERROR_CODE(0, 0x0A02)  //"No diskspace for tq")
#define TSDB_CODE_TQ_NO_DISK_PERMISSIONS        TAOS_DEF_ERROR_CODE(0, 0x0A03)  //"No permission for disk files")
#define TSDB_CODE_TQ_FILE_CORRUPTED             TAOS_DEF_ERROR_CODE(0, 0x0A04)  //"Data file(s) corrupted")
#define TSDB_CODE_TQ_OUT_OF_MEMORY              TAOS_DEF_ERROR_CODE(0, 0x0A05)  //"Out of memory")
#define TSDB_CODE_TQ_FILE_ALREADY_EXISTS        TAOS_DEF_ERROR_CODE(0, 0x0A06)  //"File already exists")
#define TSDB_CODE_TQ_FAILED_TO_CREATE_DIR       TAOS_DEF_ERROR_CODE(0, 0x0A07)  //"Failed to create dir")
#define TSDB_CODE_TQ_META_NO_SUCH_KEY           TAOS_DEF_ERROR_CODE(0, 0x0A08)  //"Target key not found")
#define TSDB_CODE_TQ_META_KEY_NOT_IN_TXN        TAOS_DEF_ERROR_CODE(0, 0x0A09)  //"Target key not in transaction")
#define TSDB_CODE_TQ_META_KEY_DUP_IN_TXN        TAOS_DEF_ERROR_CODE(0, 0x0A0A)  //"Target key duplicated in transaction")
#define TSDB_CODE_TQ_GROUP_NOT_SET              TAOS_DEF_ERROR_CODE(0, 0x0A0B)  //"Group of corresponding client is not set by mnode")

// wal
#define TSDB_CODE_WAL_APP_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x1000)  //"Unexpected generic error in wal")
#define TSDB_CODE_WAL_FILE_CORRUPTED            TAOS_DEF_ERROR_CODE(0, 0x1001)  //"WAL file is corrupted")
#define TSDB_CODE_WAL_SIZE_LIMIT                TAOS_DEF_ERROR_CODE(0, 0x1002)  //"WAL size exceeds limit")
#define TSDB_CODE_WAL_INVALID_VER               TAOS_DEF_ERROR_CODE(0, 0x1003)  //"WAL invalid version")
#define TSDB_CODE_WAL_OUT_OF_MEMORY             TAOS_DEF_ERROR_CODE(0, 0x1004)  //"WAL out of memory")

// tfs
#define TSDB_CODE_FS_APP_ERROR                  TAOS_DEF_ERROR_CODE(0, 0x2200)  //"tfs out of memory")
#define TSDB_CODE_FS_INVLD_CFG                  TAOS_DEF_ERROR_CODE(0, 0x2201)  //"tfs invalid mount config")
#define TSDB_CODE_FS_TOO_MANY_MOUNT             TAOS_DEF_ERROR_CODE(0, 0x2202)  //"tfs too many mount")
#define TSDB_CODE_FS_DUP_PRIMARY                TAOS_DEF_ERROR_CODE(0, 0x2203)  //"tfs duplicate primary mount")
#define TSDB_CODE_FS_NO_PRIMARY_DISK            TAOS_DEF_ERROR_CODE(0, 0x2204)  //"tfs no primary mount")
#define TSDB_CODE_FS_NO_MOUNT_AT_TIER           TAOS_DEF_ERROR_CODE(0, 0x2205)  //"tfs no mount at tier")
#define TSDB_CODE_FS_FILE_ALREADY_EXISTS        TAOS_DEF_ERROR_CODE(0, 0x2206)  //"tfs file already exists")
#define TSDB_CODE_FS_INVLD_LEVEL                TAOS_DEF_ERROR_CODE(0, 0x2207)  //"tfs invalid level")
#define TSDB_CODE_FS_NO_VALID_DISK              TAOS_DEF_ERROR_CODE(0, 0x2208)  //"tfs no valid disk")

// monitor
#define TSDB_CODE_MON_CONNECTION_INVALID        TAOS_DEF_ERROR_CODE(0, 0x2300)  //"monitor invalid monitor db connection")

// catalog
#define TSDB_CODE_CTG_INTERNAL_ERROR            TAOS_DEF_ERROR_CODE(0, 0x2400)  //catalog interval error
#define TSDB_CODE_CTG_INVALID_INPUT             TAOS_DEF_ERROR_CODE(0, 0x2401)  //invalid catalog input parameters
#define TSDB_CODE_CTG_NOT_READY                 TAOS_DEF_ERROR_CODE(0, 0x2402)  //catalog is not ready
#define TSDB_CODE_CTG_MEM_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x2403)  //catalog memory error
#define TSDB_CODE_CTG_SYS_ERROR                 TAOS_DEF_ERROR_CODE(0, 0x2404)  //catalog system error
#define TSDB_CODE_CTG_DB_DROPPED                TAOS_DEF_ERROR_CODE(0, 0x2405)  //Database is dropped
#define TSDB_CODE_CTG_OUT_OF_SERVICE            TAOS_DEF_ERROR_CODE(0, 0x2406)  //catalog is out of service

//scheduler
#define TSDB_CODE_SCH_STATUS_ERROR              TAOS_DEF_ERROR_CODE(0, 0x2501)  //scheduler status error
#define TSDB_CODE_SCH_INTERNAL_ERROR            TAOS_DEF_ERROR_CODE(0, 0x2502)  //scheduler internal error

//parser
#define TSDB_CODE_PAR_INVALID_COLUMN            TAOS_DEF_ERROR_CODE(0, 0x2601)  //Invalid column name
#define TSDB_CODE_PAR_TABLE_NOT_EXIST           TAOS_DEF_ERROR_CODE(0, 0x2602)  //Table does not exist
#define TSDB_CODE_PAR_AMBIGUOUS_COLUMN          TAOS_DEF_ERROR_CODE(0, 0x2603)  //Column ambiguously defined
#define TSDB_CODE_PAR_WRONG_VALUE_TYPE          TAOS_DEF_ERROR_CODE(0, 0x2604)  //Invalid value type
#define TSDB_CODE_PAR_INVALID_FUNTION           TAOS_DEF_ERROR_CODE(0, 0x2605)  //Invalid function name
#define TSDB_CODE_PAR_FUNTION_PARA_NUM          TAOS_DEF_ERROR_CODE(0, 0x2606)  //Invalid number of arguments
#define TSDB_CODE_PAR_FUNTION_PARA_TYPE         TAOS_DEF_ERROR_CODE(0, 0x2607)  //Inconsistent datatypes
#define TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION  TAOS_DEF_ERROR_CODE(0, 0x2608)  //There mustn't be aggregation
#define TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT    TAOS_DEF_ERROR_CODE(0, 0x2609)  //ORDER BY item must be the number of a SELECT-list expression
#define TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION   TAOS_DEF_ERROR_CODE(0, 0x260A)  //Not a GROUP BY expression
#define TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION   TAOS_DEF_ERROR_CODE(0, 0x260B)  //Not SELECTed expression
#define TSDB_CODE_PAR_NOT_SINGLE_GROUP          TAOS_DEF_ERROR_CODE(0, 0x260C)  //Not a single-group group function

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TAOS_ERROR_H_*/
