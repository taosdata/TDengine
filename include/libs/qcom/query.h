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

#ifndef _TD_QUERY_H_
#define _TD_QUERY_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tarray.h"
#include "thash.h"
#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"

typedef enum {
  JOB_TASK_STATUS_NULL = 0,
  JOB_TASK_STATUS_NOT_START = 1,
  JOB_TASK_STATUS_EXECUTING,
  JOB_TASK_STATUS_PARTIAL_SUCCEED,
  JOB_TASK_STATUS_SUCCEED,
  JOB_TASK_STATUS_FAILED,
  JOB_TASK_STATUS_CANCELLING,
  JOB_TASK_STATUS_CANCELLED,
  JOB_TASK_STATUS_DROPPING,
} EJobTaskType;

typedef enum {
  TASK_TYPE_PERSISTENT = 1,
  TASK_TYPE_TEMP,
} ETaskType;

typedef struct STableComInfo {
  uint8_t  numOfTags;     // the number of tags in schema
  uint8_t  precision;     // the number of precision
  col_id_t numOfColumns;  // the number of columns
  int32_t  rowSize;       // row size of the schema
} STableComInfo;

typedef struct SIndexMeta {
#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif

} SIndexMeta;

/*
 * ASSERT(sizeof(SCTableMeta) == 24)
 * ASSERT(tableType == TSDB_CHILD_TABLE)
 * The cached child table meta info. For each child table, 24 bytes are required to keep the essential table info.
 */
typedef struct SCTableMeta {
  int32_t  vgId : 24;
  int8_t   tableType;
  uint64_t uid;
  uint64_t suid;
} SCTableMeta;

/*
 * Note that the first 24 bytes of STableMeta are identical to SCTableMeta, it is safe to cast a STableMeta to be a
 * SCTableMeta.
 */
typedef struct STableMeta {
  // BEGIN: KEEP THIS PART SAME WITH SCTableMeta
  int32_t  vgId : 24;
  int8_t   tableType;
  uint64_t uid;
  uint64_t suid;
  // END: KEEP THIS PART SAME WITH SCTableMeta

  // if the table is TSDB_CHILD_TABLE, the following information is acquired from the corresponding super table meta
  // info
  int16_t       sversion;
  int16_t       tversion;
  STableComInfo tableInfo;
  SSchema       schema[];
} STableMeta;

typedef struct SDBVgInfo {
  int32_t   vgVersion;
  int8_t    hashMethod;
  int32_t   numOfTable;  // DB's table num, unit is TSDB_TABLE_NUM_UNIT
  SHashObj* vgHash;      // key:vgId, value:SVgroupInfo
} SDBVgInfo;

typedef struct SUseDbOutput {
  char       db[TSDB_DB_FNAME_LEN];
  uint64_t   dbId;
  SDBVgInfo* dbVgroup;
} SUseDbOutput;

enum { META_TYPE_NULL_TABLE = 1, META_TYPE_CTABLE, META_TYPE_TABLE, META_TYPE_BOTH_TABLE };

typedef struct STableMetaOutput {
  int32_t     metaType;
  uint64_t    dbId;
  char        dbFName[TSDB_DB_FNAME_LEN];
  char        ctbName[TSDB_TABLE_NAME_LEN];
  char        tbName[TSDB_TABLE_NAME_LEN];
  SCTableMeta ctbMeta;
  STableMeta* tbMeta;
} STableMetaOutput;

typedef struct SDataBuf {
  void*    pData;
  uint32_t len;
  void*    handle;
} SDataBuf;

typedef int32_t (*__async_send_cb_fn_t)(void* param, const SDataBuf* pMsg, int32_t code);
typedef int32_t (*__async_exec_fn_t)(void* param);

typedef struct SMsgSendInfo {
  __async_send_cb_fn_t fp;  // async callback function
  void*                param;
  uint64_t             requestId;
  uint64_t             requestObjRefId;
  int32_t              msgType;
  SDataBuf             msgInfo;
} SMsgSendInfo;

typedef struct SQueryNodeStat {
  int32_t tableNum;  // vg table number, unit is TSDB_TABLE_NUM_UNIT
} SQueryNodeStat;

int32_t initTaskQueue();
int32_t cleanupTaskQueue();

/**
 *
 * @param execFn      The asynchronously execution function
 * @param execParam   The parameters of the execFn
 * @param code        The response code during execution the execFn
 * @return
 */
int32_t taosAsyncExec(__async_exec_fn_t execFn, void* execParam, int32_t* code);

int32_t asyncSendMsgToServerExt(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, const SMsgSendInfo* pInfo,
                                bool persistHandle, void* ctx);

/**
 * Asynchronously send message to server, after the response received, the callback will be incured.
 *
 * @param pTransporter
 * @param epSet
 * @param pTransporterId
 * @param pInfo
 * @return
 */
int32_t asyncSendMsgToServer(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, const SMsgSendInfo* pInfo);

int32_t queryBuildUseDbOutput(SUseDbOutput* pOut, SUseDbRsp* usedbRsp);

void initQueryModuleMsgHandle();

const SSchema* tGetTbnameColumnSchema();
bool           tIsValidSchema(struct SSchema* pSchema, int32_t numOfCols, int32_t numOfTags);

int32_t queryCreateTableMetaFromMsg(STableMetaRsp* msg, bool isSuperTable, STableMeta** pMeta);
char*   jobTaskStatusStr(int32_t status);

SSchema createSchema(int8_t type, int32_t bytes, col_id_t colId, const char* name);

extern int32_t (*queryBuildMsg[TDMT_MAX])(void* input, char** msg, int32_t msgSize, int32_t* msgLen);
extern int32_t (*queryProcessMsgRsp[TDMT_MAX])(void* output, char* msg, int32_t msgSize);

#define SET_META_TYPE_NULL(t)       (t) = META_TYPE_NULL_TABLE
#define SET_META_TYPE_CTABLE(t)     (t) = META_TYPE_CTABLE
#define SET_META_TYPE_TABLE(t)      (t) = META_TYPE_TABLE
#define SET_META_TYPE_BOTH_TABLE(t) (t) = META_TYPE_BOTH_TABLE

#define NEED_CLIENT_RM_TBLMETA_ERROR(_code) \
  ((_code) == TSDB_CODE_PAR_TABLE_NOT_EXIST || (_code) == TSDB_CODE_VND_TB_NOT_EXIST)
#define NEED_CLIENT_REFRESH_VG_ERROR(_code) \
  ((_code) == TSDB_CODE_VND_HASH_MISMATCH || (_code) == TSDB_CODE_VND_INVALID_VGROUP_ID)
#define NEED_CLIENT_REFRESH_TBLMETA_ERROR(_code) ((_code) == TSDB_CODE_TDB_TABLE_RECREATED)
#define NEED_CLIENT_HANDLE_ERROR(_code)                                          \
  (NEED_CLIENT_RM_TBLMETA_ERROR(_code) || NEED_CLIENT_REFRESH_VG_ERROR(_code) || \
   NEED_CLIENT_REFRESH_TBLMETA_ERROR(_code))

#define NEED_SCHEDULER_RETRY_ERROR(_code) \
  ((_code) == TSDB_CODE_RPC_REDIRECT || (_code) == TSDB_CODE_RPC_NETWORK_UNAVAIL)

#define REQUEST_MAX_TRY_TIMES 5

#define qFatal(...)                                                                           \
  do {                                                                                        \
    if (qDebugFlag & DEBUG_FATAL) {                                                           \
      taosPrintLog("QRY FATAL ", DEBUG_FATAL, tsLogEmbedded ? 255 : qDebugFlag, __VA_ARGS__); \
    }                                                                                         \
  } while (0)
#define qError(...)                                                                           \
  do {                                                                                        \
    if (qDebugFlag & DEBUG_ERROR) {                                                           \
      taosPrintLog("QRY ERROR ", DEBUG_ERROR, tsLogEmbedded ? 255 : qDebugFlag, __VA_ARGS__); \
    }                                                                                         \
  } while (0)
#define qWarn(...)                                                                          \
  do {                                                                                      \
    if (qDebugFlag & DEBUG_WARN) {                                                          \
      taosPrintLog("QRY WARN ", DEBUG_WARN, tsLogEmbedded ? 255 : qDebugFlag, __VA_ARGS__); \
    }                                                                                       \
  } while (0)
#define qInfo(...)                                                                     \
  do {                                                                                 \
    if (qDebugFlag & DEBUG_INFO) {                                                     \
      taosPrintLog("QRY ", DEBUG_INFO, tsLogEmbedded ? 255 : qDebugFlag, __VA_ARGS__); \
    }                                                                                  \
  } while (0)
#define qDebug(...)                                               \
  do {                                                            \
    if (qDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("QRY ", DEBUG_DEBUG, qDebugFlag, __VA_ARGS__); \
    }                                                             \
  } while (0)
#define qTrace(...)                                               \
  do {                                                            \
    if (qDebugFlag & DEBUG_TRACE) {                               \
      taosPrintLog("QRY ", DEBUG_TRACE, qDebugFlag, __VA_ARGS__); \
    }                                                             \
  } while (0)
#define qDebugL(...)                                                     \
  do {                                                                   \
    if (qDebugFlag & DEBUG_DEBUG) {                                      \
      taosPrintLongString("QRY ", DEBUG_DEBUG, qDebugFlag, __VA_ARGS__); \
    }                                                                    \
  } while (0)

#define QRY_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define QRY_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define QRY_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_H_*/
