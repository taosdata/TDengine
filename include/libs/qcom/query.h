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

// clang-foramt off
#ifdef __cplusplus
extern "C" {
#endif

#include "systable.h"
#include "tarray.h"
#include "thash.h"
#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"

typedef enum {
  JOB_TASK_STATUS_NULL = 0,
  JOB_TASK_STATUS_INIT,
  JOB_TASK_STATUS_EXEC,
  JOB_TASK_STATUS_PART_SUCC,
  JOB_TASK_STATUS_FETCH,
  JOB_TASK_STATUS_SUCC,
  JOB_TASK_STATUS_FAIL,
  JOB_TASK_STATUS_DROP,
  JOB_TASK_STATUS_MAX,
} EJobTaskType;

typedef enum {
  TASK_TYPE_PERSISTENT = 1,
  TASK_TYPE_TEMP,
} ETaskType;

typedef enum {
  TARGET_TYPE_MNODE = 1,
  TARGET_TYPE_VNODE,
  TARGET_TYPE_OTHER,
} ETargetType;

typedef enum {
  TCOL_TYPE_COLUMN = 1,
  TCOL_TYPE_TAG,
  TCOL_TYPE_NONE,
} ETableColumnType;

#define QUERY_POLICY_VNODE  1
#define QUERY_POLICY_HYBRID 2
#define QUERY_POLICY_QNODE  3
#define QUERY_POLICY_CLIENT 4

#define QUERY_RSP_POLICY_DELAY 0
#define QUERY_RSP_POLICY_QUICK 1

#define QUERY_MSG_MASK_SHOW_REWRITE() (1 << 0)
#define QUERY_MSG_MASK_AUDIT()        (1 << 1)
#define QUERY_MSG_MASK_VIEW()         (1 << 2)
#define TEST_SHOW_REWRITE_MASK(m)     (((m) & QUERY_MSG_MASK_SHOW_REWRITE()) != 0)
#define TEST_AUDIT_MASK(m)            (((m) & QUERY_MSG_MASK_AUDIT()) != 0)
#define TEST_VIEW_MASK(m)             (((m) & QUERY_MSG_MASK_VIEW()) != 0)

typedef struct STableComInfo {
  uint8_t  numOfTags;     // the number of tags in schema
  uint8_t  precision;     // the number of precision
  col_id_t numOfColumns;  // the number of columns
  int32_t  rowSize;       // row size of the schema
} STableComInfo;

typedef struct SIndexMeta {
#if defined(WINDOWS) || defined(_TD_DARWIN_64)
  size_t avoidCompilationErrors;
#endif

} SIndexMeta;

typedef struct SExecResult {
  int32_t  code;
  uint64_t numOfRows;
  uint64_t numOfBytes;
  int32_t  msgType;
  void*    res;
} SExecResult;

#pragma pack(push, 1)
typedef struct SCTableMeta {
  uint64_t uid;
  uint64_t suid;
  int32_t  vgId;
  int8_t   tableType;
} SCTableMeta;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct STableMeta {
  // BEGIN: KEEP THIS PART SAME WITH SCTableMeta
  uint64_t uid;
  uint64_t suid;
  int32_t  vgId;
  int8_t   tableType;
  // END: KEEP THIS PART SAME WITH SCTableMeta

  // if the table is TSDB_CHILD_TABLE, the following information is acquired from the corresponding super table meta
  // info
  int32_t       sversion;
  int32_t       tversion;
  STableComInfo tableInfo;
  SSchema       schema[];
} STableMeta;
#pragma pack(pop)

typedef struct SViewMeta {
  uint64_t viewId;
  char*    user;
  char*    querySql;
  int8_t   precision;
  int8_t   type;
  int32_t  version;
  int32_t  numOfCols;
  SSchema* pSchema;
} SViewMeta;

typedef struct SDBVgInfo {
  int32_t   vgVersion;
  int16_t   hashPrefix;
  int16_t   hashSuffix;
  int8_t    hashMethod;
  int32_t   numOfTable;  // DB's table num, unit is TSDB_TABLE_NUM_UNIT
  int64_t   stateTs;
  SHashObj* vgHash;   // key:vgId, value:SVgroupInfo
  SArray*   vgArray;  // SVgroupInfo
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

typedef struct SViewMetaOutput {
  char     name[TSDB_VIEW_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  char*    querySql;
  int8_t   precision;
  int32_t  numOfCols;
  SSchema* pSchema;
} SViewMetaOutput;

typedef struct SDataBuf {
  int32_t  msgType;
  void*    pData;
  uint32_t len;
  void*    handle;
  int64_t  handleRefId;
  SEpSet*  pEpSet;
} SDataBuf;

typedef struct STargetInfo {
  ETargetType type;
  char*       dbFName;  // used to update db's vgroup epset
  int32_t     vgId;
} STargetInfo;

typedef struct SBoundColInfo {
  int16_t* pColIndex;  // bound index => schema index
  int32_t  numOfCols;
  int32_t  numOfBound;
} SBoundColInfo;

typedef struct STableDataCxt {
  STableMeta*    pMeta;
  STSchema*      pSchema;
  SBoundColInfo  boundColsInfo;
  SArray*        pValues;
  SSubmitTbData* pData;
  TSKEY          lastTs;
  bool           ordered;
  bool           duplicateTs;
} STableDataCxt;

typedef int32_t (*__async_send_cb_fn_t)(void* param, SDataBuf* pMsg, int32_t code);
typedef int32_t (*__async_exec_fn_t)(void* param);

typedef struct SRequestConnInfo {
  void*    pTrans;
  uint64_t requestId;
  int64_t  requestObjRefId;
  SEpSet   mgmtEps;
} SRequestConnInfo;

typedef void (*__freeFunc)(void* param);

// todo add creator/destroyer function
typedef struct SMsgSendInfo {
  __async_send_cb_fn_t fp;      // async callback function
  STargetInfo          target;  // for update epset
  __freeFunc           paramFreeFp;
  void*                param;
  uint64_t             requestId;
  uint64_t             requestObjRefId;
  int32_t              msgType;
  SDataBuf             msgInfo;
} SMsgSendInfo;

typedef struct SQueryNodeStat {
  int32_t tableNum;  // vg table number, unit is TSDB_TABLE_NUM_UNIT
} SQueryNodeStat;

typedef struct SQueryStat {
  int64_t inputRowNum;
  int32_t inputRowSize;
} SQueryStat;

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

void destroySendMsgInfo(SMsgSendInfo* pMsgBody);

void destroyAhandle(void* ahandle);

int32_t asyncSendMsgToServerExt(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, SMsgSendInfo* pInfo,
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
int32_t asyncSendMsgToServer(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, SMsgSendInfo* pInfo);

int32_t queryBuildUseDbOutput(SUseDbOutput* pOut, SUseDbRsp* usedbRsp);

void initQueryModuleMsgHandle();

const SSchema* tGetTbnameColumnSchema();
bool           tIsValidSchema(struct SSchema* pSchema, int32_t numOfCols, int32_t numOfTags);

int32_t queryCreateCTableMetaFromMsg(STableMetaRsp* msg, SCTableMeta* pMeta);
int32_t queryCreateTableMetaFromMsg(STableMetaRsp* msg, bool isSuperTable, STableMeta** pMeta);
char*   jobTaskStatusStr(int32_t status);

SSchema createSchema(int8_t type, int32_t bytes, col_id_t colId, const char* name);

void    destroyQueryExecRes(SExecResult* pRes);
int32_t dataConverToStr(char* str, int type, void* buf, int32_t bufSize, int32_t* len);
char*   parseTagDatatoJson(void* p);
int32_t cloneTableMeta(STableMeta* pSrc, STableMeta** pDst);
void    getColumnTypeFromMeta(STableMeta* pMeta, char* pName, ETableColumnType* pType);
int32_t cloneDbVgInfo(SDBVgInfo* pSrc, SDBVgInfo** pDst);
int32_t cloneSVreateTbReq(SVCreateTbReq* pSrc, SVCreateTbReq** pDst);
void    freeVgInfo(SDBVgInfo* vgInfo);
void    freeDbCfgInfo(SDbCfgInfo* pInfo);

extern int32_t (*queryBuildMsg[TDMT_MAX])(void* input, char** msg, int32_t msgSize, int32_t* msgLen,
                                          void* (*mallocFp)(int64_t));
extern int32_t (*queryProcessMsgRsp[TDMT_MAX])(void* output, char* msg, int32_t msgSize);

#define SET_META_TYPE_NULL(t)       (t) = META_TYPE_NULL_TABLE
#define SET_META_TYPE_CTABLE(t)     (t) = META_TYPE_CTABLE
#define SET_META_TYPE_TABLE(t)      (t) = META_TYPE_TABLE
#define SET_META_TYPE_BOTH_TABLE(t) (t) = META_TYPE_BOTH_TABLE

#define NEED_CLIENT_RM_TBLMETA_ERROR(_code)                                                   \
  ((_code) == TSDB_CODE_PAR_TABLE_NOT_EXIST || (_code) == TSDB_CODE_TDB_TABLE_NOT_EXIST ||    \
   (_code) == TSDB_CODE_PAR_INVALID_COLUMNS_NUM || (_code) == TSDB_CODE_PAR_INVALID_COLUMN || \
   (_code) == TSDB_CODE_PAR_TAGS_NOT_MATCHED || (_code) == TSDB_CODE_PAR_VALUE_TOO_LONG ||    \
   (_code) == TSDB_CODE_PAR_INVALID_DROP_COL || ((_code) == TSDB_CODE_TDB_INVALID_TABLE_ID))
#define NEED_CLIENT_REFRESH_VG_ERROR(_code) \
  ((_code) == TSDB_CODE_VND_HASH_MISMATCH || (_code) == TSDB_CODE_VND_INVALID_VGROUP_ID)
#define NEED_CLIENT_REFRESH_TBLMETA_ERROR(_code) \
  ((_code) == TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER || (_code) == TSDB_CODE_MND_INVALID_SCHEMA_VER)
#define NEED_CLIENT_HANDLE_ERROR(_code)                                          \
  (NEED_CLIENT_RM_TBLMETA_ERROR(_code) || NEED_CLIENT_REFRESH_VG_ERROR(_code) || \
   NEED_CLIENT_REFRESH_TBLMETA_ERROR(_code))

#define SYNC_UNKNOWN_LEADER_REDIRECT_ERROR(_code)                                    \
  ((_code) == TSDB_CODE_SYN_NOT_LEADER || (_code) == TSDB_CODE_SYN_INTERNAL_ERROR || \
   (_code) == TSDB_CODE_VND_STOPPED || (_code) == TSDB_CODE_APP_IS_STARTING || (_code) == TSDB_CODE_APP_IS_STOPPING)
#define SYNC_SELF_LEADER_REDIRECT_ERROR(_code) \
  ((_code) == TSDB_CODE_SYN_NOT_LEADER || (_code) == TSDB_CODE_SYN_RESTORING || (_code) == TSDB_CODE_SYN_INTERNAL_ERROR)
#define SYNC_OTHER_LEADER_REDIRECT_ERROR(_code) ((_code) == TSDB_CODE_MNODE_NOT_FOUND)

#define NO_RET_REDIRECT_ERROR(_code)                                                   \
  ((_code) == TSDB_CODE_RPC_BROKEN_LINK || (_code) == TSDB_CODE_RPC_NETWORK_UNAVAIL || \
   (_code) == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED)

#define NEED_REDIRECT_ERROR(_code)                                              \
  (NO_RET_REDIRECT_ERROR(_code) || SYNC_UNKNOWN_LEADER_REDIRECT_ERROR(_code) || \
   SYNC_SELF_LEADER_REDIRECT_ERROR(_code) || SYNC_OTHER_LEADER_REDIRECT_ERROR(_code))

#define IS_VIEW_REQUEST(_type) ((_type) == TDMT_MND_CREATE_VIEW || (_type) == TDMT_MND_DROP_VIEW)

#define NEED_CLIENT_RM_TBLMETA_REQ(_type)                                                                  \
  ((_type) == TDMT_VND_CREATE_TABLE || (_type) == TDMT_MND_CREATE_STB || (_type) == TDMT_VND_DROP_TABLE || \
   (_type) == TDMT_MND_DROP_STB || (_type) == TDMT_MND_CREATE_VIEW || (_type) == TDMT_MND_DROP_VIEW)

#define NEED_SCHEDULER_REDIRECT_ERROR(_code)                                              \
  (SYNC_UNKNOWN_LEADER_REDIRECT_ERROR(_code) || SYNC_SELF_LEADER_REDIRECT_ERROR(_code) || \
   SYNC_OTHER_LEADER_REDIRECT_ERROR(_code))

#define REQUEST_TOTAL_EXEC_TIMES 2

#define IS_INFORMATION_SCHEMA_DB(_name) ((*(_name) == 'i') && (0 == strcmp(_name, TSDB_INFORMATION_SCHEMA_DB)))
#define IS_PERFORMANCE_SCHEMA_DB(_name) ((*(_name) == 'p') && (0 == strcmp(_name, TSDB_PERFORMANCE_SCHEMA_DB)))

#define IS_SYS_DBNAME(_dbname) (IS_INFORMATION_SCHEMA_DB(_dbname) || IS_PERFORMANCE_SCHEMA_DB(_dbname))

#define IS_AUDIT_DBNAME(_dbname)    ((*(_dbname) == 'a') && (0 == strcmp(_dbname, TSDB_AUDIT_DB)))
#define IS_AUDIT_STB_NAME(_stbname) ((*(_stbname) == 'o') && (0 == strcmp(_stbname, TSDB_AUDIT_STB_OPERATION)))
#define IS_AUDIT_CTB_NAME(_ctbname) \
  ((*(_ctbname) == 't') && (0 == strncmp(_ctbname, TSDB_AUDIT_CTB_OPERATION, TSDB_AUDIT_CTB_OPERATION_LEN)))

#define qFatal(...)                                                     \
  do {                                                                  \
    if (qDebugFlag & DEBUG_FATAL) {                                     \
      taosPrintLog("QRY FATAL ", DEBUG_FATAL, qDebugFlag, __VA_ARGS__); \
    }                                                                   \
  } while (0)
#define qError(...)                                                     \
  do {                                                                  \
    if (qDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("QRY ERROR ", DEBUG_ERROR, qDebugFlag, __VA_ARGS__); \
    }                                                                   \
  } while (0)
#define qWarn(...)                                                    \
  do {                                                                \
    if (qDebugFlag & DEBUG_WARN) {                                    \
      taosPrintLog("QRY WARN ", DEBUG_WARN, qDebugFlag, __VA_ARGS__); \
    }                                                                 \
  } while (0)
#define qInfo(...)                                               \
  do {                                                           \
    if (qDebugFlag & DEBUG_INFO) {                               \
      taosPrintLog("QRY ", DEBUG_INFO, qDebugFlag, __VA_ARGS__); \
    }                                                            \
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
       // clang-foramt on
