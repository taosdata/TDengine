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

#ifndef _TD_VNODE_TQ_H_
#define _TD_VNODE_TQ_H_

#include "vnodeInt.h"

#include "executor.h"
#include "os.h"
#include "thash.h"
#include "tmsg.h"
#include "tqueue.h"
#include "trpc.h"
#include "ttimer.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

// tqDebug ===================
// clang-format off
#define tqFatal(...) do { if (tqDebugFlag & DEBUG_FATAL) { taosPrintLog("TQ  FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define tqError(...) do { if (tqDebugFlag & DEBUG_ERROR) { taosPrintLog("TQ  ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define tqWarn(...)  do { if (tqDebugFlag & DEBUG_WARN)  { taosPrintLog("TQ  WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define tqInfo(...)  do { if (tqDebugFlag & DEBUG_INFO)  { taosPrintLog("TQ  ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define tqDebug(...) do { if (tqDebugFlag & DEBUG_DEBUG) { taosPrintLog("TQ  ", DEBUG_DEBUG, tqDebugFlag, __VA_ARGS__); }} while(0)
#define tqTrace(...) do { if (tqDebugFlag & DEBUG_TRACE) { taosPrintLog("TQ  ", DEBUG_TRACE, tqDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#define IS_OFFSET_RESET_TYPE(_t)  ((_t) < 0)

// tqExec
typedef struct {
  char* qmsg;  // SubPlanToString
} STqExecCol;

typedef struct {
  int64_t suid;
  char*   qmsg;  // SubPlanToString
  SNode*  node;
} STqExecTb;

typedef struct {
  SHashObj* pFilterOutTbUid;
} STqExecDb;

typedef struct {
  int8_t      subType;
  STqReader*  pTqReader;
  qTaskInfo_t task;
  union {
    STqExecCol execCol;
    STqExecTb  execTb;
    STqExecDb  execDb;
  };
  int32_t numOfCols;  // number of out pout column, temporarily used
} STqExecHandle;

typedef enum tq_handle_status {
  TMQ_HANDLE_STATUS_IDLE = 0,
  TMQ_HANDLE_STATUS_EXEC = 1,
} tq_handle_status;

typedef struct {
  char        subKey[TSDB_SUBSCRIBE_KEY_LEN];
  int64_t     consumerId;
  int32_t     epoch;
  int8_t      fetchMeta;
  int64_t     snapshotVer;
  SWalReader* pWalReader;
  SWalRef*    pRef;
  STqExecHandle    execHandle;  // exec
  SRpcMsg*         msg;
  tq_handle_status status;

  // for replay
  SSDataBlock* block;
  int64_t      blockTime;
} STqHandle;

struct STQ {
  SVnode*         pVnode;
  char*           path;
  SRWLatch        lock;
  SHashObj*       pPushMgr;    // subKey -> STqHandle
  SHashObj*       pHandle;     // subKey -> STqHandle
  SHashObj*       pCheckInfo;  // topic -> SAlterCheckInfo
  SHashObj*       pOffset;     // subKey -> STqOffsetVal
  TDB*            pMetaDB;
  TTB*            pExecStore;
  TTB*            pCheckStore;
  TTB*            pOffsetStore;
  SStreamMeta*    pStreamMeta;
};

int32_t tEncodeSTqHandle(SEncoder* pEncoder, const STqHandle* pHandle);
int32_t tDecodeSTqHandle(SDecoder* pDecoder, STqHandle* pHandle);
void    tqDestroyTqHandle(void* data);

// tqRead
int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, STaosxRsp* pRsp, SMqBatchMetaRsp* pBatchMetaRsp, STqOffsetVal* offset);
int32_t tqScanData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest);
int32_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, uint64_t reqId);

// tqExec
int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SPackedData submit, STaosxRsp* pRsp, int32_t* totalRows, int8_t sourceExcluded);
int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, void* pRsp, int32_t numOfCols, int8_t precision);
int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const void* pRsp,
                      int32_t type, int32_t vgId);
void    tqPushEmptyDataRsp(STqHandle* pHandle, int32_t vgId);

// tqMeta
int32_t tqMetaOpen(STQ* pTq);
void    tqMetaClose(STQ* pTq);
int32_t tqMetaSaveHandle(STQ* pTq, const char* key, const STqHandle* pHandle);
int32_t tqMetaSaveInfo(STQ* pTq, TTB* ttb, const void* key, int32_t kLen, const void* value, int32_t vLen);
int32_t tqMetaDeleteInfo(STQ* pTq, TTB* ttb, const void* key, int32_t kLen);
int32_t tqMetaCreateHandle(STQ* pTq, SMqRebVgReq* req, STqHandle* handle);
int32_t tqMetaDecodeCheckInfo(STqCheckInfo *info, void *pVal, int32_t vLen);
int32_t tqMetaDecodeOffsetInfo(STqOffset *info, void *pVal, int32_t vLen);
int32_t tqMetaSaveOffset(STQ* pTq, STqOffset* pOffset);
int32_t tqMetaGetHandle(STQ* pTq, const char* key, STqHandle** pHandle);
int32_t tqMetaGetOffset(STQ* pTq, const char* subkey, STqOffset** pOffset);
int32_t tqMetaTransform(STQ* pTq);
// tqSink
int32_t tqBuildDeleteReq(STQ* pTq, const char* stbFullName, const SSDataBlock* pDataBlock, SBatchDeleteReq* deleteReq,
                         const char* pIdStr, bool newSubTableRule);
void    tqSinkDataIntoDstTable(SStreamTask* pTask, void* vnode, void* data);

// tqOffset
int32_t tqBuildFName(char** data, const char* path, char* name);
int32_t tqOffsetRestoreFromFile(STQ* pTq, char* name);

// tq util
int32_t tqExtractDelDataBlock(const void* pData, int32_t len, int64_t ver, void** pRefBlock, int32_t type);
int32_t tqExtractDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg);
int32_t tqDoSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, const void* pRsp, int32_t epoch, int64_t consumerId,
                        int32_t type, int64_t sver, int64_t ever);
int32_t tqInitDataRsp(SMqDataRspCommon* pRsp, STqOffsetVal pOffset);
void    tqUpdateNodeStage(STQ* pTq, bool isLeader);
int32_t tqSetDstTableDataPayload(uint64_t suid, const STSchema* pTSchema, int32_t blockIndex, SSDataBlock* pDataBlock,
                                 SSubmitTbData* pTableData, int64_t earlyTs, const char* id);
int32_t doMergeExistedRows(SSubmitTbData* pExisted, const SSubmitTbData* pNew, const char* id);

int32_t buildAutoCreateTableReq(const char* stbFullName, int64_t suid, int32_t numOfCols, SSDataBlock* pDataBlock,
                                SArray* pTagArray, bool newSubTableRule, SVCreateTbReq** pReq);

#define TQ_ERR_GO_TO_END(c)          \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      goto END;                      \
    }                                \
  } while (0)

#define TQ_NULL_GO_TO_END(c)            \
  do {                                  \
    if (c == NULL) {                    \
      code = (terrno == 0 ? TSDB_CODE_OUT_OF_MEMORY : terrno);                   \
      goto END;                         \
    }                                   \
  } while (0)

#define TQ_SUBSCRIBE_NAME "subscribe"
#define TQ_OFFSET_NAME    "offset-ver0"

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TQ_H_*/
