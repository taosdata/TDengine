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

#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "../../../../libs/stream/inc/streamInt.h"

#define STREAM_RETURN_ROWS_NUM 4094

#define STREAM_CHECK_RET_GOTO(CMD)    \
    code = (CMD);                     \
    if (code != TSDB_CODE_SUCCESS) {  \
lino = __LINE__; \
      goto end;                       \
    }

#define STREAM_CHECK_NULL_GOTO(CMD,ret)        \
    if ((CMD) == NULL) {              \
      code = ret; \
lino = __LINE__; \
      goto end;                       \
    }

#define STREAM_CHECK_CONDITION_GOTO(CMD,ret)        \
    if (CMD) {              \
      code = ret; \
      lino = __LINE__; \
      goto end;                       \
    }

#define PRINT_LOG_END(code, lino) \
if (code != 0) { \
    vError("%s failed at line %d since %s", __func__, lino, tstrerror(code));\
  } else {\
    vDebug("%s done success", __func__);\
  }

static int32_t vnodeProcessStreamLastTsReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamFirstTsReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamTsdbMetaReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamTsDataReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamCalcDataReq(SVnode *pVnode, SRpcMsg *pRsp);
static int32_t vnodeProcessStreamWalMetaReq(SVnode *pVnode, SRpcMsg *pRsp,);
static int32_t vnodeProcessStreamWalTsDataReq(SVnode *pVnode, SRpcMsg *pRsp,);
static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode *pVnode, SRpcMsg *pRsp,);
static int32_t vnodeProcessStreamWalCalcDataReq(SVnode *pVnode, SRpcMsg *pRsp,);

SHashObj *streamInfoMap = NULL;
SHashObj *streamTaskMap = NULL;

typedef struct SStreamReaderDeployMsg {
  int64_t streamId;
}SStreamReaderDeployMsg;

typedef enum {
  STREAM_SCAN_GROUP_ONE_BY_ONE,
  STREAM_SCAN_ALL
} EScanMode;

typedef struct SStreamInfoObj{
  int32_t           order;
  SArray*           schemas;
  STimeWindow       twindows;
  uint64_t          suid;
  uint64_t          uid;
  int8_t            tableType;
  SNode*            pTagCond;
  SNode*            pTagIndexCond;
  SNode*            pConditions;
}SStreamInfoObj;

typedef struct SStreamReaderTaskOptions{
  int32_t           order;
  SArray*           schemas;
  STimeWindow       twindows;
  uint64_t          suid;
  uint64_t          uid;
  uint64_t          gid;
  int8_t            tableType;
  bool              groupSort;
  EScanMode         scanMode;
  SNode*            pTagCond;
  SNode*            pTagIndexCond;
  SNode*            pConditions;
}SStreamReaderTaskOptions;

typedef struct SStreamReaderTask {
  int64_t                     streamId;
  int64_t                     sessionId;
  SStorageAPI                 api;
  STsdbReader*                pReader;
  SHashObj*                   pIgnoreTables;
  SSDataBlock*                pResBlock;
  SSDataBlock*                pResBlockDst;
  SStreamReaderTaskOptions    options;
  void*                       pTableList;
  int32_t                     currentGroupIndex;
  SFilterInfo*                pFilterInfo;
  char*                       idStr;
}SStreamReaderTask;

static int32_t insertTableToIgnoreList(SHashObj **pIgnoreTables, uint64_t uid) {
  if (NULL == *pIgnoreTables) {
    *pIgnoreTables =
        taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (NULL == *pIgnoreTables) {
      return terrno;
    }
  }

  int32_t tempRes = taosHashPut(*pIgnoreTables, &uid, sizeof(uid), &uid, sizeof(uid));
  if (tempRes != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
    return tempRes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, SArray *schemas, STimeWindow  twindows, uint64_t suid) {
  pCond->order = order;
  pCond->numOfCols = taosArrayGetSize(schemas);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  if (!pCond->colList) {
    return terrno;
  }
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  if (pCond->pSlotList == NULL) {
    taosMemoryFreeClear(pCond->colList);
    return terrno;
  }

  pCond->twindows = twindows;
  pCond->suid = suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;
//  pCond->skipRollup = readHandle->skipRollup;

  pCond->notLoadData = false;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    SSchema *pSchema = taosArrayGet(schemas, i);
    pColInfo->type = pSchema[i].type;
    pColInfo->bytes = pSchema[i].bytes;
    pColInfo->colId = pSchema[i].colId;
    pColInfo->pk = pSchema[i].flags & COL_IS_KEY;

    pCond->pSlotList[i] = i;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId){
  SSchema* pSchema = taosArrayReserve(schemas, 1);
  if (pSchema == NULL){
    return terrno;
  }
  pSchema->type = type;
  pSchema->bytes = bytes;
  pSchema->colId = colId;
  return 0;
}

static int32_t addColData(SSDataBlock* pResBlock, int32_t colId, void* data){
  SColumnInfoData* pSrc = taosArrayGet(pResBlock->pDataBlock, colId);
  if (pSrc == NULL) {
    return terrno;
  }

  memcpy(pSrc->pData + pResBlock->info.rows * pSrc->info.bytes, data, pSrc->info.bytes);
  return 0;
}

static int32_t getTableDataInfo(SStreamReaderTask* pTask, bool *hasNext){
  int32_t code = pTask->api.tsdReader.tsdNextDataBlock(pTask->pReader, hasNext);
  if (code != TSDB_CODE_SUCCESS) {
    pTask->api.tsdReader.tsdReaderReleaseDataBlock(pTask->pReader);
  }

  return code;
}

static int32_t getTableData(SStreamReaderTask* pTask, SSDataBlock** ppRes){
  return pTask->api.tsdReader.tsdReaderRetrieveDataBlock(pTask->pReader, ppRes, NULL);
}

static int32_t createDataBlockForStream(SArray *schemas, SSDataBlock** pBlockRet) {
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t      numOfCols = taosArrayGetSize(schemas);
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSchema *pSchema = taosArrayGet(schemas, i);
    STREAM_CHECK_NULL_GOTO(pSchema, terrno);
    SColumnInfoData idata =
        createColumnInfoData(pSchema->type, pSchema->bytes, pSchema->colId);

    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  }

end:
  PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

static void releaseStreamTask(SStreamReaderTask *pTask) {
  if (pTask == NULL) return;
  taosHashCleanup(pTask->pIgnoreTables);
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  qStreamDestroyTableList(pTask->pTableList);
  pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
  filterFreeInfo(pTask->pFilterInfo);
  taosMemoryFree(pTask);
}

static int32_t createStreamTask(SVnode *pVnode, SStreamReaderTaskOptions* options, SStreamReaderTask** ppTask){
  int32_t code = 0;
  int32_t lino = 0;
  SStreamReaderTask *pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTask));
  STREAM_CHECK_NULL_GOTO(pTask, terrno);
  initStorageAPI(&pTask->api);
  pTask->pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(pTask->pIgnoreTables, terrno);

  pTask->options = *options;
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(options->schemas, &pTask->pResBlock));
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(options->schemas, &pTask->pResBlockDst));
  STREAM_CHECK_RET_GOTO(filterInitFromNode(options->pConditions, &pTask->pFilterInfo, 0));
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, options->suid, options->uid, options->tableType, options->groupSort,
    options->pTagCond, options->pTagIndexCond, &pTask->api, &pTask->pTableList));
  if (options->gid != 0) {
    int32_t index = qStreamGetGroupIndex(pTask->pTableList, options->gid);
    STREAM_CHECK_CONDITION_GOTO(index < 0, TSDB_CODE_INVALID_PARA);
    pTask->currentGroupIndex = index;
  }

  int32_t pNum = 1;
  STableKeyInfo* pList = NULL;
  if (options->scanMode = STREAM_SCAN_GROUP_ONE_BY_ONE) {
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum))
  } else if (options->scanMode = STREAM_SCAN_ALL) {
    pList = taosArrayGet(pTask->pTableList, 0);
    STREAM_CHECK_NULL_GOTO(pList, terrno);
    pNum = taosArrayGetSize(pTask->pTableList);
  }

  SQueryTableDataCond pCond = {0};
  STREAM_CHECK_RET_GOTO(initQueryTableDataCond(&pCond, options->order, options->schemas, options->twindows, options->suid));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(pVnode, &pCond, &pList, pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, &pTask->pIgnoreTables));
  *ppTask = pTask;
  pTask = NULL;

end:
  PRINT_LOG_END(code, lino);
  releaseStreamTask(pTask);
  taosMemoryFree(pTask);
  return code;
}

SStreamInfoObj* createStreamInfo(const SStreamReaderDeployMsg* pMsg){
  SStreamInfoObj* sStreamInfo = taosMemoryCalloc(1, sizeof(SStreamInfoObj));
  if (sStreamInfo == NULL){
    return NULL;
  }
  return sStreamInfo;
}

int32_t stReaderStreamDeploy(SVnode *pVnode, SRpcMsg *pMsg){
  int32_t code = 0;
  int32_t lino = 0;
  SStreamReaderDeployMsg* deployMsg = NULL;
  if (streamInfoMap == NULL){
    streamInfoMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    STREAM_CHECK_NULL_GOTO(streamInfoMap, terrno);
  }
  SStreamInfoObj* info = createStreamInfo(deployMsg);
  STREAM_CHECK_NULL_GOTO(info, terrno);

  STREAM_CHECK_RET_GOTO(taosHashPut(streamInfoMap, &deployMsg->streamId, sizeof(deployMsg->streamId), &info, POINTER_BYTES));

end:
  if (code != 0) {
    taosHashCleanup(streamInfoMap);

  }
  return code;
}

int32_t stReaderStreamUndeploy(SVnode *pVnode, SRpcMsg *pMsg){
  SStreamReaderDeployMsg* deployMsg = NULL;
  taosHashCleanup(streamInfoMap);
  int32_t code = taosHashRemove(streamInfoMap, &deployMsg->streamId, sizeof(deployMsg->streamId));
  if (code != 0){
    vError("failed to remove stream");
  }
  return code;
}
//int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);

static int32_t buildTsRsp(const SStreamTsResponse * tsRsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t len = 0;
  tEncodeSize(tEncodeSStreamTsResponse, &tsRsp, len, code);
  if(code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  void* buf = rpcMallocCont(len);
  if (buf == NULL) {
    return terrno;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  code = tEncodeSStreamTsResponse(&encoder, &tsRsp);
  tEncoderClear(&encoder);
  if (code < 0) {
    taosMemoryFree(buf);
    return TSDB_CODE_INVALID_PARA;
  }
  *data = buf;
  *size = len;
  return code;
}

static int32_t buildRsp(SSDataBlock* pBlock, void** data, size_t* size) {
  size_t  dataEncodeSize = blockGetEncodeSize(pBlock);
  void*   buf = rpcMallocCont(dataEncodeSize);
  if (buf == NULL) {
    return terrno;
  }
  int32_t actualLen = blockEncode(pBlock, buf, dataEncodeSize, taosArrayGetSize(pBlock->pDataBlock));
  if (actualLen < 0) {
    taosMemoryFree(buf);
    return terrno;
  }
  *data = buf;
  *size = dataEncodeSize;
  return 0;
}

static int32_t resetTsdbReader(SStreamReaderTask* pTask) {
  int32_t pNum = 1;
  STableKeyInfo* pList = NULL;
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdSetQueryTableList(pTask->pReader, pList, pNum));

  SQueryTableDataCond pCond = {0};
  STREAM_CHECK_RET_GOTO(initQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas, pTask->options.twindows, pTask->options.suid));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderResetStatus(pTask->pReader, &pCond));

end:
  PRINT_LOG_END(code, lino);
  return code;
}

static int32_t vnodeProcessStreamLastTsReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  SStreamReaderTask* pTask = NULL;
  SStreamTsResponse lastTsRsp = {0};
  void*  buf  = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamLastTsRequest), TSDB_CODE_INVALID_MSG);

  SStreamLastTsRequest* lastTsReq = (SStreamLastTsRequest*)pMsg->pCont;
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &lastTsReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))     // last ts

  SStreamReaderTaskOptions options = {
    .suid       = sStreamInfo->suid,
    .uid        = sStreamInfo->uid,
    .tableType  = sStreamInfo->tableType,
    .groupSort  = true,
    .order      = TSDB_ORDER_DESC,
    .twindows   = sStreamInfo->twindows,
    .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
    .schemas    = schemas,
    .scanMode   = STREAM_SCAN_GROUP_ONE_BY_ONE,
    .gid        = 0
  };
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));

  lastTsRsp.tsInfo = taosArrayInit(qStreamGetTableListGroupNum(pTask->pTableList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(lastTsRsp.tsInfo, terrno);

  lastTsRsp.ver = pMsg->info.conn.applyTerm;
  int64_t ts = INT64_MIN;
  while(true){
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      if (ts != INT64_MIN) {
        STsInfo* tsInfo = taosArrayReserve(lastTsRsp.tsInfo, 1);
        STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
        tsInfo->ts  = ts;
        tsInfo->gId = pTask->pResBlock->info.id.groupId;
      }

      pTask->currentGroupIndex++;
      if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
        break;
      }
      resetTsdbReader(pTask);
      STREAM_CHECK_RET_GOTO(resetTsdbReader(pTask));
      ts = INT64_MIN;
      continue;
    }
    if (pTask->pResBlock->info.window.ekey > ts) {
      ts = pTask->pResBlock->info.window.ekey;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);
  }

  vDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildTsRsp(&lastTsRsp, &buf, &size))
end:
  PRINT_LOG_END(code, lino);

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  taosArrayDestroy(lastTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamFirstTsReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  SStreamReaderTask* pTask = NULL;
  SStreamTsResponse firstTsRsp = {0};
  void*             buf = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamFirstTsRequest), TSDB_CODE_INVALID_MSG);

  SStreamFirstTsRequest* firstTsReq = (SStreamFirstTsRequest*)pMsg->pCont;
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &firstTsReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))     // last ts

  SStreamReaderTaskOptions options = {
    .suid       = sStreamInfo->suid,
    .uid        = sStreamInfo->uid,
    .tableType  = sStreamInfo->tableType,
    .groupSort  = true,
    .order      = TSDB_ORDER_ASC,
    .twindows   = STimeWindow{.skey = firstTsReq->startTime, .ekey = INT64_MAX},
    .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
    .schemas    = schemas,
    .scanMode   = STREAM_SCAN_GROUP_ONE_BY_ONE,
      .gid        = 0
  };
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));

  firstTsRsp.tsInfo = taosArrayInit(qStreamGetTableListGroupNum(pTask->pTableList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(firstTsRsp.tsInfo, terrno);

  int64_t ts = INT64_MAX;
  while(true){
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      if (ts != INT64_MAX) {
        STsInfo* tsInfo = taosArrayReserve(firstTsRsp.tsInfo, 1);
        STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
        tsInfo->ts  = ts;
        tsInfo->gId = pTask->pResBlock->info.id.groupId;
      }

      pTask->currentGroupIndex++;
      if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
        break;
      }
      STREAM_CHECK_RET_GOTO(resetTsdbReader(pTask));
      ts = INT64_MAX;
      continue;
    }
    if (ts <= pTask->pResBlock->info.window.skey) {
      ts = pTask->pResBlock->info.window.skey;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);
  }

  vDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);

  STREAM_CHECK_RET_GOTO(buildTsRsp(&firstTsRsp, &buf, &size));
end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  taosArrayDestroy(firstTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*  buf  = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamTsdbMetaRequest), TSDB_CODE_INVALID_MSG);
  SStreamTsdbMetaRequest* metaReq = (SStreamTsdbMetaRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbMetaRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &metaReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))       // uid
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))       // gid
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))       // skey
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))       // ekey
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))       // nrows

    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = metaReq->startTime, .ekey = INT64_MAX},
      .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
      .schemas    = schemas,
      .scanMode   = STREAM_SCAN_ALL,
      .gid        = 0
    };
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  pTask->pResBlockDst->info.rows = 0;
  while(true){
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      break;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.id.groupId));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 4, &pTask->pResBlock->info.rows));

    pTask->pResBlockDst->info.rows++;
    if (pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  return code;
}

static int32_t vnodeProcessStreamTsDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  SStreamReaderTask* pTask = NULL;
  void*  buf  = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamTsdbTsDataRequest), TSDB_CODE_INVALID_MSG);
  SStreamTsdbTsDataRequest* tsReq = (SStreamTsdbTsDataRequest*)pMsg->pCont;

  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tsReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

  SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = tsReq->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tsReq->skey, .ekey = tsReq->ekey},
    .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
      .schemas    = schemas,
    .scanMode   = STREAM_SCAN_ALL,
    .gid        = 0
  };
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      break;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
  }

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
end:
  PRINT_LOG_END(code, lino);

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas =  NULL;
  void*  buf  = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamTsdbTriggerDataRequest), TSDB_CODE_INVALID_MSG);

  SStreamTsdbTriggerDataRequest* tDataReq = (SStreamTsdbTriggerDataRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbTriggerDataRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tDataReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))

    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tDataReq->startTime, .ekey = INT64_MAX},
      .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
      .schemas    = schemas,
      .scanMode   = STREAM_SCAN_ALL,
      .gid        = 0
    };
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      goto end;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    if (pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }


  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  return code;
}

// static int32_t a() {
//   STableKeyInfo* tmp = (STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->currentTable);
//   if (!tmp) {
//     taosRUnLockLatch(&pTaskInfo->lock);
//     (*ppRes) = NULL;
//     QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
//   }
//
//   tInfo = *tmp;
//   taosRUnLockLatch(&pTaskInfo->lock);
//
//   code = pAPI->tsdReader.tsdSetQueryTableList(pInfo->base.dataReader, &tInfo, 1);
//   QUERY_CHECK_CODE(code, lino, _end);
//   qDebug("set uid:%" PRIu64 " into scanner, total tables:%d, index:%d/%d %s", tInfo.uid, numOfTables,
//          pInfo->currentTable, numOfTables, GET_TASKID(pTaskInfo));
//
//   code = pAPI->tsdReader.tsdReaderResetStatus(pInfo->base.dataReader, &pInfo->base.cond);
// }

static int32_t vnodeProcessStreamCalcDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*  buf  = NULL;
  size_t size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamTsdbCalcDataRequest), TSDB_CODE_INVALID_MSG);

  SStreamTsdbCalcDataRequest* tCalcDataReq = (SStreamTsdbCalcDataRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbCalcDataRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tCalcDataReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tCalcDataReq->skey, .ekey = tCalcDataReq->ekey},
      .pTagCond   = sStreamInfo->pTagCond,
    .pTagIndexCond = sStreamInfo->pTagIndexCond,
    .pConditions        = sStreamInfo->pConditions,
      .schemas    = schemas,
      .scanMode   = STREAM_SCAN_GROUP_ONE_BY_ONE,
      .gid        = tCalcDataReq->gid
    };
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  }
  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext){
      goto end;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    if (pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  return code;
}

int32_t retrieveWalMetaData(SSubmitTbData* pSubmitTbData, SSDataBlock* pBlock) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t        code = 0;
  int32_t        line = 0;

  int32_t ver = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;

  int32_t numOfRows = 0;
  int64_t skey = 0;
  int64_t ekey = 0;
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    TSDB_CHECK_NULL(pCol, code, line, END, terrno);
    numOfRows = pCol->nVal;

    SColVal colVal = {0};
    code = tColDataGetValue(pCol, 0, &colVal);
    TSDB_CHECK_CODE(code, line, END);
    skey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);

    code = tColDataGetValue(pCol, numOfRows - 1, &colVal);
    TSDB_CHECK_CODE(code, line, END);
    ekey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);

  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
    SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, 0);
    SColVal colVal = {0};
    STColumn column = {.colId = 1, .type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = LONG_BYTES};
    STSchema pTSchema = {.numOfCols = 1, .version = ver, .columns = &column};
    code = tRowGet(pRow, &pTSchema, 0, &colVal);
    skey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
    pRow = taosArrayGetP(pSubmitTbData->aRowP, numOfRows - 1);
    code = tRowGet(pRow, &pTSchema, 0, &colVal);
    ekey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
  }

  code = blockDataEnsureCapacity(pBlock, numOfRows);
  TSDB_CHECK_CODE(code, line, END);
  addColData(pBlock, 0, &uid);
  addColData(pBlock, 1, &uid);
  addColData(pBlock, 2, &skey);
  addColData(pBlock, 3, &ekey);
  addColData(pBlock, 4, &ver);
  addColData(pBlock, 4, &numOfRows);
  pBlock->info.rows++;

END:
  if (code != 0) {
    vError("tqRetrieveDataBlock failed, line:%d, msg:%s", line, tstrerror(code));
  }
  return code;
}

int32_t retrieveWalData(SSubmitTbData* pSubmitTbData, void* pTableList, SSDataBlock* pBlock, STimeWindow* window) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t        code = 0;
  int32_t        lino = 0;

  int32_t ver = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  int32_t numOfRows = 0;
  // code = blockDataEnsureCapacity(pBlock, numOfRows);
  // TSDB_CHECK_CODE(code, line, END);

  STREAM_CHECK_CONDITION_GOTO(!qStreamUidInTableList(pTableList, uid), TDB_CODE_SUCCESS);
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {

    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    STREAM_CHECK_NULL_GOTO(pCol, terrno);
    int32_t rowStart = 0;
    int32_t rowEnd   = pCol->nVal;
    if (window != NULL) {
      SColVal colVal = {0};
      rowStart = -1;
      rowEnd   = -1;
      for (int32_t k = 0; k < pCol->nVal; k++) {
        STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, k, &colVal));
        int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
        if (ts >= window->skey && rowStart == -1) {
          rowStart = k;
        }
        if (ts > window->ekey && rowEnd == -1) {
          rowEnd = k;
        }

      }
      STREAM_CHECK_CONDITION_GOTO (rowStart == -1 || rowStart == rowEnd, TDB_CODE_SUCCESS);

      if (rowStart != -1 && rowEnd == -1) {
        rowEnd = pCol->nVal;
      }
    }

    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pColData, terrno);
      for (int32_t j = 0; j < taosArrayGetSize(pSubmitTbData->aCol); j++) {
        SColData* pCol = taosArrayGet(pSubmitTbData->aCol, j);
        STREAM_CHECK_NULL_GOTO(pCol, terrno);
        numOfRows = pCol->nVal;
        if (pCol->cid == pColData->info.colId) {
          for (int32_t k = rowStart; k < rowEnd; k++) {
            SColVal colVal = {0};
            STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, k, &colVal));
            STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, k, VALUE_GET_TRIVIAL_DATUM(&colVal.value), !COL_VAL_IS_VALUE(&colVal)));
          }
        }
      }
    }
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pColData, terrno);
      for (int32_t j = 0; j < numOfRows; j++) {
        SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, j);
        STREAM_CHECK_NULL_GOTO(pRow, terrno);
        SColVal colVal = {0};
        STColumn column = {.colId = pColData->info.colId, .type = pColData->info.type, .bytes = pColData->info.bytes};
        STSchema pTSchema = {.numOfCols = 1, .version = ver, .columns = &column};
        STREAM_CHECK_RET_GOTO(tRowGet(pRow, &pTSchema, pColData->info.colId, &colVal));
        STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, j, VALUE_GET_TRIVIAL_DATUM(&colVal.value), !COL_VAL_IS_VALUE(&colVal)));
      }
    }
  }

  pBlock->info.rows += numOfRows;

end:
  PRINT_LOG_END(code, lino)
  return code;
}

void scanWal(void* pVnode, SSDataBlock* pBlock, int64_t ver) {
  SWalReader* pWalReader = walOpenReader(pVnode, NULL, 0);
  walReaderSeekVer(pWalReader, ver);

  while (1) {
    // try next message in wal file
    if (walNextValidMsg(pWalReader) < 0) {
      return;
    }
    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int64_t ver = pWalReader->pHead->head.version;

    // if (pHead->msgType == TDMT_VND_DELETE) {
    // }
    SSubmitReq2 submit = {0};
    int32_t         nextBlk = 0;
    SDecoder decoder = {0};
    tDecoderInit(&decoder, pBody, bodyLen);
    int32_t code = tDecodeSubmitReq(&decoder, &submit, NULL);
    tDecoderClear(&decoder);

    int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);
    while (nextBlk < numOfBlocks) {
      vDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
      SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
      if (pSubmitTbData == NULL) {
        vError("stream reader get data block null");
        return;
      }
      retrieveWalData(pSubmitTbData, pBlock);
      nextBlk += 1;
    }
    tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  }
  walCloseReader(pWalReader);
}

int32_t scanWalOneVer(void* pVnode, void* pTableList, SSDataBlock* pBlock, SSDataBlock* pBlockRet, int64_t ver, STimeWindow window) {
  int32_t code = 0;
  int32_t lino = 0;
  SWalReader* pWalReader = walOpenReader(pVnode, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  STREAM_CHECK_RET_GOTO(walReaderSeekVer(pWalReader, ver));

  while (1) {
    if (walNextValidMsg(pWalReader) < 0) {
      return;
    }
    if (pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      continue;
    }
    break;
  }
  void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
  int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
  int64_t ver = pWalReader->pHead->head.version;

  SSubmitReq2 submit = {0};
  int32_t         nextBlk = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, pBody, bodyLen);
  STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));
  tDecoderClear(&decoder);

  int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);
  while (nextBlk < numOfBlocks) {
    vDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
    SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
    STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
    STREAM_CHECK_RET_GOTO(retrieveWalData(pSubmitTbData, pTableList, pBlock, window));
    blockDataMerge(pBlockRet, pBlock);
    blockDataCleanup(pBlock);
    nextBlk += 1;
  }
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);

end:
  PRINT_LOG_END(code, lino);
  return code;
}

static int32_t vnodeProcessStreamWalMetaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamWalMetaRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamWalMetaRequest* tWalMetaReq = (SStreamWalMetaRequest*)pMsg->pCont;
  SArray* schemas = taosArrayInit(4, sizeof(SSchema));
  if (schemas == NULL){
    return terrno;
  }
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // gid
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // uid
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))   // skey
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))   // ekey
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 5))   // ver
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 6))   // nrows

  SSDataBlock* pBlock = createDataBlockForStream(schemas);

  scanWal(pVnode, pBlock, tWalMetaReq->lastVer);

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pBlock, &buf, &size));

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = 0};
  tmsgSendRsp(&rsp);

end:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vDebug("vgId:%d %s done success", TD_VID(pVnode), __func__);
  }
  return code;
}


static int32_t processWalVerData(SVnode *pVnode, SStreamInfoObj* sStreamInfo, SArray* schemas, int64_t ver, STimeWindow window, SSDataBlock** pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  SFilterInfo* pFilterInfo = NULL;
  void*        pTableList  = NULL;
  SStorageAPI  api = {0};
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;

  if (sStreamInfo->pConditions != NULL) {
    STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0));
  }

  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, sStreamInfo->suid, sStreamInfo->uid, sStreamInfo->tableType, false,
    sStreamInfo->pTagCond, sStreamInfo->pTagIndexCond, &api, &pTableList));

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock1));
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock2));

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pTableList, pBlock1, pBlock2, ver, window));
  STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock2, pFilterInfo));

  *pBlock = pBlock2;
  pBlock2 = NULL;

end:
  PRINT_LOG_END(code, lino);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock2);
  qStreamDestroyTableList(pTableList);
}

static int32_t vnodeProcessStreamWalTsDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*  buf  = NULL;
  size_t size = 0;

  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamWalTsDataRequest), TSDB_CODE_INVALID_MSG);
  SStreamWalTsDataRequest* tTsDataReq = (SStreamWalTsDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tTsDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // filter col

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamInfo, schemas, tTsDataReq->ver, &pBlock));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*  buf  = NULL;
  size_t size = 0;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamWalTriggerDataRequest), TSDB_CODE_INVALID_MSG);
  SStreamWalTriggerDataRequest* tTsDataReq = (SStreamWalTriggerDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tTsDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // trigger col
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))   // filter col

  STimeWindow window = {.ekey = tDataReq->ekey, .skey = tDataReq->skey};
  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamInfo, schemas, tTsDataReq->ver, window, &pBlock));


  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalTriggerDataReqf(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*  buf  = NULL;
  size_t size = 0;
  SFilterInfo* pFilterInfo = NULL;
  SSDataBlock* pBlock = NULL;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamWalTriggerDataRequest), TSDB_CODE_INVALID_MSG);
  SStreamWalTriggerDataRequest* tDataReq = (SStreamWalTriggerDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // trigger col
  if (sStreamInfo->pConditions != NULL) {
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))   // filter col
    STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0));
  }

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock));
  scanWalOneVer(pVnode, pBlock, tDataReq->ver);
  STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pFilterInfo));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

  end:
    PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock);
  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode *pVnode, SRpcMsg *pMsg){
  int32_t code = 0;
  EStreamTriggerRequestType *type = (EStreamTriggerRequestType*)(pMsg->pCont);
  switch (*type) {
    case TRIGGER_REQUEST_LAST_TS:
      code = vnodeProcessStreamLastTsReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_FIRST_TS:
      code = vnodeProcessStreamFirstTsReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_TSDB_META:
      code = vnodeProcessStreamTsdbMetaReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_TSDB_TS_DATA:
      code = vnodeProcessStreamTsDataReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_TSDB_TRIGGER_DATA:
      code = vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_TSDB_CALC_DATA:
      code = vnodeProcessStreamCalcDataReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_WAL_META:
      code = vnodeProcessStreamWalMetaReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_WAL_TS_DATA:
      code = vnodeProcessStreamWalTsDataReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_WAL_TRIGGER_DATA:
      code = vnodeProcessStreamWalTriggerDataReq(pVnode, pMsg);
      break;
    case TRIGGER_REQUEST_WAL_CALC_DATA:
      code = vnodeProcessStreamWalCalcDataReq(pVnode, pMsg);
      break;
    default:
      code = TSDB_CODE_INVALID_MSG;
      break;
  }
  return code;
}