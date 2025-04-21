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

#define STREAM_CHECK_GOTO(CMD)        \
    code = (CMD);                     \
    if (code != TSDB_CODE_SUCCESS) {  \
      goto end;                       \
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

typedef struct SStreamInfoObj{
  int32_t           order;
  SArray*           schemas;
  STimeWindow       twindows;
  uint64_t          suid;
  uint64_t          uid;
  int8_t            tableType;
}SStreamInfoObj;

typedef struct SStreamReaderTaskOptions{
  int32_t           order;
  SArray*           schemas;
  STimeWindow       twindows;
  uint64_t          suid;
  uint64_t          uid;
  int8_t            tableType;
  bool              groupSort;
  int32_t           pNum;
  STableKeyInfo*    pList;
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
  int32_t code = pTask->api.tsdReader.tsdReaderRetrieveDataBlock(pTask->pReader, ppRes, NULL);
  return code;
}

static SSDataBlock* createDataBlockForStream(SArray *schemas) {
  int32_t      numOfCols = taosArrayGetSize(schemas);
  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    terrno = code;
    return NULL;
  }
  code = blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM);
  if (code) {
    blockDataDestroy(pBlock);
    return code;
  }
//  pBlock->info.id.blockId = pNode->dataBlockId;
//  pBlock->info.type = STREAM_INVALID;
//  pBlock->info.calWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
//  pBlock->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSchema *pSchema = taosArrayGet(schemas, i);
    if (pSchema == NULL){
      return NULL;
    }
    SColumnInfoData idata =
        createColumnInfoData(pSchema->type, pSchema->bytes, pSchema->colId);

    code = blockDataAppendColInfo(pBlock, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      blockDataDestroy(pBlock);
      pBlock = NULL;
      terrno = code;
      break;
    }
  }

  return pBlock;
}

static void releaseStreamTask(SStreamReaderTask *pTask) {
  taosHashCleanup(pTask->pIgnoreTables);
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  qStreamDestroyTableList(pTask->pTableList);
  pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
  taosMemoryFree(pTask);
}

static int32_t createStreamTask(SVnode *pVnode, SStreamReaderTaskOptions* options, SStreamReaderTask** ppTask){
  int32_t code = 0;
  SStreamReaderTask *pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTask));
  if (pTask == NULL){
    return terrno;
  }
  initStorageAPI(&pTask->api);
  pTask->pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (NULL == pTask->pIgnoreTables) {
    return terrno;
  }
  pTask->options = *options;
  pTask->pResBlock = createDataBlockForStream(options->schemas);
  pTask->pResBlockDst = createDataBlockForStream(options->schemas);
  code = qStreamCreateTableListForReader(pVnode, options->suid, options->uid, options->tableType, options.groupSort, &pTask->api, &pTask->pTableList);
  if (code != TSDB_CODE_SUCCESS){
    return code;
  }
  int32_t pNum = 1;
  STableKeyInfo* pList = NULL;
  code = qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum);
  if (code != TSDB_CODE_SUCCESS){
    return code;
  }

  SQueryTableDataCond pCond = {0};
  code = initQueryTableDataCond(&pCond, options->order, options->schemas, options->twindows, options->suid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("tsdReader open failed, code:%s", tstrerror(code));
    return code;
  }
  code = pTask->api.tsdReader.tsdReaderOpen(pVnode, &pCond, &pList, pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, &pTask->pIgnoreTables);
  if (code != TSDB_CODE_SUCCESS) {
    qError("tsdReader open failed, code:%s", tstrerror(code));
    return code;
  }
  *ppTask = pTask;
  return 0;
}

SStreamInfoObj* createStreamInfo(const SStreamReaderDeployMsg* pMsg){
  SStreamInfoObj* sStreamInfo = taosMemoryCalloc(1, sizeof(SStreamInfoObj));
  if (sStreamInfo == NULL){
    return NULL;
  }
  return sStreamInfo;
}

int32_t stReaderStreamDeploy(SVnode *pVnode, SRpcMsg *pMsg){
//  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
//  QUERY_CHECK_CODE(code, lino, _error);
//  pInfo->currentGroupId = -1;
    SStreamReaderDeployMsg* deployMsg = NULL;
    if (streamInfoMap == NULL){
      streamInfoMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
      if (NULL == streamInfoMap) {
        return terrno;
      }
    }
    SStreamInfoObj* info = createStreamInfo(deployMsg);
    if (info == NULL){
      return terrno;
    }

    int32_t code = taosHashPut(streamInfoMap, &deployMsg->streamId, sizeof(deployMsg->streamId), &info, POINTER_BYTES);
    if (code != 0){
      vError("failed to put stream info into hash table");
    }
    return code;
}

int32_t stReaderStreamUndeploy(SVnode *pVnode, SRpcMsg *pMsg){
  SStreamReaderDeployMsg* deployMsg = NULL;
  int32_t code = taosHashRemove(streamInfoMap, &deployMsg->streamId, sizeof(deployMsg->streamId));
  if (code != 0){
    vError("failed to remove stream");
  }
  return code;
}
//int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);

static int32_t putTs2Hash(SSHashObj* hash, void* key, int64_t ts) {
  int32_t code = taosHashPut(hash, key, LONG_BYTES, &ts, LONG_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }
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
  int32_t code = qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum);
  if (code != TSDB_CODE_SUCCESS){
    return code;
  }
  code = pTask->api.tsdReader.tsdSetQueryTableList(pTask->pReader, pList, pNum);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  SQueryTableDataCond pCond = {0};
  code = initQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas, pTask->options.twindows, pTask->options.suid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("tsdReader open failed, code:%s", tstrerror(code));
    return code;
  }

  code = pTask->api.tsdReader.tsdReaderResetStatus(pTask->pReader, &pCond);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }
}

static int32_t vnodeProcessStreamLastTsReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamLastTsRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamLastTsRequest* lastTsReq = (SStreamLastTsRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamLastTsRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &lastTsReq->base.streamId, LONG_BYTES);
    if (sStreamInfo == NULL){
      return terrno;
    }
    SArray* schemas = taosArrayInit(4, sizeof(SSchema));
    if (schemas == NULL){
      return terrno;
    }
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))     // gid
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))     // last ts

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .groupSort  = true,
      .order      = TSDB_ORDER_DESC,
      .twindows   = sStreamInfo->twindows,
      .schemas    = schemas
    };
    STREAM_CHECK_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  int64_t ts = INT64_MIN;
  while(true){
    bool hasNext = false;
    code = getTableDataInfo(pTask, &hasNext);
    if (code != 0) {
      goto end;
    }
    if (!hasNext){
      if (ts != INT64_MIN) {
        addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.groupId);
        addColData(pTask->pResBlockDst, 1, &ts);
        pTask->pResBlockDst->info.rows ++;
      }

      pTask->currentGroupIndex++;
      if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
        break;
      }
      code = resetTsdbReader(pTask);
      if (code != 0) {
        goto end;
      }
      if (pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM) {
        break;
      }
      continue;
    }
    if (pTask->pResBlock->info.window.ekey > ts) {
      ts = pTask->pResBlock->info.window.ekey;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);
  }


  vDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

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

static int32_t vnodeProcessStreamFirstTsReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamFirstTsRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamFirstTsRequest* firstTsReq = (SStreamFirstTsRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamFirstTsRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &firstTsReq->base.streamId, LONG_BYTES);
    if (sStreamInfo == NULL){
      return terrno;
    }
    SArray* schemas = taosArrayInit(4, sizeof(SSchema));
    if (schemas == NULL){
      return terrno;
    }
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = sStreamInfo->twindows,
      .schemas    = schemas
    };
    STREAM_CHECK_GOTO(createStreamTask(pVnode, &options, &pTask));
    while(true){
      bool hasNext = false;
      code = getTableDataInfo(pTask, &hasNext);
      if (code != 0 && !hasNext){
        break;
      }

      qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);
      void *p = taosHashGet(pTask->lastTsInfo.groupIdTs, &pTask->pResBlock->info.id.groupId, LONG_BYTES);
      if (p == NULL) {
        // firstTsReq->startTime
        STREAM_CHECK_GOTO(putTs2Hash(pTask->lastTsInfo.groupIdTs, &pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.window.skey));
      } else {
        if ((*(int64_t*)p <= pTask->pResBlock->info.window.ekey && *(int64_t*)p >= pTask->pResBlock->info.window.skey) ||
              *(int64_t*)p <= pTask->pResBlock->info.window.skey){
          STREAM_CHECK_GOTO(putTs2Hash(pTask->lastTsInfo.groupIdTs, &pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.window.skey));
        }
      }
      STREAM_CHECK_GOTO(taosHashPut(pTask->pIgnoreTables, &pTask->pResBlock->info.id.uid, LONG_BYTES, &pTask->pResBlock->info.id.uid, LONG_BYTES));
    }
  }


  int32_t num = 0;
  while (1) {
    pTask->lastTsInfo.pIter = taosHashIterate(pTask->lastTsInfo.groupIdTs, pTask->lastTsInfo.pIter);
    if (pTask->lastTsInfo.pIter == NULL) break;
    int64_t* groupId = taosHashGetKey(pTask->lastTsInfo.groupIdTs, NULL);

    addColData(pTask->pResBlockDst, 0, groupId);
    addColData(pTask->pResBlockDst, 1, pTask->lastTsInfo.pIter);

    if (++num > STREAM_RETURN_ROWS_NUM) {
      // taosHashCancelIterate(pTask->lastTsInfo.groupIdTs, pTask->lastTsInfo.pIter);
      break;
    }
  }

  vDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

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

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamTsdbMetaRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamTsdbMetaRequest* metaReq = (SStreamTsdbMetaRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbMetaRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &metaReq->base.streamId, LONG_BYTES);
    if (sStreamInfo == NULL){
      return terrno;
    }
    SArray* schemas = taosArrayInit(4, sizeof(SSchema));
    if (schemas == NULL){
      return terrno;
    }
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = sStreamInfo->twindows,
      .schemas    = schemas
    };
    STREAM_CHECK_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  pTask->pResBlockDst->info.rows = 0;
  int64_t   ts = metaReq->startTime;
  while(true){
    bool hasNext = false;
    code = getTableDataInfo(pTask, &hasNext);
    if (code != 0 || !hasNext || pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM){
      break;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    if ((ts <= pTask->pResBlock->info.window.ekey && ts >= pTask->pResBlock->info.window.skey) ||
        ts <= pTask->pResBlock->info.window.skey){
      addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid);
      addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.groupId);
      addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.window.skey);
      addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.ekey);
      addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.rows);
        }
    pTask->pResBlockDst->info.rows++;
  }

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

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

static int32_t vnodeProcessStreamTsDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SStreamReaderTask* pTask = NULL;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamTsdbTsDataRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamTsdbTsDataRequest* tsReq = (SStreamTsdbTsDataRequest*)pMsg->pCont;

  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tsReq->base.streamId, LONG_BYTES);
  if (sStreamInfo == NULL){
    return terrno;
  }
  SArray* schemas = taosArrayInit(4, sizeof(SSchema));
  if (schemas == NULL){
    return terrno;
  }
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))

  SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = tsReq->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tsReq->skey, .ekey = tsReq->ekey},
      .schemas    = schemas
  };
  code = createStreamTask(pVnode, &options, &pTask);
  if (code != 0){
    return code;
  }
  pTask->pResBlockDst->info.rows = 0;
  while(true){
    bool hasNext = false;
    code = getTableDataInfo(pTask, &hasNext);
    if (code != 0 || !hasNext || pTask->pResBlockDst->info.rows > STREAM_RETURN_ROWS_NUM){
      break;
    }
    qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
  }

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = 0};
  tmsgSendRsp(&rsp);

end:
  releaseStreamTask(pTask);
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vDebug("vgId:%d %s done success", TD_VID(pVnode), __func__);
  }
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamTsdbTriggerDataRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamTsdbTriggerDataRequest* tDataReq = (SStreamTsdbTriggerDataRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbTriggerDataRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tDataReq->base.streamId, LONG_BYTES);
    if (sStreamInfo == NULL){
      return terrno;
    }
    SArray* schemas = taosArrayInit(4, sizeof(SSchema));
    if (schemas == NULL){
      return terrno;
    }
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tDataReq->startTime, .ekey = INT64_MAX},
      .schemas    = schemas
    };
    STREAM_CHECK_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  bool hasNext = false;
  code = getTableDataInfo(pTask, &hasNext);
  if (code != 0 || !hasNext){
    goto end;
  }
  qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_GOTO(getTableData(pTask, &pBlock));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlock->info.rows);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlock, &buf, &size));

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
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamTsdbCalcDataRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamTsdbCalcDataRequest* tCalcDataReq = (SStreamTsdbCalcDataRequest*)pMsg->pCont;
  SStreamReaderTask* pTask = taosHashGet(streamTaskMap, pMsg->pCont, sizeof(SStreamTsdbCalcDataRequest));
  if (pTask == NULL) {
    SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tCalcDataReq->base.streamId, LONG_BYTES);
    if (sStreamInfo == NULL){
      return terrno;
    }
    SArray* schemas = taosArrayInit(4, sizeof(SSchema));
    if (schemas == NULL){
      return terrno;
    }
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

    SStreamReaderTask* pTask = NULL;
    SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = STimeWindow{.skey = tCalcDataReq->skey, .ekey = tCalcDataReq->ekey},
      .schemas    = schemas
    };
    STREAM_CHECK_GOTO(createStreamTask(pVnode, &options, &pTask));
  }

  bool hasNext = false;
  code = getTableDataInfo(pTask, &hasNext);
  if (code != 0 || !hasNext){
    goto end;
  }
  qStreamSetGroupId(pTask->pTableList, pTask->pResBlock);

  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_GOTO(getTableData(pTask, &pBlock));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlock->info.rows);
  void*  buf  = NULL;
  size_t size = 0;
  STREAM_CHECK_GOTO(buildRsp(pTask->pResBlock, &buf, &size));

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

int32_t retrieveWalData(SSubmitTbData* pSubmitTbData, SSDataBlock* pBlock) {
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

int32_t retrieveWalTsData(SSubmitTbData* pSubmitTbData, SSDataBlock* pBlock) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t        code = 0;
  int32_t        line = 0;

  int32_t ver = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  int32_t numOfRows = 0;
  // code = blockDataEnsureCapacity(pBlock, numOfRows);
  // TSDB_CHECK_CODE(code, line, END);
  SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, 0);

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    TSDB_CHECK_NULL(pCol, code, line, END, terrno);
    numOfRows = pCol->nVal;
    for (int32_t i = 0; i < pCol->nVal; i++) {
      SColVal colVal = {0};
      code = tColDataGetValue(pCol, i, &colVal);
      TSDB_CHECK_CODE(code, line, END);
      code = colDataSetVal(pColData, i, VALUE_GET_TRIVIAL_DATUM(&colVal.value), !COL_VAL_IS_VALUE(&colVal));
      TSDB_CHECK_CODE(code, line, END);
    }
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
    for (int32_t i = 0; i < numOfRows; i++) {
      SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, i);
      SColVal colVal = {0};
      STColumn column = {.colId = 1, .type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = LONG_BYTES};
      STSchema pTSchema = {.numOfCols = 1, .version = ver, .columns = &column};
      code = tRowGet(pRow, &pTSchema, 0, &colVal);
      code = colDataSetVal(pColData, i, VALUE_GET_TRIVIAL_DATUM(&colVal.value), !COL_VAL_IS_VALUE(&colVal));
      TSDB_CHECK_CODE(code, line, END);
    }
  }

  pBlock->info.rows += numOfRows;

  END:
    if (code != 0) {
      vError("tqRetrieveDataBlock failed, line:%d, msg:%s", line, tstrerror(code));
    }
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

void scanWalOneVer(void* pVnode, SSDataBlock* pBlock, int64_t ver) {
  SWalReader* pWalReader = walOpenReader(pVnode, NULL, 0);
  walReaderSeekVer(pWalReader, ver);

  // try next message in wal file
  if (walNextValidMsg(pWalReader) < 0) {
    return;
  }
  void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
  int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
  int64_t ver = pWalReader->pHead->head.version;

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
    retrieveWalTsData(pSubmitTbData, pBlock);
    nextBlk += 1;
  }
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
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

static int32_t vnodeProcessStreamWalTsDataReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamWalTsDataRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamWalTsDataRequest* tTsDataReq = (SStreamWalTsDataRequest*)pMsg->pCont;
  SArray* schemas = taosArrayInit(4, sizeof(SSchema));
  if (schemas == NULL){
    return terrno;
  }
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // gid
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // uid
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))   // ts

  SSDataBlock* pBlock = createDataBlockForStream(schemas);

  scanWalOneVer(pVnode, pBlock, tTsDataReq->ver);

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