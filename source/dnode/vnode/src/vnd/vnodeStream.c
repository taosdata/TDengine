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

#define STREAM_RETURN_ROWS_NUM 4

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
  int32_t           pNum;
  STableKeyInfo*    pList;
}SStreamReaderTaskOptions;

typedef struct SStreamReaderTask {
  int64_t             streamId;
  int64_t             sessionId;
  struct SStorageAPI  api;
  STsdbReader*        pReader;
  SHashObj*           pIgnoreTables;
  SSDataBlock*        pResBlock;
  SSDataBlock*        pResBlockDst;
  void*               pTableList;
  int32_t             currentGroupIndex;
  char*               idStr;
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


static int32_t createStreamTask(SVnode *pVnode, SStreamReaderTaskOptions* options, SStreamReaderTask** ppTask){
  int32_t code = 0;
  SStreamReaderTask *pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTask));
  if (pTask == NULL){
    return terrno;
  }
  initStorageAPI(&pTask->api);
  pTask->pIgnoreTables =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (NULL == pTask->pIgnoreTables) {
    return terrno;
  }
  pTask->pResBlock = createDataBlockForStream(options->schemas);
  pTask->pResBlockDst = createDataBlockForStream(options->schemas);
  code = qStreamCreateTableListForReader(pVnode, options->suid, options.uid, options->tableType, &pTask->api, &pTask->pTableList);
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

static int32_t vnodeProcessStreamLastTsReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamLastTsRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamLastTsRequest* lastTsReq = (SStreamLastTsRequest*)pMsg->pCont;

  int64_t   ts = INT64_MIN;
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &lastTsReq->base.streamId, LONG_BYTES);
  if (sStreamInfo == NULL){
    return terrno;
  }
  SStreamReaderTask* pTask = NULL;
  SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_DESC,
      .twindows   = sStreamInfo->twindows,
      .schemas    = sStreamInfo->schemas
  };
  code = createStreamTask(pVnode, &options, &pTask);
  if (code != 0){
    return code;
  }
  while(true){
    bool hasNext = false;
    code = getTableDataInfo(pTask, &hasNext);
    if (code != 0 && !hasNext){
      break;
    }

    if (pTask->pResBlock->info.window.ekey > ts){
     ts = pTask->pResBlock->info.window.ekey;

     code = taosHashPut(pTask->pIgnoreTables, &pTask->pResBlock->info.id.uid, LONG_BYTES, &pTask->pResBlock->info.id.uid, LONG_BYTES));
     if (code != TSDB_CODE_SUCCESS) {
       qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
       return code;
     }
    }
  }

  SStreamLastTsResponse lastTsRsp = {.lastTs = ts, .vgId = TD_VID(pVnode), .ver = pMsg->info.conn.applyIndex};
  vDebug("vgId:%d %s get result lastTs:%" PRId64 ", ver:%" PRId64, TD_VID(pVnode), __func__, lastTsRsp.lastTs, lastTsRsp.ver);
  int32_t tlen = sizeof(SStreamLastTsResponse);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    code = terrno;
    goto end;
  }
  memcpy(buf, &lastTsRsp, tlen);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};
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

  int64_t   ts = firstTsReq->startTime;
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &firstTsReq->base.streamId, LONG_BYTES);
  if (sStreamInfo == NULL){
    return terrno;
  }
  SStreamReaderTask* pTask = NULL;
  SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = sStreamInfo->twindows,
      .schemas    = sStreamInfo->schemas
  };
  code = createStreamTask(pVnode, &options, &pTask);
  if (code != 0){
    return code;
  }
  while(true){
    bool hasNext = false;
    code = getTableDataInfo(pTask, &hasNext);
    if (code != 0 && !hasNext){
      break;
    }

    if ((ts <= pTask->pResBlock->info.window.ekey && ts >= pTask->pResBlock->info.window.skey) ||
        ts <= pTask->pResBlock->info.window.skey){
      ts = pTask->pResBlock->info.window.skey;

      code = taosHashPut(pTask->pIgnoreTables, &pTask->pResBlock->info.id.uid, LONG_BYTES, &pTask->pResBlock->info.id.uid, LONG_BYTES));
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return code;
      }
    }
  }

  SStreamFirstTsResponse firstTsRsp = {.firstTs = ts, .vgId = TD_VID(pVnode), .ver = pMsg->info.conn.applyIndex};
  vDebug("vgId:%d %s get result lastTs:%" PRId64 ", ver:%" PRId64, TD_VID(pVnode), __func__, firstTsRsp.firstTs, firstTsRsp.ver);
  int32_t tlen = sizeof(SStreamFirstTsResponse);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    code = terrno;
    goto end;
  }
  memcpy(buf, &firstTsRsp, tlen);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};
  tmsgSendRsp(&rsp);

end:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vDebug("vgId:%d %s done success", TD_VID(pVnode), __func__);
  }
  return code;
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
static int32_t vnodeProcessStreamTsdbMetaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  if (pMsg->contLen != sizeof(SStreamTsdbMetaRequest)){
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }
  SStreamTsdbMetaRequest* metaReq = (SStreamTsdbMetaRequest*)pMsg->pCont;

  int64_t   ts = metaReq->startTime;
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &metaReq->base.streamId, LONG_BYTES);
  if (sStreamInfo == NULL){
    return terrno;
  }
  SStreamReaderTask* pTask = NULL;
  SArray* schemas = taosArrayInit(4, sizeof(SSchema));
  if (schemas == NULL){
    return terrno;
  }
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))
  STREAM_CHECK_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))

  SStreamReaderTaskOptions options = {
      .suid       = sStreamInfo->suid,
      .uid        = sStreamInfo->uid,
      .tableType  = sStreamInfo->tableType,
      .order      = TSDB_ORDER_ASC,
      .twindows   = sStreamInfo->twindows,
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
    if ((ts <= pTask->pResBlock->info.window.ekey && ts >= pTask->pResBlock->info.window.skey) ||
        ts <= pTask->pResBlock->info.window.skey){
      addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid);
      addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.window.skey);
      addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.ekey);
      addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.rows);
    }
    pTask->pResBlockDst->info.rows++;
  }


  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  size_t  dataEncodeSize = blockGetEncodeSize(pTask->pResBlockDst);
  void*   buf = rpcMallocCont(dataEncodeSize);
  if (buf == NULL) {
    code = terrno;
    goto end;
  }
  int32_t actualLen = blockEncode(pTask->pResBlockDst, buf, dataEncodeSize, taosArrayGetSize(pTask->pResBlockDst->pDataBlock));
  if (actualLen < 0) {
    taosMemoryFree(buf);
    return terrno;
  }
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = dataEncodeSize, .code = 0};
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