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
#include "streamReader.h"


static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamTsDataReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamCalcDataReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamWalMetaReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamWalTsDataReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode* pVnode, SRpcMsg* pRsp);
static int32_t vnodeProcessStreamWalCalcDataReq(SVnode* pVnode, SRpcMsg* pRsp);



static int32_t insertTableToIgnoreList(SHashObj** pIgnoreTables, uint64_t uid) {
  if (NULL == *pIgnoreTables) {
    *pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
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

static int32_t buildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId) {
  SSchema* pSchema = taosArrayReserve(schemas, 1);
  if (pSchema == NULL) {
    return terrno;
  }
  pSchema->type = type;
  pSchema->bytes = bytes;
  pSchema->colId = colId;
  return 0;
}

static int32_t addColData(SSDataBlock* pResBlock, int32_t colId, void* data) {
  SColumnInfoData* pSrc = taosArrayGet(pResBlock->pDataBlock, colId);
  if (pSrc == NULL) {
    return terrno;
  }

  memcpy(pSrc->pData + pResBlock->info.rows * pSrc->info.bytes, data, pSrc->info.bytes);
  return 0;
}

static int32_t getTableDataInfo(SStreamReaderTaskInner* pTask, bool* hasNext) {
  int32_t code = pTask->api.tsdReader.tsdNextDataBlock(pTask->pReader, hasNext);
  if (code != TSDB_CODE_SUCCESS) {
    pTask->api.tsdReader.tsdReaderReleaseDataBlock(pTask->pReader);
  }

  return code;
}

static int32_t getTableData(SStreamReaderTaskInner* pTask, SSDataBlock** ppRes) {
  return pTask->api.tsdReader.tsdReaderRetrieveDataBlock(pTask->pReader, ppRes, NULL);
}

static int32_t buildTsRsp(const SStreamTsResponse* tsRsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t len = 0;
  void* buf =  NULL;
  tEncodeSize(tEncodeSStreamTsResponse, tsRsp, len, code);
  STREAM_CHECK_CONDITION_GOTO(code < 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  code = tEncodeSStreamTsResponse(&encoder, tsRsp);
  tEncoderClear(&encoder);
  STREAM_CHECK_CONDITION_GOTO(code < 0, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildRsp(SSDataBlock* pBlock, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  size_t dataEncodeSize = blockGetEncodeSize(pBlock);
  void*  buf = rpcMallocCont(dataEncodeSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actualLen = blockEncode(pBlock, buf, dataEncodeSize, taosArrayGetSize(pBlock->pDataBlock));
  STREAM_CHECK_CONDITION_GOTO(actualLen < 0, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = dataEncodeSize;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t resetTsdbReader(SStreamReaderTaskInner* pTask) {
  int32_t        pNum = 1;
  STableKeyInfo* pList = NULL;
  int32_t        code = 0;
  int32_t        lino = 0;
  STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdSetQueryTableList(pTask->pReader, pList, pNum));

  SQueryTableDataCond pCond = {0};
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas,
                                               pTask->options.twindows, pTask->options.suid));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderResetStatus(pTask->pReader, &pCond));

end:
  PRINT_LOG_END(code, lino);
  return code;
}

#define BUILD_OPTION(options,sStreamInfo,_groupSort,_order,startTime,endTime,_schemas,_scanMode,_gid) \
SStreamReaderTaskInnerOptions options = {.suid = (*sStreamInfo)->suid,\
  .uid = (*sStreamInfo)->uid,\
  .tableType = (*sStreamInfo)->tableType,\
  .groupSort = _groupSort,\
  .order = _order,\
  .twindows = {.skey = startTime, .ekey = endTime},\
  .pTagCond = (*sStreamInfo)->pTagCond,\
  .pTagIndexCond = (*sStreamInfo)->pTagIndexCond,\
  .pConditions = (*sStreamInfo)->pConditions,\
  .pGroupTags = (*sStreamInfo)->pGroupTags,\
  .schemas = _schemas,\
  .scanMode = _scanMode,\
  .gid = _gid}

static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       lastTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  // STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerLastTsRequest), TSDB_CODE_INVALID_MSG);

  // SSTriggerLastTsRequest* lastTsReq = (SSTriggerLastTsRequest*)pMsg->pCont;
  SSTriggerLastTsRequest lastTsReq = {.base.streamId = 1};
  SStreamInfoObj**       sStreamInfo = taosHashGet(streamInfoMap, &lastTsReq.base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
  STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // last ts

  BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_DESC,INT64_MIN,INT64_MAX,schemas,STREAM_SCAN_GROUP_ONE_BY_ONE,0);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));

  lastTsRsp.tsInfo = taosArrayInit(qStreamGetTableListGroupNum(pTask->pTableList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(lastTsRsp.tsInfo, terrno);

  lastTsRsp.ver = pMsg->info.conn.applyTerm;
  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      break;
    }
    STsInfo* tsInfo = taosArrayReserve(lastTsRsp.tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    tsInfo->ts = pTask->pResBlock->info.window.ekey;
    tsInfo->gId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);
    vDebug("vgId:%d %s get last ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, TD_VID(pVnode), __func__, tsInfo->ts, tsInfo->gId, lastTsRsp.ver);

    pTask->currentGroupIndex++;
    if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
      break;
    }
    STREAM_CHECK_RET_GOTO(resetTsdbReader(pTask));
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

static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       firstTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  // STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SStreamFirstTsRequest), TSDB_CODE_INVALID_MSG);

  // SStreamFirstTsRequest* firstTsReq = (SStreamFirstTsRequest*)pMsg->pCont;
  SSTriggerFirstTsRequest tmp = {.base.streamId = 1, .startTime = 1500000062017};
  SSTriggerFirstTsRequest* firstTsReq = &tmp;
  SStreamInfoObj**        sStreamInfo = taosHashGet(streamInfoMap, &firstTsReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
  STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts

  BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_ASC,firstTsReq->startTime,INT64_MAX,schemas,STREAM_SCAN_GROUP_ONE_BY_ONE,0);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));

  firstTsRsp.tsInfo = taosArrayInit(qStreamGetTableListGroupNum(pTask->pTableList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(firstTsRsp.tsInfo, terrno);

  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      break;
    }
    STsInfo* tsInfo = taosArrayReserve(firstTsRsp.tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    tsInfo->ts = pTask->pResBlock->info.window.skey;
    tsInfo->gId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);
    vDebug("vgId:%d %s get first ts:%" PRId64 ", gId:%" PRIu64, TD_VID(pVnode), __func__, tsInfo->ts, tsInfo->gId);

    pTask->currentGroupIndex++;
    if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
      break;
    }
    STREAM_CHECK_RET_GOTO(resetTsdbReader(pTask));
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

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerTsdbMetaRequest), TSDB_CODE_INVALID_MSG);
  SSTriggerTsdbMetaRequest* metaReq = (SSTriggerTsdbMetaRequest*)pMsg->pCont;
  SStreamReaderTaskInner* pTask = qStreamGetStreamInnerTask(metaReq->base.streamId, metaReq->base.sessionId, metaReq, sizeof(SSTriggerTsdbMetaRequest));
  if (pTask == NULL) {
    SStreamInfoObj** sStreamInfo = taosHashGet(streamInfoMap, &metaReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
    STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // ts
    BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_ASC,metaReq->startTime,INT64_MAX,schemas,STREAM_SCAN_ALL,0);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
    STREAM_CHECK_RET_GOTO(qStreamPutStreamInnerTask(metaReq->base.streamId, metaReq->base.sessionId, metaReq, sizeof(SSTriggerTsdbMetaRequest), &pTask, POINTER_BYTES));

    taosArrayClear(schemas);
    int16_t colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, colId++))  // skey
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, colId++))  // ekey
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, colId++))  // uid
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, colId++))  // gid
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, colId++))  // nrows
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(options.schemas, &pTask->pResBlockDst));
  }

  pTask->pResBlockDst->info.rows = 0;
  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      qStreamRemoveStreamInnerTask(metaReq->base.streamId, metaReq->base.sessionId, metaReq, sizeof(SSTriggerTsdbMetaRequest));
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.id.uid));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.id.groupId));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 4, &pTask->pResBlock->info.rows));

    vDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%"PRId64,
       TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
       pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
    pTask->pResBlockDst->info.rows++;
    if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
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

static int32_t vnodeProcessStreamTsDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamReaderTask task = {.task.streamId= 1};
  SStreamReaderDeployMsg tmp= {.msg.trigger = {.triggerTblType = TD_SUPER_TABLE, .triggerTblUid = 2201650780113908789, }};
  stReaderTaskDeploy(&task, &tmp);

  // STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerTsdbTsDataRequest), TSDB_CODE_INVALID_MSG);
  // SSTriggerTsdbTsDataRequest* tsReq = (SSTriggerTsdbTsDataRequest*)pMsg->pCont;
  SSTriggerTsdbTsDataRequest t = {.base.streamId = 1, .uid = 6676630383110389775, .skey = 1500000062017, .ekey = 1500000072018};
  SSTriggerTsdbTsDataRequest* tsReq = &t;
  
  SStreamInfoObj** sStreamInfo = taosHashGet(streamInfoMap, &tsReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
  STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // ts
  // STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

  BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_ASC,tsReq->skey,tsReq->ekey,schemas,STREAM_SCAN_ALL,0);
  options.uid = tsReq->uid;
  options.tableType = TD_CHILD_TABLE;
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  taosArrayClear(schemas);
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, 0))  // ts
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pTask->pResBlockDst));
  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    vDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%"PRId64,
      TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
      pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
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

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);


  // STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerTsdbTriggerDataRequest), TSDB_CODE_INVALID_MSG);

  SStreamReaderTask task = {.task.streamId= 1};
  SStreamReaderDeployMsg tmp= {.msg.trigger = {.triggerTblType = TD_SUPER_TABLE, .triggerTblUid = 1906690787880907897, }};
  stReaderTaskDeploy(&task, &tmp);

  SSTriggerTsdbTriggerDataRequest t = {.base.streamId = 1, .startTime = 1500000062017};
  SSTriggerTsdbTriggerDataRequest* tDataReq = &t;

  // SSTriggerTsdbTriggerDataRequest* tDataReq = (SSTriggerTsdbTriggerDataRequest*)pMsg->pCont;
  SStreamReaderTaskInner* pTask = qStreamGetStreamInnerTask(tDataReq->base.streamId, tDataReq->base.sessionId, tDataReq, sizeof(SSTriggerTsdbTriggerDataRequest));

  if (pTask == NULL) {
    SStreamInfoObj** sStreamInfo = taosHashGet(streamInfoMap, &tDataReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
    STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))    // ts
    // STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2)) // trigger col
    // STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3)) // uid

    BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_ASC,tDataReq->startTime,INT64_MAX,schemas,STREAM_SCAN_ALL,0);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));

    STREAM_CHECK_RET_GOTO(qStreamPutStreamInnerTask(tDataReq->base.streamId, tDataReq->base.sessionId, tDataReq, sizeof(SSTriggerTsdbTriggerDataRequest), &pTask, POINTER_BYTES));
    taosArrayClear(schemas);
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, 0))  // ts
    // STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))  // ts
    // STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))  // ts
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pTask->pResBlockDst));
  }

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      goto end;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    vDebug("vgId:%d %s get skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%"PRId64,
      TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
      pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
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

static int32_t vnodeProcessStreamCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerTsdbCalcDataRequest), TSDB_CODE_INVALID_MSG);

  SSTriggerTsdbCalcDataRequest* tCalcDataReq = (SSTriggerTsdbCalcDataRequest*)pMsg->pCont;
  SStreamReaderTaskInner* pTask = qStreamGetStreamInnerTask(tCalcDataReq->base.streamId, tCalcDataReq->base.sessionId, tCalcDataReq, sizeof(SSTriggerTsdbCalcDataRequest));
  if (pTask == NULL) {
    SStreamInfoObj** sStreamInfo = taosHashGet(streamInfoMap, &tCalcDataReq->base.streamId, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);
    STREAM_CHECK_NULL_GOTO(*sStreamInfo, terrno);

    schemas = taosArrayInit(4, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);

    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))
    STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))

    SStreamReaderTaskInner*       pTask = NULL;
    BUILD_OPTION(options,sStreamInfo,true,TSDB_ORDER_ASC,tCalcDataReq->skey,tCalcDataReq->ekey,schemas,STREAM_SCAN_ALL,tCalcDataReq->gid);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
    STREAM_CHECK_RET_GOTO(qStreamPutStreamInnerTask(tCalcDataReq->base.streamId, tCalcDataReq->base.sessionId, tCalcDataReq, sizeof(SSTriggerTsdbCalcDataRequest), &pTask, POINTER_BYTES));
  }
  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      goto end;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

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

static int32_t buildWalMetaBlock(SSDataBlock* pBlock, int8_t type, int64_t gid, int64_t uid, int64_t skey, int64_t ekey,
                                 int64_t ver, int64_t rows) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 0, &type));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 1, &gid));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 2, &uid));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 3, &skey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 4, &ekey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 5, &ver));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, 6, &rows));

end:
  PRINT_LOG_END(code, lino)
  return code;
}

static void buildTSchema(STSchema* pTSchema, int32_t ver, col_id_t colId, int8_t type, int32_t bytes) {
  pTSchema->numOfCols = 1;
  pTSchema->version = ver;
  pTSchema->columns[0].colId = colId;
  pTSchema->columns[0].type = type;
  pTSchema->columns[0].bytes = bytes;
}

int32_t retrieveWalMetaData(SSubmitTbData* pSubmitTbData, void* pTableList, SSDataBlock* pBlock) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t code = 0;
  int32_t lino = 0;

  int32_t   ver = pSubmitTbData->sver;
  int64_t   uid = pSubmitTbData->uid;
  int32_t   numOfRows = 0;
  int64_t   skey = 0;
  int64_t   ekey = 0;
  STSchema* pTSchema = NULL;
  STREAM_CHECK_CONDITION_GOTO(!qStreamUidInTableList(pTableList, uid), TDB_CODE_SUCCESS);
  uint64_t gid = qStreamGetGroupId(pTableList, uid);

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    STREAM_CHECK_NULL_GOTO(pCol, terrno);
    numOfRows = pCol->nVal;

    SColVal colVal = {0};
    STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, 0, &colVal));
    skey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);

    STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, numOfRows - 1, &colVal));
    ekey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
    SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, 0);
    STREAM_CHECK_NULL_GOTO(pRow, terrno);
    SColVal colVal = {0};
    pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn));
    STREAM_CHECK_NULL_GOTO(pTSchema, terrno);
    buildTSchema(pTSchema, ver, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES);
    STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchema, 0, &colVal));
    skey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
    pRow = taosArrayGetP(pSubmitTbData->aRowP, numOfRows - 1);
    STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchema, 0, &colVal));
    ekey = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
  }

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, numOfRows));
  STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_SUBMIT_DATA, gid, uid, skey, ekey, ver, numOfRows));
  pBlock->info.rows++;

end:
  taosMemoryFree(pTSchema);
  PRINT_LOG_END(code, lino)
  return code;
}

int32_t retrieveWalData(SSubmitTbData* pSubmitTbData, void* pTableList, SSDataBlock* pBlock, STimeWindow* window) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t code = 0;
  int32_t lino = 0;

  int32_t   ver = pSubmitTbData->sver;
  int64_t   uid = pSubmitTbData->uid;
  int32_t   numOfRows = 0;
  STSchema* pTSchemaTs = NULL;
  STSchema* pTSchema = NULL;
  // code = blockDataEnsureCapacity(pBlock, numOfRows);
  // TSDB_CHECK_CODE(code, line, END);

  STREAM_CHECK_CONDITION_GOTO(!qStreamUidInTableList(pTableList, uid), TDB_CODE_SUCCESS);
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    STREAM_CHECK_NULL_GOTO(pCol, terrno);
    int32_t rowStart = 0;
    int32_t rowEnd = pCol->nVal;
    if (window != NULL) {
      SColVal colVal = {0};
      rowStart = -1;
      rowEnd = -1;
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
      STREAM_CHECK_CONDITION_GOTO(rowStart == -1 || rowStart == rowEnd, TDB_CODE_SUCCESS);

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
        if (pCol->cid == pColData->info.colId) {
          for (int32_t k = rowStart; k < rowEnd; k++) {
            SColVal colVal = {0};
            STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, k, &colVal));
            STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, k, VALUE_GET_DATUM(&colVal.value, colVal.value.type),
                                                !COL_VAL_IS_VALUE(&colVal)));
          }
        }
      }
    }
    numOfRows = rowEnd - rowStart;
  } else {
    pTSchemaTs = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn));
    STREAM_CHECK_NULL_GOTO(pTSchemaTs, terrno);
    buildTSchema(pTSchemaTs, ver, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES);
    for (int32_t j = 0; j < taosArrayGetSize(pSubmitTbData->aRowP); j++) {
      SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, j);
      STREAM_CHECK_NULL_GOTO(pRow, terrno);
      SColVal colVal = {0};
      STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchemaTs, 0, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (ts < window->skey || ts > window->ekey) {
        continue;
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
        STREAM_CHECK_NULL_GOTO(pColData, terrno);
        SColVal colVal = {0};
        pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn));
        STREAM_CHECK_NULL_GOTO(pTSchema, terrno);
        buildTSchema(pTSchema, ver, pColData->info.colId, pColData->info.type, pColData->info.bytes);
        STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchema, pColData->info.colId, &colVal));
        taosMemoryFreeClear(pTSchema);
        STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, numOfRows++, VALUE_GET_DATUM(&colVal.value, colVal.value.type),
                                            !COL_VAL_IS_VALUE(&colVal)));
      }
    }
  }

  pBlock->info.rows = numOfRows;

end:
  taosMemoryFree(pTSchemaTs);
  taosMemoryFree(pTSchema);
  PRINT_LOG_END(code, lino)
  return code;
}

int32_t scanWal(void* pVnode, void* pTableList, SSDataBlock* pBlock, int64_t ver) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSubmitReq2 submit = {0};
  SDecoder    decoder = {0};

  SWalReader* pWalReader = walOpenReader(pVnode, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  STREAM_CHECK_RET_GOTO(walReaderSeekVer(pWalReader, ver));

  while (1) {
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader) < 0, TSDB_CODE_SUCCESS);

    void*   data = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SMsgHead));
    int32_t len = pWalReader->pHead->head.bodyLen - sizeof(SMsgHead);
    int64_t ver = pWalReader->pHead->head.version;

    if (pWalReader->pHead->head.msgType == TDMT_VND_DELETE) {
      SDeleteRes req = {0};
      tDecoderInit(&decoder, data, len);
      STREAM_CHECK_RET_GOTO(tDecodeDeleteRes(&decoder, &req));
      tDecoderClear(&decoder);
      uint64_t gid = qStreamGetGroupId(pTableList, req.suid);
      if (gid == -1) continue;
      STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_DELETE_DATA, gid, req.suid, req.skey, req.ekey, ver, 1));

    } else if (pWalReader->pHead->head.msgType == TDMT_VND_DROP_TABLE) {
      SVDropTbBatchReq req = {0};
      tDecoderInit(&decoder, data, len);
      STREAM_CHECK_RET_GOTO(tDecodeSVDropTbBatchReq(&decoder, &req));
      tDecoderClear(&decoder);
      for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
        SVDropTbReq* pDropTbReq = req.pReqs + iReq;
        STREAM_CHECK_NULL_GOTO(pDropTbReq, TSDB_CODE_INVALID_PARA);
        uint64_t gid = qStreamGetGroupId(pTableList, pDropTbReq->uid);
        if (gid == -1) continue;
        STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_DELETE_TABLE, gid, pDropTbReq->uid, 0, 0, ver, 1));
      }
    } else if (pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      continue;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int32_t nextBlk = 0;
    tDecoderInit(&decoder, pBody, bodyLen);
    STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));
    tDecoderClear(&decoder);

    int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);
    while (nextBlk < numOfBlocks) {
      vDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
      SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
      STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
      STREAM_CHECK_RET_GOTO(retrieveWalMetaData(pSubmitTbData, pTableList, pBlock));
      nextBlk += 1;
    }
    tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
    submit.aSubmitTbData = NULL;
  }

end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
  tDecoderClear(&decoder);
  PRINT_LOG_END(code, lino);
  return code;
}

int32_t scanWalOneVer(void* pVnode, void* pTableList, SSDataBlock* pBlock, SSDataBlock* pBlockRet, int64_t ver,
                      STimeWindow* window) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSubmitReq2 submit = {0};
  SDecoder    decoder = {0};

  SWalReader* pWalReader = walOpenReader(pVnode, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  STREAM_CHECK_RET_GOTO(walReaderSeekVer(pWalReader, ver));

  while (1) {
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader) < 0, TSDB_CODE_SUCCESS);
    if (pWalReader->pHead->head.msgType == TDMT_VND_SUBMIT) {
      break;
    }
  }
  void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
  int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);

  int32_t nextBlk = 0;
  tDecoderInit(&decoder, pBody, bodyLen);
  STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));

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

end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
  tDecoderClear(&decoder);
  PRINT_LOG_END(code, lino);
  return code;
}

static int32_t vnodeProcessStreamWalMetaReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  SStorageAPI  api = {0};
  void*        pTableList = NULL;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerWalMetaRequest), TSDB_CODE_INVALID_MSG);

  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, CHAR_BYTES, 1))  // type
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))  // gid
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))  // uid
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))  // skey
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 5))  // ekey
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 6))  // ver
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 7))  // nrows

  SSTriggerWalMetaRequest* tWalMetaReq = (SSTriggerWalMetaRequest*)pMsg->pCont;

  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tWalMetaReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, sStreamInfo->suid, sStreamInfo->uid,
                                                        sStreamInfo->tableType, sStreamInfo->pGroupTags, false, sStreamInfo->pTagCond,
                                                        sStreamInfo->pTagIndexCond, &api, &pTableList));

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock));
  STREAM_CHECK_RET_GOTO(scanWal(pVnode, pTableList, pBlock, tWalMetaReq->lastVer));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  qStreamDestroyTableList(pTableList);

  return code;
}

static int32_t processWalVerData(SVnode* pVnode, SStreamInfoObj* sStreamInfo, SArray* schemas, int64_t ver,
                                 STimeWindow* window, SSDataBlock** pBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SFilterInfo* pFilterInfo = NULL;
  void*        pTableList = NULL;
  SStorageAPI  api = {0};
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;

  if (sStreamInfo->pConditions != NULL) {
    STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0));
  }

  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, sStreamInfo->suid, sStreamInfo->uid,
                                                        sStreamInfo->tableType, sStreamInfo->pGroupTags, false, sStreamInfo->pTagCond,
                                                        sStreamInfo->pTagIndexCond, &api, &pTableList));

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
  return code;
}

static int32_t vnodeProcessStreamWalTsDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerWalTsDataRequest), TSDB_CODE_INVALID_MSG);
  SSTriggerWalTsDataRequest* tTsDataReq = (SSTriggerWalTsDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tTsDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))  // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))  // filter col

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamInfo, schemas, tTsDataReq->ver, NULL, &pBlock));

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

static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerWalTriggerDataRequest), TSDB_CODE_INVALID_MSG);
  SSTriggerWalTriggerDataRequest* tTsDataReq = (SSTriggerWalTriggerDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tTsDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))  // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))  // trigger col
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))  // filter col

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamInfo, schemas, tTsDataReq->ver, NULL, &pBlock));

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

static int32_t vnodeProcessStreamWalCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_CONDITION_GOTO(pMsg->contLen != sizeof(SSTriggerWalCalcDataRequest), TSDB_CODE_INVALID_MSG);
  SSTriggerWalCalcDataRequest* tCalcDataReq = (SSTriggerWalCalcDataRequest*)pMsg->pCont;
  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  SStreamInfoObj* sStreamInfo = taosHashGet(streamInfoMap, &tCalcDataReq->base.streamId, LONG_BYTES);
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))  // ts
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))  // trigger col
  STREAM_CHECK_RET_GOTO(buildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))  // filter col

  STimeWindow window = {.ekey = tCalcDataReq->ekey, .skey = tCalcDataReq->skey};
  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamInfo, schemas, tCalcDataReq->ver, &window, &pBlock));

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

static int32_t vnodeProcessStreamFetchMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SStreamReaderTask* pTask = NULL;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0, TSDB_CODE_QRY_INVALID_INPUT);
  // STREAM_CHECK_RET_GOTO(streamGetTask(req.queryId, req.taskId, &pTask));
  // STREAM_CHECK_RET_GOTO(((SExecTaskInfo*)(pTask->pExecutor))->pRoot->fpSet.getNextFn(((SExecTaskInfo*)(pTask->pExecutor))->pRoot->pRoot, pRes));
  if (pBlock && pBlock->info.rows > 0) {
    STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));
  }
end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  vTrace("vgId:%d, msg:%p in stream reader queue is processing", pVnode->config.vgId, pMsg);
  // if (!syncIsReadyForRead(pVnode->sync)) {
    // vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    // return 0;
  // }

  if (pMsg->msgType == TDMT_STREAM_FETCH) {
    return vnodeProcessStreamFetchMsg(pVnode, pMsg);
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_PULL) {
    ESTriggerPullType* type = (ESTriggerPullType*)(pMsg->pCont);
    *type = STRIGGER_PULL_TSDB_META;
    // stReaderStreamDeploy(pVnode, pMsg);
    switch (*type) {
      case STRIGGER_PULL_LAST_TS:
        return vnodeProcessStreamLastTsReq(pVnode, pMsg);
      case STRIGGER_PULL_FIRST_TS:
        return vnodeProcessStreamFirstTsReq(pVnode, pMsg);
      case STRIGGER_PULL_TSDB_META:
        return vnodeProcessStreamTsdbMetaReq(pVnode, pMsg);
      case STRIGGER_PULL_TSDB_TS_DATA:
        return vnodeProcessStreamTsDataReq(pVnode, pMsg);
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
        return vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg);
      case STRIGGER_PULL_TSDB_CALC_DATA:
        return vnodeProcessStreamCalcDataReq(pVnode, pMsg);
      case STRIGGER_PULL_WAL_META:
        return vnodeProcessStreamWalMetaReq(pVnode, pMsg);
      case STRIGGER_PULL_WAL_TS_DATA:
        return vnodeProcessStreamWalTsDataReq(pVnode, pMsg);
      case STRIGGER_PULL_WAL_TRIGGER_DATA:
        return vnodeProcessStreamWalTriggerDataReq(pVnode, pMsg);
      case STRIGGER_PULL_WAL_CALC_DATA:
        return vnodeProcessStreamWalCalcDataReq(pVnode, pMsg);
      default:
        vError("unknown msg type:%d in fetch queue", pMsg->msgType);
        return TSDB_CODE_APP_ERROR;
    }
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_CALC) {
    return 0;
  } else  {
    vError("unknown msg type:%d in stream reader queue", pMsg->msgType);
    return TSDB_CODE_APP_ERROR;
  }  
}
