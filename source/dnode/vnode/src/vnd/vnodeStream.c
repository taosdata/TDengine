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

#include "streamReader.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

#define BUILD_OPTION(options, sStreamInfo, _groupSort, _order, startTime, endTime, _schemas, _scanMode, _gid) \
  SStreamTriggerReaderTaskInnerOptions options = {.suid = sStreamInfo->suid,                                  \
                                                  .uid = sStreamInfo->uid,                                    \
                                                  .tableType = sStreamInfo->tableType,                        \
                                                  .groupSort = _groupSort,                                    \
                                                  .order = _order,                                            \
                                                  .twindows = {.skey = startTime, .ekey = endTime},           \
                                                  .pTagCond = sStreamInfo->pTagCond,                          \
                                                  .pTagIndexCond = sStreamInfo->pTagIndexCond,                \
                                                  .pConditions = sStreamInfo->pConditions,                    \
                                                  .partitionCols = sStreamInfo->partitionCols,                \
                                                  .schemas = _schemas,                                        \
                                                  .scanMode = _scanMode,                                      \
                                                  .gid = _gid}

static int64_t getSessionKey(int64_t session, int64_t type) { return (session | (type << 32)); }

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

static int32_t addColData(SSDataBlock* pResBlock, int32_t index, void* data) {
  SColumnInfoData* pSrc = taosArrayGet(pResBlock->pDataBlock, index);
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
  void*   buf = NULL;
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

static int32_t buildFetchRsp(SSDataBlock* pBlock, void** data, size_t* size, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;

  int32_t blockSize = pBlock == NULL ? 0 : blockGetEncodeSize(pBlock);
  size_t  dataEncodeBufSize = sizeof(SRetrieveTableRsp) + INT_BYTES * 2 + blockSize;
  buf = rpcMallocCont(dataEncodeBufSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->version = 0;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  *((int32_t*)(pRetrieve->data)) = blockSize;
  *((int32_t*)(pRetrieve->data + INT_BYTES)) = blockSize;
  if (pBlock == NULL || pBlock->info.rows == 0) {
    pRetrieve->numOfRows = 0;
    pRetrieve->numOfBlocks = 0;
    pRetrieve->completed = 1;
  } else {
    pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
    pRetrieve->numOfBlocks = htonl(1);
    int32_t actualLen =
        blockEncode(pBlock, pRetrieve->data + INT_BYTES * 2, blockSize, taosArrayGetSize(pBlock->pDataBlock));
    STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
  }

  *data = buf;
  *size = dataEncodeBufSize;
  buf = NULL;

end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildRsp(SSDataBlock* pBlock, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  STREAM_CHECK_CONDITION_GOTO(pBlock == NULL || pBlock->info.rows == 0, TSDB_CODE_SUCCESS);
  size_t dataEncodeSize = blockGetEncodeSize(pBlock);
  buf = rpcMallocCont(dataEncodeSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actualLen = blockEncode(pBlock, buf, dataEncodeSize, taosArrayGetSize(pBlock->pDataBlock));
  STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
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

int32_t retrieveWalMetaData(SSubmitTbData* pSubmitTbData, void* pTableList, SSDataBlock* pBlock, int64_t ver) {
  vDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t code = 0;
  int32_t lino = 0;

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
    buildTSchema(pTSchema, pSubmitTbData->sver, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES);
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
    numOfRows = rowEnd - rowStart;
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, numOfRows));
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
  } else {
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, taosArrayGetSize(pSubmitTbData->aRowP)));
    pTSchemaTs = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn));
    STREAM_CHECK_NULL_GOTO(pTSchemaTs, terrno);
    buildTSchema(pTSchemaTs, ver, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES);
    for (int32_t j = 0; j < taosArrayGetSize(pSubmitTbData->aRowP); j++) {
      SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, j);
      STREAM_CHECK_NULL_GOTO(pRow, terrno);
      SColVal colVal = {0};
      STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchemaTs, 0, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (window != NULL && (ts < window->skey || ts > window->ekey)) {
        continue;
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
        STREAM_CHECK_NULL_GOTO(pColData, terrno);
        SColVal colVal = {0};
        pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn));
        STREAM_CHECK_NULL_GOTO(pTSchema, terrno);
        buildTSchema(pTSchema, ver, pColData->info.colId, pColData->info.type, pColData->info.bytes);
        // STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchema, pColData->info.colId, &colVal));
        STREAM_CHECK_RET_GOTO(tRowGet(pRow, pTSchema, 0, &colVal));
        taosMemoryFreeClear(pTSchema);
        STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, numOfRows, VALUE_GET_DATUM(&colVal.value, colVal.value.type),
                                            !COL_VAL_IS_VALUE(&colVal)));
      }
      numOfRows++;
    }
  }

  pBlock->info.rows = numOfRows;

end:
  taosMemoryFree(pTSchemaTs);
  taosMemoryFree(pTSchema);
  PRINT_LOG_END(code, lino)
  return code;
}

int32_t scanWal(SVnode* pVnode, void* pTableList, SSDataBlock* pBlock, int64_t lastVer, int8_t deleteData,
                int8_t deleteTb) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSubmitReq2 submit = {0};
  SDecoder    decoder = {0};

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  pBlock->info.id.groupId = walGetLastVer(pWalReader->pWal);
  STREAM_CHECK_RET_GOTO(walReaderSeekVer(pWalReader, lastVer + 1));

  while (1) {
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader) < 0, TSDB_CODE_SUCCESS);

    void*   data = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SMsgHead));
    int32_t len = pWalReader->pHead->head.bodyLen - sizeof(SMsgHead);
    int64_t ver = pWalReader->pHead->head.version;

    if (pWalReader->pHead->head.msgType == TDMT_VND_DELETE) {
      if (deleteData == 0) continue;
      SDeleteRes req = {0};
      tDecoderInit(&decoder, data, len);
      STREAM_CHECK_RET_GOTO(tDecodeDeleteRes(&decoder, &req));
      tDecoderClear(&decoder);
      uint64_t gid = qStreamGetGroupId(pTableList, req.suid);
      if (gid == -1) continue;
      STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_DELETE_DATA, gid, req.suid, req.skey, req.ekey, ver, 1));
      pBlock->info.rows++;
      continue;
    } else if (pWalReader->pHead->head.msgType == TDMT_VND_DROP_TABLE) {
      if (deleteTb == 0) continue;
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
        pBlock->info.rows++;
      }
      continue;
    } else if (pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      continue;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int32_t nextBlk = -1;
    tDecoderInit(&decoder, pBody, bodyLen);
    STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));
    tDecoderClear(&decoder);

    int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);
    while (++nextBlk < numOfBlocks) {
      vDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
      SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
      STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
      STREAM_CHECK_RET_GOTO(retrieveWalMetaData(pSubmitTbData, pTableList, pBlock, ver));
    }
    tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  }

end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
  tDecoderClear(&decoder);
  if (code == TSDB_CODE_WAL_LOG_NOT_EXIST) {
    code = TSDB_CODE_SUCCESS;
    terrno = TSDB_CODE_SUCCESS;
  }
  PRINT_LOG_END(code, lino);
  return code;
}

int32_t scanWalOneVer(SVnode* pVnode, void* pTableList, SSDataBlock* pBlock, SSDataBlock* pBlockRet, int64_t ver,
                      int64_t uid, STimeWindow* window) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSubmitReq2 submit = {0};
  SDecoder    decoder = {0};

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);

  STREAM_CHECK_RET_GOTO(walFetchHead(pWalReader, ver));
  STREAM_CHECK_CONDITION_GOTO(pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT, TSDB_CODE_STREAM_WAL_VER_NOT_DATA);
  STREAM_CHECK_RET_GOTO(walFetchBody(pWalReader));
  void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
  int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);

  int32_t nextBlk = -1;
  tDecoderInit(&decoder, pBody, bodyLen);
  STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));

  int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);
  while (++nextBlk < numOfBlocks) {
    vDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
    SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
    STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
    if (pSubmitTbData->uid != uid) {
      vDebug("stream reader skip data block uid:%" PRId64, pSubmitTbData->uid);
      continue;
    }
    STREAM_CHECK_RET_GOTO(retrieveWalData(pSubmitTbData, pTableList, pBlock, window));
    blockDataMerge(pBlockRet, pBlock);
    blockDataCleanup(pBlock);
  }

end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
  tDecoderClear(&decoder);
  PRINT_LOG_END(code, lino);
  return code;
}

static int32_t processWalVerData(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamInfo, SArray* schemas, int64_t ver,
                                 int64_t uid, STimeWindow* window, SSDataBlock** pBlock) {
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
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
      pVnode, sStreamInfo->suid, sStreamInfo->uid, sStreamInfo->tableType, sStreamInfo->partitionCols, false,
      sStreamInfo->pTagCond, sStreamInfo->pTagIndexCond, &api, &pTableList));

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock1));
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock2));

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pTableList, pBlock1, pBlock2, ver, uid, window));
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

static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       lastTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // last ts

  BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_DESC, INT64_MIN, INT64_MAX, schemas,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0);
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
    vDebug("vgId:%d %s get last ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, TD_VID(pVnode), __func__, tsInfo->ts,
           tsInfo->gId, lastTsRsp.ver);

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
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  taosArrayDestroy(lastTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       firstTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts

  BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->firstTsReq.startTime, INT64_MAX, schemas,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0);
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
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  taosArrayDestroy(firstTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_META);

  if (req->base.type == STRIGGER_PULL_TSDB_META) {
    schemas = taosArrayInit(8, sizeof(SSchema));
    STREAM_CHECK_NULL_GOTO(schemas, terrno);
    // int16_t colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))     // uid
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, 1))    // gid
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, 2))  // skey
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, 3))  // ekey
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))     // nrows

    BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbMetaReq.startTime, INT64_MAX, schemas,
                 STREAM_SCAN_ALL, 0);
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(options.schemas, &pTask->pResBlockDst));

    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTask = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INTERNAL_ERROR);
  }

  pTask->pResBlockDst->info.rows = 0;
  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      if (req->base.type == STRIGGER_PULL_TSDB_META_NEXT) {
        taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      }
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.id.groupId));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 4, &pTask->pResBlock->info.rows));

    vDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
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
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  return code;
}

static int32_t vnodeProcessStreamTsDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // ts

  BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey,
               schemas, STREAM_SCAN_ALL, 0);
  options.uid = req->tsdbTsDataReq.uid;
  options.tableType = TD_CHILD_TABLE;
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
  taosArrayClear(schemas);
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, 0))  // ts
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
    vDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
           TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
           pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
  }
  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_TRIGGER_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_TRIGGER_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbTriggerDataReq.startTime, INT64_MAX,
                 sStreamReaderInfo->triggerCols, STREAM_SCAN_ALL, 0);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));

    STREAM_CHECK_RET_GOTO(createDataBlockForStream(sStreamReaderInfo->triggerCols, &pTask->pResBlockDst));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTask = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INTERNAL_ERROR);
  }

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      if (req->base.type == STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT) {
        taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      }
      goto end;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    vDebug("vgId:%d %s get skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
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
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_CALC_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_CALC_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey,
                 sStreamReaderInfo->calcCols, STREAM_SCAN_ALL, req->tsdbCalcDataReq.gid);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));

    STREAM_CHECK_RET_GOTO(createDataBlockForStream(sStreamReaderInfo->calcCols, &pTask->pResBlockDst));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTask = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INTERNAL_ERROR);
  }

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      if (req->base.type == STRIGGER_PULL_TSDB_CALC_DATA_NEXT) {
        taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      }
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
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamWalMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  SStorageAPI  api = {0};
  void*        pTableList = NULL;

  vDebug("vgId:%d %s start, request paras lastVer:%" PRId64, TD_VID(pVnode), __func__, req->walMetaReq.lastVer);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, sStreamReaderInfo->suid, sStreamReaderInfo->uid,
                                                        sStreamReaderInfo->tableType, sStreamReaderInfo->partitionCols,
                                                        false, sStreamReaderInfo->pTagCond,
                                                        sStreamReaderInfo->pTagIndexCond, &api, &pTableList));

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TINYINT, CHAR_BYTES, 0))  // type
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 1))   // gid
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 2))   // uid
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 3))   // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 4))   // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 5))   // ver
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 6))   // nrows

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock));
  STREAM_CHECK_RET_GOTO(scanWal(pVnode, pTableList, pBlock, req->walMetaReq.lastVer, sStreamReaderInfo->deleteReCalc,
                                sStreamReaderInfo->deleteOutTbl));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  qStreamDestroyTableList(pTableList);

  return code;
}

static int32_t vnodeProcessStreamWalTsDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, 0))  // ts

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, schemas, req->walTsDataReq.ver,
                                          req->walTsDataReq.uid, NULL, &pBlock))
  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, sStreamReaderInfo->triggerCols,
                                          req->walTriggerDataReq.ver, req->walTriggerDataReq.uid, NULL, &pBlock));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  vDebug("vgId:%d %s start, request skey:%" PRId64 ",ekey:%" PRId64 ",uid:%" PRId64 ",ver:%" PRId64, TD_VID(pVnode),
         __func__, req->walCalcDataReq.skey, req->walCalcDataReq.ekey, req->walCalcDataReq.uid,
         req->walCalcDataReq.ver);

  SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req->base.streamId, req->base.readerTaskId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  STimeWindow window = {.skey = req->walCalcDataReq.skey, .ekey = req->walCalcDataReq.ekey};
  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, sStreamReaderInfo->calcCols,
                                          req->walCalcDataReq.ver, req->walCalcDataReq.uid, &window, &pBlock));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamFetchMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t            code = 0;
  int32_t            lino = 0;
  void*              buf = NULL;
  size_t             size = 0;
  SStreamReaderTask* pTask = NULL;
  SSDataBlock*       pBlock = NULL;
  vDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  STREAM_CHECK_RET_GOTO(streamGetTask(req.queryId, req.taskId, (SStreamTask**)&pTask));
  STREAM_CHECK_CONDITION_GOTO(pTask->triggerReader != 0, TSDB_CODE_INVALID_PARA);
  if (req.reset || pTask->info.calcReaderInfo.pTaskInfo == NULL) {
    qDestroyTask(pTask->info.calcReaderInfo.pTaskInfo);
    SReadHandle handle = {
        .vnode = pVnode,
    };

    int32_t vgId = pTask->task.nodeId;
    int64_t streamId = pTask->task.streamId;
    int32_t taskId = pTask->task.taskId;

    initStorageAPI(&handle.api);
    STREAM_CHECK_RET_GOTO(qCreateStreamExecTaskInfo(
        &pTask->info.calcReaderInfo.pTaskInfo, pTask->info.calcReaderInfo.calcScanPlan, &handle, NULL, vgId, taskId));
    STREAM_CHECK_RET_GOTO(qSetTaskId(pTask->info.calcReaderInfo.pTaskInfo, taskId, streamId));
  }

  uint64_t ts = 0;
  // qStreamSetOpen(task);
  STREAM_CHECK_RET_GOTO(qExecTask(pTask->info.calcReaderInfo.pTaskInfo, &pBlock, &ts));

  vDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock != NULL ? pBlock->info.rows : -1);
  STREAM_CHECK_RET_GOTO(buildFetchRsp(pBlock, &buf, &size, pVnode->config.tsdbCfg.precision));

end:
  PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.msgType = TDMT_STREAM_FETCH_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  vTrace("vgId:%d, msg:%p in stream reader queue is processing", pVnode->config.vgId, pMsg);
  // if (!syncIsReadyForRead(pVnode->sync)) {
  // vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
  // return 0;
  // }

  if (pMsg->msgType == TDMT_STREAM_FETCH) {
    return vnodeProcessStreamFetchMsg(pVnode, pMsg);
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_PULL) {
    void*                     pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int32_t                   len = pMsg->contLen - sizeof(SMsgHead);
    SSTriggerPullRequestUnion req = {0};
    STREAM_CHECK_RET_GOTO(tDserializeSTriggerPullRequest(pReq, len, &req));
    switch (req.base.type) {
      case STRIGGER_PULL_LAST_TS:
        code = vnodeProcessStreamLastTsReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_FIRST_TS:
        code = vnodeProcessStreamFirstTsReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_TSDB_META:
      case STRIGGER_PULL_TSDB_META_NEXT:
        code = vnodeProcessStreamTsdbMetaReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_TSDB_TS_DATA:
        code = vnodeProcessStreamTsDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT:
        code = vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_TSDB_CALC_DATA:
      case STRIGGER_PULL_TSDB_CALC_DATA_NEXT:
        code = vnodeProcessStreamCalcDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_WAL_META:
        code = vnodeProcessStreamWalMetaReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_WAL_TS_DATA:
        code = vnodeProcessStreamWalTsDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_WAL_TRIGGER_DATA:
        code = vnodeProcessStreamWalTriggerDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_WAL_CALC_DATA:
        code = vnodeProcessStreamWalCalcDataReq(pVnode, pMsg, &req);
        break;
      default:
        vError("unknown inner msg type:%d in stream reader queue", req.base.type);
        code = TSDB_CODE_APP_ERROR;
        break;
    }
  } else {
    vError("unknown msg type:%d in stream reader queue", pMsg->msgType);
    code = TSDB_CODE_APP_ERROR;
  }
end:
  PRINT_LOG_END(code, lino);
  return code;
}
