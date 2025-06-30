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

#include "scalar.h"
#include "streamReader.h"
#include "tdb.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

#define BUILD_OPTION(options, sStreamInfo, _groupSort, _order, startTime, endTime, _schemas, _isSchema, _scanMode, \
                     _gid, _initReader)                                                                            \
  SStreamTriggerReaderTaskInnerOptions options = {.suid = sStreamInfo->suid,                                       \
                                                  .uid = sStreamInfo->uid,                                         \
                                                  .tableType = sStreamInfo->tableType,                             \
                                                  .groupSort = _groupSort,                                         \
                                                  .order = _order,                                                 \
                                                  .twindows = {.skey = startTime, .ekey = endTime},                \
                                                  .pTagCond = sStreamInfo->pTagCond,                               \
                                                  .pTagIndexCond = sStreamInfo->pTagIndexCond,                     \
                                                  .pConditions = sStreamInfo->pConditions,                         \
                                                  .partitionCols = sStreamInfo->partitionCols,                     \
                                                  .schemas = _schemas,                                             \
                                                  .isSchema = _isSchema,                                           \
                                                  .scanMode = _scanMode,                                           \
                                                  .gid = _gid,                                                     \
                                                  .initReader = _initReader};

static int64_t getSessionKey(int64_t session, int64_t type) { return (session | (type << 32)); }

// static int32_t insertTableToIgnoreList(SHashObj** pIgnoreTables, uint64_t uid) {
//   if (NULL == *pIgnoreTables) {
//     *pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
//     if (NULL == *pIgnoreTables) {
//       return terrno;
//     }
//   }

//   int32_t tempRes = taosHashPut(*pIgnoreTables, &uid, sizeof(uid), &uid, sizeof(uid));
//   if (tempRes != TSDB_CODE_SUCCESS) {
//     qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
//     return tempRes;
//   }
//   return TSDB_CODE_SUCCESS;
// }

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

static int32_t buildOTableInfoRsp(const SSTriggerOrigTableInfoRsp* rsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSTriggerOrigTableInfoRsp(NULL, 0, rsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSTriggerOrigTableInfoRsp(buf, len, rsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildVTableInfoRsp(const SStreamMsgVTableInfo* rsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSStreamMsgVTableInfo(NULL, 0, rsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSStreamMsgVTableInfo(buf, len, rsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildTsRsp(const SStreamTsResponse* tsRsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSStreamTsResponse(NULL, 0, tsRsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSStreamTsResponse(buf, len, tsRsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
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
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas, true,
                                                      pTask->options.twindows, pTask->options.suid));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderResetStatus(pTask->pReader, &pCond));

end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t buildWalMetaBlock(SSDataBlock* pBlock, int8_t type, int64_t gid, bool isVTable, int64_t uid,
                                 int64_t skey, int64_t ekey, int64_t ver, int64_t rows) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &type));
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &gid));
  }
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &uid));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &skey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ekey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &rows));

end:
  STREAM_PRINT_LOG_END(code, lino)
  return code;
}

static void buildTSchema(STSchema* pTSchema, int32_t ver, col_id_t colId, int8_t type, int32_t bytes) {
  pTSchema->numOfCols = 1;
  pTSchema->version = ver;
  pTSchema->columns[0].colId = colId;
  pTSchema->columns[0].type = type;
  pTSchema->columns[0].bytes = bytes;
}

int32_t retrieveWalMetaData(SSubmitTbData* pSubmitTbData, void* pTableList, bool isVTable, SSDataBlock* pBlock,
                            int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t   uid = pSubmitTbData->uid;
  int32_t   numOfRows = 0;
  int64_t   skey = 0;
  int64_t   ekey = 0;
  STSchema* pTSchema = NULL;
  uint64_t  gid = 0;
  if (isVTable) {
    STREAM_CHECK_CONDITION_GOTO(taosHashGet(pTableList, &uid, sizeof(uid)) == NULL, TDB_CODE_SUCCESS);
  } else {
    STREAM_CHECK_CONDITION_GOTO(!qStreamUidInTableList(pTableList, uid), TDB_CODE_SUCCESS);
    gid = qStreamGetGroupId(pTableList, uid);
  }

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

  STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_SUBMIT_DATA, gid, isVTable, uid, skey, ekey, ver, numOfRows));
  pBlock->info.rows++;
  stDebug("stream reader scan submit data:uid %" PRIu64 ", skey %" PRIu64 ", ekey %" PRIu64 ", gid %" PRIu64
          ", rows:%d",
          uid, skey, ekey, gid, numOfRows);

end:
  taosMemoryFree(pTSchema);
  STREAM_PRINT_LOG_END(code, lino)
  return code;
}

int32_t retrieveWalData(SSubmitTbData* pSubmitTbData, void* pTableList, SSDataBlock* pBlock, STSchema* schemas,
                        STimeWindow* window) {
  stDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t code = 0;
  int32_t lino = 0;

  int32_t ver = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  int32_t numOfRows = 0;
  schemas->version = ver;

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
      if (i >= taosArrayGetSize(pSubmitTbData->aCol)) {
        break;
      }
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
    for (int32_t j = 0; j < taosArrayGetSize(pSubmitTbData->aRowP); j++) {
      SRow* pRow = taosArrayGetP(pSubmitTbData->aRowP, j);
      STREAM_CHECK_NULL_GOTO(pRow, terrno);
      SColVal colVal = {0};
      STREAM_CHECK_RET_GOTO(tRowGet(pRow, schemas, 0, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (window != NULL && (ts < window->skey || ts > window->ekey)) {
        continue;
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {  // reader todo test null
        if (i >= schemas->numOfCols) {
          break;
        }
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
        STREAM_CHECK_NULL_GOTO(pColData, terrno);
        SColVal colVal = {0};
        int32_t sourceIdx = 0;
        while (1) {
          STREAM_CHECK_RET_GOTO(tRowGet(pRow, schemas, sourceIdx, &colVal));
          if (colVal.cid < pColData->info.colId) {
            sourceIdx++;
            continue;
          } else {
            break;
          }
        }
        if (colVal.cid == pColData->info.colId) {
          if (IS_VAR_DATA_TYPE(colVal.value.type) || colVal.value.type == TSDB_DATA_TYPE_DECIMAL){
            varColSetVarData(pColData, numOfRows, colVal.value.pData, colVal.value.nData, !COL_VAL_IS_VALUE(&colVal));
          } else {
            STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, numOfRows, (const char*)(&(colVal.value.val)), !COL_VAL_IS_VALUE(&colVal)));
          }
        } else {
          colDataSetNULL(pColData, numOfRows);
        }
      }
      numOfRows++;
    }
  }

  pBlock->info.rows = numOfRows;

end:
  STREAM_PRINT_LOG_END(code, lino)
  return code;
}

static int32_t scanDeleteData(void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                              int64_t ver) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SDecoder   decoder = {0};
  SDeleteRes req = {0};
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeDeleteRes(&decoder, &req));
  uint64_t gid = 0;
  if (isVTable) {
    STREAM_CHECK_CONDITION_GOTO(taosHashGet(pTableList, &req.suid, sizeof(req.suid)) == NULL, TDB_CODE_SUCCESS)
  } else {
    gid = qStreamGetGroupId(pTableList, req.suid);
    STREAM_CHECK_CONDITION_GOTO(gid == -1, TDB_CODE_SUCCESS);
  }
  STREAM_CHECK_RET_GOTO(
      buildWalMetaBlock(pBlock, WAL_DELETE_DATA, gid, isVTable, req.suid, req.skey, req.ekey, ver, 1));
  pBlock->info.rows++;
  stDebug("stream reader scan delete data:uid %" PRIu64 ", skey %" PRIu64 ", ekey %" PRIu64 ", gid %" PRIu64, req.suid,
          req.skey, req.ekey, gid);

end:
  tDecoderClear(&decoder);
  return code;
}

static int32_t scanDropTable(void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                             int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};

  SVDropTbBatchReq req = {0};
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeSVDropTbBatchReq(&decoder, &req));
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;
    STREAM_CHECK_NULL_GOTO(pDropTbReq, TSDB_CODE_INVALID_PARA);
    uint64_t gid = 0;
    if (isVTable) {
      if (taosHashGet(pTableList, &pDropTbReq->uid, sizeof(pDropTbReq->uid)) == NULL) {
        continue;
      }
    } else {
      gid = qStreamGetGroupId(pTableList, pDropTbReq->uid);
      if (gid == -1) continue;
    }

    STREAM_CHECK_RET_GOTO(buildWalMetaBlock(pBlock, WAL_DELETE_TABLE, gid, isVTable, pDropTbReq->uid, 0, 0, ver, 1));
    pBlock->info.rows++;
    stDebug("stream reader scan drop :uid %" PRIu64 ", gid %" PRIu64, pDropTbReq->uid, gid);
  }

end:
  tDecoderClear(&decoder);
  return code;
}

static int32_t scanSubmitData(void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                              int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};

  SSubmitReq2 submit = {0};
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeSubmitReq(&decoder, &submit, NULL));

  int32_t numOfBlocks = taosArrayGetSize(submit.aSubmitTbData);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, pBlock->info.rows + numOfBlocks));

  int32_t nextBlk = -1;
  while (++nextBlk < numOfBlocks) {
    stDebug("stream reader scan submit, next data block %d/%d", nextBlk, numOfBlocks);
    SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
    STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
    STREAM_CHECK_RET_GOTO(retrieveWalMetaData(pSubmitTbData, pTableList, isVTable, pBlock, ver));
  }
end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  tDecoderClear(&decoder);
  return code;
}

static int32_t scanWal(SVnode* pVnode, void* pTableList, bool isVTable, SSDataBlock* pBlock, int64_t lastVer,
                       int8_t deleteData, int8_t deleteTb, int64_t ctime, int64_t* retVer) {
  int32_t code = 0;
  int32_t lino = 0;

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  *retVer = walGetLastVer(pWalReader->pWal);
  STREAM_CHECK_CONDITION_GOTO(walReaderSeekVer(pWalReader, lastVer + 1) != 0, TSDB_CODE_SUCCESS);

  while (1) {
    *retVer = walGetLastVer(pWalReader->pWal);
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader) < 0, TSDB_CODE_SUCCESS);

    SWalCont* wCont = &pWalReader->pHead->head;
    if (wCont->ingestTs / 1000 > ctime) break;
    void*   data = POINTER_SHIFT(wCont->body, sizeof(SMsgHead));
    int32_t len = wCont->bodyLen - sizeof(SMsgHead);
    int64_t ver = wCont->version;

    if (wCont->msgType == TDMT_VND_DELETE && deleteData != 0) {
      STREAM_CHECK_RET_GOTO(scanDeleteData(pTableList, isVTable, pBlock, data, len, ver));
    } else if (wCont->msgType == TDMT_VND_DROP_TABLE && deleteTb != 0) {
      STREAM_CHECK_RET_GOTO(scanDropTable(pTableList, isVTable, pBlock, data, len, ver));
    } else if (wCont->msgType == TDMT_VND_SUBMIT) {
      data = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
      len = wCont->bodyLen - sizeof(SSubmitReq2Msg);
      STREAM_CHECK_RET_GOTO(scanSubmitData(pTableList, isVTable, pBlock, data, len, ver));
    }

    if (pBlock->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

end:
  walCloseReader(pWalReader);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t scanWalOneVer(SVnode* pVnode, void* pTableList, SSDataBlock* pBlock, SSDataBlock* pBlockRet, STSchema* schemas,
                      int64_t ver, int64_t uid, STimeWindow* window) {
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
    stDebug("stream reader next data block %d/%d", nextBlk, numOfBlocks);
    SSubmitTbData* pSubmitTbData = taosArrayGet(submit.aSubmitTbData, nextBlk);
    STREAM_CHECK_NULL_GOTO(pSubmitTbData, terrno);
    if (pSubmitTbData->uid != uid) {
      stDebug("stream reader skip data block uid:%" PRId64, pSubmitTbData->uid);
      continue;
    }
    STREAM_CHECK_RET_GOTO(retrieveWalData(pSubmitTbData, pTableList, pBlock, schemas, window));
    printDataBlock(pBlock, __func__, "");

    blockDataMerge(pBlockRet, pBlock);
    blockDataCleanup(pBlock);
  }

end:
  tDestroySubmitReq(&submit, TSDB_MSG_FLG_DECODE);
  walCloseReader(pWalReader);
  tDecoderClear(&decoder);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t processTag(SVnode* pVnode, SExprInfo* pExpr, int32_t numOfExpr, SStorageAPI* api, SSDataBlock* pBlock) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaReader mr = {0};

  if (numOfExpr == 0) {
    return TSDB_CODE_SUCCESS;
  }
  api->metaReaderFn.initReader(&mr, pVnode, META_READER_LOCK, &api->metaFn);
  code = api->metaReaderFn.getEntryGetUidCache(&mr, pBlock->info.id.uid);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to get table meta, uid:%" PRId64 ", code:%s", pBlock->info.id.uid, tstrerror(code));
  }
  api->metaReaderFn.readerReleaseLock(&mr);
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    STREAM_CHECK_NULL_GOTO(pColInfoData, terrno);
    if (mr.me.name == NULL) {
      colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
      continue;
    }
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    int32_t functionId = pExpr1->pExpr->_function.functionId;

    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      int32_t fType = pExpr1->pExpr->_function.functionType;
      if (fType == FUNCTION_TYPE_TBNAME) {
        STREAM_CHECK_RET_GOTO(setTbNameColData(pBlock, pColInfoData, functionId, mr.me.name));
        pColInfoData->info.colId = -1;
      }
    } else {  // these are tags
      STagVal tagVal = {0};
      tagVal.cid = pExpr1->base.pParam[0].pCol->colId;
      const char* p = api->metaFn.extractTagVal(mr.me.ctbEntry.pTags, pColInfoData->info.type, &tagVal);

      char* data = NULL;
      if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }

      bool isNullVal = (data == NULL) || (pColInfoData->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
      if (isNullVal) {
        colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
      } else {
        code = colDataSetNItems(pColInfoData, 0, data, pBlock->info.rows, false);
        if (IS_VAR_DATA_TYPE(((const STagVal*)p)->type)) {
          taosMemoryFree(data);
        }
        STREAM_CHECK_RET_GOTO(code);
      }
    }
  }

end:
  api->metaReaderFn.clearReader(&mr);

  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t processWalVerData(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamInfo, int64_t ver, bool isCalc,
                                 int64_t uid, STimeWindow* window, SSDataBlock** pBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SFilterInfo* pFilterInfo = NULL;
  void*        pTableList = NULL;
  SStorageAPI  api = {0};
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;

  STSchema*    schemas = sStreamInfo->triggerSchema; // alway use triggerSchema to avoid get data error from wal pRow
  SExprInfo*   pExpr = sStreamInfo->pExprInfo;
  int32_t      numOfExpr = sStreamInfo->numOfExpr;

  STREAM_CHECK_RET_GOTO(filterInitFromNode(isCalc ? sStreamInfo->pCalcConditions : sStreamInfo->pConditions, &pFilterInfo, 0, NULL));

  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
      pVnode, sStreamInfo->suid, sStreamInfo->uid, sStreamInfo->tableType, sStreamInfo->partitionCols, false,
      isCalc ? sStreamInfo->pCalcTagCond : sStreamInfo->pTagCond, NULL, &api, &pTableList, NULL));

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->triggerResBlock, false, &pBlock1));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->triggerResBlock, false, &pBlock2));
  if (isCalc) STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->calcResBlock, false, pBlock));
  pBlock2->info.id.uid = uid;
  pBlock1->info.id.uid = uid;

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pTableList, pBlock1, pBlock2, schemas, ver, uid, window));

  if (pBlock2->info.rows > 0) {
    STREAM_CHECK_RET_GOTO(processTag(pVnode, pExpr, numOfExpr, &api, pBlock2));
  }

  if (isCalc) {
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock2, pFilterInfo));
    blockDataTransform(*pBlock, pBlock2);
  } else {
    *pBlock = pBlock2;
    pBlock2 = NULL;  
  }

  printDataBlock(*pBlock, __func__, "");

end:
  STREAM_PRINT_LOG_END(code, lino);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock2);
  qStreamDestroyTableList(pTableList);
  return code;
}

static int32_t buildScheamFromCids(SVnode* pVnode, SArray* cols, int64_t uid, SArray** schemas) {
  int32_t code = 0;
  int32_t lino = 0;
  *schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(*schemas, terrno);

  SMetaReader metaReader = {0};
  SStorageAPI api = {0};
  initStorageAPI(&api);
  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);
  STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, uid));

  SSchemaWrapper* sSchemaWrapper = NULL;
  if (metaReader.me.type == TD_CHILD_TABLE) {
    int64_t suid = metaReader.me.ctbEntry.suid;
    tDecoderClear(&metaReader.coder);
    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, suid));
    sSchemaWrapper = &metaReader.me.stbEntry.schemaRow;
  } else if (metaReader.me.type == TD_NORMAL_TABLE) {
    sSchemaWrapper = &metaReader.me.ntbEntry.schemaRow;
  } else {
    qError("invalid table type:%d", metaReader.me.type);
  }

  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    col_id_t* id = taosArrayGet(cols, i);
    STREAM_CHECK_NULL_GOTO(id, terrno);
    for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
      SSchema* s = sSchemaWrapper->pSchema + j;
      if (*id == s->colId) {
        STREAM_CHECK_NULL_GOTO(taosArrayPush(*schemas, s), terrno);
        break;
      }
    }
  }

end:
  api.metaReaderFn.clearReader(&metaReader);
  if (code != 0)  taosArrayDestroy(*schemas);
  return code;
}

static int32_t processWalVerDataVTable(SVnode* pVnode, SArray *cids, int64_t ver,
  int64_t uid, STimeWindow* window, SSDataBlock** pBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;

  void*        pTableList = NULL;
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;

  STREAM_CHECK_RET_GOTO(buildScheamFromCids(pVnode, cids, uid, &schemas));
  STSchema* sSchema = tBuildTSchema(taosArrayGet(schemas, 0), taosArrayGetSize(schemas), 0);
  STREAM_CHECK_NULL_GOTO(sSchema, terrno);

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock1));
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock2));

  STREAM_CHECK_RET_GOTO(qStreamCreateTableListFromUid(uid, &pTableList));

  pBlock2->info.id.uid = uid;
  pBlock1->info.id.uid = uid;

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pTableList, pBlock1, pBlock2, sSchema, ver, uid, window));
  printDataBlock(pBlock2, __func__, "");

  *pBlock = pBlock2;
  pBlock2 = NULL;

end:
  STREAM_PRINT_LOG_END(code, lino);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock2);
  qStreamDestroyTableList(pTableList);
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createTSAndCondition(int64_t start, int64_t end, SLogicConditionNode** pCond,
                                    STargetNode* pTargetNodeTs) {
  int32_t code = 0;
  int32_t lino = 0;

  SColumnNode*         pCol = NULL;
  SColumnNode*         pCol1 = NULL;
  SValueNode*          pVal = NULL;
  SValueNode*          pVal1 = NULL;
  SOperatorNode*       op = NULL;
  SOperatorNode*       op1 = NULL;
  SLogicConditionNode* cond = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = LONG_BYTES;
  pCol->slotId = pTargetNodeTs->slotId;
  pCol->dataBlockId = pTargetNodeTs->dataBlockId;

  STREAM_CHECK_RET_GOTO(nodesCloneNode((SNode*)pCol, (SNode**)&pCol1));

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal));
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = LONG_BYTES;
  pVal->datum.i = start;
  pVal->typeData = start;

  STREAM_CHECK_RET_GOTO(nodesCloneNode((SNode*)pVal, (SNode**)&pVal1));
  pVal1->datum.i = end;
  pVal1->typeData = end;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op));
  op->opType = OP_TYPE_GREATER_EQUAL;
  op->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op->node.resType.bytes = CHAR_BYTES;
  op->pLeft = (SNode*)pCol;
  op->pRight = (SNode*)pVal;
  pCol = NULL;
  pVal = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op1));
  op1->opType = OP_TYPE_LOWER_EQUAL;
  op1->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op1->node.resType.bytes = CHAR_BYTES;
  op1->pLeft = (SNode*)pCol1;
  op1->pRight = (SNode*)pVal1;
  pCol1 = NULL;
  pVal1 = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&cond));
  cond->condType = LOGIC_COND_TYPE_AND;
  cond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  cond->node.resType.bytes = CHAR_BYTES;
  STREAM_CHECK_RET_GOTO(nodesMakeList(&cond->pParameterList));
  STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)op));
  op = NULL;
  STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)op1));
  op1 = NULL;

  *pCond = cond;

end:
  if (code != 0) {
    nodesDestroyNode((SNode*)pCol);
    nodesDestroyNode((SNode*)pCol1);
    nodesDestroyNode((SNode*)pVal);
    nodesDestroyNode((SNode*)pVal1);
    nodesDestroyNode((SNode*)op);
    nodesDestroyNode((SNode*)op1);
    nodesDestroyNode((SNode*)cond);
  }
  STREAM_PRINT_LOG_END(code, lino);

  return code;
}


static void calcTimeRange(STimeRangeNode* node, void* pStRtFuncInfo, SReadHandle* handle) {
  SStreamTSRangeParas timeStartParas = {.eType = SCL_VALUE_TYPE_START, .timeValue = INT64_MIN};
  SStreamTSRangeParas timeEndParas = {.eType = SCL_VALUE_TYPE_END, .timeValue = INT64_MAX};
  if (scalarCalculate(node->pStart, NULL, NULL, pStRtFuncInfo, &timeStartParas) == 0) {
    if (timeStartParas.opType == OP_TYPE_GREATER_THAN) {
      handle->winRange.skey = timeStartParas.timeValue + 1;
    } else if (timeStartParas.opType == OP_TYPE_GREATER_EQUAL) {
      handle->winRange.skey = timeStartParas.timeValue;
    } else {
      stError("start time range error, opType:%d", timeStartParas.opType);
      return;
    }
  } else {
    handle->winRange.skey = 0;
  }
  if (scalarCalculate(node->pEnd, NULL, NULL, pStRtFuncInfo, &timeEndParas) == 0) {
    if (timeEndParas.opType == OP_TYPE_LOWER_THAN) {
      handle->winRange.ekey = timeEndParas.timeValue - 1;
    } else if (timeEndParas.opType == OP_TYPE_LOWER_EQUAL) {
      handle->winRange.ekey = timeEndParas.timeValue;
    } else {
      stError("end time range error, opType:%d", timeEndParas.opType);
      return;
    }
  } else {
    handle->winRange.ekey = INT64_MAX;
  }
  stDebug("%s, skey:%" PRId64 ", ekey:%" PRId64, __func__, handle->winRange.skey, handle->winRange.ekey);
  handle->winRangeValid = true;
}

static int32_t createExternalConditions(SStreamRuntimeFuncInfo* data, SLogicConditionNode** pCond, STargetNode* pTargetNodeTs, STimeRangeNode* node) {
  int32_t              code = 0;
  int32_t              lino = 0;
  SLogicConditionNode* pAndCondition = NULL;
  SLogicConditionNode* cond = NULL;

  if (pTargetNodeTs == NULL) {
    vError("stream reader %s no ts column", __func__);
    return TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN;
  }
  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&cond));
  cond->condType = LOGIC_COND_TYPE_OR;
  cond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  cond->node.resType.bytes = CHAR_BYTES;
  STREAM_CHECK_RET_GOTO(nodesMakeList(&cond->pParameterList));

  for (int i = 0; i < taosArrayGetSize(data->pStreamPesudoFuncVals); ++i) {
    data->curIdx = i;

    SReadHandle handle = {0};
    calcTimeRange(node, data, &handle);
    if (!handle.winRangeValid) {
      stError("stream reader %s invalid time range, skey:%" PRId64 ", ekey:%" PRId64, __func__, handle.winRange.skey,
              handle.winRange.ekey);
      continue;
    }
    STREAM_CHECK_RET_GOTO(createTSAndCondition(handle.winRange.skey, handle.winRange.ekey, &pAndCondition, pTargetNodeTs));
    stDebug("%s create condition skey:%" PRId64 ", eksy:%" PRId64, __func__, handle.winRange.skey, handle.winRange.ekey);
    STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)pAndCondition));
    pAndCondition = NULL;
  }

  *pCond = cond;

end:
  if (code != 0) {
    nodesDestroyNode((SNode*)pAndCondition);
    nodesDestroyNode((SNode*)cond);
  }
  STREAM_PRINT_LOG_END(code, lino);

  return code;
}

static int32_t processCalaTimeRange(SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo, SResFetchReq* req,
                                    STimeRangeNode* node, SReadHandle* handle) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* funcVals = NULL;
  if (req->pStRtFuncInfo->withExternalWindow) {
    nodesDestroyNode(sStreamReaderCalcInfo->tsConditions);
    filterFreeInfo(sStreamReaderCalcInfo->pFilterInfo);
    sStreamReaderCalcInfo->pFilterInfo = NULL;

    STREAM_CHECK_RET_GOTO(createExternalConditions(req->pStRtFuncInfo,
                                                   (SLogicConditionNode**)&sStreamReaderCalcInfo->tsConditions,
                                                   sStreamReaderCalcInfo->pTargetNodeTs, node));

    STREAM_CHECK_RET_GOTO(filterInitFromNode((SNode*)sStreamReaderCalcInfo->tsConditions,
                                             (SFilterInfo**)&sStreamReaderCalcInfo->pFilterInfo, 0, NULL));
    SSTriggerCalcParam* pFirst = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, 0);
    SSTriggerCalcParam* pLast = taosArrayGetLast(req->pStRtFuncInfo->pStreamPesudoFuncVals);
    STREAM_CHECK_NULL_GOTO(pFirst, terrno);
    STREAM_CHECK_NULL_GOTO(pLast, terrno);

    handle->winRange.skey = pFirst->wstart;
    handle->winRange.ekey = pLast->wend;
    handle->winRangeValid = true;
    stDebug("%s withExternalWindow is true, skey:%" PRId64 ", ekey:%" PRId64, __func__, pFirst->wstart, pLast->wend);
  } else {
    calcTimeRange(node, req->pStRtFuncInfo, handle);
  }

end:
  taosArrayDestroy(funcVals);
  return code;
}

static int32_t createBlockForWalMeta(SSDataBlock** pBlock, bool isVTable) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TINYINT, CHAR_BYTES, index++))  // type
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // gid
  }
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // uid
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ver
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // nrows

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, pBlock));

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createOptionsForLastTs(SStreamTriggerReaderTaskInnerOptions* options,
                                      SStreamTriggerReaderInfo*             sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // last ts

  BUILD_OPTION(op, sStreamReaderInfo, true, TSDB_ORDER_DESC, INT64_MIN, INT64_MAX, schemas, true,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0, sStreamReaderInfo->uidList == NULL);
  schemas = NULL;
  *options = op;

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createOptionsForFirstTs(SStreamTriggerReaderTaskInnerOptions* options,
                                       SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t start) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts

  BUILD_OPTION(op, sStreamReaderInfo, true, TSDB_ORDER_ASC, start, INT64_MAX, schemas, true,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0, sStreamReaderInfo->uidList == NULL);
  schemas = NULL;

  *options = op;
end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createOptionsForTsdbMeta(SStreamTriggerReaderTaskInnerOptions* options,
                                        SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t start) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  int32_t index = 0;
  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // uid
  if (sStreamReaderInfo->uidList == NULL) {
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, index++))  // gid
  }
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))     // nrows

  BUILD_OPTION(op, sStreamReaderInfo, true, TSDB_ORDER_ASC, start, INT64_MAX, schemas, true, STREAM_SCAN_ALL, 0,
               sStreamReaderInfo->uidList == NULL);
  schemas = NULL;
  *options = op;

end:
  taosArrayDestroy(schemas);
  return code;
}

static int taosCompareInt64Asc(const void* elem1, const void* elem2) {
  int64_t* node1 = (int64_t*)elem1;
  int64_t* node2 = (int64_t*)elem2;

  if (*node1 < *node2) {
    return -1;
  }

  return *node1 > *node2;
}

static int32_t getTableList(SArray** pList, int32_t* pNum, int64_t* suid, int32_t index,
                            SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t* start = taosArrayGet(sStreamReaderInfo->uidListIndex, index);
  STREAM_CHECK_NULL_GOTO(start, terrno);
  int32_t  iStart = *start;
  int32_t* end = taosArrayGet(sStreamReaderInfo->uidListIndex, index + 1);
  STREAM_CHECK_NULL_GOTO(end, terrno);
  int32_t iEnd = *end;
  *pList = taosArrayInit(iEnd - iStart, sizeof(STableKeyInfo));
  STREAM_CHECK_NULL_GOTO(*pList, terrno);
  for (int32_t i = iStart; i < iEnd; ++i) {
    int64_t*       uid = taosArrayGet(sStreamReaderInfo->uidList, i);
    STableKeyInfo* info = taosArrayReserve(*pList, 1);
    STREAM_CHECK_NULL_GOTO(info, terrno);
    *suid = uid[0];
    info->uid = uid[1];
  }
  *pNum = iEnd - iStart;
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t processTsNonVTable(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                  SStreamReaderTaskInner* pTask) {
  int32_t code = 0;
  int32_t lino = 0;

  tsRsp->tsInfo = taosArrayInit(qStreamGetTableListGroupNum(pTask->pTableList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      break;
    }
    STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    tsInfo->ts = pTask->pResBlock->info.window.ekey;
    tsInfo->gId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);
    stDebug("vgId:%d %s get last ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, TD_VID(pVnode), __func__, tsInfo->ts,
            tsInfo->gId, tsRsp->ver);

    pTask->currentGroupIndex++;
    if (pTask->currentGroupIndex >= qStreamGetTableListGroupNum(pTask->pTableList)) {
      break;
    }
    STREAM_CHECK_RET_GOTO(resetTsdbReader(pTask));
  }

end:
  return code;
}

static int32_t processTsVTable(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                               SStreamReaderTaskInner* pTask) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t suid = 0;
  SArray* pList = NULL;
  int32_t pNum = 0;

  tsRsp->tsInfo = taosArrayInit(taosArrayGetSize(sStreamReaderInfo->uidList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(sStreamReaderInfo->uidListIndex) - 1; ++i) {
    STREAM_CHECK_RET_GOTO(getTableList(&pList, &pNum, &suid, i, sStreamReaderInfo));

    SQueryTableDataCond pCond = {0};
    STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas,
                                                        pTask->options.isSchema, pTask->options.twindows, suid));
    STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(
        pVnode, &pCond, taosArrayGet(pList, 0), pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, NULL));
    taosArrayDestroy(pList);
    pList = NULL;
    while (true) {
      bool hasNext = false;
      STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
      if (!hasNext) {
        break;
      }
      STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
      STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
      tsInfo->ts = pTask->pResBlock->info.window.ekey;
      tsInfo->gId = pTask->pResBlock->info.id.uid;
      stDebug("vgId:%d %s get vtable last ts:%" PRId64 ", uid:%" PRIu64 ", ver:%" PRId64, TD_VID(pVnode), __func__,
              tsInfo->ts, tsInfo->gId, tsRsp->ver);
    }
    pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
    pTask->pReader = NULL;
  }

end:
  taosArrayDestroy(pList);
  return code;
}

static int32_t processTsdbMetaNonVTable(SVnode* pVnode, int64_t key, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                        SStreamReaderTaskInner* pTask) {
  int32_t code = 0;
  int32_t lino = 0;

  while (true) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.id.groupId));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 4, &pTask->pResBlock->info.rows));

    stDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
            pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
    pTask->pResBlockDst->info.rows++;
    if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
end:
  return code;
}

static int32_t processTsdbMetaVTable(SVnode* pVnode, int64_t key, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                     SStreamReaderTaskInner* pTask) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t suid = 0;
  SArray* pList = NULL;
  int32_t pNum = 0;

  while (pTask->pReader != NULL || pTask->index < taosArrayGetSize(sStreamReaderInfo->uidListIndex) - 1) {
    if (pTask->pReader == NULL) {
      STREAM_CHECK_RET_GOTO(getTableList(&pList, &pNum, &suid, pTask->index, sStreamReaderInfo));

      SQueryTableDataCond pCond = {0};
      STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas,
                                                          pTask->options.isSchema, pTask->options.twindows, suid));
      STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(
          pVnode, &pCond, taosArrayGet(pList, 0), pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, NULL));
      taosArrayDestroy(pList);
      pList = NULL;
      pTask->index++;
    }

    while (true) {
      bool hasNext = false;
      STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
      if (!hasNext) {
        pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
        pTask->pReader = NULL;
        break;
      }
      pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

      STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 0, &pTask->pResBlock->info.id.uid));
      STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 1, &pTask->pResBlock->info.window.skey));
      STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 2, &pTask->pResBlock->info.window.ekey));
      STREAM_CHECK_RET_GOTO(addColData(pTask->pResBlockDst, 3, &pTask->pResBlock->info.rows));

      stDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
              TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
              pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
      pTask->pResBlockDst->info.rows++;

      if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
        goto end;
      }
    }
  }
  if (pTask->pReader == NULL) {
    taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
  }

end:
  taosArrayDestroy(pList);
  return code;
}

static void reSetUid(SStreamTriggerReaderTaskInnerOptions* options, int64_t suid, int64_t uid) {
  if (suid != 0) options->suid = suid;
  options->uid = uid;
  if (options->suid != 0) {
    options->tableType = TD_CHILD_TABLE;
  } else {
    options->tableType = TD_NORMAL_TABLE;
  }
}

static int32_t createOptionsForTsdbData(SVnode* pVnode, SStreamTriggerReaderTaskInnerOptions* options,
                                        SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid, SArray* cols,
                                        int64_t skey, int64_t ekey) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  STREAM_CHECK_RET_GOTO(buildScheamFromCids(pVnode, cols, uid, &schemas));
  BUILD_OPTION(op, sStreamReaderInfo, true, TSDB_ORDER_ASC, skey, ekey, schemas, true, STREAM_SCAN_ALL, 0, false);
  *options = op;

end:
  return code;
}

static int32_t vnodeProcessStreamSetTableReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  TSWAP(sStreamReaderInfo->uidList, req->setTableReq.uids);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidList, TSDB_CODE_INVALID_PARA);

  taosArraySort(sStreamReaderInfo->uidList, taosCompareInt64Asc);
  if (sStreamReaderInfo->uidListIndex != NULL) {
    taosArrayClear(sStreamReaderInfo->uidListIndex);
  } else {
    sStreamReaderInfo->uidListIndex = taosArrayInit(taosArrayGetSize(sStreamReaderInfo->uidList), sizeof(int32_t));
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidListIndex, TSDB_CODE_INVALID_PARA);
  }

  if (sStreamReaderInfo->uidHash != NULL) {
    taosHashClear(sStreamReaderInfo->uidHash);
  } else {
    sStreamReaderInfo->uidHash = taosHashInit(taosArrayGetSize(sStreamReaderInfo->uidList),
                                              taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidHash, TSDB_CODE_INVALID_PARA);
  }

  int64_t suid = 0;
  int32_t cnt = taosArrayGetSize(sStreamReaderInfo->uidList);
  for (int32_t i = 0; i < cnt; ++i) {
    int64_t* data = taosArrayGet(sStreamReaderInfo->uidList, i);
    STREAM_CHECK_NULL_GOTO(data, terrno);
    if (*data == 0 || *data != suid) {
      STREAM_CHECK_NULL_GOTO(taosArrayPush(sStreamReaderInfo->uidListIndex, &i), terrno);
    }
    suid = *data;
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->uidHash, data + 1, LONG_BYTES, &i, sizeof(int32_t)));
  }
  STREAM_CHECK_NULL_GOTO(taosArrayPush(sStreamReaderInfo->uidListIndex, &cnt), terrno);

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       lastTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  SStreamTriggerReaderTaskInnerOptions options = {0};
  STREAM_CHECK_RET_GOTO(createOptionsForLastTs(&options, sStreamReaderInfo));
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, NULL, NULL, &api));

  lastTsRsp.ver = pVnode->state.applied;
  if (sStreamReaderInfo->uidList != NULL) {
    STREAM_CHECK_RET_GOTO(processTsVTable(pVnode, &lastTsRsp, sStreamReaderInfo, pTask));
  } else {
    STREAM_CHECK_RET_GOTO(processTsNonVTable(pVnode, &lastTsRsp, sStreamReaderInfo, pTask));
  }
  stDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildTsRsp(&lastTsRsp, &buf, &size))

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(lastTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamTsResponse       firstTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  SStreamTriggerReaderTaskInnerOptions options = {0};
  STREAM_CHECK_RET_GOTO(createOptionsForFirstTs(&options, sStreamReaderInfo, req->firstTsReq.startTime));
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, NULL, NULL, &api));

  firstTsRsp.ver = pVnode->state.applied;
  if (sStreamReaderInfo->uidList != NULL) {
    STREAM_CHECK_RET_GOTO(processTsVTable(pVnode, &firstTsRsp, sStreamReaderInfo, pTask));
  } else {
    STREAM_CHECK_RET_GOTO(processTsNonVTable(pVnode, &firstTsRsp, sStreamReaderInfo, pTask));
  }

  stDebug("vgId:%d %s get result", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildTsRsp(&firstTsRsp, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(firstTsRsp.tsInfo);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_META);

  if (req->base.type == STRIGGER_PULL_TSDB_META) {
    SStreamTriggerReaderTaskInnerOptions options = {0};

    STREAM_CHECK_RET_GOTO(createOptionsForTsdbMeta(&options, sStreamReaderInfo, req->tsdbMetaReq.startTime));
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, NULL, sStreamReaderInfo->groupIdMap, &api));

    STREAM_CHECK_RET_GOTO(createOneDataBlock(pTask->pResBlock, false, &pTask->pResBlockDst));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTask = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INTERNAL_ERROR);
  }

  pTask->pResBlockDst->info.rows = 0;

  if (sStreamReaderInfo->uidList != NULL) {
    STREAM_CHECK_RET_GOTO(processTsdbMetaVTable(pVnode, key, sStreamReaderInfo, pTask));
  } else {
    STREAM_CHECK_RET_GOTO(processTsdbMetaNonVTable(pVnode, key, sStreamReaderInfo, pTask));
  }

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamTsDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey,
               sStreamReaderInfo->triggerCols, false, STREAM_SCAN_ALL, 0, true);
  reSetUid(&options, req->tsdbTsDataReq.suid, req->tsdbTsDataReq.uid);
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, sStreamReaderInfo->triggerResBlock, NULL, &api));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTask->pResBlockDst));

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
    if (!hasNext) {
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(processTag(pVnode, sStreamReaderInfo->pExprInfo, sStreamReaderInfo->numOfExpr, &api, pBlock));
    }
    
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    stDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
            pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
  }
  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  releaseStreamTask(pTask);
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_TRIGGER_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_TRIGGER_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbTriggerDataReq.startTime, INT64_MAX,
                 sStreamReaderInfo->triggerCols, false, STREAM_SCAN_ALL, 0, true);
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, sStreamReaderInfo->triggerResBlock,
                                           sStreamReaderInfo->groupIdMap, &api));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));

    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTask->pResBlockDst));

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
      taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(
        processTag(pVnode, sStreamReaderInfo->pExprInfo, sStreamReaderInfo->numOfExpr, &pTask->api, pBlock));
    }
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    stDebug("vgId:%d %s get skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTask->pResBlock->info.window.skey, pTask->pResBlock->info.window.ekey,
            pTask->pResBlock->info.id.uid, pTask->pResBlock->info.id.groupId, pTask->pResBlock->info.rows);
    if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

  SStreamReaderTaskInner* pTask = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_CALC_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_CALC_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, true, TSDB_ORDER_ASC, req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey,
                 sStreamReaderInfo->triggerCols, false, STREAM_SCAN_ALL, req->tsdbCalcDataReq.gid, true);
    options.pConditions = sStreamReaderInfo->pCalcConditions;
    options.pTagCond    = sStreamReaderInfo->pCalcTagCond;
    options.pTagIndexCond = NULL;
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, sStreamReaderInfo->triggerResBlock, NULL, &api));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));

    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &pTask->pResBlockDst));

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
      taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      break;
    }
    pTask->pResBlock->info.id.groupId = qStreamGetGroupId(pTask->pTableList, pTask->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTask->pFilterInfo));
    blockDataTransform(sStreamReaderInfo->calcResBlockTmp, pBlock);
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, sStreamReaderInfo->calcResBlockTmp));
    if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  SStreamReaderTaskInner* pTask = NULL;
  // int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_CALC_DATA);
  int64_t key = req->tsdbDataReq.uid;

  if (req->base.type == STRIGGER_PULL_TSDB_DATA) {
    SStreamTriggerReaderTaskInnerOptions options = {0};

    STREAM_CHECK_RET_GOTO(createOptionsForTsdbData(pVnode, &options, sStreamReaderInfo, req->tsdbDataReq.uid,
                                                   req->tsdbDataReq.cids, req->tsdbDataReq.skey,
                                                   req->tsdbDataReq.ekey));
    reSetUid(&options, req->tsdbDataReq.suid, req->tsdbDataReq.uid);

    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTask, NULL, NULL, &api));

    STableKeyInfo       keyInfo = {.uid = req->tsdbDataReq.uid};
    SQueryTableDataCond pCond = {0};
    STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, pTask->options.order, pTask->options.schemas,
                                                        pTask->options.isSchema, pTask->options.twindows,
                                                        req->tsdbDataReq.suid));
    STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(pVnode, &pCond, &keyInfo, 1, pTask->pResBlock,
                                                             (void**)&pTask->pReader, pTask->idStr, NULL));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTask, sizeof(pTask)));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pTask->pResBlock, false, &pTask->pResBlockDst));
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
      taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
      break;
    }

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTask, &pBlock));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTask->pResBlockDst, pBlock));
    if (pTask->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  STREAM_CHECK_RET_GOTO(buildRsp(pTask->pResBlockDst, &buf, &size));
  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTask->pResBlockDst->info.rows);

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamWalMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  void*        pTableList = NULL;
  SNodeList*   groupNew = NULL;
  int64_t      lastVer = 0;

  stDebug("vgId:%d %s start, request paras lastVer:%" PRId64 ",ctime:%" PRId64, TD_VID(pVnode), __func__,
          req->walMetaReq.lastVer, req->walMetaReq.ctime);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  bool isVTable = sStreamReaderInfo->uidList != NULL;
  if (sStreamReaderInfo->uidList == NULL) {
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));
    STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
        pVnode, sStreamReaderInfo->suid, sStreamReaderInfo->uid, sStreamReaderInfo->tableType,
        groupNew, false, sStreamReaderInfo->pTagCond, sStreamReaderInfo->pTagIndexCond, &api,
        &pTableList, sStreamReaderInfo->groupIdMap));
  }

  STREAM_CHECK_RET_GOTO(createBlockForWalMeta(&pBlock, isVTable));
  STREAM_CHECK_RET_GOTO(scanWal(pVnode, isVTable ? sStreamReaderInfo->uidHash : pTableList, isVTable, pBlock,
                                req->walMetaReq.lastVer, sStreamReaderInfo->deleteReCalc,
                                sStreamReaderInfo->deleteOutTbl, req->walMetaReq.ctime, &lastVer));

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));
  printDataBlock(pBlock, __func__, "");

end:
  if (pBlock != NULL && pBlock->info.rows == 0) {
    code = TSDB_CODE_WAL_LOG_NOT_EXIST;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = lastVer;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
    code = 0;
  }
  STREAM_PRINT_LOG_END(code, lino);
  nodesDestroyList(groupNew);
  blockDataDestroy(pBlock);
  qStreamDestroyTableList(pTableList);

  return code;
}

static int32_t vnodeProcessStreamWalTsDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;
  void*   buf = NULL;
  size_t  size = 0;

  SSDataBlock* pBlock = NULL;
  stDebug("vgId:%d %s start, uid:%" PRId64 ",ver:%" PRId64, TD_VID(pVnode), __func__, req->walTsDataReq.uid,
          req->walTsDataReq.ver);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walTsDataReq.ver, false, req->walTsDataReq.uid, NULL, &pBlock))
  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  // SArray*      schemas = NULL;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walTriggerDataReq.ver, false,
                                          req->walTriggerDataReq.uid, NULL, &pBlock));

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;

  stDebug("vgId:%d %s start, request skey:%" PRId64 ",ekey:%" PRId64 ",uid:%" PRId64 ",ver:%" PRId64, TD_VID(pVnode),
          __func__, req->walCalcDataReq.skey, req->walCalcDataReq.ekey, req->walCalcDataReq.uid,
          req->walCalcDataReq.ver);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  STimeWindow window = {.skey = req->walCalcDataReq.skey, .ekey = req->walCalcDataReq.ekey};
  STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walCalcDataReq.ver, true,
                                          req->walCalcDataReq.uid, &window, &pBlock));

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  printDataBlock(pBlock, __func__, "");
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamWalDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_RET_GOTO(processWalVerDataVTable(pVnode, req->walDataReq.cids, req->walDataReq.ver,
                                          req->walDataReq.uid, NULL, &pBlock));

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);

  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamGroupColValueReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  stDebug("vgId:%d %s start, request gid:%" PRId64, TD_VID(pVnode), __func__, req->groupColValueReq.gid);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  SArray** gInfo = taosHashGet(sStreamReaderInfo->groupIdMap, &req->groupColValueReq.gid, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(gInfo, TSDB_CODE_STREAM_NO_CONTEXT);
  SStreamGroupInfo pGroupInfo = {0};
  pGroupInfo.gInfo = *gInfo;

  size = tSerializeSStreamGroupInfo(NULL, 0, &pGroupInfo, TD_VID(pVnode));
  STREAM_CHECK_CONDITION_GOTO(size < 0, size);
  buf = rpcMallocCont(size);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  size = tSerializeSStreamGroupInfo(buf, size, &pGroupInfo, TD_VID(pVnode));
  STREAM_CHECK_CONDITION_GOTO(size < 0, size);
end:
  if (code != 0) {
    rpcFreeCont(buf);
    buf = NULL;
    size = 0;
  }
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamVTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t              code = 0;
  int32_t              lino = 0;
  void*                buf = NULL;
  size_t               size = 0;
  SStreamMsgVTableInfo vTableInfo = {0};
  SMetaReader          metaReader = {0};
  SNodeList*           groupNew = NULL;

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  SStorageAPI api = {0};
  initStorageAPI(&api);
  void* pTableList = NULL;
  STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
      pVnode, sStreamReaderInfo->suid, sStreamReaderInfo->uid, sStreamReaderInfo->tableType,
      groupNew, true, sStreamReaderInfo->pTagCond, sStreamReaderInfo->pTagIndexCond, &api,
      &pTableList, sStreamReaderInfo->groupIdMap));

  SArray* cids = req->virTableInfoReq.cids;
  STREAM_CHECK_NULL_GOTO(cids, terrno);

  vTableInfo.schema.nCols = taosArrayGetSize(cids);
  vTableInfo.schema.pSchema = taosMemoryCalloc(vTableInfo.schema.nCols, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(vTableInfo.schema.pSchema, terrno);
  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);

  SSchemaWrapper* sSchemaWrapper = NULL;
  if (sStreamReaderInfo->tableType == TD_SUPER_TABLE) {
    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, sStreamReaderInfo->suid));
    sSchemaWrapper = &metaReader.me.stbEntry.schemaRow;
  } else {
    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, sStreamReaderInfo->uid));
    sSchemaWrapper = &metaReader.me.ntbEntry.schemaRow;
  }
  for (size_t i = 0; i < taosArrayGetSize(cids); i++) {
    for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
      SSchema* s = sSchemaWrapper->pSchema + j;
      if (s->colId == *(col_id_t*)taosArrayGet(cids, i)) {
        memcpy(&vTableInfo.schema.pSchema[i], s, sizeof(SSchema));
        break;
      }
    }
  }
  tDecoderClear(&metaReader.coder);
  SArray* pTableListArray = qStreamGetTableArrayList(pTableList);
  STREAM_CHECK_NULL_GOTO(pTableListArray, terrno);

  vTableInfo.infos = taosArrayInit(taosArrayGetSize(pTableListArray), sizeof(VTableInfo));
  STREAM_CHECK_NULL_GOTO(vTableInfo.infos, terrno);

  for (size_t i = 0; i < taosArrayGetSize(pTableListArray); i++) {
    STableKeyInfo* pKeyInfo = taosArrayGet(pTableListArray, i);
    if (pKeyInfo == NULL) {
      continue;
    }
    VTableInfo* vTable = taosArrayReserve(vTableInfo.infos, 1);
    STREAM_CHECK_NULL_GOTO(vTable, terrno);
    vTable->uid = pKeyInfo->uid;
    vTable->gId = pKeyInfo->groupId;

    code = api.metaReaderFn.getTableEntryByUid(&metaReader, pKeyInfo->uid);
    vTable->ver = metaReader.me.version;
    vTable->cols.nCols = taosArrayGetSize(cids);
    vTable->cols.pColRef = taosMemoryCalloc(taosArrayGetSize(cids), sizeof(SColRef));
    for (size_t i = 0; i < taosArrayGetSize(cids); i++) {
      for (size_t j = 0; j < metaReader.me.colRef.nCols; j++) {
        if (metaReader.me.colRef.pColRef[j].hasRef &&
            metaReader.me.colRef.pColRef[j].id == *(col_id_t*)taosArrayGet(cids, i)) {
          memcpy(vTable->cols.pColRef + i, &metaReader.me.colRef.pColRef[j], sizeof(SColRef));
          break;
        }
      }
    }
    tDecoderClear(&metaReader.coder);
  }
  stDebug("vgId:%d %s end", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildVTableInfoRsp(&vTableInfo, &buf, &size));

end:
  nodesDestroyList(groupNew);
  qStreamDestroyTableList(pTableList);
  tDestroySStreamMsgVTableInfo(&vTableInfo);
  api.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamOTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  void*                     buf = NULL;
  size_t                    size = 0;
  SSTriggerOrigTableInfoRsp oTableInfo = {0};
  SMetaReader               metaReader = {0};

  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStorageAPI api = {0};
  initStorageAPI(&api);

  SArray* cols = req->origTableInfoReq.cols;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  oTableInfo.cols = taosArrayInit(taosArrayGetSize(cols), sizeof(OTableInfoRsp));
  ;
  STREAM_CHECK_NULL_GOTO(oTableInfo.cols, terrno);

  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    OTableInfo*    oInfo = taosArrayGet(cols, i);
    OTableInfoRsp* vTableInfo = taosArrayReserve(oTableInfo.cols, 1);
    STREAM_CHECK_NULL_GOTO(oInfo, terrno);
    STREAM_CHECK_NULL_GOTO(vTableInfo, terrno);
    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByName(&metaReader, oInfo->refTableName));
    vTableInfo->uid = metaReader.me.uid;

    SSchemaWrapper* sSchemaWrapper = NULL;
    if (metaReader.me.type == TD_CHILD_TABLE) {
      int64_t suid = metaReader.me.ctbEntry.suid;
      vTableInfo->suid = suid;
      tDecoderClear(&metaReader.coder);
      STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, suid));
      sSchemaWrapper = &metaReader.me.stbEntry.schemaRow;
    } else if (metaReader.me.type == TD_NORMAL_TABLE) {
      vTableInfo->suid = 0;
      sSchemaWrapper = &metaReader.me.ntbEntry.schemaRow;
    } else {
      qError("invalid table type:%d", metaReader.me.type);
    }

    for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
      SSchema* s = sSchemaWrapper->pSchema + j;
      if (strcmp(s->name, oInfo->refColName) == 0) {
        vTableInfo->cid = s->colId;
        break;
      }
    }
    if (vTableInfo->cid == 0) {
      stError("vgId:%d %s, not found col %s in table %s", TD_VID(pVnode), __func__, oInfo->refColName,
              oInfo->refTableName);
    }
    tDecoderClear(&metaReader.coder);
  }

  stDebug("vgId:%d %s end", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildOTableInfoRsp(&oTableInfo, &buf, &size));

end:
  tDestroySTriggerOrigTableInfoRsp(&oTableInfo);
  api.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamFetchMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t            code = 0;
  int32_t            lino = 0;
  void*              buf = NULL;
  size_t             size = 0;
  SStreamReaderTask* pTask = NULL;
  SSDataBlock*       pBlock = NULL;
  void*              taskAddr = NULL;
  
  stDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  SArray* calcInfoList = (SArray*)qStreamGetReaderInfo(req.queryId, req.taskId, &taskAddr);
  STREAM_CHECK_NULL_GOTO(calcInfoList, terrno);

  STREAM_CHECK_CONDITION_GOTO(req.execId < 0, TSDB_CODE_INVALID_PARA);
  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosArrayGetP(calcInfoList, req.execId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);

  if (req.reset || sStreamReaderCalcInfo->pTaskInfo == NULL) {
    qDestroyTask(sStreamReaderCalcInfo->pTaskInfo);
    int64_t uid = 0;
    if (req.dynTbname) {
      SArray* vals = req.pStRtFuncInfo->pStreamPartColVals;
      for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
        SStreamGroupValue* pValue = taosArrayGet(vals, i);
        if (pValue != NULL && pValue->isTbname) {
          uid = pValue->uid;
          break;
        }
      }
    }
    
    SReadHandle handle = {0};
    handle.vnode = pVnode;
    handle.uid = uid;

    initStorageAPI(&handle.api);
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode)){
      STimeRangeNode* node = (STimeRangeNode*)((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->pTimeRange;
      if (node != NULL) {
        STREAM_CHECK_RET_GOTO(processCalaTimeRange(sStreamReaderCalcInfo, &req, node, &handle));
      }
    }

    TSWAP(sStreamReaderCalcInfo->rtInfo.funcInfo, *req.pStRtFuncInfo);
    handle.streamRtInfo = &sStreamReaderCalcInfo->rtInfo;

    // if (sStreamReaderCalcInfo->pTaskInfo == NULL) {
    STREAM_CHECK_RET_GOTO(qCreateStreamExecTaskInfo(&sStreamReaderCalcInfo->pTaskInfo,
                                                    sStreamReaderCalcInfo->calcScanPlan, &handle, NULL, TD_VID(pVnode),
                                                    req.taskId));
    // } else {
    // STREAM_CHECK_RET_GOTO(qResetTableScan(sStreamReaderCalcInfo->pTaskInfo, handle.winRange));
    // }

    STREAM_CHECK_RET_GOTO(qSetTaskId(sStreamReaderCalcInfo->pTaskInfo, req.taskId, req.queryId));
  }

  while (1) {
    uint64_t ts = 0;
    STREAM_CHECK_RET_GOTO(qExecTask(sStreamReaderCalcInfo->pTaskInfo, &pBlock, &ts));
    printDataBlock(pBlock, __func__, "fetch");

    if (sStreamReaderCalcInfo->rtInfo.funcInfo.withExternalWindow && pBlock != NULL) {
      STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderCalcInfo->pFilterInfo));
      printDataBlock(pBlock, __func__, "fetch filter");

      if (pBlock->info.rows == 0 && !qTaskIsDone(sStreamReaderCalcInfo->pTaskInfo)) {
        continue;
      }
    }
    break;
  }

  stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock != NULL ? pBlock->info.rows : -1);
  STREAM_CHECK_RET_GOTO(buildFetchRsp(pBlock, &buf, &size, pVnode->config.tsdbCfg.precision));

end:

  streamReleaseTask(taskAddr);

  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.msgType = TDMT_STREAM_FETCH_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  tDestroySResFetchReq(&req);
  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  SSTriggerPullRequestUnion req = {0};
  void*                     taskAddr = NULL;

  vDebug("vgId:%d, msg:%p in stream reader queue is processing", pVnode->config.vgId, pMsg);
  if (!syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_STREAM_FETCH) {
    return vnodeProcessStreamFetchMsg(pVnode, pMsg);
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_PULL) {
    void*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int32_t len = pMsg->contLen - sizeof(SMsgHead);
    STREAM_CHECK_RET_GOTO(tDserializeSTriggerPullRequest(pReq, len, &req));
    stDebug("vgId:%d %s start, type:%d, streamId:%" PRId64 ", readerTaskId:%" PRId64 ", sessionId:%" PRId64,
            TD_VID(pVnode), __func__, req.base.type, req.base.streamId, req.base.readerTaskId, req.base.sessionId);
    SStreamTriggerReaderInfo* sStreamReaderInfo = (STRIGGER_PULL_OTABLE_INFO == req.base.type || STRIGGER_PULL_WAL_DATA == req.base.type) ? NULL : qStreamGetReaderInfo(req.base.streamId, req.base.readerTaskId, &taskAddr);
    switch (req.base.type) {
      case STRIGGER_PULL_SET_TABLE:
        code = vnodeProcessStreamSetTableReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_LAST_TS:
        code = vnodeProcessStreamLastTsReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_FIRST_TS:
        code = vnodeProcessStreamFirstTsReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_META:
      case STRIGGER_PULL_TSDB_META_NEXT:
        code = vnodeProcessStreamTsdbMetaReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_TS_DATA:
        code = vnodeProcessStreamTsDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT:
        code = vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_CALC_DATA:
      case STRIGGER_PULL_TSDB_CALC_DATA_NEXT:
        code = vnodeProcessStreamCalcDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_DATA:
      case STRIGGER_PULL_TSDB_DATA_NEXT:
        code = vnodeProcessStreamDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_META:
        code = vnodeProcessStreamWalMetaReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_TS_DATA:
        code = vnodeProcessStreamWalTsDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_TRIGGER_DATA:
        code = vnodeProcessStreamWalTriggerDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_CALC_DATA:
        code = vnodeProcessStreamWalCalcDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_DATA:
        code = vnodeProcessStreamWalDataReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_GROUP_COL_VALUE:
        code = vnodeProcessStreamGroupColValueReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_VTABLE_INFO:
        code = vnodeProcessStreamVTableInfoReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_OTABLE_INFO:
        code = vnodeProcessStreamOTableInfoReq(pVnode, pMsg, &req);
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

  streamReleaseTask(taskAddr);

  tDestroySTriggerPullRequest(&req);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}
