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

#include <stdbool.h>
#include <stdint.h>
#include "scalar.h"
#include "streamReader.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdb.h"
#include "tdef.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

#define BUILD_OPTION(options, sStreamInfo, _ver, _groupSort, _order, startTime, endTime, _schemas, _isSchema, _scanMode, \
                     _gid, _initReader, _uidList)                                                                        \
  SStreamTriggerReaderTaskInnerOptions options = {.suid = (_uidList == NULL ? sStreamInfo->suid : 0),              \
                                                  .uid = sStreamInfo->uid,                                         \
                                                  .ver = _ver,                                                     \
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
                                                  .initReader = _initReader,                                       \
                                                  .uidList = _uidList};

typedef struct WalMetaResult {
  uint64_t    gid;
  int64_t     skey;
  int64_t     ekey;
} WalMetaResult;

static int64_t getSessionKey(int64_t session, int64_t type) { return (session | (type << 32)); }

static int32_t buildScheamFromMeta(SVnode* pVnode, int64_t uid, SArray** schemas);

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

static bool uidInTableList(void* pTableList, bool isVTable, int64_t uid, uint64_t* gid){
  if (isVTable) {
    if(taosHashGet(pTableList, &uid, sizeof(uid)) == NULL) {
      return false;
    }
  } else {
    *gid = qStreamGetGroupId(pTableList, uid);
    if (*gid == -1) return false;
  }
  return true;
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

  cleanupQueryTableDataCond(&pTask->cond);
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTask->cond, pTask->options.order, pTask->options.schemas, true,
                                                      pTask->options.twindows, pTask->options.suid, pTask->options.ver));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderResetStatus(pTask->pReader, &pTask->cond));

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

static int32_t buildWalMetaBlockNew(SSDataBlock* pBlock, int8_t type, int64_t gid, bool isVTable, int64_t uid,
                                 int64_t skey, int64_t ekey, int64_t ver, int64_t rows) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &gid));
  }
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &skey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ekey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));

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
    gid = qStreamGetGroupId(pTableList, uid);
    if (gid == -1) return TDB_CODE_SUCCESS;
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

int32_t retrieveWalData(SVnode* pVnode, SSubmitTbData* pSubmitTbData, SSDataBlock* pBlock, STimeWindow* window) {
  stDebug("stream reader retrieve data block %p", pSubmitTbData);
  int32_t code = 0;
  int32_t lino = 0;

  int32_t ver = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  int32_t numOfRows = 0;
  STSchema* schemas = NULL;
  
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
    STSchema* schemas = metaGetTbTSchema(pVnode->pMeta, uid, ver, 1);
    STREAM_CHECK_NULL_GOTO(schemas, TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND); 
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
        if (colVal.cid == pColData->info.colId && COL_VAL_IS_VALUE(&colVal)) {
          if (IS_VAR_DATA_TYPE(colVal.value.type) || colVal.value.type == TSDB_DATA_TYPE_DECIMAL){
            STREAM_CHECK_RET_GOTO(varColSetVarData(pColData, numOfRows, colVal.value.pData, colVal.value.nData, !COL_VAL_IS_VALUE(&colVal)));
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
  taosMemoryFree(schemas);
  STREAM_PRINT_LOG_END(code, lino)
  return code;
}

static int32_t buildDeleteData(void* pTableList, bool isVTable, SSDataBlock* pBlock, SDeleteRes* req, int64_t uid, int64_t ver){
  int32_t    code = 0;
  int32_t    lino = 0;
  uint64_t   gid = 0;
  stDebug("stream reader scan delete start data:uid %" PRIu64 ", skey %" PRIu64 ", ekey %" PRIu64, uid, req->skey, req->ekey);
  if (isVTable) {
    STREAM_CHECK_CONDITION_GOTO(taosHashGet(pTableList, &uid, sizeof(uid)) == NULL, TDB_CODE_SUCCESS)
  } else {
    gid = qStreamGetGroupId(pTableList, uid);
    STREAM_CHECK_CONDITION_GOTO(gid == -1, TDB_CODE_SUCCESS);
  }
  STREAM_CHECK_RET_GOTO(
      buildWalMetaBlock(pBlock, WAL_DELETE_DATA, gid, isVTable, uid, req->skey, req->ekey, ver, 1));
  pBlock->info.rows++;

  stDebug("stream reader scan delete end data:uid %" PRIu64 ", skey %" PRIu64 ", ekey %" PRIu64, uid, req->skey, req->ekey);
end:
  return code;
}

static int32_t scanDeleteData(void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                              int64_t ver) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SDecoder   decoder = {0};
  SDeleteRes req = {0};
  req.uidList = taosArrayInit(0, sizeof(tb_uid_t));
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeDeleteRes(&decoder, &req));
  
  for (int32_t i = 0; i < taosArrayGetSize(req.uidList); i++) {
    uint64_t* uid = taosArrayGet(req.uidList, i);
    STREAM_CHECK_NULL_GOTO(uid, terrno);
    STREAM_CHECK_RET_GOTO(buildDeleteData(pTableList, isVTable, pBlock, &req, *uid, ver));
  }

end:
  taosArrayDestroy(req.uidList);
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

static int32_t processSubmitTbDataForMeta(SDecoder *pCoder, void* pTableList, bool isVTable, int64_t suid, SSHashObj* gidHash) {
  int32_t code = 0;
  int32_t lino = 0;
  WalMetaResult walMeta = {0};
  
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  SSubmitTbData submitTbData = {0};
  uint8_t       version = 0;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    if (tStartDecode(pCoder) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    tEndDecode(pCoder);
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  if (suid != 0 && submitTbData.suid != suid) {
    goto end;
  }
  if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (!uidInTableList(pTableList, isVTable, submitTbData.uid, &walMeta.gid)){
    goto end;
  }
  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    walMeta.skey = ((TSKEY *)colData.pData)[0];
    walMeta.ekey = ((TSKEY *)colData.pData)[colData.nVal - 1];

    for (uint64_t i = 1; i < nColData; i++) {
      code = tDecodeColData(version, pCoder, &colData, true);
      if (code) {
        code = TSDB_CODE_INVALID_MSG;
        TSDB_CHECK_CODE(code, lino, end);
      }
    }
  } else {
    uint64_t nRow = 0;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;
      if (iRow == 0){
#ifndef NO_UNALIGNED_ACCESS
        walMeta.skey = pRow->ts;
#else
        walMeta.skey = taosGetInt64Aligned(&pRow->ts);
#endif
      }
      if (iRow == nRow - 1) {
#ifndef NO_UNALIGNED_ACCESS
        walMeta.ekey = pRow->ts;
#else
        walMeta.ekey = taosGetInt64Aligned(&pRow->ts);
#endif
      }
    }
  }

  tEndDecode(pCoder);

  WalMetaResult* data = (WalMetaResult*)tSimpleHashGet(gidHash, &walMeta.gid, LONG_BYTES);
  if (data != NULL) {
    if (walMeta.skey < data->skey) data->skey = walMeta.skey;
    if (walMeta.ekey > data->ekey) data->ekey = walMeta.ekey;
  } else {
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(gidHash, &walMeta.gid, LONG_BYTES, &walMeta, sizeof(WalMetaResult)));
  }

end:
  if (code) {
    vError("%s:%d failed to vnodePreProcessSubmitTbData submit request since %s", __func__,
           lino, tstrerror(code));
  }
  return code;
}

static int32_t scanSubmitDataForMeta(void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                              int64_t ver, int64_t suid) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  SSHashObj* gidHash = NULL;

  tDecoderInit(&decoder, data, len);
  if (tStartDecode(&decoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint64_t nSubmitTbData = 0;
  if (tDecodeU64v(&decoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  gidHash = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(gidHash, terrno);

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    STREAM_CHECK_RET_GOTO(processSubmitTbDataForMeta(&decoder, pTableList, isVTable, suid, gidHash));
  }
  tEndDecode(&decoder);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, pBlock->info.rows + tSimpleHashGetSize(gidHash)));
  int32_t iter = 0;
  void*   px = tSimpleHashIterate(gidHash, NULL, &iter);
  while (px != NULL) {
    WalMetaResult* pMeta = (WalMetaResult*)px;
    STREAM_CHECK_RET_GOTO(buildWalMetaBlockNew(pBlock, WAL_SUBMIT_DATA, pMeta->gid, isVTable, 0, pMeta->skey, pMeta->ekey, ver, 0));
    pBlock->info.rows++;
    stDebug("stream reader scan submit data:skey %" PRId64 ", ekey %" PRId64 ", gid %" PRIu64
          ", ver:%"PRId64, pMeta->skey, pMeta->ekey, pMeta->gid, ver);
    px = tSimpleHashIterate(gidHash, px, &iter);
  }
end:
  tDecoderClear(&decoder);
  tSimpleHashCleanup( gidHash);
  return code;
}

static int32_t scanWal(SVnode* pVnode, void* pTableList, bool isVTable, SSDataBlock* pBlock, int64_t lastVer,
                       int8_t deleteData, int8_t deleteTb, int64_t ctime, int64_t* retVer) {
  int32_t code = 0;
  int32_t lino = 0;

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  *retVer = walGetAppliedVer(pWalReader->pWal);
  STREAM_CHECK_CONDITION_GOTO(walReaderSeekVer(pWalReader, lastVer + 1) != 0, TSDB_CODE_SUCCESS);

  while (1) {
    *retVer = walGetAppliedVer(pWalReader->pWal);
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader, true) < 0, TSDB_CODE_SUCCESS);

    SWalCont* wCont = &pWalReader->pHead->head;
    if (wCont->ingestTs / 1000 > ctime) break;
    void*   data = POINTER_SHIFT(wCont->body, sizeof(SMsgHead));
    int32_t len = wCont->bodyLen - sizeof(SMsgHead);
    int64_t ver = wCont->version;

    stDebug("vgId:%d stream reader scan wal ver:%" PRId64 ", type:%d, deleteData:%d, deleteTb:%d",
      TD_VID(pVnode), ver, wCont->msgType, deleteData, deleteTb);
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

static int32_t scanWalNew(SVnode* pVnode, void* pTableList, bool isVTable, SSDataBlock* pBlock, int64_t lastVer,
                       int8_t deleteData, int8_t deleteTb, int64_t suid, int64_t ctime, int64_t* retVer) {
  int32_t code = 0;
  int32_t lino = 0;

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  *retVer = walGetAppliedVer(pWalReader->pWal);
  STREAM_CHECK_CONDITION_GOTO(walReaderSeekVer(pWalReader, lastVer + 1) != 0, TSDB_CODE_SUCCESS);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));
  while (1) {
    *retVer = walGetAppliedVer(pWalReader->pWal);
    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader, true) < 0, TSDB_CODE_SUCCESS);

    SWalCont* wCont = &pWalReader->pHead->head;
    if (wCont->ingestTs / 1000 > ctime) break;
    void*   data = POINTER_SHIFT(wCont->body, sizeof(SMsgHead));
    int32_t len = wCont->bodyLen - sizeof(SMsgHead);
    int64_t ver = wCont->version;

    stDebug("vgId:%d stream reader scan wal ver:%" PRId64 ", type:%d, deleteData:%d, deleteTb:%d",
      TD_VID(pVnode), ver, wCont->msgType, deleteData, deleteTb);
    if (wCont->msgType == TDMT_VND_DELETE && deleteData != 0) {
      STREAM_CHECK_RET_GOTO(scanDeleteData(pTableList, isVTable, pBlock, data, len, ver));
    } else if (wCont->msgType == TDMT_VND_DROP_TABLE && deleteTb != 0) {
      STREAM_CHECK_RET_GOTO(scanDropTable(pTableList, isVTable, pBlock, data, len, ver));
    } else if (wCont->msgType == TDMT_VND_SUBMIT) {
      data = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
      len = wCont->bodyLen - sizeof(SSubmitReq2Msg);
      STREAM_CHECK_RET_GOTO(scanSubmitDataForMeta(pTableList, isVTable, pBlock, data, len, ver, suid));
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

int32_t scanWalOneVer(SVnode* pVnode, SSDataBlock* pBlockRet,
                      int64_t ver, int64_t uid, STimeWindow* window) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSubmitReq2 submit = {0};
  SDecoder    decoder = {0};

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
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
    STREAM_CHECK_RET_GOTO(retrieveWalData(pVnode, pSubmitTbData, pBlockRet, window));
    printDataBlock(pBlockRet, __func__, "");
    break;
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

static int32_t processWalVerData(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamInfo, int64_t ver, bool isTrigger,
                                 int64_t uid, STimeWindow* window, SSDataBlock* pSrcBlock, SSDataBlock** pBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SFilterInfo* pFilterInfo = NULL;
  SSDataBlock* pBlock2 = NULL;

  SExprInfo*   pExpr = sStreamInfo->pExprInfo;
  int32_t      numOfExpr = sStreamInfo->numOfExpr;

  STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0, NULL));

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->triggerResBlock, false, &pBlock2));
  if (!isTrigger) STREAM_CHECK_RET_GOTO(createOneDataBlock(pSrcBlock, false, pBlock));

  pBlock2->info.id.uid = uid;

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pBlock2, ver, uid, window));

  if (pBlock2->info.rows > 0) {
    SStorageAPI  api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(processTag(pVnode, pExpr, numOfExpr, &api, pBlock2));
  }
  STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock2, pFilterInfo));
  if (!isTrigger) {
    blockDataTransform(*pBlock, pBlock2);
  } else {
    *pBlock = pBlock2;
    pBlock2 = NULL;  
  }

  printDataBlock(*pBlock, __func__, "processWalVerData2");

end:
  STREAM_PRINT_LOG_END(code, lino);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock2);
  return code;
}

int32_t getRowRange(SColData* pCol, STimeWindow* window, int32_t* rowStart, int32_t* rowEnd, int32_t* nRows) {
  int32_t code = 0;
  int32_t lino = 0;
  *nRows = 0;
  *rowStart = 0;
  *rowEnd = pCol->nVal;
  if (window != NULL) {
    SColVal colVal = {0};
    *rowStart = -1;
    *rowEnd = -1;
    for (int32_t k = 0; k < pCol->nVal; k++) {
      STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, k, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (ts >= window->skey && *rowStart == -1) {
        *rowStart = k;
      }
      if (ts > window->ekey && *rowEnd == -1) {
        *rowEnd = k;
      }
    }
    STREAM_CHECK_CONDITION_GOTO(*rowStart == -1 || *rowStart == *rowEnd, TDB_CODE_SUCCESS);

    if (*rowStart != -1 && *rowEnd == -1) {
      *rowEnd = pCol->nVal;
    }
  }
  *nRows = *rowEnd - *rowStart;

end:
  return code;
}

static int32_t setColData(int64_t rows, int32_t rowStart, int32_t rowEnd, SColData* colData, SColumnInfoData* pColData) {
  int32_t code = 0;
  int32_t lino = 0;
  for (int32_t k = rowStart; k < rowEnd; k++) {
    SColVal colVal = {0};
    STREAM_CHECK_RET_GOTO(tColDataGetValue(colData, k, &colVal));
    STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, rows + k, VALUE_GET_DATUM(&colVal.value, colVal.value.type),
                                        !COL_VAL_IS_VALUE(&colVal)));
  }
  end:
  return code;
}

static int32_t processSubmitTbDataForMetaData(SVnode* pVnode, SDecoder *pCoder, void* pTableList, bool isVTable, int64_t suid, 
  SSDataBlock* pBlock, STSchema** schemas, SSHashObj* ranges) {
  int32_t code = 0;
  int32_t lino = 0;
  uint64_t gid = 0;
  
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  SSubmitTbData submitTbData = {0};
  uint8_t       version = 0;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    if (tStartDecode(pCoder) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    tEndDecode(pCoder);
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  if (suid != 0 && submitTbData.suid != suid) {
    goto end;
  }
  if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  STREAM_CHECK_CONDITION_GOTO(!uidInTableList(pTableList, isVTable, submitTbData.uid, &gid), TDB_CODE_SUCCESS);

  STimeWindow window = {.skey = INT64_MIN, .ekey = INT64_MAX};

  if (ranges != NULL){
    void* timerange = tSimpleHashGet(ranges, &gid, sizeof(gid));
    if (timerange == NULL) goto end;;
    int64_t* pRange = (int64_t*)timerange;
    window.skey = pRange[0];
    window.ekey = pRange[1];
  }
  
  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (*schemas == NULL) {
    *schemas = metaGetTbTSchema(pVnode->pMeta, submitTbData.suid != 0 ? submitTbData.suid : submitTbData.uid, submitTbData.sver, 1);
    STREAM_CHECK_NULL_GOTO(*schemas, TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND);
  }

  int32_t numOfRows = 0;
  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    int32_t rowStart = 0;
    int32_t rowEnd = 0;
    STREAM_CHECK_RET_GOTO(getRowRange(&colData, &window, &rowStart, &rowEnd, &numOfRows));
    STREAM_CHECK_CONDITION_GOTO(numOfRows <= 0, TDB_CODE_SUCCESS);
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, pBlock->info.rows + numOfRows));

    int32_t pos = pCoder->pos;
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pColData, terrno);
      if (pColData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        STREAM_CHECK_RET_GOTO(setColData(pBlock->info.rows, rowStart, rowEnd, &colData, pColData));
        continue;
      }
      pCoder->pos = pos;
      uint64_t j = 1;
      for (; j < nColData; j++) {
        int16_t cid = 0;
        int32_t posTmp = pCoder->pos;
        if ((code = tDecodeI16v(pCoder, &cid))) return code;
        if (cid == pColData->info.colId) {
          SColData colDataTmp = {0};
          code = tDecodeColData(version, pCoder, &colDataTmp, false);
          if (code) {
            code = TSDB_CODE_INVALID_MSG;
            TSDB_CHECK_CODE(code, lino, end);
          }
          STREAM_CHECK_RET_GOTO(setColData(pBlock->info.rows, rowStart, rowEnd, &colDataTmp, pColData));
          break;
        }
        pCoder->pos = posTmp;
        code = tDecodeColData(version, pCoder, &colData, true);
        if (code) {
          code = TSDB_CODE_INVALID_MSG;
          TSDB_CHECK_CODE(code, lino, end);
        }
      }
      if (j == nColData) {
        colDataSetNNULL(pColData, pBlock->info.rows, numOfRows);
      }
    }
  } else {
    uint64_t nRow = 0;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, pBlock->info.rows + nRow));
    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;
      SColVal colVal = {0};
      STREAM_CHECK_RET_GOTO(tRowGet(pRow, *schemas, 0, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (ts < window.skey || ts > window.ekey) {
        continue;
      }
     
      for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {  // reader todo test null
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
        STREAM_CHECK_NULL_GOTO(pColData, terrno);
        SColVal colVal = {0};
        int32_t sourceIdx = 0;
        while (1) {
          if (sourceIdx >= (*schemas)->numOfCols) {
            break;
          }
          STREAM_CHECK_RET_GOTO(tRowGet(pRow, *schemas, sourceIdx, &colVal));
          if (colVal.cid == pColData->info.colId) {
            break;
          }
          sourceIdx++;
        }
        if (colVal.cid == pColData->info.colId && COL_VAL_IS_VALUE(&colVal)) {
          if (IS_VAR_DATA_TYPE(colVal.value.type) || colVal.value.type == TSDB_DATA_TYPE_DECIMAL){
            STREAM_CHECK_RET_GOTO(varColSetVarData(pColData, pBlock->info.rows + numOfRows, (const char*)colVal.value.pData, colVal.value.nData, !COL_VAL_IS_VALUE(&colVal)));
          } else {
            STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, pBlock->info.rows + numOfRows, (const char*)(&(colVal.value.val)), !COL_VAL_IS_VALUE(&colVal)));
          }
        } else {
          colDataSetNULL(pColData, pBlock->info.rows + numOfRows);
        }
      }
      numOfRows++;
    }
  }

  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, -1); // uid
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  SColumnInfoData* pColData = taosArrayGetLast(pBlock->pDataBlock);
  STREAM_CHECK_NULL_GOTO(pColData, terrno);
  STREAM_CHECK_RET_GOTO(colDataSetNItems(pColData, pBlock->info.rows, (const char*)&submitTbData.uid, numOfRows, false));

  idata = createColumnInfoData(TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, -1); // gid
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  pColData = taosArrayGetLast(pBlock->pDataBlock);
  STREAM_CHECK_NULL_GOTO(pColData, terrno);
  STREAM_CHECK_RET_GOTO(colDataSetNItems(pColData, pBlock->info.rows, (const char*)&gid, numOfRows, false));
  
  pBlock->info.rows += numOfRows;

  tEndDecode(pCoder);
  
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t scanSubmitDataForMetaData(SVnode* pVnode, void* pTableList, bool isVTable, SSDataBlock* pBlock, void* data, int32_t len,
                              int64_t suid, SSHashObj* ranges) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STSchema* schemas = NULL;
  SDecoder decoder = {0};

  tDecoderInit(&decoder, data, len);
  if (tStartDecode(&decoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint64_t nSubmitTbData = 0;
  if (tDecodeU64v(&decoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    STREAM_CHECK_RET_GOTO(processSubmitTbDataForMetaData(pVnode, &decoder, pTableList, isVTable, suid, pBlock, &schemas, ranges));
  }
  tEndDecode(&decoder);

end:
  taosMemoryFree(schemas);
  tDecoderClear(&decoder);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t processWalVerDataNew(SVnode* pVnode, void* pTableList, SStreamTriggerReaderInfo* sStreamInfo, 
                                    int64_t lastVer, SArray* pBlockList, int64_t* retVer) {
  int32_t      code = 0;
  int32_t      lino = 0;

  SFilterInfo* pFilterInfo = NULL;
  SSDataBlock* pBlock = NULL;

  SExprInfo*   pExpr = sStreamInfo->pExprInfo;
  int32_t      numOfExpr = sStreamInfo->numOfExpr;

  STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0, NULL));

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  *retVer = walGetAppliedVer(pWalReader->pWal);

  STREAM_CHECK_CONDITION_GOTO(walReaderSeekVer(pWalReader, lastVer + 1) != 0, TSDB_CODE_SUCCESS);
  while (1) {
    *retVer = walGetAppliedVer(pWalReader->pWal);

    STREAM_CHECK_CONDITION_GOTO(walNextValidMsg(pWalReader, false) < 0, TSDB_CODE_SUCCESS);

    SWalCont* wCont = &pWalReader->pHead->head;
    void*   pBody = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = wCont->bodyLen - sizeof(SSubmitReq2Msg);

    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->triggerResBlock, false, &pBlock));
    STREAM_CHECK_RET_GOTO(scanSubmitDataForMetaData(pVnode, pTableList, sStreamInfo->uidList != NULL, pBlock, pBody, bodyLen, sStreamInfo->suid, NULL));
    pBlock->info.version = wCont->version;

    SStorageAPI  api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(processTag(pVnode, pExpr, numOfExpr, &api, pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pFilterInfo));
    if (pBlock->info.rows <= 0) {
      blockDataDestroy(pBlock);
      pBlock = NULL; 
      continue;
    }  
    stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
    printDataBlock(pBlock, __func__, "");
    STREAM_CHECK_NULL_GOTO(taosArrayPush(pBlockList, &pBlock), terrno);
    pBlock = NULL; 

    if (taosArrayGetSize(pBlockList) >= STREAM_RETURN_ROWS_NUM) {
      stDebug("vgId:%d %s reached max rows:%d", TD_VID(pVnode), __func__, STREAM_RETURN_ROWS_NUM);
      break;
    }
  }

end:
  walCloseReader(pWalReader);
  STREAM_PRINT_LOG_END(code, lino);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t processWalVerDataNew2(SVnode* pVnode, void* pTableList, SStreamTriggerReaderInfo* sStreamInfo, 
                                    SArray* versions, SSHashObj* ranges, SArray* pBlockList, int64_t* retVer) {
  int32_t      code = 0;
  int32_t      lino = 0;

  SFilterInfo* pFilterInfo = NULL;
  SSDataBlock* pBlock = NULL;

  SExprInfo*   pExpr = sStreamInfo->pExprInfo;
  int32_t      numOfExpr = sStreamInfo->numOfExpr;

  STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamInfo->pConditions, &pFilterInfo, 0, NULL));

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  *retVer = walGetAppliedVer(pWalReader->pWal);

  for(int32_t i = 0; i < taosArrayGetSize(versions); i++) {
    int64_t *ver = taosArrayGet(versions, i);
    if (ver == NULL) continue;

    STREAM_CHECK_RET_GOTO(walFetchHead(pWalReader, *ver));
    if(pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) continue;
    STREAM_CHECK_RET_GOTO(walFetchBody(pWalReader));
    SWalCont* wCont = &pWalReader->pHead->head;
    void*   pBody = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = wCont->bodyLen - sizeof(SSubmitReq2Msg);

    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamInfo->triggerResBlock, false, &pBlock));
    STREAM_CHECK_RET_GOTO(scanSubmitDataForMetaData(pVnode, pTableList, sStreamInfo->uidList != NULL, pBlock, pBody, bodyLen, sStreamInfo->suid, ranges));
    pBlock->info.version = wCont->version;

    SStorageAPI  api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(processTag(pVnode, pExpr, numOfExpr, &api, pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pFilterInfo));
    if (pBlock->info.rows <= 0) {
      blockDataDestroy(pBlock);
      pBlock = NULL; 
      continue;
    }  
    stDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
    printDataBlock(pBlock, __func__, "");
    STREAM_CHECK_NULL_GOTO(taosArrayPush(pBlockList, &pBlock), terrno);
    pBlock = NULL; 

    if (taosArrayGetSize(pBlockList) >= STREAM_RETURN_ROWS_NUM) {
      stDebug("vgId:%d %s reached max rows:%d", TD_VID(pVnode), __func__, STREAM_RETURN_ROWS_NUM);
      break;
    }
  }

end:
  walCloseReader(pWalReader);
  STREAM_PRINT_LOG_END(code, lino);
  filterFreeInfo(pFilterInfo);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t buildScheamFromMeta(SVnode* pVnode, int64_t uid, SArray** schemas) {
  int32_t code = 0;
  int32_t lino = 0;
  SMetaReader metaReader = {0};
  SStorageAPI api = {0};
  initStorageAPI(&api);
  *schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(*schemas, terrno);
  
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

  for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
    SSchema* s = sSchemaWrapper->pSchema + j;
    STREAM_CHECK_NULL_GOTO(taosArrayPush(*schemas, s), terrno);
  }

end:
  api.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  if (code != 0)  taosArrayDestroy(*schemas);
  return code;
}

static int32_t shrinkScheams(SArray* cols, SArray* schemas) {
  int32_t code = 0;
  int32_t lino = 0;
  for (size_t i = 0; i < taosArrayGetSize(schemas); i++) {
    SSchema* s = taosArrayGet(schemas, i);
    STREAM_CHECK_NULL_GOTO(s, terrno);

    size_t j = 0;
    for (; j < taosArrayGetSize(cols); j++) {
      col_id_t* id = taosArrayGet(cols, j);
      STREAM_CHECK_NULL_GOTO(id, terrno);
      if (*id == s->colId) {
        break;
      }
    }
    if (j == taosArrayGetSize(cols)) {
      // not found, remove it
      taosArrayRemove(schemas, i);
      i--;
    }
  }

end:
  return code;
}

static int32_t processWalVerDataVTable(SVnode* pVnode, SArray *cids, int64_t ver,
  int64_t uid, STimeWindow* window, SSDataBlock** pBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SArray*      schemas = NULL;

  SSDataBlock* pBlock2 = NULL;

  STREAM_CHECK_RET_GOTO(buildScheamFromMeta(pVnode, uid, &schemas));
  STREAM_CHECK_RET_GOTO(shrinkScheams(cids, schemas));
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlock2));

  pBlock2->info.id.uid = uid;

  STREAM_CHECK_RET_GOTO(scanWalOneVer(pVnode, pBlock2, ver, uid, window));
  printDataBlock(pBlock2, __func__, "");

  *pBlock = pBlock2;
  pBlock2 = NULL;

end:
  STREAM_PRINT_LOG_END(code, lino);
  blockDataDestroy(pBlock2);
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
    calcTimeRange(node, data, &handle.winRange, &handle.winRangeValid);
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
                                             (SFilterInfo**)&sStreamReaderCalcInfo->pFilterInfo,
                                             FLT_OPTION_NO_REWRITE | FLT_OPTION_SCALAR_MODE, NULL));
    SSTriggerCalcParam* pFirst = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, 0);
    SSTriggerCalcParam* pLast = taosArrayGetLast(req->pStRtFuncInfo->pStreamPesudoFuncVals);
    STREAM_CHECK_NULL_GOTO(pFirst, terrno);
    STREAM_CHECK_NULL_GOTO(pLast, terrno);

    handle->winRange.skey = pFirst->wstart;
    handle->winRange.ekey = pLast->wend;
    handle->winRangeValid = true;
    stDebug("%s withExternalWindow is true, skey:%" PRId64 ", ekey:%" PRId64, __func__, pFirst->wstart, pLast->wend);
  } else {
    calcTimeRange(node, req->pStRtFuncInfo, &handle->winRange, &handle->winRangeValid);
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

static int32_t createBlockForWalMetaNew(SSDataBlock** pBlock, bool isVTable) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 0;
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // gid
  }
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ver

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

  BUILD_OPTION(op, sStreamReaderInfo, -1, true, TSDB_ORDER_DESC, INT64_MIN, INT64_MAX, schemas, true,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0, sStreamReaderInfo->uidList == NULL, NULL);
  schemas = NULL;
  *options = op;

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createOptionsForFirstTs(SStreamTriggerReaderTaskInnerOptions* options,
                                       SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t start, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts

  BUILD_OPTION(op, sStreamReaderInfo, ver, true, TSDB_ORDER_ASC, start, INT64_MAX, schemas, true,
               STREAM_SCAN_GROUP_ONE_BY_ONE, 0, sStreamReaderInfo->uidList == NULL, NULL);
  schemas = NULL;

  *options = op;
end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createOptionsForTsdbMeta(SStreamTriggerReaderTaskInnerOptions* options,
                                        SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t start, int64_t end,
                                        int64_t gid, int8_t order, int64_t ver, bool onlyTs) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  int32_t index = 1;
  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // skey
  if (!onlyTs){
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // ekey
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // uid
    if (sStreamReaderInfo->uidList == NULL) {
      STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, index++))  // gid
    }
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))     // nrows
  }
  
  BUILD_OPTION(op, sStreamReaderInfo, ver, true, order, start, end, schemas, true, (gid != 0 ? STREAM_SCAN_GROUP_ONE_BY_ONE : STREAM_SCAN_ALL), gid,
               true, sStreamReaderInfo->uidList);
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
    pTask->api.tsdReader.tsdReaderReleaseDataBlock(pTask->pReader);
    STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    if (pTask->options.order == TSDB_ORDER_ASC) {
      tsInfo->ts = pTask->pResBlock->info.window.skey;
    } else {
      tsInfo->ts = pTask->pResBlock->info.window.ekey;
    }
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
                               SStreamReaderTaskInner* pTask, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t suid = 0;
  SArray* pList = NULL;
  int32_t pNum = 0;

  tsRsp->tsInfo = taosArrayInit(taosArrayGetSize(sStreamReaderInfo->uidList), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(sStreamReaderInfo->uidListIndex) - 1; ++i) {
    STREAM_CHECK_RET_GOTO(getTableList(&pList, &pNum, &suid, i, sStreamReaderInfo));

    cleanupQueryTableDataCond(&pTask->cond);
    STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTask->cond, pTask->options.order, pTask->options.schemas,
                                                        pTask->options.isSchema, pTask->options.twindows, suid, ver));
    STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(
        pVnode, &pTask->cond, taosArrayGet(pList, 0), pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, NULL));
    taosArrayDestroy(pList);
    pList = NULL;
    while (true) {
      bool hasNext = false;
      STREAM_CHECK_RET_GOTO(getTableDataInfo(pTask, &hasNext));
      if (!hasNext) {
        break;
      }
      pTask->api.tsdReader.tsdReaderReleaseDataBlock(pTask->pReader);
      STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
      STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
      if (pTask->options.order == TSDB_ORDER_ASC) {
        tsInfo->ts = pTask->pResBlock->info.window.skey;
      } else {
        tsInfo->ts = pTask->pResBlock->info.window.ekey;
      }
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
                                        int8_t order,int64_t skey, int64_t ekey, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  STREAM_CHECK_RET_GOTO(buildScheamFromMeta(pVnode, uid, &schemas));
  STREAM_CHECK_RET_GOTO(shrinkScheams(cols, schemas));
  BUILD_OPTION(op, sStreamReaderInfo, ver, true, order, skey, ekey, schemas, true, STREAM_SCAN_ALL, 0, false, NULL);
  *options = op;

end:
  return code;
}

static int32_t vnodeProcessStreamSetTableReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

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
    stDebug("vgId:%d %s suid:%" PRId64 ",uid:%" PRId64 ", index:%d", TD_VID(pVnode), __func__, suid, data[1], i);
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->uidHash, data + 1, LONG_BYTES, &i, sizeof(int32_t)));
  }
  STREAM_CHECK_NULL_GOTO(taosArrayPush(sStreamReaderInfo->uidListIndex, &cnt), terrno);

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SArray*                 schemas = NULL;
  SStreamReaderTaskInner* pTaskInner = NULL;
  SStreamTsResponse       lastTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamTriggerReaderTaskInnerOptions options = {0};
  STREAM_CHECK_RET_GOTO(createOptionsForLastTs(&options, sStreamReaderInfo));
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, NULL, NULL, &api));

  lastTsRsp.ver = pVnode->state.applied;
  if (sStreamReaderInfo->uidList != NULL) {
    STREAM_CHECK_RET_GOTO(processTsVTable(pVnode, &lastTsRsp, sStreamReaderInfo, pTaskInner, -1));
  } else {
    STREAM_CHECK_RET_GOTO(processTsNonVTable(pVnode, &lastTsRsp, sStreamReaderInfo, pTaskInner));
  }
  ST_TASK_DLOG("vgId:%d %s get result", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildTsRsp(&lastTsRsp, &buf, &size))

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(lastTsRsp.tsInfo);
  releaseStreamTask(&pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  SStreamTsResponse       firstTsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);
  SStreamTriggerReaderTaskInnerOptions options = {0};
  STREAM_CHECK_RET_GOTO(createOptionsForFirstTs(&options, sStreamReaderInfo, req->firstTsReq.startTime, req->firstTsReq.ver));
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, NULL, NULL, &api));
  
  firstTsRsp.ver = pVnode->state.applied;
  if (sStreamReaderInfo->uidList != NULL) {
    STREAM_CHECK_RET_GOTO(processTsVTable(pVnode, &firstTsRsp, sStreamReaderInfo, pTaskInner, req->firstTsReq.ver));
  } else {
    STREAM_CHECK_RET_GOTO(processTsNonVTable(pVnode, &firstTsRsp, sStreamReaderInfo, pTaskInner));
  }

  ST_TASK_DLOG("vgId:%d %s get result", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildTsRsp(&firstTsRsp, &buf, &size));

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(firstTsRsp.tsInfo);
  releaseStreamTask(&pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  SStreamTriggerReaderTaskInnerOptions options = {0};

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_META);

  if (req->base.type == STRIGGER_PULL_TSDB_META) {
    SStreamTriggerReaderTaskInnerOptions optionsTs = {0};

    STREAM_CHECK_RET_GOTO(createOptionsForTsdbMeta(&optionsTs, sStreamReaderInfo, req->tsdbMetaReq.startTime,
      req->tsdbMetaReq.endTime, req->tsdbMetaReq.gid, req->tsdbMetaReq.order, req->tsdbMetaReq.ver, true));
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &optionsTs, &pTaskInner, NULL, sStreamReaderInfo->groupIdMap, &api));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    
    STREAM_CHECK_RET_GOTO(createOptionsForTsdbMeta(&options, sStreamReaderInfo, req->tsdbMetaReq.startTime,
      req->tsdbMetaReq.endTime, req->tsdbMetaReq.gid, req->tsdbMetaReq.order, req->tsdbMetaReq.ver, false));
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(options.schemas, &pTaskInner->pResBlockDst));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  pTaskInner->pResBlockDst->info.rows = 0;
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pTaskInner->pResBlockDst, STREAM_RETURN_ROWS_NUM));
  bool hasNext = true;
  while (true) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->api.tsdReader.tsdReaderReleaseDataBlock(pTaskInner->pReader);
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupId(pTaskInner->pTableList, pTaskInner->pResBlock->info.id.uid);

    int32_t index = 0;
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.id.uid));
    if (sStreamReaderInfo->uidList == NULL) {
      STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.id.groupId));
    }
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.rows));

    stDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
            pTaskInner->pResBlockDst->info.rows++;
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTaskInner->pResBlockDst, &buf, &size));
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(options.schemas);
  return code;
}

static int32_t vnodeProcessStreamTsdbTsDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;
  SSDataBlock*            pBlockRes = NULL;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  BUILD_OPTION(options, sStreamReaderInfo, req->tsdbTsDataReq.ver, true, TSDB_ORDER_ASC, req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey,
               sStreamReaderInfo->triggerCols, false, STREAM_SCAN_ALL, 0, true, NULL);
  reSetUid(&options, req->tsdbTsDataReq.suid, req->tsdbTsDataReq.uid);
  SStorageAPI api = {0};
  initStorageAPI(&api);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock, NULL, &api));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTaskInner->pResBlockDst));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->tsBlock, false, &pBlockRes));

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupId(pTaskInner->pTableList, pTaskInner->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(processTag(pVnode, sStreamReaderInfo->pExprInfo, sStreamReaderInfo->numOfExpr, &api, pBlock));
    }
    
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTaskInner->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    ST_TASK_DLOG("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
  }

  blockDataTransform(pBlockRes, pTaskInner->pResBlockDst);

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlockRes, &buf, &size));

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlockRes);

  releaseStreamTask(&pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  SStreamReaderTaskInner* pTaskInner = NULL;
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);
  
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_TRIGGER_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_TRIGGER_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, req->tsdbTriggerDataReq.ver, true, req->tsdbTriggerDataReq.order, req->tsdbTriggerDataReq.startTime, INT64_MAX,
                 sStreamReaderInfo->triggerCols, false, (req->tsdbTriggerDataReq.gid != 0 ? STREAM_SCAN_GROUP_ONE_BY_ONE : STREAM_SCAN_ALL), 
                 req->tsdbTriggerDataReq.gid, true, NULL);
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock,
                                           sStreamReaderInfo->groupIdMap, &api));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));

    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTaskInner->pResBlockDst));

  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  pTaskInner->pResBlockDst->info.rows = 0;
  bool hasNext = true;
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupId(pTaskInner->pTableList, pTaskInner->pResBlock->info.id.uid);
    pTaskInner->pResBlockDst->info.id.groupId = qStreamGetGroupId(pTaskInner->pTableList, pTaskInner->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(
        processTag(pVnode, sStreamReaderInfo->pExprInfo, sStreamReaderInfo->numOfExpr, &pTaskInner->api, pBlock));
    }
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTaskInner->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    ST_TASK_DLOG("vgId:%d %s get skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
    if (pTaskInner->pResBlockDst->info.rows > 0) {
      break;
    }
  }

  STREAM_CHECK_RET_GOTO(buildRsp(pTaskInner->pResBlockDst, &buf, &size));
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamTsdbCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  SSDataBlock*            pBlockRes = NULL;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_CALC_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_CALC_DATA) {
    BUILD_OPTION(options, sStreamReaderInfo, req->tsdbCalcDataReq.ver, true, TSDB_ORDER_ASC, req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey,
                 sStreamReaderInfo->triggerCols, false, STREAM_SCAN_GROUP_ONE_BY_ONE, req->tsdbCalcDataReq.gid, true, NULL);
    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock, NULL, &api));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTaskInner->pResBlockDst));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &pBlockRes));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  pTaskInner->pResBlockDst->info.rows = 0;
  bool hasNext = true;
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupId(pTaskInner->pTableList, pTaskInner->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, pTaskInner->pFilterInfo));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  blockDataTransform(pBlockRes, pTaskInner->pResBlockDst);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlockRes, &buf, &size));
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlockRes->info.rows);
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlockRes);
  return code;
}

static int32_t vnodeProcessStreamTsdbVirtalDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t key = req->tsdbDataReq.uid;

  if (req->base.type == STRIGGER_PULL_TSDB_DATA) {
    SStreamTriggerReaderTaskInnerOptions options = {0};

    STREAM_CHECK_RET_GOTO(createOptionsForTsdbData(pVnode, &options, sStreamReaderInfo, req->tsdbDataReq.uid,
                                                   req->tsdbDataReq.cids, req->tsdbDataReq.order, req->tsdbDataReq.skey,
                                                   req->tsdbDataReq.ekey, req->tsdbDataReq.ver));
    reSetUid(&options, req->tsdbDataReq.suid, req->tsdbDataReq.uid);

    SStorageAPI api = {0};
    initStorageAPI(&api);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, NULL, NULL, &api));

    STableKeyInfo       keyInfo = {.uid = req->tsdbDataReq.uid};
    cleanupQueryTableDataCond(&pTaskInner->cond);
    STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTaskInner->cond, pTaskInner->options.order, pTaskInner->options.schemas,
                                                        pTaskInner->options.isSchema, pTaskInner->options.twindows,
                                                        pTaskInner->options.suid, pTaskInner->options.ver));
    STREAM_CHECK_RET_GOTO(pTaskInner->api.tsdReader.tsdReaderOpen(pVnode, &pTaskInner->cond, &keyInfo, 1, pTaskInner->pResBlock,
                                                             (void**)&pTaskInner->pReader, pTaskInner->idStr, NULL));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pTaskInner->pResBlock, false, &pTaskInner->pResBlockDst));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  pTaskInner->pResBlockDst->info.rows = 0;
  bool hasNext = true;
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  STREAM_CHECK_RET_GOTO(buildRsp(pTaskInner->pResBlockDst, &buf, &size));
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
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

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras lastVer:%" PRId64 ",ctime:%" PRId64, TD_VID(pVnode), __func__,
  req->walMetaReq.lastVer, req->walMetaReq.ctime);

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

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));
  printDataBlock(pBlock, __func__, "");

end:
  if (pBlock != NULL && pBlock->info.rows == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = lastVer;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  nodesDestroyList(groupNew);
  blockDataDestroy(pBlock);
  qStreamDestroyTableList(pTableList);

  return code;
}

static int32_t vnodeProcessStreamWalMetaNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;
  void*        pTableList = NULL;
  SNodeList*   groupNew = NULL;
  int64_t      lastVer = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras lastVer:%" PRId64, TD_VID(pVnode), __func__, req->walMetaNewReq.lastVer);

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

  STREAM_CHECK_RET_GOTO(createBlockForWalMetaNew(&pBlock, isVTable));
  STREAM_CHECK_RET_GOTO(scanWalNew(pVnode, isVTable ? sStreamReaderInfo->uidHash : pTableList, isVTable, pBlock,
                                req->walMetaNewReq.lastVer, sStreamReaderInfo->deleteReCalc,
                                sStreamReaderInfo->deleteOutTbl, sStreamReaderInfo->suid, INT64_MAX, &lastVer));

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));
  printDataBlock(pBlock, __func__, "");

end:
  if (pBlock != NULL && pBlock->info.rows == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = lastVer;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  nodesDestroyList(groupNew);
  blockDataDestroy(pBlock);
  qStreamDestroyTableList(pTableList);

  return code;
}

static int32_t vnodeProcessStreamWalMetaDataNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SArray*      pBlockList = NULL;
  void*        pTableList = NULL;
  SNodeList*   groupNew = NULL;
  int64_t      lastVer = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras lastVer:%" PRId64, TD_VID(pVnode), __func__, req->walMetaDataNewReq.lastVer);

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

  pBlockList = taosArrayInit(0, sizeof(SSDataBlock*));
  STREAM_CHECK_NULL_GOTO(pBlockList, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerDataNew(pVnode, pTableList, sStreamReaderInfo, req->walMetaDataNewReq.lastVer, pBlockList, &lastVer));

  ST_TASK_DLOG("vgId:%d %s get result block num:%zu", TD_VID(pVnode), __func__, taosArrayGetSize(pBlockList));

  size = tSerializeSStreamWalDataResponse(NULL, 0, pBlockList);
  buf = rpcMallocCont(size);
  tSerializeSStreamWalDataResponse(buf, size, pBlockList);

end:
  if (code == 0 && taosArrayGetSize(pTableList) == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = lastVer;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  nodesDestroyList(groupNew);
  qStreamDestroyTableList(pTableList);
  tDestroyBlockList(pBlockList);

  return code;
}

static int32_t vnodeProcessStreamWalDataNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SArray*      pBlockList = NULL;
  void*        pTableList = NULL;
  SNodeList*   groupNew = NULL;
  int64_t      lastVer = 0;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras size:%zu", TD_VID(pVnode), __func__, taosArrayGetSize(req->walDataNewReq.versions));

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

  pBlockList = taosArrayInit(0, sizeof(SSDataBlock*));
  STREAM_CHECK_NULL_GOTO(pBlockList, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerDataNew2(pVnode, pTableList, sStreamReaderInfo, req->walDataNewReq.versions, req->walDataNewReq.ranges, pBlockList, &lastVer));

  ST_TASK_DLOG("vgId:%d %s get result block num:%zu", TD_VID(pVnode), __func__, taosArrayGetSize(pBlockList));

  size = tSerializeSStreamWalDataResponse(NULL, 0, pBlockList);
  buf = rpcMallocCont(size);
  tSerializeSStreamWalDataResponse(buf, size, pBlockList);

end:
  if (code == 0 && taosArrayGetSize(pTableList) == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = lastVer;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  nodesDestroyList(groupNew);
  qStreamDestroyTableList(pTableList);
  tDestroyBlockList(pBlockList);

  return code;
}

static int32_t vnodeProcessStreamWalDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSDataBlock* pBlock = NULL;

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request type:%d skey:%" PRId64 ",ekey:%" PRId64 ",uid:%" PRId64 ",ver:%" PRId64, TD_VID(pVnode),
  __func__, req->walDataReq.base.type, req->walDataReq.skey, req->walDataReq.ekey, req->walDataReq.uid, req->walDataReq.ver);

  STimeWindow window = {.skey = req->walDataReq.skey, .ekey = req->walDataReq.ekey};
  if (req->walDataReq.base.type == STRIGGER_PULL_WAL_DATA){
    STREAM_CHECK_RET_GOTO(processWalVerDataVTable(pVnode, req->walDataReq.cids, req->walDataReq.ver,
      req->walDataReq.uid, &window, &pBlock));
  } else if (req->walDataReq.base.type == STRIGGER_PULL_WAL_CALC_DATA){
    STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walDataReq.ver, false,
      req->walDataReq.uid, &window, sStreamReaderInfo->calcResBlock, &pBlock));
  } else if (req->walDataReq.base.type == STRIGGER_PULL_WAL_TRIGGER_DATA) {
    STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walDataReq.ver, true,
      req->walDataReq.uid, &window, sStreamReaderInfo->triggerResBlock, &pBlock));
  } else if (req->walDataReq.base.type == STRIGGER_PULL_WAL_TS_DATA){
    STREAM_CHECK_RET_GOTO(processWalVerData(pVnode, sStreamReaderInfo, req->walDataReq.ver, false,
      req->walDataReq.uid, &window, sStreamReaderInfo->tsBlock, &pBlock));

  }
  
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  printDataBlock(pBlock, __func__, "");
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));
end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
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

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request gid:%" PRId64, TD_VID(pVnode), __func__, req->groupColValueReq.gid);

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
  STREAM_PRINT_LOG_END_WITHID(code, lino);
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
  void*                pTableList = NULL;
  SStorageAPI api = {0};
  initStorageAPI(&api);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
      pVnode, sStreamReaderInfo->suid, sStreamReaderInfo->uid, sStreamReaderInfo->tableType,
      groupNew, true, sStreamReaderInfo->pTagCond, sStreamReaderInfo->pTagIndexCond, &api,
      &pTableList, sStreamReaderInfo->groupIdMap));

  SArray* cids = req->virTableInfoReq.cids;
  STREAM_CHECK_NULL_GOTO(cids, terrno);

  SArray* pTableListArray = qStreamGetTableArrayList(pTableList);
  STREAM_CHECK_NULL_GOTO(pTableListArray, terrno);

  vTableInfo.infos = taosArrayInit(taosArrayGetSize(pTableListArray), sizeof(VTableInfo));
  STREAM_CHECK_NULL_GOTO(vTableInfo.infos, terrno);
  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);

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
    if (taosArrayGetSize(cids) == 1 && *(col_id_t*)taosArrayGet(cids, 0) == PRIMARYKEY_TIMESTAMP_COL_ID){
      vTable->cols.nCols = metaReader.me.colRef.nCols;
      vTable->cols.version = metaReader.me.colRef.version;
      vTable->cols.pColRef = taosMemoryCalloc(metaReader.me.colRef.nCols, sizeof(SColRef));
      for (size_t j = 0; j < metaReader.me.colRef.nCols; j++) {
        memcpy(vTable->cols.pColRef + j, &metaReader.me.colRef.pColRef[j], sizeof(SColRef));
      }
    } else {
      vTable->cols.nCols = taosArrayGetSize(cids);
      vTable->cols.version = metaReader.me.colRef.version;
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
    }
    tDecoderClear(&metaReader.coder);
  }
  ST_TASK_DLOG("vgId:%d %s end", TD_VID(pVnode), __func__);
  STREAM_CHECK_RET_GOTO(buildVTableInfoRsp(&vTableInfo, &buf, &size));

end:
  nodesDestroyList(groupNew);
  qStreamDestroyTableList(pTableList);
  tDestroySStreamMsgVTableInfo(&vTableInfo);
  api.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
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
  int64_t streamId = req->base.streamId;
  stsDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStorageAPI api = {0};
  initStorageAPI(&api);

  SArray* cols = req->origTableInfoReq.cols;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  oTableInfo.cols = taosArrayInit(taosArrayGetSize(cols), sizeof(OTableInfoRsp));

  STREAM_CHECK_NULL_GOTO(oTableInfo.cols, terrno);

  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    OTableInfo*    oInfo = taosArrayGet(cols, i);
    OTableInfoRsp* vTableInfo = taosArrayReserve(oTableInfo.cols, 1);
    STREAM_CHECK_NULL_GOTO(oInfo, terrno);
    STREAM_CHECK_NULL_GOTO(vTableInfo, terrno);
    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByName(&metaReader, oInfo->refTableName));
    vTableInfo->uid = metaReader.me.uid;
    stsDebug("vgId:%d %s uid:%"PRId64, TD_VID(pVnode), __func__, vTableInfo->uid);

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
      stError("invalid table type:%d", metaReader.me.type);
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

static int32_t vnodeProcessStreamVTableTagInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  void*                     buf = NULL;
  size_t                    size = 0;
  SSDataBlock* pBlock = NULL;

  SMetaReader               metaReader = {0};
  SMetaReader               metaReaderStable = {0};
  int64_t streamId = req->base.streamId;
  stsDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStorageAPI api = {0};
  initStorageAPI(&api);

  SArray* cols = req->virTablePseudoColReq.cids;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  api.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api.metaFn);
  STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReader, req->virTablePseudoColReq.uid));

  STREAM_CHECK_CONDITION_GOTO(metaReader.me.type != TD_VIRTUAL_CHILD_TABLE && metaReader.me.type != TD_VIRTUAL_NORMAL_TABLE, TSDB_CODE_INVALID_PARA);

  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  if (metaReader.me.type == TD_VIRTUAL_NORMAL_TABLE) {
    STREAM_CHECK_CONDITION_GOTO (taosArrayGetSize(cols) < 1 && *(col_id_t*)taosArrayGet(cols, 0) != -1, TSDB_CODE_INVALID_PARA);
    SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, 1));
    pBlock->info.rows = 1;
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, 0);
    STREAM_CHECK_NULL_GOTO(pDst, terrno);
    STREAM_CHECK_RET_GOTO(varColSetVarData(pDst, 0, metaReader.me.name, strlen(metaReader.me.name), false));
  } else if (metaReader.me.type == TD_VIRTUAL_CHILD_TABLE){
    int64_t suid = metaReader.me.ctbEntry.suid;
    api.metaReaderFn.readerReleaseLock(&metaReader);
    api.metaReaderFn.initReader(&metaReaderStable, pVnode, META_READER_LOCK, &api.metaFn);

    STREAM_CHECK_RET_GOTO(api.metaReaderFn.getTableEntryByUid(&metaReaderStable, suid));
    SSchemaWrapper*  sSchemaWrapper = &metaReaderStable.me.stbEntry.schemaTag;
    for (size_t i = 0; i < taosArrayGetSize(cols); i++){
      col_id_t* id = taosArrayGet(cols, i);
      STREAM_CHECK_NULL_GOTO(id, terrno);
      if (*id == -1) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
        continue;
      }
      size_t j = 0;
      for (; j < sSchemaWrapper->nCols; j++) {
        SSchema* s = sSchemaWrapper->pSchema + j;
        if (s->colId == *id) {
          SColumnInfoData idata = createColumnInfoData(s->type, s->bytes, s->colId);
          STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
          break;
        }
      }
      if (j == sSchemaWrapper->nCols) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_NULL, CHAR_BYTES, *id);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
      }
    }
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, 1));
    pBlock->info.rows = 1;
    
    for (size_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++){
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pDst, terrno);

      if (pDst->info.colId == -1) {
        STREAM_CHECK_RET_GOTO(varColSetVarData(pDst, 0, metaReader.me.name, strlen(metaReader.me.name), false));
        continue;
      }
      if (pDst->info.type == TSDB_DATA_TYPE_NULL) {
        STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, NULL, true));
        continue;
      }

      STagVal val = {0};
      val.cid = pDst->info.colId;
      const char* p = api.metaFn.extractTagVal(metaReader.me.ctbEntry.pTags, pDst->info.type, &val);

      char* data = NULL;
      if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }

      STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, data,
                            (data == NULL) || (pDst->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data))));

      if ((pDst->info.type != TSDB_DATA_TYPE_JSON) && (p != NULL) && IS_VAR_DATA_TYPE(((const STagVal*)p)->type) &&
          (data != NULL)) {
        taosMemoryFree(data);
      }
    }
  } else {
    stError("vgId:%d %s, invalid table type:%d", TD_VID(pVnode), __func__, metaReader.me.type);
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  
  stsDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  printDataBlock(pBlock, __func__, "");
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  api.metaReaderFn.clearReader(&metaReaderStable);
  api.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
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
  void*              taskAddr = NULL;
  SArray*            pResList = NULL;

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  SArray* calcInfoList = (SArray*)qStreamGetReaderInfo(req.queryId, req.taskId, &taskAddr);
  STREAM_CHECK_NULL_GOTO(calcInfoList, terrno);

  STREAM_CHECK_CONDITION_GOTO(req.execId < 0, TSDB_CODE_INVALID_PARA);
  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosArrayGetP(calcInfoList, req.execId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);
  void* pTask = sStreamReaderCalcInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, execId:%d, reset:%d, pTaskInfo:%p, scan type:%d", TD_VID(pVnode), __func__, req.execId, req.reset,
               sStreamReaderCalcInfo->pTaskInfo, nodeType(sStreamReaderCalcInfo->calcAst->pNode));

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

  if (req.pOpParam != NULL) {
    qUpdateOperatorParam(sStreamReaderCalcInfo->pTaskInfo, req.pOpParam);
  }
  
  pResList = taosArrayInit(4, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(pResList, terrno);
  uint64_t ts = 0;
  bool     hasNext = false;
  STREAM_CHECK_RET_GOTO(qExecTaskOpt(sStreamReaderCalcInfo->pTaskInfo, pResList, &ts, &hasNext, NULL, req.pOpParam != NULL));

  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL) continue;
    printDataBlock(pBlock, __func__, "fetch");
    if (sStreamReaderCalcInfo->rtInfo.funcInfo.withExternalWindow) {
      STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderCalcInfo->pFilterInfo));
      printDataBlock(pBlock, __func__, "fetch filter");
    }
  }

  STREAM_CHECK_RET_GOTO(streamBuildFetchRsp(pResList, hasNext, &buf, &size, pVnode->config.tsdbCfg.precision));
  ST_TASK_DLOG("vgId:%d %s end:", TD_VID(pVnode), __func__);

end:
  taosArrayDestroy(pResList);
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
    STREAM_CHECK_RET_GOTO(tDeserializeSTriggerPullRequest(pReq, len, &req));
    stDebug("vgId:%d %s start, type:%d, streamId:%" PRIx64 ", readerTaskId:%" PRIx64 ", sessionId:%" PRIx64,
            TD_VID(pVnode), __func__, req.base.type, req.base.streamId, req.base.readerTaskId, req.base.sessionId);
    SStreamTriggerReaderInfo* sStreamReaderInfo = (STRIGGER_PULL_OTABLE_INFO == req.base.type) ? NULL : qStreamGetReaderInfo(req.base.streamId, req.base.readerTaskId, &taskAddr);
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
        code = vnodeProcessStreamTsdbTsDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT:
        code = vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_CALC_DATA:
      case STRIGGER_PULL_TSDB_CALC_DATA_NEXT:
        code = vnodeProcessStreamTsdbCalcDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_TSDB_DATA:
      case STRIGGER_PULL_TSDB_DATA_NEXT:
        code = vnodeProcessStreamTsdbVirtalDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_META:
        code = vnodeProcessStreamWalMetaReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_TS_DATA:
      case STRIGGER_PULL_WAL_TRIGGER_DATA:
      case STRIGGER_PULL_WAL_CALC_DATA:
      case STRIGGER_PULL_WAL_DATA:
        code = vnodeProcessStreamWalDataReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_GROUP_COL_VALUE:
        code = vnodeProcessStreamGroupColValueReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_VTABLE_INFO:
        code = vnodeProcessStreamVTableInfoReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_VTABLE_PSEUDO_COL:
        code = vnodeProcessStreamVTableTagInfoReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_OTABLE_INFO:
        code = vnodeProcessStreamOTableInfoReq(pVnode, pMsg, &req);
        break;
      case STRIGGER_PULL_WAL_META_NEW:
        code = vnodeProcessStreamWalMetaNewReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_DATA_NEW:
        code = vnodeProcessStreamWalDataNewReq(pVnode, pMsg, &req, sStreamReaderInfo);
        break;
      case STRIGGER_PULL_WAL_META_DATA_NEW:
        code = vnodeProcessStreamWalMetaDataNewReq(pVnode, pMsg, &req, sStreamReaderInfo);
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
