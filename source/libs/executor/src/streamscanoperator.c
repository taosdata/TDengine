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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "streamexecutorInt.h"
#include "streamsession.h"
#include "systable.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"
#include "ttime.h"

#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

#include "storageapi.h"
#include "wal.h"

#define STREAM_DATA_SCAN_OP_NAME            "StreamDataScanOperator"
#define STREAM_DATA_SCAN_OP_STATE_NAME      "StreamDataScanFillHistoryState"
#define STREAM_DATA_SCAN_OP_CHECKPOINT_NAME "StreamDataScanOperator_Checkpoint"
#define STREAM_DATA_SCAN_OP_REC_ID_NAME     "StreamDataScanOperator_Recalculate_ID"

static int32_t getMaxTsKeyInfo(SStreamScanInfo* pInfo, SSDataBlock* pBlock, TSKEY* pCurTs, void** ppPkVal,
                               int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pLastPkVal = NULL;
  int32_t lastPkLen = 0;
  if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
    SColumnInfoData* pPkColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->basic.primaryPkIndex);
    pLastPkVal = colDataGetData(pPkColDataInfo, pBlock->info.rows - 1);
    lastPkLen = colDataGetRowLength(pPkColDataInfo, pBlock->info.rows - 1);
  }

  code = pInfo->stateStore.streamStateGetAndSetTsData(pInfo->basic.pTsDataState, pBlock->info.id.uid, pCurTs, ppPkVal,
                                                      pBlock->info.window.ekey, pLastPkVal, lastPkLen, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t copyRecDataToBuff(TSKEY calStart, TSKEY calEnd, uint64_t uid, uint64_t version, EStreamType mode,
                          const SColumnInfoData* pPkColDataInfo, int32_t rowId, SRecDataInfo* pValueBuff,
                          int32_t buffLen) {
  pValueBuff->calWin.skey = calStart;
  pValueBuff->calWin.ekey = calEnd;
  pValueBuff->tableUid = uid;
  pValueBuff->dataVersion = version;
  pValueBuff->mode = mode;

  int32_t pkLen = 0;
  if (pPkColDataInfo != NULL) {
    pkLen = colDataGetRowLength(pPkColDataInfo, rowId);
    memcpy(pValueBuff->pPkColData, colDataGetData(pPkColDataInfo, rowId), pkLen);
  }
  return pkLen + sizeof(SRecDataInfo);
}

int32_t saveRecalculateData(SStateStore* pStateStore, STableTsDataState* pTsDataState, SSDataBlock* pSrcBlock,
                            EStreamType mode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcCalStartTsCol =
      (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcCalEndTsCol =
      (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pSrcGpCol = taosArrayGet(pSrcBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  TSKEY*           srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*           srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  TSKEY*           srcCalStartTsCol = (TSKEY*)pSrcCalStartTsCol->pData;
  TSKEY*           srcCalEndTsCol = (TSKEY*)pSrcCalEndTsCol->pData;
  uint64_t*        srcUidData = (uint64_t*)pSrcUidCol->pData;
  uint64_t*        srcGp = (uint64_t*)pSrcGpCol->pData;
  TSKEY            calStart = INT64_MIN;
  TSKEY            calEnd = INT64_MIN;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    SSessionKey key = {.win.skey = srcStartTsCol[i], .win.ekey = srcEndTsCol[i], .groupId = srcGp[i]};
    if (mode == STREAM_RETRIEVE) {
      calStart = srcCalStartTsCol[i];
      calEnd = srcCalEndTsCol[i];
    } else {
      calStart = srcStartTsCol[i];
      calEnd = srcEndTsCol[i];
    }
    int32_t len = copyRecDataToBuff(calStart, calEnd, srcUidData[i], pSrcBlock->info.version, mode, NULL, 0,
                                    pTsDataState->pRecValueBuff, pTsDataState->recValueLen);
    code = pStateStore->streamStateMergeAndSaveScanRange(pTsDataState, &key.win, key.groupId,
                                                         pTsDataState->pRecValueBuff, len);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void buildRecalculateDataSnapshort(SStreamScanInfo* pInfo, SExecTaskInfo* pTaskInfo) {
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pInfo->stateStore.streamStateGetInfo(pTaskInfo->streamInfo.pState, STREAM_DATA_SCAN_OP_REC_ID_NAME,
                                                     strlen(STREAM_DATA_SCAN_OP_REC_ID_NAME), &buff, &len);
  taosMemFreeClear(buff);
  if (res == TSDB_CODE_SUCCESS) {
    qDebug("===stream===%s recalculate task is not completed yet, so no need to create a recalculate snapshot", GET_TASKID(pTaskInfo));
    return;
  }

  int32_t recID = pInfo->stateStore.streamStateGetNumber(pInfo->basic.pTsDataState->pState);
  pInfo->stateStore.streamStateSaveInfo(pTaskInfo->streamInfo.pState, STREAM_DATA_SCAN_OP_REC_ID_NAME,
                                        strlen(STREAM_DATA_SCAN_OP_REC_ID_NAME), &recID, sizeof(int32_t));
  pInfo->stateStore.streamStateSetNumber(pInfo->basic.pTsDataState->pState, recID + 1, pInfo->primaryTsIndex);
  qDebug("===stream===%s build recalculate snapshot id:%d", GET_TASKID(pTaskInfo), recID);
}

static int32_t getRecalculateId(SStateStore* pStateStore, void* pState, int32_t* pRecId) {
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res = pStateStore->streamStateGetInfo(pState, STREAM_DATA_SCAN_OP_REC_ID_NAME,
                                                strlen(STREAM_DATA_SCAN_OP_REC_ID_NAME), &buff, &len);
  if (res != TSDB_CODE_SUCCESS) {
    qError("Not receive recalculate start block, but received recalculate end block");
    return res;
  }
  *(pRecId) = *(int32_t*)buff;
  taosMemFreeClear(buff);
  return TSDB_CODE_SUCCESS;
}

static int32_t deleteRecalculateDataSnapshort(SStreamScanInfo* pInfo, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   buff = NULL;
  int32_t len = 0;
  int32_t prevRecId = 0;
  code = getRecalculateId(&pInfo->stateStore, pTaskInfo->streamInfo.pState, &prevRecId);
  QUERY_CHECK_CODE(code, lino, _end);

  pInfo->stateStore.streamStateDeleteInfo(pTaskInfo->streamInfo.pState, STREAM_DATA_SCAN_OP_REC_ID_NAME,
                                          strlen(STREAM_DATA_SCAN_OP_REC_ID_NAME));

  int32_t curID = pInfo->stateStore.streamStateGetNumber(pInfo->basic.pTsDataState->pState);
  pInfo->stateStore.streamStateSetNumber(pInfo->basic.pTsDataState->pState, prevRecId, pInfo->primaryTsIndex);
  pInfo->stateStore.streamStateSessionDeleteAll(pInfo->basic.pTsDataState->pState);

  pInfo->stateStore.streamStateSetNumber(pInfo->basic.pTsDataState->pState, curID, pInfo->primaryTsIndex);
  qDebug("===stream===%s delete recalculate snapshot id:%d", GET_TASKID(pTaskInfo), prevRecId);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t readPreVersionDataBlock(uint64_t uid, TSKEY startTs, TSKEY endTs, int64_t version, char* taskIdStr,
                                       SStreamScanInfo* pInfo, SSDataBlock* pBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uid, startTs, endTs, version);
  QUERY_CHECK_NULL(pPreRes, code, lino, _end, terrno);

  printDataBlock(pPreRes, "pre res", taskIdStr);
  code = blockDataEnsureCapacity(pBlock, pPreRes->info.rows + pBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData* pTsCol = (SColumnInfoData*)taosArrayGet(pPreRes->pDataBlock, pInfo->primaryTsIndex);
  SColumnInfoData* pPkCol = NULL;
  if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
    pPkCol = (SColumnInfoData*)taosArrayGet(pPreRes->pDataBlock, pInfo->basic.primaryPkIndex);
  }
  for (int32_t i = 0; i < pPreRes->info.rows; i++) {
    uint64_t groupId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, i);
    code = appendPkToSpecialBlock(pBlock, (TSKEY*)pTsCol->pData, pPkCol, i, &uid, &groupId, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  printDataBlock(pBlock, "new delete", taskIdStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t readPrevVersionDataByBlock(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock, char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);

  uint64_t* srcUidData = (uint64_t*)pSrcUidCol->pData;
  TSKEY*    srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*    srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  int64_t   ver = pSrcBlock->info.version - 1;
  blockDataCleanup(pDestBlock);
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    code = readPreVersionDataBlock(srcUidData[i], srcStartTsCol[i], srcEndTsCol[i], ver, pTaskIdStr, pInfo,
                                   pDestBlock);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamBlockScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("===stream===%s doStreamBlockScan", GET_TASKID(pTaskInfo));
  size_t total = taosArrayGetSize(pInfo->pBlockLists);
  while (1) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      (*ppRes) = NULL;
      break;
    }

    int32_t current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t)total, GET_TASKID(pTaskInfo));

    SPackedData* pPacked = taosArrayGet(pInfo->pBlockLists, current);
    QUERY_CHECK_NULL(pPacked, code, lino, _end, terrno);

    SSDataBlock* pBlock = pPacked->pDataBlock;
    if (pBlock->info.parTbName[0]) {
      code =
          pInfo->stateStore.streamStatePutParName(pStreamInfo->pState, pBlock->info.id.groupId, pBlock->info.parTbName);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;

    code = blockDataUpdateTsWindow(pBlock, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_INVALID:
      case STREAM_GET_ALL: 
      case STREAM_RECALCULATE_DATA: 
      case STREAM_RECALCULATE_DELETE:
      case STREAM_CREATE_CHILD_TABLE: {
        setStreamOperatorState(&pInfo->basic, pBlock->info.type);
        (*ppRes) = pBlock;
      } break;
      case STREAM_DELETE_DATA: {
        printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "delete recv", GET_TASKID(pTaskInfo));
        if (pInfo->tqReader) {
          code = filterDelBlockByUid(pInfo->pDeleteDataRes, pBlock, pInfo->tqReader, &pInfo->readerFn);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          // its parent operator is final agg operator
          code = copyDataBlock(pInfo->pDeleteDataRes, pBlock);
          QUERY_CHECK_CODE(code, lino, _end);
          pInfo->pDeleteDataRes->info.type = STREAM_RECALCULATE_DATA;
        }

        code = setBlockGroupIdByUid(pInfo, pInfo->pDeleteDataRes);
        QUERY_CHECK_CODE(code, lino, _end);
        code = rebuildDeleteBlockData(pInfo->pDeleteDataRes, &pStreamInfo->fillHistoryWindow, GET_TASKID(pTaskInfo));
        QUERY_CHECK_CODE(code, lino, _end);
        if (pInfo->pDeleteDataRes->info.rows == 0) {
          continue;
        }
        printSpecDataBlock(pInfo->pDeleteDataRes, getStreamOpName(pOperator->operatorType), "delete recv filtered",
                           GET_TASKID(pTaskInfo));
        if (pInfo->partitionSup.needCalc) {
          SSDataBlock* pTmpBlock = NULL;
          code = createOneDataBlock(pInfo->pDeleteDataRes, true, &pTmpBlock);
          QUERY_CHECK_CODE(code, lino, _end);
          readPrevVersionDataByBlock(pInfo, pTmpBlock, pInfo->pDeleteDataRes, GET_TASKID(pTaskInfo));
          blockDataDestroy(pTmpBlock);
        }
        pInfo->pDeleteDataRes->info.type = STREAM_RECALCULATE_DELETE;
        (*ppRes) = pInfo->pDeleteDataRes;
      } break;
      case STREAM_DROP_CHILD_TABLE: {
        int32_t deleteNum = 0;
        code = deletePartName(&pInfo->stateStore, pTaskInfo->streamInfo.pState, pBlock, &deleteNum);
        QUERY_CHECK_CODE(code, lino, _end);
        if (deleteNum == 0) {
          continue;
        }
        (*ppRes) = pBlock;
      } break;
      case STREAM_CHECKPOINT: {
        qError("stream check point error. msg type: STREAM_INPUT__DATA_BLOCK");
      } break;
      case STREAM_RECALCULATE_START: {
        if (!isSemiOperator(&pInfo->basic)) {
          code = pInfo->stateStore.streamStateFlushReaminInfoToDisk(pInfo->basic.pTsDataState);
          QUERY_CHECK_CODE(code, lino, _end);
          buildRecalculateDataSnapshort(pInfo, pTaskInfo);
        }
        continue;
      } break;
      case STREAM_RECALCULATE_END: {
        if (isRecalculateOperator(&pInfo->basic)) {
          qError("stream recalculate error since recalculate operator receive STREAM_RECALCULATE_END");
          continue;
        }
        code = deleteRecalculateDataSnapshort(pInfo, pTaskInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        continue;
      } break;
      default:
        break;
    }
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);
    break;
  }

_end:
  printDataBlock((*ppRes), getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    (*ppRes) = NULL;
  }
  return code;
}

#ifdef BUILD_NO_CALL
static int32_t buildAndSaveRecalculateData(SSDataBlock* pSrcBlock, TSKEY* pTsCol, SColumnInfoData* pPkColDataInfo, int32_t num,
                                    SPartitionBySupporter* pParSup, SExprSupp* pPartScalarSup, SStateStore* pStateStore,
                                    STableTsDataState* pTsDataState, SSDataBlock* pDestBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t len = 0;
  if (pParSup->needCalc) {
    blockDataEnsureCapacity(pDestBlock, num * 2);
  } else {
    blockDataEnsureCapacity(pDestBlock, num);
  }

  for (int32_t rowId = 0; rowId < num; rowId++) {
    len = copyRecDataToBuff(pTsCol[rowId], pTsCol[rowId], pSrcBlock->info.id.uid, pSrcBlock->info.version, STREAM_CLEAR,
                            NULL, 0, pTsDataState->pRecValueBuff, pTsDataState->recValueLen);
    SSessionKey key = {.win.skey = pTsCol[rowId], .win.ekey = pTsCol[rowId], .groupId = 0};
    code = pStateStore->streamState1SessionSaveToDisk(pTsDataState, &key, pTsDataState->pRecValueBuff, len);
    QUERY_CHECK_CODE(code, lino, _end);
    uint64_t gpId = 0;
    code = appendPkToSpecialBlock(pDestBlock, pTsCol, pPkColDataInfo, rowId, &pSrcBlock->info.id.uid, &gpId, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pParSup->needCalc) {
      key.groupId = calGroupIdByData(pParSup, pPartScalarSup, pSrcBlock, rowId);
      len = copyRecDataToBuff(pTsCol[rowId], pTsCol[rowId], pSrcBlock->info.id.uid, pSrcBlock->info.version,
                              STREAM_DELETE_DATA, NULL, 0, pTsDataState->pRecValueBuff,
                              pTsDataState->recValueLen);
      code = pStateStore->streamState1SessionSaveToDisk(pTsDataState, &key, pTsDataState->pRecValueBuff, len);
      QUERY_CHECK_CODE(code, lino, _end);

      code = appendPkToSpecialBlock(pDestBlock, pTsCol, pPkColDataInfo, rowId, &pSrcBlock->info.id.uid, &gpId, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
#endif

static uint64_t getCurDataGroupId(SPartitionBySupporter* pParSup, SExprSupp* pPartScalarSup, SSDataBlock* pSrcBlock, int32_t rowId) {
  if (pParSup->needCalc) {
    return calGroupIdByData(pParSup, pPartScalarSup, pSrcBlock, rowId);
  }

  return pSrcBlock->info.id.groupId;
}

static uint64_t getDataGroupIdByCol(SSteamOpBasicInfo* pBasic, SOperatorInfo* pTableScanOp,
                                    SPartitionBySupporter* pParSup, SExprSupp* pPartScalarSup, uint64_t uid, TSKEY ts,
                                    int64_t maxVersion, void* pVal, bool* pRes) {
  SSDataBlock* pPreRes = readPreVersionData(pTableScanOp, uid, ts, ts, maxVersion);
  if (!pPreRes || pPreRes->info.rows == 0) {
    if (terrno != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    }
    (*pRes) = false;
    return 0;
  }

  int32_t rowId = 0;
  if (hasSrcPrimaryKeyCol(pBasic)) {
    SColumnInfoData* pPkCol = taosArrayGet(pPreRes->pDataBlock, pBasic->primaryPkIndex);
    for (; rowId < pPreRes->info.rows; rowId++) {
      if (comparePrimaryKey(pPkCol, rowId, pVal)) {
        break;
      }
    }
  }
  if (rowId >= pPreRes->info.rows) {
    qInfo("===stream===read preversion data of primary key failed. ts:%" PRId64 ",version:%" PRId64, ts, maxVersion);
    (*pRes) = false;
    return 0;
  }
  (*pRes) = true;
  return calGroupIdByData(pParSup, pPartScalarSup, pPreRes, rowId);
}

static int32_t buildRecalculateData(SSteamOpBasicInfo* pBasic, SOperatorInfo* pTableScanOp,
                                    SPartitionBySupporter* pParSup, SExprSupp* pPartScalarSup, SSDataBlock* pSrcBlock,
                                    TSKEY* pTsCol, SColumnInfoData* pPkColDataInfo, SSDataBlock* pDestBlock,
                                    int32_t num) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  blockDataEnsureCapacity(pDestBlock, num * 2);
  for (int32_t rowId = 0; rowId < num; rowId++) {
    uint64_t gpId = getCurDataGroupId(pParSup, pPartScalarSup, pSrcBlock, rowId);
    code = appendPkToSpecialBlock(pDestBlock, pTsCol, pPkColDataInfo, rowId, &pSrcBlock->info.id.uid, &gpId, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pParSup->needCalc) {
      bool  res = false;
      void* pVal = NULL;
      if (hasSrcPrimaryKeyCol(pBasic)) {
        pVal = colDataGetData(pPkColDataInfo, rowId);
      }
      gpId = getDataGroupIdByCol(pBasic, pTableScanOp, pParSup, pPartScalarSup, pSrcBlock->info.id.uid, pTsCol[rowId],
                                 pSrcBlock->info.version - 1, pVal, &res);
      if (res == true) {
        code = appendPkToSpecialBlock(pDestBlock, pTsCol, pPkColDataInfo, rowId, &pSrcBlock->info.id.uid, &gpId, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamWALScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;
  int32_t          totalBlocks = taosArrayGetSize(pInfo->pBlockLists);

  switch (pInfo->scanMode) {
    case STREAM_SCAN_FROM_RES: {
      if (pInfo->pUpdateRes->info.rows > 0) {
        pInfo->scanMode = STREAM_SCAN_FROM_UPDATERES;
      } else {
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      }
      (*ppRes) = pInfo->pRes;
      goto _end;
    } break;
    case STREAM_SCAN_FROM_UPDATERES: {
      (*ppRes) = pInfo->pUpdateRes;
      pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      goto _end;
    } break;
    default:
      (*ppRes) = NULL;
      break;
  }

  while (1) {
    if (pInfo->readerFn.tqReaderCurrentBlockConsumed(pInfo->tqReader)) {
      if (pInfo->validBlockIndex >= totalBlocks) {
        doClearBufferedBlocks(pInfo);

        qDebug("stream scan return empty, all %d submit blocks consumed, %s", totalBlocks, GET_TASKID(pTaskInfo));
        (*ppRes) = NULL;
        goto _end;
      }

      int32_t      current = pInfo->validBlockIndex++;
      SPackedData* pSubmit = taosArrayGet(pInfo->pBlockLists, current);
      QUERY_CHECK_NULL(pSubmit, code, lino, _end, terrno);

      qDebug("set %d/%d as the input submit block, %s", current + 1, totalBlocks, GET_TASKID(pTaskInfo));
      if (pAPI->tqReaderFn.tqReaderSetSubmitMsg(pInfo->tqReader, pSubmit->msgStr, pSubmit->msgLen, pSubmit->ver, NULL) < 0) {
        qError("submit msg messed up when initializing stream submit block %p, current %d/%d, %s", pSubmit, current,
               totalBlocks, GET_TASKID(pTaskInfo));
        continue;
      }
    }

    blockDataCleanup(pInfo->pRes);

    while (pAPI->tqReaderFn.tqNextBlockImpl(pInfo->tqReader, GET_TASKID(pTaskInfo))) {
      SSDataBlock* pRes = NULL;

      code = pAPI->tqReaderFn.tqRetrieveBlock(pInfo->tqReader, &pRes, GET_TASKID(pTaskInfo));
      qDebug("retrieve data from submit completed code:%s rows:%" PRId64 " %s", tstrerror(code), pRes->info.rows,
             GET_TASKID(pTaskInfo));

      if (code != TSDB_CODE_SUCCESS || pRes->info.rows == 0) {
        qDebug("retrieve data failed, try next block in submit block, %s", GET_TASKID(pTaskInfo));
        continue;
      }

      code = setBlockIntoRes(pInfo, pRes, &pStreamInfo->fillHistoryWindow, false);
      if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        pInfo->pRes->info.rows = 0;
        code = TSDB_CODE_SUCCESS;
      }
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pRes->info.rows == 0) {
        continue;
      }

      setStreamOperatorState(&pInfo->basic, pInfo->pRes->info.type);
      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
      QUERY_CHECK_CODE(code, lino, _end);

      TSKEY   curTs = INT64_MIN;
      void*   pPkVal = NULL;
      int32_t winCode = TSDB_CODE_FAILED;
      code = getMaxTsKeyInfo(pInfo, pInfo->pRes, &curTs, &pPkVal, &winCode);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInfo->pUpdateRes->info.rows > 0) {
        blockDataCleanup(pInfo->pUpdateRes);
      }

      SColumnInfoData* pPkColDataInfo = NULL;
      if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
        pPkColDataInfo = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->basic.primaryPkIndex);
      }
      SColumnInfoData* pTsCol = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->primaryTsIndex);
      if (winCode == TSDB_CODE_SUCCESS && curTs >= pInfo->pRes->info.window.skey) {
        int32_t num = 0;
        if (curTs < pInfo->pRes->info.window.ekey) {
          num = getForwardStepsInBlock(pInfo->pRes->info.rows, binarySearchForKey, curTs, 0, TSDB_ORDER_ASC,
                                       (TSKEY*)pTsCol->pData);
          if (hasSrcPrimaryKeyCol(&pInfo->basic)) {
            for (; num >= 0; num--) {
              void* pColPkData = colDataGetData(pPkColDataInfo, num);
              if (pInfo->comparePkColFn(pColPkData, pPkVal) <= 0) {
                break;
              }
            }
          }
        } else {
          num = pInfo->pRes->info.rows;
        }

        if (num > 0) {
          qInfo("%s stream scan op ignore disorder data. rows:%d, tableUid:%" PRId64 ", last max ts:%" PRId64
                ", block start key:%" PRId64 ", end key:%" PRId64,
                GET_TASKID(pTaskInfo), num, pInfo->pRes->info.id.uid, curTs, pInfo->pRes->info.window.skey,
                pInfo->pRes->info.window.ekey);
          code = buildRecalculateData(&pInfo->basic, pInfo->pTableScanOp, &pInfo->partitionSup, pInfo->pPartScalarSup,
                                      pInfo->pRes, (TSKEY*)pTsCol->pData, pPkColDataInfo, pInfo->pUpdateRes, num);
          QUERY_CHECK_CODE(code, lino, _end);
          code = blockDataTrimFirstRows(pInfo->pRes, num);
          QUERY_CHECK_CODE(code, lino, _end);
          code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (pInfo->pCreateTbRes->info.rows > 0) {
        if (pInfo->pRes->info.rows > 0) {
          pInfo->scanMode = STREAM_SCAN_FROM_RES;
        } else if (pInfo->pUpdateRes->info.rows > 0) {
          pInfo->scanMode = STREAM_SCAN_FROM_UPDATERES;
        }
        qDebug("create table res exists, rows:%" PRId64 " return from stream scan, %s", pInfo->pCreateTbRes->info.rows,
               GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pCreateTbRes;
        break;
      }

      if (pInfo->pRes->info.rows > 0) {
        if (pInfo->pUpdateRes->info.rows > 0) {
          pInfo->scanMode = STREAM_SCAN_FROM_UPDATERES;
        }
        (*ppRes) = pInfo->pRes;
        break;
      }

      if (pInfo->pUpdateRes->info.rows > 0) {
        (*ppRes) = pInfo->pUpdateRes;
        break;
      }
    }

    if ((*ppRes) != NULL && (*ppRes)->info.rows > 0) {
      break;
    }
  }

_end:
  printDataBlock(*ppRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    (*ppRes) = NULL;
  }
  return code;
}

void streamDataScanOperatorSaveCheckpoint(SStreamScanInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (needSaveStreamOperatorInfo(&pInfo->basic)) {
    pInfo->stateStore.streamStateTsDataCommit(pInfo->basic.pTsDataState);
    saveStreamOperatorStateComplete(&pInfo->basic);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

int32_t doStreamDataScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  const char*      id = GET_TASKID(pTaskInfo);
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("stream data scan started, %s", id);

  if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE1 ||
      pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE2) {
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    memcpy(&pTSInfo->base.cond, &pStreamInfo->tableCond, sizeof(SQueryTableDataCond));

    if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE1) {
      pTSInfo->base.cond.startVersion = pStreamInfo->fillHistoryVer.minVer;
      pTSInfo->base.cond.endVersion = pStreamInfo->fillHistoryVer.maxVer;

      pTSInfo->base.cond.twindows = pStreamInfo->fillHistoryWindow;
      qDebug("stream scan step1, verRange:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64 ", %s",
             pTSInfo->base.cond.startVersion, pTSInfo->base.cond.endVersion, pTSInfo->base.cond.twindows.skey,
             pTSInfo->base.cond.twindows.ekey, id);
      pStreamInfo->recoverStep = STREAM_RECOVER_STEP__SCAN1;
      pStreamInfo->recoverScanFinished = false;
    } else {
      pTSInfo->base.cond.startVersion = pStreamInfo->fillHistoryVer.minVer;
      pTSInfo->base.cond.endVersion = pStreamInfo->fillHistoryVer.maxVer;
      pTSInfo->base.cond.twindows = pStreamInfo->fillHistoryWindow;
      qDebug("stream scan step2 (scan wal), verRange:%" PRId64 " - %" PRId64 ", window:%" PRId64 "-%" PRId64 ", %s",
             pTSInfo->base.cond.startVersion, pTSInfo->base.cond.endVersion, pTSInfo->base.cond.twindows.skey,
             pTSInfo->base.cond.twindows.ekey, id);
      pStreamInfo->recoverStep = STREAM_RECOVER_STEP__NONE;
    }

    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);

    pTSInfo->base.dataReader = NULL;
    pInfo->pTableScanOp->status = OP_OPENED;

    pTSInfo->scanTimes = 0;
    pTSInfo->currentGroupId = -1;
  }

  if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__SCAN1) {
    if (isTaskKilled(pTaskInfo)) {
      qInfo("===stream===stream scan is killed. task id:%s, code %s", id, tstrerror(pTaskInfo->code));
      (*ppRes) = NULL;
      return code;
    }

    if (pInfo->scanMode == STREAM_SCAN_FROM_RES) {
      pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      (*ppRes) = pInfo->pRecoverRes;
      return code;
    }

    while (1) {
      code = doTableScanNext(pInfo->pTableScanOp, &pInfo->pRecoverRes);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInfo->pRecoverRes == NULL) {
        break;
      }
      setStreamOperatorState(&pInfo->basic, pInfo->pRecoverRes->info.type);
      code = doFilter(pInfo->pRecoverRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pRecoverRes->info.rows <= 0) {
        continue;
      }

      TSKEY   curTs = INT64_MIN;
      void*   pPkVal = NULL;
      int32_t winCode = TSDB_CODE_FAILED;
      code = getMaxTsKeyInfo(pInfo, pInfo->pRecoverRes, &curTs, &pPkVal, &winCode);
      QUERY_CHECK_CODE(code, lino, _end);

      code = calBlockTbName(pInfo, pInfo->pRecoverRes, 0);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pCreateTbRes->info.rows > 0) {
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
        printSpecDataBlock(pInfo->pCreateTbRes, getStreamOpName(pOperator->operatorType), "recover",
                           GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pCreateTbRes;
        return code;
      }

      qDebug("stream recover scan get block, rows %" PRId64, pInfo->pRecoverRes->info.rows);
      printSpecDataBlock(pInfo->pRecoverRes, getStreamOpName(pOperator->operatorType), "recover",
                         GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pRecoverRes;
      return code;
    }
    pStreamInfo->recoverStep = STREAM_RECOVER_STEP__NONE;
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);
    pTSInfo->base.dataReader = NULL;
    pTSInfo->base.cond.startVersion = -1;
    pTSInfo->base.cond.endVersion = -1;
    pStreamInfo->recoverScanFinished = true;
    (*ppRes) = NULL;
    qDebug("===stream===%s fill history is finished.", GET_TASKID(pTaskInfo));
    return code;
  }

  size_t total = taosArrayGetSize(pInfo->pBlockLists);

  switch (pInfo->blockType) {
    case STREAM_INPUT__DATA_BLOCK: {
      doStreamBlockScan(pOperator, ppRes);
    } break;
    case STREAM_INPUT__DATA_SUBMIT: {
      doStreamWALScan(pOperator, ppRes);
    } break;
    case STREAM_INPUT__CHECKPOINT:
    case STREAM_INPUT__RECALCULATE: {
      if (pInfo->validBlockIndex >= total) {
        doClearBufferedBlocks(pInfo);
        (*ppRes) = NULL;
        return code;
      }

      int32_t current = pInfo->validBlockIndex++;
      qDebug("process %d/%d input data blocks, %s", current, (int32_t)total, id);

      SPackedData* pData = taosArrayGet(pInfo->pBlockLists, current);
      QUERY_CHECK_NULL(pData, code, lino, _end, terrno);
      SSDataBlock* pBlock = taosArrayGet(pData->pDataBlock, 0);
      QUERY_CHECK_NULL(pBlock, code, lino, _end, terrno);
      printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

      if (pBlock->info.type == STREAM_CHECKPOINT) {
        code = pInfo->stateStore.streamStateFlushReaminInfoToDisk(pInfo->basic.pTsDataState);
        QUERY_CHECK_CODE(code, lino, _end);
        streamDataScanOperatorSaveCheckpoint(pInfo);
        (*ppRes) = pInfo->pCheckpointRes;
      } else if (pBlock->info.type == STREAM_RECALCULATE_START) {
        if (!isSemiOperator(&pInfo->basic)) {
          code = pInfo->stateStore.streamStateFlushReaminInfoToDisk(pInfo->basic.pTsDataState);
          QUERY_CHECK_CODE(code, lino, _end);
          buildRecalculateDataSnapshort(pInfo, pTaskInfo);
        }
      } else if (pBlock->info.type == STREAM_RECALCULATE_END) {
        if (isRecalculateOperator(&pInfo->basic)) {
          qError("stream recalculate error since recalculate operator receive STREAM_RECALCULATE_END");
        } else {
          code = deleteRecalculateDataSnapshort(pInfo, pTaskInfo);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      (*ppRes) = pBlock;
      return code;
    } break;
    default: {
      qError("stream scan error, invalid block type %d, %s", pInfo->blockType, id);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    } break;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    (*ppRes) = NULL;
  }
  return code;
}

void streamDataScanReleaseState(SOperatorInfo* pOperator) {
  SStreamScanInfo* pInfo = pOperator->info;
  pInfo->stateStore.streamStateTsDataCommit(pInfo->basic.pTsDataState);
}

void streamDataScanReloadState(SOperatorInfo* pOperator) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamScanInfo* pInfo = pOperator->info;
  code = pInfo->stateStore.streamStateReloadTsDataState(pInfo->basic.pTsDataState);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static uint64_t getDataGroupId(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion, void* pVal,
                               bool* pRes) {
  if (pInfo->partitionSup.needCalc) {
    return getDataGroupIdByCol(&pInfo->basic, pInfo->pTableScanOp, &pInfo->partitionSup, pInfo->pPartScalarSup, uid, ts,
                               maxVersion, pVal, pRes);
  }

  *pRes = true;
  STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
  return tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, uid);
}

static int32_t generateSessionDataScanRange(SStreamScanInfo* pInfo, SSHashObj* pRecRangeMap, SArray* pSrcRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t size =  taosArrayGetSize(pSrcRange);
  for (int32_t i = 0; i < size; i++) {
    SArray* pRange = taosArrayGetP(pSrcRange, i);
    uint64_t      groupId = *(uint64_t*) taosArrayGet(pRange, 2);
    STimeWindow   resWin = {0};
    resWin.skey = *(TSKEY*) taosArrayGet(pRange, 3);
    resWin.ekey = *(TSKEY*) taosArrayGet(pRange, 4);

    SSessionKey key = {.groupId = groupId};
    key.win.skey = *(TSKEY*) taosArrayGet(pRange, 0);
    key.win.ekey = *(TSKEY*) taosArrayGet(pRange, 1);
    void* pVal = tSimpleHashGet(pRecRangeMap, &key, sizeof(SSessionKey));
    QUERY_CHECK_NULL(pVal, code, lino, _end, TSDB_CODE_FAILED);
    SRecDataInfo* pRecData = *(void**)pVal;

    code = pInfo->stateStore.streamStateMergeAndSaveScanRange(pInfo->basic.pTsDataState, &resWin, groupId, pRecData,
                                                              pInfo->basic.pTsDataState->recValueLen);
    QUERY_CHECK_CODE(code, lino, _end);
    taosArrayDestroy(pRange);
  }
  taosArrayClear(pSrcRange);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t generateSessionScanRange(SStreamScanInfo* pInfo, char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SSessionKey      firstKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  SStreamStateCur* pCur =
      pInfo->stateStore.streamStateSessionSeekKeyCurrentNext(pInfo->basic.pTsDataState->pStreamTaskState, &firstKey);
  while (1) {
    SSessionKey rangKey = {0};
    void*       pVal = NULL;
    int32_t     len = 0;
    int32_t     winRes = pInfo->stateStore.streamStateSessionGetKVByCur(pCur, &rangKey, &pVal, &len);
    if (winRes != TSDB_CODE_SUCCESS) {
      break;
    }
    qDebug("===stream===%s get range from disk. start ts:%" PRId64 ",end ts:%" PRId64 ", group id:%" PRIu64, pTaskIdStr,
           rangKey.win.skey, rangKey.win.ekey, rangKey.groupId);
    code = tSimpleHashPut(pInfo->pRecRangeMap, &rangKey, sizeof(SSessionKey), &pVal, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);

    if (tSimpleHashGetSize(pInfo->pRecRangeMap) > 1024) {
      code = streamClientGetResultRange(&pInfo->recParam, pInfo->pRecRangeMap, pInfo->pRecRangeRes);
      QUERY_CHECK_CODE(code, lino, _end);
      code = generateSessionDataScanRange(pInfo, pInfo->pRecRangeMap, pInfo->pRecRangeRes);
      QUERY_CHECK_CODE(code, lino, _end);
      tSimpleHashClear(pInfo->pRecRangeMap);
    }
    pInfo->stateStore.streamStateCurNext(pInfo->basic.pTsDataState->pStreamTaskState, pCur);
  }
  pInfo->stateStore.streamStateFreeCur(pCur);

  if (tSimpleHashGetSize(pInfo->pRecRangeMap) > 0) {
    code = streamClientGetResultRange(&pInfo->recParam, pInfo->pRecRangeMap, pInfo->pRecRangeRes);
    QUERY_CHECK_CODE(code, lino, _end);
    code = generateSessionDataScanRange(pInfo, pInfo->pRecRangeMap, pInfo->pRecRangeRes);
    QUERY_CHECK_CODE(code, lino, _end);
    tSimpleHashClear(pInfo->pRecRangeMap);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t generateIntervalDataScanRange(SStreamScanInfo* pInfo, char* pTaskIdStr, SSessionKey* pSeKey,
                                             SRecDataInfo* pRecData, int32_t len) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  int32_t        rowId = 0;
  SDataBlockInfo tmpInfo = {0};
  tmpInfo.rows = 1;
  STimeWindow win = getSlidingWindow(&pSeKey->win.skey, &pSeKey->win.ekey, &pSeKey->groupId, &pInfo->interval, &tmpInfo,
                                     &rowId, pInfo->partitionSup.needCalc);
  pInfo->stateStore.streamStateMergeAndSaveScanRange(pInfo->basic.pTsDataState, &win, pSeKey->groupId, pRecData, len);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t generateIntervalScanRange(SStreamScanInfo* pInfo, char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SSessionKey      firstKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  SStreamStateCur* pCur =
      pInfo->stateStore.streamStateSessionSeekKeyCurrentNext(pInfo->basic.pTsDataState->pStreamTaskState, &firstKey);
  while (1) {
    SSessionKey rangKey = {0};
    void*       pVal = NULL;
    int32_t     len = 0;
    int32_t     winRes = pInfo->stateStore.streamStateSessionGetKVByCur(pCur, &rangKey, &pVal, &len);
    if (winRes != TSDB_CODE_SUCCESS) {
      break;
    }
    qDebug("===stream===%s get range from disk. start ts:%" PRId64 ",end ts:%" PRId64 ", group id:%" PRIu64,
           pTaskIdStr, rangKey.win.skey, rangKey.win.ekey, rangKey.groupId);
    code = generateIntervalDataScanRange(pInfo, pTaskIdStr, &rangKey, (SRecDataInfo*)pVal, len);
    QUERY_CHECK_CODE(code, lino, _end);
    taosMemFreeClear(pVal);
    pInfo->stateStore.streamStateCurNext(pInfo->basic.pTsDataState->pStreamTaskState, pCur);
  }
  pInfo->stateStore.streamStateFreeCur(pCur);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t generateDataScanRange(SStreamScanInfo* pInfo, char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  switch (pInfo->windowSup.parentType) {
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_FINAL_INTERVAL: {
      code = generateIntervalScanRange(pInfo, pTaskIdStr);
      QUERY_CHECK_CODE(code, lino, _end);
    } break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_FINAL_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_EVENT: {
      code = generateSessionScanRange(pInfo, pTaskIdStr);
      QUERY_CHECK_CODE(code, lino, _end);
    } break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_CONTINUE_COUNT: {

    } break;
    default:
      break;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doOneRangeScan(SStreamScanInfo* pInfo, SScanRange* pRange, SSDataBlock** ppRes) {
  qDebug("do stream recalculate scan.");

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SOperatorInfo* pScanOp = NULL;
  if (pInfo->scanAllTables) {
    pScanOp = pInfo->pTableScanOp;
  } else {
    pScanOp = pInfo->pRecTableScanOp;
  }

  while (1) {
    SSDataBlock* pResult = NULL;
    code = doTableScanNext(pScanOp, &pResult);
    QUERY_CHECK_CODE(code, lino, _end);

    STableScanInfo* pTableScanInfo = pScanOp->info;
    if (pResult == NULL) {
      pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
      pTableScanInfo->base.dataReader = NULL;
      (*ppRes) = NULL;
      goto _end;
    }

    code = doFilter(pResult, pScanOp->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pResult->info.rows == 0) {
      continue;
    }

    printDataBlock(pResult, "tsdb", GET_TASKID(pScanOp->pTaskInfo));
    if (!pInfo->assignBlockUid) {
      pResult->info.id.groupId = 0;
    }

    if (pInfo->partitionSup.needCalc) {
      SSDataBlock* tmpBlock = NULL;
      code = createOneDataBlock(pResult, true, &tmpBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(pResult);
      for (int32_t i = 0; i < tmpBlock->info.rows; i++) {
        uint64_t dataGroupId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, tmpBlock, i);
        if (tSimpleHashGet(pRange->pGroupIds, &dataGroupId, sizeof(uint64_t)) != NULL) {
          for (int32_t j = 0; j < pScanOp->exprSupp.numOfExprs; j++) {
            SColumnInfoData* pSrcCol = taosArrayGet(tmpBlock->pDataBlock, j);
            SColumnInfoData* pDestCol = taosArrayGet(pResult->pDataBlock, j);
            bool             isNull = colDataIsNull(pSrcCol, tmpBlock->info.rows, i, NULL);
            char*            pSrcData = NULL;
            if (!isNull) pSrcData = colDataGetData(pSrcCol, i);
            code = colDataSetVal(pDestCol, pResult->info.rows, pSrcData, isNull);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          pResult->info.rows++;
        }
      }

      blockDataDestroy(tmpBlock);

      if (pResult->info.rows > 0) {
        pResult->info.calWin = pRange->calWin;
        (*ppRes) = pResult;
        goto _end;
      }
    } else {
      if (tSimpleHashGet(pRange->pGroupIds, &pResult->info.id.groupId, sizeof(uint64_t)) != NULL) {
        pResult->info.calWin = pRange->calWin;
        (*ppRes) = pResult;
        goto _end;
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// static void exchangeTableListInfo(SStreamScanInfo* pInfo, SOperatorInfo* pScanOp) {
//   STableScanInfo* pScanInfo = (STableScanInfo*)pScanOp->info;
//   STableListInfo* pTemp = pScanInfo->base.pTableListInfo;
//   pScanInfo->base.pTableListInfo = pInfo->pRecTableListInfo;
//   pInfo->pRecTableListInfo = pTemp;
// }

static int32_t prepareDataRangeScan(SStreamScanInfo* pInfo, SScanRange* pRange) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SOperatorParam*          pOpParam = NULL;
  STableScanOperatorParam* pTableScanParam = NULL;

  qDebug("prepare data range scan start:%" PRId64 ",end:%" PRId64 ",is all table:%d", pRange->win.skey,
         pRange->win.ekey, pInfo->scanAllTables);
  SOperatorInfo* pScanOp = NULL;
  if (pInfo->scanAllTables) {
    pScanOp = pInfo->pTableScanOp;
  } else {
    pScanOp = pInfo->pRecTableScanOp;
  }

  resetTableScanInfo(pScanOp->info, &pRange->win, -1);
  pScanOp->status = OP_OPENED;

  if (pInfo->scanAllTables == true) {
    goto _end;
  }

  pOpParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pOpParam, code, lino, _end, terrno);
  pOpParam->downstreamIdx = 0;
  pOpParam->opType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOpParam->pChildren = NULL;

  pTableScanParam = taosMemoryCalloc(1, sizeof(STableScanOperatorParam));
  QUERY_CHECK_NULL(pTableScanParam, code, lino, _end, terrno);
  pOpParam->value = pTableScanParam;
  pTableScanParam->tableSeq = false;
  int32_t size = tSimpleHashGetSize(pRange->pUIds);
  pTableScanParam->pUidList = taosArrayInit(size, sizeof(uint64_t));
  QUERY_CHECK_NULL(pTableScanParam->pUidList, code, lino, _end, terrno);

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pRange->pUIds, pIte, &iter)) != NULL) {
    void* pTempUid = tSimpleHashGetKey(pIte, NULL);
    QUERY_CHECK_NULL(pTempUid, code, lino, _end, terrno);
    void* pTemRes = taosArrayPush(pTableScanParam->pUidList, pTempUid);
    QUERY_CHECK_NULL(pTemRes, code, lino, _end, terrno);
    qDebug("prepare data range add table uid:%" PRIu64, *(uint64_t*)pTempUid);
  }
  pScanOp->pOperatorGetParam = pOpParam;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pOpParam);
    if (pTableScanParam != NULL) {
      if (pTableScanParam->pUidList != NULL) {
        taosArrayDestroy(pTableScanParam->pUidList);
      }
      taosMemoryFree(pTableScanParam);
    }
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void destroyScanRange(SScanRange* pRange) {
  pRange->win.skey = INT64_MIN;
  pRange->win.ekey = INT64_MIN;
  tSimpleHashCleanup(pRange->pUIds);
  pRange->pUIds = NULL;
  tSimpleHashCleanup(pRange->pGroupIds);
  pRange->pGroupIds = NULL;
}

static int32_t buildRecBlockByRange(SScanRange* pRange, SSDataBlock* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = tSimpleHashGetSize(pRange->pGroupIds);
  code = blockDataEnsureCapacity(pRes, size);
  QUERY_CHECK_CODE(code, lino, _end);

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pRange->pGroupIds, pIte, &iter)) != NULL) {
    uint64_t* pGroupId = (uint64_t*)tSimpleHashGetKey(pIte, NULL);
    code = appendOneRowToSpecialBlockImpl(pRes, &pRange->win.skey, &pRange->win.ekey, &pRange->calWin.skey,
                                          &pRange->calWin.ekey, NULL, pGroupId, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pRes->info.type = STREAM_RECALCULATE_DATA;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doDataRangeScan(SStreamScanInfo* pInfo, SExecTaskInfo* pTaskInfo, SSDataBlock** ppRes) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pTsdbBlock = NULL;
  pInfo->pRecoverRes = NULL;
  while (1) {
    if (IS_INVALID_RANGE(pInfo->curRange)) {
      code = pInfo->stateStore.streamStateMergeAllScanRange(pInfo->basic.pTsDataState);
      QUERY_CHECK_CODE(code, lino, _end);

      code = pInfo->stateStore.streamStatePopScanRange(pInfo->basic.pTsDataState, &pInfo->curRange);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_INVALID_RANGE(pInfo->curRange)) {
        break;
      }
      code = prepareDataRangeScan(pInfo, &pInfo->curRange);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(pInfo->pUpdateRes);
      code = buildRecBlockByRange(&pInfo->curRange, pInfo->pUpdateRes);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInfo->pUpdateRes->info.rows > 0) {
        (*ppRes) = pInfo->pUpdateRes;
        break;
      }
    }

    code = doOneRangeScan(pInfo, &pInfo->curRange, &pTsdbBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pTsdbBlock != NULL) {
      pInfo->pRangeScanRes = pTsdbBlock;
      code = calBlockTbName(pInfo, pTsdbBlock, 0);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pCreateTbRes->info.rows > 0) {
        (*ppRes) = pInfo->pCreateTbRes;
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
        break;
      }
      (*ppRes) = pTsdbBlock;
      break;
    } else {
      destroyScanRange(&pInfo->curRange);
    }
  }

_end:
  printDataBlock((*ppRes), "stream tsdb scan", GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildStreamRecalculateBlock(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  if (pInfo->basic.pTsDataState->curRecId == -1) {
    int32_t recId = 0;
    code = getRecalculateId(&pInfo->stateStore, pInfo->basic.pTsDataState->pStreamTaskState, &recId);
    QUERY_CHECK_CODE(code, lino, _end);

    qDebug("===stream===%s do recalculate.recId:%d", GET_TASKID(pTaskInfo), recId);
    pInfo->basic.pTsDataState->curRecId = recId;
    pInfo->stateStore.streamStateSetNumber(pInfo->basic.pTsDataState->pStreamTaskState, recId, pInfo->primaryTsIndex);

    SSessionKey      firstKey = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
    pInfo->basic.pTsDataState->pRecCur =
        pInfo->stateStore.streamStateSessionSeekKeyCurrentNext(pInfo->basic.pTsDataState->pStreamTaskState, &firstKey);
  }

  blockDataCleanup(pInfo->pUpdateRes);
  code = blockDataEnsureCapacity(pInfo->pUpdateRes, 1024);
  QUERY_CHECK_CODE(code, lino, _end);

  while (1) {
    SSessionKey rangKey = {0};
    void*       pVal = NULL;
    int32_t     len = 0;
    int32_t     winRes = pInfo->stateStore.streamStateSessionGetKVByCur(pInfo->basic.pTsDataState->pRecCur, &rangKey, &pVal, &len);
    if (winRes != TSDB_CODE_SUCCESS || pVal == NULL) {
      break;
    }
    SRecDataInfo* pRecData = (SRecDataInfo*)pVal;
    if (pInfo->pUpdateRes->info.rows == 0) {
      pInfo->pUpdateRes->info.type = pRecData->mode;
    } else if (pInfo->pUpdateRes->info.type != pRecData->mode || pInfo->pUpdateRes->info.rows ==  pInfo->pUpdateRes->info.capacity) {
      break;
    }

    code = appendOneRowToSpecialBlockImpl(pInfo->pUpdateRes, &rangKey.win.skey, &rangKey.win.ekey, &pRecData->calWin.skey,
                                          &pRecData->calWin.ekey, &pRecData->tableUid ,&rangKey.groupId, NULL, (void*)pRecData->pPkColData);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->stateStore.streamStateCurNext(pInfo->basic.pTsDataState->pStreamTaskState, pInfo->basic.pTsDataState->pRecCur);
  }

  if (pInfo->pUpdateRes->info.rows > 0) {
    (*ppRes) = pInfo->pUpdateRes;
  } else {
    (*ppRes) = NULL;
    pInfo->stateStore.streamStateFreeCur(pInfo->basic.pTsDataState->pRecCur);
    pInfo->basic.pTsDataState->pRecCur = NULL;
    pInfo->basic.pTsDataState->curRecId = -1;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static int32_t doStreamRecalculateDataScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  switch (pInfo->scanMode) {
    case STREAM_SCAN_FROM_RES: {
      (*ppRes) = pInfo->pRangeScanRes;
      pInfo->pRangeScanRes = NULL;
      pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      printDataBlock((*ppRes), "stream tsdb scan", GET_TASKID(pTaskInfo));
      goto _end;
    } break;
    case STREAM_SCAN_FROM_CREATE_TABLERES: {
      (*ppRes) = pInfo->pCreateTbRes;
      pInfo->scanMode = STREAM_SCAN_FROM_RES;
    }
    default:
      (*ppRes) = NULL;
      break;
  }

  if (pInfo->basic.pTsDataState->curRecId == -1) {
    int32_t recId = 0;
    code = getRecalculateId(&pInfo->stateStore, pInfo->basic.pTsDataState->pStreamTaskState, &recId);
    QUERY_CHECK_CODE(code, lino, _end);
    qDebug("===stream===%s do recalculate.recId:%d", GET_TASKID(pTaskInfo), recId);
    pInfo->stateStore.streamStateSetNumber(pInfo->basic.pTsDataState->pStreamTaskState, recId, pInfo->primaryTsIndex);
    code = generateDataScanRange(pInfo, GET_TASKID(pTaskInfo));
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->basic.pTsDataState->curRecId = recId;
  }

  code = doDataRangeScan(pInfo, pTaskInfo, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((*ppRes) == NULL) {
    pInfo->stateStore.streamStateSessionDeleteAll(pInfo->basic.pTsDataState->pState);
    pInfo->basic.pTsDataState->curRecId = -1;
    pTaskInfo->streamInfo.recoverScanFinished = true;
    qInfo("===stream===%s recalculate is finished.", GET_TASKID(pTaskInfo));
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static int32_t doStreamRecalculateBlockScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("===stream===%s doStreamRecalculateBlockScan", GET_TASKID(pTaskInfo));

  if (pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RETRIEVE) {
    if (pInfo->pRangeScanRes != NULL) {
      (*ppRes) = pInfo->pRangeScanRes;
      pInfo->pRangeScanRes = NULL;
      goto _end;
    }
    SSDataBlock* pSDB = NULL;
    code = doRangeScan(pInfo, pInfo->pUpdateRes, pInfo->primaryTsIndex, &pInfo->updateResIndex, &pSDB);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pSDB) {
      STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
      pSDB->info.type = STREAM_PULL_DATA;
      
      printSpecDataBlock(pSDB, getStreamOpName(pOperator->operatorType), "update", GET_TASKID(pTaskInfo));
      code = calBlockTbName(pInfo, pSDB, 0);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pCreateTbRes->info.rows > 0) {
        printSpecDataBlock(pInfo->pCreateTbRes, getStreamOpName(pOperator->operatorType), "update",
                           GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pCreateTbRes;
        pInfo->pRangeScanRes = pSDB;
        goto _end;
      }

      (*ppRes) = pSDB;
      goto _end;
    }
    blockDataCleanup(pInfo->pUpdateRes);
    pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
    pStreamInfo->recoverScanFinished = true;
  }

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
  while (1) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      (*ppRes) = NULL;
      break;
    }
    int32_t current = pInfo->validBlockIndex++;
    qDebug("process %d/%d recalculate input data blocks, %s", current, (int32_t)total, GET_TASKID(pTaskInfo));

    SPackedData* pPacked = taosArrayGet(pInfo->pBlockLists, current);
    QUERY_CHECK_NULL(pPacked, code, lino, _end, terrno);

    SSDataBlock* pBlock = pPacked->pDataBlock;
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;

    code = blockDataUpdateTsWindow(pBlock, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "rec recv", GET_TASKID(pTaskInfo));
    switch (pBlock->info.type) {
      case STREAM_RETRIEVE: {
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RETRIEVE;
        code = copyDataBlock(pInfo->pUpdateRes, pBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->updateResIndex = 0;
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex, NULL);
        (*ppRes) = pInfo->pUpdateRes;
        goto _end;
      } break;
      default: {
        (*ppRes) = pBlock;
        goto _end;
      } break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

int32_t doStreamRecalculateScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  char*            pTaskIdStr = GET_TASKID(pTaskInfo);
  SStreamScanInfo* pInfo = pOperator->info;

  qDebug("stream recalculate scan started, %s", pTaskIdStr);

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
  switch (pInfo->blockType) {
    case STREAM_INPUT__DATA_BLOCK: {
      code = doStreamRecalculateBlockScan(pOperator, ppRes);
      QUERY_CHECK_CODE(code, lino, _end);
    } break;
    case STREAM_INPUT__CHECKPOINT: {
      doClearBufferedBlocks(pInfo);
      (*ppRes) = NULL;
      qDebug("===stream===%s process input data blocks,size:%d", pTaskIdStr, (int32_t)total);
    } break;
    case STREAM_INPUT__RECALCULATE:
    default: {
      doClearBufferedBlocks(pInfo);
      if(isFinalOperator(&pInfo->basic)) {
        code = buildStreamRecalculateBlock(pOperator, ppRes);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (isSingleOperator(&pInfo->basic)) {
        code = doStreamRecalculateDataScan(pOperator, ppRes);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        qDebug("===stream===%s return empty block", pTaskIdStr);
        (*ppRes) = NULL;
      }
    } break;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static void destroyStreamRecalculateParam(SStreamRecParam* pParam) {
  tSimpleHashCleanup(pParam->pColIdMap);
  pParam->pColIdMap = NULL;
}

static void destroyStreamDataScanOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }

  SStreamScanInfo* pStreamScan = (SStreamScanInfo*)param;
  if (pStreamScan->pTableScanOp && pStreamScan->pTableScanOp->info) {
    destroyOperator(pStreamScan->pTableScanOp);
  }

  if (pStreamScan->pRecTableScanOp && pStreamScan->pRecTableScanOp->info) {
    destroyOperator(pStreamScan->pRecTableScanOp);
  }

  if (pStreamScan->tqReader != NULL && pStreamScan->readerFn.tqReaderClose != NULL) {
    pStreamScan->readerFn.tqReaderClose(pStreamScan->tqReader);
  }
  if (pStreamScan->matchInfo.pList) {
    taosArrayDestroy(pStreamScan->matchInfo.pList);
  }
  if (pStreamScan->pPseudoExpr) {
    destroyExprInfo(pStreamScan->pPseudoExpr, pStreamScan->numOfPseudoExpr);
    taosMemoryFree(pStreamScan->pPseudoExpr);
  }

  cleanupExprSupp(&pStreamScan->tbnameCalSup);
  cleanupExprSupp(&pStreamScan->tagCalSup);

  blockDataDestroy(pStreamScan->pRes);
  blockDataDestroy(pStreamScan->pCreateTbRes);
  taosArrayDestroy(pStreamScan->pBlockLists);
  blockDataDestroy(pStreamScan->pCheckpointRes);
  blockDataDestroy(pStreamScan->pDeleteDataRes);
  blockDataDestroy(pStreamScan->pUpdateRes);

  if (pStreamScan->stateStore.streamStateDestroyTsDataState) {
    pStreamScan->stateStore.streamStateDestroyTsDataState(pStreamScan->basic.pTsDataState);
    pStreamScan->basic.pTsDataState = NULL;
  }

  if (pStreamScan->stateStore.updateInfoDestroy != NULL && pStreamScan->pUpdateInfo != NULL) {
    pStreamScan->stateStore.updateInfoDestroy(pStreamScan->pUpdateInfo);
  }
  tSimpleHashCleanup(pStreamScan->pRecRangeMap);
  pStreamScan->pRecRangeMap = NULL;

  taosArrayDestroy(pStreamScan->pRecRangeRes);
  pStreamScan->pRecRangeRes = NULL;

  destroyStreamRecalculateParam(&pStreamScan->recParam);

  taosMemoryFree(pStreamScan);
}

#if 0
static int32_t doStreamScanTest(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamScanInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock* pBlock = NULL;

  pInfo->basic.pTsDataState->pStreamTaskState = (SStreamState*)taosMemoryCalloc(1, sizeof(SStreamState));
  QUERY_CHECK_NULL(pInfo->basic.pTsDataState->pStreamTaskState, code, lino, _end, terrno);
  *((SStreamState*)pInfo->basic.pTsDataState->pStreamTaskState) = *pTaskInfo->streamInfo.pState;

  doStreamDataScanNext(pOperator, ppRes);
  if ((*ppRes) != NULL) {
    return code;
  }

  {
    code = createDataBlock(&pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    pBlock->info.type = STREAM_RECALCULATE_START;
    SPackedData pack = {0};
    pack.pDataBlock = pBlock;
    void* pBuf = taosArrayPush(pInfo->pBlockLists, &pack);
    QUERY_CHECK_NULL(pBuf, code, lino, _end, terrno);
  }

  pInfo->blockType = STREAM_INPUT__DATA_BLOCK;
  doStreamDataScanNext(pOperator, ppRes);

  SOperatorInfo* pRoot = pTaskInfo->pRoot;
  SStreamIntervalSliceOperatorInfo* pIntervalInfo = pRoot->info;
  setRecalculateOperatorFlag(&pIntervalInfo->basic);
  code = doStreamRecalculateScanNext(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((*ppRes) == NULL) {
    // unsetRecalculateOperatorFlag(&pIntervalInfo->basic);
    // pBlock->info.type = STREAM_RECALCULATE_END;
    // SPackedData pack = {0};
    // pack.pDataBlock = pBlock;
    // void* pBuf = taosArrayPush(pInfo->pBlockLists, &pack);
    // QUERY_CHECK_NULL(pBuf, code, lino, _end, terrno);
    // doStreamDataScanNext(pOperator, ppRes);
    // pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
  }

_end:
  blockDataDestroy(pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    (*ppRes) = NULL;
  }
  return code;
}
#endif

static void initStreamRecalculateParam(STableScanPhysiNode* pTableScanNode, SStreamRecParam* pParam) {
  tstrncpy(pParam->pStbFullName, pTableScanNode->pStbFullName, TSDB_TABLE_FNAME_LEN);
  tstrncpy(pParam->pWstartName, pTableScanNode->pWstartName, TSDB_COL_NAME_LEN);
  tstrncpy(pParam->pWendName, pTableScanNode->pWendName, TSDB_COL_NAME_LEN);
  tstrncpy(pParam->pGroupIdName, pTableScanNode->pGroupIdName, TSDB_COL_NAME_LEN);
  tstrncpy(pParam->pIsWindowFilledName, pTableScanNode->pIsWindowFilledName, TSDB_COL_NAME_LEN);


  pParam->sqlCapcity = tListLen(pParam->pSql);
  (void)tsnprintf(pParam->pUrl, tListLen(pParam->pUrl), "http://%s:%d/rest/sql", tsAdapterFqdn, tsAdapterPort);
  (void)tsnprintf(pParam->pAuth, tListLen(pParam->pAuth), "Authorization: Basic %s", tsAdapterToken);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pParam->pColIdMap = tSimpleHashInit(32, hashFn);
}

int32_t createStreamDataScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond,
                                         STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                         SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SArray*             pColIds = NULL;
  SScanPhysiNode*     pScanPhyNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;
  SStreamScanInfo*    pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  const char*         idstr = pTaskInfo->id.str;
  SStorageAPI*        pAPI = &pTaskInfo->storageAPI;

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->pTagCond = pTagCond;
  pInfo->pGroupTags = pTableScanNode->pGroupTags;

  int32_t numOfCols = 0;
  code = extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->basic.primaryPkIndex = -1;
  int32_t numOfOutput = taosArrayGetSize(pInfo->matchInfo.pList);
  pColIds = taosArrayInit(numOfOutput, sizeof(int16_t));
  QUERY_CHECK_NULL(pColIds, code, lino, _error, terrno);

  SDataType pkType = {0};
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchItem* id = taosArrayGet(pInfo->matchInfo.pList, i);
    QUERY_CHECK_NULL(id, code, lino, _error, terrno);

    int16_t colId = id->colId;
    void*   tmp = taosArrayPush(pColIds, &colId);
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);

    if (id->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pInfo->primaryTsIndex = id->dstSlotId;
    }
    if (id->isPk) {
      pInfo->basic.primaryPkIndex = id->dstSlotId;
      pkType = id->dataType;
    }
  }

  pInfo->pPartTbnameSup = NULL;
  if (pTableScanNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    QUERY_CHECK_NULL(pSubTableExpr, code, lino, _error, terrno);

    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    code = createExprFromOneNode(pSubTableExpr, pTableScanNode->pSubtable, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  if (pTableScanNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExpr(pTableScanNode->pTags, &numOfTags);
    QUERY_CHECK_NULL(pTagExpr, code, lino, _error, terrno);
    code = initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->pBlockLists = taosArrayInit(4, sizeof(SPackedData));
  TSDB_CHECK_NULL(pInfo->pBlockLists, code, lino, _error, terrno);

  pInfo->pTableScanOp = NULL;
  if (pHandle->vnode) {
    code = createTableScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo, &pInfo->pTableScanOp);
    QUERY_CHECK_CODE(code, lino, _error);

    STableScanInfo* pTSInfo = (STableScanInfo*)pInfo->pTableScanOp->info;
    if (pHandle->version > 0) {
      pTSInfo->base.cond.endVersion = pHandle->version;
    }

    STableKeyInfo* pList = NULL;
    int32_t        num = 0;
    code = tableListGetGroupList(pTableListInfo, 0, &pList, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    if (pHandle->initTqReader) {
      pInfo->tqReader = pAPI->tqReaderFn.tqReaderOpen(pHandle->vnode);
      QUERY_CHECK_NULL(pInfo->tqReader, code, lino, _error, terrno);
    } else {
      pInfo->tqReader = pHandle->tqReader;
      QUERY_CHECK_NULL(pInfo->tqReader, code, lino, _error, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }

    pTaskInfo->streamInfo.snapshotVer = pHandle->version;
    pInfo->pCreateTbRes = buildCreateTableBlock(&pInfo->tbnameCalSup, &pInfo->tagCalSup);
    QUERY_CHECK_NULL(pInfo->pCreateTbRes, code, lino, _error, terrno);

    code = blockDataEnsureCapacity(pInfo->pCreateTbRes, 8);
    QUERY_CHECK_CODE(code, lino, _error);

    // set the extract column id to streamHandle
    pAPI->tqReaderFn.tqReaderSetColIdList(pInfo->tqReader, pColIds);

    SArray* tableIdList = NULL;
    code = extractTableIdList(((STableScanInfo*)(pInfo->pTableScanOp->info))->base.pTableListInfo, &tableIdList);
    QUERY_CHECK_CODE(code, lino, _error);
    pAPI->tqReaderFn.tqReaderSetQueryTableList(pInfo->tqReader, tableIdList, idstr);
    taosArrayDestroy(tableIdList);
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->base.cond, sizeof(SQueryTableDataCond));

    STableListInfo* pRecTableListInfo = tableListCreate();
    QUERY_CHECK_NULL(pRecTableListInfo, code, lino, _error, terrno);
    code = createTableScanOperatorInfo(pTableScanNode, pHandle, pRecTableListInfo, pTaskInfo, &pInfo->pRecTableScanOp);
    QUERY_CHECK_CODE(code, lino, _error);
  } else {
    taosArrayDestroy(pColIds);
    tableListDestroy(pTableListInfo);
  }

  // clear the local variable to avoid repeatly free
  pColIds = NULL;

  // create the pseduo columns info
  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    code = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pInfo->pPseudoExpr, &pInfo->numOfPseudoExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  code = createSpecialDataBlock(STREAM_RECALCULATE_DELETE, &pInfo->pDeleteDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  code = createSpecialDataBlock(STREAM_RECALCULATE_DATA, &pInfo->pUpdateRes);
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->pRecoverRes = NULL;

  pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
  pInfo->twAggSup.maxTs = INT64_MIN;
  pInfo->readerFn = pTaskInfo->storageAPI.tqReaderFn;
  pInfo->readHandle = *pHandle;
  pInfo->comparePkColFn = getKeyComparFunc(pkType.type, TSDB_ORDER_ASC);
  pInfo->curRange = (SScanRange){0};
  pInfo->scanAllTables = false;
  pInfo->hasPart = false;
  pInfo->assignBlockUid = groupbyTbname(pInfo->pGroupTags);

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  SStreamState* pTempState = (SStreamState*)taosMemoryCalloc(1, sizeof(SStreamState));
  QUERY_CHECK_NULL(pTempState, code, lino, _error, terrno);
  (*pTempState) = *pTaskInfo->streamInfo.pState;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->stateStore.streamStateSetNumber(pTempState, 1, pInfo->primaryTsIndex);
  
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pRecRangeMap = tSimpleHashInit(32, hashFn);
  taosArrayDestroy(pInfo->pRecRangeRes);
  pInfo->pRecRangeRes = taosArrayInit(64, POINTER_BYTES);
  initStreamRecalculateParam(pTableScanNode, &pInfo->recParam);

  void* pOtherState = pTaskInfo->streamInfo.pOtherState;
  pAPI->stateStore.streamStateInitTsDataState(&pInfo->basic.pTsDataState, pkType.type, pkType.bytes, pTempState, pOtherState);
  pAPI->stateStore.streamStateRecoverTsData(pInfo->basic.pTsDataState);
  setSingleOperatorFlag(&pInfo->basic);

  pInfo->pStreamScanOp = pOperator;

  // for stream
  if (pTaskInfo->streamInfo.pState) {
    void*   buff = NULL;
    int32_t len = 0;
    int32_t res = pAPI->stateStore.streamStateGetInfo(pTaskInfo->streamInfo.pState, STREAM_DATA_SCAN_OP_CHECKPOINT_NAME,
                                                      strlen(STREAM_DATA_SCAN_OP_CHECKPOINT_NAME), &buff, &len);
  }

  setOperatorInfo(pOperator, STREAM_DATA_SCAN_OP_NAME, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  if (pHandle->fillHistory == STREAM_RECALCUL_OPERATOR) {
    pOperator->fpSet =
        createOperatorFpSet(optrDummyOpenFn, doStreamRecalculateScanNext, NULL, destroyStreamDataScanOperatorInfo,
                            optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  } else {
    pOperator->fpSet =
        createOperatorFpSet(optrDummyOpenFn, doStreamDataScanNext, NULL, destroyStreamDataScanOperatorInfo,
                            optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  }
  // doStreamScanTest
  // pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamScanTest, NULL, destroyStreamDataScanOperatorInfo,
  //                                        optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamDataScanReleaseState, streamDataScanReloadState);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pColIds != NULL) {
    taosArrayDestroy(pColIds);
  }

  if (pInfo != NULL) {
    STableScanInfo* p = (STableScanInfo*)pInfo->pTableScanOp->info;
    if (p != NULL) {
      p->base.pTableListInfo = NULL;
    }
    destroyStreamDataScanOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}
