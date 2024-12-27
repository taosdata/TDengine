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
#define IS_INVALID_RANGE(range)             (range.pGroupIds == NULL)

bool hasDataPrimaryKeyCol(SSteamOpBasicInfo* pInfo) { return pInfo->primaryPkIndex != -1; }

static int32_t getMaxTsKeyInfo(SStreamScanInfo* pInfo, SSDataBlock* pBlock, TSKEY* pCurTs, void** ppPkVal,
                               int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pLastPkVal = NULL;
  int32_t lastPkLen = 0;
  if (hasDataPrimaryKeyCol(&pInfo->basic)) {
    SColumnInfoData* pPkColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->basic.primaryPkIndex);
    pLastPkVal = colDataGetData(pPkColDataInfo, pBlock->info.rows - 1);
    lastPkLen = colDataGetRowLength(pPkColDataInfo, pBlock->info.rows - 1);
  }

  code = pInfo->stateStore.streamStateGetAndSetTsData(&pInfo->tsDataState, pBlock->info.id.uid, pCurTs, ppPkVal,
                                                      pBlock->info.window.ekey, pLastPkVal, lastPkLen, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

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
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_GET_ALL:
        printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        setStreamOperatorState(&pInfo->basic, pBlock->info.type);
        (*ppRes) = pBlock;
        break;
      case STREAM_DELETE_DATA: {
        printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "delete recv", GET_TASKID(pTaskInfo));
        if (pInfo->tqReader) {
          code = filterDelBlockByUid(pInfo->pDeleteDataRes, pBlock, pInfo->tqReader, &pInfo->readerFn);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          code = copyDataBlock(pInfo->pDeleteDataRes, pBlock);
          QUERY_CHECK_CODE(code, lino, _end);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
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
      default:
        break;
    }
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);
    break;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    (*ppRes) = NULL;
  }
  return code;
}

static int32_t buildRecalculateData(SSDataBlock* pSrcBlock, TSKEY* pTsCol, SColumnInfoData* pPkColDataInfo,
                                    SSDataBlock* pDestBlock, int32_t num, SPartitionBySupporter* pParSup,
                                    SExprSupp* pPartScalarSup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  blockDataCleanup(pDestBlock);
  blockDataEnsureCapacity(pDestBlock, num);
  for (int32_t rowId = 0; rowId < num; rowId++) {
    uint64_t gpId = 0;
    code = appendPkToSpecialBlock(pDestBlock, pTsCol, pPkColDataInfo, rowId, &pSrcBlock->info.id.uid, &gpId, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pParSup->needCalc) {
      gpId = calGroupIdByData(pParSup, pPartScalarSup, pSrcBlock, rowId);
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

static int32_t doStreamWALScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;
  int32_t          totalBlocks = taosArrayGetSize(pInfo->pBlockLists);

  if (pInfo->scanMode == STREAM_SCAN_FROM_RES) {
    pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
    (*ppRes) = pInfo->pRes;
    return code;
  }

  while (1) {
    if (pInfo->readerFn.tqReaderCurrentBlockConsumed(pInfo->tqReader)) {
      if (pInfo->validBlockIndex >= totalBlocks) {
        doClearBufferedBlocks(pInfo);

        qDebug("stream scan return empty, all %d submit blocks consumed, %s", totalBlocks, GET_TASKID(pTaskInfo));
        (*ppRes) = NULL;
        return code;
      }

      int32_t      current = pInfo->validBlockIndex++;
      SPackedData* pSubmit = taosArrayGet(pInfo->pBlockLists, current);
      QUERY_CHECK_NULL(pSubmit, code, lino, _end, terrno);

      qDebug("set %d/%d as the input submit block, %s", current + 1, totalBlocks, GET_TASKID(pTaskInfo));
      if (pAPI->tqReaderFn.tqReaderSetSubmitMsg(pInfo->tqReader, pSubmit->msgStr, pSubmit->msgLen, pSubmit->ver) < 0) {
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

      SColumnInfoData* pPkColDataInfo = NULL;
      if (hasDataPrimaryKeyCol(&pInfo->basic)) {
        pPkColDataInfo = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->basic.primaryPkIndex);
      }
      SColumnInfoData* pTsCol = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->primaryTsIndex);
      if (winCode == TSDB_CODE_SUCCESS && curTs >= pInfo->pRes->info.window.skey) {
        int32_t num = 0;
        if (curTs < pInfo->pRes->info.window.ekey) {
          num = getForwardStepsInBlock(pInfo->pRes->info.rows, binarySearchForKey, curTs, 0, TSDB_ORDER_ASC,
                                       (TSKEY*)pTsCol->pData);
          if (hasDataPrimaryKeyCol(&pInfo->basic)) {
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
          code = buildRecalculateData(pInfo->pRes, (TSKEY*)pTsCol->pData, pPkColDataInfo, pInfo->pDisorderDataRes, num,
                                      &pInfo->partitionSup, pInfo->pPartScalarSup);
          QUERY_CHECK_CODE(code, lino, _end);
          code = blockDataTrimFirstRows(pInfo->pRes, num);
          QUERY_CHECK_CODE(code, lino, _end);
          code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      if (pInfo->pCreateTbRes->info.rows > 0) {
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
        qDebug("create table res exists, rows:%" PRId64 " return from stream scan, %s", pInfo->pCreateTbRes->info.rows,
               GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pCreateTbRes;
        break;
      }

      if (pInfo->pRes->info.rows > 0) {
        (*ppRes) = pInfo->pRes;
        break;
      }
    }

    if ((*ppRes) != NULL && (*ppRes)->info.rows > 0) {
      break;
    }
  }

  // record the scan action.
  pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;

  qDebug("stream scan completed, and return source rows:%" PRId64 ", %s", pInfo->pRes->info.rows,
         GET_TASKID(pTaskInfo));
  if (pInfo->pRes->info.rows > 0) {
    printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pRes;
  }

_end:
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
    pInfo->stateStore.streamStateTsDataCommit(&pInfo->tsDataState);
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
    case STREAM_INPUT__CHECKPOINT: {
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

      if (pBlock->info.type == STREAM_CHECKPOINT) {
        streamDataScanOperatorSaveCheckpoint(pInfo);
      }
      printDataBlock(pInfo->pCheckpointRes, "stream scan ck", GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
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
  pInfo->stateStore.streamStateTsDataCommit(&pInfo->tsDataState);
}

void streamDataScanReloadState(SOperatorInfo* pOperator) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamScanInfo* pInfo = pOperator->info;
  code = pInfo->stateStore.streamStateReloadTsDataState(&pInfo->tsDataState);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static int32_t readPreVersionDataBlock(uint64_t uid, TSKEY startTs, TSKEY endTs, int64_t version, char* taskIdStr,
                                       SStreamScanInfo* pInfo, SSDataBlock* pBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uid, startTs, endTs, version);
  printDataBlock(pPreRes, "pre res", taskIdStr);
  blockDataCleanup(pBlock);
  QUERY_CHECK_NULL(pPreRes, code, lino, _end, terrno);
  code = blockDataEnsureCapacity(pBlock, pPreRes->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData* pTsCol = (SColumnInfoData*)taosArrayGet(pPreRes->pDataBlock, pInfo->primaryTsIndex);
  SColumnInfoData* pPkCol = NULL;
  if (hasDataPrimaryKeyCol(&pInfo->basic)) {
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

static uint64_t getDataGroupIdByCol(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion, void* pVal,
                                    bool* pRes) {
  SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uid, ts, ts, maxVersion);
  if (!pPreRes || pPreRes->info.rows == 0) {
    if (terrno != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    }
    (*pRes) = false;
    return 0;
  }

  int32_t rowId = 0;
  if (hasDataPrimaryKeyCol(&pInfo->basic)) {
    SColumnInfoData* pPkCol = taosArrayGet(pPreRes->pDataBlock, pInfo->basic.primaryPkIndex);
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
  return calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, rowId);
}

static uint64_t getDataGroupId(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion, void* pVal,
                               bool* pRes) {
  if (pInfo->partitionSup.needCalc) {
    return getDataGroupIdByCol(pInfo, uid, ts, maxVersion, pVal, pRes);
  }

  *pRes = true;
  STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
  return tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, uid);
}

static int32_t generateIntervalDataScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, EStreamType mode,
                                             char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  SSHashObj*       pScanRange = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pSrcGpCol = taosArrayGet(pSrcBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pSrcPkCol = NULL;
  if (taosArrayGetSize(pSrcBlock->pDataBlock) > PRIMARY_KEY_COLUMN_INDEX) {
    pSrcPkCol = taosArrayGet(pSrcBlock->pDataBlock, PRIMARY_KEY_COLUMN_INDEX);
  }

  uint64_t* srcUidData = (uint64_t*)pSrcUidCol->pData;
  TSKEY*    srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*    srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  int64_t   ver = pSrcBlock->info.version - 1;

  if (pInfo->partitionSup.needCalc &&
      (srcStartTsCol[0] != srcEndTsCol[0] || (hasDataPrimaryKeyCol(&pInfo->basic) && mode == STREAM_DELETE_DATA))) {
    code = readPreVersionDataBlock(srcUidData[0], srcStartTsCol[0], srcEndTsCol[0], ver, pTaskIdStr, pInfo, pSrcBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
    srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
    srcUidData = (uint64_t*)pSrcUidCol->pData;
  }

  uint64_t* srcGp = (uint64_t*)pSrcGpCol->pData;
  bool      hasGroupId = false;
  for (int32_t i = 0; i < pSrcBlock->info.rows;) {
    uint64_t srcUid = srcUidData[i];
    uint64_t groupId = srcGp[i];
    if (groupId == 0) {
      void* pVal = NULL;
      if (hasDataPrimaryKeyCol(&pInfo->basic) && pSrcPkCol) {
        pVal = colDataGetData(pSrcPkCol, i);
      }
      groupId = getDataGroupId(pInfo, srcUid, srcStartTsCol[i], ver, pVal, &hasGroupId);
    }

    STimeWindow win = getSlidingWindow(srcStartTsCol, srcEndTsCol, srcGp, &pInfo->interval, &pSrcBlock->info, &i,
                                       pInfo->partitionSup.needCalc);
    pInfo->stateStore.streamStateMergeAndSaveScanRange(&pInfo->tsDataState, &win, groupId, srcUidData[i]);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t generateDataScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, EStreamType type,
                                     char* pTaskIdStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  switch (pInfo->windowSup.parentType) {
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL: {
      code = generateIntervalDataScanRange(pInfo, pSrcBlock, type, pTaskIdStr);
      QUERY_CHECK_CODE(code, lino, _end);
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
  bool    prepareRes = true;

  while (1) {
    SSDataBlock* pResult = NULL;
    code = doTableScanNext(pInfo->pTableScanOp, &pResult);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pResult != NULL) {
      if (prepareRes) {
        continue;
      }
      STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
      pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
      pTableScanInfo->base.dataReader = NULL;
      (*ppRes) = NULL;
      goto _end;
    }

    code = doFilter(pResult, pInfo->pTableScanOp->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pResult->info.rows == 0) {
      continue;
    }

    if (pInfo->partitionSup.needCalc) {
      SSDataBlock* tmpBlock = NULL;
      code = createOneDataBlock(pResult, true, &tmpBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(pResult);
      for (int32_t i = 0; i < tmpBlock->info.rows; i++) {
        uint64_t dataGroupId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, tmpBlock, i);
        if (tSimpleHashGet(pRange->pGroupIds, &dataGroupId, sizeof(uint64_t)) != NULL) {
          for (int32_t j = 0; j < pInfo->pTableScanOp->exprSupp.numOfExprs; j++) {
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
        pResult->info.calWin = pRange->win;
        (*ppRes) = pResult;
        goto _end;
      }
    } else {
      if (tSimpleHashGet(pRange->pGroupIds, &pResult->info.id.groupId, sizeof(uint64_t)) != NULL) {
        pResult->info.calWin = pRange->win;
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

static int32_t prepareDataRangeScan(SStreamScanInfo* pInfo, SScanRange* pRange) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SOperatorParam*          pOpParam = NULL;
  STableScanOperatorParam* pTableScanParam = NULL;

  resetTableScanInfo(pInfo->pTableScanOp->info, &pRange->win, -1);
  pOpParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pOpParam, code, lino, _end, terrno);
  pOpParam->downstreamIdx = 0;
  pOpParam->opType = 0;
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
  }

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

static int32_t doDataRangeScan(SStreamScanInfo* pInfo, SExecTaskInfo* pTaskInfo, SSDataBlock** ppRes) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pTsdbBlock = NULL;
  pInfo->pRecoverRes = NULL;
  while (1) {
    if (IS_INVALID_RANGE(pInfo->curRange)) {
      code = pInfo->stateStore.streamStateMergeAllScanRange(&pInfo->tsDataState);
      QUERY_CHECK_CODE(code, lino, _end);

      code = pInfo->stateStore.streamStatePopScanRange(&pInfo->tsDataState, &pInfo->curRange);
      QUERY_CHECK_CODE(code, lino, _end);
      if (IS_INVALID_RANGE(pInfo->curRange)) {
        break;
      }
      prepareDataRangeScan(pInfo, &pInfo->curRange);
    }

    code = doOneRangeScan(pInfo, &pInfo->curRange, &pTsdbBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pTsdbBlock != NULL) {
      code = calBlockTbName(pInfo, pTsdbBlock, 0);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pInfo->pCreateTbRes->info.rows > 0) {
        (*ppRes) = pInfo->pCreateTbRes;
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
      }
      pInfo->pRecoverRes = pTsdbBlock;
      break;
    } else {
      pInfo->curRange = (SScanRange){0};
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doStreamRecalculateBlockScan(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;
  char*            pTaskIdStr = GET_TASKID(pTaskInfo);

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
  while (1) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      break;
    }

    int32_t current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t)total, pTaskIdStr);

    SPackedData* pPacked = taosArrayGet(pInfo->pBlockLists, current);
    QUERY_CHECK_NULL(pPacked, code, lino, _end, terrno);

    SSDataBlock* pBlock = pPacked->pDataBlock;
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;

    code = blockDataUpdateTsWindow(pBlock, 0);
    QUERY_CHECK_CODE(code, lino, _end);
    switch (pBlock->info.type) {
      case STREAM_RECALCULATE_DATA: {
        generateDataScanRange(pInfo, pBlock, STREAM_CLEAR, pTaskIdStr);
      } break;
      case STREAM_RECALCULATE_DELETE: {
        generateDataScanRange(pInfo, pBlock, STREAM_DELETE_DATA, pTaskIdStr);
      } break;
      case STREAM_DROP_CHILD_TABLE: {
        int32_t deleteNum = 0;
        code = deletePartName(&pInfo->stateStore, pTaskInfo->streamInfo.pState, pBlock, &deleteNum);
        QUERY_CHECK_CODE(code, lino, _end);
      } break;
      default:
        qError("stream check point error. msg type: %d", pBlock->info.type);
        break;
    }
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);
  }

  code = doDataRangeScan(pInfo, pTaskInfo, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

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
  const char*      id = GET_TASKID(pTaskInfo);
  SStreamScanInfo* pInfo = pOperator->info;

  size_t total = taosArrayGetSize(pInfo->pBlockLists);

  switch (pInfo->blockType) {
    case STREAM_INPUT__DATA_BLOCK: {
      code = doStreamRecalculateBlockScan(pOperator, ppRes);
      QUERY_CHECK_CODE(code, lino, _end);
    } break;
    case STREAM_INPUT__CHECKPOINT: {
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

      if (pBlock->info.type == STREAM_CHECKPOINT) {
        streamDataScanOperatorSaveCheckpoint(pInfo);
      }
      printDataBlock(pInfo->pCheckpointRes, "stream scan ck", GET_TASKID(pTaskInfo));
      (*ppRes) = pInfo->pCheckpointRes;
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

static void destroyStreamDataScanOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }

  SStreamScanInfo* pStreamScan = (SStreamScanInfo*)param;
  if (pStreamScan->pTableScanOp && pStreamScan->pTableScanOp->info) {
    destroyOperator(pStreamScan->pTableScanOp);
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

  if (pStreamScan->stateStore.streamStateDestroyTsDataState) {
    pStreamScan->stateStore.streamStateDestroyTsDataState(&pStreamScan->tsDataState);
  }

  taosMemoryFree(pStreamScan);
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

  code = createSpecialDataBlock(STREAM_DELETE_DATA, &pInfo->pDeleteDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  code = createSpecialDataBlock(STREAM_RECALCULATE_DATA, &pInfo->pDisorderDataRes);
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->pRecoverRes = NULL;

  pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
  pInfo->twAggSup.maxTs = INT64_MIN;
  pInfo->readerFn = pTaskInfo->storageAPI.tqReaderFn;
  pInfo->readHandle = *pHandle;
  pInfo->comparePkColFn = getKeyComparFunc(pkType.type, TSDB_ORDER_ASC);
  pInfo->curRange = (SScanRange){0};

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  SStreamState* pTempState = (SStreamState*)taosMemoryCalloc(1, sizeof(SStreamState));
  QUERY_CHECK_NULL(pTempState, code, lino, _error, terrno);
  (*pTempState) = *pTaskInfo->streamInfo.pState;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->stateStore.streamStateSetNumber(pTempState, -1, pInfo->primaryTsIndex);
  if (pInfo->pTableScanOp->pTaskInfo->streamInfo.pState) {
    pAPI->stateStore.streamStateSetNumber(pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, -2, pInfo->primaryTsIndex);
  }

  pAPI->stateStore.streamStateInitTsDataState(&pInfo->tsDataState, pkType.type, pkType.bytes,
                                              pTaskInfo->streamInfo.pState);
  pAPI->stateStore.streamStateRecoverTsData(&pInfo->tsDataState);
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

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamDataScanNext, NULL, destroyStreamDataScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
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
