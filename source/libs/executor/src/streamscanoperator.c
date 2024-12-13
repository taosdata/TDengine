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

static int32_t getMaxTsKeyInfo(SStreamScanInfo* pInfo, SSDataBlock* pBlock, TSKEY* pCurTs, void** ppPkVal,
                               int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pLastPkVal = NULL;
  int32_t lastPkLen = 0;
  if (hasPrimaryKeyCol(pInfo)) {
    SColumnInfoData* pPkColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryKeyIndex);
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
          code = filterDelBlockByUid(pInfo->pDeleteDataRes, pBlock, pInfo);
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
        code = deletePartName(pInfo, pBlock, &deleteNum);
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

      SColumnInfoData* pTsCol = taosArrayGet(pInfo->pRes->pDataBlock, pInfo->primaryTsIndex);
      if (winCode == TSDB_CODE_SUCCESS && curTs >= pInfo->pRes->info.window.skey) {
        int32_t num = 0;
        if (curTs < pInfo->pRes->info.window.ekey) {
          num = getForwardStepsInBlock(pInfo->pRes->info.rows, binarySearchForKey, curTs, 0, TSDB_ORDER_ASC,
                                       (TSKEY*)pTsCol->pData);
        } else {
          num = pInfo->pRes->info.rows;
        }

        if (num > 0) {
          qInfo("%s stream scan op ignore disorder data. rows:%d, tableUid:%" PRId64 ", last max ts:%" PRId64
                ", block start key:%" PRId64 ", end key:%" PRId64,
                GET_TASKID(pTaskInfo), num, pInfo->pRes->info.id.uid, curTs, pInfo->pRes->info.window.skey,
                pInfo->pRes->info.window.ekey);
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
  pInfo->numOfExec++;
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

int32_t doStreamDataScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  const char*    id = GET_TASKID(pTaskInfo);

  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("stream scan started, %s", id);

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
        streamScanOperatorSaveCheckpoint(pInfo);
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
