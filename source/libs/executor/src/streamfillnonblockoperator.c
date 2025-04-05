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

#include "filter.h"
#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "streamexecutorInt.h"
#include "streaminterval.h"
#include "streamsession.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "function.h"
#include "operator.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tfill.h"

static int32_t doStreamNonblockFillImpl(SOperatorInfo* pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcBlock;
  uint64_t                 groupId = pBlock->info.id.groupId;
  SColumnInfoData*         pTsCol = taosArrayGet(pInfo->pSrcBlock->pDataBlock, pInfo->primaryTsCol);
  TSKEY*                   tsCol = (TSKEY*)pTsCol->pData;
  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    code = keepBlockRowInStateBuf(pInfo, pFillInfo, pBlock, tsCol, i, groupId, pFillSup->rowSize);
    QUERY_CHECK_CODE(code, lino, _end);
    SWinKey key = {.groupId = groupId, .ts = tsCol[i]};
    void*   pPushRes = taosArrayPush(pInfo->pUpdated, &key);
    QUERY_CHECK_NULL(pPushRes, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static int32_t getResultInfoFromState(SStateStore* pStateStore, SStreamState* pState, SStreamFillSupporter* pFillSup,
                                      SWinKey* pKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  resetTimeSlicePrevAndNextWindow(pFillSup);

  int32_t      tmpRes = TSDB_CODE_SUCCESS;
  SRowBuffPos* pStatePos = NULL;
  int32_t      curVLen = 0;
  code = pStateStore->streamStateFillGet(pState, pKey, (void**)&pStatePos, &curVLen, &tmpRes);
  QUERY_CHECK_CODE(code, lino, _end);

  bool hasCurKey = true;
  if (tmpRes == TSDB_CODE_SUCCESS) {
    pFillSup->cur.key = pKey->ts;
    pFillSup->cur.pRowVal = (SResultCellData*)pStatePos->pRowBuff;
  } else {
    qDebug("streamStateFillGet key failed, Data may be deleted. ts:%" PRId64 ", groupId:%" PRId64, pKey->ts,
           pKey->groupId);
    pFillSup->cur.key = pKey->ts;
    pFillSup->cur.pRowVal = NULL;
    hasCurKey = false;
  }

  SWinKey      preKey = {.groupId = pKey->groupId};
  SRowBuffPos* pPrevStatePos = NULL;
  int32_t      preVLen = 0;
  code = pStateStore->streamStateFillGetPrev(pState, pKey, &preKey, (void**)&pPrevStatePos, &preVLen, &tmpRes);
  QUERY_CHECK_CODE(code, lino, _end);
  qDebug("===stream=== set stream prev buf.ts:%" PRId64 ", groupId:%" PRIu64 ", res:%d", preKey.ts, preKey.groupId,
         tmpRes);
  if (tmpRes == TSDB_CODE_SUCCESS) {
    pFillSup->prevOriginKey = preKey.ts;
    pFillSup->prev.key = adustPrevTsKey(preKey.ts, preKey.ts, &pFillSup->interval);
    pFillSup->prev.pRowVal = (SResultCellData*)pPrevStatePos->pRowBuff;
  }

  SWinKey      nextKey = {.groupId = pKey->groupId};
  SRowBuffPos* pNextStatePos = NULL;
  int32_t      nextVLen = 0;
  code = pStateStore->streamStateFillGetNext(pState, pKey, &nextKey, (void**)&pNextStatePos, &nextVLen, &tmpRes);
  QUERY_CHECK_CODE(code, lino, _end);
  qDebug("===stream=== set stream next buf.ts:%" PRId64 ", groupId:%" PRIu64 ", res:%d", nextKey.ts, nextKey.groupId,
         tmpRes);
  if (tmpRes == TSDB_CODE_SUCCESS) {
    pFillSup->nextOriginKey = nextKey.ts;
    pFillSup->next.key = adustEndTsKey(nextKey.ts, nextKey.ts, &pFillSup->interval);
    pFillSup->next.pRowVal = (SResultCellData*)pNextStatePos->pRowBuff;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t getResultInfoFromResult(SStreamRecParam* pParam, SStreamFillSupporter* pFillSup, SWinKey* pKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = streamClientGetFillRange(pParam, pKey, pFillSup->pResultRange, pFillSup->pEmptyRow, pFillSup->rowSize, pFillSup->pOffsetInfo, pFillSup->numOfAllCols);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < taosArrayGetSize(pFillSup->pResultRange); i++) {
    SSliceRowData* pSRdata = taosArrayGetP(pFillSup->pResultRange, i);
    if (pSRdata->key < pKey->ts) {
      if (pFillSup->prev.key < pSRdata->key) {
        pFillSup->prevOriginKey = pSRdata->key;
        pFillSup->prev.key = adustPrevTsKey(pSRdata->key, pSRdata->key, &pFillSup->interval);
        pFillSup->prev.pRowVal = (SResultCellData*)pSRdata->pRowVal;
      }
    } else if (pSRdata->key > pKey->ts) {
      if ((pFillSup->next.key > pSRdata->key) || (!hasNextWindow(pFillSup) && pSRdata->key != INT64_MIN)) {
        pFillSup->nextOriginKey = pSRdata->key;
        pFillSup->next.key = adustEndTsKey(pSRdata->key, pSRdata->key, &pFillSup->interval);
        pFillSup->next.pRowVal = (SResultCellData*)pSRdata->pRowVal;
      }
    }
  }
  
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void doBuildNonblockFillResult(SOperatorInfo* pOperator, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo,
                               SSDataBlock* pBlock, SGroupResInfo* pGroupResInfo) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  blockDataCleanup(pBlock);
  if (!hasRemainResults(pGroupResInfo)) {
    return;
  }

  int32_t numOfRows = getNumOfTotalRes(pGroupResInfo);
  while (pGroupResInfo->index < numOfRows) {
    SWinKey* pKey = (SWinKey*)taosArrayGet(pGroupResInfo->pRows, pGroupResInfo->index);
    if (pBlock->info.id.groupId == 0) {
      pBlock->info.id.groupId = pKey->groupId;
    } else if (pBlock->info.id.groupId != pKey->groupId) {
      break;
    }

    code = getResultInfoFromState(&pInfo->stateStore, pInfo->pState, pInfo->pFillSup, pKey);
    QUERY_CHECK_CODE(code, lino, _end);
    if (isRecalculateOperator(&pInfo->basic)) {
      taosArrayClearP(pFillSup->pResultRange, NULL);
      getResultInfoFromResult(&pInfo->nbSup.recParam, pFillSup, pKey);
    }

    setTimeSliceFillRule(pFillSup, pFillInfo, pKey->ts);
    doStreamTimeSliceFillRange(pFillSup, pFillInfo, pBlock);
    pGroupResInfo->index++;
    if (pBlock->info.rows >= pBlock->info.capacity) {
      break;
    }
  }

  if (pBlock->info.rows > 0) {
    SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
    void*          tbname = NULL;
    int32_t        winCode = TSDB_CODE_SUCCESS;
    code = pInfo->stateStore.streamStateGetParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname,
                                                   false, &winCode);
    QUERY_CHECK_CODE(code, lino, _end);
    if (winCode != TSDB_CODE_SUCCESS) {
      pBlock->info.parTbName[0] = 0;
    } else {
      memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      qTrace("%s partName:%s, groupId:%"PRIu64, __FUNCTION__, (char*)tbname, pBlock->info.id.groupId);
      pInfo->stateStore.streamStateFreeVal(tbname);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

static int32_t buildNonblockFillResult(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  uint16_t                 opType = pOperator->operatorType;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  doBuildNonblockFillResult(pOperator, pInfo->pFillSup, pInfo->pFillInfo, pInfo->pRes, &pInfo->groupResInfo);
  if (pInfo->pRes->info.rows != 0) {
    printDataBlock(pInfo->pRes, getStreamOpName(opType), GET_TASKID(pTaskInfo));
    (*ppRes) = pInfo->pRes;
    goto _end;
  }

  (*ppRes) = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doDeleteNonblockFillResult(SOperatorInfo* pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStorageAPI*             pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SSDataBlock*             pBlock = pInfo->pSrcDelBlock;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  SResultRowData   prev = {0};
  SResultRowData   next = {0};
  
  taosArrayClearP(pFillSup->pResultRange, NULL);
  while (pInfo->srcDelRowIndex < pBlock->info.rows) {
    TSKEY    curDelTs = tsStarts[pInfo->srcDelRowIndex];
    uint64_t curDelGroupId = groupIds[pInfo->srcDelRowIndex];
    SWinKey  curDelKey = {.ts = curDelTs, .groupId = curDelGroupId};

    resetTimeSlicePrevAndNextWindow(pFillSup);
    getResultInfoFromResult(&pInfo->nbSup.recParam, pFillSup, &curDelKey);
    prev = pFillSup->prev;
    next = pFillSup->next;

    pInfo->srcDelRowIndex++;

    while (pInfo->srcDelRowIndex < pBlock->info.rows) {
      TSKEY    nextDelTs = tsStarts[pInfo->srcDelRowIndex];
      uint64_t nextGroupId = groupIds[pInfo->srcDelRowIndex];
      if (curDelGroupId != nextGroupId || next.key < nextDelTs) {
        break;
      }
      taosArrayClear(pFillSup->pResultRange);
      curDelKey.ts = nextDelTs;
      getResultInfoFromResult(&pInfo->nbSup.recParam, pFillSup, &curDelKey);
      void* pPrevRowData = taosArrayGetP(pFillSup->pResultRange, 0);
      taosMemFreeClear(pPrevRowData);
      if (hasNextWindow(pFillSup)) {
        next = pFillSup->next;
      }
      pInfo->srcDelRowIndex++;
    }

    if (hasNextWindow(pFillSup)) {
      STimeFillRange tw = {
          .skey = prev.key,
          .ekey = next.key,
          .groupId = curDelGroupId,
          .pStartRow = prev.pRowVal,
          .pEndRow = next.pRowVal,
      };
      void* tmpRes = taosArrayPush(pInfo->pFillInfo->delRanges, &tw);
      QUERY_CHECK_NULL(tmpRes, code, lino, _end, terrno);
    } else {
      code = buildDeleteResult(pOperator, prev.key, next.key, curDelGroupId, pInfo->pDelRes);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
  }
  return code;
}

static void doFillDeleteRange(SStreamFillInfo* pFillInfo, SStreamFillSupporter* pFillSup, SSDataBlock* pBlock) {
  int32_t size = taosArrayGetSize(pFillInfo->delRanges);
  while (pFillInfo->delIndex < size) {
    STimeFillRange* range = taosArrayGet(pFillInfo->delRanges, pFillInfo->delIndex);
    if (pBlock->info.id.groupId != 0 && pBlock->info.id.groupId != range->groupId) {
      return;
    }
    pFillSup->prev.key = range->skey;
    pFillSup->prev.pRowVal = range->pStartRow;
    pFillSup->next.key = range->ekey;
    pFillSup->next.pRowVal = range->pEndRow;

    setDeleteFillValueInfo(range->skey, range->ekey, pFillSup, pFillInfo);
    pFillInfo->delIndex++;
    if (pFillInfo->needFill) {
      doStreamFillRange(pFillInfo, pFillSup, pBlock);
      pBlock->info.id.groupId = range->groupId;
    }
  }
}

int32_t initFillSupRowInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;

  code = initOffsetInfo(&pFillSup->pOffsetInfo, pRes);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t  numOfCol = taosArrayGetSize(pRes->pDataBlock);
  pFillSup->pEmptyRow = taosMemoryCalloc(1, pFillSup->rowSize);
  QUERY_CHECK_NULL(pFillSup->pEmptyRow, code, lino, _end, lino);

  for (int32_t i = 0; i < numOfCol; i++) {
    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, i);
    int32_t bytes = 1;
    int32_t type = 1;
    if (pColInfo != NULL) {
      bytes = pColInfo->info.bytes;
      type = pColInfo->info.type;
    }
    SResultCellData* pCell = getSliceResultCell(pFillSup->pEmptyRow, i, pFillSup->pOffsetInfo);
    pCell->bytes = bytes;
    pCell->type = type;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doStreamNonblockFillNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    if (isRecalculateOperator(&pInfo->basic)) {
      blockDataCleanup(pInfo->pRes);
      code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
      QUERY_CHECK_CODE(code, lino, _end);
      doFillDeleteRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      if (pInfo->pRes->info.rows > 0) {
        printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pRes;
        return code;
      }
    }

    if (hasRemainCalc(pInfo->pFillInfo) ||
        (pInfo->pFillInfo->pos != FILL_POS_INVALID && pInfo->pFillInfo->needFill == true)) {
      blockDataCleanup(pInfo->pRes);
      doStreamTimeSliceFillRange(pInfo->pFillSup, pInfo->pFillInfo, pInfo->pRes);
      if (pInfo->pRes->info.rows > 0) {
        printDataBlock(pInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        (*ppRes) = pInfo->pRes;
        goto _end;
      }
    }

    SSDataBlock* resBlock = NULL;
    code = buildNonblockFillResult(pOperator, &resBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (resBlock != NULL) {
      (*ppRes) = resBlock;
      goto _end;
    }

    pInfo->stateStore.streamStateClearExpiredState(pInfo->pState, pInfo->nbSup.numOfKeep, pInfo->nbSup.tsOfKeep);
    resetStreamFillInfo(pInfo);
    setStreamOperatorCompleted(pOperator);
    (*ppRes) = NULL;
    goto _end;
  }

  SSDataBlock*   fillResult = NULL;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      pOperator->status = OP_RES_TO_RETURN;
      qDebug("===stream===%s return data:%s.", GET_TASKID(pTaskInfo), getStreamOpName(pOperator->operatorType));
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    setStreamOperatorState(&pInfo->basic, pBlock->info.type);

    switch (pBlock->info.type) {
      case STREAM_PULL_DATA:
      case STREAM_NORMAL:
      case STREAM_INVALID: {
        code = doApplyStreamScalarCalculation(pOperator, pBlock, pInfo->pSrcBlock);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->srcRowIndex = -1;
      } break;
      case STREAM_CHECKPOINT: {
        pInfo->stateStore.streamStateCommit(pInfo->pState);
        (*ppRes) = pBlock;
        goto _end;
      } break;
      case STREAM_DELETE_RESULT: {
        if (!isRecalculateOperator(&pInfo->basic)) {
          qDebug("===stream===%s ignore recv block. type:%d", GET_TASKID(pTaskInfo), pBlock->info.type);
          continue;
        }
        pInfo->pSrcDelBlock = pBlock;
        pInfo->srcDelRowIndex = 0;
        blockDataCleanup(pInfo->pDelRes);
        pInfo->pFillSup->hasDelete = true;
        code = doDeleteNonblockFillResult(pOperator);
        QUERY_CHECK_CODE(code, lino, _end);

        if (pInfo->pDelRes->info.rows > 0) {
          printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
          (*ppRes) = pInfo->pDelRes;
          return code;
        }
        continue;
      } break;
      case STREAM_RETRIEVE:
      case STREAM_CREATE_CHILD_TABLE: {
        (*ppRes) = pBlock;
        goto _end;
      } break;
      default:
        qDebug("===stream===%s ignore recv block. type:%d", GET_TASKID(pTaskInfo), pBlock->info.type);
        continue;
    }

    code = doStreamNonblockFillImpl(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  removeDuplicateResult(pInfo->pUpdated, winKeyCmprImpl);

  initMultiResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->groupResInfo.freeItem = false;

  pInfo->pUpdated = taosArrayInit(1024, sizeof(SWinKey));
  QUERY_CHECK_NULL(pInfo->pUpdated, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  code = buildNonblockFillResult(pOperator, ppRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((*ppRes) == NULL) {
    pInfo->stateStore.streamStateClearExpiredState(pInfo->pState, pInfo->nbSup.numOfKeep, pInfo->nbSup.tsOfKeep);
    resetStreamFillInfo(pInfo);
    setStreamOperatorCompleted(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. task:%s", __func__, lino, tstrerror(code), GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
  }
  return code;
}

void destroyStreamNonblockFillOperatorInfo(void* param) {
  SStreamFillOperatorInfo* pInfo = (SStreamFillOperatorInfo*)param;
  resetTimeSlicePrevAndNextWindow(pInfo->pFillSup);
  destroyStreamFillOperatorInfo(param);
}

static int32_t doInitStreamColumnMapInfo(SExprSupp* pExprSup, SSHashObj* pColMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pOneExpr = &pExprSup->pExprInfo[i];
    int32_t    destSlotId = pOneExpr->base.resSchema.slotId;
    for (int32_t j = 0; j < pOneExpr->base.numOfParams; ++j) {
      SFunctParam* pFuncParam = &pOneExpr->base.pParam[j];
      if (pFuncParam->type == FUNC_PARAM_TYPE_COLUMN) {
        int32_t sourceSlotId = pFuncParam->pCol->slotId;
        code = tSimpleHashPut(pColMap, &sourceSlotId, sizeof(int32_t), &destSlotId, sizeof(int32_t));
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s.", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t initStreamFillOperatorColumnMapInfo(SExprSupp* pExprSup, SOperatorInfo* pOperator) {
  if (pOperator != NULL && pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL) {
    SStreamFillOperatorInfo* pInfo = (SStreamFillOperatorInfo*)pOperator->info;
    if (pInfo->nbSup.recParam.pColIdMap == NULL) {
      return TSDB_CODE_SUCCESS;
    }
    return doInitStreamColumnMapInfo(pExprSup, pInfo->nbSup.recParam.pColIdMap);
  }
  return TSDB_CODE_SUCCESS;
}
