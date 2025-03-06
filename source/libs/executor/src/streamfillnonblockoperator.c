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

    setTimeSliceFillRule(pFillSup, pFillInfo, pKey->ts);
    doStreamTimeSliceFillRange(pFillSup, pFillInfo, pBlock);
    pGroupResInfo->index++;
    if (pBlock->info.rows >= pBlock->info.capacity) {
      break;
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

        memcpy(pInfo->pSrcBlock->info.parTbName, pBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
        pInfo->srcRowIndex = -1;
      } break;
      case STREAM_CHECKPOINT: {
        pInfo->stateStore.streamStateCommit(pInfo->pState);
        (*ppRes) = pBlock;
        goto _end;
      } break;
      case STREAM_DELETE_RESULT: {
        // todo(liuyao) add
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