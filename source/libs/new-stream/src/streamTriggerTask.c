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

#include "streamTriggerTask.h"

#include "streamInt.h"
#include "tdatablock.h"
#include "ttime.h"

static TdThreadOnce gStreamTriggerModuleInit = PTHREAD_ONCE_INIT;
volatile int32_t    gStreamTriggerInitRes = TSDB_CODE_SUCCESS;
static tsem_t       gStreamTriggerCalcReqSem;

static void streamTriggerEnvDoInit() {
  gStreamTriggerInitRes = tsem_init(&gStreamTriggerCalcReqSem, 0, 10);  // todo(kjq): ajust the limit according to mnode
}

int32_t streamTriggerEnvInit() {
  int32_t code = taosThreadOnce(&gStreamTriggerModuleInit, streamTriggerEnvDoInit);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to init stream trigger module since %s", tstrerror(code));
    return code;
  }
  return gStreamTriggerInitRes;
}

void streamTriggerEnvCleanup() {
  int32_t code = tsem_destroy(&gStreamTriggerCalcReqSem);
  if (code != TSDB_CODE_SUCCESS) {
    stWarn("failed to destroy gTriggerCalcReqSem since %s", tstrerror(code));
  }
}

int32_t streamTriggerKickCalc() { return tsem_post(&gStreamTriggerCalcReqSem); }

static int32_t streamTriggerAcquireCalcReq() { return tsem_wait(&gStreamTriggerCalcReqSem); }

#define TRIGGER_META_SKEY_INACCURATE_MASK 0x01
#define TRIGGER_META_EKEY_INACCURATE_MASK 0x02
#define TRIGGER_META_DATA_EMPTY_MASK      0x04

#define SET_TRIGGER_META_SKEY_INACCURATE(pMeta)          \
  do {                                                   \
    if ((pMeta)->nrows >= 0) {                           \
      (pMeta)->nrows = INT64_MIN;                        \
    }                                                    \
    (pMeta)->nrows |= TRIGGER_META_SKEY_INACCURATE_MASK; \
  } while (0)

#define SET_TRIGGER_META_EKEY_INACCURATE(pMeta)          \
  do {                                                   \
    if ((pMeta)->nrows >= 0) {                           \
      (pMeta)->nrows = INT64_MIN;                        \
    }                                                    \
    (pMeta)->nrows |= TRIGGER_META_EKEY_INACCURATE_MASK; \
  } while (0)

#define SET_TRIGGER_META_DATA_EMPTY(pMeta)          \
  do {                                              \
    (pMeta)->nrows |= TRIGGER_META_DATA_EMPTY_MASK; \
  } while (0)

#define IS_TRIGGER_META_SKEY_INACCURATE(pMeta) \
  (((pMeta)->nrows < 0) && ((pMeta)->nrows & TRIGGER_META_SKEY_INACCURATE_MASK))
#define IS_TRIGGER_META_EKEY_INACCURATE(pMeta) \
  (((pMeta)->nrows < 0) && ((pMeta)->nrows & TRIGGER_META_EKEY_INACCURATE_MASK))
#define IS_TRIGGER_META_DATA_EMPTY(pMeta)      (((pMeta)->nrows < 0) && ((pMeta)->nrows & TRIGGER_META_DATA_EMPTY_MASK))
#define IS_TRIGGER_META_NROW_INACCURATE(pMeta) ((pMeta)->nrows < 0)

#define IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)         (taosArrayGetSize((pMerger)->pMetaNodeBuf) == 0)
#define SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger) pMerger->sessRange.ekey = INT64_MIN
#define IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger)  ((pMerger)->sessRange.ekey == INT64_MIN)
#define SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger) pMerger->dataReadRange.ekey = INT64_MIN
#define IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)  ((pMerger)->dataReadRange.ekey == INT64_MIN)

static int32_t stwmInit(SSTriggerWalMetaMerger *pMerger, SSTriggerRealtimeContext *pContext) {
  memset(pMerger, 0, sizeof(SSTriggerWalMetaMerger));
  pMerger->pContext = pContext;
  SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger);
  SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
  return TSDB_CODE_SUCCESS;
}

static void stwmClear(SSTriggerWalMetaMerger *pMerger) {
  if (pMerger->pMetaNodeBuf != NULL) {
    taosArrayClear(pMerger->pMetaNodeBuf);
  }
  if (pMerger->pMetaLists != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pMerger->pMetaLists); ++i) {
      SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayClear(pMerger->pMetaLists);
  }
  SET_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger);
  SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
}

static void stwmDestroy(SSTriggerWalMetaMerger **ppMerger) {
  if (ppMerger == NULL || *ppMerger == NULL) {
    return;
  }

  SSTriggerWalMetaMerger *pMerger = *ppMerger;
  if (pMerger->pMetaNodeBuf != NULL) {
    taosArrayDestroy(pMerger->pMetaNodeBuf);
  }
  if (pMerger->pMetaLists != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pMerger->pMetaLists); ++i) {
      SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayDestroy(pMerger->pMetaLists);
  }
  if (pMerger->pSessMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pSessMerger);
  }
  if (pMerger->pDataMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  taosMemoryFreeClear(*ppMerger);
}

static int32_t stwmSetWalMetas(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMeta *pMetas, int32_t nMetas) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pMerger->pMetaNodeBuf == NULL) {
    pMerger->pMetaNodeBuf = taosArrayInit(nMetas, sizeof(SSTriggerWalMetaNode));
    QUERY_CHECK_NULL(pMerger->pMetaNodeBuf, code, lino, _end, terrno);
  } else {
    code = taosArrayEnsureCap(pMerger->pMetaNodeBuf, nMetas);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  TARRAY_SIZE(pMerger->pMetaNodeBuf) = nMetas;
  if (pMerger->pMetaLists == NULL) {
    pMerger->pMetaLists = taosArrayInit(0, sizeof(SSTriggerWalMetaList));
    QUERY_CHECK_NULL(pMerger->pMetaLists, code, lino, _end, terrno);
  }
  TARRAY_SIZE(pMerger->pMetaLists) = 0;

  for (int32_t i = 0; i < nMetas; ++i) {
    SSTriggerWalMetaNode *pNode = TARRAY_GET_ELEM(pMerger->pMetaNodeBuf, i);
    pNode->pMeta = &pMetas[i];
    pNode->next = NULL;
    int32_t j = 0;
    while (j < TARRAY_SIZE(pMerger->pMetaLists)) {
      SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, j);
      if (pNode->pMeta->skey > pList->tail->pMeta->ekey) {
        pList->tail->next = pNode;
        pList->tail = pNode;
        break;
      }
      ++j;
    }
    if (j >= TARRAY_SIZE(pMerger->pMetaLists)) {
      SSTriggerWalMetaList list = {.head = pNode, .tail = pNode};
      void                *px = taosArrayPush(pMerger->pMetaLists, &list);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmMetaListCompareSess(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pMetaLists = (SArray *)param;

  if (left < TARRAY_SIZE(pMetaLists) && right < TARRAY_SIZE(pMetaLists)) {
    SSTriggerWalMetaList *pLeft = TARRAY_GET_ELEM(pMetaLists, left);
    SSTriggerWalMetaList *pRight = TARRAY_GET_ELEM(pMetaLists, right);
    if (pLeft->curSessWin.skey < pRight->curSessWin.skey) {
      return -1;
    } else if (pLeft->curSessWin.skey > pRight->curSessWin.skey) {
      return 1;
    } else if (pLeft->curSessWin.ekey != pRight->curSessWin.ekey) {
      return pLeft->curSessWin.ekey - pRight->curSessWin.ekey;
    }
  }
  return left - right;
}

static int32_t stwmMetaListNextSess(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int64_t gap) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  int64_t                   globalEnd = pMerger->sessRange.ekey;

  int64_t               start = pList->curSessWin.ekey + 1;
  SSTriggerWalMetaNode *pSessNode = pList->nextSessNode;
  while (pSessNode != NULL) {
    if (pSessNode->pMeta->ekey < start) {
      pSessNode = pSessNode->next;
      continue;
    }
    if (pSessNode->pMeta->skey > globalEnd) {
      pSessNode = NULL;
      break;
    }
    if (pSessNode->pMeta->skey >= start && !IS_TRIGGER_META_SKEY_INACCURATE(pSessNode->pMeta)) {
      start = pSessNode->pMeta->skey;
      break;
    }
    if (pSessNode->pMeta->ekey <= globalEnd && !IS_TRIGGER_META_EKEY_INACCURATE(pSessNode->pMeta)) {
      start = pSessNode->pMeta->ekey;
      break;
    }
    pSessNode = pSessNode->next;
  }

  if (pSessNode == NULL) {
    pList->nextSessNode = NULL;
    pList->curSessWin = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MAX};
    goto _end;
  }

  int64_t end = start;
  while (pSessNode != NULL) {
    if (pSessNode->pMeta->ekey <= end) {
      pSessNode = pSessNode->next;
      continue;
    }
    if (pSessNode->pMeta->skey > globalEnd) {
      pSessNode = NULL;
      break;
    }
    if (pSessNode->pMeta->skey > end && !IS_TRIGGER_META_SKEY_INACCURATE(pSessNode->pMeta)) {
      if (pSessNode->pMeta->skey <= end + gap) {
        end = pSessNode->pMeta->skey;
      } else {
        break;
      }
    }
    if (pSessNode->pMeta->ekey <= globalEnd && !IS_TRIGGER_META_EKEY_INACCURATE(pSessNode->pMeta)) {
      if (pSessNode->pMeta->ekey <= end + gap) {
        end = pSessNode->pMeta->ekey;
      } else {
        break;
      }
    }
    pSessNode = pSessNode->next;
  }

  pList->nextSessNode = pSessNode;
  pList->curSessWin = (STimeWindow){.skey = start, .ekey = end};

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBuildSessMerger(SSTriggerWalMetaMerger *pMerger, int64_t skey, int64_t ekey, int64_t gap) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  pMerger->sessRange = (STimeWindow){.skey = skey, .ekey = ekey};

  int32_t numList = TARRAY_SIZE(pMerger->pMetaLists);
  for (int32_t i = 0; i < numList; ++i) {
    SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, i);
    pList->nextSessNode = pList->head;
    pList->curSessWin = (STimeWindow){.skey = INT64_MIN, .ekey = skey - 1};
    code = stwmMetaListNextSess(pMerger, pList, gap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->pSessMerger && pMerger->pSessMerger->numOfSources < numList) {
    tMergeTreeDestroy(&pMerger->pSessMerger);
  }
  if (pMerger->pSessMerger == NULL) {
    int capacity = (numList + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pSessMerger, capacity, pMerger->pMetaLists, stwmMetaListCompareSess);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pSessMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSTriggerWalMetaList *stwmGetSessWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t idx = tMergeTreeGetChosenIndex(pMerger->pSessMerger);
  return TARRAY_GET_ELEM(pMerger->pMetaLists, idx);
}

static int32_t stwmAdjustSessWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_SESS_MERGER_INVALID(pMerger)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t idx = tMergeTreeGetAdjustIndex(pMerger->pSessMerger);
  return tMergeTreeAdjust(pMerger->pSessMerger, idx);
}

static int32_t stwmMetaListCompareData(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pMetaLists = (SArray *)param;

  if (left < TARRAY_SIZE(pMetaLists) && right < TARRAY_SIZE(pMetaLists)) {
    SSTriggerWalMetaList *pLeft = TARRAY_GET_ELEM(pMetaLists, left);
    SSTriggerWalMetaList *pRight = TARRAY_GET_ELEM(pMetaLists, right);
    if (pLeft->nextTs < pRight->nextTs) {
      return -1;
    } else if (pLeft->nextTs > pRight->nextTs) {
      return 1;
    } else if (pLeft->nextTs != INT64_MAX) {
      int64_t verLeft = pLeft->head->pMeta->ver;
      int64_t verRight = pRight->head->pMeta->ver;
      return verRight - verLeft;
    }
  }
  return left - right;
}

static int32_t stwmMetaListSkip2Ts(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int64_t start) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  while (pList->head != NULL) {
    if (pList->head->pMeta->ekey >= start) {
      break;
    }
    pList->head = pList->head->next;
    pList->nextIdx = -1;
    if (pList->pDataBlock != NULL) {
      blockDataDestroy(pList->pDataBlock);
      pList->pDataBlock = NULL;
    }
  }

  if (pList->head == NULL) {
    pList->nextTs = INT64_MAX;
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    pList->nextTs = TMAX(pList->head->pMeta->skey, start);
  } else {
    int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
    SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pTask->primaryTsIndex);
    while (pList->nextIdx < nrows) {
      int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, pList->nextIdx);
      if (ts >= start) {
        pList->nextTs = ts;
        break;
      }
      ++pList->nextIdx;
    }
    QUERY_CHECK_CONDITION(pList->nextIdx < nrows, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBuildDataMerger(SSTriggerWalMetaMerger *pMerger, int64_t skey, int64_t ekey) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (TARRAY_SIZE(pMerger->pMetaLists) <= 0 || skey > ekey) {
    SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
    goto _end;
  }

  pMerger->dataReadRange = (STimeWindow){.skey = skey, .ekey = ekey};

  int32_t numList = TARRAY_SIZE(pMerger->pMetaLists);
  for (int32_t i = 0; i < numList; ++i) {
    SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, i);
    code = stwmMetaListSkip2Ts(pMerger, pList, skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->pDataMerger && pMerger->pDataMerger->numOfSources < numList) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  if (pMerger->pDataMerger == NULL) {
    int capacity = (numList + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pDataMerger, capacity, pMerger->pMetaLists, stwmMetaListCompareData);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSTriggerWalMetaList *stwmGetDataWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
    return NULL;
  }

  int32_t               idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, idx);

  if (pList->nextTs > pMerger->dataReadRange.ekey) {
    SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
    return NULL;
  }

  // set the reading range for winner to avoid row-by-row comparison
  if (TARRAY_SIZE(pMerger->pMetaLists) == 1) {
    pMerger->stepReadRange = pMerger->dataReadRange;
  } else {
    int32_t               idx2 = pMerger->pDataMerger->pNode[1].index;
    SSTriggerWalMetaList *pList2 = TARRAY_GET_ELEM(pMerger->pMetaLists, idx2);
    pMerger->stepReadRange.skey = pMerger->dataReadRange.skey;
    if (pList->nextTs == pList2->nextTs) {
      pMerger->stepReadRange.ekey = pList->nextTs;
    } else {
      pMerger->stepReadRange.ekey = TMIN(pList2->nextTs - 1, pMerger->dataReadRange.ekey);
    }
  }
  return pList;
}

static int32_t stwmAdjustDataWinner(SSTriggerWalMetaMerger *pMerger) {
  if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t idx = tMergeTreeGetAdjustIndex(pMerger->pDataMerger);
  return tMergeTreeAdjust(pMerger->pDataMerger, idx);
}

static int32_t stwmMetaListNextData(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList,
                                    SSDataBlock **ppDataBlock, int32_t *pStartIdx, int32_t *pEndIdx, bool *pNeedFetch,
                                    bool *pNeddFree) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  *ppDataBlock = NULL;
  *pNeedFetch = false;
  *pNeddFree = false;

  if (pList->nextTs < pMerger->stepReadRange.skey) {
    code = stwmMetaListSkip2Ts(pMerger, pList, pMerger->stepReadRange.skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pList->nextTs > pMerger->stepReadRange.ekey) {
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    *pNeedFetch = true;
    goto _end;
  }

  QUERY_CHECK_CONDITION(pList->nextIdx >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
  SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pTask->primaryTsIndex);
  *ppDataBlock = pList->pDataBlock;
  *pStartIdx = pList->nextIdx;
  *pEndIdx = (*pStartIdx) + 1;
  while (*pEndIdx < nrows) {
    int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, *pEndIdx);
    if (ts > pMerger->stepReadRange.ekey) {
      break;
    }
    ++(*pEndIdx);
  }

  int64_t lastTs = *(int64_t *)colDataGetNumData(pTsCol, (*pEndIdx) - 1);
  pMerger->dataReadRange.skey = lastTs + 1;

  if (*pEndIdx >= nrows) {
    pList->head = pList->head->next;
    pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
    pList->nextIdx = -1;
    pList->pDataBlock = NULL;
    *pNeddFree = true;
  } else {
    pList->nextTs = *(int64_t *)colDataGetNumData(pTsCol, *pEndIdx);
    pList->nextIdx = *pEndIdx;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmBindDataBlock(SSTriggerWalMetaMerger *pMerger, SSDataBlock *pDataBlock) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t               idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SSTriggerWalMetaList *pList = TARRAY_GET_ELEM(pMerger->pMetaLists, idx);
  QUERY_CHECK_CONDITION(pList->pDataBlock == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pList->head, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nrows = blockDataGetNumOfRows(pDataBlock);
  if (nrows <= 0) {
    SET_TRIGGER_META_DATA_EMPTY(pList->head->pMeta);

    pList->head = pList->head->next;
    pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
    pList->nextIdx = -1;
    pList->pDataBlock = NULL;
    blockDataDestroy(pDataBlock);
  } else {
    SColumnInfoData  *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->primaryTsIndex);
    SSTriggerWalMeta *pMeta = pList->head->pMeta;
    if (IS_TRIGGER_META_NROW_INACCURATE(pMeta)) {
      // update accurate meta info, which helps with subsequent data merging
      int64_t skey = *(int64_t *)colDataGetNumData(pTsCol, 0);
      int64_t ekey = *(int64_t *)colDataGetNumData(pTsCol, nrows - 1);
      QUERY_CHECK_CONDITION(skey >= pMeta->skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      QUERY_CHECK_CONDITION(ekey <= pMeta->ekey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pMeta->skey = skey;
      pMeta->ekey = ekey;
      pMeta->nrows = nrows;
    }

    pList->pDataBlock = pDataBlock;
    for (pList->nextIdx = 0; pList->nextIdx < nrows; ++pList->nextIdx) {
      int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, pList->nextIdx);
      if (ts >= pList->nextTs) {
        pList->nextTs = ts;
        break;
      }
    }
    QUERY_CHECK_CONDITION(pList->nextIdx < nrows, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stwmMetaListSkipNrow(SSTriggerWalMetaMerger *pMerger, SSTriggerWalMetaList *pList, int32_t nrowsToSkip,
                                    int32_t *pSkipped, int64_t *pLastTs, bool *pNeedFetch) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pMerger->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  *pSkipped = 0;
  *pNeedFetch = false;
  *pLastTs = INT64_MIN;

  if (pList->nextTs < pMerger->stepReadRange.skey) {
    code = stwmMetaListSkip2Ts(pMerger, pList, pMerger->stepReadRange.skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pList->nextTs > pMerger->stepReadRange.ekey || nrowsToSkip <= 0) {
    goto _end;
  }

  if (pList->pDataBlock != NULL) {
    int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
    SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pTask->primaryTsIndex);
    int32_t          nextIdx = pList->nextIdx;
    while (nextIdx < nrows && *pSkipped < nrowsToSkip) {
      int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, nextIdx);
      if (ts > pMerger->stepReadRange.ekey) {
        break;
      }
      ++nextIdx;
      ++(*pSkipped);
    }

    int64_t lastTs = *(int64_t *)colDataGetNumData(pTsCol, nextIdx - 1);
    pMerger->dataReadRange.skey = lastTs + 1;
    *pLastTs = lastTs;

    if (nextIdx >= nrows) {
      pList->head = pList->head->next;
      pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
      pList->nextIdx = -1;
      blockDataDestroy(pList->pDataBlock);
      pList->pDataBlock = NULL;
    } else {
      pList->nextTs = *(int64_t *)colDataGetNumData(pTsCol, nextIdx);
      pList->nextIdx = nextIdx;
    }
  } else {
    SSTriggerWalMeta *pMeta = pList->head->pMeta;
    if (!IS_TRIGGER_META_NROW_INACCURATE(pMeta) && pMeta->ekey <= pMerger->stepReadRange.ekey &&
        pMeta->nrows <= nrowsToSkip) {
      *pSkipped = pMeta->nrows;

      int64_t lastTs = pMeta->ekey;
      pMerger->dataReadRange.skey = lastTs + 1;
      *pLastTs = lastTs;

      pList->head = pList->head->next;
      pList->nextTs = (pList->head == NULL) ? INT64_MAX : pList->head->pMeta->skey;
      pList->nextIdx = -1;
      pList->pDataBlock = NULL;
    } else {
      *pNeedFetch = false;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgInit(SSTriggerRealtimeGroup *pGroup, struct SSTriggerRealtimeContext *pContext, int64_t groupId) {
  memset(pGroup, 0, sizeof(SSTriggerRealtimeGroup));
  pGroup->pContext = pContext;
  pGroup->groupId = groupId;
  pGroup->status = STRIGGER_GROUP_WAITING_META;
  pGroup->maxMetaDelta = 100;     // todo(kjq): adjust dynamically
  pGroup->minMetaThreshold = 10;  // todo(kjq): adjust dynamically
  pGroup->oldThreshold = INT64_MIN;
  pGroup->newThreshold = INT64_MIN;
  pGroup->pMetas = taosArrayInit(0, sizeof(SSTriggerWalMeta));
  if (pGroup->pMetas == NULL) {
    return terrno;
  }
  pGroup->curWindow = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  pGroup->winStatus = STRIGGER_WINDOW_INITIALIZED;
  if (pContext->pTask->triggerType == STREAM_TRIGGER_COUNT) {
    TRINGBUF_INIT(&pGroup->wstartBuf);
    int32_t cap = (pContext->pTask->windowCount / pContext->pTask->windowSliding) + 1;
    int32_t code = TRINGBUF_RESERVE(&pGroup->wstartBuf, cap);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SSTriggerWalMetaStat *strtgGetMetaStat(SSTriggerRealtimeGroup *pGroup, int64_t readerTaskId) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerWalMetaStat     *pStat = NULL;

  if (pTask->singleTableGroup) {
    if (pGroup->pMetaStat == NULL) {
      pGroup->pMetaStat = taosMemCalloc(1, sizeof(SSTriggerWalMetaStat));
      QUERY_CHECK_NULL(pGroup->pMetaStat, code, lino, _end, terrno);
      pGroup->pMetaStat->readerTaskId = readerTaskId;
      pGroup->pMetaStat->threshold = INT64_MIN;
    }
    pStat = pGroup->pMetaStat;
  } else {
    if (pGroup->pMetaStats == NULL) {
      pGroup->pMetaStats = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      QUERY_CHECK_NULL(pGroup->pMetaStats, code, lino, _end, terrno);
    }
    pStat = tSimpleHashGet(pGroup->pMetaStats, &readerTaskId, sizeof(int64_t));
    if (pStat == NULL) {
      SSTriggerWalMetaStat metaStat = {.readerTaskId = readerTaskId, .threshold = INT64_MIN};
      code = tSimpleHashPut(pGroup->pMetaStats, &readerTaskId, sizeof(int64_t), &metaStat, sizeof(metaStat));
      QUERY_CHECK_CODE(code, lino, _end);
      pStat = tSimpleHashGet(pGroup->pMetaStats, &readerTaskId, sizeof(int64_t));
      QUERY_CHECK_NULL(pStat, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    }
  }
  QUERY_CHECK_CONDITION(pStat->readerTaskId == readerTaskId, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    pStat = NULL;
  }
  return pStat;
}

static int32_t strtgDelMetaInRange(SSTriggerRealtimeGroup *pGroup, int64_t uid, int64_t skey, int64_t ekey,
                                   int32_t *pNumAdded) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  *pNumAdded = 0;

  int32_t origSize = TARRAY_SIZE(pGroup->pMetas);
  if (pGroup->metaIdx >= origSize) {
    goto _end;
  }

  for (int32_t i = pGroup->metaIdx; i < TARRAY_SIZE(pGroup->pMetas); ++i) {
    SSTriggerWalMeta *pMeta = TARRAY_GET_ELEM(pGroup->pMetas, i);
    if (pMeta->uid != uid || pMeta->skey > ekey || pMeta->ekey < skey) {
      continue;
    } else if (pMeta->skey >= skey) {
      pMeta->skey = ekey + 1;
      SET_TRIGGER_META_SKEY_INACCURATE(pMeta);
    } else if (pMeta->ekey <= ekey) {
      pMeta->ekey = skey - 1;
      SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
    } else {
      SSTriggerWalMeta *px = taosArrayPush(pGroup->pMetas, pMeta);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pMeta->ekey = skey - 1;
      SET_TRIGGER_META_EKEY_INACCURATE(pMeta);
      px->skey = ekey + 1;
      SET_TRIGGER_META_SKEY_INACCURATE(px);
      (*pNumAdded)++;
    }
  }

  int32_t j = pGroup->metaIdx;
  for (int32_t i = pGroup->metaIdx; i < TARRAY_SIZE(pGroup->pMetas); ++i) {
    SSTriggerWalMeta *pMeta = TARRAY_GET_ELEM(pGroup->pMetas, i);
    if (pMeta->skey > pMeta->ekey) {
      continue;
    }
    if (i != j) {
      SSTriggerWalMeta *px = TARRAY_GET_ELEM(pGroup->pMetas, j);
      *px = *pMeta;
    }
    ++j;
  }
  TARRAY_SIZE(pGroup->pMetas) = j;
  *pNumAdded = j - origSize;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgAddNewMeta(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSDataBlock              *pWalMetaData = pContext->pWalMetaData;

  QUERY_CHECK_NULL(pWalMetaData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(blockDataGetNumOfCols(pWalMetaData) == 7, code, lino, _end, TSDB_CODE_INVALID_PARA);

  SStreamTaskAddr *pReader = taosArrayGet(pTask->readerList, pContext->curReaderIdx);
  QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);

  SSTriggerWalMetaStat *pStat = strtgGetMetaStat(pGroup, pReader->taskId);
  QUERY_CHECK_NULL(pStat, code, lino, _end, terrno);

  int32_t          numNewMeta = blockDataGetNumOfRows(pWalMetaData);
  int32_t          iCol = 0;
  SColumnInfoData *pGidCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pUidCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pSkeyCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pEkeyCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pVerCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pNrowsCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);
  SColumnInfoData *pTypeCol = taosArrayGet(pWalMetaData->pDataBlock, iCol++);

  for (int32_t i = 0; i < numNewMeta; ++i) {
    // todo(kjq): fix the check condition for virtual tables
    int64_t gid = *(int64_t *)colDataGetNumData(pGidCol, i);
    bool    isValid = (gid == pGroup->groupId);
    if (isValid) {
      int64_t skey = *(int64_t *)colDataGetNumData(pSkeyCol, i);
      int64_t ekey = *(int64_t *)colDataGetNumData(pEkeyCol, i);
      if (skey <= pStat->threshold) {
        code = stTriggerTaskMarkRecalc(pTask, pGroup->groupId, skey, TMIN(ekey, pStat->threshold));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (ekey > pStat->threshold) {
        skey = TMAX(skey, pStat->threshold + 1);
        int64_t uid = *(int64_t *)colDataGetNumData(pUidCol, i);
        int8_t  type = *(int8_t *)colDataGetNumData(pTypeCol, i);
        if (type == 0) {
          int32_t numAdded = 0;
          code = strtgDelMetaInRange(pGroup, uid, skey, ekey, &numAdded);
          QUERY_CHECK_CODE(code, lino, _end);
          pStat->numHoldMetas += numAdded;
        } else if (type == 1) {
          SSTriggerWalMeta *pMeta = taosArrayReserve(pGroup->pMetas, 1);
          QUERY_CHECK_NULL(pMeta, code, lino, _end, terrno);
          pMeta->uid = uid;
          pMeta->skey = skey;
          pMeta->ekey = ekey;
          pMeta->ver = *(int64_t *)colDataGetNumData(pVerCol, i);
          if (skey != *(int64_t *)colDataGetNumData(pSkeyCol, i)) {
            SET_TRIGGER_META_SKEY_INACCURATE(pMeta);
          } else {
            pMeta->nrows = *(int32_t *)colDataGetNumData(pNrowsCol, i);
          }
          pStat->threshold = TMAX(pStat->threshold, ekey - pTask->watermark);
          pStat->numHoldMetas++;
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgUpdateThreshold(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  if (pTask->singleTableGroup) {
    QUERY_CHECK_NULL(pGroup->pMetaStat, code, lino, _end, TSDB_CODE_INVALID_PARA);
    if (pGroup->pMetaStat->numHoldMetas > 0) {
      pGroup->newThreshold = pGroup->pMetaStat->threshold;
    }
  } else {
    QUERY_CHECK_NULL(pGroup->pMetaStats, code, lino, _end, TSDB_CODE_INVALID_PARA);
    int32_t               iter = 0;
    SSTriggerWalMetaStat *pStat = tSimpleHashIterate(pGroup->pMetaStats, NULL, &iter);
    int32_t               maxNumHold = 0;
    while (pStat != NULL) {
      maxNumHold = TMAX(maxNumHold, pStat->numHoldMetas);
      pStat = tSimpleHashIterate(pGroup->pMetaStats, pStat, &iter);
    }
    if (maxNumHold == 0) {
      goto _end;
    }
    int32_t numHoldThreshold = maxNumHold - pGroup->maxMetaDelta;
    bool    holdAllVnodes = (tSimpleHashGetSize(pGroup->pMetaStats) == taosArrayGetSize(pTask->readerList));
    if (numHoldThreshold > 0 || holdAllVnodes) {
      pGroup->newThreshold = INT64_MAX;
      pStat = tSimpleHashIterate(pGroup->pMetaStats, NULL, &iter);
      while (pStat != NULL) {
        if (pStat->numHoldMetas >= numHoldThreshold) {
          pGroup->newThreshold = TMIN(pGroup->newThreshold, pStat->threshold);
        }
        pStat = tSimpleHashIterate(pGroup->pMetaStats, pStat, &iter);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgCompareMeta(const void *pLeft, const void *pRight) {
  const SSTriggerWalMeta *left = pLeft;
  const SSTriggerWalMeta *right = pRight;
  if (left->ekey < right->ekey) {
    return -1;
  } else if (left->ekey > right->ekey) {
    return 1;
  } else {
    return 0;
  }
}

static int32_t strtgSearchMeta(const void *pLeft, const void *pRight) {
  int64_t                 watermark = *(int64_t *)pLeft;
  const SSTriggerWalMeta *right = pRight;
  if (watermark < right->ekey) {
    return -1;
  } else if (watermark > right->ekey) {
    return 1;
  } else {
    return 0;
  }
}

static STimeWindow strtgGetIntervalWindow(const SInterval *pInterval, int64_t ts) {
  STimeWindow win;
  win.skey = taosTimeTruncate(ts, pInterval);
  win.ekey = taosTimeGetIntervalEnd(win.skey, pInterval);
  if (win.ekey < win.skey) {
    win.ekey = INT64_MAX;
  }
  return win;
}

static void strtgNextIntervalWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  TSKEY nextStart =
      taosTimeAdd(pWindow->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->sliding, pInterval->slidingUnit, pInterval->precision, NULL);
  nextStart = taosTimeAdd(nextStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision, NULL);
  pWindow->skey = nextStart;
  pWindow->ekey = taosTimeAdd(nextStart, pInterval->interval, pInterval->intervalUnit, pInterval->precision, NULL);
}

static STimeWindow strtgGetPeriodWindow(const SInterval *pInterval, int64_t ts) {
  int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
  // truncate to the start of day
  SInterval interval = {.intervalUnit = 'd',
                        .slidingUnit = 'd',
                        .offsetUnit = pInterval->offsetUnit,
                        .precision = pInterval->precision,
                        .offset = 0};
  interval.interval = day;
  interval.sliding = day;
  int64_t     first = taosTimeTruncate(ts, &interval) + pInterval->offset;
  STimeWindow win;
  if (pInterval->sliding > day) {
    if (first > ts) {
      win.skey = first - pInterval->sliding;
      win.ekey = first - 1;
    } else {
      win.skey = first;
      win.ekey = first + pInterval->sliding - 1;
    }
  } else {
    if (first > ts) {
      int64_t prev = first - day;
      win.skey = (ts - prev) / pInterval->sliding * pInterval->sliding + prev;
      win.ekey = first - 1;
    } else {
      win.skey = (ts - first) / pInterval->sliding * pInterval->sliding + first;
      win.ekey = win.skey + pInterval->sliding - 1;
    }
  }
  return win;
}

static void strtgNextPeriodWindow(const SInterval *pInterval, STimeWindow *pWindow) {
  int64_t day = convertTimePrecision(24 * 60 * 60 * 1000, TSDB_TIME_PRECISION_MILLI, pInterval->precision);
  if (pInterval->sliding > day) {
    pWindow->skey += pInterval->sliding;
    pWindow->ekey += pInterval->sliding;
  } else {
    pWindow->skey = pWindow->ekey + 1;
    pWindow->ekey += pInterval->sliding;
    // truncate to the start of day
    SInterval interval = {.intervalUnit = 'd',
                          .slidingUnit = 'd',
                          .offsetUnit = pInterval->offsetUnit,
                          .precision = pInterval->precision,
                          .offset = 0};
    interval.interval = day;
    interval.sliding = day;
    int64_t first = taosTimeTruncate(pWindow->ekey + 1, &interval) + pInterval->offset;
    if (first > pWindow->skey && first <= pWindow->ekey) {
      pWindow->ekey = first - 1;
    }
  }
}

static int32_t strtgOpenNewWindow(SSTriggerRealtimeGroup *pGroup, int64_t ts) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  STimeWindow              *pWindow = &pGroup->curWindow;

  QUERY_CHECK_CONDITION(pGroup->winStatus != STRIGGER_WINDOW_OPENED, code, lino, _end, TSDB_CODE_INVALID_PARA);

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      SInterval *pInterval = &pTask->interval;
      if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
        *pWindow = strtgGetPeriodWindow(pInterval, ts);
      } else {
        QUERY_CHECK_CONDITION(pWindow->ekey < ts, code, lino, _end, TSDB_CODE_INVALID_PARA);
        strtgNextPeriodWindow(pInterval, pWindow);
      }
      break;
    }
    case STREAM_TRIGGER_SLIDING: {
      SInterval *pInterval = &pTask->interval;
      if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
        if (pInterval->interval > 0) {
          *pWindow = strtgGetIntervalWindow(pInterval, ts);
        } else {
          *pWindow = strtgGetPeriodWindow(pInterval, ts);
        }
      } else {
        QUERY_CHECK_CONDITION(pWindow->ekey < ts, code, lino, _end, TSDB_CODE_INVALID_PARA);
        if (pInterval->interval > 0) {
          strtgNextIntervalWindow(pInterval, pWindow);
        } else {
          strtgNextPeriodWindow(pInterval, pWindow);
        }
      }
      QUERY_CHECK_CONDITION(pWindow->skey <= ts && pWindow->ekey >= ts, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      break;
    }
    case STREAM_TRIGGER_SESSION:
    case STREAM_TRIGGER_COUNT:
    case STREAM_TRIGGER_STATE:
    case STREAM_TRIGGER_EVENT: {
      pWindow->skey = ts;
      pWindow->ekey = ts;
      break;
    }

    default: {
      ST_TASK_ELOG("invalid trigger type %d", pTask->triggerType);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  // todo(kjq): add calc and notify here
  pGroup->winStatus = STRIGGER_WINDOW_OPENED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgCloseCurrentWindow(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  STimeWindow              *pWindow = &pGroup->curWindow;

  if (pGroup->winStatus != STRIGGER_WINDOW_OPENED) {
    goto _end;
  }

  if (pTask->calcEventType & STRIGGER_EVENT_WINDOW_CLOSE) {
    SSTriggerCalcParam param = {
        .wstart = pWindow->skey,
        .wend = pWindow->ekey,
        .wduration = pWindow->ekey - pWindow->skey,
        .wrownum = (pTask->triggerType == STREAM_TRIGGER_COUNT) ? pTask->windowCount : pGroup->nrowsInWindow,
        .triggerTime = taosGetTimestampNs(),
        // todo(kjq): add extraNotifyContent here
    };
    void *px = taosArrayPush(pContext->calcReq.params, &param);
    QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  }

  pGroup->winStatus = STRIGGER_WINDOW_CLOSED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgDoCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;
  SSTriggerCalcRequest     *pReq = &pContext->calcReq;
  int64_t                   endtime = INT64_MIN;
  SSDataBlock              *pDataBlock = NULL;
  int32_t                   startIdx = 0;
  int32_t                   endIdx = 0;
  bool                      needFetch = false;
  bool                      needFree = false;

  QUERY_CHECK_CONDITION(pReq->gid == pGroup->groupId, code, lino, _end, TSDB_CODE_INVALID_PARA);

  switch (pTask->triggerType) {
    case STREAM_TRIGGER_PERIOD: {
      // todo(kjq): implement period trigger
    }
    case STREAM_TRIGGER_SLIDING: {
      int64_t ts = INT64_MIN;
      switch (pGroup->winStatus) {
        case STRIGGER_WINDOW_INITIALIZED: {
          SSTriggerWalMeta *pMeta = TARRAY_GET_ELEM(pGroup->pMetas, 0);
          ts = pMeta->skey;
          for (int32_t i = 0; i < TARRAY_SIZE(pGroup->pMetas); ++i) {
            ts = TMIN((pMeta + i)->skey, ts);
          }
          break;
        }
        case STRIGGER_WINDOW_OPENED: {
          ts = pGroup->curWindow.ekey;
          break;
        }
        case STRIGGER_WINDOW_CLOSED: {
          ts = pGroup->curWindow.ekey + 1;
          break;
        }
        default: {
          ST_TASK_ELOG("invalid window status %d", pGroup->winStatus);
          code = TSDB_CODE_INVALID_PARA;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      while (ts <= pGroup->newThreshold) {
        if (pGroup->winStatus == STRIGGER_WINDOW_OPENED) {
          code = strtgCloseCurrentWindow(pGroup);
          QUERY_CHECK_CODE(code, lino, _end);
          if (TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
            break;
          }
          ts = pGroup->curWindow.ekey + 1;
        } else {
          code = strtgOpenNewWindow(pGroup, ts);
          QUERY_CHECK_CODE(code, lino, _end);
          if (TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
            break;
          }
          ts = pGroup->curWindow.ekey;
        }
      }
      endtime = TMIN(pGroup->newThreshold, ts);
      break;
    }
    case STREAM_TRIGGER_SESSION: {
      SSTriggerWalMetaMerger *pMerger = pTask->pRealtimeCtx->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        // build session window merger
        SSTriggerWalMeta *pMetas = TARRAY_GET_ELEM(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = TARRAY_SIZE(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildSessMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold, pTask->gap);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      while (true) {
        int64_t nextStart = INT64_MAX;
        if (IS_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger)) {
          // use metadata to determine windows first, reducing the need for data fetches
          while (true) {
            SSTriggerWalMetaList *pSessWinner = stwmGetSessWinner(pMerger);
            QUERY_CHECK_NULL(pSessWinner, code, lino, _end, terrno);
            if (pGroup->curWindow.ekey + pTask->gap < pSessWinner->curSessWin.skey) {
              nextStart = pSessWinner->curSessWin.skey;
              break;
            } else {
              QUERY_CHECK_CONDITION(pGroup->winStatus == STRIGGER_WINDOW_OPENED, code, lino, _end,
                                    TSDB_CODE_INTERNAL_ERROR);
              pGroup->curWindow.ekey = TMAX(pGroup->curWindow.ekey, pSessWinner->curSessWin.ekey);
            }
            code = stwmMetaListNextSess(pMerger, pSessWinner, pTask->gap);
            QUERY_CHECK_CODE(code, lino, _end);
            code = stwmAdjustSessWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
          }

          code = stwmBuildDataMerger(pMerger, pGroup->curWindow.ekey + 1, TMIN(pGroup->newThreshold, nextStart - 1));
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          SSTriggerWalMetaList *pSessWinner = stwmGetSessWinner(pMerger);
          QUERY_CHECK_NULL(pSessWinner, code, lino, _end, terrno);
          nextStart = pSessWinner->curSessWin.skey;
        }

        bool hasRemain = false;

        while (true) {
          SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
          if (pDataWinner == NULL) {
            break;
          }
          while (true) {
            code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
            QUERY_CHECK_CODE(code, lino, _end);
            if (needFetch) {
              if (pGroup->curWindow.ekey + pTask->gap >= nextStart ||
                  TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
                SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
                hasRemain = true;
                break;
              }
              // todo(kjq): send WalTsDataRequest to vnode
              pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
              goto _end;
            }
            if (pDataBlock == NULL) {
              code = stwmAdjustDataWinner(pMerger);
              QUERY_CHECK_CODE(code, lino, _end);
              break;
            }
            SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->primaryTsIndex);
            for (int32_t i = startIdx; i < endIdx; ++i) {
              int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
              if (pGroup->curWindow.ekey + pTask->gap < ts) {
                code = strtgCloseCurrentWindow(pGroup);
                QUERY_CHECK_CODE(code, lino, _end);
                code = strtgOpenNewWindow(pGroup, ts);
                QUERY_CHECK_CODE(code, lino, _end);
              } else {
                QUERY_CHECK_CONDITION(pGroup->winStatus == STRIGGER_WINDOW_OPENED, code, lino, _end,
                                      TSDB_CODE_INTERNAL_ERROR);
                pGroup->curWindow.ekey = ts;
              }
            }
            if (needFree) {
              blockDataDestroy(pDataBlock);
              pDataBlock = NULL;
            }
          }
        }

        if (hasRemain) {
          if (pGroup->curWindow.ekey + pTask->gap >= nextStart) {
            // go back to use metadata to extend the window
            continue;
          } else {
            // calculation requests is full
            endtime = pGroup->curWindow.ekey;
            break;
          }
        } else {
          if (nextStart == INT64_MAX) {
            if (pGroup->curWindow.ekey + pTask->gap <= pGroup->newThreshold) {
              code = strtgCloseCurrentWindow(pGroup);
              QUERY_CHECK_CODE(code, lino, _end);
            }
            endtime = pGroup->newThreshold;
            break;
          } else {
            code = strtgCloseCurrentWindow(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
            code = strtgOpenNewWindow(pGroup, nextStart);
            QUERY_CHECK_CODE(code, lino, _end);
          }
        }
      }
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_COUNT: {
      SSTriggerWalMetaMerger *pMerger = pTask->pRealtimeCtx->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = TARRAY_GET_ELEM(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = TARRAY_SIZE(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          int32_t skipped = 0;
          int64_t lastTs = INT64_MIN;
          if (pGroup->nrowsInWindow >= pTask->windowCount) {
            code = strtgCloseCurrentWindow(pGroup);
            QUERY_CHECK_CODE(code, lino, _end);
            pGroup->nrowsInWindow -= pTask->windowSliding;
            if (TRINGBUF_IS_EMPTY(&pGroup->wstartBuf)) {
              int64_t wstart = TRINGBUF_FIRST(&pGroup->wstartBuf);
              TRINGBUF_DEQUEUE(&pGroup->wstartBuf);
              code = strtgOpenNewWindow(pGroup, wstart);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
#define ALIGN_UP(x, b) (((x) + (b) - 1) / (b) * (b))
            int32_t nrowsNextWstart = ALIGN_UP(pGroup->nrowsInWindow, pTask->windowSliding) + 1;
            int32_t nrowsToSkip = nrowsNextWstart - pGroup->nrowsInWindow;
            int32_t nrowsCurWend = pTask->windowCount;
            if (pTask->needWend) {
              nrowsToSkip = TMIN(nrowsToSkip, nrowsCurWend - pGroup->nrowsInWindow);
            }
            code = stwmMetaListSkipNrow(pMerger, pDataWinner, nrowsToSkip, &skipped, &lastTs, &needFetch);
            QUERY_CHECK_CODE(code, lino, _end);
            if (needFetch) {
              if (TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
                SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
                hasRemain = true;
                break;
              }
              // todo(kjq): send WalTsDataRequest to vnode
              pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
              goto _end;
            }
            if (skipped == 0) {
              code = stwmAdjustDataWinner(pMerger);
              QUERY_CHECK_CODE(code, lino, _end);
              break;
            }
            pGroup->nrowsInWindow += skipped;
            pGroup->curWindow.ekey = lastTs;
            if (pGroup->winStatus != STRIGGER_WINDOW_OPENED) {
              code = strtgOpenNewWindow(pGroup, lastTs);
              QUERY_CHECK_CODE(code, lino, _end);
            } else if (pGroup->nrowsInWindow == nrowsNextWstart) {
              code = TRINGBUF_APPEND(&pGroup->wstartBuf, lastTs);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      }

      endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_STATE: {
      SSTriggerWalMetaMerger *pMerger = pTask->pRealtimeCtx->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = TARRAY_GET_ELEM(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = TARRAY_SIZE(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
          QUERY_CHECK_CODE(code, lino, _end);
          if (needFetch) {
            if (TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
              SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
              hasRemain = true;
              break;
            }
            // todo(kjq): send WalTriggerDataRequest to vnode
            pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
            goto _end;
          }
          if (pDataBlock == NULL) {
            code = stwmAdjustDataWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
          SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->primaryTsIndex);
          SColumnInfoData *pStateCol = taosArrayGet(pDataBlock->pDataBlock, pTask->stateColId);
          SValue          *pStateVal = &pGroup->stateVal;
          bool             isVarType = IS_VAR_DATA_TYPE(pStateCol->info.type);
          if (pGroup->winStatus == STRIGGER_WINDOW_INITIALIZED) {
            // initialize state value
            pStateVal->type = pStateCol->info.type;
            char *val = colDataGetData(pStateCol, startIdx);
            if (isVarType) {
              pStateVal->nData = pStateCol->info.bytes;
              pStateVal->pData = taosMemoryCalloc(pStateVal->nData, 1);
              QUERY_CHECK_CONDITION(pStateVal->pData, code, lino, _end, terrno);
              varDataCopy(pStateVal->pData, val);
            } else {
              memcpy(pStateVal->pData, val, pStateCol->info.bytes);
            }
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, startIdx);
            code = strtgOpenNewWindow(pGroup, ts);
            QUERY_CHECK_CODE(code, lino, _end);
          }
          for (int32_t i = startIdx; i < endIdx; ++i) {
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
            char   *val = colDataGetData(pStateCol, i);
            int32_t bytes = isVarType ? varDataTLen(val) : pStateCol->info.bytes;
            if (memcmp(pStateCol->pData, val, bytes) == 0) {
              pGroup->curWindow.ekey = ts;
            } else {
              code = strtgCloseCurrentWindow(pGroup);
              QUERY_CHECK_CODE(code, lino, _end);
              code = strtgOpenNewWindow(pGroup, ts);
              QUERY_CHECK_CODE(code, lino, _end);
              memcpy(pStateCol->pData, val, bytes);
              pGroup->nrowsInWindow = 0;
            }
            pGroup->nrowsInWindow++;
          }
        }
      }

      endtime = hasRemain ? pGroup->curWindow.ekey : pGroup->newThreshold;
      stwmClear(pMerger);
      break;
    }
    case STREAM_TRIGGER_EVENT: {
      SSTriggerWalMetaMerger *pMerger = pTask->pRealtimeCtx->pMerger;
      if (IS_TRIGGER_WAL_META_MERGER_EMPTY(pMerger)) {
        SSTriggerWalMeta *pMetas = TARRAY_GET_ELEM(pGroup->pMetas, pGroup->metaIdx);
        int32_t           nMetas = TARRAY_SIZE(pGroup->pMetas) - pGroup->metaIdx;
        code = stwmSetWalMetas(pMerger, pMetas, nMetas);
        QUERY_CHECK_CODE(code, lino, _end);
        code = stwmBuildDataMerger(pMerger, pGroup->oldThreshold + 1, pGroup->newThreshold);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      bool hasRemain = false;

      while (true) {
        SSTriggerWalMetaList *pDataWinner = stwmGetDataWinner(pMerger);
        if (pDataWinner == NULL) {
          break;
        }
        while (true) {
          code = stwmMetaListNextData(pMerger, pDataWinner, &pDataBlock, &startIdx, &endIdx, &needFetch, &needFree);
          QUERY_CHECK_CODE(code, lino, _end);
          if (needFetch) {
            if (TARRAY_SIZE(pReq->params) >= pTask->calcParamLimit) {
              SET_TRIGGER_WAL_META_DATA_MERGER_INVALID(pMerger);
              hasRemain = true;
              break;
            }
            // todo(kjq): send WalTriggerDataRequest to vnode
            pGroup->status = STRIGGER_GROUP_WAITING_TDATA;
            goto _end;
          }
          if (pDataBlock == NULL) {
            code = stwmAdjustDataWinner(pMerger);
            QUERY_CHECK_CODE(code, lino, _end);
            break;
          }
          SColumnInfoData *ps = NULL, *pe = NULL;
          SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pTask->primaryTsIndex);
          for (int32_t i = startIdx; i < endIdx; ++i) {
            int64_t ts = *(int64_t *)colDataGetNumData(pTsCol, i);
            if (pGroup->winStatus == STRIGGER_WINDOW_OPENED) {
              pGroup->curWindow.ekey = ts;
              if (pe == NULL) {
                SFilterColumnParam param = {.numOfCols = TARRAY_SIZE(pDataBlock->pDataBlock),
                                            .pDataBlock = pDataBlock->pDataBlock};
                code = filterSetDataFromSlotId(pTask->pEndCond, &param);
                QUERY_CHECK_CODE(code, lino, _end);
                int32_t status = 0;
                code = filterExecute(pTask->pEndCond, pDataBlock, &pe, NULL, param.numOfCols, &status);
                QUERY_CHECK_CODE(code, lino, _end);
              }
              if (*(bool *)colDataGetNumData(pe, i)) {
                code = strtgCloseCurrentWindow(pGroup);
                QUERY_CHECK_CODE(code, lino, _end);
              }
            } else {
              if (ps == NULL) {
                SFilterColumnParam param = {.numOfCols = TARRAY_SIZE(pDataBlock->pDataBlock),
                                            .pDataBlock = pDataBlock->pDataBlock};
                code = filterSetDataFromSlotId(pTask->pStartCond, &param);
                QUERY_CHECK_CODE(code, lino, _end);
                int32_t status = 0;
                code = filterExecute(pTask->pStartCond, pDataBlock, &ps, NULL, param.numOfCols, &status);
              }
              if (*(bool *)colDataGetNumData(ps, i)) {
                code = strtgOpenNewWindow(pGroup, ts);
                QUERY_CHECK_CODE(code, lino, _end);
                pGroup->nrowsInWindow = 1;
              }
            }
          }
        }
      }
    }
  }

  pGroup->oldThreshold = endtime;
  pGroup->metaIdx = taosArraySearchIdx(pGroup->pMetas, &endtime, strtgSearchMeta, TD_GT);
  if (pGroup->metaIdx < 0) {
    pGroup->metaIdx = TARRAY_SIZE(pGroup->pMetas);
  }

  // if data caching is needed, metas will be held until send calculation request
  if (!pTask->needCacheData) {
    taosArrayPopFrontBatch(pGroup->pMetas, pGroup->metaIdx);
    pGroup->metaIdx = 0;
  }

  if (endtime == pGroup->newThreshold) {
    // all metas are checked, wait for more metas
    pGroup->status = STRIGGER_GROUP_WAITING_META;
  } else {
    // not enough metas to check, wait for calculation
    pGroup->status = STRIGGER_GROUP_WAITING_CALC;
  }

_end:
  if (pDataBlock != NULL && needFree) {
    blockDataDestroy(pDataBlock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strtgResumeCheck(SSTriggerRealtimeGroup *pGroup) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SSTriggerRealtimeContext *pContext = pGroup->pContext;
  SStreamTriggerTask       *pTask = pContext->pTask;

  switch (pGroup->status) {
    case STRIGGER_GROUP_WAITING_META: {
      code = strtgAddNewMeta(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);

      code = strtgUpdateThreshold(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pGroup->metaIdx < TARRAY_SIZE(pGroup->pMetas)) {
        SSTriggerWalMeta *pMetas = TARRAY_GET_ELEM(pGroup->pMetas, pGroup->metaIdx);
        taosSort(pMetas, TARRAY_SIZE(pGroup->pMetas) - pGroup->metaIdx, pGroup->pMetas->elemSize, strtgCompareMeta);
      }

      // don't break, continue to the next case
    }
    case STRIGGER_GROUP_WAITING_CALC: {
      int32_t numToCheck = taosArraySearchIdx(pGroup->pMetas, &pGroup->newThreshold, strtgSearchMeta, TD_GT);
      if (numToCheck < 0) {
        numToCheck = TARRAY_SIZE(pGroup->pMetas);
      }
      numToCheck -= pGroup->metaIdx;
      // todo(kjq):  check if the timeout expired
      if (numToCheck < pGroup->minMetaThreshold) {
        // not enough metas to check, wait for more metas
        pGroup->status = STRIGGER_GROUP_WAITING_META;
        goto _end;
      }

      if (pContext->calcStatus != STRIGGER_CALC_IDLE) {
        // calc is running, wait for it to finish
        pGroup->status = STRIGGER_GROUP_WAITING_CALC;
        goto _end;
      }
      SSTriggerCalcRequest *pReq = &pContext->calcReq;
      pReq->gid = pGroup->groupId;
      QUERY_CHECK_CONDITION(taosArrayGetSize(pReq->params) == 0, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      // wait until the thread can send calc request
      code = streamTriggerAcquireCalcReq();
      QUERY_CHECK_CODE(code, lino, _end);

      // don't break, continue to the next case
    }
    case STRIGGER_GROUP_WAITING_TDATA: {
      code = strtgDoCheck(pGroup);
      QUERY_CHECK_CODE(code, lino, _end);

      break;
    }
    defaut: {
      ST_TASK_ELOG("invalid group status %d", pGroup->status);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskMarkRecalc(SStreamTriggerTask *pTask, int64_t groupId, int64_t skey, int64_t ekey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): mark recalculation interval

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, const SStreamTriggerDeployMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): create stream trigger task

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskUndeploy(SStreamTriggerTask *pTask, const SStreamUndeployTaskMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): destroy stream trigger task

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // todo(kjq): start stream trigger task by sending request to reader

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
