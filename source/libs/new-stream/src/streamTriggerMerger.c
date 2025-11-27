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

#include "streamTriggerMerger.h"

#include "streamTriggerTask.h"
#include "tcompare.h"
#include "tdatablock.h"

typedef struct SSTriggerMetaDataNode {
  SSTriggerMetaData *pMeta;
  TD_DLIST_NODE(SSTriggerMetaDataNode);
} SSTriggerMetaDataNode;

typedef struct SSTriggerMetaDataList {
  TD_DLIST(SSTriggerMetaDataNode);

  SSDataBlock *pDataBlock;
  int32_t      startIdx;
  int32_t      endIdx;
  int64_t      nextTs;
} SSTriggerMetaDataList;

static int32_t stMergeTreeGetSecondIndex(SMultiwayMergeTreeInfo *pTree, int32_t *pIdx) {
  *pIdx = tMergeTreeGetAdjustIndex(pTree);
  if (pTree->totalSources == 2) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t   parentId = (*pIdx) >> 1;
  STreeNode kLeaf = pTree->pNode[parentId];
  parentId = parentId >> 1;
  while (parentId > 0) {
    STreeNode *pCur = &pTree->pNode[parentId];
    if (pCur->index == -1) {
      return TSDB_CODE_INVALID_PARA;
    }
    int32_t ret = pTree->comparFn(pCur, &kLeaf, pTree->param);
    if (ret < 0) {
      kLeaf = *pCur;
    }
    parentId = parentId >> 1;
  }
  *pIdx = kLeaf.index;
  return TSDB_CODE_SUCCESS;
}

int32_t stTimestampSorterInit(SSTriggerTimestampSorter *pSorter, SStreamTriggerTask *pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pSorter->pTask = pTask;
  pSorter->readRange = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};

  pSorter->pMetaNodeBuf = taosArrayInit(0, sizeof(SSTriggerMetaDataNode));
  QUERY_CHECK_NULL(pSorter->pMetaNodeBuf, code, lino, _end, terrno);

  pSorter->pMetaLists = taosArrayInit(0, sizeof(SSTriggerMetaDataList));
  QUERY_CHECK_NULL(pSorter->pMetaLists, code, lino, _end, terrno);

  pSorter->pSessWins = taosArrayInit(0, sizeof(STimeWindow));
  QUERY_CHECK_NULL(pSorter->pSessWins, code, lino, _end, terrno);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void stTimestampSorterDestroy(void *ptr) {
  SSTriggerTimestampSorter **ppSorter = ptr;
  if (ppSorter == NULL || *ppSorter == NULL) {
    return;
  }

  SSTriggerTimestampSorter *pSorter = *ppSorter;
  if (pSorter->pMetaNodeBuf != NULL) {
    taosArrayDestroy(pSorter->pMetaNodeBuf);
    pSorter->pMetaNodeBuf = NULL;
  }

  if (pSorter->pMetaLists != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pSorter->pMetaLists); i++) {
      SSTriggerMetaDataList *pList = TARRAY_GET_ELEM(pSorter->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayDestroy(pSorter->pMetaLists);
    pSorter->pMetaLists = NULL;
  }

  if (pSorter->pDataMerger != NULL) {
    tMergeTreeDestroy(&pSorter->pDataMerger);
  }

  if (pSorter->pSessWins != NULL) {
    taosArrayDestroy(pSorter->pSessWins);
    pSorter->pSessWins = NULL;
  }
  taosMemoryFreeClear(*ppSorter);
}

void stTimestampSorterReset(SSTriggerTimestampSorter *pSorter) {
  if (pSorter == NULL) {
    return;
  }

  pSorter->flags = 0;
  pSorter->readRange = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};

  if (pSorter->pMetaNodeBuf != NULL) {
    taosArrayClear(pSorter->pMetaNodeBuf);
  }

  if (pSorter->pMetaLists != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pSorter->pMetaLists); i++) {
      SSTriggerMetaDataList *pList = TARRAY_GET_ELEM(pSorter->pMetaLists, i);
      if (pList->pDataBlock != NULL) {
        blockDataDestroy(pList->pDataBlock);
        pList->pDataBlock = NULL;
      }
    }
    taosArrayClear(pSorter->pMetaLists);
  }

  if (pSorter->pSessWins != NULL) {
    taosArrayClear(pSorter->pSessWins);
  }
}

int32_t stTimestampSorterSetSortInfo(SSTriggerTimestampSorter *pSorter, STimeWindow *pRange, int64_t tbUid,
                                     int32_t tsSlotId) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;

  QUERY_CHECK_CONDITION(pSorter->flags == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pRange != NULL && pRange->skey <= pRange->ekey, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pSorter->readRange = *pRange;
  pSorter->tbUid = tbUid;
  pSorter->tsSlotId = tsSlotId;

  BIT_FLAG_SET_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_SORT_INFO_SET);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTimestampSorterSetMetaDatas(SSTriggerTimestampSorter *pSorter, SSTriggerTableMeta *pTableMeta) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;
  SArray             *pMetaNodeBuf = pSorter->pMetaNodeBuf;
  SArray             *pMetaLists = pSorter->pMetaLists;
  SArray             *pMetas = pTableMeta->pMetas;

  QUERY_CHECK_CONDITION(pSorter->flags == TRIGGER_TS_SORTER_MASK_SORT_INFO_SET, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pMetaNodeBuf != NULL && TARRAY_SIZE(pMetaNodeBuf) == 0, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pMetaLists != NULL && TARRAY_SIZE(pMetaLists) == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nMetas = taosArrayGetSize(pMetas);
  for (int32_t i = 0; i < nMetas; i++) {
    SSTriggerMetaData *pMeta = TARRAY_GET_ELEM(pMetas, i);
    if ((pMeta->skey > pSorter->readRange.ekey) || (pMeta->ekey < pSorter->readRange.skey) ||
        IS_TRIGGER_META_DATA_EMPTY(pMeta)) {
      continue;
    }

    SSTriggerMetaDataNode *pNode = taosArrayReserve(pMetaNodeBuf, 1);
    QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);
    pNode->pMeta = pMeta;
  }

  nMetas = TARRAY_SIZE(pMetaNodeBuf);
  for (int32_t i = 0; i < nMetas; i++) {
    SSTriggerMetaDataNode *pNode = TARRAY_GET_ELEM(pMetaNodeBuf, i);
    SSTriggerMetaDataList *pList = NULL;
    for (int32_t j = 0; j < TARRAY_SIZE(pMetaLists); j++) {
      SSTriggerMetaDataList *pCurList = TARRAY_GET_ELEM(pMetaLists, j);
      if (TD_DLIST_TAIL(pCurList) && TD_DLIST_TAIL(pCurList)->pMeta->ekey < pNode->pMeta->skey) {
        pList = pCurList;
        break;
      }
    }
    if (pList == NULL) {
      pList = taosArrayReserve(pMetaLists, 1);
      *pList = (SSTriggerMetaDataList){.nextTs = pNode->pMeta->skey};
      QUERY_CHECK_NULL(pList, code, lino, _end, terrno);
    }
    TD_DLIST_APPEND(pList, pNode);
  }

  BIT_FLAG_SET_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_META_DATA_SET);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTimestampSorterSetEmptyMetaDatas(SSTriggerTimestampSorter *pSorter) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;
  SArray             *pMetaNodeBuf = pSorter->pMetaNodeBuf;
  SArray             *pMetaLists = pSorter->pMetaLists;

  QUERY_CHECK_CONDITION(pSorter->flags == TRIGGER_TS_SORTER_MASK_SORT_INFO_SET, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pMetaNodeBuf != NULL && TARRAY_SIZE(pMetaNodeBuf) == 0, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pMetaLists != NULL && TARRAY_SIZE(pMetaLists) == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  SSTriggerMetaDataList *pList = taosArrayReserve(pMetaLists, 1);
  QUERY_CHECK_NULL(pList, code, lino, _end, terrno);
  *pList = (SSTriggerMetaDataList){.nextTs = INT64_MIN};

  BIT_FLAG_SET_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_NO_META_DATA);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static FORCE_INLINE void stTimestampSorterMetaListMoveForward(SSTriggerMetaDataList *pList) {
  SSTriggerMetaDataNode *pHead = TD_DLIST_HEAD(pList);
  if (pHead != NULL) {
    TD_DLIST_POP(pList, pHead);
  }
  if (pList->pDataBlock != NULL) {
    blockDataDestroy(pList->pDataBlock);
    pList->pDataBlock = NULL;
  }
  pList->startIdx = pList->endIdx = 0;
  pList->nextTs = (TD_DLIST_HEAD(pList) == NULL) ? INT64_MAX : TD_DLIST_HEAD(pList)->pMeta->skey;
}

static int32_t stTimestampSorterMetaListSkip2Ts(SSTriggerTimestampSorter *pSorter, SSTriggerMetaDataList *pList,
                                                int64_t ts) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;

  while (TD_DLIST_HEAD(pList) != NULL) {
    if (TD_DLIST_HEAD(pList)->pMeta->ekey >= ts) {
      break;  // found the first meta with ekey >= ts
    }
    stTimestampSorterMetaListMoveForward(pList);
  }

  if (pList->nextTs >= ts) {
    goto _end;
  }

  if (pList->pDataBlock == NULL) {
    pList->nextTs = ts;
  } else {
    int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
    SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pSorter->tsSlotId);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    void    *px =
        taosbsearch(&ts, pTsData + pList->startIdx, nrows - pList->startIdx, sizeof(int64_t), compareInt64Val, TD_GE);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pList->startIdx = POINTER_DISTANCE(px, pTsData) / sizeof(int64_t);
    pList->endIdx = pList->startIdx;
    pList->nextTs = *(int64_t *)px;
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stTimestampSorterMetaListCompare(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pMetaLists = (SArray *)param;

  if (left < TARRAY_SIZE(pMetaLists) && right < TARRAY_SIZE(pMetaLists)) {
    SSTriggerMetaDataList *pLeftList = TARRAY_GET_ELEM(pMetaLists, left);
    SSTriggerMetaDataList *pRightList = TARRAY_GET_ELEM(pMetaLists, right);

    if (pLeftList->nextTs < pRightList->nextTs) {
      return -1;
    } else if (pLeftList->nextTs > pRightList->nextTs) {
      return 1;
    } else if (pLeftList->nextTs != INT64_MAX) {
      // sort by version in descending order
      int64_t verLeft = TD_DLIST_HEAD(pLeftList)->pMeta->ver;
      int64_t verRight = TD_DLIST_HEAD(pRightList)->pMeta->ver;
      if (verLeft < verRight) {
        return 1;
      } else if (verLeft > verRight) {
        return -1;
      }
    }
  }
  // fallback to index comparison
  if (left < right) {
    return -1;
  } else if (left > right) {
    return 1;
  }
  return 0;
}

static int32_t stTimestampSorterBuildDataMerger(SSTriggerTimestampSorter *pSorter) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;

  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_META_DATA_SET), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(!BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  int32_t numList = TARRAY_SIZE(pSorter->pMetaLists);
  if (numList == 0) {
    SET_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter);
    goto _end;
  }

  for (int32_t i = 0; i < numList; i++) {
    SSTriggerMetaDataList *pList = TARRAY_GET_ELEM(pSorter->pMetaLists, i);
    code = stTimestampSorterMetaListSkip2Ts(pSorter, pList, pSorter->readRange.skey);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pSorter->pDataMerger && pSorter->pDataMerger->numOfSources < numList) {
    // destroy the old merger if it has less sources than needed
    tMergeTreeDestroy(&pSorter->pDataMerger);
  }
  if (pSorter->pDataMerger == NULL) {
    // round up to the nearest multiple of 8
    int32_t capacity = (numList + 7) / 8 * 8;
    code = tMergeTreeCreate(&pSorter->pDataMerger, capacity, pSorter->pMetaLists, stTimestampSorterMetaListCompare);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pSorter->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    BIT_FLAG_SET_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD);
  }
  return code;
}

int32_t stTimestampSorterNextDataBlock(SSTriggerTimestampSorter *pSorter, SSDataBlock **ppDataBlock, int32_t *pStartIdx,
                                       int32_t *pEndIdx) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SStreamTriggerTask    *pTask = pSorter->pTask;
  SSTriggerMetaDataList *pList = NULL;

  *ppDataBlock = NULL;
  *pStartIdx = 0;
  *pEndIdx = 0;

  if (BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_NO_META_DATA)) {
    pList = TARRAY_DATA(pSorter->pMetaLists);
    if (pList->pDataBlock != NULL && pList->startIdx < pList->endIdx) {
      int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
      SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pSorter->tsSlotId);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      pList->startIdx = pList->endIdx;
      if (pList->startIdx < nrows) {
        pList->nextTs = pTsData[pList->startIdx];
      } else {
        pList->nextTs = pTsData[nrows - 1] + 1;
        blockDataDestroy(pList->pDataBlock);
        pList->startIdx = pList->endIdx = 0;
      }
    }
    if (pList->pDataBlock != NULL) {
      int32_t nrows = blockDataGetNumOfRows(pList->pDataBlock);
      pList->endIdx = nrows;
      *ppDataBlock = pList->pDataBlock;
      *pStartIdx = pList->startIdx;
      *pEndIdx = pList->endIdx;
    } else {
      // need to fetch new data block
    }
    goto _end;
  }

  if (!BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD)) {
    code = stTimestampSorterBuildDataMerger(pSorter);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter)) {
    goto _end;
  } else {
    pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
    if (pList->pDataBlock != NULL && pList->startIdx < pList->endIdx) {
      int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
      SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pSorter->tsSlotId);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      // update read progress
      pSorter->readRange.skey = TMAX(pSorter->readRange.skey, pTsData[pList->endIdx - 1] + 1);
      // move forward to next block range
      pList->startIdx = pList->endIdx;
      if (pList->startIdx < nrows) {
        pList->nextTs = pTsData[pList->startIdx];
      } else {
        stTimestampSorterMetaListMoveForward(pList);
      }
      code = tMergeTreeAdjust(pSorter->pDataMerger, tMergeTreeGetAdjustIndex(pSorter->pDataMerger));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  while (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter)) {
    pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
    if (pList->nextTs > pSorter->readRange.ekey) {
      SET_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter);
      continue;
    }

    if (pList->nextTs < pSorter->readRange.skey) {
      code = stTimestampSorterMetaListSkip2Ts(pSorter, pList, pSorter->readRange.skey);
      QUERY_CHECK_CODE(code, lino, _end);
      code = tMergeTreeAdjust(pSorter->pDataMerger, tMergeTreeGetAdjustIndex(pSorter->pDataMerger));
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    }

    int64_t endTime = pSorter->readRange.ekey;
    if (TARRAY_SIZE(pSorter->pMetaLists) > 1) {
      int32_t idx2 = 0;
      code = stMergeTreeGetSecondIndex(pSorter->pDataMerger, &idx2);
      SSTriggerMetaDataList *pList2 = TARRAY_GET_ELEM(pSorter->pMetaLists, idx2);
      if (pList->nextTs == pList2->nextTs) {
        endTime = TMIN(pList->nextTs, endTime);
      } else {
        endTime = TMIN(pList2->nextTs - 1, endTime);
      }
    }
    QUERY_CHECK_CONDITION(endTime >= pSorter->readRange.skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    if (pList->pDataBlock != NULL) {
      int32_t          nrows = blockDataGetNumOfRows(pList->pDataBlock);
      SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pSorter->tsSlotId);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      void    *px = taosbsearch(&endTime, pTsData + pList->startIdx, nrows - pList->startIdx, sizeof(int64_t),
                                compareInt64Val, TD_GT);
      pList->endIdx = (px != NULL) ? (POINTER_DISTANCE(px, pTsData) / sizeof(int64_t)) : nrows;
      *ppDataBlock = pList->pDataBlock;
      *pStartIdx = pList->startIdx;
      *pEndIdx = pList->endIdx;
    } else {
      // need to fetch data block
    }
    break;
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTimestampSorterForwardNrows(SSTriggerTimestampSorter *pSorter, int64_t nrowsToSkip, int64_t *pSkipped,
                                      int64_t *pLastTs) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SStreamTriggerTask    *pTask = pSorter->pTask;
  SSTriggerMetaDataList *pList = NULL;
  int64_t                skipped = 0;
  int64_t                lastTs = INT64_MIN;

  while (skipped < nrowsToSkip) {
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code = stTimestampSorterNextDataBlock(pSorter, &pDataBlock, &startIdx, &endIdx);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pDataBlock != NULL) {
      // update skipped rows
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pSorter->tsSlotId);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      int32_t  stepSkipped = TMIN(endIdx - startIdx, nrowsToSkip - skipped);
      skipped += stepSkipped;
      lastTs = pTsData[startIdx + stepSkipped - 1];

      // shrink the data block range to only contain the skipped rows
      pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
      QUERY_CHECK_CONDITION(pList->pDataBlock == pDataBlock, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      pList->endIdx = startIdx + stepSkipped;
    } else if (!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter)) {
      pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
      // try to use metadata to skip rows
      int64_t endTime = pSorter->readRange.ekey;
      if (TARRAY_SIZE(pSorter->pMetaLists) > 1) {
        int32_t idx2 = 0;
        code = stMergeTreeGetSecondIndex(pSorter->pDataMerger, &idx2);
        SSTriggerMetaDataList *pList2 = TARRAY_GET_ELEM(pSorter->pMetaLists, idx2);
        if (pList->nextTs == pList2->nextTs) {
          endTime = TMIN(pList->nextTs, endTime);
        } else {
          endTime = TMIN(pList2->nextTs - 1, endTime);
        }
      }
      QUERY_CHECK_CONDITION(endTime >= pSorter->readRange.skey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

      SSTriggerMetaData *pMeta = TD_DLIST_HEAD(pList)->pMeta;
      if (!IS_TRIGGER_META_NROW_INACCURATE(pMeta) && (pMeta->skey >= pList->nextTs) && (pMeta->ekey <= endTime) &&
          (skipped + pMeta->nrows <= nrowsToSkip)) {
        // update skipped rows
        skipped += pMeta->nrows;
        lastTs = pMeta->ekey;
        // update read progress
        pSorter->readRange.skey = TMAX(pSorter->readRange.skey, pMeta->ekey + 1);
        // move forward to next meta
        stTimestampSorterMetaListMoveForward(pList);
        code = tMergeTreeAdjust(pSorter->pDataMerger, tMergeTreeGetAdjustIndex(pSorter->pDataMerger));
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        // need to fetch data block
        break;
      }
    } else {
      // no more data
      break;
    }
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    if (pSkipped != NULL) *pSkipped = skipped;
    if (pLastTs != NULL) *pLastTs = lastTs;
  }
  return code;
}

static int32_t stTimestampSorterWindowReverseCompare(const void *pLeft, const void *pRight) {
  STimeWindow *pLeftWin = (STimeWindow *)pLeft;
  STimeWindow *pRightWin = (STimeWindow *)pRight;
  if (pLeftWin->ekey < pRightWin->ekey) {
    return 1;
  } else if (pLeftWin->ekey > pRightWin->ekey) {
    return -1;
  } else if (pLeftWin->skey < pRightWin->skey) {
    return 1;
  } else if (pLeftWin->skey > pRightWin->skey) {
    return -1;
  }
  return 0;
}

static int32_t stTimestampSorterBuildSessWin(SSTriggerTimestampSorter *pSorter, int64_t gap) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;
  SArray             *pSessWins = pSorter->pSessWins;

  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_META_DATA_SET), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(!BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_SESS_WIN_BUILD), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pSessWins != NULL && TARRAY_SIZE(pSessWins) == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t numList = TARRAY_SIZE(pSorter->pMetaLists);
  for (int32_t i = 0; i < numList; i++) {
    SSTriggerMetaDataList *pList = TARRAY_GET_ELEM(pSorter->pMetaLists, i);
    STimeWindow           *pLastWin = NULL;
    for (SSTriggerMetaDataNode *pNode = TD_DLIST_HEAD(pList); pNode != NULL; pNode = TD_DLIST_NODE_NEXT(pNode)) {
      if (!IS_TRIGGER_META_SKEY_INACCURATE(pNode->pMeta) && (pNode->pMeta->skey >= pSorter->readRange.skey) &&
          (pNode->pMeta->skey <= pSorter->readRange.ekey)) {
        int64_t ts = pNode->pMeta->skey;
        if (pLastWin != NULL && pLastWin->ekey + gap >= ts) {
          pLastWin->ekey = TMAX(pLastWin->ekey, ts);
        } else {
          pLastWin = taosArrayReserve(pSorter->pSessWins, 1);
          *pLastWin = (STimeWindow){.skey = ts, .ekey = ts};
        }
      }
      if (!IS_TRIGGER_META_EKEY_INACCURATE(pNode->pMeta) && (pNode->pMeta->ekey >= pSorter->readRange.skey) &&
          (pNode->pMeta->ekey <= pSorter->readRange.ekey)) {
        int64_t ts = pNode->pMeta->ekey;
        if (pLastWin != NULL && pLastWin->ekey + gap >= ts) {
          pLastWin->ekey = TMAX(pLastWin->ekey, ts);
        } else {
          pLastWin = taosArrayReserve(pSorter->pSessWins, 1);
          *pLastWin = (STimeWindow){.skey = ts, .ekey = ts};
        }
      }
    }
  }

  int32_t numWins = TARRAY_SIZE(pSorter->pSessWins);
  if (numWins == 0) {
    goto _end;
  }

  taosArraySort(pSorter->pSessWins, stTimestampSorterWindowReverseCompare);
  STimeWindow *pWin = TARRAY_GET_ELEM(pSorter->pSessWins, 0);
  for (int32_t i = 1; i < numWins; i++) {
    STimeWindow *pCurWin = TARRAY_GET_ELEM(pSorter->pSessWins, i);
    if (pCurWin->ekey + gap >= pWin->skey) {
      pWin->skey = TMIN(pWin->skey, pCurWin->skey);
    } else {
      ++pWin;
      *pWin = *pCurWin;
    }
  }
  TARRAY_SIZE(pSorter->pSessWins) = TARRAY_ELEM_IDX(pSorter->pSessWins, pWin) + 1;

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    BIT_FLAG_SET_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_SESS_WIN_BUILD);
  }
  return code;
}

int32_t stTimestampSorterForwardTs(SSTriggerTimestampSorter *pSorter, int64_t ts, int64_t gap, int64_t *pLastTs,
                                   int64_t *pNextTs) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SStreamTriggerTask    *pTask = pSorter->pTask;
  SSTriggerMetaDataList *pList = NULL;
  int64_t                nextTs = INT64_MAX;

  if (!BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_SESS_WIN_BUILD)) {
    code = stTimestampSorterBuildSessWin(pSorter, gap);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  while (true) {
    nextTs = INT64_MAX;
    // forward ts using session windows generated from metadata
    while (TARRAY_SIZE(pSorter->pSessWins) > 0) {
      STimeWindow *pWin = taosArrayPop(pSorter->pSessWins);
      if (ts + gap >= pWin->skey) {
        ts = TMAX(pWin->ekey, ts);
      } else {
        // push back the window
        nextTs = pWin->skey;
        TARRAY_SIZE(pSorter->pSessWins)++;
        break;
      }
    }

    // try to read data between ts and nextTs
    int64_t savedEkey = pSorter->readRange.ekey;
    pSorter->readRange.skey = TMAX(pSorter->readRange.skey, ts + 1);
    pSorter->readRange.ekey = TMIN(pSorter->readRange.ekey, nextTs - 1);
    SSDataBlock *pDataBlock = NULL;
    int32_t      startIdx = 0;
    int32_t      endIdx = 0;
    code = stTimestampSorterNextDataBlock(pSorter, &pDataBlock, &startIdx, &endIdx);
    bool needFetch = (pDataBlock == NULL) && !IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter);
    pSorter->readRange.ekey = savedEkey;
    QUERY_CHECK_CODE(code, lino, _end);

    if (pDataBlock != NULL) {
      // forward ts using data block
      SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, pSorter->tsSlotId);
      QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      while (startIdx < endIdx) {
        if (ts + gap >= pTsData[startIdx]) {
          ts = TMAX(ts, pTsData[startIdx]);
          startIdx++;
        } else {
          nextTs = pTsData[startIdx];
          break;
        }
      }

      if (startIdx < endIdx) {
        // shrink the data block range to only contain the checked rows
        pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
        QUERY_CHECK_CONDITION(pList->pDataBlock == pDataBlock, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pList->endIdx = startIdx;

        // find time diff between two consecutive rows larger than gap
        break;
      }
    } else if (needFetch) {
      // need to fetch data block
      nextTs = INT64_MAX;
      break;
    } else {
      // no more data
      break;
    }
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    if (pLastTs != NULL) *pLastTs = ts;
    if (pNextTs != NULL) *pNextTs = nextTs;
  }
  return code;
}

int32_t stTimestampSorterGetMetaToFetch(SSTriggerTimestampSorter *pSorter, SSTriggerMetaData **ppMeta) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;

  *ppMeta = NULL;

  if (BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_NO_META_DATA) ||
      IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter)) {
    goto _end;
  }

  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  SSTriggerMetaDataList *pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
  if (pList->pDataBlock == NULL && TD_DLIST_HEAD(pList) != NULL) {
    *ppMeta = TD_DLIST_HEAD(pList)->pMeta;
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stTimestampSorterBindDataBlock(SSTriggerTimestampSorter *pSorter, SSDataBlock **ppDataBlock) {
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  SStreamTriggerTask    *pTask = pSorter->pTask;
  SSTriggerMetaDataList *pList = NULL;

  if (BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_NO_META_DATA)) {
    SColumnInfoData *pTsCol = taosArrayGet((*ppDataBlock)->pDataBlock, pSorter->tsSlotId);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;

    pList = TARRAY_DATA(pSorter->pMetaLists);
    QUERY_CHECK_CONDITION(pList->pDataBlock == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pList->pDataBlock = *ppDataBlock;
    pList->startIdx = pList->endIdx = 0;
    pList->nextTs = pTsData[0];
    goto _end;
  }

  QUERY_CHECK_CONDITION(!IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pSorter->flags, TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  pList = TARRAY_GET_ELEM(pSorter->pMetaLists, tMergeTreeGetChosenIndex(pSorter->pDataMerger));
  QUERY_CHECK_CONDITION(pList->pDataBlock == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(TD_DLIST_HEAD(pList), code, lino, _end, TSDB_CODE_INVALID_PARA);
  pList->pDataBlock = *ppDataBlock;
  *ppDataBlock = NULL;

  SSTriggerMetaData *pMeta = TD_DLIST_HEAD(pList)->pMeta;
  int32_t            nrows = blockDataGetNumOfRows(pList->pDataBlock);
  if (nrows <= 0) {
    SET_TRIGGER_META_DATA_EMPTY(pMeta);
    stTimestampSorterMetaListMoveForward(pList);
  } else {
    SColumnInfoData *pTsCol = taosArrayGet(pList->pDataBlock->pDataBlock, pSorter->tsSlotId);
    QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
    int64_t *pTsData = (int64_t *)pTsCol->pData;
    // update meta info, which may help with subsequent data merging
    int64_t skey = pTsData[0];
    int64_t ekey = pTsData[nrows - 1];
    QUERY_CHECK_CONDITION(skey >= pMeta->skey && ekey <= pMeta->ekey, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pMeta->skey = skey;
    pMeta->ekey = ekey;
    pMeta->nrows = nrows;

    void *px = taosbsearch(&pList->nextTs, pTsData, nrows, sizeof(int64_t), compareInt64Val, TD_GE);
    if (px == NULL) {
      stTimestampSorterMetaListMoveForward(pList);
    } else {
      pList->startIdx = POINTER_DISTANCE(px, pTsData) / sizeof(int64_t);
      pList->endIdx = pList->startIdx;
      pList->nextTs = *(int64_t *)px;
    }
  }
  code = tMergeTreeAdjust(pSorter->pDataMerger, tMergeTreeGetAdjustIndex(pSorter->pDataMerger));
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

typedef struct SVtableMergerReaderInfo {
  SSTriggerTableColRef *pColRef;
  SSDataBlock          *pDataBlock;
  int32_t               startIdx;
  int32_t               endIdx;
  int64_t               nextTs;
} SVtableMergerReaderInfo;

static int32_t stVtableMergerReaderInfoCompare(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pReaderInfos = (SArray *)param;

  if (left < TARRAY_SIZE(pReaderInfos) && right < TARRAY_SIZE(pReaderInfos)) {
    SVtableMergerReaderInfo *pLeftReaderInfo = TARRAY_GET_ELEM(pReaderInfos, left);
    SVtableMergerReaderInfo *pRightReaderInfo = TARRAY_GET_ELEM(pReaderInfos, right);

    if (pLeftReaderInfo->nextTs < pRightReaderInfo->nextTs) {
      return -1;
    } else if (pLeftReaderInfo->nextTs > pRightReaderInfo->nextTs) {
      return 1;
    }
  }
  // fallback to index comparison
  if (left < right) {
    return -1;
  } else if (left > right) {
    return 1;
  }
  return 0;
}

int32_t stVtableMergerInit(SSTriggerVtableMerger *pMerger, struct SStreamTriggerTask *pTask, SSDataBlock **ppDataBlock,
                           SFilterInfo **ppFilter, int32_t nVirDataCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pMerger->pTask = pTask;
  pMerger->pDataBlock = *ppDataBlock;
  *ppDataBlock = NULL;
  pMerger->pFilter = *ppFilter;
  *ppFilter = NULL;
  pMerger->nVirDataCols = nVirDataCols;
  pMerger->readRange = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};

  pMerger->pReaderInfos = taosArrayInit(0, sizeof(SVtableMergerReaderInfo));
  QUERY_CHECK_NULL(pMerger->pReaderInfos, code, lino, _end, terrno);

  pMerger->pReaders = taosArrayInit(0, sizeof(SSTriggerTimestampSorter *));
  QUERY_CHECK_NULL(pMerger->pReaders, code, lino, _end, terrno);

  if (pMerger->pFilter != NULL) {
    SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pMerger->pDataBlock->pDataBlock),
                                .pDataBlock = pMerger->pDataBlock->pDataBlock};
    code = filterSetDataFromSlotId(pMerger->pFilter, &param);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void stVtableMergerDestroy(void *ptr) {
  SSTriggerVtableMerger **ppMerger = ptr;
  if (ppMerger == NULL || *ppMerger == NULL) {
    return;
  }

  SSTriggerVtableMerger *pMerger = *ppMerger;
  if (pMerger->pDataBlock != NULL) {
    blockDataDestroy(pMerger->pDataBlock);
    pMerger->pDataBlock = NULL;
  }

  if (pMerger->pFilter != NULL) {
    filterFreeInfo(pMerger->pFilter);
    pMerger->pFilter = NULL;
  }

  if (pMerger->pReaderInfos != NULL) {
    taosArrayDestroy(pMerger->pReaderInfos);
    pMerger->pReaderInfos = NULL;
  }
  if (pMerger->pPseudoCols != NULL) {
    blockDataDestroy(pMerger->pPseudoCols);
    pMerger->pPseudoCols = NULL;
  }

  if (pMerger->pReaders != NULL) {
    taosArrayDestroyEx(pMerger->pReaders, stTimestampSorterDestroy);
    pMerger->pReaders = NULL;
  }

  if (pMerger->pDataMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
    pMerger->pDataMerger = NULL;
  }
  taosMemFreeClear(*ppMerger);
}

void stVtableMergerReset(SSTriggerVtableMerger *pMerger) {
  if (pMerger == NULL) {
    return;
  }

  pMerger->flags = 0;
  pMerger->readRange = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};
  blockDataEmpty(pMerger->pDataBlock);

  if (pMerger->pReaderInfos != NULL) {
    taosArrayClear(pMerger->pReaderInfos);
  }
  if (pMerger->pPseudoCols != NULL) {
    blockDataDestroy(pMerger->pPseudoCols);
    pMerger->pPseudoCols = NULL;
  }
}

int32_t stVtableMergerSetMergeInfo(SSTriggerVtableMerger *pMerger, STimeWindow *pRange, SArray *pTableColRefs) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SArray             *pReaderInfos = pMerger->pReaderInfos;
  SArray             *pReaders = pMerger->pReaders;

  QUERY_CHECK_CONDITION(pMerger->flags == 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pRange != NULL && pRange->skey <= pRange->ekey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pReaderInfos != NULL && TARRAY_SIZE(pReaderInfos) == 0, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pReaders != NULL && TARRAY_SIZE(pReaders) >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger->readRange = *pRange;
  int32_t nTables = taosArrayGetSize(pTableColRefs);

  if (nTables == 0) {
    SET_TRIGGER_VTABLE_MERGER_EMPTY(pMerger);
    goto _end;
  }

  for (int32_t i = 0; i < nTables; i++) {
    SVtableMergerReaderInfo *pReaderInfo = taosArrayReserve(pReaderInfos, 1);
    QUERY_CHECK_NULL(pReaderInfo, code, lino, _end, terrno);
    pReaderInfo->pColRef = TARRAY_GET_ELEM(pTableColRefs, i);
    pReaderInfo->pDataBlock = NULL;
    pReaderInfo->startIdx = pReaderInfo->endIdx = 0;
    pReaderInfo->nextTs = pRange->skey;

    SSTriggerTimestampSorter *pReader = NULL;
    if (i < TARRAY_SIZE(pReaders)) {
      pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pReaders, i);
      QUERY_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      stTimestampSorterReset(pReader);
    } else {
      void *px = taosArrayReserve(pReaders, 1);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      pReader = taosMemoryCalloc(1, sizeof(SSTriggerTimestampSorter));
      QUERY_CHECK_NULL(pReader, code, lino, _end, terrno);
      *(SSTriggerTimestampSorter **)px = pReader;
      code = stTimestampSorterInit(pReader, pTask);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = stTimestampSorterSetSortInfo(pReader, pRange, pReaderInfo->pColRef->otbUid, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->pDataMerger && pMerger->pDataMerger->numOfSources < nTables) {
    // destroy the old merger if it has less sources than needed
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  if (pMerger->pDataMerger == NULL) {
    // round up to the nearest multiple of 8
    int32_t capacity = (nTables + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pDataMerger, capacity, pReaderInfos, stVtableMergerReaderInfoCompare);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    BIT_FLAG_SET_MASK(pMerger->flags, TRIGGER_VTABLE_MERGER_MASK_MERGE_INFO_SET);
  }
  return code;
}

int32_t stVtableMergerSetPseudoCols(SSTriggerVtableMerger *pMerger, SSDataBlock **ppDataBlock) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SArray             *pReaders = pMerger->pReaders;

  QUERY_CHECK_CONDITION(pMerger->pPseudoCols == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger->pPseudoCols = *ppDataBlock;
  *ppDataBlock = NULL;

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stVtableMergerSetMetaDatas(SSTriggerVtableMerger *pMerger, SSHashObj *pOrigTableMetas) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SArray             *pReaders = pMerger->pReaders;

  QUERY_CHECK_CONDITION(pMerger->flags == TRIGGER_VTABLE_MERGER_MASK_MERGE_INFO_SET, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(TARRAY_SIZE(pReaders) >= TARRAY_SIZE(pMerger->pReaderInfos), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  int32_t nReaders = TARRAY_SIZE(pMerger->pReaderInfos);
  for (int32_t i = 0; i < nReaders; i++) {
    SVtableMergerReaderInfo *pInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, i);
    SSTriggerTableMeta      *pTableMeta = tSimpleHashGet(pOrigTableMetas, &pInfo->pColRef->otbUid, sizeof(int64_t));
    if (pTableMeta != NULL) {
      SSTriggerTimestampSorter *pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pReaders, i);
      code = stTimestampSorterSetMetaDatas(pReader, pTableMeta);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  BIT_FLAG_SET_MASK(pMerger->flags, TRIGGER_VTABLE_MERGER_MASK_META_DATA_SET);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stVtableMergerSetEmptyMetaDatas(SSTriggerVtableMerger *pMerger) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SArray             *pReaders = pMerger->pReaders;

  QUERY_CHECK_CONDITION(pMerger->flags == TRIGGER_VTABLE_MERGER_MASK_MERGE_INFO_SET, code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(TARRAY_SIZE(pReaders) >= TARRAY_SIZE(pMerger->pReaderInfos), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  int32_t nReaders = TARRAY_SIZE(pMerger->pReaderInfos);
  for (int32_t i = 0; i < nReaders; i++) {
    SSTriggerTimestampSorter *pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pReaders, i);
    code = stTimestampSorterSetEmptyMetaDatas(pReader);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

#define stVtableMerger_NUM_OF_ROWS_PER_BLOCK 4096

static int32_t stVtableMergerCopyDataBlock(SSTriggerVtableMerger *pMerger, SVtableMergerReaderInfo *pReaderInfo,
                                           int64_t endTime, bool *pIsFull) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SSDataBlock        *pVirDataBlock = pMerger->pDataBlock;
  SSDataBlock        *pOrigDataBlock = pReaderInfo->pDataBlock;

  SColumnInfoData *pVirTsCol = taosArrayGet(pVirDataBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(pVirTsCol, code, lino, _end, terrno);
  int64_t *pVirTsData = (int64_t *)pVirTsCol->pData;

  SColumnInfoData *pOrigTsCol = taosArrayGet(pOrigDataBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(pOrigTsCol, code, lino, _end, terrno);
  int64_t *pOrigTsData = (int64_t *)pOrigTsCol->pData;

  int32_t virStartIdx = blockDataGetNumOfRows(pVirDataBlock);
  if (virStartIdx > 0 && pVirTsData[virStartIdx - 1] == pOrigTsData[pReaderInfo->startIdx]) {
    // merge to the last row
    --virStartIdx;
  }

  void   *px = taosbsearch(&endTime, pOrigTsData + pReaderInfo->startIdx, pReaderInfo->endIdx - pReaderInfo->startIdx,
                           sizeof(int64_t), compareInt64Val, TD_GT);
  int32_t origEndIdx = (px != NULL) ? (POINTER_DISTANCE(px, pOrigTsData) / sizeof(int64_t)) : pReaderInfo->endIdx;

  // copy data from original data block to virtual data block
  int32_t nRowsToCopy = TMIN(pVirDataBlock->info.capacity - virStartIdx, origEndIdx - pReaderInfo->startIdx);
  code = colDataAssignNRows(pVirTsCol, virStartIdx, pOrigTsCol, pReaderInfo->startIdx, nRowsToCopy);
  QUERY_CHECK_CODE(code, lino, _end);
  int32_t nCols = taosArrayGetSize(pReaderInfo->pColRef->pColMatches);
  for (int32_t i = 0; i < nCols; i++) {
    SSTriggerColMatch *pColMatch = TARRAY_GET_ELEM(pReaderInfo->pColRef->pColMatches, i);
    SColumnInfoData   *pVirCol = taosArrayGet(pVirDataBlock->pDataBlock, pColMatch->vtbSlotId);
    QUERY_CHECK_NULL(pVirCol, code, lino, _end, terrno);
    SColumnInfoData *pOrigCol = taosArrayGet(pOrigDataBlock->pDataBlock, i + 1);
    QUERY_CHECK_NULL(pOrigCol, code, lino, _end, terrno);
    code = colDataAssignNRows(pVirCol, virStartIdx, pOrigCol, pReaderInfo->startIdx, nRowsToCopy);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // update block info
  pVirDataBlock->info.rows = virStartIdx + nRowsToCopy;
  pReaderInfo->startIdx += nRowsToCopy;
  *pIsFull = (pReaderInfo->startIdx < origEndIdx);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stVtableMergerNextDataBlock(SSTriggerVtableMerger *pMerger, SSDataBlock **ppDataBlock) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SStreamTriggerTask      *pTask = pMerger->pTask;
  SSDataBlock             *pDataBlock = pMerger->pDataBlock;
  SVtableMergerReaderInfo *pReaderInfo = NULL;
  SColumnInfoData         *p = NULL;

  *ppDataBlock = NULL;

  if (IS_TRIGGER_VTABLE_MERGER_EMPTY(pMerger)) {
    goto _end;
  } else {
    int32_t nrows = blockDataGetNumOfRows(pDataBlock);
    if (nrows > 0) {
      SColumnInfoData *pVirTsCol = taosArrayGet(pDataBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(pVirTsCol, code, lino, _end, terrno);
      int64_t *pVirTsData = (int64_t *)pVirTsCol->pData;
      if (pMerger->readRange.skey > pVirTsData[nrows - 1]) {
        // need to get next data block
        blockDataReset(pDataBlock);
        nrows = 0;
      }
    }
    code = blockDataEnsureCapacity(pDataBlock, stVtableMerger_NUM_OF_ROWS_PER_BLOCK);
    QUERY_CHECK_CODE(code, lino, _end);
    if (nrows == 0) {
      // set all columns to NULL by default
      for (int32_t i = 0; i < TARRAY_SIZE(pDataBlock->pDataBlock); i++) {
        SColumnInfoData *pCol = taosArrayGet(pDataBlock->pDataBlock, i);
        QUERY_CHECK_NULL(pCol, code, lino, _end, terrno);
        colDataSetNNULL(pCol, 0, stVtableMerger_NUM_OF_ROWS_PER_BLOCK);
      }
    }
  }

  bool    needFetchDataBlock = false;
  int32_t nCols = taosArrayGetSize(pDataBlock->pDataBlock);
  while (!IS_TRIGGER_VTABLE_MERGER_EMPTY(pMerger)) {
    int32_t idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
    pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, idx);
    if (pReaderInfo->nextTs > pMerger->readRange.ekey) {
      SET_TRIGGER_VTABLE_MERGER_EMPTY(pMerger);
      continue;
    }

    if (pMerger->nVirDataCols < nCols && pMerger->pPseudoCols == NULL) {
      // need to fetch pseudo columns
      needFetchDataBlock = true;
      break;
    }

    if (pReaderInfo->pDataBlock == NULL) {
      // get next data block from reader
      SSTriggerTimestampSorter *pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pMerger->pReaders, idx);
      code = stTimestampSorterNextDataBlock(pReader, &pReaderInfo->pDataBlock, &pReaderInfo->startIdx,
                                            &pReaderInfo->endIdx);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pReaderInfo->pDataBlock == NULL) {
        if (IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pReader)) {
          // no more data
          pReaderInfo->nextTs = INT64_MAX;
        } else {
          // need to fetch data block
          needFetchDataBlock = true;
          break;
        }
      } else {
        SColumnInfoData *pOrigTsCol = taosArrayGet(pReaderInfo->pDataBlock->pDataBlock, 0);
        QUERY_CHECK_NULL(pOrigTsCol, code, lino, _end, terrno);
        int64_t *pOrigTsData = (int64_t *)pOrigTsCol->pData;
        pReaderInfo->nextTs = pOrigTsData[pReaderInfo->startIdx];
      }
      code = tMergeTreeAdjust(pMerger->pDataMerger, tMergeTreeGetAdjustIndex(pMerger->pDataMerger));
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      int64_t endTime = pMerger->readRange.ekey;
      if (TARRAY_SIZE(pMerger->pReaderInfos) > 1) {
        int32_t idx2 = 0;
        code = stMergeTreeGetSecondIndex(pMerger->pDataMerger, &idx2);
        SVtableMergerReaderInfo *pReaderInfo2 = TARRAY_GET_ELEM(pMerger->pReaderInfos, idx2);
        if (pReaderInfo->nextTs == pReaderInfo2->nextTs) {
          endTime = TMIN(pReaderInfo->nextTs, endTime);
        } else {
          endTime = TMIN(pReaderInfo2->nextTs - 1, endTime);
        }
      }
      bool isFull = false;
      code = stVtableMergerCopyDataBlock(pMerger, pReaderInfo, endTime, &isFull);
      QUERY_CHECK_CODE(code, lino, _end);
      SColumnInfoData *pOrigTsCol = taosArrayGet(pReaderInfo->pDataBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(pOrigTsCol, code, lino, _end, terrno);
      int64_t *pOrigTsData = (int64_t *)pOrigTsCol->pData;
      if (pReaderInfo->startIdx < pReaderInfo->endIdx) {
        pReaderInfo->nextTs = pOrigTsData[pReaderInfo->startIdx];
      } else {
        pReaderInfo->nextTs = pOrigTsData[pReaderInfo->endIdx - 1] + 1;
        pReaderInfo->pDataBlock = NULL;
        pReaderInfo->startIdx = pReaderInfo->endIdx = 0;
      }
      code = tMergeTreeAdjust(pMerger->pDataMerger, tMergeTreeGetAdjustIndex(pMerger->pDataMerger));
      QUERY_CHECK_CODE(code, lino, _end);
      if (isFull) {
        // result data block is full, return it
        break;
      }
    }
  }

  int32_t nrows = blockDataGetNumOfRows(pDataBlock);
  if (!needFetchDataBlock && nrows > 0) {
    if (pMerger->nVirDataCols < nCols) {
      QUERY_CHECK_NULL(pMerger->pPseudoCols, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      int32_t nPseudoCols = blockDataGetNumOfCols(pMerger->pPseudoCols);
      QUERY_CHECK_CONDITION(pMerger->nVirDataCols + nPseudoCols == nCols, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
      for (int32_t i = 0; i < nPseudoCols; i++) {
        SColumnInfoData *pSrc = TARRAY_GET_ELEM(pMerger->pPseudoCols->pDataBlock, i);
        SColumnInfoData *pDst = TARRAY_GET_ELEM(pMerger->pDataBlock->pDataBlock, pMerger->nVirDataCols + i);
        if (!colDataIsNull_s(pSrc, 0)) {
          if (!IS_VAR_DATA_TYPE(pDst->info.type) && pDst->nullbitmap != NULL) {
            int32_t bmLen = BitmapLen(nrows);
            memset(pDst->nullbitmap, 0, bmLen);
          }
          code = colDataCopyNItems(pDst, 0, colDataGetData(pSrc, 0), nrows, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
    if (pMerger->pFilter != NULL) {
      int32_t status = 0;
      code = filterExecute(pMerger->pFilter, pMerger->pDataBlock, &p, NULL, blockDataGetNumOfCols(pDataBlock), &status);
      QUERY_CHECK_CODE(code, lino, _end);
      code = trimDataBlock(pMerger->pDataBlock, nrows, (bool *)p->pData);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    *ppDataBlock = pMerger->pDataBlock;
    SColumnInfoData *pVirTsCol = taosArrayGet(pDataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pVirTsCol, code, lino, _end, terrno);
    int64_t *pVirTsData = (int64_t *)pVirTsCol->pData;
    pMerger->readRange.skey = TMAX(pMerger->readRange.skey, pVirTsData[nrows - 1] + 1);
  }

_end:
  if (p != NULL) {
    colDataDestroy(p);
    taosMemoryFreeClear(p);
  }
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stVtableMergerGetMetaToFetch(SSTriggerVtableMerger *pMerger, SSTriggerMetaData **ppMeta,
                                     SSTriggerTableColRef **ppColRef) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;

  *ppMeta = NULL;
  *ppColRef = NULL;

  if (IS_TRIGGER_VTABLE_MERGER_EMPTY(pMerger)) {
    goto _end;
  }

  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pMerger->flags, TRIGGER_VTABLE_MERGER_MASK_META_DATA_SET), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);
  int32_t                   idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SVtableMergerReaderInfo  *pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, idx);
  SSTriggerTimestampSorter *pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pMerger->pReaders, idx);
  if (pMerger->nVirDataCols < blockDataGetNumOfCols(pMerger->pDataBlock) && pMerger->pPseudoCols == NULL) {
    *ppColRef = pReaderInfo->pColRef;
  } else if (pReaderInfo->pDataBlock == NULL) {
    code = stTimestampSorterGetMetaToFetch(pReader, ppMeta);
    QUERY_CHECK_CODE(code, lino, _end);
    if (*ppMeta != NULL) {
      *ppColRef = pReaderInfo->pColRef;
    }
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stVtableMergerBindDataBlock(SSTriggerVtableMerger *pMerger, SSDataBlock **ppDataBlock) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;

  QUERY_CHECK_CONDITION(!IS_TRIGGER_VTABLE_MERGER_EMPTY(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(BIT_FLAG_TEST_MASK(pMerger->flags, TRIGGER_VTABLE_MERGER_MASK_META_DATA_SET), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  int32_t                   idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SVtableMergerReaderInfo  *pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, idx);
  SSTriggerTimestampSorter *pReader = *(SSTriggerTimestampSorter **)TARRAY_GET_ELEM(pMerger->pReaders, idx);
  QUERY_CHECK_CONDITION(pReaderInfo->pDataBlock == NULL, code, lino, _end, TSDB_CODE_INVALID_PARA);
  code = stTimestampSorterBindDataBlock(pReader, ppDataBlock);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

typedef struct SNewTimestampSorterSlice {
  int32_t startIdx;
  int32_t endIdx;
  TD_DLIST_NODE(SNewTimestampSorterSlice);
} SNewTimestampSorterSlice;

typedef TD_DLIST(SNewTimestampSorterSlice) SNewTimestampSorterSliceList;

static int32_t stNewTimestampSorterSliceListCompare(const void *pLeft, const void *pRight, void *param) {
  int32_t                      left = *(const int32_t *)pLeft;
  int32_t                      right = *(const int32_t *)pRight;
  SSTriggerNewTimestampSorter *pSorter = (SSTriggerNewTimestampSorter *)param;
  SArray                      *pSliceLists = pSorter->pSliceLists;

  if (left < TARRAY_SIZE(pSorter->pSliceLists) && right < TARRAY_SIZE(pSorter->pSliceLists)) {
    SNewTimestampSorterSliceList *pLeftList = TARRAY_GET_ELEM(pSorter->pSliceLists, left);
    SNewTimestampSorterSliceList *pRightList = TARRAY_GET_ELEM(pSorter->pSliceLists, right);

    SColumnInfoData *pTsCol = TARRAY_GET_ELEM(pSorter->pDataBlock->pDataBlock, pSorter->tsSlotId);
    int64_t         *pTsData = (int64_t *)pTsCol->pData;
    int32_t          leftIdx = (TD_DLIST_HEAD(pLeftList) != NULL) ? TD_DLIST_HEAD(pLeftList)->startIdx : -1;
    int64_t          leftTs = (leftIdx >= 0) ? pTsData[leftIdx] : INT64_MAX;
    int32_t          rightIdx = (TD_DLIST_HEAD(pRightList) != NULL) ? TD_DLIST_HEAD(pRightList)->startIdx : -1;
    int64_t          rightTs = (rightIdx >= 0) ? pTsData[rightIdx] : INT64_MAX;

    // compare by start timestamp first, then by start index
    if (leftTs < rightTs) {
      return -1;
    } else if (leftTs > rightTs) {
      return 1;
    } else if (leftIdx < rightIdx) {
      return 1;
    } else if (leftIdx > rightIdx) {
      return -1;
    }
  }
_end:
  // fallback to index comparison
  if (left < right) {
    return -1;
  } else if (left > right) {
    return 1;
  }
  return 0;
}

int32_t stNewTimestampSorterInit(SSTriggerNewTimestampSorter *pSorter, struct SStreamTriggerTask *pTask,
                                 int32_t verColBias) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pSorter->pTask = pTask;
  pSorter->verColBias = verColBias;

  pSorter->pSliceBuf = taosArrayInit(0, sizeof(SNewTimestampSorterSlice));
  QUERY_CHECK_NULL(pSorter->pSliceBuf, code, lino, _end, terrno);
  pSorter->pSliceLists = taosArrayInit(0, sizeof(SNewTimestampSorterSliceList));
  QUERY_CHECK_NULL(pSorter->pSliceLists, code, lino, _end, terrno);

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void stNewTimestampSorterDestroy(void *ptr) {
  SSTriggerNewTimestampSorter **ppSorter = ptr;
  if (ppSorter == NULL || *ppSorter == NULL) {
    return;
  }

  SSTriggerNewTimestampSorter *pSorter = *ppSorter;
  if (pSorter->pSliceBuf != NULL) {
    taosArrayDestroy(pSorter->pSliceBuf);
    pSorter->pSliceBuf = NULL;
  }
  if (pSorter->pSliceLists != NULL) {
    taosArrayDestroy(pSorter->pSliceLists);
    pSorter->pSliceLists = NULL;
  }
  if (pSorter->pDataMerger != NULL) {
    tMergeTreeDestroy(&pSorter->pDataMerger);
  }

  taosMemoryFreeClear(*ppSorter);
}

void stNewTimestampSorterReset(SSTriggerNewTimestampSorter *pSorter) {
  if (pSorter == NULL) {
    return;
  }

  pSorter->inUse = false;
  pSorter->pDataBlock = NULL;

  if (pSorter->pSliceBuf != NULL) {
    taosArrayClear(pSorter->pSliceBuf);
  }
  if (pSorter->pSliceLists != NULL) {
    taosArrayClear(pSorter->pSliceLists);
  }
}

int32_t stNewTimestampSorterSetData(SSTriggerNewTimestampSorter *pSorter, int64_t tbUid, int32_t tsSlotId,
                                    STimeWindow *pReadRange, SObjList *pMetas, SSTriggerDataSlice *pSlice) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;
  SSDataBlock        *pDataBlock = pSlice->pDataBlock;

  QUERY_CHECK_CONDITION(!pSorter->inUse, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pSorter->inUse = true;
  pSorter->pDataBlock = pDataBlock;
  pSorter->tsSlotId = tsSlotId;
  pDataBlock->info.id.uid = tbUid;

  // collect all data slices; data in each slice is in ascending order
  SColumnInfoData *pTsCol = taosArrayGet(pDataBlock->pDataBlock, tsSlotId);
  QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
  int64_t         *pTsData = (int64_t *)pTsCol->pData;
  SColumnInfoData *pVerCol = taosArrayGetLast(pDataBlock->pDataBlock);
  QUERY_CHECK_NULL(pVerCol, code, lino, _end, terrno);
  pVerCol -= pSorter->verColBias;
  int64_t *pVerData = (int64_t *)pVerCol->pData;

  int32_t            i = pSlice->startIdx;
  int64_t            lastTs = INT64_MIN;
  SSTriggerMetaData *pMeta = NULL;
  SObjListIter       iter;
  taosObjListInitIter(pMetas, &iter, TOBJLIST_ITER_FORWARD);
  while ((i < pSlice->endIdx) && (pMeta = (SSTriggerMetaData *)taosObjListIterNext(&iter)) != NULL) {
    while (i < pSlice->endIdx && pVerData[i] < pMeta->ver) {
      lastTs = TMAX(lastTs, pTsData[i]);
      i++;
    }
    if (i < pSlice->endIdx && pVerData[i] > pMeta->ver) {
      continue;
    }
    while (pTask->ignoreDisorder && i < pSlice->endIdx && lastTs != INT64_MIN &&
           pTsData[i] <= lastTs - pTask->watermark) {
      i++;
    }
    int64_t skey = TMAX(pMeta->skey, pReadRange->skey);
    while (i < pSlice->endIdx && pVerData[i] == pMeta->ver && pTsData[i] < skey) {
      i++;
    }
    SNewTimestampSorterSlice slice = {.startIdx = i};
    int64_t                  ekey = TMIN(pMeta->ekey, pReadRange->ekey);
    while (i < pSlice->endIdx && pVerData[i] == pMeta->ver && pTsData[i] <= ekey) {
      i++;
    }
    slice.endIdx = i;
    if (slice.startIdx < slice.endIdx) {
      SNewTimestampSorterSlice *pLastSlice = NULL;
      if (TARRAY_SIZE(pSorter->pSliceBuf) > 0) {
        pLastSlice = TARRAY_GET_ELEM(pSorter->pSliceBuf, TARRAY_SIZE(pSorter->pSliceBuf) - 1);
      }
      if (pLastSlice != NULL && pLastSlice->endIdx == slice.startIdx &&
          pTsData[pLastSlice->endIdx - 1] < pTsData[slice.startIdx]) {
        // merge with the last slice
        pLastSlice->endIdx = slice.endIdx;
      } else {
        void *px = taosArrayPush(pSorter->pSliceBuf, &slice);
        QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      }
    }
    if (i > 0) {
      lastTs = TMAX(lastTs, pTsData[i - 1]);
    }
  }

  // combine slices into lists; data in each list is in ascending order
  for (int32_t i = 0; i < TARRAY_SIZE(pSorter->pSliceBuf); i++) {
    SNewTimestampSorterSlice     *pSlice = TARRAY_GET_ELEM(pSorter->pSliceBuf, i);
    SNewTimestampSorterSliceList *pList = NULL;
    int64_t                       firstTs = pTsData[pSlice->startIdx];
    for (int32_t j = 0; j < TARRAY_SIZE(pSorter->pSliceLists); j++) {
      SNewTimestampSorterSliceList *pTempList = TARRAY_GET_ELEM(pSorter->pSliceLists, j);
      if (pTsData[TD_DLIST_TAIL(pTempList)->endIdx - 1] < firstTs) {
        pList = pTempList;
        break;
      }
    }
    if (pList == NULL) {
      pList = taosArrayReserve(pSorter->pSliceLists, 1);
      QUERY_CHECK_NULL(pList, code, lino, _end, terrno);
    }
    TD_DLIST_APPEND(pList, pSlice);
  }

  if (stDebugFlag & DEBUG_DEBUG) {
    for (int32_t i = 0; i < TARRAY_SIZE(pSorter->pSliceLists); i++) {
      SNewTimestampSorterSliceList *pList = TARRAY_GET_ELEM(pSorter->pSliceLists, i);
      SNewTimestampSorterSlice     *pSlice = TD_DLIST_HEAD(pList);
      while (pSlice != NULL) {
        ST_TASK_DLOG("Slice List %d: add [%d, %d)", i, pSlice->startIdx, pSlice->endIdx);
        pSlice = TD_DLIST_NODE_NEXT(pSlice);
      }
    }
  }

  if (TARRAY_SIZE(pSorter->pSliceLists) == 0) {
    goto _end;
  }

  // merge data from all lists
  if (pSorter->pDataMerger != NULL && pSorter->pDataMerger->numOfSources < TARRAY_SIZE(pSorter->pSliceLists)) {
    tMergeTreeDestroy(&pSorter->pDataMerger);
  }
  if (pSorter->pDataMerger == NULL) {
    // round up to the nearest multiple of 8
    int32_t capacity = (TARRAY_SIZE(pSorter->pSliceLists) + 7) / 8 * 8;
    code = tMergeTreeCreate(&pSorter->pDataMerger, capacity, pSorter, stNewTimestampSorterSliceListCompare);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pSorter->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stNewTimestampSorterNextDataBlock(SSTriggerNewTimestampSorter *pSorter, SSDataBlock **ppDataBlock,
                                          int32_t *pStartIdx, int32_t *pEndIdx) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pSorter->pTask;
  bool                needRebuild = false;

  QUERY_CHECK_CONDITION(pSorter->inUse, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (ppDataBlock != NULL) {
    *ppDataBlock = NULL;
  }
  *pStartIdx = 0;
  *pEndIdx = 0;

  if (TARRAY_SIZE(pSorter->pSliceLists) == 0) {
    goto _end;
  }

  int32_t                       idx = tMergeTreeGetChosenIndex(pSorter->pDataMerger);
  SNewTimestampSorterSliceList *pList = TARRAY_GET_ELEM(pSorter->pSliceLists, idx);
  SNewTimestampSorterSlice     *pSlice = TD_DLIST_HEAD(pList);
  if (pSlice == NULL) {
    goto _end;
  }

  if (ppDataBlock != NULL) {
    *ppDataBlock = pSorter->pDataBlock;
  }
  *pStartIdx = pSlice->startIdx;

  if (TARRAY_SIZE(pSorter->pSliceLists) == 1) {
    *pEndIdx = pSlice->endIdx;
    TD_DLIST_POP(pList, pSlice);
    goto _end;
  }

  SColumnInfoData *pTsCol = TARRAY_GET_ELEM(pSorter->pDataBlock->pDataBlock, pSorter->tsSlotId);
  int64_t         *pTsData = (int64_t *)pTsCol->pData;
  int64_t          startTs = pTsData[pSlice->startIdx];
  int64_t          endTs = INT64_MAX;
  for (int32_t i = 0; i < TARRAY_SIZE(pSorter->pSliceLists); i++) {
    SNewTimestampSorterSliceList *pTempList = TARRAY_GET_ELEM(pSorter->pSliceLists, i);
    SNewTimestampSorterSlice     *pTempSlice = TD_DLIST_HEAD(pTempList);
    if (i == idx || pTempSlice == NULL) {
      continue;
    }
    if (pTsData[pTempSlice->startIdx] == startTs) {
      // skip the current row
      ST_TASK_DLOG("Slice List %d: pop [%d, %d)", i, pTempSlice->startIdx, pTempSlice->startIdx + 1);
      needRebuild = true;
      pTempSlice->startIdx++;
      if (pTempSlice->startIdx == pTempSlice->endIdx) {
        TD_DLIST_POP(pTempList, pTempSlice);
        pTempSlice = TD_DLIST_HEAD(pTempList);
        if (pTempSlice == NULL) {
          continue;
        }
      }
    }
    endTs = TMIN(endTs, pTsData[pTempSlice->startIdx]);
  }
  QUERY_CHECK_CONDITION(endTs > startTs, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (pTsData[pSlice->endIdx - 1] < endTs) {
    *pEndIdx = pSlice->endIdx;
    TD_DLIST_POP(pList, pSlice);
    pSlice = TD_DLIST_HEAD(pList);
  } else {
    void *px = taosbsearch(&endTs, pTsData + pSlice->startIdx, pSlice->endIdx - pSlice->startIdx, sizeof(int64_t),
                           compareInt64Val, TD_GE);
    QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    *pEndIdx = POINTER_DISTANCE(px, pTsData) / sizeof(int64_t);
    pSlice->startIdx = *pEndIdx;
  }

  ST_TASK_DLOG("Slice List %d: pop [%d, %d)", idx, *pStartIdx, *pEndIdx);

  if (needRebuild) {
    code = tMergeTreeRebuild(pSorter->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pSlice == NULL || pTsData[pSlice->startIdx] >= endTs) {
    code = tMergeTreeAdjust(pSorter->pDataMerger, tMergeTreeGetAdjustIndex(pSorter->pDataMerger));
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

typedef struct SNewVtableMergerReaderInfo {
  SSTriggerNewTimestampSorter *pReader;
  SSTriggerTableColRef        *pColRef;
  int32_t                      startIdx;
  int32_t                      endIdx;
} SNewVtableMergerReaderInfo;

static int32_t stNewVtableMergerReaderInfoCompare(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(const int32_t *)pLeft;
  int32_t right = *(const int32_t *)pRight;
  SArray *pReaderInfos = (SArray *)param;

  if (left < TARRAY_SIZE(pReaderInfos) && right < TARRAY_SIZE(pReaderInfos)) {
    SNewVtableMergerReaderInfo *pLeftReaderInfo = TARRAY_GET_ELEM(pReaderInfos, left);
    SNewVtableMergerReaderInfo *pRightReaderInfo = TARRAY_GET_ELEM(pReaderInfos, right);

    int64_t leftTs = INT64_MAX;
    if (pLeftReaderInfo->startIdx < pLeftReaderInfo->endIdx) {
      SColumnInfoData *pTsCol =
          TARRAY_GET_ELEM(pLeftReaderInfo->pReader->pDataBlock->pDataBlock, pLeftReaderInfo->pReader->tsSlotId);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      leftTs = pTsData[pLeftReaderInfo->startIdx];
    }
    int64_t rightTs = INT64_MAX;
    if (pRightReaderInfo->startIdx < pRightReaderInfo->endIdx) {
      SColumnInfoData *pTsCol =
          TARRAY_GET_ELEM(pRightReaderInfo->pReader->pDataBlock->pDataBlock, pRightReaderInfo->pReader->tsSlotId);
      int64_t *pTsData = (int64_t *)pTsCol->pData;
      rightTs = pTsData[pRightReaderInfo->startIdx];
    }

    if (leftTs < rightTs) {
      return -1;
    } else if (leftTs > rightTs) {
      return 1;
    }
  }
  // fallback to index comparison
  if (left < right) {
    return -1;
  } else if (left > right) {
    return 1;
  }
  return 0;
}

#define VTABLE_MERGER_NROWS_PER_BLOCK 4096

static void stNewVtableMergerDestoryReaderInfo(void *ptr) {
  SNewVtableMergerReaderInfo *pInfo = ptr;
  if (pInfo && pInfo->pReader != NULL) {
    stNewTimestampSorterDestroy(&pInfo->pReader);
  }
}

int32_t stNewVtableMergerInit(SSTriggerNewVtableMerger *pMerger, struct SStreamTriggerTask *pTask,
                              SSDataBlock *pDataBlock, bool *pIsPseudoCol, SNode *pFilter) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pMerger->pTask = pTask;
  code = createOneDataBlock(pDataBlock, false, &pMerger->pDataBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  bool hasPseudo = false;
  for (int32_t i = 0; i < TARRAY_SIZE(pMerger->pDataBlock->pDataBlock); i++) {
    if (pIsPseudoCol[i]) {
      hasPseudo = true;
      break;
    }
  }
  pMerger->pIsPseudoCol = hasPseudo ? pIsPseudoCol : NULL;
  code = filterInitFromNode(pFilter, &pMerger->pFilter, 0, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDataEnsureCapacity(pMerger->pDataBlock, VTABLE_MERGER_NROWS_PER_BLOCK);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pMerger->pIsPseudoCol != NULL) {
    pMerger->pPseudoColValues = taosMemoryCalloc(1, sizeof(SSDataBlock));
    QUERY_CHECK_NULL(pMerger->pPseudoColValues, code, lino, _end, terrno);
  }

  pMerger->pReaderInfos = taosArrayInit(0, sizeof(SNewVtableMergerReaderInfo));
  QUERY_CHECK_NULL(pMerger->pReaderInfos, code, lino, _end, terrno);

  if (pMerger->pFilter != NULL) {
    SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pMerger->pDataBlock->pDataBlock),
                                .pDataBlock = pMerger->pDataBlock->pDataBlock};
    code = filterSetDataFromSlotId(pMerger->pFilter, &param);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

bool stNewVtableMergerNeedPseudoCols(SSTriggerNewVtableMerger *pMerger) {
  return (pMerger->pIsPseudoCol != NULL) && (blockDataGetNumOfRows(pMerger->pPseudoColValues) == 0);
}

void stNewVtableMergerDestroy(void *ptr) {
  SSTriggerNewVtableMerger **ppMerger = ptr;
  if (ppMerger == NULL || *ppMerger == NULL) {
    return;
  }

  SSTriggerNewVtableMerger *pMerger = *ppMerger;
  if (pMerger->pDataBlock != NULL) {
    blockDataDestroy(pMerger->pDataBlock);
    pMerger->pDataBlock = NULL;
  }
  if (pMerger->pFilter != NULL) {
    filterFreeInfo(pMerger->pFilter);
    pMerger->pFilter = NULL;
  }

  if (pMerger->pPseudoColValues != NULL) {
    blockDataDestroy(pMerger->pPseudoColValues);
    pMerger->pPseudoColValues = NULL;
  }

  if (pMerger->pReaderInfos != NULL) {
    taosArrayDestroyEx(pMerger->pReaderInfos, stNewVtableMergerDestoryReaderInfo);
    pMerger->pReaderInfos = NULL;
  }
  if (pMerger->pDataMerger != NULL) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
    pMerger->pDataMerger = NULL;
  }
  taosMemFreeClear(*ppMerger);
}

void stNewVtableMergerReset(SSTriggerNewVtableMerger *pMerger) {
  if (pMerger == NULL) {
    return;
  }

  pMerger->inUse = false;
  blockDataEmpty(pMerger->pDataBlock);

  if (pMerger->pPseudoColValues != NULL) {
    blockDataEmpty(pMerger->pPseudoColValues);
  }

  if (pMerger->pReaderInfos != NULL) {
    for (int32_t i = 0; i < TARRAY_SIZE(pMerger->pReaderInfos); i++) {
      SNewVtableMergerReaderInfo *pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, i);
      stNewTimestampSorterReset(pReaderInfo->pReader);
      pReaderInfo->startIdx = 0;
      pReaderInfo->endIdx = 0;
    }
    pMerger->nReaders = 0;
  }
}

int32_t stNewVtableMergerSetData(SSTriggerNewVtableMerger *pMerger, int64_t vtbUid, int32_t tsSlotId,
                                 STimeWindow *pReadRange, SObjList *pTableUids, SArray *pTableColRefs,
                                 SSHashObj *pMetas, SSHashObj *pSlices) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;

  QUERY_CHECK_CONDITION(!pMerger->inUse, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(!stNewVtableMergerNeedPseudoCols(pMerger), code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger->inUse = true;
  pMerger->nReaders = 0;
  pMerger->pDataBlock->info.id.uid = vtbUid;
  pMerger->tsSlotId = tsSlotId;

  int32_t      nTables = taosArrayGetSize(pTableColRefs);
  int64_t     *ar = NULL;
  SObjListIter iter = {0};
  taosObjListInitIter(pTableUids, &iter, TOBJLIST_ITER_FORWARD);
  while ((ar = taosObjListIterNext(&iter)) != NULL) {
    if (ar[0] != vtbUid) {
      continue;
    }
    int64_t               otbUid = ar[1];
    SSTriggerTableColRef *pColRef = NULL;
    for (int32_t i = 0; i < nTables; i++) {
      SSTriggerTableColRef *pTmpColRef = TARRAY_GET_ELEM(pTableColRefs, i);
      if (pTmpColRef->otbUid == otbUid) {
        pColRef = pTmpColRef;
        break;
      }
    }
    SSTriggerDataSlice *pSlice = tSimpleHashGet(pSlices, &otbUid, sizeof(int64_t));
    QUERY_CHECK_NULL(pSlice, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    SNewVtableMergerReaderInfo *pReaderInfo = NULL;
    if (pMerger->nReaders < TARRAY_SIZE(pMerger->pReaderInfos)) {
      pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, pMerger->nReaders);
    } else {
      pReaderInfo = taosArrayReserve(pMerger->pReaderInfos, 1);
      QUERY_CHECK_NULL(pReaderInfo, code, lino, _end, terrno);
    }
    pMerger->nReaders++;
    if (pReaderInfo->pReader == NULL) {
      pReaderInfo->pReader = taosMemoryCalloc(1, sizeof(SSTriggerNewTimestampSorter));
      QUERY_CHECK_NULL(pReaderInfo->pReader, code, lino, _end, terrno);
      code = stNewTimestampSorterInit(pReaderInfo->pReader, pTask, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    SObjList *pMeta = tSimpleHashGet(pMetas, &pColRef->otbVgId, sizeof(int32_t));
    QUERY_CHECK_NULL(pMeta, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    code = stNewTimestampSorterSetData(pReaderInfo->pReader, pColRef->otbUid, tsSlotId, pReadRange, pMeta, pSlice);
    QUERY_CHECK_CODE(code, lino, _end);
    pReaderInfo->pColRef = pColRef;
    code = stNewTimestampSorterNextDataBlock(pReaderInfo->pReader, NULL, &pReaderInfo->startIdx, &pReaderInfo->endIdx);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pMerger->nReaders == 0) {
    goto _end;
  }

  if (pMerger->pDataMerger != NULL && pMerger->pDataMerger->numOfSources < pMerger->nReaders) {
    tMergeTreeDestroy(&pMerger->pDataMerger);
  }
  if (pMerger->pDataMerger == NULL) {
    // round up to the nearest multiple of 8
    int32_t capacity = (pMerger->nReaders + 7) / 8 * 8;
    code = tMergeTreeCreate(&pMerger->pDataMerger, capacity, pMerger->pReaderInfos, stNewVtableMergerReaderInfoCompare);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = tMergeTreeRebuild(pMerger->pDataMerger);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t stNewVtableMergerDoRetrieve(SSTriggerNewVtableMerger *pMerger, bool *filled) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;

  QUERY_CHECK_CONDITION(pMerger->nReaders > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *filled = false;

  int32_t                     idx = tMergeTreeGetChosenIndex(pMerger->pDataMerger);
  SNewVtableMergerReaderInfo *pReaderInfo = TARRAY_GET_ELEM(pMerger->pReaderInfos, idx);
  if (pReaderInfo->startIdx >= pReaderInfo->endIdx) {
    goto _end;
  }

  SSDataBlock     *pVirDataBlock = pMerger->pDataBlock;
  SColumnInfoData *pVirTsCol = taosArrayGet(pVirDataBlock->pDataBlock, pMerger->tsSlotId);
  QUERY_CHECK_NULL(pVirTsCol, code, lino, _end, terrno);
  int64_t *pVirTsData = (int64_t *)pVirTsCol->pData;

  SSDataBlock     *pOrigDataBlock = pReaderInfo->pReader->pDataBlock;
  SColumnInfoData *pOrigTsCol = taosArrayGet(pOrigDataBlock->pDataBlock, pMerger->tsSlotId);
  QUERY_CHECK_NULL(pOrigTsCol, code, lino, _end, terrno);
  int64_t *pOrigTsData = (int64_t *)pOrigTsCol->pData;

  int32_t virStartIdx = blockDataGetNumOfRows(pVirDataBlock);
  if (virStartIdx > 0 && pVirTsData[virStartIdx - 1] == pOrigTsData[pReaderInfo->startIdx]) {
    // merge to the last row
    --virStartIdx;
  }

  if (virStartIdx >= VTABLE_MERGER_NROWS_PER_BLOCK) {
    // current virtual data block is full
    goto _end;
  }

  *filled = true;
  int32_t nCols = taosArrayGetSize(pReaderInfo->pColRef->pNewColMatches);
  if (pMerger->nReaders == 1) {
    // copy whole data block from the single original table
    int32_t nRowsToCopy =
        TMIN(VTABLE_MERGER_NROWS_PER_BLOCK - virStartIdx, pReaderInfo->endIdx - pReaderInfo->startIdx);
    code = colDataAssignNRows(pVirTsCol, virStartIdx, pOrigTsCol, pReaderInfo->startIdx, nRowsToCopy);
    QUERY_CHECK_CODE(code, lino, _end);
    for (int32_t i = 0; i < nCols; i++) {
      SSTriggerColMatch *pColMatch = TARRAY_GET_ELEM(pReaderInfo->pColRef->pNewColMatches, i);
      SColumnInfoData   *pVirCol = taosArrayGet(pVirDataBlock->pDataBlock, pColMatch->vtbSlotId);
      QUERY_CHECK_NULL(pVirCol, code, lino, _end, terrno);
      SColumnInfoData *pOrigCol = taosArrayGet(pOrigDataBlock->pDataBlock, pColMatch->otbSlotId);
      QUERY_CHECK_NULL(pOrigCol, code, lino, _end, terrno);
      code = colDataAssignNRows(pVirCol, virStartIdx, pOrigCol, pReaderInfo->startIdx, nRowsToCopy);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    pReaderInfo->startIdx += nRowsToCopy;
    pVirDataBlock->info.rows = virStartIdx + nRowsToCopy;
    if (pReaderInfo->startIdx >= pReaderInfo->endIdx) {
      // this original data block is fully processed
      code =
          stNewTimestampSorterNextDataBlock(pReaderInfo->pReader, NULL, &pReaderInfo->startIdx, &pReaderInfo->endIdx);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    // copy single row from the selected original table
    QUERY_CHECK_CONDITION(!colDataIsNull_s(pOrigTsCol, pReaderInfo->startIdx), code, lino, _end,
                          TSDB_CODE_INVALID_PARA);
    char *pData = colDataGetData(pOrigTsCol, pReaderInfo->startIdx);
    code = colDataSetVal(pVirTsCol, virStartIdx, pData, false);
    QUERY_CHECK_CODE(code, lino, _end);
    for (int32_t i = 0; i < nCols; i++) {
      SSTriggerColMatch *pColMatch = TARRAY_GET_ELEM(pReaderInfo->pColRef->pNewColMatches, i);
      SColumnInfoData   *pVirCol = taosArrayGet(pVirDataBlock->pDataBlock, pColMatch->vtbSlotId);
      QUERY_CHECK_NULL(pVirCol, code, lino, _end, terrno);
      SColumnInfoData *pOrigCol = taosArrayGet(pOrigDataBlock->pDataBlock, pColMatch->otbSlotId);
      QUERY_CHECK_NULL(pOrigCol, code, lino, _end, terrno);
      if (colDataIsNull_s(pOrigCol, pReaderInfo->startIdx)) {
        colDataSetNULL(pVirCol, virStartIdx);
      } else {
        pData = colDataGetData(pOrigCol, pReaderInfo->startIdx);
        code = colDataSetVal(pVirCol, virStartIdx, pData, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    pVirDataBlock->info.rows = virStartIdx + 1;
    pReaderInfo->startIdx++;
    if (pReaderInfo->startIdx >= pReaderInfo->endIdx) {
      // this original data block is fully processed
      code =
          stNewTimestampSorterNextDataBlock(pReaderInfo->pReader, NULL, &pReaderInfo->startIdx, &pReaderInfo->endIdx);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = tMergeTreeAdjust(pMerger->pDataMerger, tMergeTreeGetAdjustIndex(pMerger->pDataMerger));
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t stNewVtableMergerNextDataBlock(SSTriggerNewVtableMerger *pMerger, SSDataBlock **ppDataBlock, int32_t *pStartIdx,
                                       int32_t *pEndIdx) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask *pTask = pMerger->pTask;
  SColumnInfoData    *p = NULL;

  QUERY_CHECK_CONDITION(pMerger->inUse, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (ppDataBlock != NULL) {
    *ppDataBlock = NULL;
  }
  *pStartIdx = 0;
  *pEndIdx = 0;

  if (pMerger->nReaders == 0) {
    goto _end;
  }

_retrieve:
  blockDataReset(pMerger->pDataBlock);
  // set all columns to NULL by default
  int32_t ncols = blockDataGetNumOfCols(pMerger->pDataBlock);
  for (int32_t i = 0; i < ncols; i++) {
    SColumnInfoData *pCol = TARRAY_GET_ELEM(pMerger->pDataBlock->pDataBlock, i);
    colDataSetNNULL(pCol, 0, VTABLE_MERGER_NROWS_PER_BLOCK);
  }
  bool filled = true;
  while (filled) {
    code = stNewVtableMergerDoRetrieve(pMerger, &filled);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  int32_t nrows = blockDataGetNumOfRows(pMerger->pDataBlock);
  if (nrows > 0) {
    if (pMerger->pIsPseudoCol != NULL) {
      int32_t j = 0;
      for (int32_t i = 0; i < ncols; i++) {
        if (!pMerger->pIsPseudoCol[i]) {
          continue;
        }
        SColumnInfoData *pSrc = taosArrayGet(pMerger->pPseudoColValues->pDataBlock, j);
        QUERY_CHECK_NULL(pSrc, code, lino, _end, terrno);
        j++;
        SColumnInfoData *pDst = TARRAY_GET_ELEM(pMerger->pDataBlock->pDataBlock, i);
        if (!colDataIsNull_s(pSrc, 0)) {
          if (!IS_VAR_DATA_TYPE(pDst->info.type) && pDst->nullbitmap != NULL) {
            int32_t bmLen = BitmapLen(nrows);
            memset(pDst->nullbitmap, 0, bmLen);
          }
          code = colDataCopyNItems(pDst, 0, colDataGetData(pSrc, 0), nrows, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
    if (pMerger->pFilter != NULL) {
      int32_t status = 0;
      code = filterExecute(pMerger->pFilter, pMerger->pDataBlock, &p, NULL, ncols, &status);
      QUERY_CHECK_CODE(code, lino, _end);
      code = trimDataBlock(pMerger->pDataBlock, nrows, (bool *)p->pData);
      QUERY_CHECK_CODE(code, lino, _end);
      int32_t nrowsAfterFilter = blockDataGetNumOfRows(pMerger->pDataBlock);
      ST_TASK_DLOG("vtable merger filter applied, before: %d, after: %d", nrows, nrowsAfterFilter);
      if (nrowsAfterFilter == 0 && nrows == VTABLE_MERGER_NROWS_PER_BLOCK) {
        // need to continue retrieve data until at least one row passes the filter
        goto _retrieve;
      }
      nrows = nrowsAfterFilter;
    }
  }

  if (nrows == 0) {
    goto _end;
  }

  printDataBlock(pMerger->pDataBlock, __func__, "stream_vtable_data", pTask->task.streamId);

  if (ppDataBlock != NULL) {
    *ppDataBlock = pMerger->pDataBlock;
  }
  *pStartIdx = 0;
  *pEndIdx = nrows;

_end:
  if (p != NULL) {
    colDataDestroy(p);
    taosMemoryFreeClear(p);
  }
  if (TSDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
