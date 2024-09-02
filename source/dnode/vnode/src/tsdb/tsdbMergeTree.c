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

#include "tsdb.h"
#include "tsdbFSet2.h"
#include "tsdbMerge.h"
#include "tsdbReadUtil.h"
#include "tsdbSttFileRW.h"
#include "tsdbUtil2.h"

static void tLDataIterClose2(SLDataIter *pIter);

// SLDataIter =================================================
int32_t tCreateSttBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols, SSttBlockLoadInfo **pInfo) {
  *pInfo = NULL;

  SSttBlockLoadInfo *pLoadInfo = taosMemoryCalloc(1, sizeof(SSttBlockLoadInfo));
  if (pLoadInfo == NULL) {
    return terrno;
  }

  pLoadInfo->blockData[0].sttBlockIndex = -1;
  pLoadInfo->blockData[1].sttBlockIndex = -1;

  pLoadInfo->currentLoadBlockIndex = 1;

  int32_t code = tBlockDataCreate(&pLoadInfo->blockData[0].data);
  if (code) {
    taosMemoryFreeClear(pLoadInfo);
    return code;
  }

  code = tBlockDataCreate(&pLoadInfo->blockData[1].data);
  if (code) {
    taosMemoryFreeClear(pLoadInfo);
    return code;
  }

  pLoadInfo->aSttBlk = taosArrayInit(4, sizeof(SSttBlk));
  if (pLoadInfo->aSttBlk == NULL) {
    taosMemoryFreeClear(pLoadInfo);
    return terrno;
  }

  pLoadInfo->pSchema = pSchema;
  pLoadInfo->colIds = colList;
  pLoadInfo->numOfCols = numOfCols;

  *pInfo = pLoadInfo;
  return code;
}

static void freeItem(void *pValue) {
  SValue *p = (SValue *)pValue;
  if (IS_VAR_DATA_TYPE(p->type)) {
    taosMemoryFree(p->pData);
  }
}

void destroySttBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo) {
  if (pLoadInfo == NULL) {
    return;
  }

  pLoadInfo->currentLoadBlockIndex = 1;

  SBlockDataInfo *pInfo = &pLoadInfo->blockData[0];
  tBlockDataDestroy(&pInfo->data);
  pInfo->sttBlockIndex = -1;
  pInfo->pin = false;

  pInfo = &pLoadInfo->blockData[1];
  tBlockDataDestroy(&pInfo->data);
  pInfo->sttBlockIndex = -1;
  pInfo->pin = false;

  taosArrayDestroy(pLoadInfo->info.pUid);
  taosArrayDestroyEx(pLoadInfo->info.pFirstKey, freeItem);
  taosArrayDestroyEx(pLoadInfo->info.pLastKey, freeItem);
  taosArrayDestroy(pLoadInfo->info.pCount);
  taosArrayDestroy(pLoadInfo->info.pFirstTs);
  taosArrayDestroy(pLoadInfo->info.pLastTs);

  pLoadInfo->info.pUid = NULL;
  pLoadInfo->info.pFirstKey = NULL;
  pLoadInfo->info.pLastKey = NULL;
  pLoadInfo->info.pCount = NULL;
  pLoadInfo->info.pFirstTs = NULL;
  pLoadInfo->info.pLastTs = NULL;

  taosArrayDestroy(pLoadInfo->aSttBlk);
  taosMemoryFree(pLoadInfo);
}

void destroyLDataIter(SLDataIter *pIter) {
  tLDataIterClose2(pIter);
  destroySttBlockLoadInfo(pIter->pBlockLoadInfo);
  taosMemoryFree(pIter);
}

void destroySttBlockReader(SArray *pLDataIterArray, SSttBlockLoadCostInfo *pLoadCost) {
  if (pLDataIterArray == NULL) {
    return;
  }

  int32_t numOfLevel = taosArrayGetSize(pLDataIterArray);
  for (int32_t i = 0; i < numOfLevel; ++i) {
    SArray *pList = taosArrayGetP(pLDataIterArray, i);
    for (int32_t j = 0; j < taosArrayGetSize(pList); ++j) {
      SLDataIter *pIter = taosArrayGetP(pList, j);
      if (pIter->pBlockLoadInfo != NULL) {
        SSttBlockLoadCostInfo *pCost = &pIter->pBlockLoadInfo->cost;
        if (pLoadCost != NULL) {
          pLoadCost->loadBlocks += pCost->loadBlocks;
          pLoadCost->loadStatisBlocks += pCost->loadStatisBlocks;
          pLoadCost->blockElapsedTime += pCost->blockElapsedTime;
          pLoadCost->statisElapsedTime += pCost->statisElapsedTime;
        }
      }

      destroyLDataIter(pIter);
    }

    taosArrayDestroy(pList);
  }

  taosArrayDestroy(pLDataIterArray);
}

// choose the unpinned slot to load next data block
static void updateBlockLoadSlot(SSttBlockLoadInfo *pLoadInfo) {
  int32_t nextSlotIndex = pLoadInfo->currentLoadBlockIndex ^ 1;
  if (pLoadInfo->blockData[nextSlotIndex].pin) {
    nextSlotIndex = nextSlotIndex ^ 1;
  }

  pLoadInfo->currentLoadBlockIndex = nextSlotIndex;
}

static int32_t loadLastBlock(SLDataIter *pIter, const char *idStr, SBlockData **pResBlock) {
  if (pResBlock != NULL) {
    *pResBlock = NULL;
  }

  int32_t            code = 0;
  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;

  if (pInfo->blockData[0].sttBlockIndex == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 0) {
      tsdbDebug("current load index is set to 0, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 0;
    }

    *pResBlock = &pInfo->blockData[0].data;
    return code;
  }

  if (pInfo->blockData[1].sttBlockIndex == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 1) {
      tsdbDebug("current load index is set to 1, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 1;
    }

    *pResBlock = &pInfo->blockData[1].data;
    return code;
  }

  if (pIter->pSttBlk == NULL || pInfo->pSchema == NULL) {
    return code;
  }

  updateBlockLoadSlot(pInfo);
  int64_t st = taosGetTimestampUs();

  SBlockData *pBlock = &pInfo->blockData[pInfo->currentLoadBlockIndex].data;
  code = tsdbSttFileReadBlockDataByColumn(pIter->pReader, pIter->pSttBlk, pBlock, pInfo->pSchema, &pInfo->colIds[1],
                                          pInfo->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pInfo->cost.blockElapsedTime += el;
  pInfo->cost.loadBlocks += 1;

  tsdbDebug("read stt block, total load:%" PRId64 ", trigger by uid:%" PRIu64 ", stt-fileVer:%" PRId64
            ", last block index:%d, entry:%d, rows:%d, uidRange:%" PRId64 "-%" PRId64 " tsRange:%" PRId64 "-%" PRId64
            " %p, elapsed time:%.2f ms, %s",
            pInfo->cost.loadBlocks, pIter->uid, pIter->cid, pIter->iSttBlk, pInfo->currentLoadBlockIndex, pBlock->nRow,
            pIter->pSttBlk->minUid, pIter->pSttBlk->maxUid, pIter->pSttBlk->minKey, pIter->pSttBlk->maxKey, pBlock, el,
            idStr);

  pInfo->blockData[pInfo->currentLoadBlockIndex].sttBlockIndex = pIter->iSttBlk;
  pIter->iRow = (pIter->backward) ? pInfo->blockData[pInfo->currentLoadBlockIndex].data.nRow : -1;

  tsdbDebug("last block index list:%d, %d, rowIndex:%d %s", pInfo->blockData[0].sttBlockIndex,
            pInfo->blockData[1].sttBlockIndex, pIter->iRow, idStr);

  *pResBlock = &pInfo->blockData[pInfo->currentLoadBlockIndex].data;
  return code;
}

// find the earliest block that contains the required records
static FORCE_INLINE int32_t findEarliestIndex(int32_t index, uint64_t uid, const SSttBlk *pBlockList, int32_t num,
                                              int32_t backward) {
  int32_t i = index;
  int32_t step = backward ? 1 : -1;
  while (i >= 0 && i < num && uid >= pBlockList[i].minUid && uid <= pBlockList[i].maxUid) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartBlock(SSttBlk *pBlockList, int32_t num, uint64_t uid, int32_t backward) {
  int32_t midPos = -1;
  if (num <= 0) {
    return -1;
  }

  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  // find the first position which is bigger than the key
  if ((uid > pBlockList[lastPos].maxUid) || (uid < pBlockList[firstPos].minUid)) {
    return -1;
  }

  while (1) {
    if (uid >= pBlockList[firstPos].minUid && uid <= pBlockList[firstPos].maxUid) {
      return findEarliestIndex(firstPos, uid, pBlockList, num, backward);
    }

    if (uid > pBlockList[lastPos].maxUid || uid < pBlockList[firstPos].minUid) {
      return -1;
    }

    int32_t numOfRows = lastPos - firstPos + 1;
    midPos = (numOfRows >> 1u) + firstPos;

    if (uid < pBlockList[midPos].minUid) {
      lastPos = midPos - 1;
    } else if (uid > pBlockList[midPos].maxUid) {
      firstPos = midPos + 1;
    } else {
      return findEarliestIndex(midPos, uid, pBlockList, num, backward);
    }
  }
}

static FORCE_INLINE int32_t findEarliestRow(int32_t index, uint64_t uid, const uint64_t *uidList, int32_t num,
                                            int32_t backward) {
  int32_t i = index;
  int32_t step = backward ? 1 : -1;
  while (i >= 0 && i < num && uid == uidList[i]) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartRowIndex(uint64_t *uidList, int32_t num, uint64_t uid, int32_t backward) {
  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  // find the first position which is bigger than the key
  if ((uid > uidList[lastPos]) || (uid < uidList[firstPos])) {
    return -1;
  }

  while (1) {
    if (uid == uidList[firstPos]) {
      return findEarliestRow(firstPos, uid, uidList, num, backward);
    }

    if (uid > uidList[lastPos] || uid < uidList[firstPos]) {
      return -1;
    }

    int32_t numOfRows = lastPos - firstPos + 1;
    int32_t midPos = (numOfRows >> 1u) + firstPos;

    if (uid < uidList[midPos]) {
      lastPos = midPos - 1;
    } else if (uid > uidList[midPos]) {
      firstPos = midPos + 1;
    } else {
      return findEarliestRow(midPos, uid, uidList, num, backward);
    }
  }
}

static int32_t extractSttBlockInfo(SLDataIter *pIter, const TSttBlkArray *pArray, SSttBlockLoadInfo *pBlockLoadInfo,
                                   uint64_t suid) {
  void   *px = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  if (TARRAY2_SIZE(pArray) <= 0) {
    return code;
  }

  SSttBlk *pStart = &pArray->data[0];
  SSttBlk *pEnd = &pArray->data[TARRAY2_SIZE(pArray) - 1];

  // all identical
  if (pStart->suid == pEnd->suid) {
    if (pStart->suid != suid) {  // no qualified stt block existed
      taosArrayClear(pBlockLoadInfo->aSttBlk);
      pIter->iSttBlk = -1;
      return TSDB_CODE_SUCCESS;
    } else {  // all blocks are qualified
      taosArrayClear(pBlockLoadInfo->aSttBlk);
      px = taosArrayAddBatch(pBlockLoadInfo->aSttBlk, pArray->data, pArray->size);
      if (px == NULL) {
        return terrno;
      }
    }
  } else {
    SArray *pTmp = taosArrayInit(TARRAY2_SIZE(pArray), sizeof(SSttBlk));
    if (pTmp == NULL) {
      return terrno;
    }

    for (int32_t i = 0; i < TARRAY2_SIZE(pArray); ++i) {
      SSttBlk *p = &pArray->data[i];
      if (p->suid < suid) {
        continue;
      }

      if (p->suid == suid) {
        void *px = taosArrayPush(pTmp, p);
        if (px == NULL) {
          code = terrno;
          break;
        }
      } else if (p->suid > suid) {
        break;
      }
    }

    taosArrayDestroy(pBlockLoadInfo->aSttBlk);
    pBlockLoadInfo->aSttBlk = pTmp;
  }

  return code;
}

static int32_t tValueDupPayload(SValue *pVal) {
  if (IS_VAR_DATA_TYPE(pVal->type)) {
    char *p = (char *)pVal->pData;
    char *pBuf = taosMemoryMalloc(pVal->nData);
    if (pBuf == NULL) {
      return terrno;
    }

    memcpy(pBuf, p, pVal->nData);
    pVal->pData = (uint8_t *)pBuf;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t loadSttStatisticsBlockData(SSttFileReader *pSttFileReader, SSttBlockLoadInfo *pBlockLoadInfo,
                                          TStatisBlkArray *pStatisBlkArray, uint64_t suid, const char *id) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void   *px = NULL;
  int32_t startIndex = 0;

  int32_t numOfBlocks = TARRAY2_SIZE(pStatisBlkArray);
  if (numOfBlocks <= 0) {
    return code;
  }

  while ((startIndex < numOfBlocks) && (pStatisBlkArray->data[startIndex].maxTbid.suid < suid)) {
    ++startIndex;
  }

  if (startIndex >= numOfBlocks || pStatisBlkArray->data[startIndex].minTbid.suid > suid) {
    return 0;
  }

  int32_t endIndex = startIndex;
  while (endIndex < numOfBlocks && pStatisBlkArray->data[endIndex].minTbid.suid <= suid) {
    ++endIndex;
  }

  int32_t num = endIndex - startIndex;
  pBlockLoadInfo->cost.loadStatisBlocks += num;

  STbStatisBlock block;
  TAOS_UNUSED(tStatisBlockInit(&block));

  int64_t st = taosGetTimestampUs();

  for (int32_t k = startIndex; k < endIndex; ++k) {
    code = tsdbSttFileReadStatisBlock(pSttFileReader, &pStatisBlkArray->data[k], &block);
    if (code) {
      return code;
    }

    int32_t i = 0;
    int32_t rows = block.numOfRecords;
    while (i < rows && ((int64_t *)block.suids.data)[i] != suid) {
      ++i;
    }

    // existed
    if (i < rows) {
      SSttTableRowsInfo *pInfo = &pBlockLoadInfo->info;

      if (pInfo->pUid == NULL) {
        pInfo->pUid = taosArrayInit(rows, sizeof(int64_t));
        pInfo->pFirstTs = taosArrayInit(rows, sizeof(int64_t));
        pInfo->pLastTs = taosArrayInit(rows, sizeof(int64_t));
        pInfo->pCount = taosArrayInit(rows, sizeof(int64_t));

        pInfo->pFirstKey = taosArrayInit(rows, sizeof(SValue));
        pInfo->pLastKey = taosArrayInit(rows, sizeof(SValue));

        if (pInfo->pUid == NULL || pInfo->pFirstTs == NULL || pInfo->pLastTs == NULL || pInfo->pCount == NULL ||
            pInfo->pFirstKey == NULL || pInfo->pLastKey == NULL) {
          code = terrno;
          goto _end;
        }
      }

      if (pStatisBlkArray->data[k].maxTbid.suid == suid) {
        int32_t size = rows - i;
        int32_t offset = i * sizeof(int64_t);

        px = taosArrayAddBatch(pInfo->pUid, tBufferGetDataAt(&block.uids, offset), size);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);

        px = taosArrayAddBatch(pInfo->pFirstTs, tBufferGetDataAt(&block.firstKeyTimestamps, offset), size);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);

        px = taosArrayAddBatch(pInfo->pLastTs, tBufferGetDataAt(&block.lastKeyTimestamps, offset), size);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);

        px = taosArrayAddBatch(pInfo->pCount, tBufferGetDataAt(&block.counts, offset), size);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);

        if (block.numOfPKs > 0) {
          SValue vFirst = {0}, vLast = {0};
          for (int32_t f = i; f < rows; ++f) {
            code = tValueColumnGet(&block.firstKeyPKs[0], f, &vFirst);
            TSDB_CHECK_CODE(code, lino, _end);

            code = tValueDupPayload(&vFirst);
            TSDB_CHECK_CODE(code, lino, _end);

            px = taosArrayPush(pInfo->pFirstKey, &vFirst);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);

            // todo add api to clone the original data
            code = tValueColumnGet(&block.lastKeyPKs[0], f, &vLast);
            TSDB_CHECK_CODE(code, lino, _end);

            code = tValueDupPayload(&vLast);
            TSDB_CHECK_CODE(code, lino, _end);

            px = taosArrayPush(pInfo->pLastKey, &vLast);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);
          }
        } else {
          SValue vFirst = {0};
          for (int32_t j = 0; j < size; ++j) {
            px = taosArrayPush(pInfo->pFirstKey, &vFirst);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);

            px = taosArrayPush(pInfo->pLastKey, &vFirst);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);
          }
        }
      } else {
        STbStatisRecord record = {0};
        while (i < rows) {
          (void)tStatisBlockGet(&block, i, &record);
          if (record.suid != suid) {
            break;
          }

          px = taosArrayPush(pInfo->pUid, &record.uid);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);

          px = taosArrayPush(pInfo->pCount, &record.count);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);

          px = taosArrayPush(pInfo->pFirstTs, &record.firstKey.ts);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);

          px = taosArrayPush(pInfo->pLastTs, &record.lastKey.ts);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);

          if (record.firstKey.numOfPKs > 0) {
            SValue s = record.firstKey.pks[0];
            code = tValueDupPayload(&s);
            TSDB_CHECK_CODE(code, lino, _end);

            px = taosArrayPush(pInfo->pFirstKey, &s);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);

            s = record.lastKey.pks[0];
            code = tValueDupPayload(&s);
            TSDB_CHECK_CODE(code, lino, _end);

            px = taosArrayPush(pInfo->pLastKey, &s);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);
          } else {
            SValue v = {0};
            px = taosArrayPush(pInfo->pFirstKey, &v);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);

            px = taosArrayPush(pInfo->pLastKey, &v);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);
          }

          i += 1;
        }
      }
    }
  }

_end:
  (void)tStatisBlockDestroy(&block);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pBlockLoadInfo->cost.statisElapsedTime += el;

  tsdbDebug("%s load %d statis blocks into buf, elapsed time:%.2fms", id, num, el);
  return code;
}

static int32_t doLoadSttFilesBlk(SSttBlockLoadInfo *pBlockLoadInfo, SLDataIter *pIter, int64_t suid,
                                 _load_tomb_fn loadTombFn, void *pReader1, const char *idStr) {
  int64_t st = taosGetTimestampUs();

  const TSttBlkArray *pSttBlkArray = NULL;
  pBlockLoadInfo->sttBlockLoaded = true;

  // load the stt block info for each stt-block
  int32_t code = tsdbSttFileReadSttBlk(pIter->pReader, &pSttBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("load stt blk failed, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load the stt block info for each stt file block
  code = extractSttBlockInfo(pIter, pSttBlkArray, pBlockLoadInfo, suid);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("load stt block info failed, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load stt statistics block for all stt-blocks, to decide if the data of queried table exists in current stt file
  TStatisBlkArray *pStatisBlkArray = NULL;
  code = tsdbSttFileReadStatisBlk(pIter->pReader, (const TStatisBlkArray **)&pStatisBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed to load stt block statistics, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load statistics block for all tables in current stt file
  code = loadSttStatisticsBlockData(pIter->pReader, pIter->pBlockLoadInfo, pStatisBlkArray, suid, idStr);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed to load stt statistics block data, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  code = loadTombFn(pReader1, pIter->pReader, pIter->pBlockLoadInfo);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("load the stt file blk info completed, elapsed time:%.2fms, %s", el, idStr);
  return code;
}

static int32_t uidComparFn(const void *p1, const void *p2) {
  const uint64_t *pFirst = p1;
  const uint64_t *pVal = p2;

  if (*pFirst == *pVal) {
    return 0;
  } else {
    return *pFirst < *pVal ? -1 : 1;
  }
}

static void setSttInfoForCurrentTable(SSttBlockLoadInfo *pLoadInfo, uint64_t uid, SSttKeyRange *pRange,
                                      int64_t *numOfRows) {
  if (pRange == NULL || taosArrayGetSize(pLoadInfo->info.pUid) == 0) {
    return;
  }

  int32_t index = taosArraySearchIdx(pLoadInfo->info.pUid, &uid, uidComparFn, TD_EQ);
  if (index >= 0) {
    pRange->skey.ts = *(int64_t *)taosArrayGet(pLoadInfo->info.pFirstTs, index);
    pRange->ekey.ts = *(int64_t *)taosArrayGet(pLoadInfo->info.pLastTs, index);

    *numOfRows += *(int64_t *)taosArrayGet(pLoadInfo->info.pCount, index);

    if (pRange->skey.numOfPKs > 0) {
      memcpy(&pRange->skey.pks[0], taosArrayGet(pLoadInfo->info.pFirstKey, index), sizeof(SValue));
      memcpy(&pRange->ekey.pks[0], taosArrayGet(pLoadInfo->info.pLastKey, index), sizeof(SValue));
    }
  }
}

int32_t tLDataIterOpen2(SLDataIter *pIter, SSttFileReader *pSttFileReader, int32_t cid, int8_t backward,
                        SMergeTreeConf *pConf, SSttBlockLoadInfo *pBlockLoadInfo, SSttKeyRange *pKeyRange,
                        int64_t *numOfRows, const char *idStr) {
  int32_t code = TSDB_CODE_SUCCESS;

  pIter->uid = pConf->uid;
  pIter->cid = cid;
  pIter->backward = backward;
  pIter->verRange.minVer = pConf->verRange.minVer;
  pIter->verRange.maxVer = pConf->verRange.maxVer;
  pIter->timeWindow.skey = pConf->timewindow.skey;
  pIter->timeWindow.ekey = pConf->timewindow.ekey;

  pIter->pStartRowKey = pConf->pCurRowKey;
  pIter->pReader = pSttFileReader;
  pIter->pBlockLoadInfo = pBlockLoadInfo;

  // open stt file failed, ignore and continue
  if (pIter->pReader == NULL) {
    tsdbError("stt file reader is null, %s", idStr);
    pIter->pSttBlk = NULL;
    pIter->iSttBlk = -1;
    return TSDB_CODE_SUCCESS;
  }

  if (!pBlockLoadInfo->sttBlockLoaded) {
    code = doLoadSttFilesBlk(pBlockLoadInfo, pIter, pConf->suid, pConf->loadTombFn, pConf->pReader, idStr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  setSttInfoForCurrentTable(pBlockLoadInfo, pConf->uid, pKeyRange, numOfRows);

  // find the start block, actually we could load the position to avoid repeatly searching for the start position when
  // the skey is updated.
  size_t size = taosArrayGetSize(pBlockLoadInfo->aSttBlk);
  pIter->iSttBlk = binarySearchForStartBlock(pBlockLoadInfo->aSttBlk->pData, size, pConf->uid, backward);
  if (pIter->iSttBlk != -1) {
    pIter->pSttBlk = taosArrayGet(pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    pIter->iRow = (pIter->backward) ? pIter->pSttBlk->nRow : -1;

    if ((!backward) && ((pConf->strictTimeRange && pIter->pSttBlk->minKey >= pIter->timeWindow.ekey) ||
                        (!pConf->strictTimeRange && pIter->pSttBlk->minKey > pIter->timeWindow.ekey))) {
      pIter->pSttBlk = NULL;
    }

    if (backward && ((pConf->strictTimeRange && pIter->pSttBlk->maxKey <= pIter->timeWindow.skey) ||
                     (!pConf->strictTimeRange && pIter->pSttBlk->maxKey < pIter->timeWindow.skey))) {
      pIter->pSttBlk = NULL;
      pIter->ignoreEarlierTs = true;
    }
  }

  return code;
}

void tLDataIterClose2(SLDataIter *pIter) {
  (void)tsdbSttFileReaderClose(&pIter->pReader);  // always return 0
  pIter->pReader = NULL;
}

void tLDataIterNextBlock(SLDataIter *pIter, const char *idStr) {
  int32_t step = pIter->backward ? -1 : 1;
  int32_t oldIndex = pIter->iSttBlk;

  pIter->iSttBlk += step;

  int32_t index = -1;
  size_t  size = pIter->pBlockLoadInfo->aSttBlk->size;
  for (int32_t i = pIter->iSttBlk; i < size && i >= 0; i += step) {
    SSttBlk *p = taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, i);
    if ((!pIter->backward) && p->minUid > pIter->uid) {
      break;
    }

    if (pIter->backward && p->maxUid < pIter->uid) {
      break;
    }

    // check uid firstly
    if (p->minUid <= pIter->uid && p->maxUid >= pIter->uid) {
      if ((!pIter->backward) && p->minKey > pIter->timeWindow.ekey) {
        break;
      }

      if (pIter->backward && p->maxKey < pIter->timeWindow.skey) {
        break;
      }

      // check time range secondly
      if (p->minKey <= pIter->timeWindow.ekey && p->maxKey >= pIter->timeWindow.skey) {
        if ((!pIter->backward) && p->minVer > pIter->verRange.maxVer) {
          break;
        }

        if (pIter->backward && p->maxVer < pIter->verRange.minVer) {
          break;
        }

        if (p->minVer <= pIter->verRange.maxVer && p->maxVer >= pIter->verRange.minVer) {
          index = i;
          break;
        }
      }
    }
  }

  pIter->pSttBlk = NULL;
  if (index != -1) {
    SSttBlk *p = taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, index);

    pIter->iSttBlk = index;
    pIter->pSttBlk = (SSttBlk *)taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    tsdbDebug("try next stt-file block:%d from %d, trigger by uid:%" PRIu64 ", stt-fileVer:%" PRId64
              ", uidRange:%" PRId64 "-%" PRId64 " %s",
              pIter->iSttBlk, oldIndex, pIter->uid, pIter->cid, p->minUid, p->maxUid, idStr);
  } else {
    tsdbDebug("no more last block qualified, uid:%" PRIu64 ", stt-file block:%d, %s", pIter->uid, oldIndex, idStr);
  }
}

static int32_t findNextValidRow(SLDataIter *pIter, const char *idStr) {
  bool        hasVal = false;
  int32_t     step = pIter->backward ? -1 : 1;
  int32_t     i = pIter->iRow;
  SBlockData *pData = NULL;

  int32_t code = loadLastBlock(pIter, idStr, &pData);
  if (code) {
    tsdbError("failed to load stt block, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // mostly we only need to find the start position for a given table
  if ((((i == 0) && (!pIter->backward)) || (i == pData->nRow - 1 && pIter->backward)) && pData->aUid != NULL) {
    i = binarySearchForStartRowIndex((uint64_t *)pData->aUid, pData->nRow, pIter->uid, pIter->backward);
    if (i == -1) {
      tsdbDebug("failed to find the data in pBlockData, uid:%" PRIu64 " , %s", pIter->uid, idStr);
      pIter->iRow = -1;
      return code;
    }
  }

  for (; i < pData->nRow && i >= 0; i += step) {
    if (pData->aUid != NULL) {
      if (!pIter->backward) {
        if (pData->aUid[i] > pIter->uid) {
          break;
        }
      } else {
        if (pData->aUid[i] < pIter->uid) {
          break;
        }
      }
    }

    int64_t ts = pData->aTSKEY[i];
    if (!pIter->backward) {               // asc
      if (ts > pIter->timeWindow.ekey) {  // no more data
        break;
      } else {
        if (ts < pIter->timeWindow.skey) {
          continue;
        }

        if (ts == pIter->timeWindow.skey && pIter->pStartRowKey->numOfPKs > 0) {
          SRowKey key;
          tColRowGetKey(pData, i, &key);
          int32_t ret = pkCompEx(&key, pIter->pStartRowKey);
          if (ret < 0) {
            continue;
          }
        }
      }
    } else {
      if (ts < pIter->timeWindow.skey) {
        break;
      } else {
        if (ts > pIter->timeWindow.ekey) {
          continue;
        }

        if (ts == pIter->timeWindow.ekey && pIter->pStartRowKey->numOfPKs > 0) {
          SRowKey key;
          tColRowGetKey(pData, i, &key);
          int32_t ret = pkCompEx(&key, pIter->pStartRowKey);
          if (ret > 0) {
            continue;
          }
        }
      }
    }

    int64_t ver = pData->aVersion[i];
    if (ver < pIter->verRange.minVer) {
      continue;
    }

    // todo opt handle desc case
    if (ver > pIter->verRange.maxVer) {
      continue;
    }

    hasVal = true;
    break;
  }

  pIter->iRow = (hasVal) ? i : -1;
  return code;
}

int32_t tLDataIterNextRow(SLDataIter *pIter, const char *idStr, bool *hasNext) {
  int32_t     step = pIter->backward ? -1 : 1;
  int32_t     code = 0;
  int32_t     iBlockL = pIter->iSttBlk;
  SBlockData *pBlockData = NULL;
  int32_t     lino = 0;

  *hasNext = false;

  // no qualified last file block in current file, no need to fetch row
  if (pIter->pSttBlk == NULL) {
    return code;
  }

  code = loadLastBlock(pIter, idStr, &pBlockData);
  if (pBlockData == NULL || code != TSDB_CODE_SUCCESS) {
    lino = __LINE__;
    goto _exit;
  }

  pIter->iRow += step;

  while (1) {
    bool skipBlock = false;
    code = findNextValidRow(pIter, idStr);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pIter->pBlockLoadInfo->checkRemainingRow) {
      skipBlock = true;
      int16_t *aCols = pIter->pBlockLoadInfo->colIds;
      int      nCols = pIter->pBlockLoadInfo->numOfCols;
      bool     isLast = pIter->pBlockLoadInfo->isLast;
      for (int inputColIndex = 0; inputColIndex < nCols; ++inputColIndex) {
        for (int colIndex = 0; colIndex < pBlockData->nColData; ++colIndex) {
          SColData *pColData = &pBlockData->aColData[colIndex];
          int16_t   cid = pColData->cid;

          if (cid == aCols[inputColIndex]) {
            if (isLast && (pColData->flag & HAS_VALUE)) {
              skipBlock = false;
              break;
            } else if (pColData->flag & (HAS_VALUE | HAS_NULL)) {
              skipBlock = false;
              break;
            }
          }
        }
      }
    }

    if (skipBlock || pIter->iRow >= pBlockData->nRow || pIter->iRow < 0) {
      tLDataIterNextBlock(pIter, idStr);
      if (pIter->pSttBlk == NULL) {  // no more data
        goto _exit;
      }
    } else {
      break;
    }

    if (iBlockL != pIter->iSttBlk) {
      code = loadLastBlock(pIter, idStr, &pBlockData);
      if ((pBlockData == NULL) || (code != 0)) {
        lino = __LINE__;
        goto _exit;
      }

      // set start row index
      pIter->iRow = pIter->backward ? pBlockData->nRow - 1 : 0;
    }
  }

  pIter->rInfo.suid = pBlockData->suid;
  pIter->rInfo.uid = pBlockData->uid;
  pIter->rInfo.row = tsdbRowFromBlockData(pBlockData, pIter->iRow);

_exit:
  if (code) {
    tsdbError("failed to exec stt-file nextIter, lino:%d, code:%s, %s", lino, tstrerror(code), idStr);
  }

  *hasNext = (code == TSDB_CODE_SUCCESS) && (pIter->pSttBlk != NULL) && (pBlockData != NULL);
  return code;
}

// SMergeTree =================================================
static FORCE_INLINE int32_t tLDataIterCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  SLDataIter *pIter1 = (SLDataIter *)(((uint8_t *)p1) - offsetof(SLDataIter, node));
  SLDataIter *pIter2 = (SLDataIter *)(((uint8_t *)p2) - offsetof(SLDataIter, node));

  SRowKey rkey1 = {0}, rkey2 = {0};
  tRowGetKeyEx(&pIter1->rInfo.row, &rkey1);
  tRowGetKeyEx(&pIter2->rInfo.row, &rkey2);

  int32_t ret = tRowKeyCompare(&rkey1, &rkey2);
  if (ret < 0) {
    return -1;
  } else if (ret > 0) {
    return 1;
  } else {
    int64_t ver1 = TSDBROW_VERSION(&pIter1->rInfo.row);
    int64_t ver2 = TSDBROW_VERSION(&pIter2->rInfo.row);

    if (ver1 < ver2) {
      return -1;
    } else if (ver1 > ver2) {
      return 1;
    } else {
      return 0;
    }
  }
}

static FORCE_INLINE int32_t tLDataIterDescCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  return -1 * tLDataIterCmprFn(p1, p2);
}

int32_t tMergeTreeOpen2(SMergeTree *pMTree, SMergeTreeConf *pConf, SSttDataInfoForTable *pSttDataInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  pMTree->pIter = NULL;
  pMTree->backward = pConf->backward;
  pMTree->idStr = pConf->idstr;

  if (!pMTree->backward) {  // asc
    tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  } else {  // desc
    tRBTreeCreate(&pMTree->rbt, tLDataIterDescCmprFn);
  }

  pMTree->ignoreEarlierTs = false;

  // no data exists, go to end
  int32_t numOfLevels = ((STFileSet *)pConf->pCurrentFileset)->lvlArr->size;
  if (numOfLevels == 0) {
    goto _end;
  }

  code = adjustSttDataIters(pConf->pSttFileBlockIterArray, pConf->pCurrentFileset);
  if (code) {
    goto _end;
  }

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl *pSttLevel = ((STFileSet *)pConf->pCurrentFileset)->lvlArr->data[j];
    SArray  *pList = taosArrayGetP(pConf->pSttFileBlockIterArray, j);

    for (int32_t i = 0; i < TARRAY2_SIZE(pSttLevel->fobjArr); ++i) {  // open all last file
      SLDataIter *pIter = taosArrayGetP(pList, i);

      SSttFileReader    *pSttFileReader = pIter->pReader;
      SSttBlockLoadInfo *pLoadInfo = pIter->pBlockLoadInfo;

      // open stt file reader if not opened yet
      // if failed to open this stt file, ignore the error and try next one
      if (pSttFileReader == NULL) {
        SSttFileReaderConfig conf = {.tsdb = pConf->pTsdb, .szPage = pConf->pTsdb->pVnode->config.tsdbPageSize};
        conf.file[0] = *pSttLevel->fobjArr->data[i]->f;

        code = tsdbSttFileReaderOpen(pSttLevel->fobjArr->data[i]->fname, &conf, &pSttFileReader);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("open stt file reader error. file name %s, code %s, %s", pSttLevel->fobjArr->data[i]->fname,
                    tstrerror(code), pMTree->idStr);
        }
      }

      if (pLoadInfo == NULL) {
        code = tCreateSttBlockLoadInfo(pConf->pSchema, pConf->pCols, pConf->numOfCols, &pLoadInfo);
        if (code != TSDB_CODE_SUCCESS) {
          goto _end;
        }
      }

      memset(pIter, 0, sizeof(SLDataIter));

      SSttKeyRange range = {.skey.numOfPKs = pConf->pCurRowKey->numOfPKs, .ekey.numOfPKs = pConf->pCurRowKey->numOfPKs};
      int64_t      numOfRows = 0;
      int64_t      cid = pSttLevel->fobjArr->data[i]->f->cid;

      code = tLDataIterOpen2(pIter, pSttFileReader, cid, pMTree->backward, pConf, pLoadInfo, &range, &numOfRows,
                             pMTree->idStr);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      bool hasVal = NULL;
      code = tLDataIterNextRow(pIter, pMTree->idStr, &hasVal);
      if (code) {
        goto _end;
      }

      if (hasVal) {
        tMergeTreeAddIter(pMTree, pIter);

        // let's record the time window for current table of uid in the stt files
        if (pSttDataInfo != NULL && numOfRows > 0) {
          void *px = taosArrayPush(pSttDataInfo->pKeyRangeList, &range);
          if (px == NULL) {
            return terrno;
          }
          pSttDataInfo->numOfRows += numOfRows;
        }
      } else {
        if (!pMTree->ignoreEarlierTs) {
          pMTree->ignoreEarlierTs = pIter->ignoreEarlierTs;
        }
      }
    }
  }

  return code;

_end:
  tMergeTreeClose(pMTree);
  return code;
}

void tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter) { (void)tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pIter); }

bool tMergeTreeIgnoreEarlierTs(SMergeTree *pMTree) { return pMTree->ignoreEarlierTs; }

static void tLDataIterPinSttBlock(SLDataIter *pIter, const char *id) {
  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;

  if (pInfo->blockData[0].sttBlockIndex == pIter->iSttBlk) {
    pInfo->blockData[0].pin = true;
    tsdbTrace("pin stt-block, blockIndex:%d, stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
    return;
  }

  if (pInfo->blockData[1].sttBlockIndex == pIter->iSttBlk) {
    pInfo->blockData[1].pin = true;
    tsdbTrace("pin stt-block, blockIndex:%d, stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
    return;
  }

  tsdbError("failed to pin any stt block, sttBlock:%d stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
}

static void tLDataIterUnpinSttBlock(SLDataIter *pIter, const char *id) {
  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockData[0].pin) {
    pInfo->blockData[0].pin = false;
    tsdbTrace("unpin stt-block:%d, stt-fileVer:%" PRId64 " %s", pInfo->blockData[0].sttBlockIndex, pIter->cid, id);
    return;
  }

  if (pInfo->blockData[1].pin) {
    pInfo->blockData[1].pin = false;
    tsdbTrace("unpin stt-block:%d, stt-fileVer:%" PRId64 " %s", pInfo->blockData[1].sttBlockIndex, pIter->cid, id);
    return;
  }

  tsdbError("failed to unpin any stt block, sttBlock:%d stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
}

void tMergeTreePinSttBlock(SMergeTree *pMTree) {
  if (pMTree->pIter == NULL) {
    return;
  }

  SLDataIter *pIter = pMTree->pIter;
  pMTree->pPinnedBlockIter = pIter;
  tLDataIterPinSttBlock(pIter, pMTree->idStr);
}

void tMergeTreeUnpinSttBlock(SMergeTree *pMTree) {
  if (pMTree->pPinnedBlockIter == NULL) {
    return;
  }

  SLDataIter *pIter = pMTree->pPinnedBlockIter;
  pMTree->pPinnedBlockIter = NULL;
  tLDataIterUnpinSttBlock(pIter, pMTree->idStr);
}

int32_t tMergeTreeNext(SMergeTree *pMTree, bool *pHasNext) {
  int32_t code = 0;
  if (pHasNext == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pHasNext = false;
  if (pMTree->pIter) {
    SLDataIter *pIter = pMTree->pIter;
    bool        hasVal = false;
    code = tLDataIterNextRow(pIter, pMTree->idStr, &hasVal);
    if (!hasVal || (code != 0)) {
      if (code == TSDB_CODE_FILE_CORRUPTED) {
        code = 0;  // suppress the file corrupt error to enable all queries within this cluster can run without failed.
      }

      pMTree->pIter = NULL;
    }

    // compare with min in RB Tree
    pIter = (SLDataIter *)tRBTreeMin(&pMTree->rbt);
    if (pMTree->pIter && pIter) {
      int32_t c = pMTree->rbt.cmprFn(&pMTree->pIter->node, &pIter->node);
      if (c > 0) {
        (void)tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pMTree->pIter);
        pMTree->pIter = NULL;
      } else if (!c) {
        return TSDB_CODE_INTERNAL_ERROR;
      }
    }
  }

  if (pMTree->pIter == NULL) {
    pMTree->pIter = (SLDataIter *)tRBTreeMin(&pMTree->rbt);
    if (pMTree->pIter) {
      tRBTreeDrop(&pMTree->rbt, (SRBTreeNode *)pMTree->pIter);
    }
  }

  *pHasNext = (pMTree->pIter != NULL);
  return code;
}

void tMergeTreeClose(SMergeTree *pMTree) {
  pMTree->pIter = NULL;
  pMTree->pPinnedBlockIter = NULL;
}
