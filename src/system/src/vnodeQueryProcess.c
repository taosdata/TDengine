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

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "taosmsg.h"
#include "textbuffer.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"

#include "vnodeQueryImpl.h"

static bool doCheckWithPrevQueryRange(SQInfo *pQInfo, TSKEY nextKey, SMeterDataInfo *pMeterInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQuery *               pQuery = &pQInfo->query;
  SMeterObj *            pMeterObj = pMeterInfo->pMeterObj;

  /* no data for current query */
  if ((nextKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    if (((nextKey > pSupporter->rawEKey) && QUERY_IS_ASC_QUERY(pQuery)) ||
        ((nextKey < pSupporter->rawEKey) && (!QUERY_IS_ASC_QUERY(pQuery)))) {
      dTrace("QInfo:%p vid:%d sid:%d id:%s, no data qualified in block, ignore", pQInfo, pMeterObj->vnode,
             pMeterObj->sid, pMeterObj->meterId);

      return false;
    } else {  // in case of interval query, forward the query range
      setIntervalQueryRange(pSupporter, nextKey, pMeterInfo);
    }
  }

  return true;
}

static SMeterDataInfo *queryOnMultiDataCache(SQInfo *pQInfo, SMeterDataInfo *pMeterInfo) {
  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;

  SMeterObj *pTempMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[0]->sid);
  assert(pTempMeterObj != NULL);

  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pTempMeterObj->searchAlgorithm];
  int32_t             step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  dTrace("QInfo:%p start to query data in cache", pQInfo);
  int64_t st = taosGetTimestampUs();
  int32_t totalBlocks = 0;

  for (int32_t groupIdx = 0; groupIdx < pSupporter->pSidSet->numOfSubSet; ++groupIdx) {
    int32_t start = pSupporter->pSidSet->starterPos[groupIdx];
    int32_t end = pSupporter->pSidSet->starterPos[groupIdx + 1] - 1;

    if (isQueryKilled(pQuery)) {
      return pMeterInfo;
    }

    for (int32_t k = start; k <= end; ++k) {
      SMeterObj *pMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[k]->sid);
      if (pMeterObj == NULL) {
        dError("QInfo:%p failed to find meterId:%d, continue", pQInfo, pMeterSidExtInfo[k]->sid);
        continue;
      }

      pQInfo->pObj = pMeterObj;
      pRuntimeEnv->pMeterObj = pMeterObj;

      setMeterQueryInfo(pSupporter, &pMeterInfo[k]);
      if (pMeterInfo[k].pMeterObj == NULL) { /* no data in disk for this meter, set its pointer */
        setMeterDataInfo(&pMeterInfo[k], pMeterObj, k, groupIdx);
      }

      assert(pMeterInfo[k].meterOrderIdx == k && pMeterObj == pMeterInfo[k].pMeterObj);

      SMeterQueryInfo *pMeterQueryInfo = pMeterInfo[k].pMeterQInfo;
      restoreIntervalQueryRange(pQuery, pMeterQueryInfo);

      /*
       * Update the query meter column index and the corresponding filter column index
       * the original column index info may be inconsistent with current meter in cache.
       *
       * The stable schema has been changed, but the meter schema, along with the data in cache,
       * will not be updated until data with new schema arrive.
       */
      vnodeUpdateQueryColumnIndex(pQuery, pMeterObj);
      vnodeUpdateFilterColumnIndex(pQuery);

      if (pQuery->nAggTimeInterval == 0) {
        if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
          dTrace("QInfo:%p vid:%d sid:%d id:%s, query completed, no need to scan data in cache. qrange:%lld-%lld, lastKey:%lld",
                 pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
                 pQuery->lastKey);

          continue;
        }

        setExecutionContext(pSupporter, pSupporter->pResult, pMeterInfo[k].meterOrderIdx, pMeterInfo[k].groupIdx);
      } else {
        setIntervalQueryExecutionContext(pSupporter, k, pMeterQueryInfo);
      }

      qTrace("QInfo:%p vid:%d sid:%d id:%s, query in cache, qrange:%lld-%lld, lastKey:%lld", pQInfo, pMeterObj->vnode,
             pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

      /*
       * find the appropriated start position in cache
       * NOTE: (taking ascending order query for example)
       * for the specific query range [pQuery->lastKey, pQuery->ekey], there may be no qualified result in cache.
       * Therefore, we need the first point that is greater(less) than the pQuery->lastKey, so the boundary check
       * should be ignored (the fourth parameter).
       */
      TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, true);
      if (nextKey < 0) {
        qTrace("QInfo:%p vid:%d sid:%d id:%s, no data qualified in cache, cache blocks:%d, lastKey:%lld", pQInfo,
               pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->numOfBlocks, pQuery->lastKey);
        continue;
      }

      // data in this block may be flushed to disk and this block is allocated to other meter
      // todo try with remain cache blocks
      SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
      if (pBlock == NULL) {
        continue;
      }

      if (!doCheckWithPrevQueryRange(pQInfo, nextKey, &pMeterInfo[k])) {
        continue;
      }

      SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
      for (int32_t i = 0; i < pCacheInfo->maxBlocks; ++i) {
        pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);

        // cache block may be flushed to disk, so it is not available, ignore it and try next
        if (pBlock == NULL) {
          pQuery->slot = (pQuery->slot + step + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
          continue;
        }

        TSKEY *primaryKeys = (TSKEY *)pBlock->offset[0];
        // in handling file data block, this query condition is checked during fetching candidate file blocks
        if ((primaryKeys[pQuery->pos] > pSupporter->rawEKey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (primaryKeys[pQuery->pos] < pSupporter->rawEKey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        /* only record the key on last block */
        SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
        SBlockInfo binfo = getBlockBasicInfo(pBlock, BLK_CACHE_BLOCK);

        dTrace("QInfo:%p check data block, brange:%lld-%lld, fileId:%d, slot:%d, pos:%d, bstatus:%d",
               GET_QINFO_ADDR(pQuery), binfo.keyFirst, binfo.keyLast, pQuery->fileId, pQuery->slot, pQuery->pos,
               pRuntimeEnv->blockStatus);

        totalBlocks++;
        queryOnBlock(pSupporter, primaryKeys, pRuntimeEnv->blockStatus, (char *)pBlock, &binfo, &pMeterInfo[k], NULL,
                     searchFn);

        // todo refactor
        if ((pQuery->slot == pQuery->currentSlot && QUERY_IS_ASC_QUERY(pQuery)) ||
            (pQuery->slot == pQuery->firstSlot && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        // try next cache block
        pQuery->slot = (pQuery->slot + step + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          pQuery->pos = 0;
        } else {  // backwards traverse encounter the cache invalid, abort scan cache.
          SCacheBlock *pNextBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
          if (pNextBlock == NULL) {
            break;  // todo fix
          } else {
            pQuery->pos = pNextBlock->numOfPoints - 1;
          }
        }
      }
    }
  }

  int64_t               time = taosGetTimestampUs() - st;
  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;

  pSummary->blocksInCache += totalBlocks;
  pSummary->cacheTimeUs += time;
  pSummary->numOfTables = pSupporter->pSidSet->numOfSids;

  dTrace("QInfo:%p complete check %d cache blocks, elapsed time:%.3fms", pQInfo, totalBlocks, time / 1000.0);

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  return pMeterInfo;
}

static SMeterDataInfo *queryOnMultiDataFiles(SQInfo *pQInfo, SMeterQuerySupportObj *pSupporter,
                                             SMeterDataInfo *pMeterDataInfo) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  SMeterDataBlockInfoEx *pDataBlockInfoEx = NULL;
  int32_t                nAllocBlocksInfoSize = 0;

  SMeterObj *         pTempMeter = getMeterObj(pSupporter->pMeterObj, pSupporter->pMeterSidExtInfo[0]->sid);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pTempMeter->searchAlgorithm];

  int32_t vnodeId = pTempMeter->vnode;
  dTrace("QInfo:%p start to check data blocks in %d files", pQInfo, pRuntimeEnv->numOfFiles);

  int32_t               fid = QUERY_IS_ASC_QUERY(pQuery) ? -1 : INT32_MAX;
  int32_t               step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;

  int64_t totalBlocks = 0;
  int64_t st = taosGetTimestampUs();

  while (1) {
    if (isQueryKilled(pQuery)) {
      break;
    }

    int32_t fileIdx = vnodeGetVnodeHeaderFileIdx(&fid, pRuntimeEnv, pQuery->order.order);
    if (fileIdx < 0) {
      // no valid file, abort current search
      break;
    }

    pRuntimeEnv->startPos.fileId = fid;
    pQuery->fileId = fid;
    pSummary->numOfFiles++;

    SQueryFileInfo *pQueryFileInfo = &pRuntimeEnv->pHeaderFiles[fileIdx];
    char *          pHeaderData = pQueryFileInfo->pHeaderFileData;

    int32_t          numOfQualifiedMeters = 0;
    SMeterDataInfo **pReqMeterDataInfo = vnodeFilterQualifiedMeters(
        pQInfo, vnodeId, pQueryFileInfo, pSupporter->pSidSet, pMeterDataInfo, &numOfQualifiedMeters);
    dTrace("QInfo:%p file:%s, %d meters qualified", pQInfo, pQueryFileInfo->dataFilePath, numOfQualifiedMeters);

    /* none of meters in query set have pHeaderData in this file, try next file
     */
    if (numOfQualifiedMeters == 0) {
      fid += step;
      tfree(pReqMeterDataInfo);
      continue;
    }

    uint32_t numOfBlocks = getDataBlocksForMeters(pSupporter, pQuery, pHeaderData, numOfQualifiedMeters, pQueryFileInfo,
                                                  pReqMeterDataInfo);

    dTrace("QInfo:%p file:%s, %d meters contains %d blocks to be checked", pQInfo, pQueryFileInfo->dataFilePath,
           numOfQualifiedMeters, numOfBlocks);
    if (numOfBlocks == 0) {
      fid += step;
      tfree(pReqMeterDataInfo);
      continue;
    }

    createDataBlocksInfoEx(pReqMeterDataInfo, numOfQualifiedMeters, &pDataBlockInfoEx, numOfBlocks,
                           &nAllocBlocksInfoSize, (int64_t)pQInfo);

    dTrace("QInfo:%p start to load %d blocks and check", pQInfo, numOfBlocks);
    int64_t TRACE_OUTPUT_BLOCK_CNT = 10000;
    int64_t stimeUnit = 0;
    int64_t etimeUnit = 0;

    totalBlocks += numOfBlocks;

    // sequentially scan the pHeaderData file
    int32_t j = QUERY_IS_ASC_QUERY(pQuery) ? 0 : numOfBlocks - 1;

    for (; j < numOfBlocks && j >= 0; j += step) {
      if (isQueryKilled(pQuery)) {
        break;
      }

      /* output elapsed time for log every TRACE_OUTPUT_BLOCK_CNT blocks */
      if (j == 0) {
        stimeUnit = taosGetTimestampMs();
      } else if ((j % TRACE_OUTPUT_BLOCK_CNT) == 0) {
        etimeUnit = taosGetTimestampMs();
        dTrace("QInfo:%p load and check %ld blocks, and continue. elapsed:%ldms", pQInfo, TRACE_OUTPUT_BLOCK_CNT,
               etimeUnit - stimeUnit);
        stimeUnit = taosGetTimestampMs();
      }

      SMeterDataBlockInfoEx *pInfoEx = &pDataBlockInfoEx[j];
      SMeterDataInfo *       pOneMeterDataInfo = pInfoEx->pMeterDataInfo;
      SMeterQueryInfo *      pMeterQueryInfo = pOneMeterDataInfo->pMeterQInfo;
      SMeterObj *            pMeterObj = pOneMeterDataInfo->pMeterObj;

      pQInfo->pObj = pMeterObj;
      pRuntimeEnv->pMeterObj = pMeterObj;

      restoreIntervalQueryRange(pQuery, pMeterQueryInfo);

      if (pQuery->nAggTimeInterval == 0) {  // normal query
        if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
          qTrace("QInfo:%p vid:%d sid:%d id:%s, query completed, no need to scan this data block. qrange:%lld-%lld, "
                 "lastKey:%lld",
                 pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
                 pQuery->lastKey);

          continue;
        }
        setExecutionContext(pSupporter, pSupporter->pResult, pOneMeterDataInfo->meterOrderIdx,
                            pOneMeterDataInfo->groupIdx);
      } else {  // interval query
        setIntervalQueryExecutionContext(pSupporter, pOneMeterDataInfo->meterOrderIdx, pMeterQueryInfo);
      }

      SCompBlock *pBlock = pInfoEx->pBlock.compBlock;
      bool        ondemandLoad = onDemandLoadDatablock(pQuery, pMeterQueryInfo->queryRangeSet);
      int32_t     ret = LoadDatablockOnDemand(pBlock, &pInfoEx->pBlock.fields, &pRuntimeEnv->blockStatus, pRuntimeEnv,
                                          fileIdx, pInfoEx->blockIndex, searchFn, ondemandLoad);
      if (ret != DISK_DATA_LOADED) {
        pSummary->skippedFileBlocks++;
        continue;
      }

      SBlockInfo binfo = getBlockBasicInfo(pBlock, BLK_FILE_BLOCK);

      assert(pQuery->pos >= 0 && pQuery->pos < pBlock->numOfPoints);
      TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

      if (IS_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus) && needPrimaryTimestampCol(pQuery, &binfo)) {
        TSKEY nextKey = primaryKeys[pQuery->pos];
        if (!doCheckWithPrevQueryRange(pQInfo, nextKey, pOneMeterDataInfo)) {
          continue;
        }
      } else {
        // if data block is not loaded, it must be the intermediate blocks
        assert((pBlock->keyFirst >= pQuery->lastKey && pBlock->keyLast <= pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
               (pBlock->keyFirst >= pQuery->ekey && pBlock->keyLast <= pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));
      }

      queryOnBlock(pSupporter, primaryKeys, pRuntimeEnv->blockStatus, (char *)pRuntimeEnv->colDataBuffer, &binfo,
                   pOneMeterDataInfo, pInfoEx->pBlock.fields, searchFn);
    }

    tfree(pReqMeterDataInfo);

    // try next file
    fid += step;
  }

  int64_t time = taosGetTimestampUs() - st;
  dTrace("QInfo:%p complete check %d files, %d blocks, elapsed time:%.3fms", pQInfo, pRuntimeEnv->numOfFiles,
         totalBlocks, time / 1000.0);

  pSummary->fileTimeUs += time;
  pSummary->readDiskBlocks += totalBlocks;
  pSummary->numOfTables = pSupporter->pSidSet->numOfSids;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  freeMeterBlockInfoEx(pDataBlockInfoEx, nAllocBlocksInfoSize);
  return pMeterDataInfo;
}

static bool multimeterMultioutputHelper(SQInfo *pQInfo, bool *dataInDisk, bool *dataInCache, int32_t index,
                                        int32_t start) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *           pQuery = &pQInfo->query;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SMeterObj *pMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[index]->sid);
  if (pMeterObj == NULL) {
    dError("QInfo:%p do not find required meter id: %d, all meterObjs id is:", pQInfo, pMeterSidExtInfo[index]->sid);
    return false;
  }

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pMeterSidExtInfo[index]);

  dTrace("QInfo:%p query on (%d): vid:%d sid:%d meterId:%s, qrange:%lld-%lld", pQInfo, index - start, pMeterObj->vnode,
         pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey);

  pQInfo->pObj = pMeterObj;
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->pMeterObj = pMeterObj;

  vnodeCheckIfDataExists(pRuntimeEnv, pMeterObj, dataInDisk, dataInCache);

  if (pQuery->lastKey > pMeterObj->lastKey && QUERY_IS_ASC_QUERY(pQuery)) {
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, qrange:%lld-%lld, nores, %p", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery);
    return false;
  }

  return true;
}

static int64_t doCheckMetersInGroup(SQInfo *pQInfo, int32_t index, int32_t start) {
  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  bool dataInDisk = true;
  bool dataInCache = true;
  if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, index, start)) {
    return 0;
  }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
  for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
    resetMMapWindow(&pRuntimeEnv->pHeaderFiles[i]);
  }
#endif
  SPointInterpoSupporter pointInterpSupporter = {0};
  pointInterpSupporterInit(pQuery, &pointInterpSupporter);

  if (!normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter)) {
    pointInterpSupporterDestroy(&pointInterpSupporter);
    return 0;
  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for
   * interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &pointInterpSupporter);
  pointInterpSupporterDestroy(&pointInterpSupporter);

  vnodeScanAllData(pRuntimeEnv);
  doFinalizeResult(pRuntimeEnv);

  int64_t numOfRes = getNumOfResult(pRuntimeEnv);
  assert(numOfRes == 1 || numOfRes == 0);

  // accumulate the point interpolation result
  if (numOfRes > 0) {
    pQuery->pointsRead += numOfRes;
    forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
  }

  return numOfRes;
}

static void vnodeMultiMeterMultiOutputProcessor(SQInfo *pQInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery * pQuery = &pQInfo->query;
  tSidSet *pSids = pSupporter->pSidSet;

  SMeterObj *pOneMeter = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[0]->sid);

  resetCtxOutputBuf(pRuntimeEnv);
  cleanCtxOutputBuf(pRuntimeEnv);
  initCtxOutputBuf(pRuntimeEnv);

  if (isPointInterpoQuery(pQuery)) {
    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pSupporter->subgroupIdx < pSids->numOfSubSet) {
      int32_t start = pSids->starterPos[pSupporter->subgroupIdx];
      int32_t end = pSids->starterPos[pSupporter->subgroupIdx + 1] - 1;

      if (isFirstLastRowQuery(pQuery)) {
        dTrace("QInfo:%p last_row query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, pOneMeter->vnode,
               pSids->numOfSubSet, pSupporter->subgroupIdx);

        TSKEY   key = -1;
        int32_t index = -1;

        // choose the last key for one group
        pSupporter->meterIdx = start;

        for (int32_t k = start; k <= end; ++k, pSupporter->meterIdx++) {
          if (isQueryKilled(pQuery)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }

          SMeterObj *pMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[k]->sid);
          if (pMeterObj != NULL) {
            if (key < pMeterObj->lastKey) {
              key = pMeterObj->lastKey;
              index = k;
            }
          }
        }

        pQuery->skey = key;
        pQuery->ekey = key;
        pSupporter->rawSKey = key;
        pSupporter->rawEKey = key;

        int64_t num = doCheckMetersInGroup(pQInfo, index, start);
        assert(num >= 0);
      } else {
        dTrace("QInfo:%p interp query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, pOneMeter->vnode,
               pSids->numOfSubSet, pSupporter->subgroupIdx);

        for (int32_t k = start; k <= end; ++k) {
          if (isQueryKilled(pQuery)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }

          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;

          int64_t res = doCheckMetersInGroup(pQInfo, k, start);
          if (res == 1) {
            break;
          }
        }
      }

      pSupporter->subgroupIdx++;

      // output buffer is full, return to client
      if (pQuery->pointsRead >= pQuery->pointsToRead) {
        break;
      }
    }
  } else {
    int32_t start = pSids->starterPos[0];
    int32_t end = pSids->starterPos[1] - 1;

    // NOTE: for group by interpolation query, the number of subset may be greater than 1
    assert(pSids->numOfSubSet == 1 && start == 0 && end == pSids->numOfSids - 1 && pSupporter->meterIdx >= start &&
           pSupporter->meterIdx <= end);

    for (int32_t k = pSupporter->meterIdx; k <= end; ++k, ++pSupporter->meterIdx) {
      if (isQueryKilled(pQuery)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        return;
      }

      bool dataInDisk = true;
      bool dataInCache = true;
      if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, k, start)) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;
        continue;
      }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
      for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
        resetMMapWindow(&pRuntimeEnv->pHeaderFiles[i]);
      }
#endif
      SPointInterpoSupporter pointInterpSupporter = {0};
      if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter) == false) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;
        continue;
      }

      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        forwardQueryStartPosition(pRuntimeEnv);

        if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;
          continue;
        }
      }

      vnodeUpdateQueryColumnIndex(pQuery, pRuntimeEnv->pMeterObj);
      vnodeUpdateFilterColumnIndex(pQuery);

      vnodeScanAllData(pRuntimeEnv);
      doFinalizeResult(pRuntimeEnv);

      pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
      doSkipResults(pRuntimeEnv);

      // set query completed
      if (doRevisedResultsByLimit(pQInfo)) {
        pSupporter->meterIdx = pSupporter->pSidSet->numOfSids;
        break;
      }

      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        /*
         * query range is identical in terms of all meters involved in query,
         * so we need to restore them at the *beginning* of query on each meter,
         * not the consecutive query on meter on which is aborted due to buffer limitation
         * to ensure that, we can reset the query range once query on a meter is completed.
         */
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;

        if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
          pSupporter->meterIdx++;
          break;
        }
      } else {
        assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));

        // forward query range
        pQuery->skey = pQuery->lastKey;
        break;
      }
    }
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQuery->pointsOffset = pQuery->pointsToRead;

  moveDescOrderResultsToFront(pRuntimeEnv);

  dTrace("QInfo %p vid:%d, numOfMeters:%d, index:%d, numOfGroups:%d, %d points returned, totalRead:%d totalReturn:%d,"
         "next skey:%lld, offset:%ld",
         pQInfo, pOneMeter->vnode, pSupporter->pSidSet->numOfSids, pSupporter->meterIdx,
         pSupporter->pSidSet->numOfSubSet,
         pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned, pQuery->skey, pQuery->limit.offset);
}

static void doMultiMeterSupplementaryScan(SQInfo *pQInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = &pQInfo->query;

  if (!needSupplementaryScan(pQuery)) {
    dTrace("QInfo:%p no need to do supplementary scan, query completed", pQInfo);
    return;
  }

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);
  disableFunctForSuppleScanAndSetSortOrder(pRuntimeEnv, pQuery->order.order);

  SWAP(pSupporter->rawSKey, pSupporter->rawEKey);

  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    SMeterQueryInfo *pMeterQInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
    if (pMeterQInfo != NULL) {
      pMeterQInfo->skey = pSupporter->rawSKey;
      pMeterQInfo->ekey = pSupporter->rawEKey;
      pMeterQInfo->lastKey = pMeterQInfo->skey;
      pMeterQInfo->queryRangeSet = 0;

      /* previous does not generate any results*/
      if (pMeterQInfo->numOfPages == 0) {
        pMeterQInfo->reverseFillRes = 0;
      } else {
        pMeterQInfo->reverseIndex = pMeterQInfo->numOfRes;
        pMeterQInfo->reverseFillRes = 1;
      }
    }
  }

  int64_t st = taosGetTimestampMs();
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    pSupporter->pMeterDataInfo = queryOnMultiDataFiles(pQInfo, pSupporter, pSupporter->pMeterDataInfo);
    pSupporter->pMeterDataInfo = queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
  } else {
    pSupporter->pMeterDataInfo = queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
    pSupporter->pMeterDataInfo = queryOnMultiDataFiles(pQInfo, pSupporter, pSupporter->pMeterDataInfo);
  }

  SWAP(pSupporter->rawSKey, pSupporter->rawEKey);
  enableFunctForMasterScan(pRuntimeEnv, pQuery->order.order);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);

  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p supplementary scan completed, elapsed time: %lldms", pQInfo, et - st);
}

static void vnodeMultiMeterQueryProcessor(SQInfo *pQInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQuery *               pQuery = &pQInfo->query;

  if (pSupporter->subgroupIdx > 0) {
    /*
     * if the subgroupIdx > 0, the query process must be completed yet, we only need to
     * copy the data into output buffer
     */
    if (pQuery->nAggTimeInterval > 0) {
      copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    } else {
      copyFromGroupBuf(pQInfo, pSupporter->pResult);
    }

    pQInfo->pointsRead += pQuery->pointsRead;

    if (pQuery->pointsRead == 0) {
      vnodePrintQueryStatistics(pSupporter);
    }
    dTrace("QInfo:%p points returned:%d, totalRead:%d totalReturn:%d", pQInfo, pQuery->pointsRead, pQInfo->pointsRead,
           pQInfo->pointsReturned);
    return;
  }

  pSupporter->pMeterDataInfo = (SMeterDataInfo *)calloc(1, sizeof(SMeterDataInfo) * pSupporter->numOfMeters);
  if (pSupporter->pMeterDataInfo == NULL) {
    dError("QInfo:%p failed to allocate memory, %s", pQInfo, strerror(errno));
    return;
  }

  dTrace("QInfo:%p query start, qrange:%lld-%lld, order:%d, group:%d", pQInfo, pSupporter->rawSKey, pSupporter->rawEKey,
         pQuery->order.order, pSupporter->pSidSet->numOfSubSet);

  dTrace("QInfo:%p main query scan start", pQInfo);
  int64_t st = taosGetTimestampMs();

  if (QUERY_IS_ASC_QUERY(pQuery)) {  // order: asc
    pSupporter->pMeterDataInfo = queryOnMultiDataFiles(pQInfo, pSupporter, pSupporter->pMeterDataInfo);
    pSupporter->pMeterDataInfo = queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
  } else {  // order: desc
    pSupporter->pMeterDataInfo = queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
    pSupporter->pMeterDataInfo = queryOnMultiDataFiles(pQInfo, pSupporter, pSupporter->pMeterDataInfo);
  }

  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p main scan completed, elapsed time: %lldms, supplementary scan start, order:%d", pQInfo, et - st,
         pQuery->order.order ^ 1);

  doCloseAllOpenedResults(pSupporter);
  doMultiMeterSupplementaryScan(pQInfo);

  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query killed, abort", pQInfo);
    return;
  }

  if (pQuery->nAggTimeInterval > 0) {
    assert(pSupporter->subgroupIdx == 0 && pSupporter->numOfGroupResultPages == 0);

    mergeMetersResultToOneGroups(pSupporter);
    copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
    displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
  } else {  // not a interval query
    copyFromGroupBuf(pQInfo, pSupporter->pResult);
  }

  /* handle the limitation of output buffer */
  // displayInterResult(pQuery->sdata, pQuery, pQuery->pointsRead);
  pQInfo->pointsRead += pQuery->pointsRead;
  dTrace("QInfo:%p points returned:%d, totalRead:%d totalReturn:%d", pQInfo, pQuery->pointsRead, pQInfo->pointsRead,
         pQInfo->pointsReturned);
}

/*
 * in each query, this function will be called only once, no retry for further result is not needed.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a]
 */
static void vnodeSingleMeterFixedOutputProcessor(SQInfo *pQInfo) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;

  assert(pQuery->slot >= 0 && pQuery->pos >= 0);

  vnodeScanAllData(pRuntimeEnv);
  doFinalizeResult(pRuntimeEnv);

  if (isQueryKilled(pQuery)) {
    return;
  }

  // since the numOfOutputElems must be identical for all sql functions that are allowed to be executed simutanelously.
  pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
  assert(pQuery->pointsRead <= pQuery->pointsToRead &&
         Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK));

  // must be top/bottom query if offset > 0
  if (pQuery->limit.offset > 0) {
    assert(isTopBottomQuery(pQuery));
  }

  doSkipResults(pRuntimeEnv);
  doRevisedResultsByLimit(pQInfo);
  moveDescOrderResultsToFront(pRuntimeEnv);

  pQInfo->pointsRead = pQuery->pointsRead;
}

static void vnodeSingleMeterMultiOutputProcessor(SQInfo *pQInfo) {
  SQuery *   pQuery = &pQInfo->query;
  SMeterObj *pMeterObj = pQInfo->pObj;

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;

  while (1) {
    vnodeScanAllData(pRuntimeEnv);
    doFinalizeResult(pRuntimeEnv);

    if (isQueryKilled(pQuery)) {
      return;
    }

    pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols > 0 && pQuery->pointsRead > 0) {
      doSkipResults(pRuntimeEnv);
    }

    /*
     * 1. if pQuery->pointsRead == 0, pQuery->limit.offset >= 0, still need to check data
     * 2. if pQuery->pointsRead > 0, pQuery->limit.offset must be 0
     */
    if (pQuery->pointsRead > 0 || Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
      break;
    }

    TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    assert(nextTimestamp > 0 || ((nextTimestamp < 0) && Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)));

    dTrace("QInfo:%p vid:%d sid:%d id:%s, skip current result, offset:%lld, next qrange:%lld-%lld", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->limit.offset, pQuery->lastKey, pQuery->ekey);

    resetCtxOutputBuf(pRuntimeEnv);
    cleanCtxOutputBuf(pRuntimeEnv);
  }

  doRevisedResultsByLimit(pQInfo);
  moveDescOrderResultsToFront(pRuntimeEnv);

  pQInfo->pointsRead += pQuery->pointsRead;

  if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    assert(nextTimestamp > 0 || ((nextTimestamp < 0) && Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)));

    dTrace("QInfo:%p vid:%d sid:%d id:%s, query abort due to buffer limitation, next qrange:%lld-%lld", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->lastKey, pQuery->ekey);
  }

  dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d", pQInfo, pMeterObj->vnode,
         pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned);

  resetCtxOutputBuf(pRuntimeEnv);

  pQuery->pointsOffset = pQuery->pointsToRead;  // restore the available buffer
  assert(pQuery->pointsRead <= pQuery->pointsToRead);
}

static void vnodeSingleMeterIntervalMainLooper(SMeterQuerySupportObj *pSupporter, SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    assert((pQuery->skey <= pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
           (pQuery->skey >= pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery)));

    initCtxOutputBuf(pRuntimeEnv);
    vnodeScanAllData(pRuntimeEnv);
    if (isQueryKilled(pQuery)) {
      return;
    }

    assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_NOT_COMPLETED));

    // clear tag, used to decide if the whole interval query is completed or not
    pQuery->over &= (~QUERY_COMPLETED);
    doFinalizeResult(pRuntimeEnv);

    int64_t maxOutput = getNumOfResult(pRuntimeEnv);

    /* here we can ignore the records in case of no interpolation */
    if (pQuery->numOfFilterCols > 0 && pQuery->limit.offset > 0 && pQuery->interpoType == TSDB_INTERPO_NONE) {
      /* maxOutput <= 0, means current query does not generate any results */
      // todo handle offset, in case of top/bottom interval query
      if (maxOutput > 0) {
        pQuery->limit.offset--;
      }
    } else {
      pQuery->pointsRead += maxOutput;
      forwardCtxOutputBuf(pRuntimeEnv, maxOutput);
    }

    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      break;
    }

    forwardIntervalQueryRange(pSupporter, pRuntimeEnv);
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED)) {
      break;
    }

    /*
     * the scan limitation mechanism is upon here,
     * 1. since there is only one(k) record is generated in one scan operation
     * 2. remain space is not sufficient for next query output, abort
     */
    if ((pQuery->pointsRead % pQuery->pointsToRead == 0 && pQuery->pointsRead != 0) ||
        ((pQuery->pointsRead + maxOutput) > pQuery->pointsToRead)) {
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
      break;
    }
  }
}

/* handle time interval query on single table */
static void vnodeSingleMeterIntervalProcessor(SQInfo *pQInfo) {
  SQuery *   pQuery = &(pQInfo->query);
  SMeterObj *pMeterObj = pQInfo->pObj;

  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  int32_t numOfInterpo = 0;

  while (1) {
    resetCtxOutputBuf(pRuntimeEnv);
    vnodeSingleMeterIntervalMainLooper(pSupporter, pRuntimeEnv);

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->interpoType == TSDB_INTERPO_NONE) {
      doRevisedResultsByLimit(pQInfo);
      break;
    } else {
      taosInterpoSetStartInfo(&pRuntimeEnv->interpoInfo, pQuery->pointsRead, pQuery->interpoType);
      SData **pInterpoBuf = pRuntimeEnv->pInterpoBuf;

      if (QUERY_IS_ASC_QUERY(pQuery)) {
        for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
          memcpy(pInterpoBuf[i]->data, pQuery->sdata[i]->data, pQuery->pointsRead * pQuery->pSelectExpr[i].resBytes);
        }
      } else {
        int32_t size = pMeterObj->pointsPerFileBlock;
        for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
          memcpy(pInterpoBuf[i]->data,
                 pQuery->sdata[i]->data + (size - pQuery->pointsRead) * pQuery->pSelectExpr[i].resBytes,
                 pQuery->pointsRead * pQuery->pSelectExpr[i].resBytes);
        }
      }

      numOfInterpo = 0;
      pQuery->pointsRead = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata, (tFilePage **)pInterpoBuf,
                                                       pQuery->pointsRead, &numOfInterpo);

      dTrace("QInfo: %p interpo completed, final:%d", pQInfo, pQuery->pointsRead);
      if (pQuery->pointsRead > 0 || Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
        doRevisedResultsByLimit(pQInfo);
        break;
      }

      // no result generated yet, continue retrieve data
      pQuery->pointsRead = 0;
    }
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQInfo->pointsInterpo += numOfInterpo;

  moveDescOrderResultsToFront(pRuntimeEnv);

  dTrace("%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d totalReturn:%d",
         pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
         pQInfo->pointsRead - pQInfo->pointsInterpo, pQInfo->pointsInterpo, pQInfo->pointsReturned);
}

void vnodeSingleMeterQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pMeterQuerySupporter == NULL) {
    dTrace("%p freed abort query", pQInfo);
    return;
  }

  if (pQInfo->killed) {
    TSDB_QINFO_RESET_SIG(pQInfo);
    dTrace("QInfo:%p it is already killed, reset signature and abort", pQInfo);
    return;
  }

  assert(pQInfo->signature == TSDB_QINFO_QUERY_FLAG);

  SQuery *   pQuery = &pQInfo->query;
  SMeterObj *pMeterObj = pQInfo->pObj;

  dTrace("vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d, QInfo:%p", pMeterObj->vnode, pMeterObj->sid,
         pMeterObj->meterId, pMeterObj->numOfQueries, pQInfo);

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;
  assert(pQuery->pGroupbyExpr == NULL && pRuntimeEnv->pMeterObj == pMeterObj);

  if (vnodeHasRemainResults(pQInfo)) {
    /*
     * there are remain results that are not returned due to result
     * interpolation
     * So, we do keep in this procedure instead of launching retrieve procedure
     */
    int32_t numOfInterpo = 0;

    int32_t remain = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    pQuery->pointsRead = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata,
                                                     (tFilePage **)pRuntimeEnv->pInterpoBuf, remain, &numOfInterpo);

    doRevisedResultsByLimit(pQInfo);
    moveDescOrderResultsToFront(pRuntimeEnv);

    pQInfo->pointsInterpo += numOfInterpo;
    pQInfo->pointsRead += pQuery->pointsRead;

    dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d totalReturn:%d",
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
           pQInfo->pointsRead, pQInfo->pointsInterpo, pQInfo->pointsReturned);

    dTrace("QInfo:%p reset signature", pQInfo);

    TSDB_QINFO_RESET_SIG(pQInfo);
    sem_post(&pQInfo->dataReady);

    return;
  }

  /* here we have scan all qualified data in both data file and cache. */
  if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
    pQInfo->over = 1;
    dTrace("QInfo:%p vid:%d sid:%d id:%s, query over, %d points are returned, reset signature", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQInfo->pointsRead);

    vnodePrintQueryStatistics(pQInfo->pMeterQuerySupporter);
    TSDB_QINFO_RESET_SIG(pQInfo);
    sem_post(&pQInfo->dataReady);

    return;
  }

  /* number of points returned during this query  */
  pQuery->pointsRead = 0;
  assert(pQuery->pos >= 0 && pQuery->slot >= 0);

  int64_t st = taosGetTimestampUs();

  if (pQuery->nAggTimeInterval != 0) {  // interval (downsampling operation)
    assert(pQuery->nAggTimeInterval != 0 && pQuery->checkBufferInLoop == 0 &&
           pQuery->pointsOffset == pQuery->pointsToRead);
    vnodeSingleMeterIntervalProcessor(pQInfo);
  } else {
    if (isFixedOutputQuery(pQuery)) {
      assert(pQuery->checkBufferInLoop == 0 && pQuery->nAggTimeInterval == 0);
      vnodeSingleMeterFixedOutputProcessor(pQInfo);
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkBufferInLoop == 1 && pQuery->nAggTimeInterval == 0);
      vnodeSingleMeterMultiOutputProcessor(pQInfo);
    }
  }

  /* record the total elapsed time */
  pQInfo->useconds += (taosGetTimestampUs() - st);

  /* check if query is killed or not */
  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query is killed, reset signature", pQInfo);
    pQInfo->over = 1;
  } else {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, meter query thread completed, %d points are returned, reset signature",
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead);
  }

  TSDB_QINFO_RESET_SIG(pQInfo);
  sem_post(&pQInfo->dataReady);
}

void vnodeMultiMeterQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pMeterQuerySupporter == NULL) {
    return;
  }

  if (pQInfo->killed) {
    TSDB_QINFO_RESET_SIG(pQInfo);
    dTrace("QInfo:%p it is already killed, reset signature and abort", pQInfo);
    return;
  }

  assert(pQInfo->signature == TSDB_QINFO_QUERY_FLAG);

  SQuery *pQuery = &pQInfo->query;
  pQuery->pointsRead = 0;

  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  if (pSupporter->meterIdx >= pSupporter->pSidSet->numOfSids) {
    pQInfo->over = 1;
    dTrace("QInfo:%p over, %d meters queried, %d points are returned, reset signature", pQInfo, pSupporter->numOfMeters,
           pQInfo->pointsRead);

    // reset status
    TSDB_QINFO_RESET_SIG(pQInfo);

    vnodePrintQueryStatistics(pSupporter);
    sem_post(&pQInfo->dataReady);
    return;
  }

  int64_t st = taosGetTimestampUs();
  if (pQuery->nAggTimeInterval > 0 || (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)))) {
    assert(pQuery->checkBufferInLoop == 0);
    vnodeMultiMeterQueryProcessor(pQInfo);
  } else {
    assert((pQuery->checkBufferInLoop == 1 && pQuery->nAggTimeInterval == 0 && pQuery->pGroupbyExpr == NULL) ||
           isPointInterpoQuery(pQuery));
    vnodeMultiMeterMultiOutputProcessor(pQInfo);
  }

  /* record the total elapsed time */
  pQInfo->useconds += (taosGetTimestampUs() - st);
  pQInfo->over = isQueryKilled(pQuery) ? 1 : 0;

  dTrace("QInfo:%p reset signature", pQInfo);
  taosInterpoSetStartInfo(&pQInfo->pMeterQuerySupporter->runtimeEnv.interpoInfo, pQuery->pointsRead,
                          pQInfo->query.interpoType);

  TSDB_QINFO_RESET_SIG(pQInfo);
  sem_post(&pQInfo->dataReady);
}
