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

#define _DEFAULT_SOURCE
#include "os.h"

#include "taosmsg.h"
#include "textbuffer.h"
#include "tscJoinProcess.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"

#include "vnodeQueryImpl.h"

#define ALL_CACHE_BLOCKS_CHECKED(q) \
  (((q)->slot == (q)->currentSlot && QUERY_IS_ASC_QUERY(q)) || ((q)->slot == (q)->firstSlot && (!QUERY_IS_ASC_QUERY(q))))

#define FORWARD_CACHE_BLOCK_CHECK_SLOT(slot, step, maxblocks) (slot) = ((slot) + (step) + (maxblocks)) % (maxblocks);

static bool isGroupbyEachTable(SSqlGroupbyExpr *pGroupbyExpr, tSidSet *pSidset) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndexEx *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_TAG) {
      assert(pSidset->numOfSids == pSidset->numOfSubSet);
      return true;
    }
  }

  return false;
}

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
      setIntervalQueryRange(pMeterInfo->pMeterQInfo, pSupporter, nextKey);
    }
  }

  return true;
}

/**
 * The start position of the first check cache block is located before starting the loop.
 * And the start position for next cache blocks needs to be decided before checking each cache block.
 */
static void setStartPositionForCacheBlock(SQuery *pQuery, SCacheBlock *pBlock, bool *firstCheckSlot) {
  if (!(*firstCheckSlot)) {
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pQuery->pos = 0;
    } else {
      pQuery->pos = pBlock->numOfPoints - 1;
    }
  } else {
    (*firstCheckSlot) = false;
  }
}

static void queryOnMultiDataCache(SQInfo *pQInfo, SMeterDataInfo *pMeterInfo) {
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
      return;
    }

    for (int32_t k = start; k <= end; ++k) {
      SMeterObj *pMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[k]->sid);
      if (pMeterObj == NULL) {
        dError("QInfo:%p failed to find meterId:%d, continue", pQInfo, pMeterSidExtInfo[k]->sid);
        continue;
      }

      pQInfo->pObj = pMeterObj;
      pRuntimeEnv->pMeterObj = pMeterObj;

      if (pMeterInfo[k].pMeterQInfo == NULL) {
        pMeterInfo[k].pMeterQInfo = createMeterQueryInfo(pQuery, pSupporter->rawSKey, pSupporter->rawEKey);
      }

      if (pMeterInfo[k].pMeterObj == NULL) {  // no data in disk for this meter, set its pointer
        setMeterDataInfo(&pMeterInfo[k], pMeterObj, k, groupIdx);
      }

      assert(pMeterInfo[k].meterOrderIdx == k && pMeterObj == pMeterInfo[k].pMeterObj);

      SMeterQueryInfo *pMeterQueryInfo = pMeterInfo[k].pMeterQInfo;
      restoreIntervalQueryRange(pRuntimeEnv, pMeterQueryInfo);

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
          dTrace(
              "QInfo:%p vid:%d sid:%d id:%s, query completed, no need to scan data in cache. qrange:%lld-%lld, "
              "lastKey:%lld",
              pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
              pQuery->lastKey);

          continue;
        }

        setExecutionContext(pSupporter, pSupporter->pResult, k, pMeterInfo[k].groupIdx, pMeterQueryInfo);
      } else {
        int32_t ret = setIntervalQueryExecutionContext(pSupporter, k, pMeterQueryInfo);
        if (ret != TSDB_CODE_SUCCESS) {
          pQInfo->killed = 1;
          return;
        }
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

      bool        firstCheckSlot = true;
      SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

      for (int32_t i = 0; i < pCacheInfo->maxBlocks; ++i) {
        pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);

        /*
         * 1. pBlock == NULL. The cache block may be flushed to disk, so it is not available, skip and try next
         *
         * 2. pBlock->numOfPoints == 0. There is a empty block, which is caused by allocate-and-write data into cache
         *    procedure. The block has been allocated but data has not been put into yet. If the block is the last
         *    block(newly allocated block), abort query. Otherwise, skip it and go on.
         */
        if ((pBlock == NULL) || (pBlock->numOfPoints == 0)) {
          if (ALL_CACHE_BLOCKS_CHECKED(pQuery)) {
            break;
          }

          FORWARD_CACHE_BLOCK_CHECK_SLOT(pQuery->slot, step, pCacheInfo->maxBlocks);
          continue;
        }

        setStartPositionForCacheBlock(pQuery, pBlock, &firstCheckSlot);

        TSKEY *primaryKeys = (TSKEY *)pBlock->offset[0];

        // in handling file data block, the timestamp range validation is done during fetching candidate file blocks
        if ((primaryKeys[pQuery->pos] > pSupporter->rawEKey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (primaryKeys[pQuery->pos] < pSupporter->rawEKey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        // only record the key on last block
        SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
        SBlockInfo binfo = getBlockBasicInfo(pBlock, BLK_CACHE_BLOCK);

        dTrace("QInfo:%p check data block, brange:%lld-%lld, fileId:%d, slot:%d, pos:%d, bstatus:%d",
               GET_QINFO_ADDR(pQuery), binfo.keyFirst, binfo.keyLast, pQuery->fileId, pQuery->slot, pQuery->pos,
               pRuntimeEnv->blockStatus);

        totalBlocks++;
        queryOnBlock(pSupporter, primaryKeys, pRuntimeEnv->blockStatus, (char *)pBlock, &binfo, &pMeterInfo[k], NULL,
                     searchFn);

        if (ALL_CACHE_BLOCKS_CHECKED(pQuery)) {
          break;
        }

        FORWARD_CACHE_BLOCK_CHECK_SLOT(pQuery->slot, step, pCacheInfo->maxBlocks);
      }
    }
  }

  int64_t            time = taosGetTimestampUs() - st;
  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;

  pSummary->blocksInCache += totalBlocks;
  pSummary->cacheTimeUs += time;
  pSummary->numOfTables = pSupporter->pSidSet->numOfSids;

  dTrace("QInfo:%p complete check %d cache blocks, elapsed time:%.3fms", pQInfo, totalBlocks, time / 1000.0);

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
}

static void queryOnMultiDataFiles(SQInfo *pQInfo, SMeterDataInfo *pMeterDataInfo) {
  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SMeterDataBlockInfoEx *pDataBlockInfoEx = NULL;
  int32_t                nAllocBlocksInfoSize = 0;

  SMeterObj *         pTempMeter = getMeterObj(pSupporter->pMeterObj, pSupporter->pMeterSidExtInfo[0]->sid);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pTempMeter->searchAlgorithm];

  int32_t vnodeId = pTempMeter->vnode;
  SQueryFilesInfo* pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;
  
  dTrace("QInfo:%p start to check data blocks in %d files", pQInfo, pVnodeFileInfo->numOfFiles);

  int32_t            fid = QUERY_IS_ASC_QUERY(pQuery) ? -1 : INT32_MAX;
  int32_t            step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;

  int64_t totalBlocks = 0;
  int64_t st = taosGetTimestampUs();

  while (1) {
    if (isQueryKilled(pQuery)) {
      break;
    }

    int32_t fileIdx = vnodeGetVnodeHeaderFileIdx(&fid, pRuntimeEnv, pQuery->order.order);
    if (fileIdx < 0) {  // no valid file, abort current search
      break;
    }

    pRuntimeEnv->startPos.fileId = fid;
    pQuery->fileId = fid;
    pSummary->numOfFiles++;

    if (vnodeGetHeaderFile(pRuntimeEnv, fileIdx) != TSDB_CODE_SUCCESS) {
      fid += step;
      continue;
    }
    
    int32_t numOfQualifiedMeters = 0;
    assert(fileIdx == pRuntimeEnv->vnodeFileInfo.current);
    
    SMeterDataInfo **pReqMeterDataInfo = NULL;
    int32_t ret = vnodeFilterQualifiedMeters(pQInfo, vnodeId, pSupporter->pSidSet, pMeterDataInfo,
                                             &numOfQualifiedMeters, &pReqMeterDataInfo);
    if (ret != TSDB_CODE_SUCCESS) {
      dError("QInfo:%p failed to create meterdata struct to perform query processing, abort", pQInfo);
  
      tfree(pReqMeterDataInfo);
      pQInfo->code = -ret;
      pQInfo->killed = 1;
      
      return;
    }

    dTrace("QInfo:%p file:%s, %d meters qualified", pQInfo, pVnodeFileInfo->dataFilePath, numOfQualifiedMeters);
    
    // none of meters in query set have pHeaderFileData in this file, try next file
    if (numOfQualifiedMeters == 0) {
      fid += step;
      tfree(pReqMeterDataInfo);
      continue;
    }

    uint32_t numOfBlocks = 0;
    ret = getDataBlocksForMeters(pSupporter, pQuery, numOfQualifiedMeters, pVnodeFileInfo->headerFilePath,
                                 pReqMeterDataInfo, &numOfBlocks);
    if (ret != TSDB_CODE_SUCCESS) {
      dError("QInfo:%p failed to get data block before scan data blocks, abort", pQInfo);
  
      tfree(pReqMeterDataInfo);
      pQInfo->code = -ret;
      pQInfo->killed = 1;
  
      return;
    }

    dTrace("QInfo:%p file:%s, %d meters contains %d blocks to be checked", pQInfo, pVnodeFileInfo->dataFilePath,
           numOfQualifiedMeters, numOfBlocks);
    
    if (numOfBlocks == 0) {
      fid += step;
      tfree(pReqMeterDataInfo);
      continue;
    }

    ret = createDataBlocksInfoEx(pReqMeterDataInfo, numOfQualifiedMeters, &pDataBlockInfoEx, numOfBlocks,
                                       &nAllocBlocksInfoSize, (int64_t)pQInfo);
    if (ret != TSDB_CODE_SUCCESS) {  // failed to create data blocks
      dError("QInfo:%p build blockInfoEx failed, abort", pQInfo);
      tfree(pReqMeterDataInfo);

      pQInfo->code = -ret;
      pQInfo->killed = 1;
      return;
    }

    dTrace("QInfo:%p start to load %d blocks and check", pQInfo, numOfBlocks);
    int64_t TRACE_OUTPUT_BLOCK_CNT = 10000;
    int64_t stimeUnit = 0;
    int64_t etimeUnit = 0;

    totalBlocks += numOfBlocks;

    // sequentially scan the pHeaderFileData file
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

      restoreIntervalQueryRange(pRuntimeEnv, pMeterQueryInfo);

      if (pQuery->nAggTimeInterval == 0) {  // normal query
        if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
          qTrace(
              "QInfo:%p vid:%d sid:%d id:%s, query completed, no need to scan this data block. qrange:%lld-%lld, "
              "lastKey:%lld",
              pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
              pQuery->lastKey);

          continue;
        }

        setExecutionContext(pSupporter, pSupporter->pResult, pOneMeterDataInfo->meterOrderIdx,
                            pOneMeterDataInfo->groupIdx, pMeterQueryInfo);
      } else {  // interval query
        ret = setIntervalQueryExecutionContext(pSupporter, pOneMeterDataInfo->meterOrderIdx, pMeterQueryInfo);
        if (ret != TSDB_CODE_SUCCESS) {
          tfree(pReqMeterDataInfo);  // error code has been set
          pQInfo->killed = 1;
          return;
        }
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
  dTrace("QInfo:%p complete check %d files, %d blocks, elapsed time:%.3fms", pQInfo, pVnodeFileInfo->numOfFiles,
         totalBlocks, time / 1000.0);

  pSummary->fileTimeUs += time;
  pSummary->readDiskBlocks += totalBlocks;
  pSummary->numOfTables = pSupporter->pSidSet->numOfSids;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
  freeMeterBlockInfoEx(pDataBlockInfoEx, nAllocBlocksInfoSize);
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

  // data in file or cache is not qualified for the query. abort
  if (!(dataInCache || dataInDisk)) {
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, qrange:%lld-%lld, nores, %p", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery);
    return false;
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pRuntimeEnv->cur.vnodeIndex == -1) {
      int64_t tag = pRuntimeEnv->pCtx[0].tag.i64Key;
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, tag);

      // failed to find data with the specified tag value
      if (elem.vnode < 0) {
        return false;
      }
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pRuntimeEnv->cur);
    }
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
    resetMMapWindow(&pRuntimeEnv->pVnodeFiles[i]);
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
   * parameter for interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &pointInterpSupporter);
  pointInterpSupporterDestroy(&pointInterpSupporter);

  vnodeScanAllData(pRuntimeEnv);

  // first/last_row query, do not invoke the finalize for super table query
  if (!isFirstLastRowQuery(pQuery)) {
    doFinalizeResult(pRuntimeEnv);
  }

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
  
  if (isPointInterpoQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
  
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

          // get the last key of meters that belongs to this group
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

          int64_t num = doCheckMetersInGroup(pQInfo, k, start);
          if (num == 1) {
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
    // this procedure treats all tables as single group
    assert(pSupporter->meterIdx >= 0);

    /*
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pSupporter->subgroupIdx > 0) {
      copyFromGroupBuf(pQInfo, pSupporter->pResult);
      pQInfo->pointsRead += pQuery->pointsRead;

      if (pQuery->pointsRead > 0) {
        return;
      }
    }

    if (pSupporter->meterIdx >= pSids->numOfSids) {
      return;
    }
  
    resetCtxOutputBuf(pRuntimeEnv);
  
    for (int32_t i = 0; i < pRuntimeEnv->usedIndex; ++i) {
      SOutputRes *pOneRes = &pRuntimeEnv->pResult[i];
      clearGroupResultBuf(pOneRes, pQuery->numOfOutputCols);
    }

    pRuntimeEnv->usedIndex = 0;
    taosCleanUpIntHash(pRuntimeEnv->hashList);

    int32_t primeHashSlot = 10039;
    pRuntimeEnv->hashList = taosInitIntHash(primeHashSlot, POINTER_BYTES, taosHashInt);

    while (pSupporter->meterIdx < pSupporter->numOfMeters) {
      int32_t k = pSupporter->meterIdx;

      if (isQueryKilled(pQuery)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        return;
      }

      bool dataInDisk = true;
      bool dataInCache = true;
      if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, k, 0)) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;

        pSupporter->meterIdx++;
        continue;
      }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
      for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
        resetMMapWindow(&pRuntimeEnv->pVnodeFiles[i]);
      }
#endif

      SPointInterpoSupporter pointInterpSupporter = {0};
      if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter) == false) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;

        pSupporter->meterIdx++;
        continue;
      }

      // TODO handle the limit problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        forwardQueryStartPosition(pRuntimeEnv);

        if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;

          pSupporter->meterIdx++;
          continue;
        }
      }

      vnodeUpdateQueryColumnIndex(pQuery, pRuntimeEnv->pMeterObj);
      vnodeUpdateFilterColumnIndex(pQuery);

      vnodeScanAllData(pRuntimeEnv);

      pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
      doSkipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
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
        pSupporter->meterIdx++;

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL) ||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)) {
          break;
        }

      } else {
        // forward query range
        pQuery->skey = pQuery->lastKey;

        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->pointsRead == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          continue;
        } else {
          // buffer is full, wait for the next round to retrieve data from current meter
          assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          break;
        }
      }
    }
  }

  if (!isGroupbyNormalCol(pQuery->pGroupbyExpr) && !isFirstLastRowQuery(pQuery)) {
    doFinalizeResult(pRuntimeEnv);
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->cur = pRuntimeEnv->pTSBuf->cur;
  }

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    for (int32_t i = 0; i < pRuntimeEnv->usedIndex; ++i) {
      SOutputRes *buf = &pRuntimeEnv->pResult[i];
      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        buf->numOfRows = MAX(buf->numOfRows, buf->resultInfo[j].numOfRes);
      }
    }

    pQInfo->pMeterQuerySupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromGroupBuf(pQInfo, pRuntimeEnv->pResult);
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQuery->pointsOffset = pQuery->pointsToRead;

  moveDescOrderResultsToFront(pRuntimeEnv);

  dTrace(
      "QInfo %p vid:%d, numOfMeters:%d, index:%d, numOfGroups:%d, %d points returned, totalRead:%d totalReturn:%d,"
      "next skey:%lld, offset:%lld",
      pQInfo, pOneMeter->vnode, pSids->numOfSids, pSupporter->meterIdx, pSids->numOfSubSet, pQuery->pointsRead,
      pQInfo->pointsRead, pQInfo->pointsReturned, pQuery->skey, pQuery->limit.offset);
}

static void doOrderedScan(SQInfo *pQInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQuery *               pQuery = &pQInfo->query;

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    queryOnMultiDataFiles(pQInfo, pSupporter->pMeterDataInfo);
    if (pQInfo->code != TSDB_CODE_SUCCESS) {
      return;
    }

    queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
  } else {
    queryOnMultiDataCache(pQInfo, pSupporter->pMeterDataInfo);
    if (pQInfo->code != TSDB_CODE_SUCCESS) {
      return;
    }

    queryOnMultiDataFiles(pQInfo, pSupporter->pMeterDataInfo);
  }
}

static void setupMeterQueryInfoForSupplementQuery(SMeterQuerySupportObj *pSupporter) {
  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
    changeMeterQueryInfoForSuppleQuery(pMeterQueryInfo, pSupporter->rawSKey, pSupporter->rawEKey);
  }
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
  disableFunctForSuppleScan(pRuntimeEnv, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1;
  }

  SWAP(pSupporter->rawSKey, pSupporter->rawEKey, TSKEY);
  setupMeterQueryInfoForSupplementQuery(pSupporter);

  int64_t st = taosGetTimestampMs();
  doOrderedScan(pQInfo);
  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p supplementary scan completed, elapsed time: %lldms", pQInfo, et - st);

  /*
   * restore the env
   * the meter query info is not reset to the original state
   */
  SWAP(pSupporter->rawSKey, pSupporter->rawEKey, TSKEY);
  enableFunctForMasterScan(pRuntimeEnv, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1;
  }

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
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
    pQInfo->code = -TSDB_CODE_SERV_OUT_OF_MEMORY;
    return;
  }

  dTrace("QInfo:%p query start, qrange:%lld-%lld, order:%d, group:%d", pQInfo, pSupporter->rawSKey, pSupporter->rawEKey,
         pQuery->order.order, pSupporter->pSidSet->numOfSubSet);

  dTrace("QInfo:%p main query scan start", pQInfo);
  int64_t st = taosGetTimestampMs();
  doOrderedScan(pQInfo);
  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p main scan completed, elapsed time: %lldms, supplementary scan start, order:%d", pQInfo, et - st,
         pQuery->order.order ^ 1);

  // failed to save all intermediate results into disk, abort further query processing
  if (doCloseAllOpenedResults(pSupporter) != TSDB_CODE_SUCCESS) {
    dError("QInfo:%p failed to save intermediate results, abort further query processing", pQInfo);
    return;
  }
  
  doMultiMeterSupplementaryScan(pQInfo);

  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query killed, abort", pQInfo);
    return;
  }

  if (pQuery->nAggTimeInterval > 0) {
    assert(pSupporter->subgroupIdx == 0 && pSupporter->numOfGroupResultPages == 0);

    if (mergeMetersResultToOneGroups(pSupporter) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pSupporter, pQuery);
      
#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    }
  } else {  // not a interval query
    copyFromGroupBuf(pQInfo, pSupporter->pResult);
  }

  // handle the limitation of output buffer
  pQInfo->pointsRead += pQuery->pointsRead;
  dTrace("QInfo:%p points returned:%d, totalRead:%d totalReturn:%d", pQInfo, pQuery->pointsRead, pQInfo->pointsRead,
         pQInfo->pointsReturned);
}

/*
 * in each query, this function will be called only once, no retry for further result.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a];
 * select count(*) from table_name group by status_column;
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

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    pQInfo->pMeterQuerySupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromGroupBuf(pQInfo, pRuntimeEnv->pResult);
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

  // for ts_comp query, re-initialized is not allowed
  if (!isTSCompQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

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

  pQuery->pointsOffset = pQuery->pointsToRead;  // restore the available buffer
  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->pointsRead <= pQuery->pointsToRead);
  }
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

    // here we can ignore the records in case of no interpolation
    if ((pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) && pQuery->limit.offset > 0 &&
        pQuery->interpoType == TSDB_INTERPO_NONE) { // maxOutput <= 0, means current query does not generate any results
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
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    vnodeDecRefCount(pQInfo);
  
    return;
  }

  assert(pQInfo->refCount >= 1);

  SQuery *   pQuery = &pQInfo->query;
  SMeterObj *pMeterObj = pQInfo->pObj;

  dTrace("vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d, QInfo:%p", pMeterObj->vnode, pMeterObj->sid,
         pMeterObj->meterId, pMeterObj->numOfQueries, pQInfo);

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;
  assert(pRuntimeEnv->pMeterObj == pMeterObj);

  if (vnodeHasRemainResults(pQInfo)) {
    /*
     * There are remain results that are not returned due to result interpolation
     * So, we do keep in this procedure instead of launching retrieve procedure for next results.
     */
    int32_t numOfInterpo = 0;

    int32_t remain = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    pQuery->pointsRead = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata,
                                                     (tFilePage **)pRuntimeEnv->pInterpoBuf, remain, &numOfInterpo);

    doRevisedResultsByLimit(pQInfo);
    moveDescOrderResultsToFront(pRuntimeEnv);

    pQInfo->pointsInterpo += numOfInterpo;
    pQInfo->pointsRead += pQuery->pointsRead;

    dTrace(
        "QInfo:%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d "
        "totalReturn:%d",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
        pQInfo->pointsRead, pQInfo->pointsInterpo, pQInfo->pointsReturned);

    sem_post(&pQInfo->dataReady);
    vnodeDecRefCount(pQInfo);

    return;
  }

  // here we have scan all qualified data in both data file and cache
  if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
    // continue to get push data from the group result
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
      pQuery->pointsRead = 0;
      if (pQInfo->pMeterQuerySupporter->subgroupIdx > 0) {
        copyFromGroupBuf(pQInfo, pQInfo->pMeterQuerySupporter->pResult);
        pQInfo->pointsRead += pQuery->pointsRead;

        if (pQuery->pointsRead > 0) {
          dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned %d from group results, totalRead:%d totalReturn:%d",
                 pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, pQInfo->pointsRead,
                 pQInfo->pointsInterpo, pQInfo->pointsReturned);

          sem_post(&pQInfo->dataReady);
          vnodeDecRefCount(pQInfo);
  
          return;
        }
      }
    }

    pQInfo->over = 1;
    dTrace("QInfo:%p vid:%d sid:%d id:%s, query over, %d points are returned", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQInfo->pointsRead);

    vnodePrintQueryStatistics(pQInfo->pMeterQuerySupporter);
    sem_post(&pQInfo->dataReady);
    
    vnodeDecRefCount(pQInfo);
    return;
  }

  /* number of points returned during this query  */
  pQuery->pointsRead = 0;
  assert(pQuery->pos >= 0 && pQuery->slot >= 0);

  int64_t st = taosGetTimestampUs();

  if (pQuery->nAggTimeInterval != 0) {  // interval (down sampling operation)
    assert(pQuery->checkBufferInLoop == 0 && pQuery->pointsOffset == pQuery->pointsToRead);
    vnodeSingleMeterIntervalProcessor(pQInfo);
  } else {
    if (isFixedOutputQuery(pQuery)) {
      assert(pQuery->checkBufferInLoop == 0);
      vnodeSingleMeterFixedOutputProcessor(pQInfo);
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkBufferInLoop == 1);
      vnodeSingleMeterMultiOutputProcessor(pQInfo);
    }
  }

  // record the total elapsed time
  pQInfo->useconds += (taosGetTimestampUs() - st);

  /* check if query is killed or not */
  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query is killed", pQInfo);
    pQInfo->over = 1;
  } else {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, meter query thread completed, %d points are returned",
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}

void vnodeMultiMeterQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pMeterQuerySupporter == NULL) {
    return;
  }

  if (pQInfo->killed) {
    vnodeDecRefCount(pQInfo);
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    return;
  }

  assert(pQInfo->refCount >= 1);

  SQuery *pQuery = &pQInfo->query;
  pQuery->pointsRead = 0;

  int64_t st = taosGetTimestampUs();
  if (pQuery->nAggTimeInterval > 0 ||
      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && !isGroupbyNormalCol(pQuery->pGroupbyExpr))) {
    assert(pQuery->checkBufferInLoop == 0);
    vnodeMultiMeterQueryProcessor(pQInfo);
  } else {
    assert((pQuery->checkBufferInLoop == 1 && pQuery->nAggTimeInterval == 0) || isPointInterpoQuery(pQuery) ||
           isGroupbyNormalCol(pQuery->pGroupbyExpr));

    vnodeMultiMeterMultiOutputProcessor(pQInfo);
  }

  /* record the total elapsed time */
  pQInfo->useconds += (taosGetTimestampUs() - st);
  pQInfo->over = isQueryKilled(pQuery) ? 1 : 0;

  taosInterpoSetStartInfo(&pQInfo->pMeterQuerySupporter->runtimeEnv.interpoInfo, pQuery->pointsRead,
                          pQInfo->query.interpoType);

  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  if (pQuery->pointsRead == 0) {
    pQInfo->over = 1;
    dTrace("QInfo:%p over, %d meters queried, %d points are returned", pQInfo, pSupporter->numOfMeters,
           pQInfo->pointsRead);
    vnodePrintQueryStatistics(pSupporter);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}
