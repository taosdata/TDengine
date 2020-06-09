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

#define ALL_CACHE_BLOCKS_CHECKED(q)                            \
  (((q)->slot == (q)->currentSlot && QUERY_IS_ASC_QUERY(q)) || \
   ((q)->slot == (q)->firstSlot && (!QUERY_IS_ASC_QUERY(q))))

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

static bool doCheckWithPrevQueryRange(SQuery *pQuery, TSKEY nextKey) {
  if ((nextKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    return false;
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

static void enableExecutionForNextTable(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[i]);
    if (pResInfo != NULL) {
      pResInfo->complete = false;
    }
  }
}

static void queryOnMultiDataCache(SQInfo *pQInfo, SMeterDataInfo *pMeterDataInfo) {
  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;

  SMeterObj *pTempMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[0]->sid);
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
      SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[k]->sid);
      if (pMeterObj == NULL) {
        dError("QInfo:%p failed to find meterId:%d, continue", pQInfo, pMeterSidExtInfo[k]->sid);
        continue;
      }

      pQInfo->pObj = pMeterObj;
      pRuntimeEnv->pMeterObj = pMeterObj;

      if (pMeterDataInfo[k].pMeterQInfo == NULL) {
        pMeterDataInfo[k].pMeterQInfo =
            createMeterQueryInfo(pSupporter, pMeterObj->sid, pSupporter->rawSKey, pSupporter->rawEKey);
      }

      if (pMeterDataInfo[k].pMeterObj == NULL) {  // no data in disk for this meter, set its pointer
        setMeterDataInfo(&pMeterDataInfo[k], pMeterObj, k, groupIdx);
      }

      assert(pMeterDataInfo[k].meterOrderIdx == k && pMeterObj == pMeterDataInfo[k].pMeterObj);

      SMeterQueryInfo *pMeterQueryInfo = pMeterDataInfo[k].pMeterQInfo;
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

      if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
        dTrace("QInfo:%p vid:%d sid:%d id:%s, query completed, ignore data in cache. qrange:%" PRId64 "-%" PRId64
               ", lastKey:%" PRId64,
               pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
               pQuery->lastKey);

        continue;
      }

      qTrace("QInfo:%p vid:%d sid:%d id:%s, query in cache, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64, pQInfo,
             pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

      /*
       * find the appropriated start position in cache
       * NOTE: (taking ascending order query for example)
       * for the specific query range [pQuery->lastKey, pQuery->ekey], there may be no qualified result in cache.
       * Therefore, we need the first point that is greater(less) than the pQuery->lastKey, so the boundary check
       * should be ignored (the fourth parameter).
       */
      TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, true);
      if (nextKey < 0 || !doCheckWithPrevQueryRange(pQuery, nextKey)) {
        qTrace("QInfo:%p vid:%d sid:%d id:%s, no data qualified in cache, cache blocks:%d, lastKey:%" PRId64, pQInfo,
               pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->numOfBlocks, pQuery->lastKey);
        continue;
      }

      // data in this block may be flushed to disk and this block is allocated to other meter
      // todo try with remain cache blocks
      SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);
      if (pBlock == NULL) {
        continue;
      }

      bool        firstCheckSlot = true;
      SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

      for (int32_t i = 0; i < pCacheInfo->maxBlocks; ++i) {
        pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);

        /*
         * 1. pBlock == NULL. The cache block may be flushed to disk, so it is not available, skip and try next
         * The check for empty block is refactor to getCacheDataBlock function
         */
        if (pBlock == NULL) {
          if (ALL_CACHE_BLOCKS_CHECKED(pQuery)) {
            break;
          }

          FORWARD_CACHE_BLOCK_CHECK_SLOT(pQuery->slot, step, pCacheInfo->maxBlocks);
          continue;
        }

        setStartPositionForCacheBlock(pQuery, pBlock, &firstCheckSlot);

        TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;
        TSKEY  key = primaryKeys[pQuery->pos];

        // in handling file data block, the timestamp range validation is done during fetching candidate file blocks
        if ((key > pSupporter->rawEKey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (key < pSupporter->rawEKey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        if (pQuery->intervalTime == 0) {
          setExecutionContext(pSupporter, pMeterQueryInfo, k, pMeterDataInfo[k].groupIdx, key);
        } else {
          setIntervalQueryRange(pMeterQueryInfo, pSupporter, key);
          int32_t ret = setAdditionalInfo(pSupporter, k, pMeterQueryInfo);
          if (ret != TSDB_CODE_SUCCESS) {
            pQInfo->killed = 1;
            return;
          }
        }

        qTrace("QInfo:%p vid:%d sid:%d id:%s, query in cache, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64, pQInfo,
               pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

        // only record the key on last block
        SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
        SBlockInfo binfo = getBlockInfo(pRuntimeEnv);

        dTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", fileId:%d, slot:%d, pos:%d, bstatus:%d",
               GET_QINFO_ADDR(pQuery), binfo.keyFirst, binfo.keyLast, pQuery->fileId, pQuery->slot, pQuery->pos,
               pRuntimeEnv->blockStatus);

        totalBlocks++;
        stableApplyFunctionsOnBlock(pSupporter, &pMeterDataInfo[k], &binfo, NULL, searchFn);

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
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SMeterDataBlockInfoEx *pDataBlockInfoEx = NULL;
  int32_t                nAllocBlocksInfoSize = 0;

  SMeterObj *         pTempMeter = getMeterObj(pSupporter->pMetersHashTable, pSupporter->pMeterSidExtInfo[0]->sid);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pTempMeter->searchAlgorithm];

  int32_t          vnodeId = pTempMeter->vnode;
  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

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

    int32_t fileIdx = vnodeGetVnodeHeaderFileIndex(&fid, pRuntimeEnv, pQuery->order.order);
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
    int32_t          ret = vnodeFilterQualifiedMeters(pQInfo, vnodeId, pSupporter->pSidSet, pMeterDataInfo,
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
        dTrace("QInfo:%p load and check %" PRId64 " blocks, and continue. elapsed:%" PRId64 " ms", pQInfo,
               TRACE_OUTPUT_BLOCK_CNT, etimeUnit - stimeUnit);
        stimeUnit = taosGetTimestampMs();
      }

      SMeterDataBlockInfoEx *pInfoEx = &pDataBlockInfoEx[j];
      SMeterDataInfo *       pOneMeterDataInfo = pInfoEx->pMeterDataInfo;
      SMeterQueryInfo *      pMeterQueryInfo = pOneMeterDataInfo->pMeterQInfo;
      SMeterObj *            pMeterObj = pOneMeterDataInfo->pMeterObj;

      pQInfo->pObj = pMeterObj;
      pRuntimeEnv->pMeterObj = pMeterObj;

      restoreIntervalQueryRange(pRuntimeEnv, pMeterQueryInfo);

      if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
        qTrace("QInfo:%p vid:%d sid:%d id:%s, query completed, no need to scan this data block. qrange:%" PRId64
               "-%" PRId64 ", lastKey:%" PRId64,
               pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey,
               pQuery->lastKey);

        continue;
      }

      SCompBlock *pBlock = pInfoEx->pBlock.compBlock;
      bool        ondemandLoad = onDemandLoadDatablock(pQuery, pMeterQueryInfo->queryRangeSet);
      ret = LoadDatablockOnDemand(pBlock, &pInfoEx->pBlock.fields, &pRuntimeEnv->blockStatus, pRuntimeEnv, fileIdx,
                                  pInfoEx->blockIndex, searchFn, ondemandLoad);
      if (ret != DISK_DATA_LOADED) {
        pSummary->skippedFileBlocks++;
        continue;
      }

      SBlockInfo binfo = getBlockBasicInfo(pRuntimeEnv, pBlock, BLK_FILE_BLOCK);
      int64_t    nextKey = -1;

      assert(pQuery->pos >= 0 && pQuery->pos < pBlock->numOfPoints);
      TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

      if (IS_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus) && needPrimaryTimestampCol(pQuery, &binfo)) {
        nextKey = primaryKeys[pQuery->pos];

        if (!doCheckWithPrevQueryRange(pQuery, nextKey)) {
          qTrace("QInfo:%p vid:%d sid:%d id:%s, no data qualified in data file, lastKey:%" PRId64, pQInfo,
                 pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->numOfBlocks, pQuery->lastKey);
          continue;
        }
      } else {
        // if data block is not loaded, it must be the intermediate blocks
        assert((pBlock->keyFirst >= pQuery->lastKey && pBlock->keyLast <= pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
               (pBlock->keyFirst >= pQuery->ekey && pBlock->keyLast <= pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));
        nextKey = QUERY_IS_ASC_QUERY(pQuery) ? pBlock->keyFirst : pBlock->keyLast;
      }

      if (pQuery->intervalTime == 0) {
        setExecutionContext(pSupporter, pMeterQueryInfo, pOneMeterDataInfo->meterOrderIdx, pOneMeterDataInfo->groupIdx,
                            nextKey);
      } else {  // interval query
        setIntervalQueryRange(pMeterQueryInfo, pSupporter, nextKey);
        ret = setAdditionalInfo(pSupporter, pOneMeterDataInfo->meterOrderIdx, pMeterQueryInfo);
        if (ret != TSDB_CODE_SUCCESS) {
          tfree(pReqMeterDataInfo);  // error code has been set
          pQInfo->killed = 1;
          return;
        }
      }

      stableApplyFunctionsOnBlock(pSupporter, pOneMeterDataInfo, &binfo, pInfoEx->pBlock.fields, searchFn);
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
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *           pQuery = &pQInfo->query;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[index]->sid);
  if (pMeterObj == NULL) {
    dError("QInfo:%p do not find required meter id: %d, all meterObjs id is:", pQInfo, pMeterSidExtInfo[index]->sid);
    return false;
  }

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pMeterSidExtInfo[index]);

  dTrace("QInfo:%p query on (%d): vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64, pQInfo, index - start,
         pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey);

  pQInfo->pObj = pMeterObj;
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->pMeterObj = pMeterObj;

  vnodeUpdateQueryColumnIndex(pQuery, pRuntimeEnv->pMeterObj);
  vnodeUpdateFilterColumnIndex(pQuery);

  vnodeCheckIfDataExists(pRuntimeEnv, pMeterObj, dataInDisk, dataInCache);

  // data in file or cache is not qualified for the query. abort
  if (!(dataInCache || dataInDisk)) {
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64 ", nores, %p", pQInfo, pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery);
    return false;
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pRuntimeEnv->cur.vnodeIndex == -1) {
      tVariant tag = {0};
      tVariantAssign(&tag, &pRuntimeEnv->pCtx[0].tag);
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, &tag);

      // failed to find data with the specified tag value
      if (elem.vnode < 0) {
        return false;
      }
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pRuntimeEnv->cur);
    }
  }

  initCtxOutputBuf(pRuntimeEnv);
  return true;
}

static int64_t doCheckMetersInGroup(SQInfo *pQInfo, int32_t index, int32_t start) {
  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
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

  if (!normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL)) {
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

/**
 * super table query handler
 * 1. super table projection query, group-by on normal columns query, ts-comp query
 * 2. point interpolation query, last row query
 *
 * @param pQInfo
 */
static void vnodeSTableSeqProcessor(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery * pQuery = &pQInfo->query;
  tSidSet *pSids = pSupporter->pSidSet;

  int32_t vid = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[0]->sid)->vnode;

  if (isPointInterpoQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);

    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pSupporter->subgroupIdx < pSids->numOfSubSet) {
      int32_t start = pSids->starterPos[pSupporter->subgroupIdx];
      int32_t end = pSids->starterPos[pSupporter->subgroupIdx + 1] - 1;

      if (isFirstLastRowQuery(pQuery)) {
        dTrace("QInfo:%p last_row query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pSids->numOfSubSet,
               pSupporter->subgroupIdx);

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
          SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[k]->sid);
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
        dTrace("QInfo:%p interp query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pSids->numOfSubSet,
               pSupporter->subgroupIdx);

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
    /*
     * 1. super table projection query, 2. group-by on normal columns query, 3. ts-comp query
     */
    assert(pSupporter->meterIdx >= 0);

    /*
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pSupporter->subgroupIdx > 0) {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
      pQInfo->pointsRead += pQuery->pointsRead;

      if (pQuery->pointsRead > 0) {
        return;
      }
    }

    if (pSupporter->meterIdx >= pSids->numOfSids) {
      return;
    }

    resetCtxOutputBuf(pRuntimeEnv);
    resetTimeWindowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);

    while (pSupporter->meterIdx < pSupporter->numOfMeters) {
      int32_t k = pSupporter->meterIdx;

      if (isQueryKilled(pQuery)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        return;
      }

      TSKEY skey = pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key;
      if (skey > 0) {
        pQuery->skey = skey;
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
      if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL) == false) {
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

      vnodeScanAllData(pRuntimeEnv);

      pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
      doSkipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
      if (doRevisedResultsByLimit(pQInfo)) {
        pSupporter->meterIdx = pSupporter->pSidSet->numOfSids;
        break;
      }

      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);

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

        pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL) ||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)) {
          break;
        }

      } else {  // forward query range
        pQuery->skey = pQuery->lastKey;

        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->pointsRead == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          continue;
        } else {
          pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;
          // buffer is full, wait for the next round to retrieve data from current meter
          assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          break;
        }
      }
    }
  }

  /*
   * 1. super table projection query, group-by on normal columns query, ts-comp query
   * 2. point interpolation query, last row query
   *
   * group-by on normal columns query and last_row query do NOT invoke the finalizer here,
   * since the finalize stage will be done at the client side.
   *
   * projection query, point interpolation query do not need the finalizer.
   *
   * Only the ts-comp query requires the finalizer function to be executed here.
   */
  if (isTSCompQuery(pQuery)) {
    doFinalizeResult(pRuntimeEnv);
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->cur = pRuntimeEnv->pTSBuf->cur;
  }

  // todo refactor
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowStatus *pStatus = &pWindowResInfo->pResult[i].status;
      pStatus->closed = true;  // enable return all results for group by normal columns

      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
      }
    }

    pQInfo->pTableQuerySupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromWindowResToSData(pQInfo, pWindowResInfo->pResult);
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQuery->pointsOffset = pQuery->pointsToRead;

  dTrace(
      "QInfo %p vid:%d, numOfMeters:%d, index:%d, numOfGroups:%d, %d points returned, totalRead:%d totalReturn:%d,"
      "next skey:%" PRId64 ", offset:%" PRId64,
      pQInfo, vid, pSids->numOfSids, pSupporter->meterIdx, pSids->numOfSubSet, pQuery->pointsRead, pQInfo->pointsRead,
      pQInfo->pointsReturned, pQuery->skey, pQuery->limit.offset);
}

static void doOrderedScan(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
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

static void setupMeterQueryInfoForSupplementQuery(STableQuerySupportObj *pSupporter) {
  SQuery *pQuery = pSupporter->runtimeEnv.pQuery;

  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
    changeMeterQueryInfoForSuppleQuery(pQuery, pMeterQueryInfo, pSupporter->rawSKey, pSupporter->rawEKey);
  }
}

static void doMultiMeterSupplementaryScan(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = &pQInfo->query;

  if (!needSupplementaryScan(pQuery)) {
    dTrace("QInfo:%p no need to do supplementary scan, query completed", pQInfo);
    return;
  }

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);
  disableFunctForSuppleScan(pSupporter, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1u;
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
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *               pQuery = &pQInfo->query;

  if (pSupporter->subgroupIdx > 0) {
    /*
     * if the subgroupIdx > 0, the query process must be completed yet, we only need to
     * copy the data into output buffer
     */
    if (pQuery->intervalTime > 0) {
      copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    } else {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
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

  dTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", order:%d, group:%d", pQInfo, pSupporter->rawSKey,
         pSupporter->rawEKey, pQuery->order.order, pSupporter->pSidSet->numOfSubSet);

  dTrace("QInfo:%p main query scan start", pQInfo);
  int64_t st = taosGetTimestampMs();
  doOrderedScan(pQInfo);
  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p main scan completed, elapsed time: %lldms, supplementary scan start, order:%d", pQInfo, et - st,
         pQuery->order.order ^ 1u);

  if (pQuery->intervalTime > 0) {
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
      closeAllTimeWindow(&pMeterQueryInfo->windowResInfo);
    }
  } else {  // close results for group result
    closeAllTimeWindow(&pRuntimeEnv->windowResInfo);
  }

  doMultiMeterSupplementaryScan(pQInfo);

  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query killed, abort", pQInfo);
    return;
  }

  if (pQuery->intervalTime > 0 || isSumAvgRateQuery(pQuery)) {
    assert(pSupporter->subgroupIdx == 0 && pSupporter->numOfGroupResultPages == 0);

    if (mergeMetersResultToOneGroups(pSupporter) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    }
  } else {  // not a interval query
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
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
static void vnodeSingleTableFixedOutputProcessor(SQInfo *pQInfo) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

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

  pQInfo->pointsRead = pQuery->pointsRead;
}

static void vnodeSingleTableMultiOutputProcessor(SQInfo *pQInfo) {
  SQuery *   pQuery = &pQInfo->query;
  SMeterObj *pMeterObj = pQInfo->pObj;

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

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

    dTrace("QInfo:%p vid:%d sid:%d id:%s, skip current result, offset:%" PRId64 ", next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->limit.offset, pQuery->lastKey,
           pQuery->ekey);

    resetCtxOutputBuf(pRuntimeEnv);
  }

  doRevisedResultsByLimit(pQInfo);
  pQInfo->pointsRead += pQuery->pointsRead;

  if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    assert(nextTimestamp > 0 || ((nextTimestamp < 0) && Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)));

    dTrace("QInfo:%p vid:%d sid:%d id:%s, query abort due to buffer limitation, next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->lastKey, pQuery->ekey);
  }

  dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d", pQInfo, pMeterObj->vnode,
         pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned);

  pQuery->pointsOffset = pQuery->pointsToRead;  // restore the available buffer
  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->pointsRead <= pQuery->pointsToRead);
  }
}

static void vnodeSingleMeterIntervalMainLooper(STableQuerySupportObj *pSupporter, SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    initCtxOutputBuf(pRuntimeEnv);
    vnodeScanAllData(pRuntimeEnv);
    
    if (isQueryKilled(pQuery)) {
      return;
    }

    assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_NOT_COMPLETED));
    doFinalizeResult(pRuntimeEnv);

    // here we can ignore the records in case of no interpolation
    // todo handle offset, in case of top/bottom interval query
    if ((pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) && pQuery->limit.offset > 0 &&
        pQuery->interpoType == TSDB_INTERPO_NONE) {
      // maxOutput <= 0, means current query does not generate any results
      int32_t numOfClosed = numOfClosedTimeWindow(&pRuntimeEnv->windowResInfo);

      int32_t c = MIN(numOfClosed, pQuery->limit.offset);
      clearFirstNTimeWindow(pRuntimeEnv, c);
      pQuery->limit.offset -= c;
    }

    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
      break;
    }

    // load the data block for the next retrieve
    loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
      break;
    }
  }
}

/* handle time interval query on single table */
static void vnodeSingleTableIntervalProcessor(SQInfo *pQInfo) {
  SQuery *   pQuery = &(pQInfo->query);
  SMeterObj *pMeterObj = pQInfo->pObj;

  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  int32_t numOfInterpo = 0;

  while (1) {
    resetCtxOutputBuf(pRuntimeEnv);
    vnodeSingleMeterIntervalMainLooper(pSupporter, pRuntimeEnv);

    if (pQuery->intervalTime > 0) {
      pSupporter->subgroupIdx = 0;  // always start from 0
      pQuery->pointsRead = 0;
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);

      clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);
    }

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->interpoType == TSDB_INTERPO_NONE) {
      doRevisedResultsByLimit(pQInfo);
      break;
    } else {
      taosInterpoSetStartInfo(&pRuntimeEnv->interpoInfo, pQuery->pointsRead, pQuery->interpoType);
      SData **pInterpoBuf = pRuntimeEnv->pInterpoBuf;

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        memcpy(pInterpoBuf[i]->data, pQuery->sdata[i]->data, pQuery->pointsRead * pQuery->pSelectExpr[i].resBytes);
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

  // all data scanned, the group by normal column can return
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {//todo refactor with merge interval time result
    pSupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQInfo->pointsInterpo += numOfInterpo;

  dTrace("%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d totalReturn:%d",
         pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
         pQInfo->pointsRead - pQInfo->pointsInterpo, pQInfo->pointsInterpo, pQInfo->pointsReturned);
}

void vnodeSingleTableQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pTableQuerySupporter == NULL) {
    dTrace("%p freed abort query", pQInfo);
    return;
  }

  if (pQInfo->killed) {
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    vnodeDecRefCount(pQInfo);

    return;
  }

  assert(pQInfo->refCount >= 1);

  SQuery *               pQuery = &pQInfo->query;
  SMeterObj *            pMeterObj = pQInfo->pObj;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  assert(pRuntimeEnv->pMeterObj == pMeterObj);

  dTrace("vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d, QInfo:%p", pMeterObj->vnode, pMeterObj->sid,
         pMeterObj->meterId, pMeterObj->numOfQueries, pQInfo);

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
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr) ||
        (pQuery->intervalTime > 0 && pQInfo->pointsReturned < pQuery->limit.limit)) {
      //todo limit the output for interval query?
      pQuery->pointsRead = 0;
      pSupporter->subgroupIdx = 0;  // always start from 0

      if (pRuntimeEnv->windowResInfo.size > 0) {
        copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
        pQInfo->pointsRead += pQuery->pointsRead;

        clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);

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
    dTrace("QInfo:%p vid:%d sid:%d id:%s, query over, %d points are returned", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pQInfo->pointsRead);

    vnodePrintQueryStatistics(pSupporter);
    sem_post(&pQInfo->dataReady);

    vnodeDecRefCount(pQInfo);
    return;
  }

  /* number of points returned during this query  */
  pQuery->pointsRead = 0;
  assert(pQuery->pos >= 0 && pQuery->slot >= 0);

  int64_t st = taosGetTimestampUs();

  // group by normal column, sliding window query, interval query are handled by interval query processor
  if (pQuery->intervalTime != 0 || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // interval (down sampling operation)
    assert(pQuery->checkBufferInLoop == 0 && pQuery->pointsOffset == pQuery->pointsToRead);
    vnodeSingleTableIntervalProcessor(pQInfo);
  } else {
    if (isFixedOutputQuery(pQuery)) {
      assert(pQuery->checkBufferInLoop == 0);
      vnodeSingleTableFixedOutputProcessor(pQInfo);
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkBufferInLoop == 1);
      vnodeSingleTableMultiOutputProcessor(pQInfo);
    }
  }

  // record the total elapsed time
  pQInfo->useconds += (taosGetTimestampUs() - st);

  /* check if query is killed or not */
  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query is killed", pQInfo);
    pQInfo->over = 1;
  } else {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, meter query thread completed, %d points are returned", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}

void vnodeMultiMeterQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pTableQuerySupporter == NULL) {
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
  if (pQuery->intervalTime > 0 ||
      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && !isGroupbyNormalCol(pQuery->pGroupbyExpr))) {
    assert(pQuery->checkBufferInLoop == 0);
    vnodeMultiMeterQueryProcessor(pQInfo);
  } else {
    assert((pQuery->checkBufferInLoop == 1 && pQuery->intervalTime == 0) || isPointInterpoQuery(pQuery) ||
           isGroupbyNormalCol(pQuery->pGroupbyExpr));

    vnodeSTableSeqProcessor(pQInfo);
  }

  /* record the total elapsed time */
  pQInfo->useconds += (taosGetTimestampUs() - st);
  pQInfo->over = isQueryKilled(pQuery) ? 1 : 0;

  taosInterpoSetStartInfo(&pQInfo->pTableQuerySupporter->runtimeEnv.interpoInfo, pQuery->pointsRead,
                          pQInfo->query.interpoType);

  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  if (pQuery->pointsRead == 0) {
    pQInfo->over = 1;
    dTrace("QInfo:%p over, %d meters queried, %d points are returned", pQInfo, pSupporter->numOfMeters,
           pQInfo->pointsRead);
    vnodePrintQueryStatistics(pSupporter);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}
