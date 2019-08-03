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

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "taosmsg.h"
#include "textbuffer.h"
#include "tinterpolation.h"
#include "tscSecondaryMerge.h"
#include "tscompression.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"

#include "vnodeCache.h"
#include "vnodeDataFilterFunc.h"
#include "vnodeFile.h"
#include "vnodeQueryImpl.h"

static int32_t copyDataFromMMapBuffer(int fd, SQInfo *pQInfo, SQueryFileInfo *pQueryFile, char *buf, uint64_t offset,
                                      int32_t size);
static int32_t readDataFromDiskFile(int fd, SQInfo *pQInfo, SQueryFileInfo *pQueryFile, char *buf, uint64_t offset,
                                    int32_t size);

__read_data_fn_t readDataFunctor[2] = {
    copyDataFromMMapBuffer, readDataFromDiskFile,
};

#define IS_DISK_DATA_BLOCK(q) ((q)->fileId >= 0)

static int64_t comp_block_info_read_bytes = 0;

static void destroyMeterQueryInfo(SMeterQueryInfo *pMeterQInfo);
static void vnodeInitLoadCompBlockInfo(SQueryLoadCompBlockInfo *pCompBlockLoadInfo);
static int32_t moveToNextBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn,
                               bool loadData);
static int32_t doMergeMetersResultsToGroupRes(SMeterQuerySupportObj *pSupporter, SQuery *pQuery,
                                              SQueryRuntimeEnv *pRuntimeEnv, SMeterDataInfo *pMeterHeadDataInfo,
                                              int32_t start, int32_t end);

static TSKEY getTimestampInCacheBlock(SCacheBlock *pBlock, int32_t index);
static TSKEY getTimestampInDiskBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t index);

static void savePointPosition(SPositionInfo *position, int32_t fileId, int32_t slot, int32_t pos);
static int32_t getNextDataFileCompInfo(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t step);

static void setOutputBuffer(SQueryRuntimeEnv *pRuntimeEnv, SOutputRes *pResult);

static void getAlignedIntervalQueryRange(SQuery *pQuery, TSKEY keyInData, TSKEY skey, TSKEY ekey);
static void doApplyIntervalQueryOnBlock(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *pInfo,
                                        SBlockInfo *pBlockInfo, int64_t *pPrimaryCol, char *sdata, SField *pFields,
                                        __block_search_fn_t searchFn);

static void saveResult(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *sqinfo, int32_t numOfResult);
static void applyIntervalQueryOnBlock(SMeterQuerySupportObj *pSupporter, SMeterDataInfo *pInfoEx, char *data,
                                      int64_t *pPrimaryData, SBlockInfo *pBlockInfo, int32_t blockStatus,
                                      SField *pFields, __block_search_fn_t searchFn);

static void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx);
static void flushFromResultBuf(SMeterQuerySupportObj *pSupporter, const SQuery *pQuery,
                               const SQueryRuntimeEnv *pRuntimeEnv);
static void validateTimestampForSupplementResult(SQueryRuntimeEnv *pRuntimeEnv, int64_t numOfIncrementRes);
static void getBasicCacheInfoSnapshot(SQuery *pQuery, SCacheInfo *pCacheInfo, int32_t vid);
static void getQueryPositionForCacheInvalid(SQueryRuntimeEnv *pRuntimeEnv, __block_search_fn_t searchFn);

// check the offset value integrity
static FORCE_INLINE int32_t validateHeaderOffsetSegment(SQInfo *pQInfo, char *filePath, int32_t vid, char *data,
                                                        int32_t size) {
  if (!taosCheckChecksumWhole((uint8_t *)data + TSDB_FILE_HEADER_LEN, size)) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file offset area is broken", pQInfo, vid, filePath);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t getCompHeaderSegSize(SVnodeCfg *pCfg) {
  return pCfg->maxSessions * sizeof(SCompHeader) + sizeof(TSCKSUM);
}

static FORCE_INLINE int32_t getCompHeaderStartPosition(SVnodeCfg *pCfg) {
  return TSDB_FILE_HEADER_LEN + getCompHeaderSegSize(pCfg);
}

static FORCE_INLINE int32_t validateCompBlockOffset(SQInfo *pQInfo, SMeterObj *pMeterObj, SCompHeader *pCompHeader,
                                                    SQueryFileInfo *pQueryFileInfo, int32_t headerSize) {
  if (pCompHeader->compInfoOffset < headerSize || pCompHeader->compInfoOffset > pQueryFileInfo->headFileSize) {
    dError("QInfo:%p vid:%d sid:%d id:%s, compInfoOffset:%d is not valid, size:%ld",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pCompHeader->compInfoOffset,
        pQueryFileInfo->headFileSize);

    return -1;
  }

  return 0;
}

// check compinfo integrity
static FORCE_INLINE int32_t validateCompBlockInfoSegment(SQInfo *pQInfo, char *filePath, int32_t vid,
                                                         SCompInfo *compInfo, int64_t offset) {
  if (!taosCheckChecksumWhole((uint8_t *)compInfo, sizeof(SCompInfo))) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compInfo broken, offset:%lld", pQInfo, vid, filePath,
            offset);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t validateCompBlockSegment(SQInfo *pQInfo, char *filePath, SCompInfo *compInfo, char *pBlock,
                                                     int32_t vid, TSCKSUM checksum) {
  uint32_t size = compInfo->numOfBlocks * sizeof(SCompBlock);

  if (checksum != taosCalcChecksum(0, (uint8_t *)pBlock, size)) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compblock is broken:%ld", pQInfo, vid, filePath,
            (char *)compInfo + sizeof(SCompInfo));
    return -1;
  }

  return 0;
}

static void vnodeFreeFieldsEx(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  vnodeFreeFields(pQuery);

  vnodeInitLoadCompBlockInfo(&pRuntimeEnv->loadCompBlockInfo);
}

static bool vnodeIsCompBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  /*
   * check if data file header of this table has been loaded into memory, avoid to reloaded comp Block info
   */
  SQueryLoadCompBlockInfo *pLoadCompBlockInfo = &pRuntimeEnv->loadCompBlockInfo;

  // if vnodeFreeFields is called, the pQuery->pFields is NULL
  if (pLoadCompBlockInfo->fileListIndex == fileIndex && pLoadCompBlockInfo->sid == pMeterObj->sid &&
      pQuery->pFields != NULL && pQuery->fileId > 0) {
    assert(pRuntimeEnv->pHeaderFiles[fileIndex].fileID == pLoadCompBlockInfo->fileId && pQuery->numOfBlocks > 0);
    return true;
  }

  return false;
}

static void vnodeSetCompBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIndex, int32_t sid) {
  SQueryLoadCompBlockInfo *pLoadCompBlockInfo = &pRuntimeEnv->loadCompBlockInfo;

  pLoadCompBlockInfo->sid = sid;
  pLoadCompBlockInfo->fileListIndex = fileIndex;
  pLoadCompBlockInfo->fileId = pRuntimeEnv->pHeaderFiles[fileIndex].fileID;
}

static void vnodeInitLoadCompBlockInfo(SQueryLoadCompBlockInfo *pCompBlockLoadInfo) {
  pCompBlockLoadInfo->sid = -1;
  pCompBlockLoadInfo->fileId = -1;
  pCompBlockLoadInfo->fileListIndex = -1;
}

static bool vnodeIsDatablockLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex) {
  SQuery *             pQuery = pRuntimeEnv->pQuery;
  SQueryLoadBlockInfo *pLoadInfo = &pRuntimeEnv->loadBlockInfo;

  /* this block has been loaded into memory, return directly */
  if (pLoadInfo->fileId == pQuery->fileId && pLoadInfo->slotIdx == pQuery->slot && pQuery->slot != -1 &&
      pLoadInfo->sid == pMeterObj->sid) {
    assert(fileIndex == pLoadInfo->fileListIndex);
    return true;
  }

  return false;
}

static void vnodeSetDataBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex) {
  SQuery *             pQuery = pRuntimeEnv->pQuery;
  SQueryLoadBlockInfo *pLoadInfo = &pRuntimeEnv->loadBlockInfo;

  pLoadInfo->fileId = pQuery->fileId;
  pLoadInfo->slotIdx = pQuery->slot;
  pLoadInfo->fileListIndex = fileIndex;
  pLoadInfo->sid = pMeterObj->sid;
}

static void vnodeInitDataBlockInfo(SQueryLoadBlockInfo *pBlockLoadInfo) {
  pBlockLoadInfo->slotIdx = -1;
  pBlockLoadInfo->fileId = -1;
  pBlockLoadInfo->sid = -1;
  pBlockLoadInfo->fileListIndex = -1;
}

/*
 * read comp block info from header file
 *
 */
static int vnodeGetCompBlockInfo(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIndex) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  SVnodeCfg *     pCfg = &vnodeList[pMeterObj->vnode].cfg;
  SQueryFileInfo *pQueryFileInfo = &pRuntimeEnv->pHeaderFiles[fileIndex];
  int32_t         fd = pQueryFileInfo->headerFd;

  int64_t st = taosGetTimestampUs();

  if (vnodeIsCompBlockInfoLoaded(pRuntimeEnv, pMeterObj, fileIndex)) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d compBlock info is loaded, not reload",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQueryFileInfo->fileID);

    return pQuery->numOfBlocks;
  }

  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;
  pSummary->readCompInfo++;
  pSummary->numOfSeek++;

#if 1
  char *data = pRuntimeEnv->pHeaderFiles[fileIndex].pHeaderFileData;
  UNUSED(fd);
#else
  char *data = calloc(1, tmsize + TSDB_FILE_HEADER_LEN);
  read(fd, data, tmsize + TSDB_FILE_HEADER_LEN);
#endif

  // check the offset value integrity
  if (validateHeaderOffsetSegment(pQInfo, pQueryFileInfo->headerFilePath, pMeterObj->vnode, data,
                                  getCompHeaderSegSize(pCfg)) < 0) {
    return -1;
  }

  int64_t      offset = TSDB_FILE_HEADER_LEN + sizeof(SCompHeader) * pMeterObj->sid;
  SCompHeader *compHeader = (SCompHeader *)(data + offset);

  // no data in this file for specified meter, abort
  if (compHeader->compInfoOffset == 0) {
    return 0;
  }

  // corrupted file may cause the invalid compInfoOffset, check needs
  if (validateCompBlockOffset(pQInfo, pMeterObj, compHeader, pQueryFileInfo, getCompHeaderStartPosition(pCfg)) < 0) {
    return -1;
  }

#if 1
  SCompInfo *compInfo = (SCompInfo *)(data + compHeader->compInfoOffset);
#else
  lseek(fd, compHeader->compInfoOffset, SEEK_SET);
  SCompInfo  CompInfo = {0};
  SCompInfo *compInfo = &CompInfo;
  read(fd, compInfo, sizeof(SCompInfo));
#endif

  // check compblock info integrity
  if (validateCompBlockInfoSegment(pQInfo, pQueryFileInfo->headerFilePath, pMeterObj->vnode, compInfo,
                                   compHeader->compInfoOffset) < 0) {
    return -1;
  }

  if (compInfo->numOfBlocks <= 0 || compInfo->uid != pMeterObj->uid) {
    return 0;
  }

  // free allocated SField data
  vnodeFreeFieldsEx(pRuntimeEnv);
  pQuery->numOfBlocks = (int32_t)compInfo->numOfBlocks;

  int32_t compBlockSize = compInfo->numOfBlocks * sizeof(SCompBlock);
  size_t  bufferSize = compBlockSize + POINTER_BYTES * compInfo->numOfBlocks;

  // prepare buffer to hold compblock data
  if (pQuery->blockBufferSize != bufferSize) {
    pQuery->pBlock = realloc(pQuery->pBlock, bufferSize);
    pQuery->blockBufferSize = (int32_t)bufferSize;
  }

  memset(pQuery->pBlock, 0, (size_t)pQuery->blockBufferSize);

#if 1
  memcpy(pQuery->pBlock, (char *)compInfo + sizeof(SCompInfo), (size_t)compBlockSize);
  TSCKSUM checksum = *(TSCKSUM *)((char *)compInfo + sizeof(SCompInfo) + compBlockSize);
#else
  TSCKSUM checksum;
  read(fd, pQuery->pBlock, compBlockSize);
  read(fd, &checksum, sizeof(TSCKSUM));
#endif

  // check comp block integrity
  if (validateCompBlockSegment(pQInfo, pQueryFileInfo->headerFilePath, compInfo, (char *)pQuery->pBlock,
                               pMeterObj->vnode, checksum) < 0) {
    return -1;
  }

  pQuery->pFields = (SField **)((char *)pQuery->pBlock + compBlockSize);
  vnodeSetCompBlockInfoLoaded(pRuntimeEnv, fileIndex, pMeterObj->sid);

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load compblock info, size:%d, elapsed:%f ms",
      pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pRuntimeEnv->pHeaderFiles[fileIndex].fileID,
      compBlockSize, (et - st) / 1000.0);

  pSummary->totalCompInfoSize += compBlockSize;
  pSummary->loadCompInfoUs += (et - st);

  return pQuery->numOfBlocks;
}

bool doRevisedResultsByLimit(SQInfo *pQInfo) {
  SQuery *pQuery = &pQInfo->query;

  if ((pQuery->limit.limit > 0) && (pQuery->pointsRead + pQInfo->pointsRead > pQuery->limit.limit)) {
    pQuery->pointsRead = pQuery->limit.limit - pQInfo->pointsRead;

    setQueryStatus(pQuery, QUERY_COMPLETED);  // query completed
    return true;
  }

  return false;
}

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, int64_t StartQueryTimestamp, void *inputData,
                          char *primaryColumnData, int32_t size, int32_t functionId, SField *pField, bool hasNull,
                          int32_t blockStatus, void *param, int32_t scanFlag);

static tFilePage **createInMemGroupResultBuf(SQLFunctionCtx *pCtx, int32_t nOutputCols, int32_t nAlloc);
static void destroyBuf(tFilePage **pBuf, int32_t nOutputCols);

static int32_t binarySearchForBlockImpl(SCompBlock *pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
  int32_t firstSlot = 0;
  int32_t lastSlot = numOfBlocks - 1;

  int32_t midSlot = firstSlot;

  while (1) {
    numOfBlocks = lastSlot - firstSlot + 1;
    midSlot = (firstSlot + (numOfBlocks >> 1));

    if (numOfBlocks == 1) break;

    if (skey > pBlock[midSlot].keyLast) {
      if (numOfBlocks == 2) break;
      if ((order == TSQL_SO_DESC) && (skey < pBlock[midSlot + 1].keyFirst)) break;
      firstSlot = midSlot + 1;
    } else if (skey < pBlock[midSlot].keyFirst) {
      if ((order == TSQL_SO_ASC) && (skey > pBlock[midSlot - 1].keyLast)) break;
      lastSlot = midSlot - 1;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

static int32_t binarySearchForBlock(SQuery *pQuery, int64_t key) {
  return binarySearchForBlockImpl(pQuery->pBlock, pQuery->numOfBlocks, key, pQuery->order.order);
}

static UNUSED_FUNC int32_t resetMMapWindow(SQueryFileInfo *pQueryFileInfo) {
  /* unmap previous buffer */
  munmap(pQueryFileInfo->pDataFileData, pQueryFileInfo->defaultMappingSize);

  pQueryFileInfo->dtFileMappingOffset = 0;
  pQueryFileInfo->pDataFileData = mmap(NULL, pQueryFileInfo->defaultMappingSize, PROT_READ, MAP_PRIVATE | MAP_POPULATE,
                                       pQueryFileInfo->dataFd, pQueryFileInfo->dtFileMappingOffset);
  if (pQueryFileInfo->pDataFileData == MAP_FAILED) {
    dError("failed to mmaping data file:%s, reason:%s", pQueryFileInfo->dataFilePath, strerror(errno));
    return -1;
  }

  return 0;
}

static int32_t moveMMapWindow(SQueryFileInfo *pQueryFileInfo, uint64_t offset) {
  uint64_t upperBnd = (pQueryFileInfo->dtFileMappingOffset + pQueryFileInfo->defaultMappingSize - 1);

  /* data that are located in current mmapping window */
  if ((offset >= pQueryFileInfo->dtFileMappingOffset && offset <= upperBnd) &&
      pQueryFileInfo->pDataFileData != MAP_FAILED) {
    // if it mapping failed, try again when it is called.
    return 0;
  }

  /*
  * 1. there is import data that locate farther from the beginning, but with less timestamp, so we need to move the
  * window backwards
  * 2. otherwise, move the mmaping window forward
  */
  upperBnd = (offset / pQueryFileInfo->defaultMappingSize + 1) * pQueryFileInfo->defaultMappingSize - 1;

  /* unmap previous buffer */
  if (pQueryFileInfo->pDataFileData != MAP_FAILED) {
    int32_t ret = munmap(pQueryFileInfo->pDataFileData, pQueryFileInfo->defaultMappingSize);
    pQueryFileInfo->pDataFileData = MAP_FAILED;
    if (ret != 0) {
      dError("failed to unmmaping data file:%s, handle:%d, offset:%ld, reason:%s", pQueryFileInfo->dataFilePath,
             pQueryFileInfo->dataFd, pQueryFileInfo->dtFileMappingOffset, strerror(errno));
      return -1;
    }
  }

  /* mmap from the new position */
  pQueryFileInfo->dtFileMappingOffset = upperBnd - pQueryFileInfo->defaultMappingSize + 1;
  pQueryFileInfo->pDataFileData = mmap(NULL, pQueryFileInfo->defaultMappingSize, PROT_READ, MAP_PRIVATE | MAP_POPULATE,
                                       pQueryFileInfo->dataFd, pQueryFileInfo->dtFileMappingOffset);
  if (pQueryFileInfo->pDataFileData == MAP_FAILED) {
    dError("failed to mmaping data file:%s, handle:%d, offset:%ld, reason:%s", pQueryFileInfo->dataFilePath,
           pQueryFileInfo->dataFd, pQueryFileInfo->dtFileMappingOffset, strerror(errno));
    return -1;
  }

  /* advise kernel the usage of mmaped data */
  if (madvise(pQueryFileInfo->pDataFileData, pQueryFileInfo->defaultMappingSize, MADV_SEQUENTIAL) == -1) {
    dError("failed to advise kernel the usage of data file:%s, handle:%d, reason:%s",
        pQueryFileInfo->dataFilePath, pQueryFileInfo->dataFd, strerror(errno));
  }

  return 0;
}

static int32_t copyDataFromMMapBuffer(int fd, SQInfo *pQInfo, SQueryFileInfo *pQueryFile, char *buf, uint64_t offset,
                                      int32_t size) {
  assert(size >= 0);

  int32_t ret = moveMMapWindow(pQueryFile, offset);
  dTrace("QInfo:%p finished move to correct position:%ld", pQInfo, taosGetTimestampUs());

  if (pQueryFile->pDataFileData == MAP_FAILED || ret != TSDB_CODE_SUCCESS) {
    dTrace("QInfo:%p move window failed. ret:%d", pQInfo, ret);
    return -1;
  }

  uint64_t upperBnd = pQueryFile->dtFileMappingOffset + pQueryFile->defaultMappingSize - 1;

  /* data are enclosed in current mmap window */
  if (offset + size <= upperBnd) {
    uint64_t startPos = offset - pQueryFile->dtFileMappingOffset;
    memcpy(buf, pQueryFile->pDataFileData + startPos, size);

    dTrace("QInfo:%p copy data completed, size:%d, time:%ld", pQInfo, size, taosGetTimestampUs());

  } else {
    uint32_t firstPart = upperBnd - offset + 1;
    memcpy(buf, pQueryFile->pDataFileData + (offset - pQueryFile->dtFileMappingOffset), firstPart);

    dTrace("QInfo:%p copy data first part,size:%d, time:%ld", pQInfo, firstPart, taosGetTimestampUs());

    char *dst = buf + firstPart;

    /* remain data */
    uint32_t remain = size - firstPart;
    while (remain > 0) {
      int32_t ret1 = moveMMapWindow(pQueryFile, pQueryFile->dtFileMappingOffset + pQueryFile->defaultMappingSize);
      if (ret1 != 0) {
        return ret1;
      }

      uint32_t len = (remain > pQueryFile->defaultMappingSize) ? pQueryFile->defaultMappingSize : remain;

      /* start from the 0 position */
      memcpy(dst, pQueryFile->pDataFileData, len);
      remain -= len;
      dst += len;

      dTrace("QInfo:%p copy data part,size:%d, time:%ld", pQInfo, len, taosGetTimestampUs());
    }
  }

  return 0;
}

static int32_t readDataFromDiskFile(int fd, SQInfo *pQInfo, SQueryFileInfo *pQueryFile, char *buf, uint64_t offset,
                                    int32_t size) {
  assert(size >= 0);

  int32_t ret = (int32_t)lseek(fd, offset, SEEK_SET);
  if (ret == -1) {
    //        qTrace("QInfo:%p seek failed, reason:%s", pQInfo, strerror(errno));
    return -1;
  }

  ret = read(fd, buf, size);
  //    qTrace("QInfo:%p read data %d completed", pQInfo, size);
  return 0;
}

static int32_t loadColumnIntoMem(SQuery *pQuery, SQueryFileInfo *pQueryFileInfo, SCompBlock *pBlock, SField *pFields,
                                 int32_t col, SData *sdata, void *tmpBuf, char *buffer, int32_t buffersize) {
  char *dst = (pBlock->algorithm) ? tmpBuf : sdata->data;

  int64_t offset = pBlock->offset + pFields[col].offset;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  int     fd = pBlock->last ? pQueryFileInfo->lastFd : pQueryFileInfo->dataFd;
  int32_t ret = (*readDataFunctor[DEFAULT_IO_ENGINE])(fd, pQInfo, pQueryFileInfo, dst, offset, pFields[col].len);
  if (ret != 0) {
    return ret;
  }

  // load checksum
  TSCKSUM checksum = 0;
  ret = (*readDataFunctor[DEFAULT_IO_ENGINE])(fd, pQInfo, pQueryFileInfo, (char *)&checksum, offset + pFields[col].len,
                                              sizeof(TSCKSUM));
  if (ret != 0) {
    return ret;
  }

  // check column data integrity
  if (checksum != taosCalcChecksum(0, (const uint8_t *)dst, pFields[col].len)) {
    dLError("QInfo:%p, column data checksum error, file:%s, col: %d, offset:%ld", GET_QINFO_ADDR(pQuery),
            pQueryFileInfo->dataFilePath, col, offset);

    return -1;
  }

  if (pBlock->algorithm) {
    (*pDecompFunc[pFields[col].type])(tmpBuf, pFields[col].len, pBlock->numOfPoints, sdata->data,
                                      pFields[col].bytes*pBlock->numOfPoints, pBlock->algorithm, buffer, buffersize);
  }

  return 0;
}

static int32_t loadDataBlockFieldsInfo(SQueryRuntimeEnv *pRuntimeEnv, SQueryFileInfo *pQueryFileInfo,
                                       SCompBlock *pBlock, SField **pField) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SQInfo *   pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  size_t size = sizeof(SField) * (pBlock->numOfCols) + sizeof(TSCKSUM);

  // if *pField != NULL, this block is loaded once, in current query do nothing
  if (*pField == NULL) {  // load the fields information once
    *pField = malloc(size);
  }

  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;
  pSummary->totalFieldSize += size;
  pSummary->readField++;
  pSummary->numOfSeek++;

  int64_t st = taosGetTimestampUs();

  int     fd = pBlock->last ? pQueryFileInfo->lastFd : pQueryFileInfo->dataFd;
  int32_t ret =
      (*readDataFunctor[DEFAULT_IO_ENGINE])(fd, pQInfo, pQueryFileInfo, (char *)(*pField), pBlock->offset, size);
  if (ret != 0) {
    return ret;
  }

  // check fields integrity
  if (!taosCheckChecksumWhole((uint8_t *)(*pField), size)) {
    dLError("QInfo:%p vid:%d sid:%d id:%s, slot:%d, failed to read sfields, file:%s, sfields area broken:%lld",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, pQueryFileInfo->dataFilePath,
        pBlock->offset);
    return -1;
  }

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load field info, size:%d, elapsed:%f ms",
      pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, size, (et - st) / 1000.0);

  pSummary->loadFieldUs += (et - st);
  return 0;
}

static void fillWithNull(SQuery *pQuery, char *dst, int32_t col, int32_t numOfPoints) {
  int32_t bytes = pQuery->colList[col].data.bytes;
  int32_t type = pQuery->colList[col].data.type;

  setNullN(dst, type, bytes, numOfPoints);
}

static int32_t loadDataBlockIntoMem(SCompBlock *pBlock, SField **pField, SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIdx,
                                    bool loadPrimaryCol, bool loadSField) {
  int32_t i = 0, j = 0;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  SData **   sdata = pRuntimeEnv->colDataBuffer;

  SQueryFileInfo *pQueryFileInfo = &pRuntimeEnv->pHeaderFiles[fileIdx];
  SData **        primaryTSBuf = &pRuntimeEnv->primaryColBuffer;
  void *          tmpBuf = pRuntimeEnv->unzipBuffer;

  if (vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, fileIdx)) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, data block has been loaded, ts:%d, slot:%d, brange:%lld-%lld, rows:%d",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, loadPrimaryCol, pQuery->slot,
        pBlock->keyFirst, pBlock->keyLast, pBlock->numOfPoints);

    return 0;
  }

  /* failed to load fields info, return with error info */
  if (loadSField && (loadDataBlockFieldsInfo(pRuntimeEnv, pQueryFileInfo, pBlock, pField) != 0)) {
    return -1;
  }

  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;
  int32_t               columnBytes = 0;

  int64_t st = taosGetTimestampUs();

  if (loadPrimaryCol) {
    if (PRIMARY_TSCOL_LOADED(pQuery)) {
      *primaryTSBuf = sdata[0];
    } else {
      columnBytes += (*pField)[PRIMARYKEY_TIMESTAMP_COL_INDEX].len + sizeof(TSCKSUM);
      int32_t ret = loadColumnIntoMem(pQuery, pQueryFileInfo, pBlock, *pField, PRIMARYKEY_TIMESTAMP_COL_INDEX,
                                      *primaryTSBuf, tmpBuf, pRuntimeEnv->secondaryUnzipBuffer,
                                      pRuntimeEnv->internalBufSize);
      if (ret != 0) {
        return -1;
      }

      pSummary->numOfSeek++;
      j += 1;  // first column of timestamp is not needed to be read again
    }
  }

  int32_t ret = 0;

  /* the first round always be 1, the secondary round is determined by queried
   * function */
  int32_t round = pRuntimeEnv->scanFlag;

  while (j < pBlock->numOfCols && i < pQuery->numOfCols) {
    if ((*pField)[j].colId < pQuery->colList[i].data.colId) {
      ++j;
    } else if ((*pField)[j].colId == pQuery->colList[i].data.colId) {
      /*
       * during supplementary scan:
       * 1. primary ts column (always loaded)
       * 2. query specified columns
       * 3. in case of filter column required, filter columns must be loaded.
       */
      if (pQuery->colList[i].req[round] == 1 || pQuery->colList[i].data.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        /* if data of this column in current block are all null, do NOT read it from disk */
        if ((*pField)[j].numOfNullPoints == pBlock->numOfPoints) {
          fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);
        } else {
          columnBytes += (*pField)[j].len + sizeof(TSCKSUM);
          ret = loadColumnIntoMem(pQuery, pQueryFileInfo, pBlock, *pField, j, sdata[i], tmpBuf,
                                  pRuntimeEnv->secondaryUnzipBuffer, pRuntimeEnv->internalBufSize);

          pSummary->numOfSeek++;
        }
      }
      ++i;
      ++j;
    } else {
      /*
       * pQuery->colList[i].colIdx < (*pFields)[j].colId this column is not existed in current block, fill with NULL value
       */
      fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);

      pSummary->totalGenData += (pBlock->numOfPoints * pQuery->colList[i].data.bytes);
      ++i;
    }
  }

  if (j >= pBlock->numOfCols && i < pQuery->numOfCols) {
    // remain columns need to set null value
    while (i < pQuery->numOfCols) {
      fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);

      pSummary->totalGenData += (pBlock->numOfPoints * pQuery->colList[i].data.bytes);
      ++i;
    }
  }

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load block completed, ts loaded:%d, rec:%d, elapsed:%f ms",
      GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, loadPrimaryCol,
      pBlock->numOfPoints, (et - st) / 1000.0);

  pSummary->totalBlockSize += columnBytes;
  pSummary->loadBlocksUs += (et - st);
  pSummary->readDiskBlocks++;

  vnodeSetDataBlockInfoLoaded(pRuntimeEnv, pMeterObj, fileIdx);
  return ret;
}

// todo ignore the blockType, pass the pQuery into this function
SBlockInfo getBlockBasicInfo(void *pBlock, int32_t blockType) {
  SBlockInfo blockInfo = {0};
  if (IS_FILE_BLOCK(blockType)) {
    SCompBlock *pDiskBlock = (SCompBlock *)pBlock;

    blockInfo.keyFirst = pDiskBlock->keyFirst;
    blockInfo.keyLast = pDiskBlock->keyLast;
    blockInfo.size = pDiskBlock->numOfPoints;
    blockInfo.numOfCols = pDiskBlock->numOfCols;
  } else {
    SCacheBlock *pCacheBlock = (SCacheBlock *)pBlock;

    blockInfo.keyFirst = getTimestampInCacheBlock(pCacheBlock, 0);
    blockInfo.keyLast = getTimestampInCacheBlock(pCacheBlock, pCacheBlock->numOfPoints - 1);
    blockInfo.size = pCacheBlock->numOfPoints;
    blockInfo.numOfCols = pCacheBlock->pMeterObj->numOfColumns;
  }

  return blockInfo;
}

static bool checkQueryRangeAgainstNextBlock(SBlockInfo *pBlockInfo, SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if ((QUERY_IS_ASC_QUERY(pQuery) && pBlockInfo->keyFirst > pQuery->ekey) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && pBlockInfo->keyLast < pQuery->ekey)) {
    int32_t pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pBlockInfo->size - 1;

    savePointPosition(&pRuntimeEnv->nextPos, pQuery->fileId, pQuery->slot, pos);
    setQueryStatus(pQuery, QUERY_COMPLETED);

    return false;
  }

  return true;
}

/**
 *
 * @param pQuery
 * @param pBlockInfo
 * @param forwardStep
 * @return  TRUE means query not completed, FALSE means query is completed
 */
static bool queryCompleteInBlock(SQuery *pQuery, SBlockInfo *pBlockInfo, int32_t forwardStep) {
  if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    assert(pQuery->checkBufferInLoop == 1 && pQuery->over == QUERY_RESBUF_FULL && pQuery->pointsOffset == 0);

    assert((QUERY_IS_ASC_QUERY(pQuery) && forwardStep + pQuery->pos <= pBlockInfo->size) ||
           (!QUERY_IS_ASC_QUERY(pQuery) && pQuery->pos - forwardStep + 1 >= 0));

    // current query completed
    if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
    }

    return true;
  } else {  // query completed
    if ((pQuery->ekey <= pBlockInfo->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pQuery->ekey >= pBlockInfo->keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return true;
    }

    return false;
  }
}

/**
 * save triple tuple of (fileId, slot, pos) to SPositionInfo
 */
void savePointPosition(SPositionInfo *position, int32_t fileId, int32_t slot, int32_t pos) {
  /*
   * slot == -1 && pos == -1 means no data left anymore
   */
  assert(fileId >= -1 && slot >= -1 && pos >= -1);

  position->fileId = fileId;
  position->slot = slot;
  position->pos = pos;
}

static FORCE_INLINE void saveNextAccessPositionInCache(SPositionInfo *position, int32_t slotIdx, int32_t pos) {
  savePointPosition(position, -1, slotIdx, pos);
}

// todo all functions that call this function should check the returned data
// blocks status
SCacheBlock *getCacheDataBlock(SMeterObj *pMeterObj, SQuery *pQuery, int32_t slot) {
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || slot < 0) {
    return NULL;
  }

  assert(slot < pCacheInfo->maxBlocks);

  SCacheBlock *pBlock = pCacheInfo->cacheBlocks[slot];
  if (pBlock == NULL) {
    dError("QInfo:%p NULL Block In Cache, available block:%d, last block:%d, accessed null block:%d, pBlockId:%d",
        GET_QINFO_ADDR(pQuery), pCacheInfo->numOfBlocks, pCacheInfo->currentSlot, slot, pQuery->blockId);
    return NULL;
  }

  if (pMeterObj != pBlock->pMeterObj || pBlock->blockId > pQuery->blockId || pBlock->numOfPoints <= 0) {
    dWarn("QInfo:%p vid:%d sid:%d id:%s, cache block is overwritten, slot:%d blockId:%d qBlockId:%d",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, pBlock->blockId,
        pQuery->blockId);
    return NULL;
  }

  return pBlock;
}

static SCompBlock *getDiskDataBlock(SQuery *pQuery, int32_t slot) {
  assert(pQuery->fileId >= 0 && slot >= 0 && slot < pQuery->numOfBlocks && pQuery->pBlock != NULL);
  return &pQuery->pBlock[slot];
}

static void *getGenericDataBlock(SMeterObj *pMeterObj, SQuery *pQuery, int32_t slot) {
  if (IS_DISK_DATA_BLOCK(pQuery)) {
    return getDiskDataBlock(pQuery, slot);
  } else {
    return getCacheDataBlock(pMeterObj, pQuery, slot);
  }
}

static int32_t getFileIdFromKey(int32_t vid, TSKEY key) {
  SVnodeObj *pVnode = &vnodeList[vid];
  int64_t    delta = (int64_t)pVnode->cfg.daysPerFile * tsMsPerDay[pVnode->cfg.precision];

  return (int32_t)(key / delta);  // set the starting fileId
}

enum {
  QUERY_RANGE_LESS_EQUAL = 0,
  QUERY_RANGE_GREATER_EQUAL = 1,
};

static bool getQualifiedDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t type,
                                  __block_search_fn_t searchFn) {
  int32_t blkIdx = -1;
  int32_t fid = -1;
  int32_t step = (type == QUERY_RANGE_GREATER_EQUAL) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->slot = -1;

  TSKEY key = pQuery->lastKey;

  SData *primaryColBuffer = pRuntimeEnv->primaryColBuffer;
  pQuery->fileId = getFileIdFromKey(pMeterObj->vnode, key) - step;

  while (1) {
    if ((fid = getNextDataFileCompInfo(pRuntimeEnv, pMeterObj, step)) < 0) {
      break;
    }

    blkIdx = binarySearchForBlock(pQuery, key);

    if (type == QUERY_RANGE_GREATER_EQUAL) {
      if (key <= pQuery->pBlock[blkIdx].keyLast) {
        break;
      } else {
        blkIdx = -1;
      }
    } else {
      if (key >= pQuery->pBlock[blkIdx].keyFirst) {
        break;
      } else {
        blkIdx = -1;
      }
    }
  }

  /* failed to find qualified point in file, abort */
  if (blkIdx == -1) {
    return false;
  }

  assert(blkIdx >= 0 && blkIdx < pQuery->numOfBlocks);

  // load first data block into memory failed, caused by disk block error
  bool blockLoaded = false;
  while (blkIdx < pQuery->numOfBlocks) {
    pQuery->slot = blkIdx;
    if (loadDataBlockIntoMem(&pQuery->pBlock[pQuery->slot], &pQuery->pFields[pQuery->slot], pRuntimeEnv, fid, true,
                             true) == 0) {
      SET_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus);
      blockLoaded = true;
      break;
    }

    dError("QInfo:%p fileId:%d total numOfBlks:%d blockId:%d into memory failed due to error in disk files",
        GET_QINFO_ADDR(pQuery), pQuery->fileId, pQuery->numOfBlocks, blkIdx);
    blkIdx += step;
  }

  // failed to load data from disk, abort current query
  if (blockLoaded == false) {
    return false;
  }

  SCompBlock *pBlocks = getDiskDataBlock(pQuery, blkIdx);

  /*
   * search qualified points in blk, according to primary key (timestamp) column
   */
  pQuery->pos = searchFn(primaryColBuffer->data, pBlocks->numOfPoints, key, pQuery->order.order);
  assert(pQuery->pos >= 0 && pQuery->fileId >= 0 && pQuery->slot >= 0);

  return true;
}

static char *doGetDataBlockImpl(const char *sdata, int32_t colIdx, bool isDiskFileBlock) {
  if (isDiskFileBlock) {
    return ((SData **)sdata)[colIdx]->data;
  } else {
    return ((SCacheBlock *)sdata)->offset[colIdx];
  }
}

static SField *getFieldInfo(SQuery *pQuery, SBlockInfo *pBlockInfo, SField *pFields, int32_t column) {
  // no SField info exist, or column index larger than the output column, no result.
  if (pFields == NULL || column >= pQuery->numOfOutputCols) {
    return NULL;
  }

  SColIndexEx *pColIndexEx = &pQuery->pSelectExpr[column].pBase.colInfo;

  // for a tag column, no corresponding field info
  if (pColIndexEx->isTag) {
    return NULL;
  }

  /*
   * Choose the right column field info by field id, since the file block may be out of date,
   * which means the newest table schema is not equalled to the schema of this block.
   */
  for (int32_t i = 0; i < pBlockInfo->numOfCols; ++i) {
    if (pColIndexEx->colId == pFields[i].colId) {
      return &pFields[i];
    }
  }

  return NULL;
}

/*
 * not null data in two cases:
 * 1. tags data: isTag == true;
 * 2. data locate in file, numOfNullPoints == 0 or pFields does not needed to be loaded
 */
static bool hasNullVal(SQuery *pQuery, int32_t col, SBlockInfo *pBlockInfo, SField *pFields, bool isDiskFileBlock) {
  bool ret = true;

  if (pQuery->pSelectExpr[col].pBase.colInfo.isTag) {
    ret = false;
  } else if (isDiskFileBlock) {
    if (pFields == NULL) {
      ret = false;
    } else {
      SField *pField = getFieldInfo(pQuery, pBlockInfo, pFields, col);
      if (pField != NULL && pField->numOfNullPoints == 0) {
        ret = false;
      }
    }
  }

  return ret;
}

static char *doGetDataBlocks(bool isDiskFileBlock, SQueryRuntimeEnv *pRuntimeEnv, char *data, int32_t colIdx,
                             int32_t colId, int16_t type, int16_t bytes, int32_t tmpBufIndex) {
  char *pData = NULL;

  if (isDiskFileBlock) {
    pData = doGetDataBlockImpl(data, colIdx, isDiskFileBlock);
  } else {
    SCacheBlock* pCacheBlock = (SCacheBlock*) data;
    SMeterObj* pMeter = pRuntimeEnv->pMeterObj;

    if (colIdx < 0 || pMeter->numOfColumns <= colIdx || pMeter->schema[colIdx].colId != colId) {
      /* data in cache is not current available, we need fill the data block in null value */
      pData = pRuntimeEnv->colDataBuffer[tmpBufIndex]->data;
      setNullN(pData, type, bytes, pCacheBlock->numOfPoints);
    } else {
      pData = doGetDataBlockImpl(data, colIdx, isDiskFileBlock);
    }
  }

  return pData;
}

static char *getDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, char *data, SArithmeticSupport *sas, int32_t col,
                           bool isDiskFileBlock) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  char *dataBlock = NULL;

  int32_t functionId = pQuery->pSelectExpr[col].pBase.functionId;

  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pExpr = &pQuery->pSelectExpr[col];

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pCtx->startOffset = pQuery->pos;  // set the offset value
    } else {
      pCtx->startOffset = pQuery->pos - pCtx->size + 1;
    }

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      int32_t colIdx = isDiskFileBlock ? pQuery->colList[i].colIdxInBuf : pQuery->colList[i].colIdx;

      SColumnFilterMsg *pColMsg = &pQuery->colList[i].data;
      char *pData = doGetDataBlocks(isDiskFileBlock, pRuntimeEnv, data, colIdx, pColMsg->colId, pColMsg->type,
                                    pColMsg->bytes, pQuery->colList[i].colIdxInBuf);

      sas->elemSize[i] = pColMsg->bytes;
      sas->data[i] = pData + pCtx->startOffset * sas->elemSize[i];  // start from the offset
    }
    sas->numOfCols = pQuery->numOfCols;
    sas->offset = 0;
  } else {  // other type of query function
    SColIndexEx *pCol = &pQuery->pSelectExpr[col].pBase.colInfo;
    int32_t      colIdx = isDiskFileBlock ? pCol->colIdxInBuf : pCol->colIdx;

    if (pCol->isTag) {
      dataBlock = NULL;
    } else {
      /*
       *  the colIdx is acquired from the first meter of all qualified meters in this vnode during query prepare stage,
       *  the remain meter may not have the required column in cache actually.
       *  So, the validation of required column in cache with the corresponding meter schema is reinforced.
       */
      dataBlock = doGetDataBlocks(isDiskFileBlock, pRuntimeEnv, data, colIdx, pCol->colId, pCtx[col].inputType,
                                  pCtx[col].inputBytes, pCol->colIdxInBuf);
    }
  }

  return dataBlock;
}

/**
 *
 * @param pRuntimeEnv
 * @param forwardStep
 * @param primaryKeyCol
 * @param data
 * @param pFields
 * @param isDiskFileBlock
 * @return                  the incremental number of output value, so it maybe 0 for fixed number of query,
 *                          such as count/min/max etc.
 */
static int32_t applyAllFunctions(SQueryRuntimeEnv *pRuntimeEnv, int32_t forwardStep, TSKEY *primaryKeyCol, char *data,
                                 SField *pFields, SBlockInfo *pBlockInfo, bool isDiskFileBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;

  int64_t prevNumOfRes = getNumOfResult(pRuntimeEnv);

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    if (pRuntimeEnv->go[k]) {
      SField             dummyField = {0};
      SArithmeticSupport sas = {0};

      // todo refactor
      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

      if (!IS_MASTER_SCAN(pRuntimeEnv) &&
          !(functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_FIRST ||
            functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS)) {
        continue;
      }

      bool  hasNull = hasNullVal(pQuery, k, pBlockInfo, pFields, isDiskFileBlock);
      char *dataBlock = getDataBlocks(pRuntimeEnv, data, &sas, k, isDiskFileBlock);

      SField *tpField = NULL;

      if (pFields != NULL) {
        tpField = getFieldInfo(pQuery, pBlockInfo, pFields, k);
        /*
         * Field info not exist, the required column is not present in current block,
         * so all data must be null value in current block.
         */
        if (tpField == NULL) {
          tpField = &dummyField;
          tpField->numOfNullPoints = (int32_t)forwardStep;
        }
      }

      TSKEY ts = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->skey : pQuery->ekey;

      int64_t alignedTimestamp = taosGetIntervalStartTimestamp(ts, pQuery->nAggTimeInterval, pQuery->intervalTimeUnit);
      setExecParams(pQuery, &pCtx[k], alignedTimestamp, dataBlock, (char *)primaryKeyCol, forwardStep, functionId,
                    tpField, hasNull, pRuntimeEnv->blockStatus, &sas, pRuntimeEnv->scanFlag);

      pRuntimeEnv->go[k] = aAggs[functionId].xFunction(&pCtx[k]);
    }
  }

  int64_t numOfIncrementRes = getNumOfResult(pRuntimeEnv) - prevNumOfRes;
  validateTimestampForSupplementResult(pRuntimeEnv, numOfIncrementRes);

  return (int32_t)numOfIncrementRes;
}

/**
 * if sfields is null
 * 1. count(*)/spread(ts) is invoked
 * 2. this column does not exists
 *
 * first filter the data block according to the value filter condition, then, if
 * the top/bottom query applied, invoke the filter function to decide if the data block need to be accessed or not.
 * @param pQuery
 * @param pField
 * @return
 */
static bool needToLoadDataBlock(SQuery *pQuery, SField *pField, SQLFunctionCtx *pCtx) {
  if (pField == NULL) {
    return false;  // no need to load data
  }

  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    int32_t            colIndex = pFilterInfo->pFilter.colIdx;

    // this column not valid in current data block
    if (colIndex < 0 || pField[colIndex].colId != pFilterInfo->pFilter.data.colId) {
      continue;
    }

    // not support pre-filter operation on binary/nchar data type
    if (!vnodeSupportPrefilter(pFilterInfo->pFilter.data.type)) {
      continue;
    }

    if (pFilterInfo->pFilter.data.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = *(double *)(&pField[colIndex].min);
      float maxval = *(double *)(&pField[colIndex].max);

      if (!pFilterInfo->fp(&pFilterInfo->pFilter, (char *)&minval, (char *)&maxval)) {
        return false;
      }
    } else {
      if (!pFilterInfo->fp(&pFilterInfo->pFilter, (char *)&pField[colIndex].min, (char *)&pField[colIndex].max)) {
        return false;
      }
    }
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_TOP_DST || functId == TSDB_FUNC_BOTTOM ||
        functId == TSDB_FUNC_BOTTOM_DST) {
      return top_bot_datablock_filter(&pCtx[i], functId, (char *)&pField[i].min, (char *)&pField[i].max);
    }
  }

  return true;
}

static int32_t applyAllFunctions_Filter(SQueryRuntimeEnv *pRuntimeEnv, int32_t *forwardStep, TSKEY *primaryKeyCol,
                                        char *data, SField *pFields, SBlockInfo *pBlockInfo, bool isDiskFileBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;

  int64_t prevNumOfRes = getNumOfResult(pRuntimeEnv);

  SArithmeticSupport *sasArray = calloc(pQuery->numOfOutputCols, sizeof(SArithmeticSupport));

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

    bool  hasNull = hasNullVal(pQuery, k, pBlockInfo, pFields, isDiskFileBlock);
    char *dataBlock = getDataBlocks(pRuntimeEnv, data, &sasArray[k], k, isDiskFileBlock);

    TSKEY   ts = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->skey : pQuery->ekey;
    int64_t alignedTimestamp = taosGetIntervalStartTimestamp(ts, pQuery->nAggTimeInterval, pQuery->intervalTimeUnit);

    setExecParams(pQuery, &pCtx[k], alignedTimestamp, dataBlock, (char *)primaryKeyCol, (*forwardStep), functionId,
                  pFields, hasNull, pRuntimeEnv->blockStatus, &sasArray[k], pRuntimeEnv->scanFlag);
  }

  // set the input column data
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    int32_t            colIdx = isDiskFileBlock ? pFilterInfo->pFilter.colIdxInBuf : pFilterInfo->pFilter.colIdx;
    SColumnFilterMsg * pFilterMsg = &pFilterInfo->pFilter.data;
    /* NOTE: here the tbname/tags column cannot reach here, so we do NOT check if is a tag or not */
    pFilterInfo->pData = doGetDataBlocks(isDiskFileBlock, pRuntimeEnv, data, colIdx, pFilterMsg->colId,
                                         pFilterMsg->type, pFilterMsg->bytes, pFilterInfo->pFilter.colIdxInBuf);
  }

  int32_t numOfRes = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // from top to bottom in desc
  // from bottom to top in asc order
  for (int32_t j = 0; j < (*forwardStep); ++j) {
    if (!vnodeDoFilterData(pQuery, pQuery->pos + j * step)) {
      continue;
    }

    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      if (pRuntimeEnv->go[k]) {
        int32_t offset = pQuery->pos + j * step;
        offset -= pCtx[k].startOffset;

        pRuntimeEnv->go[k] = aAggs[pQuery->pSelectExpr[k].pBase.functionId].xFunctionF(&pCtx[k], offset);
      }
    }

    /*
     * pointsOffset is the maximum available space in result buffer
     * update the actual forward step for query that requires checking buffer during loop
     */
    if ((pQuery->checkBufferInLoop == 1) && (++numOfRes) >= pQuery->pointsOffset) {
      pQuery->lastKey = primaryKeyCol[pQuery->pos + j * step] + step;
      *forwardStep = j + 1;
      break;
    }
  }

  free(sasArray);

  return getNumOfResult(pRuntimeEnv) - prevNumOfRes;
}

static int32_t getForwardStepsInBlock(int32_t numOfPoints, __block_search_fn_t searchFn, SQuery *pQuery,
                                      int64_t *pData) {
  int32_t endPos = searchFn((char *)pData, numOfPoints, pQuery->ekey, pQuery->order.order);
  int32_t forwardStep = 0;

  if (endPos >= 0) {
    forwardStep = QUERY_IS_ASC_QUERY(pQuery) ? (endPos - pQuery->pos) : (pQuery->pos - endPos);
    assert(forwardStep >= 0);

    /* endPos data is equalled to the key so, we do need to read the element in
     * endPos */
    if (pData[endPos] == pQuery->ekey) {
      forwardStep += 1;
    }
  }
  return forwardStep;
}

static int32_t reviseForwardSteps(SQuery *pQuery, int32_t forwardStep) {
  /*
   * if value filter exists, we try all data in current block, and do not set the QUERY_RESBUF_FULL flag.
   *
   * in handing of top/bottom query, the checkBufferInLoop == 1 and pQuery->numOfFilterCols may be 0 or not,
   * so we have to exclude the query of top/bottom from checking for buffer status.
   */
  if (pQuery->checkBufferInLoop == 1 && pQuery->numOfFilterCols == 0 && !isTopBottomQuery(pQuery)) {
    /* current buffer does not have enough space, try in the next loop */
    if (pQuery->pointsOffset <= forwardStep) {
      forwardStep = pQuery->pointsOffset;
    }
  }
  return forwardStep;
}

static void validateQueryRangeAndData(SQueryRuntimeEnv *pRuntimeEnv, const TSKEY *pPrimaryColumn,
                                      SBlockInfo *pBlockBasicInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  TSKEY startKey = -1;
  // timestamp qualification check
  if (IS_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus) && needPrimaryTimestampCol(pQuery, pBlockBasicInfo)) {
    startKey = pPrimaryColumn[pQuery->pos];
  } else {
    startKey = pBlockBasicInfo->keyFirst;
    TSKEY endKey = pBlockBasicInfo->keyLast;

    assert((endKey <= pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
           (endKey >= pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery)));
  }

  assert((startKey >= pQuery->lastKey && startKey <= pQuery->ekey && pQuery->skey <= pQuery->lastKey &&
          QUERY_IS_ASC_QUERY(pQuery)) ||
         (startKey <= pQuery->lastKey && startKey >= pQuery->ekey && pQuery->skey >= pQuery->lastKey &&
          !QUERY_IS_ASC_QUERY(pQuery)));
}

static int32_t applyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SBlockInfo *pBlockInfo, int64_t *pPrimaryColumn,
                                     char *sdata, SField *pFields, __block_search_fn_t searchFn, int32_t *numOfRes) {
  int32_t forwardStep = 0;
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  validateQueryRangeAndData(pRuntimeEnv, pPrimaryColumn, pBlockInfo);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (pQuery->ekey < pBlockInfo->keyLast) {
      forwardStep = getForwardStepsInBlock(pBlockInfo->size, searchFn, pQuery, pPrimaryColumn);
      assert(forwardStep >= 0);

      if (forwardStep == 0) {
        /* no qualified data in current block, do not update the lastKey value
         */
        assert(pQuery->ekey < pPrimaryColumn[pQuery->pos]);
      } else {
        pQuery->lastKey = pPrimaryColumn[pQuery->pos + (forwardStep - 1)] + step;
      }

    } else {
      forwardStep = pBlockInfo->size - pQuery->pos;
      assert(forwardStep > 0);

      pQuery->lastKey = pBlockInfo->keyLast + step;
    }
  } else {  // desc
    if (pQuery->ekey > pBlockInfo->keyFirst) {
      forwardStep = getForwardStepsInBlock(pBlockInfo->size, searchFn, pQuery, pPrimaryColumn);
      assert(forwardStep >= 0);

      if (forwardStep == 0) {
        /* no qualified data in current block, do not update the lastKey value
         */
        assert(pQuery->ekey > pPrimaryColumn[pQuery->pos]);
      } else {
        pQuery->lastKey = pPrimaryColumn[pQuery->pos - (forwardStep - 1)] + step;
      }
    } else {
      forwardStep = pQuery->pos + 1;
      assert(forwardStep > 0);

      pQuery->lastKey = pBlockInfo->keyFirst + step;
    }
  }

  int32_t newForwardStep = reviseForwardSteps(pQuery, forwardStep);
  assert(newForwardStep <= forwardStep && newForwardStep >= 0);

  /* if buffer limitation is applied, there must be primary column(timestamp) loaded */
  if (newForwardStep < forwardStep && newForwardStep > 0) {
    pQuery->lastKey = pPrimaryColumn[pQuery->pos + (newForwardStep - 1) * step] + step;
  }

  bool isFileBlock = IS_FILE_BLOCK(pRuntimeEnv->blockStatus);
  if (pQuery->numOfFilterCols > 0) {
    *numOfRes =
        applyAllFunctions_Filter(pRuntimeEnv, &newForwardStep, pPrimaryColumn, sdata, pFields, pBlockInfo, isFileBlock);
  } else {
    *numOfRes = applyAllFunctions(pRuntimeEnv, newForwardStep, pPrimaryColumn, sdata, pFields, pBlockInfo, isFileBlock);
  }

  assert(*numOfRes >= 0);

  // check if buffer is large enough for accommodating all qualified points
  if (*numOfRes > 0 && pQuery->checkBufferInLoop == 1) {
    pQuery->pointsOffset -= *numOfRes;
    if (pQuery->pointsOffset == 0) {
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    }
  }

  return newForwardStep;
}

int32_t vnodeGetVnodeHeaderFileIdx(int32_t *fid, SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  if (pRuntimeEnv->numOfFiles == 0) {
    return -1;
  }

  /* set the initial file for current query */
  if (order == TSQL_SO_ASC && *fid < pRuntimeEnv->pHeaderFiles[0].fileID) {
    *fid = pRuntimeEnv->pHeaderFiles[0].fileID;
    return 0;
  } else if (order == TSQL_SO_DESC && *fid > pRuntimeEnv->pHeaderFiles[pRuntimeEnv->numOfFiles - 1].fileID) {
    *fid = pRuntimeEnv->pHeaderFiles[pRuntimeEnv->numOfFiles - 1].fileID;
    return pRuntimeEnv->numOfFiles - 1;
  }

  int32_t numOfFiles = pRuntimeEnv->numOfFiles;

  if (order == TSQL_SO_DESC && *fid > pRuntimeEnv->pHeaderFiles[numOfFiles - 1].fileID) {
    *fid = pRuntimeEnv->pHeaderFiles[numOfFiles - 1].fileID;
    return numOfFiles - 1;
  }

  if (order == TSQL_SO_ASC) {
    int32_t i = 0;
    int32_t step = 1;

    while(i < numOfFiles && *fid > pRuntimeEnv->pHeaderFiles[i].fileID) {
      i += step;
    }

    if (i < numOfFiles && *fid <= pRuntimeEnv->pHeaderFiles[i].fileID) {
      *fid = pRuntimeEnv->pHeaderFiles[i].fileID;
      return i;
    } else {
      return -1;
    }
  } else {
    int32_t i = numOfFiles - 1;
    int32_t step = -1;

    while(i >= 0 && *fid < pRuntimeEnv->pHeaderFiles[i].fileID) {
      i += step;
    }

    if (i >= 0 && *fid >= pRuntimeEnv->pHeaderFiles[i].fileID) {
      *fid = pRuntimeEnv->pHeaderFiles[i].fileID;
      return i;
    } else {
      return -1;
    }
  }
}

int32_t getNextDataFileCompInfo(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t step) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->fileId += step;

  int32_t fid = 0;
  int32_t order = (step == QUERY_ASC_FORWARD_STEP) ? TSQL_SO_ASC : TSQL_SO_DESC;
  while (1) {
    fid = vnodeGetVnodeHeaderFileIdx(&pQuery->fileId, pRuntimeEnv, order);

    // no files left, abort
    if (fid < 0) {
      if (step == QUERY_ASC_FORWARD_STEP) {
        dTrace("QInfo:%p no file to access, try data in cache", GET_QINFO_ADDR(pQuery));
      } else {
        dTrace("QInfo:%p no file to access in desc order, query completed", GET_QINFO_ADDR(pQuery));
      }

      vnodeFreeFieldsEx(pRuntimeEnv);
      pQuery->fileId = -1;
      break;
    }

    if (vnodeGetCompBlockInfo(pMeterObj, pRuntimeEnv, fid) > 0) {
      break;
    }

    /*
     * 1. failed to read blk information from header file or open data file failed
     * 2. header file is empty
     *
     * try next one
     */
    pQuery->fileId += step;

    /* for backwards search, if the first file is not valid, abort */
    if (step < 0 && fid == 0) {
      vnodeFreeFieldsEx(pRuntimeEnv);
      pQuery->fileId = -1;
      fid = -1;
      break;
    }
  }

  return fid;
}

void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, int64_t StartQueryTimestamp, void *inputData,
                   char *primaryColumnData, int32_t size, int32_t functionId, SField *pField, bool hasNull,
                   int32_t blockStatus, void *param, int32_t scanFlag) {
  int32_t startOffset = (QUERY_IS_ASC_QUERY(pQuery)) ? pQuery->pos : pQuery->pos - (size - 1);

  pCtx->nStartQueryTimestamp = StartQueryTimestamp;
  pCtx->scanFlag = scanFlag;

  pCtx->aInputElemBuf = inputData;
  pCtx->hasNullValue = hasNull;
  pCtx->blockStatus = blockStatus;

  if (pField != NULL) {
    pCtx->preAggVals.isSet = true;
    pCtx->preAggVals.wsum = pField->wsum;
    pCtx->preAggVals.sum = pField->sum;
    pCtx->preAggVals.max = pField->max;
    pCtx->preAggVals.min = pField->min;
    pCtx->preAggVals.numOfNullPoints = pField->numOfNullPoints;
  } else {
    pCtx->preAggVals.isSet = false;
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_WAVG ||
             functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP_DST || functionId == TSDB_FUNC_BOTTOM_DST) {
    /*
     * leastsquares function needs two columns of input,
     * currently, the x value of linear equation is set to timestamp column, and the y-value is the column specified in
     * pQuery->pSelectExpr[i].colIdxInBuffer
     *
     * top/bottom function needs timestamp to indicate when the
     * top/bottom values emerge, so does diff function
     *
     */
    if (functionId == TSDB_FUNC_WAVG) {
      pCtx->intermediateBuf[3].nType = TSDB_DATA_TYPE_TIMESTAMP;
      pCtx->intermediateBuf[3].i64Key = pQuery->ekey;
    }

    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);

  } else if (functionId == TSDB_FUNC_ARITHM) {
    pCtx->param[0].pz = param;
  } else if (functionId == TSDB_FUNC_WAVG_DST) {
    ((SWavgRuntime *)pCtx->aOutputBuf)->sKey = StartQueryTimestamp;
    ((SWavgRuntime *)pCtx->aOutputBuf)->eKey = pQuery->ekey;

    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  }

  pCtx->startOffset = startOffset;
  pCtx->size = size;

#if defined(_DEBUG_VIEW)
  int64_t *tsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  int64_t  s = tsList[0];
  int64_t  e = tsList[size - 1];

//    if (IS_DATA_BLOCK_LOADED(blockStatus)) {
//        dTrace("QInfo:%p query ts:%lld-%lld, offset:%d, rows:%d, bstatus:%d,
//        functId:%d", GET_QINFO_ADDR(pQuery),
//               s, e, startOffset, size, blockStatus, functionId);
//    } else {
//        dTrace("QInfo:%p block not loaded, bstatus:%d",
//        GET_QINFO_ADDR(pQuery), blockStatus);
//    }
#endif
}

static int32_t setupQueryRuntimeEnv(SMeterObj *pMeterObj, SQuery *pQuery, SQueryRuntimeEnv *pRuntimeEnv,
                                    SSchema *pTagsSchema, int16_t order) {
  dTrace("QInfo:%p setup runtime env", GET_QINFO_ADDR(pQuery));

  pRuntimeEnv->pMeterObj = pMeterObj;
  pRuntimeEnv->pQuery = pQuery;

  //todo free all allocated resource
  pRuntimeEnv->go = (bool *)malloc(sizeof(bool) * pQuery->numOfOutputCols);
  if (pRuntimeEnv->go == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  memset(pRuntimeEnv->go, 1, sizeof(bool) * pQuery->numOfOutputCols);

  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutputCols, sizeof(SQLFunctionCtx));

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].pBase;
    SColIndexEx *    pColIndexEx = &pSqlFuncMsg->colInfo;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

    if (pSqlFuncMsg->colInfo.isTag) {  // process tag column info
      pCtx->inputType = pTagsSchema[pColIndexEx->colIdx].type;
      pCtx->inputBytes = pTagsSchema[pColIndexEx->colIdx].bytes;
    } else {
      pCtx->inputType = GET_COLUMN_TYPE(pQuery, i);
      pCtx->inputBytes = GET_COLUMN_BYTES(pQuery, i);
    }

    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes = pQuery->pSelectExpr[i].resBytes;
    pCtx->outputType = pQuery->pSelectExpr[i].resType;
    pCtx->order = pQuery->order.order;

    pCtx->numOfParams = pSqlFuncMsg->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      /*
       * tricky: in case of char array parameters, we employ the shallow copy method
       * and get the ownership of the char array, it later release the allocated memory if exists
       */
      pCtx->param[j].nType = pSqlFuncMsg->arg[j].argType;
      pCtx->param[j].i64Key = pSqlFuncMsg->arg[j].argValue.i64;
    }

    /* set the order information for top/bottom query */
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      assert(pQuery->pSelectExpr[0].pBase.functionId == TSDB_FUNC_TS ||
             pQuery->pSelectExpr[0].pBase.functionId == TSDB_FUNC_TS_DUMMY);

      pCtx->param[2].i64Key = order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64Key = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64Key = pQuery->order.orderColId;
    }

    if (i > 0) {
      pRuntimeEnv->offset[i] = pRuntimeEnv->offset[i - 1] + pRuntimeEnv->pCtx[i - 1].outputBytes;
    }
  }

  resetCtxOutputBuf(pRuntimeEnv);
  initCtxOutputBuf(pRuntimeEnv);

  /* for loading block data in memory */
  assert(vnodeList[pMeterObj->vnode].cfg.rowsInFileBlock == pMeterObj->pointsPerFileBlock);

  pRuntimeEnv->buffer =
      (char *)malloc(pQuery->dataRowSize * pMeterObj->pointsPerFileBlock + sizeof(SData) * pQuery->numOfCols);

  if (pRuntimeEnv->buffer == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  pRuntimeEnv->colDataBuffer[0] = (SData *)pRuntimeEnv->buffer;
  for (int32_t i = 1; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i - 1].data.bytes;
    pRuntimeEnv->colDataBuffer[i] =
        (SData *)(((void *)pRuntimeEnv->colDataBuffer[i - 1]) + sizeof(SData) + pMeterObj->pointsPerFileBlock * bytes);
  }

  /* record the maximum column width among columns of this meter/metric */
  int32_t maxColWidth = pQuery->colList[0].data.bytes;
  for (int32_t i = 1; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i].data.bytes;
    if (bytes > maxColWidth) {
      maxColWidth = bytes;
    }
  }

  pRuntimeEnv->primaryColBuffer = NULL;
  if (PRIMARY_TSCOL_LOADED(pQuery)) {
    pRuntimeEnv->primaryColBuffer = pRuntimeEnv->colDataBuffer[0];
  } else {
    pRuntimeEnv->primaryColBuffer = (SData *)malloc(pMeterObj->pointsPerFileBlock * TSDB_KEYSIZE + sizeof(SData));
  }

  pRuntimeEnv->internalBufSize = (size_t) (maxColWidth*pMeterObj->pointsPerFileBlock + EXTRA_BYTES);//plus extra_bytes

  pRuntimeEnv->unzipBuffer = (char *)malloc(pRuntimeEnv->internalBufSize);
  pRuntimeEnv->secondaryUnzipBuffer = (char *)calloc(1, pRuntimeEnv->internalBufSize);

  if (pRuntimeEnv->unzipBuffer == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  if (pRuntimeEnv->secondaryUnzipBuffer == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  /* query runtime env is not called */
  if (pRuntimeEnv->pQuery == NULL) {
    return;
  }

  dTrace("QInfo:%p teardown runtime env", GET_QINFO_ADDR(pRuntimeEnv->pQuery));
  tfree(pRuntimeEnv->buffer);
  tfree(pRuntimeEnv->secondaryUnzipBuffer);
  tfree(pRuntimeEnv->go);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutputCols; ++i) {
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

      for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
        tVariantDestroy(&pCtx->param[j]);
      }

      for (int32_t j = 0; j < tListLen(pCtx->intermediateBuf); ++j) {
        tVariantDestroy(&pCtx->intermediateBuf[j]);
      }
    }

    tfree(pRuntimeEnv->pCtx);
  }

  tfree(pRuntimeEnv->unzipBuffer);

  if (pRuntimeEnv->pQuery && (!PRIMARY_TSCOL_LOADED(pRuntimeEnv->pQuery))) {
    tfree(pRuntimeEnv->primaryColBuffer);
  }

  for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
    SQueryFileInfo *pQFileInfo = &(pRuntimeEnv->pHeaderFiles[i]);
    if (pQFileInfo->pHeaderFileData != NULL && pQFileInfo->pHeaderFileData != MAP_FAILED) {
      munmap(pQFileInfo->pHeaderFileData, pQFileInfo->headFileSize);
    }
    tclose(pQFileInfo->headerFd);

    if (pQFileInfo->pDataFileData != NULL && pQFileInfo->pDataFileData != MAP_FAILED) {
      munmap(pQFileInfo->pDataFileData, pQFileInfo->defaultMappingSize);
    }

    tclose(pQFileInfo->dataFd);
    tclose(pQFileInfo->lastFd);
  }

  if (pRuntimeEnv->pHeaderFiles != NULL) {
    pRuntimeEnv->numOfFiles = 0;
    free(pRuntimeEnv->pHeaderFiles);
  }

  if (pRuntimeEnv->pInterpoBuf != NULL) {
    for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutputCols; ++i) {
      tfree(pRuntimeEnv->pInterpoBuf[i]);
    }

    tfree(pRuntimeEnv->pInterpoBuf);
  }
}

// get maximum time interval in each file
static int64_t getOldestKey(int32_t numOfFiles, int64_t fileId, SVnodeCfg *pCfg) {
  int64_t duration = pCfg->daysPerFile * tsMsPerDay[pCfg->precision];
  return (fileId - numOfFiles + 1) * duration;
}

bool isQueryKilled(SQuery *pQuery) {
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  /*
   * check if the queried meter is going to be deleted.
   * if it will be deleted soon, stop current query ASAP.
   */
  SMeterObj* pMeterObj = pQInfo->pObj;
  if (vnodeIsMeterState(pMeterObj, TSDB_METER_STATE_DELETING)) {
    pQInfo->killed = 1;
    return true;
  }

  return (pQInfo->killed == 1);
}

bool isFixedOutputQuery(SQuery *pQuery) {
  if (pQuery->nAggTimeInterval != 0) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (isTopBottomQuery(pQuery)) {
    return true;
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (IS_MULTIOUTPUT(aAggs[functionId].nStatus)) {
      return false;
    }
  }

  return true;
}

bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW || functionID == TSDB_FUNC_LAST_ROW_DST) {
      return true;
    }
  }

  return false;
}

// TODO REFACTOR:MERGE WITH CLIENT-SIDE FUNCTION
bool isTopBottomQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TOP_DST ||
        functionId == TSDB_FUNC_BOTTOM_DST) {
      return true;
    }
  }

  return false;
}

bool isFirstLastRowQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_LAST_ROW || functionID == TSDB_FUNC_LAST_ROW_DST) {
      return true;
    }
  }

  return false;
}

bool needSupplementaryScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (((functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) && QUERY_IS_ASC_QUERY(pQuery)) ||
        ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && !QUERY_IS_ASC_QUERY(pQuery))) {
      return true;
    }
  }

  return false;
}
/////////////////////////////////////////////////////////////////////////////////////////////
static int32_t binarySearchInCacheBlk(SCacheInfo *pCacheInfo, SQuery *pQuery, int32_t keyLen, int32_t firstSlot,
                                      int32_t lastSlot) {
  int32_t midSlot = 0;

  while (1) {
    int32_t numOfBlocks = (lastSlot - firstSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    if (numOfBlocks == 0) {
      numOfBlocks = pCacheInfo->maxBlocks;
    }

    midSlot = (firstSlot + (numOfBlocks >> 1)) % pCacheInfo->maxBlocks;
    SCacheBlock *pBlock = pCacheInfo->cacheBlocks[midSlot];

    TSKEY keyFirst = *((TSKEY *)pBlock->offset[0]);
    TSKEY keyLast = *((TSKEY *)(pBlock->offset[0] + (pBlock->numOfPoints - 1) * keyLen));

    if (numOfBlocks == 1) {
      break;
    }

    if (pQuery->skey > keyLast) {
      if (numOfBlocks == 2) break;
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        int          nextSlot = (midSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pNextBlock = pCacheInfo->cacheBlocks[nextSlot];
        TSKEY        nextKeyFirst = *((TSKEY *)(pNextBlock->offset[0]));
        if (pQuery->skey < nextKeyFirst) break;
      }
      firstSlot = (midSlot + 1) % pCacheInfo->maxBlocks;
    } else if (pQuery->skey < keyFirst) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        int          prevSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pPrevBlock = pCacheInfo->cacheBlocks[prevSlot];
        TSKEY        prevKeyLast = *((TSKEY *)(pPrevBlock->offset[0] + (pPrevBlock->numOfPoints - 1) * keyLen));
        if (pQuery->skey > prevKeyLast) {
          break;
        }
      }
      lastSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

static void getQueryRange(SQuery *pQuery, TSKEY *min, TSKEY *max) {
  *min = pQuery->lastKey < pQuery->ekey ? pQuery->lastKey : pQuery->ekey;
  *max = pQuery->lastKey >= pQuery->ekey ? pQuery->lastKey : pQuery->ekey;
}

static int32_t getFirstCacheSlot(int32_t numOfBlocks, int32_t lastSlot, SCacheInfo *pCacheInfo) {
  return (lastSlot - numOfBlocks + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
}

static bool cacheBoundaryCheck(SQuery *pQuery, SMeterObj *pMeterObj) {
  /*
   * here we get the first slot from the meter cache, not from the cache snapshot from pQuery, since the
   * snapshot value in pQuery may have been expired now.
   */
  SCacheInfo * pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  SCacheBlock *pBlock = NULL;

  // earliest key in cache
  TSKEY keyFirst = 0;
  TSKEY keyLast = pMeterObj->lastKey;

  while (1) {
    // keep the value in local variable, since it may be changed by other thread any time
    int32_t numOfBlocks = pCacheInfo->numOfBlocks;
    int32_t currentSlot = pCacheInfo->currentSlot;

    // no data in cache, return false directly
    if (numOfBlocks == 0) {
      return false;
    }

    int32_t first = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);

    /*
     * pBlock may be null value since this block is flushed to disk, and re-distributes to
     * other meter, so go on until we get the first not flushed cache block.
     */
    if ((pBlock = getCacheDataBlock(pMeterObj, pQuery, first)) != NULL) {
      keyFirst = getTimestampInCacheBlock(pBlock, 0);
      break;
    } else {
      /*
       * there may be only one empty cache block existed caused by import.
       */
      if (numOfBlocks == 1) {
        return false;
      }
    }
  }

  TSKEY min, max;
  getQueryRange(pQuery, &min, &max);

  // the query time range is earlier than the first element in cache. abort
  if (max < keyFirst) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return false;
  }

  if (min > keyLast) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return false;
  }

  return true;
}

void getBasicCacheInfoSnapshot(SQuery *pQuery, SCacheInfo *pCacheInfo, int32_t vid) {
  // commitSlot here denotes the first uncommitted block in cache
  int32_t numOfBlocks = 0;
  int32_t lastSlot = 0;

  SCachePool *pPool = (SCachePool *)vnodeList[vid].pCachePool;
  pthread_mutex_lock(&pPool->vmutex);
  numOfBlocks = pCacheInfo->numOfBlocks;
  lastSlot = pCacheInfo->currentSlot;
  pthread_mutex_unlock(&pPool->vmutex);

  // make sure it is there, otherwise, return right away
  pQuery->currentSlot = lastSlot;
  pQuery->numOfBlocks = numOfBlocks;
  pQuery->firstSlot = getFirstCacheSlot(numOfBlocks, lastSlot, pCacheInfo);
  ;

  /*
   * Note: the block id is continuous increasing, never becomes smaller.
   *
   * blockId is the maximum block id in cache of current meter during query.
   * If any blocks' id are greater than this value, those blocks may be reallocated to other meters,
   * or assigned new data of this meter, on which the query is performed should be ignored.
   */
  if (pQuery->numOfBlocks > 0) {
    pQuery->blockId = pCacheInfo->cacheBlocks[pQuery->currentSlot]->blockId;
  }
}

int64_t getQueryStartPositionInCache(SQueryRuntimeEnv *pRuntimeEnv, int32_t *slot, int32_t *pos,
                                     bool ignoreQueryRange) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  pQuery->fileId = -1;
  vnodeFreeFieldsEx(pRuntimeEnv);

  //keep in-memory cache status in local variables in case that it may be changed by write operation
  getBasicCacheInfoSnapshot(pQuery, pMeterObj->pCache, pMeterObj->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pQuery->numOfBlocks == 0) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return -1;
  }

  assert((pQuery->lastKey >= pQuery->skey && QUERY_IS_ASC_QUERY(pQuery)) ||
         (pQuery->lastKey <= pQuery->skey && !QUERY_IS_ASC_QUERY(pQuery)));

  if (!ignoreQueryRange && !cacheBoundaryCheck(pQuery, pMeterObj)) {
    return -1;
  }

  /* find the appropriated slot that contains the requested points */
  TSKEY rawskey = pQuery->skey;

  /* here we actual start to query from pQuery->lastKey */
  pQuery->skey = pQuery->lastKey;

  (*slot) = binarySearchInCacheBlk(pCacheInfo, pQuery, TSDB_KEYSIZE, pQuery->firstSlot, pQuery->currentSlot);

  /* locate the first point of which time stamp is no less than pQuery->skey */
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  SCacheBlock *pBlock = pCacheInfo->cacheBlocks[*slot];
  (*pos) = searchFn(pBlock->offset[0], pBlock->numOfPoints, pQuery->skey, pQuery->order.order);

  // restore skey before return
  pQuery->skey = rawskey;

  // all data are less(greater) than the pQuery->lastKey in case of ascending(descending) query
  if (*pos == -1) {
    return -1;
  }

  int64_t nextKey = getTimestampInCacheBlock(pBlock, *pos);
  if ((nextKey < pQuery->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey > pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery))) {
    // all data are less than the pQuery->lastKey(pQuery->sKey) for asc query
    return -1;
  }

  SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
  return nextKey;
}

/**
 * check if data in disk.
 */
bool hasDataInDisk(SQuery *pQuery, SMeterObj *pMeterObj) {
  SVnodeObj *pVnode = &vnodeList[pMeterObj->vnode];
  if (pVnode->numOfFiles <= 0) {
    pQuery->fileId = -1;
    return false;
  }

  int64_t latestKey = pMeterObj->lastKeyOnFile;
  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);

  TSKEY min, max;
  getQueryRange(pQuery, &min, &max);

  /* query range is out of current time interval of table */
  if ((min > latestKey) || (max < oldestKey)) {
    pQuery->fileId = -1;
    return false;
  }

  return true;
}

bool hasDataInCache(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj) {
  SQuery *    pQuery = pRuntimeEnv->pQuery;
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

  /* no data in cache, return */
  if ((pCacheInfo == NULL) || (pCacheInfo->cacheBlocks == NULL)) {
    return false;
  }

  /* numOfBlocks value has been overwrite, release pFields data if exists */
  vnodeFreeFieldsEx(pRuntimeEnv);
  getBasicCacheInfoSnapshot(pQuery, pCacheInfo, pMeterObj->vnode);
  if (pQuery->numOfBlocks <= 0) {
    return false;
  }

  return cacheBoundaryCheck(pQuery, pMeterObj);
}

/**
 *  Get cache snapshot will destroy the comp block info in SQuery, in order to speedup the query
 *  process, we always check cache first.
 */
void vnodeCheckIfDataExists(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, bool *dataInDisk, bool *dataInCache) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  *dataInCache = hasDataInCache(pRuntimeEnv, pMeterObj);
  *dataInDisk = hasDataInDisk(pQuery, pMeterObj);
}

static void doGetAlignedIntervalQueryRangeImpl(SQuery *pQuery, int64_t qualifiedKey, int64_t keyFirst, int64_t keyLast,
                                               int64_t *skey, int64_t *ekey) {
  assert(qualifiedKey >= keyFirst && qualifiedKey <= keyLast);

  if (keyFirst > (INT64_MAX - pQuery->nAggTimeInterval)) {
    /*
     * if the skey > INT64_MAX - pQuery->nAggTimeInterval, the query duration between
     * skey and ekey must be less than one interval.Therefore, no need to adjust the query ranges.
     */
    assert(keyLast - keyFirst < pQuery->nAggTimeInterval);

    *skey = keyFirst;
    *ekey = keyLast;
    return;
  }

  *skey = taosGetIntervalStartTimestamp(qualifiedKey, pQuery->nAggTimeInterval, pQuery->intervalTimeUnit);
  int64_t endKey = *skey + pQuery->nAggTimeInterval - 1;

  if (*skey < keyFirst) {
    *skey = keyFirst;
  }

  if (endKey < keyLast) {
    *ekey = endKey;
  } else {
    *ekey = keyLast;
  }
}

static void doGetAlignedIntervalQueryRange(SQuery *pQuery, TSKEY key, TSKEY skey, TSKEY ekey) {
  TSKEY skey1, ekey1;

  TSKEY skey2 = (skey < ekey) ? skey : ekey;
  TSKEY ekey2 = (skey < ekey) ? ekey : skey;

  doGetAlignedIntervalQueryRangeImpl(pQuery, key, skey2, ekey2, &skey1, &ekey1);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    pQuery->skey = skey1;
    pQuery->ekey = ekey1;
    assert(pQuery->skey <= pQuery->ekey);
  } else {
    pQuery->skey = ekey1;
    pQuery->ekey = skey1;
    assert(pQuery->skey >= pQuery->ekey);
  }

  pQuery->lastKey = pQuery->skey;
}

static void getOneRowFromDiskBlock(SQueryRuntimeEnv *pRuntimeEnv, char **dst, int32_t pos) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i].data.bytes;
    memcpy(dst[i], pRuntimeEnv->colDataBuffer[i]->data + pos * bytes, bytes);
  }
}

static void getOneRowFromCacheBlock(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, SCacheBlock *pBlock,
                                    char **dst, int32_t pos) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  /*
   * in case of cache block expired, the pos may exceed the number of points in block, so check
   * the range in the first place.
   */
  if (pos > pBlock->numOfPoints) {
    pos = pBlock->numOfPoints;
  }

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    int32_t colIdx = pQuery->colList[i].colIdx;
    int32_t colId = pQuery->colList[i].data.colId;

    SColumn *pCols = &pMeterObj->schema[colIdx];

    if (colIdx < 0 || colIdx >= pMeterObj->numOfColumns || pCols->colId != colId) {  // set null
      setNull(dst[i], pCols->type, pCols->bytes);
    } else {
      memcpy(dst[i], pBlock->offset[colIdx] + pos * pCols->bytes, pCols->bytes);
    }
  }
}

static bool getNeighborPoints(SMeterQuerySupportObj *pSupporter, SMeterObj *pMeterObj,
                              SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (!isPointInterpoQuery(pQuery)) {
    return false;
  }

  /*
   * for interpolate point query, points that are directly before/after the specified point are required
   */
  if (isFirstLastRowQuery(pQuery)) {
    assert(!QUERY_IS_ASC_QUERY(pQuery));
  } else {
    assert(QUERY_IS_ASC_QUERY(pQuery));
  }
  assert(pPointInterpSupporter != NULL && pQuery->skey == pQuery->ekey);

  SCacheBlock *pBlock = NULL;

  qTrace("QInfo:%p get next data point, fileId:%d, slot:%d, pos:%d", GET_QINFO_ADDR(pQuery), pQuery->fileId,
         pQuery->slot, pQuery->pos);

  // save the point that is directly after or equals to the specified point
  if (IS_DISK_DATA_BLOCK(pQuery)) {
    getOneRowFromDiskBlock(pRuntimeEnv, pPointInterpSupporter->pNextPoint, pQuery->pos);
  } else {
    pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
    __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

    while (pBlock == NULL) {
      // cache block is flushed to disk, try to find new query position again
      getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

      // new position is located in file, load data and abort
      if (IS_DISK_DATA_BLOCK(pQuery)) {
        getOneRowFromDiskBlock(pRuntimeEnv, pPointInterpSupporter->pNextPoint, pQuery->pos);
        break;
      } else {
        pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
      }
    }

    if (!IS_DISK_DATA_BLOCK(pQuery)) {
      getOneRowFromCacheBlock(pRuntimeEnv, pMeterObj, pBlock, pPointInterpSupporter->pNextPoint, pQuery->pos);
    }
  }

  /*
   * 1. for last_row query, return immediately.
   * 2. the specified timestamp equals to the required key, interpolation according to neighbor points is not necessary
   *    for interp query.
   */
  TSKEY actualKey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  if (isFirstLastRowQuery(pQuery) || actualKey == pQuery->skey) {
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

    /*
     * the retrieved ts may not equals to pMeterObj->lastKey due to cache re-allocation
     * set the pQuery->ekey/pQuery->skey/pQuery->lastKey to be the new value.
     */
    if (pQuery->ekey != actualKey) {
      pQuery->skey = actualKey;
      pQuery->ekey = actualKey;
      pQuery->lastKey = actualKey;
      pSupporter->rawSKey = actualKey;
      pSupporter->rawEKey = actualKey;
    }
    return true;
  }

  /* the qualified point is not the first point in data block */
  if (pQuery->pos > 0) {
    int32_t prevPos = pQuery->pos - 1;

    if (IS_DISK_DATA_BLOCK(pQuery)) {
      /* save the point that is directly after the specified point */
      getOneRowFromDiskBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, prevPos);
    } else {
      getOneRowFromCacheBlock(pRuntimeEnv, pMeterObj, pBlock, pPointInterpSupporter->pPrevPoint, prevPos);
    }
  } else {
    __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

    savePointPosition(&pRuntimeEnv->startPos, pQuery->fileId, pQuery->slot, pQuery->pos);

    // backwards movement would not set the pQuery->pos correct. We need to set it manually later.
    moveToNextBlock(pRuntimeEnv, QUERY_DESC_FORWARD_STEP, searchFn, true);

    /*
     * no previous data exists reset the status and load the data block that contains the qualified point
     */
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      dTrace("QInfo:%p no previous data block, start fileId:%d, slot:%d, pos:%d, qrange:%lld-%lld, out of range",
          GET_QINFO_ADDR(pQuery), pRuntimeEnv->startPos.fileId, pRuntimeEnv->startPos.slot, pRuntimeEnv->startPos.pos,
          pQuery->skey, pQuery->ekey);

      // no result, return immediately
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return false;
    } else {  // prev has been located
      if (pQuery->fileId >= 0) {
        pQuery->pos = pQuery->pBlock[pQuery->slot].numOfPoints - 1;
        getOneRowFromDiskBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, pQuery->pos);

        qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d",
            GET_QINFO_ADDR(pQuery), pQuery->fileId, pQuery->slot, pQuery->pos, pQuery->pos);
      } else {
        pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
        if (pBlock == NULL) {
          // todo nothing, the previous block is flushed to disk
        } else {
          pQuery->pos = pBlock->numOfPoints - 1;
          getOneRowFromCacheBlock(pRuntimeEnv, pMeterObj, pBlock, pPointInterpSupporter->pPrevPoint, pQuery->pos);

          qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d",
              GET_QINFO_ADDR(pQuery), pQuery->fileId, pQuery->slot, pBlock->numOfPoints - 1, pQuery->pos);
        }
      }
    }
  }

  pQuery->skey = *(TSKEY *)pPointInterpSupporter->pPrevPoint[0];
  pQuery->ekey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  pQuery->lastKey = pQuery->skey;

  return true;
}

static bool doGetQueryPos(TSKEY key, SMeterQuerySupportObj *pSupporter, SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  SMeterObj *       pMeterObj = pRuntimeEnv->pMeterObj;

  /* key in query range. If not, no qualified in disk file */
  if (key != -1 && key <= pQuery->ekey) {
    if (isPointInterpoQuery(pQuery)) { /* no qualified data in this query range */
      return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
    } else {
      getAlignedIntervalQueryRange(pQuery, key, pQuery->skey, pQuery->ekey);
      return true;
    }
  } else {  // key > pQuery->ekey, abort for normal query, continue for interp query
    if (isPointInterpoQuery(pQuery)) {
      return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
    } else {
      return false;
    }
  }
}

/**
 * determine the first query range, according to raw query range [skey, ekey] and group-by interval.
 * the time interval for aggregating is not enforced to check its validation, the minimum interval is not less than 10ms,
 * which is guaranteed by parser at client-side
 */
bool normalizedFirstQueryRange(bool dataInDisk, bool dataInCache, SMeterQuerySupportObj *pSupporter,
                               SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *  pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  SMeterObj *         pMeterObj = pRuntimeEnv->pMeterObj;
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    // todo: the action return as the getQueryStartPositionInCache function
    if (dataInDisk && getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_GREATER_EQUAL, searchFn)) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
      assert(key >= pQuery->skey);

      return doGetQueryPos(key, pSupporter, pPointInterpSupporter);
    }

    // set no data in file
    pQuery->fileId = -1;
    SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

    /* if it is a interpolation query, the any points in cache that is greater than the query range is required */
    if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pCacheInfo->numOfBlocks == 0 || !dataInCache) {
      return false;
    }

    TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
    return doGetQueryPos(nextKey, pSupporter, pPointInterpSupporter);

  } else {              // descending order
    if (dataInCache) {  // todo handle error
      TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
      assert(nextKey == -1 || nextKey <= pQuery->skey);

      // valid data in cache
      if (nextKey != -1) {
        if (nextKey >= pQuery->ekey) {
          if (isFirstLastRowQuery(pQuery)) {
            return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
          } else {
            getAlignedIntervalQueryRange(pQuery, nextKey, pQuery->skey, pQuery->ekey);
            return true;
          }
        } else {
          /*
           * nextKey < pQuery->ekey && nextKey < pQuery->lastKey, query range is
           * larger than all data, abort NOTE: Interp query does not reach here, since for all interp query,
           * the query order is ascending order.
           */
          return false;
        }
      } else {  // all data in cache are greater than pQuery->lastKey, try file
      }
    }

    if (dataInDisk && getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn)) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
      assert(key <= pQuery->skey);

      /* key in query range. If not, no qualified in disk file */
      if (key >= pQuery->ekey) {
        if (isFirstLastRowQuery(pQuery)) { /* no qualified data in this query range */
          return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
        } else {
          getAlignedIntervalQueryRange(pQuery, key, pQuery->skey, pQuery->ekey);
          return true;
        }
      } else {  // Goes on in case of key in file less than pMeterObj->lastKey,
                // which is also the pQuery->skey
        if (isFirstLastRowQuery(pQuery)) {
          return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
        }
      }
    }
  }

  return false;
}

// todo handle the mmap relative offset value assert problem
int64_t loadRequiredBlockIntoMem(SQueryRuntimeEnv *pRuntimeEnv, SPositionInfo *position) {
  TSKEY nextTimestamp = -1;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  pQuery->fileId = position->fileId;
  pQuery->slot = position->slot;
  pQuery->pos = position->pos;

  if (position->fileId == -1) {
    SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
    if (pCacheInfo == NULL || pCacheInfo->numOfBlocks == 0 || pCacheInfo->cacheBlocks == NULL) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return -1;
    }

    SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
    if (pBlock != NULL) {
      nextTimestamp = getTimestampInCacheBlock(pBlock, position->pos);
    } else {
      // todo fix it
    }

    SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
  } else {
    // todo handle the file broken situation
    /*
     * load the file metadata into buffer first, then the specific data block.
     * currently opened file is not the start file, reset to the start file
     */
    int32_t fileIdx = vnodeGetVnodeHeaderFileIdx(&pQuery->fileId, pRuntimeEnv, pQuery->order.order);
    if (fileIdx < 0) {
      dError("QInfo:%p failed to get data file:%d", GET_QINFO_ADDR(pQuery), pQuery->fileId);
      // ignore the files on disk
      position->fileId = -1;
      return -1;
    }

    /*
     * NOTE: the compblock information may not be loaded yet, here loaded it firstly
     * If the compBlock info is loaded, it wont be loaded again.
     */
    int32_t numOfBlocks = vnodeGetCompBlockInfo(pMeterObj, pRuntimeEnv, fileIdx);
    assert(numOfBlocks > 0);

    nextTimestamp = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  }

  return nextTimestamp;
}

static void setScanLimitationByResultBuffer(SQuery *pQuery) {
  bool multiOutput = false;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    multiOutput = IS_MULTIOUTPUT(aAggs[functionId].nStatus) && (functionId != TSDB_FUNC_TOP) &&
                  (functionId != TSDB_FUNC_BOTTOM) && (functionId != TSDB_FUNC_TOP_DST) &&
                  (functionId != TSDB_FUNC_BOTTOM_DST);
    if (multiOutput) {
      break;
    }
  }

  pQuery->checkBufferInLoop = multiOutput ? 1 : 0;
  pQuery->pointsOffset = pQuery->pointsToRead;
}

/*
 * todo add more parameters to check soon..
 */
bool vnodeParametersSafetyCheck(SQuery *pQuery) {
  // load data column information is incorrect
  for (int32_t i = 0; i < pQuery->numOfCols - 1; ++i) {
    if (pQuery->colList[i].data.colId == pQuery->colList[i + 1].data.colId) {
      dError("QInfo:%p invalid data load column for query", GET_QINFO_ADDR(pQuery));
      return false;
    }
  }
  return true;
}

static int file_order_comparator(const void *p1, const void *p2) {
  SQueryFileInfo *pInfo1 = (SQueryFileInfo *)p1;
  SQueryFileInfo *pInfo2 = (SQueryFileInfo *)p2;

  if (pInfo1->fileID == pInfo2->fileID) {
    return 0;
  }

  return (pInfo1->fileID > pInfo2->fileID) ? 1 : -1;
}

/**
 * open a data files and header file for metric meta query
 * @param pQInfo
 * @param pVnodeFiles
 * @param fid
 * @param vnodeId
 * @param fileName
 * @param prefix
 * @return
 */
static int32_t vnodeOpenVnodeDBFiles(SQInfo *pQInfo, SQueryFileInfo *pVnodeFiles, int32_t fid, int32_t vnodeId,
                                     char *fileName, char *prefix) {
  __off_t size = 0;

  pVnodeFiles->fileID = fid;
  pVnodeFiles->defaultMappingSize = DEFAULT_DATA_FILE_MMAP_WINDOW_SIZE;

  snprintf(pVnodeFiles->headerFilePath, 256, "%s%s", prefix, fileName);

#if 1
  pVnodeFiles->headerFd = open(pVnodeFiles->headerFilePath, O_RDONLY);
#else
  int32_t *val = (int32_t *)taosGetStrHashData(fileHandleHashList, pVnodeFiles->headerFilePath);
  if (val == NULL) {
    pVnodeFiles->headerFd = open(pVnodeFiles->headerFilePath, O_RDONLY);
    taosAddStrHash(fileHandleHashList, pVnodeFiles->headerFilePath, (char *)&pVnodeFiles->headerFd);
  } else {
    pVnodeFiles->headerFd = *val;
  }
#endif

  if (!VALIDFD(pVnodeFiles->headerFd)) {
    dError("QInfo:%p failed open header file:%s reason:%s", pQInfo, pVnodeFiles->headerFilePath, strerror(errno));
    goto _clean;
  }

  struct stat fstat;
  if (stat(pVnodeFiles->headerFilePath, &fstat) < 0) return -1;
  pVnodeFiles->headFileSize = fstat.st_size;
  size = fstat.st_size;

  pVnodeFiles->pHeaderFileData = mmap(NULL, size, PROT_READ, MAP_SHARED, pVnodeFiles->headerFd, 0);
  if (pVnodeFiles->pHeaderFileData == MAP_FAILED) {
    dError("QInfo:%p failed to map header file:%s, %s", pQInfo, pVnodeFiles->headerFilePath, strerror(errno));
    goto _clean;
  }

  if (madvise(pVnodeFiles->pHeaderFileData, size, MADV_SEQUENTIAL) == -1) {
    /* even the advise failed, continue.. */
    dError("QInfo:%p failed to advise kernel the usage of header files, reason:%s", pQInfo, strerror(errno));
  }

  snprintf(pVnodeFiles->dataFilePath, 256, "%sv%df%d.data", prefix, vnodeId, fid);
  snprintf(pVnodeFiles->lastFilePath, 256, "%sv%df%d.last", prefix, vnodeId, fid);

#if 1
  pVnodeFiles->dataFd = open(pVnodeFiles->dataFilePath, O_RDONLY);
  pVnodeFiles->lastFd = open(pVnodeFiles->lastFilePath, O_RDONLY);
#else
  val = (int32_t *)taosGetStrHashData(fileHandleHashList, pVnodeFiles->dataFilePath);
  if (val == NULL) {
    pVnodeFiles->dataFd = open(pVnodeFiles->dataFilePath, O_RDONLY);
    taosAddStrHash(fileHandleHashList, pVnodeFiles->dataFilePath, (char *)&pVnodeFiles->dataFd);
  } else {
    pVnodeFiles->dataFd = *val;
  }
#endif

  if (!VALIDFD(pVnodeFiles->dataFd)) {
    dError("QInfo:%p failed to open data file:%s, reason:%s", pQInfo, pVnodeFiles->dataFilePath, strerror(errno));
    goto _clean;
  }

  if (!VALIDFD(pVnodeFiles->lastFd)) {
    dError("QInfo:%p failed to open last file:%s, reason:%s", pQInfo, pVnodeFiles->lastFilePath, strerror(errno));
    goto _clean;
  }

#if 0
    size = lseek(pVnodeFiles->dataFd, 0, SEEK_END);
    pVnodeFiles->dataFileSize = size;
    pVnodeFiles->dtFileMappingOffset = 0;
#else
  if (stat(pVnodeFiles->dataFilePath, &fstat) < 0) return -1;
  pVnodeFiles->dataFileSize = fstat.st_size;

  if (stat(pVnodeFiles->lastFilePath, &fstat) < 0) return -1;
  pVnodeFiles->lastFileSize = fstat.st_size;
#endif

  /* enforce kernel to preload data when the file is mapping */
  pVnodeFiles->pDataFileData = mmap(NULL, pVnodeFiles->defaultMappingSize, PROT_READ, MAP_PRIVATE | MAP_POPULATE,
                                    pVnodeFiles->dataFd, pVnodeFiles->dtFileMappingOffset);
  if (pVnodeFiles->pDataFileData == MAP_FAILED) {
    dError("QInfo:%p failed to map data file:%s, %s", pQInfo, pVnodeFiles->dataFilePath, strerror(errno));
    goto _clean;
  }

  /* advise kernel the usage of mmaped data */
  if (madvise(pVnodeFiles->pDataFileData, pVnodeFiles->defaultMappingSize, MADV_SEQUENTIAL) == -1) {
    dError("QInfo:%p failed to advise kernel the usage of data file:%s, reason:%s", pQInfo, pVnodeFiles->dataFilePath,
           strerror(errno));
  }

  return 0;

_clean:
  if (pVnodeFiles->pHeaderFileData != MAP_FAILED && pVnodeFiles->pDataFileData != NULL) {
    munmap(pVnodeFiles->pHeaderFileData, pVnodeFiles->headFileSize);
    pVnodeFiles->pHeaderFileData = NULL;
  }

  if (pVnodeFiles->pDataFileData != MAP_FAILED && pVnodeFiles->pDataFileData != NULL) {
    munmap(pVnodeFiles->pDataFileData, pVnodeFiles->defaultMappingSize);
    pVnodeFiles->pDataFileData = NULL;
  }

  tclose(pVnodeFiles->headerFd);
  tclose(pVnodeFiles->dataFd);
  tclose(pVnodeFiles->lastFd);
  return -1;
}

static void vnodeOpenAllFiles(SQInfo *pQInfo, int32_t vnodeId) {
  char dbFilePathPrefix[TSDB_FILENAME_LEN] = {0};

  sprintf(dbFilePathPrefix, "%s/vnode%d/db/", tsDirectory, vnodeId);
  DIR *pDir = opendir(dbFilePathPrefix);
  if (pDir == NULL) {
    dError("QInfo:%p failed to open directory:%s", pQInfo, dbFilePathPrefix);
    return;
  }

  char suffix[] = ".head";

  struct dirent *pEntry = NULL;
  int32_t        alloc = 4;  // default allocated size

  SQueryRuntimeEnv *pRuntimeEnv = &(pQInfo->pMeterQuerySupporter->runtimeEnv);
  pRuntimeEnv->pHeaderFiles = calloc(1, sizeof(SQueryFileInfo) * alloc);
  SVnodeObj *pVnode = &vnodeList[vnodeId];

  while ((pEntry = readdir(pDir)) != NULL) {
    if ((pEntry->d_name[0] == '.' && pEntry->d_name[1] == '\0') || (strcmp(pEntry->d_name, "..") == 0)) {
      continue;
    }

    if (pEntry->d_type & DT_DIR) {
      continue;
    }

    size_t len = strlen(pEntry->d_name);
    if (strcasecmp(&pEntry->d_name[len - 5], suffix) != 0) {
      continue;
    }

    int32_t vid = 0;
    int32_t fid = 0;
    sscanf(pEntry->d_name, "v%df%d", &vid, &fid);
    if (vid != vnodeId) { /* ignore error files */
      dError("QInfo:%p error data file:%s in vid:%d, ignore", pQInfo, pEntry->d_name, vnodeId);
      continue;
    }

    int32_t firstFid = pVnode->fileId - pVnode->numOfFiles + 1;
    if (fid > pVnode->fileId || fid < firstFid) {
      dError("QInfo:%p error data file:%s in vid:%d, fid:%d, fid range:%d-%d", pQInfo, pEntry->d_name, vnodeId,
             firstFid, pVnode->fileId);
      continue;
    }

    assert(fid >= 0 && vid >= 0);

    if (++pRuntimeEnv->numOfFiles > alloc) {
      alloc = alloc << 1;
      pRuntimeEnv->pHeaderFiles = realloc(pRuntimeEnv->pHeaderFiles, alloc * sizeof(SQueryFileInfo));
      memset(&pRuntimeEnv->pHeaderFiles[alloc >> 1], 0, (alloc >> 1) * sizeof(SQueryFileInfo));
    }

    SQueryFileInfo *pVnodeFiles = &pRuntimeEnv->pHeaderFiles[pRuntimeEnv->numOfFiles - 1];
    int32_t         ret = vnodeOpenVnodeDBFiles(pQInfo, pVnodeFiles, fid, vnodeId, pEntry->d_name, dbFilePathPrefix);
    if (ret < 0) {
      memset(pVnodeFiles, 0, sizeof(SQueryFileInfo));  // reset information
      pRuntimeEnv->numOfFiles -= 1;
    }
  }

  closedir(pDir);

  dTrace("QInfo:%p find %d data files in %s to be checked", pQInfo, pRuntimeEnv->numOfFiles, dbFilePathPrefix);

  /* order the files information according their names */
  qsort(pRuntimeEnv->pHeaderFiles, (size_t)pRuntimeEnv->numOfFiles, sizeof(SQueryFileInfo), file_order_comparator);
}

static void updateOffsetVal(SQueryRuntimeEnv *pRuntimeEnv, void *pBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    pQuery->pos += pQuery->limit.offset;
  } else {
    pQuery->pos -= pQuery->limit.offset;
  }

  if (IS_DISK_DATA_BLOCK(pQuery)) {
    pQuery->skey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  } else {
    pQuery->skey = getTimestampInCacheBlock(pBlock, pQuery->pos);
  }

  pQuery->lastKey = pQuery->skey;
  pQuery->limit.offset = 0;
}

// todo ignore the avg/sum/min/max/count/stddev/top/bottom functions, of which
// the scan order is not matter
static bool onlyOneQueryType(SQuery *pQuery, int32_t functId, int32_t functIdDst) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }
    if (functionId != functId && functionId != functIdDst) {
      return false;
    }
  }

  return true;
}

static bool onlyFirstQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_FIRST, TSDB_FUNC_FIRST_DST); }

static bool onlyLastQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_LAST, TSDB_FUNC_LAST_DST); }

static void rewriteExecOrder(SQuery *pQuery, bool metricQuery) {
  // in case of point-interpolation query, use asc order scan
  char msg[] = "QInfo:%p scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%lld-%lld, "
               "new qrange:%lld-%lld";

  // descending order query
  if (isFirstLastRowQuery(pQuery)) {
    dTrace("QInfo:%p scan order changed for last_row query, old:%d, new:%d", GET_QINFO_ADDR(pQuery),
           pQuery->order.order, TSQL_SO_DESC);

    pQuery->order.order = TSQL_SO_DESC;
    return;
  }

  if (isPointInterpoQuery(pQuery) && pQuery->nAggTimeInterval == 0) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      dTrace(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSQL_SO_ASC, pQuery->skey, pQuery->ekey,
             pQuery->ekey, pQuery->skey);
      SWAP(pQuery->skey, pQuery->ekey, TSKEY);
    }

    pQuery->order.order = TSQL_SO_ASC;
    return;
  }

  if (pQuery->nAggTimeInterval == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSQL_SO_ASC, pQuery->skey, pQuery->ekey,
               pQuery->ekey, pQuery->skey);

        SWAP(pQuery->skey, pQuery->ekey, TSKEY);
      }

      pQuery->order.order = TSQL_SO_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSQL_SO_DESC, pQuery->skey, pQuery->ekey,
               pQuery->ekey, pQuery->skey);

        SWAP(pQuery->skey, pQuery->ekey, TSKEY);
      }

      pQuery->order.order = TSQL_SO_DESC;
    }

  } else {  // interval query
    if (metricQuery) {
      if (onlyFirstQuery(pQuery)) {
        if (!QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first stable", pQuery->order.order, TSQL_SO_ASC, pQuery->skey,
                 pQuery->ekey, pQuery->ekey, pQuery->skey);

          SWAP(pQuery->skey, pQuery->ekey, TSKEY);
        }

        pQuery->order.order = TSQL_SO_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSQL_SO_DESC, pQuery->skey,
                 pQuery->ekey, pQuery->ekey, pQuery->skey);

          SWAP(pQuery->skey, pQuery->ekey, TSKEY);
        }

        pQuery->order.order = TSQL_SO_DESC;
      }
    }
  }
}

static int32_t doSkipDataBlock(SQueryRuntimeEnv *pRuntimeEnv) {
  SMeterObj *         pMeterObj = pRuntimeEnv->pMeterObj;
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  int32_t             step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  while (1) {
    int32_t ret = moveToNextBlock(pRuntimeEnv, step, searchFn, false);
    UNUSED(ret);
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      break;
    }

    void *pBlock = getGenericDataBlock(pMeterObj, pQuery, pQuery->slot);

    int32_t    blockType = IS_DISK_DATA_BLOCK(pQuery) ? BLK_FILE_BLOCK : BLK_CACHE_BLOCK;
    SBlockInfo blockInfo = getBlockBasicInfo(pBlock, blockType);

    int32_t maxReads = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.size - pQuery->pos : pQuery->pos + 1;

    if (pQuery->limit.offset < maxReads) {  // start position in current block
      updateOffsetVal(pRuntimeEnv, pBlock);
      break;
    } else {
      pQuery->limit.offset -= maxReads;
      pQuery->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.keyLast : blockInfo.keyFirst;
      pQuery->lastKey += step;
    }
  }

  return 0;
}

void forwardQueryStartPosition(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  if (pQuery->limit.offset <= 0) {
    return;
  }

  void *  pBlock = getGenericDataBlock(pMeterObj, pQuery, pQuery->slot);
  int32_t blockType = (IS_DISK_DATA_BLOCK(pQuery)) ? BLK_FILE_BLOCK : BLK_CACHE_BLOCK;

  SBlockInfo blockInfo = getBlockBasicInfo(pBlock, blockType);
  int32_t    maxReads = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.size - pQuery->pos : pQuery->pos + 1;

  if (pQuery->limit.offset < maxReads) {  // start position in current block
    updateOffsetVal(pRuntimeEnv, pBlock);
  } else {
    pQuery->limit.offset -= maxReads;
    doSkipDataBlock(pRuntimeEnv);
  }
}

static bool forwardQueryStartPosIfNeeded(SQInfo *pQInfo, SMeterQuerySupportObj *pSupporter, bool dataInDisk,
                                         bool dataInCache) {
  SQuery *pQuery = &pQInfo->query;

  /* if queried with value filter, do NOT forward query start position */
  if (pQuery->numOfFilterCols > 0) {
    return true;
  }

  if (pQuery->limit.offset > 0 && (!isTopBottomQuery(pQuery)) && pQuery->interpoType == TSDB_INTERPO_NONE) {
    /*
     * 1. for top/bottom query, the offset applies to the final result, not here
     * 2. for interval without interpolation query we forward pQuery->nAggTimeInterval at a time for
     *    pQuery->limit.offset times. Since hole exists, pQuery->nAggTimeInterval*pQuery->limit.offset value is
     *    not valid. otherwise, we only forward pQuery->limit.offset number of points
     */
    if (pQuery->nAggTimeInterval > 0) {
      while (1) {
        /*
         * the skey may not be the aligned start time
         * 1. it is the value of first existed data point, therefore, the range
         * between skey and ekey may be less than the interval value.
         * 2. the ekey may not be the actual end value of time interval, in case of the
         */
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          pQuery->skey = pQuery->ekey + 1;
        } else {
          pQuery->skey = pQuery->ekey - 1;
        }

        // boundary check
        if ((pQuery->skey > pSupporter->rawEKey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (pQuery->skey < pSupporter->rawEKey && !QUERY_IS_ASC_QUERY(pQuery))) {
          setQueryStatus(pQuery, QUERY_COMPLETED);

          sem_post(&pQInfo->dataReady);  // hack for next read for empty return
          pQInfo->over = 1;
          return false;
        }

        /*
         * NOTE: the end key must be set the last value, to cover all possible data. Otherwise, it may
         * contain no data with only one interval time range
         */
        pQuery->ekey = pSupporter->rawEKey;
        pQuery->lastKey = pQuery->skey;

        // todo opt performance
        if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, NULL) == false) {
          sem_post(&pQInfo->dataReady);  // hack for next read for empty return
          pQInfo->over = 1;
          return false;
        }

        if (--pQuery->limit.offset == 0) {
          break;
        }
      }
    } else {
      forwardQueryStartPosition(&pSupporter->runtimeEnv);
      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
        setQueryStatus(pQuery, QUERY_COMPLETED);

        sem_post(&pQInfo->dataReady);  // hack for next read for empty return
        pQInfo->over = 1;
        return false;
      }
    }
  }

  return true;
}

/**
 * param[0]: tags. reserved for tags
 * param[1]: default value/previous value of specified timestamp
 * param[2]: next value of specified timestamp
 * param[3]: denotes if the result is a precious result or interpolation results
 *
 * intermediate[0]: interpolation type
 * intermediate[1]: precious specified timestamp, the pCtx->startTimetamp is
 * changed during query to satisfy the query procedure
 * intermediate[2]: flag that denotes if it is a primary timestamp column or not
 *
 * todo refactor
 * @param pQInfo
 * @param pSupporter
 * @param pInterpoRaw
 */
void pointInterpSupporterSetData(SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupport) {
  /* not point interpolation query, abort */
  if (!isPointInterpoQuery(&pQInfo->query)) {
    return;
  }

  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  int32_t count = 1;
  TSKEY   key = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

  if (key == pSupporter->rawSKey) {
    /* the queried timestamp has value, return it directly without interpolation
     */
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      tVariantCreateB(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

      pRuntimeEnv->pCtx[i].intermediateBuf[1].i64Key = key;
      pRuntimeEnv->pCtx[i].intermediateBuf[1].nType = TSDB_DATA_TYPE_BIGINT;
    }
  } else {
    /* set the direct previous(next) point for process */
    count = 2;

    if (pQuery->interpoType == TSDB_INTERPO_SET_VALUE) {
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        /* for primary timestamp column, set the flag*/
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == 0) {
          pCtx->intermediateBuf[2].i64Key = 1;
          pCtx->intermediateBuf[2].nType = TSDB_DATA_TYPE_BIGINT;
        }

        tVariantCreateB(&pCtx->param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);
        tVariantCreateB(&pCtx->param[1], (char *)&pQuery->defaultVal[i], pCtx->inputBytes, pCtx->inputType);
        pCtx->intermediateBuf[1].i64Key = pSupporter->rawSKey;
        pCtx->intermediateBuf[1].nType = TSDB_DATA_TYPE_BIGINT;

        /* the interpolation type is set in intermediateBuf[0] */
        tVariantCreateB(&pCtx->intermediateBuf[0], (char *)&pQuery->interpoType, sizeof(pQuery->interpoType),
                        TSDB_DATA_TYPE_SMALLINT);
      }
    } else {
      TSKEY prevKey = *(TSKEY *)pPointInterpSupport->pPrevPoint[0];
      TSKEY nextKey = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        int32_t colInBuf = pQuery->pSelectExpr[i].pBase.colInfo.colIdxInBuf;
        int32_t type = GET_COLUMN_TYPE(pQuery, i);

        char    tmp[TSDB_MAX_BYTES_PER_ROW] = {0};
        int32_t len = 0;

        /* for primary timestamp column, set the flag*/
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == 0) {
          pRuntimeEnv->pCtx[i].intermediateBuf[2].i64Key = 1;
          pRuntimeEnv->pCtx[i].intermediateBuf[2].nType = TSDB_DATA_TYPE_BIGINT;
        } else {
          if ((type >= TSDB_DATA_TYPE_BOOL && type <= TSDB_DATA_TYPE_BIGINT) || type == TSDB_DATA_TYPE_TIMESTAMP) {
            len = sprintf(tmp, "%ld,%ld", prevKey, *(int64_t *)pPointInterpSupport->pPrevPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], tmp, len, TSDB_DATA_TYPE_BINARY);

            len = sprintf(tmp, "%ld,%ld", nextKey, *(int64_t *)pPointInterpSupport->pNextPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[2], tmp, len, TSDB_DATA_TYPE_BINARY);
          } else if (type == TSDB_DATA_TYPE_FLOAT) {
            if (isNull(pPointInterpSupport->pPrevPoint[colInBuf], type)) {
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], TSDB_DATA_NULL_STR_L, strlen(TSDB_DATA_NULL_STR_L),
                              TSDB_DATA_TYPE_BINARY);
            } else {
              len = sprintf(tmp, "%ld,%.9f", prevKey, *(float *)pPointInterpSupport->pPrevPoint[colInBuf]);
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], tmp, len, TSDB_DATA_TYPE_BINARY);
            }

            if (isNull(pPointInterpSupport->pNextPoint[colInBuf], type)) {
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], TSDB_DATA_NULL_STR_L, strlen(TSDB_DATA_NULL_STR_L),
                              TSDB_DATA_TYPE_BINARY);
            } else {
              len = sprintf(tmp, "%ld,%.9f", nextKey, *(float *)pPointInterpSupport->pNextPoint[colInBuf]);
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[2], tmp, len, TSDB_DATA_TYPE_BINARY);
            }
          } else if (type == TSDB_DATA_TYPE_DOUBLE) {
            if (isNull(pPointInterpSupport->pPrevPoint[colInBuf], type)) {
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], TSDB_DATA_NULL_STR_L, strlen(TSDB_DATA_NULL_STR_L),
                              TSDB_DATA_TYPE_BINARY);
            } else {
              len = sprintf(tmp, "%ld,%.9f", prevKey, *(double *)pPointInterpSupport->pPrevPoint[colInBuf]);
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], tmp, len, TSDB_DATA_TYPE_BINARY);
            }

            if (isNull(pPointInterpSupport->pPrevPoint[colInBuf], type)) {
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], TSDB_DATA_NULL_STR_L, strlen(TSDB_DATA_NULL_STR_L),
                              TSDB_DATA_TYPE_BINARY);
            } else {
              len = sprintf(tmp, "%ld,%.9f", nextKey, *(double *)pPointInterpSupport->pNextPoint[colInBuf]);
              tVariantCreateB(&pRuntimeEnv->pCtx[i].param[2], tmp, len, TSDB_DATA_TYPE_BINARY);
            }
          } else if (type == TSDB_DATA_TYPE_BINARY) {
            len = sprintf(tmp, "%ld,%s", prevKey, pPointInterpSupport->pPrevPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], tmp, len, TSDB_DATA_TYPE_BINARY);

            len = sprintf(tmp, "%ld,%s", nextKey, pPointInterpSupport->pNextPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[2], tmp, len, TSDB_DATA_TYPE_BINARY);
          } else if (type == TSDB_DATA_TYPE_NCHAR) {
            int32_t maxLen = TSDB_MAX_BYTES_PER_ROW / TSDB_NCHAR_SIZE;
            len = swprintf((wchar_t *)tmp, maxLen, L"%ld,%ls", prevKey, pPointInterpSupport->pPrevPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[1], tmp, len * TSDB_NCHAR_SIZE, TSDB_DATA_TYPE_NCHAR);

            len = swprintf((wchar_t *)tmp, maxLen, L"%ld,%ls", nextKey, pPointInterpSupport->pNextPoint[colInBuf]);
            tVariantCreateB(&pRuntimeEnv->pCtx[i].param[2], tmp, len * TSDB_NCHAR_SIZE, TSDB_DATA_TYPE_NCHAR);
          }
        }

        tVariantCreateB(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

        /* the interpolation type is set in intermediateBuf[0] */
        tVariantCreateB(&pRuntimeEnv->pCtx[i].intermediateBuf[0], (char *)&pQuery->interpoType,
                        sizeof(pQuery->interpoType), TSDB_DATA_TYPE_SMALLINT);
        pRuntimeEnv->pCtx[i].intermediateBuf[1].i64Key = pSupporter->rawSKey;
        pRuntimeEnv->pCtx[i].intermediateBuf[1].nType = TSDB_DATA_TYPE_BIGINT;
      }
    }
  }
}

void pointInterpSupporterInit(SQuery *pQuery, SPointInterpoSupporter *pInterpoSupport) {
  if (isPointInterpoQuery(pQuery)) {
    pInterpoSupport->pPrevPoint = malloc(pQuery->numOfCols * sizeof(void *));
    pInterpoSupport->pNextPoint = malloc(pQuery->numOfCols * sizeof(void *));

    pInterpoSupport->numOfCols = pQuery->numOfCols;

    /* get appropriated size for one row data source*/
    int32_t len = 0;
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      len += pQuery->colList[i].data.bytes;
    }

    assert(PRIMARY_TSCOL_LOADED(pQuery));

    void *prev = calloc(1, len);
    void *next = calloc(1, len);

    int32_t offset = 0;

    for (int32_t i = 0, j = 0; i < pQuery->numOfCols; ++i, ++j) {
      pInterpoSupport->pPrevPoint[j] = prev + offset;
      pInterpoSupport->pNextPoint[j] = next + offset;

      offset += pQuery->colList[i].data.bytes;
    }
  }
}

void pointInterpSupporterDestroy(SPointInterpoSupporter *pPointInterpSupport) {
  if (pPointInterpSupport->numOfCols <= 0 || pPointInterpSupport->pPrevPoint == NULL) {
    return;
  }

  tfree(pPointInterpSupport->pPrevPoint[0]);
  tfree(pPointInterpSupport->pNextPoint[0]);

  tfree(pPointInterpSupport->pPrevPoint);
  tfree(pPointInterpSupport->pNextPoint);

  pPointInterpSupport->numOfCols = 0;
}

static void allocMemForInterpo(SMeterQuerySupportObj *pSupporter, SQuery *pQuery, SMeterObj *pMeterObj) {
  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    assert(pQuery->nAggTimeInterval > 0 || (pQuery->nAggTimeInterval == 0 && isPointInterpoQuery(pQuery)));

    if (pQuery->nAggTimeInterval > 0) {
      pSupporter->runtimeEnv.pInterpoBuf = malloc(POINTER_BYTES * pQuery->numOfOutputCols);

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        pSupporter->runtimeEnv.pInterpoBuf[i] =
            calloc(1, sizeof(tFilePage) + pQuery->pSelectExpr[i].resBytes * pMeterObj->pointsPerFileBlock);
      }
    }
  }
}

int32_t vnodeQuerySingleMeterPrepare(SQInfo *pQInfo, SMeterObj *pMeterObj, SMeterQuerySupportObj *pSupporter) {
  SQuery *pQuery = &pQInfo->query;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey > pQuery->ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey > pQuery->skey))) {

    dTrace("QInfo:%p no result in time range %lld-%lld, order %d", pQInfo, pQuery->skey, pQuery->ekey,
           pQuery->order.order);

    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;

    return TSDB_CODE_SUCCESS;
  }

  setScanLimitationByResultBuffer(pQuery);
  rewriteExecOrder(pQuery, false);

  pQInfo->over = 0;
  pQInfo->pointsRead = 0;
  pQuery->pointsRead = 0;

  // dataInCache requires lastKey value
  pQuery->lastKey = pQuery->skey;

  vnodeInitDataBlockInfo(&pSupporter->runtimeEnv.loadBlockInfo);
  vnodeInitLoadCompBlockInfo(&pSupporter->runtimeEnv.loadCompBlockInfo);

  // check data in file or cache
  bool dataInCache = true;
  bool dataInDisk = true;
  pSupporter->runtimeEnv.pQuery = pQuery;

  vnodeCheckIfDataExists(&pSupporter->runtimeEnv, pMeterObj, &dataInDisk, &dataInCache);

  /* data in file or cache is not qualified for the query. abort */
  if (!(dataInCache || dataInDisk)) {
    dTrace("QInfo:%p no result in query", pQInfo);
    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;

    return TSDB_CODE_SUCCESS;
  }

  /* create runtime environment */
  int32_t ret = setupQueryRuntimeEnv(pMeterObj, pQuery, &pSupporter->runtimeEnv, NULL, pQuery->order.order);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  vnodeOpenAllFiles(pQInfo, pMeterObj->vnode);

  // in case of last_row query, we set the query timestamp to pMeterObj->lastKey;
  if (isFirstLastRowQuery(pQuery)) {
    pQuery->skey = pMeterObj->lastKey;
    pQuery->ekey = pMeterObj->lastKey;
    pQuery->lastKey = pQuery->skey;
  }

  pSupporter->rawSKey = pQuery->skey;
  pSupporter->rawEKey = pQuery->ekey;

  /* query on single table */
  pSupporter->numOfMeters = 1;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SPointInterpoSupporter interpInfo = {0};
  pointInterpSupporterInit(pQuery, &interpInfo);

  if ((normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &interpInfo) == false) ||
      (isFixedOutputQuery(pQuery) && !isTopBottomQuery(pQuery) && (pQuery->limit.offset > 0)) ||
      (isTopBottomQuery(pQuery) && pQuery->limit.offset >= pQuery->pSelectExpr[1].pBase.arg[0].argValue.i64)) {
    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;

    pointInterpSupporterDestroy(&interpInfo);
    return TSDB_CODE_SUCCESS;
  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for
   * interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &interpInfo);
  pointInterpSupporterDestroy(&interpInfo);

  if (!forwardQueryStartPosIfNeeded(pQInfo, pSupporter, dataInDisk, dataInCache)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t rs = taosGetIntervalStartTimestamp(pSupporter->rawSKey, pQuery->nAggTimeInterval, pQuery->intervalTimeUnit);
  taosInitInterpoInfo(&pSupporter->runtimeEnv.interpoInfo, pQuery->order.order, rs, 0, 0);
  allocMemForInterpo(pSupporter, pQuery, pMeterObj);

  if (!isPointInterpoQuery(pQuery)) {
    assert(pQuery->pos >= 0 && pQuery->slot >= 0);
  }

  // the pQuery->skey is changed during normalizedFirstQueryRange, so set the newest lastkey value
  pQuery->lastKey = pQuery->skey;
  return TSDB_CODE_SUCCESS;
}

void vnodeQueryFreeQInfoEx(SQInfo *pQInfo) {
  if (pQInfo == NULL || pQInfo->pMeterQuerySupporter == NULL) {
    return;
  }

  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  teardownQueryRuntimeEnv(&pSupporter->runtimeEnv);
  tfree(pSupporter->pMeterSidExtInfo);

  if (pSupporter->pMeterObj != NULL) {
    taosCleanUpIntHash(pSupporter->pMeterObj);
    pSupporter->pMeterObj = NULL;
  }

  if (pSupporter->pSidSet != NULL) {
    for (int32_t i = 0; i < pSupporter->pSidSet->numOfSubSet; ++i) {
      destroyBuf(pSupporter->pResult[i].result, pQInfo->query.numOfOutputCols);
    }
  }

  if (VALIDFD(pSupporter->meterOutputFd)) {
    assert(pSupporter->meterOutputMMapBuf != NULL);
    dTrace("QInfo:%p disk-based output buffer during query:%lld bytes", pQInfo, pSupporter->bufSize);
    munmap(pSupporter->meterOutputMMapBuf, pSupporter->bufSize);
    tclose(pSupporter->meterOutputFd);

    unlink(pSupporter->extBufFile);
  }

  tSidSetDestroy(&pSupporter->pSidSet);

  if (pSupporter->pMeterDataInfo != NULL) {
    for (int32_t j = 0; j < pSupporter->numOfMeters; ++j) {
      destroyMeterQueryInfo(pSupporter->pMeterDataInfo[j].pMeterQInfo);
      free(pSupporter->pMeterDataInfo[j].pBlock);
    }
  }

  tfree(pSupporter->pMeterDataInfo);

  tfree(pSupporter->pResult);
  tfree(pQInfo->pMeterQuerySupporter);
}

int32_t vnodeMultiMeterQueryPrepare(SQInfo *pQInfo, SQuery *pQuery) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey > pQuery->ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey > pQuery->skey))) {

    dTrace("QInfo:%p no result in time range %lld-%lld, order %d", pQInfo, pQuery->skey, pQuery->ekey,
           pQuery->order.order);

    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;

    return TSDB_CODE_SUCCESS;
  }

  pQInfo->over = 0;
  pQInfo->pointsRead = 0;
  pQuery->pointsRead = 0;

  rewriteExecOrder(pQuery, true);

  vnodeInitDataBlockInfo(&pSupporter->runtimeEnv.loadBlockInfo);
  vnodeInitLoadCompBlockInfo(&pSupporter->runtimeEnv.loadCompBlockInfo);

  /*
   * since we employ the output control mechanism in main loop.
   * so, disable it during data block scan procedure.
   */
  setScanLimitationByResultBuffer(pQuery);

  // save raw query range for applying to each subgroup
  pSupporter->rawEKey = pQuery->ekey;
  pSupporter->rawSKey = pQuery->skey;
  pQuery->lastKey = pQuery->skey;

  /* create runtime environment */
  SSchema *pTagSchema = NULL;

  tTagSchema *pTagSchemaInfo = pSupporter->pSidSet->pTagSchema;
  if (pTagSchemaInfo != NULL) {
    pTagSchema = pTagSchemaInfo->pSchema;
  }

  /* get one queried meter */
  SMeterObj *pMeter = getMeterObj(pSupporter->pMeterObj, pSupporter->pSidSet->pSids[0]->sid);

  int32_t ret = setupQueryRuntimeEnv(pMeter, pQuery, &pSupporter->runtimeEnv, pTagSchema, TSQL_SO_ASC);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  tSidSetSort(pSupporter->pSidSet);

  vnodeOpenAllFiles(pQInfo, pMeter->vnode);
  pSupporter->pResult = calloc(1, sizeof(SOutputRes) * pSupporter->pSidSet->numOfSubSet);
  if (pSupporter->pResult == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  /* create group result buffer */
  for (int32_t k = 0; k < pSupporter->pSidSet->numOfSubSet; ++k) {
    SOutputRes *pOneRes = &pSupporter->pResult[k];
    pOneRes->nAlloc = 1;
    pOneRes->result = createInMemGroupResultBuf(pSupporter->runtimeEnv.pCtx, pQuery->numOfOutputCols, pOneRes->nAlloc);
  }

  if (pQuery->nAggTimeInterval != 0) {
    getExtTmpfilePath("/tb_metric_mmap_%lld_%lld_%d_%d", pthread_self(), 0, 0, pSupporter->extBufFile);
    pSupporter->meterOutputFd = open(pSupporter->extBufFile, O_CREAT | O_RDWR, 0666);

    if (!VALIDFD(pSupporter->meterOutputFd)) {
      dError("QInfo:%p failed to create file: %s on disk. %s", pQInfo, pSupporter->extBufFile, strerror(errno));
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    // set 4k page for each meter
    pSupporter->numOfPages = pSupporter->numOfMeters;

    ftruncate(pSupporter->meterOutputFd, pSupporter->numOfPages * DEFAULT_INTERN_BUF_SIZE);
    pSupporter->runtimeEnv.numOfRowsPerPage = (DEFAULT_INTERN_BUF_SIZE - sizeof(tFilePage)) / pQuery->rowSize;
    pSupporter->lastPageId = -1;
    pSupporter->bufSize = pSupporter->numOfPages * DEFAULT_INTERN_BUF_SIZE;

    pSupporter->meterOutputMMapBuf =
        mmap(NULL, pSupporter->bufSize, PROT_READ | PROT_WRITE, MAP_SHARED, pSupporter->meterOutputFd, 0);
    if (pSupporter->meterOutputMMapBuf == MAP_FAILED) {
      dError("QInfo:%p failed to map data file: %s to disk. %s", pQInfo, pSupporter->extBufFile, strerror(errno));
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }
  }

  /* metric query do not invoke interpolation, it will be done at the
   * second-stage merge */
  if (!isPointInterpoQuery(pQuery)) {
    pQuery->interpoType = TSDB_INTERPO_NONE;
  }

  TSKEY revisedStime =
      taosGetIntervalStartTimestamp(pSupporter->rawSKey, pQuery->nAggTimeInterval, pQuery->intervalTimeUnit);
  taosInitInterpoInfo(&pSupporter->runtimeEnv.interpoInfo, pQuery->order.order, revisedStime, 0, 0);

  return TSDB_CODE_SUCCESS;
}

/**
 * decrease the refcount for each table involved in this query
 * @param pQInfo
 */
void vnodeDecMeterRefcnt(SQInfo *pQInfo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  if (pSupporter == NULL || pSupporter->numOfMeters == 1) {
    __sync_fetch_and_sub(&pQInfo->pObj->numOfQueries, 1);
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pQInfo->pObj->vnode,
           pQInfo->pObj->sid, pQInfo->pObj->meterId, pQInfo->pObj->numOfQueries);
  } else {
    int32_t num = 0;
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      SMeterObj *pMeter = getMeterObj(pSupporter->pMeterObj, pSupporter->pSidSet->pSids[i]->sid);
      __sync_fetch_and_sub(&(pMeter->numOfQueries), 1);

      if (pMeter->numOfQueries > 0) {
        dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }

    /*
     * in order to reduce log output, for all meters of which numOfQueries count are 0,
     * we do not output corresponding information
     */
    num = pSupporter->numOfMeters - num;
    dTrace("QInfo:%p metric query is over, dec query ref for %d meters, numOfQueries on %d meters are 0",
        pQInfo, pSupporter->numOfMeters, num);
  }
}

// todo merge with doRevisedResultsByLimit
void UNUSED_FUNC truncateResultByLimit(SQInfo *pQInfo, int64_t * final, int32_t *interpo) {
  SQuery *pQuery = &(pQInfo->query);

  if (pQuery->limit.limit > 0 && ((* final) + pQInfo->pointsRead > pQuery->limit.limit)) {
    int64_t num = (* final) + pQInfo->pointsRead - pQuery->limit.limit;
    (*interpo) -= num;
    (* final) -= num;

    setQueryStatus(pQuery, QUERY_COMPLETED);  // query completed
  }
}

TSKEY getTimestampInCacheBlock(SCacheBlock *pBlock, int32_t index) {
  if (pBlock == NULL || index >= pBlock->numOfPoints || index < 0) {
    return -1;
  }

  TSKEY *ts = (TSKEY *)pBlock->offset[0];
  return ts[index];
}

/*
 * NOTE: pQuery->pos will not change, the corresponding data block will be loaded into buffer
 * loadDataBlockOnDemand will change the value of pQuery->pos, according to the pQuery->lastKey
 * */
TSKEY getTimestampInDiskBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t index) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  /*
   * the corresponding compblock info has been loaded already
   * todo add check for compblock loaded
   */
  SCompBlock *pBlock = getDiskDataBlock(pQuery, pQuery->slot);

  // this block must be loaded into buffer
  SQueryLoadBlockInfo *pLoadInfo = &pRuntimeEnv->loadBlockInfo;
  assert(pQuery->pos >= 0 && pQuery->pos < pBlock->numOfPoints);

  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  bool    loadTimestamp = true;
  int32_t fileId = pQuery->fileId;
  int32_t fileIndex = vnodeGetVnodeHeaderFileIdx(&fileId, pRuntimeEnv, pQuery->order.order);

  if (!vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, fileIndex)) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, slot:%d load data block due to primary key required",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot);

    // todo handle failed to load data, file corrupted
    // todo refactor the return value
    int32_t ret =
        loadDataBlockIntoMem(pBlock, &pQuery->pFields[pQuery->slot], pRuntimeEnv, fileIndex, loadTimestamp, true);
    UNUSED(ret);
  }

  SET_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus);
  SET_FILE_BLOCK_FLAG(pRuntimeEnv->blockStatus);

  assert(pQuery->fileId == pLoadInfo->fileId && pQuery->slot == pLoadInfo->slotIdx);
  return ((TSKEY *)pRuntimeEnv->primaryColBuffer->data)[index];
}

// todo remove this function
static void getFirstDataBlockInCache(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  assert(pQuery->fileId == -1 && QUERY_IS_ASC_QUERY(pQuery));

  /*
   * get the start position in cache according to the pQuery->lastkey
   *
   * In case of cache and disk file data overlaps and all required data are commit to disk file,
   * there are no qualified data available in cache, we need to set the QUERY_COMPLETED flag.
   *
   * If cache data and disk-based data are not completely overlapped, cacheBoundaryCheck function will set the
   * correct status flag.
   */
  TSKEY nextTimestamp = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, true);
  if (nextTimestamp < 0) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
  } else if (nextTimestamp > pQuery->ekey) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }
}

void getQueryPositionForCacheInvalid(SQueryRuntimeEnv *pRuntimeEnv, __block_search_fn_t searchFn) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SQInfo *   pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  int32_t    step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  dTrace("QInfo:%p vid:%d sid:%d id:%s cache block re-allocated to other meter, "
      "try get query start position in file/cache, qrange:%lld-%lld, lastKey:%lld",
      pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

  if (step == QUERY_DESC_FORWARD_STEP) {
    /*
     * In descending order query, if the cache is invalid, it must be flushed to disk.
     * Try to find the appropriate position in file, and no need to search cache any more.
     */
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);

    dTrace("QInfo:%p vid:%d sid:%d id:%s find the possible position, fileId:%d, slot:%d, pos:%d",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);

    if (ret) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key < pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }
    } else {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    }
  } else {
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_GREATER_EQUAL, searchFn);
    if (ret) {
      dTrace("QInfo:%p vid:%d sid:%d id:%s find the possible position, fileId:%d, slot:%d, pos:%d",
          pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);

      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key > pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }
    } else {
      /*
       * all data in file is less than the pQuery->lastKey, try cache.
       * cache block status will be set in getFirstDataBlockInCache function
       */
      getFirstDataBlockInCache(pRuntimeEnv);

      dTrace("QInfo:%p vid:%d sid:%d id:%s find the new position in cache, fileId:%d, slot:%d, pos:%d",
          pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);
    }
  }
}

static int32_t moveToNextBlockInCache(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn) {
  SQuery *    pQuery = pRuntimeEnv->pQuery;
  SMeterObj * pMeterObj = pRuntimeEnv->pMeterObj;
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

  assert(pQuery->fileId < 0);

  /*
   * ascending order to last cache block all data block in cache have been iterated, no need to set
   * pRuntimeEnv->nextPos. done
   */
  if (step == QUERY_ASC_FORWARD_STEP && pQuery->slot == pQuery->currentSlot) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return DISK_DATA_LOADED;
  }

  /*
   * descending order to first cache block, try file
   * NOTE: use the real time cache information, not the snapshot
   */
  int32_t numOfBlocks = pCacheInfo->numOfBlocks;
  int32_t currentSlot = pCacheInfo->currentSlot;

  int32_t firstSlot = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);
  if (step == QUERY_DESC_FORWARD_STEP && pQuery->slot == firstSlot) {
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);
    if (ret) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key < pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }

      assert(pRuntimeEnv->startPos.fileId < 0);

    } else {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    }
    return DISK_DATA_LOADED;
  }

  /* now still iterate the cache data blocks */
  pQuery->slot = (pQuery->slot + step + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
  SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);

  /*
   * data in this cache block has been flushed to disk, then we should locate the start position in file.
   * In both desc/asc query, this situation may occur. And we need to locate the start query position in file or cache.
   */
  if (pBlock == NULL) {
    getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

    return DISK_DATA_LOADED;
  } else {
    pQuery->pos = (QUERY_IS_ASC_QUERY(pQuery)) ? 0 : pBlock->numOfPoints - 1;

    TSKEY startkey = getTimestampInCacheBlock(pBlock, pQuery->pos);
    if (startkey < 0) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
    }

    SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);

    dTrace("QInfo:%p check cache block, blockId:%d slot:%d pos:%d, blockstatus:%d", GET_QINFO_ADDR(pQuery),
           pQuery->blockId, pQuery->slot, pQuery->pos, pRuntimeEnv->blockStatus);
  }

  return DISK_DATA_LOADED;
}

/**
 *  move the cursor to next block and not load
 */
static int32_t moveToNextBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn,
                               bool loadData) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  SET_DATA_BLOCK_NOT_LOADED(pRuntimeEnv->blockStatus);

  if (pQuery->fileId >= 0) {
    int32_t fileIndex = -1;

    /*
     * 1. ascending  order. The last data block of data file
     * 2. descending order. The first block of file
     */
    if ((step == QUERY_ASC_FORWARD_STEP && (pQuery->slot == pQuery->numOfBlocks - 1)) ||
        (step == QUERY_DESC_FORWARD_STEP && (pQuery->slot == 0))) {
      fileIndex = getNextDataFileCompInfo(pRuntimeEnv, pMeterObj, step);
      /* data maybe in cache */
      if (fileIndex < 0) {
        assert(pQuery->fileId == -1);
        if (step == QUERY_ASC_FORWARD_STEP) {
          getFirstDataBlockInCache(pRuntimeEnv);
        } else { /* no data any more */
          setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        }

        return DISK_DATA_LOADED;
      } else {
        pQuery->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->pBlock[pQuery->slot].numOfPoints - 1;
        pQuery->slot = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->numOfBlocks - 1;
      }
    } else {  // next block in the same file
      int32_t fid = pQuery->fileId;
      fileIndex = vnodeGetVnodeHeaderFileIdx(&fid, pRuntimeEnv, pQuery->order.order);
      pQuery->slot += step;
      pQuery->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->pBlock[pQuery->slot].numOfPoints - 1;
    }

    assert(pQuery->pBlock != NULL);

    /* no need to load data, return directly */
    if (!loadData) {
      return DISK_DATA_LOADED;
    }

    int32_t ret =
        LoadDatablockOnDemand(&pQuery->pBlock[pQuery->slot], &pQuery->pFields[pQuery->slot], &pRuntimeEnv->blockStatus,
                              pRuntimeEnv, fileIndex, pQuery->slot, searchFn, true);

    if (ret != DISK_DATA_LOADED) {
      /*
       * if it is the last block of file, set current access position at the last point of the meter in this file,
       * in order to get the correct next access point,
       */
      return ret;
    }
  } else {  // data in cache
    return moveToNextBlockInCache(pRuntimeEnv, step, searchFn);
  }

  return DISK_DATA_LOADED;
}

static void doHandleFileBlockImpl(SQueryRuntimeEnv *pRuntimeEnv, SBlockInfo *pblockInfo, __block_search_fn_t searchFn,
                                  SData **sdata, int32_t *numOfRes, int32_t blockLoadStatus, int32_t *forwardStep) {
  SQuery *              pQuery = pRuntimeEnv->pQuery;
  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;

  int64_t start = taosGetTimestampUs();

  SCompBlock *pBlock = getDiskDataBlock(pQuery, pQuery->slot);
  *pblockInfo = getBlockBasicInfo(pBlock, BLK_FILE_BLOCK);

  TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

  if (blockLoadStatus == DISK_DATA_LOADED) {
    *forwardStep = applyFunctionsOnBlock(pRuntimeEnv, pblockInfo, primaryKeys, (char *)sdata,
                                         pQuery->pFields[pQuery->slot], searchFn, numOfRes);
  } else {
    *forwardStep = pblockInfo->size;
  }

  pSummary->fileTimeUs += (taosGetTimestampUs() - start);
}

static void doHandleCacheBlockImpl(SQueryRuntimeEnv *pRuntimeEnv, SBlockInfo *pblockInfo, __block_search_fn_t searchFn,
                                   int32_t *numOfRes, int32_t *forwardStep) {
  SQuery *              pQuery = pRuntimeEnv->pQuery;
  SMeterObj *           pMeterObj = pRuntimeEnv->pMeterObj;
  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;

  int64_t start = taosGetTimestampUs();

  // todo refactor getCacheDataBlock.
  //#ifdef _CACHE_INVALID_TEST
  //        taosMsleep(20000);
  //#endif
  SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
  while (pBlock == NULL) {
    getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

    if (IS_DISK_DATA_BLOCK(pQuery)) {  // do check data block in file
      break;
    } else {
      pBlock = getCacheDataBlock(pMeterObj, pQuery, pQuery->slot);
    }
  }

  if (IS_DISK_DATA_BLOCK(pQuery)) {
    // start query position is located in file, try query on file block
    doHandleFileBlockImpl(pRuntimeEnv, pblockInfo, searchFn, pRuntimeEnv->colDataBuffer, numOfRes, DISK_DATA_LOADED,
                          forwardStep);
  } else {  // also query in cache block
    *pblockInfo = getBlockBasicInfo(pBlock, BLK_CACHE_BLOCK);

    TSKEY *primaryKeys = (TSKEY *)pBlock->offset[0];
    *forwardStep =
        applyFunctionsOnBlock(pRuntimeEnv, pblockInfo, primaryKeys, (char *)pBlock, NULL, searchFn, numOfRes);

    pSummary->cacheTimeUs += (taosGetTimestampUs() - start);
  }
}

static int64_t doScanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    LOAD_DATA = true;

  int32_t forwardStep = 0;
  int64_t cnt = 0;

  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  SData **   sdata = pRuntimeEnv->colDataBuffer;

  __block_search_fn_t   searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];
  int32_t               blockLoadStatus = DISK_DATA_LOADED;
  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // initial data block always be loaded
  SPositionInfo *pStartPos = &pRuntimeEnv->startPos;
  assert(pQuery->slot == pStartPos->slot);

  dTrace("QInfo:%p query start, qrange:%lld-%lld, lastkey:%lld, order:%d, start fileId:%d, slot:%d, pos:%d, bstatus:%d",
      GET_QINFO_ADDR(pQuery), pQuery->skey, pQuery->ekey, pQuery->lastKey, pQuery->order.order, pStartPos->fileId,
      pStartPos->slot, pStartPos->pos, pRuntimeEnv->blockStatus);

  while (1) {
    // check if query is killed or not set the status of query to pass the status check
    if (isQueryKilled(pQuery)) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return cnt;
    }

    int32_t    numOfRes = 0;
    SBlockInfo blockInfo = {0};

    if (IS_DISK_DATA_BLOCK(pQuery)) {
      doHandleFileBlockImpl(pRuntimeEnv, &blockInfo, searchFn, sdata, &numOfRes, blockLoadStatus, &forwardStep);
    } else {
      doHandleCacheBlockImpl(pRuntimeEnv, &blockInfo, searchFn, &numOfRes, &forwardStep);
    }

    dTrace("QInfo:%p check data block, brange:%lld-%lld, fileId:%d, slot:%d, pos:%d, bstatus:%d, rows:%d, checked:%d",
        GET_QINFO_ADDR(pQuery), blockInfo.keyFirst, blockInfo.keyLast, pQuery->fileId, pQuery->slot, pQuery->pos,
        pRuntimeEnv->blockStatus, blockInfo.size, forwardStep);

    // save last access position
    int32_t accessPos = pQuery->pos + (forwardStep - 1) * step;
    savePointPosition(&pRuntimeEnv->endPos, pQuery->fileId, pQuery->slot, accessPos);

    cnt += forwardStep;

    if (queryCompleteInBlock(pQuery, &blockInfo, forwardStep)) {
      int32_t nextPos = accessPos + step;

      /*
       * set the next access position, nextPos only required by
       * 1. interval query.
       * 2. multi-output query that may cause buffer overflow.
       */
      if (pQuery->nAggTimeInterval > 0 ||
          (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL) && pQuery->checkBufferInLoop == 1)) {
        //                if (IS_DISK_DATA_BLOCK(pQuery)) {
        //                    getNextQueryStartPos(&pRuntimeEnv->nextPos,
        //                    pQuery->pBlock, pQuery->slot, accessPos,
        //                    pRuntimeEnv);
        //                } else {
        //                    getNextQueryStartPosInCache(&pRuntimeEnv->nextPos,
        //                    pData, pQuery->slot, accessPos, pRuntimeEnv);
        //                }

        if (nextPos >= blockInfo.size || nextPos < 0) {
          moveToNextBlock(pRuntimeEnv, step, searchFn, !LOAD_DATA);

          // slot/pos/fileId is updated in moveToNextBlock function
          savePointPosition(&pRuntimeEnv->nextPos, pQuery->fileId, pQuery->slot, pQuery->pos);
        } else {
          savePointPosition(&pRuntimeEnv->nextPos, pQuery->fileId, pQuery->slot, accessPos + step);
        }
      }
      break;
    } else {  // query not completed, move to next block
      int64_t start = taosGetTimestampUs();

      blockLoadStatus = moveToNextBlock(pRuntimeEnv, step, searchFn, LOAD_DATA);
      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        savePointPosition(&pRuntimeEnv->nextPos, pQuery->fileId, pQuery->slot, pQuery->pos);
        setQueryStatus(pQuery, QUERY_COMPLETED);
        break;
      }

      int64_t delta = (taosGetTimestampUs() - start);
      if (IS_DISK_DATA_BLOCK(pQuery)) {
        pSummary->fileTimeUs += delta;
      } else {
        pSummary->cacheTimeUs += delta;
      }
    }

    // check next block
    void *pNextBlock = getGenericDataBlock(pMeterObj, pQuery, pQuery->slot);

    int32_t blockType = (IS_DISK_DATA_BLOCK(pQuery)) ? BLK_FILE_BLOCK : BLK_CACHE_BLOCK;
    blockInfo = getBlockBasicInfo(pNextBlock, blockType);
    if (!checkQueryRangeAgainstNextBlock(&blockInfo, pRuntimeEnv)) {
      break;
    }
  }  // while(1)

  return cnt;
}

static void updatelastkey(SQuery *pQuery, SMeterQueryInfo *pMeterQInfo) { pMeterQInfo->lastKey = pQuery->lastKey; }

void queryOnBlock(SMeterQuerySupportObj *pSupporter, int64_t *primaryKeys, int32_t blockStatus, char *data,
                  SBlockInfo *pBlockBasicInfo, SMeterDataInfo *pDataHeadInfoEx, SField *pFields,
                  __block_search_fn_t searchFn) {
  /* cache blocks may be assign to other meter, abort */
  if (pBlockBasicInfo->size <= 0) {
    return;
  }

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (pQuery->nAggTimeInterval == 0) {  // not interval query
    int32_t numOfRes = 0;
    applyFunctionsOnBlock(pRuntimeEnv, pBlockBasicInfo, primaryKeys, data, pFields, searchFn, &numOfRes);

    // note: only fixed number of output for each group by operation
    if (numOfRes > 0) {
      pSupporter->pResult[pDataHeadInfoEx->groupIdx].numOfRows = numOfRes;
    }

    /* used to decide the correct start position in cache after check all data in files */
    updatelastkey(pQuery, pDataHeadInfoEx->pMeterQInfo);
  } else {
    applyIntervalQueryOnBlock(pSupporter, pDataHeadInfoEx, data, primaryKeys, pBlockBasicInfo, blockStatus, pFields,
                              searchFn);
  }
}

/*
 * set tag value in SQLFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(tTagSchema *pTagSchema, int32_t tagColIdx, SMeterSidExtInfo *pMeterSidInfo,
                                 tVariant *param) {
  assert(tagColIdx >= 0);

  int32_t *fieldValueOffset = pTagSchema->colOffset;

  void *   pStr = (char *)pMeterSidInfo->tags + fieldValueOffset[tagColIdx];
  SSchema *pCol = &pTagSchema->pSchema[tagColIdx];

  tVariantDestroy(param);
  tVariantCreateB(param, pStr, pCol->bytes, pCol->type);
}

void vnodeSetTagValueInParam(tSidSet *pSidSet, SQueryRuntimeEnv *pRuntimeEnv, SMeterSidExtInfo *pMeterInfo) {
  SQuery *    pQuery = pRuntimeEnv->pQuery;
  tTagSchema *pTagSchema = pSidSet->pTagSchema;

  // set tag value, by which the results are aggregated.
  for (int32_t idx = 0; idx < pQuery->numOfOutputCols; ++idx) {
    SColIndexEx *pColEx = &pQuery->pSelectExpr[idx].pBase.colInfo;
    if (!pColEx->isTag) {
      continue;
    }

    doSetTagValueInParam(pTagSchema, pColEx->colIdx, pMeterInfo, &pRuntimeEnv->pCtx[idx].intermediateBuf[3]);
  }
}

static void doMerge(SQuery *pQuery, SQueryRuntimeEnv *pRuntimeEnv, int64_t timestamp, tFilePage *inputSrc,
                    int32_t inputIdx, int16_t *offset, int32_t maxRow, bool mergeFlag) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t funcId = pQuery->pSelectExpr[i].pBase.functionId;
    if (!mergeFlag) {
      pCtx[i].aOutputBuf = pCtx[i].aOutputBuf + pCtx[i].outputBytes;
      aAggs[funcId].init(&pCtx[i]);
    }
    pCtx[i].hasNullValue = true;
    pCtx[i].nStartQueryTimestamp = timestamp;
    pCtx[i].aInputElemBuf = inputSrc->data + (offset[i] * maxRow) + pCtx[i].outputBytes * inputIdx;

    aAggs[funcId].distMergeFunc(&pCtx[i]);
  }
}

static void printBinaryData(int32_t functionId, char *data, int32_t srcDataType) {
  if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_FIRST_DST ||
      functionId == TSDB_FUNC_LAST_DST) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_BINARY:
        printf("%ld,%s\t", *(TSKEY *)data, (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%ld,%d\t", *(TSKEY *)data, *(int8_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%ld,%d\t", *(TSKEY *)data, *(int16_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%ld,%ld\t", *(TSKEY *)data, *(TSKEY *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%ld,%d\t", *(TSKEY *)data, *(int32_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%ld,%f\t", *(TSKEY *)data, *(float *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%ld,%lf\t", *(TSKEY *)data, *(double *)(data + TSDB_KEYSIZE + 1));
        break;
    }
  } else if (functionId == TSDB_FUNC_AVG_DST) {
    printf("%lf,%d\t", *(double *)data, *(int32_t *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_SPREAD_DST) {
    printf("%lf,%lf\t", *(double *)data, *(double *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_WAVG_DST) {
    data += 1;
    printf("%lf,%ld,%ld,%ld\t", *(double *)data, *(int64_t *)(data + 8), *(int64_t *)(data + 16),
           *(int64_t *)(data + 24));
  } else if (functionId == TSDB_FUNC_MIN_DST || functionId == TSDB_FUNC_MAX_DST) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%d\t", *(int8_t *)data);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d\t", *(int16_t *)data);
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%ld\t", *(int64_t *)data);
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%d\t", *(int *)data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f\t", *(float *)data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%f\t", *(float *)data);
        break;
    }
  } else if (functionId == TSDB_FUNC_SUM_DST) {
    if (srcDataType == TSDB_DATA_TYPE_FLOAT || srcDataType == TSDB_DATA_TYPE_DOUBLE) {
      printf("%lf\t", *(float *)data);
    } else {
      printf("%ld\t", *(int64_t *)data);
    }
  } else {
    printf("%s\t", data);
  }
}

void UNUSED_FUNC displayInterResult(SData **pdata, SQuery *pQuery, int32_t numOfRows) {
  int32_t numOfCols = pQuery->numOfOutputCols;
  printf("metric query intern-result, total:%d\n", numOfRows);

  SQInfo *   pQInfo = (SQInfo *)(GET_QINFO_ADDR(pQuery));
  SMeterObj *pMeterObj = pQInfo->pObj;

  for (int32_t j = 0; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      switch (pQuery->pSelectExpr[i].resType) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t colIdx = pQuery->pSelectExpr[i].pBase.colInfo.colIdx;
          int32_t type = 0;

          if (pQuery->pSelectExpr[i].pBase.colInfo.isTag) {
            type = pQuery->pSelectExpr[i].resType;
          } else {
            type = pMeterObj->schema[colIdx].type;
          }
          printBinaryData(pQuery->pSelectExpr[i].pBase.functionId, pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j,
                          type);
          break;
        }
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%ld\t", *(int64_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
      }
    }
    printf("\n");
  }
}

static tFilePage *getFilePage(SMeterQuerySupportObj *pSupporter, int32_t pageId) {
  assert(pageId <= pSupporter->lastPageId && pageId >= 0);
  return (tFilePage *)(pSupporter->meterOutputMMapBuf + DEFAULT_INTERN_BUF_SIZE * pageId);
}

static tFilePage *getMeterDataPage(SMeterQuerySupportObj *pSupporter, SMeterDataInfo *pInfoEx, int32_t pageId) {
  SMeterQueryInfo *pInfo = pInfoEx->pMeterQInfo;
  if (pageId >= pInfo->numOfPages) {
    return NULL;
  }

  int32_t realId = pInfo->pageList[pageId];
  return getFilePage(pSupporter, realId);
}

typedef struct Position {
  int32_t pageIdx;
  int32_t rowIdx;
} Position;

typedef struct SCompSupporter {
  SMeterDataInfo **      pInfoEx;
  Position *             pPosition;
  SMeterQuerySupportObj *pSupporter;
} SCompSupporter;

int64_t getCurrentTimestamp(SCompSupporter *pSupportor, int32_t meterIdx) {
  Position * pPos = &pSupportor->pPosition[meterIdx];
  tFilePage *pPage = getMeterDataPage(pSupportor->pSupporter, pSupportor->pInfoEx[meterIdx], pPos->pageIdx);
  return *(int64_t *)(pPage->data + TSDB_KEYSIZE * pPos->rowIdx);
}

int32_t meterResultComparator(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(int32_t *)pLeft;
  int32_t right = *(int32_t *)pRight;

  SCompSupporter *supportor = (SCompSupporter *)param;

  Position leftPos = supportor->pPosition[left];
  Position rightPos = supportor->pPosition[right];

  /* left source is exhausted */
  if (leftPos.pageIdx == -1 && leftPos.rowIdx == -1) {
    return 1;
  }

  /* right source is exhausted*/
  if (rightPos.pageIdx == -1 && rightPos.rowIdx == -1) {
    return -1;
  }

  tFilePage *pPageLeft = getMeterDataPage(supportor->pSupporter, supportor->pInfoEx[left], leftPos.pageIdx);
  int64_t    leftTimestamp = *(int64_t *)(pPageLeft->data + TSDB_KEYSIZE * leftPos.rowIdx);

  tFilePage *pPageRight = getMeterDataPage(supportor->pSupporter, supportor->pInfoEx[right], rightPos.pageIdx);
  int64_t    rightTimestamp = *(int64_t *)(pPageRight->data + TSDB_KEYSIZE * rightPos.rowIdx);

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  return leftTimestamp > rightTimestamp ? 1 : -1;
}

int32_t mergeMetersResultToOneGroups(SMeterQuerySupportObj *pSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int64_t st = taosGetTimestampMs();

  while (pSupporter->subgroupIdx < pSupporter->pSidSet->numOfSubSet) {
    int32_t start = pSupporter->pSidSet->starterPos[pSupporter->subgroupIdx];
    int32_t end = pSupporter->pSidSet->starterPos[pSupporter->subgroupIdx + 1];

    int32_t ret =
        doMergeMetersResultsToGroupRes(pSupporter, pQuery, pRuntimeEnv, pSupporter->pMeterDataInfo, start, end);
    pSupporter->subgroupIdx += 1;

    /* this group generates at least one result, return results */
    if (ret > 0) {
      break;
    }

    assert(pSupporter->numOfGroupResultPages == 0);
    dTrace("QInfo:%p no result in group %d, continue", GET_QINFO_ADDR(pQuery), pSupporter->subgroupIdx - 1);
  }

  dTrace("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%lldms",
      GET_QINFO_ADDR(pQuery), pSupporter->subgroupIdx - 1, pSupporter->pSidSet->numOfSubSet, taosGetTimestampMs() - st);

  return pSupporter->numOfGroupResultPages;
}

void copyResToQueryResultBuf(SMeterQuerySupportObj *pSupporter, SQuery *pQuery) {
  if (pSupporter->offset == pSupporter->numOfGroupResultPages) {
    pSupporter->numOfGroupResultPages = 0;

    // current results of group has been sent to client, try next group
    mergeMetersResultToOneGroups(pSupporter);

    // set current query completed
    if (pSupporter->numOfGroupResultPages == 0 && pSupporter->subgroupIdx == pSupporter->pSidSet->numOfSubSet) {
      pSupporter->meterIdx = pSupporter->pSidSet->numOfSids;
      return;
    }
  }

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  char *            pStart = pSupporter->meterOutputMMapBuf + DEFAULT_INTERN_BUF_SIZE * (pSupporter->lastPageId + 1) +
                 pSupporter->groupResultSize * pSupporter->offset;

  uint64_t numOfElem = ((tFilePage *)pStart)->numOfElems;
  assert(numOfElem <= pQuery->pointsToRead);

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    memcpy(pQuery->sdata[i], pStart, pRuntimeEnv->pCtx[i].outputBytes * numOfElem + sizeof(tFilePage));
    pStart += pRuntimeEnv->pCtx[i].outputBytes * pQuery->pointsToRead + sizeof(tFilePage);
  }

  pQuery->pointsRead += numOfElem;
  pSupporter->offset += 1;
}

int32_t doMergeMetersResultsToGroupRes(SMeterQuerySupportObj *pSupporter, SQuery *pQuery, SQueryRuntimeEnv *pRuntimeEnv,
                                       SMeterDataInfo *pMeterHeadDataInfo, int32_t start, int32_t end) {
  if (pSupporter->groupResultSize == 0) {
    /* calculate the maximum required space */
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      pSupporter->groupResultSize += sizeof(tFilePage) + pQuery->pointsToRead * pRuntimeEnv->pCtx[i].outputBytes;
    }
  }

  tFilePage **     buffer = (tFilePage **)pQuery->sdata;
  Position *       posArray = calloc(1, sizeof(Position) * (end - start));
  SMeterDataInfo **pValidMeter = malloc(POINTER_BYTES * (end - start));

  int32_t numOfMeters = 0;
  for (int32_t i = start; i < end; ++i) {
    if (pMeterHeadDataInfo[i].pMeterQInfo->numOfPages > 0 && pMeterHeadDataInfo[i].pMeterQInfo->numOfRes > 0) {
      pValidMeter[numOfMeters] = &pMeterHeadDataInfo[i];
      // set the merge start position: page:0, index:0
      posArray[numOfMeters].pageIdx = 0;
      posArray[numOfMeters++].rowIdx = 0;
    }
  }

  if (numOfMeters == 0) {
    tfree(posArray);
    tfree(pValidMeter);
    assert(pSupporter->numOfGroupResultPages == 0);
    return 0;
  }

  SCompSupporter  cs = {pValidMeter, posArray, pSupporter};  // 1 == ascending order
  SLoserTreeInfo *pTree = NULL;

  tLoserTreeCreate(&pTree, numOfMeters, &cs, meterResultComparator);

  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  resetMergeResultBuf(pQuery, pCtx);

  int64_t lastTimestamp = -1;

  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t    pos = pTree->pNode[0].index;
    Position * position = &cs.pPosition[pos];
    tFilePage *pPage = getMeterDataPage(cs.pSupporter, pValidMeter[pos], position->pageIdx);

    int64_t ts = getCurrentTimestamp(&cs, pos);
    if (ts == lastTimestamp) {  // merge with the last one
      doMerge(pQuery, pRuntimeEnv, ts, pPage, position->rowIdx, pRuntimeEnv->offset, pRuntimeEnv->numOfRowsPerPage,
              true);
    } else {
      // copy data to disk buffer
      if (buffer[0]->numOfElems == pQuery->pointsToRead) {
        flushFromResultBuf(pSupporter, pQuery, pRuntimeEnv);
        resetMergeResultBuf(pQuery, pCtx);
      }

      pPage = getMeterDataPage(cs.pSupporter, pValidMeter[pos], position->pageIdx);
      if (pPage->numOfElems <= 0) {  // current source data page is empty
        // do nothing
      } else {
        doMerge(pQuery, pRuntimeEnv, ts, pPage, position->rowIdx, pRuntimeEnv->offset, pRuntimeEnv->numOfRowsPerPage,
                false);
        buffer[0]->numOfElems += 1;
      }
    }

    lastTimestamp = ts;

    if (cs.pPosition[pos].rowIdx >= pPage->numOfElems - 1) {
      cs.pPosition[pos].rowIdx = 0;
      cs.pPosition[pos].pageIdx += 1;  // try next page

      /*check if current page is empty or not. if it is empty, ignore it and try
       * next*/
      if (cs.pPosition[pos].pageIdx <= cs.pInfoEx[pos]->pMeterQInfo->numOfPages - 1) {
        tFilePage *newPage = getMeterDataPage(cs.pSupporter, pValidMeter[pos], position->pageIdx);
        if (newPage->numOfElems <= 0) {
          /* if current source data page is null, it must be the last page of
           * source output page */
          cs.pPosition[pos].pageIdx += 1;
          assert(cs.pPosition[pos].pageIdx >= cs.pInfoEx[pos]->pMeterQInfo->numOfPages - 1);
        }
      }

      /* the following code must be executed if current source pages are
       * exhausted */
      if (cs.pPosition[pos].pageIdx >= cs.pInfoEx[pos]->pMeterQInfo->numOfPages) {
        cs.pPosition[pos].pageIdx = -1;
        cs.pPosition[pos].rowIdx = -1;

        /* all input sources are exhausted */
        if (--numOfMeters == 0) {
          break;
        }
      }
    } else {
      cs.pPosition[pos].rowIdx += 1;
    }

    tLoserTreeAdjust(pTree, pos + pTree->numOfEntries);
  }

  if (buffer[0]->numOfElems != 0) {  // there are data in buffer
    flushFromResultBuf(pSupporter, pQuery, pRuntimeEnv);
  }

  int64_t endt = taosGetTimestampMs();

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif

  dTrace("QInfo:%p result merge completed, elapsed time:%lld ms", GET_QINFO_ADDR(pQuery), endt - startt);
  tfree(pTree);
  tfree(pValidMeter);
  tfree(posArray);

  pSupporter->offset = 0;

  return pSupporter->numOfGroupResultPages;
}

static void extendDiskBuf(SMeterQuerySupportObj *pSupporter, int32_t numOfPages) {
  assert(pSupporter->numOfPages * DEFAULT_INTERN_BUF_SIZE == pSupporter->bufSize);

  munmap(pSupporter->meterOutputMMapBuf, pSupporter->bufSize);
  pSupporter->numOfPages = numOfPages;

  // disk-based output buffer is exhausted, try to extend the disk-based buffer
  int32_t ret = ftruncate(pSupporter->meterOutputFd, pSupporter->numOfPages * DEFAULT_INTERN_BUF_SIZE);
  if (ret != 0) {
    perror("error in allocate the disk-based buffer");
    return;
  }

  pSupporter->bufSize = pSupporter->numOfPages * DEFAULT_INTERN_BUF_SIZE;
  pSupporter->meterOutputMMapBuf =
      mmap(NULL, pSupporter->bufSize, PROT_READ | PROT_WRITE, MAP_SHARED, pSupporter->meterOutputFd, 0);
}

void flushFromResultBuf(SMeterQuerySupportObj *pSupporter, const SQuery *pQuery, const SQueryRuntimeEnv *pRuntimeEnv) {
  int32_t numOfMeterResultBufPages = pSupporter->lastPageId + 1;
  int64_t dstSize = numOfMeterResultBufPages * DEFAULT_INTERN_BUF_SIZE +
                    pSupporter->groupResultSize * (pSupporter->numOfGroupResultPages + 1);

  int32_t requiredPages = pSupporter->numOfPages;
  if (requiredPages * DEFAULT_INTERN_BUF_SIZE < dstSize) {
    while (requiredPages * DEFAULT_INTERN_BUF_SIZE < dstSize) {
      requiredPages += pSupporter->numOfMeters;
    }

    extendDiskBuf(pSupporter, requiredPages);
  }

  char *lastPosition = pSupporter->meterOutputMMapBuf + DEFAULT_INTERN_BUF_SIZE * numOfMeterResultBufPages +
                       pSupporter->groupResultSize * pSupporter->numOfGroupResultPages;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t size = pRuntimeEnv->pCtx[i].outputBytes * pQuery->sdata[0]->len + sizeof(tFilePage);
    memcpy(lastPosition, pQuery->sdata[i], size);

    lastPosition += pRuntimeEnv->pCtx[i].outputBytes * pQuery->pointsToRead + sizeof(tFilePage);
  }

  pSupporter->numOfGroupResultPages += 1;
}

void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx) {
  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    pCtx[k].aOutputBuf = pQuery->sdata[k]->data - pCtx[k].outputBytes;
    pCtx[k].size = 1;
    pCtx[k].startOffset = 0;
    pCtx[k].numOfIteratedElems = 0;
    pQuery->sdata[k]->len = 0;
  }
}

void setMeterDataInfo(SMeterDataInfo *pMeterDataInfo, SMeterObj *pMeterObj, int32_t meterIdx, int32_t groupId) {
  pMeterDataInfo->pMeterObj = pMeterObj;
  pMeterDataInfo->groupIdx = groupId;
  pMeterDataInfo->meterOrderIdx = meterIdx;
}

void doCloseAllOpenedResults(SMeterQuerySupportObj *pSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  /* for interval query, close all unclosed results */
  if (pQuery->nAggTimeInterval > 0) {
    SMeterDataInfo *pMeterInfo = pSupporter->pMeterDataInfo;
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      if (pMeterInfo[i].pMeterQInfo != NULL && pMeterInfo[i].pMeterQInfo->lastResRows > 0) {
        int32_t index = pMeterInfo[i].meterOrderIdx;

        pRuntimeEnv->pMeterObj = getMeterObj(pSupporter->pMeterObj, pSupporter->pSidSet->pSids[index]->sid);
        assert(pRuntimeEnv->pMeterObj == pMeterInfo[i].pMeterObj);

        setIntervalQueryExecutionContext(pSupporter, i, pMeterInfo[i].pMeterQInfo);
        saveResult(pSupporter, pMeterInfo[i].pMeterQInfo, pMeterInfo[i].pMeterQInfo->lastResRows);
      }
    }
  }
}

void disableFunctForSuppleScanAndSetSortOrder(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1;
    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;

    if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSQL_SO_DESC) ||
        ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSQL_SO_ASC)) {
      pRuntimeEnv->go[i] = true;
    } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
      pRuntimeEnv->go[i] = false;
    }
  }

  pQuery->order.order = pQuery->order.order ^ 1;
}

void enableFunctForMasterScan(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1;
    pRuntimeEnv->go[i] = true;
  }

  pQuery->order.order = (pQuery->order.order ^ 1);
}

tFilePage **createInMemGroupResultBuf(SQLFunctionCtx *pCtx, int32_t nOutputCols, int32_t nAlloc) {
  tFilePage **pTempBuf = malloc(POINTER_BYTES * nOutputCols);
  for (int32_t i = 0; i < nOutputCols; ++i) {
    pTempBuf[i] = malloc(sizeof(tFilePage) + pCtx[i].outputBytes * nAlloc);
    pTempBuf[i]->numOfElems = 0;
  }
  return pTempBuf;
}

void destroyBuf(tFilePage **pBuf, int32_t nOutputCols) {
  for (int32_t i = 0; i < nOutputCols; ++i) {
    free(pBuf[i]);
  }

  free(pBuf);
}

void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t rows = pRuntimeEnv->pMeterObj->pointsPerFileBlock;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].numOfOutputElems = 0;
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data;
    } else {  // point to the last position of output buffer for desc query
      pRuntimeEnv->pCtx[i].aOutputBuf = pQuery->sdata[i]->data + (rows - 1) * pRuntimeEnv->pCtx[i].outputBytes;
    }
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pRuntimeEnv->pCtx[i].ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }
  }
}

void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
    assert(functionId != TSDB_FUNC_DIFF);

    // set next output position
    if (IS_OUTER_FORWARD(aAggs[functionId].nStatus)) {
      pRuntimeEnv->pCtx[j].aOutputBuf += pRuntimeEnv->pCtx[j].outputBytes * output * factor;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      /*
       * NOTE: for top/bottom query, the value of first column of output (timestamp) are assigned
       * in the procedure of top/bottom routine
       * the output buffer in top/bottom routine is ptsOutputBuf, so we need to forward the output buffer
       *
       * diff function is handled in multi-output function
       */
      pRuntimeEnv->pCtx[j].ptsOutputBuf += TSDB_KEYSIZE * output * factor;
    }
  }
}

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
    aAggs[functionId].init(&pRuntimeEnv->pCtx[j]);
  }
}

void cleanCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t rows = pRuntimeEnv->pMeterObj->pointsPerFileBlock;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    memset(pQuery->sdata[i]->data, 0, (size_t)pQuery->pSelectExpr[i].resBytes * rows);
  }
}

void doSkipResults(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->pointsRead == 0 || pQuery->limit.offset == 0) {
    return;
  }

  if (pQuery->pointsRead <= pQuery->limit.offset) {
    pQuery->limit.offset -= pQuery->pointsRead;

    pQuery->pointsRead = 0;
    pQuery->pointsOffset = pQuery->pointsToRead;  // clear all data in result buffer

    resetCtxOutputBuf(pRuntimeEnv);
  } else {
    int32_t numOfSkip = (int32_t)pQuery->limit.offset;
    int32_t size = pQuery->pointsRead;

    pQuery->pointsRead -= numOfSkip;

    int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;

      if (QUERY_IS_ASC_QUERY(pQuery)) {
        memmove(pQuery->sdata[i]->data, pQuery->sdata[i]->data + bytes * numOfSkip, pQuery->pointsRead * bytes);
      } else {  // DESC query
        int32_t maxrows = pQuery->pointsToRead;

        memmove(pQuery->sdata[i]->data + (maxrows - pQuery->pointsRead) * bytes,
                pQuery->sdata[i]->data + (maxrows - size) * bytes, pQuery->pointsRead * bytes);
      }

      pRuntimeEnv->pCtx[i].aOutputBuf -= bytes * numOfSkip * step;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf -= TSDB_KEYSIZE * numOfSkip * step;
      }
    }

    pQuery->limit.offset = 0;
  }
}

/**
 * move remain data to the start position of output buffer
 * @param pRuntimeEnv
 */
void moveDescOrderResultsToFront(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t maxrows = pQuery->pointsToRead;

  if (pQuery->pointsRead > 0 && pQuery->pointsRead < maxrows && !QUERY_IS_ASC_QUERY(pQuery)) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      memmove(pQuery->sdata[i]->data, pQuery->sdata[i]->data + (maxrows - pQuery->pointsRead) * bytes,
              pQuery->pointsRead * bytes);
    }
  }
}

typedef struct SQueryStatus {
  SPositionInfo start;
  SPositionInfo next;
  SPositionInfo end;

  TSKEY  skey;
  TSKEY  ekey;
  int8_t overStatus;
  TSKEY  lastKey;
} SQueryStatus;

static void queryStatusSave(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pStatus->overStatus = pQuery->over;
  pStatus->lastKey = pQuery->lastKey;

  pStatus->start = pRuntimeEnv->startPos;
  pStatus->next = pRuntimeEnv->nextPos;
  pStatus->end = pRuntimeEnv->endPos;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SWAP(pQuery->skey, pQuery->ekey, TSKEY);
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->startPos = pRuntimeEnv->endPos;
}

static void queryStatusRestore(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SWAP(pQuery->skey, pQuery->ekey, TSKEY);

  pQuery->lastKey = pStatus->lastKey;

  pQuery->over = pStatus->overStatus;

  pRuntimeEnv->startPos = pStatus->start;
  pRuntimeEnv->nextPos = pStatus->next;
  pRuntimeEnv->endPos = pStatus->end;
}

static void doSupplementaryScan(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *     pQuery = pRuntimeEnv->pQuery;
  SQueryStatus qStatus = {0};

  if (!needSupplementaryScan(pQuery)) {
    return;
  }

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);

  // usually this load operation will incure load disk block operation
  TSKEY endKey = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->endPos);
  assert((QUERY_IS_ASC_QUERY(pQuery) && endKey <= pQuery->ekey) ||
         (!QUERY_IS_ASC_QUERY(pQuery) && endKey >= pQuery->ekey));

  /* close necessary function execution during supplementary scan */
  disableFunctForSuppleScanAndSetSortOrder(pRuntimeEnv, pQuery->order.order);
  queryStatusSave(pRuntimeEnv, &qStatus);

  doScanAllDataBlocks(pRuntimeEnv);

  // set the correct start position, and load the corresponding block in buffer if required.
  TSKEY actKey = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->startPos);
  assert((QUERY_IS_ASC_QUERY(pQuery) && actKey >= pQuery->skey) ||
         (!QUERY_IS_ASC_QUERY(pQuery) && actKey <= pQuery->skey));

  queryStatusRestore(pRuntimeEnv, &qStatus);
  enableFunctForMasterScan(pRuntimeEnv, pQuery->order.order);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

void setQueryStatus(SQuery *pQuery, int8_t status) {
  if (status == QUERY_NOT_COMPLETED) {
    pQuery->over = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    pQuery->over &= (~QUERY_NOT_COMPLETED);
    pQuery->over |= status;
  }
}

void vnodeScanAllData(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  /* store the start query position */
  savePointPosition(&pRuntimeEnv->startPos, pQuery->fileId, pQuery->slot, pQuery->pos);

  while (1) {
    doScanAllDataBlocks(pRuntimeEnv);

    /* applied to agg functions (e.g., stddev) */
    bool more = false;
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      pRuntimeEnv->go[j] = aAggs[pQuery->pSelectExpr[j].pBase.functionId].xNextStep(&pRuntimeEnv->pCtx[j]);
      more |= pRuntimeEnv->go[j];
    }

    if (!more) {
      break;
    }

    // set the correct start position, and load the corresponding block in
    // buffer if required.
    TSKEY actKey = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->startPos);
    assert((QUERY_IS_ASC_QUERY(pQuery) && actKey >= pQuery->skey) ||
           (!QUERY_IS_ASC_QUERY(pQuery) && actKey <= pQuery->skey));

    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pQuery->lastKey = pQuery->skey;

    /* check if query is killed or not */
    if (isQueryKilled(pQuery)) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return;
    }
  }

  doSupplementaryScan(pRuntimeEnv);

  /* reset status code */
  memset(pRuntimeEnv->go, true, (size_t)pQuery->numOfOutputCols);
}

void doFinalizeResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    aAggs[pQuery->pSelectExpr[j].pBase.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
  }
}

int64_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int64_t maxOutput = 0;

  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (maxOutput < pRuntimeEnv->pCtx[j].numOfOutputElems) {
      maxOutput = pRuntimeEnv->pCtx[j].numOfOutputElems;
    }
  }
  return maxOutput;
}

/*
 * forward the query range for next interval query
 */
void forwardIntervalQueryRange(SMeterQuerySupportObj *pSupporter, SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  pQuery->ekey += (pQuery->nAggTimeInterval * factor);
  pQuery->skey = pQuery->ekey - (pQuery->nAggTimeInterval - 1) * factor;

  // boundary check
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (pQuery->skey > pSupporter->rawEKey) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return;
    }

    if (pQuery->ekey > pSupporter->rawEKey) {
      pQuery->ekey = pSupporter->rawEKey;
    }
  } else {
    if (pQuery->skey < pSupporter->rawEKey) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return;
    }

    if (pQuery->ekey < pSupporter->rawEKey) {
      pQuery->ekey = pSupporter->rawEKey;
    }
  }

  /* ensure the search in cache will return right position */
  pQuery->lastKey = pQuery->skey;

  TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
  if ((nextTimestamp > pSupporter->rawEKey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextTimestamp < pSupporter->rawEKey && !QUERY_IS_ASC_QUERY(pQuery)) ||
      Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
    return;
  }

  // bridge the gap in group by time function
  if ((nextTimestamp > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextTimestamp < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    getAlignedIntervalQueryRange(pQuery, nextTimestamp, pSupporter->rawSKey, pSupporter->rawEKey);
  }
}

static int32_t offsetComparator(const void *pLeft, const void *pRight) {
  SMeterDataInfo **pLeft1 = (SMeterDataInfo **)pLeft;
  SMeterDataInfo **pRight1 = (SMeterDataInfo **)pRight;

  if ((*pLeft1)->offsetInHeaderFile == (*pRight1)->offsetInHeaderFile) {
    return 0;
  }

  return ((*pLeft1)->offsetInHeaderFile > (*pRight1)->offsetInHeaderFile) ? 1 : -1;
}

/**
 *
 * @param pQInfo
 * @param fid
 * @param pQueryFileInfo
 * @param start
 * @param end
 * @param pMeterHeadDataInfo
 * @return
 */
SMeterDataInfo **vnodeFilterQualifiedMeters(SQInfo *pQInfo, int32_t vid, SQueryFileInfo *pQueryFileInfo,
                                            tSidSet *pSidSet, SMeterDataInfo *pMeterDataInfo, int32_t *numOfMeters) {
  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SMeterSidExtInfo **    pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;

  SVnodeObj *pVnode = &vnodeList[vid];

  char *  pHeaderData = pQueryFileInfo->pHeaderFileData;
  int32_t tmsize = sizeof(SCompHeader) * (pVnode->cfg.maxSessions) + sizeof(TSCKSUM);

  if (validateHeaderOffsetSegment(pQInfo, pQueryFileInfo->headerFilePath, vid, pHeaderData, tmsize) < 0) {
    /* file is corrupted, abort query in current file */
    *numOfMeters = 0;
    return 0;
  }

  int64_t          oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);
  SMeterDataInfo **pReqMeterDataInfo = malloc(POINTER_BYTES * pSidSet->numOfSids);

  int32_t groupId = 0;
  TSKEY   skey, ekey;

  for (int32_t i = 0; i < pSidSet->numOfSids; ++i) {  // load all meter meta info
    SMeterObj *pMeterObj = getMeterObj(pSupporter->pMeterObj, pMeterSidExtInfo[i]->sid);
    if (pMeterObj == NULL) {
      dError("QInfo:%p failed to find required sid:%d", pQInfo, pMeterSidExtInfo[i]->sid);
      continue;
    }

    if (i >= pSidSet->starterPos[groupId + 1]) {
      groupId += 1;
    }

    SMeterDataInfo *pOneMeterDataInfo = &pMeterDataInfo[i];
    if (pOneMeterDataInfo->pMeterObj == NULL) {
      setMeterDataInfo(pOneMeterDataInfo, pMeterObj, i, groupId);
    }

    /* restore possible exists new query range for this meter, which starts from cache */
    if (pOneMeterDataInfo->pMeterQInfo != NULL) {
      skey = pOneMeterDataInfo->pMeterQInfo->lastKey;
    } else {
      skey = pSupporter->rawSKey;
    }

    // query on disk data files, which actually starts from the lastkey
    ekey = pSupporter->rawEKey;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      assert(skey >= pSupporter->rawSKey);
      if (ekey < oldestKey || skey > pMeterObj->lastKeyOnFile) {
        continue;
      }
    } else {
      assert(skey <= pSupporter->rawSKey);
      if (skey < oldestKey || ekey > pMeterObj->lastKeyOnFile) {
        continue;
      }
    }

    int64_t headerOffset = TSDB_FILE_HEADER_LEN + sizeof(SCompHeader) * pMeterObj->sid;

    SCompHeader *compHeader = (SCompHeader *)(pHeaderData + headerOffset);
    comp_block_info_read_bytes += sizeof(SCompHeader);

    if (compHeader->compInfoOffset == 0) {
      continue;
    }

    if (compHeader->compInfoOffset < sizeof(SCompHeader) * pVnode->cfg.maxSessions + TSDB_FILE_HEADER_LEN ||
        compHeader->compInfoOffset > pQueryFileInfo->headFileSize) {
      dError("QInfo:%p vid:%d sid:%d id:%s, compInfoOffset:%d is not valid", pQuery, pMeterObj->vnode, pMeterObj->sid,
             pMeterObj->meterId, compHeader->compInfoOffset);
      continue;
    }

    pOneMeterDataInfo->offsetInHeaderFile = (uint64_t)compHeader->compInfoOffset;
    setMeterQueryInfo(pSupporter, pOneMeterDataInfo);

    pReqMeterDataInfo[*numOfMeters] = pOneMeterDataInfo;
    (*numOfMeters) += 1;
  }

  assert(*numOfMeters <= pSidSet->numOfSids);

  /* enable access sequentially */
  if (*numOfMeters > 1) {
    qsort(pReqMeterDataInfo, *numOfMeters, POINTER_BYTES, offsetComparator);
  }

  return pReqMeterDataInfo;
}

void setMeterQueryInfo(SMeterQuerySupportObj *pSupporter, SMeterDataInfo *pMeterDataInfo) {
  if (pMeterDataInfo->pMeterQInfo != NULL) {
    return;
  }

  pMeterDataInfo->pMeterQInfo = calloc(1, sizeof(SMeterQueryInfo));
  SMeterQueryInfo *pMQInfo = pMeterDataInfo->pMeterQInfo;

  pMQInfo->skey = pSupporter->rawSKey;
  pMQInfo->ekey = pSupporter->rawEKey;
  pMQInfo->lastKey = pSupporter->rawSKey;

  pMQInfo->numOfPages = 0;  // one page
  pMQInfo->numOfAlloc = INIT_ALLOCATE_DISK_PAGES;
  pMQInfo->pageList = calloc(pMQInfo->numOfAlloc, sizeof(uint32_t));
  pMQInfo->lastResRows = 0;
}

void incOutputPageId(SMeterQueryInfo *pMeterQInfo, uint32_t pageId) {
  if (pMeterQInfo->numOfPages >= pMeterQInfo->numOfAlloc) {
    pMeterQInfo->numOfAlloc = pMeterQInfo->numOfAlloc << 1;

    pMeterQInfo->pageList = realloc(pMeterQInfo->pageList, sizeof(uint32_t) * pMeterQInfo->numOfAlloc);
  }

  pMeterQInfo->pageList[pMeterQInfo->numOfPages++] = pageId;
}

void destroyMeterQueryInfo(SMeterQueryInfo *pMeterQInfo) {
  if (pMeterQInfo == NULL) {
    return;
  }

  free(pMeterQInfo->pageList);
  free(pMeterQInfo);
}

static void clearMeterDataBlockInfo(SMeterDataInfo *pMeterDataInfo) {
  tfree(pMeterDataInfo->pBlock);
  pMeterDataInfo->numOfBlocks = 0;
  pMeterDataInfo->start = 0;
}

static bool getValidDataBlocksRangeIndex(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, SCompBlock *pCompBlock,
                                         int64_t numOfBlocks, TSKEY minval, TSKEY maxval, int32_t *end) {
  SMeterObj *pMeterObj = pMeterDataInfo->pMeterObj;
  SQInfo *   pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  /*
   * search the possible blk that may satisfy the query condition always start from the min value, therefore,
   * the order is always ascending order
   */
  pMeterDataInfo->start = binarySearchForBlockImpl(pCompBlock, (int32_t)numOfBlocks, minval, TSQL_SO_ASC);
  if (minval > pCompBlock[pMeterDataInfo->start].keyLast || maxval < pCompBlock[pMeterDataInfo->start].keyFirst) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId);
    return false;
  }

  // incremental checks following blocks until whose time range does not overlap with the query range
  *end = pMeterDataInfo->start;
  while (*end <= (numOfBlocks - 1)) {
    if (pCompBlock[*end].keyFirst <= maxval && pCompBlock[*end].keyLast >= maxval) {
      break;
    }

    if (pCompBlock[*end].keyFirst > maxval) {
      *end -= 1;
      break;
    }

    if (*end == numOfBlocks - 1) {
      break;
    } else {
      ++(*end);
    }
  }

  return true;
}

static bool setValidDataBlocks(SMeterDataInfo *pMeterDataInfo, SCompBlock *pCompBlock, int32_t end) {
  int32_t size = (end - pMeterDataInfo->start) + 1;
  assert(size > 0);

  if (size != pMeterDataInfo->numOfBlocks) {
    char *tmp = realloc(pMeterDataInfo->pBlock, POINTER_BYTES * size);
    if (tmp == NULL) {
      return false;
    }

    pMeterDataInfo->pBlock = (SCompBlock **)tmp;
    pMeterDataInfo->numOfBlocks = size;
  }

  for (int32_t i = pMeterDataInfo->start, j = 0; i <= end; ++i, ++j) {
    pMeterDataInfo->pBlock[j] = &pCompBlock[i];
  }

  return true;
}

static bool setCurrentQueryRange(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, TSKEY endKey, TSKEY *minval,
                                 TSKEY *maxval) {
  SQInfo *         pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *      pMeterObj = pMeterDataInfo->pMeterObj;
  SMeterQueryInfo *pMeterQInfo = pMeterDataInfo->pMeterQInfo;

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    *minval = pMeterQInfo->lastKey;
    *maxval = endKey;
  } else {
    *minval = endKey;
    *maxval = pMeterQInfo->lastKey;
  }

  if (*minval > *maxval) {
    qTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files, qrange:%lld-%lld, lastKey:%lld",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
        pMeterQInfo->lastKey);
    return false;
  } else {
    qTrace("QInfo:%p vid:%d sid:%d id:%s, query in files, qrange:%lld-%lld, lastKey:%lld",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
        pMeterQInfo->lastKey);
    return true;
  }
}

/**
 *
 * @param pQuery
 * @param pHeaderData
 * @param numOfMeters
 * @param pMeterDataInfo
 * @return
 */
uint32_t getDataBlocksForMeters(SMeterQuerySupportObj *pSupporter, SQuery *pQuery, char *pHeaderData,
                                int32_t numOfMeters, SQueryFileInfo *pQueryFileInfo, SMeterDataInfo **pMeterDataInfo) {
  uint32_t              numOfBlocks = 0;
  SQInfo *              pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SQueryCostStatistics *pSummary = &pSupporter->runtimeEnv.summary;

  TSKEY minval, maxval;

  // sequentially scan this header file to extract the compHeader info
  for (int32_t j = 0; j < numOfMeters; ++j) {
    SMeterObj *pMeterObj = pMeterDataInfo[j]->pMeterObj;

    SCompInfo *compInfo = (SCompInfo *)(pHeaderData + pMeterDataInfo[j]->offsetInHeaderFile);
    int32_t    ret = validateCompBlockInfoSegment(pQInfo, pQueryFileInfo->headerFilePath, pMeterObj->vnode, compInfo,
                                               pMeterDataInfo[j]->offsetInHeaderFile);
    if (ret != 0) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      continue;
    }

    if (compInfo->numOfBlocks <= 0 || compInfo->uid != pMeterDataInfo[j]->pMeterObj->uid) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      continue;
    }

    int32_t     size = compInfo->numOfBlocks * sizeof(SCompBlock);
    SCompBlock *pCompBlock = (SCompBlock *)((char *)compInfo + sizeof(SCompInfo));

    int64_t st = taosGetTimestampUs();

    // check compblock integrity
    TSCKSUM checksum = *(TSCKSUM *)((char *)compInfo + sizeof(SCompInfo) + size);
    ret = validateCompBlockSegment(pQInfo, pQueryFileInfo->headerFilePath, compInfo, (char *)pCompBlock,
                                   pMeterObj->vnode, checksum);
    if (ret < 0) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      continue;
    }

    int64_t et = taosGetTimestampUs();

    pSummary->readCompInfo++;
    pSummary->totalCompInfoSize += (size + sizeof(SCompInfo) + sizeof(TSCKSUM));
    pSummary->loadCompInfoUs += (et - st);

    if (!setCurrentQueryRange(pMeterDataInfo[j], pQuery, pSupporter->rawEKey, &minval, &maxval)) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      continue;
    }

    int32_t end = 0;
    if (!getValidDataBlocksRangeIndex(pMeterDataInfo[j], pQuery, pCompBlock, compInfo->numOfBlocks, minval, maxval,
                                      &end)) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      continue;
    }

    if (!setValidDataBlocks(pMeterDataInfo[j], pCompBlock, end)) {
      clearMeterDataBlockInfo(pMeterDataInfo[j]);
      pQInfo->killed = 1;  // todo set query kill, abort current query since no
                           // memory available
      return 0;
    }

    qTrace("QInfo:%p vid:%d sid:%d id:%s, startIndex:%d, %d blocks qualified", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pMeterDataInfo[j]->start, pMeterDataInfo[j]->numOfBlocks);

    numOfBlocks += pMeterDataInfo[j]->numOfBlocks;
  }

  return numOfBlocks;
}

static void freeDataBlockFieldInfo(SMeterDataBlockInfoEx *pDataBlockInfoEx, int32_t len) {
  for (int32_t i = 0; i < len; ++i) {
    tfree(pDataBlockInfoEx[i].pBlock.fields);
  }
}

void freeMeterBlockInfoEx(SMeterDataBlockInfoEx *pDataBlockInfoEx, int32_t len) {
  freeDataBlockFieldInfo(pDataBlockInfoEx, len);
  tfree(pDataBlockInfoEx);
}

typedef struct SBlockOrderSupporter {
  int32_t                 numOfMeters;
  SMeterDataBlockInfoEx **pDataBlockInfoEx;
  int32_t *               blockIndexArray;
  int32_t *               numOfBlocksPerMeter;
} SBlockOrderSupporter;

static int32_t blockAccessOrderComparator(const void *pLeft, const void *pRight, void *param) {
  int32_t leftTableIndex = *(int32_t *)pLeft;
  int32_t rightTableIndex = *(int32_t *)pRight;

  SBlockOrderSupporter *pSupporter = (SBlockOrderSupporter *)param;

  int32_t leftTableBlockIndex = pSupporter->blockIndexArray[leftTableIndex];
  int32_t rightTableBlockIndex = pSupporter->blockIndexArray[rightTableIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerMeter[leftTableIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerMeter[rightTableIndex]) {
    /* right block is empty */
    return -1;
  }

  SMeterDataBlockInfoEx *pLeftBlockInfoEx = &pSupporter->pDataBlockInfoEx[leftTableIndex][leftTableBlockIndex];
  SMeterDataBlockInfoEx *pRightBlockInfoEx = &pSupporter->pDataBlockInfoEx[rightTableIndex][rightTableBlockIndex];

  //    assert(pLeftBlockInfoEx->pBlock.compBlock->offset != pRightBlockInfoEx->pBlock.compBlock->offset);
  if (pLeftBlockInfoEx->pBlock.compBlock->offset == pRightBlockInfoEx->pBlock.compBlock->offset &&
      pLeftBlockInfoEx->pBlock.compBlock->last == pRightBlockInfoEx->pBlock.compBlock->last) {
    // todo add more information
    dError("error in header file, two block with same offset:%p", pLeftBlockInfoEx->pBlock.compBlock->offset);
  }

  return pLeftBlockInfoEx->pBlock.compBlock->offset > pRightBlockInfoEx->pBlock.compBlock->offset ? 1 : -1;
}

int32_t createDataBlocksInfoEx(SMeterDataInfo **pMeterDataInfo, int32_t numOfMeters,
                               SMeterDataBlockInfoEx **pDataBlockInfoEx, int32_t numOfCompBlocks,
                               int32_t *nAllocBlocksInfoSize, int64_t addr) {
  /* release allocated memory first */
  freeDataBlockFieldInfo(*pDataBlockInfoEx, *nAllocBlocksInfoSize);

  if (*nAllocBlocksInfoSize == 0 || *nAllocBlocksInfoSize < numOfCompBlocks) {
    *pDataBlockInfoEx =
        (SMeterDataBlockInfoEx *)realloc((*pDataBlockInfoEx), sizeof(SMeterDataBlockInfoEx) * numOfCompBlocks);
    memset((*pDataBlockInfoEx), 0, sizeof(SMeterDataBlockInfoEx) * numOfCompBlocks);
    *nAllocBlocksInfoSize = numOfCompBlocks;
  }

  SBlockOrderSupporter supporter = {0};
  supporter.numOfMeters = numOfMeters;
  supporter.numOfBlocksPerMeter = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.blockIndexArray = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.pDataBlockInfoEx = calloc(1, POINTER_BYTES * numOfMeters);

  int32_t cnt = 0;
  int32_t numOfQualMeters = 0;
  for (int32_t j = 0; j < numOfMeters; ++j) {
    if (pMeterDataInfo[j]->numOfBlocks == 0) {
      continue;
    }

    SCompBlock **pBlock = pMeterDataInfo[j]->pBlock;
    supporter.numOfBlocksPerMeter[numOfQualMeters] = pMeterDataInfo[j]->numOfBlocks;
    supporter.pDataBlockInfoEx[numOfQualMeters] =
        calloc(1, sizeof(SMeterDataBlockInfoEx) * pMeterDataInfo[j]->numOfBlocks);

    for (int32_t k = 0; k < pMeterDataInfo[j]->numOfBlocks; ++k) {
      SMeterDataBlockInfoEx *pInfoEx = &supporter.pDataBlockInfoEx[numOfQualMeters][k];

      pInfoEx->pBlock.compBlock = pBlock[k];
      pInfoEx->pBlock.fields = NULL;

      pInfoEx->pMeterDataInfo = pMeterDataInfo[j];
      pInfoEx->groupIdx = pMeterDataInfo[j]->groupIdx;     // set the group index
      pInfoEx->blockIndex = pMeterDataInfo[j]->start + k;  // set the block index in original meter
      cnt++;
    }

    numOfQualMeters++;
  }

  dTrace("QInfo %p create data blocks info struct completed", addr);

  assert(cnt <= numOfCompBlocks && numOfQualMeters <= numOfMeters);
  supporter.numOfMeters = numOfQualMeters;
  SLoserTreeInfo *pTree = NULL;

  uint8_t ret = tLoserTreeCreate(&pTree, supporter.numOfMeters, &supporter, blockAccessOrderComparator);
  UNUSED(ret);

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t                pos = pTree->pNode[0].index;
    SMeterDataBlockInfoEx *pBlocksInfoEx = supporter.pDataBlockInfoEx[pos];
    int32_t                index = supporter.blockIndexArray[pos]++;

    (*pDataBlockInfoEx)[numOfTotal++] = pBlocksInfoEx[index];

    if (supporter.blockIndexArray[pos] >= supporter.numOfBlocksPerMeter[pos]) {
      /* set data block index overflow, in order to disable the offset comparator */
      supporter.blockIndexArray[pos] = supporter.numOfBlocksPerMeter[pos] + 1;
    }
    tLoserTreeAdjust(pTree, pos + supporter.numOfMeters);
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfoEx)[i].pBlock.compBlock->offset < (*pDataBlockInfoEx)[i+1].pBlock.compBlock->offset);
   * }
   */

  dTrace("QInfo %p %d data blocks sort completed", addr, cnt);

  tfree(supporter.numOfBlocksPerMeter);
  tfree(supporter.blockIndexArray);

  for (int32_t i = 0; i < numOfMeters; ++i) {
    tfree(supporter.pDataBlockInfoEx[i]);
  }

  tfree(supporter.pDataBlockInfoEx);
  free(pTree);

  return cnt;
}

/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfoEx
 */
void setExecutionContext(SMeterQuerySupportObj *pSupporter, SOutputRes *outputRes, int32_t meterIdx, int32_t groupIdx) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  setOutputBuffer(pRuntimeEnv, &outputRes[groupIdx]);

  if (outputRes[groupIdx].numOfRows == 0) {
    initCtxOutputBuf(pRuntimeEnv);
  }

  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    pRuntimeEnv->pCtx[j].numOfOutputElems = 0;
    pRuntimeEnv->pCtx[j].numOfIteratedElems = 0;
  }

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pSupporter->pMeterSidExtInfo[meterIdx]);
}

static void setOutputBuffer(SQueryRuntimeEnv *pRuntimeEnv, SOutputRes *pResult) {
  // Note: pResult->result[i]->numOfElems == 0, there is only fixed number of
  // results for each group
  for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutputCols; ++i) {
    // the value is not set yet
    assert(pResult->result[i]->numOfElems == 0);
    pRuntimeEnv->pCtx[i].aOutputBuf =
        pResult->result[i]->data + pRuntimeEnv->pCtx[i].outputBytes * pResult->result[i]->numOfElems;
  }
}

static tFilePage *allocNewPage(SMeterQuerySupportObj *pSupporter, uint32_t *pageId) {
  if (pSupporter->lastPageId == pSupporter->numOfPages - 1) {
    extendDiskBuf(pSupporter, pSupporter->numOfPages + pSupporter->numOfMeters);
  }

  *pageId = (++pSupporter->lastPageId);

  return getFilePage(pSupporter, *pageId);
}

static char *getOutputResPos(SQueryRuntimeEnv *pRuntimeEnv, tFilePage *pData, int32_t row, int32_t col) {
  return (char *)pData->data + pRuntimeEnv->offset[col] * pRuntimeEnv->numOfRowsPerPage +
         pRuntimeEnv->pCtx[col].outputBytes * row;
}

void setCtxOutputPointerForSupplementScan(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *sqinfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t    index = sqinfo->reverseIndex;
  tFilePage *pData = NULL;
  int32_t    i = 0;

  // find the position for this output result
  for (; i < sqinfo->numOfPages; ++i) {
    pData = getFilePage(pSupporter, sqinfo->pageList[i]);
    if (index <= pData->numOfElems) {
      break;
    }
    index -= pData->numOfElems;
  }

  assert(index >= 0);

  /*
   * if it is the first records in master scan, no next results exist, so no need to init the result buffer
   * all data are processed and save to buffer during supplementary scan.
   */
  if (index == 0) {
    return;
  }

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    pRuntimeEnv->pCtx[k].aOutputBuf = getOutputResPos(pRuntimeEnv, pData, index - 1, k);
    if (!pRuntimeEnv->go[k]) {
      continue;
    }

    int32_t functId = pQuery->pSelectExpr[k].pBase.functionId;

    /* setup runtime environment */
    if ((QUERY_IS_ASC_QUERY(pQuery) && functId == TSDB_FUNC_FIRST_DST) ||
        (!QUERY_IS_ASC_QUERY(pQuery) && functId == TSDB_FUNC_LAST_DST)) {
      if (sqinfo->lastResRows == 0) {
        aAggs[functId].init(&pRuntimeEnv->pCtx[k]);
      }
    }
  }

  TSKEY      ts = *(TSKEY *)pRuntimeEnv->pCtx[0].aOutputBuf;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  qTrace("QInfo:%p vid:%d sid:%d id:%s, set output result pointer, ts:%lld, index:%d",
      GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, ts, sqinfo->reverseIndex);
}

void validateTimestampForSupplementResult(SQueryRuntimeEnv *pRuntimeEnv, int64_t numOfIncrementRes) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (pRuntimeEnv->scanFlag == SUPPLEMENTARY_SCAN && numOfIncrementRes > 0) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      if (functionId == TSDB_FUNC_TS) {
        assert(*(TSKEY *)pCtx[i].aOutputBuf == pCtx[i].nStartQueryTimestamp);
      }
    }
  }
}

void setOutputBufferForIntervalQuery(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *sqinfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  tFilePage *       pData = NULL;
  uint32_t          newPageId = 0;

  // in the first scan, new space needed for results
  if (sqinfo->numOfPages == 0) {
    pData = allocNewPage(pSupporter, &newPageId);
    incOutputPageId(sqinfo, newPageId);
  } else {
    int32_t lastPageId = sqinfo->pageList[sqinfo->numOfPages - 1];
    pData = getFilePage(pSupporter, lastPageId);

    if (pData->numOfElems >= pRuntimeEnv->numOfRowsPerPage) {
      pData = allocNewPage(pSupporter, &newPageId);
      incOutputPageId(sqinfo, newPageId);

      // number of elements must be 0 for newly allocated buffer
      assert(pData->numOfElems == 0);
    }
  }

  for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].aOutputBuf = getOutputResPos(pRuntimeEnv, pData, pData->numOfElems, i);
  }
}

void setIntervalQueryExecutionContext(SMeterQuerySupportObj *pSupporter, int32_t meterIdx, SMeterQueryInfo *sqinfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (IS_MASTER_SCAN(pRuntimeEnv)) {
    setOutputBufferForIntervalQuery(pSupporter, sqinfo);

    if (sqinfo->lastResRows == 0) {
      initCtxOutputBuf(pRuntimeEnv);
    }

    /* reset the number of iterated elements, once this function is called. since the pCtx for different
     */
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      pRuntimeEnv->pCtx[j].numOfOutputElems = 0;
      pRuntimeEnv->pCtx[j].numOfIteratedElems = 0;
    }

  } else {
    if (sqinfo->reverseFillRes) {
      setCtxOutputPointerForSupplementScan(pSupporter, sqinfo);
    } else {  // find the correct output position of existed results during
              // reverse scan data blocks
              /*
               * the master scan does not produce any results yet,
               * new spaces needed to be allocated during supplementary scan
               */
      setOutputBufferForIntervalQuery(pSupporter, sqinfo);
    }
  }

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pSupporter->pMeterSidExtInfo[meterIdx]);
}

static void doApplyIntervalQueryOnBlock(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *pInfo,
                                        SBlockInfo *pBlockInfo, int64_t *pPrimaryCol, char *sdata, SField *pFields,
                                        __block_search_fn_t searchFn) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  int32_t           factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  int64_t nextKey = -1;
  bool    queryCompleted = false;

  while (1) {
    int32_t numOfRes = 0;
    int32_t steps = applyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, pPrimaryCol, sdata, pFields, searchFn, &numOfRes);
    assert(steps > 0);

    // NOTE: in case of stable query, only ONE(or ZERO) row of result generated for each query range
    if (pInfo->lastResRows == 0) {
      pInfo->lastResRows = numOfRes;
    } else {
      assert(pInfo->lastResRows == 1);
    }

    int32_t pos = pQuery->pos + steps * factor;

    // query does not reach the end of current block
    if ((pos < pBlockInfo->size && QUERY_IS_ASC_QUERY(pQuery)) || (pos >= 0 && !QUERY_IS_ASC_QUERY(pQuery))) {
      nextKey = pPrimaryCol[pos];
    } else {
      assert((pQuery->lastKey > pBlockInfo->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
             (pQuery->lastKey < pBlockInfo->keyFirst && !QUERY_IS_ASC_QUERY(pQuery)));
    }

    // all data satisfy current query are checked, query completed
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      queryCompleted = (nextKey > pQuery->ekey || pQuery->ekey <= pBlockInfo->keyLast);
    } else {
      queryCompleted = (nextKey < pQuery->ekey || pQuery->ekey >= pBlockInfo->keyFirst);
    }

    /*
     * 1. there may be more date that satisfy current query interval, other than current block, we need to
     *    try next data blocks
     * 2. query completed, since reaches the upper bound of the main query range
     */
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      if (pQuery->lastKey > pBlockInfo->keyLast || pQuery->lastKey > pSupporter->rawEKey ||
          nextKey > pSupporter->rawEKey) {
        /*
         * current interval query is completed, set query result flag closed and try next data block
         * if pQuery->ekey == pSupporter->rawEKey, whole query is completed
         */
        if (pQuery->lastKey > pBlockInfo->keyLast) {
          assert(pQuery->ekey >= pBlockInfo->keyLast);
        }

        if (pQuery->lastKey > pSupporter->rawEKey || nextKey > pSupporter->rawEKey) {
          /* whole query completed, save result and abort */
          assert(queryCompleted);
          saveResult(pSupporter, pInfo, pInfo->lastResRows);

          /*
           * save the pQuery->lastKey for retrieve data in cache, actually, there will be no qualified data in cache.
           */
          saveIntervalQueryRange(pQuery, pInfo);
        } else if (pQuery->ekey == pBlockInfo->keyLast) {
          /* current interval query is completed, set the next query range on other data blocks if exist */
          int64_t prevEKey = pQuery->ekey;

          getAlignedIntervalQueryRange(pQuery, pQuery->lastKey, pSupporter->rawSKey, pSupporter->rawEKey);
          saveIntervalQueryRange(pQuery, pInfo);

          assert(queryCompleted && prevEKey < pQuery->skey);
          if (pInfo->lastResRows > 0) {
            saveResult(pSupporter, pInfo, pInfo->lastResRows);
          }
        } else {
          /*
           * Data that satisfy current query range may locate in current block and blocks that are
           * directly right next to current block. Therefore, we need to keep the query range(interval)
           * unchanged until reaching the direct next data block, while only forwards the pQuery->lastKey.
           *
           * With the information of the directly next data block, whether locates in cache or disk,
           * current interval query being completed or not can be decided.
           */
          saveIntervalQueryRange(pQuery, pInfo);
          assert(pQuery->lastKey > pBlockInfo->keyLast && pQuery->lastKey <= pQuery->ekey);

          /*
           * if current block is the last block of current file,
           * we still close the result flag, and merge with other meters in the same group
           */
          if (queryCompleted) {
            saveResult(pSupporter, pInfo, pInfo->lastResRows);
          }
        }

        break;
      }
    } else {
      if (pQuery->lastKey < pBlockInfo->keyFirst || pQuery->lastKey < pSupporter->rawEKey ||
          nextKey < pSupporter->rawEKey) {
        if (pQuery->lastKey < pBlockInfo->keyFirst) {
          assert(pQuery->ekey <= pBlockInfo->keyFirst);
        }

        if (pQuery->lastKey < pSupporter->rawEKey || (nextKey < pSupporter->rawEKey && nextKey != -1)) {
          /* whole query completed, save result and abort */
          assert(queryCompleted);
          saveResult(pSupporter, pInfo, pInfo->lastResRows);

          /*
           * save the pQuery->lastKey for retrieve data in cache, actually, there will be no qualified data in cache.
           */
          saveIntervalQueryRange(pQuery, pInfo);
        } else if (pQuery->ekey == pBlockInfo->keyFirst) {
          /* current interval query is completed, set the next query range on other data blocks if exist */
          int64_t prevEKey = pQuery->ekey;

          getAlignedIntervalQueryRange(pQuery, pQuery->lastKey, pSupporter->rawSKey, pSupporter->rawEKey);
          saveIntervalQueryRange(pQuery, pInfo);

          assert(queryCompleted && prevEKey > pQuery->skey);
          if (pInfo->lastResRows > 0) {
            saveResult(pSupporter, pInfo, pInfo->lastResRows);
          }
        } else {
          /*
           * Data that satisfy current query range may locate in current block and blocks that are
           * directly right next to current block. Therefore, we need to keep the query range(interval)
           * unchanged until reaching the direct next data block, while only forwards the pQuery->lastKey.
           *
           * With the information of the directly next data block, whether locates in cache or disk,
           * current interval query being completed or not can be decided.
           */
          saveIntervalQueryRange(pQuery, pInfo);
          assert(pQuery->lastKey < pBlockInfo->keyFirst && pQuery->lastKey >= pQuery->ekey);

          /*
           * if current block is the last block of current file,
           * we still close the result flag, and merge with other meters in the same group
           */
          if (queryCompleted) {
            saveResult(pSupporter, pInfo, pInfo->lastResRows);
          }
        }

        break;
      }
    }

    assert(queryCompleted);
    saveResult(pSupporter, pInfo, pInfo->lastResRows);

    assert((nextKey >= pQuery->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
           (nextKey <= pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));

    /* still in the same block to query */
    getAlignedIntervalQueryRange(pQuery, nextKey, pSupporter->rawSKey, pSupporter->rawEKey);
    saveIntervalQueryRange(pQuery, pInfo);

    int32_t newPos = searchFn((char *)pPrimaryCol, pBlockInfo->size, pQuery->skey, pQuery->order.order);
    assert(newPos == pQuery->pos + steps * factor);

    pQuery->pos = newPos;
  }
}

int64_t getNextAccessedKeyInData(SQuery *pQuery, int64_t *pPrimaryCol, SBlockInfo *pBlockInfo, int32_t blockStatus) {
  assert(pQuery->pos >= 0 && pQuery->pos <= pBlockInfo->size - 1);

  TSKEY key = -1;
  if (IS_DATA_BLOCK_LOADED(blockStatus)) {
    key = pPrimaryCol[pQuery->pos];
  } else {
    assert(pQuery->pos == pBlockInfo->size - 1 || pQuery->pos == 0);
    key = QUERY_IS_ASC_QUERY(pQuery) ? pBlockInfo->keyFirst : pBlockInfo->keyLast;
  }

  assert((key >= pQuery->skey && QUERY_IS_ASC_QUERY(pQuery)) || (key <= pQuery->skey && !QUERY_IS_ASC_QUERY(pQuery)));
  return key;
}

void setIntervalQueryRange(SMeterQuerySupportObj *pSupporter, TSKEY key, SMeterDataInfo *pInfoEx) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SMeterQueryInfo *pMeterQueryInfo = pInfoEx->pMeterQInfo;
  if (pMeterQueryInfo->queryRangeSet) {
    assert((QUERY_IS_ASC_QUERY(pQuery) && pQuery->lastKey >= pQuery->skey) ||
           (!QUERY_IS_ASC_QUERY(pQuery) && pQuery->lastKey <= pQuery->skey));

    if ((pQuery->ekey < key && QUERY_IS_ASC_QUERY(pQuery)) || (pQuery->ekey > key && !QUERY_IS_ASC_QUERY(pQuery))) {
      /*
       * last query on this block of the meter is done, start next interval on this block
       * otherwise, keep the previous query range and proceed
       */
      getAlignedIntervalQueryRange(pQuery, key, pSupporter->rawSKey, pSupporter->rawEKey);
      saveIntervalQueryRange(pQuery, pMeterQueryInfo);

      // previous query does not be closed, close it
      if (pMeterQueryInfo->lastResRows > 0) {
        saveResult(pSupporter, pMeterQueryInfo, pMeterQueryInfo->lastResRows);
      }
    } else {
      /* current query not completed, continue. do nothing with respect to query range, */
    }
  } else {
    /*
     * There are two cases to handle for the first block.
     * 1. Query range is not set yet. we need to set the query range, pQuery->lastKey, pQuery->skey, pQuery->eKey.
     * 2. Query range is set and in progress. There may be another result with the same query ranges to be merged
     *    during merge stage. However, in this case, we need the
     * pMeterQueryInfo->lastQueryClosed to decide if there is a previous result be generated or not.
     */
    pQuery->skey = key;
    assert(pMeterQueryInfo->lastResRows == 0);

    if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey < pQuery->skey)) ||
        (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey < pQuery->ekey))) {
      // for too small query range, no data in this interval.
      return;
    }

    getAlignedIntervalQueryRange(pQuery, pQuery->skey, pSupporter->rawSKey, pSupporter->rawEKey);
    saveIntervalQueryRange(pQuery, pMeterQueryInfo);
    pMeterQueryInfo->queryRangeSet = 1;
  }
}

bool requireTimestamp(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; i++) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_NEED_TS) != 0) {
      return true;
    }
  }
  return false;
}

static void setTimestampRange(SQueryRuntimeEnv *pRuntimeEnv, int64_t stime, int64_t etime) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_SPREAD || functionId == TSDB_FUNC_SPREAD_DST) {
      pRuntimeEnv->pCtx[i].intermediateBuf[1].dKey = stime;
      pRuntimeEnv->pCtx[i].intermediateBuf[2].dKey = etime;
    }
  }
}

bool needPrimaryTimestampCol(SQuery *pQuery, SBlockInfo *pBlockInfo) {
  /*
   * 1. if skey or ekey locates in this block, we need to load the timestamp column to decide the precise position
   * 2. if there are top/bottom, first_dst/last_dst functions, we need to load timestamp column in any cases;
   */
  bool loadPrimaryTS = (pQuery->lastKey >= pBlockInfo->keyFirst && pQuery->lastKey <= pBlockInfo->keyLast) ||
                       (pQuery->ekey >= pBlockInfo->keyFirst && pQuery->ekey <= pBlockInfo->keyLast) ||
                       requireTimestamp(pQuery);

  return loadPrimaryTS;
}

int32_t LoadDatablockOnDemand(SCompBlock *pBlock, SField **pFields, int8_t *blkStatus, SQueryRuntimeEnv *pRuntimeEnv,
                              int32_t fileIdx, int32_t slotIdx, __block_search_fn_t searchFn, bool onDemand) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SMeterObj *     pMeterObj = pRuntimeEnv->pMeterObj;
  SQueryFileInfo *pQueryFileInfo = &pRuntimeEnv->pHeaderFiles[fileIdx];

  TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

  pQuery->slot = slotIdx;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pBlock->numOfPoints - 1;

  SET_FILE_BLOCK_FLAG(*blkStatus);
  SET_DATA_BLOCK_NOT_LOADED(*blkStatus);

  if (((pQuery->lastKey <= pBlock->keyFirst && pQuery->ekey >= pBlock->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
       (pQuery->ekey <= pBlock->keyFirst && pQuery->lastKey >= pBlock->keyLast && !QUERY_IS_ASC_QUERY(pQuery))) &&
      onDemand) {
    int32_t req = 0;
    if (pQuery->numOfFilterCols > 0) {
      req = BLK_DATA_ALL_NEEDED;
    } else {
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        int32_t functID = pQuery->pSelectExpr[i].pBase.functionId;
        req |= aAggs[functID].dataReqFunc(&pRuntimeEnv->pCtx[i], pBlock->keyFirst, pBlock->keyLast,
                                          pQuery->pSelectExpr[i].pBase.colInfo.colId, *blkStatus);
      }
    }

    if (req == BLK_DATA_NO_NEEDED) {
      qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, data block ignored, brange:%lld-%lld, rows:%d",
          GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, pBlock->keyFirst,
          pBlock->keyLast, pBlock->numOfPoints);

      setTimestampRange(pRuntimeEnv, pBlock->keyFirst, pBlock->keyLast);
    } else if (req == BLK_DATA_FILEDS_NEEDED) {
      if (loadDataBlockFieldsInfo(pRuntimeEnv, pQueryFileInfo, pBlock, pFields) < 0) {
        return DISK_DATA_LOAD_FAILED;
      }
    } else {
      assert(req == BLK_DATA_ALL_NEEDED);
      goto _load_all;
    }
  } else {
  _load_all:
    // load sfield first
    if (loadDataBlockFieldsInfo(pRuntimeEnv, pQueryFileInfo, pBlock, pFields) < 0) {
      return DISK_DATA_LOAD_FAILED;
    }

    if ((pQuery->lastKey <= pBlock->keyFirst && pQuery->ekey >= pBlock->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pQuery->lastKey >= pBlock->keyLast && pQuery->ekey <= pBlock->keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {
      /*
       * if this block is completed included in the query range, do more filter operation
       * filter the data block according to the value filter condition.
       * no need to load the data block, continue for next block
       */
      if (!needToLoadDataBlock(pQuery, *pFields, pRuntimeEnv->pCtx)) {
#if defined(_DEBUG_VIEW)
        dTrace("QInfo:%p fileId:%d, slot:%d, block discarded by per-filter, ", GET_QINFO_ADDR(pQuery), pQuery->fileId,
               pQuery->slot);
#endif
        qTrace("QInfo:%p id:%s slot:%d, data block ignored by pre-filter, fields loaded, brange:%lld-%lld, rows:%d",
            GET_QINFO_ADDR(pQuery), pMeterObj->meterId, pQuery->slot, pBlock->keyFirst, pBlock->keyLast,
            pBlock->numOfPoints);
        return DISK_DATA_DISCARDED;
      }
    }

    SBlockInfo binfo = getBlockBasicInfo(pBlock, BLK_FILE_BLOCK);
    bool       loadTS = needPrimaryTimestampCol(pQuery, &binfo);

    /*
     * the pRuntimeEnv->pMeterObj is not updated during loop, since which meter this block is belonged to is not matter
     * in order to enforce to load the data block, we HACK the load check procedure,
     * by changing pQuery->slot each time to IGNORE the pLoadInfo data check. It is NOT a normal way.
     */
    int32_t ret = loadDataBlockIntoMem(pBlock, pFields, pRuntimeEnv, fileIdx, loadTS, false);
    SET_DATA_BLOCK_LOADED(*blkStatus);

    if (ret < 0) {
      return DISK_DATA_LOAD_FAILED;
    }

    /* find first qualified record position in this block */
    if (loadTS) {
      /* find first qualified record position in this block */
      pQuery->pos =
          searchFn(pRuntimeEnv->primaryColBuffer->data, pBlock->numOfPoints, pQuery->lastKey, pQuery->order.order);
      /* boundary timestamp check */
      assert(pBlock->keyFirst == primaryKeys[0] && pBlock->keyLast == primaryKeys[pBlock->numOfPoints - 1]);
    }

    assert((pQuery->skey <= pQuery->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
           (pQuery->ekey <= pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery)));
  }

  return DISK_DATA_LOADED;
}

bool onDemandLoadDatablock(SQuery *pQuery, int16_t queryRangeSet) {
  return (pQuery->nAggTimeInterval == 0) || ((queryRangeSet == 1) && (pQuery->nAggTimeInterval > 0));
}

void saveIntervalQueryRange(SQuery *pQuery, SMeterQueryInfo *pInfo) {
  pInfo->skey = pQuery->skey;
  pInfo->ekey = pQuery->ekey;
  pInfo->lastKey = pQuery->lastKey;

  assert(((pQuery->lastKey >= pQuery->skey) && QUERY_IS_ASC_QUERY(pQuery)) ||
         ((pQuery->lastKey <= pQuery->skey) && !QUERY_IS_ASC_QUERY(pQuery)));
}

void restoreIntervalQueryRange(SQuery *pQuery, SMeterQueryInfo *pInfo) {
  pQuery->skey = pInfo->skey;
  pQuery->ekey = pInfo->ekey;
  pQuery->lastKey = pInfo->lastKey;

  assert(((pQuery->lastKey >= pQuery->skey) && QUERY_IS_ASC_QUERY(pQuery)) ||
         ((pQuery->lastKey <= pQuery->skey) && !QUERY_IS_ASC_QUERY(pQuery)));
}

static void validateResultBuf(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *sqinfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pSupporter->runtimeEnv.pQuery;

  tFilePage *newOutput = getFilePage(pSupporter, sqinfo->pageList[sqinfo->numOfPages - 1]);
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    assert(pRuntimeEnv->pCtx[i].aOutputBuf - newOutput->data < DEFAULT_INTERN_BUF_SIZE);
  }
}

void saveResult(SMeterQuerySupportObj *pSupporter, SMeterQueryInfo *sqinfo, int32_t numOfResult) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (numOfResult <= 0) {
    return;
  }

  assert(sqinfo->lastResRows == 1);
  numOfResult = 1;
  sqinfo->lastResRows = 0;

  if (IS_SUPPLEMENT_SCAN(pRuntimeEnv) && sqinfo->reverseFillRes == 1) {
    assert(sqinfo->numOfRes > 0 && sqinfo->reverseIndex > 0 && sqinfo->reverseIndex <= sqinfo->numOfRes);
    // backward one step from the previous position, the start position is (sqinfo->numOfRows-1);
    sqinfo->reverseIndex -= 1;
    setCtxOutputPointerForSupplementScan(pSupporter, sqinfo);
  } else {
    int32_t    pageId = sqinfo->pageList[sqinfo->numOfPages - 1];
    tFilePage *pData = getFilePage(pSupporter, pageId);

    // in handling records occuring around '1970-01-01', the aligned start
    // timestamp may be 0.
    TSKEY ts = *(TSKEY *)getOutputResPos(pRuntimeEnv, pData, pData->numOfElems, 0);
    assert(ts >= 0);

    SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
    qTrace("QInfo:%p vid:%d sid:%d id:%s, save results, ts:%lld, total:%d", GET_QINFO_ADDR(pQuery), pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, ts, sqinfo->numOfRes + 1);

    pData->numOfElems += numOfResult;
    sqinfo->numOfRes += numOfResult;
    assert(pData->numOfElems <= pRuntimeEnv->numOfRowsPerPage);

    setOutputBufferForIntervalQuery(pSupporter, sqinfo);

    validateResultBuf(pSupporter, sqinfo);
    initCtxOutputBuf(pRuntimeEnv);
#if 0
        SSchema sc[TSDB_MAX_COLUMNS] = {0};
    sc[0].type = TSDB_DATA_TYPE_BIGINT;
    sc[0].bytes = 8;

    sc[1].type = TSDB_DATA_TYPE_BIGINT;
    sc[1].bytes = 8;

    UNUSED(sc);
    tColModel *cm = tColModelCreate(sc, pQuery->numOfOutputCols, pRuntimeEnv->numOfRowsPerPage);

//    if (outputPage->numOfElems + numOfResult >= pRuntimeEnv->numOfRowsPerPage)
        tColModelDisplay(cm, outputPage->data, outputPage->numOfElems, pRuntimeEnv->numOfRowsPerPage);
#endif
  }
}

static int32_t doCopyFromGroupBuf(SMeterQuerySupportObj *pSupporter, SOutputRes *result, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  /* pointsToRead is the max number of rows of results for output*/

  int32_t startIdx = 0;
  int32_t forward = 1;

  dTrace("QInfo:%p start to copy data to dest buf", GET_QINFO_ADDR(pSupporter->runtimeEnv.pQuery));

  if (orderType == TSQL_SO_ASC) {
    startIdx = pSupporter->subgroupIdx;
  } else {  // desc
    startIdx = pSupporter->pSidSet->numOfSubSet - pSupporter->subgroupIdx - 1;
    forward = -1;
  }

  for (int32_t i = startIdx; (i < pSupporter->pSidSet->numOfSubSet) && (i >= 0); i += forward) {
    if (result[i].numOfRows == 0) {
      pSupporter->offset = 0;
      pSupporter->subgroupIdx += 1;
      continue;
    }

    assert(result[i].numOfRows <= 1 && pSupporter->offset <= 1);

    tFilePage **srcBuf = result[i].result;

    int32_t numOfRowsToCopy = result[i].numOfRows - pSupporter->offset;
    int32_t oldOffset = pSupporter->offset;

    if (numOfRowsToCopy > pQuery->pointsToRead - numOfResult) {
      /* current output space is not enough for the keep the data of this group
       */
      numOfRowsToCopy = pQuery->pointsToRead - numOfResult;
      pSupporter->offset += numOfRowsToCopy;
    } else {
      pSupporter->offset = 0;
      pSupporter->subgroupIdx += 1;
    }

    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t elemSize = pRuntimeEnv->pCtx[j].outputBytes;
      char *  outputBuf = pQuery->sdata[j]->data + numOfResult * elemSize;

      memcpy(outputBuf, srcBuf[j]->data + oldOffset * elemSize, elemSize * numOfRowsToCopy);
    }

    numOfResult += numOfRowsToCopy;
    if (numOfResult == pQuery->pointsToRead) {
      break;
    }
  }

  dTrace("QInfo:%p done copy data to dst buf", GET_QINFO_ADDR(pSupporter->runtimeEnv.pQuery));

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, numOfResult);
#endif
  return numOfResult;
}

/**
 * copyFromGroupBuf support copy data in ascending/descending order
 * @param pQInfo
 * @param result
 */
void copyFromGroupBuf(SQInfo *pQInfo, SOutputRes *result) {
  SQuery *               pQuery = &pQInfo->query;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSQL_SO_DESC;

  int32_t numOfResult = doCopyFromGroupBuf(pSupporter, result, orderType);

  pQuery->pointsRead += numOfResult;
  assert(pQuery->pointsRead <= pQuery->pointsToRead);

  if (pSupporter->subgroupIdx == pSupporter->pSidSet->numOfSubSet) {
    /* set the end of query flag */
    pSupporter->meterIdx = pSupporter->numOfMeters;
  }
}

// todo refactor according to its called env!!
static void getAlignedIntervalQueryRange(SQuery *pQuery, TSKEY keyInData, TSKEY skey, TSKEY ekey) {
  if (pQuery->nAggTimeInterval == 0) {
    return;
  }

  doGetAlignedIntervalQueryRange(pQuery, keyInData, skey, ekey);
}

static void applyIntervalQueryOnBlock(SMeterQuerySupportObj *pSupporter, SMeterDataInfo *pInfoEx, char *data,
                                      int64_t *pPrimaryData, SBlockInfo *pBlockInfo, int32_t blockStatus,
                                      SField *pFields, __block_search_fn_t searchFn) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  SMeterQueryInfo * pInfo = pInfoEx->pMeterQInfo;

  /*
   * for each block, we need to handle the previous query, since the determination of previous query being completed
   * or not is based on the start key of current block.
   */
  TSKEY key = getNextAccessedKeyInData(pQuery, pPrimaryData, pBlockInfo, blockStatus);
  setIntervalQueryRange(pSupporter, key, pInfoEx);
  if (((pQuery->skey > pQuery->ekey) && QUERY_IS_ASC_QUERY(pQuery)) ||
      ((pQuery->skey < pQuery->ekey) && !QUERY_IS_ASC_QUERY(pQuery))) {
    return;
  }

  if (((pBlockInfo->keyLast < pQuery->ekey) && QUERY_IS_ASC_QUERY(pQuery)) ||
      ((pBlockInfo->keyFirst > pQuery->ekey) && !QUERY_IS_ASC_QUERY(pQuery))) {
    int32_t numOfRes = 0;
    /* current block is included in this interval */
    int32_t steps = applyFunctionsOnBlock(pRuntimeEnv, pBlockInfo, pPrimaryData, data, pFields, searchFn, &numOfRes);
    assert(numOfRes <= 1 && numOfRes >= 0 && steps > 0);

    if (pInfoEx->pMeterQInfo->lastResRows == 0) {
      pInfoEx->pMeterQInfo->lastResRows = numOfRes;
    } else {
      assert(pInfoEx->pMeterQInfo->lastResRows == 1);
    }

    saveIntervalQueryRange(pQuery, pInfo);
  } else {
    doApplyIntervalQueryOnBlock(pSupporter, pInfoEx->pMeterQInfo, pBlockInfo, pPrimaryData, data, pFields, searchFn);
  }
}

// we need to split the result into different packages.
int32_t vnodeGetResultSize(void *thandle, int32_t *numOfRows) {
  SQInfo *pQInfo = (SQInfo *)thandle;
  return pQInfo->query.rowSize * (*numOfRows);
}

int64_t vnodeGetOffsetVal(void *thandle) {
  SQInfo *pQInfo = (SQInfo *)thandle;
  return pQInfo->query.limit.offset;
}

bool vnodeHasRemainResults(void *handle) {
  SQInfo *               pQInfo = (SQInfo *)handle;
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;

  if (pSupporter == NULL || pQInfo->query.interpoType == TSDB_INTERPO_NONE) {
    return false;
  }

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SInterpolationInfo *pInterpoInfo = &pRuntimeEnv->interpoInfo;

  if (pQuery->limit.limit > 0 && pQInfo->pointsRead >= pQuery->limit.limit) {
    return false;
  }

  int32_t remain = taosNumOfRemainPoints(pInterpoInfo);
  if (remain > 0) {
    return true;
  } else {
    if (pRuntimeEnv->pInterpoBuf == NULL) {
      return false;
    }

    if (Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
      /* query has completed */
      TSKEY ekey = taosGetRevisedEndKey(pSupporter->rawEKey, pQuery->order.order, pQuery->nAggTimeInterval,
                                        pQuery->intervalTimeUnit);
      int32_t numOfTotal = taosGetNumOfResultWithInterpo(pInterpoInfo, (TSKEY *)pRuntimeEnv->pInterpoBuf[0]->data,
                                                         remain, pQuery->nAggTimeInterval, ekey, pQuery->pointsToRead);
      return numOfTotal > 0;
    }

    return false;
  }
}

static int32_t resultInterpolate(SQInfo *pQInfo, tFilePage **data, tFilePage **pDataSrc, int32_t numOfRows,
                                 int32_t outputRows) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pMeterQuerySupporter->runtimeEnv;

  assert(pRuntimeEnv->pCtx[0].outputBytes == TSDB_KEYSIZE);

  // build support structure for performing interpolation
  SSchema *pSchema = calloc(1, sizeof(SSchema) * pQuery->numOfOutputCols);
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pSchema[i].bytes = pRuntimeEnv->pCtx[i].outputBytes;
    pSchema[i].type = pQuery->pSelectExpr[i].resType;
  }

  tColModel *pModel = tColModelCreate(pSchema, pQuery->numOfOutputCols, pQuery->pointsToRead);

  char *  srcData[TSDB_MAX_COLUMNS] = {0};
  int32_t functions[TSDB_MAX_COLUMNS] = {0};

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    srcData[i] = pDataSrc[i]->data;
    functions[i] = pQuery->pSelectExpr[i].pBase.functionId;
  }

  int32_t numOfRes = taosDoInterpoResult(&pRuntimeEnv->interpoInfo, pQuery->interpoType, data, numOfRows, outputRows,
                                         pQuery->nAggTimeInterval, (int64_t *)pDataSrc[0]->data, pModel, srcData,
                                         pQuery->defaultVal, functions, pRuntimeEnv->pMeterObj->pointsPerFileBlock);

  tColModelDestroy(pModel);
  free(pSchema);

  return numOfRes;
}

/**
 *
 * @param handle
 * @param data
 * @param numOfRows the number of rows that are not returned in current retrieve
 * @return
 */
int32_t vnodeCopyQueryResultToMsg(void *handle, char *data, int32_t numOfRows) {
  SQInfo *pQInfo = (SQInfo *)handle;

  SMeterObj *pObj = pQInfo->pObj;
  SQuery *   pQuery = &pQInfo->query;

  assert(pQuery->pSelectExpr != NULL && pQuery->numOfOutputCols > 0);

  // copy data into dst buffer directly
  int   tnumOfRows = vnodeList[pObj->vnode].cfg.rowsInFileBlock;
  char *pData = data;

  /* for metric query, bufIndex always be 0. */
  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {  // pQInfo->bufIndex == 0
    int32_t bytes = pQuery->pSelectExpr[col].resBytes;

    memmove(pData, pQuery->sdata[col]->data + bytes * tnumOfRows * pQInfo->bufIndex, bytes * numOfRows);
    pData += bytes * numOfRows;
  }

  return numOfRows;
}

int32_t vnodeQueryResultInterpolate(SQInfo *pQInfo, tFilePage **pDst, tFilePage **pDataSrc, int32_t numOfRows,
                                    int32_t *numOfInterpo) {
  SMeterQuerySupportObj *pSupporter = pQInfo->pMeterQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *               pQuery = pRuntimeEnv->pQuery;

  while (1) {
    numOfRows = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);

    TSKEY ekey = taosGetRevisedEndKey(pSupporter->rawEKey, pQuery->order.order, pQuery->nAggTimeInterval,
                                      pQuery->intervalTimeUnit);
    int32_t numOfFinalRows =
        taosGetNumOfResultWithInterpo(&pRuntimeEnv->interpoInfo, (TSKEY *)pDataSrc[0]->data, numOfRows,
                                      pQuery->nAggTimeInterval, ekey, pQuery->pointsToRead);

    int32_t ret = resultInterpolate(pQInfo, pDst, pDataSrc, numOfRows, numOfFinalRows);
    assert(ret == numOfFinalRows);

    if (pQuery->limit.offset == 0) {
      /* reached the start position of according to offset value, return immediately */
      return ret;
    }

    if (pQuery->limit.offset < ret) {
      ret -= pQuery->limit.offset;
      // todo !!!!there exactly number of interpo is not valid.
      // todo refactor move to the beginning of buffer
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
          memmove(pDst[i]->data, pDst[i]->data + pQuery->pSelectExpr[i].resBytes * pQuery->limit.offset,
                  ret * pQuery->pSelectExpr[i].resBytes);
        }
      } else {
        for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
          memmove(pDst[i]->data + (pQuery->pointsToRead - ret) * pQuery->pSelectExpr[i].resBytes,
                  pDst[i]->data + (pQuery->pointsToRead - ret - pQuery->limit.offset) * pQuery->pSelectExpr[i].resBytes,
                  ret * pQuery->pSelectExpr[i].resBytes);
        }
      }
      pQuery->limit.offset = 0;
      return ret;
    } else {
      pQuery->limit.offset -= ret;
      ret = 0;
    }

    if (!vnodeHasRemainResults(pQInfo)) {
      return ret;
    }
  }
}

void vnodePrintQueryStatistics(SMeterQuerySupportObj *pSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  SQueryCostStatistics *pSummary = &pRuntimeEnv->summary;
  pSummary->tmpBufferInDisk = pSupporter->bufSize;

  dTrace("QInfo:%p statis: comp blocks:%d, size:%d Bytes, elapsed time:%.2f ms", pQInfo, pSummary->readCompInfo,
         pSummary->totalCompInfoSize, pSummary->loadCompInfoUs / 1000.0);

  dTrace("QInfo:%p statis: field info: %d, size:%d Bytes, avg size:%.2f Bytes, elapsed time:%.2f ms", pQInfo,
         pSummary->readField, pSummary->totalFieldSize, (double)pSummary->totalFieldSize / pSummary->readField,
         pSummary->loadFieldUs / 1000.0);

  dTrace("QInfo:%p statis: file blocks:%d, size:%d Bytes, elapsed time:%.2f ms, skipped:%d, in-memory gen null:%d Bytes",
      pQInfo, pSummary->readDiskBlocks, pSummary->totalBlockSize, pSummary->loadBlocksUs / 1000.0,
      pSummary->skippedFileBlocks, pSummary->totalGenData);

  dTrace("QInfo:%p statis: cache blocks:%d", pQInfo, pSummary->blocksInCache, 0);
  dTrace("QInfo:%p statis: temp file:%d Bytes", pQInfo, pSummary->tmpBufferInDisk);

  dTrace("QInfo:%p statis: file:%d, table:%d", pQInfo, pSummary->numOfFiles, pSummary->numOfTables);
  dTrace("QInfo:%p statis: seek ops:%d", pQInfo, pSummary->numOfSeek);

  double total = pSummary->fileTimeUs + pSummary->cacheTimeUs;
  double io = pSummary->loadCompInfoUs + pSummary->loadBlocksUs + pSummary->loadFieldUs;
  //    assert(io <= pSummary->fileTimeUs);

  // todo add the intermediate result save cost!!
  double computing = total - io;

  dTrace("QInfo:%p statis: total elapsed time:%.2f ms, file:%.2f ms(%.2f%), cache:%.2f ms(%.2f%). io:%.2f ms(%.2f%),"
         "comput:%.2fms(%.2f%)",
         pQInfo, total / 1000.0, pSummary->fileTimeUs / 1000.0, pSummary->fileTimeUs * 100 / total,
         pSummary->cacheTimeUs / 1000.0, pSummary->cacheTimeUs * 100 / total, io / 1000.0, io * 100 / total,
         computing / 1000.0, computing * 100 / total);
}
