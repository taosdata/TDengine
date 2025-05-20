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

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include "dataSink.h"
#include "osAtomic.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

extern SDataSinkManager2 g_pDataSinkManager;

void* getNextBuffStart(SAlignBlocksInMem* pAlignBlockInfo) {
  return (void*)pAlignBlockInfo + sizeof(SAlignBlocksInMem) + pAlignBlockInfo->dataLen;
}

void moveBlockBuf(SAlignBlocksInMem* pAlignBlockInfo, size_t dataEncodeBufSize) {
  ++pAlignBlockInfo->nWindow;
  pAlignBlockInfo->dataLen += dataEncodeBufSize;
}

static void freeWindowsBufferImmediate(SWindowData* pWindowData) {
  syncWindowDataMemSub(pWindowData);
  if (pWindowData->pDataBuf) {
    taosMemoryFree(pWindowData->pDataBuf);
    pWindowData->pDataBuf = NULL;
  }
  pWindowData->dataLen = 0;
}
static void windowsBufferMoveout(SWindowData* pWindowData) {
  pWindowData->saveMode = DATA_BLOCK_MOVED;
  pWindowData->pDataBuf = NULL;
  pWindowData->dataLen = 0;
}

static int32_t getBuffInMem(SResultIter* pResult, SWindowData* pWindowData, SSDataBlock** ppBlock, SCleanMode cleanMode) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDecode(pBlock, pWindowData->pDataBuf, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to decode data since %s, lineno:%d", tstrerror(code), lino);
    if (pBlock) {
      blockDataDestroy(pBlock);
    }
  } else {
    if (cleanMode == DATA_CLEAN_IMMEDIATE) {
      freeWindowsBufferImmediate(pWindowData);
      // 不用立即释放 pwindow 本身的资源，占用空间少，且需要移动 group 上 window array 的数据表，batch 方式处理
    }
    *ppBlock = pBlock;
  }
  return code;
}

static int32_t getBlockInMem(SResultIter* pResult, SWindowData* pWindowData, SSDataBlock** ppBlock) {
  *ppBlock = pWindowData->pDataBuf;
  windowsBufferMoveout(pWindowData);
  return TSDB_CODE_SUCCESS;
}

static int32_t getWindowDataInMem(SResultIter* pResult, SWindowData* pWindowData, SSDataBlock** ppBlock,
                                  SCleanMode cleanMode) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pWindowData->saveMode == DATA_SAVEMODE_BUFF) {
    return getBuffInMem(pResult, pWindowData, ppBlock, cleanMode);
  } else if (pWindowData->saveMode == DATA_BLOCK_MOVED) {
    stError("failed to get data from cache, since block cache cannot be reread.");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  } else {
    return getBlockInMem(pWindowData->pDataBuf, pWindowData, ppBlock);
  }
}

static void* getWindowDataBuf(SSlidingWindowInMem* pWindowData) { return (void*)pWindowData + sizeof(SSlidingWindowInMem); }

static int32_t getRangeInWindowBlock(SSlidingWindowInMem* pWindowData, int32_t tsColSlotId, TSKEY start, TSKEY end, SSDataBlock** ppBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockSpecialDecodeLaterPart(pBlock, getWindowDataBuf(pWindowData), tsColSlotId, start, end);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to decode data since %s, lineno:%d", tstrerror(code), lino);
    if (pBlock) {
      blockDataDestroy(pBlock);
    }
  } else {
    if(pBlock->info.rows == 0) {
      blockDataDestroy(pBlock);
      *ppBlock = NULL;
      return TSDB_CODE_SUCCESS;
    }
    *ppBlock = pBlock;
  }
  return code;
}

/*
static int32_t getSlidingWindowLaterDataInMem(SWindowData* pWindowData, int32_t tsColSlotId, TSKEY start,
                                              SSDataBlock** ppBlock) {
  return getRangeInWindowBlock(pWindowData, tsColSlotId, start, INT64_MAX, ppBlock);
}

static int32_t getSlidingWindowEarlierDataInMem(SWindowData* pWindowData, int32_t tsColSlotId, TSKEY end,
                                                SSDataBlock** ppBlock) {
  return getRangeInWindowBlock(pWindowData, tsColSlotId, INT64_MIN, end, ppBlock);
}

static int32_t getSlidingWindowDataInMem(SResultIter* pResult, SGroupDSManager* pGroupData, SWindowData* pWindowData,
                                         SSDataBlock** ppBlock) {
  if (pWindowData->wstart >= pResult->reqStartTime && pWindowData->wend <= pResult->reqEndTime) {
    return getWindowDataInMem(pResult, pWindowData, ppBlock, DATA_CLEAN_EXPIRED);
  } else if (pResult->reqStartTime >= pWindowData->wstart && pResult->reqStartTime <= pWindowData->wend) {
    return getSlidingWindowLaterDataInMem(pWindowData, pGroupData->pSinkManager->tsSlotId, pResult->reqStartTime,
                                          ppBlock);
  } else {  // (pResult->reqEndTime >= pWindowData->wstart && pResult->reqEndTime <= pWindowData->wend)
    return getSlidingWindowEarlierDataInMem(pWindowData, pGroupData->pSinkManager->tsSlotId, pResult->reqEndTime,
                                            ppBlock);
  }
}
  */

static int32_t getAlignDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
  for (; pAlignGrpMgr->blocksInMem->size > 0;) {
    SAlignBlocksInMem** ppBlockInfo = (SAlignBlocksInMem**)taosArrayGet(pAlignGrpMgr->blocksInMem, 0);
    SAlignBlocksInMem* pBlockInfo = *ppBlockInfo;
    if (pBlockInfo == NULL) {
      stError("failed to get block info from mem, since block is NULL");
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    while (pResult->winIndex < pBlockInfo->nWindow) {
      SSlidingWindowInMem* pWindowData = ((void*)pBlockInfo + sizeof(SAlignBlocksInMem) + pResult->offset);

      bool found = false;
      if(pWindowData->startTime > pResult->reqEndTime) {
        *finished = true;
        return code;
      }
      if (pWindowData->endTime >= pResult->reqStartTime) {
        found = true;
        code = getRangeInWindowBlock(pWindowData, pResult->tsColSlotId, pResult->reqStartTime, pResult->reqEndTime,
                                     ppBlock);
        if (code) {
          return code;
        }
      }
      if (++pResult->winIndex >= pBlockInfo->nWindow) {
        pResult->winIndex = 0;
        pResult->offset += 0;
        destoryAlignBlockInMem(ppBlockInfo);
        taosArrayRemove(pAlignGrpMgr->blocksInMem, 0);
        if (pAlignGrpMgr->blocksInMem->size == 0) {
          *finished = true;
          return code;
        }
        if (!found) {
          break;  // break the while loop
        }
      } else {
        pResult->offset += pWindowData->dataLen;
        if (!found) {
          continue;  // to check next window
        }
      }
      if (found) {
        return code;
      }
    }
  }
  return code;
}

static int32_t getSlidingDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SSlidingGrpMgr* pSlidingGrpMgr = pResult->groupData;
  for (; pResult->offset < pSlidingGrpMgr->winDataInMem->size; ++pResult->offset) {
    SSlidingWindowInMem* pWindowData =
        *(SSlidingWindowInMem**)taosArrayGet(pSlidingGrpMgr->winDataInMem, pResult->offset);
    if (pWindowData == NULL) {
      continue;
    }
    if (pWindowData->endTime < pResult->reqStartTime) {
      continue;  // to check next window
    } else {
      return getRangeInWindowBlock(pWindowData, pResult->tsColSlotId, pResult->reqStartTime, pResult->reqEndTime,
                                   ppBlock);
    }
  }
  *finished = true;
  return code;
}

int32_t readDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  if (pResult->cleanMode == DATA_CLEAN_IMMEDIATE) {
    return getAlignDataFromMem(pResult, ppBlock, finished);
  } else {
    return getSlidingDataFromMem(pResult, ppBlock, finished);
  }
}

void syncWindowDataMemAdd(SGrpCacheMgr* pGrpCacheMgr, int64_t size) {
  atomic_add_fetch_64(&pGrpCacheMgr->usedMemSize, size);
  atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

void syncWindowDataMemSub(SGrpCacheMgr* pGrpCacheMgr, int64_t size) {
  atomic_sub_fetch_64(&pGrpCacheMgr->usedMemSize, size);
  atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

bool setNextIteratorFromMem(SResultIter** ppResult) {
  SResultIter* pResult = *ppResult;

  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    if (pResult->offset++ < pSlidingGrpMgr->winDataInMem->size) {
      return false;
    } else {
      return true;
    }
  } else {
    SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
    return pAlignGrpMgr->blocksInMem->size == 0;
  }
  return true;
}

int32_t buildSlidingWindowInMem(SSDataBlock* pBlock, int32_t tsColSlotId, int32_t startIndex, int32_t endIndex,
                                SSlidingWindowInMem** ppSlidingWinInMem) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  // todo dataEncodeBufSize > real len
  size_t dataEncodeBufSize = blockGetEncodeSizeOfRows(pBlock, startIndex, endIndex);
  char*  buf = taosMemoryCalloc(1, sizeof(SSlidingWindowInMem) + dataEncodeBufSize);
  if (buf == NULL) {
    return terrno;
  }
  *ppSlidingWinInMem = (SSlidingWindowInMem*)buf;
  code = getStreamBlockTS(pBlock, tsColSlotId, endIndex, &(*ppSlidingWinInMem)->endTime);
  QUERY_CHECK_CODE(code, lino, _end);

  code = getStreamBlockTS(pBlock, tsColSlotId, 0, &(*ppSlidingWinInMem)->startTime);
  QUERY_CHECK_CODE(code, lino, _end);

  char*   pStart = buf + sizeof(SSlidingWindowInMem);
  int32_t len = 0;
  code = blockEncodeAsRows(pBlock, pStart, dataEncodeBufSize, numOfCols, startIndex, endIndex, &len);
  QUERY_CHECK_CODE(code, lino, _end);
  (*ppSlidingWinInMem)->dataLen = dataEncodeBufSize;

  return TSDB_CODE_SUCCESS;
_end:
  stError("failed to encode data since %s, lineno:%d", tstrerror(code), lino);
  if (buf) {
    taosMemoryFree(buf);
  }
  return code;
}

void destorySlidingWindowInMem(void* ppData) {
  SSlidingWindowInMem* pSlidingWinInMem = *(SSlidingWindowInMem**)ppData;
  if (pSlidingWinInMem) {
    taosMemoryFree(pSlidingWinInMem);
  }
}

void destoryAlignWindowInMem(void* ppData) {
  SSlidingWindowInMem* pSlidingWinInMem = *(SSlidingWindowInMem**)ppData;
  if (pSlidingWinInMem) {
    taosMemoryFree(pSlidingWinInMem);
  }
}

void destoryAlignBlockInMem(void* ppData) {
  SAlignBlocksInMem* ppAlignBlockInfo = *(SAlignBlocksInMem**)ppData;
  if (ppAlignBlockInfo) {
    taosMemoryFree(ppAlignBlockInfo);
  }
}

int32_t getEnoughBuffWindow(SAlignGrpMgr* pAlignGrpMgr, size_t dataEncodeBufSize,
                            SAlignBlocksInMem** ppAlignBlockInfo) {
  if (pAlignGrpMgr->blocksInMem == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SAlignBlocksInMem* pAlignBlockInfo = NULL;
  if (pAlignGrpMgr->blocksInMem->size > 0) {
    pAlignBlockInfo =
        *(SAlignBlocksInMem**)taosArrayGet(pAlignGrpMgr->blocksInMem, pAlignGrpMgr->blocksInMem->size - 1);
    if (pAlignBlockInfo && pAlignBlockInfo->capacity - pAlignBlockInfo->dataLen >= dataEncodeBufSize) {
      *ppAlignBlockInfo = pAlignBlockInfo;
      return TSDB_CODE_SUCCESS;
    }
  }

  pAlignBlockInfo = (SAlignBlocksInMem*)taosMemoryCalloc(1, gDSFileBlockDefaultSize + sizeof(SAlignBlocksInMem));
  if (pAlignBlockInfo == NULL) {
    return terrno;
  }
  pAlignBlockInfo->capacity = gDSFileBlockDefaultSize;
  pAlignBlockInfo->nWindow = 0;
  pAlignBlockInfo->dataLen = 0;
  if (taosArrayPush(pAlignGrpMgr->blocksInMem, &pAlignBlockInfo) == NULL) {
    taosMemoryFree(pAlignBlockInfo);
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    return terrno;
  }
  *ppAlignBlockInfo = pAlignBlockInfo;
  return TSDB_CODE_SUCCESS;
}

int32_t buildAlignWindowInMemBlock(SAlignGrpMgr* pAlignGrpMgr, SSDataBlock* pBlock, int32_t tsColSlotId, TSKEY wstart,
                                   TSKEY wend) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  // todo dataEncodeBufSize > real len
  size_t             dataEncodeBufSize = sizeof(SSlidingWindowInMem) + blockGetEncodeSize(pBlock);
  SAlignBlocksInMem* pAlignBlockInfo = NULL;
  code = getEnoughBuffWindow(pAlignGrpMgr, dataEncodeBufSize, &pAlignBlockInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SSlidingWindowInMem* pSlidingWinInMem = (SSlidingWindowInMem*)(getNextBuffStart(pAlignBlockInfo));
  pSlidingWinInMem->endTime = wend;
  pSlidingWinInMem->startTime = wstart;
  pSlidingWinInMem->dataLen = dataEncodeBufSize;

  char*   pStart = getWindowDataBuf(pSlidingWinInMem);
  int32_t len = 0;
  len = blockEncode(pBlock, pStart, dataEncodeBufSize, numOfCols);
  if(len < 0) {
    stError("failed to encode data since %s, lineno:%d", tstrerror(len), lino);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  moveBlockBuf(pAlignBlockInfo, dataEncodeBufSize);

  return TSDB_CODE_SUCCESS;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to encode data since %s, lineno:%d", tstrerror(code), lino);
  }
  return code;
}
