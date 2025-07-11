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
#include "tglobal.h"
#include "thash.h"

extern SDataSinkManager2 g_pDataSinkManager;
SSlidingGrpMemList g_slidigGrpMemList = {0};

void* getNextBuffStart(SAlignBlocksInMem* pAlignBlockInfo) {
  return (void*)pAlignBlockInfo + sizeof(SAlignBlocksInMem) + pAlignBlockInfo->dataLen;
}

void moveBlockBuf(SAlignBlocksInMem* pAlignBlockInfo, size_t dataEncodeBufSize) {
  ++pAlignBlockInfo->nWindow;
  pAlignBlockInfo->dataLen += dataEncodeBufSize;
}

void* getWindowDataBuf(SSlidingWindowInMem* pWindowData) { return (void*)pWindowData + sizeof(SSlidingWindowInMem); }

static int32_t getRangeInWindowBlock(SSlidingWindowInMem* pWindowData, int32_t tsColSlotId, TSKEY start, TSKEY end,
                                     SSDataBlock** ppBlock) {
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
    if (pBlock->info.rows == 0) {
      blockDataDestroy(pBlock);
      *ppBlock = NULL;
      return TSDB_CODE_SUCCESS;
    }
    *ppBlock = pBlock;
  }
  return code;
}

static int32_t getAlignDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
  for (; pAlignGrpMgr->blocksInMem->size > 0;) {
    SAlignBlocksInMem** ppBlockInfo = (SAlignBlocksInMem**)taosArrayGet(pAlignGrpMgr->blocksInMem, 0);
    SAlignBlocksInMem*  pBlockInfo = *ppBlockInfo;
    if (pBlockInfo == NULL) {
      stError("failed to get block info from mem, since block is NULL");
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    while (pResult->winIndex < pBlockInfo->nWindow) {
      SSlidingWindowInMem* pWindowData = ((void*)pBlockInfo + sizeof(SAlignBlocksInMem) + pResult->offset);

      bool found = false;
      if (pWindowData->startTime > pResult->reqEndTime) {
        *finished = true;
        return code;
      }

      if (pWindowData->endTime >= pResult->reqStartTime) {
        found = true;
        if (pWindowData->dataLen == 0) {
          SMoveWindowInfo* pMoveWinInfo = getWindowDataBuf(pWindowData);
          if (pMoveWinInfo->pData != NULL) {
            *ppBlock = pMoveWinInfo->pData;
            pMoveWinInfo->pData = NULL;
          }
          atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pMoveWinInfo->moveSize);
        } else {
          code = getRangeInWindowBlock(pWindowData, pResult->tsColSlotId, TSKEY_MIN, TSKEY_MAX,
                                       ppBlock);
          if (code) {
            return code;
          }
        }
      }

      if (++pResult->winIndex >= pBlockInfo->nWindow) {
        pResult->winIndex = 0;
        pResult->offset += 0;
        destroyAlignBlockInMemPP(ppBlockInfo);
        taosArrayRemove(pAlignGrpMgr->blocksInMem, 0);
        if (pAlignGrpMgr->blocksInMem->size == 0) {
          *finished = true;
          return code;
        }
        if (!found) {
          break;  // break the while loop
        }
      } else {
        pResult->offset += pWindowData->dataLen == 0 ? (sizeof(SAlignBlocksInMem) + sizeof(SMoveWindowInfo)) : pWindowData->dataLen;
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

bool shouldWriteSlidingGrpMemList(SSlidingGrpMgr* pSlidingGrpMgr) {
  if (pSlidingGrpMgr->usedMemSize < (1 * 1024 * 1024) && g_slidigGrpMemList.waitMoveMemSize < DS_MEM_SIZE_RESERVED) {
    return false;
  }
  int64_t size = taosHashGetSize(g_slidigGrpMemList.pSlidingGrpList);
  if (size == 0) {
    return true;
  }
  if (g_slidigGrpMemList.waitMoveMemSize > DS_MEM_SIZE_RESERVED) {
    return true;
  }

  if (g_slidigGrpMemList.waitMoveMemSize < g_pDataSinkManager.memAlterSize ||
      (pSlidingGrpMgr->usedMemSize >
       g_slidigGrpMemList.waitMoveMemSize / taosHashGetSize(g_slidigGrpMemList.pSlidingGrpList))) {
    return true;
  }
  return false;
}

static void updateSlidingGrpUsedMemSize(SSlidingGrpMgr* pSlidingGrpMgr) {
  if (!g_slidigGrpMemList.enabled) {
    return;
  }
  int32_t code = TSDB_CODE_SUCCESS;

  if (shouldWriteSlidingGrpMemList(pSlidingGrpMgr)) {
    int64_t* oldSize = taosHashGet(g_slidigGrpMemList.pSlidingGrpList, &pSlidingGrpMgr, sizeof(SSlidingGrpMgr*));
    if (oldSize == NULL) {
      code = taosHashPut(g_slidigGrpMemList.pSlidingGrpList, &pSlidingGrpMgr, sizeof(SSlidingGrpMgr*),
                         &pSlidingGrpMgr->usedMemSize, sizeof(int64_t));
      if (code == TSDB_CODE_SUCCESS) {
        atomic_add_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, pSlidingGrpMgr->usedMemSize);
      }
    } else {
      atomic_add_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, pSlidingGrpMgr->usedMemSize);
      atomic_sub_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, *oldSize);
    }
  }
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
      destroySlidingWindowInMem(pWindowData);
      // todo: remove from array has low performance, need to optimize.
      taosArrayRemove(pSlidingGrpMgr->winDataInMem, pResult->offset);
      --pResult->offset;  // adjust offset since we removed the current window
      continue;           // to check next window
    } else if (pWindowData->startTime > pResult->reqEndTime) {
      *finished = true;
      updateSlidingGrpUsedMemSize(pSlidingGrpMgr);
      return code;
    } else {
      return getRangeInWindowBlock(pWindowData, pResult->tsColSlotId, pResult->reqStartTime, pResult->reqEndTime,
                                   ppBlock);
    }
  }
  *finished = true;
  updateSlidingGrpUsedMemSize(pSlidingGrpMgr);
  return code;
}

int32_t readDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  if (pResult->cleanMode == DATA_CLEAN_IMMEDIATE) {
    return getAlignDataFromMem(pResult, ppBlock, finished);
  } else {
    return getSlidingDataFromMem(pResult, ppBlock, finished);
  }
}

void slidingGrpMgrUsedMemAdd(SSlidingGrpMgr* pGrpCacheMgr, int64_t size) {
  atomic_add_fetch_64(&pGrpCacheMgr->usedMemSize, size);
  atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

bool setNextIteratorFromMem(SResultIter** ppResult) {
  SResultIter* pResult = *ppResult;

  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    if (++pResult->offset < pSlidingGrpMgr->winDataInMem->size) {
      return false;
    } else {
      return true;
    }
  } else {
    // 在读取数据时已完成指针移动
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

  code = getStreamBlockTS(pBlock, tsColSlotId, startIndex, &(*ppSlidingWinInMem)->startTime);
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

void destroySlidingWindowInMem(void* pData) {
  SSlidingWindowInMem* pSlidingWinInMem = (SSlidingWindowInMem*)pData;
  if (pSlidingWinInMem) {
    atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pSlidingWinInMem->dataLen + sizeof(SSlidingWindowInMem));
    taosMemoryFree(pSlidingWinInMem);
  }
}

void destroySlidingWindowInMemPP(void* pData) {
  SSlidingWindowInMem* pSlidingWinInMem = *(SSlidingWindowInMem**)pData;
  if (pSlidingWinInMem) {
    atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pSlidingWinInMem->dataLen + sizeof(SSlidingWindowInMem));
    taosMemoryFree(pSlidingWinInMem);
    *(SSlidingWindowInMem**)pData = NULL;
  }
}

void destroyAlignBlockInMemPP(void* ppData) {
  SAlignBlocksInMem* pAlignBlockInfo = *(SAlignBlocksInMem**)ppData;
  if (pAlignBlockInfo) {
    atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem));
    taosMemoryFree(pAlignBlockInfo);
    *(SAlignBlocksInMem**)ppData = NULL;
  }
}

void destroyAlignBlockInMem(void* pData) {
  SAlignBlocksInMem* pAlignBlockInfo = (SAlignBlocksInMem*)pData;
  if (pAlignBlockInfo) {
    taosMemoryFree(pAlignBlockInfo);
    atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem));
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

  pAlignBlockInfo = (SAlignBlocksInMem*)taosMemoryCalloc(1, DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem));
  if (pAlignBlockInfo == NULL) {
    return terrno;
  }
  pAlignBlockInfo->capacity = DS_FILE_BLOCK_SIZE;
  pAlignBlockInfo->nWindow = 0;
  pAlignBlockInfo->dataLen = 0;
  if (taosArrayPush(pAlignGrpMgr->blocksInMem, &pAlignBlockInfo) == NULL) {
    taosMemoryFree(pAlignBlockInfo);
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    return terrno;
  }
  *ppAlignBlockInfo = pAlignBlockInfo;

  atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem));
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
  if (len < 0) {
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

int32_t buildMoveAlignWindowInMem(SAlignGrpMgr* pAlignGrpMgr, SSDataBlock* pBlock, int32_t tsColSlotId, TSKEY wstart,
                                  TSKEY wend) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  size_t             dataEncodeBufSize = sizeof(SSlidingWindowInMem) + sizeof(SMoveWindowInfo);
  size_t             moveSize = blockGetEncodeSize(pBlock);
  SAlignBlocksInMem* pAlignBlockInfo = NULL;
  code = getEnoughBuffWindow(pAlignGrpMgr, dataEncodeBufSize, &pAlignBlockInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SSlidingWindowInMem* pSlidingWinInMem = (SSlidingWindowInMem*)(getNextBuffStart(pAlignBlockInfo));
  pSlidingWinInMem->endTime = wend;
  pSlidingWinInMem->startTime = wstart;
  pSlidingWinInMem->dataLen = 0;  // move data, so no data length

  char*            pStart = getWindowDataBuf(pSlidingWinInMem);
  SMoveWindowInfo* pMoveInfo = (SMoveWindowInfo*)pStart;
  pMoveInfo->moveSize = moveSize;
  pMoveInfo->pData = pBlock;
  atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, moveSize);
  moveBlockBuf(pAlignBlockInfo, dataEncodeBufSize);

  return TSDB_CODE_SUCCESS;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to encode data since %s, lineno:%d", tstrerror(code), lino);
  }
  return code;
}

int32_t moveMemFromWaitList(int8_t mode) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (!g_slidigGrpMemList.enabled) {
    return TSDB_CODE_SUCCESS;
  }
  stInfo("start to move sliding group mem cache, waitMoveMemSize:%" PRId64 ", usedMemSize:%" PRId64,
         g_slidigGrpMemList.waitMoveMemSize, g_pDataSinkManager.usedMemSize);

  int64_t size = taosHashGetSize(g_slidigGrpMemList.pSlidingGrpList);
  if (size == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSlidingGrpMgr** ppSlidingGrpMgr = (SSlidingGrpMgr**)taosHashIterate(g_slidigGrpMemList.pSlidingGrpList, NULL);
  while (ppSlidingGrpMgr != NULL) {
    SSlidingGrpMgr* pSlidingGrp = *ppSlidingGrpMgr;
    if (pSlidingGrp == NULL) {
      ppSlidingGrpMgr = taosHashIterate(g_slidigGrpMemList.pSlidingGrpList, ppSlidingGrpMgr);
      continue;
    }
    if (hasEnoughMemSize()) {
      break;  // no need to move more mem
    }
    bool canMove = changeMgrStatusToMoving(&pSlidingGrp->status, mode);
    if (!canMove) {
      ppSlidingGrpMgr = taosHashIterate(g_slidigGrpMemList.pSlidingGrpList, ppSlidingGrpMgr);
      continue;  // another thread is using this group, skip it
    }

    code = moveSlidingGrpMemCache(NULL, pSlidingGrp);
    changeMgrStatus(&pSlidingGrp->status, GRP_DATA_IDLE);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to move sliding group mem cache, code: %d err: %s", code, terrMsg);
    }
    ppSlidingGrpMgr = taosHashIterate(g_slidigGrpMemList.pSlidingGrpList, ppSlidingGrpMgr);
  }
  if (ppSlidingGrpMgr != NULL) {
    taosHashCancelIterate(g_slidigGrpMemList.pSlidingGrpList, ppSlidingGrpMgr);
  }
  stInfo("move sliding group mem cache finished, used mem size: %" PRId64 ", max mem size: %" PRId64,
         g_pDataSinkManager.usedMemSize, tsStreamBufferSizeBytes);
  return TSDB_CODE_SUCCESS;
}

int32_t moveSlidingTaskMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSlidingGrpMgr** ppSlidingGrpMgr = (SSlidingGrpMgr**)taosHashIterate(pSlidingTaskMgr->pSlidingGrpList, NULL);
  while (ppSlidingGrpMgr != NULL) {
    SSlidingGrpMgr* pSlidingGrp = *ppSlidingGrpMgr;
    if (pSlidingGrp == NULL) {
      ppSlidingGrpMgr = taosHashIterate(pSlidingTaskMgr->pSlidingGrpList, ppSlidingGrpMgr);
      continue;
    }
    bool canMove = changeMgrStatusToMoving(&pSlidingGrp->status, GRP_DATA_WAITREAD_MOVING);
    if (!canMove) {
      ppSlidingGrpMgr = taosHashIterate(pSlidingTaskMgr->pSlidingGrpList, ppSlidingGrpMgr);
      continue;  // another thread is using this group, skip it
    }
    code = moveSlidingGrpMemCache(pSlidingTaskMgr, pSlidingGrp);
    changeMgrStatus(&pSlidingGrp->status, GRP_DATA_IDLE);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to move sliding group mem cache, code: %d err: %s", code, terrMsg);
    }
    if (hasEnoughMemSize()) {
      break;
    }
    ppSlidingGrpMgr = taosHashIterate(pSlidingTaskMgr->pSlidingGrpList, ppSlidingGrpMgr);
  }
  if (ppSlidingGrpMgr != NULL) {
    taosHashCancelIterate(pSlidingTaskMgr->pSlidingGrpList, ppSlidingGrpMgr);
  }
  return code;
}
