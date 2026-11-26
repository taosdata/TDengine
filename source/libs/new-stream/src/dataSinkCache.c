/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <string.h>
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
  return (char*)pAlignBlockInfo + sizeof(SAlignBlocksInMem) + pAlignBlockInfo->dataLen;
}

void setUsedBlockBuf(SAlignBlocksInMem* pAlignBlockInfo, size_t usedSize) {
  const SSlidingWindowInMem* pWindow = getNextBuffStart(pAlignBlockInfo);
  if (pAlignBlockInfo->nWindow > 0 && pWindow->startTime < pAlignBlockInfo->lastWindowStart &&
      !pAlignBlockInfo->unorderedWindows) {
    pAlignBlockInfo->unorderedWindows = true;
    stDebug("align cache block received an out-of-order window, start:%" PRId64 " previous:%" PRId64,
            pWindow->startTime, pAlignBlockInfo->lastWindowStart);
  }
  pAlignBlockInfo->lastWindowStart = pWindow->startTime;
  ++pAlignBlockInfo->nWindow;
  pAlignBlockInfo->dataLen += usedSize;
}

/* Grow the per-window consumption flags so the block can hold needWindows entries. */
static int32_t ensureAlignConsumedCapacity(SAlignBlocksInMem* pAlignBlockInfo, int32_t needWindows) {
  if (pAlignBlockInfo == NULL || needWindows <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pAlignBlockInfo->pConsumed != NULL && pAlignBlockInfo->consumedCap >= needWindows) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t  newCap = pAlignBlockInfo->consumedCap * 2;
  if (newCap < needWindows) {
    newCap = needWindows;
  }
  if (newCap < 8) {
    newCap = 8;
  }
  uint8_t* pNew = taosMemoryRealloc(pAlignBlockInfo->pConsumed, newCap);
  if (pNew == NULL) {
    return terrno;
  }
  memset(pNew + pAlignBlockInfo->consumedCap, 0, newCap - pAlignBlockInfo->consumedCap);
  // Keep the manager accounting in sync with the consumption flags allocation.
  (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, (int64_t)(newCap - pAlignBlockInfo->consumedCap));
  pAlignBlockInfo->pConsumed = pNew;
  pAlignBlockInfo->consumedCap = newCap;
  return TSDB_CODE_SUCCESS;
}

void* getWindowDataBuf(SSlidingWindowInMem* pWindowData) { return (char*)pWindowData + sizeof(SSlidingWindowInMem); }

static int32_t getRangeInWindowBlock(SSlidingWindowInMem* pWindowData, int64_t available, int32_t tsColSlotId,
                                     TSKEY start, TSKEY end, SSDataBlock** ppBlock) {
  if (available <= 0) return TSDB_CODE_INVALID_MSG;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  if (start == TSKEY_MIN && end == TSKEY_MAX) {
    // Aligned windows are selected by their header; projected columns need no timestamp filtering.
    code = blockDecodeBounded(pBlock, getWindowDataBuf(pWindowData), available, NULL);
  } else {
    code = blockSpecialDecodeLaterPartBounded(pBlock, getWindowDataBuf(pWindowData), available, tsColSlotId, start, end,
                                              NULL);
  }
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

/* -------------------------------------------------------------------------
 * Align data cache: per-window reads
 *
 * One block holds several windows, and the same group can dispatch its windows
 * through more than one calc request (COUNT_WINDOW/interval streams emit one
 * window per param, and a mixed stream may dispatch them over several rounds).
 * A read therefore has to locate exactly the window it was asked for and may
 * release a block only after every window inside it has been read by its own
 * request. The previous implementation walked a shared per-block cursor and
 * destroyed the whole block when the cursor reached its end, which discarded
 * windows no request had read yet (defect 7147722181).
 * ------------------------------------------------------------------------- */

static int32_t alignGetWindowRecord(SAlignBlocksInMem* pBlockInfo, int64_t offset, SSlidingWindowInMem** ppWindowData,
                                    int64_t* pWindowSize) {
  if (pBlockInfo == NULL || ppWindowData == NULL || pWindowSize == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlockInfo->dataLen < 0 || pBlockInfo->dataLen > pBlockInfo->capacity || offset < 0 ||
      offset > pBlockInfo->dataLen || pBlockInfo->dataLen - offset < (int64_t)sizeof(SSlidingWindowInMem)) {
    return TSDB_CODE_INVALID_MSG;
  }
  SSlidingWindowInMem* pWindowData = (SSlidingWindowInMem*)((char*)pBlockInfo + sizeof(SAlignBlocksInMem) + offset);
  int64_t windowSize =
      pWindowData->dataLen == 0 ? sizeof(SSlidingWindowInMem) + sizeof(SMoveWindowInfo) : pWindowData->dataLen;
  if (windowSize < (int64_t)sizeof(SSlidingWindowInMem) || windowSize > pBlockInfo->dataLen - offset) {
    return TSDB_CODE_INVALID_MSG;
  }
  *ppWindowData = pWindowData;
  *pWindowSize = windowSize;
  return TSDB_CODE_SUCCESS;
}

static bool alignWindowConsumed(const SAlignBlocksInMem* pBlockInfo, int32_t winIndex) {
  return pBlockInfo->pConsumed != NULL && winIndex >= 0 && winIndex < pBlockInfo->consumedCap &&
         pBlockInfo->pConsumed[winIndex] != 0;
}

static void alignMarkWindowConsumed(SAlignBlocksInMem* pBlockInfo, int32_t winIndex, int64_t nextOffset) {
  if (pBlockInfo->pConsumed != NULL && winIndex >= 0 && winIndex < pBlockInfo->consumedCap) {
    if (!pBlockInfo->pConsumed[winIndex]) {
      pBlockInfo->pConsumed[winIndex] = 1;
      ++pBlockInfo->consumedCount;
    }
    if (winIndex == pBlockInfo->firstUnreadIndex) {
      ++pBlockInfo->firstUnreadIndex;
      pBlockInfo->firstUnreadOffset = nextOffset;
    }
  }
}

static void alignRecordWindowOrder(SAlignGrpMgr* pGroup, int64_t startTime) {
  if (pGroup->hasWindow && startTime < pGroup->lastWindowStart && !pGroup->unorderedWindows) {
    pGroup->unorderedWindows = true;
    stDebug("align cache group received an out-of-order window, groupId:%" PRId64 " start:%" PRId64
            " previous:%" PRId64, pGroup->groupId, startTime, pGroup->lastWindowStart);
  }
  pGroup->hasWindow = true;
  pGroup->lastWindowStart = startTime;
}

static int32_t getAlignDataFromMemByWindow(SResultIter* pResult, SAlignGrpMgr* pAlignGrpMgr, SSDataBlock** ppBlock,
                                           bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    served = false;

  *finished = false;

  // Serve the next unread record that overlaps this request. Windows are
  // addressed by their own [start, end] range instead of through a shared
  // per-block cursor, so reading one window can never consume or release the
  // windows that other pending calculations of the same group still have to
  // read. Every stored record is returned exactly once, which also keeps a
  // window whose rows were written in several pieces complete.
  for (int32_t blockIdx = 0; blockIdx < taosArrayGetSize(pAlignGrpMgr->blocksInMem) && !served;) {
    SAlignBlocksInMem** ppBlockInfo = taosArrayGet(pAlignGrpMgr->blocksInMem, blockIdx);
    if (ppBlockInfo == NULL || *ppBlockInfo == NULL) {
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    SAlignBlocksInMem* pBlockInfo = *ppBlockInfo;
    int64_t            offset = pBlockInfo->firstUnreadOffset;
    for (int32_t winIndex = pBlockInfo->firstUnreadIndex; winIndex < pBlockInfo->nWindow; ++winIndex) {
      SSlidingWindowInMem* pWindowData = NULL;
      int64_t              windowSize = 0;
      code = alignGetWindowRecord(pBlockInfo, offset, &pWindowData, &windowSize);
      QUERY_CHECK_CODE(code, lino, _end);
      offset += windowSize;
      if (alignWindowConsumed(pBlockInfo, winIndex)) {
        alignMarkWindowConsumed(pBlockInfo, winIndex, offset);
        continue;
      }
      if (pWindowData->startTime > pResult->reqEndTime && !pBlockInfo->unorderedWindows) {
        if (!pAlignGrpMgr->unorderedWindows) {
          *finished = true;
          return TSDB_CODE_SUCCESS;
        }
        break;
      }
      if (pWindowData->startTime > pResult->reqEndTime || pWindowData->endTime < pResult->reqStartTime) {
        continue;
      }
      if (pWindowData->dataLen == 0) {
        SMoveWindowInfo* pMoveWinInfo = getWindowDataBuf(pWindowData);
        if (pMoveWinInfo->pData == NULL) {
          // The moved data was handed out already, so this record has nothing left to serve. Mark it
          // consumed so the block can still be reclaimed, but skip the size accounting that was done
          // when the data was handed out.
          alignMarkWindowConsumed(pBlockInfo, winIndex, offset);
          continue;
        }
        *ppBlock = pMoveWinInfo->pData;
        pMoveWinInfo->pData = NULL;
        (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pMoveWinInfo->moveSize);
      } else {
        code = getRangeInWindowBlock(pWindowData, pWindowData->dataLen - (int64_t)sizeof(SSlidingWindowInMem),
                                     pResult->tsColSlotId, TSKEY_MIN, TSKEY_MAX, ppBlock);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      // Mark the window read only after its data is in hand: a failed decode must leave the record
      // readable for a later request instead of dropping it silently.
      alignMarkWindowConsumed(pBlockInfo, winIndex, offset);
      // A decoded window with no rows yields no block; keep looking for a window that has data.
      served = (*ppBlock != NULL);
      if (served) {
        break;
      }
    }
    if (pBlockInfo->nWindow > 0 && pBlockInfo->consumedCount == pBlockInfo->nWindow) {
      stDebug("release consumed align cache block, groupId:%" PRId64 " windows:%d", pResult->groupId,
              pBlockInfo->nWindow);
      destroyAlignBlockInMemPP(ppBlockInfo);
      taosArrayRemove(pAlignGrpMgr->blocksInMem, blockIdx);
    } else {
      ++blockIdx;
    }
  }

  *finished = !served;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to read align data cache by window since %s, lineno:%d", tstrerror(code), lino);
  }
  return code;
}

static int32_t getAlignDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
  if (!pResult->exactWindow) {
    // Non exact-window streams (COUNT_WINDOW/interval/state/period) address windows by
    // their own [start, end], never through a shared per-block cursor.
    return getAlignDataFromMemByWindow(pResult, pAlignGrpMgr, ppBlock, finished);
  }
  // The loop below now only serves EVENT streams (exactWindow); their cursor
  // based scan is intentionally left unchanged.
  for (;;) {
    if (pResult->blockIndex >= pAlignGrpMgr->blocksInMem->size) {
      *finished = true;
      return code;
    }

    int32_t             blockIndex = pResult->blockIndex;
    SAlignBlocksInMem** ppBlockInfo = (SAlignBlocksInMem**)taosArrayGet(pAlignGrpMgr->blocksInMem, blockIndex);
    SAlignBlocksInMem*  pBlockInfo = *ppBlockInfo;
    if (pBlockInfo == NULL) {
      stError("failed to get block info from mem, since block is NULL");
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    while (pResult->winIndex < pBlockInfo->nWindow) {
      if (pBlockInfo->dataLen < 0 || pBlockInfo->dataLen > pBlockInfo->capacity || pResult->offset < 0 ||
          pResult->offset > pBlockInfo->dataLen ||
          pBlockInfo->dataLen - pResult->offset < sizeof(SSlidingWindowInMem)) {
        return TSDB_CODE_INVALID_MSG;
      }
      SSlidingWindowInMem* pWindowData = (SSlidingWindowInMem*)((char*)pBlockInfo + sizeof(SAlignBlocksInMem) + pResult->offset);

      int64_t windowSize =
          pWindowData->dataLen == 0 ? sizeof(SSlidingWindowInMem) + sizeof(SMoveWindowInfo) : pWindowData->dataLen;
      if (windowSize < (int64_t)sizeof(SSlidingWindowInMem) || windowSize > pBlockInfo->dataLen - pResult->offset) {
        return TSDB_CODE_INVALID_MSG;
      }
      bool found = false;
      bool windowMatched =
          pWindowData->startTime == pResult->reqStartTime && pWindowData->endTime == pResult->reqEndTime &&
          strcmp(pWindowData->eventConditionPath, pResult->eventConditionPath) == 0;
      if (pResult->exactWindowMatched && !windowMatched) {
        *finished = true;
        return code;
      }
      if (windowMatched) {
        found = true;
        if (pWindowData->dataLen == 0) {
          SMoveWindowInfo* pMoveWinInfo = getWindowDataBuf(pWindowData);
          if (pMoveWinInfo->pData != NULL) {
            *ppBlock = pMoveWinInfo->pData;
            pMoveWinInfo->pData = NULL;
          }
          (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pMoveWinInfo->moveSize);
        } else {
          code = getRangeInWindowBlock(pWindowData, pWindowData->dataLen - (int64_t)sizeof(SSlidingWindowInMem),
                                       pResult->tsColSlotId, pResult->reqStartTime, pResult->reqEndTime, ppBlock);
          if (code) {
            return code;
          }
        }
        pResult->exactWindowMatched = true;
      }

      if (++pResult->winIndex >= pBlockInfo->nWindow) {
        pResult->winIndex = 0;
        pResult->offset = 0;
        pResult->blockIndex++;
        if (pResult->blockIndex >= pAlignGrpMgr->blocksInMem->size) {
          *finished = true;
          return code;
        }
        if (!found) {
          break;  // break the while loop
        }
      } else {
        pResult->offset += windowSize;
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
        (void)atomic_add_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, pSlidingGrpMgr->usedMemSize);
      }
    } else {
      (void)atomic_add_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, pSlidingGrpMgr->usedMemSize);
      (void)atomic_sub_fetch_64(&g_slidigGrpMemList.waitMoveMemSize, *oldSize);
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
      return getRangeInWindowBlock(pWindowData, pWindowData->dataLen, pResult->tsColSlotId, pResult->reqStartTime,
                                   pResult->reqEndTime, ppBlock);
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
  (void)atomic_add_fetch_64(&pGrpCacheMgr->usedMemSize, size);
  (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, size);
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
    // pointer movement is completed while reading data
    SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
    return pAlignGrpMgr->blocksInMem->size == 0;
  }
  return true;
}

int32_t buildSlidingWindowInMem(SSDataBlock* pBlock, int32_t tsColSlotId, int32_t startIndex, int32_t endIndex,
                                const char* eventConditionPath, SSlidingWindowInMem** ppSlidingWinInMem) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  int64_t encodeSize = blockGetEncodeSizeOfRows(pBlock, startIndex, endIndex);
  if (encodeSize <= 0) {
    stError("failed to get encode size of rows, startIndex:%d endIndex:%d rows:%" PRId64, startIndex, endIndex,
            pBlock->info.rows);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  size_t dataEncodeBufSize = (size_t)encodeSize;
  char*  buf = taosMemoryCalloc(1, sizeof(SSlidingWindowInMem) + dataEncodeBufSize);
  if (buf == NULL) {
    return terrno;
  }
  *ppSlidingWinInMem = (SSlidingWindowInMem*)buf;
  code = getStreamBlockTS(pBlock, tsColSlotId, endIndex, &(*ppSlidingWinInMem)->endTime);
  QUERY_CHECK_CODE(code, lino, _end);

  code = getStreamBlockTS(pBlock, tsColSlotId, startIndex, &(*ppSlidingWinInMem)->startTime);
  QUERY_CHECK_CODE(code, lino, _end);
  if (eventConditionPath != NULL) {
    tstrncpy((*ppSlidingWinInMem)->eventConditionPath, eventConditionPath,
             sizeof((*ppSlidingWinInMem)->eventConditionPath));
  }

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
    (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pSlidingWinInMem->dataLen + sizeof(SSlidingWindowInMem));
    taosMemoryFree(pSlidingWinInMem);
  }
}

void destroySlidingWindowInMemPP(void* pData) {
  SSlidingWindowInMem* pSlidingWinInMem = *(SSlidingWindowInMem**)pData;
  if (pSlidingWinInMem) {
    (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pSlidingWinInMem->dataLen + sizeof(SSlidingWindowInMem));
    taosMemoryFree(pSlidingWinInMem);
    *(SSlidingWindowInMem**)pData = NULL;
  }
}

void destroyAlignBlockInMemPP(void* ppData) {
  SAlignBlocksInMem* pAlignBlockInfo = *(SAlignBlocksInMem**)ppData;
  if (pAlignBlockInfo) {
    (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize,
                              DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem) + pAlignBlockInfo->consumedCap);
    taosMemoryFree(pAlignBlockInfo->pConsumed);
    pAlignBlockInfo->pConsumed = NULL;
    pAlignBlockInfo->consumedCap = 0;
    taosMemoryFree(pAlignBlockInfo);
    *(SAlignBlocksInMem**)ppData = NULL;
  }
}

void destroyAlignBlockInMem(void* pData) {
  SAlignBlocksInMem* pAlignBlockInfo = (SAlignBlocksInMem*)pData;
  if (pAlignBlockInfo) {
    int32_t consumedCap = pAlignBlockInfo->consumedCap;
    taosMemoryFree(pAlignBlockInfo->pConsumed);
    pAlignBlockInfo->pConsumed = NULL;
    pAlignBlockInfo->consumedCap = 0;
    taosMemoryFree(pAlignBlockInfo);
    (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize,
                              DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem) + consumedCap);
  }
}

int32_t getEnoughBuffWindow(SAlignGrpMgr* pAlignGrpMgr, size_t dataEncodeBufSize,
                            SAlignBlocksInMem** ppAlignBlockInfo) {
  if (pAlignGrpMgr->blocksInMem == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  // A window is stored as one contiguous record, and the same DS_FILE_BLOCK_SIZE limit is
  // enforced when the memory block is flushed to a file (dataSinkFile.c). Reporting success for
  // a larger request would hand the encoder a block smaller than it is told it can use.
  if (dataEncodeBufSize > DS_FILE_BLOCK_SIZE) {
    stError("window data size:%zu exceeds align block capacity:%d", dataEncodeBufSize, DS_FILE_BLOCK_SIZE);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SAlignBlocksInMem* pAlignBlockInfo = NULL;
  if (pAlignGrpMgr->blocksInMem->size > 0) {
    pAlignBlockInfo =
        *(SAlignBlocksInMem**)taosArrayGet(pAlignGrpMgr->blocksInMem, pAlignGrpMgr->blocksInMem->size - 1);
    if (pAlignBlockInfo && pAlignBlockInfo->dataLen <= pAlignBlockInfo->capacity &&
        pAlignBlockInfo->capacity - pAlignBlockInfo->dataLen >= dataEncodeBufSize) {
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

  (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, DS_FILE_BLOCK_SIZE + sizeof(SAlignBlocksInMem));
  return TSDB_CODE_SUCCESS;
}

/* Release the unread fragments of the window a finished or cancelled read was serving. A non-exact
 * (immediate-clean) stream can store one logical window as several cache records; a LIMIT projection may
 * finish before every fragment is fetched, and the request cleanup path would otherwise leave them behind.
 * Those records then keep their block's consumedCount below nWindow, so the block is never reclaimed and
 * later reads rescan the stale fragments. Drop exactly this request's fragments and reclaim the block once
 * every window it holds has been consumed, leaving other windows untouched so out-of-order reads keep
 * working. */
int32_t alignReleaseWindowFragments(SAlignGrpMgr* pAlignGrpMgr, int64_t reqStartTime, int64_t reqEndTime,
                                    const char* eventConditionPath) {
  if (pAlignGrpMgr == NULL || pAlignGrpMgr->blocksInMem == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t blockIdx = 0; blockIdx < taosArrayGetSize(pAlignGrpMgr->blocksInMem);) {
    SAlignBlocksInMem** ppBlockInfo = taosArrayGet(pAlignGrpMgr->blocksInMem, blockIdx);
    if (ppBlockInfo == NULL || *ppBlockInfo == NULL) {
      ++blockIdx;
      continue;
    }
    SAlignBlocksInMem* pBlockInfo = *ppBlockInfo;
    int64_t            offset = 0;
    for (int32_t winIndex = 0; winIndex < pBlockInfo->nWindow; ++winIndex) {
      SSlidingWindowInMem* pWindowData =
          (SSlidingWindowInMem*)((char*)pBlockInfo + sizeof(SAlignBlocksInMem) + offset);
      int64_t windowSize = pWindowData->dataLen == 0 ? sizeof(SSlidingWindowInMem) + sizeof(SMoveWindowInfo)
                                                     : pWindowData->dataLen;
      offset += windowSize;
      if (alignWindowConsumed(pBlockInfo, winIndex)) {
        continue;
      }
      if (pWindowData->startTime != reqStartTime || pWindowData->endTime != reqEndTime) {
        continue;
      }
      if (eventConditionPath != NULL && eventConditionPath[0] != '\0' &&
          strcmp(pWindowData->eventConditionPath, eventConditionPath) != 0) {
        continue;
      }
      if (pWindowData->dataLen == 0) {
        SMoveWindowInfo* pMoveWinInfo = getWindowDataBuf(pWindowData);
        if (pMoveWinInfo->pData != NULL) {
          blockDataDestroy(pMoveWinInfo->pData);
          pMoveWinInfo->pData = NULL;
          (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pMoveWinInfo->moveSize);
        }
      }
      if (pBlockInfo->pConsumed != NULL && winIndex < pBlockInfo->consumedCap && !pBlockInfo->pConsumed[winIndex]) {
        pBlockInfo->pConsumed[winIndex] = 1;
        ++pBlockInfo->consumedCount;
      }
    }
    if (pBlockInfo->nWindow > 0 && pBlockInfo->consumedCount == pBlockInfo->nWindow) {
      stDebug("release align cache block after window read finished, groupId:%" PRId64 " windows:%d",
              pAlignGrpMgr->groupId, pBlockInfo->nWindow);
      destroyAlignBlockInMemPP(ppBlockInfo);
      taosArrayRemove(pAlignGrpMgr->blocksInMem, blockIdx);
    } else {
      ++blockIdx;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/* Drop the block a caller just obtained but could not use: only a block that holds neither a
 * window nor data is removed, so a reused block that already carries windows is never touched.
 * Without this, a failed flags allocation would leave an empty block in the group cache that
 * the reader never reclaims (it only releases blocks with nWindow > 0). */
static void dropUnusedAlignBlock(SAlignGrpMgr* pAlignGrpMgr, SAlignBlocksInMem* pAlignBlockInfo) {
  if (pAlignGrpMgr == NULL || pAlignGrpMgr->blocksInMem == NULL || pAlignBlockInfo == NULL) {
    return;
  }
  if (pAlignBlockInfo->nWindow != 0 || pAlignBlockInfo->dataLen != 0) {
    return;
  }
  // getEnoughBuffWindow only hands out the trailing block, and a block that is still referenced by
  // the array must not be freed: releasing it here would leave a dangling pointer behind.
  int32_t lastIdx = taosArrayGetSize(pAlignGrpMgr->blocksInMem) - 1;
  if (lastIdx < 0) {
    return;
  }
  SAlignBlocksInMem** ppLast = taosArrayGet(pAlignGrpMgr->blocksInMem, lastIdx);
  if (ppLast == NULL || *ppLast != pAlignBlockInfo) {
    stError("cannot release align block since it is not the trailing block, lastIdx:%d", lastIdx);
    return;
  }
  taosArrayRemove(pAlignGrpMgr->blocksInMem, lastIdx);
  destroyAlignBlockInMem(pAlignBlockInfo);
}

int32_t buildAlignWindowInMemBlock(SAlignGrpMgr* pAlignGrpMgr, SSDataBlock* pBlock, int32_t tsColSlotId, TSKEY wstart,
                                   TSKEY wend, const char* eventConditionPath, int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  int64_t encodeSize = blockGetEncodeSizeOfRows(pBlock, startIndex, endIndex);
  if (encodeSize <= 0) {
    stError("failed to get encode size of rows, startIndex:%d endIndex:%d rows:%" PRId64, startIndex, endIndex,
            pBlock->info.rows);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  size_t dataEncodeBufSize = (size_t)encodeSize;
  size_t buffSize = sizeof(SSlidingWindowInMem) + dataEncodeBufSize;

  SAlignBlocksInMem* pAlignBlockInfo = NULL;
  code = getEnoughBuffWindow(pAlignGrpMgr, buffSize, &pAlignBlockInfo);
  QUERY_CHECK_CODE(code, lino, _end);
  code = ensureAlignConsumedCapacity(pAlignBlockInfo, pAlignBlockInfo->nWindow + 1);
  if (code != TSDB_CODE_SUCCESS) {
    dropUnusedAlignBlock(pAlignGrpMgr, pAlignBlockInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SSlidingWindowInMem* pSlidingWinInMem = (SSlidingWindowInMem*)(getNextBuffStart(pAlignBlockInfo));
  pSlidingWinInMem->endTime = wend;
  pSlidingWinInMem->startTime = wstart;
  pSlidingWinInMem->dataLen = buffSize;
  if (eventConditionPath != NULL) {
    tstrncpy(pSlidingWinInMem->eventConditionPath, eventConditionPath, sizeof(pSlidingWinInMem->eventConditionPath));
  }

  char*   pStart = getWindowDataBuf(pSlidingWinInMem);
  int32_t len = 0;
  // pass the real writable tail of the block instead of the requested size, so a wrong size
  // estimate can not make the encoder write past the end of the allocated block
  size_t used = pAlignBlockInfo->dataLen + sizeof(SSlidingWindowInMem);
  size_t writable = pAlignBlockInfo->capacity > used ? pAlignBlockInfo->capacity - used : 0;
  code = blockEncodeAsRows(pBlock, pStart, writable, numOfCols, startIndex, endIndex, &len);
  QUERY_CHECK_CODE(code, lino, _end);
  if (len < 0) {
    stError("failed to encode data since %s, lineno:%d", tstrerror(len), lino);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  setUsedBlockBuf(pAlignBlockInfo, buffSize);
  alignRecordWindowOrder(pAlignGrpMgr, wstart);

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
  code = ensureAlignConsumedCapacity(pAlignBlockInfo, pAlignBlockInfo->nWindow + 1);
  if (code != TSDB_CODE_SUCCESS) {
    dropUnusedAlignBlock(pAlignGrpMgr, pAlignBlockInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SSlidingWindowInMem* pSlidingWinInMem = (SSlidingWindowInMem*)(getNextBuffStart(pAlignBlockInfo));
  pSlidingWinInMem->endTime = wend;
  pSlidingWinInMem->startTime = wstart;
  pSlidingWinInMem->dataLen = 0;  // move data, so no data length

  char*            pStart = getWindowDataBuf(pSlidingWinInMem);
  SMoveWindowInfo* pMoveInfo = (SMoveWindowInfo*)pStart;
  pMoveInfo->moveSize = moveSize;
  pMoveInfo->pData = pBlock;
  (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, moveSize);
  setUsedBlockBuf(pAlignBlockInfo, dataEncodeBufSize);
  alignRecordWindowOrder(pAlignGrpMgr, wstart);

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
    (void)changeMgrStatus(&pSlidingGrp->status, GRP_DATA_IDLE);
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
    (void)changeMgrStatus(&pSlidingGrp->status, GRP_DATA_IDLE);
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
