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

#include <stdint.h>
#include <stdio.h>
#include "dataSink.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

int32_t writeToCache(SStreamTaskDSManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                     SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGroupDSManager* pGroupDataInfo = NULL;
  code = getOrCreateSGroupDSManager(pStreamDataSink, groupId, &pGroupDataInfo);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  char*  buf = taosMemoryCalloc(1, dataEncodeBufSize);
  char*  pStart = buf;

  if (pStart == NULL) {
    return terrno;
  }

  int32_t len = blockEncode(pBlock, pStart, dataEncodeBufSize, numOfCols);
  if (len < 0) {
    taosMemoryFree(buf);
    stError("failed to encode data since %s", tstrerror(terrno));
    return terrno;
  }
  SWindowData* pWindowData = (SWindowData*)taosMemoryCalloc(1, sizeof(SWindowData));
  if (pWindowData == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }
  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  pWindowData->saveMode = DATA_SAVEMODE_BUFF;
  pWindowData->dataLen = dataEncodeBufSize;
  pWindowData->pDataBuf = buf;
  if (pGroupDataInfo->windowDataInMem == NULL) {
    pGroupDataInfo->windowDataInMem = taosArrayInit(0, sizeof(SWindowData*));
    if (pGroupDataInfo->windowDataInMem == NULL) {
      taosMemoryFree(buf);
      return terrno;
    }
  }
  if (taosArrayPush(pGroupDataInfo->windowDataInMem, &pWindowData) == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }
  pGroupDataInfo->usedMemSize += dataEncodeBufSize;
  pGroupDataInfo->lastWstartInMem = wstart;
  return TSDB_CODE_SUCCESS;
}

int32_t moveToCache(SStreamTaskDSManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                    SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGroupDSManager* pGroupDataInfo = NULL;
  code = getOrCreateSGroupDSManager(pStreamDataSink, groupId, &pGroupDataInfo);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }

  size_t       dataEncodeBufSize = blockGetEncodeSize(pBlock);
  SWindowData* pWindowData = (SWindowData*)taosMemoryCalloc(1, sizeof(SWindowData));
  if (pWindowData == NULL) {
    return terrno;
  }
  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  pWindowData->saveMode = DATA_SAVEMODE_BLOCK;
  pWindowData->dataLen = dataEncodeBufSize;
  pWindowData->pDataBuf = pBlock;
  if (pGroupDataInfo->windowDataInMem == NULL) {
    pGroupDataInfo->windowDataInMem = taosArrayInit(0, sizeof(SWindowData*));
    if (pGroupDataInfo->windowDataInMem == NULL) {
      // taosMemoryFree(pBlcok);
      // When it fails, pBlock returns it to the caller for processing
      return terrno;
    }
  }
  if (taosArrayPush(pGroupDataInfo->windowDataInMem, &pWindowData) == NULL) {
    return terrno;
  }
  pGroupDataInfo->usedMemSize += dataEncodeBufSize;
  pGroupDataInfo->lastWstartInMem = wstart;
  return TSDB_CODE_SUCCESS;
}

static void freeWindowsBufferImmediate(SWindowData* pWindowData) {
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

static int32_t getRangeInWindowBlock(SWindowData* pWindowData, TSKEY start, TSKEY end, SSDataBlock** ppBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockSpecialDecodeLaterPart(pBlock, pWindowData->pDataBuf, start, end);
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

static int32_t getSlidingWindowLaterDataInMem(SWindowData* pWindowData, TSKEY start, SSDataBlock** ppBlock) {
  return getRangeInWindowBlock(pWindowData, start, INT64_MAX, ppBlock);
}

static int32_t getSlidingWindowEarlierDataInMem(SWindowData* pWindowData, TSKEY end, SSDataBlock** ppBlock) {
  return getRangeInWindowBlock(pWindowData, INT64_MIN, end, ppBlock);
}

static int32_t getSlidingWindowDataInMem(SResultIter* pResult, SWindowData* pWindowData, SSDataBlock** ppBlock) {
  if (pWindowData->wstart >= pResult->reqStartTime && pWindowData->wend <= pResult->reqEndTime) {
    return getWindowDataInMem(pResult, pWindowData, ppBlock, DATA_CLEAN_EXPIRED);
  } else if (pResult->reqStartTime >= pWindowData->wstart && pResult->reqStartTime <= pWindowData->wend) {
    return getSlidingWindowLaterDataInMem(pWindowData, pResult->reqStartTime, ppBlock);
  } else {  // (pResult->reqEndTime >= pWindowData->wstart && pResult->reqEndTime <= pWindowData->wend)
    return getSlidingWindowEarlierDataInMem(pWindowData, pResult->reqEndTime, ppBlock);
  }
}

int32_t readDataFromCache(SResultIter* pResult, SSDataBlock** ppBlock) {
  SGroupDSManager* pGroupData = pResult->groupData;
  SWindowData**    ppWindowData = (SWindowData**)taosArrayGet(pGroupData->windowDataInMem, pResult->offset);
  if (ppWindowData == NULL || *ppWindowData == NULL) {
    stError("failed to get data from cache, offset:%" PRId64, pResult->offset);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pGroupData->pSinkManager->cleanMode == DATA_CLEAN_IMMEDIATE) {
    return getWindowDataInMem(pResult, *ppWindowData, ppBlock, DATA_CLEAN_IMMEDIATE);
  } else {
    return getSlidingWindowDataInMem(pResult, *ppWindowData, ppBlock);
  }
}

void clearGroupExpiredDataInMem(SGroupDSManager* pGroupData, TSKEY start) {
  if (pGroupData->windowDataInMem == NULL) {
    return;
  }

  int32_t size = taosArrayGetSize(pGroupData->windowDataInMem);
  int     deleteCount = 0;
  for (int i = 0; i < size; ++i) {
    SWindowData* pWindowData = *(SWindowData**)taosArrayGet(pGroupData->windowDataInMem, i);
    if (pWindowData && pWindowData->wend <= start) {
      deleteCount++;
    } else {
      break;
    }
  }

  taosArrayRemoveBatch(pGroupData->windowDataInMem, 0, deleteCount, destorySWindowDataPP);
}
