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
#include "osAtomic.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"

extern SDataSinkManager2 g_pDataSinkManager;

int32_t writeToCache(SStreamTaskDSManager* pStreamDataSink, SGroupDSManager* pGroupDataInfoMgr, TSKEY wstart, TSKEY wend,
                     SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SWindowData* pWindowData = NULL;

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  // todo dataEncodeBufSize > real len
  size_t dataEncodeBufSize = blockGetEncodeSizeOfRows(pBlock, startIndex, endIndex);
  char*  buf = taosMemoryCalloc(1, dataEncodeBufSize);
  char*  pStart = buf;

  if (pStart == NULL) {
    return terrno;
  }

  int32_t len = 0;
  code = blockEncodeAsRows(pBlock, pStart, dataEncodeBufSize, numOfCols, startIndex, endIndex, &len);
  QUERY_CHECK_CODE(code, lino, _end);

  pWindowData = (SWindowData*)taosMemoryCalloc(1, sizeof(SWindowData));
  if (pWindowData == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pWindowData->pGroupDataInfoMgr = pGroupDataInfoMgr;
  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  code = getStreamBlockTS(pBlock, startIndex, pStreamDataSink->tsSlotId, &pWindowData->start);
  QUERY_CHECK_CODE(code, lino, _end);

  code = getStreamBlockTS(pBlock, endIndex, pStreamDataSink->tsSlotId, &pWindowData->end);
  QUERY_CHECK_CODE(code, lino, _end);

  pWindowData->saveMode = DATA_SAVEMODE_BUFF;
  pWindowData->dataLen = dataEncodeBufSize;
  pWindowData->pDataBuf = buf;
  if (pGroupDataInfoMgr->windowDataInMem == NULL) {
    pGroupDataInfoMgr->windowDataInMem = taosArrayInit(0, sizeof(SWindowData*));
    if (pGroupDataInfoMgr->windowDataInMem == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  if (taosArrayPush(pGroupDataInfoMgr->windowDataInMem, &pWindowData) == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pGroupDataInfoMgr->lastWstartInMem = wstart;
  syncWindowDataMemAdd(pWindowData);
  return TSDB_CODE_SUCCESS;
_end:
  if (pWindowData) {
    taosMemoryFree(pWindowData);
  }
  if (buf) {
    taosMemoryFree(buf);
  }
  return code;
}

int32_t moveToCache(SStreamTaskDSManager* pStreamDataSink, SGroupDSManager* pGroupDataInfoMgr, TSKEY wstart, TSKEY wend,
                    SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SWindowData* pWindowData = NULL;

  size_t       dataEncodeBufSize = blockGetEncodeSize(pBlock);
  pWindowData = (SWindowData*)taosMemoryCalloc(1, sizeof(SWindowData));
  if (pWindowData == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pWindowData->pGroupDataInfoMgr = pGroupDataInfoMgr;
  pWindowData->wstart = wstart;
  pWindowData->wend = wend;
  code = getStreamBlockTS(pBlock, 0, pStreamDataSink->tsSlotId, &pWindowData->start);
  QUERY_CHECK_CODE(code, lino, _end);
  code = getStreamBlockTS(pBlock, pBlock->info.rows - 1, pStreamDataSink->tsSlotId, &pWindowData->end);
  QUERY_CHECK_CODE(code, lino, _end);
  pWindowData->saveMode = DATA_SAVEMODE_BLOCK;
  pWindowData->dataLen = dataEncodeBufSize;
  pWindowData->pDataBuf = pBlock;
  if (pGroupDataInfoMgr->windowDataInMem == NULL) {
    pGroupDataInfoMgr->windowDataInMem = taosArrayInit(0, sizeof(SWindowData*));
    if (pGroupDataInfoMgr->windowDataInMem == NULL) {
      // taosMemoryFree(pBlcok);
      // When it fails, pBlock returns it to the caller for processing
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  if (taosArrayPush(pGroupDataInfoMgr->windowDataInMem, &pWindowData) == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pGroupDataInfoMgr->lastWstartInMem = wstart;
  syncWindowDataMemAdd(pWindowData);
  return TSDB_CODE_SUCCESS;
  _end:
  if (pWindowData) {
    taosMemoryFree(pWindowData);
  }
  return code;
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

static int32_t getRangeInWindowBlock(SWindowData* pWindowData, int32_t tsColSlotId, TSKEY start, TSKEY end, SSDataBlock** ppBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockSpecialDecodeLaterPart(pBlock, pWindowData->pDataBuf, tsColSlotId, start, end);
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

int32_t readDataFromCache(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished) {
  SGroupDSManager* pGroupData = (SGroupDSManager*)pResult->groupData;
  SWindowData**    ppWindowData = (SWindowData**)taosArrayGet(pGroupData->windowDataInMem, pResult->offset);
  if (ppWindowData == NULL || *ppWindowData == NULL) {
    stError("failed to get data from cache, offset:%" PRId64, pResult->offset);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if ((*ppWindowData)->start > pResult->reqEndTime) {
    *finished = true;
    return TSDB_CODE_SUCCESS;
  } else if ((*ppWindowData)->end < pResult->reqStartTime) {
    *finished = false;  // 查看下一个窗口
    return TSDB_CODE_SUCCESS;
  }

  if (pGroupData->pSinkManager->cleanMode == DATA_CLEAN_IMMEDIATE) {
    return getWindowDataInMem(pResult, *ppWindowData, ppBlock, DATA_CLEAN_IMMEDIATE);
  } else {
    return getSlidingWindowDataInMem(pResult, pGroupData, *ppWindowData, ppBlock);
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
    if (pWindowData && (pWindowData->wend <= start) || pWindowData->pDataBuf == NULL) {
      deleteCount++;
    } else {
      break;
    }
  }

  if (deleteCount == 0) {
    return;
  }
  taosArrayRemoveBatch(pGroupData->windowDataInMem, 0, deleteCount, destorySWindowDataPP);
}

void syncWindowDataMemAdd(SWindowData* pWindowData) {
  int32_t size = pWindowData->dataLen;
  atomic_add_fetch_64(&pWindowData->pGroupDataInfoMgr->usedMemSize, size);
  atomic_add_fetch_64(&pWindowData->pGroupDataInfoMgr->pSinkManager->usedMemSize, size);
  atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

void syncWindowDataMemSub(SWindowData* pWindowData) {
  int32_t size = pWindowData->dataLen;
  atomic_sub_fetch_64(&pWindowData->pGroupDataInfoMgr->usedMemSize, size);
  atomic_sub_fetch_64(&pWindowData->pGroupDataInfoMgr->pSinkManager->usedMemSize, size);
  atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, size);
}

void clearGroupExpiredData(SGroupDSManager* pGroupData, TSKEY start) {
  if (pGroupData->windowDataInMem == NULL) {
    return;
  }
  clearGroupExpiredDataInMem(pGroupData, start);
}

SGroupDSManager* getGroupDataInfo(SStreamTaskDSManager* pStreamData, int64_t groupId) {
  SGroupDSManager** ppGroupData =
      (SGroupDSManager**)taosHashGet(pStreamData->DataSinkGroupList, &groupId, sizeof(groupId));
  if (ppGroupData == NULL) {
    return NULL;
  }
  return *ppGroupData;
}

int32_t getFirstDataIterFromCache(SStreamTaskDSManager* StreamTaskDSMgr, int64_t groupId, TSKEY start, TSKEY end, void** ppResult) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGroupDSManager* pGroupDataInfo = getGroupDataInfo(StreamTaskDSMgr, groupId);
  if (pGroupDataInfo == NULL) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }

  clearGroupExpiredData(pGroupDataInfo, start);

  if (!pGroupDataInfo->windowDataInMem || pGroupDataInfo->windowDataInMem->size == 0) {
    *ppResult = NULL;
    return TSDB_CODE_SUCCESS;
  }
  SResultIter* pResult = taosMemoryCalloc(1, sizeof(SResultIter));
  if (ppResult == NULL) {
    return terrno;
  }
  if (pGroupDataInfo->windowDataInMem && pGroupDataInfo->windowDataInMem->size != 0) {
    pResult->groupData = pGroupDataInfo;
    pResult->offset = 0;
    pResult->dataPos = DATA_SINK_MEM;
    pResult->groupId = groupId;
    pResult->reqStartTime = start;
    pResult->reqEndTime = end;
    pResult->tsColSlotId = StreamTaskDSMgr->tsSlotId;
    *ppResult = pResult;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

bool setNextIteratorFromCache(SResultIter** ppResult) {
  SResultIter*     pResult = *ppResult;
  SGroupDSManager* pGroupData = (SGroupDSManager*)pResult->groupData;

  pResult->offset++;
  if (pResult->offset < taosArrayGetSize(pGroupData->windowDataInMem)) {
    SWindowData* pWindowData = *(SWindowData**)taosArrayGet(pGroupData->windowDataInMem, pResult->offset);
    if ((pResult->reqStartTime >= pWindowData->start && pResult->reqStartTime <= pWindowData->end) ||
        (pResult->reqEndTime >= pWindowData->start && pResult->reqEndTime <= pWindowData->end) ||
        (pWindowData->start >= pResult->reqStartTime && pWindowData->end <= pResult->reqEndTime)) {
      return false;
    } else {
      releaseDataIterator((void**)ppResult);
      *ppResult = NULL;
      return false;  // 后续数据已超出时间范围，结束查找
    }
  }
  // *ppResult = NULL;
  return true;  // 内存没有数据，还需要查看文件
}
