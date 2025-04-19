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
#include "tdef.h"

static SDataSinkManager2 g_pDataSinkManager = {0};
#define DATA_SINK_MAX_MEM_SIZE_DEFAULT (1024 * 1024 * 1024)  // 1G

int32_t initStreamDataSinkOnce() {
  static int8_t init = 0;
  int8_t        flag = atomic_val_compare_exchange_8(&init, 0, 1);
  if (flag != 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  code = initDataSinkFileDir();
  if (code != 0) {
    stError("failed to create data sink file dir, err: %s", terrMsg);
    return code;
  }

  g_pDataSinkManager.usedMemSize = 0;
  g_pDataSinkManager.maxMemSize = DATA_SINK_MAX_MEM_SIZE_DEFAULT;
  g_pDataSinkManager.fileBlockSize = 0;
  g_pDataSinkManager.readDataFromMemTimes = 0;
  g_pDataSinkManager.readDataFromFileTimes = 0;
  g_pDataSinkManager.DataSinkStreamList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (g_pDataSinkManager.DataSinkStreamList == NULL) {
    return -1;
  }
  init = true;
  return TSDB_CODE_SUCCESS;
};

SGroupDSManager* getGroupDataInfo(SStreamTaskDSManager* pStreamData, int64_t groupId) {
  SGroupDSManager** ppGroupData =
      (SGroupDSManager**)taosHashGet(pStreamData->DataSinkGroupList, &groupId, sizeof(groupId));
  if (ppGroupData == NULL) {
    return NULL;
  }
  return *ppGroupData;
}

void clearGroupExpiredData(SGroupDSManager* pGroupData, TSKEY start) {
  if (pGroupData->windowDataInMem == NULL) {
    return;
  }
  // read lock SRWLatch
  int32_t size = taosArrayGetSize(pGroupData->windowDataInMem);
  for (int32_t i = 0; i < size; ++i) {
    SDataSinkFileHeader* pHeader = (SDataSinkFileHeader*)taosArrayGet(pGroupData->windowDataInMem, i);
    if (pHeader->endTime < start) {
      taosArrayRemove(pGroupData->windowDataInMem, i);
    }
  }
}

int32_t getFirstDataIter(SGroupDSManager* pGroupDataInfo, TSKEY start, TSKEY end, void** ppResult) {
  int32_t code = TSDB_CODE_SUCCESS;
  clearGroupExpiredData(pGroupDataInfo, start);

  if ((!pGroupDataInfo->windowDataInMem || pGroupDataInfo->windowDataInMem->size == 0) &&
      (!pGroupDataInfo->windowDataInFile || pGroupDataInfo->windowDataInFile->size == 0)) {
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
    pResult->reqStartTime = start;
    pResult->reqEndTime = end;
    *ppResult = pResult;
    return code;
  }
  if (pGroupDataInfo->windowDataInMem && pGroupDataInfo->windowDataInFile->size != 0) {
    pResult->groupData = pGroupDataInfo;
    pResult->offset = 0;
    pResult->dataPos = DATA_SINK_FILE;
    pResult->reqStartTime = start;
    pResult->reqEndTime = end;
    *ppResult = pResult;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static bool shouldWriteIntoFile(SStreamTaskDSManager* pStreamDataSink, int64_t groupId) {
  // 如果当前 task 已经开始在文件中写入数据，则继续写入文件，保证文件中数据时间总是大于内存中数据，并且数据连续
  // 为防止某个 task 一直在文件中写入数据，当内存使用小于 50% 时，读取这个任务所有文件数据迁入内存
  if (pStreamDataSink->pFile && pStreamDataSink->pFile->fileBlockUsedCount > 0) {
    return true;
  }

  // 内存使用小于 70% 时，写入内存
  if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.7) {
    return false;
  }
  // 内存使用大于 70% 但小于 90%，旧的 task 继续写入内存
  if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.9 && pStreamDataSink->usedMemSize > 0) {
    return false;
  }
  // 内存使用大于 70% 并且是新的 task，或者内存使用大于 90% 全部写入文件
  return true;
}

static bool isManagerReady() {
  if (g_pDataSinkManager.DataSinkStreamList != NULL) {
    return true;
  }
  return false;
}

static int32_t createSStreamDataSinkManager(int64_t streamId, int64_t taskId, int32_t cleanMode,
                                            SStreamTaskDSManager** ppStreamDataSink) {
  SStreamTaskDSManager* pStreamDataSink = taosMemoryCalloc(1, sizeof(SStreamTaskDSManager));
  if (pStreamDataSink == NULL) {
    return terrno;
  }
  pStreamDataSink->streamId = streamId;
  pStreamDataSink->taskId = taskId;
  pStreamDataSink->cleanMode = cleanMode;
  pStreamDataSink->pFile = NULL;

  *ppStreamDataSink = pStreamDataSink;
  return TSDB_CODE_SUCCESS;
}

static void destroySStreamDataSinkManager(SStreamTaskDSManager* pStreamDataSink) {
  if (pStreamDataSink->pFile) {
    taosCloseFile(pStreamDataSink->pFile->pFile);
    taosMemoryFreeClear(pStreamDataSink->pFile);
  }
  taosMemoryFreeClear(pStreamDataSink);
}

// @brief 初始化数据缓存
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int32_t cleanMode, void** ppCache) {
  int32_t code = initStreamDataSinkOnce();
  if (code != 0) {
    return code;
  }

  char key[128] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64, streamId, taskId);
  SStreamTaskDSManager* pStreamDataSink = NULL;
  pStreamDataSink = (SStreamTaskDSManager*)taosHashGet(g_pDataSinkManager.DataSinkStreamList, key, strlen(key));
  if (pStreamDataSink == NULL) {
    code = createSStreamDataSinkManager(streamId, taskId, cleanMode, &pStreamDataSink);
    if (code != 0) {
      code = taosHashPut(g_pDataSinkManager.DataSinkStreamList, key, strlen(key), pStreamDataSink,
                         sizeof(SStreamTaskDSManager));
      if (code != 0) {
        destroySStreamDataSinkManager(pStreamDataSink);
        pStreamDataSink = NULL;
        return code;
      }
    }
    pStreamDataSink->DataSinkGroupList =
        taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pStreamDataSink->DataSinkGroupList == NULL) {
      destroySStreamDataSinkManager(pStreamDataSink);
      pStreamDataSink = NULL;
      return terrno;
    }
    pStreamDataSink->usedMemSize = 0;
    pStreamDataSink->cleanMode = cleanMode;
  }
  *ppCache = pStreamDataSink;
  return TSDB_CODE_SUCCESS;
}

// @brief 销毁数据缓存
void destroyStreamDataCache(void* pCache) {}

int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex) {
  if (isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (shouldWriteIntoFile((SStreamTaskDSManager*)pCache, groupId)) {
    return writeToFile((SStreamTaskDSManager*)pCache, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    return writeToCache((SStreamTaskDSManager*)pCache, groupId, wstart, wend, pBlock, startIndex, endIndex);
    ;
  }
}

int32_t getStreamDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter) {
  if (isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SGroupDSManager* pGroupDataInfo = getGroupDataInfo((SStreamTaskDSManager*)pCache, groupId);
  if (pGroupDataInfo == NULL) {
    *pIter = NULL;
    return TSDB_CODE_SUCCESS;
  }

  return getFirstDataIter(pGroupDataInfo, start, end, pIter);
}

void releaseDataIterator(void** pIter) {
  if (pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult != NULL) {
    taosMemoryFree(pResult);
    *pIter = NULL;
  }
}

void getNextIterator(SGroupDSManager* pGroupData, void** pIter) {
  if (pIter == NULL || *pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult == NULL) {
    return;
  }
  if (pResult->dataPos == DATA_SINK_MEM) {
    pResult->offset++;
    if (pResult->offset < taosArrayGetSize(pGroupData->windowDataInMem)) {
      SWindowData* pWindowData = (SWindowData*)taosArrayGet(pGroupData->windowDataInMem, pResult->offset);
      if (pWindowData->wstart >= pResult->reqStartTime && pWindowData->wend <= pResult->reqEndTime) {
        return;
      } else if (pWindowData->wstart >= pResult->reqEndTime) {
        goto _nodata;
      } else {
        stError("failed to get data from cache, get timeRange %" PRId64 ":%" PRId64 " cache timeRange %" PRId64
                ":%" PRId64,
                pResult->reqStartTime, pResult->reqEndTime, pWindowData->wstart, pWindowData->wend);
        goto _nodata;
      }
    } else {
      pResult->dataPos = DATA_SINK_FILE;
      pResult->offset = 0;
    }
  } else {
    pResult->offset++;
  }

  if (pResult->offset < taosArrayGetSize(pGroupData->windowDataInFile)) {
    SWindowData* pDataSink = (SWindowData*)taosArrayGet(pGroupData->windowDataInFile, pResult->offset);
    if (pDataSink->wstart >= pResult->reqStartTime && pDataSink->wend <= pResult->reqEndTime) {
      return;
    } else if (pDataSink->wstart >= pResult->reqEndTime) {
      releaseDataIterator(pIter);
      *pIter = NULL;
      return;
    } else {
      stError("failed to get data from file, get timeRange %" PRId64 ":%" PRId64 " cache timeRange %" PRId64
              ":%" PRId64,
              pResult->reqStartTime, pResult->reqEndTime, pDataSink->wstart, pDataSink->wend);
      goto _nodata;
    }
  }
_nodata:
  releaseDataIterator(pIter);
  *pIter = NULL;
}

int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  SGroupDSManager* pGroupData = pResult->groupData;
  if (pResult->dataPos == DATA_SINK_MEM) {
    code = readDataFromCache(pResult, ppBlock);
    QUERY_CHECK_CODE(code, lino, _end);

  } else {
    // read from file
  }
  getNextIterator(pGroupData, pIter);
_end:
  return code;
}

void cancelStreamDataCacheIterate(void** pIter) { releaseDataIterator(pIter); }

int32_t destroyDataSinkManager2() {
  // todo: destroy all data sink

  if (g_pDataSinkManager.DataSinkStreamList) {
    taosHashCleanup(g_pDataSinkManager.DataSinkStreamList);
    g_pDataSinkManager.DataSinkStreamList = NULL;
  }
  return TSDB_CODE_SUCCESS;
}
