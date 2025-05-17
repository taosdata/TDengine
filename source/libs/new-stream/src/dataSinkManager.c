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
#include "osFile.h"
#include "osMemory.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcompare.h"
#include "tdef.h"
#include "thash.h"

SDataSinkManager2 g_pDataSinkManager = {0};
int64_t           gDSMaxMemSizeDefault = (1024 * 1024 * 1024);   // 1G
int64_t           gDSFileBlockDefaultSize = (10 * 1024 * 1024);  // 10M

void setDataSinkMaxMemSize(int64_t maxMemSize) {
  if (maxMemSize >= 0) {
    gDSMaxMemSizeDefault = maxMemSize;
    g_pDataSinkManager.maxMemSize = maxMemSize;
  }
  stInfo("set data sink max mem size to %" PRId64, gDSMaxMemSizeDefault);
}

static void destroySStreamDSTaskMgr(void* pData);
int32_t     initStreamDataSinkOnce() {
  int8_t flag = atomic_val_compare_exchange_8(&g_pDataSinkManager.status, 0, 1);
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
  g_pDataSinkManager.maxMemSize = gDSMaxMemSizeDefault;
  g_pDataSinkManager.fileBlockSize = 0;
  g_pDataSinkManager.readDataFromFileTimes = 0;
  g_pDataSinkManager.dsStreamTaskList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (g_pDataSinkManager.dsStreamTaskList == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(g_pDataSinkManager.dsStreamTaskList, destroySStreamDSTaskMgr);
  stInfo("data sink manager init success, max mem size: %" PRId64, gDSMaxMemSizeDefault);
  return TSDB_CODE_SUCCESS;
};

void destorySWindowDataPP(void* pData) {
  SWindowData** ppWindowData = (SWindowData**)pData;
  if (ppWindowData == NULL || (*ppWindowData) == NULL) {
    return;
  }
  syncWindowDataMemSub(*ppWindowData);
  if ((*ppWindowData)->pDataBuf) {
    taosMemoryFree((*ppWindowData)->pDataBuf);
  }
  taosMemoryFree((*ppWindowData));
}

void destorySWindowDataP(void* pData) {
  SWindowData* pWindowData = (SWindowData*)pData;
  if (pWindowData == NULL) {
    return;
  }
  syncWindowDataMemSub(pWindowData);
  if (pWindowData->pDataBuf) {
    taosMemoryFree(pWindowData->pDataBuf);
  }
  taosMemoryFree((pWindowData));
}

static bool shouldWriteIntoFile(SSlidingTaskDSMgr* pStreamDataSink, int64_t groupId, bool isMove) {
  // 如果当前 task 已经开始在文件中写入数据，则继续写入文件，保证文件中数据时间总是大于内存中数据，并且数据连续
  // 为防止某个 task 一直在文件中写入数据，当内存使用小于 50% 时，读取这个任务所有文件数据迁入内存
  if (pStreamDataSink->pFileMgr && pStreamDataSink->pFileMgr->fileBlockUsedCount > 0) {
    return true;
  }

  // 内存使用小于 70% 时，写入内存
  if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.7) {
    return false;
  }
  // 内存使用大于 70% 但小于 90%，旧的 task 继续写入内存
  // if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.9 && pStreamDataSink->usedMemSize > 0) {
  //   return false;
  // }

  // 内存使用小于 90% 时，但是使用了 move 语义，认为已经写入了内存，计入内存管理
  if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.9 && isMove) {
    return false;
  }

  // 内存使用大于 70% 并且是新的 task，或者内存使用大于 90% 全部写入文件
  return true;
}

static bool isManagerReady() {
  if (g_pDataSinkManager.dsStreamTaskList != NULL) {
    return true;
  }
  return false;
}

SCleanMode getCleanModeFromDSMgr(void* pData) {
  if (pData == NULL) {
    return DATA_CLEAN_NONE;
  }
  STaskDSMgr* pTaskDSMgr = (STaskDSMgr*)pData;
  return pTaskDSMgr->cleanMode;
}

static void destroySlidingTaskDSMgr(SSlidingTaskDSMgr** pData) {
  SSlidingTaskDSMgr* pSlidingTaskDSMgr = *pData;
  if (pSlidingTaskDSMgr == NULL) {
    return;
  }
  if (pSlidingTaskDSMgr->pSlidingGrpList) {
    taosHashCleanup(pSlidingTaskDSMgr->pSlidingGrpList);
    pSlidingTaskDSMgr->pSlidingGrpList = NULL;
  }
  if (pSlidingTaskDSMgr->pFileMgr) {
    destroyStreamDataSinkFile(&pSlidingTaskDSMgr->pFileMgr);
  }
  taosMemoryFreeClear(pSlidingTaskDSMgr);
}

static void destroyAlignTaskDSMgr(SAlignTaskDSMgr** pData) {
  SAlignTaskDSMgr* pAlignTaskDSMgr = *pData;
  if (pAlignTaskDSMgr == NULL) {
    return;
  }

  taosMemoryFreeClear(pAlignTaskDSMgr);
}

static void destroySStreamDSTaskMgr(void* pData) {
  if (pData == NULL || *(void**)pData == NULL) {
    stError("invalid data sink manager");
    return;
  }
  SCleanMode cleanMode = getCleanModeFromDSMgr(*(void**)pData);
  if (cleanMode == DATA_CLEAN_IMMEDIATE) {
    destroySlidingTaskDSMgr((SSlidingTaskDSMgr**)pData);
    return;
  } else if (cleanMode == DATA_CLEAN_EXPIRED) {
    destroyAlignTaskDSMgr((SAlignTaskDSMgr**)pData);

    return;
  } else {
    stError("invalid clean mode: %d", cleanMode);
  }
}

int32_t createSGroupDSManager(int64_t groupId, SGroupDSManager** ppGroupDataInfo) {
  *ppGroupDataInfo = (SGroupDSManager*)taosMemoryCalloc(1, sizeof(SGroupDSManager));
  if (*ppGroupDataInfo == NULL) {
    return terrno;
  }
  (*ppGroupDataInfo)->groupId = groupId;
  (*ppGroupDataInfo)->winDataInMem = NULL;
  return TSDB_CODE_SUCCESS;
}

static void destroySGroupDSManager(void* pData) {
  SGroupDSManager* pGroupData = *(SGroupDSManager**)pData;
  if (pGroupData->winDataInMem) {
    taosArrayDestroyP(pGroupData->winDataInMem, destorySWindowDataP);
    pGroupData->winDataInMem = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

int32_t createAlignGrpMgr(int64_t groupId, SAlignGrpMgr** ppAlignGrpMgr) {
  *ppAlignGrpMgr = (SAlignGrpMgr*)taosMemoryCalloc(1, sizeof(SAlignGrpMgr));
  if (*ppAlignGrpMgr == NULL) {
    return terrno;
  }
  (*ppAlignGrpMgr)->groupId = groupId;
  (*ppAlignGrpMgr)->usedMemSize = 0;
  (*ppAlignGrpMgr)->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
  if ((*ppAlignGrpMgr)->blocksInMem == NULL) {
    taosMemoryFree(*ppAlignGrpMgr);
    stError("failed to create window data in mem, err: %s", terrMsg);
    return terrno;
  }
  (*ppAlignGrpMgr)->blocksInFile = NULL;  // delay init
  (*ppAlignGrpMgr)->status = GRP_DATA_WRITING;
  return TSDB_CODE_SUCCESS;
}

static void destroyAlignGrpMgr(void* pData) {
  SAlignGrpMgr* pGroupData = *(SAlignGrpMgr**)pData;
  if (pGroupData->blocksInMem) {
    taosArrayDestroyP(pGroupData->blocksInMem, destoryAlignBlockInMem);
    pGroupData->blocksInMem = NULL;
  }
  if (pGroupData->blocksInFile) {
    taosArrayDestroyP(pGroupData->blocksInFile, NULL);
    pGroupData->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

static int32_t createAlignTaskMgr(int64_t streamId, int64_t taskId, int32_t tsSlotId, void** ppCache) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SAlignTaskDSMgr* pAlignTaskDSMgr = taosMemCalloc(1, sizeof(SAlignTaskDSMgr));
  if (pAlignTaskDSMgr == NULL) {
    return terrno;
  }

  pAlignTaskDSMgr->cleanMode = DATA_CLEAN_IMMEDIATE;
  pAlignTaskDSMgr->streamId = streamId;
  pAlignTaskDSMgr->taskId = taskId;
  pAlignTaskDSMgr->tsSlotId = tsSlotId;
  pAlignTaskDSMgr->pFileMgr = NULL;
  pAlignTaskDSMgr->pAlignGrpList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pAlignTaskDSMgr->pAlignGrpList == NULL) {
    taosMemoryFree(pAlignTaskDSMgr);
    stError("failed to create align group list, err: %s", terrMsg);
    return terrno;
  }
  taosHashSetFreeFp(pAlignTaskDSMgr->pAlignGrpList, destroyAlignGrpMgr);
  char key[64] = {0};
  *ppCache = pAlignTaskDSMgr;

  return code;
}

int32_t createSlidingGrpMgr(int64_t groupId, SSlidingGrpMgr** ppSlidingGrpMgr) {
  *ppSlidingGrpMgr = (SSlidingGrpMgr*)taosMemoryCalloc(1, sizeof(SSlidingGrpMgr));
  if (*ppSlidingGrpMgr == NULL) {
    return terrno;
  }
  (*ppSlidingGrpMgr)->groupId = groupId;
  (*ppSlidingGrpMgr)->usedMemSize = 0;
  (*ppSlidingGrpMgr)->winDataInMem = taosArrayInit(0, sizeof(SSlidingWindowInMem*));
  if ((*ppSlidingGrpMgr)->winDataInMem == NULL) {
    taosMemoryFree(*ppSlidingGrpMgr);
    stError("failed to create window data in mem, err: %s", terrMsg);
    return terrno;
  }
  (*ppSlidingGrpMgr)->blocksInFile = NULL;  // delay init
  (*ppSlidingGrpMgr)->status = GRP_DATA_WRITING;

  return TSDB_CODE_SUCCESS;
}

static void destroySSlidingGrpMgr(void* pData) {
  SSlidingGrpMgr* pGroupData = *(SSlidingGrpMgr**)pData;
  if (pGroupData->winDataInMem) {
    taosArrayDestroyP(pGroupData->winDataInMem, destorySlidingWindowInMem);
    pGroupData->winDataInMem = NULL;
  }
  if (pGroupData->blocksInFile) {
    taosArrayDestroyP(pGroupData->blocksInFile, NULL);
    pGroupData->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

static int32_t createSlidingTaskMgr(int64_t streamId, int64_t taskId, int32_t tsSlotId, void** ppCache) {
  SSlidingTaskDSMgr* pSlidingTaskDSMgr = taosMemCalloc(1, sizeof(SSlidingTaskDSMgr));
  if (pSlidingTaskDSMgr == NULL) {
    return terrno;
  }
  pSlidingTaskDSMgr->cleanMode = DATA_CLEAN_EXPIRED;
  pSlidingTaskDSMgr->streamId = streamId;
  pSlidingTaskDSMgr->taskId = taskId;
  pSlidingTaskDSMgr->tsSlotId = tsSlotId;
  pSlidingTaskDSMgr->capacity = gDSFileBlockDefaultSize;
  pSlidingTaskDSMgr->pFileMgr = NULL;
  pSlidingTaskDSMgr->pSlidingGrpList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pSlidingTaskDSMgr->pSlidingGrpList == NULL) {
    taosMemoryFree(pSlidingTaskDSMgr);
    stError("failed to create sliding group list, err: %s", terrMsg);
    return terrno;
  }
  taosHashSetFreeFp(pSlidingTaskDSMgr->pSlidingGrpList, destroySSlidingGrpMgr);
  *ppCache = pSlidingTaskDSMgr;
  return TSDB_CODE_SUCCESS;
}

// @brief 初始化数据缓存
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int32_t cleanMode, int32_t tsSlotId, void** ppCache) {
  int32_t code = initStreamDataSinkOnce();
  if (code != 0) {
    return code;
  }
  *ppCache = NULL;
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64, streamId, taskId);

  void** ppStreamTaskDSManager = (void**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (ppStreamTaskDSManager == NULL) {
    if (cleanMode == DATA_CLEAN_IMMEDIATE) {
      return createAlignTaskMgr(streamId, taskId, tsSlotId, ppCache);
    } else {
      return createSlidingTaskMgr(streamId, taskId, tsSlotId, ppCache);
    }

  } else {
    stError("streamId: %" PRId64 " taskId: %" PRId64 " already exist", streamId, taskId);

    *ppCache = *ppStreamTaskDSManager;
  }
  return TSDB_CODE_SUCCESS;
}

// @brief 销毁数据缓存
void destroyStreamDataCache(void* pCache) {
  if (pCache == NULL) {
    return;
  }
  SSlidingTaskDSMgr* pStreamDataSink = (SSlidingTaskDSMgr*)pCache;
  char               key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId);
  SSlidingTaskDSMgr** ppStreamTaskDSManager =
      (SSlidingTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (ppStreamTaskDSManager != NULL) {
    taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  }
}

static int32_t getOrCreateSSlidingGrpMgr(SSlidingTaskDSMgr* pSlidingTaskMgr, int64_t groupId,
                                         SSlidingGrpMgr** ppSlidingGrpMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSlidingGrpMgr*  pGrpMgr = NULL;
  SSlidingGrpMgr** ppExistGrpMgr =
      (SSlidingGrpMgr**)taosHashGet(pSlidingTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    code = createSlidingGrpMgr(groupId, &pGrpMgr);
    if (code != 0) {
      stError("failed to create group data sink manager, err: %s", terrMsg);
      return code;
    }

    code = taosHashPut(pSlidingTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId), &pGrpMgr, sizeof(SSlidingGrpMgr*));
    if (code != 0) {
      destroySSlidingGrpMgr(&pGrpMgr);
      stError("failed to put group data sink manager, err: %s", terrMsg);
      return code;
    }
    *ppSlidingGrpMgr = pGrpMgr;
  } else {
    *ppSlidingGrpMgr = *ppExistGrpMgr;
  }
  return code;
}

static int32_t getOrCreateAlignGrpMgr(SAlignTaskDSMgr* pStreamTaskMgr, int64_t groupId, SAlignGrpMgr** ppAlignGrpMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SAlignGrpMgr*  pAlignGrpMgr = NULL;
  SAlignGrpMgr** ppExistGrpMgr = (SAlignGrpMgr**)taosHashGet(pStreamTaskMgr->pAlignGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    code = createAlignGrpMgr(groupId, &pAlignGrpMgr);
    if (code != 0) {
      stError("failed to create group data sink manager, err: %s", terrMsg);
      return code;
    }

    code = taosHashPut(pStreamTaskMgr->pAlignGrpList, &groupId, sizeof(groupId), &pAlignGrpMgr, sizeof(SAlignGrpMgr*));
    if (code != 0) {
      destroyAlignGrpMgr(&pAlignGrpMgr);
      stError("failed to put group data sink manager, err: %s", terrMsg);
      return code;
    }
    *ppAlignGrpMgr = pAlignGrpMgr;
  } else {
    *ppAlignGrpMgr = *ppExistGrpMgr;
  }
  return code;
}

static int32_t addASlidingWindowInMem(SSlidingGrpMgr* pSlidingGrpMgr, SSlidingWindowInMem* pSlidingWindowInMem) {
  // todo mem size check
  int32_t code = TSDB_CODE_SUCCESS;
  void*   p = taosArrayPush(pSlidingGrpMgr->winDataInMem, &pSlidingWindowInMem);
  if (p == NULL) {
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    return terrno;
  }
  pSlidingGrpMgr->usedMemSize += sizeof(SSlidingWindowInMem);
  return code;
}

int32_t putDataToSlidingTaskMgr(SSlidingTaskDSMgr* pStreamTaskMgr, int64_t groupId, SSDataBlock* pBlock,
                                int32_t startIndex, int32_t endIndex) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SSlidingGrpMgr* pSlidingGrpMgr = NULL;
  code = getOrCreateSSlidingGrpMgr(pStreamTaskMgr, groupId, &pSlidingGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  checkAndReleaseBuffer();

  SSlidingWindowInMem* pSlidingWinInMem = NULL;
  code = buildSlidingWindowInMem(pBlock, pStreamTaskMgr->tsSlotId, startIndex, endIndex, &pSlidingWinInMem);
  if (code != 0) {
    stError("failed to build sliding window in mem, err: %s", terrMsg);
    return code;
  }
  void* p = taosArrayPush(pSlidingGrpMgr->winDataInMem, &pSlidingWinInMem);
  if (p == NULL) {
    destorySlidingWindowInMem(&pSlidingWinInMem);
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    return terrno;
  }

  // todo mem size add
  pSlidingGrpMgr->usedMemSize += sizeof(SSlidingWindowInMem);

  return TSDB_CODE_SUCCESS;
}

int32_t putDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, int64_t groupId, TSKEY wstart, TSKEY wend,
                              SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SAlignGrpMgr* pAlignGrpMgr = NULL;
  code = getOrCreateAlignGrpMgr(pStreamTaskMgr, groupId, &pAlignGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  checkAndReleaseBuffer();
  if (pAlignGrpMgr->blocksInMem == NULL) {
    pAlignGrpMgr->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
    if (pAlignGrpMgr->blocksInMem == NULL) {
      stError("failed to create window data in mem, err: %s", terrMsg);
      return terrno;
    }
  }

  code = buildAlignWindowInMemBlock(pAlignGrpMgr, pBlock, pStreamTaskMgr->tsSlotId, wstart, wend);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  return code;
}

int32_t moveDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, SSDataBlock* pBlock, int64_t groupId, TSKEY wstart,
                               TSKEY wend) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SAlignGrpMgr* pAlignGrpMgr = NULL;
  code = getOrCreateAlignGrpMgr(pStreamTaskMgr, groupId, &pAlignGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  checkAndReleaseBuffer();
  if (pAlignGrpMgr->blocksInMem == NULL) {
    pAlignGrpMgr->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
    if (pAlignGrpMgr->blocksInMem == NULL) {
      stError("failed to create window data in mem, err: %s", terrMsg);
      return terrno;
    }
  }

  code = buildAlignWindowInMemBlock(pAlignGrpMgr, pBlock, pStreamTaskMgr->tsSlotId, wstart, wend);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  return code;
}

int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCache == NULL) {
    stError("putStreamDataCache param invalid, pCache is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (wstart < 0 || wstart >= wend) {
    stError("putStreamDataCache param invalid, wstart:%" PRId64 "wend:%" PRId64, wstart, wend);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (startIndex < 0 || startIndex > endIndex) {
    stError("putStreamDataCache param invalid, startIndex:%d endIndex:%d", startIndex, endIndex);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    return putDataToAlignTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    return putDataToSlidingTaskMgr(pStreamTaskMgr, groupId, pBlock, startIndex, endIndex);
  }
}

int32_t moveStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pCache == NULL) {
    stError("moveStreamDataCache param invalid, pCache is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (getCleanModeFromDSMgr(pCache) != DATA_CLEAN_IMMEDIATE) {
    stError("moveStreamDataCache param invalid, cleanMode is not immediate");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return moveDataToAlignTaskMgr((SAlignTaskDSMgr*)pCache, pBlock, groupId, wstart, wend);
}

int32_t getAlignDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
  SResultIter*     pResultIter = NULL;
  *pIter = NULL;
  SAlignGrpMgr** ppExistGrpMgr = (SAlignGrpMgr**)taosHashGet(pStreamTaskMgr->pAlignGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    return code;
  }
  SAlignGrpMgr* pExistGrpMgr = *ppExistGrpMgr;
  if (pExistGrpMgr->blocksInMem->size == 0 && (!pExistGrpMgr->blocksInFile || pExistGrpMgr->blocksInFile->size == 0)) {
    return TSDB_CODE_SUCCESS;
  }
  code = createDataResult((void**)(&pResultIter));
  QUERY_CHECK_CODE(code, lino, _end);
  *pIter = pResultIter;
  pResultIter->cleanMode = pStreamTaskMgr->cleanMode;
  pResultIter->groupData = pExistGrpMgr;
  pResultIter->pFileMgr = pStreamTaskMgr->pFileMgr;
  pResultIter->tsColSlotId = pStreamTaskMgr->tsSlotId;
  pResultIter->offset = 0;
  pResultIter->groupId = groupId;
  pResultIter->reqStartTime = start;
  pResultIter->reqEndTime = end;
  if (pExistGrpMgr->blocksInFile && pExistGrpMgr->blocksInFile->size > 0) {  // read from file first
    pResultIter->dataPos = DATA_SINK_FILE;
    return code;
  }
  if (pExistGrpMgr->blocksInMem && pExistGrpMgr->blocksInMem->size > 0) {
    pResultIter->dataPos = DATA_SINK_MEM;
    return code;
  }

  return code;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    releaseDataResult((void**)&pResultIter);
    *pIter = NULL;
    stError("failed to get align data cache, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

int32_t getSlidingDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
  SResultIter*       pResultIter = NULL;
  *pIter = NULL;

  SSlidingGrpMgr** ppExistGrpMgr =
      (SSlidingGrpMgr**)taosHashGet(pStreamTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    return code;
  }
  SSlidingGrpMgr* pExistGrpMgr = *ppExistGrpMgr;

  if (pExistGrpMgr->winDataInMem->size == 0 && (!pExistGrpMgr->blocksInFile || pExistGrpMgr->blocksInFile->size == 0)) {
    return TSDB_CODE_SUCCESS;
  }

  code = createDataResult((void**)(&pResultIter));
  QUERY_CHECK_CODE(code, lino, _end);
  *pIter = pResultIter;

  pResultIter->cleanMode = pStreamTaskMgr->cleanMode;
  pResultIter->groupData = pExistGrpMgr;
  pResultIter->pFileMgr = pStreamTaskMgr->pFileMgr;
  pResultIter->tsColSlotId = pStreamTaskMgr->tsSlotId;
  pResultIter->offset = 0;
  pResultIter->groupId = groupId;
  pResultIter->reqStartTime = start;
  pResultIter->reqEndTime = end;

  if (pExistGrpMgr->blocksInFile && pExistGrpMgr->blocksInFile->size > 0) {  // read from file first
    pResultIter->dataPos = DATA_SINK_FILE;
    return code;
  }
  if (pExistGrpMgr->winDataInMem && pExistGrpMgr->winDataInMem->size > 0) {
    pResultIter->dataPos = DATA_SINK_MEM;
    return code;
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    releaseDataResult((void**)&pResultIter);
    *pIter = NULL;
    stError("failed to get sliding data cache, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

int32_t getStreamDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter) {
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pCache == NULL || pIter == NULL) {
    stError("getStreamDataCache param invalid, pCache or pIter is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (start < 0 || start >= end) {
    stError("getStreamDataCache param invalid, start > end");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    return getAlignDataCache(pStreamTaskMgr, groupId, start, end, pIter);
  } else if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_EXPIRED) {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    return getSlidingDataCache(pStreamTaskMgr, groupId, start, end, pIter);
  } else {
    stError("invalid clean mode: %d", getCleanModeFromDSMgr(pCache));
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
}

int32_t createDataResult(void** pIter) {
  if (pIter == NULL) {
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SResultIter* pResult = (SResultIter*)taosMemoryCalloc(1, sizeof(SResultIter));
  if (pResult == NULL) {
    stError("failed to create data result iterator, err: %s", terrMsg);
    return terrno;
  }
  *pIter = pResult;
  return TSDB_CODE_SUCCESS;
}

void releaseDataResult(void** pIter) {
  if (pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult != NULL) {
    taosMemoryFree(pResult);
    *pIter = NULL;
  }
}

void moveToNextIterator(void** ppIter) {
  if (ppIter == NULL || *ppIter == NULL) {
    return;
  }
  SResultIter* pResult = *(SResultIter**)ppIter;

  bool finished = false;
  if (pResult->dataPos == DATA_SINK_FILE) {
    finished = setNextIteratorFromFile((SResultIter**)ppIter);
    if (finished) {
      pResult->dataPos = DATA_SINK_MEM;
      pResult->offset = -1;
      finished = setNextIteratorFromMem((SResultIter**)ppIter);
    }
  } else {
    finished = setNextIteratorFromMem((SResultIter**)ppIter);
  }
  if (finished) {
    releaseDataResult(ppIter);
    *ppIter = NULL;
  }

  return;
}

int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock) {
  if (ppBlock == NULL) {
    stError("getNextStreamDataCache param invalid, ppBlock is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t      code = 0;
  int32_t      lino = 0;
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  bool finished = false;
  if (pResult->dataPos == DATA_SINK_MEM) {
    code = readDataFromMem(pResult, ppBlock, &finished);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = readDataFromFile(pResult, ppBlock, pResult->tsColSlotId, &finished);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if (finished) {
    releaseDataResult(pIter);
    *pIter = NULL;
    return TSDB_CODE_SUCCESS;
  }
  moveToNextIterator(pIter);

  if (code == TSDB_CODE_SUCCESS && *ppBlock == NULL && *pIter != NULL) {
    return getNextStreamDataCache(pIter, ppBlock);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to get next data from cache, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

void cancelStreamDataCacheIterate(void** pIter) { releaseDataResult(pIter); }

int32_t destroyDataSinkMgr() {
  int8_t flag = atomic_val_compare_exchange_8(&g_pDataSinkManager.status, 1, 0);
  if (flag == 1) {
    if (g_pDataSinkManager.dsStreamTaskList) {
      taosHashCleanup(g_pDataSinkManager.dsStreamTaskList);
      g_pDataSinkManager.dsStreamTaskList = NULL;
    }
  }
  return TSDB_CODE_SUCCESS;
}

void useMemSizeAdd(int64_t size) { atomic_fetch_add_64(&g_pDataSinkManager.usedMemSize, size); }

void useMemSizeSub(int64_t size) { atomic_fetch_sub_64(&g_pDataSinkManager.usedMemSize, size); }

void checkAndReleaseBuffer() {}
