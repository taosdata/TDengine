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
int64_t gDSMaxMemSizeDefault = (1024 * 1024 * 1024);  // 1G
int64_t gDSFileBlockDefaultSize = (10 * 1024 * 1024);        // 10M

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
  if (g_pDataSinkManager.usedMemSize < g_pDataSinkManager.maxMemSize * 0.9 && pStreamDataSink->usedMemSize > 0) {
    return false;
  }

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

static int32_t createStreamTaskDSManager(int64_t streamId, int64_t taskId, int32_t cleanMode,
                                            SSlidingTaskDSMgr** ppStreamDataSink) {
  SSlidingTaskDSMgr* pStreamDataSink = taosMemoryCalloc(1, sizeof(SSlidingTaskDSMgr));
  if (pStreamDataSink == NULL) {
    return terrno;
  }
  pStreamDataSink->streamId = streamId;
  pStreamDataSink->taskId = taskId;
  pStreamDataSink->cleanMode = cleanMode;
  pStreamDataSink->pFileMgr = NULL;

  *ppStreamDataSink = pStreamDataSink;
  return TSDB_CODE_SUCCESS;
}

static void doDestoryStreamTaskDSManager(SSlidingTaskDSMgr* pStreamTaskDSManager) {
  if (pStreamTaskDSManager->DataSinkGroupList) {
    taosHashCleanup(pStreamTaskDSManager->DataSinkGroupList);
    pStreamTaskDSManager->DataSinkGroupList = NULL;
  }
  if (pStreamTaskDSManager->pFileMgr) {
    destroyStreamDataSinkFile(&pStreamTaskDSManager->pFileMgr);
  }
  if (pStreamTaskDSManager->usedMemSize > 0) {
    atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pStreamTaskDSManager->usedMemSize);
  }
  taosMemoryFreeClear(pStreamTaskDSManager);
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
  if (pAlignTaskDSMgr->blocksInMem) {
    taosArrayDestroy(pAlignTaskDSMgr->blocksInMem);
    pAlignTaskDSMgr->blocksInMem = NULL;
  }
  if (pAlignTaskDSMgr->blocksInFile) {
    taosArrayDestroy(pAlignTaskDSMgr->blocksInFile);
    pAlignTaskDSMgr->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pAlignTaskDSMgr);
}

static void destroySStreamDSTaskMgr(void* pData) {
  if(pData == NULL || *(void**)pData == NULL) {
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

static void destroySSlidingGrpMgr(void* pData) {
  SSlidingGrpMgr* pGroupData = *(SSlidingGrpMgr**)pData;
  if (pGroupData->winDataInMem) {
    taosArrayDestroyP(pGroupData->winDataInMem, destorySWindowDat23aPP);
    pGroupData->winDataInMem = NULL;
  }
  if (pGroupData->blocksInFile) {
    taosArrayDestroyP(pGroupData->blocksInFile, destorySWindowData2PP);
    pGroupData->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

int32_t getOrCreateSGroupDSManager(SSlidingTaskDSMgr* pStreamDataSink, int64_t groupId,
                                   SGroupDSManager** ppGroupDataInfoMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SGroupDSManager*  pGroupDataInfo = NULL;
  SGroupDSManager** ppGroupDIM =
      (SGroupDSManager**)taosHashGet(pStreamDataSink->DataSinkGroupList, &groupId, sizeof(groupId));
  if (ppGroupDIM == NULL) {
    code = createSGroupDSManager(groupId, &pGroupDataInfo);
    if (code != 0) {
      stError("failed to create group data sink manager, err: %s", terrMsg);
      return code;
    }
    pGroupDataInfo->pSinkManager = pStreamDataSink;
    code = taosHashPut(pStreamDataSink->DataSinkGroupList, &groupId, sizeof(groupId), &pGroupDataInfo,
                       sizeof(SGroupDSManager*));
    if (code != 0) {
      destroySGroupDSManager(&pGroupDataInfo);
      stError("failed to put group data sink manager, err: %s", terrMsg);
      return code;
    }
    *ppGroupDataInfoMgr = pGroupDataInfo;
  } else {
    *ppGroupDataInfoMgr = *ppGroupDIM;
  }
  return code;
}

static int32_t createAlignTaskMgr(int64_t streamId, int64_t taskId, void** ppCache) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SAlignTaskDSMgr* pStreamTaskDSManager = taosMemCalloc(1, sizeof(SAlignTaskDSMgr));
  if (pStreamTaskDSManager == NULL) {
    return terrno;
  }
  pStreamTaskDSManager->cleanMode = DATA_CLEAN_EXPIRED;
  pStreamTaskDSManager->currentGroupId = -1;
  pStreamTaskDSManager->usedMemSize = 0;
  pStreamTaskDSManager->pFileMgr = NULL;
  pStreamTaskDSManager->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
  if (pStreamTaskDSManager->blocksInMem == NULL) {
    stError("failed to create blocks in mem, err: %s", terrMsg);
    code = terrno;
    goto _exit;
  }
  pStreamTaskDSManager->blocksInFile = taosArrayInit(0, sizeof(SBlocksInfoFile*));
  if (pStreamTaskDSManager->blocksInFile == NULL) {
    stError("failed to create blocks in file, err: %s", terrMsg);
    code = terrno;
    goto _exit;
  }
  *ppCache = pStreamTaskDSManager;
  return TSDB_CODE_SUCCESS;
_exit:
  if (pStreamTaskDSManager->blocksInFile) {
    taosArrayDestroy(pStreamTaskDSManager->blocksInFile);
    pStreamTaskDSManager->blocksInFile = NULL;
  }
  if (pStreamTaskDSManager->blocksInMem) {
    taosArrayDestroy(pStreamTaskDSManager->blocksInMem);
    pStreamTaskDSManager->blocksInMem = NULL;
  }
  if (pStreamTaskDSManager) {
    taosMemoryFree(pStreamTaskDSManager);
  }
  return code;
}

static int32_t createSlidingTaskMgr(int64_t streamId, int64_t taskId, void** ppCache) {
  SSlidingTaskDSMgr* pStreamTaskDSManager =  taosMemCalloc(1, sizeof(SSlidingTaskDSMgr));
  if (pStreamTaskDSManager == NULL) {
    return terrno;
  }
  pStreamTaskDSManager->cleanMode = DATA_CLEAN_IMMEDIATE;
  pStreamTaskDSManager->streamId = streamId;
  pStreamTaskDSManager->taskId = taskId;
  pStreamTaskDSManager->capacity = gDSFileBlockDefaultSize;
  pStreamTaskDSManager->pFileMgr = NULL;
  pStreamTaskDSManager->pSlidingGrpList = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pStreamTaskDSManager->pSlidingGrpList == NULL) {
    stError("failed to create sliding group list, err: %s", terrMsg);
    return terrno;
  }
  taosHashSetFreeFp(pStreamTaskDSManager->pSlidingGrpList, destroySSlidingGrpMgr);
  *ppCache = pStreamTaskDSManager;
}

// @brief 初始化数据缓存
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int32_t cleanMode, void** ppCache) {
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
      return createAlignTaskMgr(streamId, taskId, ppCache);
    } else {
      return createSlidingTaskMgr(streamId, taskId, ppCache);
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
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId);
  SSlidingTaskDSMgr** ppStreamTaskDSManager =
      (SSlidingTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (ppStreamTaskDSManager != NULL) {
    taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  }
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
    stError("failed to create window data in mem, err: %s", terrMsg);
    return terrno;
  }
  (*ppSlidingGrpMgr)->blocksInFile = NULL;
  (*ppSlidingGrpMgr)->status = GRP_DATA_WRITING;
  return TSDB_CODE_SUCCESS;
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

    code = taosHashPut(pSlidingTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId), &pGrpMgr,
                       sizeof(SSlidingGrpMgr*));
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

int32_t putDataToSlidingTaskMgr(SSlidingTaskDSMgr* pStreamTaskMgr, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSlidingGrpMgr*      pSlidingGrpMgr = NULL;
  code = getOrCreateSSlidingGrpMgr(pStreamTaskMgr, groupId, &pSlidingGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  checkAndReleaseBuffer();

  SSlidingWindowInMem* pSlidingWinInMem = NULL;
  code = buildSlidingWindowInMem(pBlock, startIndex, endIndex, &pSlidingWinInMem);
  if (code != 0) {
    stError("failed to build sliding window in mem, err: %s", terrMsg);
    return code;
  }
  return addASlidingWindowInMem(pSlidingGrpMgr, pSlidingWinInMem);
}

int32_t putDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                          int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pStreamTaskMgr->currentGroupId == -1) {
    pStreamTaskMgr->currentGroupId = groupId;
  } else if (pStreamTaskMgr->currentGroupId != groupId) {
    stError("groupId not match, currentGroupId: %" PRId64 " groupId: %" PRId64, pStreamTaskMgr->currentGroupId,
            groupId);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  
  SGroupDSManager*      pGroupDataInfoMgr = NULL;
  code = getOrCreateSGroupDSManager(pStreamTaskMgr, groupId, &pGroupDataInfoMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  return writeToCache(pStreamTaskMgr, pGroupDataInfoMgr, wstart, wend, pBlock, startIndex, endIndex);
}

int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (wstart < 0 || wstart >= wend) {
    stError("putStreamDataCache param invalid, wstart:%" PRId64 "wend:%" PRId64, wstart, wend);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (startIndex < 0 || startIndex >= endIndex) {
    stError("putStreamDataCache param invalid, startIndex:%d endIndex:%d", startIndex, endIndex);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    return putDataToSlidingTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    return putDataToAlignTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
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
  if (((SSlidingTaskDSMgr*)pCache)->cleanMode != DATA_CLEAN_IMMEDIATE) {
    stError("moveStreamDataCache param invalid, cleanMode is not immediate");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SSlidingTaskDSMgr* pStreamDataSink = (SSlidingTaskDSMgr*)pCache;
  SGroupDSManager*      pGroupDataInfoMgr = NULL;
  code = getOrCreateSGroupDSManager(pStreamDataSink, groupId, &pGroupDataInfoMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: %s", terrMsg);
    return code;
  }
  if (shouldWriteIntoFile(pStreamDataSink, groupId, true)) {
    return writeToFile(pStreamDataSink, groupId, wstart, wend, pBlock, 0, 1);
  } else {
    return moveToCache(pStreamDataSink, pGroupDataInfoMgr, wstart, wend, pBlock);
  }
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

  *pIter = NULL;
  int32_t code = getFirstDataIterFromCache((SSlidingTaskDSMgr*)pCache, groupId, start, end, pIter);
  if( code != 0) {
    stError("failed to get first data iterator, err: %s", terrMsg);
    return code;
  }

  if (*pIter == NULL) {
    code = getFirstDataIterFromFile(((SSlidingTaskDSMgr*)pCache)->pFileMgr, groupId, start, end, pIter);
    if (code != 0) {
      stError("failed to get first data iterator from file, err: %s", terrMsg);
      return code;
    }
  } else {
    ((SResultIter*)*pIter)->pFileMgr = ((SSlidingTaskDSMgr*)pCache)->pFileMgr;
  }
  return code;
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

void moveToNextIterator(void** pIter) {
  if (pIter == NULL || *pIter == NULL) {
    return;
  }
  SResultIter* pResult = *(SResultIter**)pIter;

  if (pResult->dataPos == DATA_SINK_MEM) {
    bool needSearchFile = setNextIteratorFromCache((SResultIter**)pIter);
    if (needSearchFile) {
      pResult->dataPos = DATA_SINK_FILE;
      pResult->offset = -1;
    }
  }
  if (pResult->dataPos == DATA_SINK_FILE) {
    setNextIteratorFromFile((SResultIter**)pIter);
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
    code = readDataFromCache(pResult, ppBlock, &finished);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = readDataFromFile(pResult, ppBlock, &finished);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  if(finished) {
    releaseDataIterator(pIter);
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

void cancelStreamDataCacheIterate(void** pIter) { releaseDataIterator(pIter); }

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

void checkAndReleaseBuffer(){

}
