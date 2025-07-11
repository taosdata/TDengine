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
#include "osMemory.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "thash.h"

SDataSinkManager2 g_pDataSinkManager = {0};

void setDataSinkMaxMemSize(int64_t maxMemSize) {
  if (maxMemSize >= 0) {
    tsStreamBufferSizeBytes = maxMemSize;
  }
  stInfo("set data sink max mem size to %" PRId64, tsStreamBufferSizeBytes);
}

static void destroySStreamDSTaskMgr(void* pData);
int32_t     initStreamDataSink() {
  int32_t code = 0;
  code = initDataSinkFileDir();
  if (code != 0) {
    stError("failed to create data sink file dir, err: 0x%0x", code);
    return code;
  }

  g_pDataSinkManager.memAlterSize = TMIN(100 * 1024 * 1024, tsStreamBufferSizeBytes * 0.1);
  g_pDataSinkManager.usedMemSize = 0;
  g_pDataSinkManager.fileBlockSize = 0;
  g_pDataSinkManager.readDataFromFileTimes = 0;
  g_pDataSinkManager.dsStreamTaskList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (g_pDataSinkManager.dsStreamTaskList == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(g_pDataSinkManager.dsStreamTaskList, destroySStreamDSTaskMgr);
  stInfo("data sink manager init success, max mem size: %" PRId64, tsStreamBufferSizeBytes);
  return TSDB_CODE_SUCCESS;
};

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
  if (pAlignTaskDSMgr->pFileMgr) {
    destroyStreamDataSinkFile(&pAlignTaskDSMgr->pFileMgr);
    pAlignTaskDSMgr->pFileMgr = NULL;
  }
  if(pAlignTaskDSMgr->pAlignGrpList) {
    taosHashCleanup(pAlignTaskDSMgr->pAlignGrpList);
    pAlignTaskDSMgr->pAlignGrpList = NULL;
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
    destroyAlignTaskDSMgr((SAlignTaskDSMgr**)pData);
    return;
  } else if (cleanMode == DATA_CLEAN_EXPIRED) {
    destroySlidingTaskDSMgr((SSlidingTaskDSMgr**)pData);
    return;
  } else {
    stError("invalid clean mode: %d", cleanMode);
  }
}

int32_t createAlignGrpMgr(int64_t groupId, SAlignGrpMgr** ppAlignGrpMgr) {
  *ppAlignGrpMgr = (SAlignGrpMgr*)taosMemoryCalloc(1, sizeof(SAlignGrpMgr));
  if (*ppAlignGrpMgr == NULL) {
    return terrno;
  }
  (*ppAlignGrpMgr)->groupId = groupId;
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
    taosArrayDestroyP(pGroupData->blocksInMem, destroyAlignBlockInMem);
    pGroupData->blocksInMem = NULL;
  }
  if (pGroupData->blocksInFile) {
    taosArrayDestroyP(pGroupData->blocksInFile, NULL);
    pGroupData->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

static int32_t createAlignTaskMgr(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t tsSlotId, void** ppCache) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SAlignTaskDSMgr* pAlignTaskDSMgr = taosMemCalloc(1, sizeof(SAlignTaskDSMgr));
  if (pAlignTaskDSMgr == NULL) {
    return terrno;
  }

  pAlignTaskDSMgr->cleanMode = DATA_CLEAN_IMMEDIATE;
  pAlignTaskDSMgr->streamId = streamId;
  pAlignTaskDSMgr->taskId = taskId;
  pAlignTaskDSMgr->sessionId = sessionId;
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
    taosArrayDestroyP(pGroupData->winDataInMem, destroySlidingWindowInMem);
    pGroupData->winDataInMem = NULL;
  }
  if (pGroupData->blocksInFile) {
    // todo destroy blocks in file
    taosArrayDestroy(pGroupData->blocksInFile);
    pGroupData->blocksInFile = NULL;
  }
  taosMemoryFreeClear(pGroupData);
}

static int32_t createSlidingTaskMgr(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t tsSlotId, void** ppCache) {
  SSlidingTaskDSMgr* pSlidingTaskDSMgr = taosMemCalloc(1, sizeof(SSlidingTaskDSMgr));
  if (pSlidingTaskDSMgr == NULL) {
    return terrno;
  }
  pSlidingTaskDSMgr->cleanMode = DATA_CLEAN_EXPIRED;
  pSlidingTaskDSMgr->streamId = streamId;
  pSlidingTaskDSMgr->taskId = taskId;
  pSlidingTaskDSMgr->sessionId = sessionId;
  pSlidingTaskDSMgr->tsSlotId = tsSlotId;
  pSlidingTaskDSMgr->capacity = DS_FILE_BLOCK_SIZE;
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
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t cleanMode, int32_t tsSlotId,
                            void** ppCache) {
  int32_t code = 0;                            
  *ppCache = NULL;
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%"PRId64, streamId, taskId, sessionId);

  void** ppStreamTaskDSManager = (void**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (ppStreamTaskDSManager == NULL) {
    if (cleanMode == DATA_CLEAN_IMMEDIATE) {
      code = createAlignTaskMgr(streamId, taskId, sessionId, tsSlotId, ppCache);
    } else {
      code = createSlidingTaskMgr(streamId, taskId, sessionId, tsSlotId, ppCache);
    }
    if (code != 0) {
      stError("failed to create stream task data sink manager, cleanMode:%d, err: 0x%0x", cleanMode, code);
      return code;
    } else {
      code = taosHashPut(g_pDataSinkManager.dsStreamTaskList, key, strlen(key), ppCache, sizeof(void*));
      if (code != 0) {
        destroyStreamDataCache(*ppCache);
        stError("failed to put stream task data sink manager, err: 0x%0x", code);
        return code;
      }
      stDebug("streamId: %" PRId64 " taskId: %" PRId64 " data sink manager created", streamId, taskId);
    }
  } else {
    stError("streamId: %" PRId64 " taskId: %" PRId64 " already exist", streamId, taskId);
    *ppCache = *ppStreamTaskDSManager;
  }
  return code;
}

// @brief 销毁数据缓存
void destroyStreamDataCache(void* pCache) {
  if (pCache == NULL) {
    return;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamDataSink = (SAlignTaskDSMgr*)pCache;
    char             key[64] = {0};
    snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId,
             pStreamDataSink->sessionId);
    SAlignTaskDSMgr** ppStreamTaskDSManager =
        (SAlignTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    if (ppStreamTaskDSManager != NULL) {
      taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    }
  } else if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_EXPIRED) {
    SSlidingTaskDSMgr* pStreamDataSink = (SSlidingTaskDSMgr*)pCache;
    char               key[64] = {0};
    snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId,
             pStreamDataSink->sessionId);
    SSlidingTaskDSMgr** ppStreamTaskDSManager =
        (SSlidingTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    if (ppStreamTaskDSManager != NULL) {
      taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    }
  } else {
    stError("invalid clean mode: %d", getCleanModeFromDSMgr(pCache));
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
      stError("failed to create group data sink manager, err: 0x%0x", code);
      return code;
    }

    code = taosHashPut(pSlidingTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId), &pGrpMgr, sizeof(SSlidingGrpMgr*));
    if (code != 0) {
      destroySSlidingGrpMgr(&pGrpMgr);
      stError("failed to put group data sink manager, err: 0x%0x", code);
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
      stError("failed to create group data sink manager, err: 0x%0x", code);
      return code;
    }

    code = taosHashPut(pStreamTaskMgr->pAlignGrpList, &groupId, sizeof(groupId), &pAlignGrpMgr, sizeof(SAlignGrpMgr*));
    if (code != 0) {
      destroyAlignGrpMgr(&pAlignGrpMgr);
      stError("failed to put group data sink manager, err: 0x%0x", code);
      return code;
    }
    *ppAlignGrpMgr = pAlignGrpMgr;
  } else {
    *ppAlignGrpMgr = *ppExistGrpMgr;
  }
  return code;
}

int32_t putDataToSlidingTaskMgr(SSlidingTaskDSMgr* pStreamTaskMgr, int64_t groupId, SSDataBlock* pBlock,
                                int32_t startIndex, int32_t endIndex) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSlidingGrpMgr* pSlidingGrpMgr = NULL;
  stDebug("[put data cache] slding, groupId: %" PRId64 " block rows: %" PRId64 " startIndex: %d endIndex: %d",
          groupId, pBlock->info.rows, startIndex, endIndex);
  code = getOrCreateSSlidingGrpMgr(pStreamTaskMgr, groupId, &pSlidingGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: 0x%0x", code);
    return code;
  }
  bool canPut = changeMgrStatus(&pSlidingGrpMgr->status, GRP_DATA_WRITING);
  if (!canPut) {
    stError("failed to change group data sink manager status when put data, status: %d", pSlidingGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SSlidingWindowInMem* pSlidingWinInMem = NULL;
  code = buildSlidingWindowInMem(pBlock, pStreamTaskMgr->tsSlotId, startIndex, endIndex, &pSlidingWinInMem);
  QUERY_CHECK_CODE(code, lino, _end);

  void* p = taosArrayPush(pSlidingGrpMgr->winDataInMem, &pSlidingWinInMem);
  if (p == NULL) {
    destroySlidingWindowInMem(pSlidingWinInMem);
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  slidingGrpMgrUsedMemAdd(pSlidingGrpMgr, sizeof(SSlidingWindowInMem) + pSlidingWinInMem->dataLen);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to put data to align task manager, lino:%d err: %0x", lino, code);
    if (pSlidingGrpMgr) {
      (void)changeMgrStatus(&pSlidingGrpMgr->status, GRP_DATA_IDLE);
    }
  } else {
    (void)changeMgrStatus(&pSlidingGrpMgr->status, GRP_DATA_WIAT_READ);
  }

  return code;
}

int32_t putDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, int64_t groupId, TSKEY wstart, TSKEY wend,
                              SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SAlignGrpMgr* pAlignGrpMgr = NULL;
  stDebug("[put data cache] align, groupId: %" PRId64 " wstart: %" PRId64 " wend: %" PRId64
          " block rows: %" PRId64 " startIndex: %d endIndex: %d", groupId, wstart, wend, pBlock->info.rows, startIndex,
          endIndex);
  code = getOrCreateAlignGrpMgr(pStreamTaskMgr, groupId, &pAlignGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: 0x%0x", code);
    return code;
  }
  bool canPut = changeMgrStatus(&pAlignGrpMgr->status, GRP_DATA_WRITING);
  if (!canPut) {
    stError("failed to change group data sink manager status when put data, status: %d", pAlignGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pAlignGrpMgr->blocksInMem == NULL) {
    pAlignGrpMgr->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
    if (pAlignGrpMgr->blocksInMem == NULL) {
      stError("failed to create window data in mem, err: %s", terrMsg);
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  code = buildAlignWindowInMemBlock(pAlignGrpMgr, pBlock, pStreamTaskMgr->tsSlotId, wstart, wend);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to put data to align task manager, lino:%d err: %0x", lino, code);
    if (pAlignGrpMgr) {
      (void)changeMgrStatus(&pAlignGrpMgr->status, GRP_DATA_IDLE);
    }
  } else {
    (void)changeMgrStatus(&pAlignGrpMgr->status, GRP_DATA_WIAT_READ);
  }

  return code;
}

int32_t moveDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, SSDataBlock* pBlock, int64_t groupId, TSKEY wstart,
                               TSKEY wend) {
  int32_t       code = TSDB_CODE_SUCCESS;
  SAlignGrpMgr* pAlignGrpMgr = NULL;
  code = getOrCreateAlignGrpMgr(pStreamTaskMgr, groupId, &pAlignGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: 0x%0x", code);
    return code;
  }
  if (pAlignGrpMgr->blocksInMem == NULL) {
    pAlignGrpMgr->blocksInMem = taosArrayInit(0, sizeof(SAlignBlocksInMem*));
    if (pAlignGrpMgr->blocksInMem == NULL) {
      stError("failed to create window data in mem, err: %s", terrMsg);
      return terrno;
    }
  }

  code = buildMoveAlignWindowInMem(pAlignGrpMgr, pBlock, pStreamTaskMgr->tsSlotId, wstart, wend);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: 0x%0x", code);
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
  if (wstart < 0 || wstart > wend) {
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
  code = checkAndMoveMemCache(true);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to check and move mem cache for write, code: %d err: %s", code, terrMsg);
    return code;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    code = putDataToAlignTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    code = putDataToSlidingTaskMgr(pStreamTaskMgr, groupId, pBlock, startIndex, endIndex);
  }
  (void)checkAndMoveMemCache(false);

  return code;
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

  stDebug("[get data cache] init start groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64,
          groupId, start, end, pStreamTaskMgr->streamId);

  SAlignGrpMgr** ppExistGrpMgr = (SAlignGrpMgr**)taosHashGet(pStreamTaskMgr->pAlignGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    stDebug("[get data cache] init nogroup groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }
  SAlignGrpMgr* pExistGrpMgr = *ppExistGrpMgr;
  if (pExistGrpMgr->blocksInMem->size == 0 && (!pExistGrpMgr->blocksInFile || pExistGrpMgr->blocksInFile->size == 0)) {
    stDebug("[get data cache] init nodata groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
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

  stDebug("[get data cache] init groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64 ,
          groupId, start, end, pStreamTaskMgr->streamId);

  SSlidingGrpMgr** ppExistGrpMgr =
      (SSlidingGrpMgr**)taosHashGet(pStreamTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    stDebug("[get data cache] init nogroup groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 "STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }
  SSlidingGrpMgr* pExistGrpMgr = *ppExistGrpMgr;
  bool            canRead = changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
  if (!canRead) {
    stError("failed to change group data sink manager status when get data, status: %d", pExistGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  if (pExistGrpMgr->winDataInMem->size == 0 && (!pExistGrpMgr->blocksInFile || pExistGrpMgr->blocksInFile->size == 0)) {
    (void)changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_IDLE);
    stDebug("[get data cache] init nodata groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 "STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
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
    changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
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
  if (start < 0 || start > end) {
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
  if (pIter == NULL || *pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult->tmpBlocksInMem) {
    for (int32_t i = 0; i < pResult->tmpBlocksInMem->size; ++i) {
      SSDataBlock** ppBlk = (SSDataBlock**)taosArrayGet(pResult->tmpBlocksInMem, i);
      if (*ppBlk != NULL) {
        taosMemoryFree(*ppBlk);
        *ppBlk = NULL;
      }
    }
    taosArrayDestroy(pResult->tmpBlocksInMem);
    pResult->tmpBlocksInMem = NULL;
  }
  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    changeMgrStatus(&pSlidingGrpMgr->status, GRP_DATA_IDLE);
  } else {
    SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
    changeMgrStatus(&pAlignGrpMgr->status, GRP_DATA_IDLE);
  }

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
  } else if (pResult->dataPos == DATA_SINK_MEM) {
    finished = setNextIteratorFromMem((SResultIter**)ppIter);
  } else if (pResult->dataPos == DATA_SINK_ALL_TMP) {
    pResult->winIndex++;
    if (pResult->tmpBlocksInMem == NULL || pResult->winIndex >= pResult->tmpBlocksInMem->size) {
      finished = true;
      taosArrayClear(pResult->tmpBlocksInMem);
    }
  } else {
    // DATA_SINK_PART_TMP
    pResult->winIndex++;
    if (pResult->winIndex >= pResult->tmpBlocksInMem->size) {
      pResult->dataPos = DATA_SINK_FILE;  // switch to file
      taosArrayClear(pResult->tmpBlocksInMem);
      return moveToNextIterator(ppIter);
    }
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
  *ppBlock = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;
  SResultIter* pResult = (SResultIter*)*pIter;
  if (pResult == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  stDebug("[get data cache] start groupID:%" PRId64 ", start:%" PRId64 " end:%" PRId64 " dataPos: %d, winIndex: %d",
          pResult->groupId, pResult->reqStartTime, pResult->reqEndTime, pResult->dataPos, pResult->winIndex);
  code = checkAndMoveMemCache(true);
  QUERY_CHECK_CODE(code, lino, _end);

  bool finished = false;

  if (pResult->dataPos == DATA_SINK_MEM) {
    code = readDataFromMem(pResult, ppBlock, &finished);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pResult->dataPos == DATA_SINK_FILE) {
    finished = false;
    code = readDataFromFile(pResult, ppBlock, pResult->tsColSlotId);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    if (pResult->tmpBlocksInMem != NULL) {
      if (pResult->winIndex < pResult->tmpBlocksInMem->size) {
        SSDataBlock** ppBlk = (SSDataBlock**)taosArrayGet(pResult->tmpBlocksInMem, pResult->winIndex);
        if (*ppBlk != NULL) {
          *ppBlock = *ppBlk;
          *ppBlk = NULL;  // clear the block to avoid double free

          goto _end;
        } else {
          code = TSDB_CODE_STREAM_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

  if (finished) {
    releaseDataResult(pIter);
    *pIter = NULL;
    goto _end;
  }
  moveToNextIterator(pIter);

  if (code == TSDB_CODE_SUCCESS && *ppBlock == NULL && *pIter != NULL) {
    code = getNextStreamDataCache(pIter, ppBlock);
    goto _end;
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("[get data cache] end, failed to get next data from cache, err: %s, lineno:%d", terrMsg, lino);
  } else if (ppBlock != NULL && *ppBlock != NULL) {
    stDebug("[get data cache] end, block rows: %" PRId64 " next:%p", (*ppBlock)->info.rows, *pIter);
  } else {
    stDebug("[get data cache] end, not found data, next:%p", *pIter);
  }
  return code;
}

void cancelStreamDataCacheIterate(void** pIter) { releaseDataResult(pIter); }

int32_t destroyDataSinkMgr() {
  if (g_pDataSinkManager.dsStreamTaskList) {
    taosHashCleanup(g_pDataSinkManager.dsStreamTaskList);
    g_pDataSinkManager.dsStreamTaskList = NULL;
  }
  return TSDB_CODE_SUCCESS;
}

void useMemSizeAdd(int64_t size) { atomic_fetch_add_64(&g_pDataSinkManager.usedMemSize, size); }

void useMemSizeSub(int64_t size) { atomic_fetch_sub_64(&g_pDataSinkManager.usedMemSize, size); }

bool hasEnoughMemSize() {
  int64_t usedMemSize = atomic_load_64(&g_pDataSinkManager.usedMemSize);
  return (usedMemSize < tsStreamBufferSizeBytes - DS_MEM_SIZE_RESERVED);
}

int32_t moveMemCacheAllList() {
  if (g_pDataSinkManager.dsStreamTaskList == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  stInfo("moveMemCache started, from all list");

  STaskDSMgr** ppTaskMgr = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, NULL);
  while (ppTaskMgr != NULL) {
    STaskDSMgr* pTaskMgr = *ppTaskMgr;
    if (pTaskMgr == NULL) continue;
    if (pTaskMgr->cleanMode == DATA_CLEAN_EXPIRED) {
      SSlidingTaskDSMgr* pSlidingTaskMgr = (SSlidingTaskDSMgr*)pTaskMgr;
      int32_t            code = moveSlidingTaskMemCache(pSlidingTaskMgr);
      if (code != TSDB_CODE_SUCCESS) {
        taosHashCancelIterate(g_pDataSinkManager.dsStreamTaskList, ppTaskMgr);
        stError("failed to move sliding task mem cache, lino:%d code: %d err: %s", __LINE__, code, terrMsg);
        return code;
      }
      if (hasEnoughMemSize()) {
        break;
      }
    }

    ppTaskMgr = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, ppTaskMgr);
  }
  if (ppTaskMgr != NULL) {
    taosHashCancelIterate(g_pDataSinkManager.dsStreamTaskList, ppTaskMgr);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t moveMemCache() {
  if (g_pDataSinkManager.dsStreamTaskList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int8_t status = atomic_val_compare_exchange_8(&g_slidigGrpMemList.status, DATA_NORMAL, DATA_MOVING);
  if (status != DATA_NORMAL) {
    return TSDB_CODE_SUCCESS;
  }

  stInfo("moveMemCache started, used mem size: %" PRId64 ", max mem size: %" PRId64, g_pDataSinkManager.usedMemSize,
         tsStreamBufferSizeBytes);

  int32_t code = moveMemFromWaitList(0);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to move mem from wait list, err: 0x%0x", code);
  }

  if (!hasEnoughMemSize()) {
    code = moveMemFromWaitList(GRP_DATA_WAITREAD_MOVING);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to move mem from wait list, err: 0x%0x", code);
    }
  }
  if (!hasEnoughMemSize()) {
    code = moveMemCacheAllList();
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to move mem cache all list, err: 0x%0x", code);
    }
  }
  stInfo("moveMemCache finished, used mem size: %" PRId64 ", max mem size: %" PRId64, g_pDataSinkManager.usedMemSize,
         tsStreamBufferSizeBytes);
  atomic_val_compare_exchange_8(&g_slidigGrpMemList.status, DATA_MOVING, DATA_NORMAL);

  return code;
}

static int32_t enableSlidingGrpMemList() {
  if (!g_slidigGrpMemList.enabled) {
    g_slidigGrpMemList.enabled = true;
    static int8_t slidingGrpMemListInit = 0;
    int8_t        init = atomic_val_compare_exchange_8(&slidingGrpMemListInit, 0, 1);
    if (init != 0) {
      return TSDB_CODE_SUCCESS;
    }

    if (g_slidigGrpMemList.pSlidingGrpList == NULL) {
      g_slidigGrpMemList.pSlidingGrpList =
          taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      if (g_slidigGrpMemList.pSlidingGrpList == NULL) {
        stError("failed to create sliding group mem list, err: %s", terrMsg);
        return terrno;
      }
    }
    stInfo("enableSlidingGrpMemList, sliding group mem list set enabled");
  }
  return TSDB_CODE_SUCCESS;
}

static void disableSlidingGrpMemList() {
  if (g_slidigGrpMemList.enabled) {
    g_slidigGrpMemList.enabled = false;
    if (g_slidigGrpMemList.pSlidingGrpList) {
      taosHashClear(g_slidigGrpMemList.pSlidingGrpList);
    }
    stInfo("disableSlidingGrpMemList, sliding group mem list set disabled");
  }
}

int32_t checkAndMoveMemCache(bool forWrite) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!g_slidigGrpMemList.enabled && g_pDataSinkManager.usedMemSize > tsStreamBufferSizeBytes - g_pDataSinkManager.memAlterSize) {
    return enableSlidingGrpMemList();
  } else if (g_slidigGrpMemList.enabled && g_pDataSinkManager.usedMemSize < tsStreamBufferSizeBytes - DS_MEM_SIZE_ALTER_QUIT) {
    disableSlidingGrpMemList();
    return TSDB_CODE_SUCCESS;
  }

  if ((forWrite && g_pDataSinkManager.usedMemSize < tsStreamBufferSizeBytes - DS_MEM_SIZE_RESERVED_FOR_WIRTE) ||
      (!forWrite && hasEnoughMemSize())) {
    return TSDB_CODE_SUCCESS;
  }
  if (forWrite) {
    return moveMemCache();
  } else {
    stDebug("checkAndReleaseBuffer, used mem size: %" PRId64 ", max mem size: %" PRId64 ", for write: %d",
            g_pDataSinkManager.usedMemSize, tsStreamBufferSizeBytes, forWrite);
  }
  return TSDB_CODE_SUCCESS;
}

bool isValidStatusChange(int8_t oldStatus, int8_t newStatus, int8_t mode) {
  bool valid = false;
  switch (oldStatus) {
    case GRP_DATA_IDLE:
      valid = true;  // always valid to change from idle to any status
      break;
    case GRP_DATA_WRITING:
      valid = (newStatus == GRP_DATA_IDLE || newStatus == GRP_DATA_WIAT_READ);
      break;
    case GRP_DATA_WIAT_READ:
      valid = (newStatus == GRP_DATA_IDLE || newStatus == GRP_DATA_READING || newStatus == GRP_DATA_WRITING ||
               (mode | GRP_DATA_WAITREAD_MOVING));
      break;
    case GRP_DATA_READING:
      valid = (newStatus == GRP_DATA_IDLE);
      break;
    case GRP_DATA_MOVING:
      valid = newStatus == GRP_DATA_IDLE;
      break;
    default:
      valid = false;  // invalid old status
  }
  return valid;
}

bool changeMgrStatus(int8_t* pStatus, int8_t status) {
  int8_t        oldStatus = 0;
  int32_t       nums = 0;
  const int32_t retryInterval = 10;  // milliseconds to wait before retrying
  const int32_t maxRetry = 500;      // maximum retry count to change status
  while (true) {
    oldStatus = atomic_load_8(pStatus);
    if (oldStatus == status) {
      return true;  // already in the target status
    }
    if (isValidStatusChange(oldStatus, status, 0)) {
      // try to change status
      int8_t tmp = atomic_val_compare_exchange_8(pStatus, oldStatus, status);
      if (tmp == oldStatus) {
        return true;  // successfully changed status
      }
    } else if (status == GRP_DATA_MOVING) {
      return false;
    }
    nums++;
    if (nums > maxRetry) {
      stError("failed to change status from %d to %d, oldStatus:%d, nums:%d", *pStatus, status, oldStatus, nums);
      return false;
    }
    taosMsleep(retryInterval);  // wait for a while before retrying
  }
}

bool changeMgrStatusToMoving(int8_t* pStatus, int8_t mode) {
  int8_t        oldStatus = 0;
  int32_t       nums = 0;
  const int32_t retryInterval = 10;  // milliseconds to wait before retrying
  const int32_t maxRetry = 500;      // maximum retry count to change status
  while (true) {
    oldStatus = atomic_load_8(pStatus);
    if (oldStatus == GRP_DATA_MOVING) {
      return true;  // already in the target status
    }
    if (isValidStatusChange(oldStatus, GRP_DATA_WAITREAD_MOVING, mode)) {
      // try to change status
      int8_t tmp = atomic_val_compare_exchange_8(pStatus, oldStatus, GRP_DATA_MOVING);
      if (tmp == oldStatus) {
        return true;  // successfully changed status
      }
    }
    return false;
  }
}
