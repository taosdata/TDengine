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

SDataMgrMode getDataModeFromDSMgr(void* pData) {
  if (pData == NULL) {
    return 0;
  }
  STaskDSMgr* pTaskDSMgr = (STaskDSMgr*)pData;
  return pTaskDSMgr->dataMode;
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
  if (pAlignTaskDSMgr->pAlignGrpList) {
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
  SDataMgrMode cleanMode = getDataModeFromDSMgr(*(void**)pData);
  if (cleanMode & DATA_ALLOC_MODE_ALIGN) {
    destroyAlignTaskDSMgr((SAlignTaskDSMgr**)pData);
    return;
  } else if (cleanMode & DATA_ALLOC_MODE_SLIDING) {
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

static int32_t createAlignTaskMgr(int32_t dataMode, int64_t streamId, int64_t taskId, int64_t sessionId,
                                  int32_t tsSlotId, void** ppCache) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SAlignTaskDSMgr* pAlignTaskDSMgr = taosMemCalloc(1, sizeof(SAlignTaskDSMgr));
  if (pAlignTaskDSMgr == NULL) {
    return terrno;
  }

  pAlignTaskDSMgr->dataMode = dataMode;
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
  (*ppSlidingGrpMgr)->winDataInMem = taosArrayInit(0, sizeof(SWindowDataInMem*));
  if ((*ppSlidingGrpMgr)->winDataInMem == NULL) {
    taosMemoryFree(*ppSlidingGrpMgr);
    stError("failed to create window data in mem, err: %s", terrMsg);
    return terrno;
  }
  (*ppSlidingGrpMgr)->blocksInFile = NULL;  // delay init
  (*ppSlidingGrpMgr)->status = GRP_DATA_WRITING;

  return TSDB_CODE_SUCCESS;
}

int32_t createReorderGrpMgr(int64_t groupId, SReorderGrpMgr** ppReorderGrpMgr) {
  *ppReorderGrpMgr = (SReorderGrpMgr*)taosMemoryCalloc(1, sizeof(SReorderGrpMgr));
  if (*ppReorderGrpMgr == NULL) {
    return terrno;
  }
  (*ppReorderGrpMgr)->groupId = groupId;
  (*ppReorderGrpMgr)->usedMemSize = 0;
  tdListInit(&(*ppReorderGrpMgr)->winAllData,  sizeof(SDatasInWindow));
  (*ppReorderGrpMgr)->status = GRP_DATA_WRITING;

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

static int32_t createSlidingTaskMgr(int32_t dataMode, int64_t streamId, int64_t taskId, int64_t sessionId,
                                    int32_t tsSlotId, void** ppCache) {
  SSlidingTaskDSMgr* pSlidingTaskDSMgr = taosMemCalloc(1, sizeof(SSlidingTaskDSMgr));
  if (pSlidingTaskDSMgr == NULL) {
    return terrno;
  }
  pSlidingTaskDSMgr->dataMode = dataMode;
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
  if (dataMode & DATA_ALLOC_MODE_SLIDING) {
    taosHashSetFreeFp(pSlidingTaskDSMgr->pSlidingGrpList, destroySSlidingGrpMgr);
  } else {
    taosHashSetFreeFp(pSlidingTaskDSMgr->pSlidingGrpList, destroyReorderGrpMgr);
  }
  *ppCache = pSlidingTaskDSMgr;
  return TSDB_CODE_SUCCESS;
}

// @brief 初始化数据缓存
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t dataMode, int32_t tsSlotId,
                            void** ppCache) {
  int32_t code = 0;
  *ppCache = NULL;
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, streamId, taskId, sessionId);

  void** ppStreamTaskDSManager = (void**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (ppStreamTaskDSManager == NULL) {
    if (dataMode & DATA_ALLOC_MODE_ALIGN) {
      code = createAlignTaskMgr(dataMode, streamId, taskId, sessionId, tsSlotId, ppCache);
    } else {
      code = createSlidingTaskMgr(dataMode, streamId, taskId, sessionId, tsSlotId, ppCache);
    }
    if (code != 0) {
      stError("failed to create stream task data sink manager, dataMode:%d, err: 0x%0x", dataMode, code);
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
  int32_t code = 0;
  if (pCache == NULL) {
    return;
  }
  if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_ALIGN) {
    SAlignTaskDSMgr* pStreamDataSink = (SAlignTaskDSMgr*)pCache;
    char             key[64] = {0};
    snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId,
             pStreamDataSink->sessionId);
    SAlignTaskDSMgr** ppStreamTaskDSManager =
        (SAlignTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    if (ppStreamTaskDSManager != NULL) {
      code = taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    }
  } else if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_SLIDING) {
    SSlidingTaskDSMgr* pStreamDataSink = (SSlidingTaskDSMgr*)pCache;
    char               key[64] = {0};
    snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, pStreamDataSink->streamId, pStreamDataSink->taskId,
             pStreamDataSink->sessionId);
    SSlidingTaskDSMgr** ppStreamTaskDSManager =
        (SSlidingTaskDSMgr**)taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    if (ppStreamTaskDSManager != NULL) {
      code = taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    }
  } else {
    stError("invalid clean mode: %d", getDataModeFromDSMgr(pCache));
  }
  if (code != 0) {
    stError("failed to remove stream task data sink manager, err: 0x%0x", code);
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

static int32_t getOrCreateSReorderGrpMgr(SSlidingTaskDSMgr* pSlidingTaskMgr, int64_t groupId,
                                          SReorderGrpMgr** ppSlidingGrpMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SReorderGrpMgr*  pGrpMgr = NULL;
  SReorderGrpMgr** ppExistGrpMgr =
      (SReorderGrpMgr**)taosHashGet(pSlidingTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    code = createReorderGrpMgr(groupId, &pGrpMgr);
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
  stDebug("[put data cache] slding, groupId: %" PRId64 " block rows: %" PRId64 " startIndex: %d endIndex: %d", groupId,
          pBlock->info.rows, startIndex, endIndex);
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

  SWindowDataInMem* pSlidingWinInMem = NULL;
  code = buildSingleWindowInMem(pBlock, pStreamTaskMgr->tsSlotId, startIndex, endIndex, &pSlidingWinInMem);
  QUERY_CHECK_CODE(code, lino, _end);

  void* p = taosArrayPush(pSlidingGrpMgr->winDataInMem, &pSlidingWinInMem);
  if (p == NULL) {
    destroySlidingWindowInMem(pSlidingWinInMem);
    stError("failed to push window data into group data sink manager, err: %s", terrMsg);
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  slidingGrpMgrUsedMemAdd(pSlidingGrpMgr, sizeof(SWindowDataInMem) + pSlidingWinInMem->dataLen);

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

int32_t declareStreamDataWindows(void* pCache, int64_t groupId, SArray* pWindows) {
  if (pCache == NULL || pWindows == NULL || pWindows->size <= 0) {
    stError("invalid parameters, pCache: %p, pWindows: %p", pCache, pWindows);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t            nWindows = pWindows->size;
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
  SReorderGrpMgr*   pReorderGrpMgr = NULL;

  code = getOrCreateSReorderGrpMgr(pStreamTaskMgr, groupId, &pReorderGrpMgr);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < nWindows; ++i) {
    STimeRange* pWin = (STimeRange*)taosArrayGet(pWindows, i);
    TSDB_CHECK_NULL(pWin, code, lino, _end, terrno);

    SDatasInWindow dataWindows = {0};
    dataWindows.timeRange.startTime = pWin->startTime;
    dataWindows.timeRange.endTime = pWin->endTime;
    dataWindows.datas = taosArrayInit(4, sizeof(SBlockBuffer));
    TSDB_CHECK_NULL(dataWindows.datas, code, lino, _end, terrno);
    code = tdListAppend(&pReorderGrpMgr->winAllData, &dataWindows);
    TSDB_CHECK_CODE(code, lino, _end);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to declare stream data windows, lino:%d err: %0x  ", lino, code);
  }
  return code;
}

int32_t putDataToReorderTaskMgr(SSlidingTaskDSMgr* pStreamTaskMgr, int64_t groupId, SSDataBlock* pBlock) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SReorderGrpMgr* pReorderGrpMgr = NULL;
  stDebug("[put data cache] multiwins data, groupId: %" PRId64 " block rows: %" PRId64, groupId, pBlock->info.rows);
  code = getOrCreateSReorderGrpMgr(pStreamTaskMgr, groupId, &pReorderGrpMgr);
  if (code != 0) {
    stError("failed to get or create group data sink manager, err: 0x%0x", code);
    return code;
  }
  bool canPut = changeMgrStatus(&pReorderGrpMgr->status, GRP_DATA_WRITING);
  if (!canPut) {
    stError("failed to change group data sink manager status when put data, status: %d", pReorderGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  code = splitBlockToWindows(pReorderGrpMgr, pStreamTaskMgr->tsSlotId, pBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  updateReorderGrpUsedMemSize(pReorderGrpMgr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to put data to align task manager, lino:%d err: %0x", lino, code);
    if (pReorderGrpMgr) {
      (void)changeMgrStatus(&pReorderGrpMgr->status, GRP_DATA_IDLE);
    }
  } else {
    (void)changeMgrStatus(&pReorderGrpMgr->status, GRP_DATA_WIAT_READ);
  }

  return code;
}

int32_t getReorderDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
  SResultIter*       pResultIter = NULL;
  *pIter = NULL;

  stDebug("[get data cache] init groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64, groupId,
          start, end, pStreamTaskMgr->streamId);

  SReorderGrpMgr** ppExistGrpMgr =
      (SReorderGrpMgr**)taosHashGet(pStreamTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    stDebug("[get data cache] init nogroup groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 "STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }
  SReorderGrpMgr* pExistGrpMgr = *ppExistGrpMgr;
  bool             canRead = changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
  if (!canRead) {
    stError("failed to change group data sink manager status when get data, status: %d", pExistGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  SListIter  foundDataIter = {0};
  SListNode* pNode = NULL;
  SDataSinkPos dataPos = DATA_SINK_MEM;


  tdListInitIter(&pExistGrpMgr->winAllData, &foundDataIter, TD_LIST_FORWARD);
  while ((pNode = tdListNext(&foundDataIter)) != NULL) {
    SDatasInWindow* pWinData = (SDatasInWindow*)pNode->data;
    if (pWinData->timeRange.startTime >= start && pWinData->timeRange.startTime <= end) {
      if (pWinData->pBlockFileBuffer && pWinData->pBlockFileBuffer->size > 0) {
        dataPos = DATA_SINK_FILE;
        break;
      }
      if (pWinData->datas->size > 0) {
         dataPos = DATA_SINK_MEM;
        break;
      }
    }
    if (pWinData->timeRange.startTime > end) {
      pNode = NULL;
      break;
    }
  }

  if (pNode == NULL) {
    (void)changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_IDLE);
    stDebug("[get data cache] init nodata 1 groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 "STREAMID:%" PRIx64,
            groupId, start, end, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }

  code = createDataResult((void**)(&pResultIter));
  QUERY_CHECK_CODE(code, lino, _end);
  *pIter = pResultIter;

  pResultIter->dataMode = pStreamTaskMgr->dataMode;
  pResultIter->groupData = pExistGrpMgr;
  pResultIter->dataPos = dataPos;
  pResultIter->pFileMgr = pStreamTaskMgr->pFileMgr;
  pResultIter->tsColSlotId = pStreamTaskMgr->tsSlotId;
  pResultIter->winIndex = (int64_t)pNode;
  pResultIter->offset = 0;
  pResultIter->groupId = groupId;
  pResultIter->reqStartTime = start;
  pResultIter->reqEndTime = end;

_end:
  changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
  if (code != TSDB_CODE_SUCCESS) {
    releaseDataResultAndResetMgrStatus((void**)&pResultIter);
    *pIter = NULL;
    stError("failed to get sliding data cache, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

int32_t putDataToAlignTaskMgr(SAlignTaskDSMgr* pStreamTaskMgr, int64_t groupId, TSKEY wstart, TSKEY wend,
                              SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SAlignGrpMgr* pAlignGrpMgr = NULL;
  stDebug("[put data cache] align, groupId: %" PRId64 " wstart: %" PRId64 " wend: %" PRId64 " block rows: %" PRId64
          " startIndex: %d endIndex: %d",
          groupId, wstart, wend, pBlock->info.rows, startIndex, endIndex);
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

  code = buildAlignWindowInMemBlock(pAlignGrpMgr, pBlock, pStreamTaskMgr->tsSlotId, wstart, wend, startIndex, endIndex);
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
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
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
    TAOS_CHECK_EXIT(code);
  }
  if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_ALIGN) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    code = putDataToAlignTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_SLIDING) {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    code = putDataToSlidingTaskMgr(pStreamTaskMgr, groupId, pBlock, startIndex, endIndex);
  } else {
    stError("putStreamDataCache failed, unknown data allocation mode");
    code = TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  (void)checkAndMoveMemCache(false);

_exit:

  if (code) {
    stError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stDebug("group %" PRId64 " time range [%" PRId64 ", %" PRId64 "] rows range [%d, %d] added to cache", groupId,
            wstart, wend, startIndex, endIndex);
  }
  return code;
}

int32_t putStreamMultiWinDataCache(void* pCache, int64_t groupId, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  if (pCache == NULL) {
    stError("putStreamDataCache param invalid, pCache is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  code = checkAndMoveMemCache(true);
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to check and move mem cache for write, code: %d err: %s", code, terrMsg);
    TAOS_CHECK_EXIT(code);
  }
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
  code = putDataToReorderTaskMgr(pStreamTaskMgr, groupId, pBlock);
  (void)checkAndMoveMemCache(false);

_exit:

  if (code) {
    stError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
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
  if ((getDataModeFromDSMgr(pCache) & DATA_CLEAN_IMMEDIATE) == 0) {
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
  pResultIter->dataMode = pStreamTaskMgr->dataMode;
  pResultIter->groupData = pExistGrpMgr;
  pResultIter->pFileMgr = pStreamTaskMgr->pFileMgr;
  pResultIter->tsColSlotId = pStreamTaskMgr->tsSlotId;
  pResultIter->blockIndex = 0;
  pResultIter->winIndex = 0;
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
    releaseDataResultAndResetMgrStatus((void**)&pResultIter);
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

  stDebug("[get data cache] init groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 " STREAMID:%" PRIx64, groupId,
          start, end, pStreamTaskMgr->streamId);

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

  pResultIter->dataMode = pStreamTaskMgr->dataMode;
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
  (void)changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
  if (code != TSDB_CODE_SUCCESS) {
    releaseDataResultAndResetMgrStatus((void**)&pResultIter);
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

  if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_ALIGN) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    return getAlignDataCache(pStreamTaskMgr, groupId, start, end, pIter);
  } else if (getDataModeFromDSMgr(pCache) & DATA_ALLOC_MODE_SLIDING) {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    return getSlidingDataCache(pStreamTaskMgr, groupId, start, end, pIter);
  } else { // DATA_ALLOC_MODE_UNSORTED
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    return getReorderDataCache(pStreamTaskMgr, groupId, start, end, pIter);
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

  if (pResult != NULL) {
    taosMemoryFree(pResult);
    *pIter = NULL;
  }
}

void releaseDataResultAndResetMgrStatus(void** pIter) {
  if (pIter == NULL || *pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;

  if (pResult->dataMode & DATA_ALLOC_MODE_SLIDING) {
    SSlidingGrpMgr* pSlidingGrpMgr = (SSlidingGrpMgr*)pResult->groupData;
    (void)changeMgrStatus(&pSlidingGrpMgr->status, GRP_DATA_IDLE);
  } else {
    SAlignGrpMgr* pAlignGrpMgr = (SAlignGrpMgr*)pResult->groupData;
    (void)changeMgrStatus(&pAlignGrpMgr->status, GRP_DATA_IDLE);
  }

  releaseDataResult(pIter);
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
    releaseDataResultAndResetMgrStatus(ppIter);
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
  int64_t groupId = pResult->groupId;
  stDebug("[get data cache] start groupID:%" PRId64 ", start:%" PRId64 " end:%" PRId64
          " dataPos: %d, winIndex: %" PRId64,
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
          stError("getNextStreamDataCache failed, groupId: %" PRId64 " start:%" PRId64 " end:%" PRId64
                  " dataPos: %d, winIndex: %" PRId64 ", tmpBlocksInMem size: %" PRIzu,
                  pResult->groupId, pResult->reqStartTime, pResult->reqEndTime, pResult->dataPos, pResult->winIndex,
                  pResult->tmpBlocksInMem->size);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

  if (finished) {
    releaseDataResultAndResetMgrStatus(pIter);
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
    stError("[get data cache] end, failed to get next data from cache, groupId: %" PRId64 " err: %s, lineno:%d",
            groupId, terrMsg, lino);
  } else if (ppBlock != NULL && *ppBlock != NULL) {
    stDebug("[get data cache] end, groupId: %" PRId64 " block rows: %" PRId64 " next:%p", groupId,
            (*ppBlock)->info.rows, *pIter);
  } else {
    stDebug("[get data cache] end, not found data, groupId: %" PRId64 " next:%p", groupId, *pIter);
  }
  return code;
}

void cancelStreamDataCacheIterate(void** pIter) { releaseDataResultAndResetMgrStatus(pIter); }

void destroyDataSinkMgr() {
  if (g_pDataSinkManager.dsStreamTaskList) {
    taosHashCleanup(g_pDataSinkManager.dsStreamTaskList);
    g_pDataSinkManager.dsStreamTaskList = NULL;
  }
}

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
    if (pTaskMgr->dataMode & DATA_ALLOC_MODE_REORDER) {
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

  int8_t status = atomic_val_compare_exchange_8(&g_reorderGrpMemList.status, DATA_NORMAL, DATA_MOVING);
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
  atomic_val_compare_exchange_8(&g_reorderGrpMemList.status, DATA_MOVING, DATA_NORMAL);

  return code;
}

static int32_t enableSlidingGrpMemList() {
  if (!g_reorderGrpMemList.enabled) {
    g_reorderGrpMemList.enabled = true;
    static int8_t slidingGrpMemListInit = 0;
    int8_t        init = atomic_val_compare_exchange_8(&slidingGrpMemListInit, 0, 1);
    if (init != 0) {
      return TSDB_CODE_SUCCESS;
    }

    if (g_reorderGrpMemList.pReorderGrpList == NULL) {
      g_reorderGrpMemList.pReorderGrpList =
          taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      if (g_reorderGrpMemList.pReorderGrpList == NULL) {
        stError("failed to create sliding group mem list, err: %s", terrMsg);
        return terrno;
      }
    }
    stInfo("enableSlidingGrpMemList, sliding group mem list set enabled");
  }
  return TSDB_CODE_SUCCESS;
}

static void disableSlidingGrpMemList() {
  if (g_reorderGrpMemList.enabled) {
    g_reorderGrpMemList.enabled = false;
    if (g_reorderGrpMemList.pReorderGrpList) {
      taosHashClear(g_reorderGrpMemList.pReorderGrpList);
    }
    stInfo("disableSlidingGrpMemList, sliding group mem list set disabled");
  }
}

int32_t checkAndMoveMemCache(bool forWrite) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!g_reorderGrpMemList.enabled &&
      g_pDataSinkManager.usedMemSize > tsStreamBufferSizeBytes - g_pDataSinkManager.memAlterSize) {
    return enableSlidingGrpMemList();
  } else if (g_reorderGrpMemList.enabled &&
             g_pDataSinkManager.usedMemSize < tsStreamBufferSizeBytes - DS_MEM_SIZE_ALTER_QUIT) {
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

int32_t clearStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
  SResultIter*       pResultIter = NULL;

  if (pCache == NULL) {
    stError("clearStreamDataCache param invalid, pCache is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (wstart < 0 || wstart > wend) {
    stError("clearStreamDataCache param invalid, wstart:%" PRId64 " wend:%" PRId64, wstart, wend);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SReorderGrpMgr** ppExistGrpMgr =
      (SReorderGrpMgr**)taosHashGet(pStreamTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    stDebug("[remove data cache] nogroup groupID:%" PRId64 ",  start:%" PRId64 " end:%" PRId64 "STREAMID:%" PRIx64,
            groupId, wstart, wend, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }
  return clearReorderDataInMem(*ppExistGrpMgr, wstart, wend);
}
