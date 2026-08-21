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
#include "osFile.h"
#include "osMemory.h"
#include "osTime.h"
#include "stream.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "thash.h"

SDataSinkManager2 g_pDataSinkManager = {0};

#define STREAM_SCOPED_CACHE_BUCKETS 64
#define STREAM_SCOPED_CACHE_BLOCK_ROWS 1024

typedef struct SScopedCacheBlock {
  struct SScopedCacheBlock* pNext;
  TSKEY                     minTs;
  TSKEY                     maxTs;
  int32_t                   rows;
  SDataBlockInfo            info;
  SSDataBlock*              pBlock;
  int64_t                   payloadMemSize;
  SFileBlockInfo            fileBlock;
  int32_t                   fileDataLen;
} SScopedCacheBlock;

typedef struct SScopedCacheEntry {
  struct SScopedCacheEntry* pNextScope;
  struct SScopedCacheEntry* pNextGroup;
  int64_t                   gid;
  int32_t                   keyLen;
  int32_t                   tsSlotId;
  uint64_t                  entryId;
  void*                     pKey;
  SArray*                   pSchema;
  int64_t                   metadataMemSize;
  SScopedCacheBlock*        pBlocks;
  SScopedCacheBlock*        pLastBlock;
  SScopedCacheBlock**       ppBlockTail;
} SScopedCacheEntry;

typedef struct {
  const SSDataBlock* pSourceBlock;
  int32_t            sourceRowIndex;
} SStagedScopedCacheRowKey;

typedef struct {
  SScopedCacheEntry* pEntry;
  SHashObj*          pSourceRows;
} SStagedScopedCacheScope;

typedef struct {
  TdThreadMutex       lock;
  SScopedCacheEntry*  scopeBuckets[STREAM_SCOPED_CACHE_BUCKETS];
  SScopedCacheEntry** scopeTails[STREAM_SCOPED_CACHE_BUCKETS];
  SScopedCacheEntry*  groupBuckets[STREAM_SCOPED_CACHE_BUCKETS];
  SScopedCacheEntry** groupTails[STREAM_SCOPED_CACHE_BUCKETS];
  TdFilePtr           pFile;
  char                fileName[FILENAME_MAX];
  int64_t             fileSize;
  int64_t             fileBlockCount;
  uint64_t            nextEntryId;
  SArray*             pFreeFileBlocks;
} SScopedCacheStore;

typedef struct {
  int8_t  cleanMode;
  void*   pScopedStore;
  int32_t leaseCount;
  int32_t refCount;
  bool    retired;
  bool    detached;
  int64_t streamId;
  int64_t taskId;
  int64_t sessionId;
  int32_t tsSlotId;
} SStreamDataCacheBase;

typedef struct {
  SStreamDataCacheBase* pCache;
  void*                 pKey;
  int32_t               keyLen;
  uint64_t              entryId;
  SScopedCacheBlock*    pNextBlock;
  SScopedCacheBlock*    pSnapshotLastBlock;
  int32_t               snapshotLastRows;
} SScopedCacheResultState;

typedef struct {
  void*    pCache;
  uint64_t generation;
} SStreamDataCacheRegistration;

struct SStreamDataCacheLease {
  SStreamDataCacheBase* pCache;
};

struct SStreamDataCacheWriteBatch {
  SStreamDataCacheBase* pCache;
  SArray*               pScopes;
  SHashObj*             pScopeIndex;
};

static uint32_t scopedCacheHash(const void* pData, int32_t len) {
  const uint8_t* p = pData;
  uint32_t       hash = 2166136261u;
  for (int32_t i = 0; i < len; ++i) {
    hash = (hash ^ p[i]) * 16777619u;
  }
  return hash;
}

static uint32_t scopedGroupHash(int64_t gid) {
  return scopedCacheHash(&gid, sizeof(gid)) % STREAM_SCOPED_CACHE_BUCKETS;
}

static int32_t buildScopedCacheKey(const SStreamCacheScope* pScope, void** ppKey, int32_t* pKeyLen) {
  if (pScope == NULL || ppKey == NULL || pKeyLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t depth = taosArrayGetSize(pScope->lineage.pScopes);
  if (pScope->lineage.pScopes != NULL && pScope->lineage.pScopes->elemSize != sizeof(SScopeInstanceId)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t keyLen = sizeof(pScope->gid) + sizeof(depth) +
                   depth * (sizeof(int32_t) + sizeof(int8_t) + sizeof(TSKEY) + sizeof(int64_t));
  uint8_t* pKey = taosMemoryMalloc(keyLen);
  if (pKey == NULL) {
    return terrno;
  }
  int32_t offset = 0;
  memcpy(pKey + offset, &pScope->gid, sizeof(pScope->gid));
  offset += sizeof(pScope->gid);
  memcpy(pKey + offset, &depth, sizeof(depth));
  offset += sizeof(depth);
  for (int32_t i = 0; i < depth; ++i) {
    const SScopeInstanceId* pId = taosArrayGet(pScope->lineage.pScopes, i);
    if (pId == NULL) {
      taosMemoryFree(pKey);
      return TSDB_CODE_INVALID_PARA;
    }
    memcpy(pKey + offset, &pId->layerIndex, sizeof(pId->layerIndex));
    offset += sizeof(pId->layerIndex);
    memcpy(pKey + offset, &pId->triggerType, sizeof(pId->triggerType));
    offset += sizeof(pId->triggerType);
    memcpy(pKey + offset, &pId->openingTs, sizeof(pId->openingTs));
    offset += sizeof(pId->openingTs);
    memcpy(pKey + offset, &pId->nativeDiscriminator, sizeof(pId->nativeDiscriminator));
    offset += sizeof(pId->nativeDiscriminator);
  }
  *ppKey = pKey;
  *pKeyLen = keyLen;
  return TSDB_CODE_SUCCESS;
}

static int64_t gScopedCacheFileId = 0;

static int64_t scopedCacheBlockPayloadSize(const SSDataBlock* pBlock) {
  int32_t size = blockGetEncodeSize(pBlock);
  return size > 0 ? size : 0;
}

static bool scopedCacheBlockInfoEquals(const SDataBlockInfo* pLeft, const SDataBlockInfo* pRight) {
  return pLeft->window.skey == pRight->window.skey && pLeft->window.ekey == pRight->window.ekey &&
         pLeft->id.uid == pRight->id.uid && pLeft->id.blockId == pRight->id.blockId &&
         pLeft->id.groupId == pRight->id.groupId && pLeft->id.baseGId == pRight->id.baseGId &&
         pLeft->dataLoad == pRight->dataLoad && pLeft->scanFlag == pRight->scanFlag &&
         pLeft->blankFill == pRight->blankFill && pLeft->version == pRight->version &&
         pLeft->childId == pRight->childId && pLeft->calWin.skey == pRight->calWin.skey &&
         pLeft->calWin.ekey == pRight->calWin.ekey && pLeft->watermark == pRight->watermark &&
         strcmp(pLeft->parTbName, pRight->parTbName) == 0;
}

static void restoreScopedCacheBlockInfo(const SDataBlockInfo* pStored, SSDataBlock* pBlock) {
  pBlock->info.window = pStored->window;
  pBlock->info.id = pStored->id;
  pBlock->info.dataLoad = pStored->dataLoad;
  pBlock->info.scanFlag = pStored->scanFlag;
  pBlock->info.blankFill = pStored->blankFill;
  pBlock->info.version = pStored->version;
  pBlock->info.childId = pStored->childId;
  pBlock->info.calWin = pStored->calWin;
  pBlock->info.watermark = pStored->watermark;
  tstrncpy(pBlock->info.parTbName, pStored->parTbName, sizeof(pBlock->info.parTbName));
}

static void destroyUntrackedScopedCacheBlock(SScopedCacheBlock* pBlock) {
  if (pBlock == NULL) return;
  blockDataDestroy(pBlock->pBlock);
  taosMemoryFree(pBlock);
}

static void destroyUntrackedScopedCacheEntry(SScopedCacheEntry* pEntry) {
  if (pEntry == NULL) return;
  taosMemoryFree(pEntry->pKey);
  taosArrayDestroy(pEntry->pSchema);
  taosMemoryFree(pEntry);
}

static void destroyStagedScopedCacheScope(void* pValue) {
  if (pValue == NULL) return;
  SStagedScopedCacheScope* pStaged = *(SStagedScopedCacheScope**)pValue;
  if (pStaged == NULL) return;
  if (pStaged->pEntry != NULL) {
    SScopedCacheBlock* pBlock = pStaged->pEntry->pBlocks;
    while (pBlock != NULL) {
      SScopedCacheBlock* pNext = pBlock->pNext;
      destroyUntrackedScopedCacheBlock(pBlock);
      pBlock = pNext;
    }
    pStaged->pEntry->pBlocks = NULL;
    destroyUntrackedScopedCacheEntry(pStaged->pEntry);
  }
  taosHashCleanup(pStaged->pSourceRows);
  taosMemoryFree(pStaged);
}

static int32_t closeScopedCacheFile(SScopedCacheStore* pStore) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pStore->pFile != NULL && taosCloseFile(&pStore->pFile) != 0) code = terrno;
  if (pStore->fileName[0] != '\0' && taosRemoveFile(pStore->fileName) != 0 && code == TSDB_CODE_SUCCESS) code = terrno;
  pStore->fileName[0] = '\0';
  pStore->fileSize = 0;
  taosArrayClear(pStore->pFreeFileBlocks);
  return code;
}

static void recycleScopedCacheFileRange(SScopedCacheStore* pStore, const SFileBlockInfo* pFileBlock) {
  if (pFileBlock->size <= 0) return;
  if (taosArrayEnsureCap(pStore->pFreeFileBlocks, taosArrayGetSize(pStore->pFreeFileBlocks) + 1) != TSDB_CODE_SUCCESS) {
    stError("failed to retain scoped cache free file range, err: %s", terrMsg);
    return;
  }

  SFileBlockInfo merged = *pFileBlock;
  int32_t        insertIndex = taosArrayGetSize(pStore->pFreeFileBlocks);
  for (int32_t i = 0; i < taosArrayGetSize(pStore->pFreeFileBlocks); ++i) {
    SFileBlockInfo* pFree = taosArrayGet(pStore->pFreeFileBlocks, i);
    if (pFree->offset + pFree->size < merged.offset) continue;
    if (merged.offset + merged.size < pFree->offset) {
      insertIndex = i;
      break;
    }
    int64_t mergedEnd = TMAX(merged.offset + merged.size, pFree->offset + pFree->size);
    merged.offset = TMIN(merged.offset, pFree->offset);
    merged.size = mergedEnd - merged.offset;
    (void)taosArrayRemove(pStore->pFreeFileBlocks, i);
    insertIndex = i--;
  }

  if (merged.offset + merged.size == pStore->fileSize) {
    pStore->fileSize = merged.offset;
    return;
  }
  if (taosArrayInsert(pStore->pFreeFileBlocks, insertIndex, &merged) == NULL) {
    stError("failed to retain scoped cache free file range, err: %s", terrMsg);
  }
}

static void releaseScopedCacheFileBlock(SScopedCacheStore* pStore, const SFileBlockInfo* pFileBlock) {
  if (pFileBlock->size <= 0) return;
  if (pStore->fileBlockCount > 0) --pStore->fileBlockCount;
  if (pStore->fileBlockCount == 0) {
    int32_t code = closeScopedCacheFile(pStore);
    if (code != TSDB_CODE_SUCCESS) stError("failed to remove scoped cache file, err: %s", tstrerror(code));
    return;
  }
  recycleScopedCacheFileRange(pStore, pFileBlock);
}

static void destroyTrackedScopedCacheBlock(SScopedCacheStore* pStore, SScopedCacheBlock* pBlock) {
  if (pBlock == NULL) return;
  if (pBlock->pBlock != NULL) {
    blockDataDestroy(pBlock->pBlock);
    (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pBlock->payloadMemSize);
  } else {
    releaseScopedCacheFileBlock(pStore, &pBlock->fileBlock);
  }
  (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, sizeof(*pBlock));
  taosMemoryFree(pBlock);
}

static void destroyTrackedScopedCacheEntry(SScopedCacheStore* pStore, SScopedCacheEntry* pEntry) {
  if (pEntry == NULL) return;
  SScopedCacheBlock* pBlock = pEntry->pBlocks;
  while (pBlock != NULL) {
    SScopedCacheBlock* pNext = pBlock->pNext;
    destroyTrackedScopedCacheBlock(pStore, pBlock);
    pBlock = pNext;
  }
  (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pEntry->metadataMemSize);
  destroyUntrackedScopedCacheEntry(pEntry);
}

static int32_t createScopedCacheStore(void** ppStore) {
  SScopedCacheStore* pStore = taosMemoryCalloc(1, sizeof(SScopedCacheStore));
  if (pStore == NULL) {
    return terrno;
  }
  int32_t code = taosThreadMutexInit(&pStore->lock, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pStore);
    return code;
  }
  pStore->pFreeFileBlocks = taosArrayInit(8, sizeof(SFileBlockInfo));
  if (pStore->pFreeFileBlocks == NULL) {
    taosThreadMutexDestroy(&pStore->lock);
    taosMemoryFree(pStore);
    return terrno;
  }
  for (int32_t i = 0; i < STREAM_SCOPED_CACHE_BUCKETS; ++i) {
    pStore->scopeTails[i] = &pStore->scopeBuckets[i];
    pStore->groupTails[i] = &pStore->groupBuckets[i];
  }
  *ppStore = pStore;
  return TSDB_CODE_SUCCESS;
}

static void destroyScopedCacheStore(void** ppStore) {
  if (ppStore == NULL || *ppStore == NULL) {
    return;
  }
  SScopedCacheStore* pStore = *ppStore;
  for (int32_t i = 0; i < STREAM_SCOPED_CACHE_BUCKETS; ++i) {
    SScopedCacheEntry* pEntry = pStore->scopeBuckets[i];
    while (pEntry != NULL) {
      SScopedCacheEntry* pNext = pEntry->pNextScope;
      destroyTrackedScopedCacheEntry(pStore, pEntry);
      pEntry = pNext;
    }
  }
  (void)closeScopedCacheFile(pStore);
  taosArrayDestroy(pStore->pFreeFileBlocks);
  taosThreadMutexDestroy(&pStore->lock);
  taosMemoryFree(pStore);
  *ppStore = NULL;
}

static int32_t getCacheKey(const SStreamDataCacheBase* pCache, char* pKey, int32_t keyLen) {
  int32_t written =
      snprintf(pKey, keyLen, "%" PRId64 "_%" PRId64 "_%" PRId64, pCache->streamId, pCache->taskId, pCache->sessionId);
  return written > 0 && written < keyLen ? written : TSDB_CODE_INVALID_PARA;
}

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
  if (!g_pDataSinkManager.registrationLockInited) {
    code = taosThreadMutexInit(&g_pDataSinkManager.registrationLock, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    g_pDataSinkManager.registrationLockInited = true;
  }
  g_pDataSinkManager.dsStreamTaskList =
      taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (g_pDataSinkManager.dsStreamTaskList == NULL) {
    return terrno;
  }
  stInfo("data sink manager init success, max mem size: %" PRId64, tsStreamBufferSizeBytes);
  return TSDB_CODE_SUCCESS;
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
  destroyScopedCacheStore(&pSlidingTaskDSMgr->pScopedStore);
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

  destroyScopedCacheStore(&pAlignTaskDSMgr->pScopedStore);

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
  code = createScopedCacheStore(&pAlignTaskDSMgr->pScopedStore);
  if (code != TSDB_CODE_SUCCESS) {
    taosHashCleanup(pAlignTaskDSMgr->pAlignGrpList);
    taosMemoryFree(pAlignTaskDSMgr);
    return code;
  }
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

static void cleanSlidingGrpMgr(SSlidingGrpMgr* pGroupData) {
  if (pGroupData->winDataInMem) {
    taosArrayClearEx(pGroupData->winDataInMem, destroySlidingWindowInMemPP);
  }
  if (pGroupData->blocksInFile) {
    // todo destroy blocks in file
    taosArrayDestroy(pGroupData->blocksInFile);
    pGroupData->blocksInFile = NULL;
  }
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
  int32_t code = createScopedCacheStore(&pSlidingTaskDSMgr->pScopedStore);
  if (code != TSDB_CODE_SUCCESS) {
    taosHashCleanup(pSlidingTaskDSMgr->pSlidingGrpList);
    taosMemoryFree(pSlidingTaskDSMgr);
    return code;
  }
  *ppCache = pSlidingTaskDSMgr;
  return TSDB_CODE_SUCCESS;
}

static void destroyStreamDataCacheStorage(void* pCache) {
  if (pCache == NULL) {
    return;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    destroyAlignTaskDSMgr((SAlignTaskDSMgr**)&pCache);
  } else if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_EXPIRED) {
    destroySlidingTaskDSMgr((SSlidingTaskDSMgr**)&pCache);
  }
}

int32_t createDetachedStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t cleanMode,
                                      int32_t tsSlotId, void** ppCache) {
  if (ppCache == NULL || *ppCache != NULL || (cleanMode != DATA_CLEAN_IMMEDIATE && cleanMode != DATA_CLEAN_EXPIRED)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = cleanMode == DATA_CLEAN_IMMEDIATE
                     ? createAlignTaskMgr(streamId, taskId, sessionId, tsSlotId, ppCache)
                     : createSlidingTaskMgr(streamId, taskId, sessionId, tsSlotId, ppCache);
  if (code == TSDB_CODE_SUCCESS) {
    SStreamDataCacheBase* pCache = *ppCache;
    pCache->refCount = 1;
    pCache->detached = true;
  }
  return code;
}

int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t cleanMode, int32_t tsSlotId,
                            void** ppCache) {
  if (ppCache == NULL || g_pDataSinkManager.dsStreamTaskList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppCache = NULL;
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, streamId, taskId, sessionId);

  int32_t code = taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStreamDataCacheRegistration* pRegistration = taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (pRegistration != NULL) {
    *ppCache = pRegistration->pCache;
    taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
    return TSDB_CODE_SUCCESS;
  }

  code = createDetachedStreamDataCache(streamId, taskId, sessionId, cleanMode, tsSlotId, ppCache);
  if (code == TSDB_CODE_SUCCESS) {
    ((SStreamDataCacheBase*)*ppCache)->detached = false;
    SStreamDataCacheRegistration registration = {.pCache = *ppCache, .generation = 1};
    code = taosHashPut(g_pDataSinkManager.dsStreamTaskList, key, strlen(key), &registration, sizeof(registration));
  }
  taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  if (code != TSDB_CODE_SUCCESS && *ppCache != NULL) {
    destroyStreamDataCacheStorage(*ppCache);
    *ppCache = NULL;
  }
  return code;
}

static void releaseStreamDataCacheReference(SStreamDataCacheBase* pCache) {
  if (atomic_sub_fetch_32(&pCache->refCount, 1) == 0) {
    destroyStreamDataCacheStorage(pCache);
  }
}

void retireStreamDataCache(void** ppCache) {
  if (ppCache == NULL || *ppCache == NULL) {
    return;
  }
  SStreamDataCacheBase* pCache = *ppCache;
  *ppCache = NULL;
  if (!atomic_val_compare_exchange_8((int8_t*)&pCache->retired, false, true)) {
    releaseStreamDataCacheReference(pCache);
  }
}

void destroyStreamDataCache(void* pCache) {
  if (pCache == NULL) {
    return;
  }
  SStreamDataCacheBase* pBase = pCache;
  char                  key[64] = {0};
  int32_t               keyLen = getCacheKey(pBase, key, sizeof(key));
  if (keyLen > 0 && g_pDataSinkManager.dsStreamTaskList != NULL) {
    taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
    SStreamDataCacheRegistration* pRegistration = taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, keyLen);
    if (pRegistration != NULL && pRegistration->pCache == pCache) {
      taosHashRemove(g_pDataSinkManager.dsStreamTaskList, key, keyLen);
    }
    taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  }
  retireStreamDataCache(&pCache);
}

int32_t replaceStreamDataCacheRegistration(int64_t streamId, int64_t taskId, int64_t sessionId, void* pExpectedOld,
                                           void* pNew, void** ppRetired) {
  if (pExpectedOld == NULL || pNew == NULL || ppRetired == NULL || *ppRetired != NULL ||
      g_pDataSinkManager.dsStreamTaskList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SStreamDataCacheBase* pNewBase = pNew;
  if (pNewBase->streamId != streamId || pNewBase->taskId != taskId || pNewBase->sessionId != sessionId ||
      !pNewBase->detached) {
    return TSDB_CODE_INVALID_PARA;
  }
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, streamId, taskId, sessionId);
  int32_t code = taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStreamDataCacheRegistration* pRegistration = taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
  if (pRegistration == NULL || pRegistration->pCache != pExpectedOld) {
    code = TSDB_CODE_INVALID_PARA;
  } else {
    pRegistration->pCache = pNew;
    ++pRegistration->generation;
    pNewBase->detached = false;
    ((SStreamDataCacheBase*)pExpectedOld)->detached = true;
    *ppRetired = pExpectedOld;
  }
  taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  return code;
}

int32_t acquireStreamDataCacheLease(int64_t streamId, int64_t taskId, int64_t sessionId,
                                    SStreamDataCacheLease** ppLease, void** ppCache) {
  if (ppLease == NULL || *ppLease != NULL || ppCache == NULL || g_pDataSinkManager.dsStreamTaskList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppCache = NULL;
  SStreamDataCacheLease* pLease = taosMemoryCalloc(1, sizeof(SStreamDataCacheLease));
  if (pLease == NULL) {
    return terrno;
  }
  char key[64] = {0};
  snprintf(key, sizeof(key), "%" PRId64 "_%" PRId64 "_%" PRId64, streamId, taskId, sessionId);
  int32_t code = taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
  if (code == TSDB_CODE_SUCCESS) {
    SStreamDataCacheRegistration* pRegistration = taosHashGet(g_pDataSinkManager.dsStreamTaskList, key, strlen(key));
    if (pRegistration == NULL || pRegistration->pCache == NULL) {
      code = TSDB_CODE_INVALID_PARA;
    } else {
      pLease->pCache = pRegistration->pCache;
      atomic_add_fetch_32(&pLease->pCache->refCount, 1);
      atomic_add_fetch_32(&pLease->pCache->leaseCount, 1);
      *ppCache = pLease->pCache;
      *ppLease = pLease;
    }
    taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pLease);
  }
  return code;
}

void releaseStreamDataCacheLease(SStreamDataCacheLease** ppLease) {
  if (ppLease == NULL || *ppLease == NULL) {
    return;
  }
  SStreamDataCacheBase* pCache = (*ppLease)->pCache;
  taosMemoryFree(*ppLease);
  *ppLease = NULL;
  atomic_sub_fetch_32(&pCache->leaseCount, 1);
  releaseStreamDataCacheReference(pCache);
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
  int32_t         cols = pBlock->pDataBlock ? pBlock->pDataBlock->size : 0;
  stDebug("[put data cache] slding, STREAMID:%" PRIx64 " groupId: %" PRId64 " block rows: %" PRId64
          " cols:%d startIndex: %d endIndex: %d",
          pStreamTaskMgr->streamId, groupId, pBlock->info.rows, cols, startIndex, endIndex);
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
  int32_t       cols = pBlock->pDataBlock ? pBlock->pDataBlock->size : 0;
  stDebug("[put data cache] align, STREAMID:%" PRIx64 " groupId: %" PRId64 " wstart: %" PRId64 " wend: %" PRId64
          " block rows: %" PRId64 " cols:%d startIndex: %d endIndex: %d",
          pStreamTaskMgr->streamId, groupId, wstart, wend, pBlock->info.rows, cols, startIndex, endIndex);
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
  int64_t streamId = 0;
  if (pCache == NULL) {
    stError("putStreamDataCache param invalid, pCache is NULL");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    streamId = pStreamTaskMgr->streamId;
  } else {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    streamId = pStreamTaskMgr->streamId;
  }
  stsDebug("putStreamDataCache groupId:%" PRId64 " wstart:%" PRId64 " wend:%" PRId64 " start:%d end:%d", groupId,
           wstart, wend, startIndex, endIndex);
  if (wstart > wend) {
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
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    SAlignTaskDSMgr* pStreamTaskMgr = (SAlignTaskDSMgr*)pCache;
    printDataBlock(pBlock, __func__, "", pStreamTaskMgr->streamId);
    code = putDataToAlignTaskMgr(pStreamTaskMgr, groupId, wstart, wend, pBlock, startIndex, endIndex);
  } else {
    SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;
    printDataBlock(pBlock, __func__, "", pStreamTaskMgr->streamId);
    code = putDataToSlidingTaskMgr(pStreamTaskMgr, groupId, pBlock, startIndex, endIndex);
  }
  (void)checkAndMoveMemCache(false);

_exit:

  if (code) {
    stError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stDebug("group %" PRId64 " time range [%" PRId64 ", %" PRId64 "] rows range [%d, %d] added to cache", 
        groupId, wstart, wend, startIndex, endIndex);
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
  if (getCleanModeFromDSMgr(pCache) != DATA_CLEAN_IMMEDIATE) {
    stError("moveStreamDataCache param invalid, cleanMode is not immediate");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  return moveDataToAlignTaskMgr((SAlignTaskDSMgr*)pCache, pBlock, groupId, wstart, wend);
}

static void unlinkScopedEntryFromGroup(SScopedCacheStore* pStore, SScopedCacheEntry* pTarget) {
  uint32_t            bucket = scopedGroupHash(pTarget->gid);
  SScopedCacheEntry** ppEntry = &pStore->groupBuckets[bucket];
  while (*ppEntry != NULL) {
    if (*ppEntry == pTarget) {
      if (pTarget->pNextGroup == NULL) {
        pStore->groupTails[bucket] = ppEntry;
      }
      *ppEntry = pTarget->pNextGroup;
      return;
    }
    ppEntry = &(*ppEntry)->pNextGroup;
  }
}

static bool scopedKeyEquals(const SScopedCacheEntry* pEntry, const void* pKey, int32_t keyLen) {
  return pEntry->keyLen == keyLen && memcmp(pEntry->pKey, pKey, keyLen) == 0;
}

static SScopedCacheEntry* findScopedCacheEntry(SScopedCacheStore* pStore, const void* pKey, int32_t keyLen) {
  uint32_t bucket = scopedCacheHash(pKey, keyLen) % STREAM_SCOPED_CACHE_BUCKETS;
  for (SScopedCacheEntry* pEntry = pStore->scopeBuckets[bucket]; pEntry != NULL; pEntry = pEntry->pNextScope) {
    if (scopedKeyEquals(pEntry, pKey, keyLen)) return pEntry;
  }
  return NULL;
}

static int32_t createStagedScopedCacheEntry(int64_t gid, void* pKey, int32_t keyLen, int32_t tsSlotId,
                                            const SSDataBlock* pBlock, SScopedCacheEntry** ppEntry) {
  SScopedCacheEntry* pEntry = taosMemoryCalloc(1, sizeof(*pEntry));
  if (pEntry == NULL) return terrno;
  int32_t numCols = taosArrayGetSize(pBlock->pDataBlock);
  pEntry->pSchema = taosArrayInit(numCols, sizeof(SColumnInfo));
  if (pEntry->pSchema == NULL) {
    taosMemoryFree(pEntry);
    return terrno;
  }
  for (int32_t i = 0; i < numCols; ++i) {
    const SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL || taosArrayPush(pEntry->pSchema, &pCol->info) == NULL) {
      destroyUntrackedScopedCacheEntry(pEntry);
      return pCol == NULL ? TSDB_CODE_INVALID_PARA : terrno;
    }
  }
  pEntry->gid = gid;
  pEntry->pKey = pKey;
  pEntry->keyLen = keyLen;
  pEntry->tsSlotId = tsSlotId;
  pEntry->ppBlockTail = &pEntry->pBlocks;
  pEntry->metadataMemSize = sizeof(*pEntry) + keyLen + (int64_t)numCols * sizeof(SColumnInfo);
  *ppEntry = pEntry;
  return TSDB_CODE_SUCCESS;
}

static int32_t ensureScopedCacheFile(SScopedCacheStore* pStore) {
  if (pStore->pFile != NULL) return TSDB_CODE_SUCCESS;
  int64_t fileId = atomic_add_fetch_64(&gScopedCacheFileId, 1);
  int32_t written =
      snprintf(pStore->fileName, sizeof(pStore->fileName), "%s/tdengine_stream_data/scoped_%" PRId64 "_%" PRId64,
               tsTempDir, taosGetTimestampNs(), fileId);
  if (written <= 0 || written >= sizeof(pStore->fileName)) return TSDB_CODE_INVALID_PARA;
  pStore->pFile = taosOpenFile(pStore->fileName, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pStore->pFile == NULL) {
    pStore->fileName[0] = '\0';
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static SFileBlockInfo allocateScopedCacheFileBlock(SScopedCacheStore* pStore, int32_t size) {
  for (int32_t i = 0; i < taosArrayGetSize(pStore->pFreeFileBlocks); ++i) {
    SFileBlockInfo* pFree = taosArrayGet(pStore->pFreeFileBlocks, i);
    if (pFree == NULL || pFree->size < size) continue;
    SFileBlockInfo result = {.offset = pFree->offset, .size = size};
    pFree->offset += size;
    pFree->size -= size;
    if (pFree->size == 0) (void)taosArrayRemove(pStore->pFreeFileBlocks, i);
    return result;
  }
  SFileBlockInfo result = {.offset = pStore->fileSize, .size = size};
  pStore->fileSize += size;
  return result;
}

static int32_t spillScopedCacheBlock(SScopedCacheStore* pStore, SScopedCacheBlock* pBlock) {
  if (pBlock->pBlock == NULL) return TSDB_CODE_SUCCESS;
  int32_t bufferSize = blockGetEncodeSize(pBlock->pBlock);
  if (bufferSize <= 0) return TSDB_CODE_STREAM_INTERNAL_ERROR;
  char* pBuffer = taosMemoryMalloc(bufferSize);
  if (pBuffer == NULL) return terrno;
  int32_t dataLen = blockEncode(pBlock->pBlock, pBuffer, bufferSize, blockDataGetNumOfCols(pBlock->pBlock));
  if (dataLen <= 0) {
    taosMemoryFree(pBuffer);
    return dataLen < 0 ? dataLen : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t code = ensureScopedCacheFile(pStore);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuffer);
    return code;
  }
  SFileBlockInfo fileBlock = allocateScopedCacheFileBlock(pStore, dataLen);
  int64_t        written = taosPWriteFile(pStore->pFile, pBuffer, dataLen, fileBlock.offset);
  taosMemoryFree(pBuffer);
  if (written != dataLen) {
    recycleScopedCacheFileRange(pStore, &fileBlock);
    return written < 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  pBlock->fileBlock = fileBlock;
  pBlock->fileDataLen = dataLen;
  blockDataDestroy(pBlock->pBlock);
  pBlock->pBlock = NULL;
  (void)atomic_sub_fetch_64(&g_pDataSinkManager.usedMemSize, pBlock->payloadMemSize);
  pBlock->payloadMemSize = 0;
  ++pStore->fileBlockCount;
  return TSDB_CODE_SUCCESS;
}

static int32_t spillScopedCacheStore(SScopedCacheStore* pStore) {
  int32_t code = taosThreadMutexLock(&pStore->lock);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < STREAM_SCOPED_CACHE_BUCKETS; ++i) {
    for (SScopedCacheEntry* pEntry = pStore->scopeBuckets[i]; code == TSDB_CODE_SUCCESS && pEntry != NULL;
         pEntry = pEntry->pNextScope) {
      for (SScopedCacheBlock* pBlock = pEntry->pBlocks; code == TSDB_CODE_SUCCESS && pBlock != NULL;
           pBlock = pBlock->pNext) {
        code = spillScopedCacheBlock(pStore, pBlock);
      }
    }
  }
  taosThreadMutexUnlock(&pStore->lock);
  return code;
}

static int32_t restoreScopedCacheSchema(const SScopedCacheEntry* pEntry, SSDataBlock* pBlock) {
  if (taosArrayGetSize(pEntry->pSchema) != taosArrayGetSize(pBlock->pDataBlock)) return TSDB_CODE_INVALID_MSG;
  for (int32_t i = 0; i < taosArrayGetSize(pEntry->pSchema); ++i) {
    const SColumnInfo* pInfo = taosArrayGet(pEntry->pSchema, i);
    SColumnInfoData*   pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pInfo == NULL || pCol == NULL || pInfo->type != pCol->info.type || pInfo->bytes != pCol->info.bytes) {
      return TSDB_CODE_INVALID_MSG;
    }
    pCol->info = *pInfo;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t loadScopedCacheBlock(SScopedCacheStore* pStore, const SScopedCacheEntry* pEntry,
                                    const SScopedCacheBlock* pStored, SSDataBlock** ppBlock) {
  if (pStored->pBlock != NULL) return createOneDataBlock(pStored->pBlock, true, ppBlock);
  char* pBuffer = taosMemoryMalloc(pStored->fileDataLen);
  if (pBuffer == NULL) return terrno;
  int64_t readLen = taosPReadFile(pStore->pFile, pBuffer, pStored->fileDataLen, pStored->fileBlock.offset);
  if (readLen != pStored->fileDataLen) {
    taosMemoryFree(pBuffer);
    return readLen < 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(*pBlock));
  if (pBlock == NULL) {
    taosMemoryFree(pBuffer);
    return terrno;
  }
  const char* pEnd = NULL;
  int32_t     code = blockDecode(pBlock, pBuffer, &pEnd);
  if (code == TSDB_CODE_SUCCESS && pEnd != pBuffer + pStored->fileDataLen) code = TSDB_CODE_INVALID_MSG;
  if (code == TSDB_CODE_SUCCESS) code = restoreScopedCacheSchema(pEntry, pBlock);
  if (code == TSDB_CODE_SUCCESS) restoreScopedCacheBlockInfo(&pStored->info, pBlock);
  taosMemoryFree(pBuffer);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

static int32_t mergeScopedCacheBlockOnFile(SScopedCacheStore* pStore, const SScopedCacheEntry* pEntry,
                                           SScopedCacheBlock* pStored, const SScopedCacheBlock* pNewBlock,
                                           bool* pMerged) {
  *pMerged = false;
  SSDataBlock* pBlock = NULL;
  int32_t      code = loadScopedCacheBlock(pStore, pEntry, pStored, &pBlock);
  if (code == TSDB_CODE_SUCCESS) code = blockDataMerge(pBlock, pNewBlock->pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }
  int32_t bufferSize = blockGetEncodeSize(pBlock);
  char*   pBuffer = bufferSize > 0 ? taosMemoryMalloc(bufferSize) : NULL;
  if (pBuffer == NULL) {
    blockDataDestroy(pBlock);
    return bufferSize > 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t dataLen = blockEncode(pBlock, pBuffer, bufferSize, blockDataGetNumOfCols(pBlock));
  if (dataLen <= 0) {
    taosMemoryFree(pBuffer);
    blockDataDestroy(pBlock);
    return dataLen < 0 ? dataLen : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  SFileBlockInfo replacement = allocateScopedCacheFileBlock(pStore, dataLen);
  int64_t        written = taosPWriteFile(pStore->pFile, pBuffer, dataLen, replacement.offset);
  taosMemoryFree(pBuffer);
  blockDataDestroy(pBlock);
  if (written != dataLen) {
    recycleScopedCacheFileRange(pStore, &replacement);
    return written < 0 ? terrno : TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  recycleScopedCacheFileRange(pStore, &pStored->fileBlock);
  pStored->fileBlock = replacement;
  pStored->fileDataLen = dataLen;
  pStored->rows += pNewBlock->rows;
  pStored->minTs = TMIN(pStored->minTs, pNewBlock->minTs);
  pStored->maxTs = TMAX(pStored->maxTs, pNewBlock->maxTs);
  *pMerged = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t copyScopedCacheBlockRange(const SSDataBlock* pSource, int32_t maxRows, int32_t tsSlotId, TSKEY start,
                                         TSKEY end, SSDataBlock** ppResult) {
  const SColumnInfoData* pTsCol = taosArrayGet(pSource->pDataBlock, tsSlotId);
  if (pTsCol == NULL) return TSDB_CODE_INVALID_PARA;
  maxRows = TMIN(maxRows, pSource->info.rows);
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < maxRows;) {
    while (i < maxRows && (colDataIsNull_s(pTsCol, i) || *(const TSKEY*)colDataGetData(pTsCol, i) < start ||
                           *(const TSKEY*)colDataGetData(pTsCol, i) > end)) {
      ++i;
    }
    int32_t first = i;
    while (i < maxRows && !colDataIsNull_s(pTsCol, i) && *(const TSKEY*)colDataGetData(pTsCol, i) >= start &&
           *(const TSKEY*)colDataGetData(pTsCol, i) <= end) {
      ++i;
    }
    if (first == i) continue;
    SSDataBlock* pRange = NULL;
    code = blockDataExtractBlock((SSDataBlock*)pSource, first, i - first, &pRange);
    if (code != TSDB_CODE_SUCCESS) break;
    if (*ppResult == NULL) {
      *ppResult = pRange;
    } else {
      code = blockDataMerge(*ppResult, pRange);
      blockDataDestroy(pRange);
      if (code != TSDB_CODE_SUCCESS) break;
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(*ppResult);
    *ppResult = NULL;
  }
  return code;
}

int32_t beginStreamDataCacheWriteBatch(void* pCache, SStreamDataCacheWriteBatch** ppBatch) {
  if (pCache == NULL || ppBatch == NULL || *ppBatch != NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SStreamDataCacheWriteBatch* pBatch = taosMemoryCalloc(1, sizeof(SStreamDataCacheWriteBatch));
  if (pBatch == NULL) {
    return terrno;
  }
  pBatch->pScopes = taosArrayInit(1, sizeof(SStagedScopedCacheScope*));
  pBatch->pScopeIndex = taosHashInit(16, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pBatch->pScopes == NULL || pBatch->pScopeIndex == NULL) {
    taosArrayDestroy(pBatch->pScopes);
    taosHashCleanup(pBatch->pScopeIndex);
    taosMemoryFree(pBatch);
    return terrno;
  }
  pBatch->pCache = pCache;
  *ppBatch = pBatch;
  return TSDB_CODE_SUCCESS;
}

static int32_t getOrCreateStagedScopedCacheScope(SStreamDataCacheWriteBatch* pBatch, int64_t gid, void* pKey,
                                                 int32_t keyLen, int32_t tsSlotId, const SSDataBlock* pSchemaBlock,
                                                 SStagedScopedCacheScope** ppScope) {
  SStagedScopedCacheScope** ppExisting = taosHashGet(pBatch->pScopeIndex, pKey, keyLen);
  if (ppExisting != NULL) {
    taosMemoryFree(pKey);
    *ppScope = *ppExisting;
    return TSDB_CODE_SUCCESS;
  }

  SStagedScopedCacheScope* pScope = taosMemoryCalloc(1, sizeof(*pScope));
  if (pScope == NULL) {
    taosMemoryFree(pKey);
    return terrno;
  }
  int32_t code = createStagedScopedCacheEntry(gid, pKey, keyLen, tsSlotId, pSchemaBlock, &pScope->pEntry);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pKey);
    taosMemoryFree(pScope);
    return code;
  }
  pScope->pSourceRows = taosHashInit(16, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pScope->pSourceRows == NULL) {
    SStagedScopedCacheScope* pCleanup = pScope;
    destroyStagedScopedCacheScope(&pCleanup);
    return terrno;
  }
  code = taosHashPut(pBatch->pScopeIndex, pScope->pEntry->pKey, pScope->pEntry->keyLen, &pScope, sizeof(pScope));
  if (code != TSDB_CODE_SUCCESS) {
    SStagedScopedCacheScope* pCleanup = pScope;
    destroyStagedScopedCacheScope(&pCleanup);
    return code;
  }
  if (taosArrayPush(pBatch->pScopes, &pScope) == NULL) {
    int32_t rollbackCode = taosHashRemove(pBatch->pScopeIndex, pScope->pEntry->pKey, pScope->pEntry->keyLen);
    if (rollbackCode != TSDB_CODE_SUCCESS) {
      stError("failed to roll back staged scope index since %s", tstrerror(rollbackCode));
    }
    SStagedScopedCacheScope* pCleanup = pScope;
    destroyStagedScopedCacheScope(&pCleanup);
    return terrno;
  }
  *ppScope = pScope;
  return TSDB_CODE_SUCCESS;
}

static SStagedScopedCacheRowKey makeStagedScopedCacheRowKey(const SSDataBlock* pSourceBlock, int32_t sourceRowIndex) {
  SStagedScopedCacheRowKey key = {0};
  key.pSourceBlock = pSourceBlock;
  key.sourceRowIndex = sourceRowIndex;
  return key;
}

static int32_t appendStagedScopedCacheBlock(SStagedScopedCacheScope* pScope, TSKEY ts, SSDataBlock* pBlock) {
  SScopedCacheEntry* pEntry = pScope->pEntry;
  SScopedCacheBlock* pLast = pEntry->pLastBlock;
  if (pLast != NULL && pLast->rows + pBlock->info.rows <= STREAM_SCOPED_CACHE_BLOCK_ROWS &&
      scopedCacheBlockInfoEquals(&pLast->info, &pBlock->info)) {
    int32_t code = blockDataMerge(pLast->pBlock, pBlock);
    if (code != TSDB_CODE_SUCCESS) return code;
    pLast->rows += pBlock->info.rows;
    pLast->minTs = TMIN(pLast->minTs, ts);
    pLast->maxTs = TMAX(pLast->maxTs, ts);
    pLast->payloadMemSize = scopedCacheBlockPayloadSize(pLast->pBlock);
    blockDataDestroy(pBlock);
    return TSDB_CODE_SUCCESS;
  }

  SScopedCacheBlock* pScopedBlock = taosMemoryCalloc(1, sizeof(*pScopedBlock));
  if (pScopedBlock == NULL) {
    return terrno;
  }
  pScopedBlock->minTs = ts;
  pScopedBlock->maxTs = ts;
  pScopedBlock->rows = pBlock->info.rows;
  pScopedBlock->info = pBlock->info;
  pScopedBlock->info.pks[0].pData = NULL;
  pScopedBlock->info.pks[1].pData = NULL;
  pScopedBlock->pBlock = pBlock;
  pScopedBlock->payloadMemSize = scopedCacheBlockPayloadSize(pBlock);
  *pEntry->ppBlockTail = pScopedBlock;
  pEntry->ppBlockTail = &pScopedBlock->pNext;
  pEntry->pLastBlock = pScopedBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t stageStreamDataCacheRowScoped(SStreamDataCacheWriteBatch* pBatch, const SStreamCacheScope* pScope,
                                      const SSDataBlock* pBlock, int32_t rowIndex) {
  if (pBatch == NULL || pScope == NULL || pBlock == NULL || rowIndex < 0 || rowIndex >= pBlock->info.rows) {
    return TSDB_CODE_INVALID_PARA;
  }

  void*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t code = buildScopedCacheKey(pScope, &pKey, &keyLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStagedScopedCacheScope* pStaged = NULL;
  code =
      getOrCreateStagedScopedCacheScope(pBatch, pScope->gid, pKey, keyLen, pBatch->pCache->tsSlotId, pBlock, &pStaged);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStagedScopedCacheRowKey rowKey = makeStagedScopedCacheRowKey(pBlock, rowIndex);
  if (taosHashGet(pStaged->pSourceRows, &rowKey, sizeof(rowKey)) != NULL) return TSDB_CODE_SUCCESS;

  SSDataBlock* pExtracted = NULL;
  code = blockDataExtractBlock((SSDataBlock*)pBlock, rowIndex, 1, &pExtracted);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pBatch->pCache->tsSlotId);
  if (pTsCol == NULL || colDataIsNull_s(pTsCol, rowIndex)) {
    blockDataDestroy(pExtracted);
    return TSDB_CODE_INVALID_PARA;
  }
  TSKEY ts = *(TSKEY*)colDataGetData(pTsCol, rowIndex);
  bool  seen = true;
  code = taosHashPut(pStaged->pSourceRows, &rowKey, sizeof(rowKey), &seen, sizeof(seen));
  if (code == TSDB_CODE_SUCCESS) code = appendStagedScopedCacheBlock(pStaged, ts, pExtracted);
  if (code != TSDB_CODE_SUCCESS) {
    int32_t rollbackCode = taosHashRemove(pStaged->pSourceRows, &rowKey, sizeof(rowKey));
    if (rollbackCode != TSDB_CODE_SUCCESS) {
      stError("failed to roll back staged source row since %s", tstrerror(rollbackCode));
    }
    blockDataDestroy(pExtracted);
  }
  return code;
}

int32_t stageStreamDataCacheProjectedRowScoped(SStreamDataCacheWriteBatch* pBatch, const SStreamCacheScope* pScope,
                                               const SSDataBlock* pSourceBlock, int32_t sourceRowIndex,
                                               const SSDataBlock* pPayloadTemplate, const SArray* pProjection) {
  if (pBatch == NULL || pScope == NULL || pSourceBlock == NULL || pPayloadTemplate == NULL || pProjection == NULL ||
      sourceRowIndex < 0 || sourceRowIndex >= pSourceBlock->info.rows ||
      pProjection->elemSize != sizeof(SStreamDataCacheColumnProjection)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t numSourceCols = taosArrayGetSize(pSourceBlock->pDataBlock);
  int32_t numTargetCols = taosArrayGetSize(pPayloadTemplate->pDataBlock);
  int32_t targetTsSlotId = -1;
  if (taosArrayGetSize(pProjection) != numTargetCols) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t i = 0; i < numTargetCols; ++i) {
    const SStreamDataCacheColumnProjection* pItem = taosArrayGet(pProjection, i);
    if (pItem == NULL || pItem->sourceSlotId < 0 || pItem->sourceSlotId >= numSourceCols || pItem->targetSlotId < 0 ||
        pItem->targetSlotId >= numTargetCols) {
      return TSDB_CODE_INVALID_PARA;
    }
    const SColumnInfoData* pSourceCol = taosArrayGet(pSourceBlock->pDataBlock, pItem->sourceSlotId);
    const SColumnInfoData* pTargetCol = taosArrayGet(pPayloadTemplate->pDataBlock, pItem->targetSlotId);
    if (pSourceCol == NULL || pTargetCol == NULL || pSourceCol->info.type != pTargetCol->info.type ||
        pSourceCol->info.bytes != pTargetCol->info.bytes ||
        (IS_DECIMAL_TYPE(pSourceCol->info.type) && (pSourceCol->info.precision != pTargetCol->info.precision ||
                                                    pSourceCol->info.scale != pTargetCol->info.scale))) {
      return TSDB_CODE_INVALID_PARA;
    }
    int32_t targetCount = 0;
    for (int32_t j = 0; j < numTargetCols; ++j) {
      const SStreamDataCacheColumnProjection* pCandidate = taosArrayGet(pProjection, j);
      if (pCandidate != NULL && pCandidate->targetSlotId == pItem->targetSlotId) {
        ++targetCount;
      }
    }
    if (targetCount != 1) {
      return TSDB_CODE_INVALID_PARA;
    }
    if (pItem->sourceSlotId == pBatch->pCache->tsSlotId) targetTsSlotId = pItem->targetSlotId;
  }
  if (targetTsSlotId < 0) return TSDB_CODE_INVALID_PARA;

  SColumnInfoData* pTsCol = taosArrayGet(pSourceBlock->pDataBlock, pBatch->pCache->tsSlotId);
  if (pTsCol == NULL || colDataIsNull_s(pTsCol, sourceRowIndex)) {
    return TSDB_CODE_INVALID_PARA;
  }

  void*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t code = buildScopedCacheKey(pScope, &pKey, &keyLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStagedScopedCacheScope* pStaged = NULL;
  code =
      getOrCreateStagedScopedCacheScope(pBatch, pScope->gid, pKey, keyLen, targetTsSlotId, pPayloadTemplate, &pStaged);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SStagedScopedCacheRowKey rowKey = makeStagedScopedCacheRowKey(pSourceBlock, sourceRowIndex);
  if (taosHashGet(pStaged->pSourceRows, &rowKey, sizeof(rowKey)) != NULL) return TSDB_CODE_SUCCESS;

  SSDataBlock* pProjected = NULL;
  code = createOneDataBlock(pPayloadTemplate, false, &pProjected);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = blockDataEnsureCapacity(pProjected, 1);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pProjected);
    return code;
  }
  for (int32_t i = 0; i < numTargetCols; ++i) {
    const SStreamDataCacheColumnProjection* pItem = taosArrayGet(pProjection, i);
    const SColumnInfoData*                  pSourceCol = taosArrayGet(pSourceBlock->pDataBlock, pItem->sourceSlotId);
    SColumnInfoData*                        pTargetCol = taosArrayGet(pProjected->pDataBlock, pItem->targetSlotId);
    bool        isNull = pSourceBlock->pBlockAgg == NULL ? colDataIsNull_s(pSourceCol, sourceRowIndex)
                                                         : colDataIsNull(pSourceCol, pSourceBlock->info.rows, sourceRowIndex,
                                                                         &pSourceBlock->pBlockAgg[pItem->sourceSlotId]);
    const char* pData = isNull ? NULL : colDataGetData(pSourceCol, sourceRowIndex);
    code = colDataSetVal(pTargetCol, 0, pData, isNull);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pProjected);
      return code;
    }
  }
  pProjected->info.rows = 1;
  pProjected->info.id = pSourceBlock->info.id;
  pProjected->info.window = pSourceBlock->info.window;
  pProjected->info.dataLoad = pSourceBlock->info.dataLoad;
  pProjected->info.scanFlag = pSourceBlock->info.scanFlag;
  pProjected->info.version = pSourceBlock->info.version;
  pProjected->info.childId = pSourceBlock->info.childId;
  pProjected->info.calWin = pSourceBlock->info.calWin;
  pProjected->info.watermark = pSourceBlock->info.watermark;
  tstrncpy(pProjected->info.parTbName, pSourceBlock->info.parTbName, sizeof(pProjected->info.parTbName));
  TSKEY ts = *(TSKEY*)colDataGetData(pTsCol, sourceRowIndex);
  bool  seen = true;
  code = taosHashPut(pStaged->pSourceRows, &rowKey, sizeof(rowKey), &seen, sizeof(seen));
  if (code == TSDB_CODE_SUCCESS) code = appendStagedScopedCacheBlock(pStaged, ts, pProjected);
  if (code != TSDB_CODE_SUCCESS) {
    int32_t rollbackCode = taosHashRemove(pStaged->pSourceRows, &rowKey, sizeof(rowKey));
    if (rollbackCode != TSDB_CODE_SUCCESS) {
      stError("failed to roll back staged source row since %s", tstrerror(rollbackCode));
    }
    blockDataDestroy(pProjected);
  }
  return code;
}

static void commitStagedScopedCacheBlock(SScopedCacheStore* pStore, SScopedCacheEntry* pEntry,
                                         SScopedCacheBlock* pNewBlock) {
  SScopedCacheBlock* pLast = pEntry->pLastBlock;
  if (pLast != NULL && pLast->rows + pNewBlock->rows <= STREAM_SCOPED_CACHE_BLOCK_ROWS &&
      scopedCacheBlockInfoEquals(&pLast->info, &pNewBlock->info)) {
    if (pLast->pBlock != NULL) {
      SSDataBlock* pMerged = NULL;
      int32_t      code = createOneDataBlock(pLast->pBlock, true, &pMerged);
      if (code == TSDB_CODE_SUCCESS) code = blockDataMerge(pMerged, pNewBlock->pBlock);
      if (code == TSDB_CODE_SUCCESS) {
        int64_t oldPayloadSize = pLast->payloadMemSize;
        blockDataDestroy(pLast->pBlock);
        pLast->pBlock = pMerged;
        pLast->payloadMemSize = scopedCacheBlockPayloadSize(pMerged);
        pLast->rows += pNewBlock->rows;
        pLast->minTs = TMIN(pLast->minTs, pNewBlock->minTs);
        pLast->maxTs = TMAX(pLast->maxTs, pNewBlock->maxTs);
        (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, pLast->payloadMemSize - oldPayloadSize);
        destroyUntrackedScopedCacheBlock(pNewBlock);
        return;
      }
      blockDataDestroy(pMerged);
    } else {
      bool    merged = false;
      int32_t code = mergeScopedCacheBlockOnFile(pStore, pEntry, pLast, pNewBlock, &merged);
      if (merged) {
        destroyUntrackedScopedCacheBlock(pNewBlock);
        return;
      }
      if (code != TSDB_CODE_SUCCESS) {
        stError("failed to merge scoped cache file block, err: %s", tstrerror(code));
      }
    }
  }
  pNewBlock->pNext = NULL;
  *pEntry->ppBlockTail = pNewBlock;
  pEntry->ppBlockTail = &pNewBlock->pNext;
  pEntry->pLastBlock = pNewBlock;
  (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, sizeof(*pNewBlock) + pNewBlock->payloadMemSize);
}

void commitStreamDataCacheWriteBatch(SStreamDataCacheWriteBatch** ppBatch) {
  if (ppBatch == NULL || *ppBatch == NULL) {
    return;
  }
  SStreamDataCacheWriteBatch* pBatch = *ppBatch;
  SScopedCacheStore*          pStore = pBatch->pCache->pScopedStore;
  taosThreadMutexLock(&pStore->lock);
  for (int32_t i = 0; i < taosArrayGetSize(pBatch->pScopes); ++i) {
    SStagedScopedCacheScope** ppStaged = taosArrayGet(pBatch->pScopes, i);
    SStagedScopedCacheScope*  pStaged = *ppStaged;
    SScopedCacheEntry*        pStagedEntry = pStaged->pEntry;
    SScopedCacheEntry*        pEntry = findScopedCacheEntry(pStore, pStagedEntry->pKey, pStagedEntry->keyLen);
    if (pEntry == NULL) {
      pEntry = pStagedEntry;
      pEntry->entryId = ++pStore->nextEntryId;
      uint32_t scopeBucket = scopedCacheHash(pEntry->pKey, pEntry->keyLen) % STREAM_SCOPED_CACHE_BUCKETS;
      uint32_t groupBucket = scopedGroupHash(pEntry->gid);
      *pStore->scopeTails[scopeBucket] = pEntry;
      pStore->scopeTails[scopeBucket] = &pEntry->pNextScope;
      *pStore->groupTails[groupBucket] = pEntry;
      pStore->groupTails[groupBucket] = &pEntry->pNextGroup;
      (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, pEntry->metadataMemSize);
      for (SScopedCacheBlock* pBlock = pEntry->pBlocks; pBlock != NULL; pBlock = pBlock->pNext) {
        (void)atomic_add_fetch_64(&g_pDataSinkManager.usedMemSize, sizeof(*pBlock) + pBlock->payloadMemSize);
      }
      pStaged->pEntry = NULL;
      continue;
    }

    SScopedCacheBlock* pNewBlock = pStagedEntry->pBlocks;
    pStagedEntry->pBlocks = NULL;
    pStagedEntry->pLastBlock = NULL;
    pStagedEntry->ppBlockTail = &pStagedEntry->pBlocks;
    destroyUntrackedScopedCacheEntry(pStagedEntry);
    pStaged->pEntry = NULL;
    while (pNewBlock != NULL) {
      SScopedCacheBlock* pNext = pNewBlock->pNext;
      commitStagedScopedCacheBlock(pStore, pEntry, pNewBlock);
      pNewBlock = pNext;
    }
  }
  taosThreadMutexUnlock(&pStore->lock);
  taosArrayDestroyEx(pBatch->pScopes, destroyStagedScopedCacheScope);
  taosHashCleanup(pBatch->pScopeIndex);
  if (tsStreamBufferSizeBytes > DS_MEM_SIZE_RESERVED && !hasEnoughMemSize()) {
    int32_t code = spillScopedCacheStore(pStore);
    if (code != TSDB_CODE_SUCCESS) stError("failed to spill scoped cache, err: %s", tstrerror(code));
  }
  taosMemoryFree(pBatch);
  *ppBatch = NULL;
}

void abortStreamDataCacheWriteBatch(SStreamDataCacheWriteBatch** ppBatch) {
  if (ppBatch == NULL || *ppBatch == NULL) {
    return;
  }
  SStreamDataCacheWriteBatch* pBatch = *ppBatch;
  taosArrayDestroyEx(pBatch->pScopes, destroyStagedScopedCacheScope);
  taosHashCleanup(pBatch->pScopeIndex);
  taosMemoryFree(pBatch);
  *ppBatch = NULL;
}

int32_t putStreamDataCacheScoped(void* pCache, const SStreamCacheScope* pScope, TSKEY wstart, TSKEY wend,
                                 SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex) {
  if (pScope == NULL || pBlock == NULL || wstart > wend || startIndex < 0 || endIndex < startIndex ||
      endIndex >= pBlock->info.rows) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (taosArrayGetSize(pScope->lineage.pScopes) == 0) {
    return putStreamDataCache(pCache, pScope->gid, wstart, wend, pBlock, startIndex, endIndex);
  }
  SStreamDataCacheWriteBatch* pBatch = NULL;
  int32_t                     code = beginStreamDataCacheWriteBatch(pCache, &pBatch);
  for (int32_t i = startIndex; code == TSDB_CODE_SUCCESS && i <= endIndex; ++i) {
    code = stageStreamDataCacheRowScoped(pBatch, pScope, pBlock, i);
  }
  if (code != TSDB_CODE_SUCCESS) {
    abortStreamDataCacheWriteBatch(&pBatch);
    return code;
  }
  commitStreamDataCacheWriteBatch(&pBatch);
  return TSDB_CODE_SUCCESS;
}

int32_t getStreamDataCacheScoped(void* pCache, const SStreamCacheScope* pScope, TSKEY start, TSKEY end, void** ppIter) {
  if (pCache == NULL || pScope == NULL || ppIter == NULL || start > end) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (taosArrayGetSize(pScope->lineage.pScopes) == 0) {
    return getStreamDataCache(pCache, pScope->gid, start, end, ppIter);
  }
  *ppIter = NULL;
  void*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t code = buildScopedCacheKey(pScope, &pKey, &keyLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SResultIter* pResult = NULL;
  code = createDataResult((void**)&pResult);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pKey);
    return code;
  }
  SScopedCacheResultState* pScoped = taosMemoryCalloc(1, sizeof(*pScoped));
  if (pScoped == NULL) {
    taosMemoryFree(pKey);
    releaseDataResult((void**)&pResult);
    return terrno;
  }
  pScoped->pCache = pCache;
  pScoped->pKey = pKey;
  pScoped->keyLen = keyLen;
  pResult->groupData = pScoped;
  pResult->cleanMode = getCleanModeFromDSMgr(pCache);
  pResult->dataPos = DATA_SINK_ALL_TMP;
  pResult->groupId = pScope->gid;
  pResult->reqStartTime = start;
  pResult->reqEndTime = end;
  pResult->scopedResult = true;

  SScopedCacheStore* pStore = ((SStreamDataCacheBase*)pCache)->pScopedStore;
  uint32_t           bucket = scopedCacheHash(pKey, keyLen) % STREAM_SCOPED_CACHE_BUCKETS;
  bool               found = false;
  taosThreadMutexLock(&pStore->lock);
  for (SScopedCacheEntry* pEntry = pStore->scopeBuckets[bucket]; pEntry != NULL; pEntry = pEntry->pNextScope) {
    if (!scopedKeyEquals(pEntry, pKey, keyLen)) continue;
    for (SScopedCacheBlock* pStored = pEntry->pBlocks; pStored != NULL; pStored = pStored->pNext) {
      if (pStored->maxTs < start || pStored->minTs > end) continue;
      found = true;
      pScoped->pNextBlock = pStored;
      break;
    }
    pScoped->entryId = pEntry->entryId;
    pScoped->pSnapshotLastBlock = pEntry->pLastBlock;
    pScoped->snapshotLastRows = pEntry->pLastBlock == NULL ? 0 : pEntry->pLastBlock->rows;
    break;
  }
  taosThreadMutexUnlock(&pStore->lock);
  if (!found) {
    releaseDataResult((void**)&pResult);
    return TSDB_CODE_SUCCESS;
  }
  *ppIter = pResult;
  return TSDB_CODE_SUCCESS;
}

int32_t cleanStreamDataCacheScope(void* pCache, const SStreamCacheScope* pScope) {
  if (pCache == NULL || pScope == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (taosArrayGetSize(pScope->lineage.pScopes) == 0) {
    return cleanStreamDataCache(pCache, pScope->gid);
  }
  void*   pKey = NULL;
  int32_t keyLen = 0;
  int32_t code = buildScopedCacheKey(pScope, &pKey, &keyLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SScopedCacheStore* pStore = ((SStreamDataCacheBase*)pCache)->pScopedStore;
  uint32_t           bucket = scopedCacheHash(pKey, keyLen) % STREAM_SCOPED_CACHE_BUCKETS;
  taosThreadMutexLock(&pStore->lock);
  SScopedCacheEntry** ppEntry = &pStore->scopeBuckets[bucket];
  while (*ppEntry != NULL) {
    SScopedCacheEntry* pEntry = *ppEntry;
    if (scopedKeyEquals(pEntry, pKey, keyLen)) {
      if (pEntry->pNextScope == NULL) {
        pStore->scopeTails[bucket] = ppEntry;
      }
      *ppEntry = pEntry->pNextScope;
      unlinkScopedEntryFromGroup(pStore, pEntry);
      destroyTrackedScopedCacheEntry(pStore, pEntry);
    } else {
      ppEntry = &pEntry->pNextScope;
    }
  }
  taosThreadMutexUnlock(&pStore->lock);
  taosMemoryFree(pKey);
  return TSDB_CODE_SUCCESS;
}

int32_t cleanStreamDataCacheGroup(void* pCache, int64_t gid) {
  if (pCache == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SScopedCacheStore* pStore = ((SStreamDataCacheBase*)pCache)->pScopedStore;
  uint32_t           groupBucket = scopedGroupHash(gid);
  taosThreadMutexLock(&pStore->lock);
  SScopedCacheEntry** ppGroupEntry = &pStore->groupBuckets[groupBucket];
  while (*ppGroupEntry != NULL) {
    SScopedCacheEntry* pEntry = *ppGroupEntry;
    if (pEntry->gid != gid) {
      ppGroupEntry = &pEntry->pNextGroup;
      continue;
    }
    if (pEntry->pNextGroup == NULL) {
      pStore->groupTails[groupBucket] = ppGroupEntry;
    }
    *ppGroupEntry = pEntry->pNextGroup;
    uint32_t            scopeBucket = scopedCacheHash(pEntry->pKey, pEntry->keyLen) % STREAM_SCOPED_CACHE_BUCKETS;
    SScopedCacheEntry** ppScopeEntry = &pStore->scopeBuckets[scopeBucket];
    while (*ppScopeEntry != pEntry) {
      ppScopeEntry = &(*ppScopeEntry)->pNextScope;
    }
    if (pEntry->pNextScope == NULL) {
      pStore->scopeTails[scopeBucket] = ppScopeEntry;
    }
    *ppScopeEntry = pEntry->pNextScope;
    destroyTrackedScopedCacheEntry(pStore, pEntry);
  }
  taosThreadMutexUnlock(&pStore->lock);
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_EXPIRED) {
    return cleanStreamDataCache(pCache, gid);
  }
  return TSDB_CODE_SUCCESS;
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
  (void)changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_READING);
  if (code != TSDB_CODE_SUCCESS) {
    releaseDataResultAndResetMgrStatus((void**)&pResultIter);
    *pIter = NULL;
    stError("failed to get sliding data cache, err: %s, lineno:%d", terrMsg, lino);
  }
  return code;
}

static int32_t cleanSlidingDataCache(void* pCache, int64_t groupId) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSlidingTaskDSMgr* pStreamTaskMgr = (SSlidingTaskDSMgr*)pCache;

  stDebug("[clean data cache] groupID:%" PRId64 " STREAMID:%" PRIx64, groupId, pStreamTaskMgr->streamId);

  SSlidingGrpMgr** ppExistGrpMgr =
      (SSlidingGrpMgr**)taosHashGet(pStreamTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  if (ppExistGrpMgr == NULL) {
    stDebug("[clean data cache] nogroup groupID:%" PRId64 "STREAMID:%" PRIx64, groupId, pStreamTaskMgr->streamId);
    return TSDB_CODE_SUCCESS;
  }
  SSlidingGrpMgr* pExistGrpMgr = *ppExistGrpMgr;
  bool            canClean = changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_WRITING);
  if (!canClean) {
    stError("failed to change group data sink manager status when clean data, status: %d", pExistGrpMgr->status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  cleanSlidingGrpMgr(pExistGrpMgr);

  (void)changeMgrStatus(&pExistGrpMgr->status, GRP_DATA_IDLE);
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
  if (start > end) {
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

int32_t cleanStreamDataCache(void* pCache, int64_t groupId) {
  if (!isManagerReady()) {
    stError("DataSinkManager is not ready");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_IMMEDIATE) {
    stError("cleanStreamDataCache not support immediate mode");
    return TSDB_CODE_STREAM_INTERNAL_ERROR;

  } else if (getCleanModeFromDSMgr(pCache) == DATA_CLEAN_EXPIRED) {
    return cleanSlidingDataCache((SSlidingTaskDSMgr*)pCache, groupId);
  }
  return TSDB_CODE_SUCCESS;
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
  if (pResult->scopedResult && pResult->groupData != NULL) {
    SScopedCacheResultState* pScoped = pResult->groupData;
    taosMemoryFree(pScoped->pKey);
    taosMemoryFree(pScoped);
    pResult->groupData = NULL;
  }
  if (pResult->tmpBlocksInMem) {
    for (int32_t i = 0; i < pResult->tmpBlocksInMem->size; ++i) {
      SSDataBlock** ppBlk = (SSDataBlock**)taosArrayGet(pResult->tmpBlocksInMem, i);
      if (*ppBlk != NULL) {
        if (pResult->scopedResult) {
          blockDataDestroy(*ppBlk);
        } else {
          taosMemoryFree(*ppBlk);
        }
        *ppBlk = NULL;
      }
    }
    taosArrayDestroy(pResult->tmpBlocksInMem);
    pResult->tmpBlocksInMem = NULL;
  }

  if (pResult != NULL) {
    releaseStreamDataCacheLease(&pResult->pLease);
    taosMemoryFree(pResult);
    *pIter = NULL;
  }
}

void releaseDataResultAndResetMgrStatus(void** pIter) {
  if (pIter == NULL || *pIter == NULL) {
    return;
  }
  SResultIter* pResult = (SResultIter*)*pIter;

  if (pResult->scopedResult) {
    releaseDataResult(pIter);
    return;
  }

  if (pResult->cleanMode == DATA_CLEAN_EXPIRED) {
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

static int32_t getNextScopedStreamDataCache(void** pIter, SSDataBlock** ppBlock) {
  SResultIter*             pResult = *pIter;
  SScopedCacheResultState* pScoped = pResult->groupData;
  SScopedCacheStore*       pStore = pScoped->pCache->pScopedStore;
  SSDataBlock*             pRange = NULL;
  int32_t                  code = taosThreadMutexLock(&pStore->lock);
  if (code != TSDB_CODE_SUCCESS) return code;

  SScopedCacheEntry* pEntry = findScopedCacheEntry(pStore, pScoped->pKey, pScoped->keyLen);
  if (pEntry == NULL || pEntry->entryId != pScoped->entryId) {
    pScoped->pNextBlock = NULL;
  }
  while (code == TSDB_CODE_SUCCESS && pScoped->pNextBlock != NULL && pRange == NULL) {
    SScopedCacheBlock* pStored = pScoped->pNextBlock;
    bool               isSnapshotLast = pStored == pScoped->pSnapshotLastBlock;
    pScoped->pNextBlock = isSnapshotLast ? NULL : pStored->pNext;
    if (pStored->maxTs < pResult->reqStartTime || pStored->minTs > pResult->reqEndTime) continue;

    SSDataBlock* pLoaded = NULL;
    code = loadScopedCacheBlock(pStore, pEntry, pStored, &pLoaded);
    if (code == TSDB_CODE_SUCCESS) {
      int32_t maxRows = isSnapshotLast ? pScoped->snapshotLastRows : pStored->rows;
      code = copyScopedCacheBlockRange(pLoaded, maxRows, pEntry->tsSlotId, pResult->reqStartTime, pResult->reqEndTime,
                                       &pRange);
    }
    blockDataDestroy(pLoaded);
  }
  bool finished = pScoped->pNextBlock == NULL;
  taosThreadMutexUnlock(&pStore->lock);
  if (code != TSDB_CODE_SUCCESS || finished) releaseDataResult(pIter);
  if (code == TSDB_CODE_SUCCESS)
    *ppBlock = pRange;
  else
    blockDataDestroy(pRange);
  return code;
}

int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock) {
  if (pIter == NULL || ppBlock == NULL) {
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
  if (pResult->scopedResult) {
    return getNextScopedStreamDataCache(pIter, ppBlock);
  }
  int64_t groupId = pResult->groupId;
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
          stError("getNextStreamDataCache failed, groupId: %" PRId64 " start:%" PRId64 " end:%" PRId64
                  " dataPos: %d, winIndex: %d, tmpBlocksInMem size: %" PRIzu,
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
    taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
    SStreamDataCacheRegistration* pRegistration = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, NULL);
    while (pRegistration != NULL) {
      void* pCache = pRegistration->pCache;
      retireStreamDataCache(&pCache);
      pRegistration = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, pRegistration);
    }
    taosHashCleanup(g_pDataSinkManager.dsStreamTaskList);
    g_pDataSinkManager.dsStreamTaskList = NULL;
    taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  }
  if (g_pDataSinkManager.registrationLockInited) {
    taosThreadMutexDestroy(&g_pDataSinkManager.registrationLock);
    g_pDataSinkManager.registrationLockInited = false;
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

  SArray* pPinnedCaches = taosArrayInit(taosHashGetSize(g_pDataSinkManager.dsStreamTaskList), sizeof(void*));
  if (pPinnedCaches == NULL) {
    return terrno;
  }

  int32_t code = taosThreadMutexLock(&g_pDataSinkManager.registrationLock);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pPinnedCaches);
    return code;
  }
  SStreamDataCacheRegistration* pRegistration = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, NULL);
  while (pRegistration != NULL) {
    SStreamDataCacheBase* pCache = pRegistration->pCache;
    if (pCache != NULL) {
      atomic_add_fetch_32(&pCache->refCount, 1);
      if (taosArrayPush(pPinnedCaches, &pCache) == NULL) {
        releaseStreamDataCacheReference(pCache);
        code = terrno;
        break;
      }
    }
    pRegistration = taosHashIterate(g_pDataSinkManager.dsStreamTaskList, pRegistration);
  }
  if (pRegistration != NULL) {
    taosHashCancelIterate(g_pDataSinkManager.dsStreamTaskList, pRegistration);
  }
  taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);

  for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < taosArrayGetSize(pPinnedCaches); ++i) {
    SStreamDataCacheBase* pCache = *(SStreamDataCacheBase**)taosArrayGet(pPinnedCaches, i);
    code = spillScopedCacheStore(pCache->pScopedStore);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to move scoped task mem cache, lino:%d code: %d err: %s", __LINE__, code, terrMsg);
    } else if (pCache->cleanMode == DATA_CLEAN_EXPIRED) {
      code = moveSlidingTaskMemCache((SSlidingTaskDSMgr*)pCache);
      if (code != TSDB_CODE_SUCCESS) {
        stError("failed to move sliding task mem cache, lino:%d code: %d err: %s", __LINE__, code, terrMsg);
      } else if (hasEnoughMemSize()) {
        break;
      }
    } else if (hasEnoughMemSize()) {
      break;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pPinnedCaches); ++i) {
    SStreamDataCacheBase* pCache = *(SStreamDataCacheBase**)taosArrayGet(pPinnedCaches, i);
    releaseStreamDataCacheReference(pCache);
  }
  taosArrayDestroy(pPinnedCaches);
  return code;
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
  status = atomic_val_compare_exchange_8(&g_slidigGrpMemList.status, DATA_MOVING, DATA_NORMAL);
  if(status != DATA_MOVING) {
    stError("moveMemCache status not changed, expected: %d, actual: %d", DATA_MOVING, status);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

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
