// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.

#include "interpResultStore.h"

#include "execBlockSpool.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "thash.h"

enum {
  INTERP_RESULT_STORE_PAGE_SIZE = 4 * 1024,
};

static const int64_t INTERP_RESULT_STORE_IN_MEM_BYTES =
    10 * 1024 * 1024;

typedef struct SInterpResultGroup {
  SExecBlockSpoolDirectory* pDirectory;
  int64_t                   bytesUsed;
} SInterpResultGroup;

struct SInterpResultStore {
  SHashObj*        pGroups;
  SExecBlockSpool* pSpool;
  uint64_t         readGroupId;
  int32_t          readBlockIndex;
  int32_t          pageSize;
  int64_t          inMemBytes;
  int64_t          bytesUsed;
  bool             readOpen;
  bool             readEof;
};

static int32_t interpResultStoreCreateImpl(
    SInterpResultStore** ppStore, int32_t pageSize,
    int64_t inMemBytes) {
  if (ppStore == NULL || pageSize <= 0 || inMemBytes <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppStore = NULL;

  SInterpResultStore* pStore =
      taosMemoryCalloc(1, sizeof(SInterpResultStore));
  if (pStore == NULL) {
    return terrno;
  }

  pStore->pGroups =
      taosHashInit(16,
                   taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
                   false, HASH_NO_LOCK);
  if (pStore->pGroups == NULL) {
    int32_t code = terrno;
    taosMemoryFree(pStore);
    return code;
  }

  pStore->pageSize = pageSize;
  pStore->inMemBytes = inMemBytes;
  *ppStore = pStore;
  return TSDB_CODE_SUCCESS;
}

int32_t interpResultStoreCreate(SInterpResultStore** ppStore) {
  return interpResultStoreCreateImpl(
      ppStore, INTERP_RESULT_STORE_PAGE_SIZE,
      INTERP_RESULT_STORE_IN_MEM_BYTES);
}

static int32_t interpResultStoreEnsureSpool(
    SInterpResultStore* pStore) {
  if (pStore->pSpool != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  return execBlockSpoolCreate(
      pStore->pageSize, pStore->inMemBytes, "interp-result-store",
      tsTempDir, &pStore->pSpool);
}

static int32_t interpResultStoreAddGroup(
    SInterpResultStore* pStore, uint64_t groupId,
    SExecBlockSpoolDirectory* pDirectory, int64_t blockBytes) {
  SInterpResultGroup group = {
      .pDirectory = pDirectory,
      .bytesUsed = blockBytes,
  };
  return taosHashPut(pStore->pGroups, &groupId, sizeof(groupId),
                     &group, sizeof(group));
}

static int32_t interpResultStoreAppendNewGroup(
    SInterpResultStore* pStore, uint64_t groupId,
    const SSDataBlock* pBlock, int64_t blockBytes) {
  SExecBlockSpoolDirectory* pDirectory = NULL;
  int32_t code = execBlockSpoolDirectoryCreate(&pDirectory);
  if (code == TSDB_CODE_SUCCESS) {
    code = execBlockSpoolAppend(pStore->pSpool, pDirectory, pBlock);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = interpResultStoreAddGroup(
        pStore, groupId, pDirectory, blockBytes);
  }
  if (code != TSDB_CODE_SUCCESS) {
    if (pDirectory != NULL &&
        execBlockSpoolDirectorySize(pDirectory) > 0) {
      int32_t recycleCode =
          execBlockSpoolRecycle(pStore->pSpool, pDirectory);
      if (recycleCode != TSDB_CODE_SUCCESS) {
        code = recycleCode;
      }
    }
    execBlockSpoolDirectoryDestroy(pDirectory);
  }
  return code;
}

int32_t interpResultStoreAppend(SInterpResultStore* pStore,
                                uint64_t groupId,
                                const SSDataBlock* pBlock) {
  if (pStore == NULL || pBlock == NULL || pBlock->info.rows <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pStore->readOpen && pStore->readGroupId == groupId) {
    return TSDB_CODE_INVALID_PARA;
  }

  size_t blockBytes = blockDataGetSize(pBlock);
  if (blockBytes > INT64_MAX ||
      pStore->bytesUsed > INT64_MAX - (int64_t)blockBytes) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t code = interpResultStoreEnsureSpool(pStore);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SInterpResultGroup* pGroup =
      taosHashGet(pStore->pGroups, &groupId, sizeof(groupId));
  if (pGroup == NULL) {
    code = interpResultStoreAppendNewGroup(
        pStore, groupId, pBlock, (int64_t)blockBytes);
  } else {
    code = execBlockSpoolAppend(
        pStore->pSpool, pGroup->pDirectory, pBlock);
    if (code == TSDB_CODE_SUCCESS) {
      pGroup->bytesUsed += (int64_t)blockBytes;
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pStore->bytesUsed += (int64_t)blockBytes;
  return TSDB_CODE_SUCCESS;
}

int32_t interpResultStoreOpenRead(SInterpResultStore* pStore,
                                  uint64_t groupId) {
  if (pStore == NULL || pStore->readOpen) {
    return TSDB_CODE_INVALID_PARA;
  }

  SInterpResultGroup* pGroup =
      taosHashGet(pStore->pGroups, &groupId, sizeof(groupId));
  if (pGroup == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  pStore->readGroupId = groupId;
  pStore->readBlockIndex = 0;
  pStore->readOpen = true;
  pStore->readEof = false;
  return TSDB_CODE_SUCCESS;
}

int32_t interpResultStoreNextBlock(SInterpResultStore* pStore,
                                   SSDataBlock** ppBlock) {
  if (pStore == NULL || ppBlock == NULL || !pStore->readOpen) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppBlock = NULL;
  if (pStore->readEof) {
    return TSDB_CODE_SUCCESS;
  }

  SInterpResultGroup* pGroup =
      taosHashGet(pStore->pGroups, &pStore->readGroupId,
                  sizeof(pStore->readGroupId));
  if (pGroup == NULL || pStore->pSpool == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t blocks =
      execBlockSpoolDirectorySize(pGroup->pDirectory);
  if (pStore->readBlockIndex >= blocks) {
    pStore->readEof = true;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = execBlockSpoolRead(
      pStore->pSpool, pGroup->pDirectory,
      pStore->readBlockIndex, ppBlock);
  if (code == TSDB_CODE_SUCCESS) {
    ++pStore->readBlockIndex;
  }
  return code;
}

int32_t interpResultStoreDropGroup(SInterpResultStore* pStore,
                                   uint64_t groupId) {
  if (pStore == NULL || !pStore->readOpen || !pStore->readEof ||
      pStore->readGroupId != groupId || pStore->pSpool == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SInterpResultGroup* pGroup =
      taosHashGet(pStore->pGroups, &groupId, sizeof(groupId));
  if (pGroup == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SInterpResultGroup group = *pGroup;

  // Recycling can partly succeed, so rollback or retry would recycle some
  // page IDs twice. Commit the logical removal before best-effort recycling.
  int32_t code = taosHashRemove(
      pStore->pGroups, &groupId, sizeof(groupId));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = execBlockSpoolRecycle(pStore->pSpool, group.pDirectory);
  execBlockSpoolDirectoryDestroy(group.pDirectory);
  pStore->bytesUsed -= group.bytesUsed;
  pStore->readGroupId = 0;
  pStore->readBlockIndex = 0;
  pStore->readOpen = false;
  pStore->readEof = false;
  return code;
}

int64_t interpResultStoreBytesUsed(const SInterpResultStore* pStore) {
  return pStore == NULL ? 0 : pStore->bytesUsed;
}

int64_t interpResultStoreResidentPagePayloadBytes(
    const SInterpResultStore* pStore) {
  return pStore == NULL ? 0 :
         execBlockSpoolResidentPagePayloadBytes(pStore->pSpool);
}

int64_t interpResultStoreSpillBytes(
    const SInterpResultStore* pStore) {
  return pStore == NULL ? 0 :
         execBlockSpoolSpillBytes(pStore->pSpool);
}

void interpResultStoreReset(SInterpResultStore* pStore) {
  if (pStore == NULL) {
    return;
  }

  void* pIter = NULL;
  while ((pIter = taosHashIterate(pStore->pGroups, pIter)) != NULL) {
    SInterpResultGroup* pGroup = pIter;
    execBlockSpoolDirectoryDestroy(pGroup->pDirectory);
  }
  taosHashClear(pStore->pGroups);
  execBlockSpoolDestroy(pStore->pSpool);
  pStore->pSpool = NULL;
  pStore->readGroupId = 0;
  pStore->readBlockIndex = 0;
  pStore->bytesUsed = 0;
  pStore->readOpen = false;
  pStore->readEof = false;
}

void interpResultStoreDestroy(SInterpResultStore* pStore) {
  if (pStore == NULL) {
    return;
  }

  interpResultStoreReset(pStore);
  taosHashCleanup(pStore->pGroups);
  taosMemoryFree(pStore);
}

#if defined(BUILD_TEST)
int32_t interpResultStoreTestCreate(SInterpResultStore** ppStore,
                                    int32_t pageSize,
                                    int64_t inMemBytes) {
  return interpResultStoreCreateImpl(ppStore, pageSize, inMemBytes);
}

int32_t interpResultStoreTestActivePages(
    const SInterpResultStore* pStore) {
  return pStore == NULL ? 0 :
         execBlockSpoolTestActivePages(pStore->pSpool);
}

int32_t interpResultStoreTestRegisteredPages(
    const SInterpResultStore* pStore) {
  return pStore == NULL ? 0 :
         execBlockSpoolTestRegisteredPages(pStore->pSpool);
}

void interpResultStoreTestFailPageAllocAfter(
    SInterpResultStore* pStore, int32_t successfulPages) {
  if (pStore != NULL) {
    execBlockSpoolTestFailPageAllocAfter(
        pStore->pSpool, successfulPages);
  }
}

void interpResultStoreTestFailNextDirectoryCommit(
    SInterpResultStore* pStore) {
  if (pStore != NULL) {
    execBlockSpoolTestFailNextDirectoryCommit(pStore->pSpool);
  }
}

void interpResultStoreTestFailNextSpillWrite(
    SInterpResultStore* pStore) {
  if (pStore != NULL) {
    execBlockSpoolTestFailNextSpillWrite(pStore->pSpool);
  }
}
#endif
