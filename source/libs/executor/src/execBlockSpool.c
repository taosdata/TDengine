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

#include "execBlockSpool.h"

#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tpagedbuf.h"

typedef struct SExecBlockSpoolEntry {
  SArray*        pPageIds;
  SArray*        pColumnInfos;
  SDataBlockInfo info;
  int32_t        encodedSize;
} SExecBlockSpoolEntry;

struct SExecBlockSpoolDirectory {
  SArray* pEntries;
};

struct SExecBlockSpool {
  SDiskbasedBuf* pBuf;
  SSDataBlock*   pDecoded;
  char*          pDecodeBuf;
  int32_t        decodeCapacity;
  int32_t        pageSize;
  int32_t        activePages;
#if defined(BUILD_TEST)
  int32_t failPageAllocAfter;
  bool    failNextDirectoryCommit;
#endif
};

static void execBlockSpoolInfoCleanup(SDataBlockInfo* pInfo) {
  if (pInfo != NULL && IS_VAR_DATA_TYPE(pInfo->pks[0].type)) {
    taosMemoryFreeClear(pInfo->pks[0].pData);
    taosMemoryFreeClear(pInfo->pks[1].pData);
  }
}

static int32_t execBlockSpoolInfoCopy(SDataBlockInfo* pDst,
                                      const SDataBlockInfo* pSrc) {
  *pDst = *pSrc;
  pDst->pBlockAgg = NULL;
  if (!IS_VAR_DATA_TYPE(pSrc->pks[0].type)) {
    return TSDB_CODE_SUCCESS;
  }

  pDst->pks[0].pData = NULL;
  pDst->pks[1].pData = NULL;
  int32_t code = copyPkVal(pDst, pSrc);
  if (code != TSDB_CODE_SUCCESS) {
    execBlockSpoolInfoCleanup(pDst);
  }
  return code;
}

static void execBlockSpoolEntryCleanup(SExecBlockSpoolEntry* pEntry) {
  if (pEntry == NULL) {
    return;
  }

  execBlockSpoolInfoCleanup(&pEntry->info);
  taosArrayDestroy(pEntry->pPageIds);
  taosArrayDestroy(pEntry->pColumnInfos);
  memset(pEntry, 0, sizeof(*pEntry));
}

int32_t execBlockSpoolCreate(int32_t pageSize, int64_t inMemBytes,
                             const char* id, const char* tempDir,
                             SExecBlockSpool** ppSpool) {
  if (pageSize <= 0 || pageSize > INT32_MAX / 2 ||
      inMemBytes < (int64_t)pageSize * 2 || id == NULL ||
      inMemBytes / pageSize > INT32_MAX || tempDir == NULL ||
      ppSpool == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppSpool = NULL;
  SExecBlockSpool* pSpool =
      taosMemoryCalloc(1, sizeof(SExecBlockSpool));
  if (pSpool == NULL) {
    return terrno;
  }

  int32_t code = createDiskbasedBuf(&pSpool->pBuf, pageSize,
                                    inMemBytes, id, tempDir);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pSpool);
    return code;
  }

  pSpool->pageSize = pageSize;
#if defined(BUILD_TEST)
  pSpool->failPageAllocAfter = -1;
#endif
  *ppSpool = pSpool;
  return TSDB_CODE_SUCCESS;
}

void execBlockSpoolDestroy(SExecBlockSpool* pSpool) {
  if (pSpool == NULL) {
    return;
  }

  blockDataDestroy(pSpool->pDecoded);
  taosMemoryFree(pSpool->pDecodeBuf);
  destroyDiskbasedBuf(pSpool->pBuf);
  taosMemoryFree(pSpool);
}

int32_t execBlockSpoolDirectoryCreate(
    SExecBlockSpoolDirectory** ppDirectory) {
  if (ppDirectory == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppDirectory = NULL;

  SExecBlockSpoolDirectory* pDirectory =
      taosMemoryCalloc(1, sizeof(SExecBlockSpoolDirectory));
  if (pDirectory == NULL) {
    return terrno;
  }
  pDirectory->pEntries =
      taosArrayInit(4, sizeof(SExecBlockSpoolEntry));
  if (pDirectory->pEntries == NULL) {
    int32_t code = terrno;
    taosMemoryFree(pDirectory);
    return code;
  }

  *ppDirectory = pDirectory;
  return TSDB_CODE_SUCCESS;
}

void execBlockSpoolDirectoryDestroy(
    SExecBlockSpoolDirectory* pDirectory) {
  if (pDirectory == NULL) {
    return;
  }

  int32_t entries = taosArrayGetSize(pDirectory->pEntries);
  for (int32_t i = 0; i < entries; ++i) {
    SExecBlockSpoolEntry* pEntry =
        taosArrayGet(pDirectory->pEntries, i);
    execBlockSpoolEntryCleanup(pEntry);
  }
  taosArrayDestroy(pDirectory->pEntries);
  taosMemoryFree(pDirectory);
}

int32_t execBlockSpoolDirectorySize(
    const SExecBlockSpoolDirectory* pDirectory) {
  return pDirectory == NULL ? 0 :
         taosArrayGetSize(pDirectory->pEntries);
}

static int32_t execBlockSpoolRecycleEntry(
    SExecBlockSpool* pSpool, const SExecBlockSpoolEntry* pEntry) {
  int32_t firstCode = TSDB_CODE_SUCCESS;
  int32_t pages = taosArrayGetSize(pEntry->pPageIds);
  for (int32_t i = 0; i < pages; ++i) {
    int32_t* pPageId = taosArrayGet(pEntry->pPageIds, i);
    int32_t code = pPageId == NULL ? TSDB_CODE_OUT_OF_RANGE :
        dBufRecyclePageById(pSpool->pBuf, *pPageId);
    if (code == TSDB_CODE_SUCCESS) {
      --pSpool->activePages;
    } else if (firstCode == TSDB_CODE_SUCCESS) {
      firstCode = code;
    }
  }
  return firstCode;
}

static int32_t execBlockSpoolGetNewPage(SExecBlockSpool* pSpool,
                                        int32_t* pPageId,
                                        void** ppPage) {
#if defined(BUILD_TEST)
  if (pSpool->failPageAllocAfter == 0) {
    pSpool->failPageAllocAfter = -1;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pSpool->failPageAllocAfter > 0) {
    --pSpool->failPageAllocAfter;
  }
#endif

  *ppPage = getNewBufPage(pSpool->pBuf, pPageId);
  return *ppPage == NULL ? terrno : TSDB_CODE_SUCCESS;
}

static int32_t execBlockSpoolWritePages(
    SExecBlockSpool* pSpool, const char* pEncoded, int32_t encodedSize,
    SExecBlockSpoolEntry* pEntry) {
  int32_t offset = 0;
  while (offset < encodedSize) {
    int32_t pageId = -1;
    void* pPage = NULL;
    int32_t code =
        execBlockSpoolGetNewPage(pSpool, &pageId, &pPage);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosArrayPush(pEntry->pPageIds, &pageId) == NULL) {
      code = terrno;
      releaseBufPage(pSpool->pBuf, pPage);
      int32_t recycleCode =
          dBufRecyclePageById(pSpool->pBuf, pageId);
      return recycleCode == TSDB_CODE_SUCCESS ? code : recycleCode;
    }

    int32_t remaining = encodedSize - offset;
    int32_t bytes = remaining < pSpool->pageSize ?
                    remaining : pSpool->pageSize;
    memcpy(pPage, pEncoded + offset, bytes);
    if (bytes < pSpool->pageSize) {
      memset((char*)pPage + bytes, 0, pSpool->pageSize - bytes);
    }
    setBufPageDirty(pPage, true);
    releaseBufPage(pSpool->pBuf, pPage);
    ++pSpool->activePages;
    offset += bytes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t execBlockSpoolCopyColumnInfos(
    const SSDataBlock* pBlock, SExecBlockSpoolEntry* pEntry) {
  int32_t columns = taosArrayGetSize(pBlock->pDataBlock);
  pEntry->pColumnInfos = taosArrayInit(columns, sizeof(SColumnInfo));
  if (pEntry->pColumnInfos == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < columns; ++i) {
    SColumnInfoData* pColumn = taosArrayGet(pBlock->pDataBlock, i);
    if (pColumn == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if (taosArrayPush(pEntry->pColumnInfos, &pColumn->info) == NULL) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t execBlockSpoolAppend(SExecBlockSpool* pSpool,
                             SExecBlockSpoolDirectory* pDirectory,
                             const SSDataBlock* pBlock) {
  if (pSpool == NULL || pDirectory == NULL || pBlock == NULL ||
      pBlock->info.rows <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t encodedSize = blockGetInternalEncodeSize(pBlock);
  if (encodedSize <= 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  char* pEncoded = taosMemoryMalloc(encodedSize);
  if (pEncoded == NULL) {
    return terrno;
  }

  int32_t actualSize = blockEncodeInternal(
      pBlock, pEncoded, encodedSize,
      taosArrayGetSize(pBlock->pDataBlock));
  if (actualSize != encodedSize) {
    int32_t code = actualSize < 0 ? terrno :
                   TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    taosMemoryFree(pEncoded);
    return code;
  }

  SExecBlockSpoolEntry entry = {.encodedSize = encodedSize};
  int32_t pageCapacity =
      encodedSize / pSpool->pageSize +
      (encodedSize % pSpool->pageSize != 0);
  entry.pPageIds = taosArrayInit(pageCapacity, sizeof(int32_t));
  if (entry.pPageIds == NULL) {
    int32_t code = terrno;
    taosMemoryFree(pEncoded);
    return code;
  }

  int32_t code = execBlockSpoolInfoCopy(&entry.info, &pBlock->info);
  if (code == TSDB_CODE_SUCCESS) {
    code = execBlockSpoolCopyColumnInfos(pBlock, &entry);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = execBlockSpoolWritePages(pSpool, pEncoded, encodedSize,
                                    &entry);
  }
  taosMemoryFree(pEncoded);

#if defined(BUILD_TEST)
  if (code == TSDB_CODE_SUCCESS &&
      pSpool->failNextDirectoryCommit) {
    pSpool->failNextDirectoryCommit = false;
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
#endif
  if (code == TSDB_CODE_SUCCESS &&
      taosArrayPush(pDirectory->pEntries, &entry) == NULL) {
    code = terrno;
  }
  if (code != TSDB_CODE_SUCCESS) {
    int32_t recycleCode = execBlockSpoolRecycleEntry(pSpool, &entry);
    execBlockSpoolEntryCleanup(&entry);
    return recycleCode == TSDB_CODE_SUCCESS ? code : recycleCode;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t execBlockSpoolEnsureDecodeCapacity(
    SExecBlockSpool* pSpool, int32_t encodedSize) {
  if (encodedSize <= pSpool->decodeCapacity) {
    return TSDB_CODE_SUCCESS;
  }

  char* pNew = taosMemoryRealloc(pSpool->pDecodeBuf, encodedSize);
  if (pNew == NULL) {
    return terrno;
  }
  pSpool->pDecodeBuf = pNew;
  pSpool->decodeCapacity = encodedSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t execBlockSpoolLoadPages(
    SExecBlockSpool* pSpool, const SExecBlockSpoolEntry* pEntry) {
  int32_t offset = 0;
  int32_t pages = taosArrayGetSize(pEntry->pPageIds);
  for (int32_t i = 0; i < pages; ++i) {
    int32_t* pPageId = taosArrayGet(pEntry->pPageIds, i);
    if (pPageId == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    void* pPage = getBufPage(pSpool->pBuf, *pPageId);
    if (pPage == NULL) {
      return terrno;
    }
    int32_t remaining = pEntry->encodedSize - offset;
    int32_t bytes = remaining < pSpool->pageSize ?
                    remaining : pSpool->pageSize;
    memcpy(pSpool->pDecodeBuf + offset, pPage, bytes);
    releaseBufPage(pSpool->pBuf, pPage);
    offset += bytes;
  }
  return offset == pEntry->encodedSize ? TSDB_CODE_SUCCESS :
         TSDB_CODE_FILE_CORRUPTED;
}

static int32_t execBlockSpoolRestoreInfo(
    SSDataBlock* pBlock, const SExecBlockSpoolEntry* pEntry) {
  int32_t columns = taosArrayGetSize(pBlock->pDataBlock);
  if (columns != taosArrayGetSize(pEntry->pColumnInfos)) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  for (int32_t i = 0; i < columns; ++i) {
    SColumnInfoData* pColumn = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfo* pStored = taosArrayGet(pEntry->pColumnInfos, i);
    if (pColumn == NULL || pStored == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }

  uint32_t capacity = pBlock->info.capacity;
  SDataBlockInfo restored = {0};
  int32_t code = execBlockSpoolInfoCopy(&restored, &pEntry->info);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  restored.capacity = capacity;
  restored.pBlockAgg = NULL;
  pBlock->info = restored;

  for (int32_t i = 0; i < columns; ++i) {
    SColumnInfoData* pColumn = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfo* pStored = taosArrayGet(pEntry->pColumnInfos, i);
    pColumn->info = *pStored;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t execBlockSpoolRead(SExecBlockSpool* pSpool,
                           const SExecBlockSpoolDirectory* pDirectory,
                           int32_t blockIndex, SSDataBlock** ppBlock) {
  if (pSpool == NULL || pDirectory == NULL || ppBlock == NULL ||
      blockIndex < 0 ||
      blockIndex >= execBlockSpoolDirectorySize(pDirectory)) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppBlock = NULL;

  SExecBlockSpoolEntry* pEntry =
      taosArrayGet(pDirectory->pEntries, blockIndex);
  if (pEntry == NULL || pEntry->encodedSize <= 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  blockDataDestroy(pSpool->pDecoded);
  pSpool->pDecoded = NULL;
  int32_t code = execBlockSpoolEnsureDecodeCapacity(
      pSpool, pEntry->encodedSize);
  if (code == TSDB_CODE_SUCCESS) {
    code = execBlockSpoolLoadPages(pSpool, pEntry);
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SSDataBlock* pDecoded = NULL;
  code = createDataBlock(&pDecoded);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDecodeInternalBounded(
        pDecoded, pSpool->pDecodeBuf, pEntry->encodedSize, NULL);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = execBlockSpoolRestoreInfo(pDecoded, pEntry);
  }
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDecoded);
    return code;
  }

  pSpool->pDecoded = pDecoded;
  *ppBlock = pDecoded;
  return TSDB_CODE_SUCCESS;
}

int32_t execBlockSpoolRecycle(
    SExecBlockSpool* pSpool,
    const SExecBlockSpoolDirectory* pDirectory) {
  if (pSpool == NULL || pDirectory == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t firstCode = TSDB_CODE_SUCCESS;
  int32_t entries = execBlockSpoolDirectorySize(pDirectory);
  for (int32_t i = 0; i < entries; ++i) {
    SExecBlockSpoolEntry* pEntry =
        taosArrayGet(pDirectory->pEntries, i);
    int32_t code = pEntry == NULL ? TSDB_CODE_OUT_OF_RANGE :
        execBlockSpoolRecycleEntry(pSpool, pEntry);
    if (code != TSDB_CODE_SUCCESS && firstCode == TSDB_CODE_SUCCESS) {
      firstCode = code;
    }
  }
  return firstCode;
}

int64_t execBlockSpoolResidentPagePayloadBytes(
    const SExecBlockSpool* pSpool) {
  if (pSpool == NULL) {
    return 0;
  }
  return (int64_t)getNumOfResidentBufPages(pSpool->pBuf) *
         pSpool->pageSize;
}

int64_t execBlockSpoolSpillBytes(const SExecBlockSpool* pSpool) {
  return pSpool == NULL ? 0 : getDBufStatis(pSpool->pBuf).flushBytes;
}

#if defined(BUILD_TEST)
int32_t execBlockSpoolTestActivePages(const SExecBlockSpool* pSpool) {
  return pSpool == NULL ? 0 : pSpool->activePages;
}

int32_t execBlockSpoolTestRegisteredPages(
    const SExecBlockSpool* pSpool) {
  return pSpool == NULL ? 0 :
         taosArrayGetSize(getDataBufPagesIdList(pSpool->pBuf));
}

void execBlockSpoolTestFailPageAllocAfter(SExecBlockSpool* pSpool,
                                          int32_t successfulPages) {
  if (pSpool != NULL && successfulPages >= 0) {
    pSpool->failPageAllocAfter = successfulPages;
  }
}

void execBlockSpoolTestFailNextDirectoryCommit(
    SExecBlockSpool* pSpool) {
  if (pSpool != NULL) {
    pSpool->failNextDirectoryCommit = true;
  }
}

void execBlockSpoolTestFailNextSpillWrite(SExecBlockSpool* pSpool) {
  if (pSpool != NULL) {
    dbgFailNextDiskbasedBufFlushWrite(pSpool->pBuf);
  }
}
#endif
