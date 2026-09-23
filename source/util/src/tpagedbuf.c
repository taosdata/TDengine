/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "tpagedbuf.h"
#include "crypt.h"
#include "taoserror.h"
#include "tcompression.h"
#include "tglobal.h"
#include "tlog.h"
#include "tsimplehash.h"

#define GET_PAYLOAD_DATA(_p)           ((char*)(_p)->pData + POINTER_BYTES)
#define BUF_PAGE_IN_MEM(_p)            ((_p)->pData != NULL)
#define CLEAR_BUF_PAGE_IN_MEM_FLAG(_p) ((_p)->pData = NULL)
#define HAS_DATA_IN_DISK(_p)           ((_p)->offset >= 0)
#define NO_IN_MEM_AVAILABLE_PAGES(_b)  (listNEles((_b)->lruList) >= (_b)->inMemPages)

typedef struct SPageDiskInfo {
  int64_t offset;
  int32_t length;
} SPageDiskInfo, SFreeListItem;

typedef struct SPageDiskReservation {
  int64_t offset;
  size_t  freeIndex;
  bool    fromFree;
} SPageDiskReservation;

struct SPageInfo {
  SListNode* pn;  // point to list node struct. it is NULL when the page is evicted from the in-memory buffer
  SPageInfo* pPrevFree;
  SPageInfo* pNextFree;
  void*      pData;
  int64_t    offset;
  int32_t    pageId;
  int32_t    idListIndex;
  int32_t    length : 29;
  bool       used : 1;   // set current page is in used
  bool       dirty : 1;  // set current buffer page is dirty or not
  bool       recycled : 1;
};

struct SDiskbasedBuf {
  int32_t    numOfPages;
  int64_t    totalBufSize;
  uint64_t   fileSize;  // disk file size
  TdFilePtr  pFile;
  int32_t    allocateId;  // allocated page id
  char*      path;        // file path
  char*      prefix;      // file name prefix
  int32_t    pageSize;    // current used page size
  int32_t    inMemPages;  // numOfPages that are allocated in memory
  SPageInfo* pFreePageHead;
  SPageInfo* pFreePageTail;
  SArray*    pIdList;     // page id list
  SSHashObj* all;
  SList*     lruList;
  void*      emptyDummyIdList;  // dummy id list
  void*      assistBuf;         // assistant buffer for compress/decompress data
  SArray*    pFree;             // free area in file
  bool       comp;              // compressed before flushed to disk
  uint64_t   nextPos;           // next page flush position

  char*               id;           // for debug purpose
  bool                printStatis;  // Print statistics info when closing this buffer.
  SDiskbasedBufStatis statis;
#if defined(BUILD_TEST)
  bool                dbgFailNextFlushWrite;
  bool                dbgShortNextFlushWrite;
#endif

  // One context owns the memory-only key and work buffers for this spill file.
  // Encryption is enabled iff the context is non-NULL.
  SSpillCryptCtx* pCryptCtx;
};

static int32_t createDiskFile(SDiskbasedBuf* pBuf) {
  if (pBuf->path == NULL) {  // prepare the file name when needed it
    char path[PATH_MAX] = {0};
    taosGetTmpfilePath(pBuf->prefix, "paged-buf", path);
    pBuf->path = taosStrdup(path);
    if (pBuf->path == NULL) {
      return terrno;
    }
  }

  pBuf->pFile =
      taosOpenFile(pBuf->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  if (pBuf->pFile == NULL) {
    return terrno;
  }

  int64_t realSize = -1;
  if (taosFStatFile(pBuf->pFile, &realSize, NULL) != TSDB_CODE_SUCCESS) {
    realSize = -1;
  }
  uDebug("paged buffer file opened, path:%s, realSize:%" PRId64 ", nextPos:%" PRIu64 ", fileSize:%" PRIu64 ", %s",
         pBuf->path, realSize, pBuf->nextPos, pBuf->fileSize, pBuf->id);

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  if (!pBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsCompressString(data, srcSize, 1, pBuf->assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);
  return pBuf->assistBuf;
}

static int32_t doDecompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  int32_t code = 0;
  if (!pBuf->comp) {
    *dst = srcSize;
    return code;
  }

  *dst = tsDecompressString(data, srcSize, 1, pBuf->assistBuf, pBuf->pageSize, ONE_STAGE_COMP, NULL, 0);
  if (*dst > 0) {
    memcpy(data, pBuf->assistBuf, *dst);
  } else if (*dst < 0) {
    return terrno;
  }
  return code;
  ;
}

static void removeFreeListItem(SArray* pFreeList, size_t index) {
  size_t last = taosArrayGetSize(pFreeList) - 1;
  if (index != last) {
    SFreeListItem* pItem = taosArrayGet(pFreeList, index);
    SFreeListItem* pLast = taosArrayGet(pFreeList, last);
    *pItem = *pLast;
  }
  taosArrayPop(pFreeList);
}

static int32_t prepareDiskReservation(
    SDiskbasedBuf* pBuf, int32_t size,
    SPageDiskReservation* pReservation) {
  if (size <= 0 || pReservation == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pReservation = (SPageDiskReservation){0};
  size_t i = 0;
  while (i < taosArrayGetSize(pBuf->pFree)) {
    SFreeListItem* pFree = taosArrayGet(pBuf->pFree, i);
    if (pFree == NULL || pFree->length <= 0) {
      removeFreeListItem(pBuf->pFree, i);
      continue;
    }
    if (pFree->length >= size) {
      pReservation->offset = pFree->offset;
      pReservation->freeIndex = i;
      pReservation->fromFree = true;
      return TSDB_CODE_SUCCESS;
    }
    ++i;
  }

  if (pBuf->nextPos > INT64_MAX - (uint64_t)size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  pReservation->offset = (int64_t)pBuf->nextPos;
  return TSDB_CODE_SUCCESS;
}

static int32_t commitDiskReservation(
    SDiskbasedBuf* pBuf, const SPageDiskReservation* pReservation,
    int32_t size) {
  if (!pReservation->fromFree) {
    pBuf->nextPos += (uint64_t)size;
    return TSDB_CODE_SUCCESS;
  }

  SFreeListItem* pFree =
      taosArrayGet(pBuf->pFree, pReservation->freeIndex);
  if (pFree == NULL || pFree->offset != pReservation->offset ||
      pFree->length < size) {
    return TSDB_CODE_APP_ERROR;
  }
  pFree->offset += size;
  pFree->length -= size;
  if (pFree->length == 0) {
    removeFreeListItem(pBuf->pFree, pReservation->freeIndex);
  }
  return TSDB_CODE_SUCCESS;
}

/**
 *   +--------------------------+-------------------+--------------+
 *   | PTR to SPageInfo (8bytes)| Payload (PageSize)| 2 Extra Bytes|
 *   +--------------------------+-------------------+--------------+
 * @param pBuf
 * @param pg
 * @return
 */

static FORCE_INLINE size_t getAllocPageSize(int32_t pageSize) { return pageSize + POINTER_BYTES + sizeof(SFilePage); }

// The encrypted page image layout and its size relation are owned by crypt
// (taosSpillCryptEncodedLen); the stored plaintext length is the
// post-compression size, needed to strip the padding before decompression.
// Maximum encoded size of one encrypted page image; 0 for out-of-range
// page sizes, which then rejects every positive on-disk length.
static int32_t pageEncryptedDataCap(int32_t pageSize) {
  int32_t encodedLen = 0;
  if (taosSpillCryptEncodedLen((int32_t)getAllocPageSize(pageSize),
                               &encodedLen) != TSDB_CODE_SUCCESS) {
    return 0;
  }
  return encodedLen;
}

// Encrypt `len` bytes from `src` into the shared crypt context. On success sets
// *ppOut/*pOutLen to the on-disk image ([hdr][cipher]).
static int32_t pageEncrypt(SDiskbasedBuf* pBuf, const char* src, int32_t len,
                           char** ppOut, int32_t* pOutLen) {
  int32_t alignCap = ENCRYPTED_LEN((int32_t)getAllocPageSize(pBuf->pageSize));
  int32_t alignedLen = ENCRYPTED_LEN(len);  // pad to CBC block

  // By construction len (the compressed size) never exceeds one page image.
  if (len < 0 || alignedLen > alignCap) {
    uError("%s failed at line:%d, bad len:%d, alignedLen:%d, alignCap:%d, "
           "buf id:%s", __func__, __LINE__, len, alignedLen, alignCap, pBuf->id);
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code =
      taosEncryptSpillBlock(pBuf->pCryptCtx, src, len, ppOut, pOutLen);
  if (code != TSDB_CODE_SUCCESS) {
    uError(
        "%s failed at line:%d, encrypt spill block failed because %s, buf "
        "id:%s",
        __func__, __LINE__, tstrerror(code), pBuf->id);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

// Decrypt an on-disk image of `total` bytes at `pEncrypted`; writes the
// recovered plaintext into `dst` and returns its length. Truncated or
// corrupted images are rejected by taosDecryptSpillBlock.
static int32_t pageDecrypt(SDiskbasedBuf* pBuf, const char* pEncrypted,
                           void* dst, int32_t total, int32_t* pPlainLen) {
  int32_t dstCap = pBuf->pageSize + (int32_t)sizeof(SFilePage);
  char*   pPlain = NULL;
  int32_t code = taosDecryptSpillBlock(pBuf->pCryptCtx, pEncrypted, total,
                                       &pPlain, pPlainLen);
  if (code != TSDB_CODE_SUCCESS || *pPlainLen > dstCap) {
    uError(
        "%s failed at line:%d, decrypt spill block failed because %s, buf "
        "id:%s",
        __func__, __LINE__,
        tstrerror(code == TSDB_CODE_SUCCESS ? TSDB_CODE_FILE_CORRUPTED : code),
        pBuf->id);
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_FILE_CORRUPTED : code;
  }

  memcpy(dst, pPlain, *pPlainLen);
  return TSDB_CODE_SUCCESS;
}

static int32_t doFlushBufPageImpl(SDiskbasedBuf* pBuf, int64_t offset,
                                  const char* pData, int32_t size) {
  int64_t ret = taosLSeekFile(pBuf->pFile, offset, SEEK_SET);
  if (ret < 0) {
    return terrno;
  }

#if defined(BUILD_TEST)
  if (pBuf->dbgFailNextFlushWrite) {
    pBuf->dbgFailNextFlushWrite = false;
    terrno = TSDB_CODE_APP_ERROR;
    return terrno;
  }

  if (pBuf->dbgShortNextFlushWrite) {
    pBuf->dbgShortNextFlushWrite = false;
    terrno = TSDB_CODE_SUCCESS;
    ret = size - 1;
  } else
#endif
  {
    ret = (int32_t)taosWriteFile(pBuf->pFile, pData, size);
  }
  if (ret != size) {
    // A successful short write does not set terrno.
    return ret < 0 ? terrno : TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t flushDirtyPage(SDiskbasedBuf* pBuf, SPageInfo* pPage,
                              const char* pData, int32_t size,
                              int64_t* pOffset) {
  bool needsNewPosition = !HAS_DATA_IN_DISK(pPage) ||
                          pPage->length < size;
  bool recycleOldPosition = HAS_DATA_IN_DISK(pPage) &&
                            pPage->length < size;
  if (recycleOldPosition) {
    size_t freeItems = taosArrayGetSize(pBuf->pFree);
    int32_t code = taosArrayEnsureCap(pBuf->pFree, freeItems + 1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  SPageDiskReservation reservation = {0};
  int64_t offset = pPage->offset;
  if (needsNewPosition) {
    int32_t code = prepareDiskReservation(pBuf, size, &reservation);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    offset = reservation.offset;
  }

  int32_t code = doFlushBufPageImpl(pBuf, offset, pData, size);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (needsNewPosition) {
    code = commitDiskReservation(pBuf, &reservation, size);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  if (recycleOldPosition) {
    SPageDiskInfo oldPosition = {
        .length = pPage->length,
        .offset = pPage->offset,
    };
    if (taosArrayPush(pBuf->pFree, &oldPosition) == NULL) {
      int32_t code = terrno == TSDB_CODE_SUCCESS
                         ? TSDB_CODE_APP_ERROR
                         : terrno;
      uError("failed to recycle old disk extent, offset:%" PRId64
             ", length:%d, buf id:%s, since %s",
             oldPosition.offset, oldPosition.length, pBuf->id,
             tstrerror(code));
      return code;
    }
  }

  if (pBuf->fileSize < (uint64_t)offset + (uint64_t)size) {
    pBuf->fileSize = (uint64_t)offset + (uint64_t)size;
  }
  pBuf->statis.flushBytes += size;
  pBuf->statis.flushPages += 1;
  *pOffset = offset;
  return TSDB_CODE_SUCCESS;
}

static char* doFlushBufPage(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  if (pg->pData == NULL || pg->used) {
    uError("invalid params in paged buffer process when flushing buf to disk, %s", pBuf->id);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t size = pBuf->pageSize;
  int64_t offset = pg->offset;

  char* t = NULL;
  if ((!HAS_DATA_IN_DISK(pg)) || pg->dirty) {
    void* payload = GET_PAYLOAD_DATA(pg);
    t = doCompressData(payload, pBuf->pageSize + sizeof(SFilePage), &size, pBuf);
    if (size < 0) {
      uError("failed to compress data when flushing data to disk, %s", pBuf->id);
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }
  }

  // Encrypt after compression (and only on the write path). The on-disk image
  // carries the plaintext length so the padding can be stripped on read.
  if (pBuf->pCryptCtx != NULL && t != NULL && pg->dirty) {
    char*   enc = NULL;
    int32_t encLen = 0;
    int32_t code = pageEncrypt(pBuf, t, size, &enc, &encLen);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to encrypt page when flushing data to disk, %s", pBuf->id);
      terrno = code;
      return NULL;
    }
    t = enc;
    size = encLen;
  }

  if (pg->dirty) {
    int32_t code = flushDirtyPage(pBuf, pg, t, size, &offset);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }
  } else {  // NOTE: the size may be -1, the this recycle page has not been flushed to disk yet.
    size = pg->length;
  }

  char* pDataBuf = pg->pData;
  memset(pDataBuf, 0, getAllocPageSize(pBuf->pageSize));

#ifdef BUF_PAGE_DEBUG
  uDebug("page_flush %p, pageId:%d, offset:%d", pDataBuf, pg->pageId, offset);
#endif

  pg->offset = offset;
  pg->length = size;  // on disk size
  return pDataBuf;
}

static char* flushBufPage(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (pBuf->pFile == NULL) {
    if ((ret = createDiskFile(pBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  char* p = doFlushBufPage(pBuf, pg);
  if (p == NULL) {
    return NULL;
  }
  CLEAR_BUF_PAGE_IN_MEM_FLAG(pg);

  pg->dirty = false;
  return p;
}

// load file block data in disk
static int32_t loadPageFromDisk(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  if (pg->offset < 0 || pg->length <= 0) {
    uError("failed to load buf page from disk, offset:%" PRId64 ", length:%d, %s", pg->offset, pg->length, pBuf->id);
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
  if (ret < 0) {
    ret = terrno;
    return ret;
  }

  void* pPage = (void*)GET_PAYLOAD_DATA(pg);

  // When encrypted, read the on-disk image into the staging buffer; otherwise
  // read straight into the page payload.
  void* readDst = pPage;
  if (pBuf->pCryptCtx != NULL) {
    int32_t encryptedDataCap = pageEncryptedDataCap(pBuf->pageSize);
    if (pg->length > encryptedDataCap) {
      uError(
          "invalid on-disk page length at line:%d, length:%d cap:%d, buf id:%s",
          __LINE__, pg->length, encryptedDataCap, pBuf->id);
      return TSDB_CODE_FILE_CORRUPTED;
    }
    char*   pEncrypted = NULL;
    int32_t code =
        taosPrepareSpillBlockRead(pBuf->pCryptCtx, pg->length, &pEncrypted);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    readDst = pEncrypted;
  }
  ret = taosReadFile(pBuf->pFile, readDst, pg->length);
  if (ret != pg->length) {
    ret = terrno;
    return ret;
  }

  // length of the (post-decrypt) data fed to decompression
  int32_t dataLen = pg->length;
  if (pBuf->pCryptCtx != NULL) {
    int32_t code = pageDecrypt(pBuf, readDst, pPage, pg->length, &dataLen);
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to decrypt buf page from disk, offset:%" PRId64
             ", length:%d, buf id:%s", pg->offset, pg->length, pBuf->id);
      return code;
    }
  }

  pBuf->statis.loadBytes += pg->length;
  pBuf->statis.loadPages += 1;

  int32_t fullSize = 0;
  return doDecompressData(pPage, dataLen, &fullSize, pBuf);
}

static SPageInfo* registerNewPageInfo(SDiskbasedBuf* pBuf, int32_t pageId) {
  pBuf->numOfPages += 1;

  SPageInfo* ppi = taosMemoryMalloc(sizeof(SPageInfo));
  if (ppi == NULL) {
    return NULL;
  }

  ppi->pageId = pageId;
  ppi->pData = NULL;
  ppi->offset = -1;
  ppi->length = -1;
  ppi->used = true;
  ppi->pn = NULL;
  ppi->pPrevFree = NULL;
  ppi->pNextFree = NULL;
  ppi->dirty = false;
  ppi->recycled = false;

  SPageInfo** pRet = taosArrayPush(pBuf->pIdList, &ppi);
  if (NULL == pRet) {
    taosMemoryFree(ppi);
    return NULL;
  }
  ptrdiff_t index = pRet - (SPageInfo**)pBuf->pIdList->pData;
  if (index < 0 || index > INT32_MAX) {
    taosArrayPop(pBuf->pIdList);
    pBuf->numOfPages -= 1;
    taosMemoryFree(ppi);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return NULL;
  }
  ppi->idListIndex = (int32_t)index;
  return *pRet;
}

static SListNode* getEldestUnrefedPage(SDiskbasedBuf* pBuf) {
  SListIter iter = {0};
  tdListInitIter(pBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while ((pn = tdListNext(&iter)) != NULL) {
    SPageInfo* pageInfo = *(SPageInfo**)pn->data;

    SPageInfo* p = *(SPageInfo**)(pageInfo->pData);

    if (!pageInfo->used) {
      break;
    }
  }

  return pn;
}

static char* evictBufPage(SDiskbasedBuf* pBuf) {
  SListNode* pn = getEldestUnrefedPage(pBuf);
  if (pn == NULL) {  // no available buffer pages now, return.
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  terrno = 0;
  SPageInfo* d = *(SPageInfo**)pn->data;
  char* pData = flushBufPage(pBuf, d);
  if (pData == NULL) {
    return NULL;
  }

  pn = tdListPopNode(pBuf->lruList, pn);
  d->pn = NULL;
  taosMemoryFreeClear(pn);
  return pData;
}

static int32_t lruListPushFront(SList* pList, SPageInfo* pi) {
  int32_t code = tdListPrepend(pList, &pi);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SListNode* front = tdListGetHead(pList);
  pi->pn = front;
  return TSDB_CODE_SUCCESS;
}

static void lruListMoveToFront(SList* pList, SPageInfo* pi) {
  pi->pn = tdListPopNode(pList, pi->pn);
  tdListPrependNode(pList, pi->pn);
}

static SPageInfo* getPageInfoFromPayload(void* page) {
  char* p = (char*)page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**)p)[0];
  return ppi;
}

int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int64_t inMemBufSize, const char* id,
                           const char* dir) {
  int32_t code = 0;
  *pBuf = NULL;
  SDiskbasedBuf* pPBuf = taosMemoryCalloc(1, sizeof(SDiskbasedBuf));
  if (pPBuf == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->pageSize = pagesize;
  pPBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pPBuf->totalBufSize = 0;
  pPBuf->allocateId = -1;
  pPBuf->pFile = NULL;
  pPBuf->id = taosStrdup(id);
  if (id != NULL && pPBuf->id == NULL) {
    code = terrno;
    goto _error;
  }
  pPBuf->fileSize = 0;
  pPBuf->pFree = taosArrayInit(4, sizeof(SFreeListItem));
  if (pPBuf->pFree == NULL) {
    code = terrno;
    goto _error;
  }

  // at least more than 2 pages must be in memory
  if (inMemBufSize < pagesize * 2) {
    inMemBufSize = pagesize * 2;
  }

  pPBuf->inMemPages = inMemBufSize / pagesize;  // maximum allowed pages, it is a soft limit.
  pPBuf->lruList = tdListNew(POINTER_BYTES);
  if (pPBuf->lruList == NULL) {
    code = terrno;
    goto _error;
  }

  // init id hash table
  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  pPBuf->pIdList = taosArrayInit(4, POINTER_BYTES);
  if (pPBuf->pIdList == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->all = tSimpleHashInit(64, fn);
  if (pPBuf->all == NULL) {
    code = terrno;
    goto _error;
  }

  pPBuf->prefix = (char*)dir;
  pPBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));
  if (pPBuf->emptyDummyIdList == NULL) {
    code = terrno;
    goto _error;
  }

  //  qDebug("QInfo:0x%"PRIx64 ", create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", qId,
  //  pPBuf->pageSize, pPBuf->inMemPages, pPBuf->path);

  // Preserve the existing eager allocation point for paged-buffer encryption.
  if (taosSpillCryptEnabled()) {
    code = taosCreateSpillCryptCtx(&pPBuf->pCryptCtx,
                                   (int32_t)getAllocPageSize(pagesize));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  *pBuf = pPBuf;
  return TSDB_CODE_SUCCESS;

_error:
  destroyDiskbasedBuf(pPBuf);
  *pBuf = NULL;
  return code;
}

static char* doExtractPage(SDiskbasedBuf* pBuf) {
  char* availablePage = NULL;
  if (NO_IN_MEM_AVAILABLE_PAGES(pBuf)) {
    availablePage = evictBufPage(pBuf);
    if (availablePage == NULL) {
      uWarn("no available buf pages, current:%d, max:%d, reason: %s, %s", listNEles(pBuf->lruList), pBuf->inMemPages,
            terrstr(), pBuf->id)
    }
  } else {
    availablePage =
        taosMemoryCalloc(1, getAllocPageSize(pBuf->pageSize));  // add extract bytes in case of zipped buffer increased.
  }

  return availablePage;
}

void* getNewBufPage(SDiskbasedBuf* pBuf, int32_t* pageId) {
  pBuf->statis.getPages += 1;

  char* availablePage = doExtractPage(pBuf);
  if (availablePage == NULL) {
    return NULL;
  }

  SPageInfo* pi = NULL;
  int32_t    code = 0;
  if (pBuf->pFreePageHead != NULL) {
    pi = pBuf->pFreePageHead;
    if (pBuf->pFreePageTail == NULL || pBuf->pFreePageHead->pPrevFree != NULL ||
        !pi->recycled || pi->used || pi->dirty || pi->pData != NULL || pi->pn != NULL) {
      taosMemoryFree(availablePage);
      terrno = TSDB_CODE_APP_ERROR;
      return NULL;
    }
    *pageId = pi->pageId;
    code = lruListPushFront(pBuf->lruList, pi);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(availablePage);
      terrno = code;
      return NULL;
    }
    pBuf->pFreePageHead = pi->pNextFree;
    if (pBuf->pFreePageHead == NULL) {
      pBuf->pFreePageTail = NULL;
    } else {
      pBuf->pFreePageHead->pPrevFree = NULL;
    }
    pi->pPrevFree = NULL;
    pi->pNextFree = NULL;
    pi->used = true;
    pi->recycled = false;
  } else {  // create a new pageinfo
    // register new id in this group
    *pageId = (++pBuf->allocateId);

    // register page id info
    pi = registerNewPageInfo(pBuf, *pageId);
    if (pi == NULL) {
      taosMemoryFree(availablePage);
      return NULL;
    }

    // add to hash map
    int32_t code = tSimpleHashPut(pBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

    if (TSDB_CODE_SUCCESS == code) {
      // add to LRU list
      code = lruListPushFront(pBuf->lruList, pi);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pBuf->totalBufSize += pBuf->pageSize;
    } else {
      taosMemoryFree(availablePage);
      SPageInfo **pLast = taosArrayPop(pBuf->pIdList);
      int32_t ret = tSimpleHashRemove(pBuf->all, pageId, sizeof(int32_t));
      if (ret != TSDB_CODE_SUCCESS) {
        uError("%s failed to clear pageId %d from buf hash-set since %s", __func__, *pageId, tstrerror(ret));
      }
      taosMemoryFree(pi);
      terrno = code;
      return NULL;
    }
  }

  pi->pData = availablePage;

  ((void**)pi->pData)[0] = pi;
#ifdef BUF_PAGE_DEBUG
  uDebug("page_getNewBufPage , pi->pData:%p, pageId:%d, offset:%" PRId64, pi->pData, pi->pageId, pi->offset);
#endif

  return (void*)(GET_PAYLOAD_DATA(pi));
}

static void* getBufPageImpl(SDiskbasedBuf* pBuf, int32_t id,
                            bool updateLru) {
  if (id < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    uError("invalid page id:%d, %s", id, pBuf->id);
    return NULL;
  }

  pBuf->statis.getPages += 1;

  SPageInfo** pi = tSimpleHashGet(pBuf->all, &id, sizeof(int32_t));
  if (pi == NULL || *pi == NULL) {
    uError("failed to locate the buffer page:%d, %s", id, pBuf->id);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  if (BUF_PAGE_IN_MEM(*pi)) {  // it is in memory
    // no need to update the LRU list if only one page exists
    if (pBuf->numOfPages == 1) {
      (*pi)->used = true;
      return (void*)(GET_PAYLOAD_DATA(*pi));
    }

    // The back-pointer check validates the buffer's own bookkeeping, so it
    // runs for every accessor; only the LRU refresh is optional.
    SPageInfo** pInfo = (SPageInfo**)((*pi)->pn->data);
    if (*pInfo != *pi) {
      terrno = TSDB_CODE_APP_ERROR;
      uError("inconsistently data in paged buffer, pInfo:%p, pi:%p, %s",
             *pInfo, *pi, pBuf->id);
      return NULL;
    }
    if (updateLru) {
      lruListMoveToFront(pBuf->lruList, (*pi));
    }
    (*pi)->used = true;

#ifdef BUF_PAGE_DEBUG
    uDebug("page_getBufPage1 pageId:%d, offset:%" PRId64, (*pi)->pageId, (*pi)->offset);
#endif
    return (void*)(GET_PAYLOAD_DATA(*pi));
  } else {  // not in memory

    (*pi)->pData = doExtractPage(pBuf);

    // failed to evict buffer page, return with error code.
    if ((*pi)->pData == NULL) {
      return NULL;
    }

    // set the ptr to the new SPageInfo
    ((void**)((*pi)->pData))[0] = (*pi);

    int32_t code = lruListPushFront(pBuf->lruList, *pi);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree((*pi)->pData);
      (*pi)->pData = NULL;
      terrno = code;
      return NULL;
    }
    (*pi)->used = true;

    // some data has been flushed to disk, and needs to be loaded into buffer again.
    if (HAS_DATA_IN_DISK(*pi)) {
      int32_t code = loadPageFromDisk(pBuf, *pi);
      if (code != 0) {
        taosMemoryFree((*pi)->pData);
        (*pi)->pData = NULL;
        terrno = code;
        return NULL;
      }
    }
#ifdef BUF_PAGE_DEBUG
    uDebug("page_getBufPage2 pageId:%d, offset:%" PRId64, (*pi)->pageId, (*pi)->offset);
#endif
    return (void*)(GET_PAYLOAD_DATA(*pi));
  }
}

void* getBufPage(SDiskbasedBuf* pBuf, int32_t id) {
  return getBufPageImpl(pBuf, id, true);
}

void* getBufPageNoLRU(SDiskbasedBuf* pBuf, int32_t id) {
  return getBufPageImpl(pBuf, id, false);
}

void releaseBufPage(SDiskbasedBuf* pBuf, void* page) {
  if (page == NULL) {
    return;
  }

  SPageInfo* ppi = getPageInfoFromPayload(page);
  releaseBufPageInfo(pBuf, ppi);
}

void releaseBufPageInfo(SDiskbasedBuf* pBuf, SPageInfo* pi) {
#ifdef BUF_PAGE_DEBUG
  uDebug("page_releaseBufPageInfo pageId:%d, used:%d, offset:%" PRId64, pi->pageId, pi->used, pi->offset);
#endif

  if (pi == NULL) {
    return;
  }

  if (pi->pData == NULL) {
    uError("pi->pData (page data) is null");
    return;
  }

  pi->used = false;
  pBuf->statis.releasePages += 1;
}

size_t getTotalBufSize(const SDiskbasedBuf* pBuf) { return (size_t)pBuf->totalBufSize; }

SArray* getDataBufPagesIdList(SDiskbasedBuf* pBuf) { return pBuf->pIdList; }

void destroyDiskbasedBuf(SDiskbasedBuf* pBuf) {
  if (pBuf == NULL) {
    return;
  }

  dBufPrintStatis(pBuf);

  bool needRemoveFile = false;
  if (pBuf->pFile != NULL) {
    needRemoveFile = true;
    uDebug(
        "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page "
        "size:%.2f Kb, %s",
        pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
        listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);

    int32_t code = taosCloseFile(&pBuf->pFile);
    if (TSDB_CODE_SUCCESS != code) {
      uError("failed to close paged buffer file when destroying, path:%s, closeCode:%d, err:%s, %s", pBuf->path, code,
             tstrerror(code), pBuf->id);
    }
  } else {
    uDebug("Paged buffer closed, total:%.2f Kb, no file created, %s", pBuf->totalBufSize / 1024.0, pBuf->id);
  }

  // print the statistics information
  {
    SDiskbasedBufStatis* ps = &pBuf->statis;
    if (ps->loadPages == 0) {
      uDebug("Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages)", ps->getPages,
             ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f, ps->loadPages);
    } else {
      uDebug(
          "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPgSize:%.2f Kb",
          ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
          ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
    }
  }

  if (needRemoveFile) {
    int32_t ret = taosRemoveFile(pBuf->path);
    if (ret != 0) {  // print the error and discard this error info
      uDebug("WARNING tPage remove file failed. path=%s, code:%s", pBuf->path, strerror(ERRNO));
    }
  }

  taosMemoryFreeClear(pBuf->path);

  size_t n = taosArrayGetSize(pBuf->pIdList);
  for (int32_t i = 0; i < n; ++i) {
    SPageInfo* pi = taosArrayGetP(pBuf->pIdList, i);
    taosMemoryFreeClear(pi->pData);
    taosMemoryFreeClear(pi);
  }

  taosArrayDestroy(pBuf->pIdList);

  pBuf->lruList = tdListFree(pBuf->lruList);

  taosArrayDestroy(pBuf->emptyDummyIdList);
  taosArrayDestroy(pBuf->pFree);

  tSimpleHashCleanup(pBuf->all);

  taosMemoryFreeClear(pBuf->id);
  taosMemoryFreeClear(pBuf->assistBuf);
  taosDestroySpillCryptCtx(&pBuf->pCryptCtx);
  taosMemoryFreeClear(pBuf);
}

SPageInfo* getLastPageInfo(SArray* pList) {
  size_t     size = taosArrayGetSize(pList);
  SPageInfo* pPgInfo = taosArrayGetP(pList, size - 1);
  return pPgInfo;
}

int32_t getPageId(const SPageInfo* pPgInfo) { return pPgInfo->pageId; }

int32_t getBufPageSize(const SDiskbasedBuf* pBuf) { return pBuf->pageSize; }

int32_t getNumOfInMemBufPages(const SDiskbasedBuf* pBuf) { return pBuf->inMemPages; }

int32_t getNumOfResidentBufPages(const SDiskbasedBuf* pBuf) {
  return pBuf == NULL ? 0 : listNEles(pBuf->lruList);
}

bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf) { return pBuf->fileSize == 0; }

void setBufPageDirty(void* pPage, bool dirty) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);
  ppi->dirty = dirty;
}

int32_t setBufPageCompressOnDisk(SDiskbasedBuf* pBuf, bool comp) {
  pBuf->comp = comp;
  if (comp && (pBuf->assistBuf == NULL)) {
    pBuf->assistBuf = taosMemoryMalloc(pBuf->pageSize + 2);  // EXTRA BYTES
    if (pBuf->assistBuf == NULL) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t recycleBufPageInfo(SDiskbasedBuf* pBuf, SPageInfo* ppi, bool allowUsed) {
  if (pBuf == NULL || ppi == NULL || ppi->recycled || (!allowUsed && ppi->used)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if ((pBuf->pFreePageHead == NULL) != (pBuf->pFreePageTail == NULL) ||
      (pBuf->pFreePageHead != NULL && pBuf->pFreePageHead->pPrevFree != NULL) ||
      (pBuf->pFreePageTail != NULL && pBuf->pFreePageTail->pNextFree != NULL) ||
      ppi->pPrevFree != NULL || ppi->pNextFree != NULL) {
    return TSDB_CODE_APP_ERROR;
  }
  if ((ppi->pData == NULL) != (ppi->pn == NULL) || (ppi->pData == NULL && (ppi->used || ppi->dirty))) {
    return TSDB_CODE_APP_ERROR;
  }
  if (ppi->pn != NULL && *(SPageInfo**)ppi->pn->data != ppi) {
    return TSDB_CODE_APP_ERROR;
  }

  ppi->used = false;
  ppi->dirty = false;
  ppi->recycled = true;

  if (ppi->pData != NULL) {
    SListNode* pNode = tdListPopNode(pBuf->lruList, ppi->pn);
    taosMemoryFreeClear(ppi->pData);
    taosMemoryFreeClear(pNode);
    ppi->pn = NULL;
  }

  if (pBuf->pFreePageTail == NULL) {
    pBuf->pFreePageHead = ppi;
    ppi->pPrevFree = NULL;
  } else {
    ppi->pPrevFree = pBuf->pFreePageTail;
    pBuf->pFreePageTail->pNextFree = ppi;
  }
  pBuf->pFreePageTail = ppi;
  return TSDB_CODE_SUCCESS;
}

int32_t dBufSetBufPageRecycled(SDiskbasedBuf* pBuf, void* pPage) {
  if (pPage == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  return recycleBufPageInfo(pBuf, getPageInfoFromPayload(pPage), true);
}

int32_t dBufRecyclePageById(SDiskbasedBuf* pBuf, int32_t pageId) {
  if (pBuf == NULL || pageId < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SPageInfo** ppInfo = tSimpleHashGet(pBuf->all, &pageId, sizeof(pageId));
  if (ppInfo == NULL || *ppInfo == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return recycleBufPageInfo(pBuf, *ppInfo, false);
}

int32_t dBufRemovePageById(SDiskbasedBuf* pBuf, int32_t pageId) {
  if (pBuf == NULL || pageId < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SPageInfo** ppInfo = tSimpleHashGet(pBuf->all, &pageId, sizeof(pageId));
  if (ppInfo == NULL || *ppInfo == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  SPageInfo* pInfo = *ppInfo;
  if (pInfo->used) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pInfo->pn != NULL && *(SPageInfo**)pInfo->pn->data != pInfo) {
    return TSDB_CODE_APP_ERROR;
  }
  if (pInfo->recycled) {
    if (pBuf->pFreePageHead == NULL || pBuf->pFreePageTail == NULL ||
        pBuf->pFreePageHead->pPrevFree != NULL || pInfo->pPrevFree == pInfo ||
        (pInfo->pPrevFree == NULL && pBuf->pFreePageHead != pInfo) ||
        (pInfo->pPrevFree != NULL && pInfo->pPrevFree->pNextFree != pInfo) ||
        (pInfo->pNextFree != NULL && pInfo->pNextFree->pPrevFree != pInfo)) {
      return TSDB_CODE_APP_ERROR;
    }
  }
  int32_t last = taosArrayGetSize(pBuf->pIdList) - 1;
  if (pInfo->idListIndex < 0 || pInfo->idListIndex > last ||
      taosArrayGetP(pBuf->pIdList, pInfo->idListIndex) != pInfo) {
    return TSDB_CODE_APP_ERROR;
  }
  int32_t    code = tSimpleHashRemove(pBuf->all, &pageId, sizeof(pageId));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pInfo->pn != NULL) {
    SListNode* pNode = tdListPopNode(pBuf->lruList, pInfo->pn);
    taosMemoryFreeClear(pInfo->pData);
    taosMemoryFreeClear(pNode);
    pInfo->pn = NULL;
  } else if (pInfo->recycled) {
    if (pInfo->pPrevFree == NULL) {
      pBuf->pFreePageHead = pInfo->pNextFree;
      if (pBuf->pFreePageHead != NULL) {
        pBuf->pFreePageHead->pPrevFree = NULL;
      }
    } else {
      pInfo->pPrevFree->pNextFree = pInfo->pNextFree;
    }
    if (pInfo->pNextFree != NULL) {
      pInfo->pNextFree->pPrevFree = pInfo->pPrevFree;
    }
    if (pBuf->pFreePageTail == pInfo) {
      pBuf->pFreePageTail = pInfo->pPrevFree;
    }
  }
  if (pInfo->idListIndex != last) {
    SPageInfo* pLast = taosArrayGetP(pBuf->pIdList, last);
    memcpy(taosArrayGet(pBuf->pIdList, pInfo->idListIndex), &pLast, sizeof(pLast));
    pLast->idListIndex = pInfo->idListIndex;
  }
  taosArrayPop(pBuf->pIdList);
  pBuf->numOfPages -= 1;
  pBuf->totalBufSize -= pBuf->pageSize;

  // Return the disk extent only after the page has been removed from every live
  // index. If insertion into the free-list fails, the extent is leaked safely
  // rather than being reused while a stale page reference remains reachable.
  if (pInfo->offset >= 0 && pInfo->length > 0 && pBuf->pFree != NULL) {
    SFreeListItem extent = {.offset = pInfo->offset, .length = pInfo->length};
    if (taosArrayPush(pBuf->pFree, &extent) == NULL) {
      uError("failed to recycle disk extent, pageId:%d", pageId);
      taosMemoryFree(pInfo);
      return TSDB_CODE_SUCCESS;
    }
  }
  taosMemoryFree(pInfo);
  return TSDB_CODE_SUCCESS;
}

void dBufSetPrintInfo(SDiskbasedBuf* pBuf) { pBuf->printStatis = true; }

// Test-only: read the raw on-disk spill bytes (the AUTO_DEL temp file is
// unlinked, so it can only be read back through the open handle). Used to verify
// spilled pages are stored as ciphertext.
int64_t dbgReadDiskbasedBufFile(SDiskbasedBuf* pBuf, char* out, int64_t cap) {
  if (pBuf->pFile == NULL || out == NULL) {
    return -1;
  }
  int64_t size = (int64_t)pBuf->fileSize;
  if (size > cap) {
    size = cap;
  }
  if (taosLSeekFile(pBuf->pFile, 0, SEEK_SET) < 0) {
    return -1;
  }
  return taosReadFile(pBuf->pFile, out, size);
}

#if defined(BUILD_TEST)
void dbgFailNextDiskbasedBufFlushWrite(SDiskbasedBuf* pBuf) {
  if (pBuf != NULL) {
    pBuf->dbgFailNextFlushWrite = true;
  }
}

void dbgShortNextDiskbasedBufFlushWrite(SDiskbasedBuf* pBuf) {
  if (pBuf != NULL) {
    pBuf->dbgShortNextFlushWrite = true;
  }
}
#endif

SDiskbasedBufStatis getDBufStatis(const SDiskbasedBuf* pBuf) { return pBuf->statis; }

void dBufPrintStatis(const SDiskbasedBuf* pBuf) {
  if (!pBuf->printStatis) {
    return;
  }

  const SDiskbasedBufStatis* ps = &pBuf->statis;

#if 0
  printf(
      "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page size:%.2f "
      "Kb, %s\n",
      pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
      listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);
#endif

  if (ps->loadPages > 0) {
    (void)printf(
        "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPageSize:%.2f "
        "Kb\n",
        ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
        ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
  } else {
    // printf("no page loaded\n");
  }
}

void clearDiskbasedBuf(SDiskbasedBuf* pBuf) {
  if (pBuf == NULL) {
    return;
  }

  int64_t realSizeBefore = -1;
  if (pBuf->pFile != NULL && taosFStatFile(pBuf->pFile, &realSizeBefore, NULL) != TSDB_CODE_SUCCESS) {
    realSizeBefore = -1;
  }

  const SDiskbasedBufStatis* ps = &pBuf->statis;
  uDebug(
      "clear paged buffer begin, pages:%d, inMemPages:%d, fileSize:%" PRIu64 ", nextPos:%" PRIu64
      ", realSize:%" PRId64 ", get/release:%d/%d, flush/load:%d/%d, %s",
      pBuf->numOfPages, listNEles(pBuf->lruList), pBuf->fileSize, pBuf->nextPos, realSizeBefore, ps->getPages,
      ps->releasePages, ps->flushPages, ps->loadPages, pBuf->id);

  size_t n = taosArrayGetSize(pBuf->pIdList);
  for (int32_t i = 0; i < n; ++i) {
    SPageInfo* pi = taosArrayGetP(pBuf->pIdList, i);
    taosMemoryFreeClear(pi->pData);
    taosMemoryFreeClear(pi);
  }

  taosArrayClear(pBuf->pIdList);

  tdListEmpty(pBuf->lruList);
  pBuf->pFreePageHead = NULL;
  pBuf->pFreePageTail = NULL;

  taosArrayClear(pBuf->emptyDummyIdList);
  taosArrayClear(pBuf->pFree);

  tSimpleHashClear(pBuf->all);

  pBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pBuf->totalBufSize = 0;
  pBuf->allocateId = -1;
  pBuf->fileSize = 0;
  pBuf->nextPos = 0;

  if (pBuf->pFile != NULL) {
    int32_t code = taosFtruncateFile(pBuf->pFile, 0);
    if (code != TSDB_CODE_SUCCESS) {
      uWarn("failed to truncate paged buffer file, path:%s, code:%s, %s", pBuf->path, tstrerror(code), pBuf->id);
    }
  }

  int64_t realSizeAfter = -1;
  if (pBuf->pFile != NULL && taosFStatFile(pBuf->pFile, &realSizeAfter, NULL) != TSDB_CODE_SUCCESS) {
    realSizeAfter = -1;
  }

  uDebug("clear paged buffer end, pages:%d, inMemPages:%d, fileSize:%" PRIu64 ", nextPos:%" PRIu64
         ", realSize:%" PRId64 ", %s",
         pBuf->numOfPages, listNEles(pBuf->lruList), pBuf->fileSize, pBuf->nextPos, realSizeAfter, pBuf->id);
}
