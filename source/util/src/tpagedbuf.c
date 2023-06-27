#define _DEFAULT_SOURCE
#include "tpagedbuf.h"
#include "taoserror.h"
#include "tcompression.h"
#include "tsimplehash.h"
#include "tlog.h"

#define GET_PAYLOAD_DATA(_p)           ((char*)(_p)->pData + POINTER_BYTES)
#define BUF_PAGE_IN_MEM(_p)            ((_p)->pData != NULL)
#define CLEAR_BUF_PAGE_IN_MEM_FLAG(_p) ((_p)->pData = NULL)
#define HAS_DATA_IN_DISK(_p)           ((_p)->offset >= 0)
#define NO_IN_MEM_AVAILABLE_PAGES(_b)  (listNEles((_b)->lruList) >= (_b)->inMemPages)

typedef struct SPageDiskInfo {
  int64_t offset;
  int32_t length;
} SPageDiskInfo, SFreeListItem;

struct SPageInfo {
  SListNode* pn;  // point to list node struct. it is NULL when the page is evicted from the in-memory buffer
  void*      pData;
  int64_t    offset;
  int32_t    pageId;
  int32_t    length : 29;
  bool       used : 1;   // set current page is in used
  bool       dirty : 1;  // set current buffer page is dirty or not
};

struct SDiskbasedBuf {
  int32_t   numOfPages;
  int64_t   totalBufSize;
  uint64_t  fileSize;  // disk file size
  TdFilePtr pFile;
  int32_t   allocateId;  // allocated page id
  char*     path;        // file path
  char*     prefix;      // file name prefix
  int32_t   pageSize;    // current used page size
  int32_t   inMemPages;  // numOfPages that are allocated in memory
  SList*    freePgList;  // free page list
  SArray*   pIdList;     // page id list
  SSHashObj*all;
  SList*    lruList;
  void*     emptyDummyIdList;  // dummy id list
  void*     assistBuf;         // assistant buffer for compress/decompress data
  SArray*   pFree;             // free area in file
  bool      comp;              // compressed before flushed to disk
  uint64_t  nextPos;           // next page flush position

  char*               id;           // for debug purpose
  bool                printStatis;  // Print statistics info when closing this buffer.
  SDiskbasedBufStatis statis;
};

static int32_t createDiskFile(SDiskbasedBuf* pBuf) {
  if (pBuf->path == NULL) {  // prepare the file name when needed it
    char path[PATH_MAX] = {0};
    taosGetTmpfilePath(pBuf->prefix, "paged-buf", path);
    pBuf->path = taosStrdup(path);
    if (pBuf->path == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  pBuf->pFile =
      taosOpenFile(pBuf->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  if (pBuf->pFile == NULL) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  if (!pBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsCompressString(data, srcSize, 1, pBuf->assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, pBuf->assistBuf, *dst);
  return data;
}

static char* doDecompressData(void* data, int32_t srcSize, int32_t* dst, SDiskbasedBuf* pBuf) {  // do nothing
  if (!pBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsDecompressString(data, srcSize, 1, pBuf->assistBuf, pBuf->pageSize, ONE_STAGE_COMP, NULL, 0);
  if (*dst > 0) {
    memcpy(data, pBuf->assistBuf, *dst);
  }
  return data;
}

static uint64_t allocateNewPositionInFile(SDiskbasedBuf* pBuf, size_t size) {
  if (pBuf->pFree == NULL) {
    return pBuf->nextPos;
  } else {
    int32_t offset = -1;

    size_t num = taosArrayGetSize(pBuf->pFree);
    for (int32_t i = 0; i < num; ++i) {
      SFreeListItem* pi = taosArrayGet(pBuf->pFree, i);
      if (pi->length >= size) {
        offset = pi->offset;
        pi->offset += (int32_t)size;
        pi->length -= (int32_t)size;

        return offset;
      }
    }

    // no available recycle space, allocate new area in file
    return pBuf->nextPos;
  }
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

static int32_t doFlushBufPageImpl(SDiskbasedBuf* pBuf, int64_t offset, const char* pData, int32_t size) {
  int32_t ret = taosLSeekFile(pBuf->pFile, offset, SEEK_SET);
  if (ret == -1) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  ret = (int32_t)taosWriteFile(pBuf->pFile, pData, size);
  if (ret != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  // extend the file
  if (pBuf->fileSize < offset + size) {
    pBuf->fileSize = offset + size;
  }

  pBuf->statis.flushBytes += size;
  pBuf->statis.flushPages += 1;

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

  // this page is flushed to disk for the first time
  if (pg->dirty) {
    if (!HAS_DATA_IN_DISK(pg)) {
      offset = allocateNewPositionInFile(pBuf, size);
      pBuf->nextPos += size;

      int32_t code = doFlushBufPageImpl(pBuf, offset, t, size);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    } else {
      // length becomes greater, current space is not enough, allocate new place, otherwise, do nothing
      if (pg->length < size) {
        // 1. add current space to free list
        SPageDiskInfo dinfo = {.length = pg->length, .offset = offset};
        taosArrayPush(pBuf->pFree, &dinfo);

        // 2. allocate new position, and update the info
        offset = allocateNewPositionInFile(pBuf, size);
        pBuf->nextPos += size;
      }

      int32_t code = doFlushBufPageImpl(pBuf, offset, t, size);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
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

  int32_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
  if (ret == -1) {
    ret = TAOS_SYSTEM_ERROR(errno);
    return ret;
  }

  void* pPage = (void*)GET_PAYLOAD_DATA(pg);
  ret = (int32_t)taosReadFile(pBuf->pFile, pPage, pg->length);
  if (ret != pg->length) {
    ret = TAOS_SYSTEM_ERROR(errno);
    return ret;
  }

  pBuf->statis.loadBytes += pg->length;
  pBuf->statis.loadPages += 1;

  int32_t fullSize = 0;
  doDecompressData(pPage, pg->length, &fullSize, pBuf);
  return 0;
}

static SPageInfo* registerNewPageInfo(SDiskbasedBuf* pBuf, int32_t pageId) {
  pBuf->numOfPages += 1;

  SPageInfo* ppi = taosMemoryMalloc(sizeof(SPageInfo));
  if (ppi == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  ppi->pageId = pageId;
  ppi->pData = NULL;
  ppi->offset = -1;
  ppi->length = -1;
  ppi->used = true;
  ppi->pn = NULL;
  ppi->dirty = false;

  return *(SPageInfo**)taosArrayPush(pBuf->pIdList, &ppi);
}

static SListNode* getEldestUnrefedPage(SDiskbasedBuf* pBuf) {
  SListIter iter = {0};
  tdListInitIter(pBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while ((pn = tdListNext(&iter)) != NULL) {
    SPageInfo* pageInfo = *(SPageInfo**)pn->data;

    SPageInfo* p = *(SPageInfo**)(pageInfo->pData);
    ASSERT(pageInfo->pageId >= 0 && pageInfo->pn == pn && p == pageInfo);

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
  tdListPopNode(pBuf->lruList, pn);

  SPageInfo* d = *(SPageInfo**)pn->data;

  d->pn = NULL;
  taosMemoryFreeClear(pn);

  return flushBufPage(pBuf, d);
}

static void lruListPushFront(SList* pList, SPageInfo* pi) {
  tdListPrepend(pList, &pi);
  SListNode* front = tdListGetHead(pList);
  pi->pn = front;
}

static void lruListMoveToFront(SList* pList, SPageInfo* pi) {
  tdListPopNode(pList, pi->pn);
  tdListPrependNode(pList, pi->pn);
}

static SPageInfo* getPageInfoFromPayload(void* page) {
  char* p = (char*)page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**)p)[0];
  return ppi;
}

int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int32_t inMemBufSize, const char* id,
                           const char* dir) {
  *pBuf = taosMemoryCalloc(1, sizeof(SDiskbasedBuf));

  SDiskbasedBuf* pPBuf = *pBuf;
  if (pPBuf == NULL) {
    goto _error;
  }

  pPBuf->pageSize = pagesize;
  pPBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pPBuf->totalBufSize = 0;
  pPBuf->allocateId = -1;
  pPBuf->pFile = NULL;
  pPBuf->id = taosStrdup(id);
  pPBuf->fileSize = 0;
  pPBuf->pFree = taosArrayInit(4, sizeof(SFreeListItem));
  pPBuf->freePgList = tdListNew(POINTER_BYTES);

  // at least more than 2 pages must be in memory
  if (inMemBufSize < pagesize * 2) {
    inMemBufSize = pagesize * 2;
  }

  pPBuf->inMemPages = inMemBufSize / pagesize;  // maximum allowed pages, it is a soft limit.
  pPBuf->lruList = tdListNew(POINTER_BYTES);
  if (pPBuf->lruList == NULL) {
    goto _error;
  }

  // init id hash table
  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  pPBuf->pIdList = taosArrayInit(4, POINTER_BYTES);
  if (pPBuf->pIdList == NULL) {
    goto _error;
  }

  pPBuf->all = tSimpleHashInit(64, fn);
  if (pPBuf->all == NULL) {
    goto _error;
  }

  pPBuf->prefix = (char*)dir;
  pPBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

  //  qDebug("QInfo:0x%"PRIx64" create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", qId,
  //  pPBuf->pageSize, pPBuf->inMemPages, pPBuf->path);

  return TSDB_CODE_SUCCESS;
_error:
  destroyDiskbasedBuf(pPBuf);
  return TSDB_CODE_OUT_OF_MEMORY;
}

static char* doExtractPage(SDiskbasedBuf* pBuf, bool* newPage) {
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
    if (availablePage == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
    }
    *newPage = true;
  }

  return availablePage;
}

void* getNewBufPage(SDiskbasedBuf* pBuf, int32_t* pageId) {
  pBuf->statis.getPages += 1;

  bool newPage = false;
  char* availablePage = doExtractPage(pBuf, &newPage);
  if (availablePage == NULL) {
    return NULL;
  }

  SPageInfo* pi = NULL;
  if (listNEles(pBuf->freePgList) != 0) {
    SListNode* pItem = tdListPopHead(pBuf->freePgList);
    pi = *(SPageInfo**)pItem->data;
    pi->used = true;
    *pageId = pi->pageId;
    taosMemoryFreeClear(pItem);
  } else {  // create a new pageinfo
    // register new id in this group
    *pageId = (++pBuf->allocateId);

    // register page id info
    pi = registerNewPageInfo(pBuf, *pageId);
    if (pi == NULL) {
      if (newPage) {
        taosMemoryFree(availablePage);
      }
      return NULL;
    }

    // add to hash map
    tSimpleHashPut(pBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);
    pBuf->totalBufSize += pBuf->pageSize;
  }

  // add to LRU list
  lruListPushFront(pBuf->lruList, pi);
  pi->pData = availablePage;

  ((void**)pi->pData)[0] = pi;
#ifdef BUF_PAGE_DEBUG
  uDebug("page_getNewBufPage , pi->pData:%p, pageId:%d, offset:%" PRId64, pi->pData, pi->pageId, pi->offset);
#endif

  return (void*)(GET_PAYLOAD_DATA(pi));
}

void* getBufPage(SDiskbasedBuf* pBuf, int32_t id) {
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

    SPageInfo** pInfo = (SPageInfo**)((*pi)->pn->data);
    if (*pInfo != *pi) {
      terrno = TSDB_CODE_APP_ERROR;
      uError("inconsistently data in paged buffer, pInfo:%p, pi:%p, %s", *pInfo, *pi, pBuf->id);
      return NULL;
    }

    lruListMoveToFront(pBuf->lruList, (*pi));
    (*pi)->used = true;

#ifdef BUF_PAGE_DEBUG
    uDebug("page_getBufPage1 pageId:%d, offset:%" PRId64, (*pi)->pageId, (*pi)->offset);
#endif
    return (void*)(GET_PAYLOAD_DATA(*pi));
  } else {  // not in memory
    ASSERT((!BUF_PAGE_IN_MEM(*pi)) && (*pi)->pn == NULL &&
           (((*pi)->length >= 0 && (*pi)->offset >= 0) || ((*pi)->length == -1 && (*pi)->offset == -1)));

    bool newPage = false;
    (*pi)->pData = doExtractPage(pBuf, &newPage);

    // failed to evict buffer page, return with error code.
    if ((*pi)->pData == NULL) {
      return NULL;
    }

    // set the ptr to the new SPageInfo
    ((void**)((*pi)->pData))[0] = (*pi);

    lruListPushFront(pBuf->lruList, *pi);
    (*pi)->used = true;

    // some data has been flushed to disk, and needs to be loaded into buffer again.
    if (HAS_DATA_IN_DISK(*pi)) {
      int32_t code = loadPageFromDisk(pBuf, *pi);
      if (code != 0) {
        if (newPage) {
          taosMemoryFree((*pi)->pData);
        }

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

    taosCloseFile(&pBuf->pFile);
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
      uDebug("WARNING tPage remove file failed. path=%s, code:%s", pBuf->path, strerror(errno));
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

  tdListFree(pBuf->lruList);
  tdListFree(pBuf->freePgList);

  taosArrayDestroy(pBuf->emptyDummyIdList);
  taosArrayDestroy(pBuf->pFree);

  tSimpleHashCleanup(pBuf->all);

  taosMemoryFreeClear(pBuf->id);
  taosMemoryFreeClear(pBuf->assistBuf);
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

bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf) { return pBuf->fileSize == 0; }

void setBufPageDirty(void* pPage, bool dirty) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);
  ppi->dirty = dirty;
}

void setBufPageCompressOnDisk(SDiskbasedBuf* pBuf, bool comp) {
  pBuf->comp = comp;
  if (comp  && (pBuf->assistBuf == NULL)) {
    pBuf->assistBuf = taosMemoryMalloc(pBuf->pageSize + 2);  // EXTRA BYTES
  }
}

void dBufSetBufPageRecycled(SDiskbasedBuf* pBuf, void* pPage) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);

  ppi->used = false;
  ppi->dirty = false;

  // add this pageinfo into the free page info list
  SListNode* pNode = tdListPopNode(pBuf->lruList, ppi->pn);
  taosMemoryFreeClear(ppi->pData);
  taosMemoryFreeClear(pNode);
  ppi->pn = NULL;

  tdListAppend(pBuf->freePgList, &ppi);
}

void dBufSetPrintInfo(SDiskbasedBuf* pBuf) { pBuf->printStatis = true; }

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
    printf(
        "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPageSize:%.2f "
        "Kb\n",
        ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
        ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
  } else {
    // printf("no page loaded\n");
  }
}

void clearDiskbasedBuf(SDiskbasedBuf* pBuf) {
  size_t n = taosArrayGetSize(pBuf->pIdList);
  for (int32_t i = 0; i < n; ++i) {
    SPageInfo* pi = taosArrayGetP(pBuf->pIdList, i);
    taosMemoryFreeClear(pi->pData);
    taosMemoryFreeClear(pi);
  }

  taosArrayClear(pBuf->pIdList);

  tdListEmpty(pBuf->lruList);
  tdListEmpty(pBuf->freePgList);

  taosArrayClear(pBuf->emptyDummyIdList);
  taosArrayClear(pBuf->pFree);

  tSimpleHashClear(pBuf->all);

  pBuf->numOfPages = 0;  // all pages are in buffer in the first place
  pBuf->totalBufSize = 0;
  pBuf->allocateId = -1;
  pBuf->fileSize = 0;
}
