#define _DEFAULT_SOURCE
#include "tpagedbuf.h"
#include "taoserror.h"
#include "tcompression.h"
#include "thash.h"
#include "tlog.h"

#define GET_DATA_PAYLOAD(_p)          ((char*)(_p)->pData + POINTER_BYTES)
#define NO_IN_MEM_AVAILABLE_PAGES(_b) (listNEles((_b)->lruList) >= (_b)->inMemPages)

typedef struct SPageDiskInfo {
  int64_t offset;
  int32_t length;
} SPageDiskInfo, SFreeListItem;

struct SPageInfo {
  SListNode* pn;  // point to list node struct
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
  int32_t   pageSize;    // current used page size
  int32_t   inMemPages;  // numOfPages that are allocated in memory
  SList*    freePgList;  // free page list
  SHashObj* groupSet;    // id hash table, todo remove it
  SHashObj* all;
  SList*    lruList;
  void*     emptyDummyIdList;  // dummy id list
  void*     assistBuf;         // assistant buffer for compress/decompress data
  SArray*   pFree;             // free area in file
  bool      comp;              // compressed before flushed to disk
  uint64_t  nextPos;           // next page flush position

  char*     id;          // for debug purpose
  bool      printStatis;  // Print statistics info when closing this buffer.
  SDiskbasedBufStatis statis;
};

static int32_t createDiskFile(SDiskbasedBuf* pBuf) {
  pBuf->pFile = taosOpenFile(pBuf->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
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

static uint64_t allocatePositionInFile(SDiskbasedBuf* pBuf, size_t size) {
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

static void setPageNotInBuf(SPageInfo* pPageInfo) { pPageInfo->pData = NULL; }

static FORCE_INLINE size_t getAllocPageSize(int32_t pageSize) { return pageSize + POINTER_BYTES + 2; }

/**
 *   +--------------------------+-------------------+--------------+
 *   | PTR to SPageInfo (8bytes)| Payload (PageSize)| 2 Extra Bytes|
 *   +--------------------------+-------------------+--------------+
 * @param pBuf
 * @param pg
 * @return
 */
static char* doFlushPageToDisk(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  assert(!pg->used && pg->pData != NULL);

  int32_t size = pBuf->pageSize;
  char*   t = NULL;
  if (pg->offset == -1 || pg->dirty) {
    void* payload = GET_DATA_PAYLOAD(pg);
    t = doCompressData(payload, pBuf->pageSize, &size, pBuf);
    assert(size >= 0);
  }

  // this page is flushed to disk for the first time
  if (pg->dirty) {
    if (pg->offset == -1) {
      assert(pg->dirty == true);

      pg->offset = allocatePositionInFile(pBuf, size);
      pBuf->nextPos += size;

      int32_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
      if (ret == -1) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return NULL;
      }

      ret = (int32_t)taosWriteFile(pBuf->pFile, t, size);
      if (ret != size) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return NULL;
      }

      if (pBuf->fileSize < pg->offset + size) {
        pBuf->fileSize = pg->offset + size;
      }

      pBuf->statis.flushBytes += size;
      pBuf->statis.flushPages += 1;
    } else {
      // length becomes greater, current space is not enough, allocate new place, otherwise, do nothing
      if (pg->length < size) {
        // 1. add current space to free list
        SPageDiskInfo dinfo = {.length = pg->length, .offset = pg->offset};
        taosArrayPush(pBuf->pFree, &dinfo);

        // 2. allocate new position, and update the info
        pg->offset = allocatePositionInFile(pBuf, size);
        pBuf->nextPos += size;
      }

      // 3. write to disk.
      int32_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
      if (ret == -1) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return NULL;
      }

      ret = (int32_t)taosWriteFile(pBuf->pFile, t, size);
      if (ret != size) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return NULL;
      }

      if (pBuf->fileSize < pg->offset + size) {
        pBuf->fileSize = pg->offset + size;
      }

      pBuf->statis.flushBytes += size;
      pBuf->statis.flushPages += 1;
    }
  } else {  // NOTE: the size may be -1, the this recycle page has not been flushed to disk yet.
    size = pg->length;
    if (size == -1) {
      printf("----\n");
    }
  }

  ASSERT(size > 0 || (pg->offset == -1 && pg->length == -1));

  char* pDataBuf = pg->pData;
  memset(pDataBuf, 0, getAllocPageSize(pBuf->pageSize));

  pg->length = size;  // on disk size
  return pDataBuf;
}

static char* flushPageToDisk(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  assert(((int64_t)pBuf->numOfPages * pBuf->pageSize) == pBuf->totalBufSize && pBuf->numOfPages >= pBuf->inMemPages);

  if (pBuf->pFile == NULL) {
    if ((ret = createDiskFile(pBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  char* p = doFlushPageToDisk(pBuf, pg);
  setPageNotInBuf(pg);
  pg->dirty = false;

  return p;
}

// load file block data in disk
static int32_t loadPageFromDisk(SDiskbasedBuf* pBuf, SPageInfo* pg) {
  int32_t ret = taosLSeekFile(pBuf->pFile, pg->offset, SEEK_SET);
  if (ret == -1) {
    ret = TAOS_SYSTEM_ERROR(errno);
    return ret;
  }

  void* pPage = (void*)GET_DATA_PAYLOAD(pg);
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

static SIDList addNewGroup(SDiskbasedBuf* pBuf, int32_t groupId) {
  assert(taosHashGet(pBuf->groupSet, (const char*)&groupId, sizeof(int32_t)) == NULL);

  SArray* pa = taosArrayInit(1, POINTER_BYTES);
  int32_t ret = taosHashPut(pBuf->groupSet, (const char*)&groupId, sizeof(int32_t), &pa, POINTER_BYTES);
  assert(ret == 0);

  return pa;
}

static SPageInfo* registerPage(SDiskbasedBuf* pBuf, int32_t groupId, int32_t pageId) {
  SIDList list = NULL;

  char** p = taosHashGet(pBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    list = addNewGroup(pBuf, groupId);
  } else {
    list = (SIDList)(*p);
  }

  pBuf->numOfPages += 1;

  SPageInfo* ppi = taosMemoryMalloc(sizeof(SPageInfo));

  ppi->pageId = pageId;
  ppi->pData  = NULL;
  ppi->offset = -1;
  ppi->length = -1;
  ppi->used   = true;
  ppi->pn     = NULL;
  ppi->dirty  = false;

  return *(SPageInfo**)taosArrayPush(list, &ppi);
}

static SListNode* getEldestUnrefedPage(SDiskbasedBuf* pBuf) {
  SListIter iter = {0};
  tdListInitIter(pBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while ((pn = tdListNext(&iter)) != NULL) {
    SPageInfo* pageInfo = *(SPageInfo**)pn->data;
    assert(pageInfo->pageId >= 0 && pageInfo->pn == pn);

    if (!pageInfo->used) {
      //      printf("%d is chosen\n", pageInfo->pageId);
      break;
    } else {
      //      printf("page %d is used, dirty:%d\n", pageInfo->pageId, pageInfo->dirty);
    }
  }

  //  int32_t pos = listNEles(pBuf->lruList);
  //  SListIter iter1 = {0};
  //  tdListInitIter(pBuf->lruList, &iter1, TD_LIST_BACKWARD);
  //  SListNode* pn1 = NULL;
  //  while((pn1 = tdListNext(&iter1)) != NULL) {
  //    SPageInfo* pageInfo = *(SPageInfo**) pn1->data;
  //    printf("page %d is used, dirty:%d, pos:%d\n", pageInfo->pageId, pageInfo->dirty, pos - 1);
  //    pos -= 1;
  //  }

  return pn;
}

static char* evacOneDataPage(SDiskbasedBuf* pBuf) {
  char*      bufPage = NULL;
  SListNode* pn = getEldestUnrefedPage(pBuf);

  // all pages are referenced by user, try to allocate new space
  if (pn == NULL) {
    assert(0);
    int32_t prev = pBuf->inMemPages;

    // increase by 50% of previous mem pages
    pBuf->inMemPages = (int32_t)(pBuf->inMemPages * 1.5f);

    //    qWarn("%p in memory buf page not sufficient, expand from %d to %d, page size:%d", pBuf, prev,
    //          pBuf->inMemPages, pBuf->pageSize);
  } else {
    tdListPopNode(pBuf->lruList, pn);

    SPageInfo* d = *(SPageInfo**)pn->data;
    assert(d->pn == pn);

    d->pn = NULL;
    taosMemoryFreeClear(pn);

    bufPage = flushPageToDisk(pBuf, d);
  }

  return bufPage;
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
  char*   p = (char *)page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**)p)[0];
  return ppi;
}

int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int32_t inMemBufSize, const char* id,
                           const char* dir) {
  *pBuf = taosMemoryCalloc(1, sizeof(SDiskbasedBuf));

  SDiskbasedBuf* pPBuf = *pBuf;
  if (pPBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pPBuf->pageSize = pagesize;
  pPBuf->numOfPages   = 0;  // all pages are in buffer in the first place
  pPBuf->totalBufSize = 0;
  pPBuf->inMemPages = inMemBufSize / pagesize;  // maximum allowed pages, it is a soft limit.
  pPBuf->allocateId = -1;
  pPBuf->pFile    = NULL;
  pPBuf->id       = strdup(id);
  pPBuf->fileSize = 0;
  pPBuf->pFree = taosArrayInit(4, sizeof(SFreeListItem));
  pPBuf->freePgList = tdListNew(POINTER_BYTES);

  // at least more than 2 pages must be in memory
  assert(inMemBufSize >= pagesize * 2);

  pPBuf->lruList = tdListNew(POINTER_BYTES);

  // init id hash table
  _hash_fn_t fn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  pPBuf->groupSet = taosHashInit(10, fn, true, false);
  pPBuf->assistBuf = taosMemoryMalloc(pPBuf->pageSize + 2);  // EXTRA BYTES
  pPBuf->all = taosHashInit(10, fn, true, false);

  char path[PATH_MAX] = {0};
  taosGetTmpfilePath(dir, "paged-buf", path);
  pPBuf->path = strdup(path);

  pPBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

  //  qDebug("QInfo:0x%"PRIx64" create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", qId,
  //  pPBuf->pageSize,
  //         pPBuf->inMemPages, pPBuf->path);

  return TSDB_CODE_SUCCESS;
}

void* getNewBufPage(SDiskbasedBuf* pBuf, int32_t groupId, int32_t* pageId) {
  pBuf->statis.getPages += 1;

  char* availablePage = NULL;
  if (NO_IN_MEM_AVAILABLE_PAGES(pBuf)) {
    availablePage = evacOneDataPage(pBuf);

    // Failed to allocate a new buffer page, and there is an error occurs.
    if (availablePage == NULL) {
      assert(0);
      return NULL;
    }
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
    pi = registerPage(pBuf, groupId, *pageId);

    // add to hash map
    taosHashPut(pBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);
    pBuf->totalBufSize += pBuf->pageSize;
  }

  // add to LRU list
  assert(listNEles(pBuf->lruList) < pBuf->inMemPages && pBuf->inMemPages > 0);
  lruListPushFront(pBuf->lruList, pi);

  // allocate buf
  if (availablePage == NULL) {
    pi->pData = taosMemoryCalloc(1, getAllocPageSize(pBuf->pageSize));  // add extract bytes in case of zipped buffer increased.
  } else {
    pi->pData = availablePage;
  }

  ((void**)pi->pData)[0] = pi;
  return (void*)(GET_DATA_PAYLOAD(pi));
}

void* getBufPage(SDiskbasedBuf* pBuf, int32_t id) {
  assert(pBuf != NULL && id >= 0);
  pBuf->statis.getPages += 1;

  SPageInfo** pi = taosHashGet(pBuf->all, &id, sizeof(int32_t));
  assert(pi != NULL && *pi != NULL);

  if ((*pi)->pData != NULL) {  // it is in memory
    // no need to update the LRU list if only one page exists
    if (pBuf->numOfPages == 1) {
      (*pi)->used = true;
      return (void*)(GET_DATA_PAYLOAD(*pi));
    }

    SPageInfo** pInfo = (SPageInfo**)((*pi)->pn->data);
    assert(*pInfo == *pi);

    lruListMoveToFront(pBuf->lruList, (*pi));
    (*pi)->used = true;

    return (void*)(GET_DATA_PAYLOAD(*pi));
  } else {  // not in memory
    assert((*pi)->pData == NULL && (*pi)->pn == NULL && (((*pi)->length >= 0 && (*pi)->offset >= 0) || ((*pi)->length == -1 && (*pi)->offset == -1)));

    char* availablePage = NULL;
    if (NO_IN_MEM_AVAILABLE_PAGES(pBuf)) {
      availablePage = evacOneDataPage(pBuf);
      if (availablePage == NULL) {
        return NULL;
      }
    }

    if (availablePage == NULL) {
      (*pi)->pData = taosMemoryCalloc(1, getAllocPageSize(pBuf->pageSize));
    } else {
      (*pi)->pData = availablePage;
    }

    // set the ptr to the new SPageInfo
    ((void**)((*pi)->pData))[0] = (*pi);

    lruListPushFront(pBuf->lruList, *pi);
    (*pi)->used = true;

    // some data has been flushed to disk, and needs to be loaded into buffer again.
    if ((*pi)->length > 0 && (*pi)->offset >= 0) {
      int32_t code = loadPageFromDisk(pBuf, *pi);
      if (code != 0) {
        return NULL;
      }
    }

    return (void*)(GET_DATA_PAYLOAD(*pi));
  }
}

void releaseBufPage(SDiskbasedBuf* pBuf, void* page) {
  assert(pBuf != NULL && page != NULL);
  SPageInfo* ppi = getPageInfoFromPayload(page);
  releaseBufPageInfo(pBuf, ppi);
}

void releaseBufPageInfo(SDiskbasedBuf* pBuf, SPageInfo* pi) {
  assert(pi->pData != NULL && pi->used == true);

  pi->used = false;
  pBuf->statis.releasePages += 1;
}

size_t getNumOfBufGroupId(const SDiskbasedBuf* pBuf) { return taosHashGetSize(pBuf->groupSet); }

size_t getTotalBufSize(const SDiskbasedBuf* pBuf) { return (size_t)pBuf->totalBufSize; }

SIDList getDataBufPagesIdList(SDiskbasedBuf* pBuf, int32_t groupId) {
  assert(pBuf != NULL);

  char** p = taosHashGet(pBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    return pBuf->emptyDummyIdList;
  } else {
    return (SArray*)(*p);
  }
}

void destroyDiskbasedBuf(SDiskbasedBuf* pBuf) {
  if (pBuf == NULL) {
    return;
  }

  dBufPrintStatis(pBuf);

  if (pBuf->pFile != NULL) {
    uDebug(
        "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page "
        "size:%.2f Kb, %s\n",
        pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
        listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);

    taosCloseFile(&pBuf->pFile);
  } else {
    uDebug("Paged buffer closed, total:%.2f Kb, no file created, %s", pBuf->totalBufSize / 1024.0, pBuf->id);
  }

  // print the statistics information
  {
    SDiskbasedBufStatis* ps = &pBuf->statis;
    uDebug(
        "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPageSize:%.2f "
        "Kb\n",
        ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f,
        ps->loadPages, ps->loadBytes / (1024.0 * ps->loadPages));
  }

  taosRemoveFile(pBuf->path);
  taosMemoryFreeClear(pBuf->path);

  SArray** p = taosHashIterate(pBuf->groupSet, NULL);
  while (p) {
    size_t n = taosArrayGetSize(*p);
    for (int32_t i = 0; i < n; ++i) {
      SPageInfo* pi = taosArrayGetP(*p, i);
      taosMemoryFreeClear(pi->pData);
      taosMemoryFreeClear(pi);
    }

    taosArrayDestroy(*p);
    p = taosHashIterate(pBuf->groupSet, p);
  }

  tdListFree(pBuf->lruList);
  tdListFree(pBuf->freePgList);

  taosArrayDestroy(pBuf->emptyDummyIdList);
  taosArrayDestroy(pBuf->pFree);

  taosHashCleanup(pBuf->groupSet);
  taosHashCleanup(pBuf->all);

  taosMemoryFreeClear(pBuf->id);
  taosMemoryFreeClear(pBuf->assistBuf);
  taosMemoryFreeClear(pBuf);
}

SPageInfo* getLastPageInfo(SIDList pList) {
  size_t     size = taosArrayGetSize(pList);
  SPageInfo* pPgInfo = taosArrayGetP(pList, size - 1);
  return pPgInfo;
}

int32_t getPageId(const SPageInfo* pPgInfo) {
  ASSERT(pPgInfo != NULL);
  return pPgInfo->pageId;
}

int32_t getBufPageSize(const SDiskbasedBuf* pBuf) { return pBuf->pageSize; }

int32_t getNumOfInMemBufPages(const SDiskbasedBuf* pBuf) { return pBuf->inMemPages; }

bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf) { return pBuf->fileSize == 0; }

void setBufPageDirty(void* pPage, bool dirty) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);
  ppi->dirty = dirty;
}

void setBufPageCompressOnDisk(SDiskbasedBuf* pBuf, bool comp) { pBuf->comp = comp; }

void dBufSetBufPageRecycled(SDiskbasedBuf* pBuf, void* pPage) {
  SPageInfo* ppi = getPageInfoFromPayload(pPage);

  ppi->used = false;
  ppi->dirty = false;

  // add this pageinfo into the free page info list
  SListNode* pNode = tdListPopNode(pBuf->lruList, ppi->pn);
  taosMemoryFreeClear(ppi->pData);
  taosMemoryFreeClear(pNode);

  tdListAppend(pBuf->freePgList, &ppi);
}

void dBufSetPrintInfo(SDiskbasedBuf* pBuf) { pBuf->printStatis = true; }

SDiskbasedBufStatis getDBufStatis(const SDiskbasedBuf* pBuf) { return pBuf->statis; }

void dBufPrintStatis(const SDiskbasedBuf* pBuf) {
  if (!pBuf->printStatis) {
    return;
  }

  const SDiskbasedBufStatis* ps = &pBuf->statis;

  printf(
      "Paged buffer closed, total:%.2f Kb (%d Pages), inmem size:%.2f Kb (%d Pages), file size:%.2f Kb, page size:%.2f "
      "Kb, %s\n",
      pBuf->totalBufSize / 1024.0, pBuf->numOfPages, listNEles(pBuf->lruList) * pBuf->pageSize / 1024.0,
      listNEles(pBuf->lruList), pBuf->fileSize / 1024.0, pBuf->pageSize / 1024.0f, pBuf->id);

  printf(
      "Get/Release pages:%d/%d, flushToDisk:%.2f Kb (%d Pages), loadFromDisk:%.2f Kb (%d Pages), avgPageSize:%.2f Kb\n",
      ps->getPages, ps->releasePages, ps->flushBytes / 1024.0f, ps->flushPages, ps->loadBytes / 1024.0f, ps->loadPages,
      ps->loadBytes / (1024.0 * ps->loadPages));
}
