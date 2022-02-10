#include "tpagedfile.h"
#include "thash.h"
#include "stddef.h"
#include "taoserror.h"
#include "tcompression.h"

#define GET_DATA_PAYLOAD(_p) ((char *)(_p)->pData + POINTER_BYTES)
#define NO_IN_MEM_AVAILABLE_PAGES(_b) (listNEles((_b)->lruList) >= (_b)->inMemPages)

typedef struct SFreeListItem {
  int32_t offset;
  int32_t len;
} SFreeListItem;

typedef struct SPageDiskInfo {
  int64_t  offset;
  int32_t  length;
} SPageDiskInfo;

typedef struct SPageInfo {
  SListNode*    pn;       // point to list node
  void*         pData;
  int64_t       offset;
  int32_t       pageId;
  int32_t       length:30;
  bool          used:1;     // set current page is in used
  bool          dirty:1;    // set current buffer page is dirty or not
} SPageInfo;

typedef struct SDiskbasedBufStatis {
  int32_t flushBytes;
  int32_t loadBytes;
  int32_t getPages;
  int32_t releasePages;
  int32_t flushPages;
} SDiskbasedBufStatis;

typedef struct SDiskbasedBuf {
  int32_t   numOfPages;
  int64_t   totalBufSize;
  uint64_t  fileSize;            // disk file size
  FILE*     file;
  int32_t   allocateId;          // allocated page id
  char*     path;                // file path
  int32_t   pageSize;            // current used page size
  int32_t   inMemPages;          // numOfPages that are allocated in memory
  SHashObj* groupSet;            // id hash table
  SHashObj* all;
  SList*    lruList;
  void*     emptyDummyIdList;    // dummy id list
  void*     assistBuf;           // assistant buffer for compress/decompress data
  SArray*   pFree;               // free area in file
  bool      comp;                // compressed before flushed to disk
  uint64_t  nextPos;             // next page flush position

  uint64_t  qId;                 // for debug purpose
  SDiskbasedBufStatis statis;
} SDiskbasedBuf;

int32_t createDiskbasedBuffer(SDiskbasedBuf** pResultBuf, int32_t pagesize, int32_t inMemBufSize, uint64_t qId, const char* dir) {
  *pResultBuf = calloc(1, sizeof(SDiskbasedBuf));

  SDiskbasedBuf* pResBuf = *pResultBuf;
  if (pResBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;  
  }

  pResBuf->pageSize     = pagesize;
  pResBuf->numOfPages   = 0;                        // all pages are in buffer in the first place
  pResBuf->totalBufSize = 0;
  pResBuf->inMemPages   = inMemBufSize/pagesize;    // maximum allowed pages, it is a soft limit.
  pResBuf->allocateId   = -1;
  pResBuf->comp         = true;
  pResBuf->file         = NULL;
  pResBuf->qId          = qId;
  pResBuf->fileSize     = 0;

  // at least more than 2 pages must be in memory
  assert(inMemBufSize >= pagesize * 2);

  pResBuf->lruList = tdListNew(POINTER_BYTES);

  // init id hash table
  pResBuf->groupSet  = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  pResBuf->assistBuf = malloc(pResBuf->pageSize + 2); // EXTRA BYTES
  pResBuf->all = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);

  char path[PATH_MAX] = {0};
  taosGetTmpfilePath(dir, "qbuf", path);
  pResBuf->path = strdup(path);

  pResBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

//  qDebug("QInfo:0x%"PRIx64" create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", qId, pResBuf->pageSize,
//         pResBuf->inMemPages, pResBuf->path);

  return TSDB_CODE_SUCCESS;
}

static int32_t createDiskFile(SDiskbasedBuf* pResultBuf) {
  pResultBuf->file = fopen(pResultBuf->path, "wb+");
  if (pResultBuf->file == NULL) {
//    qError("failed to create tmp file: %s on disk. %s", pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t *dst, SDiskbasedBuf* pResultBuf) { // do nothing
  if (!pResultBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsCompressString(data, srcSize, 1, pResultBuf->assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, pResultBuf->assistBuf, *dst);
  return data;
}

static char* doDecompressData(void* data, int32_t srcSize, int32_t *dst, SDiskbasedBuf* pResultBuf) { // do nothing
  if (!pResultBuf->comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsDecompressString(data, srcSize, 1, pResultBuf->assistBuf, pResultBuf->pageSize, ONE_STAGE_COMP, NULL, 0);
  if (*dst > 0) {
    memcpy(data, pResultBuf->assistBuf, *dst);
  }
  return data;
}

static uint64_t allocatePositionInFile(SDiskbasedBuf* pResultBuf, size_t size) {
  if (pResultBuf->pFree == NULL) {
    return pResultBuf->nextPos;
  } else {
    int32_t offset = -1;

    size_t num = taosArrayGetSize(pResultBuf->pFree);
    for(int32_t i = 0; i < num; ++i) {
      SFreeListItem* pi = taosArrayGet(pResultBuf->pFree, i);
      if (pi->len >= size) {
        offset = pi->offset;
        pi->offset += (int32_t)size;
        pi->len -= (int32_t)size;

        return offset;
      }
    }

    // no available recycle space, allocate new area in file
    return pResultBuf->nextPos;
  }
}

static char* doFlushPageToDisk(SDiskbasedBuf* pResultBuf, SPageInfo* pg) {
  assert(!pg->used && pg->pData != NULL);

  int32_t size = -1;
  char* t = doCompressData(GET_DATA_PAYLOAD(pg), pResultBuf->pageSize, &size, pResultBuf);

  // this page is flushed to disk for the first time
  if (pg->offset == -1) {
    pg->offset = allocatePositionInFile(pResultBuf, size);
    pResultBuf->nextPos += size;

    int32_t ret = fseek(pResultBuf->file, pg->offset, SEEK_SET);
    if (ret != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return NULL;
    }

    ret = (int32_t) fwrite(t, 1, size, pResultBuf->file);
    if (ret != size) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return NULL;
    }

    if (pResultBuf->fileSize < pg->offset + size) {
      pResultBuf->fileSize = pg->offset + size;
    }
  } else {
    // length becomes greater, current space is not enough, allocate new place, otherwise, do nothing
    if (pg->length < size) {
      // 1. add current space to free list
      SPageDiskInfo dinfo = {.length = pg->length, .offset = pg->offset};
      taosArrayPush(pResultBuf->pFree, &dinfo);

      // 2. allocate new position, and update the info
      pg->offset = allocatePositionInFile(pResultBuf, size);
      pResultBuf->nextPos += size;
    }

    //3. write to disk.
    int32_t ret = fseek(pResultBuf->file, pg->offset, SEEK_SET);
    if (ret != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return NULL;
    }

    ret = (int32_t)fwrite(t, 1, size, pResultBuf->file);
    if (ret != size) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return NULL;
    }

    if (pResultBuf->fileSize < pg->offset + size) {
      pResultBuf->fileSize = pg->offset + size;
    }
  }

  char* ret = pg->pData;
  memset(ret, 0, pResultBuf->pageSize);

  pg->pData = NULL;
  pg->length = size;

  pResultBuf->statis.flushBytes += pg->length;
  return ret;
}

static char* flushPageToDisk(SDiskbasedBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  assert(((int64_t) pResultBuf->numOfPages * pResultBuf->pageSize) == pResultBuf->totalBufSize && pResultBuf->numOfPages >= pResultBuf->inMemPages);

  if (pResultBuf->file == NULL) {
    if ((ret = createDiskFile(pResultBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  return doFlushPageToDisk(pResultBuf, pg);
}

// load file block data in disk
static char* loadPageFromDisk(SDiskbasedBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = fseek(pResultBuf->file, pg->offset, SEEK_SET);
  ret = (int32_t)fread(GET_DATA_PAYLOAD(pg), 1, pg->length, pResultBuf->file);
  if (ret != pg->length) {
    terrno = errno;
    return NULL;
  }

  pResultBuf->statis.loadBytes += pg->length;

  int32_t fullSize = 0;
  doDecompressData(GET_DATA_PAYLOAD(pg), pg->length, &fullSize, pResultBuf);

  return (char*)GET_DATA_PAYLOAD(pg);
}

static SIDList addNewGroup(SDiskbasedBuf* pResultBuf, int32_t groupId) {
  assert(taosHashGet(pResultBuf->groupSet, (const char*) &groupId, sizeof(int32_t)) == NULL);

  SArray* pa = taosArrayInit(1, POINTER_BYTES);
  int32_t ret = taosHashPut(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t), &pa, POINTER_BYTES);
  assert(ret == 0);

  return pa;
}

static SPageInfo* registerPage(SDiskbasedBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  SIDList list = NULL;

  char** p = taosHashGet(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    list = addNewGroup(pResultBuf, groupId);
  } else {
    list = (SIDList) (*p);
  }

  pResultBuf->numOfPages += 1;

  SPageInfo* ppi = malloc(sizeof(SPageInfo));//{ .info = PAGE_INFO_INITIALIZER, .pageId = pageId, .pn = NULL};

  ppi->pageId = pageId;
  ppi->pData  = NULL;
  ppi->offset = -1;
  ppi->length = -1;
  ppi->used   = true;
  ppi->pn     = NULL;

  return *(SPageInfo**) taosArrayPush(list, &ppi);
}

static SListNode* getEldestUnrefedPage(SDiskbasedBuf* pResultBuf) {
  SListIter iter = {0};
  tdListInitIter(pResultBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while((pn = tdListNext(&iter)) != NULL) {
    assert(pn != NULL);

    SPageInfo* pageInfo = *(SPageInfo**) pn->data;
    assert(pageInfo->pageId >= 0 && pageInfo->pn == pn);

    if (!pageInfo->used) {
      break;
    }
  }

  return pn;
}

static char* evicOneDataPage(SDiskbasedBuf* pResultBuf) {
  char* bufPage = NULL;
  SListNode* pn = getEldestUnrefedPage(pResultBuf);

  // all pages are referenced by user, try to allocate new space
  if (pn == NULL) {
    assert(0);
    int32_t prev = pResultBuf->inMemPages;

    // increase by 50% of previous mem pages
    pResultBuf->inMemPages = (int32_t)(pResultBuf->inMemPages * 1.5f);

//    qWarn("%p in memory buf page not sufficient, expand from %d to %d, page size:%d", pResultBuf, prev,
//          pResultBuf->inMemPages, pResultBuf->pageSize);
  } else {
    pResultBuf->statis.flushPages += 1;
    tdListPopNode(pResultBuf->lruList, pn);

    SPageInfo* d = *(SPageInfo**) pn->data;
    assert(d->pn == pn);

    d->pn = NULL;
    tfree(pn);

    bufPage = flushPageToDisk(pResultBuf, d);
  }

  return bufPage;
}

static void lruListPushFront(SList *pList, SPageInfo* pi) {
  tdListPrepend(pList, &pi);
  SListNode* front = tdListGetHead(pList);
  pi->pn = front;
}

static void lruListMoveToFront(SList *pList, SPageInfo* pi) {
  tdListPopNode(pList, pi->pn);
  tdListPrependNode(pList, pi->pn);
}

static FORCE_INLINE size_t getAllocPageSize(int32_t pageSize) {
  return pageSize + POINTER_BYTES + 2 + sizeof(SFilePage);
}

SFilePage* getNewDataBuf(SDiskbasedBuf* pResultBuf, int32_t groupId, int32_t* pageId) {
  pResultBuf->statis.getPages += 1;

  char* availablePage = NULL;
  if (NO_IN_MEM_AVAILABLE_PAGES(pResultBuf)) {
    availablePage = evicOneDataPage(pResultBuf);

    // Failed to allocate a new buffer page, and there is an error occurs.
    if (availablePage == NULL) {
      return NULL;
    }
  }

  // register new id in this group
  *pageId = (++pResultBuf->allocateId);

  // register page id info
  SPageInfo* pi = registerPage(pResultBuf, groupId, *pageId);

  // add to LRU list
  assert(listNEles(pResultBuf->lruList) < pResultBuf->inMemPages && pResultBuf->inMemPages > 0);
  lruListPushFront(pResultBuf->lruList, pi);

  // add to hash map
  taosHashPut(pResultBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

  // allocate buf
  if (availablePage == NULL) {
    pi->pData = calloc(1, getAllocPageSize(pResultBuf->pageSize));  // add extract bytes in case of zipped buffer increased.
  } else {
    pi->pData = availablePage;
  }

  pResultBuf->totalBufSize += pResultBuf->pageSize;

  ((void**)pi->pData)[0] = pi;
  pi->used = true;

  return (void *)(GET_DATA_PAYLOAD(pi));
}

SFilePage* getResBufPage(SDiskbasedBuf* pResultBuf, int32_t id) {
  assert(pResultBuf != NULL && id >= 0);
  pResultBuf->statis.getPages += 1;

  SPageInfo** pi = taosHashGet(pResultBuf->all, &id, sizeof(int32_t));
  assert(pi != NULL && *pi != NULL);

  if ((*pi)->pData != NULL) { // it is in memory
    // no need to update the LRU list if only one page exists
    if (pResultBuf->numOfPages == 1) {
      (*pi)->used = true;
      return (void *)(GET_DATA_PAYLOAD(*pi));
    }

    SPageInfo** pInfo = (SPageInfo**) ((*pi)->pn->data);
    assert(*pInfo == *pi);

    lruListMoveToFront(pResultBuf->lruList, (*pi));
    (*pi)->used = true;

    return (void *)(GET_DATA_PAYLOAD(*pi));

  } else { // not in memory
    assert((*pi)->pData == NULL && (*pi)->pn == NULL && (*pi)->length >= 0 && (*pi)->offset >= 0);

    char* availablePage = NULL;
    if (NO_IN_MEM_AVAILABLE_PAGES(pResultBuf)) {
      availablePage = evicOneDataPage(pResultBuf);
      if (availablePage == NULL) {
        return NULL;
      }
    }

    if (availablePage == NULL) {
      (*pi)->pData = calloc(1, getAllocPageSize(pResultBuf->pageSize));
    } else {
      (*pi)->pData = availablePage;
    }

    ((void**)((*pi)->pData))[0] = (*pi);

    lruListPushFront(pResultBuf->lruList, *pi);
    (*pi)->used = true;

    loadPageFromDisk(pResultBuf, *pi);
    return (void *)(GET_DATA_PAYLOAD(*pi));
  }
}

void releaseResBufPage(SDiskbasedBuf* pResultBuf, void* page) {
  assert(pResultBuf != NULL && page != NULL);
  char* p = (char*) page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**) p)[0];
  releaseResBufPageInfo(pResultBuf, ppi);
}

void releaseResBufPageInfo(SDiskbasedBuf* pResultBuf, SPageInfo* pi) {
  assert(pi->pData != NULL && pi->used);

  pi->used = false;
  pResultBuf->statis.releasePages += 1;
}

size_t getNumOfResultBufGroupId(const SDiskbasedBuf* pResultBuf) { return taosHashGetSize(pResultBuf->groupSet); }

size_t getResBufSize(const SDiskbasedBuf* pResultBuf) { return (size_t)pResultBuf->totalBufSize; }

SIDList getDataBufPagesIdList(SDiskbasedBuf* pResultBuf, int32_t groupId) {
  assert(pResultBuf != NULL);

  char** p = taosHashGet(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    return pResultBuf->emptyDummyIdList;
  } else {
    return (SArray*) (*p);
  }
}

void destroyResultBuf(SDiskbasedBuf* pResultBuf) {
  if (pResultBuf == NULL) {
    return;
  }

  if (pResultBuf->file != NULL) {
//    qDebug("QInfo:0x%"PRIx64" res output buffer closed, total:%.2f Kb, inmem size:%.2f Kb, file size:%.2f Kb",
//        pResultBuf->qId, pResultBuf->totalBufSize/1024.0, listNEles(pResultBuf->lruList) * pResultBuf->pageSize / 1024.0,
//        pResultBuf->fileSize/1024.0);

    fclose(pResultBuf->file);
  } else {
//    qDebug("QInfo:0x%"PRIx64" res output buffer closed, total:%.2f Kb, no file created", pResultBuf->qId,
//           pResultBuf->totalBufSize/1024.0);
  }

  remove(pResultBuf->path);
  tfree(pResultBuf->path);

  SArray** p = taosHashIterate(pResultBuf->groupSet, NULL);
  while(p) {
    size_t n = taosArrayGetSize(*p);
    for(int32_t i = 0; i < n; ++i) {
      SPageInfo* pi = taosArrayGetP(*p, i);
      tfree(pi->pData);
      tfree(pi);
    }

    taosArrayDestroy(*p);
    p = taosHashIterate(pResultBuf->groupSet, p);
  }

  tdListFree(pResultBuf->lruList);
  taosArrayDestroy(pResultBuf->emptyDummyIdList);
  taosHashCleanup(pResultBuf->groupSet);
  taosHashCleanup(pResultBuf->all);

  tfree(pResultBuf->assistBuf);
  tfree(pResultBuf);
}

SPageInfo* getLastPageInfo(SIDList pList) {
  size_t size = taosArrayGetSize(pList);
  SPageInfo* pPgInfo = taosArrayGetP(pList, size - 1);
  return pPgInfo;
}

int32_t getPageId(const SPageInfo* pPgInfo) {
  ASSERT(pPgInfo != NULL);
  return pPgInfo->pageId;
}

int32_t getBufPageSize(const SDiskbasedBuf* pResultBuf) {
  return pResultBuf->pageSize;
}

bool isAllDataInMemBuf(const SDiskbasedBuf* pResultBuf) {
  return pResultBuf->fileSize == 0;
}
