#include "qResultbuf.h"
#include <stddef.h>
#include <tscompression.h>
#include "hash.h"
#include "qExtbuffer.h"
#include "queryLog.h"
#include "taoserror.h"

int32_t createDiskbasedResultBuffer(SDiskbasedResultBuf** pResultBuf, int32_t numOfPages, int32_t rowSize,
    int32_t pagesize, int32_t inMemPages, const void* handle) {

  *pResultBuf = calloc(1, sizeof(SDiskbasedResultBuf));
  SDiskbasedResultBuf* pResBuf = *pResultBuf;
  if (pResBuf == NULL) {
    return TSDB_CODE_COM_OUT_OF_MEMORY;  
  }

  pResBuf->pageSize   = pagesize;
  pResBuf->numOfPages = 0;               // all pages are in buffer in the first place
  pResBuf->inMemPages = inMemPages;
  assert(inMemPages <= numOfPages);

  pResBuf->numOfRowsPerPage = (pagesize - sizeof(tFilePage)) / rowSize;

  pResBuf->totalBufSize = pResBuf->numOfPages * pagesize;
  pResBuf->allocateId = -1;

  pResBuf->lruList = tdListNew(POINTER_BYTES);

  // init id hash table
  pResBuf->groupSet = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->all = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->assistBuf = malloc(pResBuf->pageSize + 2); // EXTRA BYTES
  pResBuf->comp = true;

  char path[PATH_MAX] = {0};
  getTmpfilePath("qbuf", path);
  pResBuf->path = strdup(path);

  pResBuf->file = NULL;
  pResBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

  qDebug("QInfo:%p create resBuf for output, page size:%d, initial pages:%d, %" PRId64 "bytes", handle,
      pResBuf->pageSize, pResBuf->numOfPages, pResBuf->totalBufSize);
  
  return TSDB_CODE_SUCCESS;
}

#define NUM_OF_PAGES_ON_DISK(_r) ((_r)->numOfPages - (_r)->inMemPages)
#define FILE_SIZE_ON_DISK(_r) (NUM_OF_PAGES_ON_DISK(_r) * (_r)->pageSize)

static int32_t createDiskResidesBuf(SDiskbasedResultBuf* pResultBuf) {
  pResultBuf->file = fopen(pResultBuf->path, "wb+");
  if (pResultBuf->file == NULL) {
    qError("failed to create tmp file: %s on disk. %s", pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t *dst, bool comp, void* assistBuf) { // do nothing
  if (!comp) {
    *dst = srcSize;
    return data;
  }

  *dst = tsCompressString(data, srcSize, 1, assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, assistBuf, *dst);
  return data;
}

static int32_t allocatePositionInFile(SDiskbasedResultBuf* pResultBuf, size_t size) {
  if (pResultBuf->pFree == NULL) {
    return pResultBuf->nextPos;
  } else { //todo speed up the search procedure
    size_t num = taosArrayGetSize(pResultBuf->pFree);

    int32_t offset = -1;

    for(int32_t i = 0; i < num; ++i) {
      SFreeListItem* pi = taosArrayGet(pResultBuf->pFree, i);
      if (pi->len >= size) {
        offset = pi->offset;
        pi->offset += size;
        pi->len -= size;

        return offset;
      }
    }

    // no available recycle space, allocate new area in file
    return pResultBuf->nextPos;
  }
}

static char* doFlushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  assert(T_REF_VAL_GET(pg) == 0 && pg->pData != NULL);

  int32_t size = -1;
  char* t = doCompressData(pg->pData + POINTER_BYTES, pResultBuf->pageSize, &size, pResultBuf->comp, pResultBuf->assistBuf);
  pg->info.length = size;

  // this page is flushed to disk for the first time
  if (pg->info.offset == -1) {
    pg->info.offset = allocatePositionInFile(pResultBuf, size);
    pResultBuf->nextPos += size;

    fseek(pResultBuf->file, pg->info.offset, SEEK_SET);
    int32_t ret = fwrite(t, 1, size, pResultBuf->file);

    UNUSED(ret);
  } else {
    if (pg->info.length < size) {  // length becomes greater, current space is not enough, allocate new place.
      //1. add current space to free list
      taosArrayPush(pResultBuf->pFree, &pg->info);

      //2. allocate new position, and update the info
      pg->info.offset = allocatePositionInFile(pResultBuf, size);
      pResultBuf->nextPos += size;

      //3. write to disk.
      fseek(pResultBuf->file, pg->info.offset, SEEK_SET);
      fwrite(t, size, 1, pResultBuf->file);
    }
  }

  char* ret = pg->pData;
  pg->pData = NULL;

  return ret;
}

static char* flushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  assert(pResultBuf->numOfPages * pResultBuf->pageSize == pResultBuf->totalBufSize && pResultBuf->numOfPages >= pResultBuf->inMemPages);

  if (pResultBuf->file == NULL) {
    if ((ret = createDiskResidesBuf(pResultBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  return doFlushPageToDisk(pResultBuf, pg);
}

#define NO_AVAILABLE_PAGES(_b) ((_b)->numOfPages >= (_b)->inMemPages)

static SIDList addNewGroup(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(taosHashGet(pResultBuf->groupSet, (const char*) &groupId, sizeof(int32_t)) == NULL);

  SArray* pa = taosArrayInit(1, sizeof(SPageInfo));
  int32_t ret = taosHashPut(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t), &pa, POINTER_BYTES);
  assert(ret == 0);

  return pa;
}

static SPageInfo* registerPage(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  SIDList list = NULL;

  char** p = taosHashGet(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    list = addNewGroup(pResultBuf, groupId);
  } else {
    list = (SIDList) (*p);
  }

  pResultBuf->numOfPages += 1;

  SPageInfo ppi = { .info = PAGE_INFO_INITIALIZER, .pageId = pageId, };
  return taosArrayPush(list, &ppi);
}

tFilePage* getNewDataBuf(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t* pageId) {
  char* allocPg = NULL;

  if (NO_AVAILABLE_PAGES(pResultBuf)) {

    // get the last page in linked list
    SListIter iter = {0};
    tdListInitIter(pResultBuf->lruList, &iter, TD_LIST_BACKWARD);

    SListNode* pn = NULL;
    while((pn = tdListNext(&iter)) != NULL) {
      assert(pn != NULL);
      if (T_REF_VAL_GET(*(SPageInfo**)pn->data) == 0) {
        break;
      }
    }

    // all pages are referenced by user, try to allocate new space
    if (pn == NULL) {
      int32_t prev = pResultBuf->inMemPages;
      pResultBuf->inMemPages = pResultBuf->inMemPages * 1.5;

      qWarn("%p in memory buf page not sufficient, expand from %d to %d, page size:%d", pResultBuf, prev,
          pResultBuf->inMemPages, pResultBuf->pageSize);
    } else {
      tdListPopNode(pResultBuf->lruList, pn);
      SPageInfo* d = *(SPageInfo**) pn->data;
      tfree(pn);

      allocPg = flushPageToDisk(pResultBuf, d);
      if (allocPg == NULL) {
        return NULL;
      }
    }
  }

  // register new id in this group
  *pageId = (++pResultBuf->allocateId);

  // register page id info
  SPageInfo* pi = registerPage(pResultBuf, groupId, *pageId);

  // add to LRU list
  assert(listNEles(pResultBuf->lruList) < pResultBuf->inMemPages);
  tdListPrepend(pResultBuf->lruList, &pi);

  // add to hash map
  taosHashPut(pResultBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

  // allocate buf
  if (allocPg == NULL) {
    pi->pData = calloc(1, pResultBuf->pageSize + POINTER_BYTES);
  } else {
    pi->pData = allocPg;
  }

  pResultBuf->totalBufSize += pResultBuf->pageSize;

  T_REF_INC(pi);  // add ref count
  ((void**)pi->pData)[0] = pi;

  return pi->pData + POINTER_BYTES;
}

tFilePage* getResBufPage(SDiskbasedResultBuf* pResultBuf, int32_t id) {
  assert(pResultBuf != NULL && id >= 0);

  SPageInfo** pi = taosHashGet(pResultBuf->all, &id, sizeof(int32_t));
  assert(pi != NULL && *pi != NULL);

  if ((*pi)->pData != NULL) { // it is in memory
    // no need to update the LRU list
    if (pResultBuf->numOfPages == 1) {
      return (*pi)->pData + POINTER_BYTES;
    }

    SListNode* pnode = NULL;  // todo speed up

    SListIter iter = {0};
    tdListInitIter(pResultBuf->lruList, &iter, TD_LIST_FORWARD);

    while((pnode = tdListNext(&iter)) != NULL) {
      SPageInfo** pInfo = (SPageInfo**) pnode->data;

      // remove it and add it into the front of linked-list
      if ((*pInfo)->pageId == id) {
        tdListPopNode(pResultBuf->lruList, pnode);
        tdListPrependNode(pResultBuf->lruList, pnode);
        T_REF_INC(*(SPageInfo**)pnode->data);

        return ((*(SPageInfo**)pnode->data)->pData + POINTER_BYTES);
      }
    }
  } else { // not in memory
    assert((*pi)->pData == NULL && (*pi)->info.length >= 0 && (*pi)->info.offset >= 0);

    // choose the be flushed page: get the last page in linked list
    SListIter iter1 = {0};
    tdListInitIter(pResultBuf->lruList, &iter1, TD_LIST_BACKWARD);

    SListNode* pn = NULL;
    while((pn = tdListNext(&iter1)) != NULL) {
      assert(pn != NULL);
      if (T_REF_VAL_GET(*(SPageInfo**)(pn->data)) == 0) {
        break;
      }
    }

    // all pages are referenced by user, try to allocate new space
    if (pn == NULL) {
      int32_t prev = pResultBuf->inMemPages;
      pResultBuf->inMemPages = pResultBuf->inMemPages * 1.5;

      qWarn("%p in memory buf page not sufficient, expand from %d to %d, page size:%d", pResultBuf, prev,
            pResultBuf->inMemPages, pResultBuf->pageSize);

      (*pi)->pData = calloc(1, pResultBuf->pageSize + POINTER_BYTES);
    } else {
      tdListPopNode(pResultBuf->lruList, pn);

      if (flushPageToDisk(pResultBuf, *(SPageInfo**)pn->data) != TSDB_CODE_SUCCESS) {
        return NULL;
      }

      char* buf = (*(SPageInfo**)pn->data)->pData;
      (*(SPageInfo**)pn->data)->pData = NULL;

      (*pi)->pData = buf;

      ((void**)((*pi)->pData))[0] = (*pi);
      tfree(pn);
    }

    // load file in disk
    int32_t ret = fseek(pResultBuf->file, (*pi)->info.offset, SEEK_SET);
    ret = fread((*pi)->pData + POINTER_BYTES, 1, (*pi)->info.length, pResultBuf->file);
    if (ret != (*pi)->info.length) {
      terrno = errno;
      return NULL;
    }

    // todo do decomp

    return (*pi)->pData + POINTER_BYTES;
  }

  return NULL;
}

void releaseResBufPage(SDiskbasedResultBuf* pResultBuf, void* page) {
  assert(pResultBuf != NULL && page != NULL);
  char* p = (char*) page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**) p)[0];

  assert(T_REF_VAL_GET(ppi) > 0);
  T_REF_DEC(ppi);
}

size_t getNumOfRowsPerPage(const SDiskbasedResultBuf* pResultBuf) { return pResultBuf->numOfRowsPerPage; }

size_t getNumOfResultBufGroupId(const SDiskbasedResultBuf* pResultBuf) { return taosHashGetSize(pResultBuf->groupSet); }

size_t getResBufSize(const SDiskbasedResultBuf* pResultBuf) { return pResultBuf->totalBufSize; }

SIDList getDataBufPagesIdList(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(pResultBuf != NULL);

  char** p = taosHashGet(pResultBuf->groupSet, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    return pResultBuf->emptyDummyIdList;
  } else {
    return (SArray*) (*p);
  }
}

void destroyResultBuf(SDiskbasedResultBuf* pResultBuf, void* handle) {
  if (pResultBuf == NULL) {
    return;
  }

  if (pResultBuf->file != NULL) {
    qDebug("QInfo:%p disk-based output buffer closed, total:%" PRId64 " bytes, file created:%s, file size:%d", handle,
        pResultBuf->totalBufSize, pResultBuf->path, FILE_SIZE_ON_DISK(pResultBuf));

    fclose(pResultBuf->file);
  } else {
    qDebug("QInfo:%p disk-based output buffer closed, total:%" PRId64 " bytes, no file created", handle,
           pResultBuf->totalBufSize);
  }

  unlink(pResultBuf->path);
  tfree(pResultBuf->path);

  SHashMutableIterator* iter = taosHashCreateIter(pResultBuf->groupSet);
  while(taosHashIterNext(iter)) {
    SArray** p = (SArray**) taosHashIterGet(iter);
    size_t n = taosArrayGetSize(*p);
    for(int32_t i = 0; i < n; ++i) {
      SPageInfo* pi = taosArrayGet(*p, i);
      tfree(pi->pData);
    }
    taosArrayDestroy(*p);
  }

  taosHashDestroyIter(iter);

  tdListFree(pResultBuf->lruList);
  taosArrayDestroy(pResultBuf->emptyDummyIdList);
  taosHashCleanup(pResultBuf->groupSet);
  taosHashCleanup(pResultBuf->all);

  tfree(pResultBuf->assistBuf);
  tfree(pResultBuf);
}

int32_t getLastPageId(SIDList pList) {
  size_t size = taosArrayGetSize(pList);
  return *(int32_t*) taosArrayGet(pList, size - 1);
}

