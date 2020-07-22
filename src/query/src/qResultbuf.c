#include "qResultbuf.h"
#include <stddef.h>
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

  pResBuf->pPageList = tdListNew(POINTER_BYTES);

  // init id hash table
  pResBuf->idsTable = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->all = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);

  char path[PATH_MAX] = {0};
  getTmpfilePath("qbuf", path);
  pResBuf->path = strdup(path);

  pResBuf->file = NULL;
  pResBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

  qDebug("QInfo:%p create resBuf for output, page size:%d, initial pages:%d, %" PRId64 "bytes", handle,
      pResBuf->pageSize, pResBuf->numOfPages, pResBuf->totalBufSize);
  
  return TSDB_CODE_SUCCESS;
}

int32_t getNumOfResultBufGroupId(SDiskbasedResultBuf* pResultBuf) { return taosHashGetSize(pResultBuf->idsTable); }

int32_t getResBufSize(SDiskbasedResultBuf* pResultBuf) { return pResultBuf->totalBufSize; }

#define NUM_OF_PAGES_ON_DISK(_r) ((_r)->numOfPages - (_r)->inMemPages)
#define FILE_SIZE_ON_DISK(_r) (NUM_OF_PAGES_ON_DISK(_r) * (_r)->pageSize)

static int32_t createDiskResidesBuf(SDiskbasedResultBuf* pResultBuf) {
//  pResultBuf->fd = open(pResultBuf->path, O_CREAT | O_RDWR, 0666);
  pResultBuf->file = fopen(pResultBuf->path, "w");
  if (pResultBuf->file == NULL) {
    qError("failed to create tmp file: %s on disk. %s", pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }
  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t *dst) { // do nothing
  *dst = srcSize;
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

static void doFlushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  assert(T_REF_VAL_GET(pg) == 0);

  int32_t size = -1;
  char* t = doCompressData(pg->pData + POINTER_BYTES, pResultBuf->pageSize, &size);

  // this page is flushed to disk for the first time
  if (pg->info.offset == -1) {
    int32_t offset = allocatePositionInFile(pResultBuf, size);
    pResultBuf->nextPos += size;

    fseek(pResultBuf->file, offset, SEEK_SET);
    fwrite(t, size, 1, pResultBuf->file);
  } else {
    if (pg->info.length < size) {  // length becomes greater, current space is not enough, allocate new place.
      //1. add current space to free list
      taosArrayPush(pResultBuf->pFree, &pg->info);

      //2. allocate new position, and update the info
      int32_t offset = allocatePositionInFile(pResultBuf, size);
      pResultBuf->nextPos += size;

      //3. write to disk.
      fseek(pResultBuf->file, offset, SEEK_SET);
      fwrite(t, size, 1, pResultBuf->file);
    }
  }
}

static int32_t flushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  assert(pResultBuf->numOfPages * pResultBuf->pageSize == pResultBuf->totalBufSize && pResultBuf->numOfPages >= pResultBuf->inMemPages);

  if (pResultBuf->pBuf == NULL) {
    assert(pResultBuf->file == NULL);
    if ((ret = createDiskResidesBuf(pResultBuf)) != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  doFlushPageToDisk(pResultBuf, pg);
  return TSDB_CODE_SUCCESS;
}

#define NO_AVAILABLE_PAGES(_b) ((_b)->numOfPages >= (_b)->inMemPages)

static SIDList addNewGroup(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(taosHashGet(pResultBuf->idsTable, (const char*) &groupId, sizeof(int32_t)) == NULL);

  SArray* pa = taosArrayInit(1, sizeof(SPageInfo));
  int32_t ret = taosHashPut(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t), &pa, POINTER_BYTES);
  assert(ret == 0);

  return pa;
}

static SPageInfo* registerPage(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  SIDList list = NULL;

  char** p = taosHashGet(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t));
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
  if (NO_AVAILABLE_PAGES(pResultBuf)) {
    // get the last page in linked list
    SListIter iter = {0};
    tdListInitIter(pResultBuf->pPageList, &iter, TD_LIST_BACKWARD);

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
      tdListPopNode(pResultBuf->pPageList, pn);
      if (flushPageToDisk(pResultBuf, *(SPageInfo**)pn->data) != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    }
  }

  // register new id in this group
  *pageId = (++pResultBuf->allocateId);

  // register page id info
  SPageInfo* pi = registerPage(pResultBuf, groupId, *pageId);

  // add to LRU list
  assert(listNEles(pResultBuf->pPageList) < pResultBuf->inMemPages);
  tdListPrepend(pResultBuf->pPageList, &pi);

  // add to hash map
  taosHashPut(pResultBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

  // allocate buf
  pi->pData = calloc(1, pResultBuf->pageSize + POINTER_BYTES);
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
    tdListInitIter(pResultBuf->pPageList, &iter, TD_LIST_FORWARD);

    while((pnode = tdListNext(&iter)) != NULL) {
      SPageInfo** pInfo = (SPageInfo**) pnode->data;

      // remove it and add it into the front of linked-list
      if ((*pInfo)->pageId == id) {
        tdListPopNode(pResultBuf->pPageList, pnode);
        tdListPrependNode(pResultBuf->pPageList, pnode);
        T_REF_INC(*(SPageInfo**)pnode->data);

        return ((*(SPageInfo**)pnode->data)->pData + POINTER_BYTES);
      }
    }
  } else { // not in memory
    // choose the be flushed page
    // get the last page in linked list
    SListIter iter1 = {0};
    tdListInitIter(pResultBuf->pPageList, &iter1, TD_LIST_BACKWARD);

    SListNode* pn = NULL;
    while((pn = tdListNext(&iter1)) != NULL) {
      assert(pn != NULL);
      if (T_REF_VAL_GET(*(SPageInfo**)pn->data) == 0) {
        break;
      }
    }

    // all pages are referenced by user, try to allocate new space
    if (pn == NULL) {
      pResultBuf->inMemPages = pResultBuf->inMemPages * 1.5;
      assert(0);
      return NULL;
    } else {
      tdListPopNode(pResultBuf->pPageList, pn);
      if (flushPageToDisk(pResultBuf, *(SPageInfo**)pn->data) != TSDB_CODE_SUCCESS) {
        return NULL;
      }

      char* buf = (*(SPageInfo**)pn->data)->pData;
      (*(SPageInfo**)pn->data)->pData = NULL;

      // load file in disk
      fseek(pResultBuf->file, (*pi)->info.offset, SEEK_SET);
      fread(buf, (*pi)->info.length, 1, pResultBuf->file);

      (*pi)->pData = buf;
      return (*pi)->pData;
    }
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


int32_t getNumOfRowsPerPage(SDiskbasedResultBuf* pResultBuf) { return pResultBuf->numOfRowsPerPage; }

SIDList getDataBufPagesIdList(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(pResultBuf != NULL);

  char** p = taosHashGet(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t));
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
    pResultBuf->pBuf = NULL;
  } else {
    qDebug("QInfo:%p disk-based output buffer closed, total:%" PRId64 " bytes, no file created", handle,
           pResultBuf->totalBufSize);
  }

  unlink(pResultBuf->path);
  tfree(pResultBuf->path);

//  size_t size = taosArrayGetSize(pResultBuf->list);
//  for (int32_t i = 0; i < size; ++i) {
//    SArray* pa = taosArrayGetP(pResultBuf->list, i);
//    taosArrayDestroy(pa);
//  }

  tdListFree(pResultBuf->pPageList);
  taosArrayDestroy(pResultBuf->emptyDummyIdList);
  taosHashCleanup(pResultBuf->idsTable);

  tfree(pResultBuf);
}

int32_t getLastPageId(SIDList pList) {
  size_t size = taosArrayGetSize(pList);
  return *(int32_t*) taosArrayGet(pList, size - 1);
}

