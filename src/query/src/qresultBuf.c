#include "qresultBuf.h"
#include "hash.h"
#include "qextbuffer.h"
#include "taoserror.h"
#include "tsqlfunction.h"
#include "queryLog.h"

int32_t createDiskbasedResultBuffer(SDiskbasedResultBuf** pResultBuf, int32_t size, int32_t rowSize, void* handle) {
  SDiskbasedResultBuf* pResBuf = calloc(1, sizeof(SDiskbasedResultBuf));
  pResBuf->numOfRowsPerPage = (DEFAULT_INTERN_BUF_PAGE_SIZE - sizeof(tFilePage)) / rowSize;
  pResBuf->numOfPages = size;

  pResBuf->totalBufSize = pResBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE;
  pResBuf->incStep = 4;

  // init id hash table
  pResBuf->idsTable = taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->list = calloc(size, sizeof(SIDList));
  pResBuf->numOfAllocGroupIds = size;

  char path[4096] = {0};
  getTmpfilePath("tsdb_q_buf", path);
  pResBuf->path = strdup(path);

  pResBuf->fd = open(pResBuf->path, O_CREAT | O_RDWR, 0666);
  
  memset(path, 0, tListLen(path));

  if (!FD_VALID(pResBuf->fd)) {
    qError("failed to create tmp file: %s on disk. %s", pResBuf->path, strerror(errno));
    return TSDB_CODE_QRY_NO_DISKSPACE;
  }

  int32_t ret = ftruncate(pResBuf->fd, pResBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE);
  if (ret != TSDB_CODE_SUCCESS) {
    qError("failed to create tmp file: %s on disk. %s", pResBuf->path, strerror(errno));
    return TSDB_CODE_QRY_NO_DISKSPACE;
  }

  pResBuf->pBuf = mmap(NULL, pResBuf->totalBufSize, PROT_READ | PROT_WRITE, MAP_SHARED, pResBuf->fd, 0);
  if (pResBuf->pBuf == MAP_FAILED) {
    qError("QInfo:%p failed to map temp file: %s. %s", handle, pResBuf->path, strerror(errno));
    return TSDB_CODE_QRY_OUT_OF_MEMORY; // todo change error code
  }

  qTrace("QInfo:%p create tmp file for output result, %s, %" PRId64 "bytes", handle, pResBuf->path,
      pResBuf->totalBufSize);
  
  *pResultBuf = pResBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t getNumOfResultBufGroupId(SDiskbasedResultBuf* pResultBuf) { return taosHashGetSize(pResultBuf->idsTable); }

int32_t getResBufSize(SDiskbasedResultBuf* pResultBuf) { return pResultBuf->totalBufSize; }

static int32_t extendDiskFileSize(SDiskbasedResultBuf* pResultBuf, int32_t numOfPages) {
  assert(pResultBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE == pResultBuf->totalBufSize);

  int32_t ret = munmap(pResultBuf->pBuf, pResultBuf->totalBufSize);
  pResultBuf->numOfPages += numOfPages;

  /*
   * disk-based output buffer is exhausted, try to extend the disk-based buffer, the available disk space may
   * be insufficient
   */
  ret = ftruncate(pResultBuf->fd, pResultBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE);
  if (ret != 0) {
    //    dError("QInfo:%p failed to create intermediate result output file:%s. %s", pQInfo, pSupporter->extBufFile,
    //           strerror(errno));
    return -TSDB_CODE_QRY_NO_DISKSPACE;
  }

  pResultBuf->totalBufSize = pResultBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE;
  pResultBuf->pBuf = mmap(NULL, pResultBuf->totalBufSize, PROT_READ | PROT_WRITE, MAP_SHARED, pResultBuf->fd, 0);

  if (pResultBuf->pBuf == MAP_FAILED) {
    //    dError("QInfo:%p failed to map temp file: %s. %s", pQInfo, pSupporter->extBufFile, strerror(errno));
    return -TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static bool noMoreAvailablePages(SDiskbasedResultBuf* pResultBuf) {
  return (pResultBuf->allocateId == pResultBuf->numOfPages - 1);
}

static int32_t getGroupIndex(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(pResultBuf != NULL);

  char* p = taosHashGet(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    return -1;
  }

  int32_t slot = GET_INT32_VAL(p);
  assert(slot >= 0 && slot < pResultBuf->numOfAllocGroupIds);

  return slot;
}

static int32_t addNewGroupId(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  int32_t num = getNumOfResultBufGroupId(pResultBuf); // the num is the newest allocated group id slot

  if (pResultBuf->numOfAllocGroupIds <= num) {
    size_t n = pResultBuf->numOfAllocGroupIds << 1u;

    SIDList* p = (SIDList*)realloc(pResultBuf->list, sizeof(SIDList) * n);
    assert(p != NULL);

    memset(&p[pResultBuf->numOfAllocGroupIds], 0, sizeof(SIDList) * pResultBuf->numOfAllocGroupIds);
    
    pResultBuf->list = p;
    pResultBuf->numOfAllocGroupIds = n;
  }

  taosHashPut(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t), &num, sizeof(int32_t));
  return num;
}

static int32_t doRegisterId(SIDList* pList, int32_t id) {
  if (pList->size >= pList->alloc) {
    int32_t s = 0;
    if (pList->alloc == 0) {
      s = 4;
      assert(pList->pData == NULL);
    } else {
      s = pList->alloc << 1u;
    }
    
    int32_t* c = realloc(pList->pData, s * sizeof(int32_t));
    assert(c);
    
    memset(&c[pList->alloc], 0, sizeof(int32_t) * pList->alloc);

    pList->pData = c;
    pList->alloc = s;
  }

  pList->pData[pList->size++] = id;
  return 0;
}

static void registerPageId(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  int32_t slot = getGroupIndex(pResultBuf, groupId);
  if (slot < 0) {
    slot = addNewGroupId(pResultBuf, groupId);
  }

  SIDList* pList = &pResultBuf->list[slot];
  doRegisterId(pList, pageId);
}

tFilePage* getNewDataBuf(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t* pageId) {
  if (noMoreAvailablePages(pResultBuf)) {
    if (extendDiskFileSize(pResultBuf, pResultBuf->incStep) != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  }

  // register new id in this group
  *pageId = (pResultBuf->allocateId++);
  registerPageId(pResultBuf, groupId, *pageId);

  tFilePage* page = GET_RES_BUF_PAGE_BY_ID(pResultBuf, *pageId);
  
  // clear memory for the new page
  memset(page, 0, DEFAULT_INTERN_BUF_PAGE_SIZE);
  
  return page;
}

int32_t getNumOfRowsPerPage(SDiskbasedResultBuf* pResultBuf) { return pResultBuf->numOfRowsPerPage; }

SIDList getDataBufPagesIdList(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  SIDList list = {0};
  int32_t slot = getGroupIndex(pResultBuf, groupId);
  if (slot < 0) {
    return list;
  } else {
    return pResultBuf->list[slot];
  }
}

void destroyResultBuf(SDiskbasedResultBuf* pResultBuf, void* handle) {
  if (pResultBuf == NULL) {
    return;
  }

  if (FD_VALID(pResultBuf->fd)) {
    close(pResultBuf->fd);
  }

  qTrace("QInfo:%p disk-based output buffer closed, %" PRId64 " bytes, file:%s", handle, pResultBuf->totalBufSize, pResultBuf->path);
  munmap(pResultBuf->pBuf, pResultBuf->totalBufSize);
  unlink(pResultBuf->path);
  
  tfree(pResultBuf->path);

  for (int32_t i = 0; i < pResultBuf->numOfAllocGroupIds; ++i) {
    SIDList* pList = &pResultBuf->list[i];
    tfree(pList->pData);
  }

  tfree(pResultBuf->list);
  taosHashCleanup(pResultBuf->idsTable);
  
  tfree(pResultBuf);
}

int32_t getLastPageId(SIDList *pList) {
  if (pList == NULL && pList->size <= 0) {
    return -1;
  }
  
  return pList->pData[pList->size - 1];
}

