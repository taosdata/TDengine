#include "qresultBuf.h"
#include "hash.h"
#include "qextbuffer.h"
#include "taoserror.h"
#include "queryLog.h"

int32_t createDiskbasedResultBuffer(SDiskbasedResultBuf** pResultBuf, int32_t size, int32_t rowSize, void* handle) {
  *pResultBuf = calloc(1, sizeof(SDiskbasedResultBuf));
  SDiskbasedResultBuf* pResBuf = *pResultBuf;
  if (pResBuf == NULL) {
    return TSDB_CODE_COM_OUT_OF_MEMORY;  
  }
  
  pResBuf->numOfRowsPerPage = (DEFAULT_INTERN_BUF_PAGE_SIZE - sizeof(tFilePage)) / rowSize;
  pResBuf->numOfPages = size;

  pResBuf->totalBufSize = pResBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE;
  pResBuf->incStep = 4;

  // init id hash table
  pResBuf->idsTable = taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->list = taosArrayInit(size, POINTER_BYTES);

  char path[4096] = {0};
  getTmpfilePath("tsdb_qbuf", path);
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

  qDebug("QInfo:%p create tmp file for output result: %s, %" PRId64 "bytes", handle, pResBuf->path,
      pResBuf->totalBufSize);
  
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
    return TSDB_CODE_QRY_NO_DISKSPACE;
  }

  pResultBuf->totalBufSize = pResultBuf->numOfPages * DEFAULT_INTERN_BUF_PAGE_SIZE;
  pResultBuf->pBuf = mmap(NULL, pResultBuf->totalBufSize, PROT_READ | PROT_WRITE, MAP_SHARED, pResultBuf->fd, 0);

  if (pResultBuf->pBuf == MAP_FAILED) {
    //    dError("QInfo:%p failed to map temp file: %s. %s", pQInfo, pSupporter->extBufFile, strerror(errno));
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool noMoreAvailablePages(SDiskbasedResultBuf* pResultBuf) {
  return (pResultBuf->allocateId == pResultBuf->numOfPages - 1);
}

static FORCE_INLINE int32_t getGroupIndex(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  assert(pResultBuf != NULL);

  char* p = taosHashGet(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t));
  if (p == NULL) {  // it is a new group id
    return -1;
  }

  int32_t slot = GET_INT32_VAL(p);
  assert(slot >= 0 && slot < taosHashGetSize(pResultBuf->idsTable));

  return slot;
}

static int32_t addNewGroupId(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  int32_t num = getNumOfResultBufGroupId(pResultBuf); // the num is the newest allocated group id slot
  taosHashPut(pResultBuf->idsTable, (const char*)&groupId, sizeof(int32_t), &num, sizeof(int32_t));

  SArray* pa = taosArrayInit(1, sizeof(int32_t));
  taosArrayPush(pResultBuf->list, &pa);

  assert(taosArrayGetSize(pResultBuf->list) == taosHashGetSize(pResultBuf->idsTable));
  return num;
}

static void registerPageId(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  int32_t slot = getGroupIndex(pResultBuf, groupId);
  if (slot < 0) {
    slot = addNewGroupId(pResultBuf, groupId);
  }

  SIDList pList = taosArrayGetP(pResultBuf->list, slot);
  taosArrayPush(pList, &pageId);
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
  int32_t slot = getGroupIndex(pResultBuf, groupId);
  if (slot < 0) {
    return taosArrayInit(1, sizeof(int32_t));
  } else {
    return taosArrayGetP(pResultBuf->list, slot);
  }
}

void destroyResultBuf(SDiskbasedResultBuf* pResultBuf, void* handle) {
  if (pResultBuf == NULL) {
    return;
  }

  if (FD_VALID(pResultBuf->fd)) {
    close(pResultBuf->fd);
  }

  qDebug("QInfo:%p disk-based output buffer closed, %" PRId64 " bytes, file:%s", handle, pResultBuf->totalBufSize, pResultBuf->path);
  munmap(pResultBuf->pBuf, pResultBuf->totalBufSize);
  unlink(pResultBuf->path);
  
  tfree(pResultBuf->path);

  size_t size = taosArrayGetSize(pResultBuf->list);
  for (int32_t i = 0; i < size; ++i) {
    SArray* pa = taosArrayGetP(pResultBuf->list, i);
    taosArrayDestroy(pa);
  }

  taosArrayDestroy(pResultBuf->list);
  taosHashCleanup(pResultBuf->idsTable);
  
  tfree(pResultBuf);
}

int32_t getLastPageId(SIDList pList) {
  size_t size = taosArrayGetSize(pList);
  return *(int32_t*) taosArrayGet(pList, size - 1);
}

