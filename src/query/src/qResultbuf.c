#include "qResultbuf.h"
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
  pResBuf->numOfPages = inMemPages;  // all pages are in buffer in the first place
  pResBuf->inMemPages = inMemPages;
  assert(inMemPages <= numOfPages);

  pResBuf->numOfRowsPerPage = (pagesize - sizeof(tFilePage)) / rowSize;

  pResBuf->totalBufSize = pResBuf->numOfPages * pagesize;
  pResBuf->incStep = 4;
  pResBuf->allocateId = -1;

  // todo opt perf by on demand create in memory buffer
  pResBuf->iBuf = calloc(pResBuf->inMemPages, pResBuf->pageSize);

  // init id hash table
  pResBuf->idsTable = taosHashInit(numOfPages, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  pResBuf->list = taosArrayInit(numOfPages, POINTER_BYTES);

  char path[PATH_MAX] = {0};
  getTmpfilePath("qbuf", path);
  pResBuf->path = strdup(path);

  pResBuf->file = NULL;
  pResBuf->pBuf = NULL;
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

  assert(pResultBuf->numOfPages == pResultBuf->inMemPages);
  pResultBuf->numOfPages += pResultBuf->incStep;

  int32_t ret = ftruncate(fileno(pResultBuf->file), NUM_OF_PAGES_ON_DISK(pResultBuf) * pResultBuf->pageSize);
  if (ret != TSDB_CODE_SUCCESS) {
    qError("failed to create tmp file: %s on disk. %s", pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  pResultBuf->pBuf = mmap(NULL, FILE_SIZE_ON_DISK(pResultBuf), PROT_READ | PROT_WRITE, MAP_SHARED,
      fileno(pResultBuf->file), 0);

  if (pResultBuf->pBuf == MAP_FAILED) {
    qError("QInfo:%p failed to map temp file: %s. %s", pResultBuf->handle, pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  pResultBuf->totalBufSize = pResultBuf->numOfPages * pResultBuf->pageSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t extendDiskFileSize(SDiskbasedResultBuf* pResultBuf, int32_t incNumOfPages) {
  assert(pResultBuf->numOfPages * pResultBuf->pageSize == pResultBuf->totalBufSize);
  int32_t ret = TSDB_CODE_SUCCESS;

  if (pResultBuf->pBuf == NULL) {
    assert(pResultBuf->file == NULL);

    if ((ret = createDiskResidesBuf(pResultBuf)) != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    ret = munmap(pResultBuf->pBuf, FILE_SIZE_ON_DISK(pResultBuf));
    pResultBuf->numOfPages += incNumOfPages;

    /*
     * disk-based output buffer is exhausted, try to extend the disk-based buffer, the available disk space may
     * be insufficient
     */
    ret = ftruncate(fileno(pResultBuf->file), NUM_OF_PAGES_ON_DISK(pResultBuf) * pResultBuf->pageSize);
    if (ret != TSDB_CODE_SUCCESS) {
      //    dError("QInfo:%p failed to create intermediate result output file:%s. %s", pQInfo, pSupporter->extBufFile,
      //           strerror(errno));
      return TSDB_CODE_QRY_NO_DISKSPACE;
    }

    pResultBuf->totalBufSize = pResultBuf->numOfPages * pResultBuf->pageSize;
    pResultBuf->pBuf = mmap(NULL, FILE_SIZE_ON_DISK(pResultBuf), PROT_READ | PROT_WRITE, MAP_SHARED, fileno(pResultBuf->file), 0);

    if (pResultBuf->pBuf == MAP_FAILED) {
      //    dError("QInfo:%p failed to map temp file: %s. %s", pQInfo, pSupporter->extBufFile, strerror(errno));
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

#define NO_AVAILABLE_PAGES(_b) ((_b)->allocateId == (_b)->numOfPages - 1)

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
  if (NO_AVAILABLE_PAGES(pResultBuf)) {
    if (extendDiskFileSize(pResultBuf, pResultBuf->incStep) != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  }

  // register new id in this group
  *pageId = (++pResultBuf->allocateId);
  registerPageId(pResultBuf, groupId, *pageId);

  // clear memory for the new page
  tFilePage* page = getResBufPage(pResultBuf, *pageId);
  memset(page, 0, pResultBuf->pageSize);
  
  return page;
}

int32_t getNumOfRowsPerPage(SDiskbasedResultBuf* pResultBuf) { return pResultBuf->numOfRowsPerPage; }

SIDList getDataBufPagesIdList(SDiskbasedResultBuf* pResultBuf, int32_t groupId) {
  int32_t slot = getGroupIndex(pResultBuf, groupId);
  if (slot < 0) {
    return pResultBuf->emptyDummyIdList;
  } else {
    return taosArrayGetP(pResultBuf->list, slot);
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
    munmap(pResultBuf->pBuf, FILE_SIZE_ON_DISK(pResultBuf));
    pResultBuf->pBuf = NULL;
  } else {
    qDebug("QInfo:%p disk-based output buffer closed, total:%" PRId64 " bytes, no file created", handle,
           pResultBuf->totalBufSize);
  }

  unlink(pResultBuf->path);
  tfree(pResultBuf->path);

  size_t size = taosArrayGetSize(pResultBuf->list);
  for (int32_t i = 0; i < size; ++i) {
    SArray* pa = taosArrayGetP(pResultBuf->list, i);
    taosArrayDestroy(pa);
  }

  taosArrayDestroy(pResultBuf->list);
  taosArrayDestroy(pResultBuf->emptyDummyIdList);
  taosHashCleanup(pResultBuf->idsTable);

  tfree(pResultBuf->iBuf);
  tfree(pResultBuf);
}

int32_t getLastPageId(SIDList pList) {
  size_t size = taosArrayGetSize(pList);
  return *(int32_t*) taosArrayGet(pList, size - 1);
}

