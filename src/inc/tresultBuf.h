#ifndef TDENGINE_VNODEQUERYUTIL_H
#define TDENGINE_VNODEQUERYUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "textbuffer.h"

typedef struct SIDList {
  uint32_t alloc;
  int32_t  size;
  int32_t* pData;
} SIDList;

typedef struct SQueryResultBuf {
  int32_t  numOfRowsPerPage;
  int32_t  numOfPages;
  int64_t  totalBufSize;
  int32_t  fd;                  // data file fd
  int32_t  allocateId;          // allocated page id
  int32_t  incStep;             // minimum allocated pages
  char*    pBuf;                // mmap buffer pointer
  char*    path;                // file path
  
  uint32_t numOfAllocGroupIds;  // number of allocated id list
  void*    idsTable;            // id hash table
  SIDList* list;                // for each id, there is a page id list
} SQueryResultBuf;

/**
 * create disk-based result buffer
 * @param pResultBuf
 * @param size
 * @param rowSize
 * @return
 */
int32_t createResultBuf(SQueryResultBuf** pResultBuf, int32_t size, int32_t rowSize);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @param pageId
 * @return
 */
tFilePage* getNewDataBuf(SQueryResultBuf* pResultBuf, int32_t groupId, int32_t* pageId);

/**
 *
 * @param pResultBuf
 * @return
 */
int32_t getNumOfRowsPerPage(SQueryResultBuf* pResultBuf);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @return
 */
SIDList getDataBufPagesIdList(SQueryResultBuf* pResultBuf, int32_t groupId);

/**
 * get the specified buffer page by id
 * @param pResultBuf
 * @param id
 * @return
 */
tFilePage* getResultBufferPageById(SQueryResultBuf* pResultBuf, int32_t id);

/**
 * get the total buffer size in the format of disk file
 * @param pResultBuf
 * @return
 */
int32_t getResBufSize(SQueryResultBuf* pResultBuf);

/**
 * get the number of groups in the result buffer
 * @param pResultBuf
 * @return
 */
int32_t getNumOfResultBufGroupId(SQueryResultBuf* pResultBuf);

/**
 * destroy result buffer
 * @param pResultBuf
 */
void destroyResultBuf(SQueryResultBuf* pResultBuf);

/**
 *
 * @param pList
 * @return
 */
int32_t getLastPageId(SIDList *pList);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEQUERYUTIL_H
