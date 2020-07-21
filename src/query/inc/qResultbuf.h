/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_VNODEQUERYUTIL_H
#define TDENGINE_VNODEQUERYUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "hash.h"
#include "os.h"
#include "qExtbuffer.h"

typedef struct SArray* SIDList;

typedef struct SDiskbasedResultBuf {
  int32_t   numOfRowsPerPage;
  int32_t   numOfPages;
  int64_t   totalBufSize;
  int32_t   fd;                  // data file fd
  int32_t   allocateId;          // allocated page id
  int32_t   incStep;             // minimum allocated pages
  void*     pBuf;                // mmap buffer pointer
  char*     path;                // file path
  int32_t   pageSize;            // current used page size
  int32_t   inMemPages;          // numOfPages that are allocated in memory
  SHashObj* idsTable;            // id hash table
  SIDList   list;                // for each id, there is a page id list

  void*     iBuf;                // inmemory buf
  void*     handle;              // for debug purpose
  void*     emptyDummyIdList;    // dummy id list
} SDiskbasedResultBuf;

#define DEFAULT_INTERN_BUF_PAGE_SIZE (1024L)
#define DEFAULT_INMEM_BUF_PAGES       10

/**
 * create disk-based result buffer
 * @param pResultBuf
 * @param size
 * @param rowSize
 * @return
 */
int32_t createDiskbasedResultBuffer(SDiskbasedResultBuf** pResultBuf, int32_t numOfPages, int32_t rowSize, int32_t pagesize,
    int32_t inMemPages, void* handle);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @param pageId
 * @return
 */
tFilePage* getNewDataBuf(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t* pageId);

/**
 *
 * @param pResultBuf
 * @return
 */
int32_t getNumOfRowsPerPage(SDiskbasedResultBuf* pResultBuf);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @return
 */
SIDList getDataBufPagesIdList(SDiskbasedResultBuf* pResultBuf, int32_t groupId);

/**
 * get the specified buffer page by id
 * @param pResultBuf
 * @param id
 * @return
 */
static FORCE_INLINE tFilePage* getResBufPage(SDiskbasedResultBuf* pResultBuf, int32_t id) {
  if (id < pResultBuf->inMemPages) {
    return (tFilePage*) ((char*) pResultBuf->iBuf + id * pResultBuf->pageSize);
  } else {
    return (tFilePage*) ((char*) pResultBuf->pBuf + (id - pResultBuf->inMemPages) * pResultBuf->pageSize);
  }
}
/**
 * get the total buffer size in the format of disk file
 * @param pResultBuf
 * @return
 */
int32_t getResBufSize(SDiskbasedResultBuf* pResultBuf);

/**
 * get the number of groups in the result buffer
 * @param pResultBuf
 * @return
 */
int32_t getNumOfResultBufGroupId(SDiskbasedResultBuf* pResultBuf);

/**
 * destroy result buffer
 * @param pResultBuf
 */
void destroyResultBuf(SDiskbasedResultBuf* pResultBuf, void* handle);

/**
 *
 * @param pList
 * @return
 */
int32_t getLastPageId(SIDList pList);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEQUERYUTIL_H
