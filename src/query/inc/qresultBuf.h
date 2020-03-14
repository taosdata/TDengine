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

#include "os.h"
#include "qextbuffer.h"

typedef struct SIDList {
  uint32_t alloc;
  int32_t  size;
  int32_t* pData;
} SIDList;

typedef struct SQueryDiskbasedResultBuf {
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
} SQueryDiskbasedResultBuf;

/**
 * create disk-based result buffer
 * @param pResultBuf
 * @param size
 * @param rowSize
 * @return
 */
int32_t createDiskbasedResultBuffer(SQueryDiskbasedResultBuf** pResultBuf, int32_t size, int32_t rowSize);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @param pageId
 * @return
 */
tFilePage* getNewDataBuf(SQueryDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t* pageId);

/**
 *
 * @param pResultBuf
 * @return
 */
int32_t getNumOfRowsPerPage(SQueryDiskbasedResultBuf* pResultBuf);

/**
 *
 * @param pResultBuf
 * @param groupId
 * @return
 */
SIDList getDataBufPagesIdList(SQueryDiskbasedResultBuf* pResultBuf, int32_t groupId);

/**
 * get the specified buffer page by id
 * @param pResultBuf
 * @param id
 * @return
 */
tFilePage* getResultBufferPageById(SQueryDiskbasedResultBuf* pResultBuf, int32_t id);

/**
 * get the total buffer size in the format of disk file
 * @param pResultBuf
 * @return
 */
int32_t getResBufSize(SQueryDiskbasedResultBuf* pResultBuf);

/**
 * get the number of groups in the result buffer
 * @param pResultBuf
 * @return
 */
int32_t getNumOfResultBufGroupId(SQueryDiskbasedResultBuf* pResultBuf);

/**
 * destroy result buffer
 * @param pResultBuf
 */
void destroyResultBuf(SQueryDiskbasedResultBuf* pResultBuf);

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
