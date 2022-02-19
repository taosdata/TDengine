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

#ifndef TDENGINE_TPAGEDBUF_H
#define TDENGINE_TPAGEDBUF_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlist.h"
#include "thash.h"
#include "os.h"
#include "tlockfree.h"

typedef struct SArray* SIDList;
typedef struct SPageInfo SPageInfo;
typedef struct SDiskbasedBuf SDiskbasedBuf;

#define DEFAULT_INTERN_BUF_PAGE_SIZE  (1024L)                          // in bytes
#define DEFAULT_PAGE_SIZE             (16384L)

typedef struct SFilePage {
  int64_t num;
  char    data[];
} SFilePage;

typedef struct SDiskbasedBufStatis {
  int64_t flushBytes;
  int64_t loadBytes;
  int32_t loadPages;
  int32_t getPages;
  int32_t releasePages;
  int32_t flushPages;
} SDiskbasedBufStatis;

/**
 * create disk-based result buffer
 * @param pBuf
 * @param rowSize
 * @param pagesize
 * @param inMemPages
 * @param handle
 * @return
 */
int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int32_t inMemBufSize, uint64_t qId, const char* dir);

/**
 *
 * @param pBuf
 * @param groupId
 * @param pageId
 * @return
 */
SFilePage* getNewDataBuf(SDiskbasedBuf* pBuf, int32_t groupId, int32_t* pageId);

/**
 *
 * @param pBuf
 * @param groupId
 * @return
 */
SIDList getDataBufPagesIdList(SDiskbasedBuf* pBuf, int32_t groupId);

/**
 * get the specified buffer page by id
 * @param pBuf
 * @param id
 * @return
 */
SFilePage* getBufPage(SDiskbasedBuf* pBuf, int32_t id);

/**
 * release the referenced buf pages
 * @param pBuf
 * @param page
 */
void releaseBufPage(SDiskbasedBuf* pBuf, void* page);

/**
 *
 * @param pBuf
 * @param pi
 */
void releaseBufPageInfo(SDiskbasedBuf* pBuf, struct SPageInfo* pi);

/**
 * get the total buffer size in the format of disk file
 * @param pBuf
 * @return
 */
size_t getTotalBufSize(const SDiskbasedBuf* pBuf);

/**
 * get the number of groups in the result buffer
 * @param pBuf
 * @return
 */
size_t getNumOfBufGroupId(const SDiskbasedBuf* pBuf);

/**
 * destroy result buffer
 * @param pBuf
 */
void destroyDiskbasedBuf(SDiskbasedBuf* pBuf);

/**
 *
 * @param pList
 * @return
 */
SPageInfo* getLastPageInfo(SIDList pList);

/**
 *
 * @param pPgInfo
 * @return
 */
int32_t getPageId(const SPageInfo* pPgInfo);

/**
 * Return the buffer page size.
 * @param pBuf
 * @return
 */
int32_t getBufPageSize(const SDiskbasedBuf* pBuf);

int32_t getNumOfInMemBufPages(const SDiskbasedBuf* pBuf);

/**
 *
 * @param pBuf
 * @return
 */
bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf);

/**
 * Set the buffer page is dirty, and needs to be flushed to disk when swap out.
 * @param pPageInfo
 * @param dirty
 */
void setBufPageDirty(SFilePage* pPageInfo, bool dirty);

/**
 * Print the statistics when closing this buffer
 * @param pBuf
 */
void setPrintStatis(SDiskbasedBuf* pBuf);

/**
 * return buf statistics.
 */
SDiskbasedBufStatis getDBufStatis(const SDiskbasedBuf* pBuf);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TPAGEDBUF_H
