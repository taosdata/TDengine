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

#ifndef _TD_UTIL_PAGEDBUF_H_
#define _TD_UTIL_PAGEDBUF_H_

#include "thash.h"
#include "tlist.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SPageInfo     SPageInfo;
typedef struct SDiskbasedBuf SDiskbasedBuf;

typedef struct SFilePage {
  int32_t num;
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
int32_t createDiskbasedBuf(SDiskbasedBuf** pBuf, int32_t pagesize, int32_t inMemBufSize, const char* id,
                           const char* dir);

/**
 *
 * @param pBuf
 * @param pageId
 * @return
 */
void* getNewBufPage(SDiskbasedBuf* pBuf, int32_t* pageId);

/**
 *
 * @param pBuf
 * @return
 */
SArray* getDataBufPagesIdList(SDiskbasedBuf* pBuf);

/**
 * get the specified buffer page by id
 * @param pBuf
 * @param id
 * @return
 */
void* getBufPage(SDiskbasedBuf* pBuf, int32_t id);

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
 * destroy result buffer
 * @param pBuf
 */
void destroyDiskbasedBuf(SDiskbasedBuf* pBuf);

/**
 *
 * @param pList
 * @return
 */
SPageInfo* getLastPageInfo(SArray* pList);

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

/**
 *
 * @param pBuf
 * @return
 */
int32_t getNumOfInMemBufPages(const SDiskbasedBuf* pBuf);

/**
 *
 * @param pBuf
 * @return
 */
bool isAllDataInMemBuf(const SDiskbasedBuf* pBuf);

/**
 * Set the buffer page is dirty, and needs to be flushed to disk when swap out.
 * @param pPage
 * @param dirty
 */
void setBufPageDirty(void* pPage, bool dirty);

/**
 * Set the compress/ no-compress flag for paged buffer, when flushing data in disk.
 * @param pBuf
 */
void setBufPageCompressOnDisk(SDiskbasedBuf* pBuf, bool comp);

/**
 * Set the pageId page buffer is not need
 * @param pBuf
 * @param pageId
 */
void dBufSetBufPageRecycled(SDiskbasedBuf* pBuf, void* pPage);

/**
 * Print the statistics when closing this buffer
 * @param pBuf
 */
void dBufSetPrintInfo(SDiskbasedBuf* pBuf);

/**
 * Return buf statistics.
 * @param pBuf
 * @return
 */
SDiskbasedBufStatis getDBufStatis(const SDiskbasedBuf* pBuf);

/**
 * Print the buffer statistics information
 * @param pBuf
 */
void dBufPrintStatis(const SDiskbasedBuf* pBuf);

/**
 * Set all of page buffer are not need
 * @param pBuf
 * @return
 */
void clearDiskbasedBuf(SDiskbasedBuf* pBuf);

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_PAGEDBUF_H_
