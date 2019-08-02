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
#ifndef TDENGINE_TEXTBUFFER_H
#define TDENGINE_TEXTBUFFER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tutil.h"
#include "taosmsg.h"

#define DEFAULT_PAGE_SIZE 16384  // 16k larger than the SHistoInfo
#define MIN_BUFFER_SIZE (1 << 19)
#define MAX_TMPFILE_PATH_LENGTH 512
#define INITIAL_ALLOCATION_BUFFER_SIZE 64

// forward declare
struct tTagSchema;

typedef enum EXT_BUFFER_FLUSH_MODEL {
  /*
   * all data that have been flushed to disk is belonged to the same group
   * which means, all data in disk are sorted, or order is not matter in this case
   */
  SINGLE_APPEND_MODEL,

  /*
   * each flush operation to disk is completely independant to any other flush operation
   * we simply merge several set of data in one file, to reduce the count of flat files
   * in disk. So in this case, we need to keep the flush-out information in tFlushoutInfo
   * structure.
   */
  MULTIPLE_APPEND_MODEL,
} EXT_BUFFER_FLUSH_MODEL;

typedef struct tFlushoutInfo {
  uint32_t startPageId;
  uint32_t numOfPages;
} tFlushoutInfo;

typedef struct tFlushoutData {
  uint32_t       nAllocSize;
  uint32_t       nLength;
  tFlushoutInfo *pFlushoutInfo;
} tFlushoutData;

typedef struct tFileMeta {
  uint32_t      nFileSize;  // in pages
  uint32_t      nPageSize;
  uint32_t      numOfElemsInFile;
  tFlushoutData flushoutData;
} tFileMeta;

typedef struct tFilePage {
  uint64_t numOfElems;
  char     data[];
} tFilePage;

typedef struct tFilePagesItem {
  struct tFilePagesItem *pNext;
  tFilePage              item;
} tFilePagesItem;

typedef struct tColModel {
  int32_t         maxCapacity;
  int32_t         numOfCols;
  int16_t *       colOffset;
  struct SSchema *pFields;
} tColModel;

typedef struct tOrderIdx {
  int32_t numOfOrderedCols;
  int16_t pData[];
} tOrderIdx;

typedef struct tOrderDescriptor {
  union {
    struct tTagSchema *pTagSchema;
    tColModel *        pSchema;
  };
  int32_t   tsOrder;  // timestamp order type if exists
  tOrderIdx orderIdx;
} tOrderDescriptor;

typedef struct tExtMemBuffer {
  int32_t nMaxSizeInPages;

  int32_t nElemSize;
  int32_t nPageSize;

  int32_t numOfAllElems;
  int32_t numOfElemsInBuffer;
  int32_t numOfElemsPerPage;

  int16_t         numOfPagesInMem;
  tFilePagesItem *pHead;
  tFilePagesItem *pTail;

  tFileMeta fileMeta;

  char  dataFilePath[MAX_TMPFILE_PATH_LENGTH];
  FILE *dataFile;

  tColModel *pColModel;

  EXT_BUFFER_FLUSH_MODEL flushModel;
} tExtMemBuffer;

void getExtTmpfilePath(const char *fileNamePattern, int64_t serialNumber, int32_t seg, int32_t slot, char *dstPath);

/*
 * create ext-memory buffer
 */
void tExtMemBufferCreate(tExtMemBuffer **pMemBuffer, int32_t numOfBufferSize, int32_t elemSize,
                         const char *tmpDataFilePath, tColModel *pModel);

/*
 * destroy ext-memory buffer
 */
void tExtMemBufferDestroy(tExtMemBuffer **pMemBuffer);

/*
 * @param pMemBuffer
 * @param data       input data pointer
 * @param numOfRows  number of rows in data
 * @param pModel     column format model
 * @return           number of pages in memory
 */
int16_t tExtMemBufferPut(tExtMemBuffer *pMemBuffer, void *data, int32_t numOfRows);

/*
 * flush all data into disk and release all in-memory buffer
 */
bool tExtMemBufferFlush(tExtMemBuffer *pMemBuffer);

/*
 * remove all data that has been put into buffer, including in buffer or
 * ext-buffer(disk)
 */
void tExtMemBufferClear(tExtMemBuffer *pMemBuffer);

/*
 * this function should be removed.
 * since the flush to disk operation is transparent to client this structure should provide stream operation for data,
 * and there is an internal cursor point to the data.
 */
bool tExtMemBufferLoadData(tExtMemBuffer *pMemBuffer, tFilePage *pFilePage, int32_t flushIdx, int32_t pageIdx);

bool tExtMemBufferIsAllDataInMem(tExtMemBuffer *pMemBuffer);

tColModel *tColModelCreate(SSchema *field, int32_t numOfCols, int32_t maxCapacity);

void tColModelDestroy(tColModel *pModel);

typedef struct SSrcColumnInfo {
  int32_t functionId;
  int32_t type;
} SSrcColumnInfo;

/*
 * display data in column format model for debug purpose only
 */
void tColModelDisplay(tColModel *pModel, void *pData, int32_t numOfRows, int32_t maxCount);

void tColModelDisplayEx(tColModel *pModel, void *pData, int32_t numOfRows, int32_t maxCount, SSrcColumnInfo *pInfo);

/*
 * compress data into consecutive block without hole in data
 */
void tColModelCompact(tColModel *pModel, tFilePage *inputBuffer, int32_t maxElemsCapacity);

void tColModelErase(tColModel *pModel, tFilePage *inputBuffer, int32_t maxCapacity, int32_t s, int32_t e);

tOrderDescriptor *tOrderDesCreate(int32_t *orderColIdx, int32_t numOfOrderCols, tColModel *pModel, int32_t tsOrderType);

void tOrderDescDestroy(tOrderDescriptor *pDesc);

void tColModelAppend(tColModel *dstModel, tFilePage *dstPage, void *srcData, int32_t srcStartRows,
                     int32_t numOfRowsToWrite, int32_t srcCapacity);

///////////////////////////////////////////////////////////////////////////////////////////////////////
typedef struct MinMaxEntry {
  union {
    double  dMinVal;
    int32_t iMinVal;
    int64_t i64MinVal;
  };
  union {
    double  dMaxVal;
    int32_t iMaxVal;
    int64_t i64MaxVal;
  };
} MinMaxEntry;

typedef struct tMemBucketSegment {
  int32_t         numOfSlots;
  MinMaxEntry *   pBoundingEntries;
  tExtMemBuffer **pBuffer;
} tMemBucketSegment;

typedef struct tMemBucket {
  int16_t numOfSegs;
  int16_t nTotalSlots;
  int16_t nSlotsOfSeg;
  int16_t dataType;

  int16_t nElemSize;
  int32_t numOfElems;

  int32_t nTotalBufferSize;
  int32_t maxElemsCapacity;

  int16_t nPageSize;
  int16_t numOfTotalPages;
  int16_t numOfAvailPages; /* remain available buffer pages */

  tMemBucketSegment *pSegs;
  tOrderDescriptor * pOrderDesc;

  MinMaxEntry nRange;

  void (*HashFunc)(struct tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);
} tMemBucket;

typedef int (*__col_compar_fn_t)(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

void tColDataQSort(tOrderDescriptor *, int32_t numOfRows, int32_t start, int32_t end, char *data, int32_t orderType);

int32_t compare_sa(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

int32_t compare_sd(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

int32_t compare_a(tOrderDescriptor *, int32_t numOfRow1, int32_t s1, char *data1, int32_t numOfRow2, int32_t s2,
                  char *data2);

int32_t compare_d(tOrderDescriptor *, int32_t numOfRow1, int32_t s1, char *data1, int32_t numOfRow2, int32_t s2,
                  char *data2);

void tMemBucketCreate(tMemBucket **pBucket, int32_t totalSlots, int32_t nBufferSize, int16_t nElemSize,
                      int16_t dataType, tOrderDescriptor *pDesc);

void tMemBucketDestroy(tMemBucket **pBucket);

void tMemBucketPut(tMemBucket *pBucket, void *data, int32_t numOfRows);

double getPercentile(tMemBucket *pMemBucket, double percent);

void tBucketIntHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);

void tBucketDoubleHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);

#ifdef __cplusplus
}
#endif

#endif  // TBASE_SORT_H
