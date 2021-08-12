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

#include "os.h"
#include "taosmsg.h"

#include "tarray.h"
#include "tutil.h"
#include "tdataformat.h"
#include "talgo.h"

#define MAX_TMPFILE_PATH_LENGTH        PATH_MAX
#define INITIAL_ALLOCATION_BUFFER_SIZE 64
#define DEFAULT_PAGE_SIZE              (1024L)  // 16k larger than the SHistoInfo

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

typedef struct SExtFileInfo {
  uint32_t      nFileSize;  // in pages
  uint32_t      pageSize;
  uint32_t      numOfElemsInFile;
  tFlushoutData flushoutData;
} SExtFileInfo;

typedef struct tFilePage {
  uint64_t num;
  char     data[];
} tFilePage;

typedef struct tFilePagesItem {
  struct tFilePagesItem *pNext;
  tFilePage              item;
} tFilePagesItem;

typedef struct SSchemaEx {
  struct SSchema field;
  int32_t        offset;
} SSchemaEx;

typedef struct SColumnModel {
  int32_t    capacity;
  int32_t    numOfCols;
  int32_t    rowSize;
  SSchemaEx *pFields;
} SColumnModel;

typedef struct SColumnOrderInfo {
  int32_t numOfCols;
  int16_t colIndex[];
} SColumnOrderInfo;

typedef struct tOrderDescriptor {
  SColumnModel *   pColumnModel;
  int32_t          tsOrder;  // timestamp order type if exists
  SColumnOrderInfo orderInfo;
} tOrderDescriptor;

typedef struct tExtMemBuffer {
  int32_t inMemCapacity;
  int32_t nElemSize;
  int32_t pageSize;
  int32_t numOfTotalElems;
  int32_t numOfElemsInBuffer;
  int32_t numOfElemsPerPage;
  int16_t numOfInMemPages;

  tFilePagesItem *pHead;
  tFilePagesItem *pTail;

  char *    path;
  FILE *    file;
  SExtFileInfo fileMeta;

  SColumnModel *         pColumnModel;
  EXT_BUFFER_FLUSH_MODEL flushModel;
} tExtMemBuffer;

/**
 *
 * @param inMemSize
 * @param elemSize
 * @param pModel
 * @return
 */
tExtMemBuffer *createExtMemBuffer(int32_t inMemSize, int32_t elemSize, int32_t pagesize, SColumnModel *pModel);

/**
 *
 * @param pMemBuffer
 * @return
 */
void *destoryExtMemBuffer(tExtMemBuffer *pMemBuffer);

/**
 * @param pMemBuffer
 * @param data       input data pointer
 * @param numOfRows  number of rows in data
 * @param pModel     column format model
 * @return           number of pages in memory
 */
int16_t tExtMemBufferPut(tExtMemBuffer *pMemBuffer, void *data, int32_t numOfRows);

/**
 *
 * @param pMemBuffer
 * @return
 */
int32_t tExtMemBufferFlush(tExtMemBuffer *pMemBuffer);

/**
 *
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

/**
 *
 * @param pMemBuffer
 * @return
 */
bool tExtMemBufferIsAllDataInMem(tExtMemBuffer *pMemBuffer);

/**
 *
 * @param fields
 * @param numOfCols
 * @param blockCapacity
 * @return
 */
SColumnModel *createColumnModel(SSchema *fields, int32_t numOfCols, int32_t blockCapacity);

/**
 *
 * @param pSrc
 * @return
 */
SColumnModel *cloneColumnModel(SColumnModel *pSrc);

/**
 *
 * @param pModel
 */
void destroyColumnModel(SColumnModel *pModel);

/*
 * compress data into consecutive block without hole in data
 */
void tColModelCompact(SColumnModel *pModel, tFilePage *inputBuffer, int32_t maxElemsCapacity);

void     tColModelErase(SColumnModel *pModel, tFilePage *inputBuffer, int32_t maxCapacity, int32_t s, int32_t e);
SSchema *getColumnModelSchema(SColumnModel *pColumnModel, int32_t index);

int16_t getColumnModelOffset(SColumnModel *pColumnModel, int32_t index);

typedef struct SSrcColumnInfo {
  int32_t functionId;
  int32_t type;
} SSrcColumnInfo;

/*
 * display data in column format model for debug purpose only
 */
void tColModelDisplay(SColumnModel *pModel, void *pData, int32_t numOfRows, int32_t maxCount);

void tColModelDisplayEx(SColumnModel *pModel, void *pData, int32_t numOfRows, int32_t maxCount, SSrcColumnInfo *pInfo);

tOrderDescriptor *tOrderDesCreate(const int32_t *orderColIdx, int32_t numOfOrderCols, SColumnModel *pModel,
                                  int32_t tsOrderType);

void tOrderDescDestroy(tOrderDescriptor *pDesc);

void taoscQSort(void** pCols, SSchema* pSchema, int32_t numOfCols, int32_t numOfRows, int32_t index, __compar_fn_t compareFn);

void tColModelAppend(SColumnModel *dstModel, tFilePage *dstPage, void *srcData, int32_t srcStartRows,
                     int32_t numOfRowsToWrite, int32_t srcCapacity);

typedef int (*__col_compar_fn_t)(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

void tColDataQSort(tOrderDescriptor *, int32_t numOfRows, int32_t start, int32_t end, char *data, int32_t orderType);

int32_t compare_sa(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

int32_t compare_sd(tOrderDescriptor *, int32_t numOfRows, int32_t idx1, int32_t idx2, char *data);

int32_t compare_a(tOrderDescriptor *, int32_t numOfRow1, int32_t s1, char *data1, int32_t numOfRow2, int32_t s2,
                  char *data2);

int32_t compare_d(tOrderDescriptor *, int32_t numOfRow1, int32_t s1, char *data1, int32_t numOfRow2, int32_t s2,
                  char *data2);

struct SSDataBlock;
int32_t compare_aRv(struct SSDataBlock* pBlock, SArray* colIndex, int32_t numOfCols, int32_t rowIndex, char** buffer, int32_t order);

int32_t columnValueAscendingComparator(char *f1, char *f2, int32_t type, int32_t bytes);

#ifdef __cplusplus
}
#endif

#endif  // TBASE_SORT_H
