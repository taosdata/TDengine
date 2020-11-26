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
#include "os.h"
#include "qExtbuffer.h"
#include "queryLog.h"
#include "taos.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tsqlfunction.h"
#include "tulog.h"

#define COLMODEL_GET_VAL(data, schema, allrow, rowId, colId) \
  (data + (schema)->pFields[colId].offset * (allrow) + (rowId) * (schema)->pFields[colId].field.bytes)

/*
 * SColumnModel is deeply copy
 */
tExtMemBuffer* createExtMemBuffer(int32_t inMemSize, int32_t elemSize, int32_t pagesize, SColumnModel *pModel) {
  tExtMemBuffer* pMemBuffer = (tExtMemBuffer *)calloc(1, sizeof(tExtMemBuffer));

  pMemBuffer->pageSize = pagesize;
  pMemBuffer->inMemCapacity = ALIGN8(inMemSize) / pMemBuffer->pageSize;
  pMemBuffer->nElemSize = elemSize;

  pMemBuffer->numOfElemsPerPage = (pMemBuffer->pageSize - sizeof(tFilePage)) / pMemBuffer->nElemSize;
  
  char name[MAX_TMPFILE_PATH_LENGTH] = {0};
  taosGetTmpfilePath("extbuf", name);
  
  pMemBuffer->path = strdup(name);
  uDebug("create tmp file:%s", pMemBuffer->path);
  
  SExtFileInfo *pFMeta = &pMemBuffer->fileMeta;

  pFMeta->pageSize = DEFAULT_PAGE_SIZE;

  pFMeta->flushoutData.nAllocSize = 4;
  pFMeta->flushoutData.nLength = 0;
  pFMeta->flushoutData.pFlushoutInfo = (tFlushoutInfo *)calloc(4, sizeof(tFlushoutInfo));

  pMemBuffer->pColumnModel = cloneColumnModel(pModel);
  pMemBuffer->pColumnModel->capacity = pMemBuffer->numOfElemsPerPage;
  
  return pMemBuffer;
}

void* destoryExtMemBuffer(tExtMemBuffer *pMemBuffer) {
  if (pMemBuffer == NULL) {
    return NULL;
  }

  // release flush out info link
  SExtFileInfo *pFileMeta = &pMemBuffer->fileMeta;
  if (pFileMeta->flushoutData.nAllocSize != 0 && pFileMeta->flushoutData.pFlushoutInfo != NULL) {
    tfree(pFileMeta->flushoutData.pFlushoutInfo);
  }

  // release all in-memory buffer pages
  tFilePagesItem *pFilePages = pMemBuffer->pHead;
  while (pFilePages != NULL) {
    tFilePagesItem *pTmp = pFilePages;
    pFilePages = pFilePages->pNext;
    tfree(pTmp);
  }

  // close temp file
  if (pMemBuffer->file != 0) {
    if (fclose(pMemBuffer->file) != 0) {
      uError("failed to close file:%s, reason:%s", pMemBuffer->path, strerror(errno));
    }
    
    uDebug("remove temp file:%s for external buffer", pMemBuffer->path);
    unlink(pMemBuffer->path);
  }

  destroyColumnModel(pMemBuffer->pColumnModel);

  tfree(pMemBuffer->path);
  tfree(pMemBuffer);
  
  return NULL;
}

/*
 * alloc more memory for flush out info entries.
 */
static bool allocFlushoutInfoEntries(SExtFileInfo *pFileMeta) {
  pFileMeta->flushoutData.nAllocSize = pFileMeta->flushoutData.nAllocSize << 1;

  tFlushoutInfo *tmp = (tFlushoutInfo *)realloc(pFileMeta->flushoutData.pFlushoutInfo,
                                                sizeof(tFlushoutInfo) * pFileMeta->flushoutData.nAllocSize);
  if (tmp == NULL) {
    uError("out of memory!\n");
    return false;
  }

  pFileMeta->flushoutData.pFlushoutInfo = tmp;
  return true;
}

static bool tExtMemBufferAlloc(tExtMemBuffer *pMemBuffer) {
  /*
   * the in-mem buffer is full.
   * To flush data to disk to accommodate more data
   */
  if (pMemBuffer->numOfInMemPages > 0 && pMemBuffer->numOfInMemPages == pMemBuffer->inMemCapacity) {
    if (tExtMemBufferFlush(pMemBuffer) != 0) {
      return false;
    }
  }

  /*
   * We do not recycle the file page structure. And in flush data operations, all
   * file page that are full of data are destroyed after data being flushed to disk.
   *
   * The memory buffer pages may be recycle in order to avoid unnecessary memory
   * allocation later.
   */
  tFilePagesItem *item = (tFilePagesItem *)calloc(1, pMemBuffer->pageSize + sizeof(tFilePagesItem));
  if (item == NULL) {
    return false;
  }

  item->pNext = NULL;
  item->item.num = 0;

  if (pMemBuffer->pTail != NULL) {
    pMemBuffer->pTail->pNext = item;
    pMemBuffer->pTail = item;
  } else {
    pMemBuffer->pTail = item;
    pMemBuffer->pHead = item;
  }

  pMemBuffer->numOfInMemPages += 1;
  return true;
}

/*
 * put elements into buffer
 */
int16_t tExtMemBufferPut(tExtMemBuffer *pMemBuffer, void *data, int32_t numOfRows) {
  if (numOfRows == 0) {
    return pMemBuffer->numOfInMemPages;
  }

  tFilePagesItem *pLast = pMemBuffer->pTail;
  if (pLast == NULL) {
    if (!tExtMemBufferAlloc(pMemBuffer)) {
      return -1;
    }

    pLast = pMemBuffer->pTail;
  }

  if (pLast->item.num + numOfRows <= pMemBuffer->numOfElemsPerPage) { // enough space for records
    tColModelAppend(pMemBuffer->pColumnModel, &pLast->item, data, 0, numOfRows, numOfRows);
    
    pMemBuffer->numOfElemsInBuffer += numOfRows;
    pMemBuffer->numOfTotalElems += numOfRows;
  } else {
    int32_t numOfRemainEntries = pMemBuffer->numOfElemsPerPage - (int32_t)pLast->item.num;
    tColModelAppend(pMemBuffer->pColumnModel, &pLast->item, data, 0, numOfRemainEntries, numOfRows);

    pMemBuffer->numOfElemsInBuffer += numOfRemainEntries;
    pMemBuffer->numOfTotalElems += numOfRemainEntries;

    int32_t hasWritten = numOfRemainEntries;
    int32_t remain = numOfRows - numOfRemainEntries;

    while (remain > 0) {
      if (!tExtMemBufferAlloc(pMemBuffer)) { // failed to allocate memory buffer
        return -1;
      }

      int32_t numOfWriteElems = 0;
      if (remain > pMemBuffer->numOfElemsPerPage) {
        numOfWriteElems = pMemBuffer->numOfElemsPerPage;
      } else {
        numOfWriteElems = remain;
      }

      pMemBuffer->numOfTotalElems += numOfWriteElems;

      pLast = pMemBuffer->pTail;
      tColModelAppend(pMemBuffer->pColumnModel, &pLast->item, data, hasWritten, numOfWriteElems, numOfRows);

      remain -= numOfWriteElems;
      pMemBuffer->numOfElemsInBuffer += numOfWriteElems;
      hasWritten += numOfWriteElems;
    }
  }

  return pMemBuffer->numOfInMemPages;
}

static bool tExtMemBufferUpdateFlushoutInfo(tExtMemBuffer *pMemBuffer) {
  SExtFileInfo *pFileMeta = &pMemBuffer->fileMeta;

  if (pMemBuffer->flushModel == MULTIPLE_APPEND_MODEL) {
    if (pFileMeta->flushoutData.nLength == pFileMeta->flushoutData.nAllocSize && !allocFlushoutInfoEntries(pFileMeta)) {
      return false;
    }

    tFlushoutInfo *pFlushoutInfo = &pFileMeta->flushoutData.pFlushoutInfo[pFileMeta->flushoutData.nLength];
    if (pFileMeta->flushoutData.nLength == 0) {
      pFlushoutInfo->startPageId = 0;
    } else {
      pFlushoutInfo->startPageId =
          pFileMeta->flushoutData.pFlushoutInfo[pFileMeta->flushoutData.nLength - 1].startPageId +
          pFileMeta->flushoutData.pFlushoutInfo[pFileMeta->flushoutData.nLength - 1].numOfPages;
    }

    // only the page still in buffer is flushed out to disk
    pFlushoutInfo->numOfPages = pMemBuffer->numOfInMemPages;
    pFileMeta->flushoutData.nLength += 1;
  } else {
    // always update the first flush out array in single_flush_model
    pFileMeta->flushoutData.nLength = 1;
    tFlushoutInfo *pFlushoutInfo = &pFileMeta->flushoutData.pFlushoutInfo[0];
    pFlushoutInfo->numOfPages += pMemBuffer->numOfInMemPages;
  }

  return true;
}

static void tExtMemBufferClearFlushoutInfo(tExtMemBuffer *pMemBuffer) {
  SExtFileInfo *pFileMeta = &pMemBuffer->fileMeta;

  pFileMeta->flushoutData.nLength = 0;
  memset(pFileMeta->flushoutData.pFlushoutInfo, 0, sizeof(tFlushoutInfo) * pFileMeta->flushoutData.nAllocSize);
}

int32_t tExtMemBufferFlush(tExtMemBuffer *pMemBuffer) {
  int32_t ret = 0;
  if (pMemBuffer->numOfTotalElems == 0) {
    return ret;
  }

  if (pMemBuffer->file == NULL) {
    if ((pMemBuffer->file = fopen(pMemBuffer->path, "wb+")) == NULL) {
      ret = TAOS_SYSTEM_ERROR(errno);
      return ret;
    }
  }

  /* all data has been flushed to disk, ignore flush operation */
  if (pMemBuffer->numOfElemsInBuffer == 0) {
    return ret;
  }

  tFilePagesItem *first = pMemBuffer->pHead;
  while (first != NULL) {
    size_t retVal = fwrite((char *)&(first->item), pMemBuffer->pageSize, 1, pMemBuffer->file);
    if (retVal <= 0) {  // failed to write to buffer, may be not enough space
      ret = TAOS_SYSTEM_ERROR(errno);
      return ret;
    }

    pMemBuffer->fileMeta.numOfElemsInFile += (uint32_t)first->item.num;
    pMemBuffer->fileMeta.nFileSize += 1;

    tFilePagesItem *ptmp = first;
    first = first->pNext;

    tfree(ptmp);  // release all data in memory buffer
  }

  fflush(pMemBuffer->file);  // flush to disk

  tExtMemBufferUpdateFlushoutInfo(pMemBuffer);

  pMemBuffer->numOfElemsInBuffer = 0;
  pMemBuffer->numOfInMemPages = 0;
  pMemBuffer->pHead = NULL;
  pMemBuffer->pTail = NULL;

  return ret;
}

void tExtMemBufferClear(tExtMemBuffer *pMemBuffer) {
  if (pMemBuffer == NULL || pMemBuffer->numOfTotalElems == 0) {
    return;
  }

  //release all data in memory buffer
  tFilePagesItem *first = pMemBuffer->pHead;
  while (first != NULL) {
    tFilePagesItem *ptmp = first;
    first = first->pNext;
    tfree(ptmp);
  }

  pMemBuffer->fileMeta.numOfElemsInFile = 0;
  pMemBuffer->fileMeta.nFileSize = 0;

  pMemBuffer->numOfElemsInBuffer = 0;
  pMemBuffer->numOfInMemPages = 0;
  
  pMemBuffer->pHead = NULL;
  pMemBuffer->pTail = NULL;

  tExtMemBufferClearFlushoutInfo(pMemBuffer);

  // reset the write pointer to the header
  if (pMemBuffer->file != NULL) {
    fseek(pMemBuffer->file, 0, SEEK_SET);
  }
}

bool tExtMemBufferLoadData(tExtMemBuffer *pMemBuffer, tFilePage *pFilePage, int32_t flushoutId, int32_t pageIdx) {
  if (flushoutId < 0 || flushoutId > (int32_t)pMemBuffer->fileMeta.flushoutData.nLength) {
    return false;
  }

  tFlushoutInfo *pInfo = &(pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[flushoutId]);
  if (pageIdx > (int32_t)pInfo->numOfPages) {
    return false;
  }

  size_t ret = fseek(pMemBuffer->file, (pInfo->startPageId + pageIdx) * pMemBuffer->pageSize, SEEK_SET);
  ret = fread(pFilePage, pMemBuffer->pageSize, 1, pMemBuffer->file);

  return (ret > 0);
}

bool tExtMemBufferIsAllDataInMem(tExtMemBuffer *pMemBuffer) { return (pMemBuffer->fileMeta.nFileSize == 0); }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
static FORCE_INLINE int32_t primaryKeyComparator(int64_t f1, int64_t f2, int32_t colIdx, int32_t tsOrder) {
  if (f1 == f2) {
    return 0;
  }

  if (tsOrder == TSDB_ORDER_DESC) {  // primary column desc order
    return (f1 < f2) ? 1 : -1;
  } else {  // asc
    return (f1 < f2) ? -1 : 1;
  }
}

static FORCE_INLINE int32_t columnValueAscendingComparator(char *f1, char *f2, int32_t type, int32_t bytes) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t first  = *(int32_t *) f1;
      int32_t second = *(int32_t *) f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double first  = GET_DOUBLE_VAL(f1);
      double second = GET_DOUBLE_VAL(f2);
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      float first  = GET_FLOAT_VAL(f1);
      float second = GET_FLOAT_VAL(f2);
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t first = *(int64_t *)f1;
      int64_t second = *(int64_t *)f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t first = *(int16_t *)f1;
      int16_t second = *(int16_t *)f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t first = *(int8_t *)f1;
      int8_t second = *(int8_t *)f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_BINARY: {
      int32_t len1 = varDataLen(f1);
      int32_t len2 = varDataLen(f2);
      
      if (len1 != len2) {
        return len1 > len2? 1:-1;
      } else {
        int32_t ret = strncmp(varDataVal(f1), varDataVal(f2), len1);
        if (ret == 0) {
          return 0;
        }
        return (ret < 0) ? -1 : 1;
      }

    };
    case TSDB_DATA_TYPE_NCHAR: { // todo handle the var string compare
      int32_t ret = tasoUcs4Compare(f1, f2, bytes);
      if (ret == 0) {
        return 0;
      }
      return (ret < 0) ? -1 : 1;
    };
  }
  
  return 0;
}

int32_t compare_a(tOrderDescriptor *pDescriptor, int32_t numOfRows1, int32_t s1, char *data1, int32_t numOfRows2,
                  int32_t s2, char *data2) {
  assert(numOfRows1 == numOfRows2);

  int32_t cmpCnt = pDescriptor->orderInfo.numOfCols;
  for (int32_t i = 0; i < cmpCnt; ++i) {
    int32_t colIdx = pDescriptor->orderInfo.colIndex[i];

    char *f1 = COLMODEL_GET_VAL(data1, pDescriptor->pColumnModel, numOfRows1, s1, colIdx);
    char *f2 = COLMODEL_GET_VAL(data2, pDescriptor->pColumnModel, numOfRows2, s2, colIdx);

    if (pDescriptor->pColumnModel->pFields[colIdx].field.type == TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t ret = primaryKeyComparator(*(int64_t *)f1, *(int64_t *)f2, colIdx, pDescriptor->tsOrder);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
    } else {
      SSchemaEx *pSchema = &pDescriptor->pColumnModel->pFields[colIdx];
      int32_t  ret = columnValueAscendingComparator(f1, f2, pSchema->field.type, pSchema->field.bytes);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
    }
  }

  return 0;
}

int32_t compare_d(tOrderDescriptor *pDescriptor, int32_t numOfRows1, int32_t s1, char *data1, int32_t numOfRows2,
                  int32_t s2, char *data2) {
  assert(numOfRows1 == numOfRows2);

  int32_t cmpCnt = pDescriptor->orderInfo.numOfCols;
  for (int32_t i = 0; i < cmpCnt; ++i) {
    int32_t colIdx = pDescriptor->orderInfo.colIndex[i];

    char *f1 = COLMODEL_GET_VAL(data1, pDescriptor->pColumnModel, numOfRows1, s1, colIdx);
    char *f2 = COLMODEL_GET_VAL(data2, pDescriptor->pColumnModel, numOfRows2, s2, colIdx);

    if (pDescriptor->pColumnModel->pFields[colIdx].field.type == TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t ret = primaryKeyComparator(*(int64_t *)f1, *(int64_t *)f2, colIdx, pDescriptor->tsOrder);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
    } else {
      SSchemaEx *pSchema = &pDescriptor->pColumnModel->pFields[colIdx];
      int32_t  ret = columnValueAscendingComparator(f1, f2, pSchema->field.type, pSchema->field.bytes);
      if (ret == 0) {
        continue;
      } else {
        return -ret;  // descending order
      }
    }
  }

  return 0;
}
FORCE_INLINE int32_t compare_sa(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t idx1, int32_t idx2,
                                char *data) {
  return compare_a(pDescriptor, numOfRows, idx1, data, numOfRows, idx2, data);
}

FORCE_INLINE int32_t compare_sd(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t idx1, int32_t idx2,
                                char *data) {
  return compare_d(pDescriptor, numOfRows, idx1, data, numOfRows, idx2, data);
}

static void swap(SColumnModel *pColumnModel, int32_t count, int32_t s1, char *data1, int32_t s2, void* buf) {
  for (int32_t i = 0; i < pColumnModel->numOfCols; ++i) {
    void *first = COLMODEL_GET_VAL(data1, pColumnModel, count, s1, i);
    void *second = COLMODEL_GET_VAL(data1, pColumnModel, count, s2, i);

    SSchema* pSchema = &pColumnModel->pFields[i].field;
    tsDataSwap(first, second, pSchema->type, pSchema->bytes, buf);
  }
}

static void tColDataInsertSort(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data,
                               __col_compar_fn_t compareFn, void* buf) {
  for (int32_t i = start + 1; i <= end; ++i) {
    for (int32_t j = i; j > start; --j) {
      if (compareFn(pDescriptor, numOfRows, j, j - 1, data) == -1) {
        swap(pDescriptor->pColumnModel, numOfRows, j - 1, data, j, buf);
      } else {
        break;
      }
    }
  }
}

static void UNUSED_FUNC tSortDataPrint(int32_t type, char *prefix, char *startx, char *midx, char *endx) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      printf("%s:(%d, %d, %d)\n", prefix, *(int32_t *)startx, *(int32_t *)midx, *(int32_t *)endx);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%s:(%d, %d, %d)\n", prefix, *(int8_t *)startx, *(int8_t *)midx, *(int8_t *)endx);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%s:(%d, %d, %d)\n", prefix, *(int16_t *)startx, *(int16_t *)midx, *(int16_t *)endx);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      printf("%s:(%" PRId64 ", %" PRId64 ", %" PRId64 ")\n", prefix, *(int64_t *)startx, *(int64_t *)midx, *(int64_t *)endx);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%s:(%f, %f, %f)\n", prefix, *(float *)startx, *(float *)midx, *(float *)endx);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%s:(%lf, %lf, %lf)\n", prefix, *(double *)startx, *(double *)midx, *(double *)endx);
      break;
    case TSDB_DATA_TYPE_BINARY:
      printf("%s:(%s, %s, %s)\n", prefix, startx, midx, endx);
      break;
  }
}

static void median(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data,
                   __col_compar_fn_t compareFn, void* buf) {
  int32_t midIdx = ((end - start) >> 1) + start;

#if defined(_DEBUG_VIEW)
  int32_t f = pDescriptor->orderInfo.colIndex[0];

  char *midx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, midIdx, f);
  char *startx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, start, f);
  char *endx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, end, f);

  int32_t colIdx = pDescriptor->orderInfo.colIndex[0];
  tSortDataPrint(pDescriptor->pColumnModel->pFields[colIdx].field.type, "before", startx, midx, endx);
#endif

  SColumnModel* pModel = pDescriptor->pColumnModel;
  if (compareFn(pDescriptor, numOfRows, midIdx, start, data) == 1) {
    swap(pModel, numOfRows, start, data, midIdx, buf);
  }

  if (compareFn(pDescriptor, numOfRows, midIdx, end, data) == 1) {
    swap(pModel, numOfRows, midIdx, data, start, buf);
    swap(pModel, numOfRows, midIdx, data, end, buf);
  } else if (compareFn(pDescriptor, numOfRows, start, end, data) == 1) {
    swap(pModel, numOfRows, start, data, end, buf);
  }

  assert(compareFn(pDescriptor, numOfRows, midIdx, start, data) <= 0 &&
         compareFn(pDescriptor, numOfRows, start, end, data) <= 0);

#if defined(_DEBUG_VIEW)
  midx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, midIdx, f);
  startx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, start, f);
  endx = COLMODEL_GET_VAL(data, pDescriptor->pColumnModel, numOfRows, end, f);
  tSortDataPrint(pDescriptor->pColumnModel->pFields[colIdx].field.type, "after", startx, midx, endx);
#endif
}

static UNUSED_FUNC void tRowModelDisplay(tOrderDescriptor *pDescriptor, int32_t numOfRows, char *d, int32_t len) {
  int32_t colIdx = pDescriptor->orderInfo.colIndex[0];

  for (int32_t i = 0; i < len; ++i) {
    char *startx = COLMODEL_GET_VAL(d, pDescriptor->pColumnModel, numOfRows, i, colIdx);

    switch (pDescriptor->pColumnModel->pFields[colIdx].field.type) {
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%lf\t", *(double *)startx);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f\t", *(float *)startx);
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%d\t", *(int32_t *)startx);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d\t", *(int16_t *)startx);
        break;
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%d\t", *(int8_t *)startx);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:
        printf("%" PRId64 "\t", *(int64_t *)startx);
        break;
      case TSDB_DATA_TYPE_BINARY:
        printf("%s\t", startx);
        break;
      default:
        assert(false);
    }
  }
  printf("\n");
}

static void columnwiseQSortImpl(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data,
                                int32_t orderType, __col_compar_fn_t compareFn, void* buf) {
#ifdef _DEBUG_VIEW
  printf("before sort:\n");
  tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif

  int32_t s = start, e = end;
  median(pDescriptor, numOfRows, start, end, data, compareFn, buf);

#ifdef _DEBUG_VIEW
  //  printf("%s called: %d\n", __FUNCTION__, qsort_call++);
#endif

  int32_t end_same = end;
  int32_t start_same = start;

  while (s < e) {
    while (e > s) {
      int32_t ret = compareFn(pDescriptor, numOfRows, e, s, data);
      if (ret < 0) {
        break;
      }

      if (ret == 0 && e != end_same) {
        swap(pDescriptor->pColumnModel, numOfRows, e, data, end_same--, buf);
      }
      e--;
    }

    if (e != s) {
      swap(pDescriptor->pColumnModel, numOfRows, s, data, e, buf);
    }

#ifdef _DEBUG_VIEW
    //    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif

    while (s < e) {
      int32_t ret = compareFn(pDescriptor, numOfRows, s, e, data);
      if (ret > 0) {
        break;
      }

      if (ret == 0 && s != start_same) {
        swap(pDescriptor->pColumnModel, numOfRows, s, data, start_same++, buf);
      }
      s++;
    }

    if (s != e) {
      swap(pDescriptor->pColumnModel, numOfRows, s, data, e, buf);
    }
#ifdef _DEBUG_VIEW
    //    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  int32_t rightx = e + 1;
  if (end_same != end && e < end) {  // move end data to around the pivotal
    int32_t left = e + 1;
    int32_t right = end;

    while (right > end_same && left <= end_same) {
      swap(pDescriptor->pColumnModel, numOfRows, left++, data, right--, buf);
    }

    // (pivotal+1) + steps of number that are identical pivotal
    rightx += (end - end_same);

#ifdef _DEBUG_VIEW
    //    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  int32_t leftx = e - 1;
  if (start_same != start && s > start) {
    int32_t left = start;
    int32_t right = e - 1;

    while (left < start_same && right >= start_same) {
      swap(pDescriptor->pColumnModel, numOfRows, left++, data, right--, buf);
    }

    // (pivotal-1) - steps of number that are identical pivotal
    leftx -= (start_same - start);

#ifdef _DEBUG_VIEW
    //    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  if (leftx > start) {
    columnwiseQSortImpl(pDescriptor, numOfRows, start, leftx, data, orderType, compareFn, buf);
  }

  if (rightx < end) {
    columnwiseQSortImpl(pDescriptor, numOfRows, rightx, end, data, orderType, compareFn, buf);
  }
}

void tColDataQSort(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data, int32_t order) {
  // short array sort, incur another sort procedure instead of quick sort process
  __col_compar_fn_t compareFn = (order == TSDB_ORDER_ASC) ? compare_sa : compare_sd;

  SColumnModel* pModel = pDescriptor->pColumnModel;

  size_t width = 0;
  for(int32_t i = 0; i < pModel->numOfCols; ++i) {
    SSchema* pSchema = &pModel->pFields[i].field;
    if (width < pSchema->bytes) {
      width = pSchema->bytes;
    }
  }

  char* buf = malloc(width);
  assert(width > 0 && buf != NULL);

  if (end - start + 1 <= 8) {
    tColDataInsertSort(pDescriptor, numOfRows, start, end, data, compareFn, buf);
  } else {
    columnwiseQSortImpl(pDescriptor, numOfRows, start, end, data, order, compareFn, buf);
  }

  free(buf);
}

/*
 * deep copy of sschema
 */
SColumnModel *createColumnModel(SSchema *fields, int32_t numOfCols, int32_t blockCapacity) {
  SColumnModel *pColumnModel = (SColumnModel *)calloc(1, sizeof(SColumnModel) + numOfCols * sizeof(SSchemaEx));
  if (pColumnModel == NULL) {
    return NULL;
  }

  pColumnModel->pFields = (SSchemaEx *)(&pColumnModel[1]);
  
  for(int32_t i = 0; i < numOfCols; ++i) {
    SSchemaEx* pSchemaEx = &pColumnModel->pFields[i];
    pSchemaEx->field = fields[i];
    pSchemaEx->offset = pColumnModel->rowSize;
    
    pColumnModel->rowSize += pSchemaEx->field.bytes;
  }

  pColumnModel->numOfCols = numOfCols;
  pColumnModel->capacity = blockCapacity;

  return pColumnModel;
}

SColumnModel *cloneColumnModel(SColumnModel *pSrc) {
  if (pSrc == NULL) {
    return NULL;
  }
  
  SColumnModel *pColumnModel = (SColumnModel *)calloc(1, sizeof(SColumnModel) + pSrc->numOfCols * sizeof(SSchemaEx));
  if (pColumnModel == NULL) {
    return NULL;
  }
  
  *pColumnModel = *pSrc;
  
  pColumnModel->pFields = (SSchemaEx*) (&pColumnModel[1]);
  memcpy(pColumnModel->pFields, pSrc->pFields, pSrc->numOfCols * sizeof(SSchemaEx));
  
  return pColumnModel;
}

void destroyColumnModel(SColumnModel *pModel) {
  if (pModel == NULL) {
    return;
  }

  tfree(pModel);
}

static void printBinaryData(char *data, int32_t len) {
  bool isCharString = true;
  for (int32_t i = 0; i < len; ++i) {
    if ((data[i] <= 'Z' && data[i] >= 'A') || (data[i] <= 'z' && data[i] >= 'a') ||
        (data[i] >= '0' && data[i] <= '9')) {
      continue;
    } else if (data[i] == 0) {
      break;
    } else {
      isCharString = false;
      break;
    }
  }

  if (len == 50) {  // probably the avg intermediate result
    printf("%lf,%" PRId64 "\t", *(double *)data, *(int64_t *)(data + sizeof(double)));
  } else if (data[8] == ',') {  // in TSDB_FUNC_FIRST_DST/TSDB_FUNC_LAST_DST,
                                // the value is seperated by ','
    //printf("%" PRId64 ",%0x\t", *(int64_t *)data, data + sizeof(int64_t) + 1);
    printf("%" PRId64 ", HEX: ", *(int64_t *)data);
    int32_t tmp_len = len - sizeof(int64_t) - 1;
    for (int32_t i = 0; i < tmp_len; ++i) {
      printf("%0x ", *(data + sizeof(int64_t) + 1 + i));
    }
    printf("\t");
  } else if (isCharString) {
    printf("%s\t", data);
  }
}

// todo cast to struct to extract data
static void printBinaryDataEx(char *data, int32_t len, SSrcColumnInfo *param) {
  if (param->functionId == TSDB_FUNC_LAST_DST) {
    switch (param->type) {
      case TSDB_DATA_TYPE_TINYINT:printf("%" PRId64 ",%d\t", *(int64_t *) data, *(int8_t *) (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_SMALLINT:printf("%" PRId64 ",%d\t", *(int64_t *) data, *(int16_t *) (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:printf("%" PRId64 ",%" PRId64 "\t", *(int64_t *) data, *(int64_t *) (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_FLOAT:printf("%" PRId64 ",%f\t", *(int64_t *) data, *(float *) (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_DOUBLE:printf("%" PRId64 ",%f\t", *(int64_t *) data, *(double *) (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_BINARY:printf("%" PRId64 ",%s\t", *(int64_t *) data, (data + TSDB_KEYSIZE + 1));
        break;

      case TSDB_DATA_TYPE_INT:
      default:printf("%" PRId64 ",%d\t", *(int64_t *) data, *(int32_t *) (data + TSDB_KEYSIZE + 1));
        break;
    }
  } else if (param->functionId == TSDB_FUNC_AVG) {
      printf("%f,%" PRId64 "\t", *(double *) data, *(int64_t *) (data + sizeof(double) + 1));
  } else {
    // functionId == TSDB_FUNC_MAX_DST | TSDB_FUNC_TAG
    switch (param->type) {
      case TSDB_DATA_TYPE_TINYINT:
        printf("%d\t", *(int8_t *)data);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d\t", *(int16_t *)data);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:
        printf("%" PRId64 "\t", *(int64_t *)data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f\t", *(float *)data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%f\t", *(double *)data);
        break;
      case TSDB_DATA_TYPE_BINARY:
        printf("%s\t", data);
        break;

      case TSDB_DATA_TYPE_INT:
      default:
        printf("%f\t", *(double *)data);
        break;
    }
  }
}

void tColModelDisplay(SColumnModel *pModel, void *pData, int32_t numOfRows, int32_t totalCapacity) {
  for (int32_t i = 0; i < numOfRows; ++i) {
    for (int32_t j = 0; j < pModel->numOfCols; ++j) {
      char *val = COLMODEL_GET_VAL((char *)pData, pModel, totalCapacity, i, j);

      int type = pModel->pFields[j].field.type;
      printf("type:%d ", type);

      switch (type) {
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)val);
          break;
        case TSDB_DATA_TYPE_NCHAR: {
          char buf[4096] = {0};
          taosUcs4ToMbs(val, pModel->pFields[j].field.bytes, buf);
          printf("%s\t", buf);
          break;
        }
        case TSDB_DATA_TYPE_BINARY: {
          printBinaryData(val, pModel->pFields[j].field.bytes);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)val);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          printf("%" PRId64 "\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_TINYINT:
          printf("%d\t", *(int8_t *)val);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          printf("%d\t", *(int16_t *)val);
          break;
        case TSDB_DATA_TYPE_BOOL:
          printf("%d\t", *(int8_t *)val);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)val);
          break;
        default:
          assert(false);
      }
    }
    printf("\n");
  }
  printf("\n");
}

void tColModelDisplayEx(SColumnModel *pModel, void *pData, int32_t numOfRows, int32_t totalCapacity,
                        SSrcColumnInfo *param) {
  for (int32_t i = 0; i < numOfRows; ++i) {
    for (int32_t j = 0; j < pModel->numOfCols; ++j) {
      char *val = COLMODEL_GET_VAL((char *)pData, pModel, totalCapacity, i, j);

      printf("type:%d\t", pModel->pFields[j].field.type);

      switch (pModel->pFields[j].field.type) {
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)val);
          break;
        case TSDB_DATA_TYPE_NCHAR: {
          char buf[128] = {0};
          taosUcs4ToMbs(val, pModel->pFields[j].field.bytes, buf);
          printf("%s\t", buf);
          break;
        }
        case TSDB_DATA_TYPE_BINARY: {
          printBinaryDataEx(val, pModel->pFields[j].field.bytes, &param[j]);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)val);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          printf("%" PRId64 "\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_TINYINT:
          printf("%d\t", *(int8_t *)val);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          printf("%d\t", *(int16_t *)val);
          break;
        case TSDB_DATA_TYPE_BOOL:
          printf("%d\t", *(int8_t *)val);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)val);
          break;
        default:
          assert(false);
      }
    }
    printf("\n");
  }
  printf("\n");
}

////////////////////////////////////////////////////////////////////////////////////////////
void tColModelCompact(SColumnModel *pModel, tFilePage *inputBuffer, int32_t maxElemsCapacity) {
  if (inputBuffer->num == 0 || maxElemsCapacity == inputBuffer->num) {
    return;
  }

  /* start from the second column */
  for (int32_t i = 1; i < pModel->numOfCols; ++i) {
    SSchemaEx* pSchemaEx = &pModel->pFields[i];
    memmove(inputBuffer->data + pSchemaEx->offset * inputBuffer->num,
            inputBuffer->data + pSchemaEx->offset * maxElemsCapacity,
            (size_t)(pSchemaEx->field.bytes * inputBuffer->num));
  }
}

SSchema* getColumnModelSchema(SColumnModel *pColumnModel, int32_t index) {
  assert(pColumnModel != NULL && index >= 0 && index < pColumnModel->numOfCols);
  return &pColumnModel->pFields[index].field;
}

int16_t getColumnModelOffset(SColumnModel *pColumnModel, int32_t index) {
  assert(pColumnModel != NULL && index >= 0 && index < pColumnModel->numOfCols);
  return pColumnModel->pFields[index].offset;
}

void tColModelErase(SColumnModel *pModel, tFilePage *inputBuffer, int32_t blockCapacity, int32_t s, int32_t e) {
  if (inputBuffer->num == 0 || (e - s + 1) <= 0) {
    return;
  }

  int32_t removed = e - s + 1;
  int32_t remain = (int32_t)inputBuffer->num - removed;
  int32_t secPart = (int32_t)inputBuffer->num - e - 1;

  /* start from the second column */
  for (int32_t i = 0; i < pModel->numOfCols; ++i) {
    int16_t offset = getColumnModelOffset(pModel, i);
    SSchema* pSchema = getColumnModelSchema(pModel, i);
    
    char *startPos = inputBuffer->data + offset * blockCapacity + s * pSchema->bytes;
    char *endPos = startPos + pSchema->bytes * removed;

    memmove(startPos, endPos, pSchema->bytes * secPart);
  }

  inputBuffer->num = remain;
}

/*
 * column format data block append function
 * used in write record(s) to exist column-format block
 *
 * data in srcData must has the same schema as data in dstPage, that can be
 * described by dstModel
 */
void tColModelAppend(SColumnModel *dstModel, tFilePage *dstPage, void *srcData, int32_t start, int32_t numOfRows,
                     int32_t srcCapacity) {
  assert(dstPage->num + numOfRows <= dstModel->capacity);

  for (int32_t col = 0; col < dstModel->numOfCols; ++col) {
    char *dst = COLMODEL_GET_VAL(dstPage->data, dstModel, dstModel->capacity, dstPage->num, col);
    char *src = COLMODEL_GET_VAL((char *)srcData, dstModel, srcCapacity, start, col);

    memmove(dst, src, dstModel->pFields[col].field.bytes * numOfRows);
  }

  dstPage->num += numOfRows;
}

tOrderDescriptor *tOrderDesCreate(const int32_t *orderColIdx, int32_t numOfOrderCols, SColumnModel *pModel,
                                  int32_t tsOrderType) {
  tOrderDescriptor *desc = (tOrderDescriptor *)calloc(1, sizeof(tOrderDescriptor) + sizeof(int32_t) * numOfOrderCols);
  if (desc == NULL) {
    return NULL;
  }

  desc->pColumnModel = pModel;
  desc->tsOrder = tsOrderType;

  desc->orderInfo.numOfCols = numOfOrderCols;
  for (int32_t i = 0; i < numOfOrderCols; ++i) {
    desc->orderInfo.colIndex[i] = orderColIdx[i];
  }

  return desc;
}

void tOrderDescDestroy(tOrderDescriptor *pDesc) {
  if (pDesc == NULL) {
    return;
  }

  destroyColumnModel(pDesc->pColumnModel);
  tfree(pDesc);
}
