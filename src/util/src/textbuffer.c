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

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <assert.h>
#include <float.h>
#include <math.h>

#include <errno.h>

#include "os.h"
#include "taos.h"
#include "taosmsg.h"
#include "textbuffer.h"
#include "tlog.h"
#include "tsql.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "ttypes.h"

#pragma GCC diagnostic ignored "-Wformat"

#define COLMODEL_GET_VAL(data, schema, allrow, rowId, colId) \
  (data + (schema)->colOffset[colId] * (allrow) + (rowId) * (schema)->pFields[colId].bytes)

void getExtTmpfilePath(const char *fileNamePattern, int64_t serialNumber, int32_t seg, int32_t slot, char *dstPath) {
  char tmpPath[512] = {0};

  char *tmpDir = NULL;

#ifdef WINDOWS
  tmpDir = getenv("tmp");
#else
  tmpDir = "/tmp/";
#endif

  strcat(tmpPath, tmpDir);
  strcat(tmpPath, fileNamePattern);

  int32_t ret = sprintf(dstPath, tmpPath, taosGetTimestampUs(), serialNumber, seg, slot);
  dstPath[ret] = 0;  // ensure null-terminated string
}

/*
 * tColModel is deeply copy
 */
void tExtMemBufferCreate(tExtMemBuffer **pMemBuffer, int32_t nBufferSize, int32_t elemSize, const char *tmpDataFilePath,
                         tColModel *pModel) {
  (*pMemBuffer) = (tExtMemBuffer *)calloc(1, sizeof(tExtMemBuffer));

  (*pMemBuffer)->nPageSize = DEFAULT_PAGE_SIZE;
  (*pMemBuffer)->nMaxSizeInPages = ALIGN8(nBufferSize) / (*pMemBuffer)->nPageSize;
  (*pMemBuffer)->nElemSize = elemSize;

  (*pMemBuffer)->numOfElemsPerPage = ((*pMemBuffer)->nPageSize - sizeof(tFilePage)) / (*pMemBuffer)->nElemSize;

  strcpy((*pMemBuffer)->dataFilePath, tmpDataFilePath);

  tFileMeta *pFMeta = &(*pMemBuffer)->fileMeta;

  pFMeta->numOfElemsInFile = 0;
  pFMeta->nFileSize = 0;
  pFMeta->nPageSize = DEFAULT_PAGE_SIZE;

  pFMeta->flushoutData.nAllocSize = 4;
  pFMeta->flushoutData.nLength = 0;
  pFMeta->flushoutData.pFlushoutInfo = (tFlushoutInfo *)calloc(4, sizeof(tFlushoutInfo));

  (*pMemBuffer)->pColModel = tColModelCreate(pModel->pFields, pModel->numOfCols, (*pMemBuffer)->numOfElemsPerPage);
}

void tExtMemBufferDestroy(tExtMemBuffer **pMemBuffer) {
  if ((*pMemBuffer) == NULL) {
    return;
  }

  // release flush out info link
  tFileMeta *pFileMeta = &(*pMemBuffer)->fileMeta;
  if (pFileMeta->flushoutData.nAllocSize != 0 && pFileMeta->flushoutData.pFlushoutInfo != NULL) {
    tfree(pFileMeta->flushoutData.pFlushoutInfo);
  }

  // release all in-memory buffer pages
  tFilePagesItem *pFilePages = (*pMemBuffer)->pHead;
  while (pFilePages != NULL) {
    tFilePagesItem *pTmp = pFilePages;
    pFilePages = pFilePages->pNext;
    tfree(pTmp);
  }

  // close temp file
  if ((*pMemBuffer)->dataFile != 0) {
    int32_t ret = fclose((*pMemBuffer)->dataFile);
    if (ret != 0) {
      pError("failed to close file:%s, reason:%s", (*pMemBuffer)->dataFilePath, strerror(errno));
    }
    unlink((*pMemBuffer)->dataFilePath);
  }

  tColModelDestroy((*pMemBuffer)->pColModel);

  tfree(*pMemBuffer);
}

/*
 * alloc more memory for flush out info entries.
 */
static bool allocFlushoutInfoEntries(tFileMeta *pFileMeta) {
  pFileMeta->flushoutData.nAllocSize = pFileMeta->flushoutData.nAllocSize << 1;

  tFlushoutInfo *tmp = (tFlushoutInfo *)realloc(pFileMeta->flushoutData.pFlushoutInfo,
                                                sizeof(tFlushoutInfo) * pFileMeta->flushoutData.nAllocSize);
  if (tmp == NULL) {
    pError("out of memory!\n");
    return false;
  }

  pFileMeta->flushoutData.pFlushoutInfo = tmp;
  return true;
}

bool tExtMemBufferAlloc(tExtMemBuffer *pMemBuffer) {
  if (pMemBuffer->numOfPagesInMem > 0 && pMemBuffer->numOfPagesInMem == pMemBuffer->nMaxSizeInPages) {
    /*
     * the in-mem buffer is full.
     * To flush data to disk to accommodate more data
     */
    if (!tExtMemBufferFlush(pMemBuffer)) {
      return false;
    }
  }

  /*
   * We do not recycle the file page structure. And in flush data operations, all
   * filepage that are full of data are destroyed after data being flushed to disk.
   *
   * The memory buffer pages may be recycle in order to avoid unnecessary memory
   * allocation later.
   */
  tFilePagesItem *item = (tFilePagesItem *)calloc(1, pMemBuffer->nPageSize + sizeof(tFilePagesItem));
  if (item == NULL) {
    return false;
  }

  item->pNext = NULL;
  item->item.numOfElems = 0;

  if (pMemBuffer->pTail != NULL) {
    pMemBuffer->pTail->pNext = item;
    pMemBuffer->pTail = item;
  } else {
    pMemBuffer->pTail = item;
    pMemBuffer->pHead = item;
  }

  pMemBuffer->numOfPagesInMem += 1;

  return true;
}

/*
 * put elements into buffer
 */
int16_t tExtMemBufferPut(tExtMemBuffer *pMemBuffer, void *data, int32_t numOfRows) {
  if (numOfRows == 0) {
    return pMemBuffer->numOfPagesInMem;
  }

  tFilePagesItem *pLast = pMemBuffer->pTail;
  if (pLast == NULL) {
    if (!tExtMemBufferAlloc(pMemBuffer)) {
      return -1;
    }

    pLast = pMemBuffer->pTail;
  }

  if (pLast->item.numOfElems + numOfRows <= pMemBuffer->numOfElemsPerPage) {
    // enough space for records
    tColModelAppend(pMemBuffer->pColModel, &pLast->item, data, 0, numOfRows, numOfRows);
    pMemBuffer->numOfElemsInBuffer += numOfRows;
    pMemBuffer->numOfAllElems += numOfRows;
  } else {
    int32_t numOfRemainEntries = pMemBuffer->numOfElemsPerPage - pLast->item.numOfElems;
    tColModelAppend(pMemBuffer->pColModel, &pLast->item, data, 0, numOfRemainEntries, numOfRows);

    pMemBuffer->numOfElemsInBuffer += numOfRemainEntries;
    pMemBuffer->numOfAllElems += numOfRemainEntries;

    int32_t hasWritten = numOfRemainEntries;
    int32_t remain = numOfRows - numOfRemainEntries;

    while (remain > 0) {
      if (!tExtMemBufferAlloc(pMemBuffer)) {
        // failed to allocate memory buffer
        return -1;
      }

      int32_t numOfWriteElems = 0;
      if (remain > pMemBuffer->numOfElemsPerPage) {
        numOfWriteElems = pMemBuffer->numOfElemsPerPage;
      } else {
        numOfWriteElems = remain;
      }

      pMemBuffer->numOfAllElems += numOfWriteElems;

      pLast = pMemBuffer->pTail;
      tColModelAppend(pMemBuffer->pColModel, &pLast->item, data, hasWritten, numOfWriteElems, numOfRows);

      remain -= numOfWriteElems;
      pMemBuffer->numOfElemsInBuffer += numOfWriteElems;
      hasWritten += numOfWriteElems;
    }
  }

  return pMemBuffer->numOfPagesInMem;
}

static bool tExtMemBufferUpdateFlushoutInfo(tExtMemBuffer *pMemBuffer) {
  tFileMeta *pFileMeta = &pMemBuffer->fileMeta;

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
    pFlushoutInfo->numOfPages = pMemBuffer->numOfPagesInMem;
    pFileMeta->flushoutData.nLength += 1;
  } else {
    // always update the first flushout array in single_flush_model
    pFileMeta->flushoutData.nLength = 1;
    tFlushoutInfo *pFlushoutInfo = &pFileMeta->flushoutData.pFlushoutInfo[0];
    pFlushoutInfo->numOfPages += pMemBuffer->numOfPagesInMem;
  }

  return true;
}

static void tExtMemBufferClearFlushoutInfo(tExtMemBuffer *pMemBuffer) {
  tFileMeta *pFileMeta = &pMemBuffer->fileMeta;

  pFileMeta->flushoutData.nLength = 0;
  memset(pFileMeta->flushoutData.pFlushoutInfo, 0, sizeof(tFlushoutInfo) * pFileMeta->flushoutData.nAllocSize);
}

bool tExtMemBufferFlush(tExtMemBuffer *pMemBuffer) {
  if (pMemBuffer->numOfAllElems == 0) {
    return true;
  }

  if (pMemBuffer->dataFile == NULL) {
    if ((pMemBuffer->dataFile = fopen(pMemBuffer->dataFilePath, "wb+")) == NULL) {
      return false;
    }
  }

  if (pMemBuffer->numOfElemsInBuffer == 0) {
    /* all data has been flushed to disk, ignore flush operation */
    return true;
  }

  bool            ret = true;
  tFilePagesItem *first = pMemBuffer->pHead;

  while (first != NULL) {
    size_t retVal = fwrite((char *)&(first->item), pMemBuffer->nPageSize, 1, pMemBuffer->dataFile);
    if (retVal <= 0) {  // failed to write to buffer, may be not enough space
      ret = false;
    }

    pMemBuffer->fileMeta.numOfElemsInFile += first->item.numOfElems;
    pMemBuffer->fileMeta.nFileSize += 1;

    tFilePagesItem *ptmp = first;
    first = first->pNext;

    tfree(ptmp);  // release all data in memory buffer
  }

  fflush(pMemBuffer->dataFile);  // flush to disk

  tExtMemBufferUpdateFlushoutInfo(pMemBuffer);

  pMemBuffer->numOfElemsInBuffer = 0;
  pMemBuffer->numOfPagesInMem = 0;
  pMemBuffer->pHead = NULL;
  pMemBuffer->pTail = NULL;

  return ret;
}

void tExtMemBufferClear(tExtMemBuffer *pMemBuffer) {
  if (pMemBuffer == NULL || pMemBuffer->numOfAllElems == 0) return;

  /*
   * release all data in memory buffer
   */
  tFilePagesItem *first = pMemBuffer->pHead;
  while (first != NULL) {
    tFilePagesItem *ptmp = first;
    first = first->pNext;
    tfree(ptmp);
  }

  pMemBuffer->fileMeta.numOfElemsInFile = 0;
  pMemBuffer->fileMeta.nFileSize = 0;

  pMemBuffer->numOfElemsInBuffer = 0;
  pMemBuffer->numOfPagesInMem = 0;
  pMemBuffer->pHead = NULL;
  pMemBuffer->pTail = NULL;

  tExtMemBufferClearFlushoutInfo(pMemBuffer);

  if (pMemBuffer->dataFile != NULL) {
    // reset the write pointer to the header
    fseek(pMemBuffer->dataFile, 0, SEEK_SET);
  }
}

bool tExtMemBufferLoadData(tExtMemBuffer *pMemBuffer, tFilePage *pFilePage, int32_t flushoutId, int32_t pageIdx) {
  if (flushoutId < 0 || flushoutId > pMemBuffer->fileMeta.flushoutData.nLength) {
    return false;
  }

  tFlushoutInfo *pInfo = &(pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[flushoutId]);
  if (pageIdx > (int32_t)pInfo->numOfPages) {
    return false;
  }

  size_t ret = fseek(pMemBuffer->dataFile, (pInfo->startPageId + pageIdx) * pMemBuffer->nPageSize, SEEK_SET);
  ret = fread(pFilePage, pMemBuffer->nPageSize, 1, pMemBuffer->dataFile);

  return (ret > 0);
}

bool tExtMemBufferIsAllDataInMem(tExtMemBuffer *pMemBuffer) { return (pMemBuffer->fileMeta.nFileSize == 0); }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TODO safty check in result
void tBucketBigIntHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx) {
  int64_t v = *(int64_t *)value;

  if (pBucket->nRange.i64MaxVal == INT64_MIN) {
    if (v >= 0) {
      *segIdx = ((v >> (64 - 9)) >> 6) + 8;
      *slotIdx = (v >> (64 - 9)) & 0x3F;
    } else {  // v<0
      *segIdx = ((-v) >> (64 - 9)) >> 6;
      *slotIdx = ((-v) >> (64 - 9)) & 0x3F;
      *segIdx = 7 - (*segIdx);
    }
  } else {
    // todo hash for bigint and float and double
    int64_t span = pBucket->nRange.i64MaxVal - pBucket->nRange.i64MinVal;
    if (span < pBucket->nTotalSlots) {
      int32_t delta = (int32_t)(v - pBucket->nRange.i64MinVal);
      *segIdx = delta / pBucket->nSlotsOfSeg;
      *slotIdx = delta % pBucket->nSlotsOfSeg;
    } else {
      double x = (double)span / pBucket->nTotalSlots;
      double posx = (v - pBucket->nRange.i64MinVal) / x;
      if (v == pBucket->nRange.i64MaxVal) {
        posx -= 1;
      }

      *segIdx = ((int32_t)posx) / pBucket->nSlotsOfSeg;
      *slotIdx = ((int32_t)posx) % pBucket->nSlotsOfSeg;
    }
  }
}

// todo refactor to more generic
void tBucketIntHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx) {
  int32_t v = *(int32_t *)value;

  if (pBucket->nRange.iMaxVal == INT32_MIN) {
    /*
     * taking negative integer into consideration,
     * there is only half of pBucket->segs available for non-negative integer
     */
    //        int32_t numOfSlots = pBucket->nTotalSlots>>1;
    //        int32_t bits = bitsOfNumber(numOfSlots)-1;

    if (v >= 0) {
      *segIdx = ((v >> (32 - 9)) >> 6) + 8;
      *slotIdx = (v >> (32 - 9)) & 0x3F;
    } else {  // v<0
      *segIdx = ((-v) >> (32 - 9)) >> 6;
      *slotIdx = ((-v) >> (32 - 9)) & 0x3F;
      *segIdx = 7 - (*segIdx);
    }
  } else {
    // divide a range of [iMinVal, iMaxVal] into 1024 buckets
    int32_t span = pBucket->nRange.iMaxVal - pBucket->nRange.iMinVal;
    if (span < pBucket->nTotalSlots) {
      int32_t delta = v - pBucket->nRange.iMinVal;
      *segIdx = delta / pBucket->nSlotsOfSeg;
      *slotIdx = delta % pBucket->nSlotsOfSeg;
    } else {
      double x = (double)span / pBucket->nTotalSlots;
      double posx = (v - pBucket->nRange.iMinVal) / x;
      if (v == pBucket->nRange.iMaxVal) {
        posx -= 1;
      }
      *segIdx = ((int32_t)posx) / pBucket->nSlotsOfSeg;
      *slotIdx = ((int32_t)posx) % pBucket->nSlotsOfSeg;
    }
  }
}

void tBucketDoubleHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx) {
  double v = *(double *)value;

  if (pBucket->nRange.dMinVal == DBL_MAX) {
    /*
     * taking negative integer into consideration,
     * there is only half of pBucket->segs available for non-negative integer
     */
    double x = DBL_MAX / (pBucket->nTotalSlots >> 1);
    double posx = (v + DBL_MAX) / x;
    *segIdx = ((int32_t)posx) / pBucket->nSlotsOfSeg;
    *slotIdx = ((int32_t)posx) % pBucket->nSlotsOfSeg;
  } else {
    // divide a range of [dMinVal, dMaxVal] into 1024 buckets
    double span = pBucket->nRange.dMaxVal - pBucket->nRange.dMinVal;
    if (span < pBucket->nTotalSlots) {
      int32_t delta = (int32_t)(v - pBucket->nRange.dMinVal);
      *segIdx = delta / pBucket->nSlotsOfSeg;
      *slotIdx = delta % pBucket->nSlotsOfSeg;
    } else {
      double x = span / pBucket->nTotalSlots;
      double posx = (v - pBucket->nRange.dMinVal) / x;
      if (v == pBucket->nRange.dMaxVal) {
        posx -= 1;
      }
      *segIdx = ((int32_t)posx) / pBucket->nSlotsOfSeg;
      *slotIdx = ((int32_t)posx) % pBucket->nSlotsOfSeg;
    }

    if (*segIdx < 0 || *segIdx > 16 || *slotIdx < 0 || *slotIdx > 64) {
      pError("error in hash process. segment is: %d, slot id is: %d\n", *segIdx, *slotIdx);
    }
  }
}

void tMemBucketCreate(tMemBucket **pBucket, int32_t totalSlots, int32_t nBufferSize, int16_t nElemSize,
                      int16_t dataType, tOrderDescriptor *pDesc) {
  *pBucket = (tMemBucket *)malloc(sizeof(tMemBucket));

  (*pBucket)->nTotalSlots = totalSlots;
  (*pBucket)->nSlotsOfSeg = 1 << 6;  // 64 Segments, 16 slots each seg.
  (*pBucket)->dataType = dataType;
  (*pBucket)->nElemSize = nElemSize;
  (*pBucket)->nPageSize = DEFAULT_PAGE_SIZE;

  (*pBucket)->numOfElems = 0;
  (*pBucket)->numOfSegs = (*pBucket)->nTotalSlots / (*pBucket)->nSlotsOfSeg;

  (*pBucket)->nTotalBufferSize = nBufferSize;

  (*pBucket)->maxElemsCapacity = (*pBucket)->nTotalBufferSize / (*pBucket)->nElemSize;

  (*pBucket)->numOfTotalPages = (*pBucket)->nTotalBufferSize / (*pBucket)->nPageSize;
  (*pBucket)->numOfAvailPages = (*pBucket)->numOfTotalPages;

  (*pBucket)->pOrderDesc = pDesc;

  switch ((*pBucket)->dataType) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT: {
      (*pBucket)->nRange.iMinVal = INT32_MAX;
      (*pBucket)->nRange.iMaxVal = INT32_MIN;
      (*pBucket)->HashFunc = tBucketIntHash;
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      (*pBucket)->nRange.dMinVal = DBL_MAX;
      (*pBucket)->nRange.dMaxVal = -DBL_MAX;
      (*pBucket)->HashFunc = tBucketDoubleHash;
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      (*pBucket)->nRange.i64MinVal = INT64_MAX;
      (*pBucket)->nRange.i64MaxVal = INT64_MIN;
      (*pBucket)->HashFunc = tBucketBigIntHash;
      break;
    };
    default: {
      pError("MemBucket:%p,not support data type %d,failed", *pBucket, (*pBucket)->dataType);
      tfree(*pBucket);
      return;
    }
  }

  if (pDesc->pSchema->numOfCols != 1 || pDesc->pSchema->colOffset[0] != 0) {
    pError("MemBucket:%p,only consecutive data is allowed,invalid numOfCols:%d or offset:%d",
           *pBucket, pDesc->pSchema->numOfCols, pDesc->pSchema->colOffset[0]);
    tfree(*pBucket);
    return;
  }

  if (pDesc->pSchema->pFields[0].type != dataType) {
    pError("MemBucket:%p,data type is not consistent,%d in schema, %d in param", *pBucket,
           pDesc->pSchema->pFields[0].type, dataType);
    tfree(*pBucket);
    return;
  }

  if ((*pBucket)->numOfTotalPages < (*pBucket)->nTotalSlots) {
    pWarn("MemBucket:%p,total buffer pages %d are not enough for all slots", *pBucket, (*pBucket)->numOfTotalPages);
  }

  (*pBucket)->pSegs = (tMemBucketSegment *)malloc((*pBucket)->numOfSegs * sizeof(tMemBucketSegment));

  for (int32_t i = 0; i < (*pBucket)->numOfSegs; ++i) {
    (*pBucket)->pSegs[i].numOfSlots = (*pBucket)->nSlotsOfSeg;
    (*pBucket)->pSegs[i].pBuffer = NULL;
    (*pBucket)->pSegs[i].pBoundingEntries = NULL;
  }

  pTrace("MemBucket:%p,created,buffer size:%d,elem size:%d", *pBucket, (*pBucket)->numOfTotalPages * DEFAULT_PAGE_SIZE,
         (*pBucket)->nElemSize);
}

void tMemBucketDestroy(tMemBucket **pBucket) {
  if (*pBucket == NULL) {
    return;
  }

  if ((*pBucket)->pSegs) {
    for (int32_t i = 0; i < (*pBucket)->numOfSegs; ++i) {
      tMemBucketSegment *pSeg = &((*pBucket)->pSegs[i]);
      tfree(pSeg->pBoundingEntries);

      if (pSeg->pBuffer == NULL || pSeg->numOfSlots == 0) {
        continue;
      }

      for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
        if (pSeg->pBuffer[j] != NULL) {
          tExtMemBufferDestroy(&pSeg->pBuffer[j]);
        }
      }
      tfree(pSeg->pBuffer);
    }
  }

  tfree((*pBucket)->pSegs);
  tfree(*pBucket);
}

/*
 * find the slots which accounts for largest proportion of total in-memory buffer
 */
static void tBucketGetMaxMemSlot(tMemBucket *pBucket, int16_t *segIdx, int16_t *slotIdx) {
  *segIdx = -1;
  *slotIdx = -1;

  int32_t val = 0;
  for (int32_t k = 0; k < pBucket->numOfSegs; ++k) {
    tMemBucketSegment *pSeg = &pBucket->pSegs[k];
    for (int32_t i = 0; i < pSeg->numOfSlots; ++i) {
      if (pSeg->pBuffer == NULL || pSeg->pBuffer[i] == NULL) {
        continue;
      }

      if (val < pSeg->pBuffer[i]->numOfPagesInMem) {
        val = pSeg->pBuffer[i]->numOfPagesInMem;
        *segIdx = k;
        *slotIdx = i;
      }
    }
  }
}

static void resetBoundingBox(tMemBucketSegment *pSeg, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT: {
      for (int32_t i = 0; i < pSeg->numOfSlots; ++i) {
        pSeg->pBoundingEntries[i].i64MaxVal = INT64_MIN;
        pSeg->pBoundingEntries[i].i64MinVal = INT64_MAX;
      }
      break;
    };
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT: {
      for (int32_t i = 0; i < pSeg->numOfSlots; ++i) {
        pSeg->pBoundingEntries[i].iMaxVal = INT32_MIN;
        pSeg->pBoundingEntries[i].iMinVal = INT32_MAX;
      }
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      for (int32_t i = 0; i < pSeg->numOfSlots; ++i) {
        pSeg->pBoundingEntries[i].dMaxVal = -DBL_MAX;
        pSeg->pBoundingEntries[i].dMinVal = DBL_MAX;
      }
      break;
    }
  }
}

void tMemBucketUpdateBoundingBox(MinMaxEntry *r, char *data, int32_t dataType) {
  switch (dataType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t val = *(int32_t *)data;
      if (r->iMinVal > val) {
        r->iMinVal = val;
      }

      if (r->iMaxVal < val) {
        r->iMaxVal = val;
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t val = *(int64_t *)data;
      if (r->i64MinVal > val) {
        r->i64MinVal = val;
      }

      if (r->i64MaxVal < val) {
        r->i64MaxVal = val;
      }
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int32_t val = *(int16_t *)data;
      if (r->iMinVal > val) {
        r->iMinVal = val;
      }

      if (r->iMaxVal < val) {
        r->iMaxVal = val;
      }
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int32_t val = *(int8_t *)data;
      if (r->iMinVal > val) {
        r->iMinVal = val;
      }

      if (r->iMaxVal < val) {
        r->iMaxVal = val;
      }

      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double val = *(double *)data;
      if (r->dMinVal > val) {
        r->dMinVal = val;
      }

      if (r->dMaxVal < val) {
        r->dMaxVal = val;
      }
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      double val = *(float *)data;

      if (r->dMinVal > val) {
        r->dMinVal = val;
      }

      if (r->dMaxVal < val) {
        r->dMaxVal = val;
      }
      break;
    };
    default: { assert(false); }
  }
}

/*
 * in memory bucket, we only accept the simple data consecutive put in a row/column
 * no column-model in this case.
 */
void tMemBucketPut(tMemBucket *pBucket, void *data, int32_t numOfRows) {
  pBucket->numOfElems += numOfRows;
  int16_t segIdx = 0, slotIdx = 0;

  for (int32_t i = 0; i < numOfRows; ++i) {
    char *d = (char *)data + i * tDataTypeDesc[pBucket->dataType].nSize;

    switch (pBucket->dataType) {
      case TSDB_DATA_TYPE_SMALLINT: {
        int32_t val = *(int16_t *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        int32_t val = *(int8_t *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t val = *(int32_t *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t val = *(int64_t *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double val = *(double *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        double val = *(float *)d;
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
    }

    tMemBucketSegment *pSeg = &pBucket->pSegs[segIdx];
    if (pSeg->pBoundingEntries == NULL) {
      pSeg->pBoundingEntries = (MinMaxEntry *)malloc(sizeof(MinMaxEntry) * pBucket->nSlotsOfSeg);
      resetBoundingBox(pSeg, pBucket->dataType);
    }

    if (pSeg->pBuffer == NULL) {
      pSeg->pBuffer = (tExtMemBuffer **)calloc(pBucket->nSlotsOfSeg, sizeof(void *));
    }

    if (pSeg->pBuffer[slotIdx] == NULL) {
      int64_t pid = taosGetPthreadId();
      char    name[512] = {0};
      getExtTmpfilePath("/tb_ex_bk_%lld_%lld_%d_%d", pid, segIdx, slotIdx, name);
      tExtMemBufferCreate(&pSeg->pBuffer[slotIdx], pBucket->numOfTotalPages * pBucket->nPageSize, pBucket->nElemSize,
                          name, pBucket->pOrderDesc->pSchema);
      pSeg->pBuffer[slotIdx]->flushModel = SINGLE_APPEND_MODEL;
      pBucket->pOrderDesc->pSchema->maxCapacity = pSeg->pBuffer[slotIdx]->numOfElemsPerPage;
    }

    tMemBucketUpdateBoundingBox(&pSeg->pBoundingEntries[slotIdx], d, pBucket->dataType);

    // ensure available memory pages to allocate
    int16_t cseg = 0, cslot = 0;
    if (pBucket->numOfAvailPages == 0) {
      pTrace("MemBucket:%p,max avail size:%d, no avail memory pages,", pBucket, pBucket->numOfTotalPages);

      tBucketGetMaxMemSlot(pBucket, &cseg, &cslot);
      if (cseg == -1 || cslot == -1) {
        pError("MemBucket:%p,failed to find appropriated avail buffer", pBucket);
        return;
      }

      if (cseg != segIdx || cslot != slotIdx) {
        pBucket->numOfAvailPages += pBucket->pSegs[cseg].pBuffer[cslot]->numOfPagesInMem;

        int32_t avail = pBucket->pSegs[cseg].pBuffer[cslot]->numOfPagesInMem;
        UNUSED(avail);
        tExtMemBufferFlush(pBucket->pSegs[cseg].pBuffer[cslot]);

        pTrace("MemBucket:%p,seg:%d,slot:%d flushed to disk,new avail pages:%d", pBucket, cseg, cslot,
               pBucket->numOfAvailPages);
      } else {
        pTrace("MemBucket:%p,failed to choose slot to flush to disk seg:%d,slot:%d",
               pBucket, cseg, cslot);
      }
    }
    int16_t consumedPgs = pSeg->pBuffer[slotIdx]->numOfPagesInMem;

    int16_t newPgs = tExtMemBufferPut(pSeg->pBuffer[slotIdx], d, 1);
    /*
     * trigger 1. page re-allocation, to reduce the available pages
     *         2. page flushout, to increase the available pages
     */
    pBucket->numOfAvailPages += (consumedPgs - newPgs);
  }
}

void releaseBucket(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx) {
  if (segIdx < 0 || segIdx > pMemBucket->numOfSegs || slotIdx < 0) {
    return;
  }

  tMemBucketSegment *pSeg = &pMemBucket->pSegs[segIdx];
  if (slotIdx < 0 || slotIdx >= pSeg->numOfSlots || pSeg->pBuffer[slotIdx] == NULL) {
    return;
  }

  tExtMemBufferDestroy(&pSeg->pBuffer[slotIdx]);
}

static FORCE_INLINE int32_t primaryKeyComparator(int64_t f1, int64_t f2, int32_t colIdx, int32_t tsOrder) {
  if (f1 == f2) {
    return 0;
  }

  if (colIdx == 0 && tsOrder == TSQL_SO_DESC) {  // primary column desc order
    return (f1 < f2) ? 1 : -1;
  } else {  // asc
    return (f1 < f2) ? -1 : 1;
  }
}

static FORCE_INLINE int32_t columnValueAscendingComparator(char *f1, char *f2, int32_t type, int32_t bytes) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t first = *(int32_t *)f1;
      int32_t second = *(int32_t *)f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double first = *(double *)f1;
      double second = *(double *)f2;
      if (first == second) {
        return 0;
      }
      return (first < second) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      float first = *(float *)f1;
      float second = *(float *)f2;
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
      int32_t ret = strncmp(f1, f2, bytes);
      if (ret == 0) {
        return 0;
      }
      return (ret < 0) ? -1 : 1;
    };
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t b = bytes / TSDB_NCHAR_SIZE;
      int32_t ret = wcsncmp((wchar_t *)f1, (wchar_t *)f2, b);
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

  int32_t cmpCnt = pDescriptor->orderIdx.numOfOrderedCols;
  for (int32_t i = 0; i < cmpCnt; ++i) {
    int32_t colIdx = pDescriptor->orderIdx.pData[i];

    char *f1 = COLMODEL_GET_VAL(data1, pDescriptor->pSchema, numOfRows1, s1, colIdx);
    char *f2 = COLMODEL_GET_VAL(data2, pDescriptor->pSchema, numOfRows2, s2, colIdx);

    if (pDescriptor->pSchema->pFields[colIdx].type == TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t ret = primaryKeyComparator(*(int64_t *)f1, *(int64_t *)f2, colIdx, pDescriptor->tsOrder);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
    } else {
      SSchema *pSchema = &pDescriptor->pSchema->pFields[colIdx];
      int32_t  ret = columnValueAscendingComparator(f1, f2, pSchema->type, pSchema->bytes);
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

  int32_t cmpCnt = pDescriptor->orderIdx.numOfOrderedCols;
  for (int32_t i = 0; i < cmpCnt; ++i) {
    int32_t colIdx = pDescriptor->orderIdx.pData[i];

    char *f1 = COLMODEL_GET_VAL(data1, pDescriptor->pSchema, numOfRows1, s1, colIdx);
    char *f2 = COLMODEL_GET_VAL(data2, pDescriptor->pSchema, numOfRows2, s2, colIdx);

    if (pDescriptor->pSchema->pFields[colIdx].type == TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t ret = primaryKeyComparator(*(int64_t *)f1, *(int64_t *)f2, colIdx, pDescriptor->tsOrder);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
    } else {
      SSchema *pSchema = &pDescriptor->pSchema->pFields[colIdx];
      int32_t  ret = columnValueAscendingComparator(f1, f2, pSchema->type, pSchema->bytes);
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

static void swap(tOrderDescriptor *pDescriptor, int32_t count, int32_t s1, char *data1, int32_t s2) {
  for (int32_t i = 0; i < pDescriptor->pSchema->numOfCols; ++i) {
    void *first = COLMODEL_GET_VAL(data1, pDescriptor->pSchema, count, s1, i);
    void *second = COLMODEL_GET_VAL(data1, pDescriptor->pSchema, count, s2, i);

    tsDataSwap(first, second, pDescriptor->pSchema->pFields[i].type, pDescriptor->pSchema->pFields[i].bytes);
  }
}

static void tColDataInsertSort(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data,
                               __col_compar_fn_t compareFn) {
  for (int32_t i = start + 1; i <= end; ++i) {
    for (int32_t j = i; j > start; --j) {
      if (compareFn(pDescriptor, numOfRows, j, j - 1, data) == -1) {
        swap(pDescriptor, numOfRows, j - 1, data, j);
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
      printf("%s:(%lld, %lld, %lld)\n", prefix, *(int64_t *)startx, *(int64_t *)midx, *(int64_t *)endx);
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
                   __col_compar_fn_t compareFn) {
  int32_t midIdx = ((end - start) >> 1) + start;

#if defined(_DEBUG_VIEW)
  int32_t f = pDescriptor->orderIdx.pData[0];

  char *midx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, midIdx, f);
  char *startx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, start, f);
  char *endx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, end, f);

  int32_t colIdx = pDescriptor->orderIdx.pData[0];
  tSortDataPrint(pDescriptor->pSchema->pFields[colIdx].type, "before", startx, midx, endx);
#endif

  if (compareFn(pDescriptor, numOfRows, midIdx, start, data) == 1) {
    swap(pDescriptor, numOfRows, start, data, midIdx);
  }

  if (compareFn(pDescriptor, numOfRows, midIdx, end, data) == 1) {
    swap(pDescriptor, numOfRows, midIdx, data, start);
    swap(pDescriptor, numOfRows, midIdx, data, end);
  } else if (compareFn(pDescriptor, numOfRows, start, end, data) == 1) {
    swap(pDescriptor, numOfRows, start, data, end);
  }

  assert(compareFn(pDescriptor, numOfRows, midIdx, start, data) <= 0 &&
         compareFn(pDescriptor, numOfRows, start, end, data) <= 0);

#if defined(_DEBUG_VIEW)
  midx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, midIdx, f);
  startx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, start, f);
  endx = COLMODEL_GET_VAL(data, pDescriptor->pSchema, numOfRows, end, f);
  tSortDataPrint(pDescriptor->pSchema->pFields[colIdx].type, "after", startx, midx, endx);
#endif
}

static UNUSED_FUNC void tRowModelDisplay(tOrderDescriptor *pDescriptor, int32_t numOfRows, char *d, int32_t len) {
  int32_t colIdx = pDescriptor->orderIdx.pData[0];

  for (int32_t i = 0; i < len; ++i) {
    char *startx = COLMODEL_GET_VAL(d, pDescriptor->pSchema, numOfRows, i, colIdx);

    switch (pDescriptor->pSchema->pFields[colIdx].type) {
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
        printf("%lld\t", *(int64_t *)startx);
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

static int32_t qsort_call = 0;

void tColDataQSort(tOrderDescriptor *pDescriptor, int32_t numOfRows, int32_t start, int32_t end, char *data,
                   int32_t orderType) {
  // short array sort, incur another sort procedure instead of quick sort process
  __col_compar_fn_t compareFn = (orderType == TSQL_SO_ASC) ? compare_sa : compare_sd;

  if (end - start + 1 <= 8) {
    tColDataInsertSort(pDescriptor, numOfRows, start, end, data, compareFn);
    return;
  }

#ifdef _DEBUG_VIEW
  printf("before sort:\n");
  tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif

  int32_t s = start, e = end;
  median(pDescriptor, numOfRows, start, end, data, compareFn);

#ifdef _DEBUG_VIEW
  printf("%s called: %d\n", __FUNCTION__, qsort_call++);
#endif

  UNUSED(qsort_call);

  int32_t end_same = end;
  int32_t start_same = start;

  while (s < e) {
    while (e > s) {
      int32_t ret = compareFn(pDescriptor, numOfRows, e, s, data);
      if (ret < 0) {
        break;
      }

      if (ret == 0 && e != end_same) {
        swap(pDescriptor, numOfRows, e, data, end_same--);
      }
      e--;
    }

    if (e != s) {
      swap(pDescriptor, numOfRows, s, data, e);
    }

#ifdef _DEBUG_VIEW
    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif

    while (s < e) {
      int32_t ret = compareFn(pDescriptor, numOfRows, s, e, data);
      if (ret > 0) {
        break;
      }

      if (ret == 0 && s != start_same) {
        swap(pDescriptor, numOfRows, s, data, start_same++);
      }
      s++;
    }

    if (s != e) {
      swap(pDescriptor, numOfRows, s, data, e);
    }
#ifdef _DEBUG_VIEW
    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  int32_t rightx = e + 1;
  if (end_same != end && e < end) {  // move end data to around the pivotal
    int32_t left = e + 1;
    int32_t right = end;

    while (right > end_same && left <= end_same) {
      swap(pDescriptor, numOfRows, left++, data, right--);
    }
    rightx += (end - end_same);  // (pivotal+1) + steps of number that are identical pivotal

#ifdef _DEBUG_VIEW
    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  int32_t leftx = e - 1;
  if (start_same != start && s > start) {
    int32_t left = start;
    int32_t right = e - 1;

    while (left < start_same && right >= start_same) {
      swap(pDescriptor, numOfRows, left++, data, right--);
    }
    leftx -= (start_same - start);  // (pivotal-1) - steps of number that are identical pivotal

#ifdef _DEBUG_VIEW
    tRowModelDisplay(pDescriptor, numOfRows, data, end - start + 1);
#endif
  }

  if (leftx > start) {
    tColDataQSort(pDescriptor, numOfRows, start, leftx, data, orderType);
  }

  if (rightx < end) {
    tColDataQSort(pDescriptor, numOfRows, rightx, end, data, orderType);
  }
}

tExtMemBuffer *releaseBucketsExceptFor(tMemBucket *pMemBucket, int16_t segIdx, int16_t slotIdx) {
  tExtMemBuffer *pBuffer = NULL;

  for (int32_t i = 0; i < pMemBucket->numOfSegs; ++i) {
    tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];

    for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
      if (i == segIdx && j == slotIdx) {
        pBuffer = pSeg->pBuffer[j];
      } else {
        if (pSeg->pBuffer && pSeg->pBuffer[j]) {
          tExtMemBufferDestroy(&pSeg->pBuffer[j]);
        }
      }
    }
  }

  return pBuffer;
}

static tFilePage *loadIntoBucketFromDisk(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx,
                                         tOrderDescriptor *pDesc) {
  // release all data in other slots
  tExtMemBuffer *pMemBuffer = pMemBucket->pSegs[segIdx].pBuffer[slotIdx];
  tFilePage *    buffer = (tFilePage *)calloc(1, pMemBuffer->nElemSize * pMemBuffer->numOfAllElems + sizeof(tFilePage));
  int32_t        oldCapacity = pDesc->pSchema->maxCapacity;
  pDesc->pSchema->maxCapacity = pMemBuffer->numOfAllElems;

  if (!tExtMemBufferIsAllDataInMem(pMemBuffer)) {
    pMemBuffer = releaseBucketsExceptFor(pMemBucket, segIdx, slotIdx);
    assert(pMemBuffer->numOfAllElems > 0);

    // load data in disk to memory
    tFilePage *pPage = (tFilePage *)calloc(1, pMemBuffer->nPageSize);

    for (int32_t i = 0; i < pMemBuffer->fileMeta.flushoutData.nLength; ++i) {
      tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[i];

      int32_t ret = fseek(pMemBuffer->dataFile, pFlushInfo->startPageId * pMemBuffer->nPageSize, SEEK_SET);
      UNUSED(ret);

      for (uint32_t j = 0; j < pFlushInfo->numOfPages; ++j) {
        ret = fread(pPage, pMemBuffer->nPageSize, 1, pMemBuffer->dataFile);
        assert(pPage->numOfElems > 0);

        tColModelAppend(pDesc->pSchema, buffer, pPage->data, 0, pPage->numOfElems, pPage->numOfElems);
        printf("id: %d  count: %d\n", j, buffer->numOfElems);
      }
    }
    tfree(pPage);

    assert(buffer->numOfElems == pMemBuffer->fileMeta.numOfElemsInFile);
  }

  // load data in pMemBuffer to buffer
  tFilePagesItem *pListItem = pMemBuffer->pHead;
  while (pListItem != NULL) {
    tColModelAppend(pDesc->pSchema, buffer, pListItem->item.data, 0, pListItem->item.numOfElems,
                    pListItem->item.numOfElems);
    pListItem = pListItem->pNext;
  }

  tColDataQSort(pDesc, buffer->numOfElems, 0, buffer->numOfElems - 1, buffer->data, TSQL_SO_ASC);

  pDesc->pSchema->maxCapacity = oldCapacity;  // restore value
  return buffer;
}

double findOnlyResult(tMemBucket *pMemBucket) {
  assert(pMemBucket->numOfElems == 1);

  for (int32_t i = 0; i < pMemBucket->numOfSegs; ++i) {
    tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];
    if (pSeg->pBuffer) {
      for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
        tExtMemBuffer *pBuffer = pSeg->pBuffer[j];
        if (pBuffer) {
          assert(pBuffer->numOfAllElems == 1);
          tFilePage *pPage = &pBuffer->pHead->item;
          if (pBuffer->numOfElemsInBuffer == 1) {
            switch (pMemBucket->dataType) {
              case TSDB_DATA_TYPE_INT:
                return *(int32_t *)pPage->data;
              case TSDB_DATA_TYPE_SMALLINT:
                return *(int16_t *)pPage->data;
              case TSDB_DATA_TYPE_TINYINT:
                return *(int8_t *)pPage->data;
              case TSDB_DATA_TYPE_BIGINT:
                return (double)(*(int64_t *)pPage->data);
              case TSDB_DATA_TYPE_DOUBLE:
                return *(double *)pPage->data;
              case TSDB_DATA_TYPE_FLOAT:
                return *(float *)pPage->data;
              default:
                return 0;
            }
          }
        }
      }
    }
  }
  return 0;
}

/*
 * deep copy of sschema
 */
tColModel *tColModelCreate(SSchema *field, int32_t numOfCols, int32_t maxCapacity) {
  tColModel *pSchema =
      (tColModel *)calloc(1, sizeof(tColModel) + numOfCols * sizeof(SSchema) + numOfCols * sizeof(int16_t));
  if (pSchema == NULL) {
    return NULL;
  }

  pSchema->pFields = (SSchema *)(&pSchema[1]);
  memcpy(pSchema->pFields, field, sizeof(SSchema) * numOfCols);

  pSchema->colOffset = (int16_t *)(&pSchema->pFields[numOfCols]);
  pSchema->colOffset[0] = 0;
  for (int32_t i = 1; i < numOfCols; ++i) {
    pSchema->colOffset[i] = pSchema->colOffset[i - 1] + pSchema->pFields[i - 1].bytes;
  }

  pSchema->numOfCols = numOfCols;
  pSchema->maxCapacity = maxCapacity;

  return pSchema;
}

void tColModelDestroy(tColModel *pModel) {
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
    printf("%lf,%d\t", *(double *)data, *(int64_t *)(data + sizeof(double)));
  } else if (data[8] == ',') {  // in TSDB_FUNC_FIRST_DST/TSDB_FUNC_LAST_DST,
                                // the value is seperated by ','
    printf("%ld,%0x\t", *(int64_t *)data, data + sizeof(int64_t) + 1);
  } else if (isCharString) {
    printf("%s\t", data);
  }
}

// todo cast to struct to extract data
static void printBinaryDataEx(char *data, int32_t len, SSrcColumnInfo *param) {
  if (param->functionId == TSDB_FUNC_LAST_DST) {
    switch (param->type) {
      case TSDB_DATA_TYPE_TINYINT:
        printf("%lld,%d\t", *(int64_t *)data, *(int8_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%lld,%d\t", *(int64_t *)data, *(int16_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:
        printf("%lld,%lld\t", *(int64_t *)data, *(int64_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%lld,%d\t", *(int64_t *)data, *(float *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%lld,%d\t", *(int64_t *)data, *(double *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_BINARY:
        printf("%lld,%s\t", *(int64_t *)data, (data + TSDB_KEYSIZE + 1));
        break;

      case TSDB_DATA_TYPE_INT:
      default:
        printf("%lld,%d\t", *(int64_t *)data, *(int32_t *)(data + TSDB_KEYSIZE + 1));
        break;
    }
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
        printf("%lld\t", *(int64_t *)data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%d\t", *(float *)data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%d\t", *(double *)data);
        break;
      case TSDB_DATA_TYPE_BINARY:
        printf("%s\t", data);
        break;

      case TSDB_DATA_TYPE_INT:
      default:
        printf("%d\t", *(int32_t *)data);
        break;
    }
  }
}

void tColModelDisplay(tColModel *pModel, void *pData, int32_t numOfRows, int32_t totalCapacity) {
  for (int32_t i = 0; i < numOfRows; ++i) {
    for (int32_t j = 0; j < pModel->numOfCols; ++j) {
      char *val = COLMODEL_GET_VAL((char *)pData, pModel, totalCapacity, i, j);

      printf("type:%d\t", pModel->pFields[j].type);

      switch (pModel->pFields[j].type) {
        case TSDB_DATA_TYPE_BIGINT:
          printf("%lld\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)val);
          break;
        case TSDB_DATA_TYPE_NCHAR: {
          char buf[4096] = {0};
          taosUcs4ToMbs(val, pModel->pFields[j].bytes, buf);
          printf("%s\t", buf);
        }
        case TSDB_DATA_TYPE_BINARY: {
          printBinaryData(val, pModel->pFields[j].bytes);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)val);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          printf("%lld\t", *(int64_t *)val);
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

void tColModelDisplayEx(tColModel *pModel, void *pData, int32_t numOfRows, int32_t totalCapacity,
                        SSrcColumnInfo *param) {
  for (int32_t i = 0; i < numOfRows; ++i) {
    for (int32_t j = 0; j < pModel->numOfCols; ++j) {
      char *val = COLMODEL_GET_VAL((char *)pData, pModel, totalCapacity, i, j);

      printf("type:%d\t", pModel->pFields[j].type);

      switch (pModel->pFields[j].type) {
        case TSDB_DATA_TYPE_BIGINT:
          printf("%lld\t", *(int64_t *)val);
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)val);
          break;
        case TSDB_DATA_TYPE_NCHAR: {
          char buf[128] = {0};
          taosUcs4ToMbs(val, pModel->pFields[j].bytes, buf);
          printf("%s\t", buf);
        }
        case TSDB_DATA_TYPE_BINARY: {
          printBinaryDataEx(val, pModel->pFields[j].bytes, &param[j]);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)val);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          printf("%lld\t", *(int64_t *)val);
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
void tColModelCompact(tColModel *pModel, tFilePage *inputBuffer, int32_t maxElemsCapacity) {
  if (inputBuffer->numOfElems == 0 || maxElemsCapacity == inputBuffer->numOfElems) {
    return;
  }

  /* start from the second column */
  for (int32_t m = 1; m < pModel->numOfCols; ++m) {
    memmove(inputBuffer->data + pModel->colOffset[m] * inputBuffer->numOfElems,
            inputBuffer->data + pModel->colOffset[m] * maxElemsCapacity,
            pModel->pFields[m].bytes * inputBuffer->numOfElems);
  }
}

void tColModelErase(tColModel *pModel, tFilePage *inputBuffer, int32_t maxCapacity, int32_t s, int32_t e) {
  if (inputBuffer->numOfElems == 0 || (e - s + 1) <= 0) {
    return;
  }

  int32_t removed = e - s + 1;
  int32_t remain = inputBuffer->numOfElems - removed;
  int32_t secPart = inputBuffer->numOfElems - e - 1;

  /* start from the second column */
  for (int32_t m = 0; m < pModel->numOfCols; ++m) {
    char *startPos = inputBuffer->data + pModel->colOffset[m] * maxCapacity + s * pModel->pFields[m].bytes;
    char *endPos = startPos + pModel->pFields[m].bytes * removed;

    memmove(startPos, endPos, pModel->pFields[m].bytes * secPart);
  }

  inputBuffer->numOfElems = remain;
}

/*
 * column format data block append function
 * used in write record(s) to exist column-format block
 *
 * data in srcData must has the same schema as data in dstPage, that can be
 * described by dstModel
 */
void tColModelAppend(tColModel *dstModel, tFilePage *dstPage, void *srcData, int32_t start, int32_t numOfRows,
                     int32_t srcCapacity) {
  assert(dstPage->numOfElems + numOfRows <= dstModel->maxCapacity);

  for (int32_t col = 0; col < dstModel->numOfCols; ++col) {
    char *dst = COLMODEL_GET_VAL(dstPage->data, dstModel, dstModel->maxCapacity, dstPage->numOfElems, col);
    char *src = COLMODEL_GET_VAL((char *)srcData, dstModel, srcCapacity, start, col);

    memmove(dst, src, dstModel->pFields[col].bytes * numOfRows);
  }

  dstPage->numOfElems += numOfRows;
}

tOrderDescriptor *tOrderDesCreate(int32_t *orderColIdx, int32_t numOfOrderCols, tColModel *pModel,
                                  int32_t tsOrderType) {
  tOrderDescriptor *desc = (tOrderDescriptor *)malloc(sizeof(tOrderDescriptor) + sizeof(int32_t) * numOfOrderCols);
  if (desc == NULL) {
    return NULL;
  }

  desc->pSchema = pModel;
  desc->tsOrder = tsOrderType;

  desc->orderIdx.numOfOrderedCols = numOfOrderCols;
  for (int32_t i = 0; i < numOfOrderCols; ++i) {
    desc->orderIdx.pData[i] = orderColIdx[i];
  }

  return desc;
}

void tOrderDescDestroy(tOrderDescriptor *pDesc) {
  if (pDesc == NULL) {
    return;
  }

  tColModelDestroy(pDesc->pSchema);
  tfree(pDesc);
}

////////////////////////////////////////////////////////////////////////////////////////////
static void findMaxMinValue(tMemBucket *pMemBucket, double *maxVal, double *minVal) {
  *minVal = DBL_MAX;
  *maxVal = -DBL_MAX;

  for (int32_t i = 0; i < pMemBucket->numOfSegs; ++i) {
    tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];
    if (pSeg->pBuffer == NULL) {
      continue;
    }
    switch (pMemBucket->dataType) {
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_TINYINT: {
        for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
          double minv = pSeg->pBoundingEntries[j].iMinVal;
          double maxv = pSeg->pBoundingEntries[j].iMaxVal;

          if (*minVal > minv) {
            *minVal = minv;
          }
          if (*maxVal < maxv) {
            *maxVal = maxv;
          }
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_FLOAT: {
        for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
          double minv = pSeg->pBoundingEntries[j].dMinVal;
          double maxv = pSeg->pBoundingEntries[j].dMaxVal;

          if (*minVal > minv) {
            *minVal = minv;
          }
          if (*maxVal < maxv) {
            *maxVal = maxv;
          }
        }
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
          double minv = (double)pSeg->pBoundingEntries[j].i64MinVal;
          double maxv = (double)pSeg->pBoundingEntries[j].i64MaxVal;

          if (*minVal > minv) {
            *minVal = minv;
          }
          if (*maxVal < maxv) {
            *maxVal = maxv;
          }
        }
        break;
      }
    }
  }
}

static MinMaxEntry getMinMaxEntryOfNearestSlotInNextSegment(tMemBucket *pMemBucket, int32_t segIdx) {
  int32_t i = segIdx + 1;
  while (i < pMemBucket->numOfSegs && pMemBucket->pSegs[i].numOfSlots == 0) ++i;

  tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];
  assert(pMemBucket->numOfSegs > i && pMemBucket->pSegs[i].pBuffer != NULL);

  i = 0;
  while (i < pMemBucket->nSlotsOfSeg && pSeg->pBuffer[i] == NULL) ++i;

  assert(i < pMemBucket->nSlotsOfSeg);
  return pSeg->pBoundingEntries[i];
}

/*
 *
 * now, we need to find the minimum value of the next slot for
 * interpolating the percentile value
 * j is the last slot of current segment, we need to get the first
 * slot of the next segment.
 */
static MinMaxEntry getMinMaxEntryOfNextSlotWithData(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx) {
  tMemBucketSegment *pSeg = &pMemBucket->pSegs[segIdx];

  MinMaxEntry next;
  if (slotIdx == pSeg->numOfSlots - 1) {  // find next segment with data
    return getMinMaxEntryOfNearestSlotInNextSegment(pMemBucket, segIdx);
  } else {
    int32_t j = slotIdx + 1;
    for (; j < pMemBucket->nSlotsOfSeg && pMemBucket->pSegs[segIdx].pBuffer[j] == 0; ++j) {
    };

    if (j == pMemBucket->nSlotsOfSeg) {  // current slot has no available
                                         // slot,try next segment
      return getMinMaxEntryOfNearestSlotInNextSegment(pMemBucket, segIdx);
    } else {
      next = pSeg->pBoundingEntries[slotIdx + 1];
      assert(pSeg->pBuffer[slotIdx + 1] != NULL);
    }
  }

  return next;
}

bool isIdenticalData(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx);
char *getFirstElemOfMemBuffer(tMemBucketSegment *pSeg, int32_t slotIdx, tFilePage *pPage);

double getPercentileImpl(tMemBucket *pMemBucket, int32_t count, double fraction) {
  int32_t num = 0;

  for (int32_t i = 0; i < pMemBucket->numOfSegs; ++i) {
    tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];
    for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
      if (pSeg->pBuffer == NULL || pSeg->pBuffer[j] == NULL) {
        continue;
      }
      // required value in current slot
      if (num < (count + 1) && num + pSeg->pBuffer[j]->numOfAllElems >= (count + 1)) {
        if (pSeg->pBuffer[j]->numOfAllElems + num == (count + 1)) {
          /*
           * now, we need to find the minimum value of the next slot for interpolating the percentile value
           * j is the last slot of current segment, we need to get the first slot of the next segment.
           *
           */
          MinMaxEntry next = getMinMaxEntryOfNextSlotWithData(pMemBucket, i, j);

          double maxOfThisSlot = 0;
          double minOfNextSlot = 0;
          switch (pMemBucket->dataType) {
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_TINYINT: {
              maxOfThisSlot = pSeg->pBoundingEntries[j].iMaxVal;
              minOfNextSlot = next.iMinVal;
              break;
            };
            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE: {
              maxOfThisSlot = pSeg->pBoundingEntries[j].dMaxVal;
              minOfNextSlot = next.dMinVal;
              break;
            };
            case TSDB_DATA_TYPE_BIGINT: {
              maxOfThisSlot = (double)pSeg->pBoundingEntries[j].i64MaxVal;
              minOfNextSlot = (double)next.i64MinVal;
              break;
            }
          };

          assert(minOfNextSlot > maxOfThisSlot);

          double val = (1 - fraction) * maxOfThisSlot + fraction * minOfNextSlot;
          return val;
        }
        if (pSeg->pBuffer[j]->numOfAllElems <= pMemBucket->maxElemsCapacity) {
          // data in buffer and file are merged together to be processed.
          tFilePage *buffer = loadIntoBucketFromDisk(pMemBucket, i, j, pMemBucket->pOrderDesc);
          int32_t    currentIdx = count - num;

          char * thisVal = buffer->data + pMemBucket->nElemSize * currentIdx;
          char * nextVal = thisVal + pMemBucket->nElemSize;
          double td, nd;
          switch (pMemBucket->dataType) {
            case TSDB_DATA_TYPE_SMALLINT: {
              td = *(int16_t *)thisVal;
              nd = *(int16_t *)nextVal;
              break;
            }
            case TSDB_DATA_TYPE_TINYINT: {
              td = *(int8_t *)thisVal;
              nd = *(int8_t *)nextVal;
              break;
            }
            case TSDB_DATA_TYPE_INT: {
              td = *(int32_t *)thisVal;
              nd = *(int32_t *)nextVal;
              break;
            };
            case TSDB_DATA_TYPE_FLOAT: {
              td = *(float *)thisVal;
              nd = *(float *)nextVal;
              break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
              td = *(double *)thisVal;
              nd = *(double *)nextVal;
              break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
              td = (double)*(int64_t *)thisVal;
              nd = (double)*(int64_t *)nextVal;
              break;
            }
          }
          double val = (1 - fraction) * td + fraction * nd;
          tfree(buffer);

          return val;
        } else {  // incur a second round bucket split
          if (isIdenticalData(pMemBucket, i, j)) {
            tExtMemBuffer *pMemBuffer = pSeg->pBuffer[j];

            tFilePage *pPage = (tFilePage *)malloc(pMemBuffer->nPageSize);

            char *thisVal = getFirstElemOfMemBuffer(pSeg, j, pPage);

            double finalResult = 0.0;

            switch (pMemBucket->dataType) {
              case TSDB_DATA_TYPE_SMALLINT: {
                finalResult = *(int16_t *)thisVal;
                break;
              }
              case TSDB_DATA_TYPE_TINYINT: {
                finalResult = *(int8_t *)thisVal;
                break;
              }
              case TSDB_DATA_TYPE_INT: {
                finalResult = *(int32_t *)thisVal;
                break;
              };
              case TSDB_DATA_TYPE_FLOAT: {
                finalResult = *(float *)thisVal;
                break;
              }
              case TSDB_DATA_TYPE_DOUBLE: {
                finalResult = *(double *)thisVal;
                break;
              }
              case TSDB_DATA_TYPE_BIGINT: {
                finalResult = (double)*(int64_t *)thisVal;
                break;
              }
            }

            free(pPage);
            return finalResult;
          }

          pTrace("MemBucket:%p,start second round bucketing", pMemBucket);

          if (pSeg->pBuffer[j]->numOfElemsInBuffer != 0) {
            pTrace("MemBucket:%p,flush %d pages to disk, clear status", pMemBucket, pSeg->pBuffer[j]->numOfPagesInMem);

            pMemBucket->numOfAvailPages += pSeg->pBuffer[j]->numOfPagesInMem;
            tExtMemBufferFlush(pSeg->pBuffer[j]);
          }

          tExtMemBuffer *pMemBuffer = pSeg->pBuffer[j];
          pSeg->pBuffer[j] = NULL;

          // release all
          for (int32_t tt = 0; tt < pMemBucket->numOfSegs; ++tt) {
            tMemBucketSegment *pSeg = &pMemBucket->pSegs[tt];
            for (int32_t ttx = 0; ttx < pSeg->numOfSlots; ++ttx) {
              if (pSeg->pBuffer && pSeg->pBuffer[ttx]) {
                tExtMemBufferDestroy(&pSeg->pBuffer[ttx]);
              }
            }
          }

          pMemBucket->nRange.i64MaxVal = pSeg->pBoundingEntries->i64MaxVal;
          pMemBucket->nRange.i64MinVal = pSeg->pBoundingEntries->i64MinVal;
          pMemBucket->numOfElems = 0;

          for (int32_t tt = 0; tt < pMemBucket->numOfSegs; ++tt) {
            tMemBucketSegment *pSeg = &pMemBucket->pSegs[tt];
            for (int32_t ttx = 0; ttx < pSeg->numOfSlots; ++ttx) {
              if (pSeg->pBoundingEntries) {
                resetBoundingBox(pSeg, pMemBucket->dataType);
              }
            }
          }

          tFilePage *pPage = (tFilePage *)malloc(pMemBuffer->nPageSize);

          tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[0];
          assert(pFlushInfo->numOfPages == pMemBuffer->fileMeta.nFileSize);

          int32_t ret = fseek(pMemBuffer->dataFile, pFlushInfo->startPageId * pMemBuffer->nPageSize, SEEK_SET);
          UNUSED(ret);

          for (uint32_t jx = 0; jx < pFlushInfo->numOfPages; ++jx) {
            ret = fread(pPage, pMemBuffer->nPageSize, 1, pMemBuffer->dataFile);
            tMemBucketPut(pMemBucket, pPage->data, pPage->numOfElems);
          }

          fclose(pMemBuffer->dataFile);
          if (unlink(pMemBuffer->dataFilePath) != 0) {
            pError("MemBucket:%p,remove tmp file %s failed", pMemBucket, pMemBuffer->dataFilePath);
          }
          tfree(pMemBuffer);
          tfree(pPage);

          return getPercentileImpl(pMemBucket, count - num, fraction);
        }
      } else {
        num += pSeg->pBuffer[j]->numOfAllElems;
      }
    }
  }
  return 0;
}

double getPercentile(tMemBucket *pMemBucket, double percent) {
  if (pMemBucket->numOfElems == 0) {
    return 0.0;
  }

  if (pMemBucket->numOfElems == 1) {  // return the only element
    return findOnlyResult(pMemBucket);
  }

  percent = fabs(percent);

  // validate the parameters
  if (fabs(percent - 100.0) < DBL_EPSILON || (percent < DBL_EPSILON)) {
    double minx = 0, maxx = 0;
    /*
     * find the min/max value, no need to scan all data in bucket
     */
    findMaxMinValue(pMemBucket, &maxx, &minx);

    return fabs(percent - 100) < DBL_EPSILON ? maxx : minx;
  }

  double  percentVal = (percent * (pMemBucket->numOfElems - 1)) / ((double)100.0);
  int32_t orderIdx = (int32_t)percentVal;

  // do put data by using buckets
  return getPercentileImpl(pMemBucket, orderIdx, percentVal - orderIdx);
}

/*
 * check if data in one slot are all identical
 * only need to compare with the bounding box
 */
bool isIdenticalData(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx) {
  tMemBucketSegment *pSeg = &pMemBucket->pSegs[segIdx];

  if (pMemBucket->dataType == TSDB_DATA_TYPE_INT || pMemBucket->dataType == TSDB_DATA_TYPE_BIGINT ||
      pMemBucket->dataType == TSDB_DATA_TYPE_SMALLINT || pMemBucket->dataType == TSDB_DATA_TYPE_TINYINT) {
    return pSeg->pBoundingEntries[slotIdx].i64MinVal == pSeg->pBoundingEntries[slotIdx].i64MaxVal;
  }

  if (pMemBucket->dataType == TSDB_DATA_TYPE_FLOAT || pMemBucket->dataType == TSDB_DATA_TYPE_DOUBLE) {
    return fabs(pSeg->pBoundingEntries[slotIdx].dMaxVal - pSeg->pBoundingEntries[slotIdx].dMinVal) < DBL_EPSILON;
  }

  return false;
}

/*
 * get the first element of one slot into memory.
 * if no data of current slot in memory, load it from disk
 */
char *getFirstElemOfMemBuffer(tMemBucketSegment *pSeg, int32_t slotIdx, tFilePage *pPage) {
  tExtMemBuffer *pMemBuffer = pSeg->pBuffer[slotIdx];
  char *         thisVal = NULL;

  if (pSeg->pBuffer[slotIdx]->numOfElemsInBuffer != 0) {
    thisVal = pSeg->pBuffer[slotIdx]->pHead->item.data;
  } else {
    /*
     * no data in memory, load one page into memory
     */
    tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[0];
    assert(pFlushInfo->numOfPages == pMemBuffer->fileMeta.nFileSize);

    fseek(pMemBuffer->dataFile, pFlushInfo->startPageId * pMemBuffer->nPageSize, SEEK_SET);
    size_t ret = fread(pPage, pMemBuffer->nPageSize, 1, pMemBuffer->dataFile);
    UNUSED(ret);
    thisVal = pPage->data;
  }
  return thisVal;
}
