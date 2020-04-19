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
#include "tulog.h"
#include "qpercentile.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "queryLog.h"

tExtMemBuffer *releaseBucketsExceptFor(tMemBucket *pMemBucket, int16_t segIdx, int16_t slotIdx) {
  tExtMemBuffer *pBuffer = NULL;
  
  for (int32_t i = 0; i < pMemBucket->numOfSegs; ++i) {
    tMemBucketSegment *pSeg = &pMemBucket->pSegs[i];
    
    for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
      if (i == segIdx && j == slotIdx) {
        pBuffer = pSeg->pBuffer[j];
      } else {
        if (pSeg->pBuffer && pSeg->pBuffer[j]) {
          pSeg->pBuffer[j] = destoryExtMemBuffer(pSeg->pBuffer[j]);
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
  tFilePage *    buffer = (tFilePage *)calloc(1, pMemBuffer->nElemSize * pMemBuffer->numOfTotalElems + sizeof(tFilePage));
  int32_t        oldCapacity = pDesc->pColumnModel->capacity;
  pDesc->pColumnModel->capacity = pMemBuffer->numOfTotalElems;
  
  if (!tExtMemBufferIsAllDataInMem(pMemBuffer)) {
    pMemBuffer = releaseBucketsExceptFor(pMemBucket, segIdx, slotIdx);
    assert(pMemBuffer->numOfTotalElems > 0);
    
    // load data in disk to memory
    tFilePage *pPage = (tFilePage *)calloc(1, pMemBuffer->pageSize);
    
    for (int32_t i = 0; i < pMemBuffer->fileMeta.flushoutData.nLength; ++i) {
      tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[i];
      
      int32_t ret = fseek(pMemBuffer->file, pFlushInfo->startPageId * pMemBuffer->pageSize, SEEK_SET);
      UNUSED(ret);
      
      for (uint32_t j = 0; j < pFlushInfo->numOfPages; ++j) {
        ret = fread(pPage, pMemBuffer->pageSize, 1, pMemBuffer->file);
        UNUSED(ret);
        assert(pPage->numOfElems > 0);
        
        tColModelAppend(pDesc->pColumnModel, buffer, pPage->data, 0, pPage->numOfElems, pPage->numOfElems);
        printf("id: %d  count: %" PRIu64 "\n", j, buffer->numOfElems);
      }
    }
    tfree(pPage);
    
    assert(buffer->numOfElems == pMemBuffer->fileMeta.numOfElemsInFile);
  }
  
  // load data in pMemBuffer to buffer
  tFilePagesItem *pListItem = pMemBuffer->pHead;
  while (pListItem != NULL) {
    tColModelAppend(pDesc->pColumnModel, buffer, pListItem->item.data, 0, pListItem->item.numOfElems,
                    pListItem->item.numOfElems);
    pListItem = pListItem->pNext;
  }
  
  tColDataQSort(pDesc, buffer->numOfElems, 0, buffer->numOfElems - 1, buffer->data, TSDB_ORDER_ASC);
  
  pDesc->pColumnModel->capacity = oldCapacity;  // restore value
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
          assert(pBuffer->numOfTotalElems == 1);
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
              case TSDB_DATA_TYPE_DOUBLE: {
                double dv = GET_DOUBLE_VAL(pPage->data);
                //return *(double *)pPage->data;
                return dv;
              }
              case TSDB_DATA_TYPE_FLOAT: {
                float fv = GET_FLOAT_VAL(pPage->data);
                //return *(float *)pPage->data;
                return fv;
              }
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
  // double v = *(double *)value;
  double v = GET_DOUBLE_VAL(value);

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
      uError("error in hash process. segment is: %d, slot id is: %d\n", *segIdx, *slotIdx);
    }
  }
}

tMemBucket *tMemBucketCreate(int32_t totalSlots, int32_t nBufferSize, int16_t nElemSize, int16_t dataType,
                             tOrderDescriptor *pDesc) {
  tMemBucket *pBucket = (tMemBucket *)malloc(sizeof(tMemBucket));

  pBucket->nTotalSlots = totalSlots;
  pBucket->nSlotsOfSeg = 1 << 6;  // 64 Segments, 16 slots each seg.
  pBucket->dataType = dataType;
  pBucket->nElemSize = nElemSize;
  pBucket->pageSize = DEFAULT_PAGE_SIZE;

  pBucket->numOfElems = 0;
  pBucket->numOfSegs = pBucket->nTotalSlots / pBucket->nSlotsOfSeg;

  pBucket->nTotalBufferSize = nBufferSize;

  pBucket->maxElemsCapacity = pBucket->nTotalBufferSize / pBucket->nElemSize;

  pBucket->numOfTotalPages = pBucket->nTotalBufferSize / pBucket->pageSize;
  pBucket->numOfAvailPages = pBucket->numOfTotalPages;

  pBucket->pOrderDesc = pDesc;

  switch (pBucket->dataType) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT: {
      pBucket->nRange.iMinVal = INT32_MAX;
      pBucket->nRange.iMaxVal = INT32_MIN;
      pBucket->HashFunc = tBucketIntHash;
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      pBucket->nRange.dMinVal = DBL_MAX;
      pBucket->nRange.dMaxVal = -DBL_MAX;
      pBucket->HashFunc = tBucketDoubleHash;
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      pBucket->nRange.i64MinVal = INT64_MAX;
      pBucket->nRange.i64MaxVal = INT64_MIN;
      pBucket->HashFunc = tBucketBigIntHash;
      break;
    };
    default: {
      uError("MemBucket:%p,not support data type %d,failed", *pBucket, pBucket->dataType);
      tfree(pBucket);
      return NULL;
    }
  }

  int32_t numOfCols = pDesc->pColumnModel->numOfCols;
  if (numOfCols != 1) {
    uError("MemBucket:%p,only consecutive data is allowed,invalid numOfCols:%d", pBucket, numOfCols);
    tfree(pBucket);
    return NULL;
  }

  SSchema* pSchema = getColumnModelSchema(pDesc->pColumnModel, 0);
  if (pSchema->type != dataType) {
    uError("MemBucket:%p,data type is not consistent,%d in schema, %d in param", pBucket, pSchema->type, dataType);
    tfree(pBucket);
    return NULL;
  }

  if (pBucket->numOfTotalPages < pBucket->nTotalSlots) {
    uWarn("MemBucket:%p,total buffer pages %d are not enough for all slots", pBucket, pBucket->numOfTotalPages);
  }

  pBucket->pSegs = (tMemBucketSegment *)malloc(pBucket->numOfSegs * sizeof(tMemBucketSegment));

  for (int32_t i = 0; i < pBucket->numOfSegs; ++i) {
    pBucket->pSegs[i].numOfSlots = pBucket->nSlotsOfSeg;
    pBucket->pSegs[i].pBuffer = NULL;
    pBucket->pSegs[i].pBoundingEntries = NULL;
  }

  uTrace("MemBucket:%p,created,buffer size:%d,elem size:%d", pBucket, pBucket->numOfTotalPages * DEFAULT_PAGE_SIZE,
         pBucket->nElemSize);

  return pBucket;
}

void tMemBucketDestroy(tMemBucket *pBucket) {
  if (pBucket == NULL) {
    return;
  }

  if (pBucket->pSegs) {
    for (int32_t i = 0; i < pBucket->numOfSegs; ++i) {
      tMemBucketSegment *pSeg = &(pBucket->pSegs[i]);
      tfree(pSeg->pBoundingEntries);

      if (pSeg->pBuffer == NULL || pSeg->numOfSlots == 0) {
        continue;
      }

      for (int32_t j = 0; j < pSeg->numOfSlots; ++j) {
        if (pSeg->pBuffer[j] != NULL) {
          pSeg->pBuffer[j] = destoryExtMemBuffer(pSeg->pBuffer[j]);
        }
      }
      tfree(pSeg->pBuffer);
    }
  }

  tfree(pBucket->pSegs);
  tfree(pBucket);
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

      if (val < pSeg->pBuffer[i]->numOfInMemPages) {
        val = pSeg->pBuffer[i]->numOfInMemPages;
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
      // double val = *(double *)data;
      double val = GET_DOUBLE_VAL(data);
      if (r->dMinVal > val) {
        r->dMinVal = val;
      }

      if (r->dMaxVal < val) {
        r->dMaxVal = val;
      }
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      // double val = *(float *)data;
      double val = GET_FLOAT_VAL(data);

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
        // double val = *(double *)d;
        double val = GET_DOUBLE_VAL(d);
        (pBucket->HashFunc)(pBucket, &val, &segIdx, &slotIdx);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        // double val = *(float *)d;
        double val = GET_FLOAT_VAL(d);
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
      pSeg->pBuffer[slotIdx] = createExtMemBuffer(pBucket->numOfTotalPages * pBucket->pageSize, pBucket->nElemSize,
                                                  pBucket->pOrderDesc->pColumnModel);
      pSeg->pBuffer[slotIdx]->flushModel = SINGLE_APPEND_MODEL;
      pBucket->pOrderDesc->pColumnModel->capacity = pSeg->pBuffer[slotIdx]->numOfElemsPerPage;
    }

    tMemBucketUpdateBoundingBox(&pSeg->pBoundingEntries[slotIdx], d, pBucket->dataType);

    // ensure available memory pages to allocate
    int16_t cseg = 0, cslot = 0;
    if (pBucket->numOfAvailPages == 0) {
      uTrace("MemBucket:%p,max avail size:%d, no avail memory pages,", pBucket, pBucket->numOfTotalPages);

      tBucketGetMaxMemSlot(pBucket, &cseg, &cslot);
      if (cseg == -1 || cslot == -1) {
        uError("MemBucket:%p,failed to find appropriated avail buffer", pBucket);
        return;
      }

      if (cseg != segIdx || cslot != slotIdx) {
        pBucket->numOfAvailPages += pBucket->pSegs[cseg].pBuffer[cslot]->numOfInMemPages;

        int32_t avail = pBucket->pSegs[cseg].pBuffer[cslot]->numOfInMemPages;
        UNUSED(avail);
        tExtMemBufferFlush(pBucket->pSegs[cseg].pBuffer[cslot]);

        uTrace("MemBucket:%p,seg:%d,slot:%d flushed to disk,new avail pages:%d", pBucket, cseg, cslot,
               pBucket->numOfAvailPages);
      } else {
        uTrace("MemBucket:%p,failed to choose slot to flush to disk seg:%d,slot:%d", pBucket, cseg, cslot);
      }
    }
    int16_t consumedPgs = pSeg->pBuffer[slotIdx]->numOfInMemPages;

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

  pSeg->pBuffer[slotIdx] = destoryExtMemBuffer(pSeg->pBuffer[slotIdx]);
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

bool  isIdenticalData(tMemBucket *pMemBucket, int32_t segIdx, int32_t slotIdx);
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
      if (num < (count + 1) && num + pSeg->pBuffer[j]->numOfTotalElems >= (count + 1)) {
        if (pSeg->pBuffer[j]->numOfTotalElems + num == (count + 1)) {
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
        if (pSeg->pBuffer[j]->numOfTotalElems <= pMemBucket->maxElemsCapacity) {
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
              // td = *(float *)thisVal;
              // nd = *(float *)nextVal;
              td = GET_FLOAT_VAL(thisVal);
              nd = GET_FLOAT_VAL(nextVal);
              break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
              // td = *(double *)thisVal;
              td = GET_DOUBLE_VAL(thisVal);
              // nd = *(double *)nextVal;
              nd = GET_DOUBLE_VAL(nextVal);
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

            tFilePage *pPage = (tFilePage *)malloc(pMemBuffer->pageSize);

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
                // finalResult = *(float *)thisVal;
                finalResult = GET_FLOAT_VAL(thisVal);
                break;
              }
              case TSDB_DATA_TYPE_DOUBLE: {
                // finalResult = *(double *)thisVal;
                finalResult = GET_DOUBLE_VAL(thisVal);
                break;
              }
              case TSDB_DATA_TYPE_BIGINT: {
                finalResult = (double)(*(int64_t *)thisVal);
                break;
              }
            }

            free(pPage);
            return finalResult;
          }

          uTrace("MemBucket:%p,start second round bucketing", pMemBucket);

          if (pSeg->pBuffer[j]->numOfElemsInBuffer != 0) {
            uTrace("MemBucket:%p,flush %d pages to disk, clear status", pMemBucket, pSeg->pBuffer[j]->numOfInMemPages);

            pMemBucket->numOfAvailPages += pSeg->pBuffer[j]->numOfInMemPages;
            tExtMemBufferFlush(pSeg->pBuffer[j]);
          }

          tExtMemBuffer *pMemBuffer = pSeg->pBuffer[j];
          pSeg->pBuffer[j] = NULL;

          // release all
          for (int32_t tt = 0; tt < pMemBucket->numOfSegs; ++tt) {
            tMemBucketSegment *pSeg = &pMemBucket->pSegs[tt];
            for (int32_t ttx = 0; ttx < pSeg->numOfSlots; ++ttx) {
              if (pSeg->pBuffer && pSeg->pBuffer[ttx]) {
                pSeg->pBuffer[ttx] = destoryExtMemBuffer(pSeg->pBuffer[ttx]);
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

          tFilePage *pPage = (tFilePage *)malloc(pMemBuffer->pageSize);

          tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[0];
          assert(pFlushInfo->numOfPages == pMemBuffer->fileMeta.nFileSize);

          int32_t ret = fseek(pMemBuffer->file, pFlushInfo->startPageId * pMemBuffer->pageSize, SEEK_SET);
          UNUSED(ret);

          for (uint32_t jx = 0; jx < pFlushInfo->numOfPages; ++jx) {
            ret = fread(pPage, pMemBuffer->pageSize, 1, pMemBuffer->file);
            UNUSED(ret);
            tMemBucketPut(pMemBucket, pPage->data, pPage->numOfElems);
          }

          fclose(pMemBuffer->file);
          if (unlink(pMemBuffer->path) != 0) {
            uError("MemBucket:%p, remove tmp file %s failed", pMemBucket, pMemBuffer->path);
          }
          tfree(pMemBuffer);
          tfree(pPage);

          return getPercentileImpl(pMemBucket, count - num, fraction);
        }
      } else {
        num += pSeg->pBuffer[j]->numOfTotalElems;
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

    fseek(pMemBuffer->file, pFlushInfo->startPageId * pMemBuffer->pageSize, SEEK_SET);
    size_t ret = fread(pPage, pMemBuffer->pageSize, 1, pMemBuffer->file);
    UNUSED(ret);
    thisVal = pPage->data;
  }
  return thisVal;
}
