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

#include "qPercentile.h"
#include "qResultbuf.h"
#include "os.h"
#include "queryLog.h"
#include "taosdef.h"
#include "tulog.h"
#include "tcompare.h"

#define DEFAULT_NUM_OF_SLOT 1024

int32_t getGroupId(int32_t numOfSlots, int32_t slotIndex, int32_t times) {
  return (times * numOfSlots) + slotIndex;
}

static tFilePage *loadDataFromFilePage(tMemBucket *pMemBucket, int32_t slotIdx) {
  tFilePage *buffer = (tFilePage *)calloc(1, pMemBucket->bytes * pMemBucket->pSlots[slotIdx].info.size + sizeof(tFilePage));

  int32_t groupId = getGroupId(pMemBucket->numOfSlots, slotIdx, pMemBucket->times);
  SIDList list = getDataBufPagesIdList(pMemBucket->pBuffer, groupId);

  int32_t offset = 0;
  for(int32_t i = 0; i < list->size; ++i) {
    SPageInfo* pgInfo = *(SPageInfo**) taosArrayGet(list, i);

    tFilePage* pg = getResBufPage(pMemBucket->pBuffer, pgInfo->pageId);
    memcpy(buffer->data + offset, pg->data, (size_t)(pg->num * pMemBucket->bytes));

    offset += (int32_t)(pg->num * pMemBucket->bytes);
  }

  qsort(buffer->data, pMemBucket->pSlots[slotIdx].info.size, pMemBucket->bytes, pMemBucket->comparFn);
  return buffer;
}

static void resetBoundingBox(MinMaxEntry* range, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT: {
      range->i64MaxVal = INT64_MIN;
      range->i64MinVal = INT64_MAX;
      break;
    };
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT: {
      range->iMaxVal = INT32_MIN;
      range->iMinVal = INT32_MAX;
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      range->dMaxVal = -DBL_MAX;
      range->dMinVal = DBL_MAX;
      break;
    }
  }
}

static int32_t setBoundingBox(MinMaxEntry* range, int16_t type, double minval, double maxval) {
  if (minval > maxval) {
    return -1;
  }

  switch(type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
      range->iMinVal = (int32_t) minval;
      range->iMaxVal = (int32_t) maxval;
      break;

    case TSDB_DATA_TYPE_BIGINT:
      range->i64MinVal = (int64_t) minval;
      range->i64MaxVal = (int64_t) maxval;
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      range->dMinVal = minval;
      range->dMaxVal = maxval;
      break;
  }

  return 0;
}

static void resetPosInfo(SSlotInfo* pInfo) {
  pInfo->size   = 0;
  pInfo->pageId = -1;
  pInfo->data   = NULL;
}

double findOnlyResult(tMemBucket *pMemBucket) {
  assert(pMemBucket->total == 1);

  for (int32_t i = 0; i < pMemBucket->numOfSlots; ++i) {
    tMemBucketSlot *pSlot = &pMemBucket->pSlots[i];
    if (pSlot->info.size  == 0) {
      continue;
    }

    int32_t groupId = getGroupId(pMemBucket->numOfSlots, i, pMemBucket->times);
    SIDList list = getDataBufPagesIdList(pMemBucket->pBuffer, groupId);
    assert(list->size == 1);

    SPageInfo* pgInfo = (SPageInfo*) taosArrayGetP(list, 0);
    tFilePage* pPage = getResBufPage(pMemBucket->pBuffer, pgInfo->pageId);
    assert(pPage->num == 1);

    switch (pMemBucket->type) {
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
        return dv;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float fv = GET_FLOAT_VAL(pPage->data);
        return fv;
      }
      default:
        return 0;
    }
  }

  return 0;
}

int32_t tBucketBigIntHash(tMemBucket *pBucket, const void *value) {
  int64_t v = *(int64_t *)value;
  int32_t index = -1;

  int32_t halfSlot = pBucket->numOfSlots >> 1;
//  int32_t bits = 32;//bitsOfNumber(pBucket->numOfSlots) - 1;

  if (pBucket->range.i64MaxVal == INT64_MIN) {
    if (v >= 0) {
      index = (v >> (64 - 9)) + halfSlot;
    } else {  // v<0
      index = ((-v) >> (64 - 9));
      index = -index + (halfSlot - 1);
    }

    return index;
  } else {
    // out of range
    if (v < pBucket->range.i64MinVal || v > pBucket->range.i64MaxVal) {
      return -1;
    }

    // todo hash for bigint and float and double
    int64_t span = pBucket->range.i64MaxVal - pBucket->range.i64MinVal;
    if (span < pBucket->numOfSlots) {
      int32_t delta = (int32_t)(v - pBucket->range.i64MinVal);
      index = delta % pBucket->numOfSlots;
    } else {
      double slotSpan = (double)span / pBucket->numOfSlots;
      index = (int32_t)((v - pBucket->range.i64MinVal) / slotSpan);
      if (v == pBucket->range.i64MaxVal) {
        index -= 1;
      }
    }

    return index;
  }
}

// todo refactor to more generic
int32_t tBucketIntHash(tMemBucket *pBucket, const void *value) {
  int32_t v = 0;
  switch(pBucket->type) {
    case TSDB_DATA_TYPE_SMALLINT: v = *(int16_t*) value; break;
    case TSDB_DATA_TYPE_TINYINT: v = *(int8_t*) value; break;
    default: v = *(int32_t*) value;break;
  }

  int32_t index = -1;
  if (pBucket->range.iMaxVal == INT32_MIN) {
    /*
     * taking negative integer into consideration,
     * there is only half of pBucket->segs available for non-negative integer
     */
    int32_t halfSlot = pBucket->numOfSlots >> 1;
    int32_t bits = 32;//bitsOfNumber(pBucket->numOfSlots) - 1;

    if (v >= 0) {
      index = (v >> (bits - 9)) + halfSlot;
    } else {  // v < 0
      index = ((-v) >> (32 - 9));
      index = -index + (halfSlot - 1);
    }

    return index;
  } else {
    // out of range
    if (v < pBucket->range.iMinVal || v > pBucket->range.iMaxVal) {
      return -1;
    }

    // divide a range of [iMinVal, iMaxVal] into 1024 buckets
    int32_t span = pBucket->range.iMaxVal - pBucket->range.iMinVal;
    if (span < pBucket->numOfSlots) {
      int32_t delta = v - pBucket->range.iMinVal;
      index = (delta % pBucket->numOfSlots);
    } else {
      double slotSpan = (double)span / pBucket->numOfSlots;
      index = (int32_t)((v - pBucket->range.iMinVal) / slotSpan);
      if (v == pBucket->range.iMaxVal) {
        index -= 1;
      }
    }

    return index;
  }
}

int32_t tBucketDoubleHash(tMemBucket *pBucket, const void *value) {
  double v = 0;
  if (pBucket->type == TSDB_DATA_TYPE_FLOAT) {
    v = GET_FLOAT_VAL(value);
  } else {
    v = GET_DOUBLE_VAL(value);
  }

  int32_t index = -1;

  if (pBucket->range.dMinVal == DBL_MAX) {
    /*
     * taking negative integer into consideration,
     * there is only half of pBucket->segs available for non-negative integer
     */
    double x = DBL_MAX / (pBucket->numOfSlots >> 1);
    double posx = (v + DBL_MAX) / x;
    return ((int32_t)posx) % pBucket->numOfSlots;
  } else {

    // out of range
    if (v < pBucket->range.dMinVal || v > pBucket->range.dMaxVal) {
      return -1;
    }

    // divide a range of [dMinVal, dMaxVal] into 1024 buckets
    double span = pBucket->range.dMaxVal - pBucket->range.dMinVal;
    if (span < pBucket->numOfSlots) {
      int32_t delta = (int32_t)(v - pBucket->range.dMinVal);
      index = (delta % pBucket->numOfSlots);
    } else {
      double slotSpan = span / pBucket->numOfSlots;
      index = (int32_t)((v - pBucket->range.dMinVal) / slotSpan);
      if (v == pBucket->range.dMaxVal) {
        index -= 1;
      }
    }

    if (index < 0 || index > pBucket->numOfSlots) {
      uError("error in hash process. slot id: %d", index);
    }

    return index;
  }
}

static __perc_hash_func_t getHashFunc(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT: {
      return tBucketIntHash;
    };

    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      return tBucketDoubleHash;
    };

    case TSDB_DATA_TYPE_BIGINT: {
      return tBucketBigIntHash;
    };

    default: {
      return NULL;
    }
  }
}

static void resetSlotInfo(tMemBucket* pBucket) {
  for (int32_t i = 0; i < pBucket->numOfSlots; ++i) {
    tMemBucketSlot* pSlot = &pBucket->pSlots[i];

    resetBoundingBox(&pSlot->range, pBucket->type);
    resetPosInfo(&pSlot->info);
  }
}

tMemBucket *tMemBucketCreate(int16_t nElemSize, int16_t dataType, double minval, double maxval) {
  tMemBucket *pBucket = (tMemBucket *)calloc(1, sizeof(tMemBucket));
  if (pBucket == NULL) {
    return NULL;
  }

  pBucket->numOfSlots = DEFAULT_NUM_OF_SLOT;
  pBucket->bufPageSize = DEFAULT_PAGE_SIZE * 4;   // 4k per page

  pBucket->type  = dataType;
  pBucket->bytes = nElemSize;
  pBucket->total = 0;
  pBucket->times = 1;

  pBucket->maxCapacity = 200000;

  if (setBoundingBox(&pBucket->range, pBucket->type, minval, maxval) != 0) {
    uError("MemBucket:%p, invalid value range: %f-%f", pBucket, minval, maxval);
    free(pBucket);
    return NULL;
  }

  pBucket->elemPerPage = (pBucket->bufPageSize - sizeof(tFilePage))/pBucket->bytes;
  pBucket->comparFn = getKeyComparFunc(pBucket->type);

  pBucket->hashFunc = getHashFunc(pBucket->type);
  if (pBucket->hashFunc == NULL) {
    uError("MemBucket:%p, not support data type %d, failed", pBucket, pBucket->type);
    free(pBucket);
    return NULL;
  }

  pBucket->pSlots = (tMemBucketSlot *)calloc(pBucket->numOfSlots, sizeof(tMemBucketSlot));
  if (pBucket->pSlots == NULL) {
    free(pBucket);
    return NULL;
  }

  resetSlotInfo(pBucket);

  int32_t ret = createDiskbasedResultBuffer(&pBucket->pBuffer, pBucket->bytes, pBucket->bufPageSize, pBucket->bufPageSize * 512, NULL);
  if (ret != TSDB_CODE_SUCCESS) {
    tMemBucketDestroy(pBucket);
    return NULL;
  }
  
  uDebug("MemBucket:%p, elem size:%d", pBucket, pBucket->bytes);
  return pBucket;
}

void tMemBucketDestroy(tMemBucket *pBucket) {
  if (pBucket == NULL) {
    return;
  }

  destroyResultBuf(pBucket->pBuffer);
  tfree(pBucket->pSlots);
  tfree(pBucket);
}

void tMemBucketUpdateBoundingBox(MinMaxEntry *r, const char *data, int32_t dataType) {
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
 * in memory bucket, we only accept data array list
 */
int32_t tMemBucketPut(tMemBucket *pBucket, const void *data, size_t size) {
  assert(pBucket != NULL && data != NULL && size > 0);

  pBucket->total += (int32_t)size;

  int32_t bytes = pBucket->bytes;
  for (int32_t i = 0; i < size; ++i) {
    char *d = (char *) data + i * bytes;

    int32_t index = (pBucket->hashFunc)(pBucket, d);
    if (index == -1) {  // the value is out of range, do not add it into bucket
      return -1;
    }

    tMemBucketSlot *pSlot = &pBucket->pSlots[index];
    tMemBucketUpdateBoundingBox(&pSlot->range, d, pBucket->type);

    // ensure available memory pages to allocate
    int32_t groupId = getGroupId(pBucket->numOfSlots, index, pBucket->times);
    int32_t pageId = -1;

    if (pSlot->info.data == NULL || pSlot->info.data->num >= pBucket->elemPerPage) {
      if (pSlot->info.data != NULL) {
        assert(pSlot->info.data->num >= pBucket->elemPerPage && pSlot->info.size > 0);

        // keep the pointer in memory
        releaseResBufPage(pBucket->pBuffer, pSlot->info.data);
        pSlot->info.data = NULL;
      }

      pSlot->info.data = getNewDataBuf(pBucket->pBuffer, groupId, &pageId);
      pSlot->info.pageId = pageId;
    }

    memcpy(pSlot->info.data->data + pSlot->info.data->num * pBucket->bytes, d, pBucket->bytes);

    pSlot->info.data->num += 1;
    pSlot->info.size += 1;
  }

  return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////
static UNUSED_FUNC void findMaxMinValue(tMemBucket *pMemBucket, double *maxVal, double *minVal) {
  *minVal = DBL_MAX;
  *maxVal = -DBL_MAX;

  for (int32_t i = 0; i < pMemBucket->numOfSlots; ++i) {
    tMemBucketSlot *pSlot = &pMemBucket->pSlots[i];
    if (pSlot->info.size == 0) {
      continue;
    }

    switch (pMemBucket->type) {
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_TINYINT: {
        double minv = pSlot->range.iMinVal;
        double maxv = pSlot->range.iMaxVal;

        if (*minVal > minv) {
          *minVal = minv;
        }
        if (*maxVal < maxv) {
          *maxVal = maxv;
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_FLOAT: {
        double minv = pSlot->range.dMinVal;
        double maxv = pSlot->range.dMaxVal;

        if (*minVal > minv) {
          *minVal = minv;
        }
        if (*maxVal < maxv) {
          *maxVal = maxv;
        }
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        double minv = (double)pSlot->range.i64MinVal;
        double maxv = (double)pSlot->range.i64MaxVal;

        if (*minVal > minv) {
          *minVal = minv;
        }
        if (*maxVal < maxv) {
          *maxVal = maxv;
        }
        break;
      }
    }
  }
}

/*
 *
 * now, we need to find the minimum value of the next slot for
 * interpolating the percentile value
 * j is the last slot of current segment, we need to get the first
 * slot of the next segment.
 */
static MinMaxEntry getMinMaxEntryOfNextSlotWithData(tMemBucket *pMemBucket, int32_t slotIdx) {
    int32_t j = slotIdx + 1;
    while (j < pMemBucket->numOfSlots && (pMemBucket->pSlots[j].info.size == 0)) {
      ++j;
    }

    assert(j < pMemBucket->numOfSlots);
    return pMemBucket->pSlots[j].range;
}

static bool isIdenticalData(tMemBucket *pMemBucket, int32_t index);
char *getFirstElemOfMemBuffer(tMemBucketSlot *pSeg, int32_t slotIdx, tFilePage *pPage);

static double getIdenticalDataVal(tMemBucket* pMemBucket, int32_t slotIndex) {
  assert(isIdenticalData(pMemBucket, slotIndex));

  tMemBucketSlot *pSlot = &pMemBucket->pSlots[slotIndex];

  double finalResult = 0.0;
  switch (pMemBucket->type) {
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_INT: {
      finalResult = pSlot->range.iMinVal;
      break;
    }

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      finalResult = pSlot->range.dMinVal;
      break;
    };

    case TSDB_DATA_TYPE_BIGINT: {
      finalResult = (double)pSlot->range.i64MinVal;
      break;
    }
  }

  return finalResult;
}

double getPercentileImpl(tMemBucket *pMemBucket, int32_t count, double fraction) {
  int32_t num = 0;

  for (int32_t i = 0; i < pMemBucket->numOfSlots; ++i) {
    tMemBucketSlot *pSlot = &pMemBucket->pSlots[i];
    if (pSlot->info.size == 0) {
      continue;
    }

    // required value in current slot
    if (num < (count + 1) && num + pSlot->info.size >= (count + 1)) {
      if (pSlot->info.size + num == (count + 1)) {
        /*
         * now, we need to find the minimum value of the next slot for interpolating the percentile value
         * j is the last slot of current segment, we need to get the first slot of the next segment.
         */
        MinMaxEntry next = getMinMaxEntryOfNextSlotWithData(pMemBucket, i);

        double maxOfThisSlot = 0;
        double minOfNextSlot = 0;
        switch (pMemBucket->type) {
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_TINYINT: {
            maxOfThisSlot = pSlot->range.iMaxVal;
            minOfNextSlot = next.iMinVal;
            break;
          };
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE: {
            maxOfThisSlot = pSlot->range.dMaxVal;
            minOfNextSlot = next.dMinVal;
            break;
          };
          case TSDB_DATA_TYPE_BIGINT: {
            maxOfThisSlot = (double)pSlot->range.i64MaxVal;
            minOfNextSlot = (double)next.i64MinVal;
            break;
          }
        };

        assert(minOfNextSlot > maxOfThisSlot);

        double val = (1 - fraction) * maxOfThisSlot + fraction * minOfNextSlot;
        return val;
      }

      if (pSlot->info.size <= pMemBucket->maxCapacity) {
        // data in buffer and file are merged together to be processed.
        tFilePage *buffer = loadDataFromFilePage(pMemBucket, i);
        int32_t    currentIdx = count - num;

        char *thisVal = buffer->data + pMemBucket->bytes * currentIdx;
        char *nextVal = thisVal + pMemBucket->bytes;

        double td = 1.0, nd = 1.0;
        switch (pMemBucket->type) {
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
            td = GET_FLOAT_VAL(thisVal);
            nd = GET_FLOAT_VAL(nextVal);
            break;
          }
          case TSDB_DATA_TYPE_DOUBLE: {
            td = GET_DOUBLE_VAL(thisVal);
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
       if (isIdenticalData(pMemBucket, i)) {
         return getIdenticalDataVal(pMemBucket, i);
       }

       // try next round
       pMemBucket->times += 1;
       uDebug("MemBucket:%p, start next round data bucketing, time:%d", pMemBucket, pMemBucket->times);

       pMemBucket->range = pSlot->range;
       pMemBucket->total = 0;

       resetSlotInfo(pMemBucket);

       int32_t groupId = getGroupId(pMemBucket->numOfSlots, i, pMemBucket->times - 1);
       SIDList list = getDataBufPagesIdList(pMemBucket->pBuffer, groupId);
       assert(list->size > 0);

       for (int32_t f = 0; f < list->size; ++f) {
         SPageInfo *pgInfo = *(SPageInfo **)taosArrayGet(list, f);
         tFilePage *pg = getResBufPage(pMemBucket->pBuffer, pgInfo->pageId);

         tMemBucketPut(pMemBucket, pg->data, (int32_t)pg->num);
         releaseResBufPageInfo(pMemBucket->pBuffer, pgInfo);
       }

       return getPercentileImpl(pMemBucket, count - num, fraction);
      }
    } else {
      num += pSlot->info.size;
    }
  }

  return 0;
}

double getPercentile(tMemBucket *pMemBucket, double percent) {
  if (pMemBucket->total == 0) {
    return 0.0;
  }

  // if only one elements exists, return it
  if (pMemBucket->total == 1) {
    return findOnlyResult(pMemBucket);
  }

  percent = fabs(percent);

  // find the min/max value, no need to scan all data in bucket
  if (fabs(percent - 100.0) < DBL_EPSILON || (percent < DBL_EPSILON)) {
    MinMaxEntry* pRange = &pMemBucket->range;

    switch(pMemBucket->type) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
        return fabs(percent - 100) < DBL_EPSILON? pRange->iMaxVal:pRange->iMinVal;
      case TSDB_DATA_TYPE_BIGINT: {
        double v = (double)(fabs(percent - 100) < DBL_EPSILON ? pRange->i64MaxVal : pRange->i64MinVal);
        return v;
      }
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE:
        return fabs(percent - 100) < DBL_EPSILON? pRange->dMaxVal:pRange->dMinVal;
        default:
        return -1;
    }
  }

  double  percentVal = (percent * (pMemBucket->total - 1)) / ((double)100.0);

  // do put data by using buckets
  int32_t orderIdx = (int32_t)percentVal;
  return getPercentileImpl(pMemBucket, orderIdx, percentVal - orderIdx);
}

/*
 * check if data in one slot are all identical only need to compare with the bounding box
 */
bool isIdenticalData(tMemBucket *pMemBucket, int32_t index) {
  tMemBucketSlot *pSeg = &pMemBucket->pSlots[index];

  if (pMemBucket->type == TSDB_DATA_TYPE_INT || pMemBucket->type == TSDB_DATA_TYPE_BIGINT ||
      pMemBucket->type == TSDB_DATA_TYPE_SMALLINT || pMemBucket->type == TSDB_DATA_TYPE_TINYINT) {
    return pSeg->range.i64MinVal == pSeg->range.i64MaxVal;
  }

  if (pMemBucket->type == TSDB_DATA_TYPE_FLOAT || pMemBucket->type == TSDB_DATA_TYPE_DOUBLE) {
    return fabs(pSeg->range.dMaxVal - pSeg->range.dMinVal) < DBL_EPSILON;
  }

  return false;
}

/*
 * get the first element of one slot into memory.
 * if no data of current slot in memory, load it from disk
 */
char *getFirstElemOfMemBuffer(tMemBucketSlot *pSeg, int32_t slotIdx, tFilePage *pPage) {
//  STSBuf *pMemBuffer = pSeg->pBuffer[slotIdx];
  char *thisVal = NULL;

//  if (pSeg->pBuffer[slotIdx]->numOfTotal != 0) {
////    thisVal = pSeg->pBuffer[slotIdx]->pHead->item.data;
//  } else {
//    /*
//     * no data in memory, load one page into memory
//     */
//    tFlushoutInfo *pFlushInfo = &pMemBuffer->fileMeta.flushoutData.pFlushoutInfo[0];
//    assert(pFlushInfo->numOfPages == pMemBuffer->fileMeta.nFileSize);
//    int32_t ret;
//    ret = fseek(pMemBuffer->file, pFlushInfo->startPageId * pMemBuffer->pageSize, SEEK_SET);
//    UNUSED(ret);
//    size_t sz = fread(pPage, pMemBuffer->pageSize, 1, pMemBuffer->file);
//    UNUSED(sz);
//    thisVal = pPage->data;
//  }
  return thisVal;
}
