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

#include "qPercentile.h"
#include "qResultbuf.h"
#include "queryLog.h"
#include "taosdef.h"
#include "tcompare.h"
#include "ttype.h"

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
  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    range->i64MaxVal = INT64_MIN;
    range->i64MinVal = INT64_MAX;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    range->u64MaxVal = 0;
    range->u64MinVal = UINT64_MAX;
  } else {
    range->dMaxVal = -DBL_MAX;
    range->dMinVal = DBL_MAX;
  }
}

static int32_t setBoundingBox(MinMaxEntry* range, int16_t type, double minval, double maxval) {
  if (minval > maxval) {
    return -1;
  }

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    range->i64MinVal = (int64_t) minval;
    range->i64MaxVal = (int64_t) maxval;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)){
    range->u64MinVal = (uint64_t) minval;
    range->u64MaxVal = (uint64_t) maxval;
  } else {
    range->dMinVal = minval;
    range->dMaxVal = maxval;
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

    double v = 0;
    GET_TYPED_DATA(v, double, pMemBucket->type, pPage->data);
    return v;
  }

  return 0;
}

int32_t tBucketIntHash(tMemBucket *pBucket, const void *value) {
  int64_t v = 0;
  GET_TYPED_DATA(v, int64_t, pBucket->type, value);

  int32_t index = -1;

  if (v > pBucket->range.i64MaxVal || v < pBucket->range.i64MinVal) {
    return index;
  }
  
  // divide the value range into 1024 buckets
  uint64_t span = pBucket->range.i64MaxVal - pBucket->range.i64MinVal;
  if (span < pBucket->numOfSlots) {
    int64_t delta = v - pBucket->range.i64MinVal;
    index = (delta % pBucket->numOfSlots);
  } else {
    double slotSpan = (double)span / pBucket->numOfSlots;
    index = (int32_t)((v - pBucket->range.i64MinVal) / slotSpan);
    if (v == pBucket->range.i64MaxVal) {
      index -= 1;
    }
  }

  assert(index >= 0 && index < pBucket->numOfSlots);
  return index;
}

int32_t tBucketUintHash(tMemBucket *pBucket, const void *value) {
  int64_t v = 0;
  GET_TYPED_DATA(v, uint64_t, pBucket->type, value);

  int32_t index = -1;

  if (v > pBucket->range.u64MaxVal || v < pBucket->range.u64MinVal) {
    return index;
  }
  
  // divide the value range into 1024 buckets
  uint64_t span = pBucket->range.u64MaxVal - pBucket->range.u64MinVal;
  if (span < pBucket->numOfSlots) {
    int64_t delta = v - pBucket->range.u64MinVal;
    index = (int32_t) (delta % pBucket->numOfSlots);
  } else {
    double slotSpan = (double)span / pBucket->numOfSlots;
    index = (int32_t)((v - pBucket->range.u64MinVal) / slotSpan);
    if (v == pBucket->range.u64MaxVal) {
      index -= 1;
    }
  }

  assert(index >= 0 && index < pBucket->numOfSlots);
  return index;
}

int32_t tBucketDoubleHash(tMemBucket *pBucket, const void *value) {
  double v = 0;
  if (pBucket->type == TSDB_DATA_TYPE_FLOAT) {
    v = GET_FLOAT_VAL(value);
  } else {
    v = GET_DOUBLE_VAL(value);
  }

  int32_t index = -1;

  if (v > pBucket->range.dMaxVal || v < pBucket->range.dMinVal) {
    return index;
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

  assert(index >= 0 && index < pBucket->numOfSlots);
  return index;
}

static __perc_hash_func_t getHashFunc(int32_t type) {
  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    return tBucketIntHash;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    return tBucketUintHash;
  } else {
    return tBucketDoubleHash;
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
    qError("MemBucket:%p, invalid value range: %f-%f", pBucket, minval, maxval);
    free(pBucket);
    return NULL;
  }

  pBucket->elemPerPage = (pBucket->bufPageSize - sizeof(tFilePage))/pBucket->bytes;
  pBucket->comparFn = getKeyComparFunc(pBucket->type);

  pBucket->hashFunc = getHashFunc(pBucket->type);
  if (pBucket->hashFunc == NULL) {
    qError("MemBucket:%p, not support data type %d, failed", pBucket, pBucket->type);
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
  
  qDebug("MemBucket:%p, elem size:%d", pBucket, pBucket->bytes);
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
  if (IS_SIGNED_NUMERIC_TYPE(dataType)) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, dataType, data);

    if (r->i64MinVal > v) {
      r->i64MinVal = v;
    }

    if (r->i64MaxVal < v) {
      r->i64MaxVal = v;
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(dataType)) {
    uint64_t v = 0;
    GET_TYPED_DATA(v, uint64_t, dataType, data);

    if (r->i64MinVal > v) {
      r->i64MinVal = v;
    }

    if (r->i64MaxVal < v) {
      r->i64MaxVal = v;
    }
  } else if (IS_FLOAT_TYPE(dataType)) {
    double v = 0;
    GET_TYPED_DATA(v, double, dataType, data);

    if (r->dMinVal > v) {
      r->dMinVal = v;
    }

    if (r->dMaxVal < v) {
      r->dMaxVal = v;
    }
  } else {
    assert(0);
  }
}

/*
 * in memory bucket, we only accept data array list
 */
int32_t tMemBucketPut(tMemBucket *pBucket, const void *data, size_t size) {
  assert(pBucket != NULL && data != NULL && size > 0);

  int32_t count = 0;
  int32_t bytes = pBucket->bytes;
  for (int32_t i = 0; i < size; ++i) {
    char *d = (char *) data + i * bytes;

    int32_t index = (pBucket->hashFunc)(pBucket, d);
    if (index < 0) {
      continue;
    }

    count += 1;

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

  pBucket->total += count;
  return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////
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

static double getIdenticalDataVal(tMemBucket* pMemBucket, int32_t slotIndex) {
  assert(isIdenticalData(pMemBucket, slotIndex));

  tMemBucketSlot *pSlot = &pMemBucket->pSlots[slotIndex];

  double finalResult = 0.0;
  if (IS_SIGNED_NUMERIC_TYPE(pMemBucket->type)) {
    finalResult = (double) pSlot->range.i64MinVal;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
    finalResult = (double) pSlot->range.u64MinVal;
  } else {
    finalResult = (double) pSlot->range.dMinVal;
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
        if (IS_SIGNED_NUMERIC_TYPE(pMemBucket->type)) {
          maxOfThisSlot = (double) pSlot->range.i64MaxVal;
          minOfNextSlot = (double) next.i64MinVal;
        } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
          maxOfThisSlot = (double) pSlot->range.u64MaxVal;
          minOfNextSlot = (double) next.u64MinVal;
        } else {
          maxOfThisSlot = (double) pSlot->range.dMaxVal;
          minOfNextSlot = (double) next.dMinVal;
        }

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
        GET_TYPED_DATA(td, double, pMemBucket->type, thisVal);
        GET_TYPED_DATA(nd, double, pMemBucket->type, nextVal);

        double val = (1 - fraction) * td + fraction * nd;
        tfree(buffer);

        return val;
      } else {  // incur a second round bucket split
       if (isIdenticalData(pMemBucket, i)) {
         return getIdenticalDataVal(pMemBucket, i);
       }

       // try next round
       pMemBucket->times += 1;
       qDebug("MemBucket:%p, start next round data bucketing, time:%d", pMemBucket, pMemBucket->times);

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

    if (IS_SIGNED_NUMERIC_TYPE(pMemBucket->type)) {
      double v = (double)(fabs(percent - 100) < DBL_EPSILON ? pRange->i64MaxVal : pRange->i64MinVal);
      return v;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
      double v = (double)(fabs(percent - 100) < DBL_EPSILON ? pRange->u64MaxVal : pRange->u64MinVal);
      return v;
    } else {
      return fabs(percent - 100) < DBL_EPSILON? pRange->dMaxVal:pRange->dMinVal;
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

  if (IS_FLOAT_TYPE(pMemBucket->type)) {
    return fabs(pSeg->range.dMaxVal - pSeg->range.dMinVal) < DBL_EPSILON;
  } else {
    return pSeg->range.i64MinVal == pSeg->range.i64MaxVal;
  }
}
