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

#include "taoserror.h"
#include "tcompare.h"
#include "tglobal.h"

#include "taosdef.h"
#include "tcompare.h"
#include "tpagedbuf.h"
#include "tpercentile.h"
#include "ttypes.h"
#include "tlog.h"

#define DEFAULT_NUM_OF_SLOT 1024

int32_t getGroupId(int32_t numOfSlots, int32_t slotIndex, int32_t times) { return (times * numOfSlots) + slotIndex; }

static SFilePage *loadDataFromFilePage(tMemBucket *pMemBucket, int32_t slotIdx) {
  SFilePage *buffer =
      (SFilePage *)taosMemoryCalloc(1, pMemBucket->bytes * pMemBucket->pSlots[slotIdx].info.size + sizeof(SFilePage));

  int32_t groupId = getGroupId(pMemBucket->numOfSlots, slotIdx, pMemBucket->times);

  SArray *pIdList;
  void *p = taosHashGet(pMemBucket->groupPagesMap, &groupId, sizeof(groupId));
  if (p != NULL) {
    pIdList = *(SArray **)p;
  } else {
    taosMemoryFree(buffer);
    return NULL;
  }

  int32_t offset = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pIdList); ++i) {
    int32_t *pageId = taosArrayGet(pIdList, i);

    SFilePage *pg = getBufPage(pMemBucket->pBuffer, *pageId);
    if (pg == NULL) {
      taosMemoryFree(buffer);
      return NULL;
    }

    memcpy(buffer->data + offset, pg->data, (size_t)(pg->num * pMemBucket->bytes));
    offset += (int32_t)(pg->num * pMemBucket->bytes);
  }

  taosSort(buffer->data, pMemBucket->pSlots[slotIdx].info.size, pMemBucket->bytes, pMemBucket->comparFn);
  return buffer;
}

static void resetBoundingBox(MinMaxEntry *range, int32_t type) {
  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    range->dMaxVal = INT64_MIN;
    range->dMinVal = INT64_MAX;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    range->u64MaxVal = 0;
    range->u64MinVal = UINT64_MAX;
  } else {
    range->dMaxVal = -DBL_MAX;
    range->dMinVal = DBL_MAX;
  }
}

static int32_t setBoundingBox(MinMaxEntry *range, int16_t type, double minval, double maxval) {
  if (minval > maxval) {
    return -1;
  }

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    range->dMinVal = (int64_t)minval;
    range->dMaxVal = (int64_t)maxval;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    range->u64MinVal = (uint64_t)minval;
    range->u64MaxVal = (uint64_t)maxval;
  } else {
    range->dMinVal = minval;
    range->dMaxVal = maxval;
  }

  return 0;
}

static void resetPosInfo(SSlotInfo *pInfo) {
  pInfo->size = 0;
  pInfo->pageId = -1;
  pInfo->data = NULL;
}

int32_t findOnlyResult(tMemBucket *pMemBucket, double *result) {
  ASSERT(pMemBucket->total == 1);
  terrno = 0;

  for (int32_t i = 0; i < pMemBucket->numOfSlots; ++i) {
    tMemBucketSlot *pSlot = &pMemBucket->pSlots[i];
    if (pSlot->info.size == 0) {
      continue;
    }

    int32_t groupId = getGroupId(pMemBucket->numOfSlots, i, pMemBucket->times);
    SArray **pList = taosHashGet(pMemBucket->groupPagesMap, &groupId, sizeof(groupId));
    if (pList != NULL)  {
      SArray *list = *pList;
      ASSERT(list->size == 1);

      int32_t   *pageId = taosArrayGet(list, 0);
      SFilePage *pPage = getBufPage(pMemBucket->pBuffer, *pageId);
      if (pPage == NULL) {
        return terrno;
      }
      ASSERT(pPage->num == 1);

      GET_TYPED_DATA(*result, double, pMemBucket->type, pPage->data);
      return TSDB_CODE_SUCCESS;
    }
  }

  *result = 0.0;
  return TSDB_CODE_SUCCESS;
}

int32_t tBucketIntHash(tMemBucket *pBucket, const void *value) {
  int64_t v = 0;
  GET_TYPED_DATA(v, int64_t, pBucket->type, value);

  int32_t index = -1;

  if (v > pBucket->range.dMaxVal || v < pBucket->range.dMinVal) {
    return index;
  }

  // divide the value range into 1024 buckets
  uint64_t span = pBucket->range.dMaxVal - pBucket->range.dMinVal;
  if (span < pBucket->numOfSlots) {
    int64_t delta = v - pBucket->range.dMinVal;
    index = (delta % pBucket->numOfSlots);
  } else {
    double   slotSpan = ((double)span) / pBucket->numOfSlots;
    uint64_t delta = (uint64_t)(v - pBucket->range.dMinVal);

    index = delta / slotSpan;
    if (v == pBucket->range.dMaxVal || index == pBucket->numOfSlots) {
      index -= 1;
    }
  }

  ASSERTS(index >= 0 && index < pBucket->numOfSlots, "tBucketIntHash Error, index:%d, numOfSlots:%d",
          index, pBucket->numOfSlots);
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
    index = (int32_t)(delta % pBucket->numOfSlots);
  } else {
    double slotSpan = (double)span / pBucket->numOfSlots;
    index = (int32_t)((v - pBucket->range.u64MinVal) / slotSpan);
    if (v == pBucket->range.u64MaxVal) {
      index -= 1;
    }
  }

  ASSERT(index >= 0 && index < pBucket->numOfSlots);
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

  ASSERT(index >= 0 && index < pBucket->numOfSlots);
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

static void resetSlotInfo(tMemBucket *pBucket) {
  for (int32_t i = 0; i < pBucket->numOfSlots; ++i) {
    tMemBucketSlot *pSlot = &pBucket->pSlots[i];

    resetBoundingBox(&pSlot->range, pBucket->type);
    resetPosInfo(&pSlot->info);
  }
}

tMemBucket *tMemBucketCreate(int32_t nElemSize, int16_t dataType, double minval, double maxval) {
  tMemBucket *pBucket = (tMemBucket *)taosMemoryCalloc(1, sizeof(tMemBucket));
  if (pBucket == NULL) {
    return NULL;
  }

  pBucket->numOfSlots = DEFAULT_NUM_OF_SLOT;
  pBucket->bufPageSize = 16384 * 4;  // 16k per page

  pBucket->type = dataType;
  pBucket->bytes = nElemSize;
  pBucket->total = 0;
  pBucket->times = 1;

  pBucket->maxCapacity = 200000;
  pBucket->groupPagesMap = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (setBoundingBox(&pBucket->range, pBucket->type, minval, maxval) != 0) {
    //    qError("MemBucket:%p, invalid value range: %f-%f", pBucket, minval, maxval);
    taosMemoryFree(pBucket);
    return NULL;
  }

  pBucket->elemPerPage = (pBucket->bufPageSize - sizeof(SFilePage)) / pBucket->bytes;
  pBucket->comparFn = getKeyComparFunc(pBucket->type, TSDB_ORDER_ASC);

  pBucket->hashFunc = getHashFunc(pBucket->type);
  if (pBucket->hashFunc == NULL) {
    //    qError("MemBucket:%p, not support data type %d, failed", pBucket, pBucket->type);
    taosMemoryFree(pBucket);
    return NULL;
  }

  pBucket->pSlots = (tMemBucketSlot *)taosMemoryCalloc(pBucket->numOfSlots, sizeof(tMemBucketSlot));
  if (pBucket->pSlots == NULL) {
    taosMemoryFree(pBucket);
    return NULL;
  }

  resetSlotInfo(pBucket);

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    // qError("MemBucket create disk based Buf failed since %s", terrstr(terrno));
    tMemBucketDestroy(pBucket);
    return NULL;
  }

  int32_t ret = createDiskbasedBuf(&pBucket->pBuffer, pBucket->bufPageSize, pBucket->bufPageSize * 1024, "1", tsTempDir);
  if (ret != 0) {
    tMemBucketDestroy(pBucket);
    return NULL;
  }

  //  qDebug("MemBucket:%p, elem size:%d", pBucket, pBucket->bytes);
  return pBucket;
}

void tMemBucketDestroy(tMemBucket *pBucket) {
  if (pBucket == NULL) {
    return;
  }

  void *p = taosHashIterate(pBucket->groupPagesMap, NULL);
  while (p) {
    SArray **p1 = p;
    p = taosHashIterate(pBucket->groupPagesMap, p);
    taosArrayDestroy(*p1);
  }

  destroyDiskbasedBuf(pBucket->pBuffer);
  taosMemoryFreeClear(pBucket->pSlots);
  taosHashCleanup(pBucket->groupPagesMap);
  taosMemoryFreeClear(pBucket);
}

void tMemBucketUpdateBoundingBox(MinMaxEntry *r, const char *data, int32_t dataType) {
  if (IS_SIGNED_NUMERIC_TYPE(dataType)) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, dataType, data);

    if (r->dMinVal > v) {
      r->dMinVal = v;
    }

    if (r->dMaxVal < v) {
      r->dMaxVal = v;
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(dataType)) {
    uint64_t v = 0;
    GET_TYPED_DATA(v, uint64_t, dataType, data);

    if (r->u64MinVal > v) {
      r->u64MinVal = v;
    }

    if (r->u64MaxVal < v) {
      r->u64MaxVal = v;
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
    ASSERT(0);
  }
}

/*
 * in memory bucket, we only accept data array list
 */
int32_t tMemBucketPut(tMemBucket *pBucket, const void *data, size_t size) {
  int32_t count = 0;
  int32_t bytes = pBucket->bytes;
  for (int32_t i = 0; i < size; ++i) {
    char   *d = (char *)data + i * bytes;
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
        ASSERT(pSlot->info.data->num >= pBucket->elemPerPage && pSlot->info.size > 0);

        // keep the pointer in memory
        setBufPageDirty(pSlot->info.data, true);
        releaseBufPage(pBucket->pBuffer, pSlot->info.data);
        pSlot->info.data = NULL;
      }

      SArray *pPageIdList;
      void *p = taosHashGet(pBucket->groupPagesMap, &groupId, sizeof(groupId));
      if (p == NULL) {
        pPageIdList = taosArrayInit(4, sizeof(int32_t));
        taosHashPut(pBucket->groupPagesMap, &groupId, sizeof(groupId), &pPageIdList, POINTER_BYTES);
      } else {
        pPageIdList = *(SArray **)p;
      }

      pSlot->info.data = getNewBufPage(pBucket->pBuffer, &pageId);
      if (pSlot->info.data == NULL) {
        return terrno;
      }
      pSlot->info.pageId = pageId;
      taosArrayPush(pPageIdList, &pageId);
    }

    memcpy(pSlot->info.data->data + pSlot->info.data->num * pBucket->bytes, d, pBucket->bytes);

    pSlot->info.data->num += 1;
    pSlot->info.size += 1;
  }

  pBucket->total += count;
  return TSDB_CODE_SUCCESS;
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

  ASSERT(j < pMemBucket->numOfSlots);
  return pMemBucket->pSlots[j].range;
}

static bool isIdenticalData(tMemBucket *pMemBucket, int32_t index);

static double getIdenticalDataVal(tMemBucket *pMemBucket, int32_t slotIndex) {
  ASSERT(isIdenticalData(pMemBucket, slotIndex));

  tMemBucketSlot *pSlot = &pMemBucket->pSlots[slotIndex];

  double finalResult = 0.0;
  if (IS_SIGNED_NUMERIC_TYPE(pMemBucket->type)) {
    finalResult = (double)pSlot->range.dMinVal;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
    finalResult = (double)pSlot->range.u64MinVal;
  } else {
    finalResult = (double)pSlot->range.dMinVal;
  }

  return finalResult;
}

int32_t getPercentileImpl(tMemBucket *pMemBucket, int32_t count, double fraction, double *result) {
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
          maxOfThisSlot = (double)pSlot->range.dMaxVal;
          minOfNextSlot = (double)next.dMinVal;
        } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
          maxOfThisSlot = (double)pSlot->range.u64MaxVal;
          minOfNextSlot = (double)next.u64MinVal;
        } else {
          maxOfThisSlot = (double)pSlot->range.dMaxVal;
          minOfNextSlot = (double)next.dMinVal;
        }

        ASSERT(minOfNextSlot > maxOfThisSlot);

        *result = (1 - fraction) * maxOfThisSlot + fraction * minOfNextSlot;
        return TSDB_CODE_SUCCESS;
      }

      if (pSlot->info.size <= pMemBucket->maxCapacity) {
        // data in buffer and file are merged together to be processed.
        SFilePage *buffer = loadDataFromFilePage(pMemBucket, i);
        if (buffer == NULL) {
          return terrno;
        }

        int32_t    currentIdx = count - num;

        char *thisVal = buffer->data + pMemBucket->bytes * currentIdx;
        char *nextVal = thisVal + pMemBucket->bytes;

        double td = 1.0, nd = 1.0;
        GET_TYPED_DATA(td, double, pMemBucket->type, thisVal);
        GET_TYPED_DATA(nd, double, pMemBucket->type, nextVal);

        *result = (1 - fraction) * td + fraction * nd;
        taosMemoryFreeClear(buffer);

        return TSDB_CODE_SUCCESS;
      } else {  // incur a second round bucket split
        if (isIdenticalData(pMemBucket, i)) {
          *result = getIdenticalDataVal(pMemBucket, i);
          return TSDB_CODE_SUCCESS;
        }

        // try next round
        pMemBucket->times += 1;
        //       qDebug("MemBucket:%p, start next round data bucketing, time:%d", pMemBucket, pMemBucket->times);

        pMemBucket->range = pSlot->range;
        pMemBucket->total = 0;

        resetSlotInfo(pMemBucket);

        int32_t groupId = getGroupId(pMemBucket->numOfSlots, i, pMemBucket->times - 1);

        SArray* list;
        void *p = taosHashGet(pMemBucket->groupPagesMap, &groupId, sizeof(groupId));
        if (p != NULL) {
          list = *(SArray **)p;
          if (list == NULL || list->size <= 0) {
            return -1;
          }
        } else {
          return -1;
        }

        for (int32_t f = 0; f < list->size; ++f) {
          int32_t *pageId = taosArrayGet(list, f);
          SFilePage *pg = getBufPage(pMemBucket->pBuffer, *pageId);
          if (pg == NULL) {
            return terrno;
          }

          int32_t code = tMemBucketPut(pMemBucket, pg->data, (int32_t)pg->num);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
          setBufPageDirty(pg, true);
          releaseBufPage(pMemBucket->pBuffer, pg);
        }

        return getPercentileImpl(pMemBucket, count - num, fraction, result);
      }
    } else {
      num += pSlot->info.size;
    }
  }

  *result = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t getPercentile(tMemBucket *pMemBucket, double percent, double *result) {
  if (pMemBucket->total == 0) {
    *result = 0.0;
    return TSDB_CODE_SUCCESS;
  }

  // if only one elements exists, return it
  if (pMemBucket->total == 1) {
    return findOnlyResult(pMemBucket, result);
  }

  percent = fabs(percent);

  // find the min/max value, no need to scan all data in bucket
  if (fabs(percent - 100.0) < DBL_EPSILON || (percent < DBL_EPSILON)) {
    MinMaxEntry *pRange = &pMemBucket->range;

    if (IS_SIGNED_NUMERIC_TYPE(pMemBucket->type)) {
      *result = (double)(fabs(percent - 100) < DBL_EPSILON ? pRange->dMaxVal : pRange->dMinVal);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pMemBucket->type)) {
      *result = (double)(fabs(percent - 100) < DBL_EPSILON ? pRange->u64MaxVal : pRange->u64MinVal);
    } else {
      *result = fabs(percent - 100) < DBL_EPSILON ? pRange->dMaxVal : pRange->dMinVal;
    }

    return TSDB_CODE_SUCCESS;
  }

  double percentVal = (percent * (pMemBucket->total - 1)) / ((double)100.0);

  // do put data by using buckets
  int32_t orderIdx = (int32_t)percentVal;
  return getPercentileImpl(pMemBucket, orderIdx, percentVal - orderIdx, result);
}

/*
 * check if data in one slot are all identical only need to compare with the bounding box
 */
bool isIdenticalData(tMemBucket *pMemBucket, int32_t index) {
  tMemBucketSlot *pSeg = &pMemBucket->pSlots[index];

  if (IS_FLOAT_TYPE(pMemBucket->type)) {
    return fabs(pSeg->range.dMaxVal - pSeg->range.dMinVal) < DBL_EPSILON;
  } else {
    return pSeg->range.dMinVal == pSeg->range.dMaxVal;
  }
}
