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

#include "tstreamUpdate.h"
#include "ttime.h"

#define DEFAULT_FALSE_POSITIVE 0.01
#define DEFAULT_BUCKET_SIZE    1024
#define ROWS_PER_MILLISECOND   1
#define MAX_NUM_SCALABLE_BF    100000
#define MIN_NUM_SCALABLE_BF    10
#define DEFAULT_PREADD_BUCKET  1
#define MAX_INTERVAL           MILLISECOND_PER_MINUTE
#define MIN_INTERVAL           (MILLISECOND_PER_SECOND * 10)
#define DEFAULT_EXPECTED_ENTRIES    10000

static int64_t adjustExpEntries(int64_t entries) {
  return TMIN(DEFAULT_EXPECTED_ENTRIES, entries);
}

static void windowSBfAdd(SUpdateInfo *pInfo, uint64_t count) {
  if (pInfo->numSBFs < count) {
    count = pInfo->numSBFs;
  }
  for (uint64_t i = 0; i < count; ++i) {
    int64_t rows = adjustExpEntries(pInfo->interval * ROWS_PER_MILLISECOND);
    SScalableBf *tsSBF = tScalableBfInit(rows, DEFAULT_FALSE_POSITIVE);
    taosArrayPush(pInfo->pTsSBFs, &tsSBF);
  }
}

static void windowSBfDelete(SUpdateInfo *pInfo, uint64_t count) {
  if (count < pInfo->numSBFs - 1) {
    for (uint64_t i = 0; i < count; ++i) {
      SScalableBf *pTsSBFs = taosArrayGetP(pInfo->pTsSBFs, 0);
      tScalableBfDestroy(pTsSBFs);
      taosArrayRemove(pInfo->pTsSBFs, 0);
    }
  } else {
    taosArrayClearP(pInfo->pTsSBFs, (FDelete)tScalableBfDestroy);
  }
  pInfo->minTS += pInfo->interval * count;
}

static int64_t adjustInterval(int64_t interval, int32_t precision) {
  int64_t val = interval;
  if (precision != TSDB_TIME_PRECISION_MILLI) {
    val = convertTimePrecision(interval, precision, TSDB_TIME_PRECISION_MILLI);
  }

  if (val <= 0 || val > MAX_INTERVAL) {
    val = MAX_INTERVAL;
  } else if (val < MIN_INTERVAL) {
    val = MIN_INTERVAL;
  }

  if (precision != TSDB_TIME_PRECISION_MILLI) {
    val = convertTimePrecision(val, TSDB_TIME_PRECISION_MILLI, precision);
  }
  return val;
}

static int64_t adjustWatermark(int64_t adjInterval, int64_t originInt, int64_t watermark) {
  if (watermark <= 0) {
    watermark = TMIN(originInt/adjInterval, 1) * adjInterval;
  } else if (watermark > MAX_NUM_SCALABLE_BF * adjInterval) {
    watermark = MAX_NUM_SCALABLE_BF * adjInterval;
  }/* else if (watermark < MIN_NUM_SCALABLE_BF * adjInterval) {
    watermark = MIN_NUM_SCALABLE_BF * adjInterval;
  }*/ // Todo(liuyao) save window info to tdb
  return watermark;
}

SUpdateInfo *updateInfoInitP(SInterval *pInterval, int64_t watermark) {
  return updateInfoInit(pInterval->interval, pInterval->precision, watermark);
}

SUpdateInfo *updateInfoInit(int64_t interval, int32_t precision, int64_t watermark) {
  SUpdateInfo *pInfo = taosMemoryCalloc(1, sizeof(SUpdateInfo));
  if (pInfo == NULL) {
    return NULL;
  }
  pInfo->pTsBuckets = NULL;
  pInfo->pTsSBFs = NULL;
  pInfo->minTS = -1;
  pInfo->interval = adjustInterval(interval, precision);
  pInfo->watermark = adjustWatermark(pInfo->interval, interval, watermark);

  uint64_t bfSize = (uint64_t)(pInfo->watermark / pInfo->interval);

  pInfo->pTsSBFs = taosArrayInit(bfSize, sizeof(void *));
  if (pInfo->pTsSBFs == NULL) {
    updateInfoDestroy(pInfo);
    return NULL;
  }
  pInfo->numSBFs = bfSize;
  windowSBfAdd(pInfo, bfSize);

  pInfo->pTsBuckets = taosArrayInit(DEFAULT_BUCKET_SIZE, sizeof(TSKEY));
  if (pInfo->pTsBuckets == NULL) {
    updateInfoDestroy(pInfo);
    return NULL;
  }

  TSKEY dumy = 0;
  for (uint64_t i = 0; i < DEFAULT_BUCKET_SIZE; ++i) {
    taosArrayPush(pInfo->pTsBuckets, &dumy);
  }
  pInfo->numBuckets = DEFAULT_BUCKET_SIZE;
  return pInfo;
}

static SScalableBf *getSBf(SUpdateInfo *pInfo, TSKEY ts) {
  if (ts <= 0) {
    return NULL;
  }
  if (pInfo->minTS < 0) {
    pInfo->minTS = (TSKEY)(ts / pInfo->interval * pInfo->interval);
  }
  int64_t index = (int64_t)((ts - pInfo->minTS) / pInfo->interval);
  if (index < 0) {
    return NULL;
  }
  if (index >= pInfo->numSBFs) {
    uint64_t count = index + 1 - pInfo->numSBFs;
    windowSBfDelete(pInfo, count);
    windowSBfAdd(pInfo, count);
    index = pInfo->numSBFs - 1;
  }
  SScalableBf *res = taosArrayGetP(pInfo->pTsSBFs, index);
  if (res == NULL) {
    int64_t rows = adjustExpEntries(pInfo->interval * ROWS_PER_MILLISECOND);
    res = tScalableBfInit(rows, DEFAULT_FALSE_POSITIVE);
    taosArrayPush(pInfo->pTsSBFs, &res);
  }
  return res;
}

bool updateInfoIsUpdated(SUpdateInfo *pInfo, tb_uid_t tableId, TSKEY ts) {
  int32_t      res = TSDB_CODE_FAILED;
  uint64_t     index = ((uint64_t)tableId) % pInfo->numBuckets;
  TSKEY maxTs = *(TSKEY *)taosArrayGet(pInfo->pTsBuckets, index);
  if (ts < maxTs - pInfo->watermark) {
    // this window has been closed.
    return true;
  }

  SScalableBf *pSBf = getSBf(pInfo, ts);
  // pSBf may be a null pointer
  if (pSBf) {
    res = tScalableBfPut(pSBf, &ts, sizeof(TSKEY));
  }

  if (maxTs < ts) {
    taosArraySet(pInfo->pTsBuckets, index, &ts);
    return false;
  }

  if (ts < pInfo->minTS) {
    return true;
  } else if (res == TSDB_CODE_SUCCESS) {
    return false;
  }

  // check from tsdb api
  return true;
}

void updateInfoDestroy(SUpdateInfo *pInfo) {
  if (pInfo == NULL) {
    return;
  }
  taosArrayDestroy(pInfo->pTsBuckets);

  uint64_t size = taosArrayGetSize(pInfo->pTsSBFs);
  for (uint64_t i = 0; i < size; i++) {
    SScalableBf *pSBF = taosArrayGetP(pInfo->pTsSBFs, i);
    tScalableBfDestroy(pSBF);
  }

  taosArrayDestroy(pInfo->pTsSBFs);
  taosMemoryFree(pInfo);
}
