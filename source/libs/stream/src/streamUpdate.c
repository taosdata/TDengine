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

#include "tcompare.h"
#include "tdatablock.h"
#include "tencode.h"
#include "tstreamUpdate.h"
#include "ttime.h"

#define DEFAULT_FALSE_POSITIVE   0.01
#define DEFAULT_BUCKET_SIZE      131072
#define DEFAULT_MAP_CAPACITY     131072
#define DEFAULT_MAP_SIZE         (DEFAULT_MAP_CAPACITY * 100)
#define ROWS_PER_MILLISECOND     1
#define MAX_NUM_SCALABLE_BF      64
#define MIN_NUM_SCALABLE_BF      10
#define DEFAULT_PREADD_BUCKET    1
#define MAX_INTERVAL             MILLISECOND_PER_MINUTE
#define MIN_INTERVAL             (MILLISECOND_PER_SECOND * 10)
#define DEFAULT_EXPECTED_ENTRIES 10000

static int64_t adjustExpEntries(int64_t entries) { return TMIN(DEFAULT_EXPECTED_ENTRIES, entries); }

int compareKeyTs(void* pTs1, void* pTs2, void* pPkVal, __compar_fn_t cmpPkFn) {
  return compareInt64Val(pTs1, pTs2);;
}

int compareKeyTsAndPk(void* pValue1, void* pTs, void* pPkVal, __compar_fn_t cmpPkFn) {
  int res = compareInt64Val(pValue1, pTs);
  if (res != 0) {
    return res;
  } else {
    void* pk1 = (char*)pValue1 + sizeof(TSKEY);
    return cmpPkFn(pk1, pPkVal);
  }
}

int32_t getKeyBuff(TSKEY ts, int64_t tbUid, void* pVal, int32_t len, char* buff) {
  *(TSKEY*)buff = ts;
  memcpy(buff+ sizeof(TSKEY), &tbUid, sizeof(int64_t));
  if (len == 0) {
    return sizeof(TSKEY) + sizeof(int64_t);
  }
  memcpy(buff, pVal, len);
  return sizeof(TSKEY) + sizeof(int64_t) + len;
}

int32_t getValueBuff(TSKEY ts, char* pVal, int32_t len, char* buff) {
  *(TSKEY*)buff = ts;
  if (len == 0) {
    return sizeof(TSKEY);
  }
  memcpy(buff + sizeof(TSKEY), pVal, len);
  return sizeof(TSKEY) + len;
}

void windowSBfAdd(SUpdateInfo *pInfo, uint64_t count) {
  if (pInfo->numSBFs < count) {
    count = pInfo->numSBFs;
  }
  for (uint64_t i = 0; i < count; ++i) {
    int64_t      rows = adjustExpEntries(pInfo->interval * ROWS_PER_MILLISECOND);
    SScalableBf *tsSBF = tScalableBfInit(rows, DEFAULT_FALSE_POSITIVE);
    taosArrayPush(pInfo->pTsSBFs, &tsSBF);
  }
}

static void clearItemHelper(void *p) {
  SScalableBf **pBf = p;
  tScalableBfDestroy(*pBf);
}

void windowSBfDelete(SUpdateInfo *pInfo, uint64_t count) {
  if (count < pInfo->numSBFs) {
    for (uint64_t i = 0; i < count; ++i) {
      SScalableBf *pTsSBFs = taosArrayGetP(pInfo->pTsSBFs, 0);
      tScalableBfDestroy(pTsSBFs);
      taosArrayRemove(pInfo->pTsSBFs, 0);
    }
  } else {
    taosArrayClearEx(pInfo->pTsSBFs, clearItemHelper);
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
  if (watermark <= adjInterval) {
    watermark = TMAX(originInt / adjInterval, 1) * adjInterval;
  }

  if (watermark > MAX_NUM_SCALABLE_BF * adjInterval) {
    watermark = MAX_NUM_SCALABLE_BF * adjInterval;
  }
  return watermark;
}

SUpdateInfo *updateInfoInitP(SInterval *pInterval, int64_t watermark, bool igUp, int8_t pkType, int32_t pkLen) {
  return updateInfoInit(pInterval->interval, pInterval->precision, watermark, igUp, pkType, pkLen);
}

SUpdateInfo *updateInfoInit(int64_t interval, int32_t precision, int64_t watermark, bool igUp, int8_t pkType, int32_t pkLen) {
  SUpdateInfo *pInfo = taosMemoryCalloc(1, sizeof(SUpdateInfo));
  if (pInfo == NULL) {
    return NULL;
  }
  pInfo->pTsBuckets = NULL;
  pInfo->pTsSBFs = NULL;
  pInfo->minTS = -1;
  pInfo->interval = adjustInterval(interval, precision);
  pInfo->watermark = adjustWatermark(pInfo->interval, interval, watermark);
  pInfo->numSBFs = 0;

  uint64_t bfSize = 0;
  if (!igUp) {
    bfSize = (uint64_t)(pInfo->watermark / pInfo->interval);
    pInfo->numSBFs = bfSize;

    pInfo->pTsSBFs = taosArrayInit(bfSize, sizeof(void *));
    if (pInfo->pTsSBFs == NULL) {
      updateInfoDestroy(pInfo);
      return NULL;
    }
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
    pInfo->pCloseWinSBF = NULL;
  }
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT);
  pInfo->pMap = taosHashInit(DEFAULT_MAP_CAPACITY, hashFn, true, HASH_NO_LOCK);
  pInfo->maxDataVersion = 0;
  pInfo->pkColLen = pkLen;
  pInfo->pkColType = pkType;
  pInfo->pKeyBuff = taosMemoryCalloc(1, sizeof(TSKEY) + sizeof(int64_t) + pkLen);
  pInfo->pValueBuff = taosMemoryCalloc(1, sizeof(TSKEY) + pkLen);
  if (pkLen != 0) {
    pInfo->comparePkRowFn = compareKeyTsAndPk;
    pInfo->comparePkCol = getKeyComparFunc(pkType, TSDB_ORDER_ASC);;
  } else {
    pInfo->comparePkRowFn = compareKeyTs;
    pInfo->comparePkCol = NULL;
  }
  return pInfo;
}

static SScalableBf *getSBf(SUpdateInfo *pInfo, TSKEY ts) {
  if (ts < 0) {
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

bool updateInfoIsTableInserted(SUpdateInfo *pInfo, int64_t tbUid) {
  void *pVal = taosHashGet(pInfo->pMap, &tbUid, sizeof(int64_t));
  if (pVal || taosHashGetSize(pInfo->pMap) >= DEFAULT_MAP_SIZE) return true;
  return false;
}

TSKEY updateInfoFillBlockData(SUpdateInfo *pInfo, SSDataBlock *pBlock, int32_t primaryTsCol, int32_t primaryKeyCol) {
  if (pBlock == NULL || pBlock->info.rows == 0) return INT64_MIN;
  TSKEY   maxTs = INT64_MIN;
  void*   pPkVal = NULL;
  void*   pMaxPkVal = NULL;
  int32_t maxLen = 0;
  int32_t len = 0;
  int64_t tbUid = pBlock->info.id.uid;

  SColumnInfoData *pColDataInfo = taosArrayGet(pBlock->pDataBlock, primaryTsCol);
  SColumnInfoData *pPkDataInfo = NULL;
  if (primaryKeyCol >= 0) {
    pPkDataInfo = taosArrayGet(pBlock->pDataBlock, primaryKeyCol);
  }

  for (int32_t i = 0; i < pBlock->info.rows; i++) {
    TSKEY ts = ((TSKEY *)pColDataInfo->pData)[i];
    if (maxTs < ts) {
      maxTs = ts;
      if (primaryKeyCol >= 0) {
        pMaxPkVal = colDataGetData(pPkDataInfo, i);
        maxLen = colDataGetRowLength(pPkDataInfo, i);
      }
    }
    SScalableBf *pSBf = getSBf(pInfo, ts);
    if (pSBf) {
      if (primaryKeyCol >= 0) {
        pPkVal = colDataGetData(pPkDataInfo, i);
        len = colDataGetRowLength(pPkDataInfo, i);
      }
      int32_t buffLen = getKeyBuff(ts, tbUid, pPkVal, len, pInfo->pKeyBuff);
      tScalableBfPut(pSBf, pInfo->pKeyBuff, buffLen);
    }
  }
  void *pMaxTs = taosHashGet(pInfo->pMap, &tbUid, sizeof(int64_t));
  if (pMaxTs == NULL || pInfo->comparePkRowFn(pMaxTs, &maxTs, pMaxPkVal, pInfo->comparePkCol) == -1) {
    int32_t valueLen = getValueBuff(maxTs, pMaxPkVal, maxLen, pInfo->pValueBuff);
    taosHashPut(pInfo->pMap, &tbUid, sizeof(int64_t), pInfo->pValueBuff, valueLen);
  }
  return maxTs;
}

bool updateInfoIsUpdated(SUpdateInfo *pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len) {
  int32_t res = TSDB_CODE_FAILED;
  int32_t buffLen = 0;

  buffLen = getKeyBuff(ts, tableId, pPkVal, len, pInfo->pKeyBuff);
  void*   *pMapMaxTs = taosHashGet(pInfo->pMap, &tableId, sizeof(uint64_t));
  uint64_t index = ((uint64_t)tableId) % pInfo->numBuckets;
  TSKEY    maxTs = *(TSKEY *)taosArrayGet(pInfo->pTsBuckets, index);
  if (ts < maxTs - pInfo->watermark) {
    // this window has been closed.
    if (pInfo->pCloseWinSBF) {
      res = tScalableBfPut(pInfo->pCloseWinSBF, pInfo->pKeyBuff, buffLen);
      if (res == TSDB_CODE_SUCCESS) {
        return false;
      } else {
        return true;
      }
    }
    return true;
  }

  SScalableBf *pSBf = getSBf(pInfo, ts);

  int32_t size = taosHashGetSize(pInfo->pMap);
  if ((!pMapMaxTs && size < DEFAULT_MAP_SIZE) || (pMapMaxTs && pInfo->comparePkRowFn(pMapMaxTs, &ts, pPkVal, pInfo->comparePkCol) == -1 )) {
    int32_t valueLen = getValueBuff(ts, pPkVal, len, pInfo->pValueBuff);
    taosHashPut(pInfo->pMap, &tableId, sizeof(uint64_t), pInfo->pValueBuff, valueLen);
    // pSBf may be a null pointer
    if (pSBf) {
      res = tScalableBfPutNoCheck(pSBf, pInfo->pKeyBuff, buffLen);
    }
    return false;
  }

  // pSBf may be a null pointer
  if (pSBf) {
    res = tScalableBfPut(pSBf, pInfo->pKeyBuff, buffLen);
  }

  if (!pMapMaxTs && maxTs < ts) {
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
  taosMemoryFreeClear(pInfo->pKeyBuff);
  taosMemoryFreeClear(pInfo->pValueBuff);
  taosHashCleanup(pInfo->pMap);
  updateInfoDestoryColseWinSBF(pInfo);
  taosMemoryFree(pInfo);
}

void updateInfoAddCloseWindowSBF(SUpdateInfo *pInfo) {
  if (pInfo->pCloseWinSBF) {
    return;
  }
  int64_t rows = adjustExpEntries(pInfo->interval * ROWS_PER_MILLISECOND);
  pInfo->pCloseWinSBF = tScalableBfInit(rows, DEFAULT_FALSE_POSITIVE);
}

void updateInfoDestoryColseWinSBF(SUpdateInfo *pInfo) {
  if (!pInfo || !pInfo->pCloseWinSBF) {
    return;
  }
  tScalableBfDestroy(pInfo->pCloseWinSBF);
  pInfo->pCloseWinSBF = NULL;
}

int32_t updateInfoSerialize(void *buf, int32_t bufLen, const SUpdateInfo *pInfo) {
  if (!pInfo) {
    return 0;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  int32_t size = taosArrayGetSize(pInfo->pTsBuckets);
  if (tEncodeI32(&encoder, size) < 0) return -1;
  for (int32_t i = 0; i < size; i++) {
    TSKEY *pTs = (TSKEY *)taosArrayGet(pInfo->pTsBuckets, i);
    if (tEncodeI64(&encoder, *pTs) < 0) return -1;
  }

  if (tEncodeU64(&encoder, pInfo->numBuckets) < 0) return -1;

  int32_t sBfSize = taosArrayGetSize(pInfo->pTsSBFs);
  if (tEncodeI32(&encoder, sBfSize) < 0) return -1;
  for (int32_t i = 0; i < sBfSize; i++) {
    SScalableBf *pSBf = taosArrayGetP(pInfo->pTsSBFs, i);
    if (tScalableBfEncode(pSBf, &encoder) < 0) return -1;
  }

  if (tEncodeU64(&encoder, pInfo->numSBFs) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->interval) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->watermark) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->minTS) < 0) return -1;

  if (tScalableBfEncode(pInfo->pCloseWinSBF, &encoder) < 0) return -1;

  int32_t mapSize = taosHashGetSize(pInfo->pMap);
  if (tEncodeI32(&encoder, mapSize) < 0) return -1;
  void  *pIte = NULL;
  size_t keyLen = 0;
  while ((pIte = taosHashIterate(pInfo->pMap, pIte)) != NULL) {
    void *key = taosHashGetKey(pIte, &keyLen);
    if (tEncodeU64(&encoder, *(uint64_t *)key) < 0) return -1;
    int32_t valueSize = taosHashGetValueSize(pIte);
    if (tEncodeBinary(&encoder, (const uint8_t *)pIte, valueSize) < 0) return -1;
  }

  if (tEncodeU64(&encoder, pInfo->maxDataVersion) < 0) return -1;

  if (tEncodeI32(&encoder, pInfo->pkColLen) < 0) return -1;
  if (tEncodeI8(&encoder, pInfo->pkColType) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t updateInfoDeserialize(void *buf, int32_t bufLen, SUpdateInfo *pInfo) {
  ASSERT(pInfo);
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;

  int32_t size = 0;
  if (tDecodeI32(&decoder, &size) < 0) return -1;
  pInfo->pTsBuckets = taosArrayInit(size, sizeof(TSKEY));
  TSKEY ts = INT64_MIN;
  for (int32_t i = 0; i < size; i++) {
    if (tDecodeI64(&decoder, &ts) < 0) return -1;
    taosArrayPush(pInfo->pTsBuckets, &ts);
  }

  if (tDecodeU64(&decoder, &pInfo->numBuckets) < 0) return -1;

  int32_t sBfSize = 0;
  if (tDecodeI32(&decoder, &sBfSize) < 0) return -1;
  pInfo->pTsSBFs = taosArrayInit(sBfSize, sizeof(void *));
  for (int32_t i = 0; i < sBfSize; i++) {
    SScalableBf *pSBf = tScalableBfDecode(&decoder);
    if (!pSBf) return -1;
    taosArrayPush(pInfo->pTsSBFs, &pSBf);
  }

  if (tDecodeU64(&decoder, &pInfo->numSBFs) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->interval) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->watermark) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->minTS) < 0) return -1;
  pInfo->pCloseWinSBF = tScalableBfDecode(&decoder);

  int32_t mapSize = 0;
  if (tDecodeI32(&decoder, &mapSize) < 0) return -1;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT);
  pInfo->pMap = taosHashInit(mapSize, hashFn, true, HASH_NO_LOCK);
  uint64_t uid = 0;
  void* pVal = NULL;
  int32_t valSize = 0;
  for (int32_t i = 0; i < mapSize; i++) {
    if (tDecodeU64(&decoder, &uid) < 0) return -1;
    if (tDecodeBinary(&decoder, (uint8_t**)&pVal, &valSize) < 0) return -1;
    taosHashPut(pInfo->pMap, &uid, sizeof(uint64_t), pVal, valSize);
  }
  ASSERT(mapSize == taosHashGetSize(pInfo->pMap));
  if (tDecodeU64(&decoder, &pInfo->maxDataVersion) < 0) return -1;

  if (tDecodeI32(&decoder, &pInfo->pkColLen) < 0) return -1;
  if (tDecodeI8(&decoder, &pInfo->pkColType) < 0) return -1;

  pInfo->pKeyBuff = taosMemoryCalloc(1, sizeof(TSKEY) + sizeof(int64_t) + pInfo->pkColLen);
  pInfo->pValueBuff = taosMemoryCalloc(1, sizeof(TSKEY) + pInfo->pkColLen);
  if (pInfo->pkColLen != 0) {
    pInfo->comparePkRowFn = compareKeyTsAndPk;
    pInfo->comparePkCol = getKeyComparFunc(pInfo->pkColType, TSDB_ORDER_ASC);;
  } else {
    pInfo->comparePkRowFn = compareKeyTs;
    pInfo->comparePkCol = NULL;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

bool isIncrementalTimeStamp(SUpdateInfo *pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len) {
  TSKEY *pMapMaxTs = taosHashGet(pInfo->pMap, &tableId, sizeof(uint64_t));
  bool   res = true;
  if (pMapMaxTs && pInfo->comparePkRowFn(pMapMaxTs, &ts, pPkVal, pInfo->comparePkCol) == 1) {
    res = false;
  } else {
    int32_t valueLen = getValueBuff(ts, pPkVal, len, pInfo->pValueBuff);
    taosHashPut(pInfo->pMap, &tableId, sizeof(uint64_t), pInfo->pValueBuff, valueLen);
  }
  return res;
}
