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

#define _DEFAULT_SOURCE

#include "tscalablebf.h"
#include "taoserror.h"

#define DEFAULT_GROWTH           2
#define DEFAULT_TIGHTENING_RATIO 0.5

static SBloomFilter *tScalableBfAddFilter(SScalableBf *pSBf, uint64_t expectedEntries, double errorRate);

SScalableBf *tScalableBfInit(uint64_t expectedEntries, double errorRate) {
  const uint32_t defaultSize = 8;
  if (expectedEntries < 1 || errorRate <= 0 || errorRate >= 1.0) {
    return NULL;
  }
  SScalableBf *pSBf = taosMemoryCalloc(1, sizeof(SScalableBf));
  if (pSBf == NULL) {
    return NULL;
  }
  pSBf->numBits = 0;
  pSBf->bfArray = taosArrayInit(defaultSize, sizeof(void *));
  if (tScalableBfAddFilter(pSBf, expectedEntries, errorRate * DEFAULT_TIGHTENING_RATIO) == NULL) {
    tScalableBfDestroy(pSBf);
    return NULL;
  }
  pSBf->growth = DEFAULT_GROWTH;
  pSBf->hashFn1 = HASH_FUNCTION_1;
  pSBf->hashFn2 = HASH_FUNCTION_2;
  return pSBf;
}

int32_t tScalableBfPutNoCheck(SScalableBf *pSBf, const void *keyBuf, uint32_t len) {
  int32_t size = taosArrayGetSize(pSBf->bfArray);
  SBloomFilter *pNormalBf = taosArrayGetP(pSBf->bfArray, size - 1);
  ASSERT(pNormalBf);
  if (tBloomFilterIsFull(pNormalBf)) {
    pNormalBf = tScalableBfAddFilter(pSBf, pNormalBf->expectedEntries * pSBf->growth,
                                     pNormalBf->errorRate * DEFAULT_TIGHTENING_RATIO);
    if (pNormalBf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return tBloomFilterPut(pNormalBf, keyBuf, len);
}

int32_t tScalableBfPut(SScalableBf *pSBf, const void *keyBuf, uint32_t len) {
  uint64_t h1 = (uint64_t)pSBf->hashFn1(keyBuf, len);
  uint64_t h2 = (uint64_t)pSBf->hashFn2(keyBuf, len);
  int32_t size = taosArrayGetSize(pSBf->bfArray);
  for (int32_t i = size - 2; i >= 0; --i) {
    if (tBloomFilterNoContain(taosArrayGetP(pSBf->bfArray, i), h1, h2) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_FAILED;
    }
  }

  SBloomFilter *pNormalBf = taosArrayGetP(pSBf->bfArray, size - 1);
  ASSERT(pNormalBf);
  if (tBloomFilterIsFull(pNormalBf)) {
    pNormalBf = tScalableBfAddFilter(pSBf, pNormalBf->expectedEntries * pSBf->growth,
                                     pNormalBf->errorRate * DEFAULT_TIGHTENING_RATIO);
    if (pNormalBf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return tBloomFilterPutHash(pNormalBf, h1, h2);
}

int32_t tScalableBfNoContain(const SScalableBf *pSBf, const void *keyBuf, uint32_t len) {
  uint64_t h1 = (uint64_t)pSBf->hashFn1(keyBuf, len);
  uint64_t h2 = (uint64_t)pSBf->hashFn2(keyBuf, len);
  int32_t size = taosArrayGetSize(pSBf->bfArray);
  for (int32_t i = size - 1; i >= 0; --i) {
    if (tBloomFilterNoContain(taosArrayGetP(pSBf->bfArray, i), h1, h2) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_FAILED;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SBloomFilter *tScalableBfAddFilter(SScalableBf *pSBf, uint64_t expectedEntries, double errorRate) {
  SBloomFilter *pNormalBf = tBloomFilterInit(expectedEntries, errorRate);
  if (pNormalBf == NULL) {
    return NULL;
  }
  if (taosArrayPush(pSBf->bfArray, &pNormalBf) == NULL) {
    tBloomFilterDestroy(pNormalBf);
    return NULL;
  }
  pSBf->numBits += pNormalBf->numBits;
  return pNormalBf;
}

void tScalableBfDestroy(SScalableBf *pSBf) {
  if (pSBf == NULL) {
    return;
  }
  if (pSBf->bfArray != NULL) {
    taosArrayDestroyP(pSBf->bfArray, (FDelete)tBloomFilterDestroy);
  }
  taosMemoryFree(pSBf);
}

int32_t tScalableBfEncode(const SScalableBf *pSBf, SEncoder *pEncoder) {
  if (!pSBf) {
    if (tEncodeI32(pEncoder, 0) < 0) return -1;
    return 0;
  }
  int32_t size = taosArrayGetSize(pSBf->bfArray);
  if (tEncodeI32(pEncoder, size) < 0) return -1;
  for (int32_t i = 0; i < size; i++) {
    SBloomFilter *pBF = taosArrayGetP(pSBf->bfArray, i);
    if (tBloomFilterEncode(pBF, pEncoder) < 0) return -1;
  }
  if (tEncodeU32(pEncoder, pSBf->growth) < 0) return -1;
  if (tEncodeU64(pEncoder, pSBf->numBits) < 0) return -1;
  return 0;
}

SScalableBf *tScalableBfDecode(SDecoder *pDecoder) {
  SScalableBf *pSBf = taosMemoryCalloc(1, sizeof(SScalableBf));
  pSBf->hashFn1 = HASH_FUNCTION_1;
  pSBf->hashFn2 = HASH_FUNCTION_2;
  pSBf->bfArray = NULL;
  int32_t size = 0;
  if (tDecodeI32(pDecoder, &size) < 0) goto _error;
  if (size == 0) {
    tScalableBfDestroy(pSBf);
    return NULL;
  }
  pSBf->bfArray = taosArrayInit(size * 2, sizeof(void *));
  for (int32_t i = 0; i < size; i++) {
    SBloomFilter *pBF = tBloomFilterDecode(pDecoder);
    if (!pBF) goto _error;
    taosArrayPush(pSBf->bfArray, &pBF);
  }
  if (tDecodeU32(pDecoder, &pSBf->growth) < 0) goto _error;
  if (tDecodeU64(pDecoder, &pSBf->numBits) < 0) goto _error;
  return pSBf;

_error:
  tScalableBfDestroy(pSBf);
  return NULL;
}
