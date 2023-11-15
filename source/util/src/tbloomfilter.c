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

#include "tbloomfilter.h"
#include "taos.h"
#include "taoserror.h"

#define UNIT_NUM_BITS      64ULL
#define UNIT_ADDR_NUM_BITS 6ULL

static FORCE_INLINE bool setBit(uint64_t *buf, uint64_t index) {
  uint64_t unitIndex = index >> UNIT_ADDR_NUM_BITS;
  uint64_t old = buf[unitIndex];
  buf[unitIndex] |= (1ULL << (index % UNIT_NUM_BITS));
  return buf[unitIndex] != old;
}

static FORCE_INLINE bool getBit(uint64_t *buf, uint64_t index) {
  uint64_t unitIndex = index >> UNIT_ADDR_NUM_BITS;
  uint64_t mask = 1ULL << (index % UNIT_NUM_BITS);
  return buf[unitIndex] & mask;
}

SBloomFilter *tBloomFilterInit(uint64_t expectedEntries, double errorRate) {
  if (expectedEntries < 1 || errorRate <= 0 || errorRate >= 1.0) {
    return NULL;
  }
  SBloomFilter *pBF = taosMemoryCalloc(1, sizeof(SBloomFilter));
  if (pBF == NULL) {
    return NULL;
  }
  pBF->expectedEntries = expectedEntries;
  pBF->errorRate = errorRate;

  double lnRate = fabs(log(errorRate));
  // ln(2)^2 = 0.480453013918201
  // m = - n * ln(P) / ( ln(2) )^2
  // m is the size of bloom filter, n is expected entries, P is false positive probability
  pBF->numUnits = (uint64_t)ceil(expectedEntries * lnRate / 0.480453013918201 / UNIT_NUM_BITS);
  pBF->numBits = pBF->numUnits * 64;
  pBF->size = 0;

  // ln(2) = 0.693147180559945
  pBF->hashFunctions = (uint32_t)ceil(lnRate / 0.693147180559945);
  pBF->hashFn1 = HASH_FUNCTION_1;
  pBF->hashFn2 = HASH_FUNCTION_2;
  pBF->buffer = taosMemoryCalloc(pBF->numUnits, sizeof(uint64_t));
  if (pBF->buffer == NULL) {
    tBloomFilterDestroy(pBF);
    return NULL;
  }
  return pBF;
}

int32_t tBloomFilterPutHash(SBloomFilter *pBF, uint64_t hash1, uint64_t hash2) {
  ASSERT(!tBloomFilterIsFull(pBF));
  bool                    hasChange = false;
  const register uint64_t size = pBF->numBits;
  uint64_t                cbHash = hash1;
  for (uint32_t i = 0; i < pBF->hashFunctions; ++i) {
    hasChange |= setBit(pBF->buffer, cbHash % size);
    cbHash += hash2;
  }
  if (hasChange) {
    pBF->size++;
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_FAILED;
}

int32_t tBloomFilterPut(SBloomFilter *pBF, const void *keyBuf, uint32_t len) {
  uint64_t                h1 = (uint64_t)pBF->hashFn1(keyBuf, len);
  uint64_t                h2 = (uint64_t)pBF->hashFn2(keyBuf, len);
  bool                    hasChange = false;
  const register uint64_t size = pBF->numBits;
  uint64_t                cbHash = h1;
  for (uint32_t i = 0; i < pBF->hashFunctions; ++i) {
    hasChange |= setBit(pBF->buffer, cbHash % size);
    cbHash += h2;
  }
  if (hasChange) {
    pBF->size++;
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_FAILED;
}

int32_t tBloomFilterNoContain(const SBloomFilter *pBF, uint64_t hash1, uint64_t hash2) {
  const register uint64_t size = pBF->numBits;
  uint64_t                cbHash = hash1;
  for (uint32_t i = 0; i < pBF->hashFunctions; ++i) {
    if (!getBit(pBF->buffer, cbHash % size)) {
      return TSDB_CODE_SUCCESS;
    }
    cbHash += hash2;
  }
  return TSDB_CODE_FAILED;
}

void tBloomFilterDestroy(SBloomFilter *pBF) {
  if (pBF == NULL) {
    return;
  }
  taosMemoryFree(pBF->buffer);
  taosMemoryFree(pBF);
}

int32_t tBloomFilterEncode(const SBloomFilter *pBF, SEncoder *pEncoder) {
  if (tEncodeU32(pEncoder, pBF->hashFunctions) < 0) return -1;
  if (tEncodeU64(pEncoder, pBF->expectedEntries) < 0) return -1;
  if (tEncodeU64(pEncoder, pBF->numUnits) < 0) return -1;
  if (tEncodeU64(pEncoder, pBF->numBits) < 0) return -1;
  if (tEncodeU64(pEncoder, pBF->size) < 0) return -1;
  for (uint64_t i = 0; i < pBF->numUnits; i++) {
    uint64_t *pUnits = (uint64_t *)pBF->buffer;
    if (tEncodeU64(pEncoder, pUnits[i]) < 0) return -1;
  }
  if (tEncodeDouble(pEncoder, pBF->errorRate) < 0) return -1;
  return 0;
}

SBloomFilter *tBloomFilterDecode(SDecoder *pDecoder) {
  SBloomFilter *pBF = taosMemoryCalloc(1, sizeof(SBloomFilter));
  pBF->buffer = NULL;
  if (tDecodeU32(pDecoder, &pBF->hashFunctions) < 0) goto _error;
  if (tDecodeU64(pDecoder, &pBF->expectedEntries) < 0) goto _error;
  if (tDecodeU64(pDecoder, &pBF->numUnits) < 0) goto _error;
  if (tDecodeU64(pDecoder, &pBF->numBits) < 0) goto _error;
  if (tDecodeU64(pDecoder, &pBF->size) < 0) goto _error;
  pBF->buffer = taosMemoryCalloc(pBF->numUnits, sizeof(uint64_t));
  for (int32_t i = 0; i < pBF->numUnits; i++) {
    uint64_t *pUnits = (uint64_t *)pBF->buffer;
    if (tDecodeU64(pDecoder, pUnits + i) < 0) goto _error;
  }
  if (tDecodeDouble(pDecoder, &pBF->errorRate) < 0) goto _error;
  pBF->hashFn1 = HASH_FUNCTION_1;
  pBF->hashFn2 = HASH_FUNCTION_2;
  return pBF;

_error:
  tBloomFilterDestroy(pBF);
  return NULL;
}

bool tBloomFilterIsFull(const SBloomFilter *pBF) { return pBF->size >= pBF->expectedEntries; }
