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

#ifndef _TD_UTIL_BLOOMFILTER_H_
#define _TD_UTIL_BLOOMFILTER_H_

#include "os.h"
#include "tencode.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HASH_FUNCTION_1 taosFastHash
#define HASH_FUNCTION_2 taosDJB2Hash

typedef struct SBloomFilter {
  uint32_t   hashFunctions;
  uint64_t   expectedEntries;
  uint64_t   numUnits;
  uint64_t   numBits;
  uint64_t   size;
  _hash_fn_t hashFn1;
  _hash_fn_t hashFn2;
  void      *buffer;
  double     errorRate;
} SBloomFilter;

SBloomFilter *tBloomFilterInit(uint64_t expectedEntries, double errorRate);
int32_t       tBloomFilterPutHash(SBloomFilter *pBF, uint64_t hash1, uint64_t hash2);
int32_t       tBloomFilterPut(SBloomFilter *pBF, const void *keyBuf, uint32_t len);
int32_t       tBloomFilterNoContain(const SBloomFilter *pBF, uint64_t h1, uint64_t h2);
void          tBloomFilterDestroy(SBloomFilter *pBF);
void          tBloomFilterDump(const SBloomFilter *pBF);
bool          tBloomFilterIsFull(const SBloomFilter *pBF);
int32_t       tBloomFilterEncode(const SBloomFilter *pBF, SEncoder *pEncoder);
SBloomFilter *tBloomFilterDecode(SDecoder *pDecoder);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_BLOOMFILTER_H_*/