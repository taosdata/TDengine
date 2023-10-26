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

#ifndef _TD_UTIL_SCALABLEBF_H_
#define _TD_UTIL_SCALABLEBF_H_

#include "tbloomfilter.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SScalableBf {
  SArray  *bfArray;  // array of bloom filters
  uint32_t growth;
  uint64_t numBits;
  _hash_fn_t hashFn1;
  _hash_fn_t hashFn2;
} SScalableBf;

SScalableBf *tScalableBfInit(uint64_t expectedEntries, double errorRate);
int32_t      tScalableBfPutNoCheck(SScalableBf *pSBf, const void *keyBuf, uint32_t len);
int32_t      tScalableBfPut(SScalableBf *pSBf, const void *keyBuf, uint32_t len);
int32_t      tScalableBfNoContain(const SScalableBf *pSBf, const void *keyBuf, uint32_t len);
void         tScalableBfDestroy(SScalableBf *pSBf);
int32_t      tScalableBfEncode(const SScalableBf *pSBf, SEncoder *pEncoder);
SScalableBf *tScalableBfDecode(SDecoder *pDecoder);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SCALABLEBF_H_*/