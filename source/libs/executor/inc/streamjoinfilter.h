/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef TDENGINE_STREAMJOINFILTER_H
#define TDENGINE_STREAMJOINFILTER_H

#include "tdatablock.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_JOIN_FILTER_MAX_KEYS      64
#define STREAM_JOIN_FILTER_MAX_KEY_BYTES 16384

typedef struct SStreamJoinFilterColumn {
  int32_t   joinKeyIndex;
  int32_t   buildSlot;
  int32_t   probeSlot;
  int32_t   inputSlot;
  SDataType buildType;
  SDataType probeType;
  uint8_t   keyPrecision;
} SStreamJoinFilterColumn;

typedef struct SStreamJoinFilterSpec {
  int64_t probeBlockId;
  int64_t receiverBlockId;
  int64_t inputBlockId;
  SArray* pColumns;  // Owned SStreamJoinFilterColumn descriptors.
} SStreamJoinFilterSpec;

typedef enum EStreamJoinFilterFallback {
  STREAM_JOIN_FILTER_NO_FALLBACK = 0,
  STREAM_JOIN_FILTER_KEY_LIMIT,
  STREAM_JOIN_FILTER_BYTE_LIMIT
} EStreamJoinFilterFallback;

typedef struct SStreamJoinFilter {
  SStreamJoinFilterSpec spec;  // Owned copy; no block pointers are retained.
  int32_t               execId;
  uint64_t              baseGId;
  SSHashObj*            pKeys;
  char*                 pScratch;
  int32_t               scratchCapacity;
  int32_t               keyCount;
  int32_t               keyBytes;
} SStreamJoinFilter;

/**
 * Encode a row using a validated spec. Caller owns scratch and frees it with taosMemoryFree.
 * Bytes last until the next encoding or cleanup. NULL returns no key; oversized non-NULL keys
 * return MAX_KEY_BYTES + 1 without growing scratch beyond MAX_KEY_BYTES.
 */
int32_t streamJoinFilterEncodeKey(const SStreamJoinFilterSpec* pSpec, const SSDataBlock* pBlock, int32_t row,
                                  bool build, char** ppScratch, int32_t* pCapacity, int32_t* pLength, bool* pNull);
int32_t streamJoinFilterCreate(const SStreamJoinFilterSpec* pSpec, int32_t execId, uint64_t baseGId,
                               SStreamJoinFilter** ppFilter);
int32_t streamJoinFilterAddRow(SStreamJoinFilter* pFilter, const SSDataBlock* pBlock, int32_t row,
                               EStreamJoinFilterFallback* pFallback);
int32_t streamJoinFilterMatchRow(SStreamJoinFilter* pFilter, const SSDataBlock* pBlock, int32_t row, bool* pMatch);
void    streamJoinFilterDestroy(SStreamJoinFilter** ppFilter);
void    streamJoinFilterSpecClear(SStreamJoinFilterSpec* pSpec);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_STREAMJOINFILTER_H
