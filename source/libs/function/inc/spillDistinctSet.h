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

#ifndef _TD_SPILL_DISTINCT_SET_H_
#define _TD_SPILL_DISTINCT_SET_H_

#include "os.h"
#include "tcommon.h"

typedef struct SDiskbasedBuf SDiskbasedBuf;

#ifdef __cplusplus
extern "C" {
#endif

// Spillable DISTINCT set for aggregate deduplication.
//
// A plain in-memory hash set cannot bound DISTINCT aggregates: the set lives in the function's
// result-row inter-buffer, so a query with many groups or windows holds one set per result row and
// high cardinality has no ceiling. This is a grace-hash set instead:
//
//   * while total value bytes stay under memThreshold, a single in-memory hash gives exact
//     membership on the fast path and spillDistinctSetAdd() reports each value as new or duplicate
//     immediately;
//   * on crossing the threshold the resident values are written to per-partition pages in a
//     SDiskbasedBuf (tsTempDir) and the hash is released. From then on every value is appended to
//     the page chain of its partition without a membership test, so inserts stay O(1) and memory
//     stays bounded by the paged buffer's fixed active-page cache;
//   * spillDistinctSetFinalize() then reads one partition at a time into a fresh hash and reports
//     each distinct value through a callback. Equal values always hash to the same partition, so
//     per-partition dedup is exact, and peak memory is the largest partition rather than the whole
//     set.
//
// Callers must therefore treat the return of spillDistinctSetAdd() as authoritative only while
// spillDistinctSetSpilled() is false. Once spilled, the inline running total is incomplete and the
// caller must discard it and rebuild from spillDistinctSetFinalize().
//
// The set is single-use: after spillDistinctSetFinalize() the spilled state persists, so a further
// spillDistinctSetAdd() would append to the partition pages without being reflected in the
// already-produced output. Do not add after finalizing; destroy the set instead.
typedef struct SSpillDistinctSet SSpillDistinctSet;

// The set is not thread-safe. Each execution owner must serialize Add/Finalize/Destroy calls and
// must not share one set concurrently between worker threads.

// Invoked once per distinct value during spillDistinctSetFinalize(). Returning non-zero aborts the
// scan and the code is propagated to the caller.
//
// pData is borrowed and valid only for the duration of the call. The two paths back it differently
// -- resident values point into the internal hash, replayed ones into a page buffer that is
// released as soon as the page has been walked -- so a retained pointer dangles on the spilled
// path. Copy anything you need to keep before returning.
typedef int32_t (*FSpillDistinctValueFn)(void* param, const char* pData, int32_t len);

// type is the TSDB_DATA_TYPE_* enum (from tcommon.h) identifying the value's type. Used to
// distinguish var-length types (IS_VAR_DATA_TYPE) and large blobs (IS_STR_DATA_BLOB) for
// valueBytes validation and page sizing. For fixed-width types, spillDistinctSetAdd() expects
// len == valueBytes on every call; for var types, len <= valueBytes.
//
// valueBytes is the fixed width for non-var types, or the declared maximum for var types.
// memThreshold is the resident byte budget before spilling; <= 0 selects the tsPQSortMemThreshold
// default. perResultRow shrinks the page size, mirroring tMemBucketCreate(): a query with windows
// or groups instantiates one set per result row, so large pages would multiply badly.
int32_t spillDistinctSetCreate(SSpillDistinctSet** ppSet, int8_t type, int32_t valueBytes,
                               int64_t memThreshold, bool perResultRow, const char* id);
int32_t spillDistinctSetCreateWithBuf(SSpillDistinctSet** ppSet, int8_t type, int32_t valueBytes,
                                      int64_t memThreshold, bool perResultRow, const char* id,
                                      SDiskbasedBuf** ppSharedBuf);
// When ppSharedBuf is supplied, the caller owns the shared buffer and must destroy it only after
// every set created with that pointer has been destroyed. Set destruction only releases its pages.
void    spillDistinctSetDestroy(SSpillDistinctSet* pSet);

// Adds a value. *pIsNew is true only when the set is still resident and the value was absent;
// while spilled it is always false and the value is merely recorded for the finalize pass.
int32_t spillDistinctSetAdd(SSpillDistinctSet* pSet, const char* pData, int32_t len, bool* pIsNew);

// True once the set has spilled, meaning per-add answers were incomplete and the caller must use
// spillDistinctSetFinalize() to obtain the distinct values. Accepts NULL (reported as not spilled),
// since the set is only created on the first add and an all-NULL input never creates one.
bool    spillDistinctSetSpilled(const SSpillDistinctSet* pSet);
bool    spillDistinctSetFinalized(const SSpillDistinctSet* pSet);

// Number of distinct values seen while resident. Meaningless once spilled.
int64_t spillDistinctSetResidentCount(const SSpillDistinctSet* pSet);

// Reports every distinct value exactly once. Safe to call whether or not the set spilled.
int32_t spillDistinctSetFinalize(SSpillDistinctSet* pSet, FSpillDistinctValueFn fn, void* param);

#ifdef __cplusplus
}
#endif

#endif  // _TD_SPILL_DISTINCT_SET_H_
