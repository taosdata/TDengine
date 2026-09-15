// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.

#ifndef TDENGINE_INTERP_RESULT_STORE_H
#define TDENGINE_INTERP_RESULT_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tcommon.h"

typedef struct SInterpResultStore SInterpResultStore;

int32_t interpResultStoreCreate(SInterpResultStore** ppStore);
// Appends a deep copy of pBlock. pBlock must contain at least one row;
// empty blocks are rejected with TSDB_CODE_INVALID_PARA.
int32_t interpResultStoreAppend(SInterpResultStore* pStore, uint64_t groupId,
                                const SSDataBlock* pBlock);
// Opens the store's only read session and binds it to groupId. Until the
// session reaches EOF and the group is dropped, no other group can be opened
// and no more blocks can be appended to this group.
int32_t interpResultStoreOpenRead(SInterpResultStore* pStore, uint64_t groupId);
// Returns a store-owned block through ppBlock. The block is borrowed and is
// valid only until the next NextBlock call, Reset, or Destroy. EOF is reported
// by a successful call that sets *ppBlock to NULL.
int32_t interpResultStoreNextBlock(SInterpResultStore* pStore,
                                   SSDataBlock** ppBlock);
// Drops the active group only after NextBlock has reported EOF, then closes the
// read session. The drop is committed even if page recycling reports an error;
// failed pages remain owned by the spool until Reset or Destroy. A non-success
// result is fatal cleanup status and must not be retried for the same groupId.
// This is not an abort operation: an execution that abandons a read because of
// reset, cancellation, or failure must use Reset or Destroy to release all
// resident groups and invalidate borrowed blocks.
int32_t interpResultStoreDropGroup(SInterpResultStore* pStore,
                                   uint64_t groupId);
// Returns logical column bytes held by all committed groups. This is not
// resident memory: encoded pages can be resident or spilled.
int64_t interpResultStoreBytesUsed(const SInterpResultStore* pStore);
// Returns resident page payload bytes in the shared pool. This excludes page
// allocation overhead, directories, codec scratch, and decoded blocks.
int64_t interpResultStoreResidentPagePayloadBytes(
    const SInterpResultStore* pStore);
// Returns cumulative bytes flushed to this store's spill file.
int64_t interpResultStoreSpillBytes(const SInterpResultStore* pStore);
// Releases all resident groups, closes any active read session, and
// invalidates all borrowed blocks while keeping the store reusable.
void    interpResultStoreReset(SInterpResultStore* pStore);
// Performs Reset and releases the store itself.
void    interpResultStoreDestroy(SInterpResultStore* pStore);

#if defined(BUILD_TEST)
int32_t interpResultStoreTestCreate(SInterpResultStore** ppStore,
                                    int32_t pageSize,
                                    int64_t inMemBytes);
int32_t interpResultStoreTestActivePages(
    const SInterpResultStore* pStore);
int32_t interpResultStoreTestRegisteredPages(
    const SInterpResultStore* pStore);
void interpResultStoreTestFailPageAllocAfter(
    SInterpResultStore* pStore, int32_t successfulPages);
void interpResultStoreTestFailNextDirectoryCommit(
    SInterpResultStore* pStore);
void interpResultStoreTestFailNextSpillWrite(
    SInterpResultStore* pStore);
#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_INTERP_RESULT_STORE_H
