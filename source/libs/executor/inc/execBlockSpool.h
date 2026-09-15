// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.

#ifndef TDENGINE_EXEC_BLOCK_SPOOL_H
#define TDENGINE_EXEC_BLOCK_SPOOL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tcommon.h"
#include "tdatablock.h"

typedef struct SExecBlockSpool SExecBlockSpool;
typedef struct SExecBlockSpoolDirectory SExecBlockSpoolDirectory;

// The spool owns page storage and the last decoded block. Destroy releases
// both, including pages that were not recycled; destroy directories separately.
int32_t execBlockSpoolCreate(int32_t pageSize, int64_t inMemBytes,
                             const char* id, const char* tempDir,
                             SExecBlockSpool** ppSpool);
void    execBlockSpoolDestroy(SExecBlockSpool* pSpool);

// Use each directory with one spool for its entire lifetime. Destroying a
// directory frees its metadata, but its pages remain owned by the spool.
int32_t execBlockSpoolDirectoryCreate(
    SExecBlockSpoolDirectory** ppDirectory);
void execBlockSpoolDirectoryDestroy(
    SExecBlockSpoolDirectory* pDirectory);
int32_t execBlockSpoolDirectorySize(
    const SExecBlockSpoolDirectory* pDirectory);

// Copies the input block; the caller retains ownership of pBlock. Its pages
// remain allocated until Recycle or spool destruction.
int32_t execBlockSpoolAppend(SExecBlockSpool* pSpool,
                             SExecBlockSpoolDirectory* pDirectory,
                             const SSDataBlock* pBlock);
// Returns a borrowed block owned by the spool. Do not free it. To retain the
// data across the next Read or spool destruction, make a deep copy with
// createOneDataBlock(*ppBlock, true, &pCopy) and destroy that copy normally.
int32_t execBlockSpoolRead(SExecBlockSpool* pSpool,
                           const SExecBlockSpoolDirectory* pDirectory,
                           int32_t blockIndex, SSDataBlock** ppBlock);
// Recycles pages without clearing the directory. Destroy the directory after
// this call; do not read, append, or recycle it again, since page IDs may be
// reused. On error, destroy the spool to release any pages still registered;
// retrying a partially recycled directory is unsafe.
int32_t execBlockSpoolRecycle(
    SExecBlockSpool* pSpool,
    const SExecBlockSpoolDirectory* pDirectory);

// Resident page payload capacity in bytes; excludes headers and metadata.
int64_t execBlockSpoolResidentPagePayloadBytes(
    const SExecBlockSpool* pSpool);
// Cumulative bytes successfully flushed since creation, including rewrites.
int64_t execBlockSpoolSpillBytes(const SExecBlockSpool* pSpool);

#if defined(BUILD_TEST)
int32_t execBlockSpoolTestActivePages(const SExecBlockSpool* pSpool);
int32_t execBlockSpoolTestRegisteredPages(const SExecBlockSpool* pSpool);
void execBlockSpoolTestFailPageAllocAfter(SExecBlockSpool* pSpool,
                                          int32_t successfulPages);
void execBlockSpoolTestFailNextDirectoryCommit(
    SExecBlockSpool* pSpool);
void execBlockSpoolTestFailNextSpillWrite(SExecBlockSpool* pSpool);
#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXEC_BLOCK_SPOOL_H
