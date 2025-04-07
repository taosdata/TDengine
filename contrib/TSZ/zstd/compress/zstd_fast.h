/*
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

#ifndef ZSTD_FAST_H
#define ZSTD_FAST_H

#if defined (__cplusplus)
extern "C" {
#endif

#include "mem.h"      /* U32 */
#include "zstd_compress_internal.h"

void ZSTD_fillHashTable(ZSTD_matchState_t* ms,
                        ZSTD_compressionParameters const* cParams,
                        void const* end, ZSTD_dictTableLoadMethod_e dtlm);
size_t ZSTD_compressBlock_fast(
        ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM],
        ZSTD_compressionParameters const* cParams, void const* src, size_t srcSize);
size_t ZSTD_compressBlock_fast_dictMatchState(
        ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM],
        ZSTD_compressionParameters const* cParams, void const* src, size_t srcSize);
size_t ZSTD_compressBlock_fast_extDict(
        ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM],
        ZSTD_compressionParameters const* cParams, void const* src, size_t srcSize);

#if defined (__cplusplus)
}
#endif

#endif /* ZSTD_FAST_H */
