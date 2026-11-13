/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_UTIL_TEST_SEAM_H_
#define _TD_UTIL_TEST_SEAM_H_

/*
 * Unit-test seams.
 *
 * A test that needs to force a failure inside the engine (out of memory, a
 * cursor error, a failed lock) used to do it with -Wl,--wrap or with cppstub.
 * Both patch code, so they only work while the engine is linked into the test
 * binary: with the engine in libtaosd_shared.so the engine's own call sites are
 * out of reach and the injection silently does nothing.
 *
 * Instead, every function a test needs to replace has one seam line at the top
 * of its definition:
 *
 *     int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry) {
 *       TD_SEAM_RET(metaFetchEntryByUid, int32_t (*)(SMeta *, int64_t, SMetaEntry **),
 *                   (pMeta, uid, ppEntry));
 *       ...unchanged...
 *     }
 *
 * The seam is a pointer that defaults to NULL, i.e. "call the real function".
 * The test assigns a replacement, and because the pointer is defined next to the
 * engine (inside libtaosd_shared.so when the shared engine is used) the engine's
 * internal callers see the replacement too.
 *
 * The pointers are declared as void* and cast to the real prototype at the seam
 * line, so this header stays free of engine types.
 *
 * BUILD_TEST_SEAM is defined when BUILD_TEST is on.  Otherwise the macros
 * expand to nothing, so a seam line compiles to no code at all and product
 * binaries are unchanged.
 */

#ifdef BUILD_TEST_SEAM

#include <stdint.h>

/* utility */
extern void *gTdSeam_taosArrayAddBatch;

/* tdb cursors */
extern void *gTdSeam_tdbTbcNext;
extern void *gTdSeam_tdbTbcMoveTo;
extern void *gTdSeam_tdbTbcMoveToNext;

/* meta */
extern void *gTdSeam_metaFetchEntryByUid;
extern void *gTdSeam_metaFetchEntryFree;
extern void *gTdSeam_metaDropTable2;
extern void *gTdSeam_metaRollbackAlterTable;
extern void *gTdSeam_metaTxnIdxDelete;
extern void *gTdSeam_metaScanTxnEntries;
extern void *gTdSeam_metaScanTxnMetaEntries;
extern void *gTdSeam_metaTxnMetaDelete;
extern void *gTdSeam_metaTxnMetaGet;
extern void *gTdSeam_metaTxnMetaUpsert;
extern void *gTdSeam_metaMarkTableTxnStatus;
extern void *gTdSeam_metaBumpVtableGen;
extern void *gTdSeam_metaRLock;
extern void *gTdSeam_metaULock;

/* vnode */
extern void *gTdSeam_vnodeAsync;

/* dnode mgmt, transport, stream, executor, storage api and server-side
 * deserialisers used by the node-level unit tests */
extern void *gTdSeam_dmMarkWrapper;
extern void *gTdSeam_dmReleaseWrapper;
extern void *gTdSeam_rpcOpen;
extern void *gTdSeam_rpcSendRequest;
extern void *gTdSeam_rpcSendResponse;
extern void *gTdSeam_rpcFreeCont;
extern void *gTdSeam_rpcSetIpWhite;
extern void *gTdSeam_rpcSetTimeIpWhite;
extern void *gTdSeam_streamHbHandleRspErr;
extern void *gTdSeam_streamHbProcessRspMsg;
extern void *gTdSeam_taosAnalyUpdate;
extern void *gTdSeam_tSingleWorkerCleanup;
extern void *gTdSeam_qStreamGetReaderInfo;
extern void *gTdSeam_syncIsReadyForRead;
extern void *gTdSeam_initStorageAPI;
extern void *gTdSeam_qDestroyTask;
extern void *gTdSeam_qCreateStreamExecTaskInfo;
extern void *gTdSeam_qNeedReset;
extern void *gTdSeam_qResetTableScan;
extern void *gTdSeam_qFinishReaderGroupRead;
extern void *gTdSeam_qSetTaskId;
extern void *gTdSeam_qEnableReaderGroupOutput;
extern void *gTdSeam_qUpdateOperatorParam;
extern void *gTdSeam_qSetStreamGen;
extern void *gTdSeam_setTaskScalarExtraInfo;
extern void *gTdSeam_qExecTaskOpt;
extern void *gTdSeam_tDeserializeSUpdateIpWhiteDual;
extern void *gTdSeam_tDeserializeRetrieveAnalyticAlgoRsp;
extern void *gTdSeam_tDeserializeSRetrieveDateTimeWhiteListRsp;

/* Real entry point for the one function whose replacement still has to free the
 * container.  It lives next to the engine, so a test calls it directly instead
 * of clearing the seam pointer (which other threads may be reading).
 * C linkage: it is defined in C, and a C++ test would mangle it. */
#ifdef __cplusplus
extern "C" {
#endif
void tdSeamReal_rpcFreeCont(void *pCont);
#ifdef __cplusplus
}
#endif

/* Seam line for a function that returns a value. */
#define TD_SEAM_RET(name, prototype, args)       \
  do {                                           \
    if (gTdSeam_##name != NULL) {                 \
      return ((prototype)gTdSeam_##name) args;    \
    }                                            \
  } while (0)

/* Seam line for a function returning void. */
#define TD_SEAM_VOID(name, prototype, args)    \
  do {                                          \
    if (gTdSeam_##name != NULL) {                \
      ((prototype)gTdSeam_##name) args;          \
      return;                                    \
    }                                            \
  } while (0)

#ifdef __cplusplus

/* A test injects a fault by assigning the seam pointer, and it has to assign
 * the copy the engine reads.  That copy lives inside libtaosd_shared.so when
 * the shared engine is in use (BUILD_TEST_SHARED_ENGINE=ON, Linux only), so
 * resolve it through the loader; everywhere else the engine is linked into the
 * test and the local pointer is the one in use. */
#if !defined(WINDOWS)
#include <dlfcn.h>
#endif

namespace tdtestseam {
inline void **seamSlot(void *&local, const char *name) {
#if defined(WINDOWS)
  (void)name;
#else
  static void *handle = dlopen("libtaosd_shared.so", RTLD_NOW | RTLD_NOLOAD);
  void **      found = (handle != nullptr) ? (void **)dlsym(handle, name) : nullptr;
  if (found != nullptr) {
    return found;
  }
#endif
  return &local;
}
}  // namespace tdtestseam

#endif /* __cplusplus */

#else

/* Not a test build: a seam line expands to nothing. */
#define TD_SEAM_RET(name, prototype, args)
#define TD_SEAM_VOID(name, prototype, args)

#endif /* BUILD_TEST_SEAM */

#endif /* _TD_UTIL_TEST_SEAM_H_ */
