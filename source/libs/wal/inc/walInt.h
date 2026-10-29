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

#ifndef _TD_WAL_INT_H_
#define _TD_WAL_INT_H_

#include "taoserror.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcommon.h"
#include "tcompare.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL DEBUG ", DEBUG_DEBUG, wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL TRACE ", DEBUG_TRACE, wDebugFlag, __VA_ARGS__); }}

#define wGTrace(trace, param, ...) do { if (wDebugFlag & DEBUG_TRACE) { wTrace(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define wGFatal(trace, param, ...) do { if (wDebugFlag & DEBUG_FATAL) { wFatal(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define wGError(trace, param, ...) do { if (wDebugFlag & DEBUG_ERROR) { wError(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define wGWarn(trace, param, ...)  do { if (wDebugFlag & DEBUG_WARN)  { wWarn(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define wGInfo(trace, param, ...)  do { if (wDebugFlag & DEBUG_INFO)  { wInfo(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define wGDebug(trace, param, ...) do { if (wDebugFlag & DEBUG_DEBUG) { wDebug(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)


// clang-format on

// meta section begin
typedef struct {
  int64_t firstVer;
  int64_t lastVer;
  int64_t createTs;
  int64_t closeTs;
  int64_t fileSize;
  int64_t syncedOffset;
  SDiskID diskId;  // multi-mount-point write: the tfs disk this segment actually lives on; id<0 means it lives under pWal->path (primary dir)
} SWalFileInfo;

typedef struct WalIdxEntry {
  int64_t ver;
  int64_t offset;
} SWalIdxEntry;

static inline int tSerializeWalIdxEntry(void** buf, SWalIdxEntry* pIdxEntry) {
  int tlen = 0;
  tlen += taosEncodeFixedI64(buf, pIdxEntry->ver);
  tlen += taosEncodeFixedI64(buf, pIdxEntry->offset);
  return tlen;
}

static inline void* tDeserializeWalIdxEntry(void* buf, SWalIdxEntry* pIdxEntry) {
  buf = taosDecodeFixedI64(buf, &pIdxEntry->ver);
  buf = taosDecodeFixedI64(buf, &pIdxEntry->offset);
  return buf;
}

static inline int32_t compareWalFileInfo(const void* pLeft, const void* pRight) {
  SWalFileInfo* pInfoLeft = (SWalFileInfo*)pLeft;
  SWalFileInfo* pInfoRight = (SWalFileInfo*)pRight;
  return compareInt64Val(&pInfoLeft->firstVer, &pInfoRight->firstVer);
}

static inline int64_t walGetLastFileSize(SWal* pWal) {
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) return 0;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  return pInfo->fileSize;
}

static inline int64_t walGetLastFileCachedSize(SWal* pWal) {
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) return 0;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  return (pInfo->fileSize - pInfo->syncedOffset);
}

static inline int64_t walGetLastFileFirstVer(SWal* pWal) {
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) return -1;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileFirstVer(SWal* pWal) {
  if (pWal->writeCur == -1) return -1;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->writeCur);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileLastVer(SWal* pWal) {
  if (pWal->writeCur == -1) return -1;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->writeCur);
  return pInfo->firstVer;
}

static inline int64_t walGetCurFileOffset(SWal* pWal) {
  if (pWal->writeCur == -1) return -1;
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->writeCur);
  return pInfo->fileSize;
}

static inline bool walCurFileClosed(SWal* pWal) { return taosArrayGetSize(pWal->fileInfoSet) != pWal->writeCur; }

static inline SWalFileInfo* walGetCurFileInfo(SWal* pWal) {
  if (pWal->writeCur == -1) return NULL;
  return (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, pWal->writeCur);
}

// Build a segment file name directly under an explicit directory. Used by walRollImpl
// to create a brand-new segment's files before that segment has been registered into
// pWal->fileInfoSet (so walResolveFileDir below cannot resolve it yet).
static inline void walBuildLogNameAt(const char* dir, int64_t fileFirstVer, char* buf) {
  TAOS_UNUSED(snprintf(buf, WAL_FILE_LEN, "%s%s%020" PRId64 "." WAL_LOG_SUFFIX, dir, TD_DIRSEP, fileFirstVer));
}

static inline void walBuildIdxNameAt(const char* dir, int64_t fileFirstVer, char* buf) {
  TAOS_UNUSED(snprintf(buf, WAL_FILE_LEN, "%s%s%020" PRId64 "." WAL_INDEX_SUFFIX, dir, TD_DIRSEP, fileFirstVer));
}

static inline void walBuildTxnNameAt(const char* dir, int64_t fileFirstVer, char* buf) {
  TAOS_UNUSED(snprintf(buf, WAL_FILE_LEN, "%s%s%020" PRId64 "." WAL_TXN_SUFFIX, dir, TD_DIRSEP, fileFirstVer));
}

// Resolve the directory of a segment whose diskId is already known, without consulting
// pWal->fileInfoSet and without creating anything on disk (unlike walResolveSegDir).
//
// Deletion paths MUST use this rather than walResolveFileDir below: they all drop the
// segment's fileInfoSet entry before building the file name, and once the entry is gone
// the reverse lookup finds nothing and silently falls back to pWal->path. That makes
// every unlink target the primary mount point, so segments on other level-0 mount points
// are never actually removed.
static inline void walResolveFileDirAt(SWal* pWal, SDiskID diskId, char* dirBuf, int32_t bufLen) {
  if (pWal->pTfs != NULL && diskId.id >= 0) {
    const char* diskPath = tfsGetDiskPath(pWal->pTfs, diskId);
    if (diskPath != NULL) {
      TAOS_UNUSED(snprintf(dirBuf, bufLen, "%s%s%s", diskPath, TD_DIRSEP, pWal->relDir));
      return;
    }
  }
  tstrncpy(dirBuf, pWal->path, bufLen);
}

// Resolve the directory that holds the segment starting at fileFirstVer, by looking up
// its diskId in pWal->fileInfoSet. Falls back to pWal->path when tfs is unbound, the
// segment predates this feature (diskId.id < 0), or no matching entry is found.
static inline void walResolveFileDir(SWal* pWal, int64_t fileFirstVer, char* dirBuf, int32_t bufLen) {
  if (pWal->pTfs != NULL) {
    int32_t       sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
    SWalFileInfo* pData = (SWalFileInfo*)pWal->fileInfoSet->pData;
    for (int32_t i = sz - 1; i >= 0; i--) {
      if (pData[i].firstVer == fileFirstVer) {
        if (pData[i].diskId.id >= 0) {
          const char* diskPath = tfsGetDiskPath(pWal->pTfs, pData[i].diskId);
          if (diskPath != NULL) {
            TAOS_UNUSED(snprintf(dirBuf, bufLen, "%s%s%s", diskPath, TD_DIRSEP, pWal->relDir));
            return;
          }
        }
        break;
      }
    }
  }
  tstrncpy(dirBuf, pWal->path, bufLen);
}

// These keep their original signature so all existing call sites (walRead.c, walMgmt.c,
// walMeta.c, walTxn.c, and most of walWrite.c) need no changes at all: once a segment's
// diskId has been recorded in fileInfoSet, they transparently resolve to the right disk.
static inline void walBuildLogName(SWal* pWal, int64_t fileFirstVer, char* buf) {
  char dir[WAL_PATH_LEN];
  walResolveFileDir(pWal, fileFirstVer, dir, sizeof(dir));
  walBuildLogNameAt(dir, fileFirstVer, buf);
}

static inline void walBuildIdxName(SWal* pWal, int64_t fileFirstVer, char* buf) {
  char dir[WAL_PATH_LEN];
  walResolveFileDir(pWal, fileFirstVer, dir, sizeof(dir));
  walBuildIdxNameAt(dir, fileFirstVer, buf);
}

static inline void walBuildTxnName(SWal* pWal, int64_t fileFirstVer, char* buf) {
  char dir[WAL_PATH_LEN];
  walResolveFileDir(pWal, fileFirstVer, dir, sizeof(dir));
  walBuildTxnNameAt(dir, fileFirstVer, buf);
}

#define WAL_TXN_PENDING_FILE    "txn.pending"
#define WAL_TXN_PENDING_MAGIC   0x544E5850U  // "TNXP"

// Retention tuning knobs for walTxnFilesTrim (walTxn.c). Declared here rather than in
// walTxn.c so the trim tests can state their preconditions in terms of the thresholds
// instead of restating the literals.
//   extra hours: keep .txn files no older than (oldest .log createTs - WAL_TXN_TRIM_EXTRA_HOURS)
//   extra files: keep at most (logCount + WAL_TXN_TRIM_EXTRA_FILES) .txn files
#define WAL_TXN_TRIM_EXTRA_HOURS 4
#define WAL_TXN_TRIM_EXTRA_FILES 6

static inline void walBuildTxnPendingName(SWal* pWal, char* buf) {
  TAOS_UNUSED(snprintf(buf, WAL_FILE_LEN, "%s%s" WAL_TXN_PENDING_FILE, pWal->path, TD_DIRSEP));
}

// txn.pending on-disk layout: 16 bytes total, written as single pwrite
#pragma pack(push, 1)
typedef struct {
  uint32_t magic;              // WAL_TXN_PENDING_MAGIC
  uint32_t crc32;              // checksum of firstTxnWalIndex field
  int64_t  firstTxnWalIndex;  // first txn walIndex pending in current batch; 0 = cleared
} STxnPendingFile;
#pragma pack(pop)

// Upper bound for the body length declared by a WAL record header. A body is written by
// walWriteImpl from a single client/raft message, so a real one is orders of magnitude
// below this; anything larger -- or negative -- means the bytes being inspected are not a
// record header that this code wrote.
//
// This has to be checked before bodyLen is used for ANYTHING, because every consumer
// treats it as a trusted length: walValidBodyCksum passes it to taosCheckChecksum as a
// uint32_t (a negative value becomes ~4 GB and walks off the end of the buffer),
// walFetchBody/walReadVer size a read with it, walSkipFetchBody seeks by it, and
// walCheckAndRepairIdxFile advances an idx offset by it. The header checksum is not
// evidence of sanity here: it is unkeyed CRC32C over the header, so whoever chose the
// bytes could compute it.
#define WAL_MAX_BODY_LEN (512 * 1024 * 1024)

static inline bool walValidBodyLen(int32_t bodyLen) { return bodyLen >= 0 && bodyLen <= WAL_MAX_BODY_LEN; }

static inline int walValidHeadCksum(SWalCkHead* pHead) {
  return taosCheckChecksum((uint8_t*)&pHead->head, sizeof(SWalCont), pHead->cksumHead);
}

static inline int walValidBodyCksum(SWalCkHead* pHead) {
  // never hand an out-of-range length to the checksum: taosCheckChecksum takes a
  // uint32_t size and crc32c loops until it is exhausted
  if (!walValidBodyLen(pHead->head.bodyLen)) return -1;
  return taosCheckChecksum((uint8_t*)pHead->head.body, pHead->head.bodyLen, pHead->cksumBody);
}

static inline int walValidCksum(SWalCkHead* pHead, void* body, int64_t bodyLen) {
  return walValidHeadCksum(pHead) && walValidBodyCksum(pHead);
}

static inline uint32_t walCalcHeadCksum(SWalCkHead* pHead) {
  return taosCalcChecksum(0, (uint8_t*)&pHead->head, sizeof(SWalCont));
}

static inline uint32_t walCalcBodyCksum(const void* body, uint32_t len) {
  return taosCalcChecksum(0, (uint8_t*)body, len);
}

static inline int64_t walGetVerIdxOffset(SWal* pWal, int64_t ver) {
  return (ver - walGetCurFileFirstVer(pWal)) * sizeof(SWalIdxEntry);
}

static inline void walResetVer(SWalVer* pVer) {
  pVer->firstVer = -1;
  pVer->verInSnapshotting = -1;
  pVer->snapshotVer = -1;
  pVer->commitVer = -1;
  pVer->lastVer = -1;
}

int32_t walLoadMeta(SWal* pWal);
int32_t walSaveMeta(SWal* pWal);
int32_t walRemoveMeta(SWal* pWal);
int32_t walRollImpl(SWal* pWal);
int32_t walRollFileInfo(SWal* pWal);
int32_t walScanLogGetLastVer(SWal* pWal, int32_t fileIdx, int64_t* lastVer);
int32_t walCheckAndRepairMeta(SWal* pWal);
int64_t walChangeWrite(SWal* pWal, int64_t ver);

int32_t walCheckAndRepairIdx(SWal* pWal);

int32_t walMetaSerialize(SWal* pWal, char** serialized);
int32_t walMetaDeserialize(SWal* pWal, const char* bytes);
// meta section end

// multi-mount-point write section (level 0 only), implemented in walWrite.c
int32_t walTfsAllocDisk(SWal* pWal, SDiskID* pDiskId);
int32_t walResolveSegDir(SWal* pWal, SDiskID diskId, char* dirBuf, int32_t bufLen);

int32_t decryptBody(SWalCfg* cfg, SWalCkHead* pHead, int32_t plainBodyLen, const char* func);

// txn file internal API (walTxn.c)
int32_t walTxnFilesOpen(SWal* pWal);
void    walTxnFilesClose(SWal* pWal);
void    walTxnWriteEntry(SWal* pWal, int64_t index, uint64_t txnExtFlags, txn_id_t txnId, const void* body,
                          int32_t bodyLen, const void* encBuf, int32_t encBodyLen);
void    walTxnPreFsync(SWal* pWal);   // Step A: write txn.pending before .log fsync
void    walTxnPostFsync(SWal* pWal);  // Steps C+D: fsync .txn, clear pending
void    walTxnFilesRotate(SWal* pWal, int64_t newFirstVer);
int32_t walTxnFilesRecover(SWal* pWal, int64_t committedVer);
void    walTxnFilesTrim(SWal* pWal);
// Truncate the current open .txn write file to remove entries with version > truncVer.
int32_t walTxnTruncateCurrent(SWal* pWal, int64_t truncVer);

int64_t walGetSeq();

#ifdef __cplusplus
}
#endif

#endif /*_TD_WAL_INT_H_*/
