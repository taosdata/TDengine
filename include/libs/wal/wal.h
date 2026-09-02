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

#ifndef _TD_WAL_H_
#define _TD_WAL_H_

#include "os.h"
#include "tarray.h"
#include "tdef.h"
#include "tfs.h"
#include "tlog.h"
#include "tmsg.h"
#include "ttrace.h"

#ifdef __cplusplus
extern "C" {
#endif

#define WAL_PROTO_VER     0
#define WAL_NOSUFFIX_LEN  20

// Accessors for SWalCont.flags -- use these instead of direct bit manipulation
#define WAL_PROTO_VER_MASK      ((uint8_t)0x7Fu)  // bit0-6: protocol version
#define WAL_CONT_FLAG_TXN       ((uint8_t)0x80u)  // bit7:   transaction entry flag
#define WAL_IS_TXN_MSG(cont)    (((cont)->flags & WAL_CONT_FLAG_TXN) != 0)
#define WAL_SET_TXN_MSG(cont)   ((cont)->flags |= WAL_CONT_FLAG_TXN)
#define WAL_CLEAR_TXN_MSG(cont) ((cont)->flags &= (uint8_t)~WAL_CONT_FLAG_TXN)
#define WAL_GET_PROTO_VER(cont) ((cont)->flags & WAL_PROTO_VER_MASK)
// WAL_PROTO_VER_SUPPORTED: highest protoVer this build can read; reject anything above it
#define WAL_PROTO_VER_SUPPORTED  WAL_PROTO_VER
#define WAL_SET_PROTO_VER(cont, ver) \
  ((cont)->flags = ((cont)->flags & WAL_CONT_FLAG_TXN) | (uint8_t)((ver) & WAL_PROTO_VER_MASK))

#define WAL_SUFFIX_AT     (WAL_NOSUFFIX_LEN + 1)
#define WAL_LOG_SUFFIX    "log"
#define WAL_INDEX_SUFFIX  "idx"
#define WAL_TXN_SUFFIX    "txn"
#define WAL_REFRESH_MS    1000
#define WAL_PATH_LEN      (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN      (WAL_PATH_LEN + 32)
#define WAL_MAGIC         0xFAFBFCFDF4F3F2F1ULL
#define WAL_SCAN_BUF_SIZE (1024 * 1024 * 3)
#define WAL_JSON_BUF_SIZE 30

typedef enum {
  TAOS_WAL_SKIP = 0,
  TAOS_WAL_WRITE = 1,
  TAOS_WAL_FSYNC = 2,
} EWalType;

typedef struct {
  int32_t  vgId;
  int32_t  fsyncPeriod;      // millisecond
  int32_t  retentionPeriod;  // secs
  int32_t  rollPeriod;       // secs
  int64_t  retentionSize;
  int64_t  segSize;
  int64_t  committed;
  EWalType level;  // wal level
  int32_t      encryptAlgr;
  SEncryptData encryptData;
  int8_t   clearFiles;
  int8_t   enableTxnFile;  // write .txn files alongside .log (vnode WAL only)
  // Multi-mount-point write (level 0 only). Must be set here, BEFORE calling walOpen(),
  // not via a post-open walSetTfs() call: walOpen() itself performs a one-time repair
  // pass (walCheckAndRepairMeta/walCheckAndRepairIdx) over historical segments, and that
  // pass needs pTfs/relDir already bound so it can find segments that live on non-primary
  // disks -- otherwise it will treat them as missing and drop them from the WAL. NULL
  // pTfs keeps WAL behavior exactly as before this feature existed.
  STfs *pTfs;
  char  relDir[WAL_PATH_LEN];  // wal dir path relative to a tfs mount point root (e.g. "vnode/vnode2/wal")
} SWalCfg;

typedef struct {
  int64_t firstVer;
  int64_t verInSnapshotting;
  int64_t snapshotVer;
  int64_t commitVer;
  int64_t appliedVer;
  int64_t lastVer;
  int64_t logRetention;
} SWalVer;

#pragma pack(push, 1)
// used by sync module
typedef struct {
  int8_t   isWeek;
  uint64_t seqNum;
  uint64_t term;
} SWalSyncInfo;

typedef struct {
  int64_t version;
  int64_t ingestTs;
  int32_t bodyLen;
  int16_t msgType;
  // flags byte layout (bit-exact, do not use bit-fields for portability):
  //   bit7   : isTxn    -- 1 if this is a transaction WAL entry
  //   bit0-6 : protoVer -- WAL protocol version (0-127)
  uint8_t flags;
  // sync meta
  SWalSyncInfo syncMeta;

  char body[];
} SWalCont;

typedef struct {
  uint64_t magic;
  uint32_t cksumHead;
  uint32_t cksumBody;
  SWalCont head;
} SWalCkHead;
#pragma pack(pop)

// WAL txn body prefix layout (16 bytes total):
//   [uint64_t txnExtFlags : 8B][int64_t txnId : 8B][actual body : bodyLen-16]
//
// txnExtFlags bit definitions:
#define WAL_TXN_HDR_SIZE     16           // total txn body prefix size
#define WAL_TXN_EXT_IS_BEGIN (1ULL << 0)  // first WAL entry of this txn on this vnode

// WAL body helpers — always use these instead of accessing wCont->body / wCont->bodyLen directly.
// For txn entries (WAL_IS_TXN_MSG), the on-disk body is prefixed with [txnExtFlags:8B][txnId:8B].
// walContBody / walContBodyLen strip that prefix transparently so callers never need to know.
static FORCE_INLINE const char *walContBody(const SWalCont *cont) {
  if (WAL_IS_TXN_MSG(cont)) return (const char *)cont->body + WAL_TXN_HDR_SIZE;
  return (const char *)cont->body;
}
static FORCE_INLINE int32_t walContBodyLen(const SWalCont *cont) {
  if (WAL_IS_TXN_MSG(cont)) return cont->bodyLen - (int32_t)WAL_TXN_HDR_SIZE;
  return cont->bodyLen;
}
// Extract txnExtFlags from a txn WAL entry (returns 0 for non-txn entries).
static FORCE_INLINE uint64_t walContTxnExtFlags(const SWalCont *cont) {
  uint64_t flags = 0;
  if (WAL_IS_TXN_MSG(cont)) (void)memcpy(&flags, cont->body, sizeof(uint64_t));
  return flags;
}
// Extract txnId from a txn WAL entry (returns 0 for non-txn entries).
static FORCE_INLINE int64_t walContTxnId(const SWalCont *cont) {
  int64_t id = 0;
  if (WAL_IS_TXN_MSG(cont)) (void)memcpy(&id, cont->body + sizeof(uint64_t), sizeof(int64_t));
  return id;
}

typedef void (*stopDnodeFn)();
typedef struct SWal {
  // cfg
  SWalCfg cfg;
  int32_t fsyncSeq;
  // meta
  SWalVer   vers;
  TdFilePtr pLogFile;
  TdFilePtr pIdxFile;
  int32_t   writeCur;
  SArray   *fileInfoSet;  // SArray<SWalFileInfo>
  // gc
  SArray *toDeleteFiles;  // SArray<SWalFileInfo>
  // status
  int64_t totSize;
  int64_t lastRollSeq;
  // ctl
  int64_t        refId;
  TdThreadRwlock mutex;
  // ref
  SHashObj *pRefHash;  // refId -> SWalRef
  // keep version for preventing auto deletion
  int64_t keepVersion;
  // path
  char path[WAL_PATH_LEN];

  // tiered storage (multi-mount-point write at level 0)
  STfs *pTfs;               // dnode-level shared tfs handle; NULL = unbound/feature disabled
  char  relDir[WAL_PATH_LEN];  // wal dir path relative to a tfs mount point root (e.g. "vnode/vnode2/wal"),
                                // used to rebuild this vnode's wal directory on another disk. Must be unique
                                // per vnode -- callers must NOT pass a bare constant like "wal" here, or
                                // every vnode on the dnode would collide on the same directory/file names.

  stopDnodeFn stopDnode;

  // txn file support (vnode only, cfg.enableTxnFile == true)
  TdFilePtr        txnWriteFd;            // current .txn write file
  TdFilePtr        txnPendingFd;          // txn.pending singleton file
  volatile int64_t firstPendingTxnIndex;  // first txn walIndex since last fsync; -1 = none
  SHashObj        *txnBeginIndexMap;      // txnId -> int64_t firstWalIndex; tracks in-flight txns for IS_BEGIN

  // reusable write head — MUST be last: SWalCkHead contains a flexible array member
  SWalCkHead writeHead;
} SWal;

typedef struct {
  int64_t refId;
  int64_t refVer;
  SWal   *pWal;
} SWalRef;

// todo hide this struct
typedef struct SWalReader {
  SWal     *pWal;
  int64_t   readerId;
  TdFilePtr pLogFile;
  TdFilePtr pIdxFile;
  int64_t   curFileFirstVer;
  int64_t   curVersion;
  int64_t        capacity;
  TdThreadMutex  mutex;
  SWalCkHead    *pHead;
} SWalReader;

// module initialization
int32_t walInit(stopDnodeFn stopDnode);
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(const char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
int32_t walPersist(SWal *);
void    walClose(SWal *);
// Bind a dnode-level shared tfs handle to an already-opened WAL so that subsequent
// segment rolls may spread across the tfs level-0 mount points (see walRollImpl).
// Not calling this leaves pWal->pTfs == NULL and WAL behavior is unchanged.
int32_t walSetTfs(SWal *pWal, STfs *pTfs, const char *relDir);
int32_t walClearCorruption(SWal *, int64_t commitIndex);

// write interfaces
// By assigning index by the caller, wal gurantees linearizability
int32_t walAppendLog(SWal *, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body, int32_t bodyLen,
                     txn_id_t txnId, const STraceId *trace);
int32_t walFsync(SWal *, bool force);

// apis for lifecycle management
int32_t walCommit(SWal *, int64_t ver);
int32_t walRollback(SWal *, int64_t ver);
// notify that previous logs can be pruned safely
int32_t walBeginSnapshot(SWal *, int64_t ver, int64_t logRetention);
int32_t walEndSnapshot(SWal *, bool forceTrim);
int32_t walRestoreFromSnapshot(SWal *, int64_t ver);
void    walApplyVer(SWal *, int64_t ver);

// wal reader
SWalReader *walOpenReader(SWal *, int64_t id);
void        walCloseReader(SWalReader *pRead);
void        walReadReset(SWalReader *pReader);
int32_t     walReadVer(SWalReader *pRead, int64_t ver);
int32_t     walReaderSeekVer(SWalReader *pRead, int64_t ver);
int32_t     walNextValidMsg(SWalReader *pRead, bool scanMeta);
int64_t     walReaderGetCurrentVer(const SWalReader *pReader);
int64_t     walReaderGetValidFirstVer(const SWalReader *pReader);
int64_t     walReaderGetSkipToVersion(SWalReader *pReader);
void        walReaderSetSkipToVersion(SWalReader *pReader, int64_t ver);
void        walReaderValidVersionRange(SWalReader *pReader, int64_t *sver, int64_t *ever);
void        walReaderVerifyOffset(SWalReader *pWalReader, STqOffsetVal *pOffset);

// only for tq usage
int32_t walFetchHead(SWalReader *pRead, int64_t ver);
int32_t walFetchBody(SWalReader *pRead);
int32_t walSkipFetchBody(SWalReader *pRead);

SWalRef *walOpenRef(SWal *);
void     walCloseRef(SWal *pWal, int64_t refId);
void     walRefFirstVer(SWal *, SWalRef *);
void     walRefLastVer(SWal *, SWalRef *);

// helper function for raft
bool walLogExist(SWal *, int64_t ver);
bool walIsEmpty(SWal *);

// lifecycle check
int64_t walGetFirstVer(SWal *);
int64_t walGetSnapshotVer(SWal *);
int64_t walGetLastVer(SWal *);
int64_t walGetVerRetention(SWal *pWal, int64_t bytes);
int64_t walGetCommittedVer(SWal *);
int64_t walGetAppliedVer(SWal *);
int32_t walSetKeepVersion(SWal *pWal, int64_t ver);

// txn file API (vnode WAL only, no-op when cfg.enableTxnFile == false)
// Callback invoked for each entry returned by walTxnReadRange.
typedef int32_t (*FWalTxnEntryCb)(int64_t walIndex, tmsg_t msgType, txn_id_t txnId,
                                   const void *body, int32_t bodyLen, void *arg);
// Sequential read of [beginVer, endVer] from .txn files, invoking cb per entry.
// Used by txnMgrReloadFromWal (lazy load) instead of scanning main WAL.
int32_t walTxnReadRange(SWal *pWal, int64_t beginVer, int64_t endVer, FWalTxnEntryCb cb, void *arg);

// snapshot transfer of .txn files (leader -> follower)
// Wire format per block: [int64_t firstVer : 8B][int64_t fileSize : 8B][uint8_t data : fileSize]
typedef struct SWalTxnSnapReader SWalTxnSnapReader;
int32_t walSnapTxnReaderOpen(SWal *pWal, int64_t snapVer, SWalTxnSnapReader **ppReader);
int32_t walSnapTxnRead(SWalTxnSnapReader *pReader, uint8_t **ppData, uint32_t *pLen);
void    walSnapTxnReaderClose(SWalTxnSnapReader **ppReader);
int32_t walSnapTxnWrite(SWal *pWal, uint8_t *pData, uint32_t len);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
