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
#include "tlog.h"
#include "tmsg.h"
#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN ",  DEBUG_WARN, 255,         __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL ",       DEBUG_INFO, 255,         __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL ",       DEBUG_DEBUG, wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL ",       DEBUG_TRACE, wDebugFlag, __VA_ARGS__); }}
// clang-format on

#define WAL_PROTO_VER     0
#define WAL_NOSUFFIX_LEN  20
#define WAL_SUFFIX_AT     (WAL_NOSUFFIX_LEN + 1)
#define WAL_LOG_SUFFIX    "log"
#define WAL_INDEX_SUFFIX  "idx"
#define WAL_REFRESH_MS    1000
#define WAL_PATH_LEN      (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN      (WAL_PATH_LEN + 32)
#define WAL_MAGIC         0xFAFBFCFDF4F3F2F1ULL
#define WAL_SCAN_BUF_SIZE (1024 * 1024 * 3)

typedef enum {
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
  EWalType level;  // wal level
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
  int8_t  protoVer;

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
  int64_t       refId;
  TdThreadMutex mutex;
  // ref
  SHashObj *pRefHash;  // refId -> SWalRef
  // path
  char path[WAL_PATH_LEN];
  // reusable write head
  SWalCkHead writeHead;
} SWal;

typedef struct {
  int64_t refId;
  int64_t refVer;
  //  int64_t refFile;
  SWal *pWal;
} SWalRef;

typedef struct {
  int8_t scanUncommited;
  int8_t scanNotApplied;
  int8_t scanMeta;
  int8_t deleteMsg;
  int8_t enableRef;
} SWalFilterCond;

typedef struct SWalReader SWalReader;

// todo hide this struct
struct SWalReader {
  SWal          *pWal;
  int64_t        readerId;
  TdFilePtr      pLogFile;
  TdFilePtr      pIdxFile;
  int64_t        curFileFirstVer;
  int64_t        curVersion;
  int64_t        skipToVersion; // skip data and jump to destination version, usually used by stream resume ignoring untreated data
  int64_t        capacity;
  TdThreadMutex  mutex;
  SWalFilterCond cond;
  SWalCkHead *pHead;
};

// module initialization
int32_t walInit();
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(const char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
int32_t walPersist(SWal *);
void    walClose(SWal *);

// write interfaces

// By assigning index by the caller, wal gurantees linearizability
int32_t walWrite(SWal *, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen);
int32_t walWriteWithSyncInfo(SWal *, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body,
                             int32_t bodyLen);

// Assign version automatically and return to caller,
// -1 will be returned for failed writes
int64_t walAppendLog(SWal *, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body, int32_t bodyLen);

void walFsync(SWal *, bool force);

// apis for lifecycle management
int32_t walCommit(SWal *, int64_t ver);
int32_t walRollback(SWal *, int64_t ver);
// notify that previous logs can be pruned safely
int32_t walBeginSnapshot(SWal *, int64_t ver, int64_t logRetention);
int32_t walEndSnapshot(SWal *);
int32_t walRestoreFromSnapshot(SWal *, int64_t ver);
// for tq
int32_t walApplyVer(SWal *, int64_t ver);

// int32_t  walDataCorrupted(SWal*);

// wal reader
SWalReader *walOpenReader(SWal *, SWalFilterCond *pCond, int64_t id);
void        walCloseReader(SWalReader *pRead);
void        walReadReset(SWalReader *pReader);
int32_t     walReadVer(SWalReader *pRead, int64_t ver);
int32_t     walReaderSeekVer(SWalReader *pRead, int64_t ver);
int32_t     walNextValidMsg(SWalReader *pRead);
int64_t     walReaderGetCurrentVer(const SWalReader *pReader);
int64_t     walReaderGetValidFirstVer(const SWalReader *pReader);
int64_t     walReaderGetSkipToVersion(SWalReader *pReader);
void        walReaderSetSkipToVersion(SWalReader *pReader, int64_t ver);
void        walReaderValidVersionRange(SWalReader *pReader, int64_t *sver, int64_t *ever);
void        walReaderVerifyOffset(SWalReader *pWalReader, STqOffsetVal* pOffset);

// only for tq usage
int32_t walFetchHead(SWalReader *pRead, int64_t ver);
int32_t walFetchBody(SWalReader *pRead);
int32_t walSkipFetchBody(SWalReader *pRead);

void walRefFirstVer(SWal *, SWalRef *);
void walRefLastVer(SWal *, SWalRef *);

SWalRef *walOpenRef(SWal *);
void     walCloseRef(SWal *pWal, int64_t refId);
int32_t  walSetRefVer(SWalRef *, int64_t ver);

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

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
