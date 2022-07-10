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

#define WAL_PROTO_VER    0
#define WAL_NOSUFFIX_LEN 20
#define WAL_SUFFIX_AT    (WAL_NOSUFFIX_LEN + 1)
#define WAL_LOG_SUFFIX   "log"
#define WAL_INDEX_SUFFIX "idx"
#define WAL_REFRESH_MS   1000
#define WAL_MAX_SIZE     (TSDB_MAX_WAL_SIZE + sizeof(SWalCkHead))
#define WAL_PATH_LEN     (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN     (WAL_PATH_LEN + 32)
#define WAL_MAGIC        0xFAFBFCFDULL

typedef enum {
  TAOS_WAL_NOLOG = 0,
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
  int64_t lastVer;
} SWalVer;

#pragma pack(push, 1)
// used by sync module
typedef struct {
  int8_t   isWeek;
  uint64_t seqNum;
  uint64_t term;
} SSyncLogMeta;

typedef struct {
  int8_t  protoVer;
  int64_t version;
  int16_t msgType;
  int32_t bodyLen;
  int64_t ingestTs;  // not implemented

  // sync meta
  SSyncLogMeta syncMeta;

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
  TdFilePtr pWriteLogTFile;
  TdFilePtr pWriteIdxTFile;
  int32_t   writeCur;
  SArray   *fileInfoSet;  // SArray<SWalFileInfo>
  // status
  int64_t totSize;
  int64_t lastRollSeq;
  // ctl
  int64_t       refId;
  TdThreadMutex mutex;
  // ref
  SHashObj *pRefHash;  // ref -> SWalRef
  // path
  char path[WAL_PATH_LEN];
  // reusable write head
  SWalCkHead writeHead;
} SWal;  // WAL HANDLE

typedef struct {
  int8_t scanUncommited;
  int8_t scanMeta;
} SWalFilterCond;

typedef struct {
  SWal          *pWal;
  TdFilePtr      pLogFile;
  TdFilePtr      pIdxFile;
  int64_t        curFileFirstVer;
  int64_t        curVersion;
  int64_t        capacity;
  TdThreadMutex  mutex;
  SWalFilterCond cond;
  SWalCkHead    *pHead;
} SWalReader;

// module initialization
int32_t walInit();
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(const char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
void    walClose(SWal *);

// write
int32_t walWriteWithSyncInfo(SWal *, int64_t index, tmsg_t msgType, SSyncLogMeta syncMeta, const void *body,
                             int32_t bodyLen);
int32_t walWrite(SWal *, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen);
void    walFsync(SWal *, bool force);

// apis for lifecycle management
int32_t walCommit(SWal *, int64_t ver);
int32_t walRollback(SWal *, int64_t ver);
// notify that previous logs can be pruned safely
int32_t walBeginSnapshot(SWal *, int64_t ver);
int32_t walEndSnapshot(SWal *);
int32_t walRestoreFromSnapshot(SWal *, int64_t ver);
// int32_t  walDataCorrupted(SWal*);

// read
SWalReader *walOpenReader(SWal *, SWalFilterCond *pCond);
void        walCloseReader(SWalReader *pRead);
int32_t     walReadVer(SWalReader *pRead, int64_t ver);
int32_t     walReadSeekVer(SWalReader *pRead, int64_t ver);
int32_t     walNextValidMsg(SWalReader *pRead);

// only for tq usage
void    walSetReaderCapacity(SWalReader *pRead, int32_t capacity);
int32_t walFetchHead(SWalReader *pRead, int64_t ver, SWalCkHead *pHead);
int32_t walFetchBody(SWalReader *pRead, SWalCkHead **ppHead);
int32_t walSkipFetchBody(SWalReader *pRead, const SWalCkHead *pHead);

typedef struct {
  int64_t refId;
  int64_t ver;
} SWalRef;

SWalRef *walOpenRef(SWal *);
void     walCloseRef(SWalRef *);
int32_t  walRefVer(SWalRef *, int64_t ver);
int32_t  walUnrefVer(SWal *);

// help function for raft
bool walLogExist(SWal *, int64_t ver);
bool walIsEmpty(SWal *);

// lifecycle check
int64_t walGetFirstVer(SWal *);
int64_t walGetSnapshotVer(SWal *);
int64_t walGetLastVer(SWal *);
int64_t walGetCommittedVer(SWal *);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
