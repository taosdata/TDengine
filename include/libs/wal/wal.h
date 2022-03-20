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

#define wFatal(...)                                              \
  {                                                              \
    if (wDebugFlag & DEBUG_FATAL) {                              \
      taosPrintLog("WAL FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); \
    }                                                            \
  }
#define wError(...)                                              \
  {                                                              \
    if (wDebugFlag & DEBUG_ERROR) {                              \
      taosPrintLog("WAL ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); \
    }                                                            \
  }
#define wWarn(...)                                             \
  {                                                            \
    if (wDebugFlag & DEBUG_WARN) {                             \
      taosPrintLog("WAL WARN ", DEBUG_WARN, 255, __VA_ARGS__); \
    }                                                          \
  }
#define wInfo(...)                                        \
  {                                                       \
    if (wDebugFlag & DEBUG_INFO) {                        \
      taosPrintLog("WAL ", DEBUG_INFO, 255, __VA_ARGS__); \
    }                                                     \
  }
#define wDebug(...)                                               \
  {                                                               \
    if (wDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("WAL ", DEBUG_DEBUG, wDebugFlag, __VA_ARGS__); \
    }                                                             \
  }
#define wTrace(...)                                               \
  {                                                               \
    if (wDebugFlag & DEBUG_TRACE) {                               \
      taosPrintLog("WAL ", DEBUG_TRACE, wDebugFlag, __VA_ARGS__); \
    }                                                             \
  }

#define WAL_HEAD_VER 0
#define WAL_NOSUFFIX_LEN 20
#define WAL_SUFFIX_AT (WAL_NOSUFFIX_LEN + 1)
#define WAL_LOG_SUFFIX "log"
#define WAL_INDEX_SUFFIX "idx"
#define WAL_REFRESH_MS 1000
#define WAL_MAX_SIZE (TSDB_MAX_WAL_SIZE + sizeof(SWalHead))
#define WAL_PATH_LEN (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN (WAL_PATH_LEN + 32)
#define WAL_MAGIC 0xFAFBFCFDULL

#define WAL_CUR_FAILED 1

#pragma pack(push, 1)
typedef enum { TAOS_WAL_NOLOG = 0, TAOS_WAL_WRITE = 1, TAOS_WAL_FSYNC = 2 } EWalType;

typedef struct SWalReadHead {
  int8_t  headVer;
  int16_t msgType;
  int8_t  reserved;
  int32_t len;
  int64_t ingestTs;  // not implemented
  int64_t version;
  char    body[];
} SWalReadHead;

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
  uint64_t     magic;
  uint32_t     cksumHead;
  uint32_t     cksumBody;
  SWalReadHead head;
} SWalHead;

typedef struct SWalVer {
  int64_t firstVer;
  int64_t verInSnapshotting;
  int64_t snapshotVer;
  int64_t commitVer;
  int64_t lastVer;
} SWalVer;

typedef struct SWal {
  // cfg
  SWalCfg cfg;
  int32_t fsyncSeq;
  // meta
  SWalVer vers;
  TdFilePtr pWriteLogTFile;
  TdFilePtr pWriteIdxTFile;
  int32_t writeCur;
  SArray *fileInfoSet;
  // status
  int64_t totSize;
  int64_t lastRollSeq;
  // ctl
  int64_t         refId;
  TdThreadMutex mutex;
  // path
  char path[WAL_PATH_LEN];
  // reusable write head
  SWalHead writeHead;
} SWal;  // WAL HANDLE

typedef struct SWalReadHandle {
  SWal     *pWal;
  TdFilePtr pReadLogTFile;
  TdFilePtr pReadIdxTFile;
  int64_t   curFileFirstVer;
  int64_t   curVersion;
  int64_t   capacity;
  int64_t   status;  // if cursor valid
  SWalHead *pHead;
} SWalReadHandle;
#pragma pack(pop)

// typedef int32_t (*FWalWrite)(void *ahandle, void *pHead);

// module initialization
int32_t walInit();
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(const char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
void    walClose(SWal *);

// write
int64_t walWrite(SWal *, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen);
void    walFsync(SWal *, bool force);

// apis for lifecycle management
int32_t walCommit(SWal *, int64_t ver);
// truncate after
int32_t walRollback(SWal *, int64_t ver);
// notify that previous logs can be pruned safely
int32_t walBeginSnapshot(SWal *, int64_t ver);
int32_t walEndSnapshot(SWal *);
// int32_t  walDataCorrupted(SWal*);

// read
SWalReadHandle *walOpenReadHandle(SWal *);
void            walCloseReadHandle(SWalReadHandle *);
int32_t         walReadWithHandle(SWalReadHandle *pRead, int64_t ver);

// deprecated
#if 0
int32_t walRead(SWal *, SWalHead **, int64_t ver);
int32_t walReadWithFp(SWal *, FWalWrite writeFp, int64_t verStart, int32_t readNum);
#endif

// lifecycle check
int64_t walGetFirstVer(SWal *);
int64_t walGetSnapshotVer(SWal *);
int64_t walGetLastVer(SWal *);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
