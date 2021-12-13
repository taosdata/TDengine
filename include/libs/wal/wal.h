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
#include "tdef.h"
#include "tlog.h"
#include "tarray.h"
#ifdef __cplusplus
extern "C" {
#endif

extern int32_t wDebugFlag;

#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", 255, __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", 255, __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN ", 255, __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL ", 255, __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}

typedef enum {
  TAOS_WAL_NOLOG = 0,
  TAOS_WAL_WRITE = 1,
  TAOS_WAL_FSYNC = 2
} EWalType;

typedef struct SWalReadHead {
  int8_t   sver;
  uint8_t  msgType;
  int8_t   reserved[2];
  int32_t  len;
  int64_t  version;
  char     cont[];
} SWalReadHead;

typedef struct {
  int32_t  vgId;
  int32_t  fsyncPeriod;      // millisecond
  int32_t  retentionPeriod;  // secs
  int32_t  rollPeriod;       // secs
  int32_t  retentionSize;       // secs
  int64_t  segSize;
  EWalType walLevel;         // wal level
} SWalCfg;

typedef struct {
  //union {
    //uint32_t info;
    //struct {
      //uint32_t sver:3;
      //uint32_t msgtype: 5;
      //uint32_t reserved : 24;
    //};
  //};
  uint32_t cksumHead;
  uint32_t cksumBody;
  SWalReadHead head;
} SWalHead;

#define WAL_PREFIX       "wal"
#define WAL_PREFIX_LEN   3
#define WAL_NOSUFFIX_LEN 20
#define WAL_SUFFIX_AT    (WAL_NOSUFFIX_LEN+1)
#define WAL_LOG_SUFFIX   "log"
#define WAL_INDEX_SUFFIX "idx"
#define WAL_REFRESH_MS   1000
#define WAL_MAX_SIZE     (TSDB_MAX_WAL_SIZE + sizeof(SWalHead) + 16)
#define WAL_SIGNATURE    ((uint32_t)(0xFAFBFDFEUL))
#define WAL_PATH_LEN     (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN     (WAL_PATH_LEN + 32)
//#define WAL_FILE_NUM     1 // 3
#define WAL_FILESET_MAX  128

#define WAL_IDX_ENTRY_SIZE    (sizeof(int64_t)*2)
#define WAL_CUR_POS_WRITABLE  1
#define WAL_CUR_FILE_WRITABLE 2
#define WAL_CUR_FAILED        4

typedef struct SWal {
  // cfg
  int32_t  vgId;
  int32_t  fsyncPeriod;  // millisecond
  int32_t  rollPeriod;  // second
  int64_t  segSize;
  int64_t  retentionSize;
  int32_t  retentionPeriod;
  EWalType level;
  //total size
  int64_t  totSize;
  //fsync seq
  int32_t  fsyncSeq;
  //reference
  int64_t refId;
  //write tfd
  int64_t writeLogTfd;
  int64_t writeIdxTfd;
  //wal lifecycle
  int64_t firstVersion;
  int64_t snapshotVersion;
  int64_t commitVersion;
  int64_t lastVersion;
  //snapshotting version
  int64_t snapshottingVer;
  //roll status
  int64_t lastRollSeq;
  //file set
  int32_t writeCur;
  SArray* fileInfoSet;
  //ctl
  int32_t curStatus;
  pthread_mutex_t mutex;
  //path
  char path[WAL_PATH_LEN];
  //reusable write head
  SWalHead head;
} SWal;  // WAL HANDLE

typedef int32_t (*FWalWrite)(void *ahandle, void *pHead);

// module initialization
int32_t walInit();
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(const char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
void    walClose(SWal *);

// write
int64_t walWrite(SWal *, int64_t index, uint8_t msgType, const void *body, int32_t bodyLen);
void    walFsync(SWal *, bool force);

// apis for lifecycle management
int32_t walCommit(SWal *, int64_t ver);
// truncate after
int32_t walRollback(SWal *, int64_t ver);
// notify that previous logs can be pruned safely
int32_t walBeginTakeSnapshot(SWal *, int64_t ver);
int32_t walEndTakeSnapshot(SWal *);
//int32_t  walDataCorrupted(SWal*);

// read
int32_t walRead(SWal *, SWalHead **, int64_t ver);
int32_t walReadWithFp(SWal *, FWalWrite writeFp, int64_t verStart, int32_t readNum);

// lifecycle check
int64_t walGetFirstVer(SWal *);
int64_t walGetSnapshotVer(SWal *);
int64_t walGetLastVer(SWal *);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
