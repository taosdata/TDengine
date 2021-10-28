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

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_WAL_NOLOG = 0,
  TAOS_WAL_WRITE = 1,
  TAOS_WAL_FSYNC = 2
} EWalType;

typedef struct {
  int8_t   msgType;
  int8_t   sver;  // sver 2 for WAL SDataRow/SMemRow compatibility
  int8_t   reserved[2];
  int32_t  len;
  int64_t  version;
  uint32_t signature;
  uint32_t cksum;
  char     cont[];
} SWalHead;

typedef struct {
  int32_t  vgId;
  int32_t  fsyncPeriod;  // millisecond
  EWalType walLevel;     // wal level
} SWalCfg;

struct SWal;
typedef struct SWal SWal;  // WAL HANDLE
typedef int32_t (*FWalWrite)(void *ahandle, void *pHead, int32_t qtype, void *pMsg);

// module initialization
int32_t walInit();
void    walCleanUp();

// handle open and ctl
SWal   *walOpen(char *path, SWalCfg *pCfg);
int32_t walAlter(SWal *, SWalCfg *pCfg);
void    walClose(SWal *);

// write
// int64_t  walWriteWithMsgType(SWal*, int8_t msgType, void* body, int32_t bodyLen);
int64_t walWrite(SWal *, int64_t index, void *body, int32_t bodyLen);
int64_t walWriteBatch(SWal *, void **bodies, int32_t *bodyLen, int32_t batchSize);

// apis for lifecycle management
void    walFsync(SWal *, bool force);
int32_t walCommit(SWal *, int64_t ver);
// truncate after
int32_t walRollback(SWal *, int64_t ver);
// notify that previous log can be pruned safely
int32_t walPrune(SWal *, int64_t ver);

// read
int32_t walRead(SWal *, SWalHead **, int64_t ver);
int32_t walReadWithFp(SWal *, FWalWrite writeFp, int64_t verStart, int32_t readNum);

// lifecycle check
int32_t walFirstVer(SWal *);
int32_t walPersistedVer(SWal *);
int32_t walLastVer(SWal *);
// int32_t  walDataCorrupted(SWal*);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
