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

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_WAL_NOLOG = 0,
  TAOS_WAL_WRITE = 1
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

typedef void *  twalh;  // WAL HANDLE
typedef int32_t FWalWrite(void *ahandle, void *pHead, int32_t qtype, void *pMsg);

//module initialization
int32_t  walInit();
void     walCleanUp();

//handle open and ctl
twalh    walOpen(char *path, SWalCfg *pCfg);
int32_t  walAlter(twalh, SWalCfg *pCfg);
void     walStop(twalh);
void     walClose(twalh);

//write
int32_t  walWrite(twalh, int8_t msgType, void* body, uint32_t bodyLen);
void     walWaitFsync(twalh, bool forceHint);
//int32_t  walCommit(twalh, uint64_t ver);
//int32_t  walRollback(twalh, uint64_t ver);

//read
int32_t  walRead(twalh, SWalHead **, int64_t ver);
int32_t  walReadWithFp(twalh, FWalWrite writeFp, int64_t verStart, int readNum);

//life cycle
int32_t  walDataPersisted(twalh, int64_t ver);
int32_t  walFirstVer(twalh);
int32_t  walLastVer(twal);
//int32_t  walDataCorrupted(twalh);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WAL_H_
