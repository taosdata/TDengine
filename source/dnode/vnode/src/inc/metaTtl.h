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

#ifndef _TD_VNODE_TTL_H_
#define _TD_VNODE_TTL_H_

#include "taosdef.h"
#include "thash.h"

#include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum DirtyEntryType {
  ENTRY_TYPE_DEL = 1,
  ENTRY_TYPE_UPSERT = 2,
} DirtyEntryType;

typedef struct STtlManger {
  TdThreadRwlock lock;

  TTB* pOldTtlIdx;       // btree<{deleteTime, tuid}, NULL>

  SHashObj* pTtlCache;   // key: tuid, value: {ttl, ctime}
  SHashObj* pDirtyUids;  // dirty tuid
  TTB*      pTtlIdx;     // btree<{deleteTime, tuid}, ttl>

  char* logPrefix;
} STtlManger;

typedef struct {
  int64_t ttlDays;
  int64_t changeTimeMs;
} STtlCacheEntry;

typedef struct {
  DirtyEntryType type;
} STtlDirtyEntry;

typedef struct {
  int64_t  deleteTimeSec;
  tb_uid_t uid;
} STtlIdxKey;

typedef struct {
  int64_t  deleteTimeMs;
  tb_uid_t uid;
} STtlIdxKeyV1;

typedef struct {
  int64_t ttlDays;
} STtlIdxValue;

typedef struct {
  tb_uid_t uid;
  int64_t  changeTimeMs;
} STtlUpdCtimeCtx;

typedef struct {
  tb_uid_t uid;
  int64_t  changeTimeMs;
  int64_t  ttlDays;
} STtlUpdTtlCtx;

typedef struct {
  tb_uid_t uid;
  TXN*     pTxn;
  int64_t  ttlDays;
} STtlDelTtlCtx;

int  ttlMgrOpen(STtlManger** ppTtlMgr, TDB* pEnv, int8_t rollback, const char* logPrefix);
void ttlMgrClose(STtlManger* pTtlMgr);
int  ttlMgrPostOpen(STtlManger* pTtlMgr, void* pMeta);

bool ttlMgrNeedUpgrade(TDB* pEnv);
int  ttlMgrUpgrade(STtlManger* pTtlMgr, void* pMeta);

int ttlMgrInsertTtl(STtlManger* pTtlMgr, const STtlUpdTtlCtx* pUpdCtx);
int ttlMgrDeleteTtl(STtlManger* pTtlMgr, const STtlDelTtlCtx* pDelCtx);
int ttlMgrUpdateChangeTime(STtlManger* pTtlMgr, const STtlUpdCtimeCtx* pUpdCtimeCtx);

int ttlMgrFlush(STtlManger* pTtlMgr, TXN* pTxn);
int ttlMgrFindExpired(STtlManger* pTtlMgr, int64_t timePointMs, SArray* pTbUids);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TTL_H_*/
