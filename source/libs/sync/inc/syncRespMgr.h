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

#ifndef _TD_LIBS_SYNC_RESP_MGR_H
#define _TD_LIBS_SYNC_RESP_MGR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

typedef struct SRespStub {
  SRpcMsg rpcMsg;
  int64_t createTime;
} SRespStub;

typedef struct SSyncRespMgr {
  SHashObj *    pRespHash;
  int64_t       ttl;
  void *        data;
  TdThreadMutex mutex;
  uint64_t      seqNum;
} SyncRespMgr;

SyncRespMgr *syncRespMgrCreate(void *data, int64_t ttl);
void         syncRespMgrDestroy(SyncRespMgr *pObj);
uint64_t     syncRespMgrAdd(SyncRespMgr *pObj, const SRespStub *pStub);
int32_t      syncRespMgrDel(SyncRespMgr *pObj, uint64_t seq);
int32_t      syncRespMgrGet(SyncRespMgr *pObj, uint64_t seq, SRespStub *pStub);
int32_t      syncRespMgrGetAndDel(SyncRespMgr *pObj, uint64_t seq, SRpcHandleInfo *pInfo);
void         syncRespClean(SyncRespMgr *pObj);
void         syncRespCleanRsp(SyncRespMgr *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RESP_MGR_H*/
