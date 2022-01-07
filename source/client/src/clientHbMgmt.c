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

#include "clientHb.h"

typedef struct SClientHbMgr {
  int8_t       inited;

  // statistics
  int32_t      reportCnt;
  int32_t      connKeyCnt;
  int64_t      reportBytes;  // not implemented
  int64_t      startTime;
  // thread
  pthread_t    thread;

  SHashObj*    activeInfo;    // hash<SClientHbKey, SClientHbReq>
  SHashObj*    getInfoFuncs;  // hash<SClientHbKey, FGetConnInfo>
  FHbRspHandle handle[HEARTBEAT_TYPE_MAX];
} SClientHbMgr;

static SClientHbMgr clientHbMgr = {0};

static int32_t hbCreateThread();
static void    hbStopThread();

static FORCE_INLINE void hbMgrInitHandle() {
  // init all handle
  hbMgrInitMqHbRspHandle();
}

static SClientHbBatchReq* hbGatherAllInfo() {
  SClientHbBatchReq* pReq = malloc(sizeof(SClientHbBatchReq));
  if(pReq == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }
  int32_t connKeyCnt = atomic_load_32(&clientHbMgr.connKeyCnt);
  pReq->reqs = taosArrayInit(connKeyCnt, sizeof(SClientHbReq));

  void *pIter = taosHashIterate(clientHbMgr.activeInfo, pIter);
  while (pIter != NULL) {
    taosArrayPush(pReq->reqs, pIter);
    SClientHbReq* pOneReq = pIter;
    taosHashClear(pOneReq->info);
    pIter = taosHashIterate(clientHbMgr.activeInfo, pIter);
  }

  return pReq;
}

static void* hbThreadFunc(void* param) {
  setThreadName("hb");
  while (1) {
    atomic_add_fetch_32(&clientHbMgr.reportCnt, 1);
    taosMsleep(HEARTBEAT_INTERVAL);
  }

  return NULL;
}

static int32_t hbCreateThread() {
  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pthread_create(&clientHbMgr.thread, &thAttr, hbThreadFunc, NULL) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  pthread_attr_destroy(&thAttr);
  return 0;
}

int hbMgrInit() {
  // init once
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 0, 1);
  if (old == 1) return 0;

  // init stat
  clientHbMgr.startTime = taosGetTimestampMs();

  // init handle funcs
  hbMgrInitHandle();

  // init hash info
  clientHbMgr.activeInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  clientHbMgr.activeInfo->freeFp = tFreeClientHbReq;
  // init getInfoFunc
  clientHbMgr.getInfoFuncs = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  // init backgroud thread
  return 0;
}

void hbMgrCleanUp() {
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 1, 0);
  if (old == 0) return;

  taosHashCleanup(clientHbMgr.activeInfo);
  taosHashCleanup(clientHbMgr.getInfoFuncs);
}

int hbHandleRsp(SClientHbBatchRsp* hbRsp) {
  int64_t reqId = hbRsp->reqId;
  int64_t rspId = hbRsp->rspId;

  SArray* rsps = hbRsp->rsps;
  int32_t sz = taosArrayGetSize(rsps);
  for (int i = 0; i < sz; i++) {
    SClientHbRsp* pRsp = taosArrayGet(rsps, i);
    if (pRsp->connKey.hbType < HEARTBEAT_TYPE_MAX) {
      clientHbMgr.handle[pRsp->connKey.hbType](pRsp);
    } else {
      // discard rsp
    }
  }
  return 0;
}

int hbRegisterConn(SClientHbKey connKey, FGetConnInfo func) {
  // init hash in activeinfo
  void* data = taosHashGet(clientHbMgr.activeInfo, &connKey, sizeof(SClientHbKey));
  if (data != NULL) {
    return 0;
  }
  SClientHbReq hbReq;
  hbReq.connKey = connKey;
  hbReq.info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  taosHashPut(clientHbMgr.activeInfo, &connKey, sizeof(SClientHbKey), &hbReq, sizeof(SClientHbReq));
  // init hash
  if (func != NULL) {
    taosHashPut(clientHbMgr.getInfoFuncs, &connKey, sizeof(SClientHbKey), func, sizeof(FGetConnInfo));
  }

  atomic_add_fetch_32(&clientHbMgr.connKeyCnt, 1);
  return 0;
}

void hbDeregisterConn(SClientHbKey connKey) {
  taosHashRemove(clientHbMgr.activeInfo, &connKey, sizeof(SClientHbKey));
  taosHashRemove(clientHbMgr.getInfoFuncs, &connKey, sizeof(SClientHbKey));
  atomic_sub_fetch_32(&clientHbMgr.connKeyCnt, 1);
}

int hbAddConnInfo(SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen) {
  // find req by connection id
  SClientHbReq* pReq = taosHashGet(clientHbMgr.activeInfo, &connKey, sizeof(SClientHbKey));
  ASSERT(pReq != NULL);

  taosHashPut(pReq->info, key, keyLen, value, valueLen);

  return 0;
}
