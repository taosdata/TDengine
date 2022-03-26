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

#include "tqPush.h"

int32_t tqPushMgrInit() {
  //
  int8_t old = atomic_val_compare_exchange_8(&tqPushMgmt.inited, 0, 1);
  if (old == 1) return 0;

  tqPushMgmt.timer = taosTmrInit(0, 0, 0, "TQ");
  return 0;
}

void tqPushMgrCleanUp() {
  int8_t old = atomic_val_compare_exchange_8(&tqPushMgmt.inited, 1, 0);
  if (old == 0) return;
  taosTmrStop(tqPushMgmt.timer);
  taosTmrCleanUp(tqPushMgmt.timer);
}

STqPushMgr* tqPushMgrOpen() {
  STqPushMgr* mgr = taosMemoryMalloc(sizeof(STqPushMgr));
  if (mgr == NULL) {
    return NULL;
  }
  mgr->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  return mgr;
}

void tqPushMgrClose(STqPushMgr* pushMgr) {
  taosHashCleanup(pushMgr->pHash);
  taosMemoryFree(pushMgr);
}

STqClientPusher* tqAddClientPusher(STqPushMgr* pushMgr, SRpcMsg* pMsg, int64_t consumerId, int64_t ttl) {
  STqClientPusher* clientPusher = taosMemoryMalloc(sizeof(STqClientPusher));
  if (clientPusher == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  clientPusher->type = TQ_PUSHER_TYPE__CLIENT;
  clientPusher->pMsg = pMsg;
  clientPusher->consumerId = consumerId;
  clientPusher->ttl = ttl;
  if (taosHashPut(pushMgr->pHash, &consumerId, sizeof(int64_t), &clientPusher, sizeof(void*)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(clientPusher);
    // TODO send rsp back
    return NULL;
  }
  return clientPusher;
}

STqStreamPusher* tqAddStreamPusher(STqPushMgr* pushMgr, int64_t streamId, SEpSet* pEpSet) {
  STqStreamPusher* streamPusher = taosMemoryMalloc(sizeof(STqStreamPusher));
  if (streamPusher == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  streamPusher->type = TQ_PUSHER_TYPE__STREAM;
  streamPusher->nodeType = 0;
  streamPusher->streamId = streamId;
  /*memcpy(&streamPusher->epSet, pEpSet, sizeof(SEpSet));*/

  if (taosHashPut(pushMgr->pHash, &streamId, sizeof(int64_t), &streamPusher, sizeof(void*)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(streamPusher);
    return NULL;
  }
  return streamPusher;
}
