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

static int32_t mqHbRspHandle(SClientHbRsp* pReq) {
  return 0;
}

uint32_t hbKeyHashFunc(const char* key, uint32_t keyLen) {
  return 0;
}

static void hbMgrInitMqHbFunc() {
  clientHbMgr.handle[mq] = mqHbRspHandle;
}

int hbMgrInit() {
  //init once
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 0, 1);
  if (old == 1) return 0;

  //init config
  clientHbMgr.reportInterval = 1500;

  //init stat
  clientHbMgr.stats = 0;
  
  //init lock
  taosInitRWLatch(&clientHbMgr.lock);

  //init handle funcs
  hbMgrInitMqHbFunc();

  //init hash info
  clientHbMgr.activeInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  //init getInfoFunc
  clientHbMgr.getInfoFuncs = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  return 0;
}

void hbMgrCleanUp() {
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 1, 0);
  if (old == 0) return;

  taosHashCleanup(clientHbMgr.activeInfo);
  taosHashCleanup(clientHbMgr.getInfoFuncs);
}

int hbRegisterConn(SClientHbKey connKey, FGetConnInfo func) {
  
  return 0;
}

int hbAddConnInfo(SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen) {
  //lock

  //find req by connection id
  SClientHbReq* data = taosHashGet(clientHbMgr.activeInfo, &connKey, sizeof(SClientHbKey));
  ASSERT(data != NULL);
  taosHashPut(data->info, key, keyLen, value, valueLen);

  //unlock
  return 0;
}
