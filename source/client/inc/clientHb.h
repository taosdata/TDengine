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

#include "os.h"
#include "tarray.h"
#include "thash.h"
#include "tmsg.h"

#define HEARTBEAT_INTERVAL 1500  // ms

typedef enum {
  HEARTBEAT_TYPE_MQ = 0,
  // types can be added here
  //
  HEARTBEAT_TYPE_MAX
} EHbType;

typedef int32_t (*FHbRspHandle)(SClientHbRsp* pReq);

typedef struct SAppHbMgr {
  // statistics
  int32_t reportCnt;
  int32_t connKeyCnt;
  int64_t reportBytes;  // not implemented
  int64_t startTime;
  // ctl
  SRWLatch lock;  // lock is used in serialization
  // connection
  void*  transporter;
  SEpSet epSet;
  // info
  SHashObj* activeInfo;    // hash<SClientHbKey, SClientHbReq>
  SHashObj* getInfoFuncs;  // hash<SClientHbKey, FGetConnInfo>
} SAppHbMgr;

typedef struct SClientHbMgr {
  int8_t inited;
  // ctl
  int8_t          threadStop;
  pthread_t       thread;
  pthread_mutex_t lock;       // used when app init and cleanup
  SArray*         appHbMgrs;  // SArray<SAppHbMgr*> one for each cluster
  FHbRspHandle    handle[HEARTBEAT_TYPE_MAX];
} SClientHbMgr;

// TODO: embed param into function
// return type: SArray<Skv>
typedef SArray* (*FGetConnInfo)(SClientHbKey connKey, void* param);

// global, called by mgmt
int  hbMgrInit();
void hbMgrCleanUp();
int  hbHandleRsp(SClientHbBatchRsp* hbRsp);

// cluster level
SAppHbMgr* appHbMgrInit(void* transporter, SEpSet epSet);
void appHbMgrCleanup(SAppHbMgr* pAppHbMgr);

// conn level
int  hbRegisterConn(SAppHbMgr* pAppHbMgr, SClientHbKey connKey, FGetConnInfo func);
void hbDeregisterConn(SAppHbMgr* pAppHbMgr, SClientHbKey connKey);

int hbAddConnInfo(SAppHbMgr* pAppHbMgr, SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen);

// mq
void hbMgrInitMqHbRspHandle();
