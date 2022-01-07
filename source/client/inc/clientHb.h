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

typedef enum {
  mq = 0,
  // type can be added here
  //
  HEARTBEAT_TYPE_MAX
} EHbType;

typedef int32_t (*FHbRspHandle)(SClientHbRsp* pReq);
typedef int32_t (*FGetConnInfo)(SClientHbKey connKey, void* param);

typedef struct SClientHbMgr {
  int8_t       inited;
  int32_t      reportInterval;  // unit ms
  int32_t      stats;
  SRWLatch     lock;
  SHashObj*    activeInfo;    // hash<SClientHbKey, SClientHbReq>
  SHashObj*    getInfoFuncs;  // hash<SClientHbKey, FGetConnInfo>
  FHbRspHandle handle[HEARTBEAT_TYPE_MAX];
  // input queue
} SClientHbMgr;

static SClientHbMgr clientHbMgr = {0};

int  hbMgrInit();
void hbMgrCleanUp();
int hbHandleRsp(void* hbMsg);


int hbRegisterConn(SClientHbKey connKey, FGetConnInfo func);


int hbAddConnInfo(SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen);

