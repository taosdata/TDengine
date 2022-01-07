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

#define HEARTBEAT_INTERVAL 1500 //ms

typedef enum {
  HEARTBEAT_TYPE_MQ = 0,
  // types can be added here
  //
  HEARTBEAT_TYPE_MAX
} EHbType;

typedef int32_t (*FHbRspHandle)(SClientHbRsp* pReq);
typedef int32_t (*FGetConnInfo)(SClientHbKey connKey, void* param);

// called by mgmt
int  hbMgrInit();
void hbMgrCleanUp();
int  hbHandleRsp(SClientHbBatchRsp* hbRsp);

//called by user
int hbRegisterConn(SClientHbKey connKey, FGetConnInfo func);
void hbDeregisterConn(SClientHbKey connKey);

int hbAddConnInfo(SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen);

// mq
void hbMgrInitMqHbRspHandle();
