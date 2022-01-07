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

static int32_t mqHbRspHandle(SClientHbReq* pReq) {
  return 0;
}

int hbMgrInit() {
  //init once
  //
  //init lock
  //
  //init handle funcs
  clientHbMgr.handle[mq] = mqHbRspHandle;

  //init stat
  clientHbMgr.stats = 0;
  
  //init config
  clientHbMgr.reportInterval = 1500;

  //init hash info
  //
  return 0;
}

void hbMgrCleanUp() {

}

int registerConn(int32_t connId, FGetConnInfo func, FHbRspHandle rspHandle) {
  return 0;
}

int registerHbRspHandle(int32_t connId, int32_t hbType, FHbRspHandle rspHandle) {
  return 0;
}

int HbAddConnInfo(int32_t connId, void* key, void* value, int32_t keyLen, int32_t valueLen) {
  //lock

  //find req by connection id

  //unlock
  return 0;
}
