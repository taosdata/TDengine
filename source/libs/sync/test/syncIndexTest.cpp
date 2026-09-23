/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "syncTest.h"

void print(SHashObj *pNextIndex) {
  printf("----------------\n");
  uint64_t *p = (uint64_t *)taosHashIterate(pNextIndex, NULL);
  while (p) {
    size_t len;
    void  *key = taosHashGetKey(p, &len);

    SRaftId *pRaftId = (SRaftId *)key;

    printf("key:<%" PRIu64 ", %d>, value:%" PRIu64 " \n", pRaftId->addr, pRaftId->vgId, *p);
    p = (uint64_t *)taosHashIterate(pNextIndex, p);
  }
}

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  SRaftId me;
  SRaftId peer1;
  SRaftId peer2;

  me.addr = 0;
  me.vgId = 99;
  peer1.addr = 1;
  peer1.vgId = 99;
  peer2.addr = 2;
  peer2.vgId = 99;

  uint64_t  index;
  SHashObj *pNextIndex =
      taosHashInit(sizeof(SRaftId), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

  index = 1000;
  taosHashPut(pNextIndex, &me, sizeof(me), &index, sizeof(index));
  index = 1001;
  taosHashPut(pNextIndex, &peer1, sizeof(peer1), &index, sizeof(index));
  index = 1002;
  taosHashPut(pNextIndex, &peer2, sizeof(peer2), &index, sizeof(index));

  print(pNextIndex);

  SRaftId find;
  find = peer1;
  uint64_t *p;
  p = (uint64_t *)taosHashGet(pNextIndex, &find, sizeof(find));
  (*p) += 900;

  print(pNextIndex);

  taosHashCleanup(pNextIndex);

  return 0;
}
