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

#include <gtest/gtest.h>
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

void init() {
  int code = walInit();
  TD_ALWAYS_ASSERT(code == 0);
}

void cleanup() { walCleanUp(); }

SWal* createWal(char* path, int32_t vgId) {
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal* pWal = walOpen(path, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);
  return pWal;
}

SSyncNode* createSyncNode(SWal* pWal) {
  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;
  return pSyncNode;
}

void usage(char* exe) { printf("usage: %s path vgId snapshotIndex \n", exe); }

int main(int argc, char** argv) {
  if (argc != 4) {
    usage(argv[0]);
    exit(-1);
  }
  char*   path = argv[1];
  int32_t vgId = atoi(argv[2]);
  int64_t snapshotIndex = atoll(argv[3]);

  init();
  SWal* pWal = createWal(path, vgId);
  TD_ALWAYS_ASSERT(pWal != NULL);
  SSyncNode* pSyncNode = createSyncNode(pWal);
  TD_ALWAYS_ASSERT(pSyncNode != NULL);

  SSyncLogStore* pLog = logStoreCreate(pSyncNode);
  TD_ALWAYS_ASSERT(pLog != NULL);

  int32_t code = pLog->syncLogRestoreFromSnapshot(pLog, snapshotIndex);
  TD_ALWAYS_ASSERT(code == 0);

  walClose(pWal);
  logStoreDestory(pLog);
  taosMemoryFree(pSyncNode);

  cleanup();
  return 0;
}
