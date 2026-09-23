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

SSyncNode*     pSyncNode;
SWal*          pWal;
SSyncLogStore* pLogStore;
const char*    pWalPath = "./syncLogStoreTest_wal";

void init() {
  walInit();
  taosRemoveDir(pWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  pWal = walOpen(pWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;
}

void cleanup() {
  walClose(pWal);
  walCleanUp();
  taosMemoryFree(pSyncNode);
}

void logStoreTest() {
  pLogStore = logStoreCreate(pSyncNode);
  TD_ALWAYS_ASSERT(pLogStore);
  TD_ALWAYS_ASSERT(pLogStore->syncLogLastIndex(pLogStore) == SYNC_INDEX_INVALID);

  logStoreLog2((char*)"logStoreTest", pLogStore);

  for (int i = 0; i < 5; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    TD_ALWAYS_ASSERT(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->syncLogLastIndex(pLogStore) + 1;
    snprintf(pEntry->data, dataLen, "value%d", i);

    syncEntryLog2((char*)"==write entry== :", pEntry);
    pLogStore->syncLogAppendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);

    if (i == 0) {
      TD_ALWAYS_ASSERT(pLogStore->syncLogLastIndex(pLogStore) == SYNC_INDEX_BEGIN);
    }
  }
  logStoreLog2((char*)"after appendEntry", pLogStore);
  pLogStore->syncLogTruncate(pLogStore, 3);
  logStoreLog2((char*)"after truncate 3", pLogStore);
  logStoreDestory(pLogStore);
}

int main(int argc, char** argv) {
  gRaftDetailLog = true;
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  init();
  logStoreTest();

  taosMsleep(2000);
  cleanup();

  return 0;
}
