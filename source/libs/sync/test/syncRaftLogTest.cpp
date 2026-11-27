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

#include "syncRaftLog.h"
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

const char *gWalPath = "./syncLogStoreTest_wal";

void init() { walInit(); }

void test1() {
  taosRemoveDir(gWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal *pWal = walOpen(gWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  int64_t firstVer = walGetFirstVer(pWal);
  int64_t lastVer = walGetLastVer(pWal);
  printf("firstVer:%" PRId64 " lastVer:%" PRId64 " \n", firstVer, lastVer);

  walClose(pWal);
}

void test2() {
  taosRemoveDir(gWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal *pWal = walOpen(gWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  SWalSyncInfo syncMeta = {
      .isWeek = -1,
      .seqNum = UINT64_MAX,
      .term = UINT64_MAX,
  };

  for (int i = 0; i < 5; ++i) {
    int code = walAppendLog(pWal, i, 100, syncMeta, "aa", 0, 3);
    if (code != 0) {
      printf("code:%d terror:%d msg:%s i:%d \n", code, terrno, tstrerror(terrno), i);
      TD_ALWAYS_ASSERT(0);
    }
  }

  int64_t firstVer = walGetFirstVer(pWal);
  int64_t lastVer = walGetLastVer(pWal);
  printf("firstVer:%" PRId64 " lastVer:%" PRId64 " \n", firstVer, lastVer);

  walClose(pWal);
}

void test3() {
  taosRemoveDir(gWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal *pWal = walOpen(gWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  walRestoreFromSnapshot(pWal, 5);

  int64_t firstVer = walGetFirstVer(pWal);
  int64_t lastVer = walGetLastVer(pWal);
  printf("firstVer:%" PRId64 " lastVer:%" PRId64 " \n", firstVer, lastVer);

  walClose(pWal);
}

void test4() {
  taosRemoveDir(gWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal *pWal = walOpen(gWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  SWalSyncInfo syncMeta = {
      .isWeek = -1,
      .seqNum = UINT64_MAX,
      .term = UINT64_MAX,
  };

  walRestoreFromSnapshot(pWal, 5);

  for (int i = 6; i < 10; ++i) {
    int code = walAppendLog(pWal, i, 100, syncMeta, "aa", 0, 3);
    if (code != 0) {
      printf("code:%d terror:%d msg:%s i:%d \n", code, terrno, tstrerror(terrno), i);
      TD_ALWAYS_ASSERT(0);
    }
  }

  int64_t firstVer = walGetFirstVer(pWal);
  int64_t lastVer = walGetLastVer(pWal);
  printf("firstVer:%" PRId64 " lastVer:%" PRId64 " \n", firstVer, lastVer);

  walClose(pWal);
}

void test5() {
  taosRemoveDir(gWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal *pWal = walOpen(gWalPath, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);

  walRestoreFromSnapshot(pWal, 5);
  walRestoreFromSnapshot(pWal, 7);

  int64_t firstVer = walGetFirstVer(pWal);
  int64_t lastVer = walGetLastVer(pWal);
  printf("firstVer:%" PRId64 " lastVer:%" PRId64 " \n", firstVer, lastVer);

  walClose(pWal);
}

void cleanup() { walCleanUp(); }

int main(int argc, char **argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  init();

  test1();
  test2();
  test3();
  test4();
  test5();

  cleanup();
  return 0;
}
