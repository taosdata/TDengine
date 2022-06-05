#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "wal.h"

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

SyncIndex gSnapshotLastApplyIndex;
SyncIndex gSnapshotLastApplyTerm;

int32_t GetSnapshotCb(struct SSyncFSM* pFsm, SSnapshot* pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = gSnapshotLastApplyIndex;
  pSnapshot->lastApplyTerm = gSnapshotLastApplyTerm;
  return 0;
}

void init() {
  walInit();
  
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
  assert(pWal != NULL);

  pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;

  pSyncNode->pFsm = (SSyncFSM*)taosMemoryMalloc(sizeof(SSyncFSM));
  pSyncNode->pFsm->FpGetSnapshot = GetSnapshotCb;
}

void cleanup() {
  walClose(pWal);
  walCleanUp();
  taosMemoryFree(pSyncNode);
}

void test1() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest1 ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest1 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

void test2() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pLogStore->syncLogSetBeginIndex(pLogStore, 5);
  logStoreLog2((char*)"\n\n\ntest2 ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest2 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

void test3() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest3 ----- ", pLogStore);

  for (int i = 0; i <= 4; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->syncLogWriteIndex(pLogStore);
    snprintf(pEntry->data, dataLen, "value%d", i);

    pLogStore->syncLogAppendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);
  }
  logStoreLog2((char*)"test3 after appendEntry", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest3 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

void test4() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest4 ----- ", pLogStore);
  pLogStore->syncLogSetBeginIndex(pLogStore, 5);

  for (int i = 5; i <= 9; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->syncLogWriteIndex(pLogStore);
    snprintf(pEntry->data, dataLen, "value%d", i);

    pLogStore->syncLogAppendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);
  }
  logStoreLog2((char*)"test4 after appendEntry", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest4 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

void test5() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest5 ----- ", pLogStore);
  pLogStore->syncLogSetBeginIndex(pLogStore, 5);

  for (int i = 5; i <= 9; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->syncLogWriteIndex(pLogStore);
    snprintf(pEntry->data, dataLen, "value%d", i);

    pLogStore->syncLogAppendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);
  }
  logStoreLog2((char*)"test5 after appendEntry", pLogStore);

  pLogStore->syncLogTruncate(pLogStore, 7);
  logStoreLog2((char*)"after truncate 7", pLogStore);

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest5 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

void test6() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest6 ----- ", pLogStore);
  pLogStore->syncLogSetBeginIndex(pLogStore, 5);

  for (int i = 5; i <= 9; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->syncLogWriteIndex(pLogStore);
    snprintf(pEntry->data, dataLen, "value%d", i);

    pLogStore->syncLogAppendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);
  }
  logStoreLog2((char*)"test6 after appendEntry", pLogStore);

  pLogStore->syncLogTruncate(pLogStore, 5);
  logStoreLog2((char*)"after truncate 5", pLogStore);

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  logStoreLog2((char*)"\n\n\ntest6 restart ----- ", pLogStore);
  logStoreDestory(pLogStore);
  cleanup();
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_INFO + DEBUG_SCREEN + DEBUG_FILE;

  test1();
  test2();
  test3();
  test4();
  test5();
  test6();

  return 0;
}
