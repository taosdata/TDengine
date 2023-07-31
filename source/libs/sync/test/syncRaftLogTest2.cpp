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

SyncIndex gSnapshotLastApplyIndex;
SyncIndex gSnapshotLastApplyTerm;

int32_t GetSnapshotCb(const struct SSyncFSM* pFsm, SSnapshot* pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = gSnapshotLastApplyIndex;
  pSnapshot->lastApplyTerm = gSnapshotLastApplyTerm;
  return 0;
}

bool gAssert = true;

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
  // pSyncNode->pFsm->FpGetSnapshotInfo = GetSnapshotCb;
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
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest1 ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 0);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 0);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest1 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 0);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 0);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

void test2() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  // pLogStore->syncLogSetBeginIndex(pLogStore, 5);
  pLogStore->syncLogRestoreFromSnapshot(pLogStore, 4);
  logStoreLog2((char*)"\n\n\ntest2 ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest2 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

void test3() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest3 ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 0);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 0);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

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

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 0);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 4);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 4);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 104);
  }

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest3 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 0);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 4);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 4);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 104);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

void test4() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest4 ----- ", pLogStore);
  // pLogStore->syncLogSetBeginIndex(pLogStore, 5);
  pLogStore->syncLogRestoreFromSnapshot(pLogStore, 4);

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

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 9);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 10);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 9);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 109);
  }

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest4 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 9);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 10);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 9);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 109);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

void test5() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest5 ----- ", pLogStore);
  // pLogStore->syncLogSetBeginIndex(pLogStore, 5);
  pLogStore->syncLogRestoreFromSnapshot(pLogStore, 4);

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

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 9);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 10);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 9);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 109);
  }

  pLogStore->syncLogTruncate(pLogStore, 7);
  logStoreLog2((char*)"after truncate 7", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 6);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 2);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 7);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 6);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 106);
  }

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest5 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 6);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 2);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 7);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 6);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 106);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

void test6() {
  taosRemoveDir(pWalPath);

  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;
  logStoreLog2((char*)"\n\n\ntest6 ----- ", pLogStore);
  // pLogStore->syncLogSetBeginIndex(pLogStore, 5);
  pLogStore->syncLogRestoreFromSnapshot(pLogStore, 4);

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

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == 9);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 5);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 10);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 0);
    assert(pLogStore->syncLogLastIndex(pLogStore) == 9);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 109);
  }

  pLogStore->syncLogTruncate(pLogStore, 5);
  logStoreLog2((char*)"after truncate 5", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  do {
    SyncIndex firstVer = walGetFirstVer(pWal);
    SyncIndex lastVer = walGetLastVer(pWal);
    bool      isEmpty = walIsEmpty(pWal);
    printf("before -------- firstVer:%" PRId64 " lastVer:%" PRId64 " isEmpty:%d \n", firstVer, lastVer, isEmpty);
  } while (0);

  logStoreDestory(pLogStore);
  cleanup();

  // restart
  init();
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  pSyncNode->pLogStore = pLogStore;

  do {
    SyncIndex firstVer = walGetFirstVer(pWal);
    SyncIndex lastVer = walGetLastVer(pWal);
    bool      isEmpty = walIsEmpty(pWal);
    printf("after -------- firstVer:%" PRId64 " lastVer:%" PRId64 " isEmpty:%d \n", firstVer, lastVer, isEmpty);
  } while (0);

  logStoreLog2((char*)"\n\n\ntest6 restart ----- ", pLogStore);

  if (gAssert) {
    assert(pLogStore->syncLogBeginIndex(pLogStore) == 5);
    assert(pLogStore->syncLogEndIndex(pLogStore) == -1);
    assert(pLogStore->syncLogEntryCount(pLogStore) == 0);
    assert(pLogStore->syncLogWriteIndex(pLogStore) == 5);
    assert(pLogStore->syncLogIsEmpty(pLogStore) == 1);
    assert(pLogStore->syncLogLastIndex(pLogStore) == -1);
    assert(pLogStore->syncLogLastTerm(pLogStore) == 0);
  }

  logStoreDestory(pLogStore);
  cleanup();
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_INFO + DEBUG_SCREEN + DEBUG_FILE;
  gRaftDetailLog = true;

  if (argc == 2) {
    gAssert = atoi(argv[1]);
  }
  sTrace("gAssert : %d", gAssert);

  /*
    test1();
    test2();
    test3();
    test4();
    test5();
  */
  test6();

  return 0;
}
