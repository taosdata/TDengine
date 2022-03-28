#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

void test1() {
  SSyncRaftEntry* pEntry = syncEntryBuild(10);
  assert(pEntry != NULL);
  pEntry->msgType = 1;
  pEntry->originalRpcType = 2;
  pEntry->seqNum = 3;
  pEntry->isWeak = true;
  pEntry->term = 100;
  pEntry->index = 200;
  strcpy(pEntry->data, "test1");

  syncEntryPrint(pEntry);
  syncEntryDestory(pEntry);
}

void test2() {
  SyncClientRequest* pSyncMsg = syncClientRequestBuild(10);
  pSyncMsg->originalRpcType = 33;
  pSyncMsg->seqNum = 11;
  pSyncMsg->isWeak = 1;
  strcpy(pSyncMsg->data, "test2");

  SSyncRaftEntry* pEntry = syncEntryBuild2(pSyncMsg, 100, 200);
  syncEntryPrint(pEntry);

  syncClientRequestDestroy(pSyncMsg);
  syncEntryDestory(pEntry);
}

void test3() {
  SyncClientRequest* pSyncMsg = syncClientRequestBuild(10);
  pSyncMsg->originalRpcType = 33;
  pSyncMsg->seqNum = 11;
  pSyncMsg->isWeak = 1;
  strcpy(pSyncMsg->data, "test3");

  SSyncRaftEntry* pEntry = syncEntryBuild3(pSyncMsg, 100, 200, SYNC_RAFT_ENTRY_NOOP);
  syncEntryPrint(pEntry);

  syncClientRequestDestroy(pSyncMsg);
  syncEntryDestory(pEntry);
}

void test4() {
  SSyncRaftEntry* pEntry = syncEntryBuild(10);
  assert(pEntry != NULL);
  pEntry->msgType = 11;
  pEntry->originalRpcType = 22;
  pEntry->seqNum = 33;
  pEntry->isWeak = true;
  pEntry->term = 44;
  pEntry->index = 55;
  pEntry->entryType = SYNC_RAFT_ENTRY_CONFIG;
  strcpy(pEntry->data, "test4");
  syncEntryPrint(pEntry);

  uint32_t len;
  char*    serialized = syncEntrySerialize(pEntry, &len);
  assert(serialized != NULL);
  SSyncRaftEntry* pEntry2 = syncEntryDeserialize(serialized, len);
  syncEntryPrint(pEntry2);

  taosMemoryFree(serialized);
  syncEntryDestory(pEntry2);
  syncEntryDestory(pEntry);
}

int main(int argc, char** argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  test1();
  test2();
  test3();
  test4();

  return 0;
}
