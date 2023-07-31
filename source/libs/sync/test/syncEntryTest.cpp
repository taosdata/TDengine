#include "syncTest.h"

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
  SyncClientRequest* pSyncMsg = syncClientRequestAlloc(10);
  pSyncMsg->originalRpcType = 33;
  pSyncMsg->seqNum = 11;
  pSyncMsg->isWeak = 1;
  strcpy(pSyncMsg->data, "test2");

  SSyncRaftEntry* pEntry = syncEntryBuildFromClientRequest(pSyncMsg, 100, 200);
  syncEntryPrint(pEntry);

  taosMemoryFree(pSyncMsg);
  syncEntryDestory(pEntry);
}

void test3() {
  SyncClientRequest* pSyncMsg = syncClientRequestAlloc(10);
  pSyncMsg->originalRpcType = 33;
  pSyncMsg->seqNum = 11;
  pSyncMsg->isWeak = 1;
  strcpy(pSyncMsg->data, "test3");

  SSyncRaftEntry* pEntry = syncEntryBuildFromClientRequest(pSyncMsg, 100, 200);
  syncEntryPrint(pEntry);

  taosMemoryFree(pSyncMsg);
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
  strcpy(pEntry->data, "test4");
  syncEntryPrint(pEntry);

  // syncEntryDestory(pEntry2);
  syncEntryDestory(pEntry);
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  test1();
  test2();
  test3();
  test4();

  return 0;
}
