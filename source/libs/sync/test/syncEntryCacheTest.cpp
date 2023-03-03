#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SSyncRaftEntry* createEntry(int i) {
  int32_t         dataLen = 20;
  SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
  assert(pEntry != NULL);
  pEntry->msgType = 88;
  pEntry->originalRpcType = 99;
  pEntry->seqNum = 3;
  pEntry->isWeak = true;
  pEntry->term = 100 + i;
  pEntry->index = i;
  snprintf(pEntry->data, dataLen, "value%d", i);

  return pEntry;
}

SSyncNode* createFakeNode() {
  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  ASSERT(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  return pSyncNode;
}

SRaftEntryCache* createCache(int maxCount) {
  SSyncNode* pSyncNode = createFakeNode();
  ASSERT(pSyncNode != NULL);

  SRaftEntryCache* pCache = raftEntryCacheCreate(pSyncNode, maxCount);
  ASSERT(pCache != NULL);

  return pCache;
}

void test1() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 10; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftEntryCachePutEntry(pCache, pEntry);
    sTrace("put entry code:%d, pEntry:%p", code, pEntry);
  }
  raftEntryCacheLog2((char*)"==test1 write 5 entries==", pCache);

  raftEntryCacheClear(pCache, 3);
  raftEntryCacheLog2((char*)"==test1 evict 3 entries==", pCache);

  raftEntryCacheClear(pCache, -1);
  raftEntryCacheLog2((char*)"==test1 evict -1(all) entries==", pCache);
}

void test2() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 10; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftEntryCachePutEntry(pCache, pEntry);
    sTrace("put entry code:%d, pEntry:%p", code, pEntry);
  }
  raftEntryCacheLog2((char*)"==test1 write 5 entries==", pCache);

  SyncIndex       index = 2;
  SSyncRaftEntry* pEntry = NULL;

  code = raftEntryCacheGetEntryP(pCache, index, &pEntry);
  ASSERT(code == 1 && index == pEntry->index);
  sTrace("get entry:%p for %" PRId64, pEntry, index);
  syncEntryLog2((char*)"==test2 get entry pointer 2==", pEntry);

  code = raftEntryCacheGetEntry(pCache, index, &pEntry);
  ASSERT(code == 1 && index == pEntry->index);
  sTrace("get entry:%p for %" PRId64, pEntry, index);
  syncEntryLog2((char*)"==test2 get entry 2==", pEntry);
  syncEntryDestory(pEntry);

  // not found
  index = 8;
  code = raftEntryCacheGetEntry(pCache, index, &pEntry);
  ASSERT(code == 0);
  sTrace("get entry:%p for %" PRId64, pEntry, index);
  sTrace("==test2 get entry 8 not found==");

  // not found
  index = 9;
  code = raftEntryCacheGetEntry(pCache, index, &pEntry);
  ASSERT(code == 0);
  sTrace("get entry:%p for %" PRId64, pEntry, index);
  sTrace("==test2 get entry 9 not found==");
}

void test3() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(20);
  for (int i = 0; i <= 4; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftEntryCachePutEntry(pCache, pEntry);
    sTrace("put entry code:%d, pEntry:%p", code, pEntry);
  }
  for (int i = 9; i >= 5; --i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftEntryCachePutEntry(pCache, pEntry);
    sTrace("put entry code:%d, pEntry:%p", code, pEntry);
  }
  raftEntryCacheLog2((char*)"==test3 write 10 entries==", pCache);
}

static void freeObj(void* param) {
  SSyncRaftEntry* pEntry = (SSyncRaftEntry*)param;
  syncEntryLog2((char*)"freeObj: ", pEntry);
  syncEntryDestory(pEntry);
}

void test4() {
  int32_t testRefId = taosOpenRef(200, freeObj);

  SSyncRaftEntry* pEntry = createEntry(10);
  ASSERT(pEntry != NULL);

  int64_t rid = taosAddRef(testRefId, pEntry);
  sTrace("rid: %" PRId64, rid);

  do {
    SSyncRaftEntry* pAcquireEntry = (SSyncRaftEntry*)taosAcquireRef(testRefId, rid);
    syncEntryLog2((char*)"acquire: ", pAcquireEntry);

    taosAcquireRef(testRefId, rid);
    taosAcquireRef(testRefId, rid);
    taosAcquireRef(testRefId, rid);

    // taosReleaseRef(testRefId, rid);
    // taosReleaseRef(testRefId, rid);
  } while (0);

  taosRemoveRef(testRefId, rid);

  for (int i = 0; i < 10; ++i) {
    sTrace("taosReleaseRef, %d", i);
    taosReleaseRef(testRefId, rid);
  }
}

void test5() {
  int32_t testRefId = taosOpenRef(5, freeObj);
  for (int i = 0; i < 100; i++) {
    SSyncRaftEntry* pEntry = createEntry(i);
    ASSERT(pEntry != NULL);

    int64_t rid = taosAddRef(testRefId, pEntry);
    sTrace("rid: %" PRId64, rid);
  }

  for (int64_t rid = 2; rid < 101; rid++) {
    SSyncRaftEntry* pAcquireEntry = (SSyncRaftEntry*)taosAcquireRef(testRefId, rid);
    syncEntryLog2((char*)"taosAcquireRef: ", pAcquireEntry);
  }
}

int main(int argc, char** argv) {
  gRaftDetailLog = true;
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE + DEBUG_DEBUG;

  /*
    test1();
    test2();
    test3();
  */
  test4();
  // test5();

  return 0;
}
