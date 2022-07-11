#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "tskiplist.h"

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

  SRaftEntryCache* pCache = raftCacheCreate(pSyncNode, maxCount);
  ASSERT(pCache != NULL);

  return pCache;
}

void test1() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 5; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftCachePutEntry(pCache, pEntry);
    ASSERT(code == 1);
    syncEntryDestory(pEntry);
  }
  raftCacheLog2((char*)"==test1 write 5 entries==", pCache);

  SyncIndex index;
  index = 1;
  code = raftCacheDelEntry(pCache, index);
  ASSERT(code == 0);
  index = 3;
  code = raftCacheDelEntry(pCache, index);
  ASSERT(code == 0);
  raftCacheLog2((char*)"==test1 delete 1,3==", pCache);

  code = raftCacheClear(pCache);
  ASSERT(code == 0);
  raftCacheLog2((char*)"==clear all==", pCache);
}

void test2() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 5; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftCachePutEntry(pCache, pEntry);
    ASSERT(code == 1);
    syncEntryDestory(pEntry);
  }
  raftCacheLog2((char*)"==test2 write 5 entries==", pCache);

  SyncIndex index;
  index = 1;
  SSyncRaftEntry* pEntry;
  code = raftCacheGetEntry(pCache, index, &pEntry);
  ASSERT(code == 0);
  syncEntryDestory(pEntry);
  syncEntryLog2((char*)"==test2 get entry 1==", pEntry);

  index = 2;
  code = raftCacheGetEntryP(pCache, index, &pEntry);
  ASSERT(code == 0);
  syncEntryLog2((char*)"==test2 get entry pointer 2==", pEntry);

  // not found
  index = 8;
  code = raftCacheGetEntry(pCache, index, &pEntry);
  ASSERT(code == -1 && terrno == TSDB_CODE_WAL_LOG_NOT_EXIST);
  sTrace("==test2 get entry 8 not found==");

  // not found
  index = 9;
  code = raftCacheGetEntryP(pCache, index, &pEntry);
  ASSERT(code == -1 && terrno == TSDB_CODE_WAL_LOG_NOT_EXIST);
  sTrace("==test2 get entry pointer 9 not found==");
}

void test3() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 5; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftCachePutEntry(pCache, pEntry);
    ASSERT(code == 1);
    syncEntryDestory(pEntry);
  }
  for (int i = 6; i < 10; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftCachePutEntry(pCache, pEntry);
    ASSERT(code == 0);
    syncEntryDestory(pEntry);
  }
  raftCacheLog2((char*)"==test3 write 10 entries, max count is 5==", pCache);
}

void test4() {
  int32_t          code = 0;
  SRaftEntryCache* pCache = createCache(5);
  for (int i = 0; i < 5; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    code = raftCachePutEntry(pCache, pEntry);
    ASSERT(code == 1);
    syncEntryDestory(pEntry);
  }
  raftCacheLog2((char*)"==test4 write 5 entries==", pCache);

  SyncIndex index;
  index = 3;
  SSyncRaftEntry* pEntry;
  code = raftCacheGetAndDel(pCache, index, &pEntry);
  ASSERT(code == 0);
  syncEntryLog2((char*)"==test4 get-and-del entry 3==", pEntry);
  raftCacheLog2((char*)"==test4 after get-and-del entry 3==", pCache);
}

static char* keyFn(const void* pData) {
  SSyncRaftEntry* pEntry = (SSyncRaftEntry*)pData;
  return (char*)(pEntry->index);
}

static int cmpFn(const void* p1, const void* p2) {
  SSyncRaftEntry* pEntry1 = (SSyncRaftEntry*)p1;
  SSyncRaftEntry* pEntry2 = (SSyncRaftEntry*)p2;

  if (pEntry1->index == pEntry2->index) {
    return 0;
  } else {
    return 1;
  }
}

void printSkipList(SSkipList* pSkipList) {
  ASSERT(pSkipList != NULL);

  SSkipListIterator* pIter = tSkipListCreateIter(pSkipList);
  while (tSkipListIterNext(pIter)) {
    SSkipListNode* pNode = tSkipListIterGet(pIter);
    ASSERT(pNode != NULL);
    SSyncRaftEntry* pEntry = (SSyncRaftEntry*)SL_GET_NODE_DATA(pNode);
    syncEntryPrint2((char*)"", pEntry);
  }
}

void test5() {
  SSkipList* pSkipList = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, sizeof(SSyncRaftEntry*), cmpFn,
                                         SL_DISCARD_DUP_KEY, keyFn);
  ASSERT(pSkipList != NULL);

  for (int i = 0; i <= 4; ++i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    SyncIndex       index = i;
    SSkipListNode*  pSkipListNode = tSkipListPut(pSkipList, pEntry);
  }

  for (int i = 9; i >= 5; --i) {
    SSyncRaftEntry* pEntry = createEntry(i);
    SyncIndex       index = i;
    SSkipListNode*  pSkipListNode = tSkipListPut(pSkipList, pEntry);
  }

  printSkipList(pSkipList);

  tSkipListDestroy(pSkipList);
}

int main(int argc, char** argv) {
  gRaftDetailLog = true;
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE + DEBUG_DEBUG;

  /*
    test1();
    test2();
    test3();
    test4();
  */

  test5();

  return 0;
}
