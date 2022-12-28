#include "syncBatch.h"
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SSyncRaftEntry *createEntry(int i) {
  SSyncRaftEntry *pEntry = syncEntryBuild(20);
  assert(pEntry != NULL);
  pEntry->msgType = 1;
  pEntry->originalRpcType = 2;
  pEntry->seqNum = 3;
  pEntry->isWeak = true;
  pEntry->term = 100;
  pEntry->index = 200;
  snprintf(pEntry->data, pEntry->dataLen, "value_%d", i);
  return pEntry;
}

SyncAppendEntriesBatch *createMsg() {
  SSyncRaftEntry *entryPArr[5];
  memset(entryPArr, 0, sizeof(entryPArr));

  for (int32_t i = 0; i < 5; ++i) {
    SSyncRaftEntry *pEntry = createEntry(i);
    entryPArr[i] = pEntry;
  }

  SyncAppendEntriesBatch *pMsg = syncAppendEntriesBatchBuild(entryPArr, 5, 1234);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->prevLogIndex = 11;
  pMsg->prevLogTerm = 22;
  pMsg->commitIndex = 33;
  pMsg->privateTerm = 44;
  return pMsg;
}

void test1() {
  SyncAppendEntriesBatch *pMsg = createMsg();
  syncAppendEntriesBatchLog2((char *)"==test1==", pMsg);

  /*
    SOffsetAndContLen *metaArr = syncAppendEntriesBatchMetaTableArray(pMsg);
    int32_t            retArrSize = pMsg->dataCount;
    for (int i = 0; i < retArrSize; ++i) {
      SSyncRaftEntry *pEntry = (SSyncRaftEntry*)(pMsg->data + metaArr[i].offset);
      ASSERT(pEntry->bytes == metaArr[i].contLen);
      syncEntryPrint(pEntry);
    }
  */

  syncAppendEntriesBatchDestroy(pMsg);
}

/*
void test2() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len = pMsg->bytes;
  char *             serialized = (char *)taosMemoryMalloc(len);
  syncAppendEntriesSerialize(pMsg, serialized, len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesBuild(pMsg->dataLen, 1000);
  syncAppendEntriesDeserialize(serialized, len, pMsg2);
  syncAppendEntriesLog2((char *)"test2: syncAppendEntriesSerialize -> syncAppendEntriesDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test3() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len;
  char *             serialized = syncAppendEntriesSerialize2(pMsg, &len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesDeserialize2(serialized, len);
  syncAppendEntriesLog2((char *)"test3: syncAppendEntriesSerialize3 -> syncAppendEntriesDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test4() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = (SyncAppendEntries *)taosMemoryMalloc(rpcMsg.contLen);
  syncAppendEntriesFromRpcMsg(&rpcMsg, pMsg2);
  syncAppendEntriesLog2((char *)"test4: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test5() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = syncAppendEntriesFromRpcMsg2(&rpcMsg);
  syncAppendEntriesLog2((char *)"test5: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}
*/

int main() {
  gRaftDetailLog = true;
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_DEBUG + DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  test1();

  /*
   test2();
   test3();
   test4();
   test5();
 */

  return 0;
}
