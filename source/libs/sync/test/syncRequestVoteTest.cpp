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

SyncRequestVote *createMsg() {
  SyncRequestVote *pMsg = syncRequestVoteBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 11;
  pMsg->lastLogIndex = 22;
  pMsg->lastLogTerm = 33;
  return pMsg;
}

void test1() {
  SyncRequestVote *pMsg = createMsg();
  syncRequestVoteLog2((char *)"test1:", pMsg);
  syncRequestVoteDestroy(pMsg);
}

void test2() {
  SyncRequestVote *pMsg = createMsg();
  uint32_t         len = pMsg->bytes;
  char            *serialized = (char *)taosMemoryMalloc(len);
  syncRequestVoteSerialize(pMsg, serialized, len);
  SyncRequestVote *pMsg2 = syncRequestVoteBuild(1000);
  syncRequestVoteDeserialize(serialized, len, pMsg2);
  syncRequestVoteLog2((char *)"test2: syncRequestVoteSerialize -> syncRequestVoteDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test3() {
  SyncRequestVote *pMsg = createMsg();
  uint32_t         len;
  char            *serialized = syncRequestVoteSerialize2(pMsg, &len);
  SyncRequestVote *pMsg2 = syncRequestVoteDeserialize2(serialized, len);
  syncRequestVoteLog2((char *)"test3: syncRequestVoteSerialize3 -> syncRequestVoteDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test4() {
  SyncRequestVote *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVote *pMsg2 = syncRequestVoteBuild(1000);
  syncRequestVoteFromRpcMsg(&rpcMsg, pMsg2);
  syncRequestVoteLog2((char *)"test4: syncRequestVote2RpcMsg -> syncRequestVoteFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test5() {
  SyncRequestVote *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVote *pMsg2 = syncRequestVoteFromRpcMsg2(&rpcMsg);
  syncRequestVoteLog2((char *)"test5: syncRequestVote2RpcMsg -> syncRequestVoteFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
