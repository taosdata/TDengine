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

SyncRequestVoteReply *createMsg() {
  SyncRequestVoteReply *pMsg = syncRequestVoteReplyBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 77;
  pMsg->voteGranted = true;
  return pMsg;
}

void test1() {
  SyncRequestVoteReply *pMsg = createMsg();
  syncRequestVoteReplyLog2((char *)"test1:", pMsg);
  syncRequestVoteReplyDestroy(pMsg);
}

void test2() {
  SyncRequestVoteReply *pMsg = createMsg();
  uint32_t              len = pMsg->bytes;
  char                 *serialized = (char *)taosMemoryMalloc(len);
  syncRequestVoteReplySerialize(pMsg, serialized, len);
  SyncRequestVoteReply *pMsg2 = syncRequestVoteReplyBuild(1000);
  syncRequestVoteReplyDeserialize(serialized, len, pMsg2);
  syncRequestVoteReplyLog2((char *)"test2: syncRequestVoteReplySerialize -> syncRequestVoteReplyDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteReplyDestroy(pMsg);
  syncRequestVoteReplyDestroy(pMsg2);
}

void test3() {
  SyncRequestVoteReply *pMsg = createMsg();
  uint32_t              len;
  char                 *serialized = syncRequestVoteReplySerialize2(pMsg, &len);
  SyncRequestVoteReply *pMsg2 = syncRequestVoteReplyDeserialize2(serialized, len);
  syncRequestVoteReplyLog2((char *)"test3: syncRequestVoteReplySerialize3 -> syncRequestVoteReplyDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteReplyDestroy(pMsg);
  syncRequestVoteReplyDestroy(pMsg2);
}

void test4() {
  SyncRequestVoteReply *pMsg = createMsg();
  SRpcMsg               rpcMsg;
  syncRequestVoteReply2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVoteReply *pMsg2 = syncRequestVoteReplyBuild(1000);
  syncRequestVoteReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncRequestVoteReplyLog2((char *)"test4: syncRequestVoteReply2RpcMsg -> syncRequestVoteReplyFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteReplyDestroy(pMsg);
  syncRequestVoteReplyDestroy(pMsg2);
}

void test5() {
  SyncRequestVoteReply *pMsg = createMsg();
  SRpcMsg               rpcMsg;
  syncRequestVoteReply2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVoteReply *pMsg2 = syncRequestVoteReplyFromRpcMsg2(&rpcMsg);
  syncRequestVoteReplyLog2((char *)"test5: syncRequestVoteReply2RpcMsg -> syncRequestVoteReplyFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteReplyDestroy(pMsg);
  syncRequestVoteReplyDestroy(pMsg2);
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
