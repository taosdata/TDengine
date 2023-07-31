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

SyncHeartbeatReply *createMsg() {
  SyncHeartbeatReply *pMsg = syncHeartbeatReplyBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;

  pMsg->term = 33;
  pMsg->privateTerm = 44;
  pMsg->startTime = taosGetTimestampMs();
  return pMsg;
}

void test1() {
  SyncHeartbeatReply *pMsg = createMsg();
  syncHeartbeatReplyLog2((char *)"test1:", pMsg);
  syncHeartbeatReplyDestroy(pMsg);
}

void test2() {
  SyncHeartbeatReply *pMsg = createMsg();
  uint32_t            len = pMsg->bytes;
  char *              serialized = (char *)taosMemoryMalloc(len);
  syncHeartbeatReplySerialize(pMsg, serialized, len);
  SyncHeartbeatReply *pMsg2 = syncHeartbeatReplyBuild(1000);
  syncHeartbeatReplyDeserialize(serialized, len, pMsg2);
  syncHeartbeatReplyLog2((char *)"test2: syncHeartbeatReplySerialize -> syncHeartbeatReplyDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncHeartbeatReplyDestroy(pMsg);
  syncHeartbeatReplyDestroy(pMsg2);
}

void test3() {
  SyncHeartbeatReply *pMsg = createMsg();
  uint32_t            len;
  char *              serialized = syncHeartbeatReplySerialize2(pMsg, &len);
  SyncHeartbeatReply *pMsg2 = syncHeartbeatReplyDeserialize2(serialized, len);
  syncHeartbeatReplyLog2((char *)"test3: syncHeartbeatReplySerialize3 -> syncHeartbeatReplyDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncHeartbeatReplyDestroy(pMsg);
  syncHeartbeatReplyDestroy(pMsg2);
}

void test4() {
  SyncHeartbeatReply *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncHeartbeatReply2RpcMsg(pMsg, &rpcMsg);
  SyncHeartbeatReply *pMsg2 = syncHeartbeatReplyBuild(1000);
  syncHeartbeatReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncHeartbeatReplyLog2((char *)"test4: syncHeartbeatReply2RpcMsg -> syncHeartbeatReplyFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncHeartbeatReplyDestroy(pMsg);
  syncHeartbeatReplyDestroy(pMsg2);
}

void test5() {
  SyncHeartbeatReply *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncHeartbeatReply2RpcMsg(pMsg, &rpcMsg);
  SyncHeartbeatReply *pMsg2 = syncHeartbeatReplyFromRpcMsg2(&rpcMsg);
  syncHeartbeatReplyLog2((char *)"test5: syncHeartbeatReply2RpcMsg -> syncHeartbeatReplyFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncHeartbeatReplyDestroy(pMsg);
  syncHeartbeatReplyDestroy(pMsg2);
}

int main() {
  gRaftDetailLog = true;

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
