#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SyncAppendEntriesReply *createMsg() {
  SyncAppendEntriesReply *pMsg = syncAppendEntriesReplyBuild();
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->success = true;
  pMsg->matchIndex = 77;
  return pMsg;
}

void test1() {
  SyncAppendEntriesReply *pMsg = createMsg();
  syncAppendEntriesReplyPrint2((char *)"test1:", pMsg);
  syncAppendEntriesReplyDestroy(pMsg);
}

void test2() {
  SyncAppendEntriesReply *pMsg = createMsg();
  uint32_t                len = pMsg->bytes;
  char *                  serialized = (char *)taosMemoryMalloc(len);
  syncAppendEntriesReplySerialize(pMsg, serialized, len);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyBuild();
  syncAppendEntriesReplyDeserialize(serialized, len, pMsg2);
  syncAppendEntriesReplyPrint2((char *)"test2: syncAppendEntriesReplySerialize -> syncAppendEntriesReplyDeserialize ",
                               pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test3() {
  SyncAppendEntriesReply *pMsg = createMsg();
  uint32_t                len;
  char *                  serialized = syncAppendEntriesReplySerialize2(pMsg, &len);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyDeserialize2(serialized, len);
  syncAppendEntriesReplyPrint2((char *)"test3: syncAppendEntriesReplySerialize3 -> syncAppendEntriesReplyDeserialize2 ",
                               pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test4() {
  SyncAppendEntriesReply *pMsg = createMsg();
  SRpcMsg                 rpcMsg;
  syncAppendEntriesReply2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyBuild();
  syncAppendEntriesReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncAppendEntriesReplyPrint2((char *)"test4: syncAppendEntriesReply2RpcMsg -> syncAppendEntriesReplyFromRpcMsg ",
                               pMsg2);

  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

void test5() {
  SyncAppendEntriesReply *pMsg = createMsg();
  SRpcMsg                 rpcMsg;
  syncAppendEntriesReply2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntriesReply *pMsg2 = syncAppendEntriesReplyFromRpcMsg2(&rpcMsg);
  syncAppendEntriesReplyPrint2((char *)"test5: syncAppendEntriesReply2RpcMsg -> syncAppendEntriesReplyFromRpcMsg2 ",
                               pMsg2);

  syncAppendEntriesReplyDestroy(pMsg);
  syncAppendEntriesReplyDestroy(pMsg2);
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
