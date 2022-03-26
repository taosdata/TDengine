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

SyncPingReply *createMsg() {
  SRaftId srcId, destId;
  srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  srcId.vgId = 100;
  destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  destId.vgId = 100;
  SyncPingReply *pMsg = syncPingReplyBuild3(&srcId, &destId);
  return pMsg;
}

void test1() {
  SyncPingReply *pMsg = createMsg();
  syncPingReplyPrint2((char *)"test1:", pMsg);
  syncPingReplyDestroy(pMsg);
}

void test2() {
  SyncPingReply *pMsg = createMsg();
  uint32_t       len = pMsg->bytes;
  char *         serialized = (char *)taosMemoryMalloc(len);
  syncPingReplySerialize(pMsg, serialized, len);
  SyncPingReply *pMsg2 = syncPingReplyBuild(pMsg->dataLen);
  syncPingReplyDeserialize(serialized, len, pMsg2);
  syncPingReplyPrint2((char *)"test2: syncPingReplySerialize -> syncPingReplyDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test3() {
  SyncPingReply *pMsg = createMsg();
  uint32_t       len;
  char *         serialized = syncPingReplySerialize2(pMsg, &len);
  SyncPingReply *pMsg2 = syncPingReplyDeserialize2(serialized, len);
  syncPingReplyPrint2((char *)"test3: syncPingReplySerialize3 -> syncPingReplyDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test4() {
  SyncPingReply *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  SyncPingReply *pMsg2 = (SyncPingReply *)taosMemoryMalloc(rpcMsg.contLen);
  syncPingReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncPingReplyPrint2((char *)"test4: syncPingReply2RpcMsg -> syncPingReplyFromRpcMsg ", pMsg2);

  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test5() {
  SyncPingReply *pMsg = createMsg();
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  SyncPingReply *pMsg2 = syncPingReplyFromRpcMsg2(&rpcMsg);
  syncPingReplyPrint2((char *)"test5: syncPingReply2RpcMsg -> syncPingReplyFromRpcMsg2 ", pMsg2);

  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
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
