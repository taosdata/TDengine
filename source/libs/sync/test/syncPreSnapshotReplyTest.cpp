
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

SyncPreSnapshotReply *createMsg() {
  SyncPreSnapshotReply *pMsg = syncPreSnapshotReplyBuild(789);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 9527;
  pMsg->snapStart = 12306;
  return pMsg;
}

void test1() {
  SyncPreSnapshotReply *pMsg = createMsg();
  syncPreSnapshotReplyLog2((char *)"test1:", pMsg);
  syncPreSnapshotReplyDestroy(pMsg);
}

void test2() {
  SyncPreSnapshotReply *pMsg = createMsg();
  uint32_t              len = pMsg->bytes;
  char                 *serialized = (char *)taosMemoryMalloc(len);
  syncPreSnapshotReplySerialize(pMsg, serialized, len);
  SyncPreSnapshotReply *pMsg2 = syncPreSnapshotReplyBuild(789);
  syncPreSnapshotReplyDeserialize(serialized, len, pMsg2);
  syncPreSnapshotReplyLog2((char *)"test2: syncPreSnapshotReplySerialize -> syncPreSnapshotReplyDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncPreSnapshotReplyDestroy(pMsg);
  syncPreSnapshotReplyDestroy(pMsg2);
}

void test3() {
  SyncPreSnapshotReply *pMsg = createMsg();
  uint32_t              len;
  char                 *serialized = syncPreSnapshotReplySerialize2(pMsg, &len);
  SyncPreSnapshotReply *pMsg2 = syncPreSnapshotReplyDeserialize2(serialized, len);
  syncPreSnapshotReplyLog2((char *)"test3: syncPreSnapshotReplySerialize2 -> syncPreSnapshotReplyDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncPreSnapshotReplyDestroy(pMsg);
  syncPreSnapshotReplyDestroy(pMsg2);
}

void test4() {
  SyncPreSnapshotReply *pMsg = createMsg();
  SRpcMsg               rpcMsg;
  syncPreSnapshotReply2RpcMsg(pMsg, &rpcMsg);
  SyncPreSnapshotReply *pMsg2 = (SyncPreSnapshotReply *)taosMemoryMalloc(rpcMsg.contLen);
  syncPreSnapshotReplyFromRpcMsg(&rpcMsg, pMsg2);
  syncPreSnapshotReplyLog2((char *)"test4: syncPreSnapshotReply2RpcMsg -> syncPreSnapshotReplyFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPreSnapshotReplyDestroy(pMsg);
  syncPreSnapshotReplyDestroy(pMsg2);
}

void test5() {
  SyncPreSnapshotReply *pMsg = createMsg();
  SRpcMsg               rpcMsg;
  syncPreSnapshotReply2RpcMsg(pMsg, &rpcMsg);
  SyncPreSnapshotReply *pMsg2 = syncPreSnapshotReplyFromRpcMsg2(&rpcMsg);
  syncPreSnapshotReplyLog2((char *)"test5: syncPreSnapshotReply2RpcMsg -> syncPreSnapshotReplyFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncPreSnapshotReplyDestroy(pMsg);
  syncPreSnapshotReplyDestroy(pMsg2);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  gRaftDetailLog = true;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
