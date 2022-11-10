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

SyncSnapshotRsp *createMsg() {
  SyncSnapshotRsp *pMsg = syncSnapshotRspBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 11;
  pMsg->startTime = 99;
  pMsg->lastIndex = 22;
  pMsg->lastTerm = 33;
  pMsg->ack = 44;
  pMsg->code = 55;
  return pMsg;
}

void test1() {
  SyncSnapshotRsp *pMsg = createMsg();
  syncSnapshotRspLog2((char *)"test1:", pMsg);
  syncSnapshotRspDestroy(pMsg);
}

void test2() {
  SyncSnapshotRsp *pMsg = createMsg();
  uint32_t         len = pMsg->bytes;
  char            *serialized = (char *)taosMemoryMalloc(len);
  syncSnapshotRspSerialize(pMsg, serialized, len);
  SyncSnapshotRsp *pMsg2 = syncSnapshotRspBuild(1000);
  syncSnapshotRspDeserialize(serialized, len, pMsg2);
  syncSnapshotRspLog2((char *)"test2: syncSnapshotRspSerialize -> syncSnapshotRspDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncSnapshotRspDestroy(pMsg);
  syncSnapshotRspDestroy(pMsg2);
}

void test3() {
  SyncSnapshotRsp *pMsg = createMsg();
  uint32_t         len;
  char            *serialized = syncSnapshotRspSerialize2(pMsg, &len);
  SyncSnapshotRsp *pMsg2 = syncSnapshotRspDeserialize2(serialized, len);
  syncSnapshotRspLog2((char *)"test3: syncSnapshotRspSerialize2 -> syncSnapshotRspDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncSnapshotRspDestroy(pMsg);
  syncSnapshotRspDestroy(pMsg2);
}

void test4() {
  SyncSnapshotRsp *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncSnapshotRsp2RpcMsg(pMsg, &rpcMsg);
  SyncSnapshotRsp *pMsg2 = (SyncSnapshotRsp *)taosMemoryMalloc(rpcMsg.contLen);
  syncSnapshotRspFromRpcMsg(&rpcMsg, pMsg2);
  syncSnapshotRspLog2((char *)"test4: syncSnapshotRsp2RpcMsg -> syncSnapshotRspFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncSnapshotRspDestroy(pMsg);
  syncSnapshotRspDestroy(pMsg2);
}

void test5() {
  SyncSnapshotRsp *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncSnapshotRsp2RpcMsg(pMsg, &rpcMsg);
  SyncSnapshotRsp *pMsg2 = syncSnapshotRspFromRpcMsg2(&rpcMsg);
  syncSnapshotRspLog2((char *)"test5: syncSnapshotRsp2RpcMsg -> syncSnapshotRspFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncSnapshotRspDestroy(pMsg);
  syncSnapshotRspDestroy(pMsg2);
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
