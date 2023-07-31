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

SyncSnapshotSend *createMsg() {
  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(20, 1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 11;
  pMsg->lastIndex = 22;
  pMsg->lastTerm = 33;

  pMsg->lastConfigIndex = 99;
  pMsg->lastConfig.replicaNum = 3;
  pMsg->lastConfig.myIndex = 1;
  for (int i = 0; i < pMsg->lastConfig.replicaNum; ++i) {
    ((pMsg->lastConfig.nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pMsg->lastConfig.nodeInfo)[i]).nodeFqdn, sizeof(((pMsg->lastConfig.nodeInfo)[i]).nodeFqdn),
             "100.200.300.%d", i);
  }

  pMsg->seq = 44;
  strcpy(pMsg->data, "hello world");
  return pMsg;
}

void test1() {
  SyncSnapshotSend *pMsg = createMsg();
  syncSnapshotSendLog2((char *)"test1:", pMsg);
  syncSnapshotSendDestroy(pMsg);
}

void test2() {
  SyncSnapshotSend *pMsg = createMsg();
  uint32_t          len = pMsg->bytes;
  char             *serialized = (char *)taosMemoryMalloc(len);
  syncSnapshotSendSerialize(pMsg, serialized, len);
  SyncSnapshotSend *pMsg2 = syncSnapshotSendBuild(pMsg->dataLen, 1000);
  syncSnapshotSendDeserialize(serialized, len, pMsg2);
  syncSnapshotSendLog2((char *)"test2: syncSnapshotSendSerialize -> syncSnapshotSendDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncSnapshotSendDestroy(pMsg);
  syncSnapshotSendDestroy(pMsg2);
}

void test3() {
  SyncSnapshotSend *pMsg = createMsg();
  uint32_t          len;
  char             *serialized = syncSnapshotSendSerialize2(pMsg, &len);
  SyncSnapshotSend *pMsg2 = syncSnapshotSendDeserialize2(serialized, len);
  syncSnapshotSendLog2((char *)"test3: syncSnapshotSendSerialize2 -> syncSnapshotSendDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncSnapshotSendDestroy(pMsg);
  syncSnapshotSendDestroy(pMsg2);
}

void test4() {
  SyncSnapshotSend *pMsg = createMsg();
  SRpcMsg           rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  SyncSnapshotSend *pMsg2 = (SyncSnapshotSend *)taosMemoryMalloc(rpcMsg.contLen);
  syncSnapshotSendFromRpcMsg(&rpcMsg, pMsg2);
  syncSnapshotSendLog2((char *)"test4: syncSnapshotSend2RpcMsg -> syncSnapshotSendFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncSnapshotSendDestroy(pMsg);
  syncSnapshotSendDestroy(pMsg2);
}

void test5() {
  SyncSnapshotSend *pMsg = createMsg();
  SRpcMsg           rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  SyncSnapshotSend *pMsg2 = syncSnapshotSendFromRpcMsg2(&rpcMsg);
  syncSnapshotSendLog2((char *)"test5: syncSnapshotSend2RpcMsg -> syncSnapshotSendFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncSnapshotSendDestroy(pMsg);
  syncSnapshotSendDestroy(pMsg2);
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
