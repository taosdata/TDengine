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

SRpcMsg *createRpcMsg(int32_t i, int32_t dataLen) {
  SyncPing *pSyncMsg = syncPingBuild(20);
  snprintf(pSyncMsg->data, pSyncMsg->dataLen, "value_%d", i);

  SRpcMsg *pRpcMsg = (SRpcMsg *)taosMemoryMalloc(sizeof(SRpcMsg));
  memset(pRpcMsg, 0, sizeof(SRpcMsg));
  pRpcMsg->code = 10 * i;
  syncPing2RpcMsg(pSyncMsg, pRpcMsg);

  syncPingDestroy(pSyncMsg);
  return pRpcMsg;
}

SyncClientRequestBatch *createMsg() {
  SRpcMsg *rpcMsgPArr[5];
  memset(rpcMsgPArr, 0, sizeof(rpcMsgPArr));
  for (int32_t i = 0; i < 5; ++i) {
    SRpcMsg *pRpcMsg = createRpcMsg(i, 20);
    rpcMsgPArr[i] = pRpcMsg;
    //taosMemoryFree(pRpcMsg);
  }

  SRaftMeta raftArr[5];
  memset(raftArr, 0, sizeof(raftArr));
  for (int32_t i = 0; i < 5; ++i) {
    raftArr[i].seqNum = i * 10;
    raftArr[i].isWeak = i % 2;
  }

  SyncClientRequestBatch *pMsg = syncClientRequestBatchBuild(rpcMsgPArr, raftArr, 5, 1234);
  return pMsg;
}

void test1() {
  SyncClientRequestBatch *pMsg = createMsg();
  syncClientRequestBatchLog2((char *)"==test1==", pMsg);
  syncClientRequestBatchDestroyDeep(pMsg);
}

/*
void test2() {
  SyncClientRequest *pMsg = createMsg();
  uint32_t           len = pMsg->bytes;
  char *             serialized = (char *)taosMemoryMalloc(len);
  syncClientRequestSerialize(pMsg, serialized, len);
  SyncClientRequest *pMsg2 = syncClientRequestBuild(pMsg->dataLen);
  syncClientRequestDeserialize(serialized, len, pMsg2);
  syncClientRequestLog2((char *)"test2: syncClientRequestSerialize -> syncClientRequestDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test3() {
  SyncClientRequest *pMsg = createMsg();
  uint32_t           len;
  char *             serialized = syncClientRequestSerialize2(pMsg, &len);
  SyncClientRequest *pMsg2 = syncClientRequestDeserialize2(serialized, len);
  syncClientRequestLog2((char *)"test3: syncClientRequestSerialize3 -> syncClientRequestDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test4() {
  SyncClientRequest *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncClientRequest2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequest *pMsg2 = (SyncClientRequest *)taosMemoryMalloc(rpcMsg.contLen);
  syncClientRequestFromRpcMsg(&rpcMsg, pMsg2);
  syncClientRequestLog2((char *)"test4: syncClientRequest2RpcMsg -> syncClientRequestFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
}

void test5() {
  SyncClientRequest *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncClientRequest2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequest *pMsg2 = syncClientRequestFromRpcMsg2(&rpcMsg);
  syncClientRequestLog2((char *)"test5: syncClientRequest2RpcMsg -> syncClientRequestFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncClientRequestDestroy(pMsg);
  syncClientRequestDestroy(pMsg2);
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
