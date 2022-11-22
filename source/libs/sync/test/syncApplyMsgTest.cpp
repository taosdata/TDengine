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

SyncApplyMsg *createMsg() {
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(rpcMsg));
  rpcMsg.msgType = 12345;
  rpcMsg.contLen = 20;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  strcpy((char *)rpcMsg.pCont, "hello rpc");

  SFsmCbMeta meta;
  meta.code = 11;
  meta.index = 22;
  meta.isWeak = 1;
  meta.seqNum = 33;
  meta.state = TAOS_SYNC_STATE_LEADER;

  SyncApplyMsg *pMsg = syncApplyMsgBuild2(&rpcMsg, 123, &meta);
  rpcFreeCont(rpcMsg.pCont);
  return pMsg;
}

void test1() {
  SyncApplyMsg *pMsg = createMsg();
  syncApplyMsgLog2((char *)"test1:", pMsg);
  syncApplyMsgDestroy(pMsg);
}

void test2() {
  SyncApplyMsg *pMsg = createMsg();
  uint32_t      len = pMsg->bytes;
  char         *serialized = (char *)taosMemoryMalloc(len);
  syncApplyMsgSerialize(pMsg, serialized, len);
  SyncApplyMsg *pMsg2 = syncApplyMsgBuild(pMsg->dataLen);
  syncApplyMsgDeserialize(serialized, len, pMsg2);
  syncApplyMsgLog2((char *)"test2: syncApplyMsgSerialize -> syncApplyMsgDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncApplyMsgDestroy(pMsg);
  syncApplyMsgDestroy(pMsg2);
}

void test3() {
  SyncApplyMsg *pMsg = createMsg();
  uint32_t      len;
  char         *serialized = syncApplyMsgSerialize2(pMsg, &len);
  SyncApplyMsg *pMsg2 = syncApplyMsgDeserialize2(serialized, len);
  syncApplyMsgLog2((char *)"test3: syncApplyMsgSerialize2 -> syncApplyMsgDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncApplyMsgDestroy(pMsg);
  syncApplyMsgDestroy(pMsg2);
}

void test4() {
  SyncApplyMsg *pMsg = createMsg();
  SRpcMsg       rpcMsg;
  syncApplyMsg2RpcMsg(pMsg, &rpcMsg);
  SyncApplyMsg *pMsg2 = (SyncApplyMsg *)taosMemoryMalloc(rpcMsg.contLen);
  syncApplyMsgFromRpcMsg(&rpcMsg, pMsg2);
  syncApplyMsgLog2((char *)"test4: syncApplyMsg2RpcMsg -> syncApplyMsgFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncApplyMsgDestroy(pMsg);
  syncApplyMsgDestroy(pMsg2);
}

void test5() {
  SyncApplyMsg *pMsg = createMsg();
  SRpcMsg       rpcMsg = {0};
  syncApplyMsg2RpcMsg(pMsg, &rpcMsg);
  SyncApplyMsg *pMsg2 = syncApplyMsgFromRpcMsg2(&rpcMsg);
  syncApplyMsgLog2((char *)"test5: syncClientRequest2RpcMsg -> syncApplyMsgFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncApplyMsgDestroy(pMsg);
  syncApplyMsgDestroy(pMsg2);
}

void test6() {
  SyncApplyMsg *pMsg = createMsg();
  SRpcMsg       rpcMsg;
  syncApplyMsg2RpcMsg(pMsg, &rpcMsg);
  SyncApplyMsg *pMsg2 = syncApplyMsgFromRpcMsg2(&rpcMsg);

  SRpcMsg originalRpcMsg;
  syncApplyMsg2OriginalRpcMsg(pMsg2, &originalRpcMsg);
  syncRpcMsgLog2((char *)"test6", &originalRpcMsg);

  rpcFreeCont(originalRpcMsg.pCont);
  rpcFreeCont(rpcMsg.pCont);
  syncApplyMsgDestroy(pMsg);
  syncApplyMsgDestroy(pMsg2);
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
  test6();

  return 0;
}
