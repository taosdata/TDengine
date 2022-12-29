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

SyncLeaderTransfer *createMsg() {
  SyncLeaderTransfer *pMsg = syncLeaderTransferBuild(1000);
  /*
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  */
  pMsg->newLeaderId.addr = syncUtilAddr2U64("127.0.0.1", 9999);
  pMsg->newLeaderId.vgId = 100;
  return pMsg;
}

// for debug ----------------------
void syncLeaderTransferPrint(const SyncLeaderTransfer *pMsg) {
  char *serialized = syncLeaderTransfer2Str(pMsg);
  printf("syncLeaderTransferPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLeaderTransferPrint2(char *s, const SyncLeaderTransfer *pMsg) {
  char *serialized = syncLeaderTransfer2Str(pMsg);
  printf("syncLeaderTransferPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLeaderTransferLog(const SyncLeaderTransfer *pMsg) {
  char *serialized = syncLeaderTransfer2Str(pMsg);
  sTrace("syncLeaderTransferLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncLeaderTransferLog2(char *s, const SyncLeaderTransfer *pMsg) {
  if (gRaftDetailLog) {
    char *serialized = syncLeaderTransfer2Str(pMsg);
    sTrace("syncLeaderTransferLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

void test1() {
  SyncLeaderTransfer *pMsg = createMsg();
  syncLeaderTransferLog2((char *)"test1:", pMsg);
  syncLeaderTransferDestroy(pMsg);
}

void test2() {
  SyncLeaderTransfer *pMsg = createMsg();
  uint32_t            len = pMsg->bytes;
  char               *serialized = (char *)taosMemoryMalloc(len);
  syncLeaderTransferSerialize(pMsg, serialized, len);
  SyncLeaderTransfer *pMsg2 = syncLeaderTransferBuild(1000);
  syncLeaderTransferDeserialize(serialized, len, pMsg2);
  syncLeaderTransferLog2((char *)"test2: syncLeaderTransferSerialize -> syncLeaderTransferDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncLeaderTransferDestroy(pMsg);
  syncLeaderTransferDestroy(pMsg2);
}

void test3() {
  SyncLeaderTransfer *pMsg = createMsg();
  uint32_t            len;
  char               *serialized = syncLeaderTransferSerialize2(pMsg, &len);
  SyncLeaderTransfer *pMsg2 = syncLeaderTransferDeserialize2(serialized, len);
  syncLeaderTransferLog2((char *)"test3: syncLeaderTransferSerialize2 -> syncLeaderTransferDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncLeaderTransferDestroy(pMsg);
  syncLeaderTransferDestroy(pMsg2);
}

void test4() {
  SyncLeaderTransfer *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncLeaderTransfer2RpcMsg(pMsg, &rpcMsg);
  SyncLeaderTransfer *pMsg2 = (SyncLeaderTransfer *)taosMemoryMalloc(rpcMsg.contLen);
  syncLeaderTransferFromRpcMsg(&rpcMsg, pMsg2);
  syncLeaderTransferLog2((char *)"test4: syncLeaderTransfer2RpcMsg -> syncLeaderTransferFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncLeaderTransferDestroy(pMsg);
  syncLeaderTransferDestroy(pMsg2);
}

void test5() {
  SyncLeaderTransfer *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncLeaderTransfer2RpcMsg(pMsg, &rpcMsg);
  SyncLeaderTransfer *pMsg2 = syncLeaderTransferFromRpcMsg2(&rpcMsg);
  syncLeaderTransferLog2((char *)"test5: syncLeaderTransfer2RpcMsg -> syncLeaderTransferFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncLeaderTransferDestroy(pMsg);
  syncLeaderTransferDestroy(pMsg2);
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
