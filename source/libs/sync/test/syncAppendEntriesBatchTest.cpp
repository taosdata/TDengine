//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"
#include "trpc.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

/*
SRpcMsg *createRpcMsg(int32_t i, int32_t dataLen) {
  SRpcMsg *pRpcMsg = (SRpcMsg *)taosMemoryMalloc(sizeof(SRpcMsg));
  memset(pRpcMsg, 0, sizeof(SRpcMsg));

  pRpcMsg->msgType = TDMT_SYNC_PING;
  pRpcMsg->contLen = dataLen;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  pRpcMsg->code = 10 * i;
  snprintf((char *)pRpcMsg->pCont, pRpcMsg->contLen, "value_%d", i);

  return pRpcMsg;
}

SyncAppendEntriesBatch *createMsg() {
  SRpcMsg rpcMsgArr[5];
  memset(rpcMsgArr, 0, sizeof(rpcMsgArr));

  for (int32_t i = 0; i < 5; ++i) {
    SRpcMsg *pRpcMsg = createRpcMsg(i, 20);
    rpcMsgArr[i] = *pRpcMsg;
    taosMemoryFree(pRpcMsg);
  }

  SyncAppendEntriesBatch *pMsg = syncAppendEntriesBatchBuild(rpcMsgArr, 5, 1234);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->prevLogIndex = 11;
  pMsg->prevLogTerm = 22;
  pMsg->commitIndex = 33;
  pMsg->privateTerm = 44;
  return pMsg;
}

void test1() {
  SyncAppendEntriesBatch *pMsg = createMsg();
  syncAppendEntriesBatchLog2((char *)"test1:", pMsg);

  SRpcMsg rpcMsgArr[5];
  int32_t retArrSize;
  syncAppendEntriesBatch2RpcMsgArray(pMsg, rpcMsgArr, 5, &retArrSize);
  for (int i = 0; i < retArrSize; ++i) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "==test1 decode rpc msg %d: msgType:%d, code:%d, contLen:%d, pCont:%s \n", i,
             rpcMsgArr[i].msgType, rpcMsgArr[i].code, rpcMsgArr[i].contLen, (char *)rpcMsgArr[i].pCont);
    sTrace("%s", logBuf);
  }

  syncAppendEntriesBatchDestroy(pMsg);
}

void test2() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len = pMsg->bytes;
  char *             serialized = (char *)taosMemoryMalloc(len);
  syncAppendEntriesSerialize(pMsg, serialized, len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesBuild(pMsg->dataLen, 1000);
  syncAppendEntriesDeserialize(serialized, len, pMsg2);
  syncAppendEntriesLog2((char *)"test2: syncAppendEntriesSerialize -> syncAppendEntriesDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test3() {
  SyncAppendEntries *pMsg = createMsg();
  uint32_t           len;
  char *             serialized = syncAppendEntriesSerialize2(pMsg, &len);
  SyncAppendEntries *pMsg2 = syncAppendEntriesDeserialize2(serialized, len);
  syncAppendEntriesLog2((char *)"test3: syncAppendEntriesSerialize3 -> syncAppendEntriesDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test4() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = (SyncAppendEntries *)taosMemoryMalloc(rpcMsg.contLen);
  syncAppendEntriesFromRpcMsg(&rpcMsg, pMsg2);
  syncAppendEntriesLog2((char *)"test4: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}

void test5() {
  SyncAppendEntries *pMsg = createMsg();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  SyncAppendEntries *pMsg2 = syncAppendEntriesFromRpcMsg2(&rpcMsg);
  syncAppendEntriesLog2((char *)"test5: syncAppendEntries2RpcMsg -> syncAppendEntriesFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncAppendEntriesDestroy(pMsg);
  syncAppendEntriesDestroy(pMsg2);
}
*/

int main() {
  gRaftDetailLog = true;
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_DEBUG + DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  /*
   test1();
   test2();
   test3();
   test4();
   test5();
 */

  return 0;
}
