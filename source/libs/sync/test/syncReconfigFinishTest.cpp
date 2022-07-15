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

SSyncCfg *createSyncOldCfg() {
  SSyncCfg *pCfg = (SSyncCfg *)taosMemoryMalloc(sizeof(SSyncCfg));
  memset(pCfg, 0, sizeof(SSyncCfg));

  pCfg->replicaNum = 3;
  pCfg->myIndex = 1;
  for (int i = 0; i < pCfg->replicaNum; ++i) {
    ((pCfg->nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pCfg->nodeInfo)[i]).nodeFqdn), "100.200.300.%d", i);
  }

  return pCfg;
}

SSyncCfg *createSyncNewCfg() {
  SSyncCfg *pCfg = (SSyncCfg *)taosMemoryMalloc(sizeof(SSyncCfg));
  memset(pCfg, 0, sizeof(SSyncCfg));

  pCfg->replicaNum = 3;
  pCfg->myIndex = 1;
  for (int i = 0; i < pCfg->replicaNum; ++i) {
    ((pCfg->nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pCfg->nodeInfo)[i]).nodeFqdn), "500.600.700.%d", i);
  }

  return pCfg;
}

SyncReconfigFinish *createMsg() {
  SyncReconfigFinish *pMsg = syncReconfigFinishBuild(1234);

  SSyncCfg *pOld = createSyncOldCfg();
  SSyncCfg *pNew = createSyncNewCfg();
  pMsg->oldCfg = *pOld;
  pMsg->newCfg = *pNew;

  pMsg->newCfgIndex = 11;
  pMsg->newCfgTerm = 22;
  pMsg->newCfgSeqNum = 33;

  taosMemoryFree(pOld);
  taosMemoryFree(pNew);

  return pMsg;
}

void test1() {
  SyncReconfigFinish *pMsg = createMsg();
  syncReconfigFinishLog2((char *)"test1:", pMsg);
  syncReconfigFinishDestroy(pMsg);
}

void test2() {
  SyncReconfigFinish *pMsg = createMsg();
  uint32_t            len = pMsg->bytes;
  char *              serialized = (char *)taosMemoryMalloc(len);
  syncReconfigFinishSerialize(pMsg, serialized, len);
  SyncReconfigFinish *pMsg2 = syncReconfigFinishBuild(1000);
  syncReconfigFinishDeserialize(serialized, len, pMsg2);
  syncReconfigFinishLog2((char *)"test2: syncReconfigFinishSerialize -> syncReconfigFinishDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncReconfigFinishDestroy(pMsg);
  syncReconfigFinishDestroy(pMsg2);
}

void test3() {
  SyncReconfigFinish *pMsg = createMsg();
  uint32_t            len;
  char *              serialized = syncReconfigFinishSerialize2(pMsg, &len);
  SyncReconfigFinish *pMsg2 = syncReconfigFinishDeserialize2(serialized, len);
  syncReconfigFinishLog2((char *)"test3: SyncReconfigFinishSerialize2 -> syncReconfigFinishDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncReconfigFinishDestroy(pMsg);
  syncReconfigFinishDestroy(pMsg2);
}

void test4() {
  SyncReconfigFinish *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncReconfigFinish2RpcMsg(pMsg, &rpcMsg);
  SyncReconfigFinish *pMsg2 = (SyncReconfigFinish *)taosMemoryMalloc(rpcMsg.contLen);
  syncReconfigFinishFromRpcMsg(&rpcMsg, pMsg2);
  syncReconfigFinishLog2((char *)"test4: syncReconfigFinish2RpcMsg -> syncReconfigFinishFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncReconfigFinishDestroy(pMsg);
  syncReconfigFinishDestroy(pMsg2);
}

void test5() {
  SyncReconfigFinish *pMsg = createMsg();
  SRpcMsg             rpcMsg;
  syncReconfigFinish2RpcMsg(pMsg, &rpcMsg);
  SyncReconfigFinish *pMsg2 = syncReconfigFinishFromRpcMsg2(&rpcMsg);
  syncReconfigFinishLog2((char *)"test5: syncReconfigFinish2RpcMsg -> syncReconfigFinishFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncReconfigFinishDestroy(pMsg);
  syncReconfigFinishDestroy(pMsg2);
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
