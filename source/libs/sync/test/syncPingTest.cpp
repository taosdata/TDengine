#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SSyncNode* doSync() {
  SSyncFSM* pFsm;

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./test_sync_ping");

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = 0;
  pCfg->replicaNum = 2;

  pCfg->nodeInfo[0].nodePort = 7010;
  snprintf(pCfg->nodeInfo[0].nodeFqdn, sizeof(pCfg->nodeInfo[0].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);

  pCfg->nodeInfo[1].nodePort = 7110;
  snprintf(pCfg->nodeInfo[1].nodeFqdn, sizeof(pCfg->nodeInfo[1].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[1].nodeFqdn);

  pCfg->nodeInfo[2].nodePort = 7210;
  snprintf(pCfg->nodeInfo[2].nodeFqdn, sizeof(pCfg->nodeInfo[2].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[2].nodeFqdn);

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

void timerPingAll(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  syncNodePingAll(pSyncNode);
}

int main() {
  // taosInitLog((char*)"syncPingTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret = syncIOStart((char*)"127.0.0.1", 7010);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  SSyncNode* pSyncNode = doSync();
  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;

  ret = syncNodeStartPingTimer(pSyncNode);
  assert(ret == 0);

  /*
  taosMsleep(10000);
  ret = syncNodeStopPingTimer(pSyncNode);
  assert(ret == 0);
  */

  while (1) {
    taosMsleep(1000);
  }

  return 0;
}
