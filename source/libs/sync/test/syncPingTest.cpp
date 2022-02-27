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

void doSync() {
  SSyncFSM* pFsm;

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./test_sync_ping");

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = 0;
  pCfg->replicaNum = 3;

  pCfg->nodeInfo[0].nodePort = 7010;
  taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);

  pCfg->nodeInfo[1].nodePort = 7110;
  taosGetFqdn(pCfg->nodeInfo[1].nodeFqdn);

  pCfg->nodeInfo[2].nodePort = 7210;
  taosGetFqdn(pCfg->nodeInfo[2].nodeFqdn);

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnPing = pSyncNode->FpOnPing;
  gSyncIO->pSyncNode = pSyncNode;
}

int main() {
  //taosInitLog((char*)"syncPingTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret = syncIOStart();
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  doSync();

  while (1) {
    taosMsleep(1000);
  }

  return 0;
}
