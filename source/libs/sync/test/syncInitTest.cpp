#include <gtest/gtest.h>
#include <stdio.h>
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

uint16_t ports[3] = {7010, 7110, 7210};

SSyncNode* syncInitTest() {
  SSyncFSM* pFsm;

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./test_path");
  snprintf(syncInfo.walPath, sizeof(syncInfo.walPath), "%s", "./test_wal_path");

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = 0;
  pCfg->replicaNum = 3;

  pCfg->nodeInfo[0].nodePort = ports[0];
  snprintf(pCfg->nodeInfo[0].nodeFqdn, sizeof(pCfg->nodeInfo[0].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);

  pCfg->nodeInfo[1].nodePort = ports[1];
  snprintf(pCfg->nodeInfo[1].nodeFqdn, sizeof(pCfg->nodeInfo[1].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[1].nodeFqdn);

  pCfg->nodeInfo[2].nodePort = ports[2];
  snprintf(pCfg->nodeInfo[2].nodeFqdn, sizeof(pCfg->nodeInfo[2].nodeFqdn), "%s", "127.0.0.1");
  // taosGetFqdn(pCfg->nodeInfo[2].nodeFqdn);

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[0]);
  assert(ret == 0);

  SSyncNode* pSyncNode = syncInitTest();
  assert(pSyncNode != NULL);

  cJSON* pJson = syncNode2Json(pSyncNode);
  char*  serialized = cJSON_Print(pJson);
  printf("%s\n", serialized);
  free(serialized);
  cJSON_Delete(pJson);

  return 0;
}
