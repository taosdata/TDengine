#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"
#include "syncVoteMgr.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[3] = {7010, 7110, 7210};

SSyncNode* doSync(int myIndex) {
  SSyncFSM* pFsm;

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./test_sync_ping");

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
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
  logTest();

  int     myIndex = 0;
  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  SSyncNode*     pSyncNode = doSync(myIndex);
  SVotesGranted* pVotesGranted = voteGrantedCreate(pSyncNode);
  assert(pVotesGranted != NULL);

  char* serialized = voteGranted2Str(pVotesGranted);
  assert(serialized != NULL);
  printf("%s\n", serialized);
  free(serialized);

  return 0;
}
