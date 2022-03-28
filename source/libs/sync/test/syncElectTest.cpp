#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 3;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM*  pFsm;
SWal*      pWal;
SSyncNode* gSyncNode;

SSyncNode* syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "./elect_test_%d", myIndex);

  int code = walInit();
  assert(code == 0);
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = syncInfo.vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;

  char tmpdir[128];
  snprintf(tmpdir, sizeof(tmpdir), "./elect_test_wal_%d", myIndex);
  pWal = walOpen(tmpdir, &walCfg);
  assert(pWal != NULL);

  syncInfo.pWal = pWal;

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = ports[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);
  }

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

SSyncNode* syncInitTest() { return syncNodeInit(); }

void initRaftId(SSyncNode* pSyncNode) {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i] = pSyncNode->replicasId[i];
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    taosMemoryFree(s);
  }
}

int main(int argc, char** argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  gSyncNode = syncInitTest();
  assert(gSyncNode != NULL);
  syncNodePrint2((char*)"", gSyncNode);

  initRaftId(gSyncNode);

  //---------------------------
  while (1) {
    sTrace(
        "elect sleep, state: %d, %s, term:%lu electTimerLogicClock:%lu, electTimerLogicClockUser:%lu, electTimerMS:%d",
        gSyncNode->state, syncUtilState2String(gSyncNode->state), gSyncNode->pRaftStore->currentTerm,
        gSyncNode->electTimerLogicClock, gSyncNode->electTimerLogicClockUser, gSyncNode->electTimerMS);
    taosMsleep(1000);
  }

  return 0;
}
