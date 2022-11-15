#include <gtest/gtest.h>
#include "syncTest.h"

#if 0
void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 1;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM  *pFsm;
SWal      *pWal;
SSyncNode *pSyncNode;

SSyncNode *syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.msgcb = &gSyncIO->msgcb;
  syncInfo.syncSendMSg = syncIOSendMsg;
  syncInfo.syncEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./");

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
  pWal = walOpen("./wal_test", &walCfg);
  assert(pWal != NULL);

  syncInfo.pWal = pWal;

  SSyncCfg *pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = ports[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);
  }

  pSyncNode = syncNodeOpen(&syncInfo);
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

SSyncNode *syncInitTest() { return syncNodeInit(); }

void initRaftId(SSyncNode *pSyncNode) {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i] = pSyncNode->replicasId[i];
    char *s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    taosMemoryFree(s);
  }
}

SRpcMsg *step0() {
  SRpcMsg *pMsg = (SRpcMsg *)taosMemoryMalloc(sizeof(SRpcMsg));
  memset(pMsg, 0, sizeof(SRpcMsg));
  pMsg->msgType = 9999;
  pMsg->contLen = 32;
  pMsg->pCont = taosMemoryMalloc(pMsg->contLen);
  snprintf((char *)(pMsg->pCont), pMsg->contLen, "hello, world");
  return pMsg;
}

SyncClientRequest *step1(const SRpcMsg *pMsg) {
  SyncClientRequest *pRetMsg = syncClientRequestBuild(pMsg, 123, true, 1000);
  return pRetMsg;
}

SRpcMsg *step2(const SyncClientRequest *pMsg) {
  SRpcMsg *pRetMsg = (SRpcMsg *)taosMemoryCalloc(sizeof(SRpcMsg), 1);
  syncClientRequest2RpcMsg(pMsg, pRetMsg);
  return pRetMsg;
}

SyncClientRequest *step3(const SRpcMsg *pMsg) {
  SyncClientRequest *pRetMsg = syncClientRequestFromRpcMsg2(pMsg);
  return pRetMsg;
}

SSyncRaftEntry *step4(const SyncClientRequest *pMsg) {
  SSyncRaftEntry *pRetMsg = syncEntryBuildFromClientRequest((SyncClientRequest *)pMsg, 100, 0);
  return pRetMsg;
}

SRpcMsg *step7(const SSyncRaftEntry *pMsg) {
  SRpcMsg *pRetMsg = (SRpcMsg *)taosMemoryMalloc(sizeof(SRpcMsg));
  syncEntry2OriginalRpc(pMsg, pRetMsg);
  return pRetMsg;
}
#endif
int main(int argc, char **argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  void logTest();

#if 0
  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char *)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncInit();
  assert(ret == 0);

  taosRemoveDir("./wal_test");

  // step0
  SRpcMsg *pMsg0 = step0();
  syncRpcMsgLog2((char *)"==step0==", pMsg0);

  // step1
  SyncClientRequest *pMsg1 = step1(pMsg0);
  syncClientRequestLog2((char *)"==step1==", pMsg1);

  // step2
  SRpcMsg *pMsg2 = step2(pMsg1);
  syncRpcMsgLog2((char *)"==step2==", pMsg2);

  // step3
  SyncClientRequest *pMsg3 = step3(pMsg2);
  syncClientRequestLog2((char *)"==step3==", pMsg3);

  // step4
  SSyncRaftEntry *pMsg4 = step4(pMsg3);
  syncEntryLog2((char *)"==step4==", pMsg4);

  // log, relog
  SSyncNode *pSyncNode = syncNodeInit();
  assert(pSyncNode != NULL);
  SSyncRaftEntry *pEntry = pMsg4;
  pSyncNode->pLogStore->syncLogAppendEntry(pSyncNode->pLogStore, pEntry);
 
  int32_t code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, pEntry->index, &pEntry);
  ASSERT(code == 0);

  syncEntryLog2((char *)"==pEntry==", pEntry);

  // step7
  SRpcMsg *pMsg7 = step7(pMsg6);
  syncRpcMsgLog2((char *)"==step7==", pMsg7);
#endif
  return 0;
}
