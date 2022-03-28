#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncRaftEntry.h"
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
int32_t  replicaNum = 1;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM * pFsm;
SWal *     pWal;
SSyncNode *gSyncNode;

void CommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SyncIndex index, bool isWeak, int32_t code,
              ESyncState state) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "==callback== ==CommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, index, isWeak, code, state, syncUtilState2String(state));
  syncRpcMsgPrint2(logBuf, (SRpcMsg *)pMsg);
}

void PreCommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SyncIndex index, bool isWeak, int32_t code,
                 ESyncState state) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n", pFsm, index, isWeak,
           code, state, syncUtilState2String(state));
  syncRpcMsgPrint2(logBuf, (SRpcMsg *)pMsg);
}

void RollBackCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SyncIndex index, bool isWeak, int32_t code,
                ESyncState state) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "==callback== ==RollBackCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, index, isWeak, code, state, syncUtilState2String(state));
  syncRpcMsgPrint2(logBuf, (SRpcMsg *)pMsg);
}

void initFsm() {
  pFsm = (SSyncFSM *)taosMemoryMalloc(sizeof(SSyncFSM));
  pFsm->FpCommitCb = CommitCb;
  pFsm->FpPreCommitCb = PreCommitCb;
  pFsm->FpRollBackCb = RollBackCb;
}

SSyncNode *syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./write_test");

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
  pWal = walOpen("./write_test_wal", &walCfg);
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

  SSyncNode *pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncClientRequest = pSyncNode->FpOnClientRequest;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
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
  SyncClientRequest *pRetMsg = syncClientRequestBuild2(pMsg, 123, true);
  return pRetMsg;
}

int main(int argc, char **argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  void logTest();

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char *)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  taosRemoveDir("./wal_test");

  initFsm();

  gSyncNode = syncInitTest();
  assert(gSyncNode != NULL);
  syncNodePrint2((char *)"", gSyncNode);

  initRaftId(gSyncNode);

  // step0
  SRpcMsg *pMsg0 = step0();
  syncRpcMsgPrint2((char *)"==step0==", pMsg0);

  // step1
  SyncClientRequest *pMsg1 = step1(pMsg0);
  syncClientRequestPrint2((char *)"==step1==", pMsg1);

  for (int i = 0; i < 10; ++i) {
    SyncClientRequest *pSyncClientRequest = pMsg1;
    SRpcMsg            rpcMsg;
    syncClientRequest2RpcMsg(pSyncClientRequest, &rpcMsg);
    gSyncNode->FpEqMsg(gSyncNode->queue, &rpcMsg);

    taosMsleep(1000);
  }

  while (1) {
    sTrace("while 1 sleep");
    taosMsleep(1000);
  }

  return 0;
}
