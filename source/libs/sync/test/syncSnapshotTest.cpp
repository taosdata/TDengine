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

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 1;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM  *pFsm;
SWal      *pWal;
SSyncNode *gSyncNode;
SyncIndex  snapshotLastApplyIndex = SYNC_INDEX_INVALID;

const char *pDir = "./syncSnapshotTest";
const char *pWalDir = "./syncSnapshotTest_wal";

void CommitCb(const struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  if (pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot;
    pFsm->FpGetSnapshotInfo(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    char logBuf[256];
    snprintf(logBuf, sizeof(logBuf),
             "==callback== ==CommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s \n", pFsm,
             cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state));
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
  } else {
    sTrace("==callback== ==CommitCb== do not apply again %" PRId64, cbMeta.index);
  }
}

void PreCommitCb(const struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s \n", pFsm,
           cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

void RollBackCb(const struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==RollBackCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s \n", pFsm,
           cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

int32_t GetSnapshotCb(const struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = snapshotLastApplyIndex;
  pSnapshot->lastApplyTerm = 100;
  return 0;
}

void initFsm() {
  pFsm = (SSyncFSM *)taosMemoryMalloc(sizeof(SSyncFSM));
  memset(pFsm, 0, sizeof(*pFsm));

#if 0 
  pFsm->FpCommitCb = CommitCb;
  pFsm->FpPreCommitCb = PreCommitCb;
  pFsm->FpRollBackCb = RollBackCb;
  pFsm->FpGetSnapshotInfo = GetSnapshotCb;
#endif
}

SSyncNode *syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.msgcb = &gSyncIO->msgcb;
  syncInfo.syncSendMSg = syncIOSendMsg;
  syncInfo.syncEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", pDir);

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
  pWal = walOpen(pWalDir, &walCfg);
  assert(pWal != NULL);

  syncInfo.pWal = pWal;

  SSyncCfg *pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = ports[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);
  }

  SSyncNode *pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncClientRequest = pSyncNode->FpOnClientRequest;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  // gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  // gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  // gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  // gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  // gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
  gSyncIO->pSyncNode = pSyncNode;

  syncNodeStart(pSyncNode);

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
  SRpcMsg clientRequestMsg;
  syncBuildClientRequest(&clientRequestMsg, pMsg, 123, true, 1000);
  SyncClientRequest *pMsg2 = (SyncClientRequest *)taosMemoryMalloc(clientRequestMsg.contLen);
  memcpy(pMsg2->data, clientRequestMsg.pCont, clientRequestMsg.contLen);
  return pMsg2;
}

int main(int argc, char **argv) {
  sprintf(tsTempDir, "%s", ".");

  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  void logTest();

  myIndex = 0;
  if (argc >= 2) {
    snapshotLastApplyIndex = atoi(argv[1]);
  }
  sTrace("--snapshotLastApplyIndex : %" PRId64 " \n", snapshotLastApplyIndex);

  int32_t ret = syncIOStart((char *)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncInit();
  assert(ret == 0);

  // taosRemoveDir(pWalDir);

  initFsm();

  gSyncNode = syncInitTest();
  assert(gSyncNode != NULL);
  sNTrace(gSyncNode, "");

  initRaftId(gSyncNode);

  // step0
  SRpcMsg *pMsg0 = step0();
  syncRpcMsgLog2((char *)"==step0==", pMsg0);

  // step1
  SyncClientRequest *pMsg1 = step1(pMsg0);
  syncClientRequestLog2((char *)"==step1==", pMsg1);

  for (int i = 0; i < 10; ++i) {
    SyncClientRequest *pSyncClientRequest = pMsg1;
    SRpcMsg            rpcMsg = {0};
    // syncClientRequest2RpcMsg(pSyncClientRequest, &rpcMsg);
    // gSyncNode->syncEqMsg(gSyncNode->msgcb, &rpcMsg);

    taosMsleep(1000);
  }

  while (1) {
    sTrace("while 1 sleep");
    taosMsleep(1000);
  }

  return 0;
}
