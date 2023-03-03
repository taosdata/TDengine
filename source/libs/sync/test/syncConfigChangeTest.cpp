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

uint16_t    gPorts[] = {7010, 7110, 7210, 7310, 7410};
const char* gDir = "./syncReplicateTest";
int32_t     gVgId = 1234;
SyncIndex   gSnapshotLastApplyIndex;

void init() {
  int code = walInit();
  assert(code == 0);

  code = syncInit();
  assert(code == 0);

  sprintf(tsTempDir, "%s", ".");
}

void cleanup() { walCleanUp(); }

void CommitCb(const struct SSyncFSM* pFsm, const SRpcMsg* pMsg, const SFsmCbMeta cbMeta) {
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  if (pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot;
    pFsm->FpGetSnapshotInfo(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    char logBuf[256] = {0};
    snprintf(logBuf, sizeof(logBuf),
             "==callback== ==CommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s flag:%" PRIu64 "\n",
             pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag);
    syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
  } else {
    sTrace("==callback== ==CommitCb== do not apply again %" PRId64, cbMeta.index);
  }
}

void PreCommitCb(const struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s flag:%" PRIu64
           "\n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

void RollBackCb(const struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==RollBackCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s flag:%" PRIu64 "\n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

int32_t GetSnapshotCb(const struct SSyncFSM* pFsm, SSnapshot* pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = gSnapshotLastApplyIndex;
  pSnapshot->lastApplyTerm = 100;
  return 0;
}

void RestoreFinishCb(struct SSyncFSM* pFsm) { sTrace("==callback== ==RestoreFinishCb=="); }

void ReConfigCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SReConfigCbMeta* cbMeta) {
  sTrace("==callback== ==ReConfigCb== flag:%" PRIx64 ", index:%" PRId64 ", code:%d, currentTerm:%" PRIu64
         ", term:%" PRIu64,
         cbMeta->flag, cbMeta->index, cbMeta->code, cbMeta->currentTerm, cbMeta->term);
}

SSyncFSM* createFsm() {
  SSyncFSM* pFsm = (SSyncFSM*)taosMemoryMalloc(sizeof(SSyncFSM));
  memset(pFsm, 0, sizeof(*pFsm));

#if 0
  pFsm->FpCommitCb = CommitCb;
  pFsm->FpPreCommitCb = PreCommitCb;
  pFsm->FpRollBackCb = RollBackCb;

  pFsm->FpGetSnapshotInfo = GetSnapshotCb;
  pFsm->FpRestoreFinishCb = RestoreFinishCb;

  pFsm->FpReConfigCb = ReConfigCb;
#endif

  return pFsm;
}

SWal* createWal(char* path, int32_t vgId) {
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal* pWal = walOpen(path, &walCfg);
  assert(pWal != NULL);
  return pWal;
}

int64_t createSyncNode(int32_t replicaNum, int32_t myIndex, int32_t vgId, SWal* pWal, char* path, bool isStandBy) {
  SSyncInfo syncInfo;
  syncInfo.vgId = vgId;
  syncInfo.msgcb = &gSyncIO->msgcb;
  syncInfo.syncSendMSg = syncIOSendMsg;
  syncInfo.syncEqMsg = syncIOEqMsg;
  syncInfo.pFsm = createFsm();
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s_sync_replica%d_index%d", path, replicaNum, myIndex);
  syncInfo.pWal = pWal;
  syncInfo.isStandBy = isStandBy;

  SSyncCfg* pCfg = &syncInfo.syncCfg;

  if (isStandBy) {
    pCfg->myIndex = 0;
    pCfg->replicaNum = 1;
    pCfg->nodeInfo[0].nodePort = gPorts[myIndex];
    taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);

  } else {
    pCfg->myIndex = myIndex;
    pCfg->replicaNum = replicaNum;

    for (int i = 0; i < replicaNum; ++i) {
      pCfg->nodeInfo[i].nodePort = gPorts[i];
      taosGetFqdn(pCfg->nodeInfo[i].nodeFqdn);
      // snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    }
  }

  int64_t rid = syncOpen(&syncInfo);
  assert(rid > 0);

  SSyncNode* pSyncNode = (SSyncNode*)syncNodeAcquire(rid);
  assert(pSyncNode != NULL);
  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  // gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  // gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  // gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  // gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  // gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  // gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  // gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
  // gSyncIO->FpOnSyncClientRequest = pSyncNode->FpOnClientRequest;
  gSyncIO->pSyncNode = pSyncNode;
  syncNodeRelease(pSyncNode);

  return rid;
}

void configChange(int64_t rid, int32_t replicaNum, int32_t myIndex) {
  SSyncCfg syncCfg;

  syncCfg.myIndex = myIndex;
  syncCfg.replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    syncCfg.nodeInfo[i].nodePort = gPorts[i];
    taosGetFqdn(syncCfg.nodeInfo[i].nodeFqdn);
  }

  syncReconfig(rid, &syncCfg);
}

void usage(char* exe) {
  printf("usage: %s replicaNum myIndex lastApplyIndex writeRecordNum isStandBy isConfigChange \n", exe);
}

SRpcMsg* createRpcMsg(int i, int count, int myIndex) {
  SRpcMsg* pMsg = (SRpcMsg*)taosMemoryMalloc(sizeof(SRpcMsg));
  memset(pMsg, 0, sizeof(SRpcMsg));
  pMsg->msgType = 9999;
  pMsg->contLen = 256;
  pMsg->pCont = rpcMallocCont(pMsg->contLen);
  snprintf((char*)(pMsg->pCont), pMsg->contLen, "value-myIndex:%u-%d-%d-%" PRId64, myIndex, i, count,
           taosGetTimestampMs());
  return pMsg;
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE + DEBUG_INFO;
  if (argc != 7) {
    usage(argv[0]);
    exit(-1);
  }

  int32_t replicaNum = atoi(argv[1]);
  int32_t myIndex = atoi(argv[2]);
  int32_t lastApplyIndex = atoi(argv[3]);
  int32_t writeRecordNum = atoi(argv[4]);
  bool    isStandBy = atoi(argv[5]);
  bool    isConfigChange = atoi(argv[6]);
  gSnapshotLastApplyIndex = lastApplyIndex;

  if (!isStandBy) {
    assert(replicaNum >= 1 && replicaNum <= 5);
    assert(myIndex >= 0 && myIndex < replicaNum);
    assert(lastApplyIndex >= -1);
    assert(writeRecordNum >= 0);
  }

  init();
  int32_t ret = syncIOStart((char*)"127.0.0.1", gPorts[myIndex]);
  assert(ret == 0);

  char walPath[128];
  snprintf(walPath, sizeof(walPath), "%s_wal_replica%d_index%d", gDir, replicaNum, myIndex);
  SWal* pWal = createWal(walPath, gVgId);

  int64_t rid = createSyncNode(replicaNum, myIndex, gVgId, pWal, (char*)gDir, isStandBy);
  assert(rid > 0);

  syncStart(rid);

  /*
    if (isStandBy) {
      syncStartStandBy(rid);
    } else {
      syncStart(rid);
    }
  */

  SSyncNode* pSyncNode = (SSyncNode*)syncNodeAcquire(rid);
  assert(pSyncNode != NULL);

  if (isConfigChange) {
    configChange(rid, 2, myIndex);
  }

  //---------------------------
  int32_t alreadySend = 0;
  while (1) {
    char* s = syncNode2SimpleStr(pSyncNode);

    if (alreadySend < writeRecordNum) {
      SRpcMsg* pRpcMsg = createRpcMsg(alreadySend, writeRecordNum, myIndex);
      int32_t  ret = syncPropose(rid, pRpcMsg, false, NULL);
      if (ret == -1 && terrno == TSDB_CODE_SYN_NOT_LEADER) {
        sTrace("%s value%d write not leader", s, alreadySend);
      } else {
        assert(ret == 0);
        sTrace("%s value%d write ok", s, alreadySend);
      }
      alreadySend++;

      rpcFreeCont(pRpcMsg->pCont);
      taosMemoryFree(pRpcMsg);
    } else {
      sTrace("%s", s);
    }

    taosMsleep(1000);
    taosMemoryFree(s);
    taosMsleep(1000);
  }

  syncNodeRelease(pSyncNode);
  syncStop(rid);
  walClose(pWal);
  syncIOStop();
  cleanup();
  return 0;
}
