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

uint16_t    gPorts[] = {7000, 7001, 7002, 7003, 7004};
const char* gDir = "./syncTestTool";
int32_t     gVgId = 1234;
SyncIndex   gSnapshotLastApplyIndex;
SyncIndex   gSnapshotLastApplyTerm;
int         gIterTimes = 0;

SyncIndex gFinishLastApplyIndex;
SyncIndex gFinishLastApplyTerm;

void init() {
  int code = walInit();
  assert(code == 0);

  code = syncInit();
  assert(code == 0);
}

void cleanup() { walCleanUp(); }

void CommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==CommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s, flag:%" PRIu64
           ", term:%" PRIu64
           " "
           "currentTerm:%" PRIu64 " \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag,
           cbMeta.term, cbMeta.currentTerm);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

void PreCommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s, flag:%" PRIu64
           ", term:%" PRIu64
           " "
           "currentTerm:%" PRIu64 " \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag,
           cbMeta.term, cbMeta.currentTerm);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

void RollBackCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==RollBackCb== pFsm:%p, index:%" PRId64 ", isWeak:%d, code:%d, state:%d %s, flag:%" PRIu64
           ", term:%" PRIu64
           " "
           "currentTerm:%" PRIu64 " \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag,
           cbMeta.term, cbMeta.currentTerm);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

int32_t GetSnapshotCb(struct SSyncFSM* pFsm, SSnapshot* pSnapshot) {
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = gSnapshotLastApplyIndex;
  pSnapshot->lastApplyTerm = gSnapshotLastApplyTerm;
  return 0;
}

int32_t SnapshotStartRead(struct SSyncFSM* pFsm, void* pParam, void** ppReader) {
  *ppReader = (void*)0xABCD;
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf), "==callback== ==SnapshotStartRead== pFsm:%p, *ppReader:%p", pFsm, *ppReader);
  sTrace("%s", logBuf);
  return 0;
}

int32_t SnapshotStopRead(struct SSyncFSM* pFsm, void* pReader) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf), "==callback== ==SnapshotStopRead== pFsm:%p, pReader:%p", pFsm, pReader);
  sTrace("%s", logBuf);
  return 0;
}

int32_t SnapshotDoRead(struct SSyncFSM* pFsm, void* pReader, void** ppBuf, int32_t* len) {
  static int readIter = 0;

  if (readIter == gIterTimes) {
    *len = 0;
    *ppBuf = NULL;
  } else if (readIter < gIterTimes) {
    *len = 20;
    *ppBuf = taosMemoryMalloc(*len);
    snprintf((char*)*ppBuf, *len, "data iter:%d", readIter);
  }

  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==SnapshotDoRead== pFsm:%p, pReader:%p, *len:%d, *ppBuf:[%s], readIter:%d", pFsm, pReader,
           *len, (char*)(*ppBuf), readIter);
  sTrace("%s", logBuf);

  readIter++;
  return 0;
}

int32_t SnapshotStartWrite(struct SSyncFSM* pFsm, void* pParam, void** ppWriter) {
  *ppWriter = (void*)0xCDEF;
  char logBuf[256] = {0};

  snprintf(logBuf, sizeof(logBuf), "==callback== ==SnapshotStartWrite== pFsm:%p, *ppWriter:%p", pFsm, *ppWriter);
  sTrace("%s", logBuf);
  return 0;
}

int32_t SnapshotStopWrite(struct SSyncFSM* pFsm, void* pWriter, bool isApply, SSnapshot* pSnapshot) {
  if (isApply) {
    gSnapshotLastApplyIndex = gFinishLastApplyIndex;
    gSnapshotLastApplyTerm = gFinishLastApplyTerm;
  }

  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==SnapshotStopWrite== pFsm:%p, pWriter:%p, isApply:%d, gSnapshotLastApplyIndex:%" PRId64
           ", "
           "gSnapshotLastApplyTerm:%" PRId64,
           pFsm, pWriter, isApply, gSnapshotLastApplyIndex, gSnapshotLastApplyTerm);
  sTrace("%s", logBuf);

  return 0;
}

int32_t SnapshotDoWrite(struct SSyncFSM* pFsm, void* pWriter, void* pBuf, int32_t len) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf), "==callback== ==SnapshotDoWrite== pFsm:%p, pWriter:%p, len:%d pBuf:[%s]", pFsm,
           pWriter, len, (char*)pBuf);
  sTrace("%s", logBuf);
  return 0;
}

void RestoreFinishCb(struct SSyncFSM* pFsm) { sTrace("==callback== ==RestoreFinishCb== pFsm:%p", pFsm); }

void ReConfigCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SReConfigCbMeta* cbMeta) {
  char* s = syncCfg2Str(&(cbMeta->newCfg));
  sTrace("==callback== ==ReConfigCb== flag:%" PRIx64 ", index:%" PRId64 ", code:%d, currentTerm:%" PRIu64
         ", term:%" PRIu64 ", newCfg:%s",
         cbMeta->flag, cbMeta->index, cbMeta->code, cbMeta->currentTerm, cbMeta->term, s);
  taosMemoryFree(s);
}

void LeaderTransferCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==LeaderTransferCb== pFsm:%p, index:%" PRId64
           ", isWeak:%d, code:%d, state:%d %s, flag:%" PRIu64 ", term:%" PRIu64
           " "
           "currentTerm:%" PRIu64 " \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncStr(cbMeta.state), cbMeta.flag,
           cbMeta.term, cbMeta.currentTerm);
  syncRpcMsgLog2(logBuf, (SRpcMsg*)pMsg);
}

SSyncFSM* createFsm() {
  SSyncFSM* pFsm = (SSyncFSM*)taosMemoryMalloc(sizeof(SSyncFSM));
  memset(pFsm, 0, sizeof(*pFsm));

#if 0
  pFsm->FpCommitCb = CommitCb;
  pFsm->FpPreCommitCb = PreCommitCb;
  pFsm->FpRollBackCb = RollBackCb;

  pFsm->FpReConfigCb = ReConfigCb;
  pFsm->FpGetSnapshotInfo = GetSnapshotCb;
  pFsm->FpRestoreFinishCb = RestoreFinishCb;

  pFsm->FpSnapshotStartRead = SnapshotStartRead;
  pFsm->FpSnapshotStopRead = SnapshotStopRead;
  pFsm->FpSnapshotDoRead = SnapshotDoRead;
  pFsm->FpSnapshotStartWrite = SnapshotStartWrite;
  pFsm->FpSnapshotStopWrite = SnapshotStopWrite;
  pFsm->FpSnapshotDoWrite = SnapshotDoWrite;

  pFsm->FpLeaderTransferCb = LeaderTransferCb;
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

int64_t createSyncNode(int32_t replicaNum, int32_t myIndex, int32_t vgId, SWal* pWal, char* path, bool isStandBy,
                       ESyncStrategy enableSnapshot) {
  SSyncInfo syncInfo;
  syncInfo.vgId = vgId;
  syncInfo.msgcb = &gSyncIO->msgcb;
  syncInfo.syncSendMSg = syncIOSendMsg;
  syncInfo.syncEqMsg = syncIOEqMsg;
  syncInfo.pFsm = createFsm();
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s_sync_replica%d_index%d", path, replicaNum, myIndex);
  syncInfo.pWal = pWal;
  syncInfo.isStandBy = isStandBy;
  syncInfo.snapshotStrategy = enableSnapshot;

  SSyncCfg* pCfg = &syncInfo.syncCfg;

#if 0
  {
    pCfg->myIndex = myIndex;
    pCfg->replicaNum = replicaNum;

    for (int i = 0; i < replicaNum; ++i) {
      pCfg->nodeInfo[i].nodePort = gPorts[i];
      taosGetFqdn(pCfg->nodeInfo[i].nodeFqdn);
      // snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    }
  }
#endif

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
  // gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
  // gSyncIO->FpOnSyncClientRequest = pSyncNode->FpOnClientRequest;
  // gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  // gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  // gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  // gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  // gSyncIO->FpOnSyncSnapshot = pSyncNode->FpOnSnapshot;
  // gSyncIO->FpOnSyncSnapshotReply = pSyncNode->FpOnSnapshotReply;

  gSyncIO->pSyncNode = pSyncNode;
  syncNodeRelease(pSyncNode);

  return rid;
}

void configChange(int64_t rid, int32_t newReplicaNum, int32_t myIndex) {
  SSyncCfg syncCfg;

  syncCfg.myIndex = myIndex;
  syncCfg.replicaNum = newReplicaNum;

  for (int i = 0; i < newReplicaNum; ++i) {
    syncCfg.nodeInfo[i].nodePort = gPorts[i];
    taosGetFqdn(syncCfg.nodeInfo[i].nodeFqdn);
  }

  syncReconfig(rid, &syncCfg);
}

void usage(char* exe) {
  printf(
      "usage: %s  replicaNum(1-5)  myIndex(0-..)  enableSnapshot(0/1)  lastApplyIndex(>=-1)  lastApplyTerm(>=0)  "
      "writeRecordNum(>=0)  "
      "isStandBy(0/1)  isConfigChange(0-5)  iterTimes(>=0)  finishLastApplyIndex(>=-1)  finishLastApplyTerm(>=0) "
      "leaderTransfer(0/1) \n",
      exe);
}

SRpcMsg* createRpcMsg(int i, int count, int myIndex) {
  SRpcMsg* pMsg = (SRpcMsg*)taosMemoryMalloc(sizeof(SRpcMsg));
  memset(pMsg, 0, sizeof(SRpcMsg));
  pMsg->msgType = TDMT_VND_SUBMIT;
  pMsg->contLen = 256;
  pMsg->pCont = rpcMallocCont(pMsg->contLen);
  snprintf((char*)(pMsg->pCont), pMsg->contLen, "value-myIndex:%u-%d-%d-%" PRId64, myIndex, i, count,
           taosGetTimestampMs());
  return pMsg;
}

int main(int argc, char** argv) {
  sprintf(tsTempDir, "%s", ".");
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_SCREEN + DEBUG_FILE + DEBUG_TRACE + DEBUG_INFO + DEBUG_ERROR + DEBUG_DEBUG;

  if (argc != 13) {
    usage(argv[0]);
    exit(-1);
  }

  int32_t       replicaNum = atoi(argv[1]);
  int32_t       myIndex = atoi(argv[2]);
  ESyncStrategy enableSnapshot = (ESyncStrategy)atoi(argv[3]);
  int32_t       lastApplyIndex = atoi(argv[4]);
  int32_t       lastApplyTerm = atoi(argv[5]);
  int32_t       writeRecordNum = atoi(argv[6]);
  bool          isStandBy = atoi(argv[7]);
  int32_t       isConfigChange = atoi(argv[8]);
  int32_t       iterTimes = atoi(argv[9]);
  int32_t       finishLastApplyIndex = atoi(argv[10]);
  int32_t       finishLastApplyTerm = atoi(argv[11]);
  int32_t       leaderTransfer = atoi(argv[12]);

  sInfo(
      "args: replicaNum:%d, myIndex:%d, enableSnapshot:%d, lastApplyIndex:%d, lastApplyTerm:%d, writeRecordNum:%d, "
      "isStandBy:%d, isConfigChange:%d, iterTimes:%d, finishLastApplyIndex:%d, finishLastApplyTerm:%d, "
      "leaderTransfer:%d",
      replicaNum, myIndex, enableSnapshot, lastApplyIndex, lastApplyTerm, writeRecordNum, isStandBy, isConfigChange,
      iterTimes, finishLastApplyIndex, finishLastApplyTerm, leaderTransfer);

  // check parameter
  assert(replicaNum >= 1 && replicaNum <= 5);
  // assert(myIndex >= 0 && myIndex < replicaNum);
  assert(lastApplyIndex >= -1);
  assert(lastApplyTerm >= 0);
  assert(writeRecordNum >= 0);
  assert(isConfigChange >= 0 && isConfigChange <= 5);
  assert(iterTimes >= 0);
  assert(finishLastApplyIndex >= -1);
  assert(finishLastApplyTerm >= 0);

  char logFile[256];
  snprintf(logFile, sizeof(logFile), "/tmp/%s-replicaNum%d-myIndex%d.log", gDir, replicaNum, myIndex);
  taosInitLog(logFile, 100);
  sTrace("logFile : %s", logFile);

  gSnapshotLastApplyIndex = lastApplyIndex;
  gSnapshotLastApplyTerm = lastApplyTerm;
  gIterTimes = iterTimes;

  gFinishLastApplyIndex = finishLastApplyIndex;
  gFinishLastApplyTerm = finishLastApplyTerm;

  init();
  int32_t ret = syncIOStart((char*)"127.0.0.1", gPorts[myIndex]);
  assert(ret == 0);

  char walPath[128];
  snprintf(walPath, sizeof(walPath), "%s_wal_replica%d_index%d", gDir, replicaNum, myIndex);
  SWal* pWal = createWal(walPath, gVgId);

  int64_t rid = createSyncNode(replicaNum, myIndex, gVgId, pWal, (char*)gDir, isStandBy, enableSnapshot);
  assert(rid > 0);
  syncStart(rid);

  SSyncNode* pSyncNode = (SSyncNode*)syncNodeAcquire(rid);
  assert(pSyncNode != NULL);

  if (isConfigChange > 0) {
    configChange(rid, isConfigChange, myIndex);
  }

  //---------------------------
  int32_t alreadySend = 0;
  int32_t leaderTransferWait = 0;
  while (1) {
    char* simpleStr = syncNode2SimpleStr(pSyncNode);

    leaderTransferWait++;
    if (leaderTransferWait == 7) {
      if (leaderTransfer) {
        sTrace("begin leader transfer ...");
        int32_t ret = syncLeaderTransfer(rid);
      }
    }

    if (alreadySend < writeRecordNum) {
      SRpcMsg* pRpcMsg = createRpcMsg(alreadySend, writeRecordNum, myIndex);
      int32_t  ret = syncPropose(rid, pRpcMsg, false, NULL);
      if (ret == -1 && terrno == TSDB_CODE_SYN_NOT_LEADER) {
        sTrace("%s value%d write not leader, leaderTransferWait:%d", simpleStr, alreadySend, leaderTransferWait);
      } else {
        assert(ret == 0);
        sTrace("%s value%d write ok, leaderTransferWait:%d", simpleStr, alreadySend, leaderTransferWait);
      }
      alreadySend++;

      rpcFreeCont(pRpcMsg->pCont);
      taosMemoryFree(pRpcMsg);
    } else {
      sTrace("%s, leaderTransferWait:%d", simpleStr, leaderTransferWait);
    }

    taosMsleep(1000);
    taosMemoryFree(simpleStr);
    taosMsleep(1000);
  }

  syncNodeRelease(pSyncNode);
  syncStop(rid);
  walClose(pWal);
  syncIOStop();
  cleanup();
  taosCloseLog();
  return 0;
}
