#include "syncElection.h"
#include "syncTest.h"

extern "C" void syncUtilMsgNtoH(void* msg);

namespace {

constexpr int32_t   kReplicaNum = 3;
constexpr int32_t   kVgId = 1234;
constexpr int32_t   kVnodeVersion = 1;
constexpr int32_t   kElectIntervalMs = 60000;
constexpr int32_t   kHeartbeatIntervalMs = 60000;
constexpr SyncIndex kSnapshotIndex = 0;
constexpr SyncTerm  kSnapshotTerm = 1;

uint16_t  gPorts[kReplicaNum] = {7010, 7110, 7210};
SyncIndex gAppliedIndexes[kReplicaNum] = {0, 1, 2};

struct ElectTestFsm {
  SSyncFSM  fsm;
  SyncIndex appliedIndex;
};

struct ElectCluster {
  SSyncNode* nodes[kReplicaNum];
  SWal*      wals[kReplicaNum];
  char       baseDir[TSDB_FILENAME_LEN];
};

ElectCluster gCluster = {};

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

static SyncIndex electAppliedIndexCb(const SSyncFSM* pFsm) { return ((const ElectTestFsm*)pFsm)->appliedIndex; }

static bool electApplyQueueEmptyCb(const SSyncFSM* pFsm) {
  TAOS_UNUSED(pFsm);
  return true;
}

static int32_t electApplyQueueItemsCb(const SSyncFSM* pFsm) {
  TAOS_UNUSED(pFsm);
  return 0;
}

static int32_t electSnapshotInfoCb(const SSyncFSM* pFsm, SSnapshot* pSnapshot) {
  TAOS_UNUSED(pFsm);
  memset(pSnapshot, 0, sizeof(*pSnapshot));
  pSnapshot->state = SYNC_FSM_STATE_COMPLETE;
  pSnapshot->lastApplyIndex = kSnapshotIndex;
  pSnapshot->lastApplyTerm = kSnapshotTerm;
  pSnapshot->lastConfigIndex = SYNC_INDEX_INVALID;
  return 0;
}

static ElectTestFsm* createElectFsm(SyncIndex appliedIndex) {
  ElectTestFsm* pFsm = (ElectTestFsm*)taosMemoryCalloc(1, sizeof(ElectTestFsm));
  TD_ALWAYS_ASSERT(pFsm != NULL);

  pFsm->appliedIndex = appliedIndex;
  pFsm->fsm.data = pFsm;
  pFsm->fsm.FpAppliedIndexCb = electAppliedIndexCb;
  pFsm->fsm.FpApplyQueueEmptyCb = electApplyQueueEmptyCb;
  pFsm->fsm.FpApplyQueueItems = electApplyQueueItemsCb;
  pFsm->fsm.FpGetSnapshotInfo = electSnapshotInfoCb;
  return pFsm;
}

static void initWalEnv() {
  int32_t code = walInit(NULL);
  TD_ALWAYS_ASSERT(code == 0);
}

static void cleanupWalEnv() { walCleanUp(); }

static SWal* createWal(const char* path, int32_t vgId) {
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(walCfg));
  walCfg.vgId = vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;

  SWal* pWal = walOpen(path, &walCfg);
  TD_ALWAYS_ASSERT(pWal != NULL);
  return pWal;
}

static int32_t localEqMsg(const SMsgCb* msgcb, SRpcMsg* pMsg) {
  TAOS_UNUSED(msgcb);

  if (pMsg->pCont != NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }

  return 0;
}

static SSyncNode* findNodeByPort(uint16_t port) {
  for (int32_t i = 0; i < kReplicaNum; ++i) {
    if (gCluster.nodes[i] != NULL && gCluster.nodes[i]->myNodeInfo.nodePort == port) {
      return gCluster.nodes[i];
    }
  }

  return NULL;
}

static int32_t dispatchLocalMsg(SSyncNode* pNode, SRpcMsg* pMsg) {
  switch (pMsg->msgType) {
    case TDMT_SYNC_REQUEST_VOTE:
      return syncNodeOnRequestVote(pNode, pMsg);
    case TDMT_SYNC_REQUEST_VOTE_REPLY:
      return syncNodeOnRequestVoteReply(pNode, pMsg);
    default:
      return 0;
  }
}

static int32_t localSendMsg(const SEpSet* pEpSet, SRpcMsg* pMsg) {
  SSyncNode* pTarget = NULL;
  int32_t    code = 0;

  if (pEpSet != NULL && pEpSet->numOfEps > 0) {
    pTarget = findNodeByPort(pEpSet->eps[0].port);
  }

  syncUtilMsgNtoH(pMsg->pCont);

  if (pTarget != NULL) {
    code = dispatchLocalMsg(pTarget, pMsg);
  } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE || pMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
    code = -1;
  }

  if (pMsg->pCont != NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }

  return code;
}

static SSyncNode* createSyncNode(int32_t myIndex, SyncIndex appliedIndex) {
  SSyncInfo syncInfo = {};
  syncInfo.vgId = kVgId;
  syncInfo.msgcb = NULL;
  syncInfo.syncSendMSg = localSendMsg;
  syncInfo.syncEqMsg = localEqMsg;
  syncInfo.syncEqCtrlMsg = localEqMsg;
  syncInfo.electMs = kElectIntervalMs;
  syncInfo.heartbeatMs = kHeartbeatIntervalMs;
  syncInfo.pFsm = &createElectFsm(appliedIndex)->fsm;

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s/node%d", gCluster.baseDir, myIndex);

  char walPath[TSDB_FILENAME_LEN] = {0};
  snprintf(walPath, sizeof(walPath), "%s/wal%d", gCluster.baseDir, myIndex);
  gCluster.wals[myIndex] = createWal(walPath, kVgId);
  syncInfo.pWal = gCluster.wals[myIndex];

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = kReplicaNum;
  pCfg->totalReplicaNum = kReplicaNum;
  pCfg->changeVersion = kVnodeVersion;

  for (int32_t i = 0; i < kReplicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = gPorts[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
  }

  SSyncNode* pSyncNode = syncNodeOpen(&syncInfo, kVnodeVersion, kElectIntervalMs, kHeartbeatIntervalMs);
  TD_ALWAYS_ASSERT(pSyncNode != NULL);

  int32_t code = syncNodeStart(pSyncNode);
  TD_ALWAYS_ASSERT(code == 0);

  code = syncNodeStopPingTimer(pSyncNode);
  TD_ALWAYS_ASSERT(code == 0);

  return pSyncNode;
}

static void initCluster() {
  memset(&gCluster, 0, sizeof(gCluster));
  snprintf(gCluster.baseDir, sizeof(gCluster.baseDir), "/tmp/syncElectTest-%" PRId64, taosGetTimestampMs());
  taosRemoveDir(gCluster.baseDir);
  TD_ALWAYS_ASSERT(taosMulMkDir(gCluster.baseDir) == 0);

  for (int32_t i = 0; i < kReplicaNum; ++i) {
    gCluster.nodes[i] = createSyncNode(i, gAppliedIndexes[i]);
    TD_ALWAYS_ASSERT(gCluster.nodes[i] != NULL);
    TD_ALWAYS_ASSERT(syncNodeGetAppliedIndex(gCluster.nodes[i]) == gAppliedIndexes[i]);
  }
}

static void cleanupCluster() {
  for (int32_t i = 0; i < kReplicaNum; ++i) {
    if (gCluster.nodes[i] != NULL) {
      syncNodeClose(gCluster.nodes[i]);
      gCluster.nodes[i] = NULL;
    }

    if (gCluster.wals[i] != NULL) {
      walClose(gCluster.wals[i]);
      gCluster.wals[i] = NULL;
    }
  }

  if (gCluster.baseDir[0] != '\0') {
    taosRemoveDir(gCluster.baseDir);
  }
}

static void testAppliedIndexElectionPreference() {
  SSyncNode* lowAppliedNode = gCluster.nodes[0];
  SSyncNode* highAppliedNode = gCluster.nodes[2];

  TD_ALWAYS_ASSERT(syncNodeElect(lowAppliedNode) == 0);
  TD_ALWAYS_ASSERT(lowAppliedNode->state == TAOS_SYNC_STATE_CANDIDATE);
  TD_ALWAYS_ASSERT(highAppliedNode->state != TAOS_SYNC_STATE_LEADER);

  TD_ALWAYS_ASSERT(syncNodeElect(highAppliedNode) == 0);
  TD_ALWAYS_ASSERT(highAppliedNode->state == TAOS_SYNC_STATE_LEADER);
  TD_ALWAYS_ASSERT(lowAppliedNode->state != TAOS_SYNC_STATE_LEADER);
}

}  // namespace

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  logTest();
  initWalEnv();

  int32_t ret = syncInit();
  TD_ALWAYS_ASSERT(ret == 0);

  initCluster();
  testAppliedIndexElectionPreference();
  cleanupCluster();

  syncCleanUp();
  cleanupWalEnv();
  return 0;
}
