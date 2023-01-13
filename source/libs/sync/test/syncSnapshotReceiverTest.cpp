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

void CommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {}
void PreCommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {}
void RollBackCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta) {}

void RestoreFinishCb(struct SSyncFSM* pFsm) {}
void ReConfigCb(struct SSyncFSM* pFsm, SSyncCfg newCfg, SReConfigCbMeta cbMeta) {}

int32_t GetSnapshot(struct SSyncFSM* pFsm, SSnapshot* pSnapshot) { return 0; }

int32_t SnapshotStartRead(struct SSyncFSM* pFsm, void** ppReader) { return 0; }
int32_t SnapshotStopRead(struct SSyncFSM* pFsm, void* pReader) { return 0; }
int32_t SnapshotDoRead(struct SSyncFSM* pFsm, void* pReader, void** ppBuf, int32_t* len) { return 0; }

int32_t SnapshotStartWrite(struct SSyncFSM* pFsm, void* pParam, void** ppWriter) { return 0; }
int32_t SnapshotStopWrite(struct SSyncFSM* pFsm, void* pWriter, bool isApply, SSnapshot* pSnapshot) { return 0; }
int32_t SnapshotDoWrite(struct SSyncFSM* pFsm, void* pWriter, void* pBuf, int32_t len) { return 0; }

SSyncSnapshotReceiver* createReceiver() {
  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(*pSyncNode));
  // pSyncNode->pRaftStore = (SRaftStore*)taosMemoryMalloc(sizeof(*(pSyncNode->pRaftStore)));
  pSyncNode->pFsm = (SSyncFSM*)taosMemoryMalloc(sizeof(*(pSyncNode->pFsm)));

#if 0 
  pSyncNode->pFsm->FpSnapshotStartWrite = SnapshotStartWrite;
  pSyncNode->pFsm->FpSnapshotStopWrite = SnapshotStopWrite;
  pSyncNode->pFsm->FpSnapshotDoWrite = SnapshotDoWrite;
#endif

  SRaftId id;
  id.addr = syncUtilAddr2U64("1.2.3.4", 99);
  id.vgId = 100;

  SSyncSnapshotReceiver* pReceiver = snapshotReceiverCreate(pSyncNode, id);
  pReceiver->start = true;
  pReceiver->ack = 20;
  pReceiver->pWriter = (void*)0x11;
  pReceiver->term = 66;

  return pReceiver;
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  SSyncSnapshotReceiver* pReceiver = createReceiver();
  sTrace("%s", snapshotReceiver2Str(pReceiver));

  return 0;
}
