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

int32_t SnapshotStartRead(struct SSyncFSM* pFsm, void* pParam, void** ppReader) { return 0; }
int32_t SnapshotStopRead(struct SSyncFSM* pFsm, void* pReader) { return 0; }
int32_t SnapshotDoRead(struct SSyncFSM* pFsm, void* pReader, void** ppBuf, int32_t* len) { return 0; }

int32_t SnapshotStartWrite(struct SSyncFSM* pFsm, void** ppWriter) { return 0; }
int32_t SnapshotStopWrite(struct SSyncFSM* pFsm, void* pWriter, bool isApply) { return 0; }
int32_t SnapshotDoWrite(struct SSyncFSM* pFsm, void* pWriter, void* pBuf, int32_t len) { return 0; }

SSyncSnapshotSender* createSender() {
  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(*pSyncNode));
  // pSyncNode->pRaftStore = (SRaftStore*)taosMemoryMalloc(sizeof(*(pSyncNode->pRaftStore)));
  pSyncNode->pFsm = (SSyncFSM*)taosMemoryMalloc(sizeof(*(pSyncNode->pFsm)));

#if 0 
  pSyncNode->pFsm->FpSnapshotStartRead = SnapshotStartRead;
  pSyncNode->pFsm->FpSnapshotStopRead = SnapshotStopRead;
  pSyncNode->pFsm->FpSnapshotDoRead = SnapshotDoRead;
  pSyncNode->pFsm->FpGetSnapshotInfo = GetSnapshot;
#endif

  SSyncSnapshotSender* pSender = snapshotSenderCreate(pSyncNode, 2);
  pSender->start = true;
  pSender->seq = 10;
  pSender->ack = 20;
  pSender->pReader = (void*)0x11;
  pSender->blockLen = 20;
  pSender->pCurrentBlock = taosMemoryMalloc(pSender->blockLen);
  snprintf((char*)(pSender->pCurrentBlock), pSender->blockLen, "%s", "hello");

  pSender->snapshot.lastApplyIndex = 99;
  pSender->snapshot.lastApplyTerm = 88;
  pSender->sendingMS = 77;
  pSender->term = 66;

  // pSender->privateTerm = 99;

  return pSender;
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  SSyncSnapshotSender* pSender = createSender();
  sTrace("%s", snapshotSender2Str(pSender));

  return 0;
}
