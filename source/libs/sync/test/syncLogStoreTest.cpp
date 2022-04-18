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

SSyncNode*     pSyncNode;
SWal*          pWal;
SSyncLogStore* pLogStore;
const char*    pWalPath = "./syncLogStoreTest_wal";

void init() {
  walInit();
  taosRemoveDir(pWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  pWal = walOpen(pWalPath, &walCfg);
  assert(pWal != NULL);

  pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;
}

void cleanup() {
  walClose(pWal);
  walCleanUp();
  taosMemoryFree(pSyncNode);
}

void logStoreTest() {
  pLogStore = logStoreCreate(pSyncNode);
  assert(pLogStore);
  assert(pLogStore->getLastIndex(pLogStore) == SYNC_INDEX_INVALID);

  logStoreLog2((char*)"logStoreTest", pLogStore);

  for (int i = 0; i < 5; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100 + i;
    pEntry->index = pLogStore->getLastIndex(pLogStore) + 1;
    snprintf(pEntry->data, dataLen, "value%d", i);

    syncEntryLog2((char*)"==write entry== :", pEntry);
    pLogStore->appendEntry(pLogStore, pEntry);
    syncEntryDestory(pEntry);

    if (i == 0) {
      assert(pLogStore->getLastIndex(pLogStore) == SYNC_INDEX_BEGIN);
    }
  }
  logStoreLog2((char*)"after appendEntry", pLogStore);

  pLogStore->truncate(pLogStore, 3);
  logStoreLog2((char*)"after truncate 3", pLogStore);

  logStoreDestory(pLogStore);
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  init();
  logStoreTest();

  taosMsleep(2000);
  cleanup();

  return 0;
}
