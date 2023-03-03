#include "syncRaftStore.h"
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SRaftCfg* createRaftCfg() {
  SRaftCfg* pCfg = (SRaftCfg*)taosMemoryMalloc(sizeof(SRaftCfg));
  memset(pCfg, 0, sizeof(SRaftCfg));

  pCfg->cfg.replicaNum = 3;
  pCfg->cfg.myIndex = 1;
  for (int i = 0; i < pCfg->cfg.replicaNum; ++i) {
    ((pCfg->cfg.nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pCfg->cfg.nodeInfo)[i]).nodeFqdn, sizeof(((pCfg->cfg.nodeInfo)[i]).nodeFqdn), "100.200.300.%d", i);
  }
  pCfg->isStandBy = taosGetTimestampSec() % 100;
  pCfg->batchSize = taosGetTimestampSec() % 100;

  pCfg->configIndexCount = 5;
  for (int i = 0; i < MAX_CONFIG_INDEX_COUNT; ++i) {
    (pCfg->configIndexArr)[i] = -1;
  }
  for (int i = 0; i < pCfg->configIndexCount; ++i) {
    (pCfg->configIndexArr)[i] = i * 100;
  }

  return pCfg;
}

SSyncCfg* createSyncCfg() {
  SSyncCfg* pCfg = (SSyncCfg*)taosMemoryMalloc(sizeof(SSyncCfg));
  memset(pCfg, 0, sizeof(SSyncCfg));

  pCfg->replicaNum = 3;
  pCfg->myIndex = 1;
  for (int i = 0; i < pCfg->replicaNum; ++i) {
    ((pCfg->nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pCfg->nodeInfo)[i]).nodeFqdn), "100.200.300.%d", i);
  }

  return pCfg;
}

const char* pFile = "./raft_config_index.json";

void test1() {
  // int32_t code = raftCfgIndexCreateFile(pFile);
  // ASSERT(code == 0);

  // SRaftCfgIndex* pRaftCfgIndex = raftCfgIndexOpen(pFile);

  // raftCfgIndexClose(pRaftCfgIndex);
}

void test2() {
  // SRaftCfgIndex* pRaftCfgIndex = raftCfgIndexOpen(pFile);
  // for (int i = 0; i < 500; ++i) {
  //   raftCfgIndexAddConfigIndex(pRaftCfgIndex, i);
  // }
  // raftCfgIndexPersist(pRaftCfgIndex);
  // raftCfgIndexClose(pRaftCfgIndex);
}

void test3() {
  // SRaftCfgIndex* pRaftCfgIndex = raftCfgIndexOpen(pFile);
  // raftCfgIndexClose(pRaftCfgIndex);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  logTest();
  test1();
  test2();
  test3();

  return 0;
}
