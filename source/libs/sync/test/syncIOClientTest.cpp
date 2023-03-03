#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret;

  ret = syncIOStart((char*)"127.0.0.1", 7010);
  assert(ret == 0);

  for (int i = 0; i < 10; ++i) {
    SEpSet epSet;
    epSet.inUse = 0;
    epSet.numOfEps = 0;
    addEpIntoEpSet(&epSet, "127.0.0.1", 7030);

    SRaftId srcId, destId;
    srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
    srcId.vgId = 100;
    destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
    destId.vgId = 100;

    SyncPingReply* pSyncMsg = syncPingReplyBuild2(&srcId, &destId, 1000, "syncIOClientTest");
    SRpcMsg        rpcMsg;
    syncPingReply2RpcMsg(pSyncMsg, &rpcMsg);

    syncIOSendMsg(&epSet, &rpcMsg);
    taosSsleep(1);
  }

  return 0;
}
