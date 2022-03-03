#include <stdio.h>
#include "gtest/gtest.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"

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

  ret = syncIOStart((char *)"127.0.0.1", 7010);
  assert(ret == 0);

  for (int i = 0; i < 10; ++i) {
    SEpSet epSet;
    epSet.inUse = 0;
    epSet.numOfEps = 0;
    addEpIntoEpSet(&epSet, "127.0.0.1", 7010);

    SRpcMsg rpcMsg;
    rpcMsg.contLen = 64;
    rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
    snprintf((char *)rpcMsg.pCont, rpcMsg.contLen, "%s", "syncIOSendMsgTest");
    rpcMsg.handle = NULL;
    rpcMsg.msgType = 77;

    syncIOSendMsg(gSyncIO->clientRpc, &epSet, &rpcMsg);
    sleep(1);
  }

  while (1) {
    sleep(1);
  }

  return 0;
}
