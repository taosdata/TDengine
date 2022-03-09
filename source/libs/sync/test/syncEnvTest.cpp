#include "syncEnv.h"
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"
#include "ttime.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

void *pTimer = NULL;
void *pTimerMgr = NULL;
int   g = 300;

static void timerFp(void *param, void *tmrId) {
  printf("param:%p, tmrId:%p, pTimer:%p, pTimerMgr:%p \n", param, tmrId, pTimer, pTimerMgr);
  taosTmrReset(timerFp, 1000, param, pTimerMgr, &pTimer);
}

int main() {
  // taosInitLog((char*)"syncEnvTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  int32_t ret;

  logTest();

  ret = syncEnvStart();
  assert(ret == 0);

  // timer
  pTimerMgr = taosTmrInit(1000, 50, 10000, "SYNC-ENV-TEST");
  taosTmrStart(timerFp, 1000, &g, pTimerMgr);

  while (1) {
    taosMsleep(1000);
  }

  return 0;
}
