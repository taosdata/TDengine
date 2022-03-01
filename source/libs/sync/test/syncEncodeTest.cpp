#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

#define PING_MSG_LEN 20

int main() {
  // taosInitLog((char*)"syncPingTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  char msg[PING_MSG_LEN];
  snprintf(msg, sizeof(msg), "%s", "test ping");
  SyncPing* pSyncPing = syncPingBuild(PING_MSG_LEN);
  pSyncPing->srcId.addr = 1;
  pSyncPing->srcId.vgId = 2;
  pSyncPing->destId.addr = 3;
  pSyncPing->destId.vgId = 4;
  memcpy(pSyncPing->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPing2Json(pSyncPing);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  uint32_t bufLen = pSyncPing->bytes;
  char*    buf = (char*)malloc(bufLen);
  syncPingSerialize(pSyncPing, buf, bufLen);
  
  SyncPing* pSyncPing2 = (SyncPing*)malloc(pSyncPing->bytes);
  syncPingDeserialize(buf, bufLen, pSyncPing2);

  {
    cJSON* pJson = syncPing2Json(pSyncPing2);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing2: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingDestroy(pSyncPing);
  syncPingDestroy(pSyncPing2);
  free(buf);

  return 0;
}
