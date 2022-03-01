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

void test1() {
  sTrace("test1: ---- syncPingSerialize, syncPingDeserialize");

  char msg[PING_MSG_LEN];
  snprintf(msg, sizeof(msg), "%s", "test ping");
  SyncPing* pMsg = syncPingBuild(PING_MSG_LEN);
  pMsg->srcId.addr = 1;
  pMsg->srcId.vgId = 2;
  pMsg->destId.addr = 3;
  pMsg->destId.vgId = 4;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  uint32_t bufLen = pMsg->bytes;
  char*    buf = (char*)malloc(bufLen);
  syncPingSerialize(pMsg, buf, bufLen);

  SyncPing* pMsg2 = (SyncPing*)malloc(pMsg->bytes);
  syncPingDeserialize(buf, bufLen, pMsg2);

  {
    cJSON* pJson = syncPing2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing2: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
  free(buf);
}

void test2() {
  sTrace("test2: ---- syncPing2RpcMsg, syncPingFromRpcMsg");

  char msg[PING_MSG_LEN];
  snprintf(msg, sizeof(msg), "%s", "hello raft");
  SyncPing* pMsg = syncPingBuild(PING_MSG_LEN);
  pMsg->srcId.addr = 100;
  pMsg->srcId.vgId = 200;
  pMsg->destId.addr = 300;
  pMsg->destId.vgId = 400;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  SRpcMsg rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  SyncPing* pMsg2 = (SyncPing*)malloc(pMsg->bytes);
  syncPingFromRpcMsg(&rpcMsg, pMsg2);
  rpcFreeCont(rpcMsg.pCont);

  {
    cJSON* pJson = syncPing2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPing2: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingDestroy(pMsg);
  syncPingDestroy(pMsg2);
}

void test3() {
  sTrace("test3: ---- syncPingReplySerialize, syncPingReplyDeserialize");

  char msg[PING_MSG_LEN];
  snprintf(msg, sizeof(msg), "%s", "test ping");
  SyncPingReply* pMsg = syncPingReplyBuild(PING_MSG_LEN);
  pMsg->srcId.addr = 19;
  pMsg->srcId.vgId = 29;
  pMsg->destId.addr = 39;
  pMsg->destId.vgId = 49;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPingReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPingReply: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  uint32_t bufLen = pMsg->bytes;
  char*    buf = (char*)malloc(bufLen);
  syncPingReplySerialize(pMsg, buf, bufLen);

  SyncPingReply* pMsg2 = (SyncPingReply*)malloc(pMsg->bytes);
  syncPingReplyDeserialize(buf, bufLen, pMsg2);

  {
    cJSON* pJson = syncPingReply2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPingReply2: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
  free(buf);
}

void test4() {
  sTrace("test4: ---- syncPingReply2RpcMsg, syncPingReplyFromRpcMsg");

  char msg[PING_MSG_LEN];
  snprintf(msg, sizeof(msg), "%s", "hello raft");
  SyncPingReply* pMsg = syncPingReplyBuild(PING_MSG_LEN);
  pMsg->srcId.addr = 66;
  pMsg->srcId.vgId = 77;
  pMsg->destId.addr = 88;
  pMsg->destId.vgId = 99;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPingReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPingReply: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  SRpcMsg rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  SyncPingReply* pMsg2 = (SyncPingReply*)malloc(pMsg->bytes);
  syncPingReplyFromRpcMsg(&rpcMsg, pMsg2);
  rpcFreeCont(rpcMsg.pCont);

  {
    cJSON* pJson = syncPingReply2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("SyncPingReply2: \n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}
int main() {
  // taosInitLog((char*)"syncPingTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  test1();
  test2();
  test3();
  test4();

  return 0;
}
