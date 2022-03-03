#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"

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
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1111);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 2222);
  pMsg->destId.vgId = 100;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
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
    printf("\n%s\n\n", serialized);
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
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 3333);
  pMsg->srcId.vgId = 200;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 4444);
  pMsg->destId.vgId = 200;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
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
    printf("\n%s\n\n", serialized);
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
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 5555);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 6666);
  pMsg->destId.vgId = 100;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPingReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
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
    printf("\n%s\n\n", serialized);
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
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 7777);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 8888);
  pMsg->destId.vgId = 100;
  memcpy(pMsg->data, msg, PING_MSG_LEN);

  {
    cJSON* pJson = syncPingReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
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
    printf("\n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncPingReplyDestroy(pMsg);
  syncPingReplyDestroy(pMsg2);
}

void test5() {
  sTrace("test5: ---- syncRequestVoteSerialize, syncRequestVoteDeserialize");

  SyncRequestVote* pMsg = syncRequestVoteBuild();
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("8.8.8.8", 5678);
  pMsg->destId.vgId = 100;
  pMsg->currentTerm = 20;
  pMsg->lastLogIndex = 21;
  pMsg->lastLogTerm = 22;

  {
    cJSON* pJson = syncRequestVote2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  uint32_t bufLen = pMsg->bytes;
  char*    buf = (char*)malloc(bufLen);
  syncRequestVoteSerialize(pMsg, buf, bufLen);

  SyncRequestVote* pMsg2 = (SyncRequestVote*)malloc(pMsg->bytes);
  syncRequestVoteDeserialize(buf, bufLen, pMsg2);

  {
    cJSON* pJson = syncRequestVote2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
  free(buf);
}

void test6() {
  sTrace("test6: ---- syncRequestVoteReplySerialize, syncRequestVoteReplyDeserialize");

  SyncRequestVoteReply* pMsg = SyncRequestVoteReplyBuild();
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("8.8.8.8", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 20;
  pMsg->voteGranted = 1;

  {
    cJSON* pJson = syncRequestVoteReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  uint32_t bufLen = pMsg->bytes;
  char*    buf = (char*)malloc(bufLen);
  syncRequestVoteReplySerialize(pMsg, buf, bufLen);

  SyncRequestVoteReply* pMsg2 = (SyncRequestVoteReply*)malloc(pMsg->bytes);
  syncRequestVoteReplyDeserialize(buf, bufLen, pMsg2);

  {
    cJSON* pJson = syncRequestVoteReply2Json(pMsg2);
    char*  serialized = cJSON_Print(pJson);
    printf("\n%s\n\n", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  syncRequestVoteReplyDestroy(pMsg);
  syncRequestVoteReplyDestroy(pMsg2);
  free(buf);
}

int main() {
  // taosInitLog((char*)"syncPingTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  test1();
  test2();
  test3();
  test4();
  test5();
  test6();

  return 0;
}
