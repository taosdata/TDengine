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

int          gg = 0;
SyncTimeout *createSyncTimeout() {
  SyncTimeout *pMsg = syncTimeoutBuild2(SYNC_TIMEOUT_PING, 999, 333, 1000, &gg);
  return pMsg;
}

SyncPing *createSyncPing() {
  SRaftId srcId, destId;
  srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  srcId.vgId = 100;
  destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  destId.vgId = 100;
  SyncPing *pMsg = syncPingBuild3(&srcId, &destId, 1000);
  return pMsg;
}

SyncPingReply *createSyncPingReply() {
  SRaftId srcId, destId;
  srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  srcId.vgId = 100;
  destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  destId.vgId = 100;
  SyncPingReply *pMsg = syncPingReplyBuild3(&srcId, &destId, 1000);
  return pMsg;
}

SyncClientRequest *createSyncClientRequest() {
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(rpcMsg));
  rpcMsg.msgType = 12345;
  rpcMsg.contLen = 20;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  strcpy((char *)rpcMsg.pCont, "hello rpc");

  SRpcMsg clientRequestMsg;
  syncBuildClientRequest(&clientRequestMsg, &rpcMsg, 123, true, 1000);
  SyncClientRequest *pMsg = (SyncClientRequest *)taosMemoryMalloc(clientRequestMsg.contLen);
  memcpy(pMsg->data, clientRequestMsg.pCont, clientRequestMsg.contLen);
  return pMsg;
}

SyncRequestVote *createSyncRequestVote() {
  SyncRequestVote *pMsg = syncRequestVoteBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 11;
  pMsg->lastLogIndex = 22;
  pMsg->lastLogTerm = 33;
  return pMsg;
}

SyncRequestVoteReply *createSyncRequestVoteReply() {
  SyncRequestVoteReply *pMsg = syncRequestVoteReplyBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 77;
  pMsg->voteGranted = true;
  return pMsg;
}

SyncAppendEntries *createSyncAppendEntries() {
  SyncAppendEntries *pMsg = syncAppendEntriesBuild(20, 1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->prevLogIndex = 11;
  pMsg->prevLogTerm = 22;
  pMsg->commitIndex = 33;
  strcpy(pMsg->data, "hello world");
  return pMsg;
}

SyncAppendEntriesReply *createSyncAppendEntriesReply() {
  SyncAppendEntriesReply *pMsg = syncAppendEntriesReplyBuild(1000);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->success = true;
  pMsg->matchIndex = 77;
  return pMsg;
}

void test1() {
  SyncTimeout *pMsg = createSyncTimeout();
  SRpcMsg      rpcMsg;
  syncTimeout2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test1", &rpcMsg);
  syncTimeoutDestroy(pMsg);
}

void test2() {
  SyncPing *pMsg = createSyncPing();
  SRpcMsg   rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test2", &rpcMsg);
  syncPingDestroy(pMsg);
}

void test3() {
  SyncPingReply *pMsg = createSyncPingReply();
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test3", &rpcMsg);
  syncPingReplyDestroy(pMsg);
}

void test4() {
  SyncRequestVote *pMsg = createSyncRequestVote();
  SRpcMsg          rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test4", &rpcMsg);
  syncRequestVoteDestroy(pMsg);
}

void test5() {
  SyncRequestVoteReply *pMsg = createSyncRequestVoteReply();
  SRpcMsg               rpcMsg;
  syncRequestVoteReply2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test5", &rpcMsg);
  syncRequestVoteReplyDestroy(pMsg);
}

void test6() {
  SyncAppendEntries *pMsg = createSyncAppendEntries();
  SRpcMsg            rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test6", &rpcMsg);
  syncAppendEntriesDestroy(pMsg);
}

void test7() {
  SyncAppendEntriesReply *pMsg = createSyncAppendEntriesReply();
  SRpcMsg                 rpcMsg;
  syncAppendEntriesReply2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test7", &rpcMsg);
  syncAppendEntriesReplyDestroy(pMsg);
}

void test8() {
#if 0
  SyncClientRequest *pMsg = createSyncClientRequest();
  SRpcMsg            rpcMsg = {0};
  syncClientRequest2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char *)"test8", &rpcMsg);
  taosMemoryFree(pMsg);
#endif
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();
  test6();
  test7();
  test8();

  return 0;
}
