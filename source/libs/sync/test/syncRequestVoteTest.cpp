#include "syncRequestVote.h"
#include "syncTest.h"

static SyncIndex testAppliedIndexCb(const SSyncFSM *pFsm) { return *((SyncIndex *)pFsm->data); }

static SSyncNode createAppliedIndexNode(SyncIndex localAppliedIndex) {
  SSyncNode node = {0};
  SSyncFSM  fsm = {0};

  fsm.data = taosMemoryMalloc(sizeof(SyncIndex));
  *((SyncIndex *)fsm.data) = localAppliedIndex;
  fsm.FpAppliedIndexCb = testAppliedIndexCb;

  node.pFsm = (SSyncFSM *)taosMemoryMalloc(sizeof(SSyncFSM));
  *(node.pFsm) = fsm;
  return node;
}

static void destroyAppliedIndexNode(SSyncNode *pNode) {
  if (pNode->pFsm != NULL) {
    taosMemoryFree(pNode->pFsm->data);
    taosMemoryFree(pNode->pFsm);
    pNode->pFsm = NULL;
  }
}

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SyncRequestVote *createMsg() {
  SyncRequestVote *pMsg = syncRequestVoteBuildWithAppliedIndex(1000, 44);
  pMsg->srcId.addr = syncUtilAddr2U64("127.0.0.1", 1234);
  pMsg->srcId.vgId = 100;
  pMsg->destId.addr = syncUtilAddr2U64("127.0.0.1", 5678);
  pMsg->destId.vgId = 100;
  pMsg->term = 11;
  pMsg->lastLogIndex = 22;
  pMsg->lastLogTerm = 33;
  return pMsg;
}

void test1() {
  SyncRequestVote *pMsg = createMsg();
  syncRequestVoteLog2((char *)"test1:", pMsg);
  syncRequestVoteDestroy(pMsg);
}

void test2() {
  SyncRequestVote *pMsg = createMsg();
  uint32_t         len = pMsg->bytes;
  char            *serialized = (char *)taosMemoryMalloc(len);
  syncRequestVoteSerialize(pMsg, serialized, len);
  SyncRequestVote *pMsg2 = syncRequestVoteBuild(1000);
  syncRequestVoteDeserialize(serialized, len, pMsg2);
  TD_ALWAYS_ASSERT(pMsg2->candidateAppliedIndex == pMsg->candidateAppliedIndex);
  syncRequestVoteLog2((char *)"test2: syncRequestVoteSerialize -> syncRequestVoteDeserialize ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test3() {
  SyncRequestVote *pMsg = createMsg();
  uint32_t         len;
  char            *serialized = syncRequestVoteSerialize2(pMsg, &len);
  SyncRequestVote *pMsg2 = syncRequestVoteDeserialize2(serialized, len);
  TD_ALWAYS_ASSERT(pMsg2->candidateAppliedIndex == pMsg->candidateAppliedIndex);
  syncRequestVoteLog2((char *)"test3: syncRequestVoteSerialize3 -> syncRequestVoteDeserialize2 ", pMsg2);

  taosMemoryFree(serialized);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test4() {
  SyncRequestVote *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVote *pMsg2 = syncRequestVoteBuild(1000);
  syncRequestVoteFromRpcMsg(&rpcMsg, pMsg2);
  TD_ALWAYS_ASSERT(pMsg2->candidateAppliedIndex == pMsg->candidateAppliedIndex);
  syncRequestVoteLog2((char *)"test4: syncRequestVote2RpcMsg -> syncRequestVoteFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test5() {
  SyncRequestVote *pMsg = createMsg();
  SRpcMsg          rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  SyncRequestVote *pMsg2 = syncRequestVoteFromRpcMsg2(&rpcMsg);
  TD_ALWAYS_ASSERT(pMsg2->candidateAppliedIndex == pMsg->candidateAppliedIndex);
  syncRequestVoteLog2((char *)"test5: syncRequestVote2RpcMsg -> syncRequestVoteFromRpcMsg2 ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncRequestVoteDestroy(pMsg);
  syncRequestVoteDestroy(pMsg2);
}

void test6() {
  SSyncNode        node = createAppliedIndexNode(44);
  SyncRequestVote *pMsg = createMsg();
  SyncIndex        localAppliedIndex = SYNC_INDEX_INVALID;

  pMsg->candidateAppliedIndex = 45;
  TD_ALWAYS_ASSERT(syncNodeOnRequestVoteAppliedIndexOK(&node, pMsg, &localAppliedIndex));
  TD_ALWAYS_ASSERT(localAppliedIndex == 44);

  destroyAppliedIndexNode(&node);
  syncRequestVoteDestroy(pMsg);
}

void test7() {
  SSyncNode        node = createAppliedIndexNode(44);
  SyncRequestVote *pMsg = createMsg();
  SyncIndex        localAppliedIndex = SYNC_INDEX_INVALID;

  pMsg->candidateAppliedIndex = 44;
  TD_ALWAYS_ASSERT(syncNodeOnRequestVoteAppliedIndexOK(&node, pMsg, &localAppliedIndex));
  TD_ALWAYS_ASSERT(localAppliedIndex == 44);

  destroyAppliedIndexNode(&node);
  syncRequestVoteDestroy(pMsg);
}

void test8() {
  SSyncNode        node = createAppliedIndexNode(44);
  SyncRequestVote *pMsg = createMsg();
  SyncIndex        localAppliedIndex = SYNC_INDEX_INVALID;

  pMsg->candidateAppliedIndex = 43;
  TD_ALWAYS_ASSERT(!syncNodeOnRequestVoteAppliedIndexOK(&node, pMsg, &localAppliedIndex));
  TD_ALWAYS_ASSERT(localAppliedIndex == 44);

  destroyAppliedIndexNode(&node);
  syncRequestVoteDestroy(pMsg);
}

void test9() {
  SSyncNode        node = createAppliedIndexNode(SYNC_INDEX_INVALID);
  SyncRequestVote *pMsg = createMsg();
  SyncIndex        localAppliedIndex = SYNC_INDEX_INVALID;

  pMsg->candidateAppliedIndex = SYNC_INDEX_INVALID;
  TD_ALWAYS_ASSERT(syncNodeOnRequestVoteAppliedIndexOK(&node, pMsg, &localAppliedIndex));
  TD_ALWAYS_ASSERT(localAppliedIndex == SYNC_INDEX_INVALID);

  destroyAppliedIndexNode(&node);
  syncRequestVoteDestroy(pMsg);
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
  test9();

  return 0;
}
