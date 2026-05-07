#include <atomic>
#include <cstring>

#include "gtest/gtest.h"

extern "C" {
#include "clientInt.h"
#include "query.h"
#include "tmsg.h"
}

namespace {

std::atomic<bool> g_failNextTxnVgListInit{false};
std::atomic<int>  g_rollbackReqCount{0};
std::atomic<int32_t> g_beginReqCode{TSDB_CODE_SUCCESS};
std::atomic<int32_t> g_beginRspCode{TSDB_CODE_SUCCESS};

struct FakeTxnEnv {
  int64_t     connId = 0x12345678;
  STscObj     tscObj = {0};
  SAppInstInfo appInfo = {0};

  FakeTxnEnv() {
    tscObj.id = connId;
    tscObj.pAppInfo = &appInfo;
    appInfo.pTransporter = reinterpret_cast<void*>(0x1);
    EXPECT_EQ(taosThreadMutexInit(&tscObj.mutex, NULL), 0);
  }

  ~FakeTxnEnv() {
    taosArrayDestroy(tscObj.pTxnVgList);
    tscObj.pTxnVgList = NULL;
    taosThreadMutexDestroy(&tscObj.mutex);
  }

  TAOS* taos() { return reinterpret_cast<TAOS*>(&connId); }
};

FakeTxnEnv* g_fakeTxnEnv = nullptr;

class ScopedTxnVgListInitFailure {
 public:
  ScopedTxnVgListInitFailure() { g_failNextTxnVgListInit.store(true); }
  ~ScopedTxnVgListInitFailure() { g_failNextTxnVgListInit.store(false); }
};

void expectTxnStateCleared(const STscObj& tscObj) {
  EXPECT_EQ(tscObj.txnState, 0);
  EXPECT_EQ(tscObj.txnId, 0);
  EXPECT_EQ(tscObj.pTxnVgList, nullptr);
}

void buildBeginTxnRsp(void** pCont, int32_t* pContLen, int64_t txnId) {
  SMTransReq rsp = {0};
  rsp.msgType = TDMT_MND_BEGIN_TXN;
  rsp.txnId = txnId;
  rsp.connId = g_fakeTxnEnv->connId;

  int32_t len = tSerializeSMTransReq(NULL, 0, &rsp);
  ASSERT_GT(len, 0);

  void* buf = taosMemoryCalloc(1, len);
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(tSerializeSMTransReq(buf, len, &rsp), len);

  *pCont = buf;
  *pContLen = len;
}

}  // namespace

#ifdef __linux__  // --wrap= interception is GNU ld (Linux) only
extern "C" {

SArray* __real_taosArrayInit(size_t size, size_t elemSize);
void __real_rpcFreeCont(void* pCont);

STscObj* __wrap_acquireTscObj(int64_t rid) {
  if (g_fakeTxnEnv != nullptr && rid == g_fakeTxnEnv->connId) {
    return &g_fakeTxnEnv->tscObj;
  }
  return NULL;
}

void __wrap_releaseTscObj(int64_t rid) {
  (void)rid;
}

int32_t __wrap_rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pReq, SRpcMsg* pRsp) {
  (void)shandle;
  (void)pEpSet;
  if (pReq != NULL && pReq->msgType == TDMT_MND_ROLLBACK_TXN) {
    g_rollbackReqCount.fetch_add(1);
    if (pReq->pCont != NULL) {
      __real_rpcFreeCont(pReq->pCont);
      pReq->pCont = NULL;
    }
    pRsp->pCont = NULL;
    pRsp->contLen = 0;
    pRsp->code = TSDB_CODE_SUCCESS;
    pRsp->msgType = TDMT_MND_ROLLBACK_TXN;
    return TSDB_CODE_SUCCESS;
  }

  if (pReq != NULL && pReq->pCont != NULL) {
    __real_rpcFreeCont(pReq->pCont);
    pReq->pCont = NULL;
  }

  if (g_beginReqCode.load() != TSDB_CODE_SUCCESS) {
    pRsp->pCont = NULL;
    pRsp->contLen = 0;
    pRsp->msgType = TDMT_MND_BEGIN_TXN;
    return g_beginReqCode.load();
  }

  buildBeginTxnRsp(&pRsp->pCont, &pRsp->contLen, 9527);
  pRsp->code = g_beginRspCode.load();
  pRsp->msgType = TDMT_MND_BEGIN_TXN;
  return TSDB_CODE_SUCCESS;
}

void __wrap_rpcFreeCont(void* pCont) {
  taosMemoryFree(pCont);
}

SArray* __wrap_taosArrayInit(size_t size, size_t elemSize) {
  if (g_failNextTxnVgListInit.exchange(false) && size == 4 && elemSize == sizeof(int32_t)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  return __real_taosArrayInit(size, elemSize);
}

}  // extern "C"
#endif  // __linux__

TEST(txnStateCase, cApiBeginClearsLocalStateWhenVgListInitFails) {
#if defined(TD_ENTERPRISE) && defined(__linux__)
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;
  g_rollbackReqCount.store(0);
  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);

  {
    ScopedTxnVgListInitFailure scopedFailure;
    int32_t code = taos_txn_begin(env.taos());
    EXPECT_NE(code, TSDB_CODE_SUCCESS);
  }

  expectTxnStateCleared(env.tscObj);
  EXPECT_EQ(g_rollbackReqCount.load(), 1);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "requires Enterprise on Linux (uses --wrap mocks)";
#endif
}

TEST(txnStateCase, sqlBeginClearsLocalStateWhenVgListInitFails) {
#if defined(TD_ENTERPRISE) && defined(__linux__)
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;
  g_rollbackReqCount.store(0);
  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);

  SRequestObj request = {0};
  request.pTscObj = &env.tscObj;
  ASSERT_EQ(tsem_init(&request.body.rspSem, 0, 0), 0);

  SDataBuf msg = {0};
  buildBeginTxnRsp(&msg.pData, reinterpret_cast<int32_t*>(&msg.len), 9528);
  msg.msgType = TDMT_MND_BEGIN_TXN;
  msg.pEpSet = reinterpret_cast<SEpSet*>(taosMemoryCalloc(1, sizeof(SEpSet)));
  ASSERT_NE(msg.pEpSet, nullptr);

  __async_send_cb_fn_t handler = getMsgRspHandle(TDMT_MND_BEGIN_TXN);
  ASSERT_NE(handler, nullptr);

  int32_t code = TSDB_CODE_SUCCESS;
  {
    ScopedTxnVgListInitFailure scopedFailure;
    code = handler(&request, &msg, TSDB_CODE_SUCCESS);
  }

  EXPECT_NE(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(request.code, code);
  EXPECT_EQ(tsem_wait(&request.body.rspSem), 0);
  expectTxnStateCleared(env.tscObj);
  EXPECT_EQ(g_rollbackReqCount.load(), 1);

  tsem_destroy(&request.body.rspSem);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "requires Enterprise on Linux (uses --wrap mocks)";
#endif
}

TEST(txnStateCase, cApiBeginRequestFailureDoesNotSendRollback) {
#if defined(TD_ENTERPRISE) && defined(__linux__)
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;
  g_rollbackReqCount.store(0);
  g_beginReqCode.store(TSDB_CODE_OUT_OF_MEMORY);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);

  int32_t code = taos_txn_begin(env.taos());
  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  expectTxnStateCleared(env.tscObj);
  EXPECT_EQ(g_rollbackReqCount.load(), 0);

  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "requires Enterprise on Linux (uses --wrap mocks)";
#endif
}

// =========================================================================
// 4. Server-side BEGIN failure (rspCode != 0) does not corrupt state
// =========================================================================
TEST(txnStateCase, cApiBeginServerFailureDoesNotCorruptState) {
#if defined(TD_ENTERPRISE) && defined(__linux__)
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;
  g_rollbackReqCount.store(0);
  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_MND_TXN_FULL);  // Server rejects: too many txns

  int32_t code = taos_txn_begin(env.taos());
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
  expectTxnStateCleared(env.tscObj);
  // No rollback needed since server rejected the BEGIN
  EXPECT_EQ(g_rollbackReqCount.load(), 0);

  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "requires Enterprise on Linux (uses --wrap mocks)";
#endif
}

// =========================================================================
// 5. Double BEGIN: already-in-progress txn is rejected locally
// =========================================================================
TEST(txnStateCase, cApiDoubleBeginRejected) {
#if defined(TD_ENTERPRISE) && defined(__linux__)
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;
  g_rollbackReqCount.store(0);
  g_beginReqCode.store(TSDB_CODE_SUCCESS);
  g_beginRspCode.store(TSDB_CODE_SUCCESS);

  // First BEGIN should succeed
  int32_t code1 = taos_txn_begin(env.taos());
  EXPECT_EQ(code1, TSDB_CODE_SUCCESS);
  EXPECT_NE(env.tscObj.txnId, 0);

  // Second BEGIN on same connection should fail
  int32_t code2 = taos_txn_begin(env.taos());
  EXPECT_EQ(code2, TSDB_CODE_TXN_ALREADY_IN_PROGRESS);

  // Original txn state should be untouched
  EXPECT_NE(env.tscObj.txnId, 0);

  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "batch meta txn is enterprise-only";
#endif
}