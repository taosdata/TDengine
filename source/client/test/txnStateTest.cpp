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

struct FakeTxnEnv {
  int64_t     connId = 0x12345678;
  STscObj     tscObj = {0};
  SAppInstInfo appInfo = {0};

  FakeTxnEnv() {
    tscObj.id = connId;
    tscObj.pAppInfo = &appInfo;
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
  if (pReq != NULL && pReq->pCont != NULL) {
    __real_rpcFreeCont(pReq->pCont);
    pReq->pCont = NULL;
  }

  buildBeginTxnRsp(&pRsp->pCont, &pRsp->contLen, 9527);
  pRsp->code = TSDB_CODE_SUCCESS;
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

TEST(txnStateCase, cApiBeginClearsLocalStateWhenVgListInitFails) {
#ifdef TD_ENTERPRISE
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;

  {
    ScopedTxnVgListInitFailure scopedFailure;
    int32_t code = taos_txn_begin(env.taos());
    EXPECT_NE(code, TSDB_CODE_SUCCESS);
  }

  expectTxnStateCleared(env.tscObj);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "batch meta txn is enterprise-only";
#endif
}

TEST(txnStateCase, sqlBeginClearsLocalStateWhenVgListInitFails) {
#ifdef TD_ENTERPRISE
  FakeTxnEnv env;
  g_fakeTxnEnv = &env;

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

  tsem_destroy(&request.body.rspSem);
  g_fakeTxnEnv = nullptr;
#else
  GTEST_SKIP() << "batch meta txn is enterprise-only";
#endif
}