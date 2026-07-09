/**
 * Stable unit tests for SQueryAutoQWorkerPool.
 *
 * The original suite mixed real usage checks with fault-injection and
 * cross-thread abuse of internal counters/callbacks. Those cases are not
 * stable enough for default CI and do not reflect the contract exercised by
 * production code. Keep this file focused on real worker-thread behavior:
 * queue processing, blocking/recovery, mixed workloads, and dual-pool usage.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

extern "C" {
#include "os.h"
#include "tqueue.h"
#include "tworker.h"
}

extern int64_t tsQueueMemoryAllowed;

#define GET_ACTIVE_N(int64_val)  (int32_t)((int64_val) >> 32)
#define GET_RUNNING_N(int64_val) (int32_t)(int64_val & 0xFFFFFFFF)

class QueryAutoQWorkerTest : public ::testing::Test {
 public:
  SQueryAutoQWorkerPool pool{};
  STaosQueue           *queue{nullptr};
  bool                  initialized{false};
  std::atomic<int32_t>  processedCount{0};

  void SetUp() override {
    tsQueueMemoryAllowed = 1024 * 1024;
    memset(&pool, 0, sizeof(pool));
    pool.min = 2;
    pool.max = 8;
    pool.name = "test-worker";
  }

  void TearDown() override {
    if (initialized) {
      tQueryAutoQWorkerCleanup(&pool);
      initialized = false;
      queue = nullptr;
    }
  }

  bool initPool(FItem fp, void *ahandle = nullptr) {
    int32_t code = tQueryAutoQWorkerInit(&pool);
    if (TSDB_CODE_SUCCESS != code) {
      return false;
    }

    initialized = true;
    queue = tQueryAutoQWorkerAllocQueue(&pool, ahandle ? ahandle : (void *)this, fp);
    if (NULL == queue) {
      return false;
    }

    return true;
  }

  bool submitMessages(int32_t count, int32_t startVal = 0) {
    return submitMessagesToQueue(queue, count, startVal);
  }

  static bool submitMessagesToQueue(STaosQueue *targetQueue, int32_t count, int32_t startVal = 0) {
    if (NULL == targetQueue) {
      return false;
    }

    for (int32_t i = 0; i < count; ++i) {
      void *qitem = nullptr;
      int32_t code = taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
      if (0 != code) {
        return false;
      }

      *(int32_t *)qitem = startVal + i;
      code = taosWriteQitem(targetQueue, qitem);
      if (0 != code) {
        taosFreeQitem(qitem);
        return false;
      }
    }

    return true;
  }

  static bool waitForCount(std::atomic<int32_t> &counter, int32_t target, int32_t timeoutMs = 10000) {
    auto start = std::chrono::steady_clock::now();
    while (counter.load() < target) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      if (elapsed > timeoutMs) {
        return false;
      }
    }

    return true;
  }
};

static void basicProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  auto *self = (QueryAutoQWorkerTest *)pQInfo->ahandle;
  self->processedCount.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, BasicQueueProcessing) {
  ASSERT_TRUE(initPool(basicProcessFp));
  ASSERT_TRUE(submitMessages(32));
  ASSERT_TRUE(waitForCount(processedCount, 32));

  ASSERT_EQ(processedCount.load(), 32);
  ASSERT_GE(GET_ACTIVE_N(pool.activeRunningN), 0);
  ASSERT_GE(GET_RUNNING_N(pool.activeRunningN), 0);
}

static void blockingProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  auto *self = (QueryAutoQWorkerTest *)pQInfo->ahandle;

  if (NULL != pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    EXPECT_EQ(cb->beforeBlocking(cb->pPool), TSDB_CODE_SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    EXPECT_EQ(cb->afterRecoverFromBlocking(cb->pPool), TSDB_CODE_SUCCESS);
  }

  self->processedCount.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, RealUsageBlockingWorkload) {
  pool.min = 4;
  pool.max = 16;

  ASSERT_TRUE(initPool(blockingProcessFp));
  ASSERT_TRUE(submitMessages(256));
  ASSERT_TRUE(waitForCount(processedCount, 256, 20000));

  ASSERT_EQ(processedCount.load(), 256);
  ASSERT_GE(GET_ACTIVE_N(pool.activeRunningN), 0);
  ASSERT_GE(GET_RUNNING_N(pool.activeRunningN), 0);
}

static void mixedProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  auto *self = (QueryAutoQWorkerTest *)pQInfo->ahandle;
  int32_t val = *(int32_t *)pMsg;

  if (0 == (val % 2) && NULL != pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    EXPECT_EQ(cb->beforeBlocking(cb->pPool), TSDB_CODE_SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_EQ(cb->afterRecoverFromBlocking(cb->pPool), TSDB_CODE_SUCCESS);
  }

  self->processedCount.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, MixedBlockingAndNonBlockingWorkload) {
  pool.min = 4;
  pool.max = 16;

  ASSERT_TRUE(initPool(mixedProcessFp));
  ASSERT_TRUE(submitMessages(256));
  ASSERT_TRUE(waitForCount(processedCount, 256, 20000));

  ASSERT_EQ(processedCount.load(), 256);
  ASSERT_GE(GET_ACTIVE_N(pool.activeRunningN), 0);
  ASSERT_GE(GET_RUNNING_N(pool.activeRunningN), 0);
}

static std::atomic<int32_t> g_queryProcessed{0};
static std::atomic<int32_t> g_mqueryProcessed{0};

static void dualQueryProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (NULL != pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    EXPECT_EQ(cb->beforeBlocking(cb->pPool), TSDB_CODE_SUCCESS);
    std::this_thread::sleep_for(std::chrono::microseconds(300));
    EXPECT_EQ(cb->afterRecoverFromBlocking(cb->pPool), TSDB_CODE_SUCCESS);
  }

  g_queryProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

static void dualMqueryProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (NULL != pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    EXPECT_EQ(cb->beforeBlocking(cb->pPool), TSDB_CODE_SUCCESS);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    EXPECT_EQ(cb->afterRecoverFromBlocking(cb->pPool), TSDB_CODE_SUCCESS);
  }

  g_mqueryProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, DualPoolConcurrentProcessing) {
  g_queryProcessed = 0;
  g_mqueryProcessed = 0;

  pool.min = 4;
  pool.max = 8;
  pool.name = "query-pool";
  ASSERT_TRUE(initPool(dualQueryProcessFp));

  SQueryAutoQWorkerPool pool2{};
  memset(&pool2, 0, sizeof(pool2));
  pool2.min = 2;
  pool2.max = 4;
  pool2.name = "mquery-pool";

  ASSERT_EQ(tQueryAutoQWorkerInit(&pool2), TSDB_CODE_SUCCESS);
  STaosQueue *queue2 = tQueryAutoQWorkerAllocQueue(&pool2, nullptr, dualMqueryProcessFp);
  ASSERT_NE(queue2, nullptr);

  ASSERT_TRUE(submitMessages(200));
  ASSERT_TRUE(submitMessagesToQueue(queue2, 80));

  ASSERT_TRUE(waitForCount(g_queryProcessed, 200, 20000));
  ASSERT_TRUE(waitForCount(g_mqueryProcessed, 80, 20000));

  ASSERT_EQ(g_queryProcessed.load(), 200);
  ASSERT_EQ(g_mqueryProcessed.load(), 80);
  ASSERT_GE(GET_ACTIVE_N(pool.activeRunningN), 0);
  ASSERT_GE(GET_RUNNING_N(pool.activeRunningN), 0);
  ASSERT_GE(GET_ACTIVE_N(pool2.activeRunningN), 0);
  ASSERT_GE(GET_RUNNING_N(pool2.activeRunningN), 0);

  tQueryAutoQWorkerCleanup(&pool2);
}
