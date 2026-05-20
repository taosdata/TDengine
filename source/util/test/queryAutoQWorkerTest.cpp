/**
 * Unit tests for SQueryAutoQWorkerPool self-healing mechanisms.
 *
 * Tests verify that the worker pool can:
 * 1. Prevent running counter from going negative (atomicSafeDecRunning)
 * 2. Detect and correct negative running counters (healRunning)
 * 3. Survive cross-pool beforeBlocking/afterRecoverFromBlocking abuse
 * 4. Continue processing messages after self-healing
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
#define MAKE_ACTIVE_RUNNING(active, running) (((int64_t)(active) << 32) | ((int64_t)(uint32_t)(running)))

// ============================================================================
// Test fixture
// ============================================================================

class QueryAutoQWorkerTest : public ::testing::Test {
 public:
  SQueryAutoQWorkerPool pool{};
  STaosQueue           *queue{nullptr};
  std::atomic<int32_t>  processedCount{0};
  std::atomic<int32_t>  blockingCallCount{0};

  void SetUp() override {
    tsQueueMemoryAllowed = 1024 * 1024;
    memset(&pool, 0, sizeof(pool));
    pool.num = 4;
    pool.max = 8;
    pool.min = 2;
    pool.name = "test-worker";
  }

  void TearDown() override {
    // Cleanup is done per-test where pool was initialized
  }

  // Initialize pool and allocate queue with given FItem
  bool initPool(FItem fp, void *ahandle = nullptr) {
    int32_t code = tQueryAutoQWorkerInit(&pool);
    if (code != TSDB_CODE_SUCCESS) return false;
    queue = tQueryAutoQWorkerAllocQueue(&pool, ahandle ? ahandle : (void *)this, fp);
    return queue != nullptr;
  }

  void cleanupPool() { tQueryAutoQWorkerCleanup(&pool); }

  // Submit N messages to the pool's queue
  bool submitMessages(int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
      void *qitem = nullptr;
      int32_t code = taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
      if (code != 0) return false;
      *(int32_t *)qitem = i;
      code = taosWriteQitem(queue, qitem);
      if (code != 0) return false;
    }
    return true;
  }

  // Wait for processedCount to reach target, with timeout
  bool waitForProcessed(int32_t target, int32_t timeoutMs = 5000) {
    auto start = std::chrono::steady_clock::now();
    while (processedCount.load() < target) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      if (elapsed > timeoutMs) return false;
    }
    return true;
  }
};

// ============================================================================
// Simple message processor for basic tests
// ============================================================================

static void simpleProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  auto *self = (QueryAutoQWorkerTest *)pQInfo->ahandle;
  self->processedCount.fetch_add(1);
  taosFreeQitem(pMsg);
}

// ============================================================================
// Test 1: beforeBlocking on correct pool doesn't go negative
// ============================================================================

TEST_F(QueryAutoQWorkerTest, BeforeBlockingCorrectPool) {
  ASSERT_TRUE(initPool(simpleProcessFp));

  // Let pool start workers by submitting messages
  ASSERT_TRUE(submitMessages(10));
  ASSERT_TRUE(waitForProcessed(10));

  // Directly call beforeBlocking on the correct pool
  // This simulates a normal blocking operation
  int32_t runningBefore = GET_RUNNING_N(pool.activeRunningN);

  int32_t code = pool.pCb->beforeBlocking(pool.pCb->pPool);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  // running should not be negative
  int32_t runningAfter = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(runningAfter, 0) << "running went negative after beforeBlocking";

  // Recover
  code = pool.pCb->afterRecoverFromBlocking(pool.pCb->pPool);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  int32_t runningRecovered = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(runningRecovered, 0) << "running went negative after recoverFromBlocking";

  cleanupPool();
}

// ============================================================================
// Test 2: Repeated beforeBlocking without matching recovery (simulates cross-pool abuse)
// Running counter must never go below 0
// ============================================================================

TEST_F(QueryAutoQWorkerTest, RepeatedBeforeBlockingFloorProtection) {
  ASSERT_TRUE(initPool(simpleProcessFp));

  // Submit and wait for some messages to get the pool active
  ASSERT_TRUE(submitMessages(4));
  ASSERT_TRUE(waitForProcessed(4));

  // Abuse: call beforeBlocking many more times than there are running threads
  // In the old code this would drive running to large negative values
  for (int32_t i = 0; i < 20; i++) {
    pool.pCb->beforeBlocking(pool.pCb->pPool);
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running went negative after " << (i + 1) << " beforeBlocking calls";
  }

  cleanupPool();
}

// ============================================================================
// Test 3: Direct injection of negative running, verify healRunning via
// afterRecoverFromBlocking
// ============================================================================

TEST_F(QueryAutoQWorkerTest, HealNegativeRunningViaRecover) {
  ASSERT_TRUE(initPool(simpleProcessFp));

  // Inject negative running directly
  int32_t active = GET_ACTIVE_N(pool.activeRunningN);
  pool.activeRunningN = MAKE_ACTIVE_RUNNING(active > 0 ? active : 1, -5);

  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_EQ(running, -5) << "injection failed";

  // afterRecoverFromBlocking should trigger healRunning
  int32_t code = pool.pCb->afterRecoverFromBlocking(pool.pCb->pPool);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(running, 0) << "healRunning failed to correct negative running";

  cleanupPool();
}

// ============================================================================
// Test 4: Direct injection of negative running, verify pool can still
// process messages (end-to-end self-healing)
// ============================================================================

static void blockingProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  auto *self = (QueryAutoQWorkerTest *)pQInfo->ahandle;

  // Simulate a blocking operation using the pool's callbacks
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    // Simulate some I/O wait
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    cb->afterRecoverFromBlocking(cb->pPool);
  }

  self->processedCount.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, PoolRecoverAfterNegativeRunningInjection) {
  ASSERT_TRUE(initPool(blockingProcessFp));

  // Process some messages normally first
  ASSERT_TRUE(submitMessages(8));
  ASSERT_TRUE(waitForProcessed(8));

  int32_t baseline = processedCount.load();

  // Inject anomalous negative running
  int32_t active = GET_ACTIVE_N(pool.activeRunningN);
  pool.activeRunningN = MAKE_ACTIVE_RUNNING(active > 0 ? active : pool.num, -3);

  // Submit more messages - pool should self-heal and process them
  ASSERT_TRUE(submitMessages(8));
  ASSERT_TRUE(waitForProcessed(baseline + 8, 10000))
      << "Pool failed to recover: only processed " << processedCount.load()
      << " of expected " << (baseline + 8);

  // Verify running is non-negative
  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(running, 0) << "running still negative after self-healing";

  cleanupPool();
}

// ============================================================================
// Test 5: Concurrent cross-pool abuse simulation
// Multiple threads call beforeBlocking on a pool that's not theirs
// ============================================================================

TEST_F(QueryAutoQWorkerTest, ConcurrentCrossPoolAbuse) {
  ASSERT_TRUE(initPool(simpleProcessFp));

  // Start workers
  ASSERT_TRUE(submitMessages(4));
  ASSERT_TRUE(waitForProcessed(4));

  // Simulate concurrent cross-pool abuse: multiple threads calling
  // beforeBlocking without matching afterRecoverFromBlocking
  std::vector<std::thread> abusers;
  std::atomic<int32_t>     negativeDetected{0};

  for (int i = 0; i < 8; i++) {
    abusers.emplace_back([this, &negativeDetected]() {
      for (int j = 0; j < 50; j++) {
        pool.pCb->beforeBlocking(pool.pCb->pPool);
        int32_t running = GET_RUNNING_N(pool.activeRunningN);
        if (running < 0) {
          negativeDetected.fetch_add(1);
        }
      }
    });
  }

  for (auto &t : abusers) t.join();

  // With self-healing, running should never have gone negative
  ASSERT_EQ(negativeDetected.load(), 0)
      << "running went negative " << negativeDetected.load() << " times during concurrent abuse";

  int32_t finalRunning = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(finalRunning, 0) << "final running is negative";

  cleanupPool();
}

// ============================================================================
// Test 6: Paired beforeBlocking/afterRecoverFromBlocking maintains invariant
// ============================================================================

TEST_F(QueryAutoQWorkerTest, PairedBlockingRecoveryMaintainsInvariant) {
  ASSERT_TRUE(initPool(simpleProcessFp));

  ASSERT_TRUE(submitMessages(4));
  ASSERT_TRUE(waitForProcessed(4));

  // Properly paired blocking/recovery cycles
  for (int i = 0; i < 20; i++) {
    int32_t code = pool.pCb->beforeBlocking(pool.pCb->pPool);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running negative after beforeBlocking, iteration " << i;

    code = pool.pCb->afterRecoverFromBlocking(pool.pCb->pPool);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running negative after recovery, iteration " << i;
  }

  cleanupPool();
}

// ============================================================================
// Test 7: Two pools - verify cross-pool callback doesn't corrupt either pool
// ============================================================================

static std::atomic<int32_t> g_pool1Processed{0};
static std::atomic<int32_t> g_pool2Processed{0};

static void pool1ProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  g_pool1Processed.fetch_add(1);
  taosFreeQitem(pMsg);
}

static void pool2ProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  g_pool2Processed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, TwoPoolsCrossCallbackProtection) {
  g_pool1Processed = 0;
  g_pool2Processed = 0;

  // Pool 1 (uses the fixture pool)
  ASSERT_TRUE(initPool(pool1ProcessFp));

  // Pool 2
  SQueryAutoQWorkerPool pool2{};
  memset(&pool2, 0, sizeof(pool2));
  pool2.num = 4;
  pool2.max = 8;
  pool2.min = 2;
  pool2.name = "test-worker-2";
  ASSERT_EQ(tQueryAutoQWorkerInit(&pool2), TSDB_CODE_SUCCESS);
  STaosQueue *q2 = tQueryAutoQWorkerAllocQueue(&pool2, nullptr, pool2ProcessFp);
  ASSERT_NE(q2, nullptr);

  // Submit messages to both pools
  ASSERT_TRUE(submitMessages(10));

  // Submit to pool2
  for (int32_t i = 0; i < 10; i++) {
    void *qitem = nullptr;
    ASSERT_EQ(taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem), 0);
    *(int32_t *)qitem = i;
    ASSERT_EQ(taosWriteQitem(q2, qitem), 0);
  }

  // Wait for messages to be processed
  auto start = std::chrono::steady_clock::now();
  while (g_pool1Processed.load() < 10 || g_pool2Processed.load() < 10) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed > 5000) break;
  }
  ASSERT_GE(g_pool1Processed.load(), 10);
  ASSERT_GE(g_pool2Processed.load(), 10);

  // Cross-pool abuse: call pool1's beforeBlocking with pool2's pPool
  // This is the exact scenario that caused the original deadlock
  for (int i = 0; i < 10; i++) {
    pool.pCb->beforeBlocking(pool2.pCb->pPool);  // pool1's cb on pool2's data
  }

  // Pool2's running should not go negative
  int32_t pool2Running = GET_RUNNING_N(pool2.activeRunningN);
  ASSERT_GE(pool2Running, 0) << "pool2 running went negative from cross-pool abuse";

  // Pool1's running should be unaffected
  int32_t pool1Running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(pool1Running, 0) << "pool1 running went negative";

  // Both pools should still be able to process messages
  g_pool1Processed = 0;
  g_pool2Processed = 0;

  ASSERT_TRUE(submitMessages(5));
  for (int32_t i = 0; i < 5; i++) {
    void *qitem = nullptr;
    ASSERT_EQ(taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem), 0);
    *(int32_t *)qitem = i;
    ASSERT_EQ(taosWriteQitem(q2, qitem), 0);
  }

  start = std::chrono::steady_clock::now();
  while (g_pool1Processed.load() < 5 || g_pool2Processed.load() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed > 10000) break;
  }

  ASSERT_GE(g_pool1Processed.load(), 5) << "pool1 stopped processing after cross-pool abuse";
  ASSERT_GE(g_pool2Processed.load(), 5) << "pool2 stopped processing after cross-pool abuse";

  tQueryAutoQWorkerCleanup(&pool2);
  cleanupPool();
}

// ============================================================================
// Test 8: activeRunningN bit-packing correctness
// ============================================================================

TEST_F(QueryAutoQWorkerTest, ActiveRunningBitPacking) {
  // Verify the bit-packing macros work correctly
  int64_t val;

  // Case 1: Both positive
  val = MAKE_ACTIVE_RUNNING(5, 3);
  ASSERT_EQ(GET_ACTIVE_N(val), 5);
  ASSERT_EQ(GET_RUNNING_N(val), 3);

  // Case 2: active=0, running=0
  val = MAKE_ACTIVE_RUNNING(0, 0);
  ASSERT_EQ(GET_ACTIVE_N(val), 0);
  ASSERT_EQ(GET_RUNNING_N(val), 0);

  // Case 3: Negative running (what happens when bug corrupts it)
  // -1 in 32-bit = 0xFFFFFFFF, but GET_RUNNING_N casts to int32_t
  val = MAKE_ACTIVE_RUNNING(4, -1);
  int32_t running = GET_RUNNING_N(val);
  ASSERT_EQ(running, -1) << "negative running encoding/decoding mismatch";

  // Case 4: Large values
  val = MAKE_ACTIVE_RUNNING(100, 50);
  ASSERT_EQ(GET_ACTIVE_N(val), 100);
  ASSERT_EQ(GET_RUNNING_N(val), 50);
}

// ============================================================================
// Test 9: Stress test - heavy concurrent blocking/recovery with fault injection
// ============================================================================

static std::atomic<int32_t> g_stressProcessed{0};

static void stressProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  g_stressProcessed.fetch_add(1);

  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    cb->afterRecoverFromBlocking(cb->pPool);
  }

  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, StressConcurrentBlockingWithFaultInjection) {
  g_stressProcessed = 0;

  ASSERT_TRUE(initPool(stressProcessFp));

  const int32_t totalMsgs = 100;

  // Submit messages
  ASSERT_TRUE(submitMessages(totalMsgs));

  // Concurrently inject faults while messages are being processed
  std::atomic<bool> stopFaults{false};
  std::thread faultInjector([this, &stopFaults]() {
    int iteration = 0;
    while (!stopFaults.load()) {
      if (iteration % 10 == 0) {
        // Inject negative running
        int32_t active = GET_ACTIVE_N(pool.activeRunningN);
        if (active <= 0) active = pool.num;
        pool.activeRunningN = MAKE_ACTIVE_RUNNING(active, -2);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      iteration++;
    }
  });

  // Wait for all messages to be processed
  auto start = std::chrono::steady_clock::now();
  while (g_stressProcessed.load() < totalMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed > 30000) break;  // 30s timeout
  }

  stopFaults = true;
  faultInjector.join();

  ASSERT_GE(g_stressProcessed.load(), totalMsgs)
      << "Pool failed to process all messages under fault injection: "
      << g_stressProcessed.load() << "/" << totalMsgs;

  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(running, 0) << "running still negative after stress test";

  cleanupPool();
}

// ============================================================================
// Test 10: Real-usage happy path — every message does beforeBlocking +
//          afterRecoverFromBlocking inside the worker thread (no fault injection)
// ============================================================================

static std::atomic<int32_t> g_happyProcessed{0};

static void happyBlockingProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    int32_t code = cb->beforeBlocking(cb->pPool);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    // Simulate I/O blocking (RPC wait, disk read, etc.)
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    code = cb->afterRecoverFromBlocking(cb->pPool);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  }
  g_happyProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, RealUsageBlockingWorkload) {
  g_happyProcessed = 0;

  pool.min = 4;
  pool.num = 4;
  pool.max = 32;
  ASSERT_TRUE(initPool(happyBlockingProcessFp));

  const int32_t totalMsgs = 2000;
  ASSERT_TRUE(submitMessages(totalMsgs));

  // Wait for all messages
  auto start = std::chrono::steady_clock::now();
  while (g_happyProcessed.load() < totalMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 60000) << "timeout: processed " << g_happyProcessed.load()
                               << "/" << totalMsgs;
    // Invariant: running must always be >= 0
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running went negative during normal workload";
  }

  ASSERT_EQ(g_happyProcessed.load(), totalMsgs);

  // Final state check
  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  int32_t active = GET_ACTIVE_N(pool.activeRunningN);
  ASSERT_GE(running, 0);
  ASSERT_GE(active, 0);

  cleanupPool();
}

// ============================================================================
// Test 11: Sustained workload — multiple rounds of submit + drain, simulating
//          a long-running service that continuously processes queries
// ============================================================================

static std::atomic<int32_t> g_sustainedProcessed{0};

static void sustainedProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    std::this_thread::sleep_for(std::chrono::microseconds(500));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_sustainedProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, SustainedMultiRoundWorkload) {
  g_sustainedProcessed = 0;

  pool.min = 4;
  pool.num = 4;
  pool.max = 32;
  ASSERT_TRUE(initPool(sustainedProcessFp));

  const int32_t rounds = 50;
  const int32_t msgsPerRound = 100;

  for (int32_t r = 0; r < rounds; ++r) {
    int32_t baseline = g_sustainedProcessed.load();
    ASSERT_TRUE(submitMessages(msgsPerRound))
        << "failed to submit messages in round " << r;

    auto start = std::chrono::steady_clock::now();
    while (g_sustainedProcessed.load() < baseline + msgsPerRound) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      ASSERT_LT(elapsed, 10000)
          << "round " << r << " timeout: processed "
          << (g_sustainedProcessed.load() - baseline) << "/" << msgsPerRound;
    }

    // Between rounds: invariant must hold
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running negative after round " << r;
  }

  ASSERT_EQ(g_sustainedProcessed.load(), rounds * msgsPerRound);
  cleanupPool();
}

// ============================================================================
// Test 12: Concurrent producers — multiple threads submit messages while
//          workers are actively processing with blocking
// ============================================================================

static std::atomic<int32_t> g_concurrentProducerProcessed{0};

static void concurrentProducerProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    std::this_thread::sleep_for(std::chrono::microseconds(200));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_concurrentProducerProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, ConcurrentProducersWithBlockingWorkers) {
  g_concurrentProducerProcessed = 0;

  pool.min = 4;
  pool.num = 4;
  pool.max = 64;
  ASSERT_TRUE(initPool(concurrentProducerProcessFp));

  const int32_t numProducers = 16;
  const int32_t msgsPerProducer = 500;
  const int32_t totalMsgs = numProducers * msgsPerProducer;

  // Launch producer threads
  std::vector<std::thread> producers;
  std::atomic<int32_t> submitFailures{0};
  for (int p = 0; p < numProducers; ++p) {
    producers.emplace_back([this, msgsPerProducer, &submitFailures]() {
      for (int32_t i = 0; i < msgsPerProducer; ++i) {
        void *qitem = nullptr;
        int32_t code = taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
        if (code != 0) { submitFailures.fetch_add(1); continue; }
        *(int32_t *)qitem = i;
        code = taosWriteQitem(queue, qitem);
        if (code != 0) { submitFailures.fetch_add(1); taosFreeQitem(qitem); }
      }
    });
  }

  for (auto &t : producers) t.join();
  ASSERT_EQ(submitFailures.load(), 0) << "some messages failed to submit";

  // Wait for all messages to be processed
  auto start = std::chrono::steady_clock::now();
  while (g_concurrentProducerProcessed.load() < totalMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 120000)
        << "timeout: processed " << g_concurrentProducerProcessed.load()
        << "/" << totalMsgs;
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running went negative during concurrent produce";
  }

  ASSERT_EQ(g_concurrentProducerProcessed.load(), totalMsgs);

  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(running, 0);

  cleanupPool();
}

// ============================================================================
// Test 13: Pool scaling — start with min workers, submit burst to trigger
//          dynamic scaling, verify pool grows and all messages are processed
// ============================================================================

static std::atomic<int32_t> g_scalingProcessed{0};

static void scalingProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    // Long blocking to force pool to scale up
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_scalingProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, PoolScalingUnderLoad) {
  g_scalingProcessed = 0;

  pool.min = 2;
  pool.num = 2;  // start small
  pool.max = 64; // allow significant scaling
  ASSERT_TRUE(initPool(scalingProcessFp));

  // Submit a burst of long-blocking messages to force worker scaling
  const int32_t burstSize = 200;
  ASSERT_TRUE(submitMessages(burstSize));

  // Wait for processing
  auto start = std::chrono::steady_clock::now();
  while (g_scalingProcessed.load() < burstSize) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 30000)
        << "timeout: processed " << g_scalingProcessed.load() << "/" << burstSize;

    // running must stay non-negative during scaling
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running went negative during pool scaling";
  }

  ASSERT_EQ(g_scalingProcessed.load(), burstSize);

  // Pool should have scaled beyond initial num=2
  ASSERT_GT(pool.num, 2) << "pool did not scale up under load";

  cleanupPool();
}

// ============================================================================
// Test 14: Mixed blocking and non-blocking — some messages block, others don't.
//          Verifies the pool handles mixed workloads correctly.
// ============================================================================

static std::atomic<int32_t> g_mixedProcessed{0};

static void mixedProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  int32_t val = *(int32_t *)pMsg;
  // Even-numbered messages block, odd-numbered ones don't
  if (val % 2 == 0 && pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_mixedProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, MixedBlockingAndNonBlocking) {
  g_mixedProcessed = 0;

  pool.min = 4;
  pool.num = 4;
  pool.max = 32;
  ASSERT_TRUE(initPool(mixedProcessFp));

  const int32_t totalMsgs = 2000;
  ASSERT_TRUE(submitMessages(totalMsgs));

  auto start = std::chrono::steady_clock::now();
  while (g_mixedProcessed.load() < totalMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 60000)
        << "timeout: processed " << g_mixedProcessed.load() << "/" << totalMsgs;
    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running went negative during mixed workload";
  }

  ASSERT_EQ(g_mixedProcessed.load(), totalMsgs);

  int32_t running = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(running, 0);

  cleanupPool();
}

// ============================================================================
// Test 15: Two independent pools processing concurrently — simulates the real
//          mnode scenario with queryWorker and mqueryWorker both active
// ============================================================================

static std::atomic<int32_t> g_dualQueryProcessed{0};
static std::atomic<int32_t> g_dualMqueryProcessed{0};

static void dualQueryProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    std::this_thread::sleep_for(std::chrono::microseconds(500));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_dualQueryProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

static void dualMqueryProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    // mquery tasks are heavier (merge)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_dualMqueryProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, DualPoolConcurrentProcessing) {
  g_dualQueryProcessed = 0;
  g_dualMqueryProcessed = 0;

  // Pool 1: queryWorker (uses fixture pool, larger)
  pool.min = 4;
  pool.num = 4;
  pool.max = 16;
  pool.name = "mnode-query";
  ASSERT_TRUE(initPool(dualQueryProcessFp));

  // Pool 2: mqueryWorker (smaller, like real mnode)
  SQueryAutoQWorkerPool mqueryPool{};
  memset(&mqueryPool, 0, sizeof(mqueryPool));
  mqueryPool.min = 2;
  mqueryPool.num = 2;
  mqueryPool.max = 4;
  mqueryPool.name = "mnode-mquery";
  ASSERT_EQ(tQueryAutoQWorkerInit(&mqueryPool), TSDB_CODE_SUCCESS);
  STaosQueue *mq = tQueryAutoQWorkerAllocQueue(&mqueryPool, nullptr, dualMqueryProcessFp);
  ASSERT_NE(mq, nullptr);

  // Submit messages to both pools concurrently
  const int32_t queryMsgs = 1000;
  const int32_t mqueryMsgs = 300;

  std::thread queryProducer([this, queryMsgs]() {
    for (int32_t i = 0; i < queryMsgs; ++i) {
      void *qitem = nullptr;
      (void)taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
      *(int32_t *)qitem = i;
      (void)taosWriteQitem(queue, qitem);
    }
  });

  std::thread mqueryProducer([mq, mqueryMsgs]() {
    for (int32_t i = 0; i < mqueryMsgs; ++i) {
      void *qitem = nullptr;
      (void)taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
      *(int32_t *)qitem = i;
      (void)taosWriteQitem(mq, qitem);
    }
  });

  queryProducer.join();
  mqueryProducer.join();

  // Wait for both pools to finish
  auto start = std::chrono::steady_clock::now();
  while (g_dualQueryProcessed.load() < queryMsgs ||
         g_dualMqueryProcessed.load() < mqueryMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 30000)
        << "timeout: query=" << g_dualQueryProcessed.load() << "/" << queryMsgs
        << " mquery=" << g_dualMqueryProcessed.load() << "/" << mqueryMsgs;

    // Neither pool should have negative running
    int32_t r1 = GET_RUNNING_N(pool.activeRunningN);
    int32_t r2 = GET_RUNNING_N(mqueryPool.activeRunningN);
    ASSERT_GE(r1, 0) << "queryPool running negative";
    ASSERT_GE(r2, 0) << "mqueryPool running negative";
  }

  ASSERT_EQ(g_dualQueryProcessed.load(), queryMsgs);
  ASSERT_EQ(g_dualMqueryProcessed.load(), mqueryMsgs);

  tQueryAutoQWorkerCleanup(&mqueryPool);
  cleanupPool();
}

// ============================================================================
// Test 16: Heavy stress — 32 producer threads × 1000 messages each = 32000 msgs,
//          pool max=128 workers, every message does blocking I/O.
//          Runs for extended time to expose race conditions.
// ============================================================================

static std::atomic<int32_t> g_heavyProcessed{0};
static std::atomic<int32_t> g_heavyMinRunning{INT32_MAX};

static void heavyStressProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    // Variable blocking time to create diverse scheduling patterns
    int32_t val = *(int32_t *)pMsg;
    int32_t delayUs = 50 + (val % 200);  // 50-250us
    std::this_thread::sleep_for(std::chrono::microseconds(delayUs));
    cb->afterRecoverFromBlocking(cb->pPool);
  }
  g_heavyProcessed.fetch_add(1);

  // Track minimum running observed by any worker
  auto *pool = (SQueryAutoQWorkerPool *)((SQueryAutoQWorkerPoolCB *)pQInfo->workerCb)->pPool;
  int32_t running = GET_RUNNING_N(pool->activeRunningN);
  int32_t curMin = g_heavyMinRunning.load();
  while (running < curMin) {
    if (g_heavyMinRunning.compare_exchange_weak(curMin, running)) break;
  }

  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, HeavyStress32Producers) {
  g_heavyProcessed = 0;
  g_heavyMinRunning = INT32_MAX;

  pool.min = 4;
  pool.num = 4;
  pool.max = 128;
  pool.name = "heavy-stress";
  ASSERT_TRUE(initPool(heavyStressProcessFp));

  const int32_t numProducers = 32;
  const int32_t msgsPerProducer = 1000;
  const int32_t totalMsgs = numProducers * msgsPerProducer;

  // Launch producers
  std::vector<std::thread> producers;
  std::atomic<int32_t> submitFailures{0};
  for (int p = 0; p < numProducers; ++p) {
    producers.emplace_back([this, msgsPerProducer, p, &submitFailures]() {
      for (int32_t i = 0; i < msgsPerProducer; ++i) {
        void *qitem = nullptr;
        int32_t code = taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
        if (code != 0) { submitFailures.fetch_add(1); continue; }
        *(int32_t *)qitem = p * msgsPerProducer + i;
        code = taosWriteQitem(queue, qitem);
        if (code != 0) { submitFailures.fetch_add(1); taosFreeQitem(qitem); }
      }
    });
  }

  for (auto &t : producers) t.join();
  ASSERT_EQ(submitFailures.load(), 0) << "submit failures detected";

  // Wait with continuous invariant checking
  auto start = std::chrono::steady_clock::now();
  int64_t checkCount = 0;
  while (g_heavyProcessed.load() < totalMsgs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ++checkCount;
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    ASSERT_LT(elapsed, 120000)
        << "timeout after " << elapsed << "ms: processed "
        << g_heavyProcessed.load() << "/" << totalMsgs;

    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    int32_t active = GET_ACTIVE_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running=" << running << " at check #" << checkCount;
    ASSERT_GE(active, 0) << "active=" << active << " at check #" << checkCount;
  }

  ASSERT_EQ(g_heavyProcessed.load(), totalMsgs);
  ASSERT_GE(g_heavyMinRunning.load(), 0)
      << "minimum running observed by workers was " << g_heavyMinRunning.load();

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  printf("  Heavy stress: %d msgs, %d producers, %d max workers, %" PRId64 "ms, "
         "%" PRId64 " invariant checks passed\n",
         totalMsgs, numProducers, pool.num, (int64_t)elapsed, (int64_t)checkCount);

  cleanupPool();
}

// ============================================================================
// Test 17: Endurance — sustained load for >= 10 seconds with continuous
//          producer pressure. 16 producers keep submitting at steady rate.
//          Verifies no drift, no leak, no livelock over extended operation.
// ============================================================================

static std::atomic<int64_t> g_enduranceProcessed{0};
static std::atomic<int32_t> g_enduranceNegativeDetected{0};

static void enduranceProcessFp(SQueueInfo *pQInfo, void *pMsg) {
  if (pQInfo->workerCb) {
    auto *cb = (SQueryAutoQWorkerPoolCB *)pQInfo->workerCb;
    cb->beforeBlocking(cb->pPool);
    // Realistic RPC-like latency: 100-500us
    int32_t val = *(int32_t *)pMsg;
    std::this_thread::sleep_for(std::chrono::microseconds(100 + (val % 400)));
    cb->afterRecoverFromBlocking(cb->pPool);

    auto *pool = (SQueryAutoQWorkerPool *)cb->pPool;
    int32_t running = GET_RUNNING_N(pool->activeRunningN);
    if (running < 0) g_enduranceNegativeDetected.fetch_add(1);
  }
  g_enduranceProcessed.fetch_add(1);
  taosFreeQitem(pMsg);
}

TEST_F(QueryAutoQWorkerTest, EnduranceSustainedLoad) {
  g_enduranceProcessed = 0;
  g_enduranceNegativeDetected = 0;

  pool.min = 4;
  pool.num = 4;
  pool.max = 64;
  pool.name = "endurance";
  ASSERT_TRUE(initPool(enduranceProcessFp));

  const int32_t numProducers = 16;
  const int64_t durationMs = 10000;  // run for 10 seconds
  std::atomic<bool> stopProducers{false};
  std::atomic<int64_t> totalSubmitted{0};
  std::atomic<int32_t> submitFailures{0};

  // Producers: each continuously submits messages until told to stop
  std::vector<std::thread> producers;
  for (int p = 0; p < numProducers; ++p) {
    producers.emplace_back([this, p, &stopProducers, &totalSubmitted, &submitFailures]() {
      int32_t seq = 0;
      while (!stopProducers.load()) {
        void *qitem = nullptr;
        int32_t code = taosAllocateQitem(sizeof(int32_t), DEF_QITEM, 0, &qitem);
        if (code != 0) { submitFailures.fetch_add(1); continue; }
        *(int32_t *)qitem = p * 100000 + seq++;
        code = taosWriteQitem(queue, qitem);
        if (code != 0) {
          submitFailures.fetch_add(1);
          taosFreeQitem(qitem);
          continue;
        }
        totalSubmitted.fetch_add(1);
        // Steady-rate: ~1 msg every 100-300us per producer
        std::this_thread::sleep_for(std::chrono::microseconds(100 + (seq % 200)));
      }
    });
  }

  // Monitor: check invariants throughout the run
  auto start = std::chrono::steady_clock::now();
  int64_t checkCount = 0;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ++checkCount;
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed >= durationMs) break;

    int32_t running = GET_RUNNING_N(pool.activeRunningN);
    int32_t active = GET_ACTIVE_N(pool.activeRunningN);
    ASSERT_GE(running, 0) << "running=" << running << " at " << elapsed << "ms";
    ASSERT_GE(active, 0) << "active=" << active << " at " << elapsed << "ms";
  }

  // Stop producers
  stopProducers = true;
  for (auto &t : producers) t.join();

  int64_t submitted = totalSubmitted.load();
  ASSERT_GT(submitted, 0) << "no messages were submitted";

  // Wait for remaining messages to drain
  auto drainStart = std::chrono::steady_clock::now();
  while (g_enduranceProcessed.load() < submitted) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - drainStart)
                       .count();
    ASSERT_LT(elapsed, 60000)
        << "drain timeout: processed " << g_enduranceProcessed.load()
        << "/" << submitted;
  }

  ASSERT_EQ(g_enduranceProcessed.load(), submitted);
  ASSERT_EQ(g_enduranceNegativeDetected.load(), 0)
      << "negative running detected " << g_enduranceNegativeDetected.load()
      << " times inside workers";
  ASSERT_EQ(submitFailures.load(), 0) << "submit failures: " << submitFailures.load();

  int32_t finalRunning = GET_RUNNING_N(pool.activeRunningN);
  ASSERT_GE(finalRunning, 0);

  auto totalElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
  printf("  Endurance: %" PRId64 " msgs submitted/processed, %d producers, "
         "%d max workers, %" PRId64 "ms total, %" PRId64 " invariant checks\n",
         submitted, numProducers, pool.num, (int64_t)totalElapsed, checkCount);

  cleanupPool();
}
