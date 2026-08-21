#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>

#include "dataSink.h"
#include "nodes.h"
#include "stream.h"
#include "streamInt.h"
#include "streamReader.h"
#include "streamRunner.h"
#include "stub.h"

namespace {

void releaseStackReaderTask(void *pParam) {
  auto **ppTask = static_cast<SStreamReaderTask **>(pParam);
  streamTaskStatsHandleLifecycle(&(*ppTask)->pStats, STREAM_TASK_STATS_OWNER_DESTROYED);
  *ppTask = nullptr;
}

void appendReaderPolicy(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex) {
  if (runtime->pContextPolicy == nullptr) {
    runtime->pContextPolicy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
    ASSERT_NE(runtime->pContextPolicy, nullptr);
    runtime->pContextPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    ASSERT_NE(runtime->pContextPolicy->pEntries, nullptr);
  }
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  ASSERT_NE(taosArrayPush(runtime->pContextPolicy->pEntries, &entry), nullptr);
}

void appendReaderNonePolicy(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex) {
  if (runtime->pContextPolicy == nullptr) {
    runtime->pContextPolicy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
    ASSERT_NE(runtime->pContextPolicy, nullptr);
    runtime->pContextPolicy->pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
    ASSERT_NE(runtime->pContextPolicy->pEntries, nullptr);
  }
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_NONE;
  ASSERT_NE(taosArrayPush(runtime->pContextPolicy->pEntries, &entry), nullptr);
}

void appendReaderAncestorContext(SStreamRuntimeFuncInfo *runtime, int64_t gid, int32_t paramIndex, int64_t lineageStart,
                                 int64_t ancestorStart) {
  if (runtime->pAncestorContext == nullptr) {
    runtime->pAncestorContext =
        static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
    ASSERT_NE(runtime->pAncestorContext, nullptr);
    runtime->pAncestorContext->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
    ASSERT_NE(runtime->pAncestorContext->pParamContexts, nullptr);
  }

  SStreamAncestorParamContext context = {};
  context.paramIndex = paramIndex;
  context.leafIdentity.gid = gid;
  context.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  context.leafIdentity.openingTs = lineageStart + 10;
  context.leafIdentity.nativeDiscriminator = paramIndex + 1;
  context.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(context.leafIdentity.lineage.pScopes, nullptr);
  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_INTERVAL;
  scope.openingTs = lineageStart;
  scope.nativeDiscriminator = 7;
  ASSERT_NE(taosArrayPush(context.leafIdentity.lineage.pScopes, &scope), nullptr);
  context.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  ASSERT_NE(context.pSnapshots, nullptr);
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = 0;
  snapshot.triggerType = WINDOW_TYPE_INTERVAL;
  snapshot.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND;
  snapshot.values.window.start = ancestorStart;
  snapshot.values.window.end = ancestorStart + 99;
  snapshot.values.window.duration = 100;
  snapshot.values.window.rownum = 1;
  ASSERT_NE(taosArrayPush(context.pSnapshots, &snapshot), nullptr);
  ASSERT_NE(taosArrayPush(runtime->pAncestorContext->pParamContexts, &context), nullptr);
}

void appendReaderBinding(SStreamRuntimeFuncInfo *runtime, int32_t vgId, int32_t readInfoIndex,
                         int32_t paramContextIndex) {
  if (runtime->pAncestorContext->pReadScopeBindings == nullptr) {
    runtime->pAncestorContext->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
    ASSERT_NE(runtime->pAncestorContext->pReadScopeBindings, nullptr);
  }
  const auto *context = static_cast<const SStreamAncestorParamContext *>(
      taosArrayGet(runtime->pAncestorContext->pParamContexts, paramContextIndex));
  ASSERT_NE(context, nullptr);
  SStreamReadScopeBinding binding = {};
  binding.vgId = vgId;
  binding.readInfoIndex = readInfoIndex;
  binding.scope.gid = context->leafIdentity.gid;
  binding.scope.lineage.pScopes = taosArrayDup(context->leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(binding.scope.lineage.pScopes, nullptr);
  ASSERT_NE(taosArrayPush(runtime->pAncestorContext->pReadScopeBindings, &binding), nullptr);
}

const SStreamAncestorParamContext *onlyProjectedReaderContext(const SStreamRuntimeFuncInfo &runtime) {
  if (runtime.pAncestorContext == nullptr || taosArrayGetSize(runtime.pAncestorContext->pParamContexts) != 1) {
    return nullptr;
  }
  return static_cast<const SStreamAncestorParamContext *>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0));
}

char *makeCalcSubplanJson(bool requiresAncestorContext) {
  SSubplan *subplan = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode **>(&subplan)), TSDB_CODE_SUCCESS);
  if (subplan == nullptr) return nullptr;
  subplan->requiresAncestorContext = requiresAncestorContext;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&subplan->pNode)),
            TSDB_CODE_SUCCESS);

  char   *json = nullptr;
  int32_t length = 0;
  EXPECT_EQ(nodesNodeToString(reinterpret_cast<SNode *>(subplan), false, &json, &length), TSDB_CODE_SUCCESS);
  nodesDestroyNode(reinterpret_cast<SNode *>(subplan));
  return json;
}

struct ScopedReadCalls {
  int32_t hashGet;
  int32_t hashPut;
  int32_t dataCache;
};

ScopedReadCalls gScopedReadCalls = {};
void           *gScopedReadIter = nullptr;
char            gCacheSentinel = 0;

struct CacheReadConcurrencyGate {
  std::mutex              mutex;
  std::condition_variable condition;
  bool                    fetchEntered = false;
  bool                    allowFetch = false;
};

CacheReadConcurrencyGate *gCacheReadConcurrencyGate = nullptr;

class ScopedCacheReadConcurrencyGate {
 public:
  explicit ScopedCacheReadConcurrencyGate(CacheReadConcurrencyGate *gate) {
    EXPECT_EQ(gCacheReadConcurrencyGate, nullptr);
    gCacheReadConcurrencyGate = gate;
  }

  ~ScopedCacheReadConcurrencyGate() { gCacheReadConcurrencyGate = nullptr; }
};

void *countScopedHashGet(SHashObj *, const void *, size_t) {
  ++gScopedReadCalls.hashGet;
  return gScopedReadCalls.hashGet == 1 ? nullptr : &gScopedReadIter;
}

int32_t countScopedHashPut(SHashObj *, const void *, size_t, const void *, size_t) {
  ++gScopedReadCalls.hashPut;
  return TSDB_CODE_SUCCESS;
}

int32_t countScopedDataCache(void *, int64_t, TSKEY, TSKEY, void **pIter) {
  ++gScopedReadCalls.dataCache;
  *pIter = reinterpret_cast<void *>(static_cast<uintptr_t>(0x6001));
  return TSDB_CODE_SUCCESS;
}

int32_t acquireFakeCacheLease(int64_t, int64_t, int64_t, SStreamDataCacheLease **ppLease, void **ppCache) {
  ++gScopedReadCalls.dataCache;
  *ppLease = nullptr;
  *ppCache = &gCacheSentinel;
  return TSDB_CODE_SUCCESS;
}

void releaseFakeCacheLease(SStreamDataCacheLease **ppLease) { *ppLease = nullptr; }

int32_t makeEmptyScopedIterator(void *, const SStreamCacheScope *, TSKEY, TSKEY, void **ppIter) {
  auto *pIter = static_cast<SResultIter *>(taosMemoryCalloc(1, sizeof(SResultIter)));
  if (pIter == nullptr) return terrno;
  pIter->scopedResult = true;
  *ppIter = pIter;
  return TSDB_CODE_SUCCESS;
}

int32_t makeEmptyLegacyIterator(void *, int64_t, TSKEY, TSKEY, void **ppIter) {
  return makeEmptyScopedIterator(nullptr, nullptr, 0, 0, ppIter);
}

int32_t blockConcurrentCacheRead(void **, SSDataBlock **ppBlock) {
  CacheReadConcurrencyGate *gate = gCacheReadConcurrencyGate;
  if (gate == nullptr) return TSDB_CODE_INTERNAL_ERROR;

  std::unique_lock<std::mutex> lock(gate->mutex);
  gate->fetchEntered = true;
  gate->condition.notify_all();
  if (!gate->condition.wait_for(lock, std::chrono::seconds(5), [gate] { return gate->allowFetch; })) {
    return TSDB_CODE_INTERNAL_ERROR;
  }
  *ppBlock = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t removeConcurrentCacheReadScope(SHashObj *, const void *, size_t) { return TSDB_CODE_SUCCESS; }

class StreamReaderCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(gStreamMgmt.taskMap, nullptr);
    gStreamMgmt.taskMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(gStreamMgmt.taskMap, nullptr);

    ASSERT_EQ(stCreateCalcDataCacheIterMap(&scopedIters_), TSDB_CODE_SUCCESS);
    realtime_.sessionId = kSessionId;
    realtime_.pCalcDataCacheIters = scopedIters_;

    task_.task.type = STREAM_TRIGGER_TASK;
    task_.task.streamId = kStreamId;
    task_.task.taskId = kTaskId;
    task_.triggerType = STREAM_TRIGGER_COUNT;
    task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
    task_.pRealtimeContext = &realtime_;
    SStreamTask *pTask = reinterpret_cast<SStreamTask *>(&task_);
    ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &task_.task.streamId,
                          sizeof(task_.task.streamId) + sizeof(task_.task.taskId), &pTask, POINTER_BYTES),
              TSDB_CODE_SUCCESS);

    gScopedReadCalls = {};
    gScopedReadIter = nullptr;
  }

  void TearDown() override {
    const int64_t key[2] = {kStreamId, kTaskId};
    if (gStreamMgmt.taskMap != nullptr) {
      EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, key, sizeof(key)), TSDB_CODE_SUCCESS);
      EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
      taosHashCleanup(gStreamMgmt.taskMap);
      gStreamMgmt.taskMap = nullptr;
    }
    taosHashCleanup(scopedIters_);
    scopedIters_ = nullptr;
  }

  void expectNoScopedRead() {
    EXPECT_EQ(task_.task.entryLock, 0);
    EXPECT_EQ(gScopedReadCalls.hashGet, 0);
    EXPECT_EQ(gScopedReadCalls.hashPut, 0);
    EXPECT_EQ(gScopedReadCalls.dataCache, 0);
  }

  static constexpr int64_t kStreamId = 0x1234;
  static constexpr int64_t kTaskId = 0x5678;
  static constexpr int64_t kSessionId = 0x9abc;

  SStreamTriggerTask       task_ = {};
  SSTriggerRealtimeContext realtime_ = {};
  SHashObj                *scopedIters_ = nullptr;
};

TEST(StreamReaderTest, DeployCopiesRequiresContextPolicyCapability) {
  SStreamReaderTask task = {};
  task.task.undeployCb = releaseStackReaderTask;
  SStreamReaderDeployMsg msg = {};
  msg.msg.calc.execReplica = 1;
  msg.msg.calc.calcScanPlan = makeCalcSubplanJson(true);
  ASSERT_NE(msg.msg.calc.calcScanPlan, nullptr);

  ASSERT_EQ(stReaderTaskDeploy(&task, &msg), TSDB_CODE_SUCCESS);
  ASSERT_NE(task.info, nullptr);
  auto *info = static_cast<SStreamTriggerReaderCalcInfo *>(taosArrayGetP(static_cast<SArray *>(task.info), 0));
  ASSERT_NE(info, nullptr);
  EXPECT_TRUE(info->requiresContextPolicy);

  SStreamReaderTask *taskPtr = &task;
  ASSERT_EQ(stReaderTaskUndeploy(&taskPtr, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taskPtr, nullptr);
  taosMemoryFree(msg.msg.calc.calcScanPlan);
}

TEST(StreamReaderTest, ProjectsSingleGroupFirstAndLastAncestorContextsToTemporaryParam) {
  SStreamRuntimeFuncInfo source = {};
  source.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 2);
  source.groupId = 101;
  ASSERT_NE(source.pStreamPesudoFuncVals, nullptr);
  appendReaderPolicy(&source, source.groupId, 0);
  appendReaderPolicy(&source, source.groupId, 1);
  appendReaderAncestorContext(&source, source.groupId, 0, 1000, 1100);
  appendReaderAncestorContext(&source, source.groupId, 1, 2000, 2100);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, -1, 0, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, source.groupId);
  EXPECT_EQ(target.curIdx, 0);
  ASSERT_EQ(taosArrayGetSize(target.pContextPolicy->pEntries), 1);
  const auto *first = onlyProjectedReaderContext(target);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->paramIndex, 0);
  const auto *firstSnapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(first->pSnapshots, 0));
  ASSERT_NE(firstSnapshot, nullptr);
  EXPECT_EQ(firstSnapshot->values.window.start, 1100);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, -1, 1, &target), TSDB_CODE_SUCCESS);
  const auto *last = onlyProjectedReaderContext(target);
  ASSERT_NE(last, nullptr);
  EXPECT_EQ(last->paramIndex, 0);
  const auto *lastSnapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(last->pSnapshots, 0));
  ASSERT_NE(lastSnapshot, nullptr);
  EXPECT_EQ(lastSnapshot->values.window.start, 2100);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.pStreamPesudoFuncVals);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, ProjectsMultiGroupAncestorContextSelectedByReadBinding) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1))->gid = 202;
  appendReaderPolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 3);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);
  appendReaderAncestorContext(&source, 202, 3, 2000, 2100);
  appendReaderBinding(&source, 7, 0, 0);
  appendReaderBinding(&source, 7, 1, 1);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, 1, -1, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, 202);
  EXPECT_EQ(target.curIdx, 0);
  ASSERT_EQ(taosArrayGetSize(target.pContextPolicy->pEntries), 1);
  const auto *entry = static_cast<const SStreamContextPolicyEntry *>(taosArrayGet(target.pContextPolicy->pEntries, 0));
  ASSERT_NE(entry, nullptr);
  EXPECT_EQ(entry->gid, 202);
  EXPECT_EQ(entry->paramIndex, 0);
  const auto *context = onlyProjectedReaderContext(target);
  ASSERT_NE(context, nullptr);
  EXPECT_EQ(context->leafIdentity.gid, 202);
  EXPECT_EQ(context->paramIndex, 0);
  const auto *snapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(context->pSnapshots, 0));
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->values.window.start, 2100);
  ASSERT_EQ(taosArrayGetSize(target.pAncestorContext->pReadScopeBindings), 1);
  const auto *binding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(target.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(binding, nullptr);
  EXPECT_EQ(binding->vgId, 7);
  EXPECT_EQ(binding->readInfoIndex, 1);
  EXPECT_NE(stProjectReaderCalcContext(&source, 8, 1, -1, &target), TSDB_CODE_SUCCESS);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, MultiGroupReadWithoutAncestorBindingKeepsLeafOnlyTemporaryContext) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curNodeId = 0;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 1))->gid = 202;
  appendReaderNonePolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 0);
  appendReaderAncestorContext(&source, 202, 0, 2000, 2100);
  appendReaderBinding(&source, 7, 1, 0);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  ASSERT_EQ(stProjectReaderCalcContext(&source, 7, 0, -1, &target), TSDB_CODE_SUCCESS);
  EXPECT_EQ(target.groupId, 101);
  EXPECT_EQ(target.curIdx, 0);
  EXPECT_EQ(target.pContextPolicy, nullptr);
  EXPECT_EQ(target.pAncestorContext, nullptr);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, ProjectsSameReadInfoIndexIndependentlyForEachActualNode) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(source.curGrpRead, nullptr);
  auto *currentRead = static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0));
  ASSERT_NE(currentRead, nullptr);
  currentRead->gid = 101;
  appendReaderPolicy(&source, 101, 0);
  appendReaderPolicy(&source, 202, 0);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);
  appendReaderAncestorContext(&source, 202, 0, 2000, 2100);
  appendReaderBinding(&source, 7, 0, 0);
  appendReaderBinding(&source, 8, 0, 1);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  int32_t code = stProjectReaderCalcContext(&source, 7, 0, -1, &target);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(target.groupId, 101);
    const auto *context = onlyProjectedReaderContext(target);
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->leafIdentity.gid, 101);
  }

  currentRead->gid = 202;
  code = stProjectReaderCalcContext(&source, 8, 0, -1, &target);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(target.groupId, 202);
    const auto *context = onlyProjectedReaderContext(target);
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->leafIdentity.gid, 202);
  }

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  tDestroyStreamContextPolicy(&target.pContextPolicy);
  tDestroyStreamAncestorContext(&target.pAncestorContext);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST(StreamReaderTest, RejectsAncestorDependencyWithoutBindingForActualNode) {
  SStreamRuntimeFuncInfo source = {};
  source.isMultiGroupCalc = true;
  source.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(source.curGrpRead, nullptr);
  static_cast<SSTriggerGroupReadInfo *>(taosArrayGet(source.curGrpRead, 0))->gid = 101;
  appendReaderPolicy(&source, 101, 0);
  appendReaderAncestorContext(&source, 101, 0, 1000, 1100);

  SStreamRuntimeFuncInfo target = {};
  target.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(target.pStreamPesudoFuncVals, nullptr);

  EXPECT_NE(stProjectReaderCalcContext(&source, 7, 0, -1, &target), TSDB_CODE_SUCCESS);

  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  taosArrayDestroy(source.curGrpRead);
  taosArrayDestroy(target.pStreamPesudoFuncVals);
}

TEST_F(StreamReaderCacheTest, NestedMissingPolicyRejectsBeforeScopedRead) {
  Stub scopedReadStubs;
  scopedReadStubs.set(taosHashGet, countScopedHashGet);
  scopedReadStubs.set(taosHashPut, countScopedHashPut);
  scopedReadStubs.set(getStreamDataCache, countScopedDataCache);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  bool finished = false;
  EXPECT_EQ(stRunnerFetchDataFromCache(&readInfo, &finished), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(readInfo.pBlock, nullptr);
  expectNoScopedRead();
}

TEST_F(StreamReaderCacheTest, NestedAncestorPolicyWithoutContextRejectsBeforeScopedRead) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);
  SStreamContextPolicyEntry entry = {};
  entry.gid = 101;
  entry.paramIndex = 0;
  entry.contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR;
  ASSERT_NE(taosArrayPush(policy.pEntries, &entry), nullptr);

  Stub scopedReadStubs;
  scopedReadStubs.set(taosHashGet, countScopedHashGet);
  scopedReadStubs.set(taosHashPut, countScopedHashPut);
  scopedReadStubs.set(getStreamDataCache, countScopedDataCache);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  bool finished = false;
  EXPECT_EQ(stRunnerFetchDataFromCache(&readInfo, &finished), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(readInfo.pBlock, nullptr);
  expectNoScopedRead();

  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, NestedTaskRejectsLegacyScopeBeforeCacheRead) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);

  Stub cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(releaseStreamDataCacheLease, releaseFakeCacheLease);
  cacheStubs.set(getStreamDataCache, makeEmptyLegacyIterator);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = 101;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  readInfo.cacheScope.gid = readInfo.gid;
  readInfo.hasCacheScope = true;
  bool finished = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_EQ(0, gScopedReadCalls.dataCache);
  EXPECT_EQ(0, taosHashGetSize(scopedIters_));
  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, NestedTaskRejectsPreboundPopulatedScopeWithoutRuntime) {
  SStreamContextPolicy policy = {};
  policy.pEntries = taosArrayInit(1, sizeof(SStreamContextPolicyEntry));
  ASSERT_NE(policy.pEntries, nullptr);
  SStreamCacheScope scope = {};
  scope.gid = 101;
  scope.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(scope.lineage.pScopes, nullptr);
  SScopeInstanceId scopeId = {};
  scopeId.layerIndex = 0;
  scopeId.triggerType = WINDOW_TYPE_INTERVAL;
  scopeId.openingTs = 100;
  scopeId.nativeDiscriminator = 7;
  ASSERT_NE(taosArrayPush(scope.lineage.pScopes, &scopeId), nullptr);

  Stub cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(releaseStreamDataCacheLease, releaseFakeCacheLease);
  cacheStubs.set(getStreamDataCacheScoped, makeEmptyScopedIterator);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = scope.gid;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pContextPolicy = &policy;
  readInfo.cacheScope = scope;
  readInfo.hasCacheScope = true;
  bool finished = false;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_FALSE(finished);
  EXPECT_EQ(nullptr, readInfo.pBlock);
  EXPECT_EQ(0, taosHashGetSize(scopedIters_));

  taosArrayDestroy(scope.lineage.pScopes);
  taosArrayDestroy(policy.pEntries);
}

TEST_F(StreamReaderCacheTest, ConcurrentExactScopeCleanupWaitsForActiveFetch) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  appendReaderPolicy(&runtime, runtime.groupId, 0);
  appendReaderAncestorContext(&runtime, runtime.groupId, 0, 1000, 1100);
  SStreamCacheReadInfo   readInfo = {};
  readInfo.taskInfo.streamId = kStreamId;
  readInfo.taskInfo.taskId = kTaskId;
  readInfo.taskInfo.sessionId = kSessionId;
  readInfo.gid = runtime.groupId;
  readInfo.start = 100;
  readInfo.end = 199;
  readInfo.pRuntime = &runtime;

  CacheReadConcurrencyGate       gate;
  ScopedCacheReadConcurrencyGate scopedGate(&gate);
  Stub                           cacheStubs;
  cacheStubs.set(acquireStreamDataCacheLease, acquireFakeCacheLease);
  cacheStubs.set(getStreamDataCacheScoped, makeEmptyScopedIterator);
  cacheStubs.set(getNextStreamDataCache, blockConcurrentCacheRead);
  cacheStubs.set(taosHashRemove, removeConcurrentCacheReadScope);

  auto fetch = std::async(std::launch::async, [&readInfo] {
    bool finished = false;
    return stRunnerFetchDataFromCache(&readInfo, &finished);
  });
  {
    std::unique_lock<std::mutex> lock(gate.mutex);
    ASSERT_TRUE(gate.condition.wait_for(lock, std::chrono::seconds(5), [&gate] { return gate.fetchEntered; }));
  }

  auto cleanup = std::async(std::launch::async, [&readInfo] { return stRemoveStreamCacheReadScope(&readInfo); });
  EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(gate.mutex);
    gate.allowFetch = true;
  }
  gate.condition.notify_all();

  ASSERT_EQ(fetch.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(fetch.get(), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cleanup.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(cleanup.get(), TSDB_CODE_SUCCESS);
  stClearStreamCacheReadScope(&readInfo);
  tDestroyStreamContextPolicy(&runtime.pContextPolicy);
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
