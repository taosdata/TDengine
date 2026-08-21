#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <initializer_list>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cJSON.h"
#include "cmdnodes.h"
#include "dataSink.h"
#include "dataSinkMgt.h"
#include "snode.h"
#include "stream.h"
#include "streamInt.h"
#include "streamRunner.h"
#include "streamTaskStats.h"
#include "stub.h"
#include "tdatablock.h"

extern "C" {
#include "tcurl.h"

int32_t stmHbAddTaskStatus(int64_t streamId, SStreamHbMsg *pMsg, SStreamTask *pTask, SStreamTaskStats *pStats);
}

namespace {

struct RunnerCallState {
  int32_t                                  resetCalls = 0;
  int32_t                                  executeCalls = 0;
  int32_t                                  forceOutputCalls = 0;
  int32_t                                  sinkCalls = 0;
  int32_t                                  notifyCalls = 0;
  int32_t                                  notifyTargets = 0;
  int32_t                                  sinkCode = TSDB_CODE_SUCCESS;
  int32_t                                  notifyCode = TSDB_CODE_SUCCESS;
  int32_t                                  notifyBuildCode = TSDB_CODE_SUCCESS;
  int32_t                                  forceOutputCode = TSDB_CODE_SUCCESS;
  bool                                     sinkBlockIsNull = false;
  bool                                     autoCreateTable = false;
  int64_t                                  groupId = 0;
  int64_t                                  monotonicUs = 1000;
  char                                     tableName[TSDB_TABLE_NAME_LEN] = {0};
  std::vector<int32_t>                     executeCodes;
  std::vector<int64_t>                     executeReadyTimes;
  std::vector<bool>                        executeFinished;
  std::vector<SSDataBlock *>               executeBlocks;
  std::vector<int32_t>                     executeOutIndexes;
  std::vector<bool>                        autoCreateTableCalls;
  std::vector<const SSDataBlock *>         sinkBlocks;
  SStreamRuntimeInfo                      *pRuntimeInfo = nullptr;
  std::vector<std::pair<int64_t, int64_t>> executeBlockingIntervals;
};

RunnerCallState          gCalls;
std::vector<std::string> gRunnerDebugLogs;
std::atomic<bool>        gBlockRunnerPeriodLog{false};
std::atomic<bool>        gRunnerPeriodLogEntered{false};
std::atomic<int32_t>     gRunnerUndeployCalls{0};

struct RunnerRotationInterleave {
  int32_t                   calls = 0;
  int32_t                   code = TSDB_CODE_SUCCESS;
  bool                      rotated = false;
  SStreamTaskPeriodSnapshot snapshot = {};
};

RunnerRotationInterleave gRunnerRotationInterleave;

void captureRunnerDebugLog(const char *, int32_t, int32_t, const char *format, ...) {
  char    buffer[4096] = {0};
  va_list args;
  va_start(args, format);
  int32_t len = vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  if (len >= 0 && len < sizeof(buffer)) gRunnerDebugLogs.emplace_back(buffer);
  if (strstr(buffer, "record=task_period task_type=runner") != nullptr) {
    gRunnerPeriodLogEntered.store(true);
    while (gBlockRunnerPeriodLog.load()) std::this_thread::yield();
  }
}

void captureRunnerUndeploy(void *) { ++gRunnerUndeployCalls; }

void rotateOnRunnerGaugeUpdate(SStreamTaskStats *pStats, int64_t, int64_t, int64_t) {
  ++gRunnerRotationInterleave.calls;
  gRunnerRotationInterleave.code = stTaskStatsRotatePeriod(
      pStats, STREAM_STATS_PERIOD_US + 1, &gRunnerRotationInterleave.snapshot, &gRunnerRotationInterleave.rotated);
}

int32_t failRunnerStatsLog(SStreamRunnerTask *, const SStreamTaskPeriodSnapshot *) { return TSDB_CODE_FAILED; }

class ScopedRunnerDebugLogCapture {
 public:
  ScopedRunnerDebugLogCapture() : previousDebugFlag_(stDebugFlag) {
    gRunnerDebugLogs.clear();
    stub_.set(taosPrintLog, captureRunnerDebugLog);
    stDebugFlag = previousDebugFlag_ | DEBUG_DEBUG | DEBUG_FILE;
  }

  ~ScopedRunnerDebugLogCapture() {
    stDebugFlag = previousDebugFlag_;
    gRunnerDebugLogs.clear();
  }

 private:
  Stub    stub_;
  int32_t previousDebugFlag_;
};

SSDataBlock             *gRunnerResultBlock = nullptr;
std::vector<int32_t>     gRunnerSinkCodes;
std::vector<std::string> gRunnerNotifyPayloads;

enum class RunnerPostAdmissionMappingMutation { None, RemoveTarget, DuplicateTarget };

RunnerPostAdmissionMappingMutation gRunnerPostAdmissionMappingMutation = RunnerPostAdmissionMappingMutation::None;
SStreamAncestorContext            *gRunnerPostAdmissionContext = nullptr;
bool                               gRunnerPostAdmissionMutationApplied = false;
int32_t                            gRunnerPostAdmissionMutationCode = TSDB_CODE_SUCCESS;

void applyRunnerPostAdmissionMappingMutation();

struct CacheResponseState {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t msgType = 0;
  int32_t contLen = 0;
  bool    contentIsNull = true;
};

int32_t              gResponseCode = TSDB_CODE_SUCCESS;
CacheResponseState   gCacheResponse;
std::vector<int32_t> gCacheResponseValues;

int32_t mockRpcSendResponse(const SRpcMsg *pMsg) {
  gResponseCode = pMsg->code;
  if (pMsg->msgType == TDMT_STREAM_FETCH_FROM_CACHE_RSP) {
    gCacheResponse.code = pMsg->code;
    gCacheResponse.msgType = pMsg->msgType;
    gCacheResponse.contLen = pMsg->contLen;
    gCacheResponse.contentIsNull = pMsg->pCont == nullptr;
  }
  if (pMsg->msgType == TDMT_STREAM_FETCH_FROM_CACHE_RSP && pMsg->code == TSDB_CODE_SUCCESS && pMsg->pCont != nullptr) {
    const auto *response = static_cast<const SRetrieveTableRsp *>(pMsg->pCont);
    if (be64toh(response->numOfRows) > 0) {
      auto *block = static_cast<SSDataBlock *>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
      EXPECT_NE(nullptr, block);
      if (block != nullptr) {
        EXPECT_EQ(TSDB_CODE_SUCCESS, blockDecodeInternal(block, response->data + INT_BYTES * 2, nullptr));
        if (taosArrayGetSize(block->pDataBlock) > 1) {
          const auto *values = static_cast<const SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
          for (int32_t row = 0; row < block->info.rows; ++row) {
            gCacheResponseValues.push_back(*reinterpret_cast<const int32_t *>(colDataGetData(values, row)));
          }
        }
        blockDataDestroy(block);
      }
    }
  }
  rpcFreeCont(pMsg->pCont);
  return TSDB_CODE_SUCCESS;
}

void *failRpcMallocCont(int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t failCacheMaintenance(bool) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t rejectScopedCacheLease(int64_t, int64_t, int64_t, SStreamDataCacheLease **, void **) {
  return TSDB_CODE_INVALID_PARA;
}

int32_t mockStreamClearStatesForOperators(qTaskInfo_t) {
  ++gCalls.resetCalls;
  return TSDB_CODE_SUCCESS;
}

int32_t mockStreamExecuteTask(qTaskInfo_t, SSDataBlock **ppBlock, bool *pFinished) {
  const size_t index = static_cast<size_t>(gCalls.executeCalls++);
  if (gCalls.pRuntimeInfo != nullptr && gCalls.pRuntimeInfo->blockingStatsFp != nullptr) {
    for (const auto &interval : gCalls.executeBlockingIntervals) {
      gCalls.monotonicUs = interval.first;
      gCalls.pRuntimeInfo->blockingStatsFp(gCalls.pRuntimeInfo->pBlockingStatsParam, true);
      gCalls.monotonicUs = interval.second;
      gCalls.pRuntimeInfo->blockingStatsFp(gCalls.pRuntimeInfo->pBlockingStatsParam, false);
    }
  }
  if (index < gCalls.executeReadyTimes.size()) {
    gCalls.monotonicUs = gCalls.executeReadyTimes[index];
  }
  if (gCalls.pRuntimeInfo != nullptr && index < gCalls.executeOutIndexes.size()) {
    gCalls.pRuntimeInfo->funcInfo.curOutIdx = gCalls.executeOutIndexes[index];
  }
  *ppBlock = index < gCalls.executeBlocks.size() ? gCalls.executeBlocks[index] : nullptr;
  *pFinished = index < gCalls.executeFinished.size() ? gCalls.executeFinished[index] : true;
  return index < gCalls.executeCodes.size() ? gCalls.executeCodes[index] : TSDB_CODE_SUCCESS;
}

int32_t mockStreamForceOutput(qTaskInfo_t, SSDataBlock **ppBlock, int32_t) {
  ++gCalls.forceOutputCalls;
  *ppBlock = nullptr;
  return gCalls.forceOutputCode;
}

int32_t mockDsPutDataBlock(DataSinkHandle, const SInputData *pInput, bool *pContinue) {
  ++gCalls.sinkCalls;
  gCalls.sinkBlockIsNull = pInput->pData == nullptr;
  gCalls.autoCreateTable = pInput->pStreamDataInserterInfo->isAutoCreateTable;
  gCalls.autoCreateTableCalls.push_back(pInput->pStreamDataInserterInfo->isAutoCreateTable);
  gCalls.sinkBlocks.push_back(pInput->pData);
  gCalls.groupId = pInput->pStreamDataInserterInfo->groupId;
  tstrncpy(gCalls.tableName, pInput->pStreamDataInserterInfo->tbName, sizeof(gCalls.tableName));
  *pContinue = false;
  return gCalls.sinkCalls <= gRunnerSinkCodes.size() ? gRunnerSinkCodes[gCalls.sinkCalls - 1] : gCalls.sinkCode;
}

int32_t mockStreamSendNotifyContent(SStreamTask *, const char *, const char *, int32_t, int64_t,
                                    const SArray *pNotifyAddrUrls, int32_t, const SSTriggerCalcParam *, int32_t) {
  ++gCalls.notifyCalls;
  gCalls.notifyTargets = static_cast<int32_t>(taosArrayGetSize(pNotifyAddrUrls));
  return gCalls.notifyCode;
}

int32_t mockStreamSendNotifyContentWithResult(SStreamTask *, const char *, const char *, int32_t, int64_t,
                                              const SArray *pNotifyAddrUrls, int32_t, const SSTriggerCalcParam *,
                                              int32_t, bool *pAttempted, bool *pDelivered) {
  ++gCalls.notifyCalls;
  gCalls.notifyTargets = static_cast<int32_t>(taosArrayGetSize(pNotifyAddrUrls));
  *pAttempted = true;
  *pDelivered = gCalls.notifyCode == TSDB_CODE_SUCCESS;
  return TSDB_CODE_SUCCESS;
}

int32_t mockTcurlConnectFailure(CURL **, const char *) { return TSDB_CODE_FAILED; }

int32_t mockTcurlConnectSuccess(CURL **ppConn, const char *) {
  *ppConn = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t mockTcurlSendFailure(SCURL *, const void *, size_t, size_t *pSent, curl_off_t, unsigned int) {
  *pSent = 0;
  return TSDB_CODE_FAILED;
}

int32_t mockStreamBuildBlockResultNotifyContent(const SStreamRunnerTask *, const SSDataBlock *, char **ppContent,
                                                const SArray *, int32_t, int32_t, bool *pHasNotifyRows) {
  if (gCalls.notifyBuildCode != TSDB_CODE_SUCCESS) return gCalls.notifyBuildCode;
  applyRunnerPostAdmissionMappingMutation();
  if (gRunnerPostAdmissionMutationCode != TSDB_CODE_SUCCESS) return gRunnerPostAdmissionMutationCode;
  *ppContent = taosStrdup("{}");
  *pHasNotifyRows = true;
  return *ppContent == nullptr ? terrno : TSDB_CODE_SUCCESS;
}

int64_t mockStreamTaskGetMonotonicUs() { return gCalls.monotonicUs; }

int32_t mockRunnerExecuteTask(qTaskInfo_t, SSDataBlock **ppBlock, bool *pFinished) {
  ++gCalls.executeCalls;
  *ppBlock = gRunnerResultBlock;
  *pFinished = true;
  return TSDB_CODE_SUCCESS;
}

int32_t mockRunnerForceOutput(qTaskInfo_t, SSDataBlock **ppBlock, int32_t) {
  ++gCalls.forceOutputCalls;
  *ppBlock = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t captureRunnerNotifyConnect(CURL **ppConn, const char *) {
  *ppConn = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t captureRunnerNotifySend(SCURL *, const void *pBuffer, size_t len, size_t *pSent, curl_off_t, unsigned int) {
  gRunnerNotifyPayloads.emplace_back(static_cast<const char *>(pBuffer), len);
  *pSent = len;
  return CURLE_OK;
}

void captureRunnerNotifyClose(void *pConn) {
  auto *pCurl = static_cast<SCURL *>(pConn);
  taosMemoryFreeClear(pCurl->url);
  pCurl->pConn = nullptr;
}

std::vector<std::string> runnerNotifyTriggerIds(const std::string &payload) {
  std::vector<std::string> result;
  cJSON *root = cJSON_Parse(payload.c_str());
  if (root == nullptr) {
    ADD_FAILURE() << "invalid notification JSON: " << payload;
    return result;
  }
  cJSON *streams = cJSON_GetObjectItemCaseSensitive(root, "streams");
  cJSON *stream = cJSON_IsArray(streams) ? cJSON_GetArrayItem(streams, 0) : nullptr;
  cJSON *events = cJSON_IsObject(stream) ? cJSON_GetObjectItemCaseSensitive(stream, "events") : nullptr;
  if (!cJSON_IsArray(events)) {
    ADD_FAILURE() << "notification has no events array: " << payload;
  } else {
    for (int32_t i = 0; i < cJSON_GetArraySize(events); ++i) {
      cJSON *event = cJSON_GetArrayItem(events, i);
      cJSON *triggerId = cJSON_IsObject(event) ? cJSON_GetObjectItemCaseSensitive(event, "triggerId") : nullptr;
      if (!cJSON_IsString(triggerId)) {
        ADD_FAILURE() << "notification event has no string triggerId: " << payload;
        result.clear();
        break;
      }
      result.emplace_back(cJSON_GetStringValue(triggerId));
    }
  }
  cJSON_Delete(root);
  return result;
}

std::string runnerNotifyTriggerId(const std::string &payload) {
  const std::vector<std::string> triggerIds = runnerNotifyTriggerIds(payload);
  return triggerIds.empty() ? std::string() : triggerIds.front();
}

void destroyRuntimeInfoInList(SList *pList) {
  SListNode *pNode = tdListGetHead(pList);
  while (pNode != nullptr) {
    auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(pNode->data);
    tDestroyStRtFuncInfo(&pExec->runtimeInfo.funcInfo);
    blockDataDestroy(pExec->pOutBlock);
    pExec->pOutBlock = nullptr;
    pNode = pNode->dl_next_;
  }
}

SStreamAncestorContext *makeRunnerAncestorContext(int64_t gid, int32_t paramIndex) {
  auto *context = static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(context, nullptr);
  if (context == nullptr) return nullptr;
  context->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(context->pParamContexts, nullptr);
  if (context->pParamContexts == nullptr) return context;

  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.openingTs = 1000;
  param.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  EXPECT_NE(param.leafIdentity.lineage.pScopes, nullptr);
  const SScopeInstanceId scope = {
      .layerIndex = 0, .triggerType = WINDOW_TYPE_INTERVAL, .openingTs = 100, .nativeDiscriminator = 1};
  EXPECT_NE(taosArrayPush(param.leafIdentity.lineage.pScopes, &scope), nullptr);
  param.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  EXPECT_NE(param.pSnapshots, nullptr);
  const SWindowAncestorSnapshot snapshot = {
      .layerIndex = 0, .triggerType = WINDOW_TYPE_INTERVAL, .values = {.sliding = {.currentTs = 100}}};
  EXPECT_NE(taosArrayPush(param.pSnapshots, &snapshot), nullptr);
  EXPECT_NE(taosArrayPush(context->pParamContexts, &param), nullptr);
  return context;
}

SStreamContextPolicyEntry makeRunnerContextPolicyEntry(int64_t gid, int32_t paramIndex, int8_t contextPolicy) {
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = contextPolicy;
  return entry;
}

SStreamContextPolicy *makeRunnerContextPolicy(std::initializer_list<SStreamContextPolicyEntry> entries = {}) {
  auto *policy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
  EXPECT_NE(policy, nullptr);
  if (policy == nullptr) return nullptr;
  policy->pEntries = taosArrayInit(entries.size() == 0 ? 1 : entries.size(), sizeof(SStreamContextPolicyEntry));
  EXPECT_NE(policy->pEntries, nullptr);
  if (policy->pEntries == nullptr) return policy;
  for (const auto &entry : entries) {
    EXPECT_NE(taosArrayPush(policy->pEntries, &entry), nullptr);
  }
  return policy;
}

struct OwnedTriggerCalcRequest {
  OwnedTriggerCalcRequest() = default;
  ~OwnedTriggerCalcRequest() { tDestroySTriggerCalcRequest(&value); }
  OwnedTriggerCalcRequest(const OwnedTriggerCalcRequest &) = delete;
  OwnedTriggerCalcRequest &operator=(const OwnedTriggerCalcRequest &) = delete;
  OwnedTriggerCalcRequest(OwnedTriggerCalcRequest &&) = delete;
  OwnedTriggerCalcRequest &operator=(OwnedTriggerCalcRequest &&) = delete;

  SSTriggerCalcRequest value = {};
};

bool appendRunnerGroupCalcInfo(SSTriggerCalcRequest *request, int64_t gid) {
  SSTriggerGroupCalcInfo info = {};
  info.pParams = taosArrayInit(3, sizeof(SSTriggerCalcParam));
  if (info.pParams == nullptr) return false;

  const SSTriggerCalcParam noContext = {.notifyType = STRIGGER_EVENT_WINDOW_NONE};
  const SSTriggerCalcParam target = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  if (taosArrayPush(info.pParams, &noContext) == nullptr || taosArrayPush(info.pParams, &noContext) == nullptr ||
      taosArrayPush(info.pParams, &target) == nullptr) {
    taosArrayDestroyEx(info.pParams, tDestroySSTriggerCalcParam);
    return false;
  }
  if (tSimpleHashPut(request->pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)) != TSDB_CODE_SUCCESS) {
    taosArrayDestroyEx(info.pParams, tDestroySSTriggerCalcParam);
    return false;
  }
  info.pParams = nullptr;
  return true;
}

bool appendRunnerAncestorMapping(SStreamAncestorContext *context, int64_t gid, int32_t paramIndex, TSKEY ancestorStart,
                                 TSKEY leafStart) {
  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.openingTs = leafStart;
  param.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  if (param.leafIdentity.lineage.pScopes == nullptr) return false;

  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_INTERVAL;
  scope.openingTs = ancestorStart;
  scope.nativeDiscriminator = gid + paramIndex;
  if (taosArrayPush(param.leafIdentity.lineage.pScopes, &scope) == nullptr) {
    taosArrayDestroy(param.leafIdentity.lineage.pScopes);
    return false;
  }
  param.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  if (param.pSnapshots == nullptr) {
    taosArrayDestroy(param.leafIdentity.lineage.pScopes);
    return false;
  }
  const SWindowAncestorSnapshot snapshot = {
      .layerIndex = 0, .triggerType = WINDOW_TYPE_INTERVAL, .values = {.sliding = {.currentTs = 100}}};
  if (taosArrayPush(param.pSnapshots, &snapshot) == nullptr) {
    taosArrayDestroy(param.leafIdentity.lineage.pScopes);
    taosArrayDestroy(param.pSnapshots);
    return false;
  }
  if (taosArrayPush(context->pParamContexts, &param) == nullptr) {
    taosArrayDestroy(param.leafIdentity.lineage.pScopes);
    taosArrayDestroy(param.pSnapshots);
    return false;
  }
  param.leafIdentity.lineage.pScopes = nullptr;
  param.pSnapshots = nullptr;
  return true;
}

void applyRunnerPostAdmissionMappingMutation() {
  if (gRunnerPostAdmissionMappingMutation == RunnerPostAdmissionMappingMutation::None ||
      gRunnerPostAdmissionMutationApplied) {
    return;
  }
  gRunnerPostAdmissionMutationApplied = true;
  if (gRunnerPostAdmissionContext == nullptr || gRunnerPostAdmissionContext->pParamContexts == nullptr) {
    gRunnerPostAdmissionMutationCode = TSDB_CODE_INVALID_PARA;
    return;
  }

  if (gRunnerPostAdmissionMappingMutation == RunnerPostAdmissionMappingMutation::RemoveTarget) {
    for (int32_t i = 0; i < taosArrayGetSize(gRunnerPostAdmissionContext->pParamContexts); ++i) {
      auto *param =
          static_cast<SStreamAncestorParamContext *>(taosArrayGet(gRunnerPostAdmissionContext->pParamContexts, i));
      if (param != nullptr && param->leafIdentity.gid == 42 && param->paramIndex == 2) {
        param->paramIndex = 1;
        return;
      }
    }
    gRunnerPostAdmissionMutationCode = TSDB_CODE_INVALID_PARA;
    return;
  }

  if (!appendRunnerAncestorMapping(gRunnerPostAdmissionContext, 42, 2, 100, 1000)) {
    gRunnerPostAdmissionMutationCode = terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_OUT_OF_MEMORY : terrno;
  }
}

enum class RunnerAncestorMappingOrder { TargetFirst, TargetMiddle, TargetLast };

bool makeNestedRunnerNoticeRequest(SSTriggerCalcRequest *request, TSKEY targetAncestorStart,
                                   RunnerAncestorMappingOrder mappingOrder) {
  constexpr int64_t     kTargetGid = 42;
  constexpr int64_t     kOtherGid = 84;
  constexpr int32_t     kTargetParamIndex = 2;
  constexpr TSKEY       kLeafStart = 1000;

  request->execId = 7;
  request->gid = kTargetGid;
  request->sessionId = 1;
  request->triggerType = STREAM_TRIGGER_COUNT;
  request->isWindowTrigger = true;
  request->isMultiGroupCalc = true;
  request->pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (request->pGroupCalcInfos == nullptr) return false;
  tSimpleHashSetFreeFp(request->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  if (!appendRunnerGroupCalcInfo(request, kTargetGid) || !appendRunnerGroupCalcInfo(request, kOtherGid)) return false;

  request->pContextPolicy = makeRunnerContextPolicy({
      {.gid = kTargetGid, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
      {.gid = kTargetGid, .paramIndex = 1, .contextPolicy = STREAM_CONTEXT_POLICY_NONE},
      {.gid = kTargetGid, .paramIndex = kTargetParamIndex, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
      {.gid = kOtherGid, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_NONE},
      {.gid = kOtherGid, .paramIndex = 1, .contextPolicy = STREAM_CONTEXT_POLICY_NONE},
      {.gid = kOtherGid, .paramIndex = kTargetParamIndex, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
  });
  if (request->pContextPolicy == nullptr || taosArrayGetSize(request->pContextPolicy->pEntries) != 6) return false;

  request->pAncestorContext =
      static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  if (request->pAncestorContext == nullptr) return false;
  request->pAncestorContext->pParamContexts = taosArrayInit(3, sizeof(SStreamAncestorParamContext));
  if (request->pAncestorContext->pParamContexts == nullptr) return false;
  switch (mappingOrder) {
    case RunnerAncestorMappingOrder::TargetFirst:
      return appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, kTargetParamIndex, targetAncestorStart,
                                         kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kOtherGid, kTargetParamIndex, 300, kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, 0, 400, kLeafStart);
    case RunnerAncestorMappingOrder::TargetMiddle:
      return appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, 0, 400, kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, kTargetParamIndex, targetAncestorStart,
                                         kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kOtherGid, kTargetParamIndex, 300, kLeafStart);
    case RunnerAncestorMappingOrder::TargetLast:
      return appendRunnerAncestorMapping(request->pAncestorContext, kOtherGid, kTargetParamIndex, 300, kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, 0, 400, kLeafStart) &&
             appendRunnerAncestorMapping(request->pAncestorContext, kTargetGid, kTargetParamIndex, targetAncestorStart,
                                         kLeafStart);
  }
  return false;
}

bool prepareRunnerNoticeParams(SStreamRunnerTaskExecution *exec) {
  SArray *params = taosArrayInit(3, sizeof(SSTriggerCalcParam));
  if (params == nullptr) return false;
  const SSTriggerCalcParam noContext = {.notifyType = STRIGGER_EVENT_WINDOW_NONE};
  const SSTriggerCalcParam target = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  if (taosArrayPush(params, &noContext) == nullptr || taosArrayPush(params, &noContext) == nullptr ||
      taosArrayPush(params, &target) == nullptr) {
    taosArrayDestroyEx(params, tDestroySSTriggerCalcParam);
    return false;
  }
  taosArrayDestroyEx(exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, tDestroySSTriggerCalcParam);
  exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = params;
  exec->runtimeInfo.funcInfo.isMultiGroupCalc = true;
  return true;
}

bool appendRunnerDualLeafGroupCalcInfo(SSTriggerCalcRequest *request, int64_t gid) {
  SSTriggerGroupCalcInfo info = {};
  info.pParams = taosArrayInit(4, sizeof(SSTriggerCalcParam));
  if (info.pParams == nullptr) return false;

  SSTriggerCalcParam idle = {};
  idle.idlestart = 10;
  idle.idleend = 20;
  idle.notifyType = STRIGGER_EVENT_IDLE;
  SSTriggerCalcParam noNotice = {};
  noNotice.notifyType = STRIGGER_EVENT_WINDOW_NONE;
  SSTriggerCalcParam firstLeaf = {};
  firstLeaf.wstart = 1000;
  firstLeaf.wend = 1099;
  firstLeaf.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  SSTriggerCalcParam secondLeaf = firstLeaf;
  if (taosArrayPush(info.pParams, &idle) == nullptr || taosArrayPush(info.pParams, &noNotice) == nullptr ||
      taosArrayPush(info.pParams, &firstLeaf) == nullptr || taosArrayPush(info.pParams, &secondLeaf) == nullptr) {
    taosArrayDestroyEx(info.pParams, tDestroySSTriggerCalcParam);
    return false;
  }
  if (tSimpleHashPut(request->pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)) != TSDB_CODE_SUCCESS) {
    taosArrayDestroyEx(info.pParams, tDestroySSTriggerCalcParam);
    return false;
  }
  info.pParams = nullptr;
  return true;
}

bool makeNestedRunnerDualLeafNoticeRequest(SSTriggerCalcRequest *request) {
  constexpr int64_t kGid = 42;
  request->execId = 7;
  request->gid = kGid;
  request->sessionId = 1;
  request->triggerType = STREAM_TRIGGER_COUNT;
  request->isWindowTrigger = true;
  request->isMultiGroupCalc = true;
  request->pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (request->pGroupCalcInfos == nullptr) return false;
  tSimpleHashSetFreeFp(request->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  if (!appendRunnerDualLeafGroupCalcInfo(request, kGid)) return false;

  request->pContextPolicy = makeRunnerContextPolicy({
      makeRunnerContextPolicyEntry(kGid, 0, STREAM_CONTEXT_POLICY_NONE),
      makeRunnerContextPolicyEntry(kGid, 1, STREAM_CONTEXT_POLICY_NONE),
      makeRunnerContextPolicyEntry(kGid, 2, STREAM_CONTEXT_POLICY_ANCESTOR),
      makeRunnerContextPolicyEntry(kGid, 3, STREAM_CONTEXT_POLICY_ANCESTOR),
  });
  if (request->pContextPolicy == nullptr) return false;
  request->pAncestorContext =
      static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  if (request->pAncestorContext == nullptr) return false;
  request->pAncestorContext->pParamContexts = taosArrayInit(2, sizeof(SStreamAncestorParamContext));
  if (request->pAncestorContext->pParamContexts == nullptr) return false;
  return appendRunnerAncestorMapping(request->pAncestorContext, kGid, 2, 100, 1000) &&
         appendRunnerAncestorMapping(request->pAncestorContext, kGid, 3, 200, 1000);
}

bool prepareRunnerDualLeafNoticeParams(SStreamRunnerTaskExecution *exec) {
  SSTriggerCalcRequest owner = {};
  owner.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (owner.pGroupCalcInfos == nullptr) return false;
  tSimpleHashSetFreeFp(owner.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  const bool              appended = appendRunnerDualLeafGroupCalcInfo(&owner, 42);
  const int64_t           gid = 42;
  SSTriggerGroupCalcInfo *info =
      appended ? static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(owner.pGroupCalcInfos, &gid, sizeof(gid)))
               : nullptr;
  if (info == nullptr) {
    tSimpleHashCleanup(owner.pGroupCalcInfos);
    return false;
  }
  taosArrayDestroyEx(exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, tDestroySSTriggerCalcParam);
  exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = info->pParams;
  info->pParams = nullptr;
  exec->runtimeInfo.funcInfo.isMultiGroupCalc = true;
  tSimpleHashCleanup(owner.pGroupCalcInfos);
  return true;
}

bool makeNestedRunnerGroupNoticeRequest(SSTriggerCalcRequest *request, int32_t notifyType) {
  request->execId = 7;
  request->gid = 42;
  request->sessionId = 1;
  request->triggerType = STREAM_TRIGGER_COUNT;
  request->isWindowTrigger = true;
  request->params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  if (request->params == nullptr) return false;
  SSTriggerCalcParam param = {};
  param.idlestart = 10;
  param.idleend = 20;
  param.notifyType = notifyType;
  if (taosArrayPush(request->params, &param) == nullptr) return false;
  request->pContextPolicy =
      makeRunnerContextPolicy({makeRunnerContextPolicyEntry(request->gid, 0, STREAM_CONTEXT_POLICY_NONE)});
  return request->pContextPolicy != nullptr;
}

bool configureRunnerEventNoticeRequest(SSTriggerCalcRequest *request, SStreamRunnerTaskExecution *exec,
                                       int32_t runtimeTriggerType, int8_t leafTriggerType, int64_t nativeDiscriminator,
                                       const char *extraNotifyContent) {
  if (!prepareRunnerNoticeParams(exec) ||
      !makeNestedRunnerNoticeRequest(request, 100, RunnerAncestorMappingOrder::TargetFirst)) {
    return false;
  }
  request->triggerType = runtimeTriggerType;
  SStreamAncestorParamContext *mapping = nullptr;
  for (int32_t i = 0; i < taosArrayGetSize(request->pAncestorContext->pParamContexts); ++i) {
    auto *candidate =
        static_cast<SStreamAncestorParamContext *>(taosArrayGet(request->pAncestorContext->pParamContexts, i));
    if (candidate != nullptr && candidate->leafIdentity.gid == 42 && candidate->paramIndex == 2) {
      mapping = candidate;
      break;
    }
  }
  if (mapping == nullptr) return false;
  mapping->leafIdentity.triggerType = leafTriggerType;
  mapping->leafIdentity.nativeDiscriminator = nativeDiscriminator;

  auto *runtimeParam =
      static_cast<SSTriggerCalcParam *>(taosArrayGet(exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, 2));
  if (runtimeParam == nullptr) return false;
  runtimeParam->extraNotifyContent = extraNotifyContent == nullptr ? nullptr : taosStrdup(extraNotifyContent);
  return extraNotifyContent == nullptr || runtimeParam->extraNotifyContent != nullptr;
}

void setRunnerPolicy(SSTriggerCalcRequest *request, int32_t paramIndex, int8_t contextPolicy) {
  request->pContextPolicy =
      makeRunnerContextPolicy({{.gid = request->gid, .paramIndex = paramIndex, .contextPolicy = contextPolicy}});
}

void addRunnerGroupCalcInfo(SSTriggerCalcRequest *request, int64_t gid) {
  SSTriggerGroupCalcInfo info = {};
  info.pParams = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(info.pParams, nullptr);
  const SSTriggerCalcParam window = {.wstart = gid, .wend = gid + 99, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(info.pParams, &window), nullptr);
  ASSERT_EQ(tSimpleHashPut(request->pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)), TSDB_CODE_SUCCESS);
}

SSDataBlock *makeCacheBlock(TSKEY ts, int32_t value);

class StreamRunnerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gCalls = {};
    gBlockRunnerPeriodLog.store(false);
    gRunnerPeriodLogEntered.store(false);
    gRunnerUndeployCalls.store(0);
    gResponseCode = TSDB_CODE_SUCCESS;
    gCacheResponse = {};
    gCacheResponseValues.clear();
    gRunnerResultBlock = nullptr;
    gRunnerSinkCodes.clear();
    gRunnerPostAdmissionMappingMutation = RunnerPostAdmissionMappingMutation::None;
    gRunnerPostAdmissionContext = nullptr;
    gRunnerPostAdmissionMutationApplied = false;
    gRunnerPostAdmissionMutationCode = TSDB_CODE_SUCCESS;
    stub_.set(streamClearStatesForOperators, mockStreamClearStatesForOperators);
    stub_.set(streamExecuteTask, mockStreamExecuteTask);
    stub_.set(streamForceOutput, mockStreamForceOutput);
    stub_.set(dsPutDataBlock, mockDsPutDataBlock);
    stub_.set(streamSendNotifyContent, mockStreamSendNotifyContent);
    stub_.set(streamSendNotifyContentWithResult, mockStreamSendNotifyContentWithResult);
    stub_.set(streamBuildBlockResultNotifyContent, mockStreamBuildBlockResultNotifyContent);
    stub_.set(streamTaskGetMonotonicUs, mockStreamTaskGetMonotonicUs);

    ASSERT_EQ(taosThreadMutexInit(&task_.execMgr.lock, nullptr), TSDB_CODE_SUCCESS);
    task_.execMgr.lockInited = true;
    task_.execMgr.pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    task_.execMgr.pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
    ASSERT_NE(task_.execMgr.pFreeExecs, nullptr);
    ASSERT_NE(task_.execMgr.pRunningExecs, nullptr);

    auto *pExec = static_cast<SStreamRunnerTaskExecution *>(tdListReserve(task_.execMgr.pFreeExecs));
    ASSERT_NE(pExec, nullptr);
    pExec->pExecutor = &executorSentinel_;
    pExec->pSinkHandle = &sinkSentinel_;
    pExec->runtimeInfo.execId = 7;
    tstrncpy(pExec->tbname, "out_zero_window", sizeof(pExec->tbname));
    pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(SSTriggerCalcParam));
    ASSERT_NE(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
    SSTriggerCalcParam staleWindow = {.wstart = 1000, .wend = 2000};
    ASSERT_NE(taosArrayPush(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, &staleWindow), nullptr);

    task_.topTask = true;
    task_.task.type = STREAM_RUNNER_TASK;
    task_.output.outTblType = TSDB_NORMAL_TABLE;
    task_.task.streamId = 0x1234;
    task_.task.taskId = 23;
    task_.task.seriousId = 45;
    task_.task.nodeId = 67;
    task_.task.status = STREAM_STATUS_RUNNING;
    task_.parallelExecutionNun = 3;
    task_.notification.pNotifyAddrUrls = taosArrayInit(1, sizeof(char *));
    ASSERT_NE(task_.notification.pNotifyAddrUrls, nullptr);
    char *notifyUrl = notifyUrl_;
    ASSERT_NE(taosArrayPush(task_.notification.pNotifyAddrUrls, &notifyUrl), nullptr);

    request_.brandNew = true;
    request_.execId = -1;
    request_.createTable = 1;
    request_.gid = 42;
    request_.sessionId = 1;

    ASSERT_EQ(stTaskStatsCreate(STREAM_RUNNER_TASK, STREAM_METRIC_DELIVERED_OUTPUT | STREAM_METRIC_RESULT_LATENCY, 1,
                                1000, &task_.pStats),
              TSDB_CODE_SUCCESS);

    ASSERT_EQ(gStreamMgmt.taskMap, nullptr);
    gStreamMgmt.taskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    ASSERT_NE(gStreamMgmt.taskMap, nullptr);
    SStreamTask *pTask = &task_.task;
    ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &task_.task.streamId,
                          sizeof(task_.task.streamId) + sizeof(task_.task.taskId), &pTask, POINTER_BYTES),
              TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    if (cacheTaskRegistered_) {
      EXPECT_EQ(cacheTask_.task.entryLock, 0);
      EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, &cacheTask_.task.streamId,
                               sizeof(cacheTask_.task.streamId) + sizeof(cacheTask_.task.taskId)),
                TSDB_CODE_SUCCESS);
      cacheTaskRegistered_ = false;
    }
    taosHashCleanup(cacheRealtime_.pCalcDataCacheIters);
    cacheRealtime_.pCalcDataCacheIters = nullptr;
    if (dataSinkOwned_) {
      destroyDataSinkMgr();
      dataSinkOwned_ = false;
    }
    if (snode_ != nullptr) {
      sndClose(snode_);
      snode_ = nullptr;
    }
    EXPECT_EQ(task_.task.entryLock, 0);
    EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, &task_.task.streamId,
                             sizeof(task_.task.streamId) + sizeof(task_.task.taskId)),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(taosHashGetSize(gStreamMgmt.taskMap), 0);
    taosHashCleanup(gStreamMgmt.taskMap);
    gStreamMgmt.taskMap = nullptr;
    tDestroySTriggerCalcRequest(&request_);
    if (task_.execMgr.pRunningExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pRunningExecs);
      task_.execMgr.pRunningExecs = static_cast<SList *>(tdListFree(task_.execMgr.pRunningExecs));
    }
    if (task_.execMgr.pFreeExecs != nullptr) {
      destroyRuntimeInfoInList(task_.execMgr.pFreeExecs);
      task_.execMgr.pFreeExecs = static_cast<SList *>(tdListFree(task_.execMgr.pFreeExecs));
    }
    if (task_.execMgr.lockInited) {
      EXPECT_EQ(taosThreadMutexDestroy(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
    }
    taosArrayDestroy(task_.notification.pNotifyAddrUrls);
    stTaskStatsDestroy(&task_.pStats);
    for (SSDataBlock *pBlock : ownedBlocks_) {
      blockDataDestroy(pBlock);
    }
    blockDataDestroy(gRunnerResultBlock);
    gRunnerResultBlock = nullptr;
  }

  bool setUpPublicSnode() {
    taosInitRWLatch(&task_.task.entryLock);
    request_.streamId = task_.task.streamId;
    request_.runnerTaskId = task_.task.taskId;

    stub_.set(rpcSendResponse, mockRpcSendResponse);
    SSnodeOpt option = {};
    snode_ = sndOpen(".", &option);
    return snode_ != nullptr;
  }

  bool setUpCacheTask(bool nested, int32_t cleanMode = DATA_CLEAN_IMMEDIATE) {
    if (gStreamMgmt.taskMap == nullptr || initStreamDataSink() != TSDB_CODE_SUCCESS) return false;
    dataSinkOwned_ = true;

    cacheRealtime_.sessionId = 0x9abc;
    if (stCreateCalcDataCacheIterMap(&cacheRealtime_.pCalcDataCacheIters) != TSDB_CODE_SUCCESS) return false;
    cacheTask_.task.type = STREAM_TRIGGER_TASK;
    cacheTask_.task.streamId = 0x3456;
    cacheTask_.task.taskId = 0x789a;
    cacheTask_.task.nodeId = 7;
    cacheTask_.triggerType = STREAM_TRIGGER_COUNT;
    cacheTask_.addOptions = nested ? STREAM_OPTION_NESTED_WINDOW_PLAN : 0;
    cacheTask_.pRealtimeContext = &cacheRealtime_;
    taosInitRWLatch(&cacheTask_.task.entryLock);
    if (initStreamDataCache(cacheTask_.task.streamId, cacheTask_.task.taskId, cacheRealtime_.sessionId, cleanMode, 0,
                            &cache_) != TSDB_CODE_SUCCESS) {
      return false;
    }
    cacheRealtime_.pCalcDataCache = cache_;
    SStreamTask *pTask = reinterpret_cast<SStreamTask *>(&cacheTask_);
    if (taosHashPut(gStreamMgmt.taskMap, &cacheTask_.task.streamId,
                    sizeof(cacheTask_.task.streamId) + sizeof(cacheTask_.task.taskId), &pTask,
                    POINTER_BYTES) != TSDB_CODE_SUCCESS) {
      return false;
    }
    cacheTaskRegistered_ = true;
    return true;
  }

  bool setUpCacheSnode(bool nested, int32_t cleanMode = DATA_CLEAN_IMMEDIATE) {
    if (!setUpCacheTask(nested, cleanMode)) return false;
    stub_.set(rpcSendResponse, mockRpcSendResponse);
    SSnodeOpt option = {};
    snode_ = sndOpen(".", &option);
    return snode_ != nullptr;
  }

  int32_t processCacheFetch(SResFetchReq *request) {
    const int32_t bodyLen = tSerializeSResFetchReq(nullptr, 0, request, false, false);
    if (bodyLen <= 0) return bodyLen;
    std::vector<uint8_t> body(static_cast<size_t>(bodyLen));
    const int32_t        encoded = tSerializeSResFetchReq(body.data(), bodyLen, request, false, false);
    if (encoded != bodyLen) return encoded;

    SRpcMsg msg = {};
    msg.msgType = TDMT_STREAM_FETCH_FROM_CACHE;
    msg.contLen = bodyLen;
    msg.pCont = body.data();
    return sndProcessStreamMsg(snode_, nullptr, &msg);
  }

  int32_t processPublicCalc() {
    const int32_t bodyLen = tSerializeSTriggerCalcRequest(nullptr, 0, &request_);
    if (bodyLen <= 0) {
      ADD_FAILURE() << "failed to size trigger calc request: " << bodyLen;
      return bodyLen;
    }

    SRpcMsg msg = {};
    msg.msgType = TDMT_STREAM_TRIGGER_CALC;
    msg.contLen = sizeof(SMsgHead) + bodyLen;
    msg.pCont = rpcMallocCont(msg.contLen);
    if (msg.pCont == nullptr) {
      ADD_FAILURE() << "failed to allocate trigger calc request";
      return terrno;
    }

    const int32_t encoded =
        tSerializeSTriggerCalcRequest(POINTER_SHIFT(msg.pCont, sizeof(SMsgHead)), bodyLen, &request_);
    if (encoded != bodyLen) {
      ADD_FAILURE() << "failed to encode trigger calc request: " << encoded;
      rpcFreeCont(msg.pCont);
      return encoded;
    }
    reinterpret_cast<SMsgHead *>(msg.pCont)->contLen = htonl(msg.contLen);

    const int32_t code = sndProcessStreamMsg(snode_, nullptr, &msg);
    rpcFreeCont(msg.pCont);
    return code;
  }

  void SetWindows(int32_t count) {
    request_.params = taosArrayInit(count, sizeof(SSTriggerCalcParam));
    ASSERT_NE(request_.params, nullptr);
    for (int32_t i = 0; i < count; ++i) {
      SSTriggerCalcParam param = {.wstart = i * 1000, .wend = (i + 1) * 1000};
      param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
      ASSERT_NE(taosArrayPush(request_.params, &param), nullptr);
    }
    request_.createTable = 0;
  }

  SSDataBlock *NewBlock(int64_t rows) {
    SSDataBlock *pBlock = nullptr;
    EXPECT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
    if (pBlock != nullptr) {
      pBlock->info.rows = rows;
      ownedBlocks_.push_back(pBlock);
    }
    return pBlock;
  }

  SStreamRunnerPeriodStats RotateRunnerStats() { return RotateRunnerSnapshot().period.runner; }

  SStreamTaskPeriodSnapshot RotateRunnerSnapshot() {
    SStreamTaskPeriodSnapshot snapshot = {};
    bool                      rotated = false;
    EXPECT_EQ(stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US + 1, &snapshot, &rotated),
              TSDB_CODE_SUCCESS);
    EXPECT_TRUE(rotated);
    return snapshot;
  }

  SStreamTaskPeriodSnapshot EmptyRunnerSnapshot() const {
    SStreamTaskPeriodSnapshot snapshot = {};
    snapshot.taskType = STREAM_RUNNER_TASK;
    snapshot.statsStartAtMs = 1000;
    snapshot.uptimeMs = 180000;
    snapshot.statsWindowMs = 180000;
    return snapshot;
  }

  std::string EmitRunnerDebugPeriod(const SStreamTaskPeriodSnapshot &snapshot) {
    ScopedRunnerDebugLogCapture capture;
    EXPECT_EQ(stRunnerTaskLogStats(&task_, &snapshot), TSDB_CODE_SUCCESS);
    EXPECT_EQ(gRunnerDebugLogs.size(), 1);
    return gRunnerDebugLogs.empty() ? std::string{} : gRunnerDebugLogs.front();
  }

  SStreamRunnerTaskExecution *Exec() {
    EXPECT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
    SListNode *pNode = tdListGetHead(task_.execMgr.pFreeExecs);
    return pNode == nullptr ? nullptr : reinterpret_cast<SStreamRunnerTaskExecution *>(pNode->data);
  }

  Stub                       stub_;
  char                       executorSentinel_ = 0;
  char                       sinkSentinel_ = 0;
  char                       notifyUrl_[32] = "ws://unused/zero-window";
  SStreamRunnerTask          task_ = {};
  SSTriggerCalcRequest       request_ = {};
  SSnode                    *snode_ = nullptr;
  std::vector<SSDataBlock *> ownedBlocks_;
  SStreamTriggerTask       cacheTask_ = {};
  SSTriggerRealtimeContext cacheRealtime_ = {};
  void                    *cache_ = nullptr;
  bool                     cacheTaskRegistered_ = false;
  bool                     dataSinkOwned_ = false;
};

TEST_F(StreamRunnerTest, RunnerPeriodUsesSameCountsAsMinuteSnapshot) {
  stTaskStatsRecordRunnerRequest(task_.pStats, 2, 1000001, 2000);
  stTaskStatsRecordRunnerInput(task_.pStats, 400, 20, 1000001);
  stTaskStatsRecordRunnerCalcDuration(task_.pStats, 6003, 1000001, 2000);
  stTaskStatsRecordRunnerWindow(task_.pStats, true, 5000, 1000001, 3000);
  stTaskStatsRecordRunnerOutput(task_.pStats, 100, 10, 1000001, 4000);

  SStreamTaskMetricsSnapshot minute = {};
  ASSERT_EQ(stTaskStatsSnapshot1m(task_.pStats, STREAM_STATS_BUCKET_COUNT * STREAM_STATS_BUCKET_US + 1, &minute),
            TSDB_CODE_SUCCESS);
  ASSERT_TRUE(minute.windowReady);

  const SStreamTaskPeriodSnapshot period = RotateRunnerSnapshot();
  EXPECT_EQ(period.period.runner.outputRows, minute.deliveredOutputRows1m);
  EXPECT_EQ(period.period.runner.resultLatency.totalUs, minute.resultLatencyUs1m);
  EXPECT_EQ(period.period.runner.resultLatency.samples, minute.resultLatencySamples1m);

  const std::string line = EmitRunnerDebugPeriod(period);
  EXPECT_NE(line.find("calc_request_count=1"), std::string::npos);
  EXPECT_NE(line.find("logical_window_count=2"), std::string::npos);
  EXPECT_NE(line.find("input_rows=400"), std::string::npos);
  EXPECT_NE(line.find("input_blocks=20"), std::string::npos);
  EXPECT_NE(line.find("output_rows=100"), std::string::npos);
  EXPECT_NE(line.find("output_blocks=10"), std::string::npos);
  EXPECT_NE(line.find("input_rows_per_sec=2.222"), std::string::npos);
  EXPECT_NE(line.find("input_blocks_per_sec=0.111"), std::string::npos);
  EXPECT_NE(line.find("output_rows_per_sec=0.556"), std::string::npos);
  EXPECT_NE(line.find("output_blocks_per_sec=0.056"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_avg_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_max_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_ms=6.003"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_at=2000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_avg_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_max_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_ms=5.000"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_at=3000"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodPrintsNoResultAndThreeFailureClasses) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.period.runner.noResultWindowCount = 7;
  snapshot.period.runner.calcFailureCount = 2;
  snapshot.period.runner.sinkFailureCount = 3;
  snapshot.period.runner.notifyFailureCount = 5;

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("no_result_window_count=7"), std::string::npos);
  EXPECT_NE(line.find("calc_failure_count=2"), std::string::npos);
  EXPECT_NE(line.find("sink_failure_count=3"), std::string::npos);
  EXPECT_NE(line.find("notify_failure_count=5"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodUsesActualWindowForRates) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.statsWindowMs = 200000;
  snapshot.period.runner.inputRows = 400;
  snapshot.period.runner.inputBlocks = 20;
  snapshot.period.runner.outputRows = 100;
  snapshot.period.runner.outputBlocks = 10;

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("input_rows_per_sec=2.000"), std::string::npos);
  EXPECT_NE(line.find("input_blocks_per_sec=0.100"), std::string::npos);
  EXPECT_NE(line.find("output_rows_per_sec=0.500"), std::string::npos);
  EXPECT_NE(line.find("output_blocks_per_sec=0.050"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodNoSamplesPrintsNa) {
  const std::string line = EmitRunnerDebugPeriod(EmptyRunnerSnapshot());
  EXPECT_NE(line.find("calc_duration_avg_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("calc_duration_lifetime_max_at=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_avg_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_ms=NA"), std::string::npos);
  EXPECT_NE(line.find("result_latency_lifetime_max_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_calc_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_result_at=NA"), std::string::npos);
  EXPECT_NE(line.find("last_output_at=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerExecCountsAreReadUnderLock) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  std::atomic<bool>           entered{false};
  std::atomic<bool>           finished{false};
  int32_t                     logCode = TSDB_CODE_FAILED;

  ASSERT_EQ(taosThreadMutexLock(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
  std::thread logger([&] {
    entered.store(true);
    logCode = stRunnerTaskLogStats(&task_, &snapshot);
    finished.store(true);
  });
  while (!entered.load()) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(finished.load());

  SStreamRunnerTaskExecution secondExec = {};
  EXPECT_EQ(tdListAppend(task_.execMgr.pFreeExecs, &secondExec), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosThreadMutexUnlock(&task_.execMgr.lock), TSDB_CODE_SUCCESS);
  logger.join();

  ASSERT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(finished.load());
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("free_exec_count=2"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("running_exec_count=0"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("parallel_execution_limit=3"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodUninitializedExecManagerDoesNotFabricateCounts) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  snapshot.period.runner.calcRequestCount = 7;
  task_.execMgr.lockInited = false;
  const int32_t logCode = stRunnerTaskLogStats(&task_, &snapshot);
  task_.execMgr.lockInited = true;

  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("calc_request_count=7"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("free_exec_count=NA"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("running_exec_count=NA"), std::string::npos);
  EXPECT_NE(gRunnerDebugLogs.front().find("parallel_execution_limit=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodEntryWriterSkipsPoisonedExecManagerAndPrintsNa) {
  ScopedRunnerDebugLogCapture capture;
  SStreamTaskPeriodSnapshot   snapshot = EmptyRunnerSnapshot();
  SList                      *pFreeExecs = task_.execMgr.pFreeExecs;
  SList                      *pRunningExecs = task_.execMgr.pRunningExecs;

  ASSERT_EQ(taosWTryForceLockLatch(&task_.task.entryLock), TSDB_CODE_SUCCESS);
  task_.execMgr.pFreeExecs = reinterpret_cast<SList *>(static_cast<uintptr_t>(1));
  task_.execMgr.pRunningExecs = reinterpret_cast<SList *>(static_cast<uintptr_t>(1));

  const int32_t logCode = stRunnerTaskLogStats(&task_, &snapshot);
  std::string   line;
  int32_t       periodLogs = 0;
  for (const std::string &log : gRunnerDebugLogs) {
    if (log.find("record=task_period task_type=runner") != std::string::npos) {
      ++periodLogs;
      line = log;
    }
  }

  task_.execMgr.pFreeExecs = pFreeExecs;
  task_.execMgr.pRunningExecs = pRunningExecs;
  taosWUnLockLatch(&task_.task.entryLock);

  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(periodLogs, 1);
  EXPECT_NE(line.find("free_exec_count=NA"), std::string::npos);
  EXPECT_NE(line.find("running_exec_count=NA"), std::string::npos);
  EXPECT_NE(line.find("parallel_execution_limit=NA"), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerLoggerEntryReadPreventsTeardownUntilExecSnapshotCompletes) {
  ScopedRunnerDebugLogCapture capture;
  SStreamRunnerTask           runner = {};
  runner.task.type = STREAM_RUNNER_TASK;
  runner.task.streamId = 0x5678;
  runner.task.taskId = 89;
  runner.task.status = STREAM_STATUS_RUNNING;
  runner.task.undeployCb = captureRunnerUndeploy;
  runner.parallelExecutionNun = 2;
  ASSERT_EQ(taosThreadMutexInit(&runner.execMgr.lock, nullptr), TSDB_CODE_SUCCESS);
  runner.execMgr.lockInited = true;
  runner.execMgr.pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  runner.execMgr.pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  ASSERT_NE(runner.execMgr.pFreeExecs, nullptr);
  ASSERT_NE(runner.execMgr.pRunningExecs, nullptr);
  SStreamTask *pRegisteredTask = &runner.task;
  ASSERT_EQ(taosHashPut(gStreamMgmt.taskMap, &runner.task.streamId,
                        sizeof(runner.task.streamId) + sizeof(runner.task.taskId), &pRegisteredTask, POINTER_BYTES),
            TSDB_CODE_SUCCESS);

  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  int32_t                   logCode = TSDB_CODE_FAILED;
  gBlockRunnerPeriodLog.store(true);
  std::thread logger([&] { logCode = stRunnerTaskLogStats(&runner, &snapshot); });
  const auto  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!gRunnerPeriodLogEntered.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  bool    entered = gRunnerPeriodLogEntered.load();
  int32_t undeployCode = TSDB_CODE_FAILED;
  if (entered) {
    EXPECT_EQ(taosHashRemove(gStreamMgmt.taskMap, &runner.task.streamId,
                             sizeof(runner.task.streamId) + sizeof(runner.task.taskId)),
              TSDB_CODE_SUCCESS);
    SStreamRunnerTask *pRunner = &runner;
    undeployCode = stRunnerTaskUndeploy(&pRunner, false);
    EXPECT_EQ(gRunnerUndeployCalls.load(), 0);
    EXPECT_TRUE(taosHasRWWFlag(&runner.task.entryLock));
  }

  gBlockRunnerPeriodLog.store(false);
  logger.join();

  EXPECT_TRUE(entered);
  EXPECT_EQ(undeployCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(logCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gRunnerUndeployCalls.load(), 1);
  if (!entered) {
    TAOS_UNUSED(taosHashRemove(gStreamMgmt.taskMap, &runner.task.streamId,
                               sizeof(runner.task.streamId) + sizeof(runner.task.taskId)));
    tdListFreeP(runner.execMgr.pRunningExecs, nullptr);
    tdListFreeP(runner.execMgr.pFreeExecs, nullptr);
    TAOS_UNUSED(taosThreadMutexDestroy(&runner.execMgr.lock));
  }
}

TEST_F(StreamRunnerTest, RunnerPeriodDoesNotContainWriteRetryOrQueueDepth) {
  const std::string line = EmitRunnerDebugPeriod(EmptyRunnerSnapshot());
  EXPECT_EQ(line.find("write_retry_count="), std::string::npos);
  EXPECT_EQ(line.find("queue_depth="), std::string::npos);
  EXPECT_EQ(line.find("invalid_result_latency_count="), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerPeriodContainsCommonIdentity) {
  SStreamTaskPeriodSnapshot snapshot = EmptyRunnerSnapshot();
  snapshot.statsOverflow = true;
  const std::string line = EmitRunnerDebugPeriod(snapshot);

  EXPECT_NE(line.find("record=task_period task_type=runner"), std::string::npos);
  EXPECT_NE(line.find("stream_id=4660"), std::string::npos);
  EXPECT_NE(line.find("task_id=23"), std::string::npos);
  EXPECT_NE(line.find("serious_id=45"), std::string::npos);
  EXPECT_NE(line.find("node_id=67"), std::string::npos);
  EXPECT_NE(line.find("task_type=runner"), std::string::npos);
  EXPECT_NE(line.find("status=Running"), std::string::npos);
  EXPECT_NE(line.find("stats_start_at=1000"), std::string::npos);
  EXPECT_NE(line.find("uptime_ms=180000"), std::string::npos);
  EXPECT_NE(line.find("stats_window_ms=180000"), std::string::npos);
  EXPECT_NE(line.find("stats_overflow=true"), std::string::npos);

  const char *requiredFields[] = {
      "calc_request_count=",
      "logical_window_count=",
      "input_rows=",
      "input_blocks=",
      "output_rows=",
      "output_blocks=",
      "no_result_window_count=",
      "calc_failure_count=",
      "sink_failure_count=",
      "notify_failure_count=",
      "input_rows_per_sec=",
      "input_blocks_per_sec=",
      "output_rows_per_sec=",
      "output_blocks_per_sec=",
      "calc_duration_samples=",
      "calc_duration_avg_ms=",
      "calc_duration_max_ms=",
      "calc_duration_lifetime_max_ms=",
      "calc_duration_lifetime_max_at=",
      "result_latency_samples=",
      "result_latency_avg_ms=",
      "result_latency_max_ms=",
      "result_latency_lifetime_max_ms=",
      "result_latency_lifetime_max_at=",
      "free_exec_count=",
      "running_exec_count=",
      "parallel_execution_limit=",
      "last_calc_at=",
      "last_result_at=",
      "last_output_at=",
  };
  for (const char *field : requiredFields) EXPECT_NE(line.find(field), std::string::npos) << field;
}

TEST_F(StreamRunnerTest, RunnerLastEventGaugesUseWallClockAtNaturalBoundaries) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  const int64_t beforeWallMs = taosGetTimestampMs();
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  const int64_t afterWallMs = taosGetTimestampMs();

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GE(snapshot.runnerGauges.lastCalcAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastCalcAtMs, afterWallMs);
  EXPECT_GE(snapshot.runnerGauges.lastResultAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastResultAtMs, afterWallMs);
  EXPECT_GE(snapshot.runnerGauges.lastOutputAtMs, beforeWallMs);
  EXPECT_LE(snapshot.runnerGauges.lastOutputAtMs, afterWallMs);
  EXPECT_NE(snapshot.runnerGauges.lastCalcAtMs, gCalls.monotonicUs);

  const std::string line = EmitRunnerDebugPeriod(snapshot);
  EXPECT_NE(line.find("last_calc_at=" + std::to_string(snapshot.runnerGauges.lastCalcAtMs)), std::string::npos);
  EXPECT_NE(line.find("last_result_at=" + std::to_string(snapshot.runnerGauges.lastResultAtMs)), std::string::npos);
  EXPECT_NE(line.find("last_output_at=" + std::to_string(snapshot.runnerGauges.lastOutputAtMs)), std::string::npos);
}

TEST_F(StreamRunnerTest, RunnerGaugeSetterDoesNotRegressOnOutOfOrderCallbacks) {
  stTaskStatsSetRunnerGauges(task_.pStats, 100, 200, 300);
  stTaskStatsSetRunnerGauges(task_.pStats, 90, 0, 250);
  stTaskStatsSetRunnerGauges(task_.pStats, 110, 0, 0);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastCalcAtMs, 110);
  EXPECT_EQ(snapshot.runnerGauges.lastResultAtMs, 200);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 300);
}

TEST_F(StreamRunnerTest, RunnerNoResultAdvancesResultGaugeButNotOutputGauge) {
  SetWindows(1);
  gCalls.executeReadyTimes = {6000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GT(snapshot.runnerGauges.lastCalcAtMs, 0);
  EXPECT_GT(snapshot.runnerGauges.lastResultAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerSinkFailureDoesNotAdvanceOutputGauge) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.sinkCode = TSDB_CODE_INVALID_PARA;
  task_.lowLatencyCalc = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerCalcFailureBeforeReadyDoesNotAdvanceResultGauge) {
  SetWindows(1);
  gCalls.executeCodes = {TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_GT(snapshot.runnerGauges.lastCalcAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastResultAtMs, 0);
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerNotifyFailureDoesNotAdvanceOutputGauge) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerSinkAndNotifyRecordOutputOnce) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCalls.sinkCalls, 1);
  ASSERT_EQ(gCalls.notifyCalls, 1);

  const SStreamTaskPeriodSnapshot snapshot = RotateRunnerSnapshot();
  EXPECT_EQ(snapshot.period.runner.outputRows, 7);
  EXPECT_EQ(snapshot.period.runner.outputBlocks, 1);
  EXPECT_GT(snapshot.runnerGauges.lastOutputAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerRequestAndCalcGaugeCannotSplitAcrossRotation) {
  SetWindows(1);
  gCalls.executeCodes = {TSDB_CODE_INVALID_PARA};
  gRunnerRotationInterleave = {};
  {
    Stub gaugeStub;
    gaugeStub.set(stTaskStatsSetRunnerGauges, rotateOnRunnerGaugeUpdate);
    EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  }

  if (gRunnerRotationInterleave.calls == 0) {
    gRunnerRotationInterleave.code =
        stTaskStatsRotatePeriod(task_.pStats, STREAM_STATS_PERIOD_US + 1, &gRunnerRotationInterleave.snapshot,
                                &gRunnerRotationInterleave.rotated);
  }
  ASSERT_EQ(gRunnerRotationInterleave.code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(gRunnerRotationInterleave.rotated);
  EXPECT_EQ(gRunnerRotationInterleave.snapshot.period.runner.calcRequestCount, 1);
  EXPECT_GT(gRunnerRotationInterleave.snapshot.runnerGauges.lastCalcAtMs, 0);
}

TEST_F(StreamRunnerTest, RunnerPeriodLoggerFailureDoesNotAffectHeartbeat) {
  gCalls.monotonicUs = STREAM_STATS_PERIOD_US + 1;
  SStreamHbMsg heartbeat = {};
  heartbeat.pStreamStatus = taosArrayInit(1, sizeof(SStmTaskStatusMsg));
  ASSERT_NE(heartbeat.pStreamStatus, nullptr);
  ScopedRunnerDebugLogCapture capture;
  {
    Stub failLogStub;
    failLogStub.set(stRunnerTaskLogStats, failRunnerStatsLog);
    EXPECT_EQ(stmHbAddTaskStatus(task_.task.streamId, &heartbeat, &task_.task, task_.pStats), TSDB_CODE_SUCCESS);
  }

  ASSERT_EQ(taosArrayGetSize(heartbeat.pStreamStatus), 1);
  EXPECT_EQ(taosArrayGetSize(heartbeat.pTaskMetrics), 1);
  EXPECT_EQ(task_.task.status, STREAM_STATUS_RUNNING);
  ASSERT_EQ(gRunnerDebugLogs.size(), 1);
  EXPECT_NE(gRunnerDebugLogs.front().find("failed to rotate or log task statistics"), std::string::npos);
  tCleanupStreamHbMsg(&heartbeat, true);
}

TEST_F(StreamRunnerTest, BrandNewZeroWindowRequestOnReusedExecCreatesTableWithoutExecutingOrNotifying) {
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.resetCalls, 1);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(gCalls.forceOutputCalls, 0);
  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_TRUE(gCalls.sinkBlockIsNull);
  EXPECT_TRUE(gCalls.autoCreateTable);
  EXPECT_EQ(gCalls.groupId, 42);
  EXPECT_STREQ(gCalls.tableName, "out_zero_window");
  EXPECT_EQ(gCalls.notifyCalls, 0);

  ASSERT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  ASSERT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  EXPECT_EQ(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, nullptr);
  ASSERT_NE(request_.params, nullptr);
  EXPECT_EQ(taosArrayGetSize(request_.params), 1);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 0);
  EXPECT_EQ(stats.resultLatency.samples, 0);
}

TEST_F(StreamRunnerTest, ExplicitRequestStartDrivesBatchWindowLatency) {
  SetWindows(2);
  gCalls.executeReadyTimes = {6000, 10000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 2);
  EXPECT_EQ(stats.resultLatency.samples, 2);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000 + 9000);
  EXPECT_EQ(stats.noResultWindowCount, 2);
}

TEST_F(StreamRunnerTest, MultiGroupRequestCountsWindowsFromEveryGroup) {
  request_.isMultiGroupCalc = true;
  request_.createTable = 0;
  request_.pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request_.pGroupCalcInfos, nullptr);
  tSimpleHashSetFreeFp(request_.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);

  const int32_t groupWindowCounts[] = {2, 1};
  for (int64_t gid = 0; gid < 2; ++gid) {
    SSTriggerGroupCalcInfo info = {};
    info.pParams = taosArrayInit(groupWindowCounts[gid], sizeof(SSTriggerCalcParam));
    ASSERT_NE(info.pParams, nullptr);
    for (int32_t i = 0; i < groupWindowCounts[gid]; ++i) {
      SSTriggerCalcParam param = {};
      ASSERT_NE(taosArrayPush(info.pParams, &param), nullptr);
    }
    ASSERT_EQ(tSimpleHashPut(request_.pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)), TSDB_CODE_SUCCESS);
  }

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcRequestCount, 1);
  EXPECT_EQ(stats.logicalWindowCount, 3);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, EmptySuccessfulWindowHasLatencySample) {
  SetWindows(1);
  gCalls.executeReadyTimes = {7000};

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 2000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.noResultWindowCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.outputBlocks, 0);
}

TEST_F(StreamRunnerTest, MultipleBlocksForOneWindowRecordOneReadySample) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(3), NewBlock(4), nullptr};
  gCalls.executeReadyTimes = {4000, 6000, 8000};
  gCalls.executeFinished = {false, false, true};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.executeCalls, 3);
  EXPECT_EQ(gCalls.sinkCalls, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 3000);
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 2);
}

TEST_F(StreamRunnerTest, LaterBatchFailureKeepsEarlierLatencySample) {
  SetWindows(2);
  gCalls.executeCodes = {TSDB_CODE_SUCCESS, TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000, 10000};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.calcFailureCount, 1);
  EXPECT_EQ(stats.calcDuration.samples, 1);
  EXPECT_EQ(stats.calcDuration.totalUs, 9000);
}

TEST_F(StreamRunnerTest, ExecutorBlockingIntervalsAreExcludedFromCalcDuration) {
  SetWindows(1);
  gCalls.executeReadyTimes = {10000};
  gCalls.executeBlockingIntervals = {{2000, 4000}, {5000, 8000}};
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  gCalls.pRuntimeInfo = &pExec->runtimeInfo;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.calcDuration.samples, 1);
  EXPECT_EQ(stats.calcDuration.totalUs, 4000);
}

TEST_F(StreamRunnerTest, SinkFailureDoesNotCountOutputOrLoseReadyLatency) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.sinkCode = TSDB_CODE_INVALID_PARA;
  task_.lowLatencyCalc = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.outputBlocks, 0);
  EXPECT_EQ(stats.sinkFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, NotificationTargetsDoNotMultiplyCalcNotifyOnlyOutput) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.notification.calcNotifyOnly = true;
  char  secondNotifyUrl[] = "ws://unused/second";
  char *pSecondNotifyUrl = secondNotifyUrl;
  ASSERT_NE(taosArrayPush(task_.notification.pNotifyAddrUrls, &pSecondNotifyUrl), nullptr);

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.sinkCalls, 0);
  EXPECT_EQ(gCalls.notifyCalls, 1);
  EXPECT_EQ(gCalls.notifyTargets, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, SuccessfulTopSinkAndNotificationCountLogicalOutputOnce) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_EQ(gCalls.notifyCalls, 1);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, DroppedNotificationFailureCountsFailureButNotOutput) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(7)};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, DroppedNotifyFailureDoesNotHideLaterCalcFailure) {
  SetWindows(2);
  gCalls.executeBlocks = {NewBlock(7), nullptr};
  gCalls.executeCodes = {TSDB_CODE_SUCCESS, TSDB_CODE_INVALID_PARA};
  gCalls.executeReadyTimes = {6000, 10000};
  gCalls.notifyCode = TSDB_CODE_INVALID_PARA;
  task_.notification.calcNotifyOnly = true;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 1);
  EXPECT_EQ(stats.outputRows, 0);
}

TEST_F(StreamRunnerTest, ExternalWindowIndexesRecordDataAndGapWindowsExactlyOnce) {
  SetWindows(3);
  SSDataBlock *pBlock = NewBlock(3);
  gCalls.executeBlocks = {pBlock};
  gCalls.executeReadyTimes = {6000};
  task_.lowLatencyCalc = true;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(2, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  int64_t third = (INT64_C(2) << 32) | 2;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  ASSERT_NE(taosArrayPush(pIndexes, &third), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.logicalWindowCount, 3);
  EXPECT_EQ(stats.resultLatency.samples, 3);
  EXPECT_EQ(stats.noResultWindowCount, 1);
}

TEST_F(StreamRunnerTest, ExternalWindowContinuationBlocksRecordOneReadySample) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(3), NewBlock(4)};
  gCalls.executeReadyTimes = {4000, 6000};
  gCalls.executeFinished = {false, false};
  task_.lowLatencyCalc = true;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  gCalls.pRuntimeInfo = &pExec->runtimeInfo;
  gCalls.executeOutIndexes = {0, 1};
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  EXPECT_EQ(gCalls.executeCalls, 2);
  EXPECT_EQ(gCalls.sinkCalls, 2);
  EXPECT_EQ(gCalls.notifyCalls, 2);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 3000);
  EXPECT_EQ(stats.outputRows, 7);
  EXPECT_EQ(stats.outputBlocks, 2);
  EXPECT_EQ(stats.notifyFailureCount, 0);
}

TEST_F(StreamRunnerTest, ExternalReadySampleSurvivesNotificationPreparationFailure) {
  SetWindows(1);
  SSDataBlock *pBlock = NewBlock(1);
  gCalls.executeBlocks = {pBlock};
  gCalls.executeReadyTimes = {6000};
  gCalls.notifyBuildCode = TSDB_CODE_INVALID_PARA;
  task_.addOptions = NOTIFY_ON_FAILURE_PAUSE;
  SStreamRunnerTaskExecution *pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.withExternalWindow = true;
  SArray *pIndexes = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIndexes, nullptr);
  int64_t first = 0;
  ASSERT_NE(taosArrayPush(pIndexes, &first), nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = pIndexes;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  pExec = Exec();
  ASSERT_NE(pExec, nullptr);
  pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx = nullptr;
  taosArrayDestroy(pIndexes);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.resultLatency.samples, 1);
  EXPECT_EQ(stats.resultLatency.totalUs, 5000);
  EXPECT_EQ(stats.notifyFailureCount, 1);
  EXPECT_EQ(stats.calcFailureCount, 0);
}

TEST_F(StreamRunnerTest, NonTopRunnerCountsReturnedBlock) {
  SetWindows(1);
  gCalls.executeBlocks = {NewBlock(5)};
  gCalls.executeReadyTimes = {6000};
  task_.topTask = false;

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  ASSERT_NE(request_.pOutBlock, nullptr);
  EXPECT_EQ(static_cast<SSDataBlock *>(request_.pOutBlock)->info.rows, 5);
  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.outputRows, 5);
  EXPECT_EQ(stats.outputBlocks, 1);
}

TEST_F(StreamRunnerTest, RuntimeInputCallbackRecordsRemoteAndFilteredLocalRows) {
  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);

  ASSERT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(pExec->runtimeInfo.inputStatsFp, nullptr);
  pExec->runtimeInfo.inputStatsFp(pExec->runtimeInfo.pInputStatsParam, 11, 1);
  pExec->runtimeInfo.inputStatsFp(pExec->runtimeInfo.pInputStatsParam, 3, 1);

  const SStreamRunnerPeriodStats stats = RotateRunnerStats();
  EXPECT_EQ(stats.inputRows, 14);
  EXPECT_EQ(stats.inputBlocks, 2);
}

TEST_F(StreamRunnerTest, DropNotificationReportsRawTargetFailureWithoutChangingBusinessCode) {
  stub_.reset(streamSendNotifyContentWithResult);
  stub_.set(tcurlConnect, mockTcurlConnectFailure);
  SSTriggerCalcParam param = {};
  param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
  bool attempted = false;
  bool delivered = true;

  EXPECT_EQ(streamSendNotifyContentWithResult(&task_.task, "db.stream", "out", STREAM_TRIGGER_SESSION, 42,
                                              task_.notification.pNotifyAddrUrls, 0, &param, 1, &attempted, &delivered),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(attempted);
  EXPECT_FALSE(delivered);
}

TEST_F(StreamRunnerTest, DropNotificationReportsRawSendFailureWithoutChangingBusinessCode) {
  stub_.reset(streamSendNotifyContentWithResult);
  stub_.set(tcurlConnect, mockTcurlConnectSuccess);
  stub_.set(tcurlSend, mockTcurlSendFailure);
  SSTriggerCalcParam param = {};
  param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
  bool attempted = false;
  bool delivered = true;

  EXPECT_EQ(streamSendNotifyContentWithResult(&task_.task, "db.stream", "out", STREAM_TRIGGER_SESSION, 42,
                                              task_.notification.pNotifyAddrUrls, 0, &param, 1, &attempted, &delivered),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(attempted);
  EXPECT_FALSE(delivered);
}

TEST_F(StreamRunnerTest, AncestorContextRejectsMissingNestedMapping) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  EXPECT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
}

TEST_F(StreamRunnerTest, AncestorContextRejectsMissingMultiGroupMapping) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.isMultiGroupCalc = true;
  request_.pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  ASSERT_NE(request_.pGroupCalcInfos, nullptr);
  tSimpleHashSetFreeFp(request_.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  addRunnerGroupCalcInfo(&request_, 101);
  addRunnerGroupCalcInfo(&request_, 202);
  request_.pContextPolicy = makeRunnerContextPolicy({
      {.gid = 101, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
      {.gid = 202, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
  });
  request_.pAncestorContext = makeRunnerAncestorContext(101, 0);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  EXPECT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
}

TEST_F(StreamRunnerTest, AncestorContextRejectsMalformedLineageBeforeExecution) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);
  auto *mapping =
      static_cast<SStreamAncestorParamContext *>(taosArrayGet(request_.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(mapping, nullptr);
  auto *scope = static_cast<SScopeInstanceId *>(taosArrayGet(mapping->leafIdentity.lineage.pScopes, 0));
  ASSERT_NE(scope, nullptr);
  scope->layerIndex = 1;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(listNEles(task_.execMgr.pFreeExecs), 1);
  EXPECT_EQ(listNEles(task_.execMgr.pRunningExecs), 0);
}

TEST_F(StreamRunnerTest, AncestorContextAcceptsMixedBatchWindowMapping) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam resume = {.idlestart = 100, .idleend = 200, .notifyType = STRIGGER_EVENT_WINDOW_NONE};
  const SSTriggerCalcParam window = {.wstart = 201, .wend = 299, .notifyType = STRIGGER_EVENT_WINDOW_NONE};
  ASSERT_NE(taosArrayPush(request_.params, &resume), nullptr);
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  request_.pContextPolicy = makeRunnerContextPolicy({
      {.gid = request_.gid, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_NONE},
      {.gid = request_.gid, .paramIndex = 1, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
  });
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 1);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 1);
}

TEST_F(StreamRunnerTest, AncestorContextRejectsMixedBatchGroupMapping) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam idle = {.idlestart = 100, .idleend = 200, .notifyType = STRIGGER_EVENT_IDLE};
  const SSTriggerCalcParam window = {.wstart = 201, .wend = 299, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &idle), nullptr);
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  request_.pContextPolicy = makeRunnerContextPolicy({
      {.gid = request_.gid, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_NONE},
      {.gid = request_.gid, .paramIndex = 1, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR},
  });
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, AncestorContextClearsPreviousIdleOnlyBatch) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 1);
  auto *firstExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(firstExec, nullptr);

  taosArrayDestroyEx(request_.params, tDestroySSTriggerCalcParam);
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam idle = {.idlestart = 200, .idleend = 300, .notifyType = STRIGGER_EVENT_WINDOW_NONE};
  ASSERT_NE(taosArrayPush(request_.params, &idle), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_NONE);
  request_.brandNew = false;
  request_.execId = firstExec->runtimeInfo.execId;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 2);
  auto *secondExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_EQ(firstExec, secondExec);
  ASSERT_EQ(taosArrayGetSize(secondExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals), 1);
  const auto *stored =
      static_cast<const SSTriggerCalcParam *>(taosArrayGet(secondExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, 0));
  ASSERT_NE(stored, nullptr);
  EXPECT_EQ(stored->notifyType, STRIGGER_EVENT_WINDOW_NONE);
  EXPECT_EQ(stored->idlestart, 200);
  EXPECT_EQ(stored->idleend, 300);
}

TEST_F(StreamRunnerTest, AncestorContextRejectsContextForNonNestedRunner) {
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, RuntimeContextTransfersAncestorContextOnce) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);
  SStreamContextPolicy   *policy = request_.pContextPolicy;
  SStreamAncestorContext *owned = request_.pAncestorContext;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(request_.pContextPolicy, nullptr);
  EXPECT_EQ(request_.pAncestorContext, nullptr);
  auto *exec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(exec, nullptr);
  EXPECT_EQ(exec->runtimeInfo.funcInfo.pContextPolicy, policy);
  EXPECT_EQ(exec->runtimeInfo.funcInfo.pAncestorContext, owned);
}

TEST_F(StreamRunnerTest, RuntimeContextClearsBeforeIdleOnlyBatch) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);
  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);

  auto *exec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(exec, nullptr);
  ASSERT_NE(exec->runtimeInfo.funcInfo.pContextPolicy, nullptr);
  ASSERT_NE(exec->runtimeInfo.funcInfo.pAncestorContext, nullptr);
  taosArrayDestroyEx(request_.params, tDestroySSTriggerCalcParam);
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam idle = {.idlestart = 200, .idleend = 300, .notifyType = STRIGGER_EVENT_WINDOW_NONE};
  ASSERT_NE(taosArrayPush(request_.params, &idle), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_NONE);
  request_.brandNew = false;
  request_.execId = exec->runtimeInfo.execId;

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_INVALID_PARA);
  exec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(exec, nullptr);
  ASSERT_NE(exec->runtimeInfo.funcInfo.pContextPolicy, nullptr);
  EXPECT_EQ(exec->runtimeInfo.funcInfo.pAncestorContext, nullptr);
}

TEST_F(StreamRunnerTest, NestedBrandNewCreateTableCreatesWithoutCalcOrNotify) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.pContextPolicy = makeRunnerContextPolicy();

  ASSERT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCalls.resetCalls, 1);
  EXPECT_EQ(gCalls.executeCalls, 0);
  EXPECT_EQ(gCalls.forceOutputCalls, 0);
  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_EQ(gCalls.notifyCalls, 0);
}

TEST_F(StreamRunnerTest, SnodePublicEntryAdmitsEmptyNestedCreateTablePolicy) {
  ASSERT_TRUE(setUpPublicSnode());
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.triggerType = STREAM_TRIGGER_COUNT;
  request_.isWindowTrigger = true;
  request_.pContextPolicy = makeRunnerContextPolicy();

  EXPECT_EQ(processPublicCalc(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gResponseCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCalls.sinkCalls, 1);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, SnodePublicEntryRejectsMissingNestedCreateTablePolicy) {
  ASSERT_TRUE(setUpPublicSnode());
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.triggerType = STREAM_TRIGGER_COUNT;
  request_.isWindowTrigger = true;

  EXPECT_EQ(processPublicCalc(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gResponseCode, TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.sinkCalls, 0);
  EXPECT_EQ(gCalls.executeCalls, 0);
}

TEST_F(StreamRunnerTest, SnodePublicEntryTransfersAndRollsBackNestedContextPair) {
  ASSERT_TRUE(setUpPublicSnode());
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  request_.createTable = 0;
  request_.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(request_.params, nullptr);
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  ASSERT_NE(taosArrayPush(request_.params, &window), nullptr);
  setRunnerPolicy(&request_, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  request_.pAncestorContext = makeRunnerAncestorContext(request_.gid, 0);

  EXPECT_EQ(processPublicCalc(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gResponseCode, TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 1);

  auto *pExec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(pExec, nullptr);
  SStreamContextPolicy   *pStoredPolicy = pExec->runtimeInfo.funcInfo.pContextPolicy;
  SStreamAncestorContext *pStoredContext = pExec->runtimeInfo.funcInfo.pAncestorContext;
  ASSERT_NE(pStoredPolicy, nullptr);
  ASSERT_NE(pStoredContext, nullptr);
  EXPECT_EQ(tAdmitStreamContext(pStoredPolicy, pStoredContext, true), TSDB_CODE_SUCCESS);

  task_.addOptions = 0;
  gResponseCode = TSDB_CODE_SUCCESS;
  EXPECT_EQ(processPublicCalc(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gResponseCode, TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(gCalls.executeCalls, 1);
  EXPECT_EQ(pExec->runtimeInfo.funcInfo.pContextPolicy, pStoredPolicy);
  EXPECT_EQ(pExec->runtimeInfo.funcInfo.pAncestorContext, pStoredContext);
}

SStreamRuntimeFuncInfo makeCacheFetchRuntime(int64_t gid, int32_t vgId, int64_t discriminator, bool multiGroup) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = multiGroup;
  runtime.curNodeId = vgId;
  runtime.groupId = gid;
  runtime.curIdx = 0;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.pContextPolicy =
      makeRunnerContextPolicy({{.gid = gid, .paramIndex = 0, .contextPolicy = STREAM_CONTEXT_POLICY_ANCESTOR}});
  runtime.pAncestorContext = makeRunnerAncestorContext(gid, 0);
  auto *param = static_cast<SStreamAncestorParamContext *>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0));
  auto *scope = static_cast<SScopeInstanceId *>(taosArrayGet(param->leafIdentity.lineage.pScopes, 0));
  scope->nativeDiscriminator = discriminator;
  if (multiGroup) {
    runtime.pAncestorContext->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
    EXPECT_NE(nullptr, runtime.pAncestorContext->pReadScopeBindings);
    SStreamReadScopeBinding binding = {.vgId = vgId, .readInfoIndex = 3, .scope = {.gid = gid}};
    binding.scope.lineage.pScopes = taosArrayDup(param->leafIdentity.lineage.pScopes, nullptr);
    EXPECT_NE(nullptr, binding.scope.lineage.pScopes);
    EXPECT_NE(nullptr, taosArrayPush(runtime.pAncestorContext->pReadScopeBindings, &binding));
  }
  return runtime;
}

void destroyCacheFetchRuntime(SStreamRuntimeFuncInfo *runtime) {
  tDestroyStreamContextPolicy(&runtime->pContextPolicy);
  tDestroyStreamAncestorContext(&runtime->pAncestorContext);
}

struct OwnedFetchRequest {
  ~OwnedFetchRequest() { tDestroySResFetchReq(&value); }

  SResFetchReq value = {};
};

SSDataBlock *makeCacheBlock(std::initializer_list<TSKEY> timestamps, std::initializer_list<int32_t> values) {
  if (timestamps.size() != values.size()) {
    ADD_FAILURE() << "timestamp and value counts differ";
    return nullptr;
  }
  SSDataBlock *block = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
  if (block == nullptr) return nullptr;
  SColumnInfoData tsInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData valueInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block, &tsInfo));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(block, &valueInfo));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(block, timestamps.size()));
  auto *tsCol = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 0));
  auto *valueCol = static_cast<SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
  auto  ts = timestamps.begin();
  auto  value = values.begin();
  for (int32_t row = 0; ts != timestamps.end(); ++row, ++ts, ++value) {
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(tsCol, row, reinterpret_cast<const char *>(&*ts), false));
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(valueCol, row, reinterpret_cast<const char *>(&*value), false));
  }
  block->info.rows = timestamps.size();
  return block;
}

SSDataBlock *makeCacheBlock(TSKEY ts, int32_t value) { return makeCacheBlock({ts}, {value}); }

int32_t putCacheRowsAsSeparateSourceBlocks(void *pCache, const SStreamCacheScope *pScope, SSDataBlock *pBlock) {
  if (pCache == nullptr || pScope == nullptr || pBlock == nullptr || pBlock->info.rows != 2) {
    return TSDB_CODE_INVALID_PARA;
  }
  const auto *pTsCol = static_cast<const SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  if (pTsCol == nullptr) return TSDB_CODE_INVALID_PARA;
  for (int32_t row = 0; row < pBlock->info.rows; ++row) {
    const TSKEY ts = *reinterpret_cast<const TSKEY *>(colDataGetData(pTsCol, row));
    pBlock->info.id.blockId = row + 1;
    const int32_t code = putStreamDataCacheScoped(pCache, pScope, ts, ts, pBlock, row, row);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t putCacheRowsAsMergedSourceBlock(void *pCache, const SStreamCacheScope *pScope, SSDataBlock *pBlock) {
  if (pCache == nullptr || pScope == nullptr || pBlock == nullptr || pBlock->info.rows != 2) {
    return TSDB_CODE_INVALID_PARA;
  }
  return putStreamDataCacheScoped(pCache, pScope, 100, 101, pBlock, 0, 1);
}

bool prepareRunnerOutputRequest(SSTriggerCalcRequest *request) {
  request->createTable = 0;
  request->params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  if (request->params == nullptr) return false;
  const SSTriggerCalcParam window = {.wstart = 100, .wend = 199, .notifyType = STRIGGER_EVENT_WINDOW_CLOSE};
  if (taosArrayPush(request->params, &window) == nullptr) return false;
  gRunnerResultBlock = makeCacheBlock(100, 7);
  return gRunnerResultBlock != nullptr;
}

TEST_F(StreamRunnerTest, NormalOutputRetriesMissingTableWithSameComputedBlock) {
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  ASSERT_TRUE(prepareRunnerOutputRequest(&request_));
  gRunnerSinkCodes = {TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND, TSDB_CODE_SUCCESS};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCalls.sinkCalls, 2);
  ASSERT_EQ(gCalls.autoCreateTableCalls.size(), 2);
  EXPECT_FALSE(gCalls.autoCreateTableCalls[0]);
  EXPECT_TRUE(gCalls.autoCreateTableCalls[1]);
  ASSERT_EQ(gCalls.sinkBlocks.size(), 2);
  EXPECT_NE(gCalls.sinkBlocks[0], nullptr);
  EXPECT_EQ(gCalls.sinkBlocks[0], gCalls.sinkBlocks[1]);
  EXPECT_EQ(gCalls.notifyCalls, 2);
}

TEST_F(StreamRunnerTest, NormalOutputReturnsRetryErrorAfterMissingTable) {
  constexpr int32_t kRetryError = TSDB_CODE_OUT_OF_MEMORY;
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  ASSERT_TRUE(prepareRunnerOutputRequest(&request_));
  gRunnerSinkCodes = {TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND, kRetryError};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), kRetryError);
  EXPECT_EQ(gCalls.sinkCalls, 2);
}

TEST_F(StreamRunnerTest, NormalOutputSuccessDoesNotRetry) {
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  ASSERT_TRUE(prepareRunnerOutputRequest(&request_));

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_SUCCESS);
  ASSERT_EQ(gCalls.sinkCalls, 1);
  ASSERT_EQ(gCalls.autoCreateTableCalls.size(), 1);
  EXPECT_FALSE(gCalls.autoCreateTableCalls[0]);
}

TEST_F(StreamRunnerTest, ChildOutputMissingTableDoesNotRetry) {
  task_.output.outTblType = TSDB_CHILD_TABLE;
  taosArrayClear(task_.notification.pNotifyAddrUrls);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  ASSERT_TRUE(prepareRunnerOutputRequest(&request_));
  gRunnerSinkCodes = {TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND, TSDB_CODE_SUCCESS};

  EXPECT_EQ(stRunnerTaskExecute(&task_, &request_, 1000), TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND);
  EXPECT_EQ(gCalls.sinkCalls, 1);
}

int32_t cacheBlockValue(const SSDataBlock *block, int32_t row = 0) {
  const auto *valueCol = static_cast<const SColumnInfoData *>(taosArrayGet(block->pDataBlock, 1));
  return *reinterpret_cast<const int32_t *>(colDataGetData(valueCol, row));
}

TEST_F(StreamRunnerTest, TriggerAndResultNoticeUseSameNestedTriggerId) {
  constexpr TSKEY       kFirstAncestorStart = 100;
  constexpr TSKEY       kSecondAncestorStart = 200;
  constexpr const char *kFirstExpectedId = "5d4db351f40826b67261959dd081f8a2";
  constexpr const char *kSecondExpectedId = "86e1e9b91a9c40215a968723e302bf7e";
  ASSERT_STRNE(kFirstExpectedId, kSecondExpectedId);

  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  task_.streamName = const_cast<char *>("1.runner_nested_notice");
  stub_.reset(streamSendNotifyContent);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  stub_.set(streamForceOutput, mockRunnerForceOutput);
  stub_.set(tcurlConnect, captureRunnerNotifyConnect);
  stub_.set(tcurlSend, captureRunnerNotifySend);
  stub_.set(tcurlClose, captureRunnerNotifyClose);
  gRunnerNotifyPayloads.clear();
  gRunnerResultBlock = nullptr;

  auto *exec = reinterpret_cast<SStreamRunnerTaskExecution *>(tdListGetHead(task_.execMgr.pFreeExecs)->data);
  ASSERT_NE(exec, nullptr);

  exec->runtimeInfo.funcInfo.withExternalWindow = false;
  ASSERT_TRUE(prepareRunnerNoticeParams(exec));
  OwnedTriggerCalcRequest oneForOne;
  ASSERT_TRUE(
      makeNestedRunnerNoticeRequest(&oneForOne.value, kFirstAncestorStart, RunnerAncestorMappingOrder::TargetFirst));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequestAncestorContext(&oneForOne.value, true));
  EXPECT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &oneForOne.value, 1000));
  ASSERT_EQ(gRunnerNotifyPayloads.size(), 1);
  EXPECT_EQ(runnerNotifyTriggerId(gRunnerNotifyPayloads[0]), kFirstExpectedId);

  exec->runtimeInfo.funcInfo.withExternalWindow = true;
  ASSERT_TRUE(prepareRunnerNoticeParams(exec));
  OwnedTriggerCalcRequest externalBatch;
  ASSERT_TRUE(makeNestedRunnerNoticeRequest(&externalBatch.value, kSecondAncestorStart,
                                            RunnerAncestorMappingOrder::TargetMiddle));
  EXPECT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &externalBatch.value, 1000));
  ASSERT_EQ(gRunnerNotifyPayloads.size(), 2);
  EXPECT_EQ(runnerNotifyTriggerId(gRunnerNotifyPayloads[1]), kSecondExpectedId);

  exec->runtimeInfo.funcInfo.withExternalWindow = false;
  ASSERT_TRUE(prepareRunnerNoticeParams(exec));
  exec->pOutBlock = makeCacheBlock(100, 7);
  ASSERT_NE(exec->pOutBlock, nullptr);
  OwnedTriggerCalcRequest currentWins;
  ASSERT_TRUE(
      makeNestedRunnerNoticeRequest(&currentWins.value, kFirstAncestorStart, RunnerAncestorMappingOrder::TargetLast));
  currentWins.value.curWinIdx = 3;
  auto *currentParam =
      static_cast<SSTriggerCalcParam *>(taosArrayGet(exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, 2));
  ASSERT_NE(currentParam, nullptr);
  currentParam->resultNotifyContent =
      taosStrdup("{\"result\":{\"data\":[],\"curSize\":0,\"curOffset\":0,\"finish\":true}}");
  ASSERT_NE(currentParam->resultNotifyContent, nullptr);
  EXPECT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &currentWins.value, 1000));
  blockDataDestroy(exec->pOutBlock);
  exec->pOutBlock = nullptr;
  ASSERT_EQ(gRunnerNotifyPayloads.size(), 3);
  EXPECT_EQ(runnerNotifyTriggerId(gRunnerNotifyPayloads[2]), kFirstExpectedId);

  OwnedTriggerCalcRequest missing;
  ASSERT_TRUE(
      makeNestedRunnerNoticeRequest(&missing.value, kFirstAncestorStart, RunnerAncestorMappingOrder::TargetLast));
  tDestroyStreamAncestorContext(&missing.value.pAncestorContext);
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerTaskExecute(&task_, &missing.value, 1000));

  OwnedTriggerCalcRequest duplicate;
  ASSERT_TRUE(
      makeNestedRunnerNoticeRequest(&duplicate.value, kFirstAncestorStart, RunnerAncestorMappingOrder::TargetLast));
  ASSERT_TRUE(appendRunnerAncestorMapping(duplicate.value.pAncestorContext, 42, 2, kFirstAncestorStart, 1000));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerTaskExecute(&task_, &duplicate.value, 1000));

  EXPECT_EQ(gRunnerNotifyPayloads.size(), 3);
}

TEST_F(StreamRunnerTest, NestedGroupNoticesDoNotRequireLeafIdentity) {
  constexpr const char *kGroupTriggerId = "4136590351924762991";
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  task_.streamName = const_cast<char *>("1.runner_nested_group_notice");
  stub_.reset(streamSendNotifyContent);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  stub_.set(tcurlConnect, captureRunnerNotifyConnect);
  stub_.set(tcurlSend, captureRunnerNotifySend);
  stub_.set(tcurlClose, captureRunnerNotifyClose);
  gRunnerNotifyPayloads.clear();
  gRunnerResultBlock = nullptr;

  auto *exec = Exec();
  ASSERT_NE(exec, nullptr);
  exec->runtimeInfo.funcInfo.withExternalWindow = false;

  const int32_t notifyTypes[] = {STRIGGER_EVENT_IDLE, STRIGGER_EVENT_RESUME};
  for (int32_t notifyType : notifyTypes) {
    OwnedTriggerCalcRequest request;
    ASSERT_TRUE(makeNestedRunnerGroupNoticeRequest(&request.value, notifyType));
    EXPECT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &request.value, 1000));
  }

  ASSERT_EQ(2U, gRunnerNotifyPayloads.size());
  EXPECT_EQ(kGroupTriggerId, runnerNotifyTriggerId(gRunnerNotifyPayloads[0]));
  EXPECT_EQ(kGroupTriggerId, runnerNotifyTriggerId(gRunnerNotifyPayloads[1]));
}

TEST_F(StreamRunnerTest, NestedExternalAndCurrentBatchesKeepEachDerivedLeafId) {
  constexpr const char          *kGroupTriggerId = "4136590351924762991";
  constexpr const char          *kFirstExpectedId = "5d4db351f40826b67261959dd081f8a2";
  constexpr const char          *kSecondExpectedId = "86e1e9b91a9c40215a968723e302bf7e";
  const std::vector<std::string> expected = {kGroupTriggerId, kFirstExpectedId, kSecondExpectedId};

  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  task_.streamName = const_cast<char *>("1.runner_nested_batch_notice");
  stub_.reset(streamSendNotifyContent);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  stub_.set(streamForceOutput, mockRunnerForceOutput);
  stub_.set(tcurlConnect, captureRunnerNotifyConnect);
  stub_.set(tcurlSend, captureRunnerNotifySend);
  stub_.set(tcurlClose, captureRunnerNotifyClose);
  gRunnerNotifyPayloads.clear();
  gRunnerResultBlock = nullptr;

  auto *exec = Exec();
  ASSERT_NE(exec, nullptr);
  exec->runtimeInfo.funcInfo.withExternalWindow = true;
  ASSERT_TRUE(prepareRunnerDualLeafNoticeParams(exec));
  OwnedTriggerCalcRequest externalBatch;
  ASSERT_TRUE(makeNestedRunnerDualLeafNoticeRequest(&externalBatch.value));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &externalBatch.value, 1000));
  ASSERT_EQ(1U, gRunnerNotifyPayloads.size());
  EXPECT_EQ(expected, runnerNotifyTriggerIds(gRunnerNotifyPayloads[0]));

  exec->runtimeInfo.funcInfo.withExternalWindow = false;
  ASSERT_TRUE(prepareRunnerDualLeafNoticeParams(exec));
  exec->pOutBlock = makeCacheBlock(100, 7);
  ASSERT_NE(exec->pOutBlock, nullptr);
  for (int32_t index : {0, 2, 3}) {
    auto *param =
        static_cast<SSTriggerCalcParam *>(taosArrayGet(exec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, index));
    ASSERT_NE(param, nullptr);
    param->resultNotifyContent = taosStrdup("{\"result\":{\"data\":[],\"curSize\":0,\"curOffset\":0,\"finish\":true}}");
    ASSERT_NE(param->resultNotifyContent, nullptr);
  }
  OwnedTriggerCalcRequest currentWins;
  ASSERT_TRUE(makeNestedRunnerDualLeafNoticeRequest(&currentWins.value));
  currentWins.value.curWinIdx = 4;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &currentWins.value, 1000));
  blockDataDestroy(exec->pOutBlock);
  exec->pOutBlock = nullptr;
  ASSERT_EQ(2U, gRunnerNotifyPayloads.size());
  EXPECT_EQ(expected, runnerNotifyTriggerIds(gRunnerNotifyPayloads[1]));
}

TEST_F(StreamRunnerTest, NestedResultIdRequiresOnePostAdmissionExactMapping) {
  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  task_.streamName = const_cast<char *>("1.runner_nested_mapping_notice");
  stub_.reset(streamSendNotifyContent);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  stub_.set(tcurlConnect, captureRunnerNotifyConnect);
  stub_.set(tcurlSend, captureRunnerNotifySend);
  stub_.set(tcurlClose, captureRunnerNotifyClose);
  gRunnerNotifyPayloads.clear();
  gRunnerResultBlock = nullptr;

  auto *exec = Exec();
  ASSERT_NE(exec, nullptr);
  exec->runtimeInfo.funcInfo.withExternalWindow = false;
  const RunnerPostAdmissionMappingMutation mutations[] = {
      RunnerPostAdmissionMappingMutation::RemoveTarget,
      RunnerPostAdmissionMappingMutation::DuplicateTarget,
  };
  for (RunnerPostAdmissionMappingMutation mutation : mutations) {
    ASSERT_TRUE(prepareRunnerNoticeParams(exec));
    OwnedTriggerCalcRequest request;
    ASSERT_TRUE(makeNestedRunnerNoticeRequest(&request.value, 100, RunnerAncestorMappingOrder::TargetFirst));
    ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequestAncestorContext(&request.value, true));
    gRunnerPostAdmissionMappingMutation = mutation;
    gRunnerPostAdmissionContext = request.value.pAncestorContext;
    gRunnerPostAdmissionMutationApplied = false;
    gRunnerPostAdmissionMutationCode = TSDB_CODE_SUCCESS;
    const int32_t executeCallsBefore = gCalls.executeCalls;

    EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerTaskExecute(&task_, &request.value, 1000));
    EXPECT_GT(gCalls.executeCalls, executeCallsBefore);
    EXPECT_TRUE(gRunnerPostAdmissionMutationApplied);
    EXPECT_EQ(TSDB_CODE_SUCCESS, gRunnerPostAdmissionMutationCode);
  }
  EXPECT_TRUE(gRunnerNotifyPayloads.empty());
}

TEST_F(StreamRunnerTest, NestedEventResultIdRequiresConsistentRuntimeLeafAndMetadata) {
  constexpr int32_t     kWindowIndex = INT32_C(0x01020304);
  constexpr const char *kParentTriggerId = "21d98cbc2c95e4238b634e5464885aef";
  constexpr const char *kExpectedId = "37e23355fcfd36c8a4040556311b110d";
  const std::string     validMetadata =
      std::string("{\"windowIndex\":16909060,\"parentTriggerId\":\"") + kParentTriggerId + "\"}";

  task_.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  task_.streamName = const_cast<char *>("1.runner_nested_event_notice");
  stub_.reset(streamSendNotifyContent);
  stub_.set(streamExecuteTask, mockRunnerExecuteTask);
  stub_.set(tcurlConnect, captureRunnerNotifyConnect);
  stub_.set(tcurlSend, captureRunnerNotifySend);
  stub_.set(tcurlClose, captureRunnerNotifyClose);
  gRunnerNotifyPayloads.clear();
  gRunnerResultBlock = nullptr;

  auto *exec = Exec();
  ASSERT_NE(exec, nullptr);
  exec->runtimeInfo.funcInfo.withExternalWindow = false;
  {
    OwnedTriggerCalcRequest valid;
    ASSERT_TRUE(configureRunnerEventNoticeRequest(&valid.value, exec, STREAM_TRIGGER_EVENT, WINDOW_TYPE_EVENT,
                                                  kWindowIndex, validMetadata.c_str()));
    ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerTaskExecute(&task_, &valid.value, 1000));
    ASSERT_EQ(1U, gRunnerNotifyPayloads.size());
    EXPECT_EQ(kExpectedId, runnerNotifyTriggerId(gRunnerNotifyPayloads[0]));
  }

  struct InvalidIdentityCase {
    const char *name;
    int32_t     runtimeTriggerType;
    int8_t      leafTriggerType;
    int64_t     nativeDiscriminator;
    const char *extraNotifyContent;
  };
  const InvalidIdentityCase invalidCases[] = {
      {"runtime-non-event", STREAM_TRIGGER_COUNT, WINDOW_TYPE_EVENT, kWindowIndex, validMetadata.c_str()},
      {"leaf-non-event", STREAM_TRIGGER_EVENT, WINDOW_TYPE_COUNT, kWindowIndex, validMetadata.c_str()},
      {"index-mismatch", STREAM_TRIGGER_EVENT, WINDOW_TYPE_EVENT, kWindowIndex,
       "{\"windowIndex\":67305985,\"parentTriggerId\":\"21d98cbc2c95e4238b634e5464885aef\"}"},
      {"below-missing-index", STREAM_TRIGGER_EVENT, WINDOW_TYPE_EVENT, -2, "{\"windowIndex\":-1}"},
      {"non-event-metadata-index", STREAM_TRIGGER_COUNT, WINDOW_TYPE_COUNT, 7, "{\"windowIndex\":7}"},
  };
  for (const InvalidIdentityCase &testCase : invalidCases) {
    SCOPED_TRACE(testCase.name);
    OwnedTriggerCalcRequest invalid;
    ASSERT_TRUE(configureRunnerEventNoticeRequest(&invalid.value, exec, testCase.runtimeTriggerType,
                                                  testCase.leafTriggerType, testCase.nativeDiscriminator,
                                                  testCase.extraNotifyContent));
    EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerTaskExecute(&task_, &invalid.value, 1000));
  }
  EXPECT_EQ(1U, gRunnerNotifyPayloads.size());
}

TEST_F(StreamRunnerTest, NestedCacheFetchUsesProjectedReadScope) {
  ASSERT_TRUE(setUpCacheTask(true));
  SStreamRuntimeFuncInfo runtime = makeCacheFetchRuntime(42, 7, 11, true);
  SResFetchReq           source = {};
  source.pStRtFuncInfo = &runtime;
  const int32_t wireSize = tSerializeSResFetchReq(nullptr, 0, &source, false, false);
  ASSERT_GT(wireSize, 0);
  std::vector<uint8_t> wire(static_cast<size_t>(wireSize));
  ASSERT_EQ(wireSize, tSerializeSResFetchReq(wire.data(), wireSize, &source, false, false));
  OwnedFetchRequest decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire.data(), wireSize, &decoded.value));
  ASSERT_NE(nullptr, decoded.value.pStRtFuncInfo);
  SStreamRuntimeFuncInfo *decodedRuntime = decoded.value.pStRtFuncInfo;
  EXPECT_EQ(0, decodedRuntime->addOptions);
  EXPECT_EQ(0, decodedRuntime->curNodeId);
  EXPECT_EQ(runtime.groupId, decodedRuntime->groupId);

  SStreamCacheReadInfo   readInfo = {};
  readInfo.taskInfo.streamId = cacheTask_.task.streamId;
  readInfo.taskInfo.taskId = cacheTask_.task.taskId;
  readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
  readInfo.gid = decodedRuntime->groupId;
  readInfo.start = 100;
  readInfo.end = 100;
  readInfo.pRuntime = decodedRuntime;

  SSDataBlock *legacy = makeCacheBlock(100, 10);
  SSDataBlock *scoped = makeCacheBlock(100, 20);
  ASSERT_NE(nullptr, legacy);
  ASSERT_NE(nullptr, scoped);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCache(cache_, decodedRuntime->groupId, 100, 100, legacy, 0, 0));
  const auto *binding = static_cast<const SStreamReadScopeBinding *>(
      taosArrayGet(decodedRuntime->pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, binding);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(cache_, &binding->scope, 100, 100, scoped, 0, 0));

  bool finished = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerFetchDataFromCache(&readInfo, &finished));
  ASSERT_NE(nullptr, readInfo.pBlock);
  EXPECT_EQ(20, cacheBlockValue(readInfo.pBlock));
  blockDataDestroy(readInfo.pBlock);
  blockDataDestroy(legacy);
  blockDataDestroy(scoped);
  stClearStreamCacheReadScope(&readInfo);
  destroyCacheFetchRuntime(&runtime);
}

TEST_F(StreamRunnerTest, CacheFetchResetRestartsCanonicalIterator) {
  ASSERT_TRUE(setUpCacheSnode(true));
  SStreamRuntimeFuncInfo targetRuntime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamRuntimeFuncInfo sideRuntime = makeCacheFetchRuntime(42, 7, 12, true);
  for (auto *runtime : {&targetRuntime, &sideRuntime}) {
    runtime->sessionId = cacheRealtime_.sessionId;
    runtime->curWindow.skey = 100;
    runtime->curWindow.ekey = 101;
  }
  const auto *targetBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(targetRuntime.pAncestorContext->pReadScopeBindings, 0));
  const auto *sideBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(sideRuntime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, targetBinding);
  ASSERT_NE(nullptr, sideBinding);
  SSDataBlock *targetSource = makeCacheBlock({100, 101}, {10, 11});
  SSDataBlock *sideSource = makeCacheBlock({100, 101}, {20, 21});
  ASSERT_NE(nullptr, targetSource);
  ASSERT_NE(nullptr, sideSource);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &targetBinding->scope, targetSource));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &sideBinding->scope, sideSource));

  SResFetchReq targetRequest = {};
  targetRequest.queryId = cacheTask_.task.streamId;
  targetRequest.taskId = cacheTask_.task.taskId;
  targetRequest.pStRtFuncInfo = &targetRuntime;
  targetRequest.reset = true;
  SResFetchReq sideRequest = targetRequest;
  sideRequest.pStRtFuncInfo = &sideRuntime;

  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10}), gCacheResponseValues);
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{20}), gCacheResponseValues);
  EXPECT_EQ(2, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10}), gCacheResponseValues);
  EXPECT_EQ(2, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  sideRequest.reset = false;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{21}), gCacheResponseValues);
  EXPECT_EQ(1, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  targetRequest.reset = false;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{11}), gCacheResponseValues);
  EXPECT_EQ(0, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  blockDataDestroy(targetSource);
  blockDataDestroy(sideSource);
  destroyCacheFetchRuntime(&targetRuntime);
  destroyCacheFetchRuntime(&sideRuntime);
}

TEST_F(StreamRunnerTest, CacheFetchResponseAllocationFailureRestartsCanonicalIterator) {
  ASSERT_TRUE(setUpCacheSnode(true));
  SStreamRuntimeFuncInfo targetRuntime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamRuntimeFuncInfo sideRuntime = makeCacheFetchRuntime(42, 7, 12, true);
  for (auto *runtime : {&targetRuntime, &sideRuntime}) {
    runtime->sessionId = cacheRealtime_.sessionId;
    runtime->curWindow.skey = 100;
    runtime->curWindow.ekey = 101;
  }
  const auto *targetBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(targetRuntime.pAncestorContext->pReadScopeBindings, 0));
  const auto *sideBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(sideRuntime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, targetBinding);
  ASSERT_NE(nullptr, sideBinding);
  SSDataBlock *targetSource = makeCacheBlock({100, 101}, {10, 11});
  SSDataBlock *sideSource = makeCacheBlock({100, 101}, {20, 21});
  ASSERT_NE(nullptr, targetSource);
  ASSERT_NE(nullptr, sideSource);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &targetBinding->scope, targetSource));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &sideBinding->scope, sideSource));

  SResFetchReq targetRequest = {};
  targetRequest.queryId = cacheTask_.task.streamId;
  targetRequest.taskId = cacheTask_.task.taskId;
  targetRequest.pStRtFuncInfo = &targetRuntime;
  targetRequest.reset = true;
  SResFetchReq sideRequest = targetRequest;
  sideRequest.pStRtFuncInfo = &sideRuntime;

  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{20}), gCacheResponseValues);
  EXPECT_EQ(1, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));
  gCacheResponseValues.clear();
  {
    Stub responseAllocationFailure;
    responseAllocationFailure.set(rpcMallocCont, failRpcMallocCont);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, processCacheFetch(&targetRequest));
    EXPECT_EQ(TDMT_STREAM_FETCH_FROM_CACHE_RSP, gCacheResponse.msgType);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, gCacheResponse.code);
    EXPECT_EQ(0, gCacheResponse.contLen);
    EXPECT_TRUE(gCacheResponse.contentIsNull);
  }
  EXPECT_EQ(1, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  sideRequest.reset = false;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{21}), gCacheResponseValues);
  targetRequest.reset = false;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10}), gCacheResponseValues);

  blockDataDestroy(targetSource);
  blockDataDestroy(sideSource);
  destroyCacheFetchRuntime(&targetRuntime);
  destroyCacheFetchRuntime(&sideRuntime);
}

TEST_F(StreamRunnerTest, MergedCacheBlockPaginationAndResetStayScopeIsolated) {
  ASSERT_TRUE(setUpCacheSnode(true));
  SStreamRuntimeFuncInfo targetRuntime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamRuntimeFuncInfo sideRuntime = makeCacheFetchRuntime(42, 7, 12, true);
  for (auto *runtime : {&targetRuntime, &sideRuntime}) {
    runtime->sessionId = cacheRealtime_.sessionId;
    runtime->curWindow.skey = 100;
    runtime->curWindow.ekey = 102;
  }
  const auto *targetBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(targetRuntime.pAncestorContext->pReadScopeBindings, 0));
  const auto *sideBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(sideRuntime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, targetBinding);
  ASSERT_NE(nullptr, sideBinding);
  SSDataBlock *targetMerged = makeCacheBlock({100, 101}, {10, 11});
  SSDataBlock *targetTail = makeCacheBlock(102, 12);
  SSDataBlock *sideMerged = makeCacheBlock({100, 101}, {20, 21});
  ASSERT_NE(nullptr, targetMerged);
  ASSERT_NE(nullptr, targetTail);
  ASSERT_NE(nullptr, sideMerged);
  targetTail->info.id.blockId = 2;
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsMergedSourceBlock(cache_, &targetBinding->scope, targetMerged));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(cache_, &targetBinding->scope, 102, 102, targetTail, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsMergedSourceBlock(cache_, &sideBinding->scope, sideMerged));

  SResFetchReq targetRequest = {};
  targetRequest.queryId = cacheTask_.task.streamId;
  targetRequest.taskId = cacheTask_.task.taskId;
  targetRequest.pStRtFuncInfo = &targetRuntime;
  targetRequest.reset = true;
  SResFetchReq sideRequest = targetRequest;
  sideRequest.pStRtFuncInfo = &sideRuntime;

  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10, 11}), gCacheResponseValues);
  targetRequest.reset = false;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{12}), gCacheResponseValues);

  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{20, 21}), gCacheResponseValues);

  targetRequest.reset = true;
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10, 11}), gCacheResponseValues);

  blockDataDestroy(targetMerged);
  blockDataDestroy(targetTail);
  blockDataDestroy(sideMerged);
  destroyCacheFetchRuntime(&targetRuntime);
  destroyCacheFetchRuntime(&sideRuntime);
}

TEST_F(StreamRunnerTest, MergedCacheBlockAllocationFailurePreservesSiblingAndTarget) {
  ASSERT_TRUE(setUpCacheSnode(true));
  SStreamRuntimeFuncInfo targetRuntime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamRuntimeFuncInfo sideRuntime = makeCacheFetchRuntime(42, 7, 12, true);
  for (auto *runtime : {&targetRuntime, &sideRuntime}) {
    runtime->sessionId = cacheRealtime_.sessionId;
    runtime->curWindow.skey = 100;
    runtime->curWindow.ekey = 101;
  }
  const auto *targetBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(targetRuntime.pAncestorContext->pReadScopeBindings, 0));
  const auto *sideBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(sideRuntime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, targetBinding);
  ASSERT_NE(nullptr, sideBinding);
  SSDataBlock *targetSource = makeCacheBlock({100, 101}, {10, 11});
  SSDataBlock *sideSource = makeCacheBlock({100, 101}, {20, 21});
  ASSERT_NE(nullptr, targetSource);
  ASSERT_NE(nullptr, sideSource);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsMergedSourceBlock(cache_, &targetBinding->scope, targetSource));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsMergedSourceBlock(cache_, &sideBinding->scope, sideSource));

  SResFetchReq targetRequest = {};
  targetRequest.queryId = cacheTask_.task.streamId;
  targetRequest.taskId = cacheTask_.task.taskId;
  targetRequest.pStRtFuncInfo = &targetRuntime;
  targetRequest.reset = true;
  SResFetchReq sideRequest = targetRequest;
  sideRequest.pStRtFuncInfo = &sideRuntime;

  {
    Stub responseAllocationFailure;
    responseAllocationFailure.set(rpcMallocCont, failRpcMallocCont);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, processCacheFetch(&targetRequest));
    EXPECT_EQ(TDMT_STREAM_FETCH_FROM_CACHE_RSP, gCacheResponse.msgType);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, gCacheResponse.code);
    EXPECT_EQ(0, gCacheResponse.contLen);
    EXPECT_TRUE(gCacheResponse.contentIsNull);
  }

  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&sideRequest));
  EXPECT_EQ((std::vector<int32_t>{20, 21}), gCacheResponseValues);
  gCacheResponseValues.clear();
  ASSERT_EQ(TSDB_CODE_SUCCESS, processCacheFetch(&targetRequest));
  EXPECT_EQ((std::vector<int32_t>{10, 11}), gCacheResponseValues);

  blockDataDestroy(targetSource);
  blockDataDestroy(sideSource);
  destroyCacheFetchRuntime(&targetRuntime);
  destroyCacheFetchRuntime(&sideRuntime);
}

TEST_F(StreamRunnerTest, NestedCacheFetchRejectsSplitAdmissionAndBindingContexts) {
  ASSERT_TRUE(setUpCacheTask(true));
  SStreamRuntimeFuncInfo runtime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamContextPolicy  *admittedPolicy = runtime.pContextPolicy;
  SStreamCacheReadInfo   readInfo = {};
  readInfo.taskInfo.streamId = cacheTask_.task.streamId;
  readInfo.taskInfo.taskId = cacheTask_.task.taskId;
  readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
  readInfo.gid = runtime.groupId;
  readInfo.start = 100;
  readInfo.end = 100;
  readInfo.pContextPolicy = admittedPolicy;
  readInfo.pAncestorContext = runtime.pAncestorContext;
  readInfo.pRuntime = &runtime;
  runtime.pContextPolicy = nullptr;

  bool finished = false;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_EQ(nullptr, readInfo.pBlock);
  EXPECT_EQ(0, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  runtime.pContextPolicy = admittedPolicy;
  stClearStreamCacheReadScope(&readInfo);
  destroyCacheFetchRuntime(&runtime);
}

TEST_F(StreamRunnerTest, NestedCacheFetchRejectsReadInfoGidMismatch) {
  ASSERT_TRUE(setUpCacheTask(true));
  SStreamRuntimeFuncInfo runtime = makeCacheFetchRuntime(42, 7, 11, true);
  SStreamCacheReadInfo   readInfo = {};
  readInfo.taskInfo.streamId = cacheTask_.task.streamId;
  readInfo.taskInfo.taskId = cacheTask_.task.taskId;
  readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
  readInfo.gid = 43;
  readInfo.start = 100;
  readInfo.end = 100;
  readInfo.pContextPolicy = runtime.pContextPolicy;
  readInfo.pAncestorContext = runtime.pAncestorContext;
  readInfo.pRuntime = &runtime;

  bool finished = false;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stRunnerFetchDataFromCache(&readInfo, &finished));
  EXPECT_EQ(nullptr, readInfo.pBlock);
  EXPECT_EQ(0, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));

  stClearStreamCacheReadScope(&readInfo);
  destroyCacheFetchRuntime(&runtime);
}

TEST_F(StreamRunnerTest, SingleGroupCacheFetchBindsParamIdentityScope) {
  SStreamRuntimeFuncInfo runtime = makeCacheFetchRuntime(42, 7, 17, false);
  SStreamCacheReadInfo   readInfo = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, stBindStreamCacheReadScope(&runtime, &readInfo));
  ASSERT_TRUE(readInfo.hasCacheScope);
  const auto *copied = static_cast<const SScopeInstanceId *>(taosArrayGet(readInfo.cacheScope.lineage.pScopes, 0));
  ASSERT_NE(nullptr, copied);
  EXPECT_EQ(17, copied->nativeDiscriminator);
  auto *source = static_cast<SScopeInstanceId *>(
      taosArrayGet(static_cast<SStreamAncestorParamContext *>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0))
                       ->leafIdentity.lineage.pScopes,
                   0));
  source->nativeDiscriminator = 99;
  EXPECT_EQ(17, copied->nativeDiscriminator);
  stClearStreamCacheReadScope(&readInfo);
  destroyCacheFetchRuntime(&runtime);
}

TEST_F(StreamRunnerTest, MultiGroupCacheFetchBindsReadInfoScope) {
  SStreamRuntimeFuncInfo runtime = makeCacheFetchRuntime(42, 7, 23, true);
  SStreamCacheReadInfo   readInfo = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, stBindStreamCacheReadScope(&runtime, &readInfo));
  ASSERT_TRUE(readInfo.hasCacheScope);
  const auto *copied = static_cast<const SScopeInstanceId *>(taosArrayGet(readInfo.cacheScope.lineage.pScopes, 0));
  ASSERT_NE(nullptr, copied);
  EXPECT_EQ(23, copied->nativeDiscriminator);
  EXPECT_EQ(3, readInfo.readInfoIndex);
  stClearStreamCacheReadScope(&readInfo);
  destroyCacheFetchRuntime(&runtime);
}

TEST_F(StreamRunnerTest, EqualRangeDifferentLineageReadsDifferentRows) {
  ASSERT_TRUE(setUpCacheTask(true));
  SStreamRuntimeFuncInfo first = makeCacheFetchRuntime(1, 7, 31, true);
  SStreamRuntimeFuncInfo second = makeCacheFetchRuntime(1, 7, 32, true);
  const auto            *firstBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(first.pAncestorContext->pReadScopeBindings, 0));
  const auto *secondBinding =
      static_cast<const SStreamReadScopeBinding *>(taosArrayGet(second.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, firstBinding);
  ASSERT_NE(nullptr, secondBinding);

  SSDataBlock *firstBlock = makeCacheBlock({100, 101}, {10, 11});
  SSDataBlock *secondBlock = makeCacheBlock({100, 101}, {20, 21});
  ASSERT_NE(nullptr, firstBlock);
  ASSERT_NE(nullptr, secondBlock);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &firstBinding->scope, firstBlock));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putCacheRowsAsSeparateSourceBlocks(cache_, &secondBinding->scope, secondBlock));

  auto makeReadInfo = [&](SStreamRuntimeFuncInfo *runtime) {
    SStreamCacheReadInfo readInfo = {};
    readInfo.taskInfo.streamId = cacheTask_.task.streamId;
    readInfo.taskInfo.taskId = cacheTask_.task.taskId;
    readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
    readInfo.gid = runtime->groupId;
    readInfo.start = 100;
    readInfo.end = 101;
    readInfo.pContextPolicy = runtime->pContextPolicy;
    readInfo.pAncestorContext = runtime->pAncestorContext;
    readInfo.pRuntime = runtime;
    return readInfo;
  };
  SStreamCacheReadInfo firstRead = makeReadInfo(&first);
  SStreamCacheReadInfo secondRead = makeReadInfo(&second);
  bool                 firstFinished = false;
  bool                 secondFinished = false;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerFetchDataFromCache(&firstRead, &firstFinished));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerFetchDataFromCache(&secondRead, &secondFinished));
  ASSERT_NE(nullptr, firstRead.pBlock);
  ASSERT_NE(nullptr, secondRead.pBlock);
  EXPECT_EQ(10, cacheBlockValue(firstRead.pBlock));
  EXPECT_EQ(20, cacheBlockValue(secondRead.pBlock));
  EXPECT_FALSE(firstFinished);
  EXPECT_FALSE(secondFinished);
  EXPECT_EQ(2, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));
  EXPECT_LT(taosHashGetMaxOverflowLinkLength(cacheRealtime_.pCalcDataCacheIters), 2);

  blockDataDestroy(firstRead.pBlock);
  blockDataDestroy(secondRead.pBlock);
  blockDataDestroy(firstBlock);
  blockDataDestroy(secondBlock);
  stClearStreamCacheReadScope(&firstRead);
  stClearStreamCacheReadScope(&secondRead);
  destroyCacheFetchRuntime(&first);
  destroyCacheFetchRuntime(&second);
}

TEST_F(StreamRunnerTest, LegacyCacheFetchUsesEmptyLineage) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 42;
  SStreamCacheReadInfo readInfo = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, stBindStreamCacheReadScope(&runtime, &readInfo));
  EXPECT_TRUE(readInfo.hasCacheScope);
  EXPECT_EQ(42, readInfo.cacheScope.gid);
  EXPECT_EQ(0, taosArrayGetSize(readInfo.cacheScope.lineage.pScopes));
  stClearStreamCacheReadScope(&readInfo);
}

TEST_F(StreamRunnerTest, LegacyCacheFetchDoesNotRequireScopedLease) {
  ASSERT_TRUE(setUpCacheTask(false));
  constexpr int64_t gid = 42;
  SSDataBlock      *source = makeCacheBlock(100, 10);
  ASSERT_NE(nullptr, source);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCache(cache_, gid, 100, 100, source, 0, 0));

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = cacheTask_.task.streamId;
  readInfo.taskInfo.taskId = cacheTask_.task.taskId;
  readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
  readInfo.gid = gid;
  readInfo.start = 100;
  readInfo.end = 100;
  bool finished = false;
  {
    Stub leaseFailure;
    leaseFailure.set(acquireStreamDataCacheLease, rejectScopedCacheLease);
    ASSERT_EQ(TSDB_CODE_SUCCESS, stRunnerFetchDataFromCache(&readInfo, &finished));
  }
  ASSERT_NE(nullptr, readInfo.pBlock);
  EXPECT_EQ(10, cacheBlockValue(readInfo.pBlock));

  blockDataDestroy(readInfo.pBlock);
  blockDataDestroy(source);
}

TEST_F(StreamRunnerTest, LegacyCacheReadErrorRestoresGroupIdle) {
  ASSERT_TRUE(setUpCacheTask(false, DATA_CLEAN_EXPIRED));
  constexpr int64_t gid = 42;
  SSDataBlock      *source = makeCacheBlock({100, 101}, {10, 20});
  ASSERT_NE(nullptr, source);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCache(cache_, gid, 100, 101, source, 0, 1));

  auto  *manager = static_cast<SSlidingTaskDSMgr *>(cache_);
  auto **group = static_cast<SSlidingGrpMgr **>(taosHashGet(manager->pSlidingGrpList, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, group);

  SStreamCacheReadInfo readInfo = {};
  readInfo.taskInfo.streamId = cacheTask_.task.streamId;
  readInfo.taskInfo.taskId = cacheTask_.task.taskId;
  readInfo.taskInfo.sessionId = cacheRealtime_.sessionId;
  readInfo.gid = gid;
  readInfo.start = 100;
  readInfo.end = 101;
  bool finished = false;
  {
    Stub readFailure;
    readFailure.set(checkAndMoveMemCache, failCacheMaintenance);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stRunnerFetchDataFromCache(&readInfo, &finished));
  }
  EXPECT_EQ(nullptr, readInfo.pBlock);
  EXPECT_EQ(0, taosHashGetSize(cacheRealtime_.pCalcDataCacheIters));
  EXPECT_EQ(GRP_DATA_IDLE, (*group)->status);

  blockDataDestroy(source);
}

}  // namespace

int main(int argc, char **argv) {
  taos_init();
  testing::InitGoogleTest(&argc, argv);
  int32_t code = RUN_ALL_TESTS();
  taos_cleanup();
  return code;
}
