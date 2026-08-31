#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cJSON.h"
#include "streamInt.h"
#include "streamTriggerTask.h"
#include "streamWindowChain.h"
#include "stub.h"
#include "tdatablock.h"
#include "ttime.h"

namespace {

constexpr int64_t kGid = 42;

SStreamWindowLayerSpec intervalLayer(int64_t interval, int64_t sliding, int64_t offset = 0,
                                     int8_t intervalUnit = TIME_UNIT_MILLISECOND,
                                     int8_t slidingUnit = TIME_UNIT_MILLISECOND,
                                     int8_t offsetUnit = TIME_UNIT_MILLISECOND) {
  SStreamWindowLayerSpec layer = {};
  layer.triggerType = WINDOW_TYPE_INTERVAL;
  layer.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  layer.input.tsSlotId = 0;
  layer.input.pkSlotId = -1;
  layer.trigger.sliding.intervalUnit = intervalUnit;
  layer.trigger.sliding.slidingUnit = slidingUnit;
  layer.trigger.sliding.offsetUnit = offsetUnit;
  layer.trigger.sliding.soffsetUnit = TIME_UNIT_MILLISECOND;
  layer.trigger.sliding.precision = TSDB_TIME_PRECISION_MILLI;
  layer.trigger.sliding.interval = interval;
  layer.trigger.sliding.sliding = sliding;
  layer.trigger.sliding.offset = offset;
  return layer;
}

SStreamWindowLayerSpec slidingLayer(int64_t sliding, int64_t soffset = 0, int8_t slidingUnit = TIME_UNIT_MILLISECOND,
                                    int8_t soffsetUnit = TIME_UNIT_MILLISECOND) {
  SStreamWindowLayerSpec layer = intervalLayer(0, sliding);
  layer.placeholderMask = PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS;
  layer.trigger.sliding.slidingUnit = slidingUnit;
  layer.trigger.sliding.soffsetUnit = soffsetUnit;
  layer.trigger.sliding.soffset = soffset;
  return layer;
}

SStreamWindowLayerSpec sessionLayer(int64_t gap) {
  SStreamWindowLayerSpec layer = {};
  layer.triggerType = WINDOW_TYPE_SESSION;
  layer.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  layer.input.tsSlotId = 0;
  layer.input.pkSlotId = -1;
  layer.trigger.session.sessionVal = gap;
  return layer;
}

SArray* slotIds(std::initializer_list<int16_t> slots) {
  SArray* result = taosArrayInit(slots.size(), sizeof(int16_t));
  EXPECT_NE(nullptr, result);
  for (int16_t slot : slots) EXPECT_NE(nullptr, taosArrayPush(result, &slot));
  return result;
}

char* encodeNode(SNode* node) {
  char* encoded = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(node, false, &encoded, nullptr));
  nodesDestroyNode(node);
  return encoded;
}

char* eventCondition(bool multiple) {
  SNode* first = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &first));
  if (!multiple || first == nullptr) return encodeNode(first);

  SNode* list = nullptr;
  SNode* second = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_NODE_LIST, &list));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &second));
  if (list == nullptr) {
    nodesDestroyNode(first);
    nodesDestroyNode(second);
    return nullptr;
  }
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&((SNodeListNode*)list)->pNodeList, first));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&((SNodeListNode*)list)->pNodeList, second));
  return encodeNode(list);
}

char* zerothState(int32_t value) {
  SNode* node = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt32(value, &node));
  SNodeList* list = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&list, node));
  char*   encoded = nullptr;
  int32_t length = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListToString(list, false, &encoded, &length));
  nodesDestroyList(list);
  return encoded;
}

char* zerothStates(std::initializer_list<int32_t> values) {
  SNodeList* list = nullptr;
  for (int32_t value : values) {
    SNode* node = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt32(value, &node));
    EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&list, node));
  }
  char*   encoded = nullptr;
  int32_t length = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListToString(list, false, &encoded, &length));
  nodesDestroyList(list);
  return encoded;
}

SStreamWindowLayerSpec stateLayer(int16_t extend = STATE_WIN_EXTEND_OPTION_DEFAULT, int32_t trueForCount = 0,
                                  bool withZeroth = false) {
  SStreamWindowLayerSpec layer = {};
  layer.triggerType = WINDOW_TYPE_STATE;
  layer.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  layer.input.tsSlotId = 0;
  layer.input.pkSlotId = -1;
  layer.input.eventStartSlotId = -1;
  layer.input.eventEndSlotId = -1;
  layer.input.pConditionSlotIds = slotIds({1});
  layer.trigger.stateWin.extend = extend;
  layer.trigger.stateWin.trueForType = trueForCount > 0 ? TRUE_FOR_COUNT_ONLY : 0;
  layer.trigger.stateWin.trueForCount = trueForCount;
  if (withZeroth) layer.trigger.stateWin.zeroth = zerothState(0);
  return layer;
}

SStreamWindowLayerSpec twoColumnStateLayer(int16_t extend, int32_t trueForCount = 0) {
  SStreamWindowLayerSpec layer = stateLayer(extend, trueForCount);
  taosArrayDestroy(layer.input.pConditionSlotIds);
  layer.input.pConditionSlotIds = slotIds({1, 4});
  return layer;
}

SStreamWindowLayerSpec twoColumnStateLayerWithZeroth(int16_t extend, std::initializer_list<int32_t> zeroths) {
  SStreamWindowLayerSpec layer = twoColumnStateLayer(extend);
  layer.trigger.stateWin.zeroth = zerothStates(zeroths);
  return layer;
}

SStreamWindowLayerSpec countLayer(int64_t count, int64_t sliding, std::initializer_list<int16_t> conditionSlots = {1}) {
  SStreamWindowLayerSpec layer = {};
  layer.triggerType = WINDOW_TYPE_COUNT;
  layer.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  layer.input.tsSlotId = 0;
  layer.input.pkSlotId = -1;
  layer.input.eventStartSlotId = -1;
  layer.input.eventEndSlotId = -1;
  layer.input.pConditionSlotIds = slotIds(conditionSlots);
  layer.trigger.count.countVal = count;
  layer.trigger.count.sliding = sliding;
  return layer;
}

SStreamWindowLayerSpec eventLayer(bool multiple = false, int32_t trueForCount = 0) {
  SStreamWindowLayerSpec layer = {};
  layer.triggerType = WINDOW_TYPE_EVENT;
  layer.placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  layer.input.tsSlotId = 0;
  layer.input.pkSlotId = -1;
  layer.input.eventStartSlotId = 2;
  layer.input.eventEndSlotId = 3;
  layer.input.pConditionSlotIds = slotIds({});
  layer.trigger.event.startCond = eventCondition(multiple);
  layer.trigger.event.endCond = eventCondition(false);
  layer.trigger.event.trueForType = trueForCount > 0 ? TRUE_FOR_COUNT_ONLY : 0;
  layer.trigger.event.trueForCount = trueForCount;
  return layer;
}

SStreamWindowLayerSpec eventLayerWithStreaks(int32_t startType, int32_t startCount, int64_t startDuration,
                                             int32_t endType, int32_t endCount, int64_t endDuration) {
  SStreamWindowLayerSpec layer = eventLayer(false);
  layer.trigger.event.startTrueForType = startType;
  layer.trigger.event.startTrueForCount = startCount;
  layer.trigger.event.startTrueForDuration = startDuration;
  layer.trigger.event.endTrueForType = endType;
  layer.trigger.event.endTrueForCount = endCount;
  layer.trigger.event.endTrueForDuration = endDuration;
  return layer;
}

void destroyOwnedLayer(void* value) {
  auto* layer = static_cast<SStreamWindowLayerSpec*>(value);
  taosArrayDestroy(layer->input.pConditionSlotIds);
  layer->input.pConditionSlotIds = nullptr;
  if (layer->triggerType == WINDOW_TYPE_STATE) {
    taosMemoryFreeClear(layer->trigger.stateWin.zeroth);
  } else if (layer->triggerType == WINDOW_TYPE_EVENT) {
    taosMemoryFreeClear(layer->trigger.event.startCond);
    taosMemoryFreeClear(layer->trigger.event.endCond);
  }
}

class Plan {
 public:
  explicit Plan(const std::vector<SStreamWindowLayerSpec>& layers) {
    plan_.version = STREAM_WINDOW_PLAN_VERSION;
    plan_.pLayers = taosArrayInit(layers.size(), sizeof(SStreamWindowLayerSpec));
    EXPECT_NE(nullptr, plan_.pLayers);
    if (plan_.pLayers != nullptr) {
      EXPECT_NE(nullptr, taosArrayAddBatch(plan_.pLayers, layers.data(), layers.size()));
      for (int32_t i = 0; i + 1 < taosArrayGetSize(plan_.pLayers); ++i) {
        auto* layer = static_cast<SStreamWindowLayerSpec*>(taosArrayGet(plan_.pLayers, i));
        snprintf(layer->name, sizeof(layer->name), "w%d", i);
      }
    }
  }

  ~Plan() { taosArrayDestroyEx(plan_.pLayers, destroyOwnedLayer); }

  Plan(const Plan&) = delete;
  Plan& operator=(const Plan&) = delete;

  const SStreamWindowPlan* get() const { return &plan_; }

 private:
  SStreamWindowPlan plan_ = {};
};

struct ChainDeleter {
  void operator()(SWindowChainState* state) const { stWindowChainDestroy(&state); }
};

using Chain = std::unique_ptr<SWindowChainState, ChainDeleter>;

Chain createChain(const Plan& plan, int64_t eventTypes = STRIGGER_EVENT_WINDOW_CLOSE, int64_t maxDelayNs = 0,
                  bool flushOnOuterClose = false, int64_t notifyEventTypes = INT64_MIN,
                  const SNodeList* eventStartCondCols = nullptr, const SNodeList* eventEndCondCols = nullptr) {
  SWindowChainPolicy policy = {};
  policy.flushOnOuterClose = flushOnOuterClose;
  policy.leafEventTypes = eventTypes;
  policy.leafNotifyEventTypes = notifyEventTypes == INT64_MIN ? eventTypes : notifyEventTypes;
  policy.maxDelayNs = maxDelayNs;
  policy.pEventStartCondCols = eventStartCondCols;
  policy.pEventEndCondCols = eventEndCondCols;
  SWindowChainState* state = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainCreate(plan.get(), kGid, &policy, &state));
  return Chain(state);
}

class ConditionColumns {
 public:
  ConditionColumns(std::initializer_list<std::pair<int16_t, const char*>> columns) {
    for (const auto& column : columns) {
      SNode* node = nullptr;
      EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &node));
      if (node == nullptr) return;
      auto* definition = reinterpret_cast<SColumnNode*>(node);
      definition->slotId = column.first;
      definition->colType = COLUMN_TYPE_COLUMN;
      definition->node.resType.type = TSDB_DATA_TYPE_INT;
      definition->node.resType.bytes = sizeof(int32_t);
      tstrncpy(definition->colName, column.second, sizeof(definition->colName));
      const int32_t code = nodesListMakeStrictAppend(&columns_, node);
      EXPECT_EQ(TSDB_CODE_SUCCESS, code);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyNode(node);
        return;
      }
    }
  }

  ~ConditionColumns() { nodesDestroyList(columns_); }

  ConditionColumns(const ConditionColumns&) = delete;
  ConditionColumns& operator=(const ConditionColumns&) = delete;

  const SNodeList* get() const { return columns_; }

 private:
  SNodeList* columns_ = nullptr;
};

struct BlockDeleter {
  void operator()(SSDataBlock* block) const {
    if (block != nullptr) blockDataDestroy(block);
  }
};

using Block = std::unique_ptr<SSDataBlock, BlockDeleter>;

Block makeBlock(TSKEY ts, const std::vector<int32_t>& values) {
  SSDataBlock* raw = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&raw));
  Block block(raw);
  if (raw == nullptr) return block;

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData valueCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &tsCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &valueCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(raw, values.size()));
  for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
    colDataSetInt64(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 0)), i, &ts);
    int32_t value = values[i];
    colDataSetInt32(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 1)), i, &value);
  }
  raw->info.rows = values.size();
  return block;
}

struct DataRow {
  DataRow(TSKEY tsValue, int32_t stateValue, bool stateIsNull, uint8_t startValue, uint8_t endValue,
          int32_t state2Value = 0, bool state2IsNull = false)
      : ts(tsValue),
        state(stateValue),
        stateNull(stateIsNull),
        eventStart(startValue),
        eventEnd(endValue),
        state2(state2Value),
        state2Null(state2IsNull) {}

  TSKEY   ts;
  int32_t state;
  bool    stateNull;
  uint8_t eventStart;
  uint8_t eventEnd;
  int32_t state2;
  bool    state2Null;
};

Block makeDataBlock(const std::vector<DataRow>& rows) {
  SSDataBlock* raw = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&raw));
  Block block(raw);
  if (raw == nullptr) return block;

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData stateCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  SColumnInfoData startCol = createColumnInfoData(TSDB_DATA_TYPE_UTINYINT, sizeof(uint8_t), 3);
  SColumnInfoData endCol = createColumnInfoData(TSDB_DATA_TYPE_UTINYINT, sizeof(uint8_t), 4);
  SColumnInfoData state2Col = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 5);
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &tsCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &stateCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &startCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &endCol));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(raw, &state2Col));
  EXPECT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(raw, rows.size()));
  for (int32_t i = 0; i < static_cast<int32_t>(rows.size()); ++i) {
    const DataRow& row = rows[i];
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 0)), i,
                                               reinterpret_cast<const char*>(&row.ts), false));
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 1)), i,
                                               reinterpret_cast<const char*>(&row.state), row.stateNull));
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 2)), i,
                                               reinterpret_cast<const char*>(&row.eventStart), false));
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 3)), i,
                                               reinterpret_cast<const char*>(&row.eventEnd), false));
    EXPECT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(raw->pDataBlock, 4)), i,
                                               reinterpret_cast<const char*>(&row.state2), row.state2Null));
  }
  raw->info.rows = rows.size();
  return block;
}

Block makeDataBlock(const DataRow& row) { return makeDataBlock(std::vector<DataRow>{row}); }

class SubmitResult {
 public:
  SubmitResult() = default;
  ~SubmitResult() { stDestroyWindowChainSubmitResult(&value_); }
  SubmitResult(const SubmitResult&) = delete;
  SubmitResult& operator=(const SubmitResult&) = delete;
  SubmitResult(SubmitResult&& other) noexcept : value_(other.value_) { other.value_ = {}; }
  SubmitResult& operator=(SubmitResult&& other) noexcept {
    if (this != &other) {
      stDestroyWindowChainSubmitResult(&value_);
      value_ = other.value_;
      other.value_ = {};
    }
    return *this;
  }

  SWindowChainSubmitResult*       get() { return &value_; }
  const SWindowChainSubmitResult& value() const { return value_; }

 private:
  SWindowChainSubmitResult value_ = {};
};

class CandidateArray {
 public:
  explicit CandidateArray(size_t capacity = 4) : values_(taosArrayInit(capacity, sizeof(SLeafEventCandidate))) {
    EXPECT_NE(nullptr, values_);
  }
  ~CandidateArray() { taosArrayDestroyEx(values_, stDestroyLeafEventCandidate); }
  CandidateArray(const CandidateArray&) = delete;
  CandidateArray& operator=(const CandidateArray&) = delete;

  SArray*                    get() { return values_; }
  int32_t                    size() const { return taosArrayGetSize(values_); }
  const SLeafEventCandidate* at(int32_t index) const {
    return static_cast<const SLeafEventCandidate*>(taosArrayGet(values_, index));
  }

 private:
  SArray* values_ = nullptr;
};

SubmitResult submit(SWindowChainState* chain, TSKEY ts, const std::vector<int32_t>& values, int64_t nowNs,
                    std::vector<Block>* blocks) {
  blocks->push_back(makeBlock(ts, values));
  SSDataBlock* block = blocks->back().get();
  SArray*      refs = taosArrayInit(values.size(), sizeof(SWindowChainRowRef));
  EXPECT_NE(nullptr, refs);
  for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
    const SWindowChainRowRef ref = {block, i, 100 + i};
    EXPECT_NE(nullptr, taosArrayPush(refs, &ref));
  }
  const SWindowChainPeerGroup group = {kGid, ts, refs};
  SubmitResult                result;
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain, &group, nowNs, result.get()));
  taosArrayDestroy(refs);
  return result;
}

SubmitResult submitData(SWindowChainState* chain, const DataRow& row, int64_t nowNs, std::vector<Block>* blocks) {
  blocks->push_back(makeDataBlock(row));
  const SWindowChainRowRef ref = {blocks->back().get(), 0, 100};
  SArray*                  refs = taosArrayInit(1, sizeof(SWindowChainRowRef));
  EXPECT_NE(nullptr, refs);
  EXPECT_NE(nullptr, taosArrayPush(refs, &ref));
  const SWindowChainPeerGroup group = {kGid, row.ts, refs};
  SubmitResult                result;
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain, &group, nowNs, result.get()));
  taosArrayDestroy(refs);
  return result;
}

SubmitResult submitDataPeerGroup(SWindowChainState* chain, const std::vector<DataRow>& rows, int64_t nowNs,
                                 std::vector<Block>* blocks) {
  EXPECT_FALSE(rows.empty());
  blocks->push_back(makeDataBlock(rows));
  SSDataBlock* block = blocks->back().get();
  SArray*      refs = taosArrayInit(rows.size(), sizeof(SWindowChainRowRef));
  EXPECT_NE(nullptr, refs);
  for (int32_t i = 0; i < static_cast<int32_t>(rows.size()); ++i) {
    EXPECT_EQ(rows[0].ts, rows[i].ts);
    const SWindowChainRowRef ref = {block, i, 100 + i};
    EXPECT_NE(nullptr, taosArrayPush(refs, &ref));
  }
  const SWindowChainPeerGroup group = {kGid, rows.empty() ? 0 : rows[0].ts, refs};
  SubmitResult                result;
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain, &group, nowNs, result.get()));
  taosArrayDestroy(refs);
  return result;
}

class DataPeerGroup {
 public:
  explicit DataPeerGroup(const DataRow& row) : DataPeerGroup(std::vector<DataRow>{row}) {}

  explicit DataPeerGroup(const std::vector<DataRow>& rows) : block_(makeDataBlock(rows)) {
    EXPECT_FALSE(rows.empty());
    refs_ = taosArrayInit(rows.size(), sizeof(SWindowChainRowRef));
    EXPECT_NE(nullptr, refs_);
    for (int32_t i = 0; i < static_cast<int32_t>(rows.size()); ++i) {
      EXPECT_EQ(rows[0].ts, rows[i].ts);
      const SWindowChainRowRef ref = {block_.get(), i, 100 + i};
      if (refs_ != nullptr) EXPECT_NE(nullptr, taosArrayPush(refs_, &ref));
    }
    group_ = {kGid, rows.empty() ? 0 : rows[0].ts, refs_};
  }

  ~DataPeerGroup() { taosArrayDestroy(refs_); }

  DataPeerGroup(const DataPeerGroup&) = delete;
  DataPeerGroup& operator=(const DataPeerGroup&) = delete;

  const SWindowChainPeerGroup* get() const { return &group_; }

 private:
  Block                 block_;
  SArray*               refs_ = nullptr;
  SWindowChainPeerGroup group_ = {};
};

std::string lineageKey(const SWindowLineage& lineage) {
  std::string key;
  for (int32_t i = 0; i < taosArrayGetSize(lineage.pScopes); ++i) {
    const auto* id = static_cast<const SScopeInstanceId*>(taosArrayGet(lineage.pScopes, i));
    key += ":" + std::to_string(id->layerIndex) + ":" + std::to_string(id->triggerType) + ":" +
           std::to_string(id->openingTs) + ":" + std::to_string(id->nativeDiscriminator);
  }
  return key;
}

std::string scopeKey(const SStreamCacheScope& scope) { return std::to_string(scope.gid) + lineageKey(scope.lineage); }

bool sameScope(const SStreamCacheScope& left, const SStreamCacheScope& right) {
  return scopeKey(left) == scopeKey(right);
}

const SWindowChainAcceptedBatch* acceptedBatch(const SubmitResult& result, int32_t index) {
  return static_cast<const SWindowChainAcceptedBatch*>(taosArrayGet(result.value().pAcceptedBatches, index));
}

std::vector<int32_t> acceptedValues(const SubmitResult& result, int32_t index) {
  const SWindowChainAcceptedBatch* batch = acceptedBatch(result, index);
  std::vector<int32_t>             values;
  for (int32_t i = 0; i < taosArrayGetSize(batch->pRows); ++i) {
    const auto* ref = static_cast<const SWindowChainRowRef*>(taosArrayGet(batch->pRows, i));
    const auto* col = static_cast<const SColumnInfoData*>(taosArrayGet(ref->pBlock->pDataBlock, 1));
    values.push_back(reinterpret_cast<const int32_t*>(col->pData)[ref->rowIndex]);
  }
  return values;
}

struct CachedRow {
  TSKEY   ts;
  int32_t value;
};

using FixtureCache = std::map<std::string, std::vector<CachedRow>>;

void commitAcceptedBatchesToFixtureCache(const SubmitResult& result, FixtureCache* cache) {
  for (int32_t batchIndex = 0; batchIndex < taosArrayGetSize(result.value().pAcceptedBatches); ++batchIndex) {
    const SWindowChainAcceptedBatch* batch = acceptedBatch(result, batchIndex);
    auto&                            rows = (*cache)[scopeKey(batch->cacheScope)];
    for (int32_t rowIndex = 0; rowIndex < taosArrayGetSize(batch->pRows); ++rowIndex) {
      const auto* ref = static_cast<const SWindowChainRowRef*>(taosArrayGet(batch->pRows, rowIndex));
      const auto* tsCol = static_cast<const SColumnInfoData*>(taosArrayGet(ref->pBlock->pDataBlock, 0));
      const auto* valueCol = static_cast<const SColumnInfoData*>(taosArrayGet(ref->pBlock->pDataBlock, 1));
      rows.push_back({reinterpret_cast<const TSKEY*>(tsCol->pData)[ref->rowIndex],
                      reinterpret_cast<const int32_t*>(valueCol->pData)[ref->rowIndex]});
    }
  }
}

std::vector<int32_t> rowsForCandidate(const SLeafEventCandidate& candidate, const FixtureCache& cache) {
  std::vector<int32_t> values;
  const auto           found = cache.find(scopeKey(candidate.cacheScope));
  if (found == cache.end()) return values;
  for (const auto& row : found->second) {
    if (row.ts >= candidate.leafParam.wstart && row.ts <= candidate.leafParam.wend) values.push_back(row.value);
  }
  return values;
}

const SWindowAncestorSnapshot* snapshot(const SLeafEventCandidate& candidate, int32_t index) {
  return static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(candidate.pAncestorSnapshots, index));
}

SArray* gArrayInitResults[4] = {};
int32_t gArrayInitIndex = 0;
Stub*   gArrayAddBatchStub = nullptr;
int32_t gArrayAddBatchCalls = 0;
Stub*   gArrayGetStub = nullptr;
int64_t gArrayGetCalls = 0;

void* countArrayGet(const SArray* array, int32_t index) {
  ++gArrayGetCalls;
  gArrayGetStub->reset(taosArrayGet);
  void* value = taosArrayGet(array, index);
  gArrayGetStub->set(taosArrayGet, countArrayGet);
  return value;
}

SArray* failNthArrayInit(size_t, size_t) {
  if (gArrayInitIndex < 4) {
    SArray* result = gArrayInitResults[gArrayInitIndex];
    gArrayInitResults[gArrayInitIndex++] = nullptr;
    return result;
  }
  ++gArrayInitIndex;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

void* failSecondArrayAddBatch(SArray* array, const void* data, int32_t count) {
  ++gArrayAddBatchCalls;
  if (gArrayAddBatchCalls == 2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  gArrayAddBatchStub->reset(taosArrayAddBatch);
  void* result = taosArrayAddBatch(array, data, count);
  gArrayAddBatchStub->set(taosArrayAddBatch, failSecondArrayAddBatch);
  return result;
}

char* failWindowChainInstanceStrdupi(const char*) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

Stub*       gStrdupiStub = nullptr;
const char* gStrdupiFailureValue = nullptr;
int32_t     gStrdupiFailureMatches = 0;

char* failSelectedWindowChainStrdupi(const char* value) {
  if (value != nullptr && gStrdupiFailureValue != nullptr && strcmp(value, gStrdupiFailureValue) == 0) {
    ++gStrdupiFailureMatches;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  gStrdupiStub->reset(taosStrdupi);
  char* result = taosStrdupi(value);
  gStrdupiStub->set(taosStrdupi, failSelectedWindowChainStrdupi);
  return result;
}

Stub* gJsonPrintStub = nullptr;

Stub*   gArrayInitStub = nullptr;
bool    gFailAncestorSnapshotArrayInit = false;
int32_t gAncestorSnapshotArrayInitFailureCall = 0;
int32_t gAncestorSnapshotArrayInitCalls = 0;

SArray* failFirstAncestorSnapshotArrayInit(size_t size, size_t elemSize) {
  if (gFailAncestorSnapshotArrayInit && elemSize == sizeof(SWindowAncestorSnapshot)) {
    gFailAncestorSnapshotArrayInit = false;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  gArrayInitStub->reset(taosArrayInit);
  SArray* result = taosArrayInit(size, elemSize);
  gArrayInitStub->set(taosArrayInit, failFirstAncestorSnapshotArrayInit);
  return result;
}

SArray* failSelectedAncestorSnapshotArrayInit(size_t size, size_t elemSize) {
  if (elemSize == sizeof(SWindowAncestorSnapshot) &&
      ++gAncestorSnapshotArrayInitCalls == gAncestorSnapshotArrayInitFailureCall) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  gArrayInitStub->reset(taosArrayInit);
  SArray* result = taosArrayInit(size, elemSize);
  gArrayInitStub->set(taosArrayInit, failSelectedAncestorSnapshotArrayInit);
  return result;
}

Stub*   gArrayEnsureCapStub = nullptr;
SArray* gArrayEnsureCapFailureTarget = nullptr;

int32_t failSelectedArrayEnsureCap(SArray* pArray, size_t targetSize) {
  if (pArray == gArrayEnsureCapFailureTarget) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  gArrayEnsureCapStub->reset(taosArrayEnsureCap);
  int32_t code = taosArrayEnsureCap(pArray, targetSize);
  gArrayEnsureCapStub->set(taosArrayEnsureCap, failSelectedArrayEnsureCap);
  return code;
}

char* failCanonicalParentJsonPrint(const cJSON* object) {
  const cJSON* parent = cJSON_GetObjectItemCaseSensitive(object, "parentTriggerId");
  if (cJSON_IsString(parent) && strlen(cJSON_GetStringValue(parent)) == STREAM_NESTED_TRIGGER_ID_LEN - 1) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  gJsonPrintStub->reset(cJSON_PrintUnformatted);
  char* result = cJSON_PrintUnformatted(object);
  gJsonPrintStub->set(cJSON_PrintUnformatted, failCanonicalParentJsonPrint);
  return result;
}

std::string jsonStringMember(const char* content, const char* name) {
  std::unique_ptr<cJSON, decltype(&cJSON_Delete)> object(cJSON_Parse(content), cJSON_Delete);
  EXPECT_NE(nullptr, object);
  if (object == nullptr) return {};
  const cJSON* item = cJSON_GetObjectItemCaseSensitive(object.get(), name);
  EXPECT_TRUE(cJSON_IsString(item));
  return cJSON_IsString(item) ? cJSON_GetStringValue(item) : "";
}

TEST(StreamWindowChainTimeTest, commitsCompletePeerGroupBeforeLeafEvent) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(1000, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  FixtureCache       cache;

  SubmitResult accepted = submit(chain.get(), 1000, {11, 22}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(accepted.value().pAcceptedBatches));
  EXPECT_EQ((std::vector<int32_t>{11, 22}), acceptedValues(accepted, 0));
  commitAcceptedBatchesToFixtureCache(accepted, &cache);

  CandidateArray events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  ASSERT_EQ(1, events.size());
  EXPECT_EQ(2, events.at(0)->rowCount);
  EXPECT_EQ(2, events.at(0)->leafParam.wrownum);
  EXPECT_EQ(1, taosArrayGetSize(events.at(0)->pAncestorSnapshots));
  EXPECT_EQ(1000, events.at(0)->calcDataRange.skey);
  EXPECT_EQ(1999, events.at(0)->calcDataRange.ekey);
  EXPECT_EQ((std::vector<int32_t>{11, 22}), rowsForCandidate(*events.at(0), cache));
}

TEST(StreamWindowChainTimeTest, peerEnumerationDoesNotChangeIdentity) {
  auto run = [](const std::vector<int32_t>& values) {
    Plan               plan({intervalLayer(10000, 10000), intervalLayer(1000, 1000)});
    Chain              chain = createChain(plan);
    std::vector<Block> blocks;
    SubmitResult       result = submit(chain.get(), 1000, values, 10, &blocks);
    CandidateArray     events;
    EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
    EXPECT_EQ(1, events.size());
    const SLeafEventCandidate* event = events.at(0);
    return std::make_pair(std::make_pair(event->instanceId.openingTs, scopeKey(event->cacheScope)),
                          event->leafParam.wrownum);
  };

  const auto first = run({11, 22});
  const auto second = run({22, 11});
  EXPECT_EQ(first, second);
}

TEST(StreamWindowChainTimeTest, returnsAcceptedRowsWithoutLeafEvent) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(10000, 10000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  SubmitResult       result = submit(chain.get(), 1000, {11}, 10, &blocks);

  EXPECT_EQ(0, taosArrayGetSize(result.value().pCandidates));
  ASSERT_EQ(1, taosArrayGetSize(result.value().pAcceptedBatches));
  EXPECT_EQ((std::vector<int32_t>{11}), acceptedValues(result, 0));
}

TEST(StreamWindowChainTimeTest, slidingBoundaryRowUsesOldScopeBeforeReset) {
  Plan               plan({slidingLayer(1000), sessionLayer(10000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult before = submit(chain.get(), 999, {9}, 10, &blocks);
  SubmitResult boundary = submit(chain.get(), 1000, {10}, 20, &blocks);
  SubmitResult after = submit(chain.get(), 1001, {11}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(before.value().pAcceptedBatches));
  ASSERT_EQ(1, taosArrayGetSize(boundary.value().pAcceptedBatches));
  ASSERT_EQ(1, taosArrayGetSize(after.value().pAcceptedBatches));
  EXPECT_TRUE(sameScope(acceptedBatch(before, 0)->cacheScope, acceptedBatch(boundary, 0)->cacheScope));
  EXPECT_FALSE(sameScope(acceptedBatch(boundary, 0)->cacheScope, acceptedBatch(after, 0)->cacheScope));
  EXPECT_EQ((std::vector<int32_t>{10}), acceptedValues(boundary, 0));
  EXPECT_EQ(0, taosArrayGetSize(boundary.value().pCandidates));
}

TEST(StreamWindowChainTimeTest, sessionGapRowStartsNewScopeBeforeRouting) {
  Plan               plan({sessionLayer(5000), sessionLayer(10000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult first = submit(chain.get(), 1000, {1}, 10, &blocks);
  SubmitResult second = submit(chain.get(), 2000, {2}, 20, &blocks);
  SubmitResult gap = submit(chain.get(), 8000, {8}, 30, &blocks);
  EXPECT_TRUE(sameScope(acceptedBatch(first, 0)->cacheScope, acceptedBatch(second, 0)->cacheScope));
  EXPECT_FALSE(sameScope(acceptedBatch(second, 0)->cacheScope, acceptedBatch(gap, 0)->cacheScope));
  EXPECT_EQ(0, taosArrayGetSize(gap.value().pCandidates));
  EXPECT_EQ((std::vector<int32_t>{8}), acceptedValues(gap, 0));
}

TEST(StreamWindowChainTimeTest, intervalGapConsumesButDoesNotRouteRow) {
  Plan               plan({intervalLayer(2000, 5000), sessionLayer(10000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult accepted = submit(chain.get(), 1000, {1}, 10, &blocks);
  SubmitResult gap = submit(chain.get(), 3000, {3}, 20, &blocks);
  EXPECT_EQ(1, taosArrayGetSize(accepted.value().pAcceptedBatches));
  EXPECT_EQ(0, taosArrayGetSize(gap.value().pAcceptedBatches));
  EXPECT_EQ(0, taosArrayGetSize(gap.value().pCandidates));
}

TEST(StreamWindowChainTimeTest, eightTimeLayersFreezeEveryAncestor) {
  std::vector<SStreamWindowLayerSpec> layers(7, intervalLayer(10000, 10000));
  layers.push_back(intervalLayer(1000, 1000));
  Plan               plan(layers);
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  SubmitResult       result = submit(chain.get(), 1000, {1}, 10, &blocks);
  CandidateArray     events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));

  ASSERT_EQ(1, events.size());
  EXPECT_EQ(7, taosArrayGetSize(events.at(0)->pAncestorSnapshots));
  EXPECT_EQ(7, taosArrayGetSize(events.at(0)->lineage.pScopes));
  EXPECT_EQ(1000, events.at(0)->calcDataRange.skey);
  EXPECT_EQ(1999, events.at(0)->calcDataRange.ekey);
}

TEST(StreamWindowChainTimeTest, partialFrontierClosesOnlyAtLeafBoundary) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(1000, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  SubmitResult       result = submit(chain.get(), 100, {1}, 10, &blocks);
  CandidateArray     events;

  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 999, 20, events.get()));
  EXPECT_EQ(0, events.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 30, events.get()));
  EXPECT_EQ(1, events.size());
}

TEST(StreamWindowChainTimeTest, initializedPureSlidingLeafEmitsFrozenZeroRowCandidate) {
  Plan               plan({slidingLayer(10000), slidingLayer(1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult row = submit(chain.get(), 100, {7}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(row.value().pAcceptedBatches));
  ASSERT_EQ(0, taosArrayGetSize(row.value().pCandidates));

  CandidateArray events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  ASSERT_EQ(2, events.size());
  const SLeafEventCandidate* populated = events.at(0);
  const SLeafEventCandidate* empty = events.at(1);
  ASSERT_NE(nullptr, populated);
  ASSERT_NE(nullptr, empty);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, populated->eventType);
  EXPECT_EQ(1, populated->rowCount);
  EXPECT_EQ(1, populated->instanceId.openingTs);
  EXPECT_EQ(1, populated->leafParam.prevTs);
  EXPECT_EQ(1000, populated->leafParam.currentTs);
  EXPECT_EQ(2000, populated->leafParam.nextTs);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, empty->eventType);
  EXPECT_EQ(0, empty->rowCount);
  EXPECT_EQ(1001, empty->instanceId.openingTs);
  EXPECT_EQ(1001, empty->leafParam.prevTs);
  EXPECT_EQ(2000, empty->leafParam.currentTs);
  EXPECT_EQ(3000, empty->leafParam.nextTs);
  ASSERT_EQ(1, taosArrayGetSize(populated->lineage.pScopes));
  ASSERT_EQ(1, taosArrayGetSize(empty->lineage.pScopes));
  EXPECT_EQ(1, static_cast<const SScopeInstanceId*>(taosArrayGet(populated->lineage.pScopes, 0))->openingTs);
  EXPECT_EQ(1, static_cast<const SScopeInstanceId*>(taosArrayGet(empty->lineage.pScopes, 0))->openingTs);
  EXPECT_TRUE(sameScope(populated->cacheScope, empty->cacheScope));
  ASSERT_EQ(1, taosArrayGetSize(populated->pAncestorSnapshots));
  ASSERT_EQ(1, taosArrayGetSize(empty->pAncestorSnapshots));
  EXPECT_EQ(1, snapshot(*populated, 0)->values.sliding.prevTs);
  EXPECT_EQ(10000, snapshot(*populated, 0)->values.sliding.currentTs);
  EXPECT_EQ(20000, snapshot(*populated, 0)->values.sliding.nextTs);
  EXPECT_EQ(1, snapshot(*empty, 0)->values.sliding.prevTs);
  EXPECT_EQ(10000, snapshot(*empty, 0)->values.sliding.currentTs);
  EXPECT_EQ(20000, snapshot(*empty, 0)->values.sliding.nextTs);

  const std::string frozenScope = scopeKey(populated->cacheScope);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 3000, 30, events.get()));
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, events.at(2)->eventType);
  EXPECT_EQ(0, events.at(2)->rowCount);
  EXPECT_EQ(2001, events.at(2)->instanceId.openingTs);
  EXPECT_EQ(2001, events.at(2)->leafParam.prevTs);
  EXPECT_EQ(3000, events.at(2)->leafParam.currentTs);
  EXPECT_EQ(4000, events.at(2)->leafParam.nextTs);
  ASSERT_EQ(1, taosArrayGetSize(events.at(2)->lineage.pScopes));
  EXPECT_EQ(1, static_cast<const SScopeInstanceId*>(taosArrayGet(events.at(2)->lineage.pScopes, 0))->openingTs);
  ASSERT_EQ(1, taosArrayGetSize(events.at(2)->pAncestorSnapshots));
  EXPECT_EQ(1, snapshot(*events.at(2), 0)->values.sliding.prevTs);
  EXPECT_EQ(10000, snapshot(*events.at(2), 0)->values.sliding.currentTs);
  EXPECT_EQ(20000, snapshot(*events.at(2), 0)->values.sliding.nextTs);
  EXPECT_TRUE(sameScope(events.at(1)->cacheScope, events.at(2)->cacheScope));
  EXPECT_EQ(1, events.at(0)->instanceId.openingTs);
  EXPECT_EQ(1, events.at(0)->rowCount);
  EXPECT_EQ(1, events.at(0)->leafParam.prevTs);
  EXPECT_EQ(1000, events.at(0)->leafParam.currentTs);
  EXPECT_EQ(2000, events.at(0)->leafParam.nextTs);
  EXPECT_EQ(frozenScope, scopeKey(events.at(0)->cacheScope));
}

TEST(StreamWindowChainTimeTest, initializedIntervalLeafEmitsFrozenZeroRowCandidate) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(1000, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult row = submit(chain.get(), 100, {7}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(row.value().pAcceptedBatches));
  ASSERT_EQ(0, taosArrayGetSize(row.value().pCandidates));

  CandidateArray events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  ASSERT_EQ(2, events.size());
  const SLeafEventCandidate* populated = events.at(0);
  const SLeafEventCandidate* empty = events.at(1);
  ASSERT_NE(nullptr, populated);
  ASSERT_NE(nullptr, empty);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, populated->eventType);
  EXPECT_EQ(1, populated->rowCount);
  EXPECT_EQ(0, populated->instanceId.openingTs);
  EXPECT_EQ(0, populated->leafParam.wstart);
  EXPECT_EQ(999, populated->leafParam.wend);
  EXPECT_EQ(1, populated->leafParam.wrownum);
  EXPECT_EQ(populated->rowCount, populated->leafParam.wrownum);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, empty->eventType);
  EXPECT_EQ(0, empty->rowCount);
  EXPECT_EQ(1000, empty->instanceId.openingTs);
  EXPECT_EQ(1000, empty->leafParam.wstart);
  EXPECT_EQ(1999, empty->leafParam.wend);
  EXPECT_EQ(0, empty->leafParam.wrownum);
  EXPECT_EQ(empty->rowCount, empty->leafParam.wrownum);
  ASSERT_EQ(1, taosArrayGetSize(populated->lineage.pScopes));
  ASSERT_EQ(1, taosArrayGetSize(empty->lineage.pScopes));
  EXPECT_EQ(0, static_cast<const SScopeInstanceId*>(taosArrayGet(populated->lineage.pScopes, 0))->openingTs);
  EXPECT_EQ(0, static_cast<const SScopeInstanceId*>(taosArrayGet(empty->lineage.pScopes, 0))->openingTs);
  EXPECT_TRUE(sameScope(populated->cacheScope, empty->cacheScope));
  ASSERT_EQ(1, taosArrayGetSize(populated->pAncestorSnapshots));
  ASSERT_EQ(1, taosArrayGetSize(empty->pAncestorSnapshots));
  EXPECT_EQ(0, snapshot(*populated, 0)->values.window.start);
  EXPECT_EQ(9999, snapshot(*populated, 0)->values.window.end);
  EXPECT_EQ(1, snapshot(*populated, 0)->values.window.rownum);
  EXPECT_EQ(0, snapshot(*empty, 0)->values.window.start);
  EXPECT_EQ(9999, snapshot(*empty, 0)->values.window.end);
  EXPECT_EQ(1, snapshot(*empty, 0)->values.window.rownum);

  const std::string frozenScope = scopeKey(populated->cacheScope);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 3000, 30, events.get()));
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, events.at(2)->eventType);
  EXPECT_EQ(0, events.at(2)->rowCount);
  EXPECT_EQ(2000, events.at(2)->instanceId.openingTs);
  EXPECT_EQ(2000, events.at(2)->leafParam.wstart);
  EXPECT_EQ(2999, events.at(2)->leafParam.wend);
  EXPECT_EQ(0, events.at(2)->leafParam.wrownum);
  EXPECT_EQ(events.at(2)->rowCount, events.at(2)->leafParam.wrownum);
  ASSERT_EQ(1, taosArrayGetSize(events.at(2)->lineage.pScopes));
  EXPECT_EQ(0, static_cast<const SScopeInstanceId*>(taosArrayGet(events.at(2)->lineage.pScopes, 0))->openingTs);
  ASSERT_EQ(1, taosArrayGetSize(events.at(2)->pAncestorSnapshots));
  EXPECT_EQ(0, snapshot(*events.at(2), 0)->values.window.start);
  EXPECT_EQ(9999, snapshot(*events.at(2), 0)->values.window.end);
  EXPECT_EQ(1, snapshot(*events.at(2), 0)->values.window.rownum);
  EXPECT_TRUE(sameScope(events.at(1)->cacheScope, events.at(2)->cacheScope));
  EXPECT_EQ(0, events.at(0)->instanceId.openingTs);
  EXPECT_EQ(1, events.at(0)->rowCount);
  EXPECT_EQ(0, events.at(0)->leafParam.wstart);
  EXPECT_EQ(999, events.at(0)->leafParam.wend);
  EXPECT_EQ(1, events.at(0)->leafParam.wrownum);
  EXPECT_EQ(frozenScope, scopeKey(events.at(0)->cacheScope));
}

TEST(StreamWindowChainTimeTest, laterRowEnumeratesInitializedPureSlidingGap) {
  Plan               plan({slidingLayer(10000), slidingLayer(1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult first = submit(chain.get(), 100, {7}, 10, &blocks);
  ASSERT_EQ(0, taosArrayGetSize(first.value().pCandidates));
  SubmitResult later = submit(chain.get(), 3000, {8}, 20, &blocks);
  ASSERT_EQ(3, taosArrayGetSize(later.value().pCandidates));

  const auto* firstRange = static_cast<const SLeafEventCandidate*>(taosArrayGet(later.value().pCandidates, 0));
  const auto* gap = static_cast<const SLeafEventCandidate*>(taosArrayGet(later.value().pCandidates, 1));
  const auto* boundary = static_cast<const SLeafEventCandidate*>(taosArrayGet(later.value().pCandidates, 2));
  ASSERT_NE(nullptr, firstRange);
  ASSERT_NE(nullptr, gap);
  ASSERT_NE(nullptr, boundary);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, firstRange->eventType);
  EXPECT_EQ(1, firstRange->rowCount);
  EXPECT_EQ(1, firstRange->instanceId.openingTs);
  EXPECT_EQ(1, firstRange->leafParam.prevTs);
  EXPECT_EQ(1000, firstRange->leafParam.currentTs);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, gap->eventType);
  EXPECT_EQ(0, gap->rowCount);
  EXPECT_EQ(1001, gap->instanceId.openingTs);
  EXPECT_EQ(1001, gap->leafParam.prevTs);
  EXPECT_EQ(2000, gap->leafParam.currentTs);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, boundary->eventType);
  EXPECT_EQ(1, boundary->rowCount);
  EXPECT_EQ(2001, boundary->instanceId.openingTs);
  EXPECT_EQ(2001, boundary->leafParam.prevTs);
  EXPECT_EQ(3000, boundary->leafParam.currentTs);
  EXPECT_TRUE(sameScope(firstRange->cacheScope, gap->cacheScope));
  EXPECT_TRUE(sameScope(gap->cacheScope, boundary->cacheScope));
}

TEST(StreamWindowChainTimeTest, emptyTimeCursorDoesNotArmMaxDelay) {
  Plan               plan({slidingLayer(10000), slidingLayer(1000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 100, true);
  std::vector<Block> blocks;
  SubmitResult       row = submit(chain.get(), 100, {7}, 10, &blocks);

  CandidateArray populated;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 20, populated.get()));
  ASSERT_EQ(1, populated.size());
  EXPECT_EQ(1, populated.at(0)->rowCount);
  EXPECT_EQ(INT64_MAX, stWindowChainNextDelayDeadline(chain.get()));

  CandidateArray delayed;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainCollectDelayedCandidates(chain.get(), 10000, delayed.get()));
  EXPECT_EQ(0, delayed.size());

  CandidateArray empty;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 30, empty.get()));
  ASSERT_EQ(1, empty.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, empty.at(0)->eventType);
  EXPECT_EQ(0, empty.at(0)->rowCount);
  EXPECT_EQ(1001, empty.at(0)->instanceId.openingTs);
  EXPECT_EQ(1001, empty.at(0)->leafParam.prevTs);
  EXPECT_EQ(2000, empty.at(0)->leafParam.currentTs);

  Plan               clippedPlan({intervalLayer(3000, 3000), slidingLayer(1000)});
  Chain              clipped = createChain(clippedPlan, STRIGGER_EVENT_WINDOW_CLOSE, 100, true);
  std::vector<Block> clippedBlocks;
  SubmitResult       clippedRow = submit(clipped.get(), 100, {7}, 40, &clippedBlocks);
  CandidateArray     clippedPopulated;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(clipped.get(), 1000, 50, clippedPopulated.get()));
  ASSERT_EQ(1, clippedPopulated.size());
  EXPECT_EQ(1, clippedPopulated.at(0)->rowCount);
  CandidateArray outerClose;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(clipped.get(), 3000, 60, outerClose.get()));
  ASSERT_EQ(1, outerClose.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, outerClose.at(0)->eventType);
  EXPECT_EQ(0, outerClose.at(0)->rowCount);
  EXPECT_EQ(1001, outerClose.at(0)->instanceId.openingTs);
  EXPECT_EQ(1001, outerClose.at(0)->leafParam.prevTs);
  EXPECT_EQ(2000, outerClose.at(0)->leafParam.currentTs);
  ASSERT_EQ(1, taosArrayGetSize(outerClose.at(0)->lineage.pScopes));
  const auto* ancestor = static_cast<const SScopeInstanceId*>(taosArrayGet(outerClose.at(0)->lineage.pScopes, 0));
  ASSERT_NE(nullptr, ancestor);
  EXPECT_EQ(0, ancestor->openingTs);
}

TEST(StreamWindowChainTimeTest, slidingAncestorPassesPartialFrontierToIntervalLeaf) {
  Plan               plan({slidingLayer(5000), intervalLayer(1000, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  SubmitResult       result = submit(chain.get(), 1000, {1}, 10, &blocks);
  CandidateArray     events;

  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  ASSERT_EQ(1, events.size());
  EXPECT_EQ(1000, events.at(0)->leafParam.wstart);
  EXPECT_EQ(1999, events.at(0)->leafParam.wend);
}

TEST(StreamWindowChainTimeTest, emptySlidingTicksAdvanceLineageWithoutOpeningLeaf) {
  Plan           plan({slidingLayer(1000), sessionLayer(10000)});
  Chain          chain = createChain(plan);
  CandidateArray events;
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 10, events.get()));
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  EXPECT_EQ(0, events.size());

  std::vector<Block> blocks;
  SubmitResult       firstRow = submit(chain.get(), 2001, {1}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstRow.value().pAcceptedBatches));
  const SScopeInstanceId* scope =
      static_cast<const SScopeInstanceId*>(taosArrayGet(acceptedBatch(firstRow, 0)->cacheScope.lineage.pScopes, 0));
  ASSERT_NE(nullptr, scope);
  EXPECT_EQ(2001, scope->openingTs);
}

TEST(StreamWindowChainTimeTest, flushOnOuterCloseFreezesIncompleteLeaf) {
  Plan               plan({slidingLayer(1000), sessionLayer(10000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
  std::vector<Block> blocks;

  SubmitResult before = submit(chain.get(), 999, {9}, 10, &blocks);
  SubmitResult boundary = submit(chain.get(), 1000, {10}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(boundary.value().pCandidates));
  const auto* candidate = static_cast<const SLeafEventCandidate*>(taosArrayGet(boundary.value().pCandidates, 0));
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, candidate->eventType);
  EXPECT_EQ(2, candidate->rowCount);
  EXPECT_EQ(2, candidate->leafParam.wrownum);
  EXPECT_EQ(1, taosArrayGetSize(candidate->pAncestorSnapshots));
}

TEST(StreamWindowChainTimeTest, sessionFrontierDoesNotInventChildProgress) {
  Plan               plan({sessionLayer(5000), intervalLayer(1000, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;
  SubmitResult       row = submit(chain.get(), 100, {1}, 10, &blocks);
  CandidateArray     events;

  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 5000, 20, events.get()));
  EXPECT_EQ(0, events.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 5200, 30, events.get()));
  EXPECT_EQ(0, events.size());
}

TEST(StreamWindowChainTimeTest, overlappingLeavesExposeEarliestMaxDelayDeadline) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(3000, 2000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 1000);
  std::vector<Block> blocks;

  SubmitResult first = submit(chain.get(), 1000, {1}, 100, &blocks);
  SubmitResult second = submit(chain.get(), 2000, {2}, 1200, &blocks);
  EXPECT_EQ(1100, stWindowChainNextDelayDeadline(chain.get()));
}

TEST(StreamWindowChainTimeTest, collectDelayUpdatesOnlyMaturedLeaves) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(3000, 2000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 1000);
  std::vector<Block> blocks;
  SubmitResult       first = submit(chain.get(), 1000, {1}, 100, &blocks);
  SubmitResult       second = submit(chain.get(), 2000, {2}, 1200, &blocks);

  CandidateArray matured;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainCollectDelayedCandidates(chain.get(), 1100, matured.get()));
  ASSERT_EQ(1, matured.size());
  EXPECT_EQ(0, matured.at(0)->instanceId.openingTs);
  EXPECT_EQ(2100, stWindowChainNextDelayDeadline(chain.get()));

  CandidateArray failed;
  gArrayInitResults[0] = taosArrayInit(2, sizeof(SLeafEventCandidate));
  gArrayInitResults[1] = taosArrayInit(2, sizeof(int32_t));
  gArrayInitResults[2] = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  gArrayInitResults[3] = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(nullptr, gArrayInitResults[0]);
  ASSERT_NE(nullptr, gArrayInitResults[1]);
  ASSERT_NE(nullptr, gArrayInitResults[2]);
  ASSERT_NE(nullptr, gArrayInitResults[3]);
  gArrayInitIndex = 0;
  {
    Stub stub;
    stub.set(taosArrayInit, failNthArrayInit);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stWindowChainCollectDelayedCandidates(chain.get(), 2200, failed.get()));
  }
  EXPECT_EQ(5, gArrayInitIndex);
  EXPECT_EQ(0, failed.size());
  EXPECT_EQ(2100, stWindowChainNextDelayDeadline(chain.get()));

  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainCollectDelayedCandidates(chain.get(), 2200, failed.get()));
  ASSERT_EQ(2, failed.size());
  EXPECT_EQ(3200, stWindowChainNextDelayDeadline(chain.get()));
}

TEST(StreamWindowChainTimeTest, openDelayAndCloseSnapshotsRemainImmutable) {
  Plan               plan({sessionLayer(10000), sessionLayer(5000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 100);
  std::vector<Block> blocks;

  SubmitResult opened = submit(chain.get(), 1000, {1}, 0, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  const auto* open = static_cast<const SLeafEventCandidate*>(taosArrayGet(opened.value().pCandidates, 0));
  ASSERT_EQ(STRIGGER_EVENT_WINDOW_OPEN, open->eventType);
  EXPECT_EQ(1, open->rowCount);
  EXPECT_EQ(1, snapshot(*open, 0)->values.window.rownum);

  CandidateArray delayed;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainCollectDelayedCandidates(chain.get(), 100, delayed.get()));
  ASSERT_EQ(1, delayed.size());
  EXPECT_EQ(1, delayed.at(0)->rowCount);
  EXPECT_EQ(1, delayed.at(0)->leafParam.wrownum);
  EXPECT_EQ(1, snapshot(*delayed.at(0), 0)->values.window.rownum);

  SubmitResult extended = submit(chain.get(), 2000, {2}, 200, &blocks);
  SubmitResult closed = submit(chain.get(), 8000, {8}, 300, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(closed.value().pCandidates));
  const auto* close = static_cast<const SLeafEventCandidate*>(taosArrayGet(closed.value().pCandidates, 0));
  ASSERT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, close->eventType);
  EXPECT_EQ(2, close->rowCount);
  EXPECT_EQ(2, close->leafParam.wrownum);
  EXPECT_EQ(3, snapshot(*close, 0)->values.window.rownum);

  SubmitResult mutate = submit(chain.get(), 9000, {9}, 400, &blocks);
  EXPECT_EQ(1, open->rowCount);
  EXPECT_EQ(1, open->leafParam.wrownum);
  EXPECT_EQ(1, snapshot(*open, 0)->values.window.rownum);
  EXPECT_EQ(1, delayed.at(0)->leafParam.wrownum);
  EXPECT_EQ(1, delayed.at(0)->rowCount);
  EXPECT_EQ(1, snapshot(*delayed.at(0), 0)->values.window.rownum);
  EXPECT_EQ(2, close->leafParam.wrownum);
  EXPECT_EQ(2, close->rowCount);
  EXPECT_EQ(3, snapshot(*close, 0)->values.window.rownum);
}

TEST(StreamWindowChainTimeTest, rearmDelayClocksUsesOneCutoverTime) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(3000, 2000)});
  Chain              chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 1000);
  std::vector<Block> blocks;
  SubmitResult       first = submit(chain.get(), 1000, {1}, 100, &blocks);
  SubmitResult       second = submit(chain.get(), 2000, {2}, 1200, &blocks);

  stWindowChainRearmDelayClocks(chain.get(), 5000);
  EXPECT_EQ(6000, stWindowChainNextDelayDeadline(chain.get()));
}

TEST(StreamWindowChainTimeTest, rowDrivenIntervalRolloverClosesCompleteLeavesAndDiscardsRemainder) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(1500, 1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult oldScope = submit(chain.get(), 9000, {9}, 10, &blocks);
  SubmitResult rollover = submit(chain.get(), 10000, {10}, 20, &blocks);

  ASSERT_EQ(1, taosArrayGetSize(rollover.value().pCandidates));
  const auto* close = static_cast<const SLeafEventCandidate*>(taosArrayGet(rollover.value().pCandidates, 0));
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, close->eventType);
  EXPECT_EQ(8000, close->leafParam.wstart);
  EXPECT_EQ(9499, close->leafParam.wend);
  ASSERT_EQ(1, taosArrayGetSize(close->lineage.pScopes));
  const auto* oldAncestor = static_cast<const SScopeInstanceId*>(taosArrayGet(close->lineage.pScopes, 0));
  ASSERT_NE(nullptr, oldAncestor);
  EXPECT_EQ(0, oldAncestor->openingTs);

  CandidateArray later;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 12000, 30, later.get()));
  ASSERT_EQ(2, later.size());
  std::vector<TSKEY> starts;
  for (int32_t i = 0; i < later.size(); ++i) {
    starts.push_back(later.at(i)->leafParam.wstart);
    const auto* newAncestor = static_cast<const SScopeInstanceId*>(taosArrayGet(later.at(i)->lineage.pScopes, 0));
    ASSERT_NE(nullptr, newAncestor);
    EXPECT_EQ(10000, newAncestor->openingTs);
  }
  std::sort(starts.begin(), starts.end());
  EXPECT_EQ((std::vector<TSKEY>{9000, 10000}), starts);
}

TEST(StreamWindowChainTimeTest, rowDrivenIntervalRolloverDiscardsPureSlidingRemainder) {
  Plan               plan({intervalLayer(10000, 10000), slidingLayer(1000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult oldScope = submit(chain.get(), 9500, {9}, 10, &blocks);
  SubmitResult rollover = submit(chain.get(), 12000, {10}, 20, &blocks);

  ASSERT_EQ(1, taosArrayGetSize(rollover.value().pCandidates));
  const auto* close = static_cast<const SLeafEventCandidate*>(taosArrayGet(rollover.value().pCandidates, 0));
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, close->eventType);
  EXPECT_EQ(1, close->rowCount);
  EXPECT_EQ(11001, close->leafParam.prevTs);
  EXPECT_EQ(12000, close->leafParam.currentTs);
  ASSERT_EQ(1, taosArrayGetSize(rollover.value().pAcceptedBatches));
  EXPECT_TRUE(sameScope(close->cacheScope, acceptedBatch(rollover, 0)->cacheScope));
  const auto* newAncestor = static_cast<const SScopeInstanceId*>(taosArrayGet(close->lineage.pScopes, 0));
  ASSERT_NE(nullptr, newAncestor);
  EXPECT_EQ(10000, newAncestor->openingTs);
}

TEST(StreamWindowChainTimeTest, slidingBoundaryRolloverClosesExactLeafUnderOldLineage) {
  Plan               plan({slidingLayer(1000), intervalLayer(1000, 1000, 1)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult before = submit(chain.get(), 500, {5}, 10, &blocks);
  SubmitResult boundary = submit(chain.get(), 1000, {10}, 20, &blocks);

  ASSERT_EQ(1, taosArrayGetSize(boundary.value().pCandidates));
  const auto* close = static_cast<const SLeafEventCandidate*>(taosArrayGet(boundary.value().pCandidates, 0));
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, close->eventType);
  EXPECT_EQ(1, close->leafParam.wstart);
  EXPECT_EQ(1000, close->leafParam.wend);
  EXPECT_EQ(2, close->leafParam.wrownum);
  const auto* oldAncestor = static_cast<const SScopeInstanceId*>(taosArrayGet(close->lineage.pScopes, 0));
  ASSERT_NE(nullptr, oldAncestor);
  EXPECT_EQ(1, oldAncestor->openingTs);
}

TEST(StreamWindowChainTimeTest, overlappingLeafEnumeratesEveryContainingWindowBeyond8192) {
  Plan               plan({intervalLayer(20000, 20000), intervalLayer(8193, 1)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult row = submit(chain.get(), 10000, {1}, 10, &blocks);
  ASSERT_EQ(0, taosArrayGetSize(row.value().pCandidates));

  CandidateArray populated;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 18193, 20, populated.get()));
  ASSERT_EQ(8193, populated.size());
  for (int32_t i = 0; i < populated.size(); ++i) {
    ASSERT_NE(nullptr, populated.at(i));
    EXPECT_EQ(1808 + i, populated.at(i)->instanceId.openingTs);
    EXPECT_EQ(1808 + i, populated.at(i)->leafParam.wstart);
    EXPECT_EQ(10000 + i, populated.at(i)->leafParam.wend);
    EXPECT_EQ(1, populated.at(i)->rowCount);
    EXPECT_EQ(1, populated.at(i)->leafParam.wrownum);
    EXPECT_EQ(populated.at(i)->rowCount, populated.at(i)->leafParam.wrownum);
  }

  CandidateArray empty;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 20000, 30, empty.get()));
  ASSERT_EQ(1807, empty.size());
  for (int32_t i = 0; i < empty.size(); ++i) {
    ASSERT_NE(nullptr, empty.at(i));
    EXPECT_EQ(10001 + i, empty.at(i)->instanceId.openingTs);
    EXPECT_EQ(10001 + i, empty.at(i)->leafParam.wstart);
    EXPECT_EQ(18193 + i, empty.at(i)->leafParam.wend);
    EXPECT_EQ(0, empty.at(i)->rowCount);
    EXPECT_EQ(0, empty.at(i)->leafParam.wrownum);
    EXPECT_EQ(empty.at(i)->rowCount, empty.at(i)->leafParam.wrownum);
  }
}

TEST(StreamWindowChainTimeTest, denseOverlapRoutesWithLinearArrayLookups) {
  constexpr int32_t  overlap = 1024;
  Plan               plan({intervalLayer(20000, 20000), intervalLayer(overlap, 1)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  Stub arrayGetStub;
  gArrayGetStub = &arrayGetStub;
  gArrayGetCalls = 0;
  arrayGetStub.set(taosArrayGet, countArrayGet);
  SubmitResult row = submit(chain.get(), 10000, {1}, 10, &blocks);
  arrayGetStub.reset(taosArrayGet);
  gArrayGetStub = nullptr;

  ASSERT_EQ(0, taosArrayGetSize(row.value().pCandidates));
  EXPECT_LT(gArrayGetCalls, overlap * 16);
}

TEST(StreamWindowChainCountTest, UnmatchedRowDoesNotCloneEveryOpenInstance) {
  constexpr int32_t openInstances = 1024;
  Plan              plan({sessionLayer(10000), countLayer(openInstances * 2, 1)});
  Chain             chain = createChain(plan);
  ASSERT_NE(nullptr, chain);

  std::vector<DataRow> rows;
  rows.reserve(openInstances);
  for (int32_t i = 0; i < openInstances; ++i) rows.push_back({1000, i, false, 0, 0});
  std::vector<Block> blocks;
  SubmitResult       populated = submitDataPeerGroup(chain.get(), rows, 10, &blocks);
  ASSERT_EQ(0, taosArrayGetSize(populated.value().pCandidates));

  DataPeerGroup unmatched({2000, 0, true, 0, 0});
  SubmitResult  result;
  Stub          arrayGetStub;
  gArrayGetStub = &arrayGetStub;
  gArrayGetCalls = 0;
  arrayGetStub.set(taosArrayGet, countArrayGet);
  const int32_t code = stWindowChainSubmitPeerGroup(chain.get(), unmatched.get(), 20, result.get());
  arrayGetStub.reset(taosArrayGet);
  gArrayGetStub = nullptr;

  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  EXPECT_LT(gArrayGetCalls, 128);
}

TEST(StreamWindowChainTimeTest, createRejectsOverlappingNonLeafInterval) {
  Plan               plan({intervalLayer(2000, 1000), sessionLayer(1000)});
  SWindowChainPolicy policy = {};
  SWindowChainState* state = nullptr;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stWindowChainCreate(plan.get(), kGid, &policy, &state));
  EXPECT_EQ(nullptr, state);
}

TEST(StreamWindowChainTimeTest, createRejectsNaturalUnitOverlappingNonLeafInterval) {
  const int64_t day = 24 * 60 * 60 * 1000LL;
  auto          outer = intervalLayer(1, day, 0, TIME_UNIT_MONTH, TIME_UNIT_DAY, TIME_UNIT_MILLISECOND);
  outer.trigger.sliding.overlap = true;
  Plan               plan({outer, sessionLayer(1000)});
  SWindowChainPolicy policy = {};
  SWindowChainState* state = nullptr;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, stWindowChainCreate(plan.get(), kGid, &policy, &state));
  EXPECT_EQ(nullptr, state);
}

TEST(StreamWindowChainTimeTest, emptySlidingFrontierLeavesChildLayersForFirstRow) {
  Plan           plan({slidingLayer(1000), slidingLayer(1000, 250), slidingLayer(1000, 500), sessionLayer(1000)});
  Chain          chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN);
  CandidateArray events;

  {
    Stub stub;
    gArrayAddBatchStub = &stub;
    gArrayAddBatchCalls = 0;
    stub.set(taosArrayAddBatch, failSecondArrayAddBatch);
    EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 10, events.get()));
  }
  gArrayAddBatchStub = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 20, events.get()));
  ASSERT_EQ(0, events.size());

  std::vector<Block> blocks;
  SubmitResult       firstRow = submit(chain.get(), 2001, {1}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstRow.value().pCandidates));
  const auto* opened = static_cast<const SLeafEventCandidate*>(taosArrayGet(firstRow.value().pCandidates, 0));
  ASSERT_NE(nullptr, opened);
  ASSERT_EQ(3, taosArrayGetSize(opened->lineage.pScopes));
  EXPECT_EQ(2001, static_cast<const SScopeInstanceId*>(taosArrayGet(opened->lineage.pScopes, 0))->openingTs);
  EXPECT_EQ(1251, static_cast<const SScopeInstanceId*>(taosArrayGet(opened->lineage.pScopes, 1))->openingTs);
  EXPECT_EQ(1501, static_cast<const SScopeInstanceId*>(taosArrayGet(opened->lineage.pScopes, 2))->openingTs);
  ASSERT_EQ(3, taosArrayGetSize(opened->pAncestorSnapshots));
  EXPECT_EQ(2001, snapshot(*opened, 0)->values.sliding.prevTs);
  EXPECT_EQ(1251, snapshot(*opened, 1)->values.sliding.prevTs);
  EXPECT_EQ(1501, snapshot(*opened, 2)->values.sliding.prevTs);
}

TEST(StreamWindowChainTimeTest, intervalOffsetKeepsClosedRangeBoundaries) {
  Plan               plan({intervalLayer(10000, 10000), intervalLayer(1000, 1000, 250)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult   first = submit(chain.get(), 250, {1}, 10, &blocks);
  SubmitResult   last = submit(chain.get(), 1249, {2}, 20, &blocks);
  CandidateArray events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1249, 30, events.get()));
  EXPECT_EQ(0, events.size());
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1250, 40, events.get()));
  ASSERT_EQ(1, events.size());
  EXPECT_EQ(250, events.at(0)->leafParam.wstart);
  EXPECT_EQ(1249, events.at(0)->leafParam.wend);
  EXPECT_EQ(2, events.at(0)->leafParam.wrownum);
}

TEST(StreamWindowChainTimeTest, slidingOffsetClosesAtItsNaturalBoundary) {
  Plan               plan({intervalLayer(10000, 10000), slidingLayer(1000, 250)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult   row = submit(chain.get(), 251, {1}, 10, &blocks);
  CandidateArray events;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1250, 20, events.get()));
  ASSERT_EQ(1, events.size());
  EXPECT_EQ(251, events.at(0)->leafParam.prevTs);
  EXPECT_EQ(1250, events.at(0)->leafParam.currentTs);
}

TEST(StreamWindowChainTimeTest, naturalMonthRangeMatchesSingleWindowSemantics) {
  const int64_t day = 24 * 60 * 60 * 1000LL;
  auto          naturalMonth = intervalLayer(1, day, 0, TIME_UNIT_MONTH, TIME_UNIT_DAY, TIME_UNIT_MILLISECOND);
  Plan          plan({sessionLayer(40 * day), naturalMonth});
  Chain         chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN);

  SStreamTriggerTask reference = {};
  reference.interval.intervalUnit = TIME_UNIT_MONTH;
  reference.interval.slidingUnit = TIME_UNIT_DAY;
  reference.interval.offsetUnit = TIME_UNIT_MILLISECOND;
  reference.interval.precision = TSDB_TIME_PRECISION_MILLI;
  reference.interval.interval = 1;
  reference.interval.sliding = day;
  const TSKEY       ts = 45 * day;
  const STimeWindow expected = stTriggerTaskGetTimeWindow(&reference, ts);

  std::vector<Block> blocks;
  SubmitResult       row = submit(chain.get(), ts, {1}, 10, &blocks);
  bool               found = false;
  for (int32_t i = 0; i < taosArrayGetSize(row.value().pCandidates); ++i) {
    const auto* opened = static_cast<const SLeafEventCandidate*>(taosArrayGet(row.value().pCandidates, i));
    if (opened->leafParam.wstart == expected.skey && opened->leafParam.wend == expected.ekey) found = true;
  }
  EXPECT_TRUE(found);
}

TEST(StreamWindowChainTimeTest, sessionExactGapBoundaryStaysInClosedRange) {
  Plan               plan({sessionLayer(20000), sessionLayer(5000)});
  Chain              chain = createChain(plan);
  std::vector<Block> blocks;

  SubmitResult first = submit(chain.get(), 1000, {1}, 10, &blocks);
  SubmitResult boundary = submit(chain.get(), 6000, {2}, 20, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(boundary.value().pCandidates));
  EXPECT_TRUE(sameScope(acceptedBatch(first, 0)->cacheScope, acceptedBatch(boundary, 0)->cacheScope));

  SubmitResult outside = submit(chain.get(), 11001, {3}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(outside.value().pCandidates));
  const auto* close = static_cast<const SLeafEventCandidate*>(taosArrayGet(outside.value().pCandidates, 0));
  EXPECT_EQ(1000, close->leafParam.wstart);
  EXPECT_EQ(6000, close->leafParam.wend);
  EXPECT_EQ(2, close->leafParam.wrownum);
}

const SLeafEventCandidate* candidateAt(const SubmitResult& result, int32_t index) {
  return static_cast<const SLeafEventCandidate*>(taosArrayGet(result.value().pCandidates, index));
}

const SScopeInstanceId* scopeAt(const SWindowLineage& lineage, int32_t index) {
  const auto* scope = static_cast<const SScopeInstanceId*>(taosArrayGet(lineage.pScopes, index));
  EXPECT_NE(nullptr, scope);
  return scope;
}

void expectWindowEvent(const SLeafEventCandidate* event, int32_t eventType, TSKEY start, TSKEY end, int64_t rows,
                       int64_t discriminator = 0) {
  ASSERT_NE(nullptr, event);
  EXPECT_EQ(eventType, event->eventType);
  EXPECT_EQ(start, event->leafParam.wstart);
  EXPECT_EQ(end, event->leafParam.wend);
  EXPECT_EQ(end - start, event->leafParam.wduration);
  EXPECT_EQ(rows, event->rowCount);
  EXPECT_EQ(rows, event->leafParam.wrownum);
  EXPECT_EQ(event->rowCount, event->leafParam.wrownum);
  EXPECT_EQ(discriminator, event->instanceId.nativeDiscriminator);
}

TEST(StreamWindowChainDataPeerTest, EventOpenFreezesAfterCompletePeerGroup) {
  Plan  plan({sessionLayer(10000), eventLayer(false)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult result =
      submitDataPeerGroup(chain.get(), {{1000, 11, false, 1, 0}, {1000, 22, false, 1, 0}}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(result.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(result, 0);
  expectWindowEvent(open, STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2, -1);
  ASSERT_EQ(1, taosArrayGetSize(open->pAncestorSnapshots));
  EXPECT_EQ(2, snapshot(*open, 0)->values.window.rownum);

  FixtureCache cache;
  commitAcceptedBatchesToFixtureCache(result, &cache);
  ASSERT_EQ(1, cache.size());
  EXPECT_EQ((std::vector<int32_t>{11, 22}), rowsForCandidate(*open, cache));
  for (int32_t i = 0; i < taosArrayGetSize(result.value().pAcceptedBatches); ++i) {
    EXPECT_TRUE(sameScope(open->cacheScope, acceptedBatch(result, i)->cacheScope));
  }
}

TEST(StreamWindowChainDataPeerTest, EventPeerEnumerationDoesNotChangeFrozenIdentity) {
  struct FrozenEvent {
    int64_t              gid;
    int8_t               triggerType;
    TSKEY                openingTs;
    int64_t              discriminator;
    std::string          instanceLineage;
    std::string          candidateLineage;
    std::string          cacheScope;
    int64_t              ancestorRownum;
    std::vector<int32_t> membership;
  };

  auto run = [](const std::vector<DataRow>& rows) {
    Plan  plan({sessionLayer(10000), eventLayer(false)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_NONE);
    EXPECT_NE(nullptr, chain);
    std::vector<Block> blocks;
    SubmitResult       result = submitDataPeerGroup(chain.get(), rows, 10, &blocks);
    EXPECT_EQ(1, taosArrayGetSize(result.value().pCandidates));
    const SLeafEventCandidate* open = candidateAt(result, 0);
    FixtureCache               cache;
    commitAcceptedBatchesToFixtureCache(result, &cache);
    std::vector<int32_t> membership = rowsForCandidate(*open, cache);
    std::sort(membership.begin(), membership.end());
    return FrozenEvent{open->instanceId.gid,
                       open->instanceId.triggerType,
                       open->instanceId.openingTs,
                       open->instanceId.nativeDiscriminator,
                       lineageKey(open->instanceId.lineage),
                       lineageKey(open->lineage),
                       scopeKey(open->cacheScope),
                       snapshot(*open, 0)->values.window.rownum,
                       membership};
  };

  const FrozenEvent first = run({{1000, 11, false, 1, 0}, {1000, 22, false, 1, 0}});
  const FrozenEvent reversed = run({{1000, 22, false, 1, 0}, {1000, 11, false, 1, 0}});
  EXPECT_EQ(first.gid, reversed.gid);
  EXPECT_EQ(first.triggerType, reversed.triggerType);
  EXPECT_EQ(first.openingTs, reversed.openingTs);
  EXPECT_EQ(first.discriminator, reversed.discriminator);
  EXPECT_EQ(first.instanceLineage, reversed.instanceLineage);
  EXPECT_EQ(first.candidateLineage, reversed.candidateLineage);
  EXPECT_EQ(first.cacheScope, reversed.cacheScope);
  EXPECT_EQ(first.ancestorRownum, reversed.ancestorRownum);
  EXPECT_EQ(first.membership, reversed.membership);
}

TEST(StreamWindowChainDataPeerTest, StateOpenFreezesAfterCompletePeerGroup) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult result = submitDataPeerGroup(chain.get(), {{1000, 7, false, 0, 0}, {1000, 7, false, 0, 0}}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(result.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(result, 0);
  expectWindowEvent(open, STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2);
  ASSERT_EQ(1, taosArrayGetSize(open->pAncestorSnapshots));
  EXPECT_EQ(2, snapshot(*open, 0)->values.window.rownum);
}

TEST(StreamWindowChainDataPeerTest, CountOpenAndCloseFreezeAfterCompletePeerGroup) {
  Plan  plan({sessionLayer(10000), countLayer(2, 2)});
  Chain chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult result =
      submitDataPeerGroup(chain.get(), {{1000, 11, false, 0, 0}, {1000, 22, false, 0, 0}}, 10, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(result.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(result, 0);
  const SLeafEventCandidate* close = candidateAt(result, 1);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_OPEN, open->eventType);
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, close->eventType);
  EXPECT_EQ(2, open->rowCount);
  EXPECT_EQ(2, open->leafParam.wrownum);
  EXPECT_EQ(2, close->rowCount);
  EXPECT_EQ(2, close->leafParam.wrownum);
  EXPECT_EQ(open->instanceId.gid, close->instanceId.gid);
  EXPECT_EQ(open->instanceId.triggerType, close->instanceId.triggerType);
  EXPECT_EQ(open->instanceId.openingTs, close->instanceId.openingTs);
  EXPECT_EQ(open->instanceId.nativeDiscriminator, close->instanceId.nativeDiscriminator);
  EXPECT_EQ(lineageKey(open->instanceId.lineage), lineageKey(close->instanceId.lineage));
  ASSERT_EQ(1, taosArrayGetSize(open->pAncestorSnapshots));
  ASSERT_EQ(1, taosArrayGetSize(close->pAncestorSnapshots));
  EXPECT_EQ(2, snapshot(*open, 0)->values.window.rownum);
  EXPECT_EQ(2, snapshot(*close, 0)->values.window.rownum);
}

TEST(StreamWindowChainDataPeerTest, CountOverlappingSameTimestampKeepsExactInstanceSnapshots) {
  Plan  plan({sessionLayer(10000), countLayer(3, 1)});
  Chain chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult result = submitDataPeerGroup(
      chain.get(), {{1000, 11, false, 0, 0}, {1000, 22, false, 0, 0}, {1000, 33, false, 0, 0}}, 10, &blocks);
  ASSERT_EQ(4, taosArrayGetSize(result.value().pCandidates));
  expectWindowEvent(candidateAt(result, 0), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 3);
  expectWindowEvent(candidateAt(result, 1), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2);
  expectWindowEvent(candidateAt(result, 2), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 1);
  expectWindowEvent(candidateAt(result, 3), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 1000, 3);
  for (int32_t i = 0; i < taosArrayGetSize(result.value().pCandidates); ++i) {
    const SLeafEventCandidate* candidate = candidateAt(result, i);
    ASSERT_EQ(1, taosArrayGetSize(candidate->pAncestorSnapshots));
    EXPECT_EQ(3, snapshot(*candidate, 0)->values.window.rownum);
  }
}

TEST(StreamWindowChainDataPeerTest, PeerCandidateFreezeFailureRollsBackCompleteGroupAndRetry) {
  Plan  plan({sessionLayer(10000), eventLayer(false)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  DataPeerGroup group({{1000, 11, false, 1, 0}, {1000, 22, false, 1, 0}});

  SubmitResult failed;
  {
    Stub stub;
    gArrayInitStub = &stub;
    gFailAncestorSnapshotArrayInit = true;
    stub.set(taosArrayInit, failFirstAncestorSnapshotArrayInit);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stWindowChainSubmitPeerGroup(chain.get(), group.get(), 10, failed.get()));
  }
  gArrayInitStub = nullptr;
  gFailAncestorSnapshotArrayInit = false;
  EXPECT_EQ(nullptr, failed.value().pAcceptedBatches);
  EXPECT_EQ(nullptr, failed.value().pCandidates);

  SubmitResult retry;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain.get(), group.get(), 10, retry.get()));
  ASSERT_EQ(1, taosArrayGetSize(retry.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(retry, 0);
  expectWindowEvent(open, STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2, -1);
  ASSERT_EQ(1, taosArrayGetSize(open->pAncestorSnapshots));
  EXPECT_EQ(2, snapshot(*open, 0)->values.window.rownum);
}

TEST(StreamWindowChainDataTest, SuppressesRecalculatedCountSiblingsButKeepsNewWindows) {
  Plan  plan({sessionLayer(10000), countLayer(3, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {1000, 1, false, 0, 0}, 10, &blocks);
  submitData(chain.get(), {2000, 2, false, 0, 0}, 20, &blocks);
  SubmitResult firstClose = submitData(chain.get(), {3000, 3, false, 0, 0}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstClose.value().pCandidates));
  expectWindowEvent(candidateAt(firstClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 3000, 3);

  STimeWindow firstOpen = {0};
  ASSERT_TRUE(stWindowChainGetFirstOpenCountLeafRange(chain.get(), &firstOpen));
  EXPECT_EQ(2000, firstOpen.skey);
  EXPECT_EQ(3000, firstOpen.ekey);
  stWindowChainSuppressOpenCountLeafBefore(chain.get(), 3000);

  SubmitResult suppressedClose = submitData(chain.get(), {4000, 4, false, 0, 0}, 40, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(suppressedClose.value().pCandidates));
  SubmitResult nextClose = submitData(chain.get(), {5000, 5, false, 0, 0}, 50, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(nextClose.value().pCandidates));
  expectWindowEvent(candidateAt(nextClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 3000, 5000, 3);
}

TEST(StreamWindowChainDataTest, InsertsDisorderIntoActiveOverlappingCountWindows) {
  Plan  plan({intervalLayer(60000, 60000), countLayer(3, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {1000, 10, false, 0, 0}, 10, &blocks);
  submitData(chain.get(), {3000, 30, false, 0, 0}, 20, &blocks);

  SubmitResult firstClose = submitData(chain.get(), {2000, 20, false, 0, 0}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstClose.value().pCandidates));
  expectWindowEvent(candidateAt(firstClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 3000, 3);

  SubmitResult secondClose = submitData(chain.get(), {4000, 40, false, 0, 0}, 40, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(secondClose.value().pCandidates));
  expectWindowEvent(candidateAt(secondClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 2000, 4000, 3);

  SubmitResult thirdClose = submitData(chain.get(), {5000, 50, false, 0, 0}, 50, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(thirdClose.value().pCandidates));
  expectWindowEvent(candidateAt(thirdClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 3000, 5000, 3);
}

TEST(StreamWindowChainDataTest, NestedLeafMatchesFrozenLegacyMatrix) {
  // Catches STATE change being routed into the old leaf and verifies literal
  // open/close calc snapshots rather than recomputing them from chain helpers.
  {
    Plan  plan({sessionLayer(10000), stateLayer()});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult first = submitData(chain.get(), {1000, 7, false, 0, 0}, 10, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
    expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 1);
    SubmitResult same = submitData(chain.get(), {2000, 7, false, 0, 0}, 20, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(same.value().pCandidates));
    SubmitResult changed = submitData(chain.get(), {3000, 8, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(changed.value().pCandidates));
    expectWindowEvent(candidateAt(changed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 2);
    expectWindowEvent(candidateAt(changed, 1), STRIGGER_EVENT_WINDOW_OPEN, 3000, 3000, 1);
  }

  // Catches COUNT(N, M) losing overlapping instances or closing one row late.
  {
    Plan  plan({sessionLayer(10000), countLayer(3, 2)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {1000, 1, false, 0, 0}, 10, &blocks);
    submitData(chain.get(), {2000, 2, false, 0, 0}, 20, &blocks);
    SubmitResult firstClose = submitData(chain.get(), {3000, 3, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(firstClose.value().pCandidates));
    expectWindowEvent(candidateAt(firstClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 3000, 3);
    submitData(chain.get(), {4000, 4, false, 0, 0}, 40, &blocks);
    SubmitResult secondClose = submitData(chain.get(), {5000, 5, false, 0, 0}, 50, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(secondClose.value().pCandidates));
    expectWindowEvent(candidateAt(secondClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 3000, 5000, 3);
  }

  // Catches multi-START EVENT collapsing all subevents into one identity.
  {
    Plan  plan({sessionLayer(10000), eventLayer(true)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {1000, 1, false, 1, 0}, 10, &blocks);
    submitData(chain.get(), {2000, 2, false, 1, 0}, 20, &blocks);
    SubmitResult switched = submitData(chain.get(), {3000, 3, false, 2, 0}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(switched.value().pCandidates));
    expectWindowEvent(candidateAt(switched, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 2, 0);
    SubmitResult ended = submitData(chain.get(), {4000, 4, false, 0, 1}, 40, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(ended.value().pCandidates));
    expectWindowEvent(candidateAt(ended, 0), STRIGGER_EVENT_WINDOW_CLOSE, 3000, 4000, 2, 1);
    expectWindowEvent(candidateAt(ended, 1), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 4000, 4, -1);
  }

  // Catches TRUE_FOR being applied only to forced close or emitting an open
  // before the literal row-count threshold is met.
  {
    Plan  plan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    SubmitResult       first = submitData(chain.get(), {1000, 7, false, 0, 0}, 10, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(first.value().pCandidates));
    SubmitResult threshold = submitData(chain.get(), {2000, 7, false, 0, 0}, 20, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(threshold.value().pCandidates));
    expectWindowEvent(candidateAt(threshold, 0), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2);
    SubmitResult changed = submitData(chain.get(), {3000, 8, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(changed.value().pCandidates));
    expectWindowEvent(candidateAt(changed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 2);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayer(false, 2)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    SubmitResult       first = submitData(chain.get(), {1000, 1, false, 1, 0}, 10, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(first.value().pCandidates));
    SubmitResult threshold = submitData(chain.get(), {2000, 2, false, 0, 0}, 20, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(threshold.value().pCandidates));
    expectWindowEvent(candidateAt(threshold, 0), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2, -1);
    SubmitResult ended = submitData(chain.get(), {3000, 3, false, 0, 1}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(ended.value().pCandidates));
    expectWindowEvent(candidateAt(ended, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 3000, 3, -1);
  }
}

TEST(StreamWindowChainDataTest, stateChangeResetsBeforeRoutingNewRow) {
  Plan  plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), countLayer(2, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {1000, 7, false, 0, 0}, 10, &blocks);
  SubmitResult oldResult = submitData(chain.get(), {2000, 7, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(oldResult.value().pCandidates));
  EXPECT_EQ(1000, scopeAt(candidateAt(oldResult, 0)->instanceId.lineage, 0)->openingTs);
  EXPECT_EQ((std::vector<int32_t>{7, 7}), rowsForCandidate(*candidateAt(oldResult, 0), [&] {
              FixtureCache cache;
              commitAcceptedBatchesToFixtureCache(first, &cache);
              commitAcceptedBatchesToFixtureCache(oldResult, &cache);
              return cache;
            }()));

  SubmitResult boundary = submitData(chain.get(), {3000, 8, false, 0, 0}, 30, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(boundary.value().pCandidates));
  ASSERT_EQ(1, taosArrayGetSize(boundary.value().pAcceptedBatches));
  EXPECT_EQ(3000, scopeAt(acceptedBatch(boundary, 0)->cacheScope.lineage, 0)->openingTs);
}

TEST(StreamWindowChainDataTest, countGapConsumesButDoesNotRouteRows) {
  Plan  plan({countLayer(2, 4), countLayer(2, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {1000, 1, false, 0, 0}, 10, &blocks);
  SubmitResult first = submitData(chain.get(), {2000, 2, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 2);

  SubmitResult gap1 = submitData(chain.get(), {3000, 3, false, 0, 0}, 30, &blocks);
  SubmitResult gap2 = submitData(chain.get(), {4000, 4, false, 0, 0}, 40, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(gap1.value().pAcceptedBatches));
  EXPECT_EQ(0, taosArrayGetSize(gap2.value().pAcceptedBatches));
  SubmitResult next = submitData(chain.get(), {5000, 5, false, 0, 0}, 50, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(next.value().pAcceptedBatches));
  EXPECT_EQ(5000, scopeAt(acceptedBatch(next, 0)->cacheScope.lineage, 0)->openingTs);
}

TEST(StreamWindowChainDataTest, CountConditionSlotsGatePositionAndChildRouting) {
  {
    Plan  plan({sessionLayer(10000), countLayer(2, 1, {1, 4})});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult skipped = submitData(chain.get(), {100, 0, true, 0, 0, 0, true}, 10, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(skipped.value().pAcceptedBatches));
    EXPECT_EQ(0, taosArrayGetSize(skipped.value().pCandidates));
    submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
    SubmitResult closed = submitData(chain.get(), {300, 0, true, 0, 0, 10, false}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 200, 300, 2);
  }
  {
    Plan  plan({sessionLayer(10000), countLayer(2, 1, {1})});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 0, true, 0, 0}, 10, &blocks);
    submitData(chain.get(), {200, 1, false, 0, 0}, 20, &blocks);
    SubmitResult closed = submitData(chain.get(), {300, 2, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 200, 300, 2);
  }
  {
    Plan  plan({sessionLayer(10000), countLayer(2, 1, {})});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 0, true, 0, 0, 0, true}, 10, &blocks);
    SubmitResult closed = submitData(chain.get(), {200, 0, true, 0, 0, 0, true}, 20, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 2);
  }
  {
    Plan  plan({countLayer(2, 2, {1, 4}), countLayer(1, 1, {})});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult skipped = submitData(chain.get(), {100, 0, true, 0, 0, 0, true}, 10, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(skipped.value().pAcceptedBatches));
    EXPECT_EQ(0, taosArrayGetSize(skipped.value().pCandidates));
    SubmitResult routed = submitData(chain.get(), {200, 0, true, 0, 0, 10, false}, 20, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(routed.value().pCandidates));
    expectWindowEvent(candidateAt(routed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 200, 200, 1);
  }
}

TEST(StreamWindowChainImpactDomainTest, CandidateFreezesRootImpactExtent) {
  Plan  plan({intervalLayer(1000, 1000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult result = submitData(chain.get(), {1500, 7, false, 0, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(result.value().pCandidates));
  const SLeafEventCandidate* candidate = candidateAt(result, 0);
  EXPECT_EQ(1000, candidate->rootImpactExtent.skey);
  EXPECT_EQ(1999, candidate->rootImpactExtent.ekey);
}

TEST(StreamWindowChainImpactDomainTest, FixedRootExpandsToCompleteScopes) {
  Plan                plan({intervalLayer(1000, 1000), stateLayer()});
  const STimeWindow   scanRange = {500, 3500};
  const STimeWindow   calcRange = {1700, 2300};
  SRecalcImpactDomain domain = {};

  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainBuildRecalcImpactDomain(plan.get(), kGid, &scanRange, &calcRange, &domain));
  ASSERT_NE(nullptr, domain.pRootExtents);
  ASSERT_EQ(1, taosArrayGetSize(domain.pRootExtents));
  const auto* extent = static_cast<const STimeWindow*>(taosArrayGet(domain.pRootExtents, 0));
  EXPECT_EQ(1000, extent->skey);
  EXPECT_EQ(2999, extent->ekey);
  EXPECT_EQ(500, domain.replayAnchor);
  EXPECT_EQ(3500, domain.capturedFrontier);
  stDestroyRecalcImpactDomain(&domain);
}

TEST(StreamWindowChainImpactDomainTest, DataDrivenRootUsesReplayAnchorAndFrontier) {
  Plan                plan({sessionLayer(10000), stateLayer()});
  const STimeWindow   scanRange = {100, 500};
  const STimeWindow   calcRange = {200, 400};
  SRecalcImpactDomain domain = {};

  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainBuildRecalcImpactDomain(plan.get(), kGid, &scanRange, &calcRange, &domain));
  ASSERT_NE(nullptr, domain.pRootExtents);
  ASSERT_EQ(1, taosArrayGetSize(domain.pRootExtents));
  const auto* extent = static_cast<const STimeWindow*>(taosArrayGet(domain.pRootExtents, 0));
  EXPECT_EQ(100, extent->skey);
  EXPECT_EQ(500, extent->ekey);
  EXPECT_EQ(100, domain.replayAnchor);
  EXPECT_EQ(500, domain.capturedFrontier);
  stDestroyRecalcImpactDomain(&domain);
}

TEST(StreamWindowChainImpactDomainTest, CloneOwnsRootExtents) {
  SRecalcImpactDomain source = {};
  source.gid = kGid;
  source.replayAnchor = 100;
  source.capturedFrontier = 500;
  source.pRootExtents = taosArrayInit(2, sizeof(STimeWindow));
  ASSERT_NE(nullptr, source.pRootExtents);
  const STimeWindow first = {100, 199};
  const STimeWindow second = {300, 399};
  ASSERT_NE(nullptr, taosArrayPush(source.pRootExtents, &first));
  ASSERT_NE(nullptr, taosArrayPush(source.pRootExtents, &second));

  SRecalcImpactDomain cloned = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, stCloneRecalcImpactDomain(&source, &cloned));
  EXPECT_EQ(source.gid, cloned.gid);
  EXPECT_EQ(source.replayAnchor, cloned.replayAnchor);
  EXPECT_EQ(source.capturedFrontier, cloned.capturedFrontier);
  ASSERT_NE(nullptr, cloned.pRootExtents);
  EXPECT_NE(source.pRootExtents, cloned.pRootExtents);
  ASSERT_EQ(2, taosArrayGetSize(cloned.pRootExtents));
  auto* pSourceFirst = static_cast<STimeWindow*>(taosArrayGet(source.pRootExtents, 0));
  ASSERT_NE(nullptr, pSourceFirst);
  pSourceFirst->skey = 0;
  const auto* pClonedFirst = static_cast<const STimeWindow*>(taosArrayGet(cloned.pRootExtents, 0));
  ASSERT_NE(nullptr, pClonedFirst);
  EXPECT_EQ(100, pClonedFirst->skey);
  EXPECT_EQ(199, pClonedFirst->ekey);

  stDestroyRecalcImpactDomain(&cloned);
  stDestroyRecalcImpactDomain(&source);
}

TEST(StreamWindowChainImpactDomainTest, UnionMergesAdjacentAndOverlappingRootExtents) {
  SRecalcImpactDomain left = {};
  left.gid = kGid;
  left.replayAnchor = 100;
  left.capturedFrontier = 400;
  left.pRootExtents = taosArrayInit(2, sizeof(STimeWindow));
  ASSERT_NE(nullptr, left.pRootExtents);
  const STimeWindow leftFirst = {0, 9};
  const STimeWindow leftSecond = {30, 39};
  ASSERT_NE(nullptr, taosArrayPush(left.pRootExtents, &leftFirst));
  ASSERT_NE(nullptr, taosArrayPush(left.pRootExtents, &leftSecond));

  SRecalcImpactDomain right = {};
  right.gid = kGid;
  right.replayAnchor = 50;
  right.capturedFrontier = 500;
  right.pRootExtents = taosArrayInit(2, sizeof(STimeWindow));
  ASSERT_NE(nullptr, right.pRootExtents);
  const STimeWindow rightFirst = {10, 20};
  const STimeWindow rightSecond = {35, 50};
  ASSERT_NE(nullptr, taosArrayPush(right.pRootExtents, &rightFirst));
  ASSERT_NE(nullptr, taosArrayPush(right.pRootExtents, &rightSecond));

  SRecalcImpactDomain merged = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, stUnionRecalcImpactDomains(&left, &right, &merged));
  EXPECT_EQ(kGid, merged.gid);
  EXPECT_EQ(50, merged.replayAnchor);
  EXPECT_EQ(500, merged.capturedFrontier);
  ASSERT_NE(nullptr, merged.pRootExtents);
  ASSERT_EQ(2, taosArrayGetSize(merged.pRootExtents));
  const auto* pFirst = static_cast<const STimeWindow*>(taosArrayGet(merged.pRootExtents, 0));
  const auto* pSecond = static_cast<const STimeWindow*>(taosArrayGet(merged.pRootExtents, 1));
  ASSERT_NE(nullptr, pFirst);
  ASSERT_NE(nullptr, pSecond);
  EXPECT_EQ(0, pFirst->skey);
  EXPECT_EQ(20, pFirst->ekey);
  EXPECT_EQ(30, pSecond->skey);
  EXPECT_EQ(50, pSecond->ekey);

  stDestroyRecalcImpactDomain(&merged);
  stDestroyRecalcImpactDomain(&right);
  stDestroyRecalcImpactDomain(&left);
}

TEST(StreamWindowChainDataTest, StateLeafFirstNullRowDoesNotReadEmptyInstanceArray) {
  Plan  plan({slidingLayer(1000), stateLayer()});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {1000, 1, false, 0, 0}, 5, &blocks);

  terrno = TSDB_CODE_SUCCESS;
  SubmitResult first = submitData(chain.get(), {1001, 0, true, 0, 0}, 10, &blocks);
  EXPECT_EQ(TSDB_CODE_SUCCESS, terrno);
  EXPECT_EQ(1, taosArrayGetSize(first.value().pAcceptedBatches));
  EXPECT_EQ(0, taosArrayGetSize(first.value().pCandidates));
}

TEST(StreamWindowChainDataTest, StateForwardPendingNullExtendsInputRetentionRange) {
  Plan  plan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_FORWARD)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {1000, 0, true, 0, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pAcceptedBatches));

  STimeWindow range = {};
  ASSERT_TRUE(stWindowChainGetInputRetentionRange(chain.get(), &range));
  EXPECT_EQ(1000, range.skey);
  EXPECT_EQ(1000, range.ekey);
}

TEST(StreamWindowChainDataTest, StateScopeFirstNullRowDoesNotReadEmptyInstanceArray) {
  Plan  plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), countLayer(2, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  terrno = TSDB_CODE_SUCCESS;
  SubmitResult first = submitData(chain.get(), {100, 0, true, 0, 0}, 10, &blocks);
  EXPECT_EQ(TSDB_CODE_SUCCESS, terrno);
  EXPECT_EQ(0, taosArrayGetSize(first.value().pAcceptedBatches));
  EXPECT_EQ(0, taosArrayGetSize(first.value().pCandidates));
}

TEST(StreamWindowChainDataTest, StatePartialNullResolutionMatchesLegacy) {
  {
    Plan  plan({sessionLayer(10000), twoColumnStateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
    SubmitResult deferred = submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(first.value().pCandidates));
    EXPECT_EQ(0, taosArrayGetSize(deferred.value().pCandidates));
    SubmitResult committed = submitData(chain.get(), {300, 1, false, 0, 0, 10, false}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(committed.value().pCandidates));
    expectWindowEvent(candidateAt(committed, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 3);
  }

  struct DualSideCase {
    int16_t extend;
    TSKEY   oldEnd;
    int64_t oldRows;
  };
  const DualSideCase dualSideCases[] = {
      {STATE_WIN_EXTEND_OPTION_DEFAULT, 200, 2},
      {STATE_WIN_EXTEND_OPTION_BACKWARD, 299, 2},
      {STATE_WIN_EXTEND_OPTION_FORWARD, 100, 1},
  };
  for (const DualSideCase& stateCase : dualSideCases) {
    Plan  plan({sessionLayer(10000), twoColumnStateLayer(stateCase.extend)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 0, 0, 0, true}, 10, &blocks);
    submitData(chain.get(), {200, 0, true, 0, 0, 10, false}, 20, &blocks);
    SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 0, true}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(cut.value().pCandidates));
    expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, stateCase.oldEnd, stateCase.oldRows);
    if (stateCase.extend == STATE_WIN_EXTEND_OPTION_FORWARD) {
      SubmitResult nextCut = submitData(chain.get(), {400, 3, false, 0, 0, 10, false}, 40, &blocks);
      ASSERT_EQ(1, taosArrayGetSize(nextCut.value().pCandidates));
      expectWindowEvent(candidateAt(nextCut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 101, 300, 2);
    }
  }

  {
    Plan  plan({sessionLayer(10000), twoColumnStateLayer(STATE_WIN_EXTEND_OPTION_FORWARD)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
    submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
    SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 10, false}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
    expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 100, 1);
    expectWindowEvent(candidateAt(cut, 1), STRIGGER_EVENT_WINDOW_CLOSE, 101, 200, 1);
    SubmitResult nextCut = submitData(chain.get(), {400, 3, false, 0, 0, 10, false}, 40, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(nextCut.value().pCandidates));
    expectWindowEvent(candidateAt(nextCut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 201, 300, 1);
  }

  struct AllNullCase {
    int16_t extend;
    TSKEY   oldEnd;
    int64_t oldRows;
    TSKEY   newStart;
    int64_t newRows;
  };
  const AllNullCase allNullCases[] = {
      {STATE_WIN_EXTEND_OPTION_DEFAULT, 100, 1, 300, 1},
      {STATE_WIN_EXTEND_OPTION_BACKWARD, 299, 2, 300, 1},
      {STATE_WIN_EXTEND_OPTION_FORWARD, 100, 1, 101, 2},
  };
  for (const AllNullCase& stateCase : allNullCases) {
    Plan  plan({sessionLayer(10000), twoColumnStateLayer(stateCase.extend)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 7, false, 0, 0, 70, false}, 10, &blocks);
    submitData(chain.get(), {200, 0, true, 0, 0, 0, true}, 20, &blocks);
    SubmitResult cut = submitData(chain.get(), {300, 8, false, 0, 0, 80, false}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
    expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, stateCase.oldEnd, stateCase.oldRows);
    expectWindowEvent(candidateAt(cut, 1), STRIGGER_EVENT_WINDOW_OPEN, stateCase.newStart, stateCase.newStart,
                      stateCase.newRows);
  }
}

TEST(StreamWindowChainDataTest, StateDeferredPartialNullCutEmitsOpenBeforeClose) {
  Plan  plan({sessionLayer(10000), twoColumnStateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
  submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
  SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 10, false}, 30, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
  expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 2);
  expectWindowEvent(candidateAt(cut, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 2);
}

TEST(StreamWindowChainDataTest, StateForwardStandaloneEmitsOpenAndClose) {
  Plan  plan({sessionLayer(10000), twoColumnStateLayer(STATE_WIN_EXTEND_OPTION_FORWARD)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
  submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
  SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 10, false}, 30, &blocks);
  ASSERT_EQ(4, taosArrayGetSize(cut.value().pCandidates));
  expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 100, 1);
  expectWindowEvent(candidateAt(cut, 1), STRIGGER_EVENT_WINDOW_OPEN, 101, 101, 1);
  expectWindowEvent(candidateAt(cut, 2), STRIGGER_EVENT_WINDOW_CLOSE, 101, 200, 1);
  expectWindowEvent(candidateAt(cut, 3), STRIGGER_EVENT_WINDOW_OPEN, 201, 201, 1);
}

TEST(StreamWindowChainDataTest, StateZerothDoesNotSuppressForwardStandalone) {
  Plan  plan({sessionLayer(10000), twoColumnStateLayerWithZeroth(STATE_WIN_EXTEND_OPTION_FORWARD, {1, 10})});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
  submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
  SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 10, false}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(cut.value().pCandidates));
  expectWindowEvent(candidateAt(cut, 0), STRIGGER_EVENT_WINDOW_CLOSE, 101, 200, 1);
}

TEST(StreamWindowChainDataTest, StateZerothUsesFinalCommittedState) {
  Plan  plan({sessionLayer(10000), twoColumnStateLayerWithZeroth(STATE_WIN_EXTEND_OPTION_DEFAULT, {1, 10})});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 0, true, 0, 0, 10, false}, 10, &blocks);
  submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
  submitData(chain.get(), {300, 1, false, 0, 0, 10, false}, 30, &blocks);
  SubmitResult suppressed = submitData(chain.get(), {400, 2, false, 0, 0, 10, false}, 40, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(suppressed.value().pCandidates));
  SubmitResult closed = submitData(chain.get(), {500, 3, false, 0, 0, 10, false}, 50, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 400, 400, 1);
}

TEST(StreamWindowChainStateNotifyTest, FirstOpenFreezesNullPreviousStateAtInstanceCreation) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(first, 0);
  ASSERT_NE(nullptr, open->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":null,\"curState\":[1]}", open->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, TransitionFreezesCloseBeforeBoundaryOverwritesCommittedState) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
  SubmitResult cut = submitData(chain.get(), {200, 2, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
  ASSERT_NE(nullptr, candidateAt(cut, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"curState\":[1],\"nextState\":[2]}", candidateAt(cut, 0)->leafParam.extraNotifyContent);
  ASSERT_NE(nullptr, candidateAt(cut, 1)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":[1],\"curState\":[2]}", candidateAt(cut, 1)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, TrueForDelayedOpenKeepsSnapshotFromNewInstanceCreation) {
  Plan  plan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks).value().pCandidates));
  SubmitResult firstOpen = submitData(chain.get(), {150, 1, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstOpen.value().pCandidates));
  EXPECT_EQ(1, taosArrayGetSize(submitData(chain.get(), {200, 2, false, 0, 0}, 30, &blocks).value().pCandidates));
  SubmitResult delayed = submitData(chain.get(), {250, 2, false, 0, 0}, 40, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(delayed.value().pCandidates));
  ASSERT_NE(nullptr, candidateAt(delayed, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":[1],\"curState\":[2]}", candidateAt(delayed, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, PartialNullOpenUsesPreCutBitmapAcrossAllExtendModes) {
  const int16_t extendModes[] = {
      STATE_WIN_EXTEND_OPTION_DEFAULT,
      STATE_WIN_EXTEND_OPTION_BACKWARD,
      STATE_WIN_EXTEND_OPTION_FORWARD,
  };
  for (int16_t extend : extendModes) {
    SCOPED_TRACE(extend);
    Plan  plan({sessionLayer(10000), twoColumnStateLayer(extend)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 0, 0, 0, true}, 10, &blocks);
    submitData(chain.get(), {200, 0, true, 0, 0, 10, false}, 20, &blocks);
    SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 0, true}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
    ASSERT_NE(nullptr, candidateAt(cut, 0)->leafParam.extraNotifyContent);
    EXPECT_STREQ("{\"curState\":[1,10],\"nextState\":[2,null]}", candidateAt(cut, 0)->leafParam.extraNotifyContent);
    ASSERT_NE(nullptr, candidateAt(cut, 1)->leafParam.extraNotifyContent);
    EXPECT_STREQ("{\"prevState\":[1,null],\"curState\":[2,null]}", candidateAt(cut, 1)->leafParam.extraNotifyContent);
  }
}

TEST(StreamWindowChainStateNotifyTest, SuppressedZerothStillFeedsNextRegularOpenPreviousState) {
  Plan  plan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 0, true)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_OPEN);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult suppressed = submitData(chain.get(), {100, 0, false, 0, 0}, 10, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(suppressed.value().pCandidates));
  SubmitResult opened = submitData(chain.get(), {200, 1, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  ASSERT_NE(nullptr, candidateAt(opened, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":[0],\"curState\":[1]}", candidateAt(opened, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, ForwardStandaloneCandidatesDoNotInheritRegularStateContent) {
  Plan  plan({sessionLayer(10000), twoColumnStateLayer(STATE_WIN_EXTEND_OPTION_FORWARD)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 1, false, 0, 0, 10, false}, 10, &blocks);
  submitData(chain.get(), {200, 1, false, 0, 0, 0, true}, 20, &blocks);
  SubmitResult cut = submitData(chain.get(), {300, 2, false, 0, 0, 10, false}, 30, &blocks);
  ASSERT_EQ(4, taosArrayGetSize(cut.value().pCandidates));
  ASSERT_NE(nullptr, candidateAt(cut, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"curState\":[1,10],\"nextState\":[2,10]}", candidateAt(cut, 0)->leafParam.extraNotifyContent);
  EXPECT_EQ(nullptr, candidateAt(cut, 1)->leafParam.extraNotifyContent);
  EXPECT_EQ(nullptr, candidateAt(cut, 2)->leafParam.extraNotifyContent);
  ASSERT_NE(nullptr, candidateAt(cut, 3)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":[1,10],\"curState\":[2,10]}", candidateAt(cut, 3)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, EofAncestorForcedCloseDoesNotReuseRegularBoundaryContent) {
  Plan  plan({intervalLayer(100, 100), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {50, 1, false, 0, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  CandidateArray forced;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), INT64_MAX - 1, 20, forced.get()));
  ASSERT_EQ(1, forced.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_CLOSE, forced.at(0)->eventType);
  EXPECT_EQ(nullptr, forced.at(0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, MaxDelayCandidateDoesNotCopyFrozenOpenContent) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 100);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 0, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  CandidateArray delayed;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainCollectDelayedCandidates(chain.get(), 100, delayed.get()));
  ASSERT_EQ(1, delayed.size());
  EXPECT_EQ(STRIGGER_EVENT_WINDOW_NONE, delayed.at(0)->eventType);
  EXPECT_EQ(nullptr, delayed.at(0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, CalculationOnlyOpenAndCloseRemainContentFree) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  EXPECT_EQ(nullptr, candidateAt(first, 0)->leafParam.extraNotifyContent);
  SubmitResult cut = submitData(chain.get(), {200, 2, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(cut.value().pCandidates));
  EXPECT_EQ(nullptr, candidateAt(cut, 0)->leafParam.extraNotifyContent);
  EXPECT_EQ(nullptr, candidateAt(cut, 1)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, CandidateStringsOutliveRowsLaterSubmitsAndInstanceRemoval) {
  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> firstBlocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 10, &firstBlocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  const SLeafEventCandidate* firstOpen = candidateAt(first, 0);
  firstBlocks.clear();

  std::vector<Block> boundaryBlocks;
  SubmitResult       boundary = submitData(chain.get(), {200, 2, false, 0, 0}, 20, &boundaryBlocks);
  ASSERT_EQ(2, taosArrayGetSize(boundary.value().pCandidates));
  int32_t replacement = 99;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(boundaryBlocks[0]->pDataBlock, 1)), 0,
                          reinterpret_cast<const char*>(&replacement), false));
  boundaryBlocks.clear();

  CandidateArray frontier;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), INT64_MAX - 1, 30, frontier.get()));
  EXPECT_STREQ("{\"prevState\":null,\"curState\":[1]}", firstOpen->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"curState\":[1],\"nextState\":[2]}", candidateAt(boundary, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":[1],\"curState\":[2]}", candidateAt(boundary, 1)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainStateNotifyTest, InstanceCloneStringFailureLeavesDelayedOpenRetryable) {
  Plan  plan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN, 0, false, STRIGGER_EVENT_WINDOW_OPEN);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
  ASSERT_EQ(0, taosArrayGetSize(first.value().pCandidates));
  blocks.push_back(makeDataBlock({150, 1, false, 0, 0}));
  const SWindowChainRowRef ref = {blocks.back().get(), 0, 100};
  SArray*                  refs = taosArrayInit(1, sizeof(SWindowChainRowRef));
  ASSERT_NE(nullptr, refs);
  ASSERT_NE(nullptr, taosArrayPush(refs, &ref));
  const SWindowChainPeerGroup group = {kGid, 150, refs};

  SubmitResult failed;
  int32_t      failedCode = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    stub.set(taosStrdupi, failWindowChainInstanceStrdupi);
    failedCode = stWindowChainSubmitPeerGroup(chain.get(), &group, 20, failed.get());
  }
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failedCode);
  if (failedCode != TSDB_CODE_OUT_OF_MEMORY) {
    taosArrayDestroy(refs);
    return;
  }
  EXPECT_EQ(nullptr, failed.value().pAcceptedBatches);
  EXPECT_EQ(nullptr, failed.value().pCandidates);

  SubmitResult  retry;
  const int32_t retryCode = stWindowChainSubmitPeerGroup(chain.get(), &group, 20, retry.get());
  taosArrayDestroy(refs);
  ASSERT_EQ(TSDB_CODE_SUCCESS, retryCode);
  ASSERT_EQ(1, taosArrayGetSize(retry.value().pCandidates));
  EXPECT_EQ(2, candidateAt(retry, 0)->rowCount);
  ASSERT_NE(nullptr, candidateAt(retry, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ("{\"prevState\":null,\"curState\":[1]}", candidateAt(retry, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, OrdinaryOpenAndCloseUseLiteralLegacyJsonFromTriggerRows) {
  ConditionColumns startColumns({{1, "startValue"}});
  ConditionColumns endColumns({{4, "endValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(false)});
  Chain            chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult opened = submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  expectWindowEvent(candidateAt(opened, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
  ASSERT_NE(nullptr, candidateAt(opened, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11}},\"windowIndex\":-1}",
      candidateAt(opened, 0)->leafParam.extraNotifyContent);

  SubmitResult closed = submitData(chain.get(), {200, 22, false, 0, 1, 202}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 2, -1);
  ASSERT_NE(nullptr, candidateAt(closed, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"endValue\":202}},\"windowIndex\":-1}",
      candidateAt(closed, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, TrueForUsesSatisfyingRowsAndFirstStreakBoundaries) {
  ConditionColumns startColumns({{1, "startValue"}});
  ConditionColumns endColumns({{4, "endValue"}});
  Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_COUNT_ONLY, 2, 0, TRUE_FOR_COUNT_ONLY, 2, 0)});
  Chain chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks).value().pCandidates));
  SubmitResult opened = submitData(chain.get(), {200, 22, false, 1, 0, 202}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  expectWindowEvent(candidateAt(opened, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
  ASSERT_NE(nullptr, candidateAt(opened, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":22}},\"windowIndex\":-1}",
      candidateAt(opened, 0)->leafParam.extraNotifyContent);

  EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {300, 33, false, 0, 1, 303}, 30, &blocks).value().pCandidates));
  SubmitResult closed = submitData(chain.get(), {400, 44, false, 0, 1, 404}, 40, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 300, 3, -1);
  ASSERT_NE(nullptr, candidateAt(closed, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"endValue\":404}},\"windowIndex\":-1}",
      candidateAt(closed, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, MultiStartFreezesParentAndChildJsonAcrossSwitchAndFinalEnd) {
  static const char* kCanonicalParentId = "723b8b71a1ab2fb1f45d8c92f2ace967";
  ConditionColumns   startColumns({{1, "startValue"}, {4, "identityValue"}});
  ConditionColumns   endColumns({{1, "startValue"}, {4, "identityValue"}});
  Plan               plan({sessionLayer(10000), eventLayer(true)});
  Chain              chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
  ASSERT_NE(nullptr, candidateAt(first, 0)->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11,\"identityValue\":101}},\"windowIndex\":-1}",
      candidateAt(first, 0)->leafParam.extraNotifyContent);
  std::array<char, STREAM_NESTED_TRIGGER_ID_LEN> parentEnvelopeId = {};
  const SLeafInstanceId*                         parentIdentity = &candidateAt(first, 0)->instanceId;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            stBuildNestedTriggerId(parentIdentity->gid, &parentIdentity->lineage, parentIdentity->openingTs,
                                   static_cast<int32_t>(parentIdentity->nativeDiscriminator), parentEnvelopeId.data()));
  EXPECT_STREQ(kCanonicalParentId, parentEnvelopeId.data());
  EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {200, 22, false, 1, 0, 202}, 20, &blocks).value().pCandidates));

  SubmitResult switched = submitData(chain.get(), {300, 33, false, 2, 0, 303}, 30, &blocks);
  ASSERT_EQ(3, taosArrayGetSize(switched.value().pCandidates));
  expectWindowEvent(candidateAt(switched, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 2, 0);
  EXPECT_STREQ(
      "{\"triggerId\":\"8704769108438314885\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11,\"identityValue\":101}},\"windowIndex\":0,"
      "\"parentTriggerId\":\"723b8b71a1ab2fb1f45d8c92f2ace967\"}",
      candidateAt(switched, 0)->leafParam.extraNotifyContent);
  expectWindowEvent(candidateAt(switched, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 2, 0);
  EXPECT_STREQ(
      "{\"triggerId\":\"8704769108438314885\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":33,\"identityValue\":303}},\"windowIndex\":0,"
      "\"parentTriggerId\":\"723b8b71a1ab2fb1f45d8c92f2ace967\"}",
      candidateAt(switched, 1)->leafParam.extraNotifyContent);
  expectWindowEvent(candidateAt(switched, 2), STRIGGER_EVENT_WINDOW_OPEN, 300, 300, 1, 1);
  EXPECT_STREQ(
      "{\"triggerId\":\"1714625911429981612\",\"triggerCondition\":{\"conditionIndex\":1,"
      "\"fieldValues\":{\"startValue\":33,\"identityValue\":303}},\"windowIndex\":1,"
      "\"parentTriggerId\":\"723b8b71a1ab2fb1f45d8c92f2ace967\"}",
      candidateAt(switched, 2)->leafParam.extraNotifyContent);

  SubmitResult ended = submitData(chain.get(), {400, 44, false, 0, 1, 404}, 40, &blocks);
  ASSERT_EQ(2, taosArrayGetSize(ended.value().pCandidates));
  expectWindowEvent(candidateAt(ended, 0), STRIGGER_EVENT_WINDOW_CLOSE, 300, 400, 2, 1);
  EXPECT_STREQ(
      "{\"triggerId\":\"1714625911429981612\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":44,\"identityValue\":404}},\"windowIndex\":1,"
      "\"parentTriggerId\":\"723b8b71a1ab2fb1f45d8c92f2ace967\"}",
      candidateAt(ended, 0)->leafParam.extraNotifyContent);
  expectWindowEvent(candidateAt(ended, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 400, 4, -1);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":44,\"identityValue\":404}},\"windowIndex\":-1}",
      candidateAt(ended, 1)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, EqualEventTimestampsUseDistinctAncestorLineageParentIds) {
  auto parentIdForAncestorStart = [](TSKEY ancestorStart) {
    ConditionColumns startColumns({{1, "startValue"}, {4, "identityValue"}});
    ConditionColumns endColumns({{1, "startValue"}, {4, "identityValue"}});
    Plan             plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), eventLayer(true)});
    Chain            chain =
        createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                    STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
    EXPECT_NE(nullptr, chain);
    std::vector<Block> blocks;
    EXPECT_EQ(
        0, taosArrayGetSize(submitData(chain.get(), {ancestorStart, 7, false, 0, 0}, 10, &blocks).value().pCandidates));
    EXPECT_EQ(1,
              taosArrayGetSize(submitData(chain.get(), {100, 7, false, 1, 0, 101}, 20, &blocks).value().pCandidates));
    SubmitResult switched = submitData(chain.get(), {200, 7, false, 2, 0, 202}, 30, &blocks);
    EXPECT_EQ(3, taosArrayGetSize(switched.value().pCandidates));
    const SLeafEventCandidate* child = candidateAt(switched, 0);
    return child == nullptr ? std::string() : jsonStringMember(child->leafParam.extraNotifyContent, "parentTriggerId");
  };

  const std::string first = parentIdForAncestorStart(50);
  const std::string second = parentIdForAncestorStart(60);
  EXPECT_EQ("726f9ff9ddebbdf0b9c34a54098e64b6", first);
  EXPECT_EQ("9c121e5a59e98a5d4887b85bfd4512d4", second);
  EXPECT_NE(first, second);
}

TEST(StreamWindowChainEventNotifyTest, CanonicalParentFreezeFailureRollsBackAndExactRetry) {
  ConditionColumns startColumns({{1, "startValue"}, {4, "identityValue"}});
  ConditionColumns endColumns({{1, "startValue"}, {4, "identityValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(true)});
  Chain            chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;
  ASSERT_EQ(1, taosArrayGetSize(submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks).value().pCandidates));
  DataPeerGroup switchRow({300, 33, false, 2, 0, 303});

  SubmitResult failed;
  int32_t      failedCode = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    gJsonPrintStub = &stub;
    stub.set(cJSON_PrintUnformatted, failCanonicalParentJsonPrint);
    failedCode = stWindowChainSubmitPeerGroup(chain.get(), switchRow.get(), 30, failed.get());
  }
  gJsonPrintStub = nullptr;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failedCode);
  if (failedCode != TSDB_CODE_OUT_OF_MEMORY) return;
  EXPECT_EQ(nullptr, failed.value().pAcceptedBatches);
  EXPECT_EQ(nullptr, failed.value().pCandidates);

  SubmitResult retry;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain.get(), switchRow.get(), 30, retry.get()));
  ASSERT_EQ(3, taosArrayGetSize(retry.value().pCandidates));
  EXPECT_EQ("723b8b71a1ab2fb1f45d8c92f2ace967",
            jsonStringMember(candidateAt(retry, 0)->leafParam.extraNotifyContent, "parentTriggerId"));
  expectWindowEvent(candidateAt(retry, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 100, 1, 0);
  expectWindowEvent(candidateAt(retry, 2), STRIGGER_EVENT_WINDOW_OPEN, 300, 300, 1, 1);
}

TEST(StreamWindowChainEventNotifyTest, CandidateStringsOutliveBlocksLaterSubmitsAndInstanceRemoval) {
  ConditionColumns startColumns({{1, "startValue"}});
  ConditionColumns endColumns({{4, "endValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(false)});
  Chain            chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> firstBlocks;

  SubmitResult opened = submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &firstBlocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  const SLeafEventCandidate* open = candidateAt(opened, 0);
  firstBlocks.clear();

  std::vector<Block> laterBlocks;
  EXPECT_EQ(
      0, taosArrayGetSize(submitData(chain.get(), {150, 77, false, 0, 0, 707}, 20, &laterBlocks).value().pCandidates));
  SubmitResult closed = submitData(chain.get(), {200, 22, false, 0, 1, 202}, 30, &laterBlocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  const SLeafEventCandidate* close = candidateAt(closed, 0);
  laterBlocks.clear();
  std::vector<Block> afterBlocks;
  EXPECT_EQ(
      0, taosArrayGetSize(submitData(chain.get(), {300, 99, false, 0, 0, 909}, 40, &afterBlocks).value().pCandidates));

  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11}},\"windowIndex\":-1}",
      open->leafParam.extraNotifyContent);
  EXPECT_STREQ(
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"endValue\":202}},\"windowIndex\":-1}",
      close->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, CalculationOnlyOpenAndCloseRemainContentFree) {
  ConditionColumns startColumns({{1, "startValue"}});
  ConditionColumns endColumns({{4, "endValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(false)});
  Chain            chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                                       STRIGGER_EVENT_WINDOW_NONE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult opened = submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
  EXPECT_EQ(nullptr, candidateAt(opened, 0)->leafParam.extraNotifyContent);
  SubmitResult closed = submitData(chain.get(), {200, 22, false, 0, 1, 202}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  EXPECT_EQ(nullptr, candidateAt(closed, 0)->leafParam.extraNotifyContent);
}

TEST(StreamWindowChainEventNotifyTest, InstanceOpenStringCloneFailureRollsBackSwitchAndRetry) {
  static const char* kFirstChildOpen =
      "{\"triggerId\":\"8704769108438314885\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11,\"identityValue\":101}},\"windowIndex\":0,"
      "\"parentTriggerId\":\"723b8b71a1ab2fb1f45d8c92f2ace967\"}";
  ConditionColumns startColumns({{1, "startValue"}, {4, "identityValue"}});
  ConditionColumns endColumns({{1, "startValue"}, {4, "identityValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(true)});
  Chain            chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;
  SubmitResult       first = submitData(chain.get(), {100, 11, false, 1, 0, 101}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  DataPeerGroup switchRow({300, 33, false, 2, 0, 303});

  SubmitResult failed;
  int32_t      failedCode = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    gStrdupiStub = &stub;
    gStrdupiFailureValue = kFirstChildOpen;
    gStrdupiFailureMatches = 0;
    stub.set(taosStrdupi, failSelectedWindowChainStrdupi);
    failedCode = stWindowChainSubmitPeerGroup(chain.get(), switchRow.get(), 30, failed.get());
  }
  gStrdupiStub = nullptr;
  gStrdupiFailureValue = nullptr;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failedCode);
  EXPECT_EQ(1, gStrdupiFailureMatches);
  EXPECT_EQ(nullptr, failed.value().pAcceptedBatches);
  EXPECT_EQ(nullptr, failed.value().pCandidates);

  SubmitResult retry;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain.get(), switchRow.get(), 30, retry.get()));
  ASSERT_EQ(3, taosArrayGetSize(retry.value().pCandidates));
  EXPECT_STREQ(kFirstChildOpen, candidateAt(retry, 0)->leafParam.extraNotifyContent);
  expectWindowEvent(candidateAt(retry, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 100, 1, 0);
  expectWindowEvent(candidateAt(retry, 2), STRIGGER_EVENT_WINDOW_OPEN, 300, 300, 1, 1);
}

TEST(StreamWindowChainEventNotifyTest, CandidateStringDupFailureRollsBackWorkingResultAndRetry) {
  static const char* kOpen =
      "{\"triggerId\":\"8022740039904917180\",\"triggerCondition\":{\"conditionIndex\":0,"
      "\"fieldValues\":{\"startValue\":11}},\"windowIndex\":-1}";
  ConditionColumns startColumns({{1, "startValue"}});
  ConditionColumns endColumns({{4, "endValue"}});
  Plan             plan({sessionLayer(10000), eventLayer(false)});
  Chain            chain =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                  STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, startColumns.get(), endColumns.get());
  ASSERT_NE(nullptr, chain);
  DataPeerGroup openRow({100, 11, false, 1, 0, 101});

  SubmitResult failed;
  int32_t      failedCode = TSDB_CODE_SUCCESS;
  {
    Stub stub;
    gStrdupiStub = &stub;
    gStrdupiFailureValue = kOpen;
    gStrdupiFailureMatches = 0;
    stub.set(taosStrdupi, failSelectedWindowChainStrdupi);
    failedCode = stWindowChainSubmitPeerGroup(chain.get(), openRow.get(), 10, failed.get());
  }
  gStrdupiStub = nullptr;
  gStrdupiFailureValue = nullptr;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, failedCode);
  EXPECT_EQ(1, gStrdupiFailureMatches);
  EXPECT_EQ(nullptr, failed.value().pAcceptedBatches);
  EXPECT_EQ(nullptr, failed.value().pCandidates);

  SubmitResult retry;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainSubmitPeerGroup(chain.get(), openRow.get(), 10, retry.get()));
  ASSERT_EQ(1, taosArrayGetSize(retry.value().pCandidates));
  EXPECT_STREQ(kOpen, candidateAt(retry, 0)->leafParam.extraNotifyContent);
  std::vector<Block> blocks;
  SubmitResult       closed = submitData(chain.get(), {200, 22, false, 0, 1, 202}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
  expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 2, -1);
}

TEST(StreamWindowChainDataTest, EventStartEndStreaksMatchLegacy) {
  {
    Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_COUNT_ONLY, 2, 0, TRUE_FOR_COUNT_ONLY, 0, 0)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks).value().pCandidates));
    EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {200, 2, false, 0, 0}, 20, &blocks).value().pCandidates));
    EXPECT_EQ(0, taosArrayGetSize(submitData(chain.get(), {300, 3, false, 1, 0}, 30, &blocks).value().pCandidates));
    SubmitResult opened = submitData(chain.get(), {400, 4, false, 1, 0}, 40, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
    expectWindowEvent(candidateAt(opened, 0), STRIGGER_EVENT_WINDOW_OPEN, 300, 300, 1, -1);
    SubmitResult closed = submitData(chain.get(), {500, 5, false, 0, 1}, 50, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 300, 500, 2, -1);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_DURATION_ONLY, 0, 100, TRUE_FOR_COUNT_ONLY, 0, 0)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    submitData(chain.get(), {150, 2, false, 0, 0}, 20, &blocks);
    submitData(chain.get(), {200, 3, false, 1, 0}, 30, &blocks);
    submitData(chain.get(), {250, 4, false, 1, 0}, 40, &blocks);
    SubmitResult opened = submitData(chain.get(), {300, 5, false, 1, 0}, 50, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(opened.value().pCandidates));
    expectWindowEvent(candidateAt(opened, 0), STRIGGER_EVENT_WINDOW_OPEN, 200, 200, 1, -1);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_COUNT_ONLY, 0, 0, TRUE_FOR_COUNT_ONLY, 2, 0)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    submitData(chain.get(), {200, 2, false, 0, 1}, 20, &blocks);
    submitData(chain.get(), {300, 3, false, 0, 0}, 30, &blocks);
    submitData(chain.get(), {400, 4, false, 0, 1}, 40, &blocks);
    SubmitResult closed = submitData(chain.get(), {500, 5, false, 0, 1}, 50, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 400, 5, -1);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_COUNT_ONLY, 0, 0, TRUE_FOR_DURATION_ONLY, 0, 100)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    submitData(chain.get(), {200, 2, false, 0, 1}, 20, &blocks);
    submitData(chain.get(), {250, 3, false, 0, 1}, 30, &blocks);
    SubmitResult closed = submitData(chain.get(), {300, 4, false, 0, 1}, 40, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 4, -1);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayerWithStreaks(TRUE_FOR_COUNT_ONLY, 2, 0, TRUE_FOR_COUNT_ONLY, 0, 0)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    SubmitResult both = submitData(chain.get(), {200, 2, false, 1, 1}, 20, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(both.value().pCandidates));
    expectWindowEvent(candidateAt(both, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
    expectWindowEvent(candidateAt(both, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 1, -1);
  }
}

TEST(StreamWindowChainDataTest, EventFirstSubeventOpenIsDelayedUntilIdentityIsKnown) {
  {
    Plan  plan({sessionLayer(10000), eventLayer(true)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult first = submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
    expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
    SubmitResult same = submitData(chain.get(), {150, 2, false, 1, 0}, 20, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(same.value().pCandidates));
    SubmitResult closed = submitData(chain.get(), {200, 3, false, 0, 1}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 200, 3, -1);
  }
  {
    Plan  plan({sessionLayer(10000), eventLayer(true)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    SubmitResult first = submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
    expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 1, -1);
    SubmitResult same = submitData(chain.get(), {150, 2, false, 1, 0}, 20, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(same.value().pCandidates));
    SubmitResult switched = submitData(chain.get(), {200, 3, false, 2, 0}, 30, &blocks);
    ASSERT_EQ(3, taosArrayGetSize(switched.value().pCandidates));
    expectWindowEvent(candidateAt(switched, 0), STRIGGER_EVENT_WINDOW_OPEN, 100, 100, 2, 0);
    expectWindowEvent(candidateAt(switched, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 150, 2, 0);
    expectWindowEvent(candidateAt(switched, 2), STRIGGER_EVENT_WINDOW_OPEN, 200, 200, 1, 1);
    SubmitResult sameSecond = submitData(chain.get(), {250, 4, false, 2, 0}, 40, &blocks);
    EXPECT_EQ(0, taosArrayGetSize(sameSecond.value().pCandidates));
    SubmitResult closed = submitData(chain.get(), {300, 5, false, 0, 1}, 50, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(closed.value().pCandidates));
    expectWindowEvent(candidateAt(closed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 200, 300, 3, 1);
    expectWindowEvent(candidateAt(closed, 1), STRIGGER_EVENT_WINDOW_CLOSE, 100, 300, 5, -1);
  }
}

TEST(StreamWindowChainDataTest, eventEndRowRoutesOldChildAndUnopenedEventStopsRouting) {
  Plan  plan({eventLayer(false), countLayer(2, 1)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult unopened = submitData(chain.get(), {500, 0, false, 0, 0}, 5, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(unopened.value().pAcceptedBatches));
  submitData(chain.get(), {1000, 1, false, 1, 0}, 10, &blocks);
  SubmitResult ended = submitData(chain.get(), {2000, 2, false, 0, 1}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(ended.value().pCandidates));
  expectWindowEvent(candidateAt(ended, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 2);
  EXPECT_EQ(1000, scopeAt(candidateAt(ended, 0)->lineage, 0)->openingTs);
  SubmitResult after = submitData(chain.get(), {3000, 3, false, 0, 0}, 30, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(after.value().pAcceptedBatches));
}

TEST(StreamWindowChainDataTest, stateNullExtendAndZerothFollowFrozenLegacySnapshots) {
  struct StateCase {
    int16_t extend;
    TSKEY   oldEnd;
    int64_t oldRows;
    TSKEY   newStart;
    int64_t newRows;
  };
  const StateCase cases[] = {
      {STATE_WIN_EXTEND_OPTION_DEFAULT, 1000, 1, 3000, 1},
      {STATE_WIN_EXTEND_OPTION_BACKWARD, 2999, 2, 3000, 1},
      {STATE_WIN_EXTEND_OPTION_FORWARD, 1000, 1, 1001, 2},
  };
  for (const StateCase& stateCase : cases) {
    Plan  plan({sessionLayer(10000), stateLayer(stateCase.extend)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {1000, 7, false, 0, 0}, 10, &blocks);
    submitData(chain.get(), {2000, 0, true, 0, 0}, 20, &blocks);
    SubmitResult changed = submitData(chain.get(), {3000, 8, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(changed.value().pCandidates));
    expectWindowEvent(candidateAt(changed, 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, stateCase.oldEnd, stateCase.oldRows);
    expectWindowEvent(candidateAt(changed, 1), STRIGGER_EVENT_WINDOW_OPEN, stateCase.newStart, stateCase.newStart,
                      stateCase.newRows);
  }

  Plan  zerothPlan({sessionLayer(10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 0, true)});
  Chain zerothChain = createChain(zerothPlan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE);
  ASSERT_NE(nullptr, zerothChain);
  std::vector<Block> blocks;
  SubmitResult       ignored = submitData(zerothChain.get(), {1000, 0, false, 0, 0}, 10, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(ignored.value().pCandidates));
  SubmitResult changed = submitData(zerothChain.get(), {2000, 1, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(changed.value().pCandidates));
  expectWindowEvent(candidateAt(changed, 0), STRIGGER_EVENT_WINDOW_OPEN, 2000, 2000, 1);
}

TEST(StreamWindowChainOuterCloseTest, flushNeedsLeafCloseEventAndDoesNotRepeatCompletedLeaf) {
  auto run = [](int64_t eventTypes, bool flush) {
    Plan  plan({intervalLayer(10000, 10000), countLayer(2, 1)});
    Chain chain = createChain(plan, eventTypes, 0, flush);
    EXPECT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {8000, 1, false, 0, 0}, 10, &blocks);
    SubmitResult completed = submitData(chain.get(), {9000, 2, false, 0, 0}, 20, &blocks);
    EXPECT_EQ(1, taosArrayGetSize(completed.value().pCandidates));
    return submitData(chain.get(), {10000, 3, false, 0, 0}, 30, &blocks);
  };

  SubmitResult openOnly = run(STRIGGER_EVENT_WINDOW_OPEN, true);
  int32_t      oldLineageEvents = 0;
  for (int32_t i = 0; i < taosArrayGetSize(openOnly.value().pCandidates); ++i) {
    if (scopeAt(candidateAt(openOnly, i)->lineage, 0)->openingTs == 0) ++oldLineageEvents;
  }
  EXPECT_EQ(0, oldLineageEvents);
  SubmitResult defaultDiscard = run(STRIGGER_EVENT_WINDOW_CLOSE, false);
  EXPECT_EQ(0, taosArrayGetSize(defaultDiscard.value().pCandidates));
  SubmitResult closeEnabled = run(STRIGGER_EVENT_WINDOW_CLOSE, true);
  ASSERT_EQ(1, taosArrayGetSize(closeEnabled.value().pCandidates));
  const SLeafEventCandidate* forced = candidateAt(closeEnabled, 0);
  expectWindowEvent(forced, STRIGGER_EVENT_WINDOW_CLOSE, 9000, 9000, 1);
  EXPECT_EQ(0, scopeAt(forced->lineage, 0)->openingTs);
  ASSERT_EQ(1, taosArrayGetSize(forced->pAncestorSnapshots));
  EXPECT_EQ(0, snapshot(*forced, 0)->values.window.start);
  EXPECT_EQ(9999, snapshot(*forced, 0)->values.window.end);
}

TEST(StreamWindowChainOuterCloseTest, forcedCloseStillAppliesLeafTrueFor) {
  Plan  plan({intervalLayer(10000, 10000), stateLayer(STATE_WIN_EXTEND_OPTION_DEFAULT, 2)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;
  submitData(chain.get(), {9000, 7, false, 0, 0}, 10, &blocks);
  SubmitResult rejected = submitData(chain.get(), {10000, 8, false, 0, 0}, 20, &blocks);
  EXPECT_EQ(0, taosArrayGetSize(rejected.value().pCandidates));
}

TEST(StreamWindowChainOuterCloseTest, EventForcedCloseFreezesMultiChildAndSingleEnvelopeIdentityMetadata) {
  static const char* kCanonicalParentId = "ea37da83b7d9ec826782840b009f4efa";
  auto               expectIdentityMetadata = [](const SLeafEventCandidate* candidate, int32_t windowIndex,
                                   const char* pParentTriggerId) {
    ASSERT_NE(nullptr, candidate);
    ASSERT_NE(nullptr, candidate->leafParam.extraNotifyContent);
    std::unique_ptr<cJSON, decltype(&cJSON_Delete)> object(cJSON_Parse(candidate->leafParam.extraNotifyContent),
                                                                         cJSON_Delete);
    ASSERT_NE(nullptr, object);
    const cJSON* pWindowIndex = cJSON_GetObjectItemCaseSensitive(object.get(), "windowIndex");
    ASSERT_TRUE(cJSON_IsNumber(pWindowIndex));
    EXPECT_EQ(windowIndex, static_cast<int32_t>(cJSON_GetNumberValue(pWindowIndex)));
    const cJSON* pParent = cJSON_GetObjectItemCaseSensitive(object.get(), "parentTriggerId");
    if (pParentTriggerId == nullptr) {
      EXPECT_EQ(nullptr, pParent);
    } else {
      ASSERT_TRUE(cJSON_IsString(pParent));
      EXPECT_STREQ(pParentTriggerId, cJSON_GetStringValue(pParent));
    }
  };

  {
    Plan  plan({intervalLayer(100, 100), eventLayer(true)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    EXPECT_EQ(1, taosArrayGetSize(submitData(chain.get(), {50, 1, false, 1, 0}, 10, &blocks).value().pCandidates));
    EXPECT_EQ(3, taosArrayGetSize(submitData(chain.get(), {60, 2, false, 2, 0}, 20, &blocks).value().pCandidates));
    SubmitResult forced = submitData(chain.get(), {100, 3, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(2, taosArrayGetSize(forced.value().pCandidates));
    expectWindowEvent(candidateAt(forced, 0), STRIGGER_EVENT_WINDOW_CLOSE, 60, 60, 1, 1);
    expectIdentityMetadata(candidateAt(forced, 0), 1, kCanonicalParentId);
    expectWindowEvent(candidateAt(forced, 1), STRIGGER_EVENT_WINDOW_CLOSE, 50, 60, 2, -1);
    expectIdentityMetadata(candidateAt(forced, 1), -1, nullptr);
  }

  {
    Plan  plan({intervalLayer(100, 100), eventLayer(true)});
    Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;

    EXPECT_EQ(1, taosArrayGetSize(submitData(chain.get(), {50, 1, false, 1, 0}, 10, &blocks).value().pCandidates));
    SubmitResult forced = submitData(chain.get(), {100, 2, false, 0, 0}, 20, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(forced.value().pCandidates));
    expectWindowEvent(candidateAt(forced, 0), STRIGGER_EVENT_WINDOW_CLOSE, 50, 50, 1, -1);
    expectIdentityMetadata(candidateAt(forced, 0), -1, nullptr);
  }
}

TEST(StreamWindowChainOuterCloseTest, eventSingleSubeventKeepsParentIdentityAcrossReset) {
  Plan  plan({intervalLayer(100, 100), eventLayer(true)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  SubmitResult first = submitData(chain.get(), {50, 1, false, 1, 0}, 10, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(first.value().pCandidates));
  expectWindowEvent(candidateAt(first, 0), STRIGGER_EVENT_WINDOW_OPEN, 50, 50, 1, -1);
  SubmitResult firstClose = submitData(chain.get(), {100, 2, false, 0, 0}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstClose.value().pCandidates));
  expectWindowEvent(candidateAt(firstClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 50, 50, 1, -1);

  SubmitResult second = submitData(chain.get(), {150, 3, false, 1, 0}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(second.value().pCandidates));
  expectWindowEvent(candidateAt(second, 0), STRIGGER_EVENT_WINDOW_OPEN, 150, 150, 1, -1);
  SubmitResult secondClose = submitData(chain.get(), {200, 4, false, 0, 0}, 40, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(secondClose.value().pCandidates));
  expectWindowEvent(candidateAt(secondClose, 0), STRIGGER_EVENT_WINDOW_CLOSE, 150, 150, 1, -1);
}

TEST(StreamWindowChainDataTest, activeDataParentAdvancesTimeLeafButGapAndUnopenedStopFrontier) {
  {
    Plan  plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), intervalLayer(1000, 1000)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
    CandidateArray events;
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 20, events.get()));
    ASSERT_EQ(1, events.size());
    expectWindowEvent(events.at(0), STRIGGER_EVENT_WINDOW_CLOSE, 0, 999, 1);
    EXPECT_EQ(1, events.at(0)->rowCount);
    EXPECT_EQ(events.at(0)->rowCount, events.at(0)->leafParam.wrownum);
    EXPECT_EQ(100, events.at(0)->calcDataRange.skey);
    EXPECT_EQ(100, events.at(0)->calcDataRange.ekey);
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 2000, 25, events.get()));
    ASSERT_EQ(2, events.size());
    expectWindowEvent(events.at(1), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 1999, 0);
    EXPECT_EQ(0, events.at(1)->rowCount);
    EXPECT_EQ(events.at(1)->rowCount, events.at(1)->leafParam.wrownum);
    EXPECT_EQ(1000, events.at(1)->calcDataRange.skey);
    EXPECT_EQ(1999, events.at(1)->calcDataRange.ekey);
    SubmitResult afterFrontier = submitData(chain.get(), {1100, 1, false, 0, 0}, 30, &blocks);
    ASSERT_EQ(1, taosArrayGetSize(afterFrontier.value().pAcceptedBatches));
    EXPECT_EQ(100, scopeAt(acceptedBatch(afterFrontier, 0)->cacheScope.lineage, 0)->openingTs);
  }
  {
    Plan  plan({countLayer(2, 4), intervalLayer(1000, 1000)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
    CandidateArray events;
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 20, events.get()));
    ASSERT_EQ(1, events.size());
    expectWindowEvent(events.at(0), STRIGGER_EVENT_WINDOW_CLOSE, 0, 999, 1);
  }
  {
    Plan  plan({countLayer(2, 4), intervalLayer(1000, 1000)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
    submitData(chain.get(), {200, 2, false, 0, 0}, 20, &blocks);
    submitData(chain.get(), {300, 3, false, 0, 0}, 30, &blocks);
    CandidateArray events;
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 40, events.get()));
    EXPECT_EQ(0, events.size());
  }
  {
    Plan  plan({eventLayer(false), intervalLayer(1000, 1000)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), {100, 1, false, 1, 0}, 10, &blocks);
    CandidateArray events;
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 20, events.get()));
    ASSERT_EQ(1, events.size());
    expectWindowEvent(events.at(0), STRIGGER_EVENT_WINDOW_CLOSE, 0, 999, 1);
  }
  {
    Plan  plan({eventLayer(false), intervalLayer(1000, 1000)});
    Chain chain = createChain(plan);
    ASSERT_NE(nullptr, chain);
    CandidateArray events;
    ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainAdvanceFrontier(chain.get(), 1000, 20, events.get()));
    EXPECT_EQ(0, events.size());
  }
}

TEST(StreamWindowChainDataTest, DeferredCalcRangeStopsAtClosedDataParentEnd) {
  Plan  plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), intervalLayer(10000, 10000)});
  Chain chain = createChain(plan, STRIGGER_EVENT_WINDOW_CLOSE, 0, true);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {0, 1, false, 0, 0}, 10, &blocks);
  submitData(chain.get(), {1, 1, false, 0, 0}, 20, &blocks);
  SubmitResult firstClose = submitData(chain.get(), {2, 2, false, 0, 0}, 30, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(firstClose.value().pCandidates));
  EXPECT_EQ(0, candidateAt(firstClose, 0)->calcDataRange.skey);
  EXPECT_EQ(1, candidateAt(firstClose, 0)->calcDataRange.ekey);

  SubmitResult secondClose = submitData(chain.get(), {3, 1, false, 0, 0}, 40, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(secondClose.value().pCandidates));
  EXPECT_EQ(2, candidateAt(secondClose, 0)->calcDataRange.skey);
  EXPECT_EQ(2, candidateAt(secondClose, 0)->calcDataRange.ekey);

  submitData(chain.get(), {4, 1, false, 0, 0}, 50, &blocks);
  SubmitResult thirdClose = submitData(chain.get(), {5, 3, false, 0, 0}, 60, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(thirdClose.value().pCandidates));
  EXPECT_EQ(3, candidateAt(thirdClose, 0)->calcDataRange.skey);
  EXPECT_EQ(4, candidateAt(thirdClose, 0)->calcDataRange.ekey);
}

TEST(StreamWindowChainDataTest, mixedEightLayerEndRowCascadesUnderFrozenLineage) {
  Plan  plan({stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), countLayer(2, 2), eventLayer(false), sessionLayer(10000),
              stateLayer(STATE_WIN_EXTEND_OPTION_BACKWARD), countLayer(2, 2), eventLayer(false),
              intervalLayer(1000, 1000)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  submitData(chain.get(), {100, 7, false, 1, 0}, 10, &blocks);
  SubmitResult ended = submitData(chain.get(), {999, 7, false, 0, 1}, 20, &blocks);
  ASSERT_EQ(1, taosArrayGetSize(ended.value().pCandidates));
  const SLeafEventCandidate* close = candidateAt(ended, 0);
  expectWindowEvent(close, STRIGGER_EVENT_WINDOW_CLOSE, 0, 999, 2);
  ASSERT_EQ(7, taosArrayGetSize(close->lineage.pScopes));
  for (int32_t i = 0; i < 7; ++i) {
    EXPECT_EQ(100, scopeAt(close->lineage, i)->openingTs);
  }
  ASSERT_EQ(7, taosArrayGetSize(close->pAncestorSnapshots));
  EXPECT_EQ(100, close->calcDataRange.skey);
  EXPECT_EQ(999, close->calcDataRange.ekey);
}

void expectRecoveryResultsEquivalent(const SubmitResult& reference, const SubmitResult& recovered) {
  ASSERT_EQ(taosArrayGetSize(reference.value().pAcceptedBatches), taosArrayGetSize(recovered.value().pAcceptedBatches));
  for (int32_t i = 0; i < taosArrayGetSize(reference.value().pAcceptedBatches); ++i) {
    SCOPED_TRACE(::testing::Message() << "accepted batch " << i);
    const SWindowChainAcceptedBatch* expected = acceptedBatch(reference, i);
    const SWindowChainAcceptedBatch* actual = acceptedBatch(recovered, i);
    ASSERT_NE(nullptr, expected);
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(scopeKey(expected->cacheScope), scopeKey(actual->cacheScope));
    EXPECT_EQ(acceptedValues(reference, i), acceptedValues(recovered, i));
    ASSERT_EQ(taosArrayGetSize(expected->pRows), taosArrayGetSize(actual->pRows));
    for (int32_t rowIndex = 0; rowIndex < taosArrayGetSize(expected->pRows); ++rowIndex) {
      const auto* expectedRow = static_cast<const SWindowChainRowRef*>(taosArrayGet(expected->pRows, rowIndex));
      const auto* actualRow = static_cast<const SWindowChainRowRef*>(taosArrayGet(actual->pRows, rowIndex));
      ASSERT_NE(nullptr, expectedRow);
      ASSERT_NE(nullptr, actualRow);
      EXPECT_EQ(expectedRow->tableUid, actualRow->tableUid);
    }
  }

  ASSERT_EQ(taosArrayGetSize(reference.value().pCandidates), taosArrayGetSize(recovered.value().pCandidates));
  for (int32_t i = 0; i < taosArrayGetSize(reference.value().pCandidates); ++i) {
    SCOPED_TRACE(::testing::Message() << "candidate " << i);
    const SLeafEventCandidate* expected = candidateAt(reference, i);
    const SLeafEventCandidate* actual = candidateAt(recovered, i);
    ASSERT_NE(nullptr, expected);
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(expected->eventType, actual->eventType);
    EXPECT_EQ(expected->rowCount, actual->rowCount);
    EXPECT_EQ(expected->leafParam.wstart, actual->leafParam.wstart);
    EXPECT_EQ(expected->leafParam.wend, actual->leafParam.wend);
    EXPECT_EQ(expected->leafParam.wduration, actual->leafParam.wduration);
    EXPECT_EQ(expected->leafParam.wrownum, actual->leafParam.wrownum);
    EXPECT_EQ(expected->leafParam.triggerTime, actual->leafParam.triggerTime);
    EXPECT_EQ(expected->leafParam.notifyType, actual->leafParam.notifyType);
    EXPECT_EQ(expected->instanceId.gid, actual->instanceId.gid);
    EXPECT_EQ(expected->instanceId.triggerType, actual->instanceId.triggerType);
    EXPECT_EQ(expected->instanceId.openingTs, actual->instanceId.openingTs);
    EXPECT_EQ(expected->instanceId.nativeDiscriminator, actual->instanceId.nativeDiscriminator);
    EXPECT_EQ(lineageKey(expected->instanceId.lineage), lineageKey(actual->instanceId.lineage));
    EXPECT_EQ(lineageKey(expected->lineage), lineageKey(actual->lineage));
    EXPECT_EQ(scopeKey(expected->cacheScope), scopeKey(actual->cacheScope));
    EXPECT_EQ(expected->rootImpactExtent.skey, actual->rootImpactExtent.skey);
    EXPECT_EQ(expected->rootImpactExtent.ekey, actual->rootImpactExtent.ekey);

    ASSERT_EQ(taosArrayGetSize(expected->pAncestorSnapshots), taosArrayGetSize(actual->pAncestorSnapshots));
    for (int32_t snapshotIndex = 0; snapshotIndex < taosArrayGetSize(expected->pAncestorSnapshots); ++snapshotIndex) {
      const SWindowAncestorSnapshot* expectedSnapshot = snapshot(*expected, snapshotIndex);
      const SWindowAncestorSnapshot* actualSnapshot = snapshot(*actual, snapshotIndex);
      ASSERT_NE(nullptr, expectedSnapshot);
      ASSERT_NE(nullptr, actualSnapshot);
      EXPECT_EQ(expectedSnapshot->layerIndex, actualSnapshot->layerIndex);
      EXPECT_EQ(expectedSnapshot->triggerType, actualSnapshot->triggerType);
      EXPECT_EQ(expectedSnapshot->placeholderMask, actualSnapshot->placeholderMask);
      EXPECT_EQ(expectedSnapshot->values.window.start, actualSnapshot->values.window.start);
      EXPECT_EQ(expectedSnapshot->values.window.end, actualSnapshot->values.window.end);
      EXPECT_EQ(expectedSnapshot->values.window.duration, actualSnapshot->values.window.duration);
      EXPECT_EQ(expectedSnapshot->values.window.rownum, actualSnapshot->values.window.rownum);
    }
  }
}

TEST(StreamWindowChainRecoveryTest, RestoresAtEveryPeerBoundary) {
  const std::array<std::vector<DataRow>, 4> peerGroups = {
      std::vector<DataRow>{{1000, 7, false, 0, 0}, {1000, 7, false, 0, 0}},
      std::vector<DataRow>{{2000, 7, false, 0, 0}},
      std::vector<DataRow>{{3000, 8, false, 0, 0}, {3000, 8, false, 0, 0}},
      std::vector<DataRow>{{4000, 8, false, 0, 0}},
  };
  const std::array<std::vector<int32_t>, 4> accepted = {std::vector<int32_t>{7, 7}, std::vector<int32_t>{7},
                                                        std::vector<int32_t>{8, 8}, std::vector<int32_t>{8}};

  Plan  plan({sessionLayer(10000), stateLayer()});
  Chain reference =
      createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false, STRIGGER_EVENT_WINDOW_NONE);
  ASSERT_NE(nullptr, reference);
  std::vector<Block>        referenceBlocks;
  std::vector<SubmitResult> referenceResults;
  referenceResults.reserve(peerGroups.size());
  for (size_t i = 0; i < peerGroups.size(); ++i) {
    SCOPED_TRACE(::testing::Message() << "reference peer " << i);
    referenceResults.push_back(submitDataPeerGroup(reference.get(), peerGroups[i], 10 + i, &referenceBlocks));
    std::vector<int32_t> acceptedAtPeer;
    for (int32_t batch = 0; batch < taosArrayGetSize(referenceResults.back().value().pAcceptedBatches); ++batch) {
      std::vector<int32_t> values = acceptedValues(referenceResults.back(), batch);
      acceptedAtPeer.insert(acceptedAtPeer.end(), values.begin(), values.end());
    }
    EXPECT_EQ(accepted[i], acceptedAtPeer);
  }
  ASSERT_EQ(1, taosArrayGetSize(referenceResults[0].value().pCandidates));
  expectWindowEvent(candidateAt(referenceResults[0], 0), STRIGGER_EVENT_WINDOW_OPEN, 1000, 1000, 2);
  EXPECT_EQ(0, taosArrayGetSize(referenceResults[1].value().pCandidates));
  ASSERT_EQ(2, taosArrayGetSize(referenceResults[2].value().pCandidates));
  expectWindowEvent(candidateAt(referenceResults[2], 0), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2000, 3);
  expectWindowEvent(candidateAt(referenceResults[2], 1), STRIGGER_EVENT_WINDOW_OPEN, 3000, 3000, 2);
  EXPECT_EQ(0, taosArrayGetSize(referenceResults[3].value().pCandidates));

  for (size_t boundary = 0; boundary < peerGroups.size(); ++boundary) {
    SCOPED_TRACE(boundary);
    Chain scratch = createChain(plan, STRIGGER_EVENT_WINDOW_OPEN | STRIGGER_EVENT_WINDOW_CLOSE, 0, false,
                                STRIGGER_EVENT_WINDOW_NONE);
    ASSERT_NE(nullptr, scratch);
    std::vector<Block> blocks;
    for (size_t replayed = 0; replayed < boundary; ++replayed) {
      submitDataPeerGroup(scratch.get(), peerGroups[replayed], 10 + replayed, &blocks);
    }
    for (size_t resumed = boundary; resumed < peerGroups.size(); ++resumed) {
      SCOPED_TRACE(::testing::Message() << "resumed peer " << resumed);
      SubmitResult recovered = submitDataPeerGroup(scratch.get(), peerGroups[resumed], 10 + resumed, &blocks);
      expectRecoveryResultsEquivalent(referenceResults[resumed], recovered);
    }
  }
}

TEST(StreamWindowChainRecoveryTest, RestoresEightLayerAndOverlappingLeaves) {
  const std::array<DataRow, 5>        rows = {{{100, 1, false, 0, 0},
                                               {200, 2, false, 0, 0},
                                               {300, 3, false, 0, 0},
                                               {400, 4, false, 0, 0},
                                               {500, 5, false, 0, 0}}};
  std::vector<SStreamWindowLayerSpec> layers(7, sessionLayer(10000));
  layers.push_back(countLayer(3, 1));
  Plan plan(layers);

  Chain reference = createChain(plan);
  ASSERT_NE(nullptr, reference);
  std::vector<Block>        referenceBlocks;
  std::vector<SubmitResult> referenceResults;
  referenceResults.reserve(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    referenceResults.push_back(submitData(reference.get(), rows[i], 10 * (i + 1), &referenceBlocks));
  }

  Chain scratch = createChain(plan);
  ASSERT_NE(nullptr, scratch);
  std::vector<Block> blocks;
  for (size_t replayed = 0; replayed < 3; ++replayed) {
    submitData(scratch.get(), rows[replayed], 10 * (replayed + 1), &blocks);
  }

  SubmitResult firstAfterReplay = submitData(scratch.get(), rows[3], 40, &blocks);
  expectRecoveryResultsEquivalent(referenceResults[3], firstAfterReplay);
  ASSERT_EQ(1, taosArrayGetSize(firstAfterReplay.value().pCandidates));
  const SLeafEventCandidate* firstClose = candidateAt(firstAfterReplay, 0);
  expectWindowEvent(firstClose, STRIGGER_EVENT_WINDOW_CLOSE, 200, 400, 3);
  EXPECT_EQ(200, firstClose->instanceId.openingTs);
  ASSERT_EQ(7, taosArrayGetSize(firstClose->lineage.pScopes));
  ASSERT_EQ(7, taosArrayGetSize(firstClose->pAncestorSnapshots));
  for (int32_t i = 0; i < 7; ++i) {
    EXPECT_EQ(100, scopeAt(firstClose->lineage, i)->openingTs);
    EXPECT_EQ(4, snapshot(*firstClose, i)->values.window.rownum);
  }

  SubmitResult secondAfterReplay = submitData(scratch.get(), rows[4], 50, &blocks);
  expectRecoveryResultsEquivalent(referenceResults[4], secondAfterReplay);
  ASSERT_EQ(1, taosArrayGetSize(secondAfterReplay.value().pCandidates));
  const SLeafEventCandidate* secondClose = candidateAt(secondAfterReplay, 0);
  expectWindowEvent(secondClose, STRIGGER_EVENT_WINDOW_CLOSE, 300, 500, 3);
  EXPECT_EQ(300, secondClose->instanceId.openingTs);
  ASSERT_EQ(7, taosArrayGetSize(secondClose->lineage.pScopes));
  ASSERT_EQ(7, taosArrayGetSize(secondClose->pAncestorSnapshots));
  for (int32_t i = 0; i < 7; ++i) {
    EXPECT_EQ(100, scopeAt(secondClose->lineage, i)->openingTs);
    EXPECT_EQ(5, snapshot(*secondClose, i)->values.window.rownum);
  }
}

TEST(StreamWindowChainHistoryTailTest, FinalizesLeafWithoutTouchingAncestorState) {
  Plan  plan({sessionLayer(10000), sessionLayer(1000)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;

  EXPECT_FALSE(stWindowChainHasHistoryLeafTail(chain.get()));
  submitData(chain.get(), {100, 1, false, 0, 0}, 10, &blocks);
  EXPECT_TRUE(stWindowChainHasHistoryLeafTail(chain.get()));
  CandidateArray firstTail;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainPrepareHistoryLeafTail(chain.get(), 20, firstTail.get()));
  stWindowChainCommitHistoryLeafTail(chain.get());
  EXPECT_FALSE(stWindowChainHasHistoryLeafTail(chain.get()));
  ASSERT_EQ(1, firstTail.size());
  expectWindowEvent(firstTail.at(0), STRIGGER_EVENT_WINDOW_CLOSE, 100, 100, 1);
  ASSERT_EQ(1, taosArrayGetSize(firstTail.at(0)->pAncestorSnapshots));
  EXPECT_EQ(100, snapshot(*firstTail.at(0), 0)->values.window.start);
  EXPECT_EQ(100, snapshot(*firstTail.at(0), 0)->values.window.end);

  SubmitResult afterTail = submitData(chain.get(), {200, 2, false, 0, 0}, 30, &blocks);
  EXPECT_TRUE(stWindowChainHasHistoryLeafTail(chain.get()));
  ASSERT_EQ(1, taosArrayGetSize(afterTail.value().pAcceptedBatches));
  EXPECT_EQ(100, scopeAt(acceptedBatch(afterTail, 0)->cacheScope.lineage, 0)->openingTs);

  CandidateArray secondTail;
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainPrepareHistoryLeafTail(chain.get(), 40, secondTail.get()));
  stWindowChainCommitHistoryLeafTail(chain.get());
  EXPECT_FALSE(stWindowChainHasHistoryLeafTail(chain.get()));
  ASSERT_EQ(1, secondTail.size());
  expectWindowEvent(secondTail.at(0), STRIGGER_EVENT_WINDOW_CLOSE, 200, 200, 1);
  ASSERT_EQ(1, taosArrayGetSize(secondTail.at(0)->pAncestorSnapshots));
  EXPECT_EQ(100, snapshot(*secondTail.at(0), 0)->values.window.start);
  EXPECT_EQ(200, snapshot(*secondTail.at(0), 0)->values.window.end);
}

TEST(StreamWindowChainHistoryTailTest, AppliesOnlySlidingSessionAndStateLegacyTail) {
  auto tailCount = [](SStreamWindowLayerSpec leaf, const DataRow& row, bool expectedTail,
                      int64_t eventTypes = STRIGGER_EVENT_WINDOW_CLOSE) {
    Plan  plan({sessionLayer(10000), leaf});
    Chain chain = createChain(plan, eventTypes);
    EXPECT_NE(nullptr, chain);
    std::vector<Block> blocks;
    submitData(chain.get(), row, 10, &blocks);
    EXPECT_EQ(expectedTail, stWindowChainHasHistoryLeafTail(chain.get()));
    CandidateArray tail;
    EXPECT_EQ(TSDB_CODE_SUCCESS, stWindowChainPrepareHistoryLeafTail(chain.get(), 20, tail.get()));
    stWindowChainCommitHistoryLeafTail(chain.get());
    EXPECT_FALSE(stWindowChainHasHistoryLeafTail(chain.get()));
    return tail.size();
  };

  EXPECT_EQ(1, tailCount(intervalLayer(1000, 1000), {100, 1, false, 0, 0}, true));
  EXPECT_EQ(1, tailCount(sessionLayer(1000), {100, 1, false, 0, 0}, true));
  EXPECT_EQ(1, tailCount(stateLayer(), {100, 1, false, 0, 0}, true));
  EXPECT_EQ(0, tailCount(countLayer(2, 1), {100, 1, false, 0, 0}, false));
  EXPECT_EQ(0, tailCount(eventLayer(), {100, 1, false, 1, 0}, false));
  EXPECT_EQ(0, tailCount(sessionLayer(1000), {100, 1, false, 0, 0}, false, STRIGGER_EVENT_WINDOW_NONE));
}

TEST(StreamWindowChainHistoryTailTest, AllocationFailureLeavesChainAndOutputUnchanged) {
  Plan  sentinelPlan({sessionLayer(10000), sessionLayer(1000)});
  Chain sentinelChain = createChain(sentinelPlan);
  ASSERT_NE(nullptr, sentinelChain);
  std::vector<Block> sentinelBlocks;
  submitData(sentinelChain.get(), {100, 7, false, 0, 0}, 5, &sentinelBlocks);
  CandidateArray output(1);
  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainPrepareHistoryLeafTail(sentinelChain.get(), 6, output.get()));
  stWindowChainCommitHistoryLeafTail(sentinelChain.get());
  ASSERT_EQ(1, output.size());
  const SArray* sentinelSnapshots = output.at(0)->pAncestorSnapshots;
  const SArray* sentinelLineage = output.at(0)->lineage.pScopes;
  const TSKEY   sentinelStart = output.at(0)->leafParam.wstart;

  Plan  plan({sessionLayer(10000), intervalLayer(2000, 1000)});
  Chain chain = createChain(plan);
  ASSERT_NE(nullptr, chain);
  std::vector<Block> blocks;
  submitData(chain.get(), {1500, 1, false, 0, 0}, 10, &blocks);

  Stub stub;
  gArrayEnsureCapStub = &stub;
  gArrayEnsureCapFailureTarget = output.get();
  stub.set(taosArrayEnsureCap, failSelectedArrayEnsureCap);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stWindowChainPrepareHistoryLeafTail(chain.get(), 20, output.get()));
  stub.reset(taosArrayEnsureCap);
  gArrayEnsureCapStub = nullptr;
  gArrayEnsureCapFailureTarget = nullptr;
  ASSERT_EQ(1, output.size());
  EXPECT_EQ(sentinelSnapshots, output.at(0)->pAncestorSnapshots);
  EXPECT_EQ(sentinelLineage, output.at(0)->lineage.pScopes);
  EXPECT_EQ(sentinelStart, output.at(0)->leafParam.wstart);

  ASSERT_EQ(TSDB_CODE_SUCCESS, stWindowChainPrepareHistoryLeafTail(chain.get(), 30, output.get()));
  stWindowChainCommitHistoryLeafTail(chain.get());
  ASSERT_EQ(3, output.size());
  EXPECT_EQ(sentinelSnapshots, output.at(0)->pAncestorSnapshots);
  EXPECT_EQ(sentinelLineage, output.at(0)->lineage.pScopes);
  expectWindowEvent(output.at(1), STRIGGER_EVENT_WINDOW_CLOSE, 0, 1999, 1);
  expectWindowEvent(output.at(2), STRIGGER_EVENT_WINDOW_CLOSE, 1000, 2999, 1);
}

}  // namespace
