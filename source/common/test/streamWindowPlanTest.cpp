/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef _WIN32
#include "osWindows.h"
#endif

#include <gtest/gtest.h>
#include <stub.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "nodes.h"
#include "streamMsg.h"
#include "tdatablock.h"
#include "tencode.h"
#include "tjson.h"
#include "tmsg.h"

#include "readerFetchBenchmarkTest.h"

using reader_fetch_benchmark::ReaderFetchLayout;

extern "C" int32_t tEncodeSStmStreamDeploy(SEncoder* pEncoder, const SStmStreamDeploy* pStream);
extern "C" int32_t tDecodeSStmStreamDeploy(SDecoder* pDecoder, SStmStreamDeploy* pStream);

namespace {

struct PlanDeleter {
  void operator()(SStreamWindowPlan* plan) const { tDestroyStreamWindowPlan(&plan); }
};

using PlanPtr = std::unique_ptr<SStreamWindowPlan, PlanDeleter>;

struct RequestDeleter {
  void operator()(SCMCreateStreamReq* request) const { tFreeSCMCreateStreamReq(request); }
};

using RequestPtr = std::unique_ptr<SCMCreateStreamReq, RequestDeleter>;

struct TriggerCalcRequestGuard {
  SSTriggerCalcRequest request = {};
  ~TriggerCalcRequestGuard() { tDestroySTriggerCalcRequest(&request); }
};

struct FetchRequestGuard {
  SResFetchReq request = {};
  ~FetchRequestGuard() { tDestroySResFetchReq(&request); }
};

struct RuntimeFuncInfoGuard {
  SStreamRuntimeFuncInfo runtime = {};
  ~RuntimeFuncInfoGuard() { tDestroyStRtFuncInfo(&runtime); }
};

struct TaosMemoryDeleter {
  void operator()(void* memory) const { taosMemoryFree(memory); }
};

using TaosMemoryPtr = std::unique_ptr<void, TaosMemoryDeleter>;

Stub*   gContextFrameAllocStub = nullptr;
int64_t gContextFrameFailSize = -1;
int32_t gContextFrameMatchingAllocations = 0;

void* failMatchingContextFrameAllocation(int64_t size) {
  gContextFrameAllocStub->reset(taosMemMalloc);
  void* memory = nullptr;
  if (size == gContextFrameFailSize && gContextFrameMatchingAllocations++ == 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    memory = taosMemMalloc(size);
  }
  gContextFrameAllocStub->set(taosMemMalloc, failMatchingContextFrameAllocation);
  return memory;
}

class MatchingAllocationFailureGuard {
 public:
  explicit MatchingAllocationFailureGuard(int64_t size) {
    gContextFrameFailSize = size;
    gContextFrameMatchingAllocations = 0;
    gContextFrameAllocStub = &stub_;
    stub_.set(taosMemMalloc, failMatchingContextFrameAllocation);
  }

  ~MatchingAllocationFailureGuard() {
    stub_.reset(taosMemMalloc);
    gContextFrameAllocStub = nullptr;
    gContextFrameFailSize = -1;
  }

 private:
  Stub stub_;
};

Stub*   gNthAllocationStub = nullptr;
int32_t gNthAllocationToFail = -1;
int32_t gNthAllocationCount = 0;
bool    gNthAllocationFailed = false;

bool failNthAllocation() {
  if (gNthAllocationCount++ == gNthAllocationToFail) {
    gNthAllocationFailed = true;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return true;
  }
  return false;
}

void* failNthMalloc(int64_t size) {
  gNthAllocationStub->reset(taosMemMalloc);
  void* memory = failNthAllocation() ? nullptr : taosMemMalloc(size);
  gNthAllocationStub->set(taosMemMalloc, failNthMalloc);
  return memory;
}

void* failNthCalloc(int64_t num, int64_t size) {
  gNthAllocationStub->reset(taosMemCalloc);
  void* memory = failNthAllocation() ? nullptr : taosMemCalloc(num, size);
  gNthAllocationStub->set(taosMemCalloc, failNthCalloc);
  return memory;
}

void* failNthRealloc(void* memory, int64_t size) {
  gNthAllocationStub->reset(taosMemRealloc);
  void* result = failNthAllocation() ? nullptr : taosMemRealloc(memory, size);
  gNthAllocationStub->set(taosMemRealloc, failNthRealloc);
  return result;
}

class NthAllocationFailureGuard {
 public:
  explicit NthAllocationFailureGuard(int32_t allocationToFail) {
    gNthAllocationToFail = allocationToFail;
    gNthAllocationCount = 0;
    gNthAllocationFailed = false;
    gNthAllocationStub = &stub_;
    stub_.set(taosMemMalloc, failNthMalloc);
    stub_.set(taosMemCalloc, failNthCalloc);
    stub_.set(taosMemRealloc, failNthRealloc);
  }

  ~NthAllocationFailureGuard() {
    stub_.reset(taosMemMalloc);
    stub_.reset(taosMemCalloc);
    stub_.reset(taosMemRealloc);
    gNthAllocationStub = nullptr;
    gNthAllocationToFail = -1;
  }

  bool    failed() const { return gNthAllocationFailed; }
  int32_t allocationCount() const { return gNthAllocationCount; }

 private:
  Stub stub_;
};

char* dupText(const char* text) {
  char* copy = taosStrdup(text);
  EXPECT_NE(nullptr, copy);
  return copy;
}

SArray* makeI16Array(std::initializer_list<int16_t> values) {
  SArray* array = taosArrayInit(values.size(), sizeof(int16_t));
  EXPECT_NE(nullptr, array);
  for (int16_t value : values) {
    EXPECT_NE(nullptr, taosArrayPush(array, &value));
  }
  return array;
}

std::string nodeJson(int32_t nodeType) { return "{\"NodeType\":\"" + std::to_string(nodeType) + "\"}"; }

std::string encodedNodeJson(ENodeType nodeType) {
  SNode* node = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(nodeType, &node));
  if (node == nullptr) return {};

  char* json = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(node, false, &json, nullptr));
  std::string result = json == nullptr ? "" : json;
  taosMemoryFree(json);
  nodesDestroyNode(node);
  return result;
}

SStreamWindowLayerSpec makeLayer(int8_t type, const char* name) {
  SStreamWindowLayerSpec spec = {};
  spec.triggerType = type;
  tstrncpy(spec.name, name, sizeof(spec.name));
  spec.input.tsSlotId = 0;
  spec.input.pkSlotId = -1;
  spec.input.eventStartSlotId = -1;
  spec.input.eventEndSlotId = -1;

  switch (type) {
    case WINDOW_TYPE_INTERVAL:
      spec.trigger.sliding.precision = TSDB_TIME_PRECISION_MILLI;
      spec.trigger.sliding.intervalUnit = 's';
      spec.trigger.sliding.slidingUnit = 's';
      spec.trigger.sliding.interval = 10;
      spec.trigger.sliding.sliding = 10;
      break;
    case WINDOW_TYPE_SESSION:
      spec.trigger.session.slotId = 0;
      spec.trigger.session.sessionVal = 10;
      break;
    case WINDOW_TYPE_STATE:
      spec.input.pConditionSlotIds = makeI16Array({1});
      spec.trigger.stateWin.pSlotIds = makeI16Array({1});
      spec.trigger.stateWin.extend = 1;
      spec.trigger.stateWin.expr = dupText("[{\"NodeType\":1}]");
      break;
    case WINDOW_TYPE_EVENT: {
      spec.input.eventStartSlotId = 1;
      const std::string start = nodeJson(QUERY_NODE_OPERATOR);
      spec.trigger.event.startCond = dupText(start.c_str());
      spec.trigger.event.endCond = dupText(start.c_str());
      break;
    }
    case WINDOW_TYPE_COUNT:
      spec.trigger.count.countVal = 2;
      spec.trigger.count.sliding = 1;
      spec.trigger.count.condCols = dupText("[{\"NodeType\":1}]");
      break;
    default:
      break;
  }
  return spec;
}

PlanPtr makePlan(std::initializer_list<int8_t> types) {
  PlanPtr plan(static_cast<SStreamWindowPlan*>(taosMemoryCalloc(1, sizeof(SStreamWindowPlan))));
  EXPECT_NE(nullptr, plan);
  if (!plan) return plan;

  plan->version = STREAM_WINDOW_PLAN_VERSION;
  plan->pLayers = taosArrayInit(types.size(), sizeof(SStreamWindowLayerSpec));
  EXPECT_NE(nullptr, plan->pLayers);
  if (!plan->pLayers) return plan;

  int32_t index = 0;
  for (int8_t type : types) {
    const std::string name = index + 1 == static_cast<int32_t>(types.size()) ? "leaf" : "w" + std::to_string(index);
    SStreamWindowLayerSpec spec = makeLayer(type, name.c_str());
    EXPECT_NE(nullptr, taosArrayPush(plan->pLayers, &spec));
    ++index;
  }
  return plan;
}

SStreamWindowLayerSpec* layer(SStreamWindowPlan* plan, int32_t index) {
  return static_cast<SStreamWindowLayerSpec*>(taosArrayGet(plan->pLayers, index));
}

const SStreamWindowLayerSpec* layer(const SStreamWindowPlan* plan, int32_t index) {
  return static_cast<const SStreamWindowLayerSpec*>(taosArrayGet(plan->pLayers, index));
}

PlanPtr makeStateCountPlan() {
  PlanPtr plan = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_COUNT});
  taosArrayDestroy(layer(plan.get(), 0)->input.pConditionSlotIds);
  layer(plan.get(), 0)->input.pConditionSlotIds = makeI16Array({3, 4});
  layer(plan.get(), 0)->trigger.stateWin.zeroth = dupText("[{\"NodeType\":2}]");
  layer(plan.get(), 0)->trigger.stateWin.trueForType = 1;
  layer(plan.get(), 0)->trigger.stateWin.trueForCount = 2;
  layer(plan.get(), 1)->input.pConditionSlotIds = makeI16Array({5});
  return plan;
}

PlanPtr makeLegacyProjectedStateCountPlan() {
  PlanPtr                 plan = makeStateCountPlan();
  SStreamWindowLayerSpec* outer = layer(plan.get(), 0);
  outer->trigger.stateWin.trueForType = 0;
  outer->trigger.stateWin.trueForCount = 0;
  taosMemoryFreeClear(outer->trigger.stateWin.zeroth);
  SStreamWindowLayerSpec* leaf = layer(plan.get(), 1);
  taosMemoryFreeClear(leaf->trigger.count.condCols);
  leaf->trigger.count.sliding = 2;
  return plan;
}

void expectPlanEquals(const SStreamWindowPlan* left, const SStreamWindowPlan* right) {
  ASSERT_NE(nullptr, left);
  ASSERT_NE(nullptr, right);
  ASSERT_EQ(left->version, right->version);
  ASSERT_EQ(taosArrayGetSize(left->pLayers), taosArrayGetSize(right->pLayers));
  for (int32_t i = 0; i < taosArrayGetSize(left->pLayers); ++i) {
    const SStreamWindowLayerSpec* a = layer(left, i);
    const SStreamWindowLayerSpec* b = layer(right, i);
    EXPECT_EQ(a->triggerType, b->triggerType);
    EXPECT_STREQ(a->name, b->name);
    EXPECT_EQ(a->placeholderMask, b->placeholderMask);
    EXPECT_EQ(a->input.tsSlotId, b->input.tsSlotId);
    EXPECT_EQ(a->input.pkSlotId, b->input.pkSlotId);
    EXPECT_EQ(a->input.eventStartSlotId, b->input.eventStartSlotId);
    EXPECT_EQ(a->input.eventEndSlotId, b->input.eventEndSlotId);
    const int32_t aInputNum = a->input.pConditionSlotIds == nullptr ? 0 : taosArrayGetSize(a->input.pConditionSlotIds);
    const int32_t bInputNum = b->input.pConditionSlotIds == nullptr ? 0 : taosArrayGetSize(b->input.pConditionSlotIds);
    ASSERT_EQ(aInputNum, bInputNum);
    for (int32_t j = 0; j < aInputNum; ++j) {
      EXPECT_EQ(*static_cast<int16_t*>(taosArrayGet(a->input.pConditionSlotIds, j)),
                *static_cast<int16_t*>(taosArrayGet(b->input.pConditionSlotIds, j)));
    }
  }
}

std::vector<uint8_t> encodePlan(const SStreamWindowPlan* plan) {
  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamWindowPlan(&sizer, plan));
  std::vector<uint8_t> data(sizer.pos);
  tEncoderClear(&sizer);

  SEncoder encoder = {};
  tEncoderInit(&encoder, data.data(), data.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamWindowPlan(&encoder, plan));
  EXPECT_EQ(data.size(), encoder.pos);
  tEncoderClear(&encoder);
  return data;
}

std::vector<uint8_t> encodeRuntimeFuncInfo(const SStreamRuntimeFuncInfo* runtime) {
  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSerializeStRtFuncInfo(&sizer, runtime, false, false));
  std::vector<uint8_t> data(sizer.pos);
  tEncoderClear(&sizer);

  SEncoder encoder = {};
  tEncoderInit(&encoder, data.data(), data.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSerializeStRtFuncInfo(&encoder, runtime, false, false));
  EXPECT_EQ(data.size(), encoder.pos);
  tEncoderClear(&encoder);
  return data;
}

int32_t decodeRuntimeFuncInfo(const std::vector<uint8_t>& data, SStreamRuntimeFuncInfo* runtime) {
  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(data.data()), data.size());
  const int32_t code = tDeserializeStRtFuncInfo(&decoder, runtime);
  tDecoderClear(&decoder);
  return code;
}

SCMCreateStreamReq makeLegacyCountRequest() {
  SCMCreateStreamReq request = {};
  request.triggerType = WINDOW_TYPE_COUNT;
  request.trigger.count.countVal = 2;
  request.trigger.count.sliding = 2;
  request.calcTsSlotId = -1;
  request.triTsSlotId = -1;
  request.calcPkSlotId = -1;
  request.triPkSlotId = -1;
  return request;
}

constexpr char kLegacyCountRequestBytes[] =
    "\x5d\x03\x00\x00\xd2\x06"
    R"({"streamId":"0","igExists":"0","triggerType":"5","igDisorder":"0","deleteReCalc":"0","deleteOutTbl":"0","fillHistory":"0","fillHistoryFirst":"0","calcNotifyOnly":"0","lowLatencyCalc":"0","igNoDataTrigger":"0","multiGroupCalc":"0","notifyEventTypes":"0","addOptions":"0","notifyHistory":"0","maxDelay":"0","fillHistoryStartTime":"0","watermark":"0","expiredTime":"0","idleTimeoutMs":"0","trigger":{"countVal":"2","sliding":"2"},"triggerTblType":"0","triggerTblUid":"0","triggerTblSuid":"0","triggerPrec":"0","vtableCalc":"0","outTblType":"0","outStbExists":"0","outStbUid":"0","outStbSversion":"0","eventTypes":"0","flags":"0","tsmaId":"0","placeHolderBitmap":"0","calcTsSlotId":"-1","triTsSlotId":"-1","calcPkSlotId":"-1","triPkSlotId":"-1","triggerTblVgId":"0","outTblVgId":"0","triggerHasPF":"0","numOfCalcSubplan":"0","nodelayCreateSubtable":"0"})"
    // Tail appended by tSerializeSCMCreateStreamReq after the JSON body, in encode order:
    // txnId (tEncodeI64, 8 bytes) then outTblExists (tEncodeI8, 1 byte), both zero here
    // because this golden buffer models a legacy non-txn request. The leading 6 bytes are
    // the tStartEncode/tEncodeCStrWithLen prefix (see kLegacyCountRequestPrefixSize).
    // Adding, removing or reordering fields in that tail requires updating these bytes.
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00";
constexpr size_t      kLegacyCountRequestPrefixSize = 6;
constexpr const char* kLegacyCountRequestJson = kLegacyCountRequestBytes + kLegacyCountRequestPrefixSize;

std::string encodeCreateStreamRequestJson(const SCMCreateStreamReq* request) {
  char*   encoded = nullptr;
  int32_t len = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, scmCreateStreamReqToJson(request, false, &encoded, &len));
  EXPECT_NE(nullptr, encoded);
  if (encoded == nullptr) return {};

  std::string result(encoded, len);
  taosMemoryFree(encoded);
  return result;
}

std::vector<uint8_t> serializeCreateStreamRequest(const SCMCreateStreamReq* request) {
  const int32_t len = tSerializeSCMCreateStreamReq(nullptr, 0, request);
  EXPECT_GT(len, 0);
  if (len <= 0) return {};

  std::vector<uint8_t> result(len);
  EXPECT_EQ(len, tSerializeSCMCreateStreamReq(result.data(), result.size(), request));
  return result;
}

std::string encodeNestedCountRequestJson(PlanPtr plan, int32_t addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  source.addOptions = addOptions;
  source.pWindowPlan = plan.release();
  return encodeCreateStreamRequestJson(&source);
}

std::string unformattedJson(const SJson* json) {
  char* encoded = tjsonToUnformattedString(json);
  EXPECT_NE(nullptr, encoded);
  if (encoded == nullptr) return {};

  std::string result(encoded);
  taosMemoryFree(encoded);
  return result;
}

std::string removeWindowPlanField(const std::string& encoded, const char* field) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  EXPECT_NE(nullptr, json);
  if (json == nullptr) return {};
  SJson* plan = tjsonGetObjectItem(json.get(), "WindowPlan");
  EXPECT_NE(nullptr, plan);
  if (plan == nullptr) return {};

  tjsonDeleteItemFromObject(plan, field);
  return unformattedJson(json.get());
}

std::string replaceWindowPlanFieldWithInteger(const std::string& encoded, const char* field, int64_t value) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  EXPECT_NE(nullptr, json);
  if (json == nullptr) return {};
  SJson* plan = tjsonGetObjectItem(json.get(), "WindowPlan");
  EXPECT_NE(nullptr, plan);
  if (plan == nullptr) return {};

  tjsonDeleteItemFromObject(plan, field);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tjsonAddIntegerToObject(plan, field, value));
  return unformattedJson(json.get());
}

std::string removeFirstWindowLayerField(const std::string& encoded, const char* field) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  EXPECT_NE(nullptr, json);
  if (json == nullptr) return {};
  SJson* plan = tjsonGetObjectItem(json.get(), "WindowPlan");
  SJson* layers = plan == nullptr ? nullptr : tjsonGetObjectItem(plan, "Layers");
  SJson* firstLayer = layers == nullptr ? nullptr : tjsonGetArrayItem(layers, 0);
  EXPECT_NE(nullptr, firstLayer);
  if (firstLayer == nullptr) return {};

  tjsonDeleteItemFromObject(firstLayer, field);
  return unformattedJson(json.get());
}

void expectCreateRequestDecodeFailure(const std::string& encoded) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  SCMCreateStreamReq decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(json.get(), &decoded));
  EXPECT_EQ(nullptr, decoded.pWindowPlan);
  tFreeSCMCreateStreamReq(&decoded);
  EXPECT_EQ(nullptr, decoded.pWindowPlan);
}

std::string encodeNodeJson(const SNode* node) {
  char*   encoded = nullptr;
  int32_t len = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(node, false, &encoded, &len));
  EXPECT_NE(nullptr, encoded);
  if (encoded == nullptr) return {};

  if (len > 0 && encoded[len - 1] == '\0') --len;
  std::string result(encoded, len);
  taosMemoryFree(encoded);
  return result;
}

std::string addNestedJsonBoolean(std::string jsonText, const char* nodeName, const char* key, bool value) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(jsonText.c_str()), tjsonDelete);
  EXPECT_NE(nullptr, json);
  if (json == nullptr) return {};

  SJson* payload = tjsonGetObjectItem(json.get(), nodeName);
  EXPECT_NE(nullptr, payload);
  if (payload == nullptr) return {};

  EXPECT_EQ(TSDB_CODE_SUCCESS, tjsonAddBoolToObject(payload, key, value));
  char* encoded = tjsonToUnformattedString(json.get());
  EXPECT_NE(nullptr, encoded);
  if (encoded == nullptr) return {};

  std::string result(encoded);
  taosMemoryFree(encoded);
  return result;
}

void expectStructuredWindowPlanJson(const std::string& encoded) {
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  EXPECT_EQ(std::string::npos, encoded.find("\"windowPlan\""));

  SJson* windowPlan = tjsonGetObjectItem(json.get(), "WindowPlan");
  ASSERT_NE(nullptr, windowPlan);
  EXPECT_EQ(std::string::npos, encoded.find("\"version\""));
  EXPECT_EQ(std::string::npos, encoded.find("\"layers\""));

  int32_t version = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tjsonGetIntValue(windowPlan, "Version", &version));
  EXPECT_EQ(STREAM_WINDOW_PLAN_VERSION, version);

  SJson* layers = tjsonGetObjectItem(windowPlan, "Layers");
  ASSERT_NE(nullptr, layers);
  ASSERT_EQ(2, tjsonGetArraySize(layers));
  for (int32_t i = 0; i < tjsonGetArraySize(layers); ++i) {
    SJson* layerJson = tjsonGetArrayItem(layers, i);
    ASSERT_NE(nullptr, layerJson);
    for (const char* key : {"name", "triggerType", "placeholderMask", "input", "trigger"}) {
      EXPECT_NE(nullptr, tjsonGetObjectItem(layerJson, key));
    }

    SJson* input = tjsonGetObjectItem(layerJson, "input");
    ASSERT_NE(nullptr, input);
    for (const char* key : {"tsSlotId", "pkSlotId", "eventStartSlotId", "eventEndSlotId", "conditionSlotIds"}) {
      EXPECT_NE(nullptr, tjsonGetObjectItem(input, key));
    }
  }
}

int32_t decodePlan(const std::vector<uint8_t>& data, SStreamWindowPlan** plan) {
  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(data.data()), data.size());
  const int32_t code = tDecodeStreamWindowPlan(&decoder, plan);
  tDecoderClear(&decoder);
  return code;
}

void expectPreflightDecodeFailure(const std::vector<uint8_t>& data) {
  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(data.data()), data.size());
  SStreamWindowPlan* decoded = nullptr;
  EXPECT_NE(TSDB_CODE_SUCCESS, tDecodeStreamWindowPlan(&decoder, &decoded));
  EXPECT_EQ(nullptr, decoded);
  EXPECT_EQ(0U, decoder.pos);
  tDecoderClear(&decoder);
}

void writeI32(std::vector<uint8_t>* data, size_t offset, int32_t value) {
  ASSERT_NE(nullptr, data);
  ASSERT_LE(offset + sizeof(value), data->size());
  memcpy(data->data() + offset, &value, sizeof(value));
}

size_t firstLayerConditionCountOffset() {
  return sizeof(int32_t) * 2 + sizeof(int8_t) + TSDB_TABLE_NAME_LEN + sizeof(int64_t) + sizeof(int16_t) * 4;
}

size_t firstLayerTriggerOffset() { return firstLayerConditionCountOffset() + sizeof(int32_t); }

std::vector<uint8_t> encodeHeader(int32_t version, int32_t layerCount) {
  std::vector<uint8_t> data(sizeof(version) + sizeof(layerCount));
  SEncoder             encoder = {};
  tEncoderInit(&encoder, data.data(), data.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeI32(&encoder, version));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeI32(&encoder, layerCount));
  tEncoderClear(&encoder);
  return data;
}

TEST(StreamWindowPlanTest, cloneOwnsEveryVariableLengthField) {
  PlanPtr            plan = makeStateCountPlan();
  SStreamWindowPlan* rawCopy = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(plan.get(), &rawCopy));
  PlanPtr copy(rawCopy);

  ASSERT_NE(plan->pLayers, copy->pLayers);
  ASSERT_NE(layer(plan.get(), 0)->input.pConditionSlotIds, layer(copy.get(), 0)->input.pConditionSlotIds);
  ASSERT_NE(layer(plan.get(), 0)->trigger.stateWin.pSlotIds, layer(copy.get(), 0)->trigger.stateWin.pSlotIds);
  ASSERT_NE(layer(plan.get(), 0)->trigger.stateWin.zeroth, layer(copy.get(), 0)->trigger.stateWin.zeroth);
  ASSERT_NE(layer(plan.get(), 0)->trigger.stateWin.expr, layer(copy.get(), 0)->trigger.stateWin.expr);
  ASSERT_NE(layer(plan.get(), 1)->trigger.count.condCols, layer(copy.get(), 1)->trigger.count.condCols);

  plan.reset();
  EXPECT_STREQ("[{\"NodeType\":2}]", static_cast<char*>(layer(copy.get(), 0)->trigger.stateWin.zeroth));
  EXPECT_STREQ("[{\"NodeType\":1}]", static_cast<char*>(layer(copy.get(), 1)->trigger.count.condCols));
}

TEST(StreamWindowPlanTest, cloneOwnsEventConditionStrings) {
  PlanPtr            plan = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  SStreamWindowPlan* rawCopy = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(plan.get(), &rawCopy));
  PlanPtr copy(rawCopy);
  EXPECT_NE(layer(plan.get(), 1)->trigger.event.startCond, layer(copy.get(), 1)->trigger.event.startCond);
  EXPECT_NE(layer(plan.get(), 1)->trigger.event.endCond, layer(copy.get(), 1)->trigger.event.endCond);
}

TEST(StreamWindowPlanTest, rejectsDepthOutsideTwoToEight) {
  SStreamWindowPlanValidationCtx ctx = {};
  PlanPtr                        one = makePlan({WINDOW_TYPE_SESSION});
  PlanPtr                        nine =
      makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION,
                WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION});
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(one.get(), &ctx));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(nine.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesCanonicalLayerNames) {
  SStreamWindowPlanValidationCtx ctx = {};
  PlanPtr                        plan = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->name[0] = '\0';
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  tstrncpy(layer(plan.get(), 0)->name, "Window", sizeof(layer(plan.get(), 0)->name));
  tstrncpy(layer(plan.get(), 1)->name, "window", sizeof(layer(plan.get(), 1)->name));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  tstrncpy(layer(plan.get(), 0)->name, "\xE7\xAA\x97\xE5\x8F\xA3 layer!", sizeof(layer(plan.get(), 0)->name));
  tstrncpy(layer(plan.get(), 1)->name, "leaf", sizeof(layer(plan.get(), 1)->name));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  memset(layer(plan.get(), 1)->name, 'x', sizeof(layer(plan.get(), 1)->name));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsMalformedIntrinsicLayerParameters) {
  SStreamWindowPlanValidationCtx ctx = {};

  PlanPtr session = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(session.get(), 0)->trigger.session.sessionVal = -1;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(session.get(), &ctx));

  PlanPtr pureSliding = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  layer(pureSliding.get(), 0)->trigger.sliding.interval = 0;
  layer(pureSliding.get(), 0)->trigger.sliding.sliding = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(pureSliding.get(), &ctx));

  PlanPtr interval = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  layer(interval.get(), 0)->trigger.sliding.interval = 10;
  layer(interval.get(), 0)->trigger.sliding.sliding = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(interval.get(), &ctx));

  interval = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  layer(interval.get(), 0)->trigger.sliding.overlap = true;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(interval.get(), &ctx));

  PlanPtr state = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_COUNT});
  taosArrayDestroy(layer(state.get(), 0)->input.pConditionSlotIds);
  layer(state.get(), 0)->input.pConditionSlotIds = nullptr;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(state.get(), &ctx));

  state = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_COUNT});
  layer(state.get(), 0)->trigger.stateWin.extend = 3;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(state.get(), &ctx));

  PlanPtr count = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(count.get(), 1)->trigger.count.countVal = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(count.get(), &ctx));
  layer(count.get(), 1)->trigger.count.countVal = 2;
  layer(count.get(), 1)->trigger.count.sliding = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(count.get(), &ctx));

  PlanPtr event = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  layer(event.get(), 1)->input.eventStartSlotId = -1;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tValidateStreamWindowPlan(event.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesNonLeafOverlapAndLeafOverlap) {
  SStreamWindowPlanValidationCtx ctx = {};
  PlanPtr                        interval = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  layer(interval.get(), 0)->trigger.sliding.interval = 10;
  layer(interval.get(), 0)->trigger.sliding.sliding = 5;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(interval.get(), &ctx));

  PlanPtr intervalLeaf = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_INTERVAL});
  layer(intervalLeaf.get(), 1)->trigger.sliding.interval = 10;
  layer(intervalLeaf.get(), 1)->trigger.sliding.sliding = 5;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(intervalLeaf.get(), &ctx));

  PlanPtr count = makePlan({WINDOW_TYPE_COUNT, WINDOW_TYPE_SESSION});
  layer(count.get(), 0)->trigger.count.countVal = 3;
  layer(count.get(), 0)->trigger.count.sliding = 2;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(count.get(), &ctx));

  PlanPtr countLeaf = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(countLeaf.get(), 1)->trigger.count.countVal = 3;
  layer(countLeaf.get(), 1)->trigger.count.sliding = 2;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(countLeaf.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesPureSlidingAndStateCapabilities) {
  SStreamWindowPlanValidationCtx ctx = {};
  PlanPtr                        sliding = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_SESSION});
  layer(sliding.get(), 0)->trigger.sliding.interval = 0;
  layer(sliding.get(), 0)->trigger.sliding.sliding = 10;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(sliding.get(), &ctx));

  PlanPtr state = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_SESSION});
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(state.get(), &ctx));
  layer(state.get(), 0)->trigger.stateWin.extend = 0;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(state.get(), &ctx));
  layer(state.get(), 0)->trigger.stateWin.extend = 2;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(state.get(), &ctx));
  layer(state.get(), 0)->trigger.stateWin.extend = 1;
  layer(state.get(), 0)->trigger.stateWin.trueForDuration = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(state.get(), &ctx));
  layer(state.get(), 0)->trigger.stateWin.trueForDuration = 0;
  layer(state.get(), 0)->trigger.stateWin.zeroth = dupText("[{\"NodeType\":2}]");
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(state.get(), &ctx));

  PlanPtr stateLeaf = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_STATE});
  layer(stateLeaf.get(), 1)->trigger.stateWin.extend = 0;
  layer(stateLeaf.get(), 1)->trigger.stateWin.trueForDuration = 1;
  layer(stateLeaf.get(), 1)->trigger.stateWin.zeroth = dupText("[{\"NodeType\":2}]");
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(stateLeaf.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsMultiStartEventOnlyWhenNonLeaf) {
  SStreamWindowPlanValidationCtx ctx = {};
  const std::string              multiStart = nodeJson(QUERY_NODE_NODE_LIST);
  PlanPtr                        nonLeaf = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  taosMemoryFreeClear(layer(nonLeaf.get(), 0)->trigger.event.startCond);
  layer(nonLeaf.get(), 0)->trigger.event.startCond = dupText(multiStart.c_str());
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(nonLeaf.get(), &ctx));

  PlanPtr leafEvent = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  taosMemoryFreeClear(layer(leafEvent.get(), 1)->trigger.event.startCond);
  layer(leafEvent.get(), 1)->trigger.event.startCond = dupText(multiStart.c_str());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(leafEvent.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesRepositoryEncodedEventStartKinds) {
  SStreamWindowPlanValidationCtx ctx = {};
  const std::string              singleStart = encodedNodeJson(QUERY_NODE_OPERATOR);
  ASSERT_NE(std::string::npos, singleStart.find("\"NodeType\":\"3\""));
  PlanPtr nonLeaf = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  taosMemoryFreeClear(layer(nonLeaf.get(), 0)->trigger.event.startCond);
  layer(nonLeaf.get(), 0)->trigger.event.startCond = dupText(singleStart.c_str());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(nonLeaf.get(), &ctx));

  const std::string multiStart = encodedNodeJson(QUERY_NODE_NODE_LIST);
  ASSERT_NE(std::string::npos, multiStart.find("\"NodeType\":\"15\""));
  taosMemoryFreeClear(layer(nonLeaf.get(), 0)->trigger.event.startCond);
  layer(nonLeaf.get(), 0)->trigger.event.startCond = dupText(multiStart.c_str());
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(nonLeaf.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsMalformedRepositoryEventStartKinds) {
  SStreamWindowPlanValidationCtx ctx = {};
  for (const char* start : {"{\"Name\":\"Operator\"}", "{\"NodeType\":\"3x\"}", "{\"NodeType\":\"2147483648\"}",
                            "{\"NodeType\":\"0\"}", "{\"NodeType\":\"-1\"}", "{\"NodeType\":3.5}", "{"}) {
    PlanPtr plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
    taosMemoryFreeClear(layer(plan.get(), 0)->trigger.event.startCond);
    layer(plan.get(), 0)->trigger.event.startCond = dupText(start);
    EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
  }
}

TEST(StreamWindowPlanTest, rejectsAllNonLeafEventTrueForVariants) {
  SStreamWindowPlanValidationCtx ctx = {};

  PlanPtr plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.trueForType = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.trueForCount = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.trueForDuration = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.startTrueForType = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.startTrueForCount = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.startTrueForDuration = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.endTrueForType = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.endTrueForCount = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  plan = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  layer(plan.get(), 0)->trigger.event.endTrueForDuration = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsUnsupportedWindowTypes) {
  SStreamWindowPlanValidationCtx ctx = {};
  for (int8_t type : {static_cast<int8_t>(WINDOW_TYPE_PERIOD), static_cast<int8_t>(WINDOW_TYPE_ANOMALY),
                      static_cast<int8_t>(WINDOW_TYPE_EXTERNAL), static_cast<int8_t>(99)}) {
    PlanPtr plan = makePlan({WINDOW_TYPE_SESSION, type});
    EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
  }
}

TEST(StreamWindowPlanTest, validatesChainCapabilityContext) {
  PlanPtr                        rowPlan = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_COUNT});
  SStreamWindowPlanValidationCtx ctx = {};
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));

  ctx.isExtTrigger = true;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));
  ctx = {};
  ctx.hasCompositePrimaryKey = true;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));

  ctx = {};
  ctx.isSuperTable = true;
  ctx.partitionByTag = true;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));
  ctx.partitionByTbname = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));
  ctx.partitionByTag = false;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));

  PlanPtr timePlan = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_SESSION});
  ctx = {};
  ctx.isSuperTable = true;
  ctx.partitionByTag = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(timePlan.get(), &ctx));

  ctx = {};
  ctx.hasRollup = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(timePlan.get(), &ctx));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(rowPlan.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesDeleteRecalcCountStep) {
  SStreamWindowPlanValidationCtx ctx = {};
  ctx.deleteRecalc = true;
  PlanPtr                        sliding = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_SESSION});
  layer(sliding.get(), 0)->trigger.sliding.interval = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(sliding.get(), &ctx));

  PlanPtr count = makePlan({WINDOW_TYPE_COUNT, WINDOW_TYPE_SESSION});
  layer(count.get(), 0)->trigger.count.countVal = 1;
  layer(count.get(), 0)->trigger.count.sliding = 2;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(count.get(), &ctx));
  layer(count.get(), 0)->trigger.count.sliding = 1;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(count.get(), &ctx));
}

TEST(StreamWindowPlanTest, validatesLeafScopedIgnoreNoDataOption) {
  SStreamWindowPlanValidationCtx ctx = {};
  ctx.ignoreNoDataTrigger = true;
  PlanPtr                        countInterval = makePlan({WINDOW_TYPE_COUNT, WINDOW_TYPE_INTERVAL});
  layer(countInterval.get(), 0)->trigger.count.countVal = 1;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(countInterval.get(), &ctx));
  PlanPtr intervalCount = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(intervalCount.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsIdleResumeWithLayerPlaceholders) {
  PlanPtr plan = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(plan.get(), 0)->placeholderMask = PLACE_HOLDER_WSTART;
  SStreamWindowPlanValidationCtx ctx = {};
  ctx.flushOnOuterClose = true;
  ctx.eventTypes = BIT_FLAG_MASK(2);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
  ctx.eventTypes = BIT_FLAG_MASK(3);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
  ctx.eventTypes = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsPlaceholderMasksUnsupportedByLayerType) {
  SStreamWindowPlanValidationCtx ctx = {};

  PlanPtr sliding = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_INTERVAL});
  layer(sliding.get(), 1)->trigger.sliding.interval = 0;
  layer(sliding.get(), 1)->placeholderMask = PLACE_HOLDER_WSTART;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(sliding.get(), &ctx));
  layer(sliding.get(), 1)->placeholderMask = PLACE_HOLDER_PREV_TS;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(sliding.get(), &ctx));

  PlanPtr count = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(count.get(), 1)->placeholderMask = PLACE_HOLDER_PREV_TS;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(count.get(), &ctx));
  layer(count.get(), 1)->placeholderMask = PLACE_HOLDER_WSTART;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(count.get(), &ctx));
}

TEST(StreamWindowPlanTest, rejectsMismatchedLeafProjection) {
  PlanPtr        plan = makeStateCountPlan();
  SStreamTrigger wrong = layer(plan.get(), 1)->trigger;
  wrong.count.countVal = 3;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlanLeafProjection(plan.get(), WINDOW_TYPE_COUNT, &wrong));
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(plan.get(), WINDOW_TYPE_SESSION, &layer(plan.get(), 1)->trigger));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(plan.get(), WINDOW_TYPE_COUNT, &layer(plan.get(), 1)->trigger));
}

TEST(StreamWindowPlanTest, comparesVariableLeafProjectionFields) {
  PlanPtr        state = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_STATE});
  SStreamTrigger stateProjection = layer(state.get(), 1)->trigger;
  stateProjection.stateWin.expr = const_cast<char*>("different");
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(state.get(), WINDOW_TYPE_STATE, &stateProjection));

  PlanPtr        event = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  SStreamTrigger eventProjection = layer(event.get(), 1)->trigger;
  eventProjection.event.endCond = const_cast<char*>("different");
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(event.get(), WINDOW_TYPE_EVENT, &eventProjection));

  PlanPtr        count = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  SStreamTrigger countProjection = layer(count.get(), 1)->trigger;
  countProjection.count.condCols = const_cast<char*>("different");
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(count.get(), WINDOW_TYPE_COUNT, &countProjection));
}

TEST(StreamWindowPlanTest, comparesSemanticLeafProjectionFields) {
  PlanPtr        session = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_SESSION});
  SStreamTrigger projection = layer(session.get(), 1)->trigger;
  ++projection.session.sessionVal;
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(session.get(), WINDOW_TYPE_SESSION, &projection));

  PlanPtr interval = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_INTERVAL});
  projection = layer(interval.get(), 1)->trigger;
  ++projection.sliding.offset;
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(interval.get(), WINDOW_TYPE_INTERVAL, &projection));

  projection = layer(interval.get(), 1)->trigger;
  projection.sliding.overlap = !projection.sliding.overlap;
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tValidateStreamWindowPlanLeafProjection(interval.get(), WINDOW_TYPE_INTERVAL, &projection));

  PlanPtr event = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  projection = layer(event.get(), 1)->trigger;
  ++projection.event.startTrueForCount;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateStreamWindowPlanLeafProjection(event.get(), WINDOW_TYPE_EVENT, &projection));
}

TEST(StreamWindowPlanTest, codecRoundTripOwnsAllPayloads) {
  PlanPtr source = makeStateCountPlan();
  layer(source.get(), 0)->placeholderMask = PLACE_HOLDER_WSTART;
  std::vector<uint8_t> data = encodePlan(source.get());
  SStreamWindowPlan*   rawDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePlan(data, &rawDecoded));
  PlanPtr decoded(rawDecoded);
  expectPlanEquals(source.get(), decoded.get());
  EXPECT_NE(layer(source.get(), 0)->trigger.stateWin.pSlotIds, layer(decoded.get(), 0)->trigger.stateWin.pSlotIds);
  EXPECT_EQ(layer(source.get(), 0)->trigger.stateWin.trueForType,
            layer(decoded.get(), 0)->trigger.stateWin.trueForType);
  EXPECT_EQ(layer(source.get(), 0)->trigger.stateWin.trueForCount,
            layer(decoded.get(), 0)->trigger.stateWin.trueForCount);
  EXPECT_STREQ(static_cast<char*>(layer(source.get(), 0)->trigger.stateWin.zeroth),
               static_cast<char*>(layer(decoded.get(), 0)->trigger.stateWin.zeroth));
  EXPECT_STREQ(static_cast<char*>(layer(source.get(), 0)->trigger.stateWin.expr),
               static_cast<char*>(layer(decoded.get(), 0)->trigger.stateWin.expr));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlanLeafProjection(decoded.get(), WINDOW_TYPE_COUNT,
                                                                       &layer(source.get(), 1)->trigger));

  PlanPtr        eventSource = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_EVENT});
  SEventTrigger* event = &layer(eventSource.get(), 1)->trigger.event;
  event->trueForType = 1;
  event->trueForCount = 2;
  event->trueForDuration = 3;
  event->startTrueForType = 4;
  event->startTrueForCount = 5;
  event->startTrueForDuration = 6;
  event->endTrueForType = 7;
  event->endTrueForCount = 8;
  event->endTrueForDuration = 9;
  data = encodePlan(eventSource.get());
  rawDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePlan(data, &rawDecoded));
  PlanPtr eventDecoded(rawDecoded);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlanLeafProjection(eventDecoded.get(), WINDOW_TYPE_EVENT,
                                                                       &layer(eventSource.get(), 1)->trigger));
  EXPECT_NE(layer(eventSource.get(), 1)->trigger.event.startCond,
            layer(eventDecoded.get(), 1)->trigger.event.startCond);
  EXPECT_NE(layer(eventSource.get(), 1)->trigger.event.endCond, layer(eventDecoded.get(), 1)->trigger.event.endCond);
}

TEST(StreamWindowPlanTest, codecRejectsMalformedHeadersAndTruncatedPayload) {
  for (const auto& data :
       {encodeHeader(STREAM_WINDOW_PLAN_VERSION + 1, 2), encodeHeader(STREAM_WINDOW_PLAN_VERSION, -1),
        encodeHeader(STREAM_WINDOW_PLAN_VERSION, STREAM_WINDOW_MAX_LAYERS + 1)}) {
    expectPreflightDecodeFailure(data);
  }

  for (int8_t type : {static_cast<int8_t>(WINDOW_TYPE_INTERVAL), static_cast<int8_t>(WINDOW_TYPE_SESSION),
                      static_cast<int8_t>(WINDOW_TYPE_STATE), static_cast<int8_t>(WINDOW_TYPE_EVENT),
                      static_cast<int8_t>(WINDOW_TYPE_COUNT), static_cast<int8_t>(WINDOW_TYPE_PERIOD)}) {
    PlanPtr              source = makePlan({WINDOW_TYPE_SESSION, type});
    std::vector<uint8_t> truncated = encodePlan(source.get());
    truncated.pop_back();
    expectPreflightDecodeFailure(truncated);
  }
}

TEST(StreamWindowPlanTest, codecRejectsNegativeNestedLengthsBeforeAllocation) {
  PlanPtr              source = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_SESSION});
  std::vector<uint8_t> data = encodePlan(source.get());
  writeI32(&data, firstLayerConditionCountOffset(), -1);
  expectPreflightDecodeFailure(data);

  source = makePlan({WINDOW_TYPE_STATE, WINDOW_TYPE_SESSION});
  data = encodePlan(source.get());
  writeI32(&data, firstLayerTriggerOffset(), -1);
  expectPreflightDecodeFailure(data);

  source = makePlan({WINDOW_TYPE_EVENT, WINDOW_TYPE_SESSION});
  data = encodePlan(source.get());
  writeI32(&data, firstLayerTriggerOffset(), -1);
  expectPreflightDecodeFailure(data);
}

TEST(StreamWindowPlanTest, createRequestCloneDeepOwnsWindowPlan) {
  SCMCreateStreamReq src = {};
  PlanPtr            expected = makeStateCountPlan();
  src.pWindowPlan = expected.release();
  SCMCreateStreamReq* dst = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamCreateDeployPointers(&src, &dst));
  ASSERT_NE(nullptr, dst);
  ASSERT_NE(src.pWindowPlan, dst->pWindowPlan);

  SStreamWindowPlan* expectedCopy = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(src.pWindowPlan, &expectedCopy));
  PlanPtr expectedOwner(expectedCopy);
  tFreeSCMCreateStreamReq(&src);
  expectPlanEquals(dst->pWindowPlan, expectedOwner.get());
  tFreeSCMCreateStreamReq(dst);
  taosMemoryFree(dst);
}

TEST(StreamWindowPlanTest, createRequestJsonRoundTripDeepOwnsWindowPlan) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  PlanPtr            expected = makeLegacyProjectedStateCountPlan();
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(expected.get(), &source.pWindowPlan));

  const std::string encoded = encodeCreateStreamRequestJson(&source);
  expectStructuredWindowPlanJson(encoded);

  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  SCMCreateStreamReq decoded = {};
  RequestPtr         decodedGuard(&decoded);
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(json.get(), &decoded));
  ASSERT_NE(nullptr, decoded.pWindowPlan);
  ASSERT_NE(source.pWindowPlan, decoded.pWindowPlan);
  ASSERT_NE(source.pWindowPlan->pLayers, decoded.pWindowPlan->pLayers);

  tFreeSCMCreateStreamReq(&source);
  EXPECT_EQ(nullptr, source.pWindowPlan);
  expectPlanEquals(expected.get(), decoded.pWindowPlan);
}

TEST(StreamWindowPlanTest, createRequestJsonRoundTripsNullAndEmptyConditionSlotIds) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  PlanPtr            plan = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  layer(plan.get(), 1)->trigger.count.sliding = 2;
  taosMemoryFreeClear(layer(plan.get(), 1)->trigger.count.condCols);
  taosArrayDestroy(layer(plan.get(), 0)->input.pConditionSlotIds);
  layer(plan.get(), 0)->input.pConditionSlotIds = nullptr;
  taosArrayDestroy(layer(plan.get(), 1)->input.pConditionSlotIds);
  layer(plan.get(), 1)->input.pConditionSlotIds = taosArrayInit(0, sizeof(int16_t));
  ASSERT_NE(nullptr, layer(plan.get(), 1)->input.pConditionSlotIds);
  source.pWindowPlan = plan.release();

  const std::string encoded = encodeCreateStreamRequestJson(&source);
  EXPECT_NE(std::string::npos, encoded.find("\"conditionSlotIds\":[]"));

  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  SCMCreateStreamReq decoded = {};
  RequestPtr         decodedGuard(&decoded);
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(json.get(), &decoded));
  ASSERT_NE(nullptr, decoded.pWindowPlan);
  for (int32_t i = 0; i < taosArrayGetSize(decoded.pWindowPlan->pLayers); ++i) {
    const auto* pLayer = layer(decoded.pWindowPlan, i);
    ASSERT_NE(nullptr, pLayer->input.pConditionSlotIds);
    EXPECT_EQ(0, taosArrayGetSize(pLayer->input.pConditionSlotIds));
  }
}

TEST(StreamWindowPlanTest, createRequestJsonLegacyWindowPlanOmissionIsByteStable) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  const std::string  legacy = encodeCreateStreamRequestJson(&source);
  EXPECT_EQ(kLegacyCountRequestJson, legacy);
  EXPECT_EQ(std::string::npos, legacy.find("\"WindowPlan\""));

  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(legacy.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  SCMCreateStreamReq decoded = {};
  RequestPtr         decodedGuard(&decoded);
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(json.get(), &decoded));
  EXPECT_EQ(nullptr, decoded.pWindowPlan);
  EXPECT_EQ(legacy, encodeCreateStreamRequestJson(&decoded));
}

TEST(StreamWindowPlanTest, createRequestLegacySerializationMatchesGoldenBytes) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  const auto         serialized = serializeCreateStreamRequest(&source);

  ASSERT_EQ(sizeof(kLegacyCountRequestBytes), serialized.size());
  EXPECT_EQ(0, memcmp(kLegacyCountRequestBytes, serialized.data(), serialized.size()));
}

TEST(StreamWindowPlanTest, createRequestJsonRequiresNestedBitAndWindowPlanPairing) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  {
    SCOPED_TRACE("nested bit without WindowPlan");
    expectCreateRequestDecodeFailure(encodeCreateStreamRequestJson(&source));
  }
  {
    SCOPED_TRACE("WindowPlan without nested bit");
    expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(makeLegacyProjectedStateCountPlan(), 0));
  }
}

TEST(StreamWindowPlanTest, createRequestJsonRequiresNestedPlanForFlushOnOuterClose) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  source.addOptions = STREAM_OPTION_FLUSH_ON_OUTER_CLOSE;
  expectCreateRequestDecodeFailure(encodeCreateStreamRequestJson(&source));
}

TEST(StreamWindowPlanTest, createRequestJsonRejectsMalformedWindowPlanStructure) {
  const std::string valid = encodeNestedCountRequestJson(makeLegacyProjectedStateCountPlan());
  struct MalformedCase {
    const char* name;
    std::string encoded;
  };
  std::vector<MalformedCase> cases;
  cases.push_back({"missing Layers", removeWindowPlanField(valid, "Layers")});
  cases.push_back({"non-array Layers", replaceWindowPlanFieldWithInteger(valid, "Layers", 1)});

  std::string nonObjectLayer = valid;
  const auto  layersPos = nonObjectLayer.find("\"Layers\":[{");
  ASSERT_NE(std::string::npos, layersPos);
  nonObjectLayer.replace(layersPos, strlen("\"Layers\":[{"), "\"Layers\":[1,{");
  cases.push_back({"non-object layer", std::move(nonObjectLayer)});
  cases.push_back({"missing input", removeFirstWindowLayerField(valid, "input")});
  cases.push_back({"missing trigger", removeFirstWindowLayerField(valid, "trigger")});

  for (const auto& testCase : cases) {
    SCOPED_TRACE(testCase.name);
    expectCreateRequestDecodeFailure(testCase.encoded);
  }
}

TEST(StreamWindowPlanTest, createRequestJsonRejectsUnknownVersionAndExcessiveDepth) {
  const std::string valid = encodeNestedCountRequestJson(makeLegacyProjectedStateCountPlan());
  {
    SCOPED_TRACE("unknown version");
    expectCreateRequestDecodeFailure(
        replaceWindowPlanFieldWithInteger(valid, "Version", STREAM_WINDOW_PLAN_VERSION + 1));
  }
  {
    SCOPED_TRACE("depth greater than eight");
    PlanPtr maxDepth = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL,
                                 WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
    SStreamWindowLayerSpec* maxDepthLeaf = layer(maxDepth.get(), 7);
    taosMemoryFreeClear(maxDepthLeaf->trigger.count.condCols);
    maxDepthLeaf->trigger.count.sliding = 2;
    SStreamWindowPlanValidationCtx ctx = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(maxDepth.get(), &ctx));
    SCMCreateStreamReq projection = makeLegacyCountRequest();
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tValidateStreamWindowPlanLeafProjection(maxDepth.get(), projection.triggerType, &projection.trigger));

    PlanPtr deep = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL,
                             WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL, WINDOW_TYPE_INTERVAL,
                             WINDOW_TYPE_COUNT});
    SStreamWindowLayerSpec* deepLeaf = layer(deep.get(), 8);
    taosMemoryFreeClear(deepLeaf->trigger.count.condCols);
    deepLeaf->trigger.count.sliding = 2;
    expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(std::move(deep)));
  }
}

TEST(StreamWindowPlanTest, createRequestJsonRejectsIntrinsicallyInvalidWindowPlans) {
  {
    SCOPED_TRACE("non-leaf interval overlap");
    PlanPtr plan = makePlan({WINDOW_TYPE_INTERVAL, WINDOW_TYPE_COUNT});
    layer(plan.get(), 0)->trigger.sliding.sliding = 5;
    layer(plan.get(), 1)->trigger.count.sliding = 2;
    expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(std::move(plan)));
  }
  {
    SCOPED_TRACE("non-leaf count overlap");
    PlanPtr plan = makePlan({WINDOW_TYPE_COUNT, WINDOW_TYPE_COUNT});
    layer(plan.get(), 1)->trigger.count.sliding = 2;
    expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(std::move(plan)));
  }
  {
    SCOPED_TRACE("PERIOD layer");
    PlanPtr plan = makePlan({WINDOW_TYPE_PERIOD, WINDOW_TYPE_COUNT});
    layer(plan.get(), 1)->trigger.count.sliding = 2;
    expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(std::move(plan)));
  }
}

TEST(StreamWindowPlanTest, createRequestJsonRejectsLeafProjectionMismatch) {
  PlanPtr plan = makeLegacyProjectedStateCountPlan();
  ++layer(plan.get(), 1)->trigger.count.countVal;
  SStreamWindowPlanValidationCtx ctx = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateStreamWindowPlan(plan.get(), &ctx));

  expectCreateRequestDecodeFailure(encodeNestedCountRequestJson(std::move(plan)));
}

TEST(StreamWindowPlanTest, createRequestJsonRejectsMalformedStructuredWindowPlan) {
  SCMCreateStreamReq source = makeLegacyCountRequest();
  RequestPtr         sourceGuard(&source);
  PlanPtr            plan = makeLegacyProjectedStateCountPlan();
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(plan.get(), &source.pWindowPlan));

  const std::string                              encoded = encodeCreateStreamRequestJson(&source);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(encoded.c_str()), tjsonDelete);
  ASSERT_NE(nullptr, json);
  SJson* windowPlan = tjsonGetObjectItem(json.get(), "WindowPlan");
  ASSERT_NE(nullptr, windowPlan);
  tjsonDeleteItemFromObject(windowPlan, "Version");

  SCMCreateStreamReq decoded = {};
  RequestPtr         decodedGuard(&decoded);
  EXPECT_NE(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(json.get(), &decoded));
  EXPECT_EQ(nullptr, decoded.pWindowPlan);
}

TEST(StreamWindowPlanTest, subplanAncestorContextJsonAndCloneRoundTrip) {
  SNode* rawSubplan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &rawSubplan));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> rawGuard(rawSubplan, nodesDestroyNode);
  const std::string                                   withAncestorContext =
      addNestedJsonBoolean(encodeNodeJson(rawSubplan), "PhysiSubplan", "RequiresAncestorContext", true);

  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(withAncestorContext.c_str(), &decoded));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(decoded, nodesDestroyNode);
  const std::string                                   decodedJson = encodeNodeJson(decoded);
  ASSERT_NE(std::string::npos, decodedJson.find("\"RequiresAncestorContext\""));

  SNode* cloned = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(decoded, &cloned));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> clonedGuard(cloned, nodesDestroyNode);
  const std::string                                   clonedJson = encodeNodeJson(cloned);
  EXPECT_NE(std::string::npos, clonedJson.find("\"RequiresAncestorContext\""));
}

TEST(StreamWindowPlanTest, subplanAncestorContextLegacyOmissionRemainsFalse) {
  SNode* rawSubplan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &rawSubplan));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> rawGuard(rawSubplan, nodesDestroyNode);
  const std::string                                   legacy = encodeNodeJson(rawSubplan);
  ASSERT_EQ(std::string::npos, legacy.find("\"RequiresAncestorContext\""));

  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(legacy.c_str(), &decoded));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(decoded, nodesDestroyNode);
  EXPECT_EQ(legacy, encodeNodeJson(decoded));

  SNode* cloned = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(decoded, &cloned));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> clonedGuard(cloned, nodesDestroyNode);
  EXPECT_EQ(legacy, encodeNodeJson(cloned));
}

TEST(StreamWindowPlanTest, tempTableExplicitAliasJsonAndCloneRoundTrip) {
  SNode* rawTempTable = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_TEMP_TABLE, &rawTempTable));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> rawGuard(rawTempTable, nodesDestroyNode);
  const std::string                                   withExplicitAlias =
      addNestedJsonBoolean(encodeNodeJson(rawTempTable), "TempTable", "HasExplicitAlias", true);

  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(withExplicitAlias.c_str(), &decoded));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(decoded, nodesDestroyNode);
  const std::string                                   decodedJson = encodeNodeJson(decoded);
  ASSERT_NE(std::string::npos, decodedJson.find("\"HasExplicitAlias\""));

  SNode* cloned = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(decoded, &cloned));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> clonedGuard(cloned, nodesDestroyNode);
  const std::string                                   clonedJson = encodeNodeJson(cloned);
  EXPECT_NE(std::string::npos, clonedJson.find("\"HasExplicitAlias\""));
}

TEST(StreamWindowPlanTest, tempTableExplicitAliasLegacyOmissionRemainsFalse) {
  SNode* rawTempTable = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_TEMP_TABLE, &rawTempTable));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> rawGuard(rawTempTable, nodesDestroyNode);
  const std::string                                   legacy = encodeNodeJson(rawTempTable);
  ASSERT_EQ(std::string::npos, legacy.find("\"HasExplicitAlias\""));

  SNode* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(legacy.c_str(), &decoded));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(decoded, nodesDestroyNode);
  EXPECT_EQ(legacy, encodeNodeJson(decoded));

  SNode* cloned = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(decoded, &cloned));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> clonedGuard(cloned, nodesDestroyNode);
  EXPECT_EQ(legacy, encodeNodeJson(cloned));
}

TEST(StreamWindowPlanTest, streamDeployTaskTypesMatchSlots) {
  const struct {
    std::array<EStreamTaskType, 3> types;
    bool                           succeeds;
  } cases[] = {
      {{STREAM_READER_TASK, STREAM_TRIGGER_TASK, STREAM_RUNNER_TASK}, true},
      {{STREAM_TRIGGER_TASK, STREAM_TRIGGER_TASK, STREAM_RUNNER_TASK}, false},
      {{STREAM_RUNNER_TASK, STREAM_TRIGGER_TASK, STREAM_RUNNER_TASK}, false},
      {{STREAM_READER_TASK, STREAM_READER_TASK, STREAM_RUNNER_TASK}, false},
      {{STREAM_READER_TASK, STREAM_RUNNER_TASK, STREAM_RUNNER_TASK}, false},
      {{STREAM_READER_TASK, STREAM_TRIGGER_TASK, STREAM_READER_TASK}, false},
      {{STREAM_READER_TASK, STREAM_TRIGGER_TASK, STREAM_TRIGGER_TASK}, false},
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(::testing::Message() << "reader=" << static_cast<int>(testCase.types[0])
                                      << " trigger=" << static_cast<int>(testCase.types[1])
                                      << " runner=" << static_cast<int>(testCase.types[2]));
    SStmStreamDeploy                                                        source = {};
    std::unique_ptr<SStmStreamDeploy, decltype(&tDeepFreeSStmStreamDeploy)> sourceGuard(&source,
                                                                                        tDeepFreeSStmStreamDeploy);
    source.streamId = 42;
    source.readerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), 1);
    source.triggerTask = static_cast<SStmTaskDeploy*>(taosMemoryCalloc(1, sizeof(SStmTaskDeploy)));
    source.runnerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), 1);
    ASSERT_NE(nullptr, source.readerTasks);
    ASSERT_NE(nullptr, source.triggerTask);
    ASSERT_NE(nullptr, source.runnerTasks);
    const std::array<SStmTaskDeploy*, 3> tasks = {static_cast<SStmTaskDeploy*>(taosArrayGet(source.readerTasks, 0)),
                                                  source.triggerTask,
                                                  static_cast<SStmTaskDeploy*>(taosArrayGet(source.runnerTasks, 0))};
    for (size_t i = 0; i < tasks.size(); ++i) {
      SStmTaskDeploy* task = tasks[i];
      task->task.type = testCase.types[i];
      task->task.streamId = source.streamId;
      task->task.taskId = i + 1;
      switch (task->task.type) {
        case STREAM_READER_TASK:
          task->msg.reader.msg.calc.calcScanPlan = dupText("reader_plan");
          break;
        case STREAM_TRIGGER_TASK:
          task->msg.trigger.triggerType = WINDOW_TYPE_COUNT;
          task->msg.trigger.trigger.count.countVal = 2;
          task->msg.trigger.trigger.count.sliding = 2;
          task->msg.trigger.streamName = dupText("slot_stream");
          break;
        case STREAM_RUNNER_TASK:
          task->msg.runner.streamName = dupText("slot_stream");
          task->msg.runner.pPlan = dupText("runner_plan");
          break;
      }
    }

    SEncoder sizer = {};
    tEncoderInit(&sizer, nullptr, 0);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeSStmStreamDeploy(&sizer, &source));
    std::vector<uint8_t> bytes(sizer.pos);
    tEncoderClear(&sizer);
    SEncoder encoder = {};
    tEncoderInit(&encoder, bytes.data(), bytes.size());
    ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeSStmStreamDeploy(&encoder, &source));
    tEncoderClear(&encoder);

    SStmStreamDeploy decoded = {};
    // Both successful and partial decodes own the union selected by task.type.
    std::unique_ptr<SStmStreamDeploy, decltype(&tDeepFreeSStmStreamDeploy)> decodedGuard(&decoded,
                                                                                         tDeepFreeSStmStreamDeploy);
    SDecoder                                                                decoder = {};
    tDecoderInit(&decoder, bytes.data(), bytes.size());
    const int32_t code = tDecodeSStmStreamDeploy(&decoder, &decoded);
    tDecoderClear(&decoder);
    EXPECT_EQ(testCase.succeeds, code == TSDB_CODE_SUCCESS);
    if (testCase.succeeds) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, code);
      EXPECT_EQ(42, decoded.streamId);
      ASSERT_EQ(1, taosArrayGetSize(decoded.readerTasks));
      ASSERT_NE(nullptr, decoded.triggerTask);
      ASSERT_EQ(1, taosArrayGetSize(decoded.runnerTasks));
      const auto* reader = static_cast<const SStmTaskDeploy*>(taosArrayGet(decoded.readerTasks, 0));
      const auto* runner = static_cast<const SStmTaskDeploy*>(taosArrayGet(decoded.runnerTasks, 0));
      EXPECT_EQ(STREAM_READER_TASK, reader->task.type);
      EXPECT_EQ(STREAM_TRIGGER_TASK, decoded.triggerTask->task.type);
      EXPECT_EQ(STREAM_RUNNER_TASK, runner->task.type);
      EXPECT_STREQ("reader_plan", static_cast<const char*>(reader->msg.reader.msg.calc.calcScanPlan));
      EXPECT_STREQ("slot_stream", decoded.triggerTask->msg.trigger.streamName);
      EXPECT_STREQ("runner_plan", static_cast<const char*>(runner->msg.runner.pPlan));
    }
  }
}

SMStreamHbRspMsg makeHeartbeatWithNestedTrigger() {
  SMStreamHbRspMsg source = {};
  source.undeploy.undeployAll = 1;
  source.deploy.streamList = taosArrayInit_s(sizeof(SStmStreamDeploy), 1);
  EXPECT_NE(nullptr, source.deploy.streamList);

  auto* stream = static_cast<SStmStreamDeploy*>(taosArrayGet(source.deploy.streamList, 0));
  EXPECT_NE(nullptr, stream);
  stream->streamId = 42;
  stream->triggerTask = static_cast<SStmTaskDeploy*>(taosMemoryCalloc(1, sizeof(SStmTaskDeploy)));
  EXPECT_NE(nullptr, stream->triggerTask);
  stream->triggerTask->task.type = STREAM_TRIGGER_TASK;
  stream->triggerTask->task.streamId = stream->streamId;
  stream->triggerTask->task.taskId = 7;
  stream->triggerTask->msg.trigger.triggerType = WINDOW_TYPE_COUNT;
  stream->triggerTask->msg.trigger.trigger.count.countVal = 2;
  stream->triggerTask->msg.trigger.trigger.count.sliding = 2;
  stream->triggerTask->msg.trigger.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  stream->triggerTask->msg.trigger.streamName = dupText("nested_stream");
  PlanPtr plan = makePlan({WINDOW_TYPE_SESSION, WINDOW_TYPE_COUNT});
  layer(plan.get(), 1)->trigger.count.sliding = 2;
  stream->triggerTask->msg.trigger.pWindowPlan = plan.release();
  return source;
}

SStreamTriggerDeployMsg* onlyTriggerDeploy(SMStreamHbRspMsg* heartbeat) {
  if (heartbeat == nullptr || taosArrayGetSize(heartbeat->deploy.streamList) != 1) return nullptr;
  auto* stream = static_cast<SStmStreamDeploy*>(taosArrayGet(heartbeat->deploy.streamList, 0));
  return stream == nullptr || stream->triggerTask == nullptr ? nullptr : &stream->triggerTask->msg.trigger;
}

std::vector<uint8_t> encodeHeartbeat(const SMStreamHbRspMsg& heartbeat) {
  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamHbRsp(&sizer, &heartbeat));
  std::vector<uint8_t> data(sizer.pos);
  tEncoderClear(&sizer);

  SEncoder encoder = {};
  tEncoderInit(&encoder, data.data(), data.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamHbRsp(&encoder, &heartbeat));
  tEncoderClear(&encoder);
  return data;
}

std::vector<uint8_t> encodeHeartbeatLegacyBody(const SMStreamHbRspMsg& heartbeat) {
  EXPECT_EQ(0, taosArrayGetSize(heartbeat.start.taskList));
  EXPECT_EQ(0, taosArrayGetSize(heartbeat.undeploy.taskList));
  EXPECT_EQ(0, taosArrayGetSize(heartbeat.rsps.rspList));

  auto encode = [&heartbeat](SEncoder* encoder) {
    int32_t code = tStartEncode(encoder);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI32(encoder, heartbeat.streamGId);
    const int32_t deployNum = taosArrayGetSize(heartbeat.deploy.streamList);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI32(encoder, deployNum);
    for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < deployNum; ++i) {
      code = tEncodeSStmStreamDeploy(
          encoder, static_cast<const SStmStreamDeploy*>(taosArrayGet(heartbeat.deploy.streamList, i)));
    }
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI32(encoder, 0);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI8(encoder, heartbeat.undeploy.undeployAll);
    if (code == TSDB_CODE_SUCCESS && !heartbeat.undeploy.undeployAll) code = tEncodeI32(encoder, 0);
    if (code == TSDB_CODE_SUCCESS) code = tEncodeI32(encoder, 0);
    tEndEncode(encoder);
    return code;
  };

  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, encode(&sizer));
  std::vector<uint8_t> data(sizer.pos);
  tEncoderClear(&sizer);

  SEncoder encoder = {};
  tEncoderInit(&encoder, data.data(), data.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, encode(&encoder));
  tEncoderClear(&encoder);
  return data;
}

int32_t decodeHeartbeat(const std::vector<uint8_t>& data, SMStreamHbRspMsg* heartbeat) {
  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(data.data()), data.size());
  const int32_t code = tDecodeStreamHbRsp(&decoder, heartbeat);
  tDecoderClear(&decoder);
  return code;
}

size_t heartbeatWindowPlanFrameOffset(const std::vector<uint8_t>& data) {
  for (size_t i = sizeof(int32_t); i + sizeof(uint32_t) <= data.size(); ++i) {
    uint32_t magic = 0;
    memcpy(&magic, data.data() + i, sizeof(magic));
    if (magic == STREAM_WINDOW_PLAN_FRAME_MAGIC) return i;
  }
  return data.size();
}

void rewriteHeartbeatLength(std::vector<uint8_t>* data) {
  writeI32(data, 0, static_cast<int32_t>(data->size() - sizeof(int32_t)));
}

void writeU16(std::vector<uint8_t>* data, size_t offset, uint16_t value) {
  ASSERT_NE(nullptr, data);
  ASSERT_LE(offset + sizeof(value), data->size());
  memcpy(data->data() + offset, &value, sizeof(value));
}

void writeU32(std::vector<uint8_t>* data, size_t offset, uint32_t value) {
  ASSERT_NE(nullptr, data);
  ASSERT_LE(offset + sizeof(value), data->size());
  memcpy(data->data() + offset, &value, sizeof(value));
}

std::vector<uint8_t> makeUnknownHeartbeatFrame() {
  std::vector<uint8_t> frame(sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2 + 3);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, frame.data(), frame.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU32(&encoder, UINT32_C(0x554e4b4e)));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU16(&encoder, 9));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU16(&encoder, 7));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU32(&encoder, 3));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 1));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 2));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 3));
  tEncoderClear(&encoder);
  return frame;
}

void expectHeartbeatDecodeFailure(const std::vector<uint8_t>& bytes) {
  SMStreamHbRspMsg decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, decodeHeartbeat(bytes, &decoded));
  tDeepFreeSMStreamHbRspMsg(&decoded);
}

TEST(StreamWindowPlanTest, heartbeatBindsPlanAfterLegacyBody) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  const auto       bytes = encodeHeartbeat(source);
  const size_t     frameOffset = heartbeatWindowPlanFrameOffset(bytes);
  ASSERT_LT(frameOffset, bytes.size());
  auto legacy = encodeHeartbeatLegacyBody(source);
  ASSERT_EQ(frameOffset, legacy.size());
  // The enclosing length includes tail frames; normalize it before comparing the legacy body.
  memcpy(legacy.data(), bytes.data(), sizeof(int32_t));
  EXPECT_EQ(legacy, std::vector<uint8_t>(bytes.begin(), bytes.begin() + frameOffset));

  SMStreamHbRspMsg decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodeHeartbeat(bytes, &decoded));
  ASSERT_NE(nullptr, onlyTriggerDeploy(&decoded));
  ASSERT_NE(nullptr, onlyTriggerDeploy(&decoded)->pWindowPlan);
  expectPlanEquals(onlyTriggerDeploy(&source)->pWindowPlan, onlyTriggerDeploy(&decoded)->pWindowPlan);

  tDeepFreeSMStreamHbRspMsg(&decoded);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsRequiredMissingPlan) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  auto             bytes = encodeHeartbeat(source);
  bytes.resize(heartbeatWindowPlanFrameOffset(bytes));
  rewriteHeartbeatLength(&bytes);
  expectHeartbeatDecodeFailure(bytes);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatSkipsUnknownFramesBeforeAndAfterWindowPlan) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  const auto       original = encodeHeartbeat(source);
  const size_t     frameOffset = heartbeatWindowPlanFrameOffset(original);
  const auto       unknown = makeUnknownHeartbeatFrame();

  for (bool before : {true, false}) {
    auto bytes = original;
    bytes.insert(bytes.begin() + (before ? frameOffset : bytes.size()), unknown.begin(), unknown.end());
    rewriteHeartbeatLength(&bytes);
    SMStreamHbRspMsg decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, decodeHeartbeat(bytes, &decoded));
    ASSERT_NE(nullptr, onlyTriggerDeploy(&decoded)->pWindowPlan);
    tDeepFreeSMStreamHbRspMsg(&decoded);
  }
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsUnknownKnownFrameVersionOrFlags) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  const auto       original = encodeHeartbeat(source);
  const size_t     offset = heartbeatWindowPlanFrameOffset(original);

  auto badVersion = original;
  writeU16(&badVersion, offset + sizeof(uint32_t), STREAM_WINDOW_PLAN_FRAME_VERSION + 1);
  expectHeartbeatDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, offset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectHeartbeatDecodeFailure(badFlags);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsDuplicateWindowPlanFrame) {
  SMStreamHbRspMsg           source = makeHeartbeatWithNestedTrigger();
  auto                       bytes = encodeHeartbeat(source);
  const size_t               offset = heartbeatWindowPlanFrameOffset(bytes);
  const std::vector<uint8_t> duplicateFrame(bytes.begin() + offset, bytes.end());
  bytes.insert(bytes.end(), duplicateFrame.begin(), duplicateFrame.end());
  rewriteHeartbeatLength(&bytes);
  expectHeartbeatDecodeFailure(bytes);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsTruncatedFrameAndUnconsumedPayload) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  const auto       original = encodeHeartbeat(source);
  const size_t     offset = heartbeatWindowPlanFrameOffset(original);

  auto truncated = original;
  truncated.pop_back();
  rewriteHeartbeatLength(&truncated);
  expectHeartbeatDecodeFailure(truncated);

  auto trailing = original;
  trailing.push_back(0x5a);
  uint32_t payloadLength = 0;
  memcpy(&payloadLength, trailing.data() + offset + sizeof(uint32_t) + sizeof(uint16_t) * 2, sizeof(payloadLength));
  writeU32(&trailing, offset + sizeof(uint32_t) + sizeof(uint16_t) * 2, payloadLength + 1);
  rewriteHeartbeatLength(&trailing);
  expectHeartbeatDecodeFailure(trailing);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsDuplicateOrMissingTargetsAndExcessEntries) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  const auto       original = encodeHeartbeat(source);
  const size_t     offset = heartbeatWindowPlanFrameOffset(original);
  constexpr size_t headerSize = sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
  const size_t     entryOffset = offset + headerSize + sizeof(uint32_t);

  auto                       duplicate = original;
  const std::vector<uint8_t> duplicateEntry(duplicate.begin() + entryOffset, duplicate.end());
  duplicate.insert(duplicate.end(), duplicateEntry.begin(), duplicateEntry.end());
  writeU32(&duplicate, offset + headerSize, 2);
  writeU32(&duplicate, offset + sizeof(uint32_t) + sizeof(uint16_t) * 2,
           static_cast<uint32_t>(duplicate.size() - offset - headerSize));
  rewriteHeartbeatLength(&duplicate);
  expectHeartbeatDecodeFailure(duplicate);

  auto    missing = original;
  int64_t unknownStream = 99;
  memcpy(missing.data() + entryOffset, &unknownStream, sizeof(unknownStream));
  expectHeartbeatDecodeFailure(missing);

  auto excess = original;
  writeU32(&excess, offset + headerSize, 2);
  expectHeartbeatDecodeFailure(excess);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatRejectsDuplicateNestedDeployTargets) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  auto*            first = static_cast<SStmStreamDeploy*>(taosArrayGet(source.deploy.streamList, 0));
  ASSERT_NE(nullptr, first);

  SStmStreamDeploy duplicate = {};
  duplicate.streamId = first->streamId;
  duplicate.triggerTask = static_cast<SStmTaskDeploy*>(taosMemoryCalloc(1, sizeof(SStmTaskDeploy)));
  ASSERT_NE(nullptr, duplicate.triggerTask);
  duplicate.triggerTask->task = first->triggerTask->task;
  duplicate.triggerTask->msg.trigger = first->triggerTask->msg.trigger;
  duplicate.triggerTask->msg.trigger.streamName = dupText(first->triggerTask->msg.trigger.streamName);
  duplicate.triggerTask->msg.trigger.pWindowPlan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamWindowPlan(first->triggerTask->msg.trigger.pWindowPlan,
                                                      &duplicate.triggerTask->msg.trigger.pWindowPlan));
  const size_t entrySize = sizeof(int64_t) * 2 + encodePlan(first->triggerTask->msg.trigger.pWindowPlan).size();
  ASSERT_NE(nullptr, taosArrayPush(source.deploy.streamList, &duplicate));

  auto             bytes = encodeHeartbeat(source);
  const size_t     offset = heartbeatWindowPlanFrameOffset(bytes);
  constexpr size_t headerSize = sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
  bytes.resize(offset + headerSize + sizeof(uint32_t) + entrySize);
  writeU32(&bytes, offset + headerSize, 1);
  writeU32(&bytes, offset + sizeof(uint32_t) + sizeof(uint16_t) * 2,
           static_cast<uint32_t>(sizeof(uint32_t) + entrySize));
  rewriteHeartbeatLength(&bytes);

  expectHeartbeatDecodeFailure(bytes);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatFailureDoesNotPartiallyBindEarlierEntry) {
  SMStreamHbRspMsg     source = makeHeartbeatWithNestedTrigger();
  auto                 bytes = encodeHeartbeat(source);
  const size_t         offset = heartbeatWindowPlanFrameOffset(bytes);
  constexpr size_t     headerSize = sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
  const size_t         entryOffset = offset + headerSize + sizeof(uint32_t);
  std::vector<uint8_t> second(bytes.begin() + entryOffset, bytes.end());
  int64_t              unknownStream = 99;
  memcpy(second.data(), &unknownStream, sizeof(unknownStream));
  bytes.insert(bytes.end(), second.begin(), second.end());
  writeU32(&bytes, offset + headerSize, 2);
  writeU32(&bytes, offset + sizeof(uint32_t) + sizeof(uint16_t) * 2,
           static_cast<uint32_t>(bytes.size() - offset - headerSize));
  rewriteHeartbeatLength(&bytes);

  SMStreamHbRspMsg decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, decodeHeartbeat(bytes, &decoded));
  ASSERT_NE(nullptr, onlyTriggerDeploy(&decoded));
  EXPECT_EQ(nullptr, onlyTriggerDeploy(&decoded)->pWindowPlan);
  tDeepFreeSMStreamHbRspMsg(&decoded);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatBindsWireValidPlanForDeployValidation) {
  SMStreamHbRspMsg source = makeHeartbeatWithNestedTrigger();
  auto*            pOuter =
      static_cast<SStreamWindowLayerSpec*>(taosArrayGet(onlyTriggerDeploy(&source)->pWindowPlan->pLayers, 0));
  pOuter->triggerType = WINDOW_TYPE_INTERVAL;
  pOuter->trigger = {};
  pOuter->trigger.sliding.interval = 20;
  pOuter->trigger.sliding.sliding = 10;

  const auto       bytes = encodeHeartbeat(source);
  SMStreamHbRspMsg decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodeHeartbeat(bytes, &decoded));
  ASSERT_NE(nullptr, onlyTriggerDeploy(&decoded)->pWindowPlan);
  const auto* pDecodedOuter =
      static_cast<const SStreamWindowLayerSpec*>(taosArrayGet(onlyTriggerDeploy(&decoded)->pWindowPlan->pLayers, 0));
  EXPECT_EQ(WINDOW_TYPE_INTERVAL, pDecodedOuter->triggerType);
  EXPECT_EQ(20, pDecodedOuter->trigger.sliding.interval);
  EXPECT_EQ(10, pDecodedOuter->trigger.sliding.sliding);

  tDeepFreeSMStreamHbRspMsg(&decoded);
  tDeepFreeSMStreamHbRspMsg(&source);
}

TEST(StreamWindowPlanTest, heartbeatPreservesSingleLayerGoldenBytes) {
  SMStreamHbRspMsg legacy = makeHeartbeatWithNestedTrigger();
  auto*            trigger = onlyTriggerDeploy(&legacy);
  ASSERT_NE(nullptr, trigger);
  trigger->addOptions &= ~STREAM_OPTION_NESTED_WINDOW_PLAN;
  tDestroyStreamWindowPlan(&trigger->pWindowPlan);

  const auto            bytes = encodeHeartbeat(legacy);
  static constexpr char golden[] =
      "\xef\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x2a\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00"
      "\x2a\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00"
      "\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x0e\x6e\x65\x73\x74\x65\x64\x5f\x73\x74\x72"
      "\x65\x61\x6d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00"
      "\x00\x00\x00";
  EXPECT_EQ(std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(golden),
                                 reinterpret_cast<const uint8_t*>(golden) + sizeof(golden) - 1),
            bytes);
  tDeepFreeSMStreamHbRspMsg(&legacy);
}

static_assert(offsetof(SSTriggerCalcParam, notifyType) == 40,
              "SSTriggerCalcParam::notifyType offset changed");

SStreamContextPolicyEntry makeContextPolicyEntry(int64_t gid, int32_t paramIndex, int8_t contextPolicy) {
  SStreamContextPolicyEntry entry = {};
  entry.gid = gid;
  entry.paramIndex = paramIndex;
  entry.contextPolicy = contextPolicy;
  return entry;
}

SScopeInstanceId makeScopeInstanceId(int32_t layerIndex, int8_t triggerType, TSKEY openingTs,
                                     int64_t nativeDiscriminator) {
  SScopeInstanceId scope = {};
  scope.layerIndex = layerIndex;
  scope.triggerType = triggerType;
  scope.openingTs = openingTs;
  scope.nativeDiscriminator = nativeDiscriminator;
  return scope;
}

SWindowAncestorSnapshot makeSlidingSnapshot(int32_t layerIndex, int8_t triggerType, int64_t placeholderMask,
                                            TSKEY prevTs, TSKEY currentTs, TSKEY nextTs) {
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = layerIndex;
  snapshot.triggerType = triggerType;
  snapshot.placeholderMask = placeholderMask;
  snapshot.values.sliding.prevTs = prevTs;
  snapshot.values.sliding.currentTs = currentTs;
  snapshot.values.sliding.nextTs = nextTs;
  return snapshot;
}

SWindowAncestorSnapshot makeWindowSnapshot(int32_t layerIndex, int8_t triggerType, int64_t placeholderMask, TSKEY start,
                                           TSKEY end = 0, int64_t duration = 0, int64_t rownum = 0) {
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = layerIndex;
  snapshot.triggerType = triggerType;
  snapshot.placeholderMask = placeholderMask;
  snapshot.values.window.start = start;
  snapshot.values.window.end = end;
  snapshot.values.window.duration = duration;
  snapshot.values.window.rownum = rownum;
  return snapshot;
}

SSTriggerCalcParam makeWindowCalcParam(int64_t wstart, int64_t wend, int32_t notifyType) {
  SSTriggerCalcParam param = {};
  param.wstart = wstart;
  param.wend = wend;
  param.notifyType = notifyType;
  return param;
}

SStreamReadScopeBinding makeReadScopeBinding(int32_t vgId, int32_t readInfoIndex, int64_t gid) {
  SStreamReadScopeBinding binding = {};
  binding.vgId = vgId;
  binding.readInfoIndex = readInfoIndex;
  binding.scope.gid = gid;
  return binding;
}

SStreamContextPolicy* makeContextPolicy(std::initializer_list<SStreamContextPolicyEntry> entries = {}) {
  auto* policy = static_cast<SStreamContextPolicy*>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
  EXPECT_NE(nullptr, policy);
  if (policy == nullptr) return nullptr;
  policy->pEntries = taosArrayInit(entries.size() == 0 ? 1 : entries.size(), sizeof(SStreamContextPolicyEntry));
  EXPECT_NE(nullptr, policy->pEntries);
  if (policy->pEntries == nullptr) return policy;
  for (const auto& entry : entries) {
    EXPECT_NE(nullptr, taosArrayPush(policy->pEntries, &entry));
  }
  return policy;
}

void setSingleAncestorPolicy(SSTriggerCalcRequest* request, int32_t paramIndex = 0) {
  request->pContextPolicy =
      makeContextPolicy({makeContextPolicyEntry(request->gid, paramIndex, STREAM_CONTEXT_POLICY_ANCESTOR)});
}

SStreamAncestorContext* makeAncestorContext(int64_t gid, int32_t paramIndex = 0) {
  auto* context = static_cast<SStreamAncestorContext*>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(nullptr, context);
  if (context == nullptr) return nullptr;

  context->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(nullptr, context->pParamContexts);
  if (context->pParamContexts == nullptr) return context;

  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.openingTs = 1000;
  param.leafIdentity.nativeDiscriminator = 17;
  param.leafIdentity.lineage.pScopes = taosArrayInit(2, sizeof(SScopeInstanceId));
  EXPECT_NE(nullptr, param.leafIdentity.lineage.pScopes);
  const SScopeInstanceId root = makeScopeInstanceId(0, WINDOW_TYPE_INTERVAL, 0, 10);
  const SScopeInstanceId parent = makeScopeInstanceId(1, WINDOW_TYPE_SESSION, 500, 11);
  EXPECT_NE(nullptr, taosArrayPush(param.leafIdentity.lineage.pScopes, &root));
  EXPECT_NE(nullptr, taosArrayPush(param.leafIdentity.lineage.pScopes, &parent));
  param.pSnapshots = taosArrayInit(2, sizeof(SWindowAncestorSnapshot));
  EXPECT_NE(nullptr, param.pSnapshots);
  const SWindowAncestorSnapshot rootSnapshot = makeSlidingSnapshot(
      0, WINDOW_TYPE_INTERVAL, PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS, -1, 0, 100);
  const SWindowAncestorSnapshot parentSnapshot = makeWindowSnapshot(
      1, WINDOW_TYPE_SESSION, PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WROWNUM, 500, 999, 500, 9);
  EXPECT_NE(nullptr, taosArrayPush(param.pSnapshots, &rootSnapshot));
  EXPECT_NE(nullptr, taosArrayPush(param.pSnapshots, &parentSnapshot));
  EXPECT_NE(nullptr, taosArrayPush(context->pParamContexts, &param));
  return context;
}

SWindowAncestorSnapshot* firstAncestorSnapshot(SStreamAncestorContext* context) {
  if (context == nullptr) return nullptr;
  auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(context->pParamContexts, 0));
  return param == nullptr ? nullptr : static_cast<SWindowAncestorSnapshot*>(taosArrayGet(param->pSnapshots, 0));
}

SSTriggerCalcRequest makeSingleGroupCalcRequest(int32_t notifyType = BIT_FLAG_MASK(0)) {
  SSTriggerCalcRequest request = {};
  request.streamId = 11;
  request.runnerTaskId = 22;
  request.sessionId = 33;
  request.isWindowTrigger = true;
  request.precision = TSDB_TIME_PRECISION_MILLI;
  request.triggerType = STREAM_TRIGGER_COUNT;
  request.gid = 101;
  request.params = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  EXPECT_NE(nullptr, request.params);
  SSTriggerCalcParam param = makeWindowCalcParam(1000, 1099, notifyType);
  param.wduration = 100;
  param.wrownum = 7;
  param.triggerTime = 1200;
  EXPECT_NE(nullptr, taosArrayPush(request.params, &param));
  return request;
}

std::vector<uint8_t> serializeCalcRequest(const SSTriggerCalcRequest& request) {
  const int32_t size = tSerializeSTriggerCalcRequest(nullptr, 0, &request);
  EXPECT_GT(size, 0);
  if (size <= 0) return {};
  std::vector<uint8_t> bytes(size);
  EXPECT_EQ(size, tSerializeSTriggerCalcRequest(bytes.data(), bytes.size(), &request));
  return bytes;
}

std::vector<uint8_t> legacyCreateOnlyCalcRequestWire() {
  static constexpr uint8_t wire[] = {
      0x35, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
  };
  return {std::begin(wire), std::end(wire)};
}

void appendAncestorParamContext(SStreamAncestorContext* destination, int64_t gid, int32_t paramIndex);
void appendReadBindingForParam(SStreamAncestorContext* context, int32_t vgId, int32_t readInfoIndex,
                               int32_t paramContextIndex);
void installActualNodeReadView(SStreamRuntimeFuncInfo* runtime, int32_t nodeId);

bool lineagesEqual(const SWindowLineage* left, const SWindowLineage* right) {
  const int32_t size = taosArrayGetSize(left == nullptr ? nullptr : left->pScopes);
  if (size != taosArrayGetSize(right == nullptr ? nullptr : right->pScopes)) return false;
  for (int32_t i = 0; i < size; ++i) {
    const auto* lhs = static_cast<const SScopeInstanceId*>(taosArrayGet(left->pScopes, i));
    const auto* rhs = static_cast<const SScopeInstanceId*>(taosArrayGet(right->pScopes, i));
    if (lhs == nullptr || rhs == nullptr || lhs->layerIndex != rhs->layerIndex ||
        lhs->triggerType != rhs->triggerType || lhs->openingTs != rhs->openingTs ||
        lhs->nativeDiscriminator != rhs->nativeDiscriminator) {
      return false;
    }
  }
  return true;
}

std::vector<uint8_t> serializeFetchRequest(SResFetchReq& request, bool needStreamRtInfo, bool needStreamGrpInfo) {
  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &request, needStreamRtInfo, needStreamGrpInfo);
  EXPECT_GT(size, 0);
  if (size <= 0) return {};
  std::vector<uint8_t> bytes(size);
  EXPECT_EQ(size, tSerializeSResFetchReq(bytes.data(), bytes.size(), &request, needStreamRtInfo, needStreamGrpInfo));
  return bytes;
}

void rewriteFetchRequestLengths(std::vector<uint8_t>* bytes) {
  ASSERT_NE(nullptr, bytes);
  writeI32(bytes, sizeof(SMsgHead), static_cast<int32_t>(bytes->size() - sizeof(SMsgHead) - sizeof(int32_t)));
  reinterpret_cast<SMsgHead*>(bytes->data())->contLen = htonl(bytes->size());
}

SStreamRuntimeFuncInfo makeSingleGroupFetchRuntime(int32_t paramCount = 1) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.groupId = 101;
  runtime.curIdx = paramCount - 1;
  runtime.sessionId = 33;
  runtime.triggerType = STREAM_TRIGGER_COUNT;
  runtime.isWindowTrigger = true;
  runtime.precision = TSDB_TIME_PRECISION_MILLI;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.pStreamPesudoFuncVals = taosArrayInit(paramCount, sizeof(SSTriggerCalcParam));
  EXPECT_NE(nullptr, runtime.pStreamPesudoFuncVals);
  for (int32_t i = 0; i < paramCount; ++i) {
    SSTriggerCalcParam param = makeWindowCalcParam(300 + i * 100, 399 + i * 100, BIT_FLAG_MASK(0));
    EXPECT_NE(nullptr, taosArrayPush(runtime.pStreamPesudoFuncVals, &param));
  }
  runtime.pContextPolicy = makeContextPolicy();
  runtime.pAncestorContext = makeAncestorContext(runtime.groupId);
  auto* snapshot = firstAncestorSnapshot(runtime.pAncestorContext);
  EXPECT_NE(nullptr, snapshot);
  if (snapshot != nullptr) {
    snapshot->placeholderMask = PLACE_HOLDER_WSTART;
    snapshot->values = {};
    snapshot->values.window.start = 100;
  }
  for (int32_t i = 1; i < paramCount; ++i) {
    appendAncestorParamContext(runtime.pAncestorContext, runtime.groupId, i);
  }
  for (int32_t i = 0; i < paramCount; ++i) {
    const SStreamContextPolicyEntry entry = makeContextPolicyEntry(runtime.groupId, i, STREAM_CONTEXT_POLICY_ANCESTOR);
    EXPECT_NE(nullptr, taosArrayPush(runtime.pContextPolicy->pEntries, &entry));
  }
  return runtime;
}

void expectFetchDecodeFailure(const std::vector<uint8_t>& bytes) {
  SResFetchReq decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  tDestroySResFetchReq(&decoded);
}

size_t frameOffset(const std::vector<uint8_t>& bytes, uint32_t expectedMagic) {
  for (size_t i = sizeof(int32_t); i + sizeof(uint32_t) <= bytes.size(); ++i) {
    uint32_t magic = 0;
    memcpy(&magic, bytes.data() + i, sizeof(magic));
    if (magic == expectedMagic) return i;
  }
  return bytes.size();
}

size_t ancestorFrameOffset(const std::vector<uint8_t>& bytes) {
  return frameOffset(bytes, STREAM_ANCESTOR_FRAME_MAGIC);
}

size_t policyFrameOffset(const std::vector<uint8_t>& bytes) {
  return frameOffset(bytes, STREAM_CONTEXT_POLICY_FRAME_MAGIC);
}

void expectCalcRequestDecodeFailure(const std::vector<uint8_t>& bytes) {
  SSTriggerCalcRequest decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tDeserializeSTriggerCalcRequest(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  tDestroySTriggerCalcRequest(&decoded);
}

struct GroupedCalcWireOffsets {
  size_t              calcGroupCount = 0;
  std::vector<size_t> calcGids;
  size_t              vnodeCount = 0;
  std::vector<size_t> vnodeGids;
  std::vector<size_t> readEntryCounts;
  std::vector<size_t> tableCounts;
};

GroupedCalcWireOffsets locateGroupedCalcWireOffsets(const std::vector<uint8_t>& bytes) {
  GroupedCalcWireOffsets offsets = {};
  SDecoder               decoder = {};
  const size_t           framePrefixSize = sizeof(int32_t);
  tDecoderInit(&decoder, const_cast<uint8_t*>(bytes.data()), bytes.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tStartDecode(&decoder));
  int64_t i64 = 0;
  int32_t i32 = 0;
  int8_t  i8 = 0;
  for (int32_t i = 0; i < 3; ++i) EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI64(&decoder, &i64));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &i32));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI8(&decoder, &i8));
  EXPECT_TRUE(i8);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI8(&decoder, &i8));
  offsets.calcGroupCount = framePrefixSize + decoder.pos;
  int32_t calcGroups = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &calcGroups));
  for (int32_t i = 0; i < calcGroups; ++i) {
    offsets.calcGids.push_back(framePrefixSize + decoder.pos);
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI64(&decoder, &i64));
    int32_t params = 0;
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &params));
    for (int32_t j = 0; j < params; ++j) {
      SSTriggerCalcParam param = {};
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeFixed(&decoder, &param, offsetof(SSTriggerCalcParam, notifyType)));
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &i32));
      uint8_t* content = nullptr;
      uint32_t contentLen = 0;
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeBinary(&decoder, &content, &contentLen));
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeBinary(&decoder, &content, &contentLen));
    }
    int32_t groupCols = 0;
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &groupCols));
    EXPECT_EQ(0, groupCols);
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI8(&decoder, &i8));
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &i32));
  }
  offsets.vnodeCount = framePrefixSize + decoder.pos;
  int32_t vnodes = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &vnodes));
  for (int32_t i = 0; i < vnodes; ++i) {
    offsets.vnodeGids.push_back(framePrefixSize + decoder.pos);
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &i32));
    offsets.readEntryCounts.push_back(framePrefixSize + decoder.pos);
    int32_t reads = 0;
    EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &reads));
    for (int32_t j = 0; j < reads; ++j) {
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI64(&decoder, &i64));
      SSTriggerCalcParam param = {};
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeFixed(&decoder, &param, offsetof(SSTriggerCalcParam, notifyType)));
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeFixed(&decoder, &param, offsetof(SSTriggerCalcParam, notifyType)));
      offsets.tableCounts.push_back(framePrefixSize + decoder.pos);
      int32_t tables = 0;
      EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeI32(&decoder, &tables));
      if (tables > 0) {
        std::vector<int64_t> tableUids(tables);
        EXPECT_EQ(TSDB_CODE_SUCCESS, tDecodeFixed(&decoder, tableUids.data(), tables * sizeof(int64_t)));
      }
    }
  }
  tDecoderClear(&decoder);
  return offsets;
}

void writeI64(std::vector<uint8_t>* data, size_t offset, int64_t value) {
  ASSERT_LE(offset + sizeof(value), data->size());
  memcpy(data->data() + offset, &value, sizeof(value));
}

void appendAncestorParamContext(SStreamAncestorContext* destination, int64_t gid, int32_t paramIndex) {
  SStreamAncestorContext* source = makeAncestorContext(gid, paramIndex);
  ASSERT_NE(nullptr, source);
  ASSERT_NE(nullptr, source->pParamContexts);
  ASSERT_EQ(1, taosArrayGetSize(source->pParamContexts));
  ASSERT_NE(nullptr, taosArrayPush(destination->pParamContexts, taosArrayGet(source->pParamContexts, 0)));
  taosArrayDestroy(source->pParamContexts);
  source->pParamContexts = nullptr;
  tDestroyStreamAncestorContext(&source);
}

void addMultiGroupCalcInfo(SSTriggerCalcRequest* request, int64_t gid) {
  SSTriggerGroupCalcInfo info = {};
  info.pParams = taosArrayInit(1, sizeof(SSTriggerCalcParam));
  ASSERT_NE(nullptr, info.pParams);
  SSTriggerCalcParam param = makeWindowCalcParam(gid, gid + 99, BIT_FLAG_MASK(0));
  ASSERT_NE(nullptr, taosArrayPush(info.pParams, &param));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(request->pGroupCalcInfos, &gid, sizeof(gid), &info, sizeof(info)));
}

SSTriggerCalcRequest makeMultiGroupCalcRequest() {
  SSTriggerCalcRequest request = {};
  request.streamId = 11;
  request.runnerTaskId = 22;
  request.sessionId = 33;
  request.isWindowTrigger = true;
  request.precision = TSDB_TIME_PRECISION_MILLI;
  request.triggerType = STREAM_TRIGGER_COUNT;
  request.isMultiGroupCalc = true;
  request.pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  EXPECT_NE(nullptr, request.pGroupCalcInfos);
  tSimpleHashSetFreeFp(request.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  request.pGroupReadInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  EXPECT_NE(nullptr, request.pGroupReadInfos);
  tSimpleHashSetFreeFp(request.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
  addMultiGroupCalcInfo(&request, 101);
  addMultiGroupCalcInfo(&request, 202);
  request.pContextPolicy = makeContextPolicy({makeContextPolicyEntry(101, 0, STREAM_CONTEXT_POLICY_ANCESTOR),
                                              makeContextPolicyEntry(202, 0, STREAM_CONTEXT_POLICY_ANCESTOR)});
  request.pAncestorContext = makeAncestorContext(101);
  appendAncestorParamContext(request.pAncestorContext, 202, 0);

  SArray* readInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  EXPECT_NE(nullptr, readInfos);
  const int64_t gids[] = {101, 202};
  for (int32_t i = 0; i < 2; ++i) {
    auto* read = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(readInfos, i));
    auto* info =
        static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(request.pGroupCalcInfos, &gids[i], sizeof(gids[i])));
    EXPECT_NE(nullptr, read);
    EXPECT_NE(nullptr, info);
    const auto* param = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, 0));
    EXPECT_NE(nullptr, param);
    read->gid = gids[i];
    read->firstParam = *param;
    read->lastParam = *param;
  }
  const int32_t vgId = 7;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(request.pGroupReadInfos, &vgId, sizeof(vgId), &readInfos, POINTER_BYTES));

  request.pAncestorContext->pReadScopeBindings = taosArrayInit(2, sizeof(SStreamReadScopeBinding));
  EXPECT_NE(nullptr, request.pAncestorContext->pReadScopeBindings);
  for (int32_t i = 0; i < 2; ++i) {
    const auto* param =
        static_cast<const SStreamAncestorParamContext*>(taosArrayGet(request.pAncestorContext->pParamContexts, i));
    SStreamReadScopeBinding binding = makeReadScopeBinding(vgId, i, param->leafIdentity.gid);
    binding.scope.lineage.pScopes = taosArrayDup(param->leafIdentity.lineage.pScopes, nullptr);
    EXPECT_NE(nullptr, binding.scope.lineage.pScopes);
    EXPECT_NE(nullptr, taosArrayPush(request.pAncestorContext->pReadScopeBindings, &binding));
  }
  return request;
}

SSTriggerCalcRequest makeLineageSplitMultiGroupCalcRequest() {
  SSTriggerCalcRequest request = makeMultiGroupCalcRequest();
  const int64_t        removedGid = 202;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashRemove(request.pGroupCalcInfos, &removedGid, sizeof(removedGid)));

  const int64_t gid = 101;
  auto*         info = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(request.pGroupCalcInfos, &gid, sizeof(gid)));
  EXPECT_NE(nullptr, info);
  auto* firstParam = static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 0));
  EXPECT_NE(nullptr, firstParam);
  firstParam->wstart = 1000;
  firstParam->wend = 1099;
  SSTriggerCalcParam param = makeWindowCalcParam(1100, 1199, BIT_FLAG_MASK(0));
  EXPECT_NE(nullptr, taosArrayPush(info->pParams, &param));
  param = makeWindowCalcParam(2000, 2099, BIT_FLAG_MASK(0));
  EXPECT_NE(nullptr, taosArrayPush(info->pParams, &param));

  tDestroyStreamContextPolicy(&request.pContextPolicy);
  tDestroyStreamAncestorContext(&request.pAncestorContext);
  tSimpleHashClear(request.pGroupReadInfos);

  request.pContextPolicy = makeContextPolicy();
  request.pAncestorContext = makeAncestorContext(gid);
  for (int32_t i = 0; i < 3; ++i) {
    const SStreamContextPolicyEntry entry = makeContextPolicyEntry(gid, i, STREAM_CONTEXT_POLICY_ANCESTOR);
    EXPECT_NE(nullptr, taosArrayPush(request.pContextPolicy->pEntries, &entry));
    if (i > 0) appendAncestorParamContext(request.pAncestorContext, gid, i);
  }
  auto* splitContext =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(request.pAncestorContext->pParamContexts, 2));
  EXPECT_NE(nullptr, splitContext);
  splitContext->leafIdentity.openingTs = 2000;
  splitContext->leafIdentity.nativeDiscriminator = 27;
  auto* splitParent = static_cast<SScopeInstanceId*>(taosArrayGet(splitContext->leafIdentity.lineage.pScopes, 1));
  EXPECT_NE(nullptr, splitParent);
  splitParent->openingTs = 1500;
  splitParent->nativeDiscriminator = 21;

  const int32_t vgId = 7;
  SArray*       readInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  EXPECT_NE(nullptr, readInfos);
  auto* first = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(readInfos, 0));
  auto* second = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(readInfos, 1));
  EXPECT_NE(nullptr, first);
  EXPECT_NE(nullptr, second);
  first->gid = gid;
  first->firstParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 0));
  first->lastParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 1));
  second->gid = gid;
  second->firstParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 2));
  second->lastParam = second->firstParam;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(request.pGroupReadInfos, &vgId, sizeof(vgId), &readInfos, POINTER_BYTES));
  appendReadBindingForParam(request.pAncestorContext, vgId, 0, 0);
  appendReadBindingForParam(request.pAncestorContext, vgId, 1, 2);
  return request;
}

SStreamRuntimeFuncInfo makeFullMultiGroupFetchRuntime() {
  SSTriggerCalcRequest   request = makeMultiGroupCalcRequest();
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.sessionId = request.sessionId;
  runtime.triggerType = request.triggerType;
  runtime.isWindowTrigger = request.isWindowTrigger;
  TSWAP(runtime.pGroupCalcInfos, request.pGroupCalcInfos);
  TSWAP(runtime.pGroupReadInfos, request.pGroupReadInfos);
  TSWAP(runtime.pContextPolicy, request.pContextPolicy);
  TSWAP(runtime.pAncestorContext, request.pAncestorContext);
  tDestroySTriggerCalcRequest(&request);
  return runtime;
}

SStreamRuntimeFuncInfo makeActualNodeMultiGroupFetchRuntime() {
  SSTriggerCalcRequest request = makeLineageSplitMultiGroupCalcRequest();
  const int64_t        otherGid = 202;
  addMultiGroupCalcInfo(&request, otherGid);
  const SStreamContextPolicyEntry otherEntry = makeContextPolicyEntry(otherGid, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
  EXPECT_NE(nullptr, taosArrayPush(request.pContextPolicy->pEntries, &otherEntry));
  appendAncestorParamContext(request.pAncestorContext, otherGid, 0);

  const int32_t vgId = 7;
  auto**        nodeReads = static_cast<SArray**>(tSimpleHashGet(request.pGroupReadInfos, &vgId, sizeof(vgId)));
  EXPECT_NE(nullptr, nodeReads);
  SSTriggerGroupReadInfo read = {};
  read.gid = otherGid;
  auto* calc =
      static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(request.pGroupCalcInfos, &otherGid, sizeof(otherGid)));
  EXPECT_NE(nullptr, calc);
  const auto* param = static_cast<const SSTriggerCalcParam*>(taosArrayGet(calc->pParams, 0));
  EXPECT_NE(nullptr, param);
  read.firstParam = *param;
  read.lastParam = *param;
  EXPECT_NE(nullptr, taosArrayPush(*nodeReads, &read));
  appendReadBindingForParam(request.pAncestorContext, vgId, 2, 3);

  const int32_t      otherVgId = 8;
  SSTriggerCalcParam secondOtherParam = makeWindowCalcParam(3000, 3099, BIT_FLAG_MASK(0));
  EXPECT_NE(nullptr, taosArrayPush(calc->pParams, &secondOtherParam));
  const SStreamContextPolicyEntry secondOtherEntry =
      makeContextPolicyEntry(otherGid, 1, STREAM_CONTEXT_POLICY_ANCESTOR);
  EXPECT_NE(nullptr, taosArrayPush(request.pContextPolicy->pEntries, &secondOtherEntry));
  appendAncestorParamContext(request.pAncestorContext, otherGid, 1);
  auto* secondOtherContext =
      static_cast<SStreamAncestorParamContext*>(taosArrayGetLast(request.pAncestorContext->pParamContexts));
  EXPECT_NE(nullptr, secondOtherContext);
  auto* secondOtherParent =
      static_cast<SScopeInstanceId*>(taosArrayGet(secondOtherContext->leafIdentity.lineage.pScopes, 1));
  EXPECT_NE(nullptr, secondOtherParent);
  secondOtherParent->openingTs = 2500;
  secondOtherParent->nativeDiscriminator = 31;
  read.firstParam = secondOtherParam;
  read.lastParam = secondOtherParam;
  SArray* otherNodeReads = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
  EXPECT_NE(nullptr, otherNodeReads);
  EXPECT_NE(nullptr, taosArrayPush(otherNodeReads, &read));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(request.pGroupReadInfos, &otherVgId, sizeof(otherVgId), &otherNodeReads, POINTER_BYTES));
  appendReadBindingForParam(request.pAncestorContext, otherVgId, 0, 4);

  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.sessionId = request.sessionId;
  runtime.triggerType = request.triggerType;
  runtime.isWindowTrigger = request.isWindowTrigger;
  TSWAP(runtime.pGroupCalcInfos, request.pGroupCalcInfos);
  TSWAP(runtime.pGroupReadInfos, request.pGroupReadInfos);
  TSWAP(runtime.pContextPolicy, request.pContextPolicy);
  TSWAP(runtime.pAncestorContext, request.pAncestorContext);
  tDestroySTriggerCalcRequest(&request);
  return runtime;
}

SStreamRuntimeFuncInfo makeDenseActualNodeMultiGroupFetchRuntime(int32_t paramCount) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime(paramCount);
  runtime.isMultiGroupCalc = true;
  runtime.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  EXPECT_NE(nullptr, runtime.pGroupCalcInfos);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);

  SSTriggerGroupCalcInfo calc = {};
  TSWAP(calc.pParams, runtime.pStreamPesudoFuncVals);
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(runtime.pGroupCalcInfos, &runtime.groupId, sizeof(runtime.groupId), &calc, sizeof(calc)));

  const int32_t nodeId = 7;
  SArray*       reads = taosArrayInit(paramCount, sizeof(SSTriggerGroupReadInfo));
  EXPECT_NE(nullptr, reads);
  for (int32_t i = 0; i < paramCount; ++i) {
    const auto* param = static_cast<const SSTriggerCalcParam*>(taosArrayGet(calc.pParams, i));
    if (param == nullptr) {
      ADD_FAILURE() << "missing calc param " << i;
      break;
    }
    SSTriggerGroupReadInfo read = {};
    read.gid = runtime.groupId;
    read.firstParam = *param;
    read.lastParam = *param;
    if (taosArrayPush(reads, &read) == nullptr) {
      ADD_FAILURE() << "failed to append read " << i;
      break;
    }
    appendReadBindingForParam(runtime.pAncestorContext, nodeId, i, i);
  }

  runtime.pGroupReadInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  EXPECT_NE(nullptr, runtime.pGroupReadInfos);
  tSimpleHashSetFreeFp(runtime.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId), &reads, POINTER_BYTES));
  installActualNodeReadView(&runtime, nodeId);
  return runtime;
}

void setReaderFetchAncestorDepth(SStreamAncestorParamContext* context, int32_t ancestorDepth) {
  ASSERT_NE(nullptr, context);
  ASSERT_TRUE(ancestorDepth == 1 || ancestorDepth == 3);
  while (taosArrayGetSize(context->leafIdentity.lineage.pScopes) > ancestorDepth) {
    ASSERT_NE(nullptr, taosArrayPop(context->leafIdentity.lineage.pScopes));
    ASSERT_NE(nullptr, taosArrayPop(context->pSnapshots));
  }
  while (taosArrayGetSize(context->leafIdentity.lineage.pScopes) < ancestorDepth) {
    const int32_t                 layer = taosArrayGetSize(context->leafIdentity.lineage.pScopes);
    const SScopeInstanceId        scope = makeScopeInstanceId(layer, WINDOW_TYPE_COUNT, layer * 100, layer + 20);
    const SWindowAncestorSnapshot snapshot = makeWindowSnapshot(layer, WINDOW_TYPE_COUNT, 0, layer * 100);
    ASSERT_NE(nullptr, taosArrayPush(context->leafIdentity.lineage.pScopes, &scope));
    ASSERT_NE(nullptr, taosArrayPush(context->pSnapshots, &snapshot));
  }
}

SStreamRuntimeFuncInfo makeReaderFetchScaleRuntime(int32_t totalParams, int32_t nodeCount, ReaderFetchLayout layout,
                                                   int32_t ancestorDepth) {
  SStreamRuntimeFuncInfo runtime = {};
  EXPECT_GT(totalParams, 0);
  EXPECT_GT(nodeCount, 0);
  EXPECT_TRUE(ancestorDepth == 1 || ancestorDepth == 3);
  if (totalParams <= 0 || nodeCount <= 0 || (ancestorDepth != 1 && ancestorDepth != 3)) return runtime;

  const auto    groups = reader_fetch_benchmark::makeScalePlan(totalParams, nodeCount, layout);
  const int32_t groupCount = static_cast<int32_t>(groups.size());
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.sessionId = 33;
  runtime.triggerType = STREAM_TRIGGER_COUNT;
  runtime.isWindowTrigger = true;
  runtime.precision = TSDB_TIME_PRECISION_MILLI;
  runtime.pGroupCalcInfos = tSimpleHashInit(groupCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  runtime.pGroupReadInfos = tSimpleHashInit(nodeCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  runtime.pGroupCalcOrder = taosArrayInit(groupCount, sizeof(uint64_t));
  runtime.pContextPolicy = makeContextPolicy();
  runtime.pAncestorContext = static_cast<SStreamAncestorContext*>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(nullptr, runtime.pGroupCalcInfos);
  EXPECT_NE(nullptr, runtime.pGroupReadInfos);
  EXPECT_NE(nullptr, runtime.pGroupCalcOrder);
  EXPECT_NE(nullptr, runtime.pContextPolicy);
  EXPECT_NE(nullptr, runtime.pAncestorContext);
  runtime.pAncestorContext->pParamContexts = taosArrayInit(totalParams, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(nullptr, runtime.pAncestorContext->pParamContexts);
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  tSimpleHashSetFreeFp(runtime.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);

  std::vector<SArray*> nodeReads(nodeCount, nullptr);
  for (int32_t node = 0; node < nodeCount; ++node) {
    nodeReads[node] = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
    EXPECT_NE(nullptr, nodeReads[node]);
  }

  int32_t globalParamIndex = 0;
  for (const auto& group : groups) {
    const int64_t gid = group.gid;
    EXPECT_NE(nullptr, taosArrayPush(runtime.pGroupCalcOrder, &gid));
    SSTriggerGroupCalcInfo calc = {};
    calc.pParams = taosArrayInit(group.paramCount, sizeof(SSTriggerCalcParam));
    EXPECT_NE(nullptr, calc.pParams);
    for (int32_t paramIndex = 0; paramIndex < group.paramCount; ++paramIndex) {
      const int64_t      start = static_cast<int64_t>(globalParamIndex) * 100;
      SSTriggerCalcParam param = makeWindowCalcParam(start, start + 99, BIT_FLAG_MASK(0));
      EXPECT_NE(nullptr, taosArrayPush(calc.pParams, &param));
      appendAncestorParamContext(runtime.pAncestorContext, gid, paramIndex);
      auto* context =
          static_cast<SStreamAncestorParamContext*>(taosArrayGetLast(runtime.pAncestorContext->pParamContexts));
      setReaderFetchAncestorDepth(context, ancestorDepth);
      const SStreamContextPolicyEntry entry = makeContextPolicyEntry(gid, paramIndex, STREAM_CONTEXT_POLICY_ANCESTOR);
      EXPECT_NE(nullptr, taosArrayPush(runtime.pContextPolicy->pEntries, &entry));
      ++globalParamIndex;
    }
    EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(runtime.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
  }

  if (layout == ReaderFetchLayout::SharedGroup) {
    for (const auto& segment : groups.front().segments) {
      for (int32_t paramIndex = segment.beginParam; paramIndex < segment.endParam; ++paramIndex) {
        auto* context = static_cast<SStreamAncestorParamContext*>(
            taosArrayGet(runtime.pAncestorContext->pParamContexts, paramIndex));
        for (int32_t layer = 0; layer < taosArrayGetSize(context->leafIdentity.lineage.pScopes); ++layer) {
          auto* scope = static_cast<SScopeInstanceId*>(taosArrayGet(context->leafIdentity.lineage.pScopes, layer));
          scope->openingTs += static_cast<int64_t>(segment.nodeIndex) * 100000;
          scope->nativeDiscriminator += segment.nodeIndex * 100;
        }
      }
    }
  }

  for (const auto& group : groups) {
    const int64_t gid = group.gid;
    auto* calc = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(runtime.pGroupCalcInfos, &gid, sizeof(gid)));
    EXPECT_NE(nullptr, calc);
    for (const auto& segment : group.segments) {
      SSTriggerGroupReadInfo read = {};
      read.gid = gid;
      read.firstParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(calc->pParams, segment.beginParam));
      read.lastParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(calc->pParams, segment.endParam - 1));
      const int32_t readInfoIndex = taosArrayGetSize(nodeReads[segment.nodeIndex]);
      EXPECT_NE(nullptr, taosArrayPush(nodeReads[segment.nodeIndex], &read));
      appendReadBindingForParam(runtime.pAncestorContext, segment.nodeIndex + 1, readInfoIndex,
                                group.paramOffset + segment.beginParam);
    }
  }

  for (int32_t node = 0; node < nodeCount; ++node) {
    if (taosArrayGetSize(nodeReads[node]) == 0) {
      taosArrayDestroy(nodeReads[node]);
      continue;
    }
    const int32_t nodeId = node + 1;
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              tSimpleHashPut(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId), &nodeReads[node], POINTER_BYTES));
  }
  EXPECT_EQ(totalParams, globalParamIndex);
  SSTriggerCalcRequest validation = {};
  validation.isMultiGroupCalc = true;
  validation.pGroupCalcInfos = runtime.pGroupCalcInfos;
  validation.pGroupReadInfos = runtime.pGroupReadInfos;
  validation.pContextPolicy = runtime.pContextPolicy;
  validation.pAncestorContext = runtime.pAncestorContext;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&validation));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(runtime.pContextPolicy, runtime.pAncestorContext, true));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateStreamCalcContextForFetch(&runtime, true, false));
  return runtime;
}

void installActualNodeReadView(SStreamRuntimeFuncInfo* runtime, int32_t nodeId) {
  auto** nodeReads = static_cast<SArray**>(tSimpleHashGet(runtime->pGroupReadInfos, &nodeId, sizeof(nodeId)));
  ASSERT_NE(nullptr, nodeReads);
  runtime->curNodeId = nodeId;
  runtime->curGrpRead = taosArrayDup(*nodeReads, nullptr);
  ASSERT_NE(nullptr, runtime->curGrpRead);
  runtime->curGrpReadOwnership = STREAM_GRP_READ_OWNED_SHALLOW_VIEW;
}

SStreamAncestorContext* cloneWithoutLastReadBindings(const SStreamAncestorContext* source, int32_t remaining) {
  SStreamAncestorContext* clone = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tCloneStreamAncestorContext(source, &clone));
  if (clone == nullptr) return nullptr;
  while (taosArrayGetSize(clone->pReadScopeBindings) > remaining) {
    auto* binding = static_cast<SStreamReadScopeBinding*>(
        taosArrayGet(clone->pReadScopeBindings, taosArrayGetSize(clone->pReadScopeBindings) - 1));
    EXPECT_NE(nullptr, binding);
    if (binding == nullptr) break;
    taosArrayDestroy(binding->scope.lineage.pScopes);
    binding->scope.lineage.pScopes = nullptr;
    EXPECT_NE(nullptr, taosArrayPop(clone->pReadScopeBindings));
  }
  return clone;
}

std::vector<uint8_t> encodeAncestorFrame(const SStreamAncestorContext* context) {
  SEncoder encoder = {};
  tEncoderInit(&encoder, nullptr, 0);
  int32_t code = tStartEncodeStreamTailFrame(&encoder, STREAM_ANCESTOR_FRAME_MAGIC, STREAM_ANCESTOR_FRAME_VERSION, 0);
  if (code == TSDB_CODE_SUCCESS) code = tEncodeStreamAncestorContext(&encoder, context);
  tEndEncodeStreamTailFrame(&encoder);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  const int32_t size = encoder.pos;
  tEncoderClear(&encoder);
  if (code != TSDB_CODE_SUCCESS || size <= 0) return {};

  std::vector<uint8_t> frame(size);
  tEncoderInit(&encoder, frame.data(), frame.size());
  code = tStartEncodeStreamTailFrame(&encoder, STREAM_ANCESTOR_FRAME_MAGIC, STREAM_ANCESTOR_FRAME_VERSION, 0);
  if (code == TSDB_CODE_SUCCESS) code = tEncodeStreamAncestorContext(&encoder, context);
  tEndEncodeStreamTailFrame(&encoder);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  EXPECT_EQ(size, encoder.pos);
  tEncoderClear(&encoder);
  return code == TSDB_CODE_SUCCESS ? frame : std::vector<uint8_t>{};
}

void replaceAncestorFrame(std::vector<uint8_t>* bytes, const SStreamAncestorContext* context, bool fetch) {
  const size_t offset = ancestorFrameOffset(*bytes);
  ASSERT_LT(offset, bytes->size());
  const auto frame = encodeAncestorFrame(context);
  ASSERT_FALSE(frame.empty());
  bytes->resize(offset);
  bytes->insert(bytes->end(), frame.begin(), frame.end());
  if (fetch) {
    writeI32(bytes, sizeof(SMsgHead), static_cast<int32_t>(bytes->size() - sizeof(SMsgHead) - sizeof(int32_t)));
    reinterpret_cast<SMsgHead*>(bytes->data())->contLen = htonl(bytes->size());
  } else {
    writeI32(bytes, 0, static_cast<int32_t>(bytes->size() - sizeof(int32_t)));
  }
}

void appendReadBindingForParam(SStreamAncestorContext* context, int32_t vgId, int32_t readInfoIndex,
                               int32_t paramContextIndex) {
  if (context->pReadScopeBindings == nullptr) {
    context->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
    ASSERT_NE(nullptr, context->pReadScopeBindings);
  }
  const auto* param =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(context->pParamContexts, paramContextIndex));
  ASSERT_NE(nullptr, param);
  SStreamReadScopeBinding binding = makeReadScopeBinding(vgId, readInfoIndex, param->leafIdentity.gid);
  binding.scope.lineage.pScopes = taosArrayDup(param->leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(nullptr, binding.scope.lineage.pScopes);
  ASSERT_NE(nullptr, taosArrayPush(context->pReadScopeBindings, &binding));
}

std::vector<uint8_t> makeUnknownCalcFrame() {
  std::vector<uint8_t> frame(sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2 + 3);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, frame.data(), frame.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU32(&encoder, UINT32_C(0x554e4b4e)));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU16(&encoder, 9));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU16(&encoder, 7));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU32(&encoder, 3));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 1));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 2));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeU8(&encoder, 3));
  tEncoderClear(&encoder);
  return frame;
}

std::vector<uint8_t> extractContextFrames(const std::vector<uint8_t>& bytes) {
  std::vector<uint8_t> frames;
  for (uint32_t magic : {STREAM_CONTEXT_POLICY_FRAME_MAGIC, STREAM_ANCESTOR_FRAME_MAGIC}) {
    const size_t offset = frameOffset(bytes, magic);
    if (offset == bytes.size()) continue;
    uint32_t payloadLength = 0;
    memcpy(&payloadLength, bytes.data() + offset + sizeof(uint32_t) + sizeof(uint16_t) * 2, sizeof(payloadLength));
    const size_t frameLength = sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2 + payloadLength;
    EXPECT_LE(offset + frameLength, bytes.size());
    if (offset + frameLength > bytes.size()) return {};
    frames.insert(frames.end(), bytes.begin() + offset, bytes.begin() + offset + frameLength);
  }
  return frames;
}

const SStreamContextPolicyEntry* findPolicyEntry(const SStreamContextPolicy* policy, int64_t gid, int32_t paramIndex) {
  if (policy == nullptr) return nullptr;
  for (int32_t i = 0; i < taosArrayGetSize(policy->pEntries); ++i) {
    const auto* entry = static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(policy->pEntries, i));
    if (entry != nullptr && entry->gid == gid && entry->paramIndex == paramIndex) return entry;
  }
  return nullptr;
}

const SSTriggerGroupReadInfo* findGroupReadInfo(const SArray* reads, int64_t gid) {
  for (int32_t i = 0; i < taosArrayGetSize(reads); ++i) {
    const auto* read = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(reads, i));
    if (read != nullptr && read->gid == gid) return read;
  }
  return nullptr;
}

const SStreamReadScopeBinding* findReadBinding(const SStreamAncestorContext* context, int32_t vgId,
                                               int32_t readInfoIndex, int64_t gid) {
  if (context == nullptr) return nullptr;
  for (int32_t i = 0; i < taosArrayGetSize(context->pReadScopeBindings); ++i) {
    const auto* binding = static_cast<const SStreamReadScopeBinding*>(taosArrayGet(context->pReadScopeBindings, i));
    if (binding != nullptr && binding->vgId == vgId && binding->readInfoIndex == readInfoIndex &&
        binding->scope.gid == gid) {
      return binding;
    }
  }
  return nullptr;
}

const SScopeInstanceId* findScope(const SArray* scopes, int32_t layerIndex) {
  for (int32_t i = 0; i < taosArrayGetSize(scopes); ++i) {
    const auto* scope = static_cast<const SScopeInstanceId*>(taosArrayGet(scopes, i));
    if (scope != nullptr && scope->layerIndex == layerIndex) return scope;
  }
  return nullptr;
}

const SWindowAncestorSnapshot* findSnapshot(const SArray* snapshots, int32_t layerIndex) {
  for (int32_t i = 0; i < taosArrayGetSize(snapshots); ++i) {
    const auto* snapshot = static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(snapshots, i));
    if (snapshot != nullptr && snapshot->layerIndex == layerIndex) return snapshot;
  }
  return nullptr;
}

void expectFullFetchContext(const SResFetchReq& request, int8_t expectedPolicy = STREAM_CONTEXT_POLICY_ANCESTOR) {
  ASSERT_NE(nullptr, request.pStRtFuncInfo);
  EXPECT_TRUE(request.pStRtFuncInfo->isMultiGroupCalc);
  EXPECT_EQ(2, tSimpleHashGetSize(request.pStRtFuncInfo->pGroupCalcInfos));
  EXPECT_EQ(1, tSimpleHashGetSize(request.pStRtFuncInfo->pGroupReadInfos));
  ASSERT_NE(nullptr, request.pStRtFuncInfo->pContextPolicy);
  EXPECT_EQ(2, taosArrayGetSize(request.pStRtFuncInfo->pContextPolicy->pEntries));
  const int64_t gids[] = {101, 202};
  const int32_t vgId = 7;
  auto** reads = static_cast<SArray**>(tSimpleHashGet(request.pStRtFuncInfo->pGroupReadInfos, &vgId, sizeof(vgId)));
  ASSERT_NE(nullptr, reads);
  ASSERT_NE(nullptr, *reads);
  EXPECT_EQ(2, taosArrayGetSize(*reads));
  for (int32_t i = 0; i < 2; ++i) {
    const int64_t gid = gids[i];
    const auto*   policy = findPolicyEntry(request.pStRtFuncInfo->pContextPolicy, gid, 0);
    ASSERT_NE(nullptr, policy);
    EXPECT_EQ(expectedPolicy, policy->contextPolicy);

    const auto* calc = static_cast<const SSTriggerGroupCalcInfo*>(
        tSimpleHashGet(request.pStRtFuncInfo->pGroupCalcInfos, &gid, sizeof(gid)));
    ASSERT_NE(nullptr, calc);
    ASSERT_EQ(1, taosArrayGetSize(calc->pParams));
    const auto* calcParam = static_cast<const SSTriggerCalcParam*>(taosArrayGet(calc->pParams, 0));
    ASSERT_NE(nullptr, calcParam);
    EXPECT_EQ(gid, calcParam->wstart);
    EXPECT_EQ(gid + 99, calcParam->wend);
    EXPECT_EQ(BIT_FLAG_MASK(0), calcParam->notifyType);

    const auto* read = findGroupReadInfo(*reads, gid);
    ASSERT_NE(nullptr, read);
    EXPECT_EQ(gid, read->firstParam.wstart);
    EXPECT_EQ(gid + 99, read->firstParam.wend);
    EXPECT_EQ(gid, read->lastParam.wstart);
    EXPECT_EQ(gid + 99, read->lastParam.wend);
  }

  if (expectedPolicy == STREAM_CONTEXT_POLICY_NONE) {
    EXPECT_EQ(nullptr, request.pStRtFuncInfo->pAncestorContext);
    EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(request.pStRtFuncInfo->pContextPolicy, nullptr, true));
    return;
  }

  ASSERT_EQ(STREAM_CONTEXT_POLICY_ANCESTOR, expectedPolicy);
  const auto* context = request.pStRtFuncInfo->pAncestorContext;
  ASSERT_NE(nullptr, context);
  EXPECT_EQ(2, taosArrayGetSize(context->pParamContexts));
  EXPECT_EQ(2, taosArrayGetSize(context->pReadScopeBindings));
  for (int32_t i = 0; i < 2; ++i) {
    const int64_t                      gid = gids[i];
    const SStreamAncestorParamContext* param = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(context, gid, 0, &param));
    ASSERT_NE(nullptr, param);
    EXPECT_EQ(gid, param->leafIdentity.gid);
    EXPECT_EQ(WINDOW_TYPE_COUNT, param->leafIdentity.triggerType);
    EXPECT_EQ(1000, param->leafIdentity.openingTs);
    EXPECT_EQ(17, param->leafIdentity.nativeDiscriminator);

    const auto* rootScope = findScope(param->leafIdentity.lineage.pScopes, 0);
    const auto* parentScope = findScope(param->leafIdentity.lineage.pScopes, 1);
    ASSERT_NE(nullptr, rootScope);
    ASSERT_NE(nullptr, parentScope);
    EXPECT_EQ(WINDOW_TYPE_INTERVAL, rootScope->triggerType);
    EXPECT_EQ(0, rootScope->openingTs);
    EXPECT_EQ(10, rootScope->nativeDiscriminator);
    EXPECT_EQ(WINDOW_TYPE_SESSION, parentScope->triggerType);
    EXPECT_EQ(500, parentScope->openingTs);
    EXPECT_EQ(11, parentScope->nativeDiscriminator);

    const auto* rootSnapshot = findSnapshot(param->pSnapshots, 0);
    const auto* parentSnapshot = findSnapshot(param->pSnapshots, 1);
    ASSERT_NE(nullptr, rootSnapshot);
    ASSERT_NE(nullptr, parentSnapshot);
    EXPECT_EQ(WINDOW_TYPE_INTERVAL, rootSnapshot->triggerType);
    EXPECT_EQ(PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS, rootSnapshot->placeholderMask);
    EXPECT_EQ(-1, rootSnapshot->values.sliding.prevTs);
    EXPECT_EQ(0, rootSnapshot->values.sliding.currentTs);
    EXPECT_EQ(100, rootSnapshot->values.sliding.nextTs);
    EXPECT_EQ(WINDOW_TYPE_SESSION, parentSnapshot->triggerType);
    EXPECT_EQ(PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WROWNUM, parentSnapshot->placeholderMask);
    EXPECT_EQ(500, parentSnapshot->values.window.start);
    EXPECT_EQ(999, parentSnapshot->values.window.end);
    EXPECT_EQ(500, parentSnapshot->values.window.duration);
    EXPECT_EQ(9, parentSnapshot->values.window.rownum);

    const auto* binding = findReadBinding(context, vgId, i, gid);
    ASSERT_NE(nullptr, binding);
    EXPECT_EQ(2, taosArrayGetSize(binding->scope.lineage.pScopes));
  }
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(request.pStRtFuncInfo->pContextPolicy, context, true));
}

// Opt-in observations use the real receive and construction APIs, without Stub probes.
SStreamRuntimeFuncInfo makeForwardingScaleRuntime(int32_t windows, int32_t groups, int32_t layers) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.isWindowTrigger = true;
  runtime.triggerType = STREAM_TRIGGER_COUNT;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.pContextPolicy = makeContextPolicy();
  runtime.pGroupCalcInfos = tSimpleHashInit(groups, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  tSimpleHashSetFreeFp(runtime.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
  runtime.pGroupReadInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  tSimpleHashSetFreeFp(runtime.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
  SArray* reads = taosArrayInit(groups, sizeof(SSTriggerGroupReadInfo));
  for (int32_t group = 0; group < groups; ++group) {
    const int64_t          gid = 101 + group;
    const int32_t          count = windows / groups;
    SSTriggerGroupCalcInfo calc = {};
    calc.pParams = taosArrayInit(count, sizeof(SSTriggerCalcParam));
    for (int32_t index = 0; index < count; ++index) {
      auto window = makeWindowCalcParam(10000 + index * 100, 10099 + index * 100, BIT_FLAG_MASK(0));
      EXPECT_NE(nullptr, taosArrayPush(calc.pParams, &window));
      auto entry =
          makeContextPolicyEntry(gid, index, layers == 1 ? STREAM_CONTEXT_POLICY_NONE : STREAM_CONTEXT_POLICY_ANCESTOR);
      EXPECT_NE(nullptr, taosArrayPush(runtime.pContextPolicy->pEntries, &entry));
      if (layers == 1) continue;
      auto* context = makeAncestorContext(gid, index);
      auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(context->pParamContexts, 0));
      param->leafIdentity.openingTs = window.wstart;
      param->leafIdentity.nativeDiscriminator = index;
      if (layers == 2) {
        taosArrayPop(param->leafIdentity.lineage.pScopes);
        taosArrayPop(param->pSnapshots);
      }
      auto* root = static_cast<SScopeInstanceId*>(taosArrayGet(param->leafIdentity.lineage.pScopes, 0));
      root->openingTs = group * 100;
      auto* snapshot = static_cast<SWindowAncestorSnapshot*>(taosArrayGet(param->pSnapshots, 0));
      snapshot->values.sliding.currentTs = group * 100;
      if (runtime.pAncestorContext == nullptr) {
        runtime.pAncestorContext = context;
      } else {
        EXPECT_NE(nullptr, taosArrayPush(runtime.pAncestorContext->pParamContexts, param));
        taosArrayClear(context->pParamContexts);
        tDestroyStreamAncestorContext(&context);
      }
    }
    SSTriggerGroupReadInfo read = {};
    read.gid = gid;
    read.firstParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(calc.pParams, 0));
    read.lastParam = *static_cast<SSTriggerCalcParam*>(taosArrayGetLast(calc.pParams));
    EXPECT_NE(nullptr, taosArrayPush(reads, &read));
    EXPECT_EQ(0, tSimpleHashPut(runtime.pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)));
    if (layers > 1) appendReadBindingForParam(runtime.pAncestorContext, 7, group, windows / groups * group);
  }
  const int32_t nodeId = 7;
  EXPECT_EQ(0, tSimpleHashPut(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId), &reads, sizeof(reads)));
  return runtime;
}

void expectForwardingScaleContext(const SResFetchReq& request, int32_t windows, int32_t groups, int32_t layers) {
  auto* info = request.pStRtFuncInfo;
  ASSERT_NE(nullptr, info);
  EXPECT_EQ(groups, tSimpleHashGetSize(info->pGroupCalcInfos));
  ASSERT_NE(nullptr, info->pContextPolicy);
  ASSERT_EQ(windows, taosArrayGetSize(info->pContextPolicy->pEntries));
  for (int32_t i = 0; i < windows; ++i) {
    auto* entry = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(info->pContextPolicy->pEntries, i));
    ASSERT_GE(entry->gid, 101);
    ASSERT_LT(entry->gid, 101 + groups);
    EXPECT_EQ(layers == 1 ? STREAM_CONTEXT_POLICY_NONE : STREAM_CONTEXT_POLICY_ANCESTOR, entry->contextPolicy);
    EXPECT_GE(entry->paramIndex, 0);
    EXPECT_LT(entry->paramIndex, windows / groups);
  }
  for (int64_t gid = 101; gid < 101 + groups; ++gid) {
    auto* calc = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(info->pGroupCalcInfos, &gid, sizeof(gid)));
    ASSERT_NE(nullptr, calc);
    ASSERT_EQ(windows / groups, taosArrayGetSize(calc->pParams));
    for (int32_t index = 0; index < windows / groups; ++index) {
      auto* window = static_cast<SSTriggerCalcParam*>(taosArrayGet(calc->pParams, index));
      EXPECT_EQ(10000 + index * 100, window->wstart);
      EXPECT_EQ(10099 + index * 100, window->wend);
    }
  }
  const int32_t nodeId = 7;
  auto**        reads = static_cast<SArray**>(tSimpleHashGet(info->pGroupReadInfos, &nodeId, sizeof(nodeId)));
  ASSERT_NE(nullptr, reads);
  ASSERT_EQ(groups, taosArrayGetSize(*reads));
  for (int32_t group = 0; group < groups; ++group) {
    const auto* read = findGroupReadInfo(*reads, 101 + group);
    ASSERT_NE(nullptr, read);
    EXPECT_EQ(10000, read->firstParam.wstart);
    EXPECT_EQ(10099 + (windows / groups - 1) * 100, read->lastParam.wend);
    if (layers > 1) {
      const auto* binding = findReadBinding(info->pAncestorContext, 7, group, 101 + group);
      ASSERT_NE(nullptr, binding);
      ASSERT_EQ(layers - 1, taosArrayGetSize(binding->scope.lineage.pScopes));
      EXPECT_EQ(group * 100, findScope(binding->scope.lineage.pScopes, 0)->openingTs);
    }
  }
  if (layers == 1) {
    EXPECT_EQ(nullptr, info->pAncestorContext);
    return;
  }
  ASSERT_NE(nullptr, info->pAncestorContext);
  ASSERT_EQ(windows, taosArrayGetSize(info->pAncestorContext->pParamContexts));
  EXPECT_EQ(groups, taosArrayGetSize(info->pAncestorContext->pReadScopeBindings));
  for (int32_t i = 0; i < windows; ++i) {
    auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(info->pAncestorContext->pParamContexts, i));
    EXPECT_EQ(10000 + param->paramIndex * 100, param->leafIdentity.openingTs);
    EXPECT_EQ(param->paramIndex, param->leafIdentity.nativeDiscriminator);
    ASSERT_EQ(layers - 1, taosArrayGetSize(param->leafIdentity.lineage.pScopes));
    ASSERT_EQ(layers - 1, taosArrayGetSize(param->pSnapshots));
    const auto* root = findScope(param->leafIdentity.lineage.pScopes, 0);
    ASSERT_NE(nullptr, root);
    EXPECT_EQ((param->leafIdentity.gid - 101) * 100, root->openingTs);
    const auto* snapshot = findSnapshot(param->pSnapshots, 0);
    ASSERT_NE(nullptr, snapshot);
    EXPECT_EQ((param->leafIdentity.gid - 101) * 100, snapshot->values.sliding.currentTs);
    if (layers == 3) {
      const auto* parent = findSnapshot(param->pSnapshots, 1);
      ASSERT_NE(nullptr, parent);
      EXPECT_EQ(500, parent->values.window.start);
      EXPECT_EQ(999, parent->values.window.end);
    }
  }
}

TEST(StreamContextForwardingTest, BoundedScaleBenchmark) {
  if (std::getenv("TD_CONTEXT_FORWARDING_BENCHMARK") == nullptr) GTEST_SKIP() << "Opt-in scale measurement";
  using Clock = std::chrono::steady_clock;
  const int32_t matrix[][3] = {{1, 1, 1}, {128, 2, 2}, {4096, 8, 3}, {40960, 16, 3}};
  for (const auto& row : matrix) {
    const int32_t windows = row[0], groups = row[1], layers = row[2];
    ASSERT_LE(layers, STREAM_WINDOW_MAX_LAYERS);
    ASSERT_EQ(0, windows % groups);
    ASSERT_LE(windows, STREAM_CALC_REQ_MAX_WIN_NUM);
    RuntimeFuncInfoGuard source;
    source.runtime = makeForwardingScaleRuntime(windows, groups, layers);
    SResFetchReq original = {};
    original.reset = true;
    original.pStRtFuncInfo = &source.runtime;
    auto input = serializeFetchRequest(original, true, false);
    ASSERT_FALSE(input.empty());
    const auto expectedFrames = extractContextFrames(input);
    // One repetition bounds the Debug/ASAN 40960-window legacy cost. No timing threshold.
    for (int32_t children : {1, 5}) {
      for (const char* mode : {"legacy", "objects", "frames"}) {
        SCOPED_TRACE(::testing::Message() << windows << "/" << children << "/" << mode);
        FetchRequestGuard received;
        const bool        retain = std::strcmp(mode, "frames") == 0;
        auto              begin = Clock::now();
        int32_t code = retain ? tDeserializeSResFetchReqWithContextFrames(input.data(), input.size(), &received.request)
                              : tDeserializeSResFetchReq(input.data(), input.size(), &received.request);
        ASSERT_EQ(0, code);
        received.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
        ASSERT_EQ(0, tAdmitStreamContext(received.request.pStRtFuncInfo->pContextPolicy,
                                         received.request.pStRtFuncInfo->pAncestorContext, true));
        const double receiveMs = std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
        auto*        owner = received.request.pContextFrames;
        ASSERT_EQ(retain, owner != nullptr);
        const int32_t frameBytes = owner == nullptr ? 0 : owner->size;
        double        outgoingMs = 0;
        int64_t       messageBytes = 0;
        for (int32_t child = 0; child < children; ++child) {
          void*   output = nullptr;
          int32_t length = 0;
          received.request.taskId = child + 1;
          begin = Clock::now();
          if (std::strcmp(mode, "legacy") == 0) {
            length = tSerializeSResFetchReq(nullptr, 0, &received.request, true, false);
            ASSERT_GT(length, 0);
            output = taosMemoryMalloc(length);
            ASSERT_NE(nullptr, output);
            code = tSerializeSResFetchReq(output, length, &received.request, true, false);
            EXPECT_EQ(length, code);
          } else {
            code = tBuildStreamRunnerFetchReq(&received.request, owner, &output, &length);
            EXPECT_EQ(0, code);
          }
          outgoingMs += std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
          TaosMemoryPtr outgoing(output);
          ASSERT_NE(nullptr, output);
          ASSERT_GT(length, 0);
          messageBytes += length;
          EXPECT_EQ(owner, received.request.pContextFrames);
          FetchRequestGuard decoded;
          ASSERT_EQ(0, tDeserializeSResFetchReq(output, length, &decoded.request));
          EXPECT_EQ(child + 1, decoded.request.taskId);
          expectForwardingScaleContext(decoded.request, windows, groups, layers);
          if (retain) {
            std::vector<uint8_t> wire(static_cast<uint8_t*>(output), static_cast<uint8_t*>(output) + length);
            EXPECT_EQ(expectedFrames, extractContextFrames(wire));
          }
        }
        tDestroySResFetchReq(&received.request);
        EXPECT_EQ(nullptr, received.request.pContextFrames);
        memset(&received.request, 0, sizeof(received.request));
        std::cout << "CONTEXT_FORWARDING_BENCHMARK {\"windows\":" << windows << ",\"groups\":" << groups
                  << ",\"layers\":" << layers << ",\"children\":" << children << ",\"mode\":\"" << mode
                  << "\",\"repetitions\":1,\"input_bytes\":" << input.size() << ",\"message_bytes\":" << messageBytes
                  << ",\"receive_ms\":" << receiveMs << ",\"outgoing_ms\":" << outgoingMs
                  << ",\"total_preparation_ms\":" << receiveMs + outgoingMs << ",\"policy_entries\":" << windows
                  << ",\"ancestor_snapshots\":" << windows * (layers - 1) << ",\"frame_owners\":" << (owner != nullptr)
                  << ",\"retained_bytes\":" << frameBytes << ",\"owners_after_cleanup\":0,\"bytes_after_cleanup\":0}"
                  << std::endl;
      }
    }
  }
}

TEST(StreamContextForwardingTest, CalcOptInRetainsIndependentExactFrames) {
  TriggerCalcRequestGuard source;
  source.request = makeLineageSplitMultiGroupCalcRequest();
  auto       bytes = serializeCalcRequest(source.request);
  const auto expectedFrames = extractContextFrames(bytes);
  ASSERT_FALSE(expectedFrames.empty());

  TriggerCalcRequestGuard ordinary;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &ordinary.request));
  EXPECT_EQ(nullptr, ordinary.request.pContextFrames);

  TriggerCalcRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSTriggerCalcRequestWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
  ASSERT_NE(nullptr, decoded.request.pContextFrames);
  EXPECT_EQ(expectedFrames.size(), static_cast<size_t>(decoded.request.pContextFrames->size));
  EXPECT_EQ(0, memcmp(expectedFrames.data(), decoded.request.pContextFrames->pData, expectedFrames.size()));
  EXPECT_EQ(decoded.request.pContextPolicy, decoded.request.pContextFrames->pContextPolicy);
  EXPECT_EQ(decoded.request.pAncestorContext, decoded.request.pContextFrames->pAncestorContext);

  std::fill(bytes.begin(), bytes.end(), 0);
  EXPECT_EQ(0, memcmp(expectedFrames.data(), decoded.request.pContextFrames->pData, expectedFrames.size()));
  EXPECT_EQ(3, taosArrayGetSize(decoded.request.pContextPolicy->pEntries));
  EXPECT_EQ(3, taosArrayGetSize(decoded.request.pAncestorContext->pParamContexts));
}

TEST(StreamContextForwardingTest, FetchBuilderForwardsRetainedFramesWithoutProjection) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  auto* firstParam =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(runtime.runtime.pAncestorContext->pParamContexts, 0));
  auto* secondParam =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(runtime.runtime.pAncestorContext->pParamContexts, 1));
  ASSERT_NE(nullptr, firstParam);
  ASSERT_NE(nullptr, secondParam);
  TSWAP(*firstParam, *secondParam);
  auto* firstBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.runtime.pAncestorContext->pReadScopeBindings, 0));
  auto* secondBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.runtime.pAncestorContext->pReadScopeBindings, 1));
  ASSERT_NE(nullptr, firstBinding);
  ASSERT_NE(nullptr, secondBinding);
  TSWAP(*firstBinding, *secondBinding);

  SResFetchReq source = {};
  source.header.vgId = 7;
  source.sId = 11;
  source.queryId = 22;
  source.taskId = 33;
  source.srcTaskId = 44;
  source.blockIdx = 55;
  source.execId = 66;
  source.clientId = 77;
  source.pStRtFuncInfo = &runtime.runtime;
  source.reset = true;
  source.reuseReader = true;
  source.readerGroupMode = true;
  auto       bytes = serializeFetchRequest(source, true, false);
  const auto expectedFrames = extractContextFrames(bytes);
  ASSERT_FALSE(expectedFrames.empty());

  FetchRequestGuard ordinary;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &ordinary.request));
  EXPECT_EQ(nullptr, ordinary.request.pContextFrames);

  FetchRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReqWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
  ASSERT_NE(nullptr, decoded.request.pContextFrames);
  EXPECT_EQ(expectedFrames.size(), static_cast<size_t>(decoded.request.pContextFrames->size));
  EXPECT_EQ(0, memcmp(expectedFrames.data(), decoded.request.pContextFrames->pData, expectedFrames.size()));
  std::fill(bytes.begin(), bytes.end(), 0);

  decoded.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  const SStreamContextFrames* frameSources[] = {decoded.request.pContextFrames, nullptr};
  for (const SStreamContextFrames* frames : frameSources) {
    void*   message = nullptr;
    int32_t length = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamRunnerFetchReq(&decoded.request, frames, &message, &length));
    TaosMemoryPtr messageOwner(message);
    ASSERT_NE(nullptr, message);
    EXPECT_GT(length, 0);

    const std::vector<uint8_t> forwarded(static_cast<uint8_t*>(message), static_cast<uint8_t*>(message) + length);
    EXPECT_EQ(expectedFrames, extractContextFrames(forwarded));
    FetchRequestGuard forwardedRequest;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(message, length, &forwardedRequest.request));
    EXPECT_EQ(11, forwardedRequest.request.sId);
    EXPECT_EQ(22, forwardedRequest.request.queryId);
    EXPECT_EQ(33, forwardedRequest.request.taskId);
    EXPECT_EQ(44, forwardedRequest.request.srcTaskId);
    EXPECT_EQ(55, forwardedRequest.request.blockIdx);
    EXPECT_EQ(66, forwardedRequest.request.execId);
    EXPECT_EQ(77, forwardedRequest.request.clientId);
    EXPECT_TRUE(forwardedRequest.request.reset);
    EXPECT_TRUE(forwardedRequest.request.reuseReader);
    EXPECT_TRUE(forwardedRequest.request.readerGroupMode);
    expectFullFetchContext(forwardedRequest.request);
  }
}

TEST(StreamContextForwardingTest, PolicyOnlyFramesAndObjectFallbackHaveSameSemantics) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  for (int32_t i = 0; i < taosArrayGetSize(runtime.runtime.pContextPolicy->pEntries); ++i) {
    auto* entry = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(runtime.runtime.pContextPolicy->pEntries, i));
    ASSERT_NE(nullptr, entry);
    entry->contextPolicy = STREAM_CONTEXT_POLICY_NONE;
  }
  tDestroyStreamAncestorContext(&runtime.runtime.pAncestorContext);

  SResFetchReq source = {};
  source.header.vgId = 8;
  source.sId = 101;
  source.queryId = 202;
  source.taskId = 303;
  source.pStRtFuncInfo = &runtime.runtime;
  source.reset = true;
  auto       bytes = serializeFetchRequest(source, true, false);
  const auto expectedFrames = extractContextFrames(bytes);
  ASSERT_FALSE(expectedFrames.empty());
  EXPECT_EQ(bytes.size(), ancestorFrameOffset(bytes));

  FetchRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReqWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
  ASSERT_NE(nullptr, decoded.request.pContextFrames);
  EXPECT_EQ(expectedFrames.size(), static_cast<size_t>(decoded.request.pContextFrames->size));
  decoded.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;

  const SStreamContextFrames* frameSources[] = {decoded.request.pContextFrames, nullptr};
  for (const SStreamContextFrames* frames : frameSources) {
    void*   message = nullptr;
    int32_t length = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamRunnerFetchReq(&decoded.request, frames, &message, &length));
    TaosMemoryPtr     messageOwner(message);
    FetchRequestGuard forwarded;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(message, length, &forwarded.request));
    expectFullFetchContext(forwarded.request, STREAM_CONTEXT_POLICY_NONE);
  }
}

TEST(StreamContextForwardingTest, BuilderRejectsContextWithoutNestedTaskFlag) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime.runtime;
  source.reset = true;
  auto bytes = serializeFetchRequest(source, true, false);

  FetchRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReqWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
  ASSERT_NE(nullptr, decoded.request.pContextFrames);
  ASSERT_FALSE(BIT_FLAG_TEST_MASK(decoded.request.pStRtFuncInfo->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tSerializeSResFetchReq(nullptr, 0, &decoded.request, true, false));

  const SStreamContextFrames* frameSources[] = {decoded.request.pContextFrames, nullptr};
  for (const SStreamContextFrames* frames : frameSources) {
    void*         message = reinterpret_cast<void*>(1);
    int32_t       length = 1;
    const int32_t code = tBuildStreamRunnerFetchReq(&decoded.request, frames, &message, &length);
    TaosMemoryPtr unexpectedMessage(code == TSDB_CODE_SUCCESS ? message : nullptr);
    EXPECT_EQ(TSDB_CODE_INVALID_PARA, code);
    EXPECT_EQ(nullptr, message);
    EXPECT_EQ(0, length);
  }
}

TEST(StreamContextForwardingTest, OptInDecodeRetainsOnlyCompleteFirstRequests) {
  TriggerCalcRequestGuard calc;
  calc.request = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&calc.request.pContextPolicy);
  tDestroyStreamAncestorContext(&calc.request.pAncestorContext);
  auto                    calcBytes = serializeCalcRequest(calc.request);
  TriggerCalcRequestGuard decodedCalc;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSTriggerCalcRequestWithContextFrames(calcBytes.data(), calcBytes.size(), &decodedCalc.request));
  EXPECT_EQ(nullptr, decodedCalc.request.pContextFrames);

  RuntimeFuncInfoGuard fullRuntime;
  fullRuntime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq nonResetSource = {};
  nonResetSource.pStRtFuncInfo = &fullRuntime.runtime;
  auto              nonResetBytes = serializeFetchRequest(nonResetSource, true, false);
  FetchRequestGuard decodedNonReset;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReqWithContextFrames(nonResetBytes.data(), nonResetBytes.size(),
                                                                         &decodedNonReset.request));
  EXPECT_EQ(nullptr, decodedNonReset.request.pContextFrames);

  RuntimeFuncInfoGuard singleRuntime;
  singleRuntime.runtime = makeSingleGroupFetchRuntime();
  SResFetchReq singleSource = {};
  singleSource.pStRtFuncInfo = &singleRuntime.runtime;
  singleSource.reset = true;
  auto              singleBytes = serializeFetchRequest(singleSource, true, false);
  FetchRequestGuard decodedSingle;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSResFetchReqWithContextFrames(singleBytes.data(), singleBytes.size(), &decodedSingle.request));
  EXPECT_EQ(nullptr, decodedSingle.request.pContextFrames);
}

TEST(StreamContextForwardingTest, BuilderRejectsFramesFromAnotherDecodedContext) {
  RuntimeFuncInfoGuard firstRuntime;
  firstRuntime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq firstSource = {};
  firstSource.pStRtFuncInfo = &firstRuntime.runtime;
  firstSource.reset = true;
  auto              firstBytes = serializeFetchRequest(firstSource, true, false);
  FetchRequestGuard first;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSResFetchReqWithContextFrames(firstBytes.data(), firstBytes.size(), &first.request));
  ASSERT_NE(nullptr, first.request.pContextFrames);
  first.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;

  RuntimeFuncInfoGuard secondRuntime;
  secondRuntime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq secondSource = {};
  secondSource.pStRtFuncInfo = &secondRuntime.runtime;
  secondSource.reset = true;
  auto              secondBytes = serializeFetchRequest(secondSource, true, false);
  FetchRequestGuard second;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSResFetchReqWithContextFrames(secondBytes.data(), secondBytes.size(), &second.request));
  ASSERT_NE(nullptr, second.request.pContextFrames);
  second.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;

  void*   message = reinterpret_cast<void*>(1);
  int32_t length = 1;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            tBuildStreamRunnerFetchReq(&first.request, second.request.pContextFrames, &message, &length));
  EXPECT_EQ(nullptr, message);
  EXPECT_EQ(0, length);
}

TEST(StreamContextForwardingTest, MalformedFrameDoesNotPublishRetainedBytes) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime.runtime;
  source.reset = true;
  auto         bytes = serializeFetchRequest(source, true, false);
  const size_t policyOffset = policyFrameOffset(bytes);
  ASSERT_LT(policyOffset + sizeof(uint32_t) + sizeof(uint16_t), bytes.size());
  const uint16_t badVersion = STREAM_CONTEXT_POLICY_FRAME_VERSION + 1;
  memcpy(bytes.data() + policyOffset + sizeof(uint32_t), &badVersion, sizeof(badVersion));

  FetchRequestGuard decoded;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG,
            tDeserializeSResFetchReqWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
  EXPECT_EQ(nullptr, decoded.request.pContextFrames);
}

TEST(StreamContextForwardingTest, OptionalCopyAllocationFailureFallsBackToObjects) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime.runtime;
  source.reset = true;
  auto       bytes = serializeFetchRequest(source, true, false);
  const auto expectedFrames = extractContextFrames(bytes);
  ASSERT_FALSE(expectedFrames.empty());

  FetchRequestGuard decoded;
  {
    MatchingAllocationFailureGuard allocationFailure(expectedFrames.size());
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tDeserializeSResFetchReqWithContextFrames(bytes.data(), bytes.size(), &decoded.request));
    EXPECT_EQ(1, gContextFrameMatchingAllocations);
  }
  EXPECT_EQ(nullptr, decoded.request.pContextFrames);
  decoded.request.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;

  void*   message = nullptr;
  int32_t length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamRunnerFetchReq(&decoded.request, nullptr, &message, &length));
  TaosMemoryPtr     messageOwner(message);
  FetchRequestGuard forwarded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(message, length, &forwarded.request));
  expectFullFetchContext(forwarded.request);
}

TEST(StreamContextForwardingTest, FinalMessageAllocationFailureLeavesOutputsEmpty) {
  RuntimeFuncInfoGuard runtime;
  runtime.runtime = makeFullMultiGroupFetchRuntime();
  SResFetchReq request = {};
  request.pStRtFuncInfo = &runtime.runtime;
  request.reset = true;
  const int32_t expectedLength = tSerializeSResFetchReq(nullptr, 0, &request, true, false);
  ASSERT_GT(expectedLength, 0);

  void*   message = reinterpret_cast<void*>(1);
  int32_t length = 1;
  {
    MatchingAllocationFailureGuard allocationFailure(expectedLength);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tBuildStreamRunnerFetchReq(&request, nullptr, &message, &length));
    EXPECT_EQ(1, gContextFrameMatchingAllocations);
  }
  EXPECT_EQ(nullptr, message);
  EXPECT_EQ(0, length);
}

TEST(StreamCalcRequestAdmissionTest, OrdinaryMultiGroupRoundTripsWithoutNestedContext) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&decoded));
  EXPECT_EQ(nullptr, decoded.pContextPolicy);
  EXPECT_EQ(nullptr, decoded.pAncestorContext);
  EXPECT_EQ(2, tSimpleHashGetSize(decoded.pGroupCalcInfos));
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamCalcRequestAdmissionTest, NestedMultiGroupRoundTripsEveryGidAndParamIndex) {
  SSTriggerCalcRequest source = makeLineageSplitMultiGroupCalcRequest();
  auto                 bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&decoded));
  const int64_t gid = 101;
  const auto*   info =
      static_cast<const SSTriggerGroupCalcInfo*>(tSimpleHashGet(decoded.pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, info);
  ASSERT_EQ(3, taosArrayGetSize(info->pParams));
  const int32_t vgId = 7;
  const auto*   readInfosPtr = static_cast<SArray**>(tSimpleHashGet(decoded.pGroupReadInfos, &vgId, sizeof(vgId)));
  ASSERT_NE(nullptr, readInfosPtr);
  ASSERT_NE(nullptr, *readInfosPtr);
  ASSERT_EQ(2, taosArrayGetSize(*readInfosPtr));
  const auto* firstRead = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(*readInfosPtr, 0));
  const auto* secondRead = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(*readInfosPtr, 1));
  ASSERT_NE(nullptr, firstRead);
  ASSERT_NE(nullptr, secondRead);
  EXPECT_EQ(gid, firstRead->gid);
  EXPECT_EQ(gid, secondRead->gid);
  const auto* firstCalcParam = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, 0));
  const auto* secondCalcParam = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, 1));
  const auto* splitCalcParam = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, 2));
  ASSERT_NE(nullptr, firstCalcParam);
  ASSERT_NE(nullptr, secondCalcParam);
  ASSERT_NE(nullptr, splitCalcParam);
  EXPECT_EQ(0, memcmp(&firstRead->firstParam, firstCalcParam, offsetof(SSTriggerCalcParam, notifyType)));
  EXPECT_EQ(0, memcmp(&firstRead->lastParam, secondCalcParam, offsetof(SSTriggerCalcParam, notifyType)));
  EXPECT_EQ(0, memcmp(&secondRead->firstParam, splitCalcParam, offsetof(SSTriggerCalcParam, notifyType)));
  EXPECT_EQ(0, memcmp(&secondRead->lastParam, splitCalcParam, offsetof(SSTriggerCalcParam, notifyType)));
  ASSERT_EQ(3, taosArrayGetSize(decoded.pContextPolicy->pEntries));
  ASSERT_EQ(3, taosArrayGetSize(decoded.pAncestorContext->pParamContexts));
  const TSKEY starts[] = {1000, 1100, 2000};
  const TSKEY ends[] = {1099, 1199, 2099};
  for (int32_t i = 0; i < 3; ++i) {
    const auto* calcParam = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, i));
    const auto* policy =
        static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(decoded.pContextPolicy->pEntries, i));
    const auto* context =
        static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, i));
    ASSERT_NE(nullptr, calcParam);
    ASSERT_NE(nullptr, policy);
    ASSERT_NE(nullptr, context);
    EXPECT_EQ(starts[i], calcParam->wstart);
    EXPECT_EQ(ends[i], calcParam->wend);
    EXPECT_EQ(gid, policy->gid);
    EXPECT_EQ(i, policy->paramIndex);
    EXPECT_EQ(STREAM_CONTEXT_POLICY_ANCESTOR, policy->contextPolicy);
    EXPECT_EQ(gid, context->leafIdentity.gid);
    EXPECT_EQ(i, context->paramIndex);
  }
  const auto* firstContext =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, 0));
  const auto* secondContext =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, 1));
  const auto* splitContext =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, 2));
  ASSERT_NE(nullptr, firstContext);
  ASSERT_NE(nullptr, secondContext);
  ASSERT_NE(nullptr, splitContext);
  EXPECT_TRUE(lineagesEqual(&firstContext->leafIdentity.lineage, &secondContext->leafIdentity.lineage));
  EXPECT_FALSE(lineagesEqual(&firstContext->leafIdentity.lineage, &splitContext->leafIdentity.lineage));
  ASSERT_EQ(2, taosArrayGetSize(decoded.pAncestorContext->pReadScopeBindings));
  for (int32_t i = 0; i < 2; ++i) {
    const auto* binding =
        static_cast<const SStreamReadScopeBinding*>(taosArrayGet(decoded.pAncestorContext->pReadScopeBindings, i));
    ASSERT_NE(nullptr, binding);
    EXPECT_EQ(7, binding->vgId);
    EXPECT_EQ(i, binding->readInfoIndex);
    EXPECT_EQ(gid, binding->scope.gid);
    const auto* expected = i == 0 ? firstContext : splitContext;
    EXPECT_TRUE(lineagesEqual(&expected->leafIdentity.lineage, &binding->scope.lineage));
  }
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamCalcRequestAdmissionTest, MultiGroupReadInfosMaySpanVnodes) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  const int32_t        vgId = 8;
  SArray*              readInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, readInfos);
  auto*         read = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(readInfos, 0));
  const int64_t gid = 101;
  auto*         info = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(source.pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, read);
  ASSERT_NE(nullptr, info);
  const auto* param = static_cast<const SSTriggerCalcParam*>(taosArrayGet(info->pParams, 0));
  ASSERT_NE(nullptr, param);
  read->gid = gid;
  read->firstParam = *param;
  read->lastParam = *param;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(source.pGroupReadInfos, &vgId, sizeof(vgId), &readInfos, POINTER_BYTES));
  appendReadBindingForParam(source.pAncestorContext, vgId, 0, 0);

  auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&decoded));
  EXPECT_EQ(2, tSimpleHashGetSize(decoded.pGroupReadInfos));
  EXPECT_EQ(3, taosArrayGetSize(decoded.pAncestorContext->pReadScopeBindings));
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamCalcRequestAdmissionTest, RejectsZeroAndMoreThan40960Params) {
  SSTriggerCalcRequest zero = makeSingleGroupCalcRequest();
  taosArrayClearEx(zero.params, tDestroySSTriggerCalcParam);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&zero));
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &zero), 0);
  tDestroySTriggerCalcRequest(&zero);

  SSTriggerCalcRequest excessive = makeSingleGroupCalcRequest();
  taosArrayDestroyEx(excessive.params, tDestroySSTriggerCalcParam);
  excessive.params = taosArrayInit_s(sizeof(SSTriggerCalcParam), STREAM_CALC_REQ_MAX_WIN_NUM + 1);
  ASSERT_NE(nullptr, excessive.params);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&excessive));
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &excessive), 0);
  tDestroySTriggerCalcRequest(&excessive);
}

TEST(StreamCalcRequestAdmissionTest, EnforcesRequestWideGroupedParamBoundary) {
  for (const auto& counts : {std::pair<int32_t, int32_t>{40959, 1}, std::pair<int32_t, int32_t>{40960, 1}}) {
    SCOPED_TRACE(counts.first);
    SSTriggerCalcRequest request = makeMultiGroupCalcRequest();
    tDestroyStreamContextPolicy(&request.pContextPolicy);
    tDestroyStreamAncestorContext(&request.pAncestorContext);
    tSimpleHashClear(request.pGroupReadInfos);
    const int64_t gids[] = {101, 202};
    const int32_t sizes[] = {counts.first, counts.second};
    for (int32_t i = 0; i < 2; ++i) {
      auto* info =
          static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(request.pGroupCalcInfos, &gids[i], sizeof(gids[i])));
      ASSERT_NE(nullptr, info);
      taosArrayDestroyEx(info->pParams, tDestroySSTriggerCalcParam);
      info->pParams = taosArrayInit_s(sizeof(SSTriggerCalcParam), sizes[i]);
      ASSERT_NE(nullptr, info->pParams);
    }
    if (counts.first == 40959) {
      EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
      EXPECT_GT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
    } else {
      EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
      EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
    }
    tDestroySTriggerCalcRequest(&request);
  }
}

TEST(StreamCalcRequestAdmissionTest, RejectsUnknownOrMismatchedReadBinding) {
  for (int32_t testCase = 0; testCase < 2; ++testCase) {
    SCOPED_TRACE(testCase);
    SSTriggerCalcRequest request = makeMultiGroupCalcRequest();
    const int32_t        vgId = 7;
    auto* readInfos = static_cast<SArray**>(tSimpleHashGet(request.pGroupReadInfos, &vgId, sizeof(vgId)));
    ASSERT_NE(nullptr, readInfos);
    ASSERT_NE(nullptr, *readInfos);
    auto* first = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(*readInfos, 0));
    ASSERT_NE(nullptr, first);
    if (testCase == 0) {
      tDestroyStreamContextPolicy(&request.pContextPolicy);
      tDestroyStreamAncestorContext(&request.pAncestorContext);
      first->gid = 303;
    } else {
      auto* binding =
          static_cast<SStreamReadScopeBinding*>(taosArrayGet(request.pAncestorContext->pReadScopeBindings, 0));
      ASSERT_NE(nullptr, binding);
      binding->scope.gid = 202;
    }
    EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
    EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
    tDestroySTriggerCalcRequest(&request);
  }
}

TEST(StreamCalcRequestAdmissionTest, RejectsReadRangeOutsideBoundLineageSegment) {
  for (int32_t testCase = 0; testCase < 2; ++testCase) {
    SCOPED_TRACE(testCase);
    SSTriggerCalcRequest request = makeLineageSplitMultiGroupCalcRequest();
    const int64_t        gid = 101;
    const int32_t        vgId = 7;
    auto* info = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(request.pGroupCalcInfos, &gid, sizeof(gid)));
    auto* readInfos = static_cast<SArray**>(tSimpleHashGet(request.pGroupReadInfos, &vgId, sizeof(vgId)));
    ASSERT_NE(nullptr, info);
    ASSERT_NE(nullptr, readInfos);
    ASSERT_NE(nullptr, *readInfos);
    auto* firstRead = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(*readInfos, 0));
    ASSERT_NE(nullptr, firstRead);
    if (testCase == 0) {
      firstRead->firstParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 1));
    } else {
      firstRead->lastParam = *static_cast<SSTriggerCalcParam*>(taosArrayGet(info->pParams, 2));
    }
    EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
    EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
    tDestroySTriggerCalcRequest(&request);
  }
}

TEST(StreamCalcRequestAdmissionTest, RejectsReadBindingForDifferentSameGidLineage) {
  SSTriggerCalcRequest request = makeLineageSplitMultiGroupCalcRequest();
  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(request.pAncestorContext->pReadScopeBindings, 0));
  const auto* splitContext =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(request.pAncestorContext->pParamContexts, 2));
  ASSERT_NE(nullptr, binding);
  ASSERT_NE(nullptr, splitContext);
  taosArrayDestroy(binding->scope.lineage.pScopes);
  binding->scope.lineage.pScopes = taosArrayDup(splitContext->leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(nullptr, binding->scope.lineage.pScopes);

  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
  tDestroySTriggerCalcRequest(&request);
}

TEST(StreamCalcRequestAdmissionTest, RejectsMixedSingleAndMultiGroupShape) {
  SSTriggerCalcRequest request = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&request.pContextPolicy);
  tDestroyStreamAncestorContext(&request.pAncestorContext);
  request.params = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, request.params);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&request));
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
  tDestroySTriggerCalcRequest(&request);
}

TEST(StreamCalcRequestAdmissionTest, LegacySingleGroupPayloadWithoutTailStillDecodes) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  auto                 bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  EXPECT_EQ(bytes.size(), policyFrameOffset(bytes));
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&decoded));
  EXPECT_EQ(1, taosArrayGetSize(decoded.params));
  EXPECT_EQ(nullptr, decoded.pContextPolicy);
  EXPECT_EQ(nullptr, decoded.pAncestorContext);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamCalcRequestAdmissionTest, MalformedOrUnknownTailFrameFailsClosed) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  auto                 bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  const auto unknown = makeUnknownCalcFrame();
  bytes.insert(bytes.end(), unknown.begin(), unknown.end());
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(bytes);

  bytes.pop_back();
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(bytes);
}

TEST(StreamCalcRequestAdmissionTest, DuplicateGroupedCalcAndVnodeWireKeysFailClosed) {
  SSTriggerCalcRequest duplicateCalc = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&duplicateCalc.pContextPolicy);
  tDestroyStreamAncestorContext(&duplicateCalc.pAncestorContext);
  tSimpleHashClear(duplicateCalc.pGroupReadInfos);
  auto calcBytes = serializeCalcRequest(duplicateCalc);
  tDestroySTriggerCalcRequest(&duplicateCalc);
  auto calcOffsets = locateGroupedCalcWireOffsets(calcBytes);
  ASSERT_EQ(2, calcOffsets.calcGids.size());
  int64_t firstGid = 0;
  memcpy(&firstGid, calcBytes.data() + calcOffsets.calcGids[0], sizeof(firstGid));
  writeI64(&calcBytes, calcOffsets.calcGids[1], firstGid);
  expectCalcRequestDecodeFailure(calcBytes);

  SSTriggerCalcRequest duplicateVnode = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&duplicateVnode.pContextPolicy);
  tDestroyStreamAncestorContext(&duplicateVnode.pAncestorContext);
  const int32_t vgId = 8;
  SArray*       readInfos = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 1);
  ASSERT_NE(nullptr, readInfos);
  static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(readInfos, 0))->gid = 101;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tSimpleHashPut(duplicateVnode.pGroupReadInfos, &vgId, sizeof(vgId), &readInfos, POINTER_BYTES));
  auto vnodeBytes = serializeCalcRequest(duplicateVnode);
  tDestroySTriggerCalcRequest(&duplicateVnode);
  auto vnodeOffsets = locateGroupedCalcWireOffsets(vnodeBytes);
  ASSERT_EQ(2, vnodeOffsets.vnodeGids.size());
  int32_t firstVgId = 0;
  memcpy(&firstVgId, vnodeBytes.data() + vnodeOffsets.vnodeGids[0], sizeof(firstVgId));
  writeI32(&vnodeBytes, vnodeOffsets.vnodeGids[1], firstVgId);
  expectCalcRequestDecodeFailure(vnodeBytes);
}

TEST(StreamCalcRequestAdmissionTest, MalformedGroupedBaseCountsAndCopiesFailClosed) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  tDestroyStreamContextPolicy(&source.pContextPolicy);
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  const int32_t vgId = 7;
  auto*         readInfos = static_cast<SArray**>(tSimpleHashGet(source.pGroupReadInfos, &vgId, sizeof(vgId)));
  ASSERT_NE(nullptr, readInfos);
  auto* readInfo = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(*readInfos, 0));
  ASSERT_NE(nullptr, readInfo);
  readInfo->pTables = taosArrayInit_s(sizeof(int64_t), 1);
  ASSERT_NE(nullptr, readInfo->pTables);
  *static_cast<int64_t*>(taosArrayGet(readInfo->pTables, 0)) = 9001;
  const auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  const auto offsets = locateGroupedCalcWireOffsets(bytes);
  ASSERT_EQ(1, offsets.readEntryCounts.size());
  ASSERT_EQ(2, offsets.tableCounts.size());

  for (size_t countOffset :
       {offsets.calcGroupCount, offsets.vnodeCount, offsets.readEntryCounts[0], offsets.tableCounts[0]}) {
    SCOPED_TRACE(countOffset);
    auto malformed = bytes;
    writeI32(&malformed, countOffset, -1);
    expectCalcRequestDecodeFailure(malformed);
  }

  auto overflowingTableCount = bytes;
  writeI32(&overflowingTableCount, offsets.tableCounts[0], INT32_MAX);
  expectCalcRequestDecodeFailure(overflowingTableCount);

  auto truncatedParam = bytes;
  truncatedParam.resize(offsets.tableCounts[0] - 1);
  writeI32(&truncatedParam, 0, static_cast<int32_t>(truncatedParam.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(truncatedParam);

  auto truncatedTable = bytes;
  truncatedTable.resize(offsets.tableCounts[0] + sizeof(int32_t) + sizeof(int32_t));
  writeI32(&truncatedTable, 0, static_cast<int32_t>(truncatedTable.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(truncatedTable);

  auto truncatedFrame = bytes;
  truncatedFrame.resize(offsets.tableCounts[0] + sizeof(int32_t) + sizeof(int32_t));
  expectCalcRequestDecodeFailure(truncatedFrame);
}

TEST(StreamAncestorContextTest, calcRequestRoundTripsByGidAndParamIndex) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  source.pContextPolicy = makeContextPolicy({makeContextPolicyEntry(source.gid, 0, STREAM_CONTEXT_POLICY_ANCESTOR)});
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto encoded = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(encoded.data(), encoded.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pContextPolicy);
  ASSERT_EQ(1, taosArrayGetSize(decoded.pContextPolicy->pEntries));
  ASSERT_NE(nullptr, decoded.pAncestorContext);
  ASSERT_NE(nullptr, decoded.pAncestorContext->pParamIndex);
  ASSERT_EQ(1, taosArrayGetSize(decoded.pAncestorContext->pParamContexts));
  const auto* decodedParam =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(nullptr, decodedParam);
  EXPECT_EQ(0, decodedParam->paramIndex);
  EXPECT_EQ(101, decodedParam->leafIdentity.gid);
  EXPECT_EQ(WINDOW_TYPE_COUNT, decodedParam->leafIdentity.triggerType);
  EXPECT_EQ(1000, decodedParam->leafIdentity.openingTs);
  EXPECT_EQ(17, decodedParam->leafIdentity.nativeDiscriminator);
  ASSERT_EQ(2, taosArrayGetSize(decodedParam->leafIdentity.lineage.pScopes));
  ASSERT_EQ(2, taosArrayGetSize(decodedParam->pSnapshots));
  const auto* decodedParent =
      static_cast<const SScopeInstanceId*>(taosArrayGet(decodedParam->leafIdentity.lineage.pScopes, 1));
  const auto* decodedParentSnapshot =
      static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(decodedParam->pSnapshots, 1));
  ASSERT_NE(nullptr, decodedParent);
  ASSERT_NE(nullptr, decodedParentSnapshot);
  EXPECT_EQ(WINDOW_TYPE_SESSION, decodedParent->triggerType);
  EXPECT_EQ(500, decodedParentSnapshot->values.window.start);
  EXPECT_EQ(9, decodedParentSnapshot->values.window.rownum);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, FetchFrameComesAfterLegacyBooleans) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime();
  SResFetchReq           source = {};
  source.header.vgId = 7;
  source.queryId = 11;
  source.taskId = 22;
  source.reset = true;
  source.dynTbname = true;
  source.forceFetchCompleted = true;
  source.pStRtFuncInfo = &runtime;

  SStreamContextPolicy*   policy = runtime.pContextPolicy;
  SStreamAncestorContext* context = runtime.pAncestorContext;
  const int32_t           addOptions = runtime.addOptions;
  runtime.pContextPolicy = nullptr;
  runtime.pAncestorContext = nullptr;
  runtime.addOptions = 0;
  const auto legacy = serializeFetchRequest(source, true, false);
  runtime.pContextPolicy = policy;
  runtime.pAncestorContext = context;
  runtime.addOptions = addOptions;
  const auto bytes = serializeFetchRequest(source, true, false);

  ASSERT_GT(bytes.size(), legacy.size());
  ASSERT_EQ(legacy.size(), policyFrameOffset(bytes));
  ASSERT_LT(policyFrameOffset(bytes), ancestorFrameOffset(bytes));
  ASSERT_GT(legacy.size(), sizeof(SMsgHead) + sizeof(int32_t));
  EXPECT_TRUE(std::equal(legacy.begin() + sizeof(SMsgHead) + sizeof(int32_t), legacy.end(),
                         bytes.begin() + sizeof(SMsgHead) + sizeof(int32_t)));

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  auto* decodedSnapshot = firstAncestorSnapshot(decoded.pStRtFuncInfo->pAncestorContext);
  ASSERT_NE(nullptr, decodedSnapshot);
  EXPECT_EQ(100, decodedSnapshot->values.window.start);
  tDestroySResFetchReq(&decoded);

  auto         reversed = bytes;
  const size_t policyOffset = policyFrameOffset(reversed);
  const size_t contextOffset = ancestorFrameOffset(reversed);
  ASSERT_LT(policyOffset, contextOffset);
  std::vector<uint8_t> policyFrame(reversed.begin() + policyOffset, reversed.begin() + contextOffset);
  reversed.erase(reversed.begin() + policyOffset, reversed.begin() + contextOffset);
  reversed.insert(reversed.end(), policyFrame.begin(), policyFrame.end());
  expectFetchDecodeFailure(reversed);

  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, FullSingleGroupFetchCoversEveryEncodedParam) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime(2);
  SResFetchReq           source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = true;
  const auto bytes = serializeFetchRequest(source, true, false);

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
  ASSERT_EQ(2, taosArrayGetSize(decoded.pStRtFuncInfo->pContextPolicy->pEntries));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  ASSERT_EQ(2, taosArrayGetSize(decoded.pStRtFuncInfo->pAncestorContext->pParamContexts));
  for (int32_t i = 0; i < 2; ++i) {
    const auto* param = static_cast<const SStreamAncestorParamContext*>(
        taosArrayGet(decoded.pStRtFuncInfo->pAncestorContext->pParamContexts, i));
    ASSERT_NE(nullptr, param);
    EXPECT_EQ(runtime.groupId, param->leafIdentity.gid);
    EXPECT_EQ(i, param->paramIndex);
  }
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, FetchRejectsPolicyThatDoesNotCoverCarrierProjection) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime(2);
  ASSERT_NE(taosArrayPop(runtime.pContextPolicy->pEntries), nullptr);
  auto* removedContext =
      static_cast<SStreamAncestorParamContext*>(taosArrayPop(runtime.pAncestorContext->pParamContexts));
  ASSERT_NE(removedContext, nullptr);
  taosArrayDestroy(removedContext->leafIdentity.lineage.pScopes);
  taosArrayDestroy(removedContext->pSnapshots);

  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = true;
  EXPECT_LT(tSerializeSResFetchReq(nullptr, 0, &source, true, false), 0);
  tDestroyStRtFuncInfo(&runtime);

  SStreamRuntimeFuncInfo  fullRuntime = makeSingleGroupFetchRuntime(2);
  SStreamContextPolicy*   fullPolicy = fullRuntime.pContextPolicy;
  SStreamAncestorContext* fullContext = fullRuntime.pAncestorContext;
  fullRuntime.pContextPolicy = nullptr;
  fullRuntime.pAncestorContext = nullptr;
  fullRuntime.addOptions = 0;
  SResFetchReq fullSource = {};
  fullSource.pStRtFuncInfo = &fullRuntime;
  fullSource.reset = true;
  auto malformed = serializeFetchRequest(fullSource, true, false);
  fullRuntime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  fullRuntime.pContextPolicy = fullPolicy;
  fullRuntime.pAncestorContext = fullContext;

  SStreamRuntimeFuncInfo oneRuntime = makeSingleGroupFetchRuntime(1);
  SResFetchReq           oneSource = {};
  oneSource.pStRtFuncInfo = &oneRuntime;
  oneSource.reset = true;
  const auto   oneParam = serializeFetchRequest(oneSource, true, false);
  const size_t tailOffset = policyFrameOffset(oneParam);
  ASSERT_LT(tailOffset, oneParam.size());
  malformed.insert(malformed.end(), oneParam.begin() + tailOffset, oneParam.end());
  writeI32(&malformed, sizeof(SMsgHead), static_cast<int32_t>(malformed.size() - sizeof(SMsgHead) - sizeof(int32_t)));
  reinterpret_cast<SMsgHead*>(malformed.data())->contLen = htonl(malformed.size());
  expectFetchDecodeFailure(malformed);

  tDestroyStRtFuncInfo(&oneRuntime);
  tDestroyStRtFuncInfo(&fullRuntime);
}

TEST(StreamAncestorContextTest, FetchProjectsExplicitMixedPolicy) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime(2);
  auto* first = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(runtime.pContextPolicy->pEntries, 0));
  ASSERT_NE(nullptr, first);
  first->contextPolicy = STREAM_CONTEXT_POLICY_NONE;
  tDestroyStreamAncestorContext(&runtime.pAncestorContext);
  runtime.pAncestorContext = makeAncestorContext(runtime.groupId, 1);

  SStreamContextPolicy*   projectedPolicy = nullptr;
  SStreamAncestorContext* projectedContext = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tProjectStreamCalcContextForFetch(&runtime, true, false, &projectedPolicy, &projectedContext));
  ASSERT_NE(nullptr, projectedPolicy);
  ASSERT_EQ(2, taosArrayGetSize(projectedPolicy->pEntries));
  ASSERT_NE(nullptr, projectedContext);
  ASSERT_EQ(1, taosArrayGetSize(projectedContext->pParamContexts));
  const auto* projectedParam =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(projectedContext->pParamContexts, 0));
  ASSERT_NE(nullptr, projectedParam);
  EXPECT_EQ(1, projectedParam->paramIndex);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(projectedPolicy, projectedContext, true));
  tDestroyStreamContextPolicy(&projectedPolicy);
  tDestroyStreamAncestorContext(&projectedContext);

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tProjectStreamCalcContextForFetch(&runtime, false, false, &projectedPolicy, &projectedContext));
  ASSERT_NE(nullptr, projectedPolicy);
  ASSERT_EQ(1, taosArrayGetSize(projectedPolicy->pEntries));
  const auto* cacheEntry = static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(projectedPolicy->pEntries, 0));
  ASSERT_NE(nullptr, cacheEntry);
  EXPECT_EQ(runtime.curIdx, cacheEntry->paramIndex);
  EXPECT_EQ(STREAM_CONTEXT_POLICY_ANCESTOR, cacheEntry->contextPolicy);
  ASSERT_NE(nullptr, projectedContext);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(projectedPolicy, projectedContext, true));
  tDestroyStreamContextPolicy(&projectedPolicy);
  tDestroyStreamAncestorContext(&projectedContext);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, ReaderBindingProjectionKeepsOnlyCanonicalDependency) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  installActualNodeReadView(&runtime, 7);

  SStreamContextPolicy*   projectedPolicy = nullptr;
  SStreamAncestorContext* projectedContext = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tProjectStreamCalcContextForFetch(&runtime, true, true, &projectedPolicy, &projectedContext));
  ASSERT_NE(nullptr, projectedPolicy);
  ASSERT_EQ(5, taosArrayGetSize(projectedPolicy->pEntries));
  ASSERT_NE(nullptr, projectedContext);
  EXPECT_EQ(5, taosArrayGetSize(projectedContext->pParamContexts));
  EXPECT_EQ(3, taosArrayGetSize(projectedContext->pReadScopeBindings));
  const auto* projectedBinding =
      static_cast<const SStreamReadScopeBinding*>(taosArrayGet(projectedContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, projectedBinding);
  EXPECT_EQ(0, projectedBinding->readInfoIndex);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(projectedPolicy, projectedContext, true));

  tDestroyStreamContextPolicy(&projectedPolicy);
  tDestroyStreamAncestorContext(&projectedContext);

  auto* sourceBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, sourceBinding);
  sourceBinding->readInfoIndex = 1;
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tProjectStreamCalcContextForFetch(&runtime, true, true, &projectedPolicy, &projectedContext));
  EXPECT_EQ(nullptr, projectedPolicy);
  EXPECT_EQ(nullptr, projectedContext);

  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, FullMultiGroupFetchCarriesEveryPolicyKey) {
  SSTriggerCalcRequest   request = makeMultiGroupCalcRequest();
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.sessionId = request.sessionId;
  runtime.triggerType = request.triggerType;
  runtime.isWindowTrigger = request.isWindowTrigger;
  TSWAP(runtime.pGroupCalcInfos, request.pGroupCalcInfos);
  TSWAP(runtime.pGroupReadInfos, request.pGroupReadInfos);
  TSWAP(runtime.pContextPolicy, request.pContextPolicy);
  TSWAP(runtime.pAncestorContext, request.pAncestorContext);
  tDestroySTriggerCalcRequest(&request);

  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = false;
  const auto   bytes = serializeFetchRequest(source, true, true);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
  EXPECT_EQ(2, taosArrayGetSize(decoded.pStRtFuncInfo->pContextPolicy->pEntries));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  EXPECT_EQ(2, taosArrayGetSize(decoded.pStRtFuncInfo->pAncestorContext->pParamContexts));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tAdmitStreamContext(decoded.pStRtFuncInfo->pContextPolicy, decoded.pStRtFuncInfo->pAncestorContext, true));
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamFetchGroupProjectionTest, FetchRoundTripCarriesEveryGidForActualNode) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  installActualNodeReadView(&runtime, 7);
  auto* triggerRead = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(runtime.curGrpRead, 0));
  ASSERT_NE(nullptr, triggerRead);
  triggerRead->pTables = taosArrayInit(1, sizeof(uint64_t));
  ASSERT_NE(nullptr, triggerRead->pTables);
  const uint64_t uid = 9001;
  ASSERT_NE(nullptr, taosArrayPush(triggerRead->pTables, &uid));
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.reset = true;
  source.pStRtFuncInfo = &runtime;
  const auto bytes = serializeFetchRequest(source, true, true);

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->curGrpRead);
  ASSERT_EQ(3, taosArrayGetSize(decoded.pStRtFuncInfo->curGrpRead));
  EXPECT_EQ(2, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
  EXPECT_EQ(5, taosArrayGetSize(decoded.pStRtFuncInfo->pContextPolicy->pEntries));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  EXPECT_EQ(5, taosArrayGetSize(decoded.pStRtFuncInfo->pAncestorContext->pParamContexts));
  tDestroySResFetchReq(&decoded);
  taosArrayDestroy(triggerRead->pTables);
  triggerRead->pTables = nullptr;
  tDestroyStRtFuncInfo(&runtime);
}

class StreamReaderFetchBuildTest : public testing::Test {
 protected:
  void SetUp() override {
    source = makeActualNodeMultiGroupFetchRuntime();
    request.readerFetchId = 200;
    request.pStRtFuncInfo = &source;
  }

  void TearDown() override {
    tDestroySResFetchReq(&decoded);
    taosMemoryFree(wire);
    tDestroyStRtFuncInfo(&source);
  }

  SStreamRuntimeFuncInfo source = {};
  SResFetchReq           request = {};
  SResFetchReq           decoded = {};
  void*                  wire = nullptr;
  int32_t                wireLen = 0;
};

TEST_F(StreamReaderFetchBuildTest, MissingIdOrContinuationReuseDoesNotPublishMessage) {
  request.readerFetchId = 0;
  EXPECT_EQ(tBuildStreamReaderFetchReq(&request, &wire, &wireLen), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(wire, nullptr);
  EXPECT_EQ(wireLen, 0);
  request.readerFetchId = 200;
  request.reuseReader = true;
  EXPECT_EQ(tBuildStreamReaderFetchReq(&request, &wire, &wireLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(wire, nullptr);
  EXPECT_EQ(wireLen, 0);
}

TEST_F(StreamReaderFetchBuildTest, ThinContinuationHasNoContext) {
  source.streamGen = 12;
  source.sessionId = 2;
  source.curIdx = 3;
  request.header.vgId = 8;
  request.readerGroupMode = true;
  ASSERT_EQ(tBuildStreamReaderFetchReq(&request, &wire, &wireLen), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tDeserializeSResFetchReq(wire, wireLen, &decoded), TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded.readerFetchId, 200);
  EXPECT_TRUE(decoded.readerGroupMode);
  auto* rt = decoded.pStRtFuncInfo;
  ASSERT_NE(rt, nullptr);
  EXPECT_TRUE(rt->isMultiGroupCalc);
  EXPECT_EQ(rt->streamGen, 12);
  EXPECT_EQ(rt->sessionId, 2);
  EXPECT_EQ(rt->curIdx, 3);
  EXPECT_EQ(rt->pGroupCalcInfos, nullptr);
  EXPECT_EQ(rt->pGroupReadInfos, nullptr);
  EXPECT_EQ(rt->curGrpRead, nullptr);
  EXPECT_EQ(rt->pContextPolicy, nullptr);
  EXPECT_EQ(rt->pAncestorContext, nullptr);
  EXPECT_EQ(decoded.pContextFrames, nullptr);
}

TEST(StreamReaderFetchBuildTestProtocol, RejectsMalformedReaderFetchIdFrame) {
  SResFetchReq source = {};
  source.readerFetchId = 200;
  const auto   original = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(original, STREAM_READER_FETCH_ID_FRAME_MAGIC);
  ASSERT_LT(offset, original.size());
  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&duplicate);
  expectFetchDecodeFailure(duplicate);
  auto badVersion = original;
  writeU16(&badVersion, offset + 4, 2);
  expectFetchDecodeFailure(badVersion);
  auto badFlags = original;
  writeU16(&badFlags, offset + 6, 1);
  expectFetchDecodeFailure(badFlags);
  auto zero = original;
  std::fill(zero.end() - 8, zero.end(), 0);
  expectFetchDecodeFailure(zero);
  auto truncated = original;
  truncated.pop_back();
  rewriteFetchRequestLengths(&truncated);
  expectFetchDecodeFailure(truncated);
  auto extra = original;
  extra.push_back(1);
  writeU32(&extra, offset + 8, 9);
  rewriteFetchRequestLengths(&extra);
  expectFetchDecodeFailure(extra);
}

TEST_F(StreamReaderFetchBuildTest, NodeProjectionKeepsWholeCrossNodeGroup) {
  request.reset = true;
  request.header.vgId = 8;
  installActualNodeReadView(&source, 8);
  auto* firstContext =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(source.pAncestorContext->pParamContexts, 0));
  auto* lastContext =
      static_cast<SStreamAncestorParamContext*>(taosArrayGetLast(source.pAncestorContext->pParamContexts));
  auto* firstBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(source.pAncestorContext->pReadScopeBindings, 0));
  auto* lastBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGetLast(source.pAncestorContext->pReadScopeBindings));
  ASSERT_NE(firstContext, nullptr);
  ASSERT_NE(lastContext, nullptr);
  ASSERT_NE(firstBinding, nullptr);
  ASSERT_NE(lastBinding, nullptr);
  std::swap(*firstContext, *lastContext);
  std::swap(*firstBinding, *lastBinding);
  auto* const sourceCalcInfos = source.pGroupCalcInfos;
  auto* const sourcePolicy = source.pContextPolicy;
  auto* const sourceAncestor = source.pAncestorContext;
  ASSERT_EQ(tBuildStreamReaderFetchReq(&request, &wire, &wireLen), TSDB_CODE_SUCCESS);
  EXPECT_EQ(source.pGroupCalcInfos, sourceCalcInfos);
  EXPECT_EQ(source.pContextPolicy, sourcePolicy);
  EXPECT_EQ(source.pAncestorContext, sourceAncestor);
  EXPECT_EQ(tSimpleHashGetSize(source.pGroupCalcInfos), 2);
  tDestroyStRtFuncInfo(&source);
  memset(&source, 0, sizeof(source));
  request.pStRtFuncInfo = nullptr;
  ASSERT_EQ(tDeserializeSResFetchReq(wire, wireLen, &decoded), TSDB_CODE_SUCCESS);
  auto* rt = decoded.pStRtFuncInfo;
  ASSERT_NE(rt, nullptr);
  EXPECT_EQ(tSimpleHashGetSize(rt->pGroupCalcInfos), 1);
  const int64_t gid = 202;
  auto*         calc = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(rt->pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(calc, nullptr);
  EXPECT_EQ(taosArrayGetSize(calc->pParams), 2);
  EXPECT_EQ(taosArrayGetSize(rt->pContextPolicy->pEntries), 2);
  EXPECT_EQ(taosArrayGetSize(rt->pAncestorContext->pParamContexts), 2);
  EXPECT_EQ(taosArrayGetSize(rt->pAncestorContext->pReadScopeBindings), 1);
}

TEST_F(StreamReaderFetchBuildTest, RejectsInvalidUnselectedGroup) {
  request.reset = true;
  request.header.vgId = 8;
  installActualNodeReadView(&source, 8);
  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(source.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(binding, nullptr);
  taosArrayDestroy(binding->scope.lineage.pScopes);
  binding->scope.lineage.pScopes = nullptr;
  taosArrayRemove(source.pAncestorContext->pReadScopeBindings, 0);

  EXPECT_NE(tBuildStreamReaderFetchReq(&request, &wire, &wireLen), TSDB_CODE_SUCCESS);
  EXPECT_EQ(wire, nullptr);
  EXPECT_EQ(wireLen, 0);
}

TEST_F(StreamReaderFetchBuildTest, AllocationFailureLeavesOutputsEmptyAndSourceReusable) {
  request.reset = true;
  request.header.vgId = 8;
  installActualNodeReadView(&source, 8);
  auto* const sourceCalcInfos = source.pGroupCalcInfos;
  auto* const sourcePolicy = source.pContextPolicy;
  auto* const sourceAncestor = source.pAncestorContext;
  bool        completedPrepareFailurePoints = false;

  for (int32_t allocationToFail = 0; allocationToFail < 256; ++allocationToFail) {
    SStreamReaderFetchView view = {};
    int32_t                code = TSDB_CODE_SUCCESS;
    bool                   allocationFailed = false;
    {
      NthAllocationFailureGuard allocationFailure(allocationToFail);
      code = tPrepareStreamReaderFetchView(&source, 8, &view);
      allocationFailed = allocationFailure.failed();
    }

    if (!allocationFailed) {
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
      tClearStreamReaderFetchView(&view);
      completedPrepareFailurePoints = true;
      break;
    }
    EXPECT_NE(code, TSDB_CODE_SUCCESS) << "prepare allocation " << allocationToFail;
    EXPECT_EQ(view.runtime.pGroupCalcInfos, nullptr);
    EXPECT_EQ(view.runtime.pGroupReadInfos, nullptr);
    EXPECT_EQ(view.policy.pEntries, nullptr);
    EXPECT_EQ(view.ancestor.pParamContexts, nullptr);
    EXPECT_EQ(view.ancestor.pReadScopeBindings, nullptr);
    tClearStreamReaderFetchView(&view);
  }
  ASSERT_TRUE(completedPrepareFailurePoints);

  bool completedBuildFailurePoints = false;

  for (int32_t allocationToFail = 0; allocationToFail < 256; ++allocationToFail) {
    int32_t code = TSDB_CODE_SUCCESS;
    bool    allocationFailed = false;
    {
      NthAllocationFailureGuard allocationFailure(allocationToFail);
      code = tBuildStreamReaderFetchReq(&request, &wire, &wireLen);
      allocationFailed = allocationFailure.failed();
    }

    if (!allocationFailed) {
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
      completedBuildFailurePoints = true;
      break;
    }
    EXPECT_NE(code, TSDB_CODE_SUCCESS) << "allocation " << allocationToFail;
    EXPECT_EQ(wire, nullptr) << "allocation " << allocationToFail;
    EXPECT_EQ(wireLen, 0) << "allocation " << allocationToFail;
    EXPECT_EQ(source.pGroupCalcInfos, sourceCalcInfos);
    EXPECT_EQ(source.pContextPolicy, sourcePolicy);
    EXPECT_EQ(source.pAncestorContext, sourceAncestor);
    EXPECT_EQ(tSimpleHashGetSize(source.pGroupCalcInfos), 2);
  }

  EXPECT_TRUE(completedBuildFailurePoints);
}

TEST(StreamFetchGroupProjectionTest, FetchRoundTripKeepsOriginalReadInfoIndexes) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  installActualNodeReadView(&runtime, 7);
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.reset = true;
  source.pStRtFuncInfo = &runtime;
  const auto bytes = serializeFetchRequest(source, true, true);

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->curGrpRead);
  const auto* first = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(decoded.pStRtFuncInfo->curGrpRead, 0));
  const auto* last = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(decoded.pStRtFuncInfo->curGrpRead, 2));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, last);
  EXPECT_EQ(101, first->gid);
  EXPECT_EQ(202, last->gid);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  const auto* bindings = decoded.pStRtFuncInfo->pAncestorContext->pReadScopeBindings;
  ASSERT_NE(nullptr, bindings);
  ASSERT_EQ(3, taosArrayGetSize(bindings));
  const auto* sparseBinding = static_cast<const SStreamReadScopeBinding*>(taosArrayGet(bindings, 2));
  ASSERT_NE(nullptr, sparseBinding);
  EXPECT_EQ(7, sparseBinding->vgId);
  EXPECT_EQ(2, sparseBinding->readInfoIndex);
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamFetchGroupProjectionTest, FetchRoundTripKeepsLineageSplitEntriesPerGid) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  installActualNodeReadView(&runtime, 7);
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.reset = true;
  source.pStRtFuncInfo = &runtime;
  const auto bytes = serializeFetchRequest(source, true, true);

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->curGrpRead);
  const auto* first = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(decoded.pStRtFuncInfo->curGrpRead, 0));
  const auto* second = static_cast<const SSTriggerGroupReadInfo*>(taosArrayGet(decoded.pStRtFuncInfo->curGrpRead, 1));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(101, first->gid);
  EXPECT_EQ(101, second->gid);
  EXPECT_NE(first->firstParam.wstart, second->firstParam.wstart);
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

template <typename MeasureCpuTicks>
void expectLinearProjectionScaling(MeasureCpuTicks measureCpuTicks) {
  constexpr int32_t kSmallCount = 4096;
  constexpr int32_t kLargeCount = 8192;
  std::array<double, 5> smallTicks = {};
  std::array<double, 5> largeTicks = {};

  // Warm both sizes, then use paired samples to limit allocator and CI load noise.
  measureCpuTicks(kSmallCount);
  measureCpuTicks(kLargeCount);
  for (size_t i = 0; i < smallTicks.size(); ++i) {
    smallTicks[i] = measureCpuTicks(kSmallCount);
    largeTicks[i] = measureCpuTicks(kLargeCount);
  }
  std::sort(smallTicks.begin(), smallTicks.end());
  std::sort(largeTicks.begin(), largeTicks.end());
  const double smallMedianTicks = smallTicks[smallTicks.size() / 2];
  const double large = largeTicks[largeTicks.size() / 2];
  ASSERT_GT(smallMedianTicks, 0);
  EXPECT_LT(large, smallMedianTicks * 3.25);
}

TEST(StreamPreparedFetchContextTest, MissingNodeDoesNotPublishSelection) {
  auto                         source = makeReaderFetchScaleRuntime(128, 3, ReaderFetchLayout::Even, 3);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  ASSERT_NE(nullptr, prepared);
  const SStreamFetchContextSelection* selected = nullptr;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tSelectStreamFetchContextForNode(prepared, &source, 99, &selected));
  EXPECT_EQ(nullptr, selected);
  tDestroyStreamFetchContext(&prepared);
  EXPECT_EQ(nullptr, prepared);
  tDestroyStRtFuncInfo(&source);
}

std::vector<uint8_t> encodePreparedFetchContext(const SStreamFetchContextSelection* selected) {
  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamFetchContext(&sizer, selected));
  std::vector<uint8_t> bytes(sizer.pos);
  tEncoderClear(&sizer);
  SEncoder encoder = {};
  tEncoderInit(&encoder, bytes.data(), bytes.size());
  EXPECT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamFetchContext(&encoder, selected));
  EXPECT_EQ(bytes.size(), encoder.pos);
  tEncoderClear(&encoder);
  return bytes;
}

int32_t decodePreparedFetchContext(const std::vector<uint8_t>& bytes, SStreamContextPolicy** ppPolicy,
                                   SStreamAncestorContext** ppAncestor) {
  *ppPolicy = nullptr;
  *ppAncestor = nullptr;
  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(bytes.data()), bytes.size());
  SStreamTailFrameDecoder frame = {};
  int32_t                 code = tDecodeNextStreamTailFrame(&decoder, &frame);
  if (code == TSDB_CODE_SUCCESS) {
    code = frame.magic == STREAM_CONTEXT_POLICY_FRAME_MAGIC
               ? tDecodeStreamContextPolicy(&frame.payloadDecoder, ppPolicy)
               : TSDB_CODE_INVALID_MSG;
    const int32_t finish = tFinishDecodeStreamTailFrame(&frame, code == TSDB_CODE_SUCCESS);
    if (code == TSDB_CODE_SUCCESS) code = finish;
  }
  if (code == TSDB_CODE_SUCCESS && !tDecodeIsEnd(&decoder)) {
    code = tDecodeNextStreamTailFrame(&decoder, &frame);
    if (code == TSDB_CODE_SUCCESS) {
      code = frame.magic == STREAM_ANCESTOR_FRAME_MAGIC
                 ? tDecodeStreamAncestorContext(&frame.payloadDecoder, ppAncestor)
                 : TSDB_CODE_INVALID_MSG;
      const int32_t finish = tFinishDecodeStreamTailFrame(&frame, code == TSDB_CODE_SUCCESS);
      if (code == TSDB_CODE_SUCCESS) code = finish;
    }
  }
  if (code == TSDB_CODE_SUCCESS && !tDecodeIsEnd(&decoder)) code = TSDB_CODE_INVALID_MSG;
  tDecoderClear(&decoder);
  if (code != TSDB_CODE_SUCCESS) {
    tDestroyStreamContextPolicy(ppPolicy);
    tDestroyStreamAncestorContext(ppAncestor);
  }
  return code;
}

void expectPreparedPoliciesEqual(const SStreamContextPolicy* expected, const SStreamContextPolicy* actual) {
  ASSERT_NE(nullptr, expected);
  ASSERT_NE(nullptr, actual);
  ASSERT_EQ(taosArrayGetSize(expected->pEntries), taosArrayGetSize(actual->pEntries));
  for (int32_t i = 0; i < taosArrayGetSize(expected->pEntries); ++i) {
    SCOPED_TRACE(i);
    const auto* left = static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(expected->pEntries, i));
    const auto* right = static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(actual->pEntries, i));
    ASSERT_NE(nullptr, left);
    ASSERT_NE(nullptr, right);
    EXPECT_EQ(left->gid, right->gid);
    EXPECT_EQ(left->paramIndex, right->paramIndex);
    EXPECT_EQ(left->contextPolicy, right->contextPolicy);
  }
}

void expectPreparedAncestorsEqual(const SStreamAncestorContext* expected, const SStreamAncestorContext* actual) {
  if (expected == nullptr || actual == nullptr) {
    EXPECT_EQ(expected, actual);
    return;
  }
  ASSERT_EQ(taosArrayGetSize(expected->pParamContexts), taosArrayGetSize(actual->pParamContexts));
  for (int32_t i = 0; i < taosArrayGetSize(expected->pParamContexts); ++i) {
    SCOPED_TRACE(i);
    const auto* left = static_cast<const SStreamAncestorParamContext*>(taosArrayGet(expected->pParamContexts, i));
    const auto* right = static_cast<const SStreamAncestorParamContext*>(taosArrayGet(actual->pParamContexts, i));
    ASSERT_NE(nullptr, left);
    ASSERT_NE(nullptr, right);
    EXPECT_EQ(left->paramIndex, right->paramIndex);
    EXPECT_EQ(left->leafIdentity.gid, right->leafIdentity.gid);
    EXPECT_EQ(left->leafIdentity.triggerType, right->leafIdentity.triggerType);
    EXPECT_EQ(left->leafIdentity.openingTs, right->leafIdentity.openingTs);
    EXPECT_EQ(left->leafIdentity.nativeDiscriminator, right->leafIdentity.nativeDiscriminator);
    EXPECT_TRUE(lineagesEqual(&left->leafIdentity.lineage, &right->leafIdentity.lineage));
    ASSERT_EQ(taosArrayGetSize(left->pSnapshots), taosArrayGetSize(right->pSnapshots));
    for (int32_t j = 0; j < taosArrayGetSize(left->pSnapshots); ++j) {
      SCOPED_TRACE(j);
      const auto* oldSnapshot = static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(left->pSnapshots, j));
      const auto* newSnapshot = static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(right->pSnapshots, j));
      ASSERT_NE(nullptr, oldSnapshot);
      ASSERT_NE(nullptr, newSnapshot);
      EXPECT_EQ(oldSnapshot->layerIndex, newSnapshot->layerIndex);
      EXPECT_EQ(oldSnapshot->triggerType, newSnapshot->triggerType);
      EXPECT_EQ(oldSnapshot->placeholderMask, newSnapshot->placeholderMask);
      const int64_t slidingMask = PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS;
      if ((oldSnapshot->placeholderMask & slidingMask) != 0) {
        EXPECT_EQ(oldSnapshot->values.sliding.prevTs, newSnapshot->values.sliding.prevTs);
        EXPECT_EQ(oldSnapshot->values.sliding.currentTs, newSnapshot->values.sliding.currentTs);
        EXPECT_EQ(oldSnapshot->values.sliding.nextTs, newSnapshot->values.sliding.nextTs);
      } else {
        EXPECT_EQ(oldSnapshot->values.window.start, newSnapshot->values.window.start);
        EXPECT_EQ(oldSnapshot->values.window.end, newSnapshot->values.window.end);
        EXPECT_EQ(oldSnapshot->values.window.duration, newSnapshot->values.window.duration);
        EXPECT_EQ(oldSnapshot->values.window.rownum, newSnapshot->values.window.rownum);
      }
    }
  }
  ASSERT_EQ(taosArrayGetSize(expected->pReadScopeBindings), taosArrayGetSize(actual->pReadScopeBindings));
  for (int32_t i = 0; i < taosArrayGetSize(expected->pReadScopeBindings); ++i) {
    SCOPED_TRACE(i);
    const auto* left = static_cast<const SStreamReadScopeBinding*>(taosArrayGet(expected->pReadScopeBindings, i));
    const auto* right = static_cast<const SStreamReadScopeBinding*>(taosArrayGet(actual->pReadScopeBindings, i));
    ASSERT_NE(nullptr, left);
    ASSERT_NE(nullptr, right);
    EXPECT_EQ(left->vgId, right->vgId);
    EXPECT_EQ(left->readInfoIndex, right->readInfoIndex);
    EXPECT_EQ(left->scope.gid, right->scope.gid);
    EXPECT_TRUE(lineagesEqual(&left->scope.lineage, &right->scope.lineage));
  }
}

void expectPreparedNodeWireMatchesLegacy(SStreamRuntimeFuncInfo* source, int32_t nodeId) {
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(source, nullptr, &prepared));
  installActualNodeReadView(source, nodeId);
  SStreamReaderFetchView legacy = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamReaderFetchView(source, nodeId, &legacy));
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, source, nodeId, &selected));
  const auto              bytes = encodePreparedFetchContext(selected);
  SStreamContextPolicy*   policy = nullptr;
  SStreamAncestorContext* ancestor = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePreparedFetchContext(bytes, &policy, &ancestor));
  expectPreparedPoliciesEqual(&legacy.policy, policy);
  expectPreparedAncestorsEqual(&legacy.ancestor, ancestor);
  tDestroyStreamContextPolicy(&policy);
  tDestroyStreamAncestorContext(&ancestor);
  tClearStreamReaderFetchView(&legacy);
  tDestroyStreamFetchContext(&prepared);
  tClearStreamCurrentGroupRead(source);
}

TEST(StreamPreparedFetchContextTest, PresentCorruptRefFailsButMissingRefRebuilds) {
  auto         source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 1);
  SResFetchReq request = {};
  request.reset = true;
  request.pStRtFuncInfo = &source;
  const auto   bytes = serializeFetchRequest(request, true, false);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSResFetchReqWithContextFrames(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  decoded.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  ASSERT_NE(nullptr, decoded.pContextFrames);
  ASSERT_NE(nullptr, decoded.pContextFrames->pPolicyRefs);
  struct EntryRef {
    int32_t offset;
    int32_t length;
  };
  auto* ref = static_cast<EntryRef*>(taosArrayGet(decoded.pContextFrames->pPolicyRefs, 0));
  ASSERT_NE(nullptr, ref);
  const EntryRef original = *ref;
  ref->offset = decoded.pContextFrames->size;
  ref->length = 1;
  SStreamPreparedFetchContext* prepared = nullptr;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            tPrepareStreamFetchContext(decoded.pStRtFuncInfo, decoded.pContextFrames, &prepared));
  EXPECT_EQ(nullptr, prepared);
  tDestroyStreamFetchContext(&prepared);
  *ref = original;
  SArray* savedRefs = decoded.pContextFrames->pPolicyRefs;
  decoded.pContextFrames->pPolicyRefs = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(decoded.pStRtFuncInfo, decoded.pContextFrames, &prepared));
  tDestroyStreamFetchContext(&prepared);
  decoded.pContextFrames->pPolicyRefs = savedRefs;
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, NodeAndGroupSelectionPreserveProjectionAndWireEntries) {
  auto                         source = makeReaderFetchScaleRuntime(128, 3, ReaderFetchLayout::Even, 3);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  ASSERT_NE(nullptr, prepared);
  installActualNodeReadView(&source, 1);
  SStreamReaderFetchView legacy = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamReaderFetchView(&source, 1, &legacy));

  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  ASSERT_NE(nullptr, selected);
  const auto expected = reader_fetch_benchmark::expectedSelection(
      reader_fetch_benchmark::makeScalePlan(128, 3, ReaderFetchLayout::Even), 0);
  SStreamReaderFetchView fresh = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchRuntimeView(selected, &source, &source, &fresh));
  EXPECT_EQ(expected.second, taosArrayGetSize(fresh.policy.pEntries));
  EXPECT_EQ(tSimpleHashGetSize(legacy.runtime.pGroupCalcInfos), tSimpleHashGetSize(fresh.runtime.pGroupCalcInfos));
  expectPreparedPoliciesEqual(&legacy.policy, &fresh.policy);
  expectPreparedAncestorsEqual(&legacy.ancestor, &fresh.ancestor);
  const auto encoded = encodePreparedFetchContext(selected);
  SStreamContextPolicy*   decodedPolicy = nullptr;
  SStreamAncestorContext* decodedAncestor = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePreparedFetchContext(encoded, &decodedPolicy, &decodedAncestor));
  expectPreparedPoliciesEqual(&legacy.policy, decodedPolicy);
  expectPreparedAncestorsEqual(&legacy.ancestor, decodedAncestor);
  tDestroyStreamContextPolicy(&decodedPolicy);
  tDestroyStreamAncestorContext(&decodedAncestor);

  const auto* firstGroup =
      static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(source.pContextPolicy->pEntries, 0));
  const SStreamFetchContextSelection* group = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForGroup(prepared, &source, firstGroup->gid, &group));
  ASSERT_NE(nullptr, group);
  SStreamRuntimeFuncInfo current = source;
  current.isMultiGroupCalc = false;
  current.groupId = firstGroup->gid;
  SStreamReaderFetchView groupView = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchRuntimeView(group, &source, &current, &groupView));
  EXPECT_EQ(0, taosArrayGetSize(groupView.ancestor.pReadScopeBindings));
  SStreamContextPolicy*   oldGroupPolicy = nullptr;
  SStreamAncestorContext* oldGroupAncestor = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tProjectStreamCalcContextForGroup(source.pContextPolicy, source.pAncestorContext,
                                                                 firstGroup->gid, &oldGroupPolicy, &oldGroupAncestor));
  expectPreparedPoliciesEqual(oldGroupPolicy, &groupView.policy);
  SStreamAncestorContext oldGroupWithoutBindings = *oldGroupAncestor;
  oldGroupWithoutBindings.pReadScopeBindings = nullptr;
  expectPreparedAncestorsEqual(&oldGroupWithoutBindings, &groupView.ancestor);
  const auto groupBytes = encodePreparedFetchContext(group);
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePreparedFetchContext(groupBytes, &decodedPolicy, &decodedAncestor));
  expectPreparedPoliciesEqual(oldGroupPolicy, decodedPolicy);
  expectPreparedAncestorsEqual(&oldGroupWithoutBindings, decodedAncestor);
  tDestroyStreamContextPolicy(&decodedPolicy);
  tDestroyStreamAncestorContext(&decodedAncestor);
  tDestroyStreamContextPolicy(&oldGroupPolicy);
  tDestroyStreamAncestorContext(&oldGroupAncestor);
  tClearStreamReaderFetchView(&groupView);
  tClearStreamReaderFetchView(&fresh);
  tClearStreamReaderFetchView(&legacy);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, PreparedRequestCarriesLiveGroupFieldsAndOwnsWire) {
  auto                         source = makeReaderFetchScaleRuntime(128, 3, ReaderFetchLayout::Even, 3);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  installActualNodeReadView(&source, 1);
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  SResFetchReq request = {};
  request.reset = true;
  request.readerGroupMode = true;
  request.readerFetchId = 200;
  request.header.vgId = 1;
  request.pStRtFuncInfo = &source;
  const int64_t gid = 1000;
  auto*         group = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(source.pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, group);
  for (int8_t createTable : {int8_t{0}, int8_t{1}}) {
    group->createTable = createTable;
    void*                    wire = nullptr;
    int32_t                  length = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamPreparedFetchReq(&request, &source, selected, &wire, &length));
    ASSERT_NE(nullptr, wire);
    EXPECT_GT(length, 0);
    SResFetchReq decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, length, &decoded));
    auto* actual =
        static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(decoded.pStRtFuncInfo->pGroupCalcInfos, &gid, sizeof(gid)));
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(createTable, actual->createTable);
    tDestroySResFetchReq(&decoded);
    taosMemoryFree(wire);
  }
  tClearStreamCurrentGroupRead(&source);
  installActualNodeReadView(&source, 2);
  const SStreamFetchContextSelection* secondNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 2, &secondNode));
  request.header.vgId = 2;
  void*   secondWire = nullptr;
  int32_t secondLength = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamPreparedFetchReq(&request, &source, secondNode, &secondWire, &secondLength));
  SResFetchReq secondDecoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(secondWire, secondLength, &secondDecoded));
  ASSERT_GT(taosArrayGetSize(secondDecoded.pStRtFuncInfo->curGrpRead), 0);
  auto* expectedRead = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(source.curGrpRead, 0));
  auto* actualRead = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(secondDecoded.pStRtFuncInfo->curGrpRead, 0));
  ASSERT_NE(nullptr, expectedRead);
  ASSERT_NE(nullptr, actualRead);
  EXPECT_EQ(expectedRead->gid, actualRead->gid);
  EXPECT_NE(gid, actualRead->gid);
  tDestroySResFetchReq(&secondDecoded);
  taosMemoryFree(secondWire);
  tClearStreamCurrentGroupRead(&source);
  installActualNodeReadView(&source, 1);
  request.header.vgId = 2;
  void*   wire = nullptr;
  int32_t length = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tBuildStreamPreparedFetchReq(&request, &source, selected, &wire, &length));
  EXPECT_EQ(nullptr, wire);
  request.header.vgId = 1;
  auto other = makeReaderFetchScaleRuntime(128, 3, ReaderFetchLayout::Even, 3);
  installActualNodeReadView(&other, 1);
  request.pStRtFuncInfo = &other;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tBuildStreamPreparedFetchReq(&request, &other, selected, &wire, &length));
  EXPECT_EQ(nullptr, wire);
  request.pStRtFuncInfo = &source;
  tDestroyStRtFuncInfo(&other);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamPreparedFetchReq(&request, &source, selected, &wire, &length));
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, length, &decoded));
  auto* actual =
      static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashGet(decoded.pStRtFuncInfo->pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(nullptr, actual);
  EXPECT_EQ(1, actual->createTable);
  tDestroySResFetchReq(&decoded);
  taosMemoryFree(wire);
}

TEST(StreamPreparedFetchContextTest, SharedGroupSelectionPreservesAllWindowsAcrossNodes) {
  auto                         source = makeReaderFetchScaleRuntime(128, 30, ReaderFetchLayout::SharedGroup, 1);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  ASSERT_NE(nullptr, prepared);
  for (int32_t nodeId : {1, 15, 30}) {
    const SStreamFetchContextSelection* selected = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, nodeId, &selected));
    installActualNodeReadView(&source, nodeId);
    SStreamReaderFetchView view = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchRuntimeView(selected, &source, &source, &view));
    EXPECT_EQ(128, taosArrayGetSize(view.policy.pEntries));
    tClearStreamReaderFetchView(&view);
    tClearStreamCurrentGroupRead(&source);
    EXPECT_NE(nullptr, selected);
  }
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, ContextSizerUsesCachedLengthWithoutAllocating) {
  auto                         source = makeReaderFetchScaleRuntime(128, 3, ReaderFetchLayout::Even, 3);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  SEncoder sizer = {};
  tEncoderInit(&sizer, nullptr, 0);
  bool failed = false;
  {
    NthAllocationFailureGuard allocationFailure(0);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamFetchContext(&sizer, selected));
    failed = allocationFailure.failed();
  }
  EXPECT_FALSE(failed);
  const auto length = sizer.pos;
  tEncoderClear(&sizer);
  std::vector<uint8_t> bytes(length);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, bytes.data(), bytes.size());
  ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeStreamFetchContext(&encoder, selected));
  EXPECT_EQ(length, encoder.pos);
  tEncoderClear(&encoder);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, LegacyReaderBuilderSendsSelectedContext) {
  auto source = makeReaderFetchScaleRuntime(12, 3, ReaderFetchLayout::Even, 3);
  installActualNodeReadView(&source, 1);
  SResFetchReq request = {};
  request.reset = true;
  request.readerFetchId = 200;
  request.header.vgId = 1;
  request.pStRtFuncInfo = &source;
  void*                    wire = nullptr;
  int32_t                  length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &wire, &length));
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, length, &decoded));
  EXPECT_EQ(4, taosArrayGetSize(decoded.pStRtFuncInfo->pContextPolicy->pEntries));
  tDestroySResFetchReq(&decoded);
  taosMemoryFree(wire);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, PreparedRequestAllocationFailureLeavesNoWire) {
  auto                         source = makeReaderFetchScaleRuntime(12, 3, ReaderFetchLayout::Even, 3);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  installActualNodeReadView(&source, 1);
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  SResFetchReq request = {};
  request.reset = true;
  request.readerFetchId = 200;
  request.header.vgId = 1;
  request.pStRtFuncInfo = &source;
  int32_t failedAllocations = 0;
  bool    completed = false;
  for (int32_t index = 0; index < 256; ++index) {
    void*   wire = nullptr;
    int32_t length = 0;
    int32_t code = TSDB_CODE_SUCCESS;
    bool    failed = false;
    {
      NthAllocationFailureGuard allocationFailure(index);
      code = tBuildStreamPreparedFetchReq(&request, &source, selected, &wire, &length);
      failed = allocationFailure.failed();
    }
    if (!failed) {
      EXPECT_EQ(TSDB_CODE_SUCCESS, code);
      taosMemoryFree(wire);
      completed = true;
      break;
    }
    ++failedAllocations;
    EXPECT_NE(TSDB_CODE_SUCCESS, code);
    EXPECT_EQ(nullptr, wire);
    EXPECT_EQ(0, length);
  }
  EXPECT_TRUE(completed);
  EXPECT_GT(failedAllocations, 0);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, DepthAndSharedGidWireMatchLegacyNodeProjection) {
  auto depthOne = makeReaderFetchScaleRuntime(12, 3, ReaderFetchLayout::Even, 1);
  expectPreparedNodeWireMatchesLegacy(&depthOne, 1);
  tDestroyStRtFuncInfo(&depthOne);

  auto shared = makeReaderFetchScaleRuntime(12, 3, ReaderFetchLayout::SharedGroup, 3);
  expectPreparedNodeWireMatchesLegacy(&shared, 1);
  expectPreparedNodeWireMatchesLegacy(&shared, 3);
  tDestroyStRtFuncInfo(&shared);

  auto repeatedRead = makeActualNodeMultiGroupFetchRuntime();
  expectPreparedNodeWireMatchesLegacy(&repeatedRead, 7);
  tDestroyStRtFuncInfo(&repeatedRead);
}

TEST(StreamPreparedFetchContextTest, InvalidSourceCannotBeRebuiltAsPreparedBytes) {
  auto  source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 1);
  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(source.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, binding);
  binding->readInfoIndex = 99;
  SStreamPreparedFetchContext* prepared = nullptr;
  EXPECT_NE(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  EXPECT_EQ(nullptr, prepared);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, RetainedTriggerFramesTrackPayloadAfterAncestorReorder) {
  auto  source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 3);
  auto* first = static_cast<SStreamAncestorParamContext*>(taosArrayGet(source.pAncestorContext->pParamContexts, 0));
  auto* last = static_cast<SStreamAncestorParamContext*>(taosArrayGetLast(source.pAncestorContext->pParamContexts));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, last);
  std::swap(*first, *last);
  SSTriggerCalcRequest request = {};
  request.isMultiGroupCalc = true;
  request.pGroupCalcInfos = source.pGroupCalcInfos;
  request.pGroupReadInfos = source.pGroupReadInfos;
  request.pContextPolicy = source.pContextPolicy;
  request.pAncestorContext = source.pAncestorContext;
  const auto           bytes = serializeCalcRequest(request);
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequestWithContextFrames(const_cast<uint8_t*>(bytes.data()),
                                                                                bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pContextFrames);
  EXPECT_NE(nullptr, decoded.pContextFrames->pPolicyRefs);
  EXPECT_NE(nullptr, decoded.pContextFrames->pParamRefs);
  EXPECT_NE(nullptr, decoded.pContextFrames->pBindingRefs);
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  runtime.pGroupCalcInfos = decoded.pGroupCalcInfos;
  runtime.pGroupReadInfos = decoded.pGroupReadInfos;
  runtime.pContextPolicy = decoded.pContextPolicy;
  runtime.pAncestorContext = decoded.pAncestorContext;
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&runtime, decoded.pContextFrames, &prepared));
  ASSERT_NE(nullptr, prepared);
  installActualNodeReadView(&runtime, 1);
  SStreamReaderFetchView legacy = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamReaderFetchView(&runtime, 1, &legacy));
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &runtime, 1, &selected));
  const auto              encoded = encodePreparedFetchContext(selected);
  SStreamContextPolicy*   policy = nullptr;
  SStreamAncestorContext* ancestor = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodePreparedFetchContext(encoded, &policy, &ancestor));
  expectPreparedPoliciesEqual(&legacy.policy, policy);
  expectPreparedAncestorsEqual(&legacy.ancestor, ancestor);
  tDestroyStreamContextPolicy(&policy);
  tDestroyStreamAncestorContext(&ancestor);
  tClearStreamReaderFetchView(&legacy);
  tClearStreamCurrentGroupRead(&runtime);
  tDestroyStreamFetchContext(&prepared);
  tDestroySTriggerCalcRequest(&decoded);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, RetainedRunnerFetchFramesCarrySelectableOffsets) {
  auto         source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 1);
  SResFetchReq request = {};
  request.reset = true;
  request.pStRtFuncInfo = &source;
  const auto   bytes = serializeFetchRequest(request, true, false);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSResFetchReqWithContextFrames(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pContextFrames);
  ASSERT_NE(nullptr, decoded.pContextFrames->pPolicyRefs);
  ASSERT_NE(nullptr, decoded.pContextFrames->pParamRefs);
  ASSERT_NE(nullptr, decoded.pContextFrames->pBindingRefs);
  decoded.pStRtFuncInfo->addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(decoded.pStRtFuncInfo, decoded.pContextFrames, &prepared));
  ASSERT_NE(nullptr, prepared);
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, decoded.pStRtFuncInfo, 1, &selected));
  const auto encoded = encodePreparedFetchContext(selected);
  EXPECT_LT(encoded.size(), decoded.pContextFrames->size);
  tDestroyStreamFetchContext(&prepared);
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, SourceContainerChangeRejectsSelection) {
  auto                         source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 1);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  const SStreamFetchContextSelection* selected = nullptr;
  auto*                               originalReadMap = source.pGroupReadInfos;
  source.pGroupReadInfos = nullptr;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  EXPECT_EQ(nullptr, selected);
  source.pGroupReadInfos = originalReadMap;
  auto copy = source;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tSelectStreamFetchContextForNode(prepared, &copy, 1, &selected));
  EXPECT_EQ(nullptr, selected);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, CurrentReadListMustMatchPreparedNode) {
  auto                         source = makeReaderFetchScaleRuntime(6, 3, ReaderFetchLayout::Even, 1);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  installActualNodeReadView(&source, 1);
  auto* read = static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(source.curGrpRead, 0));
  ASSERT_NE(nullptr, read);
  ++read->gid;
  SStreamReaderFetchView view = {};
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tPrepareStreamFetchRuntimeView(selected, &source, &source, &view));
  EXPECT_EQ(nullptr, view.runtime.pGroupCalcInfos);
  tClearStreamReaderFetchView(&view);
  --read->gid;
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, NoAncestorOmitsAncestorFrame) {
  auto source = makeReaderFetchScaleRuntime(2, 3, ReaderFetchLayout::Even, 1);
  for (int32_t i = 0; i < taosArrayGetSize(source.pContextPolicy->pEntries); ++i) {
    auto* entry = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(source.pContextPolicy->pEntries, i));
    entry->contextPolicy = STREAM_CONTEXT_POLICY_NONE;
  }
  tDestroyStreamAncestorContext(&source.pAncestorContext);
  SStreamPreparedFetchContext* prepared = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamFetchContext(&source, nullptr, &prepared));
  const SStreamFetchContextSelection* selected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSelectStreamFetchContextForNode(prepared, &source, 1, &selected));
  const auto encoded = encodePreparedFetchContext(selected);
  SDecoder   decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(encoded.data()), encoded.size());
  SStreamTailFrameDecoder frame = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeNextStreamTailFrame(&decoder, &frame));
  SStreamContextPolicy* policy = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeStreamContextPolicy(&frame.payloadDecoder, &policy));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tFinishDecodeStreamTailFrame(&frame, true));
  EXPECT_TRUE(tDecodeIsEnd(&decoder));
  tDestroyStreamContextPolicy(&policy);
  tDecoderClear(&decoder);
  installActualNodeReadView(&source, 1);
  SResFetchReq request = {};
  request.reset = true;
  request.readerFetchId = 200;
  request.header.vgId = 1;
  request.pStRtFuncInfo = &source;
  void*   wire = nullptr;
  int32_t length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamPreparedFetchReq(&request, &source, selected, &wire, &length));
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, length, &decoded));
  EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  tDestroySResFetchReq(&decoded);
  taosMemoryFree(wire);
  tDestroyStreamFetchContext(&prepared);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamPreparedFetchContextTest, AllocationFailureDoesNotPublishPreparedState) {
  auto source = makeReaderFetchScaleRuntime(2, 3, ReaderFetchLayout::Even, 1);
  bool reachedSuccess = false;
  for (int32_t allocationToFail = 0; allocationToFail < 256; ++allocationToFail) {
    SStreamPreparedFetchContext* prepared = nullptr;
    int32_t                      code = TSDB_CODE_SUCCESS;
    bool                         failed = false;
    {
      NthAllocationFailureGuard guard(allocationToFail);
      code = tPrepareStreamFetchContext(&source, nullptr, &prepared);
      failed = guard.failed();
    }
    if (!failed) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, code);
      ASSERT_NE(nullptr, prepared);
      tDestroyStreamFetchContext(&prepared);
      reachedSuccess = true;
      break;
    }
    EXPECT_NE(TSDB_CODE_SUCCESS, code) << allocationToFail;
    EXPECT_EQ(nullptr, prepared) << allocationToFail;
    tDestroyStreamFetchContext(&prepared);
  }
  EXPECT_TRUE(reachedSuccess);
  installActualNodeReadView(&source, 1);
  SStreamReaderFetchView legacy = {};
  EXPECT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamReaderFetchView(&source, 1, &legacy));
  tClearStreamReaderFetchView(&legacy);
  tDestroyStRtFuncInfo(&source);
}

TEST(StreamReaderFetchScaleTest, BoundaryInputsPreserveSelectionAndThinContinuation) {
  struct BoundaryCase {
    int32_t           params;
    int32_t           nodes;
    ReaderFetchLayout layout;
    int32_t           depth;
  };
  int32_t thinBytes = 0;
  for (const auto& testCase :
       {BoundaryCase{2, 3, ReaderFetchLayout::Even, 1}, BoundaryCase{128, 3, ReaderFetchLayout::Skewed, 3},
        BoundaryCase{128, 3, ReaderFetchLayout::SharedGroup, 3}, BoundaryCase{40960, 30, ReaderFetchLayout::Even, 1}}) {
    SCOPED_TRACE(testCase.params);
    SStreamRuntimeFuncInfo runtime =
        makeReaderFetchScaleRuntime(testCase.params, testCase.nodes, testCase.layout, testCase.depth);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateStreamCalcContextForFetch(&runtime, true, false));
    const int32_t nodeId = 1;
    installActualNodeReadView(&runtime, nodeId);
    SResFetchReq request = {};
    request.header.vgId = nodeId;
    request.reset = true;
    request.readerGroupMode = true;
    request.readerFetchId = 300;
    request.pStRtFuncInfo = &runtime;
    const auto expected = reader_fetch_benchmark::expectedSelection(
        reader_fetch_benchmark::makeScalePlan(testCase.params, testCase.nodes, testCase.layout), nodeId - 1);
    const int32_t legacyLen = tSerializeSResFetchReq(nullptr, 0, &request, true, true);
    ASSERT_GT(legacyLen, 0);
    void*   firstWire = nullptr;
    int32_t firstLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &firstWire, &firstLen));
    SResFetchReq decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(firstWire, firstLen, &decoded));
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
    EXPECT_EQ(expected.first, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos));
    EXPECT_EQ(expected.second, reader_fetch_benchmark::paramCount(decoded.pStRtFuncInfo));
    if (expected.first < tSimpleHashGetSize(runtime.pGroupCalcInfos)) EXPECT_LT(firstLen, legacyLen);
    tDestroySResFetchReq(&decoded);
    taosMemoryFree(firstWire);

    request.reset = false;
    void*   continuationWire = nullptr;
    int32_t continuationLen = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &continuationWire, &continuationLen));
    if (thinBytes == 0) thinBytes = continuationLen;
    EXPECT_EQ(thinBytes, continuationLen);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(continuationWire, continuationLen, &decoded));
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupCalcInfos);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupReadInfos);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
    EXPECT_EQ(nullptr, decoded.pContextFrames);
    tDestroySResFetchReq(&decoded);
    taosMemoryFree(continuationWire);
    tDestroyStRtFuncInfo(&runtime);
  }
}

TEST(StreamReaderFetchBenchmark, DISABLED_Scale) {
  const std::array<int32_t, 4>           params = {2, 128, 4096, 40960};
  const std::array<int32_t, 3>           nodes = {1, 3, 30};
  const std::array<ReaderFetchLayout, 3> layouts = {ReaderFetchLayout::Even, ReaderFetchLayout::Skewed,
                                                    ReaderFetchLayout::SharedGroup};
  const std::array<int32_t, 2>           depths = {1, 3};
  int32_t                                thinBytes = 0;

  for (int32_t totalParams : params) {
    for (int32_t nodeCount : nodes) {
      for (ReaderFetchLayout layout : layouts) {
        for (int32_t ancestorDepth : depths) {
          SStreamRuntimeFuncInfo runtime = makeReaderFetchScaleRuntime(totalParams, nodeCount, layout, ancestorDepth);
          ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateStreamCalcContextForFetch(&runtime, true, false));
          for (int32_t nodeId = 1; nodeId <= nodeCount; ++nodeId) {
            auto** reads = static_cast<SArray**>(tSimpleHashGet(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId)));
            if (reads == nullptr || taosArrayGetSize(*reads) == 0) {
              reader_fetch_benchmark::emit("first", "skip", totalParams, nodeCount, layout, ancestorDepth, nodeId, 0, 0,
                                           0, 0, 0, 0, 0);
              continue;
            }
            tClearStreamCurrentGroupRead(&runtime);
            installActualNodeReadView(&runtime, nodeId);
            SStreamReaderFetchView validation = {};
            ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamReaderFetchView(&runtime, nodeId, &validation));
            tClearStreamReaderFetchView(&validation);

            SResFetchReq request = {};
            request.header.vgId = nodeId;
            request.reset = true;
            request.readerGroupMode = true;
            request.readerFetchId = 400;
            request.pStRtFuncInfo = &runtime;
            int32_t legacyBytes = 0;
            for (const char* path : {"legacy", "pruned"}) {
              auto build = [&](void** wire, int32_t* wireLen) {
                if (strcmp(path, "legacy") == 0) {
                  *wireLen = tSerializeSResFetchReq(nullptr, 0, &request, true, true);
                  ASSERT_GT(*wireLen, 0);
                  *wire = taosMemoryMalloc(*wireLen);
                  ASSERT_NE(nullptr, *wire);
                  ASSERT_EQ(*wireLen, tSerializeSResFetchReq(*wire, *wireLen, &request, true, true));
                } else {
                  ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, wire, wireLen));
                }
              };
              void*   warmupWire = nullptr;
              int32_t warmupLen = 0;
              build(&warmupWire, &warmupLen);
              if (strcmp(path, "legacy") == 0) legacyBytes = warmupLen;
              SResFetchReq warmupDecoded = {};
              ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(warmupWire, warmupLen, &warmupDecoded));
              tDestroySResFetchReq(&warmupDecoded);
              taosMemoryFree(warmupWire);
              for (int32_t sample = 0; sample < 3; ++sample) {
                void*         wire = nullptr;
                int32_t       wireLen = 0;
                const int64_t buildNs = reader_fetch_benchmark::elapsedNs([&] { build(&wire, &wireLen); });
                SResFetchReq  decoded = {};
                const int64_t decodeNs = reader_fetch_benchmark::elapsedNs(
                    [&] { ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &decoded)); });
                ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
                const int32_t selectedGroups = tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos);
                const int32_t selectedParams = reader_fetch_benchmark::paramCount(decoded.pStRtFuncInfo);
                if (strcmp(path, "pruned") == 0 && selectedGroups < tSimpleHashGetSize(runtime.pGroupCalcInfos)) {
                  EXPECT_LT(selectedParams, totalParams);
                  EXPECT_LT(wireLen, legacyBytes);
                }
                reader_fetch_benchmark::emit("first", path, totalParams, nodeCount, layout, ancestorDepth, nodeId,
                                             sample, selectedGroups, selectedParams, wireLen, buildNs, decodeNs, 0);
                tDestroySResFetchReq(&decoded);
                taosMemoryFree(wire);
              }
            }

            request.reset = false;
            void*   warmupWire = nullptr;
            int32_t warmupLen = 0;
            ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &warmupWire, &warmupLen));
            SResFetchReq warmupDecoded = {};
            ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(warmupWire, warmupLen, &warmupDecoded));
            tDestroySResFetchReq(&warmupDecoded);
            taosMemoryFree(warmupWire);
            if (thinBytes == 0) thinBytes = warmupLen;
            EXPECT_EQ(thinBytes, warmupLen);
            for (int32_t sample = 0; sample < 3; ++sample) {
              void*         wire = nullptr;
              int32_t       wireLen = 0;
              const int64_t buildNs = reader_fetch_benchmark::elapsedNs(
                  [&] { ASSERT_EQ(TSDB_CODE_SUCCESS, tBuildStreamReaderFetchReq(&request, &wire, &wireLen)); });
              SResFetchReq  decoded = {};
              const int64_t decodeNs = reader_fetch_benchmark::elapsedNs(
                  [&] { ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(wire, wireLen, &decoded)); });
              ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
              EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupCalcInfos);
              EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupReadInfos);
              EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
              EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
              EXPECT_EQ(nullptr, decoded.pContextFrames);
              reader_fetch_benchmark::emit("continuation", "pruned", totalParams, nodeCount, layout, ancestorDepth,
                                           nodeId, sample, 0, 0, wireLen, buildNs, decodeNs, 0);
              tDestroySResFetchReq(&decoded);
              taosMemoryFree(wire);
            }
          }
          tDestroyStRtFuncInfo(&runtime);
        }
      }
    }
  }
}

TEST(StreamFetchGroupProjectionTest, ActualNodeProjectionScalesLinearly) {
  const auto measureCpuTicks = [](int32_t paramCount) {
    SStreamRuntimeFuncInfo  runtime = makeDenseActualNodeMultiGroupFetchRuntime(paramCount);
    SStreamContextPolicy*   policy = nullptr;
    SStreamAncestorContext* context = nullptr;
    const std::clock_t      start = std::clock();
    EXPECT_EQ(TSDB_CODE_SUCCESS, tProjectStreamCalcContextForFetch(&runtime, true, true, &policy, &context));
    const double elapsed = static_cast<double>(std::clock() - start);
    tDestroyStreamContextPolicy(&policy);
    tDestroyStreamAncestorContext(&context);
    tDestroyStRtFuncInfo(&runtime);
    return elapsed;
  };

  expectLinearProjectionScaling(measureCpuTicks);
}

TEST(StreamFetchGroupProjectionTest, GroupParamLookupScalesLinearly) {
  const auto measureCpuTicks = [](int32_t paramCount) {
    SStreamRuntimeFuncInfo  runtime = makeSingleGroupFetchRuntime(paramCount);
    SStreamContextPolicy*   policy = nullptr;
    SStreamAncestorContext* context = nullptr;
    const std::clock_t      start = std::clock();
    EXPECT_EQ(TSDB_CODE_SUCCESS, tProjectStreamCalcContextForGroup(runtime.pContextPolicy, runtime.pAncestorContext,
                                                                   runtime.groupId, &policy, &context));
    const double elapsed = static_cast<double>(std::clock() - start);
    tDestroyStreamContextPolicy(&policy);
    tDestroyStreamAncestorContext(&context);
    tDestroyStRtFuncInfo(&runtime);
    return elapsed;
  };

  expectLinearProjectionScaling(measureCpuTicks);
}

TEST(StreamFetchGroupProjectionTest, AllGroupsBuildsIndependentAdmittedViews) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  SArray*                views = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tProjectStreamCalcContextForAllGroups(runtime.pContextPolicy, runtime.pAncestorContext, &views));
  ASSERT_NE(nullptr, views);
  ASSERT_EQ(2, taosArrayGetSize(views));

  for (int32_t i = 0; i < taosArrayGetSize(views); ++i) {
    const auto* view = static_cast<const SStreamCalcContextGroupView*>(taosArrayGet(views, i));
    ASSERT_NE(nullptr, view);
    EXPECT_EQ(i == 0 ? 101 : 202, view->gid);
    ASSERT_NE(nullptr, view->pContextPolicy);
    EXPECT_NE(runtime.pContextPolicy, view->pContextPolicy);
    for (int32_t entryIndex = 0; entryIndex < taosArrayGetSize(view->pContextPolicy->pEntries); ++entryIndex) {
      const auto* entry =
          static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(view->pContextPolicy->pEntries, entryIndex));
      ASSERT_NE(nullptr, entry);
      EXPECT_EQ(view->gid, entry->gid);
    }
    EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(view->pContextPolicy, view->pAncestorContext, true));
  }

  tDestroyStreamCalcContextGroupViews(&views);
  EXPECT_EQ(nullptr, views);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(runtime.pContextPolicy, runtime.pAncestorContext, true));
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamFetchGroupProjectionTest, AllGroupsProjectionScalesLinearly) {
  const auto measureCpuTicks = [](int32_t groupCount) {
    SStreamContextPolicy policy = {};
    policy.pEntries = taosArrayInit(groupCount, sizeof(SStreamContextPolicyEntry));
    EXPECT_NE(nullptr, policy.pEntries);
    for (int32_t i = 0; i < groupCount; ++i) {
      SStreamContextPolicyEntry entry = {};
      entry.gid = i + 1;
      entry.paramIndex = 0;
      entry.contextPolicy = STREAM_CONTEXT_POLICY_NONE;
      EXPECT_NE(nullptr, taosArrayPush(policy.pEntries, &entry));
    }

    SArray*            views = nullptr;
    const std::clock_t start = std::clock();
    EXPECT_EQ(TSDB_CODE_SUCCESS, tProjectStreamCalcContextForAllGroups(&policy, nullptr, &views));
    const double elapsed = static_cast<double>(std::clock() - start);
    tDestroyStreamCalcContextGroupViews(&views);
    taosArrayDestroy(policy.pEntries);
    return elapsed;
  };

  expectLinearProjectionScaling(measureCpuTicks);
}

TEST(StreamFetchGroupProjectionTest, FetchRejectsMissingCalcForAnyNodeRead) {
  SStreamRuntimeFuncInfo runtime = makeActualNodeMultiGroupFetchRuntime();
  installActualNodeReadView(&runtime, 7);
  const int64_t missingGid = 202;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tSimpleHashRemove(runtime.pGroupCalcInfos, &missingGid, sizeof(missingGid)));
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.reset = true;
  source.pStRtFuncInfo = &runtime;
  EXPECT_LT(tSerializeSResFetchReq(nullptr, 0, &source, true, true), 0);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamFetchGroupProjectionTest, FromRunnerIgnoresResidualNodeViewAndKeepsFullBatch) {
  SStreamRuntimeFuncInfo runtime = makeFullMultiGroupFetchRuntime();
  const int32_t          nodeId = 7;
  auto** nodeReads = static_cast<SArray**>(tSimpleHashGet(runtime.pGroupReadInfos, &nodeId, sizeof(nodeId)));
  ASSERT_NE(nullptr, nodeReads);
  runtime.curGrpRead = taosArrayDup(*nodeReads, nullptr);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  SResFetchReq source = {};
  source.header.vgId = nodeId;
  source.reset = true;
  source.pStRtFuncInfo = &runtime;
  const int32_t size = tSerializeSResFetchReq(nullptr, 0, &source, true, false);
  EXPECT_GT(size, 0);
  if (size > 0) {
    std::vector<uint8_t> bytes(size);
    ASSERT_EQ(size, tSerializeSResFetchReq(bytes.data(), bytes.size(), &source, true, false));

    SResFetchReq decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &decoded));
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->curGrpRead);
    EXPECT_EQ(2, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupCalcInfos));
    EXPECT_EQ(1, tSimpleHashGetSize(decoded.pStRtFuncInfo->pGroupReadInfos));
    tDestroySResFetchReq(&decoded);
  }
  taosArrayDestroy(runtime.curGrpRead);
  runtime.curGrpRead = nullptr;
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, FullMultiGroupFetchAcceptsReorderedContextArrays) {
  SStreamRuntimeFuncInfo runtime = makeFullMultiGroupFetchRuntime();
  auto*                  firstParam =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 0));
  auto* secondParam =
      static_cast<SStreamAncestorParamContext*>(taosArrayGet(runtime.pAncestorContext->pParamContexts, 1));
  ASSERT_NE(nullptr, firstParam);
  ASSERT_NE(nullptr, secondParam);
  TSWAP(*firstParam, *secondParam);

  auto* firstBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.pAncestorContext->pReadScopeBindings, 0));
  auto* secondBinding =
      static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.pAncestorContext->pReadScopeBindings, 1));
  ASSERT_NE(nullptr, firstBinding);
  ASSERT_NE(nullptr, secondBinding);
  TSWAP(*firstBinding, *secondBinding);

  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  const auto   bytes = serializeFetchRequest(source, true, false);
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
  ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            tAdmitStreamContext(decoded.pStRtFuncInfo->pContextPolicy, decoded.pStRtFuncInfo->pAncestorContext, true));
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, MultiGroupCalcRejectsMissingReadBindingSet) {
  for (const int32_t remaining : {1, 0}) {
    SCOPED_TRACE(remaining);
    SSTriggerCalcRequest    source = makeMultiGroupCalcRequest();
    const auto              validBytes = serializeCalcRequest(source);
    SStreamAncestorContext* missingBindings = cloneWithoutLastReadBindings(source.pAncestorContext, remaining);
    ASSERT_NE(nullptr, missingBindings);
    tDestroyStreamAncestorContext(&source.pAncestorContext);
    source.pAncestorContext = missingBindings;

    auto malicious = validBytes;
    replaceAncestorFrame(&malicious, source.pAncestorContext, false);
    if (remaining == 0) {
      EXPECT_GT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
      SSTriggerCalcRequest decoded = {};
      ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(malicious.data(), malicious.size(), &decoded));
      EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequestAncestorContext(&decoded, true));
      tDestroySTriggerCalcRequest(&decoded);
    } else {
      EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
      expectCalcRequestDecodeFailure(malicious);
    }
    tDestroySTriggerCalcRequest(&source);
  }
}

TEST(StreamAncestorContextTest, FullMultiGroupFetchRejectsMissingReadBindingSet) {
  for (const int32_t remaining : {1, 0}) {
    SCOPED_TRACE(remaining);
    SStreamRuntimeFuncInfo runtime = makeFullMultiGroupFetchRuntime();
    SResFetchReq           source = {};
    source.pStRtFuncInfo = &runtime;
    source.reset = true;
    const auto              validBytes = serializeFetchRequest(source, true, false);
    SStreamAncestorContext* missingBindings = cloneWithoutLastReadBindings(runtime.pAncestorContext, remaining);
    ASSERT_NE(nullptr, missingBindings);
    tDestroyStreamAncestorContext(&runtime.pAncestorContext);
    runtime.pAncestorContext = missingBindings;

    EXPECT_LT(tSerializeSResFetchReq(nullptr, 0, &source, true, false), 0);
    void*   message = reinterpret_cast<void*>(1);
    int32_t length = 1;
    EXPECT_NE(TSDB_CODE_SUCCESS, tBuildStreamRunnerFetchReq(&source, nullptr, &message, &length));
    EXPECT_EQ(nullptr, message);
    EXPECT_EQ(0, length);

    auto malicious = validBytes;
    replaceAncestorFrame(&malicious, runtime.pAncestorContext, true);
    expectFetchDecodeFailure(malicious);
    tDestroyStRtFuncInfo(&runtime);
  }
}

TEST(StreamAncestorContextTest, FirstResetMultiGroupFetchValidatesCurGroupReadCarrier) {
  SStreamRuntimeFuncInfo runtime = makeFullMultiGroupFetchRuntime();
  runtime.curNodeId = 7;
  runtime.curGrpRead = taosArrayInit_s(sizeof(SSTriggerGroupReadInfo), 2);
  ASSERT_NE(nullptr, runtime.curGrpRead);
  static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(runtime.curGrpRead, 0))->gid = 101;
  static_cast<SSTriggerGroupReadInfo*>(taosArrayGet(runtime.curGrpRead, 1))->gid = 202;

  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = true;
  const auto bytes = serializeFetchRequest(source, true, true);

  SResFetchReq  decoded = {};
  const int32_t code = tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  if (code == TSDB_CODE_SUCCESS) {
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
    EXPECT_NE(nullptr, decoded.pStRtFuncInfo->curGrpRead);
    EXPECT_NE(nullptr, decoded.pStRtFuncInfo->pGroupCalcInfos);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupReadInfos);
  }
  if (decoded.pStRtFuncInfo != nullptr) {
    taosArrayDestroyEx(decoded.pStRtFuncInfo->curGrpRead, tDestroySSTriggerGroupReadInfo);
    decoded.pStRtFuncInfo->curGrpRead = nullptr;
  }
  tDestroySResFetchReq(&decoded);
  taosArrayDestroyEx(runtime.curGrpRead, tDestroySSTriggerGroupReadInfo);
  runtime.curGrpRead = nullptr;
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, MultiGroupCacheFetchRoundTripsExplicitCurrentProjection) {
  SStreamRuntimeFuncInfo runtime = makeFullMultiGroupFetchRuntime();
  runtime.groupId = 202;
  runtime.curIdx = 0;
  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  const auto bytes = serializeFetchRequest(source, false, false);
  ASSERT_FALSE(bytes.empty());

  SResFetchReq  decoded = {};
  const int32_t code = tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  if (code == TSDB_CODE_SUCCESS) {
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupCalcInfos);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->pGroupReadInfos);
    EXPECT_EQ(nullptr, decoded.pStRtFuncInfo->curGrpRead);
    EXPECT_EQ(202, decoded.pStRtFuncInfo->groupId);
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pContextPolicy);
    ASSERT_EQ(1, taosArrayGetSize(decoded.pStRtFuncInfo->pContextPolicy->pEntries));
    const auto* entry =
        static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(decoded.pStRtFuncInfo->pContextPolicy->pEntries, 0));
    ASSERT_NE(nullptr, entry);
    EXPECT_EQ(202, entry->gid);
    EXPECT_EQ(decoded.pStRtFuncInfo->curIdx, entry->paramIndex);
    ASSERT_NE(nullptr, decoded.pStRtFuncInfo->pAncestorContext);
    ASSERT_EQ(1, taosArrayGetSize(decoded.pStRtFuncInfo->pAncestorContext->pReadScopeBindings));
    const auto* binding = static_cast<const SStreamReadScopeBinding*>(
        taosArrayGet(decoded.pStRtFuncInfo->pAncestorContext->pReadScopeBindings, 0));
    ASSERT_NE(nullptr, binding);
    EXPECT_EQ(202, binding->scope.gid);
  }
  tDestroySResFetchReq(&decoded);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, SingleGroupFetchRejectsUnprojectedReadBinding) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime();
  SResFetchReq           source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = true;
  auto malicious = serializeFetchRequest(source, true, false);

  SStreamAncestorContext* contextWithBinding = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamAncestorContext(runtime.pAncestorContext, &contextWithBinding));
  ASSERT_NE(nullptr, contextWithBinding);
  appendReadBindingForParam(contextWithBinding, 7, 0, 0);
  replaceAncestorFrame(&malicious, contextWithBinding, true);
  expectFetchDecodeFailure(malicious);

  tDestroyStreamAncestorContext(&contextWithBinding);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, FullMultiGroupFetchRejectsBindingOutsideCarrierReadInfos) {
  SSTriggerCalcRequest   request = makeMultiGroupCalcRequest();
  SStreamRuntimeFuncInfo runtime = {};
  runtime.isMultiGroupCalc = true;
  runtime.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  TSWAP(runtime.pGroupCalcInfos, request.pGroupCalcInfos);
  TSWAP(runtime.pGroupReadInfos, request.pGroupReadInfos);
  TSWAP(runtime.pContextPolicy, request.pContextPolicy);
  TSWAP(runtime.pAncestorContext, request.pAncestorContext);
  tDestroySTriggerCalcRequest(&request);

  SResFetchReq source = {};
  source.pStRtFuncInfo = &runtime;
  const auto validBytes = serializeFetchRequest(source, true, false);

  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(runtime.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, binding);
  binding->readInfoIndex = 2;

  EXPECT_LT(tSerializeSResFetchReq(nullptr, 0, &source, true, false), 0);

  auto         malformed = validBytes;
  const size_t contextOffset = ancestorFrameOffset(malformed);
  ASSERT_LT(contextOffset, malformed.size());
  std::array<uint8_t, sizeof(int32_t) * 2 + sizeof(int64_t)> bindingPrefix = {};
  const int32_t                                              vgId = 7;
  const int32_t                                              readInfoIndex = 0;
  const int64_t                                              gid = 101;
  memcpy(bindingPrefix.data(), &vgId, sizeof(vgId));
  memcpy(bindingPrefix.data() + sizeof(vgId), &readInfoIndex, sizeof(readInfoIndex));
  memcpy(bindingPrefix.data() + sizeof(vgId) + sizeof(readInfoIndex), &gid, sizeof(gid));
  const auto match =
      std::search(malformed.begin() + contextOffset, malformed.end(), bindingPrefix.begin(), bindingPrefix.end());
  ASSERT_NE(malformed.end(), match);
  writeI32(&malformed, std::distance(malformed.begin(), match) + sizeof(vgId), 2);
  expectFetchDecodeFailure(malformed);
  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, SingleLayerFetchGoldenUnchanged) {
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.queryId = 11;
  source.taskId = 22;
  source.reset = true;
  source.dynTbname = true;
  source.forceFetchCompleted = true;
  const auto            bytes = serializeFetchRequest(source, false, false);
  static constexpr char golden[] =
      "\x00\x00\x00\x4b\x00\x00\x00\x07\x3f\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01";
  EXPECT_EQ(std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(golden),
                                 reinterpret_cast<const uint8_t*>(golden) + sizeof(golden) - 1),
            bytes);
}

TEST(StreamFetchResetModeTest, AbsentFrameKeepsLegacyBytesAndDefaultsFresh) {
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.queryId = 11;
  source.taskId = 22;
  source.reset = true;
  source.dynTbname = true;
  source.forceFetchCompleted = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  EXPECT_EQ(bytes.size(), frameOffset(bytes, STREAM_FETCH_RESET_MODE_FRAME_MAGIC));

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_FALSE(decoded.reuseReader);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamFetchResetModeTest, RoundTripsReuseReaderTailFrame) {
  SResFetchReq source = {};
  source.reset = true;
  source.reuseReader = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  EXPECT_LT(frameOffset(bytes, STREAM_FETCH_RESET_MODE_FRAME_MAGIC), bytes.size());

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_TRUE(decoded.reset);
  EXPECT_TRUE(decoded.reuseReader);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamFetchResetModeTest, RejectsDuplicateVersionFlagsFalsePayloadAndReuseWithoutReset) {
  SResFetchReq source = {};
  source.reset = true;
  source.reuseReader = true;
  const auto   original = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(original, STREAM_FETCH_RESET_MODE_FRAME_MAGIC);
  ASSERT_LT(offset, original.size());

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&duplicate);
  expectFetchDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, offset + sizeof(uint32_t), STREAM_FETCH_RESET_MODE_FRAME_VERSION + 1);
  expectFetchDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, offset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectFetchDecodeFailure(badFlags);

  auto falsePayload = original;
  falsePayload.back() = 0;
  expectFetchDecodeFailure(falsePayload);

  source.reset = false;
  EXPECT_LT(tSerializeSResFetchReq(nullptr, 0, &source, false, false), 0);

  SResFetchReq noResetSource = {};
  auto         noReset = serializeFetchRequest(noResetSource, false, false);
  noReset.insert(noReset.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&noReset);
  expectFetchDecodeFailure(noReset);

  auto truncated = original;
  truncated.pop_back();
  rewriteFetchRequestLengths(&truncated);
  expectFetchDecodeFailure(truncated);

  auto unknown = original;
  writeU32(&unknown, offset, UINT32_C(0x4e575858));
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(unknown.data(), unknown.size(), &decoded));
  EXPECT_FALSE(decoded.reuseReader);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamTwoPassRescanTest, AbsentFlagKeepsLegacyBytesAndDefaultsFresh) {
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.queryId = 11;
  source.taskId = 22;
  source.reset = true;
  source.dynTbname = true;
  source.forceFetchCompleted = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  EXPECT_EQ(frameOffset(bytes, STREAM_TWO_PASS_RESCAN_FRAME_MAGIC), bytes.size());

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_FALSE(decoded.phase2Rescan);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamTwoPassRescanTest, RoundTripsTwoPassRescanTailFrame) {
  SResFetchReq source = {};
  source.reset = true;
  source.phase2Rescan = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(bytes, STREAM_TWO_PASS_RESCAN_FRAME_MAGIC);
  ASSERT_LT(offset, bytes.size());

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_TRUE(decoded.reset);
  EXPECT_TRUE(decoded.phase2Rescan);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamTwoPassRescanTest, RejectsDuplicateVersionFlagsFalsePayload) {
  SResFetchReq source = {};
  source.reset = true;
  source.phase2Rescan = true;
  const auto   original = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(original, STREAM_TWO_PASS_RESCAN_FRAME_MAGIC);
  ASSERT_LT(offset, original.size());

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&duplicate);
  expectFetchDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, offset + sizeof(uint32_t), STREAM_TWO_PASS_RESCAN_FRAME_VERSION + 1);
  expectFetchDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, offset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectFetchDecodeFailure(badFlags);

  auto falsePayload = original;
  falsePayload.back() = 0;
  expectFetchDecodeFailure(falsePayload);

  auto truncated = original;
  truncated.pop_back();
  rewriteFetchRequestLengths(&truncated);
  expectFetchDecodeFailure(truncated);

  // An old decoder that does not know the magic skips the frame and keeps
  // serving the classic fetch semantics.
  auto unknown = original;
  writeU32(&unknown, offset, UINT32_C(0x4e575858));
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(unknown.data(), unknown.size(), &decoded));
  EXPECT_FALSE(decoded.phase2Rescan);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamReaderGroupModeTest, AbsentFrameKeepsLegacyBytesAndDefaultsFresh) {
  SResFetchReq source = {};
  source.header.vgId = 7;
  source.queryId = 11;
  source.taskId = 22;
  source.reset = true;
  source.dynTbname = true;
  source.forceFetchCompleted = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  EXPECT_EQ(bytes.size(), frameOffset(bytes, STREAM_READER_GROUP_MODE_FRAME_MAGIC));

  SResFetchReq decoded = {};
  decoded.readerGroupMode = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_FALSE(decoded.readerGroupMode);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamReaderGroupModeTest, RoundTripsEnabledTailFrame) {
  SResFetchReq source = {};
  source.readerGroupMode = true;

  const auto bytes = serializeFetchRequest(source, false, false);
  EXPECT_LT(frameOffset(bytes, STREAM_READER_GROUP_MODE_FRAME_MAGIC), bytes.size());

  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(const_cast<uint8_t*>(bytes.data()), bytes.size(), &decoded));
  EXPECT_TRUE(decoded.readerGroupMode);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamReaderGroupModeTest, RejectsDuplicateVersionFlagsAndInvalidBoolean) {
  SResFetchReq source = {};
  source.readerGroupMode = true;
  const auto   original = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(original, STREAM_READER_GROUP_MODE_FRAME_MAGIC);
  ASSERT_LT(offset, original.size());

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&duplicate);
  expectFetchDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, offset + sizeof(uint32_t), STREAM_READER_GROUP_MODE_FRAME_VERSION + 1);
  expectFetchDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, offset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectFetchDecodeFailure(badFlags);

  auto falsePayload = original;
  falsePayload.at(falsePayload.size() - 1) = 0;
  expectFetchDecodeFailure(falsePayload);

  auto nonBooleanPayload = original;
  nonBooleanPayload.at(nonBooleanPayload.size() - 1) = 2;
  expectFetchDecodeFailure(nonBooleanPayload);

  auto truncated = original;
  truncated.pop_back();
  rewriteFetchRequestLengths(&truncated);
  expectFetchDecodeFailure(truncated);

  auto unknown = original;
  writeU32(&unknown, offset, UINT32_C(0x4e575858));
  SResFetchReq decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(unknown.data(), unknown.size(), &decoded));
  EXPECT_FALSE(decoded.readerGroupMode);
  tDestroySResFetchReq(&decoded);
}

TEST(StreamRunnerEogBatchModeTest, RejectsInvalidCapabilityBoolean) {
  SResFetchReq source = {};
  source.readerGroupMode = true;
  auto         bytes = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(bytes, STREAM_READER_GROUP_MODE_FRAME_MAGIC);
  ASSERT_LT(offset, bytes.size());
  writeU32(&bytes, offset, UINT32_C(0x4e575242));
  bytes.back() = 2;
  expectFetchDecodeFailure(bytes);
}

TEST(StreamRunnerEogBatchModeTest, AbsentCapabilityKeepsLegacyBytesAndClearsReusedRequest) {
  SResFetchReq source = {};
  auto         bytes = serializeFetchRequest(source, false, false);
  EXPECT_EQ(bytes.size(), frameOffset(bytes, STREAM_RUNNER_EOG_BATCH_FRAME_MAGIC));
  FetchRequestGuard decoded;
  decoded.request.runnerEogBatch = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &decoded.request));
  EXPECT_FALSE(decoded.request.runnerEogBatch);
}

TEST(StreamRunnerEogBatchModeTest, RoundTripsCapabilityOnInitialAndContinuationRequests) {
  for (bool reset : {false, true}) {
    SResFetchReq source = {};
    source.reset = reset;
    source.runnerEogBatch = true;
    source.readerGroupMode = true;
    auto bytes = serializeFetchRequest(source, false, false);
    EXPECT_LT(frameOffset(bytes, STREAM_RUNNER_EOG_BATCH_FRAME_MAGIC), bytes.size());
    FetchRequestGuard decoded;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(bytes.data(), bytes.size(), &decoded.request));
    EXPECT_TRUE(decoded.request.runnerEogBatch);
    EXPECT_TRUE(decoded.request.readerGroupMode);
    EXPECT_EQ(reset, decoded.request.reset);
  }
}

TEST(StreamRunnerEogBatchModeTest, RejectsMalformedKnownFramesAndSkipsUnknownFrames) {
  SResFetchReq source = {};
  source.runnerEogBatch = true;
  const auto   original = serializeFetchRequest(source, false, false);
  const size_t offset = frameOffset(original, STREAM_RUNNER_EOG_BATCH_FRAME_MAGIC);
  ASSERT_LT(offset, original.size());

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + offset, original.end());
  rewriteFetchRequestLengths(&duplicate);
  expectFetchDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, offset + sizeof(uint32_t), STREAM_RUNNER_EOG_BATCH_FRAME_VERSION + 1);
  expectFetchDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, offset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectFetchDecodeFailure(badFlags);

  auto disabled = original;
  disabled.back() = 0;
  expectFetchDecodeFailure(disabled);

  auto truncated = original;
  truncated.pop_back();
  rewriteFetchRequestLengths(&truncated);
  expectFetchDecodeFailure(truncated);

  auto extraPayload = original;
  extraPayload.push_back(1);
  writeU32(&extraPayload, offset + sizeof(uint32_t) + 2 * sizeof(uint16_t), 2);
  rewriteFetchRequestLengths(&extraPayload);
  expectFetchDecodeFailure(extraPayload);

  auto unknown = original;
  writeU32(&unknown, offset, UINT32_C(0x4e575858));
  FetchRequestGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSResFetchReq(unknown.data(), unknown.size(), &decoded.request));
  EXPECT_FALSE(decoded.request.runnerEogBatch);
}

TEST(StreamAncestorContextTest, FetchRejectsFrameWithoutRuntimeInfo) {
  SStreamRuntimeFuncInfo runtime = makeSingleGroupFetchRuntime();
  SResFetchReq           source = {};
  source.pStRtFuncInfo = &runtime;
  source.reset = true;
  auto         bytes = serializeFetchRequest(source, true, false);
  const size_t tailOffset = policyFrameOffset(bytes);
  ASSERT_LT(tailOffset, bytes.size());

  SResFetchReq legacySource = source;
  legacySource.pStRtFuncInfo = nullptr;
  auto missingRuntime = serializeFetchRequest(legacySource, true, false);
  missingRuntime.insert(missingRuntime.end(), bytes.begin() + tailOffset, bytes.end());
  writeI32(&missingRuntime, sizeof(SMsgHead),
           static_cast<int32_t>(missingRuntime.size() - sizeof(SMsgHead) - sizeof(int32_t)));
  reinterpret_cast<SMsgHead*>(missingRuntime.data())->contLen = htonl(missingRuntime.size());
  expectFetchDecodeFailure(missingRuntime);

  tDestroyStRtFuncInfo(&runtime);
}

TEST(StreamAncestorContextTest, ordinaryIntervalWindowSnapshotRoundTripsFourValues) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto* snapshot = firstAncestorSnapshot(source.pAncestorContext);
  ASSERT_NE(nullptr, snapshot);
  snapshot->placeholderMask = PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  snapshot->values.window.start = 100;
  snapshot->values.window.end = 199;
  snapshot->values.window.duration = 100;
  snapshot->values.window.rownum = 9;

  auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pAncestorContext);
  const auto* decodedSnapshot = firstAncestorSnapshot(decoded.pAncestorContext);
  ASSERT_NE(nullptr, decodedSnapshot);
  EXPECT_EQ(100, decodedSnapshot->values.window.start);
  EXPECT_EQ(199, decodedSnapshot->values.window.end);
  EXPECT_EQ(100, decodedSnapshot->values.window.duration);
  EXPECT_EQ(9, decodedSnapshot->values.window.rownum);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, pureSlidingSnapshotRoundTripsThreeValues) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pAncestorContext);
  const auto* decodedSnapshot = firstAncestorSnapshot(decoded.pAncestorContext);
  ASSERT_NE(nullptr, decodedSnapshot);
  EXPECT_EQ(-1, decodedSnapshot->values.sliding.prevTs);
  EXPECT_EQ(0, decodedSnapshot->values.sliding.currentTs);
  EXPECT_EQ(100, decodedSnapshot->values.sliding.nextTs);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, zeroPlaceholderMaskUsesCanonicalWindowValues) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto* snapshot = firstAncestorSnapshot(source.pAncestorContext);
  ASSERT_NE(nullptr, snapshot);
  snapshot->placeholderMask = 0;
  snapshot->values.window.start = 100;
  snapshot->values.window.end = 199;
  snapshot->values.window.duration = 100;
  snapshot->values.window.rownum = 9;

  auto bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pAncestorContext);
  const auto* decodedSnapshot = firstAncestorSnapshot(decoded.pAncestorContext);
  ASSERT_NE(nullptr, decodedSnapshot);
  EXPECT_EQ(9, decodedSnapshot->values.window.rownum);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, rejectsMixedPlaceholderMaskFamilies) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto* snapshot = firstAncestorSnapshot(source.pAncestorContext);
  ASSERT_NE(nullptr, snapshot);
  snapshot->placeholderMask = PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_WSTART;
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, v2WireUsesLiteralGoldenFrameAndRejectsV1Payload) {
  SStreamAncestorContext* source = makeAncestorContext(101);
  ASSERT_NE(nullptr, source);
  const auto actual = encodeAncestorFrame(source);
  tDestroyStreamAncestorContext(&source);

  static constexpr char golden[] =
      "\x54\x43\x57\x4e\x01\x00\x00\x00\xad\x00\x00\x00"
      "\x02\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"
      "\x65\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00"
      "\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"
      "\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00"
      "\x00\x02\xf4\x01\x00\x00\x00\x00\x00\x00\x0b\x00"
      "\x00\x00\x00\x00\x00\x00\x05\xe8\x03\x00\x00\x00"
      "\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x02"
      "\x00\x00\x00\x00\x00\x00\x00\x01\x07\x00\x00\x00"
      "\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff"
      "\x00\x00\x00\x00\x00\x00\x00\x00\x64\x00\x00\x00"
      "\x00\x00\x00\x00\x01\x00\x00\x00\x02\x58\x00\x00"
      "\x00\x00\x00\x00\x00\xf4\x01\x00\x00\x00\x00\x00"
      "\x00\xe7\x03\x00\x00\x00\x00\x00\x00\xf4\x01\x00"
      "\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00";
  const std::vector<uint8_t> expected(reinterpret_cast<const uint8_t*>(golden),
                                      reinterpret_cast<const uint8_t*>(golden) + sizeof(golden) - 1);
  EXPECT_EQ(185U, expected.size());
  EXPECT_EQ(expected, actual);

  SDecoder decoder = {};
  tDecoderInit(&decoder, const_cast<uint8_t*>(actual.data()), actual.size());
  SStreamTailFrameDecoder frame = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeNextStreamTailFrame(&decoder, &frame));
  ASSERT_EQ(STREAM_ANCESTOR_FRAME_MAGIC, frame.magic);
  ASSERT_EQ(STREAM_ANCESTOR_FRAME_VERSION, frame.version);
  ASSERT_EQ(173U, frame.payloadDecoder.size);
  SStreamAncestorContext* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeStreamAncestorContext(&frame.payloadDecoder, &decoded));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tFinishDecodeStreamTailFrame(&frame, true));
  ASSERT_NE(nullptr, decoded);
  const auto* param = static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded->pParamContexts, 0));
  ASSERT_NE(nullptr, param);
  EXPECT_EQ(101, param->leafIdentity.gid);
  EXPECT_EQ(2, taosArrayGetSize(param->leafIdentity.lineage.pScopes));
  EXPECT_EQ(2, taosArrayGetSize(param->pSnapshots));
  tDestroyStreamAncestorContext(&decoded);
  tDecoderClear(&decoder);

  auto v1 = actual;
  writeI32(&v1, sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t), 1);
  tDecoderInit(&decoder, v1.data(), v1.size());
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeNextStreamTailFrame(&decoder, &frame));
  ASSERT_NE(TSDB_CODE_SUCCESS, tDecodeStreamAncestorContext(&frame.payloadDecoder, &decoded));
  EXPECT_EQ(nullptr, decoded);
  tFinishDecodeStreamTailFrame(&frame, false);
  tDecoderClear(&decoder);
}

TEST(StreamAncestorContextTest, rejectsPlaceholderBitsOutsideWindowFamilies) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto* snapshot = firstAncestorSnapshot(source.pAncestorContext);
  ASSERT_NE(nullptr, snapshot);
  snapshot->placeholderMask = PLACE_HOLDER_LOCALTIME;
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, rejectsSlidingPlaceholderFamilyForNonIntervalSnapshot) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(source.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(nullptr, param);
  auto* snapshot = static_cast<SWindowAncestorSnapshot*>(taosArrayGet(param->pSnapshots, 1));
  ASSERT_NE(nullptr, snapshot);
  ASSERT_EQ(WINDOW_TYPE_SESSION, snapshot->triggerType);
  snapshot->placeholderMask = PLACE_HOLDER_CURRENT_TS;
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, rejectsReadBindingWithoutMatchingReadInfo) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  source.pAncestorContext->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
  ASSERT_NE(nullptr, source.pAncestorContext->pReadScopeBindings);
  SStreamReadScopeBinding binding = makeReadScopeBinding(7, 0, source.gid);
  binding.scope.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  ASSERT_NE(nullptr, binding.scope.lineage.pScopes);
  const SScopeInstanceId scope = makeScopeInstanceId(0, WINDOW_TYPE_INTERVAL, 0, 10);
  ASSERT_NE(nullptr, taosArrayPush(binding.scope.lineage.pScopes, &scope));
  ASSERT_NE(nullptr, taosArrayPush(source.pAncestorContext->pReadScopeBindings, &binding));

  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, multiGroupRoundTripsByGidAndParamIndex) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  auto                 bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pAncestorContext);
  ASSERT_EQ(2, taosArrayGetSize(decoded.pAncestorContext->pParamContexts));
  bool found101 = false;
  bool found202 = false;
  for (int32_t i = 0; i < taosArrayGetSize(decoded.pAncestorContext->pParamContexts); ++i) {
    const auto* param =
        static_cast<const SStreamAncestorParamContext*>(taosArrayGet(decoded.pAncestorContext->pParamContexts, i));
    ASSERT_NE(nullptr, param);
    EXPECT_EQ(0, param->paramIndex);
    found101 = found101 || param->leafIdentity.gid == 101;
    found202 = found202 || param->leafIdentity.gid == 202;
  }
  EXPECT_TRUE(found101);
  EXPECT_TRUE(found202);
  EXPECT_EQ(2, taosArrayGetSize(decoded.pAncestorContext->pReadScopeBindings));
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, rejectsReadBindingOutsideMultiGroupReadInfos) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(source.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, binding);
  binding->readInfoIndex = 2;
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, rejectsReadBindingWithoutMatchingParamLineage) {
  SSTriggerCalcRequest source = makeMultiGroupCalcRequest();
  auto* binding = static_cast<SStreamReadScopeBinding*>(taosArrayGet(source.pAncestorContext->pReadScopeBindings, 0));
  ASSERT_NE(nullptr, binding);
  auto* scope = static_cast<SScopeInstanceId*>(taosArrayGet(binding->scope.lineage.pScopes, 0));
  ASSERT_NE(nullptr, scope);
  ++scope->openingTs;

  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &source), 0);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, cloneIsDeepAndProjectRekeysUniqueMapping) {
  SStreamAncestorContext* source = makeAncestorContext(101);
  ASSERT_NE(nullptr, source);
  SStreamReadScopeBinding binding = makeReadScopeBinding(7, 0, 101);
  const auto* sourceParam = static_cast<const SStreamAncestorParamContext*>(taosArrayGet(source->pParamContexts, 0));
  binding.scope.lineage.pScopes = taosArrayDup(sourceParam->leafIdentity.lineage.pScopes, nullptr);
  ASSERT_NE(nullptr, binding.scope.lineage.pScopes);
  source->pReadScopeBindings = taosArrayInit(1, sizeof(SStreamReadScopeBinding));
  ASSERT_NE(nullptr, source->pReadScopeBindings);
  ASSERT_NE(nullptr, taosArrayPush(source->pReadScopeBindings, &binding));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(source));

  SStreamAncestorContext* clone = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamAncestorContext(source, &clone));
  ASSERT_NE(nullptr, clone);
  ASSERT_NE(nullptr, clone->pParamIndex);
  EXPECT_NE(source->pParamIndex, clone->pParamIndex);
  auto* mutableSourceParam = static_cast<SStreamAncestorParamContext*>(taosArrayGet(source->pParamContexts, 0));
  auto* mutableSourceScope =
      static_cast<SScopeInstanceId*>(taosArrayGet(mutableSourceParam->leafIdentity.lineage.pScopes, 0));
  mutableSourceScope->openingTs = 999;
  const auto* clonedParam = static_cast<const SStreamAncestorParamContext*>(taosArrayGet(clone->pParamContexts, 0));
  const auto* clonedScope =
      static_cast<const SScopeInstanceId*>(taosArrayGet(clonedParam->leafIdentity.lineage.pScopes, 0));
  EXPECT_EQ(0, clonedScope->openingTs);
  tDestroyStreamAncestorContext(&source);

  const SStreamAncestorParamContext* clonedMatch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(clone, 101, 0, &clonedMatch));
  EXPECT_EQ(clonedParam, clonedMatch);

  SStreamAncestorContext* projected = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tProjectStreamAncestorContext(clone, 101, 0, 3, &projected));
  ASSERT_NE(nullptr, projected);
  const auto* projectedParam =
      static_cast<const SStreamAncestorParamContext*>(taosArrayGet(projected->pParamContexts, 0));
  EXPECT_EQ(3, projectedParam->paramIndex);
  EXPECT_EQ(1, taosArrayGetSize(projected->pReadScopeBindings));
  EXPECT_NE(projectedParam->leafIdentity.lineage.pScopes, clonedParam->leafIdentity.lineage.pScopes);
  const SStreamAncestorParamContext* projectedMatch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(projected, 101, 3, &projectedMatch));
  EXPECT_EQ(projectedParam, projectedMatch);
  EXPECT_NE(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(projected, 101, 0, &projectedMatch));
  EXPECT_EQ(nullptr, projectedMatch);

  SStreamAncestorContext* missing = reinterpret_cast<SStreamAncestorContext*>(UINTPTR_MAX);
  EXPECT_NE(TSDB_CODE_SUCCESS, tProjectStreamAncestorContext(clone, 202, 0, 0, &missing));
  EXPECT_EQ(nullptr, missing);
  SStreamAncestorContext* invalidDestination = reinterpret_cast<SStreamAncestorContext*>(UINTPTR_MAX);
  EXPECT_NE(TSDB_CODE_SUCCESS, tProjectStreamAncestorContext(clone, 101, 0, -1, &invalidDestination));
  EXPECT_EQ(nullptr, invalidDestination);
  SStreamAncestorContext* invalidDecode = reinterpret_cast<SStreamAncestorContext*>(UINTPTR_MAX);
  EXPECT_NE(TSDB_CODE_SUCCESS, tDecodeStreamAncestorContext(nullptr, &invalidDecode));
  EXPECT_EQ(nullptr, invalidDecode);
  tDestroyStreamAncestorContext(&projected);
  tDestroyStreamAncestorContext(&clone);
}

TEST(StreamAncestorContextTest, preparedIndexLooksUpSameLocalParamAcrossGroups) {
  SStreamAncestorContext* context = makeAncestorContext(101, 0);
  ASSERT_NE(nullptr, context);
  appendAncestorParamContext(context, 202, 0);
  auto* second = static_cast<SStreamAncestorParamContext*>(taosArrayGet(context->pParamContexts, 1));
  ASSERT_NE(nullptr, second);
  auto* secondSnapshot = static_cast<SWindowAncestorSnapshot*>(taosArrayGet(second->pSnapshots, 0));
  ASSERT_NE(nullptr, secondSnapshot);
  secondSnapshot->values.sliding.nextTs = 202;

  const SStreamAncestorParamContext* match = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(context));
  SSHashObj* const index = context->pParamIndex;
  ASSERT_NE(nullptr, index);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(context));
  EXPECT_EQ(index, context->pParamIndex);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(context, 202, 0, &match));
  ASSERT_NE(nullptr, match);
  EXPECT_EQ(202, match->leafIdentity.gid);
  EXPECT_EQ(0, match->paramIndex);
  const auto* snapshot = static_cast<const SWindowAncestorSnapshot*>(taosArrayGet(match->pSnapshots, 0));
  ASSERT_NE(nullptr, snapshot);
  EXPECT_EQ(202, snapshot->values.sliding.nextTs);

  tDestroyStreamAncestorContext(&context);
}

TEST(StreamAncestorContextTest, prepareRejectsInvalidParamIndexAndMissingSnapshots) {
  SStreamAncestorContext* invalidIndex = makeAncestorContext(101, 0);
  ASSERT_NE(nullptr, invalidIndex);
  auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(invalidIndex->pParamContexts, 0));
  ASSERT_NE(nullptr, param);
  param->paramIndex = -1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(invalidIndex));
  EXPECT_EQ(nullptr, invalidIndex->pParamIndex);
  tDestroyStreamAncestorContext(&invalidIndex);

  SStreamAncestorContext* missingSnapshots = makeAncestorContext(101, 0);
  ASSERT_NE(nullptr, missingSnapshots);
  param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(missingSnapshots->pParamContexts, 0));
  ASSERT_NE(nullptr, param);
  taosArrayDestroy(param->pSnapshots);
  param->pSnapshots = nullptr;
  EXPECT_NE(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(missingSnapshots));
  EXPECT_EQ(nullptr, missingSnapshots->pParamIndex);
  tDestroyStreamAncestorContext(&missingSnapshots);
}

TEST(StreamAncestorContextTest, unpreparedLookupRejectsDuplicatesAndPrepareClearsFailedIndex) {
  SStreamAncestorContext* context = makeAncestorContext(101, 0);
  ASSERT_NE(nullptr, context);

  const SStreamAncestorParamContext* match = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(context, 101, 0, &match));
  ASSERT_NE(nullptr, match);
  appendAncestorParamContext(context, 101, 0);
  EXPECT_NE(TSDB_CODE_SUCCESS, tLookupStreamAncestorParam(context, 101, 0, &match));
  EXPECT_EQ(nullptr, match);
  EXPECT_NE(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(context));
  EXPECT_EQ(nullptr, context->pParamIndex);

  tDestroyStreamAncestorContext(&context);
}

TEST(StreamAncestorContextTest, preparedIndexIsNotEncoded) {
  SStreamAncestorContext* context = makeAncestorContext(101, 0);
  ASSERT_NE(nullptr, context);
  const auto withoutIndex = encodeAncestorFrame(context);
  ASSERT_FALSE(withoutIndex.empty());
  ASSERT_EQ(TSDB_CODE_SUCCESS, tPrepareStreamAncestorParamIndex(context));
  ASSERT_NE(nullptr, context->pParamIndex);
  EXPECT_EQ(withoutIndex, encodeAncestorFrame(context));

  tDestroyStreamAncestorContext(&context);
}

TEST(StreamAncestorContextTest, rejectsInvalidMappingIdentityAndShape) {
  for (int32_t testCase = 0; testCase < 4; ++testCase) {
    SCOPED_TRACE(testCase);
    SSTriggerCalcRequest request = makeSingleGroupCalcRequest();
    setSingleAncestorPolicy(&request);
    request.pAncestorContext = makeAncestorContext(request.gid);
    auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(request.pAncestorContext->pParamContexts, 0));
    ASSERT_NE(nullptr, param);
    switch (testCase) {
      case 0:
        appendAncestorParamContext(request.pAncestorContext, request.gid, 0);
        break;
      case 1:
        param->paramIndex = 1;
        break;
      case 2:
        static_cast<SWindowAncestorSnapshot*>(taosArrayGet(param->pSnapshots, 1))->triggerType = WINDOW_TYPE_STATE;
        break;
      case 3:
        static_cast<SScopeInstanceId*>(taosArrayGet(param->leafIdentity.lineage.pScopes, 0))->layerIndex = 1;
        break;
    }
    EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
    tDestroySTriggerCalcRequest(&request);
  }
}

TEST(StreamAncestorContextTest, rejectsLineageDeeperThanSevenAncestors) {
  SSTriggerCalcRequest request = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&request);
  request.pAncestorContext = makeAncestorContext(request.gid);
  auto* param = static_cast<SStreamAncestorParamContext*>(taosArrayGet(request.pAncestorContext->pParamContexts, 0));
  for (int32_t i = 2; i < STREAM_WINDOW_MAX_LAYERS; ++i) {
    const SScopeInstanceId        scope = makeScopeInstanceId(i, WINDOW_TYPE_COUNT, i * 100, i);
    const SWindowAncestorSnapshot snapshot = makeWindowSnapshot(i, WINDOW_TYPE_COUNT, 0, i * 100);
    ASSERT_NE(nullptr, taosArrayPush(param->leafIdentity.lineage.pScopes, &scope));
    ASSERT_NE(nullptr, taosArrayPush(param->pSnapshots, &snapshot));
  }
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &request), 0);
  tDestroySTriggerCalcRequest(&request);
}

TEST(StreamAncestorContextTest, rejectsUnknownAndMalformedKnownFrames) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  const auto original = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  const size_t policyOffset = policyFrameOffset(original);
  const size_t frameOffset = ancestorFrameOffset(original);
  ASSERT_LT(policyOffset, frameOffset);
  ASSERT_LT(frameOffset, original.size());
  const auto unknown = makeUnknownCalcFrame();

  for (size_t insertAt : {policyOffset, frameOffset, original.size()}) {
    auto bytes = original;
    bytes.insert(bytes.begin() + insertAt, unknown.begin(), unknown.end());
    writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));
    expectCalcRequestDecodeFailure(bytes);
  }

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + frameOffset, original.end());
  writeI32(&duplicate, 0, static_cast<int32_t>(duplicate.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, frameOffset + sizeof(uint32_t), STREAM_ANCESTOR_FRAME_VERSION + 1);
  expectCalcRequestDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, frameOffset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectCalcRequestDecodeFailure(badFlags);

  auto badPayloadVersion = original;
  writeI32(&badPayloadVersion, frameOffset + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2,
           STREAM_ANCESTOR_CONTEXT_VERSION + 1);
  expectCalcRequestDecodeFailure(badPayloadVersion);

  auto truncated = original;
  truncated.pop_back();
  writeI32(&truncated, 0, static_cast<int32_t>(truncated.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(truncated);

  auto     trailingPayload = original;
  uint32_t payloadLength = 0;
  memcpy(&payloadLength, trailingPayload.data() + frameOffset + sizeof(uint32_t) + sizeof(uint16_t) * 2,
         sizeof(payloadLength));
  writeU32(&trailingPayload, frameOffset + sizeof(uint32_t) + sizeof(uint16_t) * 2, payloadLength + 1);
  trailingPayload.push_back(0);
  writeI32(&trailingPayload, 0, static_cast<int32_t>(trailingPayload.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(trailingPayload);
}

TEST(StreamAncestorContextTest, rejectsTruncatedLegacyBodyWithoutPublishingContext) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  auto                 bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  bytes.resize(sizeof(int32_t) + sizeof(int64_t));
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));

  SSTriggerCalcRequest decoded = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(nullptr, decoded.pAncestorContext);
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, ExplicitGroupPolicyDoesNotDependOnNotifyType) {
  for (int32_t notifyType : {0, BIT_FLAG_MASK(2), BIT_FLAG_MASK(3)}) {
    SSTriggerCalcRequest groupOnly = makeSingleGroupCalcRequest(notifyType);
    groupOnly.pContextPolicy =
        makeContextPolicy({makeContextPolicyEntry(groupOnly.gid, 0, STREAM_CONTEXT_POLICY_NONE)});
    auto                 bytes = serializeCalcRequest(groupOnly);
    tDestroySTriggerCalcRequest(&groupOnly);
    SSTriggerCalcRequest decoded = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
    ASSERT_NE(nullptr, decoded.pContextPolicy);
    ASSERT_EQ(1, taosArrayGetSize(decoded.pContextPolicy->pEntries));
    const auto* entry =
        static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(decoded.pContextPolicy->pEntries, 0));
    ASSERT_NE(nullptr, entry);
    EXPECT_EQ(STREAM_CONTEXT_POLICY_NONE, entry->contextPolicy);
    EXPECT_EQ(nullptr, decoded.pAncestorContext);
    EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(decoded.pContextPolicy, decoded.pAncestorContext, true));
    tDestroySTriggerCalcRequest(&decoded);
  }

  SSTriggerCalcRequest mixed = makeSingleGroupCalcRequest(0);
  SSTriggerCalcParam   window = makeWindowCalcParam(1000, 1099, 0);
  ASSERT_NE(nullptr, taosArrayPush(mixed.params, &window));
  mixed.pContextPolicy = makeContextPolicy({makeContextPolicyEntry(mixed.gid, 0, STREAM_CONTEXT_POLICY_NONE),
                                            makeContextPolicyEntry(mixed.gid, 1, STREAM_CONTEXT_POLICY_ANCESTOR)});
  mixed.pAncestorContext = makeAncestorContext(mixed.gid, 1);
  EXPECT_GT(tSerializeSTriggerCalcRequest(nullptr, 0, &mixed), 0);
  static_cast<SStreamAncestorParamContext*>(taosArrayGet(mixed.pAncestorContext->pParamContexts, 0))->paramIndex = 0;
  EXPECT_LT(tSerializeSTriggerCalcRequest(nullptr, 0, &mixed), 0);
  tDestroySTriggerCalcRequest(&mixed);
}

TEST(StreamAncestorContextTest, NestedBrandNewCreateTableEncodesEmptyPolicy) {
  SSTriggerCalcRequest source = {};
  source.streamId = 11;
  source.runnerTaskId = 22;
  source.sessionId = 33;
  source.triggerType = STREAM_TRIGGER_COUNT;
  source.gid = 101;
  source.createTable = 1;
  source.isWindowTrigger = true;
  source.params = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, source.params);
  source.pContextPolicy = makeContextPolicy();

  auto bytes = serializeCalcRequest(source);
  ASSERT_LT(policyFrameOffset(bytes), bytes.size());
  EXPECT_EQ(bytes.size(), ancestorFrameOffset(bytes));

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.pContextPolicy);
  EXPECT_EQ(0, taosArrayGetSize(decoded.pContextPolicy->pEntries));
  EXPECT_EQ(nullptr, decoded.pAncestorContext);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(decoded.pContextPolicy, decoded.pAncestorContext, true));
  tDestroySTriggerCalcRequest(&decoded);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, NestedBrandNewCreateTableRejectsStrippedPolicy) {
  SSTriggerCalcRequest source = {};
  source.streamId = 11;
  source.runnerTaskId = 22;
  source.sessionId = 33;
  source.triggerType = STREAM_TRIGGER_COUNT;
  source.gid = 101;
  source.createTable = 1;
  source.isWindowTrigger = true;
  source.params = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, source.params);
  source.pContextPolicy = makeContextPolicy();
  auto         bytes = serializeCalcRequest(source);
  const size_t tailOffset = policyFrameOffset(bytes);
  ASSERT_LT(tailOffset, bytes.size());
  bytes.resize(tailOffset);
  writeI32(&bytes, 0, static_cast<int32_t>(bytes.size() - sizeof(int32_t)));

  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_EQ(nullptr, decoded.pContextPolicy);
  EXPECT_NE(TSDB_CODE_SUCCESS, tAdmitStreamContext(decoded.pContextPolicy, decoded.pAncestorContext, true));
  tDestroySTriggerCalcRequest(&decoded);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamAncestorContextTest, SingleLayerBrandNewCreateTableRequiresSyntheticParam) {
  SSTriggerCalcRequest source = {};
  source.streamId = 11;
  source.runnerTaskId = 22;
  source.sessionId = 33;
  source.triggerType = STREAM_TRIGGER_COUNT;
  source.gid = 101;
  source.createTable = 1;
  source.isWindowTrigger = true;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&source));
  const auto legacyBytes = legacyCreateOnlyCalcRequestWire();
  EXPECT_EQ(legacyBytes, serializeCalcRequest(source));
  EXPECT_EQ(nullptr, source.params);
  source.params = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  ASSERT_NE(nullptr, source.params);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&source));
  auto bytes = serializeCalcRequest(source);
  EXPECT_EQ(legacyBytes, bytes);
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  EXPECT_TRUE(decoded.createTable);
  EXPECT_EQ(1, taosArrayGetSize(decoded.params));
  tDestroySTriggerCalcRequest(&decoded);
  tDestroySTriggerCalcRequest(&source);
}

TEST(StreamCalcRequestAdmissionTest, LegacyCreateOnlyWireDecodesToOwnedSyntheticParam) {
  auto                 bytes = legacyCreateOnlyCalcRequestWire();
  SSTriggerCalcRequest decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSTriggerCalcRequest(bytes.data(), bytes.size(), &decoded));
  ASSERT_NE(nullptr, decoded.params);
  ASSERT_EQ(1, taosArrayGetSize(decoded.params));
  const auto* param = static_cast<const SSTriggerCalcParam*>(taosArrayGet(decoded.params, 0));
  ASSERT_NE(nullptr, param);
  const SSTriggerCalcParam empty = {};
  EXPECT_EQ(0, memcmp(param, &empty, sizeof(empty)));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSTriggerCalcRequest(&decoded));
  tDestroySTriggerCalcRequest(&decoded);
}

TEST(StreamAncestorContextTest, ContextPolicyCloneIsDeepAndAdmissionIsSetExact) {
  SStreamContextPolicy* source = makeContextPolicy({makeContextPolicyEntry(101, 0, STREAM_CONTEXT_POLICY_NONE),
                                                    makeContextPolicyEntry(101, 1, STREAM_CONTEXT_POLICY_ANCESTOR)});
  SStreamContextPolicy* clone = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamContextPolicy(source, &clone));
  ASSERT_NE(nullptr, clone);
  EXPECT_NE(source, clone);
  EXPECT_NE(source->pEntries, clone->pEntries);
  static_cast<SStreamContextPolicyEntry*>(taosArrayGet(source->pEntries, 1))->paramIndex = 7;
  EXPECT_EQ(1, static_cast<const SStreamContextPolicyEntry*>(taosArrayGet(clone->pEntries, 1))->paramIndex);

  SStreamAncestorContext* context = makeAncestorContext(101, 1);
  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(clone, context, true));
  static_cast<SStreamAncestorParamContext*>(taosArrayGet(context->pParamContexts, 0))->paramIndex = 0;
  EXPECT_NE(TSDB_CODE_SUCCESS, tAdmitStreamContext(clone, context, true));
  EXPECT_NE(TSDB_CODE_SUCCESS, tAdmitStreamContext(clone, nullptr, true));
  EXPECT_NE(TSDB_CODE_SUCCESS, tAdmitStreamContext(nullptr, context, true));
  EXPECT_NE(TSDB_CODE_SUCCESS, tAdmitStreamContext(clone, nullptr, false));

  tDestroyStreamAncestorContext(&context);
  tDestroyStreamContextPolicy(&clone);
  tDestroyStreamContextPolicy(&source);
}

TEST(StreamAncestorContextTest, LargeContextAdmissionDoesNotRescanEveryPolicyEntry) {
  constexpr int32_t     kEntryCount = 8192;
  SStreamContextPolicy* policy = makeContextPolicy();
  ASSERT_NE(nullptr, policy);
  SStreamAncestorContext* context = static_cast<SStreamAncestorContext*>(taosMemoryCalloc(1, sizeof(*context)));
  ASSERT_NE(nullptr, context);
  context->pParamContexts = taosArrayInit(kEntryCount, sizeof(SStreamAncestorParamContext));
  ASSERT_NE(nullptr, context->pParamContexts);

  for (int32_t i = 0; i < kEntryCount; ++i) {
    const int64_t                   gid = 1000 + i;
    const SStreamContextPolicyEntry entry = makeContextPolicyEntry(gid, 0, STREAM_CONTEXT_POLICY_ANCESTOR);
    ASSERT_NE(nullptr, taosArrayPush(policy->pEntries, &entry));

    SStreamAncestorContext* one = makeAncestorContext(gid);
    ASSERT_NE(nullptr, one);
    ASSERT_NE(nullptr, one->pParamContexts);
    ASSERT_EQ(1, taosArrayGetSize(one->pParamContexts));
    ASSERT_NE(nullptr, taosArrayPush(context->pParamContexts, taosArrayGet(one->pParamContexts, 0)));
    taosArrayDestroy(one->pParamContexts);
    one->pParamContexts = nullptr;
    tDestroyStreamAncestorContext(&one);
  }

  EXPECT_EQ(TSDB_CODE_SUCCESS, tAdmitStreamContext(policy, context, true));

  tDestroyStreamAncestorContext(&context);
  tDestroyStreamContextPolicy(&policy);
}

TEST(StreamAncestorContextTest, ContextPolicyRejectsMalformedEntries) {
  for (int32_t testCase = 0; testCase < 4; ++testCase) {
    SCOPED_TRACE(testCase);
    SStreamContextPolicy* policy = makeContextPolicy({makeContextPolicyEntry(101, 0, STREAM_CONTEXT_POLICY_NONE),
                                                      makeContextPolicyEntry(202, 0, STREAM_CONTEXT_POLICY_ANCESTOR)});
    auto*                 first = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(policy->pEntries, 0));
    auto*                 second = static_cast<SStreamContextPolicyEntry*>(taosArrayGet(policy->pEntries, 1));
    ASSERT_NE(nullptr, first);
    ASSERT_NE(nullptr, second);
    switch (testCase) {
      case 0:
        second->gid = first->gid;
        break;
      case 1:
        second->gid = first->gid;
        second->paramIndex = first->paramIndex;
        break;
      case 2:
        first->paramIndex = -1;
        break;
      case 3:
        first->contextPolicy = 2;
        break;
    }
    SEncoder encoder = {};
    tEncoderInit(&encoder, nullptr, 0);
    EXPECT_NE(TSDB_CODE_SUCCESS, tEncodeStreamContextPolicy(&encoder, policy));
    tEncoderClear(&encoder);
    tDestroyStreamContextPolicy(&policy);
  }
}

TEST(StreamAncestorContextTest, ContextPolicyFrameMalformedMatrix) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest(0);
  setSingleAncestorPolicy(&source);
  source.pAncestorContext = makeAncestorContext(source.gid);
  const auto original = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  const size_t policyOffset = policyFrameOffset(original);
  const size_t contextOffset = ancestorFrameOffset(original);
  ASSERT_LT(policyOffset, contextOffset);

  auto duplicate = original;
  duplicate.insert(duplicate.end(), original.begin() + policyOffset, original.begin() + contextOffset);
  writeI32(&duplicate, 0, static_cast<int32_t>(duplicate.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(duplicate);

  auto badVersion = original;
  writeU16(&badVersion, policyOffset + sizeof(uint32_t), STREAM_CONTEXT_POLICY_FRAME_VERSION + 1);
  expectCalcRequestDecodeFailure(badVersion);

  auto badFlags = original;
  writeU16(&badFlags, policyOffset + sizeof(uint32_t) + sizeof(uint16_t), 1);
  expectCalcRequestDecodeFailure(badFlags);

  const size_t payloadOffset = policyOffset + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
  auto         badPayloadVersion = original;
  writeI32(&badPayloadVersion, payloadOffset, STREAM_CONTEXT_POLICY_VERSION + 1);
  expectCalcRequestDecodeFailure(badPayloadVersion);

  for (int32_t count : {-1, INT32_MAX}) {
    auto badCount = original;
    writeI32(&badCount, payloadOffset + sizeof(int32_t), count);
    expectCalcRequestDecodeFailure(badCount);
  }

  auto badPolicy = original;
  badPolicy[payloadOffset + sizeof(int32_t) * 3 + sizeof(int64_t)] = 2;
  expectCalcRequestDecodeFailure(badPolicy);

  auto truncated = original;
  truncated.resize(policyOffset + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2 + 1);
  writeI32(&truncated, 0, static_cast<int32_t>(truncated.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(truncated);

  auto     trailing = original;
  uint32_t payloadLength = 0;
  memcpy(&payloadLength, trailing.data() + policyOffset + sizeof(uint32_t) + sizeof(uint16_t) * 2,
         sizeof(payloadLength));
  const size_t policyEnd = contextOffset;
  trailing.insert(trailing.begin() + policyEnd, 0);
  writeU32(&trailing, policyOffset + sizeof(uint32_t) + sizeof(uint16_t) * 2, payloadLength + 1);
  writeI32(&trailing, 0, static_cast<int32_t>(trailing.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(trailing);

  auto                 reversed = original;
  std::vector<uint8_t> policyFrame(reversed.begin() + policyOffset, reversed.begin() + contextOffset);
  reversed.erase(reversed.begin() + policyOffset, reversed.begin() + contextOffset);
  reversed.insert(reversed.end(), policyFrame.begin(), policyFrame.end());
  expectCalcRequestDecodeFailure(reversed);

  auto missingPolicy = original;
  missingPolicy.erase(missingPolicy.begin() + policyOffset, missingPolicy.begin() + contextOffset);
  writeI32(&missingPolicy, 0, static_cast<int32_t>(missingPolicy.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(missingPolicy);

  auto missingContext = original;
  missingContext.resize(contextOffset);
  writeI32(&missingContext, 0, static_cast<int32_t>(missingContext.size() - sizeof(int32_t)));
  expectCalcRequestDecodeFailure(missingContext);

  SSTriggerCalcRequest     twoParams = makeSingleGroupCalcRequest(0);
  const SSTriggerCalcParam secondParam = makeWindowCalcParam(2000, 2099, 0);
  ASSERT_NE(nullptr, taosArrayPush(twoParams.params, &secondParam));
  twoParams.pContextPolicy =
      makeContextPolicy({makeContextPolicyEntry(twoParams.gid, 0, STREAM_CONTEXT_POLICY_NONE),
                         makeContextPolicyEntry(twoParams.gid, 1, STREAM_CONTEXT_POLICY_ANCESTOR)});
  twoParams.pAncestorContext = makeAncestorContext(twoParams.gid, 1);
  auto unordered = serializeCalcRequest(twoParams);
  tDestroySTriggerCalcRequest(&twoParams);
  const size_t unorderedPayload = policyFrameOffset(unordered) + sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
  writeI32(
      &unordered,
      unorderedPayload + sizeof(int32_t) * 2 + sizeof(int64_t) + sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t), 0);
  expectCalcRequestDecodeFailure(unordered);
}

TEST(StreamAncestorContextTest, calcRequestWithoutContextPreservesLegacyGoldenBytes) {
  SSTriggerCalcRequest source = makeSingleGroupCalcRequest();
  const auto           bytes = serializeCalcRequest(source);
  tDestroySTriggerCalcRequest(&source);
  static constexpr char golden[] =
      "\x64\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00"
      "\x00\x00\x00\x00\x21\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00"
      "\x00\x00\x65\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\xe8\x03"
      "\x00\x00\x00\x00\x00\x00\x4b\x04\x00\x00\x00\x00\x00\x00\x64\x00"
      "\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\xb0\x04"
      "\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00"
      "\x00\x00\x00\x00\x00\x00\x01\x00";
  EXPECT_EQ(std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(golden),
                                 reinterpret_cast<const uint8_t*>(golden) + sizeof(golden) - 1),
            bytes);
}

TEST(StreamRuntimeCodecTest, CanonicalOrderIsLocalOnly) {
  SStreamRuntimeFuncInfo source = {};
  source.pGroupCalcOrder = taosArrayInit(2, sizeof(uint64_t));
  ASSERT_NE(nullptr, source.pGroupCalcOrder);
  const uint64_t first = 101;
  const uint64_t second = 202;
  ASSERT_NE(nullptr, taosArrayPush(source.pGroupCalcOrder, &first));
  ASSERT_NE(nullptr, taosArrayPush(source.pGroupCalcOrder, &second));

  const std::vector<uint8_t> encoded = encodeRuntimeFuncInfo(&source);
  SStreamRuntimeFuncInfo     decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, decodeRuntimeFuncInfo(encoded, &decoded));
  EXPECT_EQ(nullptr, decoded.pGroupCalcOrder);
  tDestroyStRtFuncInfo(&decoded);
  tDestroyStRtFuncInfo(&source);
}

static int64_t packLineage(int32_t winOutIdx, int32_t rowStartIdx) {
  return (static_cast<int64_t>(rowStartIdx) << 32) | static_cast<uint32_t>(winOutIdx);
}

void writeRunnerFetchU32(std::vector<uint8_t>* data, size_t offset, uint32_t value) {
  ASSERT_LE(offset + sizeof(value), data->size());
  for (size_t i = 0; i < sizeof(value); ++i) {
    (*data)[offset + i] = static_cast<uint8_t>(value >> ((sizeof(value) - i - 1) * CHAR_BIT));
  }
}

void writeRunnerFetchU64(std::vector<uint8_t>* data, size_t offset, uint64_t value) {
  ASSERT_LE(offset + sizeof(value), data->size());
  for (size_t i = 0; i < sizeof(value); ++i) {
    (*data)[offset + i] = static_cast<uint8_t>(value >> ((sizeof(value) - i - 1) * CHAR_BIT));
  }
}

SArray* makeRunnerFetchLineage(std::initializer_list<int64_t> entries) {
  SArray* lineage = taosArrayInit(static_cast<int32_t>(std::max<size_t>(entries.size(), 1)), sizeof(int64_t));
  EXPECT_NE(nullptr, lineage);
  if (lineage == nullptr) return nullptr;

  for (int64_t entry : entries) {
    EXPECT_NE(nullptr, taosArrayPush(lineage, &entry));
  }
  return lineage;
}

std::vector<uint8_t> serializeRunnerFetchFrame(const SStreamRunnerFetchFrame& frame) {
  const int32_t encodedLen = tSerializeSStreamRunnerFetchFrame(nullptr, 0, &frame);
  EXPECT_GT(encodedLen, 0);
  if (encodedLen <= 0) return {};

  std::vector<uint8_t> encoded(encodedLen);
  EXPECT_EQ(encodedLen, tSerializeSStreamRunnerFetchFrame(encoded.data(), encoded.size(), &frame));
  return encoded;
}

void expectRunnerFetchFrameDecodeFailure(const std::vector<uint8_t>& encoded) {
  SStreamRunnerFetchFrame decoded = {};
  decoded.type = STREAM_RUNNER_FETCH_EOG;
  decoded.baseGId = 999;
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tDeserializeSStreamRunnerFetchFrame(encoded.data(), static_cast<int32_t>(encoded.size()), &decoded));
  EXPECT_EQ(STREAM_RUNNER_FETCH_LEGACY, decoded.type);
  EXPECT_EQ(0, decoded.baseGId);
  EXPECT_EQ(nullptr, decoded.pLineage);
  tDestroySStreamRunnerFetchFrame(&decoded);
}

TEST(StreamRunnerFetchFrameTest, DataRoundTripPreservesCompressedLineageAndExactLength) {
  SStreamRunnerFetchFrame source = {};
  source.type = STREAM_RUNNER_FETCH_DATA;
  source.baseGId = 101;
  source.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2)});
  ASSERT_NE(nullptr, source.pLineage);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&source, 4));

  const int32_t encodedLen = tSerializeSStreamRunnerFetchFrame(nullptr, 0, &source);
  ASSERT_EQ(34, encodedLen);
  std::vector<uint8_t> encoded(encodedLen);
  ASSERT_EQ(encodedLen, tSerializeSStreamRunnerFetchFrame(encoded.data(), encoded.size(), &source));
  static const uint8_t golden[] = {
      0x00, 0x00, 0x00, 0x1e, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00,
      0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x07,
  };
  EXPECT_EQ(std::vector<uint8_t>(golden, golden + sizeof(golden)), encoded);
  std::vector<uint8_t> shortBuffer(encodedLen - 1);
  EXPECT_NE(encodedLen, tSerializeSStreamRunnerFetchFrame(shortBuffer.data(), shortBuffer.size(), &source));

  SStreamRunnerFetchFrame decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchFrame(encoded.data(), encoded.size(), &decoded));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&decoded, 4));
  ASSERT_EQ(STREAM_RUNNER_FETCH_DATA, decoded.type);
  ASSERT_EQ(101, decoded.baseGId);
  ASSERT_NE(nullptr, decoded.pLineage);
  ASSERT_EQ(2, taosArrayGetSize(decoded.pLineage));
  EXPECT_EQ(packLineage(3, 0), *static_cast<int64_t*>(taosArrayGet(decoded.pLineage, 0)));
  EXPECT_EQ(packLineage(7, 2), *static_cast<int64_t*>(taosArrayGet(decoded.pLineage, 1)));

  tDestroySStreamRunnerFetchFrame(&decoded);
  tDestroySStreamRunnerFetchFrame(&source);
}

TEST(StreamRunnerFetchFrameTest, EogRoundTripCarriesIdentityWithoutLineage) {
  SStreamRunnerFetchFrame source = {};
  source.type = STREAM_RUNNER_FETCH_EOG;
  source.baseGId = 101;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&source, 0));

  const std::vector<uint8_t> encoded = serializeRunnerFetchFrame(source);
  ASSERT_EQ(18, encoded.size());
  static const uint8_t golden[] = {
      0x00, 0x00, 0x00, 0x0e, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00,
  };
  EXPECT_EQ(std::vector<uint8_t>(golden, golden + sizeof(golden)), encoded);
  SStreamRunnerFetchFrame decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchFrame(encoded.data(), encoded.size(), &decoded));
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&decoded, 0));
  EXPECT_EQ(STREAM_RUNNER_FETCH_EOG, decoded.type);
  EXPECT_EQ(101, decoded.baseGId);
  EXPECT_EQ(nullptr, decoded.pLineage);

  tDestroySStreamRunnerFetchFrame(&decoded);
  EXPECT_EQ(STREAM_RUNNER_FETCH_LEGACY, decoded.type);
  EXPECT_EQ(0, decoded.baseGId);
  EXPECT_EQ(nullptr, decoded.pLineage);
}

TEST(StreamRunnerFetchFrameTest, DecoderRejectsUnknownVersionTypeAndNonExactLengths) {
  SStreamRunnerFetchFrame source = {};
  source.type = STREAM_RUNNER_FETCH_DATA;
  source.baseGId = 101;
  source.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2)});
  ASSERT_NE(nullptr, source.pLineage);
  const std::vector<uint8_t> valid = serializeRunnerFetchFrame(source);
  ASSERT_EQ(34, valid.size());

  auto unknownVersion = valid;
  unknownVersion[sizeof(uint32_t)] = STREAM_RUNNER_FETCH_FRAME_VERSION + 1;
  expectRunnerFetchFrameDecodeFailure(unknownVersion);

  auto unknownType = valid;
  unknownType[sizeof(uint32_t) + sizeof(uint8_t)] = 99;
  expectRunnerFetchFrameDecodeFailure(unknownType);

  for (size_t cut = 0; cut < valid.size(); ++cut) {
    SCOPED_TRACE(cut);
    expectRunnerFetchFrameDecodeFailure(std::vector<uint8_t>(valid.begin(), valid.begin() + cut));
  }

  auto trailing = valid;
  trailing.push_back(0);
  expectRunnerFetchFrameDecodeFailure(trailing);
  writeRunnerFetchU32(&trailing, 0, static_cast<uint32_t>(trailing.size() - sizeof(uint32_t)));
  expectRunnerFetchFrameDecodeFailure(trailing);

  auto negativeCount = valid;
  writeRunnerFetchU32(&negativeCount, sizeof(uint32_t) + sizeof(uint8_t) + sizeof(int8_t) + sizeof(uint64_t),
                      UINT32_MAX);
  expectRunnerFetchFrameDecodeFailure(negativeCount);

  auto excessiveCount = valid;
  writeRunnerFetchU32(&excessiveCount, sizeof(uint32_t) + sizeof(uint8_t) + sizeof(int8_t) + sizeof(uint64_t),
                      STREAM_CALC_REQ_MAX_WIN_NUM + 1);
  expectRunnerFetchFrameDecodeFailure(excessiveCount);

  tDestroySStreamRunnerFetchFrame(&source);
}

TEST(StreamRunnerFetchFrameTest, StrictShapeRejectsLegacyZeroGidAndWrongLineageOwnership) {
  SStreamRunnerFetchFrame legacy = {};
  EXPECT_NE(TSDB_CODE_SUCCESS, tSerializeSStreamRunnerFetchFrame(nullptr, 0, &legacy));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&legacy, 0));

  SStreamRunnerFetchFrame data = {};
  data.type = STREAM_RUNNER_FETCH_DATA;
  data.baseGId = 101;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&data, 4));
  data.pLineage = makeRunnerFetchLineage({packLineage(3, 0)});
  ASSERT_NE(nullptr, data.pLineage);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&data, 0));
  data.baseGId = 0;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&data, 1));
  EXPECT_NE(TSDB_CODE_SUCCESS, tSerializeSStreamRunnerFetchFrame(nullptr, 0, &data));
  data.baseGId = 101;

  SStreamRunnerFetchFrame eog = {};
  eog.type = STREAM_RUNNER_FETCH_EOG;
  eog.baseGId = 101;
  eog.pLineage = makeRunnerFetchLineage({packLineage(3, 0)});
  ASSERT_NE(nullptr, eog.pLineage);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&eog, 0));
  EXPECT_NE(TSDB_CODE_SUCCESS, tSerializeSStreamRunnerFetchFrame(nullptr, 0, &eog));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&eog, 1));

  SStreamRunnerFetchFrame emptyEog = {};
  emptyEog.type = STREAM_RUNNER_FETCH_EOG;
  emptyEog.baseGId = 101;
  emptyEog.pLineage = makeRunnerFetchLineage({});
  ASSERT_NE(nullptr, emptyEog.pLineage);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchFrame(&emptyEog, 0));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tSerializeSStreamRunnerFetchFrame(nullptr, 0, &emptyEog));

  SStreamRunnerFetchFrame excessiveLineage = {};
  excessiveLineage.type = STREAM_RUNNER_FETCH_EOG;
  excessiveLineage.baseGId = 101;
  excessiveLineage.pLineage = makeRunnerFetchLineage({});
  ASSERT_NE(nullptr, excessiveLineage.pLineage);
  excessiveLineage.pLineage->size = static_cast<size_t>(UINT32_MAX) + 1;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&excessiveLineage, 0));
  excessiveLineage.pLineage->size = 0;

  const std::vector<uint8_t> dataBytes = serializeRunnerFetchFrame(data);
  auto                       zeroGid = dataBytes;
  writeRunnerFetchU64(&zeroGid, sizeof(uint32_t) + sizeof(uint8_t) + sizeof(int8_t), 0);
  expectRunnerFetchFrameDecodeFailure(zeroGid);

  auto eogWithLineage = dataBytes;
  eogWithLineage[sizeof(uint32_t) + sizeof(uint8_t)] = STREAM_RUNNER_FETCH_EOG;
  expectRunnerFetchFrameDecodeFailure(eogWithLineage);

  SStreamRunnerFetchFrame validEog = {};
  validEog.type = STREAM_RUNNER_FETCH_EOG;
  validEog.baseGId = 101;
  const std::vector<uint8_t> eogBytes = serializeRunnerFetchFrame(validEog);
  auto                       dataWithoutLineage = eogBytes;
  dataWithoutLineage[sizeof(uint32_t) + sizeof(uint8_t)] = STREAM_RUNNER_FETCH_DATA;
  expectRunnerFetchFrameDecodeFailure(dataWithoutLineage);

  tDestroySStreamRunnerFetchFrame(&emptyEog);
  tDestroySStreamRunnerFetchFrame(&excessiveLineage);
  tDestroySStreamRunnerFetchFrame(&eog);
  tDestroySStreamRunnerFetchFrame(&data);
}

TEST(StreamRunnerFetchFrameTest, ValidatorRejectsInvalidPackedRowStarts) {
  SStreamRunnerFetchFrame source = {};
  source.type = STREAM_RUNNER_FETCH_DATA;
  source.baseGId = 101;
  source.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2), packLineage(9, 3)});
  ASSERT_NE(nullptr, source.pLineage);
  const std::vector<uint8_t> valid = serializeRunnerFetchFrame(source);
  ASSERT_EQ(42, valid.size());
  constexpr size_t lineageOffset =
      sizeof(uint32_t) + sizeof(uint8_t) + sizeof(int8_t) + sizeof(uint64_t) + sizeof(uint32_t);

  auto invalidFirst = valid;
  writeRunnerFetchU64(&invalidFirst, lineageOffset, packLineage(3, 1));
  expectRunnerFetchFrameDecodeFailure(invalidFirst);

  auto duplicateStart = valid;
  writeRunnerFetchU64(&duplicateStart, lineageOffset + sizeof(int64_t), packLineage(7, 0));
  expectRunnerFetchFrameDecodeFailure(duplicateStart);

  auto decreasingStart = valid;
  writeRunnerFetchU64(&decreasingStart, lineageOffset + sizeof(int64_t) * 2, packLineage(9, 1));
  expectRunnerFetchFrameDecodeFailure(decreasingStart);

  auto negativeStart = valid;
  writeRunnerFetchU64(&negativeStart, lineageOffset + sizeof(int64_t), (UINT64_C(0xffffffff) << 32) | UINT64_C(7));
  expectRunnerFetchFrameDecodeFailure(negativeStart);

  auto overflowingStart = valid;
  writeRunnerFetchU64(&overflowingStart, lineageOffset + sizeof(int64_t), (UINT64_C(0x80000000) << 32) | UINT64_C(7));
  expectRunnerFetchFrameDecodeFailure(overflowingStart);

  SStreamRunnerFetchFrame negativeWinOut = {};
  negativeWinOut.type = STREAM_RUNNER_FETCH_DATA;
  negativeWinOut.baseGId = 101;
  negativeWinOut.pLineage = makeRunnerFetchLineage({packLineage(-1, 0)});
  ASSERT_NE(nullptr, negativeWinOut.pLineage);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&negativeWinOut, 1));
  tDestroySStreamRunnerFetchFrame(&negativeWinOut);

  SStreamRunnerFetchFrame minimumWinOut = {};
  minimumWinOut.type = STREAM_RUNNER_FETCH_DATA;
  minimumWinOut.baseGId = 101;
  minimumWinOut.pLineage = makeRunnerFetchLineage({packLineage(INT32_MIN, 0)});
  ASSERT_NE(nullptr, minimumWinOut.pLineage);
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&minimumWinOut, 1));
  tDestroySStreamRunnerFetchFrame(&minimumWinOut);

  SStreamRunnerFetchFrame rangeSource = {};
  rangeSource.type = STREAM_RUNNER_FETCH_DATA;
  rangeSource.baseGId = 101;
  rangeSource.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2)});
  ASSERT_NE(nullptr, rangeSource.pLineage);
  auto outOfRangeStart = serializeRunnerFetchFrame(rangeSource);
  writeRunnerFetchU64(&outOfRangeStart, lineageOffset + sizeof(int64_t), packLineage(7, 4));
  SStreamRunnerFetchFrame decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            tDeserializeSStreamRunnerFetchFrame(outOfRangeStart.data(), outOfRangeStart.size(), &decoded));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchFrame(&decoded, 4));
  tDestroySStreamRunnerFetchFrame(&decoded);

  tDestroySStreamRunnerFetchFrame(&rangeSource);
  tDestroySStreamRunnerFetchFrame(&source);
}

TEST(StreamRunnerFetchFrameTest, DecoderReleasesPriorLineageBeforeSuccessAndFailure) {
  SStreamRunnerFetchFrame firstSource = {};
  firstSource.type = STREAM_RUNNER_FETCH_DATA;
  firstSource.baseGId = 101;
  firstSource.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2)});
  ASSERT_NE(nullptr, firstSource.pLineage);
  const std::vector<uint8_t> first = serializeRunnerFetchFrame(firstSource);

  SStreamRunnerFetchFrame secondSource = {};
  secondSource.type = STREAM_RUNNER_FETCH_DATA;
  secondSource.baseGId = 202;
  secondSource.pLineage = makeRunnerFetchLineage({packLineage(11, 0)});
  ASSERT_NE(nullptr, secondSource.pLineage);
  const std::vector<uint8_t> second = serializeRunnerFetchFrame(secondSource);

  SStreamRunnerFetchFrame decoded = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchFrame(first.data(), first.size(), &decoded));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchFrame(second.data(), second.size(), &decoded));
  ASSERT_EQ(STREAM_RUNNER_FETCH_DATA, decoded.type);
  ASSERT_EQ(202, decoded.baseGId);
  ASSERT_NE(nullptr, decoded.pLineage);
  ASSERT_EQ(1, taosArrayGetSize(decoded.pLineage));
  EXPECT_EQ(packLineage(11, 0), *static_cast<int64_t*>(taosArrayGet(decoded.pLineage, 0)));

  auto unknownVersion = first;
  unknownVersion[sizeof(uint32_t)] = STREAM_RUNNER_FETCH_FRAME_VERSION + 1;
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tDeserializeSStreamRunnerFetchFrame(unknownVersion.data(), unknownVersion.size(), &decoded));
  EXPECT_EQ(STREAM_RUNNER_FETCH_LEGACY, decoded.type);
  EXPECT_EQ(0, decoded.baseGId);
  EXPECT_EQ(nullptr, decoded.pLineage);

  tDestroySStreamRunnerFetchFrame(&decoded);
  tDestroySStreamRunnerFetchFrame(&secondSource);
  tDestroySStreamRunnerFetchFrame(&firstSource);
}

struct RunnerFetchBatchGuard {
  SStreamRunnerFetchBatch batch = {};
  ~RunnerFetchBatchGuard() { tDestroySStreamRunnerFetchBatch(&batch); }
};

void fillRunnerFetchBatch(SStreamRunnerFetchBatch* batch, bool data, std::initializer_list<uint64_t> eogs) {
  if (data) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&batch->pBlock));
    SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(batch->pBlock, &column));
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(batch->pBlock, 4));
    auto* storedColumn = static_cast<SColumnInfoData*>(taosArrayGet(batch->pBlock->pDataBlock, 0));
    for (int64_t row = 0; row < 4; ++row) {
      const int64_t value = 1000 + row;
      ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(storedColumn, row, reinterpret_cast<const char*>(&value), row == 2));
    }
    batch->pBlock->info.rows = 4;
    batch->pBlock->info.id.baseGId = 101;
    batch->pBlock->info.id.groupId = 101;
    batch->dataFrame.type = STREAM_RUNNER_FETCH_DATA;
    batch->dataFrame.baseGId = 101;
    batch->dataFrame.pLineage = makeRunnerFetchLineage({packLineage(3, 0), packLineage(7, 2)});
    ASSERT_NE(nullptr, batch->dataFrame.pLineage);
  }
  if (eogs.size() > 0) {
    batch->pEogGids = taosArrayFromList(eogs.begin(), eogs.size(), sizeof(uint64_t));
    ASSERT_NE(nullptr, batch->pEogGids);
  }
}

std::vector<uint8_t> serializeRunnerFetchBatch(const SStreamRunnerFetchBatch& batch) {
  const int32_t encodedLen = tSerializeSStreamRunnerFetchBatch(nullptr, 0, &batch);
  EXPECT_GT(encodedLen, 0);
  if (encodedLen <= 0) return {};
  std::vector<uint8_t> encoded(encodedLen);
  EXPECT_EQ(encodedLen, tSerializeSStreamRunnerFetchBatch(encoded.data(), encoded.size(), &batch));
  return encoded;
}

void expectEmptyRunnerFetchBatch(const SStreamRunnerFetchBatch& batch) {
  EXPECT_EQ(nullptr, batch.pBlock);
  EXPECT_EQ(nullptr, batch.dataFrame.pLineage);
  EXPECT_EQ(STREAM_RUNNER_FETCH_LEGACY, batch.dataFrame.type);
  EXPECT_EQ(0, batch.dataFrame.baseGId);
  EXPECT_EQ(nullptr, batch.pEogGids);
}

void expectRunnerFetchBatchDecodeFailure(const std::vector<uint8_t>& bytes) {
  RunnerFetchBatchGuard decoded;
  EXPECT_NE(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
  expectEmptyRunnerFetchBatch(decoded.batch);
}

TEST(StreamRunnerFetchBatchTest, EogOnlyRoundTripMatchesGoldenBytes) {
  RunnerFetchBatchGuard source;
  fillRunnerFetchBatch(&source.batch, false, {101, 202});
  const auto           bytes = serializeRunnerFetchBatch(source.batch);
  static const uint8_t golden[] = {
      0x00, 0x00, 0x00, 0x1e, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xca,
  };
  EXPECT_EQ(std::vector<uint8_t>(golden, golden + sizeof(golden)), bytes);
  RunnerFetchBatchGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
  EXPECT_EQ(nullptr, decoded.batch.pBlock);
  EXPECT_EQ(nullptr, decoded.batch.dataFrame.pLineage);
  ASSERT_EQ(2, taosArrayGetSize(decoded.batch.pEogGids));
  EXPECT_EQ(101, *static_cast<uint64_t*>(taosArrayGet(decoded.batch.pEogGids, 0)));
  EXPECT_EQ(202, *static_cast<uint64_t*>(taosArrayGet(decoded.batch.pEogGids, 1)));
}

TEST(StreamRunnerFetchBatchTest, DataWithOptionalTailOwnsCompleteBlockAndLineage) {
  for (bool withEogs : {false, true}) {
    RunnerFetchBatchGuard source;
    fillRunnerFetchBatch(&source.batch, true,
                         withEogs ? std::initializer_list<uint64_t>{101, 202} : std::initializer_list<uint64_t>{});
    const auto           bytes = serializeRunnerFetchBatch(source.batch);
    std::vector<uint8_t> shortBuffer(bytes.size() - 1);
    EXPECT_LT(tSerializeSStreamRunnerFetchBatch(shortBuffer.data(), shortBuffer.size(), &source.batch), 0);
    RunnerFetchBatchGuard decoded;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
    tDestroySStreamRunnerFetchBatch(&source.batch);
    ASSERT_NE(nullptr, decoded.batch.pBlock);
    EXPECT_EQ(4, decoded.batch.pBlock->info.rows);
    EXPECT_EQ(101, decoded.batch.pBlock->info.id.baseGId);
    EXPECT_EQ(101, decoded.batch.pBlock->info.id.groupId);
    auto* column = static_cast<SColumnInfoData*>(taosArrayGet(decoded.batch.pBlock->pDataBlock, 0));
    ASSERT_NE(nullptr, column);
    EXPECT_EQ(1000, *reinterpret_cast<int64_t*>(colDataGetData(column, 0)));
    EXPECT_TRUE(colDataIsNull_s(column, 2));
    EXPECT_EQ(1003, *reinterpret_cast<int64_t*>(colDataGetData(column, 3)));
    ASSERT_EQ(2, taosArrayGetSize(decoded.batch.dataFrame.pLineage));
    EXPECT_EQ(packLineage(3, 0), *static_cast<int64_t*>(taosArrayGet(decoded.batch.dataFrame.pLineage, 0)));
    EXPECT_EQ(packLineage(7, 2), *static_cast<int64_t*>(taosArrayGet(decoded.batch.dataFrame.pLineage, 1)));
    EXPECT_EQ(withEogs ? 2 : 0, taosArrayGetSize(decoded.batch.pEogGids));
  }
}

TEST(StreamRunnerFetchBatchTest, RejectsEmptyInvalidIdentityAndExcessiveEogs) {
  RunnerFetchBatchGuard source;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  fillRunnerFetchBatch(&source.batch, false, {101, 0});
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  *static_cast<uint64_t*>(taosArrayGet(source.batch.pEogGids, 1)) = 101;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  taosArrayClear(source.batch.pEogGids);
  for (uint64_t gid = 1; gid <= STREAM_RUNNER_EOG_BATCH_MAX; ++gid) {
    ASSERT_NE(nullptr, taosArrayPush(source.batch.pEogGids, &gid));
  }
  EXPECT_EQ(TSDB_CODE_SUCCESS, tValidateSStreamRunnerFetchBatch(&source.batch));
  const auto            atLimit = serializeRunnerFetchBatch(source.batch);
  RunnerFetchBatchGuard decoded;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchBatch(atLimit.data(), atLimit.size(), &decoded.batch));
  EXPECT_EQ(STREAM_RUNNER_EOG_BATCH_MAX, taosArrayGetSize(decoded.batch.pEogGids));
  uint64_t extraGid = STREAM_RUNNER_EOG_BATCH_MAX + 1;
  ASSERT_NE(nullptr, taosArrayPush(source.batch.pEogGids, &extraGid));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  tDestroySStreamRunnerFetchBatch(&source.batch);
  fillRunnerFetchBatch(&source.batch, true, {202});
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  *static_cast<uint64_t*>(taosArrayGet(source.batch.pEogGids, 0)) = 101;
  source.batch.pBlock->info.id.baseGId = 202;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  source.batch.pBlock->info.id.baseGId = 101;
  source.batch.pBlock->info.id.groupId = 202;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  source.batch.pBlock->info.id.groupId = 101;
  *static_cast<int64_t*>(taosArrayGet(source.batch.dataFrame.pLineage, 1)) = packLineage(7, 4);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
  blockDataDestroy(source.batch.pBlock);
  source.batch.pBlock = nullptr;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateSStreamRunnerFetchBatch(&source.batch));
}

TEST(StreamRunnerFetchBatchTest, DecoderRejectsMalformedShapeLengthsBlocksAndLineage) {
  RunnerFetchBatchGuard source;
  fillRunnerFetchBatch(&source.batch, true, {101, 202});
  const auto   original = serializeRunnerFetchBatch(source.batch);
  const size_t blockStart = 18;
  const size_t frameStart = blockStart + blockGetInternalEncodeSize(source.batch.pBlock);
  const size_t eogStart = original.size() - 2 * sizeof(uint64_t);
  for (size_t cut = 0; cut < original.size(); ++cut) {
    expectRunnerFetchBatchDecodeFailure(std::vector<uint8_t>(original.begin(), original.begin() + cut));
  }
  for (size_t offset : {size_t{4}, size_t{5}}) {
    auto badVersionOrFlag = original;
    badVersionOrFlag[offset] = 2;
    expectRunnerFetchBatchDecodeFailure(badVersionOrFlag);
  }
  for (size_t offset : {size_t{0}, size_t{6}, size_t{10}, size_t{14}}) {
    auto excessiveLength = original;
    writeRunnerFetchU32(&excessiveLength, offset, UINT32_MAX);
    expectRunnerFetchBatchDecodeFailure(excessiveLength);
  }
  auto missingData = original;
  missingData[5] = 0;
  expectRunnerFetchBatchDecodeFailure(missingData);
  std::vector<uint8_t> emptyBatch(18, 0);
  writeRunnerFetchU32(&emptyBatch, 0, 14);
  emptyBatch[4] = STREAM_RUNNER_FETCH_BATCH_VERSION;
  expectRunnerFetchBatchDecodeFailure(emptyBatch);
  auto secondData = original;
  secondData.insert(secondData.end(), original.begin(), original.end());
  writeRunnerFetchU32(&secondData, 0, secondData.size() - sizeof(uint32_t));
  expectRunnerFetchBatchDecodeFailure(secondData);
  auto invalidBlock = original;
  writeI32(&invalidBlock, blockStart + 2 * sizeof(int32_t), INT32_MAX);
  expectRunnerFetchBatchDecodeFailure(invalidBlock);
  auto           wrongBlockGroup = original;
  const uint64_t wrongGid = 202;
  memcpy(wrongBlockGroup.data() + blockStart + 5 * sizeof(int32_t), &wrongGid, sizeof(wrongGid));
  expectRunnerFetchBatchDecodeFailure(wrongBlockGroup);
  auto truncatedBlock = original;
  truncatedBlock.erase(truncatedBlock.begin() + frameStart - 1);
  writeRunnerFetchU32(&truncatedBlock, 0, truncatedBlock.size() - sizeof(uint32_t));
  writeRunnerFetchU32(&truncatedBlock, 6, frameStart - blockStart - 1);
  expectRunnerFetchBatchDecodeFailure(truncatedBlock);
  auto eogBeforeData = original;
  eogBeforeData[frameStart + 5] = STREAM_RUNNER_FETCH_EOG;
  expectRunnerFetchBatchDecodeFailure(eogBeforeData);
  auto invalidLineage = original;
  writeRunnerFetchU64(&invalidLineage, frameStart + 18 + sizeof(uint64_t), packLineage(7, 4));
  expectRunnerFetchBatchDecodeFailure(invalidLineage);
  auto wrongFirstEog = original;
  writeRunnerFetchU64(&wrongFirstEog, eogStart, 202);
  expectRunnerFetchBatchDecodeFailure(wrongFirstEog);
  auto duplicateEog = original;
  writeRunnerFetchU64(&duplicateEog, eogStart + sizeof(uint64_t), 101);
  expectRunnerFetchBatchDecodeFailure(duplicateEog);
  auto zeroEog = original;
  writeRunnerFetchU64(&zeroEog, eogStart + sizeof(uint64_t), 0);
  expectRunnerFetchBatchDecodeFailure(zeroEog);
}

TEST(StreamRunnerFetchBatchTest, DecodeReplacesOwnedStateAndClearsOnFailure) {
  RunnerFetchBatchGuard source;
  fillRunnerFetchBatch(&source.batch, true, {101, 202});
  auto                  bytes = serializeRunnerFetchBatch(source.batch);
  RunnerFetchBatchGuard decoded;
  fillRunnerFetchBatch(&decoded.batch, true, {101});
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
  ASSERT_EQ(2, taosArrayGetSize(decoded.batch.pEogGids));
  writeRunnerFetchU64(&bytes, bytes.size() - sizeof(uint64_t), 101);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
  expectEmptyRunnerFetchBatch(decoded.batch);
  tDestroySStreamRunnerFetchBatch(&decoded.batch);
  expectEmptyRunnerFetchBatch(decoded.batch);
}

TEST(StreamRunnerFetchBatchTest, AllocationFailureReleasesPartiallyDecodedState) {
#ifdef WINDOWS
  // The failure is injected by patching the machine code of taosMemMalloc
  // (cppstub), and the Windows release build compiles with /GL and links with
  // LTCG, which folds this small allocator wrapper into its callers so the
  // patched entry point is never reached. macOS needs an explicit post-build
  // step for the same reason, Windows has none.
  GTEST_SKIP() << "cppstub patching of taosMemMalloc is unreliable in the Windows release build";
#endif
  for (bool withData : {false, true}) {
    RunnerFetchBatchGuard source;
    fillRunnerFetchBatch(&source.batch, withData, {101, 202});
    auto                  bytes = serializeRunnerFetchBatch(source.batch);
    RunnerFetchBatchGuard decoded;
    fillRunnerFetchBatch(&decoded.batch, true, {101});
    {
      MatchingAllocationFailureGuard failure(sizeof(SArray));
      EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
                tDeserializeSStreamRunnerFetchBatch(bytes.data(), bytes.size(), &decoded.batch));
      EXPECT_EQ(1, gContextFrameMatchingAllocations);
    }
    expectEmptyRunnerFetchBatch(decoded.batch);
  }
}

TEST(StreamRunnerFetchBatchTest, CalcRequestDestructorReleasesBatchOutput) {
  TriggerCalcRequestGuard request;
  fillRunnerFetchBatch(&request.request.outBatch, true, {101, 202});
  tDestroySTriggerCalcRequest(&request.request);
  expectEmptyRunnerFetchBatch(request.request.outBatch);
}

}  // namespace

namespace {

class StreamWalDataCodecTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&source_));
    SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0);
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(source_, &column));
    ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(source_, 2));
    auto* data = static_cast<SColumnInfoData*>(taosArrayGet(source_->pDataBlock, 0));
    for (int64_t row = 0; row < 2; ++row) {
      ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(data, row, reinterpret_cast<const char*>(&row), false));
    }
    source_->info.rows = 2;
    index_ = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(nullptr, index_);
    slices_ = taosArrayInit(1, sizeof(int64_t) * 3);
    ASSERT_NE(nullptr, slices_);
    decoded_ = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
    ASSERT_NE(nullptr, decoded_);
  }

  void TearDown() override {
    blockDataDestroy(source_);
    blockDataDestroy(decoded_);
    tSimpleHashCleanup(index_);
    taosArrayDestroy(slices_);
  }

  std::vector<char> encode(int32_t start = 0, int32_t rows = 2) {
    SStreamWalDataSlice slice = {};
    slice.uid = 1;
    slice.gId = 2;
    slice.startRowIdx = start;
    slice.numRows = rows;
    EXPECT_EQ(TSDB_CODE_SUCCESS, tSimpleHashPut(index_, &slice.uid, sizeof(slice.uid), &slice, sizeof(slice)));
    SSTriggerWalNewRsp response = {};
    response.indexHash = index_;
    response.dataBlock = source_;
    response.ver = 3;
    response.verTime = 4;
    int32_t            len = tSerializeSStreamWalDataResponse(nullptr, 0, &response);
    EXPECT_GT(len, 0);
    if (len <= 0) return {};
    std::vector<char> result(len);
    EXPECT_EQ(len, tSerializeSStreamWalDataResponse(result.data(), len, &response));
    return result;
  }

  int32_t decode(std::vector<char>& bytes, SArray* slices) {
    SSTriggerWalNewRsp response = {};
    response.dataBlock = decoded_;
    return tDeserializeSStreamWalDataResponse(bytes.data(), bytes.size(), &response, slices);
  }

  SSDataBlock* source_ = nullptr;
  SSDataBlock* decoded_ = nullptr;
  SSHashObj*   index_ = nullptr;
  SArray*      slices_ = nullptr;
};

TEST_F(StreamWalDataCodecTest, SliceRangesStayInsideDecodedRows) {
  for (const auto& range : std::vector<std::pair<int32_t, int32_t>>{{-1, 1}, {0, -1}, {3, 0}, {1, 2}, {1, INT32_MAX}}) {
    SCOPED_TRACE(::testing::Message() << range.first << "/" << range.second);
    auto bytes = encode(range.first, range.second);
    EXPECT_NE(TSDB_CODE_SUCCESS, decode(bytes, slices_));
  }
  for (const auto& range : std::vector<std::pair<int32_t, int32_t>>{{0, 2}, {1, 1}, {2, 0}}) {
    auto bytes = encode(range.first, range.second);
    ASSERT_EQ(TSDB_CODE_SUCCESS, decode(bytes, slices_));
    ASSERT_EQ(1, taosArrayGetSize(slices_));
    auto* slice = static_cast<int64_t*>(taosArrayGet(slices_, 0));
    EXPECT_EQ(range.first, slice[2] >> 32);
    EXPECT_EQ(range.first + range.second, static_cast<int32_t>(slice[2]));
  }
}

TEST_F(StreamWalDataCodecTest, SliceCountIsBoundedBeforeAllocation) {
  const size_t countOffset = sizeof(int32_t) + sizeof(int8_t) + blockGetEncodeSize(source_);
  for (int32_t count : {-1, INT32_MAX, 2}) {
    auto bytes = encode();
    ASSERT_LT(countOffset + sizeof(count), bytes.size());
    memcpy(bytes.data() + countOffset, &count, sizeof(count));
    EXPECT_NE(TSDB_CODE_SUCCESS, decode(bytes, slices_));
  }
}

TEST_F(StreamWalDataCodecTest, DataRequiresSliceOutputContainer) {
  auto bytes = encode();
  EXPECT_NE(TSDB_CODE_SUCCESS, decode(bytes, nullptr));
}

TEST_F(StreamWalDataCodecTest, TruncatedBlocksAndTailUseActualFrameLength) {
  const auto complete = encode();
  for (size_t len = sizeof(int32_t) + 1; len < complete.size(); ++len) {
    SCOPED_TRACE(len);
    std::vector<char> bytes(complete.begin(), complete.begin() + len);
    const int32_t     frameLen = len - sizeof(int32_t);
    memcpy(bytes.data(), &frameLen, sizeof(frameLen));
    EXPECT_NE(TSDB_CODE_SUCCESS, decode(bytes, slices_));
  }
  auto bytes = complete;
  EXPECT_EQ(TSDB_CODE_SUCCESS, decode(bytes, slices_));
}

TEST_F(StreamWalDataCodecTest, EmptyResponseDoesNotRequireSchemaOrSlices) {
  SSTriggerWalNewRsp source = {};
  source.ver = 3;
  source.verTime = 4;
  const int32_t      len = tSerializeSStreamWalDataResponse(nullptr, 0, &source);
  ASSERT_GT(len, 0);
  std::vector<char> bytes(len);
  ASSERT_EQ(len, tSerializeSStreamWalDataResponse(bytes.data(), len, &source));
  SSTriggerWalNewRsp response = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSStreamWalDataResponse(bytes.data(), len, &response, nullptr));
  EXPECT_EQ(3, response.ver);
  EXPECT_EQ(4, response.verTime);
}

}  // namespace
