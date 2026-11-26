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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <vector>

#include "nodes.h"
#include "planner.h"
#include "plannodes.h"
#include "tconv.h"
#include "tmsg.h"
#include "types.h"

namespace {

std::vector<uint8_t> serializeSubQuery(SSubQueryMsg* pMsg) {
  int32_t length = tSerializeSSubQueryMsg(nullptr, 0, pMsg);
  if (length <= 0) return {};
  std::vector<uint8_t> wire(length);
  if (length != tSerializeSSubQueryMsg(wire.data(), length, pMsg)) return {};
  return wire;
}

}  // namespace

TEST(SubQueryMsgTest, OrdinaryMessageAndIndexedProducerIdentity) {
  char msg = 'x';
  SSubQueryMsg source = {0};
  source.queryId = 101;
  source.msg = &msg;
  source.msgLen = 1;

  std::vector<uint8_t> oldWire = serializeSubQuery(&source);
  ASSERT_FALSE(oldWire.empty());
  SSubQueryMsg oldDecoded = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSSubQueryMsg(oldWire.data(), oldWire.size(), &oldDecoded));
  EXPECT_EQ(0U, oldDecoded.correlation.rootExecutionId);
  EXPECT_EQ(nullptr, oldDecoded.subEndPoints);
  tFreeSSubQueryMsg(&oldDecoded);

  SDownstreamSourceNode endpoint = {};
  endpoint.producerQueryId = 202;
  source.allowFollowerRead = true;
  source.subEndPoints = taosArrayInit(1, sizeof(SDownstreamSourceNode*));
  ASSERT_NE(nullptr, source.subEndPoints);
  SDownstreamSourceNode* pEndpoint = &endpoint;
  ASSERT_NE(nullptr, taosArrayPush(source.subEndPoints, &pEndpoint));
  std::vector<uint8_t> newWire = serializeSubQuery(&source);
  taosArrayDestroy(source.subEndPoints);
  ASSERT_FALSE(newWire.empty());
  SSubQueryMsg newDecoded = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSSubQueryMsg(newWire.data(), newWire.size(), &newDecoded));
  ASSERT_EQ(1, taosArrayGetSize(newDecoded.subEndPoints));
  EXPECT_EQ(202U, ((SDownstreamSourceNode*)taosArrayGetP(newDecoded.subEndPoints, 0))->producerQueryId);
  EXPECT_EQ(QUERY_NODE_DOWNSTREAM_SOURCE,
            ((SDownstreamSourceNode*)taosArrayGetP(newDecoded.subEndPoints, 0))->type);
  EXPECT_TRUE(newDecoded.allowFollowerRead);
  SSubQueryMsg peek = {};
  EXPECT_EQ(0, tDeserializeSSubQueryMsgExt(newWire.data(), newWire.size(), &peek, TSDB_SUBQ_DEC_SKIP_ALL));
  EXPECT_TRUE(peek.allowFollowerRead);
  tFreeSSubQueryMsg(&peek);
  EXPECT_EQ(0U, newDecoded.correlation.rootExecutionId);
  tFreeSSubQueryMsg(&newDecoded);
}

TEST(SubQueryMsgTest, CorrelationNodeCandidatesRoundTrip) {
  char msg = 'x';
  char templates = 't';
  SSubQueryMsg source = {0};
  source.queryId = 101;
  source.msg = &msg;
  source.msgLen = 1;
  source.pCorrelationTemplates = &templates;
  source.correlationTemplatesLen = 1;
  source.correlation.rootExecutionId = 1;
  source.correlation.rootQueryId = 101;
  source.correlation.executionGeneration = 1;
  source.correlation.templateId = -1;
  source.pCorrelationNodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));
  ASSERT_NE(nullptr, source.pCorrelationNodeList);
  SQueryNodeLoad node = {};
  node.addr.nodeId = 7;
  node.addr.epSet.numOfEps = 1;
  node.addr.epSet.eps[0].port = 26330;
  strcpy(node.addr.epSet.eps[0].fqdn, "localhost");
  ASSERT_NE(nullptr, taosArrayPush(source.pCorrelationNodeList, &node));

  std::vector<uint8_t> wire = serializeSubQuery(&source);
  taosArrayDestroy(source.pCorrelationNodeList);
  ASSERT_FALSE(wire.empty());
  SSubQueryMsg decoded = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSSubQueryMsg(wire.data(), wire.size(), &decoded));
  ASSERT_EQ(1, taosArrayGetSize(decoded.pCorrelationNodeList));
  const SQueryNodeLoad* actual = (const SQueryNodeLoad*)taosArrayGet(decoded.pCorrelationNodeList, 0);
  EXPECT_EQ(7, actual->addr.nodeId);
  EXPECT_EQ(26330, actual->addr.epSet.eps[0].port);
  EXPECT_STREQ("localhost", actual->addr.epSet.eps[0].fqdn);
  tFreeSSubQueryMsg(&decoded);
}

TEST(SubQueryMsgTest, TypedParametersOwnEmbeddedNulAndRejectInvalidPayload) {
  char msg = 'x';
  uint8_t text[VARSTR_HEADER_SIZE + 3] = {0};
  varDataSetLen(text, 3);
  memcpy(varDataVal(text), "a\0b", 3);
  int32_t number = 42;
  SCorrelationParamDef defs[3] = {};
  SCorrelationWireValue values[3] = {};
  for (int32_t i = 0; i < 3; ++i) {
    defs[i].paramId = i;
    defs[i].dataType.type = i == 1 ? TSDB_DATA_TYPE_VARCHAR : TSDB_DATA_TYPE_INT;
    defs[i].dataType.bytes = i == 1 ? sizeof(text) : sizeof(number);
    values[i].paramId = i;
    values[i].dataType = defs[i].dataType;
  }
  values[0].dataLen = sizeof(number);
  values[0].pData = (uint8_t*)&number;
  values[1].dataLen = sizeof(text);
  values[1].pData = text;
  values[2].isNull = true;
  SSubQueryMsg source = {0};
  source.queryId = 303;
  source.txnId = 404;
  source.firstDayOfWeek = 1;
  source.msg = &msg;
  source.msgLen = 1;
  source.correlation.rootExecutionId = 1;
  source.correlation.rootQueryId = 101;
  source.correlation.executionGeneration = 1;
  source.correlation.templateId = 0;
  strcpy(source.correlation.user, "root");
  strcpy(source.correlation.dbFName, "1.db");
  strcpy(source.correlation.timezoneName, "UTC");
  strcpy(source.correlation.charsetName, "UTF-8");
  source.pCorrelationParamDefs = taosArrayInit(3, sizeof(SCorrelationParamDef));
  source.pCorrelationParamValues = taosArrayInit(3, sizeof(SCorrelationWireValue));
  ASSERT_NE(nullptr, source.pCorrelationParamDefs);
  ASSERT_NE(nullptr, source.pCorrelationParamValues);
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, taosArrayPush(source.pCorrelationParamDefs, &defs[i]));
    ASSERT_NE(nullptr, taosArrayPush(source.pCorrelationParamValues, &values[i]));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS, tValidateCorrelationParams(source.pCorrelationParamDefs,
                                                            source.pCorrelationParamValues));
  std::vector<uint8_t> wire = serializeSubQuery(&source);
  std::vector<uint8_t> damagedWire = wire;
  damagedWire.push_back(0x42);
  taosArrayDestroy(source.pCorrelationParamDefs);
  taosArrayDestroy(source.pCorrelationParamValues);
  ASSERT_FALSE(wire.empty());

  SSubQueryMsg decoded = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSSubQueryMsg(wire.data(), wire.size(), &decoded));
  SSubQueryMsg skipped = {};
  ASSERT_EQ(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &skipped, TSDB_SUBQ_DEC_SKIP_ALL));
  EXPECT_EQ(404, skipped.txnId);
  EXPECT_EQ(nullptr, skipped.pCorrelationParamDefs);
  EXPECT_EQ(nullptr, skipped.pCorrelationParamValues);
  tFreeSSubQueryMsg(&skipped);
  wire.clear();
  wire.shrink_to_fit();
  EXPECT_EQ(404, decoded.txnId);
  EXPECT_EQ(1, decoded.firstDayOfWeek);
  EXPECT_STREQ("UTC", decoded.correlation.timezoneName);
  EXPECT_STREQ("UTF-8", decoded.correlation.charsetName);
  ASSERT_EQ(3, taosArrayGetSize(decoded.pCorrelationParamValues));
  const SCorrelationWireValue* pText = (const SCorrelationWireValue*)taosArrayGet(decoded.pCorrelationParamValues, 1);
  ASSERT_NE(nullptr, pText->pData);
  EXPECT_EQ(0, memcmp(pText->pData, text, sizeof(text)));
  EXPECT_EQ(0, pText->pData[VARSTR_HEADER_SIZE + 1]);
  EXPECT_TRUE(((SCorrelationWireValue*)taosArrayGet(decoded.pCorrelationParamValues, 2))->isNull);
  tFreeSSubQueryMsg(&decoded);

  SSubQueryMsg damaged = {0};
  EXPECT_NE(TSDB_CODE_SUCCESS,
            tDeserializeSSubQueryMsg(damagedWire.data(), damagedWire.size(), &damaged));
  values[1].paramId = 0;
  SArray* pDefs = taosArrayInit(2, sizeof(SCorrelationParamDef));
  SArray* pValues = taosArrayInit(2, sizeof(SCorrelationWireValue));
  ASSERT_NE(nullptr, taosArrayPush(pDefs, &defs[0]));
  ASSERT_NE(nullptr, taosArrayPush(pDefs, &defs[1]));
  ASSERT_NE(nullptr, taosArrayPush(pValues, &values[0]));
  ASSERT_NE(nullptr, taosArrayPush(pValues, &values[1]));
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateCorrelationParams(pDefs, pValues));
  auto* pFirstDef = (SCorrelationParamDef*)taosArrayGet(pDefs, 0);
  auto* pFirstValue = (SCorrelationWireValue*)taosArrayGet(pValues, 0);
  auto* pSecondValue = (SCorrelationWireValue*)taosArrayGet(pValues, 1);
  pSecondValue->paramId = 1;
  pFirstValue->dataType.type = TSDB_DATA_TYPE_FLOAT;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateCorrelationParams(pDefs, pValues));
  pFirstValue->dataType.type = TSDB_DATA_TYPE_INT;
  pFirstDef->dataType.bytes = 8;
  pFirstValue->dataType.bytes = 8;
  pFirstValue->dataLen = 8;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateCorrelationParams(pDefs, pValues));
  pFirstDef->dataType.type = TSDB_DATA_TYPE_DECIMAL64;
  pFirstDef->dataType.precision = TSDB_DECIMAL64_MAX_PRECISION + 1;
  pFirstValue->dataType = pFirstDef->dataType;
  EXPECT_NE(TSDB_CODE_SUCCESS, tValidateCorrelationParams(pDefs, pValues));
  taosArrayDestroy(pDefs);
  taosArrayDestroy(pValues);
}

TEST(SubQueryMsgTest, SkipUnalignedVariableWidthCorrelationValues) {
  // A payload starts immediately after its length prefix, at any byte offset.
  alignas(BlobDataLenT) uint8_t storage[1 + BLOBSTR_HEADER_SIZE + 1] = {};
  uint8_t* data = storage + 1;
  for (int8_t type : {TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_BLOB}) {
    SCorrelationParamDef def = {};
    def.dataType.type = type;
    if (type == TSDB_DATA_TYPE_VARCHAR) {
      VarDataLenT len = 1;
      memcpy(data, &len, sizeof(len));
      def.dataType.bytes = sizeof(len) + 1;
    } else {
      BlobDataLenT len = 1;
      memcpy(data, &len, sizeof(len));
      def.dataType.bytes = sizeof(len) + 1;
    }
    data[def.dataType.bytes - 1] = 'x';
    SCorrelationWireValue value = {};
    value.dataType = def.dataType;
    value.dataLen = def.dataType.bytes;
    value.pData = data;
    SSubQueryMsg source = {};
    source.correlation.rootExecutionId = 1;
    source.correlation.rootQueryId = 1;
    source.correlation.executionGeneration = 1;
    source.pCorrelationParamDefs = taosArrayInit(1, sizeof(def));
    source.pCorrelationParamValues = taosArrayInit(1, sizeof(value));
    ASSERT_NE(nullptr, taosArrayPush(source.pCorrelationParamDefs, &def));
    ASSERT_NE(nullptr, taosArrayPush(source.pCorrelationParamValues, &value));
    std::vector<uint8_t> wire = serializeSubQuery(&source);
    taosArrayDestroy(source.pCorrelationParamDefs);
    taosArrayDestroy(source.pCorrelationParamValues);
    ASSERT_FALSE(wire.empty());
    SSubQueryMsg skipped = {};
    EXPECT_EQ(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &skipped, TSDB_SUBQ_DEC_SKIP_CORRELATION));
    EXPECT_EQ(nullptr, skipped.pCorrelationParamValues);
    tFreeSSubQueryMsg(&skipped);
  }
}

TEST(SubQueryMsgTest, BinaryFieldLengthFitsUint32) {
  for (uint64_t length : {uint64_t{UINT32_MAX}, uint64_t{UINT32_MAX} + 1, uint64_t{1} << 35}) {
    uint8_t buffer[10] = {};
    SEncoder encoder = {};
    tEncoderInit(&encoder, buffer, sizeof(buffer));
    ASSERT_EQ(0, tEncodeU64v(&encoder, length));
    SDecoder decoder = {};
    tDecoderInit(&decoder, buffer, encoder.pos);
    uint32_t decoded = 0;
    if (length == UINT32_MAX) {
      EXPECT_EQ(0, tDecodeU32v(&decoder, &decoded));
      EXPECT_EQ(length, decoded);
    } else {
      EXPECT_EQ(TSDB_CODE_OUT_OF_RANGE, tDecodeU32v(&decoder, &decoded));
    }
    tDecoderClear(&decoder);
    tEncoderClear(&encoder);
  }
}

TEST(SubQueryMsgTest, SelectiveDecodePreservesRequestedFields) {
  char sql[] = "select c1 from meters";
  char payload[] = "physical plan";
  char wireTemplate[] = "template";
  char timeInit[] = "init";
  SSubQueryMsg msg = {};
  msg.header.vgId = 73;
  msg.sql = sql;
  msg.sqlLen = strlen(sql);
  msg.msg = payload;
  msg.msgLen = sizeof(payload);
  msg.clientId = 91;
  msg.queryId = 101;
  msg.txnId = 103;
  msg.allowFollowerRead = true;
  msg.correlation.rootQueryId = 107;
  msg.correlation.rootExecutionId = 109;
  msg.correlation.executionGeneration = 1;
  msg.pCorrelationTemplates = wireTemplate;
  msg.correlationTemplatesLen = sizeof(wireTemplate);
  msg.pCorrelationTimeInit = timeInit;
  msg.correlationTimeInitLen = sizeof(timeInit);
  SQueryNodeLoad node = {};
  node.addr.nodeId = 127;
  msg.pCorrelationNodeList = taosArrayInit(1, sizeof(node));
  ASSERT_NE(nullptr, taosArrayPush(msg.pCorrelationNodeList, &node));
  SDownstreamSourceNode source = {};
  source.producerQueryId = 113;
  SDownstreamSourceNode* endpoint = &source;
  msg.subEndPoints = taosArrayInit(1, sizeof(endpoint));
  ASSERT_NE(nullptr, taosArrayPush(msg.subEndPoints, &endpoint));
  auto wire = serializeSubQuery(&msg);
  taosArrayDestroy(msg.subEndPoints);
  taosArrayDestroy(msg.pCorrelationNodeList);
  ASSERT_FALSE(wire.empty());

  const auto originalWire = wire;
  for (uint32_t flags = 0; flags <= TSDB_SUBQ_DEC_SKIP_ALL; ++flags) {
    SCOPED_TRACE(flags);
    SSubQueryMsg out = {};
    ASSERT_EQ(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &out, flags));
    EXPECT_EQ(originalWire, wire);
    EXPECT_EQ(msg.clientId, out.clientId);
    EXPECT_EQ(msg.txnId, out.txnId);
    EXPECT_TRUE(out.allowFollowerRead);
    if (flags & TSDB_SUBQ_DEC_SKIP_SQL) {
      EXPECT_EQ(nullptr, out.sql);
      EXPECT_EQ(0U, out.sqlLen);
    } else {
      EXPECT_STREQ(sql, out.sql);
      EXPECT_EQ(msg.sqlLen, out.sqlLen);
    }
    if (flags & TSDB_SUBQ_DEC_SKIP_MSG) {
      EXPECT_EQ(nullptr, out.msg);
      EXPECT_EQ(0U, out.msgLen);
    } else {
      ASSERT_EQ(sizeof(payload), out.msgLen);
      EXPECT_EQ(0, memcmp(payload, out.msg, sizeof(payload)));
    }
    if (flags & TSDB_SUBQ_DEC_SKIP_ENDPOINTS) EXPECT_EQ(nullptr, out.subEndPoints);
    else {
      ASSERT_EQ(1, taosArrayGetSize(out.subEndPoints));
      EXPECT_EQ(113U, ((SDownstreamSourceNode*)taosArrayGetP(out.subEndPoints, 0))->producerQueryId);
    }
    if (flags & TSDB_SUBQ_DEC_SKIP_CORRELATION) {
      EXPECT_EQ(0U, out.correlation.rootQueryId);
      EXPECT_EQ(nullptr, out.pCorrelationTemplates);
      EXPECT_EQ(nullptr, out.pCorrelationTimeInit);
      EXPECT_EQ(0U, out.correlationTimeInitLen);
      EXPECT_EQ(nullptr, out.pCorrelationNodeList);
    } else {
      EXPECT_EQ(107U, out.correlation.rootQueryId);
      ASSERT_EQ(sizeof(wireTemplate), out.correlationTemplatesLen);
      EXPECT_EQ(0, memcmp(wireTemplate, out.pCorrelationTemplates, sizeof(wireTemplate)));
      ASSERT_EQ(sizeof(timeInit), out.correlationTimeInitLen);
      EXPECT_EQ(0, memcmp(timeInit, out.pCorrelationTimeInit, sizeof(timeInit)));
      ASSERT_EQ(1, taosArrayGetSize(out.pCorrelationNodeList));
      auto* candidate = static_cast<SQueryNodeLoad*>(taosArrayGet(out.pCorrelationNodeList, 0));
      EXPECT_EQ(node.addr.nodeId, candidate->addr.nodeId);
    }
    tFreeSSubQueryMsg(&out);
    SSubQueryMsg truncated = {};
    EXPECT_NE(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size() - 1, &truncated, flags));
  }
}

TEST(SubQueryMsgTest, SkipsLargeCorrelationParameterList) {
  SSubQueryMsg msg = {};
  msg.correlation.rootQueryId = 71;
  msg.correlation.rootExecutionId = 73;
  msg.correlation.executionGeneration = 1;
  msg.allowFollowerRead = true;
  constexpr int32_t count = 128;
  msg.pCorrelationParamDefs = taosArrayInit(count, sizeof(SCorrelationParamDef));
  msg.pCorrelationParamValues = taosArrayInit(count, sizeof(SCorrelationWireValue));
  ASSERT_NE(nullptr, msg.pCorrelationParamDefs);
  ASSERT_NE(nullptr, msg.pCorrelationParamValues);
  for (int32_t i = 0; i < count; ++i) {
    SCorrelationParamDef def = {};
    def.paramId = i;
    def.dataType.type = TSDB_DATA_TYPE_INT;
    def.dataType.bytes = sizeof(int32_t);
    SCorrelationWireValue value = {};
    value.paramId = i;
    value.dataType = def.dataType;
    value.isNull = true;
    ASSERT_NE(nullptr, taosArrayPush(msg.pCorrelationParamDefs, &def));
    ASSERT_NE(nullptr, taosArrayPush(msg.pCorrelationParamValues, &value));
  }
  auto wire = serializeSubQuery(&msg);
  tFreeSSubQueryMsg(&msg);
  ASSERT_FALSE(wire.empty());
  SSubQueryMsg decoded = {};
  ASSERT_EQ(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &decoded, TSDB_SUBQ_DEC_SKIP_CORRELATION));
  EXPECT_EQ(nullptr, decoded.pCorrelationParamDefs);
  EXPECT_EQ(nullptr, decoded.pCorrelationParamValues);
  EXPECT_TRUE(decoded.allowFollowerRead);
  tFreeSSubQueryMsg(&decoded);
}

TEST(SubQueryMsgTest, SkippedProducerOverridesAcceptUnorderedUniqueIndices) {
  for (int32_t count : {256, 257}) {
    SCOPED_TRACE(count);
    std::vector<SDownstreamSourceNode> endpoints(count);
    endpoints.front().producerQueryId = 11;
    endpoints.back().producerQueryId = 22;
    SSubQueryMsg msg = {};
    msg.subEndPoints = taosArrayInit(count, sizeof(SDownstreamSourceNode*));
    ASSERT_NE(nullptr, msg.subEndPoints);
    for (auto& endpoint : endpoints) {
      auto* ptr = &endpoint;
      ASSERT_NE(nullptr, taosArrayPush(msg.subEndPoints, &ptr));
    }
    auto wire = serializeSubQuery(&msg);
    taosArrayDestroy(msg.subEndPoints);
    // With no optional fields after the producer list, its two records end the frame.
    constexpr size_t recordBytes = sizeof(int32_t) + sizeof(uint64_t);
    ASSERT_GT(wire.size(), 2 * recordBytes);
    auto last = wire.end() - recordBytes;
    auto first = last - recordBytes;
    std::swap_ranges(first, last, last);
    SSubQueryMsg full = {};
    ASSERT_EQ(0, tDeserializeSSubQueryMsg(wire.data(), wire.size(), &full));
    EXPECT_EQ(11U, static_cast<SDownstreamSourceNode*>(taosArrayGetP(full.subEndPoints, 0))->producerQueryId);
    EXPECT_EQ(22U, static_cast<SDownstreamSourceNode*>(taosArrayGetP(full.subEndPoints, count - 1))->producerQueryId);
    tFreeSSubQueryMsg(&full);
    SSubQueryMsg skipped = {};
    ASSERT_EQ(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &skipped, TSDB_SUBQ_DEC_SKIP_ENDPOINTS));
    EXPECT_EQ(nullptr, skipped.subEndPoints);
    tFreeSSubQueryMsg(&skipped);
    std::copy_n(first, sizeof(int32_t), last);
    EXPECT_EQ(TSDB_CODE_INVALID_MSG,
              tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &skipped, TSDB_SUBQ_DEC_SKIP_ENDPOINTS));
  }
}

TEST(SubQueryMsgTest, RejectsIncompleteSqlInFullAndSelectiveDecodes) {
  char sql[] = "select unique_sql_marker";
  SSubQueryMsg msg = {};
  msg.sql = sql;
  msg.sqlLen = strlen(sql);
  auto wire = serializeSubQuery(&msg);
  auto start = std::search(wire.begin(), wire.end(), sql, sql + sizeof(sql));
  ASSERT_NE(wire.end(), start);
  auto terminator = start + msg.sqlLen;
  *terminator = 'X';
  for (uint32_t flags : {0U, TSDB_SUBQ_DEC_SKIP_ALL}) {
    SSubQueryMsg decoded = {};
    EXPECT_EQ(TSDB_CODE_INVALID_MSG, tDeserializeSSubQueryMsgExt(wire.data(), -1, &decoded, flags));
    EXPECT_EQ(TSDB_CODE_INVALID_MSG, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &decoded, flags));
  }

  // Keep the outer framing consistent so decoding reaches the incomplete SQL field.
  wire.resize(terminator - wire.begin());
  int32_t frameLen = static_cast<int32_t>(wire.size() - sizeof(SMsgHead) - sizeof(int32_t));
  memcpy(wire.data() + sizeof(SMsgHead), &frameLen, sizeof(frameLen));
  auto headerLen = htonl(static_cast<uint32_t>(wire.size()));
  memcpy(wire.data() + offsetof(SMsgHead, contLen), &headerLen, sizeof(headerLen));
  for (uint32_t flags : {0U, TSDB_SUBQ_DEC_SKIP_ALL}) {
    SSubQueryMsg decoded = {};
    EXPECT_NE(0, tDeserializeSSubQueryMsgExt(wire.data(), wire.size(), &decoded, flags));
  }
}

TEST(SubQueryMsgTest, SharedBodyProducesTheSameTargetPlan) {
  SSubplan plan = {};
  plan.type = QUERY_NODE_PHYSICAL_SUBPLAN;
  plan.id.queryId = 17;
  plan.id.groupId = 23;
  plan.subplanType = SUBPLAN_TYPE_SCAN;
  plan.msgType = TDMT_SCH_QUERY;
  plan.inheritedMultiGroupJoin = true;
  plan.delayFreeTask = true;
  plan.localBlockExchange = true;
  plan.execOnChildNode = true;
  plan.subqueryDepsKnown = true;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, (SNode**)&plan.pNode));
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&plan.pDataSink));
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_COLUMN, &plan.pTagCond));
  reinterpret_cast<SColumnNode*>(plan.pTagCond)->colId = 17;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_COLUMN, &plan.pTagIndexCond));
  reinterpret_cast<SColumnNode*>(plan.pTagIndexCond)->colId = 31;
  SDownstreamSourceNode endpoint = {};
  endpoint.type = QUERY_NODE_DOWNSTREAM_SOURCE;
  endpoint.taskId = 47;
  ASSERT_EQ(0, nodesListMakeAppend(&plan.pSubQ, reinterpret_cast<SNode*>(&endpoint)));
  plan.pSubqueryDeps = taosArrayInit(2, sizeof(int32_t));
  ASSERT_NE(nullptr, plan.pSubqueryDeps);
  for (int32_t dep : {2, 5}) ASSERT_NE(nullptr, taosArrayPush(plan.pSubqueryDeps, &dep));
  for (bool wrapped : {false, true}) {
    plan.requiresAncestorContext = wrapped;
    char* body = nullptr;
    int32_t bodyLen = 0;
    ASSERT_EQ(0, qSubPlanBuildInvariantBody(&plan, &body, &bodyLen));
    for (int32_t i = 1; i <= 3; ++i) {
      SSubplanTarget target = {};
      target.subplanId = i;
      target.execNode.nodeId = 100 + i;
      target.execNode.epSet.numOfEps = 1;
      strcpy(target.execNode.epSet.eps[0].fqdn, "localhost");
      target.execNode.epSet.eps[0].port = 6000 + i;
      target.execNodeStat.tableNum = 3 * i;
      SSubplan expected = plan;
      expected.id.subplanId = i;
      expected.execNode = target.execNode;
      expected.execNodeStat = target.execNodeStat;
      char *actualWire = nullptr, *expectedWire = nullptr;
      int32_t actualLen = 0, expectedLen = 0;
      ASSERT_EQ(0, qSubPlanAssembleMsg(&plan, body, bodyLen, &target, &actualWire, &actualLen));
      ASSERT_EQ(0, qSubPlanToMsg(&expected, &expectedWire, &expectedLen));
      ASSERT_EQ(expectedLen, actualLen);
      SSubplan* actual = nullptr;
      ASSERT_EQ(0, qMsgToSubplan(actualWire, actualLen, &actual));
      EXPECT_EQ(i, actual->id.subplanId);
      EXPECT_EQ(target.execNode.nodeId, actual->execNode.nodeId);
      EXPECT_EQ(target.execNodeStat.tableNum, actual->execNodeStat.tableNum);
      EXPECT_EQ(wrapped, actual->requiresAncestorContext);
      ASSERT_NE(nullptr, actual->pNode);
      ASSERT_NE(nullptr, actual->pDataSink);
      EXPECT_EQ(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, nodeType(actual->pNode));
      EXPECT_EQ(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, nodeType(actual->pDataSink));
      ASSERT_NE(nullptr, actual->pTagCond);
      ASSERT_NE(nullptr, actual->pTagIndexCond);
      EXPECT_EQ(17, reinterpret_cast<SColumnNode*>(actual->pTagCond)->colId);
      EXPECT_EQ(31, reinterpret_cast<SColumnNode*>(actual->pTagIndexCond)->colId);
      ASSERT_EQ(1, LIST_LENGTH(actual->pSubQ));
      EXPECT_EQ(47U, reinterpret_cast<SDownstreamSourceNode*>(nodesListGetNode(actual->pSubQ, 0))->taskId);
      ASSERT_EQ(2, taosArrayGetSize(actual->pSubqueryDeps));
      EXPECT_EQ(2, *static_cast<int32_t*>(taosArrayGet(actual->pSubqueryDeps, 0)));
      EXPECT_EQ(5, *static_cast<int32_t*>(taosArrayGet(actual->pSubqueryDeps, 1)));
      char* roundTrip = nullptr;
      int32_t roundTripLen = 0;
      ASSERT_EQ(0, qSubPlanToMsg(actual, &roundTrip, &roundTripLen));
      ASSERT_EQ(expectedLen, roundTripLen);
      EXPECT_EQ(0, memcmp(expectedWire, roundTrip, expectedLen));
      taosMemoryFree(roundTrip);
      nodesDestroyNode((SNode*)actual);
      taosMemoryFree(expectedWire);
      taosMemoryFree(actualWire);
    }
    taosMemoryFree(body);
  }
  nodesClearList(plan.pSubQ);
  taosArrayDestroy(plan.pSubqueryDeps);
  nodesDestroyNode(plan.pTagIndexCond);
  nodesDestroyNode(plan.pTagCond);
  nodesDestroyNode((SNode*)plan.pNode);
  nodesDestroyNode((SNode*)plan.pDataSink);
}

TEST(SubQueryMsgTest, RejectsInvalidConstantMatchPatternDuringPlanDecode) {
  SSubplan* plan = nullptr;
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&plan));
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_OPERATOR, &plan->pTagCond));
  auto* op = (SOperatorNode*)plan->pTagCond;
  op->opType = OP_TYPE_MATCH;
  op->charsetCxt = taosConvInit("UTF-8");
  ASSERT_NE(nullptr, op->charsetCxt);
  ASSERT_EQ(0, nodesMakeNode(QUERY_NODE_COLUMN, &op->pLeft));
  char pattern[] = "[\xe4\xb8\xad";
  ASSERT_EQ(0, nodesMakeValueNodeFromString(pattern, (SValueNode**)&op->pRight));

  char* wire = nullptr;
  int32_t length = 0;
  ASSERT_EQ(0, qSubPlanToMsg(plan, &wire, &length));
  SSubplan* decoded = nullptr;
  EXPECT_EQ(TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR, qMsgToSubplan(wire, length, &decoded));
  nodesDestroyNode((SNode*)decoded);
  taosMemoryFree(wire);
  nodesDestroyNode((SNode*)plan);
}

TEST(SubQueryMsgTest, CorrelatedPlanRejectsDecoderWithoutCapability) {
  SSubplan* plan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&plan));
  plan->requiresAncestorContext = true;
  char* wire = nullptr;
  int32_t length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSubPlanToMsg(plan, &wire, &length));
  ASSERT_GT(length, 6);

  std::vector<char> oldDecoderInput(wire, wire + length);
  SNode* unsupported = nullptr;
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT,
            nodesMsgToNode(oldDecoderInput.data(), oldDecoderInput.size(), &unsupported));
  EXPECT_EQ(nullptr, unsupported);

  SSubplan* decoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qMsgToSubplan(wire, length, &decoded));
  ASSERT_NE(nullptr, decoded);
  EXPECT_TRUE(decoded->requiresAncestorContext);
  nodesDestroyNode((SNode*)decoded);

  SSubplan* damaged = nullptr;
  EXPECT_NE(TSDB_CODE_SUCCESS, qMsgToSubplan(wire, length - 1, &damaged));
  taosMemoryFree(wire);

  char* unwrapped = nullptr;
  int32_t unwrappedLen = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg((SNode*)plan, &unwrapped, &unwrappedLen));
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE,
            qMsgToSubplan(unwrapped, unwrappedLen, &damaged));
  taosMemoryFree(unwrapped);

  plan->requiresAncestorContext = false;
  char* ordinary = nullptr;
  int32_t ordinaryLen = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSubPlanToMsg(plan, &ordinary, &ordinaryLen));
  std::vector<char> compatibleInput(ordinary, ordinary + ordinaryLen);
  SNode* compatible = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            nodesMsgToNode(compatibleInput.data(), compatibleInput.size(), &compatible));
  nodesDestroyNode(compatible);
  SSubplan* ordinaryDecoded = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, qMsgToSubplan(ordinary, ordinaryLen, &ordinaryDecoded));
  nodesDestroyNode((SNode*)ordinaryDecoded);
  taosMemoryFree(ordinary);
  nodesDestroyNode((SNode*)plan);
}

TEST(SubQueryMsgTest, PhysicalExchangeKeepsActualProducerQueryId) {
  SSubplan* plan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&plan));
  plan->id.queryId = 303;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, (SNode**)&plan->pNode));
  auto* exchange = (SExchangePhysiNode*)plan->pNode;
  exchange->srcStartGroupId = 1;
  exchange->srcEndGroupId = 1;
  SDownstreamSourceNode producer = {};
  producer.type = QUERY_NODE_DOWNSTREAM_SOURCE;
  producer.producerQueryId = 202;
  producer.addr.nodeId = 1;
  producer.taskId = 4;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetSubplanExecutionNode(plan, 1, &producer));

  char* wire = nullptr;
  int32_t length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSubPlanToMsg(plan, &wire, &length));
  SSubplan* privatePlan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, qMsgToSubplan(wire, length, &privatePlan));
  auto* privateExchange = (SExchangePhysiNode*)privatePlan->pNode;
  ASSERT_EQ(1, LIST_LENGTH(privateExchange->pSrcEndPoints));
  auto* restored = (SDownstreamSourceNode*)nodesListGetNode(privateExchange->pSrcEndPoints, 0);
  EXPECT_EQ(202U, restored->producerQueryId);
  EXPECT_EQ(4U, restored->taskId);
  taosMemoryFree(wire);
  nodesDestroyNode((SNode*)privatePlan);
  nodesDestroyNode((SNode*)plan);
}
