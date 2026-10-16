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

#include <cstring>
#include <vector>

#include "nodes.h"
#include "planner.h"
#include "plannodes.h"
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
