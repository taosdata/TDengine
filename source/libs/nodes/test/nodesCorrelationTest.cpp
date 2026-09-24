/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <memory>
#include <string>
#include <vector>

#include "functionMgt.h"
#include "nodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "tjson.h"

extern "C" void nodesTestSetStrictAppendFailOnce(void);

namespace {

using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;
using ListPtr = std::unique_ptr<SNodeList, decltype(&nodesDestroyList)>;

constexpr int64_t kBindingId = INT64_C(9007199254740993);
constexpr int64_t kTableRefId = INT64_C(9007199254740997);
constexpr int64_t kExprId = INT64_C(9007199254741001);
const char kPlanBytes[] = {'D', '\0', 'A', 'G', static_cast<char>(0xff), '\0'};

NodePtr makeNode(ENodeType type) {
  SNode* pNode = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(type, &pNode));
  EXPECT_NE(nullptr, pNode);
  return NodePtr(pNode, nodesDestroyNode);
}

SDataType decimalType() {
  SDataType type = {};
  type.type = TSDB_DATA_TYPE_DECIMAL;
  type.bytes = 16;
  type.precision = 30;
  type.scale = 6;
  return type;
}

void setExpr(SExprNode* pExpr) {
  pExpr->resType = decimalType();
  tstrncpy(pExpr->aliasName, "scalar_result", sizeof(pExpr->aliasName));
  tstrncpy(pExpr->userAlias, "visible_result", sizeof(pExpr->userAlias));
}

NodePtr makeColumn(const char* name, int16_t colId) {
  NodePtr node = makeNode(QUERY_NODE_COLUMN);
  auto* pCol = reinterpret_cast<SColumnNode*>(node.get());
  setExpr(&pCol->node);
  pCol->tableId = 10;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->colId = colId;
  pCol->dataBlockId = 42;
  pCol->slotId = colId;
  tstrncpy(pCol->tableAlias, "outer_table", sizeof(pCol->tableAlias));
  tstrncpy(pCol->colName, name, sizeof(pCol->colName));
  return node;
}

NodePtr makeParam(int32_t paramId) {
  NodePtr node = makeNode(QUERY_NODE_CORRELATION_PARAM);
  auto* pParam = reinterpret_cast<SCorrelationParamNode*>(node.get());
  setExpr(&pParam->node);
  pParam->paramId = paramId;
  return node;
}

NodePtr makeBinding(int32_t paramId, ECorrelationSourceKind kind) {
  NodePtr node = makeNode(QUERY_NODE_CORRELATION_PARAM_BINDING);
  auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(node.get());
  pBinding->paramId = paramId;
  pBinding->dataType = decimalType();
  pBinding->sourceKind = kind;
  pBinding->scopeId = 3;
  pBinding->bindingId = kBindingId + paramId;
  pBinding->tableRefId = kTableRefId;
  if (kind == CORRELATION_SOURCE_INHERITED) {
    pBinding->inheritedParamId = 0;
  } else {
    pBinding->pSource = makeColumn(paramId == 0 ? "hidden_logical" : "hidden_block", paramId + 1).release();
    if (kind == CORRELATION_SOURCE_BLOCK) {
      pBinding->sourceBlockId = 42;
      pBinding->sourceSlotId = paramId + 1;
    }
  }
  return node;
}

NodePtr makeSubquery() {
  NodePtr node = makeNode(QUERY_NODE_CORRELATED_SUBQUERY);
  auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(node.get());
  setExpr(&pSubquery->node);
  pSubquery->templateId = 0;
  pSubquery->exprId = kExprId;
  pSubquery->scopeId = 3;
  pSubquery->parentScopeId = 1;
  pSubquery->pSubquerySql = taosStrdup("SELECT sum(v) FROM inner_table WHERE k = outer_table.k");
  EXPECT_NE(nullptr, pSubquery->pSubquerySql);
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(
      &pSubquery->pParamBindings, makeBinding(0, CORRELATION_SOURCE_LOGICAL).release()));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(
      &pSubquery->pParamBindings, makeBinding(1, CORRELATION_SOURCE_BLOCK).release()));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(
      &pSubquery->pParamBindings, makeBinding(2, CORRELATION_SOURCE_INHERITED).release()));
  return node;
}

NodePtr makeTemplate(int32_t templateId, bool nested) {
  NodePtr node = makeNode(QUERY_NODE_CORRELATION_TEMPLATE);
  auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(node.get());
  pTemplate->templateId = templateId;
  pTemplate->scopeId = 3;
  pTemplate->resultType = decimalType();
  tstrncpy(pTemplate->resultName, "scalar_result", sizeof(pTemplate->resultName));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pTemplate->pParams, makeParam(0).release()));
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pTemplate->pParams, makeParam(1).release()));
  pTemplate->planLen = sizeof(kPlanBytes);
  pTemplate->pPlan = static_cast<char*>(taosMemoryMalloc(pTemplate->planLen));
  EXPECT_NE(nullptr, pTemplate->pPlan);
  if (pTemplate->pPlan != nullptr) {
    memcpy(pTemplate->pPlan, kPlanBytes, sizeof(kPlanBytes));
  }
  if (nested) {
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              nodesListMakeStrictAppend(&pTemplate->pNestedTemplates, makeTemplate(templateId + 1, false).release()));
  }
  return node;
}

NodePtr makeFixture(ENodeType type) {
  if (type == QUERY_NODE_CORRELATION_PARAM) return makeParam(0);
  if (type == QUERY_NODE_CORRELATION_PARAM_BINDING) return makeBinding(0, CORRELATION_SOURCE_BLOCK);
  if (type == QUERY_NODE_CORRELATED_SUBQUERY) return makeSubquery();
  if (type == QUERY_NODE_CORRELATION_TEMPLATE) return makeTemplate(0, true);
  NodePtr node = makeNode(type);
  auto* pOuter = reinterpret_cast<SOuterRefNode*>(node.get());
  setExpr(&pOuter->node);
  pOuter->scopeId = 3;
  pOuter->bindingId = kBindingId;
  pOuter->tableRefId = kTableRefId;
  tstrncpy(pOuter->tableAlias, "outer_table", sizeof(pOuter->tableAlias));
  tstrncpy(pOuter->colName, "outer_column", sizeof(pOuter->colName));
  return node;
}

SDataType* resultType(SNode* pNode) {
  if (nodeType(pNode) == QUERY_NODE_CORRELATION_PARAM_BINDING) {
    return &reinterpret_cast<SCorrelationParamBindingNode*>(pNode)->dataType;
  }
  if (nodeType(pNode) == QUERY_NODE_CORRELATION_TEMPLATE) {
    return &reinterpret_cast<SCorrelationTemplateNode*>(pNode)->resultType;
  }
  return &reinterpret_cast<SExprNode*>(pNode)->resType;
}

NodePtr cloneNode(SNode* pNode) {
  SNode* pClone = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pNode, &pClone));
  EXPECT_NE(nullptr, pClone);
  return NodePtr(pClone, nodesDestroyNode);
}

std::string saveJson(SNode* pNode) {
  char* pJson = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pNode, false, &pJson, nullptr));
  std::string json = pJson == nullptr ? "" : pJson;
  taosMemoryFree(pJson);
  return json;
}

std::vector<char> saveMsg(SNode* pNode) {
  char* pMsg = nullptr;
  int32_t len = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(pNode, &pMsg, &len));
  std::vector<char> msg;
  if (pMsg != nullptr && len > 0) msg.assign(pMsg, pMsg + len);
  taosMemoryFree(pMsg);
  return msg;
}

void expectFixture(SNode* pNode) {
  ASSERT_NE(nullptr, pNode);
  const SDataType* pType = resultType(pNode);
  EXPECT_EQ(TSDB_DATA_TYPE_DECIMAL, pType->type);
  EXPECT_EQ(16, pType->bytes);
  EXPECT_EQ(30, pType->precision);
  EXPECT_EQ(6, pType->scale);
  switch (nodeType(pNode)) {
    case QUERY_NODE_OUTER_REF: {
      auto* pOuter = reinterpret_cast<SOuterRefNode*>(pNode);
      EXPECT_EQ(kBindingId, pOuter->bindingId);
      EXPECT_EQ(kTableRefId, pOuter->tableRefId);
      EXPECT_STREQ("outer_column", pOuter->colName);
      break;
    }
    case QUERY_NODE_CORRELATION_PARAM:
      EXPECT_EQ(0, reinterpret_cast<SCorrelationParamNode*>(pNode)->paramId);
      break;
    case QUERY_NODE_CORRELATION_PARAM_BINDING: {
      auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(pNode);
      EXPECT_EQ(kBindingId, pBinding->bindingId);
      EXPECT_EQ(kTableRefId, pBinding->tableRefId);
      EXPECT_EQ(42, pBinding->sourceBlockId);
      EXPECT_EQ(1, pBinding->sourceSlotId);
      ASSERT_NE(nullptr, pBinding->pSource);
      EXPECT_STREQ("hidden_logical", reinterpret_cast<SColumnNode*>(pBinding->pSource)->colName);
      break;
    }
    case QUERY_NODE_CORRELATED_SUBQUERY: {
      auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(pNode);
      EXPECT_EQ(kExprId, pSubquery->exprId);
      EXPECT_EQ(0, pSubquery->templateId);
      ASSERT_EQ(3, LIST_LENGTH(pSubquery->pParamBindings));
      auto* pLogical = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 0));
      auto* pBlock = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 1));
      auto* pInherited = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 2));
      EXPECT_EQ(CORRELATION_SOURCE_LOGICAL, pLogical->sourceKind);
      EXPECT_EQ(-1, pLogical->sourceBlockId);
      EXPECT_EQ(CORRELATION_SOURCE_BLOCK, pBlock->sourceKind);
      EXPECT_EQ(42, pBlock->sourceBlockId);
      EXPECT_EQ(2, pBlock->sourceSlotId);
      EXPECT_EQ(CORRELATION_SOURCE_INHERITED, pInherited->sourceKind);
      EXPECT_EQ(0, pInherited->inheritedParamId);
      EXPECT_EQ(nullptr, pInherited->pSource);
      EXPECT_STREQ("SELECT sum(v) FROM inner_table WHERE k = outer_table.k", pSubquery->pSubquerySql);
      break;
    }
    case QUERY_NODE_CORRELATION_TEMPLATE: {
      auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(pNode);
      EXPECT_EQ(0, pTemplate->templateId);
      ASSERT_EQ(sizeof(kPlanBytes), pTemplate->planLen);
      ASSERT_NE(nullptr, pTemplate->pPlan);
      EXPECT_EQ(0, memcmp(kPlanBytes, pTemplate->pPlan, sizeof(kPlanBytes)));
      EXPECT_EQ(2, LIST_LENGTH(pTemplate->pParams));
      ASSERT_EQ(1, LIST_LENGTH(pTemplate->pNestedTemplates));
      auto* pNested = reinterpret_cast<SCorrelationTemplateNode*>(nodesListGetNode(pTemplate->pNestedTemplates, 0));
      EXPECT_EQ(1, pNested->templateId);
      ASSERT_EQ(sizeof(kPlanBytes), pNested->planLen);
      EXPECT_EQ(0, memcmp(kPlanBytes, pNested->pPlan, sizeof(kPlanBytes)));
      break;
    }
    default: FAIL() << "Unexpected correlation node";
  }
}

std::string replacePrimitive(std::string json, const char* field, const char* replacement) {
  std::string key = std::string("\"") + field + "\":";
  size_t start = json.find(key);
  EXPECT_NE(std::string::npos, start) << field;
  if (start == std::string::npos) return "";
  size_t end = json.find_first_of(",}", start + key.size());
  if (replacement != nullptr) {
    json.replace(start + key.size(), end - start - key.size(), replacement);
  } else if (json[end] == ',') {
    json.erase(start, end - start + 1);
  } else {
    if (start > 0 && json[start - 1] == ',') --start;
    json.erase(start, end - start);
  }
  return json;
}

// Wire headers are a network-order int16 tag followed by an int32 byte count.
constexpr size_t kTlvHeaderSize = 6;
uint32_t payloadSize(const std::vector<char>& msg, size_t offset) {
  if (offset > msg.size() || msg.size() - offset < kTlvHeaderSize) {
    ADD_FAILURE() << "Truncated TLV header at offset " << offset;
    return 0;
  }

  uint32_t size = 0;
  memcpy(&size, msg.data() + offset + sizeof(uint16_t), sizeof(size));
  return ntohl(size);
}

void updateOuterSize(std::vector<char>* pMsg) {
  ASSERT_GE(pMsg->size(), kTlvHeaderSize);
  uint32_t size = htonl(pMsg->size() - kTlvHeaderSize);
  memcpy(pMsg->data() + sizeof(uint16_t), &size, sizeof(size));
}

size_t findTlvField(const std::vector<char>& msg, size_t parentOffset, uint16_t wantedTag) {
  if (parentOffset > msg.size() || msg.size() - parentOffset < kTlvHeaderSize) {
    ADD_FAILURE() << "Truncated parent TLV header at offset " << parentOffset;
    return msg.size();
  }

  uint32_t parentSize = payloadSize(msg, parentOffset);
  if (parentSize > msg.size() - parentOffset - kTlvHeaderSize) {
    ADD_FAILURE() << "Truncated parent TLV payload at offset " << parentOffset;
    return msg.size();
  }

  size_t end = parentOffset + kTlvHeaderSize + parentSize;
  for (size_t offset = parentOffset + kTlvHeaderSize; end - offset >= kTlvHeaderSize;) {
    uint16_t tag = 0;
    memcpy(&tag, msg.data() + offset, sizeof(tag));
    if (ntohs(tag) == wantedTag) return offset;

    uint32_t fieldSize = payloadSize(msg, offset);
    if (fieldSize > end - offset - kTlvHeaderSize) {
      ADD_FAILURE() << "Truncated child TLV payload at offset " << offset;
      return msg.size();
    }
    offset += kTlvHeaderSize + fieldSize;
  }
  return msg.size();
}

EDealRes recordNode(SNode* pNode, void* pContext) {
  static_cast<std::vector<ENodeType>*>(pContext)->push_back(nodeType(pNode));
  return DEAL_RES_CONTINUE;
}

EDealRes recordRewrite(SNode** ppNode, void* pContext) { return recordNode(*ppNode, pContext); }

EDealRes remapColumn(SNode** ppNode, void* pContext) {
  if (nodeType(*ppNode) == QUERY_NODE_COLUMN) {
    auto* pCol = reinterpret_cast<SColumnNode*>(*ppNode);
    pCol->dataBlockId = 88;
    pCol->slotId += 10;
    ++*static_cast<int*>(pContext);
  }
  return DEAL_RES_CONTINUE;
}

class CorrelationNodeTest : public testing::TestWithParam<ENodeType> {};

TEST_P(CorrelationNodeTest, ConstructsAndDestroysPartialNodes) {
  NodePtr node = makeNode(GetParam());
  switch (GetParam()) {
    case QUERY_NODE_CORRELATION_PARAM:
      EXPECT_EQ(-1, reinterpret_cast<SCorrelationParamNode*>(node.get())->paramId);
      break;
    case QUERY_NODE_CORRELATION_PARAM_BINDING: {
      auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(node.get());
      EXPECT_EQ(-1, pBinding->paramId);
      EXPECT_EQ(-1, pBinding->sourceBlockId);
      EXPECT_EQ(-1, pBinding->sourceSlotId);
      EXPECT_EQ(-1, pBinding->inheritedParamId);
      pBinding->pSource = makeColumn("partial", 1).release();
      break;
    }
    case QUERY_NODE_CORRELATED_SUBQUERY: {
      auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(node.get());
      EXPECT_EQ(-1, pSubquery->templateId);
      pSubquery->pSubquerySql = taosStrdup("SELECT 1");
      EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pSubquery->pParamBindings,
          makeNode(QUERY_NODE_CORRELATION_PARAM_BINDING).release()));
      break;
    }
    case QUERY_NODE_CORRELATION_TEMPLATE: {
      auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(node.get());
      EXPECT_EQ(-1, pTemplate->templateId);
      EXPECT_EQ(TSDB_CODE_SUCCESS,
                nodesListMakeStrictAppend(&pTemplate->pParams, makeNode(QUERY_NODE_CORRELATION_PARAM).release()));
      pTemplate->pPlan = static_cast<char*>(taosMemoryMalloc(1));
      break;
    }
    default: break;
  }
  node.reset();
}

TEST_P(CorrelationNodeTest, CloneOwnsPayloadAfterOriginalIsDestroyed) {
  NodePtr source = makeFixture(GetParam());
  NodePtr copy = cloneNode(source.get());
  ASSERT_NE(nullptr, copy);
  EXPECT_TRUE(nodesEqualNode(source.get(), copy.get()));
  if (GetParam() == QUERY_NODE_CORRELATION_PARAM_BINDING) {
    EXPECT_NE(reinterpret_cast<SCorrelationParamBindingNode*>(source.get())->pSource,
              reinterpret_cast<SCorrelationParamBindingNode*>(copy.get())->pSource);
  } else if (GetParam() == QUERY_NODE_CORRELATED_SUBQUERY) {
    auto* pSource = reinterpret_cast<SCorrelatedSubqueryNode*>(source.get());
    auto* pCopy = reinterpret_cast<SCorrelatedSubqueryNode*>(copy.get());
    EXPECT_NE(pSource->pSubquerySql, pCopy->pSubquerySql);
    EXPECT_NE(pSource->pParamBindings, pCopy->pParamBindings);
    EXPECT_NE(nodesListGetNode(pSource->pParamBindings, 0), nodesListGetNode(pCopy->pParamBindings, 0));
  } else if (GetParam() == QUERY_NODE_CORRELATION_TEMPLATE) {
    auto* pSource = reinterpret_cast<SCorrelationTemplateNode*>(source.get());
    auto* pCopy = reinterpret_cast<SCorrelationTemplateNode*>(copy.get());
    EXPECT_NE(pSource->pPlan, pCopy->pPlan);
    EXPECT_NE(pSource->pParams, pCopy->pParams);
    EXPECT_NE(nodesListGetNode(pSource->pNestedTemplates, 0), nodesListGetNode(pCopy->pNestedTemplates, 0));
    memset(pSource->pPlan, 0, pSource->planLen);
  }
  source.reset();
  expectFixture(copy.get());
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(copy.get()));
  EXPECT_FALSE(saveJson(copy.get()).empty());
  EXPECT_FALSE(saveMsg(copy.get()).empty());
}

TEST_P(CorrelationNodeTest, EqualityIncludesEveryResultTypeComponent) {
  NodePtr source = makeFixture(GetParam());
  NodePtr copy = cloneNode(source.get());
  SDataType* pType = resultType(copy.get());
  for (int field = 0; field < 4; ++field) {
    *pType = decimalType();
    if (field == 0) pType->type = TSDB_DATA_TYPE_DECIMAL64;
    if (field == 1) ++pType->bytes;
    if (field == 2) --pType->precision;
    if (field == 3) ++pType->scale;
    EXPECT_FALSE(nodesEqualNode(source.get(), copy.get())) << field;
  }
  *pType = decimalType();
  EXPECT_TRUE(nodesEqualNode(source.get(), copy.get()));
}

TEST_P(CorrelationNodeTest, JsonAndTlvPreserveIndependentIdentityAndPayload) {
  NodePtr source = makeFixture(GetParam());
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(source.get()));
  std::string json = saveJson(source.get());
  std::vector<char> msg = saveMsg(source.get());
  ASSERT_FALSE(json.empty());
  ASSERT_FALSE(msg.empty());
  SNode* pJsonNode = nullptr;
  SNode* pMsgNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pJsonNode));
  NodePtr jsonNode(pJsonNode, nodesDestroyNode);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pMsgNode));
  NodePtr msgNode(pMsgNode, nodesDestroyNode);
  EXPECT_TRUE(nodesEqualNode(source.get(), jsonNode.get()));
  EXPECT_TRUE(nodesEqualNode(source.get(), msgNode.get()));
  source.reset();
  expectFixture(jsonNode.get());
  expectFixture(msgNode.get());
  EXPECT_TRUE(nodesEqualNode(jsonNode.get(), msgNode.get()));
}

TEST_P(CorrelationNodeTest, RejectsMissingAndWrongTypedJsonIdentity) {
  NodePtr node = makeFixture(GetParam());
  const char* field = "ParamId";
  if (GetParam() == QUERY_NODE_OUTER_REF) field = "ScopeId";
  if (GetParam() == QUERY_NODE_CORRELATED_SUBQUERY || GetParam() == QUERY_NODE_CORRELATION_TEMPLATE) field = "TemplateId";
  std::string json = saveJson(node.get());
  for (const char* replacement : {static_cast<const char*>(nullptr), "[]"}) {
    std::string bad = replacePrimitive(json, field, replacement);
    SNode* pDecoded = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, nodesStringToNode(bad.c_str(), &pDecoded)) << bad;
    nodesDestroyNode(pDecoded);
  }
}

TEST_P(CorrelationNodeTest, RejectsMissingAndDuplicateTlvFields) {
  NodePtr node = makeFixture(GetParam());
  std::vector<char> msg = saveMsg(node.get());
  ASSERT_GT(msg.size(), 2 * kTlvHeaderSize);
  size_t fieldSize = kTlvHeaderSize + payloadSize(msg, kTlvHeaderSize);
  ASSERT_LE(2 * kTlvHeaderSize + payloadSize(msg, kTlvHeaderSize), msg.size());
  std::vector<std::vector<char>> badMessages;
  std::vector<char> missing = msg;
  missing.erase(missing.begin() + kTlvHeaderSize, missing.begin() + kTlvHeaderSize + fieldSize);
  updateOuterSize(&missing);
  badMessages.push_back(missing);
  std::vector<char> duplicate = msg;
  duplicate.insert(duplicate.end(), msg.begin() + kTlvHeaderSize, msg.begin() + kTlvHeaderSize + fieldSize);
  updateOuterSize(&duplicate);
  badMessages.push_back(duplicate);
  for (const auto& bad : badMessages) {
    SNode* pDecoded = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, nodesMsgToNode(bad.data(), bad.size(), &pDecoded));
    nodesDestroyNode(pDecoded);
  }
}

INSTANTIATE_TEST_SUITE_P(CorrelationContracts, CorrelationNodeTest,
                        testing::Values(QUERY_NODE_OUTER_REF, QUERY_NODE_CORRELATION_PARAM,
                                        QUERY_NODE_CORRELATION_PARAM_BINDING, QUERY_NODE_CORRELATED_SUBQUERY,
                                        QUERY_NODE_CORRELATION_TEMPLATE));

TEST(CorrelationContractTest, OwnedFieldsRejectWrongNodeTypesBeforeDecodingTheirPayloads) {
  NodePtr value = makeNode(QUERY_NODE_VALUE);
  auto* pValue = reinterpret_cast<SValueNode*>(value.get());
  setExpr(&pValue->node);
  pValue->translate = true;
  pValue->literal = taosStrdup("1");
  pValue->datum.p = static_cast<char*>(taosMemoryCalloc(1, pValue->node.resType.bytes));
  ASSERT_NE(nullptr, pValue->datum.p);
  pValue->datum.p[0] = 1;
  std::string valueJson = saveJson(value.get());
  std::vector<char> valueMsg = saveMsg(value.get());

  // Start with a valid legacy Value packet, then make its Decimal Datum (16
  // bytes) larger than the allocation declared by DataType.Bytes (1 byte).
  // Correlation-owned fields must reject Value before entering that decoder.
  size_t exprOffset = findTlvField(valueMsg, 0, 1);  // VALUE_CODE_EXPR_BASE.
  ASSERT_LT(exprOffset, valueMsg.size());
  size_t typeOffset = findTlvField(valueMsg, exprOffset, 1);  // EXPR_CODE_RES_TYPE.
  ASSERT_LT(typeOffset, valueMsg.size());
  size_t bytesOffset = findTlvField(valueMsg, typeOffset, 4);  // DATA_TYPE_CODE_BYTES.
  ASSERT_LT(bytesOffset, valueMsg.size());
  ASSERT_EQ(sizeof(int32_t), payloadSize(valueMsg, bytesOffset));
  uint32_t oneByte = htonl(1);
  memcpy(valueMsg.data() + bytesOffset + kTlvHeaderSize, &oneByte, sizeof(oneByte));
  size_t datumOffset = findTlvField(valueMsg, 0, 7);  // VALUE_CODE_DATUM.
  ASSERT_LT(datumOffset, valueMsg.size());
  ASSERT_EQ(16, payloadSize(valueMsg, datumOffset));

  const struct {
    ENodeType type;
    const char* field;
    uint16_t tag;
    bool isList;
  } cases[] = {
      {QUERY_NODE_CORRELATION_PARAM_BINDING, "Source", 10, false},
      {QUERY_NODE_CORRELATED_SUBQUERY, "ParamBindings", 5, true},
      {QUERY_NODE_CORRELATION_TEMPLATE, "Params", 5, true},
      {QUERY_NODE_CORRELATION_TEMPLATE, "NestedTemplates", 6, true},
      {QUERY_NODE_CORRELATION_TEMPLATE, "TimeRangeInit", 7, true},
      {QUERY_NODE_PHYSICAL_PLAN, "CorrelationTemplates", 4, true},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.field);
    NodePtr parent = test.type == QUERY_NODE_PHYSICAL_PLAN ? makeNode(test.type) : makeFixture(test.type);
    if (test.type == QUERY_NODE_PHYSICAL_PLAN) {
      auto* pPlan = reinterpret_cast<SQueryPlan*>(parent.get());
      ASSERT_EQ(TSDB_CODE_SUCCESS,
                nodesListMakeStrictAppend(&pPlan->pCorrelationTemplates, makeTemplate(0, false).release()));
    }
    std::string validJson = saveJson(parent.get());
    std::unique_ptr<SJson, decltype(&tjsonDelete)> json(tjsonParse(validJson.c_str()), tjsonDelete);
    ASSERT_NE(nullptr, json);
    SJson* pBody = tjsonGetObjectItem(json.get(), nodesNodeName(test.type));
    ASSERT_NE(nullptr, pBody);
    tjsonDeleteItemFromObject(pBody, test.field);
    SJson* pReplacement = tjsonParse(valueJson.c_str());
    ASSERT_NE(nullptr, pReplacement);
    if (test.isList) {
      SJson* pArray = tjsonCreateArray();
      ASSERT_NE(nullptr, pArray);
      ASSERT_EQ(TSDB_CODE_SUCCESS, tjsonAddItemToArray(pArray, pReplacement));
      pReplacement = pArray;
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, tjsonAddItemToObject(pBody, test.field, pReplacement));
    char* pBadJson = tjsonToUnformattedString(json.get());
    ASSERT_NE(nullptr, pBadJson);
    SNode* pDecoded = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, nodesStringToNode(pBadJson, &pDecoded));
    EXPECT_EQ(nullptr, pDecoded);
    taosMemoryFree(pBadJson);
    nodesDestroyNode(pDecoded);

    // Each node-list payload is a sequence of complete node envelopes; a
    // single malformed Value works for both Source and list-member cases.
    std::vector<char> msg = saveMsg(parent.get());
    size_t fieldOffset = findTlvField(msg, 0, test.tag);
    if (fieldOffset < msg.size()) {
      size_t fieldEnd = fieldOffset + kTlvHeaderSize + payloadSize(msg, fieldOffset);
      msg.erase(msg.begin() + fieldOffset, msg.begin() + fieldEnd);
    }
    std::vector<char> replacement(kTlvHeaderSize);
    uint16_t tag = htons(test.tag);
    uint32_t size = htonl(valueMsg.size());
    memcpy(replacement.data(), &tag, sizeof(tag));
    memcpy(replacement.data() + sizeof(tag), &size, sizeof(size));
    replacement.insert(replacement.end(), valueMsg.begin(), valueMsg.end());
    msg.insert(msg.begin() + fieldOffset, replacement.begin(), replacement.end());
    updateOuterSize(&msg);
    pDecoded = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
    EXPECT_EQ(nullptr, pDecoded);
    nodesDestroyNode(pDecoded);
  }
}

TEST(CorrelationContractTest, BindingSourceKindsHaveDistinctAllowedExpressionTypes) {
  NodePtr binding = makeBinding(0, CORRELATION_SOURCE_LOGICAL);
  auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(binding.get());
  nodesDestroyNode(pBinding->pSource);
  pBinding->pSource = makeFixture(QUERY_NODE_OUTER_REF).release();
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(binding.get()));
  pBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
  pBinding->sourceBlockId = 42;
  pBinding->sourceSlotId = 1;
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(binding.get()));
  nodesDestroyNode(pBinding->pSource);
  pBinding->pSource = makeParam(0).release();
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(binding.get()));
  pBinding->sourceKind = CORRELATION_SOURCE_INHERITED;
  pBinding->sourceBlockId = -1;
  pBinding->sourceSlotId = -1;
  pBinding->inheritedParamId = 0;
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(binding.get()));
  nodesDestroyNode(pBinding->pSource);
  pBinding->pSource = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(binding.get()));
}

TEST(CorrelationContractTest, StableIdentityIgnoresDiagnosticColumnNames) {
  NodePtr source = makeFixture(QUERY_NODE_OUTER_REF);
  NodePtr copy = cloneNode(source.get());
  auto* pCopy = reinterpret_cast<SOuterRefNode*>(copy.get());
  tstrncpy(pCopy->tableAlias, "renamed_alias", sizeof(pCopy->tableAlias));
  tstrncpy(pCopy->colName, "renamed_column", sizeof(pCopy->colName));
  EXPECT_TRUE(nodesEqualNode(source.get(), copy.get()));
  ++pCopy->scopeId;
  EXPECT_FALSE(nodesEqualNode(source.get(), copy.get()));
  --pCopy->scopeId;
  ++pCopy->bindingId;
  EXPECT_FALSE(nodesEqualNode(source.get(), copy.get()));
  --pCopy->bindingId;
  ++pCopy->tableRefId;
  EXPECT_FALSE(nodesEqualNode(source.get(), copy.get()));
}

TEST(CorrelationContractTest, CloneAppendFailureReclaimsPartialCopyAndPreservesSource) {
  for (ENodeType type : {QUERY_NODE_CORRELATED_SUBQUERY, QUERY_NODE_CORRELATION_TEMPLATE}) {
    NodePtr source = makeFixture(type);
    SNode* pCopy = nullptr;
    // nodesCloneList clones the first owned child before strict append fails.
    nodesTestSetStrictAppendFailOnce();
    int32_t code = nodesCloneNode(source.get(), &pCopy);
    NodePtr failedCopy(pCopy, nodesDestroyNode);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, code);
    EXPECT_EQ(nullptr, pCopy);
    expectFixture(source.get());
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(source.get()));
    NodePtr retry = cloneNode(source.get());
    ASSERT_NE(nullptr, retry);
    EXPECT_TRUE(nodesEqualNode(source.get(), retry.get()));
    source.reset();
    expectFixture(retry.get());
  }
}

TEST(CorrelationContractTest, RejectsAmbiguousSourcesAndRepeatedParameterDefinitions) {
  for (ECorrelationSourceKind kind : {CORRELATION_SOURCE_LOGICAL, CORRELATION_SOURCE_BLOCK,
                                     CORRELATION_SOURCE_INHERITED}) {
    NodePtr node = makeBinding(0, kind);
    auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(node.get());
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(node.get()));
    if (kind == CORRELATION_SOURCE_INHERITED) {
      pBinding->pSource = makeColumn("must_not_be_local", 1).release();
    } else {
      pBinding->inheritedParamId = 0;
    }
    EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(node.get()));
  }
  NodePtr subquery = makeSubquery();
  auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(subquery.get());
  auto* pSecond = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 1));
  pSecond->paramId = 0;
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(subquery.get()));
  NodePtr planTemplate = makeTemplate(0, false);
  auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(planTemplate.get());
  auto* pSecondParam = reinterpret_cast<SCorrelationParamNode*>(nodesListGetNode(pTemplate->pParams, 1));
  pSecondParam->paramId = 0;
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, nodesValidateCorrelationNode(planTemplate.get()));
}

TEST(CorrelationContractTest, UnfrozenTemplateCanHaveNoDagBytes) {
  NodePtr node = makeTemplate(0, false);
  auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(node.get());
  taosMemoryFreeClear(pTemplate->pPlan);
  pTemplate->planLen = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesValidateCorrelationNode(node.get()));
  std::string json = saveJson(node.get());
  std::vector<char> msg = saveMsg(node.get());
  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pDecoded));
  NodePtr jsonNode(pDecoded, nodesDestroyNode);
  EXPECT_TRUE(nodesEqualNode(node.get(), jsonNode.get()));
  pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
  NodePtr msgNode(pDecoded, nodesDestroyNode);
  EXPECT_TRUE(nodesEqualNode(node.get(), msgNode.get()));
}

TEST(CorrelationContractTest, DependencyCheckStopsAtTemplateAndIncludesBindings) {
  NodePtr pTemplate = makeTemplate(0, false);
  NodePtr pBinding = makeBinding(0, CORRELATION_SOURCE_LOGICAL);
  NodePtr pParam = makeParam(0);
  NodePtr pCall = makeSubquery();
  NodePtr pColumn = makeColumn("ordinary", 1);

  EXPECT_FALSE(nodesExprHasCorrelation(pTemplate.get()));
  EXPECT_FALSE(nodesExprHasCorrelation(pColumn.get()));
  EXPECT_TRUE(nodesExprHasCorrelation(pBinding.get()));
  EXPECT_TRUE(nodesExprHasCorrelation(pParam.get()));
  EXPECT_TRUE(nodesExprHasCorrelation(pCall.get()));
}

TEST(CorrelationContractTest, WalkAndRewriteVisitOnlyLocalBindingDependencies) {
  NodePtr node = makeSubquery();
  const std::vector<ENodeType> pre = {QUERY_NODE_CORRELATED_SUBQUERY, QUERY_NODE_CORRELATION_PARAM_BINDING,
      QUERY_NODE_COLUMN, QUERY_NODE_CORRELATION_PARAM_BINDING, QUERY_NODE_COLUMN, QUERY_NODE_CORRELATION_PARAM_BINDING};
  const std::vector<ENodeType> post = {QUERY_NODE_COLUMN, QUERY_NODE_CORRELATION_PARAM_BINDING, QUERY_NODE_COLUMN,
      QUERY_NODE_CORRELATION_PARAM_BINDING, QUERY_NODE_CORRELATION_PARAM_BINDING, QUERY_NODE_CORRELATED_SUBQUERY};
  std::vector<ENodeType> visited;
  nodesWalkExpr(node.get(), recordNode, &visited);
  EXPECT_EQ(pre, visited);
  visited.clear();
  nodesWalkExprPostOrder(node.get(), recordNode, &visited);
  EXPECT_EQ(post, visited);
  SNode* pExpr = node.get();
  visited.clear();
  nodesRewriteExpr(&pExpr, recordRewrite, &visited);
  EXPECT_EQ(pre, visited);
  visited.clear();
  nodesRewriteExprPostOrder(&pExpr, recordRewrite, &visited);
  EXPECT_EQ(post, visited);

  SNodeList* pList = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pList, node.release()));
  ListPtr list(pList, nodesDestroyList);
  visited.clear();
  nodesWalkExprs(pList, recordNode, &visited);
  EXPECT_EQ(pre, visited);
  visited.clear();
  nodesWalkExprsPostOrder(pList, recordNode, &visited);
  EXPECT_EQ(post, visited);
  visited.clear();
  nodesRewriteExprs(pList, recordRewrite, &visited);
  EXPECT_EQ(pre, visited);
  visited.clear();
  nodesRewriteExprsPostOrder(pList, recordRewrite, &visited);
  EXPECT_EQ(post, visited);
  int remapped = 0;
  nodesRewriteExprs(pList, remapColumn, &remapped);
  EXPECT_EQ(2, remapped);
  auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(nodesListGetNode(pList, 0));
  auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 0));
  EXPECT_EQ(88, reinterpret_cast<SColumnNode*>(pBinding->pSource)->dataBlockId);
  EXPECT_EQ(11, reinterpret_cast<SColumnNode*>(pBinding->pSource)->slotId);

  NodePtr planTemplate = makeTemplate(0, true);
  visited.clear();
  nodesWalkExpr(planTemplate.get(), recordNode, &visited);
  EXPECT_EQ(std::vector<ENodeType>({QUERY_NODE_CORRELATION_TEMPLATE}), visited);
  pExpr = planTemplate.get();
  remapped = 0;
  nodesRewriteExprPostOrder(&pExpr, remapColumn, &remapped);
  EXPECT_EQ(0, remapped);
}

TEST(CorrelationContractTest, ColumnCollectionRetainsHiddenSourcesWithoutInheritedParameters) {
  NodePtr select = makeNode(QUERY_NODE_SELECT_STMT);
  auto* pSelect = reinterpret_cast<SSelectStmt*>(select.get());
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pSelect->pProjectionList, makeSubquery().release()));
  SNodeList* pColumns = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesCollectColumns(pSelect, SQL_CLAUSE_SELECT, "outer_table", COLLECT_COL_TYPE_ALL, &pColumns));
  ListPtr columns(pColumns, nodesDestroyList);
  ASSERT_EQ(2, LIST_LENGTH(pColumns));
  EXPECT_STREQ("hidden_logical", reinterpret_cast<SColumnNode*>(nodesListGetNode(pColumns, 0))->colName);
  EXPECT_STREQ("hidden_block", reinterpret_cast<SColumnNode*>(nodesListGetNode(pColumns, 1))->colName);
  select.reset();
  EXPECT_EQ(42, reinterpret_cast<SColumnNode*>(nodesListGetNode(pColumns, 1))->dataBlockId);
}

TEST(CorrelationContractTest, SourceCoordinatesStaySeparateFromTargetCoordinates) {
  NodePtr target = makeNode(QUERY_NODE_TARGET);
  auto* pTarget = reinterpret_cast<STargetNode*>(target.get());
  pTarget->dataBlockId = 91;
  pTarget->slotId = 12;
  pTarget->pExpr = makeSubquery().release();
  std::vector<char> msg = saveMsg(target.get());
  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
  NodePtr decoded(pDecoded, nodesDestroyNode);
  pTarget = reinterpret_cast<STargetNode*>(decoded.get());
  EXPECT_EQ(91, pTarget->dataBlockId);
  EXPECT_EQ(12, pTarget->slotId);
  auto* pSubquery = reinterpret_cast<SCorrelatedSubqueryNode*>(pTarget->pExpr);
  auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(pSubquery->pParamBindings, 1));
  EXPECT_EQ(42, pBinding->sourceBlockId);
  EXPECT_EQ(2, pBinding->sourceSlotId);
  NodePtr copy = cloneNode(pTarget->pExpr);
  reinterpret_cast<SCorrelationParamBindingNode*>(nodesListGetNode(
      reinterpret_cast<SCorrelatedSubqueryNode*>(copy.get())->pParamBindings, 1))->sourceSlotId = 12;
  EXPECT_FALSE(nodesEqualNode(pTarget->pExpr, copy.get()));
}

TEST(CorrelationContractTest, SqlUsesOriginalSubqueryAndNeverRendersInternalParameterAsPlaceholder) {
  NodePtr subquery = makeSubquery();
  char sql[512] = {};
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToSQL(subquery.get(), sql, sizeof(sql), &len));
  EXPECT_STREQ("(SELECT sum(v) FROM inner_table WHERE k = outer_table.k)", sql);
  NodePtr param = makeParam(0);
  memset(sql, 0, sizeof(sql));
  EXPECT_NE(TSDB_CODE_SUCCESS, nodesNodeToSQL(param.get(), sql, sizeof(sql), &len));
  EXPECT_EQ(nullptr, strchr(sql, '?'));
  for (ENodeType type : {QUERY_NODE_OUTER_REF, QUERY_NODE_CORRELATION_PARAM, QUERY_NODE_CORRELATED_SUBQUERY}) {
    NodePtr node = makeFixture(type);
    EXPECT_NE(TSDB_CODE_SUCCESS,
              nodesExprToExtSQL(node.get(), EXT_SQL_DIALECT_MYSQL, sql, sizeof(sql), &len));
  }
}

TEST(CorrelationContractTest, RemoteInputAliasCannotHideCorrelationInsideParentFunction) {
  for (bool correlated : {false, true}) {
    NodePtr function = makeNode(QUERY_NODE_FUNCTION);
    auto* pFunction = reinterpret_cast<SFunctionNode*>(function.get());
    setExpr(&pFunction->node);
    pFunction->funcType = FUNCTION_TYPE_ABS;
    tstrncpy(pFunction->functionName, "abs", sizeof(pFunction->functionName));
    tstrncpy(pFunction->node.aliasName, "child_abs", sizeof(pFunction->node.aliasName));
    NodePtr argument = correlated ? makeParam(0) : makeColumn("reading", 1);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pFunction->pParameterList, argument.release()));

    NodePtr scan = makeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
    auto* pScan = reinterpret_cast<SScanLogicNode*>(scan.get());
    pScan->scanType = SCAN_TYPE_EXTERNAL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pScan->pScanCols, makeColumn("reading", 1).release()));
    NodePtr table = makeNode(QUERY_NODE_EXTERNAL_TABLE);
    auto* pTable = reinterpret_cast<SExtTableNode*>(table.get());
    tstrncpy(pTable->sourceName, "source", sizeof(pTable->sourceName));
    tstrncpy(pTable->table.tableName, "readings", sizeof(pTable->table.tableName));
    pScan->pExtTableNode = table.release();

    NodePtr child = makeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
    auto* pChild = reinterpret_cast<SProjectLogicNode*>(child.get());
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pChild->pProjections, cloneNode(function.get()).release()));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pChild->node.pChildren, scan.release()));
    NodePtr parent = makeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
    auto* pParent = reinterpret_cast<SProjectLogicNode*>(parent.get());
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pParent->pProjections, function.release()));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pParent->node.pChildren, child.release()));

    // Use a real derived-table output namespace: renderNode replaces caller
    // context aliases. Verify that the whole parent ABS expression can match.
    SNodesRemoteSelectOutputs outputs = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCollectRemoteQuerySelectOutputs(&pChild->node, &outputs));
    ASSERT_EQ(1, LIST_LENGTH(outputs.pSelectExprs));
    SNode* pOutput = nodesListGetNode(outputs.pSelectExprs, 0);
    EXPECT_TRUE(nodesEqualNode(nodesListGetNode(pParent->pProjections, 0), pOutput));
    EXPECT_STREQ("child_abs", reinterpret_cast<SExprNode*>(pOutput)->aliasName);
    nodesDestroyRemoteSelectOutputs(&outputs);

    char* pSql = nullptr;
    int32_t code = nodesRemotePlanToSQL(&pParent->node, EXT_SOURCE_MYSQL, nullptr, &pSql);
    if (correlated) {
      EXPECT_EQ(TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, code);
    } else {
      EXPECT_EQ(TSDB_CODE_SUCCESS, code);
      ASSERT_NE(nullptr, pSql);
      EXPECT_NE(nullptr, strstr(pSql, "child_abs"));
      EXPECT_NE(nullptr, strstr(pSql, "FROM ("));
    }
    taosMemoryFree(pSql);
  }
}

TEST(CorrelationContractTest, QueryPlanRoundTripKeepsTemplatesSeparateFromJobs) {
  NodePtr plan = makeNode(QUERY_NODE_PHYSICAL_PLAN);
  auto* pPlan = reinterpret_cast<SQueryPlan*>(plan.get());
  pPlan->queryId = 123;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&pPlan->pCorrelationTemplates, makeTemplate(0, true).release()));
  for (bool useJson : {true, false}) {
    SNode* pDecoded = nullptr;
    if (useJson) {
      std::string json = saveJson(plan.get());
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pDecoded));
    } else {
      std::vector<char> msg = saveMsg(plan.get());
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
    }
    NodePtr decoded(pDecoded, nodesDestroyNode);
    auto* pBack = reinterpret_cast<SQueryPlan*>(decoded.get());
    ASSERT_EQ(1, LIST_LENGTH(pBack->pCorrelationTemplates));
    EXPECT_EQ(0, LIST_LENGTH(pBack->pChildren));
    EXPECT_EQ(0, pBack->numOfSubplans);
    expectFixture(nodesListGetNode(pBack->pCorrelationTemplates, 0));
  }
  nodesDestroyList(pPlan->pCorrelationTemplates);
  pPlan->pCorrelationTemplates = nullptr;
  for (bool useJson : {true, false}) {
    SNode* pDecoded = nullptr;
    if (useJson) {
      std::string json = saveJson(plan.get());
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pDecoded));
    } else {
      std::vector<char> msg = saveMsg(plan.get());
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
    }
    NodePtr decoded(pDecoded, nodesDestroyNode);
    EXPECT_EQ(0, LIST_LENGTH(reinterpret_cast<SQueryPlan*>(decoded.get())->pCorrelationTemplates));
  }
}

TEST(CorrelationContractTest, SingleLevelPlanJsonKeepsLegacyShape) {
  NodePtr plan = makeNode(QUERY_NODE_PHYSICAL_PLAN);
  auto* pPlan = reinterpret_cast<SQueryPlan*>(plan.get());
  NodePtr level = makeNode(QUERY_NODE_NODE_LIST);
  NodePtr subplan = makeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListMakeStrictAppend(&reinterpret_cast<SNodeListNode*>(level.get())->pNodeList, subplan.release()));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pPlan->pSubplans, level.release()));
  pPlan->numOfSubplans = 1;

  std::string json = saveJson(plan.get());
  EXPECT_NE(std::string::npos, json.find("\"Subplans\":"));
  EXPECT_EQ(std::string::npos, json.find("\"SubplanLevels\":"));
  EXPECT_EQ(std::string::npos, json.find("\"SubplanLinks\":"));
  EXPECT_EQ(std::string::npos, json.find("\"ExecNodeTableNum\":"));
  EXPECT_EQ(std::string::npos, json.find("\"ExplainMode\":"));
}

TEST(CorrelationContractTest, MergeSourceRangeSurvivesTemplateMessage) {
  NodePtr merge = makeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE);
  auto* pMerge = reinterpret_cast<SMergePhysiNode*>(merge.get());
  pMerge->srcGroupId = 5;
  pMerge->srcEndGroupId = 7;
  pMerge->numOfChannels = 3;

  std::vector<char> msg = saveMsg(merge.get());
  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(msg.data(), msg.size(), &pDecoded));
  NodePtr decoded(pDecoded, nodesDestroyNode);
  auto* pBack = reinterpret_cast<SMergePhysiNode*>(decoded.get());
  EXPECT_EQ(5, pBack->srcGroupId);
  EXPECT_EQ(7, pBack->srcEndGroupId);
  EXPECT_EQ(3, pBack->numOfChannels);
}

TEST(CorrelationContractTest, CompleteDagRoundTripKeepsSharedChildAndPrivateInstances) {
  NodePtr source = makeNode(QUERY_NODE_PHYSICAL_PLAN);
  auto* pPlan = reinterpret_cast<SQueryPlan*>(source.get());
  pPlan->queryId = 1234;
  pPlan->subQType = E_SUB_QUERY_SCALAR;
  pPlan->subSql = const_cast<char*>("select scalar result");
  pPlan->explainInfo.mode = EXPLAIN_MODE_ANALYZE;
  pPlan->explainInfo.verbose = true;
  pPlan->explainInfo.ratio = 0.25;
  pPlan->hasScan = true;
  pPlan->hasFederatedScan = true;
  pPlan->pPostPlan = reinterpret_cast<void*>(1);

  SSubplan* entities[4] = {nullptr};
  const int32_t sizes[] = {1, 2, 1};
  for (int32_t level = 0; level < 3; ++level) {
    NodePtr levelNode = makeNode(QUERY_NODE_NODE_LIST);
    auto* pLevel = reinterpret_cast<SNodeListNode*>(levelNode.get());
    for (int32_t i = 0; i < sizes[level]; ++i) {
      NodePtr entityNode = makeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
      auto* pEntity = reinterpret_cast<SSubplan*>(entityNode.get());
      pEntity->id = {pPlan->queryId, 2, pPlan->numOfSubplans + 1};
      pEntity->level = level;
      pEntity->execNodeStat.tableNum = 100 + pPlan->numOfSubplans;
      pEntity->requiresAncestorContext = true;
      if (0 == level) {
        pEntity->pDataSink = reinterpret_cast<SDataSinkNode*>(makeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH).release());
      }
      entities[pPlan->numOfSubplans] = pEntity;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pLevel->pNodeList, entityNode.release()));
      ++pPlan->numOfSubplans;
    }
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pPlan->pSubplans, levelNode.release()));
  }

  for (int32_t child = 1; child <= 2; ++child) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&entities[0]->pChildren, reinterpret_cast<SNode*>(entities[child])));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&entities[child]->pParents, reinterpret_cast<SNode*>(entities[0])));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&entities[child]->pChildren, reinterpret_cast<SNode*>(entities[3])));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&entities[3]->pParents, reinterpret_cast<SNode*>(entities[child])));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pPlan->pCorrelationTemplates, makeTemplate(0, true).release()));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pPlan->pChildren, makeNode(QUERY_NODE_PHYSICAL_PLAN).release()));

  std::string json = saveJson(source.get());
  std::vector<char> msg = saveMsg(source.get());
  source.reset();

  std::vector<char> damagedLength = msg;
  ASSERT_GE(damagedLength.size(), 6U);
  damagedLength[2] = 0x7f;
  damagedLength[3] = 0x7f;
  damagedLength[4] = 0x7f;
  damagedLength[5] = 0x7f;
  SNode* damagedPlan = nullptr;
  EXPECT_NE(TSDB_CODE_SUCCESS, nodesMsgToNode(damagedLength.data(), damagedLength.size(), &damagedPlan));
  nodesDestroyNode(damagedPlan);

  for (bool useJson : {true, false}) {
    for (int32_t instance = 0; instance < 2; ++instance) {
      SNode* pDecoded = nullptr;
      std::vector<char> privateMsg = msg;
      int32_t code = useJson ? nodesStringToNode(json.c_str(), &pDecoded)
                             : nodesMsgToNode(privateMsg.data(), privateMsg.size(), &pDecoded);
      ASSERT_EQ(TSDB_CODE_SUCCESS, code);
      NodePtr privatePlan(pDecoded, nodesDestroyNode);
      auto* pBack = reinterpret_cast<SQueryPlan*>(privatePlan.get());
      ASSERT_EQ(4, pBack->numOfSubplans);
      ASSERT_EQ(3, LIST_LENGTH(pBack->pSubplans));
      EXPECT_EQ(E_SUB_QUERY_SCALAR, pBack->subQType);
      EXPECT_STREQ("select scalar result", pBack->subSql);
      EXPECT_EQ(EXPLAIN_MODE_ANALYZE, pBack->explainInfo.mode);
      EXPECT_TRUE(pBack->explainInfo.verbose);
      EXPECT_DOUBLE_EQ(0.25, pBack->explainInfo.ratio);
      EXPECT_TRUE(pBack->hasScan);
      EXPECT_TRUE(pBack->hasFederatedScan);
      EXPECT_EQ(nullptr, pBack->pPostPlan);
      EXPECT_EQ(1, LIST_LENGTH(pBack->pChildren));
      ASSERT_EQ(1, LIST_LENGTH(pBack->pCorrelationTemplates));
      auto* pTemplate = reinterpret_cast<SCorrelationTemplateNode*>(nodesListGetNode(pBack->pCorrelationTemplates, 0));
      EXPECT_EQ(1, LIST_LENGTH(pTemplate->pNestedTemplates));

      auto* pRoot = reinterpret_cast<SSubplan*>(nodesListGetNode(
          reinterpret_cast<SNodeListNode*>(nodesListGetNode(pBack->pSubplans, 0))->pNodeList, 0));
      auto* pMiddleLevel = reinterpret_cast<SNodeListNode*>(nodesListGetNode(pBack->pSubplans, 1));
      auto* pMiddle1 = reinterpret_cast<SSubplan*>(nodesListGetNode(pMiddleLevel->pNodeList, 0));
      auto* pMiddle2 = reinterpret_cast<SSubplan*>(nodesListGetNode(pMiddleLevel->pNodeList, 1));
      auto* pLeaf = reinterpret_cast<SSubplan*>(nodesListGetNode(
          reinterpret_cast<SNodeListNode*>(nodesListGetNode(pBack->pSubplans, 2))->pNodeList, 0));
      EXPECT_EQ(2, LIST_LENGTH(pRoot->pChildren));
      EXPECT_EQ(reinterpret_cast<SNode*>(pLeaf), nodesListGetNode(pMiddle1->pChildren, 0));
      EXPECT_EQ(reinterpret_cast<SNode*>(pLeaf), nodesListGetNode(pMiddle2->pChildren, 0));
      EXPECT_EQ(2, LIST_LENGTH(pLeaf->pParents));
      EXPECT_EQ(100, pRoot->execNodeStat.tableNum);
      EXPECT_TRUE(pRoot->requiresAncestorContext);
    }
  }
}

TEST(CorrelationContractTest, PhysicalDagRejectsAmbiguousAndBrokenEdges) {
  auto makePlan = []() -> NodePtr {
    NodePtr plan = makeNode(QUERY_NODE_PHYSICAL_PLAN);
    auto* pPlan = reinterpret_cast<SQueryPlan*>(plan.get());
    pPlan->queryId = 77;
    pPlan->numOfSubplans = 2;
    for (int32_t level = 0; level < 2; ++level) {
      NodePtr levelNode = makeNode(QUERY_NODE_NODE_LIST);
      NodePtr entityNode = makeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
      auto* pEntity = reinterpret_cast<SSubplan*>(entityNode.get());
      pEntity->id = {77, 3, level + 1};
      pEntity->level = level;
      if (0 == level) {
        pEntity->pDataSink = reinterpret_cast<SDataSinkNode*>(makeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH).release());
      }
      auto* pLevel = reinterpret_cast<SNodeListNode*>(levelNode.get());
      EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pLevel->pNodeList, entityNode.release()));
      EXPECT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pPlan->pSubplans, levelNode.release()));
    }
    return plan;
  };
  auto entities = [](SQueryPlan* pPlan, int32_t level) -> SSubplan* {
    auto* pLevel = reinterpret_cast<SNodeListNode*>(nodesListGetNode(pPlan->pSubplans, level));
    return reinterpret_cast<SSubplan*>(nodesListGetNode(pLevel->pNodeList, 0));
  };

  for (int32_t damage = 0; damage < 3; ++damage) {
    NodePtr plan = makePlan();
    auto* pPlan = reinterpret_cast<SQueryPlan*>(plan.get());
    SSubplan* pRoot = entities(pPlan, 0);
    SSubplan* pChild = entities(pPlan, 1);
    SSubplanLink link = {pRoot->id, pChild->id};
    if (0 == damage) {
      pChild->id = pRoot->id;
    }
    if (1 == damage) {
      link.childId.subplanId = 99;
    }
    if (2 == damage) {
      link.parentId = pChild->id;
      link.childId = pRoot->id;
    }
    SArray* links = taosArrayInit(1, sizeof(SSubplanLink));
    ASSERT_NE(nullptr, links);
    ASSERT_NE(nullptr, taosArrayPush(links, &link));
    EXPECT_NE(TSDB_CODE_SUCCESS, nodesRestoreSubplanDag(pPlan, links));
    taosArrayDestroy(links);
  }
}

}  // namespace
