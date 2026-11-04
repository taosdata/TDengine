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

#include <memory>
#include <string>

#include "cmdnodes.h"
#include "query.h"
#include "querynodes.h"
#include "plannodes.h"

namespace {

using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;

NodePtr makeComparison(const char* column, const char* literal, EOperatorType op = OP_TYPE_EQUAL,
                       int8_t type = TSDB_DATA_TYPE_BINARY) {
  SNode* pNode = nullptr;
  if (nodesMakeNode(QUERY_NODE_OPERATOR, &pNode) != TSDB_CODE_SUCCESS) {
    return NodePtr(nullptr, nodesDestroyNode);
  }
  NodePtr result(pNode, nodesDestroyNode);
  auto*   pOp = reinterpret_cast<SOperatorNode*>(pNode);
  pOp->opType = op;
  if (nodesMakeNode(QUERY_NODE_COLUMN, &pOp->pLeft) != TSDB_CODE_SUCCESS ||
      nodesMakeNode(QUERY_NODE_VALUE, &pOp->pRight) != TSDB_CODE_SUCCESS) {
    return NodePtr(nullptr, nodesDestroyNode);
  }
  auto* pCol = reinterpret_cast<SColumnNode*>(pOp->pLeft);
  tstrncpy(pCol->colName, column, sizeof(pCol->colName));
  auto* pValue = reinterpret_cast<SValueNode*>(pOp->pRight);
  pValue->literal = taosStrdup(literal);
  if (pValue->literal == nullptr) {
    return NodePtr(nullptr, nodesDestroyNode);
  }
  pValue->node.resType.type = type;
  pValue->node.resType.bytes = IS_VAR_DATA_TYPE(type) ? strlen(literal) : tDataTypes[type].bytes;
  return result;
}

NodePtr joinConditions(NodePtr left, NodePtr right, ELogicConditionType type = LOGIC_COND_TYPE_AND) {
  SNode* pNode = nullptr;
  if (nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &pNode) != TSDB_CODE_SUCCESS) {
    return NodePtr(nullptr, nodesDestroyNode);
  }
  NodePtr result(pNode, nodesDestroyNode);
  auto*   pLogic = reinterpret_cast<SLogicConditionNode*>(pNode);
  pLogic->condType = type;
  if (nodesListMakeStrictAppend(&pLogic->pParameterList, left.release()) != TSDB_CODE_SUCCESS ||
      nodesListMakeStrictAppend(&pLogic->pParameterList, right.release()) != TSDB_CODE_SUCCESS) {
    return NodePtr(nullptr, nodesDestroyNode);
  }
  return result;
}

class ExternalFilterTest : public testing::TestWithParam<EExtSourceType> {
 protected:
  void SetUp() override {
    meta_.pCols = columns_;
    meta_.numOfCols = 3;
    tstrncpy(columns_[0].colName, "ts", sizeof(columns_[0].colName));
    tstrncpy(columns_[0].extTypeName, GetParam() == EXT_SOURCE_MYSQL ? "DATETIME" : "TIMESTAMP",
             sizeof(columns_[0].extTypeName));
    columns_[0].isRemotePrimaryKey = true;
    tstrncpy(columns_[1].colName, "device", sizeof(columns_[1].colName));
    tstrncpy(columns_[1].extTypeName, "VARCHAR(16)", sizeof(columns_[1].extTypeName));
    columns_[1].isRemotePrimaryKey = true;
    tstrncpy(columns_[2].colName, "value", sizeof(columns_[2].colName));
    tstrncpy(columns_[2].extTypeName, "INT", sizeof(columns_[2].extTypeName));
  }

  int32_t validate(SNode* pFilter) {
    return nodesValidateExternalFilter(GetParam(), &meta_, pFilter, nullptr, nullptr, message_, sizeof(message_),
                                       &tsIndex_);
  }

  std::string save(SNode* pFilter) {
    char*   pJson = nullptr;
    int32_t length = 0;
    EXPECT_EQ(nodesNodeToString(pFilter, false, &pJson, &length), TSDB_CODE_SUCCESS);
    std::string result = pJson == nullptr ? "" : pJson;
    taosMemoryFree(pJson);
    return result;
  }

  int32_t validateSaved(const std::string& json) {
    return nodesValidateExternalFilterCurrentMeta(GetParam(), &meta_, json.c_str(), json.size(), nullptr, nullptr,
                                                  message_, sizeof(message_), &tsIndex_);
  }

  SExtColumnDef columns_[3] = {};
  SExtTableMeta meta_ = {};
  char          message_[256] = {};
  int32_t       tsIndex_ = -1;
};

TEST_P(ExternalFilterTest, NormalizesAndRevalidatesSavedFilter) {
  auto filter = makeComparison(GetParam() == EXT_SOURCE_MYSQL ? "DEVICE" : "device", "d1");
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  EXPECT_EQ(tsIndex_, 0);
  auto* pOp = reinterpret_cast<SOperatorNode*>(filter.get());
  EXPECT_STREQ(reinterpret_cast<SColumnNode*>(pOp->pLeft)->colName, "device");
  auto* pValue = reinterpret_cast<SValueNode*>(pOp->pRight);
  EXPECT_TRUE(pValue->translate);
  EXPECT_EQ(varDataLen(pValue->datum.p), 2);
  EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
}

TEST_P(ExternalFilterTest, MatchesColumnCaseBySourceType) {
  auto filter = makeComparison("DEVICE", "d1");
  ASSERT_NE(filter, nullptr);
  const auto saved = save(filter.get());
  if (GetParam() == EXT_SOURCE_MYSQL) {
    EXPECT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
    EXPECT_EQ(validateSaved(saved), TSDB_CODE_SUCCESS) << message_;
  } else {
    EXPECT_EQ(validate(filter.get()), TSDB_CODE_EXT_COLUMN_NOT_EXIST);
    EXPECT_EQ(validateSaved(saved), TSDB_CODE_EXT_SCHEMA_CHANGED);
  }
}

TEST_P(ExternalFilterTest, DistinguishesCaseSensitivePrimaryKeyColumns) {
  if (GetParam() != EXT_SOURCE_POSTGRESQL) {
    GTEST_SKIP() << "MySQL column names are case insensitive";
  }
  tstrncpy(columns_[2].colName, "Device", sizeof(columns_[2].colName));
  columns_[2].isRemotePrimaryKey = true;
  auto incomplete = makeComparison("device", "d1");
  EXPECT_EQ(validate(incomplete.get()), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  auto filter = joinConditions(makeComparison("device", "d1"), makeComparison("Device", "1"));
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;

  SNode* pSaved = nullptr;
  ASSERT_EQ(nodesStringToNode(save(filter.get()).c_str(), &pSaved), TSDB_CODE_SUCCESS);
  NodePtr saved(pSaved, nodesDestroyNode);
  char    sql[256] = {};
  int32_t length = 0;
  ASSERT_EQ(nodesExprToExtSQL(pSaved, EXT_SQL_DIALECT_POSTGRES, sql, sizeof(sql), &length), TSDB_CODE_SUCCESS);
  EXPECT_NE(std::string(sql).find("\"device\""), std::string::npos);
  EXPECT_NE(std::string(sql).find("\"Device\""), std::string::npos);
  ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pSaved, true), TSDB_CODE_SUCCESS);
  length = 0;
  ASSERT_EQ(nodesNodeToSQL(pSaved, sql, sizeof(sql), &length), TSDB_CODE_SUCCESS);
  EXPECT_NE(std::string(sql).find("`device`"), std::string::npos);
  EXPECT_NE(std::string(sql).find("`Device`"), std::string::npos);

  auto duplicate = joinConditions(std::move(filter), makeComparison("Device", "2"));
  EXPECT_EQ(validate(duplicate.get()), TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN);
  EXPECT_EQ(validateSaved(save(duplicate.get())), TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN);
}

TEST_P(ExternalFilterTest, RevalidatesColumnCaseChanges) {
  auto filter = makeComparison("device", "d1");
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  const auto saved = save(filter.get());
  tstrncpy(columns_[1].colName, "Device", sizeof(columns_[1].colName));
  EXPECT_EQ(validateSaved(saved),
            GetParam() == EXT_SOURCE_MYSQL ? TSDB_CODE_SUCCESS : TSDB_CODE_EXT_SCHEMA_CHANGED);
}

TEST_P(ExternalFilterTest, RequiresStoredBusinessPrimaryKeyCoverage) {
  EXPECT_EQ(validate(nullptr), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  EXPECT_EQ(validateSaved(""), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  auto filter = makeComparison("device", "d1", OP_TYPE_NOT_EQUAL);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(validate(filter.get()), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
  EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
}

TEST_P(ExternalFilterTest, RevalidatesEmptyStringLiteral) {
  auto filter = makeComparison("device", "");
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
}

TEST_P(ExternalFilterTest, PreservesActualUnicodeLiteralSize) {
  tstrncpy(columns_[1].extCharsetName, GetParam() == EXT_SOURCE_MYSQL ? "utf8mb4" : "UTF8",
           sizeof(columns_[1].extCharsetName));
  for (const char* literal : {"", "d1"}) {
    auto filter = makeComparison("device", literal);
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
    const auto* value =
        reinterpret_cast<const SValueNode*>(reinterpret_cast<const SOperatorNode*>(filter.get())->pRight);
    ASSERT_EQ(value->node.resType.type, TSDB_DATA_TYPE_NCHAR);
    EXPECT_EQ(value->node.resType.bytes, varDataTLen(value->datum.p));
    EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
  }
}

TEST_P(ExternalFilterTest, RejectsInvalidNumericAndBooleanLiterals) {
  for (const char* type : {"INT", "DOUBLE PRECISION", "BOOLEAN"}) {
    tstrncpy(columns_[1].extTypeName, type, sizeof(columns_[1].extTypeName));
    for (const char* literal : {"", "invalid", "1invalid"}) {
      auto filter = makeComparison("device", literal);
      ASSERT_NE(filter, nullptr);
      EXPECT_EQ(validate(filter.get()), TSDB_CODE_PAR_WRONG_VALUE_TYPE) << type << ": " << literal;
    }
  }
}

TEST_P(ExternalFilterTest, RejectsEmptyStringAfterNumericSchemaChange) {
  auto filter = makeComparison("device", "");
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS);
  const auto json = save(filter.get());
  for (const char* type : {"INT", "DOUBLE PRECISION", "BOOLEAN"}) {
    tstrncpy(columns_[1].extTypeName, type, sizeof(columns_[1].extTypeName));
    EXPECT_EQ(validateSaved(json), TSDB_CODE_EXT_SCHEMA_CHANGED) << type;
  }
}

TEST_P(ExternalFilterTest, NormalizesNumericBooleanLiteral) {
  tstrncpy(columns_[1].extTypeName, "BOOLEAN", sizeof(columns_[1].extTypeName));
  for (const char* literal : {"0", "1"}) {
    auto filter = makeComparison("device", literal, OP_TYPE_EQUAL, TSDB_DATA_TYPE_BIGINT);
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
    const auto* value =
        reinterpret_cast<const SValueNode*>(reinterpret_cast<const SOperatorNode*>(filter.get())->pRight);
    EXPECT_STREQ(value->literal, literal[0] == '0' ? "false" : "true");
    EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
  }
}

TEST_P(ExternalFilterTest, AllowsTimestampOnlyPrimaryKeyWithoutFilter) {
  columns_[1].isRemotePrimaryKey = false;
  EXPECT_EQ(validate(nullptr), TSDB_CODE_SUCCESS);
  EXPECT_EQ(validateSaved(""), TSDB_CODE_SUCCESS);
  EXPECT_EQ(tsIndex_, 0);
}

TEST_P(ExternalFilterTest, RejectsMissingOrAmbiguousTimestampPrimaryKey) {
  columns_[0].isRemotePrimaryKey = false;
  columns_[1].isRemotePrimaryKey = false;
  EXPECT_EQ(validate(nullptr), TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);
  EXPECT_EQ(validateSaved(""), TSDB_CODE_EXT_SCHEMA_CHANGED);
  columns_[0].isRemotePrimaryKey = true;
  columns_[1].isRemotePrimaryKey = true;
  tstrncpy(columns_[1].extTypeName, columns_[0].extTypeName, sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validate(nullptr), TSDB_CODE_EXT_NO_TS_PRIMARY_KEY);
  EXPECT_EQ(validateSaved(""), TSDB_CODE_EXT_SCHEMA_CHANGED);
  columns_[1].isRemotePrimaryKey = false;
  tstrncpy(columns_[0].extTypeName, "TIME", sizeof(columns_[0].extTypeName));
  EXPECT_EQ(validateSaved(""), TSDB_CODE_EXT_SCHEMA_CHANGED);
}

TEST_P(ExternalFilterTest, RejectsDuplicateColumnsAndOr) {
  auto duplicate = joinConditions(makeComparison("device", "d1"),
                                  makeComparison(GetParam() == EXT_SOURCE_MYSQL ? "DEVICE" : "device", "d2"));
  ASSERT_NE(duplicate, nullptr);
  EXPECT_EQ(validate(duplicate.get()), TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN);
  EXPECT_EQ(validateSaved(save(duplicate.get())), TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN);
  auto disjunction = joinConditions(makeComparison("device", "d1"), makeComparison("value", "1"), LOGIC_COND_TYPE_OR);
  ASSERT_NE(disjunction, nullptr);
  EXPECT_EQ(validate(disjunction.get()), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_EQ(validateSaved(save(disjunction.get())), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
}

TEST_P(ExternalFilterTest, AcceptsCharacterOrderingComparison) {
  // The DDL validates the expression shape and the value type only.  A
  // predicate the dialect cannot render exactly (SQL Server character
  // ordering, supplementary-plane literals) is split to the local
  // FederatedScan filter by the planner, so CREATE/ALTER VTABLE must accept
  // it (SQL Server DS 5.3.11, judged items 13 and 14).
  // `device` is an ordinary character column here; as a primary key it may
  // only take `=`, which is covered by the other cases.
  columns_[1].isRemotePrimaryKey = false;
  for (EOperatorType op : {OP_TYPE_GREATER_THAN, OP_TYPE_GREATER_EQUAL, OP_TYPE_LOWER_THAN, OP_TYPE_LOWER_EQUAL}) {
    auto filter = makeComparison("device", "d1", op);
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
    EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
    EXPECT_EQ(tsIndex_, 0);
  }
}

TEST_P(ExternalFilterTest, RejectsQualifiedColumnsAndNonLiteralValues) {
  auto qualified = makeComparison("device", "d1");
  ASSERT_NE(qualified, nullptr);
  auto* pOp = reinterpret_cast<SOperatorNode*>(qualified.get());
  tstrncpy(reinterpret_cast<SColumnNode*>(pOp->pLeft)->tableName, "t", TSDB_TABLE_NAME_LEN);
  EXPECT_EQ(validate(qualified.get()), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  auto nonLiteral = makeComparison("device", "d1");
  ASSERT_NE(nonLiteral, nullptr);
  pOp = reinterpret_cast<SOperatorNode*>(nonLiteral.get());
  reinterpret_cast<SValueNode*>(pOp->pRight)->flag = VALUE_FLAG_RAW_SQL_FRAG;
  EXPECT_EQ(validate(nonLiteral.get()), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
}

TEST_P(ExternalFilterTest, RevalidatesTimestampLiteralAfterSerialization) {
  auto filter = joinConditions(makeComparison("device", "d1"), makeComparison("ts", "2026-01-02 03:04:05"));
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  EXPECT_EQ(validateSaved(save(filter.get())), TSDB_CODE_SUCCESS) << message_;
}

TEST_P(ExternalFilterTest, RestoresOnlyTimestampLiteralsForRemoteSql) {
  auto filter = joinConditions(joinConditions(makeComparison("device", "d1"),
                                              makeComparison("ts", "2026-01-02 03:04:05")),
                               makeComparison("value", "42"));
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS) << message_;
  SNode* pSaved = nullptr;
  ASSERT_EQ(nodesStringToNode(save(filter.get()).c_str(), &pSaved), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pSaved, false), TSDB_CODE_SUCCESS);
  NodePtr saved(pSaved, nodesDestroyNode);
  auto* pNumericOp = reinterpret_cast<SOperatorNode*>(
      nodesListGetNode(reinterpret_cast<SLogicConditionNode*>(pSaved)->pParameterList, 1));
  auto* pValue = reinterpret_cast<SValueNode*>(pNumericOp->pRight);
  EXPECT_EQ(pValue->node.resType.type, TSDB_DATA_TYPE_INT);
  EXPECT_EQ(pValue->datum.i, 42);

  char    sql[1024] = {};
  int32_t length = 0;
  auto    dialect = GetParam() == EXT_SOURCE_MYSQL ? EXT_SQL_DIALECT_MYSQL : EXT_SQL_DIALECT_POSTGRES;
  ASSERT_EQ(nodesExprToExtSQL(pSaved, dialect, sql, sizeof(sql), &length), TSDB_CODE_SUCCESS);
  EXPECT_NE(std::string(sql).find("'2026-01-02 03:04:05'"), std::string::npos);
  EXPECT_EQ(std::string(sql).find("TIMESTAMP '"), std::string::npos);
  EXPECT_EQ(nodesRestoreExternalFilterLiterals(&pSaved, false), TSDB_CODE_SUCCESS);
  EXPECT_EQ(validateSaved(save(pSaved)), TSDB_CODE_SUCCESS) << message_;
}

TEST_P(ExternalFilterTest, RejectsChangedFilterColumnOrPrimaryKey) {
  auto filter = makeComparison("device", "d1");
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(validate(filter.get()), TSDB_CODE_SUCCESS);
  const auto json = save(filter.get());
  tstrncpy(columns_[1].colName, "renamed", sizeof(columns_[1].colName));
  EXPECT_EQ(validateSaved(json), TSDB_CODE_EXT_SCHEMA_CHANGED);
  tstrncpy(columns_[1].colName, "device", sizeof(columns_[1].colName));
  tstrncpy(columns_[1].extTypeName, "VARCHAR(1)", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validateSaved(json), TSDB_CODE_EXT_SCHEMA_CHANGED);
  tstrncpy(columns_[1].extTypeName, "INT", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validateSaved(json), TSDB_CODE_EXT_SCHEMA_CHANGED);
  tstrncpy(columns_[1].extTypeName, "UNSUPPORTED_TYPE", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validateSaved(json), TSDB_CODE_EXT_SCHEMA_CHANGED);
  tstrncpy(columns_[1].extTypeName, "VARCHAR(16)", sizeof(columns_[1].extTypeName));
  columns_[2].isRemotePrimaryKey = true;
  EXPECT_EQ(validateSaved(json), TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER);
}

TEST_P(ExternalFilterTest, RejectsInvalidSavedJsonAndUnsupportedSource) {
  EXPECT_EQ(validateSaved("invalid"), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(GetParam(), &meta_, "{}", 3, nullptr, nullptr, message_,
                                                   sizeof(message_), &tsIndex_),
            TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(GetParam(), &meta_, nullptr, 1, nullptr, nullptr, message_,
                                                   sizeof(message_), &tsIndex_),
            TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(GetParam(), &meta_, nullptr, -1, nullptr, nullptr, message_,
                                                   sizeof(message_), &tsIndex_),
            TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  auto filter = makeComparison("device", "d1");
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(nodesValidateExternalFilter(EXT_SOURCE_INFLUXDB, &meta_, filter.get(), nullptr, nullptr, message_,
                                        sizeof(message_), &tsIndex_),
            TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED);
  const auto json = save(filter.get());
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(EXT_SOURCE_INFLUXDB, &meta_, json.c_str(), json.size(), nullptr,
                                                   nullptr, message_, sizeof(message_), &tsIndex_),
            TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED);
}

TEST_P(ExternalFilterTest, PreservesNewBindingColumnAndLiteralErrors) {
  auto missing = makeComparison("missing", "d1");
  ASSERT_NE(missing, nullptr);
  EXPECT_EQ(validate(missing.get()), TSDB_CODE_EXT_COLUMN_NOT_EXIST);

  auto tooLong = makeComparison("device", "d1");
  ASSERT_NE(tooLong, nullptr);
  tstrncpy(columns_[1].extTypeName, "VARCHAR(1)", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validate(tooLong.get()), TSDB_CODE_PAR_VALUE_TOO_LONG);

  auto wrongType = makeComparison("device", "d1");
  ASSERT_NE(wrongType, nullptr);
  tstrncpy(columns_[1].extTypeName, "INT", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validate(wrongType.get()), TSDB_CODE_PAR_WRONG_VALUE_TYPE);

  auto unsupported = makeComparison("device", "d1");
  ASSERT_NE(unsupported, nullptr);
  tstrncpy(columns_[1].extTypeName, "UNSUPPORTED_TYPE", sizeof(columns_[1].extTypeName));
  EXPECT_EQ(validate(unsupported.get()), TSDB_CODE_EXT_TYPE_NOT_MAPPABLE);
}

TEST_P(ExternalFilterTest, PreservesInvalidParameterError) {
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(GetParam(), nullptr, nullptr, 0, nullptr, nullptr, message_,
                                                   sizeof(message_), &tsIndex_),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(nodesValidateExternalFilterCurrentMeta(GetParam(), &meta_, nullptr, 0, nullptr, nullptr, message_,
                                                   sizeof(message_), nullptr),
            TSDB_CODE_INVALID_PARA);
}

INSTANTIATE_TEST_SUITE_P(RelationalSources, ExternalFilterTest,
                         testing::Values(EXT_SOURCE_MYSQL, EXT_SOURCE_POSTGRESQL));

class ExternalFilterMetaCloneTest : public testing::TestWithParam<ENodeType> {};

TEST_P(ExternalFilterMetaCloneTest, OwnsClonedConditions) {
  SNode* pNode = nullptr;
  ASSERT_EQ(nodesMakeNode(GetParam(), &pNode), TSDB_CODE_SUCCESS);
  NodePtr original(pNode, nodesDestroyNode);
  auto* pMeta = static_cast<STableMeta*>(
      taosMemoryCalloc(1, sizeof(STableMeta) + 3 * sizeof(SSchema) + 3 * sizeof(SColRef)));
  ASSERT_NE(pMeta, nullptr);
  switch (GetParam()) {
    case QUERY_NODE_REAL_TABLE:
      reinterpret_cast<SRealTableNode*>(pNode)->pMeta = pMeta;
      break;
    case QUERY_NODE_VIRTUAL_TABLE:
      reinterpret_cast<SVirtualTableNode*>(pNode)->pMeta = pMeta;
      break;
    case QUERY_NODE_PLACE_HOLDER_TABLE:
      reinterpret_cast<SPlaceHolderTableNode*>(pNode)->pMeta = pMeta;
      break;
    default:
      FAIL() << "Unexpected table node type";
  }
  pMeta->tableType = TSDB_VIRTUAL_CHILD_TABLE;
  pMeta->tableInfo.numOfColumns = 2;
  pMeta->tableInfo.numOfTags = 1;
  pMeta->numOfColRefs = 2;
  pMeta->numOfTagRefs = 1;
  pMeta->colRef = reinterpret_cast<SColRef*>(pMeta->schema + 3);
  pMeta->tagRef = pMeta->colRef + 2;
  pMeta->colRef[1].filterCondJson = taosStrdup("{\"filter\":1}");
  pMeta->colRef[1].filterCondLen = strlen("{\"filter\":1}");
  pMeta->colRef[1].tagCondJson = taosStrdup("{\"tag\":1}");
  pMeta->colRef[1].tagCondLen = strlen("{\"tag\":1}");
  pMeta->tagRef[0].tagCondJson = taosStrdup("{\"tag\":2}");
  pMeta->tagRef[0].tagCondLen = strlen("{\"tag\":2}");
  pMeta->seriesEntries = static_cast<SSeriesEntry*>(taosMemoryCalloc(1, sizeof(SSeriesEntry)));
  ASSERT_NE(pMeta->seriesEntries, nullptr);
  pMeta->numOfSeries = 1;
  pMeta->seriesEntries[0].tagCondJson = taosStrdup("{\"series\":1}");
  pMeta->seriesEntries[0].tagCondLen = strlen("{\"series\":1}");

  SNode* pCopy = nullptr;
  ASSERT_EQ(nodesCloneNode(pNode, &pCopy), TSDB_CODE_SUCCESS);
  NodePtr copy(pCopy, nodesDestroyNode);
  STableMeta* pClonedMeta = nullptr;
  switch (GetParam()) {
    case QUERY_NODE_REAL_TABLE:
      pClonedMeta = reinterpret_cast<SRealTableNode*>(pCopy)->pMeta;
      break;
    case QUERY_NODE_VIRTUAL_TABLE:
      pClonedMeta = reinterpret_cast<SVirtualTableNode*>(pCopy)->pMeta;
      break;
    case QUERY_NODE_PLACE_HOLDER_TABLE:
      pClonedMeta = reinterpret_cast<SPlaceHolderTableNode*>(pCopy)->pMeta;
      break;
    default:
      FAIL() << "Unexpected table node type";
  }
  ASSERT_NE(pClonedMeta, nullptr);
  ASSERT_NE(pClonedMeta->colRef[1].filterCondJson, pMeta->colRef[1].filterCondJson);
  ASSERT_NE(pClonedMeta->colRef[1].tagCondJson, pMeta->colRef[1].tagCondJson);
  ASSERT_NE(pClonedMeta->tagRef[0].tagCondJson, pMeta->tagRef[0].tagCondJson);
  ASSERT_NE(pClonedMeta->seriesEntries, pMeta->seriesEntries);
  ASSERT_NE(pClonedMeta->seriesEntries[0].tagCondJson, pMeta->seriesEntries[0].tagCondJson);
  original.reset();
  EXPECT_STREQ(pClonedMeta->colRef[1].filterCondJson, "{\"filter\":1}");
  EXPECT_EQ(pClonedMeta->colRef[1].filterCondLen, strlen("{\"filter\":1}"));
  EXPECT_STREQ(pClonedMeta->colRef[1].tagCondJson, "{\"tag\":1}");
  EXPECT_STREQ(pClonedMeta->tagRef[0].tagCondJson, "{\"tag\":2}");
  EXPECT_STREQ(pClonedMeta->seriesEntries[0].tagCondJson, "{\"series\":1}");
}

INSTANTIATE_TEST_SUITE_P(TableNodes, ExternalFilterMetaCloneTest,
                        testing::Values(QUERY_NODE_REAL_TABLE, QUERY_NODE_VIRTUAL_TABLE,
                                        QUERY_NODE_PLACE_HOLDER_TABLE));

class TableMetaDestroyTest : public testing::TestWithParam<ENodeType> {
 protected:
  static void SetUpTestSuite() { ASSERT_EQ(nodesInitAllocatorSet(), TSDB_CODE_SUCCESS); }
  static void TearDownTestSuite() { nodesDestroyAllocatorSet(); }

  void SetUp() override {
    ASSERT_EQ(nodesCreateAllocator(1, 4096, &allocatorId_), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesAcquireAllocator(allocatorId_), TSDB_CODE_SUCCESS);
    acquired_ = true;
  }

  void TearDown() override {
    if (acquired_) {
      EXPECT_EQ(nodesReleaseAllocator(allocatorId_), TSDB_CODE_SUCCESS);
    }
    nodesDestroyAllocator(allocatorId_);
  }

  int64_t allocatorId_ = 0;
  bool    acquired_ = false;
};

TEST_P(TableMetaDestroyTest, ClearsMetadataBeforeRepeatedCleanup) {
  for (int8_t tableType : {TSDB_CHILD_TABLE, TSDB_VIRTUAL_CHILD_TABLE}) {
    SCOPED_TRACE(static_cast<int32_t>(tableType));
    SNode* pNode = nullptr;
    ASSERT_EQ(nodesMakeNode(GetParam(), &pNode), TSDB_CODE_SUCCESS);
    NodePtr node(pNode, nodesDestroyNode);
    STableMeta** ppMeta = nullptr;
    switch (GetParam()) {
      case QUERY_NODE_REAL_TABLE:
        ppMeta = &reinterpret_cast<SRealTableNode*>(pNode)->pMeta;
        break;
      case QUERY_NODE_VIRTUAL_TABLE:
        ppMeta = &reinterpret_cast<SVirtualTableNode*>(pNode)->pMeta;
        break;
      case QUERY_NODE_PLACE_HOLDER_TABLE:
        ppMeta = &reinterpret_cast<SPlaceHolderTableNode*>(pNode)->pMeta;
        break;
      case QUERY_NODE_DESCRIBE_STMT:
        ppMeta = &reinterpret_cast<SDescribeStmt*>(pNode)->pMeta;
        break;
      default:
        FAIL() << "Unexpected metadata-owning node type";
    }
    *ppMeta = static_cast<STableMeta*>(taosMemoryCalloc(1, sizeof(STableMeta) + sizeof(SColRef)));
    ASSERT_NE(*ppMeta, nullptr);
    (*ppMeta)->tableType = tableType;
    if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
      (*ppMeta)->numOfColRefs = 1;
      (*ppMeta)->colRef = reinterpret_cast<SColRef*>((*ppMeta)->schema);
      (*ppMeta)->colRef[0].filterCondJson = taosStrdup("{\"filter\":1}");
      ASSERT_NE((*ppMeta)->colRef[0].filterCondJson, nullptr);
      (*ppMeta)->colRef[0].filterCondLen = strlen("{\"filter\":1}");
    }

    // Failed subquery translation can reach the same arena node twice.
    nodesDestroyNode(node.release());
    ASSERT_EQ(*ppMeta, nullptr);
    nodesDestroyNode(pNode);
  }
}

INSTANTIATE_TEST_SUITE_P(MetadataNodes, TableMetaDestroyTest,
                        testing::Values(QUERY_NODE_REAL_TABLE, QUERY_NODE_VIRTUAL_TABLE,
                                        QUERY_NODE_PLACE_HOLDER_TABLE, QUERY_NODE_DESCRIBE_STMT));

TEST(ExternalFilterMetaTest, DestroysShowCreateConditions) {
  SNode* pNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_SHOW_CREATE_TABLE_STMT, &pNode), TSDB_CODE_SUCCESS);
  NodePtr node(pNode, nodesDestroyNode);
  auto* pCfg = static_cast<STableCfg*>(taosMemoryCalloc(1, sizeof(STableCfg)));
  ASSERT_NE(pCfg, nullptr);
  reinterpret_cast<SShowCreateTableStmt*>(pNode)->pTableCfg = pCfg;
  pCfg->numOfColumns = 2;
  pCfg->pColRefs = static_cast<SColRef*>(taosMemoryCalloc(2, sizeof(SColRef)));
  ASSERT_NE(pCfg->pColRefs, nullptr);
  pCfg->pColRefs[1].filterCondJson = taosStrdup("{\"filter\":1}");
  pCfg->pColRefs[1].filterCondLen = strlen("{\"filter\":1}");
  pCfg->pColRefs[1].tagCondJson = taosStrdup("{\"tag\":1}");
  pCfg->pColRefs[1].tagCondLen = strlen("{\"tag\":1}");
  pCfg->numOfTagRefs = 1;
  pCfg->pTagRefs = static_cast<SColRef*>(taosMemoryCalloc(1, sizeof(SColRef)));
  ASSERT_NE(pCfg->pTagRefs, nullptr);
  pCfg->pTagRefs[0].tagCondJson = taosStrdup("{\"tag\":2}");
  pCfg->pTagRefs[0].tagCondLen = strlen("{\"tag\":2}");
}

TEST(ExternalFilterErrorTest, ProvidesDistinctRuntimeMessages) {
  EXPECT_STREQ(tstrerror(TSDB_CODE_VTABLE_INVALID_EXT_FILTER), "Invalid external column filter expression");
  EXPECT_STREQ(tstrerror(TSDB_CODE_VTABLE_DUPLICATE_EXT_FILTER_COLUMN), "Duplicate column in external column filter");
  EXPECT_STREQ(tstrerror(TSDB_CODE_VTABLE_INCOMPLETE_EXT_PK_FILTER),
               "External column filter must constrain every business primary key column with an equality literal");
  EXPECT_STREQ(tstrerror(TSDB_CODE_VTABLE_EXT_FILTER_NOT_SUPPORTED),
               "External column filters require a direct MySQL or PostgreSQL column reference");
}

TEST(ExternalFilterErrorTest, StoredFilterSqlFailureDoesNotTriggerClientRetry) {
  EXPECT_STREQ(tstrerror(TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED),
               "Cannot generate or execute remote SQL with the stored external column filter");
  EXPECT_FALSE(NEED_CLIENT_HANDLE_ERROR(TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED));
  EXPECT_FALSE(NEED_CLIENT_HANDLE_EXT_ERROR(TSDB_CODE_VTABLE_EXT_FILTER_SQL_FAILED));
}

TEST(NodesValueLiteralTest, PreservesStrictIntegerAndTimestampConversion) {
  auto filter = makeComparison("value", "128", OP_TYPE_EQUAL, TSDB_DATA_TYPE_BIGINT);
  ASSERT_NE(filter, nullptr);
  auto*     pValue = reinterpret_cast<SValueNode*>(reinterpret_cast<SOperatorNode*>(filter.get())->pRight);
  SDataType type = {};
  type.type = TSDB_DATA_TYPE_TINYINT;
  type.bytes = 1;
  EXPECT_EQ(nodesConvertValueLiteral(pValue, type, nullptr, true), TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  EXPECT_EQ(nodesConvertValueLiteral(pValue, type, nullptr, false), TSDB_CODE_SUCCESS);
  type.type = TSDB_DATA_TYPE_TIMESTAMP;
  type.bytes = 8;
  EXPECT_EQ(nodesConvertValueLiteral(pValue, type, nullptr, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pValue->datum.i, 128);
  EXPECT_EQ(pValue->typeData, 128);
}

TEST(NodesValueLiteralTest, RejectsInvalidStrictTimestampNumbers) {
  SDataType type = {};
  type.type = TSDB_DATA_TYPE_TIMESTAMP;
  type.bytes = sizeof(int64_t);
  for (const char* literal : {"9223372036854775808", "-9223372036854775809", ""}) {
    SCOPED_TRACE(literal);
    auto filter = makeComparison("ts", literal);
    ASSERT_NE(filter, nullptr);
    auto* value = reinterpret_cast<SValueNode*>(reinterpret_cast<SOperatorNode*>(filter.get())->pRight);
    EXPECT_EQ(nodesConvertValueLiteral(value, type, nullptr, true), TSDB_CODE_PAR_WRONG_VALUE_TYPE);
    EXPECT_EQ(nodesConvertValueLiteral(value, type, nullptr, false), TSDB_CODE_SUCCESS);
    value->node.resType = type;
  }
  for (const char* literal : {"9223372036854775807", "-9223372036854775808", "0"}) {
    SCOPED_TRACE(literal);
    auto filter = makeComparison("ts", literal);
    ASSERT_NE(filter, nullptr);
    auto* value = reinterpret_cast<SValueNode*>(reinterpret_cast<SOperatorNode*>(filter.get())->pRight);
    EXPECT_EQ(nodesConvertValueLiteral(value, type, nullptr, true), TSDB_CODE_SUCCESS);
    value->node.resType = type;
  }
}

TEST(ExternalFilterCloneTest, PreservesTypedColumnReferenceAfterSourceDestruction) {
  SNode* original = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN_REF, &original), TSDB_CODE_SUCCESS);
  NodePtr source(original, nodesDestroyNode);
  auto* ref = reinterpret_cast<SColumnRefNode*>(original);
  tstrncpy(ref->colName, "own_value", sizeof(ref->colName));
  ref->hasDataType = true;
  ref->dataType.type = TSDB_DATA_TYPE_DECIMAL;
  ref->dataType.bytes = 16;
  ref->dataType.precision = 20;
  ref->dataType.scale = 4;
  ref->pFilterCond = makeComparison("device", "d1").release();
  ASSERT_NE(ref->pFilterCond, nullptr);

  SNode* cloned = nullptr;
  ASSERT_EQ(nodesCloneNode(original, &cloned), TSDB_CODE_SUCCESS);
  NodePtr copy(cloned, nodesDestroyNode);
  auto* clonedRef = reinterpret_cast<SColumnRefNode*>(cloned);
  ASSERT_NE(clonedRef->pFilterCond, ref->pFilterCond);
  source.reset();

  EXPECT_STREQ(clonedRef->colName, "own_value");
  EXPECT_TRUE(clonedRef->hasDataType);
  EXPECT_EQ(clonedRef->dataType.type, TSDB_DATA_TYPE_DECIMAL);
  EXPECT_EQ(clonedRef->dataType.bytes, 16);
  EXPECT_EQ(clonedRef->dataType.precision, 20);
  EXPECT_EQ(clonedRef->dataType.scale, 4);
  auto* condition = reinterpret_cast<SOperatorNode*>(clonedRef->pFilterCond);
  EXPECT_STREQ(reinterpret_cast<SValueNode*>(condition->pRight)->literal, "d1");
}

TEST(NodesValueLiteralTest, ReleasesNcharBufferAfterConversionFailure) {
  auto filter = makeComparison("value", "abc", OP_TYPE_EQUAL, TSDB_DATA_TYPE_BINARY);
  ASSERT_NE(filter, nullptr);
  auto* value = reinterpret_cast<SValueNode*>(reinterpret_cast<SOperatorNode*>(filter.get())->pRight);
  SDataType type = {};
  type.type = TSDB_DATA_TYPE_NCHAR;
  type.bytes = TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  EXPECT_EQ(nodesConvertValueLiteral(value, type, nullptr, true), TSDB_CODE_PAR_WRONG_VALUE_TYPE);
  ASSERT_EQ(value->datum.p, nullptr);
  type.bytes = 3 * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  int32_t code = nodesConvertValueLiteral(value, type, nullptr, true);
  value->node.resType = type;
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(value->datum.p, nullptr);
  EXPECT_EQ(varDataLen(value->datum.p), 3 * TSDB_NCHAR_SIZE);
}

TEST(NodesValueLiteralTest, ConvertsScalarAndVariableWidthLiterals) {
  struct Case {
    const char* literal;
    int8_t      sourceType;
    int8_t      targetType;
    int32_t     bytes;
    int32_t     expectedCode;
  };
  const Case cases[] = {
      {"true", TSDB_DATA_TYPE_BOOL, TSDB_DATA_TYPE_BOOL, 1, TSDB_CODE_SUCCESS},
      {"1", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BOOL, 1, TSDB_CODE_SUCCESS},
      {"1.5", TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, 4, TSDB_CODE_SUCCESS},
      {"1.5", TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, 8, TSDB_CODE_SUCCESS},
      {"255", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_UTINYINT, 1, TSDB_CODE_SUCCESS},
      {"256", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_UTINYINT, 1, TSDB_CODE_PAR_WRONG_VALUE_TYPE},
      {"32767", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, 2, TSDB_CODE_SUCCESS},
      {"32768", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, 2, TSDB_CODE_PAR_WRONG_VALUE_TYPE},
      {"-2147483648", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, 4, TSDB_CODE_SUCCESS},
      {"-2147483649", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, 4, TSDB_CODE_PAR_WRONG_VALUE_TYPE},
      {"9223372036854775807", TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, 8, TSDB_CODE_SUCCESS},
      {"18446744073709551615", TSDB_DATA_TYPE_UBIGINT, TSDB_DATA_TYPE_UBIGINT, 8, TSDB_CODE_SUCCESS},
      {"abc", TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_VARCHAR, 3 + VARSTR_HEADER_SIZE, TSDB_CODE_SUCCESS},
      {"abcd", TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_VARCHAR, 3 + VARSTR_HEADER_SIZE, TSDB_CODE_PAR_VALUE_TOO_LONG},
      {"abc", TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_NCHAR, 12 + VARSTR_HEADER_SIZE, TSDB_CODE_SUCCESS},
      {"abc", TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_VARBINARY, 3 + VARSTR_HEADER_SIZE, TSDB_CODE_SUCCESS},
      {"1.25", TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DECIMAL64, 8, TSDB_CODE_SUCCESS},
      {"1.25", TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DECIMAL, 16, TSDB_CODE_SUCCESS},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.literal);
    SCOPED_TRACE(test.targetType);
    auto filter = makeComparison("value", test.literal, OP_TYPE_EQUAL, test.sourceType);
    ASSERT_NE(filter, nullptr);
    auto*     pValue = reinterpret_cast<SValueNode*>(reinterpret_cast<SOperatorNode*>(filter.get())->pRight);
    SDataType type = {};
    type.type = test.targetType;
    type.bytes = test.bytes;
    if (IS_DECIMAL_TYPE(type.type)) {
      type.precision = 18;
      type.scale = 2;
    }
    EXPECT_EQ(nodesConvertValueLiteral(pValue, type, nullptr, true), test.expectedCode);
    pValue->node.resType = type;
  }
}

}  // namespace
