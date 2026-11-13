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

#include "influxql.h"
#include "nodes.h"
#include "functionMgt.h"
#include "taoserror.h"
#include "tvariant.h"

namespace {
using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;
using RenderPtr = std::unique_ptr<SInfluxQLRender, decltype(&nodesDestroyInfluxQLRender)>;

SNode *makeNode(ENodeType type) {
  SNode *node = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(type, &node));
  return node;
}

void append(SNodeList **list, SNode *node) {
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(list, node));
}

SColumnNode *column(const char *name, uint8_t type = TSDB_DATA_TYPE_DOUBLE,
                    EColumnType role = COLUMN_TYPE_COLUMN) {
  auto *col = reinterpret_cast<SColumnNode *>(makeNode(QUERY_NODE_COLUMN));
  tstrncpy(col->colName, name, sizeof(col->colName));
  tstrncpy(col->node.aliasName, name, sizeof(col->node.aliasName));
  col->node.resType.type = type;
  col->node.resType.bytes = type == TSDB_DATA_TYPE_BINARY ? 100 : 8;
  col->colType = role;
  col->isPrimTs = type == TSDB_DATA_TYPE_TIMESTAMP && std::string(name) == "time";
  return col;
}

SValueNode *integer(int64_t value) {
  auto *node = reinterpret_cast<SValueNode *>(makeNode(QUERY_NODE_VALUE));
  node->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  node->node.resType.bytes = 8;
  node->datum.i = value;
  node->translate = true;
  return node;
}

SValueNode *real(double value) {
  auto *node = integer(0);
  node->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  node->datum.d = value;
  return node;
}

SValueNode *string(const char *text) {
  auto *node = integer(0);
  node->node.resType.type = TSDB_DATA_TYPE_BINARY;
  node->literal = taosStrdup(text);
  return node;
}

SOperatorNode *operation(EOperatorType type, SNode *left, SNode *right = nullptr) {
  auto *node = reinterpret_cast<SOperatorNode *>(makeNode(QUERY_NODE_OPERATOR));
  node->opType = type;
  node->pLeft = left;
  node->pRight = right;
  return node;
}

SFunctionNode *function(const char *name, const char *alias, SNode *arg) {
  auto *node = reinterpret_cast<SFunctionNode *>(makeNode(QUERY_NODE_FUNCTION));
  tstrncpy(node->functionName, name, sizeof(node->functionName));
  tstrncpy(node->node.aliasName, alias, sizeof(node->node.aliasName));
  node->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  node->node.resType.bytes = 8;
  if (arg != nullptr) append(&node->pParameterList, arg);
  return node;
}

SScanLogicNode *scan() {
  auto *node = reinterpret_cast<SScanLogicNode *>(makeNode(QUERY_NODE_LOGIC_PLAN_SCAN));
  node->scanType = SCAN_TYPE_EXTERNAL;
  node->scanRange = {INT64_MIN, INT64_MAX};
  auto *table = reinterpret_cast<SExtTableNode *>(makeNode(QUERY_NODE_EXTERNAL_TABLE));
  table->sourceType = EXT_SOURCE_INFLUXDB;
  table->capability.dialect = EXT_SQL_DIALECT_INFLUXDB_1_8;
  node->pExtTableNode = reinterpret_cast<SNode *>(table);
  append(&table->pInfluxSchema, reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY, COLUMN_TYPE_TAG)));
  append(&table->pInfluxSchema, reinterpret_cast<SNode *>(column("value")));
  append(&node->pScanCols, reinterpret_cast<SNode *>(column("value")));
  append(&node->node.pTargets, reinterpret_cast<SNode *>(column("value")));
  return node;
}

SAggLogicNode *aggregate(SScanLogicNode *leaf, const char *name = "sum") {
  auto *agg = reinterpret_cast<SAggLogicNode *>(makeNode(QUERY_NODE_LOGIC_PLAN_AGG));
  append(&agg->node.pChildren, reinterpret_cast<SNode *>(leaf));
  append(&agg->pAggFuncs, reinterpret_cast<SNode *>(function(name, "total", reinterpret_cast<SNode *>(column("value")))));
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("total")));
  return agg;
}

RenderPtr render(SLogicNode *plan) {
  SInfluxQLRender *result = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesRemotePlanToInfluxQL(plan, &result));
  return RenderPtr(result, nodesDestroyInfluxQLRender);
}

TEST(InfluxQLRender, SharedQuoteCodec) {
  char *quoted = nodesInfluxQLQuote("a\"b\\c\nd\re\tf", '"');
  ASSERT_NE(nullptr, quoted);
  EXPECT_STREQ("\"a\\\"b\\\\c\\nd\\re\\tf\"", quoted);
  taosMemoryFree(quoted);
  quoted = nodesInfluxQLQuote("a'b\\c", '\'');
  ASSERT_NE(nullptr, quoted);
  EXPECT_STREQ("'a\\'b\\\\c'", quoted);
  taosMemoryFree(quoted);
}

TEST(InfluxQLRender, RawFieldAnchoringIsPreserved) {
  NodePtr owner(reinterpret_cast<SNode *>(scan()), nodesDestroyNode);
  auto result = render(reinterpret_cast<SLogicNode *>(owner.get()));
  ASSERT_NE(nullptr, result);
  EXPECT_EQ(nullptr, result->pSelect);
  EXPECT_EQ(nullptr, result->pWhere);
  EXPECT_FALSE(result->aggregate);
  EXPECT_FALSE(result->hasTimeRange);
}

TEST(InfluxQLRender, MissingTagIsExcludedFromInequality) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_NOT_EQUAL,
      reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)), reinterpret_cast<SNode *>(string("west"))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("(\"host\" != 'west' AND \"host\" != '')", result->pWhere);
  EXPECT_EQ(1, taosArrayGetSize(result->pReferencedColumns));
}

TEST(InfluxQLRender, FoldedNcharDoesNotRequireLiteralText) {
  for (auto op : {OP_TYPE_EQUAL, OP_TYPE_NOT_EQUAL, OP_TYPE_LIKE}) {
    auto *leaf = scan();
    NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
    auto *value = integer(0);
    value->node.resType.type = TSDB_DATA_TYPE_NCHAR;
    const TdUcs4 text[] = {0x4e2d, 0x6587};
    value->node.resType.bytes = sizeof(text) + VARSTR_HEADER_SIZE;
    value->datum.p = static_cast<char *>(taosMemoryCalloc(1, value->node.resType.bytes));
    ASSERT_NE(nullptr, value->datum.p);
    varDataSetLen(value->datum.p, sizeof(text));
    memcpy(varDataVal(value->datum.p), text, sizeof(text));
    ASSERT_EQ(nullptr, value->literal);
    leaf->node.pConditions = reinterpret_cast<SNode *>(operation(op,
        reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)), reinterpret_cast<SNode *>(value)));
    auto result = render(&leaf->node);
    ASSERT_NE(nullptr, result);
    // "中文" (U+4E2D U+6587) 的 UTF-8 字节.
    // Byte escapes preserve UTF-8 regardless of the Windows code page.
    EXPECT_STREQ(op == OP_TYPE_EQUAL ? "(\"host\" = '\xE4\xB8\xAD\xE6\x96\x87')" :
                 op == OP_TYPE_NOT_EQUAL ? "(\"host\" != '\xE4\xB8\xAD\xE6\x96\x87' AND \"host\" != '')" :
                 "(\"host\" =~ /(?s)^\xE4\xB8\xAD\xE6\x96\x87$/ AND \"host\" != '')", result->pWhere);
  }
}

TEST(InfluxQLRender, TagNullUsesMissingTagToken) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_IS_NULL,
      reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("\"host\" = ''", result->pWhere);
}

TEST(InfluxQLRender, InListAndNotInList) {
  for (auto op : {OP_TYPE_IN, OP_TYPE_NOT_IN}) {
    auto *leaf = scan();
    NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
    auto *values = reinterpret_cast<SNodeListNode *>(makeNode(QUERY_NODE_NODE_LIST));
    append(&values->pNodeList, reinterpret_cast<SNode *>(string("a")));
    append(&values->pNodeList, reinterpret_cast<SNode *>(string("b")));
    leaf->node.pConditions = reinterpret_cast<SNode *>(operation(op,
        reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)), reinterpret_cast<SNode *>(values)));
    auto result = render(&leaf->node);
    ASSERT_NE(nullptr, result);
    EXPECT_STREQ(op == OP_TYPE_IN ? "((\"host\" = 'a' OR \"host\" = 'b') AND \"host\" != '')" :
        "((\"host\" != 'a' AND \"host\" != 'b') AND \"host\" != '')", result->pWhere);
  }
}

TEST(InfluxQLRender, LikeConvertsWildcardsAndEscapesRegex) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  // "中" (U+4E2D) 的 UTF-8 字节为 E4 B8 AD.
  // Use the same UTF-8 byte escapes in the input and expected regex,
  // regardless of the Windows code page.
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_LIKE,
      reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)),
      reinterpret_cast<SNode *>(string("a%_\\%.[x]/\n\xE4\xB8\xAD"))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("(\"host\" =~ /(?s)^a.*.%\\.\\[x\\]\\/\\n\xE4\xB8\xAD$/ AND \"host\" != '')", result->pWhere);
}

TEST(InfluxQLRender, DivisionRendersFloatingLiteral) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  auto *divide = operation(OP_TYPE_DIV, reinterpret_cast<SNode *>(column("value")),
                           reinterpret_cast<SNode *>(integer(2)));
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_GREATER_THAN,
      reinterpret_cast<SNode *>(divide), reinterpret_cast<SNode *>(real(3))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("((\"value\" / 2.0) > 3.0)", result->pWhere);
}

TEST(InfluxQLRender, TimeValuesUseNanoseconds) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  auto *time = column("time", TSDB_DATA_TYPE_TIMESTAMP);
  time->node.resType.precision = TSDB_TIME_PRECISION_MICRO;
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_GREATER_EQUAL,
      reinterpret_cast<SNode *>(time), reinterpret_cast<SNode *>(integer(123))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("(time >= 123000)", result->pWhere);
}

TEST(InfluxQLRender, StrictTimeComparisonsUseInclusiveNanosecondBounds) {
  const struct {
    EOperatorType op;
    int8_t precision;
    int64_t value;
    const char *expected;
    bool empty;
  } cases[] = {
    {OP_TYPE_GREATER_THAN, TSDB_TIME_PRECISION_MICRO, 123, "(time >= 123001)", false},
    {OP_TYPE_LOWER_THAN, TSDB_TIME_PRECISION_MILLI, 123, "(time <= 122999999)", false},
    {OP_TYPE_GREATER_THAN, TSDB_TIME_PRECISION_NANO, 123, "(time >= 124)", false},
    {OP_TYPE_LOWER_THAN, TSDB_TIME_PRECISION_NANO, 123, "(time <= 122)", false},
    {OP_TYPE_EQUAL, TSDB_TIME_PRECISION_NANO, 123, "(time >= 123 AND time <= 123)", false},
    {OP_TYPE_GREATER_THAN, TSDB_TIME_PRECISION_NANO, INT64_MAX - 1,
      "(time >= -9223372036854775806 AND time <= -9223372036854775807)", true},
    {OP_TYPE_LOWER_THAN, TSDB_TIME_PRECISION_NANO, INT64_MIN + 2,
      "(time >= -9223372036854775806 AND time <= -9223372036854775807)", true},
    {OP_TYPE_GREATER_EQUAL, TSDB_TIME_PRECISION_NANO, INT64_MIN + 2,
      "(time >= -9223372036854775806)", false},
    {OP_TYPE_LOWER_EQUAL, TSDB_TIME_PRECISION_NANO, INT64_MAX - 1,
      "(time <= 9223372036854775806)", false},
  };
  for (const auto &test : cases) {
    auto *leaf = scan();
    NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
    auto *time = column("time", TSDB_DATA_TYPE_TIMESTAMP);
    time->node.resType.precision = test.precision;
    leaf->node.pConditions = reinterpret_cast<SNode *>(operation(test.op,
        reinterpret_cast<SNode *>(time), reinterpret_cast<SNode *>(integer(test.value))));
    auto result = render(&leaf->node);
    ASSERT_NE(nullptr, result);
    EXPECT_STREQ(test.expected, result->pWhere);
    EXPECT_EQ(test.empty, result->hasTimeRange && result->scanRange.skey > result->scanRange.ekey);
  }
}

TEST(InfluxQLRender, ImpossibleTimeRangeIsEmpty) {
  auto *leaf = scan();
  NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
  leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_GREATER_THAN,
      reinterpret_cast<SNode *>(column("time", TSDB_DATA_TYPE_TIMESTAMP)),
      reinterpret_cast<SNode *>(integer(INT64_MAX))));
  auto result = render(&leaf->node);
  ASSERT_NE(nullptr, result);
  EXPECT_TRUE(result->hasTimeRange);
  EXPECT_GT(result->scanRange.skey, result->scanRange.ekey);
}

TEST(InfluxQLRender, RejectsUnsafeArithmeticAndEmptyTagLiterals) {
  for (int scenario = 0; scenario < 4; ++scenario) {
    auto *leaf = scan();
    NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
    if (scenario == 0) {
      leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_DIV,
          reinterpret_cast<SNode *>(column("value")), reinterpret_cast<SNode *>(integer(0))));
    } else if (scenario == 1) {
      leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_ADD,
          reinterpret_cast<SNode *>(column("value", TSDB_DATA_TYPE_BIGINT)),
          reinterpret_cast<SNode *>(integer(2))));
    } else if (scenario == 2) {
      leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_EQUAL,
          reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)), reinterpret_cast<SNode *>(string(""))));
    } else {
      leaf->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_IN, nullptr, nullptr));
    }
    SInfluxQLRender *result = nullptr;
    EXPECT_EQ(TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, nodesRemotePlanToInfluxQL(&leaf->node, &result));
    EXPECT_EQ(nullptr, result);
  }
}

TEST(InfluxQLRender, RejectsTimeAndMixedRoleDisjunctions) {
  for (bool time : {false, true}) {
    auto *leaf = scan();
    NodePtr owner(reinterpret_cast<SNode *>(leaf), nodesDestroyNode);
    auto *condition = reinterpret_cast<SLogicConditionNode *>(makeNode(QUERY_NODE_LOGIC_CONDITION));
    condition->condType = LOGIC_COND_TYPE_OR;
    append(&condition->pParameterList, reinterpret_cast<SNode *>(operation(OP_TYPE_EQUAL,
        reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)), reinterpret_cast<SNode *>(string("a")))));
    append(&condition->pParameterList, reinterpret_cast<SNode *>(operation(OP_TYPE_GREATER_THAN,
        reinterpret_cast<SNode *>(column(time ? "time" : "value", time ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_DOUBLE)),
        reinterpret_cast<SNode *>(integer(1)))));
    leaf->node.pConditions = reinterpret_cast<SNode *>(condition);
    SInfluxQLRender *result = nullptr;
    EXPECT_EQ(TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, nodesRemotePlanToInfluxQL(&leaf->node, &result));
    EXPECT_EQ(nullptr, result);
  }
}

TEST(InfluxQLRender, AggregatesExposeTypedOutputSlots) {
  for (const char *name : {"count", "sum", "min", "max", "avg", "spread", "stddev_samp"}) {
    auto *agg = aggregate(scan(), name);
    NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
    auto result = render(&agg->node);
    ASSERT_NE(nullptr, result);
    std::string expected = std::string(name == std::string("avg") ? "MEAN" :
        (name == std::string("stddev_samp") ? "STDDEV" : name));
    for (char &c : expected) c = static_cast<char>(toupper(c));
    expected += "(\"value\") AS \"total\"";
    if (name == std::string("stddev_samp")) {
      expected += ", COUNT(\"value\") AS \"__fq_sample_0\"";
    }
    EXPECT_STREQ(expected.c_str(), result->pSelect);
    ASSERT_EQ(1, taosArrayGetSize(result->pOutputs));
    auto *output = static_cast<SInfluxQLOutput *>(taosArrayGet(result->pOutputs, 0));
    EXPECT_STREQ("total", output->name);
    EXPECT_EQ(TSDB_DATA_TYPE_DOUBLE, output->dataType.type);
    EXPECT_EQ(name == std::string("count"), output->emptyCount);
    EXPECT_STREQ(name == std::string("stddev_samp") ? "__fq_sample_0" : "", output->countName);
    EXPECT_TRUE(result->aggregate);
    EXPECT_FALSE(result->grouped);
  }
}

TEST(InfluxQLRender, ExplicitTagGroupingUsesSeriesMetadata) {
  auto *agg = aggregate(scan());
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  append(&agg->pGroupKeys, reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)));
  nodesDestroyList(agg->node.pTargets);
  agg->node.pTargets = nullptr;
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)));
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("total")));
  auto result = render(&agg->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("\"host\" fill(none)", result->pGroupBy);
  EXPECT_STREQ("SUM(\"value\") AS \"total\"", result->pSelect);
  EXPECT_TRUE(result->tagGrouped);
  ASSERT_EQ(2, taosArrayGetSize(result->pOutputs));
  auto *output = static_cast<SInfluxQLOutput *>(taosArrayGet(result->pOutputs, 0));
  EXPECT_EQ(INFLUXQL_OUTPUT_TAG, output->role);
  EXPECT_STREQ("host", output->sourceName);
}

TEST(InfluxQLRender, SparseGroupedFieldsHaveHiddenCountAnchors) {
  auto *leaf = scan();
  auto *table = reinterpret_cast<SExtTableNode *>(leaf->pExtTableNode);
  append(&table->pInfluxSchema, reinterpret_cast<SNode *>(column("other_field")));
  auto *agg = aggregate(leaf);
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  append(&agg->pGroupKeys, reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)));
  nodesDestroyList(agg->node.pTargets);
  agg->node.pTargets = nullptr;
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("host", TSDB_DATA_TYPE_BINARY)));
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("total")));
  auto result = render(&agg->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("SUM(\"value\") AS \"total\", COUNT(\"other_field\") AS \"__fq_sparse_0\"", result->pSelect);
  EXPECT_EQ(2, taosArrayGetSize(result->pOutputs));
  EXPECT_EQ(3, taosArrayGetSize(result->pReferencedColumns));
}

TEST(InfluxQLRender, SparseUngroupedInputHasHiddenCountAnchors) {
  auto *leaf = scan();
  auto *table = reinterpret_cast<SExtTableNode *>(leaf->pExtTableNode);
  append(&table->pInfluxSchema, reinterpret_cast<SNode *>(column("other_field")));
  auto *agg = aggregate(leaf);
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  auto result = render(&agg->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("SUM(\"value\") AS \"total\", COUNT(\"other_field\") AS \"__fq_sparse_0\"", result->pSelect);
  EXPECT_FALSE(result->grouped);
  EXPECT_EQ(1, taosArrayGetSize(result->pOutputs));
  EXPECT_EQ(2, taosArrayGetSize(result->pReferencedColumns));
}

TEST(InfluxQLRender, SampleCountAliasAvoidsVisibleSlotNames) {
  auto *agg = aggregate(scan(), "stddev_samp");
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  auto *func = reinterpret_cast<SFunctionNode *>(nodesListGetNode(agg->pAggFuncs, 0));
  tstrncpy(func->node.aliasName, "__fq_sample_0", sizeof(func->node.aliasName));
  auto *target = reinterpret_cast<SColumnNode *>(nodesListGetNode(agg->node.pTargets, 0));
  tstrncpy(target->colName, "__fq_sample_0", sizeof(target->colName));
  auto result = render(&agg->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("STDDEV(\"value\") AS \"__fq_sample_0\", COUNT(\"value\") AS \"__fq_sample_1\"", result->pSelect);
  ASSERT_EQ(1, taosArrayGetSize(result->pOutputs));
  const auto *output = static_cast<SInfluxQLOutput *>(taosArrayGet(result->pOutputs, 0));
  EXPECT_STREQ("__fq_sample_1", output->countName);
}

TEST(InfluxQLRender, WindowGroupsHaveExplicitTimeRangeContract) {
  auto *agg = aggregate(scan());
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  auto *bucket = function("_fq_time_bucket", "_wstart", reinterpret_cast<SNode *>(column("time", TSDB_DATA_TYPE_TIMESTAMP)));
  append(&bucket->pParameterList, reinterpret_cast<SNode *>(integer(1000)));
  append(&bucket->pParameterList, reinterpret_cast<SNode *>(integer(TSDB_TIME_PRECISION_MILLI)));
  append(&bucket->pParameterList, reinterpret_cast<SNode *>(integer(10)));
  bucket->funcType = FUNCTION_TYPE_FQ_TIME_BUCKET;
  bucket->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  append(&agg->pGroupKeys, reinterpret_cast<SNode *>(bucket));
  nodesDestroyList(agg->node.pTargets);
  agg->node.pTargets = nullptr;
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("_wstart", TSDB_DATA_TYPE_TIMESTAMP, COLUMN_TYPE_WINDOW_START)));
  append(&agg->node.pTargets, reinterpret_cast<SNode *>(column("total")));
  auto result = render(&agg->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("time(1000000000ns, 10000000ns) fill(none)", result->pGroupBy);
  EXPECT_TRUE(result->timeGrouped);
  EXPECT_EQ(1000, result->windowStep);
  EXPECT_EQ(INT64_MIN, result->scanRange.skey);
  EXPECT_EQ(INT64_MAX, result->scanRange.ekey);
  EXPECT_EQ(INFLUXQL_OUTPUT_TIME, static_cast<SInfluxQLOutput *>(taosArrayGet(result->pOutputs, 0))->role);
}

TEST(InfluxQLRender, TimeSortAndTopLimit) {
  auto *sort = reinterpret_cast<SSortLogicNode *>(makeNode(QUERY_NODE_LOGIC_PLAN_SORT));
  NodePtr owner(reinterpret_cast<SNode *>(sort), nodesDestroyNode);
  append(&sort->node.pChildren, reinterpret_cast<SNode *>(scan()));
  auto *order = reinterpret_cast<SOrderByExprNode *>(makeNode(QUERY_NODE_ORDER_BY_EXPR));
  order->pExpr = reinterpret_cast<SNode *>(column("time", TSDB_DATA_TYPE_TIMESTAMP));
  order->order = ORDER_DESC;
  append(&sort->pSortKeys, reinterpret_cast<SNode *>(order));
  auto *limit = reinterpret_cast<SLimitNode *>(makeNode(QUERY_NODE_LIMIT));
  limit->limit = integer(4);
  limit->offset = integer(2);
  sort->node.pLimit = reinterpret_cast<SNode *>(limit);
  auto result = render(&sort->node);
  ASSERT_NE(nullptr, result);
  EXPECT_STREQ("time DESC", result->pOrderBy);
  EXPECT_STREQ("4 OFFSET 2", result->pLimit);
  EXPECT_TRUE(result->hasLimit);
  EXPECT_EQ(4, result->limit);
  EXPECT_EQ(2, result->offset);
}

TEST(InfluxQLRender, RejectsHavingAndUnsupportedFunctions) {
  auto *agg = aggregate(scan(), "variance");
  NodePtr owner(reinterpret_cast<SNode *>(agg), nodesDestroyNode);
  SInfluxQLRender *result = nullptr;
  EXPECT_EQ(TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, nodesRemotePlanToInfluxQL(&agg->node, &result));
  EXPECT_EQ(nullptr, result);
  agg->node.pConditions = reinterpret_cast<SNode *>(operation(OP_TYPE_GREATER_THAN,
      reinterpret_cast<SNode *>(column("total")), reinterpret_cast<SNode *>(integer(0))));
  EXPECT_EQ(TSDB_CODE_EXT_SYNTAX_UNSUPPORTED, nodesRemotePlanToInfluxQL(&agg->node, &result));
  EXPECT_EQ(nullptr, result);
}
}  // namespace
