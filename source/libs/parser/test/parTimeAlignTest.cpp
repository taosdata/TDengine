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

// Grammar/AST tests for the TABLE(TIME_ALIGN(...)) table function.
//
// These cover the parse stage only: the translate stage needs virtual-table
// metadata, which the unit-test mock catalog does not provide.  The default
// values and argument mapping asserted here are what the translator later
// consumes, so parser regressions surface without a running cluster.

#define ALLOW_FORBID_FUNC

#include <array>
#include <cstring>
#include <memory>

#include "parTestUtil.h"
#include "parInt.h"

using namespace std;

namespace ParserTest {

namespace {

using QueryGuard = unique_ptr<SQuery, decltype(&qDestroyQuery)>;

QueryGuard parseTimeAlignSql(const char* pSql, int32_t* pCode) {
  initKeywordsTableOnce();
  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "test";
  cxt.pUser = "root";
  cxt.isSuperUser = true;
  cxt.enableSysInfo = true;
  cxt.privInfo = UINT16_MAX;
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();
  cxt.svrVer = "3.0.0.0";

  SQuery* pQuery = nullptr;
  *pCode = parse(&cxt, &pQuery);
  return QueryGuard(pQuery, qDestroyQuery);
}

STimeAlignTableNode* timeAlignTableOf(QueryGuard& query) {
  if (!query || nullptr == query->pRoot || QUERY_NODE_SELECT_STMT != nodeType(query->pRoot)) {
    return nullptr;
  }
  SSelectStmt* pSelect = reinterpret_cast<SSelectStmt*>(query->pRoot);
  if (nullptr == pSelect->pFromTable || QUERY_NODE_TIME_ALIGN_TABLE != nodeType(pSelect->pFromTable)) {
    return nullptr;
  }
  return reinterpret_cast<STimeAlignTableNode*>(pSelect->pFromTable);
}

void expectParseError(const char* pSql) {
  int32_t    code = TSDB_CODE_SUCCESS;
  QueryGuard query = parseTimeAlignSql(pSql, &code);
  EXPECT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code) << pSql;
}

}  // namespace

TEST(ParserTimeAlignTest, defaultArguments) {
  int32_t    code = TSDB_CODE_SUCCESS;
  QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE db1.vt1))", &code);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  STimeAlignTableNode* pTable = timeAlignTableOf(query);
  ASSERT_NE(nullptr, pTable);
  EXPECT_STREQ("db1", pTable->dbName);
  EXPECT_STREQ("vt1", pTable->tbName);
  EXPECT_STREQ("vt1", pTable->table.tableAlias);
  EXPECT_FALSE(pTable->outputRaw);
  EXPECT_EQ(nullptr, pTable->columnList);
}

TEST(ParserTimeAlignTest, unqualifiedTableUsesCurrentDb) {
  int32_t    code = TSDB_CODE_SUCCESS;
  QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt2))", &code);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  STimeAlignTableNode* pTable = timeAlignTableOf(query);
  ASSERT_NE(nullptr, pTable);
  EXPECT_STREQ("test", pTable->dbName);
  EXPECT_STREQ("vt2", pTable->tbName);
}

TEST(ParserTimeAlignTest, outputRawBooleanArguments) {
  {
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, TRUE))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_TRUE(pTable->outputRaw);
    EXPECT_EQ(nullptr, pTable->columnList);
  }
  {
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, false))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_FALSE(pTable->outputRaw);
    EXPECT_EQ(nullptr, pTable->columnList);
  }
}

TEST(ParserTimeAlignTest, columnDefinitionArguments) {
  {
    // String-only form: output_raw keeps its default (FALSE).
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query =
        parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, 'voltage:PREV:_a'))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_FALSE(pTable->outputRaw);
    ASSERT_NE(nullptr, pTable->columnList);
    EXPECT_STREQ("voltage:PREV:_a", pTable->columnList);
  }
  {
    // Boolean + column definition.
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query = parseTimeAlignSql(
        "SELECT * FROM TABLE(time_align(TABLE db1.vt1, TRUE, '*:PREV:_a;voltage:PREV'))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_TRUE(pTable->outputRaw);
    ASSERT_NE(nullptr, pTable->columnList);
    EXPECT_STREQ("*:PREV:_a;voltage:PREV", pTable->columnList);
  }
  {
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query =
        parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, FALSE, 'voltage:PREV:_a'))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_FALSE(pTable->outputRaw);
    ASSERT_NE(nullptr, pTable->columnList);
    EXPECT_STREQ("voltage:PREV:_a", pTable->columnList);
  }
  {
    // An explicit empty string stays distinguishable from an omitted
    // definition: NULL means the default, "" is rejected by the translator.
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, ''))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    ASSERT_NE(nullptr, pTable->columnList);
    EXPECT_STREQ("", pTable->columnList);
  }
  {
    int32_t    code = TSDB_CODE_SUCCESS;
    QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE vt, TRUE, ''))", &code);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    STimeAlignTableNode* pTable = timeAlignTableOf(query);
    ASSERT_NE(nullptr, pTable);
    EXPECT_TRUE(pTable->outputRaw);
    ASSERT_NE(nullptr, pTable->columnList);
    EXPECT_STREQ("", pTable->columnList);
  }
}

TEST(ParserTimeAlignTest, tableAlias) {
  int32_t    code = TSDB_CODE_SUCCESS;
  QueryGuard query = parseTimeAlignSql("SELECT * FROM TABLE(time_align(TABLE db1.vt1)) AS align_vt", &code);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  STimeAlignTableNode* pTable = timeAlignTableOf(query);
  ASSERT_NE(nullptr, pTable);
  EXPECT_STREQ("db1", pTable->dbName);
  EXPECT_STREQ("vt1", pTable->tbName);
  EXPECT_STREQ("align_vt", pTable->table.tableAlias);
}

TEST(ParserTimeAlignTest, quotedTablePathIsUnescaped) {
  // Regression: the database token used to be copied verbatim, so a quoted
  // qualifier (`db`.`tbl`) kept its backticks and the later metadata lookup
  // reported "Database does not exist" (defect 7179993062).
  // The parser un-escapes identifiers in place, so the SQL must be writable.
  int32_t code = TSDB_CODE_SUCCESS;
  char    sql[] = "SELECT * FROM TABLE(time_align(TABLE `db1`.`vt-1`))";
  QueryGuard query = parseTimeAlignSql(sql, &code);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  STimeAlignTableNode* pTable = timeAlignTableOf(query);
  ASSERT_NE(nullptr, pTable);
  EXPECT_STREQ("db1", pTable->dbName);
  EXPECT_STREQ("vt-1", pTable->tbName);
  EXPECT_STREQ("vt-1", pTable->table.tableAlias);

  // Escaped backticks inside a quoted identifier decode to a literal backtick.
  code = TSDB_CODE_SUCCESS;
  char escapedSql[] = "SELECT * FROM TABLE(time_align(TABLE `d``b`.`v``t`))";
  QueryGuard escaped = parseTimeAlignSql(escapedSql, &code);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  pTable = timeAlignTableOf(escaped);
  ASSERT_NE(nullptr, pTable);
  EXPECT_STREQ("d`b", pTable->dbName);
  EXPECT_STREQ("v`t", pTable->tbName);
}

TEST(ParserTimeAlignTest, rejectsInvalidArguments) {
  // Unknown table function inside TABLE(...).
  expectParseError("SELECT * FROM TABLE(other_func(TABLE vt))");
  // Missing table name.
  expectParseError("SELECT * FROM TABLE(time_align(TABLE))");
  // The second argument must be a boolean or a string literal.
  expectParseError("SELECT * FROM TABLE(time_align(TABLE vt, 123))");
  // The third argument must be a string literal.
  expectParseError("SELECT * FROM TABLE(time_align(TABLE vt, TRUE, 123))");
  expectParseError("SELECT * FROM TABLE(time_align(TABLE vt, 'a', 'b'))");
}

TEST(ParserTimeAlignTest, ClonesTriggerSource) {
  int32_t code = 0;
  auto    query = parseTimeAlignSql(
      "CREATE STREAM s INTERVAL(1s) FROM TABLE(TIME_ALIGN(TABLE vt, 'a:PREV(8s):_a')) "
         "INTO out AS SELECT _twstart, count(*) FROM %%tbname",
      &code);
  ASSERT_EQ(0, code);
  ASSERT_NE(nullptr, query);
  auto* stream = reinterpret_cast<SCreateStreamStmt*>(query->pRoot);
  ASSERT_EQ(QUERY_NODE_TIME_ALIGN_TABLE,
            nodeType(reinterpret_cast<SStreamTriggerNode*>(stream->pTrigger)->pTrigerTable));
  auto* source =
      reinterpret_cast<STimeAlignTableNode*>(reinterpret_cast<SStreamTriggerNode*>(stream->pTrigger)->pTrigerTable);
  EXPECT_STREQ("a:PREV(8s):_a", source->columnList);
  SNode* copy = nullptr;
  ASSERT_EQ(0, nodesCloneNode(query->pRoot, &copy));
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> guard(copy, nodesDestroyNode);
  auto* copiedTrigger = reinterpret_cast<SStreamTriggerNode*>(reinterpret_cast<SCreateStreamStmt*>(copy)->pTrigger);
  auto* aligned = reinterpret_cast<STimeAlignTableNode*>(copiedTrigger->pTrigerTable);
  ASSERT_NE(nullptr, aligned->pRealTableNode);
  EXPECT_NE(source->pRealTableNode, aligned->pRealTableNode);
  EXPECT_EQ(QUERY_NODE_REAL_TABLE, nodeType(aligned->pRealTableNode));
  char* json = nullptr;
  ASSERT_EQ(0, nodesNodeToString(copy, false, &json, nullptr));
  SNode*  decoded = nullptr;
  int32_t decodeCode = nodesStringToNode(json, &decoded);
  taosMemoryFree(json);
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(decoded, nodesDestroyNode);
  ASSERT_EQ(0, decodeCode);
  auto* trigger = reinterpret_cast<SStreamTriggerNode*>(reinterpret_cast<SCreateStreamStmt*>(decoded)->pTrigger);
  auto* restored = reinterpret_cast<STimeAlignTableNode*>(trigger->pTrigerTable);
  ASSERT_NE(nullptr, restored->pRealTableNode);
  EXPECT_STREQ("vt", reinterpret_cast<STableNode*>(restored->pRealTableNode)->tableName);
  EXPECT_STREQ(source->columnList, restored->columnList);
}

}  // namespace ParserTest
