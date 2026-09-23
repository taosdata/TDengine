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
#include <tuple>

extern "C" {
#include "command.h"
#include "parInt.h"
#include "tdatablock.h"
#include "tglobal.h"
}

namespace {

using QueryPtr = std::unique_ptr<SQuery, decltype(&qDestroyQuery)>;
using FilterCase = std::tuple<int8_t, EExtSourceType>;

class ShowCreateFilterTest : public testing::TestWithParam<FilterCase> {
 protected:
  static void SetUpTestSuite() { ASSERT_EQ(qInitKeywordsTable(), TSDB_CODE_SUCCESS); }
  static void TearDownTestSuite() { taosCleanupKeywordsTable(); }

  void SetUp() override {
    cfg_.tableType = std::get<0>(GetParam());
    cfg_.numOfColumns = 3;
    cfg_.numOfTags = 1;
    cfg_.pSchemas = schemas_;
    cfg_.pColRefs = refs_;
    tstrncpy(cfg_.stbName, "stb", sizeof(cfg_.stbName));
    const char* names[] = {"ts", "v1", "v2", "group_id"};
    for (int32_t i = 0; i < 4; ++i) {
      schemas_[i].colId = i + 1;
      schemas_[i].type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT;
      schemas_[i].bytes = tDataTypes[schemas_[i].type].bytes;
      tstrncpy(schemas_[i].name, names[i], sizeof(schemas_[i].name));
    }
    for (int32_t i = 1; i < 3; ++i) {
      refs_[i].id = schemas_[i].colId;
      refs_[i].hasRef = true;
      refs_[i].refType = 1;
      tstrncpy(refs_[i].refSourceName, sourceName(), sizeof(refs_[i].refSourceName));
      tstrncpy(refs_[i].refDbName, "db", sizeof(refs_[i].refDbName));
      tstrncpy(refs_[i].refTableName, "t", sizeof(refs_[i].refTableName));
      tstrncpy(refs_[i].refColName, "value", sizeof(refs_[i].refColName));
    }
    SArray* pTags = taosArrayInit(1, sizeof(STagVal));
    ASSERT_NE(pTags, nullptr);
    STagVal tag = {};
    tag.cid = schemas_[3].colId;
    tag.type = TSDB_DATA_TYPE_INT;
    tag.i64 = 7;
    ASSERT_NE(taosArrayPush(pTags, &tag), nullptr);
    int32_t code = tTagNew(pTags, 1, false, reinterpret_cast<STag**>(&cfg_.pTags));
    taosArrayDestroy(pTags);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    meta_.numOfCols = 7;
    meta_.pCols = columns_;
    const char* colNames[] = {"ts", "device", "value", "enabled", "price", "ratio", "dev`ice"};
    const char* types[] = {"TIMESTAMP", "VARCHAR(60000)", "INT", "BOOL", "DECIMAL(38,8)", "DOUBLE", "VARCHAR(32)"};
    for (int32_t i = 0; i < meta_.numOfCols; ++i) {
      tstrncpy(columns_[i].colName, colNames[i], sizeof(columns_[i].colName));
      tstrncpy(columns_[i].extTypeName, types[i], sizeof(columns_[i].extTypeName));
      tstrncpy(columns_[i].extCharsetName, "ascii", sizeof(columns_[i].extCharsetName));
      columns_[i].isRemotePrimaryKey = i < 2;
    }
    if (sourceType() == EXT_SOURCE_MYSQL) {
      tstrncpy(columns_[0].extTypeName, "DATETIME", sizeof(columns_[0].extTypeName));
    } else {
      tstrncpy(columns_[5].extTypeName, "DOUBLE PRECISION", sizeof(columns_[5].extTypeName));
    }
  }

  void TearDown() override {
    for (auto& ref : refs_) {
      taosMemoryFree(ref.filterCondJson);
      taosMemoryFree(ref.tagCondJson);
    }
    taosMemoryFree(series_.tagCondJson);
    taosMemoryFree(cfg_.pTags);
    tsMaxSQLLength = maxSqlLength_;
  }

  EExtSourceType sourceType() const { return std::get<1>(GetParam()); }
  const char*    sourceName() const { return sourceType() == EXT_SOURCE_MYSQL ? "mysql_src" : "pg_src"; }

  QueryPtr parseSQL(const std::string& sql) {
    SParseContext context = {};
    context.db = "test";
    context.pSql = sql.c_str();
    context.sqlLen = sql.size();
    context.pMsg = message_;
    context.msgLen = sizeof(message_);
    SQuery* pQuery = nullptr;
    int32_t code = parse(&context, &pQuery);
    QueryPtr query(pQuery, qDestroyQuery);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS) << message_ << ": " << sql.substr(0, 256);
    if (TSDB_CODE_SUCCESS != code) {
      return QueryPtr(nullptr, qDestroyQuery);
    }
    return query;
  }

  SNode* filterOf(SQuery* pQuery, int32_t colIndex) {
    if (nodeType(pQuery->pRoot) == QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT) {
      auto* pStmt = reinterpret_cast<SCreateVTableStmt*>(pQuery->pRoot);
      auto* pCol = reinterpret_cast<SColumnDefNode*>(nodesListGetNode(pStmt->pCols, colIndex));
      return reinterpret_cast<SColumnOptions*>(pCol->pOptions)->pFilterCond;
    }
    auto* pStmt = reinterpret_cast<SCreateVSubTableStmt*>(pQuery->pRoot);
    auto* pRef = reinterpret_cast<SColumnRefNode*>(nodesListGetNode(pStmt->pSpecificColRefs, colIndex - 1));
    return pRef->pFilterCond;
  }

  int32_t validate(SNode* pFilter) {
    int32_t tsIndex = -1;
    return nodesValidateExternalFilter(sourceType(), &meta_, pFilter, nullptr, nullptr, message_, sizeof(message_),
                                       &tsIndex);
  }

  void setFilter(int32_t colIndex, const std::string& condition) {
    auto query = parseSQL(std::string("CREATE VTABLE vt (ts TIMESTAMP, v INT FROM ") + sourceName() +
                          ".db.t.value WHERE " + condition + ")");
    ASSERT_NE(query, nullptr);
    SNode* pFilter = filterOf(query.get(), 1);
    ASSERT_NE(pFilter, nullptr);
    ASSERT_EQ(validate(pFilter), TSDB_CODE_SUCCESS) << message_;
    taosMemoryFreeClear(refs_[colIndex].filterCondJson);
    int32_t len = 0;
    ASSERT_EQ(nodesNodeToString(pFilter, false, &refs_[colIndex].filterCondJson, &len), TSDB_CODE_SUCCESS);
    refs_[colIndex].filterCondLen = strlen(refs_[colIndex].filterCondJson);
  }

  int32_t showCreate(std::string* pSQL) {
    SShowCreateTableStmt stmt = {};
    stmt.type = QUERY_NODE_SHOW_CREATE_VTABLE_STMT;
    tstrncpy(stmt.tableName, "vt", sizeof(stmt.tableName));
    stmt.pTableCfg = &cfg_;
    stmt.pDbCfg = &dbCfg_;
    SRetrieveTableRsp* pRsp = nullptr;
    int32_t            code = qExecCommand(nullptr, true, 0, reinterpret_cast<SNode*>(&stmt), &pRsp, 0, nullptr);
    if (code == TSDB_CODE_SUCCESS) {
      SSDataBlock block = {};
      const char* pEnd = nullptr;
      code = blockDecode(&block, pRsp->data + PAYLOAD_PREFIX_LEN, &pEnd);
      if (code == TSDB_CODE_SUCCESS) {
        auto* pCol = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 1));
        char* pValue = colDataGetData(pCol, 0);
        pSQL->assign(varDataVal(pValue), varDataLen(pValue));
      }
      blockDataFreeRes(&block);
    }
    taosMemoryFree(pRsp);
    return code;
  }

  void expectReplay(const std::string& sql, int32_t colIndex) {
    auto query = parseSQL(sql);
    ASSERT_NE(query, nullptr);
    SNode* pFilter = filterOf(query.get(), colIndex);
    ASSERT_NE(pFilter, nullptr);
    ASSERT_EQ(validate(pFilter), TSDB_CODE_SUCCESS) << message_;
    SNode* pSaved = nullptr;
    int32_t code = nodesStringToNode(refs_[colIndex].filterCondJson, &pSaved);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> saved(pSaved, nodesDestroyNode);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    // Restore both sides to literal nodes so typed values (including DECIMAL)
    // compare consistently while retaining operators, columns, and tree shape.
    ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pSaved, true), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pFilter, true), TSDB_CODE_SUCCESS);
    EXPECT_TRUE(nodesEqualNode(pSaved, pFilter));
  }

  STableCfg     cfg_ = {};
  SDbCfgInfo    dbCfg_ = {};
  SSchema       schemas_[4] = {};
  SColRef       refs_[3] = {};
  SSeriesEntry  series_ = {};
  SExtTableMeta meta_ = {};
  SExtColumnDef columns_[7] = {};
  char          message_[512] = {};
  int32_t       maxSqlLength_ = tsMaxSQLLength;
};

TEST_P(ShowCreateFilterTest, EmitsEachColumnsOwnCondition) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = 'd2'"));
  std::string saved = refs_[1].filterCondJson;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find(".`value` WHERE (`device` = 'd1')"), std::string::npos);
  EXPECT_NE(sql.find(".`value` WHERE (`device` = 'd2')"), std::string::npos);
  EXPECT_EQ(saved, refs_[1].filterCondJson);
  expectReplay(sql, 1);
  expectReplay(sql, 2);
}

TEST_P(ShowCreateFilterTest, EscapesIdentifiersQuotesAndBackslashes) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, R"(device = 'O''Reilly\\path' AND `dev``ice` != 'a"b')"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("`dev``ice`"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, PreservesTimestampAndExactNumericLiterals) {
  ASSERT_NO_FATAL_FAILURE(
      setFilter(1,
                "device = 'd1' AND ts >= '2026-09-01 12:34:56.123' AND value < -42 "
                "AND enabled = true AND price <= '12345678901234567890.12345678' AND `ratio` > 1.23456789012345"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("'2026-09-01 12:34:56.123'"), std::string::npos);
  EXPECT_NE(sql.find("'12345678901234567890.12345678'"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, ReplaysEmptyString) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = ''"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("`device` = ''"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, ReplaysUnicodeLiteral) {
  tstrncpy(columns_[1].extCharsetName, sourceType() == EXT_SOURCE_MYSQL ? "utf8mb4" : "UTF8",
           sizeof(columns_[1].extCharsetName));
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '设备一'"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("'设备一'"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, GrowsBufferAndKeepsTrailingTags) {
  std::string value(12000, 'x');
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '" + value + "'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = 'd2'"));
  tsMaxSQLLength = 128;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_GT(sql.size(), static_cast<size_t>(SHOW_CREATE_TB_RESULT_FIELD2_LEN));
  EXPECT_NE(sql.find(value), std::string::npos);
  EXPECT_NE(sql.find("`group_id`"), std::string::npos);
  tsMaxSQLLength = maxSqlLength_;
  expectReplay(sql, 1);
  expectReplay(sql, 2);
}

TEST_P(ShowCreateFilterTest, SetNullDoesNotEmitReferenceOrFilter) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].hasRef = false;
  taosMemoryFreeClear(refs_[1].filterCondJson);
  refs_[1].filterCondLen = 0;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_EQ(sql.find(" WHERE "), std::string::npos);
  EXPECT_EQ(sql.find("`v1` FROM"), std::string::npos);
  if (cfg_.tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    EXPECT_NE(sql.find("`v1` INT,"), std::string::npos);
  }
  ASSERT_NE(parseSQL(sql), nullptr);
}

TEST_P(ShowCreateFilterTest, DoesNotEmitFiltersOnIndirectReferences) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].refType = 0;
  refs_[1].refSourceName[0] = '\0';
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_EQ(sql.find(" WHERE "), std::string::npos);
}

TEST_P(ShowCreateFilterTest, RejectsInvalidSavedJson) {
  refs_[1].filterCondJson = taosStrdup("invalid-json");
  refs_[1].filterCondLen = strlen(refs_[1].filterCondJson);
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, RejectsOversizeResultInsteadOfTruncating) {
  std::string value(33000, 'x');
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '" + value + "'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = '" + value + "'"));
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_OUT_OF_BUFFER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, RejectsInconsistentFilterLength) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].filterCondLen = 0;
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, PreservesInfluxSeriesCondition) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  auto seriesQuery = parseSQL("CREATE VTABLE vseries (ts TIMESTAMP, v INT FROM series1.value) "
                              "SERIES series1 AS influx_src.db.measurement (device = 'influx-tag')");
  ASSERT_NE(seriesQuery, nullptr);
  auto* stmt = reinterpret_cast<SCreateVTableStmt*>(seriesQuery->pRoot);
  ASSERT_EQ(LIST_LENGTH(stmt->pSeriesList), 1);
  auto* decl = reinterpret_cast<SSeriesDeclNode*>(nodesListGetNode(stmt->pSeriesList, 0));
  tstrncpy(series_.alias, decl->alias, sizeof(series_.alias));
  tstrncpy(series_.sourceName, decl->sourceName, sizeof(series_.sourceName));
  tstrncpy(series_.dbName, decl->dbName, sizeof(series_.dbName));
  tstrncpy(series_.measurementName, decl->measurementName, sizeof(series_.measurementName));
  ASSERT_EQ(nodesNodeToString(decl->pTagCond, false, &series_.tagCondJson, &series_.tagCondLen), TSDB_CODE_SUCCESS);
  --series_.tagCondLen;
  tstrncpy(refs_[2].refSourceName, series_.sourceName, sizeof(refs_[2].refSourceName));
  tstrncpy(refs_[2].refTableName, series_.measurementName, sizeof(refs_[2].refTableName));
  cfg_.numOfSeries = 1;
  cfg_.pSeries = &series_;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("SERIES `series1` AS influx_src.db.measurement (device='influx-tag')"), std::string::npos);
  EXPECT_NE(sql.find("WHERE (`device` = 'd1')"), std::string::npos);
}

TEST_P(ShowCreateFilterTest, ParsesAlterFilterAndSetNull) {
  std::string reference = std::string(sourceName()) + ".db.t.value WHERE (device = 'd1' AND value >= 1)";
  auto        add = parseSQL("ALTER VTABLE vt ADD COLUMN v3 INT FROM " + reference);
  ASSERT_NE(add, nullptr);
  auto* pAdd = reinterpret_cast<SAlterTableStmt*>(add->pRoot);
  ASSERT_NE(pAdd->pColOptions, nullptr);
  ASSERT_NE(pAdd->pColOptions->pFilterCond, nullptr);
  EXPECT_EQ(validate(pAdd->pColOptions->pFilterCond), TSDB_CODE_SUCCESS) << message_;

  auto set = parseSQL("ALTER VTABLE vt ALTER COLUMN v1 SET " + reference);
  ASSERT_NE(set, nullptr);
  auto* pSet = reinterpret_cast<SAlterTableStmt*>(set->pRoot);
  ASSERT_NE(pSet->pFilterCond, nullptr);
  EXPECT_EQ(validate(pSet->pFilterCond), TSDB_CODE_SUCCESS) << message_;

  auto clear = parseSQL("ALTER VTABLE vt ALTER COLUMN v1 SET NULL");
  ASSERT_NE(clear, nullptr);
  EXPECT_EQ(reinterpret_cast<SAlterTableStmt*>(clear->pRoot)->pFilterCond, nullptr);
}

INSTANTIATE_TEST_SUITE_P(VirtualTables, ShowCreateFilterTest,
                         testing::Combine(testing::Values(TSDB_VIRTUAL_NORMAL_TABLE, TSDB_VIRTUAL_CHILD_TABLE),
                                          testing::Values(EXT_SOURCE_MYSQL, EXT_SOURCE_POSTGRESQL)));

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
