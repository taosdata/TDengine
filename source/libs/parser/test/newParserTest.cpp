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

#include "parserImpl.h"

using namespace std;
using namespace testing;

class NewParserTest : public Test {
protected:
  void setDatabase(const string& acctId, const string& db) {
    acctId_ = acctId;
    db_ = db;
  }

  void bind(const char* sql) {
    reset();
    cxt_.acctId = atoi(acctId_.c_str());
    cxt_.db = (char*) db_.c_str();
    strcpy(sqlBuf_, sql);
    cxt_.sqlLen = strlen(sql);
    sqlBuf_[cxt_.sqlLen] = '\0';
    cxt_.pSql = sqlBuf_;

  }

  bool run(int32_t parseCode = TSDB_CODE_SUCCESS, int32_t translateCode = TSDB_CODE_SUCCESS) {
    int32_t code = doParse(&cxt_, &query_);
    // cout << "doParse return " << code << endl;
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] code:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return (TSDB_CODE_SUCCESS != parseCode);
    }
    if (TSDB_CODE_SUCCESS != parseCode) {
      return false;
    }
    code = doTranslate(&cxt_, &query_);
    // cout << "doTranslate return " << code << endl;
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] code:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return (TSDB_CODE_SUCCESS != translateCode);
    }
    if (NULL != query_.pRoot && QUERY_NODE_SELECT_STMT == nodeType(query_.pRoot)) {
      string sql;
      selectToSql(query_.pRoot, sql);
      cout << "input sql : [" << cxt_.pSql << "]" << endl;
      cout << "output sql : [" << sql << "]" << endl;
    }
    return (TSDB_CODE_SUCCESS == translateCode);
  }

private:
  static const int max_err_len = 1024;
  static const int max_sql_len = 1024 * 1024;

  void selectToSql(const SNode* node, string& sql) {
    SSelectStmt* select = (SSelectStmt*)node;
    sql.append("SELECT ");
    if (select->isDistinct) {
      sql.append("DISTINCT ");
    }
    if (nullptr == select->pProjectionList) {
      sql.append("* ");
    } else {
      nodeListToSql(select->pProjectionList, sql);
      sql.append(" ");
    }
    sql.append("FROM ");
    tableToSql(select->pFromTable, sql);
    if (nullptr != select->pWhere) {
      sql.append(" WHERE ");
      nodeToSql(select->pWhere, sql);
    }
  }

  void tableToSql(const SNode* node, string& sql) {
    const STableNode* table = (const STableNode*)node;
    switch (nodeType(node)) {
      case QUERY_NODE_REAL_TABLE: {
        SRealTableNode* realTable = (SRealTableNode*)table;
        if ('\0' != realTable->table.dbName[0]) {
          sql.append(realTable->table.dbName);
          sql.append(".");
        }
        sql.append(realTable->table.tableName);
        break;
      }
      case QUERY_NODE_TEMP_TABLE: {
        STempTableNode* tempTable = (STempTableNode*)table;
        sql.append("(");
        selectToSql(tempTable->pSubquery, sql);
        sql.append(") ");
        sql.append(tempTable->table.tableAlias);
        break;
      }
      case QUERY_NODE_JOIN_TABLE: {
        SJoinTableNode* joinTable = (SJoinTableNode*)table;
        tableToSql(joinTable->pLeft, sql);
        sql.append(" JOIN ");
        tableToSql(joinTable->pRight, sql);
        if (nullptr != joinTable->pOnCond) {
          sql.append(" ON ");
          nodeToSql(joinTable->pOnCond, sql);
        }
        break;
      }
      default:
        break;
    }
  }

  string opTypeToSql(EOperatorType type) {
    switch (type) {
      case OP_TYPE_ADD:
        return " + ";
      case OP_TYPE_SUB:
        return " - ";
      case OP_TYPE_MULTI:
      case OP_TYPE_DIV:
      case OP_TYPE_MOD:
      case OP_TYPE_GREATER_THAN:
      case OP_TYPE_GREATER_EQUAL:
      case OP_TYPE_LOWER_THAN:
      case OP_TYPE_LOWER_EQUAL:
      case OP_TYPE_EQUAL:
        return " = ";
      case OP_TYPE_NOT_EQUAL:
      case OP_TYPE_IN:
      case OP_TYPE_NOT_IN:
      case OP_TYPE_LIKE:
      case OP_TYPE_NOT_LIKE:
      case OP_TYPE_MATCH:
      case OP_TYPE_NMATCH:
      case OP_TYPE_JSON_GET_VALUE:
      case OP_TYPE_JSON_CONTAINS:
      default:
        break;
    }
    return " unknown operator ";
  }

  void nodeToSql(const SNode* node, string& sql) {
    if (nullptr == node) {
      return;
    }

    switch (nodeType(node)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)node;
        if ('\0' != pCol->dbName[0]) {
          sql.append(pCol->dbName);
          sql.append(".");
        }
        if ('\0' != pCol->tableAlias[0]) {
          sql.append(pCol->tableAlias);
          sql.append(".");
        }
        sql.append(pCol->colName);
        break;
      }
      case QUERY_NODE_VALUE:
        break;
      case QUERY_NODE_OPERATOR: {
        SOperatorNode* pOp = (SOperatorNode*)node;
        nodeToSql(pOp->pLeft, sql);
        sql.append(opTypeToSql(pOp->opType));
        nodeToSql(pOp->pRight, sql);
        break;
      }
      default:
        break;
    }
  }

  void nodeListToSql(const SNodeList* nodelist, string& sql, const string& seq = ",") {
    SNode* node = nullptr;
    bool firstNode = true;
    FOREACH(node, nodelist) {
      if (!firstNode) {
        sql.append(", ");
      }
      firstNode = false;
      nodeToSql(node, sql);
    }
  }

  void reset() {
    memset(&cxt_, 0, sizeof(cxt_));
    memset(errMagBuf_, 0, max_err_len);
    cxt_.pMsg = errMagBuf_;
    cxt_.msgLen = max_err_len;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  char sqlBuf_[max_sql_len];
  SParseContext cxt_;
  SQuery query_;
};

// SELECT * FROM t1
TEST_F(NewParserTest, selectStar) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT * FROM test.t1");
  ASSERT_TRUE(run());

  bind("SELECT ts, c1 FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT ts, t.c1 FROM (SELECT * FROM t1) t");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");
  ASSERT_TRUE(run());
}

TEST_F(NewParserTest, syntaxError) {
  setDatabase("root", "test");

  bind("SELECTT * FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT *");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT *, * FROM test.t1");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT * FROM test.t1 t WHER");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));
}

TEST_F(NewParserTest, semanticError) {
  setDatabase("root", "test");

  bind("SELECT * FROM t10");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));

  bind("SELECT c1, c3 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));

  bind("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));
}
