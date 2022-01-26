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

  int32_t run() {
    int32_t code = doParse(&cxt_, &query_);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "code:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return code;
    }
    cout << nodeType(query_.pRoot) << endl;
    if (NULL != query_.pRoot && QUERY_NODE_SELECT_STMT == nodeType(query_.pRoot)) {
  // SNode* pWhereCond;
  // SNodeList* pPartitionByList; // SNode
  // SNode* pWindowClause;
  // SNodeList* pGroupByList; // SGroupingSetNode
  // SNodeList* pOrderByList; // SOrderByExprNode
  // SLimitNode limit;
  // SLimitNode slimit;

      SSelectStmt* select = (SSelectStmt*)query_.pRoot;
      string sql("SELECT ");
      if (select->isDistinct) {
        sql.append("DISTINCT ");
      }
      if (nullptr == select->pProjectionList) {
        sql.append("* ");
      } else {
        nodeListToSql(select->pProjectionList, sql);
      }
      sql.append("FROM ");
      tableToSql(select->pFromTable, sql);
      cout << sql << endl;
    }
    // char* pStr = NULL;
    // int32_t len = 0;
    // code = nodesNodeToString(query_.pRoot, &pStr, &len);
    // if (code != TSDB_CODE_SUCCESS) {
    //   cout << "code:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
    //   return code;
    // }
    // cout << "node tree:\n" << pStr << endl;
    return TSDB_CODE_SUCCESS;
  }

private:
  static const int max_err_len = 1024;
  static const int max_sql_len = 1024 * 1024;

  void tableToSql(const SNode* node, string& sql) {
    const STableNode* table = (const STableNode*)node;
    cout << "node : " << nodeType(node) << endl;
    switch (nodeType(node)) {
      case QUERY_NODE_REAL_TABLE: {
        SRealTableNode* realTable = (SRealTableNode*)table;
        if ('\0' != realTable->dbName[0]) {
          sql.append(realTable->dbName);
          sql.append(".");
        }
        sql.append(realTable->table.tableName);
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
      switch (nodeType(node)) {
        case QUERY_NODE_COLUMN:
          sql.append(((SColumnNode*)node)->colName);
          break;
      }
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
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);

  bind("SELECT * FROM test.t1");
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
}
