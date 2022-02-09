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
      cout << "input sql : [" << cxt_.pSql << "]" << endl;
      // string sql;
      // selectToSql(query_.pRoot, sql);
      // cout << "output sql : [" << sql << "]" << endl;
      string str;
      selectToStr(query_.pRoot, str);
      cout << "translate str : \n" << str << endl;
    }
    return (TSDB_CODE_SUCCESS == translateCode);
  }

private:
  static const int max_err_len = 1024;
  static const int max_sql_len = 1024 * 1024;

  string dataTypeToStr(const SDataType& dt) {
    switch (dt.type) {
      case TSDB_DATA_TYPE_NULL:
        return "NULL";
      case TSDB_DATA_TYPE_BOOL:
        return "BOOL";
      case TSDB_DATA_TYPE_TINYINT:
        return "TINYINT";
      case TSDB_DATA_TYPE_SMALLINT:
        return "SMALLINT";
      case TSDB_DATA_TYPE_INT:
        return "INT";
      case TSDB_DATA_TYPE_BIGINT:
        return "BIGINT";
      case TSDB_DATA_TYPE_FLOAT:
        return "FLOAT";
      case TSDB_DATA_TYPE_DOUBLE:
        return "DOUBLE";
      case TSDB_DATA_TYPE_BINARY:
        return "BINART(" + to_string(dt.bytes) + ")";
      case TSDB_DATA_TYPE_TIMESTAMP:
        return "TIMESTAMP";
      case TSDB_DATA_TYPE_NCHAR:
        return "NCHAR(" + to_string(dt.bytes) + ")";
      case TSDB_DATA_TYPE_UTINYINT:
        return "UTINYINT";
      case TSDB_DATA_TYPE_USMALLINT:
        return "USMALLINT";
      case TSDB_DATA_TYPE_UINT:
        return "UINT";
      case TSDB_DATA_TYPE_UBIGINT:
        return "UBIGINT";
      case TSDB_DATA_TYPE_VARCHAR:
        return "VARCHAR(" + to_string(dt.bytes) + ")";
      case TSDB_DATA_TYPE_VARBINARY:
        return "VARBINARY(" + to_string(dt.bytes) + ")";
      case TSDB_DATA_TYPE_JSON:
        return "JSON";
      case TSDB_DATA_TYPE_DECIMAL:
        return "DECIMAL(" + to_string(dt.precision) + ", "  + to_string(dt.scale) + ")";
      case TSDB_DATA_TYPE_BLOB:
        return "BLOB";
      default:
        break;
    }
    return "Unknown Data Type " + to_string(dt.type);
  }

  void nodeToStr(const SNode* node, string& str, bool isProject) {
    if (nullptr == node) {
      return;
    }

    switch (nodeType(node)) {
      case QUERY_NODE_COLUMN: {
        SColumnNode* pCol = (SColumnNode*)node;
        if ('\0' != pCol->dbName[0]) {
          str.append(pCol->dbName);
          str.append(".");
        }
        if ('\0' != pCol->tableAlias[0]) {
          str.append(pCol->tableAlias);
          str.append(".");
        }
        str.append(pCol->colName);
        str.append(" [" + dataTypeToStr(pCol->node.resType) + "]");
        if (isProject) {
          str.append(" AS " + string(pCol->node.aliasName));
        }
        break;
      }
      case QUERY_NODE_VALUE: {
        SValueNode* pVal = (SValueNode*)node;
        str.append(pVal->literal);
        str.append(" [" + dataTypeToStr(pVal->node.resType) + "]");
        if (isProject) {
          str.append(" AS " + string(pVal->node.aliasName));
        }
        break;
      }
      case QUERY_NODE_OPERATOR: {
        SOperatorNode* pOp = (SOperatorNode*)node;
        nodeToStr(pOp->pLeft, str, false);
        str.append(opTypeToStr(pOp->opType));
        nodeToStr(pOp->pRight, str, false);
        str.append(" [" + dataTypeToStr(pOp->node.resType) + "]");
        if (isProject) {
          str.append(" AS " + string(pOp->node.aliasName));
        }
        break;
      }
      default:
        break;
    }
  }

  void nodeListToStr(const SNodeList* nodelist, const string& prefix, string& str, bool isProject = false) {
    SNode* node = nullptr;
    FOREACH(node, nodelist) {
      str.append(prefix);
      nodeToStr(node, str, isProject);
      str.append("\n");
    }
  }

  void tableToStr(const SNode* node, const string& prefix, string& str) {
    const STableNode* table = (const STableNode*)node;
    switch (nodeType(node)) {
      case QUERY_NODE_REAL_TABLE: {
        SRealTableNode* realTable = (SRealTableNode*)table;
        str.append(prefix);
        if ('\0' != realTable->table.dbName[0]) {
          str.append(realTable->table.dbName);
          str.append(".");
        }
        str.append(realTable->table.tableName);
        str.append(string(" ") + realTable->table.tableAlias);
        break;
      }
      case QUERY_NODE_TEMP_TABLE: {
        STempTableNode* tempTable = (STempTableNode*)table;
        str.append(prefix + "(\n");
        selectToStr(tempTable->pSubquery, str, prefix + "\t");
        str.append("\n");
        str.append(prefix + ") ");
        str.append(tempTable->table.tableAlias);
        break;
      }
      case QUERY_NODE_JOIN_TABLE: {
        SJoinTableNode* joinTable = (SJoinTableNode*)table;
        tableToStr(joinTable->pLeft, prefix, str);
        str.append("\n" + prefix + "JOIN\n");
        tableToStr(joinTable->pRight, prefix, str);
        if (nullptr != joinTable->pOnCond) {
          str.append("\n" + prefix + "\tON ");
          nodeToStr(joinTable->pOnCond, str, false);
        }
        break;
      }
      default:
        break;
    }
  }

  void selectToStr(const SNode* node, string& str, const string& prefix = "") {
    SSelectStmt* select = (SSelectStmt*)node;
    str.append(prefix + "SELECT ");
    if (select->isDistinct) {
      str.append("DISTINCT");
    }
    str.append("\n");
    nodeListToStr(select->pProjectionList, prefix + "\t", str, true);
    str.append("\n" + prefix + "FROM\n");
    tableToStr(select->pFromTable, prefix + "\t", str);
  }

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

  string opTypeToStr(EOperatorType type) {
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
        sql.append(opTypeToStr(pOp->opType));
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

TEST_F(NewParserTest, selectSimple) {
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

TEST_F(NewParserTest, selectExpression) {
  setDatabase("root", "test");

  bind("SELECT c1 + 10, c2 FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(NewParserTest, selectSyntaxError) {
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

TEST_F(NewParserTest, selectSemanticError) {
  setDatabase("root", "test");

  bind("SELECT * FROM t10");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));

  bind("SELECT c1, c3 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));

  bind("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_FAILED));
}
