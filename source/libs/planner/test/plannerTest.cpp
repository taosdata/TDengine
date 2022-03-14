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

#include <algorithm>

#include <gtest/gtest.h>

#include "parser.h"
#include "planInt.h"

using namespace std;
using namespace testing;

class PlannerTest : public Test {
protected:
  enum TestTarget {
    TEST_LOGIC_PLAN,
    TEST_PHYSICAL_PLAN
  };

  void setDatabase(const string& acctId, const string& db) {
    acctId_ = acctId;
    db_ = db;
  }

  void bind(const char* sql) {
    reset();
    cxt_.acctId = atoi(acctId_.c_str());
    cxt_.db = db_.c_str();
    sqlBuf_ = string(sql);
    transform(sqlBuf_.begin(), sqlBuf_.end(), sqlBuf_.begin(), ::tolower);
    cxt_.sqlLen = strlen(sql);
    cxt_.pSql = sqlBuf_.c_str();
  }

  bool run(TestTarget target = TEST_PHYSICAL_PLAN) {
    int32_t code = qParseQuerySql(&cxt_, &query_);

    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] parser code:" << code << ", strerror:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return false;
    }

    const string syntaxTreeStr = toString(query_->pRoot, false);
  
    SLogicNode* pLogicPlan = nullptr;
    SPlanContext cxt = { .queryId = 1, .acctId = 0, .pAstRoot = query_->pRoot };
    code = createLogicPlan(&cxt, &pLogicPlan);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] logic plan code:" << code << ", strerror:" << tstrerror(code) << endl;
      return false;
    }
  
    cout << "====================sql : [" << cxt_.pSql << "]" << endl;
    cout << "syntax test : " << endl;
    cout << syntaxTreeStr << endl;
    cout << "unformatted logic plan : " << endl;
    cout << toString((const SNode*)pLogicPlan, false) << endl;

    if (TEST_PHYSICAL_PLAN == target) {
      SQueryPlan* pPlan = nullptr;
      code = createPhysiPlan(&cxt, pLogicPlan, &pPlan, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        cout << "sql:[" << cxt_.pSql << "] physical plan code:" << code << ", strerror:" << tstrerror(code) << endl;
        return false;
      }
      cout << "unformatted physical plan : " << endl;
      cout << toString((const SNode*)pPlan, false) << endl;
      SNode* pNode;
      FOREACH(pNode, pPlan->pSubplans) {
        SNode* pSubplan;
        FOREACH(pSubplan, ((SNodeListNode*)pNode)->pNodeList) {
          cout << "unformatted physical subplan : " << endl;
          cout << toString(pSubplan, false) << endl;
        }
      }
    }

    return true;
  }

private:
  static const int max_err_len = 1024;

  void reset() {
    memset(&cxt_, 0, sizeof(cxt_));
    memset(errMagBuf_, 0, max_err_len);
    cxt_.pMsg = errMagBuf_;
    cxt_.msgLen = max_err_len;
  }

  string toString(const SNode* pRoot, bool format = true) {
    char* pStr = NULL;
    int32_t len = 0;
    int32_t code = nodesNodeToString(pRoot, format, &pStr, &len);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] toString code:" << code << ", strerror:" << tstrerror(code) << endl;
      return string();
    }
    SNode* pNode;
    code = nodesStringToNode(pStr, &pNode);
    if (code != TSDB_CODE_SUCCESS) {
      tfree(pStr);
      cout << "sql:[" << cxt_.pSql << "] toObject code:" << code << ", strerror:" << tstrerror(code) << endl;
      return string();
    }
    nodesDestroyNode(pNode);
    string str(pStr);
    tfree(pStr);
    return str;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  string sqlBuf_;
  SParseContext cxt_;
  SQuery* query_;
};

TEST_F(PlannerTest, simple) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, stSimple) {
  setDatabase("root", "test");

  bind("SELECT * FROM st1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, groupBy) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT c1, count(*) FROM t1 GROUP BY c1");
  ASSERT_TRUE(run());

  bind("SELECT c1 + c3, c1 + count(*) FROM t1 where c2 = 'abc' GROUP BY c1, c3");
  ASSERT_TRUE(run());

  bind("SELECT c1 + c3, sum(c4 * c5) FROM t1 where concat(c2, 'wwww') = 'abcwww' GROUP BY c1 + c3");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, subquery) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM (SELECT c1 + c3 a, c1 + count(*) b FROM t1 where c2 = 'abc' GROUP BY c1, c3) where a > 100 group by b");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, interval) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1 interval(10s)");
  ASSERT_TRUE(run());
}
