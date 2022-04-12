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

#include "cmdnodes.h"
#include "parser.h"
#include "planInt.h"

using namespace std;
using namespace testing;

class PlannerTest : public Test {
protected:
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

  bool run(bool streamQuery = false) {
    int32_t code = qParseQuerySql(&cxt_, &query_);

    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] parser code:" << code << ", strerror:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return false;
    }

    const string syntaxTreeStr = toString(query_->pRoot, false);
  
    SLogicNode* pLogicNode = nullptr;
    SPlanContext cxt = {0};
    cxt.queryId = 1;
    cxt.acctId = 0;
    cxt.streamQuery = streamQuery;

    setPlanContext(query_, &cxt);
    code = createLogicPlan(&cxt, &pLogicNode);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] createLogicPlan code:" << code << ", strerror:" << tstrerror(code) << endl;
      return false;
    }
  
    cout << "====================sql : [" << cxt_.pSql << "]" << endl;
    cout << "syntax tree : " << endl;
    cout << syntaxTreeStr << endl;
    cout << "unformatted logic plan : " << endl;
    cout << toString((const SNode*)pLogicNode, false) << endl;

    SLogicSubplan* pLogicSubplan = nullptr;
    code = splitLogicPlan(&cxt, pLogicNode, &pLogicSubplan);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] splitLogicPlan code:" << code << ", strerror:" << tstrerror(code) << endl;
      return false;
    }

    SQueryLogicPlan* pLogicPlan = NULL;
    code = scaleOutLogicPlan(&cxt, pLogicSubplan, &pLogicPlan);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] createPhysiPlan code:" << code << ", strerror:" << tstrerror(code) << endl;
      return false;
    }

    SQueryPlan* pPlan = nullptr;
    code = createPhysiPlan(&cxt, pLogicPlan, &pPlan, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] createPhysiPlan code:" << code << ", strerror:" << tstrerror(code) << endl;
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

    return true;
  }

private:
  static const int max_err_len = 1024;

  void setPlanContext(SQuery* pQuery, SPlanContext* pCxt) {
    if (QUERY_NODE_CREATE_TOPIC_STMT == nodeType(pQuery->pRoot)) {
      pCxt->pAstRoot = ((SCreateTopicStmt*)pQuery->pRoot)->pQuery;
      pCxt->topicQuery = true;
    } else if (QUERY_NODE_CREATE_INDEX_STMT == nodeType(pQuery->pRoot)) {
      SMCreateSmaReq req = {0};
      tDeserializeSMCreateSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req);
      nodesStringToNode(req.ast, &pCxt->pAstRoot);
      pCxt->streamQuery = true;
    } else {
      pCxt->pAstRoot = pQuery->pRoot;
    }
  }

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
    string str(pStr);
    taosMemoryFreeClear(pStr);
    return str;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  string sqlBuf_;
  SParseContext cxt_;
  SQuery* query_;
};

TEST_F(PlannerTest, selectBasic) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectConstant) {
  setDatabase("root", "test");

  bind("SELECT 2-1 FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectStableBasic) {
  setDatabase("root", "test");

  bind("SELECT * FROM st1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectJoin) {
  setDatabase("root", "test");

  bind("SELECT * FROM st1s1 t1, st1s2 t2 where t1.ts = t2.ts");
  ASSERT_TRUE(run());

  bind("SELECT * FROM st1s1 t1 join st1s2 t2 on t1.ts = t2.ts where t1.c1 > t2.c1");
  ASSERT_TRUE(run());

  bind("SELECT t1.* FROM st1s1 t1 join st1s2 t2 on t1.ts = t2.ts where t1.c1 > t2.c1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectGroupBy) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1");
  ASSERT_TRUE(run());

  // bind("SELECT c1, max(c3), min(c2), count(*) FROM t1 GROUP BY c1");
  // ASSERT_TRUE(run());

  // bind("SELECT c1 + c3, c1 + count(*) FROM t1 where c2 = 'abc' GROUP BY c1, c3");
  // ASSERT_TRUE(run());

  // bind("SELECT c1 + c3, sum(c4 * c5) FROM t1 where concat(c2, 'wwww') = 'abcwww' GROUP BY c1 + c3");
  // ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectSubquery) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM (SELECT c1 + c3 a, c1 + count(*) b FROM t1 where c2 = 'abc' GROUP BY c1, c3) where a > 100 group by b");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectInterval) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1 interval(10s)");
  ASSERT_TRUE(run());

  bind("SELECT _wstartts, _wduration, _wendts, count(*) FROM t1 interval(10s)");
  ASSERT_TRUE(run());

  bind("SELECT count(*) FROM t1 interval(10s) fill(linear)");
  ASSERT_TRUE(run());

  bind("SELECT count(*), sum(c1) FROM t1 interval(10s) fill(value, 10, 20)");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectSessionWindow) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1 session(ts, 10s)");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectStateWindow) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1 state_window(c1)");
  ASSERT_TRUE(run());

  bind("SELECT count(*) FROM t1 state_window(c1 + 10)");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectPartitionBy) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1 partition by c1");
  ASSERT_TRUE(run());

  bind("SELECT count(*) FROM t1 partition by c1");
  ASSERT_TRUE(run());

  bind("SELECT count(*) FROM t1 partition by c1 group by c2");
  ASSERT_TRUE(run());

  bind("SELECT count(*) FROM st1 partition by tag1, tag2 interval(10s)");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectOrderBy) {
  setDatabase("root", "test");

  bind("SELECT c1 FROM t1 order by c1");
  ASSERT_TRUE(run());

  bind("SELECT c1 FROM t1 order by c2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 order by c1 + 10, c2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 order by c1 desc nulls first");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectGroupByOrderBy) {
  setDatabase("root", "test");

  bind("select count(*), sum(c1) from t1 order by sum(c1)");
  ASSERT_TRUE(run());

  bind("select count(*), sum(c1) a from t1 order by a");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectDistinct) {
  setDatabase("root", "test");

  bind("SELECT distinct c1 FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT distinct c1, c2 + 10 FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT distinct c1 + 10 a FROM t1 order by a");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectLimit) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1 limit 2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 limit 5 offset 2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 limit 2, 5");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, selectSlimit) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1 partition by c1 slimit 2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 partition by c1 slimit 5 soffset 2");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 partition by c1 slimit 2, 5");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, showTables) {
  setDatabase("root", "test");

  bind("show tables");
  ASSERT_TRUE(run());

  setDatabase("root", "information_schema");

  bind("show tables");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, showStables) {
  setDatabase("root", "test");

  bind("show stables");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, createTopic) {
  setDatabase("root", "test");

  bind("create topic tp as SELECT * FROM st1");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, stream) {
  setDatabase("root", "test");

  bind("SELECT sum(c1) FROM st1");
  ASSERT_TRUE(run(true));
}

TEST_F(PlannerTest, createSmaIndex) {
  setDatabase("root", "test");

  bind("create sma index index1 on t1 function(max(c1), min(c3 + 10), sum(c4)) INTERVAL(10s)");
  ASSERT_TRUE(run());
}

TEST_F(PlannerTest, explain) {
  setDatabase("root", "test");

  bind("explain SELECT * FROM t1");
  ASSERT_TRUE(run());

  bind("explain analyze SELECT * FROM t1");
  ASSERT_TRUE(run());

  bind("explain analyze verbose true ratio 0.01 SELECT * FROM t1");
  ASSERT_TRUE(run());
}
