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

#include "planTestUtil.h"

#include <algorithm>

#include "cmdnodes.h"
#include "parser.h"
#include "planInt.h"

using namespace std;
using namespace testing;

#define DO_WITH_THROW(func, ...) \
  do { \
    int32_t code__ = func(__VA_ARGS__); \
    if (TSDB_CODE_SUCCESS != code__) { \
      throw runtime_error("sql:[" + stmtEnv_.sql_ + "] " #func " code:" + to_string(code__) + ", strerror:" + string(tstrerror(code__)) + ", msg:" + string(stmtEnv_.msgBuf_.data())); \
    } \
  } while(0);

bool g_isDump = false;

class PlannerTestBaseImpl {
public:
  void useDb(const string& acctId, const string& db) {
    caseEnv_.acctId_ = acctId;
    caseEnv_.db_ = db;
  }

  void run(const string& sql) {
    reset();
    try {
      SQuery* pQuery = nullptr;
      doParseSql(sql, &pQuery);

      SPlanContext cxt = {0};
      setPlanContext(pQuery, &cxt);

      SLogicNode* pLogicNode = nullptr;
      doCreateLogicPlan(&cxt, &pLogicNode);

      doOptimizeLogicPlan(&cxt, pLogicNode);

      SLogicSubplan* pLogicSubplan = nullptr;
      doSplitLogicPlan(&cxt, pLogicNode, &pLogicSubplan);

      SQueryLogicPlan* pLogicPlan = nullptr;
      doScaleOutLogicPlan(&cxt, pLogicSubplan, &pLogicPlan);

      SQueryPlan* pPlan = nullptr;
      doCreatePhysiPlan(&cxt, pLogicPlan, &pPlan);

      if (g_isDump) {
        dump();
      }
    } catch (...) {
      dump();
      throw;
    }
  }

private:
  struct caseEnv {
    string acctId_;
    string db_;
  };

  struct stmtEnv {
    string sql_;
    array<char, 1024> msgBuf_;
  };

  struct stmtRes {
    string ast_;
    string rawLogicPlan_;
    string optimizedLogicPlan_;
    string splitLogicPlan_;
    string scaledLogicPlan_;
    string physiPlan_;
    vector<string> physiSubplans_;
  };

  void reset() {
    stmtEnv_.sql_.clear();
    stmtEnv_.msgBuf_.fill(0);

    res_.ast_.clear();
    res_.rawLogicPlan_.clear();
    res_.optimizedLogicPlan_.clear();
    res_.splitLogicPlan_.clear();
    res_.scaledLogicPlan_.clear();
    res_.physiPlan_.clear();
  }

  void dump() {
    cout << "==========================================sql : [" << stmtEnv_.sql_ << "]" << endl;
    cout << "syntax tree : " << endl;
    cout << res_.ast_ << endl;
    cout << "raw logic plan : " << endl;
    cout << res_.rawLogicPlan_ << endl;
    cout << "optimized logic plan : " << endl;
    cout << res_.optimizedLogicPlan_ << endl;
    cout << "split logic plan : " << endl;
    cout << res_.splitLogicPlan_ << endl;
    cout << "scaled logic plan : " << endl;
    cout << res_.scaledLogicPlan_ << endl;
    cout << "physical plan : " << endl;
    cout << res_.physiPlan_ << endl;
    cout << "physical subplan : " << endl;
    for (const auto& subplan : res_.physiSubplans_) {
      cout << subplan << endl;
    }
  }
  
  void doParseSql(const string& sql, SQuery** pQuery) {
    stmtEnv_.sql_ = sql;
    transform(stmtEnv_.sql_.begin(), stmtEnv_.sql_.end(), stmtEnv_.sql_.begin(), ::tolower);
  
    SParseContext cxt = {0};
    cxt.acctId = atoi(caseEnv_.acctId_.c_str());
    cxt.db = caseEnv_.db_.c_str();
    cxt.pSql = stmtEnv_.sql_.c_str();
    cxt.sqlLen = stmtEnv_.sql_.length();
    cxt.pMsg = stmtEnv_.msgBuf_.data();
    cxt.msgLen = stmtEnv_.msgBuf_.max_size();
  
    DO_WITH_THROW(qParseQuerySql, &cxt, pQuery);
    res_.ast_ = toString((*pQuery)->pRoot);
  }

  void doCreateLogicPlan(SPlanContext* pCxt, SLogicNode** pLogicNode) {
    DO_WITH_THROW(createLogicPlan, pCxt, pLogicNode);
    res_.rawLogicPlan_ = toString((SNode*)(*pLogicNode));
  }

  void doOptimizeLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode) {
    DO_WITH_THROW(optimizeLogicPlan, pCxt, pLogicNode);
    res_.optimizedLogicPlan_ = toString((SNode*)pLogicNode);
  }

  void doSplitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan) {
    DO_WITH_THROW(splitLogicPlan, pCxt, pLogicNode, pLogicSubplan);
    res_.splitLogicPlan_ = toString((SNode*)(*pLogicSubplan));
  }

  void doScaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan) {
    DO_WITH_THROW(scaleOutLogicPlan, pCxt, pLogicSubplan, pLogicPlan);
    res_.scaledLogicPlan_ = toString((SNode*)(*pLogicPlan));
  }

  void doCreatePhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan) {
    SArray* pExecNodeList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SQueryNodeAddr));
    DO_WITH_THROW(createPhysiPlan, pCxt, pLogicPlan, pPlan, pExecNodeList);
    res_.physiPlan_ = toString((SNode*)(*pPlan));
    SNode* pNode;
    FOREACH(pNode, (*pPlan)->pSubplans) {
      SNode* pSubplan;
      FOREACH(pSubplan, ((SNodeListNode*)pNode)->pNodeList) {
        res_.physiSubplans_.push_back(toString(pSubplan));
      }
    }
  }

  void setPlanContext(SQuery* pQuery, SPlanContext* pCxt) {
    if (QUERY_NODE_CREATE_TOPIC_STMT == nodeType(pQuery->pRoot)) {
      pCxt->pAstRoot = ((SCreateTopicStmt*)pQuery->pRoot)->pQuery;
      pCxt->topicQuery = true;
    } else if (QUERY_NODE_CREATE_INDEX_STMT == nodeType(pQuery->pRoot)) {
      SMCreateSmaReq req = {0};
      tDeserializeSMCreateSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req);
      nodesStringToNode(req.ast, &pCxt->pAstRoot);
      pCxt->streamQuery = true;
    } else if (QUERY_NODE_CREATE_STREAM_STMT == nodeType(pQuery->pRoot)) {
      SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
      pCxt->pAstRoot = pStmt->pQuery;
      pCxt->streamQuery = true;
      pCxt->triggerType = pStmt->pOptions->triggerType;
      pCxt->watermark = (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
    } else {
      pCxt->pAstRoot = pQuery->pRoot;
    }
  }

  string toString(const SNode* pRoot) {
    char* pStr = NULL;
    int32_t len = 0;
    DO_WITH_THROW(nodesNodeToString, pRoot, false, &pStr, &len)
    string str(pStr);
    taosMemoryFreeClear(pStr);
    return str;
  }

  caseEnv caseEnv_;
  stmtEnv stmtEnv_;
  stmtRes res_;
};

PlannerTestBase::PlannerTestBase() : impl_(new PlannerTestBaseImpl()) {
}

PlannerTestBase::~PlannerTestBase() {
}

void PlannerTestBase::useDb(const std::string& acctId, const std::string& db) {
  impl_->useDb(acctId, db);
}

void PlannerTestBase::run(const std::string& sql) {
  return impl_->run(sql);
}
