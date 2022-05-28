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
#include <getopt.h>

#include <algorithm>
#include <array>

#include "cmdnodes.h"
#include "parser.h"
#include "planInt.h"

using namespace std;
using namespace testing;

#define DO_WITH_THROW(func, ...)                                                                                   \
  do {                                                                                                             \
    int32_t code__ = func(__VA_ARGS__);                                                                            \
    if (TSDB_CODE_SUCCESS != code__) {                                                                             \
      throw runtime_error("sql:[" + stmtEnv_.sql_ + "] " #func " code:" + to_string(code__) +                      \
                          ", strerror:" + string(tstrerror(code__)) + ", msg:" + string(stmtEnv_.msgBuf_.data())); \
    }                                                                                                              \
  } while (0);

enum DumpModule {
  DUMP_MODULE_NOTHING = 1,
  DUMP_MODULE_PARSER,
  DUMP_MODULE_LOGIC,
  DUMP_MODULE_OPTIMIZED,
  DUMP_MODULE_SPLIT,
  DUMP_MODULE_SCALED,
  DUMP_MODULE_PHYSICAL,
  DUMP_MODULE_SUBPLAN,
  DUMP_MODULE_ALL
};

DumpModule g_dumpModule = DUMP_MODULE_NOTHING;
int32_t    g_skipSql = 0;
int32_t    g_logLevel = 131;

void setDumpModule(const char* pModule) {
  if (NULL == pModule) {
    g_dumpModule = DUMP_MODULE_ALL;
  } else if (0 == strncasecmp(pModule, "parser", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_PARSER;
  } else if (0 == strncasecmp(pModule, "logic", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_LOGIC;
  } else if (0 == strncasecmp(pModule, "optimized", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_OPTIMIZED;
  } else if (0 == strncasecmp(pModule, "split", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_SPLIT;
  } else if (0 == strncasecmp(pModule, "scaled", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_SCALED;
  } else if (0 == strncasecmp(pModule, "physical", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_PHYSICAL;
  } else if (0 == strncasecmp(pModule, "subplan", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_SUBPLAN;
  } else if (0 == strncasecmp(pModule, "all", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_PHYSICAL;
  }
}

void setSkipSqlNum(const char* pNum) { g_skipSql = stoi(pNum); }

void setLogLevel(const char* pLogLevel) { g_logLevel = stoi(pLogLevel); }

int32_t getLogLevel() { return g_logLevel; }

class PlannerTestBaseImpl {
 public:
  void useDb(const string& acctId, const string& db) {
    caseEnv_.acctId_ = acctId;
    caseEnv_.db_ = db;
    caseEnv_.nsql_ = g_skipSql;
  }

  void run(const string& sql) {
    if (caseEnv_.nsql_ > 0) {
      --(caseEnv_.nsql_);
      return;
    }

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

      dump(g_dumpModule);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      throw;
    }
  }

  void prepare(const string& sql) {
    if (caseEnv_.nsql_ > 0) {
      return;
    }

    reset();
    try {
      doParseSql(sql, &stmtEnv_.pQuery_, true);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      throw;
    }
  }

  void bindParams(TAOS_MULTI_BIND* pParams, int32_t colIdx) {
    if (caseEnv_.nsql_ > 0) {
      return;
    }

    try {
      doBindParams(stmtEnv_.pQuery_, pParams, colIdx);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      throw;
    }
  }

  void exec() {
    if (caseEnv_.nsql_ > 0) {
      --(caseEnv_.nsql_);
      return;
    }

    try {
      doParseBoundSql(stmtEnv_.pQuery_);

      SPlanContext cxt = {0};
      setPlanContext(stmtEnv_.pQuery_, &cxt);

      SLogicNode* pLogicNode = nullptr;
      doCreateLogicPlan(&cxt, &pLogicNode);

      doOptimizeLogicPlan(&cxt, pLogicNode);

      SLogicSubplan* pLogicSubplan = nullptr;
      doSplitLogicPlan(&cxt, pLogicNode, &pLogicSubplan);

      SQueryLogicPlan* pLogicPlan = nullptr;
      doScaleOutLogicPlan(&cxt, pLogicSubplan, &pLogicPlan);

      SQueryPlan* pPlan = nullptr;
      doCreatePhysiPlan(&cxt, pLogicPlan, &pPlan);

      dump(g_dumpModule);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      throw;
    }
  }

 private:
  struct caseEnv {
    string  acctId_;
    string  db_;
    int32_t nsql_;
  };

  struct stmtEnv {
    string            sql_;
    array<char, 1024> msgBuf_;
    SQuery*           pQuery_;

    ~stmtEnv() { qDestroyQuery(pQuery_); }
  };

  struct stmtRes {
    string         ast_;
    string         prepareAst_;
    string         boundAst_;
    string         rawLogicPlan_;
    string         optimizedLogicPlan_;
    string         splitLogicPlan_;
    string         scaledLogicPlan_;
    string         physiPlan_;
    vector<string> physiSubplans_;
  };

  void reset() {
    stmtEnv_.sql_.clear();
    stmtEnv_.msgBuf_.fill(0);
    qDestroyQuery(stmtEnv_.pQuery_);

    res_.ast_.clear();
    res_.boundAst_.clear();
    res_.rawLogicPlan_.clear();
    res_.optimizedLogicPlan_.clear();
    res_.splitLogicPlan_.clear();
    res_.scaledLogicPlan_.clear();
    res_.physiPlan_.clear();
    res_.physiSubplans_.clear();
  }

  void dump(DumpModule module) {
    if (DUMP_MODULE_NOTHING == module) {
      return;
    }

    cout << "==========================================sql : [" << stmtEnv_.sql_ << "]" << endl;

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_PARSER == module) {
      if (res_.prepareAst_.empty()) {
        cout << "+++++++++++++++++++++syntax tree : " << endl;
        cout << res_.ast_ << endl;
      } else {
        cout << "+++++++++++++++++++++prepare syntax tree : " << endl;
        cout << res_.prepareAst_ << endl;
        cout << "+++++++++++++++++++++bound syntax tree : " << endl;
        cout << res_.boundAst_ << endl;
        cout << "+++++++++++++++++++++syntax tree : " << endl;
        cout << res_.ast_ << endl;
      }
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_LOGIC == module) {
      cout << "+++++++++++++++++++++raw logic plan : " << endl;
      cout << res_.rawLogicPlan_ << endl;
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_OPTIMIZED == module) {
      cout << "+++++++++++++++++++++optimized logic plan : " << endl;
      cout << res_.optimizedLogicPlan_ << endl;
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_SPLIT == module) {
      cout << "+++++++++++++++++++++split logic plan : " << endl;
      cout << res_.splitLogicPlan_ << endl;
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_SCALED == module) {
      cout << "+++++++++++++++++++++scaled logic plan : " << endl;
      cout << res_.scaledLogicPlan_ << endl;
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_PHYSICAL == module) {
      cout << "+++++++++++++++++++++physical plan : " << endl;
      cout << res_.physiPlan_ << endl;
    }

    if (DUMP_MODULE_ALL == module || DUMP_MODULE_SUBPLAN == module) {
      cout << "+++++++++++++++++++++physical subplan : " << endl;
      for (const auto& subplan : res_.physiSubplans_) {
        cout << subplan << endl;
      }
    }
  }

  void doParseSql(const string& sql, SQuery** pQuery, bool prepare = false) {
    stmtEnv_.sql_ = sql;
    transform(stmtEnv_.sql_.begin(), stmtEnv_.sql_.end(), stmtEnv_.sql_.begin(), ::tolower);

    SParseContext cxt = {0};
    cxt.acctId = atoi(caseEnv_.acctId_.c_str());
    cxt.db = caseEnv_.db_.c_str();
    cxt.pSql = stmtEnv_.sql_.c_str();
    cxt.sqlLen = stmtEnv_.sql_.length();
    cxt.pMsg = stmtEnv_.msgBuf_.data();
    cxt.msgLen = stmtEnv_.msgBuf_.max_size();

    DO_WITH_THROW(qParseSql, &cxt, pQuery);
    if (prepare) {
      res_.prepareAst_ = toString((*pQuery)->pPrepareRoot);
    } else {
      res_.ast_ = toString((*pQuery)->pRoot);
    }
  }

  void doBindParams(SQuery* pQuery, TAOS_MULTI_BIND* pParams, int32_t colIdx) {
    DO_WITH_THROW(qStmtBindParams, pQuery, pParams, colIdx);
    if (colIdx < 0 || pQuery->placeholderNum == colIdx + 1) {
      res_.boundAst_ = toString(pQuery->pRoot);
    }
  }

  void doParseBoundSql(SQuery* pQuery) {
    SParseContext cxt = {0};
    cxt.acctId = atoi(caseEnv_.acctId_.c_str());
    cxt.db = caseEnv_.db_.c_str();
    cxt.pSql = stmtEnv_.sql_.c_str();
    cxt.sqlLen = stmtEnv_.sql_.length();
    cxt.pMsg = stmtEnv_.msgBuf_.data();
    cxt.msgLen = stmtEnv_.msgBuf_.max_size();

    DO_WITH_THROW(qStmtParseQuerySql, &cxt, pQuery);
    res_.ast_ = toString(pQuery->pRoot);
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
      FOREACH(pSubplan, ((SNodeListNode*)pNode)->pNodeList) { res_.physiSubplans_.push_back(toString(pSubplan)); }
    }
  }

  void setPlanContext(SQuery* pQuery, SPlanContext* pCxt) {
    pCxt->queryId = 1;
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
    char*   pStr = NULL;
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

PlannerTestBase::PlannerTestBase() : impl_(new PlannerTestBaseImpl()) {}

PlannerTestBase::~PlannerTestBase() {}

void PlannerTestBase::useDb(const std::string& acctId, const std::string& db) { impl_->useDb(acctId, db); }

void PlannerTestBase::run(const std::string& sql) { return impl_->run(sql); }

void PlannerTestBase::prepare(const std::string& sql) { return impl_->prepare(sql); }

void PlannerTestBase::bindParams(TAOS_MULTI_BIND* pParams, int32_t colIdx) {
  return impl_->bindParams(pParams, colIdx);
}

void PlannerTestBase::exec() { return impl_->exec(); }
