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
#include <chrono>

#include "cmdnodes.h"
#include "mockCatalogService.h"
#include "parser.h"
#include "planInt.h"
#include "tglobal.h"

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
  DUMP_MODULE_SQL,
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
int32_t    g_limitSql = 0;
int32_t    g_logLevel = 131;
int32_t    g_queryPolicy = QUERY_POLICY_VNODE;
bool       g_useNodeAllocator = false;

void setDumpModule(const char* pModule) {
  if (NULL == pModule) {
    g_dumpModule = DUMP_MODULE_ALL;
  } else if (0 == strncasecmp(pModule, "sql", strlen(pModule))) {
    g_dumpModule = DUMP_MODULE_SQL;
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

void setSkipSqlNum(const char* pArg) { g_skipSql = stoi(pArg); }
void setLimitSqlNum(const char* pArg) { g_limitSql = stoi(pArg); }
void setLogLevel(const char* pArg) { g_logLevel = stoi(pArg); }
void setQueryPolicy(const char* pArg) { g_queryPolicy = stoi(pArg); }
void setUseNodeAllocator(const char* pArg) { g_useNodeAllocator = stoi(pArg); }

int32_t getLogLevel() { return g_logLevel; }

class PlannerTestBaseImpl {
 public:
  PlannerTestBaseImpl() : sqlNo_(0), sqlNum_(0) {}

  void useDb(const string& user, const string& db) {
    caseEnv_.acctId_ = 0;
    caseEnv_.user_ = user;
    caseEnv_.db_ = db;
    caseEnv_.numOfSkipSql_ = g_skipSql;
    caseEnv_.numOfLimitSql_ = g_limitSql;
  }

  void run(const string& sql) {
    ++sqlNo_;
    if (caseEnv_.numOfSkipSql_ > 0) {
      --(caseEnv_.numOfSkipSql_);
      return;
    }
    if (caseEnv_.numOfLimitSql_ > 0 && caseEnv_.numOfLimitSql_ == sqlNum_) {
      return;
    }
    ++sqlNum_;

    switch (g_queryPolicy) {
      case QUERY_POLICY_VNODE:
      case QUERY_POLICY_HYBRID:
      case QUERY_POLICY_QNODE:
        runImpl(sql, g_queryPolicy);
        break;
      default:
        runImpl(sql, QUERY_POLICY_VNODE);
        runImpl(sql, QUERY_POLICY_HYBRID);
        runImpl(sql, QUERY_POLICY_QNODE);
        break;
    }
  }

  void runImpl(const string& sql, int32_t queryPolicy) {
    int64_t allocatorId = 0;
    if (g_useNodeAllocator) {
      nodesCreateAllocator(sqlNo_, 32 * 1024, &allocatorId);
      nodesAcquireAllocator(allocatorId);
    }

    reset();
    tsQueryPolicy = queryPolicy;
    try {
      unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), _destroyQuery);
      doParseSql(sql, query.get());
      SQuery* pQuery = *(query.get());

      SPlanContext cxt = {0};
      setPlanContext(pQuery, &cxt);

      SLogicSubplan* pLogicSubplan = nullptr;
      doCreateLogicPlan(&cxt, &pLogicSubplan);
      unique_ptr<SLogicSubplan, void (*)(SLogicSubplan*)> logicSubplan(pLogicSubplan,
                                                                       (void (*)(SLogicSubplan*))nodesDestroyNode);

      doOptimizeLogicPlan(&cxt, pLogicSubplan);

      doSplitLogicPlan(&cxt, pLogicSubplan);

      SQueryLogicPlan* pLogicPlan = nullptr;
      doScaleOutLogicPlan(&cxt, pLogicSubplan, &pLogicPlan);
      unique_ptr<SQueryLogicPlan, void (*)(SQueryLogicPlan*)> logicPlan(pLogicPlan,
                                                                        (void (*)(SQueryLogicPlan*))nodesDestroyNode);

      SQueryPlan* pPlan = nullptr;
      doCreatePhysiPlan(&cxt, pLogicPlan, &pPlan);
      unique_ptr<SQueryPlan, void (*)(SQueryPlan*)> plan(pPlan, (void (*)(SQueryPlan*))nodesDestroyNode);

      dump(g_dumpModule);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      nodesReleaseAllocator(allocatorId);
      nodesDestroyAllocator(allocatorId);
      throw;
    }

    nodesReleaseAllocator(allocatorId);
    nodesDestroyAllocator(allocatorId);
  }

  void prepare(const string& sql) {
    if (caseEnv_.numOfSkipSql_ > 0) {
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
    if (caseEnv_.numOfSkipSql_ > 0) {
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
    if (caseEnv_.numOfSkipSql_ > 0) {
      --(caseEnv_.numOfSkipSql_);
      return;
    }

    try {
      doParseBoundSql(stmtEnv_.pQuery_);

      SPlanContext cxt = {0};
      setPlanContext(stmtEnv_.pQuery_, &cxt);

      SLogicSubplan* pLogicSubplan = nullptr;
      doCreateLogicPlan(&cxt, &pLogicSubplan);
      unique_ptr<SLogicSubplan, void (*)(SLogicSubplan*)> logicSubplan(pLogicSubplan,
                                                                       (void (*)(SLogicSubplan*))nodesDestroyNode);

      doOptimizeLogicPlan(&cxt, pLogicSubplan);

      doSplitLogicPlan(&cxt, pLogicSubplan);

      SQueryLogicPlan* pLogicPlan = nullptr;
      doScaleOutLogicPlan(&cxt, pLogicSubplan, &pLogicPlan);
      unique_ptr<SQueryLogicPlan, void (*)(SQueryLogicPlan*)> logicPlan(pLogicPlan,
                                                                        (void (*)(SQueryLogicPlan*))nodesDestroyNode);

      SQueryPlan* pPlan = nullptr;
      doCreatePhysiPlan(&cxt, pLogicPlan, &pPlan);
      unique_ptr<SQueryPlan, void (*)(SQueryPlan*)> plan(pPlan, (void (*)(SQueryPlan*))nodesDestroyNode);

      checkPlanMsg((SNode*)pPlan);

      dump(g_dumpModule);
    } catch (...) {
      dump(DUMP_MODULE_ALL);
      throw;
    }
  }

 private:
  struct caseEnv {
    int32_t acctId_;
    string  user_;
    string  db_;
    int32_t numOfSkipSql_;
    int32_t numOfLimitSql_;

    caseEnv() : numOfSkipSql_(0) {}
  };

  struct stmtEnv {
    string            sql_;
    array<char, 1024> msgBuf_;
    SQuery*           pQuery_;

    stmtEnv() : pQuery_(nullptr) {}
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

  static void _destroyQuery(SQuery** pQuery) {
    if (nullptr == pQuery) {
      return;
    }
    qDestroyQuery(*pQuery);
    taosMemoryFree(pQuery);
  }

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

    cout << "========================================== " << sqlNo_ << " sql : [" << stmtEnv_.sql_ << "]" << endl;

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
    cxt.acctId = caseEnv_.acctId_;
    cxt.db = caseEnv_.db_.c_str();
    cxt.pSql = stmtEnv_.sql_.c_str();
    cxt.sqlLen = stmtEnv_.sql_.length();
    cxt.pMsg = stmtEnv_.msgBuf_.data();
    cxt.msgLen = stmtEnv_.msgBuf_.max_size();
    cxt.svrVer = "3.0.0.0";
    cxt.enableSysInfo = true;
    if (prepare) {
      SStmtCallback stmtCb = {0};
      cxt.pStmtCb = &stmtCb;
    }

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
    cxt.acctId = caseEnv_.acctId_;
    cxt.db = caseEnv_.db_.c_str();
    cxt.pSql = stmtEnv_.sql_.c_str();
    cxt.sqlLen = stmtEnv_.sql_.length();
    cxt.pMsg = stmtEnv_.msgBuf_.data();
    cxt.msgLen = stmtEnv_.msgBuf_.max_size();
    cxt.pUser = caseEnv_.user_.c_str();

    DO_WITH_THROW(qStmtParseQuerySql, &cxt, pQuery);
    res_.ast_ = toString(pQuery->pRoot);
  }

  void doCreateLogicPlan(SPlanContext* pCxt, SLogicSubplan** pLogicSubplan) {
    DO_WITH_THROW(createLogicPlan, pCxt, pLogicSubplan);
    res_.rawLogicPlan_ = toString((SNode*)(*pLogicSubplan));
  }

  void doOptimizeLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
    DO_WITH_THROW(optimizeLogicPlan, pCxt, pLogicSubplan);
    res_.optimizedLogicPlan_ = toString((SNode*)pLogicSubplan);
  }

  void doSplitLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
    DO_WITH_THROW(splitLogicPlan, pCxt, pLogicSubplan);
    res_.splitLogicPlan_ = toString((SNode*)(pLogicSubplan));
  }

  void doScaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan) {
    DO_WITH_THROW(scaleOutLogicPlan, pCxt, pLogicSubplan, pLogicPlan);
    res_.scaledLogicPlan_ = toString((SNode*)(*pLogicPlan));
  }

  void doCreatePhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan) {
    unique_ptr<SArray, void (*)(SArray*)> execNodeList((SArray*)taosArrayInit(TARRAY_MIN_SIZE, sizeof(SQueryNodeAddr)),
                                                       (void (*)(SArray*))taosArrayDestroy);
    DO_WITH_THROW(createPhysiPlan, pCxt, pLogicPlan, pPlan, execNodeList.get());
    res_.physiPlan_ = toString((SNode*)(*pPlan));
    SNode* pNode;
    FOREACH(pNode, (*pPlan)->pSubplans) {
      SNode* pSubplan;
      FOREACH(pSubplan, ((SNodeListNode*)pNode)->pNodeList) { res_.physiSubplans_.push_back(toString(pSubplan)); }
    }
  }

  void setPlanContext(SQuery* pQuery, SPlanContext* pCxt) {
    pCxt->queryId = 1;
    pCxt->pUser = caseEnv_.user_.c_str();
    if (QUERY_NODE_CREATE_TOPIC_STMT == nodeType(pQuery->pRoot)) {
      SCreateTopicStmt* pStmt = (SCreateTopicStmt*)pQuery->pRoot;
      pCxt->pAstRoot = pStmt->pQuery;
      pStmt->pQuery = nullptr;
      nodesDestroyNode(pQuery->pRoot);
      pQuery->pRoot = pCxt->pAstRoot;
      pCxt->topicQuery = true;
    } else if (QUERY_NODE_CREATE_INDEX_STMT == nodeType(pQuery->pRoot)) {
      SMCreateSmaReq req = {0};
      SCreateIndexStmt* pStmt = (SCreateIndexStmt*)pQuery->pRoot;
      SCmdMsgInfo* pCmdMsg = (SCmdMsgInfo*)taosMemoryMalloc(sizeof(SCmdMsgInfo));
      if (NULL == pCmdMsg) FAIL();
      pCmdMsg->msgType = TDMT_MND_CREATE_SMA;
      pCmdMsg->msgLen = tSerializeSMCreateSmaReq(NULL, 0, pStmt->pReq);
      pCmdMsg->pMsg = taosMemoryMalloc(pCmdMsg->msgLen);
      if (!pCmdMsg->pMsg) FAIL();
      tSerializeSMCreateSmaReq(pCmdMsg->pMsg, pCmdMsg->msgLen, pStmt->pReq);
      ((SQuery*)pQuery)->pCmdMsg = pCmdMsg;

      tDeserializeSMCreateSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req);
      g_mockCatalogService->createSmaIndex(&req);
      nodesStringToNode(req.ast, &pCxt->pAstRoot);
      pCxt->deleteMark = req.deleteMark;
      tFreeSMCreateSmaReq(&req);
      nodesDestroyNode(pQuery->pRoot);
      pQuery->pRoot = pCxt->pAstRoot;
      pCxt->streamQuery = true;
    } else if (QUERY_NODE_CREATE_STREAM_STMT == nodeType(pQuery->pRoot)) {
      SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
      pCxt->pAstRoot = pStmt->pQuery;
      pStmt->pQuery = nullptr;
      pCxt->streamQuery = true;
      pCxt->triggerType = pStmt->pOptions->triggerType;
      pCxt->watermark = (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
      nodesDestroyNode(pQuery->pRoot);
      pQuery->pRoot = pCxt->pAstRoot;
    } else {
      pCxt->pAstRoot = pQuery->pRoot;
    }
  }

  string toString(const SNode* pRoot) {
    char*   pStr = NULL;
    int32_t len = 0;
    DO_WITH_THROW(nodesNodeToString, pRoot, false, &pStr, &len)
    // check toObject
    SNode* pCopy = NULL;
    DO_WITH_THROW(nodesStringToNode, pStr, &pCopy)
    nodesDestroyNode(pCopy);
    string str(pStr);
    taosMemoryFreeClear(pStr);

    return str;
  }

  void checkPlanMsg(const SNode* pRoot) {
    char*   pStr = NULL;
    int32_t len = 0;
    DO_WITH_THROW(nodesNodeToMsg, pRoot, &pStr, &len)

    string  copyStr(pStr, len);
    SNode*  pNode = NULL;
    char*   pNewStr = NULL;
    int32_t newlen = 0;
    DO_WITH_THROW(nodesMsgToNode, copyStr.c_str(), len, &pNode)
    DO_WITH_THROW(nodesNodeToMsg, pNode, &pNewStr, &newlen)
    if (newlen != len || 0 != memcmp(pStr, pNewStr, len)) {
      cout << "nodesNodeToMsg error!!!!!!!!!!!!!! len = " << len << ", newlen = " << newlen << endl;
      taosMemoryFreeClear(pNewStr);
      DO_WITH_THROW(nodesNodeToString, pRoot, false, &pNewStr, &newlen)
      cout << "orac node: " << pNewStr << endl;
      taosMemoryFreeClear(pNewStr);
      DO_WITH_THROW(nodesNodeToString, pNode, false, &pNewStr, &newlen)
      cout << "new node: " << pNewStr << endl;
    }
    nodesDestroyNode(pNode);
    taosMemoryFreeClear(pNewStr);

    taosMemoryFreeClear(pStr);
  }

  caseEnv caseEnv_;
  stmtEnv stmtEnv_;
  stmtRes res_;
  int32_t sqlNo_;
  int32_t sqlNum_;
};

PlannerTestBase::PlannerTestBase() : impl_(new PlannerTestBaseImpl()) {}

PlannerTestBase::~PlannerTestBase() {}

void PlannerTestBase::useDb(const std::string& user, const std::string& db) { impl_->useDb(user, db); }

void PlannerTestBase::run(const std::string& sql) { return impl_->run(sql); }

void PlannerTestBase::prepare(const std::string& sql) { return impl_->prepare(sql); }

void PlannerTestBase::bindParams(TAOS_MULTI_BIND* pParams, int32_t colIdx) {
  return impl_->bindParams(pParams, colIdx);
}

void PlannerTestBase::exec() { return impl_->exec(); }
