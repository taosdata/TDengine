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

#include "parTestUtil.h"

#include <algorithm>
#include <array>
#include <thread>

#include "catalog.h"
#include "mockCatalogService.h"
#include "parInt.h"

using namespace std;
using namespace std::placeholders;
using namespace testing;

namespace ParserTest {

#define DO_WITH_THROW(func, ...)                                                                                     \
  do {                                                                                                               \
    int32_t code__ = func(__VA_ARGS__);                                                                              \
    if (!checkResultCode(#func, code__)) {                                                                           \
      if (TSDB_CODE_SUCCESS != code__) {                                                                             \
        throw runtime_error("sql:[" + stmtEnv_.sql_ + "] " #func " code:" + to_string(code__) +                      \
                            ", strerror:" + string(tstrerror(code__)) + ", msg:" + string(stmtEnv_.msgBuf_.data())); \
      } else {                                                                                                       \
        throw runtime_error("sql:[" + stmtEnv_.sql_ + "] " #func " expect " + to_string(stmtEnv_.expect_) +          \
                            " actual " + to_string(code__));                                                         \
      }                                                                                                              \
    } else if (TSDB_CODE_SUCCESS != code__) {                                                                        \
      throw TerminateFlag();                                                                                         \
    }                                                                                                                \
  } while (0);

bool    g_dump = false;
bool    g_testAsyncApis = true;
int32_t g_logLevel = 131;
int32_t g_skipSql = 0;
int32_t g_limitSql = 0;

void setAsyncFlag(const char* pArg) { g_testAsyncApis = stoi(pArg) > 0 ? true : false; }
void setSkipSqlNum(const char* pArg) { g_skipSql = stoi(pArg); }
void setLimitSqlNum(const char* pArg) { g_limitSql = stoi(pArg); }

struct TerminateFlag : public exception {
  const char* what() const throw() { return "success and terminate"; }
};

void setLogLevel(const char* pLogLevel) { g_logLevel = stoi(pLogLevel); }

int32_t getLogLevel() { return g_logLevel; }

class ParserTestBaseImpl {
 public:
  ParserTestBaseImpl(ParserTestBase* pBase) : pBase_(pBase), sqlNo_(0), sqlNum_(0) {
    caseEnv_.numOfSkipSql_ = g_skipSql;
    caseEnv_.numOfLimitSql_ = g_limitSql;
  }

  void login(const std::string& user) { caseEnv_.user_ = user; }

  void useDb(const string& acctId, const string& db) {
    caseEnv_.acctId_ = acctId;
    caseEnv_.db_ = db;
  }

  void run(const string& sql, int32_t expect, ParserStage checkStage) {
    ++sqlNo_;
    if (caseEnv_.numOfSkipSql_ > 0) {
      --(caseEnv_.numOfSkipSql_);
      return;
    }
    if (caseEnv_.numOfLimitSql_ > 0 && caseEnv_.numOfLimitSql_ == sqlNum_) {
      return;
    }
    ++sqlNum_;

    runInternalFuncs(sql, expect, checkStage);
    runApis(sql, expect, checkStage);

    if (g_testAsyncApis) {
      runAsyncInternalFuncs(sql, expect, checkStage);
      runAsyncApis(sql, expect, checkStage);
    }
  }

 private:
  struct caseEnv {
    string  acctId_;
    string  user_;
    string  db_;
    int32_t numOfSkipSql_;
    int32_t numOfLimitSql_;

    caseEnv() : user_("wangxiaoyu"), numOfSkipSql_(0) {}
  };

  struct stmtEnv {
    string            sql_;
    array<char, 1024> msgBuf_;
    int32_t           expect_;
    string            checkFunc_;
  };

  struct stmtRes {
    string parsedAst_;
    string translatedAst_;
    string calcConstAst_;
  };

  enum TestInterfaceType {
    TEST_INTERFACE_INTERNAL = 1,
    TEST_INTERFACE_API,
    TEST_INTERFACE_ASYNC_INTERNAL,
    TEST_INTERFACE_ASYNC_API
  };

  static void destoryParseContext(SParseContext* pCxt) {
    taosArrayDestroy(pCxt->pTableMetaPos);
    taosArrayDestroy(pCxt->pTableVgroupPos);
    delete pCxt;
  }

  static void destoryParseMetaCacheWarpper(SParseMetaCache* pMetaCache, bool request) {
    destoryParseMetaCache(pMetaCache, request);
    delete pMetaCache;
  }

  static void destroyQuery(SQuery** pQuery) {
    if (nullptr == pQuery) {
      return;
    }
    qDestroyQuery(*pQuery);
    taosMemoryFree(pQuery);
  }

  bool checkResultCode(const string& pFunc, int32_t resultCode) {
    return !(stmtEnv_.checkFunc_.empty())
               ? ((stmtEnv_.checkFunc_ == pFunc) ? stmtEnv_.expect_ == resultCode : TSDB_CODE_SUCCESS == resultCode)
               : true;
  }

  string stageFunc(ParserStage stage, TestInterfaceType type) {
    switch (type) {
      case TEST_INTERFACE_INTERNAL:
      case TEST_INTERFACE_ASYNC_INTERNAL:
        switch (stage) {
          case PARSER_STAGE_PARSE:
            return "parse";
          case PARSER_STAGE_TRANSLATE:
            return "translate";
          case PARSER_STAGE_CALC_CONST:
            return "calculateConstant";
          default:
            break;
        }
        break;
      case TEST_INTERFACE_API:
        return "qParseSql";
      case TEST_INTERFACE_ASYNC_API:
        switch (stage) {
          case PARSER_STAGE_PARSE:
            return "qParseSqlSyntax";
          case PARSER_STAGE_TRANSLATE:
          case PARSER_STAGE_CALC_CONST:
            return "qAnalyseSqlSemantic";
          default:
            break;
        }
        break;
      default:
        break;
    }
    return "unknown";
  }

  void reset(int32_t expect, ParserStage checkStage, TestInterfaceType type) {
    stmtEnv_.sql_.clear();
    stmtEnv_.msgBuf_.fill(0);
    stmtEnv_.expect_ = expect;
    stmtEnv_.checkFunc_ = stageFunc(checkStage, type);

    res_.parsedAst_.clear();
    res_.translatedAst_.clear();
    res_.calcConstAst_.clear();
  }

  void dump() {
    cout << "========================================== " << sqlNo_ << " sql : [" << stmtEnv_.sql_ << "]" << endl;
    if (!res_.parsedAst_.empty()) {
      cout << "raw syntax tree : " << endl;
      cout << res_.parsedAst_ << endl;
    }
    if (!res_.translatedAst_.empty()) {
      cout << "translated syntax tree : " << endl;
      cout << res_.translatedAst_ << endl;
    }
    if (!res_.calcConstAst_.empty()) {
      cout << "optimized syntax tree : " << endl;
      cout << res_.calcConstAst_ << endl;
    }
  }

  void setParseContext(const string& sql, SParseContext* pCxt, bool async = false) {
    stmtEnv_.sql_ = sql;
    strtolower((char*)stmtEnv_.sql_.c_str(), sql.c_str());

    pCxt->acctId = atoi(caseEnv_.acctId_.c_str());
    pCxt->db = caseEnv_.db_.c_str();
    pCxt->pUser = caseEnv_.user_.c_str();
    pCxt->isSuperUser = caseEnv_.user_ == "root";
    pCxt->enableSysInfo = true;
    pCxt->pSql = stmtEnv_.sql_.c_str();
    pCxt->sqlLen = stmtEnv_.sql_.length();
    pCxt->pMsg = stmtEnv_.msgBuf_.data();
    pCxt->msgLen = stmtEnv_.msgBuf_.max_size();
    pCxt->async = async;
    pCxt->svrVer = "3.0.0.0";
  }

  void doParse(SParseContext* pCxt, SQuery** pQuery) {
    DO_WITH_THROW(parse, pCxt, pQuery);
    ASSERT_NE(*pQuery, nullptr);
    res_.parsedAst_ = toString((*pQuery)->pRoot);
  }

  void doCollectMetaKey(SParseContext* pCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
    DO_WITH_THROW(collectMetaKey, pCxt, pQuery, pMetaCache);
  }

  void doBuildCatalogReq(SParseContext* pCxt, const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq) {
    DO_WITH_THROW(buildCatalogReq, pMetaCache, pCatalogReq);
  }

  void doGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) {
    DO_WITH_THROW(g_mockCatalogService->catalogGetAllMeta, pCatalogReq, pMetaData);
  }

  void doPutMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache) {
    DO_WITH_THROW(putMetaDataToCache, pCatalogReq, pMetaData, pMetaCache);
  }

  void doAuthenticate(SParseContext* pCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
    DO_WITH_THROW(authenticate, pCxt, pQuery, pMetaCache);
  }

  void doTranslate(SParseContext* pCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
    DO_WITH_THROW(translate, pCxt, pQuery, pMetaCache);
    checkQuery(pQuery, PARSER_STAGE_TRANSLATE);
    res_.translatedAst_ = toString(pQuery->pRoot);
  }

  void doCalculateConstant(SParseContext* pCxt, SQuery* pQuery) {
    DO_WITH_THROW(calculateConstant, pCxt, pQuery);
    res_.calcConstAst_ = toString(pQuery->pRoot);
  }

  void doParseSql(SParseContext* pCxt, SQuery** pQuery) {
    DO_WITH_THROW(qParseSql, pCxt, pQuery);
    ASSERT_NE(*pQuery, nullptr);
    res_.calcConstAst_ = toString((*pQuery)->pRoot);
  }

  void doParseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, SCatalogReq* pCatalogReq) {
    DO_WITH_THROW(qParseSqlSyntax, pCxt, pQuery, pCatalogReq);
    ASSERT_NE(*pQuery, nullptr);
    if (nullptr != (*pQuery)->pRoot) {
      res_.parsedAst_ = toString((*pQuery)->pRoot);
    }
  }

  void doAnalyseSqlSemantic(SParseContext* pCxt, const SCatalogReq* pCatalogReq, const SMetaData* pMetaData,
                            SQuery* pQuery) {
    DO_WITH_THROW(qAnalyseSqlSemantic, pCxt, pCatalogReq, pMetaData, pQuery);
    res_.calcConstAst_ = toString(pQuery->pRoot);
  }

  void doParseInsertSql(SParseContext* pCxt, SQuery** pQuery, SCatalogReq* pCatalogReq, const SMetaData* pMetaData) {
    DO_WITH_THROW(parseInsertSql, pCxt, pQuery, pCatalogReq, pMetaData);
    ASSERT_NE(*pQuery, nullptr);
    res_.parsedAst_ = toString((*pQuery)->pRoot);
  }

  void doContinueParseSql(SParseContext* pCxt, SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SQuery* pQuery) {
    DO_WITH_THROW(qContinueParseSql, pCxt, pCatalogReq, pMetaData, pQuery);
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

  void checkQuery(const SQuery* pQuery, ParserStage stage) { pBase_->checkDdl(pQuery, stage); }

  void runInternalFuncs(const string& sql, int32_t expect, ParserStage checkStage) {
    reset(expect, checkStage, TEST_INTERFACE_INTERNAL);
    try {
      SParseContext cxt = {0};
      setParseContext(sql, &cxt);

      if (qIsInsertValuesSql(cxt.pSql, cxt.sqlLen)) {
        unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
        doParseInsertSql(&cxt, query.get(), nullptr, nullptr);
      } else {
        unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
        doParse(&cxt, query.get());
        SQuery* pQuery = *(query.get());

        doAuthenticate(&cxt, pQuery, nullptr);

        doTranslate(&cxt, pQuery, nullptr);

        doCalculateConstant(&cxt, pQuery);
      }

      if (g_dump) {
        dump();
      }
    } catch (const TerminateFlag& e) {
      // success and terminate
      return;
    } catch (...) {
      dump();
      throw;
    }
  }

  void runApis(const string& sql, int32_t expect, ParserStage checkStage) {
    reset(expect, checkStage, TEST_INTERFACE_API);
    try {
      SParseContext cxt = {0};
      setParseContext(sql, &cxt);

      unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
      doParseSql(&cxt, query.get());

      if (g_dump) {
        dump();
      }
    } catch (const TerminateFlag& e) {
      // success and terminate
      return;
    } catch (...) {
      dump();
      throw;
    }
  }

  void runQueryAsyncInternalFuncs(SParseContext* pParCxt) {
    unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
    bool                                    request = true;
    unique_ptr<SParseMetaCache, function<void(SParseMetaCache*)> > metaCache(
        new SParseMetaCache(), bind(destoryParseMetaCacheWarpper, _1, cref(request)));
    doParse(pParCxt, query.get());
    doCollectMetaKey(pParCxt, *(query.get()), metaCache.get());

    SQuery* pQuery = *(query.get());

    unique_ptr<SCatalogReq, void (*)(SCatalogReq*)> catalogReq(new SCatalogReq(),
                                                               MockCatalogService::destoryCatalogReq);
    doBuildCatalogReq(pParCxt, metaCache.get(), catalogReq.get());

    string err;
    thread t1([&]() {
      try {
        unique_ptr<SMetaData, void (*)(SMetaData*)> metaData(new SMetaData(), MockCatalogService::destoryMetaData);
        doGetAllMeta(catalogReq.get(), metaData.get());

        metaCache.reset(new SParseMetaCache());
        request = false;
        doPutMetaDataToCache(catalogReq.get(), metaData.get(), metaCache.get());

        doAuthenticate(pParCxt, pQuery, metaCache.get());

        doTranslate(pParCxt, pQuery, metaCache.get());

        doCalculateConstant(pParCxt, pQuery);
      } catch (const TerminateFlag& e) {
        // success and terminate
      } catch (const runtime_error& e) {
        err = e.what();
      } catch (...) {
        err = "unknown error";
      }
    });

    t1.join();
    if (!err.empty()) {
      throw runtime_error(err);
    }
  }

  void runInsertAsyncInternalFuncsImpl(SParseContext* pParCxt, SQuery** pQuery, SCatalogReq* pCatalogReq,
                                       SMetaData* pMetaData) {
    doParseInsertSql(pParCxt, pQuery, pCatalogReq, pMetaData);

    if (QUERY_EXEC_STAGE_SCHEDULE == (*pQuery)->execStage) {
      return;
    }

    string err;
    thread t1([&]() {
      try {
        doGetAllMeta(pCatalogReq, pMetaData);

        doParseInsertSql(pParCxt, pQuery, pCatalogReq, pMetaData);

        if (QUERY_EXEC_STAGE_SCHEDULE != (*pQuery)->execStage) {
          runInsertAsyncInternalFuncsImpl(pParCxt, pQuery, pCatalogReq, pMetaData);
        }
      } catch (const TerminateFlag& e) {
        // success and terminate
      } catch (const runtime_error& e) {
        err = e.what();
      } catch (...) {
        err = "unknown error";
      }
    });

    t1.join();
    if (!err.empty()) {
      throw runtime_error(err);
    }
  }

  void runInsertAsyncInternalFuncs(SParseContext* pParCxt) {
    unique_ptr<SQuery*, void (*)(SQuery**)>         query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
    unique_ptr<SCatalogReq, void (*)(SCatalogReq*)> catalogReq(new SCatalogReq(),
                                                               MockCatalogService::destoryCatalogReq);
    unique_ptr<SMetaData, void (*)(SMetaData*)>     metaData(new SMetaData(), MockCatalogService::destoryMetaData);
    runInsertAsyncInternalFuncsImpl(pParCxt, query.get(), catalogReq.get(), metaData.get());
  }

  void runAsyncInternalFuncs(const string& sql, int32_t expect, ParserStage checkStage) {
    reset(expect, checkStage, TEST_INTERFACE_ASYNC_INTERNAL);
    try {
      unique_ptr<SParseContext, function<void(SParseContext*)> > cxt(new SParseContext(), destoryParseContext);
      setParseContext(sql, cxt.get(), true);

      bool isInsertValues = qIsInsertValuesSql(cxt->pSql, cxt->sqlLen);
      if (isInsertValues) {
        runInsertAsyncInternalFuncs(cxt.get());
      } else {
        runQueryAsyncInternalFuncs(cxt.get());
      }

      if (g_dump) {
        dump();
      }
    } catch (const TerminateFlag& e) {
      // success and terminate
      return;
    } catch (...) {
      dump();
      throw;
    }
  }

  void runAsyncApis(const string& sql, int32_t expect, ParserStage checkStage) {
    reset(expect, checkStage, TEST_INTERFACE_ASYNC_API);
    try {
      unique_ptr<SParseContext, function<void(SParseContext*)> > cxt(new SParseContext(), destoryParseContext);
      setParseContext(sql, cxt.get());

      unique_ptr<SCatalogReq, void (*)(SCatalogReq*)> catalogReq(new SCatalogReq(),
                                                                 MockCatalogService::destoryCatalogReq);
      unique_ptr<SQuery*, void (*)(SQuery**)> query((SQuery**)taosMemoryCalloc(1, sizeof(SQuery*)), destroyQuery);
      doParseSqlSyntax(cxt.get(), query.get(), catalogReq.get());
      SQuery* pQuery = *(query.get());

      switch (pQuery->execStage) {
        case QUERY_EXEC_STAGE_PARSE:
        case QUERY_EXEC_STAGE_ANALYSE: {
          string err;
          thread t1([&]() {
            try {
              unique_ptr<SMetaData, void (*)(SMetaData*)> metaData(new SMetaData(),
                                                                   MockCatalogService::destoryMetaData);
              doGetAllMeta(catalogReq.get(), metaData.get());
              if (QUERY_EXEC_STAGE_PARSE == pQuery->execStage) {
                doContinueParseSql(cxt.get(), catalogReq.get(), metaData.get(), pQuery);
              } else {
                doAnalyseSqlSemantic(cxt.get(), catalogReq.get(), metaData.get(), pQuery);
              }
            } catch (const TerminateFlag& e) {
              // success and terminate
            } catch (const runtime_error& e) {
              err = e.what();
            } catch (...) {
              err = "unknown error";
            }
          });

          t1.join();
          if (!err.empty()) {
            throw runtime_error(err);
          }
          break;
        }
        case QUERY_EXEC_STAGE_SCHEDULE:
          break;
        default:
          break;
      }

      if (g_dump) {
        dump();
      }
    } catch (const TerminateFlag& e) {
      // success and terminate
      return;
    } catch (...) {
      dump();
      throw;
    }
  }

  caseEnv         caseEnv_;
  stmtEnv         stmtEnv_;
  stmtRes         res_;
  ParserTestBase* pBase_;
  int32_t         sqlNo_;
  int32_t         sqlNum_;
};

ParserTestBase::ParserTestBase() : impl_(new ParserTestBaseImpl(this)) {}

ParserTestBase::~ParserTestBase() {}

void ParserTestBase::login(const std::string& user) { return impl_->login(user); }

void ParserTestBase::useDb(const std::string& acctId, const std::string& db) { impl_->useDb(acctId, db); }

void ParserTestBase::run(const std::string& sql, int32_t expect, ParserStage checkStage) {
  return impl_->run(sql, expect, checkStage);
}

void ParserTestBase::checkDdl(const SQuery* pQuery, ParserStage stage) { return; }

}  // namespace ParserTest
