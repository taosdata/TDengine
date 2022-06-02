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

void setAsyncFlag(const char* pFlag) { g_testAsyncApis = stoi(pFlag) > 0 ? true : false; }
void setSkipSqlNum(const char* pNum) { g_skipSql = stoi(pNum); }

struct TerminateFlag : public exception {
  const char* what() const throw() { return "success and terminate"; }
};

void setLogLevel(const char* pLogLevel) { g_logLevel = stoi(pLogLevel); }

int32_t getLogLevel() { return g_logLevel; }

class ParserTestBaseImpl {
 public:
  ParserTestBaseImpl(ParserTestBase* pBase) : pBase_(pBase) {}

  void login(const std::string& user) { caseEnv_.user_ = user; }

  void useDb(const string& acctId, const string& db) {
    caseEnv_.acctId_ = acctId;
    caseEnv_.db_ = db;
    caseEnv_.nsql_ = g_skipSql;
  }

  void run(const string& sql, int32_t expect, ParserStage checkStage) {
    if (caseEnv_.nsql_ > 0) {
      --(caseEnv_.nsql_);
      return;
    }

    reset(expect, checkStage);
    try {
      SParseContext cxt = {0};
      setParseContext(sql, &cxt);

      SQuery* pQuery = nullptr;
      doParse(&cxt, &pQuery);

      doAuthenticate(&cxt, pQuery);

      doTranslate(&cxt, pQuery);

      doCalculateConstant(&cxt, pQuery);

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

    if (g_testAsyncApis) {
      runAsync(sql, expect, checkStage);
    }
  }

 private:
  struct caseEnv {
    string  acctId_;
    string  user_;
    string  db_;
    int32_t nsql_;

    caseEnv() : user_("wangxiaoyu"), nsql_(0) {}
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

  bool checkResultCode(const string& pFunc, int32_t resultCode) {
    return !(stmtEnv_.checkFunc_.empty())
               ? (("*" == stmtEnv_.checkFunc_ || stmtEnv_.checkFunc_ == pFunc) ? stmtEnv_.expect_ == resultCode
                                                                               : TSDB_CODE_SUCCESS == resultCode)
               : true;
  }

  string stageFunc(ParserStage stage) {
    switch (stage) {
      case PARSER_STAGE_PARSE:
        return "parse";
      case PARSER_STAGE_TRANSLATE:
        return "translate";
      case PARSER_STAGE_CALC_CONST:
        return "calculateConstant";
      case PARSER_STAGE_ALL:
        return "*";
      default:
        break;
    }
    return "unknown";
  }

  void reset(int32_t expect, ParserStage checkStage) {
    stmtEnv_.sql_.clear();
    stmtEnv_.msgBuf_.fill(0);
    stmtEnv_.expect_ = expect;
    stmtEnv_.checkFunc_ = stageFunc(checkStage);

    res_.parsedAst_.clear();
    res_.translatedAst_.clear();
    res_.calcConstAst_.clear();
  }

  void dump() {
    cout << "==========================================sql : [" << stmtEnv_.sql_ << "]" << endl;
    cout << "raw syntax tree : " << endl;
    cout << res_.parsedAst_ << endl;
    cout << "translated syntax tree : " << endl;
    cout << res_.translatedAst_ << endl;
    cout << "optimized syntax tree : " << endl;
    cout << res_.calcConstAst_ << endl;
  }

  void setParseContext(const string& sql, SParseContext* pCxt, bool async = false) {
    stmtEnv_.sql_ = sql;
    transform(stmtEnv_.sql_.begin(), stmtEnv_.sql_.end(), stmtEnv_.sql_.begin(), ::tolower);

    pCxt->acctId = atoi(caseEnv_.acctId_.c_str());
    pCxt->db = caseEnv_.db_.c_str();
    pCxt->pUser = caseEnv_.user_.c_str();
    pCxt->isSuperUser = caseEnv_.user_ == "root";
    pCxt->pSql = stmtEnv_.sql_.c_str();
    pCxt->sqlLen = stmtEnv_.sql_.length();
    pCxt->pMsg = stmtEnv_.msgBuf_.data();
    pCxt->msgLen = stmtEnv_.msgBuf_.max_size();
    pCxt->async = async;
  }

  void doParse(SParseContext* pCxt, SQuery** pQuery) {
    DO_WITH_THROW(parse, pCxt, pQuery);
    ASSERT_NE(*pQuery, nullptr);
    res_.parsedAst_ = toString((*pQuery)->pRoot);
  }

  void doCollectMetaKey(SParseContext* pCxt, SQuery* pQuery) {
    DO_WITH_THROW(collectMetaKey, pCxt, pQuery);
    ASSERT_NE(pQuery->pMetaCache, nullptr);
  }

  void doBuildCatalogReq(const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq) {
    DO_WITH_THROW(buildCatalogReq, pMetaCache, pCatalogReq);
  }

  void doGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) {
    DO_WITH_THROW(g_mockCatalogService->catalogGetAllMeta, pCatalogReq, pMetaData);
  }

  void doPutMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache) {
    DO_WITH_THROW(putMetaDataToCache, pCatalogReq, pMetaData, pMetaCache);
  }

  void doAuthenticate(SParseContext* pCxt, SQuery* pQuery) { DO_WITH_THROW(authenticate, pCxt, pQuery); }

  void doTranslate(SParseContext* pCxt, SQuery* pQuery) {
    DO_WITH_THROW(translate, pCxt, pQuery);
    checkQuery(pQuery, PARSER_STAGE_TRANSLATE);
    res_.translatedAst_ = toString(pQuery->pRoot);
  }

  void doCalculateConstant(SParseContext* pCxt, SQuery* pQuery) {
    DO_WITH_THROW(calculateConstant, pCxt, pQuery);
    res_.calcConstAst_ = toString(pQuery->pRoot);
  }

  string toString(const SNode* pRoot) {
    char*   pStr = NULL;
    int32_t len = 0;
    DO_WITH_THROW(nodesNodeToString, pRoot, false, &pStr, &len)
    string str(pStr);
    taosMemoryFreeClear(pStr);
    return str;
  }

  void checkQuery(const SQuery* pQuery, ParserStage stage) { pBase_->checkDdl(pQuery, stage); }

  void runAsync(const string& sql, int32_t expect, ParserStage checkStage) {
    reset(expect, checkStage);
    try {
      SParseContext cxt = {0};
      setParseContext(sql, &cxt, true);

      SQuery* pQuery = nullptr;
      doParse(&cxt, &pQuery);

      doCollectMetaKey(&cxt, pQuery);

      SCatalogReq catalogReq = {0};
      doBuildCatalogReq(pQuery->pMetaCache, &catalogReq);

      string err;
      thread t1([&]() {
        try {
          SMetaData metaData = {0};
          doGetAllMeta(&catalogReq, &metaData);

          doPutMetaDataToCache(&catalogReq, &metaData, pQuery->pMetaCache);

          doAuthenticate(&cxt, pQuery);

          doTranslate(&cxt, pQuery);

          doCalculateConstant(&cxt, pQuery);
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
