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

#include "parInt.h"

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

bool g_isDump = false;

class ParserTestBaseImpl {
 public:
  void useDb(const string& acctId, const string& db) {
    caseEnv_.acctId_ = acctId;
    caseEnv_.db_ = db;
  }

  void run(const string& sql) {
    reset();
    try {
      SParseContext cxt = {0};
      setParseContext(sql, &cxt);

      SQuery* pQuery = nullptr;
      doParse(&cxt, &pQuery);

      doTranslate(&cxt, pQuery);

      doCalculateConstant(&cxt, pQuery);

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
    string            sql_;
    array<char, 1024> msgBuf_;
  };

  struct stmtRes {
    string parsedAst_;
    string translatedAst_;
    string calcConstAst_;
  };

  void reset() {
    stmtEnv_.sql_.clear();
    stmtEnv_.msgBuf_.fill(0);

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

  void setParseContext(const string& sql, SParseContext* pCxt) {
    stmtEnv_.sql_ = sql;
    transform(stmtEnv_.sql_.begin(), stmtEnv_.sql_.end(), stmtEnv_.sql_.begin(), ::tolower);

    pCxt->acctId = atoi(caseEnv_.acctId_.c_str());
    pCxt->db = caseEnv_.db_.c_str();
    pCxt->pSql = stmtEnv_.sql_.c_str();
    pCxt->sqlLen = stmtEnv_.sql_.length();
    pCxt->pMsg = stmtEnv_.msgBuf_.data();
    pCxt->msgLen = stmtEnv_.msgBuf_.max_size();
  }

  void doParse(SParseContext* pCxt, SQuery** pQuery) {
    DO_WITH_THROW(parse, pCxt, pQuery);
    res_.parsedAst_ = toString((*pQuery)->pRoot);
  }

  void doTranslate(SParseContext* pCxt, SQuery* pQuery) {
    DO_WITH_THROW(translate, pCxt, pQuery);
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

  caseEnv caseEnv_;
  stmtEnv stmtEnv_;
  stmtRes res_;
};

ParserTestBase::ParserTestBase() : impl_(new ParserTestBaseImpl()) {}

ParserTestBase::~ParserTestBase() {}

void ParserTestBase::useDb(const std::string& acctId, const std::string& db) { impl_->useDb(acctId, db); }

void ParserTestBase::run(const std::string& sql) { return impl_->run(sql); }
