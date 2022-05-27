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

#ifndef PARSER_TEST_UTIL_H
#define PARSER_TEST_UTIL_H

#include <gtest/gtest.h>

#define ALLOW_FORBID_FUNC

#include "querynodes.h"
#include "taoserror.h"

namespace ParserTest {

class ParserTestBaseImpl;

enum ParserStage { PARSER_STAGE_PARSE, PARSER_STAGE_TRANSLATE, PARSER_STAGE_CALC_CONST, PARSER_STAGE_ALL };

class ParserTestBase : public testing::Test {
 public:
  ParserTestBase();
  virtual ~ParserTestBase();

  void useDb(const std::string& acctId, const std::string& db);
  void run(const std::string& sql, int32_t expect = TSDB_CODE_SUCCESS, ParserStage checkStage = PARSER_STAGE_ALL);
  void runAsync(const std::string& sql, int32_t expect = TSDB_CODE_SUCCESS, ParserStage checkStage = PARSER_STAGE_ALL);

  virtual void checkDdl(const SQuery* pQuery, ParserStage stage);

 private:
  std::unique_ptr<ParserTestBaseImpl> impl_;
};

class ParserDdlTest : public ParserTestBase {
 public:
  void setCheckDdlFunc(const std::function<void(const SQuery*, ParserStage)>& func) { checkDdl_ = func; }

  virtual void checkDdl(const SQuery* pQuery, ParserStage stage) {
    ASSERT_NE(pQuery, nullptr);
    ASSERT_EQ(pQuery->haveResultSet, false);
    ASSERT_NE(pQuery->pRoot, nullptr);
    ASSERT_EQ(pQuery->numOfResCols, 0);
    ASSERT_EQ(pQuery->pResSchema, nullptr);
    ASSERT_EQ(pQuery->precision, 0);
    if (nullptr != checkDdl_) {
      checkDdl_(pQuery, stage);
    }
  }

 private:
  std::function<void(const SQuery*, ParserStage)> checkDdl_;
};

extern bool g_dump;
extern bool g_testAsyncApis;

}  // namespace ParserTest

#endif  // PARSER_TEST_UTIL_H
