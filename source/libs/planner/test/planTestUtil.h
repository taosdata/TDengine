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

#ifndef PLAN_TEST_UTIL_H
#define PLAN_TEST_UTIL_H

#include <gtest/gtest.h>

#define ALLOW_FORBID_FUNC

#include "planInt.h"

class PlannerTestBaseImpl;
struct TAOS_MULTI_BIND;

class PlannerTestBase : public testing::Test {
 public:
  PlannerTestBase();
  virtual ~PlannerTestBase();

  void useDb(const std::string& user, const std::string& db);
  void run(const std::string& sql);
  // stmt mode APIs
  void prepare(const std::string& sql);
  void bindParams(TAOS_MULTI_BIND* pParams, int32_t colIdx);
  void exec();

 private:
  std::unique_ptr<PlannerTestBaseImpl> impl_;
};

extern void    setDumpModule(const char* pArg);
extern void    setSkipSqlNum(const char* pArg);
extern void    setLimitSqlNum(const char* pArg);
extern void    setLogLevel(const char* pArg);
extern void    setQueryPolicy(const char* pArg);
extern void    setUseNodeAllocator(const char* pArg);
extern int32_t getLogLevel();

#endif  // PLAN_TEST_UTIL_H
