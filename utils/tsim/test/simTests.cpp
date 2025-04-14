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

#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "simInt.h"

void simHandleSignal(int32_t signo, void *sigInfo, void *context);

TEST(simTests, parameters) {
  int32_t ret = 0;
  int32_t argc = 3;
  char   *argv[4] = {0};

  argc = 3;
  argv[1] = "-f";
  argv[2] = "";
  ret = simEntry(argc, argv);
  EXPECT_EQ(ret, -1);

  argc = 4;
  argv[3] = "-v";
  ret = simEntry(argc, argv);
  EXPECT_EQ(ret, -1);

  argc = 5;
  argv[3] = "-c";
  argv[4] = "/etc/taos";
  ret = simEntry(argc, argv);
  EXPECT_EQ(ret, -1);

  argc = 4;
  argv[3] = "-h";
  ret = simEntry(argc, argv);
  EXPECT_EQ(ret, 0);

  simHandleSignal(0, NULL, NULL);
}
