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

#include "toolsdef.h"

TEST(taosdump, toolsSys) {
  // errorPrintReqArg3
  errorPrintReqArg3((char *)"taosdump", (char *)"test parameters");
  printf("ut function errorPrintReqArg3 ....................  [Passed]\n");

  // setConsoleEcho
  setConsoleEcho(false);
  setConsoleEcho(true);
  printf("ut function setConsoleEcho .......................  [Passed]\n");
}

int main(int argc, char **argv) {
  printf("hello world taosdump unit test for C\n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
