/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "syncTest.h"
// #include <gtest/gtest.h>

/*
typedef enum {
  DEBUG_FATAL = 1,
  DEBUG_ERROR = 1,
  DEBUG_WARN = 2,
  DEBUG_INFO = 2,
  DEBUG_DEBUG = 4,
  DEBUG_TRACE = 8,
  DEBUG_DUMP = 16,
  DEBUG_SCREEN = 64,
  DEBUG_FILE = 128
} ELogLevel;
*/

void logTest(char* s) {
  sFatal("==%s== sync log test: fatal", s);
  sError("==%s== sync log test: error", s);

  sWarn("==%s== sync log test: warn", s);
  sInfo("==%s== sync log test: info", s);

  sDebug("==%s== sync log test: debug", s);

  sTrace("==%s== sync log test: trace", s);
}

void test1() {
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test2() {
  sDebugFlag = DEBUG_DEBUG + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test3() {
  sDebugFlag = DEBUG_INFO + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test4() {
  sDebugFlag = DEBUG_ERROR + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

int main(int argc, char** argv) {
  taosInitLog("/tmp/syncTest.log", 100, false);
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_SCREEN + DEBUG_FILE + DEBUG_TRACE + DEBUG_INFO + DEBUG_ERROR;

  test1();
  test2();
  test3();
  test4();

  /*
  if (argc == 2) {
    bool bTaosDirExist = taosDirExist(argv[1]);
    printf("%s bTaosDirExist:%d \n", argv[1], bTaosDirExist);

    bool bTaosCheckExistFile = taosCheckExistFile(argv[1]);
    printf("%s bTaosCheckExistFile:%d \n", argv[1], bTaosCheckExistFile);
  }
  */

  taosCloseLog();
  return 0;
}
