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

#include <gtest/gtest.h>
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret;

  ret = syncIOStart((char*)"127.0.0.1", 7010);
  TD_ALWAYS_ASSERT(ret == 0);

  for (int i = 0; i < 3; ++i) {
    ret = syncIOPingTimerStart();
    TD_ALWAYS_ASSERT(ret == 0);
    taosMsleep(5000);

    ret = syncIOPingTimerStop();
    TD_ALWAYS_ASSERT(ret == 0);
    taosMsleep(5000);
  }

  while (1) {
    taosSsleep(1);
  }
  return 0;
}
