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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tlog.h"
#include "taos.h"

static TdThread  threadId;
static TdThread  stopFlag = false;

static void *threadFunc(void *arg) {
  static int32_t count = 0;
  while (!stopFlag) {
    uInfo("moduleTest2 thread is running, count:%d", count);
    count++;
    taosMsleep(1000);
  }

  uInfo("moduleTest2 thread is stopped");
  return NULL;
}

int32_t taosModuleStart() {
  uInfo("moduleTest2 start func is called");

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&threadId, &thattr, threadFunc, NULL) != 0) {
    uError("failed to run thread for moduleTest2");
  } else {
    uInfo("moduleTest2 thread create successfully");
  }

  return 0;
}

void taosModuleStop() {
  uInfo("moduleTest2 stop func is called");

  if (!stopFlag) {
    stopFlag = true;
    taosThreadJoin(threadId, NULL);
  }
}
