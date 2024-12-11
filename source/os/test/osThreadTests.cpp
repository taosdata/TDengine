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

#include "os.h"
#include "tlog.h"

static void *funcPtr0(void *param) { return NULL; }
static void *funcPtr1(void *param) {
  taosMsleep(1000);
  return NULL;
}

TEST(osThreadTests, invalidParameter) {
  TdThread     tid1 = {0};
  TdThread     tid2 = {0};
  int32_t      reti = 0;
  TdThreadAttr attr = {0};
  TdThreadAttr attr2 = {0};
  int32_t      param = 0;
  (void)taosThreadAttrInit(&attr);

  reti = taosThreadCreate(NULL, NULL, funcPtr0, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCreate(&tid1, NULL, NULL, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCreate(&tid1, NULL, funcPtr1, NULL);
  EXPECT_EQ(reti, 0);
  reti = taosThreadCancel(tid1);
  EXPECT_EQ(reti, 0);
  reti = taosThreadCreate(&tid2, NULL, funcPtr0, NULL);
  EXPECT_EQ(reti, 0);
  taosMsleep(1000);
  reti = taosThreadCancel(tid2);
  EXPECT_EQ(reti, 0);

  reti = taosThreadAttrDestroy(NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_JOINABLE);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetDetachState(&attr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetDetachState(NULL, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetDetachState(NULL, &param);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetDetachState(&attr, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetDetachState(&attr, &param);
  EXPECT_EQ(reti, 0);

  reti = taosThreadAttrSetInheritSched(&attr, PTHREAD_INHERIT_SCHED);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetInheritSched(&attr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetInheritSched(NULL, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetInheritSched(NULL, &param);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetInheritSched(&attr, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetInheritSched(&attr, &param);
  EXPECT_EQ(reti, 0);

  struct sched_param schedparam = {0};
  reti = taosThreadAttrGetSchedParam(&attr, &schedparam);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetSchedParam(&attr, &schedparam);
  EXPECT_EQ(reti, 0);
  schedparam.sched_priority = -1;
  reti = taosThreadAttrSetSchedParam(&attr, &schedparam);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetSchedParam(NULL, &schedparam);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetSchedParam(NULL, &schedparam);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetSchedParam(&attr, NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadAttrSetSchedPolicy(&attr, SCHED_FIFO);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetSchedPolicy(&attr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetSchedPolicy(NULL, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetSchedPolicy(NULL, &param);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetSchedPolicy(&attr, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetSchedPolicy(&attr, &param);
  EXPECT_EQ(reti, 0);

  reti = taosThreadAttrSetScope(&attr, PTHREAD_SCOPE_SYSTEM);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetScope(&attr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetScope(NULL, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetScope(NULL, &param);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetScope(&attr, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetScope(&attr, &param);
  EXPECT_EQ(reti, 0);

  size_t stacksize;
  reti = taosThreadAttrGetStackSize(&attr, &stacksize);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetStackSize(&attr, stacksize);
  EXPECT_EQ(reti, 0);
  reti = taosThreadAttrSetStackSize(&attr, 2048);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrSetStackSize(NULL, stacksize);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetStackSize(NULL, &stacksize);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrGetStackSize(&attr, NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadAttrInit(NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadCondDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondInit(NULL, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondSignal(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondBroadcast(NULL);
  EXPECT_NE(reti, 0);

  TdThreadCond  cond;
  TdThreadMutex mutex;
  reti = taosThreadCondWait(&cond, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondWait(NULL, &mutex);
  EXPECT_NE(reti, 0);

  struct timespec abstime = {0};
  reti = taosThreadCondTimedWait(&cond, NULL, &abstime);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondTimedWait(NULL, &mutex, &abstime);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondTimedWait(&cond, &mutex, NULL);
  EXPECT_NE(reti, 0);

  TdThreadCondAttr condattr;
  (void)taosThreadCondAttrInit(&condattr);
  reti = taosThreadCondAttrInit(NULL);
  EXPECT_NE(reti, 0);
  int32_t pshared;
  reti = taosThreadCondAttrGetPshared(&condattr, &pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadCondAttrSetPshared(&condattr, pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadCondAttrSetPshared(&condattr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondAttrSetPshared(NULL, pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondAttrGetPshared(NULL, &pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondAttrGetPshared(&condattr, NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadCondAttrDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondAttrDestroy(&condattr);
  EXPECT_EQ(reti, 0);
}
