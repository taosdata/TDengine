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

static int32_t globalVar = 0;

static void funcPtrKey(void *param) { taosMsleep(100); }

static void *funcPtr200(void *param) {
  TdThread thread = taosThreadSelf();

  TdThreadKey key = {0};
  taosThreadKeyCreate(&key, funcPtrKey);
  void *oldVal = taosThreadGetSpecific(key);
  taosThreadSetSpecific(key, oldVal);
  taosThreadKeyDelete(key);

  int32_t oldType = 0;
  taosThreadSetCancelType(-1, &oldType);
  taosThreadSetCancelType(0, &oldType);

  int32_t oldState = 0;
  taosThreadSetCancelState(-1, &oldState);
  taosThreadSetCancelState(0, &oldState);

  int32_t            policy;
  struct sched_param para;
  taosThreadGetSchedParam(thread, &policy, &para);
  taosThreadGetSchedParam(thread, NULL, &para);
  taosThreadGetSchedParam(thread, &policy, NULL);
  // taosThreadSetSchedParam(NULL, 0, &para);
  taosThreadSetSchedParam(thread, 0, &para);
  taosMsleep(200);

  return NULL;
}

static void *funcPtr501(void *param) {
  taosMsleep(500);
  TdThread thread = taosThreadSelf();
  return NULL;
}

static void *funcPtr502(void *param) {
  taosMsleep(500);
  TdThread thread = taosThreadSelf();
  return NULL;
}

static void *funcPtr503(void *param) {
  taosMsleep(500);
  TdThread thread = taosThreadSelf();
  return NULL;
}

static void *funcPtr504(void *param) {
  taosMsleep(500);
  TdThread thread = taosThreadSelf();
  return NULL;
}

static void *funcPtrExit1(void *param) {
  taosThreadExit(NULL);
  return NULL;
}

static void *funcPtrExit2(void *param) {
  taosThreadExit(&globalVar);
  return NULL;
}

TEST(osThreadTests, thread) {
  TdThread tid1 = {0};
  TdThread tid2 = {0};
  int32_t  reti = 0;

  reti = taosThreadCreate(NULL, NULL, funcPtr200, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCreate(&tid1, NULL, NULL, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCreate(&tid1, NULL, funcPtr200, NULL);
  EXPECT_EQ(reti, 0);
  taosMsleep(300);

  (void)taosThreadCancel(tid1);

  reti = taosThreadCreate(&tid2, NULL, funcPtr501, NULL);
  EXPECT_EQ(reti, 0);
  taosMsleep(1000);
  (void)taosThreadCancel(tid2);

  taosThreadDetach(tid1);
  reti = taosThreadCreate(&tid2, NULL, funcPtr502, NULL);
  EXPECT_EQ(reti, 0);
  reti = taosThreadDetach(tid2);

  reti = taosThreadEqual(tid1, tid2);
  EXPECT_NE(reti, 0);

  reti = taosThreadCreate(&tid2, NULL, funcPtrExit1, NULL);
  EXPECT_EQ(reti, 0);
  reti = taosThreadCreate(&tid2, NULL, funcPtrExit2, NULL);
  EXPECT_EQ(reti, 0);

  taosMsleep(1000);

  // reti = taosThreadCreate(&tid2, NULL, funcPtr503, NULL);
  // EXPECT_EQ(reti, 0);
  // taosThreadKill(tid2, SIGINT);

  int32_t            policy;
  struct sched_param para;
  taosThreadGetSchedParam(tid2, &policy, &para);
  taosThreadGetSchedParam(tid2, NULL, &para);
  taosThreadGetSchedParam(tid2, &policy, NULL);
  // taosThreadSetSchedParam(NULL, 0, &para);
  taosThreadSetSchedParam(tid2, 0, &para);

  TdThreadKey key = {0};
  taosThreadKeyCreate(&key, funcPtrKey);
  void *oldVal = taosThreadGetSpecific(key);
  taosThreadSetSpecific(key, oldVal);
  taosThreadKeyDelete(key);
}

TEST(osThreadTests, attr) {
  int32_t      reti = 0;
  TdThreadAttr attr = {0};
  int32_t      param = 0;

  reti = taosThreadAttrInit(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadAttrDestroy(NULL);
  EXPECT_NE(reti, 0);

  (void)taosThreadAttrInit(&attr);

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
}

TEST(osThreadTests, cond) {
  int32_t reti = 0;

  reti = taosThreadCondInit(NULL, NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondSignal(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondBroadcast(NULL);
  EXPECT_NE(reti, 0);

  TdThreadCond  cond{0};
  TdThreadMutex mutex = {0};
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
  EXPECT_EQ(reti, 0);

  TdThreadCondAttr condattr = {0};
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

  reti = taosThreadCondAttrSetclock(NULL, -1);
  EXPECT_NE(reti, 0);

  reti = taosThreadCondAttrDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadCondAttrDestroy(&condattr);
  EXPECT_EQ(reti, 0);
}

TEST(osThreadTests, mutex) {
  int32_t       reti = 0;
  TdThreadMutex mutex;
  reti = taosThreadMutexInit(NULL, 0);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexInit(&mutex, 0);
  EXPECT_EQ(reti, 0);

  reti = taosThreadMutexTryLock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexTryLock(&mutex);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexTryLock(&mutex);
  EXPECT_NE(reti, 0);

  reti = taosThreadMutexUnlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexUnlock(&mutex);
  EXPECT_EQ(reti, 0);

  reti = taosThreadMutexLock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexLock(&mutex);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexUnlock(&mutex);
  EXPECT_EQ(reti, 0);

  reti = taosThreadMutexDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexDestroy(&mutex);
  EXPECT_EQ(reti, 0);
}

TEST(osThreadTests, mutexAttr) {
  int32_t           reti = 0;
  TdThreadMutexAttr mutexAttr;
  reti = taosThreadMutexAttrInit(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrInit(&mutexAttr);
  EXPECT_EQ(reti, 0);

  int32_t pshared;
  reti = taosThreadMutexAttrGetPshared(&mutexAttr, &pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexAttrSetPshared(&mutexAttr, pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexAttrSetPshared(&mutexAttr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrSetPshared(NULL, pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrGetPshared(NULL, &pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrGetPshared(&mutexAttr, NULL);
  EXPECT_NE(reti, 0);

  int32_t kind;
  reti = taosThreadMutexAttrGetType(&mutexAttr, &kind);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexAttrSetType(&mutexAttr, kind);
  EXPECT_EQ(reti, 0);
  reti = taosThreadMutexAttrSetType(&mutexAttr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrSetType(NULL, kind);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrGetType(NULL, &kind);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrGetType(&mutexAttr, NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadMutexAttrDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadMutexAttrDestroy(&mutexAttr);
  EXPECT_EQ(reti, 0);
}

TEST(osThreadTests, rwlock) {
  int32_t        reti = 0;
  TdThreadRwlock rwlock;
  reti = taosThreadRwlockInit(NULL, 0);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockInit(&rwlock, 0);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockTryRdlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockTryRdlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockUnlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockUnlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockRdlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockRdlock(&rwlock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadRwlockUnlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockDestroy(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockInit(NULL, 0);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockInit(&rwlock, 0);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockTryWrlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockTryWrlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockUnlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockUnlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockWrlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockWrlock(&rwlock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadRwlockUnlock(&rwlock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadRwlockDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockDestroy(&rwlock);
  EXPECT_EQ(reti, 0);
}

TEST(osThreadTests, rdlockAttr) {
  int32_t            reti = 0;
  TdThreadRwlockAttr rdlockAttr;
  reti = taosThreadRwlockAttrInit(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockAttrInit(&rdlockAttr);
  EXPECT_EQ(reti, 0);

  int32_t pshared;
  reti = taosThreadRwlockAttrGetPshared(&rdlockAttr, &pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadRwlockAttrSetPshared(&rdlockAttr, pshared);
  EXPECT_EQ(reti, 0);
  reti = taosThreadRwlockAttrSetPshared(&rdlockAttr, -1);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockAttrSetPshared(NULL, pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockAttrGetPshared(NULL, &pshared);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockAttrGetPshared(&rdlockAttr, NULL);
  EXPECT_NE(reti, 0);

  reti = taosThreadRwlockAttrDestroy(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadRwlockAttrDestroy(&rdlockAttr);
  EXPECT_EQ(reti, 0);
}

TEST(osThreadTests, spinlock) {
  int32_t reti = 0;

  TdThreadSpinlock lock = {0};
  reti = taosThreadSpinInit(&lock, -1);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinLock(&lock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinTrylock(&lock);
  EXPECT_NE(reti, 0);
  reti = taosThreadSpinUnlock(&lock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinDestroy(&lock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadSpinInit(&lock, -1);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinTrylock(&lock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinUnlock(&lock);
  EXPECT_EQ(reti, 0);
  reti = taosThreadSpinDestroy(&lock);
  EXPECT_EQ(reti, 0);

  reti = taosThreadSpinInit(NULL, 0);
  EXPECT_NE(reti, 0);
  reti = taosThreadSpinLock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadSpinTrylock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadSpinUnlock(NULL);
  EXPECT_NE(reti, 0);
  reti = taosThreadSpinDestroy(NULL);
  EXPECT_NE(reti, 0);
}

TEST(osThreadTests, others) {
  taosThreadTestCancel();
  taosThreadClear(NULL);

  TdThread  tid1 = {0};
  TdThread  tid2 = {0};
  TdThread *thread = &tid1;
  taosResetPthread(NULL);
  taosResetPthread(thread);

  bool retb = taosComparePthread(tid1, tid2);
  EXPECT_TRUE(retb);

  int32_t ret32 = taosGetAppName(NULL, NULL);
  EXPECT_NE(ret32, 0);

  char    name[32] = {0};
  int32_t pid;
  ret32 = taosGetPIdByName(name, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetPIdByName(NULL, &pid);
  EXPECT_NE(ret32, 0);

  ret32 = tsem_timewait(NULL, 124);
  EXPECT_NE(ret32, 0);
  ret32 = tsem_wait(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem2_init(NULL, 0, 0);
  EXPECT_NE(ret32, 0);
  ret32 = tsem_post(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem_destroy(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem2_post(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem2_destroy(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem2_wait(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = tsem2_timewait(NULL, 128);
  EXPECT_NE(ret32, 0);
}