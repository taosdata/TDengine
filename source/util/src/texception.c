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
#include "texception.h"
#include "tlog.h"

static threadlocal SExceptionNode* expList;

void exceptionPushNode(SExceptionNode* node) {
  node->prev = expList;
  expList = node;
}

int32_t exceptionPopNode() {
  SExceptionNode* node = expList;
  expList = node->prev;
  return node->code;
}

void exceptionThrow(int32_t code) {
  expList->code = code;
  longjmp(expList->jb, 1);
}

static void cleanupWrapper_void_ptr_ptr(SCleanupAction* ca) {
  void (*func)(void*, void*) = ca->func;
  func(ca->arg1.Ptr, ca->arg2.Ptr);
}

static void cleanupWrapper_void_ptr_bool(SCleanupAction* ca) {
  void (*func)(void*, bool) = ca->func;
  func(ca->arg1.Ptr, ca->arg2.Bool);
}

static void cleanupWrapper_void_ptr(SCleanupAction* ca) {
  void (*func)(void*) = ca->func;
  func(ca->arg1.Ptr);
}

static void cleanupWrapper_int_int(SCleanupAction* ca) {
  int32_t (*func)(int32_t) = ca->func;
  func(ca->arg1.Int);
}

static void cleanupWrapper_void(SCleanupAction* ca) {
  void (*func)() = ca->func;
  func();
}

static void cleanupWrapper_int_ptr(SCleanupAction* ca) {
  int32_t (*func)(void*) = ca->func;
  func(ca->arg1.Ptr);
}

typedef void (*wrapper)(SCleanupAction*);
static wrapper wrappers[] = {
    cleanupWrapper_void_ptr_ptr, cleanupWrapper_void_ptr_bool, cleanupWrapper_void_ptr,
    cleanupWrapper_int_int,      cleanupWrapper_void,          cleanupWrapper_int_ptr,
};

void cleanupPush_void_ptr_ptr(bool failOnly, void* func, void* arg1, void* arg2) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 0;
  ca->failOnly = failOnly;
  ca->func = func;
  ca->arg1.Ptr = arg1;
  ca->arg2.Ptr = arg2;
}

void cleanupPush_void_ptr_bool(bool failOnly, void* func, void* arg1, bool arg2) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 1;
  ca->failOnly = failOnly;
  ca->func = func;
  ca->arg1.Ptr = arg1;
  ca->arg2.Bool = arg2;
}

void cleanupPush_void_ptr(bool failOnly, void* func, void* arg) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 2;
  ca->failOnly = failOnly;
  ca->func = func;
  ca->arg1.Ptr = arg;
}

void cleanupPush_int_int(bool failOnly, void* func, int32_t arg) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 3;
  ca->failOnly = failOnly;
  ca->func = func;
  ca->arg1.Int = arg;
}

void cleanupPush_void(bool failOnly, void* func) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 4;
  ca->failOnly = failOnly;
  ca->func = func;
}

void cleanupPush_int_ptr(bool failOnly, void* func, void* arg) {
  ASSERTS(expList->numCleanupAction < expList->maxCleanupAction, "numCleanupAction over maxCleanupAction");

  SCleanupAction* ca = expList->cleanupActions + expList->numCleanupAction++;
  ca->wrapper = 5;
  ca->failOnly = failOnly;
  ca->func = func;
  ca->arg1.Ptr = arg;
}

int32_t cleanupGetActionCount() { return expList->numCleanupAction; }

static void doExecuteCleanup(SExceptionNode* node, int32_t anchor, bool failed) {
  while (node->numCleanupAction > anchor) {
    --node->numCleanupAction;
    SCleanupAction* ca = node->cleanupActions + node->numCleanupAction;
    if (failed || !(ca->failOnly)) {
      wrappers[ca->wrapper](ca);
    }
  }
}

void cleanupExecuteTo(int32_t anchor, bool failed) { doExecuteCleanup(expList, anchor, failed); }

void cleanupExecute(SExceptionNode* node, bool failed) { doExecuteCleanup(node, 0, failed); }
bool cleanupExceedLimit() { return expList->numCleanupAction >= expList->maxCleanupAction; }
