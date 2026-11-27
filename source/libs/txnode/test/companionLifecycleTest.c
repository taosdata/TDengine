/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define ALLOW_FORBID_FUNC
#include "uv.h"
#include "os.h"
#include "tglobal.h"
#include "txnode.h"
#include "tmqtt.h"

// Exercise the real management code and real libuv handles. Only selected API
// boundaries are wrapped, to inject failures and observe ownership/order.
static const char *testFault;
static const char *testChildPath;
static char        testDirectory[PATH_MAX];
static char        testInvalidPath[PATH_MAX];
static uv_thread_t testStarter;
static bool        testIsXnode;
static uv_sem_t    testWorkerGate;
static int         testLoopInitCalls;
static int         testLoopCloseCalls;
static int         testLoopCloseResult;
static int         testBarrierInitCalls;
static int         testBarrierDestroyCalls;
static int         testThreadCreateCalls;
static int         testThreadJoinCalls;
static int         testThreadJoinAttempts;
static int         testAsyncSendCalls;
static int32_t     testStartDuringCreateResult;

static int32_t startCompanion(bool xnode);

#define REQUIRE(condition)                                                                            \
  do {                                                                                                \
    if (!(condition)) {                                                                               \
      fprintf(stderr, "%s:%d: %s (fault=%s)\n", __FILE__, __LINE__, #condition, testFault);               \
      exit(1);                                                                                        \
    }                                                                                                 \
  } while (0)

static bool faultIs(const char *name) { return strcmp(testFault, name) == 0; }

static void joinTestPath(char *path, size_t size, const char *directory, const char *suffix) {
  size_t length = strlen(directory);
  size_t suffixLength = strlen(suffix);
  REQUIRE(length + suffixLength < size);
  memcpy(path, directory, length);
  memcpy(path + length, suffix, suffixLength + 1);
}

static int testLoopInit(uv_loop_t *loop) {
  ++testLoopInitCalls;
  if (faultIs("loop")) return UV_ENOMEM;
  return uv_loop_init(loop);
}

static void *testMemoryCalloc(size_t count, size_t size) {
  if (faultIs("td-error")) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  return taosMemoryCalloc(count, size);
}

static int testAsyncInit(uv_loop_t *loop, uv_async_t *async, uv_async_cb cb) {
  return faultIs("async") ? UV_ENOMEM : uv_async_init(loop, async, cb);
}

static int testPipeInit(uv_loop_t *loop, uv_pipe_t *pipe, int ipc) {
  return faultIs("pipe") ? UV_ENOMEM : uv_pipe_init(loop, pipe, ipc);
}

static int testLoopClose(uv_loop_t *loop) {
  ++testLoopCloseCalls;
  testLoopCloseResult = uv_loop_close(loop);
  return testLoopCloseResult;
}

static int testBarrierInit(uv_barrier_t *barrier, unsigned int count) {
  ++testBarrierInitCalls;
  return faultIs("barrier") ? UV_ENOMEM : uv_barrier_init(barrier, count);
}

static int testBarrierWait(uv_barrier_t *barrier) {
  int         result = uv_barrier_wait(barrier);
  uv_thread_t self = uv_thread_self();
  if (!uv_thread_equal(&self, &testStarter) && faultIs("join")) {
    uv_sem_wait(&testWorkerGate);
  } else if (!uv_thread_equal(&self, &testStarter) && !faultIs("none")) {
    // Force the starter to observe spawnErr before the worker can do anything
    // after the barrier.
    uv_sleep(250);
  }
  return result;
}

static void testBarrierDestroy(uv_barrier_t *barrier) {
  REQUIRE(testThreadCreateCalls == 0 || faultIs("thread") || testThreadJoinCalls == 1);
  ++testBarrierDestroyCalls;
  uv_barrier_destroy(barrier);
}

static int testThreadCreate(uv_thread_t *thread, uv_thread_cb cb, void *arg) {
  ++testThreadCreateCalls;
  // Reenter after the started flag is set, before the worker can clear a stale
  // error from the preceding attempt. No concurrent access to plain flags.
  testStartDuringCreateResult = startCompanion(testIsXnode);
  return faultIs("thread") ? UV_EAGAIN : uv_thread_create(thread, cb, arg);
}

static int testThreadJoin(uv_thread_t *thread) {
  ++testThreadJoinAttempts;
  if (faultIs("join")) return UV_EBUSY;
  int result = uv_thread_join(thread);
  REQUIRE(result == 0);
  ++testThreadJoinCalls;
  return result;
}

static int testAsyncSend(uv_async_t *async) {
  ++testAsyncSendCalls;
  return uv_async_send(async);
}

static int32_t testLocateCompanionExec(const char *name, char *path, size_t size) {
  (void)name;
  if (faultIs("missing") || faultIs("join")) return TSDB_CODE_FAILED;
  snprintf(path, size, "%s", faultIs("invalid") ? testInvalidPath : testChildPath);
  return 0;
}

#define uv_loop_init testLoopInit
#define uv_async_init testAsyncInit
#define uv_pipe_init testPipeInit
#define uv_loop_close testLoopClose
#define uv_barrier_init testBarrierInit
#define uv_barrier_wait testBarrierWait
#define uv_barrier_destroy testBarrierDestroy
#define uv_thread_create testThreadCreate
#define uv_thread_join testThreadJoin
#define uv_async_send testAsyncSend
#define taosLocateCompanionExec testLocateCompanionExec
#undef taosMemoryCalloc
#define taosMemoryCalloc testMemoryCalloc
#include "../src/txnodeMgmt.c"
#include "../../tmqtt/mgmt/src/tmqttMgmt.c"
#undef uv_loop_init
#undef uv_async_init
#undef uv_pipe_init
#undef uv_loop_close
#undef uv_barrier_init
#undef uv_barrier_wait
#undef uv_barrier_destroy
#undef uv_thread_create
#undef uv_thread_join
#undef uv_async_send
#undef taosLocateCompanionExec
#undef taosMemoryCalloc

// Keep pid files and socket removal inside the test's private directory.
void getXnodedPipeName(char *path, int32_t size) { joinTestPath(path, size, testDirectory, "/unused.sock"); }

static void resetObservations(const char *fault) {
  testFault = fault;
  testLoopInitCalls = 0;
  testLoopCloseCalls = 0;
  testLoopCloseResult = 0;
  testBarrierInitCalls = 0;
  testBarrierDestroyCalls = 0;
  testThreadCreateCalls = 0;
  testThreadJoinCalls = 0;
  testThreadJoinAttempts = 0;
  testAsyncSendCalls = 0;
  testStartDuringCreateResult = TSDB_CODE_FAILED;
}

static int32_t startCompanion(bool xnode) {
  if (!xnode) return mqttMgmtStartMqttd(1);
  SXnode node = {0};
  node.dnodeId = 1;
  node.clusterId = 1;
  return xnodeMgmtStartXnoded(&node);
}

static void stopCompanion(bool xnode) {
  if (xnode) {
    xnodeMgmtStopXnoded();
  } else {
    mqttMgmtStopMqttd();
  }
}

static void checkStopped(bool xnode) {
  REQUIRE(xnode ? !xnodedGlobal.isStarted : !mqttdGlobal.startCalled);
  REQUIRE(xnode ? !xnodedGlobal.needCleanUp : !mqttdGlobal.needCleanUp);
}

static void checkFailedStart(bool xnode, const char *fault) {
  resetObservations(fault);
  int32_t expected = xnode ? TSDB_CODE_XNODE_UV_EXEC_FAILURE : TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  if (faultIs("td-error")) expected = TSDB_CODE_OUT_OF_MEMORY;
  REQUIRE(startCompanion(xnode) == expected);
  checkStopped(xnode);
  REQUIRE(testAsyncSendCalls == 0);
  REQUIRE(testLoopCloseResult == 0);
  REQUIRE(testBarrierInitCalls == 1);
  REQUIRE(testBarrierDestroyCalls == (faultIs("barrier") ? 0 : 1));
  if (!faultIs("barrier")) REQUIRE(testStartDuringCreateResult == 0);
  if (faultIs("barrier") || faultIs("thread")) {
    REQUIRE(testLoopInitCalls == 0);
    REQUIRE(testThreadJoinCalls == 0);
  } else {
    REQUIRE(testLoopInitCalls == 1);
    REQUIRE(testThreadJoinCalls == 1);
  }
  bool loopCreated = !faultIs("barrier") && !faultIs("thread") && !faultIs("loop");
  REQUIRE(testLoopCloseCalls == (loopCreated ? 1 : 0));
  stopCompanion(xnode);
  REQUIRE(testAsyncSendCalls == 0);
}

static void checkSuccessfulStart(bool xnode) {
  resetObservations("none");
  REQUIRE(startCompanion(xnode) == 0);
  REQUIRE(xnode ? xnodedGlobal.isStarted : mqttdGlobal.startCalled);
  REQUIRE(xnode ? xnodedGlobal.needCleanUp : mqttdGlobal.needCleanUp);
  REQUIRE(startCompanion(xnode) == 0);
  REQUIRE(testThreadCreateCalls == 1);
  REQUIRE(testStartDuringCreateResult == 0);
  stopCompanion(xnode);
  checkStopped(xnode);
  REQUIRE(testThreadJoinCalls == 1);
  REQUIRE(testBarrierDestroyCalls == 1);
  REQUIRE(testLoopCloseCalls == 1);
  REQUIRE(testLoopCloseResult == 0);
  REQUIRE(testAsyncSendCalls == 1);
  stopCompanion(xnode);
  REQUIRE(testThreadJoinCalls == 1);
}

static void checkFailedJoin(bool xnode) {
  resetObservations("join");
  REQUIRE(uv_sem_init(&testWorkerGate, 0) == 0);
  int32_t expected = xnode ? TSDB_CODE_XNODE_UV_EXEC_FAILURE : TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  REQUIRE(startCompanion(xnode) == expected);
  REQUIRE(testThreadJoinAttempts == 1);
  REQUIRE(testThreadJoinCalls == 0);
  REQUIRE(testBarrierDestroyCalls == 0);
  REQUIRE(testLoopCloseCalls == 0);
  REQUIRE(testAsyncSendCalls == 0);
  REQUIRE(xnode ? xnodedGlobal.isStarted : mqttdGlobal.startCalled);
  REQUIRE(xnode ? !xnodedGlobal.needCleanUp : !mqttdGlobal.needCleanUp);
  REQUIRE(startCompanion(xnode) == expected);
  REQUIRE(testThreadCreateCalls == 1);
  stopCompanion(xnode);
  REQUIRE(testAsyncSendCalls == 0);

  // Recover only in the harness: the injected join did not wait for the worker.
  // Release it and really join before destroying or resetting shared resources.
  uv_sem_post(&testWorkerGate);
  REQUIRE(uv_thread_join(xnode ? &xnodedGlobal.thread : &mqttdGlobal.thread) == 0);
  REQUIRE(testLoopCloseCalls == 1);
  REQUIRE(testLoopCloseResult == 0);
  REQUIRE(testBarrierDestroyCalls == 0);
  uv_barrier_destroy(xnode ? &xnodedGlobal.barrier : &mqttdGlobal.barrier);
  uv_sem_destroy(&testWorkerGate);
  if (xnode) {
    xnodedGlobal.isStarted = false;
  } else {
    mqttdGlobal.startCalled = false;
  }
}

static void checkErrorMapping(void) {
  int32_t modules[] = {TSDB_CODE_XNODE_UV_EXEC_FAILURE, TSDB_CODE_BNODE_UV_EXEC_FAILURE, TSDB_CODE_UDF_UV_EXEC_FAILURE};
  for (size_t index = 0; index < sizeof(modules) / sizeof(modules[0]); ++index) {
    REQUIRE(tsdbMapUvErr(0, modules[index]) == modules[index]);
    REQUIRE(tsdbMapUvErr(UV_ENOENT, modules[index]) == modules[index]);
    REQUIRE(tsdbMapUvErr(UV_UNKNOWN, modules[index]) == modules[index]);
    REQUIRE(tsdbMapUvErr(TSDB_CODE_OUT_OF_MEMORY, modules[index]) == TSDB_CODE_OUT_OF_MEMORY);
    REQUIRE(tsdbMapUvErr(TSDB_UV_ERRNO_MIN, modules[index]) == TSDB_UV_ERRNO_MIN);
  }
}

int main(int argc, char **argv) {
  testFault = "setup";
  REQUIRE(argc == 4);
  bool xnode = strcmp(argv[1], "xnode") == 0;
  testIsXnode = xnode;
  checkErrorMapping();
  testChildPath = argv[3];
  testStarter = uv_thread_self();
  char   temp[PATH_MAX];
  size_t tempSize = sizeof(temp);
  REQUIRE(uv_os_tmpdir(temp, &tempSize) == 0);
  char templatePath[PATH_MAX];
  joinTestPath(templatePath, sizeof(templatePath), temp, "/companion-lifecycle-XXXXXX");
  uv_fs_t req = {0};
  REQUIRE(uv_fs_mkdtemp(NULL, &req, templatePath, NULL) == 0);
  snprintf(testDirectory, sizeof(testDirectory), "%s", req.path);
  uv_fs_req_cleanup(&req);
  snprintf(tsDataDir, PATH_MAX, "%s", testDirectory);
  joinTestPath(testInvalidPath, sizeof(testInvalidPath), testDirectory, "/invalid.exe");
  FILE *invalid = fopen(testInvalidPath, "wb");
  REQUIRE(invalid != NULL);
  REQUIRE(fputs("not an executable\n", invalid) >= 0);
  REQUIRE(fclose(invalid) == 0);
#ifdef WINDOWS
  REQUIRE(uv_fs_chmod(NULL, &req, testInvalidPath, 0700, NULL) == 0);
#else
  // execvp may run an invalid executable as a shell script. Deny execution on
  // Unix so uv_spawn itself reliably fails, before any child exit callback.
  REQUIRE(uv_fs_chmod(NULL, &req, testInvalidPath, 0600, NULL) == 0);
#endif
  uv_fs_req_cleanup(&req);

  // Reuse the same global state without resetting it between starts.
  for (int attempt = 0; attempt < 3; ++attempt) {
    if (strcmp(argv[2], "join") == 0) {
      checkFailedJoin(xnode);
    } else {
      checkFailedStart(xnode, argv[2]);
    }
  }
  checkSuccessfulStart(xnode);
  checkSuccessfulStart(xnode);

  REQUIRE(remove(testInvalidPath) == 0);
  REQUIRE(uv_fs_rmdir(NULL, &req, testDirectory, NULL) == 0);
  uv_fs_req_cleanup(&req);
  printf("PASS %s %s: repeated failure, recovery, stop and restart\n", argv[1], argv[2]);
  return 0;
}
