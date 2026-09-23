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

// clang-format off
#include "uv.h"
#include "os.h"
#include "tarray.h"
#include "tglobal.h"
#include "txnode.h"
#include "txnodeInt.h"
#include "osString.h"

// clang-format on

#ifdef WINDOWS
#define XNODED_DEFAULT_PATH_1    "C:\\TDengine"
#define XNODED_DEFAULT_PATH_2    "C:\\TDengine"
#define XNODED_DEFAULT_EXEC_NAME "xnoded"
#define XNODED_DEFAULT_EXEC      "\\xnoded.exe"
#else
#define XNODED_DEFAULT_PATH_1    "/usr/bin"
#define XNODED_DEFAULT_PATH_2    "/usr/local/taos/bin"
#define XNODED_DEFAULT_EXEC_NAME "xnoded"
#define XNODED_DEFAULT_EXEC      "/xnoded"
#endif

#define XNODED_XNODED_PID_NAME ".xnoded.pid"

typedef struct {
  bool         isStarted;
  bool         needCleanUp;
  uv_loop_t    loop;
  uv_thread_t  thread;
  uv_barrier_t barrier;
  uv_process_t process;
  int32_t      spawnErr;
  uv_pipe_t    ctrlPipe;
  uv_async_t   stopAsync;
  int32_t      isStopped;
  int32_t      dnodeId;
  int64_t      clusterId;
  char         userPass[XNODE_USER_PASS_LEN];
  char         token[TSDB_TOKEN_LEN + 1];
  SEp          leaderEp;
} SXnodedData;

SXnodedData xnodedGlobal = {0};

static int32_t xnodeMgmtSpawnXnoded(SXnodedData *pData);

bool txnodeShouldRestartXnoded(int64_t exitStatus, int32_t termSignal, bool isStopped) {
  if (isStopped || (exitStatus == XNODED_PRIVATE_KEY_CONFIG_EXIT_CODE && termSignal == 0)) {
    return false;
  }
  return !(exitStatus == 0 && (termSignal == 0 || termSignal == SIGINT || termSignal == SIGTERM));
}

static bool txnodeEnvironmentNameEquals(const char *left, size_t left_len, const char *right, size_t right_len) {
  if (left_len != right_len) {
    return false;
  }
#ifdef WINDOWS
  return strncasecmp(left, right, left_len) == 0;
#else
  return strncmp(left, right, left_len) == 0;
#endif
}

static bool txnodeEnvironmentOverridesName(const char *name, size_t name_len, const char *const *overrides,
                                           int32_t override_count) {
  for (int32_t index = 0; index < override_count; ++index) {
    const char *override = overrides[index];
    if (override == NULL || override[0] == '\0') {
      continue;
    }
    const char *separator = strchr(override, '=');
    if (separator == NULL) {
      continue;
    }
    size_t override_name_len = (size_t)(separator - override);
    if (txnodeEnvironmentNameEquals(name, name_len, override, override_name_len)) {
      return true;
    }
  }
  return false;
}

void txnodeFreeEnvironment(char **environment) {
  if (environment == NULL) {
    return;
  }
  for (int32_t index = 0; environment[index] != NULL; ++index) {
    taosMemoryFree(environment[index]);
  }
  taosMemoryFree(environment);
}

int32_t txnodeBuildXnodedEnvironment(const char *const *inherited, int32_t inherited_count,
                                     const char *const *overrides, int32_t override_count, char ***environment) {
  if (inherited_count < 0 || override_count < 0 || overrides == NULL || environment == NULL ||
      (inherited_count > 0 && inherited == NULL) || *environment != NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint64_t environment_slots = (uint64_t)inherited_count + (uint64_t)override_count + 1;
  if (environment_slots > SIZE_MAX / sizeof(char *)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  char **result = taosMemoryCalloc((size_t)environment_slots, sizeof(char *));
  if (result == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t output_index = 0;
  for (int32_t index = 0; index < inherited_count; ++index) {
    const char *entry = inherited[index];
    if (entry == NULL || entry[0] == '\0') {
      continue;
    }
    const char *separator = strchr(entry, '=');
    if (separator == NULL) {
      continue;
    }
    size_t name_len = (size_t)(separator - entry);
    if (txnodeEnvironmentOverridesName(entry, name_len, overrides, override_count)) {
      continue;
    }
    size_t length = strlen(entry) + 1;
    result[output_index] = taosMemoryCalloc(length, 1);
    if (result[output_index] == NULL) {
      txnodeFreeEnvironment(result);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tstrncpy(result[output_index], entry, length);
    ++output_index;
  }

  for (int32_t index = 0; index < override_count; ++index) {
    if (overrides[index] == NULL || overrides[index][0] == '\0') {
      continue;
    }
    size_t length = strlen(overrides[index]) + 1;
    result[output_index] = taosMemoryCalloc(length, 1);
    if (result[output_index] == NULL) {
      txnodeFreeEnvironment(result);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tstrncpy(result[output_index], overrides[index], length);
    ++output_index;
  }

  result[output_index] = NULL;
  *environment = result;
  return TSDB_CODE_SUCCESS;
}

static int32_t txnodeInheritProcessEnvironment(char ***environment, int32_t *environment_count) {
  *environment = NULL;
  *environment_count = 0;

  uv_env_item_t *items = NULL;
  int            item_count = 0;
  int            uv_error = uv_os_environ(&items, &item_count);
  if (uv_error != 0) {
    if (items != NULL) {
      uv_os_free_environ(items, item_count);
    }
    xndError("failed to read process environment for xnoded: %s", uv_strerror(uv_error));
    return TSDB_CODE_XNODE_UV_EXEC_FAILURE;
  }

  char **result = taosMemoryCalloc((size_t)item_count + 1, sizeof(char *));
  if (result == NULL) {
    uv_os_free_environ(items, item_count);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t output_index = 0;
  for (int32_t index = 0; index < item_count; ++index) {
    const char *name = items[index].name;
    const char *value = items[index].value;
    if (name == NULL || value == NULL) {
      continue;
    }
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);
    if (name_len > SIZE_MAX - value_len - 2) {
      txnodeFreeEnvironment(result);
      uv_os_free_environ(items, item_count);
      return TSDB_CODE_OUT_OF_RANGE;
    }
    size_t length = name_len + value_len + 2;
    result[output_index] = taosMemoryCalloc(length, 1);
    if (result[output_index] == NULL) {
      txnodeFreeEnvironment(result);
      uv_os_free_environ(items, item_count);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    (void)snprintf(result[output_index], length, "%s=%s", name, value);
    ++output_index;
  }

  uv_os_free_environ(items, item_count);
  *environment = result;
  *environment_count = output_index;
  return TSDB_CODE_SUCCESS;
}

static void getXnodedPidPath(char *pipeName, int32_t size) {
  int32_t len = strlen(tsDataDir);

  if (len > 0 && (tsDataDir[len - 1] != '/' && tsDataDir[len - 1] != '\\')) {
  #ifdef _WIN32
    snprintf(pipeName, size, "%s\\%s", tsDataDir, XNODED_XNODED_PID_NAME);
  #else
    snprintf(pipeName, size, "%s/%s", tsDataDir, XNODED_XNODED_PID_NAME);
  #endif
  } else {
    snprintf(pipeName, size, "%s%s", tsDataDir, XNODED_XNODED_PID_NAME);
  }
  xndDebug("xnode get xnoded pid path:%s", pipeName);
}

static void    xnodeMgmtXnodedExit(uv_process_t *process, int64_t exitStatus, int32_t termSignal) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(process);
  SXnodedData *pData = process->data;
  xndInfo("process xnoded exit with status %" PRId64 ", signal %d, isStopped:%d", exitStatus, termSignal,
          pData->isStopped);
  if (pData == NULL) {
    xndError("xnoded process data is NULL");
    return;
  }
  if (!txnodeShouldRestartXnoded(exitStatus, termSignal, atomic_load_32(&pData->isStopped))) {
    if (uv_async_send(&pData->stopAsync) != 0) {
      xndError("stop xnoded: failed to send stop async");
    }
    char xnodedPipeSocket[PATH_MAX] = {0};
    getXnodedPipeName(xnodedPipeSocket, PATH_MAX);
    if (0 != unlink(xnodedPipeSocket)) {
      xndWarn("txnode failed to unlink, socket: %s, err:%s", xnodedPipeSocket, terrstr());
    }

    char *pidPath = xnodedPipeSocket;
    memset(pidPath, 0, PATH_MAX);
    getXnodedPidPath(pidPath, PATH_MAX);
    (void)taosRemoveFile(pidPath);
    if (exitStatus == XNODED_PRIVATE_KEY_CONFIG_EXIT_CODE && termSignal == 0) {
      xndError("xnoded exited due to invalid private key configuration; restart disabled");
    } else {
      xndInfo("xnoded process exit success");
    }
  } else {
    xndInfo("xnoded process restart, exit status %" PRId64 ", signal %d", exitStatus, termSignal);
    int32_t code = xnodeMgmtSpawnXnoded(pData);
    if (code != 0) {
      xndError("xnoded process restart failed with code:%d", code);
    }
    uv_sleep(500);
  }
}
void killPreXnoded() {
  char buf[PATH_MAX] = {0};
  getXnodedPidPath(buf, sizeof(buf));

  TdFilePtr pFile = NULL;
  pFile = taosOpenFile(buf, TD_FILE_READ);
  if (pFile == NULL) {
    xndWarn("xnode failed to open xnoded pid file:%s, file may not exist", buf);
    return;
  }
  int64_t readSize = taosReadFile(pFile, buf, sizeof(buf));
  if (readSize <= 0) {
    if (readSize < 0) {
      xndError("xnode failed to read len from file:%p since %s", pFile, terrstr());
    }
    (void)taosCloseFile(&pFile);
    return;
  }
  (void)taosCloseFile(&pFile);

  int32_t pid = taosStr2Int32(buf, NULL, 10);
  int result = uv_kill((uv_pid_t)pid, SIGTERM);
  if (result != 0) {
    if (result != UV_ESRCH) {
      xndError("xnode failed to kill process %d: %s", pid, uv_strerror(result));
    }
    return;
  }
}

void saveXnodedPid(int32_t pid) {
  char buf[PATH_MAX] = {0};
  getXnodedPidPath(buf, sizeof(buf));
  TdFilePtr testFilePtr = taosCreateFile(buf, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC);
  snprintf(buf, PATH_MAX, "%d", pid);
  (void)taosWriteFile(testFilePtr, buf, strlen(buf));
  (void)taosCloseFile(&testFilePtr);
}

static void locateXnodedExecFile(char *path) {
#ifdef WINDOWS
  if (taosAppPath(path, PATH_MAX) != 0) {
    path[0] = '\0';
  }
#else
  if (tsProcPath == NULL) {
    path[0] = '.';
#if defined(_TD_DARWIN_64)
    uint32_t pathSize = sizeof(path);
    _NSGetExecutablePath(path, &pathSize);
#endif
  } else {
    TAOS_STRNCPY(path, tsProcPath, PATH_MAX);
  }

  TAOS_DIRNAME(path);
#endif
  if (strlen(path) != 0) {
    TAOS_STRCAT(path, XNODED_DEFAULT_EXEC);
    if (taosCheckExistFile(path)) {
      goto _ok;
    }
    xndDebug("can't find xnoded exec file:%s", path);
    path[0] = '\0';
  }

  TAOS_STRCAT(path, XNODED_DEFAULT_PATH_1);
  TAOS_STRCAT(path, XNODED_DEFAULT_EXEC);
  if (taosCheckExistFile(path)) {
    goto _ok;
  }
  xndDebug("can't find xnoded exec file:%s", path);
  path[0] = '\0';

  TAOS_STRCAT(path, XNODED_DEFAULT_PATH_2);
  TAOS_STRCAT(path, XNODED_DEFAULT_EXEC);
  if (taosCheckExistFile(path)) {
    goto _ok;
  }
  xndDebug("can't find xnoded exec file:%s", path);
  path[0] = '\0';

  path[0] = '.';
  path[1] = '\0';
  TAOS_STRCAT(path, XNODED_DEFAULT_EXEC);
  if (taosCheckExistFile(path)) {
    goto _ok;
  }
  xndDebug("can't find xnoded exec file:%s", path);
  path[0] = '\0';

  TAOS_STRNCPY(path, XNODED_DEFAULT_EXEC_NAME, PATH_MAX);
  xndInfo("can't find xnoded exec file, use default: %s", path);
  return;

_ok:
  xndInfo("find xnoded exec file:%s", path);
  return;
}

static int32_t xnodeMgmtSpawnXnoded(SXnodedData *pData) {
  xndDebug("start to init xnoded");
  TAOS_XNODED_MGMT_CHECK_PTR_RCODE(pData);

  int32_t              err = 0;
  uv_process_options_t options = {0};

  char path[PATH_MAX] = {0};
  locateXnodedExecFile(path);

  xndInfo("xnode mgmt spawn xnoded path: %s", path);
  // char *argsXnoded[] = {path, "-c", configDir, "-d", dnodeId, NULL};
  char *argsXnoded[] = {path, NULL};
  options.args = argsXnoded;
  options.file = path;

  options.exit_cb = xnodeMgmtXnodedExit;

  killPreXnoded();

  char xnodedPipeSocket[PATH_MAX] = {0};
  getXnodedPipeName(xnodedPipeSocket, PATH_MAX);
  if (0 != unlink(xnodedPipeSocket)) {
    xndWarn("txnode failed to unlink, ignore if first time, socket: %s, detail:%s", xnodedPipeSocket, terrstr());
  }

  // This pipe only detects parent shutdown through EOF. Enabling libuv IPC
  // triggers a child-process handshake that is unsupported by xnoded on Windows.
  TAOS_UV_LIB_ERROR_RET(uv_pipe_init(&pData->loop, &pData->ctrlPipe, 0));

  uv_stdio_container_t child_stdio[3];
  child_stdio[0].flags = UV_CREATE_PIPE | UV_READABLE_PIPE;
  child_stdio[0].data.stream = (uv_stream_t *)&pData->ctrlPipe;
  child_stdio[1].flags = UV_IGNORE;
  child_stdio[2].flags = UV_INHERIT_FD;
  child_stdio[2].data.fd = 2;
  options.stdio_count = 3;
  options.stdio = child_stdio;

  options.flags = UV_PROCESS_DETACHED;

  char xnodedCfgDir[PATH_MAX + 32] = {0};
  snprintf(xnodedCfgDir, PATH_MAX + 32, "%s=%s", "XNODED_CFG_DIR", configDir);
  char xnodedLogDir[PATH_MAX + 32] = {0};
  snprintf(xnodedLogDir, PATH_MAX + 32, "%s=%s", "XNODED_LOG_DIR", tsLogDir);
  char dnodeIdEnvItem[128] = {0};
  snprintf(dnodeIdEnvItem, 128, "%s=%s:%d", "XNODED_LEADER_EP", pData->leaderEp.fqdn, pData->leaderEp.port);
  char xnodedUserPass[XNODE_USER_PASS_LEN + 32] = {0};
  if (pData->userPass[0] != '\0') {
    snprintf(xnodedUserPass, XNODE_USER_PASS_LEN + 32, "%s=%s", "XNODED_USER_PASS", pData->userPass);
  }
  char xnodedToken[TSDB_TOKEN_LEN + 32] = {0};
  if (pData->token[0] != '\0') {
    snprintf(xnodedToken, TSDB_TOKEN_LEN + 32, "%s=%s", "XNODED_TOKEN", pData->token);
  }
  char xnodeClusterId[64] = {0};
  snprintf(xnodeClusterId, 64, "%s=%" PRIu64, "XNODED_CLUSTER_ID", pData->clusterId);
  char xnodePipeSocket[PATH_MAX + 32] = {0};
  snprintf(xnodePipeSocket, PATH_MAX + 32, "%s=%s", "XNODED_LISTEN", xnodedPipeSocket);
  char xnodedDsnPrivateKeyFile[PATH_MAX + 64] = {0};
  if (tsXnodedDsnPrivateKeyFile[0] != '\0') {
    snprintf(xnodedDsnPrivateKeyFile, sizeof(xnodedDsnPrivateKeyFile), "%s=%s",
             XNODED_XNODE_DSN_PRIVATE_KEY_FILE, tsXnodedDsnPrivateKeyFile);
  }

  char xnodedLogLevel[32] = {0};
  if (xndDebugFlag & DEBUG_INFO) {
    snprintf(xnodedLogLevel, 32, "%s=%s", "XNODED_LOG_LEVEL", "info");
  }
  if (xndDebugFlag & DEBUG_DEBUG) {
    snprintf(xnodedLogLevel, 32, "%s=%s", "XNODED_LOG_LEVEL", "debug");
  }
  if (xndDebugFlag & DEBUG_TRACE) {
    snprintf(xnodedLogLevel, 32, "%s=%s", "XNODED_LOG_LEVEL", "trace");
  }

  xndDebug("txnode env: leader ep: %s, pipe socket:%s, log level:%s, cluster_id: %s", dnodeIdEnvItem, xnodePipeSocket,
           xnodedLogLevel, xnodeClusterId);

  const char *envXnoded[] = {xnodedCfgDir,           xnodedLogDir,   dnodeIdEnvItem,
                             xnodedUserPass,         xnodedToken,    xnodeClusterId,
                             xnodePipeSocket,        xnodedLogLevel, xnodedDsnPrivateKeyFile,
                             NULL};

  char **envXnodedWithPEnv = NULL;
  char **inheritedEnvironment = NULL;
  int32_t inheritedEnvironmentCount = 0;
  err = txnodeInheritProcessEnvironment(&inheritedEnvironment, &inheritedEnvironmentCount);
  if (err != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }
  err = txnodeBuildXnodedEnvironment((const char *const *)inheritedEnvironment, inheritedEnvironmentCount, envXnoded,
                                     ARRAY_SIZE(envXnoded) - 1, &envXnodedWithPEnv);
  txnodeFreeEnvironment(inheritedEnvironment);
  inheritedEnvironment = NULL;
  if (err != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }
  options.env = envXnodedWithPEnv;

  err = uv_spawn(&pData->loop, &pData->process, &options);
  pData->process.data = (void *)pData;
  if (err != 0) {
    xndError("can not spawn xnoded. path: %s, error: %s", path, uv_strerror(err));
  } else {
    xndInfo("xnoded is initialized, xnoded pid: %d", pData->process.pid);
    saveXnodedPid(pData->process.pid);
  }

_OVER:
  txnodeFreeEnvironment(envXnodedWithPEnv);

  return err;
}

static void xnodeMgmtXnodedCloseWalkCb(uv_handle_t *handle, void *arg) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(handle);
  if (!uv_is_closing(handle)) {
    xndDebug("xnoded closing handle type:%d, ptr:%p", handle->type, handle);
    uv_close(handle, NULL);
  }
}

static void xnodeMgmtXnodedStopAsyncCb(uv_async_t *async) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(async);
  SXnodedData *pData = async->data;
  uv_stop(&pData->loop);
}

static void xnodeMgmtWatchXnoded(void *args) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(args);
  SXnodedData *pData = args;
  TAOS_UV_CHECK_ERRNO(uv_loop_init(&pData->loop));
  TAOS_UV_CHECK_ERRNO(uv_async_init(&pData->loop, &pData->stopAsync, xnodeMgmtXnodedStopAsyncCb));
  pData->stopAsync.data = pData;
  TAOS_UV_CHECK_ERRNO(xnodeMgmtSpawnXnoded(pData));
  atomic_store_32(&pData->spawnErr, 0);
  (void)uv_barrier_wait(&pData->barrier);
  int32_t num = uv_run(&pData->loop, UV_RUN_DEFAULT);
  xndInfo("xnoded loop exit with %d active handles, line:%d", num, __LINE__);

  uv_walk(&pData->loop, xnodeMgmtXnodedCloseWalkCb, NULL);
  num = uv_run(&pData->loop, UV_RUN_DEFAULT);
  xndInfo("xnoded loop exit with %d active handles, line:%d", num, __LINE__);
  if (uv_loop_close(&pData->loop) != 0) {
    xndError("xnoded loop close failed, lino:%d", __LINE__);
  }
  return;

_exit:
  if (terrno != 0) {
    (void)uv_barrier_wait(&pData->barrier);
    atomic_store_32(&pData->spawnErr, terrno);
    if (uv_loop_close(&pData->loop) != 0) {
      xndError("xnoded loop close failed, lino:%d", __LINE__);
    }

    xndError("xnoded thread exit with code:%d lino:%d", terrno, __LINE__);
    terrno = TSDB_CODE_XNODE_UV_EXEC_FAILURE;
  }
}

/**
 * start xnoded that serves xnode function invocation under dnode startDnodeId
 * @param startDnodeId
 * @return
 */
int32_t xnodeMgmtStartXnoded(SXnode *pXnode) {
  int32_t code = 0, lino = 0;

  SXnodedData *pData = &xnodedGlobal;
  pData->leaderEp = pXnode->ep;
  if (pData->isStarted) {
    xndInfo("dnode start xnoded already called");
    return 0;
  }
  pData->isStarted = true;
  char dnodeId[8] = {0};
  snprintf(dnodeId, sizeof(dnodeId), "%d", pXnode->dnodeId);
  TAOS_CHECK_GOTO(uv_os_setenv("DNODE_ID", dnodeId), &lino, _exit);
  pData->dnodeId = pXnode->dnodeId;
  pData->clusterId = pXnode->clusterId;
  memset(pData->userPass, 0, sizeof(pData->userPass));
  memcpy(pData->userPass, pXnode->userPass, pXnode->upLen);
  memset(pData->token, 0, sizeof(pData->token));
  memcpy(pData->token, pXnode->token, TSDB_TOKEN_LEN);

  TAOS_CHECK_GOTO(uv_barrier_init(&pData->barrier, 2), &lino, _exit);
  TAOS_CHECK_GOTO(uv_thread_create(&pData->thread, xnodeMgmtWatchXnoded, pData), &lino, _exit);
  (void)uv_barrier_wait(&pData->barrier);
  uv_sleep(100);
  int32_t err = atomic_load_32(&pData->spawnErr);
  if (err != 0) {
    uv_barrier_destroy(&pData->barrier);
    if (uv_async_send(&pData->stopAsync) != 0) {
      xndError("start xnoded: failed to send stop async");
    }
    if (uv_thread_join(&pData->thread) != 0) {
      xndError("start xnoded: failed to join xnoded thread");
    }
    pData->needCleanUp = false;
    xndInfo("xnoded is cleaned up after spawn err");
    TAOS_CHECK_GOTO(err, &lino, _exit);
  } else {
    pData->needCleanUp = true;
    atomic_store_32(&pData->isStopped, 0);
  }
_exit:
  if (code != 0) {
    xndError("xnoded start failed with lino:%d, code:%d, error: %s", code, lino, uv_strerror(code));
  }
  return code;
}
/**
 * stop xnoded
 * @return
 */
void xnodeMgmtStopXnoded(void) {
  SXnodedData *pData = &xnodedGlobal;
  xndInfo("stopping xnoded, need cleanup:%d, spawn err:%d, isStopped:%d", pData->needCleanUp, pData->spawnErr, atomic_load_32(&pData->isStopped));
  if (!pData->needCleanUp || atomic_load_32(&pData->isStopped)) {
    return;
  }
  atomic_store_32(&pData->isStopped, 1);
  pData->needCleanUp = false;
  (void)uv_process_kill(&pData->process, SIGTERM);
  uv_sleep(1000);
  uv_barrier_destroy(&pData->barrier);

  xndInfo("xnoded waiting to clean up");
  if (uv_thread_join(&pData->thread) != 0) {
    xndError("stop xnoded: failed to join xnoded thread");
  }
  xndInfo("xnoded is cleaned up");

  pData->isStarted = false;

  return;
}
