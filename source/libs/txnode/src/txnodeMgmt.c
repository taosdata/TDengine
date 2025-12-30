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

// clang-format off
#include "uv.h"
#include "os.h"
#include "tarray.h"
#include "tglobal.h"
#include "txnode.h"
#include "txnodeInt.h"
#include "osString.h"

// clang-format on

extern char **environ;

#ifdef WINDOWS
#define TAOSMQTT_DEFAULT_PATH "C:\\TDengine"
#define TAOSMQTT_DEFAULT_EXEC "\\xnoded.exe"
#else
#define XNODED_DEFAULT_PATH "/usr/bin"
#define XNODED_DEFAULT_EXEC "/xnoded"

#define XNODED_XNODED_PID_NAME ".xnoded.pid"

#endif

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
  SEp          leaderEp;
} SXnodedData;

SXnodedData xnodedGlobal = {0};

static int32_t xnodeMgmtSpawnXnoded(SXnodedData *pData);

static void getXnodedPidPath(char *pipeName, int32_t size) {
#ifdef _WIN32
  snprintf(pipeName, size, "%s", XNODED_XNODED_PID_NAME);
#else
  snprintf(pipeName, size, "%s%s", tsDataDir, XNODED_XNODED_PID_NAME);
#endif
  xndDebug("xnode get xnoded pid path:%s", pipeName);
}

static void    xnodeMgmtXnodedExit(uv_process_t *process, int64_t exitStatus, int32_t termSignal) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(process);
  xndDebug("xnoded process exited with status %" PRId64 ", signal %d", exitStatus, termSignal);
  SXnodedData *pData = process->data;
  if (pData == NULL) {
    xndError("xnoded process data is NULL");
    return;
  }
  if ((exitStatus == 0 && termSignal == 0) || atomic_load_32(&pData->isStopped)) {
    xndInfo("xnoded process exit due to exit status 0 or dnode-mgmt called stop");
    if (uv_async_send(&pData->stopAsync) != 0) {
      xndError("stop xnoded: failed to send stop async");
    }
    char xnodedPipeSocket[PATH_MAX] = {0};
    getXnodedPipeName(xnodedPipeSocket, PATH_MAX);
    (void)unlink(xnodedPipeSocket);

    char *pidPath = xnodedPipeSocket;
    memset(pidPath, 0, PATH_MAX);
    getXnodedPidPath(pidPath, PATH_MAX);
    (void)taosRemoveFile(pidPath);
  } else {
    xndInfo("xnoded process restart, exit status %ld, signal %d", exitStatus, termSignal);
    uv_sleep(2000);
    int32_t code = xnodeMgmtSpawnXnoded(pData);
    if (code != 0) {
      xndError("xnoded process restart failed with code:%d", code);
    }
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

static int32_t xnodeMgmtSpawnXnoded(SXnodedData *pData) {
  xndDebug("start to init xnoded");
  TAOS_XNODED_MGMT_CHECK_PTR_RCODE(pData);

  int32_t              err = 0;
  uv_process_options_t options = {0};

  char path[PATH_MAX] = {0};
  if (tsProcPath == NULL) {
    path[0] = '.';
#ifdef WINDOWS
    GetModuleFileName(NULL, path, PATH_MAX);
#elif defined(_TD_DARWIN_64)
    uint32_t pathSize = sizeof(path);
    _NSGetExecutablePath(path, &pathSize);
#endif
  } else {
    TAOS_STRNCPY(path, tsProcPath, PATH_MAX);
  }

  TAOS_DIRNAME(path);

  if (strlen(path) == 0) {
    TAOS_STRCAT(path, XNODED_DEFAULT_PATH);
  }
  TAOS_STRCAT(path, XNODED_DEFAULT_EXEC);

  xndInfo("xnode mgmt spawn xnoded path: %s", path);
  // char *argsXnoded[] = {path, "-c", configDir, "-d", dnodeId, NULL};
  char *argsXnoded[] = {path, NULL};
  options.args = argsXnoded;
  options.file = path;

  options.exit_cb = xnodeMgmtXnodedExit;

  killPreXnoded();

  char xnodedPipeSocket[PATH_MAX] = {0};
  getXnodedPipeName(xnodedPipeSocket, PATH_MAX);
  (void)unlink(xnodedPipeSocket);

  TAOS_UV_LIB_ERROR_RET(uv_pipe_init(&pData->loop, &pData->ctrlPipe, 1));

  uv_stdio_container_t child_stdio[3];
  child_stdio[0].flags = UV_CREATE_PIPE | UV_READABLE_PIPE;
  child_stdio[0].data.stream = (uv_stream_t *)&pData->ctrlPipe;
  child_stdio[1].flags = UV_IGNORE;
  child_stdio[2].flags = UV_INHERIT_FD;
  child_stdio[2].data.fd = 2;
  options.stdio_count = 3;
  options.stdio = child_stdio;

  options.flags = UV_PROCESS_DETACHED;

  char xnodedCfgDir[PATH_MAX] = {0};
  snprintf(xnodedCfgDir, PATH_MAX, "%s=%s", "XNODED_CFG_DIR", configDir);
  char xnodedLogDir[PATH_MAX] = {0};
  snprintf(xnodedLogDir, PATH_MAX, "%s=%s", "XNODED_LOG_DIR", tsLogDir);
  char dnodeIdEnvItem[64] = {0};
  snprintf(dnodeIdEnvItem, 64, "%s=%s:%d", "XNODED_LEADER_EP", pData->leaderEp.fqdn, pData->leaderEp.port);
  char xnodedUserPass[XNODE_USER_PASS_LEN] = {0};
  snprintf(xnodedUserPass, XNODE_USER_PASS_LEN, "%s=%s", "XNODED_USER_PASS", pData->userPass);
  char xnodeClusterId[32] = {0};
  snprintf(xnodeClusterId, 32, "%s=%lu", "XNODED_CLUSTER_ID", pData->clusterId);

  char xnodePipeSocket[PATH_MAX + 64] = {0};
  snprintf(xnodePipeSocket, PATH_MAX + 64, "%s=%s", "XNODED_LISTEN", xnodedPipeSocket);

  char *envXnoded[] = {xnodedCfgDir,    xnodedLogDir, dnodeIdEnvItem, xnodedUserPass, xnodeClusterId,
                       xnodePipeSocket, NULL};

  char **envXnodedWithPEnv = NULL;
  if (environ != NULL) {
    int32_t lenEnvXnoded = ARRAY_SIZE(envXnoded);
    int32_t numEnviron = 0;
    while (environ[numEnviron] != NULL) {
      numEnviron++;
    }

    envXnodedWithPEnv = (char **)taosMemoryCalloc(numEnviron + lenEnvXnoded, sizeof(char *));
    if (envXnodedWithPEnv == NULL) {
      err = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    for (int32_t i = 0; i < numEnviron; i++) {
      int32_t len = strlen(environ[i]) + 1;
      envXnodedWithPEnv[i] = (char *)taosMemoryCalloc(len, 1);
      if (envXnodedWithPEnv[i] == NULL) {
        err = TSDB_CODE_OUT_OF_MEMORY;
        goto _OVER;
      }

      tstrncpy(envXnodedWithPEnv[i], environ[i], len);
    }

    for (int32_t i = 0; i < lenEnvXnoded; i++) {
      if (envXnoded[i] != NULL) {
        int32_t len = strlen(envXnoded[i]) + 1;
        envXnodedWithPEnv[numEnviron + i] = (char *)taosMemoryCalloc(len, 1);
        if (envXnodedWithPEnv[numEnviron + i] == NULL) {
          err = TSDB_CODE_OUT_OF_MEMORY;
          goto _OVER;
        }

        tstrncpy(envXnodedWithPEnv[numEnviron + i], envXnoded[i], len);
      }
    }
    envXnodedWithPEnv[numEnviron + lenEnvXnoded - 1] = NULL;

    options.env = envXnodedWithPEnv;
  } else {
    options.env = envXnoded;
  }

  err = uv_spawn(&pData->loop, &pData->process, &options);
  pData->process.data = (void *)pData;
  if (err != 0) {
    xndError("can not spawn xnoded. path: %s, error: %s", path, uv_strerror(err));
  } else {
    xndInfo("xnoded is initialized, xnoded pid: %d", pData->process.pid);
    saveXnodedPid(pData->process.pid);
  }

_OVER:
  // if (taosFqdnEnvItem) {
  //   taosMemoryFree(taosFqdnEnvItem);
  // }

  if (envXnodedWithPEnv != NULL) {
    int32_t i = 0;
    while (envXnodedWithPEnv[i] != NULL) {
      taosMemoryFree(envXnodedWithPEnv[i]);
      i++;
    }
    taosMemoryFree(envXnodedWithPEnv);
  }

  return err;
}

static void xnodeMgmtXnodedCloseWalkCb(uv_handle_t *handle, void *arg) {
  TAOS_XNODED_MGMT_CHECK_PTR_RVOID(handle);
  if (!uv_is_closing(handle)) {
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

  TAOS_CHECK_GOTO(uv_barrier_init(&pData->barrier, 2), &lino, _exit);
  TAOS_CHECK_GOTO(uv_thread_create(&pData->thread, xnodeMgmtWatchXnoded, pData), &lino, _exit);
  (void)uv_barrier_wait(&pData->barrier);
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
  xndInfo("stopping xnoded, need cleanup:%d, spawn err:%d", pData->needCleanUp, pData->spawnErr);
  if (!pData->needCleanUp || atomic_load_32(&pData->isStopped)) {
    return;
  }
  atomic_store_32(&pData->isStopped, 1);
  pData->needCleanUp = false;
  (void)uv_process_kill(&pData->process, SIGTERM);
  uv_barrier_destroy(&pData->barrier);

  if (uv_thread_join(&pData->thread) != 0) {
    xndError("stop xnoded: failed to join xnoded thread");
  }
  xndInfo("xnoded is cleaned up");

  pData->isStarted = false;

  return;
}
