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
#include "tmqtt.h"

#ifdef _TD_DARWIN_64
#include <mach-o/dyld.h>
#endif
// clang-format on

typedef struct {
  bool         startCalled;
  bool         needCleanUp;
  uv_loop_t    loop;
  uv_thread_t  thread;
  uv_barrier_t barrier;
  uv_process_t process;
  int32_t      spawnErr;
  uv_pipe_t    ctrlPipe;
  uv_async_t   stopAsync;
  int32_t      stopCalled;
  int32_t      dnodeId;
} SMqttdData;

SMqttdData mqttdGlobal = {0};

extern char **environ;

#ifdef WINDOWS
#define TAOSMQTT_DEFAULT_PATH "C:\\TDengine"
#define TAOSMQTT_DEFAULT_EXEC "\\taosmqtt.exe"
#else
#define TAOSMQTT_DEFAULT_PATH "/usr/bin"
#define TAOSMQTT_DEFAULT_EXEC "/taosmqtt"
#endif

static void mqttMgmtMqttdExit(uv_process_t *process, int64_t exitStatus, int32_t termSignal);

static int32_t mqttMgmtSpawnMqttd(SMqttdData *pData) {
  bndInfo("start to init taosmqtt");
  TAOS_MQTT_MGMT_CHECK_PTR_RCODE(pData);

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
    TAOS_STRCAT(path, TAOSMQTT_DEFAULT_PATH);
  }
  TAOS_STRCAT(path, TAOSMQTT_DEFAULT_EXEC);

  char dnodeId[8] = "1";
  snprintf(dnodeId, sizeof(dnodeId), "%d", pData->dnodeId);

  char *argsMqttd[] = {path, "-c", configDir, "-d", dnodeId, NULL};
  options.args = argsMqttd;
  options.file = path;

  options.exit_cb = mqttMgmtMqttdExit;

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

  char dnodeIdEnvItem[32] = {0};
  char thrdPoolSizeEnvItem[32] = {0};
  snprintf(dnodeIdEnvItem, 32, "%s=%d", "DNODE_ID", pData->dnodeId);

  float   numCpuCores = 4;
  int32_t code = taosGetCpuCores(&numCpuCores, false);
  if (code != 0) {
    bndError("failed to get cpu cores, code:0x%x", code);
  }
  numCpuCores = TMAX(numCpuCores, 2);
  snprintf(thrdPoolSizeEnvItem, 32, "%s=%d", "UV_THREADPOOL_SIZE", (int32_t)numCpuCores * 2);

  char *taosFqdnEnvItem = NULL;
  char *taosFqdn = getenv("TAOS_FQDN");
  if (taosFqdn != NULL) {
    int32_t subLen = strlen(taosFqdn);
    int32_t len = strlen("TAOS_FQDN=") + subLen + 1;
    taosFqdnEnvItem = taosMemoryMalloc(len);
    if (taosFqdnEnvItem != NULL) {
      tstrncpy(taosFqdnEnvItem, "TAOS_FQDN=", len);
      TAOS_STRNCAT(taosFqdnEnvItem, taosFqdn, subLen);
      bndInfo("[MQTTD]Success to set TAOS_FQDN:%s", taosFqdn);
    } else {
      bndError("[MQTTD]Failed to allocate memory for TAOS_FQDN");
      return terrno;
    }
  }

  char *envMqttd[] = {dnodeIdEnvItem, thrdPoolSizeEnvItem, taosFqdnEnvItem, NULL};

  char **envMqttdWithPEnv = NULL;
  if (environ != NULL) {
    int32_t lenEnvMqttd = ARRAY_SIZE(envMqttd);
    int32_t numEnviron = 0;
    while (environ[numEnviron] != NULL) {
      numEnviron++;
    }

    envMqttdWithPEnv = (char **)taosMemoryCalloc(numEnviron + lenEnvMqttd, sizeof(char *));
    if (envMqttdWithPEnv == NULL) {
      err = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    for (int32_t i = 0; i < numEnviron; i++) {
      int32_t len = strlen(environ[i]) + 1;
      envMqttdWithPEnv[i] = (char *)taosMemoryCalloc(len, 1);
      if (envMqttdWithPEnv[i] == NULL) {
        err = TSDB_CODE_OUT_OF_MEMORY;
        goto _OVER;
      }

      tstrncpy(envMqttdWithPEnv[i], environ[i], len);
    }

    for (int32_t i = 0; i < lenEnvMqttd; i++) {
      if (envMqttd[i] != NULL) {
        int32_t len = strlen(envMqttd[i]) + 1;
        envMqttdWithPEnv[numEnviron + i] = (char *)taosMemoryCalloc(len, 1);
        if (envMqttdWithPEnv[numEnviron + i] == NULL) {
          err = TSDB_CODE_OUT_OF_MEMORY;
          goto _OVER;
        }

        tstrncpy(envMqttdWithPEnv[numEnviron + i], envMqttd[i], len);
      }
    }
    envMqttdWithPEnv[numEnviron + lenEnvMqttd - 1] = NULL;

    options.env = envMqttdWithPEnv;
  } else {
    options.env = envMqttd;
  }

  err = uv_spawn(&pData->loop, &pData->process, &options);
  pData->process.data = (void *)pData;
  if (err != 0) {
    bndError("can not spawn taosmqtt. path: %s, error: %s", path, uv_strerror(err));
  } else {
    bndInfo("taosmqtt is initialized");
  }

_OVER:
  if (taosFqdnEnvItem) {
    taosMemoryFree(taosFqdnEnvItem);
  }

  if (envMqttdWithPEnv != NULL) {
    int32_t i = 0;
    while (envMqttdWithPEnv[i] != NULL) {
      taosMemoryFree(envMqttdWithPEnv[i]);
      i++;
    }
    taosMemoryFree(envMqttdWithPEnv);
  }

  return err;
}

// static int32_t mqttMgmtSpawnMqttd(SMqttdData *pData);

static void mqttMgmtMqttdExit(uv_process_t *process, int64_t exitStatus, int32_t termSignal) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(process);
  bndInfo("taosmqtt process exited with status %" PRId64 ", signal %d", exitStatus, termSignal);
  SMqttdData *pData = process->data;
  if (pData == NULL) {
    bndError("taosmqtt process data is NULL");
    return;
  }
  if (exitStatus == 0 && termSignal == 0 || atomic_load_32(&pData->stopCalled)) {
    bndInfo("taosmqtt process exit due to SIGINT or dnode-mgmt called stop");
    if (uv_async_send(&pData->stopAsync) != 0) {
      bndError("stop taosmqtt: failed to send stop async");
    }

  } else {
    bndInfo("taosmqtt process restart");
    int32_t code = mqttMgmtSpawnMqttd(pData);
    if (code != 0) {
      bndError("taosmqtt process restart failed with code:%d", code);
    }
  }
}

static void mqttMgmtMqttdCloseWalkCb(uv_handle_t *handle, void *arg) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(handle);
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

static void mqttMgmtMqttdStopAsyncCb(uv_async_t *async) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(async);
  SMqttdData *pData = async->data;
  uv_stop(&pData->loop);
}

static void mqttMgmtWatchMqttd(void *args) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(args);
  SMqttdData *pData = args;
  TAOS_UV_CHECK_ERRNO(uv_loop_init(&pData->loop));
  TAOS_UV_CHECK_ERRNO(uv_async_init(&pData->loop, &pData->stopAsync, mqttMgmtMqttdStopAsyncCb));
  pData->stopAsync.data = pData;
  TAOS_UV_CHECK_ERRNO(mqttMgmtSpawnMqttd(pData));
  atomic_store_32(&pData->spawnErr, 0);
  (void)uv_barrier_wait(&pData->barrier);
  int32_t num = uv_run(&pData->loop, UV_RUN_DEFAULT);
  bndInfo("taosmqtt loop exit with %d active handles, line:%d", num, __LINE__);

  uv_walk(&pData->loop, mqttMgmtMqttdCloseWalkCb, NULL);
  num = uv_run(&pData->loop, UV_RUN_DEFAULT);
  bndInfo("taosmqtt loop exit with %d active handles, line:%d", num, __LINE__);
  if (uv_loop_close(&pData->loop) != 0) {
    bndError("taosmqtt loop close failed, lino:%d", __LINE__);
  }
  return;

_exit:
  if (terrno != 0) {
    (void)uv_barrier_wait(&pData->barrier);
    atomic_store_32(&pData->spawnErr, terrno);
    if (uv_loop_close(&pData->loop) != 0) {
      bndError("taosmqtt loop close failed, lino:%d", __LINE__);
    }

    bndError("taosmqtt thread exit with code:%d lino:%d", terrno, __LINE__);
    terrno = TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  }
}

int32_t mqttMgmtStartMqttd(int32_t startDnodeId) {
  int32_t code = 0, lino = 0;

  SMqttdData *pData = &mqttdGlobal;
  if (pData->startCalled) {
    bndInfo("dnode start taosmqtt already called");
    return 0;
  }
  pData->startCalled = true;
  char dnodeId[8] = {0};
  snprintf(dnodeId, sizeof(dnodeId), "%d", startDnodeId);
  TAOS_CHECK_GOTO(uv_os_setenv("DNODE_ID", dnodeId), &lino, _exit);
  pData->dnodeId = startDnodeId;

  TAOS_CHECK_GOTO(uv_barrier_init(&pData->barrier, 2), &lino, _exit);
  TAOS_CHECK_GOTO(uv_thread_create(&pData->thread, mqttMgmtWatchMqttd, pData), &lino, _exit);
  (void)uv_barrier_wait(&pData->barrier);
  int32_t err = atomic_load_32(&pData->spawnErr);
  if (err != 0) {
    uv_barrier_destroy(&pData->barrier);
    if (uv_async_send(&pData->stopAsync) != 0) {
      bndError("start taosmqtt: failed to send stop async");
    }
    if (uv_thread_join(&pData->thread) != 0) {
      bndError("start taosmqtt: failed to join taosmqtt thread");
    }
    pData->needCleanUp = false;
    bndInfo("taosmqtt is cleaned up after spawn err");
    TAOS_CHECK_GOTO(err, &lino, _exit);
  } else {
    pData->needCleanUp = true;
    atomic_store_32(&pData->stopCalled, 0);
  }
_exit:
  if (code != 0) {
    bndError("taosmqtt start failed with code:%d, lino:%d", code, lino);
  }
  return code;
}

void mqttMgmtStopMqttd() {
  SMqttdData *pData = &mqttdGlobal;
  bndInfo("taosmqtt start to stop, need cleanup:%d, spawn err:%d", pData->needCleanUp, pData->spawnErr);
  if (!pData->needCleanUp || atomic_load_32(&pData->stopCalled)) {
    return;
  }
  atomic_store_32(&pData->stopCalled, 1);
  pData->needCleanUp = false;
  uv_process_kill(&pData->process, SIGTERM);
  uv_barrier_destroy(&pData->barrier);

  if (uv_thread_join(&pData->thread) != 0) {
    bndError("stop taosmqtt: failed to join taosmqtt thread");
  }
  bndInfo("taosmqtt is cleaned up");

  pData->startCalled = false;

  return;
}

/*
int32_t mqttMgmtGetMqttdPid(int32_t* pMqttdPid) {
  SMqttdData *pData = &mqttdGlobal;
  if (pData->spawnErr) {
    return pData->spawnErr;
  }
  uv_pid_t pid = uv_process_get_pid(&pData->process);
  if (pMqttdPid) {
    *pMqttdPid = (int32_t)pid;
  }
  return TSDB_CODE_SUCCESS;
}
*/
