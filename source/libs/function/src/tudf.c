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
#include "uv.h"

#include "os.h"

#include "builtinsimpl.h"
#include "fnLog.h"
#include "functionMgt.h"
#include "querynodes.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tudf.h"
#include "tudfInt.h"

typedef struct SUdfdData {
  bool         startCalled;
  bool         needCleanUp;
  uv_loop_t    loop;
  uv_thread_t  thread;
  uv_barrier_t barrier;
  uv_process_t process;
#ifdef WINDOWS
  HANDLE jobHandle;
#endif
  int        spawnErr;
  uv_pipe_t  ctrlPipe;
  uv_async_t stopAsync;
  int32_t    stopCalled;

  int32_t dnodeId;
} SUdfdData;

SUdfdData udfdGlobal = {0};

int32_t udfStartUdfd(int32_t startDnodeId);
int32_t udfStopUdfd();

static int32_t udfSpawnUdfd(SUdfdData *pData);
void           udfUdfdExit(uv_process_t *process, int64_t exitStatus, int termSignal);
static int32_t udfSpawnUdfd(SUdfdData *pData);
static void    udfUdfdCloseWalkCb(uv_handle_t *handle, void *arg);
static void    udfUdfdStopAsyncCb(uv_async_t *async);
static void    udfWatchUdfd(void *args);

void udfUdfdExit(uv_process_t *process, int64_t exitStatus, int termSignal) {
  fnInfo("udfd process exited with status %" PRId64 ", signal %d", exitStatus, termSignal);
  SUdfdData *pData = process->data;
  if (exitStatus == 0 && termSignal == 0 || atomic_load_32(&pData->stopCalled)) {
    fnInfo("udfd process exit due to SIGINT or dnode-mgmt called stop");
  } else {
    fnInfo("udfd process restart");
    udfSpawnUdfd(pData);
  }
}

static int32_t udfSpawnUdfd(SUdfdData *pData) {
  fnInfo("start to init udfd");
  uv_process_options_t options = {0};

  char path[PATH_MAX] = {0};
  if (tsProcPath == NULL) {
    path[0] = '.';
#ifdef WINDOWS
    GetModuleFileName(NULL, path, PATH_MAX);
    taosDirName(path);
#elif defined(_TD_DARWIN_64)
    uint32_t pathSize = sizeof(path);
    _NSGetExecutablePath(path, &pathSize);
    taosDirName(path);
#endif
  } else {
    strncpy(path, tsProcPath, PATH_MAX);
    taosDirName(path);
  }
#ifdef WINDOWS
  if (strlen(path) == 0) {
    strcat(path, "C:\\TDengine");
  }
  strcat(path, "\\udfd.exe");
#else
  if (strlen(path) == 0) {
    strcat(path, "/usr/bin");
  }
  strcat(path, "/udfd");
#endif
  char *argsUdfd[] = {path, "-c", configDir, NULL};
  options.args = argsUdfd;
  options.file = path;

  options.exit_cb = udfUdfdExit;

  uv_pipe_init(&pData->loop, &pData->ctrlPipe, 1);

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

  float numCpuCores = 4;
  taosGetCpuCores(&numCpuCores, false);
  numCpuCores = TMAX(numCpuCores, 2);
  snprintf(thrdPoolSizeEnvItem, 32, "%s=%d", "UV_THREADPOOL_SIZE", (int)numCpuCores * 2);

  char   pathTaosdLdLib[512] = {0};
  size_t taosdLdLibPathLen = sizeof(pathTaosdLdLib);
  int ret = uv_os_getenv("LD_LIBRARY_PATH", pathTaosdLdLib, &taosdLdLibPathLen);
  if (ret != UV_ENOBUFS) {
    taosdLdLibPathLen = strlen(pathTaosdLdLib);
  }

  char   udfdPathLdLib[1024] = {0};
  size_t udfdLdLibPathLen = strlen(tsUdfdLdLibPath);
  strncpy(udfdPathLdLib, tsUdfdLdLibPath, tListLen(udfdPathLdLib));

  udfdPathLdLib[udfdLdLibPathLen] = ':';
  strncpy(udfdPathLdLib + udfdLdLibPathLen + 1, pathTaosdLdLib, sizeof(udfdPathLdLib) - udfdLdLibPathLen - 1);
  if (udfdLdLibPathLen + taosdLdLibPathLen < 1024) {
    fnInfo("udfd LD_LIBRARY_PATH: %s", udfdPathLdLib);
  } else {
    fnError("can not set correct udfd LD_LIBRARY_PATH");
  }
  char ldLibPathEnvItem[1024 + 32] = {0};
  snprintf(ldLibPathEnvItem, 1024 + 32, "%s=%s", "LD_LIBRARY_PATH", udfdPathLdLib);

  char *envUdfd[] = {dnodeIdEnvItem, thrdPoolSizeEnvItem, ldLibPathEnvItem, NULL};
  options.env = envUdfd;

  int err = uv_spawn(&pData->loop, &pData->process, &options);
  pData->process.data = (void *)pData;

#ifdef WINDOWS
  // End udfd.exe by Job.
  if (pData->jobHandle != NULL) CloseHandle(pData->jobHandle);
  pData->jobHandle = CreateJobObject(NULL, NULL);
  bool add_job_ok = AssignProcessToJobObject(pData->jobHandle, pData->process.process_handle);
  if (!add_job_ok) {
    fnError("Assign udfd to job failed.");
  } else {
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limit_info;
    memset(&limit_info, 0x0, sizeof(limit_info));
    limit_info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    bool set_auto_kill_ok =
        SetInformationJobObject(pData->jobHandle, JobObjectExtendedLimitInformation, &limit_info, sizeof(limit_info));
    if (!set_auto_kill_ok) {
      fnError("Set job auto kill udfd failed.");
    }
  }
#endif

  if (err != 0) {
    fnError("can not spawn udfd. path: %s, error: %s", path, uv_strerror(err));
  } else {
    fnInfo("udfd is initialized");
  }
  return err;
}

static void udfUdfdCloseWalkCb(uv_handle_t *handle, void *arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

static void udfUdfdStopAsyncCb(uv_async_t *async) {
  SUdfdData *pData = async->data;
  uv_stop(&pData->loop);
}

static void udfWatchUdfd(void *args) {
  SUdfdData *pData = args;
  uv_loop_init(&pData->loop);
  uv_async_init(&pData->loop, &pData->stopAsync, udfUdfdStopAsyncCb);
  pData->stopAsync.data = pData;
  int32_t err = udfSpawnUdfd(pData);
  atomic_store_32(&pData->spawnErr, err);
  uv_barrier_wait(&pData->barrier);
  uv_run(&pData->loop, UV_RUN_DEFAULT);
  uv_loop_close(&pData->loop);

  uv_walk(&pData->loop, udfUdfdCloseWalkCb, NULL);
  uv_run(&pData->loop, UV_RUN_DEFAULT);
  uv_loop_close(&pData->loop);
  return;
}

int32_t udfStartUdfd(int32_t startDnodeId) {
  if (!tsStartUdfd) {
    fnInfo("start udfd is disabled.") return 0;
  }
  SUdfdData *pData = &udfdGlobal;
  if (pData->startCalled) {
    fnInfo("dnode start udfd already called");
    return 0;
  }
  pData->startCalled = true;
  char dnodeId[8] = {0};
  snprintf(dnodeId, sizeof(dnodeId), "%d", startDnodeId);
  uv_os_setenv("DNODE_ID", dnodeId);
  pData->dnodeId = startDnodeId;

  uv_barrier_init(&pData->barrier, 2);
  uv_thread_create(&pData->thread, udfWatchUdfd, pData);
  uv_barrier_wait(&pData->barrier);
  int32_t err = atomic_load_32(&pData->spawnErr);
  if (err != 0) {
    uv_barrier_destroy(&pData->barrier);
    uv_async_send(&pData->stopAsync);
    uv_thread_join(&pData->thread);
    pData->needCleanUp = false;
    fnInfo("udfd is cleaned up after spawn err");
  } else {
    pData->needCleanUp = true;
  }
  return err;
}

int32_t udfStopUdfd() {
  SUdfdData *pData = &udfdGlobal;
  fnInfo("udfd start to stop, need cleanup:%d, spawn err:%d", pData->needCleanUp, pData->spawnErr);
  if (!pData->needCleanUp || atomic_load_32(&pData->stopCalled)) {
    return 0;
  }
  atomic_store_32(&pData->stopCalled, 1);
  pData->needCleanUp = false;
  uv_barrier_destroy(&pData->barrier);
  uv_async_send(&pData->stopAsync);
  uv_thread_join(&pData->thread);
#ifdef WINDOWS
  if (pData->jobHandle != NULL) CloseHandle(pData->jobHandle);
#endif
  fnInfo("udfd is cleaned up");
  return 0;
}

int32_t udfGetUdfdPid(int32_t* pUdfdPid) {
  SUdfdData *pData = &udfdGlobal;
  if (pData->spawnErr) {
    return pData->spawnErr;
  }
  uv_pid_t pid = uv_process_get_pid(&pData->process);
  if (pUdfdPid) {
    *pUdfdPid = (int32_t)pid;
  }
  return TSDB_CODE_SUCCESS;
}

//==============================================================================================
/* Copyright (c) 2013, Ben Noordhuis <info@bnoordhuis.nl>
 * The QUEUE is copied from queue.h under libuv
 * */

typedef void *QUEUE[2];

/* Private macros. */
#define QUEUE_NEXT(q)      (*(QUEUE **)&((*(q))[0]))
#define QUEUE_PREV(q)      (*(QUEUE **)&((*(q))[1]))
#define QUEUE_PREV_NEXT(q) (QUEUE_NEXT(QUEUE_PREV(q)))
#define QUEUE_NEXT_PREV(q) (QUEUE_PREV(QUEUE_NEXT(q)))

/* Public macros. */
#define QUEUE_DATA(ptr, type, field) ((type *)((char *)(ptr)-offsetof(type, field)))

/* Important note: mutating the list while QUEUE_FOREACH is
 * iterating over its elements results in undefined behavior.
 */
#define QUEUE_FOREACH(q, h) for ((q) = QUEUE_NEXT(h); (q) != (h); (q) = QUEUE_NEXT(q))

#define QUEUE_EMPTY(q) ((const QUEUE *)(q) == (const QUEUE *)QUEUE_NEXT(q))

#define QUEUE_HEAD(q) (QUEUE_NEXT(q))

#define QUEUE_INIT(q)    \
  do {                   \
    QUEUE_NEXT(q) = (q); \
    QUEUE_PREV(q) = (q); \
  } while (0)

#define QUEUE_ADD(h, n)                 \
  do {                                  \
    QUEUE_PREV_NEXT(h) = QUEUE_NEXT(n); \
    QUEUE_NEXT_PREV(n) = QUEUE_PREV(h); \
    QUEUE_PREV(h) = QUEUE_PREV(n);      \
    QUEUE_PREV_NEXT(h) = (h);           \
  } while (0)

#define QUEUE_SPLIT(h, q, n)       \
  do {                             \
    QUEUE_PREV(n) = QUEUE_PREV(h); \
    QUEUE_PREV_NEXT(n) = (n);      \
    QUEUE_NEXT(n) = (q);           \
    QUEUE_PREV(h) = QUEUE_PREV(q); \
    QUEUE_PREV_NEXT(h) = (h);      \
    QUEUE_PREV(q) = (n);           \
  } while (0)

#define QUEUE_MOVE(h, n)        \
  do {                          \
    if (QUEUE_EMPTY(h))         \
      QUEUE_INIT(n);            \
    else {                      \
      QUEUE *q = QUEUE_HEAD(h); \
      QUEUE_SPLIT(h, q, n);     \
    }                           \
  } while (0)

#define QUEUE_INSERT_HEAD(h, q)    \
  do {                             \
    QUEUE_NEXT(q) = QUEUE_NEXT(h); \
    QUEUE_PREV(q) = (h);           \
    QUEUE_NEXT_PREV(q) = (q);      \
    QUEUE_NEXT(h) = (q);           \
  } while (0)

#define QUEUE_INSERT_TAIL(h, q)    \
  do {                             \
    QUEUE_NEXT(q) = (h);           \
    QUEUE_PREV(q) = QUEUE_PREV(h); \
    QUEUE_PREV_NEXT(q) = (q);      \
    QUEUE_PREV(h) = (q);           \
  } while (0)

#define QUEUE_REMOVE(q)                 \
  do {                                  \
    QUEUE_PREV_NEXT(q) = QUEUE_NEXT(q); \
    QUEUE_NEXT_PREV(q) = QUEUE_PREV(q); \
  } while (0)

enum { UV_TASK_CONNECT = 0, UV_TASK_REQ_RSP = 1, UV_TASK_DISCONNECT = 2 };

int64_t gUdfTaskSeqNum = 0;
typedef struct SUdfcFuncStub {
  char           udfName[TSDB_FUNC_NAME_LEN + 1];
  UdfcFuncHandle handle;
  int32_t        refCount;
  int64_t        createTime;
} SUdfcFuncStub;

typedef struct SUdfcProxy {
  char         udfdPipeName[PATH_MAX + UDF_LISTEN_PIPE_NAME_LEN + 2];
  uv_barrier_t initBarrier;

  uv_loop_t   uvLoop;
  uv_thread_t loopThread;
  uv_async_t  loopTaskAync;

  uv_async_t loopStopAsync;

  uv_mutex_t taskQueueMutex;
  int8_t     udfcState;
  QUEUE      taskQueue;
  QUEUE      uvProcTaskQueue;

  uv_mutex_t udfStubsMutex;
  SArray    *udfStubs;  // SUdfcFuncStub
  SArray    *expiredUdfStubs; //SUdfcFuncStub

  uv_mutex_t udfcUvMutex;
  int8_t     initialized;
} SUdfcProxy;

SUdfcProxy gUdfcProxy = {0};

typedef struct SUdfcUvSession {
  SUdfcProxy *udfc;
  int64_t     severHandle;
  uv_pipe_t  *udfUvPipe;

  int8_t  outputType;
  int32_t bytes;
  int32_t bufSize;

  char udfName[TSDB_FUNC_NAME_LEN + 1];
} SUdfcUvSession;

typedef struct SClientUvTaskNode {
  SUdfcProxy *udfc;
  int8_t      type;
  int         errCode;

  uv_pipe_t *pipe;

  int64_t  seqNum;
  uv_buf_t reqBuf;

  uv_sem_t taskSem;
  uv_buf_t rspBuf;

  QUEUE recvTaskQueue;
  QUEUE procTaskQueue;
  QUEUE connTaskQueue;
} SClientUvTaskNode;

typedef struct SClientUdfTask {
  int8_t type;

  SUdfcUvSession *session;

  int32_t errCode;

  union {
    struct {
      SUdfSetupRequest  req;
      SUdfSetupResponse rsp;
    } _setup;
    struct {
      SUdfCallRequest  req;
      SUdfCallResponse rsp;
    } _call;
    struct {
      SUdfTeardownRequest  req;
      SUdfTeardownResponse rsp;
    } _teardown;
  };

} SClientUdfTask;

typedef struct SClientConnBuf {
  char   *buf;
  int32_t len;
  int32_t cap;
  int32_t total;
} SClientConnBuf;

typedef struct SClientUvConn {
  uv_pipe_t      *pipe;
  QUEUE           taskQueue;
  SClientConnBuf  readBuf;
  SUdfcUvSession *session;
} SClientUvConn;

enum {
  UDFC_STATE_INITAL = 0,  // initial state
  UDFC_STATE_STARTNG,     // starting after udfcOpen
  UDFC_STATE_READY,       // started and begin to receive quests
  UDFC_STATE_STOPPING,    // stopping after udfcClose
};

int32_t getUdfdPipeName(char *pipeName, int32_t size);
int32_t encodeUdfSetupRequest(void **buf, const SUdfSetupRequest *setup);
void   *decodeUdfSetupRequest(const void *buf, SUdfSetupRequest *request);
int32_t encodeUdfInterBuf(void **buf, const SUdfInterBuf *state);
void   *decodeUdfInterBuf(const void *buf, SUdfInterBuf *state);
int32_t encodeUdfCallRequest(void **buf, const SUdfCallRequest *call);
void   *decodeUdfCallRequest(const void *buf, SUdfCallRequest *call);
int32_t encodeUdfTeardownRequest(void **buf, const SUdfTeardownRequest *teardown);
void   *decodeUdfTeardownRequest(const void *buf, SUdfTeardownRequest *teardown);
int32_t encodeUdfRequest(void **buf, const SUdfRequest *request);
void   *decodeUdfRequest(const void *buf, SUdfRequest *request);
int32_t encodeUdfSetupResponse(void **buf, const SUdfSetupResponse *setupRsp);
void   *decodeUdfSetupResponse(const void *buf, SUdfSetupResponse *setupRsp);
int32_t encodeUdfCallResponse(void **buf, const SUdfCallResponse *callRsp);
void   *decodeUdfCallResponse(const void *buf, SUdfCallResponse *callRsp);
int32_t encodeUdfTeardownResponse(void **buf, const SUdfTeardownResponse *teardownRsp);
void   *decodeUdfTeardownResponse(const void *buf, SUdfTeardownResponse *teardownResponse);
int32_t encodeUdfResponse(void **buf, const SUdfResponse *rsp);
void   *decodeUdfResponse(const void *buf, SUdfResponse *rsp);
void    freeUdfColumnData(SUdfColumnData *data, SUdfColumnMeta *meta);
void    freeUdfColumn(SUdfColumn *col);
void    freeUdfDataDataBlock(SUdfDataBlock *block);
void    freeUdfInterBuf(SUdfInterBuf *buf);
int32_t convertDataBlockToUdfDataBlock(SSDataBlock *block, SUdfDataBlock *udfBlock);
int32_t convertUdfColumnToDataBlock(SUdfColumn *udfCol, SSDataBlock *block);
int32_t convertScalarParamToDataBlock(SScalarParam *input, int32_t numOfCols, SSDataBlock *output);
int32_t convertDataBlockToScalarParm(SSDataBlock *input, SScalarParam *output);

int32_t getUdfdPipeName(char *pipeName, int32_t size) {
  char    dnodeId[8] = {0};
  size_t  dnodeIdSize = sizeof(dnodeId);
  int32_t err = uv_os_getenv(UDF_DNODE_ID_ENV_NAME, dnodeId, &dnodeIdSize);
  if (err != 0) {
    fnError("failed to get dnodeId from env since %s", uv_err_name(err));
    dnodeId[0] = '1';
  }
#ifdef _WIN32
  snprintf(pipeName, size, "%s.%x.%s", UDF_LISTEN_PIPE_NAME_PREFIX, MurmurHash3_32(tsDataDir, strlen(tsDataDir)),
           dnodeId);
#else
  snprintf(pipeName, size, "%s/%s%s", tsDataDir, UDF_LISTEN_PIPE_NAME_PREFIX, dnodeId);
#endif
  fnInfo("get dnodeId:%s from env, pipe path:%s", dnodeId, pipeName);
  return 0;
}

int32_t encodeUdfSetupRequest(void **buf, const SUdfSetupRequest *setup) {
  int32_t len = 0;
  len += taosEncodeBinary(buf, setup->udfName, TSDB_FUNC_NAME_LEN);
  return len;
}

void *decodeUdfSetupRequest(const void *buf, SUdfSetupRequest *request) {
  buf = taosDecodeBinaryTo(buf, request->udfName, TSDB_FUNC_NAME_LEN);
  return (void *)buf;
}

int32_t encodeUdfInterBuf(void **buf, const SUdfInterBuf *state) {
  int32_t len = 0;
  len += taosEncodeFixedI8(buf, state->numOfResult);
  len += taosEncodeFixedI32(buf, state->bufLen);
  len += taosEncodeBinary(buf, state->buf, state->bufLen);
  return len;
}

void *decodeUdfInterBuf(const void *buf, SUdfInterBuf *state) {
  buf = taosDecodeFixedI8(buf, &state->numOfResult);
  buf = taosDecodeFixedI32(buf, &state->bufLen);
  buf = taosDecodeBinary(buf, (void **)&state->buf, state->bufLen);
  return (void *)buf;
}

int32_t encodeUdfCallRequest(void **buf, const SUdfCallRequest *call) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, call->udfHandle);
  len += taosEncodeFixedI8(buf, call->callType);
  if (call->callType == TSDB_UDF_CALL_SCALA_PROC) {
    len += tEncodeDataBlock(buf, &call->block);
  } else if (call->callType == TSDB_UDF_CALL_AGG_INIT) {
    len += taosEncodeFixedI8(buf, call->initFirst);
  } else if (call->callType == TSDB_UDF_CALL_AGG_PROC) {
    len += tEncodeDataBlock(buf, &call->block);
    len += encodeUdfInterBuf(buf, &call->interBuf);
  } else if (call->callType == TSDB_UDF_CALL_AGG_MERGE) {
    len += encodeUdfInterBuf(buf, &call->interBuf);
    len += encodeUdfInterBuf(buf, &call->interBuf2);
  } else if (call->callType == TSDB_UDF_CALL_AGG_FIN) {
    len += encodeUdfInterBuf(buf, &call->interBuf);
  }
  return len;
}

void *decodeUdfCallRequest(const void *buf, SUdfCallRequest *call) {
  buf = taosDecodeFixedI64(buf, &call->udfHandle);
  buf = taosDecodeFixedI8(buf, &call->callType);
  switch (call->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      buf = tDecodeDataBlock(buf, &call->block);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      buf = taosDecodeFixedI8(buf, &call->initFirst);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      buf = tDecodeDataBlock(buf, &call->block);
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      buf = decodeUdfInterBuf(buf, &call->interBuf2);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      buf = decodeUdfInterBuf(buf, &call->interBuf);
      break;
  }
  return (void *)buf;
}

int32_t encodeUdfTeardownRequest(void **buf, const SUdfTeardownRequest *teardown) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, teardown->udfHandle);
  return len;
}

void *decodeUdfTeardownRequest(const void *buf, SUdfTeardownRequest *teardown) {
  buf = taosDecodeFixedI64(buf, &teardown->udfHandle);
  return (void *)buf;
}

int32_t encodeUdfRequest(void **buf, const SUdfRequest *request) {
  int32_t len = 0;
  if (buf == NULL) {
    len += sizeof(request->msgLen);
  } else {
    *(int32_t *)(*buf) = request->msgLen;
    *buf = POINTER_SHIFT(*buf, sizeof(request->msgLen));
  }
  len += taosEncodeFixedI64(buf, request->seqNum);
  len += taosEncodeFixedI8(buf, request->type);
  if (request->type == UDF_TASK_SETUP) {
    len += encodeUdfSetupRequest(buf, &request->setup);
  } else if (request->type == UDF_TASK_CALL) {
    len += encodeUdfCallRequest(buf, &request->call);
  } else if (request->type == UDF_TASK_TEARDOWN) {
    len += encodeUdfTeardownRequest(buf, &request->teardown);
  }
  return len;
}

void *decodeUdfRequest(const void *buf, SUdfRequest *request) {
  request->msgLen = *(int32_t *)(buf);
  buf = POINTER_SHIFT(buf, sizeof(request->msgLen));

  buf = taosDecodeFixedI64(buf, &request->seqNum);
  buf = taosDecodeFixedI8(buf, &request->type);

  if (request->type == UDF_TASK_SETUP) {
    buf = decodeUdfSetupRequest(buf, &request->setup);
  } else if (request->type == UDF_TASK_CALL) {
    buf = decodeUdfCallRequest(buf, &request->call);
  } else if (request->type == UDF_TASK_TEARDOWN) {
    buf = decodeUdfTeardownRequest(buf, &request->teardown);
  }
  return (void *)buf;
}

int32_t encodeUdfSetupResponse(void **buf, const SUdfSetupResponse *setupRsp) {
  int32_t len = 0;
  len += taosEncodeFixedI64(buf, setupRsp->udfHandle);
  len += taosEncodeFixedI8(buf, setupRsp->outputType);
  len += taosEncodeFixedI32(buf, setupRsp->bytes);
  len += taosEncodeFixedI32(buf, setupRsp->bufSize);
  return len;
}

void *decodeUdfSetupResponse(const void *buf, SUdfSetupResponse *setupRsp) {
  buf = taosDecodeFixedI64(buf, &setupRsp->udfHandle);
  buf = taosDecodeFixedI8(buf, &setupRsp->outputType);
  buf = taosDecodeFixedI32(buf, &setupRsp->bytes);
  buf = taosDecodeFixedI32(buf, &setupRsp->bufSize);
  return (void *)buf;
}

int32_t encodeUdfCallResponse(void **buf, const SUdfCallResponse *callRsp) {
  int32_t len = 0;
  len += taosEncodeFixedI8(buf, callRsp->callType);
  switch (callRsp->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      len += tEncodeDataBlock(buf, &callRsp->resultData);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      len += encodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
  }
  return len;
}

void *decodeUdfCallResponse(const void *buf, SUdfCallResponse *callRsp) {
  buf = taosDecodeFixedI8(buf, &callRsp->callType);
  switch (callRsp->callType) {
    case TSDB_UDF_CALL_SCALA_PROC:
      buf = tDecodeDataBlock(buf, &callRsp->resultData);
      break;
    case TSDB_UDF_CALL_AGG_INIT:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_PROC:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_MERGE:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
    case TSDB_UDF_CALL_AGG_FIN:
      buf = decodeUdfInterBuf(buf, &callRsp->resultBuf);
      break;
  }
  return (void *)buf;
}

int32_t encodeUdfTeardownResponse(void **buf, const SUdfTeardownResponse *teardownRsp) { return 0; }

void *decodeUdfTeardownResponse(const void *buf, SUdfTeardownResponse *teardownResponse) { return (void *)buf; }

int32_t encodeUdfResponse(void **buf, const SUdfResponse *rsp) {
  int32_t len = 0;
  if (buf == NULL) {
    len += sizeof(rsp->msgLen);
  } else {
    *(int32_t *)(*buf) = rsp->msgLen;
    *buf = POINTER_SHIFT(*buf, sizeof(rsp->msgLen));
  }

  if (buf == NULL) {
    len += sizeof(rsp->seqNum);
  } else {
    *(int64_t *)(*buf) = rsp->seqNum;
    *buf = POINTER_SHIFT(*buf, sizeof(rsp->seqNum));
  }

  len += taosEncodeFixedI64(buf, rsp->seqNum);
  len += taosEncodeFixedI8(buf, rsp->type);
  len += taosEncodeFixedI32(buf, rsp->code);

  switch (rsp->type) {
    case UDF_TASK_SETUP:
      len += encodeUdfSetupResponse(buf, &rsp->setupRsp);
      break;
    case UDF_TASK_CALL:
      len += encodeUdfCallResponse(buf, &rsp->callRsp);
      break;
    case UDF_TASK_TEARDOWN:
      len += encodeUdfTeardownResponse(buf, &rsp->teardownRsp);
      break;
    default:
      fnError("encode udf response, invalid udf response type %d", rsp->type);
      break;
  }
  return len;
}

void *decodeUdfResponse(const void *buf, SUdfResponse *rsp) {
  rsp->msgLen = *(int32_t *)(buf);
  buf = POINTER_SHIFT(buf, sizeof(rsp->msgLen));
  rsp->seqNum = *(int64_t *)(buf);
  buf = POINTER_SHIFT(buf, sizeof(rsp->seqNum));
  buf = taosDecodeFixedI64(buf, &rsp->seqNum);
  buf = taosDecodeFixedI8(buf, &rsp->type);
  buf = taosDecodeFixedI32(buf, &rsp->code);

  switch (rsp->type) {
    case UDF_TASK_SETUP:
      buf = decodeUdfSetupResponse(buf, &rsp->setupRsp);
      break;
    case UDF_TASK_CALL:
      buf = decodeUdfCallResponse(buf, &rsp->callRsp);
      break;
    case UDF_TASK_TEARDOWN:
      buf = decodeUdfTeardownResponse(buf, &rsp->teardownRsp);
      break;
    default:
      fnError("decode udf response, invalid udf response type %d", rsp->type);
      break;
  }
  return (void *)buf;
}

void freeUdfColumnData(SUdfColumnData *data, SUdfColumnMeta *meta) {
  if (IS_VAR_DATA_TYPE(meta->type)) {
    taosMemoryFree(data->varLenCol.varOffsets);
    data->varLenCol.varOffsets = NULL;
    taosMemoryFree(data->varLenCol.payload);
    data->varLenCol.payload = NULL;
  } else {
    taosMemoryFree(data->fixLenCol.nullBitmap);
    data->fixLenCol.nullBitmap = NULL;
    taosMemoryFree(data->fixLenCol.data);
    data->fixLenCol.data = NULL;
  }
}

void freeUdfColumn(SUdfColumn *col) { freeUdfColumnData(&col->colData, &col->colMeta); }

void freeUdfDataDataBlock(SUdfDataBlock *block) {
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    freeUdfColumn(block->udfCols[i]);
    taosMemoryFree(block->udfCols[i]);
    block->udfCols[i] = NULL;
  }
  taosMemoryFree(block->udfCols);
  block->udfCols = NULL;
}

void freeUdfInterBuf(SUdfInterBuf *buf) {
  taosMemoryFree(buf->buf);
  buf->buf = NULL;
}

int32_t convertDataBlockToUdfDataBlock(SSDataBlock *block, SUdfDataBlock *udfBlock) {
  udfBlock->numOfRows = block->info.rows;
  udfBlock->numOfCols = taosArrayGetSize(block->pDataBlock);
  udfBlock->udfCols = taosMemoryCalloc(taosArrayGetSize(block->pDataBlock), sizeof(SUdfColumn *));
  for (int32_t i = 0; i < udfBlock->numOfCols; ++i) {
    udfBlock->udfCols[i] = taosMemoryCalloc(1, sizeof(SUdfColumn));
    SColumnInfoData *col = (SColumnInfoData *)taosArrayGet(block->pDataBlock, i);
    SUdfColumn      *udfCol = udfBlock->udfCols[i];
    udfCol->colMeta.type = col->info.type;
    udfCol->colMeta.bytes = col->info.bytes;
    udfCol->colMeta.scale = col->info.scale;
    udfCol->colMeta.precision = col->info.precision;
    udfCol->colData.numOfRows = udfBlock->numOfRows;
    udfCol->hasNull = col->hasNull;
    if (IS_VAR_DATA_TYPE(udfCol->colMeta.type)) {
      udfCol->colData.varLenCol.varOffsetsLen = sizeof(int32_t) * udfBlock->numOfRows;
      udfCol->colData.varLenCol.varOffsets = taosMemoryMalloc(udfCol->colData.varLenCol.varOffsetsLen);
      memcpy(udfCol->colData.varLenCol.varOffsets, col->varmeta.offset, udfCol->colData.varLenCol.varOffsetsLen);
      udfCol->colData.varLenCol.payloadLen = colDataGetLength(col, udfBlock->numOfRows);
      udfCol->colData.varLenCol.payload = taosMemoryMalloc(udfCol->colData.varLenCol.payloadLen);
      if (col->reassigned) {
        for (int32_t row = 0; row < udfCol->colData.numOfRows; ++row) {
          char* pColData = col->pData + col->varmeta.offset[row];
          int32_t colSize = 0;
          if (col->info.type == TSDB_DATA_TYPE_JSON) {
            colSize = getJsonValueLen(pColData);
          } else {
            colSize = varDataTLen(pColData);
          }
          memcpy(udfCol->colData.varLenCol.payload, pColData, colSize);
          udfCol->colData.varLenCol.payload += colSize;
        }
      } else {
        memcpy(udfCol->colData.varLenCol.payload, col->pData, udfCol->colData.varLenCol.payloadLen);
      }
    } else {
      udfCol->colData.fixLenCol.nullBitmapLen = BitmapLen(udfCol->colData.numOfRows);
      int32_t bitmapLen = udfCol->colData.fixLenCol.nullBitmapLen;
      udfCol->colData.fixLenCol.nullBitmap = taosMemoryMalloc(udfCol->colData.fixLenCol.nullBitmapLen);
      char *bitmap = udfCol->colData.fixLenCol.nullBitmap;
      memcpy(bitmap, col->nullbitmap, bitmapLen);
      udfCol->colData.fixLenCol.dataLen = colDataGetLength(col, udfBlock->numOfRows);
      int32_t dataLen = udfCol->colData.fixLenCol.dataLen;
      udfCol->colData.fixLenCol.data = taosMemoryMalloc(udfCol->colData.fixLenCol.dataLen);
      char *data = udfCol->colData.fixLenCol.data;
      memcpy(data, col->pData, dataLen);
    }
  }
  return 0;
}

int32_t convertUdfColumnToDataBlock(SUdfColumn *udfCol, SSDataBlock *block) {
  SUdfColumnMeta* meta = &udfCol->colMeta;

  SColumnInfoData colInfoData = createColumnInfoData(meta->type, meta->bytes, 1);
  blockDataAppendColInfo(block, &colInfoData);
  blockDataEnsureCapacity(block, udfCol->colData.numOfRows);

  SColumnInfoData *col = bdGetColumnInfoData(block, 0);
  for (int i = 0; i < udfCol->colData.numOfRows; ++i) {
    if (udfColDataIsNull(udfCol, i)) {
      colDataSetNULL(col, i);
    } else {
      char* data = udfColDataGetData(udfCol, i);
      colDataSetVal(col, i, data, false);
    }
  }
  block->info.rows = udfCol->colData.numOfRows;
  return 0;
}

int32_t convertUdfColumnToDataBlock2(SUdfColumn *udfCol, SSDataBlock *block) {
  block->info.rows = udfCol->colData.numOfRows;
  block->info.hasVarCol = IS_VAR_DATA_TYPE(udfCol->colMeta.type);

  block->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
  taosArrayPush(block->pDataBlock, &(SColumnInfoData){0});
  SColumnInfoData *col = taosArrayGet(block->pDataBlock, 0);
  SUdfColumnMeta  *meta = &udfCol->colMeta;
  col->info.precision = meta->precision;
  col->info.bytes = meta->bytes;
  col->info.scale = meta->scale;
  col->info.type = meta->type;
  col->hasNull = udfCol->hasNull;
  SUdfColumnData *data = &udfCol->colData;

  if (!IS_VAR_DATA_TYPE(meta->type)) {
    col->nullbitmap = taosMemoryMalloc(data->fixLenCol.nullBitmapLen);
    memcpy(col->nullbitmap, data->fixLenCol.nullBitmap, data->fixLenCol.nullBitmapLen);
    col->pData = taosMemoryMalloc(data->fixLenCol.dataLen);
    memcpy(col->pData, data->fixLenCol.data, data->fixLenCol.dataLen);
  } else {
    col->varmeta.offset = taosMemoryMalloc(data->varLenCol.varOffsetsLen);
    memcpy(col->varmeta.offset, data->varLenCol.varOffsets, data->varLenCol.varOffsetsLen);
    col->pData = taosMemoryMalloc(data->varLenCol.payloadLen);
    memcpy(col->pData, data->varLenCol.payload, data->varLenCol.payloadLen);
  }
  return 0;
}

int32_t convertScalarParamToDataBlock(SScalarParam *input, int32_t numOfCols, SSDataBlock *output) {
  int32_t numOfRows = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    numOfRows = (input[i].numOfRows > numOfRows) ? input[i].numOfRows : numOfRows;
  }

  // create the basic block info structure
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pInfo = input[i].columnData;
    SColumnInfoData d = {0};
    d.info = pInfo->info;

    blockDataAppendColInfo(output, &d);
  }

  blockDataEnsureCapacity(output, numOfRows);

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDest = taosArrayGet(output->pDataBlock, i);

    SColumnInfoData* pColInfoData = input[i].columnData;
    colDataAssign(pDest, pColInfoData, input[i].numOfRows, &output->info);

    if (input[i].numOfRows < numOfRows) {
      int32_t startRow = input[i].numOfRows;
      int expandRows = numOfRows - startRow;
      bool isNull = colDataIsNull_s(pColInfoData, (input+i)->numOfRows - 1);
      if (isNull) {
        colDataSetNNULL(pDest, startRow, expandRows);
      } else {
        char* src = colDataGetData(pColInfoData, (input + i)->numOfRows - 1);
        for (int j = 0; j < expandRows; ++j) {
          colDataSetVal(pDest, startRow+j, src, false);
        }
        //colDataSetNItems(pColInfoData, startRow, data, expandRows);
      }
    }
  }

  output->info.rows = numOfRows;

  return 0;
}

int32_t convertDataBlockToScalarParm(SSDataBlock *input, SScalarParam *output) {
  if (taosArrayGetSize(input->pDataBlock) != 1) {
    fnError("scalar function only support one column");
    return -1;
  }
  output->numOfRows = input->info.rows;

  output->columnData = taosMemoryMalloc(sizeof(SColumnInfoData));
  memcpy(output->columnData, taosArrayGet(input->pDataBlock, 0), sizeof(SColumnInfoData));
  output->colAlloced = true;

  return 0;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
// memory layout |---SUdfAggRes----|-----final result-----|---inter result----|
typedef struct SUdfAggRes {
  int8_t finalResNum;
  int8_t interResNum;
  int32_t interResBufLen;
  char  *finalResBuf;
  char  *interResBuf;
} SUdfAggRes;

void    onUdfcPipeClose(uv_handle_t *handle);
int32_t udfcGetUdfTaskResultFromUvTask(SClientUdfTask *task, SClientUvTaskNode *uvTask);
void    udfcAllocateBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf);
bool    isUdfcUvMsgComplete(SClientConnBuf *connBuf);
void    udfcUvHandleRsp(SClientUvConn *conn);
void    udfcUvHandleError(SClientUvConn *conn);
void    onUdfcPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf);
void    onUdfcPipeWrite(uv_write_t *write, int status);
void    onUdfcPipeConnect(uv_connect_t *connect, int status);
int32_t udfcInitializeUvTask(SClientUdfTask *task, int8_t uvTaskType, SClientUvTaskNode *uvTask);
int32_t udfcQueueUvTask(SClientUvTaskNode *uvTask);
int32_t udfcStartUvTask(SClientUvTaskNode *uvTask);
void    udfcAsyncTaskCb(uv_async_t *async);
void    cleanUpUvTasks(SUdfcProxy *udfc);
void    udfStopAsyncCb(uv_async_t *async);
void    constructUdfService(void *argsThread);
int32_t udfcRunUdfUvTask(SClientUdfTask *task, int8_t uvTaskType);
int32_t doSetupUdf(char udfName[], UdfcFuncHandle *funcHandle);
int     compareUdfcFuncSub(const void *elem1, const void *elem2);
int32_t doTeardownUdf(UdfcFuncHandle handle);

int32_t callUdf(UdfcFuncHandle handle, int8_t callType, SSDataBlock *input, SUdfInterBuf *state, SUdfInterBuf *state2,
                SSDataBlock *output, SUdfInterBuf *newState);
int32_t doCallUdfAggInit(UdfcFuncHandle handle, SUdfInterBuf *interBuf);
int32_t doCallUdfAggProcess(UdfcFuncHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState);
int32_t doCallUdfAggMerge(UdfcFuncHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2,
                          SUdfInterBuf *resultBuf);
int32_t doCallUdfAggFinalize(UdfcFuncHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData);
int32_t doCallUdfScalarFunc(UdfcFuncHandle handle, SScalarParam *input, int32_t numOfCols, SScalarParam *output);
int32_t callUdfScalarFunc(char *udfName, SScalarParam *input, int32_t numOfCols, SScalarParam *output);

int32_t udfcOpen();
int32_t udfcClose();

int32_t acquireUdfFuncHandle(char *udfName, UdfcFuncHandle *pHandle);
void    releaseUdfFuncHandle(char *udfName, UdfcFuncHandle handle);
int32_t cleanUpUdfs();

bool    udfAggGetEnv(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv);
bool    udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo *pResultCellInfo);
int32_t udfAggProcess(struct SqlFunctionCtx *pCtx);
int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock *pBlock);

void cleanupNotExpiredUdfs();
void cleanupExpiredUdfs();
int compareUdfcFuncSub(const void *elem1, const void *elem2) {
  SUdfcFuncStub *stub1 = (SUdfcFuncStub *)elem1;
  SUdfcFuncStub *stub2 = (SUdfcFuncStub *)elem2;
  return strcmp(stub1->udfName, stub2->udfName);
}

int32_t acquireUdfFuncHandle(char *udfName, UdfcFuncHandle *pHandle) {
  int32_t code = 0;
  uv_mutex_lock(&gUdfcProxy.udfStubsMutex);
  SUdfcFuncStub key = {0};
  strncpy(key.udfName, udfName, TSDB_FUNC_NAME_LEN);
  int32_t stubIndex = taosArraySearchIdx(gUdfcProxy.udfStubs, &key, compareUdfcFuncSub, TD_EQ);
  if (stubIndex != -1) {
    SUdfcFuncStub *foundStub = taosArrayGet(gUdfcProxy.udfStubs, stubIndex);
    UdfcFuncHandle handle = foundStub->handle;
    int64_t currUs = taosGetTimestampUs();
    bool expired = (currUs - foundStub->createTime) >= 10 * 1000 * 1000;
    if (!expired) {
      if (handle != NULL && ((SUdfcUvSession *)handle)->udfUvPipe != NULL) {
        *pHandle = foundStub->handle;
        ++foundStub->refCount;
        uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
        return 0;
      } else {
        fnInfo("udf invalid handle for %s, refCount: %d, create time: %" PRId64 ". remove it from cache", udfName,
               foundStub->refCount, foundStub->createTime);
        taosArrayRemove(gUdfcProxy.udfStubs, stubIndex);
      }
    } else {
      fnInfo("udf handle expired for %s, will setup udf. move it to expired list", udfName);
      taosArrayRemove(gUdfcProxy.udfStubs, stubIndex);
      taosArrayPush(gUdfcProxy.expiredUdfStubs, foundStub);
      taosArraySort(gUdfcProxy.expiredUdfStubs, compareUdfcFuncSub);
    }
  }
  *pHandle = NULL;
  code = doSetupUdf(udfName, pHandle);
  if (code == TSDB_CODE_SUCCESS) {
    SUdfcFuncStub stub = {0};
    strncpy(stub.udfName, udfName, TSDB_FUNC_NAME_LEN);
    stub.handle = *pHandle;
    ++stub.refCount;
    stub.createTime = taosGetTimestampUs();
    taosArrayPush(gUdfcProxy.udfStubs, &stub);
    taosArraySort(gUdfcProxy.udfStubs, compareUdfcFuncSub);
  } else {
    *pHandle = NULL;
  }

  uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
  return code;
}

void releaseUdfFuncHandle(char *udfName, UdfcFuncHandle handle) {
  uv_mutex_lock(&gUdfcProxy.udfStubsMutex);
  SUdfcFuncStub key = {0};
  strncpy(key.udfName, udfName, TSDB_FUNC_NAME_LEN);
  SUdfcFuncStub *foundStub = taosArraySearch(gUdfcProxy.udfStubs, &key, compareUdfcFuncSub, TD_EQ);
  SUdfcFuncStub *expiredStub = taosArraySearch(gUdfcProxy.expiredUdfStubs, &key, compareUdfcFuncSub, TD_EQ);
  if (!foundStub && !expiredStub) {
    uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
    return;
  }
  if (foundStub != NULL && foundStub->handle == handle && foundStub->refCount > 0) {
    --foundStub->refCount;
  }
  if (expiredStub != NULL && expiredStub->handle == handle && expiredStub->refCount > 0) {
    --expiredStub->refCount;
  }
  uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
}

void cleanupExpiredUdfs() {
  int32_t i = 0;
  SArray *expiredUdfStubs = taosArrayInit(16, sizeof(SUdfcFuncStub));
  while (i < taosArrayGetSize(gUdfcProxy.expiredUdfStubs)) {
    SUdfcFuncStub *stub = taosArrayGet(gUdfcProxy.expiredUdfStubs, i);
    if (stub->refCount == 0) {
      fnInfo("tear down udf. expired. udf name: %s, handle: %p, ref count: %d", stub->udfName, stub->handle, stub->refCount);
      doTeardownUdf(stub->handle);
    } else {
      fnInfo("udf still in use. expired. udf name: %s, ref count: %d, create time: %" PRId64 ", handle: %p", stub->udfName,
             stub->refCount, stub->createTime, stub->handle);
      UdfcFuncHandle handle = stub->handle;
      if (handle != NULL && ((SUdfcUvSession *)handle)->udfUvPipe != NULL) {
        taosArrayPush(expiredUdfStubs, stub);
      } else {
        fnInfo("udf invalid handle for %s, expired. refCount: %d, create time: %" PRId64 ". remove it from cache",
               stub->udfName, stub->refCount, stub->createTime);
      }
    }
    ++i;
  }
  taosArrayDestroy(gUdfcProxy.expiredUdfStubs);
  gUdfcProxy.expiredUdfStubs = expiredUdfStubs;
}

void cleanupNotExpiredUdfs() {
  SArray *udfStubs = taosArrayInit(16, sizeof(SUdfcFuncStub));
  int32_t i = 0;
  while (i < taosArrayGetSize(gUdfcProxy.udfStubs)) {
    SUdfcFuncStub *stub = taosArrayGet(gUdfcProxy.udfStubs, i);
    if (stub->refCount == 0) {
      fnInfo("tear down udf. udf name: %s, handle: %p, ref count: %d", stub->udfName, stub->handle, stub->refCount);
      doTeardownUdf(stub->handle);
    } else {
      fnInfo("udf still in use. udf name: %s, ref count: %d, create time: %" PRId64 ", handle: %p", stub->udfName,
             stub->refCount, stub->createTime, stub->handle);
      UdfcFuncHandle handle = stub->handle;
      if (handle != NULL && ((SUdfcUvSession *)handle)->udfUvPipe != NULL) {
        taosArrayPush(udfStubs, stub);
      } else {
        fnInfo("udf invalid handle for %s, refCount: %d, create time: %" PRId64 ". remove it from cache",
               stub->udfName, stub->refCount, stub->createTime);
      }
    }
    ++i;
  }
  taosArrayDestroy(gUdfcProxy.udfStubs);
  gUdfcProxy.udfStubs = udfStubs;
}

int32_t cleanUpUdfs() {
  int8_t initialized = atomic_load_8(&gUdfcProxy.initialized);
  if (!initialized) {
    return TSDB_CODE_SUCCESS;
  }

  uv_mutex_lock(&gUdfcProxy.udfStubsMutex);
  if ((gUdfcProxy.udfStubs == NULL || taosArrayGetSize(gUdfcProxy.udfStubs) == 0) &&
      (gUdfcProxy.expiredUdfStubs == NULL || taosArrayGetSize(gUdfcProxy.expiredUdfStubs) == 0)) {
    uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
    return TSDB_CODE_SUCCESS;
  }

  cleanupNotExpiredUdfs();
  cleanupExpiredUdfs();

  uv_mutex_unlock(&gUdfcProxy.udfStubsMutex);
  return 0;
}

int32_t callUdfScalarFunc(char *udfName, SScalarParam *input, int32_t numOfCols, SScalarParam *output) {
  UdfcFuncHandle handle = NULL;
  int32_t        code = acquireUdfFuncHandle(udfName, &handle);
  if (code != 0) {
    return code;
  }

  SUdfcUvSession *session = handle;
  code = doCallUdfScalarFunc(handle, input, numOfCols, output);
  if (code != TSDB_CODE_SUCCESS) {
    fnError("udfc scalar function execution failure");
    releaseUdfFuncHandle(udfName, handle);
    return code;
  }

  if (output->columnData == NULL) {
    fnError("udfc scalar function calculate error. no column data");
    code = TSDB_CODE_UDF_INVALID_OUTPUT_TYPE;
  } else {
    if (session->outputType != output->columnData->info.type || session->bytes != output->columnData->info.bytes) {
      fnError("udfc scalar function calculate error. type mismatch. session type: %d(%d), output type: %d(%d)",
              session->outputType, session->bytes, output->columnData->info.type, output->columnData->info.bytes);
      code = TSDB_CODE_UDF_INVALID_OUTPUT_TYPE;
    }
  }
  releaseUdfFuncHandle(udfName, handle);
  return code;
}

bool udfAggGetEnv(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv) {
  if (fmIsScalarFunc(pFunc->funcId)) {
    return false;
  }
  pEnv->calcMemSize = sizeof(SUdfAggRes) + pFunc->node.resType.bytes + pFunc->udfBufSize;
  return true;
}

bool udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo *pResultCellInfo) {
  if (functionSetup(pCtx, pResultCellInfo) != true) {
    return false;
  }
  UdfcFuncHandle handle;
  int32_t        udfCode = 0;
  if ((udfCode = acquireUdfFuncHandle((char *)pCtx->udfName, &handle)) != 0) {
    fnError("udfAggInit error. step doSetupUdf. udf code: %d", udfCode);
    return false;
  }
  SUdfcUvSession *session = (SUdfcUvSession *)handle;
  SUdfAggRes     *udfRes = (SUdfAggRes *)GET_ROWCELL_INTERBUF(pResultCellInfo);
  int32_t         envSize = sizeof(SUdfAggRes) + session->bytes + session->bufSize;
  memset(udfRes, 0, envSize);

  udfRes->finalResBuf = (char *)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char *)udfRes + sizeof(SUdfAggRes) + session->bytes;

  SUdfInterBuf buf = {0};
  if ((udfCode = doCallUdfAggInit(handle, &buf)) != 0) {
    fnError("udfAggInit error. step doCallUdfAggInit. udf code: %d", udfCode);
    releaseUdfFuncHandle(pCtx->udfName, handle);
    return false;
  }
  if (buf.bufLen <= session->bufSize) {
    memcpy(udfRes->interResBuf, buf.buf, buf.bufLen);
    udfRes->interResBufLen = buf.bufLen;
    udfRes->interResNum = buf.numOfResult;
  } else {
    fnError("udfc inter buf size %d is greater than function bufSize %d", buf.bufLen, session->bufSize);
    releaseUdfFuncHandle(pCtx->udfName, handle);
    return false;
  }
  releaseUdfFuncHandle(pCtx->udfName, handle);
  freeUdfInterBuf(&buf);
  return true;
}

int32_t udfAggProcess(struct SqlFunctionCtx *pCtx) {
  int32_t        udfCode = 0;
  UdfcFuncHandle handle = 0;
  if ((udfCode = acquireUdfFuncHandle((char *)pCtx->udfName, &handle)) != 0) {
    fnError("udfAggProcess  error. step acquireUdfFuncHandle. udf code: %d", udfCode);
    return udfCode;
  }

  SUdfcUvSession *session = handle;
  SUdfAggRes     *udfRes = (SUdfAggRes *)GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  udfRes->finalResBuf = (char *)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char *)udfRes + sizeof(SUdfAggRes) + session->bytes;

  SInputColumnInfoData *pInput = &pCtx->input;
  int32_t               numOfCols = pInput->numOfInputCols;
  int32_t               start = pInput->startRowIndex;
  int32_t               numOfRows = pInput->numOfRows;

  SSDataBlock *pTempBlock = createDataBlock();
  pTempBlock->info.rows = pInput->totalRows;
  pTempBlock->info.id.uid = pInput->uid;
  for (int32_t i = 0; i < numOfCols; ++i) {
    blockDataAppendColInfo(pTempBlock, pInput->pData[i]);
  }

  SSDataBlock *inputBlock = blockDataExtractBlock(pTempBlock, start, numOfRows);

  SUdfInterBuf state = {.buf = udfRes->interResBuf, .bufLen = udfRes->interResBufLen, .numOfResult = udfRes->interResNum};
  SUdfInterBuf newState = {0};

  udfCode = doCallUdfAggProcess(session, inputBlock, &state, &newState);
  if (udfCode != 0) {
    fnError("udfAggProcess error. code: %d", udfCode);
    newState.numOfResult = 0;
  } else {
    if (newState.bufLen <= session->bufSize) {
      memcpy(udfRes->interResBuf, newState.buf, newState.bufLen);
      udfRes->interResBufLen = newState.bufLen;
      udfRes->interResNum = newState.numOfResult;
    } else {
      fnError("udfc inter buf size %d is greater than function bufSize %d", newState.bufLen, session->bufSize);
      udfCode = TSDB_CODE_UDF_INVALID_BUFSIZE;
    }
  }

  GET_RES_INFO(pCtx)->numOfRes = udfRes->interResNum;

  blockDataDestroy(inputBlock);

  taosArrayDestroy(pTempBlock->pDataBlock);
  taosMemoryFree(pTempBlock);

  releaseUdfFuncHandle(pCtx->udfName, handle);
  freeUdfInterBuf(&newState);
  return udfCode;
}

int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock *pBlock) {
  int32_t        udfCode = 0;
  UdfcFuncHandle handle = 0;
  if ((udfCode = acquireUdfFuncHandle((char *)pCtx->udfName, &handle)) != 0) {
    fnError("udfAggProcess  error. step acquireUdfFuncHandle. udf code: %d", udfCode);
    return udfCode;
  }

  SUdfcUvSession *session = handle;
  SUdfAggRes     *udfRes = (SUdfAggRes *)GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  udfRes->finalResBuf = (char *)udfRes + sizeof(SUdfAggRes);
  udfRes->interResBuf = (char *)udfRes + sizeof(SUdfAggRes) + session->bytes;

  SUdfInterBuf resultBuf = {0};
  SUdfInterBuf state = {.buf = udfRes->interResBuf, .bufLen = udfRes->interResBufLen, .numOfResult = udfRes->interResNum};
  int32_t      udfCallCode = 0;
  udfCallCode = doCallUdfAggFinalize(session, &state, &resultBuf);
  if (udfCallCode != 0) {
    fnError("udfAggFinalize error. doCallUdfAggFinalize step. udf code:%d", udfCallCode);
    GET_RES_INFO(pCtx)->numOfRes = 0;
  } else {
    if (resultBuf.numOfResult == 0) {
      udfRes->finalResNum = 0;
      GET_RES_INFO(pCtx)->numOfRes = 0;
    } else {
      if (resultBuf.bufLen <= session->bytes) {
        memcpy(udfRes->finalResBuf, resultBuf.buf, resultBuf.bufLen);
        udfRes->finalResNum = resultBuf.numOfResult;
        GET_RES_INFO(pCtx)->numOfRes = udfRes->finalResNum;
      } else {
        fnError("udfc inter buf size %d is greater than function output size %d", resultBuf.bufLen, session->bytes);
        GET_RES_INFO(pCtx)->numOfRes = 0;
        udfCallCode = TSDB_CODE_UDF_INVALID_OUTPUT_TYPE;
      }
    }
  }

  freeUdfInterBuf(&resultBuf);

  int32_t numOfResults = functionFinalizeWithResultBuf(pCtx, pBlock, udfRes->finalResBuf);
  releaseUdfFuncHandle(pCtx->udfName, handle);
  return udfCallCode == 0 ? numOfResults : udfCallCode;
}

void onUdfcPipeClose(uv_handle_t *handle) {
  SClientUvConn *conn = handle->data;
  if (!QUEUE_EMPTY(&conn->taskQueue)) {
    QUEUE             *h = QUEUE_HEAD(&conn->taskQueue);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
    task->errCode = 0;
    QUEUE_REMOVE(&task->procTaskQueue);
    uv_sem_post(&task->taskSem);
  }
  uv_mutex_lock(&gUdfcProxy.udfcUvMutex);
  if (conn->session != NULL) {
    conn->session->udfUvPipe = NULL;
  }
  uv_mutex_unlock(&gUdfcProxy.udfcUvMutex);
  taosMemoryFree(conn->readBuf.buf);
  taosMemoryFree(conn);
  taosMemoryFree((uv_pipe_t *)handle);
}

int32_t udfcGetUdfTaskResultFromUvTask(SClientUdfTask *task, SClientUvTaskNode *uvTask) {
  fnDebug("udfc get uv task result. task: %p, uvTask: %p", task, uvTask);
  if (uvTask->type == UV_TASK_REQ_RSP) {
    if (uvTask->rspBuf.base != NULL) {
      SUdfResponse rsp = {0};
      void        *buf = decodeUdfResponse(uvTask->rspBuf.base, &rsp);
      task->errCode = rsp.code;

      switch (task->type) {
        case UDF_TASK_SETUP: {
          task->_setup.rsp = rsp.setupRsp;
          break;
        }
        case UDF_TASK_CALL: {
          task->_call.rsp = rsp.callRsp;
          break;
        }
        case UDF_TASK_TEARDOWN: {
          task->_teardown.rsp = rsp.teardownRsp;
          break;
        }
        default: {
          break;
        }
      }

      // TODO: the call buffer is setup and freed by udf invocation
      taosMemoryFree(uvTask->rspBuf.base);
    } else {
      task->errCode = uvTask->errCode;
    }
  } else if (uvTask->type == UV_TASK_CONNECT) {
    task->errCode = uvTask->errCode;
  } else if (uvTask->type == UV_TASK_DISCONNECT) {
    task->errCode = uvTask->errCode;
  }
  return 0;
}

void udfcAllocateBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
  SClientUvConn  *conn = handle->data;
  SClientConnBuf *connBuf = &conn->readBuf;

  int32_t msgHeadSize = sizeof(int32_t) + sizeof(int64_t);
  if (connBuf->cap == 0) {
    connBuf->buf = taosMemoryMalloc(msgHeadSize);
    if (connBuf->buf) {
      connBuf->len = 0;
      connBuf->cap = msgHeadSize;
      connBuf->total = -1;

      buf->base = connBuf->buf;
      buf->len = connBuf->cap;
    } else {
      fnError("udfc allocate buffer failure. size: %d", msgHeadSize);
      buf->base = NULL;
      buf->len = 0;
    }
  } else if (connBuf->total == -1 && connBuf->len < msgHeadSize) {
    buf->base = connBuf->buf + connBuf->len;
    buf->len = msgHeadSize - connBuf->len;
  } else {
    connBuf->cap = connBuf->total > connBuf->cap ? connBuf->total : connBuf->cap;
    void *resultBuf = taosMemoryRealloc(connBuf->buf, connBuf->cap);
    if (resultBuf) {
      connBuf->buf = resultBuf;
      buf->base = connBuf->buf + connBuf->len;
      buf->len = connBuf->cap - connBuf->len;
    } else {
      fnError("udfc re-allocate buffer failure. size: %d", connBuf->cap);
      buf->base = NULL;
      buf->len = 0;
    }
  }

  fnDebug("udfc uv alloc buffer: cap - len - total : %d - %d - %d", connBuf->cap, connBuf->len, connBuf->total);
}

bool isUdfcUvMsgComplete(SClientConnBuf *connBuf) {
  if (connBuf->total == -1 && connBuf->len >= sizeof(int32_t)) {
    connBuf->total = *(int32_t *)(connBuf->buf);
  }
  if (connBuf->len == connBuf->cap && connBuf->total == connBuf->cap) {
    fnDebug("udfc complete message is received, now handle it");
    return true;
  }
  return false;
}

void udfcUvHandleRsp(SClientUvConn *conn) {
  SClientConnBuf *connBuf = &conn->readBuf;
  int64_t         seqNum = *(int64_t *)(connBuf->buf + sizeof(int32_t));  // msglen then seqnum

  if (QUEUE_EMPTY(&conn->taskQueue)) {
    fnError("udfc no task waiting on connection. response seqnum:%" PRId64, seqNum);
    return;
  }
  bool               found = false;
  SClientUvTaskNode *taskFound = NULL;
  QUEUE             *h = QUEUE_NEXT(&conn->taskQueue);
  SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);

  while (h != &conn->taskQueue) {
    fnDebug("udfc handle response iterate through queue. uvTask:%" PRId64 "-%p", task->seqNum, task);
    if (task->seqNum == seqNum) {
      if (found == false) {
        found = true;
        taskFound = task;
      } else {
        fnError("udfc more than one task waiting for the same response");
        continue;
      }
    }
    h = QUEUE_NEXT(h);
    task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
  }

  if (taskFound) {
    taskFound->rspBuf = uv_buf_init(connBuf->buf, connBuf->len);
    QUEUE_REMOVE(&taskFound->connTaskQueue);
    QUEUE_REMOVE(&taskFound->procTaskQueue);
    uv_sem_post(&taskFound->taskSem);
  } else {
    fnError("no task is waiting for the response.");
  }
  connBuf->buf = NULL;
  connBuf->total = -1;
  connBuf->len = 0;
  connBuf->cap = 0;
}

void udfcUvHandleError(SClientUvConn *conn) {
  fnDebug("handle error on conn: %p, pipe: %p", conn, conn->pipe);
  while (!QUEUE_EMPTY(&conn->taskQueue)) {
    QUEUE             *h = QUEUE_HEAD(&conn->taskQueue);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, connTaskQueue);
    task->errCode = TSDB_CODE_UDF_PIPE_READ_ERR;
    QUEUE_REMOVE(&task->connTaskQueue);
    QUEUE_REMOVE(&task->procTaskQueue);
    uv_sem_post(&task->taskSem);
  }
  if (!uv_is_closing((uv_handle_t *)conn->pipe)) {
    uv_close((uv_handle_t *)conn->pipe, onUdfcPipeClose);
  }
}

void onUdfcPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  fnDebug("udfc client %p, client read from pipe. nread: %zd", client, nread);
  if (nread == 0) return;

  SClientUvConn  *conn = client->data;
  SClientConnBuf *connBuf = &conn->readBuf;
  if (nread > 0) {
    connBuf->len += nread;
    if (isUdfcUvMsgComplete(connBuf)) {
      udfcUvHandleRsp(conn);
    }
  }
  if (nread < 0) {
    fnError("udfc client pipe %p read error: %zd(%s).", client, nread, uv_strerror(nread));
    if (nread == UV_EOF) {
      fnError("\tudfc client pipe %p closed", client);
    }
    udfcUvHandleError(conn);
  }
}

void onUdfcPipeWrite(uv_write_t *write, int status) {
  SClientUvConn *conn = write->data;
  if (status < 0) {
    fnError("udfc client connection %p write failed. status: %d(%s)", conn, status, uv_strerror(status));
    udfcUvHandleError(conn);
  } else {
    fnDebug("udfc client connection %p write succeed", conn);
  }
  taosMemoryFree(write);
}

void onUdfcPipeConnect(uv_connect_t *connect, int status) {
  SClientUvTaskNode *uvTask = connect->data;
  if (status != 0) {
    fnError("client connect error, task seq: %" PRId64 ", code: %s", uvTask->seqNum, uv_strerror(status));
  }
  uvTask->errCode = status;

  uv_read_start((uv_stream_t *)uvTask->pipe, udfcAllocateBuffer, onUdfcPipeRead);
  taosMemoryFree(connect);
  QUEUE_REMOVE(&uvTask->procTaskQueue);
  uv_sem_post(&uvTask->taskSem);
}

int32_t udfcInitializeUvTask(SClientUdfTask *task, int8_t uvTaskType, SClientUvTaskNode *uvTask) {
  uvTask->type = uvTaskType;
  uvTask->udfc = task->session->udfc;

  if (uvTaskType == UV_TASK_CONNECT) {
  } else if (uvTaskType == UV_TASK_REQ_RSP) {
    uvTask->pipe = task->session->udfUvPipe;
    SUdfRequest request;
    request.type = task->type;
    request.seqNum = atomic_fetch_add_64(&gUdfTaskSeqNum, 1);

    if (task->type == UDF_TASK_SETUP) {
      request.setup = task->_setup.req;
      request.type = UDF_TASK_SETUP;
    } else if (task->type == UDF_TASK_CALL) {
      request.call = task->_call.req;
      request.type = UDF_TASK_CALL;
    } else if (task->type == UDF_TASK_TEARDOWN) {
      request.teardown = task->_teardown.req;
      request.type = UDF_TASK_TEARDOWN;
    } else {
      fnError("udfc create uv task, invalid task type : %d", task->type);
    }
    int32_t bufLen = encodeUdfRequest(NULL, &request);
    request.msgLen = bufLen;
    void *bufBegin = taosMemoryMalloc(bufLen);
    void *buf = bufBegin;
    encodeUdfRequest(&buf, &request);
    uvTask->reqBuf = uv_buf_init(bufBegin, bufLen);
    uvTask->seqNum = request.seqNum;
  } else if (uvTaskType == UV_TASK_DISCONNECT) {
    uvTask->pipe = task->session->udfUvPipe;
  }
  uv_sem_init(&uvTask->taskSem, 0);

  return 0;
}

int32_t udfcQueueUvTask(SClientUvTaskNode *uvTask) {
  fnDebug("queue uv task to event loop, uvTask: %d-%p", uvTask->type, uvTask);
  SUdfcProxy *udfc = uvTask->udfc;
  uv_mutex_lock(&udfc->taskQueueMutex);
  QUEUE_INSERT_TAIL(&udfc->taskQueue, &uvTask->recvTaskQueue);
  uv_mutex_unlock(&udfc->taskQueueMutex);
  uv_async_send(&udfc->loopTaskAync);

  uv_sem_wait(&uvTask->taskSem);
  fnInfo("udfc uvTask finished. uvTask:%" PRId64 "-%d-%p", uvTask->seqNum, uvTask->type, uvTask);
  uv_sem_destroy(&uvTask->taskSem);

  return 0;
}

int32_t udfcStartUvTask(SClientUvTaskNode *uvTask) {
  fnDebug("event loop start uv task. uvTask: %" PRId64 "-%d-%p", uvTask->seqNum, uvTask->type, uvTask);
  int32_t code = 0;

  switch (uvTask->type) {
    case UV_TASK_CONNECT: {
      uv_pipe_t *pipe = taosMemoryMalloc(sizeof(uv_pipe_t));
      uv_pipe_init(&uvTask->udfc->uvLoop, pipe, 0);
      uvTask->pipe = pipe;

      SClientUvConn *conn = taosMemoryCalloc(1, sizeof(SClientUvConn));
      conn->pipe = pipe;
      conn->readBuf.len = 0;
      conn->readBuf.cap = 0;
      conn->readBuf.buf = 0;
      conn->readBuf.total = -1;
      QUEUE_INIT(&conn->taskQueue);

      pipe->data = conn;

      uv_connect_t *connReq = taosMemoryMalloc(sizeof(uv_connect_t));
      connReq->data = uvTask;
      uv_pipe_connect(connReq, pipe, uvTask->udfc->udfdPipeName, onUdfcPipeConnect);
      code = 0;
      break;
    }
    case UV_TASK_REQ_RSP: {
      uv_pipe_t *pipe = uvTask->pipe;
      if (pipe == NULL) {
        code = TSDB_CODE_UDF_PIPE_NOT_EXIST;
      } else {
        uv_write_t *write = taosMemoryMalloc(sizeof(uv_write_t));
        write->data = pipe->data;
        QUEUE *connTaskQueue = &((SClientUvConn *)pipe->data)->taskQueue;
        QUEUE_INSERT_TAIL(connTaskQueue, &uvTask->connTaskQueue);
        int err = uv_write(write, (uv_stream_t *)pipe, &uvTask->reqBuf, 1, onUdfcPipeWrite);
        if (err != 0) {
          taosMemoryFree(write);
          fnError("udfc event loop start req_rsp task uv_write failed. uvtask: %p, code: %s", uvTask, uv_strerror(err));
        }
        code = err;
      }
      break;
    }
    case UV_TASK_DISCONNECT: {
      uv_pipe_t *pipe = uvTask->pipe;
      if (pipe == NULL) {
        code = TSDB_CODE_UDF_PIPE_NOT_EXIST;
      } else {
        SClientUvConn *conn = pipe->data;
        QUEUE_INSERT_TAIL(&conn->taskQueue, &uvTask->connTaskQueue);
        if (!uv_is_closing((uv_handle_t *)uvTask->pipe)) {
          uv_close((uv_handle_t *)uvTask->pipe, onUdfcPipeClose);
        }
        code = 0;
      }
      break;
    }
    default: {
      fnError("udfc event loop unknown task type.") break;
    }
  }

  return code;
}

void udfcAsyncTaskCb(uv_async_t *async) {
  SUdfcProxy *udfc = async->data;
  QUEUE       wq;

  uv_mutex_lock(&udfc->taskQueueMutex);
  QUEUE_MOVE(&udfc->taskQueue, &wq);
  uv_mutex_unlock(&udfc->taskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE *h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    int32_t            code = udfcStartUvTask(task);
    if (code == 0) {
      QUEUE_INSERT_TAIL(&udfc->uvProcTaskQueue, &task->procTaskQueue);
    } else {
      task->errCode = code;
      uv_sem_post(&task->taskSem);
    }
  }
}

void cleanUpUvTasks(SUdfcProxy *udfc) {
  fnDebug("clean up uv tasks") QUEUE wq;

  uv_mutex_lock(&udfc->taskQueueMutex);
  QUEUE_MOVE(&udfc->taskQueue, &wq);
  uv_mutex_unlock(&udfc->taskQueueMutex);

  while (!QUEUE_EMPTY(&wq)) {
    QUEUE *h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, recvTaskQueue);
    if (udfc->udfcState == UDFC_STATE_STOPPING) {
      task->errCode = TSDB_CODE_UDF_STOPPING;
    }
    uv_sem_post(&task->taskSem);
  }

  while (!QUEUE_EMPTY(&udfc->uvProcTaskQueue)) {
    QUEUE *h = QUEUE_HEAD(&udfc->uvProcTaskQueue);
    QUEUE_REMOVE(h);
    SClientUvTaskNode *task = QUEUE_DATA(h, SClientUvTaskNode, procTaskQueue);
    if (udfc->udfcState == UDFC_STATE_STOPPING) {
      task->errCode = TSDB_CODE_UDF_STOPPING;
    }
    uv_sem_post(&task->taskSem);
  }
}

void udfStopAsyncCb(uv_async_t *async) {
  SUdfcProxy *udfc = async->data;
  cleanUpUvTasks(udfc);
  if (udfc->udfcState == UDFC_STATE_STOPPING) {
    uv_stop(&udfc->uvLoop);
  }
}

void constructUdfService(void *argsThread) {
  SUdfcProxy *udfc = (SUdfcProxy *)argsThread;
  uv_loop_init(&udfc->uvLoop);

  uv_async_init(&udfc->uvLoop, &udfc->loopTaskAync, udfcAsyncTaskCb);
  udfc->loopTaskAync.data = udfc;
  uv_async_init(&udfc->uvLoop, &udfc->loopStopAsync, udfStopAsyncCb);
  udfc->loopStopAsync.data = udfc;
  uv_mutex_init(&udfc->taskQueueMutex);
  QUEUE_INIT(&udfc->taskQueue);
  QUEUE_INIT(&udfc->uvProcTaskQueue);
  uv_barrier_wait(&udfc->initBarrier);
  // TODO return value of uv_run
  uv_run(&udfc->uvLoop, UV_RUN_DEFAULT);
  uv_loop_close(&udfc->uvLoop);

  uv_walk(&udfc->uvLoop, udfUdfdCloseWalkCb, NULL);
  uv_run(&udfc->uvLoop, UV_RUN_DEFAULT);
  uv_loop_close(&udfc->uvLoop);
}

int32_t udfcOpen() {
  int8_t old = atomic_val_compare_exchange_8(&gUdfcProxy.initialized, 0, 1);
  if (old == 1) {
    return 0;
  }
  SUdfcProxy *proxy = &gUdfcProxy;
  getUdfdPipeName(proxy->udfdPipeName, sizeof(proxy->udfdPipeName));
  proxy->udfcState = UDFC_STATE_STARTNG;
  uv_barrier_init(&proxy->initBarrier, 2);
  uv_thread_create(&proxy->loopThread, constructUdfService, proxy);
  atomic_store_8(&proxy->udfcState, UDFC_STATE_READY);
  proxy->udfcState = UDFC_STATE_READY;
  uv_barrier_wait(&proxy->initBarrier);
  uv_mutex_init(&proxy->udfStubsMutex);
  proxy->udfStubs = taosArrayInit(8, sizeof(SUdfcFuncStub));
  proxy->expiredUdfStubs = taosArrayInit(8, sizeof(SUdfcFuncStub));
  uv_mutex_init(&proxy->udfcUvMutex);
  fnInfo("udfc initialized") return 0;
}

int32_t udfcClose() {
  int8_t old = atomic_val_compare_exchange_8(&gUdfcProxy.initialized, 1, 0);
  if (old == 0) {
    return 0;
  }

  SUdfcProxy *udfc = &gUdfcProxy;
  udfc->udfcState = UDFC_STATE_STOPPING;
  uv_async_send(&udfc->loopStopAsync);
  uv_thread_join(&udfc->loopThread);
  uv_mutex_destroy(&udfc->taskQueueMutex);
  uv_barrier_destroy(&udfc->initBarrier);
  taosArrayDestroy(udfc->expiredUdfStubs);
  taosArrayDestroy(udfc->udfStubs);
  uv_mutex_destroy(&udfc->udfStubsMutex);
  uv_mutex_destroy(&udfc->udfcUvMutex);
  udfc->udfcState = UDFC_STATE_INITAL;
  fnInfo("udfc is cleaned up");
  return 0;
}

int32_t udfcRunUdfUvTask(SClientUdfTask *task, int8_t uvTaskType) {
  SClientUvTaskNode *uvTask = taosMemoryCalloc(1, sizeof(SClientUvTaskNode));
  fnDebug("udfc client task: %p created uvTask: %p. pipe: %p", task, uvTask, task->session->udfUvPipe);

  udfcInitializeUvTask(task, uvTaskType, uvTask);
  udfcQueueUvTask(uvTask);
  udfcGetUdfTaskResultFromUvTask(task, uvTask);
  if (uvTaskType == UV_TASK_CONNECT) {
    task->session->udfUvPipe = uvTask->pipe;
    SClientUvConn *conn = uvTask->pipe->data;
    conn->session = task->session;
  }
  taosMemoryFree(uvTask->reqBuf.base);
  uvTask->reqBuf.base = NULL;
  taosMemoryFree(uvTask);
  fnDebug("udfc freed uvTask: %p", task);

  uvTask = NULL;
  return task->errCode;
}

int32_t doSetupUdf(char udfName[], UdfcFuncHandle *funcHandle) {
  SClientUdfTask *task = taosMemoryCalloc(1, sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = taosMemoryCalloc(1, sizeof(SUdfcUvSession));
  task->session->udfc = &gUdfcProxy;
  task->type = UDF_TASK_SETUP;

  SUdfSetupRequest *req = &task->_setup.req;
  strncpy(req->udfName, udfName, TSDB_FUNC_NAME_LEN);

  int32_t errCode = udfcRunUdfUvTask(task, UV_TASK_CONNECT);
  if (errCode != 0) {
    fnError("failed to connect to pipe. udfName: %s, pipe: %s", udfName, (&gUdfcProxy)->udfdPipeName);
    taosMemoryFree(task->session);
    taosMemoryFree(task);
    return TSDB_CODE_UDF_PIPE_CONNECT_ERR;
  }

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  SUdfSetupResponse *rsp = &task->_setup.rsp;
  task->session->severHandle = rsp->udfHandle;
  task->session->outputType = rsp->outputType;
  task->session->bytes = rsp->bytes;
  task->session->bufSize = rsp->bufSize;
  strncpy(task->session->udfName, udfName, TSDB_FUNC_NAME_LEN);
  if (task->errCode != 0) {
    fnError("failed to setup udf. udfname: %s, err: %d", udfName, task->errCode)
  } else {
    fnInfo("successfully setup udf func handle. udfName: %s, handle: %p", udfName, task->session);
    *funcHandle = task->session;
  }
  int32_t err = task->errCode;
  taosMemoryFree(task);
  return err;
}

int32_t callUdf(UdfcFuncHandle handle, int8_t callType, SSDataBlock *input, SUdfInterBuf *state, SUdfInterBuf *state2,
                SSDataBlock *output, SUdfInterBuf *newState) {
  fnDebug("udfc call udf. callType: %d, funcHandle: %p", callType, handle);
  SUdfcUvSession *session = (SUdfcUvSession *)handle;
  if (session->udfUvPipe == NULL) {
    fnError("No pipe to udfd");
    return TSDB_CODE_UDF_PIPE_NOT_EXIST;
  }
  SClientUdfTask *task = taosMemoryCalloc(1, sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = (SUdfcUvSession *)handle;
  task->type = UDF_TASK_CALL;

  SUdfCallRequest *req = &task->_call.req;
  req->udfHandle = task->session->severHandle;
  req->callType = callType;

  switch (callType) {
    case TSDB_UDF_CALL_AGG_INIT: {
      req->initFirst = 1;
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      req->block = *input;
      req->interBuf = *state;
      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      req->interBuf = *state;
      req->interBuf2 = *state2;
      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      req->interBuf = *state;
      break;
    }
    case TSDB_UDF_CALL_SCALA_PROC: {
      req->block = *input;
      break;
    }
  }

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  if (task->errCode != 0) {
    fnError("call udf failure. err: %d", task->errCode);
  } else {
    SUdfCallResponse *rsp = &task->_call.rsp;
    switch (callType) {
      case TSDB_UDF_CALL_AGG_INIT: {
        *newState = rsp->resultBuf;
        break;
      }
      case TSDB_UDF_CALL_AGG_PROC: {
        *newState = rsp->resultBuf;
        break;
      }
      case TSDB_UDF_CALL_AGG_MERGE: {
        *newState = rsp->resultBuf;
        break;
      }
      case TSDB_UDF_CALL_AGG_FIN: {
        *newState = rsp->resultBuf;
        break;
      }
      case TSDB_UDF_CALL_SCALA_PROC: {
        *output = rsp->resultData;
        break;
      }
    }
  };
  int err = task->errCode;
  taosMemoryFree(task);
  return err;
}

int32_t doCallUdfAggInit(UdfcFuncHandle handle, SUdfInterBuf *interBuf) {
  int8_t callType = TSDB_UDF_CALL_AGG_INIT;

  int32_t err = callUdf(handle, callType, NULL, NULL, NULL, NULL, interBuf);

  return err;
}

// input: block, state
// output: interbuf,
int32_t doCallUdfAggProcess(UdfcFuncHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState) {
  int8_t  callType = TSDB_UDF_CALL_AGG_PROC;
  int32_t err = callUdf(handle, callType, block, state, NULL, NULL, newState);
  return err;
}

// input: interbuf1, interbuf2
// output: resultBuf
int32_t doCallUdfAggMerge(UdfcFuncHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2,
                          SUdfInterBuf *resultBuf) {
  int8_t  callType = TSDB_UDF_CALL_AGG_MERGE;
  int32_t err = callUdf(handle, callType, NULL, interBuf1, interBuf2, NULL, resultBuf);
  return err;
}

// input: interBuf
// output: resultData
int32_t doCallUdfAggFinalize(UdfcFuncHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData) {
  int8_t  callType = TSDB_UDF_CALL_AGG_FIN;
  int32_t err = callUdf(handle, callType, NULL, interBuf, NULL, NULL, resultData);
  return err;
}

int32_t doCallUdfScalarFunc(UdfcFuncHandle handle, SScalarParam *input, int32_t numOfCols, SScalarParam *output) {
  int8_t      callType = TSDB_UDF_CALL_SCALA_PROC;
  SSDataBlock inputBlock = {0};
  convertScalarParamToDataBlock(input, numOfCols, &inputBlock);
  SSDataBlock resultBlock = {0};
  int32_t     err = callUdf(handle, callType, &inputBlock, NULL, NULL, &resultBlock, NULL);
  if (err == 0) {
    convertDataBlockToScalarParm(&resultBlock, output);
    taosArrayDestroy(resultBlock.pDataBlock);
  }
  
  blockDataFreeRes(&inputBlock);
  return err;
}

int32_t doTeardownUdf(UdfcFuncHandle handle) {
  SUdfcUvSession *session = (SUdfcUvSession *)handle;

  if (session->udfUvPipe == NULL) {
    fnError("tear down udf. pipe to udfd does not exist. udf name: %s", session->udfName);
    taosMemoryFree(session);
    return TSDB_CODE_UDF_PIPE_NOT_EXIST;
  }

  SClientUdfTask *task = taosMemoryCalloc(1, sizeof(SClientUdfTask));
  task->errCode = 0;
  task->session = session;
  task->type = UDF_TASK_TEARDOWN;

  SUdfTeardownRequest *req = &task->_teardown.req;
  req->udfHandle = task->session->severHandle;

  udfcRunUdfUvTask(task, UV_TASK_REQ_RSP);

  int32_t err = task->errCode;

  udfcRunUdfUvTask(task, UV_TASK_DISCONNECT);

  fnInfo("tear down udf. udf name: %s, udf func handle: %p", session->udfName, handle);
  // TODO: synchronization refactor between libuv event loop and request thread
  uv_mutex_lock(&gUdfcProxy.udfcUvMutex);
  if (session->udfUvPipe != NULL && session->udfUvPipe->data != NULL) {
    SClientUvConn *conn = session->udfUvPipe->data;
    conn->session = NULL;
  }
  uv_mutex_unlock(&gUdfcProxy.udfcUvMutex);
  taosMemoryFree(session);
  taosMemoryFree(task);

  return err;
}
