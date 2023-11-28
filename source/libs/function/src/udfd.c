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
#include "fnLog.h"
#include "thash.h"

#include "tudf.h"
#include "tudfInt.h"
#include "version.h"

#include "tdatablock.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tmsg.h"
#include "trpc.h"
#include "tmisce.h"
#include "tversion.h"
// clang-format on

#define UDFD_MAX_SCRIPT_PLUGINS 64
#define UDFD_MAX_SCRIPT_TYPE    1
#define UDFD_MAX_PLUGIN_FUNCS   9

typedef struct SUdfCPluginCtx {
  uv_lib_t lib;

  TUdfScalarProcFunc scalarProcFunc;

  TUdfAggStartFunc   aggStartFunc;
  TUdfAggProcessFunc aggProcFunc;
  TUdfAggFinishFunc  aggFinishFunc;
  TUdfAggMergeFunc   aggMergeFunc;

  TUdfInitFunc    initFunc;
  TUdfDestroyFunc destroyFunc;
} SUdfCPluginCtx;

int32_t udfdCPluginOpen(SScriptUdfEnvItem *items, int numItems) { return 0; }

int32_t udfdCPluginClose() { return 0; }

const char *udfdCPluginUdfInitLoadInitDestoryFuncs(SUdfCPluginCtx *udfCtx, const char *udfName) {
  char  initFuncName[TSDB_FUNC_NAME_LEN + 6] = {0};
  char *initSuffix = "_init";
  snprintf(initFuncName, sizeof(initFuncName), "%s%s", udfName, initSuffix);
  uv_dlsym(&udfCtx->lib, initFuncName, (void **)(&udfCtx->initFunc));

  char  destroyFuncName[TSDB_FUNC_NAME_LEN + 9] = {0};
  char *destroySuffix = "_destroy";
  snprintf(destroyFuncName, sizeof(destroyFuncName), "%s%s", udfName, destroySuffix);
  uv_dlsym(&udfCtx->lib, destroyFuncName, (void **)(&udfCtx->destroyFunc));
  return udfName;
}

void udfdCPluginUdfInitLoadAggFuncs(SUdfCPluginCtx *udfCtx, const char *udfName) {
  char processFuncName[TSDB_FUNC_NAME_LEN] = {0};
  snprintf(processFuncName, sizeof(processFuncName), "%s", udfName);
  uv_dlsym(&udfCtx->lib, processFuncName, (void **)(&udfCtx->aggProcFunc));

  char  startFuncName[TSDB_FUNC_NAME_LEN + 7] = {0};
  char *startSuffix = "_start";
  snprintf(startFuncName, sizeof(startFuncName), "%s%s", processFuncName, startSuffix);
  uv_dlsym(&udfCtx->lib, startFuncName, (void **)(&udfCtx->aggStartFunc));

  char  finishFuncName[TSDB_FUNC_NAME_LEN + 8] = {0};
  char *finishSuffix = "_finish";
  snprintf(finishFuncName, sizeof(finishFuncName), "%s%s", processFuncName, finishSuffix);
  uv_dlsym(&udfCtx->lib, finishFuncName, (void **)(&udfCtx->aggFinishFunc));

  char  mergeFuncName[TSDB_FUNC_NAME_LEN + 7] = {0};
  char *mergeSuffix = "_merge";
  snprintf(mergeFuncName, sizeof(mergeFuncName), "%s%s", processFuncName, mergeSuffix);
  uv_dlsym(&udfCtx->lib, mergeFuncName, (void **)(&udfCtx->aggMergeFunc));
}

int32_t udfdCPluginUdfInit(SScriptUdfInfo *udf, void **pUdfCtx) {
  int32_t         err = 0;
  SUdfCPluginCtx *udfCtx = taosMemoryCalloc(1, sizeof(SUdfCPluginCtx));
  err = uv_dlopen(udf->path, &udfCtx->lib);
  if (err != 0) {
    fnError("can not load library %s. error: %s", udf->path, uv_strerror(err));
    taosMemoryFree(udfCtx);
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
  }
  const char *udfName = udf->name;

  udfdCPluginUdfInitLoadInitDestoryFuncs(udfCtx, udfName);

  if (udf->funcType == UDF_FUNC_TYPE_SCALAR) {
    char processFuncName[TSDB_FUNC_NAME_LEN] = {0};
    snprintf(processFuncName, sizeof(processFuncName), "%s", udfName);
    uv_dlsym(&udfCtx->lib, processFuncName, (void **)(&udfCtx->scalarProcFunc));
  } else if (udf->funcType == UDF_FUNC_TYPE_AGG) {
    udfdCPluginUdfInitLoadAggFuncs(udfCtx, udfName);
  }

  int32_t code = 0;
  if (udfCtx->initFunc) {
    code = (udfCtx->initFunc)();
    if (code != 0) {
      uv_dlclose(&udfCtx->lib);
      taosMemoryFree(udfCtx);
      return code;
    }
  }
  *pUdfCtx = udfCtx;
  return 0;
}

int32_t udfdCPluginUdfDestroy(void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  int32_t         code = 0;
  if (ctx->destroyFunc) {
    code = (ctx->destroyFunc)();
  }
  uv_dlclose(&ctx->lib);
  taosMemoryFree(ctx);
  return code;
}

int32_t udfdCPluginUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  if (ctx->scalarProcFunc) {
    return ctx->scalarProcFunc(block, resultCol);
  } else {
    fnError("udfd c plugin scalar proc not implemented");
    return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  }
}

int32_t udfdCPluginUdfAggStart(SUdfInterBuf *buf, void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  if (ctx->aggStartFunc) {
    return ctx->aggStartFunc(buf);
  } else {
    fnError("udfd c plugin aggregation start not implemented");
    return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  }
  return 0;
}

int32_t udfdCPluginUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  if (ctx->aggProcFunc) {
    return ctx->aggProcFunc(block, interBuf, newInterBuf);
  } else {
    fnError("udfd c plugin aggregation process not implemented");
    return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  }
}

int32_t udfdCPluginUdfAggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf,
                               void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  if (ctx->aggMergeFunc) {
    return ctx->aggMergeFunc(inputBuf1, inputBuf2, outputBuf);
  } else {
    fnError("udfd c plugin aggregation merge not implemented");
    return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  }
}

int32_t udfdCPluginUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx) {
  SUdfCPluginCtx *ctx = udfCtx;
  if (ctx->aggFinishFunc) {
    return ctx->aggFinishFunc(buf, resultData);
  } else {
    fnError("udfd c plugin aggregation finish not implemented");
    return TSDB_CODE_UDF_FUNC_EXEC_FAILURE;
  }
  return 0;
}

// for c, the function pointer are filled directly and libloaded = true;
// for others, dlopen/dlsym to find function pointers
typedef struct SUdfScriptPlugin {
  int8_t scriptType;

  char     libPath[PATH_MAX];
  bool     libLoaded;
  uv_lib_t lib;

  TScriptUdfScalarProcFunc udfScalarProcFunc;
  TScriptUdfAggStartFunc   udfAggStartFunc;
  TScriptUdfAggProcessFunc udfAggProcFunc;
  TScriptUdfAggMergeFunc   udfAggMergeFunc;
  TScriptUdfAggFinishFunc  udfAggFinishFunc;

  TScriptUdfInitFunc    udfInitFunc;
  TScriptUdfDestoryFunc udfDestroyFunc;

  TScriptOpenFunc  openFunc;
  TScriptCloseFunc closeFunc;
} SUdfScriptPlugin;

typedef struct SUdfdContext {
  uv_loop_t  *loop;
  uv_pipe_t   ctrlPipe;
  uv_signal_t intrSignal;
  char        listenPipeName[PATH_MAX + UDF_LISTEN_PIPE_NAME_LEN + 2];
  uv_pipe_t   listeningPipe;

  void     *clientRpc;
  SCorEpSet mgmtEp;

  uv_mutex_t udfsMutex;
  SHashObj  *udfsHash;

  uv_mutex_t        scriptPluginsMutex;
  SUdfScriptPlugin *scriptPlugins[UDFD_MAX_SCRIPT_PLUGINS];

  SArray *residentFuncs;

  char udfDataDir[PATH_MAX];
  bool printVersion;
} SUdfdContext;

SUdfdContext global;

struct SUdfdUvConn;
struct SUvUdfWork;

typedef struct SUdfdUvConn {
  uv_stream_t *client;
  char        *inputBuf;
  int32_t      inputLen;
  int32_t      inputCap;
  int32_t      inputTotal;

  struct SUvUdfWork *pWorkList;  // head of work list
} SUdfdUvConn;

typedef struct SUvUdfWork {
  SUdfdUvConn *conn;
  uv_buf_t     input;
  uv_buf_t     output;

  struct SUvUdfWork *pWorkNext;
} SUvUdfWork;

typedef enum { UDF_STATE_INIT = 0, UDF_STATE_LOADING, UDF_STATE_READY } EUdfState;

typedef struct SUdf {
  char    name[TSDB_FUNC_NAME_LEN + 1];
  int32_t version;
  int64_t createdTime;

  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;

  char path[PATH_MAX];

  int32_t    refCount;
  EUdfState  state;
  uv_mutex_t lock;
  uv_cond_t  condReady;
  bool       resident;

  SUdfScriptPlugin *scriptPlugin;
  void             *scriptUdfCtx;

  int64_t lastFetchTime;  // last fetch time in milliseconds
  bool    expired;
} SUdf;

typedef struct SUdfcFuncHandle {
  SUdf *udf;
} SUdfcFuncHandle;

typedef enum EUdfdRpcReqRspType {
  UDFD_RPC_MNODE_CONNECT = 0,
  UDFD_RPC_RETRIVE_FUNC,
} EUdfdRpcReqRspType;

typedef struct SUdfdRpcSendRecvInfo {
  EUdfdRpcReqRspType rpcType;
  int32_t            code;
  void              *param;
  uv_sem_t           resultSem;
} SUdfdRpcSendRecvInfo;

static void    udfdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t udfdFillUdfInfoFromMNode(void *clientRpc, char *udfName, SUdf *udf);
static int32_t udfdConnectToMnode();
static bool    udfdRpcRfp(int32_t code, tmsg_t msgType);
static int     initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static int32_t udfdOpenClientRpc();
static int32_t udfdCloseClientRpc();

static void udfdProcessSetupRequest(SUvUdfWork *uvUdf, SUdfRequest *request);
static void udfdProcessCallRequest(SUvUdfWork *uvUdf, SUdfRequest *request);
static void udfdProcessTeardownRequest(SUvUdfWork *uvUdf, SUdfRequest *request);
static void udfdProcessRequest(uv_work_t *req);
static void udfdOnWrite(uv_write_t *req, int status);
static void udfdSendResponse(uv_work_t *work, int status);
static void udfdAllocBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf);
static bool isUdfdUvMsgComplete(SUdfdUvConn *pipe);
static void udfdHandleRequest(SUdfdUvConn *conn);
static void udfdPipeCloseCb(uv_handle_t *pipe);
static void udfdUvHandleError(SUdfdUvConn *conn) { uv_close((uv_handle_t *)conn->client, udfdPipeCloseCb); }
static void udfdPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf);
static void udfdOnNewConnection(uv_stream_t *server, int status);

static void    udfdIntrSignalHandler(uv_signal_t *handle, int signum);
static int32_t removeListeningPipe();

static void    udfdPrintVersion();
static int32_t udfdParseArgs(int32_t argc, char *argv[]);
static int32_t udfdInitLog();

static void    udfdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void    udfdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf);
static int32_t udfdUvInit();
static void    udfdCloseWalkCb(uv_handle_t *handle, void *arg);
static int32_t udfdRun();
static void    udfdConnectMnodeThreadFunc(void *args);

SUdf *udfdNewUdf(const char *udfName);
void  udfdGetFuncBodyPath(const SUdf *udf, char *path);

void udfdInitializeCPlugin(SUdfScriptPlugin *plugin) {
  plugin->scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
  plugin->openFunc = udfdCPluginOpen;
  plugin->closeFunc = udfdCPluginClose;
  plugin->udfInitFunc = udfdCPluginUdfInit;
  plugin->udfDestroyFunc = udfdCPluginUdfDestroy;
  plugin->udfScalarProcFunc = udfdCPluginUdfScalarProc;
  plugin->udfAggStartFunc = udfdCPluginUdfAggStart;
  plugin->udfAggProcFunc = udfdCPluginUdfAggProc;
  plugin->udfAggMergeFunc = udfdCPluginUdfAggMerge;
  plugin->udfAggFinishFunc = udfdCPluginUdfAggFinish;

  SScriptUdfEnvItem items[1] = {{"LD_LIBRARY_PATH", tsUdfdLdLibPath}};
  plugin->openFunc(items, 1);
  return;
}

int32_t udfdLoadSharedLib(char *libPath, uv_lib_t *pLib, const char *funcName[], void **func[], int numOfFuncs) {
  int err = uv_dlopen(libPath, pLib);
  if (err != 0) {
    fnError("can not load library %s. error: %s", libPath, uv_strerror(err));
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
  }

  for (int i = 0; i < numOfFuncs; ++i) {
    err = uv_dlsym(pLib, funcName[i], func[i]);
    if (err != 0) {
      fnError("load library function failed. lib %s function %s", libPath, funcName[i]);
    }
  }
  return 0;
}

int32_t udfdInitializePythonPlugin(SUdfScriptPlugin *plugin) {
  plugin->scriptType = TSDB_FUNC_SCRIPT_PYTHON;
  // todo: windows support
  sprintf(plugin->libPath, "%s", "libtaospyudf.so");
  plugin->libLoaded = false;
  const char *funcName[UDFD_MAX_PLUGIN_FUNCS] = {"pyOpen",         "pyClose",         "pyUdfInit",
                                                 "pyUdfDestroy",   "pyUdfScalarProc", "pyUdfAggStart",
                                                 "pyUdfAggFinish", "pyUdfAggProc",    "pyUdfAggMerge"};
  void      **funcs[UDFD_MAX_PLUGIN_FUNCS] = {
           (void **)&plugin->openFunc,         (void **)&plugin->closeFunc,         (void **)&plugin->udfInitFunc,
           (void **)&plugin->udfDestroyFunc,   (void **)&plugin->udfScalarProcFunc, (void **)&plugin->udfAggStartFunc,
           (void **)&plugin->udfAggFinishFunc, (void **)&plugin->udfAggProcFunc,    (void **)&plugin->udfAggMergeFunc};
  int32_t err = udfdLoadSharedLib(plugin->libPath, &plugin->lib, funcName, funcs, UDFD_MAX_PLUGIN_FUNCS);
  if (err != 0) {
    fnError("can not load python plugin. lib path %s", plugin->libPath);
    return err;
  }

  if (plugin->openFunc) {
    int16_t lenPythonPath =
        strlen(tsUdfdLdLibPath) + strlen(global.udfDataDir) + 1 + 1;  // global.udfDataDir:tsUdfdLdLibPath
    char *pythonPath = taosMemoryMalloc(lenPythonPath);
#ifdef WINDOWS
    snprintf(pythonPath, lenPythonPath, "%s;%s", global.udfDataDir, tsUdfdLdLibPath);
#else
    snprintf(pythonPath, lenPythonPath, "%s:%s", global.udfDataDir, tsUdfdLdLibPath);
#endif
    SScriptUdfEnvItem items[] = {{"PYTHONPATH", pythonPath}, {"LOGDIR", tsLogDir}};
    err = plugin->openFunc(items, 2);
    taosMemoryFree(pythonPath);
  }
  if (err != 0) {
    fnError("udf script python plugin open func failed. error: %d", err);
    uv_dlclose(&plugin->lib);
    return err;
  }
  plugin->libLoaded = true;

  return 0;
}

void udfdDeinitCPlugin(SUdfScriptPlugin *plugin) {
  if (plugin->closeFunc) {
    plugin->closeFunc();
  }
  plugin->openFunc = NULL;
  plugin->closeFunc = NULL;
  plugin->udfInitFunc = NULL;
  plugin->udfDestroyFunc = NULL;
  plugin->udfScalarProcFunc = NULL;
  plugin->udfAggStartFunc = NULL;
  plugin->udfAggProcFunc = NULL;
  plugin->udfAggMergeFunc = NULL;
  plugin->udfAggFinishFunc = NULL;
  return;
}

void udfdDeinitPythonPlugin(SUdfScriptPlugin *plugin) {
  if (plugin->closeFunc) {
    plugin->closeFunc();
  }
  uv_dlclose(&plugin->lib);
  if (plugin->libLoaded) {
    plugin->libLoaded = false;
  }
  plugin->openFunc = NULL;
  plugin->closeFunc = NULL;
  plugin->udfInitFunc = NULL;
  plugin->udfDestroyFunc = NULL;
  plugin->udfScalarProcFunc = NULL;
  plugin->udfAggStartFunc = NULL;
  plugin->udfAggProcFunc = NULL;
  plugin->udfAggMergeFunc = NULL;
  plugin->udfAggFinishFunc = NULL;
}

int32_t udfdInitScriptPlugin(int8_t scriptType) {
  SUdfScriptPlugin *plugin = taosMemoryCalloc(1, sizeof(SUdfScriptPlugin));

  switch (scriptType) {
    case TSDB_FUNC_SCRIPT_BIN_LIB:
      udfdInitializeCPlugin(plugin);
      break;
    case TSDB_FUNC_SCRIPT_PYTHON: {
      int32_t err = udfdInitializePythonPlugin(plugin);
      if (err != 0) {
        taosMemoryFree(plugin);
        return err;
      }
      break;
    }
    default:
      fnError("udf script type %d not supported", scriptType);
      taosMemoryFree(plugin);
      return TSDB_CODE_UDF_SCRIPT_NOT_SUPPORTED;
  }

  global.scriptPlugins[scriptType] = plugin;
  return TSDB_CODE_SUCCESS;
}

void udfdDeinitScriptPlugins() {
  SUdfScriptPlugin *plugin = NULL;
  plugin = global.scriptPlugins[TSDB_FUNC_SCRIPT_PYTHON];
  if (plugin != NULL) {
    udfdDeinitPythonPlugin(plugin);
    taosMemoryFree(plugin);
  }

  plugin = global.scriptPlugins[TSDB_FUNC_SCRIPT_BIN_LIB];
  if (plugin != NULL) {
    udfdDeinitCPlugin(plugin);
    taosMemoryFree(plugin);
  }
  return;
}

void udfdProcessRequest(uv_work_t *req) {
  SUvUdfWork *uvUdf = (SUvUdfWork *)(req->data);
  SUdfRequest request = {0};
  decodeUdfRequest(uvUdf->input.base, &request);

  switch (request.type) {
    case UDF_TASK_SETUP: {
      udfdProcessSetupRequest(uvUdf, &request);
      break;
    }

    case UDF_TASK_CALL: {
      udfdProcessCallRequest(uvUdf, &request);
      break;
    }
    case UDF_TASK_TEARDOWN: {
      udfdProcessTeardownRequest(uvUdf, &request);
      break;
    }
    default: {
      break;
    }
  }
}

void convertUdf2UdfInfo(SUdf *udf, SScriptUdfInfo *udfInfo) {
  udfInfo->bufSize = udf->bufSize;
  if (udf->funcType == TSDB_FUNC_TYPE_AGGREGATE) {
    udfInfo->funcType = UDF_FUNC_TYPE_AGG;
  } else if (udf->funcType == TSDB_FUNC_TYPE_SCALAR) {
    udfInfo->funcType = UDF_FUNC_TYPE_SCALAR;
  }
  udfInfo->name = udf->name;
  udfInfo->version = udf->version;
  udfInfo->createdTime = udf->createdTime;
  udfInfo->outputLen = udf->outputLen;
  udfInfo->outputType = udf->outputType;
  udfInfo->path = udf->path;
  udfInfo->scriptType = udf->scriptType;
}

int32_t udfdInitUdf(char *udfName, SUdf *udf) {
  int32_t err = 0;
  err = udfdFillUdfInfoFromMNode(global.clientRpc, udfName, udf);
  if (err != 0) {
    fnError("can not retrieve udf from mnode. udf name %s", udfName);
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
  }
  if (udf->scriptType > UDFD_MAX_SCRIPT_TYPE) {
    fnError("udf name %s script type %d not supported", udfName, udf->scriptType);
    return TSDB_CODE_UDF_SCRIPT_NOT_SUPPORTED;
  }

  uv_mutex_lock(&global.scriptPluginsMutex);
  SUdfScriptPlugin *scriptPlugin = global.scriptPlugins[udf->scriptType];
  if (scriptPlugin == NULL) {
    err = udfdInitScriptPlugin(udf->scriptType);
    if (err != 0) {
      uv_mutex_unlock(&global.scriptPluginsMutex);
      return err;
    }
  }
  uv_mutex_unlock(&global.scriptPluginsMutex);
  udf->scriptPlugin = global.scriptPlugins[udf->scriptType];

  SScriptUdfInfo info = {0};
  convertUdf2UdfInfo(udf, &info);
  err = udf->scriptPlugin->udfInitFunc(&info, &udf->scriptUdfCtx);
  if (err != 0) {
    fnError("udf name %s init failed. error %d", udfName, err);
    return err;
  }

  fnInfo("udf init succeeded. name %s type %d context %p", udf->name, udf->scriptType, (void *)udf->scriptUdfCtx);
  return 0;
}

SUdf *udfdNewUdf(const char *udfName) {
  SUdf *udfNew = taosMemoryCalloc(1, sizeof(SUdf));
  udfNew->refCount = 1;
  udfNew->lastFetchTime = taosGetTimestampMs();
  strncpy(udfNew->name, udfName, TSDB_FUNC_NAME_LEN);

  udfNew->state = UDF_STATE_INIT;
  uv_mutex_init(&udfNew->lock);
  uv_cond_init(&udfNew->condReady);

  udfNew->resident = false;
  udfNew->expired = false;
  for (int32_t i = 0; i < taosArrayGetSize(global.residentFuncs); ++i) {
    char *funcName = taosArrayGet(global.residentFuncs, i);
    if (strcmp(udfName, funcName) == 0) {
      udfNew->resident = true;
      break;
    }
  }
  return udfNew;
}

SUdf *udfdGetOrCreateUdf(const char *udfName) {
  uv_mutex_lock(&global.udfsMutex);
  SUdf  **pUdfHash = taosHashGet(global.udfsHash, udfName, strlen(udfName));
  int64_t currTime = taosGetTimestampMs();
  bool    expired = false;
  if (pUdfHash) {
    expired = currTime - (*pUdfHash)->lastFetchTime > 10 * 1000;  // 10s
    if (!expired) {
      ++(*pUdfHash)->refCount;
      SUdf *udf = *pUdfHash;
      uv_mutex_unlock(&global.udfsMutex);
      fnInfo("udfd reuse existing udf. udf  %s udf version %d, udf created time %" PRIx64, udf->name, udf->version,
             udf->createdTime);
      return udf;
    } else {
      (*pUdfHash)->expired = true;
      fnInfo("udfd expired, check for new version. existing udf %s udf version %d, udf created time %" PRIx64,
             (*pUdfHash)->name, (*pUdfHash)->version, (*pUdfHash)->createdTime);
      taosHashRemove(global.udfsHash, udfName, strlen(udfName));
    }
  }

  SUdf *udf = udfdNewUdf(udfName);

  SUdf **pUdf = &udf;
  taosHashPut(global.udfsHash, udfName, strlen(udfName), pUdf, POINTER_BYTES);
  uv_mutex_unlock(&global.udfsMutex);

  return udf;
}

void udfdProcessSetupRequest(SUvUdfWork *uvUdf, SUdfRequest *request) {
  // TODO: tracable id from client. connect, setup, call, teardown
  fnInfo("setup request. seq num: %" PRId64 ", udf name: %s", request->seqNum, request->setup.udfName);

  SUdfSetupRequest *setup = &request->setup;
  int32_t           code = TSDB_CODE_SUCCESS;
  SUdf             *udf = NULL;

  udf = udfdGetOrCreateUdf(setup->udfName);

  uv_mutex_lock(&udf->lock);
  if (udf->state == UDF_STATE_INIT) {
    udf->state = UDF_STATE_LOADING;
    code = udfdInitUdf(setup->udfName, udf);
    if (code == 0) {
      udf->state = UDF_STATE_READY;
    } else {
      udf->state = UDF_STATE_INIT;
    }
    uv_cond_broadcast(&udf->condReady);
    uv_mutex_unlock(&udf->lock);
  } else {
    while (udf->state == UDF_STATE_LOADING) {
      uv_cond_wait(&udf->condReady, &udf->lock);
    }
    uv_mutex_unlock(&udf->lock);
  }
  SUdfcFuncHandle *handle = taosMemoryMalloc(sizeof(SUdfcFuncHandle));
  handle->udf = udf;

  SUdfResponse rsp;
  rsp.seqNum = request->seqNum;
  rsp.type = request->type;
  rsp.code = (code != 0) ? TSDB_CODE_UDF_FUNC_EXEC_FAILURE : 0;
  rsp.setupRsp.udfHandle = (int64_t)(handle);
  rsp.setupRsp.outputType = udf->outputType;
  rsp.setupRsp.bytes = udf->outputLen;
  rsp.setupRsp.bufSize = udf->bufSize;

  int32_t len = encodeUdfResponse(NULL, &rsp);
  rsp.msgLen = len;
  void *bufBegin = taosMemoryMalloc(len);
  void *buf = bufBegin;
  encodeUdfResponse(&buf, &rsp);

  uvUdf->output = uv_buf_init(bufBegin, len);

  taosMemoryFree(uvUdf->input.base);
  return;
}

void udfdProcessCallRequest(SUvUdfWork *uvUdf, SUdfRequest *request) {
  SUdfCallRequest *call = &request->call;
  fnDebug("call request. call type %d, handle: %" PRIx64 ", seq num %" PRId64, call->callType, call->udfHandle,
          request->seqNum);
  SUdfcFuncHandle  *handle = (SUdfcFuncHandle *)(call->udfHandle);
  SUdf             *udf = handle->udf;
  SUdfResponse      response = {0};
  SUdfResponse     *rsp = &response;
  SUdfCallResponse *subRsp = &rsp->callRsp;

  int32_t code = TSDB_CODE_SUCCESS;
  switch (call->callType) {
    case TSDB_UDF_CALL_SCALA_PROC: {
      SUdfColumn output = {0};
      output.colMeta.bytes = udf->outputLen;
      output.colMeta.type = udf->outputType;
      output.colMeta.precision = 0;
      output.colMeta.scale = 0;
      udfColEnsureCapacity(&output, call->block.info.rows);

      SUdfDataBlock input = {0};
      convertDataBlockToUdfDataBlock(&call->block, &input);
      code = udf->scriptPlugin->udfScalarProcFunc(&input, &output, udf->scriptUdfCtx);
      freeUdfDataDataBlock(&input);
      convertUdfColumnToDataBlock(&output, &response.callRsp.resultData);
      freeUdfColumn(&output);
      break;
    }
    case TSDB_UDF_CALL_AGG_INIT: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->scriptPlugin->udfAggStartFunc(&outBuf, udf->scriptUdfCtx);
      subRsp->resultBuf = outBuf;
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      SUdfDataBlock input = {0};
      convertDataBlockToUdfDataBlock(&call->block, &input);
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->scriptPlugin->udfAggProcFunc(&input, &call->interBuf, &outBuf, udf->scriptUdfCtx);
      freeUdfInterBuf(&call->interBuf);
      freeUdfDataDataBlock(&input);
      subRsp->resultBuf = outBuf;

      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->scriptPlugin->udfAggMergeFunc(&call->interBuf, &call->interBuf2, &outBuf, udf->scriptUdfCtx);
      freeUdfInterBuf(&call->interBuf);
      freeUdfInterBuf(&call->interBuf2);
      subRsp->resultBuf = outBuf;

      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->scriptPlugin->udfAggFinishFunc(&call->interBuf, &outBuf, udf->scriptUdfCtx);
      freeUdfInterBuf(&call->interBuf);
      subRsp->resultBuf = outBuf;
      break;
    }
    default:
      break;
  }

  rsp->seqNum = request->seqNum;
  rsp->type = request->type;
  rsp->code = (code != 0) ? TSDB_CODE_UDF_FUNC_EXEC_FAILURE : 0;
  subRsp->callType = call->callType;

  int32_t len = encodeUdfResponse(NULL, rsp);
  rsp->msgLen = len;
  void *bufBegin = taosMemoryMalloc(len);
  void *buf = bufBegin;
  encodeUdfResponse(&buf, rsp);
  uvUdf->output = uv_buf_init(bufBegin, len);

  switch (call->callType) {
    case TSDB_UDF_CALL_SCALA_PROC: {
      blockDataFreeRes(&call->block);
      blockDataFreeRes(&subRsp->resultData);
      break;
    }
    case TSDB_UDF_CALL_AGG_INIT: {
      freeUdfInterBuf(&subRsp->resultBuf);
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      blockDataFreeRes(&call->block);
      freeUdfInterBuf(&subRsp->resultBuf);
      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      freeUdfInterBuf(&subRsp->resultBuf);
      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      freeUdfInterBuf(&subRsp->resultBuf);
      break;
    }
    default:
      break;
  }

  taosMemoryFree(uvUdf->input.base);
  return;
}

void udfdProcessTeardownRequest(SUvUdfWork *uvUdf, SUdfRequest *request) {
  SUdfTeardownRequest *teardown = &request->teardown;
  fnInfo("teardown. seq number: %" PRId64 ", handle:%" PRIx64, request->seqNum, teardown->udfHandle);
  SUdfcFuncHandle *handle = (SUdfcFuncHandle *)(teardown->udfHandle);
  SUdf            *udf = handle->udf;
  bool             unloadUdf = false;
  int32_t          code = TSDB_CODE_SUCCESS;

  uv_mutex_lock(&global.udfsMutex);
  udf->refCount--;
  if (udf->refCount == 0 && (!udf->resident || udf->expired)) {
    unloadUdf = true;
    taosHashRemove(global.udfsHash, udf->name, strlen(udf->name));
  }
  uv_mutex_unlock(&global.udfsMutex);
  if (unloadUdf) {
    fnInfo("udf teardown. udf name: %s type %d: context %p", udf->name, udf->scriptType, (void *)(udf->scriptUdfCtx));
    uv_cond_destroy(&udf->condReady);
    uv_mutex_destroy(&udf->lock);
    code = udf->scriptPlugin->udfDestroyFunc(udf->scriptUdfCtx);
    fnDebug("udfd destroy function returns %d", code);
    taosMemoryFree(udf);
  }
  taosMemoryFree(handle);

  SUdfResponse  response = {0};
  SUdfResponse *rsp = &response;
  rsp->seqNum = request->seqNum;
  rsp->type = request->type;
  rsp->code = code;
  int32_t len = encodeUdfResponse(NULL, rsp);
  rsp->msgLen = len;
  void *bufBegin = taosMemoryMalloc(len);
  void *buf = bufBegin;
  encodeUdfResponse(&buf, rsp);
  uvUdf->output = uv_buf_init(bufBegin, len);

  taosMemoryFree(uvUdf->input.base);
  return;
}

void udfdGetFuncBodyPath(const SUdf *udf, char *path) {
  if (udf->scriptType == TSDB_FUNC_SCRIPT_BIN_LIB) {
#ifdef WINDOWS
    snprintf(path, PATH_MAX, "%s%s_%d_%" PRIx64 ".dll", global.udfDataDir, udf->name, udf->version, udf->createdTime);
#else
    snprintf(path, PATH_MAX, "%s/lib%s_%d_%" PRIx64 ".so", global.udfDataDir, udf->name, udf->version,
             udf->createdTime);
#endif
  } else if (udf->scriptType == TSDB_FUNC_SCRIPT_PYTHON) {
#ifdef WINDOWS
    snprintf(path, PATH_MAX, "%s%s_%d_%" PRIx64 ".py", global.udfDataDir, udf->name, udf->version, udf->createdTime);
#else
    snprintf(path, PATH_MAX, "%s/%s_%d_%" PRIx64 ".py", global.udfDataDir, udf->name, udf->version, udf->createdTime);
#endif
  } else {
#ifdef WINDOWS
    snprintf(path, PATH_MAX, "%s%s_%d_%" PRIx64, global.udfDataDir, udf->name, udf->version, udf->createdTime);
#else
    snprintf(path, PATH_MAX, "%s/lib%s_%d_%" PRIx64, global.udfDataDir, udf->name, udf->version, udf->createdTime);
#endif
  }
}

int32_t udfdSaveFuncBodyToFile(SFuncInfo *pFuncInfo, SUdf *udf) {
  if (!osDataSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    fnError("udfd create shared library failed since %s", terrstr(terrno));
    return terrno;
  }

  char path[PATH_MAX] = {0};
  udfdGetFuncBodyPath(udf, path);
  bool fileExist = !(taosStatFile(path, NULL, NULL, NULL) < 0);
  if (fileExist) {
    strncpy(udf->path, path, PATH_MAX);
    fnInfo("udfd func body file. reuse existing file %s", path);
    return TSDB_CODE_SUCCESS;
  }

  TdFilePtr file = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC);
  if (file == NULL) {
    fnError("udfd write udf shared library: %s failed, error: %d %s", path, errno, strerror(errno));
    return TSDB_CODE_FILE_CORRUPTED;
  }
  int64_t count = taosWriteFile(file, pFuncInfo->pCode, pFuncInfo->codeSize);
  if (count != pFuncInfo->codeSize) {
    fnError("udfd write udf shared library failed");
    return TSDB_CODE_FILE_CORRUPTED;
  }
  taosCloseFile(&file);

  strncpy(udf->path, path, PATH_MAX);
  return TSDB_CODE_SUCCESS;
}

void udfdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SUdfdRpcSendRecvInfo *msgInfo = (SUdfdRpcSendRecvInfo *)pMsg->info.ahandle;

  if (pEpSet) {
    if (!isEpsetEqual(&global.mgmtEp.epSet, pEpSet)) {
      updateEpSet_s(&global.mgmtEp, pEpSet);
    }
  }

  if (pMsg->code != TSDB_CODE_SUCCESS) {
    fnError("udfd rpc error. code: %s", tstrerror(pMsg->code));
    msgInfo->code = pMsg->code;
    goto _return;
  }

  if (msgInfo->rpcType == UDFD_RPC_MNODE_CONNECT) {
    SConnectRsp connectRsp = {0};
    tDeserializeSConnectRsp(pMsg->pCont, pMsg->contLen, &connectRsp);

    int32_t now = taosGetTimestampSec();
    int32_t delta = abs(now - connectRsp.svrTimestamp);
    if (delta > 900) {
      msgInfo->code = TSDB_CODE_TIME_UNSYNCED;
      goto _return;
    }

    if (connectRsp.epSet.numOfEps == 0) {
      msgInfo->code = TSDB_CODE_APP_ERROR;
      goto _return;
    }

    if (connectRsp.dnodeNum > 1 && !isEpsetEqual(&global.mgmtEp.epSet, &connectRsp.epSet)) {
      updateEpSet_s(&global.mgmtEp, &connectRsp.epSet);
    }
    msgInfo->code = 0;
  } else if (msgInfo->rpcType == UDFD_RPC_RETRIVE_FUNC) {
    SRetrieveFuncRsp retrieveRsp = {0};
    tDeserializeSRetrieveFuncRsp(pMsg->pCont, pMsg->contLen, &retrieveRsp);

    SFuncInfo *pFuncInfo = (SFuncInfo *)taosArrayGet(retrieveRsp.pFuncInfos, 0);
    SUdf      *udf = msgInfo->param;
    udf->funcType = pFuncInfo->funcType;
    udf->scriptType = pFuncInfo->scriptType;
    udf->outputType = pFuncInfo->outputType;
    udf->outputLen = pFuncInfo->outputLen;
    udf->bufSize = pFuncInfo->bufSize;

    SFuncExtraInfo *pFuncExtraInfo = (SFuncExtraInfo *)taosArrayGet(retrieveRsp.pFuncExtraInfos, 0);
    udf->version = pFuncExtraInfo->funcVersion;
    udf->createdTime = pFuncExtraInfo->funcCreatedTime;
    msgInfo->code = udfdSaveFuncBodyToFile(pFuncInfo, udf);
    if (msgInfo->code != 0) {
      udf->lastFetchTime = 0;
    }
    tFreeSFuncInfo(pFuncInfo);
    taosArrayDestroy(retrieveRsp.pFuncInfos);
    taosArrayDestroy(retrieveRsp.pFuncExtraInfos);
  }

_return:
  rpcFreeCont(pMsg->pCont);
  uv_sem_post(&msgInfo->resultSem);
  return;
}

int32_t udfdFillUdfInfoFromMNode(void *clientRpc, char *udfName, SUdf *udf) {
  SRetrieveFuncReq retrieveReq = {0};
  retrieveReq.numOfFuncs = 1;
  retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
  taosArrayPush(retrieveReq.pFuncNames, udfName);

  int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
  taosArrayDestroy(retrieveReq.pFuncNames);

  SUdfdRpcSendRecvInfo *msgInfo = taosMemoryCalloc(1, sizeof(SUdfdRpcSendRecvInfo));
  msgInfo->rpcType = UDFD_RPC_RETRIVE_FUNC;
  msgInfo->param = udf;
  uv_sem_init(&msgInfo->resultSem, 0);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_RETRIEVE_FUNC;
  rpcMsg.info.ahandle = msgInfo;
  rpcSendRequest(clientRpc, &global.mgmtEp.epSet, &rpcMsg, NULL);

  uv_sem_wait(&msgInfo->resultSem);
  uv_sem_destroy(&msgInfo->resultSem);
  int32_t code = msgInfo->code;
  taosMemoryFree(msgInfo);
  return code;
}

static bool udfdRpcRfp(int32_t code, tmsg_t msgType) {
  if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_SYN_NOT_LEADER ||
      code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED || code == TSDB_CODE_SYN_RESTORING ||
      code == TSDB_CODE_MNODE_NOT_FOUND || code == TSDB_CODE_APP_IS_STARTING || code == TSDB_CODE_APP_IS_STOPPING) {
    if (msgType == TDMT_SCH_QUERY || msgType == TDMT_SCH_MERGE_QUERY || msgType == TDMT_SCH_FETCH ||
        msgType == TDMT_SCH_MERGE_FETCH || msgType == TDMT_SCH_TASK_NOTIFY) {
      return false;
    }
    return true;
  } else {
    return false;
  }
}

int initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet) {
  pEpSet->version = 0;

  // init mnode ip set
  SEpSet *mgmtEpSet = &(pEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse = 0;

  if (firstEp && firstEp[0] != 0) {
    if (strlen(firstEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    int32_t code = taosGetFqdnPortFromEp(firstEp, &mgmtEpSet->eps[0]);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return terrno;
    }

    mgmtEpSet->numOfEps++;
  }

  if (secondEp && secondEp[0] != 0) {
    if (strlen(secondEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(secondEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

int32_t udfdOpenClientRpc() {
  SRpcInit rpcInit = {0};
  rpcInit.label = "UDFD";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)udfdProcessRpcRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.parent = &global;
  rpcInit.rfp = udfdRpcRfp;
  rpcInit.compressSize = tsCompressMsgSize;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);
  rpcInit.connLimitNum = connLimitNum;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));
  global.clientRpc = rpcOpen(&rpcInit);
  if (global.clientRpc == NULL) {
    fnError("failed to init dnode rpc client");
    return -1;
  }
  return 0;
}

int32_t udfdCloseClientRpc() {
  fnInfo("udfd begin closing rpc");
  rpcClose(global.clientRpc);
  fnInfo("udfd finish closing rpc");
  return 0;
}

void udfdOnWrite(uv_write_t *req, int status) {
  SUvUdfWork *work = (SUvUdfWork *)req->data;
  if (status < 0) {
    fnError("udfd send response error, length: %zu code: %s", work->output.len, uv_err_name(status));
  }
  // remove work from the connection work list
  if (work->conn != NULL) {
    SUvUdfWork **ppWork;
    for (ppWork = &work->conn->pWorkList; *ppWork && (*ppWork != work); ppWork = &((*ppWork)->pWorkNext)) {
    }
    if (*ppWork == work) {
      *ppWork = work->pWorkNext;
    } else {
      fnError("work not in conn any more");
    }
  }
  taosMemoryFree(work->output.base);
  taosMemoryFree(work);
  taosMemoryFree(req);
}

void udfdSendResponse(uv_work_t *work, int status) {
  SUvUdfWork *udfWork = (SUvUdfWork *)(work->data);

  if (udfWork->conn != NULL) {
    uv_write_t *write_req = taosMemoryMalloc(sizeof(uv_write_t));
    write_req->data = udfWork;
    uv_write(write_req, udfWork->conn->client, &udfWork->output, 1, udfdOnWrite);
  }
  taosMemoryFree(work);
}

void udfdAllocBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
  SUdfdUvConn *ctx = handle->data;
  int32_t      msgHeadSize = sizeof(int32_t) + sizeof(int64_t);
  if (ctx->inputCap == 0) {
    ctx->inputBuf = taosMemoryMalloc(msgHeadSize);
    if (ctx->inputBuf) {
      ctx->inputLen = 0;
      ctx->inputCap = msgHeadSize;
      ctx->inputTotal = -1;

      buf->base = ctx->inputBuf;
      buf->len = ctx->inputCap;
    } else {
      fnError("udfd can not allocate enough memory") buf->base = NULL;
      buf->len = 0;
    }
  } else if (ctx->inputTotal == -1 && ctx->inputLen < msgHeadSize) {
    buf->base = ctx->inputBuf + ctx->inputLen;
    buf->len = msgHeadSize - ctx->inputLen;
  } else {
    ctx->inputCap = ctx->inputTotal > ctx->inputCap ? ctx->inputTotal : ctx->inputCap;
    void *inputBuf = taosMemoryRealloc(ctx->inputBuf, ctx->inputCap);
    if (inputBuf) {
      ctx->inputBuf = inputBuf;
      buf->base = ctx->inputBuf + ctx->inputLen;
      buf->len = ctx->inputCap - ctx->inputLen;
    } else {
      fnError("udfd can not allocate enough memory") buf->base = NULL;
      buf->len = 0;
    }
  }
}

bool isUdfdUvMsgComplete(SUdfdUvConn *pipe) {
  if (pipe->inputTotal == -1 && pipe->inputLen >= sizeof(int32_t)) {
    pipe->inputTotal = *(int32_t *)(pipe->inputBuf);
  }
  if (pipe->inputLen == pipe->inputCap && pipe->inputTotal == pipe->inputCap) {
    fnDebug("receive request complete. length %d", pipe->inputLen);
    return true;
  }
  return false;
}

void udfdHandleRequest(SUdfdUvConn *conn) {
  char   *inputBuf = conn->inputBuf;
  int32_t inputLen = conn->inputLen;

  uv_work_t  *work = taosMemoryMalloc(sizeof(uv_work_t));
  SUvUdfWork *udfWork = taosMemoryMalloc(sizeof(SUvUdfWork));
  udfWork->conn = conn;
  udfWork->pWorkNext = conn->pWorkList;
  conn->pWorkList = udfWork;
  udfWork->input = uv_buf_init(inputBuf, inputLen);
  conn->inputBuf = NULL;
  conn->inputLen = 0;
  conn->inputCap = 0;
  conn->inputTotal = -1;
  work->data = udfWork;
  uv_queue_work(global.loop, work, udfdProcessRequest, udfdSendResponse);
}

void udfdPipeCloseCb(uv_handle_t *pipe) {
  SUdfdUvConn *conn = pipe->data;
  SUvUdfWork  *pWork = conn->pWorkList;
  while (pWork != NULL) {
    pWork->conn = NULL;
    pWork = pWork->pWorkNext;
  }

  taosMemoryFree(conn->client);
  taosMemoryFree(conn->inputBuf);
  taosMemoryFree(conn);
}

void udfdPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  fnDebug("udfd read %zd bytes from client", nread);
  if (nread == 0) return;

  SUdfdUvConn *conn = client->data;

  if (nread > 0) {
    conn->inputLen += nread;
    if (isUdfdUvMsgComplete(conn)) {
      udfdHandleRequest(conn);
    } else {
      // log error or continue;
    }
    return;
  }

  if (nread < 0) {
    if (nread == UV_EOF) {
      fnInfo("udfd pipe read EOF");
    } else {
      fnError("Receive error %s", uv_err_name(nread));
    }
    udfdUvHandleError(conn);
  }
}

void udfdOnNewConnection(uv_stream_t *server, int status) {
  if (status < 0) {
    fnError("udfd new connection error. code: %s", uv_strerror(status));
    return;
  }

  uv_pipe_t *client = (uv_pipe_t *)taosMemoryMalloc(sizeof(uv_pipe_t));
  uv_pipe_init(global.loop, client, 0);
  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    SUdfdUvConn *ctx = taosMemoryMalloc(sizeof(SUdfdUvConn));
    ctx->pWorkList = NULL;
    ctx->client = (uv_stream_t *)client;
    ctx->inputBuf = 0;
    ctx->inputLen = 0;
    ctx->inputCap = 0;
    client->data = ctx;
    ctx->client = (uv_stream_t *)client;
    uv_read_start((uv_stream_t *)client, udfdAllocBuffer, udfdPipeRead);
  } else {
    uv_close((uv_handle_t *)client, NULL);
  }
}

void udfdIntrSignalHandler(uv_signal_t *handle, int signum) {
  fnInfo("udfd signal received: %d\n", signum);
  uv_fs_t req;
  uv_fs_unlink(global.loop, &req, global.listenPipeName, NULL);
  uv_signal_stop(handle);
  uv_stop(global.loop);
}

static int32_t udfdParseArgs(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
    } else {
    }
  }

  return 0;
}

static void udfdPrintVersion() {
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
  printf("buildInfo: %s\n", buildinfo);
}

static int32_t udfdInitLog() {
  char logName[12] = {0};
  snprintf(logName, sizeof(logName), "%slog", "udfd");
  return taosCreateLog(logName, 1, configDir, NULL, NULL, NULL, NULL, 0);
}

void udfdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  buf->base = taosMemoryMalloc(suggested_size);
  buf->len = suggested_size;
}

void udfdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf) {
  if (nread < 0) {
    fnError("udfd ctrl pipe read error. %s", uv_err_name(nread));
    taosMemoryFree(buf->base);
    uv_close((uv_handle_t *)q, NULL);
    uv_stop(global.loop);
    return;
  }
  fnError("udfd ctrl pipe read %zu bytes", nread);
  taosMemoryFree(buf->base);
}

static int32_t removeListeningPipe() {
  uv_fs_t req;
  int     err = uv_fs_unlink(global.loop, &req, global.listenPipeName, NULL);
  uv_fs_req_cleanup(&req);
  return err;
}

static int32_t udfdUvInit() {
  uv_loop_t *loop = taosMemoryMalloc(sizeof(uv_loop_t));
  if (loop) {
    uv_loop_init(loop);
  } else {
    return -1;
  }
  global.loop = loop;

  if (tsStartUdfd) {  // udfd is started by taosd, which shall exit when taosd exit
    uv_pipe_init(global.loop, &global.ctrlPipe, 1);
    uv_pipe_open(&global.ctrlPipe, 0);
    uv_read_start((uv_stream_t *)&global.ctrlPipe, udfdCtrlAllocBufCb, udfdCtrlReadCb);
  }
  getUdfdPipeName(global.listenPipeName, sizeof(global.listenPipeName));

  removeListeningPipe();

  uv_pipe_init(global.loop, &global.listeningPipe, 0);

  uv_signal_init(global.loop, &global.intrSignal);
  uv_signal_start(&global.intrSignal, udfdIntrSignalHandler, SIGINT);

  int r;
  fnInfo("bind to pipe %s", global.listenPipeName);
  if ((r = uv_pipe_bind(&global.listeningPipe, global.listenPipeName))) {
    fnError("Bind error %s", uv_err_name(r));
    removeListeningPipe();
    return -2;
  }
  if ((r = uv_listen((uv_stream_t *)&global.listeningPipe, 128, udfdOnNewConnection))) {
    fnError("Listen error %s", uv_err_name(r));
    removeListeningPipe();
    return -3;
  }
  return 0;
}

static void udfdCloseWalkCb(uv_handle_t *handle, void *arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

static int32_t udfdRun() {
  uv_mutex_init(&global.scriptPluginsMutex);

  global.udfsHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  uv_mutex_init(&global.udfsMutex);

  fnInfo("start udfd event loop");
  uv_run(global.loop, UV_RUN_DEFAULT);
  fnInfo("udfd event loop stopped.");

  uv_loop_close(global.loop);

  uv_walk(global.loop, udfdCloseWalkCb, NULL);
  uv_run(global.loop, UV_RUN_DEFAULT);
  uv_loop_close(global.loop);

  return 0;
}

int32_t udfdInitResidentFuncs() {
  if (strlen(tsUdfdResFuncs) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  global.residentFuncs = taosArrayInit(2, TSDB_FUNC_NAME_LEN);
  char *pSave = tsUdfdResFuncs;
  char *token;
  while ((token = strtok_r(pSave, ",", &pSave)) != NULL) {
    char func[TSDB_FUNC_NAME_LEN + 1] = {0};
    strncpy(func, token, TSDB_FUNC_NAME_LEN);
    fnInfo("udfd add resident function %s", func);
    taosArrayPush(global.residentFuncs, func);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t udfdDeinitResidentFuncs() {
  for (int32_t i = 0; i < taosArrayGetSize(global.residentFuncs); ++i) {
    char  *funcName = taosArrayGet(global.residentFuncs, i);
    SUdf **udfInHash = taosHashGet(global.udfsHash, funcName, strlen(funcName));
    if (udfInHash) {
      SUdf   *udf = *udfInHash;
      int32_t code = udf->scriptPlugin->udfDestroyFunc(udf->scriptUdfCtx);
      fnDebug("udfd destroy function returns %d", code);
      taosHashRemove(global.udfsHash, funcName, strlen(funcName));
      taosMemoryFree(udf);
    }
  }
  taosArrayDestroy(global.residentFuncs);
  return TSDB_CODE_SUCCESS;
}

int32_t udfdCleanup() {
  uv_mutex_destroy(&global.udfsMutex);
  taosHashCleanup(global.udfsHash);
  return 0;
}

int32_t udfdCreateUdfSourceDir() {
  snprintf(global.udfDataDir, PATH_MAX, "%s/.udf", tsDataDir);
  int32_t code = taosMkDir(global.udfDataDir);
  if (code != TSDB_CODE_SUCCESS) {
    snprintf(global.udfDataDir, PATH_MAX, "%s/.udf", tsTempDir);
    code = taosMkDir(global.udfDataDir);
  }
  fnInfo("udfd create udf source directory %s. result: %s", global.udfDataDir, tstrerror(code));

  return code;
}

int32_t udfdDestroyUdfSourceDir() {
  fnInfo("destory udf source directory %s", global.udfDataDir);
  taosRemoveDir(global.udfDataDir);
  return 0;
}

int main(int argc, char *argv[]) {
  if (!taosCheckSystemIsLittleEnd()) {
    printf("failed to start since on non-little-end machines\n");
    return -1;
  }

  if (udfdParseArgs(argc, argv) != 0) {
    printf("failed to start since parse args error\n");
    return -1;
  }

  if (global.printVersion) {
    udfdPrintVersion();
    return 0;
  }

  if (udfdInitLog() != 0) {
    // ignore create log failed, because this error no matter
    printf("failed to start since init log error\n");
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    fnError("failed to start since read config error");
    return -2;
  }

  initEpSetFromCfg(tsFirst, tsSecond, &global.mgmtEp);
  if (udfdOpenClientRpc() != 0) {
    fnError("open rpc connection to mnode failed");
    return -3;
  }

  if (udfdCreateUdfSourceDir() != 0) {
    fnError("create udf source directory failed");
    return -4;
  }

  if (udfdUvInit() != 0) {
    fnError("uv init failure");
    return -5;
  }

  udfdInitResidentFuncs();

  udfdRun();

  removeListeningPipe();
  udfdDestroyUdfSourceDir();
  udfdCloseClientRpc();

  udfdDeinitResidentFuncs();

  udfdDeinitScriptPlugins();

  udfdCleanup();
  return 0;
}
