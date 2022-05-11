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
#include "fnLog.h"
#include "thash.h"

#include "tudf.h"
#include "tudfInt.h"

#include "tdatablock.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tmsg.h"
#include "trpc.h"

typedef struct SUdfdContext {
  uv_loop_t  *loop;
  uv_pipe_t   ctrlPipe;
  uv_signal_t intrSignal;
  char        listenPipeName[PATH_MAX + UDF_LISTEN_PIPE_NAME_LEN + 2];
  uv_pipe_t   listeningPipe;

  void       *clientRpc;
  SCorEpSet  mgmtEp;
  uv_mutex_t udfsMutex;
  SHashObj  *udfsHash;

  bool printVersion;
} SUdfdContext;

SUdfdContext global;

typedef struct SUdfdUvConn {
  uv_stream_t *client;
  char        *inputBuf;
  int32_t      inputLen;
  int32_t      inputCap;
  int32_t      inputTotal;
} SUdfdUvConn;

typedef struct SUvUdfWork {
  uv_stream_t *client;
  uv_buf_t     input;
  uv_buf_t     output;
} SUvUdfWork;

typedef enum { UDF_STATE_INIT = 0, UDF_STATE_LOADING, UDF_STATE_READY, UDF_STATE_UNLOADING } EUdfState;

typedef struct SUdf {
  int32_t    refCount;
  EUdfState  state;
  uv_mutex_t lock;
  uv_cond_t  condReady;

  char   name[TSDB_FUNC_NAME_LEN];
  int8_t funcType;
  int8_t scriptType;
  int8_t outputType;
  int32_t outputLen;
  int32_t bufSize;

  char   path[PATH_MAX];

  uv_lib_t              lib;

  TUdfScalarProcFunc    scalarProcFunc;

  TUdfAggStartFunc      aggStartFunc;
  TUdfAggProcessFunc    aggProcFunc;
  TUdfAggFinishFunc     aggFinishFunc;

  TUdfInitFunc          initFunc;
  TUdfDestroyFunc       destroyFunc;
} SUdf;

// TODO: low priority: change name onxxx to xxxCb, and udfc or udfd as prefix
// TODO: add private udf structure.
typedef struct SUdfcFuncHandle {
  SUdf *udf;
} SUdfcFuncHandle;

int32_t udfdFillUdfInfoFromMNode(void *clientRpc, char *udfName, SUdf *udf);

int32_t udfdLoadUdf(char *udfName, SUdf *udf) {
  strcpy(udf->name, udfName);

  udfdFillUdfInfoFromMNode(global.clientRpc, udf->name, udf);
  //strcpy(udf->path, "/home/slzhou/TDengine/debug/build/lib/libudf1.so");
  int err = uv_dlopen(udf->path, &udf->lib);
  if (err != 0) {
    fnError("can not load library %s. error: %s", udf->path, uv_strerror(err));
    return UDFC_CODE_LOAD_UDF_FAILURE;
  }

  char initFuncName[TSDB_FUNC_NAME_LEN+5] = {0};
  char *initSuffix = "_init";
  strcpy(initFuncName, udfName);
  strncat(initFuncName, initSuffix, strlen(initSuffix));
  uv_dlsym(&udf->lib, initFuncName, (void**)(&udf->initFunc));

  char destroyFuncName[TSDB_FUNC_NAME_LEN+5] = {0};
  char *destroySuffix = "_destroy";
  strcpy(destroyFuncName, udfName);
  strncat(destroyFuncName, destroySuffix, strlen(destroySuffix));
  uv_dlsym(&udf->lib, destroyFuncName, (void**)(&udf->destroyFunc));

  if (udf->funcType == TSDB_FUNC_TYPE_SCALAR) {
    char processFuncName[TSDB_FUNC_NAME_LEN] = {0};
    strcpy(processFuncName, udfName);
    uv_dlsym(&udf->lib, processFuncName, (void **)(&udf->scalarProcFunc));
  } else if (udf->funcType == TSDB_FUNC_TYPE_AGGREGATE) {
    char processFuncName[TSDB_FUNC_NAME_LEN] = {0};
    strcpy(processFuncName, udfName);
    uv_dlsym(&udf->lib, processFuncName, (void **)(&udf->aggProcFunc));
    char  startFuncName[TSDB_FUNC_NAME_LEN + 6] = {0};
    char *startSuffix = "_start";
    strncpy(startFuncName, processFuncName, strlen(processFuncName));
    strncat(startFuncName, startSuffix, strlen(startSuffix));
    uv_dlsym(&udf->lib, startFuncName, (void **)(&udf->aggStartFunc));
    char  finishFuncName[TSDB_FUNC_NAME_LEN + 7] = {0};
    char *finishSuffix = "_finish";
    strncpy(finishFuncName, processFuncName, strlen(processFuncName));
    strncat(finishFuncName, finishSuffix, strlen(finishSuffix));
    uv_dlsym(&udf->lib, finishFuncName, (void **)(&udf->aggFinishFunc));
    //TODO: merge
  }
  return 0;
}

void udfdProcessRequest(uv_work_t *req) {
  SUvUdfWork *uvUdf = (SUvUdfWork *)(req->data);
  SUdfRequest request = {0};
  decodeUdfRequest(uvUdf->input.base, &request);

  switch (request.type) {
    case UDF_TASK_SETUP: {
      // TODO: tracable id from client. connect, setup, call, teardown
      fnInfo("%" PRId64 " setup request. udf name: %s", request.seqNum, request.setup.udfName);
      SUdfSetupRequest *setup = &request.setup;

      SUdf *udf = NULL;
      uv_mutex_lock(&global.udfsMutex);
      SUdf **udfInHash = taosHashGet(global.udfsHash, request.setup.udfName, strlen(request.setup.udfName));
      if (udfInHash) {
        ++(*udfInHash)->refCount;
        udf = *udfInHash;
        uv_mutex_unlock(&global.udfsMutex);
      } else {
        SUdf *udfNew = taosMemoryCalloc(1, sizeof(SUdf));
        udfNew->refCount = 1;
        udfNew->state = UDF_STATE_INIT;

        uv_mutex_init(&udfNew->lock);
        uv_cond_init(&udfNew->condReady);
        udf = udfNew;
        taosHashPut(global.udfsHash, request.setup.udfName, strlen(request.setup.udfName), &udfNew, sizeof(&udfNew));
        uv_mutex_unlock(&global.udfsMutex);
      }

      uv_mutex_lock(&udf->lock);
      if (udf->state == UDF_STATE_INIT) {
        udf->state = UDF_STATE_LOADING;
        udfdLoadUdf(setup->udfName, udf);
        if (udf->initFunc) {
          udf->initFunc();
        }
        udf->state = UDF_STATE_READY;
        uv_cond_broadcast(&udf->condReady);
        uv_mutex_unlock(&udf->lock);
      } else {
        while (udf->state != UDF_STATE_READY) {
          uv_cond_wait(&udf->condReady, &udf->lock);
        }
        uv_mutex_unlock(&udf->lock);
      }
      SUdfcFuncHandle *handle = taosMemoryMalloc(sizeof(SUdfcFuncHandle));
      handle->udf = udf;
      SUdfResponse rsp;
      rsp.seqNum = request.seqNum;
      rsp.type = request.type;
      rsp.code = 0;
      rsp.setupRsp.udfHandle = (int64_t)(handle);
      rsp.setupRsp.outputType = udf->outputType;
      rsp.setupRsp.outputLen = udf->outputLen;
      rsp.setupRsp.bufSize = udf->bufSize;
      int32_t len = encodeUdfResponse(NULL, &rsp);
      rsp.msgLen = len;
      void *bufBegin = taosMemoryMalloc(len);
      void *buf = bufBegin;
      encodeUdfResponse(&buf, &rsp);

      uvUdf->output = uv_buf_init(bufBegin, len);

      taosMemoryFree(uvUdf->input.base);
      break;
    }

    case UDF_TASK_CALL: {
      SUdfCallRequest *call = &request.call;
      fnDebug("%" PRId64 "call request. call type %d, handle: %" PRIx64, request.seqNum, call->callType,
              call->udfHandle);
      SUdfcFuncHandle *handle = (SUdfcFuncHandle *)(call->udfHandle);
      SUdf            *udf = handle->udf;
      SUdfResponse  response = {0};
      SUdfResponse *rsp = &response;
      SUdfCallResponse *subRsp = &rsp->callRsp;

      switch(call->callType) {
        case TSDB_UDF_CALL_SCALA_PROC: {
          SUdfColumn output = {0};

          SUdfDataBlock input = {0};
          convertDataBlockToUdfDataBlock(&call->block, &input);
          udf->scalarProcFunc(&input, &output);

          convertUdfColumnToDataBlock(&output, &response.callRsp.resultData);
          freeUdfColumn(&output);
          break;
        }
        case TSDB_UDF_CALL_AGG_INIT: {
          SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize),
                                 .bufLen= udf->bufSize,
                                 .numOfResult = 0};
          udf->aggStartFunc(&outBuf);
          subRsp->resultBuf = outBuf;
          break;
        }
        case TSDB_UDF_CALL_AGG_PROC: {
          SUdfDataBlock input = {0};
          convertDataBlockToUdfDataBlock(&call->block, &input);
          SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize),
                                 .bufLen= udf->bufSize,
                                 .numOfResult = 0};
          udf->aggProcFunc(&input, &call->interBuf, &outBuf);
          subRsp->resultBuf = outBuf;

          break;
        }
        case TSDB_UDF_CALL_AGG_FIN: {
          SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize),
                                 .bufLen= udf->bufSize,
                                 .numOfResult = 0};
          udf->aggFinishFunc(&call->interBuf, &outBuf);
          subRsp->resultBuf = outBuf;
          break;
        }
        default:
          break;
      }

      rsp->seqNum = request.seqNum;
      rsp->type = request.type;
      rsp->code = 0;
      subRsp->callType = call->callType;

      int32_t len = encodeUdfResponse(NULL, rsp);
      rsp->msgLen = len;
      void *bufBegin = taosMemoryMalloc(len);
      void *buf = bufBegin;
      encodeUdfResponse(&buf, rsp);
      uvUdf->output = uv_buf_init(bufBegin, len);

      taosMemoryFree(uvUdf->input.base);
      break;
    }
    case UDF_TASK_TEARDOWN: {
      SUdfTeardownRequest *teardown = &request.teardown;
      fnInfo("teardown. %" PRId64 "handle:%" PRIx64, request.seqNum, teardown->udfHandle) SUdfcFuncHandle *handle =
          (SUdfcFuncHandle *)(teardown->udfHandle);
      SUdf *udf = handle->udf;
      bool  unloadUdf = false;
      uv_mutex_lock(&global.udfsMutex);
      udf->refCount--;
      if (udf->refCount == 0) {
        unloadUdf = true;
        taosHashRemove(global.udfsHash, udf->name, strlen(udf->name));
      }
      uv_mutex_unlock(&global.udfsMutex);
      if (unloadUdf) {
        uv_cond_destroy(&udf->condReady);
        uv_mutex_destroy(&udf->lock);
        if (udf->destroyFunc) {
          (udf->destroyFunc)();
        }
        uv_dlclose(&udf->lib);
        taosMemoryFree(udf);
      }
      taosMemoryFree(handle);

      SUdfResponse  response;
      SUdfResponse *rsp = &response;
      rsp->seqNum = request.seqNum;
      rsp->type = request.type;
      rsp->code = 0;
      int32_t len = encodeUdfResponse(NULL, rsp);
      rsp->msgLen = len;
      void *bufBegin = taosMemoryMalloc(len);
      void *buf = bufBegin;
      encodeUdfResponse(&buf, rsp);
      uvUdf->output = uv_buf_init(bufBegin, len);

      taosMemoryFree(uvUdf->input.base);
      break;
    }
    default: {
      break;
    }
  }
}

void udfdOnWrite(uv_write_t *req, int status) {
  SUvUdfWork *work = (SUvUdfWork *)req->data;
  if (status < 0) {
    // TODO:log error and process it.
  }
  fnDebug("send response. length:%zu, status: %s", work->output.len, uv_err_name(status));
  taosMemoryFree(work->output.base);
  taosMemoryFree(work);
  taosMemoryFree(req);
}

void udfdSendResponse(uv_work_t *work, int status) {
  SUvUdfWork *udfWork = (SUvUdfWork *)(work->data);

  uv_write_t *write_req = taosMemoryMalloc(sizeof(uv_write_t));
  write_req->data = udfWork;
  uv_write(write_req, udfWork->client, &udfWork->output, 1, udfdOnWrite);

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
      // TODO: log error
      buf->base = NULL;
      buf->len = 0;
    }
  } else {
    ctx->inputCap = ctx->inputTotal > ctx->inputCap ? ctx->inputTotal : ctx->inputCap;
    void *inputBuf = taosMemoryRealloc(ctx->inputBuf, ctx->inputCap);
    if (inputBuf) {
      ctx->inputBuf = inputBuf;
      buf->base = ctx->inputBuf + ctx->inputLen;
      buf->len = ctx->inputCap - ctx->inputLen;
    } else {
      // TODO: log error
      buf->base = NULL;
      buf->len = 0;
    }
  }
  fnDebug("allocate buf. input buf cap - len - total : %d - %d - %d", ctx->inputCap, ctx->inputLen, ctx->inputTotal);
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
  uv_work_t  *work = taosMemoryMalloc(sizeof(uv_work_t));
  SUvUdfWork *udfWork = taosMemoryMalloc(sizeof(SUvUdfWork));
  udfWork->client = conn->client;
  udfWork->input = uv_buf_init(conn->inputBuf, conn->inputLen);
  conn->inputBuf = NULL;
  conn->inputLen = 0;
  conn->inputCap = 0;
  conn->inputTotal = -1;
  work->data = udfWork;
  uv_queue_work(global.loop, work, udfdProcessRequest, udfdSendResponse);
}

void udfdPipeCloseCb(uv_handle_t *pipe) {
  SUdfdUvConn *conn = pipe->data;
  taosMemoryFree(conn->client);
  taosMemoryFree(conn->inputBuf);
  taosMemoryFree(conn);
}

void udfdUvHandleError(SUdfdUvConn *conn) { uv_close((uv_handle_t *)conn->client, udfdPipeCloseCb); }

void udfdPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  fnDebug("udf read %zd bytes from client", nread);
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
    fnDebug("Receive error %s", uv_err_name(nread));
    if (nread == UV_EOF) {
      // TODO check more when close
    } else {
    }
    udfdUvHandleError(conn);
  }
}

void udfdOnNewConnection(uv_stream_t *server, int status) {
  fnDebug("new connection");
  if (status < 0) {
    // TODO
    return;
  }

  uv_pipe_t *client = (uv_pipe_t *)taosMemoryMalloc(sizeof(uv_pipe_t));
  uv_pipe_init(global.loop, client, 0);
  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    SUdfdUvConn *ctx = taosMemoryMalloc(sizeof(SUdfdUvConn));
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

void udfdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) { return; }

int initEpSetFromCfg(const char* firstEp, const char* secondEp, SCorEpSet* pEpSet) {
  pEpSet->version = 0;

  // init mnode ip set
  SEpSet* mgmtEpSet = &(pEpSet->epSet);
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

int32_t udfdFillUdfInfoFromMNode(void *clientRpc, char *udfName, SUdf *udf) {
  SRetrieveFuncReq retrieveReq = {0};
  retrieveReq.numOfFuncs = 1;
  retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
  taosArrayPush(retrieveReq.pFuncNames, udfName);

  int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
  taosArrayDestroy(retrieveReq.pFuncNames);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_RETRIEVE_FUNC;

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(clientRpc, &global.mgmtEp.epSet, &rpcMsg, &rpcRsp);
  SRetrieveFuncRsp retrieveRsp = {0};
  tDeserializeSRetrieveFuncRsp(rpcRsp.pCont, rpcRsp.contLen, &retrieveRsp);

  SFuncInfo *pFuncInfo = (SFuncInfo *)taosArrayGet(retrieveRsp.pFuncInfos, 0);

  udf->funcType = pFuncInfo->funcType;
  udf->scriptType = pFuncInfo->scriptType;
  udf->outputType = pFuncInfo->funcType;
  udf->outputLen = pFuncInfo->outputLen;
  udf->bufSize = pFuncInfo->bufSize;

  char path[PATH_MAX] = {0};
  snprintf(path, sizeof(path), "%s/lib%s.so", "/tmp", udfName);
  TdFilePtr file = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  // TODO check for failure of flush to disk
  taosWriteFile(file, pFuncInfo->pCode, pFuncInfo->codeSize);
  taosCloseFile(&file);
  strncpy(udf->path, path, strlen(path));
  taosArrayDestroy(retrieveRsp.pFuncInfos);

  rpcFreeCont(rpcRsp.pCont);
  return 0;
}

int32_t udfdOpenClientRpc() {
  char *pass = "taosdata";
  char *user = "root";
  char  secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), secretEncrypt);
  SRpcInit rpcInit = {0};
  rpcInit.label = (char *)"UDFD";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = udfdProcessRpcRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.parent = &global;

  rpcInit.user = (char *)user;
  rpcInit.ckey = (char *)"key";
  rpcInit.secret = (char *)secretEncrypt;
  rpcInit.spi = 1;

  global.clientRpc = rpcOpen(&rpcInit);

  return 0;
}

int32_t udfdCloseClientRpc() {
  rpcClose(global.clientRpc);
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
  }
  global.loop = loop;

  uv_pipe_init(global.loop, &global.ctrlPipe, 1);
  uv_pipe_open(&global.ctrlPipe, 0);
  uv_read_start((uv_stream_t *)&global.ctrlPipe, udfdCtrlAllocBufCb, udfdCtrlReadCb);

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
    return -1;
  }
  if ((r = uv_listen((uv_stream_t *)&global.listeningPipe, 128, udfdOnNewConnection))) {
    fnError("Listen error %s", uv_err_name(r));
    removeListeningPipe();
    return -2;
  }
  return 0;
}

static int32_t udfdRun() {
  global.udfsHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  uv_mutex_init(&global.udfsMutex);

  // TOOD: client rpc to fetch udf function info from mnode
  if (udfdOpenClientRpc() != 0) {
    fnError("open rpc connection to mnode failure");
    return -1;
  }

  if (udfdUvInit() != 0) {
    fnError("uv init failure");
    return -2;
  }

  fnInfo("start the udfd");
  int code = uv_run(global.loop, UV_RUN_DEFAULT);
  fnInfo("udfd stopped. result: %s, code: %d", uv_err_name(code), code);
  int codeClose = uv_loop_close(global.loop);
  fnDebug("uv loop close. result: %s", uv_err_name(codeClose));
  removeListeningPipe();
  udfdCloseClientRpc();
  uv_mutex_destroy(&global.udfsMutex);
  taosHashCleanup(global.udfsHash);
  return 0;
}

int main(int argc, char *argv[]) {
  if (!taosCheckSystemIsSmallEnd()) {
    printf("failed to start since on non-small-end machines\n");
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
    printf("failed to start since init log error\n");
    return -1;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    fnError("failed to start since read config error");
    return -1;
  }

  initEpSetFromCfg(tsFirst, tsSecond, &global.mgmtEp);
  return udfdRun();
}
