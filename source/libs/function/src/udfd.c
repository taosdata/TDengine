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
// clang-format on

typedef struct SUdfdContext {
  uv_loop_t  *loop;
  uv_pipe_t   ctrlPipe;
  uv_signal_t intrSignal;
  char        listenPipeName[PATH_MAX + UDF_LISTEN_PIPE_NAME_LEN + 2];
  uv_pipe_t   listeningPipe;

  void      *clientRpc;
  SCorEpSet  mgmtEp;
  uv_mutex_t udfsMutex;
  SHashObj  *udfsHash;

  SArray *residentFuncs;

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

typedef enum { UDF_STATE_INIT = 0, UDF_STATE_LOADING, UDF_STATE_READY, UDF_STATE_UNLOADING } EUdfState;

typedef struct SUdf {
  int32_t    refCount;
  EUdfState  state;
  uv_mutex_t lock;
  uv_cond_t  condReady;
  bool       resident;

  char    name[TSDB_FUNC_NAME_LEN + 1];
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;

  char path[PATH_MAX];

  uv_lib_t lib;

  TUdfScalarProcFunc scalarProcFunc;

  TUdfAggStartFunc   aggStartFunc;
  TUdfAggProcessFunc aggProcFunc;
  TUdfAggFinishFunc  aggFinishFunc;
  TUdfAggMergeFunc   aggMergeFunc;

  TUdfInitFunc    initFunc;
  TUdfDestroyFunc destroyFunc;
} SUdf;

// TODO: add private udf structure.
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
static int32_t udfdLoadUdf(char *udfName, SUdf *udf);
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

void udfdProcessSetupRequest(SUvUdfWork *uvUdf, SUdfRequest *request) {
  // TODO: tracable id from client. connect, setup, call, teardown
  fnInfo("setup request. seq num: %" PRId64 ", udf name: %s", request->seqNum, request->setup.udfName);
  SUdfSetupRequest *setup = &request->setup;
  int32_t           code = TSDB_CODE_SUCCESS;
  SUdf             *udf = NULL;
  uv_mutex_lock(&global.udfsMutex);
  SUdf **udfInHash = taosHashGet(global.udfsHash, request->setup.udfName, strlen(request->setup.udfName));
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
    SUdf **pUdf = &udf;
    taosHashPut(global.udfsHash, request->setup.udfName, strlen(request->setup.udfName), pUdf, POINTER_BYTES);
    uv_mutex_unlock(&global.udfsMutex);
  }

  uv_mutex_lock(&udf->lock);
  if (udf->state == UDF_STATE_INIT) {
    udf->state = UDF_STATE_LOADING;
    code = udfdLoadUdf(setup->udfName, udf);
    if (udf->initFunc) {
      udf->initFunc();
    }
    udf->resident = false;
    for (int32_t i = 0; i < taosArrayGetSize(global.residentFuncs); ++i) {
      char *funcName = taosArrayGet(global.residentFuncs, i);
      if (strcmp(setup->udfName, funcName) == 0) {
        udf->resident = true;
        break;
      }
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
  rsp.seqNum = request->seqNum;
  rsp.type = request->type;
  rsp.code = code;
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

      SUdfDataBlock input = {0};
      convertDataBlockToUdfDataBlock(&call->block, &input);
      code = udf->scalarProcFunc(&input, &output);
      freeUdfDataDataBlock(&input);
      convertUdfColumnToDataBlock(&output, &response.callRsp.resultData);
      freeUdfColumn(&output);
      break;
    }
    case TSDB_UDF_CALL_AGG_INIT: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      udf->aggStartFunc(&outBuf);
      subRsp->resultBuf = outBuf;
      break;
    }
    case TSDB_UDF_CALL_AGG_PROC: {
      SUdfDataBlock input = {0};
      convertDataBlockToUdfDataBlock(&call->block, &input);
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->aggProcFunc(&input, &call->interBuf, &outBuf);
      freeUdfInterBuf(&call->interBuf);
      freeUdfDataDataBlock(&input);
      subRsp->resultBuf = outBuf;

      break;
    }
    case TSDB_UDF_CALL_AGG_MERGE: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->aggMergeFunc(&call->interBuf, &call->interBuf2, &outBuf);
      freeUdfInterBuf(&call->interBuf);
      freeUdfInterBuf(&call->interBuf2);
      subRsp->resultBuf = outBuf;

      break;
    }
    case TSDB_UDF_CALL_AGG_FIN: {
      SUdfInterBuf outBuf = {.buf = taosMemoryMalloc(udf->bufSize), .bufLen = udf->bufSize, .numOfResult = 0};
      code = udf->aggFinishFunc(&call->interBuf, &outBuf);
      freeUdfInterBuf(&call->interBuf);
      subRsp->resultBuf = outBuf;
      break;
    }
    default:
      break;
  }

  rsp->seqNum = request->seqNum;
  rsp->type = request->type;
  rsp->code = code;
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
  if (udf->refCount == 0 && !udf->resident) {
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
    if (retrieveRsp.pFuncInfos == NULL) {
      goto _return;
    }
    SFuncInfo *pFuncInfo = (SFuncInfo *)taosArrayGet(retrieveRsp.pFuncInfos, 0);
    SUdf      *udf = msgInfo->param;
    udf->funcType = pFuncInfo->funcType;
    udf->scriptType = pFuncInfo->scriptType;
    udf->outputType = pFuncInfo->outputType;
    udf->outputLen = pFuncInfo->outputLen;
    udf->bufSize = pFuncInfo->bufSize;

    if (!osTempSpaceAvailable()) {
      terrno = TSDB_CODE_NO_AVAIL_DISK;
      msgInfo->code = terrno;
      fnError("udfd create shared library failed since %s", terrstr(terrno));
      goto _return;
    }

    char path[PATH_MAX] = {0};
#ifdef WINDOWS
    snprintf(path, sizeof(path), "%s%s.dll", tsTempDir, pFuncInfo->name);
#else
    snprintf(path, sizeof(path), "%s/lib%s.so", tsTempDir, pFuncInfo->name);
#endif

    TdFilePtr file = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC);
    if (file == NULL) {
      fnError("udfd write udf shared library: %s failed, error: %d %s", path, errno, strerror(errno));
      msgInfo->code = TSDB_CODE_FILE_CORRUPTED;
      goto _return;
    }

    int64_t count = taosWriteFile(file, pFuncInfo->pCode, pFuncInfo->codeSize);
    if (count != pFuncInfo->codeSize) {
      fnError("udfd write udf shared library failed");
      msgInfo->code = TSDB_CODE_FILE_CORRUPTED;
      goto _return;
    }
    taosCloseFile(&file);
    strncpy(udf->path, path, PATH_MAX);
    tFreeSFuncInfo(pFuncInfo);
    taosArrayDestroy(retrieveRsp.pFuncInfos);
    msgInfo->code = 0;
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

int32_t udfdConnectToMnode() {
  SConnectReq connReq = {0};
  connReq.connType = CONN_TYPE__UDFD;
  tstrncpy(connReq.app, "udfd", sizeof(connReq.app));
  tstrncpy(connReq.user, TSDB_DEFAULT_USER, sizeof(connReq.user));
  char pass[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)(TSDB_DEFAULT_PASS), strlen(TSDB_DEFAULT_PASS), pass);
  tstrncpy(connReq.passwd, pass, sizeof(connReq.passwd));
  connReq.pid = taosGetPId();
  connReq.startTime = taosGetTimestampMs();
  strcpy(connReq.sVer, version);

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSConnectReq(pReq, contLen, &connReq);

  SUdfdRpcSendRecvInfo *msgInfo = taosMemoryCalloc(1, sizeof(SUdfdRpcSendRecvInfo));
  msgInfo->rpcType = UDFD_RPC_MNODE_CONNECT;
  uv_sem_init(&msgInfo->resultSem, 0);

  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_MND_CONNECT;
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = contLen;
  rpcMsg.info.ahandle = msgInfo;
  rpcSendRequest(global.clientRpc, &global.mgmtEp.epSet, &rpcMsg, NULL);

  uv_sem_wait(&msgInfo->resultSem);
  int32_t code = msgInfo->code;
  uv_sem_destroy(&msgInfo->resultSem);
  taosMemoryFree(msgInfo);
  return code;
}

int32_t udfdLoadUdf(char *udfName, SUdf *udf) {
  strncpy(udf->name, udfName, TSDB_FUNC_NAME_LEN);
  int32_t err = 0;

  err = udfdFillUdfInfoFromMNode(global.clientRpc, udf->name, udf);
  if (err != 0) {
    fnError("can not retrieve udf from mnode. udf name %s", udfName);
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
  }

  err = uv_dlopen(udf->path, &udf->lib);
  if (err != 0) {
    fnError("can not load library %s. error: %s", udf->path, uv_strerror(err));
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
  }

  char  initFuncName[TSDB_FUNC_NAME_LEN + 5] = {0};
  char *initSuffix = "_init";
  strcpy(initFuncName, udfName);
  strncat(initFuncName, initSuffix, strlen(initSuffix));
  uv_dlsym(&udf->lib, initFuncName, (void **)(&udf->initFunc));

  char  destroyFuncName[TSDB_FUNC_NAME_LEN + 5] = {0};
  char *destroySuffix = "_destroy";
  strcpy(destroyFuncName, udfName);
  strncat(destroyFuncName, destroySuffix, strlen(destroySuffix));
  uv_dlsym(&udf->lib, destroyFuncName, (void **)(&udf->destroyFunc));

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
    strncpy(startFuncName, processFuncName, sizeof(startFuncName));
    strncat(startFuncName, startSuffix, strlen(startSuffix));
    uv_dlsym(&udf->lib, startFuncName, (void **)(&udf->aggStartFunc));
    char  finishFuncName[TSDB_FUNC_NAME_LEN + 7] = {0};
    char *finishSuffix = "_finish";
    strncpy(finishFuncName, processFuncName, sizeof(finishFuncName));
    strncat(finishFuncName, finishSuffix, strlen(finishSuffix));
    uv_dlsym(&udf->lib, finishFuncName, (void **)(&udf->aggFinishFunc));
    char  mergeFuncName[TSDB_FUNC_NAME_LEN + 6] = {0};
    char *mergeSuffix = "_merge";
    strncpy(mergeFuncName, processFuncName, sizeof(mergeFuncName));
    strncat(mergeFuncName, mergeSuffix, strlen(mergeSuffix));
    uv_dlsym(&udf->lib, mergeFuncName, (void **)(&udf->aggMergeFunc));
  }
  return 0;
}
static bool udfdRpcRfp(int32_t code, tmsg_t msgType) {
  if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_SYN_NOT_LEADER ||
      code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED || code == TSDB_CODE_SYN_RESTORING ||
      code == TSDB_CODE_MNODE_NOT_FOUND || code == TSDB_CODE_APP_IS_STARTING || code == TSDB_CODE_APP_IS_STOPPING) {
    if (msgType == TDMT_SCH_QUERY || msgType == TDMT_SCH_MERGE_QUERY || msgType == TDMT_SCH_FETCH ||
        msgType == TDMT_SCH_MERGE_FETCH) {
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

void udfdConnectMnodeThreadFunc(void *args) {
  int32_t retryMnodeTimes = 0;
  int32_t code = 0;
  while (retryMnodeTimes++ <= TSDB_MAX_REPLICA) {
    uv_sleep(100 * (1 << retryMnodeTimes));
    code = udfdConnectToMnode();
    if (code == 0) {
      break;
    }
    fnError("udfd can not connect to mnode, code: %s. retry", tstrerror(code));
  }

  if (code != 0) {
    fnError("udfd can not connect to mnode");
  }
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
      SUdf *udf = *udfInHash;
      if (udf->destroyFunc) {
        (udf->destroyFunc)();
      }
      uv_dlclose(&udf->lib);
      taosMemoryFree(udf);
      taosHashRemove(global.udfsHash, funcName, strlen(funcName));
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
    fnError("open rpc connection to mnode failure");
    return -3;
  }

  if (udfdUvInit() != 0) {
    fnError("uv init failure");
    return -5;
  }

  udfdInitResidentFuncs();

  uv_thread_t mnodeConnectThread;
  uv_thread_create(&mnodeConnectThread, udfdConnectMnodeThreadFunc, NULL);

  udfdRun();

  removeListeningPipe();
  udfdCloseClientRpc();

  udfdDeinitResidentFuncs();
  udfdCleanup();
  return 0;
}
