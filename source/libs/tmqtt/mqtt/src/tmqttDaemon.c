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
#include "bnode.h"
#include "tmqtt.h"
#include "version.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tmsg.h"
#include "trpc.h"
#include "tmisce.h"
#include "tversion.h"
#include "thash.h"
#include "tmqttCtx.h"
// clang-format on

SMqttdContext global;

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

  // SUdfScriptPlugin *scriptPlugin;
  void *scriptUdfCtx;

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
static void    udfdCloseClientRpc();

static void udfdIntrSignalHandler(uv_signal_t *handle, int signum);
static void removeListeningPipe();

static void    udfdPrintVersion();
static int32_t udfdParseArgs(int32_t argc, char *argv[]);
static int32_t udfdInitLog();

static void    udfdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void    udfdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf);
static int32_t udfdUvInit();
static void    udfdCloseWalkCb(uv_handle_t *handle, void *arg);
static void    udfdRun();
static void    udfdConnectMnodeThreadFunc(void *args);

void udfdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(parent, pMsg);
  SUdfdRpcSendRecvInfo *msgInfo = (SUdfdRpcSendRecvInfo *)pMsg->info.ahandle;

  if (pEpSet) {
    if (!isEpsetEqual(&global.mgmtEp.epSet, pEpSet)) {
      updateEpSet_s(&global.mgmtEp, pEpSet);
    }
  }

  if (pMsg->code != TSDB_CODE_SUCCESS) {
    bndError("udfd rpc error. code: %s", tstrerror(pMsg->code));
    msgInfo->code = pMsg->code;
    goto _return;
  }

  if (msgInfo->rpcType == UDFD_RPC_MNODE_CONNECT) {
    SConnectRsp connectRsp = {0};
    if (tDeserializeSConnectRsp(pMsg->pCont, pMsg->contLen, &connectRsp) < 0) {
      bndError("udfd deserialize connect response failed");
      goto _return;
    }

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
    if (tDeserializeSRetrieveFuncRsp(pMsg->pCont, pMsg->contLen, &retrieveRsp) < 0) {
      bndError("udfd deserialize retrieve func response failed");
      goto _return;
    }

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
    // msgInfo->code = udfdSaveFuncBodyToFile(pFuncInfo, udf);
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
  TAOS_MQTT_MGMT_CHECK_PTR_RCODE(clientRpc, udfName, udf);
  SRetrieveFuncReq retrieveReq = {0};
  retrieveReq.numOfFuncs = 1;
  retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
  if (taosArrayPush(retrieveReq.pFuncNames, udfName) == NULL) {
    taosArrayDestroy(retrieveReq.pFuncNames);
    return terrno;
  }

  int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
  if (contLen < 0) {
    taosArrayDestroy(retrieveReq.pFuncNames);
    return terrno;
  }
  void *pReq = rpcMallocCont(contLen);
  if (tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq) < 0) {
    taosArrayDestroy(retrieveReq.pFuncNames);
    rpcFreeCont(pReq);
    return terrno;
  }
  taosArrayDestroy(retrieveReq.pFuncNames);

  SUdfdRpcSendRecvInfo *msgInfo = taosMemoryCalloc(1, sizeof(SUdfdRpcSendRecvInfo));
  if (NULL == msgInfo) {
    return terrno;
  }
  msgInfo->rpcType = UDFD_RPC_RETRIVE_FUNC;
  msgInfo->param = udf;
  if (uv_sem_init(&msgInfo->resultSem, 0) != 0) {
    taosMemoryFree(msgInfo);
    return TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  }

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_RETRIEVE_FUNC;
  rpcMsg.info.ahandle = msgInfo;
  int32_t code = rpcSendRequest(clientRpc, &global.mgmtEp.epSet, &rpcMsg, NULL);
  if (code == 0) {
    uv_sem_wait(&msgInfo->resultSem);
    uv_sem_destroy(&msgInfo->resultSem);
    code = msgInfo->code;
  }
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

    int32_t code = taosGetFqdnPortFromEp(secondEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    if (code != TSDB_CODE_SUCCESS) {
      bndError("invalid ep %s", secondEp);
    } else {
      mgmtEpSet->numOfEps++;
    }
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
  TAOS_CHECK_RETURN(taosVersionStrToInt(td_version, &rpcInit.compatibilityVer));
  global.clientRpc = rpcOpen(&rpcInit);
  if (global.clientRpc == NULL) {
    bndError("failed to init dnode rpc client");
    return terrno;
  }
  return 0;
}

void udfdCloseClientRpc() {
  bndInfo("udfd begin closing rpc");
  rpcClose(global.clientRpc);
  bndInfo("udfd finish closing rpc");
}

void udfdOnWrite(uv_write_t *req, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(req);
  SUvUdfWork *work = (SUvUdfWork *)req->data;
  if (status < 0) {
    bndError("udfd send response error, length: %zu code: %s", work->output.len, uv_err_name(status));
  }
  // remove work from the connection work list
  if (work->conn != NULL) {
    SUvUdfWork **ppWork;
    for (ppWork = &work->conn->pWorkList; *ppWork && (*ppWork != work); ppWork = &((*ppWork)->pWorkNext)) {
    }
    if (*ppWork == work) {
      *ppWork = work->pWorkNext;
    } else {
      bndError("work not in conn any more");
    }
  }
  taosMemoryFree(work->output.base);
  taosMemoryFree(work);
  taosMemoryFree(req);
}

void udfdSendResponse(uv_work_t *work, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(work);
  SUvUdfWork *udfWork = (SUvUdfWork *)(work->data);

  if (udfWork->conn != NULL) {
    uv_write_t *write_req = taosMemoryMalloc(sizeof(uv_write_t));
    if (write_req == NULL) {
      bndError("udfd send response error, malloc failed");
      taosMemoryFree(work);
      return;
    }
    write_req->data = udfWork;
    int32_t code = uv_write(write_req, udfWork->conn->client, &udfWork->output, 1, udfdOnWrite);
    if (code != 0) {
      bndError("udfd send response error %s", uv_strerror(code));
      taosMemoryFree(write_req);
    }
  }
  taosMemoryFree(work);
}

void udfdAllocBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(handle, buf);
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
      bndError("udfd can not allocate enough memory");
      buf->base = NULL;
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
      bndError("udfd can not allocate enough memory");
      buf->base = NULL;
      buf->len = 0;
    }
  }
}

bool isUdfdUvMsgComplete(SUdfdUvConn *pipe) {
  if (pipe == NULL) {
    bndError("udfd pipe is NULL, LINE:%d", __LINE__);
    return false;
  }
  if (pipe->inputTotal == -1 && pipe->inputLen >= sizeof(int32_t)) {
    pipe->inputTotal = *(int32_t *)(pipe->inputBuf);
  }
  if (pipe->inputLen == pipe->inputCap && pipe->inputTotal == pipe->inputCap) {
    bndDebug("receive request complete. length %d", pipe->inputLen);
    return true;
  }
  return false;
}
/*
void udfdHandleRequest(SUdfdUvConn *conn) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(conn);
  char   *inputBuf = conn->inputBuf;
  int32_t inputLen = conn->inputLen;

  uv_work_t *work = taosMemoryMalloc(sizeof(uv_work_t));
  if (work == NULL) {
    bndError("udfd malloc work failed");
    return;
  }
  SUvUdfWork *udfWork = taosMemoryMalloc(sizeof(SUvUdfWork));
  if (udfWork == NULL) {
    bndError("udfd malloc udf work failed");
    taosMemoryFree(work);
    return;
  }
  udfWork->conn = conn;
  udfWork->pWorkNext = conn->pWorkList;
  conn->pWorkList = udfWork;
  udfWork->input = uv_buf_init(inputBuf, inputLen);
  conn->inputBuf = NULL;
  conn->inputLen = 0;
  conn->inputCap = 0;
  conn->inputTotal = -1;
  work->data = udfWork;
  if (uv_queue_work(global.loop, work, udfdProcessRequest, udfdSendResponse) != 0) {
    bndError("udfd queue work failed");
    taosMemoryFree(work);
    taosMemoryFree(udfWork);
  }
}
*/
void udfdPipeCloseCb(uv_handle_t *pipe) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(pipe);
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
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(client, buf);
  bndDebug("udfd read %zd bytes from client", nread);
  if (nread == 0) return;

  SUdfdUvConn *conn = client->data;

  if (nread > 0) {
    conn->inputLen += nread;
    if (isUdfdUvMsgComplete(conn)) {
      // udfdHandleRequest(conn);
    } else {
      // log error or continue;
    }
    return;
  }

  if (nread < 0) {
    if (nread == UV_EOF) {
      bndInfo("udfd pipe read EOF");
    } else {
      bndError("Receive error %s", uv_err_name(nread));
    }
    // udfdUvHandleError(conn);
  }
}

void udfdOnNewConnection(uv_stream_t *server, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(server);
  if (status < 0) {
    bndError("udfd new connection error. code: %s", uv_strerror(status));
    return;
  }
  int32_t code = 0;

  uv_pipe_t *client = (uv_pipe_t *)taosMemoryMalloc(sizeof(uv_pipe_t));
  if (client == NULL) {
    bndError("udfd pipe malloc failed");
    return;
  }
  code = uv_pipe_init(global.loop, client, 0);
  if (code) {
    bndError("udfd pipe init error %s", uv_strerror(code));
    taosMemoryFree(client);
    return;
  }
  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    SUdfdUvConn *ctx = taosMemoryMalloc(sizeof(SUdfdUvConn));
    if (ctx == NULL) {
      bndError("udfd conn malloc failed");
      goto _exit;
    }
    ctx->pWorkList = NULL;
    ctx->client = (uv_stream_t *)client;
    ctx->inputBuf = 0;
    ctx->inputLen = 0;
    ctx->inputCap = 0;
    client->data = ctx;
    ctx->client = (uv_stream_t *)client;
    code = uv_read_start((uv_stream_t *)client, udfdAllocBuffer, udfdPipeRead);
    if (code) {
      bndError("udfd read start error %s", uv_strerror(code));
      // udfdUvHandleError(ctx);
      taosMemoryFree(ctx);
      taosMemoryFree(client);
    }
    return;
  }
_exit:
  uv_close((uv_handle_t *)client, NULL);
  taosMemoryFree(client);
}

void udfdIntrSignalHandler(uv_signal_t *handle, int signum) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(handle);
  bndInfo("udfd signal received: %d\n", signum);
  uv_fs_t req;
  int32_t code = uv_fs_unlink(global.loop, &req, global.listenPipeName, NULL);
  if (code) {
    bndError("remove listening pipe %s failed, reason:%s, lino:%d", global.listenPipeName, uv_strerror(code), __LINE__);
  }
  code = uv_signal_stop(handle);
  if (code) {
    bndError("stop signal handler failed, reason:%s", uv_strerror(code));
  }
  uv_stop(global.loop);
}

static int32_t mqttdParseArgs(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          (void)printf("config file path overflow");
          bndError("config file path too long:%s", argv[i]);
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        (void)printf("'-c' requires config parameter, default is %s\n", configDir);
        bndError("'-c' requires config parameter, default is %d\n", 1);
        return -1;
      }
    } else if (strcmp(argv[i], "-d") == 0) {
      if (i < argc - 1) {
        global.dnode_id = atoi(argv[++i]);
      } else {
        (void)printf("'-d' requires dnode_id parameter, default is %d\n", 1);
        bndError("'-d' requires dnode_id parameter, default is %d\n", 1);
        return -1;
      }
    } else if (strcmp(argv[i], "-V") == 0) {
      global.printVersion = true;
    } else {
    }
  }

  return 0;
}

static void mqttdPrintVersion() {
  (void)printf("taosmqtt version: %s compatible_version: %s\n", td_version, td_compatible_version);
  (void)printf("git: %s\n", td_gitinfo);
  (void)printf("build: %s\n", td_buildinfo);
}

static int32_t mqttdInitLog() {
  const char *logName = "mqttdlog";
  TAOS_CHECK_RETURN(taosInitLogOutput(&logName));
  return taosCreateLog(logName, 1, configDir, NULL, NULL, NULL, NULL, 0);
}

void udfdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(buf);
  buf->base = taosMemoryMalloc(suggested_size);
  if (buf->base == NULL) {
    bndError("udfd ctrl pipe alloc buffer failed");
    return;
  }
  buf->len = suggested_size;
}

void udfdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(q, buf);
  if (nread < 0) {
    bndError("udfd ctrl pipe read error. %s", uv_err_name(nread));
    taosMemoryFree(buf->base);
    uv_close((uv_handle_t *)q, NULL);
    uv_stop(global.loop);
    return;
  }
  bndError("udfd ctrl pipe read %zu bytes", nread);
  taosMemoryFree(buf->base);
}

static void removeListeningPipe() {
  uv_fs_t req;
  int     err = uv_fs_unlink(global.loop, &req, global.listenPipeName, NULL);
  uv_fs_req_cleanup(&req);
  if (err) {
    bndInfo("remove listening pipe %s : %s, lino:%d", global.listenPipeName, uv_strerror(err), __LINE__);
  }
}

static int32_t udfdUvInit() {
  TAOS_CHECK_RETURN(uv_loop_init(global.loop));

  if (tsStartUdfd) {  // udfd is started by taosd, which shall exit when taosd exit
    TAOS_CHECK_RETURN(uv_pipe_init(global.loop, &global.ctrlPipe, 1));
    TAOS_CHECK_RETURN(uv_pipe_open(&global.ctrlPipe, 0));
    TAOS_CHECK_RETURN(uv_read_start((uv_stream_t *)&global.ctrlPipe, udfdCtrlAllocBufCb, udfdCtrlReadCb));
  }
  // getUdfdPipeName(global.listenPipeName, sizeof(global.listenPipeName));

  // removeListeningPipe();

  TAOS_CHECK_RETURN(uv_pipe_init(global.loop, &global.listeningPipe, 0));

  TAOS_CHECK_RETURN(uv_signal_init(global.loop, &global.intrSignal));
  TAOS_CHECK_RETURN(uv_signal_start(&global.intrSignal, udfdIntrSignalHandler, SIGINT));

  int r;
  bndInfo("bind to pipe %s", global.listenPipeName);
  if ((r = uv_pipe_bind(&global.listeningPipe, global.listenPipeName))) {
    bndError("Bind error %s", uv_err_name(r));
    removeListeningPipe();
    return -2;
  }
  if ((r = uv_listen((uv_stream_t *)&global.listeningPipe, 128, udfdOnNewConnection))) {
    bndError("Listen error %s", uv_err_name(r));
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

static int32_t udfdGlobalDataInit() {
  uv_loop_t *loop = taosMemoryMalloc(sizeof(uv_loop_t));
  if (loop == NULL) {
    bndError("udfd init uv loop failed, mem overflow");
    return terrno;
  }
  global.loop = loop;
  /*
  if (uv_mutex_init(&global.scriptPluginsMutex) != 0) {
    bndError("udfd init script plugins mutex failed");
    return TSDB_CODE_MQTT_MGMT_UV_EXEC_FAILURE;
  }
  */
  global.udfsHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (global.udfsHash == NULL) {
    return terrno;
  }
  // taosHashSetFreeFp(global.udfsHash, udfdFreeUdf);

  if (uv_mutex_init(&global.udfsMutex) != 0) {
    bndError("udfd init udfs mutex failed");
    return TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  }

  return 0;
}

static void udfdGlobalDataDeinit() {
  uv_mutex_destroy(&global.udfsMutex);
  // uv_mutex_destroy(&global.scriptPluginsMutex);
  taosMemoryFreeClear(global.loop);
  bndInfo("udfd global data deinit");
}

static void udfdRun() {
  bndInfo("start udfd event loop");
  int32_t code = uv_run(global.loop, UV_RUN_DEFAULT);
  if (code != 0) {
    bndError("udfd event loop still has active handles or requests.");
  }
  bndInfo("udfd event loop stopped.");

  (void)uv_loop_close(global.loop);

  uv_walk(global.loop, udfdCloseWalkCb, NULL);
  code = uv_run(global.loop, UV_RUN_DEFAULT);
  if (code != 0) {
    bndError("udfd event loop still has active handles or requests.");
  }
  (void)uv_loop_close(global.loop);
}

int32_t udfdCreateUdfSourceDir() {
  snprintf(global.mqttDataDir, PATH_MAX, "%s/.mqttd", tsDataDir);
  int32_t code = taosMkDir(global.mqttDataDir);
  if (code != TSDB_CODE_SUCCESS) {
    snprintf(global.mqttDataDir, PATH_MAX, "%s/.mqttd", tsTempDir);
    code = taosMkDir(global.mqttDataDir);
  }
  bndInfo("udfd create udf source directory %s. result: %s", global.mqttDataDir, tstrerror(code));

  return code;
}

void udfdDestroyUdfSourceDir() {
  bndInfo("destory udf source directory %s", global.mqttDataDir);
  taosRemoveDir(global.mqttDataDir);
}

extern int ttq_main(int argc, char *argv[]);

int main(int argc, char *argv[]) {
  int  code = 0;
  bool logInitialized = false;
  bool cfgInitialized = false;
  // bool openClientRpcFinished = false;
  // bool residentFuncsInited = false;
  // bool udfSourceDirInited = false;
  // bool globalDataInited = false;

  if (!taosCheckSystemIsLittleEnd()) {
    bndError("failed to start since on non-little-end machines\n");
    return -1;
  }

  if (mqttdParseArgs(argc, argv) != 0) {
    bndError("failed to start since parse args error\n");
    return -1;
  }

  if (global.printVersion) {
    mqttdPrintVersion();
    return 0;
  }

  if (mqttdInitLog() != 0) {
    // ignore create log failed, because this error doesnot matter
    //(void)printf("failed to init mqttd log.");
    bndError("failed to init mqttd log.");
  } else {
    logInitialized = true;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    bndError("failed to start since read config error");
    code = -2;
    goto _exit;
  }
  cfgInitialized = true;
  bndInfo("mqttd start with config file %s", configDir);

  if (initEpSetFromCfg(tsFirst, tsSecond, &global.mgmtEp) != 0) {
    bndError("init ep set from cfg failed");
    code = -3;
    goto _exit;
  }
  bndInfo("mqttd start with mnode ep %s:%hu", global.mgmtEp.epSet.eps[0].fqdn, global.mgmtEp.epSet.eps[0].port);

  code = ttq_main(argc, argv);

  bndInfo("mqttd exit normally: %d", code);
  /*
  if (udfdOpenClientRpc() != 0) {
    bndError("open rpc connection to mnode failed");
    code = -4;
    goto _exit;
  }
  bndInfo("udfd rpc client is opened");
  openClientRpcFinished = true;  // rpc is opened

  if (udfdCreateUdfSourceDir() != 0) {
    bndError("create udf source directory failed");
    code = -5;
    goto _exit;
  }
  udfSourceDirInited = true;  // udf source dir is created
  bndInfo("udfd udf source directory is created");

  if (udfdGlobalDataInit() != 0) {
    bndError("init global data failed");
    code = -6;
    goto _exit;
  }
  globalDataInited = true;  // global data is inited
  bndInfo("udfd global data is inited");

  if (udfdUvInit() != 0) {
    bndError("uv init failure");
    code = -7;
    goto _exit;
  }
  bndInfo("udfd uv is inited");

  udfdRun();

  removeListeningPipe();
  */
_exit:
  /*
  if (globalDataInited) {
    udfdGlobalDataDeinit();
  }
  if (udfSourceDirInited) {
    udfdDestroyUdfSourceDir();
  }
  if (openClientRpcFinished) {
    udfdCloseClientRpc();
  }
  */
  if (cfgInitialized) {
    taosCleanupCfg();
  }
  if (logInitialized) {
    taosCloseLog();
  }

  return code;
}
