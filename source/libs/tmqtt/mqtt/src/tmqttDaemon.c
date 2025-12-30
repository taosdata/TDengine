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

struct SMqttdUvConn;
struct SUvMqttWork;

typedef struct SMqttdUvConn {
  uv_stream_t *client;
  char        *inputBuf;
  int32_t      inputLen;
  int32_t      inputCap;
  int32_t      inputTotal;

  struct SUvMqttWork *pWorkList;  // head of work list
} SMqttdUvConn;

typedef struct SUvMqttWork {
  SMqttdUvConn *conn;
  uv_buf_t      input;
  uv_buf_t      output;

  struct SUvMqttWork *pWorkNext;
} SUvMqttWork;

typedef enum { MQTT_STATE_INIT = 0, MQTT_STATE_LOADING, MQTT_STATE_READY } EMqttState;

typedef struct SMqtt {
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
  EMqttState state;
  uv_mutex_t lock;
  uv_cond_t  condReady;
  bool       resident;

  // SMqttScriptPlugin *scriptPlugin;
  void *scriptMqttCtx;

  int64_t lastFetchTime;  // last fetch time in milliseconds
  bool    expired;
} SMqtt;

typedef struct SMqttcFuncHandle {
  SMqtt *mqtt;
} SMqttcFuncHandle;

typedef enum EMqttdRpcReqRspType {
  MQTTD_RPC_MNODE_CONNECT = 0,
  MQTTD_RPC_RETRIVE_FUNC,
} EMqttdRpcReqRspType;

typedef struct SMqttdRpcSendRecvInfo {
  EMqttdRpcReqRspType rpcType;
  int32_t             code;
  void               *param;
  uv_sem_t            resultSem;
} SMqttdRpcSendRecvInfo;

static void    mqttdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t mqttdFillMqttInfoFromMNode(void *clientRpc, char *mqttName, SMqtt *mqtt);
static int32_t mqttdConnectToMnode();
static bool    mqttdRpcRfp(int32_t code, tmsg_t msgType);
static int     initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static int32_t mqttdOpenClientRpc();
static void    mqttdCloseClientRpc();

static void mqttdIntrSignalHandler(uv_signal_t *handle, int signum);
static void removeListeningPipe();

static void    mqttdPrintVersion();
static int32_t mqttdParseArgs(int32_t argc, char *argv[]);
static int32_t mqttdInitLog();

static void    mqttdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void    mqttdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf);
static int32_t mqttdUvInit();
static void    mqttdCloseWalkCb(uv_handle_t *handle, void *arg);
static void    mqttdRun();
static void    mqttdConnectMnodeThreadFunc(void *args);

void mqttdProcessRpcRsp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(parent, pMsg);
  SMqttdRpcSendRecvInfo *msgInfo = (SMqttdRpcSendRecvInfo *)pMsg->info.ahandle;

  if (pEpSet) {
    if (!isEpsetEqual(&global.mgmtEp.epSet, pEpSet)) {
      updateEpSet_s(&global.mgmtEp, pEpSet);
    }
  }

  if (pMsg->code != TSDB_CODE_SUCCESS) {
    bndError("mqttd rpc error. code: %s", tstrerror(pMsg->code));
    msgInfo->code = pMsg->code;
    goto _return;
  }

  if (msgInfo->rpcType == MQTTD_RPC_MNODE_CONNECT) {
    SConnectRsp connectRsp = {0};
    if (tDeserializeSConnectRsp(pMsg->pCont, pMsg->contLen, &connectRsp) < 0) {
      bndError("mqttd deserialize connect response failed");
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
  } else if (msgInfo->rpcType == MQTTD_RPC_RETRIVE_FUNC) {
    SRetrieveFuncRsp retrieveRsp = {0};
    if (tDeserializeSRetrieveFuncRsp(pMsg->pCont, pMsg->contLen, &retrieveRsp) < 0) {
      bndError("mqttd deserialize retrieve func response failed");
      goto _return;
    }

    SFuncInfo *pFuncInfo = (SFuncInfo *)taosArrayGet(retrieveRsp.pFuncInfos, 0);
    SMqtt     *mqtt = msgInfo->param;
    mqtt->funcType = pFuncInfo->funcType;
    mqtt->scriptType = pFuncInfo->scriptType;
    mqtt->outputType = pFuncInfo->outputType;
    mqtt->outputLen = pFuncInfo->outputLen;
    mqtt->bufSize = pFuncInfo->bufSize;

    SFuncExtraInfo *pFuncExtraInfo = (SFuncExtraInfo *)taosArrayGet(retrieveRsp.pFuncExtraInfos, 0);
    mqtt->version = pFuncExtraInfo->funcVersion;
    mqtt->createdTime = pFuncExtraInfo->funcCreatedTime;
    // msgInfo->code = mqttdSaveFuncBodyToFile(pFuncInfo, mqtt);
    if (msgInfo->code != 0) {
      mqtt->lastFetchTime = 0;
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

int32_t mqttdFillMqttInfoFromMNode(void *clientRpc, char *mqttName, SMqtt *mqtt) {
  TAOS_MQTT_MGMT_CHECK_PTR_RCODE(clientRpc, mqttName, mqtt);
  SRetrieveFuncReq retrieveReq = {0};
  retrieveReq.numOfFuncs = 1;
  retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
  if (taosArrayPush(retrieveReq.pFuncNames, mqttName) == NULL) {
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

  SMqttdRpcSendRecvInfo *msgInfo = taosMemoryCalloc(1, sizeof(SMqttdRpcSendRecvInfo));
  if (NULL == msgInfo) {
    return terrno;
  }
  msgInfo->rpcType = MQTTD_RPC_RETRIVE_FUNC;
  msgInfo->param = mqtt;
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

static bool mqttdRpcRfp(int32_t code, tmsg_t msgType) {
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

int32_t mqttdOpenClientRpc() {
  SRpcInit rpcInit = {0};
  rpcInit.label = "MQTTD";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)mqttdProcessRpcRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.parent = &global;
  rpcInit.rfp = mqttdRpcRfp;
  rpcInit.compressSize = tsCompressMsgSize;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);
  rpcInit.connLimitNum = connLimitNum;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  TAOS_CHECK_RETURN(taosVersionStrToInt(td_version, &rpcInit.compatibilityVer));

  memcpy(rpcInit.caPath, tsTLSCaPath, strlen(tsTLSCaPath));
  memcpy(rpcInit.certPath, tsTLSSvrCertPath, strlen(tsTLSSvrCertPath));
  memcpy(rpcInit.keyPath, tsTLSSvrKeyPath, strlen(tsTLSSvrKeyPath));
  memcpy(rpcInit.cliCertPath, tsTLSCliCertPath, strlen(tsTLSCliCertPath));
  memcpy(rpcInit.cliKeyPath, tsTLSCliKeyPath, strlen(tsTLSCliKeyPath));

  global.clientRpc = rpcOpen(&rpcInit);
  if (global.clientRpc == NULL) {
    bndError("failed to init dnode rpc client");
    return terrno;
  }
  return 0;
}

void mqttdCloseClientRpc() {
  bndInfo("mqttd begin closing rpc");
  rpcClose(global.clientRpc);
  bndInfo("mqttd finish closing rpc");
}

void mqttdOnWrite(uv_write_t *req, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(req);
  SUvMqttWork *work = (SUvMqttWork *)req->data;
  if (status < 0) {
    bndError("mqttd send response error, length: %zu code: %s", work->output.len, uv_err_name(status));
  }
  // remove work from the connection work list
  if (work->conn != NULL) {
    SUvMqttWork **ppWork;
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

void mqttdSendResponse(uv_work_t *work, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(work);
  SUvMqttWork *mqttWork = (SUvMqttWork *)(work->data);

  if (mqttWork->conn != NULL) {
    uv_write_t *write_req = taosMemoryMalloc(sizeof(uv_write_t));
    if (write_req == NULL) {
      bndError("mqttd send response error, malloc failed");
      taosMemoryFree(work);
      return;
    }
    write_req->data = mqttWork;
    int32_t code = uv_write(write_req, mqttWork->conn->client, &mqttWork->output, 1, mqttdOnWrite);
    if (code != 0) {
      bndError("mqttd send response error %s", uv_strerror(code));
      taosMemoryFree(write_req);
    }
  }
  taosMemoryFree(work);
}

void mqttdAllocBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(handle, buf);
  SMqttdUvConn *ctx = handle->data;
  int32_t       msgHeadSize = sizeof(int32_t) + sizeof(int64_t);
  if (ctx->inputCap == 0) {
    ctx->inputBuf = taosMemoryMalloc(msgHeadSize);
    if (ctx->inputBuf) {
      ctx->inputLen = 0;
      ctx->inputCap = msgHeadSize;
      ctx->inputTotal = -1;

      buf->base = ctx->inputBuf;
      buf->len = ctx->inputCap;
    } else {
      bndError("mqttd can not allocate enough memory");
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
      bndError("mqttd can not allocate enough memory");
      buf->base = NULL;
      buf->len = 0;
    }
  }
}

bool isMqttdUvMsgComplete(SMqttdUvConn *pipe) {
  if (pipe == NULL) {
    bndError("mqttd pipe is NULL, LINE:%d", __LINE__);
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
void mqttdPipeCloseCb(uv_handle_t *pipe) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(pipe);
  SMqttdUvConn *conn = pipe->data;
  SUvMqttWork  *pWork = conn->pWorkList;
  while (pWork != NULL) {
    pWork->conn = NULL;
    pWork = pWork->pWorkNext;
  }

  taosMemoryFree(conn->client);
  taosMemoryFree(conn->inputBuf);
  taosMemoryFree(conn);
}

void mqttdPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(client, buf);
  bndDebug("mqttd read %zd bytes from client", nread);
  if (nread == 0) return;

  SMqttdUvConn *conn = client->data;

  if (nread > 0) {
    conn->inputLen += nread;
    if (isMqttdUvMsgComplete(conn)) {
      // mqttdHandleRequest(conn);
    } else {
      // log error or continue;
    }
    return;
  }

  if (nread < 0) {
    if (nread == UV_EOF) {
      bndInfo("mqttd pipe read EOF");
    } else {
      bndError("Receive error %s", uv_err_name(nread));
    }
  }
}

void mqttdOnNewConnection(uv_stream_t *server, int status) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(server);
  if (status < 0) {
    bndError("mqttd new connection error. code: %s", uv_strerror(status));
    return;
  }
  int32_t code = 0;

  uv_pipe_t *client = (uv_pipe_t *)taosMemoryMalloc(sizeof(uv_pipe_t));
  if (client == NULL) {
    bndError("mqttd pipe malloc failed");
    return;
  }
  code = uv_pipe_init(global.loop, client, 0);
  if (code) {
    bndError("mqttd pipe init error %s", uv_strerror(code));
    taosMemoryFree(client);
    return;
  }
  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    SMqttdUvConn *ctx = taosMemoryMalloc(sizeof(SMqttdUvConn));
    if (ctx == NULL) {
      bndError("mqttd conn malloc failed");
      goto _exit;
    }
    ctx->pWorkList = NULL;
    ctx->client = (uv_stream_t *)client;
    ctx->inputBuf = 0;
    ctx->inputLen = 0;
    ctx->inputCap = 0;
    client->data = ctx;
    ctx->client = (uv_stream_t *)client;
    code = uv_read_start((uv_stream_t *)client, mqttdAllocBuffer, mqttdPipeRead);
    if (code) {
      bndError("mqttd read start error %s", uv_strerror(code));
      taosMemoryFree(ctx);
      taosMemoryFree(client);
    }
    return;
  }
_exit:
  uv_close((uv_handle_t *)client, NULL);
  taosMemoryFree(client);
}

void mqttdIntrSignalHandler(uv_signal_t *handle, int signum) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(handle);
  bndInfo("mqttd signal received: %d\n", signum);
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

void mqttdCtrlAllocBufCb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(buf);
  buf->base = taosMemoryMalloc(suggested_size);
  if (buf->base == NULL) {
    bndError("mqttd ctrl pipe alloc buffer failed");
    return;
  }
  buf->len = suggested_size;
}

void mqttdCtrlReadCb(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf) {
  TAOS_MQTT_MGMT_CHECK_PTR_RVOID(q, buf);
  if (nread < 0) {
    bndError("mqttd ctrl pipe read error. %s", uv_err_name(nread));
    taosMemoryFree(buf->base);
    uv_close((uv_handle_t *)q, NULL);
    uv_stop(global.loop);
    return;
  }
  bndError("mqttd ctrl pipe read %zu bytes", nread);
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

static int32_t mqttdUvInit() {
  TAOS_CHECK_RETURN(uv_loop_init(global.loop));

  TAOS_CHECK_RETURN(uv_pipe_init(global.loop, &global.listeningPipe, 0));

  TAOS_CHECK_RETURN(uv_signal_init(global.loop, &global.intrSignal));
  TAOS_CHECK_RETURN(uv_signal_start(&global.intrSignal, mqttdIntrSignalHandler, SIGINT));

  int r;
  bndInfo("bind to pipe %s", global.listenPipeName);
  if ((r = uv_pipe_bind(&global.listeningPipe, global.listenPipeName))) {
    bndError("Bind error %s", uv_err_name(r));
    removeListeningPipe();
    return -2;
  }
  if ((r = uv_listen((uv_stream_t *)&global.listeningPipe, 128, mqttdOnNewConnection))) {
    bndError("Listen error %s", uv_err_name(r));
    removeListeningPipe();
    return -3;
  }
  return 0;
}

static void mqttdCloseWalkCb(uv_handle_t *handle, void *arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

static int32_t mqttdGlobalDataInit() {
  uv_loop_t *loop = taosMemoryMalloc(sizeof(uv_loop_t));
  if (loop == NULL) {
    bndError("mqttd init uv loop failed, mem overflow");
    return terrno;
  }
  global.loop = loop;

  global.mqttsHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (global.mqttsHash == NULL) {
    return terrno;
  }

  if (uv_mutex_init(&global.mqttsMutex) != 0) {
    bndError("mqttd init mqtts mutex failed");
    return TSDB_CODE_BNODE_UV_EXEC_FAILURE;
  }

  return 0;
}

static void mqttdGlobalDataDeinit() {
  uv_mutex_destroy(&global.mqttsMutex);
  // uv_mutex_destroy(&global.scriptPluginsMutex);
  taosMemoryFreeClear(global.loop);
  bndInfo("mqttd global data deinit");
}

static void mqttdRun() {
  bndInfo("start mqttd event loop");
  int32_t code = uv_run(global.loop, UV_RUN_DEFAULT);
  if (code != 0) {
    bndError("mqttd event loop still has active handles or requests.");
  }
  bndInfo("mqttd event loop stopped.");

  (void)uv_loop_close(global.loop);

  uv_walk(global.loop, mqttdCloseWalkCb, NULL);
  code = uv_run(global.loop, UV_RUN_DEFAULT);
  if (code != 0) {
    bndError("mqttd event loop still has active handles or requests.");
  }
  (void)uv_loop_close(global.loop);
}

int32_t mqttdCreateMqttSourceDir() {
  snprintf(global.mqttDataDir, PATH_MAX, "%s/.mqttd", tsDataDir);
  int32_t code = taosMkDir(global.mqttDataDir);
  if (code != TSDB_CODE_SUCCESS) {
    snprintf(global.mqttDataDir, PATH_MAX, "%s/.mqttd", tsTempDir);
    code = taosMkDir(global.mqttDataDir);
  }
  bndInfo("mqttd create mqtt source directory %s. result: %s", global.mqttDataDir, tstrerror(code));

  return code;
}

void mqttdDestroyMqttSourceDir() {
  bndInfo("destory mqtt source directory %s", global.mqttDataDir);
  taosRemoveDir(global.mqttDataDir);
}

extern int ttq_main(int argc, char *argv[]);

int main(int argc, char *argv[]) {
  int  code = 0;
  bool logInitialized = false;
  bool cfgInitialized = false;

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

_exit:
  if (cfgInitialized) {
    taosCleanupCfg();
  }
  if (logInitialized) {
    taosCloseLog();
  }

  return code;
}
