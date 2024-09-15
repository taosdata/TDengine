/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 * * This program is free software: you can use, redistribute, and/or modify
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

#include "transComm.h"

static TdThreadOnce transModuleInit = PTHREAD_ONCE_INIT;

static char* notify = "a";

typedef struct {
  int       notifyCount;  //
  int       init;         // init or not
  STransMsg msg;
} SSvrRegArg;

typedef struct SSvrConn {
  int32_t    ref;
  uv_tcp_t*  pTcp;
  uv_timer_t pTimer;

  queue       queue;
  SConnBuffer readBuf;  // read buf,
  int         inType;
  void*       pInst;    // rpc init
  void*       ahandle;  //
  void*       hostThrd;
  STransQueue resps;

  // SSvrRegArg regArg;
  bool broken;  // conn broken;

  ConnStatus status;

  uint32_t serverIp;
  uint32_t clientIp;
  uint16_t port;

  char src[32];
  char dst[32];

  int64_t refId;
  int     spi;
  char    info[64];
  char    user[TSDB_UNI_LEN];  // user ID for the link
  int8_t  userInited;
  char    secret[TSDB_PASSWORD_LEN];
  char    ckey[TSDB_PASSWORD_LEN];  // ciphering key

  int64_t whiteListVer;

  // state req dict
  SHashObj* pQTable;
  uv_buf_t* buf;
  int32_t   bufSize;
  queue     wq;  // uv_write_t queue
} SSvrConn;

typedef struct SSvrRespMsg {
  SSvrConn*     pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
  int32_t       seqNum;
  void*         arg;
  FilteFunc     func;
  int8_t        sent;

} SSvrRespMsg;

typedef struct {
  int64_t       ver;
  SIpWhiteList* pList;
  // SArray* list;

} SWhiteUserList;
typedef struct {
  SHashObj* pList;
  int64_t   ver;
} SIpWhiteListTab;
typedef struct SWorkThrd {
  TdThread     thread;
  uv_connect_t connect_req;
  uv_pipe_t*   pipe;
  uv_os_fd_t   fd;
  uv_loop_t*   loop;
  SAsyncPool*  asyncPool;
  queue        msg;

  queue conn;
  void* pInst;
  bool  quit;

  SIpWhiteListTab* pWhiteList;
  int64_t          whiteListVer;
  int8_t           enableIpWhiteList;

  int32_t connRefMgt;
} SWorkThrd;

typedef struct SServerObj {
  TdThread   thread;
  uv_tcp_t   server;
  uv_loop_t* loop;

  // work thread info
  int         workerIdx;
  int         numOfThreads;
  int         numOfWorkerReady;
  SWorkThrd** pThreadObj;

  uv_pipe_t   pipeListen;
  uv_pipe_t** pipe;
  uint32_t    ip;
  uint32_t    port;
  uv_async_t* pAcceptAsync;  // just to quit from from accept thread

  bool inited;
} SServerObj;

SIpWhiteListTab* uvWhiteListCreate();
void             uvWhiteListDestroy(SIpWhiteListTab* pWhite);
int32_t          uvWhiteListAdd(SIpWhiteListTab* pWhite, char* user, SIpWhiteList* pList, int64_t ver);
void             uvWhiteListUpdate(SIpWhiteListTab* pWhite, SHashObj* pTable);
bool             uvWhiteListCheckConn(SIpWhiteListTab* pWhite, SSvrConn* pConn);
bool             uvWhiteListFilte(SIpWhiteListTab* pWhite, char* user, uint32_t ip, int64_t ver);
void             uvWhiteListSetConnVer(SIpWhiteListTab* pWhite, SSvrConn* pConn);

static void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvOnRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
static void uvOnTimeoutCb(uv_timer_t* handle);
static void uvOnSendCb(uv_write_t* req, int status);
static void uvOnPipeWriteCb(uv_write_t* req, int status);
static void uvOnAcceptCb(uv_stream_t* stream, int status);
static void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf);
static void uvWorkerAsyncCb(uv_async_t* handle);
static void uvAcceptAsyncCb(uv_async_t* handle);
static void uvShutDownCb(uv_shutdown_t* req, int status);

/*
 * time-consuming task throwed into BG work thread
 */
static void uvWorkDoTask(uv_work_t* req);
static void uvWorkAfterTask(uv_work_t* req, int status);

static void uvWalkCb(uv_handle_t* handle, void* arg);
static void uvFreeCb(uv_handle_t* handle);

static FORCE_INLINE void uvStartSendRespImpl(SSvrRespMsg* smsg);

static int  uvPrepareSendData(SSvrRespMsg* msg, uv_buf_t* wb);
static void uvStartSendResp(SSvrRespMsg* msg);

static void uvNotifyLinkBrokenToApp(SSvrConn* conn);

static FORCE_INLINE void      destroySmsg(SSvrRespMsg* smsg);
static FORCE_INLINE SSvrConn* createConn(void* hThrd);
static FORCE_INLINE void      destroyConn(SSvrConn* conn, bool clear /*clear handle or not*/);

int32_t uvGetConnRefOfThrd(SWorkThrd* thrd) { return thrd ? thrd->connRefMgt : -1; }

static void uvHandleQuit(SSvrRespMsg* msg, SWorkThrd* thrd);
static void uvHandleRelease(SSvrRespMsg* msg, SWorkThrd* thrd);
static void uvHandleResp(SSvrRespMsg* msg, SWorkThrd* thrd);
static void uvHandleRegister(SSvrRespMsg* msg, SWorkThrd* thrd);
static void uvHandleUpdate(SSvrRespMsg* pMsg, SWorkThrd* thrd);
static void (*transAsyncHandle[])(SSvrRespMsg* msg, SWorkThrd* thrd) = {uvHandleResp, uvHandleQuit, uvHandleRelease,
                                                                        uvHandleRegister, uvHandleUpdate};

static void uvDestroyConn(uv_handle_t* handle);

// server and worker thread
static void* transWorkerThread(void* arg);
static void* transAcceptThread(void* arg);

static void destroyWorkThrd(SWorkThrd* pThrd);
static void destroyWorkThrdObj(SWorkThrd* pThrd);

static void sendQuitToWorkThrd(SWorkThrd* pThrd);

// add handle loop
static int32_t addHandleToWorkloop(SWorkThrd* pThrd, char* pipeName);
static int32_t addHandleToAcceptloop(void* arg);

#define SRV_RELEASE_UV(loop)             \
  do {                                   \
    (void)uv_walk(loop, uvWalkCb, NULL); \
    (void)uv_run(loop, UV_RUN_DEFAULT);  \
    (void)uv_loop_close(loop);           \
  } while (0);

#define ASYNC_ERR_JRET(thrd)                            \
  do {                                                  \
    if (thrd->quit) {                                   \
      tTrace("worker thread already quit, ignore msg"); \
      goto _return1;                                    \
    }                                                   \
  } while (0)

void uvAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SSvrConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  int32_t      code = transAllocBuffer(pBuf, buf);
  if (code < 0) {
    tError("conn %p failed to alloc buffer, since %s", conn, tstrerror(code));
  }
}

// refers specifically to query or insert timeout
static void uvHandleActivityTimeout(uv_timer_t* handle) {
  SSvrConn* conn = handle->data;
  tDebug("%p timeout since no activity", conn);
}

static bool uvCheckIp(SIpV4Range* pRange, int32_t ip) {
  // impl later
  SubnetUtils subnet = {0};
  if (subnetInit(&subnet, pRange) != 0) {
    return false;
  }
  return subnetCheckIp(&subnet, ip);
}
SIpWhiteListTab* uvWhiteListCreate() {
  SIpWhiteListTab* pWhiteList = taosMemoryCalloc(1, sizeof(SIpWhiteListTab));
  if (pWhiteList == NULL) {
    return NULL;
  }

  pWhiteList->pList = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 0, HASH_NO_LOCK);
  if (pWhiteList->pList == NULL) {
    taosMemoryFree(pWhiteList);
    return NULL;
  }

  pWhiteList->ver = -1;
  return pWhiteList;
}
void uvWhiteListDestroy(SIpWhiteListTab* pWhite) {
  SHashObj* pWhiteList = pWhite->pList;
  void*     pIter = taosHashIterate(pWhiteList, NULL);
  while (pIter) {
    SWhiteUserList* pUserList = *(SWhiteUserList**)pIter;
    taosMemoryFree(pUserList->pList);
    taosMemoryFree(pUserList);

    pIter = taosHashIterate(pWhiteList, pIter);
  }
  taosHashCleanup(pWhiteList);
  taosMemoryFree(pWhite);
}

int32_t uvWhiteListToStr(SWhiteUserList* plist, char* user, char** ppBuf) {
  char*   tmp = NULL;
  int32_t tlen = transUtilSWhiteListToStr(plist->pList, &tmp);
  if (tlen < 0) {
    return tlen;
  }

  char* pBuf = taosMemoryCalloc(1, tlen + 64);
  if (pBuf == NULL) {
    return terrno;
  }

  int32_t len = sprintf(pBuf, "user: %s, ver: %" PRId64 ", ip: {%s}", user, plist->ver, tmp);
  taosMemoryFree(tmp);

  *ppBuf = pBuf;
  return len;
}
void uvWhiteListDebug(SIpWhiteListTab* pWrite) {
  int32_t   code = 0;
  SHashObj* pWhiteList = pWrite->pList;
  void*     pIter = taosHashIterate(pWhiteList, NULL);
  while (pIter) {
    size_t klen = 0;
    char   user[TSDB_USER_LEN + 1] = {0};
    char*  pUser = taosHashGetKey(pIter, &klen);
    memcpy(user, pUser, klen);

    SWhiteUserList* pUserList = *(SWhiteUserList**)pIter;

    char* buf = NULL;

    code = uvWhiteListToStr(pUserList, user, &buf);
    if (code != 0) {
      tDebug("ip-white-list failed to debug to str since %s", buf);
    }
    taosMemoryFree(buf);
    pIter = taosHashIterate(pWhiteList, pIter);
  }
}
int32_t uvWhiteListAdd(SIpWhiteListTab* pWhite, char* user, SIpWhiteList* plist, int64_t ver) {
  int32_t   code = 0;
  SHashObj* pWhiteList = pWhite->pList;

  SWhiteUserList** ppUserList = taosHashGet(pWhiteList, user, strlen(user));
  if (ppUserList == NULL || *ppUserList == NULL) {
    SWhiteUserList* pUserList = taosMemoryCalloc(1, sizeof(SWhiteUserList));
    if (pUserList == NULL) {
      return terrno;
    }

    pUserList->ver = ver;

    pUserList->pList = plist;

    code = taosHashPut(pWhiteList, user, strlen(user), &pUserList, sizeof(void*));
    if (code != 0) {
      taosMemoryFree(pUserList);
      return code;
    }
  } else {
    SWhiteUserList* pUserList = *ppUserList;

    taosMemoryFreeClear(pUserList->pList);
    pUserList->ver = ver;
    pUserList->pList = plist;
  }
  uvWhiteListDebug(pWhite);
  return 0;
}

void uvWhiteListUpdate(SIpWhiteListTab* pWhite, SHashObj* pTable) {
  pWhite->ver++;
  // impl later
}

static bool uvWhiteListIsDefaultAddr(uint32_t ip) {
  // 127.0.0.1
  static SIpV4Range range = {.ip = 16777343, .mask = 32};
  return range.ip == ip;
}
bool uvWhiteListFilte(SIpWhiteListTab* pWhite, char* user, uint32_t ip, int64_t ver) {
  // impl check
  SHashObj* pWhiteList = pWhite->pList;
  bool      valid = false;

  if (uvWhiteListIsDefaultAddr(ip)) return true;

  SWhiteUserList** ppList = taosHashGet(pWhiteList, user, strlen(user));
  if (ppList == NULL || *ppList == NULL) {
    return false;
  }
  SWhiteUserList* pUserList = *ppList;
  if (pUserList->ver == ver) return true;

  SIpWhiteList* pIpWhiteList = pUserList->pList;
  for (int i = 0; i < pIpWhiteList->num; i++) {
    SIpV4Range* range = &pIpWhiteList->pIpRange[i];
    if (uvCheckIp(range, ip)) {
      valid = true;
      break;
    }
  }
  return valid;
}
bool uvWhiteListCheckConn(SIpWhiteListTab* pWhite, SSvrConn* pConn) {
  if (pConn->inType == TDMT_MND_STATUS || pConn->inType == TDMT_MND_RETRIEVE_IP_WHITE ||
      pConn->serverIp == pConn->clientIp ||
      pWhite->ver == pConn->whiteListVer /*|| strncmp(pConn->user, "_dnd", strlen("_dnd")) == 0*/)
    return true;

  return uvWhiteListFilte(pWhite, pConn->user, pConn->clientIp, pConn->whiteListVer);
}
void uvWhiteListSetConnVer(SIpWhiteListTab* pWhite, SSvrConn* pConn) {
  // if conn already check by current whiteLis
  pConn->whiteListVer = pWhite->ver;
}

static void uvPerfLog_receive(SSvrConn* pConn, STransMsgHead* pHead, STransMsg* pTransMsg) {
  if (!(rpcDebugFlag & DEBUG_DEBUG)) {
    return;
  }

  STrans*   pInst = pConn->pInst;
  STraceId* trace = &pHead->traceId;

  int64_t        cost = taosGetTimestampUs() - taosNtoh64(pHead->timestamp);
  static int64_t EXCEPTION_LIMIT_US = 100 * 1000;

  if (pConn->status == ConnNormal && pHead->noResp == 0) {
    if (cost >= EXCEPTION_LIMIT_US) {
      tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, cost:%dus, recv exception, seqNum:%d, qid:%ld",
              transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
              (int)cost, pTransMsg->info.seqNum, pTransMsg->info.qId);
    } else {
      tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, cost:%dus, seqNum:%d, qid:%ld", transLabel(pInst),
              pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen, (int)cost,
              pTransMsg->info.seqNum, pTransMsg->info.qId);
    }
  } else {
    if (cost >= EXCEPTION_LIMIT_US) {
      tGDebug(
          "%s conn %p %s received from %s, local info:%s, len:%d, noResp:%d, code:%d, cost:%dus, recv exception, "
          "seqNum:%d, qid:%ld",
          transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
          pHead->noResp, pTransMsg->code, (int)(cost), pTransMsg->info.seqNum, pTransMsg->info.qId);
    } else {
      tGDebug(
          "%s conn %p %s received from %s, local info:%s, len:%d, noResp:%d, code:%d, cost:%dus, seqNum:%d, qid:%ld",
          transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
          pHead->noResp, pTransMsg->code, (int)(cost), pTransMsg->info.seqNum, pTransMsg->info.qId);
    }
  }
  tGTrace("%s handle %p conn:%p translated to app, refId:%" PRIu64, transLabel(pInst), pTransMsg->info.handle, pConn,
          pConn->refId);
}

static int8_t uvValidConn(SSvrConn* pConn) {
  STrans*    pInst = pConn->pInst;
  SWorkThrd* pThrd = pConn->hostThrd;
  int8_t     forbiddenIp = 0;
  if (pThrd->enableIpWhiteList) {
    forbiddenIp = !uvWhiteListCheckConn(pThrd->pWhiteList, pConn) ? 1 : 0;
    if (forbiddenIp == 0) {
      uvWhiteListSetConnVer(pThrd->pWhiteList, pConn);
    }
  }
  return forbiddenIp;
}

static int32_t uvMayHandleReleaseReq(SSvrConn* pConn, STransMsgHead* pHead) {
  int32_t code = 0;
  STrans* pInst = pConn->pInst;
  int64_t qId = taosHton64(pHead->qid);
  if (pHead->msgType == TDMT_SCH_TASK_RELEASE && qId > 0) {
    void* p = taosHashGet(pConn->pQTable, &qId, sizeof(qId));
    if (p == NULL) {
      code = TSDB_CODE_RPC_NO_STATE;
      tTrace("conn %p recv release, and releady release by server qid:%ld", pConn, qId);
    } else {
      SSvrRegArg* arg = p;
      (pInst->cfp)(pInst->parent, &(arg->msg), NULL);
      tTrace("conn %p recv release, notify server app, qid:%ld", pConn, qId);

      (void)taosHashRemove(pConn->pQTable, &qId, sizeof(qId));
      tTrace("conn %p clear state,qid:%ld", pConn, qId);
    }

    STransMsg tmsg = {.code = code,
                      .msgType = pHead->msgType + 1,
                      .info.qId = qId,
                      .info.traceId = pHead->traceId,
                      .info.seqNum = htonl(pHead->seqNum)};

    SSvrRespMsg* srvMsg = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
    srvMsg->msg = tmsg;
    srvMsg->type = Normal;
    srvMsg->pConn = pConn;

    transQueuePush(&pConn->resps, &srvMsg->q);

    uvStartSendRespImpl(srvMsg);
    taosMemoryFree(pHead);
    return code;
  }
  return 0;
}

bool uvConnMayGetUserInfo(SSvrConn* pConn, STransMsgHead** ppHead, int32_t* msgLen) {
  if (pConn->userInited) {
    return false;
  }

  STrans*        pInst = pConn->pInst;
  STransMsgHead* pHead = *ppHead;
  int32_t        len = *msgLen;
  if (pHead->withUserInfo) {
    STransMsgHead* tHead = taosMemoryCalloc(1, len - sizeof(pInst->user));
    memcpy((char*)tHead, (char*)pHead, TRANS_MSG_OVERHEAD);
    memcpy((char*)tHead + TRANS_MSG_OVERHEAD, (char*)pHead + TRANS_MSG_OVERHEAD + sizeof(pInst->user),
           len - sizeof(STransMsgHead) - sizeof(pInst->user));
    tHead->msgLen = htonl(htonl(pHead->msgLen) - sizeof(pInst->user));

    memcpy(pConn->user, (char*)pHead + TRANS_MSG_OVERHEAD, sizeof(pConn->user));
    pConn->userInited = 1;

    taosMemoryFree(pHead);
    *ppHead = tHead;
    *msgLen = len - sizeof(pInst->user);
    return true;
  }
  return false;
}
static bool uvHandleReq(SSvrConn* pConn) {
  STrans*    pInst = pConn->pInst;
  SWorkThrd* pThrd = pConn->hostThrd;

  int8_t         acquire = 0;
  STransMsgHead* pHead = NULL;

  int8_t resetBuf = 0;
  int    msgLen = transDumpFromBuffer(&pConn->readBuf, (char**)&pHead, 0);
  if (msgLen <= 0) {
    tError("%s conn %p read invalid packet", transLabel(pInst), pConn);
    return false;
  }
  if (uvConnMayGetUserInfo(pConn, &pHead, &msgLen) == true) {
    tDebug("%s conn %p get user info", transLabel(pInst), pConn);
  }

  if (resetBuf == 0) {
    tTrace("%s conn %p not reset read buf", transLabel(pInst), pConn);
  }

  if (transDecompressMsg((char**)&pHead, msgLen) < 0) {
    tError("%s conn %p recv invalid packet, failed to decompress", transLabel(pInst), pConn);
    taosMemoryFree(pHead);
    return false;
  }
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);

  pConn->inType = pHead->msgType;

  int8_t forbiddenIp = 0;
  if (pThrd->enableIpWhiteList && tsEnableWhiteList) {
    forbiddenIp = !uvWhiteListCheckConn(pThrd->pWhiteList, pConn) ? 1 : 0;
    if (forbiddenIp == 0) {
      uvWhiteListSetConnVer(pThrd->pWhiteList, pConn);
    }
  }

  if (uvMayHandleReleaseReq(pConn, pHead)) {
    return true;
  }

  STransMsg transMsg = {0};
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = pHead->content;
  transMsg.msgType = pHead->msgType;
  transMsg.code = pHead->code;

  if (transMsg.info.qId > 0) {
    // int32_t code = taosHashPut(pConn->pQTable, &transMsg.info.qId, sizeof(int64_t), &transMsg, sizeof(STransMsg));
    // if (code != 0) {
    //   tError("%s conn %p failed to put msg to req dict, since %s", transLabel(pInst), pConn, tstrerror(code));
    //   return false;
    // }
  }

  if (pHead->seqNum == 0) {
    // ASSERT(0);
  }

  transMsg.info.handle = (void*)transAcquireExHandle(uvGetConnRefOfThrd(pThrd), pConn->refId);
  transMsg.info.refIdMgt = pThrd->connRefMgt;

  // ASSERTS(transMsg.info.handle != NULL, "trans-svr failed to alloc handle to msg");

  // pHead->noResp = 1,
  // 1. server application should not send resp on handle
  // 2. once send out data, cli conn released to conn pool immediately
  // 3. not mixed with persist
  transMsg.info.refId = pHead->noResp == 1 ? -1 : pConn->refId;
  transMsg.info.traceId = pHead->traceId;
  transMsg.info.cliVer = htonl(pHead->compatibilityVer);
  transMsg.info.forbiddenIp = forbiddenIp;
  transMsg.info.noResp = pHead->noResp == 1 ? 1 : 0;
  transMsg.info.seqNum = htonl(pHead->seqNum);
  transMsg.info.qId = taosHton64(pHead->qid);
  transMsg.info.msgType = pHead->msgType;

  // uvMaySetConnAcquired(pConn, pHead);

  uvPerfLog_receive(pConn, pHead, &transMsg);

  // set up conn info
  SRpcConnInfo* pConnInfo = &(transMsg.info.conn);
  pConnInfo->clientIp = pConn->clientIp;
  pConnInfo->clientPort = pConn->port;
  tstrncpy(pConnInfo->user, pConn->user, sizeof(pConnInfo->user));

  (void)transReleaseExHandle(uvGetConnRefOfThrd(pThrd), pConn->refId);

  (*pInst->cfp)(pInst->parent, &transMsg, NULL);
  return true;
}

void uvOnRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf) {
  SSvrConn*  conn = cli->data;
  SWorkThrd* pThrd = conn->hostThrd;

  STUB_RAND_NETWORK_ERR(nread);

  if (true == pThrd->quit) {
    tInfo("work thread received quit msg, destroy conn");
    destroyConn(conn, true);
    return;
  }
  STrans* pInst = conn->pInst;
  int32_t fd = 0;
  (void)uv_fileno((uv_handle_t*)cli, &fd);
  (void)taosSetSockOpt2(fd);

  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    if (pBuf->len <= TRANS_PACKET_LIMIT) {
      while (transReadComplete(pBuf)) {
        if (true == pBuf->invalid || false == uvHandleReq(conn)) {
          tError("%s conn %p read invalid packet, received from %s, local info:%s", transLabel(pInst), conn, conn->dst,
                 conn->src);
          transUnrefSrvHandle(conn);
          return;
        }
      }
      return;
    } else {
      tError("%s conn %p read invalid packet, exceed limit, received from %s, local info:%s", transLabel(pInst), conn,
             conn->dst, conn->src);
      transUnrefSrvHandle(conn);
      return;
    }
  }
  if (nread == 0) {
    return;
  }

  tDebug("%s conn %p read error:%s", transLabel(pInst), conn, uv_err_name(nread));
  if (nread < 0) {
    conn->broken = true;
    transUnrefSrvHandle(conn);
  }
}
void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  buf->len = 2;
  buf->base = taosMemoryCalloc(1, sizeof(char) * buf->len);
  if (buf->base == NULL) {
    tError("failed to alloc conn read buffer since %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
  }
}

void uvOnTimeoutCb(uv_timer_t* handle) {
  // opt
  SSvrConn* pConn = handle->data;
  tError("conn %p time out", pConn);
}

void uvOnSendCb(uv_write_t* req, int status) {
  STUB_RAND_NETWORK_ERR(status);

  SWReqsWrapper* wrapper = req->data;
  SSvrConn*      conn = wrapper->arg;

  queue src;
  QUEUE_INIT(&src);
  QUEUE_MOVE(&wrapper->node, &src);

  freeWReqToWQ(&conn->wq, wrapper);

  tDebug("%s conn %p send data out ", transLabel(conn->pInst), conn);
  if (status == 0) {
    while (!QUEUE_IS_EMPTY(&src)) {
      queue* head = QUEUE_HEAD(&src);
      QUEUE_REMOVE(head);

      SSvrRespMsg* smsg = QUEUE_DATA(head, SSvrRespMsg, q);
      STraceId*    trace = &smsg->msg.info.traceId;
      tGDebug("%s conn %p msg already send out, seqNum:%d, qid:%ld", transLabel(conn->pInst), conn,
              smsg->msg.info.seqNum, smsg->msg.info.qId);
      destroySmsg(smsg);
    }
  } else {
    while (!QUEUE_IS_EMPTY(&src)) {
      queue* head = QUEUE_HEAD(&src);
      QUEUE_REMOVE(head);

      SSvrRespMsg* smsg = QUEUE_DATA(head, SSvrRespMsg, q);
      STraceId*    trace = &smsg->msg.info.traceId;
      tGDebug("%s conn %p failed to send, seqNum:%d, qid:%ld, reason:%s", transLabel(conn->pInst), conn,
              smsg->msg.info.seqNum, smsg->msg.info.qId, uv_err_name(status));
      destroySmsg(smsg);
    }

    conn->broken = true;
    transUnrefSrvHandle(conn);
  }
  transUnrefSrvHandle(conn);
}
static void uvOnPipeWriteCb(uv_write_t* req, int status) {
  STUB_RAND_NETWORK_ERR(status);
  if (status == 0) {
    tTrace("success to dispatch conn to work thread");
  } else {
    tError("fail to dispatch conn to work thread, reason:%s", uv_strerror(status));
  }
  if (!uv_is_closing((uv_handle_t*)req->data)) {
    uv_close((uv_handle_t*)req->data, uvFreeCb);
  } else {
    taosMemoryFree(req->data);
  }
  taosMemoryFree(req);
}

static int uvPrepareSendData(SSvrRespMsg* smsg, uv_buf_t* wb) {
  SSvrConn*  pConn = smsg->pConn;
  STransMsg* pMsg = &smsg->msg;
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    if (pMsg->pCont == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pMsg->contLen = 0;
  }
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);
  pHead->traceId = pMsg->info.traceId;
  pHead->hasEpSet = pMsg->info.hasEpSet;
  pHead->magicNum = htonl(TRANS_MAGIC_NUM);
  pHead->compatibilityVer = htonl(((STrans*)pConn->pInst)->compatibilityVer);
  pHead->version = TRANS_VER;
  pHead->seqNum = htonl(pMsg->info.seqNum);
  pHead->qid = taosHton64(pMsg->info.qId);
  pHead->withUserInfo = pConn->userInited == 0 ? 1 : 0;

  // handle invalid drop_task resp, TD-20098
  if (pConn->inType == TDMT_SCH_DROP_TASK && pMsg->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
    // ASSERT(0);
    //  (void)transQueuePop(&pConn->resps);
    //  destroySmsg(smsg);
    //  return TSDB_CODE_INVALID_MSG;
    return 0;
  }

  pHead->msgType = (0 == pMsg->msgType ? pConn->inType + 1 : pMsg->msgType);
  // pHead->msgType = pMsg->msgType;
  // pHead->release = smsg->type == Release ? 1 : 0;
  pHead->code = htonl(pMsg->code);
  pHead->msgLen = htonl(pMsg->contLen + sizeof(STransMsgHead));

  char*   msg = (char*)pHead;
  int32_t len = transMsgLenFromCont(pMsg->contLen);

  STrans* pInst = pConn->pInst;
  if (pMsg->info.compressed == 0 && pConn->clientIp != pConn->serverIp && pInst->compressSize != -1 &&
      pInst->compressSize < pMsg->contLen) {
    len = transCompressMsg(pMsg->pCont, pMsg->contLen) + sizeof(STransMsgHead);
    pHead->msgLen = (int32_t)htonl((uint32_t)len);
  }

  STraceId* trace = &pMsg->info.traceId;
  tGDebug("%s conn %p %s is sent to %s, local info:%s, len:%d, seqNum:%d, qid:%ld", transLabel(pInst), pConn,
          TMSG_INFO(pHead->msgType), pConn->dst, pConn->src, len, pMsg->info.seqNum, pMsg->info.qId);

  wb->base = (char*)pHead;
  wb->len = len;
  return 0;
}
static int32_t uvBuildToSendData(SSvrConn* pConn, uv_buf_t** ppBuf, int32_t* bufNum, queue* toSendQ) {
  int32_t size = transQueueSize(&pConn->resps);
  tDebug("%s conn %p has %d msg to send", transLabel(pConn->pInst), pConn, size);
  if (size == 0) {
    return 0;
  }

  if (pConn->bufSize < size) {
    pConn->buf = taosMemoryRealloc(pConn->buf, size * sizeof(uv_buf_t));
    pConn->bufSize = size;
  }
  uv_buf_t* pWb = pConn->buf;

  int32_t count = 0;

  while (transQueueSize(&pConn->resps) > 0) {
    queue*       el = transQueuePop(&pConn->resps);
    SSvrRespMsg* pMsg = QUEUE_DATA(el, SSvrRespMsg, q);
    uv_buf_t     wb;
    (void)uvPrepareSendData(pMsg, &wb);
    pWb[count] = wb;
    pMsg->sent = 1;
    QUEUE_PUSH(toSendQ, &pMsg->q);
    count++;
  }

  if (count == 0) {
    return 0;
  }

  *bufNum = count;
  *ppBuf = pWb;

  return 0;
}

static FORCE_INLINE void uvStartSendRespImpl(SSvrRespMsg* smsg) {
  int32_t   code = 0;
  SSvrConn* pConn = smsg->pConn;
  if (pConn->broken) {
    return;
  }
  int32_t size = transQueueSize(&pConn->resps);
  if (size == 0) {
    tDebug("%s conn %p has %d msg to send", transLabel(pConn->pInst), pConn, size);
    return;
  }

  uv_write_t*    req = allocWReqFromWQ(&pConn->wq, pConn);
  SWReqsWrapper* pWreq = req->data;

  uv_buf_t* pBuf = NULL;
  int32_t   bufNum = 0;
  code = uvBuildToSendData(pConn, &pBuf, &bufNum, &pWreq->node);
  if (code != 0) {
    tError("%s conn %p failed to send data", transLabel(pConn->pInst), pConn);
    return;
  }
  if (bufNum == 0) {
    tDebug("%s conn %p no data to send", transLabel(pConn->pInst), pConn);
    return;
  }

  transRefSrvHandle(pConn);

  if (req == NULL) {
    if (!uv_is_closing((uv_handle_t*)(pConn->pTcp))) {
      tError("conn %p failed to write data, reason:%s", pConn, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      pConn->broken = true;
      freeWReqToWQ(&pConn->wq, req->data);
      taosMemoryFree(pWreq);
      transUnrefSrvHandle(pConn);
      return;
    }
  }
  (void)uv_write(req, (uv_stream_t*)pConn->pTcp, pBuf, bufNum, uvOnSendCb);
}
int32_t uvMayHandleReleaseResp(SSvrRespMsg* pMsg) {
  SSvrConn* pConn = pMsg->pConn;
  int64_t   qid = pMsg->msg.info.qId;
  if (pMsg->msg.msgType == TDMT_SCH_TASK_RELEASE && qid > 0) {
    SSvrRegArg* p = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
    if (p == NULL) {
      tError("%s conn %p already release qid:%ld", transLabel(pConn->pInst), pConn, qid);
      return TSDB_CODE_RPC_NO_STATE;
    } else {
      transFreeMsg(p->msg.pCont);
      taosHashRemove(pConn->pQTable, &qid, sizeof(qid));
    }
  }
  return 0;
}
static void uvStartSendResp(SSvrRespMsg* smsg) {
  // impl
  SSvrConn* pConn = smsg->pConn;
  if (uvMayHandleReleaseResp(smsg) == TSDB_CODE_RPC_NO_STATE) {
    destroySmsg(smsg);
    return;
  }

  transQueuePush(&pConn->resps, &smsg->q);
  uvStartSendRespImpl(smsg);
  return;
}

static FORCE_INLINE void destroySmsg(SSvrRespMsg* smsg) {
  if (smsg == NULL) {
    return;
  }
  transFreeMsg(smsg->msg.pCont);
  taosMemoryFree(smsg);
}
static FORCE_INLINE void destroySmsgWrapper(void* smsg, void* param) { destroySmsg((SSvrRespMsg*)smsg); }

static void destroyAllConn(SWorkThrd* pThrd) {
  tTrace("thread %p destroy all conn ", pThrd);
  while (!QUEUE_IS_EMPTY(&pThrd->conn)) {
    queue* h = QUEUE_HEAD(&pThrd->conn);
    QUEUE_REMOVE(h);
    QUEUE_INIT(h);

    SSvrConn* c = QUEUE_DATA(h, SSvrConn, queue);
    while (c->ref >= 2) {
      transUnrefSrvHandle(c);
    }
    transUnrefSrvHandle(c);
  }
}
void uvWorkerAsyncCb(uv_async_t* handle) {
  SAsyncItem* item = handle->data;
  SWorkThrd*  pThrd = item->pThrd;
  SSvrConn*   conn = NULL;
  queue       wq;

  // batch process to avoid to lock/unlock frequently
  (void)taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  (void)taosThreadMutexUnlock(&item->mtx);

  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* head = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(head);

    SSvrRespMsg* msg = QUEUE_DATA(head, SSvrRespMsg, q);
    if (msg == NULL) {
      tError("unexcept occurred, continue");
      continue;
    }

    // release handle to rpc init
    if (msg->type == Quit || msg->type == Update) {
      (*transAsyncHandle[msg->type])(msg, pThrd);
    } else {
      STransMsg transMsg = msg->msg;

      SExHandle* exh1 = transMsg.info.handle;
      int64_t    refId = transMsg.info.refId;
      msg->seqNum = transMsg.info.seqNum;

      SExHandle* exh2 = transAcquireExHandle(uvGetConnRefOfThrd(pThrd), refId);
      if (exh2 == NULL || exh1 != exh2) {
        tTrace("handle except msg %p, ignore it", exh1);
        (void)transReleaseExHandle(uvGetConnRefOfThrd(pThrd), refId);
        destroySmsg(msg);
        continue;
      }
      msg->pConn = exh1->handle;
      (void)transReleaseExHandle(uvGetConnRefOfThrd(pThrd), refId);
      (*transAsyncHandle[msg->type])(msg, pThrd);
    }
  }
}
static void uvWalkCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}
static void uvFreeCb(uv_handle_t* handle) {
  //
  taosMemoryFree(handle);
}

static void uvAcceptAsyncCb(uv_async_t* async) {
  SServerObj* srv = async->data;
  tDebug("close server port %d", srv->port);
  uv_walk(srv->loop, uvWalkCb, NULL);
}

static void uvShutDownCb(uv_shutdown_t* req, int status) {
  if (status != 0) {
    tDebug("conn failed to shut down:%s", uv_err_name(status));
  }
  uv_close((uv_handle_t*)req->handle, uvDestroyConn);
  taosMemoryFree(req);
}
static void uvWorkDoTask(uv_work_t* req) {
  // doing time-consumeing task
  // only auth conn currently, add more func later
  tTrace("conn %p start to be processed in BG Thread", req->data);
  return;
}

static void uvWorkAfterTask(uv_work_t* req, int status) {
  if (status != 0) {
    tTrace("conn %p failed to processed ", req->data);
  }
  // Done time-consumeing task
  // add more func later
  // this func called in main loop
  tTrace("conn %p already processed ", req->data);
  taosMemoryFree(req);
}

void uvOnAcceptCb(uv_stream_t* stream, int status) {
  if (status == -1) {
    return;
  }
  SServerObj* pObj = container_of(stream, SServerObj, server);

  uv_tcp_t* cli = (uv_tcp_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
  if (cli == NULL) return;

  int err = uv_tcp_init(pObj->loop, cli);
  if (err != 0) {
    tError("failed to create tcp: %s", uv_err_name(err));
    taosMemoryFree(cli);
    return;
  }
  err = uv_accept(stream, (uv_stream_t*)cli);
  if (err == 0) {
#if defined(WINDOWS) || defined(DARWIN)
    if (pObj->numOfWorkerReady < pObj->numOfThreads) {
      tError("worker-threads are not ready for all, need %d instead of %d.", pObj->numOfThreads,
             pObj->numOfWorkerReady);
      uv_close((uv_handle_t*)cli, uvFreeCb);
      return;
    }
#endif

    uv_write_t* wr = (uv_write_t*)taosMemoryMalloc(sizeof(uv_write_t));
    wr->data = cli;
    uv_buf_t buf = uv_buf_init((char*)notify, strlen(notify));

    pObj->workerIdx = (pObj->workerIdx + 1) % pObj->numOfThreads;

    tTrace("new connection accepted by main server, dispatch to %dth worker-thread", pObj->workerIdx);

    (void)uv_write2(wr, (uv_stream_t*)&(pObj->pipe[pObj->workerIdx][0]), &buf, 1, (uv_stream_t*)cli, uvOnPipeWriteCb);
  } else {
    if (!uv_is_closing((uv_handle_t*)cli)) {
      tError("failed to accept tcp: %s", uv_err_name(err));
      uv_close((uv_handle_t*)cli, uvFreeCb);
    } else {
      tError("failed to accept tcp: %s", uv_err_name(err));
      taosMemoryFree(cli);
    }
  }
}
void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf) {
  STUB_RAND_NETWORK_ERR(nread);
  if (nread < 0) {
    if (nread != UV_EOF) {
      tError("read error %s", uv_err_name(nread));
    }
    // TODO(log other failure reason)
    tWarn("failed to create connect:%p, reason: %s", q, uv_err_name(nread));
    taosMemoryFree(buf->base);
    // uv_close((uv_handle_t*)q, NULL);
    return;
  }
  // free memory allocated by
  if (nread != strlen(notify) || strncmp(buf->base, notify, strlen(notify)) != 0) {
    tError("failed to read pip ");
    taosMemoryFree(buf->base);
    uv_close((uv_handle_t*)q, NULL);
  }

  taosMemoryFree(buf->base);

  SWorkThrd* pThrd = q->data;

  uv_pipe_t* pipe = (uv_pipe_t*)q;
  if (!uv_pipe_pending_count(pipe)) {
    tError("No pending count");
    // uv_close((uv_handle_t*)q, NULL);
    return;
  }
  if (pThrd->quit) {
    tWarn("thread already received quit msg, ignore incoming conn");
    // uv_close((uv_handle_t*)q, NULL);
    return;
  }

  SSvrConn* pConn = createConn(pThrd);
  if (pConn == NULL) {
    // uv_close((uv_handle_t*)q, NULL);
    return;
  }

  if (uv_accept(q, (uv_stream_t*)(pConn->pTcp)) == 0) {
    uv_os_fd_t fd;
    (void)uv_fileno((const uv_handle_t*)pConn->pTcp, &fd);
    tTrace("conn %p created, fd:%d", pConn, fd);

    struct sockaddr peername, sockname;
    int             addrlen = sizeof(peername);
    if (0 != uv_tcp_getpeername(pConn->pTcp, (struct sockaddr*)&peername, &addrlen)) {
      tError("conn %p failed to get peer info", pConn);
      transUnrefSrvHandle(pConn);
      return;
    }
    (void)transSockInfo2Str(&peername, pConn->dst);

    addrlen = sizeof(sockname);
    if (0 != uv_tcp_getsockname(pConn->pTcp, (struct sockaddr*)&sockname, &addrlen)) {
      tError("conn %p failed to get local info", pConn);
      transUnrefSrvHandle(pConn);
      return;
    }
    (void)transSockInfo2Str(&sockname, pConn->src);

    struct sockaddr_in addr = *(struct sockaddr_in*)&peername;
    struct sockaddr_in saddr = *(struct sockaddr_in*)&sockname;

    pConn->clientIp = addr.sin_addr.s_addr;
    pConn->serverIp = saddr.sin_addr.s_addr;
    pConn->port = ntohs(addr.sin_port);

    transSetConnOption((uv_tcp_t*)pConn->pTcp, 20);
    (void)uv_read_start((uv_stream_t*)(pConn->pTcp), uvAllocRecvBufferCb, uvOnRecvCb);

  } else {
    tDebug("failed to create new connection");
    transUnrefSrvHandle(pConn);
  }
}

void* transAcceptThread(void* arg) {
  // opt
  setThreadName("trans-accept");
  SServerObj* srv = (SServerObj*)arg;
  (void)uv_run(srv->loop, UV_RUN_DEFAULT);

  return NULL;
}
void uvOnPipeConnectionCb(uv_connect_t* connect, int status) {
  STUB_RAND_NETWORK_ERR(status);
  if (status != 0) {
    return;
  };

  SWorkThrd* pThrd = container_of(connect, SWorkThrd, connect_req);
  (void)uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
}
static int32_t addHandleToWorkloop(SWorkThrd* pThrd, char* pipeName) {
  int32_t code = 0;
  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  if (pThrd->loop == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if ((code = uv_loop_init(pThrd->loop)) != 0) {
    tError("failed to init loop since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

#if defined(WINDOWS) || defined(DARWIN)
  code = uv_pipe_init(pThrd->loop, pThrd->pipe, 1);
  if (code != 0) {
    tError("failed to init pip since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
#else
  code = uv_pipe_init(pThrd->loop, pThrd->pipe, 1);
  if (code != 0) {
    tError("failed to init pip since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  code = uv_pipe_open(pThrd->pipe, pThrd->fd);
  if (code != 0) {
    tError("failed to open pip since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
#endif

  pThrd->pipe->data = pThrd;

  QUEUE_INIT(&pThrd->msg);

  // conn set
  QUEUE_INIT(&pThrd->conn);

  code = transAsyncPoolCreate(pThrd->loop, 8, pThrd, uvWorkerAsyncCb, &pThrd->asyncPool);
  if (code != 0) {
    tError("failed to init async pool since:%s", tstrerror(code));
    return code;
  }
#if defined(WINDOWS) || defined(DARWIN)
  uv_pipe_connect(&pThrd->connect_req, pThrd->pipe, pipeName, uvOnPipeConnectionCb);

#else
  code = uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
  if (code != 0) {
    tError("failed to start read pipe:%s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
#endif
  return 0;
}

static int32_t addHandleToAcceptloop(void* arg) {
  // impl later
  SServerObj* srv = arg;

  int code = 0;
  if ((code = uv_tcp_init(srv->loop, &srv->server)) != 0) {
    tError("failed to init accept server since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  // register an async here to quit server gracefully
  srv->pAcceptAsync = taosMemoryCalloc(1, sizeof(uv_async_t));
  if (srv->pAcceptAsync == NULL) {
    tError("failed to create async since %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return terrno;
  }

  code = uv_async_init(srv->loop, srv->pAcceptAsync, uvAcceptAsyncCb);
  if (code != 0) {
    tError("failed to init async since:%s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  srv->pAcceptAsync->data = srv;

  struct sockaddr_in bind_addr;
  if ((code = uv_ip4_addr("0.0.0.0", srv->port, &bind_addr)) != 0) {
    tError("failed to bind addr since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  if ((code = uv_tcp_bind(&srv->server, (const struct sockaddr*)&bind_addr, 0)) != 0) {
    tError("failed to bind since %s", uv_err_name(code));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  if ((code = uv_listen((uv_stream_t*)&srv->server, 4096 * 2, uvOnAcceptCb)) != 0) {
    tError("failed to listen since %s", uv_err_name(code));
    return TSDB_CODE_RPC_PORT_EADDRINUSE;
  }
  return 0;
}

void* transWorkerThread(void* arg) {
  setThreadName("trans-svr-work");
  SWorkThrd* pThrd = (SWorkThrd*)arg;
  tsEnableRandErr = true;
  (void)uv_run(pThrd->loop, UV_RUN_DEFAULT);

  return NULL;
}
void uvDestroyResp(void* e) {
  SSvrRespMsg* pMsg = QUEUE_DATA(e, SSvrRespMsg, q);
  destroySmsg(pMsg);
}
static FORCE_INLINE SSvrConn* createConn(void* hThrd) {
  int32_t    code = 0;
  SWorkThrd* pThrd = hThrd;
  int32_t    lino;

  SSvrConn* pConn = (SSvrConn*)taosMemoryCalloc(1, sizeof(SSvrConn));
  if (pConn == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  QUEUE_INIT(&pConn->queue);

  if ((code = transQueueInit(&pConn->resps, uvDestroyResp)) != 0) {
    TAOS_CHECK_GOTO(code, &lino, _end);
  }

  if ((code = transInitBuffer(&pConn->readBuf)) != 0) {
    TAOS_CHECK_GOTO(code, &lino, _end);
  }

  // memset(&pConn->regArg, 0, sizeof(pConn->regArg));
  pConn->broken = false;
  pConn->status = ConnNormal;

  SExHandle* exh = taosMemoryMalloc(sizeof(SExHandle));
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  exh->handle = pConn;
  exh->pThrd = pThrd;
  exh->refId = transAddExHandle(uvGetConnRefOfThrd(pThrd), exh);
  if (exh->refId < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, &lino, _end);
  }

  QUEUE_INIT(&exh->q);

  SExHandle* pSelf = transAcquireExHandle(uvGetConnRefOfThrd(pThrd), exh->refId);
  if (pSelf != exh) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _end);
  }

  STrans* pInst = pThrd->pInst;
  pConn->refId = exh->refId;

  QUEUE_INIT(&exh->q);
  transRefSrvHandle(pConn);
  tTrace("%s handle %p, conn %p created, refId:%" PRId64, transLabel(pInst), exh, pConn, pConn->refId);

  pConn->pQTable = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pConn->pQTable == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  code = initWQ(&pConn->wq);
  TAOS_CHECK_GOTO(code, &lino, _end);

  // init client handle
  pConn->pTcp = (uv_tcp_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
  if (pConn->pTcp == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  pConn->bufSize = BUFFER_LIMIT;
  pConn->buf = taosMemoryCalloc(1, sizeof(uv_buf_t));

  code = uv_tcp_init(pThrd->loop, pConn->pTcp);
  if (code != 0) {
    tError("%s failed to create conn since %s" PRId64, transLabel(pInst), uv_strerror(code));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, NULL, _end);
  }
  pConn->pTcp->data = pConn;
  QUEUE_PUSH(&pThrd->conn, &pConn->queue);

  pConn->pInst = pThrd->pInst;
  pConn->hostThrd = pThrd;

  return pConn;
_end:
  if (pConn) {
    transQueueDestroy(&pConn->resps);
    (void)transDestroyBuffer(&pConn->readBuf);
    taosHashCleanup(pConn->pQTable);
    taosMemoryFree(pConn->pTcp);
    destroyWQ(&pConn->wq);
    taosMemoryFree(pConn->buf);
    taosMemoryFree(pConn);
    pConn = NULL;
  }
  tError("%s failed to create conn since %s" PRId64, transLabel(pInst), tstrerror(code));
  return NULL;
}

static FORCE_INLINE void destroyConn(SSvrConn* conn, bool clear) {
  if (conn == NULL) {
    return;
  }

  if (clear) {
    if (!uv_is_closing((uv_handle_t*)conn->pTcp)) {
      tTrace("conn %p to be destroyed", conn);
      uv_close((uv_handle_t*)conn->pTcp, uvDestroyConn);
    }
  }
}

void uvConnDestroyAllState(SSvrConn* p) {
  STrans*   pInst = p->pInst;
  SHashObj* pQTable = p->pQTable;
  if (pQTable == NULL) return;

  void* pIter = taosHashIterate(pQTable, NULL);
  while (pIter) {
    SSvrRegArg* arg = pIter;
    int64_t*    qid = taosHashGetKey(pIter, NULL);
    (pInst->cfp)(pInst->parent, &(arg->msg), NULL);
    tTrace("conn %p broken, notify server app, qid%ld", p, *qid);
    pIter = taosHashIterate(pQTable, pIter);
  }

  taosHashCleanup(pQTable);
  pQTable = NULL;
  return;
}
static void uvDestroyConn(uv_handle_t* handle) {
  SSvrConn* conn = handle->data;

  if (conn == NULL) {
    return;
  }
  SWorkThrd* thrd = conn->hostThrd;

  (void)transReleaseExHandle(uvGetConnRefOfThrd(thrd), conn->refId);
  (void)transRemoveExHandle(uvGetConnRefOfThrd(thrd), conn->refId);

  STrans* pInst = thrd->pInst;
  tDebug("%s conn %p destroy", transLabel(pInst), conn);

  transQueueDestroy(&conn->resps);

  QUEUE_REMOVE(&conn->queue);

  taosMemoryFree(conn->pTcp);

  uvConnDestroyAllState(conn);

  (void)transDestroyBuffer(&conn->readBuf);

  destroyWQ(&conn->wq);
  taosMemoryFree(conn->buf);
  taosMemoryFree(conn);

  if (thrd->quit && QUEUE_IS_EMPTY(&thrd->conn)) {
    tTrace("work thread quit");
    uv_walk(thrd->loop, uvWalkCb, NULL);
  }
}
static void uvPipeListenCb(uv_stream_t* handle, int status) {
  if (status != 0) {
    tError("server failed to init pipe, errmsg: %s", uv_err_name(status));
    return;
  }

  SServerObj* srv = container_of(handle, SServerObj, pipeListen);
  uv_pipe_t*  pipe = &(srv->pipe[srv->numOfWorkerReady][0]);

  int ret = uv_pipe_init(srv->loop, pipe, 1);
  if (ret != 0) {
    tError("trans-svr failed to init pipe, errmsg: %s", uv_err_name(ret));
  }

  ret = uv_accept((uv_stream_t*)&srv->pipeListen, (uv_stream_t*)pipe);
  if (ret != 0) {
    tError("trans-svr failed to accept pipe, errmsg: %s", uv_err_name(ret));
    return;
  }

  ret = uv_is_readable((uv_stream_t*)pipe);
  if (ret != 1) {
    tError("trans-svr failed to check pipe, pip not readable");
    return;
  }
  ret = uv_is_writable((uv_stream_t*)pipe);
  if (ret != 1) {
    tError("trans-svr failed to check pipe, pip not writable");
    return;
  }

  ret = uv_is_closing((uv_handle_t*)pipe);
  if (ret != 0) {
    tError("trans-svr failed to check pipe, pip is closing");
    return;
  }
  srv->numOfWorkerReady++;
}

void* transInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* pInit) {
  int32_t code = 0;

  SServerObj* srv = taosMemoryCalloc(1, sizeof(SServerObj));
  if (srv == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    tError("failed to init server since: %s", tstrerror(code));
    return NULL;
  }

  srv->ip = ip;
  srv->port = port;
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->numOfWorkerReady = 0;
  srv->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  srv->pThreadObj = (SWorkThrd**)taosMemoryCalloc(srv->numOfThreads, sizeof(SWorkThrd*));
  srv->pipe = (uv_pipe_t**)taosMemoryCalloc(srv->numOfThreads, sizeof(uv_pipe_t*));
  if (srv->loop == NULL || srv->pThreadObj == NULL || srv->pipe == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto End;
  }

  code = uv_loop_init(srv->loop);
  if (code != 0) {
    tError("failed to init server since: %s", uv_err_name(code));
    code = TSDB_CODE_THIRDPARTY_ERROR;
    goto End;
  }

  if (false == taosValidIpAndPort(srv->ip, srv->port)) {
    code = TAOS_SYSTEM_ERROR(errno);
    tError("invalid ip/port, %d:%d, reason:%s", srv->ip, srv->port, terrstr());
    goto End;
  }
  char pipeName[PATH_MAX];

#if defined(WINDOWS) || defined(DARWIN)
  int ret = uv_pipe_init(srv->loop, &srv->pipeListen, 0);
  if (ret != 0) {
    tError("failed to init pipe, errmsg: %s", uv_err_name(ret));
    goto End;
  }
#if defined(WINDOWS)
  snprintf(pipeName, sizeof(pipeName), "\\\\?\\pipe\\trans.rpc.%d-%" PRIu64, taosSafeRand(), GetCurrentProcessId());
#elif defined(DARWIN)
  snprintf(pipeName, sizeof(pipeName), "%s%spipe.trans.rpc.%08d-%" PRIu64, tsTempDir, TD_DIRSEP, taosSafeRand(),
           taosGetSelfPthreadId());
#endif

  ret = uv_pipe_bind(&srv->pipeListen, pipeName);
  if (ret != 0) {
    tError("failed to bind pipe, errmsg: %s", uv_err_name(ret));
    goto End;
  }

  ret = uv_listen((uv_stream_t*)&srv->pipeListen, SOMAXCONN, uvPipeListenCb);
  if (ret != 0) {
    tError("failed to listen pipe, errmsg: %s", uv_err_name(ret));
    goto End;
  }

  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrd* thrd = (SWorkThrd*)taosMemoryCalloc(1, sizeof(SWorkThrd));
    thrd->pInst = pInit;
    thrd->quit = false;
    thrd->pInst = pInit;
    thrd->pWhiteList = uvWhiteListCreate();
    if (thrd->pWhiteList == NULL) {
      destroyWorkThrdObj(thrd);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto End;
    }

    srv->pipe[i] = (uv_pipe_t*)taosMemoryCalloc(2, sizeof(uv_pipe_t));
    if (srv->pipe[i] == NULL) {
      destroyWorkThrdObj(thrd);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto End;
    }

    thrd->pipe = &(srv->pipe[i][1]);  // init read
    srv->pThreadObj[i] = thrd;

    if ((code = addHandleToWorkloop(thrd, pipeName)) != 0) {
      goto End;
    }

    int err = taosThreadCreate(&(thrd->thread), NULL, transWorkerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("success to create worker-thread:%d", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread:%d", i);
      goto End;
    }
    thrd->inited = 1;
  }
#else

  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrd* thrd = (SWorkThrd*)taosMemoryCalloc(1, sizeof(SWorkThrd));
    if (thrd == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto End;
    }
    srv->pThreadObj[i] = thrd;

    thrd->pInst = pInit;
    thrd->quit = false;
    thrd->pInst = pInit;
    thrd->pWhiteList = uvWhiteListCreate();
    if (thrd->pWhiteList == NULL) {
      destroyWorkThrdObj(thrd);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto End;
    }

    thrd->connRefMgt = transOpenRefMgt(50000, transDestroyExHandle);
    if (thrd->connRefMgt < 0) {
      code = thrd->connRefMgt;
      goto End;
    }

    srv->pipe[i] = (uv_pipe_t*)taosMemoryCalloc(2, sizeof(uv_pipe_t));
    if (srv->pipe[i] == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto End;
    }

    uv_os_sock_t fds[2];
    if ((code = uv_socketpair(SOCK_STREAM, 0, fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE)) != 0) {
      tError("failed to create pipe, errmsg: %s", uv_err_name(code));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      goto End;
    }

    code = uv_pipe_init(srv->loop, &(srv->pipe[i][0]), 1);
    if (code != 0) {
      tError("failed to init pipe, errmsg: %s", uv_err_name(code));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      goto End;
    }

    code = uv_pipe_open(&(srv->pipe[i][0]), fds[1]);
    if (code != 0) {
      tError("failed to init pipe, errmsg: %s", uv_err_name(code));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      goto End;
    }

    thrd->pipe = &(srv->pipe[i][1]);  // init read
    thrd->fd = fds[0];

    if ((code = addHandleToWorkloop(thrd, pipeName)) != 0) {
      goto End;
    }

    int err = taosThreadCreate(&(thrd->thread), NULL, transWorkerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("success to create worker-thread:%d", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread:%d", i);
      goto End;
    }
  }
#endif

  if ((code = addHandleToAcceptloop(srv)) != 0) {
    goto End;
  }

  code = taosThreadCreate(&srv->thread, NULL, transAcceptThread, (void*)srv);
  if (code == 0) {
    tDebug("success to create accept-thread");
  } else {
    code = TAOS_SYSTEM_ERROR(errno);
    tError("failed  to create accept-thread since %s", tstrerror(code));

    goto End;
    // clear all resource later
  }

  srv->inited = true;
  return srv;
End:
  for (int i = 0; i < srv->numOfThreads; i++) {
    if (srv->pThreadObj[i] != NULL) {
      SWorkThrd* thrd = srv->pThreadObj[i];
      destroyWorkThrd(thrd);
    }
  }
  transCloseServer(srv);
  terrno = code;
  return NULL;
}

void uvHandleQuit(SSvrRespMsg* msg, SWorkThrd* thrd) {
  thrd->quit = true;
  if (QUEUE_IS_EMPTY(&thrd->conn)) {
    uv_walk(thrd->loop, uvWalkCb, NULL);
  } else {
    destroyAllConn(thrd);
  }
  taosMemoryFree(msg);
}
void uvHandleRelease(SSvrRespMsg* msg, SWorkThrd* thrd) {
  return;
  // int32_t   code = 0;
  // SSvrConn* conn = msg->pConn;
  // if (conn->status == ConnAcquire) {
  //   if (!transQueuePush(&conn->resps, msg)) {
  //     return;
  //   }
  //   uvStartSendRespImpl(msg);
  //   return;
  // } else if (conn->status == ConnRelease || conn->status == ConnNormal) {
  //   tDebug("%s conn %p already released, ignore release-msg", transLabel(thrd->pInst), conn);
  // }

  // destroySmsg(msg);
}
void uvHandleResp(SSvrRespMsg* msg, SWorkThrd* thrd) {
  // send msg to client
  tDebug("%s conn %p start to send resp (2/2)", transLabel(thrd->pInst), msg->pConn);
  uvStartSendResp(msg);
}

int32_t uvHandleStateReq(SSvrRespMsg* msg) {
  int32_t   code = 0;
  SSvrConn* conn = msg->pConn;
  tDebug("%s conn %p start to register brokenlink callback, qid:%" PRId64 "", transLabel(conn->pInst), conn,
         msg->msg.info.qId);

  SSvrRegArg  arg = {.notifyCount = 0, .init = 1, .msg = msg->msg};
  SSvrRegArg* p = taosHashGet(conn->pQTable, &msg->msg.info.qId, sizeof(msg->msg.info.qId));
  if (p != NULL) {
    transFreeMsg(p->msg.pCont);
  }

  code = taosHashPut(conn->pQTable, &msg->msg.info.qId, sizeof(msg->msg.info.qId), &arg, sizeof(arg));
  if (code == 0) tDebug("conn %p register brokenlink callback succ", conn);
  return code;
}
void uvHandleRegister(SSvrRespMsg* msg, SWorkThrd* thrd) {
  SSvrConn* conn = msg->pConn;
  tDebug("%s conn %p register brokenlink callback", transLabel(thrd->pInst), conn);
  int32_t code = uvHandleStateReq(msg);
  taosMemoryFree(msg);
}

void uvHandleUpdate(SSvrRespMsg* msg, SWorkThrd* thrd) {
  SUpdateIpWhite* req = msg->arg;
  if (req == NULL) {
    tDebug("ip-white-list disable on trans");
    thrd->enableIpWhiteList = 0;
    taosMemoryFree(msg);
    return;
  }
  int32_t code = 0;
  for (int i = 0; i < req->numOfUser; i++) {
    SUpdateUserIpWhite* pUser = &req->pUserIpWhite[i];

    int32_t sz = pUser->numOfRange * sizeof(SIpV4Range);

    SIpWhiteList* pList = taosMemoryCalloc(1, sz + sizeof(SIpWhiteList));
    if (pList == NULL) {
      tError("failed to create ip-white-list since %s", tstrerror(code));
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
    pList->num = pUser->numOfRange;
    memcpy(pList->pIpRange, pUser->pIpRanges, sz);
    code = uvWhiteListAdd(thrd->pWhiteList, pUser->user, pList, pUser->ver);
    if (code != 0) {
      break;
    }
  }

  if (code == 0) {
    thrd->pWhiteList->ver = req->ver;
    thrd->enableIpWhiteList = 1;
  } else {
    tError("failed to update ip-white-list since %s", tstrerror(code));
  }
  tFreeSUpdateIpWhiteReq(req);
  taosMemoryFree(req);
  taosMemoryFree(msg);
}

void destroyWorkThrdObj(SWorkThrd* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  transAsyncPoolDestroy(pThrd->asyncPool);
  uvWhiteListDestroy(pThrd->pWhiteList);
  taosMemoryFree(pThrd->loop);
  taosMemoryFree(pThrd);
}
void destroyWorkThrd(SWorkThrd* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  (void)taosThreadJoin(pThrd->thread, NULL);
  SRV_RELEASE_UV(pThrd->loop);
  TRANS_DESTROY_ASYNC_POOL_MSG(pThrd->asyncPool, SSvrRespMsg, destroySmsgWrapper, NULL);
  transAsyncPoolDestroy(pThrd->asyncPool);

  uvWhiteListDestroy(pThrd->pWhiteList);

  taosMemoryFree(pThrd->loop);
  taosMemoryFree(pThrd);
}
void sendQuitToWorkThrd(SWorkThrd* pThrd) {
  SSvrRespMsg* msg = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  msg->type = Quit;
  tDebug("server send quit msg to work thread");
  (void)transAsyncSend(pThrd->asyncPool, &msg->q);
}

void transCloseServer(void* arg) {
  // impl later
  SServerObj* srv = arg;

  if (srv->inited) {
    tDebug("send quit msg to accept thread");
    (void)uv_async_send(srv->pAcceptAsync);
    (void)taosThreadJoin(srv->thread, NULL);

    SRV_RELEASE_UV(srv->loop);
    for (int i = 0; i < srv->numOfThreads; i++) {
      destroyWorkThrd(srv->pThreadObj[i]);
    }
  } else {
    SRV_RELEASE_UV(srv->loop);
  }

  taosMemoryFree(srv->pThreadObj);
  taosMemoryFree(srv->pAcceptAsync);
  taosMemoryFree(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    if (srv->pipe[i] != NULL) {
      taosMemoryFree(srv->pipe[i]);
    }
  }
  taosMemoryFree(srv->pipe);

  taosMemoryFree(srv);
}

void transRefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  SSvrConn* pConn = handle;
  pConn->ref++;
  tTrace("conn %p ref count:%d", pConn, pConn->ref);
}

void transUnrefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  SSvrConn* pConn = handle;
  pConn->ref--;
  tTrace("conn %p ref count:%d", pConn, pConn->ref);
  if (pConn->ref == 0) {
    destroyConn((SSvrConn*)handle, true);
  }
}

int32_t transReleaseSrvHandle(void* handle) {
  int32_t         code = 0;
  SRpcHandleInfo* info = handle;
  SExHandle*      exh = info->handle;
  int64_t         qId = info->qId;
  int64_t         refId = info->refId;

  ASYNC_CHECK_HANDLE(info->refIdMgt, refId, exh);

  SWorkThrd* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  STransMsg tmsg = {.msgType = TDMT_SCH_TASK_RELEASE,
                    .code = 0,
                    .info.handle = exh,
                    .info.ahandle = NULL,
                    .info.refId = refId,
                    .info.qId = qId,
                    .info.traceId = info->traceId};

  SSvrRespMsg* m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }

  m->msg = tmsg;
  m->type = Normal;

  tDebug("%s conn %p start to send %s, qid:%" PRId64 "", transLabel(pThrd->pInst), exh->handle, TMSG_INFO(tmsg.msgType),
         qId);
  if ((code = transAsyncSend(pThrd->asyncPool, &m->q)) != 0) {
    destroySmsg(m);
    (void)transReleaseExHandle(info->refIdMgt, refId);
    return code;
  }

  (void)transReleaseExHandle(info->refIdMgt, refId);
  return 0;
_return1:
  tDebug("handle %p failed to send to release handle", exh);
  (void)transReleaseExHandle(info->refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to send to release handle", exh);
  return code;
}

int32_t transSendResponse(const STransMsg* msg) {
  int32_t code = 0;

  if (msg->info.noResp) {
    rpcFreeCont(msg->pCont);
    tTrace("no need send resp");
    return 0;
  }
  SExHandle* exh = msg->info.handle;

  if (exh == NULL) {
    rpcFreeCont(msg->pCont);
    return 0;
  }
  int64_t refId = msg->info.refId;
  ASYNC_CHECK_HANDLE(msg->info.refIdMgt, refId, exh);

  STransMsg tmsg = *msg;
  tmsg.info.refId = refId;
  if (tmsg.info.qId == 0) {
    tmsg.msgType = msg->info.msgType + 1;
  }

  SWorkThrd* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrRespMsg* m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }
  m->msg = tmsg;

  m->type = Normal;

  STraceId* trace = (STraceId*)&msg->info.traceId;
  tGDebug("conn %p start to send resp (1/2)", exh->handle);
  if ((code = transAsyncSend(pThrd->asyncPool, &m->q)) != 0) {
    destroySmsg(m);
    (void)transReleaseExHandle(msg->info.refIdMgt, refId);
    return code;
  }

  (void)transReleaseExHandle(msg->info.refIdMgt, refId);
  return 0;

_return1:
  tDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  (void)transReleaseExHandle(msg->info.refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  return code;
}
int32_t transRegisterMsg(const STransMsg* msg) {
  int32_t code = 0;

  SExHandle* exh = msg->info.handle;
  int64_t    refId = msg->info.refId;
  ASYNC_CHECK_HANDLE(msg->info.refIdMgt, refId, exh);

  STransMsg tmsg = *msg;
  tmsg.info.noResp = 1;

  tmsg.info.qId = msg->info.qId;
  tmsg.info.seqNum = msg->info.seqNum;
  tmsg.info.refId = refId;
  tmsg.info.refIdMgt = msg->info.refIdMgt;

  SWorkThrd* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrRespMsg* m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }

  m->msg = tmsg;
  m->type = Register;

  STrans* pInst = pThrd->pInst;
  tDebug("%s conn %p start to register brokenlink callback", transLabel(pInst), exh->handle);
  if ((code = transAsyncSend(pThrd->asyncPool, &m->q)) != 0) {
    destroySmsg(m);
    (void)transReleaseExHandle(msg->info.refIdMgt, refId);
    return code;
  }

  (void)transReleaseExHandle(msg->info.refIdMgt, refId);
  return 0;

_return1:
  tDebug("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  (void)transReleaseExHandle(msg->info.refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  return code;
}

int32_t transSetIpWhiteList(void* thandle, void* arg, FilteFunc* func) {
  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)thandle);
  if (pInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  int32_t code = 0;

  tDebug("ip-white-list update on rpc");
  SServerObj* svrObj = pInst->tcphandle;
  for (int i = 0; i < svrObj->numOfThreads; i++) {
    SWorkThrd* pThrd = svrObj->pThreadObj[i];

    SSvrRespMsg* msg = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
    if (msg == NULL) {
      code = terrno;
      break;
    }

    SUpdateIpWhite* pReq = NULL;
    code = cloneSUpdateIpWhiteReq((SUpdateIpWhite*)arg, &pReq);
    if (code != 0) {
      taosMemoryFree(msg);
      break;
    }

    msg->type = Update;
    msg->arg = pReq;

    if ((code = transAsyncSend(pThrd->asyncPool, &msg->q)) != 0) {
      code = (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
      tFreeSUpdateIpWhiteReq(pReq);
      taosMemoryFree(pReq);
      taosMemoryFree(msg);
      break;
    }
  }
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)thandle);

  if (code != 0) {
    tError("ip-white-list update failed since %s", tstrerror(code));
  }
  return code;
}
