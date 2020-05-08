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

#include "os.h"
#include "tidpool.h"
#include "tmd5.h"
#include "tmempool.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "lz4.h"
#include "taoserror.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taosmsg.h"
#include "trpc.h"
#include "hash.h"
#include "rpcLog.h"
#include "rpcUdp.h"
#include "rpcCache.h"
#include "rpcTcp.h"
#include "rpcHead.h"

#define RPC_MSG_OVERHEAD (sizeof(SRpcReqContext) + sizeof(SRpcHead) + sizeof(SRpcDigest)) 
#define rpcHeadFromCont(cont) ((SRpcHead *) (cont - sizeof(SRpcHead)))
#define rpcContFromHead(msg) (msg + sizeof(SRpcHead))
#define rpcMsgLenFromCont(contLen) (contLen + sizeof(SRpcHead))
#define rpcContLenFromMsg(msgLen) (msgLen - sizeof(SRpcHead))
#define rpcIsReq(type) (type & 1U)

typedef struct {
  int      sessions;     // number of sessions allowed
  int      numOfThreads; // number of threads to process incoming messages
  int      idleTime;     // milliseconds;
  uint16_t localPort;
  int8_t   connType;
  int      index;        // for UDP server only, round robin for multiple threads
  char     label[12];

  char     user[TSDB_UNI_LEN];   // meter ID
  char     spi;                  // security parameter index
  char     encrypt;              // encrypt algorithm
  char     secret[TSDB_KEY_LEN]; // secret for the link
  char     ckey[TSDB_KEY_LEN];   // ciphering key 

  void   (*cfp)(SRpcMsg *);
  int    (*afp)(char *user, char *spi, char *encrypt, char *secret, char *ckey); 
  void   (*ufp)(void *ahandle, SRpcIpSet *pIpSet);

  void     *idPool;   // handle to ID pool
  void     *tmrCtrl;  // handle to timer
  void     *hash;     // handle returned by hash utility
  void     *tcphandle;// returned handle from TCP initialization
  void     *udphandle;// returned handle from UDP initialization
  void     *pCache;   // connection cache
  pthread_mutex_t  mutex;
  struct SRpcConn *connList;  // connection list
} SRpcInfo;

typedef struct {
  SRpcInfo *pRpc;       // associated SRpcInfo
  SRpcIpSet ipSet;      // ip list provided by app
  void     *ahandle;    // handle provided by app
  char      msgType;    // message type
  uint8_t  *pCont;      // content provided by app
  int32_t   contLen;    // content length
  int32_t   code;       // error code
  int16_t   numOfTry;   // number of try for different servers
  int8_t    oldInUse;   // server IP inUse passed by app
  int8_t    redirect;   // flag to indicate redirect
  int8_t    connType;   // connection type
  SRpcMsg  *pRsp;       // for synchronous API
  tsem_t   *pSem;       // for synchronous API
  SRpcIpSet *pSet;      // for synchronous API 
  char      msg[0];     // RpcHead starts from here
} SRpcReqContext;

typedef struct SRpcConn {
  int       sid;     // session ID
  uint32_t  ownId;   // own link ID
  uint32_t  peerId;  // peer link ID
  char      user[TSDB_UNI_LEN]; // user ID for the link
  char      spi;     // security parameter index
  char      encrypt; // encryption, 0:1 
  char      secret[TSDB_KEY_LEN]; // secret for the link
  char      ckey[TSDB_KEY_LEN];   // ciphering key 
  char      secured;              // if set to 1, no authentication
  uint16_t  localPort;      // for UDP only
  uint32_t  linkUid;        // connection unique ID assigned by client
  uint32_t  peerIp;         // peer IP
  uint16_t  peerPort;       // peer port
  char      peerFqdn[TSDB_FQDN_LEN]; // peer FQDN or ip string
  uint16_t  tranId;         // outgoing transcation ID, for build message
  uint16_t  outTranId;      // outgoing transcation ID
  uint16_t  inTranId;       // transcation ID for incoming msg
  uint8_t   outType;        // message type for outgoing request
  char      inType;         // message type for incoming request  
  void     *chandle;  // handle passed by TCP/UDP connection layer
  void     *ahandle;  // handle provided by upper app layter
  int       retry;    // number of retry for sending request
  int       tretry;   // total retry
  void     *pTimer;   // retry timer to monitor the response
  void     *pIdleTimer; // idle timer
  char     *pRspMsg;    // response message including header
  int       rspMsgLen;  // response messag length
  char     *pReqMsg;    // request message including header
  int       reqMsgLen;  // request message length
  SRpcInfo *pRpc;       // the associated SRpcInfo
  int8_t    connType;   // connection type
  int64_t   lockedBy;   // lock for connection
  SRpcReqContext *pContext; // request context
} SRpcConn;

int tsRpcMaxUdpSize = 15000;  // bytes
int tsRpcProgressTime = 10;  // milliseocnds

// not configurable
int tsRpcMaxRetry;
int tsRpcHeadSize;
int tsRpcOverhead;

// server:0 client:1  tcp:2 udp:0
#define RPC_CONN_UDPS   0
#define RPC_CONN_UDPC   1
#define RPC_CONN_TCPS   2
#define RPC_CONN_TCPC   3
#define RPC_CONN_TCP    2

void *(*taosInitConn[])(uint32_t ip, uint16_t port, char *label, int threads, void *fp, void *shandle) = {
    taosInitUdpConnection,
    taosInitUdpConnection,
    taosInitTcpServer, 
    taosInitTcpClient
};

void (*taosCleanUpConn[])(void *thandle) = {
    taosCleanUpUdpConnection, 
    taosCleanUpUdpConnection, 
    taosCleanUpTcpServer,
    taosCleanUpTcpClient
};

int (*taosSendData[])(uint32_t ip, uint16_t port, void *data, int len, void *chandle) = {
    taosSendUdpData, 
    taosSendUdpData, 
    taosSendTcpData, 
    taosSendTcpData
};

void *(*taosOpenConn[])(void *shandle, void *thandle, uint32_t ip, uint16_t port) = {
    taosOpenUdpConnection,
    taosOpenUdpConnection,
    NULL,
    taosOpenTcpClientConnection,
};

void (*taosCloseConn[])(void *chandle) = {
    NULL, 
    NULL, 
    taosCloseTcpConnection, 
    taosCloseTcpConnection
};

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerFqdn, uint16_t peerPort, int8_t connType);
static void      rpcCloseConn(void *thandle);
static SRpcConn *rpcSetupConnToServer(SRpcReqContext *pContext);
static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc);
static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, SRecvInfo *pRecv);
static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, SRecvInfo *pRecv);

static void  rpcSendReqToServer(SRpcInfo *pRpc, SRpcReqContext *pContext);
static void  rpcSendQuickRsp(SRpcConn *pConn, int32_t code);
static void  rpcSendErrorMsgToPeer(SRecvInfo *pRecv, int32_t code);
static void  rpcSendMsgToPeer(SRpcConn *pConn, void *data, int dataLen);
static void  rpcSendReqHead(SRpcConn *pConn);

static void *rpcProcessMsgFromPeer(SRecvInfo *pRecv);
static void  rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHead *pHead);
static void  rpcProcessConnError(void *param, void *id);
static void  rpcProcessRetryTimer(void *, void *);
static void  rpcProcessIdleTimer(void *param, void *tmrId);
static void  rpcProcessProgressTimer(void *param, void *tmrId);

static void  rpcFreeMsg(void *msg);
static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen);
static SRpcHead *rpcDecompressRpcMsg(SRpcHead *pHead);
static int   rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen);
static int   rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen);
static void  rpcLockConn(SRpcConn *pConn);
static void  rpcUnlockConn(SRpcConn *pConn);

void *rpcOpen(const SRpcInit *pInit) {
  SRpcInfo *pRpc;

  tsRpcMaxRetry = tsRpcMaxTime * 1000 / tsRpcProgressTime;
  tsRpcHeadSize = RPC_MSG_OVERHEAD; 
  tsRpcOverhead = sizeof(SRpcReqContext);

  pRpc = (SRpcInfo *)calloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) return NULL;

  if(pInit->label) strcpy(pRpc->label, pInit->label);
  pRpc->connType = pInit->connType;
  pRpc->idleTime = pInit->idleTime;
  pRpc->numOfThreads = pInit->numOfThreads>TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS:pInit->numOfThreads;
  pRpc->localPort = pInit->localPort;
  pRpc->afp = pInit->afp;
  pRpc->sessions = pInit->sessions+1;
  if (pInit->user) strcpy(pRpc->user, pInit->user);
  if (pInit->secret) strcpy(pRpc->secret, pInit->secret);
  if (pInit->ckey) strcpy(pRpc->ckey, pInit->ckey);
  pRpc->spi = pInit->spi;
  pRpc->ufp = pInit->ufp;
  pRpc->cfp = pInit->cfp;
  pRpc->afp = pInit->afp;

  size_t size = sizeof(SRpcConn) * pRpc->sessions;
  pRpc->connList = (SRpcConn *)calloc(1, size);
  if (pRpc->connList == NULL) {
    tError("%s failed to allocate memory for taos connections, size:%d", pRpc->label, size);
    rpcClose(pRpc);
    return NULL;
  }

  pRpc->idPool = taosInitIdPool(pRpc->sessions-1);
  if (pRpc->idPool == NULL) {
    tError("%s failed to init ID pool", pRpc->label);
    rpcClose(pRpc);
    return NULL;
  }

  pRpc->tmrCtrl = taosTmrInit(pRpc->sessions*2 + 1, 50, 10000, pRpc->label);
  if (pRpc->tmrCtrl == NULL) {
    tError("%s failed to init timers", pRpc->label);
    rpcClose(pRpc);
    return NULL;
  }

  if (pRpc->connType == TAOS_CONN_SERVER) {
    pRpc->hash = taosHashInit(pRpc->sessions, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true);
    if (pRpc->hash == NULL) {
      tError("%s failed to init string hash", pRpc->label);
      rpcClose(pRpc);
      return NULL;
    }
  } else {
    pRpc->pCache = rpcOpenConnCache(pRpc->sessions, rpcCloseConn, pRpc->tmrCtrl, pRpc->idleTime); 
    if ( pRpc->pCache == NULL ) {
      tError("%s failed to init connection cache", pRpc->label);
      rpcClose(pRpc);
      return NULL;
    }
  }

  pthread_mutex_init(&pRpc->mutex, NULL);

  pRpc->tcphandle = (*taosInitConn[pRpc->connType|RPC_CONN_TCP])(0, pRpc->localPort, pRpc->label, 
                                                  pRpc->numOfThreads, rpcProcessMsgFromPeer, pRpc);
  pRpc->udphandle = (*taosInitConn[pRpc->connType])(0, pRpc->localPort, pRpc->label, 
                                                  pRpc->numOfThreads, rpcProcessMsgFromPeer, pRpc);

  if (pRpc->tcphandle == NULL || pRpc->udphandle == NULL) {
    tError("%s failed to init network, port:%d", pRpc->label, pRpc->localPort);
    rpcClose(pRpc);
    return NULL;
  }

  tTrace("%s RPC is openned, numOfThreads:%d", pRpc->label, pRpc->numOfThreads);

  return pRpc;
}

void rpcClose(void *param) {
  SRpcInfo *pRpc = (SRpcInfo *)param;

  for (int i = 0; i < pRpc->sessions; ++i) {
    if (pRpc->connList && pRpc->connList[i].user[0]) {
      rpcCloseConn((void *)(pRpc->connList + i));
    }
  }

  (*taosCleanUpConn[pRpc->connType | RPC_CONN_TCP])(pRpc->tcphandle);
  (*taosCleanUpConn[pRpc->connType])(pRpc->udphandle);

  taosHashCleanup(pRpc->hash);
  taosTmrCleanUp(pRpc->tmrCtrl);
  taosIdPoolCleanUp(pRpc->idPool);
  rpcCloseConnCache(pRpc->pCache);

  tfree(pRpc->connList);
  pthread_mutex_destroy(&pRpc->mutex);
  tTrace("%s RPC is closed", pRpc->label);
  tfree(pRpc);
}

void *rpcMallocCont(int contLen) {
  int size = contLen + RPC_MSG_OVERHEAD;

  char *start = (char *)calloc(1, (size_t)size);
  if (start == NULL) {
    tError("failed to malloc msg, size:%d", size);
    return NULL;
  }

  return start + sizeof(SRpcReqContext) + sizeof(SRpcHead);
}

void rpcFreeCont(void *cont) {
  if ( cont ) {
    char *temp = ((char *)cont) - sizeof(SRpcHead) - sizeof(SRpcReqContext);
    free(temp);
  }
}

void *rpcReallocCont(void *ptr, int contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  char *start = ((char *)ptr) - sizeof(SRpcReqContext) - sizeof(SRpcHead);
  if (contLen == 0 ) {
    free(start); 
    return NULL;
  }

  int size = contLen + RPC_MSG_OVERHEAD;
  start = realloc(start, size);
  if (start == NULL) {
    tError("failed to realloc cont, size:%d", size);
    return NULL;
  } 

  return start + sizeof(SRpcReqContext) + sizeof(SRpcHead);
}

void rpcSendRequest(void *shandle, const SRpcIpSet *pIpSet, const SRpcMsg *pMsg) {
  SRpcInfo       *pRpc = (SRpcInfo *)shandle;
  SRpcReqContext *pContext;

  int contLen = rpcCompressRpcMsg(pMsg->pCont, pMsg->contLen);
  pContext = (SRpcReqContext *) (pMsg->pCont-sizeof(SRpcHead)-sizeof(SRpcReqContext));
  pContext->ahandle = pMsg->handle;
  pContext->pRpc = (SRpcInfo *)shandle;
  pContext->ipSet = *pIpSet;
  pContext->contLen = contLen;
  pContext->pCont = pMsg->pCont;
  pContext->msgType = pMsg->msgType;
  pContext->oldInUse = pIpSet->inUse;

  pContext->connType = RPC_CONN_UDPC; 
  if (contLen > tsRpcMaxUdpSize) pContext->connType = RPC_CONN_TCPC;

  // connection type is application specific. 
  // for TDengine, all the query, show commands shall have TCP connection
  char type = pMsg->msgType;
  if (type == TSDB_MSG_TYPE_QUERY || type == TSDB_MSG_TYPE_CM_RETRIEVE || type == TSDB_MSG_TYPE_FETCH ||
      type == TSDB_MSG_TYPE_CM_STABLE_VGROUP || type == TSDB_MSG_TYPE_CM_TABLES_META ||
      type == TSDB_MSG_TYPE_CM_SHOW )
    pContext->connType = RPC_CONN_TCPC;
  
  rpcSendReqToServer(pRpc, pContext);

  return;
}

void rpcSendResponse(const SRpcMsg *pRsp) {
  int        msgLen = 0;
  SRpcConn  *pConn = (SRpcConn *)pRsp->handle;
  SRpcInfo  *pRpc = pConn->pRpc;

  SRpcMsg    rpcMsg = *pRsp;
  SRpcMsg   *pMsg = &rpcMsg;

  if ( pMsg->pCont == NULL ) {
    pMsg->pCont = rpcMallocCont(0);
    pMsg->contLen = 0;
  }

  SRpcHead  *pHead = rpcHeadFromCont(pMsg->pCont);
  char      *msg = (char *)pHead;

  pMsg->contLen = rpcCompressRpcMsg(pMsg->pCont, pMsg->contLen);
  msgLen = rpcMsgLenFromCont(pMsg->contLen);

  rpcLockConn(pConn);

  if ( pConn->inType == 0 || pConn->user[0] == 0 ) {
    tTrace("%s %p, connection is already released, rsp wont be sent", pRpc->label, pConn);
    rpcUnlockConn(pConn);
    return;
  }

  // set msg header
  pHead->version = 1;
  pHead->msgType = pConn->inType+1;
  pHead->spi = pConn->spi;
  pHead->encrypt = pConn->encrypt;
  pHead->tranId = pConn->inTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  pHead->port = htons(pConn->localPort);
  pHead->code = htonl(pMsg->code);

  // set pConn parameters
  pConn->inType = 0;

  // response message is released until new response is sent
  rpcFreeMsg(pConn->pRspMsg); 
  pConn->pRspMsg = msg;
  pConn->rspMsgLen = msgLen;
  if (pMsg->code == TSDB_CODE_ACTION_IN_PROGRESS) pConn->inTranId--;

  rpcUnlockConn(pConn);

  taosTmrStopA(&pConn->pTimer);
  // taosTmrReset(rpcProcessIdleTimer, pRpc->idleTime, pConn, pRpc->tmrCtrl, &pConn->pIdleTimer);
  rpcSendMsgToPeer(pConn, msg, msgLen);
  pConn->secured = 1; // connection shall be secured

  return;
}

void rpcSendRedirectRsp(void *thandle, const SRpcIpSet *pIpSet) {
  SRpcMsg  rpcMsg;
  
  rpcMsg.contLen = sizeof(SRpcIpSet);
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  if (rpcMsg.pCont == NULL) return;

  memcpy(rpcMsg.pCont, pIpSet, sizeof(SRpcIpSet));

  rpcMsg.code = TSDB_CODE_REDIRECT;
  rpcMsg.handle = thandle;

  rpcSendResponse(&rpcMsg);

  return;
}

int rpcGetConnInfo(void *thandle, SRpcConnInfo *pInfo) {
  SRpcConn  *pConn = (SRpcConn *)thandle;
  if (pConn->user[0] == 0) return -1;

  pInfo->clientIp = pConn->peerIp;
  pInfo->clientPort = pConn->peerPort;
  // pInfo->serverIp = pConn->destIp;

  strcpy(pInfo->user, pConn->user);
  return 0;
}

void rpcSendRecv(void *shandle, SRpcIpSet *pIpSet, const SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SRpcReqContext *pContext;
  pContext = (SRpcReqContext *) (pMsg->pCont-sizeof(SRpcHead)-sizeof(SRpcReqContext));

  memset(pRsp, 0, sizeof(SRpcMsg));
  
  tsem_t   sem;        
  tsem_init(&sem, 0, 0);
  pContext->pSem = &sem;
  pContext->pRsp = pRsp;
  pContext->pSet = pIpSet;

  rpcSendRequest(shandle, pIpSet, pMsg);

  tsem_wait(&sem);
  tsem_destroy(&sem);

  return;
}

static void rpcFreeMsg(void *msg) {
  if ( msg ) {
    char *temp = (char *)msg - sizeof(SRpcReqContext);
    free(temp);
  }
}

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerFqdn, uint16_t peerPort, int8_t connType) {
  SRpcConn *pConn;

  uint32_t peerIp = taosGetIpFromFqdn(peerFqdn);
  if (peerIp == -1) {
    tError("%s, failed to resolve FQDN:%s", pRpc->label, peerFqdn); 
    return NULL;
  }

  pConn = rpcAllocateClientConn(pRpc); 

  if (pConn) { 
    strcpy(pConn->peerFqdn, peerFqdn);
    pConn->peerIp = peerIp;
    pConn->peerPort = peerPort;
    strcpy(pConn->user, pRpc->user);
    pConn->connType = connType;

    if (taosOpenConn[connType]) {
      void *shandle = (connType & RPC_CONN_TCP)? pRpc->tcphandle:pRpc->udphandle;
      pConn->chandle = (*taosOpenConn[connType])(shandle, pConn, pConn->peerIp, pConn->peerPort);
      if (pConn->chandle) {
        tTrace("%s %p, rpc connection is set up, sid:%d id:%s %s:%hu connType:%d", pRpc->label, 
                pConn, pConn->sid, pRpc->user, peerFqdn, pConn->peerPort, pConn->connType);
      } else {
        tError("%s %p, failed to set up connection to %s:%hu", pRpc->label, pConn, peerFqdn, pConn->peerPort);
        terrno = TSDB_CODE_NETWORK_UNAVAIL;
        rpcCloseConn(pConn);
        pConn = NULL;
      }
    }
  }

  return pConn;
}

static void rpcCloseConn(void *thandle) {
  SRpcConn *pConn = (SRpcConn *)thandle;
  SRpcInfo *pRpc = pConn->pRpc;
  if (pConn->user[0] == 0) return;

  rpcLockConn(pConn);

  if (pConn->user[0] == 0) {
    rpcUnlockConn(pConn);
    return;
  }

  pConn->user[0] = 0;
  if (taosCloseConn[pConn->connType]) (*taosCloseConn[pConn->connType])(pConn->chandle);

  taosTmrStopA(&pConn->pTimer);
  taosTmrStopA(&pConn->pIdleTimer);

  if ( pRpc->connType == TAOS_CONN_SERVER) {
    char hashstr[40] = {0};
    size_t size = sprintf(hashstr, "%x:%x:%x:%d", pConn->peerIp, pConn->linkUid, pConn->peerId, pConn->connType);
    taosHashRemove(pRpc->hash, hashstr, size);
  
    rpcFreeMsg(pConn->pRspMsg); // it may have a response msg saved, but not request msg
    pConn->pRspMsg = NULL;
    pConn->inType = 0;
    pConn->inTranId = 0;
  } else {
    pConn->outType = 0;
    pConn->outTranId = 0;
    pConn->pReqMsg = NULL;
  }

  taosFreeId(pRpc->idPool, pConn->sid);
  pConn->pContext = NULL;

  tTrace("%s %p, rpc connection is closed", pRpc->label, pConn);

  rpcUnlockConn(pConn);
}

static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc) {
  SRpcConn *pConn = NULL;

  int sid = taosAllocateId(pRpc->idPool);
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_MAX_SESSIONS;
  } else {
    pConn = pRpc->connList + sid;
    memset(pConn, 0, sizeof(SRpcConn));

    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    pConn->linkUid = (uint32_t)((int64_t)pConn + (int64_t)getpid());
    pConn->spi = pRpc->spi;
    pConn->encrypt = pRpc->encrypt;
    if (pConn->spi) memcpy(pConn->secret, pRpc->secret, TSDB_KEY_LEN);
  }

  return pConn;
}

static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, SRecvInfo *pRecv) {
  SRpcConn *pConn = NULL;
  char      hashstr[40] = {0};
  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  size_t size = sprintf(hashstr, "%x:%x:%x:%d", pRecv->ip, pHead->linkUid, pHead->sourceId, pRecv->connType);
 
  // check if it is already allocated
  SRpcConn **ppConn = (SRpcConn **)(taosHashGet(pRpc->hash, hashstr, size));
  if (ppConn) pConn = *ppConn;
  if (pConn) return pConn;

  int sid = taosAllocateId(pRpc->idPool);
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_MAX_SESSIONS;
  } else {
    pConn = pRpc->connList + sid;
    memset(pConn, 0, sizeof(SRpcConn));
    memcpy(pConn->user, pHead->user, tListLen(pConn->user));
    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    pConn->linkUid = pHead->linkUid;
    if (pRpc->afp) {
      terrno = (*pRpc->afp)(pConn->user, &pConn->spi, &pConn->encrypt, pConn->secret, pConn->ckey);
      if (terrno != 0) {
        tWarn("%s %p, user not there or server not ready", pRpc->label, pConn);
        taosFreeId(pRpc->idPool, sid);   // sid shall be released
        pConn = NULL;
      }
    }
  }      

  if (pConn) {
    if (pRecv->connType == RPC_CONN_UDPS && pRpc->numOfThreads > 1) {
      // UDP server, assign to new connection
      pRpc->index = (pRpc->index+1) % pRpc->numOfThreads;
      pConn->localPort = (pRpc->localPort + pRpc->index);
    }
  
    taosHashPut(pRpc->hash, hashstr, size, (char *)&pConn, POINTER_BYTES);
    tTrace("%s %p, rpc connection is allocated, sid:%d id:%s port:%u", 
           pRpc->label, pConn, sid, pConn->user, pConn->localPort);
  }

  return pConn;
}

static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, SRecvInfo *pRecv) {
  SRpcConn *pConn = NULL;  
  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  if (sid) {
    pConn = pRpc->connList + sid;
    if (pConn->user[0] == 0) pConn = NULL;
  } 

  if (pConn == NULL) { 
    if (pRpc->connType == TAOS_CONN_SERVER) {
      pConn = rpcAllocateServerConn(pRpc, pRecv);
    } else {
      terrno = TSDB_CODE_UNEXPECTED_RESPONSE;
    }
  }

  if (pConn) {
    if (pConn->linkUid != pHead->linkUid) {
      tTrace("%s %p, linkUid:0x%x not matched, received:0x%x", pRpc->label, pConn, pConn->linkUid, pHead->linkUid);
      terrno = TSDB_CODE_MISMATCHED_METER_ID;
      pConn = NULL;
    }
  }

  return pConn;
}

static SRpcConn *rpcSetupConnToServer(SRpcReqContext *pContext) {
  SRpcConn   *pConn;
  SRpcInfo   *pRpc = pContext->pRpc;
  SRpcIpSet  *pIpSet = &pContext->ipSet;

  pConn = rpcGetConnFromCache(pRpc->pCache, pIpSet->fqdn[pIpSet->inUse], pIpSet->port[pIpSet->inUse], pContext->connType);
  if ( pConn == NULL || pConn->user[0] == 0) {
    pConn = rpcOpenConn(pRpc, pIpSet->fqdn[pIpSet->inUse], pIpSet->port[pIpSet->inUse], pContext->connType);
  } else {
    tTrace("%s %p, connection is retrieved from cache", pRpc->label, pConn);
  }

  return pConn;
}

static int rpcProcessReqHead(SRpcConn *pConn, SRpcHead *pHead) {
    SRpcInfo *pRpc= pConn->pRpc;

    if (pConn->peerId == 0) {
      pConn->peerId = pHead->sourceId;
    } else {
      if (pConn->peerId != pHead->sourceId) {
        tTrace("%s %p, source Id is changed, old:0x%08x new:0x%08x", pRpc->label, pConn, 
               pConn->peerId, pHead->sourceId);
        return TSDB_CODE_INVALID_VALUE;
      }
    }

    if (pConn->inTranId == pHead->tranId) {
      if (pConn->inType == pHead->msgType) {
        if (pHead->code == 0) {
          tTrace("%s %p, %s is retransmitted", pRpc->label, pConn, taosMsg[pHead->msgType]);
          rpcSendQuickRsp(pConn, TSDB_CODE_ACTION_IN_PROGRESS);
        } else {
          // do nothing, it is heart beat from client
        }
      } else if (pConn->inType == 0) {
        tTrace("%s %p, %s is already processed, tranId:%d", pRpc->label, pConn, 
                taosMsg[pHead->msgType], pConn->inTranId);
        rpcSendMsgToPeer(pConn, pConn->pRspMsg, pConn->rspMsgLen); // resend the response
      } else {
        tTrace("%s %p, mismatched message %s and tranId", pRpc->label, pConn, taosMsg[pHead->msgType]);
      }

      // do not reply any message
      return TSDB_CODE_ALREADY_PROCESSED;
    }

    if (pConn->inType != 0) {
      tTrace("%s %p, last session is not finished, inTranId:%d tranId:%d", pRpc->label, pConn, 
              pConn->inTranId, pHead->tranId);
      return TSDB_CODE_LAST_SESSION_NOT_FINISHED;
    }

    pConn->inTranId = pHead->tranId;
    pConn->inType = pHead->msgType;

    return 0;
}

static int rpcProcessRspHead(SRpcConn *pConn, SRpcHead *pHead) {
  SRpcInfo *pRpc = pConn->pRpc;
  pConn->peerId = pHead->sourceId;

  if (pConn->outType == 0 || pConn->pContext == NULL) {
    return TSDB_CODE_UNEXPECTED_RESPONSE;
  }

  if (pHead->tranId != pConn->outTranId) {
    return TSDB_CODE_INVALID_TRAN_ID;
  }

  if (pHead->msgType != pConn->outType + 1) {
    return TSDB_CODE_INVALID_RESPONSE_TYPE;
  }

  taosTmrStopA(&pConn->pTimer);
  pConn->retry = 0;

  if (pHead->code == TSDB_CODE_ACTION_IN_PROGRESS) {
    if (pConn->tretry <= tsRpcMaxRetry) {
      tTrace("%s %p, peer is still processing the transaction", pRpc->label, pConn);
      pConn->tretry++;
      rpcSendReqHead(pConn);
      taosTmrReset(rpcProcessRetryTimer, tsRpcTimer, pConn, pRpc->tmrCtrl, &pConn->pTimer);
      return TSDB_CODE_ALREADY_PROCESSED;
    } else {
      // peer still in processing, give up
      return TSDB_CODE_TOO_SLOW;
    }
  }

  pConn->tretry = 0;
  pConn->outType = 0;
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;

  return TSDB_CODE_SUCCESS;
}

static SRpcConn *rpcProcessMsgHead(SRpcInfo *pRpc, SRecvInfo *pRecv) {
  int32_t    sid;
  SRpcConn  *pConn = NULL;

  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  sid = htonl(pHead->destId);

  if (pHead->msgType >= TSDB_MSG_TYPE_MAX || pHead->msgType <= 0) {
    tTrace("%s sid:%d, invalid message type:%d", pRpc->label, sid, pHead->msgType);
    terrno = TSDB_CODE_INVALID_MSG_TYPE; return NULL;
  }

  if (sid < 0 || sid >= pRpc->sessions) {
    tTrace("%s sid:%d, sid is out of range, max sid:%d, %s discarded", pRpc->label, sid,
           pRpc->sessions, taosMsg[pHead->msgType]);
    terrno = TSDB_CODE_INVALID_SESSION_ID; return NULL;
  }

  pConn = rpcGetConnObj(pRpc, sid, pRecv);
  if (pConn == NULL) return NULL;

  rpcLockConn(pConn);
  sid = pConn->sid;

  pConn->chandle = pRecv->chandle;
  pConn->peerIp = pRecv->ip; 
  if (pConn->peerPort == 0) pConn->peerPort = pRecv->port;
  if (pHead->port) pConn->peerPort = htons(pHead->port); 

  terrno = rpcCheckAuthentication(pConn, (char *)pHead, pRecv->msgLen);

  // code can be transformed only after authentication
  pHead->code = htonl(pHead->code);

  if (terrno == 0) {
    if (pHead->encrypt) {
      // decrypt here
    }

    if ( rpcIsReq(pHead->msgType) ) {
      terrno = rpcProcessReqHead(pConn, pHead);
      pConn->connType = pRecv->connType;
      taosTmrReset(rpcProcessIdleTimer, pRpc->idleTime, pConn, pRpc->tmrCtrl, &pConn->pIdleTimer);
    } else {
      terrno = rpcProcessRspHead(pConn, pHead);
    }
  }

  rpcUnlockConn(pConn);

  return pConn;
}

static void rpcProcessBrokenLink(SRpcConn *pConn) {
  SRpcInfo *pRpc = pConn->pRpc;
  
  tTrace("%s %p, link is broken", pRpc->label, pConn);
  // pConn->chandle = NULL;

  if (pConn->outType) {
    SRpcReqContext *pContext = pConn->pContext;
    pContext->code = TSDB_CODE_NETWORK_UNAVAIL;
    taosTmrStart(rpcProcessConnError, 0, pContext, pRpc->tmrCtrl);
  }

  if (pConn->inType) {
    // if there are pending request, notify the app
    tTrace("%s %p, connection is gone, notify the app", pRpc->label, pConn);
/*
    SRpcMsg rpcMsg;
    rpcMsg.pCont = NULL;
    rpcMsg.contLen = 0;
    rpcMsg.handle = pConn;
    rpcMsg.msgType = pConn->inType;
    rpcMsg.code = TSDB_CODE_NETWORK_UNAVAIL;
    (*(pRpc->cfp))(&rpcMsg);
*/
  }
 
  rpcCloseConn(pConn);
}

static void *rpcProcessMsgFromPeer(SRecvInfo *pRecv) {
  SRpcHead  *pHead = (SRpcHead *)pRecv->msg;
  SRpcInfo  *pRpc = (SRpcInfo *)pRecv->shandle;
  SRpcConn  *pConn = (SRpcConn *)pRecv->thandle;

  tDump(pRecv->msg, pRecv->msgLen);

  // underlying UDP layer does not know it is server or client
  pRecv->connType = pRecv->connType | pRpc->connType;  

  if (pRecv->ip==0 && pConn) {
    rpcProcessBrokenLink(pConn); 
    tfree(pRecv->msg);
    return NULL;
  }

  terrno = 0;
  pConn = rpcProcessMsgHead(pRpc, pRecv);

  if (pHead->msgType < TSDB_MSG_TYPE_CM_HEARTBEAT || (rpcDebugFlag & 16)) {
    tTrace("%s %p, %s received from 0x%x:%hu, parse code:0x%x len:%d sig:0x%08x:0x%08x:%d code:0x%x",
        pRpc->label, pConn, taosMsg[pHead->msgType], pRecv->ip, pRecv->port, terrno, 
        pRecv->msgLen, pHead->sourceId, pHead->destId, pHead->tranId, pHead->code);
  }

  int32_t code = terrno;
  if (code != TSDB_CODE_ALREADY_PROCESSED) {
    if (code != 0) { // parsing error
      if ( rpcIsReq(pHead->msgType) ) {
        rpcSendErrorMsgToPeer(pRecv, code);
        tTrace("%s %p, %s is sent with error code:%x", pRpc->label, pConn, taosMsg[pHead->msgType+1], code);
      } 
    } else { // parsing OK
      rpcProcessIncomingMsg(pConn, pHead);
    }
  }

  if (code) rpcFreeMsg(pRecv->msg);
  return pConn;
}

static void rpcNotifyClient(SRpcReqContext *pContext, SRpcMsg *pMsg) {
  SRpcInfo       *pRpc = pContext->pRpc;

  if (pContext->pRsp) { 
    // for synchronous API
    tsem_post(pContext->pSem);
    memcpy(pContext->pSet, &pContext->ipSet, sizeof(SRpcIpSet));
    memcpy(pContext->pRsp, pMsg, sizeof(SRpcMsg));
  } else {
    // for asynchronous API 
    if (pRpc->ufp && (pContext->ipSet.inUse != pContext->oldInUse || pContext->redirect)) 
      (*pRpc->ufp)(pContext->ahandle, &pContext->ipSet);  // notify the update of ipSet

    (*pRpc->cfp)(pMsg);  
  }

  // free the request message
  rpcFreeCont(pContext->pCont); 
}

static void rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHead *pHead) {

  SRpcInfo *pRpc = pConn->pRpc;
  SRpcMsg   rpcMsg;

  pHead = rpcDecompressRpcMsg(pHead);
  rpcMsg.contLen = rpcContLenFromMsg(pHead->msgLen);
  rpcMsg.pCont = pHead->content;
  rpcMsg.msgType = pHead->msgType;
  rpcMsg.code = pHead->code; 
   
  if ( rpcIsReq(pHead->msgType) ) {
    rpcMsg.handle = pConn;
    taosTmrReset(rpcProcessProgressTimer, tsRpcTimer/2, pConn, pRpc->tmrCtrl, &pConn->pTimer);
    (*(pRpc->cfp))(&rpcMsg);
  } else {
    // it's a response
    SRpcReqContext *pContext = pConn->pContext;
    rpcMsg.handle = pContext->ahandle;
    pConn->pContext = NULL;

    // for UDP, port may be changed by server, the port in ipSet shall be used for cache
    rpcAddConnIntoCache(pRpc->pCache, pConn, pConn->peerFqdn, pContext->ipSet.port[pContext->ipSet.inUse], pConn->connType);    

    if (pHead->code == TSDB_CODE_REDIRECT) {
      pContext->redirect = 1;
      pContext->numOfTry = 0;
      memcpy(&pContext->ipSet, pHead->content, sizeof(pContext->ipSet));
      tTrace("%s %p, redirect is received, numOfIps:%d", pRpc->label, pConn, pContext->ipSet.numOfIps);
      for (int i=0; i<pContext->ipSet.numOfIps; ++i) 
        pContext->ipSet.port[i] = htons(pContext->ipSet.port[i]);
      rpcSendReqToServer(pRpc, pContext);
    } else if (pHead->code == TSDB_CODE_NOT_READY) {
      pContext->code = pHead->code;
      rpcProcessConnError(pContext, NULL);
    } else {
      rpcNotifyClient(pContext, &rpcMsg);
    }
  }
}

static void rpcSendQuickRsp(SRpcConn *pConn, int32_t code) {
  char       msg[RPC_MSG_OVERHEAD];
  SRpcHead  *pHead;

  // set msg header
  memset(msg, 0, sizeof(SRpcHead));
  pHead = (SRpcHead *)msg;
  pHead->version = 1;
  pHead->msgType = pConn->inType+1;
  pHead->spi = pConn->spi;
  pHead->encrypt = 0;
  pHead->tranId = pConn->inTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  memcpy(pHead->user, pConn->user, tListLen(pHead->user));
  pHead->code = htonl(code);

  rpcSendMsgToPeer(pConn, msg, sizeof(SRpcHead));
  pConn->secured = 1; // connection shall be secured
}

static void rpcSendReqHead(SRpcConn *pConn) {
  char       msg[RPC_MSG_OVERHEAD];
  SRpcHead  *pHead;

  // set msg header
  memset(msg, 0, sizeof(SRpcHead));
  pHead = (SRpcHead *)msg;
  pHead->version = 1;
  pHead->msgType = pConn->outType;
  pHead->spi = pConn->spi;
  pHead->encrypt = 0;
  pHead->tranId = pConn->outTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  memcpy(pHead->user, pConn->user, tListLen(pHead->user));
  pHead->code = 1;

  rpcSendMsgToPeer(pConn, msg, sizeof(SRpcHead));
}

static void rpcSendErrorMsgToPeer(SRecvInfo *pRecv, int32_t code) {
  SRpcHead  *pRecvHead, *pReplyHead;
  char       msg[sizeof(SRpcHead) + sizeof(SRpcDigest) + sizeof(uint32_t) ];
  uint32_t   timeStamp;
  int        msgLen;

  pRecvHead = (SRpcHead *)pRecv->msg;
  pReplyHead = (SRpcHead *)msg;

  memset(msg, 0, sizeof(SRpcHead));
  pReplyHead->version = pRecvHead->version;
  pReplyHead->msgType = (char)(pRecvHead->msgType + 1);
  pReplyHead->spi = 0;
  pReplyHead->encrypt = pRecvHead->encrypt;
  pReplyHead->tranId = pRecvHead->tranId;
  pReplyHead->sourceId = pRecvHead->destId;
  pReplyHead->destId = pRecvHead->sourceId;
  pReplyHead->linkUid = pRecvHead->linkUid;

  pReplyHead->code = htonl(code);
  msgLen = sizeof(SRpcHead);

  if (code == TSDB_CODE_INVALID_TIME_STAMP) {
    // include a time stamp if client's time is not synchronized well
    uint8_t *pContent = pReplyHead->content;
    timeStamp = htonl(taosGetTimestampSec());
    memcpy(pContent, &timeStamp, sizeof(timeStamp));
    msgLen += sizeof(timeStamp);
  }

  pReplyHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  (*taosSendData[pRecv->connType])(pRecv->ip, pRecv->port, msg, msgLen, pRecv->chandle);

  return; 
}

static void rpcSendReqToServer(SRpcInfo *pRpc, SRpcReqContext *pContext) {
  SRpcHead  *pHead = rpcHeadFromCont(pContext->pCont);
  char      *msg = (char *)pHead;
  int        msgLen = rpcMsgLenFromCont(pContext->contLen);
  char       msgType = pContext->msgType;

  pContext->numOfTry++;
  SRpcConn *pConn = rpcSetupConnToServer(pContext);
  if (pConn == NULL) {
    pContext->code = terrno;
    taosTmrStart(rpcProcessConnError, 0, pContext, pRpc->tmrCtrl);
    return;
  }

  rpcLockConn(pConn);

  // set the message header  
  pHead->version = 1;
  pHead->msgType = msgType;
  pHead->encrypt = 0;
  pConn->tranId++;
  if ( pConn->tranId == 0 ) pConn->tranId++;
  pHead->tranId = pConn->tranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->port = 0;
  pHead->linkUid = pConn->linkUid;
  if (!pConn->secured) memcpy(pHead->user, pConn->user, tListLen(pHead->user));

  // set the connection parameters
  pConn->outType = msgType;
  pConn->outTranId = pHead->tranId;
  pConn->pReqMsg = msg;
  pConn->reqMsgLen = msgLen;
  pConn->pContext = pContext;

  rpcUnlockConn(pConn);

  taosTmrReset(rpcProcessRetryTimer, tsRpcTimer, pConn, pRpc->tmrCtrl, &pConn->pTimer);
  rpcSendMsgToPeer(pConn, msg, msgLen);
}

static void rpcSendMsgToPeer(SRpcConn *pConn, void *msg, int msgLen) {
  int        writtenLen = 0;
  SRpcInfo  *pRpc = pConn->pRpc;
  SRpcHead  *pHead = (SRpcHead *)msg;

  msgLen = rpcAddAuthPart(pConn, msg, msgLen);

  if ( rpcIsReq(pHead->msgType)) {
    if (pHead->msgType < TSDB_MSG_TYPE_CM_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace("%s %p, %s is sent to %s:%hu, len:%d sig:0x%08x:0x%08x:%d",
             pRpc->label, pConn, taosMsg[pHead->msgType], pConn->peerFqdn,
             pConn->peerPort, msgLen, pHead->sourceId, pHead->destId, pHead->tranId);
  } else {
    if (pHead->code == 0) pConn->secured = 1; // for success response, set link as secured
    if (pHead->msgType < TSDB_MSG_TYPE_CM_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace( "%s %p, %s is sent to %s:%hu, code:0x%x len:%d sig:0x%08x:0x%08x:%d",
          pRpc->label, pConn, taosMsg[pHead->msgType], pConn->peerFqdn, pConn->peerPort, 
          htonl(pHead->code), msgLen, pHead->sourceId, pHead->destId, pHead->tranId);
  }

  writtenLen = (*taosSendData[pConn->connType])(pConn->peerIp, pConn->peerPort, pHead, msgLen, pConn->chandle);

  if (writtenLen != msgLen) {
    tError("%s %p, failed to send, dataLen:%d writtenLen:%d, reason:%s", pRpc->label, pConn, 
           msgLen, writtenLen, strerror(errno));
  }
 
  tDump(msg, msgLen);
}

static void rpcProcessConnError(void *param, void *id) {
  SRpcReqContext *pContext = (SRpcReqContext *)param;
  SRpcInfo       *pRpc = pContext->pRpc;
  SRpcMsg         rpcMsg;
 
  if (pRpc == NULL) {
    return;
  }
  
  tTrace("%s connection error happens", pRpc->label);

  if (pContext->numOfTry >= pContext->ipSet.numOfIps) {
    rpcMsg.msgType = pContext->msgType+1;
    rpcMsg.handle = pContext->ahandle;
    rpcMsg.code = pContext->code;
    rpcMsg.pCont = NULL;
    rpcMsg.contLen = 0;

    rpcNotifyClient(pContext, &rpcMsg);
  } else {
    // move to next IP 
    pContext->ipSet.inUse++;
    pContext->ipSet.inUse = pContext->ipSet.inUse % pContext->ipSet.numOfIps;
    rpcSendReqToServer(pRpc, pContext);
  }
}

static void rpcProcessRetryTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  SRpcInfo *pRpc = pConn->pRpc;
  int       reportDisc = 0;

  rpcLockConn(pConn);

  if (pConn->outType && pConn->user[0]) {
    tTrace("%s %p, expected %s is not received", pRpc->label, pConn, taosMsg[(int)pConn->outType + 1]);
    pConn->pTimer = NULL;
    pConn->retry++;

    if (pConn->retry < 4) {
      tTrace("%s %p, re-send msg:%s to %s:%hu", pRpc->label, pConn, 
             taosMsg[pConn->outType], pConn->peerFqdn, pConn->peerPort);
      rpcSendMsgToPeer(pConn, pConn->pReqMsg, pConn->reqMsgLen);      
      taosTmrReset(rpcProcessRetryTimer, tsRpcTimer, pConn, pRpc->tmrCtrl, &pConn->pTimer);
    } else {
      // close the connection
      tTrace("%s %p, failed to send msg:%s to %s:%hu", pRpc->label, pConn,
              taosMsg[pConn->outType], pConn->peerFqdn, pConn->peerPort);
      reportDisc = 1;
    }
  } else {
    tTrace("%s %p, retry timer not processed", pRpc->label, pConn);
  }

  rpcUnlockConn(pConn);

  if (reportDisc && pConn->pContext) { 
    pConn->pContext->code = TSDB_CODE_NETWORK_UNAVAIL;
    rpcProcessConnError(pConn->pContext, NULL);
    rpcCloseConn(pConn);
  }
}

static void rpcProcessIdleTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  SRpcInfo *pRpc = pConn->pRpc;

  if (pConn->user[0]) {
    tTrace("%s %p, close the connection since no activity", pRpc->label, pConn);
    if (pConn->inType && pRpc->cfp) {
      // if there are pending request, notify the app
      tTrace("%s %p, notify the app, connection is gone", pRpc->label, pConn);
/*
      SRpcMsg rpcMsg;
      rpcMsg.pCont = NULL;
      rpcMsg.contLen = 0;
      rpcMsg.handle = pConn;
      rpcMsg.msgType = pConn->inType;
      rpcMsg.code = TSDB_CODE_NETWORK_UNAVAIL; 
      (*(pRpc->cfp))(&rpcMsg);
*/
    }
    rpcCloseConn(pConn);
  } else {
    tTrace("%s %p, idle timer:%p not processed", pRpc->label, pConn, tmrId);
  }
}

static void rpcProcessProgressTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  SRpcInfo *pRpc = pConn->pRpc;

  rpcLockConn(pConn);

  if (pConn->inType && pConn->user[0]) {
    tTrace("%s %p, progress timer expired, send progress", pRpc->label, pConn);
    rpcSendQuickRsp(pConn, TSDB_CODE_ACTION_IN_PROGRESS);
    taosTmrReset(rpcProcessProgressTimer, tsRpcTimer/2, pConn, pRpc->tmrCtrl, &pConn->pTimer);
  } else {
    tTrace("%s %p, progress timer:%p not processed", pRpc->label, pConn, tmrId);
  }

  rpcUnlockConn(pConn);
}

static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen) {
  SRpcHead  *pHead = rpcHeadFromCont(pCont);
  int32_t    finalLen = 0;
  int        overhead = sizeof(SRpcComp);
  
  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }
  
  char *buf = malloc (contLen + overhead + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", contLen);
    return contLen;
  }
  
  int32_t compLen = LZ4_compress_default(pCont, buf, contLen, contLen + overhead);
  
  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (compLen < contLen - overhead) {
    SRpcComp *pComp = (SRpcComp *)pCont;
    pComp->reserved = 0; 
    pComp->contLen = htonl(contLen); 
    memcpy(pCont + overhead, buf, compLen);
    
    pHead->comp = 1;
    tTrace("compress rpc msg, before:%d, after:%d", contLen, compLen);
    finalLen = compLen + overhead;
  } else {
    finalLen = contLen;
  }

  free(buf);
  return finalLen;
}

static SRpcHead *rpcDecompressRpcMsg(SRpcHead *pHead) {
  int overhead = sizeof(SRpcComp);
  SRpcHead   *pNewHead = NULL;  
  uint8_t    *pCont = pHead->content;
  SRpcComp   *pComp = (SRpcComp *)pHead->content;

  if (pHead->comp) {
    // decompress the content
    assert(pComp->reserved == 0);
    int contLen = htonl(pComp->contLen);
  
    // prepare the temporary buffer to decompress message
    char *temp = (char *)malloc(contLen + RPC_MSG_OVERHEAD);
    pNewHead = (SRpcHead *)(temp + sizeof(SRpcReqContext)); // reserve SRpcReqContext
  
    if (pNewHead) {
      int compLen = rpcContLenFromMsg(pHead->msgLen) - overhead;
      int origLen = LZ4_decompress_safe((char*)(pCont + overhead), (char *)pNewHead->content, compLen, contLen);
      assert(origLen == contLen);
    
      memcpy(pNewHead, pHead, sizeof(SRpcHead));
      pNewHead->msgLen = rpcMsgLenFromCont(origLen);
      rpcFreeMsg(pHead); // free the compressed message buffer
      pHead = pNewHead; 
      tTrace("decompress rpc msg, compLen:%d, after:%d", compLen, contLen);
    } else {
      tError("failed to allocate memory to decompress msg, contLen:%d", contLen);
    }
  }

  return pHead;
}

static int rpcAuthenticateMsg(void *pMsg, int msgLen, void *pAuth, void *pKey) {
  MD5_CTX context;
  int     ret = -1;

  MD5Init(&context);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  if (memcmp(context.digest, pAuth, sizeof(context.digest)) == 0) ret = 0;

  return ret;
}

static void rpcBuildAuthHead(void *pMsg, int msgLen, void *pAuth, void *pKey) {
  MD5_CTX context;

  MD5Init(&context);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));
}

static int rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHead *pHead = (SRpcHead *)msg;

  if (pConn->spi && pConn->secured == 0) {
    // add auth part
    pHead->spi = pConn->spi;
    SRpcDigest *pDigest = (SRpcDigest *)(msg + msgLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(SRpcDigest);
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    rpcBuildAuthHead(pHead, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHead->spi = 0;
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  return msgLen;
}

static int rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHead *pHead = (SRpcHead *)msg;
  SRpcInfo *pRpc = pConn->pRpc;
  int       code = 0;

  if ((pConn->secured && pHead->spi == 0) || (pHead->spi == 0 && pConn->spi == 0)){
    // secured link, or no authentication 
    pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
    return 0;
  }

  if ( !rpcIsReq(pHead->msgType) ) {
    // for response, if code is auth failure, it shall bypass the auth process
    code = htonl(pHead->code);
    if (code==TSDB_CODE_INVALID_TIME_STAMP || code==TSDB_CODE_AUTH_FAILURE || 
        code==TSDB_CODE_INVALID_USER || code == TSDB_CODE_NOT_READY) {
      pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
      return 0;
    } 
  }
 
  code = 0;
  if (pHead->spi == pConn->spi) {
    // authentication
    SRpcDigest *pDigest = (SRpcDigest *)((char *)pHead + msgLen - sizeof(SRpcDigest));

    int32_t delta;
    delta = (int32_t)htonl(pDigest->timeStamp);
    delta -= (int32_t)taosGetTimestampSec();
    if (abs(delta) > 900) {
      tWarn("%s %p, time diff:%d is too big, msg discarded", pRpc->label, pConn, delta);
      code = TSDB_CODE_INVALID_TIME_STAMP;
    } else {
      if (rpcAuthenticateMsg(pHead, msgLen-TSDB_AUTH_LEN, pDigest->auth, pConn->secret) < 0) {
        tError("%s %p, authentication failed, msg discarded", pRpc->label, pConn);
        code = TSDB_CODE_AUTH_FAILURE;
      } else {
        pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen) - sizeof(SRpcDigest);
        if ( !rpcIsReq(pHead->msgType) ) pConn->secured = 1;  // link is secured for client
        tTrace("%s %p, message is authenticated", pRpc->label, pConn);
      }
    }
  } else {
    tTrace("%s %p, auth spi:%d not matched with received:%d", pRpc->label, pConn, pConn->spi, pHead->spi);
    code = TSDB_CODE_AUTH_FAILURE;
  }

  return code;
}

static void rpcLockConn(SRpcConn *pConn) {
  int64_t tid = taosGetPthreadId();
  int     i = 0;
  while (atomic_val_compare_exchange_64(&(pConn->lockedBy), 0, tid) != 0) {
    if (++i % 1000 == 0) {
      sched_yield();
    }
  }
}

static void rpcUnlockConn(SRpcConn *pConn) {
  int64_t tid = taosGetPthreadId();
  if (atomic_val_compare_exchange_64(&(pConn->lockedBy), tid, 0) != tid) {
    assert(false);
  }
}

