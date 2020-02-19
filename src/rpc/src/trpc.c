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
#include "shash.h"
#include "taosmsg.h"
#include "tidpool.h"
#include "tlog.h"
#include "tmd5.h"
#include "tmempool.h"
#include "tsocket.h"
#include "ttcpclient.h"
#include "ttcpserver.h"
#include "ttime.h"
#include "ttimer.h"
#include "tudp.h"
#include "tutil.h"
#include "lz4.h"
#include "tconncache.h"
#include "trpc.h"
#include "taoserror.h"

#define RPC_MSG_OVERHEAD (sizeof(SRpcReqContext) + sizeof(SRpcHeader) + sizeof(SRpcDigest)) 
#define rpcHeaderFromCont(cont) ((SRpcHeader *) (cont - sizeof(SRpcHeader)))
#define rpcContFromHeader(msg) ( msg + sizeof(SRpcHeader))
#define rpcMsgLenFromCont(contLen) ( contLen + sizeof(SRpcHeader))
#define rpcContLenFromMsg(msgLen) (msgLen - sizeof(SRpcHeader))
#define rpcIsReq(type) (type & 1U)

typedef struct {
  int      sessions;
  int      numOfThreads;
  int      idleTime; // milliseconds;
  char     localIp[TSDB_IPv4ADDR_LEN];
  uint16_t localPort;
  int      connType;
  char     label[12];

  char    *meterId;   // meter ID
  char     spi;       // security parameter index
  char     encrypt;   // encrypt algorithm
  char    *secret;    // key for authentication
  char    *ckey;      // ciphering key

  void *(*fp)(char type, void *pCont, int contLen, void *handle, int index);
  int   (*afp)(char *meterId, char *spi, char *encrypt, uint8_t *secret, uint8_t *ckey); // FP to retrieve auth info
  struct _RpcConn *connList;
  void     *idPool;
  void     *tmrCtrl;
  void     *hash;
  void     *shandle;  // returned handle from lower layer during initialization
  void     *pCache;   // connection cache
  pthread_mutex_t mutex;
} SRpcInfo;

typedef struct {
  SRpcIpSet ipSet;
  void     *ahandle;
  SRpcInfo *pRpc;
  char      msgType;
  char     *pCont;
  int       contLen;
  int       numOfRetry;
  int32_t   code;
  char      msg[];
} SRpcReqContext;

typedef struct _RpcConn {
  int       sid;     // session ID
  uint32_t  ownId;   // own link ID
  uint32_t  peerId;  // peer link ID
  char      meterId[TSDB_UNI_LEN]; // user ID for the link
  char      spi;     // security parameter index
  char      encrypt; // encryption, 0:1 
  uint8_t   secret[TSDB_KEY_LEN]; // secret for the link
  uint8_t   ckey[TSDB_KEY_LEN];   // ciphering key 
  uint16_t  localPort;      // for UDP only
  uint32_t  peerUid;        // peer UID
  uint32_t  peerIp;         // peer IP
  uint16_t  peerPort;       // peer port
  char      peerIpstr[20];  // peer IP string
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
  SRpcReqContext *pContext; // request context
} SRpcConn;

typedef struct {
  char     version:4; // RPC version
  char     comp:4;    // compression algorithm, 0:no compression 1:lz4
  char     tcp:2;     // tcp flag 
  char     spi:3;     // security parameter index
  char     encrypt:3; // encrypt algorithm, 0: no encryption
  uint16_t tranId;    // transcation ID
  uint32_t uid;       // for unique ID inside a client
  uint32_t sourceId;  // source ID, an index for connection list  
  uint32_t destId;    // destination ID, an index for connection list
  char     meterId[TSDB_UNI_LEN]; 
  uint16_t port;      // for UDP only, port may be changed
  char     empty[1];  // reserved
  uint8_t  msgType;   // message type  
  int32_t  msgLen;    // message length including the header iteslf
  uint8_t  content[0]; // message body starts from here
} SRpcHeader;

typedef struct {
  uint32_t timeStamp;
  uint8_t  auth[TSDB_AUTH_LEN];
} SRpcDigest;

int tsRpcProgressTime = 10;  // milliseocnds

// not configurable
int tsRpcMaxRetry;
int tsRpcHeadSize;

void *(*taosInitConn[])(char *ip, uint16_t port, char *label, int threads, void *fp, void *shandle) = {
    taosInitUdpServer, 
    taosInitUdpClient, 
    taosInitTcpServer, 
    taosInitTcpClient
};

void (*taosCleanUpConn[])(void *thandle) = {
    taosCleanUpUdpConnection, 
    taosCleanUpUdpConnection, 
    taosCleanUpTcpServer,
    taosCleanUpTcpClient
};

int (*taosSendData[])(uint32_t ip, uint16_t port, char *data, int len, void *chandle) = {
    taosSendUdpData, 
    taosSendUdpData, 
    taosSendTcpServerData, 
    taosSendTcpClientData
};

void *(*taosOpenConn[])(void *shandle, void *thandle, char *ip, uint16_t port) = {
    taosOpenUdpConnection,
    taosOpenUdpConnection,
    NULL,
    taosOpenTcpClientConnection,
};

void (*taosCloseConn[])(void *chandle) = {
    NULL, 
    NULL, 
    taosCloseTcpServerConnection, 
    taosCloseTcpClientConnection
};

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerIpStr, uint16_t peerPort);
static void  rpcCloseConn(void *thandle);
static SRpcConn *rpcSetConnToServer(void *shandle, SRpcIpSet ipSet);
static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc);
static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, char *meterId, char *hashstr);
static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, char *meterId, char *hashstr);

static void  rpcSendReqToServer(SRpcConn *pConn, SRpcReqContext *pContext);
static void  rpcSendQuickRsp(SRpcConn *pConn, char code);
static void  rpcSendErrorMsgToPeer(SRpcInfo *pRpc, char *pMsg, int32_t code, uint32_t ip, uint16_t port, void *chandle);
static void  rpcSendDataToPeer(SRpcConn *pConn, void *data, int dataLen);

static void *rpcProcessDataFromPeer(void *data, int dataLen, uint32_t ip, uint16_t port, void *shandle, void *chandle);
static void  rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHeader *pHeader);
static void  rpcProcessConnError(void *param, void *id);
static void  rpcProcessRetryTimer(void *, void *);
static void  rpcProcessIdleTimer(void *param, void *tmrId);
static void  rpcProcessProgressTimer(void *param, void *tmrId);

static void  rpcFreeMsg(void *msg);
static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen);
static SRpcHeader *rpcDecompressRpcMsg(SRpcHeader *pHeader);
static int   rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen);
static int   rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen);

void *rpcOpen(SRpcInit *pInit) {
  SRpcInfo *pRpc;

  tsRpcMaxRetry = tsRpcMaxTime * 1000 / tsRpcProgressTime;
  tsRpcHeadSize = RPC_MSG_OVERHEAD; 

  pRpc = (SRpcInfo *)calloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) return NULL;

  strcpy(pRpc->label, pInit->label);
  pRpc->fp = pInit->fp;
  pRpc->connType = pInit->connType;
  pRpc->idleTime = pInit->idleTime;
  pRpc->numOfThreads = pInit->numOfThreads;
  if (pRpc->numOfThreads > TSDB_MAX_RPC_THREADS) {
    pRpc->numOfThreads = TSDB_MAX_RPC_THREADS;
  }

  strcpy(pRpc->localIp, pInit->localIp);
  pRpc->localPort = pInit->localPort;
  pRpc->afp = pInit->afp;
  pRpc->sessions = pInit->sessions;
  strcpy(pRpc->meterId, pInit->meterId);
  pRpc->spi = pInit->spi;
  strcpy(pRpc->secret, pInit->secret);
  strcpy(pRpc->ckey, pInit->ckey);
  pRpc->afp = pInit->afp;

  pRpc->shandle = (*taosInitConn[pRpc->connType])(pRpc->localIp, pRpc->localPort, pRpc->label, 
                                                  pRpc->numOfThreads, rpcProcessDataFromPeer, pRpc);
  if (pRpc->shandle == NULL) {
    tError("%s failed to init network, %s:%d", pRpc->label, pRpc->localIp, pRpc->localPort);
    rpcClose(pRpc);
    return NULL;
  }

  size_t size = sizeof(SRpcConn) * pRpc->sessions;
  pRpc->connList = (SRpcConn *)calloc(1, size);
  if (pRpc->connList == NULL) {
    tError("%s failed to allocate memory for taos connections, size:%d", pRpc->label, size);
    rpcClose(pRpc);
    return NULL;
  }

  pRpc->idPool = taosInitIdPool(pRpc->sessions);
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

  pRpc->hash = taosInitStrHash(pRpc->sessions, sizeof(pRpc), taosHashString);
  if (pRpc->hash == NULL) {
    tError("%s failed to init string hash", pRpc->label);
    rpcClose(pRpc);
    return NULL;
  }

  pRpc->pCache = taosOpenConnCache(pRpc->sessions, rpcCloseConn, pRpc->tmrCtrl, tsShellActivityTimer*1000); 
  if ( pRpc->pCache == NULL ) {
    tError("%s failed to init connection cache", pRpc->label);
    rpcClose(pRpc);
    return NULL;
  }

  pthread_mutex_init(&pRpc->mutex, NULL);

  tTrace("%s RPC is openned, numOfThreads:%d", pRpc->label, pRpc->numOfThreads);

  return pRpc;
}

void rpcClose(void *param) {
  SRpcInfo *pRpc = (SRpcInfo *)param;

  (*taosCleanUpConn[pRpc->connType])(pRpc->shandle);

  for (int i = 0; i < pRpc->sessions; ++i) {
    if (pRpc->connList[i].meterId[0]) {
      rpcCloseConn((void *)(pRpc->connList + i));
    }
  }

  taosCleanUpStrHash(pRpc->hash);
  taosTmrCleanUp(pRpc->tmrCtrl);
  taosIdPoolCleanUp(pRpc->idPool);
  taosCloseConnCache(pRpc->pCache);

  tfree(pRpc->connList);
  pthread_mutex_destroy(&pRpc->mutex);
  tfree(pRpc);
}

void *rpcMallocCont(int size) {
  char *pMsg = NULL;

  size += RPC_MSG_OVERHEAD;
  pMsg = (char *)calloc(1, (size_t)size);
  if (pMsg == NULL) {
    tError("failed to malloc msg, size:%d", size);
    return NULL;
  }

  return pMsg + sizeof(SRpcReqContext) + sizeof(SRpcHeader);
}

void rpcFreeCont(void *cont) {
  char *msg = ((char *)cont) - sizeof(SRpcHeader);
  free(msg);
}

void rpcSendRequest(void *shandle, SRpcIpSet ipSet, char type, void *pCont, int contLen, void *ahandle) {
  SRpcInfo       *pRpc = (SRpcInfo *)shandle;
  SRpcConn       *pConn;
  SRpcReqContext *pContext;

  contLen = rpcCompressRpcMsg(pCont, contLen);
  pContext = (SRpcReqContext *) (pCont-sizeof(SRpcHeader)-sizeof(SRpcReqContext));
  pContext->ahandle = ahandle;
  pContext->pRpc = (SRpcInfo *)shandle;
  pContext->ipSet = ipSet;
  pContext->contLen = contLen;
  pContext->pCont = pCont;
  pContext->msgType = type;

  pConn = rpcSetConnToServer(shandle, ipSet);
  pContext->code = terrno;
  if (pConn == NULL) taosTmrStart(rpcProcessConnError, 0, pContext, pRpc->tmrCtrl); 

  rpcSendReqToServer(pConn, pContext);

  return;
}

void rpcSendResponse(void *handle, void *pCont, int contLen) {
  int          msgLen = 0;
  SRpcConn    *pConn = (SRpcConn *)handle;
  SRpcInfo    *pRpc = pConn->pRpc;
  SRpcHeader  *pHeader = rpcHeaderFromCont(pCont);
  char        *msg = (char *)pHeader;

  contLen = rpcCompressRpcMsg(pCont, contLen);
  msgLen = rpcMsgLenFromCont(contLen);

  pthread_mutex_lock(&pRpc->mutex);

  if ( pConn->inType == 0 || pConn->meterId[0] == 0 ) {
    tTrace("%s pConn:%p, connection is already released, rsp wont be sent", pRpc->label, pConn);
    pthread_mutex_lock(&pRpc->mutex);
    return;
  }

  // set msg header
  pHeader->version = 1;
  pHeader->msgType = pConn->inType+1;
  pHeader->spi = 0;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pHeader->tranId = pConn->inTranId;
  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;
  pHeader->uid = 0;
  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  // set pConn parameters
  pConn->inType = 0;

  // response message is released until new response is sent
  rpcFreeMsg(pConn->pRspMsg); 
  pConn->pRspMsg = msg;
  pConn->rspMsgLen = msgLen;
  if (pHeader->content[0] == TSDB_CODE_ACTION_IN_PROGRESS) pConn->inTranId--;

  pthread_mutex_unlock(&pRpc->mutex);

  taosTmrStopA(&pConn->pTimer);
  rpcSendDataToPeer(pConn, msg, msgLen);

  return;
}

void rpcSendSimpleRsp(void *thandle, int32_t code) {
  char     *pMsg;
  STaosRsp *pRsp;
  int       msgLen = sizeof(STaosRsp);

  if (thandle == NULL) {
    tError("connection is gone, response could not be sent");
    return;
  }

  pMsg = rpcMallocCont(msgLen);
  if (pMsg == NULL) return;

  pRsp = (STaosRsp *)pMsg;
  pRsp->code = htonl(code);

  rpcSendResponse(thandle, pMsg, msgLen);

  return;
}

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerIpStr, uint16_t peerPort) {
  SRpcConn *pConn;

  pConn = rpcAllocateClientConn(pRpc); 

  if (pConn) { 
    strcpy(pConn->peerIpstr, peerIpStr);
    pConn->peerIp = inet_addr(peerIpStr);
    pConn->peerPort = peerPort;
    strcpy(pConn->meterId, pRpc->meterId);

    if (taosOpenConn[pRpc->connType]) {
      pConn->chandle = (*taosOpenConn[pRpc->connType])(pRpc->shandle, pConn, pConn->peerIpstr, pConn->peerPort);
      if (pConn->chandle) {
        tTrace("%s pConn:%p, rpc connection is set up, sid:%d id:%s ip:%s:%hu localPort:%d", pRpc->label, 
                pConn, pConn->sid, pRpc->meterId, pConn->peerIpstr, pConn->peerPort, pConn->localPort);
      } else {
        tError("%s pConn:%p, failed to set up nw connection to ip:%s:%hu", pRpc->label, pConn,
               pConn->sid, pRpc->meterId, pConn->peerIpstr, pConn->peerPort);
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

  pthread_mutex_lock(&pRpc->mutex);

  if (taosCloseConn[pRpc->connType]) (*taosCloseConn[pRpc->connType])(pConn->chandle);

  taosTmrStopA(&pConn->pTimer);
  taosTmrStopA(&pConn->pIdleTimer);

  if ( pRpc->connType == TAOS_CONN_UDPS || TAOS_CONN_TCPS) {
    char hashstr[40] = {0};
    sprintf(hashstr, "%x:%x:%x", pConn->peerIp, pConn->peerUid, pConn->peerId);
    taosDeleteStrHash(pRpc->hash, hashstr);
    rpcFreeMsg(pConn->pRspMsg); // it may have a response msg saved, but not request msg
  }

  taosFreeId(pRpc->idPool, pConn->sid);

  // reset the link parameters
  pConn->meterId[0] = 0;
  pConn->outType = 0;
  pConn->inType = 0;
  pConn->inTranId = 0;
  pConn->outTranId = 0;
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;
  pConn->pRspMsg = NULL;
  pConn->rspMsgLen = 0;
  pConn->pContext = NULL;

  tTrace("%s pConn:%p, rpc connection is closed", pRpc->label, pConn);

  pthread_mutex_unlock(&pRpc->mutex);
}

static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc) {
  SRpcConn *pConn = NULL;

  int sid = taosAllocateId(pRpc->idPool);
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_MAX_SESSIONS;
  } else {
    tTrace("%s sid:%d, ID allocated, used:%d, old id:%d", pRpc->label, sid, taosIdPoolNumOfUsed(pRpc->idPool));

    pConn = pRpc->connList + sid;
    memset(pConn, 0, sizeof(SRpcConn));

    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    pConn->spi = pRpc->spi;
    pConn->encrypt = pRpc->encrypt;
    if (pConn->spi) memcpy(pConn->secret, pRpc->secret, TSDB_KEY_LEN);
  }

  return pConn;
}

static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, char *meterId, char *hashstr) {
  SRpcConn *pConn;

  // check if it is already allocated
  pConn = *(SRpcConn **)(taosGetStrHashData(pRpc->hash, hashstr));
  if (pConn) return pConn;

  int sid = taosAllocateId(pRpc->idPool);
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_MAX_SESSIONS;
  } else {
    pConn = pRpc->connList + sid;
    memset(pConn, 0, sizeof(SRpcConn));
    memcpy(pConn->meterId, meterId, tListLen(pConn->meterId));
    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    if (pRpc->afp && (*pRpc->afp)(meterId, &pConn->spi, &pConn->encrypt, pConn->secret, pConn->ckey)) {
      tWarn("%s pConn:%p, meterId not there", pRpc->label, pConn);
      taosFreeId(pRpc->idPool, sid);   // sid shall be released
      terrno = TSDB_CODE_INVALID_USER;
      pConn = NULL;
    }
  }      

  if (pConn) {
    taosAddStrHash(pRpc->hash, hashstr, (char *)&pConn);
    tTrace("%s pConn:%p, rpc connection is allocated, sid:%d id:%s", pRpc->label, pConn, sid);
  }

  return pConn;
}

static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, char *meterId, char *hashstr) {
  SRpcConn *pConn = NULL;  

  if (sid) {
    pConn = pRpc->connList + sid;
  } else {
    pConn = rpcAllocateServerConn(pRpc, meterId, hashstr);
  } 

  if (pConn) {
    if (memcmp(pConn->meterId, meterId, tListLen(pConn->meterId)) != 0) {
      tTrace("%s pConn:%p, meterId:%s is not matched, received:%s", pRpc->label, pConn, pConn->meterId, meterId);
      terrno = TSDB_CODE_MISMATCHED_METER_ID;
      pConn = NULL;
    }
  }

  return pConn;
}

SRpcConn *rpcSetConnToServer(void *shandle, SRpcIpSet ipSet) {
  SRpcInfo *pRpc = (SRpcInfo *)shandle;

  SRpcConn *pConn = taosGetConnFromCache(pRpc->pCache, ipSet.ip[ipSet.index], ipSet.port, pRpc->meterId);

  if ( pConn == NULL ) {
    pConn = rpcOpenConn(pRpc, ipSet.ipStr[ipSet.index], ipSet.port);
  } 

  return pConn;
}

static int rpcProcessReqHeader(SRpcConn *pConn, SRpcHeader *pHeader) {
    SRpcInfo *pRpc= pConn->pRpc;

    if (pConn->peerId == 0) {
      pConn->peerId = pHeader->sourceId;
    } else {
      if (pConn->peerId != pHeader->sourceId) {
        tTrace("%s pConn:%p, source Id is changed, old:0x%08x new:0x%08x", pRpc->label, pConn, 
               pConn->peerId, pHeader->sourceId);
        return TSDB_CODE_INVALID_VALUE;
      }
    }

    if (pConn->inTranId == pHeader->tranId) {
      if (pConn->inType == pHeader->msgType) {
        tTrace("%s pConn:%p, %s is retransmitted", pRpc->label, pConn, taosMsg[pHeader->msgType]);
        rpcSendQuickRsp(pConn, TSDB_CODE_ACTION_IN_PROGRESS);
      } else if (pConn->inType == 0) {
        tTrace("%s pConn:%p, %s is already processed, tranId:%d", pRpc->label, pConn, 
                taosMsg[pHeader->msgType], pConn->inTranId);
        rpcSendDataToPeer(pConn, pConn->pRspMsg, pConn->rspMsgLen); // resend the response
      } else {
        tTrace("%s pConn:%p, mismatched message %s and tranId", pRpc->label, pConn, taosMsg[pHeader->msgType]);
      }

      // do not reply any message
      return TSDB_CODE_ALREADY_PROCESSED;
    }

    if (pConn->inType != 0) {
      tTrace("%s pConn:%p, last session is not finished, inTranId:%d tranId:%d", pRpc->label, pConn, 
              pConn->inTranId, pHeader->tranId);
      return TSDB_CODE_LAST_SESSION_NOT_FINISHED;
    }

    pConn->inTranId = pHeader->tranId;
    pConn->inType = pHeader->msgType;

    return 0;
}

static int rpcProcessRspHeader(SRpcConn *pConn, SRpcHeader *pHeader) {
  SRpcInfo *pRpc = pConn->pRpc;
  pConn->peerId = pHeader->sourceId;

  if (pConn->outType == 0 || pConn->pContext == NULL) {
    return TSDB_CODE_UNEXPECTED_RESPONSE;
  }

  if (pHeader->tranId != pConn->outTranId) {
    return TSDB_CODE_INVALID_TRAN_ID;
  }

  if (pHeader->msgType != pConn->outType + 1) {
    return TSDB_CODE_INVALID_RESPONSE_TYPE;
  }

  if (*pHeader->content == TSDB_CODE_NOT_READY) {
    return TSDB_CODE_ALREADY_PROCESSED;
  }

  taosTmrStopA(&pConn->pTimer);
  pConn->retry = 0;

  if (*pHeader->content == TSDB_CODE_ACTION_IN_PROGRESS || pHeader->tcp) {
    if (pConn->tretry <= tsRpcMaxRetry) {
      pConn->tretry++;
      tTrace("%s pConn:%p, peer is still processing the transaction", pRpc->label, pConn);
      taosTmrReset(rpcProcessRetryTimer, tsRpcProgressTime, pConn, pRpc->tmrCtrl, &pConn->pTimer);
      return TSDB_CODE_ALREADY_PROCESSED;
    } else {
      // peer still in processing, give up
      *pHeader->content = TSDB_CODE_TOO_SLOW;
    }
  }

  pConn->tretry = 0;
  pConn->outType = 0;
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;

  return TSDB_CODE_SUCCESS;
}

static int rpcProcessHeader(SRpcInfo *pRpc, SRpcConn **ppConn, void *data, int dataLen, uint32_t ip) {
  int32_t    sid, code = 0;
  SRpcConn * pConn = NULL;
  char       hashstr[40] = {0};

  *ppConn = NULL;
  SRpcHeader *pHeader = (SRpcHeader *)data;

  sid = htonl(pHeader->destId);

  if (pHeader->msgType >= TSDB_MSG_TYPE_MAX || pHeader->msgType <= 0) {
    tTrace("%s sid:%d, invalid message type:%d", pRpc->label, sid, pHeader->msgType);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  pHeader->msgLen = (int32_t)htonl((uint32_t)pHeader->msgLen);
  if (dataLen != pHeader->msgLen) {
    tTrace("%s sid:%d, %s has invalid length, dataLen:%d, msgLen:%d", pRpc->label, sid,
           taosMsg[pHeader->msgType], dataLen, pHeader->msgLen);
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  if (sid < 0 || sid >= pRpc->sessions) {
    tTrace("%s sid:%d, sid is out of range, max sid:%d, %s discarded", pRpc->label, sid,
           pRpc->sessions, taosMsg[pHeader->msgType]);
    return TSDB_CODE_INVALID_SESSION_ID;
  }

  if (sid == 0) sprintf(hashstr, "%x:%x:%x", ip, pHeader->uid, pHeader->sourceId);
  pConn = rpcGetConnObj(pRpc, sid, pHeader->meterId, hashstr);
  if (pConn == NULL ) return terrno;

  *ppConn = pConn;
  sid = pConn->sid;

  if (pHeader->uid) pConn->peerUid = pHeader->uid;

  if (pHeader->tcp) {
    tTrace("%s pConn:%p, content will be transfered via TCP", pRpc->label, pConn);
    if (pConn->outType) taosTmrReset(rpcProcessRetryTimer, tsRpcTimer, pConn, pRpc->tmrCtrl, &pConn->pTimer);
    return TSDB_CODE_ALREADY_PROCESSED;
  }

  code = rpcCheckAuthentication(pConn, (char *)pHeader, dataLen);
  if ( code != 0 ) return code;

  if (pHeader->msgType != TSDB_MSG_TYPE_REG && pHeader->encrypt) {
    // decrypt here
  }

  if ( rpcIsReq(pHeader->msgType) ) {
    code = rpcProcessReqHeader(pConn, pHeader);
  } else {
    code = rpcProcessRspHeader(pConn, pHeader);
  }

  return code;
}

static void *rpcProcessDataFromPeer(void *data, int dataLen, uint32_t ip, uint16_t port, void *shandle, void *chandle) {
  SRpcHeader  *pHeader = (SRpcHeader *)data;
  SRpcInfo    *pRpc = (SRpcInfo *)shandle;
  SRpcConn    *pConn = NULL;
  uint8_t      code = 0;

  tDump(data, dataLen);

  pthread_mutex_lock(&pRpc->mutex);

  code = rpcProcessHeader(pRpc, &pConn, data, dataLen, ip);

  if (pConn) {
    // update connection info
    pConn->chandle = chandle;
    if (pConn->peerIp != ip) {
      pConn->peerIp = ip;
      char ipstr[20] = {0};
      tinet_ntoa(ipstr, ip);
      strcpy(pConn->peerIpstr, ipstr);
    }
  
    if (port) pConn->peerPort = port;
    if (pHeader->port)  // port maybe changed by the peer
      pConn->peerPort = pHeader->port;
  }

  pthread_mutex_unlock(&pRpc->mutex);

  if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16)) {
    tTrace("%s pConn:%p, %s received from 0x%x:%hu, parse code:%u len:%d source:0x%08x dest:0x%08x tranId:%d",
        pRpc->label, pConn, taosMsg[pHeader->msgType], ip, port, code, 
        dataLen, pHeader->sourceId, pHeader->destId, pHeader->tranId);
  }

  if (pConn && pRpc->idleTime) {
    taosTmrReset(rpcProcessIdleTimer, pRpc->idleTime, pConn, pRpc->tmrCtrl, &pConn->pIdleTimer);
  }

  if (code != TSDB_CODE_ALREADY_PROCESSED) {
    if (code != 0) { // parsing error
      if ( rpcIsReq(pHeader->msgType) ) {
        rpcSendErrorMsgToPeer(pRpc, data, code, ip, port, chandle);
        tTrace("%s pConn:%p, %s is sent with error code:%u", pRpc->label, pConn, taosMsg[pHeader->msgType+1], code);
      } 
    } else { // parsing OK
      rpcProcessIncomingMsg(pConn, pHeader);
    }
  }

  if ( code != 0 ) free (data);
  return pConn;
}

static void rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHeader *pHeader) {
  SRpcInfo *pRpc = pConn->pRpc;
  int       msgLen = rpcContFromHeader(pHeader->msgLen);

  pHeader = rpcDecompressRpcMsg(pHeader);
   
  if ( rpcIsReq(pHeader->msgType) ) {
    taosTmrReset(rpcProcessProgressTimer, tsRpcTimer/2, pConn, pRpc->tmrCtrl, &pConn->pTimer);
    (*(pRpc->fp))(pHeader->msgType, pHeader->content, msgLen, pConn, 0);
  } else {
    // it's a response
    STaosRsp *pRsp = (STaosRsp *)pHeader->content;
    int32_t code = htonl(pRsp->code);

    SRpcReqContext *pContext = pConn->pContext;
    pConn->pContext = NULL;

    taosAddConnIntoCache(pRpc->pCache, pConn, pConn->peerIp, pConn->peerPort, pConn->meterId);    

    if (code == TSDB_CODE_NO_MASTER) {
      pContext->code = code;
      taosTmrStart(rpcProcessConnError, 0, pContext, pRpc->tmrCtrl); 
    } else {
      rpcFreeMsg(rpcHeaderFromCont(pContext->pCont)); // free the request msg
      (*(pRpc->fp))(pHeader->msgType, pHeader->content, msgLen, pContext->ahandle, pContext->ipSet.index);
    }
  }
}

static void rpcSendQuickRsp(SRpcConn *pConn, char code) {
  char         msg[RPC_MSG_OVERHEAD + sizeof(STaosRsp)];
  SRpcHeader  *pHeader;
  int          msgLen;
  STaosRsp    *pRsp;

  pRsp = (STaosRsp *)rpcContFromHeader(msg);
  pRsp->code = htonl(code);
  msgLen = sizeof(STaosRsp);

  // set msg header
  memset(msg, 0, sizeof(SRpcHeader));
  pHeader = (SRpcHeader *)msg;
  pHeader->version = 1;
  pHeader->msgType = pConn->inType+1;
  pHeader->spi = 0;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pHeader->tranId = pConn->inTranId;
  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;
  pHeader->uid = 0;
  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  rpcSendDataToPeer(pConn, msg, msgLen);
}

static void rpcSendErrorMsgToPeer(SRpcInfo *pRpc, char *pMsg, int32_t code, uint32_t ip, uint16_t port, void *chandle) {
  SRpcHeader  *pRecvHeader, *pReplyHeader;
  char         msg[sizeof(SRpcHeader) + sizeof(SRpcDigest) + sizeof(STaosRsp)];
  STaosRsp    *pRsp;
  uint32_t     timeStamp;
  int          msgLen;

  pRecvHeader = (SRpcHeader *)pMsg;
  pReplyHeader = (SRpcHeader *)msg;

  memset(msg, 0, sizeof(SRpcHeader));
  pReplyHeader->version = pRecvHeader->version;
  pReplyHeader->msgType = (char)(pRecvHeader->msgType + 1);
  pReplyHeader->tcp = 0;
  pReplyHeader->spi = 0;
  pReplyHeader->encrypt = 0;
  pReplyHeader->tranId = pRecvHeader->tranId;
  pReplyHeader->sourceId = 0;
  pReplyHeader->destId = pRecvHeader->sourceId;
  memcpy(pReplyHeader->meterId, pRecvHeader->meterId, tListLen(pReplyHeader->meterId));

  pRsp = (STaosRsp *)pReplyHeader->content;
  pRsp->code = htonl(code);
  msgLen = sizeof(STaosRsp);
  char *pContent = pRsp->more;

  if (code == TSDB_CODE_INVALID_TIME_STAMP) {
    // include a time stamp if client's time is not synchronized well
    timeStamp = taosGetTimestampSec();
    memcpy(pContent, &timeStamp, sizeof(timeStamp));
    msgLen += sizeof(timeStamp);
  }

  pReplyHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
  (*taosSendData[pRpc->connType])(ip, port, msg, msgLen, chandle);

  return; 
}

static void rpcSendReqToServer(SRpcConn *pConn, SRpcReqContext *pContext) {
  SRpcHeader  *pHeader = rpcHeaderFromCont(pContext->pCont);
  SRpcInfo    *pRpc = pConn->pRpc;
  char        *msg = (char *)pHeader;
  int          msgLen = rpcMsgLenFromCont(pContext->contLen);
  char         msgType = pContext->msgType;

  pthread_mutex_lock(&pRpc->mutex);

  // set the message header  
  pHeader->version = 1;
  pHeader->msgType = msgType;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pConn->tranId++;
  if ( pConn->tranId == 0 ) pConn->tranId++;
  pHeader->tranId = pConn->tranId;
  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;
  pHeader->port = 0;
  pHeader->uid = (uint32_t)((int64_t)pConn + (int64_t)getpid());
  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  // set the connection parameters
  pConn->outType = msgType;
  pConn->outTranId = pHeader->tranId;
  pConn->pReqMsg = msg;
  pConn->reqMsgLen = msgLen;
  pConn->pContext = pContext;

  pthread_mutex_unlock(&pRpc->mutex);

  rpcSendDataToPeer(pConn, msg, msgLen);
  taosTmrReset(rpcProcessRetryTimer, tsRpcTimer, pConn, pRpc->tmrCtrl, &pConn->pTimer);
}
 
static void rpcSendDataToPeer(SRpcConn *pConn, void *data, int dataLen) {
  int          writtenLen = 0;
  SRpcInfo    *pRpc = pConn->pRpc;
  SRpcHeader  *pHeader = (SRpcHeader *)data;

  dataLen = rpcAddAuthPart(pConn, data, dataLen);

  if ( rpcIsReq(pHeader->msgType)) {
    if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace("%s pConn:%p, %s is sent to %s:%hu, len:%d source:0x%08x dest:0x%08x tranId:%d",
             pRpc->label, pConn, taosMsg[pHeader->msgType], pConn->peerIpstr,
             pConn->peerPort, dataLen, pHeader->sourceId, pHeader->destId, pHeader->tranId);
  } else {
    if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace( "%s pConn:%p, %s is sent to %s:%hu, code:%u len:%d source:0x%08x dest:0x%08x tranId:%d",
          pRpc->label, pConn, taosMsg[pHeader->msgType], pConn->peerIpstr, pConn->peerPort, 
          (uint8_t)pHeader->content[0], dataLen, pHeader->sourceId, pHeader->destId, pHeader->tranId);
  }

  writtenLen = (*taosSendData[pRpc->connType])(pConn->peerIp, pConn->peerPort, (char *)pHeader, dataLen, pConn->chandle);

  if (writtenLen != dataLen) {
    tError("%s pConn:%p, failed to send, dataLen:%d writtenLen:%d, reason:%s", pRpc->label, pConn, 
           dataLen, writtenLen, strerror(errno));
  }
 
  tDump(data, dataLen);
}

static void rpcProcessConnError(void *param, void *id) {
  SRpcReqContext *pContext = (SRpcReqContext *)param;
  SRpcInfo *pRpc = pContext->pRpc;

  if ( pContext->numOfRetry >= pContext->ipSet.numOfIps ) {
    rpcFreeMsg(rpcHeaderFromCont(pContext->pCont)); // free the request msg
    char *rsp = calloc(1, RPC_MSG_OVERHEAD + sizeof(STaosRsp));
    if ( rsp ) {
      STaosRsp *pRsp = (STaosRsp *)(rsp+sizeof(SRpcHeader));
      pRsp->code = pContext->code;
      (*(pRpc->fp))(pContext->msgType+1, pRsp, sizeof(STaosRsp), pContext->ahandle, 0);  
    } else {
      tError("%s failed to malloc RSP", pRpc->label);
    }
  } else {
    // move to next IP 
    pContext->ipSet.index++;
    pContext->ipSet.index = pContext->ipSet.index % pContext->ipSet.numOfIps;
    
    SRpcConn *pConn = rpcSetConnToServer(pContext->pRpc, pContext->ipSet);
    pContext->code = terrno;
    if (pConn == NULL) taosTmrStart(rpcProcessConnError, 0, pContext, pRpc->tmrCtrl); 

    rpcSendReqToServer(pConn, pContext);
  }
}

static void rpcProcessRetryTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  int       reportDisc = 0;

  SRpcInfo *pRpc = pConn->pRpc;
  if (pRpc == NULL) return;  // it means it is already released

  pthread_mutex_lock(&pRpc->mutex);

  if (pConn->outType && pConn->meterId[0]) {
    tTrace("%s pConn:%p, expected %s is not received", pRpc->label, pConn, taosMsg[(int)pConn->outType + 1]);
    pConn->pTimer = NULL;
    pConn->retry++;

    if (pConn->retry < 4) {
      tTrace("%s pConn:%p, re-send msg:%s to %s:%hu", pRpc->label,
             taosMsg[pConn->outType], pConn->peerIpstr, pConn->peerPort);
      rpcSendDataToPeer(pConn, pConn->pReqMsg, pConn->reqMsgLen);      
      taosTmrReset(rpcProcessRetryTimer, tsRpcTimer<<pConn->retry, pConn, pRpc->tmrCtrl, &pConn->pTimer);
    } else {
      // close the connection
      tTrace("%s pConn:%p, failed to send msg:%s to %s:%hu", pRpc->label, pConn,
              taosMsg[pConn->outType], pConn->peerIpstr, pConn->peerPort);
      reportDisc = 1;
    }
  } else {
    tTrace("%s pConn:%p, retry timer not processed", pRpc->label, pConn);
  }

  pthread_mutex_unlock(&pRpc->mutex);

  pConn->pContext->code = TSDB_CODE_NETWORK_UNAVAIL;
  if (reportDisc) { 
    rpcProcessConnError(pConn->pContext, NULL);
    rpcCloseConn(pConn);
  }
}

static void rpcProcessIdleTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  SRpcInfo *pRpc = pConn->pRpc;
  assert(pRpc);

  pthread_mutex_lock(&pRpc->mutex);

  if (pConn->inType == 0 && pConn->meterId[0]) {
    tTrace("%s pConn:%p, close the connection since no activity", pRpc->label, pConn);
    rpcCloseConn(pConn);
  } else {
    tTrace("%s pConn:%p, idle timer:%p not processed", pRpc->label, pConn, tmrId);
  }

  pthread_mutex_unlock(&pRpc->mutex);
}

static void rpcProcessProgressTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  SRpcInfo *pRpc = pConn->pRpc;

  pthread_mutex_lock(&pRpc->mutex);

  if (pConn->inType && pConn->meterId[0]) {
    tTrace("%s pConn:%p, progress timer expired, send progress", pRpc->label, pConn);
    rpcSendQuickRsp(pConn, TSDB_CODE_ACTION_IN_PROGRESS);
    taosTmrReset(rpcProcessProgressTimer, tsRpcTimer<<pConn->retry, pConn, pRpc->tmrCtrl, &pConn->pTimer);
  } else {
    tTrace("%s pConn:%p, progress timer:%p not processed", pRpc->label, pConn, tmrId);
  }

  pthread_mutex_unlock(&pRpc->mutex);
}

static void rpcFreeMsg(void *msg) {
  if ( msg == NULL ) return;
  char *req = ((char *)msg) - sizeof(SRpcReqContext);
  free(req);
}

static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen) {
  SRpcHeader  *pHeader = rpcHeaderFromCont(pCont);
  int32_t      overhead = sizeof(int32_t) * 2;
  int32_t      finalLen = 0;
  
  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }
  
  char *buf = malloc (contLen + overhead+8);  // 16 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d, reason:%s", contLen, strerror(errno));
    return contLen;
  }
  
  int32_t compLen = LZ4_compress_default(pCont, buf, contLen, contLen + overhead);
  
  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (compLen < contLen - overhead) {
    //tDump(pCont, contLen);
    int32_t *pLen = (int32_t *)pCont;
    
    *pLen = 0;    // first 4 bytes must be zero
    pLen = (int32_t *)(pCont + sizeof(int32_t));
    
    *pLen = htonl(contLen); // contLen is encoded in second 4 bytes
    memcpy(pCont + overhead, buf, compLen);
    
    pHeader->comp = 1;
    tTrace("compress rpc msg, before:%d, after:%d", contLen, compLen);
    
    finalLen = compLen + overhead;
    //tDump(pCont, contLen);
  } else {
    finalLen = contLen;
  }

  free(buf);
  return finalLen;
}

static SRpcHeader *rpcDecompressRpcMsg(SRpcHeader *pHeader) {
  int overhead = sizeof(int32_t) * 2;
  SRpcHeader *pNewHeader = NULL;  
  uint8_t    *pCont = pHeader->content;

  if (pHeader->comp) {
    // decompress the content
    assert(GET_INT32_VAL(pHeader->content) == 0);
  
    // contLen is original message length before compression applied
    int contLen = htonl(GET_INT32_VAL(pCont + sizeof(int32_t)));
  
    // prepare the temporary buffer to decompress message
    char *buf = rpcMallocCont(contLen);
  
    if (buf) {
      pNewHeader = rpcHeaderFromCont(buf);
      int compLen = rpcContLenFromMsg(pHeader->msgLen) - overhead;
      int32_t originalLen = LZ4_decompress_safe((const char*)(pCont + overhead), buf, compLen, contLen);
      assert(originalLen == contLen);
    
      memcpy(pNewHeader, pHeader, sizeof(SRpcHeader));
      pNewHeader->msgLen = rpcMsgLenFromCont(originalLen);
      free(pHeader); // free the compressed message buffer
      pHeader = pNewHeader; 
    } else {
      tError("failed to allocate memory to decompress msg, contLen:%d, reason:%s", contLen, strerror(errno));
    }
  }

  return pHeader;
}

static int rpcAuthenticateMsg(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey) {
  MD5_CTX context;
  int     ret = -1;

  MD5Init(&context);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Update(&context, pMsg, msgLen);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  if (memcmp(context.digest, pAuth, sizeof(context.digest)) == 0) ret = 0;

  return ret;
}

static int rpcBuildAuthHeader(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey) {
  MD5_CTX context;

  MD5Init(&context);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));

  return 0;
}

static int rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHeader *pHeader = (SRpcHeader *)msg;

  if (pConn->spi) {
    // add auth part
    pHeader->spi = pConn->spi;
    SRpcDigest *pDigest = (SRpcDigest *)(msg + msgLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(SRpcDigest);
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
    rpcBuildAuthHeader((uint8_t *)pHeader, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  return msgLen;
}

static int rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHeader *pHeader = (SRpcHeader *)msg;
  SRpcInfo   *pRpc = pConn->pRpc;
  int         code = 0;

  if (pConn->spi == 0 ) return 0;

  if (pHeader->spi == pConn->spi) {
    // authentication
    SRpcDigest *pDigest = (SRpcDigest *)((char *)pHeader + msgLen - sizeof(SRpcDigest));

    int32_t delta;
    delta = (int32_t)htonl(pDigest->timeStamp);
    delta -= (int32_t)taosGetTimestampSec();
    if (abs(delta) > 900) {
      tWarn("%s pConn:%p, time diff:%d is too big, msg discarded, timestamp:%d", pRpc->label, pConn,
             delta, htonl(pDigest->timeStamp));
      code = TSDB_CODE_INVALID_TIME_STAMP;
    } else {
      if (rpcAuthenticateMsg((uint8_t *)pHeader, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret) < 0) {
        tError("%s pConn:%p, authentication failed, msg discarded", pRpc->label, pConn);
        code = TSDB_CODE_AUTH_FAILURE;
      } else {
        pHeader->msgLen -= sizeof(SRpcDigest);
      }
    }
  } else {
    // if it is request or response with code 0, msg shall be discarded
    if (rpcIsReq(pHeader->msgType) || (pHeader->content[0] == 0)) {
      tTrace("%s pConn:%p, auth spi not matched, msg discarded", pRpc->label, pConn);
      code = TSDB_CODE_AUTH_FAILURE;
    }
  }

  return code;
}


