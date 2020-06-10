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
#include "trpc.h"
#include "tsdb.h"
#include "tsocket.h"
#include "ttcpclient.h"
#include "ttcpserver.h"
#include "ttime.h"
#include "ttimer.h"
#include "tudp.h"
#include "tutil.h"
#include "lz4.h"
#include "tglobalcfg.h"

typedef struct _msg_node {
  struct _msg_node *next;
  void *            ahandle;
  int               msgLen;
} SMsgNode;

typedef struct {
  void *   signature;
  int      chann;   // channel ID
  int      sid;     // session ID
  uint32_t ownId;   // own link ID
  uint32_t peerId;  // peer link ID
  char     meterId[TSDB_UNI_LEN];
  char     spi;
  char     encrypt;
  uint8_t  secret[TSDB_KEY_LEN];
  uint8_t  ckey[TSDB_KEY_LEN];
  uint16_t           localPort;      // for UDP only
  uint32_t           peerUid;
  uint32_t           peerIp;         // peer IP
  uint16_t           peerPort;       // peer port
  char               peerIpstr[20];  // peer IP string
  uint16_t           tranId;         // outgoing transcation ID, for build message
  uint16_t           outTranId;      // outgoing transcation ID
  uint16_t           inTranId;
  uint8_t            outType;
  char               inType;
  char               closing;
  char               rspReceived;
  void *             chandle;  // handle passed by TCP/UDP connection layer
  void *             ahandle;  // handle returned by upper app layter
  int                retry;
  int                tretry;   // total retry
  void *             pTimer;
  void *             pIdleTimer;
  char *             pRspMsg;
  char *             pQuickRsp;
  int                rspMsgLen;
  SMsgNode *         pMsgNode;
  SMsgNode *         pHead, *pTail;
  struct rpc_server *pServer;
} SRpcConn;

typedef struct {
  int             sessions;
  void *          qhandle; // for scheduler
  SRpcConn *      connList;
  void *          idPool;
  void *          tmrCtrl;
  void *          hash;
  pthread_mutex_t mutex;
} SRpcChann;

typedef struct rpc_server {
  void *shandle;  // returned handle from lower layer during initialization
  void *qhandle;  // for scheduler
  int   bits;     // number of bits for session ID
  int   mask;
  int   numOfChanns;
  int   numOfThreads;
  int   idMgmt;   // ID management method
  int   type;
  int   idleTime; // milliseconds;
  int   noFree;   // do not free the request msg when rsp is received
  int   index;    // for UDP server, next thread for new connection
  uint16_t localPort;
  char  label[12];
  void *(*fp)(char *, void *ahandle, void *thandle);
  void (*efp)(int);                                                                     // FP to report error
  int (*afp)(char *meterId, char *spi, char *encrypt, uint8_t *secret, uint8_t *ckey);  // FP to retrieve auth info
  int (*ufp)(char *user, int32_t *failCount, int32_t *allowTime, bool opSet);           // FP to update auth fail retry info
  SRpcChann *channList;
} STaosRpc;

int tsRpcProgressTime = 10;  // milliseocnds

// not configurable
int tsRpcMaxRetry;
int tsRpcHeadSize;

void *(*taosInitConn[])(char *ip, uint16_t port, char *label, int threads, void *fp, void *shandle) = {
    taosInitUdpServer, taosInitUdpClient, taosInitTcpServer, taosInitTcpClient};

void (*taosCleanUpConn[])(void *thandle) = {taosCleanUpUdpConnection, taosCleanUpUdpConnection, taosCleanUpTcpServer,
                                            taosCleanUpTcpClient};

int (*taosSendData[])(uint32_t ip, uint16_t port, char *data, int len, void *chandle) = {
    taosSendUdpData, taosSendUdpData, taosSendTcpServerData, taosSendTcpClientData};

void *(*taosOpenConn[])(void *shandle, void *thandle, char *ip, uint16_t port) = {
    taosOpenUdpConnection,
    taosOpenUdpConnection,
    NULL,
    taosOpenTcpClientConnection,
};

void (*taosCloseConn[])(void *chandle) = {NULL, NULL, taosCloseTcpServerConnection, taosCloseTcpClientConnection};

int   taosReSendRspToPeer(SRpcConn *pConn);
void  taosProcessTaosTimer(void *, void *);
void *taosProcessDataFromPeer(char *data, int dataLen, uint32_t ip, uint16_t port, void *shandle, void *thandle,
                              void *chandle);
int   taosSendDataToPeer(SRpcConn *pConn, char *data, int dataLen);
void  taosProcessSchedMsg(SSchedMsg *pMsg);
int   taosAuthenticateMsg(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey);
int   taosBuildAuthHeader(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey);

static int32_t taosCompressRpcMsg(char* pCont, int32_t contLen) {
  STaosHeader* pHeader = (STaosHeader *)(pCont - sizeof(STaosHeader));
  int32_t overhead = sizeof(int32_t) * 2;
  int32_t finalLen = 0;
  
  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }
  
  char *buf = malloc (contLen + overhead + 8);  // 16 extra bytes
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

static STaosHeader* taosDecompressRpcMsg(STaosHeader* pHeader, SSchedMsg* pSchedMsg, int32_t msgLen) {
  int overhead = sizeof(int32_t) * 2;
  
  if (pHeader->comp == 0) {
    pSchedMsg->msg = (char *)(&(pHeader->destId));
    return pHeader;
  }
  
  // decompress the content
  assert(GET_INT32_VAL(pHeader->content) == 0);
  
  // contLen is original message length before compression applied
  int contLen = htonl(GET_INT32_VAL(pHeader->content + sizeof(int32_t)));
  
  // prepare the temporary buffer to decompress message
  char *buf = malloc(sizeof(STaosHeader) + contLen);
  
  //tDump(pHeader->content, msgLen);
  
  if (buf) {
    int32_t originalLen = LZ4_decompress_safe((const char*)(pHeader->content + overhead), buf + sizeof(STaosHeader),
        msgLen - overhead, contLen);
    
    memcpy(buf, pHeader, sizeof(STaosHeader));
    free(pHeader); // free the compressed message buffer
  
    STaosHeader* pNewHeader = (STaosHeader *) buf;
    pNewHeader->msgLen = originalLen + (int) sizeof(SIntMsg);
    assert(originalLen == contLen);

    pSchedMsg->msg = (char *)(&(pNewHeader->destId));
    //tDump(pHeader->content, contLen);
    return pNewHeader;
  } else {
    tError("failed to allocate memory to decompress msg, contLen:%d, reason:%s", contLen, strerror(errno));
    pSchedMsg->msg = NULL;
  }

  return NULL;
}

char *taosBuildReqHeader(void *param, char type, char *msg) {
  STaosHeader *pHeader;
  SRpcConn *   pConn = (SRpcConn *)param;

  if (pConn == NULL || pConn->signature != pConn) {
    tError("pConn:%p, connection has to be openned first before building a message", pConn);
    return NULL;
  }

  pHeader = (STaosHeader *)(msg + sizeof(SMsgNode));
  memset(pHeader, 0, sizeof(STaosHeader));
  pHeader->version = 1;
  pHeader->comp = 0;
  pHeader->msgType = type;
  pHeader->spi = 0;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pHeader->tranId = atomic_add_fetch_16(&pConn->tranId, 1);
  if (pHeader->tranId == 0) pHeader->tranId = atomic_add_fetch_16(&pConn->tranId, 1);

  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;
  pHeader->port = 0;

  pHeader->uid = (uint32_t)((int64_t)pConn + (int64_t)getpid());

  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  return (char *)pHeader->content;
}

char *taosBuildReqMsgWithSize(void *param, char type, int size) {
  STaosHeader *pHeader;
  char *       pMsg;
  SRpcConn *   pConn = (SRpcConn *)param;

  if (pConn == NULL || pConn->signature != pConn) {
    tError("pConn:%p, connection has to be openned first before building a message", pConn);
    return NULL;
  }

  size += sizeof(SMsgNode) + sizeof(STaosHeader) + sizeof(STaosDigest);
  pMsg = (char *)malloc((size_t)size);
  memset(pMsg, 0, (size_t)size);
  pHeader = (STaosHeader *)(pMsg + sizeof(SMsgNode));
  pHeader->version = 1;
  pHeader->msgType = type;
  pHeader->spi = 0;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pHeader->tranId = atomic_add_fetch_32(&pConn->tranId, 1);
  if (pHeader->tranId == 0) pHeader->tranId = atomic_add_fetch_32(&pConn->tranId, 1);

  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;

  pHeader->uid = (uint32_t)((int64_t)pConn + (int64_t)getpid());

  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  return (char *)pHeader->content;
}

char *taosBuildRspMsgWithSize(void *param, char type, int size) {
  STaosHeader *pHeader;
  char *       pMsg;
  SRpcConn *   pConn = (SRpcConn *)param;

  if (pConn == NULL || pConn->signature != pConn) {
    tError("pConn:%p, connection has to be opened first before building a message", pConn);
    return NULL;
  }

  size += sizeof(SMsgNode) + sizeof(STaosHeader) + sizeof(STaosDigest);
  pMsg = (char *)malloc((size_t)size);
  if (pMsg == NULL) {
    tError("pConn:%p, malloc(%d) failed when building a type:%d message", pConn, size, type);
    return NULL;
  }

  memset(pMsg, 0, (size_t)size);
  pHeader = (STaosHeader *)pMsg;
  pHeader->version = 1;
  pHeader->msgType = type;
  pHeader->spi = 0;
  pHeader->tcp = 0;
  pHeader->encrypt = 0;
  pHeader->tranId = pConn->inTranId;
  pHeader->sourceId = pConn->ownId;
  pHeader->destId = pConn->peerId;
  pHeader->uid = 0;
  memcpy(pHeader->meterId, pConn->meterId, tListLen(pHeader->meterId));

  return (char *)pHeader->content;
}

int taosSendSimpleRsp(void *thandle, char rsptype, char code) {
  char *pMsg, *pStart;
  int   msgLen;

  if (thandle == NULL) {
    tError("connection is gone, response could not be sent");
    return -1;
  }

  pStart = taosBuildRspMsgWithSize(thandle, rsptype, 32);
  if (pStart == NULL) {
    tError("build rsp msg error, return null prt");
    return -1;
  }
  pMsg = pStart;

  *pMsg = code;
  pMsg++;

  msgLen = (int)(pMsg - pStart);
  taosSendMsgToPeer(thandle, pStart, msgLen);

  return msgLen;
}

int taosSendQuickRsp(void *thandle, char rsptype, char code) {
  char *       pCont;
  int          contLen;
  STaosHeader *pHeader;
  char *       msg;
  int          msgLen;
  SRpcConn *   pConn = (SRpcConn *)thandle;

  pCont = taosBuildRspMsgWithSize(thandle, rsptype, 32);
  if (pCont == NULL) return 0;

  *pCont = code;
  contLen = 1;

  pHeader = (STaosHeader *)(pCont - sizeof(STaosHeader));
  msg = (char *)pHeader;
  msgLen = contLen + (int32_t)sizeof(STaosHeader);

  if (pConn->spi) {
    // add auth part
    pHeader->spi = pConn->spi;
    STaosDigest *pDigest = (STaosDigest *)(pCont + contLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(STaosDigest);
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
    taosBuildAuthHeader((uint8_t *)pHeader, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  tfree(pConn->pQuickRsp);
  pConn->pQuickRsp = msg;
  taosSendDataToPeer(pConn, (char *)pHeader, msgLen);

  return msgLen;
}

void *taosOpenRpc(SRpcInit *pRpc) {
  STaosRpc *pServer;

  tsRpcMaxRetry = tsRpcMaxTime * 1000 / tsRpcProgressTime;
  tsRpcHeadSize = sizeof(STaosHeader) + sizeof(SMsgNode);

  pServer = (STaosRpc *)malloc(sizeof(STaosRpc));
  if (pServer == NULL) return NULL;
  memset(pServer, 0, sizeof(STaosRpc));

  pServer->bits = pRpc->bits;
  pServer->mask = (1 << (pRpc->bits)) - 1;
  pServer->numOfChanns = pRpc->numOfChanns;
  strcpy(pServer->label, pRpc->label);
  pServer->fp = pRpc->fp;
  pServer->idMgmt = pRpc->idMgmt;
  pServer->type = pRpc->connType;
  pServer->idleTime = pRpc->idleTime;
  pServer->noFree = pRpc->noFree;
  pServer->numOfThreads = pRpc->numOfThreads;
  if (pServer->numOfThreads > TSDB_MAX_RPC_THREADS) {
    pServer->numOfThreads = TSDB_MAX_RPC_THREADS;
    pRpc->numOfThreads = TSDB_MAX_RPC_THREADS;
  }
  pServer->localPort = pRpc->localPort;
  pServer->qhandle = pRpc->qhandle;
  pServer->efp = pRpc->efp;
  pServer->afp = pRpc->afp;
  pServer->ufp = pRpc->ufp;

  int size = (int)sizeof(SRpcChann) * pRpc->numOfChanns;
  pServer->channList = (SRpcChann *)malloc((size_t)size);
  if (pServer->channList == NULL) {
    tError("%s, failed to malloc channList", pRpc->label);
    tfree(pServer);
    return NULL;
  }
  memset(pServer->channList, 0, (size_t)size);

  pServer->shandle = (*taosInitConn[pRpc->connType])(pRpc->localIp, pRpc->localPort, pRpc->label, pRpc->numOfThreads,
                                                     taosProcessDataFromPeer, pServer);
  if (pServer->shandle == NULL) {
    tError("%s, failed to init network, %s:%d", pRpc->label, pRpc->localIp, pRpc->localPort);
    taosCloseRpc(pServer);
    return NULL;
  }

  if (pServer->numOfChanns == 1) {
    int retVal = taosOpenRpcChann(pServer, 0, pRpc->sessionsPerChann);
    if (0 != retVal) {
      tError("%s, failed to open rpc chann", pRpc->label);
      taosCloseRpc(pServer);
      return NULL;
    }
  }

  tTrace("%s RPC is openned, numOfThreads:%d", pRpc->label, pRpc->numOfThreads);

  return pServer;
}

int taosOpenRpcChannWithQ(void *handle, int cid, int sessions, void *qhandle) {
  STaosRpc * pServer = (STaosRpc *)handle;
  SRpcChann *pChann;

  tTrace("cid:%d, handle:%p open rpc chann", cid, handle);

  if (pServer == NULL) return -1;
  if (cid >= pServer->numOfChanns || cid < 0) {
    tError("%s: cid:%d, chann is out of range, max:%d", pServer->label, cid, pServer->numOfChanns);
    return -1;
  }

  pChann = pServer->channList + cid;
  memset(pChann, 0, sizeof(SRpcChann));

  size_t size = sizeof(SRpcConn) * sessions;
  pChann->connList = (SRpcConn *)calloc(1, size);
  if (pChann->connList == NULL) {
    tError("%s cid:%d, failed to allocate memory for taos connections, size:%d", pServer->label, cid, size);
    return -1;
  }

  if (pServer->idMgmt == TAOS_ID_FREE) {
    pChann->idPool = taosInitIdPool(sessions);
    if (pChann->idPool == NULL) {
      tError("%s cid:%d, failed to init ID pool", pServer->label, cid);
      return -1;
    }
  }

  pChann->tmrCtrl = taosTmrInit(sessions * 2 + 1, 50, 10000, pServer->label);
  if (pChann->tmrCtrl == NULL) {
    tError("%s cid:%d, failed to init timers", pServer->label, cid);
    return -1;
  }

  pChann->hash = taosInitStrHash(sessions, sizeof(pChann), taosHashString);
  if (pChann->hash == NULL) {
    tError("%s cid:%d, failed to init string hash", pServer->label, cid);
    return -1;
  }

  pthread_mutex_init(&pChann->mutex, NULL);
  pChann->sessions = sessions;

  pChann->qhandle = qhandle ? qhandle : pServer->qhandle;

  return TSDB_CODE_SUCCESS;
}

void taosCloseRpcChann(void *handle, int cid) {
  STaosRpc * pServer = (STaosRpc *)handle;
  SRpcChann *pChann;

  tTrace("cid:%d, handle:%p close rpc chann", cid, handle);

  if (pServer == NULL) return;
  if (cid >= pServer->numOfChanns || cid < 0) {
    tError("%s cid:%d, chann is out of range, max:%d", pServer->label, cid, pServer->numOfChanns);
    return;
  }

  pChann = pServer->channList + cid;

  for (int i = 0; i < pChann->sessions; ++i) {
    if (pChann->connList[i].signature != NULL) {
      taosCloseRpcConn((void *)(pChann->connList + i));
    }
  }

  taosCleanUpStrHash(pChann->hash);
  taosTmrCleanUp(pChann->tmrCtrl);
  taosIdPoolCleanUp(pChann->idPool);
  tfree(pChann->connList);
  pthread_mutex_destroy(&pChann->mutex);

  memset(pChann, 0, sizeof(SRpcChann));
}

void taosCloseRpcConn(void *thandle) {
  SRpcConn *pConn = (SRpcConn *)thandle;
  if (pConn == NULL) return;

  STaosRpc *pServer = pConn->pServer;
  if (pConn->signature != thandle || pServer == NULL) return;
  if (pConn->closing) return;
  SRpcChann *pChann = pServer->channList + pConn->chann;

  pthread_mutex_lock(&pChann->mutex);

  pConn->closing = 1;
  pConn->signature = NULL;

  if (taosCloseConn[pServer->type]) (*taosCloseConn[pServer->type])(pConn->chandle);

  taosTmrStopA(&pConn->pTimer);
  taosTmrStopA(&pConn->pIdleTimer);
  tfree(pConn->pRspMsg);

  if (pServer->noFree == 0) free(pConn->pMsgNode);
  pConn->pMsgNode = NULL;

  tfree(pConn->pQuickRsp);

  SMsgNode *pMsgNode;
  while (pConn->pHead) {
    pMsgNode = pConn->pHead;
    pConn->pHead = pConn->pHead->next;
    memset(pMsgNode, 0, sizeof(SMsgNode));
    if (pServer->noFree == 0) free(pMsgNode);
  }

  char hashstr[40] = {0};
  sprintf(hashstr, "%x:%x:%x", pConn->peerIp, pConn->peerUid, pConn->peerId);
  taosDeleteStrHash(pChann->hash, hashstr);

  tTrace("%s cid:%d sid:%d id:%s, TAOS connection closed, pConn:%p", pServer->label, pConn->chann, pConn->sid,
         pConn->meterId, pConn);
  int freeId = pConn->sid;
  memset(pConn, 0, sizeof(SRpcConn));

  if (pChann->idPool) taosFreeId(pChann->idPool, freeId);

  pthread_mutex_unlock(&pChann->mutex);
}

int taosGetRpcConn(int chann, int sid, char *meterId, STaosRpc *pServer, SRpcConn **ppConn, char req, char *hashstr) {
  SRpcConn * pConn = NULL;
  SRpcChann *pChann;

  if (pServer == NULL) return -1;
  pChann = pServer->channList + chann;

  if (pServer->idMgmt == TAOS_ID_FREE) {
    if (sid == 0) {
      if (req) {
        int        osid = sid;
        SRpcConn **ppConn = (SRpcConn **)taosGetStrHashData(pChann->hash, hashstr);
        if (ppConn) pConn = *ppConn;
        if (pConn == NULL) {
          sid = taosAllocateId(pChann->idPool);
          if (sid <= 0) {
            tError("%s cid:%d, maximum number of sessions:%d is reached", pServer->label, chann, pChann->sessions);
            return TSDB_CODE_MAX_SESSIONS;
          } else {
            tTrace("%s cid:%d sid:%d, ID allocated, used:%d, old id:%d", pServer->label, chann, sid,
                   taosIdPoolNumOfUsed(pChann->idPool), osid);
          }
        } else {
          sid = pConn->sid;
          tTrace("%s cid:%d sid:%d id:%s, session is already there", pServer->label, pConn->chann, pConn->sid,
                 pConn->meterId);
        }
      } else {
        return TSDB_CODE_UNEXPECTED_RESPONSE;
      }
    } else {
      if (pChann->connList[sid].signature == NULL) {
        tError("%s cid:%d, sid:%d session is already released", pServer->label, chann, sid);
        return TSDB_CODE_INVALID_VALUE;
      }
    } 
  }

  pConn = pChann->connList + sid;
  if (pChann == NULL || pChann->connList == NULL) {
    tTrace("%s cid:%d sid:%d, connlist is null, received:%s", pServer->label, chann, sid, meterId);
    return TSDB_CODE_MISMATCHED_METER_ID;
  }

  if (pConn->signature == NULL) {
    memset(pConn, 0, sizeof(SRpcConn));
    pConn->signature = pConn;
    memcpy(pConn->meterId, meterId, tListLen(pConn->meterId));
    pConn->pServer = pServer;
    pConn->chann = chann;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl((uint32_t)((pConn->chann << pServer->bits) + pConn->sid));
    if (pServer->afp) {
      int ret = (*pServer->afp)(meterId, &pConn->spi, &pConn->encrypt, pConn->secret, pConn->ckey);
      if (ret != 0) {
        tWarn("%s cid:%d sid:%d id:%s, meterId not there pConn:%p", pServer->label, chann, sid, pConn->meterId, pConn);
        taosFreeId(pChann->idPool, sid);   // sid shall be released
        memset(pConn, 0, sizeof(SRpcConn)); 
        return ret;
      }
    }

    if ((pServer->type == TAOS_CONN_UDPC || pServer->type == TAOS_CONN_UDPS) && pServer->numOfThreads > 1 &&
        pServer->localPort) {
      // UDP server, assign to new connection
      pServer->index = (pServer->index + 1) % pServer->numOfThreads;
      pConn->localPort = (int16_t)(pServer->localPort + pServer->index);
    }

    taosAddStrHash(pChann->hash, hashstr, (char *)&pConn);
    tTrace("%s cid:%d sid:%d id:%s, TAOS connection is allocated, localPort:%d pConn:%p", pServer->label, chann, sid,
           pConn->meterId, pConn->localPort, pConn);
  } else {
    if (memcmp(pConn->meterId, meterId, tListLen(pConn->meterId)) != 0) {
      tTrace("%s cid:%d sid:%d id:%s, meterId is not matched, received:%s", pServer->label, chann, sid, pConn->meterId,
             meterId);
      return TSDB_CODE_MISMATCHED_METER_ID;
    }
  }

  *ppConn = pConn;

  return TSDB_CODE_SUCCESS;
}

void *taosOpenRpcConn(SRpcConnInit *pInit, uint8_t *code) {
  SRpcConn *pConn;
  STaosRpc *pServer = (STaosRpc *)pInit->shandle;

  *code = (uint8_t)(taosGetRpcConn(pInit->cid, pInit->sid, pInit->meterId, pServer, &pConn, 1, NULL));
  if (*code == TSDB_CODE_MAX_SESSIONS) *code = TSDB_CODE_MAX_CONNECTIONS;
  if (*code != TSDB_CODE_SUCCESS) return NULL;

  if (pConn->peerId == 0) pConn->peerId = pInit->peerId;

  strcpy(pConn->peerIpstr, pInit->peerIp);
  pConn->peerIp = inet_addr(pInit->peerIp);
  pConn->peerPort = pInit->peerPort;
  pConn->ahandle = pInit->ahandle;
  pConn->spi = pInit->spi;
  pConn->encrypt = pInit->encrypt;
  if (pConn->spi) memcpy(pConn->secret, pInit->secret, TSDB_KEY_LEN);

  // if it is client, it shall set up connection first
  if (taosOpenConn[pServer->type]) {
    pConn->chandle = (*taosOpenConn[pServer->type])(pServer->shandle, pConn, pConn->peerIpstr, pConn->peerPort);
    if (pConn->chandle) {
      tTrace("%s cid:%d sid:%d id:%s, nw connection is set up, ip:%s:%hu localPort:%d pConn:%p", pServer->label,
             pConn->chann, pConn->sid, pInit->meterId, pConn->peerIpstr, pConn->peerPort, pConn->localPort, pConn);
    } else {
      tError("%s cid:%d sid:%d id:%s, failed to set up nw connection to ip:%s:%hu", pServer->label, pConn->chann,
             pConn->sid, pInit->meterId, pConn->peerIpstr, pConn->peerPort);
      *code = TSDB_CODE_NETWORK_UNAVAIL;
      taosCloseRpcConn(pConn);
      pConn = NULL;
    }
  }

  return pConn;
}

void taosCloseRpc(void *param) {
  STaosRpc *pServer = (STaosRpc *)param;

  (*taosCleanUpConn[pServer->type])(pServer->shandle);

  for (int cid = 0; cid < pServer->numOfChanns; ++cid) taosCloseRpcChann(pServer, cid);

  tfree(pServer->channList);
  tfree(pServer);
}

int taosSetSecurityInfo(int chann, int sid, char *id, int spi, int encrypt, char *secret, char *ckey) {
  /*
    SRpcConn *pConn;

    pConn = connList[chann*tsSessionsPerChann + sid];

    if ( pConn == NULL ) {
      pConn = (SRpcConn *)sizeof(SRpcConn);

      if ( pConn == NULL ) {
        tError("failed to allocate memory for taosConn");
        return -1;
      }

      memset(pConn, 0, sizeof(SRpcConn));
      pConn->chann = chann;
      pConn->sid = sid;
    }

    pConn->spi = spi;
    pConn->encrypt = encrypt;
    memcpy(pConn->secret, pConn->secret, TSDB_KEY_LEN);
    memcpy(pConn->cipheringKey, ckey, TSDB_KEY_LEN);
    memcpy(pConn->meterId, id, TSDB_METER_ID_LEN);
  */
  return -1;
}

int taosSendDataToPeer(SRpcConn *pConn, char *data, int dataLen) {
  int          writtenLen = 0;
  STaosRpc *   pServer = pConn->pServer;
  STaosHeader *pHeader = (STaosHeader *)data;

  if (pConn->signature != pConn || pServer == NULL) return -1;

  if (pHeader->msgType & 1) {
    if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace("%s cid:%d sid:%d id:%s, %s is sent to %s:%hu, len:%d source:0x%08x dest:0x%08x tranId:%d pConn:%p",
             pServer->label, pConn->chann, pConn->sid, pConn->meterId, taosMsg[pHeader->msgType], pConn->peerIpstr,
             pConn->peerPort, dataLen, pHeader->sourceId, pHeader->destId, pHeader->tranId, pConn);
  } else {
    if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16))
      tTrace(
          "%s cid:%d sid:%d id:%s, %s is sent to %s:%hu, code:%u len:%d source:0x%08x dest:0x%08x tranId:%d pConn:%p",
          pServer->label, pConn->chann, pConn->sid, pConn->meterId, taosMsg[pHeader->msgType], pConn->peerIpstr,
          pConn->peerPort, (uint8_t)pHeader->content[0], dataLen, pHeader->sourceId, pHeader->destId, pHeader->tranId,
          pConn);
  }

  writtenLen = (*taosSendData[pServer->type])(pConn->peerIp, pConn->peerPort, (char *)pHeader, dataLen, pConn->chandle);

  if (writtenLen != dataLen)
    tError("%s cid:%d sid:%d id:%s, dataLen:%d writtenLen:%d, not good, reason:%s", pServer->label, pConn->chann,
           pConn->sid, pConn->meterId, dataLen, writtenLen, strerror(errno));
  // assert ( writtenLen == dataLen );
  tDump(data, dataLen);

  return 0;
}

void taosProcessResponse(SRpcConn *pConn) {
  STaosHeader *pHeader;
  char *       msg = NULL;
  int          msgLen = 0;

  if (pConn == NULL) return;
  STaosRpc *pServer = pConn->pServer;
  if (pConn->signature != pConn || pServer == NULL) return;
  SRpcChann *pChann = pServer->channList + pConn->chann;

  pthread_mutex_lock(&pChann->mutex);

  pConn->outType = 0;
  pConn->rspReceived = 0;
  if (pServer->noFree == 0) tfree(pConn->pMsgNode);
  pConn->pMsgNode = NULL;

  if (pConn->pHead) {
    SMsgNode *pMsgNode = pConn->pHead;
    //      assert ( pMsgNode->msgLen >= sizeof(STaosHeader) && pMsgNode->msgLen < RPC_MAX_UDP_SIZE);
    if (pMsgNode->msgLen >= sizeof(STaosHeader)) {
      pConn->pMsgNode = pMsgNode;
      pConn->pHead = pMsgNode->next;
      if (pMsgNode->ahandle) pConn->ahandle = pMsgNode->ahandle;

      pHeader = (STaosHeader *)((char *)pMsgNode + sizeof(SMsgNode));
      pConn->outType = pHeader->msgType;
      pConn->outTranId = pHeader->tranId;

      msg = (char *)pHeader;
      msgLen = pMsgNode->msgLen;

    } else {
      tError("%s cid:%d sid:%d id:%s, invalid msgLen:%d pConn:%p", pServer->label, pConn->chann, pConn->sid,
             pConn->meterId, pMsgNode->msgLen, pConn);
      pConn->pHead = NULL;
    }

    if (pConn->pHead == NULL) pConn->pTail = NULL;
  }

  if (msg) {
    taosSendDataToPeer(pConn, msg, msgLen);
    taosTmrReset(taosProcessTaosTimer, tsRpcTimer, pConn, pChann->tmrCtrl, &pConn->pTimer);
  }

  pthread_mutex_unlock(&pChann->mutex);

}

int taosProcessMsgHeader(STaosHeader *pHeader, SRpcConn **ppConn, STaosRpc *pServer, int dataLen, uint32_t ip,
                         uint16_t port, void *chandle) {
  int        chann, sid, code = 0;
  SRpcConn * pConn = NULL;
  SRpcChann *pChann;
  int        msgLen;
  char       hashstr[40] = {0};
  // int        reSend = 0;

  *ppConn = NULL;
  uint32_t destId = htonl(pHeader->destId);
  chann = destId >> pServer->bits;
  sid = destId & pServer->mask;

  if (pHeader->msgType >= TSDB_MSG_TYPE_MAX || pHeader->msgType <= 0) {
    tTrace("%s cid:%d sid:%d, invalid message type:%d", pServer->label, chann, sid, pHeader->msgType);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  msgLen = (int32_t)htonl((uint32_t)pHeader->msgLen);
  if (dataLen != msgLen) {
    tTrace("%s cid:%d sid:%d, %s has invalid length, dataLen:%d, msgLen:%d", pServer->label, chann, sid,
           taosMsg[pHeader->msgType], dataLen, msgLen);
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  if (chann < 0 || chann >= pServer->numOfChanns) {
    tTrace("%s cid:%d sid:%d, chann is out of range, max:%d, %s discarded", pServer->label, chann, sid,
           pServer->numOfChanns, taosMsg[pHeader->msgType]);
    return TSDB_CODE_INVALID_SESSION_ID;
  }

  pChann = pServer->channList + chann;
  if (pChann->sessions == 0) {
    tTrace("%s cid:%d, chann is not activated yet, %s discarded", pServer->label, chann, taosMsg[pHeader->msgType]);
    if (pServer->efp) (*(pServer->efp))(chann);
    return TSDB_CODE_NOT_ACTIVE_SESSION;
  }

  if (sid < 0 || sid >= pChann->sessions) {
    tTrace("%s cid:%d sid:%d, sid is out of range, max sid:%d, %s discarded", pServer->label, chann, sid,
           pChann->sessions, taosMsg[pHeader->msgType]);
    return TSDB_CODE_INVALID_SESSION_ID;
  }

  //  if ( pHeader->tcp ) return TSDB_CODE_ALREADY_PROCESSED;
  if (sid == 0) sprintf(hashstr, "%x:%x:%x", ip, pHeader->uid, pHeader->sourceId);

  pthread_mutex_lock(&pChann->mutex);

  code = taosGetRpcConn(chann, sid, pHeader->meterId, pServer, &pConn, pHeader->msgType & 1, hashstr);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  *ppConn = pConn;
  sid = pConn->sid;

  if (pConn->peerIp != ip) {
    pConn->peerIp = ip;
    char ipstr[20] = {0};
    tinet_ntoa(ipstr, ip);
    strcpy(pConn->peerIpstr, ipstr);
  }

  if (pHeader->uid) pConn->peerUid = pHeader->uid;

  if (port) pConn->peerPort = port;

  if (pHeader->port)  // port maybe changed by the peer
    pConn->peerPort = pHeader->port;

  if (chandle) pConn->chandle = chandle;

  if (pHeader->tcp) {
    tTrace("%s cid:%d sid:%d id:%s, content will be transfered via TCP pConn:%p", pServer->label, chann, sid,
           pConn->meterId, pConn);
    if (pConn->outType) taosTmrReset(taosProcessTaosTimer, tsRpcTimer, pConn, pChann->tmrCtrl, &pConn->pTimer);
    code = TSDB_CODE_ALREADY_PROCESSED;
    goto _exit;
  }

  if (pConn->spi != 0) {
    if (pHeader->spi == pConn->spi) {
      // authentication
      STaosDigest *pDigest = (STaosDigest *)((char *)pHeader + dataLen - sizeof(STaosDigest));

      int32_t delta, authTime,failedCount,authAllowTime;
      delta = (int32_t)htonl(pDigest->timeStamp);
      delta -= (int32_t)taosGetTimestampSec();
      authTime = (int32_t)taosGetTimestampSec();

      if (abs(delta) > 900) {
        tWarn("%s cid:%d sid:%d id:%s, time diff:%d is too big, msg discarded pConn:%p, timestamp:%d", pServer->label,
              chann, sid, pConn->meterId, delta, pConn, htonl(pDigest->timeStamp));
        // the requirement of goldwind, should not return error in this case
        code = TSDB_CODE_INVALID_TIME_STAMP;
        goto _exit;
      }
      
      if (pServer->ufp) {
        int ret = (*pServer->ufp)(pHeader->meterId,&failedCount,&authAllowTime,false);
        if (0 == ret) {
          if (authTime < authAllowTime) {
            char ipstr[24];
            tinet_ntoa(ipstr, ip);
            char timestr[50];
            taosTimeSecToString((time_t)authAllowTime,timestr);
            mLError("user:%s login from %s, authentication not allowed until %s", pHeader->meterId, ipstr,timestr);
            tTrace("%s cid:%d sid:%d id:%s, auth not allowed because failed authentication exceeds max limit, msg discarded pConn:%p, until %s", pServer->label, chann, sid,
               pConn->meterId, pConn, timestr);
            code = TSDB_CODE_AUTH_BANNED_PERIOD;
            goto _exit;        
          }          
        }else {
          code = ret;
          goto _exit;
        }
      }
      
      if (taosAuthenticateMsg((uint8_t *)pHeader, dataLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret) < 0) {
        char ipstr[24];
        tinet_ntoa(ipstr, ip);
        failedCount++;
        if (failedCount >= tsMaxAuthRetry) {
          authAllowTime = authTime + 600;//ban the user for 600 seconds
          failedCount = 0;
        }
        (*pServer->ufp)(pHeader->meterId,&failedCount,&authAllowTime,true);
        
        mLError("user:%s login from %s, authentication failed", pHeader->meterId, ipstr);
        tError("%s cid:%d sid:%d id:%s, authentication failed, msg discarded pConn:%p", pServer->label, chann, sid,
               pConn->meterId, pConn);
        code = TSDB_CODE_AUTH_FAILURE;
        goto _exit;
      }
    } else {
      // if it is request or response with code 0, msg shall be discarded
      if ((pHeader->msgType & 1) || (pHeader->content[0] == 0)) {
        tTrace("%s cid:%d sid:%d id:%s, auth spi not matched, msg discarded pConn:%p", pServer->label, chann, sid,
               pConn->meterId, pConn);
        code = TSDB_CODE_AUTH_FAILURE;
        goto _exit;
      }
    }
  }

  if (pHeader->msgType != TSDB_MSG_TYPE_REG && pHeader->encrypt) {
    // decrypt here
  }

  pHeader->destId = pConn->ownId;  // destId maybe 0, it shall be changed

  if (pHeader->msgType & 1) {
    if (pConn->peerId == 0) {
      pConn->peerId = pHeader->sourceId;
    } else {
      if (pConn->peerId != pHeader->sourceId) {
        tTrace("%s cid:%d sid:%d id:%s, source Id is changed, old:0x%08x new:0x%08x pConn:%p", pServer->label, chann,
               sid, pConn->meterId, pConn->peerId, pHeader->sourceId, pConn);
        code = TSDB_CODE_INVALID_VALUE;
        goto _exit;
      }
    }

    if (pConn->inTranId == pHeader->tranId) {
      if (pConn->inType == pHeader->msgType) {
        tTrace("%s cid:%d sid:%d id:%s, %s is retransmitted, pConn:%p", pServer->label, chann, sid, pConn->meterId,
               taosMsg[pHeader->msgType], pConn);
        taosSendQuickRsp(pConn, (char)(pHeader->msgType + 1), TSDB_CODE_ACTION_IN_PROGRESS);
      } else if (pConn->inType == 0) {
        tTrace("%s cid:%d sid:%d id:%s, %s is already processed, tranId:%d pConn:%p", pServer->label, chann, sid,
               pConn->meterId, taosMsg[pHeader->msgType], pConn->inTranId, pConn);
        taosReSendRspToPeer(pConn);
      } else {
        tTrace("%s cid:%d sid:%d id:%s, mismatched message %s and tranId pConn:%p", pServer->label, chann, sid,
               pConn->meterId, taosMsg[pHeader->msgType], pConn);
      }

      // do not reply any message
      code = TSDB_CODE_ALREADY_PROCESSED;
      goto _exit;
    }

    if (pConn->inType != 0) {
      tTrace("%s cid:%d sid:%d id:%s, last session is not finished, inTranId:%d tranId:%d pConn:%p", pServer->label,
             chann, sid, pConn->meterId, pConn->inTranId, pHeader->tranId, pConn);
      code = TSDB_CODE_LAST_SESSION_NOT_FINISHED;
      goto _exit;
    }

    pConn->inTranId = pHeader->tranId;
    pConn->inType = pHeader->msgType;

    if (sid == 0)  // send a response first
      taosSendQuickRsp(pConn, (char)(pConn->inType + 1), TSDB_CODE_ACTION_IN_PROGRESS);

  } else {
    // response from taos
    pConn->peerId = pHeader->sourceId;

    if (pConn->outType == 0) {
      code = TSDB_CODE_UNEXPECTED_RESPONSE;
      goto _exit;
    }

    if (pHeader->tranId != pConn->outTranId) {
      code = TSDB_CODE_INVALID_TRAN_ID;
      goto _exit;
    }

    if (pHeader->msgType != pConn->outType + 1) {
      code = TSDB_CODE_INVALID_RESPONSE_TYPE;
      goto _exit;
    }

    if (*pHeader->content == TSDB_CODE_NOT_READY) {
      code = TSDB_CODE_ALREADY_PROCESSED;
      goto _exit;
    }

    taosTmrStopA(&pConn->pTimer);
    pConn->retry = 0;

    if (*pHeader->content == TSDB_CODE_ACTION_IN_PROGRESS || pHeader->tcp) {
      if (pConn->tretry <= tsRpcMaxRetry) {
        tTrace("%s cid:%d sid:%d id:%s, peer is still processing the transaction, pConn:%p", pServer->label, chann, sid,
               pHeader->meterId, pConn);
        pConn->tretry++;
        taosTmrReset(taosProcessTaosTimer, tsRpcProgressTime, pConn, pChann->tmrCtrl, &pConn->pTimer);
        code = TSDB_CODE_ALREADY_PROCESSED;
        goto _exit;
      } else {
        // peer still in processing, give up
        *pHeader->content = TSDB_CODE_TOO_SLOW;
      }
    }

    pConn->tretry = 0;
    if (pConn->rspReceived) {
      code = TSDB_CODE_UNEXPECTED_RESPONSE;
      goto _exit;
    } else {
      pConn->rspReceived = 1;
    }
  }

_exit:
  pthread_mutex_unlock(&pChann->mutex);

  // if (reSend) taosReSendRspToPeer(pConn);

  return code;
}

int taosBuildErrorMsgToPeer(char *pMsg, int code, char *pReply) {
  STaosHeader *pRecvHeader, *pReplyHeader;
  char *       pContent;
  uint32_t     timeStamp;
  int          msgLen;

  pRecvHeader = (STaosHeader *)pMsg;
  pReplyHeader = (STaosHeader *)pReply;

  pReplyHeader->version = pRecvHeader->version;
  pReplyHeader->msgType = (char)(pRecvHeader->msgType + 1);
  pReplyHeader->tcp = 0;
  pReplyHeader->spi = 0;
  pReplyHeader->encrypt = 0;
  pReplyHeader->tranId = pRecvHeader->tranId;
  pReplyHeader->sourceId = 0;
  pReplyHeader->destId = pRecvHeader->sourceId;
  memcpy(pReplyHeader->meterId, pRecvHeader->meterId, tListLen(pReplyHeader->meterId));

  pContent = (char *)pReplyHeader->content;
  *pContent = (char)code;
  pContent++;

  if (code == TSDB_CODE_INVALID_TIME_STAMP) {
    // include a time stamp if client's time is not synchronized well
    timeStamp = taosGetTimestampSec();
    memcpy(pContent, &timeStamp, sizeof(timeStamp));
    pContent += sizeof(timeStamp);
  }

  msgLen = (int)(pContent - pReply);
  pReplyHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);

  return msgLen;
}

void taosReportDisconnection(SRpcChann *pChann, SRpcConn *pConn)
{
    SSchedMsg schedMsg;
    schedMsg.fp = taosProcessSchedMsg;
    schedMsg.msg = NULL;
    schedMsg.ahandle = pConn->ahandle;
    schedMsg.thandle = pConn;
    taosScheduleTask(pChann->qhandle, &schedMsg);
}

void taosProcessIdleTimer(void *param, void *tmrId) {
  SRpcConn *pConn = (SRpcConn *)param;
  if (pConn->signature != param) {
    tError("idle timer pConn Signature:0x%x, pConn:0x%x not matched", pConn->signature, param);
    return;
  }

  STaosRpc * pServer = pConn->pServer;
  SRpcChann *pChann = pServer->channList + pConn->chann;
  if (pConn->pIdleTimer != tmrId) {
    tTrace("%s cid:%d sid:%d id:%s, idle timer:%p already processed pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, tmrId, pConn);
    return;
  }

  int reportDisc = 0;

  pthread_mutex_lock(&pChann->mutex);

  tTrace("%s cid:%d sid:%d id:%s, close the connection since no activity pConn:%p", pServer->label, pConn->chann,
         pConn->sid, pConn->meterId, pConn);
  if (pConn->rspReceived == 0) {
    pConn->rspReceived = 1;
    reportDisc = 1;    
  }

  pthread_mutex_unlock(&pChann->mutex);

  if (reportDisc) taosReportDisconnection(pChann, pConn);
}

void *taosProcessDataFromPeer(char *data, int dataLen, uint32_t ip, uint16_t port, void *shandle, void *thandle,
                              void *chandle) {
  STaosHeader *pHeader;
  uint8_t      code;
  SRpcConn *   pConn = (SRpcConn *)thandle;
  STaosRpc *   pServer = (STaosRpc *)shandle;
  int          msgLen;
  char         pReply[128];
  SSchedMsg    schedMsg;
  int          chann, sid;
  SRpcChann *  pChann = NULL;

  tDump(data, dataLen);

  if (ip == 0 && taosCloseConn[pServer->type]) {
    // it means the connection is broken
    if (pConn) {
      pChann = pServer->channList + pConn->chann;
      tTrace("%s cid:%d sid:%d id:%s, underlying link is gone pConn:%p", pServer->label, pConn->chann, pConn->sid,
             pConn->meterId, pConn);
      pConn->rspReceived = 1;
      pConn->chandle = NULL;
      taosReportDisconnection(pChann, pConn);
    }
    tfree(data);
    return NULL;
  }

  pHeader = (STaosHeader *)data;
  msgLen = (int32_t)htonl((uint32_t)pHeader->msgLen);

  code = (uint8_t)taosProcessMsgHeader(pHeader, &pConn, pServer, dataLen, ip, port, chandle);

  pHeader->destId = htonl(pHeader->destId);
  chann = pHeader->destId >> pServer->bits;
  sid = pHeader->destId & pServer->mask;

  if (pConn && pServer->idleTime) {
    SRpcChann *pChann = pServer->channList + pConn->chann;
    taosTmrReset(taosProcessIdleTimer, pServer->idleTime, pConn, pChann->tmrCtrl, &pConn->pIdleTimer);
  }

  if (code == TSDB_CODE_ALREADY_PROCESSED || code == TSDB_CODE_LAST_SESSION_NOT_FINISHED) {
    tTrace("%s code:%d, cid:%d sid:%d id:%s, %s wont be processed, source:0x%08x dest:0x%08x tranId:%d pConn:%p", pServer->label,
           code, chann, sid, pHeader->meterId, taosMsg[pHeader->msgType], pHeader->sourceId, htonl(pHeader->destId),
           pHeader->tranId, pConn);
    free(data);
    return pConn;
  }

  if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16)) {
    tTrace(
        "%s cid:%d sid:%d id:%s, %s received from 0x%x:%hu, parse code:%u, first:%u len:%d source:0x%08x dest:0x%08x "
        "tranId:%d pConn:%p",
        pServer->label, chann, sid, pHeader->meterId, taosMsg[pHeader->msgType], ip, port, code, pHeader->content[0],
        dataLen, pHeader->sourceId, htonl(pHeader->destId), pHeader->tranId, pConn);
  }

  if (code != 0) {
    // parsing error

    if (pHeader->msgType & 1U) {
      memset(pReply, 0, sizeof(pReply));
      
      msgLen = taosBuildErrorMsgToPeer(data, code, pReply);
      (*taosSendData[pServer->type])(ip, port, pReply, msgLen, chandle);
      tTrace("%s cid:%d sid:%d id:%s, %s is sent with error code:%u pConn:%p", pServer->label, chann, sid,
             pHeader->meterId, taosMsg[pHeader->msgType + 1], code, pConn);
    } else {
      tTrace("%s cid:%d sid:%d id:%s, %s is received, parsing error:%u pConn:%p", pServer->label, chann, sid,
             pHeader->meterId, taosMsg[pHeader->msgType], code, pConn);
    }

    free(data);
  } else {
    // parsing OK

    // internal communication is based on TAOS protocol, a trick here to make it efficient
    if (pHeader->spi) msgLen -= sizeof(STaosDigest);
    msgLen -= (int)sizeof(STaosHeader);
    pHeader->msgLen = msgLen + (int)sizeof(SIntMsg);

    if ((pHeader->msgType & 1U) == 0 && (pHeader->content[0] == TSDB_CODE_INVALID_VALUE)) {
      schedMsg.msg = NULL;  // connection shall be closed
    } else {
      pHeader = taosDecompressRpcMsg(pHeader, &schedMsg, msgLen);
    }

    if (pHeader->msgType < TSDB_MSG_TYPE_HEARTBEAT || (rpcDebugFlag & 16U)) {
      tTrace("%s cid:%d sid:%d id:%s, %s is put into queue, msgLen:%d pConn:%p pTimer:%p", pServer->label, chann, sid,
             pHeader->meterId, taosMsg[pHeader->msgType], pHeader->msgLen, pConn, pConn->pTimer);
    }

    pChann = pServer->channList + pConn->chann;
    schedMsg.fp = taosProcessSchedMsg;
    schedMsg.ahandle = pConn->ahandle;
    schedMsg.thandle = pConn;
    taosScheduleTask(pChann->qhandle, &schedMsg);
  }

  return pConn;
}

int taosSendMsgToPeerH(void *thandle, char *pCont, int contLen, void *ahandle) {
  STaosHeader *pHeader;
  SMsgNode *   pMsgNode;
  char *       msg;
  int          msgLen = 0;
  SRpcConn *   pConn = (SRpcConn *)thandle;
  STaosRpc *   pServer;
  SRpcChann *  pChann;
  uint8_t      msgType;

  if (pConn == NULL) return -1;
  if (pConn->signature != pConn) return -1;

  pServer = pConn->pServer;
  pChann = pServer->channList + pConn->chann;
  pHeader = (STaosHeader *)(pCont - sizeof(STaosHeader));
  pHeader->destIp = pConn->peerIp;
  msg = (char *)pHeader;

  if ((pHeader->msgType & 1U) == 0 && pConn->localPort) pHeader->port = pConn->localPort;
  
  contLen = taosCompressRpcMsg(pCont, contLen);

  msgLen = contLen + (int32_t)sizeof(STaosHeader);

  if (pConn->spi) {
    // add auth part
    pHeader->spi = pConn->spi;
    STaosDigest *pDigest = (STaosDigest *)(pCont + contLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(STaosDigest);
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
    taosBuildAuthHeader((uint8_t *)pHeader, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHeader->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  pthread_mutex_lock(&pChann->mutex);
  msgType = pHeader->msgType;

  if ((msgType & 1U) == 0) {
    // response
    pConn->inType = 0;
    tfree(pConn->pRspMsg);
    pConn->pRspMsg = msg;
    pConn->rspMsgLen = msgLen;

    if (pHeader->content[0] == TSDB_CODE_ACTION_IN_PROGRESS) pConn->inTranId--;

  } else {
    // request
    pMsgNode = (SMsgNode *)(pCont - sizeof(STaosHeader) - sizeof(SMsgNode));
    pMsgNode->msgLen = msgLen;
    pMsgNode->next = NULL;
    pMsgNode->ahandle = ahandle;

    if (pConn->outType) {
      if (pConn->pTail) {
        pConn->pTail->next = pMsgNode;
        pConn->pTail = pMsgNode;
      } else {
        pConn->pTail = pMsgNode;
        pConn->pHead = pMsgNode;
      }

      tTrace("%s cid:%d sid:%d id:%s, msg:%s is put into queue pConn:%p", pServer->label, pConn->chann, pConn->sid,
             pConn->meterId, taosMsg[msgType], pConn);
      msgLen = 0;

    } else {
      assert(pConn->pMsgNode == NULL);
      if (pConn->pMsgNode) {
        tError("%s cid:%d sid:%d id:%s, bug, there shall be no pengding req pConn:%p", pServer->label, pConn->chann,
               pConn->sid, pConn->meterId, pConn);
      }

      pConn->outType = msgType;
      pConn->outTranId = pHeader->tranId;
      pConn->pMsgNode = pMsgNode;
      pConn->rspReceived = 0;
      if (pMsgNode->ahandle) pConn->ahandle = pMsgNode->ahandle;
    }
  }

  if (msgLen) {
    taosSendDataToPeer(pConn, (char *)pHeader, msgLen);
    if (msgType & 1U) {
      taosTmrReset(taosProcessTaosTimer, tsRpcTimer, pConn, pChann->tmrCtrl, &pConn->pTimer);
    }
  }

  pthread_mutex_unlock(&pChann->mutex);

  return contLen;
}

int taosReSendRspToPeer(SRpcConn *pConn) {
  STaosHeader *pHeader;
  int          writtenLen;
  STaosRpc *   pServer = pConn->pServer;

  if (pConn->pRspMsg == NULL || pConn->rspMsgLen <= 0) {
    tError("%s cid:%d sid:%d id:%s, rsp is null", pServer->label, pConn->chann, pConn->sid, pConn->meterId);
    return -1;
  }

  pHeader = (STaosHeader *)pConn->pRspMsg;
  if (pHeader->msgLen <= sizeof(SIntMsg) + 1 || pHeader->msgType <= 0) {
    tError("%s cid:%d sid:%d id:%s, rsp is null, rspLen:%d, msgType:%d", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, pHeader->msgLen, pHeader->msgType);
    return -1;
  }

  writtenLen =
      (*taosSendData[pServer->type])(pConn->peerIp, pConn->peerPort, pConn->pRspMsg, pConn->rspMsgLen, pConn->chandle);

  if (writtenLen != pConn->rspMsgLen) {
    tError("%s cid:%d sid:%d id:%s, failed to re-send %s, reason:%s pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, taosMsg[(int)pHeader->msgType], strerror(errno), pConn);
  } else {
    tTrace("%s cid:%d sid:%d id:%s, msg:%s is re-sent to %s:%hu, len:%d pConn:%p", pServer->label, pConn->chann,
           pConn->sid, pConn->meterId, taosMsg[(int)pHeader->msgType], pConn->peerIpstr, pConn->peerPort,
           pConn->rspMsgLen, pConn);
  }

  return 0;
}

void taosProcessTaosTimer(void *param, void *tmrId) {
  STaosHeader *pHeader = NULL;
  SRpcConn *   pConn = (SRpcConn *)param;
  int          msgLen;
  int          reportDisc = 0;

  if (pConn->signature != param) {
    tError("pConn Signature:0x%x, pConn:0x%x not matched", pConn->signature, param);
    return;
  }

  STaosRpc * pServer = pConn->pServer;
  SRpcChann *pChann = pServer->channList + pConn->chann;

  if (pConn->pTimer != tmrId) {
    tTrace("%s cid:%d sid:%d id:%s, timer:%p already processed pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, tmrId, pConn);
    return;
  }

  pthread_mutex_lock(&pChann->mutex);

  if (pConn->rspReceived) {
    tTrace("%s cid:%d sid:%d id:%s, rsp just received, pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, pConn);
  } else if (pConn->outType == 0) {
    tTrace("%s cid:%d sid:%d id:%s, outtype is zero, pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, pConn);
  } else {
    tTrace("%s cid:%d sid:%d id:%s, expected %s is not received, pConn:%p", pServer->label, pConn->chann, pConn->sid,
           pConn->meterId, taosMsg[(int)pConn->outType + 1], pConn);
    pConn->pTimer = NULL;
    pConn->retry++;

    if (pConn->retry < 4) {
      tTrace("%s cid:%d sid:%d id:%s, re-send msg:%s to %s:%hu pConn:%p", pServer->label, pConn->chann, pConn->sid,
             pConn->meterId, taosMsg[pConn->outType], pConn->peerIpstr, pConn->peerPort, pConn);
      if (pConn->pMsgNode && pConn->pMsgNode->msgLen > 0) {
        pHeader = (STaosHeader *)((char *)pConn->pMsgNode + sizeof(SMsgNode));
        pHeader->destId = pConn->peerId;
        msgLen = pConn->pMsgNode->msgLen;
        if (pConn->spi) {
          STaosDigest *pDigest = (STaosDigest *)(((char *)pHeader) + pConn->pMsgNode->msgLen - sizeof(STaosDigest));
          pDigest->timeStamp = htonl(taosGetTimestampSec());
          taosBuildAuthHeader((uint8_t *)pHeader, pConn->pMsgNode->msgLen - TSDB_AUTH_LEN, pDigest->auth,
                              pConn->secret);
        }
      }
    } else {
      // close the connection
      tTrace("%s cid:%d sid:%d id:%s, failed to send msg:%s to %s:%hu pConn:%p", pServer->label, pConn->chann,
             pConn->sid, pConn->meterId, taosMsg[pConn->outType], pConn->peerIpstr, pConn->peerPort, pConn);
      if (pConn->rspReceived == 0) {
        pConn->rspReceived = 1;
        reportDisc = 1;
      }
    }
  }

  if (pHeader) {
    (*taosSendData[pServer->type])(pConn->peerIp, pConn->peerPort, (char *)pHeader, msgLen, pConn->chandle);
    taosTmrReset(taosProcessTaosTimer, tsRpcTimer<<pConn->retry, pConn, pChann->tmrCtrl, &pConn->pTimer);
  }

  pthread_mutex_unlock(&pChann->mutex);

  if (reportDisc) taosReportDisconnection(pChann, pConn);
}

void taosGetRpcConnInfo(void *thandle, uint32_t *peerId, uint32_t *peerIp, uint16_t *peerPort, int *cid, int *sid) {
  SRpcConn *pConn = (SRpcConn *)thandle;

  *peerId = pConn->peerId;
  *peerIp = pConn->peerIp;
  *peerPort = pConn->peerPort;

  *cid = pConn->chann;
  *sid = pConn->sid;
}

int taosGetOutType(void *thandle) {
  SRpcConn *pConn = (SRpcConn *)thandle;
  if (pConn == NULL) return -1;

  return pConn->outType;
}

void taosProcessSchedMsg(SSchedMsg *pMsg) {
  SIntMsg * pHeader = (SIntMsg *)pMsg->msg;
  SRpcConn *pConn = (SRpcConn *)pMsg->thandle;
  if (pConn == NULL || pConn->signature != pMsg->thandle || pConn->pServer == NULL) return;
  STaosRpc *pRpc = pConn->pServer;

  void *ahandle = (*(pRpc->fp))(pMsg->msg, pMsg->ahandle, pMsg->thandle);

  if (ahandle == NULL || pMsg->msg == NULL) {
    taosCloseRpcConn(pConn);
  } else {
    pConn->ahandle = ahandle;
    if (pHeader && ((pHeader->msgType & 1) == 0)) taosProcessResponse(pConn);
  }

  if (pMsg->msg) free(pMsg->msg - sizeof(STaosHeader) + sizeof(SIntMsg));
}

void taosStopRpcConn(void *thandle) {
  SRpcConn * pConn = (SRpcConn *)thandle;
  STaosRpc * pServer = pConn->pServer;
  SRpcChann *pChann = pServer->channList + pConn->chann;

  tTrace("%s cid:%d sid:%d id:%s, stop the connection pConn:%p", pServer->label, pConn->chann, pConn->sid,
         pConn->meterId, pConn);

  int reportDisc = 0;
  pthread_mutex_lock(&pChann->mutex);

  if (pConn->outType) {
    pConn->rspReceived = 1;
    reportDisc = 1;
    pthread_mutex_unlock(&pChann->mutex);
  } else {
    pthread_mutex_unlock(&pChann->mutex);
    taosCloseRpcConn(pConn);
  }

  if (reportDisc) taosReportDisconnection(pChann, pConn);
}

int taosAuthenticateMsg(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey) {
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

int taosBuildAuthHeader(uint8_t *pMsg, int msgLen, uint8_t *pAuth, uint8_t *pKey) {
  MD5_CTX context;

  MD5Init(&context);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));

  return 0;
}
