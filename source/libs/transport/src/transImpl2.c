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
#include "tversion.h"

#ifdef TD_ACORE
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#endif

#define DIV_ROUNDUP(x, y) (((x) + ((y)-1)) / (y))

#define EVT_TIMEOUT 0x01
#define EVT_READ    0x02
#define EVT_WRITE   0x04
#define EVT_SIGNAL  0x08
#define EVT_ASYNC   0x10

/* How many bytes to allocate for N fds? */
#define SELECT_ALLOC_SIZE(n) (DIV_ROUNDUP(n, NFDBITS) * sizeof(fd_mask))

typedef void (*AsyncCb)(void *async, int32_t status);

typedef struct {
  void         *data;
  int32_t       sendFd;
  AsyncCb       cb;
  queue         q;
  TdThreadMutex mutex;
  void         *hostThrd;
} SAsyncHandle;
typedef struct {
  queue   q;
  int32_t acceptFd;
} SFdArg;
typedef struct {
  queue q;
} SFdQueue;

typedef struct SSvrConn {
  int32_t ref;
  // uv_tcp_t*  pTcp;
  // uv_timer_t pTimer;

  queue       queue;
  SConnBuffer readBuf;  // read buf,
  int         inType;
  void       *pInst;    // rpc init
  void       *ahandle;  //
  void       *hostThrd;
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
  SHashObj *pQTable;
  // uv_buf_t *buf;
  int32_t bufSize;
  queue   wq;  // uv_write_t queue
  int32_t fd;

} SSvrConn;
typedef struct SSvrRespMsg {
  SSvrConn     *pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
  int64_t       seqNum;
  void         *arg;
  FilteFunc     func;
  int8_t        sent;

} SSvrRespMsg;

#define ASYNC_ERR_JRET(thrd)                            \
  do {                                                  \
    if (thrd->quit) {                                   \
      tTrace("worker thread already quit, ignore msg"); \
      goto _return1;                                    \
    }                                                   \
  } while (0)

static FORCE_INLINE void destroySmsg(SSvrRespMsg *smsg) {
  if (smsg == NULL) {
    return;
  }
  transFreeMsg(smsg->msg.pCont);
  taosMemoryFree(smsg);
}
typedef struct {
  char   *buf;
  int32_t len;
  int8_t  inited;
  void   *data;
} SEvtBuf;
typedef struct SWorkThrd {
  TdThread thread;
  // //uv_connect_t connect_req;
  // uv_pipe_t*   pipe;
  // uv_os_fd_t   fd;
  // uv_loop_t*   loop;
  // SAsyncPool*  asyncPool;
  queue msg;

  queue conn;
  void *pInst;
  bool  quit;

  // SIpWhiteListTab* pWhiteList;
  int64_t whiteListVer;
  int8_t  enableIpWhiteList;

  int32_t connRefMgt;

  int32_t       client_count;
  int8_t        inited;
  TdThreadMutex mutex;
  SFdQueue      fdQueue;

  int32_t       pipe_fd[2];  //
  SAsyncHandle *notifyNewConnHandle;

  int32_t       pipe_queue_fd[2];
  SAsyncHandle *asyncHandle;
} SWorkThrd2;
typedef struct SServerObj {
  TdThread     thread;
  int          workerIdx;
  int          numOfThreads;
  int          numOfWorkerReady;
  SWorkThrd2 **pThreadObj;

  // uv_pipe_t   pipeListen;
  // uv_pipe_t** pipe;
  uint32_t ip;
  uint32_t port;
  // uv_async_t* pAcceptAsync;  // just to quit from from accept thread
  int32_t serverFd;
  bool    inited;
} SServerObj2;

typedef int32_t (*__sendCb)(void *arg, SEvtBuf *buf, int32_t status);
typedef int32_t (*__readCb)(void *arg, SEvtBuf *buf, int32_t status);
typedef int32_t (*__sendFinishCb)(void *arg, int32_t status);
typedef void (*__asyncCb)(void *arg, int32_t status);

enum EVT_TYPE { EVT_ASYNC_T = 0, EVT_CONN_T = 1, EVT_SIGANL_T, EVT_NEW_CONN_T };

typedef struct {
  int32_t fd;
  void   *data;
  int32_t event;

  __sendCb       sendCb;
  __sendFinishCb sendFinishCb;
  __readCb       readCb;
  __asyncCb      asyncCb;

  int8_t  evtType;
  void   *arg;
  SEvtBuf buf;
  SEvtBuf sendBuf;
  int8_t  rwRef;
} SFdCbArg;

typedef struct {
  int32_t   evtFds;  // highest fd in the set
  int32_t   evtFdsSize;
  int32_t   resizeOutSets;
  fd_set   *evtReadSetIn;
  fd_set   *evtWriteSetIn;
  fd_set   *evtReadSetOut;
  fd_set   *evtWriteSetOut;
  SHashObj *pFdTable;
  void     *hostThrd;
  int32_t   fd[2048];
  int32_t   fdIdx;

} SEvtMgt;

static int32_t evtMgtResize(SEvtMgt *pOpt, int32_t cap);
static int32_t evtMgtCreate(SEvtMgt **pOpt);
static int32_t evtMgtDispath(SEvtMgt *pOpt, struct timeval *tv);
static int32_t evtMgtAdd(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg);
static int32_t evtMgtRemove(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg);
static void    evtMgtDestroy(SEvtMgt *pOpt);

int32_t evtMgtHandle(SEvtMgt *pOpt, int32_t res, int32_t fd);

static int32_t evtMgtCreate(SEvtMgt **pOpt) {
  int32_t  code = 0;
  SEvtMgt *pRes = taosMemoryCalloc(1, sizeof(SEvtMgt));
  if (pRes == NULL) {
    *pOpt = NULL;
    return terrno;
  }
  pRes->evtFds = 0;
  pRes->evtFdsSize = 0;
  pRes->resizeOutSets = 0;
  pRes->evtReadSetIn = NULL;
  pRes->evtWriteSetIn = NULL;
  pRes->evtReadSetOut = NULL;
  pRes->evtWriteSetOut = NULL;
  pRes->pFdTable = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  pRes->fdIdx = 0;

  code = evtMgtResize(pRes, (32 + 1));
  if (code != 0) {
    evtMgtDestroy(pRes);
  } else {
    *pOpt = pRes;
  }

  return code;
}

// int32_t selectUtilRange()

static int32_t evtMayShoudInitBuf(SEvtBuf *evtBuf) {
  int32_t code = 0;
  if (evtBuf->inited == 0) {
    evtBuf->buf = taosMemoryCalloc(1, 4096);
    if (evtBuf->buf == NULL) {
      code = terrno;
    } else {
      evtBuf->len = 4096;
      evtBuf->inited = 1;
    }
  }
  return code;
}
int32_t evtMgtHandleImpl(SEvtMgt *pOpt, SFdCbArg *pArg, int res) {
  int32_t nBytes = 0;
  char    buf[2] = {0};
  int32_t code = 0;
  if (pArg->evtType == EVT_NEW_CONN_T || pArg->evtType == EVT_ASYNC_T) {
    // handle new coming conn;
    if (res & EVT_READ) {
      SAsyncHandle *handle = pArg->arg;
      nBytes = read(pArg->fd, buf, 2);
      if (nBytes >= 1) {
        handle->cb(handle, 0);
      }
    }
    if (res & EVT_WRITE) {
      // handle err
    }
  } else if (pArg->event == EVT_CONN_T) {
    if (res & EVT_READ) {
      SEvtBuf *pBuf = &pArg->buf;
      code = evtMayShoudInitBuf(pBuf);
      if (code != 0) {
        tError("failed to init buf since %s", tstrerror(code));
        return code;
      }

      nBytes = read(pArg->fd, pBuf->buf, pBuf->len);
      if (nBytes > 0) {
        code = pArg->readCb(pArg, pBuf, nBytes);
      } else {
        code = pArg->readCb(pArg, pBuf, nBytes);
      }
      // handle read
    }

    if (res & EVT_WRITE) {
      SEvtBuf *pBuf = &pArg->sendBuf;

      code = evtMayShoudInitBuf(pBuf);
      if (code != 0) {
        tError("failed to init wbuf since %s", tstrerror(code));
        return code;
      }

      code = pArg->sendCb(pArg->arg, pBuf, 0);
      if (code != 0) {
        tError("failed to build send buf since %s", tstrerror(code));
      }
      int32_t total = pBuf->len;
      int32_t offset = 0;
      do {
        int32_t n = send(pArg->fd, pBuf->buf + offset, total, 0);
        if (n < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            code = TAOS_SYSTEM_ERROR(errno);
            break;
          }
          code = TAOS_SYSTEM_ERROR(errno);
          break;
        }
        offset += n;
        total -= n;
      } while (total > 0);

      if (code != 0) {
        tError("failed to send buf since %s", tstrerror(code));
        pArg->sendFinishCb(pArg->arg, code);
        return code;
      }
      code = evtMgtRemove(pOpt, pArg->fd, EVT_WRITE, pArg);
      return code;
    }
  }

  return code;
}
int32_t evtMgtHandle(SEvtMgt *pOpt, int32_t res, int32_t fd) {
  int32_t   code = 0;
  SFdCbArg *pArg = taosHashGet(pOpt->pFdTable, &fd, sizeof(fd));
  if (pArg == NULL) {
    return TAOS_SYSTEM_ERROR(EBADF);
  }
  return evtMgtHandleImpl(pOpt, pArg, res);
}
static int32_t evtMgtDispath(SEvtMgt *pOpt, struct timeval *tv) {
  int32_t code = 0, res = 0, j, nfds = 0, active_Fds = 0;

  if (pOpt->resizeOutSets) {
    fd_set *readSetOut = NULL, *writeSetOut = NULL;
    int32_t sz = pOpt->evtFdsSize;

    readSetOut = taosMemoryRealloc(pOpt->evtReadSetOut, sz);
    if (readSetOut == NULL) {
      return terrno;
    }
    pOpt->evtReadSetOut = readSetOut;

    writeSetOut = taosMemoryRealloc(pOpt->evtWriteSetOut, sz);
    if (writeSetOut == NULL) {
      return terrno;
    }
    pOpt->evtWriteSetOut = writeSetOut;

    pOpt->resizeOutSets = 0;
  }

  memcpy(pOpt->evtReadSetOut, pOpt->evtReadSetIn, pOpt->evtFdsSize);
  memcpy(pOpt->evtWriteSetOut, pOpt->evtWriteSetIn, pOpt->evtFdsSize);

  nfds = pOpt->evtFds + 1;
  // TODO lock or not
  active_Fds = select(nfds, pOpt->evtReadSetOut, pOpt->evtWriteSetOut, NULL, tv);
  if (active_Fds < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  } else if (active_Fds == 0) {
    tDebug("select timeout occurred");
    return code;
  }

  for (j = 0; j < nfds && active_Fds > 0; j++) {
    int32_t fd = pOpt->fd[j];
    res = 0;
    if (FD_ISSET(fd, pOpt->evtReadSetOut)) {
      res |= EVT_READ;
    }
    if (FD_ISSET(fd, pOpt->evtWriteSetOut)) {
      res |= EVT_WRITE;
    }

    if (res == 0) {
      continue;
    } else {
      active_Fds--;
      code = evtMgtHandle(pOpt, res, fd);
      if (code != 0) {
        tError("failed to handle fd %d since %s", fd, tstrerror(code));
      } else {
        tDebug("success to handle fd %d", fd);
      }
    }
  }

  return code;
}

static int32_t evtMgtResize(SEvtMgt *pOpt, int32_t cap) {
  int32_t code = 0;

  fd_set *readSetIn = NULL;
  fd_set *writeSetIn = NULL;

  readSetIn = taosMemoryRealloc(pOpt->evtReadSetIn, cap);
  if (readSetIn == NULL) {
    return terrno;
  }
  pOpt->evtReadSetIn = readSetIn;

  writeSetIn = taosMemoryRealloc(pOpt->evtWriteSetIn, cap);
  if (writeSetIn == NULL) {
    return terrno;
  }

  pOpt->evtWriteSetIn = writeSetIn;
  pOpt->resizeOutSets = 1;

  memset((char *)pOpt->evtReadSetIn + pOpt->evtFdsSize, 0, cap - pOpt->evtFdsSize);
  memset((char *)pOpt->evtWriteSetIn + pOpt->evtFdsSize, 0, cap - pOpt->evtFdsSize);

  pOpt->evtFdsSize = cap;
  return code;
}

static int32_t evtMgtAdd(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg) {
  // add new fd to the set
  int32_t code = 0;
  if (pOpt->evtFds < fd) {
    int32_t fdSize = pOpt->evtFdsSize;

    if (fdSize < (int32_t)sizeof(fd_mask)) {
      fdSize = (int32_t)sizeof(fd_mask);
    }
    while (fdSize < (int32_t)SELECT_ALLOC_SIZE(fd + 1)) {
      fdSize *= 2;
    }
    if (fdSize != pOpt->evtFdsSize) {
      if (evtMgtResize(pOpt, fdSize)) {
        return -1;
      }
    }
    pOpt->evtFds = fd;
  }
  int8_t rwRef = 0;
  if (fd != 0) {
    if (events & EVT_READ) {
      FD_SET(fd, pOpt->evtReadSetIn);
      rwRef++;
    }

    if (events & EVT_WRITE) {
      FD_SET(fd, pOpt->evtWriteSetIn);
      rwRef++;
    }
    pOpt->fd[pOpt->fdIdx++] = fd;
    code = taosHashPut(pOpt->pFdTable, &fd, sizeof(fd), arg, sizeof(*arg));
  }

  return 0;
}

static int32_t evtMgtRemove(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg) {
  int32_t line = 0;
  int32_t code = 0;
  ASSERT((events & EVT_SIGNAL) == 0);
  int8_t rwRef = 0;
  if (pOpt->evtFds < fd) {
    return TAOS_SYSTEM_ERROR(EBADF);
  }

  if (events & EVT_READ) {
    FD_CLR(fd, pOpt->evtReadSetIn);
    rwRef++;
  }

  if (events & EVT_WRITE) {
    FD_CLR(fd, pOpt->evtWriteSetIn);
    rwRef++;
  }
  SFdCbArg *pArg = taosHashGet(pOpt->pFdTable, &fd, sizeof(fd));
  if (pArg == NULL) {
    tError("%s failed to get fd %d since %s", __func__, fd, tstrerror(TAOS_SYSTEM_ERROR(EBADF)));
    return 0;
  }
  pArg->rwRef -= rwRef;
  if (pArg->rwRef == 0) {
    code = taosHashRemove(pOpt->pFdTable, &fd, sizeof(fd));
    for (int32_t i = 0; i < sizeof(pOpt->fd) / sizeof(pOpt->fd[0]); i++) {
      if (pOpt->fd[i] == fd) {
        pOpt->fd[i] = -1;
      }
    }
  }
  return code;
}
static void evtMgtDestroy(SEvtMgt *pOpt) {
  if (pOpt == NULL) return;

  if (pOpt->evtReadSetIn) {
    taosMemoryFree(pOpt->evtReadSetIn);
  }

  if (pOpt->evtWriteSetIn) {
    taosMemoryFree(pOpt->evtWriteSetIn);
  }

  if (pOpt->evtReadSetOut) {
    taosMemoryFree(pOpt->evtReadSetOut);
  }

  if (pOpt->evtWriteSetOut) {
    taosMemoryFree(pOpt->evtWriteSetOut);
  }

  taosHashCleanup(pOpt->pFdTable);
  taosMemoryFree(pOpt);
}

static int32_t evtAsyncInit(SEvtMgt *pOpt, int32_t fd[2], SAsyncHandle **async, AsyncCb cb, int8_t evtType,
                            void *pThrd) {
  int32_t       code = 0;
  SAsyncHandle *pAsync = taosMemoryCalloc(1, sizeof(SAsyncHandle));
  if (pAsync == NULL) {
    return terrno;
  }
  pAsync->hostThrd = pThrd;
  pAsync->data = pOpt;

  taosThreadMutexInit(&pAsync->mutex, NULL);
  pAsync->sendFd = fd[1];
  pAsync->cb = cb;

  SFdCbArg arg = {.evtType = evtType, .arg = pAsync, .fd = fd[0]};
  arg.arg = pAsync;

  code = evtMgtAdd(pOpt, fd[0], EVT_READ, &arg);
  if (code != 0) {
    taosMemoryFree(pAsync);
    return code;
  }
  QUEUE_INIT(&pAsync->q);
  *async = pAsync;
  return code;
}

static int32_t evtAsyncSend(SAsyncHandle *async, queue *q) {
  int32_t code = 0;
  taosThreadMutexLock(&async->mutex);
  QUEUE_PUSH(&async->q, q);
  taosThreadMutexUnlock(&async->mutex);
  int32_t nBytes = write(async->sendFd, "1", 1);
  if (nBytes != 1) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  return code;
}

void *transAcceptThread(void *arg) {
  int32_t code = 0;

  setThreadName("trans-accept-work");
  SServerObj2       *srv = arg;
  struct sockaddr_in client_addr;
  socklen_t          client_addr_len = sizeof(client_addr);
  int32_t            workerIdx = 0;

  while (1) {
    int32_t client_fd = accept(srv->serverFd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (client_fd < 0) {
      tError("failed to accept since %s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
      continue;
    }
    workerIdx = (workerIdx + 1) % srv->numOfThreads;
    SWorkThrd2 *pThrd = srv->pThreadObj[workerIdx];

    SFdArg *arg = taosMemoryCalloc(1, sizeof(SFdArg));
    if (arg == NULL) {
      tError("failed to create fd arg since %s", tstrerror(terrno));
      continue;
    }
    arg->acceptFd = client_fd;
    QUEUE_INIT(&arg->q);

    code = evtAsyncSend(pThrd->notifyNewConnHandle, &arg->q);
    if (code != 0) {
      tError("failed to send async msg since %s", tstrerror(code));
    }
  }
  return NULL;
}
int32_t uvGetConnRefOfThrd(SWorkThrd2 *thrd) { return thrd ? thrd->connRefMgt : -1; }

void uvDestroyResp(void *e) {
  SSvrRespMsg *pMsg = QUEUE_DATA(e, SSvrRespMsg, q);
  destroySmsg(pMsg);
}
static int32_t connGetSockInfo(SSvrConn *pConn) {
  int32_t code = 0;

  struct sockaddr_in addr;
  socklen_t          addr_len = sizeof(addr);
  char               ip_str[INET_ADDRSTRLEN];
  int                port;

  if (getpeername(pConn->fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }

  inet_ntop(AF_INET, &addr.sin_addr, ip_str, sizeof(ip_str));
  port = ntohs(addr.sin_port);
  snprintf(pConn->dst, sizeof(pConn->dst), "%s:%d", ip_str, port);

  if (getsockname(pConn->fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }
  inet_ntop(AF_INET, &addr.sin_addr, ip_str, sizeof(ip_str));
  port = ntohs(addr.sin_port);

  snprintf(pConn->src, sizeof(pConn->src), "%s:%d", ip_str, port);

  return code;
}
static SSvrConn *createConn(void *tThrd, int32_t fd) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SWorkThrd2 *pThrd = tThrd;
  SSvrConn   *pConn = taosMemoryCalloc(1, sizeof(SSvrConn));
  if (pConn == NULL) {
    return NULL;
  }
  pConn->fd = fd;
  QUEUE_INIT(&pConn->queue);

  code = connGetSockInfo(pConn);
  TAOS_CHECK_GOTO(code, &lino, _end);

  if ((code = transInitBuffer(&pConn->readBuf)) != 0) {
    TAOS_CHECK_GOTO(code, &lino, _end);
  }

  // if ((code = transQueueInit(&pConn->resps, uvDestroyResp)) != 0) {
  //   TAOS_CHECK_GOTO(code, &lino, _end);
  //}

  pConn->broken = false;
  pConn->status = ConnNormal;

  SExHandle *exh = taosMemoryMalloc(sizeof(SExHandle));
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  exh->handle = pConn;
  exh->pThrd = pThrd;
  exh->refId = transAddExHandle(uvGetConnRefOfThrd(pThrd), exh);
  if (exh->refId < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, &lino, _end);
  }

  SExHandle *pSelf = transAcquireExHandle(uvGetConnRefOfThrd(pThrd), exh->refId);
  if (pSelf != exh) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _end);
  }

  STrans *pInst = pThrd->pInst;
  pConn->refId = exh->refId;

  QUEUE_INIT(&exh->q);
  tTrace("%s handle %p, conn %p created, refId:%" PRId64, transLabel(pInst), exh, pConn, pConn->refId);

  pConn->pQTable = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pConn->pQTable == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }
  QUEUE_PUSH(&pThrd->conn, &pConn->queue);

  //  code = initWQ(&pConn->wq);
  // TAOS_CHECK_GOTO(code, &lino, _end);
  // wqInited = 1;

  return pConn;
_end:
  if (code != 0) {
    tError("failed to create conn since %s", tstrerror(code));
    // destroryConn(pConn);
  }
  return NULL;
}
static void destroryConn(SSvrConn *pConn) {
  if (pConn == NULL) {
    return;
  }
  // transDestroyBuffer(&pConn->readBuf);
  // transQueueCleanup(&pConn->resps);
  taosHashCleanup(pConn->pQTable);
  QUEUE_REMOVE(&pConn->queue);
  taosMemoryFree(pConn);
}

bool uvConnMayGetUserInfo(SSvrConn *pConn, STransMsgHead **ppHead, int32_t *msgLen) {
  if (pConn->userInited) {
    return false;
  }

  STrans        *pInst = pConn->pInst;
  STransMsgHead *pHead = *ppHead;
  int32_t        len = *msgLen;
  if (pHead->withUserInfo) {
    STransMsgHead *tHead = taosMemoryCalloc(1, len - sizeof(pInst->user));
    if (tHead == NULL) {
      tError("conn %p failed to get user info since %s", pConn, tstrerror(terrno));
      return false;
    }
    memcpy((char *)tHead, (char *)pHead, TRANS_MSG_OVERHEAD);
    memcpy((char *)tHead + TRANS_MSG_OVERHEAD, (char *)pHead + TRANS_MSG_OVERHEAD + sizeof(pInst->user),
           len - sizeof(STransMsgHead) - sizeof(pInst->user));
    tHead->msgLen = htonl(htonl(pHead->msgLen) - sizeof(pInst->user));

    memcpy(pConn->user, (char *)pHead + TRANS_MSG_OVERHEAD, sizeof(pConn->user));
    pConn->userInited = 1;

    taosMemoryFree(pHead);
    *ppHead = tHead;
    *msgLen = len - sizeof(pInst->user);
    return true;
  }
  return false;
}

static int32_t evtConnHandleReleaseReq(SSvrConn *pConn, STransMsgHead *phead) {
  int32_t code = 0;
  return code;
}
static int32_t evtSvrHandleRep(SSvrConn *pConn, char *req, int32_t len) {
  SWorkThrd2 *pThrd = pConn->hostThrd;
  STrans     *pInst = pThrd->pInst;

  int32_t        code = 0;
  int32_t        msgLen = 0;
  STransMsgHead *pHead = (STransMsgHead *)req;
  if (uvConnMayGetUserInfo(pConn, &pHead, &msgLen) == true) {
    tDebug("%s conn %p get user info", transLabel(pInst), pConn);
  } else {
    if (pConn->userInited == 0) {
      taosMemoryFree(pHead);
      tDebug("%s conn %p failed get user info since %s", transLabel(pInst), pConn, tstrerror(terrno));
      return TSDB_CODE_INVALID_MSG;
    }
    tDebug("%s conn %p no need get user info", transLabel(pInst), pConn);
  }
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);

  pConn->inType = pHead->msgType;

  STransMsg transMsg = {0};
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = pHead->content;
  transMsg.msgType = pHead->msgType;
  transMsg.code = pHead->code;
  if (pHead->seqNum == 0) {
    STraceId *trace = &pHead->traceId;
    tGError("%s conn %p received invalid seqNum, msgType:%s", transLabel(pInst), pConn, TMSG_INFO(pHead->msgType));
    return TSDB_CODE_INVALID_MSG;
  }

  transMsg.info.handle = (void *)transAcquireExHandle(uvGetConnRefOfThrd(pThrd), pConn->refId);
  transMsg.info.refIdMgt = pThrd->connRefMgt;

  transMsg.info.refId = pHead->noResp == 1 ? -1 : pConn->refId;
  transMsg.info.traceId = pHead->traceId;
  transMsg.info.cliVer = htonl(pHead->compatibilityVer);
  transMsg.info.forbiddenIp = 0;
  transMsg.info.noResp = pHead->noResp == 1 ? 1 : 0;
  transMsg.info.seq = taosHton64(pHead->seqNum);
  transMsg.info.qId = taosHton64(pHead->qid);
  transMsg.info.msgType = pHead->msgType;

  SRpcConnInfo *pConnInfo = &(transMsg.info.conn);
  pConnInfo->clientIp = pConn->clientIp;
  pConnInfo->clientPort = pConn->port;
  tstrncpy(pConnInfo->user, pConn->user, sizeof(pConnInfo->user));

  transReleaseExHandle(uvGetConnRefOfThrd(pThrd), pConn->refId);

  (*pInst->cfp)(pInst->parent, &transMsg, NULL);

  return code;
}
static int32_t evtSvrReadCb(void *arg, SEvtBuf *buf, int32_t bytes) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SFdCbArg *pArg = arg;
  SSvrConn *pConn = pArg->data;

  if (bytes == 0) {
    tDebug("client %s closed", pConn->src);
    return TSDB_CODE_RPC_NETWORK_ERROR;
  }
  SConnBuffer *p = &pConn->readBuf;
  if (p->cap - p->len < bytes) {
    int32_t newCap = p->cap + bytes;
    char   *newBuf = taosMemoryRealloc(p->buf, newCap);
    if (newBuf == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _end);
    }
    p->buf = newBuf;
    p->cap = newCap;
  }

  memcpy(p->buf + p->len, buf->buf, bytes);
  p->len += bytes;

  while (p->len >= sizeof(STransMsgHead)) {
    STransMsgHead head;
    memcpy(&head, p->buf, sizeof(head));
    int32_t msgLen = (int32_t)htonl(head.msgLen);
    if (p->len >= msgLen) {
      char *pMsg = taosMemoryCalloc(1, msgLen);
      if (pMsg == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _end);
      }
      memcpy(pMsg, p->buf, msgLen);
      memcpy(p->buf + msgLen, p->buf, p->len - msgLen);
      p->len -= msgLen;

      code = evtSvrHandleRep(pConn, pMsg, msgLen);
      TAOS_CHECK_GOTO(terrno, &lino, _end);
    } else {
      break;
    }
  }

  return code;
_end:
  if (code != 0) {
    tError("%s failed to handle read at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
void evtNewConnNotifyCb(void *async, int32_t status) {
  int32_t code = 0;

  SAsyncHandle *handle = async;
  SEvtMgt      *pEvtMgt = handle->data;

  queue wq;
  QUEUE_INIT(&wq);

  taosThreadMutexLock(&handle->mutex);
  QUEUE_MOVE(&handle->q, &wq);
  taosThreadMutexUnlock(&handle->mutex);

  if (QUEUE_IS_EMPTY(&wq)) {
    return;
  }
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue *el = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(el);

    SFdArg *pArg = QUEUE_DATA(el, SFdArg, q);

    SSvrConn *pConn = createConn(pEvtMgt->hostThrd, pArg->acceptFd);
    if (pConn == NULL) {
      tError("failed to create conn since %s", tstrerror(code));
      taosMemoryFree(pArg);
      continue;
    } else {
      tDebug("success to create conn %p, src:%s, dst:%s", pConn, pConn->src, pConn->dst);
    }

    SFdCbArg arg = {.evtType = EVT_CONN_T,
                    .arg = pArg,
                    .fd = pArg->acceptFd,
                    .readCb = evtSvrReadCb,
                    .sendCb = NULL,
                    .data = pConn};
    code = evtMgtAdd(pEvtMgt, pArg->acceptFd, EVT_READ, &arg);

    if (code != 0) {
      tError("failed to add fd to evt since %s", tstrerror(code));
    }
    taosMemoryFree(pArg);
  }

  return;
}

int32_t evtSvrHandleSendResp(SWorkThrd2 *pThrd, SSvrRespMsg *pResp) {
  // TODO
  int32_t code = 0;
  return code;
}
void evtHandleRespCb(void *async, int32_t status) {
  int32_t code = 0;

  SAsyncHandle *handle = async;
  // SEvtMgt      *pEvtMgt = handle->data;

  queue wq;
  QUEUE_INIT(&wq);
  taosThreadMutexLock(&handle->mutex);
  QUEUE_MOVE(&handle->q, &wq);
  taosThreadMutexUnlock(&handle->mutex);

  if (QUEUE_IS_EMPTY(&wq)) {
    return;
  }
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue *el = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(el);

    SSvrRespMsg *pResp = QUEUE_DATA(el, SSvrRespMsg, q);
    code = evtSvrHandleSendResp(handle->hostThrd, pResp);
  }

  return;
}

void *transWorkerThread(void *arg) {
  int32_t code = 0;
  int32_t line = 0;

  struct timeval tv = {5, 0};
  setThreadName("trans-svr-work");
  SWorkThrd2 *pThrd = (SWorkThrd2 *)arg;

  SEvtMgt *pOpt = NULL;

  code = evtMgtCreate(&pOpt);
  if (code != 0) {
    tError("failed to create select op since %s", tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  pOpt->hostThrd = pThrd;

  code = evtAsyncInit(pOpt, pThrd->pipe_fd, &pThrd->notifyNewConnHandle, evtNewConnNotifyCb, EVT_CONN_T, (void *)pThrd);
  if (code != 0) {
    tError("failed to create select op since %s", tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  code = evtAsyncInit(pOpt, pThrd->pipe_queue_fd, &pThrd->asyncHandle, evtHandleRespCb, EVT_ASYNC_T, (void *)pThrd);
  if (code != 0) {
    tError("failed to create select op since %s", tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  while (!pThrd->quit) {
    code = evtMgtDispath(pOpt, &tv);
    if (code != 0) {
      tError("failed to dispatch since %s", tstrerror(code));
      continue;
    }
  }
_end:
  if (code != 0) {
    tError("failed to do work %s", tstrerror(code));
  }
  evtMgtDestroy(pOpt);
  return NULL;
}
static int32_t addHandleToAcceptloop(void *arg) {
  // impl later
  int32_t      code = 0;
  SServerObj2 *srv = arg;
  int32_t      server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  srv->serverFd = server_fd;

  int32_t opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = inet_addr("0.0.0.0");
  server_addr.sin_port = htons(srv->port);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  if (listen(server_fd, 128) < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  code = taosThreadCreate(&srv->thread, NULL, transAcceptThread, srv);
  return code;
}

void *transInitServer2(uint32_t ip, uint32_t port, char *label, int numOfThreads, void *fp, void *pInit) {
  int32_t code = 0;

  SServerObj2 *srv = taosMemoryCalloc(1, sizeof(SServerObj2));
  if (srv == NULL) {
    code = terrno;
    tError("failed to init server since: %s", tstrerror(code));
    return NULL;
  }

  srv->ip = ip;
  srv->port = port;
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->numOfWorkerReady = 0;
  srv->pThreadObj = (SWorkThrd2 **)taosMemoryCalloc(srv->numOfThreads, sizeof(SWorkThrd2 *));
  if (srv->pThreadObj == NULL) {
    code = terrno;
    return NULL;
  }
  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrd2 *thrd = (SWorkThrd2 *)taosMemoryCalloc(1, sizeof(SWorkThrd2));
    thrd->pInst = pInit;
    thrd->quit = false;
    thrd->pInst = pInit;
    thrd->connRefMgt = transOpenRefMgt(50000, transDestroyExHandle);
    if (thrd->connRefMgt < 0) {
      code = thrd->connRefMgt;
      goto End;
    }

    // QUEUE_INIT(&thrd->fdQueue.q);
    {
      int fd[2] = {0};
      code = pipe(fd);
      if (code < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        goto End;
      }
      thrd->pipe_fd[0] = fd[0];
      thrd->pipe_fd[1] = fd[1];
    }

    {
      int fd2[2] = {0};
      code = pipe(fd2);
      if (code < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        goto End;
      }
      thrd->pipe_queue_fd[0] = fd2[0];
      thrd->pipe_queue_fd[1] = fd2[1];
    }

    int err = taosThreadCreate(&(thrd->thread), NULL, transWorkerThread, (void *)(thrd));
    if (err == 0) {
      tDebug("success to create worker-thread:%d", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread:%d", i);
      goto End;
    }
    thrd->inited = 1;
  }
  code = addHandleToAcceptloop(srv);
  if (code != 0) {
    goto End;
  }
  return NULL;
End:
  return NULL;
}

// int32_t transReleaseSrvHandle(void *handle) { return 0; }
// void    transRefSrvHandle(void *handle) { return; }

// void    transUnrefSrvHandle(void *handle) { return; }

int32_t transSendResponse2(STransMsg *msg) {
  int32_t code = 0;

  if (msg->info.noResp) {
    rpcFreeCont(msg->pCont);
    tTrace("no need send resp");
    return 0;
  }
  SExHandle *exh = msg->info.handle;

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

  SWorkThrd2 *pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrRespMsg *m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }
  m->msg = tmsg;

  m->type = Normal;

  STraceId *trace = (STraceId *)&msg->info.traceId;
  tGDebug("conn %p start to send resp (1/2)", exh->handle);
  if ((code = evtAsyncSend(pThrd->asyncHandle, &m->q)) != 0) {
    destroySmsg(m);
    transReleaseExHandle(msg->info.refIdMgt, refId);
    return code;
  }

  transReleaseExHandle(msg->info.refIdMgt, refId);
  return 0;

_return1:
  tDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  transReleaseExHandle(msg->info.refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  return code;
}
// int32_t transRegisterMsg(const STransMsg *msg) { return 0; }
// int32_t transSetIpWhiteList(void *thandle, void *arg, FilteFunc *func) { return 0; }

void transCloseServer2(void *arg) {
  // impl later
  return;
}
// impl client with poll
typedef struct SCliConn {
  int32_t ref;
  // uv_connect_t connReq;
  // uv_stream_t *stream;

  // uv_timer_t *timer;  // read timer, forbidden

  void *hostThrd;

  SConnBuffer readBuf;
  STransQueue reqsToSend;
  STransQueue reqsSentOut;

  queue q;
  // SConnList *list;

  STransCtx  ctx;
  bool       broken;  // link broken or not
  ConnStatus status;  //

  // SCliBatch *pBatch;

  // SDelayTask *task;

  // HeapNode node;  // for heap
  int8_t   inHeap;
  int32_t  reqRefCnt;
  uint32_t clientIp;
  uint32_t serverIp;

  char *dstAddr;
  char  src[32];
  char  dst[32];

  char   *ipStr;
  int32_t port;

  int64_t seq;

  int8_t    registered;
  int8_t    connnected;
  SHashObj *pQTable;
  int8_t    userInited;
  void     *pInitUserReq;

  void   *heap;  // point to req conn heap
  int32_t heapMissHit;
  int64_t lastAddHeapTime;
  int8_t  forceDelFromHeap;

  // uv_buf_t *buf;
  int32_t bufSize;
  int32_t readerStart;

  queue wq;  // uv_write_t queue

  queue   batchSendq;
  int8_t  inThreadSendq;
  int32_t fd;
  queue   reqsToSend2;
} SCliConn;

typedef struct {
  SCliConn *conn;
  void     *arg;
} SReqState;

typedef struct {
  int64_t seq;
  int32_t msgType;
} SFiterArg;
typedef struct SCliReq {
  SReqCtx      *ctx;
  queue         q;
  queue         sendQ;
  STransMsgType type;
  uint64_t      st;
  int64_t       seq;
  int32_t       sent;  //(0: no send, 1: alread sent)
  int8_t        inSendQ;
  STransMsg     msg;
  int8_t        inRetry;

} SCliReq;

#define EPSET_IS_VALID(epSet)       ((epSet) != NULL && (epSet)->numOfEps >= 0 && (epSet)->inUse >= 0)
#define EPSET_GET_SIZE(epSet)       (epSet)->numOfEps
#define EPSET_GET_INUSE_IP(epSet)   ((epSet)->eps[(epSet)->inUse].fqdn)
#define EPSET_GET_INUSE_PORT(epSet) ((epSet)->eps[(epSet)->inUse].port)
#define EPSET_FORWARD_INUSE(epSet)                             \
  do {                                                         \
    if ((epSet)->numOfEps != 0) {                              \
      ++((epSet)->inUse);                                      \
      (epSet)->inUse = ((epSet)->inUse) % ((epSet)->numOfEps); \
    }                                                          \
  } while (0)
typedef struct SCliThrd2 {
  TdThread thread;  // tid
  int64_t  pid;     // pid

  // uv_loop_t  *loop;
  // SAsyncPool *asyncPool;
  void *pool;  // conn pool
  // timer handles
  SArray *timerList;
  // msg queue
  queue         msg;
  TdThreadMutex msgMtx;
  // SDelayQueue  *delayQueue;
  // SDelayQueue  *timeoutQueue;
  // SDelayQueue  *waitConnQueue;
  uint64_t nextTimeout;  // next timeout
  STrans  *pInst;        //

  void (*destroyAhandleFp)(void *ahandle);
  SHashObj *fqdn2ipCache;
  SCvtAddr *pCvtAddr;

  SHashObj *failFastCache;
  SHashObj *batchCache;
  SHashObj *connHeapCache;

  SCliReq *stopMsg;
  bool     quit;

  int32_t (*initCb)(void *arg, SCliReq *pReq, STransMsg *pResp);
  int32_t (*notifyCb)(void *arg, SCliReq *pReq, STransMsg *pResp);
  int32_t (*notifyExceptCb)(void *arg, SCliReq *pReq, STransMsg *pResp);

  SHashObj *pIdConnTable;  // <qid, conn>

  SArray       *pQIdBuf;  // tmp buf to avoid alloc buf;
  queue         batchSendSet;
  int8_t        thrdInited;
  int32_t       pipe_queue_fd[2];
  SEvtMgt      *pEvtMgt;
  SAsyncHandle *asyncHandle;
} SCliThrd2;

typedef struct SCliObj2 {
  char        label[TSDB_LABEL_LEN];
  int32_t     index;
  int         numOfThreads;
  SCliThrd2 **pThreadObj;
} SCliObj2;

static int32_t createSocket(char *ip, int32_t port, int32_t *fd) {
  int32_t code = 0;
  int32_t line = 0;
  int32_t sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }
  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }

  if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }

  return code;
_end:
  if (code != 0) {
    tError("%s failed to connect to %s:%d at line %d since %s", __func__, ip, port, line, tstrerror(code));
  }
  return code;
}
static int32_t cliConnGetSockInfo(SCliConn *pConn) {
  int32_t            code = 0;
  int32_t            line = 0;
  struct sockaddr_in addr;

  socklen_t addr_len = sizeof(addr);
  char      ip_str[INET_ADDRSTRLEN];
  int       port;

  // 获取对端地址
  if (getpeername(pConn->fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }
  inet_ntop(AF_INET, &addr.sin_addr, ip_str, sizeof(ip_str));
  port = ntohs(addr.sin_port);
  snprintf(pConn->dst, sizeof(pConn->dst), "%s:%d", ip_str, port);

  // 获取本地地址
  if (getsockname(pConn->fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }
  inet_ntop(AF_INET, &addr.sin_addr, ip_str, sizeof(ip_str));
  port = ntohs(addr.sin_port);
  snprintf(pConn->src, sizeof(pConn->src), "%s:%d", ip_str, port);
  return code;
_end:
  if (code != 0) {
    tError("%s failed to get sock info at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}
static int32_t createCliConn(SCliThrd2 *pThrd, char *ip, int32_t port, SCliConn **ppConn) {
  int32_t   code = 0;
  int32_t   line = 0;
  STrans   *pInst = pThrd->pInst;
  SCliConn *pConn = taosMemoryCalloc(1, sizeof(SCliConn));
  if (pConn == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _end);
  }
  char addr[TSDB_FQDN_LEN + 64] = {0};
  snprintf(addr, sizeof(addr), "%s:%d", ip, port);
  pConn->hostThrd = pThrd;
  pConn->dstAddr = taosStrdup(addr);
  pConn->ipStr = taosStrdup(ip);
  pConn->port = port;
  if (pConn->dstAddr == NULL || pConn->ipStr == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &line, _end);
  }
  pConn->status = ConnNormal;
  pConn->broken = false;
  QUEUE_INIT(&pConn->q);

  TAOS_CHECK_GOTO(transInitBuffer(&pConn->readBuf), NULL, _end);
  pConn->seq = 0;

  pConn->pQTable = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pConn->pQTable == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }
  QUEUE_INIT(&pConn->batchSendq);
  pConn->bufSize = pInst->shareConnLimit;
  return code;

  QUEUE_INIT(&pConn->reqsToSend2);

  TAOS_CHECK_GOTO(createSocket(ip, port, &pConn->fd), &line, _end);
  TAOS_CHECK_GOTO(cliConnGetSockInfo(pConn), &line, _end);
  return code;

_end:
  if (code != 0) {
    // TODO, delete conn mem
    tError("%s failed to create conn at line %d since %s", __func__, line, tstrerror(code));
    taosMemoryFree(pConn);
  }
  return code;
}

static FORCE_INLINE void destroyReqCtx(SReqCtx *ctx) {
  if (ctx) {
    taosMemoryFree(ctx->epSet);
    taosMemoryFree(ctx->origEpSet);
    taosMemoryFree(ctx);
  }
}
static FORCE_INLINE void removeReqFromSendQ(SCliReq *pReq) {
  if (pReq == NULL || pReq->inSendQ == 0) {
    return;
  }
  QUEUE_REMOVE(&pReq->sendQ);
  pReq->inSendQ = 0;
}
static void destroyThrdObj(SCliThrd2 *pThrd) {
  if (pThrd == NULL) {
    return;
  }
  if (pThrd->pEvtMgt) {
    evtMgtDestroy(pThrd->pEvtMgt);
  }
  taosMemoryFree(pThrd);
}
static int32_t createThrdObj(void *trans, SCliThrd2 **ppThrd) {
  int32_t line = 0;
  int32_t code = 0;

  STrans *pInst = trans;

  SCliThrd2 *pThrd = (SCliThrd2 *)taosMemoryCalloc(1, sizeof(SCliThrd2));
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _end);
  }

  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  QUEUE_INIT(&pThrd->msg);

  *ppThrd = pThrd;
  return code;
_end:
  if (code != 0) {
    destroyThrdObj(pThrd);
    tError("%s failed to init rpc client at line %d since %s,", __func__, line, tstrerror(code));
  }
  return code;
}

FORCE_INLINE int cliRBChoseIdx(STrans *pInst) {
  int32_t index = pInst->index;
  if (pInst->numOfThreads == 0) {
    return -1;
  }
  /*
   * no lock, and to avoid CPU load imbalance, set limit pInst->numOfThreads * 2000;
   */
  if (pInst->index++ >= pInst->numOfThreads * 2000) {
    pInst->index = 0;
  }
  return index % pInst->numOfThreads;
}
static FORCE_INLINE SCliThrd2 *transGetWorkThrdFromHandle(STrans *trans, int64_t handle) {
  SCliThrd2 *pThrd = NULL;
  SExHandle *exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return NULL;
  }
  taosWLockLatch(&exh->latch);
  if (exh->pThrd == NULL && trans != NULL) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    exh->pThrd = ((SCliObj2 *)trans->tcphandle)->pThreadObj[idx];
  }

  pThrd = exh->pThrd;
  taosWUnLockLatch(&exh->latch);
  transReleaseExHandle(transGetRefMgt(), handle);

  return pThrd;
}
SCliThrd2 *transGetWorkThrd(STrans *trans, int64_t handle) {
  if (handle == 0) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    return ((SCliObj2 *)trans->tcphandle)->pThreadObj[idx];
  }
  SCliThrd2 *pThrd = transGetWorkThrdFromHandle(trans, handle);
  return pThrd;
}
static int32_t transInitMsg(void *pInstRef, const SEpSet *pEpSet, STransMsg *pReq, STransCtx *ctx, SCliReq **pCliMsg) {
  int32_t code = 0;
  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  SCliReq *pCliReq = NULL;
  SReqCtx *pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _exception);
  }

  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->epSet);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, NULL, _exception);
  }

  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->origEpSet);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, NULL, _exception);
  }

  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if (ctx != NULL) pCtx->userCtx = *ctx;

  pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pCliReq == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _exception);
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  *pCliMsg = pCliReq;
  return code;
_exception:
  if (pCtx != NULL) {
    taosMemoryFree(pCtx->epSet);
    taosMemoryFree(pCtx->origEpSet);
    taosMemoryFree(pCtx);
  }
  taosMemoryFree(pCliReq);
  return code;
}

static FORCE_INLINE void destroyReq(void *arg) {
  SCliReq *pReq = arg;
  if (pReq == NULL) {
    return;
  }

  removeReqFromSendQ(pReq);
  STraceId *trace = &pReq->msg.info.traceId;
  tGDebug("free memory:%p, free ctx: %p", pReq, pReq->ctx);

  if (pReq->ctx) {
    destroyReqCtx(pReq->ctx);
  }
  transFreeMsg(pReq->msg.pCont);
  taosMemoryFree(pReq);
}
int32_t transSendRequest2(void *pInstRef, const SEpSet *pEpSet, STransMsg *pReq, STransCtx *ctx) {
  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t    code = 0;
  int64_t    handle = (int64_t)pReq->info.handle;
  SCliThrd2 *pThrd = transGetWorkThrd(pInst, handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception);
  }

  pReq->info.qId = handle;

  SCliReq *pCliMsg = NULL;
  TAOS_CHECK_GOTO(transInitMsg(pInstRef, pEpSet, pReq, ctx, &pCliMsg), NULL, _exception);

  STraceId *trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);

  code = evtAsyncSend(pThrd->asyncHandle, &pCliMsg->q);
  if (code != 0) {
    destroyReq(pCliMsg);
    transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }

  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (code != 0) {
    tError("failed to send request since %s", tstrerror(code));
  }
  return code;
}

static int32_t evtCliHandleResp(SCliConn *pConn, char *msg, int32_t msgLen) {
  int32_t code = 0;

  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  SCliReq       *pReq = NULL;
  STransMsgHead *pHead = (STransMsgHead *)msg;

  int64_t qId = taosHton64(pHead->qid);
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);
  int64_t seq = taosHton64(pHead->seqNum);

  STransMsg resp = {0};
  (pInst->cfp)(pInst->parent, &resp, NULL);

  return code;
}
static int32_t evtCliPreSendReq(void *arg, SEvtBuf *buf, int32_t status) {
  int32_t   code = 0;
  SCliConn *pConn = arg;

  return code;
}

static int32_t evtCliSendCb(void *arg, int32_t status) {
  int32_t   code = status;
  SCliConn *pConn = arg;
  if (code != 0) {
    tError("failed to send request since %s", tstrerror(code));
    return code;
  }
  return code;
}

static int32_t evtCliReadResp(void *arg, SEvtBuf *buf, int32_t bytes) {
  int32_t   code;
  int32_t   line = 0;
  SFdCbArg *pArg = arg;
  SCliConn *pConn = pArg->data;
  if (bytes == 0) {
    tDebug("client %s closed", pConn->src);
    return TSDB_CODE_RPC_NETWORK_ERROR;
  }
  SConnBuffer *p = &pConn->readBuf;
  if (p->cap - p->len < bytes) {
    int32_t newCap = p->cap + bytes;
    char   *newBuf = taosMemoryRealloc(p->buf, newCap);
    if (newBuf == NULL) {
      TAOS_CHECK_GOTO(terrno, &line, _end);
    }
    p->buf = newBuf;
    p->cap = newCap;
  }

  memcpy(p->buf + p->len, buf->buf, bytes);
  p->len += bytes;

  while (p->len >= sizeof(STransMsgHead)) {
    STransMsgHead head;
    memcpy(&head, p->buf, sizeof(head));
    int32_t msgLen = (int32_t)htonl(head.msgLen);
    if (p->len >= msgLen) {
      char *pMsg = taosMemoryCalloc(1, msgLen);
      if (pMsg == NULL) {
        TAOS_CHECK_GOTO(terrno, &line, _end);
      }
      memcpy(pMsg, p->buf, msgLen);
      memcpy(p->buf + msgLen, p->buf, p->len - msgLen);
      p->len -= msgLen;

      code = evtCliHandleResp(pConn, pMsg, msgLen);
      TAOS_CHECK_GOTO(terrno, &line, _end);
    } else {
      break;
    }
  }

  return code;
_end:
  if (code != 0) {
    tError("%s failed to handle resp at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}
static int32_t evtHandleCliReq(SCliThrd2 *pThrd, SCliReq *req) {
  int32_t code = 0;
  char   *fqdn = EPSET_GET_INUSE_IP(req->ctx->epSet);
  int32_t port = EPSET_GET_INUSE_PORT(req->ctx->epSet);

  SCliConn *pConn = NULL;
  code = createCliConn(pThrd, fqdn, port, &pConn);
  if (code != 0) {
    tError("failed to create conn since %s", tstrerror(code));
    return code;
  } else {
    QUEUE_PUSH(&pConn->reqsToSend2, &req->q);
    STraceId *trace = &req->msg.info.traceId;
    tGDebug("success to create conn %p, src:%s, dst:%s", pConn, pConn->src, pConn->dst);

    SFdCbArg arg = {.evtType = EVT_CONN_T,
                    .arg = pConn,
                    .fd = pConn->fd,
                    .readCb = evtCliReadResp,
                    .sendCb = evtCliPreSendReq,
                    .sendFinishCb = evtCliSendCb,
                    .data = pConn};

    code = evtMgtAdd(pThrd->pEvtMgt, pConn->fd, EVT_READ | EVT_WRITE, &arg);
  }
  return code;
}
static void evtHandleCliReqCb(void *arg, int32_t status) {
  int32_t       code = 0;
  SAsyncHandle *handle = arg;
  SEvtMgt      *pEvtMgt = handle->data;

  SCliThrd2 *pThrd = pEvtMgt->hostThrd;

  queue wq;
  QUEUE_INIT(&wq);

  taosThreadMutexLock(&handle->mutex);
  QUEUE_MOVE(&handle->q, &wq);
  taosThreadMutexUnlock(&handle->mutex);

  if (QUEUE_IS_EMPTY(&wq)) {
    return;
  }
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue *el = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(el);

    SCliReq *pReq = QUEUE_DATA(el, SCliReq, q);
    if (pReq == NULL) {
      continue;
    }
    STraceId *trace = &pReq->msg.info.traceId;
    tGDebug("handle request at thread:%08" PRId64 ", dst:%s:%d, app:%p", pThrd->pid, pReq->ctx->epSet->eps[0].fqdn,
            pReq->ctx->epSet->eps[0].port, pReq->msg.info.ahandle);

    if (pReq->msg.info.handle == 0) {
      destroyReq(pReq);
      continue;
    }
  }
}

static void *cliWorkThread2(void *arg) {
  int32_t        line = 0;
  int32_t        code = 0;
  char           threadName[TSDB_LABEL_LEN] = {0};
  struct timeval tv = {30, 0};
  SCliThrd2     *pThrd = (SCliThrd2 *)arg;

  pThrd->pid = taosGetSelfPthreadId();

  tsEnableRandErr = false;
  TAOS_UNUSED(strtolower(threadName, pThrd->pInst->label));
  setThreadName(threadName);

  code = evtMgtCreate(&pThrd->pEvtMgt);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  pThrd->pEvtMgt->hostThrd = pThrd;

  code = evtAsyncInit(pThrd->pEvtMgt, pThrd->pipe_queue_fd, &pThrd->asyncHandle, evtHandleCliReqCb, EVT_ASYNC_T,
                      (void *)pThrd);
  TAOS_CHECK_GOTO(code, &line, _end);

  while (!pThrd->quit) {
    code = evtMgtDispath(pThrd->pEvtMgt, &tv);
    if (code != 0) {
      tError("failed to dispatch since %s", tstrerror(code));
      continue;
    } else {
      tDebug("success to dispatch");
    }
  }

  tDebug("thread quit-thread:%08" PRId64 "", pThrd->pid);
  return NULL;
_end:
  if (code != 0) {
    tError("failed to do work %s", tstrerror(code));
  }
  return NULL;
}
void *transInitClient2(uint32_t ip, uint32_t port, char *label, int numOfThreads, void *fp, void *pInstRef) {
  int32_t   code = 0;
  SCliObj2 *cli = taosMemoryCalloc(1, sizeof(SCliObj2));
  if (cli == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _err);
  }

  STrans *pInst = pInstRef;
  memcpy(cli->label, label, TSDB_LABEL_LEN);
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrd2 **)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd2 *));
  if (cli->pThreadObj == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _err);
  }
  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd2 *pThrd = NULL;
    code = createThrdObj(pInstRef, &pThrd);
    if (code != 0) {
      goto _err;
    }
    int fd[2] = {0};

    code = pipe(fd);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      TAOS_CHECK_GOTO(code, NULL, _err);
    }
    pThrd->pipe_queue_fd[0] = fd[0];
    pThrd->pipe_queue_fd[1] = fd[1];
    pThrd->pInst = pInst;

    int err = taosThreadCreate(&pThrd->thread, NULL, cliWorkThread2, (void *)(pThrd));
    if (err != 0) {
      destroyThrdObj(pThrd);
      code = TAOS_SYSTEM_ERROR(errno);
      TAOS_CHECK_GOTO(code, NULL, _err);
    } else {
      tDebug("success to create tranport-cli thread:%d", i);
    }
    pThrd->thrdInited = 1;
    cli->pThreadObj[i] = pThrd;
  }
  return cli;

_err:
  if (cli) {
    for (int i = 0; i < cli->numOfThreads; i++) {
      // send quit msg
      destroyThrdObj(cli->pThreadObj[i]);
    }
    taosMemoryFree(cli->pThreadObj);
    taosMemoryFree(cli);
  }
  terrno = code;
  return NULL;
}
void transCloseClient2(void *arg) {
  int32_t code = 0;
  return;
}
