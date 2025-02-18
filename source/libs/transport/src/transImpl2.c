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
#define ALLOW_FORBID_FUNC
#include "theap.h"
#include "transComm.h"
#include "tversion.h"
#ifdef TD_ASTRA
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <select.h>
#include <sockLib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "tmisce.h"

#endif

#define DIV_ROUNDUP(x, y) (((x) + ((y)-1)) / (y))

#define EVT_TIMEOUT 0x01
#define EVT_READ    0x02
#define EVT_WRITE   0x04
#define EVT_SIGNAL  0x08
#define EVT_ASYNC   0x10
#define NFDBITS     (8 * sizeof(long))

#define SELECT_ALLOC_SIZE(n) (DIV_ROUNDUP(n, NFDBITS) * sizeof(fd_mask))

typedef void (*AsyncCb)(void *async, int32_t status);

typedef struct {
  void         *data;
  int32_t       sendFd;
  AsyncCb       cb;
  queue         q;
  TdThreadMutex mutex;
  void         *hostThrd;
  int8_t        stop;
} SAsyncHandle;
typedef struct {
  queue   q;
  int32_t acceptFd;
} SFdArg;
typedef struct {
  queue q;
} SFdQueue;

typedef struct STaskArg {
  void *param1;
  void *param2;
} STaskArg;
typedef struct SDelayTask {
  void (*func)(void *arg);
  void    *arg;
  uint64_t execTime;
  HeapNode node;
} SDelayTask;

typedef struct {
  Heap *heap;
  void *mgt;
} SDelayQueue;

static int32_t transDQCreate2(void *loop, SDelayQueue **queue);
static void    transDQDestroy2(SDelayQueue *queue, void (*freeFunc)(void *arg));
static void    transDQCancel2(SDelayQueue *queue, SDelayTask *task);
static int32_t transDQHandleTimeout2(SDelayQueue *queue);
static int32_t transDQGetNextTimeout2(SDelayQueue *queue);
static int32_t transDQSched2(SDelayQueue *queue, void (*func)(void *arg), void *arg, uint64_t timeoutMs,
                             SDelayTask **pTask);
typedef struct {
  int       notifyCount;  //
  int       init;         // init or not
  STransMsg msg;
} SSvrRegArg;
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

typedef struct {
  char   *buf;
  int32_t len;
  int8_t  inited;
  int32_t offset;
  void   *data;

} SEvtBuf;
typedef struct SSvrRespMsg {
  SSvrConn     *pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
  int64_t       seqNum;
  void         *arg;
  FilteFunc     func;
  int8_t        sent;

  SEvtBuf buf;
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

  void *pEvtMgt;
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

enum EVT_TYPE { EVT_ASYNC_T = 0x1, EVT_CONN_T = 0x2, EVT_SIGANL_T = 0x4, EVT_NEW_CONN_T = 0x8 };

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

  char  label[128];
  void *arg;
  int32_t (*timeoutFunc)(void *arg);
  int32_t (*caclTimeout)(void *arg, int64_t *timeout);
} SEvtMgt;

static int32_t evtMgtResize(SEvtMgt *pOpt, int32_t cap);
static int32_t evtMgtCreate(SEvtMgt **pOpt, char *label);
static int32_t evtMgtDispath(SEvtMgt *pOpt, struct timeval *tv);
static int32_t evtMgtAdd(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg);
static int32_t evtMgtRemove(SEvtMgt *pOpt, int32_t fd, int32_t events, SFdCbArg *arg);
static void    evtMgtDestroy(SEvtMgt *pOpt);

int32_t evtMgtHandle(SEvtMgt *pOpt, int32_t res, int32_t fd);

static int32_t evtInitPipe(int32_t dst[2]) {
  int32_t code = 0;
  int     fd[2] = {0};
  code = pipe(fd);
  if (code < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }
  dst[0] = fd[0];
  dst[1] = fd[1];
  return code;
}
static int32_t evtMgtCreate(SEvtMgt **pOpt, char *label) {
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
  memcpy(pRes->label, label, strlen(label));

  code = evtMgtResize(pRes, 32);
  if (code != 0) {
    evtMgtDestroy(pRes);
  } else {
    *pOpt = pRes;
  }

  return code;
}
static int32_t evtMgtAddTimoutFunc(SEvtMgt *pOpt, void *arg, int32_t (*timeoutFunc)(void *arg),
                                   int32_t (*caclTimeout)(void *arg, int64_t *timeout)) {
  int32_t code = 0;

  // pOpt->arg = arg;
  pOpt->timeoutFunc = timeoutFunc;
  pOpt->caclTimeout = caclTimeout;
  return code;
}

static int32_t evtBufInit(SEvtBuf *evtBuf) {
  int32_t code = 0;
  if (evtBuf->inited == 0) {
    evtBuf->buf = taosMemoryCalloc(1, 4096);
    if (evtBuf->buf == NULL) {
      code = terrno;
    } else {
      evtBuf->len = 4096;
      evtBuf->inited = 1;
      evtBuf->offset = 0;
    }
  }
  return code;
}

static int32_t evtBufPush(SEvtBuf *evtBuf, char *buf, int32_t len) {
  int32_t code = 0;
  if (evtBuf->inited == 0) {
    code = evtBufInit(evtBuf);
    if (code != 0) {
      return code;
    }
  }
  int32_t need = evtBuf->offset + len;
  if (need >= evtBuf->len) {
    // TOOD opt need
    char *tbuf = taosMemoryRealloc(evtBuf->buf, need);
    if (tbuf == NULL) {
      return terrno;
    }
    evtBuf->buf = tbuf;
    evtBuf->len = need;
  }
  memcpy(evtBuf->buf + evtBuf->offset, buf, len);
  evtBuf->offset += len;
  return code;
}

static int32_t evtBufClear(SEvtBuf *evtBuf) {
  int32_t code = 0;
  if (evtBuf->inited) {
    evtBuf->offset = 0;
    memset(evtBuf->buf, 0, evtBuf->len);
  }
  return code;
}

static int32_t evtBufDestroy(SEvtBuf *evtBuf) {
  if (evtBuf->inited) {
    taosMemoryFree(evtBuf->buf);
  }
  return 0;
}
int32_t evtMgtHandleImpl(SEvtMgt *pOpt, SFdCbArg *pArg, int res) {
  int32_t nBytes = 0;
  char    buf[512] = {0};
  int32_t code = 0;
  if (pArg->evtType == EVT_NEW_CONN_T || pArg->evtType == EVT_ASYNC_T) {
    // handle new coming conn;
    if (res & EVT_READ) {
      SAsyncHandle *handle = pArg->arg;

      nBytes = read(pArg->fd, buf, sizeof(buf));
      if (nBytes >= 1 && buf[0] == '1') {
        handle->cb(handle, 0);
      }
      tTrace("%s handle async read on fd:%d", pOpt->label, pArg->fd);
    }
    if (res & EVT_WRITE) {
      tTrace("%s handle async write on fd:%d", pOpt->label, pArg->fd);
      // handle err
    }
  } else if (pArg->evtType == EVT_CONN_T) {
    if (res & EVT_READ) {
      SEvtBuf *pBuf = &pArg->buf;
      code = evtBufInit(pBuf);
      if (code != 0) {
        tError("%s failed to init buf since %s", pOpt->label, tstrerror(code));
        return code;
      }
      tTrace("%s handle read on fd:%d", pOpt->label, pArg->fd);
      nBytes = recv(pArg->fd, pBuf->buf, pBuf->len, 0);
      if (nBytes > 0) {
        code = pArg->readCb(pArg, pBuf, nBytes);
      } else {
        code = pArg->readCb(pArg, pBuf, nBytes);
      }
      // handle read
    }

    if (res & EVT_WRITE) {
      SEvtBuf *pBuf = &pArg->sendBuf;
      code = evtBufInit(pBuf);
      if (code != 0) {
        tError("%s failed to init wbuf since %s", pOpt->label, tstrerror(code));
        return code;
      }
      tTrace("%s handle write on fd:%d", pOpt->label, pArg->fd);

      code = pArg->sendCb(pArg, pBuf, 0);
      if (code != 0) {
        tError("%s failed to build send buf since %s", pOpt->label, tstrerror(code));
      }
      int32_t total = pBuf->offset;
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

      evtBufClear(&pArg->sendBuf);
      tTrace("%s handle write finish on fd:%d", pOpt->label, pArg->fd);
      pArg->sendFinishCb(pArg, code);
      if (code != 0) {
        tError("%s failed to send buf since %s", pOpt->label, tstrerror(code));
      } else {
        tTrace("%s succ to send buf", pOpt->label);
      }
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

static int32_t evtCalcNextTimeout(SEvtMgt *pOpt, struct timeval *tv) {
  int32_t code = 0;
  int64_t timeout = 0;
  if (pOpt->caclTimeout) {
    code = pOpt->caclTimeout(pOpt->arg, &timeout);
    if (timeout == 0) {
      tv->tv_sec = 0;
      tv->tv_usec = 10 * 1000;
    } else if (timeout < 0) {
      tv->tv_sec = 30;
      tv->tv_usec = 0;
    } else {
      tv->tv_sec = timeout / 1000;
      tv->tv_usec = (timeout % 1000) * 1000;
    }
  } else {
    tv->tv_sec = 30;
    tv->tv_usec = 0;
  }

  if (tv->tv_sec <= 0) {
    if (tv->tv_usec <= 50 * 1000) tv->tv_usec = 50 * 1000;
  }
  tTrace("%s next timeout %ld, sec:%d, usec:%d", pOpt->label, timeout, (int32_t)tv->tv_sec, (int32_t)tv->tv_usec);
  return code;
}
static int32_t evtMgtDispath(SEvtMgt *pOpt, struct timeval *tv) {
  int32_t code = 0, res = 0, j, nfds = 0, active_Fds = 0;

  if (pOpt->timeoutFunc != NULL) {
    code = pOpt->timeoutFunc(pOpt->arg);
    if (code != 0) {
      tError("%s failed to handle timeout since %s", pOpt->label, tstrerror(code));
    } else {
      tTrace("%s succ to handle timeout", pOpt->label);
    }
  }

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
  tTrace("%s start to select, timeout:%d s: %d ms", pOpt->label, (int32_t)tv->tv_sec, (int32_t)tv->tv_usec / 1000);
  active_Fds = select(nfds, pOpt->evtReadSetOut, pOpt->evtWriteSetOut, NULL, tv);
  if (active_Fds < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  } else if (active_Fds == 0) {
    tTrace("%s select timeout occurred", pOpt->label);
    return code;
  } else {
    tTrace("%s select count %d", pOpt->label, active_Fds);
  }

  for (j = 0; j < nfds && active_Fds > 0; j++) {
    int32_t fd = pOpt->fd[j];
    res = 0;
    if ((fd > 0) && FD_ISSET(fd, pOpt->evtReadSetOut)) {
      res |= EVT_READ;
    }
    if ((fd > 0) && FD_ISSET(fd, pOpt->evtWriteSetOut)) {
      res |= EVT_WRITE;
    }
    if (fd > 0) {
      tTrace("%s start to handle fd %d", pOpt->label, fd);
    }

    if (res == 0) {
      continue;
    } else {
      active_Fds--;
      code = evtMgtHandle(pOpt, res, fd);
      if (code != 0) {
        tError("%s failed to handle fd %d since %s", pOpt->label, fd, tstrerror(code));
      } else {
        tTrace("%s succ to handle fd %d", pOpt->label, fd);
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
  int8_t  rwRef = 0;
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

  SFdCbArg *p = taosHashGet(pOpt->pFdTable, &fd, sizeof(fd));
  if (p != NULL) {
    if (events & EVT_READ) {
      FD_SET(fd, pOpt->evtReadSetIn);
      rwRef++;
    }
    if (events & EVT_WRITE) {
      FD_SET(fd, pOpt->evtWriteSetIn);
      rwRef++;
    }
    p->rwRef += rwRef;
    if (p->rwRef >= 2) {
      p->rwRef = 2;
    }
  } else {
    if (arg == NULL) {
      return TSDB_CODE_INVALID_MSG;
    }
    if (pOpt->fdIdx >= 1024) {
      return TSDB_CODE_INVALID_MSG;
      // taosClose(fd);
    }
    if (fd != 0) {
      if (events & EVT_READ) {
        FD_SET(fd, pOpt->evtReadSetIn);
        rwRef++;
      }

      if (events & EVT_WRITE) {
        FD_SET(fd, pOpt->evtWriteSetIn);
        rwRef++;
      }
      arg->rwRef = rwRef;
      if (arg->rwRef >= 2) {
        arg->rwRef = 2;
      }
      pOpt->fd[pOpt->fdIdx++] = fd;
      code = taosHashPut(pOpt->pFdTable, &fd, sizeof(fd), arg, sizeof(*arg));
    }
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
    tError("%s %s failed to get fd %d since %s", pOpt->label, __func__, fd, tstrerror(TAOS_SYSTEM_ERROR(EBADF)));
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
  tDebug("%s succ to destroy evt mgt", pOpt->label);
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
  tTrace("%s succ to init async on fd:%d", pOpt->label, fd[0]);
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

void *evtSvrAcceptThread(void *arg) {
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
int32_t evtSvrGetConnRefOfThrd(SWorkThrd2 *thrd) { return thrd ? thrd->connRefMgt : -1; }

void evtSvrDestroyResp(void *e) {
  SSvrRespMsg *pMsg = QUEUE_DATA(e, SSvrRespMsg, q);
  destroySmsg(pMsg);
}
static int32_t evtSvrConnGetSockInfo(SSvrConn *pConn) {
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
static void evtSvrDestroyConn(SSvrConn *pConn) {
  if (pConn == NULL) {
    return;
  }
  if (pConn->refId > 0) {
    SWorkThrd2 *pThrd = pConn->hostThrd;
    transReleaseExHandle(evtSvrGetConnRefOfThrd(pThrd), pConn->refId);
    transRemoveExHandle(evtSvrGetConnRefOfThrd(pThrd), pConn->refId);
  }
  if (pConn->fd > 0) {
    close(pConn->fd);
    pConn->fd = -1;
  }

  transDestroyBuffer(&pConn->readBuf);
  taosHashCleanup(pConn->pQTable);
  taosMemoryFree(pConn);
}
static SSvrConn *evtSvrCreateConn(void *tThrd, int32_t fd) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SWorkThrd2 *pThrd = tThrd;
  SSvrConn   *pConn = taosMemoryCalloc(1, sizeof(SSvrConn));
  if (pConn == NULL) {
    return NULL;
  }
  pConn->hostThrd = pThrd;

  pConn->fd = fd;
  pConn->pInst = pThrd->pInst;
  QUEUE_INIT(&pConn->queue);

  code = evtSvrConnGetSockInfo(pConn);
  TAOS_CHECK_GOTO(code, &lino, _end);

  if ((code = transInitBuffer(&pConn->readBuf)) != 0) {
    TAOS_CHECK_GOTO(code, &lino, _end);
  }

  pConn->broken = false;
  pConn->status = ConnNormal;

  SExHandle *exh = taosMemoryMalloc(sizeof(SExHandle));
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
  }

  exh->handle = pConn;
  exh->pThrd = pThrd;
  exh->refId = transAddExHandle(evtSvrGetConnRefOfThrd(pThrd), exh);
  if (exh->refId < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, &lino, _end);
  }

  SExHandle *pSelf = transAcquireExHandle(evtSvrGetConnRefOfThrd(pThrd), exh->refId);
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
  transQueueInit(&pConn->resps, NULL);

  return pConn;
_end:
  if (code != 0) {
    tError("failed to create conn since %s", tstrerror(code));
    evtSvrDestroyConn(pConn);
  }
  return NULL;
}
static int32_t evtSvrHandleSendResp(SWorkThrd2 *pThrd, SSvrRespMsg *pResp);

bool evtSvrConnMayGetUserInfo(SSvrConn *pConn, STransMsgHead **ppHead, int32_t *msgLen) {
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

static int32_t evtSvrHandleReleaseReq(SSvrConn *pConn, STransMsgHead *pHead) {
  int32_t code = 0;
  STrans *pInst = pConn->pInst;
  if (pHead->msgType == TDMT_SCH_TASK_RELEASE) {
    int64_t qId = taosHton64(pHead->qid);
    if (qId <= 0) {
      tError("conn %p recv release, but invalid sid:%" PRId64 "", pConn, qId);
      code = TSDB_CODE_RPC_NO_STATE;
    } else {
      void *p = taosHashGet(pConn->pQTable, &qId, sizeof(qId));
      if (p == NULL) {
        code = TSDB_CODE_RPC_NO_STATE;
        tTrace("conn %p recv release, and releady release by server sid:%" PRId64 "", pConn, qId);
      } else {
        SSvrRegArg *arg = p;
        (pInst->cfp)(pInst->parent, &(arg->msg), NULL);
        tTrace("conn %p recv release, notify server app, sid:%" PRId64 "", pConn, qId);

        code = taosHashRemove(pConn->pQTable, &qId, sizeof(qId));
        if (code != 0) {
          tDebug("conn %p failed to remove sid:%" PRId64 "", pConn, qId);
        }
        tTrace("conn %p clear state,sid:%" PRId64 "", pConn, qId);
      }
    }

    STransMsg tmsg = {.code = code,
                      .msgType = pHead->msgType + 1,
                      .info.qId = qId,
                      .info.traceId = pHead->traceId,
                      .info.seq = taosHton64(pHead->seqNum)};

    SSvrRespMsg *srvMsg = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
    if (srvMsg == NULL) {
      tError("conn %p recv release, failed to send release-resp since %s", pConn, tstrerror(terrno));
      taosMemoryFree(pHead);
      return terrno;
    }
    srvMsg->msg = tmsg;
    srvMsg->type = Normal;
    srvMsg->pConn = pConn;

    evtSvrHandleSendResp(pConn->hostThrd, srvMsg);
    taosMemoryFree(pHead);
    return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
  }

  return 0;
}

static void evtSvrPerfLog_receive(SSvrConn *pConn, STransMsgHead *pHead, STransMsg *pTransMsg) {
  if (!(rpcDebugFlag & DEBUG_DEBUG)) {
    return;
  }

  STrans   *pInst = pConn->pInst;
  STraceId *trace = &pHead->traceId;

  int64_t        cost = taosGetTimestampUs() - taosNtoh64(pHead->timestamp);
  static int64_t EXCEPTION_LIMIT_US = 100 * 1000;

  if (pConn->status == ConnNormal && pHead->noResp == 0) {
    if (cost >= EXCEPTION_LIMIT_US) {
      tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, cost:%dus, recv exception, seqNum:%" PRId64
              ", sid:%" PRId64 "",
              transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
              (int)cost, pTransMsg->info.seq, pTransMsg->info.qId);
    } else {
      tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, cost:%dus, seqNum:%" PRId64 ", sid:%" PRId64 "",
              transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
              (int)cost, pTransMsg->info.seq, pTransMsg->info.qId);
    }
  } else {
    if (cost >= EXCEPTION_LIMIT_US) {
      tGDebug(
          "%s conn %p %s received from %s, local info:%s, len:%d, noResp:%d, code:%d, cost:%dus, recv exception, "
          "seqNum:%" PRId64 ", sid:%" PRId64 "",
          transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
          pHead->noResp, pTransMsg->code, (int)(cost), pTransMsg->info.seq, pTransMsg->info.qId);
    } else {
      tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, noResp:%d, code:%d, cost:%dus, seqNum:%" PRId64
              ", "
              "sid:%" PRId64 "",
              transLabel(pInst), pConn, TMSG_INFO(pTransMsg->msgType), pConn->dst, pConn->src, pTransMsg->contLen,
              pHead->noResp, pTransMsg->code, (int)(cost), pTransMsg->info.seq, pTransMsg->info.qId);
    }
  }
  tGTrace("%s handle %p conn:%p translated to app, refId:%" PRIu64, transLabel(pInst), pTransMsg->info.handle, pConn,
          pConn->refId);
}
static int32_t evtSvrHandleReq(SSvrConn *pConn, char *req, int32_t len) {
  SWorkThrd2 *pThrd = pConn->hostThrd;
  STrans     *pInst = pThrd->pInst;

  int32_t        code = 0;
  int32_t        msgLen = len;
  STransMsgHead *pHead = (STransMsgHead *)req;
  if (evtSvrConnMayGetUserInfo(pConn, &pHead, &msgLen) == true) {
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

  if (evtSvrHandleReleaseReq(pConn, pHead) != 0) {
    return 0;
  }
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

  transMsg.info.handle = (void *)transAcquireExHandle(evtSvrGetConnRefOfThrd(pThrd), pConn->refId);
  transMsg.info.refIdMgt = pThrd->connRefMgt;

  transMsg.info.refId = pHead->noResp == 1 ? -1 : pConn->refId;
  transMsg.info.traceId = pHead->traceId;
  transMsg.info.cliVer = htonl(pHead->compatibilityVer);
  transMsg.info.forbiddenIp = 0;
  transMsg.info.noResp = pHead->noResp == 1 ? 1 : 0;
  transMsg.info.seq = taosHton64(pHead->seqNum);
  transMsg.info.qId = taosHton64(pHead->qid);
  transMsg.info.msgType = pHead->msgType;

  evtSvrPerfLog_receive(pConn, pHead, &transMsg);

  SRpcConnInfo *pConnInfo = &(transMsg.info.conn);
  pConnInfo->clientIp = pConn->clientIp;
  pConnInfo->clientPort = pConn->port;
  tstrncpy(pConnInfo->user, pConn->user, sizeof(pConnInfo->user));

  transReleaseExHandle(evtSvrGetConnRefOfThrd(pThrd), pConn->refId);

  (*pInst->cfp)(pInst->parent, &transMsg, NULL);

  return code;
}
static int32_t evtSvrReadCb(void *arg, SEvtBuf *buf, int32_t bytes) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SFdCbArg *pArg = arg;
  SSvrConn *pConn = pArg->data;
  STrans   *pInst = pConn->pInst;

  if (bytes <= 0) {
    tDebug("%s conn %p closed", transLabel(pInst), pConn);
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
      memmove(p->buf, p->buf + msgLen, p->len - msgLen);
      p->len -= msgLen;

      code = evtSvrHandleReq(pConn, pMsg, msgLen);
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
static int32_t evtSvrPreSendImpl(SSvrConn *pConn, SEvtBuf *pBuf) {
  int32_t code = 0;
  int32_t batchLimit = 32;
  int32_t j = 0;
  while (!transQueueEmpty(&pConn->resps)) {
    queue *el = transQueuePop(&pConn->resps);
    QUEUE_REMOVE(el);

    SSvrRespMsg *pResp = QUEUE_DATA(el, SSvrRespMsg, q);
    STransMsg   *pMsg = &pResp->msg;
    if (pMsg->pCont == 0) {
      pMsg->pCont = (void *)rpcMallocCont(0);
      if (pMsg->pCont == NULL) {
        return terrno;
      }
      pMsg->contLen = 0;
    }
    STransMsgHead *pHead = transHeadFromCont(pMsg->pCont);
    pHead->traceId = pMsg->info.traceId;
    pHead->hasEpSet = pMsg->info.hasEpSet;
    pHead->magicNum = htonl(TRANS_MAGIC_NUM);
    pHead->compatibilityVer = htonl(((STrans *)pConn->pInst)->compatibilityVer);
    pHead->version = TRANS_VER;
    pHead->seqNum = taosHton64(pMsg->info.seq);
    pHead->qid = taosHton64(pMsg->info.qId);
    pHead->withUserInfo = pConn->userInited == 0 ? 1 : 0;

    pHead->msgType = (0 == pMsg->msgType ? pConn->inType + 1 : pMsg->msgType);
    pHead->code = htonl(pMsg->code);
    pHead->msgLen = htonl(pMsg->contLen + sizeof(STransMsgHead));

    char   *msg = (char *)pHead;
    int32_t len = transMsgLenFromCont(pMsg->contLen);

    STrans   *pInst = pConn->pInst;
    STraceId *trace = &pMsg->info.traceId;
    tGDebug("%s conn %p %s is sent to %s, local info:%s, len:%d, seqNum:%" PRId64 ", sid:%" PRId64 "",
            transLabel(pInst), pConn, TMSG_INFO(pHead->msgType), pConn->dst, pConn->src, len, pMsg->info.seq,
            pMsg->info.qId);
    evtBufPush(pBuf, (char *)pHead, len);
    pResp->sent = 1;
    if (j++ > batchLimit) {
      break;
    }
    destroySmsg(pResp);
  }
  return code;
}
static int32_t evtSvrPreSend(void *arg, SEvtBuf *buf, int32_t status) {
  int32_t   code = 0;
  SFdCbArg *pArg = arg;
  SSvrConn *pConn = pArg->data;

  code = evtSvrPreSendImpl(pConn, buf);
  return code;
}

static int32_t evtSvrSendFinishCb(void *arg, int32_t status) {
  int32_t   code = 0;
  SFdCbArg *pArg = arg;
  SSvrConn *pConn = pArg->data;

  SWorkThrd2 *pThrd = pConn->hostThrd;

  if (transQueueEmpty(&pConn->resps)) {
    // stop write evt
    tTrace("%s conn %p stop write evt on fd:%d", transLabel(pThrd->pInst), pConn, pConn->fd);
    code = evtMgtRemove(pThrd->pEvtMgt, pConn->fd, EVT_WRITE, NULL);
  }
  return code;
}
void evtSvrNewConnNotifyCb(void *async, int32_t status) {
  int32_t code = 0;

  SAsyncHandle *handle = async;
  SEvtMgt      *pEvtMgt = handle->data;
  SWorkThrd2   *pThrd = handle->hostThrd;
  STrans       *pInst = pThrd->pInst;

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

    SSvrConn *pConn = evtSvrCreateConn(pEvtMgt->hostThrd, pArg->acceptFd);
    if (pConn == NULL) {
      tError("%s failed to create conn since %s", pInst->label, tstrerror(code));
      taosMemoryFree(pArg);
      continue;
    } else {
      tDebug("%s succ to create svr conn %p, src:%s, dst:%s", pInst->label, pConn, pConn->dst, pConn->src);
    }

    SFdCbArg arg = {.evtType = EVT_CONN_T,
                    .arg = pArg,
                    .fd = pArg->acceptFd,
                    .readCb = evtSvrReadCb,
                    .sendCb = evtSvrPreSend,
                    .sendFinishCb = evtSvrSendFinishCb,
                    .data = pConn};
    code = evtMgtAdd(pEvtMgt, pArg->acceptFd, EVT_READ, &arg);

    if (code != 0) {
      tError("%s failed to add fd to evt since %s", pInst->label, tstrerror(code));
    }
    taosMemoryFree(pArg);
  }

  return;
}

static int32_t evtSvrMayHandleReleaseResp(SSvrRespMsg *pMsg) {
  int32_t   code = 0;
  SSvrConn *pConn = pMsg->pConn;
  int64_t   qid = pMsg->msg.info.qId;
  if (pMsg->msg.msgType == TDMT_SCH_TASK_RELEASE && qid > 0) {
    SSvrRegArg *p = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
    if (p == NULL) {
      tError("%s conn %p already release sid:%" PRId64 "", transLabel(pConn->pInst), pConn, qid);
      return TSDB_CODE_RPC_NO_STATE;
    } else {
      transFreeMsg(p->msg.pCont);
      code = taosHashRemove(pConn->pQTable, &qid, sizeof(qid));
      if (code != 0) {
        tError("%s conn %p failed to release sid:%" PRId64 " since %s", transLabel(pConn->pInst), pConn, qid,
               tstrerror(code));
      }
    }
  }
  return 0;
}
static int32_t evtSvrHandleSendResp(SWorkThrd2 *pThrd, SSvrRespMsg *pResp) {
  int32_t   code = 0;
  SSvrConn *pConn = pResp->pConn;
  if (evtSvrMayHandleReleaseResp(pResp) == TSDB_CODE_RPC_NO_STATE) {
    destroySmsg(pResp);
    return 0;
  }
  transQueuePush(&pConn->resps, &pResp->q);
  return evtMgtAdd(pThrd->pEvtMgt, pConn->fd, EVT_WRITE, NULL);
}

static void evtSvrHandleResp(SSvrRespMsg *pResp, SWorkThrd2 *pThrd) {
  int32_t code = evtSvrHandleSendResp(pThrd, pResp);
  if (code != 0) {
    tError("failed to send resp since %s", tstrerror(code));
  }
}
static void evtSvrHandleUpdate(SSvrRespMsg *pResp, SWorkThrd2 *pThrd) {
  tDebug("conn %p recv update, do nothing", pResp->pConn);
  taosMemoryFree(pResp);
}

static void evtSvrHandleRegiter(SSvrRespMsg *pResp, SWorkThrd2 *pThrd) {
  int32_t   code = 0;
  SSvrConn *pConn = pResp->pConn;

  int64_t     qid = pResp->msg.info.qId;
  SSvrRegArg *p = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
  if (p != NULL) {
    transFreeMsg(p->msg.pCont);
  }

  SSvrRegArg arg = {.notifyCount = 0, .msg = pResp->msg};
  code = taosHashPut(pConn->pQTable, &arg.msg.info.qId, sizeof(arg.msg.info.qId), &arg, sizeof(arg));
  if (code != 0) {
    tError("failed to put qid to hash since %s", tstrerror(code));
    return;
  }
  tDebug("conn %p succ to register sid:%" PRId64 "", pConn, arg.msg.info.qId);
  taosMemoryFree(pResp);
}
static void evtSvrHandleQuit(SSvrRespMsg *pResp, SWorkThrd2 *pThrd) {
  pThrd->quit = 1;
  taosMemoryFree(pResp);
}
static void evtSvrHandleRelease(SSvrRespMsg *pResp, SWorkThrd2 *pThrd) {
  int32_t code = 0;
  taosMemoryFree(pResp);
  return;
}

static void (*transAsyncFuncs[])(SSvrRespMsg *, SWorkThrd2 *) = {
    evtSvrHandleResp, evtSvrHandleQuit, evtSvrHandleRelease, evtSvrHandleRegiter, evtSvrHandleUpdate};

void evtSvrHandleAyncCb(void *async, int32_t status) {
  int32_t code = 0;

  SAsyncHandle *handle = async;
  SWorkThrd2   *pThrd = handle->hostThrd;
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
    if (pResp == NULL) {
      continue;
    }

    if (pResp->type == Quit || pResp->type == Update) {
      transAsyncFuncs[pResp->type](pResp, pThrd);
    } else {
      STransMsg  transMsg = pResp->msg;
      SExHandle *exh1 = transMsg.info.handle;
      int64_t    refId = transMsg.info.refId;
      SExHandle *exh2 = transAcquireExHandle(evtSvrGetConnRefOfThrd(pThrd), refId);
      if (exh1 != exh2) {
        tError("failed to acquire handle since %s", tstrerror(TSDB_CODE_REF_INVALID_ID));
        continue;
      }

      pResp->seqNum = transMsg.info.seq;
      pResp->pConn = exh2->handle;

      transReleaseExHandle(evtSvrGetConnRefOfThrd(pThrd), refId);
      transAsyncFuncs[pResp->type](pResp, pThrd);
    }
  }

  return;
}

void *evtSvrWorkerThread(void *arg) {
  int32_t code = 0;
  int32_t line = 0;

  setThreadName("trans-svr-work");
  SWorkThrd2 *pThrd = (SWorkThrd2 *)arg;
  STrans     *pInst = pThrd->pInst;

  SEvtMgt *pOpt = NULL;

  code = evtMgtCreate(&pOpt, pInst->label);
  if (code != 0) {
    tError("%s failed to create select op since %s", pInst->label, tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  pThrd->pEvtMgt = pOpt;
  pOpt->hostThrd = pThrd;

  code = evtAsyncInit(pOpt, pThrd->pipe_fd, &pThrd->notifyNewConnHandle, evtSvrNewConnNotifyCb, EVT_NEW_CONN_T,
                      (void *)pThrd);
  if (code != 0) {
    tError("%s failed to create evt since %s", pInst->label, tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }

  code = evtAsyncInit(pOpt, pThrd->pipe_queue_fd, &pThrd->asyncHandle, evtSvrHandleAyncCb, EVT_ASYNC_T, (void *)pThrd);
  if (code != 0) {
    tError("%s failed to create select op since %s", pInst->label, tstrerror(code));
    TAOS_CHECK_GOTO(code, &line, _end);
  }
  int32_t count = 0;
  while (!pThrd->quit) {
    struct timeval tv = {30, 0};
    tTrace("%s-------------------- dispatch count:%d -----------------------------", pInst->label, count++);
    code = evtMgtDispath(pOpt, &tv);
    if (code != 0) {
      tError("%s failed to dispatch since %s", pInst->label, tstrerror(code));
      continue;
    }
  }
_end:
  if (code != 0) {
    tError("%s failed to do work %s", pInst->label, tstrerror(code));
  }
  evtMgtDestroy(pOpt);
  return NULL;
}
static int32_t evtSvrAddHandleToAcceptLoop(void *arg) {
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
  code = taosThreadCreate(&srv->thread, NULL, evtSvrAcceptThread, srv);
  return code;
}

void *transInitServer2(uint32_t ip, uint32_t port, char *label, int numOfThreads, void *fp, void *arg) {
  int32_t code = 0;
  int32_t lino = 0;
  STrans *pInst = arg;

  SServerObj2 *srv = taosMemoryCalloc(1, sizeof(SServerObj2));
  if (srv == NULL) {
    code = terrno;
    TAOS_CHECK_EXIT(code);
  }

  srv->ip = ip;
  srv->port = port;
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->numOfWorkerReady = 0;
  srv->pThreadObj = (SWorkThrd2 **)taosMemoryCalloc(srv->numOfThreads, sizeof(SWorkThrd2 *));
  if (srv->pThreadObj == NULL) {
    code = terrno;
    TAOS_CHECK_EXIT(code);
  }
  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrd2 *thrd = (SWorkThrd2 *)taosMemoryCalloc(1, sizeof(SWorkThrd2));
    thrd->pInst = arg;
    thrd->connRefMgt = transOpenRefMgt(50000, transDestroyExHandle);
    if (thrd->connRefMgt < 0) {
      code = thrd->connRefMgt;
      TAOS_CHECK_EXIT(code);
    }

    TAOS_CHECK_EXIT(evtInitPipe(thrd->pipe_fd));
    TAOS_CHECK_EXIT(evtInitPipe(thrd->pipe_queue_fd));
    QUEUE_INIT(&thrd->conn);

    code = taosThreadCreate(&(thrd->thread), NULL, evtSvrWorkerThread, (void *)(thrd));
    TAOS_CHECK_EXIT(code);
    thrd->inited = 1;
    thrd->quit = false;
    srv->pThreadObj[i] = thrd;
  }
  code = evtSvrAddHandleToAcceptLoop(srv);
  TAOS_CHECK_EXIT(code);
  return NULL;
_exit:
  if (code != 0) {
    tError("%s failed to init server at line %d since %s", pInst->label, lino, tstrerror(code));
  }
  return NULL;
}

int32_t transReleaseSrvHandle2(void *handle, int32_t status) {
  int32_t         code = 0;
  SRpcHandleInfo *info = handle;
  SExHandle      *exh = info->handle;
  int64_t         qId = info->qId;
  int64_t         refId = info->refId;

  ASYNC_CHECK_HANDLE(info->refIdMgt, refId, exh);

  SWorkThrd2 *pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  STransMsg tmsg = {.msgType = TDMT_SCH_TASK_RELEASE,
                    .code = status,
                    .info.handle = exh,
                    .info.ahandle = NULL,
                    .info.refId = refId,
                    .info.qId = qId,
                    .info.traceId = info->traceId};

  SSvrRespMsg *m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }

  m->msg = tmsg;
  m->type = Normal;

  tDebug("%s conn %p start to send %s, sid:%" PRId64 "", transLabel(pThrd->pInst), exh->handle, TMSG_INFO(tmsg.msgType),
         qId);
  if ((code = evtAsyncSend(pThrd->asyncHandle, &m->q)) != 0) {
    destroySmsg(m);
    transReleaseExHandle(info->refIdMgt, refId);
    return code;
  }

  transReleaseExHandle(info->refIdMgt, refId);
  return 0;
_return1:
  tDebug("handle %p failed to send to release handle", exh);
  transReleaseExHandle(info->refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to send to release handle", exh);
  return code;
}
int32_t transRegisterMsg2(const STransMsg *msg) {
  int32_t code = 0;

  SExHandle *exh = msg->info.handle;
  int64_t    refId = msg->info.refId;
  ASYNC_CHECK_HANDLE(msg->info.refIdMgt, refId, exh);

  STransMsg tmsg = *msg;
  tmsg.info.noResp = 1;

  tmsg.info.qId = msg->info.qId;
  tmsg.info.seq = msg->info.seq;
  tmsg.info.refId = refId;
  tmsg.info.refIdMgt = msg->info.refIdMgt;

  SWorkThrd2 *pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrRespMsg *m = taosMemoryCalloc(1, sizeof(SSvrRespMsg));
  if (m == NULL) {
    code = terrno;
    goto _return1;
  }

  m->msg = tmsg;
  m->type = Register;

  STrans *pInst = pThrd->pInst;
  tDebug("%s conn %p start to register brokenlink callback", transLabel(pInst), exh->handle);
  if ((code = evtAsyncSend(pThrd->asyncHandle, &m->q)) != 0) {
    destroySmsg(m);
    transReleaseExHandle(msg->info.refIdMgt, refId);
    return code;
  }

  transReleaseExHandle(msg->info.refIdMgt, refId);
  return 0;

_return1:
  tDebug("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  transReleaseExHandle(msg->info.refIdMgt, refId);
  return code;
_return2:
  tDebug("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  return code;
}
int32_t transSetIpWhiteList2(void *thandle, void *arg, FilteFunc *func) { return 0; }
int32_t transSendResponse2(STransMsg *msg) {
  int32_t code = 0;

  if (msg->info.noResp) {
    rpcFreeCont(msg->pCont);
    tTrace("no need send resp");
    return 0;
  }

  STraceId *trace = &msg->info.traceId;
  tGDebug("start to send resp %p", msg);

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

  tGDebug("conn %p start to send resp (1/2)", exh->handle);
  if ((code = evtAsyncSend(pThrd->asyncHandle, &m->q)) != 0) {
    destroySmsg(m);
    transReleaseExHandle(msg->info.refIdMgt, refId);
    return code;
  }

  transReleaseExHandle(msg->info.refIdMgt, refId);
  return 0;

_return1:
  tGDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  transReleaseExHandle(msg->info.refIdMgt, refId);
  return code;
_return2:
  tGDebug("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  return code;
}

void transCloseServer2(void *arg) {
  // impl later
  return;
}
// impl client with poll

typedef struct SConnList {
  queue   conns;
  int32_t size;
  int32_t totalSize;
} SConnList;

typedef struct SCliConn {
  int32_t ref;

  void *hostThrd;

  SConnBuffer readBuf;
  STransQueue reqsToSend;
  STransQueue reqsSentOut;

  queue      q;
  SConnList *list;

  STransCtx  ctx;
  bool       broken;  // link broken or not
  ConnStatus status;  //

  SDelayTask *task;

  HeapNode node;  // for heap
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

  void         *pool;  // conn pool
  SArray       *timerList;
  queue         msg;
  TdThreadMutex msgMtx;
  uint64_t      nextTimeout;  // next timeout
  STrans       *pInst;        //

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
  int32_t       shareConnLimit;
} SCliThrd2;

typedef struct {
  SDelayQueue *queue;
  void        *pEvtMgt;
} STimeoutArg;

int32_t createTimeoutArg(void *pEvtMgt, STimeoutArg **pArg) {
  int32_t code = 0;
  int32_t line = 0;

  STimeoutArg *arg = taosMemoryCalloc(1, sizeof(STimeoutArg));
  if (arg == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &line, _end);
  }
  SDelayQueue *queue = NULL;
  transDQCreate2(arg, &queue);

  arg->queue = queue;
  arg->pEvtMgt = pEvtMgt;
  *pArg = arg;
  return code;
_end:
  if (code != 0) {
    tError("%s failed to create timeout arg at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

void destroyTimeoutArg(STimeoutArg *arg) {
  if (arg == NULL) {
    return;
  }

  SDelayQueue *queue = arg->queue;
  transDQDestroy2(queue, NULL);
  taosMemoryFree(arg);
  return;
}

typedef struct SCliObj2 {
  char        label[TSDB_LABEL_LEN];
  int32_t     index;
  int         numOfThreads;
  SCliThrd2 **pThreadObj;
} SCliObj2;
#define REQS_ON_CONN(conn) (conn ? (transQueueSize(&conn->reqsToSend) + transQueueSize(&conn->reqsSentOut)) : 0)
typedef struct {
  void    *p;
  HeapNode node;
} SHeapNode;
typedef struct {
  // void*    p;
  Heap *heap;
  int32_t (*cmpFunc)(const HeapNode *a, const HeapNode *b);
  int64_t lastUpdateTs;
  int64_t lastConnFailTs;
} SHeap;

static FORCE_INLINE void evtCliDestroyReq(void *arg);
static int32_t           evtHandleCliReq(SCliThrd2 *pThrd, SCliReq *req);

static int32_t compareHeapNode(const HeapNode *a, const HeapNode *b);
static int32_t transHeapInit(SHeap *heap, int32_t (*cmpFunc)(const HeapNode *a, const HeapNode *b));
static void    transHeapDestroy(SHeap *heap);

static int32_t transHeapGet(SHeap *heap, SCliConn **p);
static int32_t transHeapInsert(SHeap *heap, SCliConn *p);
static int32_t transHeapDelete(SHeap *heap, SCliConn *p);
static int32_t transHeapBalance(SHeap *heap, SCliConn *p);
static int32_t transHeapUpdateFailTs(SHeap *heap, SCliConn *p);
static int32_t transHeapMayBalance(SHeap *heap, SCliConn *p);

static int32_t getOrCreateHeapCache(SHashObj *pConnHeapCache, char *key, SHeap **pHeap);
static int8_t  balanceConnHeapCache(SHashObj *pConnHeapCache, SCliConn *pConn, SCliConn **pNewConn);
static int32_t delConnFromHeapCache(SHashObj *pConnHeapCache, SCliConn *pConn);

static SCliConn *getConnFromHeapCache(SHashObj *pConnHeap, char *key);
static int32_t   addConnToHeapCache(SHashObj *pConnHeap, SCliConn *pConn);

static void evtCliHandlReq(SCliReq *pReq, SCliThrd2 *pThrd);
static void evtCliHandleQuit(SCliReq *pReq, SCliThrd2 *pThrd);
static void evtCliHandleRelease(SCliReq *pReq, SCliThrd2 *pThrd);
static void evtCliHandleRegiter(SCliReq *pReq, SCliThrd2 *pThrd);
static void evtCliHandleUpdate(SCliReq *pReq, SCliThrd2 *pThrd);
static void (*transCliAsyncFuncs[])(SCliReq *, SCliThrd2 *) = {evtCliHandlReq, evtCliHandleQuit, evtCliHandleRelease,
                                                               evtCliHandleRelease, evtCliHandleUpdate};

static FORCE_INLINE void evtCliRemoveReqFromSendQ(SCliReq *pReq);

static void    transRefCliHandle(void *handle);
static int32_t transUnrefCliHandle(void *handle);
// static int32_t transGetRefCount(void *handle);

static void evtCliHandlReq(SCliReq *pReq, SCliThrd2 *pThrd) {
  int32_t code = evtHandleCliReq(pThrd, pReq);
  if (code != 0) {
    tDebug("failed to handle req");
  }
}
static void evtCliHandleQuit(SCliReq *pReq, SCliThrd2 *pThrd) {
  tDebug("recv quit, set quit flag");
  pThrd->quit = 1;
  evtCliDestroyReq(pReq);
  return;
}
static void evtCliHandleRelease(SCliReq *pReq, SCliThrd2 *pThrd) {
  tDebug("recv release, do nothing");
  evtCliDestroyReq(pReq);
  return;
}
static void evtCliHandleRegiter(SCliReq *pReq, SCliThrd2 *pThrd) {
  tDebug("recv register, do nothing");
  evtCliDestroyReq(pReq);
  return;
}
static void evtCliHandleUpdate(SCliReq *pReq, SCliThrd2 *pThrd) {
  tDebug("recv update, do nothing");
  evtCliDestroyReq(pReq);
  return;
}
static int32_t evtCliCreateSocket(uint32_t ip, int32_t port, int32_t *fd) {
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
  server_addr.sin_addr.s_addr = ip;

  if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }
  *fd = sockfd;
  return code;
_end:
  if (code != 0) {
    if (sockfd > 0) {
      close(sockfd);
    }
    tError("%s failed to connect to %d:%d at line %d since %s", __func__, ip, port, line, tstrerror(code));
  }
  return code;
}
static int32_t evtCliConnGetSockInfo(SCliConn *pConn) {
  int32_t            code = 0;
  int32_t            line = 0;
  struct sockaddr_in addr;

  socklen_t addr_len = sizeof(addr);
  char      ip_str[INET_ADDRSTRLEN];
  int       port;

  // get peer address
  if (getpeername(pConn->fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _end);
  }
  inet_ntop(AF_INET, &addr.sin_addr, ip_str, sizeof(ip_str));
  port = ntohs(addr.sin_port);
  snprintf(pConn->dst, sizeof(pConn->dst), "%s:%d", ip_str, port);

  // get local address
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

static int32_t evtCliGetOrCreateConnList(SCliThrd2 *pThrd, const char *key, SConnList **ppList) {
  int32_t    code = 0;
  void      *pool = pThrd->pool;
  size_t     klen = strlen(key);
  SConnList *plist = taosHashGet((SHashObj *)pool, key, klen);
  if (plist == NULL) {
    SConnList list = {0};
    QUEUE_INIT(&list.conns);
    code = taosHashPut((SHashObj *)pool, key, klen, (void *)&list, sizeof(list));
    if (code != 0) {
      return code;
    }

    plist = taosHashGet(pool, key, klen);
    if (plist == NULL) {
      return TSDB_CODE_INVALID_PTR;
    }
    QUEUE_INIT(&plist->conns);
    *ppList = plist;
    tDebug("create conn list %p for key %s", plist, key);
  } else {
    *ppList = plist;
  }
  return 0;
}
static void evtCliCloseIdleConn(void *param) {
  STaskArg  *arg = param;
  SCliConn  *conn = arg->param1;
  SCliThrd2 *thrd = arg->param2;
  tDebug("%s conn %p idle, close it", thrd->pInst->label, conn);
  conn->task = NULL;
  taosMemoryFree(arg);

  // taosCloseSocketNoCheck1(conn->fd);
  int32_t ref = transUnrefCliHandle(conn);
  if (ref <= 0) {
    return;
  }
}
static void evtCliAddConnToPool(void *pool, SCliConn *conn) {
  int32_t code = 0;
  if (conn->status == ConnInPool) {
    return;
  }

  SCliThrd2 *thrd = conn->hostThrd;
  if (thrd->quit == true) {
    return;
  }

  // cliResetConnTimer(conn);
  if (conn->list == NULL && conn->dstAddr != NULL) {
    conn->list = taosHashGet((SHashObj *)pool, conn->dstAddr, strlen(conn->dstAddr));
  }

  conn->status = ConnInPool;
  QUEUE_INIT(&conn->q);
  QUEUE_PUSH(&conn->list->conns, &conn->q);
  conn->list->size += 1;
  tDebug("conn %p added to pool, pool size: %d, dst: %s", conn, conn->list->size, conn->dstAddr);

  conn->heapMissHit = 0;

  if (conn->list->size >= 5) {
    STaskArg *arg = taosMemoryCalloc(1, sizeof(STaskArg));
    if (arg == NULL) return;
    arg->param1 = conn;
    arg->param2 = thrd;

    STrans *pInst = thrd->pInst;
    code = transDQSched2(thrd->pEvtMgt->arg, evtCliCloseIdleConn, arg, (1000 * (pInst->idleTime)), &conn->task);
    if (code != 0) {
      taosMemoryFree(arg);
      tError("failed to schedule idle conn %p since %s", tstrerror(code));
    }
  }
}

static int32_t evtCliGetConnFromPool(SCliThrd2 *pThrd, const char *key, SCliConn **ppConn) {
  int32_t code = 0;
  void   *pool = pThrd->pool;
  STrans *pInst = pThrd->pInst;

  SConnList *plist = NULL;
  code = evtCliGetOrCreateConnList(pThrd, key, &plist);
  if (code != 0) {
    return code;
  }

  if (QUEUE_IS_EMPTY(&plist->conns)) {
    if (plist->totalSize >= pInst->connLimitNum) {
      return TSDB_CODE_RPC_MAX_SESSIONS;
    }
    return TSDB_CODE_RPC_NETWORK_BUSY;
  }

  queue *h = QUEUE_HEAD(&plist->conns);
  plist->size -= 1;
  QUEUE_REMOVE(h);

  SCliConn *conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_INIT(&conn->q);
  conn->list = plist;

  if (conn->task != NULL) {
    SDelayTask *task = conn->task;
    conn->task = NULL;
    transDQCancel2(((SCliThrd2 *)conn->hostThrd)->pEvtMgt->arg, task);
  }

  tDebug("conn %p get from pool, pool size:%d, dst:%s", conn, conn->list->size, conn->dstAddr);

  *ppConn = conn;
  return 0;
}
static int32_t evtCliGetIpFromFqdn(SHashObj *pTable, char *fqdn, uint32_t *ip) {
  int32_t   code = 0;
  uint32_t  addr = 0;
  size_t    len = strlen(fqdn);
  uint32_t *v = taosHashGet(pTable, fqdn, len);
  if (v == NULL) {
    code = taosGetIpv4FromFqdn(fqdn, &addr);
    if (code != 0) {
      code = TSDB_CODE_RPC_FQDN_ERROR;
      tError("failed to get ip from fqdn:%s since %s", fqdn, tstrerror(code));
      return code;
    }

    if ((code = taosHashPut(pTable, fqdn, len, &addr, sizeof(addr)) != 0)) {
      return code;
    }
    *ip = addr;
  } else {
    *ip = *v;
  }
  return 0;
}
static void evtCliDestroySocket(SCliConn *pConn) {
  if (pConn == NULL) return;

  if (pConn->fd > 0) {
    taosCloseSocketNoCheck1(pConn->fd);
    pConn->fd = -1;
  }
  taosMemoryFreeClear(pConn->dstAddr);
  taosMemoryFreeClear(pConn->ipStr);
  transDestroyBuffer(&pConn->readBuf);

  if (pConn->pQTable) {
    taosHashCleanup(pConn->pQTable);
  }

  taosMemoryFree(pConn);
}
static int32_t evtCliCreateNewSocket(SCliThrd2 *pThrd, char *ip, int32_t port, SCliConn **ppConn) {
  int32_t   code = 0;
  int32_t   line = 0;
  STrans   *pInst = pThrd->pInst;
  SCliConn *pConn = NULL;
  uint32_t  ipAddr;

  char addr[TSDB_FQDN_LEN + 64] = {0};
  snprintf(addr, sizeof(addr), "%s:%d", ip, port);

  pConn = taosMemoryCalloc(1, sizeof(SCliConn));
  if (pConn == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _end);
  }
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

  transQueueInit(&pConn->reqsToSend, NULL);
  transQueueInit(&pConn->reqsSentOut, NULL);

  TAOS_CHECK_GOTO(evtCliGetIpFromFqdn(pThrd->fqdn2ipCache, ip, &ipAddr), &line, _end);

  TAOS_CHECK_GOTO(evtCliCreateSocket(ipAddr, port, &pConn->fd), &line, _end);
  TAOS_CHECK_GOTO(evtCliConnGetSockInfo(pConn), &line, _end);

  pConn->connnected = 1;
  addConnToHeapCache(pThrd->connHeapCache, pConn);

  pConn->list = taosHashGet(pThrd->pool, addr, strlen(addr));
  pConn->list->totalSize += 1;

  tDebug("%s succ to create cli conn %p src:%s, dst:%s", pInst->label, pConn, pConn->src, pConn->dst);

  *ppConn = pConn;
  return code;
_end:
  if (code != 0) {
    // TODO, delete conn mem
    tError("%s failed to create new conn at line %d since %s", __func__, line, tstrerror(code));
    evtCliDestroySocket(pConn);
  }
  return code;
}
static int32_t evtCliGetOrCreateConn(SCliThrd2 *pThrd, char *ip, int32_t port, SCliConn **ppConn) {
  int32_t   code = 0;
  int32_t   line = 0;
  STrans   *pInst = pThrd->pInst;
  SCliConn *pConn = NULL;
  uint32_t  ipAddr;

  char addr[TSDB_FQDN_LEN + 64] = {0};
  snprintf(addr, sizeof(addr), "%s:%d", ip, port);
  pConn = getConnFromHeapCache(pThrd->connHeapCache, addr);
  if (pConn != NULL) {
    *ppConn = pConn;
    return code;
  }

  code = evtCliGetConnFromPool(pThrd, addr, &pConn);
  if (pConn != NULL) {
    *ppConn = pConn;
    addConnToHeapCache(pThrd->connHeapCache, pConn);
    return 0;
  }
  if (code == TSDB_CODE_RPC_MAX_SESSIONS) {
    return code;
  }

  code = evtCliCreateNewSocket(pThrd, ip, port, ppConn);

_end:
  if (code != 0) {
    tError("%s failed to create conn at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

static FORCE_INLINE void evtCliDestroyReqCtx(SReqCtx *ctx) {
  if (ctx) {
    taosMemoryFree(ctx->epSet);
    taosMemoryFree(ctx->origEpSet);
    taosMemoryFree(ctx);
  }
}

static bool    filterAllReq(void *e, void *arg) { return 1; }
static int32_t cliNotifyCb(SCliConn *pConn, SCliReq *pReq, STransMsg *pResp);

static void evtCliNotifyAndDestroyReq(SCliConn *pConn, SCliReq *pReq, int32_t code) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  SReqCtx  *pCtx = pReq ? pReq->ctx : NULL;
  STransMsg resp = {0};
  resp.code = (pConn->connnected ? TSDB_CODE_RPC_BROKEN_LINK : TSDB_CODE_RPC_NETWORK_UNAVAIL);
  if (code != 0) {
    resp.code = code;
  }

  resp.msgType = pReq ? pReq->msg.msgType + 1 : 0;
  resp.info.cliVer = pInst->compatibilityVer;
  resp.info.ahandle = pCtx ? pCtx->ahandle : 0;
  resp.info.handle = pReq->msg.info.handle;
  if (pReq) {
    resp.info.traceId = pReq->msg.info.traceId;
  }

  STraceId *trace = &resp.info.traceId;
  tDebug("%s conn %p notify user and destroy msg %s since %s", pInst->label, pConn, TMSG_INFO(pReq->msg.msgType),
         tstrerror(resp.code));

  // handle noresp and inter manage msg
  if (pCtx == NULL || pReq->msg.info.noResp) {
    tDebug("%s conn %p destroy %s msg directly since %s", pInst->label, pConn, TMSG_INFO(pReq->msg.msgType),
           tstrerror(resp.code));
    evtCliDestroyReq(pReq);
    return;
  }

  pReq->seq = 0;
  code = cliNotifyCb(pConn, pReq, &resp);
  if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
    return;
  } else {
    // already notify user
    evtCliDestroyReq(pReq);
  }
}

static FORCE_INLINE void evtCliDestroyReqInQueue(SCliConn *conn, queue *set, int32_t code) {
  while (!QUEUE_IS_EMPTY(set)) {
    queue *el = QUEUE_HEAD(set);
    QUEUE_REMOVE(el);

    SCliReq *pReq = QUEUE_DATA(el, SCliReq, q);
    evtCliRemoveReqFromSendQ(pReq);
    evtCliNotifyAndDestroyReq(conn, pReq, code);
  }
}
static FORCE_INLINE int32_t evtCliDestroyAllReqs(SCliConn *conn) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = conn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  queue      set;
  QUEUE_INIT(&set);
  // TODO
  // 1. from qId from thread table
  // 2. not itera to all reqs
  transQueueRemoveByFilter(&conn->reqsSentOut, filterAllReq, NULL, &set, -1);
  transQueueRemoveByFilter(&conn->reqsToSend, filterAllReq, NULL, &set, -1);

  evtCliDestroyReqInQueue(conn, &set, 0);
  return 0;
}
static void evtCliDestroyAllQidFromThrd(SCliConn *conn) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = conn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  void *pIter = taosHashIterate(conn->pQTable, NULL);
  while (pIter != NULL) {
    int64_t *qid = taosHashGetKey(pIter, NULL);

    code = taosHashRemove(pThrd->pIdConnTable, qid, sizeof(*qid));
    if (code != 0) {
      tDebug("%s conn %p failed to remove state %" PRId64 " since %s", pInst->label, conn, *qid, tstrerror(code));
    } else {
      tDebug("%s conn %p destroy sid::%" PRId64 "", pInst->label, conn, *qid);
    }

    STransCtx *ctx = pIter;
    transCtxCleanup(ctx);

    transReleaseExHandle(transGetRefMgt(), *qid);
    transRemoveExHandle(transGetRefMgt(), *qid);

    pIter = taosHashIterate(conn->pQTable, pIter);
  }
  taosHashCleanup(conn->pQTable);
  conn->pQTable = NULL;
}
static void evtCliDestroy(SCliConn *pConn) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  tDebug("%s conn %p try to destroy", pInst->label, pConn);

  code = evtCliDestroyAllReqs(pConn);
  if (code != 0) {
    tDebug("%s conn %p failed to all reqs since %s", pInst->label, pConn, tstrerror(code));
  }

  pConn->forceDelFromHeap = 1;
  code = delConnFromHeapCache(pThrd->connHeapCache, pConn);
  if (code != 0) {
    tDebug("%s conn %p failed to del conn from heapcach since %s", pInst->label, pConn, tstrerror(code));
  }

  taosMemoryFreeClear(pConn->dstAddr);
  taosMemoryFreeClear(pConn->ipStr);

  if (pConn->pInitUserReq) {
    taosMemoryFreeClear(pConn->pInitUserReq);
  }

  taosCloseSocketNoCheck1(pConn->fd);
  transDestroyBuffer(&pConn->readBuf);

  tTrace("%s conn %p destroy successfully", pInst->label, pConn);

  taosMemoryFree(pConn);
}
static void evtCliDestroyConn(SCliConn *pConn, bool force) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  code = evtCliDestroyAllReqs(pConn);
  if (code != 0) {
    tError("%s conn %p failed to destroy all reqs on conn since %s", pInst->label, pConn, tstrerror(code));
  }

  evtCliDestroyAllQidFromThrd(pConn);

  if (pThrd->quit == false && pConn->list) {
    QUEUE_REMOVE(&pConn->q);
    pConn->list->totalSize -= 1;
    pConn->list = NULL;
  }

  if (pConn->task != NULL) {
    transDQCancel2(((SCliThrd2 *)pConn->hostThrd)->pEvtMgt->arg, pConn->task);
    pConn->task = NULL;
  }
  pConn->forceDelFromHeap = 1;
  code = delConnFromHeapCache(pThrd->connHeapCache, pConn);
  if (code != 0) {
    tError("%s conn %p failed to del conn from heapcach since %s", pInst->label, pConn, tstrerror(code));
  }

  if (pConn->connnected) {
    int8_t ref = pConn->ref;
    if (ref == 0) {
      evtCliDestroy(pConn);
    }
  }
  return;
}
static void transRefCliHandle(void *handle) {
  int32_t ref = 0;
  if (handle == NULL) {
    return;
  }
  SCliConn  *conn = (SCliConn *)handle;
  SCliThrd2 *thrd = conn->hostThrd;
  conn->ref++;

  tTrace("%s conn %p ref %d", thrd->pInst->label, conn, conn->ref);
}
static int32_t transUnrefCliHandle(void *handle) {
  if (handle == NULL) {
    return 0;
  }
  int32_t    ref = 0;
  SCliConn  *conn = (SCliConn *)handle;
  SCliThrd2 *thrd = conn->hostThrd;
  conn->ref--;
  ref = conn->ref;

  tTrace("%s conn %p ref:%d", thrd->pInst->label, conn, conn->ref);
  if (conn->ref == 0) {
    evtCliDestroyConn(conn, false);
  }
  return ref;
}
// static int32_t transGet

static FORCE_INLINE void evtCliRemoveReqFromSendQ(SCliReq *pReq) {
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
static int32_t evtCliCreateThrdObj(void *trans, SCliThrd2 **ppThrd) {
  int32_t line = 0;
  int32_t code = 0;

  STrans *pInst = trans;

  SCliThrd2 *pThrd = (SCliThrd2 *)taosMemoryCalloc(1, sizeof(SCliThrd2));
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _end);
  }
  pThrd->pool = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

  QUEUE_INIT(&pThrd->msg);
  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  pThrd->destroyAhandleFp = pInst->destroyFp;

  pThrd->fqdn2ipCache = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->fqdn2ipCache == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }

  pThrd->batchCache = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->batchCache == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }

  pThrd->connHeapCache = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->connHeapCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->pIdConnTable = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pThrd->connHeapCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->pQIdBuf = taosArrayInit(8, sizeof(int64_t));
  if (pThrd->pQIdBuf == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->pInst = pInst;
  pThrd->shareConnLimit = pInst->shareConnLimit;

  *ppThrd = pThrd;
  return code;
_end:
  if (code != 0) {
    destroyThrdObj(pThrd);
    tError("%s failed to init rpc client at line %d since %s,", __func__, line, tstrerror(code));
  }
  return code;
}

FORCE_INLINE int evtCliRBCloseIdx(STrans *pInst) {
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
    int idx = evtCliRBCloseIdx(trans);
    if (idx < 0) return NULL;
    exh->pThrd = ((SCliObj2 *)trans->tcphandle)->pThreadObj[idx];
  }

  pThrd = exh->pThrd;
  taosWUnLockLatch(&exh->latch);
  transReleaseExHandle(transGetRefMgt(), handle);

  return pThrd;
}
static SCliThrd2 *transGetWorkThrd(STrans *trans, int64_t handle) {
  if (handle == 0) {
    int idx = evtCliRBCloseIdx(trans);
    if (idx < 0) return NULL;
    return ((SCliObj2 *)trans->tcphandle)->pThreadObj[idx];
  }
  SCliThrd2 *pThrd = transGetWorkThrdFromHandle(trans, handle);
  return pThrd;
}
static int32_t evtCliInitMsg(void *pInstRef, const SEpSet *pEpSet, STransMsg *pReq, STransCtx *ctx, SCliReq **pCliMsg) {
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

static FORCE_INLINE void evtCliDestroyReq(void *arg) {
  SCliReq *pReq = arg;
  if (pReq == NULL) {
    return;
  }

  evtCliRemoveReqFromSendQ(pReq);
  STraceId *trace = &pReq->msg.info.traceId;
  tGDebug("free memory:%p, free ctx: %p", pReq, pReq->ctx);

  if (pReq->ctx) {
    evtCliDestroyReqCtx(pReq->ctx);
  }
  transFreeMsg(pReq->msg.pCont);
  taosMemoryFree(pReq);
}

int32_t transReleaseCliHandle2(void *handle, int32_t status) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = transGetWorkThrdFromHandle(NULL, (int64_t)handle);
  if (pThrd == NULL) {
    return TSDB_CODE_RPC_BROKEN_LINK;
  }

  STransMsg tmsg = {.msgType = TDMT_SCH_TASK_RELEASE,
                    .info.handle = handle,
                    .info.ahandle = (void *)0,
                    .info.qId = (int64_t)handle,
                    code = status};

  TRACE_SET_MSGID(&tmsg.info.traceId, tGenIdPI64());

  SReqCtx *pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    return terrno;
  }
  pCtx->ahandle = tmsg.info.ahandle;
  SCliReq *cmsg = taosMemoryCalloc(1, sizeof(SCliReq));

  if (cmsg == NULL) {
    taosMemoryFree(pCtx);
    return terrno;
  }
  cmsg->msg = tmsg;
  cmsg->st = taosGetTimestampUs();
  cmsg->type = Normal;
  cmsg->ctx = pCtx;

  STraceId *trace = &tmsg.info.traceId;
  tGDebug("send release request at thread:%08" PRId64 ", malloc memory:%p", pThrd->pid, cmsg);

  if ((code = evtAsyncSend(pThrd->asyncHandle, &cmsg->q)) != 0) {
    evtCliDestroyReq(cmsg);
    return code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code;
  }
  return code;
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
  TAOS_CHECK_GOTO(evtCliInitMsg(pInstRef, pEpSet, pReq, ctx, &pCliMsg), NULL, _exception);

  STraceId *trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);

  code = evtAsyncSend(pThrd->asyncHandle, &pCliMsg->q);
  if (code != 0) {
    evtCliDestroyReq(pCliMsg);
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
static bool evtCliIsReqExceedLimit(STransMsg *pReq) {
  if (pReq != NULL && pReq->contLen >= TRANS_MSG_LIMIT) {
    return true;
  }
  return false;
}
int32_t transSendRequestWithId2(void *pInstRef, const SEpSet *pEpSet, STransMsg *pReq, int64_t *transpointId) {
  if (transpointId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (evtCliIsReqExceedLimit(pReq)) {
    return TSDB_CODE_RPC_MSG_EXCCED_LIMIT;
  }
  int32_t code = 0;
  int8_t  transIdInited = 0;

  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }

  TAOS_CHECK_GOTO(transAllocHandle2(transpointId), NULL, _exception);

  SCliThrd2 *pThrd = transGetWorkThrd(pInst, *transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception);
  }

  SExHandle *exh = transAcquireExHandle(transGetRefMgt(), *transpointId);
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }
  transIdInited = 1;

  pReq->info.handle = (void *)(*transpointId);
  pReq->info.qId = *transpointId;

  SCliReq *pCliMsg = NULL;
  TAOS_CHECK_GOTO(evtCliInitMsg(pInstRef, pEpSet, pReq, NULL, &pCliMsg), NULL, _exception);

  STraceId *trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);
  if ((code = evtAsyncSend(pThrd->asyncHandle, &(pCliMsg->q))) != 0) {
    evtCliDestroyReq(pCliMsg);
    transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }

  transReleaseExHandle(transGetRefMgt(), *transpointId);
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  if (transIdInited) transReleaseExHandle(transGetRefMgt(), *transpointId);
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);

  tError("failed to send request since %s", tstrerror(code));
  return code;
}
int32_t transSendRecv2(void *pInstRef, const SEpSet *pEpSet, STransMsg *pReq, STransMsg *pRsp) {
  if (evtCliIsReqExceedLimit(pReq)) {
    return TSDB_CODE_RPC_MSG_EXCCED_LIMIT;
  }
  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t  code = 0;
  SCliReq *pCliReq = NULL;
  SReqCtx *pCtx = NULL;

  STransMsg *pTransRsp = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  SCliThrd2 *pThrd = transGetWorkThrd(pInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN1);
  }

  tsem_t *sem = taosMemoryCalloc(1, sizeof(tsem_t));
  if (sem == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  code = tsem_init(sem, 0, 0);
  if (code != 0) {
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    TAOS_UNUSED(tsem_destroy(sem));
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->epSet);
  if (code != 0) {
    (TAOS_UNUSED(tsem_destroy(sem)));
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(code, NULL, _RETURN1);
  }

  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->origEpSet);
  if (code != 0) {
    (TAOS_UNUSED(tsem_destroy(sem)));
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(code, NULL, _RETURN1);
  }

  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->pSem = sem;
  pCtx->pRsp = pTransRsp;

  pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pCliReq == NULL) {
    (TAOS_UNUSED(tsem_destroy(sem)));
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  STraceId *trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pCtx->epSet), EPSET_GET_INUSE_PORT(pCtx->epSet), pReq->info.ahandle);

  code = evtAsyncSend(pThrd->asyncHandle, &pCliReq->q);
  if (code != 0) {
    evtCliDestroyReq(pReq);
    TAOS_CHECK_GOTO((code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code), NULL, _RETURN);
  }
  TAOS_UNUSED(tsem_wait(sem));

  memcpy(pRsp, pTransRsp, sizeof(STransMsg));

_RETURN:
  tsem_destroy(sem);
  taosMemoryFree(sem);
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  taosMemoryFree(pTransRsp);
  return code;
_RETURN1:
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  taosMemoryFree(pTransRsp);
  taosMemoryFree(pReq->pCont);
  pReq->pCont = NULL;
  if (pCtx != NULL) {
    taosMemoryFree(pCtx->epSet);
    taosMemoryFree(pCtx->origEpSet);
    taosMemoryFree(pCtx);
  }
  return code;
}
static int32_t transCreateSyncMsg(STransMsg *pTransMsg, int64_t *refId) {
  int32_t  code = 0;
  tsem2_t *sem = taosMemoryCalloc(1, sizeof(tsem2_t));
  if (sem == NULL) {
    return terrno;
  }

  if (tsem2_init(sem, 0, 0) != 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), NULL, _EXIT);
  }

  STransSyncMsg *pSyncMsg = taosMemoryCalloc(1, sizeof(STransSyncMsg));
  if (pSyncMsg == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _EXIT);
  }

  taosInitRWLatch(&pSyncMsg->latch);
  pSyncMsg->inited = 0;
  pSyncMsg->pRsp = pTransMsg;
  pSyncMsg->pSem = sem;
  pSyncMsg->hasEpSet = 0;

  int64_t id = taosAddRef(transGetSyncMsgMgt(), pSyncMsg);
  if (id < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _EXIT);
  } else {
    *refId = id;
  }
  return 0;

_EXIT:
  TAOS_UNUSED(tsem2_destroy(sem));
  taosMemoryFree(sem);
  taosMemoryFree(pSyncMsg);
  return code;
}

int32_t transSendRecvWithTimeout2(void *pInstRef, SEpSet *pEpSet, STransMsg *pReq, STransMsg *pRsp, int8_t *epUpdated,
                                  int32_t timeoutMs) {
  int32_t code = 0;
  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  STransMsg *pTransMsg = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransMsg == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
  }

  SCliThrd2 *pThrd = transGetWorkThrd(pInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN2);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  SReqCtx *pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
  }

  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->epSet);
  if (code != 0) {
    taosMemoryFreeClear(pCtx->epSet);
    TAOS_CHECK_GOTO(code, NULL, _RETURN2);
  }
  code = transCreateReqEpsetFromUserEpset(pEpSet, &pCtx->origEpSet);
  if (code != 0) {
    taosMemoryFreeClear(pCtx->epSet);
    TAOS_CHECK_GOTO(code, NULL, _RETURN2);
  }
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if ((code = transCreateSyncMsg(pTransMsg, &pCtx->syncMsgRef)) != 0) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(code, NULL, _RETURN2);
  }

  int64_t        ref = pCtx->syncMsgRef;
  STransSyncMsg *pSyncMsg = taosAcquireRef(transGetSyncMsgMgt(), ref);
  if (pSyncMsg == NULL) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _RETURN2);
  }

  SCliReq *pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pReq == NULL) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  STraceId *trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pCtx->epSet), EPSET_GET_INUSE_PORT(pCtx->epSet), pReq->info.ahandle);

  code = evtAsyncSend(pThrd->asyncHandle, &pCliReq->q);
  if (code != 0) {
    evtCliDestroyReq(pReq);
    TAOS_CHECK_GOTO(code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code, NULL, _RETURN);
    goto _RETURN;
  }

  code = tsem2_timewait(pSyncMsg->pSem, timeoutMs);
  if (code != 0) {
    pRsp->code = code;
  } else {
    memcpy(pRsp, pSyncMsg->pRsp, sizeof(STransMsg));
    pSyncMsg->pRsp->pCont = NULL;
    if (pSyncMsg->hasEpSet == 1) {
      epsetAssign(pEpSet, &pSyncMsg->epSet);
      *epUpdated = 1;
    }
  }
_RETURN:
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  TAOS_UNUSED(taosReleaseRef(transGetSyncMsgMgt(), ref));
  TAOS_UNUSED(taosRemoveRef(transGetSyncMsgMgt(), ref));
  return code;
_RETURN2:
  transFreeMsg(pReq->pCont);

  if (pCtx != NULL) {
    taosMemoryFree(pCtx->epSet);
    taosMemoryFree(pCtx->origEpSet);
    taosMemoryFree(pCtx);
  }
  pReq->pCont = NULL;
  taosMemoryFree(pTransMsg);
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}

int32_t transSetDefaultAddr2(void *pInstRef, const char *ip, const char *fqdn) {
  if (ip == NULL || fqdn == NULL) return TSDB_CODE_INVALID_PARA;

  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  SCvtAddr cvtAddr = {0};
  tstrncpy(cvtAddr.ip, ip, sizeof(cvtAddr.ip));
  tstrncpy(cvtAddr.fqdn, fqdn, sizeof(cvtAddr.fqdn));
  cvtAddr.cvt = true;

  int32_t code = 0;
  for (int8_t i = 0; i < pInst->numOfThreads; i++) {
    SReqCtx *pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
    if (pCtx == NULL) {
      code = terrno;
      break;
    }

    pCtx->pCvtAddr = (SCvtAddr *)taosMemoryCalloc(1, sizeof(SCvtAddr));
    if (pCtx->pCvtAddr == NULL) {
      taosMemoryFree(pCtx);
      code = terrno;
      break;
    }

    memcpy(pCtx->pCvtAddr, &cvtAddr, sizeof(SCvtAddr));

    SCliReq *pReq = taosMemoryCalloc(1, sizeof(SCliReq));
    if (pReq == NULL) {
      taosMemoryFree(pCtx->pCvtAddr);
      taosMemoryFree(pCtx);
      code = terrno;
      break;
    }

    pReq->ctx = pCtx;
    pReq->type = Update;

    SCliThrd2 *thrd = ((SCliObj2 *)pInst->tcphandle)->pThreadObj[i];
    tDebug("%s update epset at thread:%08" PRId64, pInst->label, thrd->pid);

    if ((code = evtAsyncSend(thrd->asyncHandle, &(pReq->q))) != 0) {
      taosMemoryFree(pCtx->pCvtAddr);
      evtCliDestroyReq(pReq);
      if (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT) {
        code = TSDB_CODE_RPC_MODULE_QUIT;
      }
      break;
    }
  }

  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}
int32_t transAllocHandle2(int64_t *refId) {
  SExHandle *exh = taosMemoryCalloc(1, sizeof(SExHandle));
  if (exh == NULL) {
    return terrno;
  }

  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  if (exh->refId < 0) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  SExHandle *self = transAcquireExHandle(transGetRefMgt(), exh->refId);
  if (exh != self) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  QUEUE_INIT(&exh->q);
  taosInitRWLatch(&exh->latch);
  tDebug("trans alloc sid:%" PRId64 ", malloc:%p", exh->refId, exh);
  *refId = exh->refId;
  return 0;
}

int32_t transFreeConnById2(void *pInstRef, int64_t transpointId) {
  int32_t code = 0;
  STrans *pInst = (STrans *)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  if (transpointId == 0) {
    tDebug("not free by refId:%" PRId64 "", transpointId);
    TAOS_CHECK_GOTO(0, NULL, _exception);
  }

  SCliThrd2 *pThrd = transGetWorkThrdFromHandle(pInst, transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _exception);
  }

  SCliReq *pCli = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pCli == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _exception);
  }
  pCli->type = Normal;

  STransMsg msg = {.msgType = TDMT_SCH_TASK_RELEASE, .info.handle = (void *)transpointId};
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());
  msg.info.qId = transpointId;
  pCli->msg = msg;

  STraceId *trace = &pCli->msg.info.traceId;
  tGDebug("%s start to free conn sid:%" PRId64 "", pInst->label, transpointId);

  code = evtAsyncSend(pThrd->asyncHandle, &pCli->q);
  if (code != 0) {
    taosMemoryFreeClear(pCli);
    TAOS_CHECK_GOTO(code, NULL, _exception);
  }

_exception:
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}

static int32_t evtCliRecycleConn(SCliConn *pConn) {
  int32_t code = 0;

  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  tTrace("%s conn %p in-process req summary:reqsToSend:%d, reqsSentOut:%d, statusTableSize:%d", pInst->label, pConn,
         transQueueSize(&pConn->reqsToSend), transQueueSize(&pConn->reqsSentOut), taosHashGetSize(pConn->pQTable));

  if (transQueueSize(&pConn->reqsToSend) == 0 && transQueueSize(&pConn->reqsSentOut) == 0 &&
      taosHashGetSize(pConn->pQTable) == 0) {
    // cliResetConnTimer(conn);
    pConn->forceDelFromHeap = 1;
    code = delConnFromHeapCache(pThrd->connHeapCache, pConn);
    if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
      tDebug("%s conn %p failed to remove conn from heap cache since %s", pInst->label, pConn, tstrerror(code));

      TAOS_UNUSED(transHeapMayBalance(pConn->heap, pConn));
      return 1;
    } else {
      if (code != 0) {
        tDebug("%s conn %p failed to remove conn from heap cache since %s", pInst->label, pConn, tstrerror(code));
        return 0;
      }
    }
    evtCliAddConnToPool(pThrd->pool, pConn);
    return 1;
  } else if ((transQueueSize(&pConn->reqsToSend) == 0) && (transQueueSize(&pConn->reqsSentOut) == 0) &&
             (taosHashGetSize(pConn->pQTable) != 0)) {
    tDebug("%s conn %p do balance directly", pInst->label, pConn);
    TAOS_UNUSED(transHeapMayBalance(pConn->heap, pConn));
  } else {
    // tTrace("%s conn %p may do balance", CONN_GET_INST_LABEL(conn), conn);
    TAOS_UNUSED(transHeapMayBalance(pConn->heap, pConn));
  }

  return code;
}
static bool filteBySeq(void *key, void *arg) {
  SFiterArg *targ = arg;
  SCliReq   *pReq = QUEUE_DATA(key, SCliReq, q);
  if (pReq->seq == targ->seq && pReq->msg.msgType + 1 == targ->msgType) {
    evtCliRemoveReqFromSendQ(pReq);
    return true;
  } else {
    return false;
  }
}
static int32_t cliGetReqBySeq(SCliConn *pConn, int64_t seq, int32_t msgType, SCliReq **ppReq) {
  int32_t code = 0;
  queue   set;
  QUEUE_INIT(&set);

  SFiterArg arg = {.seq = seq, .msgType = msgType};
  transQueueRemoveByFilter(&pConn->reqsSentOut, filteBySeq, &arg, &set, 1);
  while (QUEUE_IS_EMPTY(&set)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  queue   *e = QUEUE_HEAD(&set);
  SCliReq *pReq = QUEUE_DATA(e, SCliReq, q);

  *ppReq = pReq;
  return 0;
}

#define CONN_GET_INST_LABEL(conn) (((STrans *)(((SCliThrd2 *)(conn)->hostThrd)->pInst))->label)
static bool cliIsEpsetUpdated(int32_t code, SReqCtx *pCtx) {
  if (code != 0) return false;
  return transReqEpsetIsEqual(pCtx->epSet, pCtx->origEpSet) ? false : true;
}
static int32_t evtCliNotifyImplCb(SCliConn *pConn, SCliReq *pReq, STransMsg *pResp) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  SReqCtx   *pCtx = pReq ? pReq->ctx : NULL;
  STraceId  *trace = &pResp->info.traceId;

  if (pCtx == NULL) {
    pInst->cfp(pInst->parent, pResp, NULL);
    return 0;
  }
  if (pCtx->pSem || pCtx->syncMsgRef != 0) {
    tGTrace("%s conn %p(sync) handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (pCtx->pSem) {
      if (pCtx->pRsp == NULL) {
        tGTrace("%s conn %p(sync) failed to resp, ignore", CONN_GET_INST_LABEL(pConn), pConn);
      } else {
        memcpy((char *)pCtx->pRsp, (char *)pResp, sizeof(*pResp));
      }
      TAOS_UNUSED(tsem_post(pCtx->pSem));
      pCtx->pRsp = NULL;
    } else {
      STransSyncMsg *pSyncMsg = taosAcquireRef(transGetSyncMsgMgt(), pCtx->syncMsgRef);
      if (pSyncMsg != NULL) {
        memcpy(pSyncMsg->pRsp, (char *)pResp, sizeof(*pResp));
        if (cliIsEpsetUpdated(pResp->code, pCtx)) {
          pSyncMsg->hasEpSet = 1;

          SEpSet epset = {0};
          if (transCreateUserEpsetFromReqEpset(pCtx->epSet, &epset) != 0) {
            tError("failed to create user epset from req epset");
          }
          epsetAssign(&pSyncMsg->epSet, &epset);
        }
        TAOS_UNUSED(tsem2_post(pSyncMsg->pSem));
        TAOS_UNUSED(taosReleaseRef(transGetSyncMsgMgt(), pCtx->syncMsgRef));
      } else {
        rpcFreeCont(pResp->pCont);
      }
    }
  } else {
    tGTrace("%s conn %p handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (pResp->info.hasEpSet == 1) {
      SEpSet epset = {0};
      if (transCreateUserEpsetFromReqEpset(pCtx->epSet, &epset) != 0) {
        tError("failed to create user epset from req epset");
      }
      pInst->cfp(pInst->parent, pResp, &epset);
    } else {
      if (!cliIsEpsetUpdated(pResp->code, pCtx)) {
        pInst->cfp(pInst->parent, pResp, NULL);
      } else {
        SEpSet epset = {0};
        if (transCreateUserEpsetFromReqEpset(pCtx->epSet, &epset) != 0) {
          tError("failed to create user epset from req epset");
        }
        pInst->cfp(pInst->parent, pResp, &epset);
      }
    }
  }

  return code;
}
FORCE_INLINE bool evtCliTryUpdateEpset(SCliReq *pReq, STransMsg *pResp) {
  int32_t  code = 0;
  SReqCtx *ctx = pReq->ctx;

  if ((pResp == NULL || pResp->info.hasEpSet == 0)) {
    return false;
  }
  // rebuild resp msg
  SEpSet epset;
  if ((code = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epset)) < 0) {
    tError("failed to deserialize epset, code:%d", code);
    return false;
  }
  SEpSet  tepset;
  int32_t tlen = tSerializeSEpSet(NULL, 0, &tepset);

  char   *buf = NULL;
  int32_t len = pResp->contLen - tlen;
  if (len != 0) {
    buf = rpcMallocCont(len);
    if (buf == NULL) {
      pResp->code = TSDB_CODE_OUT_OF_MEMORY;
      return false;
    }
    // TODO: check buf
    memcpy(buf, (char *)pResp->pCont + tlen, len);
  }
  rpcFreeCont(pResp->pCont);

  pResp->pCont = buf;
  pResp->contLen = len;

  pResp->info.hasEpSet = 1;

  if (transCreateReqEpsetFromUserEpset(&epset, &ctx->epSet) != 0) {
    return false;
  }
  return true;
}
static void evtCliMayResetRespCode(SCliReq *pReq, STransMsg *pResp) {
  SReqCtx *pCtx = pReq->ctx;
  if (pCtx->retryCode != TSDB_CODE_SUCCESS) {
    int32_t code = pResp->code;
    // return internal code app
    if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK ||
        code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED) {
      pResp->code = pCtx->retryCode;
    }
  }

  // check whole vnodes is offline on this vgroup
  if (((pCtx->epSet != NULL) && pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) || pCtx->retryStep > 0) {
    if (pResp->code == TSDB_CODE_RPC_BROKEN_LINK) {
      pResp->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;  // TSDB_CODE_RPC_SOMENODE_BROKEN_LINK;
    }
  }
}
void cliRetryMayInitCtx(STrans *pInst, SCliReq *pReq) {
  SReqCtx *pCtx = pReq->ctx;
  if (!pCtx->retryInit) {
    pCtx->retryMinInterval = pInst->retryMinInterval;
    pCtx->retryMaxInterval = pInst->retryMaxInterval;
    pCtx->retryStepFactor = pInst->retryStepFactor;
    pCtx->retryMaxTimeout = pInst->retryMaxTimeout;
    pCtx->retryInitTimestamp = taosGetTimestampMs();
    pCtx->retryNextInterval = pCtx->retryMinInterval;
    pCtx->retryStep = 0;
    pCtx->retryInit = 1;
    pCtx->retryCode = TSDB_CODE_SUCCESS;
    pReq->msg.info.handle = 0;
  }
}
int32_t cliRetryIsTimeout(STrans *pInst, SCliReq *pReq) {
  SReqCtx *pCtx = pReq->ctx;
  if (pCtx->retryMaxTimeout != -1 && taosGetTimestampMs() - pCtx->retryInitTimestamp >= pCtx->retryMaxTimeout) {
    return 1;
  }
  return 0;
}

int8_t cliRetryShouldRetry(STrans *pInst, STransMsg *pResp) {
  bool retry = pInst->retry != NULL ? pInst->retry(pResp->code, pResp->msgType - 1) : false;
  return retry == false ? 0 : 1;
}

void cliRetryUpdateRule(SReqCtx *pCtx, int8_t noDelay) {
  if (noDelay == false) {
    pCtx->epsetRetryCnt = 1;
    pCtx->retryStep++;

    int64_t factor = pow(pCtx->retryStepFactor, pCtx->retryStep - 1);
    pCtx->retryNextInterval = factor * pCtx->retryMinInterval;
    if (pCtx->retryNextInterval >= pCtx->retryMaxInterval) {
      pCtx->retryNextInterval = pCtx->retryMaxInterval;
    }
  } else {
    pCtx->retryNextInterval = 0;
    pCtx->epsetRetryCnt++;
  }
}
bool cliResetEpset(SReqCtx *pCtx, STransMsg *pResp, bool hasEpSet) {
  bool noDelay = true;
  if (hasEpSet == false) {
    if (pResp->contLen == 0) {
      if (pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) {
        noDelay = false;
      } else {
        EPSET_FORWARD_INUSE(pCtx->epSet);
      }
    } else if (pResp->contLen != 0) {
      SEpSet  epSet;
      int32_t valid = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epSet);
      if (valid < 0) {
        tDebug("get invalid epset, epset equal, continue");
        if (pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) {
          noDelay = false;
        } else {
          EPSET_FORWARD_INUSE(pCtx->epSet);
        }
      } else {
        if (!transCompareReqAndUserEpset(pCtx->epSet, &epSet)) {
          tDebug("epset not equal, retry new epset1");
          transPrintEpSet((SEpSet *)pCtx->epSet);
          transPrintEpSet(&epSet);

          if (transCreateReqEpsetFromUserEpset(&epSet, &pCtx->epSet) != 0) {
            tDebug("failed to create req epset from user epset");
          }
          noDelay = false;
        } else {
          if (pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) {
            noDelay = false;
          } else {
            tDebug("epset equal, continue");
            EPSET_FORWARD_INUSE(pCtx->epSet);
          }
        }
      }
    }
  } else {
    SEpSet  epSet;
    int32_t valid = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epSet);
    if (valid < 0) {
      tDebug("get invalid epset, epset equal, continue");
      if (pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) {
        noDelay = false;
      } else {
        EPSET_FORWARD_INUSE(pCtx->epSet);
      }
    } else {
      if (!transCompareReqAndUserEpset(pCtx->epSet, &epSet)) {
        tDebug("epset not equal, retry new epset2");
        transPrintEpSet((SEpSet *)pCtx->epSet);
        transPrintEpSet(&epSet);
        if (transCreateReqEpsetFromUserEpset(&epSet, &pCtx->epSet) != 0) {
          tError("failed to create req epset from user epset");
        }
        noDelay = false;
      } else {
        if (pCtx->epsetRetryCnt >= pCtx->epSet->numOfEps) {
          noDelay = false;
        } else {
          tDebug("epset equal, continue");
          EPSET_FORWARD_INUSE(pCtx->epSet);
        }
      }
    }
  }
  return noDelay;
}

static void doDelayTask(void *param) {
  STaskArg *arg = param;
  if (arg && arg->param1) {
    SCliReq *pReq = arg->param1;
    pReq->inRetry = 1;
  }
  evtCliHandlReq((SCliReq *)arg->param1, (SCliThrd2 *)arg->param2);
  taosMemoryFree(arg);
}
static int32_t evtCliDoSched(SCliReq *pReq, SCliThrd2 *pThrd) {
  int32_t  code = 0;
  STrans  *pInst = pThrd->pInst;
  SReqCtx *pCtx = pReq->ctx;

  STaskArg *arg = taosMemoryMalloc(sizeof(STaskArg));
  if (arg == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  arg->param1 = pReq;
  arg->param2 = pThrd;

  SDelayTask *pTask = NULL;
  code = transDQSched2(pThrd->pEvtMgt->arg, doDelayTask, arg, pCtx->retryNextInterval, &pTask);
  if (pTask == NULL) {
    taosMemoryFree(arg);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return code;
}
static bool evtCliMayRetry(SCliConn *pConn, SCliReq *pReq, STransMsg *pResp) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  SReqCtx *pCtx = pReq->ctx;
  int32_t  code = pResp->code;

  if (pReq && pReq->msg.info.qId != 0) {
    return false;
  }
  cliRetryMayInitCtx(pInst, pReq);

  if (!cliRetryShouldRetry(pInst, pResp)) {
    return false;
  }

  if (cliRetryIsTimeout(pInst, pReq)) {
    return false;
  }

  bool noDelay = false;
  if (code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    transFreeMsg(pResp->pCont);
  } else if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_INTERNAL_ERROR ||
             code == TSDB_CODE_SYN_PROPOSE_NOT_READY || code == TSDB_CODE_VND_STOPPED ||
             code == TSDB_CODE_MNODE_NOT_FOUND || code == TSDB_CODE_APP_IS_STARTING ||
             code == TSDB_CODE_APP_IS_STOPPING || code == TSDB_CODE_VND_STOPPED) {
    tTrace("code str %s, contlen:%d 1", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    transFreeMsg(pResp->pCont);
  } else if (code == TSDB_CODE_SYN_RESTORING) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    transFreeMsg(pResp->pCont);
  } else {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    transFreeMsg(pResp->pCont);
  }
  pResp->pCont = NULL;
  pResp->info.hasEpSet = 0;
  if (code != TSDB_CODE_RPC_BROKEN_LINK && code != TSDB_CODE_RPC_NETWORK_UNAVAIL && code != TSDB_CODE_SUCCESS) {
    // save one internal code
    pCtx->retryCode = code;
  }

  cliRetryUpdateRule(pCtx, noDelay);

  pReq->sent = 0;
  pReq->seq = 0;

  code = evtCliDoSched(pReq, pThrd);
  if (code != 0) {
    pResp->code = code;
    tError("failed to sched msg to next node since %s", tstrerror(code));
    return false;
  }
  return true;
}
static int32_t cliNotifyCb(SCliConn *pConn, SCliReq *pReq, STransMsg *pResp) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  if (pReq != NULL) {
    evtCliRemoveReqFromSendQ(pReq);
    if (pResp->code != TSDB_CODE_SUCCESS) {
      if (evtCliMayRetry(pConn, pReq, pResp)) {
        return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
      }
      evtCliMayResetRespCode(pReq, pResp);
    }
    if (evtCliTryUpdateEpset(pReq, pResp)) {
      // cliPerfLog_epset(pConn, pReq);
    }
  }
  return evtCliNotifyImplCb(pConn, pReq, pResp);
}
static int32_t evtCliBuildRespFromCont(SCliReq *pReq, STransMsg *pResp, STransMsgHead *pHead) {
  pResp->contLen = transContLenFromMsg(pHead->msgLen);
  pResp->pCont = transContFromHead((char *)pHead);
  pResp->code = pHead->code;
  pResp->msgType = pHead->msgType;
  if (pResp->info.ahandle == 0) {
    pResp->info.ahandle = (pReq && pReq->ctx) ? pReq->ctx->ahandle : NULL;
  }
  pResp->info.traceId = pHead->traceId;
  pResp->info.hasEpSet = pHead->hasEpSet;
  pResp->info.cliVer = htonl(pHead->compatibilityVer);
  pResp->info.seq = taosHton64(pHead->seqNum);

  int64_t qid = taosHton64(pHead->qid);
  pResp->info.handle = (void *)qid;
  return 0;
}

static bool filterByQid(void *key, void *arg) {
  int64_t *qid = arg;
  SCliReq *pReq = QUEUE_DATA(key, SCliReq, q);

  if (pReq->msg.info.qId == *qid) {
    evtCliRemoveReqFromSendQ(pReq);
    return true;
  } else {
    return false;
  }
}

static FORCE_INLINE void evtCliDestroyReqWrapper(void *arg, void *param) {
  if (arg == NULL) return;

  SCliReq   *pReq = arg;
  SCliThrd2 *pThrd = param;

  if (pReq->ctx != NULL && pReq->ctx->ahandle != NULL) {
    if (pReq->msg.info.notFreeAhandle == 0 && pThrd != NULL && pThrd->destroyAhandleFp != NULL) {
      (*pThrd->destroyAhandleFp)(pReq->ctx->ahandle);
    }
  }
  evtCliDestroyReq(pReq);
}
static FORCE_INLINE void evtCliDestroyReqAndAhanlde(void *param) {
  if (param == NULL) return;

  STaskArg  *arg = param;
  SCliReq   *pReq = arg->param1;
  SCliThrd2 *pThrd = arg->param2;
  evtCliDestroyReqWrapper(pReq, pThrd);
}

int32_t evtCliMayHandleReleaseResp(SCliConn *conn, STransMsgHead *pHead) {
  int32_t    code = 0;
  SCliThrd2 *pThrd = conn->hostThrd;
  if (pHead->msgType == TDMT_SCH_TASK_RELEASE || pHead->msgType == TDMT_SCH_TASK_RELEASE + 1) {
    int64_t   qId = taosHton64(pHead->qid);
    STraceId *trace = &pHead->traceId;
    int64_t   seqNum = taosHton64(pHead->seqNum);
    tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, seqNum:%" PRId64 ", sid:%" PRId64 "",
            CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pHead->msgType), conn->dst, conn->src, pHead->msgLen, seqNum,
            qId);

    STransCtx *p = taosHashGet(conn->pQTable, &qId, sizeof(qId));
    transCtxCleanup(p);

    code = taosHashRemove(conn->pQTable, &qId, sizeof(qId));
    if (code != 0) {
      tDebug("%s conn %p failed to release req:%" PRId64 " from conn", CONN_GET_INST_LABEL(conn), conn, qId);
    }

    code = taosHashRemove(pThrd->pIdConnTable, &qId, sizeof(qId));
    if (code != 0) {
      tDebug("%s conn %p failed to release req:%" PRId64 " from thrd ", CONN_GET_INST_LABEL(conn), conn, qId);
    }

    tDebug("%s %p reqToSend:%d, sentOut:%d", CONN_GET_INST_LABEL(conn), conn, transQueueSize(&conn->reqsToSend),
           transQueueSize(&conn->reqsSentOut));

    queue set;
    QUEUE_INIT(&set);
    transQueueRemoveByFilter(&conn->reqsSentOut, filterByQid, &qId, &set, -1);
    transQueueRemoveByFilter(&conn->reqsToSend, filterByQid, &qId, &set, -1);

    transReleaseExHandle(transGetRefMgt(), qId);
    transRemoveExHandle(transGetRefMgt(), qId);

    while (!QUEUE_IS_EMPTY(&set)) {
      queue *el = QUEUE_HEAD(&set);
      QUEUE_REMOVE(el);
      SCliReq *pReq = QUEUE_DATA(el, SCliReq, q);
      evtCliRemoveReqFromSendQ(pReq);
      STraceId *trace = &pReq->msg.info.traceId;
      tGDebug("start to free msg %p", pReq);
      evtCliDestroyReqWrapper(pReq, pThrd);
    }
    taosMemoryFree(pHead);
    return 1;
  }
  return 0;
}
int32_t cliHandleStateMayCreateAhandle(SCliConn *conn, STransMsgHead *pHead, STransMsg *pResp) {
  int32_t code = 0;
  int64_t qId = taosHton64(pHead->qid);
  if (qId == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  }

  STransCtx *pCtx = taosHashGet(conn->pQTable, &qId, sizeof(qId));
  if (pCtx == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  }
  pCtx->st = taosGetTimestampUs();
  STraceId *trace = &pHead->traceId;
  pResp->info.ahandle = transCtxDumpVal(pCtx, pHead->msgType);
  tGDebug("%s conn %p %s received from %s, local info:%s, sid:%" PRId64 ", create ahandle %p by %s",
          CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pHead->msgType), conn->dst, conn->src, qId, pResp->info.ahandle,
          TMSG_INFO(pHead->msgType));
  return 0;
}

static FORCE_INLINE void evtCliConnClearInitUserMsg(SCliConn *conn) {
  if (conn->pInitUserReq) {
    taosMemoryFree(conn->pInitUserReq);
    conn->pInitUserReq = NULL;
  }
}
int32_t cliHandleStateMayUpdateStateTime(SCliConn *pConn, SCliReq *pReq) {
  int64_t qid = pReq->msg.info.qId;
  if (qid > 0) {
    STransCtx *pUserCtx = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
    if (pUserCtx != NULL) {
      pUserCtx->st = taosGetTimestampUs();
    }
  }
  return 0;
}

int32_t evtCliMayUpdateStateCtx(SCliConn *pConn, SCliReq *pReq) {
  int32_t    code = 0;
  int64_t    qid = pReq->msg.info.qId;
  SReqCtx   *pCtx = pReq->ctx;
  SCliThrd2 *pThrd = pConn->hostThrd;
  if (pCtx == NULL) {
    tDebug("%s conn %p not need to update statue ctx, sid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
    return 0;
  }

  STransCtx *pUserCtx = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
  if (pUserCtx == NULL) {
    pCtx->userCtx.st = taosGetTimestampUs();
    code = taosHashPut(pConn->pQTable, &qid, sizeof(qid), &pCtx->userCtx, sizeof(pCtx->userCtx));
    tDebug("%s conn %p succ to add statue ctx, sid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
  } else {
    transCtxMerge(pUserCtx, &pCtx->userCtx);
    pUserCtx->st = taosGetTimestampUs();
    tDebug("%s conn %p succ to update statue ctx, sid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
  }
  return 0;
}
int32_t evtCliMayUpdateState(SCliConn *pConn, SCliReq *pReq) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  int32_t    code = 0;
  int64_t    qid = pReq->msg.info.qId;
  if (qid == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  }

  SReqState state = {.conn = pConn, .arg = NULL};
  code = taosHashPut(pThrd->pIdConnTable, &qid, sizeof(qid), &state, sizeof(state));
  if (code != 0) {
    tDebug("%s conn %p failed to statue, sid:%" PRId64 " since %s", transLabel(pThrd->pInst), pConn, qid,
           tstrerror(code));
  } else {
    tDebug("%s conn %p succ to add statue, sid:%" PRId64 " (1)", transLabel(pThrd->pInst), pConn, qid);
  }

  TAOS_UNUSED(evtCliMayUpdateStateCtx(pConn, pReq));
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

  evtCliConnClearInitUserMsg(pConn);

  if (evtCliMayHandleReleaseResp(pConn, pHead)) {
    if (evtCliRecycleConn(pConn)) {
      return code;
    }
    return 0;
  }

  code = cliGetReqBySeq(pConn, seq, pHead->msgType, &pReq);
  if (code == TSDB_CODE_OUT_OF_RANGE) {
    code = cliHandleStateMayCreateAhandle(pConn, pHead, &resp);
    if (code == 0) {
      code = evtCliBuildRespFromCont(NULL, &resp, pHead);
      code = cliNotifyCb(pConn, NULL, &resp);
      return 0;
    }

    if (code != 0) {
      tWarn("%s conn %p recv unexpected packet, msgType:%s, seqNum:%" PRId64 ", sid:%" PRId64
            ", the sever may sends repeated response since %s",
            CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pHead->msgType), seq, qId, tstrerror(code));
      // TODO: notify cb
      taosMemoryFree(pHead);
      if (evtCliRecycleConn(pConn)) {
        return 0;
      }
      return 0;
    }
  } else {
    code = cliHandleStateMayUpdateStateTime(pConn, pReq);
    if (code != 0) {
      tDebug("%s conn %p failed to update state time sid:%" PRId64 " since %s", CONN_GET_INST_LABEL(pConn), pConn, qId,
             tstrerror(code));
    }
  }
  evtCliRemoveReqFromSendQ(pReq);

  code = evtCliBuildRespFromCont(pReq, &resp, pHead);
  STraceId *trace = &resp.info.traceId;
  tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, seq:%" PRId64 ", sid:%" PRId64 ", code:%s",
          CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(resp.msgType), pConn->dst, pConn->src, pHead->msgLen, seq, qId,
          tstrerror(pHead->code));
  code = cliNotifyCb(pConn, pReq, &resp);
  if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
    tGWarn("%s msg need retry", CONN_GET_INST_LABEL(pConn));
  } else {
    evtCliDestroyReq(pReq);
  }

  if (evtCliRecycleConn(pConn)) {
    return 0;
  }
  return code;
}

bool connMayAddUserInfo(SCliConn *pConn, STransMsgHead **ppHead, int32_t *msgLen) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  if (pConn->userInited == 1) {
    return false;
  }
  STransMsgHead *pHead = *ppHead;
  STransMsgHead *tHead = taosMemoryCalloc(1, *msgLen + sizeof(pInst->user));
  if (tHead == NULL) {
    return false;
  }
  memcpy((char *)tHead, (char *)pHead, TRANS_MSG_OVERHEAD);
  memcpy((char *)tHead + TRANS_MSG_OVERHEAD, pInst->user, sizeof(pInst->user));

  memcpy((char *)tHead + TRANS_MSG_OVERHEAD + sizeof(pInst->user), (char *)pHead + TRANS_MSG_OVERHEAD,
         *msgLen - TRANS_MSG_OVERHEAD);

  tHead->withUserInfo = 1;
  *ppHead = tHead;
  *msgLen += sizeof(pInst->user);

  pConn->pInitUserReq = tHead;
  pConn->userInited = 1;
  return true;
}
static int32_t evtCliPreSendReq(void *arg, SEvtBuf *buf, int32_t status) {
  int32_t code = 0, line = 0;
  code = evtBufInit(buf);
  TAOS_CHECK_GOTO(code, &line, _end);

  SFdCbArg  *pArg = arg;
  SCliConn  *pConn = pArg->data;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  int32_t j = 0;
  int32_t batchLimit = 16;
  queue   reqToSend;
  QUEUE_INIT(&reqToSend);
  while (!transQueueEmpty(&pConn->reqsToSend)) {
    queue *el = transQueuePop(&pConn->reqsToSend);
    QUEUE_REMOVE(el);

    SCliReq *pCliMsg = QUEUE_DATA(el, SCliReq, q);
    SReqCtx *pCtx = pCliMsg->ctx;
    pConn->seq++;

    STransMsg *pReq = (STransMsg *)(&pCliMsg->msg);
    if (pReq->pCont == 0) {
      pReq->pCont = (void *)rpcMallocCont(0);
      if (pReq->pCont == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pReq->contLen = 0;
    }
    int32_t        msgLen = transMsgLenFromCont(pReq->contLen);
    STransMsgHead *pHead = transHeadFromCont(pReq->pCont);

    char   *content = pReq->pCont;
    int32_t contLen = pReq->contLen;
    if (connMayAddUserInfo(pConn, &pHead, &msgLen)) {
      content = transContFromHead(pHead);
      contLen = transContLenFromMsg(msgLen);
      // pReq->pCont = (void *)pHead;
    } else {
      if (pConn->userInited == 0) {
        return terrno;
      }
    }
    if (pHead->comp == 0) {
      pHead->noResp = (pReq->info.noResp ? 1 : 0);
      pHead->msgType = pReq->msgType;
      pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
      pHead->traceId = pReq->info.traceId;
      pHead->magicNum = htonl(TRANS_MAGIC_NUM);
      pHead->version = TRANS_VER;
      pHead->compatibilityVer = htonl(pInst->compatibilityVer);
    }
    pHead->timestamp = taosHton64(pCliMsg->st);
    pHead->seqNum = taosHton64(pConn->seq);
    pHead->qid = taosHton64(pReq->info.qId);

    pCliMsg->seq = pConn->seq;
    pCliMsg->sent = 1;

    transQueuePush(&pConn->reqsSentOut, &pCliMsg->q);

    QUEUE_INIT(&pCliMsg->sendQ);
    QUEUE_PUSH(&reqToSend, &pCliMsg->sendQ);

    code = evtBufPush(buf, (char *)pHead, msgLen);
    TAOS_CHECK_GOTO(code, &line, _end);
    j++;

    if (j >= batchLimit) {
      break;
    }
    STraceId *trace = &pReq->info.traceId;
    tGDebug("%s conn %p %s is sent to %s, local info:%s, len:%d, seqNum:%" PRId64 ", sid:%" PRId64 "",
            CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pHead->msgType), pConn->dst, pConn->src, contLen, pConn->seq,
            pReq->info.qId);
  }
  return code;
_end:
  if (code != 0) {
    tError("%s failed to send request at line %d since %s", __func__, line, tstrerror(code));
  }
  return code;
}

static int32_t evtCliSendCb(void *arg, int32_t status) {
  int32_t    code = status;
  SFdCbArg  *pArg = arg;
  SCliConn  *pConn = pArg->data;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  if (code != 0) {
    tError("failed to send request since %s", tstrerror(code));
    return code;
  } else {
    tDebug("%s succ to send out request", pInst->label);
  }
  if (transQueueEmpty(&pConn->reqsToSend)) {
    tDebug("%s conn %p stop write evt on fd:%d", transLabel(pThrd->pInst), pConn, pConn->fd);
    code = evtMgtRemove(pThrd->pEvtMgt, pConn->fd, EVT_WRITE, NULL);
  }

  return code;
}

static int32_t evtCliReadResp(void *arg, SEvtBuf *buf, int32_t bytes) {
  int32_t    code;
  int32_t    line = 0;
  SFdCbArg  *pArg = arg;
  SCliConn  *pConn = pArg->data;
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  if (bytes == 0) {
    tDebug("%s client %p closed", pInst->label, pConn);
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
      memmove(p->buf, p->buf + msgLen, p->len - msgLen);
      p->len -= msgLen;

      code = evtCliHandleResp(pConn, pMsg, msgLen);
      TAOS_CHECK_GOTO(code, &line, _end);
    } else {
      break;
    }
  }
  tDebug("%s conn %p succ to read resp", pInst->label, pConn);
  return code;
_end:
  if (code != 0) {
    tError("%s %s conn %p failed to handle resp at line %d since %s", pInst->label, __func__, pConn, line,
           tstrerror(code));
  }
  return code;
}

static FORCE_INLINE void evtLogConnMissHit(SCliConn *pConn) {
  // queue set;
  // QUEUE_INIT(&set);
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  pConn->heapMissHit++;
  tDebug("conn %p has %d reqs, %d sentout and %d status in process, total limit:%d, switch to other conn", pConn,
         transQueueSize(&pConn->reqsToSend), transQueueSize(&pConn->reqsSentOut), taosHashGetSize(pConn->pQTable),
         pThrd->shareConnLimit);
}
static int32_t getOrCreateHeapCache(SHashObj *pConnHeapCache, char *key, SHeap **pHeap) {
  int32_t code = 0;
  size_t  klen = strlen(key);

  SHeap *p = taosHashGet(pConnHeapCache, key, klen);
  if (p == NULL) {
    SHeap heap = {0};
    code = transHeapInit(&heap, compareHeapNode);
    if (code != 0) {
      tError("failed to init heap cache for key:%s since %s", key, tstrerror(code));
      return code;
    }

    code = taosHashPut(pConnHeapCache, key, klen, &heap, sizeof(heap));
    if (code != 0) {
      transHeapDestroy(&heap);
      tError("failed to put heap to cache for key:%s since %s", key, tstrerror(code));
      return code;
    }
    p = taosHashGet(pConnHeapCache, key, klen);
    if (p == NULL) {
      code = TSDB_CODE_INVALID_PARA;
    }
  }
  *pHeap = p;
  return code;
}
static FORCE_INLINE int8_t shouldSWitchToOtherConn(SCliConn *pConn, char *key) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;

  int32_t reqsNum = transQueueSize(&pConn->reqsToSend);
  int32_t reqsSentOut = transQueueSize(&pConn->reqsSentOut);
  int32_t stateNum = taosHashGetSize(pConn->pQTable);
  int32_t totalReqs = reqsNum + reqsSentOut;

  tDebug("%s conn %p get from heap cache for key:%s, status:%d, refCnt:%d, totalReqs:%d", pInst->label, pConn, key,
         pConn->inHeap, pConn->reqRefCnt, totalReqs);

  if (totalReqs >= pThrd->shareConnLimit) {
    evtLogConnMissHit(pConn);

    if (pConn->list == NULL && pConn->dstAddr != NULL) {
      pConn->list = taosHashGet((SHashObj *)pThrd->pool, pConn->dstAddr, strlen(pConn->dstAddr));
      if (pConn->list != NULL) {
        tTrace("conn %p get list %p from pool for key:%s", pConn, pConn->list, key);
      }
    }
    if (pConn->list && pConn->list->totalSize >= pInst->connLimitNum / 4) {
      tWarn("%s conn %p try to remove timeout msg since too many conn created", transLabel(pInst), pConn);
      pThrd->shareConnLimit = pThrd->shareConnLimit * 2;
      // TODO
      //  if (cliConnRemoveTimeoutMsg(pConn)) {
      //    tWarn("%s conn %p succ to remove timeout msg", transLabel(pInst), pConn);
      //  }
      return 1;
    }
    // check req timeout or not
    return 1;
  }

  return 0;
}
static SCliConn *getConnFromHeapCache(SHashObj *pConnHeap, char *key) {
  int32_t   code = 0;
  SCliConn *pConn = NULL;
  SHeap    *pHeap = NULL;

  code = getOrCreateHeapCache(pConnHeap, key, &pHeap);
  if (code != 0) {
    tTrace("failed to get conn heap from cache for key:%s", key);
    return NULL;
  }

  code = transHeapGet(pHeap, &pConn);
  if (code != 0) {
    tTrace("failed to get conn from heap cache for key:%s", key);
    return NULL;
  } else {
    tTrace("conn %p get conn from heap cache for key:%s", pConn, key);
    if (shouldSWitchToOtherConn(pConn, key)) {
      SCliConn *pNewConn = NULL;
      code = balanceConnHeapCache(pConnHeap, pConn, &pNewConn);
      if (code == 1) {
        tTrace("conn %p start to handle reqs", pNewConn);
        return pNewConn;
      }
      return NULL;
    }
  }

  return pConn;
}
static int32_t addConnToHeapCache(SHashObj *pConnHeapCacahe, SCliConn *pConn) {
  SHeap  *p = NULL;
  int32_t code = 0;

  if (pConn->heap != NULL) {
    p = pConn->heap;
    tTrace("conn %p add to heap cache for key:%s,status:%d, refCnt:%d, add direct", pConn, pConn->dstAddr,
           pConn->inHeap, pConn->reqRefCnt);
  } else {
    code = getOrCreateHeapCache(pConnHeapCacahe, pConn->dstAddr, &p);
    if (code != 0) {
      return code;
    }
    if (pConn->connnected == 0) {
      int64_t now = taosGetTimestampMs();
      if (now - p->lastConnFailTs < 3000) {
        return TSDB_CODE_RPC_NETWORK_UNAVAIL;
      }
    }
  }

  code = transHeapInsert(p, pConn);
  tTrace("conn %p add to heap cache for key:%s,status:%d, refCnt:%d", pConn, pConn->dstAddr, pConn->inHeap,
         pConn->reqRefCnt);
  return code;
}

static int32_t delConnFromHeapCache(SHashObj *pConnHeapCache, SCliConn *pConn) {
  if (pConn->heap != NULL) {
    tTrace("conn %p try to delete from heap cache direct", pConn);
    return transHeapDelete(pConn->heap, pConn);
  }

  SHeap *p = taosHashGet(pConnHeapCache, pConn->dstAddr, strlen(pConn->dstAddr));
  if (p == NULL) {
    tTrace("failed to get heap cache for key:%s, no need to del", pConn->dstAddr);
    return 0;
  }
  int32_t code = transHeapDelete(p, pConn);
  if (code != 0) {
    tTrace("conn %p failed delete from heap cache since %s", pConn, tstrerror(code));
  }
  return code;
}

static int8_t balanceConnHeapCache(SHashObj *pConnHeapCache, SCliConn *pConn, SCliConn **pNewConn) {
  SCliThrd2 *pThrd = pConn->hostThrd;
  STrans    *pInst = pThrd->pInst;
  SCliConn  *pTopConn = NULL;
  if (pConn->heap != NULL && pConn->inHeap != 0) {
    TAOS_UNUSED(transHeapBalance(pConn->heap, pConn));
    if (transHeapGet(pConn->heap, &pTopConn) == 0 && pConn != pTopConn) {
      int32_t curReqs = REQS_ON_CONN(pConn);
      int32_t topReqs = REQS_ON_CONN(pTopConn);
      if (curReqs > topReqs && topReqs < pThrd->shareConnLimit) {
        *pNewConn = pTopConn;
        return 1;
      }
    }
  }
  return 0;
}
FORCE_INLINE int32_t evtCliBuildExceptResp(SCliThrd2 *pThrd, SCliReq *pReq, STransMsg *pResp) {
  if (pReq == NULL) return -1;

  STrans *pInst = pThrd->pInst;

  SReqCtx *pCtx = pReq ? pReq->ctx : NULL;
  pResp->msgType = pReq ? pReq->msg.msgType + 1 : 0;
  pResp->info.cliVer = pInst->compatibilityVer;
  pResp->info.ahandle = pCtx ? pCtx->ahandle : 0;
  if (pReq) {
    pResp->info.traceId = pReq->msg.info.traceId;
  }

  // handle noresp and inter manage msg
  if (pCtx == NULL || pReq->msg.info.noResp == 1) {
    return TSDB_CODE_RPC_NO_STATE;
  }
  if (pResp->code == 0) {
    pResp->code = TSDB_CODE_RPC_BROKEN_LINK;
  }

  return 0;
}
static int32_t evtCliHandleExcept(void *thrd, SCliReq *pReq, STransMsg *pResp) {
  SCliThrd2 *pThrd = thrd;
  STrans    *pInst = pThrd->pInst;
  int32_t    code = evtCliBuildExceptResp(pThrd, pReq, pResp);
  if (code != 0) {
    evtCliDestroyReq(pReq);
    return code;
  }
  pInst->cfp(pInst->parent, pResp, NULL);
  evtCliDestroyReq(pReq);
  return code;
}

static int32_t evtCliMayGetStateByQid(SCliThrd2 *pThrd, SCliReq *pReq, SCliConn **pConn) {
  int32_t code = 0;
  int64_t qid = pReq->msg.info.qId;
  if (qid == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  } else {
    SExHandle *exh = transAcquireExHandle(transGetRefMgt(), qid);
    if (exh == NULL) {
      return TSDB_CODE_RPC_STATE_DROPED;
    }

    SReqState *pState = taosHashGet(pThrd->pIdConnTable, &qid, sizeof(qid));

    if (pState == NULL) {
      if (pReq->ctx == NULL) {
        transReleaseExHandle(transGetRefMgt(), qid);
        return TSDB_CODE_RPC_STATE_DROPED;
      }
      tDebug("%s conn %p failed to get statue, sid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
      transReleaseExHandle(transGetRefMgt(), qid);
      return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
    } else {
      *pConn = pState->conn;
      tDebug("%s conn %p succ to get conn of statue, sid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
    }
    transReleaseExHandle(transGetRefMgt(), qid);
    return 0;
  }
}
static int32_t evtHandleCliReq(SCliThrd2 *pThrd, SCliReq *req) {
  int32_t   lino = 0;
  int32_t   code = 0;
  STransMsg resp = {0};
  STrans   *pInst = pThrd->pInst;

  STraceId *trace = &req->msg.info.traceId;
  SCliConn *pConn = NULL;

  code = evtCliMayGetStateByQid(pThrd, req, &pConn);
  if (code == 0) {
    evtCliMayUpdateStateCtx(pConn, req);
  } else if (code == TSDB_CODE_RPC_STATE_DROPED) {
    TAOS_CHECK_GOTO(code, &lino, _end);
  } else if (code == TSDB_CODE_RPC_NO_STATE || code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
    char   *fqdn = EPSET_GET_INUSE_IP(req->ctx->epSet);
    int32_t port = EPSET_GET_INUSE_PORT(req->ctx->epSet);
    char    addr[TSDB_FQDN_LEN + 64] = {0};
    snprintf(addr, sizeof(addr), "%s:%d", fqdn, port);

    code = evtCliGetOrCreateConn(pThrd, fqdn, port, &pConn);
    if (code != 0) {
      tError("%s failed to create conn since %s", pInst->label, tstrerror(code));
      TAOS_CHECK_GOTO(code, &lino, _end);
    } else {
    }
    evtCliMayUpdateState(pConn, req);
  }

  transQueuePush(&pConn->reqsToSend, &req->q);
  tGDebug("%s conn %p get from pool, src:%s, dst:%s", pInst->label, pConn, pConn->src, pConn->dst);

  SFdCbArg arg = {.evtType = EVT_CONN_T,
                  .arg = pConn,
                  .fd = pConn->fd,
                  .readCb = evtCliReadResp,
                  .sendCb = evtCliPreSendReq,
                  .sendFinishCb = evtCliSendCb,
                  .data = pConn};

  code = evtMgtAdd(pThrd->pEvtMgt, pConn->fd, EVT_READ | EVT_WRITE, &arg);

  return code;
_end:
  resp.code = code;
  if (code != 0) {
    tGError("%s failed to handle req since %s", pInst->label, tstrerror(code));
  }
  evtCliHandleExcept(pThrd, req, &resp);
  return code;
}
static void evtCliHandleAsyncCb(void *arg, int32_t status) {
  int32_t       code = 0;
  SAsyncHandle *handle = arg;
  SEvtMgt      *pEvtMgt = handle->data;

  SCliThrd2 *pThrd = pEvtMgt->hostThrd;
  STrans    *pInst = pThrd->pInst;

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
    if (pReq->type == Quit) {
      transCliAsyncFuncs[pReq->type](pReq, pThrd);
      break;
    }
    transCliAsyncFuncs[pReq->type](pReq, pThrd);
  }
}

static int32_t evtCliTimeoutFunc(void *arg) {
  if (arg == 0) return 0;

  int32_t      code = 0;
  STimeoutArg *pArg = arg;
  transDQHandleTimeout2(pArg->queue);
  return code;
}
static int32_t evtCliCalcTimeout(void *arg, int64_t *timeout) {
  if (arg == 0) return 0;
  STimeoutArg *pArg = arg;
  int32_t      code = 0;

  *timeout = transDQGetNextTimeout2(pArg->queue);
  return code;
}
static void *cliWorkThread2(void *arg) {
  int32_t    line = 0;
  int32_t    code = 0;
  char       threadName[TSDB_LABEL_LEN] = {0};
  SCliThrd2 *pThrd = (SCliThrd2 *)arg;
  STrans    *pInst = pThrd->pInst;

  pThrd->pid = taosGetSelfPthreadId();

  tsEnableRandErr = false;
  TAOS_UNUSED(strtolower(threadName, pThrd->pInst->label));
  setThreadName(threadName);

  code = evtMgtCreate(&pThrd->pEvtMgt, pInst->label);
  TAOS_CHECK_GOTO(code, &line, _end);

  pThrd->pEvtMgt->hostThrd = pThrd;

  code = evtAsyncInit(pThrd->pEvtMgt, pThrd->pipe_queue_fd, &pThrd->asyncHandle, evtCliHandleAsyncCb, EVT_ASYNC_T,
                      (void *)pThrd);
  TAOS_CHECK_GOTO(code, &line, _end);

  createTimeoutArg(pThrd->pEvtMgt, (STimeoutArg **)&pThrd->pEvtMgt->arg);
  TAOS_CHECK_GOTO(code, &line, _end);

  code = evtMgtAddTimoutFunc(pThrd->pEvtMgt, NULL, evtCliTimeoutFunc, evtCliCalcTimeout);

  while (!pThrd->quit) {
    struct timeval tv = {30, 0};
    code = evtCalcNextTimeout(pThrd->pEvtMgt, &tv);
    if (code != 0) {
      tTrace("%s failed to calc next timeout", pInst->label);
    } else {
      tTrace("%s succ to calc next timeout", pInst->label);
    }
    code = evtMgtDispath(pThrd->pEvtMgt, &tv);
    if (code != 0) {
      tError("%s failed to dispatch since %s", pInst->label, tstrerror(code));
      continue;
    } else {
      tTrace("%s succe to dispatch", pInst->label);
    }
  }

  tDebug("%s thread quit-thread:%08" PRId64 "", pInst->label, pThrd->pid);
  return NULL;
_end:
  if (code != 0) {
    tError("%s failed to do work %s", pInst->label, tstrerror(code));
  }
  return NULL;
}
void *transInitClient2(uint32_t ip, uint32_t port, char *label, int numOfThreads, void *fp, void *arg) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SCliObj2 *cli = taosMemoryCalloc(1, sizeof(SCliObj2));
  if (cli == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  STrans *pInst = arg;
  if (pInst->connLimitNum >= 512) {
    pInst->connLimitNum = 512;
  }
  if (pInst->shareConnLimit <= 0) {
    pInst->shareConnLimit = 64;
  }

  memcpy(cli->label, label, TSDB_LABEL_LEN);
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrd2 **)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd2 *));
  if (cli->pThreadObj == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd2 *pThrd = NULL;

    code = evtCliCreateThrdObj(arg, &pThrd);
    TAOS_CHECK_EXIT(code);

    code = evtInitPipe(pThrd->pipe_queue_fd);
    TAOS_CHECK_EXIT(code);

    code = taosThreadCreate(&pThrd->thread, NULL, cliWorkThread2, (void *)(pThrd));
    TAOS_CHECK_EXIT(code);

    pThrd->thrdInited = 1;
    cli->pThreadObj[i] = pThrd;
  }

  return cli;

_exit:
  if (code != 0) {
    tError("%s failed to init client since %s", pInst->label, tstrerror(code));
  }
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
static int32_t transSendQuit(SCliThrd2 *pThrd) {
  if (pThrd->thrdInited == 0) {
    return 0;
  }
  int32_t  code = 0;
  SCliReq *msg = taosMemoryCalloc(1, sizeof(SCliReq));
  if (msg == NULL) {
    return terrno;
  }

  msg->type = Quit;
  if ((code = evtAsyncSend(pThrd->asyncHandle, &msg->q)) != 0) {
    code = (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
    taosMemoryFree(msg);
    return code;
  }

  atomic_store_8(&pThrd->asyncHandle->stop, 1);
  return 0;
}
void transCloseClient2(void *arg) {
  int32_t   code = 0;
  SCliObj2 *cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    code = transSendQuit(cli->pThreadObj[i]);
    if (code != 0) {
      tError("failed to send quit to thread:%d since %s", i, tstrerror(code));
    }

    destroyThrdObj(cli->pThreadObj[i]);
  }
  taosMemoryFree(cli->pThreadObj);
  taosMemoryFree(cli);
  return;
}

static int32_t compareHeapNode(const HeapNode *a, const HeapNode *b) {
  SCliConn *args1 = container_of(a, SCliConn, node);
  SCliConn *args2 = container_of(b, SCliConn, node);

  int32_t totalReq1 = REQS_ON_CONN(args1);
  int32_t totalReq2 = REQS_ON_CONN(args2);
  if (totalReq1 > totalReq2) {
    return 0;
  }
  return 1;
}
static int32_t transHeapInit(SHeap *heap, int32_t (*cmpFunc)(const HeapNode *a, const HeapNode *b)) {
  heap->heap = heapCreate(cmpFunc);
  if (heap->heap == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  heap->cmpFunc = cmpFunc;
  return 0;
}
static void transHeapDestroy(SHeap *heap) {
  if (heap != NULL) {
    heapDestroy(heap->heap);
  }
}

static int32_t transHeapGet(SHeap *heap, SCliConn **p) {
  if (heapSize(heap->heap) == 0) {
    *p = NULL;
    return -1;
  }
  HeapNode *minNode = heapMin(heap->heap);
  if (minNode == NULL) {
    *p = NULL;
    return -1;
  }
  *p = container_of(minNode, SCliConn, node);
  return 0;
}
static int32_t transHeapInsert(SHeap *heap, SCliConn *p) {
  int32_t code = 0;
  p->reqRefCnt++;
  if (p->inHeap == 1) {
    tTrace("failed to insert conn %p since already in heap", p);
    return TSDB_CODE_DUP_KEY;
  }

  heapInsert(heap->heap, &p->node);
  p->inHeap = 1;
  p->lastAddHeapTime = taosGetTimestampMs();
  p->heap = heap;
  return 0;
}
static int32_t transHeapDelete(SHeap *heap, SCliConn *p) {
  if (p->connnected == 0) {
    TAOS_UNUSED(transHeapUpdateFailTs(heap, p));
  }

  if (p->inHeap == 0) {
    tTrace("failed to del conn %p since not in heap", p);
    return 0;
  } else {
    int64_t now = taosGetTimestampMs();
    if (p->forceDelFromHeap == 0 && now - p->lastAddHeapTime < 10000) {
      tTrace("conn %p not added/delete to heap frequently", p);
      return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
    }
  }

  p->inHeap = 0;
  p->reqRefCnt--;
  if (p->reqRefCnt == 0) {
    heapRemove(heap->heap, &p->node);
    tTrace("conn %p delete from heap", p);
  } else if (p->reqRefCnt < 0) {
    tTrace("conn %p has %d reqs, not delete from heap,assert", p, p->reqRefCnt);
  } else {
    tTrace("conn %p has %d reqs, not delete from heap", p, p->reqRefCnt);
  }
  return 0;
}
static int32_t transHeapBalance(SHeap *heap, SCliConn *p) {
  if (p->inHeap == 0 || heap == NULL || heap->heap == NULL) {
    return 0;
  }
  heapRemove(heap->heap, &p->node);
  heapInsert(heap->heap, &p->node);
  return 0;
}
static int32_t transHeapUpdateFailTs(SHeap *heap, SCliConn *p) {
  heap->lastConnFailTs = taosGetTimestampMs();
  return 0;
}
static int32_t transHeapMayBalance(SHeap *heap, SCliConn *p) {
  if (p->inHeap == 0 || heap == NULL || heap->heap == NULL) {
    return 0;
  }
  SCliConn  *topConn = NULL;
  SCliThrd2 *pThrd = p->hostThrd;
  STrans    *pInst = pThrd->pInst;
  int32_t    balanceLimit = pThrd->shareConnLimit >= 4 ? pThrd->shareConnLimit / 2 : 2;

  int32_t code = transHeapGet(heap, &topConn);
  if (code != 0) {
    return code;
  }

  if (topConn == p) return code;

  int32_t reqsOnTop = REQS_ON_CONN(topConn);
  int32_t reqsOnCur = REQS_ON_CONN(p);

  if (reqsOnTop >= balanceLimit && reqsOnCur < balanceLimit) {
    TAOS_UNUSED(transHeapBalance(heap, p));
  }
  return code;
}
static FORCE_INLINE int32_t timeCompare(const HeapNode *a, const HeapNode *b) {
  SDelayTask *arg1 = container_of(a, SDelayTask, node);
  SDelayTask *arg2 = container_of(b, SDelayTask, node);
  if (arg1->execTime > arg2->execTime) {
    return 0;
  } else {
    return 1;
  }
}
int32_t transDQCreate2(void *loop, SDelayQueue **queue) {
  int32_t code = 0;
  Heap   *heap = NULL;

  SDelayQueue *q = NULL;

  heap = heapCreate(timeCompare);
  if (heap == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _return1);
  }

  q = taosMemoryCalloc(1, sizeof(SDelayQueue));
  if (q == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _return1);
  }
  q->heap = heap;
  q->mgt = loop;

  *queue = q;
  return 0;

_return1:
  heapDestroy(heap);
  taosMemoryFree(q);
  return TSDB_CODE_OUT_OF_MEMORY;
}
void transDQDestroy2(SDelayQueue *queue, void (*freeFunc)(void *arg)) {
  int32_t code = 0;
  while (heapSize(queue->heap) > 0) {
    HeapNode *minNode = heapMin(queue->heap);
    if (minNode == NULL) {
      return;
    }
    heapRemove(queue->heap, minNode);

    SDelayTask *task = container_of(minNode, SDelayTask, node);

    STaskArg *arg = task->arg;
    if (freeFunc) freeFunc(arg);
    taosMemoryFree(arg);

    taosMemoryFree(task);
  }
  heapDestroy(queue->heap);
  taosMemoryFree(queue);
  return;
}

int32_t transDQSched2(SDelayQueue *queue, void (*func)(void *arg), void *arg, uint64_t timeoutMs, SDelayTask **pTask) {
  int32_t     code = 0;
  uint64_t    now = taosGetTimestampMs();
  SDelayTask *task = taosMemoryCalloc(1, sizeof(SDelayTask));
  if (task == NULL) {
    return terrno;
  }

  task->func = func;
  task->arg = arg;
  task->execTime = now + timeoutMs;

  heapInsert(queue->heap, &task->node);

  tTrace("timer %p put task into delay queue, timeoutMs:%" PRIu64, queue->mgt, timeoutMs);
  *pTask = task;
  return code;
}
void transDQCancel2(SDelayQueue *queue, SDelayTask *task) {
  if (heapSize(queue->heap) <= 0) {
    taosMemoryFree(task->arg);
    taosMemoryFree(task);
    return;
  }
  heapRemove(queue->heap, &task->node);

  taosMemoryFree(task->arg);
  taosMemoryFree(task);
}

int32_t transDQHandleTimeout2(SDelayQueue *queue) {
  int32_t  code = 0;
  uint64_t now = taosGetTimestampMs();
  while (heapSize(queue->heap) > 0) {
    HeapNode *minNode = heapMin(queue->heap);
    if (minNode == NULL) {
      return 0;
    }
    SDelayTask *task = container_of(minNode, SDelayTask, node);
    if (task->execTime > now) {
      break;
    }
    heapRemove(queue->heap, minNode);
    task->func(task->arg);
    taosMemoryFree(task);
  }
  return code;
}
int32_t transDQGetNextTimeout2(SDelayQueue *queue) {
  if (heapSize(queue->heap) <= 0) {
    return -1;
  }
  HeapNode *minNode = heapMin(queue->heap);
  if (minNode == NULL) {
    return -1;
  }
  SDelayTask *task = container_of(minNode, SDelayTask, node);
  uint64_t    now = taosGetTimestampMs();
  return task->execTime > now ? task->execTime - now : 0;
}
