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

#include "tcurl.h"
#include "osMemPool.h"
#include "osThread.h"
#include "osTime.h"
#include "taoserror.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"

#if !defined(WINDOWS)
#include <poll.h>
#endif

void tcurlClose(void* pConn);

#define TCURL_WS_DRAIN_TIMEOUT_MS    100L
#define TCURL_WS_PING_IDLE_MS        30000LL
#define TCURL_WS_PING_TIMEOUT_MS     2000L
#define TCURL_WS_SEND_SKIP_ENSURE_MS 1000LL
#define TCURL_WS_PING_PAYLOAD        "tdnotify"

#define TCURL_CC_FAIL_THRESHOLD     3
#define TCURL_CC_OPEN_BASE_MS       1000
#define TCURL_CC_OPEN_MAX_MS        30000
#define TCURL_CC_MIN_CONNECT_GAP_MS 100

typedef struct {
  SCURL         curl;
  TdThreadMutex lock;
  bool          lockInited;
} STCurlPoolEntry;

static SHashObj*     gConnPool = NULL;
static TdThreadMutex gConnPoolMutex;
static TdThreadOnce  gConnPoolMutexOnce = PTHREAD_ONCE_INIT;

static void tcurlConnPoolMutexInitOnce(void) {
  (void)taosThreadMutexInit(&gConnPoolMutex, NULL);
}

static void tcurlEnsureConnPool(void) {
  (void)taosThreadOnce(&gConnPoolMutexOnce, tcurlConnPoolMutexInitOnce);
  taosThreadMutexLock(&gConnPoolMutex);
  if (gConnPool == NULL) {
    gConnPool = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  }
  taosThreadMutexUnlock(&gConnPoolMutex);
}

static void tcurlPoolEntryFree(void* p) {
  STCurlPoolEntry* entry = (STCurlPoolEntry*)p;
  if (entry == NULL) {
    return;
  }
  if (entry->lockInited) {
    (void)taosThreadMutexLock(&entry->lock);
    tcurlClose(&entry->curl);
    (void)taosThreadMutexUnlock(&entry->lock);
    (void)taosThreadMutexDestroy(&entry->lock);
    entry->lockInited = false;
  } else {
    tcurlClose(&entry->curl);
  }
}

static STCurlPoolEntry* tcurlPoolEntryLookupByCurl(SCURL* scurl) {
  if (scurl == NULL || scurl->url == NULL || gConnPool == NULL) {
    return NULL;
  }
  STCurlPoolEntry* entry = NULL;
  taosThreadMutexLock(&gConnPoolMutex);
  entry = (STCurlPoolEntry*)taosHashGet(gConnPool, (void*)scurl->url, (int32_t)strlen(scurl->url));
  taosThreadMutexUnlock(&gConnPoolMutex);
  if (entry != NULL && &entry->curl == scurl) {
    return entry;
  }
  return NULL;
}

static STCurlPoolEntry* tcurlPoolEntryLookupByUrl(const char* url) {
  if (url == NULL || gConnPool == NULL) {
    return NULL;
  }
  STCurlPoolEntry* entry = NULL;
  taosThreadMutexLock(&gConnPoolMutex);
  entry = (STCurlPoolEntry*)taosHashGet(gConnPool, (void*)url, (int32_t)strlen(url));
  taosThreadMutexUnlock(&gConnPoolMutex);
  return entry;
}

// Per-URL entry lock + CC limit handshake storms on each URL.

typedef enum {
  TCURL_CC_CLOSED = 0,
  TCURL_CC_OPEN,
  TCURL_CC_HALF_OPEN,
} ECurlCcState;

typedef struct {
  ECurlCcState state;
  int32_t      consecutiveFailures;
  int32_t      backoffMs;
  int64_t      openUntilMs;
  int64_t      lastConnectAttemptMs;
  bool         halfOpenInFlight;
} STCurlUrlCongestion;

static SHashObj*     gCcHash = NULL;
static TdThreadMutex gCcMutex;
static TdThreadOnce  gCcMutexOnce = PTHREAD_ONCE_INIT;

static void tcurlCcMutexInitOnce(void) { (void)taosThreadMutexInit(&gCcMutex, NULL); }

static void tcurlCcInit(void) {
  (void)taosThreadOnce(&gCcMutexOnce, tcurlCcMutexInitOnce);
  taosThreadMutexLock(&gCcMutex);
  if (gCcHash == NULL) {
    gCcHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  }
  taosThreadMutexUnlock(&gCcMutex);
}

static STCurlUrlCongestion* tcurlCcGetOrCreate(const char* url) {
  int32_t              urlLen = (int32_t)strlen(url);
  STCurlUrlCongestion* cc = (STCurlUrlCongestion*)taosHashGet(gCcHash, (void*)url, urlLen);
  if (cc != NULL) {
    return cc;
  }

  STCurlUrlCongestion fresh = {0};
  fresh.state = TCURL_CC_CLOSED;
  fresh.backoffMs = TCURL_CC_OPEN_BASE_MS;
  if (taosHashPut(gCcHash, (void*)url, urlLen, &fresh, sizeof(fresh)) != TSDB_CODE_SUCCESS) {
    return NULL;
  }
  return (STCurlUrlCongestion*)taosHashGet(gCcHash, (void*)url, urlLen);
}

static int64_t tcurlCcCalcWaitMs(STCurlUrlCongestion* cc, int64_t now) {
  int64_t waitMs = 0;
  if (cc->state == TCURL_CC_OPEN && now < cc->openUntilMs) {
    waitMs = cc->openUntilMs - now;
  }
  if (cc->lastConnectAttemptMs > 0) {
    int64_t gapWait = TCURL_CC_MIN_CONNECT_GAP_MS - (now - cc->lastConnectAttemptMs);
    if (gapWait > waitMs) {
      waitMs = gapWait;
    }
  }
  if (cc->state == TCURL_CC_HALF_OPEN && cc->halfOpenInFlight) {
    if (waitMs < 50) {
      waitMs = 50;
    }
  }
  return waitMs > 0 ? waitMs : 0;
}

static bool tcurlCcAllowConnect(const char* url, int64_t* pWaitMs) {
  tcurlCcInit();
  if (gCcHash == NULL || url == NULL) {
    if (pWaitMs) {
      *pWaitMs = 0;
    }
    return true;
  }

  int64_t now = taosGetTimestampMs();
  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = tcurlCcGetOrCreate(url);
  if (cc == NULL) {
    taosThreadMutexUnlock(&gCcMutex);
    if (pWaitMs) {
      *pWaitMs = 0;
    }
    return true;
  }

  if (cc->state == TCURL_CC_OPEN && now >= cc->openUntilMs) {
    cc->state = TCURL_CC_HALF_OPEN;
    cc->halfOpenInFlight = false;
  }

  int64_t waitMs = tcurlCcCalcWaitMs(cc, now);
  if (waitMs > 0) {
    taosThreadMutexUnlock(&gCcMutex);
    if (pWaitMs) {
      *pWaitMs = waitMs;
    }
    return false;
  }

  if (cc->state == TCURL_CC_HALF_OPEN) {
    cc->halfOpenInFlight = true;
  }
  cc->lastConnectAttemptMs = now;
  taosThreadMutexUnlock(&gCcMutex);

  if (pWaitMs) {
    *pWaitMs = 0;
  }
  return true;
}

static void tcurlCcOnConnectSuccess(const char* url) {
  if (gCcHash == NULL || url == NULL) {
    return;
  }

  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = tcurlCcGetOrCreate(url);
  if (cc != NULL) {
    cc->state = TCURL_CC_CLOSED;
    cc->consecutiveFailures = 0;
    cc->backoffMs = TCURL_CC_OPEN_BASE_MS;
    cc->openUntilMs = 0;
    cc->halfOpenInFlight = false;
  }
  taosThreadMutexUnlock(&gCcMutex);
}

static void tcurlCcOnConnectFailure(const char* url) {
  if (gCcHash == NULL || url == NULL) {
    return;
  }

  int64_t now = taosGetTimestampMs();
  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = tcurlCcGetOrCreate(url);
  if (cc != NULL) {
    cc->consecutiveFailures++;
    cc->halfOpenInFlight = false;
    if (cc->state == TCURL_CC_HALF_OPEN || cc->consecutiveFailures >= TCURL_CC_FAIL_THRESHOLD) {
      if (cc->backoffMs < TCURL_CC_OPEN_BASE_MS) {
        cc->backoffMs = TCURL_CC_OPEN_BASE_MS;
      } else {
        cc->backoffMs = TMIN(cc->backoffMs * 2, TCURL_CC_OPEN_MAX_MS);
      }
      cc->state = TCURL_CC_OPEN;
      cc->openUntilMs = now + cc->backoffMs;
      uWarn("[curl]circuit open for %s, backoff %d ms, failures:%d", url, cc->backoffMs, cc->consecutiveFailures);
    }
  }
  taosThreadMutexUnlock(&gCcMutex);
}

static void tcurlCcOnTransportFailure(const char* url) {
  if (gCcHash == NULL || url == NULL) {
    return;
  }

  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = tcurlCcGetOrCreate(url);
  if (cc != NULL) {
    cc->consecutiveFailures++;
  }
  taosThreadMutexUnlock(&gCcMutex);
}

bool tcurlUrlCongested(const char* url) {
  if (url == NULL) {
    return false;
  }

  tcurlCcInit();
  if (gCcHash == NULL) {
    return false;
  }

  int64_t now = taosGetTimestampMs();
  bool    congested = false;
  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = (STCurlUrlCongestion*)taosHashGet(gCcHash, (void*)url, (int32_t)strlen(url));
  if (cc != NULL && cc->state == TCURL_CC_OPEN && now < cc->openUntilMs) {
    congested = true;
  }
  taosThreadMutexUnlock(&gCcMutex);
  return congested;
}

int64_t tcurlUrlRetryAfterMs(const char* url) {
  if (url == NULL) {
    return 0;
  }

  tcurlCcInit();
  if (gCcHash == NULL) {
    return 0;
  }

  int64_t now = taosGetTimestampMs();
  int64_t waitMs = 0;
  taosThreadMutexLock(&gCcMutex);
  STCurlUrlCongestion* cc = (STCurlUrlCongestion*)taosHashGet(gCcHash, (void*)url, (int32_t)strlen(url));
  if (cc != NULL) {
    if (cc->state == TCURL_CC_OPEN && now >= cc->openUntilMs) {
      cc->state = TCURL_CC_HALF_OPEN;
      cc->halfOpenInFlight = false;
    }
    waitMs = tcurlCcCalcWaitMs(cc, now);
  }
  taosThreadMutexUnlock(&gCcMutex);
  return waitMs;
}

void tcurlReportUrlFailure(const char* url) { tcurlCcOnTransportFailure(url); }

static int32_t tcurlConnectWithCc(CURL** ppConn, const char* url) {
  int64_t waitMs = 0;
  if (!tcurlCcAllowConnect(url, &waitMs)) {
    uDebug("[curl]connect to %s deferred, congested, wait %" PRId64 " ms", url, waitMs);
    return TSDB_CODE_RPC_NETWORK_BUSY;
  }

  int32_t code = tcurlConnect(ppConn, url);
  if (code == TSDB_CODE_SUCCESS) {
    tcurlCcOnConnectSuccess(url);
  } else {
    tcurlCcOnConnectFailure(url);
  }
  return code;
}

static bool tcurlWsRecvWouldBlock(CURLcode res) { return res == CURLE_AGAIN || res == CURLE_OPERATION_TIMEDOUT; }

static int32_t tcurlReplyPong(SCURL* scurl, const void* payload, size_t payloadLen) {
  size_t   sent = 0;
  CURLcode res = curl_ws_send(scurl->pConn, payload, payloadLen, &sent, 0, CURLWS_PONG);
  if (res != CURLE_OK) {
    uWarn("[curl]failed to reply ws-pong to %s, res:%d", scurl->url, (int)res);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

// Drain pending inbound WS frames (server PING / CLOSE) before sending.
static int32_t tcurlDrainRecv(SCURL* scurl) {
  char                        buf[256];
  size_t                      rlen = 0;
  const struct curl_ws_frame* meta = NULL;
  CURLcode                    res = CURLE_OK;

  (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, TCURL_WS_DRAIN_TIMEOUT_MS);

  while (true) {
    rlen = 0;
    meta = NULL;
    res = curl_ws_recv(scurl->pConn, buf, sizeof(buf), &rlen, &meta);
    if (tcurlWsRecvWouldBlock(res)) {
      break;
    }
    if (res == CURLE_GOT_NOTHING) {
      uWarn("[curl]ws connection closed while draining recv from %s", scurl->url);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (res != CURLE_OK) {
      uWarn("[curl]ws recv drain failed on %s, res:%d", scurl->url, (int)res);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (meta == NULL) {
      continue;
    }
    if (meta->flags & CURLWS_CLOSE) {
      uWarn("[curl]ws close frame received from %s", scurl->url);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (meta->flags & CURLWS_PING) {
      if (tcurlReplyPong(scurl, buf, rlen) != TSDB_CODE_SUCCESS) {
        (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
        return TSDB_CODE_FAILED;
      }
    }
  }

  (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
  return TSDB_CODE_SUCCESS;
}

static int32_t tcurlPingPong(SCURL* scurl) {
  static const char kPayload[] = TCURL_WS_PING_PAYLOAD;
  size_t            sent = 0;
  CURLcode          res = CURLE_OK;
  int64_t           deadline = 0;

  res = curl_ws_send(scurl->pConn, kPayload, sizeof(kPayload) - 1, &sent, 0, CURLWS_PING);
  if (res != CURLE_OK) {
    uWarn("[curl]ws ping to %s failed, res:%d", scurl->url, (int)res);
    return TSDB_CODE_FAILED;
  }

  (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, TCURL_WS_PING_TIMEOUT_MS);
  deadline = taosGetTimestampMs() + TCURL_WS_PING_TIMEOUT_MS;

  while (taosGetTimestampMs() < deadline) {
    char                        buf[256];
    size_t                      rlen = 0;
    const struct curl_ws_frame* meta = NULL;

    res = curl_ws_recv(scurl->pConn, buf, sizeof(buf), &rlen, &meta);
    if (tcurlWsRecvWouldBlock(res)) {
      taosMsleep(1);
      continue;
    }
    if (res == CURLE_GOT_NOTHING) {
      uWarn("[curl]ws connection closed while waiting pong from %s", scurl->url);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (res != CURLE_OK) {
      uWarn("[curl]ws pong recv failed on %s, res:%d", scurl->url, (int)res);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (meta == NULL) {
      continue;
    }
    if (meta->flags & CURLWS_CLOSE) {
      uWarn("[curl]ws close frame received from %s during ping", scurl->url);
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      return TSDB_CODE_FAILED;
    }
    if (meta->flags & CURLWS_PING) {
      if (tcurlReplyPong(scurl, buf, rlen) != TSDB_CODE_SUCCESS) {
        (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
        return TSDB_CODE_FAILED;
      }
      continue;
    }
    if (meta->flags & CURLWS_PONG) {
      (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
      uDebug("[curl]ws ping-pong ok on %s", scurl->url);
      return TSDB_CODE_SUCCESS;
    }
  }

  uWarn("[curl]ws pong timeout on %s", scurl->url);
  (void)curl_easy_setopt(scurl->pConn, CURLOPT_TIMEOUT_MS, 0L);
  return TSDB_CODE_FAILED;
}


#define NOTIFICATION_SEND_TIMEOUT_SECONDS 10L

static int32_t tcurlWaitWritable(CURL* pConn, int64_t deadlineMs) {
  curl_socket_t curlSocket = CURL_SOCKET_BAD;
  CURLcode      res = curl_easy_getinfo(pConn, CURLINFO_ACTIVESOCKET, &curlSocket);
  if (res != CURLE_OK || curlSocket == CURL_SOCKET_BAD) {
    return TSDB_CODE_FAILED;
  }

  while (true) {
    int64_t remainingMs = deadlineMs - taosGetMonoTimestampMs();
    if (remainingMs <= 0) {
      return TSDB_CODE_FAILED;
    }

#if defined(WINDOWS)
    fd_set writable;
    FD_ZERO(&writable);
    FD_SET(curlSocket, &writable);
    struct timeval timeout = {.tv_sec = remainingMs / 1000, .tv_usec = (remainingMs % 1000) * 1000};
    int32_t ready = select(0, NULL, &writable, NULL, &timeout);
#else
    struct pollfd writable = {.fd = curlSocket, .events = POLLOUT};
    int32_t       ready = poll(&writable, 1, (int32_t)remainingMs);
#endif
    if (ready > 0) {
#if defined(WINDOWS)
      return TSDB_CODE_SUCCESS;
#else
      return (writable.revents & POLLOUT) != 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
#endif
    }
    if (ready == 0) {
      return TSDB_CODE_FAILED;
    }
#if defined(WINDOWS)
    if (WSAGetLastError() != WSAEINTR) {
#else
    if (errno != EINTR) {
#endif
      return TSDB_CODE_FAILED;
    }
  }
}

int32_t tcurlConnect(CURL** ppConn, const char* url) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  CURLcode res = CURLE_OK;

  CURL* pConn = curl_easy_init();
  TSDB_CHECK_NULL(pConn, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_URL, url);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_SSL_VERIFYPEER, 0L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_SSL_VERIFYHOST, 0L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_TIMEOUT, NOTIFICATION_SEND_TIMEOUT_SECONDS);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_CONNECTTIMEOUT, 3L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_TCP_KEEPALIVE, 1L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_TCP_KEEPIDLE, 120L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_TCP_KEEPINTVL, 60L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_FORBID_REUSE, 0L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_LOW_SPEED_TIME, 300L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_LOW_SPEED_LIMIT, 1L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_NOSIGNAL, 1L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  res = curl_easy_setopt(pConn, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_setopt(pConn, CURLOPT_CONNECT_ONLY, 2L);
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  res = curl_easy_perform(pConn);
  if (res != CURLE_OK) {
    uError("[curl]%s ws connect failed for %s, curl:%d %s", __func__, url, (int)res, curl_easy_strerror(res));
    code = TSDB_CODE_FAILED;
    lino = __LINE__;
    goto _end;
  }

_end:

  if (code != TSDB_CODE_SUCCESS) {
    uError("[curl]%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (pConn != NULL) {
      curl_easy_cleanup(pConn);
      pConn = NULL;
    }
  }
  *ppConn = pConn;
  return code;
}

void tcurlClose(void* pConn) {
  SCURL* pCurl = (SCURL*)pConn;
  if (pCurl == NULL) {
    return;
  }

  // status code 1000 means normal closure
  if (pCurl->pConn) {
    size_t   len = 0;
    uint16_t status = htons(1000);
    CURLcode res = curl_ws_send(pCurl->pConn, &status, sizeof(status), &len, 0, CURLWS_CLOSE);
    if (res != CURLE_OK) {
      uWarn("[curl]failed to send ws-close msg, pConn:%p, code:%d", pCurl, res);
    }
    curl_easy_cleanup(pCurl->pConn);
    pCurl->pConn = NULL;
  }
  taosMemoryFreeClear(pCurl->url);
}

int32_t tcurlGetConnection(const char* url, SCURL** ppConn) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (url == NULL || ppConn == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  tcurlEnsureConnPool();
  if (gConnPool == NULL) {
    uError("[curl]failed to init global ws connection pool");
    return TSDB_CODE_FAILED;
  }

  int32_t          urlLen = (int32_t)strlen(url);
  STCurlPoolEntry* entry = NULL;

  taosThreadMutexLock(&gConnPoolMutex);
  entry = (STCurlPoolEntry*)taosHashGet(gConnPool, (void*)url, urlLen);
  if (entry == NULL) {
    STCurlPoolEntry fresh = {0};
    fresh.curl.url = taosStrdup(url);
    if (fresh.curl.url == NULL) {
      taosThreadMutexUnlock(&gConnPoolMutex);
      return terrno;
    }
    if (taosThreadMutexInit(&fresh.lock, NULL) != 0) {
      taosMemoryFree(fresh.curl.url);
      taosThreadMutexUnlock(&gConnPoolMutex);
      return TSDB_CODE_FAILED;
    }
    fresh.lockInited = true;
    code = taosHashPut(gConnPool, (void*)url, urlLen, &fresh, sizeof(fresh));
    if (code != TSDB_CODE_SUCCESS) {
      taosThreadMutexDestroy(&fresh.lock);
      taosMemoryFree(fresh.curl.url);
      taosThreadMutexUnlock(&gConnPoolMutex);
      return code;
    }
    entry = (STCurlPoolEntry*)taosHashGet(gConnPool, (void*)url, urlLen);
  }
  taosThreadMutexUnlock(&gConnPoolMutex);

  if (entry == NULL) {
    return TSDB_CODE_FAILED;
  }

  taosThreadMutexLock(&entry->lock);
  if (entry->curl.pConn == NULL) {
    code = tcurlConnectWithCc(&entry->curl.pConn, url);
    if (code == TSDB_CODE_SUCCESS) {
      entry->curl.lastActiveMs = taosGetTimestampMs();
    }
  }
  if (code == TSDB_CODE_SUCCESS && entry->curl.pConn != NULL) {
    *ppConn = &entry->curl;
  } else if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_FAILED;
  }
  taosThreadMutexUnlock(&entry->lock);

  if (code != TSDB_CODE_SUCCESS) {
    uError("[curl]get connection failed for %s, code:0x%x", url, code);
  }
  return code;
}

void tcurlRemoveConnection(const char* url) {
  STCurlPoolEntry* entry = tcurlPoolEntryLookupByUrl(url);
  if (entry == NULL || !entry->lockInited) {
    return;
  }

  taosThreadMutexLock(&entry->lock);
  if (entry->curl.pConn != NULL) {
    curl_easy_cleanup(entry->curl.pConn);
    entry->curl.pConn = NULL;
  }
  taosThreadMutexUnlock(&entry->lock);
}

static int32_t tcurlResetConnectionLocked(SCURL* pConn) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pConn == NULL || pConn->url == NULL) {
    return TSDB_CODE_FAILED;
  }

  if (pConn->pConn != NULL) {
    curl_easy_cleanup(pConn->pConn);
    pConn->pConn = NULL;
  }

  code = tcurlConnectWithCc(&pConn->pConn, pConn->url);
  if (code != TSDB_CODE_SUCCESS) {
    uError("[curl]failed to reconnect to %s", pConn->url);
  } else {
    pConn->lastActiveMs = taosGetTimestampMs();
  }
  return code;
}

int32_t tcurlResetConnection(SCURL* pConn) {
  STCurlPoolEntry* entry = tcurlPoolEntryLookupByCurl(pConn);
  if (entry == NULL) {
    return tcurlResetConnectionLocked(pConn);
  }

  taosThreadMutexLock(&entry->lock);
  int32_t code = tcurlResetConnectionLocked(pConn);
  taosThreadMutexUnlock(&entry->lock);
  return code;
}

static int32_t tcurlEnsureAliveLocked(SCURL* scurl) {
  if (scurl == NULL || scurl->pConn == NULL) {
    return TSDB_CODE_FAILED;
  }

  int64_t now = taosGetTimestampMs();
  if (scurl->lastActiveMs > 0 && (now - scurl->lastActiveMs) < TCURL_WS_SEND_SKIP_ENSURE_MS) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tcurlDrainRecv(scurl);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (scurl->lastActiveMs > 0 && (now - scurl->lastActiveMs) < TCURL_WS_PING_IDLE_MS) {
    return TSDB_CODE_SUCCESS;
  }

  code = tcurlPingPong(scurl);
  if (code == TSDB_CODE_SUCCESS) {
    scurl->lastActiveMs = now;
  }
  return code;
}

int32_t tcurlEnsureAlive(SCURL* scurl) {
  STCurlPoolEntry* entry = tcurlPoolEntryLookupByCurl(scurl);
  if (entry == NULL) {
    return tcurlEnsureAliveLocked(scurl);
  }

  taosThreadMutexLock(&entry->lock);
  int32_t code = tcurlEnsureAliveLocked(scurl);
  taosThreadMutexUnlock(&entry->lock);
  return code;
}

static int32_t tcurlSendLocked(SCURL* scurl, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                               unsigned int flags) {
  if (scurl == NULL) {
    return TSDB_CODE_FAILED;
  }
  if (scurl->pConn == NULL) {
    int32_t code = tcurlResetConnectionLocked(scurl);
    if (code != TSDB_CODE_SUCCESS || scurl->pConn == NULL) {
      return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_FAILED;
    }
  }
  CURLcode res = curl_ws_send(scurl->pConn, buffer, buflen, sent, fragsize, flags);
  if (res != CURLE_OK) {
    // Only the caller knows the complete message boundary. Reconnecting here
    // could resume a fragmented message on a new WebSocket connection.
    uError("[curl]send to:%s failed, res:%d", scurl->url, res);
    tcurlReportUrlFailure(scurl->url);
    return TSDB_CODE_FAILED;
  }
  if (buflen > 0 && sent != NULL && *sent == 0) {
    uError("[curl]send to:%s made no progress (sent=0, fragsize:%" CURL_FORMAT_CURL_OFF_T ")", scurl->url, fragsize);
    tcurlReportUrlFailure(scurl->url);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tcurlSendFrameLocked(SCURL* scurl, const char* buffer, size_t frameLen, unsigned int flags) {
  size_t frameSentLen = 0;
  while (frameSentLen < frameLen) {
    size_t nbytes = 0;
    int32_t code = tcurlSendLocked(scurl, buffer + frameSentLen, frameLen - frameSentLen, &nbytes,
                                   frameSentLen == 0 ? (curl_off_t)frameLen : 0, flags);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (nbytes == 0) {
      uError("[curl]send frame to:%s stalled at offset %zu/%zu", scurl->url, frameSentLen, frameLen);
      tcurlReportUrlFailure(scurl->url);
      return TSDB_CODE_FAILED;
    }
    frameSentLen += nbytes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tcurlSendMessageLocked(SCURL* scurl, const void* buffer, size_t buflen, size_t frameSize) {
  if (buflen == 0) {
    return TSDB_CODE_SUCCESS;
  }
  if (frameSize == 0) {
    frameSize = buflen;
  }

  size_t sentLen = 0;
  while (sentLen < buflen) {
    size_t frameLen = TMIN(frameSize, buflen - sentLen);
    unsigned int flags = CURLWS_TEXT | CURLWS_OFFSET;
    if (sentLen + frameLen < buflen) {
      flags |= CURLWS_CONT;
    }

    int32_t code = tcurlSendFrameLocked(scurl, (const char*)buffer + sentLen, frameLen, flags);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    sentLen += frameLen;
  }
  scurl->lastActiveMs = taosGetTimestampMs();
  return TSDB_CODE_SUCCESS;
}

int32_t tcurlSendMessage(SCURL* scurl, const void* buffer, size_t buflen, size_t frameSize) {
  STCurlPoolEntry* entry = tcurlPoolEntryLookupByCurl(scurl);
  if (entry == NULL) {
    return tcurlSendMessageLocked(scurl, buffer, buflen, frameSize);
  }

  taosThreadMutexLock(&entry->lock);
  int32_t code = tcurlSendMessageLocked(scurl, buffer, buflen, frameSize);
  taosThreadMutexUnlock(&entry->lock);
  return code;
}


int32_t tcurlSend(SCURL* scurl, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                  unsigned int flags, int64_t sendStartMonoMs) {
  int64_t  deadlineMs = sendStartMonoMs + NOTIFICATION_SEND_TIMEOUT_SECONDS * 1000;
  CURLcode res = CURLE_OK;

  while (true) {
    res = curl_ws_send(scurl->pConn, buffer, buflen, sent, fragsize, flags);
    if (res == CURLE_OK || (res == CURLE_AGAIN && *sent > 0)) {
      return TSDB_CODE_SUCCESS;
    }

    if (res != CURLE_AGAIN || tcurlWaitWritable(scurl->pConn, deadlineMs) != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  uError("[curl]send to:%s failed, res:%d", scurl->url, res);
  return TSDB_CODE_FAILED;
}

void closeThreadNotificationConn() {
  // Global pool: connections outlive individual worker threads.
}

void tcurlGlobalPoolShutdown(void) {
  (void)taosThreadOnce(&gConnPoolMutexOnce, tcurlConnPoolMutexInitOnce);
  taosThreadMutexLock(&gConnPoolMutex);
  if (gConnPool != NULL) {
    taosHashSetFreeFp(gConnPool, tcurlPoolEntryFree);
    taosHashCleanup(gConnPool);
    gConnPool = NULL;
  }
  taosThreadMutexUnlock(&gConnPoolMutex);

  (void)taosThreadOnce(&gCcMutexOnce, tcurlCcMutexInitOnce);
  taosThreadMutexLock(&gCcMutex);
  if (gCcHash != NULL) {
    taosHashCleanup(gCcHash);
    gCcHash = NULL;
  }
  taosThreadMutexUnlock(&gCcMutex);
}
