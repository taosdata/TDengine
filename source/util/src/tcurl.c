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


#include "osMemPool.h"
#include "taoserror.h"
#include "thash.h"
#include "tlog.h"
#include "tcurl.h"
#include "tutil.h"

#ifndef WINDOWS

#include "curl/curl.h"

static threadlocal SHashObj* tNotificationConnHash = NULL;  // key: url, value: CURL*
static threadlocal bool      tInitialized = false;

static int32_t tcurlConnect(CURL** ppConn, const char* url) {
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

  res = curl_easy_setopt(pConn, CURLOPT_TIMEOUT, 0L);  
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
  TSDB_CHECK_CONDITION(res == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

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

static void tcurlClose(void* pConn) {
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
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SCURL curlInfo = {0};
  if (!tInitialized) {
    tNotificationConnHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (tNotificationConnHash == NULL) {
      uError("[curl]failed to init NotificationConnHash");
      return terrno;
    }
    tInitialized = true;
    taosHashSetFreeFp(tNotificationConnHash, tcurlClose);
  }

  SCURL* pCurlInfo = (SCURL*)taosHashGet(tNotificationConnHash, (void*)url, strlen(url));
  if (pCurlInfo == NULL) {
    curlInfo.url = taosStrdup(url);
    TSDB_CHECK_NULL(curlInfo.url, code, lino, _end, terrno);

    code = tcurlConnect(&curlInfo.pConn, url);
    TSDB_CHECK_CODE(code, lino, _end);

    code = taosHashPut(tNotificationConnHash, (void*)url, strlen(url), &curlInfo, sizeof(SCURL));
    TSDB_CHECK_CODE(code, lino, _end);

    *ppConn = (SCURL*)taosHashGet(tNotificationConnHash, (void*)url, strlen(url));
    if (*ppConn == NULL) {
      uError("[curl]failed to get connection after put");
      return terrno;
    }
  } else {
    *ppConn = pCurlInfo;
    return TSDB_CODE_SUCCESS;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    uError("[curl]get connection failed, lino:%d, code:0x%0x", lino, code);
    if (curlInfo.pConn != NULL) {
      curl_easy_cleanup(curlInfo.pConn);
    }
    if (curlInfo.url != NULL) {
      taosMemoryFree(curlInfo.url);
      curlInfo.url = NULL;
    }
  }
  return code;
}

int32_t tcurlResetConnection(SCURL* pConn) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  curl_easy_cleanup(pConn->pConn);
  pConn->pConn = NULL;

  SCURL* pTmp = (SCURL*)taosHashGet(tNotificationConnHash, (void*)pConn->url, strlen(pConn->url));
  if (pTmp == NULL) {
    uError("[curl]failed to get connection when reset");
    code = TSDB_CODE_FAILED;
  } else {
    code = tcurlConnect(&pTmp->pConn, pTmp->url);
    if (code != TSDB_CODE_SUCCESS) {
      uError("[curl]failed to reconnect to %s", pTmp->url);
      int32_t ret = taosHashRemove(tNotificationConnHash, (void*)pConn->url, strlen(pConn->url));
    }
  }

  return code;
}

int32_t tcurlSend(SCURL* scurl, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                   unsigned int flags) {
  CURLcode res;
  res = curl_ws_send(scurl->pConn, buffer, buflen, sent, fragsize, flags);
  if (res != CURLE_OK) {
    uDebug("[curl]send to:%s failed, res:%d, start retry", scurl->url, res);
    int32_t code = tcurlResetConnection(scurl);
    if (code) {
      return code;
    }
    res = curl_ws_send(scurl->pConn, buffer, buflen, sent, fragsize, flags);
    if (res != CURLE_OK) {
      uError("[curl]send failed, res:%d", res);
      return TSDB_CODE_FAILED;
    }
    uDebug("[curl]send to: %s retry success.", scurl->url);
  }
  return TSDB_CODE_SUCCESS;
}

void closeThreadNotificationConn() {
  if (!tInitialized) {
    return;
  }

  taosHashCleanup(tNotificationConnHash);
  tNotificationConnHash = NULL;
  tInitialized = false;
}

#else
void closeThreadNotificationConn() {
  // no-op on Windows
}

#endif
