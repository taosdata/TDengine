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

#ifndef _TD_UTIL_TCURL_H_
#define _TD_UTIL_TCURL_H_

#include "os.h"
#include "taos.h"

#if defined(WINDOWS)
#define CURL_STATICLIB
#endif

#include "curl/curl.h"

typedef struct SCurl {
  CURL*   pConn;
  char*   url;
  int64_t lastActiveMs;  // last successful send or ping-pong (epoch ms)
} SCURL;

int32_t tcurlConnect(CURL** ppConn, const char* url);
void    tcurlClose(void* pConn);
int32_t tcurlGetConnection(const char* url, SCURL** pConn);
void    tcurlRemoveConnection(const char* url);
int32_t tcurlSend(SCURL* curl, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                   unsigned int flags);
int32_t tcurlSendMessage(SCURL* curl, const void* buffer, size_t buflen);
int32_t tcurlEnsureAlive(SCURL* scurl);

// Per-URL WS congestion control (circuit breaker + connect rate limit).
bool    tcurlUrlCongested(const char* url);
int64_t tcurlUrlRetryAfterMs(const char* url);
void    tcurlReportUrlFailure(const char* url);

void tcurlGlobalPoolShutdown(void);
void closeThreadNotificationConn();

#endif
