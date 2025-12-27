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

#ifndef WINDOWS
#include "curl/curl.h"

typedef struct SCurl {
  CURL* pConn;
  char* url;
} SCURL;

int32_t tcurlConnect(CURL** ppConn, const char* url);
void    tcurlClose(void* pConn);
int32_t tcurlGetConnection(const char* url, SCURL** pConn);
int32_t tcurlSend(SCURL* curl, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                   unsigned int flags);

#endif

void closeThreadNotificationConn();

#endif
