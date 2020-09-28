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

#ifndef TDENGINE_HTTP_RESP_H
#define TDENGINE_HTTP_RESP_H

#include "httpInt.h"

enum _httpRespTempl {
  HTTP_RESPONSE_JSON_OK,
  HTTP_RESPONSE_JSON_ERROR,
  HTTP_RESPONSE_OK,
  HTTP_RESPONSE_ERROR,
  HTTP_RESPONSE_CHUNKED_UN_COMPRESS,
  HTTP_RESPONSE_CHUNKED_COMPRESS,
  HTTP_RESPONSE_OPTIONS,
  HTTP_RESPONSE_GRAFANA,
  HTTP_RESP_END
};

extern const char *httpRespTemplate[];

void httpSendErrorResp(HttpContext *pContext, int32_t errNo);
void httpSendTaosdInvalidSqlErrorResp(HttpContext *pContext, char* errMsg);
void httpSendSuccResp(HttpContext *pContext, char *desc);
void httpSendOptionResp(HttpContext *pContext, char *desc);

#endif