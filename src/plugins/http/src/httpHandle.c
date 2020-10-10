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

#define _DEFAULT_SOURCE
#include "os.h"
#include "httpInt.h"
#include "httpResp.h"
#include "httpContext.h"
#include "httpHandle.h"

bool httpDecodeRequest(HttpContext* pContext) {
  if (pContext->decodeMethod->decodeFp == NULL) {
    return false;
  }

  return (*pContext->decodeMethod->decodeFp)(pContext);
}

/**
 * Process the request from http pServer
 */
bool httpProcessData(HttpContext* pContext) {
  if (!httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_HANDLING)) {
    httpTrace("context:%p, fd:%d, state:%s not in ready state, stop process request", pContext, pContext->fd,
              httpContextStateStr(pContext->state));
    httpCloseContextByApp(pContext);
    return false;
  }

  // handle Cross-domain request
  if (strcmp(pContext->parser->method, "OPTIONS") == 0) {
    httpTrace("context:%p, fd:%d, process options request", pContext, pContext->fd);
    httpSendOptionResp(pContext, "process options request success");
  } else {
    if (!httpDecodeRequest(pContext)) {
      /*
       * httpCloseContextByApp has been called when parsing the error
       */
      //httpCloseContextByApp(pContext);
    } else {
      httpProcessRequest(pContext);
    }
  }

  return true;
}
