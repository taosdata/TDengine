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

#include "mndTrans.h"
#include "mndUser.h"
#include "mndToken.h"

#define XNODE_DEF_TOKEN_NAME "__xnode__"

int32_t mndXnodeCreateDefaultToken(SRpcMsg* pReq, char** ppToken) {
  int32_t   code = 0, lino = 0;
  SMnode*   pMnode = pReq->info.node;
  void* oldCont = pReq->pCont;
  int32_t oldContLen = pReq->contLen;

  // generate default token
  SCreateTokenReq createReq = {0};
  (void)memcpy(createReq.user, pReq->info.conn.user, sizeof(createReq.user));
  tstrncpy(createReq.name, XNODE_DEF_TOKEN_NAME, sizeof(createReq.name));
  createReq.enable = 1;

  pReq->contLen = tSerializeSCreateTokenReq(NULL, 0, &createReq);
  pReq->pCont = taosMemoryMalloc(pReq->contLen);
  if (-1 == tSerializeSCreateTokenReq(pReq->pCont, pReq->contLen, &createReq)) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  code = mndProcessCreateTokenReq(pReq);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS && code != TSDB_CODE_MND_TOKEN_ALREADY_EXIST) {
    goto _OVER;
  }

  taosMsleep(500);
  STokenObj* tokenObj = NULL;
  int32_t tmpcode = mndAcquireToken(pMnode, XNODE_DEF_TOKEN_NAME, &tokenObj);
  if (tokenObj == NULL) {
    code = tmpcode;
    goto _OVER;
  }
  *ppToken = tstrndup(tokenObj->token, sizeof(tokenObj->token));
  if (*ppToken == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

_OVER:
  taosMemoryFreeClear(pReq->pCont);
  pReq->pCont = oldCont;
  pReq->contLen = oldContLen;

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS && code != TSDB_CODE_MND_TOKEN_ALREADY_EXIST) {
    mError("xnode failed create default token, user %s, err:%s", pReq->info.conn.user, tstrerror(code));
  }
  if (tokenObj != NULL) {
    mndReleaseToken(pMnode, tokenObj);
  }
  TAOS_RETURN(code);
}