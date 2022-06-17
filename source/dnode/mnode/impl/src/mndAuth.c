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
#include "mndAuth.h"
#include "mndUser.h"

static int32_t mndProcessAuthReq(SRpcMsg *pReq);

int32_t mndInitAuth(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH, mndProcessAuthReq);
  return 0;
}

void mndCleanupAuth(SMnode *pMnode) {}

static int32_t mndRetriveAuth(SMnode *pMnode, SAuthRsp *pRsp) {
  SUserObj *pUser = mndAcquireUser(pMnode, pRsp->user);
  if (pUser == NULL) {
    *pRsp->secret = 0;
    mError("user:%s, failed to auth user since %s", pRsp->user, terrstr());
    return -1;
  }

  pRsp->spi = 1;
  pRsp->encrypt = 0;
  *pRsp->ckey = 0;

  memcpy(pRsp->secret, pUser->pass, TSDB_PASSWORD_LEN);
  mndReleaseUser(pMnode, pUser);

  mDebug("user:%s, auth info is returned", pRsp->user);
  return 0;
}

static int32_t mndProcessAuthReq(SRpcMsg *pReq) {
  SAuthReq authReq = {0};
  if (tDeserializeSAuthReq(pReq->pCont, pReq->contLen, &authReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SAuthReq authRsp = {0};
  memcpy(authRsp.user, authReq.user, TSDB_USER_LEN);

  int32_t code = mndRetriveAuth(pReq->info.node, &authRsp);
  mTrace("user:%s, auth req received, spi:%d encrypt:%d ruser:%s", pReq->info.conn.user, authRsp.spi, authRsp.encrypt,
         authRsp.user);

  int32_t contLen = tSerializeSAuthReq(NULL, 0, &authRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSAuthReq(pRsp, contLen, &authRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  return code;
}

int32_t mndCheckOperAuth(SMnode *pMnode, const char *user, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckAlterUserAuth(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter) {
  if (pOperUser->superUser) return 0;
  if (!pOperUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    return -1;
  }

  if (pAlter->alterType == TSDB_ALTER_USER_PASSWD) {
    if (strcmp(pUser->user, pOperUser->user) == 0) {
      if (pOperUser->sysInfo) return 0;
    }
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckShowAuth(SMnode *pMnode, const char *user, int32_t showType) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
  }

  if (!pUser->sysInfo) {
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    code = -1;
    goto _OVER;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckDbAuth(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
  }

  if (operType == MND_OPER_CREATE_DB) {
    if (pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_ALTER_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_DROP_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_COMPACT_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_USE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_WRITE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_READ_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}
