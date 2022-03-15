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

static int32_t mndProcessAuthReq(SNodeMsg *pReq);

int32_t mndInitAuth(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH, mndProcessAuthReq);
  return 0;
}

void mndCleanupAuth(SMnode *pMnode) {}

int32_t mndRetriveAuth(SMnode *pMnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SUserObj *pUser = mndAcquireUser(pMnode, user);
  if (pUser == NULL) {
    *secret = 0;
    mError("user:%s, failed to auth user since %s", user, terrstr());
    return -1;
  }

  *spi = 1;
  *encrypt = 0;
  *ckey = 0;

  memcpy(secret, pUser->pass, TSDB_PASSWORD_LEN);
  mndReleaseUser(pMnode, pUser);

  mDebug("user:%s, auth info is returned", user);
  return 0;
}

static int32_t mndProcessAuthReq(SNodeMsg *pReq) {
  SAuthReq authReq = {0};
  if (tDeserializeSAuthReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &authReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SAuthReq authRsp = {0};
  memcpy(authRsp.user, authReq.user, TSDB_USER_LEN);

  int32_t code =
      mndRetriveAuth(pReq->pNode, authRsp.user, &authRsp.spi, &authRsp.encrypt, authRsp.secret, authRsp.ckey);
  mTrace("user:%s, auth req received, spi:%d encrypt:%d ruser:%s", pReq->user, authRsp.spi, authRsp.encrypt,
         authRsp.user);

  int32_t contLen = tSerializeSAuthReq(NULL, 0, &authRsp);
  void   *pRsp = rpcMallocCont(contLen);
  tSerializeSAuthReq(pRsp, contLen, &authRsp);
  pReq->pRsp = pRsp;
  pReq->rspLen = contLen;
  return code;
}

int32_t mndCheckCreateUserAuth(SUserObj *pOperUser) {
  if (pOperUser->superUser) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckAlterUserAuth(SUserObj *pOperUser, SUserObj *pUser, SDbObj *pDb, SAlterUserReq *pAlter) {
  if (pAlter->alterType == TSDB_ALTER_USER_PASSWD) {
    if (pOperUser->superUser || strcmp(pUser->user, pOperUser->user) == 0) {
      return 0;
    }
  }

  if (pAlter->alterType == TSDB_ALTER_USER_SUPERUSER) {
    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
      terrno = TSDB_CODE_MND_NO_RIGHTS;
      return -1;
    }

    if (pOperUser->superUser) {
      return 0;
    }
  }

    if (pAlter->alterType == TSDB_ALTER_USER_CLEAR_WRITE_DB || pAlter->alterType == TSDB_ALTER_USER_CLEAR_READ_DB) {
      if (pOperUser->superUser) {
        return 0;
      }
    }

  if (pAlter->alterType == TSDB_ALTER_USER_ADD_READ_DB || pAlter->alterType == TSDB_ALTER_USER_REMOVE_READ_DB ||
      pAlter->alterType == TSDB_ALTER_USER_ADD_WRITE_DB || pAlter->alterType == TSDB_ALTER_USER_REMOVE_WRITE_DB) {
    if (pOperUser->superUser || strcmp(pUser->user, pDb->createUser) == 0) {
      return 0;
    }
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckDropUserAuth(SUserObj *pOperUser) {
  if (pOperUser->superUser) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckNodeAuth(SUserObj *pOperUser) {
  if (pOperUser->superUser) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckFuncAuth(SUserObj *pOperUser) {
  if (pOperUser->superUser) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckCreateDbAuth(SUserObj *pOperUser) { return 0; }

int32_t mndCheckAlterDropCompactSyncDbAuth(SUserObj *pOperUser, SDbObj *pDb) {
  if (pOperUser->superUser || strcmp(pOperUser->user, pDb->createUser) == 0) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckUseDbAuth(SUserObj *pOperUser, SDbObj *pDb) { return 0; }

int32_t mndCheckWriteAuth(SUserObj *pOperUser, SDbObj *pDb) {
  if (pOperUser->superUser || strcmp(pOperUser->user, pDb->createUser) == 0) {
    return 0;
  }

  if (taosHashGet(pOperUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckReadAuth(SUserObj *pOperUser, SDbObj *pDb) {
  if (pOperUser->superUser || strcmp(pOperUser->user, pDb->createUser) == 0) {
    return 0;
  }

  if (taosHashGet(pOperUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) {
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}