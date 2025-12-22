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

#include "audit.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndToken.h"

#define TOKEN_VER_NUMBER   1
#define TOKEN_RESERVE_SIZE 64


typedef struct {
  SHashObj *tokens; // key: token name, value: SCachedTokenInfo*
  TdThreadRwlock rw;
} STokenCache;

static STokenCache tokenCache;


static int32_t tokenCacheInit() {
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  SHashObj *tokens = taosHashInit(128, hashFn, 1, HASH_ENTRY_LOCK);
  if (tokens == NULL) {
    TAOS_RETURN(terrno);
  }

  tokenCache.tokens = tokens;
  (void)taosThreadRwlockInit(&tokenCache.rw, NULL);
  TAOS_RETURN(0);
}



static void tokenCacheCleanup() {
  if (tokenCache.tokens == NULL) {
    return;
  }

  void *pIter = taosHashIterate(tokenCache.tokens, NULL);
  while (pIter) {
    SCachedTokenInfo *ti = *(SCachedTokenInfo **)pIter;
    if (ti != NULL) {
      taosMemoryFree(ti);
    }
    pIter = taosHashIterate(tokenCache.tokens, pIter);
  }
  taosHashCleanup(tokenCache.tokens);
  (void)taosThreadRwlockDestroy(&tokenCache.rw);
}



static int32_t tokenCacheAdd(const STokenObj* token) {
  SCachedTokenInfo* ti = taosMemoryCalloc(1, sizeof(SCachedTokenInfo));
  if (ti == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  tstrncpy(ti->name, token->name, sizeof(ti->name));
  tstrncpy(ti->user, token->user, sizeof(ti->user));
  ti->expireTime = token->expireTime;
  ti->enabled = token->enabled;

  (void)taosThreadRwlockWrlock(&tokenCache.rw);
  int32_t code = taosHashPut(tokenCache.tokens, token->token, sizeof(token->token) - 1, &ti, sizeof(ti));
  taosThreadRwlockUnlock(&tokenCache.rw);

  if (code != 0) {
    taosMemoryFree(ti);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(0);
}



static int32_t tokenCacheUpdate(const STokenObj* token) {
  SCachedTokenInfo* ti = NULL;

  (void)taosThreadRwlockWrlock(&tokenCache.rw);
  SCachedTokenInfo** pp = taosHashGet(tokenCache.tokens, token->token, sizeof(token->token) - 1);
  if (pp != NULL && *pp != NULL) {
    ti = *pp;
  } else {
    ti = taosMemoryCalloc(1, sizeof(SCachedTokenInfo));
    if (ti == NULL) {
      taosThreadRwlockUnlock(&tokenCache.rw);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    tstrncpy(ti->user, token->user, sizeof(ti->user));

    int32_t code = taosHashPut(tokenCache.tokens, token->token, sizeof(token->token) - 1, &ti, sizeof(ti));
    if (code != 0) {
      taosMemoryFree(ti);
      taosThreadRwlockUnlock(&tokenCache.rw);
      TAOS_RETURN(code);
    }
  }

  ti->expireTime = token->expireTime;
  ti->enabled = token->enabled;

  taosThreadRwlockUnlock(&tokenCache.rw);
  TAOS_RETURN(0);
}



static void tokenCacheRemove(const char* token) {
  (void)taosThreadRwlockWrlock(&tokenCache.rw);
  SCachedTokenInfo** pp = taosHashGet(tokenCache.tokens, token, TSDB_TOKEN_LEN - 1);
  if (pp != NULL) {
    if (taosHashRemove(tokenCache.tokens, token, TSDB_TOKEN_LEN - 1) != 0) {
      mDebug("failed to remove token %s from token cache", token);
    } else {
      taosMemoryFree(*pp);
    }
  }
  taosThreadRwlockUnlock(&tokenCache.rw);
}



static bool tokenCacheExist(const char* token) {
  (void)taosThreadRwlockRdlock(&tokenCache.rw);
  SCachedTokenInfo** pp = taosHashGet(tokenCache.tokens, token, TSDB_TOKEN_LEN - 1);
  taosThreadRwlockUnlock(&tokenCache.rw);
  return pp != NULL;
}



int32_t mndTokenCacheRebuild(SMnode *pMnode) {
  int32_t    code = 0, lino = 0;
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;

  (void)taosThreadRwlockWrlock(&tokenCache.rw);
  while (1) {
    STokenObj* token = NULL;
    pIter = sdbFetch(pSdb, SDB_TOKEN, pIter, (void**)&token);
    if (pIter == NULL) {
      break;
    }
    
    SCachedTokenInfo* ti = taosMemoryCalloc(1, sizeof(SCachedTokenInfo));
    if (ti == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    tstrncpy(ti->user, token->user, sizeof(ti->user));
    ti->expireTime = token->expireTime;
    ti->enabled = token->enabled;

    code = taosHashPut(tokenCache.tokens, token->token, sizeof(token->token) - 1, &ti, sizeof(ti));
    if (code != 0) {
      taosMemoryFree(ti);
      goto _OVER;
    }
  }

_OVER:
  taosThreadRwlockUnlock(&tokenCache.rw);
  if (code < 0) {
    mError("failed to rebuild token cache at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}



SCachedTokenInfo* mndGetCachedTokenInfo(const char* token, SCachedTokenInfo* ti) {
  (void)taosThreadRwlockRdlock(&tokenCache.rw);
  SCachedTokenInfo** pp = (SCachedTokenInfo**)taosHashGet(tokenCache.tokens, token, TSDB_TOKEN_LEN - 1);
  if (pp != NULL && *pp != NULL) {
    (void)memcpy(ti, *pp, sizeof(SCachedTokenInfo));
  } else {
    ti = NULL;
  }
  taosThreadRwlockUnlock(&tokenCache.rw);
  return ti;
}



static SSdbRaw *mndTokenActionEncode(STokenObj *pObj) {
  int32_t code = 0, lino = 0;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TOKEN, TOKEN_VER_NUMBER, sizeof(STokenObj) + TOKEN_RESERVE_SIZE);
  if (pRaw == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pObj->name, sizeof(pObj->name), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->token, sizeof(pObj->token), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->user, sizeof(pObj->user), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->provider, sizeof(pObj->provider), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->extraInfo, sizeof(pObj->extraInfo), _OVER)
  SDB_SET_INT8(pRaw, dataPos, pObj->enabled, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->expireTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TOKEN_RESERVE_SIZE, _OVER)

_OVER:
  terrno = code;
  if (code != 0) {
    mError("token:%s, failed to encode to raw:%p since %s", pObj->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("token:%s, encode to raw:%p, row:%p", pObj->name, pRaw, pObj);
  return pRaw;
}



static SSdbRow *mndTokenActionDecode(SSdbRaw *pRaw) {
  int32_t    code = 0, lino = 0;
  SSdbRow   *pRow = NULL;
  STokenObj *pObj = NULL;
  int8_t     sver = 0;


  if ((code = sdbGetRawSoftVer(pRaw, &sver)) != 0) {
    goto _OVER;
  }

  if (sver != TOKEN_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(STokenObj));
  if (pRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    goto _OVER;
  }

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pObj->name, sizeof(pObj->name), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->token, sizeof(pObj->token), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->user, sizeof(pObj->user), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->provider, sizeof(pObj->provider), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->extraInfo, sizeof(pObj->extraInfo), _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pObj->enabled, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->expireTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TOKEN_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pObj->lock);

_OVER:
  terrno = code;
  if (code != 0) {
    mError("token:%s, failed to decode from raw:%p since %s", pObj == NULL ? "" : pObj->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("token:%s, decode from raw:%p, row:%p", pObj->name, pRaw, pObj);
  return pRow;
}



static int32_t mndTokenActionInsert(SSdb *pSdb, STokenObj *pObj) {
  mTrace("token:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}



static int32_t mndTokenActionDelete(SSdb *pSdb, STokenObj *pObj) {
  mTrace("token:%s, perform delete action, row:%p", pObj->name, pObj);
  return 0;
}



static int32_t mndTokenActionUpdate(SSdb *pSdb, STokenObj *pOld, STokenObj *pNew) {
  mTrace("token:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  tstrncpy(pOld->provider, pNew->provider, sizeof(pOld->provider));
  tstrncpy(pOld->extraInfo, pNew->extraInfo, sizeof(pOld->extraInfo));
  pOld->enabled    = pNew->enabled;
  pOld->expireTime = pNew->expireTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}



static void generateToken(char *token, size_t len) {
  const char* set = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  int32_t     setLen = 62;
  for (int32_t i = 0; i < len - 1; ++i) {
    token[i] = set[taosSafeRand() % setLen];
  }
  token[len - 1] = 0;
}



static int32_t mndCreateToken(SMnode* pMnode, SCreateTokenReq* pCreate, SUserObj *pUser, SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;
  STrans  *pTrans = NULL;

  STokenObj tokenObj = {0};
  tstrncpy(tokenObj.name, pCreate->name, sizeof(tokenObj.name));
  tstrncpy(tokenObj.user, pCreate->user, sizeof(tokenObj.user));
  tstrncpy(tokenObj.provider, pCreate->provider, sizeof(tokenObj.provider));
  tstrncpy(tokenObj.extraInfo, pCreate->extraInfo, sizeof(tokenObj.extraInfo));
  tokenObj.enabled    = pCreate->enable;
  tokenObj.createdTime = taosGetTimestampSec();
  tokenObj.expireTime = (pCreate->ttl > 0) ? (tokenObj.createdTime + pCreate->ttl) : 0;
  do {
    generateToken(tokenObj.token, sizeof(tokenObj.token));
  } while (tokenCacheExist(tokenObj.token));

  SUserObj newUser = {0};
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);
  newUser.tokenNum++;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-token");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  // save generated token to transaction as user data
  SCreateTokenRsp resp = { 0 };
  tstrncpy(resp.name, tokenObj.name, sizeof(resp.name));
  tstrncpy(resp.user, tokenObj.user, sizeof(resp.user));
  tstrncpy(resp.token, tokenObj.token, sizeof(resp.token));

  int32_t len = tSerializeSCreateTokenResp(NULL, 0, &resp);
  if (len < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  void *pData = taosMemoryMalloc(len);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  if (tSerializeSCreateTokenResp(pData, len, &resp) != len) {
    code = TSDB_CODE_INVALID_MSG;
    taosMemoryFree(pData);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }
  mndTransSetUserData(pTrans, pData, len);

  mInfo("trans:%d, used to create token:%s", pTrans->id, pCreate->name);

  // token commit log
  SSdbRaw *pCommitRaw = mndTokenActionEncode(&tokenObj);
  if (pCommitRaw == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pCommitRaw), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);

  // user commit log
  pCommitRaw = mndUserActionEncode(&newUser);
  if (pCommitRaw == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pCommitRaw), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);
  
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _OVER);

  TAOS_CHECK_GOTO(tokenCacheAdd(&tokenObj), &lino, _OVER);

_OVER:
  if (code != 0) {
    if (pTrans == NULL) {
      mError("token:%s, failed to create at line %d since %s", pCreate->name, lino, tstrerror(code));
    } else {
      mError("trans:%d, failed to create token:%s at line %d since %s", pTrans->id, pCreate->name, lino, tstrerror(code));
    }
  }
  mndTransDrop(pTrans);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}



static int32_t mndProcessCreateTokenReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = 0, lino = 0;
  STokenObj      *pToken = NULL;
  SUserObj       *pOperUser = NULL;
  SUserObj       *pTokenUser = NULL;
  SCreateTokenReq createReq = {0};
  int64_t         tss = taosGetTimestampMs();

  if (tDeserializeSCreateTokenReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
    goto _OVER;
  }

  mInfo("token from user:%s, start to create", createReq.user);

  if (createReq.name[0] == '\0') {
    code = TSDB_CODE_MND_INVALID_TOKEN_NAME;
    goto _OVER;
  }

  code = mndAcquireToken(pMnode, createReq.name, &pToken);
  if (pToken != NULL) {
    code = TSDB_CODE_MND_TOKEN_ALREADY_EXIST;
    goto _OVER;
  }

  code = mndAcquireUser(pMnode, createReq.user, &pTokenUser);
  if (pTokenUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_NOT_EXIST, &lino, _OVER);
  }

  if (pTokenUser->allowTokenNum > 0 && pTokenUser->tokenNum >= pTokenUser->allowTokenNum) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_TOKENS, &lino, _OVER);
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndCheckTokenPrivilege(pOperUser, createReq.user), &lino, _OVER);

  TAOS_CHECK_GOTO(mndCreateToken(pMnode, &createReq, pTokenUser, pReq), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    double duration = (taosGetTimestampMs() - tss) / 1000.0;
    char auditLog[256] = {0};
    int32_t auditLen = tsnprintf(auditLog, sizeof(auditLog), "enable:%d, ttl:%d, provider:%s", createReq.enable, createReq.ttl, createReq.provider);
    auditRecord(pReq, pMnode->clusterId, "createToken", "", createReq.name, auditLog, auditLen, duration, 0);
  }

_OVER:
  if (code == TSDB_CODE_MND_TOKEN_ALREADY_EXIST && createReq.ignoreExists) {
    code = 0;
  } else if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("token:%s, failed to create at line %d since %s", createReq.name, lino, tstrerror(code));
  }

  mndReleaseToken(pMnode, pToken);
  mndReleaseUser(pMnode, pTokenUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSCreateTokenReq(&createReq);

  TAOS_RETURN(code);
}



static int32_t mndTokenDupObj(STokenObj *pOld, STokenObj* pNew) {
  (void)memcpy(pNew, pOld, sizeof(STokenObj));
  taosInitRWLatch(&pNew->lock);
  return 0;
}



static int32_t mndAlterToken(SMnode *pMnode, STokenObj *pToken, SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "alter-token");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  mInfo("trans:%d, used to alter token:%s", pTrans->id, pToken->name);

  SSdbRaw *pCommitRaw = mndTokenActionEncode(pToken);
  if (pCommitRaw == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pCommitRaw), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _OVER);

  TAOS_CHECK_GOTO(tokenCacheUpdate(pToken), &lino, _OVER);

_OVER:
  if (code != 0) {
    if (pTrans == NULL) {
      mError("token:%s, failed to alter at line %d since %s", pToken->name, lino, tstrerror(code));
    } else {
      mError("trans:%d, failed to alter token:%s at line %d since %s", pTrans->id, pToken->name, lino, tstrerror(code));
    }
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}



static int32_t mndProcessAlterTokenReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  int32_t        code = 0, lino = 0;
  STokenObj     *pToken = NULL;
  SUserObj      *pOperUser = NULL;
  STokenObj      newToken = {0};
  SAlterTokenReq alterReq = {0};
  int64_t        tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSAlterTokenReq(pReq->pCont, pReq->contLen, &alterReq), &lino, _OVER);
  mInfo("token:%s, start to alter", alterReq.name);

  code = mndAcquireToken(pMnode, alterReq.name, &pToken);
  if (pToken == NULL) {
    code = TSDB_CODE_MND_TOKEN_NOT_EXIST;
    goto _OVER;
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndCheckTokenPrivilege(pOperUser, pToken->user), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTokenDupObj(pToken, &newToken), &lino, _OVER);

  char auditLog[256] = {0};
  int32_t auditLen = 0;

  if (alterReq.hasEnable) {
    newToken.enabled = alterReq.enable;
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "enable:%d,", alterReq.enable);
  }
  if (alterReq.hasTtl) {
    newToken.expireTime = (alterReq.ttl > 0) ? (taosGetTimestampSec() + alterReq.ttl) : 0;
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "ttl:%d,", alterReq.ttl);
  }
  if (alterReq.hasProvider) {
    tstrncpy(newToken.provider, alterReq.provider, sizeof(newToken.provider));
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "provider:%s,", alterReq.provider);
  }
  if (alterReq.hasExtraInfo) {
    tstrncpy(newToken.extraInfo, alterReq.extraInfo, sizeof(newToken.extraInfo));
    // extra info is too long, just log a fixed string
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "extraInfo,");
  }
  if (auditLen > 0) {
    auditLog[--auditLen] = '\0';  // remove last ','
  }

  TAOS_CHECK_GOTO(mndAlterToken(pMnode, &newToken, pReq), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    double duration = (taosGetTimestampMs() - tss) / 1000.0;
    auditRecord(pReq, pMnode->clusterId, "alterToken", "", alterReq.name, auditLog, auditLen, duration, 0);
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("token:%s, failed to alter at line %d since %s", alterReq.name, lino, tstrerror(code));
  }

  mndReleaseToken(pMnode, pToken);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSAlterTokenReq(&alterReq);
  return 0;
}



static int32_t mndDropToken(SMnode *pMnode, STokenObj* pToken, SUserObj* pUser, SRpcMsg *pReq) {
  STrans *pTrans = NULL;
  int32_t code = 0, lino = 0;

  SUserObj newUser = {0};
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);
  newUser.tokenNum--;
  
  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-token");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  mInfo("trans:%d, used to drop token:%s", pTrans->id, pToken->name);

  // token commit log
  SSdbRaw *pCommitRaw = mndTokenActionEncode(pToken);
  if (pCommitRaw == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pCommitRaw), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED), &lino, _OVER);

  // user commit log
  pCommitRaw = mndUserActionEncode(&newUser);
  if (pCommitRaw == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pCommitRaw), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);
  
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _OVER);

  tokenCacheRemove(pToken->token);

_OVER:
  if (code != 0) {
    if (pTrans == NULL) {
      mError("token:%s, failed to drop at line %d since %s", pToken->name, lino, tstrerror(code));
    } else {
      mError("trans:%d, failed to drop token:%s at line %d since %s", pTrans->id, pToken->name, lino, tstrerror(code));
    }
  }
  mndTransDrop(pTrans);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}



static int32_t mndProcessDropTokenReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0, lino = 0;
  STokenObj     *pToken = NULL;
  SUserObj      *pOperUser = NULL;
  SUserObj      *pTokenUser = NULL;
  SDropTokenReq  dropReq = {0};
  double         tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSDropTokenReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("token:%s, start to drop", dropReq.name);

  code = mndAcquireToken(pMnode, dropReq.name, &pToken);
  if (pToken == NULL) {
    code = TSDB_CODE_MND_TOKEN_NOT_EXIST;
    goto _OVER;
  }

  code = mndAcquireUser(pMnode, pToken->user, &pTokenUser);
  if (pTokenUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_NOT_EXIST, &lino, _OVER);
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndCheckTokenPrivilege(pOperUser, pToken->user), &lino, _OVER);

  TAOS_CHECK_GOTO(mndDropToken(pMnode, pToken, pTokenUser, pReq), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    double duration = (taosGetTimestampMs() - tss) / 1000.0;
    auditRecord(pReq, pMnode->clusterId, "dropToken", "", dropReq.name, dropReq.sql, dropReq.sqlLen, duration, 0);
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("token:%s, failed to drop at line %d since %s", dropReq.name, lino, tstrerror(code));
  }

  mndReleaseToken(pMnode, pToken);
  mndReleaseUser(pMnode, pTokenUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSDropTokenReq(&dropReq);
  TAOS_RETURN(code);
}



static int32_t mndRetrieveTokens(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0, lino = 0;
  int32_t   numOfRows = 0;
  STokenObj *pToken = NULL;
  SUserObj *pUser = NULL;
  char      buf[TSDB_TOKEN_EXTRA_INFO_LEN + VARSTR_HEADER_SIZE] = {0};

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pUser);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _exit);
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOKEN, pShow->pIter, (void **)&pToken);
    if (pShow->pIter == NULL) break;

    if (!pUser->superUser && taosStrcasecmp(pToken->user, pReq->info.conn.user) != 0) {
      sdbRelease(pSdb, pToken);
      continue;
    }

    int32_t cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pToken->name, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pToken->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pToken->provider, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pToken->enabled, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    int64_t tm = (int64_t)pToken->createdTime * 1000;
    COL_DATA_SET_VAL_GOTO((const char *)&tm, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    tm = (int64_t)pToken->expireTime * 1000;
    COL_DATA_SET_VAL_GOTO((const char *)&tm, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pToken->extraInfo, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pToken, pShow->pIter, _exit);

    numOfRows++;
    sdbRelease(pSdb, pToken);
  }

  pShow->numOfRows += numOfRows;

_exit:
  mndReleaseUser(pMnode, pUser);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}



static void mndCancelGetNextToken(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_TOKEN);
}



int32_t mndInitToken(SMnode *pMnode) {
  TAOS_CHECK_RETURN(tokenCacheInit());

  SSdbTable table = {
      .sdbType = SDB_TOKEN,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndTokenActionEncode,
      .decodeFp = (SdbDecodeFp)mndTokenActionDecode,
      .insertFp = (SdbInsertFp)mndTokenActionInsert,
      .updateFp = (SdbUpdateFp)mndTokenActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTokenActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOKEN, mndProcessCreateTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_TOKEN, mndProcessAlterTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOKEN, mndProcessDropTokenReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndRetrieveTokens);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndCancelGetNextToken);
  return sdbSetTable(pMnode->pSdb, table);
}



void mndCleanupToken(SMnode *pMnode) {
  tokenCacheCleanup();
}



int32_t mndAcquireToken(SMnode *pMnode, const char *token, STokenObj **ppToken) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppToken = sdbAcquire(pSdb, SDB_TOKEN, token);
  if (*ppToken == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_TOKEN_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_TOKEN_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}



void mndReleaseToken(SMnode *pMnode, STokenObj *pToken) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pToken);
}



int32_t mndBuildSMCreateTokenResp(STrans *pTrans, void **ppResp, int32_t *pRespLen) {
  // user data is the response
  *ppResp = pTrans->userData;
  *pRespLen = pTrans->userDataLen;
  pTrans->userData = NULL;
  pTrans->userDataLen = 0;
  return 0;
}
