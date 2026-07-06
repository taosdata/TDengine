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
#include "mndProfile.h"
#include "audit.h"
#include "crypt.h"
#include "mndCluster.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndQnode.h"
#include "mndSecurityPolicy.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndExtSource.h"
#include "mndStb.h"
#include "mndToken.h"
#include "mndTxn.h"
#include "mndUser.h"
#include "mndView.h"
#include "tglobal.h"
#include "totp.h"
#include "tversion.h"
#ifdef USE_LIBGSASL
#include <gsasl.h>
#endif

typedef struct {
  uint32_t id;
  int8_t   connType;
  int8_t   killed;
  char     user[TSDB_USER_LEN];
  char     tokenName[TSDB_TOKEN_LEN];
  char     app[TSDB_APP_NAME_LEN];  // app name that invokes taosc
  int64_t  appStartTimeMs;          // app start time
  int32_t  pid;                     // pid of app that invokes taosc
  int32_t  numOfQueries;
  int64_t  loginTimeMs;
  int64_t  lastAccessTimeMs;
  uint64_t killId;
  SArray  *pQueries;  // SArray<SQueryDesc>
  char     userApp[TSDB_APP_NAME_LEN];
  SRWLatch queryLock;
  uint32_t userIp;
  SIpAddr  userDualIp;
  SIpAddr  addr;
  char     sVer[TSDB_VERSION_LEN];
  char     cInfo[CONNECTOR_INFO_LEN];
} SConnObj;

typedef struct {
  int64_t            appId;
  SIpAddr            cliAddr;
  int32_t            pid;
  char               name[TSDB_APP_NAME_LEN];
  int64_t            startTime;
  SAppClusterSummary summary;
  int64_t            lastAccessTimeMs;
} SAppObj;

typedef struct {
  int32_t totalDnodes;
  int32_t onlineDnodes;
  SEpSet  epSet;
  SArray *pQnodeList;
  int64_t ipWhiteListVer;
} SConnPreparedObj;

#define CACHE_OBJ_KEEP_TIME 3  // s

static void      mndFreeConn(SConnObj *pConn);
static SConnObj *mndAcquireConn(SMnode *pMnode, uint32_t connId);
static void      mndReleaseConn(SMnode *pMnode, SConnObj *pConn, bool extendLifespan);
static void     *mndGetNextConn(SMnode *pMnode, SCacheIter *pIter);
static void      mndCancelGetNextConn(SMnode *pMnode, void *pIter);
static int32_t   mndProcessHeartBeatReq(SRpcMsg *pReq);
static int32_t   mndProcessConnectReq(SRpcMsg *pReq);
static int32_t   mndProcessSaslStepReq(SRpcMsg *pReq);
#ifdef USE_LIBGSASL
static int32_t   mndSaslInit(SMnode *pMnode);
static void      mndSaslCleanup(SMnode *pMnode);
#endif
static int32_t   mndProcessKillQueryReq(SRpcMsg *pReq);
static int32_t   mndProcessKillConnReq(SRpcMsg *pReq);
static int32_t   mndRetrieveConns(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t   mndRetrieveQueries(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void      mndCancelGetNextQuery(SMnode *pMnode, void *pIter);
static void      mndFreeApp(SAppObj *pApp);
static int32_t   mndRetrieveApps(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void      mndCancelGetNextApp(SMnode *pMnode, void *pIter);
static int32_t   mndProcessSvrVerReq(SRpcMsg *pReq);

int32_t mndInitProfile(SMnode *pMnode) {
  int32_t       code = 0;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  // in ms
  int32_t checkTime = CACHE_OBJ_KEEP_TIME * 1000;
  pMgmt->connCache = taosCacheInit(TSDB_DATA_TYPE_UINT, checkTime, false, (__cache_free_fn_t)mndFreeConn, "conn");
  if (pMgmt->connCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc profile cache since %s", terrstr());
    TAOS_RETURN(code);
  }

  pMgmt->appCache = taosCacheInit(TSDB_DATA_TYPE_BIGINT, checkTime, true, (__cache_free_fn_t)mndFreeApp, "app");
  if (pMgmt->appCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc profile cache since %s", terrstr());
    TAOS_RETURN(code);
  }

#ifdef USE_LIBGSASL
  TAOS_CHECK_RETURN(mndSaslInit(pMnode));
#endif

  mndSetMsgHandle(pMnode, TDMT_MND_HEARTBEAT, mndProcessHeartBeatReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONNECT, mndProcessConnectReq);
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH_SASL, mndProcessSaslStepReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_QUERY, mndProcessKillQueryReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_CONN, mndProcessKillConnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SERVER_VERSION, mndProcessSvrVerReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndRetrieveConns);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndCancelGetNextConn);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndRetrieveQueries);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndCancelGetNextQuery);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_APPS, mndRetrieveApps);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_APPS, mndCancelGetNextApp);

  TAOS_RETURN(code);
}

void mndCleanupProfile(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
#ifdef USE_LIBGSASL
  mndSaslCleanup(pMnode);
#endif
  if (pMgmt->connCache != NULL) {
    taosCacheCleanup(pMgmt->connCache);
    pMgmt->connCache = NULL;
  }

  if (pMgmt->appCache != NULL) {
    taosCacheCleanup(pMgmt->appCache);
    pMgmt->appCache = NULL;
  }
}

static void getUserIpFromConnObj(SConnObj *pConn, char *dst) {
  static char *none = "0.0.0.0";
  if (pConn->userIp != 0 && pConn->userIp != INADDR_NONE) {
    taosInetNtoa(varDataVal(dst), pConn->userIp);
    varDataLen(dst) = strlen(varDataVal(dst));
  }

  if (pConn->userDualIp.ipv4[0] != 0 && strncmp(pConn->userDualIp.ipv4, none, strlen(none)) != 0) {
    char   *ipstr = IP_ADDR_STR(&pConn->userDualIp);
    int32_t len = strlen(ipstr);
    memcpy(varDataVal(dst), ipstr, len);
    varDataLen(dst) = len;
  }
  return;
}
static void setUserInfo2Conn(SConnObj *connObj, char *userApp, uint32_t userIp, char *cInfo) {
  if (connObj == NULL) {
    return;
  }
  tstrncpy(connObj->userApp, userApp, sizeof(connObj->userApp));
  tstrncpy(connObj->cInfo, cInfo, sizeof(connObj->cInfo));
  connObj->userIp = userIp;
}
static void setUserInfoIpToConn(SConnObj *connObj, SIpRange *pRange) {
  int32_t code = 0;
  if (connObj == NULL) {
    return;
  }

  code = tIpUintToStr(pRange, &connObj->userDualIp);
  if (code != 0) {
    mError("conn:%u, failed to set user ip to conn since %s", connObj->id, tstrerror(code));
    return;
  }
}



static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, const char* tokenName, int8_t connType, SIpAddr *pAddr,
                               int32_t pid, const char *app, int64_t startTime, const char *sVer) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  char     connStr[255] = {0};
  char    *ip = IP_ADDR_STR(pAddr);
  uint16_t port = pAddr->port;

  int32_t  len = tsnprintf(connStr, sizeof(connStr), "%s%d%d%d%s", user, ip, port, pid, app);
  uint32_t connId = mndGenerateUid(connStr, len);
  if (startTime == 0) startTime = taosGetTimestampMs();

  SConnObj connObj = {
      .id = connId,
      .connType = connType,
      .appStartTimeMs = startTime,
      .pid = pid,
      .addr = *pAddr,
      .killed = 0,
      .loginTimeMs = taosGetTimestampMs(),
      .lastAccessTimeMs = 0,
      .killId = 0,
      .numOfQueries = 0,
      .pQueries = NULL,
  };

  connObj.lastAccessTimeMs = connObj.loginTimeMs;
  tstrncpy(connObj.user, user, sizeof(connObj.user));
  tstrncpy(connObj.tokenName, tokenName, sizeof(connObj.tokenName));
  tstrncpy(connObj.app, app, sizeof(connObj.app));
  tstrncpy(connObj.sVer, sVer, sizeof(connObj.sVer));

  SConnObj *pConn =
      taosCachePut(pMgmt->connCache, &connId, sizeof(uint32_t), &connObj, sizeof(connObj), CACHE_OBJ_KEEP_TIME * 1000);
  if (pConn == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("conn:%d, failed to put into cache since %s, user:%s", connId, user, terrstr());
    return NULL;
  } else {
    mTrace("conn:%u, is created, data:%p user:%s", pConn->id, pConn, user);
    return pConn;
  }
}



static void mndFreeConn(SConnObj *pConn) {
  taosWLockLatch(&pConn->queryLock);
  taosArrayDestroyEx(pConn->pQueries, tFreeClientHbQueryDesc);
  taosWUnLockLatch(&pConn->queryLock);

  mTrace("conn:%u, is destroyed, data:%p", pConn->id, pConn);
}

static SConnObj *mndAcquireConn(SMnode *pMnode, uint32_t connId) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &connId, sizeof(connId));
  if (pConn == NULL) {
    mDebug("conn:%u, already destroyed", connId);
    return NULL;
  }

  pConn->lastAccessTimeMs = taosGetTimestampMs();
  mTrace("conn:%u, acquired from cache, data:%p", pConn->id, pConn);
  return pConn;
}

static void mndReleaseConn(SMnode *pMnode, SConnObj *pConn, bool extendLifespan) {
  if (pConn == NULL) return;
  mTrace("conn:%u, released from cache, data:%p", pConn->id, pConn);

  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  if (extendLifespan) taosCacheTryExtendLifeSpan(pMgmt->connCache, (void **)&pConn);
  taosCacheRelease(pMgmt->connCache, (void **)&pConn, false);
}

void *mndGetNextConn(SMnode *pMnode, SCacheIter *pIter) {
  SConnObj *pConn = NULL;
  bool      hasNext = taosCacheIterNext(pIter);
  if (hasNext) {
    size_t dataLen = 0;
    pConn = taosCacheIterGetData(pIter, &dataLen);
  } else {
    taosCacheDestroyIter(pIter);
  }

  return pConn;
}

static void mndCancelGetNextConn(SMnode *pMnode, void *pIter) {
  if (pIter != NULL) {
    taosCacheDestroyIter(pIter);
  }
}



// TODO: if there are many connections, this function may be slow
int32_t mndCountUserConns(SMnode *pMnode, const char *user) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  SCacheIter   *pIter = taosCacheCreateIter(pMgmt->connCache);
  if (pIter == NULL) {
    mError("failed to create conn cache iterator");
    return -1;
  }

  int32_t    count = 0;
  SConnObj  *pConn = NULL;
  while ((pConn = mndGetNextConn(pMnode, pIter)) != NULL) {
    if (strncmp(pConn->user, user, TSDB_USER_LEN) == 0) {
      count++;
    }
    mndReleaseConn(pMnode, pConn, true);
  }

  return count;
}



static int32_t verifyPassword(SUserObj* pUser, const char* inputPass) {
  int32_t code = 0;

  char currPass[TSDB_PASSWORD_LEN] = {0};
  taosRLockLatch(&pUser->lock);
  (void)memcpy(currPass, pUser->passwords[0].pass, TSDB_PASSWORD_LEN);
  taosRUnLockLatch(&pUser->lock);

  // A user with no legacy password hash (all-zero) must never authenticate by password: an empty
  // supplied password would otherwise compare equal below. Such a user can only authenticate via the
  // SASL handshake (which does not call this function). taosEncryptPass_c never yields an all-zero
  // hash for a real password, so this rejects only the unprovisioned/zeroed sentinel.
  bool currPassEmpty = true;
  for (size_t i = 0; i < sizeof(currPass) - 1; i++) {
    if (currPass[i] != 0) {
      currPassEmpty = false;
      break;
    }
  }
  if (currPassEmpty) {
    return TSDB_CODE_MND_AUTH_FAILURE;
  }

  char pass[TSDB_PASSWORD_LEN] = {0};
  (void)memcpy(pass, inputPass, TSDB_PASSWORD_LEN);
  pass[TSDB_PASSWORD_LEN - 1] = 0;

  if (pUser->passEncryptAlgorithm != 0 && strlen(tsDataKey) > 0) {
    code = mndEncryptPass(pass, pUser->salt, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // constant time comparison to prevent timing attack
  volatile uint8_t res = 0;
  for (size_t i = 0; i < sizeof(pass) - 1; i++) {
    res |= pass[i] ^ currPass[i];
  }

 return (res == 0) ? TSDB_CODE_SUCCESS: TSDB_CODE_MND_AUTH_FAILURE;
}



static bool verifyTotp(SUserObj *pUser, int32_t totpCode) {
  if (!mndIsTotpEnabledUser(pUser)) {
    return true;
  }
  return taosVerifyTotpCode(pUser->totpsecret, sizeof(pUser->totpsecret), totpCode, 6, 1) != 0;
}

// ----------------------------------------------------------------------------
// SCRAM-SHA-256 application-layer authentication handshake (libgsasl).
//
// The single CONNECT is preceded by one or more TDMT_MND_AUTH_SASL rounds: the client and server
// shuttle opaque SASL tokens (gsasl_step64 output) until the exchange completes, at which point the
// server mints a short-lived one-time auth token. The client then runs the normal CONNECT carrying
// that token, and mndProcessConnectReq trusts it instead of verifying a password. Everything is
// leader-local because all mnode RPC is served by the leader.
// ----------------------------------------------------------------------------
#define MND_SASL_HANDSHAKE_TTL_MS 10000  // covers an in-flight handshake and the follow-up CONNECT

#ifdef USE_LIBGSASL
typedef struct {
  Gsasl_session *sctx;
  char           user[TSDB_USER_LEN];
} SSaslSessionCache;

typedef struct {
  char user[TSDB_USER_LEN];
} SSaslTokenCache;

static void mndFreeSaslSession(void *p) {
  SSaslSessionCache *pSess = p;
  if (pSess != NULL && pSess->sctx != NULL) {
    gsasl_finish(pSess->sctx);
    pSess->sctx = NULL;
  }
}

static void mndBinToHex(const uint8_t *in, int32_t len, char *out) {
  static const char *hex = "0123456789abcdef";
  for (int32_t i = 0; i < len; i++) {
    out[i * 2] = hex[(in[i] >> 4) & 0xf];
    out[i * 2 + 1] = hex[in[i] & 0xf];
  }
  out[len * 2] = 0;
}

// Supply the SCRAM secrets (never a plaintext password) for the authenticating user. The SMnode is
// fetched from the gsasl global callback hook; the user name comes from GSASL_AUTHID parsed by gsasl.
static int mndSaslServerCb(Gsasl *ctx, Gsasl_session *sctx, Gsasl_property prop) {
  SMnode *pMnode = gsasl_callback_hook_get(ctx);
  if (pMnode == NULL) return GSASL_NO_CALLBACK;

  if (prop != GSASL_SCRAM_ITER && prop != GSASL_SCRAM_SALT && prop != GSASL_SCRAM_STOREDKEY &&
      prop != GSASL_SCRAM_SERVERKEY) {
    return GSASL_NO_CALLBACK;
  }

  const char *authid = gsasl_property_fast(sctx, GSASL_AUTHID);
  if (authid == NULL) return GSASL_NO_AUTHID;

  SUserObj *pUser = NULL;
  if (mndAcquireUser(pMnode, authid, &pUser) != 0 || pUser == NULL) return GSASL_NO_AUTHID;
  if (pUser->scram.algo != TSDB_SCRAM_ALGO_SHA256) {
    mndReleaseUser(pMnode, pUser);
    return GSASL_NO_PASSWORD;  // user has no SCRAM credentials -> client falls back to legacy auth
  }

  int rc = GSASL_OK;
  if (prop == GSASL_SCRAM_ITER) {
    char iter[16] = {0};
    (void)snprintf(iter, sizeof(iter), "%d", pUser->scram.iter);
    (void)gsasl_property_set(sctx, GSASL_SCRAM_ITER, iter);
  } else if (prop == GSASL_SCRAM_SALT) {
    char  *b64 = NULL;
    size_t b64len = 0;
    if (gsasl_base64_to((const char *)pUser->scram.salt, pUser->scram.saltLen, &b64, &b64len) == GSASL_OK) {
      (void)gsasl_property_set(sctx, GSASL_SCRAM_SALT, b64);
      gsasl_free(b64);
    } else {
      rc = GSASL_MALLOC_ERROR;
    }
  } else {
    // gsasl's SCRAM server base64-decodes GSASL_SCRAM_STOREDKEY / GSASL_SCRAM_SERVERKEY
    // (lib/scram/server.c: extract_serverkey -> gsasl_base64_from), so they MUST be supplied
    // base64-encoded -- not hex -- or the StoredKey comparison fails with an auth error.
    const uint8_t *key = (prop == GSASL_SCRAM_STOREDKEY) ? pUser->scram.storedKey : pUser->scram.serverKey;
    char          *b64 = NULL;
    size_t         b64len = 0;
    if (gsasl_base64_to((const char *)key, TSDB_SCRAM_KEY_LEN, &b64, &b64len) == GSASL_OK) {
      (void)gsasl_property_set(sctx, prop, b64);
      gsasl_free(b64);
    } else {
      rc = GSASL_MALLOC_ERROR;
    }
  }

  mndReleaseUser(pMnode, pUser);
  return rc;
}

static int32_t mndSaslInit(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  Gsasl        *ctx = NULL;
  if (gsasl_init(&ctx) != GSASL_OK) {
    mError("failed to init gsasl context");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  gsasl_callback_hook_set(ctx, pMnode);
  gsasl_callback_set(ctx, mndSaslServerCb);
  pMgmt->saslCtx = ctx;

  int32_t ttl = MND_SASL_HANDSHAKE_TTL_MS;
  pMgmt->saslSessCache =
      taosCacheInit(TSDB_DATA_TYPE_BINARY, ttl, false, (__cache_free_fn_t)mndFreeSaslSession, "sasl-sess");
  pMgmt->saslTokenCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, ttl, false, NULL, "sasl-token");
  if (pMgmt->saslSessCache == NULL || pMgmt->saslTokenCache == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

static void mndSaslCleanup(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  if (pMgmt->saslSessCache != NULL) {
    taosCacheCleanup(pMgmt->saslSessCache);
    pMgmt->saslSessCache = NULL;
  }
  if (pMgmt->saslTokenCache != NULL) {
    taosCacheCleanup(pMgmt->saslTokenCache);
    pMgmt->saslTokenCache = NULL;
  }
  if (pMgmt->saslCtx != NULL) {
    gsasl_done((Gsasl *)pMgmt->saslCtx);
    pMgmt->saslCtx = NULL;
  }
}
#endif  // USE_LIBGSASL

// Validate (and consume, one-time) a SASL auth token previously issued to `user`. Returns 0 when the
// token is valid for the user; an error code otherwise. Available regardless of USE_LIBGSASL so the
// CONNECT path links cleanly; without gsasl the token cache is never populated so this always fails.
static int32_t mndConsumeSaslToken(SMnode *pMnode, const char *user, const char *token) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  if (pMgmt->saslTokenCache == NULL || token == NULL || token[0] == 0) {
    return TSDB_CODE_MND_AUTH_FAILURE;
  }
#ifdef USE_LIBGSASL
  SSaslTokenCache *pTok = taosCacheAcquireByKey(pMgmt->saslTokenCache, token, strlen(token));
  if (pTok == NULL) return TSDB_CODE_MND_SASL_SESSION_EXPIRED;
  int32_t code = (strcmp(pTok->user, user) == 0) ? 0 : TSDB_CODE_MND_AUTH_FAILURE;
  taosCacheRelease(pMgmt->saslTokenCache, (void **)&pTok, true);  // one-time use: drop on read
  return code;
#else
  return TSDB_CODE_MND_AUTH_FAILURE;
#endif
}

static int32_t mndProcessSaslStepReq(SRpcMsg *pReq) {
#ifndef USE_LIBGSASL
  mError("SASL handler called but USE_LIBGSASL is not defined");
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  int32_t       code = 0, lino = 0;
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  SSaslStepReq  stepReq = {0};
  SSaslStepRsp  stepRsp = {0};
  char         *out = NULL;
  void         *rspBuf = NULL;

  TAOS_CHECK_GOTO(tDeserializeSSaslStepReq(pReq->pCont, pReq->contLen, &stepReq), &lino, _OVER);

  mDebug("SASL step req: user=%s authId=%s mech=%s dataLen=%d", stepReq.user, stepReq.authId, stepReq.mech, stepReq.dataLen);

  SSaslSessionCache *pSess = NULL;
  Gsasl_session     *sctx = NULL;
  char               authId[TSDB_SASL_AUTH_ID_LEN] = {0};

  if (stepReq.authId[0] == 0) {
    // first round: if the user exists but has no SCRAM credentials, tell the client to fall back to
    // legacy password auth (OPS_NOT_SUPPORT). Unknown users fall through and fail in the handshake.
    // This pre-check re-acquires the user (the gsasl callback acquires it again to read the secrets):
    // a benign TOCTOU -- if ALTER USER changes the creds in between, the handshake just fails and the
    // client retries, with no risk of corruption or auth bypass.
    SUserObj *pCheck = NULL;
    if (mndAcquireUser(pMnode, stepReq.user, &pCheck) == 0 && pCheck != NULL) {
      int8_t algo = pCheck->scram.algo;
      mndReleaseUser(pMnode, pCheck);
      if (algo != TSDB_SCRAM_ALGO_SHA256) {
        mDebug("SASL: user %s has no SCRAM credentials (algo=%d), returning OPS_NOT_SUPPORT", stepReq.user, algo);
        TAOS_CHECK_GOTO(TSDB_CODE_OPS_NOT_SUPPORT, &lino, _OVER);
      }
    }
    // start a server session and assign a handshake id
    if (gsasl_server_start((Gsasl *)pMgmt->saslCtx, stepReq.mech, &sctx) != GSASL_OK) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_AUTH_FAILURE, &lino, _OVER);
    }
    char nonce[16] = {0};
    if (gsasl_nonce(nonce, sizeof(nonce)) != GSASL_OK) {
      gsasl_finish(sctx);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_AUTH_FAILURE, &lino, _OVER);
    }
    mndBinToHex((uint8_t *)nonce, sizeof(nonce), authId);

    SSaslSessionCache sess = {.sctx = sctx};
    tstrncpy(sess.user, stepReq.user, sizeof(sess.user));
    pSess = taosCachePut(pMgmt->saslSessCache, authId, strlen(authId), &sess, sizeof(sess), MND_SASL_HANDSHAKE_TTL_MS);
    if (pSess == NULL) {
      gsasl_finish(sctx);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
  } else {
    tstrncpy(authId, stepReq.authId, sizeof(authId));
    pSess = taosCacheAcquireByKey(pMgmt->saslSessCache, authId, strlen(authId));
    if (pSess == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_SASL_SESSION_EXPIRED, &lino, _OVER);
    }
    sctx = pSess->sctx;
  }

  int rc = gsasl_step64(sctx, stepReq.data ? (const char *)stepReq.data : "", &out);
  if (rc != GSASL_OK && rc != GSASL_NEEDS_MORE) {
    taosCacheRelease(pMgmt->saslSessCache, (void **)&pSess, true);  // failed -> drop session
    TAOS_CHECK_GOTO(TSDB_CODE_MND_AUTH_FAILURE, &lino, _OVER);
  }

  tstrncpy(stepRsp.authId, authId, sizeof(stepRsp.authId));
  if (out != NULL) {
    stepRsp.data = (uint8_t *)out;
    stepRsp.dataLen = (int32_t)strlen(out) + 1;
  }

  if (rc == GSASL_OK) {
    // handshake complete: mint a one-time auth token bound to the user
    stepRsp.done = 1;
    char tnonce[24] = {0};
    // Fail closed: if we cannot mint a token (RNG failure), do NOT report done=1 with an empty token.
    // An empty token would either drop the client to legacy auth or, under forceScram, lock out a user
    // who actually completed SCRAM. Drop the session and surface an error so the client retries.
    if (gsasl_nonce(tnonce, sizeof(tnonce)) != GSASL_OK) {
      taosCacheRelease(pMgmt->saslSessCache, (void **)&pSess, true);  // failed -> drop session
      TAOS_CHECK_GOTO(TSDB_CODE_MND_AUTH_FAILURE, &lino, _OVER);
    }
    mndBinToHex((uint8_t *)tnonce, sizeof(tnonce), stepRsp.authToken);
    SSaslTokenCache tok = {0};
    tstrncpy(tok.user, pSess->user, sizeof(tok.user));
    void *p = taosCachePut(pMgmt->saslTokenCache, stepRsp.authToken, strlen(stepRsp.authToken), &tok, sizeof(tok),
                           MND_SASL_HANDSHAKE_TTL_MS);
    if (p != NULL) taosCacheRelease(pMgmt->saslTokenCache, &p, false);
    taosCacheRelease(pMgmt->saslSessCache, (void **)&pSess, true);  // done -> drop session
  } else {
    taosCacheRelease(pMgmt->saslSessCache, (void **)&pSess, false);
  }

  int32_t rspLen = tSerializeSSaslStepRsp(NULL, 0, &stepRsp);
  if (rspLen < 0) TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  rspBuf = rpcMallocCont(rspLen);
  if (rspBuf == NULL) TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  if (tSerializeSSaslStepRsp(rspBuf, rspLen, &stepRsp) < 0) TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  pReq->info.rsp = rspBuf;
  pReq->info.rspLen = rspLen;

_OVER:
  if (out != NULL) gsasl_free(out);
  tFreeSSaslStepReq(&stepReq);
  if (code != 0) {
    mError("failed to process sasl step req at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
#endif
}

static int32_t mndProcessConnectReq(SRpcMsg *pReq) {
  int32_t          code = 0, lino = 0;

  SMnode          *pMnode = pReq->info.node;
  SConnectReq      connReq = {0};
  SUserObj        *pUser = NULL;
  SDbObj          *pDb = NULL;
  SConnObj        *pConn = NULL;
  const STraceId  *trace = &pReq->info.traceId;
  char            *ip = IP_ADDR_STR(&pReq->info.conn.cliAddr);
  uint16_t         port = pReq->info.conn.cliAddr.port;
  SCachedTokenInfo ti = {0};
  const char      *user = RPC_MSG_USER(pReq);
  const char      *token = RPC_MSG_TOKEN(pReq);
  int64_t          tss = taosGetTimestampMs();
  int64_t          now = tss / 1000;

  if (token != NULL && mndGetCachedTokenInfo(token, &ti) == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_TOKEN_NOT_EXIST, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDeserializeSConnectReq(pReq->pCont, pReq->contLen, &connReq), &lino, _OVER);
  TAOS_CHECK_GOTO(taosCheckVersionCompatibleFromStr(connReq.sVer, td_version, 3), &lino, _OVER);
  TAOS_CHECK_GOTO(tVerifyConnectReqSignature(&connReq), &lino, _OVER);
  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), &lino, _OVER);

  SLoginInfo li = {0};
  mndGetUserLoginInfo(user, &li);
  TAOS_CHECK_GOTO(mndCheckConnectPrivilege(pMnode, pUser, token, &li), &lino, _OVER);

  bool saslAuthed = false;
  if (connReq.saslToken[0] != 0) {
    int32_t saslCode = mndConsumeSaslToken(pMnode, user, connReq.saslToken);
    if (saslCode == 0) {
      saslAuthed = true;
    } else if (saslCode == TSDB_CODE_MND_SASL_SESSION_EXPIRED) {
      // The token was minted on a former leader and lost in a leadership change between the SASL
      // handshake and this CONNECT. Surface SESSION_EXPIRED so the client redoes the handshake against
      // the current leader, instead of falling through to verifyPassword -- which fails with
      // AUTH_FAILURE because the client cleared the password once it presented a token.
      TAOS_CHECK_GOTO(TSDB_CODE_MND_SASL_SESSION_EXPIRED, &lino, _OVER);
    }
    // any other failure (token/user mismatch) -> genuine auth failure handled by the chain below
  }

  if (saslAuthed || token != NULL || tsMndSkipGrant) {
    li.lastLoginTime= now;
    if (connReq.connType != CONN_TYPE__AUTH_TEST) {
      mndSetUserLoginInfo(user, &li);
    }
  } else if (tsForceScram && pUser->scram.algo == TSDB_SCRAM_ALGO_SHA256) {
    // Opt-in hardening (forceScram): a user provisioned with SCRAM creds may ONLY authenticate via a
    // completed SCRAM handshake, so a captured password hash cannot be replayed over legacy CONNECT.
    // Default off: SCRAM users fall through to verifyPassword so non-SCRAM clients (other platforms,
    // third-party connectors, REST) keep working.
    TAOS_CHECK_GOTO(TSDB_CODE_MND_AUTH_FAILURE, &lino, _OVER);
  } else if ((code = verifyPassword(pUser, connReq.passwd)) == TSDB_CODE_MND_AUTH_FAILURE) {
    if (pUser->failedLoginAttempts >= 0) {
      if (li.failedLoginCount >= pUser->failedLoginAttempts) {
        // if we can get here, it means the lock time has passed, so reset the counter
        li.failedLoginCount = 0;
      }
      li.failedLoginCount++;
      li.lastFailedLoginTime = now;
    }
    if (connReq.connType != CONN_TYPE__AUTH_TEST) {
      mndSetUserLoginInfo(user, &li);
    }
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  } else if (code != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  } else if (!verifyTotp(pUser, connReq.totpCode)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_WRONG_TOTP_CODE, &lino, _OVER);
  } else {
    li.failedLoginCount = 0;
    li.lastLoginTime= now;
    if (connReq.connType != CONN_TYPE__AUTH_TEST) {
      mndSetUserLoginInfo(user, &li);
    }
  }

  memset(connReq.passwd, 0, sizeof(connReq.passwd));

  if (connReq.db[0] != 0) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    (void)snprintf(db, TSDB_DB_FNAME_LEN, "%d%s%s", pUser->acctId, TS_PATH_DELIMITER, connReq.db);
    pDb = mndAcquireDb(pMnode, db);
    if (pDb == NULL) {
      if (0 != strcmp(connReq.db, TSDB_INFORMATION_SCHEMA_DB) && (0 != strcmp(connReq.db, TSDB_PERFORMANCE_SCHEMA_DB))) {
        TAOS_CHECK_GOTO(TSDB_CODE_MND_DB_NOT_EXIST, &lino, _OVER);
      }
    }
    TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, user,RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pDb), NULL, _OVER);
  }

  if (connReq.connType == CONN_TYPE__AUTH_TEST) {
    code = 0;
    goto _OVER;
  }

  pConn = mndCreateConn(pMnode, user, ti.name, connReq.connType, &pReq->info.conn.cliAddr, connReq.pid, connReq.app,
                        connReq.startTime, connReq.sVer);
  if (pConn == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  SConnectRsp connectRsp = {0};
  connectRsp.acctId = pUser->acctId;
  connectRsp.superUser = pUser->superUser;
  connectRsp.sysInfo = pUser->sysInfo;
  connectRsp.minSecLevel = pUser->minSecLevel;
  connectRsp.maxSecLevel = pUser->maxSecLevel;
  connectRsp.sodInitial = (pMnode->sodPhase == TSDB_SOD_PHASE_INITIAL ? 1 : 0);
  connectRsp.macActive = (pMnode->macActive == MAC_MODE_MANDATORY ? 1 : 0);
  connectRsp.clusterId = pMnode->clusterId;
  connectRsp.connId = pConn->id;
  connectRsp.connType = connReq.connType;
  connectRsp.dnodeNum = mndGetDnodeSize(pMnode);
  connectRsp.svrTimestamp = taosGetTimestampSec();
  connectRsp.passVer = pUser->passVersion;
  connectRsp.authVer = pUser->authVersion;
  connectRsp.monitorParas.tsEnableMonitor = tsEnableMonitor;
  connectRsp.monitorParas.tsMonitorInterval = tsMonitorInterval;
  connectRsp.monitorParas.tsSlowLogScope = tsSlowLogScope;
  connectRsp.monitorParas.tsSlowLogMaxLen = tsSlowLogMaxLen;
  connectRsp.monitorParas.tsSlowLogThreshold = tsSlowLogThreshold;
  connectRsp.enableAuditDelete = tsEnableAuditDelete;
  connectRsp.enableAuditSelect = tsEnableAuditSelect;
  connectRsp.enableAuditInsert = tsEnableAuditInsert;
  connectRsp.auditLevel = tsAuditLevel;
  tstrncpy(connectRsp.monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb, TSDB_DB_NAME_LEN);
  connectRsp.whiteListVer = pUser->ipWhiteListVer;
  connectRsp.timeWhiteListVer = pUser->timeWhiteListVer;
  connectRsp.userId = pUser->uid;


  tstrncpy(connectRsp.sVer, td_version, sizeof(connectRsp.sVer));
  tstrncpy(connectRsp.user, user, sizeof(connectRsp.user));
  tstrncpy(connectRsp.tokenName, ti.name, sizeof(connectRsp.tokenName));
  (void)snprintf(connectRsp.sDetailVer, sizeof(connectRsp.sDetailVer), "ver:%s\nbuild:%s\ngitinfo:%s", td_version,
                 td_buildinfo, td_gitinfo);
  mndGetMnodeEpSet(pMnode, &connectRsp.epSet);

  int32_t contLen = tSerializeSConnectRsp(NULL, 0, &connectRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(contLen, &lino, _OVER);
  }
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  contLen = tSerializeSConnectRsp(pRsp, contLen, &connectRsp);
  if (contLen < 0) {
    rpcFreeCont(pRsp);
    TAOS_CHECK_GOTO(contLen, &lino, _OVER);
  }

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

  mGDebug("user:%s, login from %s:%d, conn:%u, app:%s, db:%s", user, ip, port, pConn->id, connReq.app, connReq.db);
  code = 0;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    char    detail[1000] = {0};
    int32_t nBytes = snprintf(detail, sizeof(detail), "app:%s", connReq.app);
    if ((uint32_t)nBytes < sizeof(detail)) {
      double duration = (taosGetTimestampMs() - tss) / 1000.0;
      auditRecord(pReq, pMnode->clusterId, "login", "", "", detail, strlen(detail), duration, 0);
    } else {
      mError("failed to audit logic since %s", tstrerror(TSDB_CODE_OUT_OF_RANGE));
    }
  }

_OVER:
  if (code != 0) {
    mGError("user:%s, failed to login from %s since %s, line:%d, db:%s", user, ip, tstrerror(code), lino, connReq.db);
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseDb(pMnode, pDb);
  mndReleaseConn(pMnode, pConn, true);

  TAOS_RETURN(code);
}



static int32_t mndSaveQueryList(SConnObj *pConn, SQueryHbReqBasic *pBasic) {
  taosWLockLatch(&pConn->queryLock);

  taosArrayDestroyEx(pConn->pQueries, tFreeClientHbQueryDesc);

  pConn->pQueries = pBasic->queryDesc;
  pConn->numOfQueries = pBasic->queryDesc ? taosArrayGetSize(pBasic->queryDesc) : 0;
  pBasic->queryDesc = NULL;

  mDebug("queries updated in conn %u, num:%d", pConn->id, pConn->numOfQueries);

  taosWUnLockLatch(&pConn->queryLock);

  return TSDB_CODE_SUCCESS;
}

static SAppObj *mndCreateApp(SMnode *pMnode, const SIpAddr *pAddr, const SAppHbReq *pReq) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SAppObj app;
  app.appId = pReq->appId;
  app.cliAddr = *pAddr;
  app.pid = pReq->pid;
  tstrncpy(app.name, pReq->name, sizeof(app.name));
  app.startTime = pReq->startTime;
  (void)memcpy(&app.summary, &pReq->summary, sizeof(pReq->summary));
  app.lastAccessTimeMs = taosGetTimestampMs();

  SAppObj *pApp =
      taosCachePut(pMgmt->appCache, &pReq->appId, sizeof(pReq->appId), &app, sizeof(app), CACHE_OBJ_KEEP_TIME * 1000);
  if (pApp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to app %" PRIx64 " into cache since %s", pReq->appId, terrstr());
    return NULL;
  }

  mTrace("app %" PRIx64 " is put into cache", pReq->appId);
  return pApp;
}

static void mndFreeApp(SAppObj *pApp) { mTrace("app %" PRIx64 " is destroyed", pApp->appId); }

static SAppObj *mndAcquireApp(SMnode *pMnode, int64_t appId) {
  terrno = 0;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SAppObj *pApp = taosCacheAcquireByKey(pMgmt->appCache, &appId, sizeof(appId));
  if (pApp == NULL) {
    mDebug("app %" PRIx64 " not in cache", appId);
    return NULL;
  }

  pApp->lastAccessTimeMs = (uint64_t)taosGetTimestampMs();

  mTrace("app %" PRIx64 " acquired from cache", appId);
  return pApp;
}

static void mndReleaseApp(SMnode *pMnode, SAppObj *pApp) {
  if (pApp == NULL) return;
  mTrace("release app %" PRIx64 " to cache", pApp->appId);

  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  taosCacheRelease(pMgmt->appCache, (void **)&pApp, false);
}

SAppObj *mndGetNextApp(SMnode *pMnode, SCacheIter *pIter) {
  SAppObj *pApp = NULL;
  bool     hasNext = taosCacheIterNext(pIter);
  if (hasNext) {
    size_t dataLen = 0;
    pApp = taosCacheIterGetData(pIter, &dataLen);
  } else {
    taosCacheDestroyIter(pIter);
  }

  return pApp;
}

static void mndCancelGetNextApp(SMnode *pMnode, void *pIter) {
  if (pIter != NULL) {
    taosCacheDestroyIter(pIter);
  }
}

static SClientHbRsp *mndMqHbBuildRsp(SMnode *pMnode, SClientHbReq *pReq) {
  //
  return NULL;
}

static int32_t mndUpdateAppInfo(SMnode *pMnode, SClientHbReq *pHbReq, const SRpcConnInfo *connInfo) {
  int32_t    code = 0;
  SAppHbReq *pReq = &pHbReq->app;
  SAppObj   *pApp = mndAcquireApp(pMnode, pReq->appId);
  if (pApp == NULL) {
    pApp = mndCreateApp(pMnode, &connInfo->cliAddr, pReq);
    if (pApp == NULL) {
      mError("failed to create new app %" PRIx64 " since %s", pReq->appId, terrstr());
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    } else {
      mDebug("a new app %" PRIx64 " is created", pReq->appId);
      mndReleaseApp(pMnode, pApp);
      return TSDB_CODE_SUCCESS;
    }
  }

  (void)memcpy(&pApp->summary, &pReq->summary, sizeof(pReq->summary));

  mndReleaseApp(pMnode, pApp);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndGetOnlineDnodeNum(SMnode *pMnode, int32_t *num) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  int64_t    curMs = taosGetTimestampMs();
  void      *pIter = NULL;

  while (true) {
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    bool online = mndIsDnodeOnline(pDnode, curMs);
    if (online) {
      (*num)++;
    }

    sdbRelease(pSdb, pDnode);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessQueryHeartBeat(SMnode *pMnode, SRpcMsg *pMsg, SClientHbReq *pHbReq,
                                        SClientHbBatchRsp *pBatchRsp, SConnPreparedObj *pObj) {
  int32_t       code = 0;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  SClientHbRsp  hbRsp = {.connKey = pHbReq->connKey, .status = 0, .info = NULL, .query = NULL};

  if (0 != pHbReq->app.appId) {
    TAOS_CHECK_RETURN(mndUpdateAppInfo(pMnode, pHbReq, &pMsg->info.conn));
  }

  if (pHbReq->query) {
    SQueryHbReqBasic *pBasic = pHbReq->query;
    SConnObj *pConn = mndAcquireConn(pMnode, pBasic->connId);
    if (pConn == NULL) {
      SRpcConnInfo  connInfo = pMsg->info.conn;
      const char* user = pHbReq->user;
      pConn = mndCreateConn(pMnode, user, pHbReq->tokenName, pHbReq->connKey.connType, &connInfo.cliAddr, pHbReq->app.pid,
                            pHbReq->app.name, 0, pHbReq->sVer);
      if (pConn == NULL) {
        mError("user:%s, conn:%u is freed and failed to create new since %s", user, pBasic->connId, terrstr());
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      } else {
        mDebug("user:%s, conn:%u is freed, will create a new conn:%u", user, pBasic->connId, pConn->id);
      }
    }

    setUserInfo2Conn(pConn, pHbReq->userApp, pHbReq->userIp, pHbReq->cInfo);
    setUserInfoIpToConn(pConn, &pHbReq->userDualIp);

    SQueryHbRspBasic *rspBasic = taosMemoryCalloc(1, sizeof(SQueryHbRspBasic));
    if (rspBasic == NULL) {
      mndReleaseConn(pMnode, pConn, true);
      code = terrno;
      mError("user:%s, conn:%u failed to process hb while since %s", pConn->user, pBasic->connId, terrstr());
      TAOS_RETURN(code);
    }

    TAOS_CHECK_RETURN(mndSaveQueryList(pConn, pBasic));
    if (pConn->killed != 0) {
      rspBasic->killConnection = 1;
    }

    // kick stale clients: if the user's password changed after this client authenticated
    // (client-reported passVer < current passVersion), force it to reconnect. Token
    // connections use independent credentials, so they are not affected.
    if (pHbReq->passVer >= 0 && pHbReq->tokenName[0] == 0) {
      const char *hbUser = RPC_MSG_USER(pMsg);
      if (hbUser != NULL && hbUser[0] != 0) {
        SUserObj *pChkUser = NULL;
        if (mndAcquireUser(pMnode, hbUser, &pChkUser) == 0 && pChkUser != NULL) {
          if (pHbReq->passVer < pChkUser->passVersion) {
            rspBasic->killConnection = HB_KILL_CONN_AUTH;
            
            mInfo("user:%s, conn:%u killed by hb, client passVer:%d < server passVer:%d", hbUser, pConn->id,
                  pHbReq->passVer, pChkUser->passVersion);
          }
          mndReleaseUser(pMnode, pChkUser);
        }
      }
    }

    if (pConn->killId != 0) {
      rspBasic->killRid = pConn->killId;
      pConn->killId = 0;
    }

    rspBasic->connId = pConn->id;
    rspBasic->connId = pConn->id;
    rspBasic->totalDnodes = pObj->totalDnodes;
    rspBasic->onlineDnodes = pObj->onlineDnodes;
    rspBasic->epSet = pObj->epSet;
    rspBasic->pQnodeList = taosArrayDup(pObj->pQnodeList, NULL);

    mndReleaseConn(pMnode, pConn, true);

    hbRsp.query = rspBasic;
  } else {
    mDebug("no query info in hb msg");
  }

  int32_t kvNum = taosHashGetSize(pHbReq->info);
  if (NULL == pHbReq->info || kvNum <= 0) {
    if (taosArrayPush(pBatchRsp->rsps, &hbRsp) == NULL) {
      mError("failed to put rsp into array, but continue at this heartbeat");
    }
    return TSDB_CODE_SUCCESS;
  }

  hbRsp.info = taosArrayInit(kvNum, sizeof(SKv));
  if (NULL == hbRsp.info) {
    mError("taosArrayInit %d rsp kv failed", kvNum);
    code = terrno;
    tFreeClientHbRsp(&hbRsp);
    TAOS_RETURN(code);
  }

#ifdef TD_ENTERPRISE
  bool             needCheck = true;
  int32_t          key = HEARTBEAT_KEY_DYN_VIEW;
  SDynViewVersion *pDynViewVer = NULL;
  SKv             *pKv = taosHashGet(pHbReq->info, &key, sizeof(key));
  if (NULL != pKv) {
    pDynViewVer = pKv->value;
    mTrace("recv view dyn ver, bootTs:%" PRId64 ", ver:%" PRIu64, pDynViewVer->svrBootTs, pDynViewVer->dynViewVer);

    SDynViewVersion *pRspVer = NULL;
    if (0 != (code = mndValidateDynViewVersion(pMnode, pDynViewVer, &needCheck, &pRspVer))) {
      TAOS_RETURN(code);
    }

    if (needCheck) {
      SKv kv1 = {.key = HEARTBEAT_KEY_DYN_VIEW, .valueLen = sizeof(*pDynViewVer), .value = pRspVer};
      if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      };
      mTrace("need to check view ver, lastest bootTs:%" PRId64 ", ver:%" PRIu64, pRspVer->svrBootTs,
             pRspVer->dynViewVer);
    }
  }
#endif

  void *pIter = taosHashIterate(pHbReq->info, NULL);
  while (pIter != NULL) {
    SKv *kv = pIter;

    switch (kv->key) {
      case HEARTBEAT_KEY_USER_AUTHINFO: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateUserAuthInfo(pMnode, kv->value, kv->valueLen / sizeof(SUserAuthVersion), &rspMsg, &rspLen,
                                      pObj->ipWhiteListVer);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_USER_AUTHINFO, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
      case HEARTBEAT_KEY_DBINFO: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateDbInfo(pMnode, kv->value, kv->valueLen / sizeof(SDbCacheInfo), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_DBINFO, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
      case HEARTBEAT_KEY_STBINFO: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateStbInfo(pMnode, kv->value, kv->valueLen / sizeof(SSTableVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_STBINFO, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
#ifdef TD_ENTERPRISE
      case HEARTBEAT_KEY_DYN_VIEW: {
        break;
      }
      case HEARTBEAT_KEY_VIEWINFO: {
        if (!needCheck) {
          break;
        }

        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateViewInfo(pMnode, kv->value, kv->valueLen / sizeof(SViewVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_VIEWINFO, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
#endif
      case HEARTBEAT_KEY_TSMA: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateTSMAInfo(pMnode, kv->value, kv->valueLen / sizeof(STSMAVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv = {.key = HEARTBEAT_KEY_TSMA, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
      case HEARTBEAT_KEY_TXN_KEEPALIVE: {
        if (kv->value != NULL && kv->valueLen >= (int32_t)sizeof(txn_id_t)) {
          txn_id_t txnId = *(txn_id_t *)kv->value;
          if (txnId > 0) {
            mndTxnRefreshKeepalive(pMnode, txnId);
            // Notify the client if this txn was forcibly rolled back due to timeout.
            // The client will transition to UTXN_STAGE_TIMEOUT_KILLED and stop keepalive.
            if (mndTxnIsTimeoutKilled(pMnode, txnId)) {
              txn_id_t *pKilledId = (txn_id_t *)taosMemoryMalloc(sizeof(txn_id_t));
              if (pKilledId != NULL) {
                *pKilledId = txnId;
                SKv killedKv = {
                    .key = HEARTBEAT_KEY_TXN_KILLED, .valueLen = (int32_t)sizeof(txn_id_t), .value = pKilledId};
                if (taosArrayPush(hbRsp.info, &killedKv) == NULL) {
                  taosMemoryFree(pKilledId);
                  mWarn("txn:%" PRIi64 ", failed to push TXN_KILLED kv into hbRsp", txnId);
                } else {
                  mInfo("txn:%" PRIi64 ", notifying client of timeout rollback via HEARTBEAT_KEY_TXN_KILLED", txnId);
                }
              }
            }
          }
        }
        break;
      }
#ifdef TD_ENTERPRISE
      case HEARTBEAT_KEY_EXTSOURCE: {
        if (!needCheck) { break; }
        if (kv->valueLen != sizeof(int64_t)) {
          mError("invalid HEARTBEAT_KEY_EXTSOURCE kv len:%d, expected 8", kv->valueLen);
          break;
        }
        int64_t clientGlobalVer = (int64_t)be64toh(*(uint64_t *)kv->value);
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        (void)mndValidateExtSourceInfo(pMnode, clientGlobalVer, &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_EXTSOURCE, .valueLen = rspLen, .value = rspMsg};
          if (taosArrayPush(hbRsp.info, &kv1) == NULL) {
            mError("failed to put kv into array, but continue at this heartbeat");
          }
        }
        break;
      }
#endif
      default:
        mError("invalid kv key:%d", kv->key);
        hbRsp.status = TSDB_CODE_APP_ERROR;
        break;
    }

    pIter = taosHashIterate(pHbReq->info, pIter);
  }

  if (taosArrayPush(pBatchRsp->rsps, &hbRsp) == NULL) {
    if (terrno != 0) code = terrno;
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessHeartBeatReq(SRpcMsg *pReq) {
  int32_t code = 0;
  int32_t lino = 0;
  SMnode *pMnode = pReq->info.node;

  SClientHbBatchReq batchReq = {0};
  if (tDeserializeSClientHbBatchReq(pReq->pCont, pReq->contLen, &batchReq) != 0) {
    taosArrayDestroyEx(batchReq.reqs, tFreeClientHbReq);
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  SConnPreparedObj obj = {0};
  obj.totalDnodes = mndGetDnodeSize(pMnode);
  obj.ipWhiteListVer = batchReq.ipWhiteListVer;
  TAOS_CHECK_RETURN(mndGetOnlineDnodeNum(pMnode, &obj.onlineDnodes));
  mndGetMnodeEpSet(pMnode, &obj.epSet);
  TAOS_CHECK_RETURN(mndCreateQnodeList(pMnode, &obj.pQnodeList, -1));

  SClientHbBatchRsp batchRsp = {0};
  batchRsp.svrTimestamp = taosGetTimestampSec();
  batchRsp.rsps = taosArrayInit(0, sizeof(SClientHbRsp));
  if (batchRsp.rsps == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  batchRsp.monitorParas.tsEnableMonitor = tsEnableMonitor;
  batchRsp.monitorParas.tsMonitorInterval = tsMonitorInterval;
  batchRsp.monitorParas.tsSlowLogThreshold = tsSlowLogThreshold;
  tstrncpy(batchRsp.monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb, TSDB_DB_NAME_LEN);
  batchRsp.monitorParas.tsSlowLogMaxLen = tsSlowLogMaxLen;
  batchRsp.monitorParas.tsSlowLogScope = tsSlowLogScope;
  batchRsp.enableAuditDelete = tsEnableAuditDelete;
  batchRsp.enableAuditSelect = tsEnableAuditSelect;
  batchRsp.enableAuditInsert = tsEnableAuditInsert;
  batchRsp.auditLevel = tsAuditLevel;
  batchRsp.enableStrongPass = tsEnableStrongPassword;
  batchRsp.sodInitial = (pMnode->sodPhase == TSDB_SOD_PHASE_INITIAL ? 1 : 0);
  batchRsp.macActive = (pMnode->macActive == MAC_MODE_MANDATORY ? 1 : 0);

  int32_t sz = taosArrayGetSize(batchReq.reqs);
  for (int i = 0; i < sz; i++) {
    SClientHbReq *pHbReq = taosArrayGet(batchReq.reqs, i);
    if (pHbReq->connKey.connType == CONN_TYPE__QUERY || pHbReq->connKey.connType == CONN_TYPE__TMQ) {
      TAOS_CHECK_EXIT(mndProcessQueryHeartBeat(pMnode, pReq, pHbReq, &batchRsp, &obj));
    } 
  }
  taosArrayDestroyEx(batchReq.reqs, tFreeClientHbReq);

  int32_t tlen = tSerializeSClientHbBatchRsp(NULL, 0, &batchRsp);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }
  void *buf = rpcMallocCont(tlen);
  if (!buf) {
    TAOS_CHECK_EXIT(terrno);
  }
  tlen = tSerializeSClientHbBatchRsp(buf, tlen, &batchRsp);
  if (tlen < 0) {
    rpcFreeCont(buf);
    TAOS_CHECK_EXIT(tlen);
  }
  pReq->info.rspLen = tlen;
  pReq->info.rsp = buf;
_exit:
  tFreeClientHbBatchRsp(&batchRsp);

  taosArrayDestroy(obj.pQnodeList);

  TAOS_RETURN(code);
}

static int32_t mndProcessKillQueryReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SKillQueryReq killReq = {0};
  TAOS_CHECK_RETURN(tDeserializeSKillQueryReq(pReq->pCont, pReq->contLen, &killReq));

  mInfo("kill query msg is received, queryId:%s", killReq.queryStrId);
  TAOS_CHECK_RETURN(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_KILL_QUERY));
  int32_t  connId = 0;
  uint64_t queryId = 0;
  char    *p = strchr(killReq.queryStrId, ':');
  if (NULL == p) {
    mError("invalid QID:%s", killReq.queryStrId);
    code = TSDB_CODE_MND_INVALID_QUERY_ID;
    TAOS_RETURN(code);
  }
  *p = 0;
  connId = taosStr2Int32(killReq.queryStrId, NULL, 16);
  queryId = taosStr2UInt64(p + 1, NULL, 16);

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%x, failed to kill queryId:%" PRIx64 ", conn not exist", connId, queryId);
    code = TSDB_CODE_MND_INVALID_CONN_ID;
    TAOS_RETURN(code);
  } else {
    mInfo("connId:%x, queryId:%" PRIx64 " is killed by user:%s", connId, queryId, RPC_MSG_USER(pReq));
    pConn->killId = queryId;
    taosCacheRelease(pMgmt->connCache, (void **)&pConn, false);
    TAOS_RETURN(code);
  }
}

static int32_t mndProcessKillConnReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SKillConnReq killReq = {0};
  TAOS_CHECK_RETURN(tDeserializeSKillConnReq(pReq->pCont, pReq->contLen, &killReq));

  TAOS_CHECK_RETURN(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_KILL_CONN));

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &killReq.connId, sizeof(uint32_t));
  if (pConn == NULL) {
    mError("connId:%u, failed to kill connection, conn not exist", killReq.connId);
    code = TSDB_CODE_MND_INVALID_CONN_ID;
    TAOS_RETURN(code);
  } else {
    mInfo("connId:%u, is killed by user:%s", killReq.connId, RPC_MSG_USER(pReq));
    pConn->killed = 1;
    taosCacheRelease(pMgmt->connCache, (void **)&pConn, false);
    TAOS_RETURN(code);
  }
}

static int32_t mndProcessSvrVerReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SServerVerRsp rsp = {0};
  tstrncpy(rsp.ver, td_version, sizeof(rsp.ver));

  int32_t contLen = tSerializeSServerVerRsp(NULL, 0, &rsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  contLen = tSerializeSServerVerRsp(pRsp, contLen, &rsp);
  if (contLen < 0) {
    rpcFreeCont(pRsp);
    TAOS_CHECK_EXIT(contLen);
  }

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

_exit:

  TAOS_RETURN(code);
}

static int32_t mndRetrieveConns(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  int32_t   code = 0;
  SConnObj *pConn = NULL;

  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->connCache);
    if (!pShow->pIter) return terrno;
  }

  while (numOfRows < rows) {
    pConn = mndGetNextConn(pMnode, pShow->pIter);
    if (pConn == NULL) {
      pShow->pIter = NULL;
      break;
    }

    if ((taosGetTimestampMs() - pConn->lastAccessTimeMs) > ((int64_t)CACHE_OBJ_KEEP_TIME * 1000)) {
      continue;
    }

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->id, false);
    if (code != 0) {
      mError("failed to set conn id:%u since %s", pConn->id, tstrerror(code));
      return code;
    }

    char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(user, pConn->user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)user, false);
    if (code != 0) {
      mError("failed to set user since %s", tstrerror(code));
      return code;
    }

    char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(app, pConn->app);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)app, false);
    if (code != 0) {
      mError("failed to set app since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->pid, false);
    if (code != 0) {
      mError("failed to set conn id:%u since %s", pConn->id, tstrerror(code));
      return code;
    }

    char addr[IP_RESERVE_CAP] = {0};
    char endpoint[TD_IP_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    if (snprintf(addr, sizeof(addr), "%s:%d", IP_ADDR_STR(&pConn->addr), pConn->addr.port) >= sizeof(addr)) {
      code = TSDB_CODE_OUT_OF_RANGE;
      mError("failed to set endpoint since %s", tstrerror(code));
      return code;
    }

    STR_TO_VARSTR(endpoint, addr);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)endpoint, false);
    if (code != 0) {
      mError("failed to set endpoint since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->loginTimeMs, false);
    if (code != 0) {
      mError("failed to set login time since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->lastAccessTimeMs, false);
    if (code != 0) {
      mError("failed to set last access time since %s", tstrerror(code));
      return code;
    }

    char userApp[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(userApp, pConn->userApp);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)userApp, false);
    if (code != 0) {
      mError("failed to set user app since %s", tstrerror(code));
      return code;
    }

    char userIp[TD_IP_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    getUserIpFromConnObj(pConn, userIp);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)userIp, false);
    if (code != 0) {
      mError("failed to set user ip since %s", tstrerror(code));
      return code;
    }

    char ver[TSDB_VERSION_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(ver, pConn->sVer);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)ver, false);
    if (code != 0) {
      mError("failed to set ver since %s", tstrerror(code));
      return code;
    }

    char cInfo[CONNECTOR_INFO_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(cInfo, pConn->cInfo);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)cInfo, false);
    if (code != 0) {
      mError("failed to set connector info since %s", tstrerror(code));
      return code;
    }

    char type[16 + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(type, pConn->connType == CONN_TYPE__QUERY ? "QUERY" : (pConn->connType == CONN_TYPE__TMQ ? "TMQ" : "UNKNOWN"));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)type, false);
    if (code != 0) {
      mError("failed to set type info since %s", tstrerror(code));
      return code;
    }

    char tokenName[TSDB_TOKEN_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tokenName, pConn->tokenName);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)tokenName, false);
    if (code != 0) {
      mError("failed to set token name since %s", tstrerror(code));
      return code;
    }

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

/**
 * @param pConn the conn queries pack from
 * @param[out] pBlock the block data packed into
 * @param offset skip [offset] queries in pConn
 * @param rowsToPack at most rows to pack
 * @return rows packed
 */
static int32_t packQueriesIntoBlock(SShowObj *pShow, SConnObj *pConn, SSDataBlock *pBlock, uint32_t offset,
                                    uint32_t rowsToPack) {
  int32_t cols = 0;
  int32_t code = 0;
  taosRLockLatch(&pConn->queryLock);
  int32_t numOfQueries = taosArrayGetSize(pConn->pQueries);
  if (NULL == pConn->pQueries || numOfQueries <= offset) {
    taosRUnLockLatch(&pConn->queryLock);
    return 0;
  }

  int32_t i = offset;
  for (; i < numOfQueries && (i - offset) < rowsToPack; ++i) {
    int32_t     curRowIndex = pBlock->info.rows;
    SQueryDesc *pQuery = taosArrayGet(pConn->pQueries, i);
    cols = 0;

    char queryId[26 + VARSTR_HEADER_SIZE] = {0};
    (void)snprintf(&queryId[VARSTR_HEADER_SIZE], sizeof(queryId) - VARSTR_HEADER_SIZE, "%x:%" PRIx64, pConn->id,
                   pQuery->reqRid);
    varDataLen(queryId) = strlen(&queryId[VARSTR_HEADER_SIZE]);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)queryId, false);
    if (code != 0) {
      mError("failed to set query id:%s since %s", queryId, tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->queryId, false);
    if (code != 0) {
      mError("failed to set query id:%" PRIx64 " since %s", pQuery->queryId, tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pConn->id, false);
    if (code != 0) {
      mError("failed to set conn id:%u since %s", pConn->id, tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(app, pConn->app);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)app, false);
    if (code != 0) {
      mError("failed to set app since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pConn->pid, false);
    if (code != 0) {
      mError("failed to set conn id:%u since %s", pConn->id, tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(user, pConn->user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)user, false);
    if (code != 0) {
      mError("failed to set user since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char endpoint[TD_IP_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    char buf[IP_RESERVE_CAP] = {0};
    (void)snprintf(buf, sizeof(buf), "%s:%d", IP_ADDR_STR(&pConn->addr), pConn->addr.port);
    STR_TO_VARSTR(endpoint, buf);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)endpoint, false);
    if (code != 0) {
      mError("failed to set endpoint since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->stime, false);
    if (code != 0) {
      mError("failed to set start time since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->useconds, false);
    if (code != 0) {
      mError("failed to set useconds since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->stableQuery, false);
    if (code != 0) {
      mError("failed to set stable query since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->isSubQuery, false);
    if (code != 0) {
      mError("failed to set sub query since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->subPlanNum, false);
    if (code != 0) {
      mError("failed to set sub plan num since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char    subStatus[TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE] = {0};
    int64_t reserve = 128;
    int32_t strSize = sizeof(subStatus);
    int32_t offset = VARSTR_HEADER_SIZE;
    for (int32_t i = 0; i < pQuery->subPlanNum && offset + reserve < strSize; ++i) {
      if (i) {
        offset += snprintf(subStatus + offset, sizeof(subStatus) - offset, ",");
      }
      if (offset + reserve >= strSize) break;

      SQuerySubDesc *pDesc = taosArrayGet(pQuery->subDesc, i);
      if (NULL == pDesc) break;

      char startBuf[32] = {0};
      (void)snprintf(startBuf, sizeof(startBuf), "-");
      if (pDesc->startTs > 0) {
        time_t    startSec = (time_t)(pDesc->startTs / 1000000);
        int32_t   startFrac = (int32_t)(pDesc->startTs % 1000000) / 1000;
        struct tm startTm;
        if (taosLocalTime(&startSec, &startTm, NULL, 0, NULL) != NULL) {
          size_t n = taosStrfTime(startBuf, sizeof(startBuf), "%Y-%m-%d %H:%M:%S", &startTm);
          if (tsnprintf(startBuf + n, sizeof(startBuf) - n, ".%03d", startFrac) < 0) {
            mError("failed to format start time for sub query since %s", tstrerror(terrno));
            code = terrno;
            taosRUnLockLatch(&pConn->queryLock);
            return code;
          }
        }
      }

      offset += tsnprintf(subStatus + offset, sizeof(subStatus) - offset,
                          "%" PRIu64 ":%s:%s", pDesc->tid, pDesc->status, startBuf);
    }
    varDataLen(subStatus) = strlen(&subStatus[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, subStatus, (varDataLen(subStatus) == 0) ? true : false);
    if (code != 0) {
      mError("failed to set sub status since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(sql, pQuery->sql);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)sql, false);
    if (code != 0) {
      mError("failed to set sql since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char userApp[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(userApp, pConn->userApp);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)userApp, false);
    if (code != 0) {
      mError("failed to set user app since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    char userIp[TD_IP_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    getUserIpFromConnObj(pConn, userIp);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)userIp, false);
    if (code != 0) {
      mError("failed to set user ip since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    const char* phaseStr = queryPhaseStr(pQuery->execPhase);
    char        phaseVarStr[64 + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(phaseVarStr, phaseStr);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)phaseVarStr, false);
    if (code != 0) {
      mError("failed to set current phase since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->phaseStartTime, false);
    if (code != 0) {
      mError("failed to set phase start time since %s", tstrerror(code));
      taosRUnLockLatch(&pConn->queryLock);
      return code;
    }

    pBlock->info.rows++;
  }

  taosRUnLockLatch(&pConn->queryLock);
  return i - offset;
}

static int32_t mndRetrieveQueries(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SConnObj *pConn = NULL;

  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->connCache);
    if (!pShow->pIter) return terrno;
  }

  // means fetched some data last time for this conn
  if (pShow->curIterPackedRows > 0) {
    size_t len = 0;
    pConn = taosCacheIterGetData(pShow->pIter, &len);
    if (pConn && (taosArrayGetSize(pConn->pQueries) > pShow->curIterPackedRows)) {
      numOfRows = packQueriesIntoBlock(pShow, pConn, pBlock, pShow->curIterPackedRows, rows);
      pShow->curIterPackedRows += numOfRows;
    }
  }

  while (numOfRows < rows) {
    pConn = mndGetNextConn(pMnode, pShow->pIter);
    if (pConn == NULL) {
      pShow->pIter = NULL;
      break;
    }

    int32_t packedRows = packQueriesIntoBlock(pShow, pConn, pBlock, 0, rows - numOfRows);
    pShow->curIterPackedRows = packedRows;
    numOfRows += packedRows;
  }
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static int32_t mndRetrieveApps(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  int32_t  cols = 0;
  SAppObj *pApp = NULL;
  int32_t  code = 0;

  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->appCache);
    if (!pShow->pIter) return terrno;
  }

  while (numOfRows < rows) {
    pApp = mndGetNextApp(pMnode, pShow->pIter);
    if (pApp == NULL) {
      pShow->pIter = NULL;
      break;
    }

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->appId, false);
    if (code != 0) {
      mError("failed to set app id since %s", tstrerror(code));
      return code;
    }

    char ip[TD_IP_LEN + VARSTR_HEADER_SIZE] = {0};
    char buf[IP_RESERVE_CAP] = {0};
    snprintf(buf, sizeof(buf), "%s", IP_ADDR_STR(&pApp->cliAddr));
    STR_TO_VARSTR(ip, buf);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)ip, false);
    if (code != 0) {
      mError("failed to set ip since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->pid, false);
    if (code != 0) {
      mError("failed to set pid since %s", tstrerror(code));
      return code;
    }

    char name[TSDB_APP_NAME_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    (void)snprintf(&name[VARSTR_HEADER_SIZE], sizeof(name) - VARSTR_HEADER_SIZE, "%s", pApp->name);
    varDataLen(name) = strlen(&name[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)name, false);
    if (code != 0) {
      mError("failed to set app name since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->startTime, false);
    if (code != 0) {
      mError("failed to set start time since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfInsertsReq, false);
    if (code != 0) {
      mError("failed to set insert req since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfInsertRows, false);
    if (code != 0) {
      mError("failed to set insert rows since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.insertElapsedTime, false);
    if (code != 0) {
      mError("failed to set insert elapsed time since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.insertBytes, false);
    if (code != 0) {
      mError("failed to set insert bytes since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.fetchBytes, false);
    if (code != 0) {
      mError("failed to set fetch bytes since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.queryElapsedTime, false);
    if (code != 0) {
      mError("failed to set query elapsed time since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfSlowQueries, false);
    if (code != 0) {
      mError("failed to set slow queries since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.totalRequests, false);
    if (code != 0) {
      mError("failed to set total requests since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.currentRequests, false);
    if (code != 0) {
      mError("failed to set current requests since %s", tstrerror(code));
      return code;
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->lastAccessTimeMs, false);
    if (code != 0) {
      mError("failed to set last access time since %s", tstrerror(code));
      return code;
    }

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextQuery(SMnode *pMnode, void *pIter) {
  if (pIter != NULL) {
    taosCacheDestroyIter(pIter);
  }
}

int32_t mndGetNumOfConnections(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  return taosCacheGetNumOfObj(pMgmt->connCache);
}
