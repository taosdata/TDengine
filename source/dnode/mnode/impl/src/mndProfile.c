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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndUser.h"
#include "mndView.h"
#include "tglobal.h"
#include "tversion.h"

typedef struct {
  uint32_t id;
  int8_t   connType;
  char     user[TSDB_USER_LEN];
  char     app[TSDB_APP_NAME_LEN];  // app name that invokes taosc
  int64_t  appStartTimeMs;          // app start time
  int32_t  pid;                     // pid of app that invokes taosc
  uint32_t ip;
  uint16_t port;
  int8_t   killed;
  int64_t  loginTimeMs;
  int64_t  lastAccessTimeMs;
  uint64_t killId;
  int32_t  numOfQueries;
  SRWLatch queryLock;
  SArray  *pQueries;  // SArray<SQueryDesc>
} SConnObj;

typedef struct {
  int64_t            appId;
  uint32_t           ip;
  int32_t            pid;
  char               name[TSDB_APP_NAME_LEN];
  int64_t            startTime;
  SAppClusterSummary summary;
  int64_t            lastAccessTimeMs;
} SAppObj;

static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, int8_t connType, uint32_t ip, uint16_t port,
                               int32_t pid, const char *app, int64_t startTime);
static void      mndFreeConn(SConnObj *pConn);
static SConnObj *mndAcquireConn(SMnode *pMnode, uint32_t connId);
static void      mndReleaseConn(SMnode *pMnode, SConnObj *pConn, bool extendLifespan);
static void     *mndGetNextConn(SMnode *pMnode, SCacheIter *pIter);
static void      mndCancelGetNextConn(SMnode *pMnode, void *pIter);
static int32_t   mndProcessHeartBeatReq(SRpcMsg *pReq);
static int32_t   mndProcessConnectReq(SRpcMsg *pReq);
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
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  // in ms
  int32_t checkTime = tsShellActivityTimer * 2 * 1000;
  pMgmt->connCache = taosCacheInit(TSDB_DATA_TYPE_UINT, checkTime, false, (__cache_free_fn_t)mndFreeConn, "conn");
  if (pMgmt->connCache == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc profile cache since %s", terrstr());
    return -1;
  }

  pMgmt->appCache = taosCacheInit(TSDB_DATA_TYPE_BIGINT, checkTime, true, (__cache_free_fn_t)mndFreeApp, "app");
  if (pMgmt->appCache == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc profile cache since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_MND_HEARTBEAT, mndProcessHeartBeatReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONNECT, mndProcessConnectReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_QUERY, mndProcessKillQueryReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_CONN, mndProcessKillConnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SERVER_VERSION, mndProcessSvrVerReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndRetrieveConns);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndCancelGetNextConn);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndRetrieveQueries);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndCancelGetNextQuery);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_APPS, mndRetrieveApps);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_APPS, mndCancelGetNextApp);

  return 0;
}

void mndCleanupProfile(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  if (pMgmt->connCache != NULL) {
    taosCacheCleanup(pMgmt->connCache);
    pMgmt->connCache = NULL;
  }

  if (pMgmt->appCache != NULL) {
    taosCacheCleanup(pMgmt->appCache);
    pMgmt->appCache = NULL;
  }
}

static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, int8_t connType, uint32_t ip, uint16_t port,
                               int32_t pid, const char *app, int64_t startTime) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  char     connStr[255] = {0};
  int32_t  len = snprintf(connStr, sizeof(connStr), "%s%d%d%d%s", user, ip, port, pid, app);
  uint32_t connId = mndGenerateUid(connStr, len);
  if (startTime == 0) startTime = taosGetTimestampMs();

  SConnObj connObj = {
      .id = connId,
      .connType = connType,
      .appStartTimeMs = startTime,
      .pid = pid,
      .ip = ip,
      .port = port,
      .killed = 0,
      .loginTimeMs = taosGetTimestampMs(),
      .lastAccessTimeMs = 0,
      .killId = 0,
      .numOfQueries = 0,
      .pQueries = NULL,
  };

  connObj.lastAccessTimeMs = connObj.loginTimeMs;
  tstrncpy(connObj.user, user, TSDB_USER_LEN);
  tstrncpy(connObj.app, app, TSDB_APP_NAME_LEN);

  int32_t   keepTime = tsShellActivityTimer * 3;
  SConnObj *pConn =
      taosCachePut(pMgmt->connCache, &connId, sizeof(uint32_t), &connObj, sizeof(connObj), keepTime * 1000);
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

static int32_t mndProcessConnectReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  SUserObj       *pUser = NULL;
  SDbObj         *pDb = NULL;
  SConnObj       *pConn = NULL;
  int32_t         code = -1;
  SConnectReq     connReq = {0};
  char            ip[24] = {0};
  const STraceId *trace = &pReq->info.traceId;

  if ((code = tDeserializeSConnectReq(pReq->pCont, pReq->contLen, &connReq)) != 0) {
    terrno = (-1 == code ? TSDB_CODE_INVALID_MSG : code);
    goto _OVER;
  }

  if ((code = taosCheckVersionCompatibleFromStr(connReq.sVer, version, 3)) != 0) {
    mGError("version not compatible. client version: %s, server version: %s", connReq.sVer, version);
    terrno = code;
    goto _OVER;
  }

  code = -1;
  taosIp2String(pReq->info.conn.clientIp, ip);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONNECT) != 0) {
    mGError("user:%s, failed to login from %s since %s", pReq->info.conn.user, ip, terrstr());
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pUser == NULL) {
    mGError("user:%s, failed to login from %s while acquire user since %s", pReq->info.conn.user, ip, terrstr());
    goto _OVER;
  }

  if (strncmp(connReq.passwd, pUser->pass, TSDB_PASSWORD_LEN - 1) != 0 && !tsMndSkipGrant) {
    mGError("user:%s, failed to login from %s since invalid pass, input:%s", pReq->info.conn.user, ip, connReq.passwd);
    code = TSDB_CODE_MND_AUTH_FAILURE;
    goto _OVER;
  }

  if (connReq.db[0]) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    snprintf(db, TSDB_DB_FNAME_LEN, "%d%s%s", pUser->acctId, TS_PATH_DELIMITER, connReq.db);
    pDb = mndAcquireDb(pMnode, db);
    if (pDb == NULL) {
      if (0 != strcmp(connReq.db, TSDB_INFORMATION_SCHEMA_DB) &&
          (0 != strcmp(connReq.db, TSDB_PERFORMANCE_SCHEMA_DB))) {
        terrno = TSDB_CODE_MND_DB_NOT_EXIST;
        mGError("user:%s, failed to login from %s while use db:%s since %s", pReq->info.conn.user, ip, connReq.db,
                terrstr());
        goto _OVER;
      }
    }

    if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_OR_WRITE_DB, pDb) != 0) {
      goto _OVER;
    }
  }

_CONNECT:
  pConn = mndCreateConn(pMnode, pReq->info.conn.user, connReq.connType, pReq->info.conn.clientIp,
                        pReq->info.conn.clientPort, connReq.pid, connReq.app, connReq.startTime);
  if (pConn == NULL) {
    mGError("user:%s, failed to login from %s while create connection since %s", pReq->info.conn.user, ip, terrstr());
    goto _OVER;
  }

  SConnectRsp connectRsp = {0};
  connectRsp.acctId = pUser->acctId;
  connectRsp.superUser = pUser->superUser;
  connectRsp.sysInfo = pUser->sysInfo;
  connectRsp.clusterId = pMnode->clusterId;
  connectRsp.connId = pConn->id;
  connectRsp.connType = connReq.connType;
  connectRsp.dnodeNum = mndGetDnodeSize(pMnode);
  connectRsp.svrTimestamp = taosGetTimestampSec();
  connectRsp.passVer = pUser->passVersion;
  connectRsp.authVer = pUser->authVersion;
  connectRsp.whiteListVer = pUser->ipWhiteListVer;

  strcpy(connectRsp.sVer, version);
  snprintf(connectRsp.sDetailVer, sizeof(connectRsp.sDetailVer), "ver:%s\nbuild:%s\ngitinfo:%s", version, buildinfo,
           gitinfo);
  mndGetMnodeEpSet(pMnode, &connectRsp.epSet);

  int32_t contLen = tSerializeSConnectRsp(NULL, 0, &connectRsp);
  if (contLen < 0) goto _OVER;
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) goto _OVER;
  tSerializeSConnectRsp(pRsp, contLen, &connectRsp);

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

  mGDebug("user:%s, login from %s:%d, conn:%u, app:%s", pReq->info.conn.user, ip, pConn->port, pConn->id, connReq.app);

  code = 0;

  char detail[1000] = {0};
  sprintf(detail, "app:%s", connReq.app);

  auditRecord(pReq, pMnode->clusterId, "login", "", "", detail, strlen(detail));

_OVER:

  mndReleaseUser(pMnode, pUser);
  mndReleaseDb(pMnode, pDb);
  mndReleaseConn(pMnode, pConn, true);

  return code;
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

static SAppObj *mndCreateApp(SMnode *pMnode, uint32_t clientIp, SAppHbReq *pReq) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SAppObj app;
  app.appId = pReq->appId;
  app.ip = clientIp;
  app.pid = pReq->pid;
  strcpy(app.name, pReq->name);
  app.startTime = pReq->startTime;
  memcpy(&app.summary, &pReq->summary, sizeof(pReq->summary));
  app.lastAccessTimeMs = taosGetTimestampMs();

  const int32_t keepTime = tsShellActivityTimer * 3;
  SAppObj *pApp = taosCachePut(pMgmt->appCache, &pReq->appId, sizeof(pReq->appId), &app, sizeof(app), keepTime * 1000);
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

static int32_t mndUpdateAppInfo(SMnode *pMnode, SClientHbReq *pHbReq, SRpcConnInfo *connInfo) {
  SAppHbReq *pReq = &pHbReq->app;
  SAppObj   *pApp = mndAcquireApp(pMnode, pReq->appId);
  if (pApp == NULL) {
    pApp = mndCreateApp(pMnode, connInfo->clientIp, pReq);
    if (pApp == NULL) {
      mError("failed to create new app %" PRIx64 " since %s", pReq->appId, terrstr());
      return -1;
    } else {
      mDebug("a new app %" PRIx64 " is created", pReq->appId);
      mndReleaseApp(pMnode, pApp);
      return TSDB_CODE_SUCCESS;
    }
  }

  memcpy(&pApp->summary, &pReq->summary, sizeof(pReq->summary));

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
                                        SClientHbBatchRsp *pBatchRsp) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  SClientHbRsp  hbRsp = {.connKey = pHbReq->connKey, .status = 0, .info = NULL, .query = NULL};
  SRpcConnInfo  connInfo = pMsg->info.conn;

  if (0 != pHbReq->app.appId) {
    mndUpdateAppInfo(pMnode, pHbReq, &connInfo);
  }

  if (pHbReq->query) {
    SQueryHbReqBasic *pBasic = pHbReq->query;

    SConnObj *pConn = mndAcquireConn(pMnode, pBasic->connId);
    if (pConn == NULL) {
      pConn = mndCreateConn(pMnode, connInfo.user, CONN_TYPE__QUERY, connInfo.clientIp, connInfo.clientPort,
                            pHbReq->app.pid, pHbReq->app.name, 0);
      if (pConn == NULL) {
        mError("user:%s, conn:%u is freed and failed to create new since %s", connInfo.user, pBasic->connId, terrstr());
        return -1;
      } else {
        mDebug("user:%s, conn:%u is freed, will create a new conn:%u", connInfo.user, pBasic->connId, pConn->id);
      }
    }

    SQueryHbRspBasic *rspBasic = taosMemoryCalloc(1, sizeof(SQueryHbRspBasic));
    if (rspBasic == NULL) {
      mndReleaseConn(pMnode, pConn, true);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mError("user:%s, conn:%u failed to process hb while since %s", pConn->user, pBasic->connId, terrstr());
      return -1;
    }

    mndSaveQueryList(pConn, pBasic);
    if (pConn->killed != 0) {
      rspBasic->killConnection = 1;
    }

    if (pConn->killId != 0) {
      rspBasic->killRid = pConn->killId;
      pConn->killId = 0;
    }

    rspBasic->connId = pConn->id;
    rspBasic->totalDnodes = mndGetDnodeSize(pMnode);
    mndGetOnlineDnodeNum(pMnode, &rspBasic->onlineDnodes);
    mndGetMnodeEpSet(pMnode, &rspBasic->epSet);

    mndCreateQnodeList(pMnode, &rspBasic->pQnodeList, -1);

    mndReleaseConn(pMnode, pConn, true);

    hbRsp.query = rspBasic;
  } else {
    mDebug("no query info in hb msg");
  }

  int32_t kvNum = taosHashGetSize(pHbReq->info);
  if (NULL == pHbReq->info || kvNum <= 0) {
    taosArrayPush(pBatchRsp->rsps, &hbRsp);
    return TSDB_CODE_SUCCESS;
  }

  hbRsp.info = taosArrayInit(kvNum, sizeof(SKv));
  if (NULL == hbRsp.info) {
    mError("taosArrayInit %d rsp kv failed", kvNum);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeClientHbRsp(&hbRsp);
    return -1;
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
    if (0 != mndValidateDynViewVersion(pMnode, pDynViewVer, &needCheck, &pRspVer)) {
      return -1;
    }

    if (needCheck) {
      SKv kv1 = {.key = HEARTBEAT_KEY_DYN_VIEW, .valueLen = sizeof(*pDynViewVer), .value = pRspVer};
      taosArrayPush(hbRsp.info, &kv1);
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
        mndValidateUserAuthInfo(pMnode, kv->value, kv->valueLen / sizeof(SUserAuthVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_USER_AUTHINFO, .valueLen = rspLen, .value = rspMsg};
          taosArrayPush(hbRsp.info, &kv1);
        }
        break;
      }
      case HEARTBEAT_KEY_DBINFO: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        mndValidateDbInfo(pMnode, kv->value, kv->valueLen / sizeof(SDbCacheInfo), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_DBINFO, .valueLen = rspLen, .value = rspMsg};
          taosArrayPush(hbRsp.info, &kv1);
        }
        break;
      }
      case HEARTBEAT_KEY_STBINFO: {
        void   *rspMsg = NULL;
        int32_t rspLen = 0;
        mndValidateStbInfo(pMnode, kv->value, kv->valueLen / sizeof(SSTableVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_STBINFO, .valueLen = rspLen, .value = rspMsg};
          taosArrayPush(hbRsp.info, &kv1);
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
        mndValidateViewInfo(pMnode, kv->value, kv->valueLen / sizeof(SViewVersion), &rspMsg, &rspLen);
        if (rspMsg && rspLen > 0) {
          SKv kv1 = {.key = HEARTBEAT_KEY_VIEWINFO, .valueLen = rspLen, .value = rspMsg};
          taosArrayPush(hbRsp.info, &kv1);
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

  taosArrayPush(pBatchRsp->rsps, &hbRsp);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessHeartBeatReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  SClientHbBatchReq batchReq = {0};
  if (tDeserializeSClientHbBatchReq(pReq->pCont, pReq->contLen, &batchReq) != 0) {
    taosArrayDestroyEx(batchReq.reqs, tFreeClientHbReq);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SClientHbBatchRsp batchRsp = {0};
  batchRsp.svrTimestamp = taosGetTimestampSec();
  batchRsp.rsps = taosArrayInit(0, sizeof(SClientHbRsp));

  int32_t sz = taosArrayGetSize(batchReq.reqs);
  for (int i = 0; i < sz; i++) {
    SClientHbReq *pHbReq = taosArrayGet(batchReq.reqs, i);
    if (pHbReq->connKey.connType == CONN_TYPE__QUERY) {
      mndProcessQueryHeartBeat(pMnode, pReq, pHbReq, &batchRsp);
    } else if (pHbReq->connKey.connType == CONN_TYPE__TMQ) {
      SClientHbRsp *pRsp = mndMqHbBuildRsp(pMnode, pHbReq);
      if (pRsp != NULL) {
        taosArrayPush(batchRsp.rsps, pRsp);
        taosMemoryFree(pRsp);
      }
    }
  }
  taosArrayDestroyEx(batchReq.reqs, tFreeClientHbReq);

  int32_t tlen = tSerializeSClientHbBatchRsp(NULL, 0, &batchRsp);
  void   *buf = rpcMallocCont(tlen);
  tSerializeSClientHbBatchRsp(buf, tlen, &batchRsp);

  tFreeClientHbBatchRsp(&batchRsp);
  pReq->info.rspLen = tlen;
  pReq->info.rsp = buf;

  return 0;
}

static int32_t mndProcessKillQueryReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SKillQueryReq killReq = {0};
  if (tDeserializeSKillQueryReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("kill query msg is received, queryId:%s", killReq.queryStrId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_KILL_QUERY) != 0) {
    return -1;
  }

  int32_t  connId = 0;
  uint64_t queryId = 0;
  char    *p = strchr(killReq.queryStrId, ':');
  if (NULL == p) {
    mError("invalid query id %s", killReq.queryStrId);
    terrno = TSDB_CODE_MND_INVALID_QUERY_ID;
    return -1;
  }
  *p = 0;
  connId = taosStr2Int32(killReq.queryStrId, NULL, 16);
  queryId = taosStr2UInt64(p + 1, NULL, 16);

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%x, failed to kill queryId:%" PRIx64 ", conn not exist", connId, queryId);
    terrno = TSDB_CODE_MND_INVALID_CONN_ID;
    return -1;
  } else {
    mInfo("connId:%x, queryId:%" PRIx64 " is killed by user:%s", connId, queryId, pReq->info.conn.user);
    pConn->killId = queryId;
    taosCacheRelease(pMgmt->connCache, (void **)&pConn, false);
    return 0;
  }
}

static int32_t mndProcessKillConnReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SKillConnReq killReq = {0};
  if (tDeserializeSKillConnReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_KILL_CONN) != 0) {
    return -1;
  }

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &killReq.connId, sizeof(uint32_t));
  if (pConn == NULL) {
    mError("connId:%u, failed to kill connection, conn not exist", killReq.connId);
    terrno = TSDB_CODE_MND_INVALID_CONN_ID;
    return -1;
  } else {
    mInfo("connId:%u, is killed by user:%s", killReq.connId, pReq->info.conn.user);
    pConn->killed = 1;
    taosCacheRelease(pMgmt->connCache, (void **)&pConn, false);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mndProcessSvrVerReq(SRpcMsg *pReq) {
  int32_t       code = -1;
  SServerVerRsp rsp = {0};
  tstrncpy(rsp.ver, version, sizeof(rsp.ver));

  int32_t contLen = tSerializeSServerVerRsp(NULL, 0, &rsp);
  if (contLen < 0) goto _over;
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) goto _over;
  tSerializeSServerVerRsp(pRsp, contLen, &rsp);

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

  code = 0;

_over:

  return code;
}

static int32_t mndRetrieveConns(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  SConnObj *pConn = NULL;
  int32_t   keepTime = tsShellActivityTimer * 3;

  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->connCache);
  }

  while (numOfRows < rows) {
    pConn = mndGetNextConn(pMnode, pShow->pIter);
    if (pConn == NULL) {
      pShow->pIter = NULL;
      break;
    }

    if ((taosGetTimestampMs() - pConn->lastAccessTimeMs) > ((int64_t)keepTime * 1000)) {
      continue;
    }

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->id, false);

    char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(user, pConn->user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)user, false);

    char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(app, pConn->app);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)app, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->pid, false);

    char endpoint[TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    sprintf(&endpoint[VARSTR_HEADER_SIZE], "%s:%d", taosIpStr(pConn->ip), pConn->port);
    varDataLen(endpoint) = strlen(&endpoint[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)endpoint, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->loginTimeMs, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pConn->lastAccessTimeMs, false);

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
    sprintf(&queryId[VARSTR_HEADER_SIZE], "%x:%" PRIx64, pConn->id, pQuery->reqRid);
    varDataLen(queryId) = strlen(&queryId[VARSTR_HEADER_SIZE]);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)queryId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->queryId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pConn->id, false);

    char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(app, pConn->app);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)app, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pConn->pid, false);

    char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(user, pConn->user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)user, false);

    char endpoint[TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    sprintf(&endpoint[VARSTR_HEADER_SIZE], "%s:%d", taosIpStr(pConn->ip), pConn->port);
    varDataLen(endpoint) = strlen(&endpoint[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)endpoint, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->stime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->useconds, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->stableQuery, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->isSubQuery, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)&pQuery->subPlanNum, false);

    char    subStatus[TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE] = {0};
    int64_t reserve = 64;
    int32_t strSize = sizeof(subStatus);
    int32_t offset = VARSTR_HEADER_SIZE;
    for (int32_t i = 0; i < pQuery->subPlanNum && offset + reserve < strSize; ++i) {
      if (i) {
        offset += sprintf(subStatus + offset, ",");
      }
      if (offset + reserve < strSize) {
        SQuerySubDesc *pDesc = taosArrayGet(pQuery->subDesc, i);
        offset += sprintf(subStatus + offset, "%" PRIu64 ":%s", pDesc->tid, pDesc->status);
      } else {
        break;
      }
    }
    varDataLen(subStatus) = strlen(&subStatus[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, subStatus, (varDataLen(subStatus) == 0) ? true : false);

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(sql, pQuery->sql);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, curRowIndex, (const char *)sql, false);

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

  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->appCache);
  }

  while (numOfRows < rows) {
    pApp = mndGetNextApp(pMnode, pShow->pIter);
    if (pApp == NULL) {
      pShow->pIter = NULL;
      break;
    }

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->appId, false);

    char ip[TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    sprintf(&ip[VARSTR_HEADER_SIZE], "%s", taosIpStr(pApp->ip));
    varDataLen(ip) = strlen(&ip[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)ip, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->pid, false);

    char name[TSDB_APP_NAME_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    sprintf(&name[VARSTR_HEADER_SIZE], "%s", pApp->name);
    varDataLen(name) = strlen(&name[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)name, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->startTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfInsertsReq, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfInsertRows, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.insertElapsedTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.insertBytes, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.fetchBytes, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.queryElapsedTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.numOfSlowQueries, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.totalRequests, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->summary.currentRequests, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pApp->lastAccessTimeMs, false);

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
