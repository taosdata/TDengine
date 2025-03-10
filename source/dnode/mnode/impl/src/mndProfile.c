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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndProfile.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndSma.h"
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
  char     userApp[TSDB_APP_NAME_LEN];
  uint32_t userIp;
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

typedef struct {
  int32_t totalDnodes;
  int32_t onlineDnodes;
  SEpSet  epSet;
  SArray *pQnodeList;
  int64_t ipWhiteListVer;
} SConnPreparedObj;

#define CACHE_OBJ_KEEP_TIME 3  // s

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

  TAOS_RETURN(code);
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

static void setUserInfo2Conn(SConnObj *connObj, char *userApp, uint32_t userIp) {
  if (connObj == NULL) {
    return;
  }
  tstrncpy(connObj->userApp, userApp, sizeof(connObj->userApp));
  connObj->userIp = userIp;
}
static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, int8_t connType, uint32_t ip, uint16_t port,
                               int32_t pid, const char *app, int64_t startTime) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  char     connStr[255] = {0};
  int32_t  len = tsnprintf(connStr, sizeof(connStr), "%s%d%d%d%s", user, ip, port, pid, app);
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

static int32_t mndProcessConnectReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  SUserObj       *pUser = NULL;
  SDbObj         *pDb = NULL;
  SConnObj       *pConn = NULL;
  int32_t         code = 0;
  SConnectReq     connReq = {0};
  char            ip[TD_IP_LEN] = {0};
  const STraceId *trace = &pReq->info.traceId;

  if ((code = tDeserializeSConnectReq(pReq->pCont, pReq->contLen, &connReq)) != 0) {
    goto _OVER;
  }

  if ((code = taosCheckVersionCompatibleFromStr(connReq.sVer, td_version, 3)) != 0) {
    mGError("version not compatible. client version: %s, server version: %s", connReq.sVer, td_version);
    goto _OVER;
  }

  taosInetNtoa(ip, pReq->info.conn.clientIp);
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONNECT)) != 0) {
    mGError("user:%s, failed to login from %s since %s", pReq->info.conn.user, ip, tstrerror(code));
    goto _OVER;
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pUser);
  if (pUser == NULL) {
    mGError("user:%s, failed to login from %s while acquire user since %s", pReq->info.conn.user, ip, tstrerror(code));
    goto _OVER;
  }

  if (strncmp(connReq.passwd, pUser->pass, TSDB_PASSWORD_LEN - 1) != 0 && !tsMndSkipGrant) {
    mGError("user:%s, failed to login from %s since invalid pass, input:%s", pReq->info.conn.user, ip, connReq.passwd);
    code = TSDB_CODE_MND_AUTH_FAILURE;
    goto _OVER;
  }

  if (connReq.db[0]) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    (void)snprintf(db, TSDB_DB_FNAME_LEN, "%d%s%s", pUser->acctId, TS_PATH_DELIMITER, connReq.db);
    pDb = mndAcquireDb(pMnode, db);
    if (pDb == NULL) {
      if (0 != strcmp(connReq.db, TSDB_INFORMATION_SCHEMA_DB) &&
          (0 != strcmp(connReq.db, TSDB_PERFORMANCE_SCHEMA_DB))) {
        code = TSDB_CODE_MND_DB_NOT_EXIST;
        mGError("user:%s, failed to login from %s while use db:%s since %s", pReq->info.conn.user, ip, connReq.db,
                tstrerror(code));
        goto _OVER;
      }
    }

    TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_OR_WRITE_DB, pDb), NULL, _OVER);
  }

  pConn = mndCreateConn(pMnode, pReq->info.conn.user, connReq.connType, pReq->info.conn.clientIp,
                        pReq->info.conn.clientPort, connReq.pid, connReq.app, connReq.startTime);
  if (pConn == NULL) {
    code = terrno;
    mGError("user:%s, failed to login from %s while create connection since %s", pReq->info.conn.user, ip,
            tstrerror(code));
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
  connectRsp.monitorParas.tsEnableMonitor = tsEnableMonitor;
  connectRsp.monitorParas.tsMonitorInterval = tsMonitorInterval;
  connectRsp.monitorParas.tsSlowLogScope = tsSlowLogScope;
  connectRsp.monitorParas.tsSlowLogMaxLen = tsSlowLogMaxLen;
  connectRsp.monitorParas.tsSlowLogThreshold = tsSlowLogThreshold;
  connectRsp.enableAuditDelete = tsEnableAuditDelete;
  tstrncpy(connectRsp.monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb, TSDB_DB_NAME_LEN);
  connectRsp.whiteListVer = pUser->ipWhiteListVer;

  tstrncpy(connectRsp.sVer, td_version, sizeof(connectRsp.sVer));
  (void)snprintf(connectRsp.sDetailVer, sizeof(connectRsp.sDetailVer), "ver:%s\nbuild:%s\ngitinfo:%s", td_version,
                 td_buildinfo, td_gitinfo);
  mndGetMnodeEpSet(pMnode, &connectRsp.epSet);

  int32_t contLen = tSerializeSConnectRsp(NULL, 0, &connectRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(contLen, NULL, _OVER);
  }
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  }

  contLen = tSerializeSConnectRsp(pRsp, contLen, &connectRsp);
  if (contLen < 0) {
    rpcFreeCont(pRsp);
    TAOS_CHECK_GOTO(contLen, NULL, _OVER);
  }

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

  mGDebug("user:%s, login from %s:%d, conn:%u, app:%s", pReq->info.conn.user, ip, pConn->port, pConn->id, connReq.app);

  code = 0;

  char    detail[1000] = {0};
  int32_t nBytes = snprintf(detail, sizeof(detail), "app:%s", connReq.app);
  if ((uint32_t)nBytes < sizeof(detail)) {
    auditRecord(pReq, pMnode->clusterId, "login", "", "", detail, strlen(detail));
  } else {
    mError("failed to audit logic since %s", tstrerror(TSDB_CODE_OUT_OF_RANGE));
  }

_OVER:

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

static SAppObj *mndCreateApp(SMnode *pMnode, uint32_t clientIp, SAppHbReq *pReq) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SAppObj app;
  app.appId = pReq->appId;
  app.ip = clientIp;
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

static int32_t mndUpdateAppInfo(SMnode *pMnode, SClientHbReq *pHbReq, SRpcConnInfo *connInfo) {
  int32_t    code = 0;
  SAppHbReq *pReq = &pHbReq->app;
  SAppObj   *pApp = mndAcquireApp(pMnode, pReq->appId);
  if (pApp == NULL) {
    pApp = mndCreateApp(pMnode, connInfo->clientIp, pReq);
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
  SRpcConnInfo  connInfo = pMsg->info.conn;

  if (0 != pHbReq->app.appId) {
    TAOS_CHECK_RETURN(mndUpdateAppInfo(pMnode, pHbReq, &connInfo));
  }

  if (pHbReq->query) {
    SQueryHbReqBasic *pBasic = pHbReq->query;

    SConnObj *pConn = mndAcquireConn(pMnode, pBasic->connId);
    if (pConn == NULL) {
      pConn = mndCreateConn(pMnode, connInfo.user, CONN_TYPE__QUERY, connInfo.clientIp, connInfo.clientPort,
                            pHbReq->app.pid, pHbReq->app.name, 0);
      if (pConn == NULL) {
        mError("user:%s, conn:%u is freed and failed to create new since %s", connInfo.user, pBasic->connId, terrstr());
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      } else {
        mDebug("user:%s, conn:%u is freed, will create a new conn:%u", connInfo.user, pBasic->connId, pConn->id);
      }
    }

    setUserInfo2Conn(pConn, pHbReq->userApp, pHbReq->userIp);
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
  obj.ipWhiteListVer = batchReq.ipWhiteList;
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
  batchRsp.enableStrongPass = tsEnableStrongPassword;

  int32_t sz = taosArrayGetSize(batchReq.reqs);
  for (int i = 0; i < sz; i++) {
    SClientHbReq *pHbReq = taosArrayGet(batchReq.reqs, i);
    if (pHbReq->connKey.connType == CONN_TYPE__QUERY) {
      TAOS_CHECK_EXIT(mndProcessQueryHeartBeat(pMnode, pReq, pHbReq, &batchRsp, &obj));
    } else if (pHbReq->connKey.connType == CONN_TYPE__TMQ) {
      SClientHbRsp *pRsp = mndMqHbBuildRsp(pMnode, pHbReq);
      if (pRsp != NULL) {
        if (taosArrayPush(batchRsp.rsps, pRsp) == NULL) {
          mError("failed to put kv into array, but continue at this heartbeat");
        }
        taosMemoryFree(pRsp);
      }
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
  TAOS_CHECK_RETURN(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_KILL_QUERY));

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
    mInfo("connId:%x, queryId:%" PRIx64 " is killed by user:%s", connId, queryId, pReq->info.conn.user);
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

  TAOS_CHECK_RETURN(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_KILL_CONN));

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->connCache, &killReq.connId, sizeof(uint32_t));
  if (pConn == NULL) {
    mError("connId:%u, failed to kill connection, conn not exist", killReq.connId);
    code = TSDB_CODE_MND_INVALID_CONN_ID;
    TAOS_RETURN(code);
  } else {
    mInfo("connId:%u, is killed by user:%s", killReq.connId, pReq->info.conn.user);
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

    char endpoint[TD_IP_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    taosInetNtoa(varDataVal(endpoint), pConn->ip);
    (void)tsnprintf(varDataVal(endpoint) + strlen(varDataVal(endpoint)),
              sizeof(endpoint) - VARSTR_HEADER_SIZE - strlen(varDataVal(endpoint)), ":%d", pConn->port);
    varDataLen(endpoint) = strlen(varDataVal(endpoint));
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
    if (pConn->userIp != 0 && pConn->userIp != INADDR_NONE) {
      taosInetNtoa(varDataVal(userIp), pConn->userIp);
      varDataLen(userIp) = strlen(varDataVal(userIp));
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)userIp, false);
    if (code != 0) {
      mError("failed to set user ip since %s", tstrerror(code));
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
    (void)tsnprintf(&queryId[VARSTR_HEADER_SIZE], sizeof(queryId) - VARSTR_HEADER_SIZE, "%x:%" PRIx64, pConn->id,
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
    taosInetNtoa(varDataVal(endpoint), pConn->ip);
    (void)tsnprintf(varDataVal(endpoint) + strlen(varDataVal(endpoint)),
              sizeof(endpoint) - VARSTR_HEADER_SIZE - strlen(varDataVal(endpoint)), ":%d", pConn->port);
    varDataLen(endpoint) = strlen(&endpoint[VARSTR_HEADER_SIZE]);
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
    int64_t reserve = 64;
    int32_t strSize = sizeof(subStatus);
    int32_t offset = VARSTR_HEADER_SIZE;
    for (int32_t i = 0; i < pQuery->subPlanNum && offset + reserve < strSize; ++i) {
      if (i) {
        offset += tsnprintf(subStatus + offset, sizeof(subStatus) - offset, ",");
      }
      if (offset + reserve < strSize) {
        SQuerySubDesc *pDesc = taosArrayGet(pQuery->subDesc, i);
        offset +=
            tsnprintf(subStatus + offset, sizeof(subStatus) - offset, "%" PRIu64 ":%s", pDesc->tid, pDesc->status);
      } else {
        break;
      }
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
    if (pConn->userIp != 0 && pConn->userIp != INADDR_NONE) {
      taosInetNtoa(varDataVal(userIp), pConn->userIp);
      varDataLen(userIp) = strlen(varDataVal(userIp));
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, curRowIndex, (const char *)userIp, false);
    if (code != 0) {
      mError("failed to set user ip since %s", tstrerror(code));
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
    taosInetNtoa(varDataVal(ip), pApp->ip);
    varDataLen(ip) = strlen(varDataVal(ip));
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
    (void)tsnprintf(&name[VARSTR_HEADER_SIZE], sizeof(name) - VARSTR_HEADER_SIZE, "%s", pApp->name);
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
