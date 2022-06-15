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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndUser.h"
#include "tglobal.h"
#include "version.h"

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

static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, int8_t connType, uint32_t ip, uint16_t port,
                               int32_t pid, const char *app, int64_t startTime);
static void      mndFreeConn(SConnObj *pConn);
static SConnObj *mndAcquireConn(SMnode *pMnode, uint32_t connId);
static void      mndReleaseConn(SMnode *pMnode, SConnObj *pConn);
static void     *mndGetNextConn(SMnode *pMnode, SCacheIter *pIter);
static void      mndCancelGetNextConn(SMnode *pMnode, void *pIter);
static int32_t   mndProcessHeartBeatReq(SRpcMsg *pReq);
static int32_t   mndProcessConnectReq(SRpcMsg *pReq);
static int32_t   mndProcessKillQueryReq(SRpcMsg *pReq);
static int32_t   mndProcessKillConnReq(SRpcMsg *pReq);
static int32_t   mndRetrieveConns(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t   mndRetrieveQueries(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void      mndCancelGetNextQuery(SMnode *pMnode, void *pIter);

int32_t mndInitProfile(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  // in ms
  int32_t connCheckTime = tsShellActivityTimer * 2 * 1000;
  pMgmt->cache = taosCacheInit(TSDB_DATA_TYPE_INT, connCheckTime, true, (__cache_free_fn_t)mndFreeConn, "conn");
  if (pMgmt->cache == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc profile cache since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_MND_HEARTBEAT, mndProcessHeartBeatReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONNECT, mndProcessConnectReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_QUERY, mndProcessKillQueryReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_CONN, mndProcessKillConnReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndRetrieveConns);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONNS, mndCancelGetNextConn);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndRetrieveQueries);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_QUERIES, mndCancelGetNextQuery);

  return 0;
}

void mndCleanupProfile(SMnode *pMnode) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  if (pMgmt->cache != NULL) {
    taosCacheCleanup(pMgmt->cache);
    pMgmt->cache = NULL;
  }
}

static SConnObj *mndCreateConn(SMnode *pMnode, const char *user, int8_t connType, uint32_t ip, uint16_t port,
                               int32_t pid, const char *app, int64_t startTime) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  char    connStr[255] = {0};
  int32_t len = snprintf(connStr, sizeof(connStr), "%s%d%d%d%s", user, ip, port, pid, app);
  int32_t connId = mndGenerateUid(connStr, len);
  if (startTime == 0) startTime = taosGetTimestampMs();

  SConnObj connObj = {.id = connId,
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
                      .pQueries = NULL};

  connObj.lastAccessTimeMs = connObj.loginTimeMs;
  tstrncpy(connObj.user, user, TSDB_USER_LEN);
  tstrncpy(connObj.app, app, TSDB_APP_NAME_LEN);

  int32_t   keepTime = tsShellActivityTimer * 3;
  SConnObj *pConn = taosCachePut(pMgmt->cache, &connId, sizeof(int32_t), &connObj, sizeof(connObj), keepTime * 1000);
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

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->cache, &connId, sizeof(connId));
  if (pConn == NULL) {
    mDebug("conn:%u, already destroyed", connId);
    return NULL;
  }

  int32_t keepTime = tsShellActivityTimer * 3;
  pConn->lastAccessTimeMs = keepTime * 1000 + (uint64_t)taosGetTimestampMs();

  mTrace("conn:%u, acquired from cache, data:%p", pConn->id, pConn);
  return pConn;
}

static void mndReleaseConn(SMnode *pMnode, SConnObj *pConn) {
  if (pConn == NULL) return;
  mTrace("conn:%u, released from cache, data:%p", pConn->id, pConn);

  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  taosCacheRelease(pMgmt->cache, (void **)&pConn, false);
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
  SMnode     *pMnode = pReq->info.node;
  SUserObj   *pUser = NULL;
  SDbObj     *pDb = NULL;
  SConnObj   *pConn = NULL;
  int32_t     code = -1;
  SConnectReq connReq = {0};
  char        ip[30] = {0};

  if (tDeserializeSConnectReq(pReq->pCont, pReq->contLen, &connReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CONN_OVER;
  }

  taosIp2String(pReq->conn.clientIp, ip);

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    mError("user:%s, failed to login while acquire user since %s", pReq->conn.user, terrstr());
    goto CONN_OVER;
  }
  if (0 != strncmp(connReq.passwd, pUser->pass, TSDB_PASSWORD_LEN - 1)) {
    mError("user:%s, failed to auth while acquire user, input:%s", pReq->conn.user, connReq.passwd);
    code = TSDB_CODE_RPC_AUTH_FAILURE;
    goto CONN_OVER;
  }

  if (connReq.db[0]) {
    char db[TSDB_DB_FNAME_LEN];
    snprintf(db, TSDB_DB_FNAME_LEN, "%d%s%s", pUser->acctId, TS_PATH_DELIMITER, connReq.db);
    pDb = mndAcquireDb(pMnode, db);
    if (pDb == NULL) {
      terrno = TSDB_CODE_MND_INVALID_DB;
      mError("user:%s, failed to login from %s while use db:%s since %s", pReq->conn.user, ip, connReq.db, terrstr());
      goto CONN_OVER;
    }
  }

  pConn = mndCreateConn(pMnode, pReq->conn.user, connReq.connType, pReq->conn.clientIp, pReq->conn.clientPort,
                        connReq.pid, connReq.app, connReq.startTime);
  if (pConn == NULL) {
    mError("user:%s, failed to login from %s while create connection since %s", pReq->conn.user, ip, terrstr());
    goto CONN_OVER;
  }

  SConnectRsp connectRsp = {0};
  connectRsp.acctId = pUser->acctId;
  connectRsp.superUser = pUser->superUser;
  connectRsp.clusterId = pMnode->clusterId;
  connectRsp.connId = pConn->id;
  connectRsp.connType = connReq.connType;
  connectRsp.dnodeNum = mndGetDnodeSize(pMnode);

  snprintf(connectRsp.sVersion, sizeof(connectRsp.sVersion), "ver:%s\nbuild:%s\ngitinfo:%s", version, buildinfo,
           gitinfo);
  mndGetMnodeEpSet(pMnode, &connectRsp.epSet);

  int32_t contLen = tSerializeSConnectRsp(NULL, 0, &connectRsp);
  if (contLen < 0) goto CONN_OVER;
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) goto CONN_OVER;
  tSerializeSConnectRsp(pRsp, contLen, &connectRsp);

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pRsp;

  mDebug("user:%s, login from %s:%d, conn:%u, app:%s", pReq->conn.user, ip, pConn->port, pConn->id, connReq.app);

  code = 0;

CONN_OVER:

  mndReleaseUser(pMnode, pUser);
  mndReleaseDb(pMnode, pDb);
  mndReleaseConn(pMnode, pConn);

  return code;
}

static int32_t mndSaveQueryList(SConnObj *pConn, SQueryHbReqBasic *pBasic) {
  taosWLockLatch(&pConn->queryLock);

  taosArrayDestroyEx(pConn->pQueries, tFreeClientHbQueryDesc);

  pConn->pQueries = pBasic->queryDesc;
  pConn->numOfQueries = pBasic->queryDesc ? taosArrayGetSize(pBasic->queryDesc) : 0;
  pBasic->queryDesc = NULL;

  mDebug("queries updated in conn %d, num:%d", pConn->id, pConn->numOfQueries);

  taosWUnLockLatch(&pConn->queryLock);

  return TSDB_CODE_SUCCESS;
}

static SClientHbRsp *mndMqHbBuildRsp(SMnode *pMnode, SClientHbReq *pReq) {
#if 0
  SClientHbRsp* pRsp = taosMemoryMalloc(sizeof(SClientHbRsp));
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pRsp->connKey = pReq->connKey;
  SMqHbBatchRsp batchRsp;
  batchRsp.batchRsps = taosArrayInit(0, sizeof(SMqHbRsp));
  if (batchRsp.batchRsps == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  SClientHbKey connKey = pReq->connKey;
  SHashObj* pObj =  pReq->info;
  SKv* pKv = taosHashGet(pObj, "mq-tmp", strlen("mq-tmp") + 1);
  if (pKv == NULL) {
    taosMemoryFree(pRsp);
    return NULL;
  }
  SMqHbMsg mqHb;
  taosDecodeSMqMsg(pKv->value, &mqHb);
  /*int64_t clientUid = htonl(pKv->value);*/
  /*if (mqHb.epoch )*/
  int sz = taosArrayGetSize(mqHb.pTopics);
  SMqConsumerObj* pConsumer = mndAcquireConsumer(pMnode, mqHb.consumerId); 
  for (int i = 0; i < sz; i++) {
    SMqHbOneTopicBatchRsp innerBatchRsp;
    innerBatchRsp.rsps = taosArrayInit(sz, sizeof(SMqHbRsp));
    if (innerBatchRsp.rsps == NULL) {
      //TODO
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    SMqHbTopicInfo* topicInfo = taosArrayGet(mqHb.pTopics, i);
    SMqConsumerTopic* pConsumerTopic = taosHashGet(pConsumer->topicHash, topicInfo->name, strlen(topicInfo->name)+1);
    if (pConsumerTopic->epoch != topicInfo->epoch) {
      //add new vgids into rsp
      int vgSz = taosArrayGetSize(topicInfo->pVgInfo);
      for (int j = 0; j < vgSz; j++) {
        SMqHbRsp innerRsp;
        SMqHbVgInfo* pVgInfo = taosArrayGet(topicInfo->pVgInfo, i);
        SVgObj* pVgObj = mndAcquireVgroup(pMnode, pVgInfo->vgId);
        innerRsp.epSet = mndGetVgroupEpset(pMnode, pVgObj);
        taosArrayPush(innerBatchRsp.rsps, &innerRsp);
      }
    }
    taosArrayPush(batchRsp.batchRsps, &innerBatchRsp);
  }
  int32_t tlen = taosEncodeSMqHbBatchRsp(NULL, &batchRsp);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    //TODO
    return NULL;
  }
  void* abuf = buf;
  taosEncodeSMqHbBatchRsp(&abuf, &batchRsp);
  pRsp->body = buf;
  pRsp->bodyLen = tlen;
  return pRsp;
#endif
  return NULL;
}

static int32_t mndProcessQueryHeartBeat(SMnode *pMnode, SRpcMsg *pMsg, SClientHbReq *pHbReq,
                                        SClientHbBatchRsp *pBatchRsp) {
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;
  SClientHbRsp  hbRsp = {.connKey = pHbReq->connKey, .status = 0, .info = NULL, .query = NULL};

  if (pHbReq->query) {
    SQueryHbReqBasic *pBasic = pHbReq->query;

    SRpcConnInfo connInfo = pMsg->conn;

    SConnObj *pConn = mndAcquireConn(pMnode, pBasic->connId);
    if (pConn == NULL) {
      pConn = mndCreateConn(pMnode, connInfo.user, CONN_TYPE__QUERY, connInfo.clientIp, connInfo.clientPort,
                            pBasic->pid, pBasic->app, 0);
      if (pConn == NULL) {
        mError("user:%s, conn:%u is freed and failed to create new since %s", connInfo.user, pBasic->connId, terrstr());
        return -1;
      } else {
        mDebug("user:%s, conn:%u is freed and create a new conn:%u", connInfo.user, pBasic->connId, pConn->id);
        pConn = mndAcquireConn(pMnode, pBasic->connId);
      }
    }
    
    SQueryHbRspBasic *rspBasic = taosMemoryCalloc(1, sizeof(SQueryHbRspBasic));
    if (rspBasic == NULL) {
      mndReleaseConn(pMnode, pConn);
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
    rspBasic->onlineDnodes = 1;  // TODO
    mndGetMnodeEpSet(pMnode, &rspBasic->epSet);

    mndCreateQnodeList(pMnode, &rspBasic->pQnodeList, -1);
    
    mndReleaseConn(pMnode, pConn);

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
        mndValidateDbInfo(pMnode, kv->value, kv->valueLen / sizeof(SDbVgVersion), &rspMsg, &rspLen);
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
      default:
        mError("invalid kv key:%d", kv->key);
        hbRsp.status = TSDB_CODE_MND_APP_ERROR;
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

  SUserObj *pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) return 0;
  if (!pUser->superUser) {
    mndReleaseUser(pMnode, pUser);
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    return -1;
  }
  mndReleaseUser(pMnode, pUser);

  SKillQueryReq killReq = {0};
  if (tDeserializeSKillQueryReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("kill query msg is received, queryId:%d", killReq.queryId);

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->cache, &killReq.connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%d, failed to kill queryId:%d, conn not exist", killReq.connId, killReq.queryId);
    terrno = TSDB_CODE_MND_INVALID_CONN_ID;
    return -1;
  } else {
    mInfo("connId:%d, queryId:%d is killed by user:%s", killReq.connId, killReq.queryId, pReq->conn.user);
    pConn->killId = killReq.queryId;
    taosCacheRelease(pMgmt->cache, (void **)&pConn, false);
    return 0;
  }
}

static int32_t mndProcessKillConnReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SProfileMgmt *pMgmt = &pMnode->profileMgmt;

  SUserObj *pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) return 0;
  if (!pUser->superUser) {
    mndReleaseUser(pMnode, pUser);
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    return -1;
  }
  mndReleaseUser(pMnode, pUser);

  SKillConnReq killReq = {0};
  if (tDeserializeSKillConnReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SConnObj *pConn = taosCacheAcquireByKey(pMgmt->cache, &killReq.connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%d, failed to kill connection, conn not exist", killReq.connId);
    terrno = TSDB_CODE_MND_INVALID_CONN_ID;
    return -1;
  } else {
    mInfo("connId:%d, is killed by user:%s", killReq.connId, pReq->conn.user);
    pConn->killed = 1;
    taosCacheRelease(pMgmt->cache, (void **)&pConn, false);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mndRetrieveConns(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SConnObj  *pConn = NULL;
  
  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->cache);
  }

  while (numOfRows < rows) {
    pConn = mndGetNextConn(pMnode, pShow->pIter);
    if (pConn == NULL) {
      pShow->pIter = NULL;
      break;
    }

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pConn->id, false);

    char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(user, pConn->user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)user, false);

    char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
    STR_TO_VARSTR(app, pConn->app);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)app, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pConn->pid, false);

    char endpoint[TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
    sprintf(&endpoint[VARSTR_HEADER_SIZE], "%s:%d", taosIpStr(pConn->ip), pConn->port);
    varDataLen(endpoint) = strlen(&endpoint[VARSTR_HEADER_SIZE]);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)endpoint, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pConn->loginTimeMs, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pConn->lastAccessTimeMs, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static int32_t mndRetrieveQueries(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SConnObj  *pConn = NULL;
  
  if (pShow->pIter == NULL) {
    SProfileMgmt *pMgmt = &pMnode->profileMgmt;
    pShow->pIter = taosCacheCreateIter(pMgmt->cache);
  }

  while (numOfRows < rows) {
    pConn = mndGetNextConn(pMnode, pShow->pIter);
    if (pConn == NULL) {
      pShow->pIter = NULL;
      break;
    }

    taosRLockLatch(&pConn->queryLock);
    if (NULL == pConn->pQueries || taosArrayGetSize(pConn->pQueries) <= 0) {
      taosRUnLockLatch(&pConn->queryLock);
      continue;
    }

    int32_t numOfQueries = taosArrayGetSize(pConn->pQueries);
    for (int32_t i = 0; i < numOfQueries; ++i) {
      SQueryDesc* pQuery = taosArrayGet(pConn->pQueries, i);
      cols = 0;

      char queryId[26 + VARSTR_HEADER_SIZE] = {0};
      sprintf(&queryId[VARSTR_HEADER_SIZE], "%x:%" PRIx64, pConn->id, pQuery->reqRid);
      varDataLen(queryId) = strlen(&queryId[VARSTR_HEADER_SIZE]);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)queryId, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->queryId, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pConn->id, false);

      char app[TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE];
      STR_TO_VARSTR(app, pConn->app);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)app, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->pid, false);

      char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(user, pConn->user);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)user, false);

      char endpoint[TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE] = {0};
      sprintf(&endpoint[VARSTR_HEADER_SIZE], "%s:%d", taosIpStr(pConn->ip), pConn->port);
      varDataLen(endpoint) = strlen(&endpoint[VARSTR_HEADER_SIZE]);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)endpoint, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->stime, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->useconds, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->stableQuery, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pQuery->subPlanNum, false);

      char subStatus[TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE] = {0};
      int32_t strSize = sizeof(subStatus);
      int32_t offset = VARSTR_HEADER_SIZE;
      for (int32_t i = 0; i < pQuery->subPlanNum && offset < strSize; ++i) {
        if (i) {
          offset += snprintf(subStatus + offset, strSize - offset - 1, ",");
        }
        SQuerySubDesc* pDesc = taosArrayGet(pQuery->subDesc, i);
        offset += snprintf(subStatus + offset, strSize - offset - 1, "%" PRIu64 ":%s", pDesc->tid, pDesc->status);
      }
      varDataLen(subStatus) = strlen(&subStatus[VARSTR_HEADER_SIZE]);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, subStatus, false);

      char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(sql, pQuery->sql);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)sql, false);

      numOfRows++;
    }
    
    taosRUnLockLatch(&pConn->queryLock);
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
  return taosCacheGetNumOfObj(pMgmt->cache);
}
