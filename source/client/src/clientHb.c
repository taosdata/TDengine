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

#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "scheduler.h"
#include "trpc.h"

static SClientHbMgr clientHbMgr = {0};

static int32_t hbCreateThread();
static void    hbStopThread();

static int32_t hbMqHbReqHandle(SClientHbKey *connKey, void *param, SClientHbReq *req) { return 0; }

static int32_t hbMqHbRspHandle(SAppHbMgr *pAppHbMgr, SClientHbRsp *pRsp) { return 0; }

static int32_t hbProcessUserAuthInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  SUserAuthBatchRsp batchRsp = {0};
  if (tDeserializeSUserAuthBatchRsp(value, valueLen, &batchRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    SGetUserAuthRsp *rsp = taosArrayGet(batchRsp.pArray, i);
    tscDebug("hb user auth rsp, user:%s, version:%d", rsp->user, rsp->version);

    catalogUpdateUserAuthInfo(pCatalog, rsp);
  }

  taosArrayDestroy(batchRsp.pArray);
  return TSDB_CODE_SUCCESS;
}

static int32_t hbProcessDBInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  SUseDbBatchRsp batchUseRsp = {0};
  if (tDeserializeSUseDbBatchRsp(value, valueLen, &batchUseRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchUseRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    SUseDbRsp *rsp = taosArrayGet(batchUseRsp.pArray, i);
    tscDebug("hb db rsp, db:%s, vgVersion:%d, uid:%" PRIx64, rsp->db, rsp->vgVersion, rsp->uid);

    if (rsp->vgVersion < 0) {
      code = catalogRemoveDB(pCatalog, rsp->db, rsp->uid);
    } else {
      SDBVgInfo vgInfo = {0};
      vgInfo.vgVersion = rsp->vgVersion;
      vgInfo.hashMethod = rsp->hashMethod;
      vgInfo.vgHash = taosHashInit(rsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
      if (NULL == vgInfo.vgHash) {
        tscError("hash init[%d] failed", rsp->vgNum);
        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }

      for (int32_t j = 0; j < rsp->vgNum; ++j) {
        SVgroupInfo *pInfo = taosArrayGet(rsp->pVgroupInfos, j);
        if (taosHashPut(vgInfo.vgHash, &pInfo->vgId, sizeof(int32_t), pInfo, sizeof(SVgroupInfo)) != 0) {
          tscError("hash push failed, errno:%d", errno);
          taosHashCleanup(vgInfo.vgHash);
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      catalogUpdateDBVgInfo(pCatalog, rsp->db, rsp->uid, &vgInfo);
    }

    if (code) {
      return code;
    }
  }

  tFreeSUseDbBatchRsp(&batchUseRsp);
  return TSDB_CODE_SUCCESS;
}

static int32_t hbProcessStbInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  STableMetaBatchRsp batchMetaRsp = {0};
  if (tDeserializeSTableMetaBatchRsp(value, valueLen, &batchMetaRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchMetaRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    STableMetaRsp *rsp = taosArrayGet(batchMetaRsp.pArray, i);

    if (rsp->numOfColumns < 0) {
      tscDebug("hb remove stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      catalogRemoveStbMeta(pCatalog, rsp->dbFName, rsp->dbId, rsp->stbName, rsp->suid);
    } else {
      tscDebug("hb update stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      if (rsp->pSchemas[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
        tscError("invalid colId[%" PRIi16 "] for the first column in table meta rsp msg", rsp->pSchemas[0].colId);
        tFreeSTableMetaBatchRsp(&batchMetaRsp);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      catalogUpdateSTableMeta(pCatalog, rsp);
    }
  }

  tFreeSTableMetaBatchRsp(&batchMetaRsp);
  return TSDB_CODE_SUCCESS;
}

static int32_t hbQueryHbRspHandle(SAppHbMgr *pAppHbMgr, SClientHbRsp *pRsp) {
  SHbConnInfo *info = taosHashGet(pAppHbMgr->connInfo, &pRsp->connKey, sizeof(SClientHbKey));
  if (NULL == info) {
    tscWarn("fail to get connInfo, may be dropped, refId:%" PRIx64 ", type:%d", pRsp->connKey.tscRid,
            pRsp->connKey.connType);
    return TSDB_CODE_SUCCESS;
  }

  if (pRsp->query) {
    STscObj *pTscObj = (STscObj *)acquireTscObj(pRsp->connKey.tscRid);
    if (NULL == pTscObj) {
      tscDebug("tscObj rid %" PRIx64 " not exist", pRsp->connKey.tscRid);
    } else {      
      if (pRsp->query->totalDnodes > 1 && !isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, &pRsp->query->epSet)) {
        updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &pRsp->query->epSet);
      }
      pTscObj->connId = pRsp->query->connId;

      if (pRsp->query->killRid) {
        SRequestObj *pRequest = acquireRequest(pRsp->query->killRid);
        if (NULL == pRequest) {
          tscDebug("request 0x%" PRIx64 " not exist to kill", pRsp->query->killRid);
        } else {
          taos_stop_query((TAOS_RES *)pRequest);
          releaseRequest(pRsp->query->killRid);
        }
      }

      if (pRsp->query->killConnection) {
        taos_close(pTscObj);
      }

      releaseTscObj(pRsp->connKey.tscRid);
    }
  }

  int32_t kvNum = pRsp->info ? taosArrayGetSize(pRsp->info) : 0;

  tscDebug("hb got %d rsp kv", kvNum);

  for (int32_t i = 0; i < kvNum; ++i) {
    SKv *kv = taosArrayGet(pRsp->info, i);
    switch (kv->key) {
      case HEARTBEAT_KEY_USER_AUTHINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb user auth info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }

        int64_t         *clusterId = (int64_t *)info->param;
        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(*clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", *clusterId, tstrerror(code));
          break;
        }

        hbProcessUserAuthInfoRsp(kv->value, kv->valueLen, pCatalog);
        break;
      }
      case HEARTBEAT_KEY_DBINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb db info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }

        int64_t         *clusterId = (int64_t *)info->param;
        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(*clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", *clusterId, tstrerror(code));
          break;
        }

        hbProcessDBInfoRsp(kv->value, kv->valueLen, pCatalog);
        break;
      }
      case HEARTBEAT_KEY_STBINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb stb info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }

        int64_t         *clusterId = (int64_t *)info->param;
        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(*clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", *clusterId, tstrerror(code));
          break;
        }

        hbProcessStbInfoRsp(kv->value, kv->valueLen, pCatalog);
        break;
      }
      default:
        tscError("invalid hb key type:%d", kv->key);
        break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t hbAsyncCallBack(void *param, const SDataBuf *pMsg, int32_t code) {
  static int32_t emptyRspNum = 0;
  if (code != 0) {
    taosMemoryFreeClear(param);
    return -1;
  }

  char             *key = (char *)param;
  SClientHbBatchRsp pRsp = {0};
  tDeserializeSClientHbBatchRsp(pMsg->pData, pMsg->len, &pRsp);

  int32_t rspNum = taosArrayGetSize(pRsp.rsps);

  SAppInstInfo **pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  if (pInst == NULL || NULL == *pInst) {
    tscError("cluster not exist, key:%s", key);
    taosMemoryFreeClear(param);
    tFreeClientHbBatchRsp(&pRsp);
    return -1;
  }

  taosMemoryFreeClear(param);

  if (rspNum) {
    tscDebug("hb got %d rsp, %d empty rsp received before", rspNum,
             atomic_val_compare_exchange_32(&emptyRspNum, emptyRspNum, 0));
  } else {
    atomic_add_fetch_32(&emptyRspNum, 1);
  }

  for (int32_t i = 0; i < rspNum; ++i) {
    SClientHbRsp *rsp = taosArrayGet(pRsp.rsps, i);
    code = (*clientHbMgr.rspHandle[rsp->connKey.connType])((*pInst)->pAppHbMgr, rsp);
    if (code) {
      break;
    }
  }

  tFreeClientHbBatchRsp(&pRsp);

  return code;
}

int32_t hbBuildQueryDesc(SQueryHbReqBasic *hbBasic, STscObj *pObj) {
  int64_t    now = taosGetTimestampUs();
  SQueryDesc desc = {0};
  int32_t    code = 0;

  void *pIter = taosHashIterate(pObj->pRequests, NULL);
  while (pIter != NULL) {
    int64_t     *rid = pIter;
    SRequestObj *pRequest = acquireRequest(*rid);
    if (NULL == pRequest) {
      pIter = taosHashIterate(pObj->pRequests, pIter);
      continue;
    }

    tstrncpy(desc.sql, pRequest->sqlstr, sizeof(desc.sql));
    desc.stime = pRequest->metric.start;
    desc.queryId = pRequest->requestId;
    desc.useconds = now - pRequest->metric.start;
    desc.reqRid = pRequest->self;
    desc.pid = hbBasic->pid;
    taosGetFqdn(desc.fqdn);
    desc.subPlanNum = pRequest->body.pDag ? pRequest->body.pDag->numOfSubplans : 0;

    if (desc.subPlanNum) {
      desc.subDesc = taosArrayInit(desc.subPlanNum, sizeof(SQuerySubDesc));
      if (NULL == desc.subDesc) {
        releaseRequest(*rid);
        return TSDB_CODE_QRY_OUT_OF_MEMORY;
      }

      code = schedulerGetTasksStatus(pRequest->body.queryJob, desc.subDesc);
      if (code) {
        taosArrayDestroy(desc.subDesc);
        desc.subDesc = NULL;
      }
    } else {
      desc.subDesc = NULL;
    }

    releaseRequest(*rid);
    taosArrayPush(hbBasic->queryDesc, &desc);

    pIter = taosHashIterate(pObj->pRequests, pIter);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetQueryBasicInfo(SClientHbKey *connKey, SClientHbReq *req) {
  STscObj *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);
  if (NULL == pTscObj) {
    tscWarn("tscObj rid %" PRIx64 " not exist", connKey->tscRid);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  int32_t numOfQueries = pTscObj->pRequests ? taosHashGetSize(pTscObj->pRequests) : 0;
  if (numOfQueries <= 0) {
    releaseTscObj(connKey->tscRid);
    tscDebug("no queries on connection");
    return TSDB_CODE_QRY_APP_ERROR;
  }

  SQueryHbReqBasic *hbBasic = (SQueryHbReqBasic *)taosMemoryCalloc(1, sizeof(SQueryHbReqBasic));
  if (NULL == hbBasic) {
    tscError("calloc %d failed", (int32_t)sizeof(SQueryHbReqBasic));
    releaseTscObj(connKey->tscRid);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  hbBasic->queryDesc = taosArrayInit(numOfQueries, sizeof(SQueryDesc));
  if (NULL == hbBasic->queryDesc) {
    tscWarn("taosArrayInit %d queryDesc failed", numOfQueries);
    releaseTscObj(connKey->tscRid);
    taosMemoryFree(hbBasic);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  hbBasic->connId = pTscObj->connId;
  hbBasic->pid = taosGetPId();
  taosGetAppName(hbBasic->app, NULL);

  int32_t code = hbBuildQueryDesc(hbBasic, pTscObj);
  if (code) {
    releaseTscObj(connKey->tscRid);
    taosMemoryFree(hbBasic);
    return code;
  }

  req->query = hbBasic;
  releaseTscObj(connKey->tscRid);

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetExpiredUserInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SUserAuthVersion *users = NULL;
  uint32_t          userNum = 0;
  int32_t           code = 0;

  code = catalogGetExpiredUsers(pCatalog, &users, &userNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (userNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < userNum; ++i) {
    SUserAuthVersion *user = &users[i];
    user->version = htonl(user->version);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_USER_AUTHINFO,
      .valueLen = sizeof(SUserAuthVersion) * userNum,
      .value = users,
  };

  tscDebug("hb got %d expired users, valueLen:%d", userNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  }
  
  taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));

  return TSDB_CODE_SUCCESS;
}


int32_t hbGetExpiredDBInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SDbVgVersion *dbs = NULL;
  uint32_t      dbNum = 0;
  int32_t       code = 0;

  code = catalogGetExpiredDBs(pCatalog, &dbs, &dbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (dbNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < dbNum; ++i) {
    SDbVgVersion *db = &dbs[i];
    db->dbId = htobe64(db->dbId);
    db->vgVersion = htonl(db->vgVersion);
    db->numOfTable = htonl(db->numOfTable);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_DBINFO,
      .valueLen = sizeof(SDbVgVersion) * dbNum,
      .value = dbs,
  };

  tscDebug("hb got %d expired db, valueLen:%d", dbNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  }

  taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetExpiredStbInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SSTableMetaVersion *stbs = NULL;
  uint32_t            stbNum = 0;
  int32_t             code = 0;

  code = catalogGetExpiredSTables(pCatalog, &stbs, &stbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (stbNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < stbNum; ++i) {
    SSTableMetaVersion *stb = &stbs[i];
    stb->suid = htobe64(stb->suid);
    stb->sversion = htons(stb->sversion);
    stb->tversion = htons(stb->tversion);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_STBINFO,
      .valueLen = sizeof(SSTableMetaVersion) * stbNum,
      .value = stbs,
  };

  tscDebug("hb got %d expired stb, valueLen:%d", stbNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  }

  taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));

  return TSDB_CODE_SUCCESS;
}

int32_t hbQueryHbReqHandle(SClientHbKey *connKey, void *param, SClientHbReq *req) {
  int64_t         *clusterId = (int64_t *)param;
  struct SCatalog *pCatalog = NULL;

  int32_t code = catalogGetHandle(*clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", *clusterId, tstrerror(code));
    return code;
  }

  hbGetQueryBasicInfo(connKey, req);

  code = hbGetExpiredUserInfo(connKey, pCatalog, req);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = hbGetExpiredDBInfo(connKey, pCatalog, req);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = hbGetExpiredStbInfo(connKey, pCatalog, req);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

void hbMgrInitMqHbHandle() {
  clientHbMgr.reqHandle[CONN_TYPE__QUERY] = hbQueryHbReqHandle;
  clientHbMgr.reqHandle[CONN_TYPE__TMQ] = hbMqHbReqHandle;

  clientHbMgr.rspHandle[CONN_TYPE__QUERY] = hbQueryHbRspHandle;
  clientHbMgr.rspHandle[CONN_TYPE__TMQ] = hbMqHbRspHandle;
}

static FORCE_INLINE void hbMgrInitHandle() {
  // init all handle
  hbMgrInitMqHbHandle();
}

SClientHbBatchReq *hbGatherAllInfo(SAppHbMgr *pAppHbMgr) {
  SClientHbBatchReq *pBatchReq = taosMemoryCalloc(1, sizeof(SClientHbBatchReq));
  if (pBatchReq == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }
  int32_t connKeyCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
  pBatchReq->reqs = taosArrayInit(connKeyCnt, sizeof(SClientHbReq));

  int32_t code = 0;
  void   *pIter = taosHashIterate(pAppHbMgr->activeInfo, NULL);
  while (pIter != NULL) {
    SClientHbReq *pOneReq = pIter;

    pOneReq = taosArrayPush(pBatchReq->reqs, pOneReq);

    SHbConnInfo *info = taosHashGet(pAppHbMgr->connInfo, &pOneReq->connKey, sizeof(SClientHbKey));
    if (info) {
      code = (*clientHbMgr.reqHandle[pOneReq->connKey.connType])(&pOneReq->connKey, info->param, pOneReq);
      if (code) {
        pIter = taosHashIterate(pAppHbMgr->activeInfo, pIter);
        continue;
      }
    }

    //hbClearClientHbReq(pOneReq);

    pIter = taosHashIterate(pAppHbMgr->activeInfo, pIter);
  }

  //  if (code) {
  //    taosArrayDestroyEx(pBatchReq->reqs, hbFreeReq);
  //    taosMemoryFreeClear(pBatchReq);
  //  }

  return pBatchReq;
}

void hbClearReqInfo(SAppHbMgr *pAppHbMgr) {
  void *pIter = taosHashIterate(pAppHbMgr->activeInfo, NULL);
  while (pIter != NULL) {
    SClientHbReq *pOneReq = pIter;

    tFreeReqKvHash(pOneReq->info);
    taosHashClear(pOneReq->info);

    if (pOneReq->query) {
      taosArrayDestroy(pOneReq->query->queryDesc);
      taosMemoryFreeClear(pOneReq->query);
    }

    pIter = taosHashIterate(pAppHbMgr->activeInfo, pIter);
  }
}

void hbThreadFuncUnexpectedStopped(void) {
  atomic_store_8(&clientHbMgr.threadStop, 2);
}

static void *hbThreadFunc(void *param) {
  setThreadName("hb");
#ifdef WINDOWS
  atexit(hbThreadFuncUnexpectedStopped);
#endif
  while (1) {
    int8_t threadStop = atomic_val_compare_exchange_8(&clientHbMgr.threadStop, 1, 2);
    if (1 == threadStop) {
      break;
    }

    taosThreadMutexLock(&clientHbMgr.lock);

    int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
    for (int i = 0; i < sz; i++) {
      SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);

      int32_t connCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
      if (connCnt == 0) {
        continue;
      }
      SClientHbBatchReq *pReq = hbGatherAllInfo(pAppHbMgr);
      if (pReq == NULL) {
        continue;
      }
      int   tlen = tSerializeSClientHbBatchReq(NULL, 0, pReq);
      void *buf = taosMemoryMalloc(tlen);
      if (buf == NULL) {
        terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
        tFreeClientHbBatchReq(pReq);
        //hbClearReqInfo(pAppHbMgr);
        break;
      }

      tSerializeSClientHbBatchReq(buf, tlen, pReq);
      SMsgSendInfo *pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));

      if (pInfo == NULL) {
        terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
        tFreeClientHbBatchReq(pReq);
        //hbClearReqInfo(pAppHbMgr);
        taosMemoryFree(buf);
        break;
      }
      pInfo->fp = hbAsyncCallBack;
      pInfo->msgInfo.pData = buf;
      pInfo->msgInfo.len = tlen;
      pInfo->msgType = TDMT_MND_HEARTBEAT;
      pInfo->param = strdup(pAppHbMgr->key);
      pInfo->requestId = generateRequestId();
      pInfo->requestObjRefId = 0;

      SAppInstInfo *pAppInstInfo = pAppHbMgr->pAppInstInfo;
      int64_t       transporterId = 0;
      SEpSet        epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
      asyncSendMsgToServer(pAppInstInfo->pTransporter, &epSet, &transporterId, pInfo);
      tFreeClientHbBatchReq(pReq);
      //hbClearReqInfo(pAppHbMgr);

      atomic_add_fetch_32(&pAppHbMgr->reportCnt, 1);
    }

    taosThreadMutexUnlock(&clientHbMgr.lock);

    taosMsleep(HEARTBEAT_INTERVAL);
  }
  return NULL;
}

static int32_t hbCreateThread() {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&clientHbMgr.thread, &thAttr, hbThreadFunc, NULL) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void hbStopThread() {
  if (0 == atomic_load_8(&clientHbMgr.inited)) {
    return;
  }
  if (atomic_val_compare_exchange_8(&clientHbMgr.threadStop, 0, 1)) {
    tscDebug("hb thread already stopped");
    return;
  }

  while (2 != atomic_load_8(&clientHbMgr.threadStop)) {
    taosUsleep(10);
  }

  tscDebug("hb thread stopped");
}

SAppHbMgr *appHbMgrInit(SAppInstInfo *pAppInstInfo, char *key) {
  hbMgrInit();
  SAppHbMgr *pAppHbMgr = taosMemoryMalloc(sizeof(SAppHbMgr));
  if (pAppHbMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  // init stat
  pAppHbMgr->startTime = taosGetTimestampMs();
  pAppHbMgr->connKeyCnt = 0;
  pAppHbMgr->reportCnt = 0;
  pAppHbMgr->reportBytes = 0;
  pAppHbMgr->key = strdup(key);

  // init app info
  pAppHbMgr->pAppInstInfo = pAppInstInfo;

  // init hash info
  pAppHbMgr->activeInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  if (pAppHbMgr->activeInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pAppHbMgr);
    return NULL;
  }

  taosHashSetFreeFp(pAppHbMgr->activeInfo, tFreeClientHbReq);
  // init getInfoFunc
  pAppHbMgr->connInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  if (pAppHbMgr->connInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pAppHbMgr);
    return NULL;
  }

  taosThreadMutexLock(&clientHbMgr.lock);
  taosArrayPush(clientHbMgr.appHbMgrs, &pAppHbMgr);
  taosThreadMutexUnlock(&clientHbMgr.lock);

  return pAppHbMgr;
}

void appHbMgrCleanup(void) {
  int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
  for (int i = 0; i < sz; i++) {
    SAppHbMgr *pTarget = taosArrayGetP(clientHbMgr.appHbMgrs, i);

    void *pIter = taosHashIterate(pTarget->activeInfo, NULL);
    while (pIter != NULL) {
      SClientHbReq *pOneReq = pIter;
      tFreeClientHbReq(pOneReq);
      pIter = taosHashIterate(pTarget->activeInfo, pIter);
    }
    taosHashCleanup(pTarget->activeInfo);
    pTarget->activeInfo = NULL;

    pIter = taosHashIterate(pTarget->connInfo, NULL);
    while (pIter != NULL) {
      SHbConnInfo *info = pIter;
      taosMemoryFree(info->param);
      pIter = taosHashIterate(pTarget->connInfo, pIter);
    }
    taosHashCleanup(pTarget->connInfo);
    pTarget->connInfo = NULL;

    taosMemoryFree(pTarget->key);
    taosMemoryFree(pTarget);
  }
}

int hbMgrInit() {
  // init once
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 0, 1);
  if (old == 1) return 0;

  clientHbMgr.appHbMgrs = taosArrayInit(0, sizeof(void *));
  taosThreadMutexInit(&clientHbMgr.lock, NULL);

  // init handle funcs
  hbMgrInitHandle();

  // init backgroud thread
  hbCreateThread();

  return 0;
}

void hbMgrCleanUp() {
  hbStopThread();

  // destroy all appHbMgr
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 1, 0);
  if (old == 0) return;

  taosThreadMutexLock(&clientHbMgr.lock);
  appHbMgrCleanup();
  taosArrayDestroy(clientHbMgr.appHbMgrs);
  taosThreadMutexUnlock(&clientHbMgr.lock);

  clientHbMgr.appHbMgrs = NULL;
}

int hbRegisterConnImpl(SAppHbMgr *pAppHbMgr, SClientHbKey connKey, SHbConnInfo *info) {
  // init hash in activeinfo
  void *data = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  if (data != NULL) {
    return 0;
  }
  SClientHbReq hbReq = {0};
  hbReq.connKey = connKey;
  //hbReq.info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  taosHashPut(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey), &hbReq, sizeof(SClientHbReq));

  // init hash
  if (info != NULL) {
    SClientHbReq *pReq = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
    info->req = pReq;
    taosHashPut(pAppHbMgr->connInfo, &connKey, sizeof(SClientHbKey), info, sizeof(SHbConnInfo));
  }

  atomic_add_fetch_32(&pAppHbMgr->connKeyCnt, 1);
  return 0;
}

int hbRegisterConn(SAppHbMgr *pAppHbMgr, int64_t tscRefId, int64_t clusterId, int8_t connType) {
  SClientHbKey connKey = {
      .tscRid = tscRefId,
      .connType = connType,
  };
  SHbConnInfo info = {0};

  switch (connType) {
    case CONN_TYPE__QUERY: {
      int64_t *pClusterId = taosMemoryMalloc(sizeof(int64_t));
      *pClusterId = clusterId;

      info.param = pClusterId;
      return hbRegisterConnImpl(pAppHbMgr, connKey, &info);
    }
    case CONN_TYPE__TMQ: {
      return 0;
    }
    default:
      return 0;
  }
}

void hbDeregisterConn(SAppHbMgr *pAppHbMgr, SClientHbKey connKey) {
  SClientHbReq *pReq = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  if (pReq) {
    tFreeClientHbReq(pReq);
    taosHashRemove(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  }

  SHbConnInfo *info = taosHashGet(pAppHbMgr->connInfo, &connKey, sizeof(SClientHbKey));
  if (info) {
    taosMemoryFree(info->param);
    taosHashRemove(pAppHbMgr->connInfo, &connKey, sizeof(SClientHbKey));
  }

  if (NULL == pReq || NULL == info) {
    return;
  }

  atomic_sub_fetch_32(&pAppHbMgr->connKeyCnt, 1);
}

int hbAddConnInfo(SAppHbMgr *pAppHbMgr, SClientHbKey connKey, void *key, void *value, int32_t keyLen,
                  int32_t valueLen) {
  // find req by connection id
  SClientHbReq *pReq = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  ASSERT(pReq != NULL);

  taosHashPut(pReq->info, key, keyLen, value, valueLen);

  return 0;
}
