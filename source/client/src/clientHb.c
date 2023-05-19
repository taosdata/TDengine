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

typedef struct {
  union {
    struct {
      int64_t clusterId;
      int32_t passKeyCnt;
      int32_t passVer;
      int32_t reqCnt;
    };
  };
} SHbParam;

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

static int32_t hbProcessUserPassInfoRsp(void *value, int32_t valueLen, SClientHbKey *connKey, SAppHbMgr *pAppHbMgr) {
  int32_t           code = 0;
  int32_t           numOfBatchs = 0;
  SUserPassBatchRsp batchRsp = {0};
  if (tDeserializeSUserPassBatchRsp(value, valueLen, &batchRsp) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  numOfBatchs = taosArrayGetSize(batchRsp.pArray);

  SClientHbReq *pReq = NULL;
  while ((pReq = taosHashIterate(pAppHbMgr->activeInfo, pReq))) {
    STscObj *pTscObj = (STscObj *)acquireTscObj(pReq->connKey.tscRid);
    if (!pTscObj) {
      continue;
    }
    SPassInfo *passInfo = &pTscObj->passInfo;
    if (!passInfo->fp) {
      releaseTscObj(pReq->connKey.tscRid);
      continue;
    }

    for (int32_t i = 0; i < numOfBatchs; ++i) {
      SGetUserPassRsp *rsp = taosArrayGet(batchRsp.pArray, i);
      if (0 == strncmp(rsp->user, pTscObj->user, TSDB_USER_LEN)) {
        int32_t oldVer = atomic_load_32(&passInfo->ver);
        if (oldVer < rsp->version) {
          atomic_store_32(&passInfo->ver, rsp->version);
          if (passInfo->fp) {
            (*passInfo->fp)(passInfo->param, &passInfo->ver, TAOS_NOTIFY_PASSVER);
          }
          tscDebug("update passVer of user %s from %d to %d, tscRid:%" PRIi64, rsp->user, oldVer,
                   atomic_load_32(&passInfo->ver), pTscObj->id);
        }
        break;
      }
    }
    releaseTscObj(pReq->connKey.tscRid);
  }

  taosArrayDestroy(batchRsp.pArray);

  return code;
}

static int32_t hbGenerateVgInfoFromRsp(SDBVgInfo **pInfo, SUseDbRsp *rsp) {
  int32_t    code = 0;
  SDBVgInfo *vgInfo = taosMemoryCalloc(1, sizeof(SDBVgInfo));
  if (NULL == vgInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  vgInfo->vgVersion = rsp->vgVersion;
  vgInfo->stateTs = rsp->stateTs;
  vgInfo->hashMethod = rsp->hashMethod;
  vgInfo->hashPrefix = rsp->hashPrefix;
  vgInfo->hashSuffix = rsp->hashSuffix;
  vgInfo->vgHash = taosHashInit(rsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == vgInfo->vgHash) {
    taosMemoryFree(vgInfo);
    tscError("hash init[%d] failed", rsp->vgNum);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  for (int32_t j = 0; j < rsp->vgNum; ++j) {
    SVgroupInfo *pInfo = taosArrayGet(rsp->pVgroupInfos, j);
    if (taosHashPut(vgInfo->vgHash, &pInfo->vgId, sizeof(int32_t), pInfo, sizeof(SVgroupInfo)) != 0) {
      tscError("hash push failed, errno:%d", errno);
      taosHashCleanup(vgInfo->vgHash);
      taosMemoryFree(vgInfo);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
  }

_return:
  if (code) {
    taosHashCleanup(vgInfo->vgHash);
    taosMemoryFreeClear(vgInfo);
  }

  *pInfo = vgInfo;
  return code;
}

static int32_t hbProcessDBInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  SDbHbBatchRsp batchRsp = {0};
  if (tDeserializeSDbHbBatchRsp(value, valueLen, &batchRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    code = terrno;
    goto _return;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    SDbHbRsp *rsp = taosArrayGet(batchRsp.pArray, i);
    if (rsp->useDbRsp) {
      tscDebug("hb use db rsp, db:%s, vgVersion:%d, stateTs:%" PRId64 ", uid:%" PRIx64,
        rsp->useDbRsp->db, rsp->useDbRsp->vgVersion, rsp->useDbRsp->stateTs, rsp->useDbRsp->uid);

      if (rsp->useDbRsp->vgVersion < 0) {
        code = catalogRemoveDB(pCatalog, rsp->useDbRsp->db, rsp->useDbRsp->uid);
      } else {
        SDBVgInfo *vgInfo = NULL;
        code = hbGenerateVgInfoFromRsp(&vgInfo, rsp->useDbRsp);
        if (TSDB_CODE_SUCCESS != code) {
          goto _return;
        }

        catalogUpdateDBVgInfo(pCatalog, rsp->useDbRsp->db, rsp->useDbRsp->uid, vgInfo);

        if (IS_SYS_DBNAME(rsp->useDbRsp->db)) {
          code = hbGenerateVgInfoFromRsp(&vgInfo, rsp->useDbRsp);
          if (TSDB_CODE_SUCCESS != code) {
            goto _return;
          }

          catalogUpdateDBVgInfo(pCatalog, (rsp->useDbRsp->db[0] == 'i') ? TSDB_PERFORMANCE_SCHEMA_DB : TSDB_INFORMATION_SCHEMA_DB, rsp->useDbRsp->uid, vgInfo);
        }
      }
    }

    if (rsp->cfgRsp) {
      tscDebug("hb db cfg rsp, db:%s, cfgVersion:%d", rsp->cfgRsp->db, rsp->cfgRsp->cfgVersion);
      catalogUpdateDbCfg(pCatalog, rsp->cfgRsp->db, rsp->cfgRsp->dbId, rsp->cfgRsp);
      rsp->cfgRsp = NULL;
    }
  }

_return:

  tFreeSDbHbBatchRsp(&batchRsp);
  return code;
}

static int32_t hbProcessStbInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  SSTbHbRsp hbRsp = {0};
  if (tDeserializeSSTbHbRsp(value, valueLen, &hbRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfMeta = taosArrayGetSize(hbRsp.pMetaRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *rsp = taosArrayGet(hbRsp.pMetaRsp, i);

    if (rsp->numOfColumns < 0) {
      tscDebug("hb remove stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      catalogRemoveStbMeta(pCatalog, rsp->dbFName, rsp->dbId, rsp->stbName, rsp->suid);
    } else {
      tscDebug("hb update stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      if (rsp->pSchemas[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
        tscError("invalid colId[%" PRIi16 "] for the first column in table meta rsp msg", rsp->pSchemas[0].colId);
        tFreeSSTbHbRsp(&hbRsp);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      catalogUpdateTableMeta(pCatalog, rsp);
    }
  }

  int32_t numOfIndex = taosArrayGetSize(hbRsp.pIndexRsp);
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *rsp = taosArrayGet(hbRsp.pIndexRsp, i);

    catalogUpdateTableIndex(pCatalog, rsp);
  }

  taosArrayDestroy(hbRsp.pIndexRsp);
  hbRsp.pIndexRsp = NULL;

  tFreeSSTbHbRsp(&hbRsp);
  return TSDB_CODE_SUCCESS;
}

static int32_t hbQueryHbRspHandle(SAppHbMgr *pAppHbMgr, SClientHbRsp *pRsp) {
  SClientHbReq *pReq = taosHashAcquire(pAppHbMgr->activeInfo, &pRsp->connKey, sizeof(SClientHbKey));
  if (NULL == pReq) {
    tscWarn("pReq to get activeInfo, may be dropped, refId:%" PRIx64 ", type:%d", pRsp->connKey.tscRid,
            pRsp->connKey.connType);
    return TSDB_CODE_SUCCESS;
  }

  if (pRsp->query) {
    STscObj *pTscObj = (STscObj *)acquireTscObj(pRsp->connKey.tscRid);
    if (NULL == pTscObj) {
      tscDebug("tscObj rid %" PRIx64 " not exist", pRsp->connKey.tscRid);
    } else {
      if (pRsp->query->totalDnodes > 1 && !isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, &pRsp->query->epSet)) {
        SEpSet *pOrig = &pTscObj->pAppInfo->mgmtEp.epSet;
        SEp    *pOrigEp = &pOrig->eps[pOrig->inUse];
        SEp    *pNewEp = &pRsp->query->epSet.eps[pRsp->query->epSet.inUse];
        tscDebug("mnode epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d in hb", pOrig->inUse, pOrig->numOfEps,
                 pOrigEp->fqdn, pOrigEp->port, pRsp->query->epSet.inUse, pRsp->query->epSet.numOfEps, pNewEp->fqdn,
                 pNewEp->port);

        updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &pRsp->query->epSet);
      }

      pTscObj->pAppInfo->totalDnodes = pRsp->query->totalDnodes;
      pTscObj->pAppInfo->onlineDnodes = pRsp->query->onlineDnodes;
      pTscObj->connId = pRsp->query->connId;
      tscTrace("conn %u hb rsp, dnodes %d/%d", pTscObj->connId, pTscObj->pAppInfo->onlineDnodes,
               pTscObj->pAppInfo->totalDnodes);

      if (pRsp->query->killRid) {
        tscDebug("request rid %" PRIx64 " need to be killed now", pRsp->query->killRid);
        SRequestObj *pRequest = acquireRequest(pRsp->query->killRid);
        if (NULL == pRequest) {
          tscDebug("request 0x%" PRIx64 " not exist to kill", pRsp->query->killRid);
        } else {
          taos_stop_query((TAOS_RES *)pRequest);
          releaseRequest(pRsp->query->killRid);
        }
      }

      if (pRsp->query->killConnection) {
        taos_close_internal(pTscObj);
      }

      if (pRsp->query->pQnodeList) {
        updateQnodeList(pTscObj->pAppInfo, pRsp->query->pQnodeList);
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

        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(pReq->clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pReq->clusterId, tstrerror(code));
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

        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(pReq->clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pReq->clusterId, tstrerror(code));
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

        struct SCatalog *pCatalog = NULL;

        int32_t code = catalogGetHandle(pReq->clusterId, &pCatalog);
        if (code != TSDB_CODE_SUCCESS) {
          tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pReq->clusterId, tstrerror(code));
          break;
        }

        hbProcessStbInfoRsp(kv->value, kv->valueLen, pCatalog);
        break;
      }
      case HEARTBEAT_KEY_USER_PASSINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb user pass info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }

        hbProcessUserPassInfoRsp(kv->value, kv->valueLen, &pRsp->connKey, pAppHbMgr);
        break;
      }
      default:
        tscError("invalid hb key type:%d", kv->key);
        break;
    }
  }

  taosHashRelease(pAppHbMgr->activeInfo, pReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t hbAsyncCallBack(void *param, SDataBuf *pMsg, int32_t code) {
  if (0 == atomic_load_8(&clientHbMgr.inited)) {
    goto _return;
  }

  static int32_t    emptyRspNum = 0;
  int32_t           idx = *(int32_t *)param;
  SClientHbBatchRsp pRsp = {0};
  if (TSDB_CODE_SUCCESS == code) {
    tDeserializeSClientHbBatchRsp(pMsg->pData, pMsg->len, &pRsp);

    int32_t now = taosGetTimestampSec();
    int32_t delta = abs(now - pRsp.svrTimestamp);
    if (delta > timestampDeltaLimit) {
      code = TSDB_CODE_TIME_UNSYNCED;
      tscError("time diff: %ds is too big", delta);
    }
  }

  int32_t rspNum = taosArrayGetSize(pRsp.rsps);

  taosThreadMutexLock(&clientHbMgr.lock);

  SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, idx);
  if (pAppHbMgr == NULL) {
    taosThreadMutexUnlock(&clientHbMgr.lock);
    tscError("appHbMgr not exist, idx:%d", idx);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tFreeClientHbBatchRsp(&pRsp);
    return -1;
  }

  SAppInstInfo *pInst = pAppHbMgr->pAppInstInfo;

  if (code != 0) {
    pInst->onlineDnodes = pInst->totalDnodes ? 0 : -1;
    tscDebug("hb rsp error %s, update server status %d/%d", tstrerror(code), pInst->onlineDnodes, pInst->totalDnodes);
  }

  if (rspNum) {
    tscDebug("hb got %d rsp, %d empty rsp received before", rspNum,
             atomic_val_compare_exchange_32(&emptyRspNum, emptyRspNum, 0));
  } else {
    atomic_add_fetch_32(&emptyRspNum, 1);
  }

  for (int32_t i = 0; i < rspNum; ++i) {
    SClientHbRsp *rsp = taosArrayGet(pRsp.rsps, i);
    code = (*clientHbMgr.rspHandle[rsp->connKey.connType])(pAppHbMgr, rsp);
    if (code) {
      break;
    }
  }

  taosThreadMutexUnlock(&clientHbMgr.lock);

  tFreeClientHbBatchRsp(&pRsp);

_return:
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
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

    if (pRequest->killed || 0 == pRequest->body.queryJob) {
      releaseRequest(*rid);
      pIter = taosHashIterate(pObj->pRequests, pIter);
      continue;
    }

    tstrncpy(desc.sql, pRequest->sqlstr, sizeof(desc.sql));
    desc.stime = pRequest->metric.start / 1000;
    desc.queryId = pRequest->requestId;
    desc.useconds = now - pRequest->metric.start;
    desc.reqRid = pRequest->self;
    desc.stableQuery = pRequest->stableQuery;
    taosGetFqdn(desc.fqdn);
    desc.subPlanNum = pRequest->body.subplanNum;

    if (desc.subPlanNum) {
      desc.subDesc = taosArrayInit(desc.subPlanNum, sizeof(SQuerySubDesc));
      if (NULL == desc.subDesc) {
        releaseRequest(*rid);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      code = schedulerGetTasksStatus(pRequest->body.queryJob, desc.subDesc);
      if (code) {
        taosArrayDestroy(desc.subDesc);
        desc.subDesc = NULL;
        desc.subPlanNum = 0;
      }
      desc.subPlanNum = taosArrayGetSize(desc.subDesc);
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
    return TSDB_CODE_APP_ERROR;
  }

  SQueryHbReqBasic *hbBasic = (SQueryHbReqBasic *)taosMemoryCalloc(1, sizeof(SQueryHbReqBasic));
  if (NULL == hbBasic) {
    tscError("calloc %d failed", (int32_t)sizeof(SQueryHbReqBasic));
    releaseTscObj(connKey->tscRid);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  hbBasic->connId = pTscObj->connId;

  int32_t numOfQueries = pTscObj->pRequests ? taosHashGetSize(pTscObj->pRequests) : 0;
  if (numOfQueries <= 0) {
    req->query = hbBasic;
    releaseTscObj(connKey->tscRid);
    tscDebug("no queries on connection");
    return TSDB_CODE_SUCCESS;
  }

  hbBasic->queryDesc = taosArrayInit(numOfQueries, sizeof(SQueryDesc));
  if (NULL == hbBasic->queryDesc) {
    tscWarn("taosArrayInit %d queryDesc failed", numOfQueries);
    releaseTscObj(connKey->tscRid);
    taosMemoryFree(hbBasic);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = hbBuildQueryDesc(hbBasic, pTscObj);
  if (code) {
    releaseTscObj(connKey->tscRid);
    if (hbBasic->queryDesc) {
      taosArrayDestroyEx(hbBasic->queryDesc, tFreeClientHbQueryDesc);
    }
    taosMemoryFree(hbBasic);
    return code;
  }

  req->query = hbBasic;
  releaseTscObj(connKey->tscRid);

  return TSDB_CODE_SUCCESS;
}

static int32_t hbGetUserBasicInfo(SClientHbKey *connKey, SHbParam *param, SClientHbReq *req) {
  STscObj *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);
  if (!pTscObj) {
    tscWarn("tscObj rid %" PRIx64 " not exist", connKey->tscRid);
    return TSDB_CODE_APP_ERROR;
  }

  int32_t code = 0;

  if (param && (param->passVer != INT32_MIN) && (param->passVer <= pTscObj->passInfo.ver)) {
    tscDebug("hb got user basic info, no need since passVer %d <= %d", param->passVer, pTscObj->passInfo.ver);
    goto _return;
  }

  SUserPassVersion *user = taosMemoryMalloc(sizeof(SUserPassVersion));
  if (!user) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  strncpy(user->user, pTscObj->user, TSDB_USER_LEN);
  user->version = htonl(pTscObj->passInfo.ver);

  SKv kv = {
      .key = HEARTBEAT_KEY_USER_PASSINFO,
      .valueLen = sizeof(SUserPassVersion),
      .value = user,
  };

  tscDebug("hb got user basic info, valueLen:%d, user:%s, passVer:%d, tscRid:%" PRIi64, kv.valueLen, user->user,
           pTscObj->passInfo.ver, connKey->tscRid);

  if (!req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  }

  if (taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv)) < 0) {
    code = terrno ? terrno : TSDB_CODE_APP_ERROR;
    goto _return;
  }

  // assign the passVer
  if (param) {
    param->passVer = pTscObj->passInfo.ver;
  }

_return:
  releaseTscObj(connKey->tscRid);
  if (code) {
    tscError("hb got user basic info failed since %s", terrstr(code));
  }

  return code;
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
    taosMemoryFree(users);
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
  SDbCacheInfo *dbs = NULL;
  uint32_t      dbNum = 0;
  int32_t       code = 0;

  code = catalogGetExpiredDBs(pCatalog, &dbs, &dbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (dbNum <= 0) {
    taosMemoryFree(dbs);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < dbNum; ++i) {
    SDbCacheInfo *db = &dbs[i];
    tscDebug("the %dth expired dbFName:%s, dbId:%" PRId64 ", vgVersion:%d, cfgVersion:%d, numOfTable:%d, startTs:%" PRId64,
      i, db->dbFName, db->dbId, db->vgVersion, db->cfgVersion, db->numOfTable, db->stateTs);

    db->dbId = htobe64(db->dbId);
    db->vgVersion = htonl(db->vgVersion);
    db->cfgVersion = htonl(db->cfgVersion);
    db->numOfTable = htonl(db->numOfTable);
    db->stateTs = htobe64(db->stateTs);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_DBINFO,
      .valueLen = sizeof(SDbCacheInfo) * dbNum,
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
  SSTableVersion *stbs = NULL;
  uint32_t        stbNum = 0;
  int32_t         code = 0;

  code = catalogGetExpiredSTables(pCatalog, &stbs, &stbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (stbNum <= 0) {
    taosMemoryFree(stbs);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < stbNum; ++i) {
    SSTableVersion *stb = &stbs[i];
    stb->suid = htobe64(stb->suid);
    stb->sversion = htons(stb->sversion);
    stb->tversion = htons(stb->tversion);
    stb->smaVer = htonl(stb->smaVer);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_STBINFO,
      .valueLen = sizeof(SSTableVersion) * stbNum,
      .value = stbs,
  };

  tscDebug("hb got %d expired stb, valueLen:%d", stbNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  }

  taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetAppInfo(int64_t clusterId, SClientHbReq *req) {
  SAppHbReq *pApp = taosHashGet(clientHbMgr.appSummary, &clusterId, sizeof(clusterId));
  if (NULL != pApp) {
    memcpy(&req->app, pApp, sizeof(*pApp));
  } else {
    memset(&req->app.summary, 0, sizeof(req->app.summary));
    req->app.pid = taosGetPId();
    req->app.appId = clientHbMgr.appId;
    taosGetAppName(req->app.name, NULL);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbQueryHbReqHandle(SClientHbKey *connKey, void *param, SClientHbReq *req) {
  int32_t   code = 0;
  SHbParam *hbParam = (SHbParam *)param;
  SCatalog *pCatalog = NULL;

  if (hbParam->reqCnt == 0) {
    code = catalogGetHandle(hbParam->clusterId, &pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }
  }

  hbGetAppInfo(hbParam->clusterId, req);

  hbGetQueryBasicInfo(connKey, req);

  if (hbParam->passKeyCnt > 0) {
    hbGetUserBasicInfo(connKey, hbParam, req);
  }

  if (hbParam->reqCnt == 0) {
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
  }

  ++hbParam->reqCnt; // success to get catalog info

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void hbMgrInitHandle() {
  // init all handle
  clientHbMgr.reqHandle[CONN_TYPE__QUERY] = hbQueryHbReqHandle;
  clientHbMgr.reqHandle[CONN_TYPE__TMQ] = hbMqHbReqHandle;

  clientHbMgr.rspHandle[CONN_TYPE__QUERY] = hbQueryHbRspHandle;
  clientHbMgr.rspHandle[CONN_TYPE__TMQ] = hbMqHbRspHandle;
}

SClientHbBatchReq *hbGatherAllInfo(SAppHbMgr *pAppHbMgr) {
  SClientHbBatchReq *pBatchReq = taosMemoryCalloc(1, sizeof(SClientHbBatchReq));
  if (pBatchReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  int32_t connKeyCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
  pBatchReq->reqs = taosArrayInit(connKeyCnt, sizeof(SClientHbReq));
  if (!pBatchReq->reqs) {
    tFreeClientHbBatchReq(pBatchReq);
    return NULL;
  }

  void    *pIter = NULL;
  SHbParam param = {0};
  while ((pIter = taosHashIterate(pAppHbMgr->activeInfo, pIter))) {
    SClientHbReq *pOneReq = pIter;
    SClientHbKey *connKey = &pOneReq->connKey;
    STscObj      *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);

    if (!pTscObj) {
      continue;
    }

    pOneReq = taosArrayPush(pBatchReq->reqs, pOneReq);

    switch (connKey->connType) {
      case CONN_TYPE__QUERY: {
        if (param.clusterId == 0) {
          // init
          param.clusterId = pOneReq->clusterId;
          param.passVer = INT32_MIN;
        }
        param.passKeyCnt = atomic_load_32(&pAppHbMgr->passKeyCnt);
        break;
      }
      default:
        break;
    }
    if (clientHbMgr.reqHandle[connKey->connType]) {
      int32_t code = (*clientHbMgr.reqHandle[connKey->connType])(connKey, &param, pOneReq);
      if (code) {
        tscWarn("hbGatherAllInfo failed since %s, tscRid:%" PRIi64 ", connType:%" PRIi8, tstrerror(code),
                connKey->tscRid, connKey->connType);
      }
    }

    releaseTscObj(connKey->tscRid);
  }

  return pBatchReq;
}

void hbThreadFuncUnexpectedStopped(void) { atomic_store_8(&clientHbMgr.threadStop, 2); }

void hbMergeSummary(SAppClusterSummary *dst, SAppClusterSummary *src) {
  dst->numOfInsertsReq += src->numOfInsertsReq;
  dst->numOfInsertRows += src->numOfInsertRows;
  dst->insertElapsedTime += src->insertElapsedTime;
  dst->insertBytes += src->insertBytes;
  dst->fetchBytes += src->fetchBytes;
  dst->queryElapsedTime += src->queryElapsedTime;
  dst->numOfSlowQueries += src->numOfSlowQueries;
  dst->totalRequests += src->totalRequests;
  dst->currentRequests += src->currentRequests;
}

int32_t hbGatherAppInfo(void) {
  SAppHbReq req = {0};
  int       sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
  if (sz > 0) {
    req.pid = taosGetPId();
    req.appId = clientHbMgr.appId;
    taosGetAppName(req.name, NULL);
  }

  taosHashClear(clientHbMgr.appSummary);

  for (int32_t i = 0; i < sz; ++i) {
    SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pAppHbMgr == NULL) continue;

    uint64_t   clusterId = pAppHbMgr->pAppInstInfo->clusterId;
    SAppHbReq *pApp = taosHashGet(clientHbMgr.appSummary, &clusterId, sizeof(clusterId));
    if (NULL == pApp) {
      memcpy(&req.summary, &pAppHbMgr->pAppInstInfo->summary, sizeof(req.summary));
      req.startTime = pAppHbMgr->startTime;
      taosHashPut(clientHbMgr.appSummary, &clusterId, sizeof(clusterId), &req, sizeof(req));
    } else {
      if (pAppHbMgr->startTime < pApp->startTime) {
        pApp->startTime = pAppHbMgr->startTime;
      }

      hbMergeSummary(&pApp->summary, &pAppHbMgr->pAppInstInfo->summary);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void *hbThreadFunc(void *param) {
  setThreadName("hb");
#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(hbThreadFuncUnexpectedStopped);
  }
#endif
  while (1) {
    if (1 == clientHbMgr.threadStop) {
      break;
    }

    taosThreadMutexLock(&clientHbMgr.lock);

    int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
    if (sz > 0) {
      hbGatherAppInfo();
    }

    for (int i = 0; i < sz; i++) {
      SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);
      if (pAppHbMgr == NULL) {
        continue;
      }

      int32_t connCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
      if (connCnt == 0) {
        continue;
      }
      SClientHbBatchReq *pReq = hbGatherAllInfo(pAppHbMgr);
      if (pReq == NULL || taosArrayGetP(clientHbMgr.appHbMgrs, i) == NULL) {
        tFreeClientHbBatchReq(pReq);
        continue;
      }
      int   tlen = tSerializeSClientHbBatchReq(NULL, 0, pReq);
      void *buf = taosMemoryMalloc(tlen);
      if (buf == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tFreeClientHbBatchReq(pReq);
        // hbClearReqInfo(pAppHbMgr);
        break;
      }

      tSerializeSClientHbBatchReq(buf, tlen, pReq);
      SMsgSendInfo *pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));

      if (pInfo == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tFreeClientHbBatchReq(pReq);
        // hbClearReqInfo(pAppHbMgr);
        taosMemoryFree(buf);
        break;
      }
      pInfo->fp = hbAsyncCallBack;
      pInfo->msgInfo.pData = buf;
      pInfo->msgInfo.len = tlen;
      pInfo->msgType = TDMT_MND_HEARTBEAT;
      pInfo->param = taosMemoryMalloc(sizeof(int32_t));
      *(int32_t *)pInfo->param = i;
      pInfo->paramFreeFp = taosMemoryFree;
      pInfo->requestId = generateRequestId();
      pInfo->requestObjRefId = 0;

      SAppInstInfo *pAppInstInfo = pAppHbMgr->pAppInstInfo;
      int64_t       transporterId = 0;
      SEpSet        epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
      asyncSendMsgToServer(pAppInstInfo->pTransporter, &epSet, &transporterId, pInfo);
      tFreeClientHbBatchReq(pReq);
      // hbClearReqInfo(pAppHbMgr);

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

  // thread quit mode kill or inner exit from self-thread
  if (clientHbMgr.quitByKill) {
    taosThreadKill(clientHbMgr.thread, 0);
  } else {
    taosThreadJoin(clientHbMgr.thread, NULL);
  }

  tscDebug("hb thread stopped");
}

SAppHbMgr *appHbMgrInit(SAppInstInfo *pAppInstInfo, char *key) {
  if (hbMgrInit() != 0) {
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return NULL;
  }
  SAppHbMgr *pAppHbMgr = taosMemoryMalloc(sizeof(SAppHbMgr));
  if (pAppHbMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  // init stat
  pAppHbMgr->startTime = taosGetTimestampMs();
  pAppHbMgr->connKeyCnt = 0;
  pAppHbMgr->passKeyCnt = 0;
  pAppHbMgr->reportCnt = 0;
  pAppHbMgr->reportBytes = 0;
  pAppHbMgr->key = taosStrdup(key);

  // init app info
  pAppHbMgr->pAppInstInfo = pAppInstInfo;

  // init hash info
  pAppHbMgr->activeInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  if (pAppHbMgr->activeInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pAppHbMgr);
    return NULL;
  }

  // taosHashSetFreeFp(pAppHbMgr->activeInfo, tFreeClientHbReq);

  taosThreadMutexLock(&clientHbMgr.lock);
  taosArrayPush(clientHbMgr.appHbMgrs, &pAppHbMgr);
  pAppHbMgr->idx = taosArrayGetSize(clientHbMgr.appHbMgrs) - 1;
  taosThreadMutexUnlock(&clientHbMgr.lock);

  return pAppHbMgr;
}

void hbFreeAppHbMgr(SAppHbMgr *pTarget) {
  void *pIter = taosHashIterate(pTarget->activeInfo, NULL);
  while (pIter != NULL) {
    SClientHbReq *pOneReq = pIter;
    tFreeClientHbReq(pOneReq);
    pIter = taosHashIterate(pTarget->activeInfo, pIter);
  }
  taosHashCleanup(pTarget->activeInfo);
  pTarget->activeInfo = NULL;

  taosMemoryFree(pTarget->key);
  taosMemoryFree(pTarget);
}

void hbRemoveAppHbMrg(SAppHbMgr **pAppHbMgr) {
  taosThreadMutexLock(&clientHbMgr.lock);
  int32_t mgrSize = taosArrayGetSize(clientHbMgr.appHbMgrs);
  for (int32_t i = 0; i < mgrSize; ++i) {
    SAppHbMgr *pItem = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pItem == *pAppHbMgr) {
      hbFreeAppHbMgr(*pAppHbMgr);
      *pAppHbMgr = NULL;
      taosArraySet(clientHbMgr.appHbMgrs, i, pAppHbMgr);
      break;
    }
  }
  taosThreadMutexUnlock(&clientHbMgr.lock);
}

void appHbMgrCleanup(void) {
  int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
  for (int i = 0; i < sz; i++) {
    SAppHbMgr *pTarget = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pTarget == NULL) continue;
    hbFreeAppHbMgr(pTarget);
  }
}

int hbMgrInit() {
  // init once
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 0, 1);
  if (old == 1) return 0;

  clientHbMgr.appId = tGenIdPI64();
  tscDebug("app %" PRIx64 " initialized", clientHbMgr.appId);

  clientHbMgr.appSummary = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  clientHbMgr.appHbMgrs = taosArrayInit(0, sizeof(void *));

  TdThreadMutexAttr attr = {0};

  int ret = taosThreadMutexAttrInit(&attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrInit error") return ret;
  }

  ret = taosThreadMutexAttrSetType(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrSetType error") return ret;
  }

  ret = taosThreadMutexInit(&clientHbMgr.lock, &attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexInit error") return ret;
  }

  ret = taosThreadMutexAttrDestroy(&attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrDestroy error") return ret;
  }

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

int hbRegisterConnImpl(SAppHbMgr *pAppHbMgr, SClientHbKey connKey, int64_t clusterId) {
  // init hash in activeinfo
  void *data = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  if (data != NULL) {
    return 0;
  }
  SClientHbReq hbReq = {0};
  hbReq.connKey = connKey;
  hbReq.clusterId = clusterId;
  // hbReq.info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  taosHashPut(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey), &hbReq, sizeof(SClientHbReq));

  atomic_add_fetch_32(&pAppHbMgr->connKeyCnt, 1);
  return 0;
}

int hbRegisterConn(SAppHbMgr *pAppHbMgr, int64_t tscRefId, int64_t clusterId, int8_t connType) {
  SClientHbKey connKey = {
      .tscRid = tscRefId,
      .connType = connType,
  };

  switch (connType) {
    case CONN_TYPE__QUERY: {
      return hbRegisterConnImpl(pAppHbMgr, connKey, clusterId);
    }
    case CONN_TYPE__TMQ: {
      return 0;
    }
    default:
      return 0;
  }
}

void hbDeregisterConn(STscObj *pTscObj, SClientHbKey connKey) {
  SAppHbMgr    *pAppHbMgr = pTscObj->pAppInfo->pAppHbMgr;
  SClientHbReq *pReq = taosHashAcquire(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  if (pReq) {
    tFreeClientHbReq(pReq);
    taosHashRemove(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
    taosHashRelease(pAppHbMgr->activeInfo, pReq);
  }

  if (NULL == pReq) {
    return;
  }

  atomic_sub_fetch_32(&pAppHbMgr->connKeyCnt, 1);

  taosThreadMutexLock(&pTscObj->mutex);
  if (pTscObj->passInfo.fp) {
    atomic_sub_fetch_32(&pAppHbMgr->passKeyCnt, 1);
  }
  taosThreadMutexUnlock(&pTscObj->mutex);
}

// set heart beat thread quit mode , if quicByKill 1 then kill thread else quit from inner
void taos_set_hb_quit(int8_t quitByKill) {
  clientHbMgr.quitByKill = quitByKill;
}
