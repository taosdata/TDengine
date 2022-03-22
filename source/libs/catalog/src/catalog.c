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

#include "trpc.h"
#include "query.h"
#include "tname.h"
#include "catalogInt.h"

int32_t ctgActUpdateVg(SCtgMetaAction *action);
int32_t ctgActUpdateTbl(SCtgMetaAction *action);
int32_t ctgActRemoveDB(SCtgMetaAction *action);
int32_t ctgActRemoveStb(SCtgMetaAction *action);
int32_t ctgActRemoveTbl(SCtgMetaAction *action);

SCatalogMgmt gCtgMgmt = {0};
SCtgDebug gCTGDebug = {0};
SCtgAction gCtgAction[CTG_ACT_MAX] = {{
                            CTG_ACT_UPDATE_VG,
                            "update vgInfo",
                            ctgActUpdateVg
                          },
                          {
                            CTG_ACT_UPDATE_TBL,
                            "update tbMeta",
                            ctgActUpdateTbl
                          },
                          {
                            CTG_ACT_REMOVE_DB,
                            "remove DB",
                            ctgActRemoveDB
                          },
                          {
                            CTG_ACT_REMOVE_STB,
                            "remove stbMeta",
                            ctgActRemoveStb
                          },
                          {
                            CTG_ACT_REMOVE_TBL,
                            "remove tbMeta",
                            ctgActRemoveTbl
                          }
};

int32_t ctgDbgEnableDebug(char *option) {
  if (0 == strcasecmp(option, "lock")) {
    gCTGDebug.lockDebug = true;
    qDebug("lock debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "cache")) {
    gCTGDebug.cacheDebug = true;
    qDebug("cache debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "api")) {
    gCTGDebug.apiDebug = true;
    qDebug("api debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "meta")) {
    gCTGDebug.metaDebug = true;
    qDebug("api debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid debug option:%s", option);
  
  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgDbgGetStatNum(char *option, void *res) {
  if (0 == strcasecmp(option, "runtime.qDoneNum")) {
    *(uint64_t *)res = atomic_load_64(&gCtgMgmt.stat.runtime.qDoneNum);
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid stat option:%s", option);
  
  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgDbgGetTbMetaNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.metaCache ? (int32_t)taosHashGetSize(dbCache->tbCache.metaCache) : 0;
}

int32_t ctgDbgGetStbNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.stbCache ? (int32_t)taosHashGetSize(dbCache->tbCache.stbCache) : 0;
}

int32_t ctgDbgGetRentNum(SCtgRentMgmt *rent) {
  int32_t num = 0;
  for (uint16_t i = 0; i < rent->slotNum; ++i) {
    SCtgRentSlot *slot = &rent->slots[i];
    if (NULL == slot->meta) {
      continue;
    }

    num += taosArrayGetSize(slot->meta);
  }

  return num;
}

int32_t ctgDbgGetClusterCacheNum(SCatalog* pCtg, int32_t type) {
  if (NULL == pCtg || NULL == pCtg->dbCache) {
    return 0;
  }

  switch (type) {
    case CTG_DBG_DB_NUM:
      return (int32_t)taosHashGetSize(pCtg->dbCache);
    case CTG_DBG_DB_RENT_NUM:
      return ctgDbgGetRentNum(&pCtg->dbRent);
    case CTG_DBG_STB_RENT_NUM:
      return ctgDbgGetRentNum(&pCtg->stbRent);
    default:
      break;
  }

  SCtgDBCache *dbCache = NULL;
  int32_t num = 0;
  void *pIter = taosHashIterate(pCtg->dbCache, NULL);
  while (pIter) {
    dbCache = (SCtgDBCache *)pIter;
    switch (type) {
      case CTG_DBG_META_NUM:
        num += ctgDbgGetTbMetaNum(dbCache);
        break;
      case CTG_DBG_STB_NUM:
        num += ctgDbgGetStbNum(dbCache);
        break;
      default:
        ctgError("invalid type:%d", type);
        break;
    }
    pIter = taosHashIterate(pCtg->dbCache, pIter);
  }

  return num;
}

void ctgDbgShowTableMeta(SCatalog* pCtg, const char *tbName, STableMeta* p) {
  if (!gCTGDebug.metaDebug) {
    return;
  }

  STableComInfo *c = &p->tableInfo;

  if (TSDB_CHILD_TABLE == p->tableType) {
    ctgDebug("table [%s] meta: type:%d, vgId:%d, uid:%" PRIx64 ",suid:%" PRIx64, tbName, p->tableType, p->vgId, p->uid, p->suid);
    return;
  } else {
    ctgDebug("table [%s] meta: type:%d, vgId:%d, uid:%" PRIx64 ",suid:%" PRIx64 ",sv:%d, tv:%d, tagNum:%d, precision:%d, colNum:%d, rowSize:%d",
     tbName, p->tableType, p->vgId, p->uid, p->suid, p->sversion, p->tversion, c->numOfTags, c->precision, c->numOfColumns, c->rowSize);
  }

  int32_t colNum = c->numOfColumns + c->numOfTags;
  for (int32_t i = 0; i < colNum; ++i) {
    SSchema *s = &p->schema[i];
    ctgDebug("[%d] name:%s, type:%d, colId:%d, bytes:%d", i, s->name, s->type, s->colId, s->bytes);
  }
}

void ctgDbgShowDBCache(SCatalog* pCtg, SHashObj *dbHash) {
  if (NULL == dbHash || !gCTGDebug.cacheDebug) {
    return;
  }

  int32_t i = 0;
  SCtgDBCache *dbCache = NULL;
  void *pIter = taosHashIterate(dbHash, NULL);
  while (pIter) {
    char *dbFName = NULL;
    size_t len = 0;
    
    dbCache = (SCtgDBCache *)pIter;

    taosHashGetKey((void **)&dbFName, &len);

    int32_t metaNum = dbCache->tbCache.metaCache ? taosHashGetSize(dbCache->tbCache.metaCache) : 0;
    int32_t stbNum = dbCache->tbCache.stbCache ? taosHashGetSize(dbCache->tbCache.stbCache) : 0;
    int32_t vgVersion = CTG_DEFAULT_INVALID_VERSION;
    int32_t hashMethod = -1;
    int32_t vgNum = 0;

    if (dbCache->vgInfo) {
      vgVersion = dbCache->vgInfo->vgVersion;
      hashMethod = dbCache->vgInfo->hashMethod;
      if (dbCache->vgInfo->vgHash) {
        vgNum = taosHashGetSize(dbCache->vgInfo->vgHash);
      }
    }
    
    ctgDebug("[%d] db [%.*s][%"PRIx64"] %s: metaNum:%d, stbNum:%d, vgVersion:%d, hashMethod:%d, vgNum:%d",
      i, (int32_t)len, dbFName, dbCache->dbId, dbCache->deleted?"deleted":"", metaNum, stbNum, vgVersion, hashMethod, vgNum);

    pIter = taosHashIterate(dbHash, pIter);
  }
}




void ctgDbgShowClusterCache(SCatalog* pCtg) {
  if (!gCTGDebug.cacheDebug || NULL == pCtg) {
    return;
  }

  ctgDebug("## cluster %"PRIx64" %p cache Info ##", pCtg->clusterId, pCtg);
  ctgDebug("db:%d meta:%d stb:%d dbRent:%d stbRent:%d", ctgDbgGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM), ctgDbgGetClusterCacheNum(pCtg, CTG_DBG_META_NUM),
    ctgDbgGetClusterCacheNum(pCtg, CTG_DBG_STB_NUM), ctgDbgGetClusterCacheNum(pCtg, CTG_DBG_DB_RENT_NUM), ctgDbgGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM));    
  
  ctgDbgShowDBCache(pCtg, pCtg->dbCache);
}



void ctgPopAction(SCtgMetaAction **action) {
  SCtgQNode *orig = gCtgMgmt.head;
  
  SCtgQNode *node = gCtgMgmt.head->next;
  gCtgMgmt.head = gCtgMgmt.head->next;

  CTG_QUEUE_SUB();
  
  tfree(orig);

  *action = &node->action;
}


int32_t ctgPushAction(SCtgMetaAction *action) {
  SCtgQNode *node = calloc(1, sizeof(SCtgQNode));
  if (NULL == node) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_RET(TSDB_CODE_CTG_MEM_ERROR);
  }
  
  node->action = *action;

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.qlock);
  gCtgMgmt.tail->next = node;
  gCtgMgmt.tail = node;
  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.qlock);

  CTG_QUEUE_ADD();
  CTG_STAT_ADD(gCtgMgmt.stat.runtime.qNum);

  tsem_post(&gCtgMgmt.sem);

  return TSDB_CODE_SUCCESS;
}


void ctgFreeMetaRent(SCtgRentMgmt *mgmt) {
  if (NULL == mgmt->slots) {
    return;
  }

  for (int32_t i = 0; i < mgmt->slotNum; ++i) {
    SCtgRentSlot *slot = &mgmt->slots[i];
    if (slot->meta) {
      taosArrayDestroy(slot->meta);
      slot->meta = NULL;
    }
  }

  tfree(mgmt->slots);
}


int32_t ctgPushRmDBMsgInQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_DB};
  SCtgRemoveDBMsg *msg = malloc(sizeof(SCtgRemoveDBMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgRemoveDBMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->dbId = dbId;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  return TSDB_CODE_SUCCESS;

_return:

  tfree(action.data);
  CTG_RET(code);
}


int32_t ctgPushRmStbMsgInQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *stbName, uint64_t suid) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_STB};
  SCtgRemoveStbMsg *msg = malloc(sizeof(SCtgRemoveStbMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgRemoveStbMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  strncpy(msg->stbName, stbName, sizeof(msg->stbName));
  msg->dbId = dbId;
  msg->suid = suid;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  return TSDB_CODE_SUCCESS;

_return:

  tfree(action.data);
  CTG_RET(code);
}



int32_t ctgPushRmTblMsgInQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *tbName) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_TBL};
  SCtgRemoveTblMsg *msg = malloc(sizeof(SCtgRemoveTblMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgRemoveTblMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  strncpy(msg->tbName, tbName, sizeof(msg->tbName));
  msg->dbId = dbId;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  return TSDB_CODE_SUCCESS;

_return:

  tfree(action.data);
  CTG_RET(code);
}


void ctgFreeTableMetaCache(SCtgTbMetaCache *cache) {
  CTG_LOCK(CTG_WRITE, &cache->stbLock);
  if (cache->stbCache) {
    taosHashCleanup(cache->stbCache);
    cache->stbCache = NULL;
  }
  CTG_UNLOCK(CTG_WRITE, &cache->stbLock);

  CTG_LOCK(CTG_WRITE, &cache->metaLock);
  if (cache->metaCache) {
    taosHashCleanup(cache->metaCache);
    cache->metaCache = NULL;
  }
  CTG_UNLOCK(CTG_WRITE, &cache->metaLock);
}

void ctgFreeVgInfo(SDBVgInfo *vgInfo) {
  if (NULL == vgInfo) {
    return;
  }

  if (vgInfo->vgHash) {
    taosHashCleanup(vgInfo->vgHash);
    vgInfo->vgHash = NULL;
  }
  
  tfree(vgInfo);
}

void ctgFreeDbCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache) {
    return;
  }

  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
  ctgFreeVgInfo (dbCache->vgInfo);
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);

  ctgFreeTableMetaCache(&dbCache->tbCache);
}


void ctgFreeHandle(SCatalog* pCtg) {
  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);
  
  if (pCtg->dbCache) {
    void *pIter = taosHashIterate(pCtg->dbCache, NULL);
    while (pIter) {
      SCtgDBCache *dbCache = pIter;

      atomic_store_8(&dbCache->deleted, 1);

      ctgFreeDbCache(dbCache);
            
      pIter = taosHashIterate(pCtg->dbCache, pIter);
    }  

    taosHashCleanup(pCtg->dbCache);
  }
  
  free(pCtg);
}


int32_t ctgAcquireVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache, bool *inCache) {
  CTG_LOCK(CTG_READ, &dbCache->vgLock);
  
  if (dbCache->deleted) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);

    ctgDebug("db is dropping, dbId:%"PRIx64, dbCache->dbId);
    
    *inCache = false;
    return TSDB_CODE_SUCCESS;
  }

  
  if (NULL == dbCache->vgInfo) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);

    *inCache = false;
    ctgDebug("db vgInfo is empty, dbId:%"PRIx64, dbCache->dbId);
    return TSDB_CODE_SUCCESS;
  }

  *inCache = true;
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgWAcquireVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache) {
  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);

  if (dbCache->deleted) {
    ctgDebug("db is dropping, dbId:%"PRIx64, dbCache->dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  return TSDB_CODE_SUCCESS;
}

void ctgReleaseDBCache(SCatalog *pCtg, SCtgDBCache *dbCache) {
  taosHashRelease(pCtg->dbCache, dbCache);
}

void ctgReleaseVgInfo(SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_READ, &dbCache->vgLock);
}

void ctgWReleaseVgInfo(SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
}


int32_t ctgAcquireDBCacheImpl(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache, bool acquire) {
  SCtgDBCache *dbCache = NULL;
  if (acquire) {
    dbCache = (SCtgDBCache *)taosHashAcquire(pCtg->dbCache, dbFName, strlen(dbFName));
  } else {
    dbCache = (SCtgDBCache *)taosHashGet(pCtg->dbCache, dbFName, strlen(dbFName));
  }
  
  if (NULL == dbCache) {
    *pCache = NULL;
    ctgDebug("db not in cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  if (dbCache->deleted) {
    if (acquire) {
      ctgReleaseDBCache(pCtg, dbCache);
    }    
    
    *pCache = NULL;
    ctgDebug("db is removing from cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  *pCache = dbCache;
    
  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireDBCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache) {
  CTG_RET(ctgAcquireDBCacheImpl(pCtg, dbFName, pCache, true));
}

int32_t ctgGetDBCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache) {
  CTG_RET(ctgAcquireDBCacheImpl(pCtg, dbFName, pCache, false));
}


int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache, bool *inCache) {
  if (NULL == pCtg->dbCache) {
    *pCache = NULL;
    *inCache = false;
    ctgWarn("empty db cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {  
    *pCache = NULL;
    *inCache = false;
    return TSDB_CODE_SUCCESS;
  }
  
  ctgAcquireVgInfo(pCtg, dbCache, inCache);
  if (!(*inCache)) {
    ctgReleaseDBCache(pCtg, dbCache);
  
    *pCache = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pCache = dbCache;
  *inCache = true;

  ctgDebug("Got db vgInfo from cache, dbFName:%s", dbFName);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetQnodeListFromMnode(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, SArray **out) {
  char *msg = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get qnode list from mnode, mgmtEpInUse:%d", pMgmtEps->inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_MND_QNODE_LIST)](NULL, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build qnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }
  
  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_QNODE_LIST,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    ctgError("error rsp for qnode list, error:%s", tstrerror(rpcRsp.code));
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_QNODE_LIST)](out, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process qnode list rsp failed, error:%s", tstrerror(rpcRsp.code));
    CTG_ERR_RET(code);
  }

  ctgDebug("Got qnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(*out));

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetDBVgInfoFromMnode(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, SBuildUseDBInput *input, SUseDbOutput *out) {
  char *msg = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get db vgInfo from mnode, dbFName:%s", input->db);

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)](input, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build use db msg failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }
  
  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_USE_DB,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    ctgError("error rsp for use db, error:%s, db:%s", tstrerror(rpcRsp.code), input->db);
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)](out, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process use db rsp failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }

  ctgDebug("Got db vgInfo from mnode, dbFName:%s", input->db);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgIsTableMetaExistInCache(SCatalog* pCtg, char *dbFName, char* tbName, int32_t *exist) {
  if (NULL == pCtg->dbCache) {
    *exist = 0;
    ctgWarn("empty db cache, dbFName:%s, tbName:%s", dbFName, tbName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  size_t sz = 0;
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);  
  STableMeta *tbMeta = taosHashGet(dbCache->tbCache.metaCache, tbName, strlen(tbName));
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
  
  if (NULL == tbMeta) {
    ctgReleaseDBCache(pCtg, dbCache);
    
    *exist = 0;
    ctgDebug("tbmeta not in cache, dbFName:%s, tbName:%s", dbFName, tbName);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;

  ctgReleaseDBCache(pCtg, dbCache);
  
  ctgDebug("tbmeta is in cache, dbFName:%s, tbName:%s", dbFName, tbName);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromCache(SCatalog* pCtg, const SName* pTableName, STableMeta** pTableMeta, int32_t *exist, int32_t flag, uint64_t *dbId) {
  if (NULL == pCtg->dbCache) {
    *exist = 0;
    ctgWarn("empty tbmeta cache, tbName:%s", pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if (CTG_FLAG_IS_INF_DB(flag)) {
    strcpy(dbFName, pTableName->dbname);
  } else {
    tNameGetFullDbName(pTableName, dbFName);
  }

  *pTableMeta = NULL;

  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t sz = 0;
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  int32_t code = taosHashGetDup_m(dbCache->tbCache.metaCache, pTableName->tname, strlen(pTableName->tname), (void **)pTableMeta, &sz);
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  if (NULL == *pTableMeta) {
    *exist = 0;
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("tbl not in cache, dbFName:%s, tbName:%s", dbFName, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  if (dbId) {
    *dbId = dbCache->dbId;
  }

  STableMeta* tbMeta = *pTableMeta;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("Got meta from cache, type:%d, dbFName:%s, tbName:%s", tbMeta->tableType, dbFName, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &dbCache->tbCache.stbLock);
  
  STableMeta **stbMeta = taosHashGet(dbCache->tbCache.stbCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgReleaseDBCache(pCtg, dbCache);
    ctgError("stb not in stbCache, suid:%"PRIx64, tbMeta->suid);
    tfree(*pTableMeta);
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgReleaseDBCache(pCtg, dbCache);
    tfree(*pTableMeta);
    ctgError("stable suid in stbCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = CTG_META_SIZE(*stbMeta);
  *pTableMeta = realloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgReleaseDBCache(pCtg, dbCache);
    ctgError("realloc size[%d] failed", metaSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);

  ctgReleaseDBCache(pCtg, dbCache);

  ctgDebug("Got tbmeta from cache, dbFName:%s, tbName:%s", dbFName, pTableName->tname);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableTypeFromCache(SCatalog* pCtg, const SName* pTableName, int32_t *tbType, int32_t flag) {
  if (NULL == pCtg->dbCache) {
    ctgWarn("empty db cache, tbName:%s", pTableName->tname);  
    return TSDB_CODE_SUCCESS;
  }

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if (CTG_FLAG_IS_INF_DB(flag)) {
    strcpy(dbFName, pTableName->dbname);
  } else {
    tNameGetFullDbName(pTableName, dbFName);
  }
  
  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  STableMeta *pTableMeta = (STableMeta *)taosHashAcquire(dbCache->tbCache.metaCache, pTableName->tname, strlen(pTableName->tname));

  if (NULL == pTableMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
    ctgWarn("tbl not in cache, dbFName:%s, tbName:%s", dbFName, pTableName->tname);  
    ctgReleaseDBCache(pCtg, dbCache);
    
    return TSDB_CODE_SUCCESS;
  }

  *tbType = atomic_load_8(&pTableMeta->tableType);

  taosHashRelease(dbCache->tbCache.metaCache, pTableMeta);

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  ctgReleaseDBCache(pCtg, dbCache);

  ctgDebug("Got tbtype from cache, dbFName:%s, tbName:%s, type:%d", dbFName, pTableName->tname, *tbType);  
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnodeImpl(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, char *dbFName, char* tbName, STableMetaOutput* output) {
  SBuildTableMetaInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = tbName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get table meta from mnode, dbFName:%s, tbName:%s", dbFName, tbName);

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_MND_TABLE_META)](&bInput, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build mnode stablemeta msg failed, code:%x", code);
    CTG_ERR_RET(code);
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pTrans, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      SET_META_TYPE_NULL(output->metaType);
      ctgDebug("stablemeta not exist in mnode, dbFName:%s, tbName:%s", dbFName, tbName);
      return TSDB_CODE_SUCCESS;
    }
    
    ctgError("error rsp for stablemeta from mnode, code:%s, dbFName:%s, tbName:%s", tstrerror(rpcRsp.code), dbFName, tbName);
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_TABLE_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process mnode stablemeta rsp failed, code:%x, dbFName:%s, tbName:%s", code, dbFName, tbName);
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from mnode, dbFName:%s, tbName:%s", dbFName, tbName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnode(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, STableMetaOutput* output) {
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  return ctgGetTableMetaFromMnodeImpl(pCtg, pTrans, pMgmtEps, dbFName, (char *)pTableName->tname, output);
}

int32_t ctgGetTableMetaFromVnode(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCtg || NULL == pTrans || NULL == pMgmtEps || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  ctgDebug("try to get table meta from vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char *)tNameGetTableName(pTableName)};
  char *msg = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)](&bInput, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build vnode tablemeta msg failed, code:%x, dbFName:%s, tbName:%s", code, dbFName, tNameGetTableName(pTableName));
    CTG_ERR_RET(code);
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_VND_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      SET_META_TYPE_NULL(output->metaType);
      ctgDebug("tablemeta not exist in vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));
      return TSDB_CODE_SUCCESS;
    }
  
    ctgError("error rsp for table meta from vnode, code:%s, dbFName:%s, tbName:%s", tstrerror(rpcRsp.code), dbFName, tNameGetTableName(pTableName));
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process vnode tablemeta rsp failed, code:%s, dbFName:%s, tbName:%s", tstrerror(code), dbFName, tNameGetTableName(pTableName));
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetHashFunction(int8_t hashMethod, tableNameHashFp *fp) {
  switch (hashMethod) {
    default:
      *fp = MurmurHash3_32;
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGenerateVgList(SCatalog *pCtg, SHashObj *vgHash, SArray** pList) {
  SHashObj *vgroupHash = NULL;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;
  int32_t code = 0;
  int32_t vgNum = taosHashGetSize(vgHash);

  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed, num:%d", vgNum);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      taosHashCancelIterate(vgHash, pIter);      
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(vgHash, pIter);
    vgInfo = NULL;
  }

  *pList = vgList;

  ctgDebug("Got vgList from cache, vgNum:%d", vgNum);

  return TSDB_CODE_SUCCESS;

_return:

  if (vgList) {
    taosArrayDestroy(vgList);
  }

  CTG_RET(code);
}

int32_t ctgGetVgInfoFromHashValue(SCatalog *pCtg, SDBVgInfo *dbInfo, const SName *pTableName, SVgroupInfo *pVgroup) {
  int32_t code = 0;
  
  int32_t vgNum = taosHashGetSize(dbInfo->vgHash);
  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", db, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  tableNameHashFp fp = NULL;
  SVgroupInfo *vgInfo = NULL;

  CTG_ERR_RET(ctgGetHashFunction(dbInfo->hashMethod, &fp));

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  uint32_t hashValue = (*fp)(tbFullName, (uint32_t)strlen(tbFullName));

  void *pIter = taosHashIterate(dbInfo->vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;
    if (hashValue >= vgInfo->hashBegin && hashValue <= vgInfo->hashEnd) {
      taosHashCancelIterate(dbInfo->vgHash, pIter);
      break;
    }
    
    pIter = taosHashIterate(dbInfo->vgHash, pIter);
    vgInfo = NULL;
  }

  if (NULL == vgInfo) {
    ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, db, taosHashGetSize(dbInfo->vgHash));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

  CTG_RET(code);
}

int32_t ctgStbVersionSearchCompare(const void* key1, const void* key2) {
  if (*(uint64_t *)key1 < ((SSTableMetaVersion*)key2)->suid) {
    return -1;
  } else if (*(uint64_t *)key1 > ((SSTableMetaVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbVgVersionSearchCompare(const void* key1, const void* key2) {
  if (*(int64_t *)key1 < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (*(int64_t *)key1 > ((SDbVgVersion*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgStbVersionSortCompare(const void* key1, const void* key2) {
  if (((SSTableMetaVersion*)key1)->suid < ((SSTableMetaVersion*)key2)->suid) {
    return -1;
  } else if (((SSTableMetaVersion*)key1)->suid > ((SSTableMetaVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbVgVersionSortCompare(const void* key1, const void* key2) {
  if (((SDbVgVersion*)key1)->dbId < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (((SDbVgVersion*)key1)->dbId > ((SDbVgVersion*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}


int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;

  size_t msgSize = sizeof(SCtgRentSlot) * mgmt->slotNum;
  
  mgmt->slots = calloc(1, msgSize);
  if (NULL == mgmt->slots) {
    qError("calloc %d failed", (int32_t)msgSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  qDebug("meta rent initialized, type:%d, slotNum:%d", type, mgmt->slotNum);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgMetaRentAdd(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    slot->meta = taosArrayInit(CTG_DEFAULT_RENT_SLOT_SIZE, size);
    if (NULL == slot->meta) {
      qError("taosArrayInit %d failed, id:%"PRIx64", slot idx:%d, type:%d", CTG_DEFAULT_RENT_SLOT_SIZE, id, widx, mgmt->type);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  if (NULL == taosArrayPush(slot->meta, meta)) {
    qError("taosArrayPush meta to rent failed, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  slot->needSort = true;

  qDebug("add meta to rent, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);
  CTG_RET(code);
}

int32_t ctgMetaRentUpdate(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size, __compar_fn_t sortCompare, __compar_fn_t searchCompare) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t code = 0;

  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("empty meta slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    qDebug("meta slot before sorte, slot idx:%d, type:%d, size:%d", widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d, size:%d", widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
  }

  void *orig = taosArraySearch(slot->meta, &id, searchCompare, TD_EQ);
  if (NULL == orig) {
    qError("meta not found in slot, id:%"PRIx64", slot idx:%d, type:%d, size:%d", id, widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  memcpy(orig, meta, size);

  qDebug("meta in rent updated, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  if (code) {
    qWarn("meta in rent update failed, will try to add it, code:%x, id:%"PRIx64", slot idx:%d, type:%d", code, id, widx, mgmt->type);
    CTG_RET(ctgMetaRentAdd(mgmt, meta, id, size));
  }

  CTG_RET(code);
}

int32_t ctgMetaRentRemove(SCtgRentMgmt *mgmt, int64_t id, __compar_fn_t sortCompare, __compar_fn_t searchCompare) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("empty meta slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d", widx, mgmt->type);
  }

  int32_t idx = taosArraySearchIdx(slot->meta, &id, searchCompare, TD_EQ);
  if (idx < 0) {
    qError("meta not found in slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  taosArrayRemove(slot->meta, idx);

  qDebug("meta in rent removed, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  CTG_RET(code);
}


int32_t ctgMetaRentGetImpl(SCtgRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
  int16_t ridx = atomic_add_fetch_16(&mgmt->slotRIdx, 1);
  if (ridx >= mgmt->slotNum) {
    ridx %= mgmt->slotNum;
    atomic_store_16(&mgmt->slotRIdx, ridx);
  }

  SCtgRentSlot *slot = &mgmt->slots[ridx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_READ, &slot->lock);
  if (NULL == slot->meta) {
    qDebug("empty meta in slot:%d, type:%d", ridx, mgmt->type);
    *num = 0;
    goto _return;
  }

  size_t metaNum = taosArrayGetSize(slot->meta);
  if (metaNum <= 0) {
    qDebug("no meta in slot:%d, type:%d", ridx, mgmt->type);
    *num = 0;
    goto _return;
  }

  size_t msize = metaNum * size;
  *res = malloc(msize);
  if (NULL == *res) {
    qError("malloc %d failed", (int32_t)msize);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  void *meta = taosArrayGet(slot->meta, 0);

  memcpy(*res, meta, msize);

  *num = (uint32_t)metaNum;

  qDebug("Got %d meta from rent, type:%d", (int32_t)metaNum, mgmt->type);

_return:

  CTG_UNLOCK(CTG_READ, &slot->lock);

  CTG_RET(code);
}

int32_t ctgMetaRentGet(SCtgRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
  while (true) {
    int64_t msec = taosGetTimestampMs();
    int64_t lsec = atomic_load_64(&mgmt->lastReadMsec);
    if ((msec - lsec) < CTG_RENT_SLOT_SECOND * 1000) {
      *res = NULL;
      *num = 0;
      qDebug("too short time period to get expired meta, type:%d", mgmt->type);
      return TSDB_CODE_SUCCESS;
    }

    if (lsec != atomic_val_compare_exchange_64(&mgmt->lastReadMsec, lsec, msec)) {
      continue;
    }

    break;
  }

  CTG_ERR_RET(ctgMetaRentGetImpl(mgmt, res, num, size));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAddNewDBCache(SCatalog *pCtg, const char *dbFName, uint64_t dbId) {
  int32_t code = 0;

  SCtgDBCache newDBCache = {0};
  newDBCache.dbId = dbId;

  newDBCache.tbCache.metaCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.tbCache.metaCache) {
    ctgError("taosHashInit %d metaCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  newDBCache.tbCache.stbCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.tbCache.stbCache) {
    ctgError("taosHashInit %d stbCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  code = taosHashPut(pCtg->dbCache, dbFName, strlen(dbFName), &newDBCache, sizeof(SCtgDBCache));
  if (code) {
    if (HASH_NODE_EXIST(code)) {
      ctgDebug("db already in cache, dbFName:%s", dbFName);
      goto _return;
    }
    
    ctgError("taosHashPut db to cache failed, dbFName:%s", dbFName);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }
  
  SDbVgVersion vgVersion = {.dbId = newDBCache.dbId, .vgVersion = -1};
  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));

  ctgDebug("db added to cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbId);

  if (CTG_IS_INF_DBNAME(dbFName)) {
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_RET(ctgMetaRentAdd(&pCtg->dbRent, &vgVersion, dbId, sizeof(SDbVgVersion)));

  ctgDebug("db added to rent, dbFName:%s, vgVersion:%d, dbId:%"PRIx64, dbFName, vgVersion.vgVersion, dbId);

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeDbCache(&newDBCache);

  CTG_RET(code);
}


void ctgRemoveStbRent(SCatalog* pCtg, SCtgTbMetaCache *cache) {
  CTG_LOCK(CTG_WRITE, &cache->stbLock);
  if (cache->stbCache) {
    void *pIter = taosHashIterate(cache->stbCache, NULL);
    while (pIter) {
      uint64_t *suid = NULL;
      suid = taosHashGetKey(pIter, NULL);

      if (TSDB_CODE_SUCCESS == ctgMetaRentRemove(&pCtg->stbRent, *suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare)) {
        ctgDebug("stb removed from rent, suid:%"PRIx64, *suid);
      }
          
      pIter = taosHashIterate(cache->stbCache, pIter);
    }
  }
  CTG_UNLOCK(CTG_WRITE, &cache->stbLock);
}


int32_t ctgRemoveDB(SCatalog* pCtg, SCtgDBCache *dbCache, const char* dbFName) {
  uint64_t dbId = dbCache->dbId;
  
  ctgInfo("start to remove db from cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);

  atomic_store_8(&dbCache->deleted, 1);

  ctgRemoveStbRent(pCtg, &dbCache->tbCache);

  ctgFreeDbCache(dbCache);

  ctgInfo("db removed from cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);

  CTG_ERR_RET(ctgMetaRentRemove(&pCtg->dbRent, dbCache->dbId, ctgDbVgVersionSortCompare, ctgDbVgVersionSearchCompare));
  
  ctgDebug("db removed from rent, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);

  if (taosHashRemove(pCtg->dbCache, dbFName, strlen(dbFName))) {
    ctgInfo("taosHashRemove from dbCache failed, may be removed, dbFName:%s", dbFName);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  ctgInfo("db removed from cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbId);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetAddDBCache(SCatalog* pCtg, const char *dbFName, uint64_t dbId, SCtgDBCache **pCache) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(pCtg, dbFName, &dbCache);
  
  if (dbCache) {
  // TODO OPEN IT
#if 0    
    if (dbCache->dbId == dbId) {
      *pCache = dbCache;
      return TSDB_CODE_SUCCESS;
    }
#else
    if (0 == dbId) {
      *pCache = dbCache;
      return TSDB_CODE_SUCCESS;
    }

    if (dbId && (dbCache->dbId == 0)) {
      dbCache->dbId = dbId;
      *pCache = dbCache;
      return TSDB_CODE_SUCCESS;
    }
    
    if (dbCache->dbId == dbId) {
      *pCache = dbCache;
      return TSDB_CODE_SUCCESS;
    }
#endif
    CTG_ERR_RET(ctgRemoveDB(pCtg, dbCache, dbFName));
  }
  
  CTG_ERR_RET(ctgAddNewDBCache(pCtg, dbFName, dbId));

  ctgGetDBCache(pCtg, dbFName, &dbCache);

  *pCache = dbCache;

  return TSDB_CODE_SUCCESS;
}


int32_t ctgUpdateDBVgInfo(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDBVgInfo** pDbInfo) {
  int32_t code = 0;
  SDBVgInfo* dbInfo = *pDbInfo;
  
  if (NULL == dbInfo->vgHash || dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgHash) <= 0) {
    ctgError("invalid db vgInfo, dbFName:%s, vgHash:%p, vgVersion:%d", dbFName, dbInfo->vgHash, dbInfo->vgVersion);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  bool newAdded = false;
  SDbVgVersion vgVersion = {.dbId = dbId, .vgVersion = dbInfo->vgVersion, .numOfTable = dbInfo->numOfTable};

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_RET(ctgGetAddDBCache(pCtg, dbFName, dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:%"PRIx64, dbFName, dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SDBVgInfo *vgInfo = NULL;
  CTG_ERR_RET(ctgWAcquireVgInfo(pCtg, dbCache));
  
  if (dbCache->vgInfo) {
    if (dbInfo->vgVersion < dbCache->vgInfo->vgVersion) {
      ctgDebug("db vgVersion is old, dbFName:%s, vgVersion:%d, currentVersion:%d", dbFName, dbInfo->vgVersion, dbCache->vgInfo->vgVersion);
      ctgWReleaseVgInfo(dbCache);
      
      return TSDB_CODE_SUCCESS;
    }

    if (dbInfo->vgVersion == dbCache->vgInfo->vgVersion && dbInfo->numOfTable == dbCache->vgInfo->numOfTable) {
      ctgDebug("no new db vgVersion or numOfTable, dbFName:%s, vgVersion:%d, numOfTable:%d", dbFName, dbInfo->vgVersion, dbInfo->numOfTable);
      ctgWReleaseVgInfo(dbCache);
      
      return TSDB_CODE_SUCCESS;
    }

    ctgFreeVgInfo(dbCache->vgInfo);
  }

  dbCache->vgInfo = dbInfo;

  *pDbInfo = NULL;

  ctgDebug("db vgInfo updated, dbFName:%s, vgVersion:%d, dbId:%"PRIx64, dbFName, vgVersion.vgVersion, vgVersion.dbId);

  ctgWReleaseVgInfo(dbCache);

  dbCache = NULL;

  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));
  CTG_ERR_RET(ctgMetaRentUpdate(&pCtg->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion), ctgDbVgVersionSortCompare, ctgDbVgVersionSearchCompare));
  
  CTG_RET(code);
}


int32_t ctgUpdateTblMeta(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, uint64_t dbId, char *tbName, STableMeta *meta, int32_t metaSize) {
  SCtgTbMetaCache *tbCache = &dbCache->tbCache;

  CTG_LOCK(CTG_READ, &tbCache->metaLock);
  if (dbCache->deleted || NULL == tbCache->metaCache || NULL == tbCache->stbCache) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);    
    ctgError("db is dropping, dbId:%"PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  int8_t origType = 0;
  uint64_t origSuid = 0;
  bool isStb = meta->tableType == TSDB_SUPER_TABLE;
  STableMeta *orig = taosHashGet(tbCache->metaCache, tbName, strlen(tbName));
  if (orig) {
    origType = orig->tableType;
    
    if (origType == TSDB_SUPER_TABLE) {
      if ((!isStb) || orig->suid != meta->suid) {
        CTG_LOCK(CTG_WRITE, &tbCache->stbLock);
        if (taosHashRemove(tbCache->stbCache, &orig->suid, sizeof(orig->suid))) {
          ctgError("stb not exist in stbCache, dbFName:%s, stb:%s, suid:%"PRIx64, dbFName, tbName, orig->suid);
        }
        CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);

        ctgDebug("stb removed from stbCache, dbFName:%s, stb:%s, suid:%"PRIx64, dbFName, tbName, orig->suid);
        
        ctgMetaRentRemove(&pCtg->stbRent, orig->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare);
      }

      origSuid = orig->suid;
    }
  }

  if (isStb) {
    CTG_LOCK(CTG_WRITE, &tbCache->stbLock);
  }
  
  if (taosHashPut(tbCache->metaCache, tbName, strlen(tbName), meta, metaSize) != 0) {
    if (isStb) {
      CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);
    }
    
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);  
    ctgError("taosHashPut tbmeta to cache failed, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  ctgDebug("tbmeta updated to cache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
  ctgDbgShowTableMeta(pCtg, tbName, meta);

  if (!isStb) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);  
    return TSDB_CODE_SUCCESS;
  }

  if (origType == TSDB_SUPER_TABLE && origSuid == meta->suid) {
    CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);  
    return TSDB_CODE_SUCCESS;
  }

  STableMeta *tbMeta = taosHashGet(tbCache->metaCache, tbName, strlen(tbName));
  if (taosHashPut(tbCache->stbCache, &meta->suid, sizeof(meta->suid), &tbMeta, POINTER_BYTES) != 0) {
    CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);    
    ctgError("taosHashPut stable to stable cache failed, suid:%"PRIx64, meta->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }
  
  CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);

  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);

  ctgDebug("stb updated to stbCache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);

  SSTableMetaVersion metaRent = {.dbId = dbId, .suid = meta->suid, .sversion = meta->sversion, .tversion = meta->tversion};
  strcpy(metaRent.dbFName, dbFName);
  strcpy(metaRent.stbName, tbName);
  CTG_ERR_RET(ctgMetaRentAdd(&pCtg->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion)));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgCloneVgInfo(SDBVgInfo *src, SDBVgInfo **dst) {
  *dst = malloc(sizeof(SDBVgInfo));
  if (NULL == *dst) {
    qError("malloc %d failed", (int32_t)sizeof(SDBVgInfo));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(*dst, src, sizeof(SDBVgInfo));

  size_t hashSize = taosHashGetSize(src->vgHash);
  (*dst)->vgHash = taosHashInit(hashSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == (*dst)->vgHash) {
    qError("taosHashInit %d failed", (int32_t)hashSize);
    tfree(*dst);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  int32_t *vgId = NULL;
  void *pIter = taosHashIterate(src->vgHash, NULL);
  while (pIter) {
    vgId = taosHashGetKey(pIter, NULL);

    if (taosHashPut((*dst)->vgHash, (void *)vgId, sizeof(int32_t), pIter, sizeof(SVgroupInfo))) {
      qError("taosHashPut failed, hashSize:%d", (int32_t)hashSize);
      taosHashCancelIterate(src->vgHash, pIter);
      taosHashCleanup((*dst)->vgHash);
      tfree(*dst);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(src->vgHash, pIter);
  }


  return TSDB_CODE_SUCCESS;
}



int32_t ctgGetDBVgInfo(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const char* dbFName, bool forceUpdate, SCtgDBCache** dbCache, SDBVgInfo **pInfo) {
  bool inCache = false;
  int32_t code = 0;

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, dbCache, &inCache));

  if (inCache && !forceUpdate) {
    return TSDB_CODE_SUCCESS;
  }

  SUseDbOutput DbOut = {0};
  SBuildUseDBInput input = {0};

  tstrncpy(input.db, dbFName, tListLen(input.db));
  if (inCache) {
    input.dbId = (*dbCache)->dbId;
    input.vgVersion = (*dbCache)->vgInfo->vgVersion;
    input.numOfTable = (*dbCache)->vgInfo->numOfTable;
  } else {
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
  }

  code = ctgGetDBVgInfoFromMnode(pCtg, pRpc, pMgmtEps, &input, &DbOut);
  if (code) {
    if (CTG_DB_NOT_EXIST(code) && input.vgVersion > CTG_DEFAULT_INVALID_VERSION) {
      ctgDebug("db no longer exist, dbFName:%s, dbId:%" PRIx64, input.db, input.dbId);
      ctgPushRmDBMsgInQueue(pCtg, input.db, input.dbId);
    }

    CTG_ERR_RET(code);
  }

  CTG_ERR_JRET(ctgCloneVgInfo(DbOut.dbVgroup, pInfo));
  
  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_VG};
  SCtgUpdateVgMsg *msg = malloc(sizeof(SCtgUpdateVgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateVgMsg));
    ctgFreeVgInfo(DbOut.dbVgroup);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->pCtg = pCtg;
  msg->dbId = DbOut.dbId;
  msg->dbInfo = DbOut.dbVgroup;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  return TSDB_CODE_SUCCESS;

_return:

  tfree(*pInfo);
  tfree(msg);
  
  *pInfo = DbOut.dbVgroup;
  
  CTG_RET(code);
}


int32_t ctgCloneMetaOutput(STableMetaOutput *output, STableMetaOutput **pOutput) {
  *pOutput = malloc(sizeof(STableMetaOutput));
  if (NULL == *pOutput) {
    qError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(*pOutput, output, sizeof(STableMetaOutput));

  if (output->tbMeta) {
    int32_t metaSize = CTG_META_SIZE(output->tbMeta);
    (*pOutput)->tbMeta = malloc(metaSize);
    if (NULL == (*pOutput)->tbMeta) {
      qError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
      tfree(*pOutput);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    memcpy((*pOutput)->tbMeta, output->tbMeta, metaSize);
  }

  return TSDB_CODE_SUCCESS;
}



int32_t ctgRefreshTblMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, int32_t flag, STableMetaOutput **pOutput) {
  if (NULL == pCtg || NULL == pTrans || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SVgroupInfo vgroupInfo = {0};
  int32_t code = 0;

  if (!CTG_FLAG_IS_INF_DB(flag)) {
    CTG_ERR_RET(catalogGetTableHashVgroup(pCtg, pTrans, pMgmtEps, pTableName, &vgroupInfo));
  }

  SCtgUpdateTblMsg *msg = NULL;
  STableMetaOutput  moutput = {0};
  STableMetaOutput *output = calloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (CTG_FLAG_IS_INF_DB(flag)) {
    ctgDebug("will refresh tbmeta, supposed in information_schema, tbName:%s", tNameGetTableName(pTableName));

    CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCtg, pTrans, pMgmtEps, (char *)pTableName->dbname, (char *)pTableName->tname, output));
  } else if (CTG_FLAG_IS_STB(flag)) {
    ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(pTableName));

    // if get from mnode failed, will not try vnode
    CTG_ERR_JRET(ctgGetTableMetaFromMnode(pCtg, pTrans, pMgmtEps, pTableName, output));

    if (CTG_IS_META_NULL(output->metaType)) {
      CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCtg, pTrans, pMgmtEps, pTableName, &vgroupInfo, output));
    }
  } else {
    ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pTableName), flag);

    // if get from vnode failed or no table meta, will not try mnode
    CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCtg, pTrans, pMgmtEps, pTableName, &vgroupInfo, output));

    if (CTG_IS_META_TABLE(output->metaType) && TSDB_SUPER_TABLE == output->tbMeta->tableType) {
      ctgDebug("will continue to refresh tbmeta since got stb, tbName:%s", tNameGetTableName(pTableName));

      tfree(output->tbMeta);
      
      CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCtg, pTrans, pMgmtEps, output->dbFName, output->tbName, output));
    } else if (CTG_IS_META_BOTH(output->metaType)) {
      int32_t exist = 0;
      if (!CTG_FLAG_IS_FORCE_UPDATE(flag)) {
        CTG_ERR_JRET(ctgIsTableMetaExistInCache(pCtg, output->dbFName, output->tbName, &exist));
      }

      if (0 == exist) {
        CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCtg, pTrans, pMgmtEps, output->dbFName, output->tbName, &moutput));

        if (CTG_IS_META_NULL(moutput.metaType)) {
          SET_META_TYPE_NULL(output->metaType);
        }
        
        tfree(output->tbMeta);
        output->tbMeta = moutput.tbMeta;
        moutput.tbMeta = NULL;
      } else {
        tfree(output->tbMeta);
        
        SET_META_TYPE_CTABLE(output->metaType); 
      }
    }
  }

  if (CTG_IS_META_NULL(output->metaType)) {
    ctgError("no tbmeta got, tbNmae:%s", tNameGetTableName(pTableName));
    CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
  }

  if (CTG_IS_META_TABLE(output->metaType)) {
    ctgDebug("tbmeta got, dbFName:%s, tbName:%s, tbType:%d", output->dbFName, output->tbName, output->tbMeta->tableType);
  } else {
    ctgDebug("tbmeta got, dbFName:%s, tbName:%s, tbType:%d, stbMetaGot:%d", output->dbFName, output->ctbName, output->ctbMeta.tableType, CTG_IS_META_BOTH(output->metaType));
  }

  if (pOutput) {
    CTG_ERR_JRET(ctgCloneMetaOutput(output, pOutput));
  }

  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_TBL};
  msg = malloc(sizeof(SCtgUpdateTblMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTblMsg));
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  msg->output = output;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  return TSDB_CODE_SUCCESS;

_return:

  tfree(output->tbMeta);
  tfree(output);
  tfree(msg);
  
  CTG_RET(code);
}

int32_t ctgGetTableMeta(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t flag) {
  if (NULL == pCtg || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pTableMeta) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  int32_t exist = 0;
  int32_t code = 0;
  uint64_t dbId = 0;
  uint64_t suid = 0;
  STableMetaOutput *output = NULL;

  if (CTG_IS_INF_DBNAME(pTableName->dbname)) {
    CTG_FLAG_SET_INF_DB(flag);
  }

  CTG_ERR_RET(ctgGetTableMetaFromCache(pCtg, pTableName, pTableMeta, &exist, flag, &dbId));

  int32_t tbType = 0;

  if (exist) {
    if (CTG_FLAG_MATCH_STB(flag, (*pTableMeta)->tableType) && ((!CTG_FLAG_IS_FORCE_UPDATE(flag)) || (CTG_FLAG_IS_INF_DB(flag)))) {
      goto _return;
    }

    tbType = (*pTableMeta)->tableType;
    suid = (*pTableMeta)->suid;

    tfree(*pTableMeta);
  }

  if (CTG_FLAG_IS_UNKNOWN_STB(flag)) {
    CTG_FLAG_SET_STB(flag, tbType);
  }


  while (true) {
    CTG_ERR_JRET(ctgRefreshTblMeta(pCtg, pRpc, pMgmtEps, pTableName, flag, &output));

    if (CTG_IS_META_TABLE(output->metaType)) {
      *pTableMeta = output->tbMeta;
      goto _return;
    }

    if (CTG_IS_META_BOTH(output->metaType)) {
      memcpy(output->tbMeta, &output->ctbMeta, sizeof(output->ctbMeta));
      
      *pTableMeta = output->tbMeta;
      goto _return;
    }

    if ((!CTG_IS_META_CTABLE(output->metaType)) || output->tbMeta) {
      ctgError("invalid metaType:%d", output->metaType);
      tfree(output->tbMeta);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    // HANDLE ONLY CHILD TABLE META

    SName stbName = *pTableName;
    strcpy(stbName.tname, output->tbName);
    
    CTG_ERR_JRET(ctgGetTableMetaFromCache(pCtg, &stbName, pTableMeta, &exist, flag, NULL));
    if (0 == exist) {
      ctgDebug("stb no longer exist, dbFName:%s, tbName:%s", output->dbFName, pTableName->tname);
      continue;
    }

    memcpy(*pTableMeta, &output->ctbMeta, sizeof(output->ctbMeta));

    break;
  }

_return:

  if (CTG_TABLE_NOT_EXIST(code) && exist) {
    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    if (CTG_FLAG_IS_INF_DB(flag)) {
      strcpy(dbFName, pTableName->dbname);
    } else {
      tNameGetFullDbName(pTableName, dbFName);
    }

    if (TSDB_SUPER_TABLE == tbType) {
      ctgPushRmStbMsgInQueue(pCtg, dbFName, dbId, pTableName->tname, suid);
    } else {
      ctgPushRmTblMsgInQueue(pCtg, dbFName, dbId, pTableName->tname);
    }
  }

  tfree(output);

  if (*pTableMeta) {
    ctgDebug("tbmeta returned, tbName:%s, tbType:%d", pTableName->tname, (*pTableMeta)->tableType);
    ctgDbgShowTableMeta(pCtg, pTableName->tname, *pTableMeta);
  }

  CTG_RET(code);
}



int32_t ctgActUpdateVg(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgUpdateVgMsg *msg = action->data;
  
  CTG_ERR_JRET(ctgUpdateDBVgInfo(msg->pCtg, msg->dbFName, msg->dbId, &msg->dbInfo));

_return:

  ctgFreeVgInfo(msg->dbInfo);
  tfree(msg);
  
  CTG_RET(code);
}

int32_t ctgActRemoveDB(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgRemoveDBMsg *msg = action->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(msg->pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    goto _return;
  }
  
  if (dbCache->dbId != msg->dbId) {
    ctgInfo("dbId already updated, dbFName:%s, dbId:%"PRIx64 ", targetId:%"PRIx64, msg->dbFName, dbCache->dbId, msg->dbId);
    goto _return;
  }
  
  CTG_ERR_JRET(ctgRemoveDB(pCtg, dbCache, msg->dbFName));

_return:

  tfree(msg);
  
  CTG_RET(code);
}


int32_t ctgActUpdateTbl(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgUpdateTblMsg *msg = action->data;
  SCatalog* pCtg = msg->pCtg;
  STableMetaOutput* output = msg->output;
  SCtgDBCache *dbCache = NULL;

  if ((!CTG_IS_META_CTABLE(output->metaType)) && NULL == output->tbMeta) {
    ctgError("no valid tbmeta got from meta rsp, dbFName:%s, tbName:%s", output->dbFName, output->tbName);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (CTG_IS_META_BOTH(output->metaType) && TSDB_SUPER_TABLE != output->tbMeta->tableType) {
    ctgError("table type error, expected:%d, actual:%d", TSDB_SUPER_TABLE, output->tbMeta->tableType);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }    

  char *p = strchr(output->dbFName, '.');
  if (p && CTG_IS_INF_DBNAME(p + 1)) {
    memmove(output->dbFName, p + 1, strlen(p + 1));
  }
  
  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, output->dbFName, output->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:%"PRIx64, output->dbFName, output->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (CTG_IS_META_TABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    int32_t metaSize = CTG_META_SIZE(output->tbMeta);
    
    CTG_ERR_JRET(ctgUpdateTblMeta(pCtg, dbCache, output->dbFName, output->dbId, output->tbName, output->tbMeta, metaSize));
  }

  if (CTG_IS_META_CTABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    CTG_ERR_JRET(ctgUpdateTblMeta(pCtg, dbCache, output->dbFName, output->dbId, output->ctbName, (STableMeta *)&output->ctbMeta, sizeof(output->ctbMeta)));
  }

_return:

  if (output) {
    tfree(output->tbMeta);
    tfree(output);
  }
  
  tfree(msg);
  
  CTG_RET(code);
}


int32_t ctgActRemoveStb(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgRemoveStbMsg *msg = action->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  if (dbCache->dbId != msg->dbId) {
    ctgDebug("dbId already modified, dbFName:%s, current:%"PRIx64", dbId:%"PRIx64", stb:%s, suid:%"PRIx64, msg->dbFName, dbCache->dbId, msg->dbId, msg->stbName, msg->suid);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  if (taosHashRemove(dbCache->tbCache.stbCache, &msg->suid, sizeof(msg->suid))) {
    CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
    ctgDebug("stb not exist in stbCache, may be removed, dbFName:%s, stb:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  if (taosHashRemove(dbCache->tbCache.metaCache, msg->stbName, strlen(msg->stbName))) {  
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
    CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
    ctgError("stb not exist in cache, dbFName:%s, stb:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }  
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
  
  CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  
  ctgInfo("stb removed from cache, dbFName:%s, stbName:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);

  CTG_ERR_JRET(ctgMetaRentRemove(&msg->pCtg->stbRent, msg->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare));
  
  ctgDebug("stb removed from rent, dbFName:%s, stbName:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  
_return:

  tfree(msg);
  
  CTG_RET(code);
}

int32_t ctgActRemoveTbl(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgRemoveTblMsg *msg = action->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  if (dbCache->dbId != msg->dbId) {
    ctgDebug("dbId already modified, dbFName:%s, current:%"PRIx64", dbId:%"PRIx64", tbName:%s", msg->dbFName, dbCache->dbId, msg->dbId, msg->tbName);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  if (taosHashRemove(dbCache->tbCache.metaCache, msg->tbName, strlen(msg->tbName))) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
    ctgError("stb not exist in cache, dbFName:%s, tbName:%s", msg->dbFName, msg->tbName);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  ctgInfo("table removed from cache, dbFName:%s, tbName:%s", msg->dbFName, msg->tbName);

_return:

  tfree(msg);

  CTG_RET(code);
}


void* ctgUpdateThreadFunc(void* param) {
  setThreadName("catalog");

  qInfo("catalog update thread started");

  CTG_LOCK(CTG_READ, &gCtgMgmt.lock);
  
  while (true) {
    tsem_wait(&gCtgMgmt.sem);
    
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
      break;
    }

    SCtgMetaAction *action = NULL;
    ctgPopAction(&action);
    SCatalog *pCtg = ((SCtgUpdateMsgHeader *)action->data)->pCtg;

    ctgDebug("process [%s] action", gCtgAction[action->act].name);
    
    (*gCtgAction[action->act].func)(action);

    CTG_STAT_ADD(gCtgMgmt.stat.runtime.qDoneNum); 

    ctgDbgShowClusterCache(pCtg);
  }

  CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock);

  qInfo("catalog update thread stopped");
  
  return NULL;
}


int32_t ctgStartUpdateThread() {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&gCtgMgmt.updateThread, &thAttr, ctgUpdateThreadFunc, NULL) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    CTG_ERR_RET(terrno);
  }
  
  taosThreadAttrDestroy(&thAttr);
  return TSDB_CODE_SUCCESS;
}


int32_t catalogInit(SCatalogCfg *cfg) {
  if (gCtgMgmt.pCluster) {
    qError("catalog already initialized");
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  atomic_store_8((int8_t*)&gCtgMgmt.exit, false);

  if (cfg) {
    memcpy(&gCtgMgmt.cfg, cfg, sizeof(*cfg));

    if (gCtgMgmt.cfg.maxDBCacheNum == 0) {
      gCtgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    }

    if (gCtgMgmt.cfg.maxTblCacheNum == 0) {
      gCtgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TBLMETA_NUMBER;
    }

    if (gCtgMgmt.cfg.dbRentSec == 0) {
      gCtgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    }

    if (gCtgMgmt.cfg.stbRentSec == 0) {
      gCtgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
    }
  } else {
    gCtgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    gCtgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TBLMETA_NUMBER;
    gCtgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    gCtgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
  }

  gCtgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == gCtgMgmt.pCluster) {
    qError("taosHashInit %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  CTG_ERR_RET(ctgStartUpdateThread());

  tsem_init(&gCtgMgmt.sem, 0, 0);

  gCtgMgmt.head = calloc(1, sizeof(SCtgQNode));
  if (NULL == gCtgMgmt.head) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  gCtgMgmt.tail = gCtgMgmt.head;

  qDebug("catalog initialized, maxDb:%u, maxTbl:%u, dbRentSec:%u, stbRentSec:%u", gCtgMgmt.cfg.maxDBCacheNum, gCtgMgmt.cfg.maxTblCacheNum, gCtgMgmt.cfg.dbRentSec, gCtgMgmt.cfg.stbRentSec);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(uint64_t clusterId, SCatalog** catalogHandle) {
  if (NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == gCtgMgmt.pCluster) {
    qError("catalog cluster cache are not ready, clusterId:%"PRIx64, clusterId);
    CTG_ERR_RET(TSDB_CODE_CTG_NOT_READY);
  }

  int32_t code = 0;
  SCatalog *clusterCtg = NULL;

  while (true) {
    SCatalog **ctg = (SCatalog **)taosHashGet(gCtgMgmt.pCluster, (char*)&clusterId, sizeof(clusterId));

    if (ctg && (*ctg)) {
      *catalogHandle = *ctg;
      qDebug("got catalog handle from cache, clusterId:%"PRIx64", CTG:%p", clusterId, *ctg);
      return TSDB_CODE_SUCCESS;
    }

    clusterCtg = calloc(1, sizeof(SCatalog));
    if (NULL == clusterCtg) {
      qError("calloc %d failed", (int32_t)sizeof(SCatalog));
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    clusterCtg->clusterId = clusterId;

    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->dbRent, gCtgMgmt.cfg.dbRentSec, CTG_RENT_DB));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->stbRent, gCtgMgmt.cfg.stbRentSec, CTG_RENT_STABLE));

    clusterCtg->dbCache = taosHashInit(gCtgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == clusterCtg->dbCache) {
      qError("taosHashInit %d dbCache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    SHashObj *metaCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == metaCache) {
      qError("taosHashInit failed, num:%d", gCtgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    code = taosHashPut(gCtgMgmt.pCluster, &clusterId, sizeof(clusterId), &clusterCtg, POINTER_BYTES);
    if (code) {
      if (HASH_NODE_EXIST(code)) {
        ctgFreeHandle(clusterCtg);
        continue;
      }
      
      qError("taosHashPut CTG to cache failed, clusterId:%"PRIx64, clusterId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("add CTG to cache, clusterId:%"PRIx64", CTG:%p", clusterId, clusterCtg);

    break;
  }

  *catalogHandle = clusterCtg;
  
  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeHandle(clusterCtg);
  
  CTG_RET(code);
}

void catalogFreeHandle(SCatalog* pCtg) {
  if (NULL == pCtg) {
    return;
  }

  if (taosHashRemove(gCtgMgmt.pCluster, &pCtg->clusterId, sizeof(pCtg->clusterId))) {
    ctgWarn("taosHashRemove from cluster failed, may already be freed, clusterId:%"PRIx64, pCtg->clusterId);
    return;
  }

  uint64_t clusterId = pCtg->clusterId;
  
  ctgFreeHandle(pCtg);

  ctgInfo("handle freed, culsterId:%"PRIx64, clusterId);
}

int32_t catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t *tableNum) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == version || NULL == dbId) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("empty db cache, dbFName:%s", dbFName);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  bool inCache = false;
  ctgAcquireVgInfo(pCtg, dbCache, &inCache);
  if (!inCache) {
    ctgReleaseDBCache(pCtg, dbCache);

    *version = CTG_DEFAULT_INVALID_VERSION;
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  *version = dbCache->vgInfo->vgVersion;
  *dbId = dbCache->dbId;
  *tableNum = dbCache->vgInfo->numOfTable;

  ctgReleaseVgInfo(dbCache);
  ctgReleaseDBCache(pCtg, dbCache);

  ctgDebug("Got db vgVersion from cache, dbFName:%s, vgVersion:%d", dbFName, *version);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetDBVgInfo(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const char* dbFName, bool forceUpdate, SArray** vgroupList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == pRpc || NULL == pMgmtEps || NULL == vgroupList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t code = 0;
  SArray *vgList = NULL;
  SHashObj *vgHash = NULL;
  SDBVgInfo *vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pRpc, pMgmtEps, dbFName, forceUpdate, &dbCache, &vgInfo));
  if (dbCache) {
    vgHash = dbCache->vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, &vgList));

  *vgroupList = vgList;
  vgList = NULL;

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    tfree(vgInfo);
  }

  CTG_API_LEAVE(code);  
}


int32_t catalogUpdateDBVgInfo(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDBVgInfo* dbInfo) {
  CTG_API_ENTER();

  int32_t code = 0;
  
  if (NULL == pCtg || NULL == dbFName || NULL == dbInfo) {
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_VG};
  SCtgUpdateVgMsg *msg = malloc(sizeof(SCtgUpdateVgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateVgMsg));
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->dbId = dbId;
  msg->dbInfo = dbInfo;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  dbInfo = NULL;

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  CTG_API_LEAVE(code);
  
_return:

  ctgFreeVgInfo(dbInfo);

  tfree(msg);
  
  CTG_API_LEAVE(code);
}

int32_t catalogUpdateVgEpSet(SCatalog* pCtg, const char* dbFName, int32_t vgId, SEpSet *epSet) {

}

int32_t catalogRemoveDB(SCatalog* pCtg, const char* dbFName, uint64_t dbId) {
  CTG_API_ENTER();

  int32_t code = 0;
  
  if (NULL == pCtg || NULL == dbFName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgPushRmDBMsgInQueue(pCtg, dbFName, dbId));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogRemoveStbMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* stbName, uint64_t suid) {
  CTG_API_ENTER();

  int32_t code = 0;
  
  if (NULL == pCtg || NULL == dbFName || NULL == stbName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgPushRmStbMsgInQueue(pCtg, dbFName, dbId, stbName, suid));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetIndexMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, const char *pIndexName, SIndexMeta** pIndexMeta) {

}

int32_t catalogGetTableMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCtg, pTrans, pMgmtEps, pTableName, pTableMeta, CTG_FLAG_UNKNOWN_STB));
}

int32_t catalogGetSTableMeta(SCatalog* pCtg, void * pTrans, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCtg, pTrans, pMgmtEps, pTableName, pTableMeta, CTG_FLAG_STB));
}

int32_t catalogUpdateSTableMeta(SCatalog* pCtg, STableMetaRsp *rspMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == rspMsg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  STableMetaOutput *output = calloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_API_LEAVE(TSDB_CODE_CTG_MEM_ERROR);
  }
  
  int32_t code = 0;

  strcpy(output->dbFName, rspMsg->dbFName);
  strcpy(output->tbName, rspMsg->tbName);

  output->dbId = rspMsg->dbId;
  
  SET_META_TYPE_TABLE(output->metaType);
  
  CTG_ERR_JRET(queryCreateTableMetaFromMsg(rspMsg, true, &output->tbMeta));

  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_TBL};
  SCtgUpdateTblMsg *msg = malloc(sizeof(SCtgUpdateTblMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTblMsg));
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  msg->output = output;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(&action));

  ctgDebug("action [%s] added into queue", gCtgAction[action.act].name);

  CTG_API_LEAVE(code);
  
_return:

  tfree(output->tbMeta);
  tfree(output);
  tfree(msg);
  
  CTG_API_LEAVE(code);
}


int32_t catalogRefreshTableMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pTrans || NULL == pMgmtEps || NULL == pTableName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgRefreshTblMeta(pCtg, pTrans, pMgmtEps, pTableName, CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable), NULL));
}

int32_t catalogRefreshGetTableMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCtg, pTrans, pMgmtEps, pTableName, pTableMeta, CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable)));
}

int32_t catalogGetTableDistVgInfo(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, SArray** pVgList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pVgList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (CTG_IS_INF_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  STableMeta *tbMeta = NULL;
  int32_t code = 0;
  SVgroupInfo vgroupInfo = {0};
  SCtgDBCache* dbCache = NULL;
  SArray *vgList = NULL;
  SDBVgInfo *vgInfo = NULL;

  *pVgList = NULL;
  
  CTG_ERR_JRET(ctgGetTableMeta(pCtg, pRpc, pMgmtEps, pTableName, &tbMeta, CTG_FLAG_UNKNOWN_STB));

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  SHashObj *vgHash = NULL;  
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pRpc, pMgmtEps, db, false, &dbCache, &vgInfo));

  if (dbCache) {
    vgHash = dbCache->vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  /* TODO REMOEV THIS ....
  if (0 == tbMeta->vgId) {
    SVgroupInfo vgroup = {0};
    
    catalogGetTableHashVgroup(pCtg, pRpc, pMgmtEps, pTableName, &vgroup);

    tbMeta->vgId = vgroup.vgId;
  }
  // TODO REMOVE THIS ....*/

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, pVgList));
  } else {
    int32_t vgId = tbMeta->vgId;
    if (taosHashGetDup(vgHash, &vgId, sizeof(vgId), &vgroupInfo) != 0) {
      ctgError("table's vgId not found in vgroup list, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
    }

    vgList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == vgList) {
      ctgError("taosArrayInit %d failed", (int32_t)sizeof(SVgroupInfo));
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
    }

    if (NULL == taosArrayPush(vgList, &vgroupInfo)) {
      ctgError("taosArrayPush vgroupInfo to array failed, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    *pVgList = vgList;
    vgList = NULL;
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  tfree(tbMeta);

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    tfree(vgInfo);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }
  
  CTG_API_LEAVE(code);
}


int32_t catalogGetTableHashVgroup(SCatalog *pCtg, void *pTrans, const SEpSet *pMgmtEps, const SName *pTableName, SVgroupInfo *pVgroup) {
  CTG_API_ENTER();

  if (CTG_IS_INF_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t code = 0;
  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  SDBVgInfo *vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pTrans, pMgmtEps, db, false, &dbCache, &vgInfo));

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, vgInfo ? vgInfo : dbCache->vgInfo, pTableName, pVgroup));

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    tfree(vgInfo);
  }

  CTG_API_LEAVE(code);
}


int32_t catalogGetAllMeta(SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pTrans || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  pRsp->pTableMeta = NULL;

  if (pReq->pTableName) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableName);
    if (tbNum <= 0) {
      ctgError("empty table name list, tbNum:%d", tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
    if (NULL == pRsp->pTableMeta) {
      ctgError("taosArrayInit %d failed", tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    for (int32_t i = 0; i < tbNum; ++i) {
      SName *name = taosArrayGet(pReq->pTableName, i);
      STableMeta *pTableMeta = NULL;
      
      CTG_ERR_JRET(ctgGetTableMeta(pCtg, pTrans, pMgmtEps, name, &pTableMeta, CTG_FLAG_UNKNOWN_STB));

      if (NULL == taosArrayPush(pRsp->pTableMeta, &pTableMeta)) {
        ctgError("taosArrayPush failed, idx:%d", i);
        tfree(pTableMeta);
        CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
      }
    }
  }

  if (pReq->qNodeRequired) {
    CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pTrans, pMgmtEps, &pRsp->pEpSetList));
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:  

  if (pRsp->pTableMeta) {
    int32_t aSize = taosArrayGetSize(pRsp->pTableMeta);
    for (int32_t i = 0; i < aSize; ++i) {
      STableMeta *pMeta = taosArrayGetP(pRsp->pTableMeta, i);
      tfree(pMeta);
    }
    
    taosArrayDestroy(pRsp->pTableMeta);
    pRsp->pTableMeta = NULL;
  }
  
  CTG_API_LEAVE(code);
}

int32_t catalogGetQnodeList(SCatalog* pCtg, void *pRpc, const SEpSet* pMgmtEps, SArray* pQnodeList) {
  CTG_API_ENTER();
  
  int32_t code = 0;
  if (NULL == pCtg || NULL == pRpc  || NULL == pMgmtEps || NULL == pQnodeList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pRpc, pMgmtEps, &pQnodeList));

_return:

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetExpiredSTables(SCatalog* pCtg, SSTableMetaVersion **stables, uint32_t *num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == stables || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->stbRent, (void **)stables, num, sizeof(SSTableMetaVersion)));
}

int32_t catalogGetExpiredDBs(SCatalog* pCtg, SDbVgVersion **dbs, uint32_t *num) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == dbs || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->dbRent, (void **)dbs, num, sizeof(SDbVgVersion)));
}


void catalogDestroy(void) {
  qInfo("start to destroy catalog");
  
  if (NULL == gCtgMgmt.pCluster || atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
    return;
  }

  atomic_store_8((int8_t*)&gCtgMgmt.exit, true);

  tsem_post(&gCtgMgmt.sem);

  while (CTG_IS_LOCKED(&gCtgMgmt.lock)) {
    taosUsleep(1);
  }
  
  CTG_LOCK(CTG_WRITE, &gCtgMgmt.lock);

  SCatalog *pCtg = NULL;
  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      catalogFreeHandle(pCtg);
    }
    
    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }
  
  taosHashCleanup(gCtgMgmt.pCluster);
  gCtgMgmt.pCluster = NULL;

  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.lock);

  qInfo("catalog destroyed");
}



