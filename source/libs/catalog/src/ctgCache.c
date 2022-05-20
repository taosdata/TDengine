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
#include "systable.h"

SCtgAction gCtgAction[CTG_ACT_MAX] = {
  {
    CTG_ACT_UPDATE_VG,
    "update vgInfo",
    ctgActUpdateVg
  },
  {
    CTG_ACT_UPDATE_TBL,
    "update tbMeta",
    ctgActUpdateTb
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
    ctgActRemoveTb
  },
  {
    CTG_ACT_UPDATE_USER,
    "update user",
    ctgActUpdateUser
  }
};




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
  char *p = strchr(dbFName, '.');
  if (p && CTG_IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

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


int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache) {
  SCtgDBCache *dbCache = NULL;

  if (NULL == pCtg->dbCache) {
    ctgDebug("empty db cache, dbFName:%s", dbFName);
    goto _return;
  }

  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {  
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }

  bool inCache = false;
  ctgAcquireVgInfo(pCtg, dbCache, &inCache);
  if (!inCache) {
    ctgDebug("vgInfo of db %s not in cache", dbFName);
    goto _return;
  }

  *pCache = dbCache;

  CTG_CACHE_STAT_ADD(vgHitNum, 1);

  ctgDebug("Got db vgInfo from cache, dbFName:%s", dbFName);
  
  return TSDB_CODE_SUCCESS;

_return:

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }

  *pCache = NULL;

  CTG_CACHE_STAT_ADD(vgMissNum, 1);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgTbMetaExistInCache(SCatalog* pCtg, char *dbFName, char* tbName, int32_t *exist) {
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


int32_t ctgReadTbMetaFromCache(SCatalog* pCtg, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  
  *pTableMeta = NULL;

  if (NULL == pCtg->dbCache) {
    ctgDebug("empty tbmeta cache, tbName:%s", ctx->pName->tname);
    return TSDB_CODE_SUCCESS;
  }

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    strcpy(dbFName, ctx->pName->dbname);
  } else {
    tNameGetFullDbName(ctx->pName, dbFName);
  }

  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", ctx->pName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t sz = 0;
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  taosHashGetDup_m(dbCache->tbCache.metaCache, ctx->pName->tname, strlen(ctx->pName->tname), (void **)pTableMeta, &sz);
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  if (NULL == *pTableMeta) {
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("tbl not in cache, dbFName:%s, tbName:%s", dbFName, ctx->pName->tname);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta* tbMeta = *pTableMeta;
  ctx->tbInfo.inCache = true;
  ctx->tbInfo.dbId = dbCache->dbId;
  ctx->tbInfo.suid = tbMeta->suid;
  ctx->tbInfo.tbType = tbMeta->tableType;
  
  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("Got meta from cache, type:%d, dbFName:%s, tbName:%s", tbMeta->tableType, dbFName, ctx->pName->tname);

    CTG_CACHE_STAT_ADD(tblHitNum, 1);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &dbCache->tbCache.stbLock);
  
  STableMeta **stbMeta = taosHashGet(dbCache->tbCache.stbCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgError("stb not in stbCache, suid:%"PRIx64, tbMeta->suid);
    goto _return;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgError("stable suid in stbCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = CTG_META_SIZE(*stbMeta);
  *pTableMeta = taosMemoryRealloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgError("realloc size[%d] failed", metaSize);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);

  ctgReleaseDBCache(pCtg, dbCache);

  CTG_CACHE_STAT_ADD(tblHitNum, 1);

  ctgDebug("Got tbmeta from cache, dbFName:%s, tbName:%s", dbFName, ctx->pName->tname);
  
  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseDBCache(pCtg, dbCache);
  taosMemoryFreeClear(*pTableMeta);

  CTG_CACHE_STAT_ADD(tblMissNum, 1);
  
  CTG_RET(code);
}

int32_t ctgReadTbSverFromCache(SCatalog* pCtg, const SName* pTableName, int32_t* sver) {
  *sver = -1;

  if (NULL == pCtg->dbCache) {
    ctgDebug("empty tbmeta cache, tbName:%s", pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *dbCache = NULL;
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, dbFName);

  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  int32_t tbType = 0;
  uint64_t suid = 0;
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  STableMeta* tbMeta = taosHashGet(dbCache->tbCache.metaCache, pTableName->tname, strlen(pTableName->tname));
  if (tbMeta) {
    tbType = tbMeta->tableType;
    suid = tbMeta->suid;
    if (tbType != TSDB_CHILD_TABLE) {
      *sver = tbMeta->sversion;
    }
  }
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  if (NULL == tbMeta) {
    ctgReleaseDBCache(pCtg, dbCache);
    return TSDB_CODE_SUCCESS;
  }

  if (tbType != TSDB_CHILD_TABLE) {
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("Got sver %d from cache, type:%d, dbFName:%s, tbName:%s", *sver, tbType, dbFName, pTableName->tname);

    return TSDB_CODE_SUCCESS;
  }

  ctgDebug("Got subtable meta from cache, dbFName:%s, tbName:%s, suid:%" PRIx64, dbFName, pTableName->tname, suid);

  CTG_LOCK(CTG_READ, &dbCache->tbCache.stbLock);

  STableMeta **stbMeta = taosHashGet(dbCache->tbCache.stbCache, &suid, sizeof(suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgReleaseDBCache(pCtg, dbCache);
    ctgDebug("stb not in stbCache, suid:%"PRIx64, suid);
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != suid) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgReleaseDBCache(pCtg, dbCache);
    ctgError("stable suid in stbCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *sver = (*stbMeta)->sversion;

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);

  ctgReleaseDBCache(pCtg, dbCache);

  ctgDebug("Got sver %d from cache, type:%d, dbFName:%s, tbName:%s", *sver, tbType, dbFName, pTableName->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbTypeFromCache(SCatalog* pCtg, const char* dbFName, const char *tableName, int32_t *tbType) {
  if (NULL == pCtg->dbCache) {
    ctgWarn("empty db cache, dbFName:%s, tbName:%s", dbFName, tableName);  
    return TSDB_CODE_SUCCESS;
  }
  
  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  STableMeta *pTableMeta = (STableMeta *)taosHashAcquire(dbCache->tbCache.metaCache, tableName, strlen(tableName));

  if (NULL == pTableMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
    ctgWarn("tbl not in cache, dbFName:%s, tbName:%s", dbFName, tableName);  
    ctgReleaseDBCache(pCtg, dbCache);
    
    return TSDB_CODE_SUCCESS;
  }

  *tbType = atomic_load_8(&pTableMeta->tableType);

  taosHashRelease(dbCache->tbCache.metaCache, pTableMeta);

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  ctgReleaseDBCache(pCtg, dbCache);

  ctgDebug("Got tbtype from cache, dbFName:%s, tbName:%s, type:%d", dbFName, tableName, *tbType);  
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgChkAuthFromCache(SCatalog* pCtg, const char* user, const char* dbFName, AUTH_TYPE type, bool *inCache, bool *pass) {
  if (NULL == pCtg->userCache) {
    ctgDebug("empty user auth cache, user:%s", user);
    goto _return;
  }
  
  SCtgUserAuth *pUser = (SCtgUserAuth *)taosHashGet(pCtg->userCache, user, strlen(user));
  if (NULL == pUser) {
    ctgDebug("user not in cache, user:%s", user);
    goto _return;
  }

  *inCache = true;

  ctgDebug("Got user from cache, user:%s", user);
  CTG_CACHE_STAT_ADD(userHitNum, 1);
  
  if (pUser->superUser) {
    *pass = true;
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &pUser->lock);
  if (pUser->createdDbs && taosHashGet(pUser->createdDbs, dbFName, strlen(dbFName))) {
    *pass = true;
    CTG_UNLOCK(CTG_READ, &pUser->lock);
    return TSDB_CODE_SUCCESS;
  }
  
  if (pUser->readDbs && taosHashGet(pUser->readDbs, dbFName, strlen(dbFName)) && type == AUTH_TYPE_READ) {
    *pass = true;
  }
  
  if (pUser->writeDbs && taosHashGet(pUser->writeDbs, dbFName, strlen(dbFName)) && type == AUTH_TYPE_WRITE) {
    *pass = true;
  }

  CTG_UNLOCK(CTG_READ, &pUser->lock);
  
  return TSDB_CODE_SUCCESS;

_return:

  *inCache = false;
  CTG_CACHE_STAT_ADD(userMissNum, 1);
  
  return TSDB_CODE_SUCCESS;
}


void ctgWaitAction(SCtgMetaAction *action) {
  while (true) {
    tsem_wait(&gCtgMgmt.queue.rspSem);
    
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
      tsem_post(&gCtgMgmt.queue.rspSem);
      break;
    }

    if (gCtgMgmt.queue.seqDone >= action->seqId) {
      break;
    }

    tsem_post(&gCtgMgmt.queue.rspSem);
    sched_yield();
  }
}

void ctgPopAction(SCtgMetaAction **action) {
  SCtgQNode *orig = gCtgMgmt.queue.head;
  
  SCtgQNode *node = gCtgMgmt.queue.head->next;
  gCtgMgmt.queue.head = gCtgMgmt.queue.head->next;

  CTG_QUEUE_SUB();
  
  taosMemoryFreeClear(orig);

  *action = &node->action;
}


int32_t ctgPushAction(SCatalog* pCtg, SCtgMetaAction *action) {
  SCtgQNode *node = taosMemoryCalloc(1, sizeof(SCtgQNode));
  if (NULL == node) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  action->seqId = atomic_add_fetch_64(&gCtgMgmt.queue.seqId, 1);
  
  node->action = *action;

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);
  gCtgMgmt.queue.tail->next = node;
  gCtgMgmt.queue.tail = node;
  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);

  CTG_QUEUE_ADD();
  CTG_RUNTIME_STAT_ADD(qNum, 1);

  tsem_post(&gCtgMgmt.queue.reqSem);

  ctgDebug("action [%s] added into queue", gCtgAction[action->act].name);

  if (action->syncReq) {
    ctgWaitAction(action);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgPutRmDBToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_DB};
  SCtgRemoveDBMsg *msg = taosMemoryMalloc(sizeof(SCtgRemoveDBMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgRemoveDBMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  char *p = strchr(dbFName, '.');
  if (p && CTG_IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->dbId = dbId;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(action.data);
  CTG_RET(code);
}


int32_t ctgPutRmStbToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *stbName, uint64_t suid, bool syncReq) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_STB, .syncReq = syncReq};
  SCtgRemoveStbMsg *msg = taosMemoryMalloc(sizeof(SCtgRemoveStbMsg));
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

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(action.data);
  CTG_RET(code);
}



int32_t ctgPutRmTbToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *tbName, bool syncReq) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_REMOVE_TBL, .syncReq = syncReq};
  SCtgRemoveTblMsg *msg = taosMemoryMalloc(sizeof(SCtgRemoveTblMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgRemoveTblMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  strncpy(msg->tbName, tbName, sizeof(msg->tbName));
  msg->dbId = dbId;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(action.data);
  CTG_RET(code);
}

int32_t ctgPutUpdateVgToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, SDBVgInfo* dbInfo, bool syncReq) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_VG, .syncReq = syncReq};
  SCtgUpdateVgMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateVgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateVgMsg));
    ctgFreeVgInfo(dbInfo);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  char *p = strchr(dbFName, '.');
  if (p && CTG_IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->pCtg = pCtg;
  msg->dbId = dbId;
  msg->dbInfo = dbInfo;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeVgInfo(dbInfo);
  taosMemoryFreeClear(action.data);
  CTG_RET(code);
}

int32_t ctgPutUpdateTbToQueue(SCatalog* pCtg, STableMetaOutput *output, bool syncReq) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_TBL, .syncReq = syncReq};
  SCtgUpdateTblMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTblMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTblMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  char *p = strchr(output->dbFName, '.');
  if (p && CTG_IS_SYS_DBNAME(p + 1)) {
    memmove(output->dbFName, p + 1, strlen(p + 1));
  }

  msg->pCtg = pCtg;
  msg->output = output;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;
  
_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgPutUpdateUserToQueue(SCatalog* pCtg, SGetUserAuthRsp *pAuth, bool syncReq) {
  int32_t code = 0;
  SCtgMetaAction action= {.act = CTG_ACT_UPDATE_USER, .syncReq = syncReq};
  SCtgUpdateUserMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateUserMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateUserMsg));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  msg->pCtg = pCtg;
  msg->userAuth = *pAuth;

  action.data = msg;

  CTG_ERR_JRET(ctgPushAction(pCtg, &action));

  return TSDB_CODE_SUCCESS;
  
_return:

  tFreeSGetUserAuthRsp(pAuth);
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;

  size_t msgSize = sizeof(SCtgRentSlot) * mgmt->slotNum;
  
  mgmt->slots = taosMemoryCalloc(1, msgSize);
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
  *res = taosMemoryMalloc(msize);
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

  CTG_CACHE_STAT_ADD(dbNum, 1);
 
  SDbVgVersion vgVersion = {.dbId = newDBCache.dbId, .vgVersion = -1};
  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));

  ctgDebug("db added to cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbId);

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


int32_t ctgRemoveDBFromCache(SCatalog* pCtg, SCtgDBCache *dbCache, const char* dbFName) {
  uint64_t dbId = dbCache->dbId;
  
  ctgInfo("start to remove db from cache, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);

  atomic_store_8(&dbCache->deleted, 1);

  ctgRemoveStbRent(pCtg, &dbCache->tbCache);

  ctgFreeDbCache(dbCache);

  CTG_ERR_RET(ctgMetaRentRemove(&pCtg->dbRent, dbCache->dbId, ctgDbVgVersionSortCompare, ctgDbVgVersionSearchCompare));
  
  ctgDebug("db removed from rent, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);

  if (taosHashRemove(pCtg->dbCache, dbFName, strlen(dbFName))) {
    ctgInfo("taosHashRemove from dbCache failed, may be removed, dbFName:%s", dbFName);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  CTG_CACHE_STAT_SUB(dbNum, 1);

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
    CTG_ERR_RET(ctgRemoveDBFromCache(pCtg, dbCache, dbFName));
  }
  
  CTG_ERR_RET(ctgAddNewDBCache(pCtg, dbFName, dbId));

  ctgGetDBCache(pCtg, dbFName, &dbCache);

  *pCache = dbCache;

  return TSDB_CODE_SUCCESS;
}


int32_t ctgWriteDBVgInfoToCache(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDBVgInfo** pDbInfo) {
  int32_t code = 0;
  SDBVgInfo* dbInfo = *pDbInfo;

  if (NULL == dbInfo->vgHash) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgHash) <= 0) {
    ctgError("invalid db vgInfo, dbFName:%s, vgHash:%p, vgVersion:%d, vgHashSize:%d", 
      dbFName, dbInfo->vgHash, dbInfo->vgVersion, taosHashGetSize(dbInfo->vgHash));
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


int32_t ctgWriteTbMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, uint64_t dbId, char *tbName, STableMeta *meta, int32_t metaSize) {
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

    if (origType == meta->tableType && orig->uid == meta->uid && orig->sversion >= meta->sversion && orig->tversion >= meta->tversion) {
      CTG_UNLOCK(CTG_READ, &tbCache->metaLock);  
      return TSDB_CODE_SUCCESS;
    }
    
    if (origType == TSDB_SUPER_TABLE) {
      CTG_LOCK(CTG_WRITE, &tbCache->stbLock);
      if (taosHashRemove(tbCache->stbCache, &orig->suid, sizeof(orig->suid))) {
        ctgError("stb not exist in stbCache, dbFName:%s, stb:%s, suid:%"PRIx64, dbFName, tbName, orig->suid);
      } else {
        CTG_CACHE_STAT_SUB(stblNum, 1);
      }
      CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);

      ctgDebug("stb removed from stbCache, dbFName:%s, stb:%s, suid:%"PRIx64, dbFName, tbName, orig->suid);
      
      ctgMetaRentRemove(&pCtg->stbRent, orig->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare);

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

  if (NULL == orig) {
    CTG_CACHE_STAT_ADD(tblNum, 1);
  }

  ctgDebug("tbmeta updated to cache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
  ctgdShowTableMeta(pCtg, tbName, meta);

  if (!isStb) {
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

  CTG_CACHE_STAT_ADD(stblNum, 1);
  
  CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);

  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);

  ctgDebug("stb updated to stbCache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);

  SSTableMetaVersion metaRent = {.dbId = dbId, .suid = meta->suid, .sversion = meta->sversion, .tversion = meta->tversion};
  strcpy(metaRent.dbFName, dbFName);
  strcpy(metaRent.stbName, tbName);
  CTG_ERR_RET(ctgMetaRentAdd(&pCtg->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion)));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgUpdateTbMetaToCache(SCatalog* pCtg, STableMetaOutput* pOut, bool syncReq) {
  STableMetaOutput* pOutput = NULL;
  int32_t code = 0;
  
  CTG_ERR_RET(ctgCloneMetaOutput(pOut, &pOutput));
  CTG_ERR_JRET(ctgPutUpdateTbToQueue(pCtg, pOutput, syncReq));

  return TSDB_CODE_SUCCESS;
  
_return:

  ctgFreeSTableMetaOutput(pOutput);
  CTG_RET(code);
}


int32_t ctgActUpdateVg(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgUpdateVgMsg *msg = action->data;
  
  CTG_ERR_JRET(ctgWriteDBVgInfoToCache(msg->pCtg, msg->dbFName, msg->dbId, &msg->dbInfo));

_return:

  ctgFreeVgInfo(msg->dbInfo);
  taosMemoryFreeClear(msg);
  
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
  
  CTG_ERR_JRET(ctgRemoveDBFromCache(pCtg, dbCache, msg->dbFName));

_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


int32_t ctgActUpdateTb(SCtgMetaAction *action) {
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
  
  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, output->dbFName, output->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:%"PRIx64, output->dbFName, output->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (CTG_IS_META_TABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    int32_t metaSize = CTG_META_SIZE(output->tbMeta);
    
    CTG_ERR_JRET(ctgWriteTbMetaToCache(pCtg, dbCache, output->dbFName, output->dbId, output->tbName, output->tbMeta, metaSize));
  }

  if (CTG_IS_META_CTABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    CTG_ERR_JRET(ctgWriteTbMetaToCache(pCtg, dbCache, output->dbFName, output->dbId, output->ctbName, (STableMeta *)&output->ctbMeta, sizeof(output->ctbMeta)));
  }

_return:

  if (output) {
    taosMemoryFreeClear(output->tbMeta);
    taosMemoryFreeClear(output);
  }
  
  taosMemoryFreeClear(msg);
  
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

  if (msg->dbId && (dbCache->dbId != msg->dbId)) {
    ctgDebug("dbId already modified, dbFName:%s, current:%"PRIx64", dbId:%"PRIx64", stb:%s, suid:%"PRIx64, msg->dbFName, dbCache->dbId, msg->dbId, msg->stbName, msg->suid);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  if (taosHashRemove(dbCache->tbCache.stbCache, &msg->suid, sizeof(msg->suid))) {
    ctgDebug("stb not exist in stbCache, may be removed, dbFName:%s, stb:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  } else {
    CTG_CACHE_STAT_SUB(stblNum, 1);
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  if (taosHashRemove(dbCache->tbCache.metaCache, msg->stbName, strlen(msg->stbName))) {  
    ctgError("stb not exist in cache, dbFName:%s, stb:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  } else {
    CTG_CACHE_STAT_SUB(tblNum, 1);
  }
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
  
  CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  
  ctgInfo("stb removed from cache, dbFName:%s, stbName:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);

  CTG_ERR_JRET(ctgMetaRentRemove(&msg->pCtg->stbRent, msg->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare));
  
  ctgDebug("stb removed from rent, dbFName:%s, stbName:%s, suid:%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  
_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgActRemoveTb(SCtgMetaAction *action) {
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
  } else {
    CTG_CACHE_STAT_SUB(tblNum, 1);
  }
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  ctgInfo("table removed from cache, dbFName:%s, tbName:%s", msg->dbFName, msg->tbName);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgActUpdateUser(SCtgMetaAction *action) {
  int32_t code = 0;
  SCtgUpdateUserMsg *msg = action->data;
  SCatalog* pCtg = msg->pCtg;

  if (NULL == pCtg->userCache) {
    pCtg->userCache = taosHashInit(gCtgMgmt.cfg.maxUserCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == pCtg->userCache) {
      ctgError("taosHashInit %d user cache failed", gCtgMgmt.cfg.maxUserCacheNum);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  
  SCtgUserAuth *pUser = (SCtgUserAuth *)taosHashGet(pCtg->userCache, msg->userAuth.user, strlen(msg->userAuth.user));
  if (NULL == pUser) {
    SCtgUserAuth userAuth = {0};

    userAuth.version = msg->userAuth.version;
    userAuth.superUser = msg->userAuth.superAuth;
    userAuth.createdDbs = msg->userAuth.createdDbs;
    userAuth.readDbs = msg->userAuth.readDbs;
    userAuth.writeDbs = msg->userAuth.writeDbs;

    if (taosHashPut(pCtg->userCache, msg->userAuth.user, strlen(msg->userAuth.user), &userAuth, sizeof(userAuth))) {
      ctgError("taosHashPut user %s to cache failed", msg->userAuth.user);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    taosMemoryFreeClear(msg);

    return TSDB_CODE_SUCCESS;
  }

  pUser->version = msg->userAuth.version;

  CTG_LOCK(CTG_WRITE, &pUser->lock);

  taosHashCleanup(pUser->createdDbs);
  pUser->createdDbs = msg->userAuth.createdDbs;
  msg->userAuth.createdDbs = NULL;

  taosHashCleanup(pUser->readDbs);
  pUser->readDbs = msg->userAuth.readDbs;
  msg->userAuth.readDbs = NULL;

  taosHashCleanup(pUser->writeDbs);
  pUser->writeDbs = msg->userAuth.writeDbs;
  msg->userAuth.writeDbs = NULL;

  CTG_UNLOCK(CTG_WRITE, &pUser->lock);

_return:


  taosHashCleanup(msg->userAuth.createdDbs);
  taosHashCleanup(msg->userAuth.readDbs);
  taosHashCleanup(msg->userAuth.writeDbs);
 
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


void* ctgUpdateThreadFunc(void* param) {
  setThreadName("catalog");

  qInfo("catalog update thread started");

  CTG_LOCK(CTG_READ, &gCtgMgmt.lock);
  
  while (true) {
    if (tsem_wait(&gCtgMgmt.queue.reqSem)) {
      qError("ctg tsem_wait failed, error:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    }
    
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
      tsem_post(&gCtgMgmt.queue.rspSem);
      break;
    }

    SCtgMetaAction *action = NULL;
    ctgPopAction(&action);
    SCatalog *pCtg = ((SCtgUpdateMsgHeader *)action->data)->pCtg;

    ctgDebug("process [%s] action", gCtgAction[action->act].name);
    
    (*gCtgAction[action->act].func)(action);

    gCtgMgmt.queue.seqDone = action->seqId;

    if (action->syncReq) {
      tsem_post(&gCtgMgmt.queue.rspSem);
    }

    CTG_RUNTIME_STAT_ADD(qDoneNum, 1); 

    ctgdShowClusterCache(pCtg);
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



