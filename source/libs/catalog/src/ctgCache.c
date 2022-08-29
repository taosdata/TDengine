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

SCtgOperation gCtgCacheOperation[CTG_OP_MAX] = {
  {
    CTG_OP_UPDATE_VGROUP,
    "update vgInfo",
    ctgOpUpdateVgroup
  },
  {
    CTG_OP_UPDATE_TB_META,
    "update tbMeta",
    ctgOpUpdateTbMeta
  },
  {
    CTG_OP_DROP_DB_CACHE,
    "drop DB",
    ctgOpDropDbCache
  },
  {
    CTG_OP_DROP_DB_VGROUP,
    "drop DBVgroup",
    ctgOpDropDbVgroup
  },
  {
    CTG_OP_DROP_STB_META,
    "drop stbMeta",
    ctgOpDropStbMeta
  },
  {
    CTG_OP_DROP_TB_META,
    "drop tbMeta",
    ctgOpDropTbMeta
  },
  {
    CTG_OP_UPDATE_USER,
    "update user",
    ctgOpUpdateUser
  },
  {
    CTG_OP_UPDATE_VG_EPSET,
    "update epset",
    ctgOpUpdateEpset
  },
  {
    CTG_OP_UPDATE_TB_INDEX,
    "update tbIndex",
    ctgOpUpdateTbIndex
  },
  {
    CTG_OP_DROP_TB_INDEX,
    "drop tbIndex",
    ctgOpDropTbIndex
  },
  {
    CTG_OP_CLEAR_CACHE,
    "clear cache",
    ctgOpClearCache
  }  
};




int32_t ctgRLockVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache, bool *inCache) {
  CTG_LOCK(CTG_READ, &dbCache->vgCache.vgLock);
  
  if (dbCache->deleted) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock);

    ctgDebug("db is dropping, dbId:0x%"PRIx64, dbCache->dbId);
    
    *inCache = false;
    return TSDB_CODE_SUCCESS;
  }

  
  if (NULL == dbCache->vgCache.vgInfo) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock);

    *inCache = false;
    ctgDebug("db vgInfo is empty, dbId:0x%"PRIx64, dbCache->dbId);
    return TSDB_CODE_SUCCESS;
  }

  *inCache = true;
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgWLockVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache) {
  CTG_LOCK(CTG_WRITE, &dbCache->vgCache.vgLock);

  if (dbCache->deleted) {
    ctgDebug("db is dropping, dbId:0x%"PRIx64, dbCache->dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgCache.vgLock);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  return TSDB_CODE_SUCCESS;
}

void ctgRUnlockVgInfo(SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock);
}

void ctgWUnlockVgInfo(SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgCache.vgLock);
}

void ctgReleaseDBCache(SCatalog *pCtg, SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_READ, &dbCache->dbLock);
}

int32_t ctgAcquireDBCacheImpl(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache, bool acquire) {
  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  SCtgDBCache *dbCache = (SCtgDBCache *)taosHashGet(pCtg->dbCache, dbFName, strlen(dbFName));
  if (NULL == dbCache) {
    *pCache = NULL;
    ctgDebug("db not in cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  if (acquire) {
    CTG_LOCK(CTG_READ, &dbCache->dbLock);
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

void ctgReleaseVgInfoToCache(SCatalog* pCtg, SCtgDBCache *dbCache) {
  ctgRUnlockVgInfo(dbCache);
  ctgReleaseDBCache(pCtg, dbCache);
}

void ctgReleaseTbMetaToCache(SCatalog* pCtg, SCtgDBCache *dbCache, SCtgTbCache* pCache) {
  if (pCache) {
    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache); 
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

void ctgReleaseTbIndexToCache(SCatalog* pCtg, SCtgDBCache *dbCache, SCtgTbCache* pCache) {
  if (pCache) {
    CTG_UNLOCK(CTG_READ, &pCache->indexLock);
    taosHashRelease(dbCache->tbCache, pCache); 
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache) {
  SCtgDBCache *dbCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {  
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }

  bool inCache = false;
  ctgRLockVgInfo(pCtg, dbCache, &inCache);
  if (!inCache) {
    ctgDebug("vgInfo of db %s not in cache", dbFName);
    goto _return;
  }

  *pCache = dbCache;

  CTG_CACHE_STAT_INC(numOfVgHit, 1);

  ctgDebug("Got db vgInfo from cache, dbFName:%s", dbFName);
  
  return TSDB_CODE_SUCCESS;

_return:

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }

  *pCache = NULL;

  CTG_CACHE_STAT_INC(numOfVgMiss, 1);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireTbMetaFromCache(SCatalog* pCtg, char *dbFName, char* tbName, SCtgDBCache **pDb, SCtgTbCache** pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache* pCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }
  
  pCache = taosHashAcquire(dbCache->tbCache, tbName, strlen(tbName));
  if (NULL == pCache) {
    ctgDebug("tb %s not in cache, dbFName:%s", tbName, dbFName);
    goto _return;
  }

  CTG_LOCK(CTG_READ, &pCache->metaLock);
  if (NULL == pCache->pMeta) {
    ctgDebug("tb %s meta not in cache, dbFName:%s", tbName, dbFName);
    goto _return;
  }

  *pDb = dbCache;
  *pTb = pCache;

  ctgDebug("tb %s meta got in cache, dbFName:%s", tbName, dbFName);
  
  CTG_CACHE_STAT_INC(numOfMetaHit, 1);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);

  CTG_CACHE_STAT_INC(numOfMetaMiss, 1);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireStbMetaFromCache(SCatalog* pCtg, char *dbFName, uint64_t suid, SCtgDBCache **pDb, SCtgTbCache** pTb) {
  SCtgDBCache* dbCache = NULL;
  SCtgTbCache* pCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }
  
  char* stName = taosHashAcquire(dbCache->stbCache, &suid, sizeof(suid));
  if (NULL == stName) {
    ctgDebug("stb 0x%" PRIx64 " not in cache, dbFName:%s", suid, dbFName);
    goto _return;
  }

  pCache = taosHashAcquire(dbCache->tbCache, stName, strlen(stName));
  if (NULL == pCache) {
    ctgDebug("stb 0x%" PRIx64 " name %s not in cache, dbFName:%s", suid, stName, dbFName);
    taosHashRelease(dbCache->stbCache, stName);
    goto _return;
  }

  CTG_LOCK(CTG_READ, &pCache->metaLock);
  if (NULL == pCache->pMeta) {
    ctgDebug("stb 0x%" PRIx64 " meta not in cache, dbFName:%s", suid, dbFName);
    goto _return;
  }

  *pDb = dbCache;
  *pTb = pCache;

  ctgDebug("stb 0x%" PRIx64 " meta got in cache, dbFName:%s", suid, dbFName);
  
  CTG_CACHE_STAT_INC(numOfMetaHit, 1);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);

  CTG_CACHE_STAT_INC(numOfMetaMiss, 1);

  *pDb = NULL;
  *pTb = NULL;
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgAcquireTbIndexFromCache(SCatalog* pCtg, char *dbFName, char* tbName, SCtgDBCache **pDb, SCtgTbCache** pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache* pCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }
  
  int32_t sz = 0;
  pCache = taosHashAcquire(dbCache->tbCache, tbName, strlen(tbName));
  if (NULL == pCache) {
    ctgDebug("tb %s not in cache, dbFName:%s", tbName, dbFName);
    goto _return;
  }

  CTG_LOCK(CTG_READ, &pCache->indexLock);
  if (NULL == pCache->pIndex) {
    ctgDebug("tb %s index not in cache, dbFName:%s", tbName, dbFName);
    goto _return;
  }

  *pDb = dbCache;
  *pTb = pCache;

  ctgDebug("tb %s index got in cache, dbFName:%s", tbName, dbFName);
  
  CTG_CACHE_STAT_INC(numOfIndexHit, 1);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbIndexToCache(pCtg, dbCache, pCache);

  CTG_CACHE_STAT_INC(numOfIndexMiss, 1);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgTbMetaExistInCache(SCatalog* pCtg, char *dbFName, char* tbName, int32_t *exist) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  ctgAcquireTbMetaFromCache(pCtg, dbFName, tbName, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
   
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgReadTbMetaFromCache(SCatalog* pCtg, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;  
  *pTableMeta = NULL;

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    strcpy(dbFName, ctx->pName->dbname);
  } else {
    tNameGetFullDbName(ctx->pName, dbFName);
  }

  ctgAcquireTbMetaFromCache(pCtg, dbFName, ctx->pName->tname, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta* tbMeta = tbCache->pMeta;
  ctx->tbInfo.inCache = true;
  ctx->tbInfo.dbId = dbCache->dbId;
  ctx->tbInfo.suid = tbMeta->suid;
  ctx->tbInfo.tbType = tbMeta->tableType;
 
  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    int32_t metaSize = CTG_META_SIZE(tbMeta);
    *pTableMeta = taosMemoryCalloc(1, metaSize);
    if (NULL == *pTableMeta) {
      ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    memcpy(*pTableMeta, tbMeta, metaSize);
    
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", ctx->pName->tname, tbMeta->tableType, dbFName);
    return TSDB_CODE_SUCCESS;
  }

  // PROCESS FOR CHILD TABLE
  
  int32_t metaSize = sizeof(SCTableMeta);
  *pTableMeta = taosMemoryCalloc(1, metaSize);
  if (NULL == *pTableMeta) {
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(*pTableMeta, tbMeta, metaSize);
  
  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  ctgDebug("Got ctb %s meta from cache, will continue to get its stb meta, type:%d, dbFName:%s", 
           ctx->pName->tname, ctx->tbInfo.tbType, dbFName);

  ctgAcquireStbMetaFromCache(pCtg, dbFName, ctx->tbInfo.suid, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    taosMemoryFreeClear(*pTableMeta);
    ctgDebug("stb 0x%" PRIx64 " meta not in cache", ctx->tbInfo.suid);
    return TSDB_CODE_SUCCESS;
  }
  
  STableMeta* stbMeta = tbCache->pMeta;
  if (stbMeta->suid != ctx->tbInfo.suid) {    
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid 0x%"PRIx64 , stbMeta->suid, ctx->tbInfo.suid);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  metaSize = CTG_META_SIZE(stbMeta);
  *pTableMeta = taosMemoryRealloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(&(*pTableMeta)->sversion, &stbMeta->sversion, metaSize - sizeof(SCTableMeta));

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  ctgDebug("Got tb %s meta from cache, dbFName:%s", ctx->pName->tname, dbFName);
  
  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  taosMemoryFreeClear(*pTableMeta);
  
  CTG_RET(code);
}

int32_t ctgReadTbVerFromCache(SCatalog *pCtg, SName *pTableName, int32_t *sver, int32_t *tver, int32_t *tbType, uint64_t *suid,
                              char *stbName) {
  *sver = -1;
  *tver = -1;

  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;  
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, dbFName);

  ctgAcquireTbMetaFromCache(pCtg, dbFName, pTableName->tname, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta* tbMeta = tbCache->pMeta;
  *tbType = tbMeta->tableType;
  *suid = tbMeta->suid;

  if (*tbType != TSDB_CHILD_TABLE) {
    *sver = tbMeta->sversion;
    *tver = tbMeta->tversion;

    ctgDebug("Got tb %s ver from cache, dbFName:%s, tbType:%d, sver:%d, tver:%d, suid:0x%" PRIx64, 
             pTableName->tname, dbFName, *tbType, *sver, *tver, *suid);

    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  // PROCESS FOR CHILD TABLE
  
  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  ctgDebug("Got ctb %s ver from cache, will continue to get its stb ver, dbFName:%s", pTableName->tname, dbFName);
    
  ctgAcquireStbMetaFromCache(pCtg, dbFName, *suid, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgDebug("stb 0x%" PRIx64 " meta not in cache", *suid);
    return TSDB_CODE_SUCCESS;
  }
  
  STableMeta* stbMeta = tbCache->pMeta;
  if (stbMeta->suid != *suid) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid:0x%" PRIx64 , stbMeta->suid, *suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  size_t nameLen = 0;
  char  *name = taosHashGetKey(tbCache, &nameLen);

  strncpy(stbName, name, nameLen);
  stbName[nameLen] = 0;

  *sver = stbMeta->sversion;
  *tver = stbMeta->tversion;

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  ctgDebug("Got tb %s sver %d tver %d from cache, type:%d, dbFName:%s", pTableName->tname, *sver, *tver, *tbType, dbFName);

  return TSDB_CODE_SUCCESS;
}


int32_t ctgReadTbTypeFromCache(SCatalog* pCtg, char* dbFName, char *tbName, int32_t *tbType) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;  
  CTG_ERR_RET(ctgAcquireTbMetaFromCache(pCtg, dbFName, tbName, &dbCache, &tbCache));
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  *tbType = tbCache->pMeta->tableType;
  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  ctgDebug("Got tb %s tbType %d from cache, dbFName:%s", tbName, *tbType, dbFName);  
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgReadTbIndexFromCache(SCatalog* pCtg, SName* pTableName, SArray** pRes) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;  
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, dbFName);

  *pRes = NULL;

  ctgAcquireTbIndexFromCache(pCtg, dbFName, pTableName->tname, &dbCache, &tbCache);
  if (NULL == tbCache) {
    ctgReleaseTbIndexToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgCloneTableIndex(tbCache->pIndex->pIndex, pRes));

_return:

  ctgReleaseTbIndexToCache(pCtg, dbCache, tbCache);

  CTG_RET(code);
}

int32_t ctgChkAuthFromCache(SCatalog* pCtg, char* user, char* dbFName, AUTH_TYPE type, bool *inCache, bool *pass) {
  char *p = strchr(dbFName, '.');
  if (p) {
    ++p;
  } else {
    p = dbFName;
  }
  
  if (IS_SYS_DBNAME(p)) {
    *inCache = true;
    *pass = true;
    ctgDebug("sysdb %s, pass", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgUserAuth *pUser = (SCtgUserAuth *)taosHashGet(pCtg->userCache, user, strlen(user));
  if (NULL == pUser) {
    ctgDebug("user not in cache, user:%s", user);
    goto _return;
  }

  *inCache = true;

  ctgDebug("Got user from cache, user:%s", user);
  CTG_CACHE_STAT_INC(numOfUserHit, 1);
  
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
  CTG_CACHE_STAT_INC(numOfUserMiss, 1);
  
  return TSDB_CODE_SUCCESS;
}

void ctgDequeue(SCtgCacheOperation **op) {
  SCtgQNode *orig = gCtgMgmt.queue.head;
  
  SCtgQNode *node = gCtgMgmt.queue.head->next;
  gCtgMgmt.queue.head = gCtgMgmt.queue.head->next;

  CTG_QUEUE_DEC();
  
  taosMemoryFreeClear(orig);

  *op = node->op;
}


int32_t ctgEnqueue(SCatalog* pCtg, SCtgCacheOperation *operation) {
  SCtgQNode *node = taosMemoryCalloc(1, sizeof(SCtgQNode));
  if (NULL == node) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  bool syncOp = operation->syncOp;
  char* opName = gCtgCacheOperation[operation->opId].name;
  if (operation->syncOp) {
    tsem_init(&operation->rspSem, 0, 0);
  }
  
  node->op = operation;

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);
  if (gCtgMgmt.queue.stopQueue) {
    ctgFreeQNode(node);
    CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);
    CTG_RET(TSDB_CODE_CTG_EXIT);
  }
  gCtgMgmt.queue.stopQueue = operation->stopQueue;
  gCtgMgmt.queue.tail->next = node;
  gCtgMgmt.queue.tail = node;
  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);

  ctgDebug("action [%s] added into queue", opName);

  CTG_QUEUE_INC();
  CTG_RT_STAT_INC(numOfOpEnqueue, 1);

  tsem_post(&gCtgMgmt.queue.reqSem);

  if (syncOp) {
    if (!operation->unLocked) {
      CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock);
    }
    tsem_wait(&operation->rspSem);
    if (!operation->unLocked) {
      CTG_LOCK(CTG_READ, &gCtgMgmt.lock);
    }
    taosMemoryFree(operation);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgDropDbCacheEnqueue(SCatalog* pCtg, const char *dbFName, int64_t dbId) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_DROP_DB_CACHE;
  
  SCtgDropDBMsg *msg = taosMemoryMalloc(sizeof(SCtgDropDBMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropDBMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->dbId = dbId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(op->data);
  CTG_RET(code);
}

int32_t ctgDropDbVgroupEnqueue(SCatalog* pCtg, const char *dbFName, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_DROP_DB_VGROUP;
  op->syncOp = syncOp;
  
  SCtgDropDbVgroupMsg *msg = taosMemoryMalloc(sizeof(SCtgDropDbVgroupMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropDbVgroupMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(op->data);
  CTG_RET(code);
}



int32_t ctgDropStbMetaEnqueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *stbName, uint64_t suid, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_DROP_STB_META;
  op->syncOp = syncOp;
  
  SCtgDropStbMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgDropStbMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropStbMetaMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  strncpy(msg->stbName, stbName, sizeof(msg->stbName));
  msg->dbId = dbId;
  msg->suid = suid;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(op->data);
  CTG_RET(code);
}



int32_t ctgDropTbMetaEnqueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *tbName, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_DROP_TB_META;
  op->syncOp = syncOp;
  
  SCtgDropTblMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgDropTblMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTblMetaMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  strncpy(msg->tbName, tbName, sizeof(msg->tbName));
  msg->dbId = dbId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(op->data);
  CTG_RET(code);
}

int32_t ctgUpdateVgroupEnqueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, SDBVgInfo* dbInfo, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_UPDATE_VGROUP;
  op->syncOp = syncOp;
  
  SCtgUpdateVgMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateVgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateVgMsg));
    ctgFreeVgInfo(dbInfo);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  strncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->pCtg = pCtg;
  msg->dbId = dbId;
  msg->dbInfo = dbInfo;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeVgInfo(dbInfo);
  taosMemoryFreeClear(op->data);
  taosMemoryFreeClear(op);
  CTG_RET(code);
}

int32_t ctgUpdateTbMetaEnqueue(SCatalog* pCtg, STableMetaOutput *output, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_UPDATE_TB_META;
  op->syncOp = syncOp;
  
  SCtgUpdateTbMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbMetaMsg));
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *p = strchr(output->dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    memmove(output->dbFName, p + 1, strlen(p + 1));
  }

  msg->pCtg = pCtg;
  msg->pMeta = output;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;
  
_return:

  if (output) {
    taosMemoryFree(output->tbMeta);
    taosMemoryFree(output);
  }

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgUpdateVgEpsetEnqueue(SCatalog* pCtg, char *dbFName, int32_t vgId, SEpSet* pEpSet) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_UPDATE_VG_EPSET;
  
  SCtgUpdateEpsetMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateEpsetMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateEpsetMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  strcpy(msg->dbFName, dbFName);
  msg->vgId = vgId;
  msg->epSet = *pEpSet;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;
  
_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}



int32_t ctgUpdateUserEnqueue(SCatalog* pCtg, SGetUserAuthRsp *pAuth, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_UPDATE_USER;
  op->syncOp = syncOp;
  
  SCtgUpdateUserMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateUserMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateUserMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  msg->userAuth = *pAuth;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));
  
  return TSDB_CODE_SUCCESS;
  
_return:

  tFreeSGetUserAuthRsp(pAuth);
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgUpdateTbIndexEnqueue(SCatalog* pCtg, STableIndex **pIndex, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_UPDATE_TB_INDEX;
  op->syncOp = syncOp;
  
  SCtgUpdateTbIndexMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbIndexMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbIndexMsg));
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  msg->pIndex = *pIndex;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  *pIndex = NULL;
  return TSDB_CODE_SUCCESS;
  
_return:

  taosArrayDestroyEx((*pIndex)->pIndex, tFreeSTableIndexInfo);
  taosMemoryFreeClear(*pIndex);
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgDropTbIndexEnqueue(SCatalog* pCtg, SName* pName, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_DROP_TB_INDEX;
  op->syncOp = syncOp;
  
  SCtgDropTbIndexMsg *msg = taosMemoryMalloc(sizeof(SCtgDropTbIndexMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTbIndexMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  tNameGetFullDbName(pName, msg->dbFName);
  strcpy(msg->tbName, pName->tname);

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));
  
  return TSDB_CODE_SUCCESS;
  
_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


int32_t ctgClearCacheEnqueue(SCatalog* pCtg, bool freeCtg, bool stopQueue, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  op->opId = CTG_OP_CLEAR_CACHE;
  op->syncOp = syncOp;
  op->stopQueue = stopQueue;
  op->unLocked = true;
  
  SCtgClearCacheMsg *msg = taosMemoryMalloc(sizeof(SCtgClearCacheMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgClearCacheMsg));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  msg->pCtg = pCtg;
  msg->freeCtg = freeCtg;
  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));
  
  return TSDB_CODE_SUCCESS;
  
_return:

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
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      qError("taosArrayInit %d failed, id:0x%"PRIx64", slot idx:%d, type:%d", CTG_DEFAULT_RENT_SLOT_SIZE, id, widx, mgmt->type);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  if (NULL == taosArrayPush(slot->meta, meta)) {
    qError("taosArrayPush meta to rent failed, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  slot->needSort = true;

  qDebug("add meta to rent, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

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
    qDebug("empty meta slot, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
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
    qDebug("meta not found in slot, id:0x%"PRIx64", slot idx:%d, type:%d, size:%d", id, widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  memcpy(orig, meta, size);

  qDebug("meta in rent updated, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  if (code) {
    qDebug("meta in rent update failed, will try to add it, code:%x, id:0x%"PRIx64", slot idx:%d, type:%d", code, id, widx, mgmt->type);
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
    qError("empty meta slot, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d", widx, mgmt->type);
  }

  int32_t idx = taosArraySearchIdx(slot->meta, &id, searchCompare, TD_EQ);
  if (idx < 0) {
    qError("meta not found in slot, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  taosArrayRemove(slot->meta, idx);

  qDebug("meta in rent removed, id:0x%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

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
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
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

  newDBCache.tbCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.tbCache) {
    ctgError("taosHashInit %d metaCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  newDBCache.stbCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.stbCache) {
    ctgError("taosHashInit %d stbCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  code = taosHashPut(pCtg->dbCache, dbFName, strlen(dbFName), &newDBCache, sizeof(SCtgDBCache));
  if (code) {
    if (HASH_NODE_EXIST(code)) {
      ctgDebug("db already in cache, dbFName:%s", dbFName);
      goto _return;
    }
    
    ctgError("taosHashPut db to cache failed, dbFName:%s", dbFName);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  CTG_CACHE_STAT_INC(numOfDb, 1);
 
  SDbVgVersion vgVersion = {.dbId = newDBCache.dbId, .vgVersion = -1};
  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));

  ctgDebug("db added to cache, dbFName:%s, dbId:0x%"PRIx64, dbFName, dbId);

  CTG_ERR_RET(ctgMetaRentAdd(&pCtg->dbRent, &vgVersion, dbId, sizeof(SDbVgVersion)));

  ctgDebug("db added to rent, dbFName:%s, vgVersion:%d, dbId:0x%"PRIx64, dbFName, vgVersion.vgVersion, dbId);

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeDbCache(&newDBCache);

  CTG_RET(code);
}


void ctgRemoveStbRent(SCatalog* pCtg, SCtgDBCache *dbCache) {
  if (NULL == dbCache->stbCache) {
    return;
  }
  
  void *pIter = taosHashIterate(dbCache->stbCache, NULL);
  while (pIter) {
    uint64_t *suid = NULL;
    suid = taosHashGetKey(pIter, NULL);

    if (TSDB_CODE_SUCCESS == ctgMetaRentRemove(&pCtg->stbRent, *suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare)) {
      ctgDebug("stb removed from rent, suid:0x%"PRIx64, *suid);
    }
        
    pIter = taosHashIterate(dbCache->stbCache, pIter);
  }
}


int32_t ctgRemoveDBFromCache(SCatalog* pCtg, SCtgDBCache *dbCache, const char* dbFName) {
  uint64_t dbId = dbCache->dbId;
  
  ctgInfo("start to remove db from cache, dbFName:%s, dbId:0x%"PRIx64, dbFName, dbCache->dbId);

  CTG_LOCK(CTG_WRITE, &dbCache->dbLock);

  atomic_store_8(&dbCache->deleted, 1);
  ctgRemoveStbRent(pCtg, dbCache);
  ctgFreeDbCache(dbCache);

  CTG_UNLOCK(CTG_WRITE, &dbCache->dbLock);

  CTG_ERR_RET(ctgMetaRentRemove(&pCtg->dbRent, dbId, ctgDbVgVersionSortCompare, ctgDbVgVersionSearchCompare));
  ctgDebug("db removed from rent, dbFName:%s, dbId:0x%"PRIx64, dbFName, dbId);

  if (taosHashRemove(pCtg->dbCache, dbFName, strlen(dbFName))) {
    ctgInfo("taosHashRemove from dbCache failed, may be removed, dbFName:%s", dbFName);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  CTG_CACHE_STAT_DEC(numOfDb, 1);
  ctgInfo("db removed from cache, dbFName:%s, dbId:0x%"PRIx64, dbFName, dbId);
  
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

int32_t ctgUpdateRentStbVersion(SCatalog *pCtg, char* dbFName, char* tbName, uint64_t dbId, uint64_t suid, SCtgTbCache* pCache) {
  SSTableVersion metaRent = {.dbId = dbId, .suid = suid};
  if (pCache->pMeta) {
    metaRent.sversion = pCache->pMeta->sversion;
    metaRent.tversion = pCache->pMeta->tversion;
  }

  if (pCache->pIndex) {
    metaRent.smaVer = pCache->pIndex->version;
  }
  
  strcpy(metaRent.dbFName, dbFName);
  strcpy(metaRent.stbName, tbName);
  
  CTG_ERR_RET(ctgMetaRentUpdate(&pCtg->stbRent, &metaRent, metaRent.suid, sizeof(SSTableVersion), ctgStbVersionSortCompare, ctgStbVersionSearchCompare));

  ctgDebug("db %s,0x%" PRIx64 " stb %s,0x%" PRIx64 " sver %d tver %d smaVer %d updated to stbRent", 
           dbFName, dbId, tbName, suid, metaRent.sversion, metaRent.tversion, metaRent.smaVer);

  return TSDB_CODE_SUCCESS;         
}


int32_t ctgWriteTbMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, uint64_t dbId, char *tbName, STableMeta *meta, int32_t metaSize) {
  if (NULL == dbCache->tbCache || NULL == dbCache->stbCache) {
    taosMemoryFree(meta);
    ctgError("db is dropping, dbId:0x%"PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  bool isStb = meta->tableType == TSDB_SUPER_TABLE;
  SCtgTbCache* pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
  STableMeta *orig = (pCache ? pCache->pMeta : NULL);
  int8_t origType = 0;
  uint64_t origSuid = 0;
  
  if (orig) {
    origType = orig->tableType;

    if (origType == meta->tableType && orig->uid == meta->uid && (origType == TSDB_CHILD_TABLE || (orig->sversion >= meta->sversion && orig->tversion >= meta->tversion))) {
      taosMemoryFree(meta);
      ctgDebug("ignore table %s meta update", tbName);
      return TSDB_CODE_SUCCESS;
    }
    
    if (origType == TSDB_SUPER_TABLE) {
      if (taosHashRemove(dbCache->stbCache, &orig->suid, sizeof(orig->suid))) {
        ctgError("stb not exist in stbCache, dbFName:%s, stb:%s, suid:0x%"PRIx64, dbFName, tbName, orig->suid);
      } else {
        CTG_CACHE_STAT_DEC(numOfStb, 1);
        ctgDebug("stb removed from stbCache, dbFName:%s, stb:%s, suid:0x%"PRIx64, dbFName, tbName, orig->suid);
      }
      
      origSuid = orig->suid;
    }
  }

  if (NULL == pCache) {
    SCtgTbCache cache = {0};
    cache.pMeta = meta;
    if (taosHashPut(dbCache->tbCache, tbName, strlen(tbName), &cache, sizeof(SCtgTbCache)) != 0) {
      taosMemoryFree(meta);
      ctgError("taosHashPut new tbCache failed, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
  } else {
    taosMemoryFree(pCache->pMeta);
    pCache->pMeta = meta;
  }

  if (NULL == orig) {
    CTG_CACHE_STAT_INC(numOfTbl, 1);
  }

  ctgDebug("tbmeta updated to cache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
  ctgdShowTableMeta(pCtg, tbName, meta);

  if (!isStb) {
    return TSDB_CODE_SUCCESS;
  }

  if (origSuid != meta->suid && taosHashPut(dbCache->stbCache, &meta->suid, sizeof(meta->suid), tbName, strlen(tbName) + 1) != 0) {
    ctgError("taosHashPut to stable cache failed, suid:0x%"PRIx64, meta->suid);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  CTG_CACHE_STAT_INC(numOfStb, 1);

  ctgDebug("stb 0x%" PRIx64 " updated to cache, dbFName:%s, tbName:%s, tbType:%d", meta->suid, dbFName, tbName, meta->tableType);

  CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbId, meta->suid, pCache));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgWriteTbIndexToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char* dbFName, char *tbName, STableIndex **index) {
  if (NULL == dbCache->tbCache) {
    ctgFreeSTableIndex(*index);
    taosMemoryFreeClear(*index);
    ctgError("db is dropping, dbId:0x%"PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  STableIndex* pIndex = *index;
  uint64_t suid = pIndex->suid;
  SCtgTbCache* pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
  if (NULL == pCache) {
    SCtgTbCache cache = {0};
    cache.pIndex = pIndex;
    
    if (taosHashPut(dbCache->tbCache, tbName, strlen(tbName), &cache, sizeof(cache)) != 0) {
      ctgFreeSTableIndex(*index);
      taosMemoryFreeClear(*index);
      ctgError("taosHashPut new tbCache failed, tbName:%s", tbName);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    *index = NULL;
    ctgDebug("table %s index updated to cache, ver:%d, num:%d", tbName, pIndex->version, (int32_t)taosArrayGetSize(pIndex->pIndex));

    if (suid) {
      CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbCache->dbId, pIndex->suid, &cache));
    }
    
    return TSDB_CODE_SUCCESS;
  }

  if (pCache->pIndex) {
    if (0 == suid) {
      suid = pCache->pIndex->suid;
    }
    taosArrayDestroyEx(pCache->pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pCache->pIndex);
  }

  pCache->pIndex = pIndex;
  *index = NULL;

  ctgDebug("table %s index updated to cache, ver:%d, num:%d", tbName, pIndex->version, (int32_t)taosArrayGetSize(pIndex->pIndex));

  if (suid) {
    CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbCache->dbId, suid, pCache));
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgUpdateTbMetaToCache(SCatalog* pCtg, STableMetaOutput* pOut, bool syncReq) {
  STableMetaOutput* pOutput = NULL;
  int32_t code = 0;
  
  CTG_ERR_RET(ctgCloneMetaOutput(pOut, &pOutput));
  CTG_ERR_JRET(ctgUpdateTbMetaEnqueue(pCtg, pOutput, syncReq));

  return TSDB_CODE_SUCCESS;
  
_return:

  ctgFreeSTableMetaOutput(pOutput);
  CTG_RET(code);
}

void ctgClearAllInstance(void) {
  SCatalog* pCtg = NULL;

  void* pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog**)pIter;

    if (pCtg) {
      ctgClearHandle(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }
}

void ctgFreeAllInstance(void) {
  SCatalog* pCtg = NULL;

  void* pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog**)pIter;

    if (pCtg) {
      ctgFreeHandle(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  taosHashClear(gCtgMgmt.pCluster);
}


int32_t ctgOpUpdateVgroup(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgUpdateVgMsg *msg = operation->data;
  SDBVgInfo* dbInfo = msg->dbInfo;
  char* dbFName = msg->dbFName;
  SCatalog* pCtg = msg->pCtg;
  
  if (NULL == dbInfo->vgHash) {
    goto _return;
  }
  
  if (dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgHash) <= 0) {
    ctgError("invalid db vgInfo, dbFName:%s, vgHash:%p, vgVersion:%d, vgHashSize:%d", 
             dbFName, dbInfo->vgHash, dbInfo->vgVersion, taosHashGetSize(dbInfo->vgHash));
    CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  bool newAdded = false;
  SDbVgVersion vgVersion = {.dbId = msg->dbId, .vgVersion = dbInfo->vgVersion, .numOfTable = dbInfo->numOfTable};

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetAddDBCache(msg->pCtg, dbFName, msg->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:0x%"PRIx64, dbFName, msg->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgVgCache *vgCache = &dbCache->vgCache;
  CTG_ERR_JRET(ctgWLockVgInfo(msg->pCtg, dbCache));
  
  if (vgCache->vgInfo) {
    SDBVgInfo *vgInfo = vgCache->vgInfo;
    
    if (dbInfo->vgVersion < vgInfo->vgVersion) {
      ctgDebug("db vgVer is old, dbFName:%s, vgVer:%d, curVer:%d", dbFName, dbInfo->vgVersion, vgInfo->vgVersion);
      ctgWUnlockVgInfo(dbCache);
      
      goto _return;
    }

    if (dbInfo->vgVersion == vgInfo->vgVersion && dbInfo->numOfTable == vgInfo->numOfTable) {
      ctgDebug("no new db vgVer or numOfTable, dbFName:%s, vgVer:%d, numOfTable:%d", dbFName, dbInfo->vgVersion, dbInfo->numOfTable);
      ctgWUnlockVgInfo(dbCache);
      
      goto _return;
    }

    ctgFreeVgInfo(vgInfo);
  }

  vgCache->vgInfo = dbInfo;
  msg->dbInfo = NULL;

  ctgDebug("db vgInfo updated, dbFName:%s, vgVer:%d, dbId:0x%"PRIx64, dbFName, vgVersion.vgVersion, vgVersion.dbId);

  ctgWUnlockVgInfo(dbCache);

  dbCache = NULL;

  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));
  CTG_ERR_RET(ctgMetaRentUpdate(&msg->pCtg->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion), ctgDbVgVersionSortCompare, ctgDbVgVersionSearchCompare));

_return:

  ctgFreeVgInfo(msg->dbInfo);
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpDropDbCache(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgDropDBMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(msg->pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    goto _return;
  }
  
  if (dbCache->dbId != msg->dbId) {
    ctgInfo("dbId already updated, dbFName:%s, dbId:0x%"PRIx64 ", targetId:0x%"PRIx64, msg->dbFName, dbCache->dbId, msg->dbId);
    goto _return;
  }
  
  CTG_ERR_JRET(ctgRemoveDBFromCache(pCtg, dbCache, msg->dbFName));

_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpDropDbVgroup(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgDropDbVgroupMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(msg->pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    goto _return;
  }
  
  CTG_ERR_RET(ctgWLockVgInfo(pCtg, dbCache));
  
  ctgFreeVgInfo(dbCache->vgCache.vgInfo);
  dbCache->vgCache.vgInfo = NULL;

  ctgDebug("db vgInfo removed, dbFName:%s", msg->dbFName);

  ctgWUnlockVgInfo(dbCache);

_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


int32_t ctgOpUpdateTbMeta(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgUpdateTbMetaMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;
  STableMetaOutput* pMeta = msg->pMeta;
  SCtgDBCache *dbCache = NULL;

  if ((!CTG_IS_META_CTABLE(pMeta->metaType)) && NULL == pMeta->tbMeta) {
    ctgError("no valid tbmeta got from meta rsp, dbFName:%s, tbName:%s", pMeta->dbFName, pMeta->tbName);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (CTG_IS_META_BOTH(pMeta->metaType) && TSDB_SUPER_TABLE != pMeta->tbMeta->tableType) {
    ctgError("table type error, expected:%d, actual:%d", TSDB_SUPER_TABLE, pMeta->tbMeta->tableType);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }    
  
  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, pMeta->dbFName, pMeta->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:0x%" PRIx64, pMeta->dbFName, pMeta->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (CTG_IS_META_TABLE(pMeta->metaType) || CTG_IS_META_BOTH(pMeta->metaType)) {
    int32_t metaSize = CTG_META_SIZE(pMeta->tbMeta);
    CTG_ERR_JRET(ctgWriteTbMetaToCache(pCtg, dbCache, pMeta->dbFName, pMeta->dbId, pMeta->tbName, pMeta->tbMeta, metaSize));
    pMeta->tbMeta = NULL;
  }

  if (CTG_IS_META_CTABLE(pMeta->metaType) || CTG_IS_META_BOTH(pMeta->metaType)) {
    SCTableMeta* ctbMeta = taosMemoryMalloc(sizeof(SCTableMeta));
    if (NULL == ctbMeta) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    memcpy(ctbMeta, &pMeta->ctbMeta, sizeof(SCTableMeta));
    CTG_ERR_JRET(ctgWriteTbMetaToCache(pCtg, dbCache, pMeta->dbFName, pMeta->dbId, pMeta->ctbName, (STableMeta *)ctbMeta, sizeof(SCTableMeta)));
  }

_return:

  if (pMeta) {
    taosMemoryFreeClear(pMeta->tbMeta);
    taosMemoryFreeClear(pMeta);
  }
  
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


int32_t ctgOpDropStbMeta(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgDropStbMetaMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  if (msg->dbId && (dbCache->dbId != msg->dbId)) {
    ctgDebug("dbId already modified, dbFName:%s, current:0x%"PRIx64", dbId:0x%"PRIx64", stb:%s, suid:0x%"PRIx64, 
             msg->dbFName, dbCache->dbId, msg->dbId, msg->stbName, msg->suid);
    return TSDB_CODE_SUCCESS;
  }
  
  if (taosHashRemove(dbCache->stbCache, &msg->suid, sizeof(msg->suid))) {
    ctgDebug("stb not exist in stbCache, may be removed, dbFName:%s, stb:%s, suid:0x%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  } else {
    CTG_CACHE_STAT_DEC(numOfStb, 1);
  }

  SCtgTbCache* pTbCache = taosHashGet(dbCache->tbCache, msg->stbName, strlen(msg->stbName));
  if (NULL == pTbCache) {
    ctgDebug("stb %s already not in cache", msg->stbName);
    goto _return;
  }

  CTG_LOCK(CTG_WRITE, &pTbCache->metaLock);
  ctgFreeTbCacheImpl(pTbCache);
  CTG_UNLOCK(CTG_WRITE, &pTbCache->metaLock);

  if (taosHashRemove(dbCache->tbCache, msg->stbName, strlen(msg->stbName))) {  
    ctgError("stb not exist in cache, dbFName:%s, stb:%s, suid:0x%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  } else {
    CTG_CACHE_STAT_DEC(numOfTbl, 1);
  }
  
  ctgInfo("stb removed from cache, dbFName:%s, stbName:%s, suid:0x%"PRIx64, msg->dbFName, msg->stbName, msg->suid);

  CTG_ERR_JRET(ctgMetaRentRemove(&msg->pCtg->stbRent, msg->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare));
  
  ctgDebug("stb removed from rent, dbFName:%s, stbName:%s, suid:0x%"PRIx64, msg->dbFName, msg->stbName, msg->suid);
  
_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpDropTbMeta(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgDropTblMetaMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;

  SCtgDBCache *dbCache = NULL;
  ctgGetDBCache(pCtg, msg->dbFName, &dbCache);
  if (NULL == dbCache) {
    goto _return;
  }

  if (dbCache->dbId != msg->dbId) {
    ctgDebug("dbId 0x%" PRIx64 " not match with curId 0x%"PRIx64", dbFName:%s, tbName:%s", msg->dbId, dbCache->dbId, msg->dbFName, msg->tbName);
    goto _return;
  }

  SCtgTbCache* pTbCache = taosHashGet(dbCache->tbCache, msg->tbName, strlen(msg->tbName));
  if (NULL == pTbCache) {
    ctgDebug("tb %s already not in cache", msg->tbName);
    goto _return;
  }

  CTG_LOCK(CTG_WRITE, &pTbCache->metaLock);
  ctgFreeTbCacheImpl(pTbCache);
  CTG_UNLOCK(CTG_WRITE, &pTbCache->metaLock);
  
  if (taosHashRemove(dbCache->tbCache, msg->tbName, strlen(msg->tbName))) {
    ctgError("tb %s not exist in cache, dbFName:%s", msg->tbName, msg->dbFName);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  } else {
    CTG_CACHE_STAT_DEC(numOfTbl, 1);
  }

  ctgDebug("table %s removed from cache, dbFName:%s", msg->tbName, msg->dbFName);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateUser(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgUpdateUserMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;
  
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

int32_t ctgOpUpdateEpset(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgUpdateEpsetMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;
  
  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    ctgDebug("db %s not exist, ignore epset update", msg->dbFName);
    goto _return;
  }

  CTG_ERR_JRET(ctgWLockVgInfo(pCtg, dbCache));

  SDBVgInfo *vgInfo = dbCache->vgCache.vgInfo;  
  if (NULL == vgInfo) {
    ctgDebug("vgroup in db %s not cached, ignore epset update", msg->dbFName);
    goto _return;
  }
  
  SVgroupInfo* pInfo = taosHashGet(vgInfo->vgHash, &msg->vgId, sizeof(msg->vgId));
  if (NULL == pInfo) {
    ctgDebug("no vgroup %d in db %s, ignore epset update", msg->vgId, msg->dbFName);
    goto _return;
  }

  SEp* pOrigEp = &pInfo->epSet.eps[pInfo->epSet.inUse];
  SEp* pNewEp = &msg->epSet.eps[msg->epSet.inUse];
  ctgDebug("vgroup %d epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d, dbFName:%s in ctg", 
          pInfo->vgId, pInfo->epSet.inUse, pInfo->epSet.numOfEps, pOrigEp->fqdn, pOrigEp->port, 
          msg->epSet.inUse, msg->epSet.numOfEps, pNewEp->fqdn, pNewEp->port, msg->dbFName);

  pInfo->epSet = msg->epSet;

_return:

  if (dbCache) {
    ctgWUnlockVgInfo(dbCache);
  }

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpUpdateTbIndex(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgUpdateTbIndexMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;
  STableIndex* pIndex = msg->pIndex;
  SCtgDBCache *dbCache = NULL;
   
  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, pIndex->dbFName, 0, &dbCache));

  CTG_ERR_JRET(ctgWriteTbIndexToCache(pCtg, dbCache, pIndex->dbFName, pIndex->tbName, &pIndex));

_return:

  if (pIndex) {
    taosArrayDestroyEx(pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pIndex);
  }
  
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpDropTbIndex(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgDropTbIndexMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;
  SCtgDBCache *dbCache = NULL;
   
  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  STableIndex* pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }
  strcpy(pIndex->tbName, msg->tbName);
  strcpy(pIndex->dbFName, msg->dbFName);
  pIndex->version = -1;

  CTG_ERR_JRET(ctgWriteTbIndexToCache(pCtg, dbCache, pIndex->dbFName, pIndex->tbName, &pIndex));

_return:

  if (pIndex) {
    taosArrayDestroyEx(pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pIndex);
  }
  
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}


int32_t ctgOpClearCache(SCtgCacheOperation *operation) {
  int32_t code = 0;
  SCtgClearCacheMsg *msg = operation->data;
  SCatalog* pCtg = msg->pCtg;

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.lock);

  if (pCtg) {
    if (msg->freeCtg) {
      ctgFreeHandle(pCtg);
    } else {
      ctgClearHandle(pCtg);
    }
    
    goto _return;
  }

  if (msg->freeCtg) {
    ctgFreeAllInstance();
  } else {
    ctgClearAllInstance();
  }

_return:

  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.lock);
 
  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

void ctgCleanupCacheQueue(void) {
  SCtgQNode *node = NULL;
  SCtgQNode *nodeNext = NULL;
  SCtgCacheOperation *op = NULL;
  bool stopQueue = false;

  while (true) {
    node = gCtgMgmt.queue.head->next;
    while (node) {
      if (node->op) {
        op = node->op;
        if (op->stopQueue) {
          SCatalog *pCtg = ((SCtgUpdateMsgHeader *)op->data)->pCtg;
          ctgDebug("process [%s] operation", gCtgCacheOperation[op->opId].name);
          (*gCtgCacheOperation[op->opId].func)(op);
          stopQueue = true;
          CTG_RT_STAT_INC(numOfOpDequeue, 1); 
        } else {
          taosMemoryFree(op->data);
          CTG_RT_STAT_INC(numOfOpAbort, 1); 
        }
        
        if (op->syncOp) {
          tsem_post(&op->rspSem);
        } else {
          taosMemoryFree(op);
        }
      }

      nodeNext = node->next;
      taosMemoryFree(node);
      
      node = nodeNext;
    }

    if (!stopQueue) {
      taosUsleep(1);
    } else {
      break;
    }
  }

  taosMemoryFreeClear(gCtgMgmt.queue.head);
  gCtgMgmt.queue.tail = NULL;
}

void* ctgUpdateThreadFunc(void* param) {
  setThreadName("catalog");

  qInfo("catalog update thread started");

  while (true) {
    if (tsem_wait(&gCtgMgmt.queue.reqSem)) {
      qError("ctg tsem_wait failed, error:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    }
    
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
      ctgCleanupCacheQueue();
      break;
    }

    SCtgCacheOperation *operation = NULL;
    ctgDequeue(&operation);
    SCatalog *pCtg = ((SCtgUpdateMsgHeader *)operation->data)->pCtg;

    ctgDebug("process [%s] operation", gCtgCacheOperation[operation->opId].name);
    
    (*gCtgCacheOperation[operation->opId].func)(operation);

    if (operation->syncOp) {
      tsem_post(&operation->rspSem);
    } else {
      taosMemoryFreeClear(operation);
    }

    CTG_RT_STAT_INC(numOfOpDequeue, 1); 

    ctgdShowCacheInfo();
    ctgdShowClusterCache(pCtg);
  }

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


int32_t ctgGetTbMetaFromCache(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  if (IS_SYS_DBNAME(ctx->pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(ctx->flag);
  }

  CTG_ERR_RET(ctgReadTbMetaFromCache(pCtg, ctx, pTableMeta));

  if (*pTableMeta) {
    if (CTG_FLAG_MATCH_STB(ctx->flag, (*pTableMeta)->tableType) &&
        ((!CTG_FLAG_IS_FORCE_UPDATE(ctx->flag)) || (CTG_FLAG_IS_SYS_DB(ctx->flag)))) {
      return TSDB_CODE_SUCCESS;
    }

    taosMemoryFreeClear(*pTableMeta);
  }

  if (CTG_FLAG_IS_UNKNOWN_STB(ctx->flag)) {
    CTG_FLAG_SET_STB(ctx->flag, ctx->tbInfo.tbType);
  }

  return TSDB_CODE_SUCCESS;
}

#if 0
int32_t ctgGetTbMetaBFromCache(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetasCtx* ctx, SArray** pResList) {
  int32_t tbNum = taosArrayGetSize(ctx->pNames);
  SName* fName = taosArrayGet(ctx->pNames, 0);
  int32_t fIdx = 0;
  
  for (int32_t i = 0; i < tbNum; ++i) {
    SName* pName = taosArrayGet(ctx->pNames, i);
    SCtgTbMetaCtx nctx = {0};
    nctx.flag = CTG_FLAG_UNKNOWN_STB;
    nctx.pName = pName;
    
    if (IS_SYS_DBNAME(pName->dbname)) {
      CTG_FLAG_SET_SYS_DB(nctx.flag);
    }

    STableMeta *pTableMeta = NULL;
    CTG_ERR_RET(ctgReadTbMetaFromCache(pCtg, &nctx, &pTableMeta));
    SMetaRes res = {0};
    
    if (pTableMeta) {
      if (CTG_FLAG_MATCH_STB(nctx.flag, pTableMeta->tableType) &&
          ((!CTG_FLAG_IS_FORCE_UPDATE(nctx.flag)) || (CTG_FLAG_IS_SYS_DB(nctx.flag)))) {
        res.pRes = pTableMeta;
      } else {
        taosMemoryFreeClear(pTableMeta);
      }
    }

    if (NULL == res.pRes) {
      if (NULL == ctx->pFetchs) {
        ctx->pFetchs = taosArrayInit(tbNum, sizeof(SCtgFetch));
      }
      
      if (CTG_FLAG_IS_UNKNOWN_STB(nctx.flag)) {
        CTG_FLAG_SET_STB(nctx.flag, nctx.tbInfo.tbType);
      }

      SCtgFetch fetch = {0};
      fetch.tbIdx = i;
      fetch.fetchIdx = fIdx++;
      fetch.flag = nctx.flag;

      taosArrayPush(ctx->pFetchs, &fetch);
    }
    
    taosArrayPush(ctx->pResList, &res);
  }

  if (NULL == ctx->pFetchs) {
    TSWAP(*pResList, ctx->pResList);
  }

  return TSDB_CODE_SUCCESS;
}
#endif

int32_t ctgGetTbMetasFromCache(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetasCtx* ctx, int32_t dbIdx, int32_t *fetchIdx, int32_t baseResIdx, SArray* pList) {
  int32_t tbNum = taosArrayGetSize(pList);
  SName* pName = taosArrayGet(pList, 0);
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  int32_t flag = CTG_FLAG_UNKNOWN_STB;
  uint64_t lastSuid = 0;
  STableMeta* lastTableMeta = NULL;
  
  if (IS_SYS_DBNAME(pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(flag);
    strcpy(dbFName, pName->dbname);
  } else {
    tNameGetFullDbName(pName, dbFName);
  }

  SCtgDBCache *dbCache = NULL;
  SCtgTbCache* pCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    for (int32_t i = 0; i < tbNum; ++i) {
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < tbNum; ++i) {
    SName* pName = taosArrayGet(pList, i);

    pCache = taosHashAcquire(dbCache->tbCache, pName->tname, strlen(pName->tname));
    if (NULL == pCache) {
      ctgDebug("tb %s not in cache, dbFName:%s", pName->tname, dbFName);
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);
      
      continue;
    }

    CTG_LOCK(CTG_READ, &pCache->metaLock);
    if (NULL == pCache->pMeta) {
      ctgDebug("tb %s meta not in cache, dbFName:%s", pName->tname, dbFName);
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);
      
      continue;
    }

    STableMeta* tbMeta = pCache->pMeta;

    SCtgTbMetaCtx nctx = {0};
    nctx.flag = flag;
    nctx.tbInfo.inCache = true;
    nctx.tbInfo.dbId = dbCache->dbId;
    nctx.tbInfo.suid = tbMeta->suid;
    nctx.tbInfo.tbType = tbMeta->tableType;

    SMetaRes res = {0};
    STableMeta* pTableMeta = NULL;
    if (tbMeta->tableType != TSDB_CHILD_TABLE) {
      int32_t metaSize = CTG_META_SIZE(tbMeta);
      pTableMeta = taosMemoryCalloc(1, metaSize);
      if (NULL == pTableMeta) {
        ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);
        CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }
    
      memcpy(pTableMeta, tbMeta, metaSize);
      
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache); 
      
      ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", pName->tname, tbMeta->tableType, dbFName);
      
      res.pRes = pTableMeta;
      taosArrayPush(ctx->pResList, &res);

      continue;
    }
    
    // PROCESS FOR CHILD TABLE

    if (lastSuid && tbMeta->suid == lastSuid && lastTableMeta) {
      cloneTableMeta(lastTableMeta, &pTableMeta);
      memcpy(pTableMeta, tbMeta, sizeof(SCTableMeta));

      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache); 
      
      ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", pName->tname, tbMeta->tableType, dbFName);
      
      res.pRes = pTableMeta;
      taosArrayPush(ctx->pResList, &res);
      
      continue;
    }
    
    int32_t metaSize = sizeof(SCTableMeta);
    pTableMeta = taosMemoryCalloc(1, metaSize);
    if (NULL == pTableMeta) {
      ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);    
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    memcpy(pTableMeta, tbMeta, metaSize);
    
    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache); 
    
    ctgDebug("Got ctb %s meta from cache, will continue to get its stb meta, type:%d, dbFName:%s", 
             pName->tname, nctx.tbInfo.tbType, dbFName);
    
    char* stName = taosHashAcquire(dbCache->stbCache, &pTableMeta->suid, sizeof(pTableMeta->suid));
    if (NULL == stName) {
      ctgDebug("stb 0x%" PRIx64 " not in cache, dbFName:%s", pTableMeta->suid, dbFName);
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);

      taosMemoryFreeClear(pTableMeta);
      continue;
    }

    pCache = taosHashAcquire(dbCache->tbCache, stName, strlen(stName));
    if (NULL == pCache) {
      ctgDebug("stb 0x%" PRIx64 " name %s not in cache, dbFName:%s", pTableMeta->suid, stName, dbFName);
      taosHashRelease(dbCache->stbCache, stName);
      
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);

      taosMemoryFreeClear(pTableMeta);      
      continue;
    }

    taosHashRelease(dbCache->stbCache, stName);

    CTG_LOCK(CTG_READ, &pCache->metaLock);
    if (NULL == pCache->pMeta) {
      ctgDebug("stb 0x%" PRIx64 " meta not in cache, dbFName:%s", pTableMeta->suid, dbFName);
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache); 
      
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);

      taosMemoryFreeClear(pTableMeta);

      continue;
    }
    
    STableMeta* stbMeta = pCache->pMeta;
    if (stbMeta->suid != nctx.tbInfo.suid) {    
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache); 
      
      ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid 0x%"PRIx64 , stbMeta->suid, nctx.tbInfo.suid);
      
      ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag);
      taosArraySetSize(ctx->pResList, taosArrayGetSize(ctx->pResList) + 1);

      taosMemoryFreeClear(pTableMeta);

      continue;
    }
    
    metaSize = CTG_META_SIZE(stbMeta);
    pTableMeta = taosMemoryRealloc(pTableMeta, metaSize);
    if (NULL == pTableMeta) {    
      ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    memcpy(&pTableMeta->sversion, &stbMeta->sversion, metaSize - sizeof(SCTableMeta));
    
    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache); 
    
    res.pRes = pTableMeta;
    taosArrayPush(ctx->pResList, &res);

    lastSuid = pTableMeta->suid;
    lastTableMeta = pTableMeta;
  }

  ctgReleaseDBCache(pCtg, dbCache);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgRemoveTbMetaFromCache(SCatalog* pCtg, SName* pTableName, bool syncReq) {
  int32_t       code = 0;
  STableMeta*   tblMeta = NULL;
  SCtgTbMetaCtx tbCtx = {0};
  tbCtx.flag = CTG_FLAG_UNKNOWN_STB;
  tbCtx.pName = pTableName;

  CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &tbCtx, &tblMeta));

  if (NULL == tblMeta) {
    ctgDebug("table already not in cache, db:%s, tblName:%s", pTableName->dbname, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  if (TSDB_SUPER_TABLE == tblMeta->tableType) {
    CTG_ERR_JRET(ctgDropStbMetaEnqueue(pCtg, dbFName, tbCtx.tbInfo.dbId, pTableName->tname, tblMeta->suid, syncReq));
  } else {
    CTG_ERR_JRET(ctgDropTbMetaEnqueue(pCtg, dbFName, tbCtx.tbInfo.dbId, pTableName->tname, syncReq));
  }

_return:

  taosMemoryFreeClear(tblMeta);

  CTG_RET(code);
}

int32_t ctgGetTbHashVgroupFromCache(SCatalog *pCtg, const SName *pTableName, SVgroupInfo **pVgroup) {
  if (IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, dbFName);

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));

  if (NULL == dbCache) {
    *pVgroup = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pVgroup = taosMemoryCalloc(1, sizeof(SVgroupInfo));
  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgCache.vgInfo, pTableName, *pVgroup));

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  if (code) {
    taosMemoryFreeClear(*pVgroup);
  }

  CTG_RET(code);
}


