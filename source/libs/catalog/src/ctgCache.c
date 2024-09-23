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

#include "catalogInt.h"
#include "query.h"
#include "systable.h"
#include "tname.h"
#include "trpc.h"

SCtgOperation gCtgCacheOperation[CTG_OP_MAX] = {{CTG_OP_UPDATE_VGROUP, "update vgInfo", ctgOpUpdateVgroup},
                                                {CTG_OP_UPDATE_DB_CFG, "update dbCfg", ctgOpUpdateDbCfg},
                                                {CTG_OP_UPDATE_TB_META, "update tbMeta", ctgOpUpdateTbMeta},
                                                {CTG_OP_DROP_DB_CACHE, "drop DB", ctgOpDropDbCache},
                                                {CTG_OP_DROP_DB_VGROUP, "drop DBVgroup", ctgOpDropDbVgroup},
                                                {CTG_OP_DROP_STB_META, "drop stbMeta", ctgOpDropStbMeta},
                                                {CTG_OP_DROP_TB_META, "drop tbMeta", ctgOpDropTbMeta},
                                                {CTG_OP_UPDATE_USER, "update user", ctgOpUpdateUser},
                                                {CTG_OP_UPDATE_VG_EPSET, "update epset", ctgOpUpdateEpset},
                                                {CTG_OP_UPDATE_TB_INDEX, "update tbIndex", ctgOpUpdateTbIndex},
                                                {CTG_OP_DROP_TB_INDEX, "drop tbIndex", ctgOpDropTbIndex},
                                                {CTG_OP_UPDATE_VIEW_META, "update viewMeta", ctgOpUpdateViewMeta},
                                                {CTG_OP_DROP_VIEW_META, "drop viewMeta", ctgOpDropViewMeta},
                                                {CTG_OP_UPDATE_TB_TSMA, "update tbTSMA", ctgOpUpdateTbTSMA},
                                                {CTG_OP_DROP_TB_TSMA, "drop tbTSMA", ctgOpDropTbTSMA},
                                                {CTG_OP_CLEAR_CACHE, "clear cache", ctgOpClearCache},
                                                {CTG_OP_UPDATE_DB_TSMA_VERSION, "update dbTsmaVersion", ctgOpUpdateDbTsmaVersion}};

SCtgCacheItemInfo gCtgStatItem[CTG_CI_MAX_VALUE] = {
    {"Cluster   ", CTG_CI_FLAG_LEVEL_GLOBAL},  //CTG_CI_CLUSTER
    {"Dnode     ", CTG_CI_FLAG_LEVEL_CLUSTER}, //CTG_CI_DNODE,
    {"Qnode     ", CTG_CI_FLAG_LEVEL_CLUSTER}, //CTG_CI_QNODE,
    {"DB        ", CTG_CI_FLAG_LEVEL_CLUSTER}, //CTG_CI_DB,
    {"DbVgroup  ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_DB_VGROUP,
    {"DbCfg     ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_DB_CFG,
    {"DbInfo    ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_DB_INFO,
    {"StbMeta   ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_STABLE_META,
    {"NtbMeta   ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_NTABLE_META,
    {"CtbMeta   ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_CTABLE_META,
    {"SysTblMeta", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_SYSTABLE_META,
    {"OthTblMeta", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_OTHERTABLE_META,
    {"TblSMA    ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_TBL_SMA,
    {"TblCfg    ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_TBL_CFG,
    {"TblTag    ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_TBL_TAG,
    {"IndexInfo ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_INDEX_INFO,
    {"viewMeta  ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_VIEW,
    {"TblTSMA   ", CTG_CI_FLAG_LEVEL_DB},      //CTG_CI_TBL_TSMA
    {"User      ", CTG_CI_FLAG_LEVEL_CLUSTER}, //CTG_CI_USER,
    {"UDF       ", CTG_CI_FLAG_LEVEL_CLUSTER}, //CTG_CI_UDF,
    {"SvrVer    ", CTG_CI_FLAG_LEVEL_CLUSTER}  //CTG_CI_SVR_VER,
};

int32_t ctgRLockVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache, bool *inCache) {
  CTG_LOCK(CTG_READ, &dbCache->vgCache.vgLock);

  if (dbCache->deleted) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock);

    ctgDebug("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);

    *inCache = false;
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == dbCache->vgCache.vgInfo) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock);

    *inCache = false;
    ctgDebug("db vgInfo is empty, dbId:0x%" PRIx64, dbCache->dbId);
    return TSDB_CODE_SUCCESS;
  }

  *inCache = true;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgWLockVgInfo(SCatalog *pCtg, SCtgDBCache *dbCache) {
  CTG_LOCK(CTG_WRITE, &dbCache->vgCache.vgLock);

  if (dbCache->deleted) {
    ctgDebug("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgCache.vgLock);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  return TSDB_CODE_SUCCESS;
}

void ctgRLockDbCfgInfo(SCtgDBCache *dbCache) {  CTG_LOCK(CTG_READ, &dbCache->cfgCache.cfgLock); }
void ctgWLockDbCfgInfo(SCtgDBCache *dbCache) {  CTG_LOCK(CTG_WRITE, &dbCache->cfgCache.cfgLock); }

void ctgRUnlockVgInfo(SCtgDBCache *dbCache) { CTG_UNLOCK(CTG_READ, &dbCache->vgCache.vgLock); }
void ctgWUnlockVgInfo(SCtgDBCache *dbCache) { CTG_UNLOCK(CTG_WRITE, &dbCache->vgCache.vgLock); }

void ctgRUnlockDbCfgInfo(SCtgDBCache *dbCache) { CTG_UNLOCK(CTG_READ, &dbCache->cfgCache.cfgLock); }
void ctgWUnlockDbCfgInfo(SCtgDBCache *dbCache) { CTG_UNLOCK(CTG_WRITE, &dbCache->cfgCache.cfgLock); }

void ctgReleaseDBCache(SCatalog *pCtg, SCtgDBCache *dbCache) {
  CTG_UNLOCK(CTG_READ, &dbCache->dbLock);
  taosHashRelease(pCtg->dbCache, dbCache);
}

int32_t ctgAcquireDBCacheImpl(SCatalog *pCtg, const char *dbFName, SCtgDBCache **pCache, bool acquire) {
  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
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
    CTG_CACHE_NHIT_INC(CTG_CI_DB, 1);
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
    CTG_CACHE_NHIT_INC(CTG_CI_DB, 1);
    ctgDebug("db is removing from cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  *pCache = dbCache;
  CTG_CACHE_HIT_INC(CTG_CI_DB, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireDBCache(SCatalog *pCtg, const char *dbFName, SCtgDBCache **pCache) {
  CTG_RET(ctgAcquireDBCacheImpl(pCtg, dbFName, pCache, true));
}

int32_t ctgGetDBCache(SCatalog *pCtg, const char *dbFName, SCtgDBCache **pCache) {
  CTG_RET(ctgAcquireDBCacheImpl(pCtg, dbFName, pCache, false));
}

void ctgReleaseVgInfoToCache(SCatalog *pCtg, SCtgDBCache *dbCache) {
  ctgRUnlockVgInfo(dbCache);
  ctgReleaseDBCache(pCtg, dbCache);
}

void ctgReleaseTbMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, SCtgTbCache *pCache) {
  if (pCache && dbCache) {
    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache);
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

void ctgReleaseViewMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, SCtgViewCache *pCache) {
  if (pCache && dbCache) {
    CTG_UNLOCK(CTG_READ, &pCache->viewLock);
    taosHashRelease(dbCache->viewCache, pCache);
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}


void ctgReleaseTbIndexToCache(SCatalog *pCtg, SCtgDBCache *dbCache, SCtgTbCache *pCache) {
  if (pCache) {
    CTG_UNLOCK(CTG_READ, &pCache->indexLock);
    taosHashRelease(dbCache->tbCache, pCache);
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

void ctgReleaseVgMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, SCtgTbCache *pCache) {
  if (pCache && dbCache) {
    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache);
  }

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

void ctgReleaseTSMAToCache(SCatalog* pCtg, SCtgDBCache* dbCache, SCtgTSMACache* pCache) {
  if (pCache && dbCache) {
    CTG_UNLOCK(CTG_READ, &pCache->tsmaLock);
    taosHashRelease(dbCache->tsmaCache, pCache);
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }
}

int32_t ctgAcquireVgInfoFromCache(SCatalog *pCtg, const char *dbFName, SCtgDBCache **pCache) {
  int32_t code = TSDB_CODE_SUCCESS;
  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }

  bool inCache = false;
  CTG_ERR_JRET(ctgRLockVgInfo(pCtg, dbCache, &inCache));
  if (!inCache) {
    ctgDebug("vgInfo of db %s not in cache", dbFName);
    goto _return;
  }

  *pCache = dbCache;

  CTG_CACHE_HIT_INC(CTG_CI_DB_VGROUP, 1);

  ctgDebug("Got db vgInfo from cache, dbFName:%s", dbFName);

  return TSDB_CODE_SUCCESS;

_return:

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }

  *pCache = NULL;

  CTG_CACHE_NHIT_INC(CTG_CI_DB_VGROUP, 1);

  return code;
}

int32_t ctgAcquireTbMetaFromCache(SCatalog *pCtg, const char *dbFName, const char *tbName, SCtgDBCache **pDb, SCtgTbCache **pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *pCache = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  
  CTG_ERR_JRET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
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

  CTG_META_HIT_INC(pCache->pMeta->tableType);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);

  CTG_META_NHIT_INC();

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireVgMetaFromCache(SCatalog *pCtg, const char *dbFName, const char *tbName, SCtgDBCache **pDb,
                                  SCtgTbCache **pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  bool         vgInCache = false;
  int32_t      code = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    CTG_CACHE_NHIT_INC(CTG_CI_DB_VGROUP, 1);
    goto _return;
  }

  CTG_ERR_JRET(ctgRLockVgInfo(pCtg, dbCache, &vgInCache));
  if (!vgInCache) {
    ctgDebug("vgInfo of db %s not in cache", dbFName);
    CTG_CACHE_NHIT_INC(CTG_CI_DB_VGROUP, 1);
    goto _return;
  }

  *pDb = dbCache;

  CTG_CACHE_HIT_INC(CTG_CI_DB_VGROUP, 1);

  ctgDebug("Got db vgInfo from cache, dbFName:%s", dbFName);

  tbCache = taosHashAcquire(dbCache->tbCache, tbName, strlen(tbName));
  if (NULL == tbCache) {
    ctgDebug("tb %s not in cache, dbFName:%s", tbName, dbFName);
    CTG_META_NHIT_INC();
    goto _return;
  }

  CTG_LOCK(CTG_READ, &tbCache->metaLock);
  if (NULL == tbCache->pMeta) {
    ctgDebug("tb %s meta not in cache, dbFName:%s", tbName, dbFName);
    CTG_META_NHIT_INC();
    goto _return;
  }

  *pTb = tbCache;

  ctgDebug("tb %s meta got in cache, dbFName:%s", tbName, dbFName);

  CTG_META_HIT_INC(tbCache->pMeta->tableType);

  return TSDB_CODE_SUCCESS;

_return:

  if (tbCache) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);
    taosHashRelease(dbCache->tbCache, tbCache);
  }

  if (vgInCache) {
    ctgRUnlockVgInfo(dbCache);
  }

  if (dbCache) {
    ctgReleaseDBCache(pCtg, dbCache);
  }

  *pDb = NULL;
  *pTb = NULL;

  return TSDB_CODE_SUCCESS;
}

/*
int32_t ctgAcquireStbMetaFromCache(SCatalog *pCtg, char *dbFName, uint64_t suid, SCtgDBCache **pDb, SCtgTbCache **pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *pCache = NULL;
  ctgAcquireDBCache(pCtg, dbFName, &dbCache);
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    goto _return;
  }

  char *stName = taosHashAcquire(dbCache->stbCache, &suid, sizeof(suid));
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

  taosHashRelease(dbCache->stbCache, stName);

  CTG_LOCK(CTG_READ, &pCache->metaLock);
  if (NULL == pCache->pMeta) {
    ctgDebug("stb 0x%" PRIx64 " meta not in cache, dbFName:%s", suid, dbFName);
    goto _return;
  }

  *pDb = dbCache;
  *pTb = pCache;

  ctgDebug("stb 0x%" PRIx64 " meta got in cache, dbFName:%s", suid, dbFName);

  CTG_META_HIT_INC(pCache->pMeta->tableType, 1);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);

  CTG_META_NHIT_INC(1);

  *pDb = NULL;
  *pTb = NULL;

  return TSDB_CODE_SUCCESS;
}
*/

int32_t ctgAcquireStbMetaFromCache(SCtgDBCache *dbCache, SCatalog *pCtg, char *dbFName, uint64_t suid,
                                   SCtgTbCache **pTb) {
  SCtgTbCache *pCache = NULL;
  char        *stName = taosHashAcquire(dbCache->stbCache, &suid, sizeof(suid));
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

  taosHashRelease(dbCache->stbCache, stName);

  CTG_LOCK(CTG_READ, &pCache->metaLock);
  if (NULL == pCache->pMeta) {
    ctgDebug("stb 0x%" PRIx64 " meta not in cache, dbFName:%s", suid, dbFName);
    goto _return;
  }

  *pTb = pCache;

  ctgDebug("stb 0x%" PRIx64 " meta got in cache, dbFName:%s", suid, dbFName);

  CTG_META_HIT_INC(pCache->pMeta->tableType);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);

  CTG_META_NHIT_INC();

  *pTb = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAcquireTbIndexFromCache(SCatalog *pCtg, char *dbFName, char *tbName, SCtgDBCache **pDb, SCtgTbCache **pTb) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *pCache = NULL;
  int32_t      code = TSDB_CODE_SUCCESS;
  
  CTG_ERR_JRET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
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

  CTG_CACHE_HIT_INC(CTG_CI_TBL_SMA, 1);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbIndexToCache(pCtg, dbCache, pCache);

  CTG_CACHE_NHIT_INC(CTG_CI_TBL_SMA, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgTbMetaExistInCache(SCatalog *pCtg, const char *dbFName, const char *tbName, int32_t *exist) {
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  
  CTG_ERR_RET(ctgAcquireTbMetaFromCache(pCtg, dbFName, tbName, &dbCache, &tbCache));
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCopyTbMeta(SCatalog *pCtg, SCtgTbMetaCtx *ctx, SCtgDBCache **pDb, SCtgTbCache **pTb, STableMeta **pTableMeta,
                      char *dbFName) {
  SCtgDBCache *dbCache = *pDb;
  SCtgTbCache *tbCache = *pTb;
  STableMeta  *tbMeta = tbCache->pMeta;
  ctx->tbInfo.inCache = true;
  ctx->tbInfo.dbId = dbCache->dbId;
  ctx->tbInfo.suid = tbMeta->suid;
  ctx->tbInfo.tbType = tbMeta->tableType;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    int32_t schemaExtSize = 0;
    int32_t metaSize = CTG_META_SIZE(tbMeta);
    if (tbMeta->schemaExt != NULL) {
      schemaExtSize = tbMeta->tableInfo.numOfColumns * sizeof(SSchemaExt);
    }
    *pTableMeta = taosMemoryCalloc(1, metaSize + schemaExtSize);
    if (NULL == *pTableMeta) {
      CTG_ERR_RET(terrno);
    }

    TAOS_MEMCPY(*pTableMeta, tbMeta, metaSize);
    if (tbMeta->schemaExt != NULL) {
      (*pTableMeta)->schemaExt = (SSchemaExt *)((char *)*pTableMeta + metaSize);
      TAOS_MEMCPY((*pTableMeta)->schemaExt, tbMeta->schemaExt, schemaExtSize);
    } else {
      (*pTableMeta)->schemaExt = NULL;
    }

    ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", ctx->pName->tname, tbMeta->tableType, dbFName);
    return TSDB_CODE_SUCCESS;
  }

  // PROCESS FOR CHILD TABLE

  int32_t metaSize = sizeof(SCTableMeta);
  *pTableMeta = taosMemoryCalloc(1, metaSize);
  if (NULL == *pTableMeta) {
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(*pTableMeta, tbMeta, metaSize);

  // ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);
  taosHashRelease(dbCache->tbCache, tbCache);
  *pTb = NULL;

  ctgDebug("Got ctb %s meta from cache, will continue to get its stb meta, type:%d, dbFName:%s", ctx->pName->tname,
           ctx->tbInfo.tbType, dbFName);

  CTG_ERR_RET(ctgAcquireStbMetaFromCache(dbCache, pCtg, dbFName, ctx->tbInfo.suid, &tbCache));
  if (NULL == tbCache) {
    taosMemoryFreeClear(*pTableMeta);
    *pDb = NULL;
    ctgDebug("stb 0x%" PRIx64 " meta not in cache", ctx->tbInfo.suid);
    return TSDB_CODE_SUCCESS;
  }

  *pTb = tbCache;

  STableMeta *stbMeta = tbCache->pMeta;
  if (stbMeta->suid != ctx->tbInfo.suid) {
    ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid 0x%" PRIx64, stbMeta->suid, ctx->tbInfo.suid);
    taosMemoryFreeClear(*pTableMeta);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  metaSize = CTG_META_SIZE(stbMeta);
  *pTableMeta = taosMemoryRealloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(&(*pTableMeta)->sversion, &stbMeta->sversion, metaSize - sizeof(SCTableMeta));
  (*pTableMeta)->schemaExt =  NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgReadTbMetaFromCache(SCatalog *pCtg, SCtgTbMetaCtx *ctx, STableMeta **pTableMeta) {
  int32_t      code = 0;
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  *pTableMeta = NULL;

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    TAOS_STRCPY(dbFName, ctx->pName->dbname);
  } else {
    (void)tNameGetFullDbName(ctx->pName, dbFName);
  }

  CTG_ERR_JRET(ctgAcquireTbMetaFromCache(pCtg, dbFName, ctx->pName->tname, &dbCache, &tbCache));
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgCopyTbMeta(pCtg, ctx, &dbCache, &tbCache, pTableMeta, dbFName));

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  ctgDebug("Got tb %s meta from cache, dbFName:%s", ctx->pName->tname, dbFName);

  return TSDB_CODE_SUCCESS;

_return:

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  taosMemoryFreeClear(*pTableMeta);
  *pTableMeta = NULL;

  CTG_RET(code);
}

int32_t ctgReadTbVerFromCache(SCatalog *pCtg, SName *pTableName, int32_t *sver, int32_t *tver, int32_t *tbType,
                              uint64_t *suid, char *stbName) {
  *sver = -1;
  *tver = -1;

  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, dbFName);

  CTG_ERR_RET(ctgAcquireTbMetaFromCache(pCtg, dbFName, pTableName->tname, &dbCache, &tbCache));
  if (NULL == tbCache) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta *tbMeta = tbCache->pMeta;
  *tbType = tbMeta->tableType;
  *suid = tbMeta->suid;

  if (*tbType != TSDB_CHILD_TABLE) {
    *sver = tbMeta->sversion;
    *tver = tbMeta->tversion;

    ctgDebug("Got tb %s ver from cache, dbFName:%s, tbType:%d, sver:%d, tver:%d, suid:0x%" PRIx64, pTableName->tname,
             dbFName, *tbType, *sver, *tver, *suid);

    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  // PROCESS FOR CHILD TABLE

  // ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
  if (tbCache) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);
    taosHashRelease(dbCache->tbCache, tbCache);
  }

  ctgDebug("Got ctb %s ver from cache, will continue to get its stb ver, dbFName:%s", pTableName->tname, dbFName);

  CTG_ERR_RET(ctgAcquireStbMetaFromCache(dbCache, pCtg, dbFName, *suid, &tbCache));
  if (NULL == tbCache) {
    // ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgDebug("stb 0x%" PRIx64 " meta not in cache", *suid);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta *stbMeta = tbCache->pMeta;
  if (stbMeta->suid != *suid) {
    ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);
    ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid:0x%" PRIx64, stbMeta->suid, *suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  size_t nameLen = 0;
  char  *name = taosHashGetKey(tbCache, &nameLen);

  TAOS_STRNCPY(stbName, name, nameLen);
  stbName[nameLen] = 0;

  *sver = stbMeta->sversion;
  *tver = stbMeta->tversion;

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  ctgDebug("Got tb %s sver %d tver %d from cache, type:%d, dbFName:%s", pTableName->tname, *sver, *tver, *tbType,
           dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgReadTbTypeFromCache(SCatalog *pCtg, char *dbFName, char *tbName, int32_t *tbType) {
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

int32_t ctgReadTbIndexFromCache(SCatalog *pCtg, SName *pTableName, SArray **pRes) {
  int32_t      code = 0;
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, dbFName);

  *pRes = NULL;

  CTG_ERR_RET(ctgAcquireTbIndexFromCache(pCtg, dbFName, pTableName->tname, &dbCache, &tbCache));
  if (NULL == tbCache) {
    ctgReleaseTbIndexToCache(pCtg, dbCache, tbCache);
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgCloneTableIndex(tbCache->pIndex->pIndex, pRes));

_return:

  ctgReleaseTbIndexToCache(pCtg, dbCache, tbCache);

  CTG_RET(code);
}

int32_t ctgReadDBCfgFromCache(SCatalog *pCtg, const char* dbFName, SDbCfgInfo* pDbCfg) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  
  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    pDbCfg->cfgVersion = -1;
    CTG_CACHE_NHIT_INC(CTG_CI_DB_CFG, 1);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->cfgCache.cfgLock);

  if (dbCache->cfgCache.cfgInfo) {
    SDbCfgInfo *pInfo = NULL;
    CTG_ERR_JRET(ctgCloneDbCfgInfo(dbCache->cfgCache.cfgInfo, &pInfo));
    
    TAOS_MEMCPY(pDbCfg, pInfo, sizeof(*pInfo));
    taosMemoryFree(pInfo);
    CTG_CACHE_HIT_INC(CTG_CI_DB_CFG, 1);
  } else {
    pDbCfg->cfgVersion = -1;
    CTG_CACHE_NHIT_INC(CTG_CI_DB_CFG, 1);
  }

_return:

  if (dbCache) {
    CTG_UNLOCK(CTG_READ, &dbCache->cfgCache.cfgLock);
    ctgReleaseDBCache(pCtg, dbCache);
  }
  
  return code;
}

int32_t ctgGetCachedStbNameFromSuid(SCatalog* pCtg, char* dbFName, uint64_t suid, char **stbName) {
  int32_t code = TSDB_CODE_SUCCESS;
  *stbName = NULL;

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    return TSDB_CODE_SUCCESS;
  }  
  
  char *stb = taosHashAcquire(dbCache->stbCache, &suid, sizeof(suid));
  if (NULL == stb) {
    ctgDebug("stb 0x%" PRIx64 " not in cache, dbFName:%s", suid, dbFName);
    ctgReleaseDBCache(pCtg, dbCache);
    return TSDB_CODE_SUCCESS;
  }

  *stbName = taosStrdup(stb);
  if (NULL == *stbName) {
    code = terrno;
  }

  taosHashRelease(dbCache->stbCache, stb);
  ctgReleaseDBCache(pCtg, dbCache);

  return code;
}


int32_t ctgChkAuthFromCache(SCatalog *pCtg, SUserAuthInfo *pReq, bool tbNotExists, bool *inCache, SCtgAuthRsp *pRes) {
  int32_t code = 0;

  SCtgUserAuth *pUser = (SCtgUserAuth *)taosHashGet(pCtg->userCache, pReq->user, strlen(pReq->user));
  if (NULL == pUser) {
    ctgDebug("user not in cache, user:%s", pReq->user);
    goto _return;
  }

  *inCache = true;

  ctgDebug("Got user from cache, user:%s", pReq->user);
  CTG_CACHE_HIT_INC(CTG_CI_USER, 1);

  SCtgAuthReq req = {0};
  req.pRawReq = pReq;
  req.onlyCache = true;
  req.tbNotExists = tbNotExists;

  CTG_LOCK(CTG_READ, &pUser->lock);
  TAOS_MEMCPY(&req.authInfo, &pUser->userAuth, sizeof(pUser->userAuth));
  code = ctgChkSetAuthRes(pCtg, &req, pRes);
  CTG_UNLOCK(CTG_READ, &pUser->lock);
  CTG_ERR_JRET(code);

  if (pRes->metaNotExists) {
    goto _return;
  }

  CTG_RET(code);

_return:

  *inCache = false;
  CTG_CACHE_NHIT_INC(CTG_CI_USER, 1);
  ctgDebug("Get user from cache failed, user:%s, metaNotExists:%d, code:%d", pReq->user, pRes->metaNotExists, code);

  return code;
}

void ctgDequeue(SCtgCacheOperation **op) {
  SCtgQNode *orig = gCtgMgmt.queue.head;

  SCtgQNode *node = gCtgMgmt.queue.head->next;
  gCtgMgmt.queue.head = gCtgMgmt.queue.head->next;

  CTG_QUEUE_DEC();

  taosMemoryFreeClear(orig);

  *op = node->op;
}

int32_t ctgEnqueue(SCatalog *pCtg, SCtgCacheOperation *operation) {
  int32_t code = TSDB_CODE_SUCCESS;
  SCtgQNode *node = taosMemoryCalloc(1, sizeof(SCtgQNode));
  if (NULL == node) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    taosMemoryFree(operation->data);
    taosMemoryFree(operation);
    CTG_RET(terrno);
  }

  node->op = operation;

  bool  syncOp = operation->syncOp;
  char *opName = gCtgCacheOperation[operation->opId].name;
  if (operation->syncOp) {
    code = tsem_init(&operation->rspSem, 0, 0);
    if (TSDB_CODE_SUCCESS != code) {
      qError("tsem_init failed, code:%x", code);
      ctgFreeQNode(node);
      CTG_RET(code);
    }
  }


  CTG_LOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);

  if (gCtgMgmt.queue.stopQueue) {
    ctgFreeQNode(node);
    CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);
    CTG_RET(TSDB_CODE_CTG_EXIT);
  }

  gCtgMgmt.queue.tail->next = node;
  gCtgMgmt.queue.tail = node;

  gCtgMgmt.queue.stopQueue = operation->stopQueue;

  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.queue.qlock);

  ctgDebug("%sync action [%s] added into queue", syncOp ? "S": "As", opName);

  CTG_QUEUE_INC();
  CTG_STAT_RT_INC(numOfOpEnqueue, 1);

  code = tsem_post(&gCtgMgmt.queue.reqSem);
  if (TSDB_CODE_SUCCESS != code) {
    qError("tsem_post failed, code:%x", code);
    CTG_RET(code);
  }

  if (syncOp) {
    if (!operation->unLocked) {
      CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock);
    }
    code = tsem_wait(&operation->rspSem);
    if (!operation->unLocked) {
      CTG_LOCK(CTG_READ, &gCtgMgmt.lock);
    }
    taosMemoryFree(operation);
  }

  return code;
}

int32_t ctgDropDbCacheEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }
  
  op->opId = CTG_OP_DROP_DB_CACHE;
  op->syncOp = true;

  SCtgDropDBMsg *msg = taosMemoryMalloc(sizeof(SCtgDropDBMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropDBMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->dbId = dbId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgDropDbVgroupEnqueue(SCatalog *pCtg, const char *dbFName, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_DB_VGROUP;
  op->syncOp = syncOp;

  SCtgDropDbVgroupMsg *msg = taosMemoryMalloc(sizeof(SCtgDropDbVgroupMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropDbVgroupMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgDropStbMetaEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId, const char *stbName, uint64_t suid,
                              bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_STB_META;
  op->syncOp = syncOp;

  SCtgDropStbMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgDropStbMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropStbMetaMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  tstrncpy(msg->stbName, stbName, sizeof(msg->stbName));
  msg->dbId = dbId;
  msg->suid = suid;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgDropTbMetaEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId, const char *tbName, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_TB_META;
  op->syncOp = syncOp;

  SCtgDropTblMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgDropTblMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTblMetaMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  tstrncpy(msg->tbName, tbName, sizeof(msg->tbName));
  msg->dbId = dbId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgUpdateVgroupEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId, SDBVgInfo *dbInfo, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_VGROUP;
  op->syncOp = syncOp;

  SCtgUpdateVgMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateVgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateVgMsg));
    taosMemoryFree(op);
    freeVgInfo(dbInfo);
    CTG_ERR_RET(terrno);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  code = ctgMakeVgArray(dbInfo);
  if (code) {
    taosMemoryFree(op);
    taosMemoryFree(msg);
    freeVgInfo(dbInfo);
    CTG_ERR_RET(code);
  }

  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->pCtg = pCtg;
  msg->dbId = dbId;
  msg->dbInfo = dbInfo;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  freeVgInfo(dbInfo);
  CTG_RET(code);
}


int32_t ctgUpdateDbCfgEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId, SDbCfgInfo *cfgInfo, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_DB_CFG;
  op->syncOp = syncOp;

  SCtgUpdateDbCfgMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateDbCfgMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateDbCfgMsg));
    taosMemoryFree(op);
    freeDbCfgInfo(cfgInfo);
    CTG_ERR_RET(terrno);
  }

  char *p = strchr(dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    dbFName = p + 1;
  }

  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->pCtg = pCtg;
  msg->dbId = dbId;
  msg->cfgInfo = cfgInfo;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  freeDbCfgInfo(cfgInfo);
  CTG_RET(code);
}


int32_t ctgUpdateTbMetaEnqueue(SCatalog *pCtg, STableMetaOutput *output, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_TB_META;
  op->syncOp = syncOp;

  SCtgUpdateTbMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbMetaMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  char *p = strchr(output->dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    int32_t len = strlen(p + 1);
    TAOS_MEMMOVE(output->dbFName, p + 1, len >= TSDB_DB_FNAME_LEN ? TSDB_DB_FNAME_LEN - 1 : len);
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

  CTG_RET(code);
}

int32_t ctgUpdateVgEpsetEnqueue(SCatalog *pCtg, char *dbFName, int32_t vgId, SEpSet *pEpSet) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_VG_EPSET;

  SCtgUpdateEpsetMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateEpsetMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateEpsetMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  msg->vgId = vgId;
  msg->epSet = *pEpSet;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgUpdateUserEnqueue(SCatalog *pCtg, SGetUserAuthRsp *pAuth, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_USER;
  op->syncOp = syncOp;

  SCtgUpdateUserMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateUserMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateUserMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  msg->pCtg = pCtg;
  msg->userAuth = *pAuth;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  tFreeSGetUserAuthRsp(pAuth);

  CTG_RET(code);
}

int32_t ctgUpdateTbIndexEnqueue(SCatalog *pCtg, STableIndex **pIndex, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_TB_INDEX;
  op->syncOp = syncOp;

  SCtgUpdateTbIndexMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbIndexMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbIndexMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
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

  CTG_RET(code);
}

int32_t ctgDropTbIndexEnqueue(SCatalog *pCtg, SName *pName, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_TB_INDEX;
  op->syncOp = syncOp;

  SCtgDropTbIndexMsg *msg = taosMemoryMalloc(sizeof(SCtgDropTbIndexMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTbIndexMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  (void)tNameGetFullDbName(pName, msg->dbFName);
  TAOS_STRCPY(msg->tbName, pName->tname);

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgClearCacheEnqueue(SCatalog *pCtg, bool clearMeta, bool freeCtg, bool stopQueue, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }
  
  op->opId = CTG_OP_CLEAR_CACHE;
  op->syncOp = syncOp;
  op->stopQueue = stopQueue;
  op->unLocked = true;

  SCtgClearCacheMsg *msg = taosMemoryMalloc(sizeof(SCtgClearCacheMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgClearCacheMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  msg->clearMeta = clearMeta;
  msg->freeCtg = freeCtg;
  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgUpdateViewMetaEnqueue(SCatalog *pCtg, SViewMetaRsp *pRsp, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_VIEW_META;
  op->syncOp = syncOp;

  SCtgUpdateViewMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateViewMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateViewMetaMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  char *p = strchr(pRsp->dbFName, '.');
  if (p && IS_SYS_DBNAME(p + 1)) {
    int32_t len = strlen(p + 1);
    TAOS_MEMMOVE(pRsp->dbFName, p + 1, len >= TSDB_DB_FNAME_LEN ? TSDB_DB_FNAME_LEN - 1 : len);
  }

  msg->pCtg = pCtg;
  msg->pRsp = pRsp;

  op->data = msg;

  CTG_ERR_RET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;


_return:

  if (pRsp) {
    tFreeSViewMetaRsp(pRsp);
    taosMemoryFree(pRsp);
  }

  CTG_RET(code);  
}

int32_t ctgDropViewMetaEnqueue(SCatalog *pCtg, const char *dbFName, uint64_t dbId, const char *viewName, uint64_t viewId, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_VIEW_META;
  op->syncOp = syncOp;

  SCtgDropViewMetaMsg *msg = taosMemoryMalloc(sizeof(SCtgDropViewMetaMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropViewMetaMsg));
    taosMemoryFree(op);
    CTG_ERR_RET(terrno);
  }

  msg->pCtg = pCtg;
  tstrncpy(msg->dbFName, dbFName, sizeof(msg->dbFName));
  tstrncpy(msg->viewName, viewName, sizeof(msg->viewName));
  msg->dbId = dbId;
  msg->viewId = viewId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t ctgUpdateTbTSMAEnqueue(SCatalog *pCtg, STSMACache **pTsma, int32_t tsmaVersion, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_TB_TSMA;
  op->syncOp = syncOp;

  SCtgUpdateTbTSMAMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbTSMAMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbTSMAMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  msg->pCtg = pCtg;
  msg->pTsma = *pTsma;
  msg->dbTsmaVersion = tsmaVersion;
  msg->dbId = (*pTsma)->dbId;

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  *pTsma = NULL;
  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}

int32_t  ctgDropTbTSMAEnqueue(SCatalog* pCtg, const STSMACache* pTsma, bool syncOp) {
  int32_t code = 0;
  SCtgCacheOperation* op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_DROP_TB_TSMA;
  op->syncOp = syncOp;

  SCtgDropTbTSMAMsg* msg = taosMemoryCalloc(1, sizeof(SCtgDropTbTSMAMsg));
  if (!msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTbTSMAMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  msg->pCtg = pCtg;
  msg->dbId = pTsma->dbId;
  msg->tbId = pTsma->suid;
  msg->tsmaId = pTsma->tsmaId;
  tstrncpy(msg->dbFName, pTsma->dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(msg->tbName, pTsma->tb, TSDB_TABLE_NAME_LEN);
  tstrncpy(msg->tsmaName, pTsma->name, TSDB_TABLE_NAME_LEN);

  op->data = msg;
  CTG_ERR_JRET(ctgEnqueue(pCtg, op));
  
  return TSDB_CODE_SUCCESS;
  
_return:

  CTG_RET(code);
}


static int32_t createDropAllTbTsmaCtgCacheOp(SCatalog* pCtg, const STSMACache* pCache, bool syncOp, SCtgCacheOperation** ppOp) {
  SCtgCacheOperation* pOp = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == pOp) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  SCtgDropTbTSMAMsg* pMsg = taosMemoryCalloc(1, sizeof(SCtgDropTbTSMAMsg));
  if (NULL == pMsg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgDropTbTSMAMsg));
    taosMemoryFree(pOp);
    CTG_ERR_RET(terrno);
  }
  
  pOp->opId = CTG_OP_DROP_TB_TSMA;
  pOp->syncOp = syncOp;
  pMsg->pCtg = pCtg;
  pMsg->dbId = pCache->dbId;
  pMsg->tbId = pCache->suid;
  pMsg->tsmaId = pCache->tsmaId;
  pMsg->dropAllForTb = true;
  tstrncpy(pMsg->tsmaName, pCache->name, TSDB_TABLE_NAME_LEN);
  tstrncpy(pMsg->dbFName, pCache->dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(pMsg->tbName, pCache->tb, TSDB_TABLE_NAME_LEN);
  pOp->data = pMsg;

  *ppOp = pOp;
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgDropTSMAForTbEnqueue(SCatalog *pCtg, SName *pName, bool syncOp) {
  ctgDebug("drop tsma meta for tb: %s.%s", pName->dbname, pName->tname);
  
  int32_t             code = 0;
  SCtgDBCache        *pDbCache = NULL;
  SCtgCacheOperation *pOp = NULL;
  char                dbFName[TSDB_DB_FNAME_LEN];
  SCtgTSMACache      *pCtgCache = NULL;
  (void)tNameGetFullDbName(pName, dbFName);
  
  CTG_ERR_JRET(ctgGetDBCache(pCtg, dbFName, &pDbCache));
  if (NULL == pDbCache || !pDbCache->tsmaCache) {
    goto _return;
  }

  pCtgCache = taosHashAcquire(pDbCache->tsmaCache, pName->tname, strlen(pName->tname));
  if (!pCtgCache) {
    goto _return;
  }

  CTG_LOCK(CTG_READ, &pCtgCache->tsmaLock);
  if (!pCtgCache->pTsmas || pCtgCache->pTsmas->size == 0) {
    CTG_UNLOCK(CTG_READ, &pCtgCache->tsmaLock);
    goto _return;
  }
  
  STSMACache *pCache = taosArrayGetP(pCtgCache->pTsmas, 0);
  if (NULL == pCache) {
    ctgError("fail to get the 0th STSMACache, total:%d", (int32_t)pCtgCache->pTsmas->size);
    code = TSDB_CODE_CTG_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createDropAllTbTsmaCtgCacheOp(pCtg, pCache, syncOp, &pOp);
  }
  CTG_UNLOCK(CTG_READ, &pCtgCache->tsmaLock);

  CTG_ERR_JRET(code);
  
  CTG_ERR_JRET(ctgEnqueue(pCtg, pOp));
  taosHashRelease(pDbCache->tsmaCache, pCtgCache);
  
  return TSDB_CODE_SUCCESS;

_return:

  if (pCtgCache) {
    taosHashRelease(pDbCache->tsmaCache, pCtgCache);
  }
  if (pOp) {
    taosMemoryFree(pOp->data);
    taosMemoryFree(pOp);
  }
  
  CTG_RET(code);
}

int32_t ctgUpdateDbTsmaVersionEnqueue(SCatalog* pCtg, int32_t tsmaVersion, const char* dbFName, int64_t dbId, bool syncOp) {
  int32_t             code = 0;
  SCtgCacheOperation *op = taosMemoryCalloc(1, sizeof(SCtgCacheOperation));
  if (NULL == op) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgCacheOperation));
    CTG_ERR_RET(terrno);
  }

  op->opId = CTG_OP_UPDATE_DB_TSMA_VERSION;
  op->syncOp = syncOp;

  SCtgUpdateTbTSMAMsg *msg = taosMemoryMalloc(sizeof(SCtgUpdateTbTSMAMsg));
  if (NULL == msg) {
    ctgError("malloc %d failed", (int32_t)sizeof(SCtgUpdateTbTSMAMsg));
    taosMemoryFree(op);
    CTG_ERR_JRET(terrno);
  }

  msg->pCtg = pCtg;
  msg->pTsma = NULL;
  msg->dbTsmaVersion = tsmaVersion;
  msg->dbId = dbId;
  memcpy(msg->dbFName, dbFName, TSDB_DB_FNAME_LEN);

  op->data = msg;

  CTG_ERR_JRET(ctgEnqueue(pCtg, op));

  return TSDB_CODE_SUCCESS;

_return:

  CTG_RET(code);
}


int32_t ctgAddNewDBCache(SCatalog *pCtg, const char *dbFName, uint64_t dbId) {
  int32_t code = 0;

  SCtgDBCache newDBCache = {0};
  newDBCache.dbId = dbId;

  newDBCache.tbCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                    true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.tbCache) {
    ctgError("taosHashInit %d tbCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_RET(terrno);
  }

  newDBCache.stbCache = taosHashInit(gCtgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
                                     true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.stbCache) {
    ctgError("taosHashInit %d stbCache failed", gCtgMgmt.cfg.maxTblCacheNum);
    CTG_ERR_JRET(terrno);
  }

  newDBCache.viewCache = taosHashInit(gCtgMgmt.cfg.maxViewCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                    true, HASH_ENTRY_LOCK);
  if (NULL == newDBCache.viewCache) {
    ctgError("taosHashInit %d viewCache failed", gCtgMgmt.cfg.maxViewCacheNum);
    CTG_ERR_RET(terrno);
  }

  newDBCache.tsmaCache = taosHashInit(gCtgMgmt.cfg.maxTSMACacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                      true, HASH_ENTRY_LOCK);
  if (!newDBCache.tsmaCache) {
    ctgError("taosHashInit %d tsmaCache failed", gCtgMgmt.cfg.maxTSMACacheNum);
    CTG_ERR_RET(terrno);
  }

  code = taosHashPut(pCtg->dbCache, dbFName, strlen(dbFName), &newDBCache, sizeof(SCtgDBCache));
  if (code) {
    if (HASH_NODE_EXIST(code)) {
      ctgDebug("db already in cache, dbFName:%s", dbFName);
      goto _return;
    }

    ctgError("taosHashPut db to cache failed, dbFName:%s", dbFName);
    CTG_ERR_JRET(terrno);
  }

  CTG_CACHE_NUM_INC(CTG_CI_DB, 1);

  SDbCacheInfo dbCacheInfo = {.dbId = newDBCache.dbId, .vgVersion = -1, .stateTs = 0, .cfgVersion = -1, .tsmaVersion = -1};
  tstrncpy(dbCacheInfo.dbFName, dbFName, sizeof(dbCacheInfo.dbFName));

  ctgDebug("db added to cache, dbFName:%s, dbId:0x%" PRIx64, dbFName, dbId);

  if (!IS_SYS_DBNAME(dbFName)) {
    CTG_ERR_RET(ctgMetaRentAdd(&pCtg->dbRent, &dbCacheInfo, dbId, sizeof(SDbCacheInfo)));

    ctgDebug("db added to rent, dbFName:%s, vgVersion:%d, dbId:0x%" PRIx64, dbFName, dbCacheInfo.vgVersion, dbId);
  }

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeDbCache(&newDBCache);

  CTG_RET(code);
}

int32_t ctgRemoveDBFromCache(SCatalog *pCtg, SCtgDBCache *dbCache, const char *dbFName) {
  uint64_t dbId = dbCache->dbId;

  ctgInfo("start to remove db from cache, dbFName:%s, dbId:0x%" PRIx64, dbFName, dbCache->dbId);

  CTG_LOCK(CTG_WRITE, &dbCache->dbLock);

  atomic_store_8(&dbCache->deleted, 1);
  ctgRemoveStbRent(pCtg, dbCache);
  ctgRemoveViewRent(pCtg, dbCache);
  ctgRemoveTSMARent(pCtg, dbCache);
  ctgFreeDbCache(dbCache);

  CTG_UNLOCK(CTG_WRITE, &dbCache->dbLock);

  CTG_ERR_RET(ctgMetaRentRemove(&pCtg->dbRent, dbId, ctgDbCacheInfoSortCompare, ctgDbCacheInfoSearchCompare));
  ctgDebug("db removed from rent, dbFName:%s, dbId:0x%" PRIx64, dbFName, dbId);

  if (taosHashRemove(pCtg->dbCache, dbFName, strlen(dbFName))) {
    ctgInfo("taosHashRemove from dbCache failed, may be removed, dbFName:%s", dbFName);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  CTG_CACHE_NUM_DEC(CTG_CI_DB, 1);
  ctgInfo("db removed from cache, dbFName:%s, dbId:0x%" PRIx64, dbFName, dbId);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetAddDBCache(SCatalog *pCtg, const char *dbFName, uint64_t dbId, SCtgDBCache **pCache) {
  int32_t      code = 0;
  SCtgDBCache *dbCache = NULL;
  CTG_ERR_RET(ctgGetDBCache(pCtg, dbFName, &dbCache));

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

  CTG_ERR_RET(ctgGetDBCache(pCtg, dbFName, &dbCache));

  *pCache = dbCache;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgWriteTbMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, uint64_t dbId, char *tbName,
                              STableMeta *meta) {
  if (NULL == dbCache->tbCache || NULL == dbCache->stbCache) {
    taosMemoryFree(meta);
    ctgError("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  bool         isStb = meta->tableType == TSDB_SUPER_TABLE;
  SCtgTbCache *pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
  STableMeta  *orig = (pCache ? pCache->pMeta : NULL);
  int8_t       origType = 0;

  if (orig) {
    origType = orig->tableType;

    if (origType == meta->tableType && orig->uid == meta->uid &&
        (origType == TSDB_CHILD_TABLE || (orig->sversion >= meta->sversion && orig->tversion >= meta->tversion))) {
      taosMemoryFree(meta);
      ctgDebug("ignore table %s meta update", tbName);
      return TSDB_CODE_SUCCESS;
    }

    if (origType == TSDB_SUPER_TABLE) {
      char *stbName = taosHashGet(dbCache->stbCache, &orig->suid, sizeof(orig->suid));
      if (stbName) {
        uint64_t metaSize = strlen(stbName) + 1 + sizeof(orig->suid);
        if (taosHashRemove(dbCache->stbCache, &orig->suid, sizeof(orig->suid))) {
          ctgError("stb not exist in stbCache, dbFName:%s, stb:%s, suid:0x%" PRIx64, dbFName, tbName, orig->suid);
        } else {
          ctgDebug("stb removed from stbCache, dbFName:%s, stb:%s, suid:0x%" PRIx64, dbFName, tbName, orig->suid);
          (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, metaSize);
        }
      }
    }
  }

  if (NULL == pCache) {
    SCtgTbCache cache = {0};
    cache.pMeta = meta;
    if (taosHashPut(dbCache->tbCache, tbName, strlen(tbName), &cache, sizeof(SCtgTbCache)) != 0) {
      ctgError("taosHashPut new tbCache failed, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
      taosMemoryFree(meta);
      CTG_ERR_RET(terrno);
    }

    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, strlen(tbName) + sizeof(SCtgTbCache) + ctgGetTbMetaCacheSize(meta));

    pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
    if (NULL == pCache) {
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }
  } else {
    CTG_LOCK(CTG_WRITE, &pCache->metaLock);
    if (orig) {
      CTG_META_NUM_DEC(origType);
    }

    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, ctgGetTbMetaCacheSize(meta) - ctgGetTbMetaCacheSize(pCache->pMeta));

    taosMemoryFree(pCache->pMeta);
    pCache->pMeta = meta;
    
    CTG_UNLOCK(CTG_WRITE, &pCache->metaLock);
  }

  CTG_META_NUM_INC(pCache->pMeta->tableType);

  ctgDebug("tbmeta updated to cache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
  ctgdShowTableMeta(pCtg, tbName, meta);

  if (!isStb) {
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashPut(dbCache->stbCache, &meta->suid, sizeof(meta->suid), tbName, strlen(tbName) + 1) != 0) {
    ctgError("taosHashPut to stable cache failed, suid:0x%" PRIx64, meta->suid);
    CTG_ERR_RET(terrno);
  }

  (void)atomic_add_fetch_64(&dbCache->dbCacheSize, sizeof(meta->suid) + strlen(tbName) + 1);

  ctgDebug("stb 0x%" PRIx64 " updated to cache, dbFName:%s, tbName:%s, tbType:%d", meta->suid, dbFName, tbName,
           meta->tableType);

  CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbId, meta->suid, pCache));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgWriteTbIndexToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, char *tbName, STableIndex **index) {
  if (NULL == dbCache->tbCache) {
    ctgFreeSTableIndex(*index);
    taosMemoryFreeClear(*index);
    ctgError("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  STableIndex *pIndex = *index;
  uint64_t     suid = pIndex->suid;
  SCtgTbCache *pCache = taosHashGet(dbCache->tbCache, tbName, strlen(tbName));
  if (NULL == pCache) {
    SCtgTbCache cache = {0};
    cache.pIndex = pIndex;

    if (taosHashPut(dbCache->tbCache, tbName, strlen(tbName), &cache, sizeof(cache)) != 0) {
      ctgFreeSTableIndex(*index);
      taosMemoryFreeClear(*index);
      ctgError("taosHashPut new tbCache failed, tbName:%s", tbName);
      CTG_ERR_RET(terrno);
    }

    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, strlen(tbName) + sizeof(SCtgTbCache) + ctgGetTbIndexCacheSize(pIndex));

    CTG_DB_NUM_INC(CTG_CI_TBL_SMA);

    *index = NULL;
    ctgDebug("table %s index updated to cache, ver:%d, num:%d", tbName, pIndex->version,
             (int32_t)taosArrayGetSize(pIndex->pIndex));

    if (suid) {
      CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbCache->dbId, pIndex->suid, &cache));
    }

    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_WRITE, &pCache->indexLock);

  if (pCache->pIndex) {
    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, ctgGetTbIndexCacheSize(pCache->pIndex));
    if (0 == suid) {
      suid = pCache->pIndex->suid;
    }
    taosArrayDestroyEx(pCache->pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pCache->pIndex);
  }

  pCache->pIndex = pIndex;
  CTG_UNLOCK(CTG_WRITE, &pCache->indexLock);

  (void)atomic_add_fetch_64(&dbCache->dbCacheSize, ctgGetTbIndexCacheSize(pIndex));

  *index = NULL;

  ctgDebug("table %s index updated to cache, ver:%d, num:%d", tbName, pIndex->version,
           (int32_t)taosArrayGetSize(pIndex->pIndex));

  if (suid) {
    CTG_ERR_RET(ctgUpdateRentStbVersion(pCtg, dbFName, tbName, dbCache->dbId, suid, pCache));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgWriteViewMetaToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, char *viewName, SViewMeta *pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == dbCache->viewCache) {
    ctgWarn("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_DB_DROPPED);
  }

  SCtgViewCache *pCache = taosHashGet(dbCache->viewCache, viewName, strlen(viewName));
  if (NULL == pCache) {
    SCtgViewCache cache = {0};
    cache.pMeta = pMeta;

    if (taosHashPut(dbCache->viewCache, viewName, strlen(viewName), &cache, sizeof(cache)) != 0) {
      ctgError("taosHashPut new tbCache failed, viewName:%s", viewName);
      CTG_ERR_JRET(terrno);
    }

    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, strlen(viewName) + sizeof(SCtgViewCache) + ctgGetViewMetaCacheSize(pMeta));

    CTG_DB_NUM_INC(CTG_CI_VIEW);

    ctgDebug("new view meta updated to cache, view:%s, id:%" PRIu64 ", ver:%d, effectiveUser:%s, querySQL:%s", 
      viewName, pMeta->viewId, pMeta->version, pMeta->user, pMeta->querySql);

    CTG_ERR_RET(ctgUpdateRentViewVersion(pCtg, dbFName, viewName, dbCache->dbId, pMeta->viewId, &cache));

    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_WRITE, &pCache->viewLock);

  if (pCache->pMeta) {
    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, ctgGetViewMetaCacheSize(pCache->pMeta));
    ctgFreeSViewMeta(pCache->pMeta);
    taosMemoryFree(pCache->pMeta);
  }

  pCache->pMeta = pMeta;
  CTG_UNLOCK(CTG_WRITE, &pCache->viewLock);

  (void)atomic_add_fetch_64(&dbCache->dbCacheSize, ctgGetViewMetaCacheSize(pMeta));

  ctgDebug("view meta updated to cache, view:%s, id:%" PRIu64 ", ver:%d, effectiveUser:%s, querySQL:%s", 
    viewName, pMeta->viewId, pMeta->version, pMeta->user, pMeta->querySql);

  CTG_ERR_RET(ctgUpdateRentViewVersion(pCtg, dbFName, viewName, dbCache->dbId, pMeta->viewId, pCache));

  pMeta = NULL;

_return:

  if (pMeta) {
    ctgFreeSViewMeta(pMeta);
    taosMemoryFree(pMeta);
  }

  CTG_RET(code);
}

int32_t ctgUpdateTbMetaToCache(SCatalog *pCtg, STableMetaOutput *pOut, bool syncReq) {
  STableMetaOutput *pOutput = NULL;
  int32_t           code = 0;

  CTG_ERR_RET(ctgCloneMetaOutput(pOut, &pOutput));
  code = ctgUpdateTbMetaEnqueue(pCtg, pOutput, syncReq);
  pOutput = NULL;
  CTG_ERR_JRET(code);

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeSTableMetaOutput(pOutput);
  CTG_RET(code);
}

int32_t ctgUpdateViewMetaToCache(SCatalog *pCtg, SViewMetaRsp *pRsp, bool syncReq) {
  CTG_RET(ctgUpdateViewMetaEnqueue(pCtg, pRsp, syncReq));
}


void ctgClearAllHandles(void) {
  SCatalog *pCtg = NULL;

  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      ctgClearHandle(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }
}

void ctgFreeAllHandles(void) {
  SCatalog *pCtg = NULL;

  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      ctgFreeHandle(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  taosHashClear(gCtgMgmt.pCluster);
}

int32_t ctgVgInfoIdComp(void const *lp, void const *rp) {
  int32_t     *key = (int32_t *)lp;
  SVgroupInfo *pVg = (SVgroupInfo *)rp;

  if (*key < pVg->vgId) {
    return -1;
  } else if (*key > pVg->vgId) {
    return 1;
  }

  return 0;
}




int32_t ctgWriteTbTSMAToCache(SCatalog *pCtg, SCtgDBCache *dbCache, char *dbFName, char *tbName,
                              STSMACache **ppTsmaCache) {
  if (NULL == dbCache->tsmaCache) {
    ctgError("db is dropping, dbId:0x%" PRIx64, dbCache->dbId);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  STSMACache *pTsmaCache = *ppTsmaCache;
  int32_t     code = TSDB_CODE_SUCCESS;

  SCtgTSMACache* pCache = taosHashGet(dbCache->tsmaCache, tbName, strlen(tbName));
  if (!pCache) {
    SCtgTSMACache cache = {0};
    cache.pTsmas = taosArrayInit(4, sizeof(POINTER_BYTES));
    if (NULL == cache.pTsmas) {
      CTG_ERR_JRET(terrno);
    }
    
    if (NULL == taosArrayPush(cache.pTsmas, &pTsmaCache)) {
      CTG_ERR_JRET(terrno);
    }
    
    if (taosHashPut(dbCache->tsmaCache, tbName, strlen(tbName), &cache, sizeof(cache))) {
      ctgError("taosHashPut new tsmacache for tb: %s.%s failed", dbFName, tbName);
      CTG_ERR_JRET(terrno);
    }
    
    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, strlen(tbName) + sizeof(STSMACache) + ctgGetTbTSMACacheSize(pTsmaCache));
    
    CTG_DB_NUM_INC(CTG_CI_TBL_TSMA);
    ctgDebug("tb %s tsma updated to cache, name: %s", tbName, pTsmaCache->name);
    
    CTG_ERR_JRET(ctgUpdateRentTSMAVersion(pCtg, dbFName, pTsmaCache));
    *ppTsmaCache = NULL;
    
    goto _return;
  }

  CTG_LOCK(CTG_WRITE, &pCache->tsmaLock);

  if (pCache->pTsmas) {
    uint64_t cacheSize = 0;
    for (int32_t i = 0; i < pCache->pTsmas->size; ++i) {
      STableTSMAInfo* pInfo = taosArrayGetP(pCache->pTsmas, i);
      if (NULL == pInfo) {
        ctgError("fail to get the %dth STableTSMAInfo, total:%d", i, (int32_t)pCache->pTsmas->size);
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
      
      if (pInfo->tsmaId == pTsmaCache->tsmaId) {
        ctgDebug("tsma: %s removed from cache, history from %d to %d, reqTs from %" PRId64 " to %" PRId64
                 "rspTs from %" PRId64 " to %" PRId64 " delay from %" PRId64 " to %" PRId64,
                 pInfo->name, pInfo->fillHistoryFinished, pTsmaCache->fillHistoryFinished, pInfo->reqTs,
                 pTsmaCache->reqTs, pInfo->rspTs, pTsmaCache->rspTs, pInfo->delayDuration, pTsmaCache->delayDuration);
                 
        cacheSize = ctgGetTbTSMACacheSize(pInfo);
        taosArrayRemove(pCache->pTsmas, i);
        (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, cacheSize);
        
        tFreeTableTSMAInfo(pInfo);
        taosMemoryFreeClear(pInfo);
        
        break;
      }
    }
  } else {
    pCache->pTsmas = taosArrayInit(4, sizeof(POINTER_BYTES));
    if (!pCache->pTsmas) {
      CTG_UNLOCK(CTG_WRITE, &pCache->tsmaLock);
      CTG_ERR_JRET(terrno);
    }
  }
  
  // push the new cache
  if (NULL == taosArrayPush(pCache->pTsmas, &pTsmaCache)) {
    code = terrno;
  } else {
    *ppTsmaCache = NULL;

    (void)atomic_add_fetch_64(&dbCache->dbCacheSize, ctgGetTbTSMACacheSize(pTsmaCache));
    
    CTG_ERR_RET(ctgUpdateRentTSMAVersion(pCtg, dbFName, pTsmaCache));

    ctgDebug("table %s tsma updated to cache, tsma: %s", tbName, pTsmaCache->name);
  }

  CTG_UNLOCK(CTG_WRITE, &pCache->tsmaLock);
  
_return:

  CTG_RET(code);
}


int32_t ctgOpUpdateVgroup(SCtgCacheOperation *operation) {
  int32_t          code = 0;
  SCtgUpdateVgMsg *msg = operation->data;
  SDBVgInfo       *dbInfo = msg->dbInfo;
  char            *dbFName = msg->dbFName;
  SCatalog        *pCtg = msg->pCtg;

  if (pCtg->stopUpdate || NULL == dbInfo->vgHash) {
    goto _return;
  }

  if (dbInfo->vgVersion < 0 || (taosHashGetSize(dbInfo->vgHash) <= 0 && !IS_SYS_DBNAME(dbFName))) {
    ctgDebug("invalid db vgInfo, dbFName:%s, vgHash:%p, vgVersion:%d, vgHashSize:%d", dbFName, dbInfo->vgHash,
             dbInfo->vgVersion, taosHashGetSize(dbInfo->vgHash));
    CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  bool         newAdded = false;
  SDbCacheInfo dbCacheInfo = {
      .dbId = msg->dbId, .vgVersion = dbInfo->vgVersion, .cfgVersion = -1, .numOfTable = dbInfo->numOfTable, .stateTs = dbInfo->stateTs, .tsmaVersion = -1};

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetAddDBCache(msg->pCtg, dbFName, msg->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:0x%" PRIx64, dbFName, msg->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgVgCache *vgCache = &dbCache->vgCache;
  
  CTG_ERR_JRET(ctgWLockVgInfo(msg->pCtg, dbCache));

  if (vgCache->vgInfo) {
    SDBVgInfo *vgInfo = vgCache->vgInfo;

    if (dbInfo->vgVersion < vgInfo->vgVersion) {
      ctgDebug("db updateVgroup is ignored, dbFName:%s, vgVer:%d, curVer:%d", dbFName, dbInfo->vgVersion,
               vgInfo->vgVersion);
      ctgWUnlockVgInfo(dbCache);

      goto _return;
    }

    if (dbInfo->vgVersion == vgInfo->vgVersion && dbInfo->numOfTable == vgInfo->numOfTable &&
        dbInfo->stateTs == vgInfo->stateTs) {
      ctgDebug("no new db vgroup update info, dbFName:%s, vgVer:%d, numOfTable:%d, stateTs:%" PRId64, dbFName,
               dbInfo->vgVersion, dbInfo->numOfTable, dbInfo->stateTs);
      ctgWUnlockVgInfo(dbCache);

      goto _return;
    }

    uint64_t groupCacheSize = ctgGetDbVgroupCacheSize(vgCache->vgInfo);
    ctgDebug("sub dbGroupCacheSize %" PRIu64 " from db, dbFName:%s", groupCacheSize, dbFName);

    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, groupCacheSize);
    
    freeVgInfo(vgInfo);
    CTG_DB_NUM_RESET(CTG_CI_DB_VGROUP);
  }

  if (dbCache->cfgCache.cfgInfo) {
    dbCacheInfo.cfgVersion = dbCache->cfgCache.cfgInfo->cfgVersion;
    dbCacheInfo.tsmaVersion = dbCache->tsmaVersion;
  }

  vgCache->vgInfo = dbInfo;
  msg->dbInfo = NULL;
  CTG_DB_NUM_SET(CTG_CI_DB_VGROUP);

  ctgDebug("db vgInfo updated, dbFName:%s, vgVer:%d, stateTs:%" PRId64 ", dbId:0x%" PRIx64, dbFName,
           dbCacheInfo.vgVersion, dbCacheInfo.stateTs, dbCacheInfo.dbId);

  ctgWUnlockVgInfo(dbCache);

  uint64_t groupCacheSize = ctgGetDbVgroupCacheSize(vgCache->vgInfo);
  (void)atomic_add_fetch_64(&dbCache->dbCacheSize, groupCacheSize);
  ctgDebug("add dbGroupCacheSize %" PRIu64 " from db, dbFName:%s", groupCacheSize, dbFName);

  dbCache = NULL;

  // if (!IS_SYS_DBNAME(dbFName)) {
  tstrncpy(dbCacheInfo.dbFName, dbFName, sizeof(dbCacheInfo.dbFName));
  CTG_ERR_JRET(ctgMetaRentUpdate(&msg->pCtg->dbRent, &dbCacheInfo, dbCacheInfo.dbId, sizeof(SDbCacheInfo),
                                 ctgDbCacheInfoSortCompare, ctgDbCacheInfoSearchCompare));
  //}

_return:

  freeVgInfo(msg->dbInfo);
  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateDbCfg(SCtgCacheOperation *operation) {
  int32_t          code = 0;
  SCtgUpdateDbCfgMsg *msg = operation->data;
  SDbCfgInfo       *cfgInfo = msg->cfgInfo;
  char            *dbFName = msg->dbFName;
  SCatalog        *pCtg = msg->pCtg;

  if (pCtg->stopUpdate || NULL == cfgInfo) {
    goto _return;
  }

  if (cfgInfo->cfgVersion < 0) {
    ctgDebug("invalid db cfgInfo, dbFName:%s, cfgVersion:%d", dbFName, cfgInfo->cfgVersion);
    CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetAddDBCache(msg->pCtg, dbFName, msg->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:0x%" PRIx64, dbFName, msg->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SDbCacheInfo cacheInfo = {0};
  cacheInfo.dbId = dbCache->dbId;
  tstrncpy(cacheInfo.dbFName, dbFName, sizeof(cacheInfo.dbFName));
  cacheInfo.cfgVersion = cfgInfo->cfgVersion;

  SCtgVgCache *vgCache = &dbCache->vgCache;
  if (vgCache->vgInfo) {
    cacheInfo.vgVersion = vgCache->vgInfo->vgVersion;
    cacheInfo.numOfTable = vgCache->vgInfo->numOfTable;
    cacheInfo.stateTs = vgCache->vgInfo->stateTs;
  } else {
    cacheInfo.vgVersion = -1;
  }
  cacheInfo.tsmaVersion = dbCache->tsmaVersion;

  ctgWLockDbCfgInfo(dbCache);

  freeDbCfgInfo(dbCache->cfgCache.cfgInfo);
  dbCache->cfgCache.cfgInfo = cfgInfo;
  cfgInfo = NULL;
  
  ctgWUnlockDbCfgInfo(dbCache);

  ctgDebug("db cfgInfo updated, dbFName:%s, cfgVer:%d", dbFName, dbCache->cfgCache.cfgInfo->cfgVersion);

  // if (!IS_SYS_DBNAME(dbFName)) {
  CTG_ERR_JRET(ctgMetaRentUpdate(&msg->pCtg->dbRent, &cacheInfo, cacheInfo.dbId, sizeof(SDbCacheInfo),
                                 ctgDbCacheInfoSortCompare, ctgDbCacheInfoSearchCompare));
  //}

_return:

  freeDbCfgInfo(cfgInfo);
  taosMemoryFreeClear(msg);

  CTG_RET(code);
}


int32_t ctgOpDropDbCache(SCtgCacheOperation *operation) {
  int32_t        code = 0;
  SCtgDropDBMsg *msg = operation->data;
  SCatalog      *pCtg = msg->pCtg;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_RET(ctgGetDBCache(msg->pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    goto _return;
  }

  if (msg->dbId && dbCache->dbId != msg->dbId) {
    ctgInfo("dbId already updated, dbFName:%s, dbId:0x%" PRIx64 ", targetId:0x%" PRIx64, msg->dbFName, dbCache->dbId,
            msg->dbId);
    goto _return;
  }

  CTG_ERR_JRET(ctgRemoveDBFromCache(pCtg, dbCache, msg->dbFName));

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpDropDbVgroup(SCtgCacheOperation *operation) {
  int32_t              code = 0;
  SCtgDropDbVgroupMsg *msg = operation->data;
  SCatalog            *pCtg = msg->pCtg;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_RET(ctgGetDBCache(msg->pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    goto _return;
  }

  CTG_ERR_JRET(ctgWLockVgInfo(pCtg, dbCache));

  (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, ctgGetDbVgroupCacheSize(dbCache->vgCache.vgInfo));

  freeVgInfo(dbCache->vgCache.vgInfo);
  dbCache->vgCache.vgInfo = NULL;

  CTG_DB_NUM_RESET(CTG_CI_DB_VGROUP);
  ctgDebug("db vgInfo removed, dbFName:%s", msg->dbFName);

  ctgWUnlockVgInfo(dbCache);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateTbMeta(SCtgCacheOperation *operation) {
  int32_t              code = 0;
  SCtgUpdateTbMetaMsg *msg = operation->data;
  SCatalog            *pCtg = msg->pCtg;
  STableMetaOutput    *pMeta = msg->pMeta;
  SCtgDBCache         *dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

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
    code = ctgWriteTbMetaToCache(pCtg, dbCache, pMeta->dbFName, pMeta->dbId, pMeta->tbName, pMeta->tbMeta);
    pMeta->tbMeta = NULL;
    CTG_ERR_JRET(code);
  }

  if (CTG_IS_META_CTABLE(pMeta->metaType) || CTG_IS_META_BOTH(pMeta->metaType)) {
    SCTableMeta *ctbMeta = taosMemoryMalloc(sizeof(SCTableMeta));
    if (NULL == ctbMeta) {
      CTG_ERR_JRET(terrno);
    }
    TAOS_MEMCPY(ctbMeta, &pMeta->ctbMeta, sizeof(SCTableMeta));
    CTG_ERR_JRET(ctgWriteTbMetaToCache(pCtg, dbCache, pMeta->dbFName, pMeta->dbId, pMeta->ctbName,
                                       (STableMeta *)ctbMeta));
  }

_return:

  taosMemoryFreeClear(pMeta->tbMeta);
  taosMemoryFreeClear(pMeta);

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpDropStbMeta(SCtgCacheOperation *operation) {
  int32_t             code = 0;
  SCtgDropStbMetaMsg *msg = operation->data;
  SCatalog           *pCtg = msg->pCtg;
  int32_t             tblType = 0;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    goto _return;
  }

  if ((0 != msg->dbId) && (dbCache->dbId != msg->dbId)) {
    ctgDebug("dbId already modified, dbFName:%s, current:0x%" PRIx64 ", dbId:0x%" PRIx64 ", stb:%s, suid:0x%" PRIx64,
             msg->dbFName, dbCache->dbId, msg->dbId, msg->stbName, msg->suid);
    goto _return;
  }

  char *stbName = taosHashGet(dbCache->stbCache, &msg->suid, sizeof(msg->suid));
  if (stbName) {
    uint64_t metaSize = strlen(stbName) + 1 + sizeof(msg->suid);
    if (taosHashRemove(dbCache->stbCache, &msg->suid, sizeof(msg->suid))) {
      ctgDebug("stb not exist in stbCache, may be removed, dbFName:%s, stb:%s, suid:0x%" PRIx64, msg->dbFName,
               msg->stbName, msg->suid);
    } else {
      (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, metaSize);
    }
  }
  
  SCtgTbCache *pTbCache = taosHashGet(dbCache->tbCache, msg->stbName, strlen(msg->stbName));
  if (NULL == pTbCache) {
    ctgDebug("stb %s already not in cache", msg->stbName);
    goto _return;
  }

  tblType = pTbCache->pMeta->tableType;
  (void)atomic_sub_fetch_64(&dbCache->dbCacheSize,
                      ctgGetTbMetaCacheSize(pTbCache->pMeta) + ctgGetTbIndexCacheSize(pTbCache->pIndex));
  ctgFreeTbCacheImpl(pTbCache, true);

  if (taosHashRemove(dbCache->tbCache, msg->stbName, strlen(msg->stbName))) {
    ctgError("stb not exist in cache, dbFName:%s, stb:%s, suid:0x%" PRIx64, msg->dbFName, msg->stbName, msg->suid);
  } else {
    CTG_META_NUM_DEC(tblType);
    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, sizeof(*pTbCache) + strlen(msg->stbName));
  }

  ctgInfo("stb removed from cache, dbFName:%s, stbName:%s, suid:0x%" PRIx64, msg->dbFName, msg->stbName, msg->suid);

  CTG_ERR_JRET(ctgMetaRentRemove(&msg->pCtg->stbRent, msg->suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare));

  ctgDebug("stb removed from rent, dbFName:%s, stbName:%s, suid:0x%" PRIx64, msg->dbFName, msg->stbName, msg->suid);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpDropTbMeta(SCtgCacheOperation *operation) {
  int32_t             code = 0;
  SCtgDropTblMetaMsg *msg = operation->data;
  SCatalog           *pCtg = msg->pCtg;
  int32_t             tblType = 0;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    goto _return;
  }

  if ((0 != msg->dbId) && (dbCache->dbId != msg->dbId)) {
    ctgDebug("dbId 0x%" PRIx64 " not match with curId 0x%" PRIx64 ", dbFName:%s, tbName:%s", msg->dbId, dbCache->dbId,
             msg->dbFName, msg->tbName);
    goto _return;
  }

  SCtgTbCache *pTbCache = taosHashGet(dbCache->tbCache, msg->tbName, strlen(msg->tbName));
  if (NULL == pTbCache) {
    ctgDebug("tb %s already not in cache", msg->tbName);
    goto _return;
  }

  tblType = pTbCache->pMeta->tableType;
  (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, ctgGetTbMetaCacheSize(pTbCache->pMeta) +
                                                 ctgGetTbIndexCacheSize(pTbCache->pIndex));
  ctgFreeTbCacheImpl(pTbCache, true);

  if (taosHashRemove(dbCache->tbCache, msg->tbName, strlen(msg->tbName))) {
    ctgError("tb %s not exist in cache, dbFName:%s", msg->tbName, msg->dbFName);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  } else {
    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, sizeof(*pTbCache) + strlen(msg->tbName));
    CTG_META_NUM_DEC(tblType);
  }

  ctgDebug("table %s removed from cache, dbFName:%s", msg->tbName, msg->dbFName);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateUser(SCtgCacheOperation *operation) {
  int32_t            code = 0;
  SCtgUpdateUserMsg *msg = operation->data;
  SCatalog          *pCtg = msg->pCtg;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgUserAuth *pUser = (SCtgUserAuth *)taosHashGet(pCtg->userCache, msg->userAuth.user, strlen(msg->userAuth.user));
  if (NULL == pUser) {
    if (msg->userAuth.dropped == 1) {
      goto _return;
    }

    SCtgUserAuth userAuth = {0};

    TAOS_MEMCPY(&userAuth.userAuth, &msg->userAuth, sizeof(msg->userAuth));
    userAuth.userCacheSize = ctgGetUserCacheSize(&userAuth.userAuth);

    if (taosHashPut(pCtg->userCache, msg->userAuth.user, strlen(msg->userAuth.user), &userAuth, sizeof(userAuth))) {
      ctgError("taosHashPut user %s to cache failed", msg->userAuth.user);
      CTG_ERR_JRET(terrno);
    }

    taosMemoryFreeClear(msg);

    CTG_CACHE_NUM_INC(CTG_CI_USER, 1);

    return TSDB_CODE_SUCCESS;
  } else if (msg->userAuth.dropped == 1) {
    if (ctgRemoveCacheUser(pCtg, pUser, msg->userAuth.user) == 0) {
      CTG_CACHE_NUM_DEC(CTG_CI_USER, 1);
    }
    goto _return;
  }

  CTG_LOCK(CTG_WRITE, &pUser->lock);

  taosHashCleanup(pUser->userAuth.createdDbs);
  taosHashCleanup(pUser->userAuth.readDbs);
  taosHashCleanup(pUser->userAuth.writeDbs);
  taosHashCleanup(pUser->userAuth.readTbs);
  taosHashCleanup(pUser->userAuth.writeTbs);
  taosHashCleanup(pUser->userAuth.alterTbs);
  taosHashCleanup(pUser->userAuth.readViews);
  taosHashCleanup(pUser->userAuth.writeViews);
  taosHashCleanup(pUser->userAuth.alterViews);
  taosHashCleanup(pUser->userAuth.useDbs);

  TAOS_MEMCPY(&pUser->userAuth, &msg->userAuth, sizeof(msg->userAuth));

  msg->userAuth.createdDbs = NULL;
  msg->userAuth.readDbs = NULL;
  msg->userAuth.writeDbs = NULL;
  msg->userAuth.readTbs = NULL;
  msg->userAuth.writeTbs = NULL;
  msg->userAuth.alterTbs = NULL;
  msg->userAuth.readViews = NULL;
  msg->userAuth.writeViews = NULL;
  msg->userAuth.alterViews = NULL;
  msg->userAuth.useDbs = NULL;

  CTG_UNLOCK(CTG_WRITE, &pUser->lock);
  
  (void)atomic_store_64(&pUser->userCacheSize, ctgGetUserCacheSize(&pUser->userAuth));

_return:

  taosHashCleanup(msg->userAuth.createdDbs);
  taosHashCleanup(msg->userAuth.readDbs);
  taosHashCleanup(msg->userAuth.writeDbs);
  taosHashCleanup(msg->userAuth.readTbs);
  taosHashCleanup(msg->userAuth.writeTbs);
  taosHashCleanup(msg->userAuth.alterTbs);
  taosHashCleanup(msg->userAuth.readViews);
  taosHashCleanup(msg->userAuth.writeViews);
  taosHashCleanup(msg->userAuth.alterViews);
  taosHashCleanup(msg->userAuth.useDbs);

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateEpset(SCtgCacheOperation *operation) {
  int32_t             code = 0;
  SCtgUpdateEpsetMsg *msg = operation->data;
  SCatalog           *pCtg = msg->pCtg;
  SCtgDBCache        *dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

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

  SVgroupInfo *pInfo = taosHashGet(vgInfo->vgHash, &msg->vgId, sizeof(msg->vgId));
  if (NULL == pInfo) {
    ctgDebug("no vgroup %d in db %s vgHash, ignore epset update", msg->vgId, msg->dbFName);
    goto _return;
  }

  SVgroupInfo *pInfo2 = taosArraySearch(vgInfo->vgArray, &msg->vgId, ctgVgInfoIdComp, TD_EQ);
  if (NULL == pInfo2) {
    ctgDebug("no vgroup %d in db %s vgArray, ignore epset update", msg->vgId, msg->dbFName);
    goto _return;
  }

  SEp *pOrigEp = &pInfo->epSet.eps[pInfo->epSet.inUse];
  SEp *pNewEp = &msg->epSet.eps[msg->epSet.inUse];
  ctgDebug("vgroup %d epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d, dbFName:%s in ctg", pInfo->vgId,
           pInfo->epSet.inUse, pInfo->epSet.numOfEps, pOrigEp->fqdn, pOrigEp->port, msg->epSet.inUse,
           msg->epSet.numOfEps, pNewEp->fqdn, pNewEp->port, msg->dbFName);

  pInfo->epSet = msg->epSet;
  pInfo2->epSet = msg->epSet;

_return:

  if (code == TSDB_CODE_SUCCESS && dbCache) {
    ctgWUnlockVgInfo(dbCache);
  }

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}

int32_t ctgOpUpdateTbIndex(SCtgCacheOperation *operation) {
  int32_t               code = 0;
  SCtgUpdateTbIndexMsg *msg = operation->data;
  SCatalog             *pCtg = msg->pCtg;
  STableIndex          *pIndex = msg->pIndex;
  SCtgDBCache          *dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

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
  int32_t             code = 0;
  SCtgDropTbIndexMsg *msg = operation->data;
  SCatalog           *pCtg = msg->pCtg;
  SCtgDBCache        *dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  STableIndex *pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_ERR_JRET(terrno);
  }
  TAOS_STRCPY(pIndex->tbName, msg->tbName);
  TAOS_STRCPY(pIndex->dbFName, msg->dbFName);
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


int32_t ctgOpUpdateViewMeta(SCtgCacheOperation *operation) {
  int32_t               code = 0;
  SCtgUpdateViewMetaMsg *msg = operation->data;
  SCatalog              *pCtg = msg->pCtg;
  SViewMetaRsp          *pRsp = msg->pRsp;
  SCtgDBCache           *dbCache = NULL;
  SViewMeta             *pMeta = NULL;

  taosMemoryFreeClear(msg);

  if (pCtg->stopUpdate) {
    goto _return;
  }

  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, pRsp->dbFName, pRsp->dbId, &dbCache));
  if (NULL == dbCache) {
    ctgInfo("conflict db update, ignore this update, dbFName:%s, dbId:0x%" PRIx64, pRsp->dbFName, pRsp->dbId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  pMeta = taosMemoryCalloc(1, sizeof(SViewMeta));
  if (NULL == pMeta) {
    CTG_ERR_JRET(terrno);
  }

  CTG_ERR_JRET(dupViewMetaFromRsp(pRsp, pMeta));

  code = ctgWriteViewMetaToCache(pCtg, dbCache, pRsp->dbFName, pRsp->name, pMeta);
  pMeta = NULL;
  
_return:

  tFreeSViewMetaRsp(pRsp);
  taosMemoryFree(pRsp);
  ctgFreeSViewMeta(pMeta);
  taosMemoryFree(pMeta);

  CTG_RET(code);
}

int32_t ctgOpDropViewMeta(SCtgCacheOperation *operation) {
  int32_t              code = 0;
  SCtgDropViewMetaMsg *msg = operation->data;
  SCatalog            *pCtg = msg->pCtg;
  int32_t              tblType = 0;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache) {
    goto _return;
  }

  if ((0 != msg->dbId) && (dbCache->dbId != msg->dbId)) {
    ctgDebug("dbId 0x%" PRIx64 " not match with curId 0x%" PRIx64 ", dbFName:%s, viewName:%s", msg->dbId, dbCache->dbId,
             msg->dbFName, msg->viewName);
    goto _return;
  }

  SCtgViewCache *pViewCache = taosHashGet(dbCache->viewCache, msg->viewName, strlen(msg->viewName));
  if (NULL == pViewCache) {
    ctgDebug("view %s already not in cache", msg->viewName);
    goto _return;
  }

  int64_t viewId = pViewCache->pMeta->viewId;
  if (0 != msg->viewId && viewId != msg->viewId) {
    ctgDebug("viewId 0x%" PRIx64 " not match with curId 0x%" PRIx64 ", viewName:%s", msg->viewId, viewId, msg->viewName);
    goto _return;
  }
  
  (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, ctgGetViewMetaCacheSize(pViewCache->pMeta));
  ctgFreeViewCacheImpl(pViewCache, true);

  if (taosHashRemove(dbCache->viewCache, msg->viewName, strlen(msg->viewName))) {
    ctgError("view %s not exist in cache, dbFName:%s", msg->viewName, msg->dbFName);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  } else {
    (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, sizeof(SCtgViewCache) + strlen(msg->viewName));
    CTG_DB_NUM_DEC(CTG_CI_VIEW);
  }

  ctgDebug("view %s removed from cache, dbFName:%s", msg->viewName, msg->dbFName);

  CTG_ERR_JRET(ctgMetaRentRemove(&msg->pCtg->viewRent, viewId, ctgViewVersionSortCompare, ctgViewVersionSearchCompare));

  ctgDebug("view %s removed from rent, dbFName:%s, viewId:0x%" PRIx64, msg->viewName, msg->dbFName, viewId);

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}


void ctgClearFreeCache(SCtgCacheOperation *operation) {
  SCtgClearCacheMsg *msg = operation->data;
  SCatalog          *pCtg = msg->pCtg;

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.lock);
  
  if (pCtg) {
    if (msg->freeCtg) {
      ctgFreeHandle(pCtg);
    } else {
      ctgClearHandle(pCtg);
    }
  } else if (msg->freeCtg) {
    ctgFreeAllHandles();
  } else {
    ctgClearAllHandles();
  }
  
  CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.lock);
}

void ctgClearMetaCache(SCtgCacheOperation *operation) {
  SCtgClearCacheMsg *msg = operation->data;
  SCatalog          *pCtg = msg->pCtg;
  int64_t            clearedSize = 0;
  int64_t            clearedNum = 0;
  int64_t            remainSize = 0;
  bool               roundDone = false;

  if (pCtg) {
    ctgClearHandleMeta(pCtg, &clearedSize, &clearedNum, &roundDone);
  } else {
    ctgClearAllHandleMeta(&clearedSize, &clearedNum, &roundDone);
  }

  qDebug("catalog finish one round meta clear, clearedSize:%" PRId64 ", clearedNum:%" PRId64 ", done:%d", clearedSize, clearedNum, roundDone);

  ctgGetGlobalCacheSize(&remainSize);
  int32_t cacheMaxSize = atomic_load_32(&tsMetaCacheMaxSize);
  
  if (CTG_CACHE_LOW(remainSize, cacheMaxSize)) {
    qDebug("catalog finish meta clear, remainSize:%" PRId64 ", cacheMaxSize:%dMB", remainSize, cacheMaxSize);
    if (taosTmrReset(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer, &gCtgMgmt.cacheTimer)) {
      qError("reset catalog cache monitor timer error, timer stoppped");
    }
    return;
  }

  if (!roundDone) {
    qDebug("catalog all meta cleared, remainSize:%" PRId64 ", cacheMaxSize:%dMB, to clear handle", remainSize, cacheMaxSize);
    ctgClearFreeCache(operation);
    if (taosTmrReset(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer, &gCtgMgmt.cacheTimer)) {
      qError("reset catalog cache monitor timer error, timer stoppped");
    }
    return;
  }
  
  int32_t code = ctgClearCacheEnqueue(NULL, true, false, false, false);
  if (code) {
    qError("clear cache enqueue failed, error:%s", tstrerror(code));
    if (taosTmrReset(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer, &gCtgMgmt.cacheTimer)) {
      qError("reset catalog cache monitor timer error, timer stoppped");
    }
  }
}

int32_t ctgOpClearCache(SCtgCacheOperation *operation) {
  int32_t            code = 0;
  SCtgClearCacheMsg *msg = operation->data;

  if (msg->clearMeta) {
    ctgClearMetaCache(operation);
  } else {
    ctgClearFreeCache(operation);
  }

_return:

  taosMemoryFreeClear(msg);

  CTG_RET(code);
}


int32_t ctgOpDropTbTSMA(SCtgCacheOperation *operation) {
  int32_t             code = 0;
  SCtgDropTbTSMAMsg * msg = operation->data;
  SCatalog           *pCtg = msg->pCtg;
  SCtgDBCache        *dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  CTG_ERR_JRET(ctgGetDBCache(pCtg, msg->dbFName, &dbCache));
  if (NULL == dbCache || !dbCache->tsmaCache || (msg->dbId != dbCache->dbId && msg->dbId != 0)) {
    goto _return;
  }

  SCtgTSMACache* pCtgCache = taosHashGet(dbCache->tsmaCache, msg->tbName, strlen(msg->tbName));
  if (!pCtgCache || !pCtgCache->pTsmas || pCtgCache->pTsmas->size == 0) {
    goto _return;
  }

  uint64_t    cacheSize = 0;
  STSMACache *pCache = NULL;
  if (msg->dropAllForTb) {
    CTG_LOCK(CTG_WRITE, &pCtgCache->tsmaLock);
    
    for (int32_t i = 0; i < pCtgCache->pTsmas->size; ++i) {
      pCache = taosArrayGetP(pCtgCache->pTsmas, i);
      if (NULL == pCache) {
        ctgError("fail to the %dth tsma in pTsmas, total:%d", i, (int32_t)pCtgCache->pTsmas->size);
        continue;
      }
      
      cacheSize += ctgGetTbTSMACacheSize(pCache);
      (void)ctgMetaRentRemove(&msg->pCtg->tsmaRent, pCache->tsmaId, ctgTSMAVersionSearchCompare, ctgTSMAVersionSearchCompare);
      
      CTG_DB_NUM_DEC(CTG_CI_TBL_TSMA);
    }
    
    taosArrayDestroyP(pCtgCache->pTsmas, tFreeAndClearTableTSMAInfo);
    pCtgCache->pTsmas = NULL;
    
    ctgDebug("all tsmas for table dropped: %s.%s", msg->dbFName, msg->tbName);
    code = taosHashRemove(dbCache->tsmaCache, msg->tbName, TSDB_TABLE_NAME_LEN);
    if (TSDB_CODE_SUCCESS != code) {
      ctgError("remove table %s.%s from tsmaCache failed, error:%s", msg->dbFName, msg->tbName, tstrerror(code));
    }
    
    CTG_UNLOCK(CTG_WRITE, &pCtgCache->tsmaLock);
  } else {
    CTG_LOCK(CTG_WRITE, &pCtgCache->tsmaLock);
    
    pCache = taosArrayGetP(pCtgCache->pTsmas, 0);
    if (NULL == pCache) {
      ctgError("fail to the 0th tsma in pTsmas, total:%d", (int32_t)pCtgCache->pTsmas->size);
      code = TSDB_CODE_CTG_INTERNAL_ERROR;
    } else {
      if (msg->tbId != 0 && pCache->suid != msg->tbId) {
        // table id mismatch, skip drops
        CTG_UNLOCK(CTG_WRITE, &pCtgCache->tsmaLock);
        goto _return;
      }
      
      for (int32_t i = 0; i < pCtgCache->pTsmas->size; ++i) {
        pCache = taosArrayGetP(pCtgCache->pTsmas, i);
        if (NULL == pCache) {
          ctgError("fail to the %dth tsma in pTsmas, total:%d", i, (int32_t)pCtgCache->pTsmas->size);
          code = TSDB_CODE_CTG_INTERNAL_ERROR;
          continue;
        }

        if (pCache->tsmaId != msg->tsmaId) {
          continue;
        }
        
        cacheSize = ctgGetTbTSMACacheSize(pCache);
        (void)ctgMetaRentRemove(&msg->pCtg->tsmaRent, pCache->tsmaId, ctgTSMAVersionSearchCompare, ctgTSMAVersionSearchCompare);
        
        taosArrayRemove(pCtgCache->pTsmas, i);
        tFreeAndClearTableTSMAInfo(pCache);
        
        CTG_DB_NUM_DEC(CTG_CI_TBL_TSMA);
        
        break;
      }
    }
    
    CTG_UNLOCK(CTG_WRITE, &pCtgCache->tsmaLock);
  }
  
  (void)atomic_sub_fetch_64(&dbCache->dbCacheSize, cacheSize);

_return:

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

static int32_t ctgOpUpdateDbRentForTsmaVersion(SCtgDBCache* pDbCache, SCtgUpdateTbTSMAMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pDbCache && pMsg->dbTsmaVersion > 0) {
    pDbCache->tsmaVersion = pMsg->dbTsmaVersion;
    SDbCacheInfo cacheInfo = {0};
    cacheInfo.dbId = pDbCache->dbId;

    if (pDbCache->cfgCache.cfgInfo) {
      cacheInfo.cfgVersion = pDbCache->cfgCache.cfgInfo->cfgVersion;
      tstrncpy(cacheInfo.dbFName, pDbCache->cfgCache.cfgInfo->db, TSDB_DB_FNAME_LEN);
    }

    if (pDbCache->vgCache.vgInfo) {
      cacheInfo.vgVersion = pDbCache->vgCache.vgInfo->vgVersion;
      cacheInfo.numOfTable = pDbCache->vgCache.vgInfo->numOfTable;
      cacheInfo.stateTs = pDbCache->vgCache.vgInfo->stateTs;
    }

    cacheInfo.tsmaVersion = pDbCache->tsmaVersion;
    CTG_ERR_JRET(ctgMetaRentUpdate(&pMsg->pCtg->dbRent, &cacheInfo, cacheInfo.dbId, sizeof(SDbCacheInfo),
                                   ctgDbCacheInfoSortCompare, ctgDbCacheInfoSearchCompare));
  }
_return:
  CTG_RET(code);
}

int32_t ctgOpUpdateTbTSMA(SCtgCacheOperation *operation) {
  int32_t              code = 0;
  SCtgUpdateTbTSMAMsg *msg = operation->data;
  SCatalog *           pCtg = msg->pCtg;
  STableTSMAInfo *     pTsmaInfo = msg->pTsma;
  SCtgDBCache *        dbCache = NULL;

  if (pCtg->stopUpdate) {
    goto _return;
  }

  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, pTsmaInfo->dbFName, pTsmaInfo->dbId, &dbCache));
  CTG_ERR_JRET(ctgWriteTbTSMAToCache(pCtg, dbCache, pTsmaInfo->dbFName, pTsmaInfo->tb, &pTsmaInfo));
  CTG_ERR_JRET(ctgOpUpdateDbRentForTsmaVersion(dbCache, msg));

_return:

  if (pTsmaInfo) {
    tFreeTableTSMAInfo(pTsmaInfo);
    taosMemoryFreeClear(pTsmaInfo);
  }

  taosMemoryFreeClear(msg);
  
  CTG_RET(code);
}

int32_t ctgOpUpdateDbTsmaVersion(SCtgCacheOperation *pOper) {
  int32_t              code = 0;
  SCtgUpdateTbTSMAMsg *pMsg = pOper->data;
  SCatalog            *pCtg = pMsg->pCtg;
  SCtgDBCache         *pDbCache = NULL;

  if (pCtg->stopUpdate) goto _return;

  CTG_ERR_JRET(ctgGetAddDBCache(pCtg, pMsg->dbFName, pMsg->dbId, &pDbCache));
  CTG_ERR_JRET(ctgOpUpdateDbRentForTsmaVersion(pDbCache, pMsg));

_return:
  taosMemoryFreeClear(pMsg);
  CTG_RET(code);
}


void ctgFreeCacheOperationData(SCtgCacheOperation *op) {
  if (NULL == op || NULL == op->data) {
    return;
  }

  switch (op->opId) {
    case CTG_OP_UPDATE_VGROUP: {
      SCtgUpdateVgMsg *msg = op->data;
      freeVgInfo(msg->dbInfo);
      taosMemoryFreeClear(op->data);
      break;
    }
    case CTG_OP_UPDATE_TB_META: {
      SCtgUpdateTbMetaMsg *msg = op->data;
      taosMemoryFreeClear(msg->pMeta->tbMeta);
      taosMemoryFreeClear(msg->pMeta);
      taosMemoryFreeClear(op->data);
      break;
    }
    case CTG_OP_DROP_DB_CACHE:
    case CTG_OP_DROP_DB_VGROUP:
    case CTG_OP_DROP_STB_META:
    case CTG_OP_DROP_TB_META:
    case CTG_OP_UPDATE_VG_EPSET:
    case CTG_OP_DROP_TB_INDEX:
    case CTG_OP_DROP_VIEW_META:
    case CTG_OP_DROP_TB_TSMA:
    case CTG_OP_CLEAR_CACHE: {
      taosMemoryFreeClear(op->data);
      break;
    }
    case CTG_OP_UPDATE_USER: {
      SCtgUpdateUserMsg *msg = op->data;
      taosHashCleanup(msg->userAuth.createdDbs);
      taosHashCleanup(msg->userAuth.readDbs);
      taosHashCleanup(msg->userAuth.writeDbs);
      taosHashCleanup(msg->userAuth.readTbs);
      taosHashCleanup(msg->userAuth.writeTbs);
      taosHashCleanup(msg->userAuth.alterTbs);
      taosHashCleanup(msg->userAuth.readViews);
      taosHashCleanup(msg->userAuth.writeViews);
      taosHashCleanup(msg->userAuth.alterViews);
      taosHashCleanup(msg->userAuth.useDbs);
      taosMemoryFreeClear(op->data);
      break;
    }
    case CTG_OP_UPDATE_TB_INDEX: {
      SCtgUpdateTbIndexMsg *msg = op->data;
      if (msg->pIndex) {
        taosArrayDestroyEx(msg->pIndex->pIndex, tFreeSTableIndexInfo);
        taosMemoryFreeClear(msg->pIndex);
      }
      taosMemoryFreeClear(op->data);
      break;
    }
    case CTG_OP_UPDATE_TB_TSMA: {
      SCtgUpdateTbTSMAMsg *msg = op->data;
      if (msg->pTsma) {
        tFreeTableTSMAInfo(msg->pTsma);
        taosMemoryFreeClear(msg->pTsma);
      }
      break;
    }
    default: {
      qError("invalid cache op id:%d", op->opId);
      break;
    }
  }
}

void ctgCleanupCacheQueue(void) {
  SCtgQNode          *node = NULL;
  SCtgQNode          *nodeNext = NULL;
  SCtgCacheOperation *op = NULL;
  bool                stopQueue = false;
  int32_t             code = 0;

  while (true) {
    node = gCtgMgmt.queue.head->next;
    while (node) {
      if (node->op) {
        op = node->op;
        if (op->stopQueue) {
          SCatalog *pCtg = ((SCtgUpdateMsgHeader *)op->data)->pCtg;
          ctgDebug("process [%s] operation", gCtgCacheOperation[op->opId].name);
          (void)(*gCtgCacheOperation[op->opId].func)(op); // ignore any error
          stopQueue = true;
          CTG_STAT_RT_INC(numOfOpDequeue, 1);
        } else {
          ctgFreeCacheOperationData(op);
          CTG_STAT_RT_INC(numOfOpAbort, 1);
        }

        if (op->syncOp) {
          code = tsem_post(&op->rspSem);
          if (code) {
            qError("tsem_post failed when cleanup cache queue, error:%s", tstrerror(code));
          }
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

void *ctgUpdateThreadFunc(void *param) {
  setThreadName("catalog");
  int32_t code = 0;

  qInfo("catalog update thread started");

  while (true) {
    if (tsem_wait(&gCtgMgmt.queue.reqSem)) {
      qError("ctg tsem_wait failed, error:%s", tstrerror(terrno));
    }

    if (atomic_load_8((int8_t *)&gCtgMgmt.queue.stopQueue)) {
      ctgCleanupCacheQueue();
      break;
    }

    SCtgCacheOperation *operation = NULL;
    ctgDequeue(&operation);
    SCatalog *pCtg = ((SCtgUpdateMsgHeader *)operation->data)->pCtg;

    ctgDebug("process [%s] operation", gCtgCacheOperation[operation->opId].name);

    (void)(*gCtgCacheOperation[operation->opId].func)(operation); // ignore any error

    if (operation->syncOp) {
      code = tsem_post(&operation->rspSem);
      if (code) {
        ctgError("tsem_post failed for syncOp update, error:%s", tstrerror(code));
      }
    } else {
      taosMemoryFreeClear(operation);
    }

    CTG_STAT_RT_INC(numOfOpDequeue, 1);

    (void)ctgdShowCacheInfo();
    (void)ctgdShowStatInfo();
  }

  qInfo("catalog update thread stopped");

  return NULL;
}

int32_t ctgStartUpdateThread() {
  int32_t code = TSDB_CODE_SUCCESS;
  TdThreadAttr thAttr;
  CTG_ERR_JRET(taosThreadAttrInit(&thAttr));
  CTG_ERR_JRET(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE));

  if (taosThreadCreate(&gCtgMgmt.updateThread, &thAttr, ctgUpdateThreadFunc, NULL) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    CTG_ERR_RET(terrno);
  }

  (void)taosThreadAttrDestroy(&thAttr);

_return:

  if (code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    CTG_ERR_RET(terrno);
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromCache(SCatalog *pCtg, SCtgTbMetaCtx *ctx, STableMeta **pTableMeta) {
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
    *pTableMeta = NULL;
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

int32_t ctgGetTbMetasFromCache(SCatalog *pCtg, SRequestConnInfo *pConn, SCtgTbMetasCtx *ctx, int32_t dbIdx,
                               int32_t *fetchIdx, int32_t baseResIdx, SArray *pList) {
  int32_t     tbNum = taosArrayGetSize(pList);
  char        dbFName[TSDB_DB_FNAME_LEN] = {0};
  int32_t     flag = CTG_FLAG_UNKNOWN_STB;
  int32_t     code = TSDB_CODE_SUCCESS;
  uint64_t    lastSuid = 0;
  STableMeta *lastTableMeta = NULL;
  SName      *pName = taosArrayGet(pList, 0);
  if (NULL == pName) {
    ctgError("fail to get the 0th SName from tableList, tableNum:%d", (int32_t)taosArrayGetSize(pList));
    return TSDB_CODE_CTG_INVALID_INPUT;
  }
  
  if (IS_SYS_DBNAME(pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(flag);
    TAOS_STRCPY(dbFName, pName->dbname);
  } else {
    (void)tNameGetFullDbName(pName, dbFName);
  }

  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *pCache = NULL;
  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));

  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    for (int32_t i = 0; i < tbNum; ++i) {
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaData){0})) {
        CTG_ERR_JRET(terrno);
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < tbNum; ++i) {
    pName = taosArrayGet(pList, i);
    if (NULL == pName) {
      ctgError("fail to get the %dth SName from tableList, tableNum:%d", i, (int32_t)taosArrayGetSize(pList));
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pCache = taosHashAcquire(dbCache->tbCache, pName->tname, strlen(pName->tname));
    if (NULL == pCache) {
      ctgDebug("tb %s not in cache, dbFName:%s", pName->tname, dbFName);
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaData){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      CTG_META_NHIT_INC();

      continue;
    }

    CTG_LOCK(CTG_READ, &pCache->metaLock);
    if (NULL == pCache->pMeta) {
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache);
      
      ctgDebug("tb %s meta not in cache, dbFName:%s", pName->tname, dbFName);
      
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaData){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      CTG_META_NHIT_INC();

      continue;
    }

    STableMeta *tbMeta = pCache->pMeta;

    CTG_META_HIT_INC(tbMeta->tableType);

    SCtgTbMetaCtx nctx = {0};
    nctx.flag = flag;
    nctx.tbInfo.inCache = true;
    nctx.tbInfo.dbId = dbCache->dbId;
    nctx.tbInfo.suid = tbMeta->suid;
    nctx.tbInfo.tbType = tbMeta->tableType;

    SMetaRes    res = {0};
    STableMeta *pTableMeta = NULL;
    if (tbMeta->tableType != TSDB_CHILD_TABLE) {
      int32_t schemaExtSize = 0;
      int32_t metaSize = CTG_META_SIZE(tbMeta);
      if (tbMeta->schemaExt != NULL) {
        schemaExtSize = tbMeta->tableInfo.numOfColumns * sizeof(SSchemaExt);
      }
      
      pTableMeta = taosMemoryCalloc(1, metaSize + schemaExtSize);
      if (NULL == pTableMeta) {
        ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);
        CTG_ERR_RET(terrno);
      }

      TAOS_MEMCPY(pTableMeta, tbMeta, metaSize);
      if (tbMeta->schemaExt != NULL) {
        pTableMeta->schemaExt = (SSchemaExt *)((char *)pTableMeta + metaSize);
        TAOS_MEMCPY(pTableMeta->schemaExt, tbMeta->schemaExt, schemaExtSize);
      } else {
        pTableMeta->schemaExt = NULL;
      }

      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache);

      ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", pName->tname, pTableMeta->tableType, dbFName);

      res.pRes = pTableMeta;
      if (NULL == taosArrayPush(ctx->pResList, &res)) {
        CTG_ERR_JRET(terrno);
      }

      continue;
    }

    // PROCESS FOR CHILD TABLE

    if (lastSuid && tbMeta->suid == lastSuid && lastTableMeta) {
      code = cloneTableMeta(lastTableMeta, &pTableMeta);
      if (code) {
        CTG_UNLOCK(CTG_READ, &pCache->metaLock);
        taosHashRelease(dbCache->tbCache, pCache);
        CTG_ERR_JRET(code);
      }
      
      TAOS_MEMCPY(pTableMeta, tbMeta, sizeof(SCTableMeta));

      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache);

      ctgDebug("Got tb %s meta from cache, type:%d, dbFName:%s", pName->tname, pTableMeta->tableType, dbFName);

      res.pRes = pTableMeta;
      if (NULL == taosArrayPush(ctx->pResList, &res)) {
        CTG_ERR_JRET(terrno);
      }

      continue;
    }

    int32_t metaSize = sizeof(SCTableMeta);
    pTableMeta = taosMemoryCalloc(1, metaSize);
    if (NULL == pTableMeta) {
      ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);
      CTG_ERR_RET(terrno);
    }

    TAOS_MEMCPY(pTableMeta, tbMeta, metaSize);

    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache);

    ctgDebug("Got ctb %s meta from cache, will continue to get its stb meta, type:%d, dbFName:%s", pName->tname,
             nctx.tbInfo.tbType, dbFName);

    char *stName = taosHashAcquire(dbCache->stbCache, &pTableMeta->suid, sizeof(pTableMeta->suid));
    if (NULL == stName) {
      ctgDebug("stb 0x%" PRIx64 " not in cache, dbFName:%s", pTableMeta->suid, dbFName);
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      taosMemoryFreeClear(pTableMeta);

      CTG_META_NHIT_INC();
      continue;
    }

    pCache = taosHashAcquire(dbCache->tbCache, stName, strlen(stName));
    if (NULL == pCache) {
      ctgDebug("stb 0x%" PRIx64 " name %s not in cache, dbFName:%s", pTableMeta->suid, stName, dbFName);
      taosHashRelease(dbCache->stbCache, stName);

      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      taosMemoryFreeClear(pTableMeta);

      CTG_META_NHIT_INC();
      
      continue;
    }

    taosHashRelease(dbCache->stbCache, stName);

    CTG_LOCK(CTG_READ, &pCache->metaLock);
    if (NULL == pCache->pMeta) {
      ctgDebug("stb 0x%" PRIx64 " meta not in cache, dbFName:%s", pTableMeta->suid, dbFName);
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache);

      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      taosMemoryFreeClear(pTableMeta);

      CTG_META_NHIT_INC();
      continue;
    }

    STableMeta *stbMeta = pCache->pMeta;
    if (stbMeta->suid != nctx.tbInfo.suid) {
      CTG_UNLOCK(CTG_READ, &pCache->metaLock);
      taosHashRelease(dbCache->tbCache, pCache);

      ctgError("stb suid 0x%" PRIx64 " in stbCache mis-match, expected suid 0x%" PRIx64, stbMeta->suid,
               nctx.tbInfo.suid);

      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      taosMemoryFreeClear(pTableMeta);

      CTG_META_NHIT_INC();
      continue;
    }

    int32_t schemaExtSize = 0;
    if (stbMeta->schemaExt != NULL) {
      schemaExtSize = stbMeta->tableInfo.numOfColumns * sizeof(SSchemaExt);
    }
    metaSize = CTG_META_SIZE(stbMeta);
    pTableMeta = taosMemoryRealloc(pTableMeta, metaSize + schemaExtSize);
    if (NULL == pTableMeta) {
      ctgReleaseTbMetaToCache(pCtg, dbCache, pCache);
      CTG_ERR_RET(terrno);
    }

    TAOS_MEMCPY(&pTableMeta->sversion, &stbMeta->sversion, metaSize + schemaExtSize - sizeof(SCTableMeta));
    if (stbMeta->schemaExt != NULL) {
      pTableMeta->schemaExt = (SSchemaExt *)((char *)pTableMeta + metaSize);
    } else {
      pTableMeta->schemaExt = NULL;
    }

    CTG_UNLOCK(CTG_READ, &pCache->metaLock);
    taosHashRelease(dbCache->tbCache, pCache);

    CTG_META_HIT_INC(pTableMeta->tableType);

    res.pRes = pTableMeta;
    if (NULL == taosArrayPush(ctx->pResList, &res)) {
      CTG_ERR_JRET(terrno);
    }

    lastSuid = pTableMeta->suid;
    lastTableMeta = pTableMeta;
  }

_return:

  ctgReleaseDBCache(pCtg, dbCache);

  return code;
}

int32_t ctgRemoveTbMetaFromCache(SCatalog *pCtg, SName *pTableName, bool syncReq) {
  int32_t       code = 0;
  STableMeta   *tblMeta = NULL;
  SCtgTbMetaCtx tbCtx = {0};
  tbCtx.flag = CTG_FLAG_UNKNOWN_STB;
  tbCtx.pName = pTableName;

  CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &tbCtx, &tblMeta));

  if (NULL != tblMeta) {
    char dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(pTableName, dbFName);

    if (TSDB_SUPER_TABLE == tblMeta->tableType) {
      CTG_ERR_JRET(ctgDropStbMetaEnqueue(pCtg, dbFName, tbCtx.tbInfo.dbId, pTableName->tname, tblMeta->suid, syncReq));
    } else {
      CTG_ERR_JRET(ctgDropTbMetaEnqueue(pCtg, dbFName, tbCtx.tbInfo.dbId, pTableName->tname, syncReq));
    }
  } else {
    ctgDebug("table already not in cache, db:%s, tblName:%s", pTableName->dbname, pTableName->tname);
  }

  CTG_ERR_JRET(ctgDropTSMAForTbEnqueue(pCtg, pTableName, syncReq));

_return:

  taosMemoryFreeClear(tblMeta);

  CTG_RET(code);
}

int32_t ctgGetTbNamesFromCache(SCatalog *pCtg, SRequestConnInfo *pConn, SCtgTbNamesCtx *ctx, int32_t dbIdx,
                               int32_t *fetchIdx, int32_t baseResIdx, SArray *pList) {
  int32_t     tbNum = taosArrayGetSize(pList);
  char        dbFName[TSDB_DB_FNAME_LEN] = {0};
  int32_t     flag = CTG_FLAG_UNKNOWN_STB;
  int32_t     code = TSDB_CODE_SUCCESS;
  uint64_t    lastSuid = 0;
  STableMeta *lastTableMeta = NULL;
  SName      *pName = taosArrayGet(pList, 0);
  if (NULL == pName) {
    ctgError("fail to get the 0th SName from tableList, tableNum:%d", (int32_t)taosArrayGetSize(pList));
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (IS_SYS_DBNAME(pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(flag);
    TAOS_STRCPY(dbFName, pName->dbname);
  } else {
    (void)tNameGetFullDbName(pName, dbFName);
  }

  ctgDebug("db %s not in cache", dbFName);
  for (int32_t i = 0; i < tbNum; ++i) {
    CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
    if (NULL == taosArrayPush(ctx->pResList, &(SMetaData){0})) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

_return:
  return code;
}

int32_t ctgGetTbHashVgroupFromCache(SCatalog *pCtg, const SName *pTableName, SVgroupInfo **pVgroup) {
  if (IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache *dbCache = NULL;
  int32_t      code = 0;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, dbFName);

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));

  if (NULL == dbCache) {
    *pVgroup = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pVgroup = taosMemoryCalloc(1, sizeof(SVgroupInfo));
  if (NULL == *pVgroup) {
    CTG_ERR_JRET(terrno);
  }
  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, NULL, dbCache->vgCache.vgInfo, pTableName, *pVgroup));

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  if (code) {
    taosMemoryFreeClear(*pVgroup);
  }

  CTG_RET(code);
}


int32_t ctgGetViewsFromCache(SCatalog *pCtg, SRequestConnInfo *pConn, SCtgViewsCtx *ctx, int32_t dbIdx,
                               int32_t *fetchIdx, int32_t baseResIdx, SArray *pList) {
  int32_t     tbNum = taosArrayGetSize(pList);
  char        dbFName[TSDB_DB_FNAME_LEN] = {0};
  int32_t     flag = CTG_FLAG_UNKNOWN_STB;
  uint64_t    lastSuid = 0;
  STableMeta *lastTableMeta = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  SName      *pName = taosArrayGet(pList, 0);
  if (NULL == pName) {
    ctgError("fail to get the 0th SName from viewList, viewNum:%d", (int32_t)taosArrayGetSize(pList));
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  if (IS_SYS_DBNAME(pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(flag);
    TAOS_STRCPY(dbFName, pName->dbname);
  } else {
    (void)tNameGetFullDbName(pName, dbFName);
  }

  SCtgDBCache *dbCache = NULL;
  SCtgViewCache *pCache = NULL;
  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));

  if (NULL == dbCache) {
    ctgDebug("db %s not in cache", dbFName);
    for (int32_t i = 0; i < tbNum; ++i) {
      CTG_ERR_RET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaData){0})) {
        CTG_ERR_RET(terrno);
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < tbNum; ++i) {
    pName = taosArrayGet(pList, i);
    if (NULL == pName) {
      ctgError("fail to get the %dth SName from viewList, viewNum:%d", i, (int32_t)taosArrayGetSize(pList));
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pCache = taosHashAcquire(dbCache->viewCache, pName->tname, strlen(pName->tname));
    if (NULL == pCache) {
      ctgDebug("view %s not in cache, dbFName:%s", pName->tname, dbFName);
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      CTG_CACHE_NHIT_INC(CTG_CI_VIEW, 1);

      continue;
    }

    CTG_LOCK(CTG_READ, &pCache->viewLock);
    if (NULL == pCache->pMeta) {
      CTG_UNLOCK(CTG_READ, &pCache->viewLock);
      taosHashRelease(dbCache->viewCache, pCache);
      ctgDebug("view %s meta not in cache, dbFName:%s", pName->tname, dbFName);
      CTG_ERR_JRET(ctgAddFetch(&ctx->pFetchs, dbIdx, i, fetchIdx, baseResIdx + i, flag));
      if (NULL == taosArrayPush(ctx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      CTG_CACHE_NHIT_INC(CTG_CI_VIEW, 1);

      continue;
    }

    CTG_CACHE_HIT_INC(CTG_CI_VIEW, 1);

    SMetaRes    res = {0};
    SViewMeta  *pViewMeta = taosMemoryCalloc(1, sizeof(SViewMeta));
    if (NULL == pViewMeta) {
      ctgReleaseViewMetaToCache(pCtg, dbCache, pCache);
      CTG_ERR_RET(terrno);
    }

    TAOS_MEMCPY(pViewMeta, pCache->pMeta, sizeof(*pViewMeta));
    pViewMeta->querySql = tstrdup(pCache->pMeta->querySql);
    pViewMeta->user = tstrdup(pCache->pMeta->user);
    if (NULL == pViewMeta->querySql || NULL == pViewMeta->user) {
      ctgReleaseViewMetaToCache(pCtg, dbCache, pCache);
      pViewMeta->pSchema = NULL;
      taosMemoryFree(pViewMeta->querySql);
      taosMemoryFree(pViewMeta->user);
      taosMemoryFree(pViewMeta);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    pViewMeta->pSchema = taosMemoryMalloc(pViewMeta->numOfCols * sizeof(SSchema));
    if (pViewMeta->pSchema == NULL) {
      ctgReleaseViewMetaToCache(pCtg, dbCache, pCache);
      ctgFreeSViewMeta(pViewMeta);
      taosMemoryFree(pViewMeta);
      CTG_ERR_RET(terrno);
    }
    TAOS_MEMCPY(pViewMeta->pSchema, pCache->pMeta->pSchema, pViewMeta->numOfCols * sizeof(SSchema));

    CTG_UNLOCK(CTG_READ, &pCache->viewLock);
    taosHashRelease(dbCache->viewCache, pCache);

    ctgDebug("Got view %s meta from cache, dbFName:%s", pName->tname, dbFName);

    res.pRes = pViewMeta;
    if (NULL == taosArrayPush(ctx->pResList, &res)) {
      CTG_ERR_JRET(terrno);
    }
  }

_return:

  ctgReleaseDBCache(pCtg, dbCache);

  return code;
}

int32_t ctgGetTbTSMAFromCache(SCatalog* pCtg, SCtgTbTSMACtx* pCtx, int32_t dbIdx, int32_t* fetchIdx, int32_t baseResIdx,
                              SArray* pList) {
  int32_t        code = 0;
  SCtgDBCache *  dbCache = NULL;
  SCtgTSMACache *pCache = NULL;
  char           dbFName[TSDB_DB_FNAME_LEN] = {0};
  int32_t        flag = CTG_FLAG_UNKNOWN_STB;
  uint64_t       lastSuid = 0;
  STableMeta *   pTableMeta = NULL;
  int32_t        tbNum = taosArrayGetSize(pList);
  SCtgTbCache *  pTbCache = NULL;
  SName *        pName = taosArrayGet(pList, 0);
  if (NULL == pName) {
    ctgError("fail to get the 0th SName from tbTSMAList, num:%d", (int32_t)taosArrayGetSize(pList));
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  if (IS_SYS_DBNAME(pName->dbname)) {
    return TSDB_CODE_SUCCESS;
  }
  (void)tNameGetFullDbName(pName, dbFName);

  // get db cache
  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &dbCache));
  if (!dbCache) {
    ctgDebug("DB %s not in cache", dbFName);
    for (int32_t i = 0; i < tbNum; ++i) {
      CTG_ERR_RET(ctgAddTSMAFetch(&pCtx->pFetches, dbIdx, i, fetchIdx, baseResIdx + i, flag, FETCH_TSMA_SOURCE_TB_META, NULL));
      if (NULL == taosArrayPush(pCtx->pResList, &(SMetaData){0})) {
        CTG_ERR_RET(terrno);
      }
    }
    
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < tbNum; ++i) {
    // get tb cache
    pName = taosArrayGet(pList, i);
    if (NULL == pName) {
      ctgError("fail to get the %dth SName from tbTSMAList, num:%d", i, (int32_t)taosArrayGetSize(pList));
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    
    pTbCache = taosHashAcquire(dbCache->tbCache, pName->tname, strlen(pName->tname));
    if (!pTbCache) {
      ctgDebug("tb: %s.%s not in cache", dbFName, pName->tname);
      CTG_ERR_JRET(ctgAddTSMAFetch(&pCtx->pFetches, dbIdx, i, fetchIdx, baseResIdx + i, flag, FETCH_TSMA_SOURCE_TB_META, NULL));
      if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      continue;
    }
    
    CTG_LOCK(CTG_READ, &pTbCache->metaLock);
    if (!pTbCache->pMeta) {
      CTG_UNLOCK(CTG_READ, &pTbCache->metaLock);
      ctgDebug("tb: %s.%s not in cache", dbFName, pName->tname);
      
      CTG_ERR_JRET(ctgAddTSMAFetch(&pCtx->pFetches, dbIdx, i, fetchIdx, baseResIdx + i, flag, FETCH_TSMA_SOURCE_TB_META, NULL));
      if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      taosHashRelease(dbCache->tbCache, pTbCache);
      
      continue;
    }
    uint64_t suid = pTbCache->pMeta->suid;
    int8_t   tbType = pTbCache->pMeta->tableType;
    CTG_UNLOCK(CTG_READ, &pTbCache->metaLock);
    
    taosHashRelease(dbCache->tbCache, pTbCache);
    SName tsmaSourceTbName = *pName;

    // if child table, get stable name
    if (tbType == TSDB_CHILD_TABLE) {
      char* stbName = taosHashAcquire(dbCache->stbCache, &suid, sizeof(uint64_t));
      if (stbName) {
        (void)snprintf(tsmaSourceTbName.tname, TMIN(TSDB_TABLE_NAME_LEN, strlen(stbName) + 1), "%s", stbName);
        taosHashRelease(dbCache->stbCache, stbName);
      } else {
        ctgDebug("stb in db: %s, uid: %" PRId64 " not in cache", dbFName, suid);
        
        CTG_ERR_JRET(ctgAddTSMAFetch(&pCtx->pFetches, dbIdx, i, fetchIdx, baseResIdx + i, flag, FETCH_TSMA_SOURCE_TB_META, NULL));
        if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
          CTG_ERR_JRET(terrno);
        }
        
        continue;
      }
    }

    // get tsma cache
    pCache = taosHashAcquire(dbCache->tsmaCache, tsmaSourceTbName.tname, strlen(tsmaSourceTbName.tname));
    if (!pCache || !pCache->pTsmas || pCache->pTsmas->size == 0) {
      if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
        ctgReleaseTSMAToCache(pCtg, dbCache, pCache);
        CTG_ERR_RET(terrno);
      }
      
      continue;
    }

    CTG_LOCK(CTG_READ, &pCache->tsmaLock);
    if (hasOutOfDateTSMACache(pCache->pTsmas)) {
      CTG_UNLOCK(CTG_READ, &pCache->tsmaLock);
      taosHashRelease(dbCache->tsmaCache, pCache);
      
      ctgDebug("tsma for tb: %s.%s not in cache", tsmaSourceTbName.tname, dbFName);
      
      CTG_ERR_JRET(ctgAddTSMAFetch(&pCtx->pFetches, dbIdx, i, fetchIdx, baseResIdx + i, flag, FETCH_TB_TSMA, &tsmaSourceTbName));
      if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
        CTG_ERR_JRET(terrno);
      }
      
      CTG_CACHE_NHIT_INC(CTG_CI_TBL_TSMA, 1);
      continue;
    }

    CTG_CACHE_HIT_INC(CTG_CI_TBL_TSMA, 1);

    STableTSMAInfoRsp *pRsp = taosMemoryCalloc(1, sizeof(STableTSMAInfoRsp));
    if (!pRsp) {
      ctgReleaseTSMAToCache(pCtg, dbCache, pCache);
      CTG_ERR_RET(terrno);
    }
    
    pRsp->pTsmas = taosArrayInit(pCache->pTsmas->size, POINTER_BYTES);
    if (!pRsp->pTsmas) {
      ctgReleaseTSMAToCache(pCtg, dbCache, pCache);
      taosMemoryFreeClear(pRsp);
      CTG_ERR_RET(terrno);
    }
    
    SMetaRes res = {0};
    for (int32_t i = 0; i < pCache->pTsmas->size; ++i) {
      STSMACache *pTsmaOut = NULL;
      STSMACache *pTsmaCache = taosArrayGetP(pCache->pTsmas, i);
      code = tCloneTbTSMAInfo(pTsmaCache, &pTsmaOut);
      if (TSDB_CODE_SUCCESS != code) {
        ctgReleaseTSMAToCache(pCtg, dbCache, pCache);
        tFreeTableTSMAInfoRsp(pRsp);
        taosMemoryFreeClear(pRsp);
        CTG_ERR_RET(code);
      }
      
      if (NULL == taosArrayPush(pRsp->pTsmas, &pTsmaOut)) {
        ctgReleaseTSMAToCache(pCtg, dbCache, pCache);
        tFreeTableTSMAInfoRsp(pRsp);
        taosMemoryFreeClear(pRsp);
        CTG_ERR_RET(terrno);
      }
    }
    res.pRes = pRsp;
    CTG_UNLOCK(CTG_READ, &pCache->tsmaLock);
    taosHashRelease(dbCache->tsmaCache, pCache);
    if (NULL == taosArrayPush(pCtx->pResList, &res)) {
      CTG_ERR_JRET(terrno);
    }
  }

_return:
  
  ctgReleaseDBCache(pCtg, dbCache);
  
  CTG_RET(code);
}

int32_t ctgGetTSMAFromCache(SCatalog* pCtg, SCtgTbTSMACtx* pCtx, SName* pTsmaName) {
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  SCtgDBCache *pDbCache = NULL;
  int32_t      code = TSDB_CODE_SUCCESS;
  SMetaRes     res = {0};
  bool         found = false;
  STSMACache * pTsmaOut = NULL;

  (void)tNameGetFullDbName(pTsmaName, dbFName);

  CTG_ERR_RET(ctgAcquireDBCache(pCtg, dbFName, &pDbCache));
  if (!pDbCache) {
    ctgDebug("DB %s not in cache", dbFName);
    CTG_RET(code);
  }

  void *pIter = taosHashIterate(pDbCache->tsmaCache, NULL);

  while (pIter && !found) {
    SCtgTSMACache* pCtgCache = pIter;
    
    CTG_LOCK(CTG_READ, &pCtgCache->tsmaLock);
    int32_t size = pCtgCache ?  (pCtgCache->pTsmas ? pCtgCache->pTsmas->size : 0) : 0;
    for (int32_t i = 0; i < size; ++i) {
      STSMACache* pCache = taosArrayGetP(pCtgCache->pTsmas, i);
      if (NULL == pCache) {
        ctgError("fail to the %dth tsma in pTsmas, total:%d", i, size);
        code = TSDB_CODE_CTG_INTERNAL_ERROR;
        break;
      }
      if (memcmp(pCache->name, pTsmaName->tname, TSDB_TABLE_NAME_LEN) == 0) {
        found = true;
        CTG_CACHE_NHIT_INC(CTG_CI_TBL_TSMA, 1);
        code = tCloneTbTSMAInfo(pCache, &pTsmaOut);
        break;
      }
    }
    CTG_UNLOCK(CTG_READ, &pCtgCache->tsmaLock);

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
    
    pIter = taosHashIterate(pDbCache->tsmaCache, pIter);
  }
  
  taosHashCancelIterate(pDbCache->tsmaCache, pIter);
  
  if (found && code == TSDB_CODE_SUCCESS) {
    res.pRes = taosMemoryCalloc(1, sizeof(STableTSMAInfoRsp));
    if (!res.pRes) {
      tFreeAndClearTableTSMAInfo(pTsmaOut);
      CTG_ERR_JRET(terrno);
    }
    
    STableTSMAInfoRsp* pRsp = res.pRes;
    pRsp->pTsmas = taosArrayInit(1, POINTER_BYTES);
    if (!pRsp->pTsmas) {
      tFreeAndClearTableTSMAInfo(pTsmaOut);
      CTG_ERR_JRET(terrno);
    }

    if (NULL == taosArrayPush(pRsp->pTsmas, &pTsmaOut)) {
      tFreeAndClearTableTSMAInfo(pTsmaOut);
      CTG_ERR_JRET(terrno);
    }
    
    if (NULL == taosArrayPush(pCtx->pResList, &res)) {
      CTG_ERR_JRET(terrno);
    }
  }

_return:

  ctgReleaseDBCache(pCtg, pDbCache);
  
  CTG_RET(code);
}

