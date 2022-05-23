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

extern SCatalogMgmt gCtgMgmt;
SCtgDebug gCTGDebug = {0};

int32_t ctgdEnableDebug(char *option) {
  if (0 == strcasecmp(option, "lock")) {
    gCTGDebug.lockEnable = true;
    qDebug("lock debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "cache")) {
    gCTGDebug.cacheEnable = true;
    qDebug("cache debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "api")) {
    gCTGDebug.apiEnable = true;
    qDebug("api debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "meta")) {
    gCTGDebug.metaEnable = true;
    qDebug("api debug enabled");
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid debug option:%s", option);
  
  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgdGetStatNum(char *option, void *res) {
  if (0 == strcasecmp(option, "runtime.qDoneNum")) {
    *(uint64_t *)res = atomic_load_64(&gCtgMgmt.stat.runtime.qDoneNum);
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid stat option:%s", option);
  
  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgdGetTbMetaNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.metaCache ? (int32_t)taosHashGetSize(dbCache->tbCache.metaCache) : 0;
}

int32_t ctgdGetStbNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.stbCache ? (int32_t)taosHashGetSize(dbCache->tbCache.stbCache) : 0;
}

int32_t ctgdGetRentNum(SCtgRentMgmt *rent) {
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

int32_t ctgdGetClusterCacheNum(SCatalog* pCtg, int32_t type) {
  if (NULL == pCtg || NULL == pCtg->dbCache) {
    return 0;
  }

  switch (type) {
    case CTG_DBG_DB_NUM:
      return (int32_t)taosHashGetSize(pCtg->dbCache);
    case CTG_DBG_DB_RENT_NUM:
      return ctgdGetRentNum(&pCtg->dbRent);
    case CTG_DBG_STB_RENT_NUM:
      return ctgdGetRentNum(&pCtg->stbRent);
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
        num += ctgdGetTbMetaNum(dbCache);
        break;
      case CTG_DBG_STB_NUM:
        num += ctgdGetStbNum(dbCache);
        break;
      default:
        ctgError("invalid type:%d", type);
        break;
    }
    pIter = taosHashIterate(pCtg->dbCache, pIter);
  }

  return num;
}

void ctgdShowTableMeta(SCatalog* pCtg, const char *tbName, STableMeta* p) {
  if (!gCTGDebug.metaEnable) {
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

void ctgdShowDBCache(SCatalog* pCtg, SHashObj *dbHash) {
  if (NULL == dbHash || !gCTGDebug.cacheEnable) {
    return;
  }

  int32_t i = 0;
  SCtgDBCache *dbCache = NULL;
  void *pIter = taosHashIterate(dbHash, NULL);
  while (pIter) {
    char *dbFName = NULL;
    size_t len = 0;
    
    dbCache = (SCtgDBCache *)pIter;

    dbFName = taosHashGetKey(pIter, &len);

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




void ctgdShowClusterCache(SCatalog* pCtg) {
  if (!gCTGDebug.cacheEnable || NULL == pCtg) {
    return;
  }

  ctgDebug("## cluster %"PRIx64" %p cache Info BEGIN ##", pCtg->clusterId, pCtg);
  ctgDebug("db:%d meta:%d stb:%d dbRent:%d stbRent:%d", ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM),
    ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_RENT_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM));    
  
  ctgdShowDBCache(pCtg, pCtg->dbCache);

  ctgDebug("## cluster %"PRIx64" %p cache Info END ##", pCtg->clusterId, pCtg);
}

int32_t ctgdShowCacheInfo(void) {
  if (!gCTGDebug.cacheEnable) {
    return TSDB_CODE_CTG_OUT_OF_SERVICE;
  }

  CTG_API_ENTER();
  
  SCatalog *pCtg = NULL;
  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      ctgdShowClusterCache(pCtg);
    }
    
    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

