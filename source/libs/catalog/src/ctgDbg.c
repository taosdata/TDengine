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
#include "tname.h"
#include "trpc.h"

extern SCatalogMgmt gCtgMgmt;
SCtgDebug           gCTGDebug = {0};

#if 0

void ctgdUserCallback(SMetaData *pResult, void *param, int32_t code) {
  taosMemoryFree(param);

  qDebug("async call result: %s", tstrerror(code));
  if (NULL == pResult) {
    qDebug("empty meta result");
    return;
  }

  int32_t num = 0;

  if (pResult->pTableMeta && taosArrayGetSize(pResult->pTableMeta) > 0) {
    num = taosArrayGetSize(pResult->pTableMeta);
    for (int32_t i = 0; i < num; ++i) {
      STableMeta    *p = *(STableMeta **)taosArrayGet(pResult->pTableMeta, i);
      STableComInfo *c = &p->tableInfo;

      if (TSDB_CHILD_TABLE == p->tableType) {
        qDebug("table meta: type:%d, vgId:%d, uid:0x%" PRIx64 ",suid:0x%" PRIx64, p->tableType, p->vgId, p->uid,
               p->suid);
      } else {
        qDebug("table meta: type:%d, vgId:%d, uid:0x%" PRIx64 ",suid:0x%" PRIx64
               ",sv:%d, tv:%d, tagNum:%d, precision:%d, colNum:%d, rowSize:%d",
               p->tableType, p->vgId, p->uid, p->suid, p->sversion, p->tversion, c->numOfTags, c->precision,
               c->numOfColumns, c->rowSize);
      }

      int32_t colNum = c->numOfColumns + c->numOfTags;
      for (int32_t j = 0; j < colNum; ++j) {
        SSchema *s = &p->schema[j];
        qDebug("[%d] name:%s, type:%d, colId:%d, bytes:%d", j, s->name, s->type, s->colId, s->bytes);
      }
    }
  } else {
    qDebug("empty table meta");
  }

  if (pResult->pDbVgroup && taosArrayGetSize(pResult->pDbVgroup) > 0) {
    num = taosArrayGetSize(pResult->pDbVgroup);
    for (int32_t i = 0; i < num; ++i) {
      SArray *pDb = *(SArray **)taosArrayGet(pResult->pDbVgroup, i);
      int32_t vgNum = taosArrayGetSize(pDb);
      qDebug("db %d vgInfo:", i);
      for (int32_t j = 0; j < vgNum; ++j) {
        SVgroupInfo *pInfo = taosArrayGet(pDb, j);
        qDebug("vg :%d info: vgId:%d", j, pInfo->vgId);
      }
    }
  } else {
    qDebug("empty db vgroup");
  }

  if (pResult->pDbInfo && taosArrayGetSize(pResult->pDbInfo) > 0) {
    num = taosArrayGetSize(pResult->pDbInfo);
    for (int32_t i = 0; i < num; ++i) {
      SDbInfo *pDb = taosArrayGet(pResult->pDbInfo, i);
      qDebug("db %d dbInfo: vgVer:%d, tbNum:%d, stateTs:%" PRId64 " dbId:0x%" PRIx64, i, pDb->vgVer, pDb->tbNum, pDb->stateTs, pDb->dbId);
    }
  } else {
    qDebug("empty db info");
  }

  if (pResult->pTableHash && taosArrayGetSize(pResult->pTableHash) > 0) {
    num = taosArrayGetSize(pResult->pTableHash);
    for (int32_t i = 0; i < num; ++i) {
      SVgroupInfo *pInfo = taosArrayGet(pResult->pTableHash, i);
      qDebug("table %d vg info: vgId:%d", i, pInfo->vgId);
    }
  } else {
    qDebug("empty table hash vgroup");
  }

  if (pResult->pUdfList && taosArrayGetSize(pResult->pUdfList) > 0) {
    num = taosArrayGetSize(pResult->pUdfList);
    for (int32_t i = 0; i < num; ++i) {
      SFuncInfo *pInfo = taosArrayGet(pResult->pUdfList, i);
      qDebug("udf %d info: name:%s, funcType:%d", i, pInfo->name, pInfo->funcType);
    }
  } else {
    qDebug("empty udf info");
  }

  if (pResult->pDbCfg && taosArrayGetSize(pResult->pDbCfg) > 0) {
    num = taosArrayGetSize(pResult->pDbCfg);
    for (int32_t i = 0; i < num; ++i) {
      SDbCfgInfo *pInfo = taosArrayGet(pResult->pDbCfg, i);
      qDebug("db %d info: numOFVgroups:%d, numOfStables:%d", i, pInfo->numOfVgroups, pInfo->numOfStables);
    }
  } else {
    qDebug("empty db cfg info");
  }

  if (pResult->pUser && taosArrayGetSize(pResult->pUser) > 0) {
    num = taosArrayGetSize(pResult->pUser);
    for (int32_t i = 0; i < num; ++i) {
      bool *auth = taosArrayGet(pResult->pUser, i);
      qDebug("user auth %d info: %d", i, *auth);
    }
  } else {
    qDebug("empty user auth info");
  }

  if (pResult->pQnodeList && taosArrayGetSize(pResult->pQnodeList) > 0) {
    num = taosArrayGetSize(pResult->pQnodeList);
    for (int32_t i = 0; i < num; ++i) {
      SQueryNodeAddr *qaddr = taosArrayGet(pResult->pQnodeList, i);
      qDebug("qnode %d info: id:%d", i, qaddr->nodeId);
    }
  } else {
    qDebug("empty qnode info");
  }
}

/*
prepare SQL:
create database db1;
use db1;
create stable st1 (ts timestamp, f1 int) tags(t1 int);
create table tb1 using st1 tags(1);
insert into tb1 values (now, 1);
create qnode on dnode 1;
create user user1 pass "abc";
create database db2;
grant write on db2.* to user1;
create function udf1 as '/tmp/libudf1.so' outputtype int;
create aggregate function udf2 as '/tmp/libudf2.so' outputtype int;
*/
int32_t ctgdLaunchAsyncCall(SCatalog *pCtg, SRequestConnInfo *pConn, uint64_t reqId, bool forceUpdate) {
  int32_t     code = 0;
  SCatalogReq req = {0};
  req.pTableMeta = taosArrayInit(2, sizeof(SName));
  req.pDbVgroup = taosArrayInit(2, TSDB_DB_FNAME_LEN);
  req.pDbInfo = taosArrayInit(2, TSDB_DB_FNAME_LEN);
  req.pTableHash = taosArrayInit(2, sizeof(SName));
  req.pUdf = taosArrayInit(2, TSDB_FUNC_NAME_LEN);
  req.pDbCfg = taosArrayInit(2, TSDB_DB_FNAME_LEN);
  req.pIndex = NULL;  // taosArrayInit(2, TSDB_INDEX_FNAME_LEN);
  req.pUser = taosArrayInit(2, sizeof(SUserAuthInfo));
  req.qNodeRequired = true;
  req.forceUpdate = forceUpdate;

  SName         name = {0};
  char          dbFName[TSDB_DB_FNAME_LEN] = {0};
  char          funcName[TSDB_FUNC_NAME_LEN] = {0};
  SUserAuthInfo user = {0};

  tNameFromString(&name, "1.db1.tb1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  taosArrayPush(req.pTableMeta, &name);
  taosArrayPush(req.pTableHash, &name);
  tNameFromString(&name, "1.db1.st1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  taosArrayPush(req.pTableMeta, &name);
  taosArrayPush(req.pTableHash, &name);

  strcpy(dbFName, "1.db1");
  taosArrayPush(req.pDbVgroup, dbFName);
  taosArrayPush(req.pDbCfg, dbFName);
  taosArrayPush(req.pDbInfo, dbFName);
  strcpy(dbFName, "1.db2");
  taosArrayPush(req.pDbVgroup, dbFName);
  taosArrayPush(req.pDbCfg, dbFName);
  taosArrayPush(req.pDbInfo, dbFName);

  strcpy(funcName, "udf1");
  taosArrayPush(req.pUdf, funcName);
  strcpy(funcName, "udf2");
  taosArrayPush(req.pUdf, funcName);

  strcpy(user.user, "root");
  strcpy(user.dbFName, "1.db1");
  user.type = AUTH_TYPE_READ;
  taosArrayPush(req.pUser, &user);
  user.type = AUTH_TYPE_WRITE;
  taosArrayPush(req.pUser, &user);
  user.type = AUTH_TYPE_OTHER;
  taosArrayPush(req.pUser, &user);

  strcpy(user.user, "user1");
  strcpy(user.dbFName, "1.db2");
  user.type = AUTH_TYPE_READ;
  taosArrayPush(req.pUser, &user);
  user.type = AUTH_TYPE_WRITE;
  taosArrayPush(req.pUser, &user);
  user.type = AUTH_TYPE_OTHER;
  taosArrayPush(req.pUser, &user);

  int32_t *param = taosMemoryCalloc(1, sizeof(int32_t));
  *param = 1;

  int64_t jobId = 0;

  CTG_ERR_JRET(catalogAsyncGetAllMeta(pCtg, pConn, &req, ctgdUserCallback, param, &jobId));

_return:

  taosArrayDestroy(req.pTableMeta);
  taosArrayDestroy(req.pDbVgroup);
  taosArrayDestroy(req.pTableHash);
  taosArrayDestroy(req.pUdf);
  taosArrayDestroy(req.pDbCfg);
  taosArrayDestroy(req.pUser);

  CTG_RET(code);
}
#endif

int32_t ctgdEnableDebug(char *option, bool enable) {
  if (0 == strcasecmp(option, "lock")) {
    gCTGDebug.lockEnable = enable;
    qDebug("catalog lock debug set to %d", enable);
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "cache")) {
    gCTGDebug.cacheEnable = enable;
    qDebug("catalog cache debug set to %d", enable);
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "api")) {
    gCTGDebug.apiEnable = enable;
    qDebug("catalog api debug set to %d", enable);
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "meta")) {
    gCTGDebug.metaEnable = enable;
    qDebug("catalog meta debug set to %d", enable);
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "stat")) {
    gCTGDebug.statEnable = enable;
    qDebug("catalog stat debug set to %d", enable);
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcasecmp(option, "stopUpdate")) {
    SCatalog *pCtg = NULL;
    
    void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
    while (pIter) {
      pCtg = *(SCatalog **)pIter;

      pCtg->stopUpdate = enable;
      
      pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
    }
    
    qDebug("catalog stopUpdate set to %d", enable);
    
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid debug option:%s", option);

  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgdHandleDbgCommand(char *command) {
  if (NULL == command) {
    CTG_RET(TSDB_CODE_INVALID_PARA);
  }

  if (strlen(command) > CTG_MAX_COMMAND_LEN) {
    CTG_RET(TSDB_CODE_INVALID_PARA);
  }

  char *dup = taosStrdup(command);
  char *option = NULL;
  char *param = NULL;
  
  int32_t i = 0;
  bool newItem = true;
  while (*(dup + i)) {
    if (isspace(*(dup + i))) {
      *(dup + i) = 0;
      ++i;
      newItem = true;
      continue;
    }

    if (!newItem) {
      ++i;
      continue;
    }
    
    newItem = false;
    if (NULL == option) {
      option = dup + i;
      ++i;
      continue;
    }

    if (NULL == param) {
      param = dup + i;
      ++i;
      continue;
    }

    taosMemoryFree(dup);
    CTG_RET(TSDB_CODE_INVALID_PARA);
  }

  if (NULL == param || NULL == option) {
    taosMemoryFree(dup);
    CTG_RET(TSDB_CODE_INVALID_PARA);
  }

  bool enable = atoi(param);

  int32_t code = ctgdEnableDebug(option, enable);

  taosMemoryFree(dup);

  CTG_RET(code);
}

int32_t ctgdGetOneHandle(SCatalog **pHandle) {
  SCatalog *pCtg = NULL;

  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    taosHashCancelIterate(gCtgMgmt.pCluster, pIter);
    break;
  }

  *pHandle = pCtg;

  return TSDB_CODE_SUCCESS;
}


int32_t ctgdGetStatNum(char *option, void *res) {
  if (0 == strcasecmp(option, "runtime.numOfOpDequeue")) {
    *(uint64_t *)res = atomic_load_64(&gCtgMgmt.statInfo.runtime.numOfOpDequeue);
    return TSDB_CODE_SUCCESS;
  }

  qError("invalid stat option:%s", option);

  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t ctgdGetTbMetaNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache ? (int32_t)taosHashGetSize(dbCache->tbCache) : 0;
}

int32_t ctgdGetViewNum(SCtgDBCache *dbCache) {
  return dbCache->viewCache ? (int32_t)taosHashGetSize(dbCache->viewCache) : 0;
}

int32_t ctgdGetStbNum(SCtgDBCache *dbCache) {
  return dbCache->stbCache ? (int32_t)taosHashGetSize(dbCache->stbCache) : 0;
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

int32_t ctgdGetClusterCacheNum(SCatalog *pCtg, int32_t type) {
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
    case CTG_DBG_VIEW_RENT_NUM:
      return ctgdGetRentNum(&pCtg->viewRent);
    default:
      break;
  }

  SCtgDBCache *dbCache = NULL;
  int32_t      num = 0;
  void        *pIter = taosHashIterate(pCtg->dbCache, NULL);
  while (pIter) {
    dbCache = (SCtgDBCache *)pIter;
    switch (type) {
      case CTG_DBG_META_NUM:
        num += ctgdGetTbMetaNum(dbCache);
        break;
      case CTG_DBG_STB_NUM:
        num += ctgdGetStbNum(dbCache);
        break;
      case CTG_DBG_VIEW_NUM:
        num += ctgdGetViewNum(dbCache);
      default:
        ctgError("invalid type:%d", type);
        break;
    }
    pIter = taosHashIterate(pCtg->dbCache, pIter);
  }

  return num;
}

void ctgdShowTableMeta(SCatalog *pCtg, const char *tbName, STableMeta *p) {
  if (!gCTGDebug.metaEnable) {
    return;
  }

  STableComInfo *c = &p->tableInfo;

  if (TSDB_CHILD_TABLE == p->tableType) {
    ctgDebug("table [%s] meta: type:%d, vgId:%d, uid:0x%" PRIx64 ",suid:0x%" PRIx64, tbName, p->tableType, p->vgId,
             p->uid, p->suid);
    return;
  } else {
    ctgDebug("table [%s] meta: type:%d, vgId:%d, uid:0x%" PRIx64 ",suid:0x%" PRIx64
             ",sv:%d, tv:%d, tagNum:%d, precision:%d, colNum:%d, rowSize:%d",
             tbName, p->tableType, p->vgId, p->uid, p->suid, p->sversion, p->tversion, c->numOfTags, c->precision,
             c->numOfColumns, c->rowSize);
  }

  int32_t colNum = c->numOfColumns + c->numOfTags;
  for (int32_t i = 0; i < colNum; ++i) {
    SSchema *s = &p->schema[i];
    ctgDebug("[%d] name:%s, type:%d, colId:%d, bytes:%d", i, s->name, s->type, s->colId, s->bytes);
  }
}

void ctgdShowDBCache(SCatalog *pCtg, SHashObj *dbHash) {
  if (NULL == dbHash || !gCTGDebug.cacheEnable) {
    return;
  }

  int32_t      i = 0;
  SCtgDBCache *dbCache = NULL;
  void        *pIter = taosHashIterate(dbHash, NULL);
  while (pIter) {
    char  *dbFName = NULL;
    size_t len = 0;

    dbCache = (SCtgDBCache *)pIter;

    dbFName = taosHashGetKey(pIter, &len);

    int32_t metaNum = dbCache->tbCache ? taosHashGetSize(dbCache->tbCache) : 0;
    int32_t viewNum = dbCache->viewCache ? taosHashGetSize(dbCache->viewCache) : 0;
    int32_t stbNum = dbCache->stbCache ? taosHashGetSize(dbCache->stbCache) : 0;
    int32_t vgVersion = CTG_DEFAULT_INVALID_VERSION;
    int32_t hashMethod = -1;
    int16_t hashPrefix = 0;
    int16_t hashSuffix = 0;
    int64_t stateTs = 0;
    int32_t vgNum = 0;

    if (dbCache->vgCache.vgInfo) {
      vgVersion = dbCache->vgCache.vgInfo->vgVersion;
      hashMethod = dbCache->vgCache.vgInfo->hashMethod;
      hashPrefix = dbCache->vgCache.vgInfo->hashPrefix;
      hashSuffix = dbCache->vgCache.vgInfo->hashSuffix;
      stateTs = dbCache->vgCache.vgInfo->stateTs;
      if (dbCache->vgCache.vgInfo->vgHash) {
        vgNum = taosHashGetSize(dbCache->vgCache.vgInfo->vgHash);
      }
    }

    ctgDebug("[%d] db [%.*s][0x%" PRIx64
             "] %s: metaNum:%d, viewNum:%d, stbNum:%d, vgVersion:%d, stateTs:%" PRId64 ", hashMethod:%d, prefix:%d, suffix:%d, vgNum:%d",
             i, (int32_t)len, dbFName, dbCache->dbId, dbCache->deleted ? "deleted" : "", metaNum, viewNum, stbNum, vgVersion, stateTs, 
             hashMethod, hashPrefix, hashSuffix, vgNum);

    if (dbCache->vgCache.vgInfo) {
      int32_t i = 0;
      void *pVgIter = taosHashIterate(dbCache->vgCache.vgInfo->vgHash, NULL);
      while (pVgIter) {
        SVgroupInfo * pVg = (SVgroupInfo *)pVgIter;

        ctgDebug("The %04dth VG [id:%d, hashBegin:%u, hashEnd:%u, numOfTable:%d, epNum:%d, inUse:%d]",
                 i++, pVg->vgId, pVg->hashBegin, pVg->hashEnd, pVg->numOfTable, pVg->epSet.numOfEps, pVg->epSet.inUse);

        for (int32_t n = 0; n < pVg->epSet.numOfEps; ++n) {
          SEp *pEp = &pVg->epSet.eps[n];
          ctgDebug("\tEp %d [fqdn:%s, port:%d]", n, pEp->fqdn, pEp->port);
        }
        
        pVgIter = taosHashIterate(dbCache->vgCache.vgInfo->vgHash, pVgIter);      
      }
    }

    if (dbCache->cfgCache.cfgInfo) {
      SDbCfgInfo *pCfg = dbCache->cfgCache.cfgInfo;
      ctgDebug("[%d] db [%.*s][0x%" PRIx64
               "] %s: cfgVersion:%d, numOfVgroups:%d, numOfStables:%d, buffer:%d, cacheSize:%d, pageSize:%d, pages:%d"
               ", daysPerFile:%d, daysToKeep0:%d, daysToKeep1:%d, daysToKeep2:%d, minRows:%d, maxRows:%d, walFsyncPeriod:%d"
               ", hashPrefix:%d, hashSuffix:%d, walLevel:%d, precision:%d, compression:%d, replications:%d, strict:%d"
               ", cacheLast:%d, tsdbPageSize:%d, walRetentionPeriod:%d, walRollPeriod:%d, walRetentionSize:%" PRId64 ""
               ", walSegmentSize:%" PRId64 ", numOfRetensions:%d, schemaless:%d, sstTrigger:%d",
               i, (int32_t)len, dbFName, dbCache->dbId, dbCache->deleted ? "deleted" : "", 
               pCfg->cfgVersion, pCfg->numOfVgroups, pCfg->numOfStables, pCfg->buffer,
               pCfg->cacheSize, pCfg->pageSize, pCfg->pages, pCfg->daysPerFile, pCfg->daysToKeep0,
               pCfg->daysToKeep1, pCfg->daysToKeep2, pCfg->minRows, pCfg->maxRows, pCfg->walFsyncPeriod,
               pCfg->hashPrefix, pCfg->hashSuffix, pCfg->walLevel, pCfg->precision, pCfg->compression,
               pCfg->replications, pCfg->strict, pCfg->cacheLast, pCfg->tsdbPageSize, pCfg->walRetentionPeriod,
               pCfg->walRollPeriod, pCfg->walRetentionSize, pCfg->walSegmentSize, pCfg->numOfRetensions,
               pCfg->schemaless, pCfg->sstTrigger);
    }

    ++i;
    pIter = taosHashIterate(dbHash, pIter);
  }
}

void ctgdShowClusterCache(SCatalog *pCtg) {
  if (!gCTGDebug.cacheEnable || NULL == pCtg) {
    return;
  }

  ctgDebug("## cluster 0x%" PRIx64 " %p cache Info BEGIN ##", pCtg->clusterId, pCtg);
  ctgDebug("db:%d tbmeta:%d viewmeta:%d stb:%d dbRent:%d stbRent:%d viewRent:%d", ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM),
           ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_VIEW_NUM), 
           ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_RENT_NUM), 
           ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM), ctgdGetClusterCacheNum(pCtg, CTG_DBG_VIEW_RENT_NUM));

  ctgdShowDBCache(pCtg, pCtg->dbCache);

  ctgDebug("## cluster 0x%" PRIx64 " %p cache Info END ##", pCtg->clusterId, pCtg);
}

int32_t ctgdShowStatInfo(void) {
  if (!gCTGDebug.statEnable) {
    return TSDB_CODE_CTG_OUT_OF_SERVICE;
  }

  CTG_API_ENTER();

  SCtgCacheStat cache;
  uint64_t cacheSize = 0;
  
  ctgGetGlobalCacheStat(&cache);
  ctgGetGlobalCacheSize(&cacheSize);

  qDebug("## Global Stat Info %s ##", "begin");
  qDebug("##            \t%s \t%s \t%s ##", "Num", "Hit", "Nhit");
  for (int32_t i = 0; i < CTG_CI_MAX_VALUE; ++i) {
    qDebug("#  %s \t%" PRIu64 " \t%" PRIu64 " \t%" PRIu64 " #", gCtgStatItem[i].name, cache.cacheNum[i], cache.cacheHit[i], cache.cacheNHit[i]);
  }
  qDebug("## Global Stat Info %s ##", "end");
  qDebug("## Global Cache Size: %" PRIu64, cacheSize);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}


int32_t ctgdShowCacheInfo(void) {
  if (!gCTGDebug.cacheEnable) {
    return TSDB_CODE_CTG_OUT_OF_SERVICE;
  }

  CTG_API_ENTER();

  qDebug("# total catalog cluster number %d #", taosHashGetSize(gCtgMgmt.pCluster));

  SCatalog *pCtg = NULL;
  void     *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      ctgdShowClusterCache(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}
