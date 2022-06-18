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
#include "tref.h"
#include "trpc.h"

SCatalogMgmt gCtgMgmt = {0};

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

int32_t ctgGetDBVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const char* dbFName, SCtgDBCache** dbCache, SDBVgInfo **pInfo) {
  int32_t code = 0;

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, dbCache));

  if (*dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  SUseDbOutput     DbOut = {0};
  SBuildUseDBInput input = {0};

  tstrncpy(input.db, dbFName, tListLen(input.db));
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

  CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, &DbOut, NULL));

  CTG_ERR_JRET(ctgCloneVgInfo(DbOut.dbVgroup, pInfo));

  CTG_ERR_RET(ctgUpdateVgroupEnqueue(pCtg, dbFName, DbOut.dbId, DbOut.dbVgroup, false));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(*pInfo);
  *pInfo = DbOut.dbVgroup;

  CTG_RET(code);
}

int32_t ctgRefreshDBVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const char* dbFName) {
  int32_t code = 0;
  SCtgDBCache* dbCache = NULL;

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));

  SUseDbOutput     DbOut = {0};
  SBuildUseDBInput input = {0};
  tstrncpy(input.db, dbFName, tListLen(input.db));

  if (NULL != dbCache) {
    input.dbId = dbCache->dbId;

    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
  input.numOfTable = 0;

  code = ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, &DbOut, NULL);
  if (code) {
    if (CTG_DB_NOT_EXIST(code) && (NULL != dbCache)) {
      ctgDebug("db no longer exist, dbFName:%s, dbId:0x%" PRIx64, input.db, input.dbId);
      ctgDropDbCacheEnqueue(pCtg, input.db, input.dbId);
    }

    CTG_ERR_RET(code);
  }

  CTG_ERR_RET(ctgUpdateVgroupEnqueue(pCtg, dbFName, DbOut.dbId, DbOut.dbVgroup, true));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgRefreshTbMeta(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetaCtx* ctx, STableMetaOutput **pOutput, bool syncReq) {
  SVgroupInfo vgroupInfo = {0};
  int32_t     code = 0;

  if (!CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    CTG_ERR_RET(catalogGetTableHashVgroup(pCtg, pConn, ctx->pName, &vgroupInfo));
  }

  STableMetaOutput  moutput = {0};
  STableMetaOutput* output = taosMemoryCalloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    ctgDebug("will refresh tbmeta, supposed in information_schema, tbName:%s", tNameGetTableName(ctx->pName));

    CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, (char *)ctx->pName->dbname, (char *)ctx->pName->tname, output, NULL));
  } else if (CTG_FLAG_IS_STB(ctx->flag)) {
    ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(ctx->pName));

    // if get from mnode failed, will not try vnode
    CTG_ERR_JRET(ctgGetTbMetaFromMnode(pCtg, pConn, ctx->pName, output, NULL));

    if (CTG_IS_META_NULL(output->metaType)) {
      CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgroupInfo, output, NULL));
    }
  } else {
    ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName),
             ctx->flag);

    // if get from vnode failed or no table meta, will not try mnode
    CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgroupInfo, output, NULL));

    if (CTG_IS_META_TABLE(output->metaType) && TSDB_SUPER_TABLE == output->tbMeta->tableType) {
      ctgDebug("will continue to refresh tbmeta since got stb, tbName:%s", tNameGetTableName(ctx->pName));

      taosMemoryFreeClear(output->tbMeta);
      
      CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, output->dbFName, output->tbName, output, NULL));
    } else if (CTG_IS_META_BOTH(output->metaType)) {
      int32_t exist = 0;
      if (!CTG_FLAG_IS_FORCE_UPDATE(ctx->flag)) {
        CTG_ERR_JRET(ctgTbMetaExistInCache(pCtg, output->dbFName, output->tbName, &exist));
      }

      if (0 == exist) {
        CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, output->dbFName, output->tbName, &moutput, NULL));

        if (CTG_IS_META_NULL(moutput.metaType)) {
          SET_META_TYPE_NULL(output->metaType);
        }

        taosMemoryFreeClear(output->tbMeta);
        output->tbMeta = moutput.tbMeta;
        moutput.tbMeta = NULL;
      } else {
        taosMemoryFreeClear(output->tbMeta);

        SET_META_TYPE_CTABLE(output->metaType);
      }
    }
  }

  if (CTG_IS_META_NULL(output->metaType)) {
    ctgError("no tbmeta got, tbNmae:%s", tNameGetTableName(ctx->pName));
    ctgRemoveTbMetaFromCache(pCtg, ctx->pName, false);
    CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
  }

  if (CTG_IS_META_TABLE(output->metaType)) {
    ctgDebug("tbmeta got, dbFName:%s, tbName:%s, tbType:%d", output->dbFName, output->tbName,
             output->tbMeta->tableType);
  } else {
    ctgDebug("tbmeta got, dbFName:%s, tbName:%s, tbType:%d, stbMetaGot:%d", output->dbFName, output->ctbName,
             output->ctbMeta.tableType, CTG_IS_META_BOTH(output->metaType));
  }

  if (pOutput) {
    CTG_ERR_JRET(ctgCloneMetaOutput(output, pOutput));
  }

  CTG_ERR_JRET(ctgUpdateTbMetaEnqueue(pCtg, output, syncReq));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(output->tbMeta);
  taosMemoryFreeClear(output);

  CTG_RET(code);
}

int32_t ctgGetTbMetaFromCache(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  if (CTG_IS_SYS_DBNAME(ctx->pName->dbname)) {
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

int32_t ctgGetTbMeta(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  int32_t code = 0;
  STableMetaOutput *output = NULL;

  CTG_ERR_RET(ctgGetTbMetaFromCache(pCtg, pConn, ctx, pTableMeta));
  if (*pTableMeta) {
    goto _return;
  }

  while (true) {
    CTG_ERR_JRET(ctgRefreshTbMeta(pCtg, pConn, ctx, &output, false));

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
      taosMemoryFreeClear(output->tbMeta);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    // HANDLE ONLY CHILD TABLE META

    taosMemoryFreeClear(output->tbMeta);

    SName stbName = *ctx->pName;
    strcpy(stbName.tname, output->tbName);
    SCtgTbMetaCtx stbCtx = {0};
    stbCtx.flag = ctx->flag;
    stbCtx.pName = &stbName;

    CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, pTableMeta));
    if (NULL == *pTableMeta) {
      ctgDebug("stb no longer exist, dbFName:%s, tbName:%s", output->dbFName, ctx->pName->tname);
      continue;
    }

    memcpy(*pTableMeta, &output->ctbMeta, sizeof(output->ctbMeta));

    break;
  }

_return:

  if (CTG_TABLE_NOT_EXIST(code) && ctx->tbInfo.inCache) {
    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
      strcpy(dbFName, ctx->pName->dbname);
    } else {
      tNameGetFullDbName(ctx->pName, dbFName);
    }

    if (TSDB_SUPER_TABLE == ctx->tbInfo.tbType) {
      ctgDropStbMetaEnqueue(pCtg, dbFName, ctx->tbInfo.dbId, ctx->pName->tname, ctx->tbInfo.suid, false);
    } else {
      ctgDropTbMetaEnqueue(pCtg, dbFName, ctx->tbInfo.dbId, ctx->pName->tname, false);
    }
  }

  taosMemoryFreeClear(output);

  if (*pTableMeta) {
    ctgDebug("tbmeta returned, tbName:%s, tbType:%d", ctx->pName->tname, (*pTableMeta)->tableType);
    ctgdShowTableMeta(pCtg, ctx->pName->tname, *pTableMeta);
  }

  CTG_RET(code);
}

int32_t ctgUpdateTbMeta(SCatalog* pCtg, STableMetaRsp* rspMsg, bool syncOp) {
  STableMetaOutput* output = taosMemoryCalloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  int32_t code = 0;

  strcpy(output->dbFName, rspMsg->dbFName);
  strcpy(output->tbName, rspMsg->tbName);

  output->dbId = rspMsg->dbId;

  SET_META_TYPE_TABLE(output->metaType);

  CTG_ERR_JRET(queryCreateTableMetaFromMsg(rspMsg, rspMsg->tableType == TSDB_SUPER_TABLE, &output->tbMeta));

  CTG_ERR_JRET(ctgUpdateTbMetaEnqueue(pCtg, output, syncOp));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(output->tbMeta);
  taosMemoryFreeClear(output);

  CTG_RET(code);
}

int32_t ctgChkAuth(SCatalog* pCtg, SRequestConnInfo *pConn, const char* user, const char* dbFName, AUTH_TYPE type, bool *pass) {
  bool inCache = false;
  int32_t code = 0;

  *pass = false;
  
  CTG_ERR_RET(ctgChkAuthFromCache(pCtg, (char*)user, (char*)dbFName, type, &inCache, pass));

  if (inCache) {
    return TSDB_CODE_SUCCESS;
  }

  SGetUserAuthRsp authRsp = {0};
  CTG_ERR_RET(ctgGetUserDbAuthFromMnode(pCtg, pConn, user, &authRsp, NULL));
  
  if (authRsp.superAuth) {
    *pass = true;
    goto _return;
  }

  if (authRsp.createdDbs && taosHashGet(authRsp.createdDbs, dbFName, strlen(dbFName))) {
    *pass = true;
    goto _return;
  }

  if (type == AUTH_TYPE_READ && authRsp.readDbs && taosHashGet(authRsp.readDbs, dbFName, strlen(dbFName))) {
    *pass = true;
  } else if (type == AUTH_TYPE_WRITE && authRsp.writeDbs && taosHashGet(authRsp.writeDbs, dbFName, strlen(dbFName))) {
    *pass = true;
  }

_return:

  ctgUpdateUserEnqueue(pCtg, &authRsp, false);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbIndex(SCatalog* pCtg, SRequestConnInfo *pConn, SName* pTableName, SArray** pRes) {
  CTG_ERR_RET(ctgReadTbIndexFromCache(pCtg, pTableName, pRes));
  if (*pRes) {
    return TSDB_CODE_SUCCESS;
  }

  STableIndex *pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  int32_t code = ctgGetTbIndexFromMnode(pCtg, pConn, (SName*)pTableName, pIndex, NULL);
  if (TSDB_CODE_MND_DB_INDEX_NOT_EXIST == code) {
    code = 0;
    goto _return;
  }
  CTG_ERR_JRET(code);
  
  SArray* pInfo = NULL;
  CTG_ERR_JRET(ctgCloneTableIndex(pIndex->pIndex, &pInfo));

  *pRes = pInfo;

  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pCtg, &pIndex, false));

  return TSDB_CODE_SUCCESS;

_return:

  tFreeSTableIndexRsp(pIndex);
  taosMemoryFree(pIndex);

  taosArrayDestroyEx(*pRes, tFreeSTableIndexInfo);
  *pRes = NULL;

  CTG_RET(code);
}


int32_t ctgGetTbDistVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, SName* pTableName, SArray** pVgList) {
  STableMeta *tbMeta = NULL;
  int32_t code = 0;
  SVgroupInfo vgroupInfo = {0};
  SCtgDBCache* dbCache = NULL;
  SArray *vgList = NULL;
  SDBVgInfo *vgInfo = NULL;
  SCtgTbMetaCtx ctx = {0};
  ctx.pName = pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;

  *pVgList = NULL;
  
  CTG_ERR_JRET(ctgGetTbMeta(pCtg, pConn, &ctx, &tbMeta));

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  SHashObj *vgHash = NULL;  
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, db, &dbCache, &vgInfo));

  if (dbCache) {
    vgHash = dbCache->vgCache.vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, pVgList));
  } else {
    // USE HASH METHOD INSTEAD OF VGID IN TBMETA
    ctgError("invalid method to get none stb vgInfo, tbType:%d", tbMeta->tableType);
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);

#if 0  
    int32_t vgId = tbMeta->vgId;
    if (taosHashGetDup(vgHash, &vgId, sizeof(vgId), &vgroupInfo) != 0) {
      ctgWarn("table's vgId not found in vgroup list, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_VG_META_MISMATCH);
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
#endif
  }

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  taosMemoryFreeClear(tbMeta);

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    taosMemoryFreeClear(vgInfo);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }

  CTG_RET(code);
}

int32_t catalogInit(SCatalogCfg* cfg) {
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

  gCtgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT),
                                   false, HASH_ENTRY_LOCK);
  if (NULL == gCtgMgmt.pCluster) {
    qError("taosHashInit %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (tsem_init(&gCtgMgmt.queue.reqSem, 0, 0)) {
    qError("tsem_init failed, error:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    CTG_ERR_RET(TSDB_CODE_CTG_SYS_ERROR);
  }

  gCtgMgmt.queue.head = taosMemoryCalloc(1, sizeof(SCtgQNode));
  if (NULL == gCtgMgmt.queue.head) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  gCtgMgmt.queue.tail = gCtgMgmt.queue.head;

  gCtgMgmt.jobPool = taosOpenRef(200, ctgFreeJob);
  if (gCtgMgmt.jobPool < 0) {
    qError("taosOpenRef failed, error:%s", tstrerror(terrno));
    CTG_ERR_RET(terrno);
  }

  CTG_ERR_RET(ctgStartUpdateThread());

  qDebug("catalog initialized, maxDb:%u, maxTbl:%u, dbRentSec:%u, stbRentSec:%u", gCtgMgmt.cfg.maxDBCacheNum,
         gCtgMgmt.cfg.maxTblCacheNum, gCtgMgmt.cfg.dbRentSec, gCtgMgmt.cfg.stbRentSec);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(uint64_t clusterId, SCatalog** catalogHandle) {
  if (NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == gCtgMgmt.pCluster) {
    qError("catalog cluster cache are not ready, clusterId:0x%" PRIx64, clusterId);
    CTG_ERR_RET(TSDB_CODE_CTG_NOT_READY);
  }

  int32_t   code = 0;
  SCatalog* clusterCtg = NULL;

  while (true) {
    SCatalog** ctg = (SCatalog**)taosHashGet(gCtgMgmt.pCluster, (char*)&clusterId, sizeof(clusterId));

    if (ctg && (*ctg)) {
      *catalogHandle = *ctg;
      qDebug("got catalog handle from cache, clusterId:0x%" PRIx64 ", CTG:%p", clusterId, *ctg);
      return TSDB_CODE_SUCCESS;
    }

    clusterCtg = taosMemoryCalloc(1, sizeof(SCatalog));
    if (NULL == clusterCtg) {
      qError("calloc %d failed", (int32_t)sizeof(SCatalog));
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    clusterCtg->clusterId = clusterId;

    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->dbRent, gCtgMgmt.cfg.dbRentSec, CTG_RENT_DB));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->stbRent, gCtgMgmt.cfg.stbRentSec, CTG_RENT_STABLE));

    clusterCtg->dbCache = taosHashInit(gCtgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                       false, HASH_ENTRY_LOCK);
    if (NULL == clusterCtg->dbCache) {
      qError("taosHashInit %d dbCache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    code = taosHashPut(gCtgMgmt.pCluster, &clusterId, sizeof(clusterId), &clusterCtg, POINTER_BYTES);
    if (code) {
      if (HASH_NODE_EXIST(code)) {
        ctgFreeHandle(clusterCtg);
        continue;
      }

      qError("taosHashPut CTG to cache failed, clusterId:0x%" PRIx64, clusterId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("add CTG to cache, clusterId:0x%" PRIx64 ", CTG:%p", clusterId, clusterCtg);

    break;
  }

  *catalogHandle = clusterCtg;

  CTG_CACHE_STAT_INC(clusterNum, 1);

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
    ctgWarn("taosHashRemove from cluster failed, may already be freed, clusterId:0x%" PRIx64, pCtg->clusterId);
    return;
  }

  CTG_CACHE_STAT_DEC(clusterNum, 1);

  uint64_t clusterId = pCtg->clusterId;

  ctgFreeHandle(pCtg);

  ctgInfo("handle freed, culsterId:0x%" PRIx64, clusterId);
}

int32_t catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t* tableNum) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == version || NULL == dbId) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;

  CTG_ERR_JRET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  *version = dbCache->vgCache.vgInfo->vgVersion;
  *dbId = dbCache->dbId;
  *tableNum = dbCache->vgCache.vgInfo->numOfTable;

  ctgReleaseVgInfoToCache(pCtg, dbCache);

  ctgDebug("Got db vgVersion from cache, dbFName:%s, vgVersion:%d", dbFName, *version);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetDBVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const char* dbFName, SArray** vgroupList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == pConn || NULL == vgroupList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t code = 0;
  SArray *vgList = NULL;
  SHashObj *vgHash = NULL;
  SDBVgInfo *vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, dbFName, &dbCache, &vgInfo));
  if (dbCache) {
    vgHash = dbCache->vgCache.vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, &vgList));

  *vgroupList = vgList;
  vgList = NULL;

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    taosMemoryFreeClear(vgInfo);
  }

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateDBVgInfo(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDBVgInfo* dbInfo) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == dbInfo) {
    ctgFreeVgInfo(dbInfo);
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  code = ctgUpdateVgroupEnqueue(pCtg, dbFName, dbId, dbInfo, false);

_return:

  CTG_API_LEAVE(code);
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

  CTG_ERR_JRET(ctgDropDbCacheEnqueue(pCtg, dbFName, dbId));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateVgEpSet(SCatalog* pCtg, const char* dbFName, int32_t vgId, SEpSet* epSet) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == epSet) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_ERR_JRET(ctgUpdateVgEpsetEnqueue(pCtg, (char*)dbFName, vgId, epSet));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateTableIndex(SCatalog* pCtg, STableIndexRsp *pRsp) {
  CTG_API_ENTER();

  int32_t code = 0;
  
  if (NULL == pCtg || NULL == pRsp) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  STableIndex* pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_API_LEAVE(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(pIndex, pRsp, sizeof(STableIndex));

  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pCtg, &pIndex, false));

_return:
  
  CTG_API_LEAVE(code);
}


int32_t catalogRemoveTableMeta(SCatalog* pCtg, SName* pTableName) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == pTableName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgRemoveTbMetaFromCache(pCtg, pTableName, true));

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

  CTG_ERR_JRET(ctgDropStbMetaEnqueue(pCtg, dbFName, dbId, stbName, suid, true));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetTableMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;
  
  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogGetSTableMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_STB;

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogUpdateTableMeta(SCatalog* pCtg, STableMetaRsp* pMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pMsg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateTbMeta(pCtg, pMsg, true));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogChkTbMetaVersion(SCatalog* pCtg, SRequestConnInfo *pConn, SArray* pTables) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTables) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SName   name;
  int32_t sver = 0;
  int32_t tver = 0;
  int32_t tbNum = taosArrayGetSize(pTables);
  for (int32_t i = 0; i < tbNum; ++i) {
    STbSVersion* pTb = (STbSVersion*)taosArrayGet(pTables, i);
    if (NULL == pTb->tbFName || 0 == pTb->tbFName[0]) {
      continue;
    }

    tNameFromString(&name, pTb->tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

    if (CTG_IS_SYS_DBNAME(name.dbname)) {
      continue;
    }

    int32_t  tbType = 0;
    uint64_t suid = 0;
    char     stbName[TSDB_TABLE_FNAME_LEN];
    ctgReadTbVerFromCache(pCtg, &name, &sver, &tver, &tbType, &suid, stbName);
    if ((sver >= 0 && sver < pTb->sver) || (tver >= 0 && tver < pTb->tver)) {
      switch (tbType) {
        case TSDB_CHILD_TABLE: {
          SName stb = name;
          strcpy(stb.tname, stbName);
          catalogRemoveTableMeta(pCtg, &stb);
          break;
        }
        case TSDB_SUPER_TABLE:
        case TSDB_NORMAL_TABLE:
          catalogRemoveTableMeta(pCtg, &name);
          break;
        default:
          ctgError("ignore table type %d", tbType);
          break;
      }
    }
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogRefreshDBVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const char* dbFName) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == dbFName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgRefreshDBVgInfo(pCtg, pConn, dbFName));
}

int32_t catalogRefreshTableMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, int32_t isSTable) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable);

  CTG_API_LEAVE(ctgRefreshTbMeta(pCtg, pConn, &ctx, NULL, true));
}

int32_t catalogRefreshGetTableMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable);

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogGetTableDistVgInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, SArray** pVgList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pVgList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (CTG_IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetTbDistVgInfo(pCtg, pConn, (SName*)pTableName, pVgList));
}

int32_t catalogGetTableHashVgroup(SCatalog *pCtg, SRequestConnInfo *pConn, const SName *pTableName, SVgroupInfo *pVgroup) {
  CTG_API_ENTER();

  if (CTG_IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  char         db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  SDBVgInfo *vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, db, &dbCache, &vgInfo));

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, vgInfo ? vgInfo : dbCache->vgCache.vgInfo, pTableName, pVgroup));

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    taosHashCleanup(vgInfo->vgHash);
    taosMemoryFreeClear(vgInfo);
  }

  CTG_API_LEAVE(code);
}

int32_t catalogGetAllMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const SCatalogReq* pReq, SMetaData* pRsp) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pReq || NULL == pRsp) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  pRsp->pTableMeta = NULL;

  if (pReq->pTableMeta) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableMeta);
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
      SName*        name = taosArrayGet(pReq->pTableMeta, i);
      STableMeta*   pTableMeta = NULL;
      SCtgTbMetaCtx ctx = {0};
      ctx.pName = name;
      ctx.flag = CTG_FLAG_UNKNOWN_STB;
      
      CTG_ERR_JRET(ctgGetTbMeta(pCtg, pConn, &ctx, &pTableMeta));

      if (NULL == taosArrayPush(pRsp->pTableMeta, &pTableMeta)) {
        ctgError("taosArrayPush failed, idx:%d", i);
        taosMemoryFreeClear(pTableMeta);
        CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
      }
    }
  }

  if (pReq->qNodeRequired) {
    pRsp->pQnodeList = taosArrayInit(10, sizeof(SQueryNodeLoad));
    CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pConn, pRsp->pQnodeList, NULL));
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  if (pRsp->pTableMeta) {
    int32_t aSize = taosArrayGetSize(pRsp->pTableMeta);
    for (int32_t i = 0; i < aSize; ++i) {
      STableMeta* pMeta = taosArrayGetP(pRsp->pTableMeta, i);
      taosMemoryFreeClear(pMeta);
    }

    taosArrayDestroy(pRsp->pTableMeta);
    pRsp->pTableMeta = NULL;
  }

  CTG_API_LEAVE(code);
}

int32_t catalogAsyncGetAllMeta(SCatalog* pCtg, SRequestConnInfo *pConn, uint64_t reqId, const SCatalogReq* pReq, catalogCallback fp, void* param, int64_t* jobId) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pReq || NULL == fp || NULL == param) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0, taskNum = 0;
  SCtgJob *pJob = NULL;
  CTG_ERR_JRET(ctgInitJob(pCtg, pConn, &pJob, reqId, pReq, fp, param, &taskNum));
  if (taskNum <= 0) {
    SMetaData* pMetaData = taosMemoryCalloc(1, sizeof(SMetaData));
    fp(pMetaData, param, TSDB_CODE_SUCCESS);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgLaunchJob(pJob));

  // NOTE: here the assignment of jobId is invalid, may over-write the true scheduler created query job.
  //  *jobId = pJob->refId;

_return:
  if (pJob) {
    taosReleaseRef(gCtgMgmt.jobPool, pJob->refId);

    if (code) {
      taosRemoveRef(gCtgMgmt.jobPool, pJob->refId);
    }
  }

  CTG_API_LEAVE(code);
}

int32_t catalogGetQnodeList(SCatalog* pCtg, SRequestConnInfo *pConn, SArray* pQnodeList) {
  CTG_API_ENTER();

  int32_t code = 0;
  if (NULL == pCtg || NULL == pConn || NULL == pQnodeList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pConn, pQnodeList, NULL));

_return:

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetExpiredSTables(SCatalog* pCtg, SSTableVersion **stables, uint32_t *num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == stables || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->stbRent, (void **)stables, num, sizeof(SSTableVersion)));
}

int32_t catalogGetExpiredDBs(SCatalog* pCtg, SDbVgVersion** dbs, uint32_t* num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbs || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->dbRent, (void**)dbs, num, sizeof(SDbVgVersion)));
}

int32_t catalogGetExpiredUsers(SCatalog* pCtg, SUserAuthVersion** users, uint32_t* num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == users || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  *num = taosHashGetSize(pCtg->userCache);
  if (*num > 0) {
    *users = taosMemoryCalloc(*num, sizeof(SUserAuthVersion));
    if (NULL == *users) {
      ctgError("calloc %d userAuthVersion failed", *num);
      CTG_API_LEAVE(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  uint32_t      i = 0;
  SCtgUserAuth* pAuth = taosHashIterate(pCtg->userCache, NULL);
  while (pAuth != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(pAuth, &len);
    strncpy((*users)[i].user, key, len);
    (*users)[i].user[len] = 0;
    (*users)[i].version = pAuth->version;
    ++i;
    pAuth = taosHashIterate(pCtg->userCache, pAuth);
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetDBCfg(SCatalog* pCtg, SRequestConnInfo *pConn, const char* dbFName, SDbCfgInfo* pDbCfg) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == pConn || NULL == dbFName || NULL == pDbCfg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetDBCfgFromMnode(pCtg, pConn, dbFName, pDbCfg, NULL));
}

int32_t catalogGetIndexMeta(SCatalog* pCtg, SRequestConnInfo *pConn, const char* indexName, SIndexInfo* pInfo) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == pConn || NULL == indexName || NULL == pInfo) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetIndexInfoFromMnode(pCtg, pConn, indexName, pInfo, NULL));
}

int32_t catalogGetTableIndex(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, SArray** pRes) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pRes) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetTbIndex(pCtg, pConn, (SName*)pTableName, pRes));
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetUdfInfo(SCatalog* pCtg, SRequestConnInfo *pConn, const char* funcName, SFuncInfo* pInfo) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == pConn || NULL == funcName || NULL == pInfo) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetUdfInfoFromMnode(pCtg, pConn, funcName, pInfo, NULL));
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogChkAuth(SCatalog* pCtg, SRequestConnInfo *pConn, const char* user, const char* dbFName, AUTH_TYPE type, bool *pass) {
  CTG_API_ENTER();
  
  if (NULL == pCtg || NULL == pConn || NULL == user || NULL == dbFName || NULL == pass) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgChkAuth(pCtg, pConn, user, dbFName, type, pass));
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateUserAuthInfo(SCatalog* pCtg, SGetUserAuthRsp* pAuth) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pAuth) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgUpdateUserEnqueue(pCtg, pAuth, false));
}

int32_t catalogClearCache(void) {
  CTG_API_ENTER();

  qInfo("start to clear catalog cache");

  if (NULL == gCtgMgmt.pCluster || atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  int32_t code = ctgClearCacheEnqueue(NULL, true);

  qInfo("clear catalog cache end, code: %s", tstrerror(code));
  
  CTG_API_LEAVE(code);
}


void catalogDestroy(void) {
  qInfo("start to destroy catalog");

  if (NULL == gCtgMgmt.pCluster || atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
    return;
  }

  atomic_store_8((int8_t*)&gCtgMgmt.exit, true);

  if (tsem_post(&gCtgMgmt.queue.reqSem)) {
    qError("tsem_post failed, error:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
  }

  while (CTG_IS_LOCKED(&gCtgMgmt.lock)) {
    taosUsleep(1);
  }

  CTG_LOCK(CTG_WRITE, &gCtgMgmt.lock);

  SCatalog* pCtg = NULL;
  void*     pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog**)pIter;

    if (pCtg) {
      catalogFreeHandle(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  taosHashCleanup(gCtgMgmt.pCluster);
  gCtgMgmt.pCluster = NULL;

  if (CTG_IS_LOCKED(&gCtgMgmt.lock) == TD_RWLATCH_WRITE_FLAG_COPY) CTG_UNLOCK(CTG_WRITE, &gCtgMgmt.lock);

  qInfo("catalog destroyed");
}
