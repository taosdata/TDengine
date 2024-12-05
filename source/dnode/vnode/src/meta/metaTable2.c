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

#include "meta.h"

extern int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry);

static int32_t metaCheckCreateSuperTableReq(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  int32_t   vgId = TD_VID(pMeta->pVnode);
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  // check name
  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s, version:%" PRId64, vgId, __func__, __FILE__, __LINE__,
              pReq->name, version);
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t r = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  if (r == 0) {  // name exists, check uid and type
    int64_t uid = *(tb_uid_t *)value;
    tdbFree(value);

    if (pReq->suid != uid) {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid:%" PRId64 " already exists, request uid:%" PRId64
                " version:%" PRId64,
                vgId, __func__, __FILE__, __LINE__, pReq->name, uid, pReq->suid, version);
      return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    }

    if (metaGetInfo(pMeta, uid, &info, NULL) == TSDB_CODE_NOT_FOUND) {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid:%" PRId64
                " not found, this is an internal error in meta, version:%" PRId64,
                vgId, __func__, __FILE__, __LINE__, pReq->name, uid, version);
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }

    if (info.uid == info.suid) {
      return TSDB_CODE_TDB_STB_ALREADY_EXIST;
    } else {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid:%" PRId64
                " already exists but not a super table, version:%" PRId64,
                vgId, __func__, __FILE__, __LINE__, pReq->name, uid, version);
      return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    }
  }

  // check suid
  if (metaGetInfo(pMeta, pReq->suid, &info, NULL) != TSDB_CODE_NOT_FOUND) {
    metaError("vgId:%d, %s failed at %s:%d since table with uid:%" PRId64 " already exist, name:%s version:%" PRId64,
              vgId, __func__, __FILE__, __LINE__, pReq->suid, pReq->name, version);
    return TSDB_CODE_INVALID_MSG;
  }

  // other fields
  if (pReq->schemaRow.nCols <= 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid row schema, version:%" PRId64, vgId, __func__, __FILE__,
              __LINE__, version);
    return TSDB_CODE_INVALID_MSG;
  }

  if (pReq->schemaTag.nCols <= 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag schema, version:%" PRId64, vgId, __func__, __FILE__,
              __LINE__, version);
    return TSDB_CODE_INVALID_MSG;
  }

  return TSDB_CODE_SUCCESS;
}

// Create Super Table
int32_t metaCreateSuperTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckCreateSuperTableReq(pMeta, version, pReq);
  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_TDB_STB_ALREADY_EXIST) {
      metaWarn("vgId:%d, super table %s uid:%" PRId64 " already exists, version:%" PRId64, TD_VID(pMeta->pVnode),
               pReq->name, pReq->suid, version);
      TAOS_RETURN(TSDB_CODE_SUCCESS);
    } else {
      TAOS_RETURN(code);
    }
  }

  // handle entry
  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = pReq->suid,
      .name = pReq->name,
      .stbEntry.schemaRow = pReq->schemaRow,
      .stbEntry.schemaTag = pReq->schemaTag,
  };
  if (pReq->rollup) {
    TABLE_SET_ROLLUP(entry.flags);
    entry.stbEntry.rsmaParam = pReq->rsmaParam;
  }
  if (pReq->colCmpred) {
    TABLE_SET_COL_COMPRESSED(entry.flags);
    entry.colCmpr = pReq->colCmpr;
  }

  code = metaHandleEntry2(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, super table %s suid:%" PRId64 " is created, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->suid, version);
  } else {
    metaError("vgId:%d, failed to create stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
              pReq->suid, tstrerror(code));
  }
  TAOS_RETURN(code);
}

// Drop Super Table

// Alter Super Table

// Create Child Table
static int32_t metaCreateChildTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;

#if 0
  SMetaEntry  me = {0};
  SMetaReader mr = {0};
  int32_t     ret;

  if (pReq->type == TSDB_CHILD_TABLE) {
    tb_uid_t suid = metaGetTableEntryUidByName(pMeta, pReq->ctb.stbName);
    if (suid != pReq->ctb.suid) {
      return terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
  }

  // validate req
  metaReaderDoInit(&mr, pMeta, META_READER_LOCK);
  if (metaGetTableEntryByName(&mr, pReq->name) == 0) {
    if (pReq->type == TSDB_CHILD_TABLE && pReq->ctb.suid != mr.me.ctbEntry.suid) {
      metaReaderClear(&mr);
      return terrno = TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
    }
    pReq->uid = mr.me.uid;
    if (pReq->type == TSDB_CHILD_TABLE) {
      pReq->ctb.suid = mr.me.ctbEntry.suid;
    }
    metaReaderClear(&mr);
    return terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  } else if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    terrno = TSDB_CODE_SUCCESS;
  }
  metaReaderClear(&mr);

  bool sysTbl = (pReq->type == TSDB_CHILD_TABLE) && metaTbInFilterCache(pMeta, pReq->ctb.stbName, 1);

  if (!sysTbl && ((terrno = grantCheck(TSDB_GRANT_TIMESERIES)) < 0)) goto _err;

  // build SMetaEntry
  SVnodeStats *pStats = &pMeta->pVnode->config.vndStats;
  me.version = version;
  me.type = pReq->type;
  me.uid = pReq->uid;
  me.name = pReq->name;
  me.ctbEntry.btime = pReq->btime;
  me.ctbEntry.ttlDays = pReq->ttl;
  me.ctbEntry.commentLen = pReq->commentLen;
  me.ctbEntry.comment = pReq->comment;
  me.ctbEntry.suid = pReq->ctb.suid;
  me.ctbEntry.pTags = pReq->ctb.pTag;

  ++pStats->numOfCTables;

  if (!sysTbl) {
    int32_t nCols = 0;
    ret = metaGetStbStats(pMeta->pVnode, me.ctbEntry.suid, 0, &nCols);
    if (ret < 0) {
      metaError("vgId:%d, failed to get stb stats:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
                pReq->ctb.suid, tstrerror(ret));
    }
    pStats->numOfTimeSeries += nCols - 1;
  }

  metaWLock(pMeta);
  metaUpdateStbStats(pMeta, me.ctbEntry.suid, 1, 0);
  ret = metaUidCacheClear(pMeta, me.ctbEntry.suid);
  if (ret < 0) {
    metaError("vgId:%d, failed to clear uid cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
              pReq->ctb.suid, tstrerror(ret));
  }
  ret = metaTbGroupCacheClear(pMeta, me.ctbEntry.suid);
  if (ret < 0) {
    metaError("vgId:%d, failed to clear group cache:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
              pReq->ctb.suid, tstrerror(ret));
  }
  metaULock(pMeta);

  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    ret = tsdbCacheNewTable(pMeta->pVnode->pTsdb, me.uid, me.ctbEntry.suid, NULL);
    if (ret < 0) {
      metaError("vgId:%d, failed to create table:%s since %s", TD_VID(pMeta->pVnode), pReq->name, tstrerror(ret));
      goto _err;
    }
  }

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  metaTimeSeriesNotifyCheck(pMeta);

  if (pMetaRsp) {
    *pMetaRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));

    if (*pMetaRsp) {
      (*pMetaRsp)->tableType = TSDB_CHILD_TABLE;
      (*pMetaRsp)->tuid = pReq->uid;
      (*pMetaRsp)->suid = pReq->ctb.suid;
      strcpy((*pMetaRsp)->tbName, pReq->name);
    }
  }

  pMeta->changed = true;
  metaDebug("vgId:%d, table:%s uid %" PRId64 " is created, type:%" PRId8, TD_VID(pMeta->pVnode), pReq->name, pReq->uid,
            pReq->type);
  return 0;

_err:
  metaError("vgId:%d, failed to create table:%s type:%s since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->type == TSDB_CHILD_TABLE ? "child table" : "normal table", tstrerror(terrno));
  return TSDB_CODE_FAILED;
#endif
}

// Drop Child Table

// Alter Child Table

// Create Normal Table
static int32_t metaCreateNormalTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

#if 0
  SMetaEntry  me = {0};
  SMetaReader mr = {0};
  int32_t     ret;

  // validate req
  metaReaderDoInit(&mr, pMeta, META_READER_LOCK);
  if (metaGetTableEntryByName(&mr, pReq->name) == 0) {
    if (pReq->type == TSDB_CHILD_TABLE && pReq->ctb.suid != mr.me.ctbEntry.suid) {
      metaReaderClear(&mr);
      return terrno = TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
    }
    pReq->uid = mr.me.uid;
    if (pReq->type == TSDB_CHILD_TABLE) {
      pReq->ctb.suid = mr.me.ctbEntry.suid;
    }
    metaReaderClear(&mr);
    return terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  } else if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    terrno = TSDB_CODE_SUCCESS;
  }
  metaReaderClear(&mr);

  bool sysTbl = (pReq->type == TSDB_CHILD_TABLE) && metaTbInFilterCache(pMeta, pReq->ctb.stbName, 1);

  if (!sysTbl && ((terrno = grantCheck(TSDB_GRANT_TIMESERIES)) < 0)) goto _err;

  // build SMetaEntry
  SVnodeStats *pStats = &pMeta->pVnode->config.vndStats;
  me.version = version;
  me.type = pReq->type;
  me.uid = pReq->uid;
  me.name = pReq->name;
  me.ntbEntry.btime = pReq->btime;
  me.ntbEntry.ttlDays = pReq->ttl;
  me.ntbEntry.commentLen = pReq->commentLen;
  me.ntbEntry.comment = pReq->comment;
  me.ntbEntry.schemaRow = pReq->ntb.schemaRow;
  me.ntbEntry.ncid = me.ntbEntry.schemaRow.pSchema[me.ntbEntry.schemaRow.nCols - 1].colId + 1;
  me.colCmpr = pReq->colCmpr;
  TABLE_SET_COL_COMPRESSED(me.flags);

  ++pStats->numOfNTables;
  pStats->numOfNTimeSeries += me.ntbEntry.schemaRow.nCols - 1;

  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    ret = tsdbCacheNewTable(pMeta->pVnode->pTsdb, me.uid, -1, &me.ntbEntry.schemaRow);
    if (ret < 0) {
      metaError("vgId:%d, failed to create table:%s since %s", TD_VID(pMeta->pVnode), pReq->name, tstrerror(ret));
      goto _err;
    }
  }

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  metaTimeSeriesNotifyCheck(pMeta);

  if (pMetaRsp) {
    *pMetaRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));

    if (*pMetaRsp) {
      ret = metaUpdateMetaRsp(pReq->uid, pReq->name, &pReq->ntb.schemaRow, *pMetaRsp);
      if (ret < 0) {
        metaError("vgId:%d, failed to update meta rsp:%s since %s", TD_VID(pMeta->pVnode), pReq->name, tstrerror(ret));
      }
      for (int32_t i = 0; i < pReq->colCmpr.nCols; i++) {
        SColCmpr *p = &pReq->colCmpr.pColCmpr[i];
        (*pMetaRsp)->pSchemaExt[i].colId = p->id;
        (*pMetaRsp)->pSchemaExt[i].compress = p->alg;
      }
    }
  }

  pMeta->changed = true;
  metaDebug("vgId:%d, table:%s uid %" PRId64 " is created, type:%" PRId8, TD_VID(pMeta->pVnode), pReq->name, pReq->uid,
            pReq->type);
  return 0;

_err:
  metaError("vgId:%d, failed to create table:%s type:%s since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->type == TSDB_CHILD_TABLE ? "child table" : "normal table", tstrerror(terrno));
  return TSDB_CODE_FAILED;
#endif
  return code;
}

// Drop Normal Table

// Alter Normal Table

int32_t metaCreateTable2(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (TSDB_CHILD_TABLE == pReq->type) {
    code = metaCreateChildTable(pMeta, version, pReq, ppRsp);
  } else if (TSDB_NORMAL_TABLE == pReq->type) {
    code = metaCreateNormalTable(pMeta, version, pReq, ppRsp);
  } else {
    code = TSDB_CODE_INVALID_MSG;
  }
  return code;
}
