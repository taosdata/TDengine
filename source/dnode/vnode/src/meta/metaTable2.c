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

extern SDmNotifyHandle dmNotifyHdl;

extern int32_t metaHandleEntry(SMeta *pMeta, const SMetaEntry *pEntry);
extern int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry);
extern int32_t metaFetchEntryByName(SMeta *pMeta, const char *name, SMetaEntry **ppEntry);
extern void    metaFetchEntryFree(SMetaEntry **ppEntry);

static int32_t metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp);
static int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress);

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

  return TSDB_CODE_SUCCESS;
}

static int32_t metaCheckDropTableReq(SMeta *pMeta, int64_t version, SVDropTbReq *pReq) {
  int32_t   code = TSDB_CODE_SUCCESS;
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    return TSDB_CODE_INVALID_MSG;
  }

  code = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  if (TSDB_CODE_SUCCESS != code) {
    if (pReq->igNotExists) {
      metaTrace("vgId:%d, %s success since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
                pReq->name, version);
    } else {
      metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, pReq->name, version);
    }
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }
  pReq->uid = *(tb_uid_t *)value;
  tdbFreeClear(value);

  code = metaGetInfo(pMeta, pReq->uid, &info, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64
              " not found, this is an internal error, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->uid, version);
    code = TSDB_CODE_INTERNAL_ERROR;
    return code;
  }
  pReq->suid = info.suid;

  return code;
}

static int32_t metaCheckDropSuperTableReq(SMeta *pMeta, int64_t version, SVDropStbReq *pReq) {
  int32_t   code = TSDB_CODE_SUCCESS;
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    return TSDB_CODE_INVALID_MSG;
  }

  code = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    return TSDB_CODE_TDB_STB_NOT_EXIST;
  } else {
    int64_t uid = *(int64_t *)value;
    tdbFreeClear(value);

    if (uid != pReq->suid) {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid:%" PRId64 " not match, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->suid, version);
      return TSDB_CODE_TDB_STB_NOT_EXIST;
    }
  }

  code = metaGetInfo(pMeta, pReq->suid, &info, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64
              " not found, this is an internal error, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->suid, version);
    return TSDB_CODE_INTERNAL_ERROR;
  }
  if (info.suid != info.uid) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64 " is not a super table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->suid, version);
    return TSDB_CODE_INVALID_MSG;
  }
  return code;
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

  code = metaHandleEntry(pMeta, &entry);
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
int32_t metaDropSuperTable(SMeta *pMeta, int64_t verison, SVDropStbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckDropSuperTableReq(pMeta, verison, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // handle entry
  SMetaEntry entry = {
      .version = verison,
      .type = -TSDB_SUPER_TABLE,
      .uid = pReq->suid,
  };
  code = metaHandleEntry(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, failed to drop stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name, pReq->suid,
              tstrerror(code));
  } else {
    metaInfo("vgId:%d, super table %s uid:%" PRId64 " is dropped, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->suid, verison);
  }
  TAOS_RETURN(code);
}

// Alter Super Table

// Create Child Table
static int32_t metaCheckCreateChildTableReq(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq) {
  int32_t   code = TSDB_CODE_SUCCESS;
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  if (NULL == pReq->name || strlen(pReq->name) == 0 || NULL == pReq->ctb.stbName || strlen(pReq->ctb.stbName) == 0 ||
      pReq->ctb.suid == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s stb name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->name, pReq->ctb.stbName, version);
    return TSDB_CODE_INVALID_MSG;
  }

  // check table existence
  if (tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize) == 0) {
    pReq->uid = *(int64_t *)value;
    tdbFreeClear(value);

    if (metaGetInfo(pMeta, pReq->uid, &info, NULL) != 0) {
      metaError("vgId:%d, %s failed at %s:%d since cannot find table with uid %" PRId64
                ", which is an internal error, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->uid, version);
      return TSDB_CODE_INTERNAL_ERROR;
    }

    // check table type
    if (info.suid == info.uid || info.suid == 0) {
      metaError("vgId:%d, %s failed at %s:%d since table with uid %" PRId64 " is not a super table, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->uid, version);
      return TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
    }

    // check suid
    if (info.suid != pReq->ctb.suid) {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64 " exists in another stable with uid %" PRId64
                " instead of stable with uid %" PRId64 " version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->uid, info.suid, pReq->ctb.suid,
                version);
      return TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
    }

    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

  // check super table existence
  if (tdbTbGet(pMeta->pNameIdx, pReq->ctb.stbName, strlen(pReq->ctb.stbName) + 1, &value, &valueSize) == 0) {
    int64_t suid = *(int64_t *)value;
    tdbFreeClear(value);
    if (suid != pReq->ctb.suid) {
      metaError("vgId:%d, %s failed at %s:%d since super table %s has uid %" PRId64 " instead of %" PRId64
                ", version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, suid, pReq->ctb.suid, version);
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
  } else {
    metaError("vgId:%d, %s failed at %s:%d since super table %s does not eixst, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, version);
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
  }

  // check super table is a super table
  if (metaGetInfo(pMeta, pReq->ctb.suid, &info, NULL) != TSDB_CODE_SUCCESS) {
    metaError("vgId:%d, %s failed at %s:%d since cannot find table with uid %" PRId64
              ", which is an internal error, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.suid, version);
    return TSDB_CODE_INTERNAL_ERROR;
  } else if (info.suid != info.uid) {
    metaError("vgId:%d, %s failed at %s:%d since table with uid %" PRId64 " is not a super table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.suid, version);
    return TSDB_CODE_INVALID_MSG;
  }

  // check grant
  if (!metaTbInFilterCache(pMeta, pReq->ctb.stbName, 1)) {
    code = grantCheck(TSDB_GRANT_TIMESERIES);
    if (TSDB_CODE_SUCCESS != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
  }
  return code;
}

static int32_t metaBuildCreateChildTableRsp(SMeta *pMeta, const SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == ppRsp) {
    return terrno;
  }

  (*ppRsp)->tableType = TSDB_CHILD_TABLE;
  (*ppRsp)->tuid = pEntry->uid;
  (*ppRsp)->suid = pEntry->ctbEntry.suid;
  tstrncpy((*ppRsp)->tbName, pEntry->name, TSDB_TABLE_NAME_LEN);

  return code;
}

static int32_t metaCreateChildTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckCreateChildTableReq(pMeta, version, pReq);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    return code;
  }

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_CHILD_TABLE,
      .uid = pReq->uid,
      .name = pReq->name,
      .ctbEntry.btime = pReq->btime,
      .ctbEntry.ttlDays = pReq->ttl,
      .ctbEntry.commentLen = pReq->commentLen,
      .ctbEntry.comment = pReq->comment,
      .ctbEntry.suid = pReq->ctb.suid,
      .ctbEntry.pTags = pReq->ctb.pTag,
  };

  // build response
  code = metaBuildCreateChildTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
  }

  // handle entry
  code = metaHandleEntry(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, child table:%s uid %" PRId64 " suid:%" PRId64 " is created, version:%" PRId64,
             TD_VID(pMeta->pVnode), pReq->name, pReq->uid, pReq->ctb.suid, version);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s suid:%" PRId64 " version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name,
              pReq->ctb.suid, version);
  }
  return code;

#if 0
  metaTimeSeriesNotifyCheck(pMeta);
#endif
}

// Create Normal Table
static int32_t metaCheckCreateNormalTableReq(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq) {
  int32_t code = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    return TSDB_CODE_INVALID_MSG;
  }

  // check name
  if (tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize) == 0) {
    // for auto create table, we return the uid of the existing table
    pReq->uid = *(tb_uid_t *)value;
    tdbFree(value);
    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

  // grant check
  code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, tstrerror(code), version, pReq->name);
  }
  return code;
}

static int32_t metaBuildCreateNormalTableRsp(SMeta *pMeta, SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    return terrno;
  }

  code = metaUpdateMetaRsp(pEntry->uid, pEntry->name, &pEntry->ntbEntry.schemaRow, *ppRsp);
  if (code) {
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
    SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
    (*ppRsp)->pSchemaExt[i].colId = p->id;
    (*ppRsp)->pSchemaExt[i].compress = p->alg;
  }

  return code;
}

static int32_t metaCreateNormalTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckCreateNormalTableReq(pMeta, version, pReq);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_NORMAL_TABLE,
      .uid = pReq->uid,
      .name = pReq->name,
      .ntbEntry.btime = pReq->btime,
      .ntbEntry.ttlDays = pReq->ttl,
      .ntbEntry.commentLen = pReq->commentLen,
      .ntbEntry.comment = pReq->comment,
      .ntbEntry.schemaRow = pReq->ntb.schemaRow,
      .ntbEntry.ncid = pReq->ntb.schemaRow.pSchema[pReq->ntb.schemaRow.nCols - 1].colId + 1,
      .colCmpr = pReq->colCmpr,
  };
  TABLE_SET_COL_COMPRESSED(entry.flags);

  // build response
  code = metaBuildCreateNormalTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
  }

  // handle entry
  code = metaHandleEntry(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, normal table:%s uid %" PRId64 " is created, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->uid, version);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
  }
  TAOS_RETURN(code);
#if 0
  metaTimeSeriesNotifyCheck(pMeta);
#endif
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
  TAOS_RETURN(code);
}

int32_t metaDropTable2(SMeta *pMeta, int64_t version, SVDropTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckDropTableReq(pMeta, version, pReq);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_NOT_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  if (pReq->suid == pReq->uid) {
    code = TSDB_CODE_INVALID_PARA;
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
    TAOS_RETURN(code);
  }

  SMetaEntry entry = {
      .version = version,
      .uid = pReq->uid,
  };

  if (pReq->suid == 0) {
    entry.type = -TSDB_NORMAL_TABLE;
  } else {
    entry.type = -TSDB_CHILD_TABLE;
  }
  code = metaHandleEntry(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is dropped, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->uid, version);
  }
  TAOS_RETURN(code);
}

static int32_t metaCheckAlterTableColumnReq(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = 0;

  if (NULL == pReq->colName || strlen(pReq->colName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid column name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // check name
  void   *value = NULL;
  int32_t valueSize = 0;
  code = tdbTbGet(pMeta->pNameIdx, pReq->tbName, strlen(pReq->tbName) + 1, &value, &valueSize);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }
  int64_t uid = *(int64_t *)value;
  tdbFreeClear(value);

  // check table type
  SMetaInfo info;
  if (metaGetInfo(pMeta, uid, &info, NULL) != 0) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64
              " not found, this is an internal error in meta, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, uid, version);
    code = TSDB_CODE_INTERNAL_ERROR;
    TAOS_RETURN(code);
  }
  if (info.suid != 0) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64 " is not a normal table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, uid, version);
    code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    TAOS_RETURN(code);
  }

  // check grant
  code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, tstrerror(code), version, pReq->tbName);
    TAOS_RETURN(code);
  }
  TAOS_RETURN(code);
}

static int32_t metaAddTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckAlterTableColumnReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }
  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // do add column
  int32_t         rowSize = 0;
  SSchemaWrapper *pSchema = &pEntry->ntbEntry.schemaRow;
  SSchema        *pColumn;
  pEntry->version = version;
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    pColumn = &pSchema->pSchema[i];
    if (strncmp(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since column %s already exists in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_COL_ALREADY_EXISTS);
    }
    rowSize += pColumn->bytes;
  }

  if (rowSize + pReq->bytes > TSDB_MAX_BYTES_PER_ROW) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d + %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, rowSize, pReq->bytes, TSDB_MAX_BYTES_PER_ROW, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_ROW_LENGTH);
  }

  SSchema *pNewSchema = taosMemoryRealloc(pSchema->pSchema, sizeof(SSchema) * (pSchema->nCols + 1));
  if (NULL == pNewSchema) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(terrno), version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(terrno);
  }
  pSchema->pSchema = pNewSchema;
  pSchema->version++;
  pSchema->nCols++;
  pColumn = &pSchema->pSchema[pSchema->nCols - 1];
  pColumn->bytes = pReq->bytes;
  pColumn->type = pReq->type;
  pColumn->flags = pReq->flags;
  pColumn->colId = pEntry->ntbEntry.ncid++;
  tstrncpy(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN);
  uint32_t compress;
  if (TSDB_ALTER_TABLE_ADD_COLUMN == pReq->action) {
    compress = createDefaultColCmprByType(pColumn->type);
  } else {
    compress = pReq->compress;
  }
  code = updataTableColCmpr(&pEntry->colCmpr, pColumn, 1, compress);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  if (metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pRsp) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
      SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
      pRsp->pSchemaExt[i].colId = p->id;
      pRsp->pSchemaExt[i].compress = p->alg;
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

static int32_t metaDropTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckAlterTableColumnReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // search the column to drop
  SSchemaWrapper *pSchema = &pEntry->ntbEntry.schemaRow;
  SSchema        *pColumn = NULL;
  SSchema         tColumn;
  int32_t         iColumn = 0;
  for (; iColumn < pSchema->nCols; iColumn++) {
    pColumn = &pSchema->pSchema[iColumn];
    if (strncmp(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      break;
    }
  }

  if (iColumn == pSchema->nCols) {
    metaError("vgId:%d, %s failed at %s:%d since column %s not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  if (pColumn->colId == 0 || pColumn->flags & COL_IS_KEY) {
    metaError("vgId:%d, %s failed at %s:%d since column %s is primary key, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (tqCheckColModifiable(pMeta->pVnode->pTq, pEntry->uid, pColumn->colId) != 0) {
    metaError("vgId:%d, %s failed at %s:%d since column %s is not modifiable, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }
  tColumn = *pColumn;

  // do drop column
  pEntry->version = version;
  if (pSchema->nCols - iColumn - 1 > 0) {
    memmove(pColumn, pColumn + 1, (pSchema->nCols - iColumn - 1) * sizeof(SSchema));
  }
  pSchema->nCols--;
  pSchema->version++;
  code = updataTableColCmpr(&pEntry->colCmpr, &tColumn, 0, 0);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }
  if (pEntry->colCmpr.nCols != pSchema->nCols) {
    metaError("vgId:%d, %s failed at %s:%d since column count mismatch, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pRsp) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
      SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
      pRsp->pSchemaExt[i].colId = p->id;
      pRsp->pSchemaExt[i].compress = p->alg;
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

static int32_t metaAlterTableColumnName(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckAlterTableColumnReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  if (NULL == pReq->colNewName) {
    metaError("vgId:%d, %s failed at %s:%d since invalid new column name, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // search the column to update
  SSchemaWrapper *pSchema = &pEntry->ntbEntry.schemaRow;
  SSchema        *pColumn = NULL;
  int32_t         iColumn = 0;
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    if (strncmp(pSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      pColumn = &pSchema->pSchema[i];
      iColumn = i;
      break;
    }
  }

  if (NULL == pColumn) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colId, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  if (tqCheckColModifiable(pMeta->pVnode->pTq, pEntry->uid, pColumn->colId) != 0) {
    metaError("vgId:%d, %s failed at %s:%d since column %s is not modifiable, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pColumn->name, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_SUBSCRIBED);
  }

  // do update column name
  pEntry->version = version;
  tstrncpy(pColumn->name, pReq->colNewName, TSDB_COL_NAME_LEN);
  pSchema->version++;

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pRsp) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
      SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
      pRsp->pSchemaExt[i].colId = p->id;
      pRsp->pSchemaExt[i].compress = p->alg;
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

static int32_t metaAlterTableColumnBytes(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckAlterTableColumnReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // search the column to update
  SSchemaWrapper *pSchema = &pEntry->ntbEntry.schemaRow;
  SSchema        *pColumn = NULL;
  int32_t         iColumn = 0;
  int32_t         rowSize = 0;
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    if (strncmp(pSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      pColumn = &pSchema->pSchema[i];
      iColumn = i;
    }
    rowSize += pSchema->pSchema[i].bytes;
  }

  if (NULL == pColumn) {
    metaError("vgId:%d, %s failed at %s:%d since column %s not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  if (!IS_VAR_DATA_TYPE(pColumn->type) || pColumn->bytes >= pReq->colModBytes) {
    metaError("vgId:%d, %s failed at %s:%d since column %s is not var data type or bytes %d >= %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pColumn->bytes, pReq->colModBytes,
              version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (tqCheckColModifiable(pMeta->pVnode->pTq, pEntry->uid, pColumn->colId) != 0) {
    metaError("vgId:%d, %s failed at %s:%d since column %s is not modifiable, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_SUBSCRIBED);
  }

  if (rowSize + pReq->colModBytes - pColumn->bytes > TSDB_MAX_BYTES_PER_ROW) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d + %d - %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, rowSize, pReq->colModBytes, pColumn->bytes, TSDB_MAX_BYTES_PER_ROW,
              version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_ROW_LENGTH);
  }

  // do change the column bytes
  pEntry->version = version;
  pSchema->version++;
  pColumn->bytes = pReq->colModBytes;

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pRsp) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
      SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
      pRsp->pSchemaExt[i].colId = p->id;
      pRsp->pSchemaExt[i].compress = p->alg;
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

static int32_t metaCheckUpdateTableTagValReq(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = 0;

  // check tag name
  if (NULL == pReq->tagName || strlen(pReq->tagName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->tagName, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // check name
  void   *value = NULL;
  int32_t valueSize = 0;
  code = tdbTbGet(pMeta->pNameIdx, pReq->tbName, strlen(pReq->tbName) + 1, &value, &valueSize);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }
  tdbFreeClear(value);

  TAOS_RETURN(code);
}

static int32_t metaUpdateTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckUpdateTableTagValReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch child entry
  SMetaEntry *pChild = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pChild);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pChild->type != TSDB_CHILD_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s is not a child table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    metaFetchEntryFree(&pChild);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // fetch super entry
  SMetaEntry *pSuper = NULL;
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pChild->ctbEntry.suid, version);
    metaFetchEntryFree(&pChild);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }

  // search the tag to update
  SSchemaWrapper *pTagSchema = &pSuper->stbEntry.schemaTag;
  SSchema        *pColumn = NULL;
  int32_t         iColumn = 0;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    if (strncmp(pTagSchema->pSchema[i].name, pReq->tagName, TSDB_COL_NAME_LEN) == 0) {
      pColumn = &pTagSchema->pSchema[i];
      iColumn = i;
      break;
    }
  }

  if (NULL == pColumn) {
    metaError("vgId:%d, %s failed at %s:%d since tag %s not found in table %s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->tagName, pReq->tbName, version);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do change tag value
  pChild->version = version;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    void *pNewTag = taosMemoryRealloc(pChild->ctbEntry.pTags, pReq->nTagVal);
    if (NULL == pNewTag) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(terrno), version);
      metaFetchEntryFree(&pChild);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }
    pChild->ctbEntry.pTags = pNewTag;
    memcpy(pChild->ctbEntry.pTags, pReq->pTagVal, pReq->nTagVal);
  } else {
    STag *pOldTag = (STag *)pChild->ctbEntry.pTags;

    SArray *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
    if (NULL == pTagArray) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(terrno), version);
      metaFetchEntryFree(&pChild);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }

    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      STagVal value = {
          .type = pTagSchema->pSchema[i].type,
          .cid = pTagSchema->pSchema[i].colId,
      };

      if (iColumn == i) {
        if (pReq->isNull) {
          continue;
        }
        if (IS_VAR_DATA_TYPE(value.type)) {
          value.pData = pReq->pTagVal;
          value.nData = pReq->nTagVal;
        } else {
          memcpy(&value.i64, pReq->pTagVal, pReq->nTagVal);
        }
      } else if (!tTagGet(pOldTag, &value)) {
        continue;
      }

      if (NULL == taosArrayPush(pTagArray, &value)) {
        metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                  __LINE__, tstrerror(terrno), version);
        taosArrayDestroy(pTagArray);
        metaFetchEntryFree(&pChild);
        metaFetchEntryFree(&pSuper);
        TAOS_RETURN(terrno);
      }
    }

    STag *pNewTag = NULL;
    code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTag);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(code), version);
      taosArrayDestroy(pTagArray);
      metaFetchEntryFree(&pChild);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(code);
    }
    taosArrayDestroy(pTagArray);
    taosMemoryFree(pChild->ctbEntry.pTags);
    pChild->ctbEntry.pTags = (uint8_t *)pNewTag;
  }

  // do handle entry
  code = metaHandleEntry(pMeta, pChild);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pChild->uid, pReq->tbName, version);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pChild->uid, version);
  }

  // free resource and return
  metaFetchEntryFree(&pChild);
  metaFetchEntryFree(&pSuper);
  TAOS_RETURN(code);
}

static int32_t metaCheckUpdateTableMultiTagValueReq(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = 0;

  // check tag name
  if (NULL == pReq->pMultiTag || taosArrayGetSize(pReq->pMultiTag) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // check name
  void   *value = NULL;
  int32_t valueSize = 0;
  code = tdbTbGet(pMeta->pNameIdx, pReq->tbName, strlen(pReq->tbName) + 1, &value, &valueSize);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }
  tdbFreeClear(value);

  if (taosArrayGetSize(pReq->pMultiTag) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  TAOS_RETURN(code);
}

static int32_t metaUpdateTableMultiTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  code = metaCheckUpdateTableMultiTagValueReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch child entry
  SMetaEntry *pChild = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pChild);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pChild->type != TSDB_CHILD_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s is not a child table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    metaFetchEntryFree(&pChild);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // fetch super entry
  SMetaEntry *pSuper = NULL;
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pChild->ctbEntry.suid, version);
    metaFetchEntryFree(&pChild);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }

  // search the tags to update
  SSchemaWrapper *pTagSchema = &pSuper->stbEntry.schemaTag;

  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since table %s has no tag, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->tbName, version);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do check if tag name exists
  SHashObj *pTagTable =
      taosHashInit(pTagSchema->nCols, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pTagTable == NULL) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(terrno), version);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(terrno);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pReq->pMultiTag); i++) {
    SMultiTagUpateVal *pTagVal = taosArrayGet(pReq->pMultiTag, i);
    if (taosHashPut(pTagTable, pTagVal->tagName, strlen(pTagVal->tagName), pTagVal, sizeof(*pTagVal)) != 0) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(terrno), version);
      taosHashCleanup(pTagTable);
      metaFetchEntryFree(&pChild);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }
  }

  int32_t numOfChangedTags = 0;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    taosHashGet(pTagTable, pTagSchema->pSchema[i].name, strlen(pTagSchema->pSchema[i].name)) != NULL
        ? numOfChangedTags++
        : 0;
  }
  if (numOfChangedTags < taosHashGetSize(pTagTable)) {
    metaError("vgId:%d, %s failed at %s:%d since tag count mismatch, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    taosHashCleanup(pTagTable);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do change tag value
  pChild->version = version;
  const STag *pOldTag = (const STag *)pChild->ctbEntry.pTags;
  SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
  if (NULL == pTagArray) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(terrno), version);
    taosHashCleanup(pTagTable);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(terrno);
  }

  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    SSchema *pCol = &pTagSchema->pSchema[i];
    STagVal  value = {
         .cid = pCol->colId,
    };

    SMultiTagUpateVal *pTagVal = taosHashGet(pTagTable, pCol->name, strlen(pCol->name));
    if (pTagVal == NULL) {
      if (!tTagGet(pOldTag, &value)) {
        continue;
      }
    } else {
      value.type = pCol->type;
      if (pTagVal->isNull) {
        continue;
      }

      if (IS_VAR_DATA_TYPE(pCol->type)) {
        value.pData = pTagVal->pTagVal;
        value.nData = pTagVal->nTagVal;
      } else {
        memcpy(&value.i64, pTagVal->pTagVal, pTagVal->nTagVal);
      }
    }

    if (taosArrayPush(pTagArray, &value) == NULL) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(terrno), version);
      taosHashCleanup(pTagTable);
      taosArrayDestroy(pTagArray);
      metaFetchEntryFree(&pChild);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }
  }

  STag *pNewTag = NULL;
  code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTag);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    taosHashCleanup(pTagTable);
    taosArrayDestroy(pTagArray);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(code);
  }
  taosArrayDestroy(pTagArray);
  taosMemoryFree(pChild->ctbEntry.pTags);
  pChild->ctbEntry.pTags = (uint8_t *)pNewTag;

  // do handle entry
  code = metaHandleEntry(pMeta, pChild);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pChild->uid, pReq->tbName, version);
    taosHashCleanup(pTagTable);
    metaFetchEntryFree(&pChild);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pChild->uid, version);
  }

  taosHashCleanup(pTagTable);
  metaFetchEntryFree(&pChild);
  metaFetchEntryFree(&pSuper);
  TAOS_RETURN(code);
}

static int32_t metaCheckUpdateTableOptionsReq(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pReq->tbName == NULL || strlen(pReq->tbName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid table name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  return code;
}

static int32_t metaUpdateTableOptions(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = 0;

  code = metaCheckUpdateTableOptionsReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  // fetch entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  // do change the entry
  pEntry->version = version;
  if (pEntry->type == TSDB_CHILD_TABLE) {
    if (pReq->updateTTL) {
      pEntry->ctbEntry.ttlDays = pReq->newTTL;
    }
    if (pReq->newCommentLen >= 0) {
      char *pNewComment = NULL;
      if (pReq->newCommentLen) {
        pNewComment = taosMemoryRealloc(pEntry->ctbEntry.comment, pReq->newCommentLen + 1);
        if (NULL == pNewComment) {
          metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                    __LINE__, tstrerror(terrno), version);
          metaFetchEntryFree(&pEntry);
          TAOS_RETURN(terrno);
        }
        memcpy(pNewComment, pReq->newComment, pReq->newCommentLen + 1);
      } else {
        taosMemoryFreeClear(pEntry->ctbEntry.comment);
      }
      pEntry->ctbEntry.comment = pNewComment;
      pEntry->ctbEntry.commentLen = pReq->newCommentLen;
    }
  } else if (pEntry->type == TSDB_NORMAL_TABLE) {
    if (pReq->updateTTL) {
      pEntry->ntbEntry.ttlDays = pReq->newTTL;
    }
    if (pReq->newCommentLen >= 0) {
      char *pNewComment = NULL;
      if (pReq->newCommentLen > 0) {
        pNewComment = taosMemoryRealloc(pEntry->ntbEntry.comment, pReq->newCommentLen + 1);
        if (NULL == pNewComment) {
          metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                    __LINE__, tstrerror(terrno), version);
          metaFetchEntryFree(&pEntry);
          TAOS_RETURN(terrno);
        }
        memcpy(pNewComment, pReq->newComment, pReq->newCommentLen + 1);
      } else {
        taosMemoryFreeClear(pEntry->ntbEntry.comment);
      }
      pEntry->ntbEntry.comment = pNewComment;
      pEntry->ntbEntry.commentLen = pReq->newCommentLen;
    }
  } else {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d is invalid, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

static int32_t metaUpdateTableColCompress(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->tbName || strlen(pReq->tbName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid table name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  if (pEntry->type != TSDB_NORMAL_TABLE && pEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d is invalid, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // do change the entry
  int8_t           updated = 0;
  SColCmprWrapper *wp = &pEntry->colCmpr;
  for (int32_t i = 0; i < wp->nCols; i++) {
    SColCmpr *p = &wp->pColCmpr[i];
    if (p->id == pReq->colId) {
      uint32_t dst = 0;
      updated = tUpdateCompress(p->alg, pReq->compress, TSDB_COLVAL_COMPRESS_DISABLED, TSDB_COLVAL_LEVEL_DISABLED,
                                TSDB_COLVAL_LEVEL_MEDIUM, &dst);
      if (updated > 0) {
        p->alg = dst;
      }
    }
  }

  if (updated == 0) {
    code = TSDB_CODE_VND_COLUMN_COMPRESS_ALREADY_EXIST;
    metaError("vgId:%d, %s failed at %s:%d since column %d compress level is not changed, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colId, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else if (updated < 0) {
    code = TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
    metaError("vgId:%d, %s failed at %s:%d since column %d compress level is invalid, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colId, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }

  pEntry->version = version;

  // do handle entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaAddIndexToSuperTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid table name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->name, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    TAOS_RETURN(code);
  }

  if (pEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d is invalid, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->name, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->uid != pReq->suid) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64 " is not equal to %" PRId64
              ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pEntry->uid, pReq->suid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // if (pEntry->stbEntry.schemaTag.version >= pReq->schemaTag.version) {
  //   metaError("vgId:%d, %s failed at %s:%d since table %s tag schema version %d is not less than %d, version:%"
  //   PRId64,
  //             TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pEntry->stbEntry.schemaTag.version,
  //             pReq->schemaTag.version, version);
  //   metaFetchEntryFree(&pEntry);
  //   TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  // }

  // do change the entry
  SSchemaWrapper *pOldTagSchema = &pEntry->stbEntry.schemaTag;
  SSchemaWrapper *pNewTagSchema = &pReq->schemaTag;
  if (pOldTagSchema->nCols == 1 && pOldTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since table %s has no tag, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->name, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  if (pOldTagSchema->nCols != pNewTagSchema->nCols) {
    metaError(
        "vgId:%d, %s failed at %s:%d since table %s tag schema column count %d is not equal to %d, version:%" PRId64,
        TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pOldTagSchema->nCols, pNewTagSchema->nCols,
        version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // if (pOldTagSchema->version >= pNewTagSchema->version) {
  //   metaError("vgId:%d, %s failed at %s:%d since table %s tag schema version %d is not less than %d, version:%"
  //   PRId64,
  //             TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pOldTagSchema->version,
  //             pNewTagSchema->version, version);
  //   metaFetchEntryFree(&pEntry);
  //   TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  // }

  int32_t numOfChangedTags = 0;
  for (int32_t i = 0; i < pOldTagSchema->nCols; i++) {
    SSchema *pOldColumn = pOldTagSchema->pSchema + i;
    SSchema *pNewColumn = pNewTagSchema->pSchema + i;

    if (pOldColumn->type != pNewColumn->type || pOldColumn->colId != pNewColumn->colId ||
        strncmp(pOldColumn->name, pNewColumn->name, sizeof(pNewColumn->name))) {
      metaError("vgId:%d, %s failed at %s:%d since table %s tag schema column %d is not equal, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, i, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_INVALID_MSG);
    }

    if (IS_IDX_ON(pNewColumn) && !IS_IDX_ON(pOldColumn)) {
      numOfChangedTags++;
      SSCHMEA_SET_IDX_ON(pOldColumn);
    } else if (!IS_IDX_ON(pNewColumn) && IS_IDX_ON(pOldColumn)) {
      metaError("vgId:%d, %s failed at %s:%d since table %s tag schema column %d is not equal, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, i, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_INVALID_MSG);
    }
  }

  if (numOfChangedTags != 1) {
    metaError(
        "vgId:%d, %s failed at %s:%d since table %s tag schema column count %d is not equal to 1, version:%" PRId64,
        TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, numOfChangedTags, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  pEntry->version = version;
  pEntry->stbEntry.schemaTag.version = pNewTagSchema->version;

  // do handle the entry
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->name, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pEntry->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaDropIndexFromSuperTable(SMeta *pMeta, int64_t version, SDropIndexReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (strlen(pReq->colName) == 0 || strlen(pReq->stb) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid table name or column name, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByUid(pMeta, pReq->stbUid, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->stb, version);
    TAOS_RETURN(code);
  }

  if (TSDB_SUPER_TABLE != pEntry->type) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d is invalid, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->stb, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  SSchemaWrapper *pTagSchema = &pEntry->stbEntry.schemaTag;
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since table %s has no tag, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->stb, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // search and set the tag index off
  int32_t numOfChangedTags = 0;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    SSchema *pCol = pTagSchema->pSchema + i;
    if (0 == strncmp(pCol->name, pReq->colName, sizeof(pReq->colName))) {
      if (!IS_IDX_ON(pCol)) {
        metaError("vgId:%d, %s failed at %s:%d since table %s column %s is not indexed, version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->stb, pReq->colName, version);
        metaFetchEntryFree(&pEntry);
        TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
      }
      numOfChangedTags++;
      SSCHMEA_SET_IDX_OFF(pCol);
      break;
    }
  }

  if (numOfChangedTags != 1) {
    metaError("vgId:%d, %s failed at %s:%d since table %s column %s is not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->stb, pReq->colName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do handle the entry
  pEntry->version = version;
  pTagSchema->version++;
  code = metaHandleEntry(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->stb, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->stb,
             pEntry->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaAlterSuperTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->name || strlen(pReq->name) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid table name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->name, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    TAOS_RETURN(TSDB_CODE_TDB_STB_NOT_EXIST);
  }

  if (pEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d is invalid, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->name, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = pReq->suid,
      .name = pReq->name,
      .stbEntry.schemaRow = pReq->schemaRow,
      .stbEntry.schemaTag = pReq->schemaTag,
      .colCmpr = pReq->colCmpr,
  };
  TABLE_SET_COL_COMPRESSED(entry.flags);

  // do handle the entry
  code = metaHandleEntry(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->suid, pReq->name, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->suid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaDropMultipleTables(SMeta *pMeta, int64_t version, SArray *uidArray) {
  int32_t code = 0;

  if (taosArrayGetSize(uidArray) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < taosArrayGetSize(uidArray); i++) {
    tb_uid_t  uid = *(tb_uid_t *)taosArrayGet(uidArray, i);
    SMetaInfo info;
    code = metaGetInfo(pMeta, uid, &info, NULL);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since table uid %" PRId64 " not found, code:%d", TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, uid, code);
      return code;
    }

    SMetaEntry entry = {
        .version = version,
        .uid = uid,
    };

    if (info.suid == 0) {
      entry.type = -TSDB_NORMAL_TABLE;
    } else if (info.suid == uid) {
      entry.type = -TSDB_SUPER_TABLE;
    } else {
      entry.type = -TSDB_CHILD_TABLE;
    }
    code = metaHandleEntry(pMeta, &entry);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), uid, version);
      return code;
    }
  }
  return code;
}

int32_t metaAlterTable(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  pMeta->changed = true;
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      return metaAddTableColumn(pMeta, version, pReq, pRsp);
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      return metaDropTableColumn(pMeta, version, pReq, pRsp);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      return metaAlterTableColumnBytes(pMeta, version, pReq, pRsp);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      return metaAlterTableColumnName(pMeta, version, pReq, pRsp);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return metaUpdateTableTagValue(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL:
      return metaUpdateTableMultiTagValue(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return metaUpdateTableOptions(pMeta, version, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      return metaUpdateTableColCompress(pMeta, version, pReq);
    default:
      return terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
      break;
  }
}

static int metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs);

static int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress) {
  int32_t nCols = pWp->nCols;
  int32_t ver = pWp->version;
  if (add) {
    SColCmpr *p = taosMemoryRealloc(pWp->pColCmpr, sizeof(SColCmpr) * (nCols + 1));
    if (p == NULL) {
      return terrno;
    }
    pWp->pColCmpr = p;

    SColCmpr *pCol = p + nCols;
    pCol->id = pSchema->colId;
    pCol->alg = compress;
    pWp->nCols = nCols + 1;
    pWp->version = ver;
  } else {
    for (int32_t i = 0; i < nCols; i++) {
      SColCmpr *pOCmpr = &pWp->pColCmpr[i];
      if (pOCmpr->id == pSchema->colId) {
        int32_t left = (nCols - i - 1) * sizeof(SColCmpr);
        if (left) {
          memmove(pWp->pColCmpr + i, pWp->pColCmpr + i + 1, left);
        }
        nCols--;
        break;
      }
    }
    pWp->nCols = nCols;
    pWp->version = ver;
  }
  return 0;
}

static int metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp) {
  pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
  if (NULL == pMetaRsp->pSchemas) {
    return terrno;
  }

  pMetaRsp->pSchemaExt = taosMemoryMalloc(pSchema->nCols * sizeof(SSchemaExt));
  if (pMetaRsp->pSchemaExt == NULL) {
    taosMemoryFree(pMetaRsp->pSchemas);
    return terrno;
  }

  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  pMetaRsp->numOfColumns = pSchema->nCols;
  pMetaRsp->tableType = TSDB_NORMAL_TABLE;
  pMetaRsp->sversion = pSchema->version;
  pMetaRsp->tuid = uid;

  memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));

  return 0;
}

void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#if defined(TD_ENTERPRISE)
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      if (tsem_post(&dmNotifyHdl.sem) != 0) {
        metaError("vgId:%d, failed to post semaphore, errno:%d", TD_VID(pMeta->pVnode), errno);
      }
    }
  }
#endif
}

static int32_t metaFilterTableByHash(SMeta *pMeta, SArray *uidList) {
  int32_t code = 0;
  // 1, tranverse table's
  // 2, validate table name using vnodeValidateTableHash
  // 3, push invalidated table's uid into uidList

  TBC *pCur;
  code = tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
  if (code < 0) {
    return code;
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    tdbTbcClose(pCur);
    return code;
  }

  void *pData = NULL, *pKey = NULL;
  int   nData = 0, nKey = 0;

  while (1) {
    int32_t ret = tdbTbcNext(pCur, &pKey, &nKey, &pData, &nData);
    if (ret < 0) {
      break;
    }

    SMetaEntry me = {0};
    SDecoder   dc = {0};
    tDecoderInit(&dc, pData, nData);
    code = metaDecodeEntry(&dc, &me);
    if (code < 0) {
      tDecoderClear(&dc);
      return code;
    }

    if (me.type != TSDB_SUPER_TABLE) {
      char tbFName[TSDB_TABLE_FNAME_LEN + 1];
      snprintf(tbFName, sizeof(tbFName), "%s.%s", pMeta->pVnode->config.dbname, me.name);
      tbFName[TSDB_TABLE_FNAME_LEN] = '\0';
      int32_t ret = vnodeValidateTableHash(pMeta->pVnode, tbFName);
      if (ret < 0 && terrno == TSDB_CODE_VND_HASH_MISMATCH) {
        if (taosArrayPush(uidList, &me.uid) == NULL) {
          code = terrno;
          break;
        }
      }
    }
    tDecoderClear(&dc);
  }
  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);

  return 0;
}

int32_t metaTrimTables(SMeta *pMeta, int64_t version) {
  int32_t code = 0;

  SArray *tbUids = taosArrayInit(8, sizeof(int64_t));
  if (tbUids == NULL) {
    return terrno;
  }

  code = metaFilterTableByHash(pMeta, tbUids);
  if (code != 0) {
    goto end;
  }
  if (TARRAY_SIZE(tbUids) == 0) {
    goto end;
  }

  metaInfo("vgId:%d, trim %ld tables", TD_VID(pMeta->pVnode), taosArrayGetSize(tbUids));
  code = metaDropMultipleTables(pMeta, version, tbUids);
  if (code) goto end;

end:
  taosArrayDestroy(tbUids);

  return code;
}

int metaTtlFindExpired(SMeta *pMeta, int64_t timePointMs, SArray *tbUids, int32_t ttlDropMaxCount) {
  metaRLock(pMeta);

  int ret = ttlMgrFindExpired(pMeta->pTtlMgr, timePointMs, tbUids, ttlDropMaxCount);

  metaULock(pMeta);

  if (ret != 0) {
    metaError("ttl failed to find expired table, ret:%d", ret);
  }

  return ret;
}

static int metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs) {
  if (!tsTtlChangeOnWrite) return 0;

  if (changeTimeMs <= 0) {
    metaWarn("Skip to change ttl deletetion time on write, uid: %" PRId64, uid);
    return TSDB_CODE_VERSION_NOT_COMPATIBLE;
  }

  STtlUpdCtimeCtx ctx = {.uid = uid, .changeTimeMs = changeTimeMs, .pTxn = pMeta->txn};

  return ttlMgrUpdateChangeTime(pMeta->pTtlMgr, &ctx);
}

int metaUpdateChangeTimeWithLock(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs) {
  if (!tsTtlChangeOnWrite) return 0;

  metaWLock(pMeta);
  int ret = metaUpdateChangeTime(pMeta, uid, changeTimeMs);
  metaULock(pMeta);
  return ret;
}

int32_t metaCreateTagIdxKey(tb_uid_t suid, int32_t cid, const void *pTagData, int32_t nTagData, int8_t type,
                            tb_uid_t uid, STagIdxKey **ppTagIdxKey, int32_t *nTagIdxKey) {
  if (IS_VAR_DATA_TYPE(type)) {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + VARSTR_HEADER_SIZE + sizeof(tb_uid_t);
  } else {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + sizeof(tb_uid_t);
  }

  *ppTagIdxKey = (STagIdxKey *)taosMemoryMalloc(*nTagIdxKey);
  if (*ppTagIdxKey == NULL) {
    return terrno;
  }

  (*ppTagIdxKey)->suid = suid;
  (*ppTagIdxKey)->cid = cid;
  (*ppTagIdxKey)->isNull = (pTagData == NULL) ? 1 : 0;
  (*ppTagIdxKey)->type = type;

  // refactor
  if (IS_VAR_DATA_TYPE(type)) {
    memcpy((*ppTagIdxKey)->data, (uint16_t *)&nTagData, VARSTR_HEADER_SIZE);
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE, pTagData, nTagData);
    *(tb_uid_t *)((*ppTagIdxKey)->data + VARSTR_HEADER_SIZE + nTagData) = uid;
  } else {
    if (pTagData != NULL) memcpy((*ppTagIdxKey)->data, pTagData, nTagData);
    *(tb_uid_t *)((*ppTagIdxKey)->data + nTagData) = uid;
  }

  return 0;
}

void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey) {
  if (pTagIdxKey) taosMemoryFree(pTagIdxKey);
}

static void colCompressDebug(SHashObj *pColCmprObj) {
  void *p = taosHashIterate(pColCmprObj, NULL);
  while (p) {
    uint32_t cmprAlg = *(uint32_t *)p;
    col_id_t colId = *(col_id_t *)taosHashGetKey(p, NULL);
    p = taosHashIterate(pColCmprObj, p);

    uint8_t l1, l2, lvl;
    tcompressDebug(cmprAlg, &l1, &l2, &lvl);

    const char *l1str = columnEncodeStr(l1);
    const char *l2str = columnCompressStr(l2);
    const char *lvlstr = columnLevelStr(lvl);
    metaDebug("colId: %d, encode:%s, compress:%s,level:%s", colId, l1str, l2str, lvlstr);
  }
  return;
}

int32_t metaGetColCmpr(SMeta *pMeta, tb_uid_t uid, SHashObj **ppColCmprObj) {
  int rc = 0;

  SHashObj *pColCmprObj = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (pColCmprObj == NULL) {
    pColCmprObj = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  void      *pData = NULL;
  int        nData = 0;
  SMetaEntry e = {0};
  SDecoder   dc = {0};

  *ppColCmprObj = NULL;

  metaRLock(pMeta);
  rc = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData);
  if (rc < 0) {
    taosHashClear(pColCmprObj);
    metaULock(pMeta);
    return TSDB_CODE_FAILED;
  }
  int64_t version = ((SUidIdxVal *)pData)[0].version;
  rc = tdbTbGet(pMeta->pTbDb, &(STbDbKey){.version = version, .uid = uid}, sizeof(STbDbKey), &pData, &nData);
  if (rc < 0) {
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    metaError("failed to get table entry");
    return rc;
  }

  tDecoderInit(&dc, pData, nData);
  rc = metaDecodeEntry(&dc, &e);
  if (rc < 0) {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    return rc;
  }
  if (useCompress(e.type)) {
    SColCmprWrapper *p = &e.colCmpr;
    for (int32_t i = 0; i < p->nCols; i++) {
      SColCmpr *pCmpr = &p->pColCmpr[i];
      rc = taosHashPut(pColCmprObj, &pCmpr->id, sizeof(pCmpr->id), &pCmpr->alg, sizeof(pCmpr->alg));
      if (rc < 0) {
        tDecoderClear(&dc);
        tdbFree(pData);
        metaULock(pMeta);
        taosHashClear(pColCmprObj);
        return rc;
      }
    }
  } else {
    tDecoderClear(&dc);
    tdbFree(pData);
    metaULock(pMeta);
    taosHashClear(pColCmprObj);
    return 0;
  }
  tDecoderClear(&dc);
  tdbFree(pData);
  metaULock(pMeta);

  *ppColCmprObj = pColCmprObj;
  colCompressDebug(pColCmprObj);

  return 0;
}
// refactor later
void *metaGetIdx(SMeta *pMeta) { return pMeta->pTagIdx; }
void *metaGetIvtIdx(SMeta *pMeta) { return pMeta->pTagIvtIdx; }