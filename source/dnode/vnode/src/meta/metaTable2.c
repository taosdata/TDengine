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
extern int32_t metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp);

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
  code = metaHandleEntry2(pMeta, &entry);
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
  code = metaHandleEntry2(pMeta, &entry);
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

// Drop Child Table

// Alter Child Table

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
  code = metaHandleEntry2(pMeta, &entry);
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
  code = metaHandleEntry2(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is dropped, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
             pReq->uid, version);
  }
  TAOS_RETURN(code);
}

int32_t metaAddTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  if (NULL == pReq->colName || strlen(pReq->colName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid column name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

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

  // TODO
  return code;

#if 0
  void           *pVal = NULL;
  int             nVal = 0;
  const void     *pData = NULL;
  int             nData = 0;
  int             ret = 0;
  tb_uid_t        uid;
  int64_t         oversion;
  SSchema        *pColumn = NULL;
  SMetaEntry      entry = {0};
  SSchemaWrapper *pSchema;
  int             c;
  bool            freeColCmpr = false;

  // search uid index
  TBC *pUidIdxc = NULL;

  TAOS_CHECK_RETURN(tdbTbcOpen(pMeta->pUidIdx, &pUidIdxc, NULL));
  ret = tdbTbcMoveTo(pUidIdxc, &uid, sizeof(uid), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    metaError("meta/table: invalide c: %" PRId32 " alt tb column failed.", c);
    return TSDB_CODE_FAILED;
  }

  ret = tdbTbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  oversion = ((SUidIdxVal *)pData)[0].version;

  // search table.db
  TBC *pTbDbc = NULL;

  TAOS_CHECK_RETURN(tdbTbcOpen(pMeta->pTbDb, &pTbDbc, NULL));
  ret = tdbTbcMoveTo(pTbDbc, &((STbDbKey){.uid = uid, .version = oversion}), sizeof(STbDbKey), &c);
  if (c != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    metaError("meta/table: invalide c: %" PRId32 " alt tb column failed.", c);
    return TSDB_CODE_FAILED;
  }

  ret = tdbTbcGet(pTbDbc, NULL, NULL, &pData, &nData);

  // get table entry
  SDecoder dc = {0};
  if ((entry.pBuf = taosMemoryMalloc(nData)) == NULL) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    return terrno;
  }
  memcpy(entry.pBuf, pData, nData);
  tDecoderInit(&dc, entry.pBuf, nData);
  ret = metaDecodeEntry(&dc, &entry);
  if (ret != 0) {
    tdbTbcClose(pUidIdxc);
    tdbTbcClose(pTbDbc);
    tDecoderClear(&dc);
    metaError("meta/table: invalide ret: %" PRId32 " alt tb column failed.", ret);
    return ret;
  }

  if (entry.type != TSDB_NORMAL_TABLE) {
    terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _err;
  }
  // search the column to add/drop/update
  pSchema = &entry.ntbEntry.schemaRow;

  // save old entry
  SMetaEntry oldEntry = {.type = TSDB_NORMAL_TABLE, .uid = entry.uid};
  oldEntry.ntbEntry.schemaRow.nCols = pSchema->nCols;

  int32_t rowLen = -1;
  if (pAlterTbReq->action == TSDB_ALTER_TABLE_ADD_COLUMN ||
      pAlterTbReq->action == TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES) {
    rowLen = 0;
  }

  int32_t  iCol = 0, jCol = 0;
  SSchema *qColumn = NULL;
  for (;;) {
    qColumn = NULL;

    if (jCol >= pSchema->nCols) break;
    qColumn = &pSchema->pSchema[jCol];

    if (!pColumn && (strcmp(qColumn->name, pAlterTbReq->colName) == 0)) {
      pColumn = qColumn;
      iCol = jCol;
      if (rowLen < 0) break;
    }
    rowLen += qColumn->bytes;
    ++jCol;
  }

  entry.version = version;
  int      tlen;
  SSchema *pNewSchema = NULL;
  SSchema  tScheam;
  switch (pAlterTbReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      if (pColumn) {
        terrno = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _err;
      }
      if ((terrno = grantCheck(TSDB_GRANT_TIMESERIES)) < 0) {
        goto _err;
      }
      if (rowLen + pAlterTbReq->bytes > TSDB_MAX_BYTES_PER_ROW) {
        terrno = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
        goto _err;
      }
      pSchema->version++;
      pSchema->nCols++;
      pNewSchema = taosMemoryMalloc(sizeof(SSchema) * pSchema->nCols);
      if (pNewSchema == NULL) {
        goto _err;
      }
      memcpy(pNewSchema, pSchema->pSchema, sizeof(SSchema) * (pSchema->nCols - 1));
      pSchema->pSchema = pNewSchema;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].bytes = pAlterTbReq->bytes;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].type = pAlterTbReq->type;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].flags = pAlterTbReq->flags;
      pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].colId = entry.ntbEntry.ncid++;
      strcpy(pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].name, pAlterTbReq->colName);

      ++pMeta->pVnode->config.vndStats.numOfNTimeSeries;
      metaTimeSeriesNotifyCheck(pMeta);

      if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
        int16_t cid = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].colId;
        int8_t  col_type = pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1].type;
        int32_t ret = tsdbCacheNewNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, col_type);
        if (ret < 0) {
          terrno = ret;
          goto _err;
        }
      }
      SSchema *pCol = &pSchema->pSchema[entry.ntbEntry.schemaRow.nCols - 1];
      uint32_t compress = pAlterTbReq->action == TSDB_ALTER_TABLE_ADD_COLUMN ? createDefaultColCmprByType(pCol->type)
                                                                             : pAlterTbReq->compress;
      if (updataTableColCmpr(&entry.colCmpr, pCol, 1, compress) != 0) {
        metaError("vgId:%d, failed to update table col cmpr:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name,
                  entry.uid);
      }
      freeColCmpr = true;
      if (entry.colCmpr.nCols != pSchema->nCols) {
        if (pNewSchema) taosMemoryFree(pNewSchema);
        if (freeColCmpr) taosMemoryFree(entry.colCmpr.pColCmpr);
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (pColumn->colId == 0) {
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      bool hasPrimayKey = false;
      if (pSchema->nCols >= 2) {
        hasPrimayKey = pSchema->pSchema[1].flags & COL_IS_KEY ? true : false;
      }

      memcpy(&tScheam, pColumn, sizeof(SSchema));
      pSchema->version++;
      tlen = (pSchema->nCols - iCol - 1) * sizeof(SSchema);
      if (tlen) {
        memmove(pColumn, pColumn + 1, tlen);
      }
      pSchema->nCols--;

      --pMeta->pVnode->config.vndStats.numOfNTimeSeries;

      if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
        int16_t cid = pColumn->colId;

        if (tsdbCacheDropNTableColumn(pMeta->pVnode->pTsdb, entry.uid, cid, hasPrimayKey) != 0) {
          metaError("vgId:%d, failed to drop ntable column:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name,
                    entry.uid);
        }
      }

      if (updataTableColCmpr(&entry.colCmpr, &tScheam, 0, 0) != 0) {
        metaError("vgId:%d, failed to update table col cmpr:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name,
                  entry.uid);
      }
      if (entry.colCmpr.nCols != pSchema->nCols) {
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (!IS_VAR_DATA_TYPE(pColumn->type) || pColumn->bytes >= pAlterTbReq->colModBytes) {
        terrno = TSDB_CODE_VND_INVALID_TABLE_ACTION;
        goto _err;
      }
      if (rowLen + pAlterTbReq->colModBytes - pColumn->bytes > TSDB_MAX_BYTES_PER_ROW) {
        terrno = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      pSchema->version++;
      pColumn->bytes = pAlterTbReq->colModBytes;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (pAlterTbReq->colNewName == NULL) {
        terrno = TSDB_CODE_INVALID_MSG;
        goto _err;
      }
      if (pColumn == NULL) {
        terrno = TSDB_CODE_VND_COL_NOT_EXISTS;
        goto _err;
      }
      if (tqCheckColModifiable(pMeta->pVnode->pTq, uid, pColumn->colId) != 0) {
        terrno = TSDB_CODE_VND_COL_SUBSCRIBED;
        goto _err;
      }
      pSchema->version++;
      strcpy(pColumn->name, pAlterTbReq->colNewName);
      break;
  }

  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    tsdbCacheInvalidateSchema(pMeta->pVnode->pTsdb, 0, entry.uid, pSchema->version);
  }

  entry.version = version;

  // do actual write
  metaWLock(pMeta);

  if (metaDeleteNcolIdx(pMeta, &oldEntry) < 0) {
    metaError("vgId:%d, failed to delete ncol idx:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  if (metaUpdateNcolIdx(pMeta, &entry) < 0) {
    metaError("vgId:%d, failed to update ncol idx:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  // save to table db
  if (metaSaveToTbDb(pMeta, &entry) < 0) {
    metaError("vgId:%d, failed to save to tb db:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  if (metaUpdateUidIdx(pMeta, &entry) < 0) {
    metaError("vgId:%d, failed to update uid idx:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  if (metaSaveToSkmDb(pMeta, &entry) < 0) {
    metaError("vgId:%d, failed to save to skm db:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  if (metaUpdateChangeTime(pMeta, entry.uid, pAlterTbReq->ctimeMs) < 0) {
    metaError("vgId:%d, failed to update change time:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }

  metaULock(pMeta);

  if (metaUpdateMetaRsp(uid, pAlterTbReq->tbName, pSchema, pMetaRsp) < 0) {
    metaError("vgId:%d, failed to update meta rsp:%s uid:%" PRId64, TD_VID(pMeta->pVnode), entry.name, entry.uid);
  }
  for (int32_t i = 0; i < entry.colCmpr.nCols; i++) {
    SColCmpr *p = &entry.colCmpr.pColCmpr[i];
    pMetaRsp->pSchemaExt[i].colId = p->id;
    pMetaRsp->pSchemaExt[i].compress = p->alg;
  }

  if (entry.pBuf) taosMemoryFree(entry.pBuf);
  if (pNewSchema) taosMemoryFree(pNewSchema);
  if (freeColCmpr) taosMemoryFree(entry.colCmpr.pColCmpr);

  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  tDecoderClear(&dc);

  return 0;

_err:
  if (entry.pBuf) taosMemoryFree(entry.pBuf);
  tdbTbcClose(pTbDbc);
  tdbTbcClose(pUidIdxc);
  tDecoderClear(&dc);

  return terrno != 0 ? terrno : TSDB_CODE_FAILED;
#endif
}

int32_t metaDropTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}

int32_t metaAlterTableColumnName(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}

int32_t metaAlterTableColumnBytes(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}

int32_t metaAlterTableColumnCompressOption(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}