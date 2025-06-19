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
extern int32_t metaUpdateVtbMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, SColRefWrapper *pRef,
                                    STableMetaRsp *pMetaRsp, int8_t tableType);
extern int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry);
extern int32_t metaFetchEntryByName(SMeta *pMeta, const char *name, SMetaEntry **ppEntry);
extern void    metaFetchEntryFree(SMetaEntry **ppEntry);
extern int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress);
extern int32_t addTableExtSchema(SMetaEntry* pEntry, const SSchema* pColumn, int32_t newColNum, SExtSchema* pExtSchema);
extern int32_t dropTableExtSchema(SMetaEntry* pEntry, int32_t dropColId, int32_t newColNum);

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
      .stbEntry.keep = pReq->keep,
  };
  if (pReq->rollup) {
    TABLE_SET_ROLLUP(entry.flags);
    entry.stbEntry.rsmaParam = pReq->rsmaParam;
  }
  if (pReq->colCmpred) {
    TABLE_SET_COL_COMPRESSED(entry.flags);
    entry.colCmpr = pReq->colCmpr;
  }

  entry.pExtSchemas = pReq->pExtSchemas;

  if (pReq->virtualStb) {
    TABLE_SET_VIRTUAL(entry.flags);
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
  SMetaEntry *pStbEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->ctb.stbName, &pStbEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table %s does not exist, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, version);
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
  }

  if (pStbEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s is not a super table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, version);
    metaFetchEntryFree(&pStbEntry);
    return TSDB_CODE_INVALID_MSG;
  }

  if (pStbEntry->uid != pReq->ctb.suid) {
    metaError("vgId:%d, %s failed at %s:%d since super table %s uid %" PRId64 " does not match request uid %" PRId64
              ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, pStbEntry->uid, pReq->ctb.suid,
              version);
    metaFetchEntryFree(&pStbEntry);
    return TSDB_CODE_INVALID_MSG;
  }

  // Check tag value
  SSchemaWrapper *pTagSchema = &pStbEntry->stbEntry.schemaTag;
  const STag     *pTag = (const STag *)pReq->ctb.pTag;
  if (pTagSchema->nCols != 1 || pTagSchema->pSchema[0].type != TSDB_DATA_TYPE_JSON) {
    for (int32_t i = 0; i < pTagSchema->nCols; ++i) {
      STagVal tagVal = {
          .cid = pTagSchema->pSchema[i].colId,
      };

      if (tTagGet(pTag, &tagVal)) {
        if (pTagSchema->pSchema[i].type != tagVal.type) {
          metaError("vgId:%d, %s failed at %s:%d since child table %s tag type does not match the expected type, version:%" PRId64,
                    TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, version);
          metaFetchEntryFree(&pStbEntry);
          return TSDB_CODE_INVALID_MSG;
        }
      }
    }
  }

  metaFetchEntryFree(&pStbEntry);

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
    metaInfo("vgId:%d, index:%" PRId64 ", child table is created, tb:%s uid:%" PRId64 " suid:%" PRId64,
             TD_VID(pMeta->pVnode), version, pReq->name, pReq->uid, pReq->ctb.suid);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s suid:%" PRId64 " version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name,
              pReq->ctb.suid, version);
  }
  return code;
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
    if (pEntry->pExtSchemas) {
      (*ppRsp)->pSchemaExt[i].typeMod = pEntry->pExtSchemas[i].typeMod;
    }
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
    .pExtSchemas = pReq->pExtSchemas,
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
    metaInfo("vgId:%d, index:%" PRId64 ", normal table is created, tb:%s uid:%" PRId64, TD_VID(pMeta->pVnode), version,
             pReq->name, pReq->uid);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
  }
  TAOS_RETURN(code);
}

static int32_t metaBuildCreateVirtualNormalTableRsp(SMeta *pMeta, SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    return terrno;
  }

  code = metaUpdateVtbMetaRsp(pEntry->uid, pEntry->name, &pEntry->ntbEntry.schemaRow, &pEntry->colRef, *ppRsp, TSDB_VIRTUAL_NORMAL_TABLE);
  if (code) {
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  return code;
}

static int32_t metaCreateVirtualNormalTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  // check request
  int32_t code = metaCheckCreateNormalTableReq(pMeta, version, pReq);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_VIRTUAL_NORMAL_TABLE,
      .uid = pReq->uid,
      .name = pReq->name,
      .ntbEntry.btime = pReq->btime,
      .ntbEntry.ttlDays = pReq->ttl,
      .ntbEntry.commentLen = pReq->commentLen,
      .ntbEntry.comment = pReq->comment,
      .ntbEntry.schemaRow = pReq->ntb.schemaRow,
      .ntbEntry.ncid = pReq->ntb.schemaRow.pSchema[pReq->ntb.schemaRow.nCols - 1].colId + 1,
      .colRef = pReq->colRef
  };

  code = metaBuildCreateVirtualNormalTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
  }

  // handle entry
  code = metaHandleEntry2(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, index:%" PRId64 ", virtual normal table is created, tb:%s uid:%" PRId64, TD_VID(pMeta->pVnode),
             version, pReq->name, pReq->uid);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->uid, pReq->name, version);
  }
  TAOS_RETURN(code);
#if 0
  metaTimeSeriesNotifyCheck(pMeta);
#endif
}

static int32_t metaBuildCreateVirtualChildTableRsp(SMeta *pMeta, SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    return terrno;
  }

  code = metaUpdateVtbMetaRsp(pEntry->uid, pEntry->name, NULL, &pEntry->colRef, *ppRsp, TSDB_VIRTUAL_CHILD_TABLE);
  if (code) {
    taosMemoryFreeClear(*ppRsp);
    return code;
  }
  (*ppRsp)->suid = pEntry->ctbEntry.suid;

  return code;
}

static int32_t metaCreateVirtualChildTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  // check request
  int32_t code = metaCheckCreateChildTableReq(pMeta, version, pReq);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_VIRTUAL_CHILD_TABLE,
      .uid = pReq->uid,
      .name = pReq->name,
      .ctbEntry.btime = pReq->btime,
      .ctbEntry.ttlDays = pReq->ttl,
      .ctbEntry.commentLen = pReq->commentLen,
      .ctbEntry.comment = pReq->comment,
      .ctbEntry.suid = pReq->ctb.suid,
      .ctbEntry.pTags = pReq->ctb.pTag,
      .colRef = pReq->colRef
  };

  code = metaBuildCreateVirtualChildTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
  }

  // handle entry
  code = metaHandleEntry2(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, index:%" PRId64 ", virtual child table is created, tb:%s uid:%" PRId64, TD_VID(pMeta->pVnode),
             version, pReq->name, pReq->uid);
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
  } else if (TSDB_VIRTUAL_NORMAL_TABLE == pReq->type) {
    code = metaCreateVirtualNormalTable(pMeta, version, pReq, ppRsp);
  } else if (TSDB_VIRTUAL_CHILD_TABLE == pReq->type) {
    code = metaCreateVirtualChildTable(pMeta, version, pReq, ppRsp);
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

  if (pReq->isVirtual) {
    if (pReq->suid == 0) {
      entry.type = -TSDB_VIRTUAL_NORMAL_TABLE;
    } else {
      entry.type = -TSDB_VIRTUAL_CHILD_TABLE;
    }
  } else {
    if (pReq->suid == 0) {
      entry.type = -TSDB_NORMAL_TABLE;
    } else {
      entry.type = -TSDB_CHILD_TABLE;
    }
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
  if (info.suid != 0 && pReq->action != TSDB_ALTER_TABLE_ALTER_COLUMN_REF && pReq->action != TSDB_ALTER_TABLE_REMOVE_COLUMN_REF) {
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

int32_t metaAddTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
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
  SExtSchema      extSchema = {0};
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

  if (pSchema->nCols + 1 > TSDB_MAX_COLUMNS) {
    metaError("vgId:%d, %s failed at %s:%d since column count %d + 1 > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pSchema->nCols, TSDB_MAX_COLUMNS, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_TOO_MANY_COLUMNS);
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
  extSchema.typeMod = pReq->typeMod;
  tstrncpy(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN);
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    SColRef tmpRef;
    if (TSDB_ALTER_TABLE_ADD_COLUMN == pReq->action) {
      tmpRef.hasRef = false;
      tmpRef.id = pColumn->colId;
    } else {
      tmpRef.hasRef = true;
      tmpRef.id = pColumn->colId;
      tstrncpy(tmpRef.refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(tmpRef.refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(tmpRef.refColName, pReq->refColName, TSDB_COL_NAME_LEN);
    }
    code = updataTableColRef(&pEntry->colRef, pColumn, 1, &tmpRef);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(code), version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(code);
    }
  } else {
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
  }
  code = addTableExtSchema(pEntry, pColumn, pSchema->nCols, &extSchema);
  if (code) {
    metaError("vgId:%d, %s failed to add ext schema at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    } else {
      for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
        SColRef *p = &pEntry->colRef.pColRef[i];
        pRsp->pColRefs[i].hasRef = p->hasRef;
        pRsp->pColRefs[i].id = p->id;
        if (p->hasRef) {
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
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
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaDropTableColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
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
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    code = updataTableColRef(&pEntry->colRef, &tColumn, 0, NULL);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
                __LINE__, tstrerror(code), version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(code);
    }
    if (pEntry->colRef.nCols != pSchema->nCols) {
      metaError("vgId:%d, %s failed at %s:%d since column count mismatch, version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
    }
  } else {
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
  }

  // update column extschema
  code = dropTableExtSchema(pEntry, iColumn, pSchema->nCols);
  if (code) {
    metaError("vgId:%d, %s failed to remove extschema at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    } else {
      for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
        SColRef *p = &pEntry->colRef.pColRef[i];
        pRsp->pColRefs[i].hasRef = p->hasRef;
        pRsp->pColRefs[i].id = p->id;
        if (p->hasRef) {
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
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
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaAlterTableColumnName(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
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
  code = metaHandleEntry2(pMeta, pEntry);
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
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    } else {
      for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
        SColRef *p = &pEntry->colRef.pColRef[i];
        pRsp->pColRefs[i].hasRef = p->hasRef;
        pRsp->pColRefs[i].id = p->id;
        if (p->hasRef) {
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
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
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaAlterTableColumnBytes(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
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
  code = metaHandleEntry2(pMeta, pEntry);
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
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    } else {
      for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
        SColRef *p = &pEntry->colRef.pColRef[i];
        pRsp->pColRefs[i].hasRef = p->hasRef;
        pRsp->pColRefs[i].id = p->id;
        if (p->hasRef) {
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
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

int32_t metaUpdateTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
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

  if (pChild->type != TSDB_CHILD_TABLE && pChild->type != TSDB_VIRTUAL_CHILD_TABLE) {
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
  code = metaHandleEntry2(pMeta, pChild);
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

int32_t metaUpdateTableMultiTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
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
  code = metaHandleEntry2(pMeta, pChild);
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

int32_t metaUpdateTableOptions2(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
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
  code = metaHandleEntry2(pMeta, pEntry);
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

int32_t metaUpdateTableColCompress2(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
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
  code = metaHandleEntry2(pMeta, pEntry);
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

int32_t metaAlterTableColumnRef(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  code = metaCheckAlterTableColumnReq(pMeta, version, pReq);
  if (code) {
    TAOS_RETURN(code);
  }

  if (NULL == pReq->refDbName) {
    metaError("vgId:%d, %s failed at %s:%d since invalid ref db name, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (NULL == pReq->refTbName) {
    metaError("vgId:%d, %s failed at %s:%d since invalid ref table name, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (NULL == pReq->refColName) {
    metaError("vgId:%d, %s failed at %s:%d since invalid ref Col name, version:%" PRId64, TD_VID(pMeta->pVnode),
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

  // fetch super entry
  SMetaEntry *pSuper = NULL;
  if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
    }
  }

  // search the column to update
  SSchemaWrapper *pSchema = pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? &pSuper->stbEntry.schemaRow : &pEntry->ntbEntry.schemaRow;
  SColRef        *pColRef = NULL;
  int32_t         iColumn = 0;
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    if (strncmp(pSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      pColRef = &pEntry->colRef.pColRef[i];
      iColumn = i;
      break;
    }
  }

  if (NULL == pColRef) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colId, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do update column name
  pEntry->version = version;
  pColRef->hasRef = true;
  pColRef->id = pSchema->pSchema[iColumn].colId;
  tstrncpy(pColRef->refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pColRef->refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pColRef->refColName, pReq->refColName, TSDB_COL_NAME_LEN);
  pSchema->version++;
  pEntry->colRef.version++;

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
      SColRef *p = &pEntry->colRef.pColRef[i];
      pRsp->pColRefs[i].hasRef = p->hasRef;
      pRsp->pColRefs[i].id = p->id;
      if (p->hasRef) {
        tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
      }
    }
  }

  metaFetchEntryFree(&pEntry);
  metaFetchEntryFree(&pSuper);
  TAOS_RETURN(code);
}

int32_t metaRemoveTableColumnRef(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
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

  // fetch super entry
  SMetaEntry *pSuper = NULL;
  if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
    }
  }

  // search the column to update
  SSchemaWrapper *pSchema = pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? &pSuper->stbEntry.schemaRow : &pEntry->ntbEntry.schemaRow;
  SColRef        *pColRef = NULL;
  int32_t         iColumn = 0;
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    if (strncmp(pSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      pColRef = &pEntry->colRef.pColRef[i];
      iColumn = i;
      break;
    }
  }

  if (NULL == pColRef) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, iColumn + 1, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // do update column name
  pEntry->version = version;
  pColRef->hasRef = false;
  pColRef->id = pSchema->pSchema[iColumn].colId;
  pSchema->version++;
  pEntry->colRef.version++;

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (metaUpdateVtbMetaRsp(pEntry->uid, pReq->tbName, pSchema, &pEntry->colRef, pRsp, pEntry->type) < 0) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
      SColRef *p = &pEntry->colRef.pColRef[i];
      pRsp->pColRefs[i].hasRef = p->hasRef;
      pRsp->pColRefs[i].id = p->id;
      if (p->hasRef) {
        tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
      }
    }
  }

  metaFetchEntryFree(&pEntry);
  metaFetchEntryFree(&pSuper);
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
  code = metaHandleEntry2(pMeta, pEntry);
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
  code = metaHandleEntry2(pMeta, pEntry);
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
      .stbEntry.keep = pReq->keep,
      .colCmpr = pReq->colCmpr,
      .pExtSchemas = pReq->pExtSchemas,
  };
  TABLE_SET_COL_COMPRESSED(entry.flags);
  if (pReq->virtualStb) {
    TABLE_SET_VIRTUAL(entry.flags);
  }

  // do handle the entry
  code = metaHandleEntry2(pMeta, &entry);
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
    code = metaHandleEntry2(pMeta, &entry);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(code), uid, version);
      return code;
    }
  }
  return code;
}
