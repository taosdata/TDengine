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
#include "scalar.h"
#include "tarray.h"
#include "tdatablock.h"
#include "querynodes.h"
#include "thash.h"

extern int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry);
extern int32_t metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, int64_t ownerId,
                                 STableMetaRsp *pMetaRsp);
extern int32_t metaUpdateVtbMetaRsp(SMetaEntry *pEntry, char *tbName, const SSchemaWrapper *pSchema,
                                    const SColRefWrapper *pRef, const SExtSchema *pExtSchemas, int64_t ownerId,
                                    STableMetaRsp *pMetaRsp,
                                    int8_t tableType);
extern int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry);
extern int32_t metaFetchEntryByName(SMeta *pMeta, const char *name, SMetaEntry **ppEntry);
extern void    metaFetchEntryFree(SMetaEntry **ppEntry);
extern int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress);
extern int32_t addTableExtSchema(SMetaEntry *pEntry, const SSchema *pColumn, int32_t newColNum, SExtSchema *pExtSchema);
extern int32_t dropTableExtSchema(SMetaEntry *pEntry, int32_t dropColId, int32_t newColNum);

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
      .stbEntry.ownerId = pReq->ownerId,
      .stbEntry.securityLevel = pReq->securityLevel,
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
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
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

  code = metaUpdateMetaRsp(pEntry->uid, pEntry->name, &pEntry->ntbEntry.schemaRow, pEntry->ntbEntry.ownerId, *ppRsp);
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
      .ntbEntry.ownerId = pReq->ntb.userId,
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

  code = metaUpdateVtbMetaRsp(pEntry, pEntry->name, &pEntry->ntbEntry.schemaRow, &pEntry->colRef, pEntry->pExtSchemas,
                              pEntry->ntbEntry.ownerId, *ppRsp, TSDB_VIRTUAL_NORMAL_TABLE);
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

  SMetaEntry entry = {.version = version,
                      .type = TSDB_VIRTUAL_NORMAL_TABLE,
                      .uid = pReq->uid,
                      .name = pReq->name,
                      .ntbEntry.btime = pReq->btime,
                      .ntbEntry.ttlDays = pReq->ttl,
                      .ntbEntry.commentLen = pReq->commentLen,
                      .ntbEntry.comment = pReq->comment,
                      .ntbEntry.schemaRow = pReq->ntb.schemaRow,
                      .ntbEntry.ncid = pReq->ntb.schemaRow.pSchema[pReq->ntb.schemaRow.nCols - 1].colId + 1,
                      .ntbEntry.ownerId = pReq->ntb.userId,
                      .pExtSchemas = pReq->pExtSchemas,
                      .colRef = pReq->colRef};

  code = metaBuildCreateVirtualNormalTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    TAOS_RETURN(code);
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
  int32_t    code = TSDB_CODE_SUCCESS;
  SMetaEntry *pSuper = NULL;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    return terrno;
  }

  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  code = metaUpdateVtbMetaRsp(pEntry, pEntry->name, &pSuper->stbEntry.schemaRow, &pEntry->colRef, pSuper->pExtSchemas,
                              pSuper->stbEntry.ownerId, *ppRsp, TSDB_VIRTUAL_CHILD_TABLE);
  if (code) {
    metaFetchEntryFree(&pSuper);
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  (*ppRsp)->suid = pEntry->ctbEntry.suid;
  metaFetchEntryFree(&pSuper);

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

  SMetaEntry entry = {.version = version,
                      .type = TSDB_VIRTUAL_CHILD_TABLE,
                      .uid = pReq->uid,
                      .name = pReq->name,
                      .ctbEntry.btime = pReq->btime,
                      .ctbEntry.ttlDays = pReq->ttl,
                      .ctbEntry.commentLen = pReq->commentLen,
                      .ctbEntry.comment = pReq->comment,
                      .ctbEntry.suid = pReq->ctb.suid,
                      .ctbEntry.pTags = pReq->ctb.pTag,
                      .colRef = pReq->colRef};

  code = metaBuildCreateVirtualChildTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    TAOS_RETURN(code);
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
  if (info.suid != 0 && pReq->action != TSDB_ALTER_TABLE_ALTER_COLUMN_REF &&
      pReq->action != TSDB_ALTER_TABLE_REMOVE_COLUMN_REF) {
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

  int32_t maxBytesPerRow = pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE ? TSDB_MAX_BYTES_PER_ROW_VIRTUAL : TSDB_MAX_BYTES_PER_ROW;
  if (rowSize + pReq->bytes > maxBytesPerRow) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d + %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, rowSize, pReq->bytes, maxBytesPerRow, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_ROW_LENGTH);
  }

  int32_t maxCols = pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE ? TSDB_MAX_COLUMNS : TSDB_MAX_COLUMNS_NON_VIRTUAL;
  if (pSchema->nCols + 1 > maxCols) {
    metaError("vgId:%d, %s failed at %s:%d since column count %d + 1 > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pSchema->nCols, maxCols, version);
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
  if (pEntry->ntbEntry.ncid > INT16_MAX) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d exceeds max column id %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ntbEntry.ncid, INT16_MAX,
              version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_EXCEED_MAX_COL_ID);
  }
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, &pEntry->colRef, pEntry->pExtSchemas,
                                pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
    if (code) {
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
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->ntbEntry.ownerId, pRsp);
    if (code) {
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
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // build response
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, &pEntry->colRef, pEntry->pExtSchemas,
                                pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
    if (code) {
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
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->ntbEntry.ownerId, pRsp);
    if (code) {
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, &pEntry->colRef, pEntry->pExtSchemas,
                                pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
    if (code) {
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
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->ntbEntry.ownerId, pRsp);
    if (code) {
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

  int32_t maxBytesPerRow = pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE ? TSDB_MAX_BYTES_PER_ROW_VIRTUAL : TSDB_MAX_BYTES_PER_ROW;
  if (rowSize + pReq->colModBytes - pColumn->bytes > maxBytesPerRow) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d + %d - %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, rowSize, pReq->colModBytes, pColumn->bytes, maxBytesPerRow,
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, &pEntry->colRef, pEntry->pExtSchemas,
                                pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
    if (code) {
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
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->ntbEntry.ownerId, pRsp);
    if (code) {
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



static bool tagValueChanged(const SUpdatedTagVal* pNewVal, const STag *pOldTag) {
  STagVal oldVal = { .cid = pNewVal->colId };

  if (pNewVal->isNull) {
    return tTagGet(pOldTag, &oldVal);
  }

  if (!tTagGet(pOldTag, &oldVal)){
    return true;
  }

  if (!IS_VAR_DATA_TYPE(oldVal.type)) {
    return (memcmp(&oldVal.i64, pNewVal->pTagVal, pNewVal->nTagVal) != 0);
  }

  if (oldVal.nData != pNewVal->nTagVal) {
    return true;
  }

  return (memcmp(pNewVal->pTagVal, oldVal.pData, oldVal.nData) != 0);
}



static int32_t updatedTagValueArrayToHashMap(SSchemaWrapper* pTagSchema, SArray* arr, SHashObj **hashMap) {
  int32_t numOfTags = arr == NULL ? 0 : taosArrayGetSize(arr);
  if (numOfTags == 0) {
    metaError("%s failed at %s:%d since no tags specified", __func__, __FILE__, __LINE__);
    return TSDB_CODE_INVALID_MSG;
  }

  *hashMap = taosHashInit(numOfTags, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (*hashMap == NULL) {
    metaError("%s failed at %s:%d since %s", __func__, __FILE__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  for (int32_t i = 0; i < taosArrayGetSize(arr); i++) {
    SUpdatedTagVal *pTagVal = taosArrayGet(arr, i);
    if (taosHashGet(*hashMap, &pTagVal->colId, sizeof(pTagVal->colId)) != NULL) {
      metaError("%s failed at %s:%d since duplicate tags %s", __func__, __FILE__, __LINE__, pTagVal->tagName);
      taosHashCleanup(*hashMap);
      return TSDB_CODE_INVALID_MSG;
    }

    int32_t code = taosHashPut(*hashMap, &pTagVal->colId, sizeof(pTagVal->colId), pTagVal, sizeof(*pTagVal));
    if (code) {
      metaError("%s failed at %s:%d since %s", __func__, __FILE__, __LINE__, tstrerror(code));
      taosHashCleanup(*hashMap);
      return code;
    }
  }

  int32_t changed = 0;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    int32_t schemaColId = pTagSchema->pSchema[i].colId;
    if (taosHashGet(*hashMap, &schemaColId, sizeof(schemaColId)) != NULL) {
      changed++;
    }
  }
  if (changed < numOfTags) {
    metaError("%s failed at %s:%d since tag count mismatch, %d:%d", __func__, __FILE__, __LINE__, changed, numOfTags);
    taosHashCleanup(*hashMap);
    return TSDB_CODE_VND_COL_NOT_EXISTS;
  }

  return TSDB_CODE_SUCCESS;
}



static int32_t metaUpdateTableJsonTagValue(SMeta* pMeta, SMetaEntry* pTable, SSchemaWrapper* pTagSchema, SHashObj* pUpdatedTagVals) {
  SSchema *pCol = &pTagSchema->pSchema[0];
  int32_t colId = pCol->colId;
  SUpdatedTagVal *pTagVal = taosHashGet(pUpdatedTagVals, &colId, sizeof(colId));
  void *pNewTag = taosMemoryRealloc(pTable->ctbEntry.pTags, pTagVal->nTagVal);
  if (pNewTag == NULL) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), pTable->version);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pTable->ctbEntry.pTags = pNewTag;
  memcpy(pTable->ctbEntry.pTags, pTagVal->pTagVal, pTagVal->nTagVal);

  int32_t code = metaHandleEntry2(pMeta, pTable);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTable->uid, pTable->name, pTable->version);
  } else {
    const char* msgFmt = "vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64;
    metaInfo(msgFmt, TD_VID(pMeta->pVnode), pTable->name, pTable->uid, pTable->version);
  }

  return code;
}



static int32_t metaUpdateTableNormalTagValue(SMeta* pMeta, SMetaEntry* pTable, SSchemaWrapper* pTagSchema, SHashObj* pUpdatedTagVals) {
  int32_t code = TSDB_CODE_SUCCESS;

  const STag *pOldTag = (const STag *)pTable->ctbEntry.pTags;
  SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
  if (pTagArray == NULL) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), pTable->version);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  bool allSame = true;

  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    SSchema *pCol = &pTagSchema->pSchema[i];
    STagVal  value = { .cid = pCol->colId };

    int32_t colId = pCol->colId;
    SUpdatedTagVal *pNewVal = taosHashGet(pUpdatedTagVals, &colId, sizeof(colId));
    if (pNewVal == NULL) {
      if (!tTagGet(pOldTag, &value)) {
        continue;
      }
    } else {
      value.type = pCol->type;
      if (tagValueChanged(pNewVal, pOldTag)) {
        allSame = false;
      }
      if (pNewVal->isNull) {
        continue;
      }

      if (IS_VAR_DATA_TYPE(pCol->type)) {
        if ((int32_t)pNewVal->nTagVal > (pCol->bytes - VARSTR_HEADER_SIZE)) {
          const char* msgFmt = "vgId:%d, %s failed at %s:%d since value too long for tag %s, version:%" PRId64;
          metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pCol->name, pTable->version);
          taosArrayDestroy(pTagArray);
          TAOS_RETURN(TSDB_CODE_PAR_VALUE_TOO_LONG);
        }
        value.pData = pNewVal->pTagVal;
        value.nData = pNewVal->nTagVal;
      } else {
        memcpy(&value.i64, pNewVal->pTagVal, pNewVal->nTagVal);
      }
    }

    if (taosArrayPush(pTagArray, &value) == NULL) {
      const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
      metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), pTable->version);
      taosArrayDestroy(pTagArray);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  if (allSame) {
    const char* msgFmt = "vgId:%d, %s warn at %s:%d all tags are same, version:%" PRId64;
    metaWarn(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pTable->version);
    taosArrayDestroy(pTagArray);
    TAOS_RETURN(TSDB_CODE_VND_SAME_TAG);
  } 

  STag *pNewTag = NULL;
  code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTag);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTable->version);
    taosArrayDestroy(pTagArray);
    TAOS_RETURN(code);
  }
  taosArrayDestroy(pTagArray);
  taosMemoryFree(pTable->ctbEntry.pTags);
  pTable->ctbEntry.pTags = (uint8_t *)pNewTag;
  return TSDB_CODE_SUCCESS;
}


static int32_t regexReplaceBuildSubstitution(const char *pReplace, const regmatch_t *pmatch, int32_t nmatch,
                                               const char *cursor, char **ppResult, size_t *pResultLen,
                                               size_t *pResultCap) {
  char  *result = *ppResult;
  size_t resultLen = *pResultLen;
  size_t resultCap = *pResultCap;

  const char *r = pReplace;
  while (*r != '\0') {
    const char *chunk = NULL;
    size_t      chunkLen = 0;

    if (*r == '$' && r[1] >= '0' && r[1] <= '9') {
      int32_t groupIdx = r[1] - '0';
      if (groupIdx < nmatch && pmatch[groupIdx].rm_so != -1) {
        chunk = cursor + pmatch[groupIdx].rm_so;
        chunkLen = (size_t)(pmatch[groupIdx].rm_eo - pmatch[groupIdx].rm_so);
      }
      r += 2;
    } else if (*r == '$' && r[1] == '$') {
      chunk = "$";
      chunkLen = 1;
      r += 2;
    } else {
      chunk = r;
      chunkLen = 1;
      r += 1;
    }

    if (chunkLen > 0) {
      if (resultLen + chunkLen >= resultCap) {
        resultCap = (resultLen + chunkLen) * 2 + 1;
        char *tmp = taosMemoryRealloc(result, resultCap);
        if (NULL == tmp) {
          taosMemoryFree(result);
          *ppResult = NULL;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        result = tmp;
      }
      (void)memcpy(result + resultLen, chunk, chunkLen);
      resultLen += chunkLen;
    }
  }

  *ppResult = result;
  *pResultLen = resultLen;
  *pResultCap = resultCap;
  return TSDB_CODE_SUCCESS;
}

static int32_t regexReplace(const char *pStr, const char *pPattern, const char *pReplace, char **ppResult) {
  regex_t *regex = NULL;
  int32_t  code = threadGetRegComp(&regex, pPattern);
  if (code != 0) {
    return code;
  }

  size_t strLen = strlen(pStr);
  size_t resultCap = strLen + 1;
  size_t resultLen = 0;
  char  *result = taosMemoryMalloc(resultCap);
  if (NULL == result) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  const int32_t nmatch = 10;
  const char   *cursor = pStr;
  regmatch_t    pmatch[10];
  int32_t       execFlags = 0;

  while (*cursor != '\0') {
    int ret = regexec(regex, cursor, nmatch, pmatch, execFlags);
    if (ret == REG_NOMATCH) {
      break;
    }
    if (ret != 0) {
      taosMemoryFree(result);
      return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
    }

    size_t prefixLen = (size_t)pmatch[0].rm_so;
    if (resultLen + prefixLen >= resultCap) {
      resultCap = (resultLen + prefixLen) * 2 + 1;
      char *tmp = taosMemoryRealloc(result, resultCap);
      if (NULL == tmp) {
        taosMemoryFree(result);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      result = tmp;
    }
    (void)memcpy(result + resultLen, cursor, prefixLen);
    resultLen += prefixLen;

    code = regexReplaceBuildSubstitution(pReplace, pmatch, nmatch, cursor, &result, &resultLen, &resultCap);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pmatch[0].rm_so == pmatch[0].rm_eo) {
      if (resultLen + 1 >= resultCap) {
        resultCap = resultCap * 2 + 1;
        char *tmp = taosMemoryRealloc(result, resultCap);
        if (NULL == tmp) {
          taosMemoryFree(result);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        result = tmp;
      }
      result[resultLen++] = cursor[pmatch[0].rm_eo];
      cursor += pmatch[0].rm_eo + 1;
    } else {
      cursor += pmatch[0].rm_eo;
    }

    execFlags = REG_NOTBOL;
  }

  size_t tailLen = strlen(cursor);
  if (resultLen + tailLen + 1 > resultCap) {
    resultCap = resultLen + tailLen + 1;
    char *tmp = taosMemoryRealloc(result, resultCap);
    if (NULL == tmp) {
      taosMemoryFree(result);
      return terrno;
    }
    result = tmp;
  }
  (void)memcpy(result + resultLen, cursor, tailLen);
  resultLen += tailLen;
  result[resultLen] = '\0';

  *ppResult = result;
  return TSDB_CODE_SUCCESS;
}



static int32_t computeNewTagValViaRegexReplace(const STag* pOldTag, SUpdatedTagVal* pNewVal) {
  STagVal oldTagVal = {.cid = pNewVal->colId};
  if (!tTagGet(pOldTag, &oldTagVal)) {
    pNewVal->isNull = 1;
    pNewVal->pTagVal = NULL;
    pNewVal->nTagVal = 0;
    return TSDB_CODE_SUCCESS;
  }

  bool isNchar = (pNewVal->tagType == TSDB_DATA_TYPE_NCHAR);
  char* oldStr = NULL;

  if (isNchar) {
    // NCHAR is stored as UCS-4, convert to MBS (VARCHAR) for regex processing
    int32_t mbsLen = oldTagVal.nData + 1;
    oldStr = taosMemoryMalloc(mbsLen);
    if (oldStr == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int32_t ret = taosUcs4ToMbs((TdUcs4*)oldTagVal.pData, oldTagVal.nData, oldStr, NULL);
    if (ret < 0) {
      taosMemoryFree(oldStr);
      return ret;
    }
    oldStr[ret] = '\0';
  } else {
    // VARCHAR: build null-terminated string from old tag value
    oldStr = taosMemoryMalloc(oldTagVal.nData + 1);
    if (oldStr == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(oldStr, oldTagVal.pData, oldTagVal.nData);
    oldStr[oldTagVal.nData] = '\0';
  }

  char* newStr = NULL;
  int32_t code = regexReplace(oldStr, pNewVal->regexp, pNewVal->replacement, &newStr);
  taosMemoryFree(oldStr);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (isNchar) {
    // Convert regex result back from MBS to UCS-4 (NCHAR)
    int32_t newStrLen = (int32_t)strlen(newStr);
    int32_t ucs4BufLen = (newStrLen + 1) * TSDB_NCHAR_SIZE;
    char*   ucs4Buf = taosMemoryMalloc(ucs4BufLen);
    if (ucs4Buf == NULL) {
      taosMemoryFree(newStr);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t ucs4Len = 0;
    bool    cvtOk = taosMbsToUcs4(newStr, newStrLen, (TdUcs4*)ucs4Buf, ucs4BufLen, &ucs4Len, NULL);
    taosMemoryFree(newStr);
    if (!cvtOk) {
      taosMemoryFree(ucs4Buf);
      return terrno;
    }

    pNewVal->isNull = 0;
    pNewVal->pTagVal = (uint8_t*)ucs4Buf;
    pNewVal->nTagVal = (uint32_t)ucs4Len;
  } else {
    pNewVal->isNull = 0;
    pNewVal->pTagVal = (uint8_t*)newStr;
    pNewVal->nTagVal = (uint32_t)strlen(newStr);
  }

  return TSDB_CODE_SUCCESS;
}



static int32_t metaUpdateTableTagValueImpl(SMeta* pMeta, SMetaEntry* pTable, SSchemaWrapper* pTagSchema, SHashObj* pUpdatedTagVals) {
  if (pTagSchema->nCols == 1 && pTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    return metaUpdateTableJsonTagValue(pMeta, pTable, pTagSchema, pUpdatedTagVals);
  }
  
  int32_t   code = TSDB_CODE_SUCCESS, lino = 0;
  SHashObj* pNewTagVals = pUpdatedTagVals;
  SArray*   pRegexResults = NULL;

  void* pIter = taosHashIterate(pUpdatedTagVals, NULL);
  while (pIter) {
    SUpdatedTagVal* pVal = (SUpdatedTagVal*)pIter;
    if (pVal->regexp != NULL) {
      break;
    }
    pIter = taosHashIterate(pUpdatedTagVals, pIter);
  }

  // if there are regular expressions, compute new tag values and store in a new hash map to avoid
  // modifying the original tag values which may be reused for computing other tag values
  if (pIter != NULL) {
    taosHashCancelIterate(pUpdatedTagVals, pIter);
    pIter = NULL;
    
    int32_t sz = taosHashGetSize(pUpdatedTagVals);
    pNewTagVals = taosHashInit(sz, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
    if (pNewTagVals == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }

    pRegexResults = taosArrayInit(sz, sizeof(char*));
    if (pRegexResults == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }

    pIter = taosHashIterate(pUpdatedTagVals, NULL);
    while (pIter) {
      int32_t       colId = *(int32_t*)taosHashGetKey(pIter, NULL);
      SUpdatedTagVal newVal = *(SUpdatedTagVal *)pIter;
      pIter = taosHashIterate(pUpdatedTagVals, pIter);

      if (newVal.regexp != NULL) {
        const STag* pOldTag = (const STag*)pTable->ctbEntry.pTags;
        TAOS_CHECK_GOTO(computeNewTagValViaRegexReplace(pOldTag, &newVal), &lino, _exit);

        newVal.regexp = NULL;
        newVal.replacement = NULL;
        if (taosArrayPush(pRegexResults, &newVal.pTagVal) == NULL) {
          TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
        }
      }

      TAOS_CHECK_GOTO(taosHashPut(pNewTagVals, &colId, sizeof(colId), &newVal, sizeof(newVal)), &lino, _exit);
    }
  }

  TAOS_CHECK_GOTO(metaUpdateTableNormalTagValue(pMeta, pTable, pTagSchema, pNewTagVals), &lino, _exit);
  TAOS_CHECK_GOTO(metaHandleEntry2(pMeta, pTable), &lino, _exit);

_exit:
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, lino, tstrerror(code), pTable->uid, pTable->name, pTable->version);
  } else {
    const char* msgFmt = "vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64;
    metaInfo(msgFmt, TD_VID(pMeta->pVnode), pTable->name, pTable->uid, pTable->version);
  }

  if (pRegexResults != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pRegexResults); i++) {
      char** pp = taosArrayGet(pRegexResults, i);
      taosMemoryFree(*pp);
    }
    taosArrayDestroy(pRegexResults);
  }

  if (pNewTagVals != pUpdatedTagVals) {
    taosHashCleanup(pNewTagVals);
  }

  if (pIter != NULL) {
    taosHashCancelIterate(pUpdatedTagVals, pIter);
  }

  TAOS_RETURN(code);
}



static int32_t metaUpdateTableTagValue(SMeta *pMeta, int64_t version, const char* tbName, SArray* tags) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMetaEntry *pChild = NULL;
  SMetaEntry *pSuper = NULL;
  SHashObj* pUpdatedTagVals = NULL;

  // fetch child entry
  code = metaFetchEntryByName(pMeta, tbName, &pChild);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tbName, version);
    goto _exit;
  }

  if (pChild->type != TSDB_CHILD_TABLE && pChild->type != TSDB_VIRTUAL_CHILD_TABLE) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since table %s is not a child table, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tbName, version);
    code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _exit;
  }

  // fetch super entry
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pChild->ctbEntry.suid, version);
    code = TSDB_CODE_INTERNAL_ERROR;
    goto _exit;
  }

  // search the tags to update
  SSchemaWrapper *pTagSchema = &pSuper->stbEntry.schemaTag;

  code = updatedTagValueArrayToHashMap(pTagSchema, tags, &pUpdatedTagVals);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    goto _exit;
  }

  // change tag values
  pChild->version = version;
  code = metaUpdateTableTagValueImpl(pMeta, pChild, pTagSchema, pUpdatedTagVals);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, name:%s version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), tbName, version);
    goto _exit;
  }

_exit:
  taosHashCleanup(pUpdatedTagVals);
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pChild);
  TAOS_RETURN(code);
}



int32_t metaUpdateTableMultiTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* uidList = NULL;
  SArray* tagListArray = NULL;

  // Pre-allocate uidList for batch notification
  int32_t nTables = taosArrayGetSize(pReq->tables);
  uidList = taosArrayInit(nTables, sizeof(tb_uid_t));
  if (uidList == NULL) {
    code = terrno;
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    TAOS_RETURN(code);
  }
  tagListArray = taosArrayInit(nTables, sizeof(void*));
  if (tagListArray == NULL) {
    code = terrno;
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < nTables; i++) {
    SUpdateTableTagVal *pTable = taosArrayGet(pReq->tables, i);
    code = metaUpdateTableTagValue(pMeta, version, pTable->tbName, pTable->tags);
    if (code == TSDB_CODE_VND_SAME_TAG) {
      // we are updating multiple tables, if one table has same tag,
      // just skip it and continue to update other tables,
      // and return success at the end
      code = TSDB_CODE_SUCCESS;
    } else if (code) {
      break;
    } else {
      // Collect UID for batch notification
      int64_t uid = metaGetTableEntryUidByName(pMeta, pTable->tbName);
      if (uid == 0) {
        metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pTable->tbName, version);
        continue;
      }
      if (taosArrayPush(uidList, &uid) == NULL){
        metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), version);
        continue;
      }
      if (taosArrayPush(tagListArray, &pTable->tags) == NULL){
        void* ret = taosArrayPop(uidList);  // make sure the size of uidList and tagListArray are same
        metaError("vgId:%d, %s failed at %s:%d since %s, ret:%p, version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), ret, version);
        continue;
      }
    }
  }

  if (taosArrayGetSize(uidList) > 0) {
    vnodeAlterTagForTmq(pMeta->pVnode, uidList, NULL, tagListArray);
  }

  taosArrayDestroy(uidList);
  taosArrayDestroy(tagListArray);
  DestoryThreadLocalRegComp();

  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
  }

  TAOS_RETURN(code);
}


// Context for translating tag column/tbname references to tag values/table name
typedef struct STagToValueCtx {
  int32_t     code;
  const void *pTags;  // STag* blob of the child table
  const char *pName;  // table name of the child table
} STagToValueCtx;

// Translate a tag column reference to the corresponding tag value
static EDealRes tagToValue(SNode **pNode, void *pContext) {
  STagToValueCtx *pCtx = (STagToValueCtx *)pContext;

  bool isTagCol = false, isTbname = false;

  if (nodeType(*pNode) == QUERY_NODE_COLUMN) {
    SColumnNode *pCol = (SColumnNode *)*pNode;
    if (pCol->colType == COLUMN_TYPE_TBNAME)
      isTbname = true;
    else
      isTagCol = true;
  } else if (nodeType(*pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode *pFunc = (SFunctionNode *)*pNode;
    if (pFunc->funcType == FUNCTION_TYPE_TBNAME)
      isTbname = true;
  }

  if (isTagCol) {
    SColumnNode *pSColumnNode = *(SColumnNode **)pNode;
    SValueNode  *res = NULL;
    pCtx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
    if (NULL == res) {
      return DEAL_RES_ERROR;
    }

    res->translate = true;
    res->node.resType = pSColumnNode->node.resType;

    STagVal tagVal = {0};
    tagVal.cid = pSColumnNode->colId;
    const char *p = metaGetTableTagVal(pCtx->pTags, pSColumnNode->node.resType.type, &tagVal);
    if (p == NULL) {
      res->node.resType.type = TSDB_DATA_TYPE_NULL;
    } else if (pSColumnNode->node.resType.type == TSDB_DATA_TYPE_JSON) {
      int32_t len = ((const STag *)p)->len;
      res->datum.p = taosMemoryCalloc(len + 1, 1);
      if (NULL == res->datum.p) {
        pCtx->code = terrno;
        return DEAL_RES_ERROR;
      }
      memcpy(res->datum.p, p, len);
    } else if (IS_VAR_DATA_TYPE(pSColumnNode->node.resType.type)) {
      res->datum.p = taosMemoryCalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1, 1);
      if (NULL == res->datum.p) {
        pCtx->code = terrno;
        return DEAL_RES_ERROR;
      }
      memcpy(varDataVal(res->datum.p), tagVal.pData, tagVal.nData);
      varDataSetLen(res->datum.p, tagVal.nData);
    } else {
      pCtx->code = nodesSetValueNodeValue(res, &(tagVal.i64));
      if (pCtx->code != TSDB_CODE_SUCCESS) {
        return DEAL_RES_ERROR;
      }
    }

    nodesDestroyNode(*pNode);
    *pNode = (SNode *)res;

  } else if (isTbname) {
    SValueNode *res = NULL;
    pCtx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode **)&res);
    if (NULL == res) {
      return DEAL_RES_ERROR;
    }

    res->translate = true;
    res->node.resType = ((SExprNode *)(*pNode))->resType;

    int32_t len = strlen(pCtx->pName);
    res->datum.p = taosMemoryCalloc(len + VARSTR_HEADER_SIZE + 1, 1);
    if (NULL == res->datum.p) {
      pCtx->code = terrno;
      return DEAL_RES_ERROR;
    }
    memcpy(varDataVal(res->datum.p), pCtx->pName, len);
    varDataSetLen(res->datum.p, len);
    nodesDestroyNode(*pNode);
    *pNode = (SNode *)res;
  }

  return DEAL_RES_CONTINUE;
}


// Check if a single child table qualifies against the tag condition.
static int32_t metaIsChildTableQualified(SMeta *pMeta, tb_uid_t uid, SNode *pTagCond, bool *pQualified) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pEntry = NULL;

  *pQualified = false;

  code = metaFetchEntryByUid(pMeta, uid, &pEntry);
  if (code != TSDB_CODE_SUCCESS || pEntry == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  // Clone the condition so we can safely rewrite it
  SNode *pTagCondTmp = NULL;
  code = nodesCloneNode(pTagCond, &pTagCondTmp);
  if (code != TSDB_CODE_SUCCESS) {
    metaFetchEntryFree(&pEntry);
    return code;
  }

  // Replace tag column and tbname references with concrete values
  STagToValueCtx ctx = {.code = TSDB_CODE_SUCCESS, .pTags = pEntry->ctbEntry.pTags, .pName = pEntry->name};
  nodesRewriteExprPostOrder(&pTagCondTmp, tagToValue, &ctx);
  if (ctx.code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(pTagCondTmp);
    metaFetchEntryFree(&pEntry);
    return ctx.code;
  }

  // Evaluate the fully-resolved expression to a constant boolean
  SNode *pResult = NULL;
  code = scalarCalculateConstants(pTagCondTmp, &pResult);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(pTagCondTmp);
    metaFetchEntryFree(&pEntry);
    return code;
  }

  if (nodeType(pResult) == QUERY_NODE_VALUE) {
    *pQualified = ((SValueNode *)pResult)->datum.b;
  }

  nodesDestroyNode(pResult);
  metaFetchEntryFree(&pEntry);
  return TSDB_CODE_SUCCESS;
}



static int32_t metaGetChildUidsByTagCond(SMeta *pMeta, tb_uid_t suid, SNode *pTagCond, SNode *pTagIndexCond, SArray *pUidList) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SVnode       *pVnode = pMeta->pVnode;
  SArray       *pCandidateUids = NULL;

  taosArrayClear(pUidList);

  // Step 1: Try index-accelerated pre-filtering with pTagIndexCond
  if (pTagIndexCond != NULL) {
    pCandidateUids = taosArrayInit(64, sizeof(uint64_t));
    if (pCandidateUids == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
    }

    SIndexMetaArg metaArg = {
        .metaEx = pVnode,
        .idx = metaGetIdx(pMeta),
        .ivtIdx = metaGetIvtIdx(pMeta),
        .suid = suid,
    };

    SMetaDataFilterAPI filterAPI = {
        .metaFilterTableIds = metaFilterTableIds,
        .metaFilterCreateTime = metaFilterCreateTime,
        .metaFilterTableName = metaFilterTableName,
        .metaFilterTtl = metaFilterTtl,
    };

    SIdxFltStatus idxStatus = SFLT_NOT_INDEX;
    code = doFilterTag(pTagIndexCond, &metaArg, pCandidateUids, &idxStatus, &filterAPI);
    if (code != TSDB_CODE_SUCCESS || idxStatus == SFLT_NOT_INDEX) {
      // Index filtering failed or not supported, fall back to full scan
      const char* msgFmt = "vgId:%d, index filter not used for suid:%" PRId64 ", status:%d code:%s";
      metaDebug(msgFmt, TD_VID(pVnode), suid, idxStatus, tstrerror(code));
      taosArrayDestroy(pCandidateUids);
      pCandidateUids = NULL;
      code = TSDB_CODE_SUCCESS;
    }
  }

  // Step 2: If index was not used, enumerate all child table UIDs
  if (pCandidateUids == NULL) {
    pCandidateUids = taosArrayInit(256, sizeof(uint64_t));
    if (pCandidateUids == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _end);
    }

    SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, suid, 1);
    if (pCur == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _end);
    }

    while (1) {
      tb_uid_t uid = metaCtbCursorNext(pCur);
      if (uid == 0) {
        break;
      }
      if (taosArrayPush(pCandidateUids, &uid) == NULL) {
        code = terrno;
        metaCloseCtbCursor(pCur);
        TAOS_CHECK_GOTO(code, &lino, _end);
      }
    }
    metaCloseCtbCursor(pCur);
  }

  // Step 3: Apply pTagCond to filter the candidate UIDs
  if (pTagCond == NULL) {
    // No tag condition — all candidates qualify
    taosArraySwap(pUidList, pCandidateUids);
  } else {
    // Evaluate pTagCond per child table
    for (int32_t i = 0; i < taosArrayGetSize(pCandidateUids); i++) {
      uint64_t uid = *(uint64_t*)taosArrayGet(pCandidateUids, i);
      bool qualified = false;
      code = metaIsChildTableQualified(pMeta, uid, pTagCond, &qualified);
      if (code != TSDB_CODE_SUCCESS) {
        const char* msgFmt = "vgId:%d, %s failed to evaluate tag cond for uid:%" PRId64 " suid:%" PRId64 " since %s";
        metaError(msgFmt, TD_VID(pVnode), __func__, uid, suid, tstrerror(code));
        TAOS_CHECK_GOTO(code, &lino, _end);
      }

      if (qualified && taosArrayPush(pUidList, &uid) == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _end);
      }
    }
  }

_end:
  taosArrayDestroy(pCandidateUids);
  if (code != TSDB_CODE_SUCCESS) {
    const char* msgFmt = "vgId:%d, %s failed at line %d for suid:%" PRId64 " since %s";
    metaError(msgFmt, TD_VID(pVnode), __func__, lino, suid, tstrerror(code));
  }
  return code;
}


// Convenience wrapper: partition a raw WHERE condition into tag-related parts,
// then call metaGetChildUidsByTagCond to get the filtered child table UIDs.
int32_t metaGetChildUidsByWhere(SMeta *pMeta, tb_uid_t suid, SNode *pWhere, SArray *pUidList) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode  *pTagCond = NULL;
  SNode  *pTagIndexCond = NULL;

  if (pWhere == NULL) {
    // No WHERE condition — return all child table UIDs
    return metaGetChildUidsByTagCond(pMeta, suid, NULL, NULL, pUidList);
  }

  // Clone pWhere so the caller's node is not destroyed by filterPartitionCond
  SNode *pWhereCopy = NULL;
  code = nodesCloneNode(pWhere, &pWhereCopy);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Partition the WHERE condition
  code = filterPartitionCond(&pWhereCopy, NULL, &pTagIndexCond, &pTagCond, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // Call the core filtering function
  code = metaGetChildUidsByTagCond(pMeta, suid, pTagCond, pTagIndexCond, pUidList);

_cleanup:
  nodesDestroyNode(pTagCond);
  nodesDestroyNode(pTagIndexCond);
  nodesDestroyNode(pWhereCopy);
  return code;
}



int32_t metaUpdateTableChildTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode* pWhere = NULL;
  SMetaEntry *pSuper = NULL;
  SArray *pUids = NULL;
  SHashObj *pUpdatedTagVals = NULL;
  SArray *uidListForTmq = NULL;

  if (pReq->tbName == NULL || strlen(pReq->tbName) == 0) {
    const char* fmt = "vgId:%d, %s failed at %s:%d since invalid table name, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, version);
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (pReq->whereLen > 0) {
    code = nodesMsgToNode(pReq->where, pReq->whereLen, &pWhere);
    if (code) {
      const char* fmt = "vgId:%d, %s failed at %s:%d since invalid where condition, version:%" PRId64;
      metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, version);
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  code = metaFetchEntryByName(pMeta, pReq->tbName, &pSuper);
  if (code) {
    const char* fmt = "vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    goto _exit;
  }

  if (pSuper->type != TSDB_SUPER_TABLE) {
    const char* fmt = "vgId:%d, %s failed at %s:%d since table %s is not a super table, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _exit;
  }

  code = updatedTagValueArrayToHashMap(&pSuper->stbEntry.schemaTag, pReq->pMultiTag, &pUpdatedTagVals);
  if (code) {
    const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    goto _exit;
  }

  pUids = taosArrayInit(16, sizeof(int64_t));
  if (pUids == NULL) {
    code = terrno;
    const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    goto _exit;
  }
  code = metaGetChildUidsByWhere(pMeta, pSuper->uid, pWhere, pUids);
  if (code) {
    const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    goto _exit;
  }

  // Pre-allocate uidList for batch notification
  int32_t nUids = taosArrayGetSize(pUids);
  uidListForTmq = taosArrayInit(nUids, sizeof(tb_uid_t));
  if (uidListForTmq == NULL) {
    code = terrno;
    const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
    metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
    goto _exit;
  }

  for (int32_t i = 0; i < nUids; i++) {
    tb_uid_t uid = *(tb_uid_t *)taosArrayGet(pUids, i);
    SMetaEntry *pChild = NULL;
    code = metaFetchEntryByUid(pMeta, uid, &pChild);
    if (code) {
      const char* fmt = "vgId:%d, %s failed at %s:%d since child table uid %" PRId64 " not found, version:%" PRId64;
      metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, uid, version);
      goto _exit;
    }

    pChild->version = version;
    code = metaUpdateTableTagValueImpl(pMeta, pChild, &pSuper->stbEntry.schemaTag, pUpdatedTagVals);
    if (code == TSDB_CODE_VND_SAME_TAG) {
      // we are updating multiple tables, if one table has same tag,
      // just skip it and continue to update other tables,
      // and return success at the end
      code = TSDB_CODE_SUCCESS;
    } else if (code) {
      const char* fmt = "vgId:%d, %s failed at %s:%d since %s, child table uid %" PRId64 " name %s, version:%" PRId64;
      metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), uid, pChild->name, version);
      metaFetchEntryFree(&pChild);
      goto _exit;
    } else {
      // Collect UID for batch notification
      if (taosArrayPush(uidListForTmq, &uid) == NULL) {
        const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
        metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, terrstr, version);
      }
    }

    metaFetchEntryFree(&pChild);
  }

_exit:
  DestoryThreadLocalRegComp();
  if (taosArrayGetSize(uidListForTmq) > 0) {
    vnodeAlterTagForTmq(pMeta->pVnode, uidListForTmq, pReq->pMultiTag, NULL);
  }
  taosArrayDestroy(pUids);
  taosArrayDestroy(uidListForTmq);
  taosHashCleanup(pUpdatedTagVals);
  metaFetchEntryFree(&pSuper);
  nodesDestroyNode(pWhere);
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
  SSchemaWrapper *pSchema =
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? &pSuper->stbEntry.schemaRow : &pEntry->ntbEntry.schemaRow;
  SColRef *pColRef = NULL;
  int32_t  iColumn = 0;
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
  code = metaUpdateVtbMetaRsp(
      pEntry, pReq->tbName, pSchema, &pEntry->colRef,
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? pSuper->pExtSchemas : pEntry->pExtSchemas,
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? pSuper->stbEntry.ownerId : pEntry->ntbEntry.ownerId, pRsp,
      pEntry->type);
  if (code) {
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
  SSchemaWrapper *pSchema =
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? &pSuper->stbEntry.schemaRow : &pEntry->ntbEntry.schemaRow;
  SColRef *pColRef = NULL;
  int32_t  iColumn = 0;
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
  code = metaUpdateVtbMetaRsp(
      pEntry, pReq->tbName, pSchema, &pEntry->colRef,
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? pSuper->pExtSchemas : pEntry->pExtSchemas,
      pEntry->type == TSDB_VIRTUAL_CHILD_TABLE ? pSuper->stbEntry.ownerId : pEntry->ntbEntry.ownerId, pRsp,
      pEntry->type);
  if (code) {
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
      .stbEntry.ownerId = pReq->ownerId,
      .stbEntry.securityLevel = pReq->securityLevel,
      .colCmpr = pReq->colCmpr,
      .pExtSchemas = pReq->pExtSchemas,
  };
  TABLE_SET_COL_COMPRESSED(entry.flags);
  if (pReq->virtualStb) {
    TABLE_SET_VIRTUAL(entry.flags);
  }
  if(TABLE_IS_ROLLUP(pEntry->flags)) {
    TABLE_SET_ROLLUP(entry.flags);
    entry.stbEntry.rsmaParam = pEntry->stbEntry.rsmaParam;
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

int metaCreateRsma(SMeta *pMeta, int64_t version, SVCreateRsmaReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->name || pReq->name[0] == 0) {
    metaError("vgId:%d, failed at %d to create rsma since invalid rsma name, version:%" PRId64, TD_VID(pMeta->pVnode),
              __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, failed at %d to create rsma %s since table %s not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, version);
    TAOS_RETURN(TSDB_CODE_TDB_STB_NOT_EXIST);
  }

  if (pEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, failed at %d to create rsma %s since table %s type %d is invalid, version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->uid != pReq->tbUid) {
    metaError("vgId:%d, failed at %d to create rsma %s since table %s uid %" PRId64 " is not equal to %" PRId64
              ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, pEntry->uid, pReq->tbUid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (TABLE_IS_ROLLUP(pEntry->flags)) {
    // overwrite the old rsma definition if exists
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcColIds);
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcIds);
  } else {
    TABLE_SET_ROLLUP(pEntry->flags);
  }

  SMetaEntry entry = *pEntry;
  entry.version = version;
  entry.stbEntry.rsmaParam.name = pReq->name;
  entry.stbEntry.rsmaParam.uid = pReq->uid;
  entry.stbEntry.rsmaParam.interval[0] = pReq->interval[0];
  entry.stbEntry.rsmaParam.interval[1] = pReq->interval[1];
  entry.stbEntry.rsmaParam.intervalUnit = pReq->intervalUnit;
  entry.stbEntry.rsmaParam.nFuncs = pReq->nFuncs;
  entry.stbEntry.rsmaParam.funcColIds = pReq->funcColIds;
  entry.stbEntry.rsmaParam.funcIds = pReq->funcIds;

  // do handle the entry
  code = metaHandleEntry2(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, failed at %d to create rsma %s since %s, uid:%" PRId64 ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, tstrerror(code), pReq->tbUid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    pMeta->pVnode->config.vndStats.numOfRSMAs++;
    pMeta->pVnode->config.isRsma = 1;
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated since rsma created %s:%" PRIi64 ", version:%" PRId64,
             TD_VID(pMeta->pVnode), pReq->tbName, pReq->tbUid, pReq->name, pReq->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int metaDropRsma(SMeta *pMeta, int64_t version, SVDropRsmaReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->name || pReq->name[0] == 0) {
    metaError("vgId:%d, %s failed at %d since invalid rsma name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (NULL == pReq->tbName || pReq->tbName[0] == 0) {
    metaError("vgId:%d, %s failed at %d since invalid table name, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaWarn("vgId:%d, %s no need at %d to drop %s since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode),
             __func__, __LINE__, pReq->name, pReq->tbName, version);
    TAOS_RETURN(TSDB_CODE_RSMA_NOT_EXIST);
  }

  if (pEntry->type != pReq->tbType) {
    metaError("vgId:%d, %s failed at %d to drop %s since table %s type %d is invalid, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __LINE__, pReq->name, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->uid != pReq->tbUid) {
    metaError("vgId:%d, %s failed at %d %s since table %s uid %" PRId64 " is not equal to %" PRId64
              ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __LINE__, pReq->name, pReq->tbName, pEntry->uid, pReq->tbUid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (TABLE_IS_ROLLUP(pEntry->flags)) {
    if (pEntry->stbEntry.rsmaParam.uid != pReq->uid ||
        strncmp(pEntry->stbEntry.rsmaParam.name, pReq->name, TSDB_TABLE_NAME_LEN) != 0) {
      metaError(
          "vgId:%d, %s failed at line %d to drop %s since table %s is rollup table with different rsma name %s or "
          "uid:%" PRIi64 ", version:%" PRId64,
          TD_VID(pMeta->pVnode), __func__, __LINE__, pReq->name, pReq->tbName, pEntry->stbEntry.rsmaParam.name,
          pReq->uid, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
    }
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcColIds);
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcIds);
  } else {
    metaWarn("vgId:%d, %s no need at %d to drop %s since table %s is not rollup table, version:%" PRId64,
             TD_VID(pMeta->pVnode), __func__, __LINE__, pReq->name, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_RSMA_NOT_EXIST);
  }

  SMetaEntry entry = *pEntry;
  entry.version = version;
  TABLE_RESET_ROLLUP(entry.flags);
  entry.stbEntry.rsmaParam.uid = 0;
  entry.stbEntry.rsmaParam.name = NULL;
  entry.stbEntry.rsmaParam.nFuncs = 0;
  entry.stbEntry.rsmaParam.funcColIds = NULL;
  entry.stbEntry.rsmaParam.funcIds = NULL;

  // do handle the entry
  code = metaHandleEntry2(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, %s failed at %d to drop %s since %s, uid:%" PRId64 ", version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __LINE__, pReq->name, tstrerror(code), pReq->uid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    if (--pMeta->pVnode->config.vndStats.numOfRSMAs <= 0) {
      pMeta->pVnode->config.isRsma = 0;
    }
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated since rsma created %s:%" PRIi64 ", version:%" PRId64,
             TD_VID(pMeta->pVnode), pReq->tbName, pReq->tbUid, pReq->name, pReq->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int metaAlterRsma(SMeta *pMeta, int64_t version, SVAlterRsmaReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->name || pReq->name[0] == 0) {
    metaError("vgId:%d, failed at %d to alter rsma since invalid rsma name, version:%" PRId64, TD_VID(pMeta->pVnode),
              __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, failed at %d to alter rsma %s since table %s not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, version);
    TAOS_RETURN(TSDB_CODE_TDB_STB_NOT_EXIST);
  }

  if (pEntry->uid != pReq->tbUid) {
    metaError("vgId:%d, failed at %d to alter rsma %s since table %s uid %" PRId64 " is not equal to %" PRId64
              ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, pEntry->uid, pReq->tbUid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  if (TABLE_IS_ROLLUP(pEntry->flags)) {
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcColIds);
    taosMemoryFreeClear(pEntry->stbEntry.rsmaParam.funcIds);
  } else {
    metaError("vgId:%d, failed at %d to alter rsma %s since table %s is not rollup table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_RSMA_NOT_EXIST);
  }

  SMetaEntry entry = *pEntry;
  entry.version = version;
  if (pReq->alterType == TSDB_ALTER_RSMA_FUNCTION) {
    entry.stbEntry.rsmaParam.nFuncs = pReq->nFuncs;
    entry.stbEntry.rsmaParam.funcColIds = pReq->funcColIds;
    entry.stbEntry.rsmaParam.funcIds = pReq->funcIds;
  }
  // do handle the entry
  code = metaHandleEntry2(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, failed at %d to alter rsma %s since %s, uid:%" PRId64 ", version:%" PRId64,
              TD_VID(pMeta->pVnode), __LINE__, pReq->name, tstrerror(code), pReq->tbUid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated since rsma altered %s:%" PRIi64 ", version:%" PRId64,
             TD_VID(pMeta->pVnode), pReq->tbName, pReq->tbUid, pReq->name, pReq->uid, version);
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}
