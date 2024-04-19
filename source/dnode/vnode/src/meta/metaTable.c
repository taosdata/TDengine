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

extern int32_t metaGetTableEntryByUidImpl(SMeta *meta, int64_t uid, SMetaEntry **entry);
extern int32_t metaGetTableEntryByNameImpl(SMeta *meta, const char *name, SMetaEntry **entry);
extern int32_t metaEntryCloneDestroy(SMetaEntry *entry);

static int  metaUpdateChangeTime(SMeta *pMeta, tb_uid_t uid, int64_t changeTimeMs);
static void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey);

static int metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema, STableMetaRsp *pMetaRsp) {
  pMetaRsp->pSchemas = taosMemoryMalloc(pSchema->nCols * sizeof(SSchema));
  if (NULL == pMetaRsp->pSchemas) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tstrncpy(pMetaRsp->tbName, tbName, TSDB_TABLE_NAME_LEN);
  pMetaRsp->numOfColumns = pSchema->nCols;
  pMetaRsp->tableType = TSDB_NORMAL_TABLE;
  pMetaRsp->sversion = pSchema->version;
  pMetaRsp->tuid = uid;

  memcpy(pMetaRsp->pSchemas, pSchema->pSchema, pSchema->nCols * sizeof(SSchema));

  return 0;
}

static inline void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#ifdef TD_ENTERPRISE
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      tsem_post(&dmNotifyHdl.sem);
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
    metaDecodeEntry(&dc, &me);

    if (me.type != TSDB_SUPER_TABLE) {
      char tbFName[TSDB_TABLE_FNAME_LEN + 1];
      snprintf(tbFName, sizeof(tbFName), "%s.%s", pMeta->pVnode->config.dbname, me.name);
      tbFName[TSDB_TABLE_FNAME_LEN] = '\0';
      int32_t ret = vnodeValidateTableHash(pMeta->pVnode, tbFName);
      if (ret < 0 && terrno == TSDB_CODE_VND_HASH_MISMATCH) {
        taosArrayPush(uidList, &me.uid);
      }
    }
    tDecoderClear(&dc);
  }
  tdbFree(pData);
  tdbFree(pKey);
  tdbTbcClose(pCur);

  return 0;
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

static int metaBuildBtimeIdxKey(SBtimeIdxKey *btimeKey, const SMetaEntry *pME) {
  int64_t btime;
  if (pME->type == TSDB_CHILD_TABLE) {
    btime = pME->ctbEntry.btime;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    btime = pME->ntbEntry.btime;
  } else {
    return -1;
  }

  btimeKey->btime = btime;
  btimeKey->uid = pME->uid;
  return 0;
}

static int metaBuildNColIdxKey(SNcolIdxKey *ncolKey, const SMetaEntry *pME) {
  if (pME->type == TSDB_NORMAL_TABLE) {
    ncolKey->ncol = pME->ntbEntry.schemaRow.nCols;
    ncolKey->uid = pME->uid;
  } else {
    return -1;
  }
  return 0;
}

static int metaDropTableByUid(SMeta *meta, int64_t version, int64_t uid) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  SMetaEntry entry = {
      .version = version,
      .uid = uid,
  };

  if (tdbTbGet(meta->pUidIdx, &uid, sizeof(uid), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  SUidIdxVal *uidIdxVal = (SUidIdxVal *)value;
  if (uidIdxVal->suid == 0) {
    entry.type = -TSDB_NORMAL_TABLE;
  } else if (uidIdxVal->suid == uid) {
    entry.type = -TSDB_SUPER_TABLE;
  } else {
    entry.type = -TSDB_CHILD_TABLE;
  }

  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return code;
}

static int metaUpdateTableOptions(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (entry->type == TSDB_CHILD_TABLE) {
    // if (request->updateTTL) {
    //   metaDeleteTtl(meta, entry);
    //   entry->ctbEntry.ttlDays = request->newTTL;
    //   metaUpdateTtl(meta, entry);
    // }
    // if (request->newCommentLen >= 0) {
    //   entry->ctbEntry.commentLen = request->newCommentLen;
    //   entry->ctbEntry.comment = request->newComment;
    // }
  } else {
    // if (request->updateTTL) {
    //   metaDeleteTtl(meta, entry);
    //   entry->ntbEntry.ttlDays = request->newTTL;
    //   metaUpdateTtl(meta, entry);
    // }
    // if (request->newCommentLen >= 0) {
    //   entry->ntbEntry.commentLen = request->newCommentLen;
    //   entry->ntbEntry.comment = request->newComment;
    // }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
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

int metaCreateTagIdxKey(tb_uid_t suid, int32_t cid, const void *pTagData, int32_t nTagData, int8_t type, tb_uid_t uid,
                        STagIdxKey **ppTagIdxKey, int32_t *nTagIdxKey) {
  if (IS_VAR_DATA_TYPE(type)) {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + VARSTR_HEADER_SIZE + sizeof(tb_uid_t);
  } else {
    *nTagIdxKey = sizeof(STagIdxKey) + nTagData + sizeof(tb_uid_t);
  }

  *ppTagIdxKey = (STagIdxKey *)taosMemoryMalloc(*nTagIdxKey);
  if (*ppTagIdxKey == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
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

static void metaDestroyTagIdxKey(STagIdxKey *pTagIdxKey) {
  if (pTagIdxKey) taosMemoryFree(pTagIdxKey);
}

void *metaGetIdx(SMeta *pMeta) { return pMeta->pTagIdx; }
void *metaGetIvtIdx(SMeta *pMeta) { return pMeta->pTagIvtIdx; }

static int32_t metaValidateCreateTableRequest(SMeta *meta, SVCreateTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (request->type != TSDB_CHILD_TABLE && request->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->type == TSDB_CHILD_TABLE) {
    if (tdbTbGet(meta->pNameIdx, request->ctb.stbName, strlen(request->ctb.stbName) + 1, &value, &valueSize) != 0 ||
        *(int64_t *)value != request->ctb.suid) {
    }
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_TABLE_NOT_EXIST, lino, _exit);
  }

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) == 0) {
    // TODO
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return code;
}

static int32_t metaCreateNormalTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_NORMAL_TABLE,
      .uid = request->uid,
      .name = request->name,
      .ntbEntry.btime = request->btime,
      .ntbEntry.ttlDays = request->ttl,
      .ntbEntry.commentLen = request->commentLen,
      .ntbEntry.comment = request->comment,
      .ntbEntry.schemaRow = request->ntb.schemaRow,
      .ntbEntry.ncid = request->ntb.schemaRow.pSchema[request->ntb.schemaRow.nCols - 1].colId + 1,
  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (response) {
    if ((*response = taosMemoryCalloc(1, sizeof(STableMetaRsp))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    metaUpdateMetaRsp(request->uid, request->name, &request->ntb.schemaRow, *response);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaCreateChildTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry entry = {
      .version = version,
      .type = TSDB_CHILD_TABLE,
      .uid = request->uid,
      .name = request->name,
      .ctbEntry.btime = request->btime,
      .ctbEntry.ttlDays = request->ttl,
      .ctbEntry.commentLen = request->commentLen,
      .ctbEntry.comment = request->comment,
      .ctbEntry.suid = request->ctb.suid,
      .ctbEntry.pTags = request->ctb.pTag,
  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (response) {
    if ((*response = taosMemoryCalloc(1, sizeof(STableMetaRsp))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    (*response)->tableType = TSDB_CHILD_TABLE;
    (*response)->tuid = request->uid;
    (*response)->suid = request->ctb.suid;
    strcpy((*response)->tbName, request->name);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaValidateCreateSuperTableRequest(SMeta *meta, SVCreateStbReq *request, bool *isExist) {
  int32_t   code = 0;
  int32_t   lino = 0;
  void     *value = NULL;
  int32_t   valueSize = 0;
  SMetaInfo info;

  if (request->name == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) == 0) {
    code = metaGetInfo(meta, *(int64_t *)value, &info, NULL);
    ASSERT(code == 0);
    if (info.uid == info.suid) {
      *isExist = true;
    } else {
      TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST, lino, _exit);
    }
  }

_exit:
  tdbFree(value);
  return code;
}

int32_t metaCreateSuperTable(SMeta *meta, int64_t version, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    isExist = false;

  // validate request
  code = metaValidateCreateSuperTableRequest(meta, request, &isExist);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (isExist) {
    return (terrno = code);
  }

  // create super table
  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = request->suid,
      .name = request->name,
      .stbEntry.schemaRow = request->schemaRow,
      .stbEntry.schemaTag = request->schemaTag,
  };
  if (request->rollup) {
    TABLE_SET_ROLLUP(entry.flags);
    entry.stbEntry.rsmaParam = request->rsmaParam;
  }
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code == 0) {
    metaInfo("vgId:%d, super table is created, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, request->suid, version);
  } else if (code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    metaTrace("vgId:%d, failed to create super table at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  } else {
    metaError("vgId:%d, failed to create super table at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  }
  return (terrno = code);
}

int32_t metaCreateTable(SMeta *meta, int64_t version, SVCreateTbReq *request, STableMetaRsp **response) {
  int32_t code = 0;
  int32_t lino = 0;

  code = metaValidateCreateTableRequest(meta, request);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (request->type == TSDB_CHILD_TABLE) {
    code = metaCreateChildTable(meta, version, request, response);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = metaCreateNormalTable(meta, version, request, response);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaInfo("vgId:%d create table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64 " type:%d",
             TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->uid, version, request->type);
  } else {
    metaInfo("vgId:%d create table success, name:%s uid:%" PRId64 " version:%" PRId64 " type:%d", TD_VID(meta->pVnode),
             request->name, request->uid, version, request->type);
  }
  return (terrno = code);
}

static int32_t metaValidateDropTableReq(SMeta *meta, SVDropTbReq *request, int64_t *uid) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_NOT_EXIST, lino, _exit);
  }

  *uid = *(tb_uid_t *)value;

_exit:
  tdbFree(value);
  return code;
}

int32_t metaDropTable(SMeta *meta, int64_t version, SVDropTbReq *request, SArray *tbUids, tb_uid_t *tbUid) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t uid;

  code = metaValidateDropTableReq(meta, request, &uid);
  if (code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
    return (terrno = code);
  } else {
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = metaDropTableByUid(meta, version, uid);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d drop table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, uid, version);
  } else {
    metaInfo("vgId:%d drop table success, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, uid, version);
  }
  return (terrno = code);
}

static int32_t metaValidateDropSuperTableReq(SMeta *meta, SVDropStbReq *request, SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pNameIdx, request->name, strlen(request->name) + 1, &value, &valueSize) != 0 ||
      *(tb_uid_t *)value != request->suid) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

_exit:
  return code;
}

int32_t metaDropSuperTable(SMeta *meta, int64_t verison, SVDropStbReq *request, SArray *tbUidList) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMetaEntry entry = {
      .version = verison,
  };

  // validate
  code = metaValidateDropSuperTableReq(meta, request, &entry);
  if (code == TSDB_CODE_TDB_STB_NOT_EXIST) {
    return (terrno = code);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  // handle
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d drop super table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, verison);
  } else {
  }
  return (terrno = code);
}

static int32_t metaValidateAlterSuperTableReq(SMeta *meta, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *superTableEntry = NULL;

  if (request->name == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  code = metaGetTableEntryByNameImpl(meta, request->name, &superTableEntry);
  if (code == TSDB_CODE_NOT_FOUND) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

  if (superTableEntry->type != TSDB_SUPER_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_STB_NOT_EXIST, lino, _exit);
  }

  if (request->suid != superTableEntry->uid) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  if (request->schemaRow.version < superTableEntry->stbEntry.schemaRow.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->schemaTag.version < superTableEntry->stbEntry.schemaTag.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (request->schemaRow.version == superTableEntry->stbEntry.schemaRow.version &&
      request->schemaTag.version == superTableEntry->stbEntry.schemaTag.version) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  return code;
}

int32_t metaAlterSuperTable(SMeta *meta, int64_t version, SVCreateStbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  // validate
  code = metaValidateAlterSuperTableReq(meta, request);
  TSDB_CHECK_CODE(code, lino, _exit);

  // handle
  SMetaEntry entry = {
      .version = version,
      .type = TSDB_SUPER_TABLE,
      .uid = request->suid,
      .name = request->name,
      .stbEntry.schemaRow = request->schemaRow,
      .stbEntry.schemaTag = request->schemaTag,

  };
  code = metaHandleEntry(meta, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d alter super table failed at line %d since %s, name:%s uid:%" PRId64 " version:%" PRId64,
              TD_VID(meta->pVnode), lino, tstrerror(code), request->name, request->suid, version);
  } else {
    metaInfo("vgId:%d alter super table success, name:%s uid:%" PRId64 " version:%" PRId64, TD_VID(meta->pVnode),
             request->name, request->suid, version);
  }
  return (terrno = code);
}

static int32_t metaAddTableColumn(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         rowBytes = 0;
  for (int32_t i = 0; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_ALREADY_EXISTS, lino, _exit);
    }
    rowBytes += schema->pSchema[i].bytes;
  }

  code = grantCheck(TSDB_GRANT_TIMESERIES);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (rowBytes + request->bytes > TSDB_MAX_BYTES_PER_ROW) {
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_INVALID_ROW_LENGTH, lino, _exit);
  }

  schema->version++;
  schema->nCols++;
  SSchema *newSchema = taosMemoryRealloc(schema->pSchema, sizeof(SSchema) * schema->nCols);
  if (newSchema == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  schema->pSchema = newSchema;
  schema->pSchema[schema->nCols - 1] = (SSchema){
      .type = request->type,
      .flags = request->flags,
      .colId = entry->ntbEntry.ncid++,
      .bytes = request->bytes,
  };
  strcpy(schema->pSchema[schema->nCols - 1].name, request->colName);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropTableColumn(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = 0;
  for (; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      break;
    }
  }

  if (i == schema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (schema->pSchema[i].colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  memmove(schema->pSchema + i, schema->pSchema + i + 1, (schema->nCols - i - 1) * sizeof(SSchema));
  schema->nCols--;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableColumnBytes(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = -1;
  int32_t         rowBytes = 0;
  for (int32_t j = 0; j < schema->nCols; j++) {
    if (strcmp(schema->pSchema[j].name, request->colName) == 0) {
      i = j;
    }
    rowBytes += schema->pSchema[j].bytes;
  }

  if (i == -1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (!IS_VAR_DATA_TYPE(schema->pSchema[i].type) || schema->pSchema[i].bytes >= request->colModBytes) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  if (rowBytes + request->colModBytes - schema->pSchema[i].bytes > TSDB_MAX_BYTES_PER_ROW) {
    TSDB_CHECK_CODE(code = TSDB_CODE_PAR_INVALID_ROW_LENGTH, lino, _exit);
  }

  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  schema->pSchema[i].bytes = request->colModBytes;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableColumnName(SMeta *meta, SMetaEntry *entry, SVAlterTbReq *request) {
  int32_t code = 0;
  int32_t lino = 0;

  if (request->colName == NULL || request->colNewName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (entry->type != TSDB_NORMAL_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  SSchemaWrapper *schema = &entry->ntbEntry.schemaRow;
  int32_t         i = 0;
  for (; i < schema->nCols; i++) {
    if (strcmp(schema->pSchema[i].name, request->colName) == 0) {
      break;
    }
  }

  if (i >= schema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }
  code = tqCheckColModifiable(meta->pVnode->pTq, entry->uid, schema->pSchema[i].colId);
  TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_SUBSCRIBED, lino, _exit);

  schema->version++;
  strncpy(schema->pSchema[i].name, request->colNewName, TSDB_COL_NAME_LEN - 1);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaUpdateTableTagValue(SMeta *meta, SMetaEntry *childTableEntry, SVAlterTbReq *request) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *superTableEntry = NULL;
  SArray     *tagValArray = NULL;

  if (request->tagName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  if (childTableEntry->type != TSDB_CHILD_TABLE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

  code = metaGetTableEntryByUidImpl(meta, childTableEntry->ctbEntry.suid, &superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  int32_t         i = 0;
  for (; i < tagSchema->nCols && strcmp(tagSchema->pSchema[i].name, request->tagName) != 0; i++) {
  }

  if (i >= tagSchema->nCols) {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_COL_NOT_EXISTS, lino, _exit);
  }

  if (tagSchema->nCols == 1 && tagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    STag *newTag = taosMemoryRealloc(childTableEntry->ctbEntry.pTags, request->nTagVal);
    if (newTag == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    memcpy(newTag, request->pTagVal, request->nTagVal);
    childTableEntry->ctbEntry.pTags = newTag;
  } else {
    if ((tagValArray = taosArrayInit(tagSchema->nCols, sizeof(STagVal))) == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }

    // generate tag value array
    for (int32_t j = 0; j < tagSchema->nCols; j++) {
      STagVal tagValue = {
          .cid = tagSchema->pSchema[j].colId,
      };

      if (i == j) {
        if (!request->isNull) {
          tagValue.type = tagSchema->pSchema[j].type;
          if (IS_VAR_DATA_TYPE(tagSchema->pSchema[j].type)) {
            tagValue.pData = request->pTagVal;
            tagValue.nData = request->nTagVal;
          } else {
            memcpy(&tagValue.i64, request->pTagVal, request->nTagVal);
          }
          taosArrayPush(tagValArray, &tagValue);
        }
      } else {
        if (tTagGet(childTableEntry->ctbEntry.pTags, &tagValue)) {
          taosArrayPush(tagValArray, &tagValue);
        }
      }

      // generate new tag
      void *newTag = NULL;
      code = tTagNew(tagValArray, tagSchema->version, false, (STag **)&newTag);
      TSDB_CHECK_CODE(code, lino, _exit);

      taosMemoryFree(childTableEntry->ctbEntry.pTags);
      childTableEntry->ctbEntry.pTags = newTag;
    }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  taosArrayDestroy(tagValArray);
  return code;
}

static int32_t metaValidateAlterTableReq(SMeta *meta, SVAlterTbReq *request, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (request->tbName == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  code = metaGetTableEntryByNameImpl(meta, request->tbName, entry);
  if (code == TSDB_CODE_NOT_FOUND) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_NOT_EXIST, lino, _exit);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  if (request->action == TSDB_ALTER_TABLE_ADD_COLUMN) {
    // TSDB_NORMAL_TABLE
    code = metaAddTableColumn(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_DROP_COLUMN) {
    // TSDB_NORMAL_TABLE
    code = metaDropTableColumn(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES) {
    // TSDB_NORMAL_TABLE
    code = metaUpdateTableColumnBytes(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME) {
    // TSDB_NORMAL_TABLE
    code = metaUpdateTableColumnName(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    // TSDB_CHILD_TABLE
    code = metaUpdateTableTagValue(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (request->action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    // TSDB_NORMAL_TABLE || TSDB_CHILD_TABLE
    code = metaUpdateTableOptions(meta, *entry, request);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    TSDB_CHECK_CODE(code = TSDB_CODE_VND_INVALID_TABLE_ACTION, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return code;
}

int32_t metaAlterTable(SMeta *meta, int64_t version, SVAlterTbReq *request, STableMetaRsp *response) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *entry = NULL;

  // validate
  code = metaValidateAlterTableReq(meta, request, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(version > entry->version);

  // do alter table
  entry->version = version;
  code = metaHandleEntry(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d alter table failed at line %d since %s", TD_VID(meta->pVnode), lino, tstrerror(code));
  } else {
    metaInfo("vgId:%d alter table success", TD_VID(meta->pVnode));
  }
  metaEntryCloneDestroy(entry);
  return (terrno = code);
}

int32_t metaDropTables(SMeta *meta, int64_t version, SArray *tbUids) {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosArrayGetSize(tbUids) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(tbUids); i++) {
      int64_t uid = *(int64_t *)taosArrayGet(tbUids, i);
      code = metaDropTableByUid(meta, version, uid);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t metaTrimTables(SMeta *meta, int64_t version) {
  int32_t code = 0;

  SArray *tbUids = taosArrayInit(8, sizeof(int64_t));
  if (tbUids == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = metaFilterTableByHash(meta, tbUids);
  if (code != 0) {
    goto end;
  }
  if (TARRAY_SIZE(tbUids) == 0) {
    goto end;
  }

  metaInfo("vgId:%d, trim %ld tables", TD_VID(meta->pVnode), taosArrayGetSize(tbUids));
  metaDropTables(meta, version, tbUids);

end:
  taosArrayDestroy(tbUids);

  return code;
}