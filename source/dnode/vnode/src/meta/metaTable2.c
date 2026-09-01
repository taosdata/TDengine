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
extern int32_t metaUpdateMetaRsp(tb_uid_t uid, char *tbName, SSchemaWrapper *pSchema,
                                 const SExtSchema *pExtSchemas, int64_t ownerId, STableMetaRsp *pMetaRsp);
extern int32_t metaUpdateVtbMetaRsp(SMetaEntry *pEntry, char *tbName, const SSchemaWrapper *pSchema,
                                    const SSchemaWrapper *pTagSchema, const SColRefWrapper *pRef,
                                    const SExtSchema *pExtSchemas, int64_t ownerId, STableMetaRsp *pMetaRsp,
                                    int8_t tableType);
extern int32_t metaFetchEntryByUid(SMeta *pMeta, int64_t uid, SMetaEntry **ppEntry);
extern int32_t metaFetchEntryByName(SMeta *pMeta, const char *name, SMetaEntry **ppEntry);
extern void    metaFetchEntryFree(SMetaEntry **ppEntry);
extern int32_t updataTableColCmpr(SColCmprWrapper *pWp, SSchema *pSchema, int8_t add, uint32_t compress);
extern int32_t addTableExtSchema(SMetaEntry *pEntry, const SSchema *pColumn, int32_t newColNum, SExtSchema *pExtSchema);
extern int32_t dropTableExtSchema(SMetaEntry *pEntry, int32_t dropColId, int32_t newColNum);

static int32_t metaValidateVirtualChildColRefReq(SMeta *pMeta, int64_t version, const SVCreateTbReq *pReq,
                                                 const SMetaEntry *pStbEntry) {
  int32_t expectedCols = pStbEntry->stbEntry.schemaRow.nCols;

  if (pReq->colRef.nCols != expectedCols) {
    metaError("vgId:%d, %s failed at %s:%d since virtual child table %s col ref count %d does not match super table "
              "%s schema count %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->colRef.nCols, pReq->ctb.stbName,
              expectedCols, version);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN;
  }

  if (expectedCols > 0 && pReq->colRef.pColRef == NULL) {
    metaError("vgId:%d, %s failed at %s:%d since virtual child table %s col ref is null while super table %s has %d "
              "columns, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->ctb.stbName, expectedCols,
              version);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN;
  }

  return TSDB_CODE_SUCCESS;
}

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

  // Hold metaRLock for pNameIdx B+tree read: async vacuum may concurrently
  // take metaWLock and mutate pNameIdx. Lock released before calling metaGetInfo
  // to avoid nested rdlock deadlock.
  metaRLock(pMeta);
  int32_t r = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  int64_t uid = 0;
  if (r == 0) {
    uid = *(tb_uid_t *)value;
    tdbFree(value);
  }
  metaULock(pMeta);

  if (r == 0) {  // name exists, check uid and type

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

  // Hold metaRLock for B+tree reads; release before metaGetInfo to avoid nested rdlock deadlock.
  metaRLock(pMeta);
  code = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  if (TSDB_CODE_SUCCESS != code) {
    metaULock(pMeta);
    if (pReq->igNotExists) {
      metaTrace("vgId:%d, %s success since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
                pReq->name, version);
    } else {
      metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, pReq->name, version);
    }
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }

  tb_uid_t uidFromName = *(tb_uid_t *)value;
  tdbFreeClear(value);

  // Vacuum internal (version < 0): verify pNameIdx[name] still maps to the caller's uid.
  // A new CREATE may have already reclaimed the name after ROLLBACK. In that case, the
  // old uid's entry is orphaned — we must NOT proceed or we would corrupt the new table.
  if (version < 0 && pReq->uid != 0 && uidFromName != pReq->uid) {
    metaULock(pMeta);
    metaInfo("vgId:%d, %s: vacuum uid %" PRId64 " but pNameIdx[%s]=%" PRId64 " (name reclaimed), skip",
             TD_VID(pMeta->pVnode), __func__, pReq->uid, pReq->name, uidFromName);
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }
  pReq->uid = uidFromName;

  // Batch meta txn: check if the entry is a finalized shadow that should be treated as non-existent.
  // Skip for internal operations (version == -1, e.g., vacuum cleanup) which need to physically
  // delete these entries.
  // Fast path: skip expensive fetch when no txns are active.
  if (version >= 0 && metaHasPendingTxnEntries(pMeta)) {
    SMetaEntry *pExist = NULL;
    if (metaFetchEntryByUid(pMeta, pReq->uid, &pExist) == 0 && pExist != NULL) {
      if (pExist->txnId != 0) {
        int8_t finalStatus = metaGetTxnMetaStatus(pMeta, pExist->txnId);
        if (pExist->txnStatus == META_TXN_PRE_CREATE && finalStatus == TXN_META_ROLLEDBACK) {
          // Rolled-back PRE_CREATE: table never existed, return NOT_EXIST
          metaFetchEntryFree(&pExist);
          metaULock(pMeta);
          return TSDB_CODE_TDB_TABLE_NOT_EXIST;
        }
      }
      metaFetchEntryFree(&pExist);
    }
  }
  metaULock(pMeta);

  // metaGetInfo locks internally — call without holding our lock
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

  // Hold metaRLock for pNameIdx B+tree read: async vacuum may concurrently
  // take metaWLock and mutate pNameIdx. Released before metaGetInfo to avoid
  // nested rdlock deadlock.
  metaRLock(pMeta);
  code = tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize);
  if (code) {
    metaULock(pMeta);
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->name, version);
    return TSDB_CODE_TDB_STB_NOT_EXIST;
  } else {
    int64_t uid = *(int64_t *)value;
    tdbFreeClear(value);
    metaULock(pMeta);

    if (uid != pReq->suid) {
      metaError("vgId:%d, %s failed at %s:%d since table %s uid:%" PRId64 " not match, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->name, pReq->suid, version);
      return TSDB_CODE_TDB_STB_NOT_EXIST;
    }
  }

  // metaGetInfo acquires RLock internally; call without holding our lock
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
  // NOTE: locking is done inside metaCheckCreateSuperTableReq per-operation
  // to avoid nested rdlock deadlock with metaGetInfo's internal rdlock
  // (writer-preference rwlock policy blocks nested readers when a writer waits).
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

  // VST inheritance (BASE ON): persist parent names + own-column/tag boundaries so the
  // TMQ snapshot path can reconstruct a replayable BASE ON clause (the incremental WAL
  // path carries these in the CREATE_STB message; the snapshot path has only this entry).
  entry.stbEntry.numParents = pReq->numParents;
  entry.stbEntry.ownColStart = pReq->ownColStart;
  entry.stbEntry.ownTagStart = pReq->ownTagStart;
  if (pReq->numParents > 0 && pReq->numParents <= TSDB_MAX_VST_PARENTS) {
    memcpy(entry.stbEntry.parentStbFNames, pReq->parentStbFNames,
           sizeof(char) * pReq->numParents * TSDB_TABLE_FNAME_LEN);
  }
  // batch-meta-txn: mark STB as PRE_CREATE (invisible to other sessions)
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_CREATE;
  }

  code = metaHandleEntry2(pMeta, &entry);
  if (TSDB_CODE_SUCCESS == code) {
    metaInfo("vgId:%d, super table %s suid:%" PRId64 " is created, version:%" PRId64 " txnId:%" PRIu64,
             TD_VID(pMeta->pVnode), pReq->name, pReq->suid, version, pReq->txnId);
    // batch-meta-txn: add to txn.idx for COMMIT/ROLLBACK handling
    if (pReq->txnId != 0) {
      code = metaTxnIdxUpsert(pMeta, pReq->suid, pReq->txnId, META_TXN_PRE_CREATE, 0);
      if (code != TSDB_CODE_SUCCESS) {
        metaError("vgId:%d, failed to upsert txn.idx for stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode),
                  pReq->name, pReq->suid, tstrerror(code));
      }
    }
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

  // batch-meta-txn: handle DROP within transaction
  if (pReq->txnId != 0) {
    SMetaEntry *pExist = NULL;
    metaRLock(pMeta);
    int32_t     fetchCode = metaFetchEntryByUid(pMeta, pReq->suid, &pExist);
    metaULock(pMeta);
    if (fetchCode == 0 && pExist != NULL && pExist->txnId == pReq->txnId) {
      if (pExist->txnStatus == META_TXN_PRE_ALTER) {
        // Same-txn ALTER→DROP: undo ALTER first, then check restored state
        int64_t prevVer = pExist->txnOrigVer;
        metaFetchEntryFree(&pExist);
        if (prevVer >= 0) {
          code = metaRollbackAlterTable(pMeta, pReq->suid, prevVer);
          if (code != 0) {
            metaError("vgId:%d, %s failed to undo ALTER for stb uid:%" PRId64 " name:%s txnId:%" PRIu64,
                      TD_VID(pMeta->pVnode), __func__, pReq->suid, pReq->name, pReq->txnId);
            TAOS_RETURN(code);
          }
          metaRLock(pMeta);
          fetchCode = metaFetchEntryByUid(pMeta, pReq->suid, &pExist);
          metaULock(pMeta);
          if (fetchCode == 0 && pExist != NULL && pExist->txnId == pReq->txnId &&
              pExist->txnStatus == META_TXN_PRE_CREATE) {
            // Fall through to PRE_CREATE undo below
          } else {
            metaFetchEntryFree(&pExist);
            goto _stb_mark_pre_drop;
          }
        } else {
          goto _stb_mark_pre_drop;
        }
      }

      if (pExist != NULL && pExist->txnStatus == META_TXN_PRE_CREATE) {
        // Same-txn CREATE→DROP: undo by physically deleting STB
        metaFetchEntryFree(&pExist);
        SMetaEntry delEntry = {
            .version = verison,
            .type = -TSDB_SUPER_TABLE,
            .uid = pReq->suid,
        };
        code = metaHandleEntry2(pMeta, &delEntry);
        if (code == 0) {
          int32_t idxCode = metaTxnIdxDelete(pMeta, pReq->suid);
          if (idxCode != 0) {
            metaError("vgId:%d, %s failed to cleanup txn.idx for stb uid:%" PRId64 " txnId:%" PRIu64 " code:0x%x",
                      TD_VID(pMeta->pVnode), __func__, pReq->suid, pReq->txnId, idxCode);
            if (idxCode != TSDB_CODE_TXN_NOT_EXIST) {
              TAOS_RETURN(idxCode);
            }
          }
          metaInfo("vgId:%d, stb %s uid %" PRId64 " PRE_CREATE undone (same-txn DROP), txnId:%" PRIu64,
                   TD_VID(pMeta->pVnode), pReq->name, pReq->suid, pReq->txnId);
        } else {
          metaError("vgId:%d, %s failed to undo PRE_CREATE for stb uid:%" PRId64 " name:%s txnId:%" PRIu64,
                    TD_VID(pMeta->pVnode), __func__, pReq->suid, pReq->name, pReq->txnId);
        }
        TAOS_RETURN(code);
      }
      metaFetchEntryFree(&pExist);
    } else {
      metaFetchEntryFree(&pExist);
    }

  _stb_mark_pre_drop:
    // Normal txn DROP: mark as PRE_DROP (snapshot isolation — STB remains visible)
    code = metaMarkTableTxnStatus(pMeta, pReq->suid, pReq->txnId, META_TXN_PRE_DROP, -1);
    if (code) {
      metaError("vgId:%d, %s failed to mark PRE_DROP for stb uid:%" PRId64 " name:%s txnId:%" PRIu64,
                TD_VID(pMeta->pVnode), __func__, pReq->suid, pReq->name, pReq->txnId);
    } else {
      int32_t idxCode = metaTxnIdxUpsert(pMeta, pReq->suid, pReq->txnId, META_TXN_PRE_DROP, -1);
      if (idxCode != 0) {
        metaError("vgId:%d, %s failed to upsert txn.idx for stb uid:%" PRId64 " txnId:%" PRIu64 " code:0x%x",
                  TD_VID(pMeta->pVnode), __func__, pReq->suid, pReq->txnId, idxCode);
        if (idxCode != TSDB_CODE_TXN_NOT_EXIST) {
          TAOS_RETURN(idxCode);
        }
      }
      metaInfo("vgId:%d, stb %s uid %" PRId64 " marked PRE_DROP, txnId:%" PRIu64, TD_VID(pMeta->pVnode), pReq->name,
               pReq->suid, pReq->txnId);
    }
    TAOS_RETURN(code);
  }

  // Non-txn path: physical drop
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
  int32_t     code = TSDB_CODE_SUCCESS;
  void       *value = NULL;
  int32_t     valueSize = 0;
  SMetaInfo   info;
  SMetaEntry *pStbEntry = NULL;

  if (NULL == pReq->name || strlen(pReq->name) == 0 || NULL == pReq->ctb.stbName || strlen(pReq->ctb.stbName) == 0 ||
      pReq->ctb.suid == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid name:%s stb name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->name, pReq->ctb.stbName, version);
    return TSDB_CODE_INVALID_MSG;
  }

  // check table existence
  // Hold metaRLock only for pNameIdx B+tree read; release before metaGetInfo
  // to avoid nested rdlock deadlock (writer-preference policy).
  metaRLock(pMeta);
  int32_t nameFound = (tdbTbGet(pMeta->pNameIdx, pReq->name, strlen(pReq->name) + 1, &value, &valueSize) == 0);
  if (nameFound) {
    pReq->uid = *(int64_t *)value;
    tdbFreeClear(value);
  }
  metaULock(pMeta);

  if (nameFound) {
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

    // Batch meta txn: if existing entry is a PRE_CREATE shadow from another txn,
    // return TXN_CONFLICT instead of TABLE_ALREADY_EXIST
    // Also: if the owning txn is ROLLEDBACK, treat entry as non-existent (allow CREATE)
    // Also: if the entry is PRE_DROP+COMMITTED, table is logically gone (allow CREATE)
    // Fast path: skip expensive fetch when no txns are active.
    if (metaHasPendingTxnEntries(pMeta)) {
      SMetaEntry *pExist = NULL;
      metaRLock(pMeta);
      int32_t fetchRet = metaFetchEntryByUid(pMeta, pReq->uid, &pExist);
      metaULock(pMeta);
      if (fetchRet == 0 && pExist != NULL) {
        if (pExist->txnId != 0) {
          int8_t finalStatus = metaGetTxnMetaStatus(pMeta, pExist->txnId);
          if (pExist->txnStatus == META_TXN_PRE_CREATE && finalStatus == TXN_META_ROLLEDBACK) {
            // Rolled-back PRE_CREATE: treat as non-existent (vacuum will clean up)
            metaFetchEntryFree(&pExist);
            goto _check_stb;
          }
          if (pExist->txnStatus == META_TXN_PRE_DROP && finalStatus == TXN_META_COMMITTED) {
            // Committed PRE_DROP: table is logically deleted (vacuum will clean up), allow CREATE
            metaFetchEntryFree(&pExist);
            goto _check_stb;
          }
          if (pExist->txnStatus == META_TXN_PRE_CREATE && pExist->txnId != pReq->txnId) {
            metaFetchEntryFree(&pExist);
            return TSDB_CODE_TXN_RESOURCE_BUSY;
          }
        }
        metaFetchEntryFree(&pExist);
      }
    }

    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

_check_stb:
  // check super table existence — use UID-based lookup (cheaper than name→UID→entry chain)
  // metaRLock guards metaFetchEntryByUid's tdb reads.
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pReq->ctb.suid, &pStbEntry);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table %s (suid %" PRId64 ") does not exist, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, pReq->ctb.suid, version);
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
  }

  if (pStbEntry->type != TSDB_SUPER_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s is not a super table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->ctb.stbName, version);
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

  if (pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
    code = metaValidateVirtualChildColRefReq(pMeta, version, pReq, pStbEntry);
    if (code) {
      metaFetchEntryFree(&pStbEntry);
      return code;
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
  if (NULL == *ppRsp) {
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
  // NOTE: locking is done inside metaCheckCreateChildTableReq per-operation
  // to avoid nested rdlock deadlock with metaGetInfo's internal rdlock.
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
  // Batch meta txn: shadow-in-B+tree — write with PRE_CREATE status, invisible until COMMIT
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_CREATE;
  }

  // build response
  code = metaBuildCreateChildTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
  }
  if (ppRsp && *ppRsp) {
    tstrncpy((*ppRsp)->stbName, pReq->ctb.stbName, TSDB_TABLE_NAME_LEN);
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

    // Batch meta txn: if existing entry is a PRE_CREATE shadow from another txn,
    // return TXN_CONFLICT instead of TABLE_ALREADY_EXIST
    // Also: if the owning txn is ROLLEDBACK, treat entry as non-existent (allow CREATE)
    // Also: if the entry is PRE_DROP+COMMITTED, table is logically gone (allow CREATE)
    // Fast path: skip expensive fetch when no txns are active.
    if (metaHasPendingTxnEntries(pMeta)) {
      SMetaEntry *pExist = NULL;
      if (metaFetchEntryByUid(pMeta, pReq->uid, &pExist) == 0 && pExist != NULL) {
        if (pExist->txnId != 0) {
          int8_t finalStatus = metaGetTxnMetaStatus(pMeta, pExist->txnId);
          if (pExist->txnStatus == META_TXN_PRE_CREATE && finalStatus == TXN_META_ROLLEDBACK) {
            metaFetchEntryFree(&pExist);
            goto _grant;
          }
          if (pExist->txnStatus == META_TXN_PRE_DROP && finalStatus == TXN_META_COMMITTED) {
            metaFetchEntryFree(&pExist);
            goto _grant;
          }
          if (pExist->txnStatus == META_TXN_PRE_CREATE && pExist->txnId != pReq->txnId) {
            metaFetchEntryFree(&pExist);
            return TSDB_CODE_TXN_RESOURCE_BUSY;
          }
        }
        metaFetchEntryFree(&pExist);
      }
    }

    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

_grant:
  // grant check
  code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, tstrerror(code), version, pReq->name);
  }
  return code;
}

static int32_t metaAppendNtbTagSchemaToRsp(SMetaEntry *pEntry, STableMetaRsp *pRsp);

static int32_t metaBuildCreateNormalTableRsp(SMeta *pMeta, SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ppRsp) {
    return code;
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    return terrno;
  }

  code = metaUpdateMetaRsp(pEntry->uid, pEntry->name, &pEntry->ntbEntry.schemaRow, pEntry->pExtSchemas,
                           pEntry->ntbEntry.ownerId, *ppRsp);
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

  // owned tags (CREATE TABLE ... TAGS on a plain normal table): append the tag schema so the
  // client catalog does not cache a tag-less meta — same fix as the virtual-normal-table path.
  code = metaAppendNtbTagSchemaToRsp(pEntry, *ppRsp);
  if (code) {
    // pSchemas/pSchemaExt are already populated by metaUpdateMetaRsp above — deep-free them
    // (mirrors metaBuildCreateVirtualNormalTableRsp), else the shallow free leaks both buffers.
    tFreeSTableMetaRsp(*ppRsp);
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  return code;
}

// ncid is the shared counter for both columns and tags (metaAddTableTag bumps it).
// When tags are declared at create time, ncid must continue past the last tag cid,
// otherwise a subsequent ADD TAG would reuse an already-assigned tag cid.
// ncid is int32_t in SMetaEntry; keep nextCid int32_t too. With 32767 columns the next
// cid is 32768, which overflows int16_t to a negative value and would silently bypass the
// INT16_MAX upper-bound checks in metaAddTableColumn/metaAddTableTag.
// schemaRow.nCols >= 1 is guaranteed by the parser in the normal path; reject a malformed
// request with an empty column schema instead of indexing pSchema[-1].
static int32_t metaNtbNextCid(const SVCreateTbReq *pReq, int32_t *pNextCid) {
  if (pReq->ntb.schemaRow.nCols <= 0 || pReq->ntb.schemaRow.pSchema == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t nextCid = pReq->ntb.schemaRow.pSchema[pReq->ntb.schemaRow.nCols - 1].colId + 1;
  if (pReq->ntb.schemaTag.nCols > 0 && pReq->ntb.schemaTag.pSchema != NULL) {
    int32_t lastTagCid = pReq->ntb.schemaTag.pSchema[pReq->ntb.schemaTag.nCols - 1].colId + 1;
    if (lastTagCid > nextCid) nextCid = lastTagCid;
  }
  *pNextCid = nextCid;
  return TSDB_CODE_SUCCESS;
}

static int32_t metaCreateNormalTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  // metaRLock guards tdb reads against concurrent vacuum mutations under metaWLock.
  metaRLock(pMeta);
  code = metaCheckCreateNormalTableReq(pMeta, version, pReq);
  metaULock(pMeta);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  // validate column compress algorithms (only when colCmpr is present)
  if (pReq->colCmpr.nCols > 0 && pReq->colCmpr.pColCmpr != NULL) {
    for (int32_t i = 0; i < pReq->colCmpr.nCols && i < pReq->ntb.schemaRow.nCols; i++) {
      SColCmpr *pCmpr = &pReq->colCmpr.pColCmpr[i];
      SSchema  *pSchema = &pReq->ntb.schemaRow.pSchema[i];
      if (pCmpr->id != pSchema->colId) {
        metaError("vgId:%d, %s failed at %s:%d since colCmpr[%d].id(%d) != schema.colId(%d), version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, i, pCmpr->id, pSchema->colId, version);
        TAOS_RETURN(TSDB_CODE_INVALID_MSG);
      }
      code = validColCmprByType(pSchema->type, pCmpr->alg);
      if (code != TSDB_CODE_SUCCESS) {
        metaError("vgId:%d, %s failed at %s:%d since %s, col:%s version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code), pSchema->name, version);
        TAOS_RETURN(code);
      }
    }
  }

  int32_t nextCid = 0;
  code = metaNtbNextCid(pReq, &nextCid);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, tstrerror(code), version, pReq->name);
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
      .ntbEntry.schemaTag = pReq->ntb.schemaTag,
      .ntbEntry.pTags = pReq->ntb.pTags,
      .ntbEntry.ncid = nextCid,
      .ntbEntry.ownerId = pReq->ntb.userId,
      .colCmpr = pReq->colCmpr,
      .pExtSchemas = pReq->pExtSchemas,
  };
  TABLE_SET_COL_COMPRESSED(entry.flags);
  // Batch meta txn: shadow-in-B+tree — write with PRE_CREATE status, invisible until COMMIT
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_CREATE;
  }

  // build response
  code = metaBuildCreateNormalTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    TAOS_RETURN(code);
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

// Append the owned-tag schema to a meta response (columns first, tags right after them), so the
// client catalog caches a complete meta. A tag-less response lets the client catalog cache stale
// meta, so same-connection tag reads (WHERE <tag> / DROP TAG) fail with Invalid column/tag name
// until a fresh RPC refreshes it. Also stamps tversion so every response path (create and alter
// alike) reports the same tag schema version vnodeGetTableMeta would.
static int32_t metaAppendNtbTagSchemaToRsp(SMetaEntry *pEntry, STableMetaRsp *pRsp) {
  if (pEntry->ntbEntry.schemaTag.nCols > 0 && pEntry->ntbEntry.schemaTag.pSchema != NULL) {
    int32_t nCols = pRsp->numOfColumns;
    int32_t nTags = pEntry->ntbEntry.schemaTag.nCols;
    SSchema *pNew = taosMemoryRealloc(pRsp->pSchemas, (nCols + nTags) * sizeof(SSchema));
    if (NULL == pNew) return terrno;
    pRsp->pSchemas = pNew;
    memcpy(pRsp->pSchemas + nCols, pEntry->ntbEntry.schemaTag.pSchema, nTags * sizeof(SSchema));
    pRsp->numOfTags = nTags;
    pRsp->tversion = pEntry->ntbEntry.schemaTag.version;
  }
  return TSDB_CODE_SUCCESS;
}

// Fill pRsp with a complete normal/virtual-normal-table meta: columns + colRef + owned-tag schema.
// Shared by create / alter-add-tag / alter-drop-tag responses.
static int32_t metaFillNtbTableMetaRsp(SMetaEntry *pEntry, const char *tbName, STableMetaRsp *pRsp) {
  int32_t code = metaUpdateVtbMetaRsp(pEntry, (char *)tbName, &pEntry->ntbEntry.schemaRow, NULL, &pEntry->colRef,
                                      pEntry->pExtSchemas, pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
  if (code) return code;
  // tversion must track the owned-tag schema version. Without it an ADD/DROP TAG response carries
  // tversion=0 with unchanged sversion/rversion (plain normal tables bump neither), so the client
  // catalog's version comparison (ctgWriteTbMetaToCache) discards the update as stale and the
  // connection keeps a tagless/tagged-outdated cached meta. Mirrors vnodeGetTableMeta, which
  // already reports schemaTag.version as tversion.
  pRsp->tversion = pEntry->ntbEntry.schemaTag.version;
  // plain normal tables carry column compress options in colCmpr (mirrors the metaUpdateMetaRsp
  // response paths, e.g. metaAddTableColumn); metaFillRspSchemaExt only fills colId/typeMod.
  if (pEntry->type == TSDB_NORMAL_TABLE) {
    for (int32_t i = 0; i < pEntry->colCmpr.nCols; i++) {
      SColCmpr *p = &pEntry->colCmpr.pColCmpr[i];
      pRsp->pSchemaExt[i].colId = p->id;
      pRsp->pSchemaExt[i].compress = p->alg;
    }
  }
  return metaAppendNtbTagSchemaToRsp(pEntry, pRsp);
}

static int32_t metaBuildCreateVirtualNormalTableRsp(SMeta *pMeta, SMetaEntry *pEntry, STableMetaRsp **ppRsp) {
  if (NULL == ppRsp) return TSDB_CODE_SUCCESS;

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) return terrno;

  int32_t code = metaFillNtbTableMetaRsp(pEntry, pEntry->name, *ppRsp);
  if (code) {
    tFreeSTableMetaRsp(*ppRsp);
    taosMemoryFreeClear(*ppRsp);
  }
  return code;
}

static int32_t metaCreateVirtualNormalTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  // check request
  // metaRLock guards tdb reads against concurrent vacuum mutations under metaWLock.
  metaRLock(pMeta);
  int32_t code = metaCheckCreateNormalTableReq(pMeta, version, pReq);
  metaULock(pMeta);
  if (code) {
    if (TSDB_CODE_TDB_TABLE_ALREADY_EXIST != code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
    }
    TAOS_RETURN(code);
  }

  int32_t nextCid = 0;
  code = metaNtbNextCid(pReq, &nextCid);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, tstrerror(code), version, pReq->name);
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
                      .ntbEntry.schemaTag = pReq->ntb.schemaTag,
                      .ntbEntry.pTags = pReq->ntb.pTags,
                      .ntbEntry.ncid = nextCid,
                      .ntbEntry.ownerId = pReq->ntb.userId,
                      .pExtSchemas = pReq->pExtSchemas,
                      .colRef = pReq->colRef,
                      .series = pReq->series};
  // Batch meta txn: shadow-in-B+tree — write with PRE_CREATE status, invisible until COMMIT
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_CREATE;
  }

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

  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  code = metaUpdateVtbMetaRsp(pEntry, pEntry->name, &pSuper->stbEntry.schemaRow, &pSuper->stbEntry.schemaTag, &pEntry->colRef, pSuper->pExtSchemas,
                              pSuper->stbEntry.ownerId, *ppRsp, TSDB_VIRTUAL_CHILD_TABLE);
  if (code) {
    metaFetchEntryFree(&pSuper);
    taosMemoryFreeClear(*ppRsp);
    return code;
  }

  // the tag region (vstb tags + own tags) is already appended by metaUpdateVtbMetaRsp so the
  // client can resolve tag names (e.g. ALTER TABLE vctb SET TAG vt1=N inside a transaction)

  (*ppRsp)->suid = pEntry->ctbEntry.suid;
  metaFetchEntryFree(&pSuper);

  return code;
}

// Set up the own (extension) schema of a virtual child table at create time (CREATE VTABLE
// declaring own columns/tags, e.g. a SHOW CREATE replay). Validates the defs against the
// vstb schema, assigns colIds from TSDB_VCHILD_OWN_COL_ID_BASE upwards (columns first, then
// tags, both in request order), fills pEntry->own, merges own tag literal values into the
// shared tag blob, and appends own tag references to the sparse colRef.pTagRef array.
//
// Memory: the buffers attached to pEntry (own.*, *ppMergedTagRefs, *ppMergedTagBlob) are
// owned by the caller and must stay valid until metaHandleEntry2/metaBuild...Rsp return
// (both encode synchronously); ref entries are shallow copies, their tagCondJson stays
// owned by pReq.
static int32_t metaSetupVChildOwnSchema(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, SMetaEntry *pEntry,
                                        SColRef **ppMergedTagRefs, uint8_t **ppMergedTagBlob) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaEntry *pSuper = NULL;

  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }

  SSchemaWrapper *pStbSchema = &pSuper->stbEntry.schemaRow;
  SSchemaWrapper *pStbTagSchema = &pSuper->stbEntry.schemaTag;
  int32_t         rowSize = 0;
  int32_t         tagsLen = 0;
  for (int32_t i = 0; i < pStbSchema->nCols; i++) {
    rowSize += pStbSchema->pSchema[i].bytes;
  }
  for (int32_t i = 0; i < pStbTagSchema->nCols; i++) {
    tagsLen += pStbTagSchema->pSchema[i].bytes;
  }

  // the shared tag blob is rebuilt with the merged tag schema, which does not support a
  // vstb JSON tag (same restriction as metaAddVChildOwnTag)
  if (pReq->nOwnTagDefs > 0 && pStbTagSchema->nCols == 1 &&
      pStbTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since super table %s has json tag, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pSuper->name, version);
    code = TSDB_CODE_PAR_ONLY_ONE_JSON_TAG;
    goto _exit;
  }

  // colId space exhaustion check (shared by own columns and own tags)
  if ((int64_t)TSDB_VCHILD_OWN_COL_ID_BASE + pReq->nOwnColDefs + pReq->nOwnTagDefs - 1 > INT16_MAX) {
    metaError("vgId:%d, %s failed at %s:%d since own column/tag count exceeds max column id, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, version);
    code = TSDB_CODE_VND_EXCEED_MAX_COL_ID;
    goto _exit;
  }

  // validate own columns: mandatory ref, name dup, type and merged limits
  for (int32_t i = 0; i < pReq->nOwnColDefs; i++) {
    SVChildOwnColDef *pDef = &pReq->pOwnColDefs[i];
    if (!pDef->ref.hasRef || pDef->ref.refDbName[0] == '\0' || pDef->ref.refTableName[0] == '\0' ||
        pDef->ref.refColName[0] == '\0') {
      metaError("vgId:%d, %s failed at %s:%d since own column %s without reference, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pDef->schema.name, version);
      code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
      goto _exit;
    }
    for (int32_t j = 0; j < pStbSchema->nCols; j++) {
      if (strncmp(pStbSchema->pSchema[j].name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    for (int32_t j = 0; j < pStbTagSchema->nCols; j++) {
      if (strncmp(pStbTagSchema->pSchema[j].name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    for (int32_t j = 0; j < i; j++) {
      if (strncmp(pReq->pOwnColDefs[j].schema.name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    rowSize += pDef->schema.bytes;
  }
  if (pStbSchema->nCols + pReq->nOwnColDefs > TSDB_MAX_COLUMNS) {
    metaError("vgId:%d, %s failed at %s:%d since too many columns, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    code = TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    goto _exit;
  }
  if (rowSize > TSDB_MAX_BYTES_PER_ROW_VIRTUAL) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, rowSize, TSDB_MAX_BYTES_PER_ROW_VIRTUAL, version);
    code = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
    goto _exit;
  }

  // validate own tags: name dup (incl. against own columns), type and merged limits
  for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
    SVChildOwnTagDef *pDef = &pReq->pOwnTagDefs[i];
    if (pDef->schema.type == TSDB_DATA_TYPE_JSON) {
      code = TSDB_CODE_PAR_ONLY_ONE_JSON_TAG;
      goto _exit;
    }
    if (IS_DECIMAL_TYPE(pDef->schema.type)) {
      code = TSDB_CODE_PAR_INVALID_COLUMN;
      goto _exit;
    }
    if ((pDef->schema.type == TSDB_DATA_TYPE_VARCHAR || pDef->schema.type == TSDB_DATA_TYPE_VARBINARY ||
         pDef->schema.type == TSDB_DATA_TYPE_NCHAR || pDef->schema.type == TSDB_DATA_TYPE_GEOMETRY) &&
        pDef->schema.bytes > TSDB_MAX_TAGS_LEN) {
      code = TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
      goto _exit;
    }
    for (int32_t j = 0; j < pStbSchema->nCols; j++) {
      if (strncmp(pStbSchema->pSchema[j].name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    for (int32_t j = 0; j < pStbTagSchema->nCols; j++) {
      if (strncmp(pStbTagSchema->pSchema[j].name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    for (int32_t j = 0; j < pReq->nOwnColDefs; j++) {
      if (strncmp(pReq->pOwnColDefs[j].schema.name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    for (int32_t j = 0; j < i; j++) {
      if (strncmp(pReq->pOwnTagDefs[j].schema.name, pDef->schema.name, TSDB_COL_NAME_LEN) == 0) {
        code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
        goto _exit;
      }
    }
    if (pDef->hasValue && pDef->ref.hasRef) {
      metaError("vgId:%d, %s failed at %s:%d since own tag %s has both value and reference, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pDef->schema.name, version);
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    tagsLen += pDef->schema.bytes;
  }
  if (pStbTagSchema->nCols + pReq->nOwnTagDefs > TSDB_MAX_TAGS) {
    metaError("vgId:%d, %s failed at %s:%d since too many tags, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, version);
    code = TSDB_CODE_PAR_INVALID_TAGS_NUM;
    goto _exit;
  }
  if (tagsLen > TSDB_MAX_TAGS_LEN) {
    metaError("vgId:%d, %s failed at %s:%d since tags length %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tagsLen, TSDB_MAX_TAGS_LEN, version);
    code = TSDB_CODE_PAR_INVALID_TAGS_LENGTH;
    goto _exit;
  }

  // fill the own schema area; colIds are assigned in request order, columns first
  col_id_t nextColId = TSDB_VCHILD_OWN_COL_ID_BASE;
  if (pReq->nOwnColDefs > 0) {
    pEntry->own.pOwnColSchemas = taosMemoryCalloc(pReq->nOwnColDefs, sizeof(SSchema));
    pEntry->own.pOwnColRefs = taosMemoryCalloc(pReq->nOwnColDefs, sizeof(SColRef));
    if (NULL == pEntry->own.pOwnColSchemas || NULL == pEntry->own.pOwnColRefs) {
      code = terrno;
      goto _exit;
    }
    pEntry->own.nOwnCols = pReq->nOwnColDefs;
    // allocate own-col ext only when at least one own column carries a typeMod (decimal); matches
    // the wire/persistence condition so non-decimal own columns stay NULL (no ext storage).
    bool hasOwnColTypeMod = false;
    for (int32_t i = 0; i < pReq->nOwnColDefs; i++) {
      if (pReq->pOwnColDefs[i].schema.flags & COL_HAS_TYPE_MOD) {
        hasOwnColTypeMod = true;
        break;
      }
    }
    if (hasOwnColTypeMod) {
      pEntry->own.pOwnColExtSchemas = taosMemoryCalloc(pReq->nOwnColDefs, sizeof(SExtSchema));
      if (NULL == pEntry->own.pOwnColExtSchemas) {
        code = terrno;
        goto _exit;
      }
    }
    for (int32_t i = 0; i < pReq->nOwnColDefs; i++) {
      pEntry->own.pOwnColSchemas[i] = pReq->pOwnColDefs[i].schema;
      pEntry->own.pOwnColSchemas[i].colId = nextColId;
      pEntry->own.pOwnColRefs[i] = pReq->pOwnColDefs[i].ref;  // shallow: tagCondJson stays owned by pReq
      pEntry->own.pOwnColRefs[i].id = nextColId;
      pEntry->own.pOwnColRefs[i].hasRef = true;
      if (pEntry->own.pOwnColExtSchemas != NULL) {
        pEntry->own.pOwnColExtSchemas[i].typeMod = pReq->pOwnColDefs[i].typeMod;
      }
      nextColId++;
    }
  }
  if (pReq->nOwnTagDefs > 0) {
    pEntry->own.pOwnTagSchemas = taosMemoryCalloc(pReq->nOwnTagDefs, sizeof(SSchema));
    if (NULL == pEntry->own.pOwnTagSchemas) {
      code = terrno;
      goto _exit;
    }
    pEntry->own.nOwnTags = pReq->nOwnTagDefs;
    for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
      pEntry->own.pOwnTagSchemas[i] = pReq->pOwnTagDefs[i].schema;
      pEntry->own.pOwnTagSchemas[i].colId = nextColId++;
    }
  }

  // own tag references: append to the sparse pTagRef array (a merged copy; the array
  // borrowed from pReq is left untouched)
  int32_t nOwnTagRefs = 0;
  for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
    if (pReq->pOwnTagDefs[i].ref.hasRef) {
      nOwnTagRefs++;
    }
  }
  if (nOwnTagRefs > 0) {
    int32_t nOld = pEntry->colRef.nTagRefs;
    SColRef *pMerged = taosMemoryCalloc(nOld + nOwnTagRefs, sizeof(SColRef));
    if (NULL == pMerged) {
      code = terrno;
      goto _exit;
    }
    for (int32_t i = 0; i < nOld; i++) {
      pMerged[i] = pEntry->colRef.pTagRef[i];  // shallow: tagCondJson stays owned by pReq
    }
    int32_t idx = nOld;
    for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
      if (!pReq->pOwnTagDefs[i].ref.hasRef) {
        continue;
      }
      pMerged[idx] = pReq->pOwnTagDefs[i].ref;  // shallow: tagCondJson stays owned by pReq
      pMerged[idx].id = pEntry->own.pOwnTagSchemas[i].colId;
      pMerged[idx].hasRef = true;
      idx++;
    }
    pEntry->colRef.pTagRef = pMerged;
    pEntry->colRef.nTagRefs = nOld + nOwnTagRefs;
    *ppMergedTagRefs = pMerged;
  }

  // own tag literal values: merge into the shared tag blob (tTagNew sorts by colId and own
  // colIds are always greater than any vstb colId, so the blob order matches the merged
  // tag schema order)
  bool hasOwnTagValue = false;
  for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
    if (pReq->pOwnTagDefs[i].hasValue) {
      hasOwnTagValue = true;
      break;
    }
  }
  if (hasOwnTagValue) {
    SArray *pTagVals = NULL;
    if (NULL != pEntry->ctbEntry.pTags) {
      code = tTagToValArray((const STag *)pEntry->ctbEntry.pTags, &pTagVals);
      if (code) {
        // tTagToValArray may leave pTagVals allocated (NULL or partially filled) on failure;
        // _exit does not destroy it, so release it here (taosArrayDestroy is NULL-safe)
        taosArrayDestroy(pTagVals);
        goto _exit;
      }
    } else {
      pTagVals = taosArrayInit(pReq->nOwnTagDefs, sizeof(STagVal));
      if (NULL == pTagVals) {
        code = terrno;
        goto _exit;
      }
    }
    for (int32_t i = 0; i < pReq->nOwnTagDefs; i++) {
      SVChildOwnTagDef *pDef = &pReq->pOwnTagDefs[i];
      if (!pDef->hasValue) {
        continue;
      }
      STagVal val = {.cid = pEntry->own.pOwnTagSchemas[i].colId, .type = pDef->valType};
      if (IS_VAR_DATA_TYPE(pDef->valType)) {
        val.nData = pDef->valLen;
        val.pData = pDef->pVal;  // borrowed; tTagNew serializes the content
      } else {
        val.i64 = pDef->valI64;
      }
      if (NULL == taosArrayPush(pTagVals, &val)) {
        taosArrayDestroy(pTagVals);
        code = terrno;
        goto _exit;
      }
    }
    STag *pNewTag = NULL;
    code = tTagNew(pTagVals, pStbTagSchema->version, false, &pNewTag);
    taosArrayDestroy(pTagVals);
    if (code) {
      goto _exit;
    }
    pEntry->ctbEntry.pTags = (uint8_t *)pNewTag;
    *ppMergedTagBlob = (uint8_t *)pNewTag;
  }

  metaInfo("vgId:%d, table %s created with %d own columns and %d own tags, version:%" PRId64, TD_VID(pMeta->pVnode),
           pReq->name, pReq->nOwnColDefs, pReq->nOwnTagDefs, version);

_exit:
  metaFetchEntryFree(&pSuper);
  TAOS_RETURN(code);
}

static int32_t metaCreateVirtualChildTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq, STableMetaRsp **ppRsp) {
  // check request
  // NOTE: locking is done inside metaCheckCreateChildTableReq per-operation
  // to avoid nested rdlock deadlock with metaGetInfo's internal rdlock.
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
                      .colRef = pReq->colRef,
                      .series = pReq->series};
  // Batch meta txn: shadow-in-B+tree — write with PRE_CREATE status, invisible until COMMIT
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_CREATE;
  }

  // own (extension) columns/tags declared at create time (SHOW CREATE replay)
  SColRef *pOwnMergedTagRefs = NULL;
  uint8_t *pOwnMergedTagBlob = NULL;
  if (pReq->nOwnColDefs > 0 || pReq->nOwnTagDefs > 0) {
    code = metaSetupVChildOwnSchema(pMeta, version, pReq, &entry, &pOwnMergedTagRefs, &pOwnMergedTagBlob);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " name:%s", TD_VID(pMeta->pVnode), __func__,
                __FILE__, __LINE__, tstrerror(code), version, pReq->name);
      goto _exit;
    }
  }

  code = metaBuildCreateVirtualChildTableRsp(pMeta, &entry, ppRsp);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    goto _exit;
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

_exit:
  // temp own-schema buffers: encoding above is synchronous, the entry only borrows them
  taosMemoryFreeClear(entry.own.pOwnColSchemas);
  taosMemoryFreeClear(entry.own.pOwnColRefs);
  taosMemoryFreeClear(entry.own.pOwnColExtSchemas);
  taosMemoryFreeClear(entry.own.pOwnTagSchemas);
  taosMemoryFreeClear(pOwnMergedTagRefs);
  taosMemoryFreeClear(pOwnMergedTagBlob);
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

/**
 * Mark an existing entry with txnId/txnStatus in-place (shadow-in-B+tree).
 * Reads the entry from pTbDb, updates txnId/txnStatus, re-encodes, and writes back.
 * Indexes are NOT modified — the entry remains visible but filtered by txnStatus.
 */
int32_t metaMarkTableTxnStatus(SMeta *pMeta, int64_t uid, int64_t txnId, int8_t txnStatus, int64_t txnOrigVer) {
  int32_t code = TSDB_CODE_SUCCESS;
  void   *uidValue = NULL, *tbValue = NULL;
  int32_t uidValueSize = 0, tbValueSize = 0;

  // Serialize concurrent foreground/vacuum updates to pTbDb/pUidIdx via shared
  // pMeta->txn handle; without this lock, btree corruption (e.g. invalid idx
  // N, nCells N-1) can occur during async vacuum-commit.
  metaWLock(pMeta);

  // Read current version from uid index
  code = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &uidValue, &uidValueSize);
  if (code) {
    metaULock(pMeta);
    metaError("vgId:%d, mark txn status: uid %" PRId64 " not found", TD_VID(pMeta->pVnode), uid);
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }

  int64_t  version = ((SUidIdxVal *)uidValue)->version;
  STbDbKey key = {.version = version, .uid = uid};
  tdbFreeClear(uidValue);

  // Read the entry from B+ tree
  code = tdbTbGet(pMeta->pTbDb, &key, sizeof(key), &tbValue, &tbValueSize);
  if (code) {
    metaULock(pMeta);
    metaError("vgId:%d, mark txn status: entry not found for uid %" PRId64 " ver %" PRId64, TD_VID(pMeta->pVnode), uid,
              version);
    return TSDB_CODE_INTERNAL_ERROR;
  }

  // Decode
  SDecoder   decoder = {0};
  SMetaEntry entry = {0};
  tDecoderInit(&decoder, tbValue, tbValueSize);
  code = metaDecodeEntry(&decoder, &entry);
  if (code) {
    tDecoderClear(&decoder);
    tdbFreeClear(tbValue);
    metaULock(pMeta);
    metaError("vgId:%d, mark txn status: decode failed for uid %" PRId64, TD_VID(pMeta->pVnode), uid);
    return code;
  }

  // Update txn fields
  entry.txnId = txnId;
  entry.txnStatus = txnStatus;
  entry.txnOrigVer = txnOrigVer;

  // Re-encode and write back to the same key (in-place update)
  // NOTE: decoder/tbValue must stay alive until after encoding because
  // the decoded entry's schema pointers reference decoder-managed memory.
  int32_t  encodeSize = 0;
  SEncoder encoder = {0};
  tEncodeSize(metaEncodeEntry, &entry, encodeSize, code);
  if (code) {
    tDecoderClear(&decoder);
    tdbFreeClear(tbValue);
    metaULock(pMeta);
    return code;
  }

  void *newValue = taosMemoryMalloc(encodeSize);
  if (!newValue) {
    tDecoderClear(&decoder);
    tdbFreeClear(tbValue);
    metaULock(pMeta);
    return terrno;
  }

  tEncoderInit(&encoder, newValue, encodeSize);
  code = metaEncodeEntry(&encoder, &entry);
  tEncoderClear(&encoder);
  tDecoderClear(&decoder);
  tdbFreeClear(tbValue);
  if (code) {
    taosMemoryFree(newValue);
    metaULock(pMeta);
    return code;
  }

  code = tdbTbUpsert(pMeta->pTbDb, &key, sizeof(key), newValue, encodeSize, pMeta->txn);
  taosMemoryFree(newValue);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, mark txn status: write back failed for uid %" PRId64, TD_VID(pMeta->pVnode), uid);
  } else {
    metaInfo("vgId:%d, marked uid %" PRId64 " with txnId %" PRId64 " status %d", TD_VID(pMeta->pVnode), uid, txnId,
             txnStatus);
  }
  return code;
}

/**
 * Rollback an ALTER operation: delete the new version entry from pTbDb,
 * restore pUidIdx to point at the old version, and clear txnId/txnStatus
 * on the old entry.
 *
 * @param pMeta       The meta handle
 * @param uid         The table UID
 * @param prevVersion  The version to restore to
 * @return TSDB_CODE_SUCCESS on success
 */
int32_t metaRollbackAlterTable(SMeta *pMeta, int64_t uid, int64_t prevVersion) {
  int32_t code = TSDB_CODE_SUCCESS;
  void   *uidValue = NULL;
  int32_t uidValueSize = 0;

  // Read current version (the new-version entry created by ALTER)
  metaRLock(pMeta);
  code = tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &uidValue, &uidValueSize);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, rollback alter: uid %" PRId64 " not found in uidIdx", TD_VID(pMeta->pVnode), uid);
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }
  int64_t newVersion = ((SUidIdxVal *)uidValue)->version;
  tdbFreeClear(uidValue);

  if (newVersion == prevVersion) {
    // No new version was created, just clear txnStatus
    return metaMarkTableTxnStatus(pMeta, uid, 0, META_TXN_NORMAL, -1);
  }

  // Read the new-version entry to get schema version for pSkmDb cleanup
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pMeta, META_READER_NOLOCK, 0);
  metaRLock(pMeta);
  int32_t readCode = metaGetTableEntryByVersion(&mr, newVersion, uid);
  metaULock(pMeta);
  if (readCode == 0) {
    // Delete orphan schema entry from pSkmDb (normal tables and STBs only)
    int32_t sver = -1;
    if (mr.me.type == TSDB_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      sver = mr.me.ntbEntry.schemaRow.version;
    } else if (mr.me.type == TSDB_SUPER_TABLE) {
      sver = mr.me.stbEntry.schemaRow.version;
    }
    if (sver >= 0) {
      SSkmDbKey skmKey = {.uid = uid, .sver = sver};
      metaWLock(pMeta);
      int32_t   skmCode = tdbTbDelete(pMeta->pSkmDb, &skmKey, sizeof(skmKey), pMeta->txn);
      metaULock(pMeta);
      if (skmCode == 0) {
        metaInfo("vgId:%d, rollback alter: deleted orphan pSkmDb entry uid %" PRId64 " sver %d", TD_VID(pMeta->pVnode),
                 uid, sver);
      } else {
        metaWarn("vgId:%d, rollback alter: failed to delete pSkmDb entry uid %" PRId64 " sver %d, code:0x%x",
                 TD_VID(pMeta->pVnode), uid, sver, skmCode);
      }
    }
  }

  // Capture info needed to undo the stats change applied by ALTER.
  // Only non-virtual STB and non-virtual NTB contribute to timeseries stats;
  // virtual types (TSDB_VIRTUAL_NORMAL_TABLE, or TSDB_SUPER_TABLE with VIRTUAL flag)
  // never affect stats — exclude them here to avoid a redundant check at apply time.
  // Capture before metaReaderClear — mr.me memory is decoder-managed.
  int8_t  statsType = -1;  // TSDB_SUPER_TABLE / TSDB_NORMAL_TABLE; -1 = skip
  int32_t newNcols = 0;
  char    stbName[TSDB_TABLE_NAME_LEN] = {0};
  if (readCode == 0) {
    if (mr.me.type == TSDB_SUPER_TABLE && !TABLE_IS_VIRTUAL(mr.me.flags)) {
      statsType = TSDB_SUPER_TABLE;
      newNcols = mr.me.stbEntry.schemaRow.nCols;
      tstrncpy(stbName, mr.me.name, TSDB_TABLE_NAME_LEN);
    } else if (mr.me.type == TSDB_NORMAL_TABLE) {
      statsType = TSDB_NORMAL_TABLE;
      newNcols = mr.me.ntbEntry.schemaRow.nCols;
    }
    // TSDB_VIRTUAL_NORMAL_TABLE, TSDB_VIRTUAL_CHILD_TABLE: no stats update needed.
  }
  metaReaderClear(&mr);

  // Rollback child table tag index (pTagIdx) and child index (pCtbIdx) if applicable.
  // Must be done BEFORE deleting the new-version pTbDb entry, since the function
  // reads both old and new entries to reverse the tag index changes.
  {
    int32_t tagCode = metaRollbackChildTableTags(pMeta, uid, prevVersion, newVersion);
    if (tagCode != 0) {
      metaWarn("vgId:%d, rollback alter: metaRollbackChildTableTags failed for uid %" PRId64 ", code:0x%x",
               TD_VID(pMeta->pVnode), uid, tagCode);
    }
  }

  // Fetch ctbNum BEFORE taking WLock below: metaGetStbStats acquires metaRLock
  // internally, calling it while holding metaWLock self-deadlocks (glibc rwlock
  // does not allow a wrlock owner to re-enter rdlock).
  bool    needStbCtbStats = (statsType == TSDB_SUPER_TABLE && !metaTbInFilterCache(pMeta, stbName, 1));
  int64_t stbCtbNum = 0;
  if (needStbCtbStats) {
    int32_t stRet = metaGetStbStats(pMeta->pVnode, uid, &stbCtbNum, 0, 0);
    if (stRet != 0) {
      metaWarn("vgId:%d, rollback alter: metaGetStbStats failed uid %" PRId64
               ", numOfTimeSeries may be inaccurate",
               TD_VID(pMeta->pVnode), uid);
      stbCtbNum = 0;
    }
  }

  // Delete the new-version entry from pTbDb, read old entry, restore pUidIdx,
  // and drop cache — all under WLock to serialize with async vacuum.
  STbDbKey newKey = {.version = newVersion, .uid = uid};
  SUidIdxVal uidVal = {.version = prevVersion};
  metaWLock(pMeta);
  code = tdbTbDelete(pMeta->pTbDb, &newKey, sizeof(newKey), pMeta->txn);
  if (code) {
    metaULock(pMeta);
    metaError("vgId:%d, rollback alter: failed to delete new ver %" PRId64 " for uid %" PRId64, TD_VID(pMeta->pVnode),
              newVersion, uid);
    return code;
  }

  // Restore pUidIdx to point at old version with correct suid/skmVer
  {
    SMetaReader mr2 = {0};
    metaReaderDoInit(&mr2, pMeta, META_READER_NOLOCK, 0);
    int32_t readOldCode = metaGetTableEntryByVersion(&mr2, prevVersion, uid);
    if (readOldCode == 0) {
      if (mr2.me.type == TSDB_SUPER_TABLE) {
        uidVal.suid = mr2.me.uid;
        uidVal.skmVer = mr2.me.stbEntry.schemaRow.version;
      } else if (mr2.me.type == TSDB_CHILD_TABLE || mr2.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
        uidVal.suid = mr2.me.ctbEntry.suid;
      } else if (mr2.me.type == TSDB_NORMAL_TABLE || mr2.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
        uidVal.skmVer = mr2.me.ntbEntry.schemaRow.version;
      }

      // Undo the stats applied optimistically when ALTER was written:
      //   STB: numOfTimeSeries  += ctbNum * (newNcols - oldNcols)
      //   NTB: numOfNTimeSeries += (newNcols - oldNcols)
      // statsType is only set for non-virtual STB and non-virtual NTB (captured above),
      // so no further virtual-table guard is needed here.
      if (statsType != -1) {
        int32_t oldNcols =
            (statsType == TSDB_SUPER_TABLE) ? mr2.me.stbEntry.schemaRow.nCols : mr2.me.ntbEntry.schemaRow.nCols;
        int32_t undoDelta = oldNcols - newNcols;
        if (undoDelta != 0) {
          if (statsType == TSDB_SUPER_TABLE && needStbCtbStats) {
            pMeta->pVnode->config.vndStats.numOfTimeSeries += stbCtbNum * undoDelta;
          } else if (statsType == TSDB_NORMAL_TABLE) {
            pMeta->pVnode->config.vndStats.numOfNTimeSeries += undoDelta;
          }
        }
      }
    } else {
      // Old entry missing from pTbDb — likely data corruption.
      // Must still write pUidIdx (otherwise table becomes invisible), but skmVer=0
      // will cause client schema mismatch on INSERT. Log ERROR for operator awareness.
      metaError("vgId:%d, rollback alter: cannot read prev entry ver %" PRId64 " uid %" PRId64
                ", skmVer will be 0 (code:0x%x). Potential data corruption",
                TD_VID(pMeta->pVnode), prevVersion, uid, readOldCode);
    }
    metaReaderClear(&mr2);
  }
  code = tdbTbUpsert(pMeta->pUidIdx, &uid, sizeof(uid), &uidVal, sizeof(uidVal), pMeta->txn);
  if (code) {
    metaULock(pMeta);
    metaError("vgId:%d, rollback alter: failed to restore uidIdx for uid %" PRId64 " to ver %" PRId64,
              TD_VID(pMeta->pVnode), uid, prevVersion);
    return code;
  }

  // Do NOT clear txnId/txnStatus on the old entry — it retains its original state.
  // For pre-existing tables: old entry already has txnId=0, txnStatus=NORMAL.
  // For same-txn CREATE→ALTER: old entry retains PRE_CREATE, enabling chained rollback.

  // Drop the meta cache entry so next lookup reads the restored version from pUidIdx.
  // The cache only updates to higher versions (never downgrades), so without this
  // the stale cache entry would point to the deleted new-version entry.
  (void)metaCacheDrop(pMeta, uid);
  metaULock(pMeta);

  metaInfo("vgId:%d, rollback alter: uid %" PRId64 " restored to version %" PRId64, TD_VID(pMeta->pVnode), uid,
           prevVersion);
  return code;
}

int32_t metaDropTable2(SMeta *pMeta, int64_t version, SVDropTbReq *pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check request
  // NOTE: locking is done inside metaCheckDropTableReq per-operation
  // to avoid nested rdlock deadlock with metaGetInfo's internal rdlock.
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

  // Batch meta txn: handle DROP within transaction.
  if (pReq->txnId != 0) {
    // Check if the table was created (or altered) within the same txn.
    // PRE_CREATE: simple undo (physically delete the entry).
    // PRE_ALTER from same txn: undo ALTER first, then check if restored entry is PRE_CREATE.
    // This handles CREATE→DROP and CREATE→ALTER→DROP chains.
    SMetaEntry *pExist = NULL;
    metaRLock(pMeta);
    int32_t     fetchCode = metaFetchEntryByUid(pMeta, pReq->uid, &pExist);
    metaULock(pMeta);
    if (fetchCode == 0 && pExist != NULL && pExist->txnId == pReq->txnId) {
      if (pExist->txnStatus == META_TXN_PRE_ALTER) {
        // Same-txn ALTER→DROP: undo ALTER first to restore previous version
        int64_t prevVer = pExist->txnOrigVer;
        metaFetchEntryFree(&pExist);
        if (prevVer >= 0) {
          code = metaRollbackAlterTable(pMeta, pReq->uid, prevVer);
          if (code != 0) {
            metaError("vgId:%d, %s failed to undo ALTER for uid:%" PRId64 " name:%s txnId:%" PRId64,
                      TD_VID(pMeta->pVnode), __func__, pReq->uid, pReq->name, pReq->txnId);
            TAOS_RETURN(code);
          }
          // Re-fetch to check if restored entry is PRE_CREATE
          metaRLock(pMeta);
          fetchCode = metaFetchEntryByUid(pMeta, pReq->uid, &pExist);
          metaULock(pMeta);
          if (fetchCode == 0 && pExist != NULL && pExist->txnId == pReq->txnId &&
              pExist->txnStatus == META_TXN_PRE_CREATE) {
            // Fall through to PRE_CREATE undo below
          } else {
            // Restored entry is pre-existing (NORMAL) — mark as PRE_DROP
            metaFetchEntryFree(&pExist);
            goto _mark_pre_drop;
          }
        } else {
          // No prevVer, fall through to normal PRE_DROP
          goto _mark_pre_drop;
        }
      }

      if (pExist != NULL && pExist->txnStatus == META_TXN_PRE_CREATE) {
        // Same-txn CREATE→DROP: undo the create by physically deleting the entry
        metaFetchEntryFree(&pExist);
        SMetaEntry delEntry = {
            .version = version,
            .uid = pReq->uid,
        };
        if (pReq->isVirtual) {
          delEntry.type = (pReq->suid == 0) ? -TSDB_VIRTUAL_NORMAL_TABLE : -TSDB_VIRTUAL_CHILD_TABLE;
        } else {
          delEntry.type = (pReq->suid == 0) ? -TSDB_NORMAL_TABLE : -TSDB_CHILD_TABLE;
        }
        code = metaHandleEntry2(pMeta, &delEntry);
        if (code == 0) {
          // Also clean up txn.idx entry that was created during the original CREATE
          int32_t idxCode = metaTxnIdxDelete(pMeta, pReq->uid);
          if (idxCode != 0) {
            metaError("vgId:%d, %s failed to cleanup txn.idx for uid:%" PRId64 " txnId:%" PRId64 " code:0x%x",
                      TD_VID(pMeta->pVnode), __func__, pReq->uid, pReq->txnId, idxCode);
            if (idxCode != TSDB_CODE_TXN_NOT_EXIST) {
              TAOS_RETURN(idxCode);
            }
          }
          metaInfo("vgId:%d, table %s uid %" PRId64 " PRE_CREATE undone (same-txn DROP), txnId:%" PRId64,
                   TD_VID(pMeta->pVnode), pReq->name, pReq->uid, pReq->txnId);
        } else {
          metaError("vgId:%d, %s failed to undo PRE_CREATE for uid:%" PRId64 " name:%s txnId:%" PRId64,
                    TD_VID(pMeta->pVnode), __func__, pReq->uid, pReq->name, pReq->txnId);
        }
        TAOS_RETURN(code);
      }
      metaFetchEntryFree(&pExist);
    } else {
      metaFetchEntryFree(&pExist);
    }

  _mark_pre_drop:
    // Normal txn DROP: mark as PRE_DROP (snapshot isolation — entry remains visible to other sessions)
    code = metaMarkTableTxnStatus(pMeta, pReq->uid, pReq->txnId, META_TXN_PRE_DROP, -1);
    if (code) {
      metaError("vgId:%d, %s failed to mark PRE_DROP for uid:%" PRId64 " name:%s txnId:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, pReq->uid, pReq->name, pReq->txnId);
    } else {
      // Update txn.idx to reflect PRE_DROP status
      int32_t idxCode = metaTxnIdxUpsert(pMeta, pReq->uid, pReq->txnId, META_TXN_PRE_DROP, -1);
      if (idxCode != 0) {
        metaError("vgId:%d, %s failed to upsert txn.idx for uid:%" PRId64 " txnId:%" PRId64 " code:0x%x",
                  TD_VID(pMeta->pVnode), __func__, pReq->uid, pReq->txnId, idxCode);
        if (idxCode != TSDB_CODE_TXN_NOT_EXIST) {
          TAOS_RETURN(idxCode);
        }
      }
      metaInfo("vgId:%d, table %s uid %" PRId64 " marked PRE_DROP, txnId:%" PRId64, TD_VID(pMeta->pVnode), pReq->name,
               pReq->uid, pReq->txnId);
    }
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
  // Hold metaRLock for pNameIdx B+tree read: async vacuum may concurrently
  // take metaWLock and mutate pNameIdx. Released before metaGetInfo to avoid
  // nested rdlock deadlock.
  void   *value = NULL;
  int32_t valueSize = 0;
  metaRLock(pMeta);
  code = tdbTbGet(pMeta->pNameIdx, pReq->tbName, strlen(pReq->tbName) + 1, &value, &valueSize);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }
  int64_t uid = *(int64_t *)value;
  tdbFreeClear(value);

  // check table type — metaGetInfo acquires RLock internally; call without holding our lock
  SMetaInfo info;
  if (metaGetInfo(pMeta, uid, &info, NULL) != 0) {
    metaError("vgId:%d, %s failed at %s:%d since table %s uid %" PRId64
              " not found, this is an internal error in meta, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, uid, version);
    code = TSDB_CODE_INTERNAL_ERROR;
    TAOS_RETURN(code);
  }
  if (info.suid != 0 && pReq->action != TSDB_ALTER_TABLE_ALTER_COLUMN_REF &&
      pReq->action != TSDB_ALTER_TABLE_REMOVE_COLUMN_REF &&
      pReq->action != TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF &&
      pReq->action != TSDB_ALTER_TABLE_DROP_COLUMN) {
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

// Add an own (extension) column to a virtual child table. The column does not exist in the
// vstb schema; it is stored in the vchild's own schema area (SMetaEntry.own) with a mandatory
// origin reference, and its colId is allocated from TSDB_VCHILD_OWN_COL_ID_BASE upwards so it
// can never collide with vstb-assigned colIds.
static int32_t metaAddVChildOwnColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, SMetaEntry *pEntry,
                                      STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // an own data column of a vchild must carry an origin reference
  if (NULL == pReq->refDbName || NULL == pReq->refTbName || NULL == pReq->refColName ||
      pReq->refDbName[0] == '\0' || pReq->refTbName[0] == '\0' || pReq->refColName[0] == '\0') {
    metaError("vgId:%d, %s failed at %s:%d since virtual child table %s add column %s without reference, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pReq->colName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // fetch super entry (vstb): name-dup check against its schema and the merged response source
  // (metaRLock guards metaFetchEntryByUid's tdb reads against a concurrent WLock/vacuum commit)
  SMetaEntry *pSuper = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }

  // duplicate name check: vstb row schema + vstb tag schema + existing own columns/tags
  SSchemaWrapper *pStbSchema = &pSuper->stbEntry.schemaRow;
  int32_t         rowSize = 0;
  for (int32_t i = 0; i < pStbSchema->nCols; i++) {
    SSchema *pColumn = &pStbSchema->pSchema[i];
    if (strncmp(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since column %s already exists in super table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pSuper->name, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    rowSize += pColumn->bytes;
  }
  SSchemaWrapper *pStbTagSchema = &pSuper->stbEntry.schemaTag;
  for (int32_t i = 0; i < pStbTagSchema->nCols; i++) {
    if (strncmp(pStbTagSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since column %s already exists as tag in super table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pSuper->name, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
  }
  col_id_t maxOwnColId = TSDB_VCHILD_OWN_COL_ID_BASE - 1;
  for (int32_t i = 0; i < pEntry->own.nOwnCols; i++) {
    SSchema *pColumn = &pEntry->own.pOwnColSchemas[i];
    if (strncmp(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since column %s already exists in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    rowSize += pColumn->bytes;
    if (pColumn->colId > maxOwnColId) {
      maxOwnColId = pColumn->colId;
    }
  }
  for (int32_t i = 0; i < pEntry->own.nOwnTags; i++) {
    SSchema *pTag = &pEntry->own.pOwnTagSchemas[i];
    if (strncmp(pTag->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since column %s already exists as own tag in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    if (pTag->colId > maxOwnColId) {
      maxOwnColId = pTag->colId;
    }
  }

  // limits are checked against the merged view (vstb columns + existing own columns + new one)
  if (rowSize + pReq->bytes > TSDB_MAX_BYTES_PER_ROW_VIRTUAL) {
    metaError("vgId:%d, %s failed at %s:%d since row size %d + %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, rowSize, pReq->bytes, TSDB_MAX_BYTES_PER_ROW_VIRTUAL, version);
    code = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
    goto _exit;
  }
  if (pStbSchema->nCols + pEntry->own.nOwnCols + 1 > TSDB_MAX_COLUMNS) {
    metaError("vgId:%d, %s failed at %s:%d since column count %d + %d + 1 > %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pStbSchema->nCols, pEntry->own.nOwnCols,
              TSDB_MAX_COLUMNS, version);
    code = TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    goto _exit;
  }

  int32_t newColId = (int32_t)maxOwnColId + 1;
  if (newColId > INT16_MAX) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d exceeds max column id %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, newColId, INT16_MAX, version);
    code = TSDB_CODE_VND_EXCEED_MAX_COL_ID;
    goto _exit;
  }

  // append the own column schema
  int32_t nOwnCols = pEntry->own.nOwnCols;
  // Grow all arrays into freshly allocated buffers and commit them to pEntry->own only after
  // every allocation succeeded, so a failure leaves the entry untouched and never breaks the
  // 1:1 position-alignment invariant between pOwnColSchemas/pOwnColRefs/pOwnColExtSchemas and
  // nOwnCols. realloc cannot provide this: a succeeded realloc consumes the old block, so a
  // later failure could not restore the original pointer. The ext array is grown only when the
  // new column (or an existing one) carries a typeMod; otherwise it stays NULL.
  bool        needExt = (pReq->flags & COL_HAS_TYPE_MOD) || (pEntry->own.pOwnColExtSchemas != NULL);
  SSchema    *pNewSchemas = taosMemoryMalloc((nOwnCols + 1) * sizeof(SSchema));
  SColRef    *pNewRefs = NULL;
  SExtSchema *pNewExt = NULL;
  if (NULL != pNewSchemas) {
    if (nOwnCols > 0) {
      memcpy(pNewSchemas, pEntry->own.pOwnColSchemas, nOwnCols * sizeof(SSchema));
    }
    pNewRefs = taosMemoryMalloc((nOwnCols + 1) * sizeof(SColRef));
  }
  if (NULL != pNewRefs && nOwnCols > 0) {
    memcpy(pNewRefs, pEntry->own.pOwnColRefs, nOwnCols * sizeof(SColRef));
  }
  if (NULL != pNewSchemas && NULL != pNewRefs && needExt) {
    // calloc also covers the first decimal own column: prior non-decimal own columns get a
    // valid typeMod (0), matching the COL_HAS_TYPE_MOD==false semantics.
    pNewExt = taosMemoryCalloc(nOwnCols + 1, sizeof(SExtSchema));
    if (NULL != pNewExt && pEntry->own.pOwnColExtSchemas != NULL) {
      memcpy(pNewExt, pEntry->own.pOwnColExtSchemas, nOwnCols * sizeof(SExtSchema));
    }
  }
  if (NULL == pNewSchemas || NULL == pNewRefs || (needExt && NULL == pNewExt)) {
    code = terrno;
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    // the entry still owns its original arrays; just release the freshly allocated buffers
    taosMemoryFree(pNewSchemas);
    taosMemoryFree(pNewRefs);
    taosMemoryFree(pNewExt);
    goto _exit;
  }
  taosMemoryFree(pEntry->own.pOwnColSchemas);
  taosMemoryFree(pEntry->own.pOwnColRefs);
  taosMemoryFree(pEntry->own.pOwnColExtSchemas);
  pEntry->own.pOwnColSchemas = pNewSchemas;
  pEntry->own.pOwnColRefs = pNewRefs;
  pEntry->own.pOwnColExtSchemas = pNewExt;

  SSchema *pColumn = &pEntry->own.pOwnColSchemas[nOwnCols];
  pColumn->bytes = pReq->bytes;
  pColumn->type = pReq->type;
  pColumn->flags = pReq->flags;
  pColumn->colId = (col_id_t)newColId;
  tstrncpy(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN);
  if (pEntry->own.pOwnColExtSchemas != NULL) {
    pEntry->own.pOwnColExtSchemas[nOwnCols].typeMod = (pReq->flags & COL_HAS_TYPE_MOD) ? pReq->typeMod : 0;
  }

  SColRef *pOwnRef = &pEntry->own.pOwnColRefs[nOwnCols];
  memset(pOwnRef, 0, sizeof(SColRef));
  pOwnRef->hasRef = true;
  pOwnRef->id = (col_id_t)newColId;
  pOwnRef->refType = pReq->refType;
  if (pReq->refSourceName && pReq->refSourceName[0] != '\0') {
    tstrncpy(pOwnRef->refSourceName, pReq->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
  }
  tstrncpy(pOwnRef->refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pOwnRef->refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pOwnRef->refColName, pReq->refColName, TSDB_COL_NAME_LEN);
  pEntry->own.nOwnCols = nOwnCols + 1;

  pEntry->version = version;
  pEntry->colRef.version++;  // rversion bump: drives client catalog cache invalidation

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    goto _exit;
  }
  metaInfo("vgId:%d, table %s uid %" PRId64 " added own column %s colId %d, version:%" PRId64, TD_VID(pMeta->pVnode),
           pReq->tbName, pEntry->uid, pReq->colName, newColId, version);

  // build the merged response (vstb schema + own columns) so the client catalog cache refreshes
  code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pStbSchema, &pSuper->stbEntry.schemaTag, &pEntry->colRef, pSuper->pExtSchemas,
                              pSuper->stbEntry.ownerId, pRsp, pEntry->type);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  }

_exit:
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

// Drop an own (extension) column from a virtual child table. The column must exist in the vchild's
// own schema area (SMetaEntry.own); a vstb cascaded column name is rejected as not-found. The own
// schema, its 1:1 position-aligned ref array and the typeMod (ext) array are shrunk in lockstep.
// Own columns carry no compression metadata, so colCmpr is untouched.
static int32_t metaDropVChildOwnColumn(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, SMetaEntry *pEntry,
                                       STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // locate the own column by name
  int32_t idx = -1;
  for (int32_t i = 0; i < pEntry->own.nOwnCols; i++) {
    if (strncmp(pEntry->own.pOwnColSchemas[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      idx = i;
      break;
    }
  }
  if (idx < 0) {
    metaError("vgId:%d, %s failed at %s:%d since own column %s not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }
  col_id_t droppedColId = pEntry->own.pOwnColSchemas[idx].colId;

  // shrink pOwnColSchemas[], pOwnColRefs[] and pOwnColExtSchemas[] in lockstep (1:1
  // position-aligned); skipping the ext shift would pair the surviving decimal column with the
  // dropped column's typeMod slot and persist it as DECIMAL(0,0)
  int32_t nOwnCols = pEntry->own.nOwnCols;
  // release the dropped slot's tagCondJson before it is overwritten (own column refs are origin
  // refs and never set it, but stay defensive)
  taosMemoryFreeClear(pEntry->own.pOwnColRefs[idx].tagCondJson);
  if (nOwnCols - idx - 1 > 0) {
    memmove(pEntry->own.pOwnColSchemas + idx, pEntry->own.pOwnColSchemas + idx + 1,
            (nOwnCols - idx - 1) * sizeof(SSchema));
    memmove(pEntry->own.pOwnColRefs + idx, pEntry->own.pOwnColRefs + idx + 1,
            (nOwnCols - idx - 1) * sizeof(SColRef));
    if (pEntry->own.pOwnColExtSchemas != NULL) {
      memmove(pEntry->own.pOwnColExtSchemas + idx, pEntry->own.pOwnColExtSchemas + idx + 1,
              (nOwnCols - idx - 1) * sizeof(SExtSchema));
    }
  }
  pEntry->own.nOwnCols = nOwnCols - 1;
  // shrink the arrays to the new size, or release them entirely when empty, so they stay
  // consistent with the create path's "empty == NULL" convention. A failed shrink realloc keeps
  // the old (valid, merely oversized) buffer; nOwnCols defines the logical length either way.
  if (pEntry->own.nOwnCols == 0) {
    taosMemoryFreeClear(pEntry->own.pOwnColSchemas);
    taosMemoryFreeClear(pEntry->own.pOwnColRefs);
    taosMemoryFreeClear(pEntry->own.pOwnColExtSchemas);
  } else {
    SSchema *pSchemas = taosMemoryRealloc(pEntry->own.pOwnColSchemas, pEntry->own.nOwnCols * sizeof(SSchema));
    if (pSchemas != NULL) pEntry->own.pOwnColSchemas = pSchemas;
    SColRef *pRefs = taosMemoryRealloc(pEntry->own.pOwnColRefs, pEntry->own.nOwnCols * sizeof(SColRef));
    if (pRefs != NULL) pEntry->own.pOwnColRefs = pRefs;
    if (pEntry->own.pOwnColExtSchemas != NULL) {
      SExtSchema *pExt = taosMemoryRealloc(pEntry->own.pOwnColExtSchemas, pEntry->own.nOwnCols * sizeof(SExtSchema));
      if (pExt != NULL) pEntry->own.pOwnColExtSchemas = pExt;
    }
  }

  pEntry->version = version;
  pEntry->colRef.version++;  // rversion bump: drives client catalog cache invalidation

  // fetch super entry (vstb): the merged response source
  // (metaRLock guards metaFetchEntryByUid's tdb reads against a concurrent WLock/vacuum commit)
  SMetaEntry *pSuper = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
    goto _exit;
  }

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    goto _exit;
  }
  metaInfo("vgId:%d, table %s uid %" PRId64 " dropped own column %s colId %d, version:%" PRId64, TD_VID(pMeta->pVnode),
           pReq->tbName, pEntry->uid, pReq->colName, droppedColId, version);

  // build the merged response (vstb schema + remaining own columns) so the client catalog cache refreshes
  code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, &pSuper->stbEntry.schemaRow, &pSuper->stbEntry.schemaTag,
                              &pEntry->colRef, pSuper->pExtSchemas, pSuper->stbEntry.ownerId, pRsp, pEntry->type);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  }

_exit:
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

// Rebuild the shared tag blob of a virtual child table with the merged tag schema (vstb tags
// followed by own tags). Stored values are carried over by colId; schema tags without a stored
// value stay NULL (absent from the blob). tTagNew sorts by colId and own tag colIds are always
// greater than any vstb colId, so the blob order always matches the merged schema order.
static int32_t metaRebuildVChildTagBlob(SMetaEntry *pEntry, const SSchemaWrapper *pMergedTagSchema) {
  int32_t     code = TSDB_CODE_SUCCESS;
  const STag *pOldTag = (const STag *)pEntry->ctbEntry.pTags;
  SArray     *pTagArray = taosArrayInit(pMergedTagSchema->nCols, sizeof(STagVal));
  if (NULL == pTagArray) {
    return terrno;
  }

  for (int32_t i = 0; i < pMergedTagSchema->nCols; i++) {
    SSchema *pCol = &pMergedTagSchema->pSchema[i];
    STagVal  value = {.cid = pCol->colId};
    if (!tTagGet(pOldTag, &value)) {
      continue;  // NULL value: not present in the blob
    }
    if (taosArrayPush(pTagArray, &value) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  // Rebuild unconditionally. tTagNew yields a valid empty STag (nTag==0) for an empty array,
  // which is the correct outcome when no remaining tag has a stored value — e.g. after dropping
  // the only tag that had a literal. Short-circuiting here would leave the old blob (which may
  // still physically contain a just-dropped colId/value) as orphaned stale data.
  STag *pNewTag = NULL;
  code = tTagNew(pTagArray, pMergedTagSchema->version, false, &pNewTag);
  if (code) {
    goto _exit;
  }
  // tTagNew serialized every value out of the old blob, it is safe to release it now
  taosMemoryFree(pEntry->ctbEntry.pTags);
  pEntry->ctbEntry.pTags = (uint8_t *)pNewTag;

_exit:
  taosArrayDestroy(pTagArray);
  return code;
}

// Add an own (extension) tag to a virtual child table. The tag does not exist in the vstb
// schema; only its schema is stored in the vchild's own schema area (SMetaEntry.own). The
// initial value is NULL; a literal value (stored in the shared ctbEntry.pTags blob) or a tag
// reference (stored in the sparse colRef.pTagRef array by colId) can be attached later via
// SET TAG / ALTER TAG SET REF. The colId is allocated from TSDB_VCHILD_OWN_COL_ID_BASE upwards
// (shared with own columns) so it can never collide with vstb-assigned colIds.
int32_t metaAddVChildOwnTag(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->colName || strlen(pReq->colName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  // only virtual child tables may add an own tag
  if (pEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d does not support add tag, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }
  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // fetch super entry (vstb): name-dup check against its schema and the merged response source
  // (metaRLock guards metaFetchEntryByUid's tdb reads against a concurrent WLock/vacuum commit)
  SMetaEntry *pSuper = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }

  // tag type restrictions, same rules as super table ADD TAG
  if (pReq->type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since json type is not allowed for own tag %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, version);
    code = TSDB_CODE_PAR_ONLY_ONE_JSON_TAG;
    goto _exit;
  }
  if (IS_DECIMAL_TYPE(pReq->type)) {
    metaError("vgId:%d, %s failed at %s:%d since decimal type is not allowed for own tag %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, version);
    code = TSDB_CODE_PAR_INVALID_COLUMN;
    goto _exit;
  }
  if ((pReq->type == TSDB_DATA_TYPE_VARCHAR || pReq->type == TSDB_DATA_TYPE_VARBINARY ||
       pReq->type == TSDB_DATA_TYPE_NCHAR || pReq->type == TSDB_DATA_TYPE_GEOMETRY) &&
      pReq->bytes > TSDB_MAX_TAGS_LEN) {
    metaError("vgId:%d, %s failed at %s:%d since tag %s bytes %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, pReq->bytes, TSDB_MAX_TAGS_LEN, version);
    code = TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    goto _exit;
  }

  // duplicate name check: vstb row schema + vstb tag schema + existing own columns/tags
  SSchemaWrapper *pStbSchema = &pSuper->stbEntry.schemaRow;
  for (int32_t i = 0; i < pStbSchema->nCols; i++) {
    if (strncmp(pStbSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since tag %s already exists as column in super table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pSuper->name, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
  }
  SSchemaWrapper *pStbTagSchema = &pSuper->stbEntry.schemaTag;
  int32_t         tagsLen = 0;
  for (int32_t i = 0; i < pStbTagSchema->nCols; i++) {
    if (strncmp(pStbTagSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since tag %s already exists in super table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pSuper->name, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    tagsLen += pStbTagSchema->pSchema[i].bytes;
  }
  // the shared tag blob is rebuilt with the merged tag schema, which does not support a vstb
  // JSON tag; refuse to mix an own tag with it
  if (pStbTagSchema->nCols == 1 && pStbTagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    metaError("vgId:%d, %s failed at %s:%d since super table %s has json tag, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pSuper->name, version);
    code = TSDB_CODE_PAR_ONLY_ONE_JSON_TAG;
    goto _exit;
  }
  col_id_t maxOwnColId = TSDB_VCHILD_OWN_COL_ID_BASE - 1;
  for (int32_t i = 0; i < pEntry->own.nOwnCols; i++) {
    SSchema *pColumn = &pEntry->own.pOwnColSchemas[i];
    if (strncmp(pColumn->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since tag %s already exists as own column in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    if (pColumn->colId > maxOwnColId) {
      maxOwnColId = pColumn->colId;
    }
  }
  for (int32_t i = 0; i < pEntry->own.nOwnTags; i++) {
    SSchema *pTag = &pEntry->own.pOwnTagSchemas[i];
    if (strncmp(pTag->name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since tag %s already exists in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      code = TSDB_CODE_VND_COL_ALREADY_EXISTS;
      goto _exit;
    }
    tagsLen += pTag->bytes;
    if (pTag->colId > maxOwnColId) {
      maxOwnColId = pTag->colId;
    }
  }

  // limits are checked against the merged view (vstb tags + existing own tags + the new one)
  if (tagsLen + pReq->bytes > TSDB_MAX_TAGS_LEN) {
    metaError("vgId:%d, %s failed at %s:%d since tags length %d + %d > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tagsLen, pReq->bytes, TSDB_MAX_TAGS_LEN, version);
    code = TSDB_CODE_PAR_INVALID_TAGS_LENGTH;
    goto _exit;
  }
  if (pStbTagSchema->nCols + pEntry->own.nOwnTags + 1 > TSDB_MAX_TAGS) {
    metaError("vgId:%d, %s failed at %s:%d since tag count %d + %d + 1 > %d, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pStbTagSchema->nCols, pEntry->own.nOwnTags, TSDB_MAX_TAGS, version);
    code = TSDB_CODE_PAR_INVALID_TAGS_NUM;
    goto _exit;
  }

  int32_t newColId = (int32_t)maxOwnColId + 1;
  if (newColId > INT16_MAX) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d exceeds max column id %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, newColId, INT16_MAX, version);
    code = TSDB_CODE_VND_EXCEED_MAX_COL_ID;
    goto _exit;
  }

  // append the own tag schema
  int32_t  nOwnTags = pEntry->own.nOwnTags;
  SSchema *pNewSchemas = taosMemoryRealloc(pEntry->own.pOwnTagSchemas, (nOwnTags + 1) * sizeof(SSchema));
  if (NULL == pNewSchemas) {
    code = terrno;
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    goto _exit;
  }
  pEntry->own.pOwnTagSchemas = pNewSchemas;

  SSchema *pTag = &pEntry->own.pOwnTagSchemas[nOwnTags];
  pTag->bytes = pReq->bytes;
  pTag->type = pReq->type;
  pTag->flags = 0;
  pTag->colId = (col_id_t)newColId;
  tstrncpy(pTag->name, pReq->colName, TSDB_COL_NAME_LEN);
  pEntry->own.nOwnTags = nOwnTags + 1;

  // rebuild the shared tag blob with the merged tag schema (vstb tags + own tags); the new tag
  // has no stored value yet (NULL), so this only normalizes the blob to the merged schema
  int32_t  nMergedTags = pStbTagSchema->nCols + pEntry->own.nOwnTags;
  SSchema *pMergedTags = taosMemoryMalloc(nMergedTags * sizeof(SSchema));
  if (NULL == pMergedTags) {
    code = terrno;
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    goto _exit;
  }
  memcpy(pMergedTags, pStbTagSchema->pSchema, pStbTagSchema->nCols * sizeof(SSchema));
  memcpy(pMergedTags + pStbTagSchema->nCols, pEntry->own.pOwnTagSchemas, pEntry->own.nOwnTags * sizeof(SSchema));
  SSchemaWrapper mergedTagSchema = {
      .nCols = nMergedTags, .version = pStbTagSchema->version, .pSchema = pMergedTags};
  code = metaRebuildVChildTagBlob(pEntry, &mergedTagSchema);
  taosMemoryFree(pMergedTags);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    goto _exit;
  }

  pEntry->version = version;
  pEntry->colRef.version++;  // rversion bump: drives client catalog cache invalidation

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    goto _exit;
  }
  metaInfo("vgId:%d, table %s uid %" PRId64 " added own tag %s colId %d, version:%" PRId64, TD_VID(pMeta->pVnode),
           pReq->tbName, pEntry->uid, pReq->colName, newColId, version);

  // build the merged response (vstb schema + own columns/tags) so the client catalog cache refreshes
  code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pStbSchema, pStbTagSchema, &pEntry->colRef, pSuper->pExtSchemas,
                              pSuper->stbEntry.ownerId, pRsp, pEntry->type);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  }

_exit:
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

// Drop an own (extension) tag from a virtual child table. The tag schema is removed from the own
// area, its reference (if any) is compacted out of the sparse colRef.pTagRef array, and the shared
// tag blob is rebuilt with the trimmed merged schema — metaRebuildVChildTagBlob drops the stored
// literal value because its colId is no longer in the merged schema. Only virtual child tables
// reach here (enforced by the parser / dispatcher).
int32_t metaDropVChildOwnTag(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->colName || strlen(pReq->colName) == 0) {
    metaError("vgId:%d, %s failed at %s:%d since invalid tag name:%s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d does not support drop tag, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }
  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // locate the own tag by name
  int32_t idx = -1;
  for (int32_t i = 0; i < pEntry->own.nOwnTags; i++) {
    if (strncmp(pEntry->own.pOwnTagSchemas[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      idx = i;
      break;
    }
  }
  if (idx < 0) {
    metaError("vgId:%d, %s failed at %s:%d since own tag %s not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }
  col_id_t droppedColId = pEntry->own.pOwnTagSchemas[idx].colId;

  // fetch super entry (vstb): name resolution and the merged response source
  // (metaRLock guards metaFetchEntryByUid's tdb reads against a concurrent WLock/vacuum commit)
  SMetaEntry *pSuper = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
  }
  SSchemaWrapper *pStbTagSchema = &pSuper->stbEntry.schemaTag;

  // shrink pOwnTagSchemas[]
  int32_t nOwnTags = pEntry->own.nOwnTags;
  if (nOwnTags - idx - 1 > 0) {
    memmove(pEntry->own.pOwnTagSchemas + idx, pEntry->own.pOwnTagSchemas + idx + 1,
            (nOwnTags - idx - 1) * sizeof(SSchema));
  }
  pEntry->own.nOwnTags = nOwnTags - 1;
  // shrink the array to the new size, or release it entirely when empty, so it stays
  // consistent with the create path's "empty == NULL" convention. A failed shrink realloc
  // keeps the old (valid, merely oversized) buffer; nOwnTags defines the logical length.
  if (pEntry->own.nOwnTags == 0) {
    taosMemoryFreeClear(pEntry->own.pOwnTagSchemas);
  } else {
    SSchema *pSchemas = taosMemoryRealloc(pEntry->own.pOwnTagSchemas, pEntry->own.nOwnTags * sizeof(SSchema));
    if (pSchemas != NULL) pEntry->own.pOwnTagSchemas = pSchemas;
  }

  // compact out the dropped tag's reference from the sparse colRef.pTagRef[] (by colId)
  for (int32_t r = 0; r < pEntry->colRef.nTagRefs; r++) {
    if (pEntry->colRef.pTagRef[r].id == droppedColId) {
      taosMemoryFreeClear(pEntry->colRef.pTagRef[r].tagCondJson);
      int32_t left = (pEntry->colRef.nTagRefs - r - 1) * sizeof(SColRef);
      if (left > 0) {
        memmove(pEntry->colRef.pTagRef + r, pEntry->colRef.pTagRef + r + 1, left);
      }
      pEntry->colRef.nTagRefs--;
      pEntry->colRef.version++;
      break;
    }
  }

  // rebuild the shared tag blob with the trimmed merged schema (vstb tags + remaining own tags);
  // the dropped tag's literal value is dropped because its colId is absent from the merged schema
  int32_t  nMergedTags = pStbTagSchema->nCols + pEntry->own.nOwnTags;
  SSchema *pMergedTags = taosMemoryMalloc(nMergedTags * sizeof(SSchema));
  if (NULL == pMergedTags) {
    code = terrno;
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    goto _exit;
  }
  memcpy(pMergedTags, pStbTagSchema->pSchema, pStbTagSchema->nCols * sizeof(SSchema));
  // after dropping the last own tag pOwnTagSchemas is NULL, so guard the copy
  if (pEntry->own.nOwnTags > 0) {
    memcpy(pMergedTags + pStbTagSchema->nCols, pEntry->own.pOwnTagSchemas, pEntry->own.nOwnTags * sizeof(SSchema));
  }
  SSchemaWrapper mergedTagSchema = {
      .nCols = nMergedTags, .version = pStbTagSchema->version, .pSchema = pMergedTags};
  code = metaRebuildVChildTagBlob(pEntry, &mergedTagSchema);
  taosMemoryFree(pMergedTags);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__, __FILE__,
              __LINE__, tstrerror(code), version);
    goto _exit;
  }

  pEntry->version = version;
  pEntry->colRef.version++;  // rversion bump: drives client catalog cache invalidation

  // do handle entry
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
    goto _exit;
  }
  metaInfo("vgId:%d, table %s uid %" PRId64 " dropped own tag %s colId %d, version:%" PRId64, TD_VID(pMeta->pVnode),
           pReq->tbName, pEntry->uid, pReq->colName, droppedColId, version);

  // build the merged response (vstb schema + remaining own columns/tags) so the client catalog cache refreshes
  code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, &pSuper->stbEntry.schemaRow, pStbTagSchema, &pEntry->colRef,
                              pSuper->pExtSchemas, pSuper->stbEntry.ownerId, pRsp, pEntry->type);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  }

_exit:
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pEntry);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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

  // virtual child table: only ADD COLUMN with an origin reference is allowed; the column goes
  // into the vchild's own (extension) schema area, not the vstb schema. Plain child tables
  // (reaching here via TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF) are still rejected.
  if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    if (pReq->action != TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF) {
      metaError("vgId:%d, %s failed at %s:%d since virtual child table %s add column %s must carry a reference, "
                "version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pReq->colName, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
    }
    TAOS_RETURN(metaAddVChildOwnColumn(pMeta, version, pReq, pEntry, pRsp));
  }
  if (pEntry->type != TSDB_NORMAL_TABLE && pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s type %d does not support add column, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->type, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
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
      tmpRef.tagCondLen = 0;
      tmpRef.tagCondJson = NULL;
    } else {
      tmpRef.hasRef = true;
      tmpRef.id = pColumn->colId;
      tmpRef.refType = pReq->refType;
      if (pReq->refSourceName && pReq->refSourceName[0] != '\0') {
        tstrncpy(tmpRef.refSourceName, pReq->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
      } else {
        tmpRef.refSourceName[0] = '\0';
      }
      tmpRef.refSchemaName[0] = '\0';
      tstrncpy(tmpRef.refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(tmpRef.refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(tmpRef.refColName, pReq->refColName, TSDB_COL_NAME_LEN);
      tmpRef.tagCondLen = 0;
      tmpRef.tagCondJson = NULL;
      if (pReq->refType == 1 && pEntry->series.nSeries > 0) {
        for (int32_t i = 0; i < pEntry->series.nSeries; i++) {
          SSeriesEntry *s = &pEntry->series.pSeries[i];
          bool matched = false;
          if (pReq->seriesAlias && pReq->seriesAlias[0] != '\0') {
            matched = (strcasecmp(s->alias, pReq->seriesAlias) == 0);
          } else {
            matched = (strcasecmp(s->sourceName, pReq->refSourceName ? pReq->refSourceName : "") == 0 &&
                       strcasecmp(s->dbName, pReq->refDbName) == 0 &&
                       strcasecmp(s->measurementName, pReq->refTbName) == 0);
          }
          if (matched) {
            if (s->tagCondLen > 0 && s->tagCondJson) {
              tmpRef.tagCondJson = taosStrdup(s->tagCondJson);
              tmpRef.tagCondLen = s->tagCondLen;
            }
            break;
          }
        }
      }
    }
    code = updataTableColRef(&pEntry->colRef, pColumn, 1, &tmpRef);
    taosMemoryFreeClear(tmpRef.tagCondJson);
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
      code = validColCmprByType(pColumn->type, pReq->compress);
      if (code != TSDB_CODE_SUCCESS) {
        metaError("vgId:%d, %s failed at %s:%d since %s, col:%s version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code), pReq->colName, version);
        metaFetchEntryFree(&pEntry);
        TAOS_RETURN(code);
      }
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, NULL, &pEntry->colRef, pEntry->pExtSchemas,
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
          pRsp->pColRefs[i].refType = p->refType;
          tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->pExtSchemas, pEntry->ntbEntry.ownerId, pRsp);
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

// Keep colRef.pTagRef sized to schemaTag (one slot per tag), so each tag — owned or ref — has a
// matching entry (owned entries stay hasRef=false). New slots are zeroed except id = tag colId.
static int32_t metaEnsureTagRefSize(SColRefWrapper *pColRef, SSchemaWrapper *pTagSchema) {
  int32_t nTags = pTagSchema->nCols;
  if (pColRef->nTagRefs >= nTags) return TSDB_CODE_SUCCESS;
  SColRef *pNew = (SColRef *)taosMemoryRealloc(pColRef->pTagRef, sizeof(SColRef) * nTags);
  if (pNew == NULL) return terrno;
  pColRef->pTagRef = pNew;
  for (int32_t i = pColRef->nTagRefs; i < nTags; i++) {
    SColRef *p = &pColRef->pTagRef[i];
    memset(p, 0, sizeof(SColRef));  // zero all fields incl. ext (refType/refSourceName/refSchemaName/tagCondLen/tagCondJson) — tEncodeSColRefExt reads them when hasRef
    p->id = pTagSchema->pSchema[i].colId;
  }
  pColRef->nTagRefs = nTags;
  return TSDB_CODE_SUCCESS;
}

int32_t metaAddTableTag(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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

  // only normal and virtual normal tables own tags (super tables use mnd; child tables inherit
  // tags from the super table)
  if (pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE && pEntry->type != TSDB_NORMAL_TABLE) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  // tag-refs are virtual-normal-table only; a ref request on a plain normal table must be
  // rejected here, not silently degraded to an owned tag (parser already gates this — this is
  // the vnode-side backstop, mirroring metaAlterTagRef's type gate)
  if (pEntry->type == TSDB_NORMAL_TABLE && pReq->refDbName != NULL) {
    metaError("vgId:%d, %s failed at %s:%d since tag-ref on normal table %s is not supported, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  pEntry->version = version;
  SSchemaWrapper *pTagSchema = &pEntry->ntbEntry.schemaTag;

  // duplicate tag name + total tag bytes
  int32_t tagsLen = 0;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    if (strncmp(pTagSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      metaError("vgId:%d, %s failed at %s:%d since tag %s already exists in table %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_COL_ALREADY_EXISTS);
    }
    tagsLen += pTagSchema->pSchema[i].bytes;
  }
  if (pTagSchema->nCols + 1 > TSDB_MAX_TAGS) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_TAGS_NUM);
  }
  if (tagsLen + pReq->bytes > TSDB_MAX_TAGS_LEN) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_TAGS_LENGTH);
  }
  // validate the column-id budget up front, before mutating the schema
  if (pEntry->ntbEntry.ncid > INT16_MAX) {
    metaError("vgId:%d, %s failed at %s:%d since column id %d exceeds max column id %d, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ntbEntry.ncid, INT16_MAX, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_EXCEED_MAX_COL_ID);
  }

  // append the new tag to schemaTag
  SSchema *pNewSchema = taosMemoryRealloc(pTagSchema->pSchema, sizeof(SSchema) * (pTagSchema->nCols + 1));
  if (pNewSchema == NULL) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(terrno);
  }
  pTagSchema->pSchema = pNewSchema;
  pTagSchema->version++;
  pTagSchema->nCols++;
  SSchema *pNewTag = &pTagSchema->pSchema[pTagSchema->nCols - 1];
  pNewTag->bytes = pReq->bytes;
  pNewTag->type = pReq->type;
  pNewTag->flags = pReq->flags;
  pNewTag->colId = pEntry->ntbEntry.ncid++;
  tstrncpy(pNewTag->name, pReq->colName, TSDB_COL_NAME_LEN);

  // For virtual normal tables, keep colRef.pTagRef in sync with schemaTag and — for a tag-ref
  // (pReq->refDbName set) — record the source reference on the new tag's slot. Normal tables have
  // no colRef and never carry tag-refs (the parser gates tag-refs to virtual normal tables).
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    code = metaEnsureTagRefSize(&pEntry->colRef, pTagSchema);
    if (code) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(code);
    }
    // Always (re)stamp the slot: after a drop the slot at nCols-1 may be a stale leftover of the
    // dropped tag (wrong id, or hasRef still set), and lookups match by colId.
    SColRef *pTagRefEntry = &pEntry->colRef.pTagRef[pTagSchema->nCols - 1];
    memset(pTagRefEntry, 0, sizeof(SColRef));  // reset a possibly stale slot (after a drop) incl. ext fields (tagCondJson ptr etc.)
    pTagRefEntry->id = pNewTag->colId;
    tstrncpy(pTagRefEntry->colName, pReq->colName, TSDB_COL_NAME_LEN);  // tmq json meta reads colName
    if (pReq->refDbName != NULL) {
      pTagRefEntry->hasRef = true;
      tstrncpy(pTagRefEntry->refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(pTagRefEntry->refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pTagRefEntry->refColName, pReq->refColName, TSDB_COL_NAME_LEN);
    } else {
      pTagRefEntry->hasRef = false;
      pTagRefEntry->refDbName[0] = '\0';
      pTagRefEntry->refTableName[0] = '\0';
      pTagRefEntry->refColName[0] = '\0';
    }
    pEntry->colRef.version++;
  }

  // rebuild pTags: STag is a sparse KV — copy existing non-null values (read via tTagGet),
  // the new tag stays absent (NULL). Rebuild with the bumped schemaTag.version for consistency.
  const STag *pOldTag = (const STag *)pEntry->ntbEntry.pTags;
  SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
  if (pTagArray == NULL) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  for (int32_t i = 0; i < pTagSchema->nCols - 1; i++) {
    SSchema *pCol = &pTagSchema->pSchema[i];
    STagVal  value = {.cid = pCol->colId};
    if (pOldTag != NULL && tTagGet(pOldTag, &value)) {
      if (taosArrayPush(pTagArray, &value) == NULL) {
        taosArrayDestroy(pTagArray);
        metaFetchEntryFree(&pEntry);
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
  }

  STag *pNewTagVal = NULL;
  code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTagVal);
  taosArrayDestroy(pTagArray);
  if (code) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  }
  taosMemoryFree(pEntry->ntbEntry.pTags);
  pEntry->ntbEntry.pTags = (uint8_t *)pNewTagVal;

  // persist
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " tag added, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // response: full meta (columns + colRef + owned-tag schema) so the client catalog does not
  // cache a tag-less/stale meta after ALTER ADD TAG. Only build it after a successful persist —
  // otherwise pRsp would describe changes that were not actually persisted.
  if (code == TSDB_CODE_SUCCESS) {
    int32_t rspCode = metaFillNtbTableMetaRsp(pEntry, pReq->tbName, pRsp);
    if (rspCode) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(rspCode), pEntry->uid, pReq->tbName, version);
      code = rspCode;
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaDropTableTag(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  // fetch old entry
  SMetaEntry *pEntry = NULL;
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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

  // only normal and virtual normal tables own tags; child tables inherit tags from the super table
  if (pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE && pEntry->type != TSDB_NORMAL_TABLE) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  pEntry->version = version;
  SSchemaWrapper *pTagSchema = &pEntry->ntbEntry.schemaTag;

  // find the tag by name (resolve by name — robust whether colId was supplied by the catalog)
  int32_t dropIdx = -1;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    if (strncmp(pTagSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      dropIdx = i;
      break;
    }
  }
  if (dropIdx < 0) {
    metaError("vgId:%d, %s failed at %s:%d since tag %s not found in table %s, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_PAR_INVALID_TAG_NAME);
  }

  // remove the tag schema entry (memmove + shrink)
  col_id_t dropColId = pTagSchema->pSchema[dropIdx].colId;
  if (dropIdx < pTagSchema->nCols - 1) {
    memmove(&pTagSchema->pSchema[dropIdx], &pTagSchema->pSchema[dropIdx + 1],
            sizeof(SSchema) * (pTagSchema->nCols - 1 - dropIdx));
  }
  pTagSchema->version++;
  pTagSchema->nCols--;
  if (pTagSchema->nCols > 0) {
    SSchema *pShrunk = taosMemoryRealloc(pTagSchema->pSchema, sizeof(SSchema) * pTagSchema->nCols);
    if (pShrunk != NULL) pTagSchema->pSchema = pShrunk;  // on failure keep the (over-sized) buffer
  } else {
    // last tag dropped: the schema goes empty but version stays monotonic — a reset to 0 would
    // make the alter response look stale to the client catalog cache (ctgWriteTbMetaToCache
    // ignores updates whose sversion/tversion/rversion do not advance) and would be lost on
    // restart anyway, breaking every later tag alter on the same connection.
    taosMemoryFreeClear(pTagSchema->pSchema);
  }

  // For virtual normal tables, keep colRef.pTagRef in sync with schemaTag: remove the dropped
  // tag's slot (matched by colId) so the positional mapping and id-based lookups stay consistent.
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE && pEntry->colRef.pTagRef != NULL) {
    SColRefWrapper *pColRef = &pEntry->colRef;
    for (int32_t i = 0; i < pColRef->nTagRefs; i++) {
      if (pColRef->pTagRef[i].id == dropColId) {
        taosMemoryFreeClear(pColRef->pTagRef[i].tagCondJson);  // free owned tag-cond JSON before the slot is moved/forgotten
        if (i < pColRef->nTagRefs - 1) {
          memmove(&pColRef->pTagRef[i], &pColRef->pTagRef[i + 1], sizeof(SColRef) * (pColRef->nTagRefs - 1 - i));
        }
        pColRef->nTagRefs--;
        if (pColRef->nTagRefs == 0) {
          taosMemoryFreeClear(pColRef->pTagRef);
        }
        pColRef->version++;
        break;
      }
    }
  }

  // rebuild pTags from the remaining tags (the dropped one is naturally absent)
  const STag *pOldTag = (const STag *)pEntry->ntbEntry.pTags;
  if (pTagSchema->nCols > 0) {
    SArray *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
    if (pTagArray == NULL) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      SSchema *pCol = &pTagSchema->pSchema[i];
      STagVal  value = {.cid = pCol->colId};
      if (pOldTag != NULL && tTagGet(pOldTag, &value)) {
        if (taosArrayPush(pTagArray, &value) == NULL) {
          taosArrayDestroy(pTagArray);
          metaFetchEntryFree(&pEntry);
          TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
        }
      }
    }
    STag *pNewTagVal = NULL;
    code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTagVal);
    taosArrayDestroy(pTagArray);
    if (code) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(code);
    }
    taosMemoryFree(pEntry->ntbEntry.pTags);
    pEntry->ntbEntry.pTags = (uint8_t *)pNewTagVal;
  } else {
    // last tag dropped: keep a valid (empty) STag instead of NULL — the entry's tag trailer is
    // still written (schemaTag.version stays monotonic) and must stay encodable.
    SArray *pTagArray = taosArrayInit(1, sizeof(STagVal));
    if (pTagArray == NULL) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    STag *pNewTagVal = NULL;
    code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTagVal);
    taosArrayDestroy(pTagArray);
    if (code) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(code);
    }
    taosMemoryFree(pEntry->ntbEntry.pTags);
    pEntry->ntbEntry.pTags = (uint8_t *)pNewTagVal;
  }

  // persist
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " tag dropped, version:%" PRId64, TD_VID(pMeta->pVnode), pReq->tbName,
             pEntry->uid, version);
  }

  // response: full meta (columns + colRef + owned-tag schema) so the client catalog does not
  // cache a stale meta after ALTER DROP TAG. Only build it after a successful persist —
  // otherwise pRsp would describe changes that were not actually persisted.
  if (code == TSDB_CODE_SUCCESS) {
    int32_t rspCode = metaFillNtbTableMetaRsp(pEntry, pReq->tbName, pRsp);
    if (rspCode) {
      metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
                __func__, __FILE__, __LINE__, tstrerror(rspCode), pEntry->uid, pReq->tbName, version);
      code = rspCode;
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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

  // virtual child table: only an own (extension) column may be dropped; cascaded columns belong
  // to the vstb and are rejected as not-found in the own schema.
  if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    TAOS_RETURN(metaDropVChildOwnColumn(pMeta, version, pReq, pEntry, pRsp));
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, NULL, &pEntry->colRef, pEntry->pExtSchemas,
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
          pRsp->pColRefs[i].refType = p->refType;
          tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->pExtSchemas, pEntry->ntbEntry.ownerId, pRsp);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, NULL, &pEntry->colRef, pEntry->pExtSchemas,
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
          pRsp->pColRefs[i].refType = p->refType;
          tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->pExtSchemas, pEntry->ntbEntry.ownerId, pRsp);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, NULL, &pEntry->colRef, pEntry->pExtSchemas,
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
          pRsp->pColRefs[i].refType = p->refType;
          tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
          tstrncpy(pRsp->pColRefs[i].refDbName, p->refDbName, TSDB_DB_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refTableName, p->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(pRsp->pColRefs[i].refColName, p->refColName, TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    code = metaUpdateMetaRsp(pEntry->uid, pReq->tbName, pSchema, pEntry->pExtSchemas, pEntry->ntbEntry.ownerId, pRsp);
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
      return TSDB_CODE_INVALID_MSG;
    }

    for (int32_t i = 0; i < pTagSchema->nCols; i++) { // update colId accrodding colName
      if (strcmp(pTagSchema->pSchema[i].name, pTagVal->tagName) == 0) {
        pTagVal->colId = pTagSchema->pSchema[i].colId;
      }
    }
    
    int32_t code = taosHashPut(*hashMap, &pTagVal->colId, sizeof(pTagVal->colId), pTagVal, sizeof(*pTagVal));
    if (code) {
      metaError("%s failed at %s:%d since %s", __func__, __FILE__, __LINE__, tstrerror(code));
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



// Set tag values on a normal/virtual-normal table (owned tags in ntbEntry.pTags).
// Mirrors metaUpdateTableNormalTagValue but operates on ntbEntry.pTags; no regex/json (v1).
// Setting a static value on a tag-ref converts it back to an owned tag: the reference is cleared
// (*pTagRefCleared set) so the caller can refresh the client catalog.
static int32_t metaUpdateNtbTagValueImpl(SMeta *pMeta, SMetaEntry *pTable, SSchemaWrapper *pTagSchema,
                                         SHashObj *pUpdatedTagVals, bool *pTagRefCleared) {
  int32_t     code = TSDB_CODE_SUCCESS;
  const STag *pOldTag = (const STag *)pTable->ntbEntry.pTags;
  SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
  if (pTagArray == NULL) {
    metaError("vgId:%d, %s failed at %s:%d since OOM, uid:%" PRId64 " version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pTable->uid, pTable->version);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    SSchema        *pCol = &pTagSchema->pSchema[i];
    int32_t         colId = pCol->colId;
    SUpdatedTagVal *pNewVal = taosHashGet(pUpdatedTagVals, &colId, sizeof(colId));

    if (pNewVal != NULL) {
      // Setting a static value on a tag-ref converts it back to an owned tag: clear the reference
      // (the value no longer follows the source table).
      if (pTable->type == TSDB_VIRTUAL_NORMAL_TABLE && pTable->colRef.pTagRef != NULL) {
        for (int32_t r = 0; r < pTable->colRef.nTagRefs; r++) {
          if (pTable->colRef.pTagRef[r].hasRef && pTable->colRef.pTagRef[r].id == colId) {
            pTable->colRef.pTagRef[r].hasRef = false;
            // Free the owned tag-cond JSON before clearing the ref, same as metaDropTableTag,
            // otherwise a ref carrying a tag condition leaks it on every such conversion.
            taosMemoryFreeClear(pTable->colRef.pTagRef[r].tagCondJson);
            pTable->colRef.pTagRef[r].tagCondLen = 0;
            memset(pTable->colRef.pTagRef[r].refDbName, 0, TSDB_DB_NAME_LEN);
            memset(pTable->colRef.pTagRef[r].refTableName, 0, TSDB_TABLE_NAME_LEN);
            memset(pTable->colRef.pTagRef[r].refColName, 0, TSDB_COL_NAME_LEN);
            pTable->colRef.version++;
            if (pTagRefCleared != NULL) *pTagRefCleared = true;
            metaInfo("vgId:%d, table %s uid %" PRId64 " tag %s (colId:%d) ref cleared by SET TAG, version:%" PRId64,
                     TD_VID(pMeta->pVnode), pTable->name, pTable->uid, pCol->name, colId, pTable->version);
            break;
          }
        }
      }
      if (pNewVal->isNull) {
        continue;  // explicitly set NULL -> absent in the sparse STag
      }
      STagVal value = {.cid = pCol->colId, .type = pCol->type};
      if (IS_VAR_DATA_TYPE(pCol->type)) {
        if ((int32_t)pNewVal->nTagVal > (pCol->bytes - VARSTR_HEADER_SIZE)) {
          taosArrayDestroy(pTagArray);
          TAOS_RETURN(TSDB_CODE_PAR_VALUE_TOO_LONG);
        }
        value.pData = pNewVal->pTagVal;
        value.nData = pNewVal->nTagVal;
      } else {
        if (pNewVal->nTagVal != tDataTypes[pCol->type].bytes) {
          metaError("vgId:%d, %s failed at %s:%d since invalid tag val len %d for tag %s, version:%" PRId64,
                    TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pNewVal->nTagVal, pCol->name, pTable->version);
          taosArrayDestroy(pTagArray);
          TAOS_RETURN(TSDB_CODE_INVALID_MSG);
        }
        memcpy(&value.i64, pNewVal->pTagVal, pNewVal->nTagVal);
      }
      if (taosArrayPush(pTagArray, &value) == NULL) {
        taosArrayDestroy(pTagArray);
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }
    } else {
      // no update for this tag: keep its existing value (if present)
      STagVal value = {.cid = pCol->colId, .type = pCol->type};
      if (pOldTag != NULL && tTagGet(pOldTag, &value)) {
        if (taosArrayPush(pTagArray, &value) == NULL) {
          taosArrayDestroy(pTagArray);
          TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
        }
      }
      // else: existing tag was NULL (absent) -> stays absent
    }
  }

  STag *pNewTag = NULL;
  code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTag);
  taosArrayDestroy(pTagArray);
  if (code) {
    TAOS_RETURN(code);
  }
  taosMemoryFree(pTable->ntbEntry.pTags);
  pTable->ntbEntry.pTags = (uint8_t *)pNewTag;

  code = metaHandleEntry2(pMeta, pTable);
  TAOS_RETURN(code);
}

static int32_t metaUpdateTableTagValue(SMeta *pMeta, int64_t version, const char* tbName, SArray* tags,
                                       STableMetaRsp *pMetaRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMetaEntry *pChild = NULL;
  SMetaEntry *pSuper = NULL;
  SHashObj* pUpdatedTagVals = NULL;
  bool tagRefCleared = false;

  // fetch child entry
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, tbName, &pChild);
  metaULock(pMeta);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tbName, version);
    goto _exit;
  }

  if (pChild->type != TSDB_CHILD_TABLE && pChild->type != TSDB_VIRTUAL_CHILD_TABLE &&
      pChild->type != TSDB_VIRTUAL_NORMAL_TABLE && pChild->type != TSDB_NORMAL_TABLE) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since table %s is not a child/virtual/normal table, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tbName, version);
    code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _exit;
  }

  // normal and virtual normal tables own their tags in ntbEntry (no super table to fetch)
  if (pChild->type == TSDB_VIRTUAL_NORMAL_TABLE || pChild->type == TSDB_NORMAL_TABLE) {
    SSchemaWrapper *pTagSchema = &pChild->ntbEntry.schemaTag;
    code = updatedTagValueArrayToHashMap(pTagSchema, tags, &pUpdatedTagVals);
    if (code) {
      const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
      metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
      goto _exit;
    }
    pChild->version = version;
    bool tagRefCleared = false;
    code = metaUpdateNtbTagValueImpl(pMeta, pChild, pTagSchema, pUpdatedTagVals, &tagRefCleared);
    if (code) {
      const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, name:%s version:%" PRId64;
      metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), tbName, version);
      goto _exit;
    }
    // SET TAG on a tag-ref cleared the reference: return the updated meta (colRef without the ref)
    // so the client catalog stops resolving the tag from the source table.
    if (tagRefCleared && pMetaRsp) {
      code = metaFillNtbTableMetaRsp(pChild, tbName, pMetaRsp);
      if (code) {
        metaError("vgId:%d, %s failed to build meta response for %s, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
                  tbName, version);
      }
    }
    goto _exit;
  }

  // fetch super entry
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pChild->ctbEntry.suid, &pSuper);
  metaULock(pMeta);
  if (code) {
    const char* msgFmt = "vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64;
    metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pChild->ctbEntry.suid, version);
    code = TSDB_CODE_INTERNAL_ERROR;
    goto _exit;
  }

  // For virtual child tables, clear tag-ref when setting static value
  if (pChild->type == TSDB_VIRTUAL_CHILD_TABLE &&
      pChild->colRef.nTagRefs > 0 && pChild->colRef.pTagRef) {
    int32_t nTags = taosArrayGetSize(tags);
    for (int32_t t = 0; t < nTags; t++) {
      SUpdatedTagVal *pTag = taosArrayGet(tags, t);
      if (pTag == NULL) continue;
      for (int32_t r = 0; r < pChild->colRef.nTagRefs; r++) {
        if (pChild->colRef.pTagRef[r].hasRef &&
            pChild->colRef.pTagRef[r].id == pTag->colId) {
          pChild->colRef.pTagRef[r].hasRef = false;
          memset(pChild->colRef.pTagRef[r].refDbName, 0, TSDB_DB_NAME_LEN);
          memset(pChild->colRef.pTagRef[r].refTableName, 0, TSDB_TABLE_NAME_LEN);
          memset(pChild->colRef.pTagRef[r].refColName, 0, TSDB_COL_NAME_LEN);
          pChild->colRef.version++;
          tagRefCleared = true;
        }
      }
    }
  }

  // search the tags to update; for a virtual child table use the merged tag view (vstb tags
  // followed by own tags) so own tags can be located by name and the shared tag blob is
  // rebuilt with the merged schema (keeping own tag values, whose colIds sort after all vstb
  // colIds, intact)
  SSchema        *pMergedTags = NULL;
  SSchemaWrapper  mergedTagSchema = {0};
  SSchemaWrapper *pTagSchema = &pSuper->stbEntry.schemaTag;
  if (pChild->type == TSDB_VIRTUAL_CHILD_TABLE && pChild->own.nOwnTags > 0) {
    int32_t nMergedTags = 0;
    code = metaBuildMergedTagView(pChild, &pSuper->stbEntry.schemaTag, &pMergedTags, &nMergedTags);
    if (code) {
      const char* msgFmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
      metaError(msgFmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
      goto _exit;
    }
    mergedTagSchema.nCols = nMergedTags;
    mergedTagSchema.version = pSuper->stbEntry.schemaTag.version;
    mergedTagSchema.pSchema = pMergedTags;
    pTagSchema = &mergedTagSchema;
  }

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

  // When tag-ref was cleared, return updated meta so client catalog gets refreshed
  if (tagRefCleared && pMetaRsp) {
    code = metaUpdateVtbMetaRsp(pChild, (char*)tbName, &pSuper->stbEntry.schemaRow, (pChild->type == TSDB_VIRTUAL_CHILD_TABLE) ? &pSuper->stbEntry.schemaTag : NULL,
                                &pChild->colRef, pSuper->pExtSchemas,
                                pSuper->stbEntry.ownerId, pMetaRsp, pChild->type);
    if (code) {
      metaError("vgId:%d, %s failed to build meta response for %s, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, tbName, version);
      goto _exit;
    }
  }

_exit:
  taosMemoryFree(pMergedTags);
  taosHashCleanup(pUpdatedTagVals);
  metaFetchEntryFree(&pSuper);
  metaFetchEntryFree(&pChild);
  TAOS_RETURN(code);
}



int32_t metaUpdateTableMultiTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq,
                                          STableMetaRsp *pMetaRsp) {
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
    code = metaUpdateTableTagValue(pMeta, version, pTable->tbName, pTable->tags, pMetaRsp);
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
    vnodeAlterTagForQuerySub(pMeta->pVnode, uidList, NULL, tagListArray);
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

  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, uid, &pEntry);
  metaULock(pMeta);
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

    SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, suid, 1, 0);
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

// Pre-scan to collect affected child table UIDs and their current versions
// for TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL transactional path.
int32_t metaPreScanChildTableTagUpdate(SMeta *pMeta, SVAlterTbReq *pReq, SArray *pUids, SArray *pVersions) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SNode      *pWhere = NULL;
  SMetaEntry *pSuper = NULL;
  SArray     *pChildUids = NULL;

  if (pReq->tbName == NULL || strlen(pReq->tbName) == 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  if (pReq->whereLen > 0) {
    // nodesMsgToNode() byte-swaps the TLV headers of its input buffer IN PLACE (tlvGetNextTlv
    // does ntohs/ntohl on pBuf despite the const contract). This prescan is the FIRST of two
    // decodes of pReq->where within one txn apply — the authoritative metaUpdateTableChildTable
    // TagValue() decodes it again. Decoding the original here would corrupt it and make the
    // second decode fail ("invalid where condition"). Decode a private copy so pReq->where stays
    // pristine for the real update. (Non-txn path has no prescan, so it never hit this.)
    char *whereCopy = taosMemoryMalloc(pReq->whereLen);
    if (whereCopy == NULL) {
      return terrno;
    }
    memcpy(whereCopy, pReq->where, pReq->whereLen);
    code = nodesMsgToNode(whereCopy, pReq->whereLen, &pWhere);
    taosMemoryFree(whereCopy);
    if (code) {
      return code;
    }
  }

  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pSuper);
  metaULock(pMeta);
  if (code) {
    goto _exit;
  }

  if (pSuper->type != TSDB_SUPER_TABLE) {
    code = TSDB_CODE_VND_INVALID_TABLE_ACTION;
    goto _exit;
  }

  pChildUids = taosArrayInit(16, sizeof(int64_t));
  if (pChildUids == NULL) {
    code = terrno;
    goto _exit;
  }

  code = metaGetChildUidsByWhere(pMeta, pSuper->uid, pWhere, pChildUids);
  if (code) {
    goto _exit;
  }

  // Hold read lock across all fetches for a consistent snapshot of versions.
  // metaGetChildUidsByWhere above uses its own cursor-level locking internally.
  int32_t nChildren = taosArrayGetSize(pChildUids);
  if (nChildren > 0) {
    metaRLock(pMeta);
    for (int32_t i = 0; i < nChildren; ++i) {
      tb_uid_t    uid = *(tb_uid_t *)TARRAY_GET_ELEM(pChildUids, i);
      SMetaEntry *pChild = NULL;
      code = metaFetchEntryByUid(pMeta, uid, &pChild);
      if (code || pChild == NULL) {
        code = TSDB_CODE_SUCCESS;
        continue;  // child may have been dropped concurrently
      }
      // Preserve the ORIGINAL pre-txn version across repeated ALTERs of the same uid within one
      // txn. If this child is already PRE_ALTER from the same txn, pChild->version is an
      // intermediate in-txn version; capturing it would make outside readers redirect to (and
      // rollback restore) an uncommitted value. Reuse the already-recorded original txnOrigVer.
      int64_t ver = pChild->version;
      if (pChild->txnStatus == META_TXN_PRE_ALTER && pChild->txnId == pReq->txnId && pChild->txnOrigVer >= 0) {
        ver = pChild->txnOrigVer;
      }
      if (taosArrayPush(pUids, &uid) == NULL || taosArrayPush(pVersions, &ver) == NULL) {
        metaFetchEntryFree(&pChild);
        metaULock(pMeta);
        code = terrno;
        goto _exit;
      }
      metaFetchEntryFree(&pChild);
    }
    metaULock(pMeta);
  }

_exit:
  taosArrayDestroy(pChildUids);
  metaFetchEntryFree(&pSuper);
  nodesDestroyNode(pWhere);
  return code;
}

int32_t metaUpdateTableChildTableTagValue(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq,
                                          STableMetaRsp *pMetaRsp) {
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

  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pSuper);
  metaULock(pMeta);
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

    // For virtual child tables, clear tag-ref when setting static value
    bool tagRefCleared = false;
    if (pChild->type == TSDB_VIRTUAL_CHILD_TABLE &&
        pChild->colRef.nTagRefs > 0 && pChild->colRef.pTagRef) {
      for (int32_t r = 0; r < pChild->colRef.nTagRefs; r++) {
        if (pChild->colRef.pTagRef[r].hasRef) {
          int32_t refColId = pChild->colRef.pTagRef[r].id;
          if (taosHashGet(pUpdatedTagVals, &refColId, sizeof(int32_t))) {
            pChild->colRef.pTagRef[r].hasRef = false;
            memset(pChild->colRef.pTagRef[r].refDbName, 0, TSDB_DB_NAME_LEN);
            memset(pChild->colRef.pTagRef[r].refTableName, 0, TSDB_TABLE_NAME_LEN);
            memset(pChild->colRef.pTagRef[r].refColName, 0, TSDB_COL_NAME_LEN);
            pChild->colRef.version++;
            tagRefCleared = true;
          }
        }
      }
    }

    pChild->version = version;
    // For a virtual child table with own (extension) tags the shared tag blob must be rebuilt
    // with the merged tag schema (vstb tags + own tags); rebuilding with the vstb-only schema
    // would silently drop the stored values of the own tags
    SSchema        *pMergedTags = NULL;
    SSchemaWrapper  mergedTagSchema = {0};
    SSchemaWrapper *pChildTagSchema = &pSuper->stbEntry.schemaTag;
    if (pChild->type == TSDB_VIRTUAL_CHILD_TABLE && pChild->own.nOwnTags > 0) {
      int32_t nMergedTags = 0;
      code = metaBuildMergedTagView(pChild, &pSuper->stbEntry.schemaTag, &pMergedTags, &nMergedTags);
      if (code) {
        const char* fmt = "vgId:%d, %s failed at %s:%d since %s, version:%" PRId64;
        metaError(fmt, TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version);
        // metaBuildMergedTagView either fills pMergedTags fully or leaves it NULL on failure;
        // this function's _exit does not free it (unlike metaUpdateTableTagValue's), so release
        // it here. pMergedTags is loop-scoped and re-initialized to NULL each iteration, so the
        // taosMemoryFree on the success path cannot double-free.
        taosMemoryFree(pMergedTags);
        metaFetchEntryFree(&pChild);
        goto _exit;
      }
      mergedTagSchema.nCols = nMergedTags;
      mergedTagSchema.version = pSuper->stbEntry.schemaTag.version;
      mergedTagSchema.pSchema = pMergedTags;
      pChildTagSchema = &mergedTagSchema;
    }
    code = metaUpdateTableTagValueImpl(pMeta, pChild, pChildTagSchema, pUpdatedTagVals);
    taosMemoryFree(pMergedTags);
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
      // Return updated meta for the first table with cleared tag-ref
      if (tagRefCleared && pMetaRsp && pMetaRsp->pSchemas == NULL) {
        int32_t rc = metaUpdateVtbMetaRsp(pChild, pChild->name, &pSuper->stbEntry.schemaRow, (pChild->type == TSDB_VIRTUAL_CHILD_TABLE) ? &pSuper->stbEntry.schemaTag : NULL,
                                          &pChild->colRef, pSuper->pExtSchemas,
                                          pSuper->stbEntry.ownerId, pMetaRsp, pChild->type);
        if (rc) {
          metaError("vgId:%d, %s failed to build meta response for %s, version:%" PRId64,
                    TD_VID(pMeta->pVnode), __func__, pChild->name, version);
          code = rc;
          metaFetchEntryFree(&pChild);
          goto _exit;
        }
      }
    }

    metaFetchEntryFree(&pChild);
  }

_exit:
  DestoryThreadLocalRegComp();
  if (taosArrayGetSize(uidListForTmq) > 0) {
    vnodeAlterTagForQuerySub(pMeta->pVnode, uidListForTmq, pReq->pMultiTag, NULL);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
  SSchemaWrapper  *pSchemaWrapper = (pEntry->type == TSDB_NORMAL_TABLE)
                                     ? &pEntry->ntbEntry.schemaRow
                                     : &pEntry->stbEntry.schemaRow;
  SColCmprWrapper *wp = &pEntry->colCmpr;
  for (int32_t i = 0; i < wp->nCols; i++) {
    SColCmpr *p = &wp->pColCmpr[i];
    if (p->id == pReq->colId) {
      // validate the new compress algorithm
      bool foundInSchema = false;
      for (int32_t j = 0; j < pSchemaWrapper->nCols; j++) {
        if (pSchemaWrapper->pSchema[j].colId == pReq->colId) {
          foundInSchema = true;
          code = validColCmprByType(pSchemaWrapper->pSchema[j].type, pReq->compress);
          if (code != TSDB_CODE_SUCCESS) {
            metaError("vgId:%d, %s failed at %s:%d since %s, colId:%d version:%" PRId64,
                      TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
                      tstrerror(code), pReq->colId, version);
            metaFetchEntryFree(&pEntry);
            TAOS_RETURN(code);
          }
          break;
        }
      }
      if (!foundInSchema) {
        metaError("vgId:%d, %s failed at %s:%d since colId %d not found in schema, version:%" PRId64,
                  TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colId, version);
        metaFetchEntryFree(&pEntry);
        TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
      }
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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

  // do update column ref
  pEntry->version = version;
  pColRef->hasRef = true;
  pColRef->id = pSchema->pSchema[iColumn].colId;
  pColRef->refType = pReq->refType;
  tstrncpy(pColRef->refSourceName, pReq->refSourceName ? pReq->refSourceName : "", TSDB_EXT_SOURCE_NAME_LEN);
  pColRef->refSchemaName[0] = '\0';
  tstrncpy(pColRef->refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pColRef->refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pColRef->refColName, pReq->refColName, TSDB_COL_NAME_LEN);

  // If external ref, look up matching series for tag condition
  taosMemoryFreeClear(pColRef->tagCondJson);
  pColRef->tagCondLen = 0;
  if (pReq->refType == 1 && pEntry->series.nSeries > 0) {
    for (int32_t i = 0; i < pEntry->series.nSeries; i++) {
      SSeriesEntry *s = &pEntry->series.pSeries[i];
      bool matched = false;
      if (pReq->seriesAlias && pReq->seriesAlias[0] != '\0') {
        matched = (strcasecmp(s->alias, pReq->seriesAlias) == 0);
      } else {
        matched = (strcasecmp(s->sourceName, pReq->refSourceName ? pReq->refSourceName : "") == 0 &&
                   strcasecmp(s->dbName, pReq->refDbName) == 0 &&
                   strcasecmp(s->measurementName, pReq->refTbName) == 0);
      }
      if (matched) {
        if (s->tagCondLen > 0 && s->tagCondJson) {
          pColRef->tagCondJson = taosStrdup(s->tagCondJson);
          pColRef->tagCondLen = s->tagCondLen;
        }
        break;
      }
    }
  }
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
      pEntry, pReq->tbName, pSchema, (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) ? &pSuper->stbEntry.schemaTag : NULL, &pEntry->colRef,
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
        pRsp->pColRefs[i].refType = p->refType;
        tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
      pEntry, pReq->tbName, pSchema, (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) ? &pSuper->stbEntry.schemaTag : NULL, &pEntry->colRef,
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
        pRsp->pColRefs[i].refType = p->refType;
        tstrncpy(pRsp->pColRefs[i].refSourceName, p->refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
        tstrncpy(pRsp->pColRefs[i].refSchemaName, p->refSchemaName, TSDB_EXT_SOURCE_SCHEMA_LEN);
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

int32_t metaAlterTagRef(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->colName || NULL == pReq->refDbName || NULL == pReq->refTbName || NULL == pReq->refColName) {
    metaError("vgId:%d, %s failed at %s:%d since invalid request params, version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, version);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  // fetch child entry
  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since table %s not found, version:%" PRId64, TD_VID(pMeta->pVnode), __func__,
              __FILE__, __LINE__, pReq->tbName, version);
    TAOS_RETURN(code);
  }

  if (pEntry->type != TSDB_VIRTUAL_CHILD_TABLE && pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE) {
    metaError("vgId:%d, %s failed at %s:%d since table %s is not virtual child/normal table, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->version >= version) {
    metaError("vgId:%d, %s failed at %s:%d since table %s version %" PRId64 " is not less than %" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->tbName, pEntry->version, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // virtual child tables resolve the tag colId from the super table's tag schema; virtual normal
  // tables own their tag schema in ntbEntry.
  SMetaEntry     *pSuper = NULL;
  SSchemaWrapper *pTagSchema = NULL;
  if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    code = metaFetchEntryByUid(pMeta, pEntry->ctbEntry.suid, &pSuper);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since super table uid %" PRId64 " not found, version:%" PRId64,
                TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pEntry->ctbEntry.suid, version);
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_INTERNAL_ERROR);
    }
    pTagSchema = &pSuper->stbEntry.schemaTag;
  } else {
    pTagSchema = &pEntry->ntbEntry.schemaTag;
  }

  // find tag colId from the tag schema (for a virtual child table this is the super table's tag
  // schema, selected above)
  col_id_t tagColId = -1;
  for (int32_t i = 0; i < pTagSchema->nCols; i++) {
    if (strncmp(pTagSchema->pSchema[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
      tagColId = pTagSchema->pSchema[i].colId;
      break;
    }
  }
  if (tagColId < 0) {
    // fall back to the vchild's own (extension) tag schemas: an own tag's reference lives in the
    // same sparse pTagRef array, indexed by its colId (allocated from TSDB_VCHILD_OWN_COL_ID_BASE
    // upwards, never colliding with vstb tag colIds). No-op for non-vchild tables (nOwnTags==0).
    for (int32_t i = 0; i < pEntry->own.nOwnTags; i++) {
      if (strncmp(pEntry->own.pOwnTagSchemas[i].name, pReq->colName, TSDB_COL_NAME_LEN) == 0) {
        tagColId = pEntry->own.pOwnTagSchemas[i].colId;
        break;
      }
    }
  }
  if (tagColId < 0) {
    metaError("vgId:%d, %s failed at %s:%d since tag %s not found in table %s, version:%" PRId64,
              TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__, pReq->colName, pReq->tbName, version);
    metaFetchEntryFree(&pEntry);
    metaFetchEntryFree(&pSuper);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // virtual normal tables keep colRef.pTagRef sized to schemaTag (one slot per tag)
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    code = metaEnsureTagRefSize(&pEntry->colRef, pTagSchema);
    if (code) {
      metaFetchEntryFree(&pEntry);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(code);
    }
  }

  // find or create pTagRef entry
  SColRef *pTagRef = NULL;
  for (int32_t i = 0; i < pEntry->colRef.nTagRefs; i++) {
    if (pEntry->colRef.pTagRef[i].id == tagColId) {
      pTagRef = &pEntry->colRef.pTagRef[i];
      break;
    }
  }

  if (NULL == pTagRef) {
    // expand pTagRef array
    int32_t  newCount = pEntry->colRef.nTagRefs + 1;
    SColRef *pNew = (SColRef *)taosMemoryRealloc(pEntry->colRef.pTagRef, newCount * sizeof(SColRef));
    if (NULL == pNew) {
      metaFetchEntryFree(&pEntry);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }
    pEntry->colRef.pTagRef = pNew;
    pTagRef = &pEntry->colRef.pTagRef[pEntry->colRef.nTagRefs];
    memset(pTagRef, 0, sizeof(SColRef));
    pTagRef->id = tagColId;
    pEntry->colRef.nTagRefs = newCount;
  }

  // set tag-ref
  pEntry->version = version;
  pTagRef->hasRef = true;
  tstrncpy(pTagRef->colName, pReq->colName, TSDB_COL_NAME_LEN);  // tmq json meta reads colName
  tstrncpy(pTagRef->refDbName, pReq->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pTagRef->refTableName, pReq->refTbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pTagRef->refColName, pReq->refColName, TSDB_COL_NAME_LEN);
  pEntry->colRef.version++;

  // set tag value to NULL (ref resolves dynamically)
  // For virtual child tables the tag value is in ctbEntry.pTags — we leave it as-is,
  // query-time resolution will override with source value.
  // For virtual normal tables, drop the static value from ntbEntry.pTags so the value
  // fully follows the source (and never resurfaces as a stale literal).
  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE && pEntry->ntbEntry.pTags != NULL) {
    const STag *pOldTag = (const STag *)pEntry->ntbEntry.pTags;
    SArray     *pTagArray = taosArrayInit(pTagSchema->nCols, sizeof(STagVal));
    if (NULL == pTagArray) {
      metaFetchEntryFree(&pEntry);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(terrno);
    }
    for (int32_t i = 0; i < pTagSchema->nCols; i++) {
      SSchema *pCol = &pTagSchema->pSchema[i];
      if (pCol->colId == tagColId) continue;  // the new ref carries no static value
      STagVal value = {.cid = pCol->colId};
      if (tTagGet(pOldTag, &value)) {
        if (taosArrayPush(pTagArray, &value) == NULL) {
          taosArrayDestroy(pTagArray);
          metaFetchEntryFree(&pEntry);
          metaFetchEntryFree(&pSuper);
          TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
        }
      }
    }
    STag *pNewTagVal = NULL;
    code = tTagNew(pTagArray, pTagSchema->version, false, &pNewTagVal);
    taosArrayDestroy(pTagArray);
    if (code) {
      metaFetchEntryFree(&pEntry);
      metaFetchEntryFree(&pSuper);
      TAOS_RETURN(code);
    }
    taosMemoryFree(pEntry->ntbEntry.pTags);
    pEntry->ntbEntry.pTags = (uint8_t *)pNewTagVal;
  }

  // persist
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pEntry->uid, pReq->tbName, version);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " tag ref updated, version:%" PRId64, TD_VID(pMeta->pVnode),
             pReq->tbName, pEntry->uid, version);
  }

  // build response
  if (TSDB_CODE_SUCCESS == code) {
    if (pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
      SSchemaWrapper *pSchemaRow = &pSuper->stbEntry.schemaRow;
      code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchemaRow, &pSuper->stbEntry.schemaTag, &pEntry->colRef,
                                  pSuper->pExtSchemas, pSuper->stbEntry.ownerId, pRsp, pEntry->type);
    } else {
      code = metaFillNtbTableMetaRsp(pEntry, pReq->tbName, pRsp);
    }
  }

  metaFetchEntryFree(&pEntry);
  metaFetchEntryFree(&pSuper);
  TAOS_RETURN(code);
}

int32_t metaAddTableSeries(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->seriesAlias || pReq->seriesAlias[0] == '\0') {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed since table %s not found", TD_VID(pMeta->pVnode), __func__, pReq->tbName);
    TAOS_RETURN(code);
  }

  if (pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE && pEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->version >= version) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // check alias doesn't already exist
  for (int32_t i = 0; i < pEntry->series.nSeries; i++) {
    if (strncmp(pEntry->series.pSeries[i].alias, pReq->seriesAlias, TSDB_COL_NAME_LEN) == 0) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_COL_ALREADY_EXISTS);
    }
  }

  // grow series array
  int32_t newCount = pEntry->series.nSeries + 1;
  SSeriesEntry *pNew = taosMemoryRealloc(pEntry->series.pSeries, newCount * sizeof(SSeriesEntry));
  if (!pNew) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(terrno);
  }
  pEntry->series.pSeries = pNew;

  SSeriesEntry *pSeries = &pEntry->series.pSeries[pEntry->series.nSeries];
  memset(pSeries, 0, sizeof(SSeriesEntry));
  tstrncpy(pSeries->alias, pReq->seriesAlias, TSDB_COL_NAME_LEN);
  tstrncpy(pSeries->sourceName, pReq->seriesSourceName ? pReq->seriesSourceName : "", TSDB_EXT_SOURCE_NAME_LEN);
  tstrncpy(pSeries->dbName, pReq->seriesDbName ? pReq->seriesDbName : "", TSDB_DB_NAME_LEN);
  tstrncpy(pSeries->measurementName, pReq->seriesMeasurementName ? pReq->seriesMeasurementName : "", TSDB_TABLE_NAME_LEN);
  if (pReq->seriesTagCondLen > 0 && pReq->seriesTagCondJson) {
    pSeries->tagCondJson = taosStrdup(pReq->seriesTagCondJson);
    pSeries->tagCondLen = pReq->seriesTagCondLen;
  }
  pEntry->series.nSeries = newCount;

  pEntry->version = version;
  pEntry->colRef.version++;

  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed since %s, table:%s", TD_VID(pMeta->pVnode), __func__, tstrerror(code), pReq->tbName);
  } else {
    metaInfo("vgId:%d, table %s added series %s", TD_VID(pMeta->pVnode), pReq->tbName, pReq->seriesAlias);
    // Build response so client catalog cache is updated with new series
    SSchemaWrapper *pSchema = &pEntry->ntbEntry.schemaRow;
    code = metaUpdateVtbMetaRsp(pEntry, pReq->tbName, pSchema, NULL, &pEntry->colRef, pEntry->pExtSchemas,
                                pEntry->ntbEntry.ownerId, pRsp, pEntry->type);
    if (code) {
      metaError("vgId:%d, %s metaUpdateVtbMetaRsp failed: %s", TD_VID(pMeta->pVnode), __func__, tstrerror(code));
    }
  }

  metaFetchEntryFree(&pEntry);
  TAOS_RETURN(code);
}

int32_t metaRemoveTableSeries(SMeta *pMeta, int64_t version, SVAlterTbReq *pReq, STableMetaRsp *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pReq->seriesAlias || pReq->seriesAlias[0] == '\0') {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  if (code) {
    metaError("vgId:%d, %s failed since table %s not found", TD_VID(pMeta->pVnode), __func__, pReq->tbName);
    TAOS_RETURN(code);
  }

  if (pEntry->type != TSDB_VIRTUAL_NORMAL_TABLE && pEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
  }

  if (pEntry->version >= version) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  // find the series by alias
  int32_t idx = -1;
  for (int32_t i = 0; i < pEntry->series.nSeries; i++) {
    if (strncmp(pEntry->series.pSeries[i].alias, pReq->seriesAlias, TSDB_COL_NAME_LEN) == 0) {
      idx = i;
      break;
    }
  }
  if (idx < 0) {
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(TSDB_CODE_VND_COL_NOT_EXISTS);
  }

  // check no column references this series
  SSeriesEntry *pTarget = &pEntry->series.pSeries[idx];
  for (int32_t i = 0; i < pEntry->colRef.nCols; i++) {
    SColRef *p = &pEntry->colRef.pColRef[i];
    bool sameTagCond = p->tagCondLen == pTarget->tagCondLen;
    if (sameTagCond && p->tagCondLen > 0) {
      sameTagCond = p->tagCondJson != NULL && pTarget->tagCondJson != NULL &&
                    strncmp(p->tagCondJson, pTarget->tagCondJson, p->tagCondLen) == 0;
    }
    if (p->hasRef &&
        strncmp(p->refSourceName, pTarget->sourceName, TSDB_EXT_SOURCE_NAME_LEN) == 0 &&
        strncmp(p->refDbName, pTarget->dbName, TSDB_DB_NAME_LEN) == 0 &&
        strncmp(p->refTableName, pTarget->measurementName, TSDB_TABLE_NAME_LEN) == 0 && sameTagCond) {
      metaFetchEntryFree(&pEntry);
      TAOS_RETURN(TSDB_CODE_VND_INVALID_TABLE_ACTION);
    }
  }

  // free and remove
  taosMemoryFreeClear(pEntry->series.pSeries[idx].tagCondJson);
  if (idx < pEntry->series.nSeries - 1) {
    memmove(&pEntry->series.pSeries[idx], &pEntry->series.pSeries[idx + 1],
            (pEntry->series.nSeries - idx - 1) * sizeof(SSeriesEntry));
  }
  pEntry->series.nSeries--;

  pEntry->version = version;
  code = metaHandleEntry2(pMeta, pEntry);
  if (code) {
    metaError("vgId:%d, %s failed since %s, table:%s", TD_VID(pMeta->pVnode), __func__, tstrerror(code), pReq->tbName);
  } else {
    metaInfo("vgId:%d, table %s removed series %s", TD_VID(pMeta->pVnode), pReq->tbName, pReq->seriesAlias);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->name, &pEntry);
  metaULock(pMeta);
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
  metaRLock(pMeta);
  code = metaFetchEntryByUid(pMeta, pReq->stbUid, &pEntry);
  metaULock(pMeta);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->name, &pEntry);
  metaULock(pMeta);
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

  // batch-meta-txn: mark STB as PRE_ALTER with the ORIGINAL pre-txn version for rollback.
  // On a repeated ALTER of the same STB within one txn, pEntry->version is an intermediate
  // in-txn version; reuse the already-recorded original txnOrigVer instead so rollback and
  // outside readers see the pre-txn value.
  if (pReq->txnId != 0) {
    entry.txnId = pReq->txnId;
    entry.txnStatus = META_TXN_PRE_ALTER;
    entry.txnOrigVer = pEntry->version;
    if (pEntry->txnStatus == META_TXN_PRE_ALTER && pEntry->txnId == pReq->txnId && pEntry->txnOrigVer >= 0) {
      entry.txnOrigVer = pEntry->txnOrigVer;
    }
  }

  // do handle the entry
  code = metaHandleEntry2(pMeta, &entry);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s, uid:%" PRId64 " name:%s version:%" PRId64, TD_VID(pMeta->pVnode),
              __func__, __FILE__, __LINE__, tstrerror(code), pReq->suid, pReq->name, version);
    metaFetchEntryFree(&pEntry);
    TAOS_RETURN(code);
  } else {
    metaInfo("vgId:%d, table %s uid %" PRId64 " is updated, version:%" PRId64 " txnId:%" PRIu64, TD_VID(pMeta->pVnode),
             pReq->name, pReq->suid, version, pReq->txnId);
    // batch-meta-txn: add to txn.idx for COMMIT/ROLLBACK handling.
    // Use the preserved pre-txn original version (entry.txnOrigVer), NOT pEntry->version:
    // on a repeated ALTER of the same STB within one txn, pEntry->version is an intermediate
    // in-txn version. txn.idx feeds pAlterPrevVers, which rollback prefers over the B+ tree
    // entry's txnOrigVer, so passing the intermediate version would roll back to the wrong
    // (in-txn) version. This mirrors the non-STB paths in vnodeSvr.c that pass prevVer/alterPrevVer.
    if (pReq->txnId != 0) {
      code = metaTxnIdxUpsert(pMeta, pReq->suid, pReq->txnId, META_TXN_PRE_ALTER, entry.txnOrigVer);
      if (code != TSDB_CODE_SUCCESS) {
        metaError("vgId:%d, failed to upsert txn.idx for ALTER stb:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode),
                  pReq->name, pReq->suid, tstrerror(code));
      }
    }
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
  metaRLock(pMeta);
  code = metaFetchEntryByName(pMeta, pReq->tbName, &pEntry);
  metaULock(pMeta);
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
