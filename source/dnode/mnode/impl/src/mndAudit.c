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

/*
 * mndAudit.c — direct-write audit path
 *
 * This module implements the bypass path that writes audit records directly
 * into the cluster's audit database vnode, without going through taoskeeper.
 *
 * Design overview
 * ---------------
 *  1.  On mnode startup (mndInitAudit), we register mndAuditFlushCb as the
 *      flush callback via auditSetFlushFp().  From this point onward, every
 *      time the audit background thread fires, it calls our callback instead
 *      of sending HTTP to taoskeeper.
 *
 *  2.  mndAuditFlushCb() is invoked with a snapshot of buffered SAuditRecord*
 *      objects (SArray<SAuditRecord*>).  It:
 *        a. Locates the audit database using mndAcquireAuditDb().
 *        b. Picks the first vgroup of the audit DB to hold the write.
 *        c. Acquires the "operations" supertable to obtain its suid and
 *           schema version.
 *        d. Builds an STSchema (column schema only) from SStbObj.
 *        e. For each record, builds an SRow using tRowBuild().
 *        f. Packs everything into an SSubmitReq2 + SSubmitReq2Msg and sends
 *           it to the vgroup leader via tmsgSendReq(TDMT_VND_SUBMIT).
 *
 *  3.  The child table "t_operations_<cluster_id_str>" is auto-created on
 *      first write via pCreateTbReq inside SSubmitTbData.
 *
 *  4.  On mnode shutdown (mndCleanupAudit) we clear the callback so the
 *      audit library falls back to the HTTP path (or simply does nothing if
 *      tsMonitorFqdn is empty).
 *
 * Notes
 * -----
 *  • auditSendRecordsViaFlushFp() already pops a snapshot of the records
 *    array and transfers ownership, so this callback MUST free every
 *    SAuditRecord* and the SArray itself before returning.
 *  • Column IDs in the operations supertable start at 1 (ts).  We derive
 *    them from pStb->pColumns[i].colId which is the authoritative source.
 */

#define _DEFAULT_SOURCE
#include "mndAudit.h"
#include "audit.h"
#include "mndDb.h"
#include "mndStb.h"
#include "mndVgroup.h"
#include "taoserror.h"
#include "tdataformat.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "ttime.h"

/* Name of the supertable that taoskeeper creates inside the audit database. */
#define AUDIT_STB_NAME        "operations"
/* Name prefix for the per-cluster child table. */
#define AUDIT_CTB_PREFIX      "t_operations_"

/* ---------------------------------------------------------------------------
 * Helper: build an SSubmitReq2Msg buffer from an SSubmitReq2.
 * Caller must taosMemoryFree(*ppBuf) when done.
 * -------------------------------------------------------------------------*/
static int32_t mndAuditBuildSubmitMsg(int32_t vgId, SSubmitReq2 *pReq, void **ppBuf, int32_t *pLen) {
  int32_t  encLen = 0;
  int32_t  code   = 0;

  tEncodeSize(tEncodeSubmitReq, pReq, encLen, code);
  if (code != TSDB_CODE_SUCCESS) {
    mError("mndAudit: tEncodeSize failed, code:%s", tstrerror(code));
    return code;
  }

  int32_t totalLen = (int32_t)sizeof(SSubmitReq2Msg) + encLen;
  void   *pBuf     = rpcMallocCont(totalLen);
  if (pBuf == NULL) {
    return terrno;
  }

  ((SSubmitReq2Msg *)pBuf)->header.vgId    = htonl(vgId);
  ((SSubmitReq2Msg *)pBuf)->header.contLen = htonl(totalLen);
  ((SSubmitReq2Msg *)pBuf)->version        = htobe64(1);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, (uint8_t *)pBuf + sizeof(SSubmitReq2Msg), encLen);
  code = tEncodeSubmitReq(&encoder, pReq);
  tEncoderClear(&encoder);

  if (code != TSDB_CODE_SUCCESS) {
    rpcFreeCont(pBuf);
    mError("mndAudit: tEncodeSubmitReq failed, code:%s", tstrerror(code));
    return code;
  }

  *ppBuf = pBuf;
  *pLen  = totalLen;
  return TSDB_CODE_SUCCESS;
}

/* ---------------------------------------------------------------------------
 * Helper: build STSchema from SStbObj column schema.
 * Caller is responsible for calling taosMemoryFree(*ppTSchema).
 * -------------------------------------------------------------------------*/
static int32_t mndAuditBuildTSchema(const SStbObj *pStb, STSchema **ppTSchema) {
  *ppTSchema = tBuildTSchema(pStb->pColumns, pStb->numOfColumns, pStb->colVer);
  if (*ppTSchema == NULL) {
    mError("mndAudit: tBuildTSchema failed");
    return terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

/* ---------------------------------------------------------------------------
 * Helper: build an SRow for one SAuditRecord.
 * pTSchema must match the columns of the "operations" supertable.
 * Returns TSDB_CODE_SUCCESS and sets *ppRow; caller frees via tRowDestroy.
 * -------------------------------------------------------------------------*/
static int32_t mndAuditBuildRow(const SAuditRecord *pRecord, const STSchema *pTSchema, int8_t precision,
                                SRow **ppRow) {
  int32_t  code  = TSDB_CODE_SUCCESS;
  int32_t  nCols = pTSchema->numOfCols;
  SArray  *pVals = taosArrayInit(nCols, sizeof(SColVal));
  if (pVals == NULL) return terrno;

  /*
   * Column layout in the "operations" supertable (created by taoskeeper):
   *   colId  name             type
   *   1      ts               TIMESTAMP
   *   2      user_name        VARCHAR(25)
   *   3      operation        VARCHAR(20)
   *   4      db               VARCHAR(65)
   *   5      resource         VARCHAR(193)
   *   6      client_address   VARCHAR(64)
   *   7      details          VARCHAR(50000)
   *
   * We map each STColumn to the correct field by comparing colId values that
   * are read from the live schema, so schema-version mismatches are tolerated
   * gracefully (unknown columns get a NULL value).
   */
  for (int32_t i = 0; i < nCols; i++) {
    const STColumn *pCol = &pTSchema->columns[i];
    SColVal         cv;

    switch (pCol->colId) {
      case 1: {  /* ts — convert from nanoseconds to the DB's precision */
        int64_t ts = convertTimePrecision(pRecord->curTime, TSDB_TIME_PRECISION_NANO, precision);
        SValue sv = {.type = TSDB_DATA_TYPE_TIMESTAMP, .val = ts};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 2: {  /* user_name — VARCHAR */
        const char *str  = pRecord->user;
        uint32_t    nStr = (uint32_t)strnlen(str, TSDB_USER_LEN - 1);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 3: {  /* operation — VARCHAR */
        const char *str  = pRecord->operation;
        uint32_t    nStr = (uint32_t)strnlen(str, AUDIT_OPERATION_LEN - 1);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 4: {  /* db — VARCHAR */
        const char *str  = pRecord->target1;
        uint32_t    nStr = (uint32_t)strnlen(str, TSDB_DB_NAME_LEN - 1);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 5: {  /* resource — VARCHAR */
        const char *str  = pRecord->target2;
        uint32_t    nStr = (uint32_t)strnlen(str, TSDB_STREAM_NAME_LEN - 1);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 6: {  /* client_address — VARCHAR */
        const char *str  = pRecord->clientAddress;
        uint32_t    nStr = (uint32_t)strnlen(str, sizeof(pRecord->clientAddress) - 1);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      case 7: {  /* details — VARCHAR */
        const char *str  = pRecord->detail ? pRecord->detail : "";
        uint32_t    nStr = (uint32_t)strlen(str);
        SValue      sv   = {.type = TSDB_DATA_TYPE_VARCHAR, .nData = nStr, .pData = (uint8_t *)str};
        cv = COL_VAL_VALUE(pCol->colId, sv);
        break;
      }
      default: {
        /* Unknown column — insert NULL so we stay forward-compatible. */
        cv = COL_VAL_NULL(pCol->colId, pCol->type);
        break;
      }
    }

    if (taosArrayPush(pVals, &cv) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  {
    SRowBuildScanInfo sinfo = {0};
    code = tRowBuild(pVals, (STSchema *)pTSchema, ppRow, &sinfo);
  }

_exit:
  taosArrayDestroy(pVals);
  return code;
}

/* ---------------------------------------------------------------------------
 * Helper: build the SVCreateTbReq for auto-creating the per-cluster child
 * table "t_operations_<cluster_id_str>".
 * -------------------------------------------------------------------------*/
static int32_t mndAuditBuildCreateTbReq(const SStbObj    *pStb,
                                         const char       *clusterIdStr,
                                         SVCreateTbReq   **ppCreateTbReq) {
  int32_t        code         = TSDB_CODE_SUCCESS;
  SVCreateTbReq *pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (pCreateTbReq == NULL) return terrno;

  pCreateTbReq->flags          = 0;
  pCreateTbReq->type           = TSDB_CHILD_TABLE;
  pCreateTbReq->ctb.suid       = pStb->uid;
  pCreateTbReq->ctb.tagNum     = pStb->numOfTags;

  /* Child table name */
  char ctbName[TSDB_TABLE_NAME_LEN] = {0};
  (void)snprintf(ctbName, sizeof(ctbName), "%s%s", AUDIT_CTB_PREFIX, clusterIdStr);
  pCreateTbReq->name = taosStrdup(ctbName);
  if (pCreateTbReq->name == NULL) {
    code = terrno;
    goto _error;
  }
  /* uid must be non-zero; the vnode rejects SVCreateTbReq with uid==0 */
  pCreateTbReq->uid = mndGenerateUid(ctbName, (int32_t)strlen(ctbName));

  /* Supertable short name (without db prefix) */
  SName stbName = {0};
  if (tNameFromString(&stbName, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _error;
  }
  pCreateTbReq->ctb.stbName = taosStrdup(tNameGetTableName(&stbName));
  if (pCreateTbReq->ctb.stbName == NULL) {
    code = terrno;
    goto _error;
  }

  /*
   * Build the tag value for "cluster_id VARCHAR(64)".
   * This must be the first (and only) tag in the schema.
   * We look up its colId from pStb->pTags[0].
   */
  if (pStb->numOfTags < 1 || pStb->pTags == NULL) {
    mError("mndAudit: operations stb has no tags, cannot create child table");
    code = TSDB_CODE_MND_STB_NOT_EXIST;
    goto _error;
  }

  col_id_t tagColId = pStb->pTags[0].colId;

  SArray *pTagArray = taosArrayInit(1, sizeof(STagVal));
  if (pTagArray == NULL) {
    code = terrno;
    goto _error;
  }

  STagVal tagVal = {0};
  tagVal.cid    = tagColId;
  tagVal.type   = TSDB_DATA_TYPE_VARCHAR;
  tagVal.pData  = (uint8_t *)clusterIdStr;
  tagVal.nData  = (uint32_t)strlen(clusterIdStr);

  if (taosArrayPush(pTagArray, &tagVal) == NULL) {
    taosArrayDestroy(pTagArray);
    code = terrno;
    goto _error;
  }

  code = tTagNew(pTagArray, 1, false, (STag **)&pCreateTbReq->ctb.pTag);
  taosArrayDestroy(pTagArray);
  if (code != TSDB_CODE_SUCCESS || pCreateTbReq->ctb.pTag == NULL) {
    mError("mndAudit: tTagNew failed, code:%s", tstrerror(code));
    goto _error;
  }

  /* Tag column name list */
  pCreateTbReq->ctb.tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
  if (pCreateTbReq->ctb.tagName == NULL) {
    code = terrno;
    goto _error;
  }
  if (taosArrayPush(pCreateTbReq->ctb.tagName, pStb->pTags[0].name) == NULL) {
    code = terrno;
    goto _error;
  }

  *ppCreateTbReq = pCreateTbReq;
  return TSDB_CODE_SUCCESS;

_error:
  tdDestroySVCreateTbReq(pCreateTbReq);
  taosMemoryFree(pCreateTbReq);
  return code;
}

/* ---------------------------------------------------------------------------
 * mndAuditFlushCb — the FAuditFlushFp callback registered with the audit lib.
 *
 * pRecords: SArray<SAuditRecord*>  — ownership is transferred to us.
 * param:    SMnode*
 * -------------------------------------------------------------------------*/
static int32_t mndAuditFlushCb(SArray *pRecords, void *param) {
  int32_t  code    = TSDB_CODE_SUCCESS;
  SMnode  *pMnode  = (SMnode *)param;
  int32_t  nRec    = (int32_t)taosArrayGetSize(pRecords);

  /* Collect the first record's cluster-id string for the child-table name. */
  if (nRec == 0) goto _cleanup;

  /* ---------------------------------------------------------------------- */
  /* 1.  Locate the audit database.                                          */
  /* ---------------------------------------------------------------------- */
  SDbObj *pAuditDb = mndAcquireAuditDb(pMnode);
  if (pAuditDb == NULL) {
    mDebug("mndAudit: no audit db found, skipping %d records", nRec);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 2.  Pick the first vgroup of the audit database.                       */
  /* ---------------------------------------------------------------------- */
  SVgObj *pVgroup  = NULL;
  void   *pVgIter  = NULL;
  while (1) {
    pVgIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pVgIter, (void **)&pVgroup);
    if (pVgIter == NULL) break;
    if (pVgroup->dbUid == pAuditDb->uid) {
      sdbCancelFetch(pMnode->pSdb, pVgIter);
      break;
    }
    sdbRelease(pMnode->pSdb, pVgroup);
    pVgroup = NULL;
  }

  if (pVgroup == NULL) {
    mError("mndAudit: no vgroup found for audit db:%s", pAuditDb->name);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 3.  Locate the "operations" supertable inside the audit database.       */
  /* ---------------------------------------------------------------------- */
  char stbFullName[TSDB_TABLE_FNAME_LEN] = {0};
  (void)snprintf(stbFullName, sizeof(stbFullName), "%s.%s", pAuditDb->name, AUDIT_STB_NAME);

  SStbObj *pStb = mndAcquireStb(pMnode, stbFullName);
  if (pStb == NULL) {
    mError("mndAudit: supertable %s not found, skipping flush", stbFullName);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 4.  Build the column STSchema.                                          */
  /* ---------------------------------------------------------------------- */
  STSchema *pTSchema = NULL;
  code = mndAuditBuildTSchema(pStb, &pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 5.  Determine the child table name from the first record's cluster id.  */
  /* ---------------------------------------------------------------------- */
  SAuditRecord *pFirstRec = *(SAuditRecord **)taosArrayGet(pRecords, 0);
  /* strClusterId holds the int64 cluster-id as a decimal string. */
  char clusterIdStr[TSDB_CLUSTER_ID_LEN] = {0};
  tstrncpy(clusterIdStr, pFirstRec->strClusterId, sizeof(clusterIdStr));

  /* ---------------------------------------------------------------------- */
  /* 6.  Build the SVCreateTbReq for auto-creating the child table.          */
  /* ---------------------------------------------------------------------- */
  SVCreateTbReq *pCreateTbReq = NULL;
  code = mndAuditBuildCreateTbReq(pStb, clusterIdStr, &pCreateTbReq);
  if (code != TSDB_CODE_SUCCESS) {
    mError("mndAudit: failed to build create-table req, code:%s", tstrerror(code));
    taosMemoryFree(pTSchema);
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 7.  Build SSubmitTbData: one entry per unique child table (here all     */
  /*     records share the same cluster-id => one child table).              */
  /* ---------------------------------------------------------------------- */
  SSubmitTbData tbData = {0};
  tbData.flags       = SUBMIT_REQ_AUTO_CREATE_TABLE;  /* tell codec to encode pCreateTbReq */
  tbData.suid        = pStb->uid;
  tbData.uid         = pCreateTbReq->uid;  /* must match pCreateTbReq->uid for auto-create */
  tbData.sver        = pStb->colVer;
  tbData.pCreateTbReq = pCreateTbReq; /* auto-create child table on write */

  tbData.aRowP = taosArrayInit(nRec, POINTER_BYTES);
  if (tbData.aRowP == NULL) {
    code = terrno;
    tdDestroySVCreateTbReq(pCreateTbReq);
    taosMemoryFree(pCreateTbReq);
    taosMemoryFree(pTSchema);
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* Build one SRow per audit record. */
  for (int32_t i = 0; i < nRec; i++) {
    SAuditRecord *pRec = *(SAuditRecord **)taosArrayGet(pRecords, i);
    SRow         *pRow = NULL;
    int32_t       rc   = mndAuditBuildRow(pRec, pTSchema, pAuditDb->cfg.precision, &pRow);
    if (rc != TSDB_CODE_SUCCESS) {
      mWarn("mndAudit: failed to build row for record[%d], code:%s — skipping", i, tstrerror(rc));
      continue;
    }
    if (taosArrayPush(tbData.aRowP, &pRow) == NULL) {
      tRowDestroy(pRow);
      mWarn("mndAudit: failed to push row[%d] into aRowP — skipping", i);
    }
  }

  if (taosArrayGetSize(tbData.aRowP) == 0) {
    mWarn("mndAudit: no rows built; skipping submit");
    tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pTSchema);
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 8.  Assemble SSubmitReq2.                                               */
  /* ---------------------------------------------------------------------- */
  SSubmitReq2 submitReq = {0};
  submitReq.aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData));
  if (submitReq.aSubmitTbData == NULL) {
    code = terrno;
    tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pTSchema);
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }
  if (taosArrayPush(submitReq.aSubmitTbData, &tbData) == NULL) {
    code = terrno;
    tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
    tDestroySubmitReq(&submitReq, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pTSchema);
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  /* ---------------------------------------------------------------------- */
  /* 9.  Encode into a wire-level SSubmitReq2Msg and send to vnode leader.   */
  /* ---------------------------------------------------------------------- */
  void   *pBuf  = NULL;
  int32_t bufLen = 0;
  code = mndAuditBuildSubmitMsg(pVgroup->vgId, &submitReq, &pBuf, &bufLen);

  /* submitReq now owns tbData internals — free via tDestroySubmitReq. */
  tDestroySubmitReq(&submitReq, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(pTSchema);
  mndReleaseStb(pMnode, pStb);

  if (code != TSDB_CODE_SUCCESS) {
    mError("mndAudit: failed to build submit msg, code:%s", tstrerror(code));
    sdbRelease(pMnode->pSdb, pVgroup);
    mndReleaseDb(pMnode, pAuditDb);
    goto _cleanup;
  }

  SEpSet  epSet  = mndGetVgroupEpset(pMnode, pVgroup);
  sdbRelease(pMnode->pSdb, pVgroup);
  mndReleaseDb(pMnode, pAuditDb);

  SRpcMsg rpcMsg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont   = pBuf,
      .contLen = bufLen,
  };

  mDebug("mndAudit: sending %d audit records to vgId:%d via TDMT_VND_SUBMIT", nRec, ntohl(((SMsgHead *)pBuf)->vgId));

  code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != TSDB_CODE_SUCCESS) {
    mError("mndAudit: tmsgSendReq failed, code:%s", tstrerror(code));
    /* tmsgSendReq already calls rpcFreeCont(pBuf) on failure; do not free here. */
  }

_cleanup:
  /* Free all audit records (we always own them regardless of success). */
  for (int32_t i = 0; i < nRec; i++) {
    SAuditRecord *pRec = *(SAuditRecord **)taosArrayGet(pRecords, i);
    if (pRec) {
      taosMemoryFree(pRec->detail);
      taosMemoryFree(pRec);
    }
  }
  taosArrayDestroy(pRecords);
  return code;
}

/* ---------------------------------------------------------------------------
 * Public API
 * -------------------------------------------------------------------------*/

int32_t mndInitAudit(SMnode *pMnode) {
#ifdef TD_ENTERPRISE
  /*
   * Register the direct-write callback.  From now on, the audit background
   * thread will call mndAuditFlushCb instead of sending HTTP to taoskeeper.
   */
  auditSetFlushFp(mndAuditFlushCb, pMnode);
  mInfo("mndAudit: direct-write audit flush registered (bypassing taoskeeper)");
#endif
  return TSDB_CODE_SUCCESS;
}

void mndCleanupAudit(SMnode *pMnode) {
#ifdef TD_ENTERPRISE
  /* Clear the callback; the audit library will fall back to the HTTP path. */
  auditSetFlushFp(NULL, NULL);
  mInfo("mndAudit: direct-write audit flush unregistered");
#endif
}
