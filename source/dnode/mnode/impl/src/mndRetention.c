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
#include "mndRetention.h"
#include "audit.h"
#include "mndCompact.h"
#include "mndCompactDetail.h"
#include "mndDb.h"
#include "mndDef.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndRetentionDetail.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#define MND_RETENTION_VER_NUMBER 1

static int32_t mndProcessTrimDbTimer(SRpcMsg *pReq);
static int32_t mndProcessQueryRetentionTimer(SRpcMsg *pReq);
static int32_t mndRetrieveRetention(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveRetention(SMnode *pMnode, void *pIter);

/**
 * @brief mndInitRetention
 *  init retention module.
 *  - trim is equivalent to retention
 * @param pMnode
 * @return
 */

int32_t mndInitRetention(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RETENTION, mndRetrieveRetention);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_RETENTION, mndCancelRetrieveRetention);
  mndSetMsgHandle(pMnode, TDMT_MND_TRIM_DB_TIMER, mndProcessTrimDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_TRIM, mndProcessKillRetentionReq);  // trim is equivalent to retention
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_TRIM_PROGRESS_RSP, mndProcessQueryRetentionRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_QUERY_TRIM_TIMER, mndProcessQueryRetentionTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_TRIM_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_RETENTION,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndRetentionActionEncode,
      .decodeFp = (SdbDecodeFp)mndRetentionActionDecode,
      .insertFp = (SdbInsertFp)mndRetentionActionInsert,
      .updateFp = (SdbUpdateFp)mndRetentionActionUpdate,
      .deleteFp = (SdbDeleteFp)mndRetentionActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRetention(SMnode *pMnode) { mDebug("mnd retention cleanup"); }

void tFreeRetentionObj(SRetentionObj *pObj) { tFreeCompactObj((SCompactObj *)pObj); }

int32_t tSerializeSRetentionObj(void *buf, int32_t bufLen, const SRetentionObj *pObj) {
  return tSerializeSCompactObj(buf, bufLen, (const SCompactObj *)pObj);
}

int32_t tDeserializeSRetentionObj(void *buf, int32_t bufLen, SRetentionObj *pObj) {
  return tDeserializeSCompactObj(buf, bufLen, (SCompactObj *)pObj);
}

SSdbRaw *mndRetentionActionEncode(SRetentionObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSRetentionObj(NULL, 0, pObj);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_RETENTION, MND_RETENTION_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSRetentionObj(buf, tlen, pObj);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OVER);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("retention:%" PRId32 ", failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("retention:%" PRId32 ", encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

SSdbRow *mndRetentionActionDecode(SSdbRaw *pRaw) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SSdbRow       *pRow = NULL;
  SRetentionObj *pObj = NULL;
  void          *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_RETENTION_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("retention read invalid ver, data ver: %d, curr ver: %d", sver, MND_RETENTION_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SRetentionObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OVER);

  if ((terrno = tDeserializeSRetentionObj(buf, tlen, pObj)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("retention:%" PRId32 ", failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("retention:%" PRId32 ", decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

int32_t mndRetentionActionInsert(SSdb *pSdb, SRetentionObj *pObj) {
  mTrace("retention:%" PRId32 ", perform insert action", pObj->id);
  return 0;
}

int32_t mndRetentionActionDelete(SSdb *pSdb, SRetentionObj *pObj) {
  mTrace("retention:%" PRId32 ", perform delete action", pObj->id);
  tFreeRetentionObj(pObj);
  return 0;
}

int32_t mndRetentionActionUpdate(SSdb *pSdb, SRetentionObj *pOldObj, SRetentionObj *pNewObj) {
  mTrace("retention:%" PRId32 ", perform update action, old row:%p new row:%p", pOldObj->id, pOldObj, pNewObj);

  return 0;
}

SRetentionObj *mndAcquireRetention(SMnode *pMnode, int32_t id) {
  SSdb          *pSdb = pMnode->pSdb;
  SRetentionObj *pObj = sdbAcquire(pSdb, SDB_RETENTION, &id);
  if (pObj == NULL && (terrno != TSDB_CODE_SDB_OBJ_NOT_THERE && terrno != TSDB_CODE_SDB_OBJ_CREATING &&
                       terrno != TSDB_CODE_SDB_OBJ_DROPPING)) {
    terrno = TSDB_CODE_APP_ERROR;
    mError("retention:%" PRId32 ", failed to acquire retention since %s", id, terrstr());
  }
  return pObj;
}

void mndReleaseRetention(SMnode *pMnode, SRetentionObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static int32_t mndRetentionGetDbInfo(SMnode *pMnode, int32_t id, char *dbname, int32_t len, int64_t *dbUid) {
  int32_t        code = 0;
  SRetentionObj *pObj = mndAcquireRetention(pMnode, id);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pObj->dbname, len);
  if (dbUid) *dbUid = pObj->dbUid;
  mndReleaseRetention(pMnode, pObj);
  TAOS_RETURN(code);
}

int32_t mndAddRetentionToTrans(SMnode *pMnode, STrans *pTrans, SRetentionObj *pObj, SDbObj *pDb, STrimDbRsp *rsp) {
  int32_t code = 0;
  pObj->id = tGenIdPI32();

  tstrncpy(pObj->dbname, pDb->name, sizeof(pObj->dbname));
  pObj->dbUid = pDb->uid;

  pObj->startTime = taosGetTimestampMs();

  SSdbRaw *pVgRaw = mndRetentionActionEncode(pObj);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pVgRaw)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  rsp->id = pObj->id;

  return 0;
}

static int32_t mndRetrieveRetention(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  int32_t        numOfRows = 0;
  SRetentionObj *pObj = NULL;
  char          *sep = NULL;
  SDbObj        *pDb = NULL;
  int32_t        code = 0, lino = 0;
  char           tmpBuf[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};

  if ((pShow->db[0] != 0) && (sep = strchr(pShow->db, '.')) && (*(++sep) != 0)) {
    if (IS_SYS_DBNAME(sep)) {
      goto _OVER;
    } else if (!(pDb = mndAcquireDb(pMnode, pShow->db))) {
      return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_RETENTION, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pObj->id, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL && strcmp(pDb->name, pObj->dbname) != 0) {
      sdbRelease(pSdb, pObj);
      continue;
    }
    SName name = {0};
    if ((code = tNameFromString(&name, pObj->dbname, T_NAME_ACCT | T_NAME_DB)) != 0) {
      sdbRelease(pSdb, pObj);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    COL_DATA_SET_VAL_GOTO((const char *)tmpBuf, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pObj->startTime, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->triggerType == TSDB_TRIGGER_MANUAL ? "manual" : "auto",
             sizeof(tmpBuf) - VARSTR_HEADER_SIZE);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    COL_DATA_SET_VAL_GOTO((const char *)tmpBuf, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    char *optr = "trim";
    if (pObj->optrType == TSDB_OPTR_SSMIGRATE) {
      optr = "ssmigrate";
    } else if (pObj->optrType == TSDB_OPTR_ROLLUP) {
      optr = "rollup";
    }
    tstrncpy(varDataVal(tmpBuf), optr, sizeof(tmpBuf) - VARSTR_HEADER_SIZE);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    COL_DATA_SET_VAL_GOTO((const char *)tmpBuf, false, pObj, pShow->pIter, _OVER);

    sdbRelease(pSdb, pObj);
    ++numOfRows;
  }

_OVER:
  mndReleaseDb(pMnode, pDb);
  if (code != 0) {
    mError("failed to retrieve retention at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelRetrieveRetention(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_RETENTION);
}

static void *mndBuildKillRetentionReq(SMnode *pMnode, SVgObj *pVgroup, int32_t *pContLen, int32_t id, int32_t dnodeId) {
  SVKillRetentionReq req = {0};
  req.taskId = id;
  req.vgId = pVgroup->vgId;
  req.dnodeId = dnodeId;
  terrno = 0;

  mInfo("vgId:%d, build kill retention req", pVgroup->vgId);
  int32_t contLen = tSerializeSVKillCompactReq(NULL, 0, &req);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  contLen += sizeof(SMsgHead);

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SMsgHead *pHead = pReq;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  int32_t ret = 0;
  if ((ret = tSerializeSVKillCompactReq((char *)pReq + sizeof(SMsgHead), contLen, &req)) < 0) {
    taosMemoryFreeClear(pReq);
    terrno = ret;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddKillRetentionAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, int32_t id, int32_t dnodeId) {
  int32_t      code = 0;
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildKillRetentionReq(pMnode, pVgroup, &contLen, id, dnodeId);
  if (pReq == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_KILL_TRIM;

  mTrace("trans:%d, kill retention msg len:%d", pTrans->id, contLen);

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  return 0;
}

static int32_t mndKillRetention(SMnode *pMnode, SRpcMsg *pReq, SRetentionObj *pObj) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "kill-retention");
  if (pTrans == NULL) {
    mError("retention:%" PRId32 ", failed to drop since %s", pObj->id, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, used to kill retention:%" PRId32, pTrans->id, pObj->id);

  mndTransSetDbName(pTrans, pObj->dbname, NULL);

  SSdbRaw *pCommitRaw = mndRetentionActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  void *pIter = NULL;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == pObj->id) {
      SVgObj *pVgroup = mndAcquireVgroup(pMnode, pDetail->vgId);
      if (pVgroup == NULL) {
        mError("trans:%d, failed to append redo action since %s", pTrans->id, terrstr());
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }

      if ((code = mndAddKillRetentionAction(pMnode, pTrans, pVgroup, pObj->id, pDetail->dnodeId)) != 0) {
        mError("trans:%d, failed to append redo action since %s", pTrans->id, terrstr());
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }

      mndReleaseVgroup(pMnode, pVgroup);
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

int32_t mndProcessKillRetentionReq(SRpcMsg *pReq) {
  int32_t           code = 0;
  int32_t           lino = 0;
  SKillRetentionReq req = {0};  // reuse SKillCompactReq
  int64_t           tss = taosGetTimestampMs();

  if ((code = tDeserializeSKillCompactReq(pReq->pCont, pReq->contLen, &req)) != 0) {
    TAOS_RETURN(code);
  }

  mInfo("start to kill retention:%" PRId32, req.id);

  SMnode        *pMnode = pReq->info.node;
  SRetentionObj *pObj = mndAcquireRetention(pMnode, req.id);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_INVALID_RETENTION_ID;
    tFreeSKillCompactReq(&req);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_TRIM_DB), &lino, _OVER);

  TAOS_CHECK_GOTO(mndKillRetention(pMnode, pReq, pObj), &lino, _OVER);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    char    obj[TSDB_INT32_ID_LEN] = {0};
    int32_t nBytes = snprintf(obj, sizeof(obj), "%d", pObj->id);
    if ((uint32_t)nBytes < sizeof(obj)) {
      int64_t tse = taosGetTimestampMs();
      double  duration = (double)(tse - tss);
      duration = duration / 1000;
      auditRecord(pReq, pMnode->clusterId, "killRetention", pObj->dbname, obj, req.sql, req.sqlLen, duration, 0);
    } else {
      mError("retention:%" PRId32 " failed to audit since %s", pObj->id, tstrerror(TSDB_CODE_OUT_OF_RANGE));
    }
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to kill retention %" PRId32 " since %s", req.id, terrstr());
  }

  tFreeSKillCompactReq((SKillCompactReq *)&req);
  mndReleaseRetention(pMnode, pObj);

  TAOS_RETURN(code);
}

// update progress
static int32_t mndUpdateRetentionProgress(SMnode *pMnode, SRpcMsg *pReq, int32_t id, SQueryRetentionProgressRsp *rsp) {
  int32_t code = 0;

  void *pIter = NULL;
  while (1) {
    SRetentionDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == id && pDetail->vgId == rsp->vgId && pDetail->dnodeId == rsp->dnodeId) {
      pDetail->newNumberFileset = rsp->numberFileset;
      pDetail->newFinished = rsp->finished;
      pDetail->progress = rsp->progress;
      pDetail->remainingTime = rsp->remainingTime;

      sdbCancelFetch(pMnode->pSdb, pIter);
      sdbRelease(pMnode->pSdb, pDetail);

      TAOS_RETURN(code);
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  return TSDB_CODE_MND_COMPACT_DETAIL_NOT_EXIST;
}

int32_t mndProcessQueryRetentionRsp(SRpcMsg *pReq) {
  int32_t                    code = 0;
  SQueryRetentionProgressRsp req = {0};
  if (pReq->code != 0) {
    mError("received wrong retention response, req code is %s", tstrerror(pReq->code));
    TAOS_RETURN(pReq->code);
  }
  code = tDeserializeSQueryCompactProgressRsp(pReq->pCont, pReq->contLen, &req);
  if (code != 0) {
    mError("failed to deserialize vnode-query-retention-progress-rsp, ret:%d, pCont:%p, len:%d", code, pReq->pCont,
           pReq->contLen);
    TAOS_RETURN(code);
  }

  mDebug("retention:%d, receive query response, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", req.id, req.vgId,
         req.dnodeId, req.numberFileset, req.finished);

  SMnode *pMnode = pReq->info.node;

  code = mndUpdateRetentionProgress(pMnode, pReq, req.id, &req);
  if (code != 0) {
    mError("retention:%d, failed to update progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", req.id,
           req.vgId, req.dnodeId, req.numberFileset, req.finished);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

// timer
void mndRetentionSendProgressReq(SMnode *pMnode, SRetentionObj *pObj) {
  void *pIter = NULL;

  while (1) {
    SRetentionDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == pObj->id) {
      SEpSet epSet = {0};

      SDnodeObj *pDnode = mndAcquireDnode(pMnode, pDetail->dnodeId);
      if (pDnode == NULL) break;
      if (addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port) != 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }
      mndReleaseDnode(pMnode, pDnode);

      SQueryRetentionProgressReq req;
      req.id = pDetail->id;
      req.vgId = pDetail->vgId;
      req.dnodeId = pDetail->dnodeId;

      int32_t contLen = tSerializeSQueryCompactProgressReq(NULL, 0, &req);
      if (contLen < 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }

      contLen += sizeof(SMsgHead);

      SMsgHead *pHead = rpcMallocCont(contLen);
      if (pHead == NULL) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }

      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(pDetail->vgId);

      if (tSerializeSQueryCompactProgressReq((char *)pHead + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req) <= 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }

      SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_TRIM_PROGRESS, .contLen = contLen};

      rpcMsg.pCont = pHead;

      char    detail[1024] = {0};
      int32_t len = tsnprintf(detail, sizeof(detail), "msgType:%s numOfEps:%d inUse:%d",
                              TMSG_INFO(TDMT_VND_QUERY_TRIM_PROGRESS), epSet.numOfEps, epSet.inUse);
      for (int32_t i = 0; i < epSet.numOfEps; ++i) {
        len += tsnprintf(detail + len, sizeof(detail) - len, " ep:%d-%s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
      }

      mDebug("retention:%d, send update progress msg to %s", pDetail->id, detail);

      if (tmsgSendReq(&epSet, &rpcMsg) < 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }
}

static int32_t mndSaveRetentionProgress(SMnode *pMnode, int32_t id) {
  int32_t code = 0;
  bool    needSave = false;
  void   *pIter = NULL;
  while (1) {
    SRetentionDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == id) {
      mDebug(
          "retention:%d, check save progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      // these 2 number will jump back after dnode restart, so < is not used here
      if (pDetail->numberFileset != pDetail->newNumberFileset || pDetail->finished != pDetail->newFinished)
        needSave = true;
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  char    dbname[TSDB_TABLE_FNAME_LEN] = {0};
  int64_t dbUid = 0;
  TAOS_CHECK_RETURN(mndRetentionGetDbInfo(pMnode, id, dbname, TSDB_TABLE_FNAME_LEN, &dbUid));

  if (!mndDbIsExist(pMnode, dbname, dbUid)) {
    needSave = true;
    mWarn("retention:%" PRId32 ", no db exist, set needSave:%s", id, dbname);
  }

  if (!needSave) {
    mDebug("retention:%" PRId32 ", no need to save", id);
    TAOS_RETURN(code);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, NULL, "update-retention-progress");
  if (pTrans == NULL) {
    mError("trans:%" PRId32 ", failed to create since %s", pTrans->id, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("retention:%d, trans:%d, used to update retention progress.", id, pTrans->id);

  mndTransSetDbName(pTrans, dbname, NULL);

  pIter = NULL;
  while (1) {
    SRetentionDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == id) {
      mInfo(
          "retention:%d, trans:%d, check compact progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->id, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      pDetail->numberFileset = pDetail->newNumberFileset;
      pDetail->finished = pDetail->newFinished;

      SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
      if (pCommitRaw == NULL) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }
      if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        mError("retention:%d, trans:%d, failed to append commit log since %s", pDetail->id, pTrans->id, terrstr());
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }
      if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  bool allFinished = true;
  pIter = NULL;
  while (1) {
    SRetentionDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->id == id) {
      mInfo("retention:%d, trans:%d, check compact finished, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d",
            pDetail->id, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished);

      if (pDetail->numberFileset == -1 && pDetail->finished == -1) {
        allFinished = false;
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
      if (pDetail->numberFileset != -1 && pDetail->finished != -1 && pDetail->numberFileset != pDetail->finished) {
        allFinished = false;
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if (!mndDbIsExist(pMnode, dbname, dbUid)) {
    allFinished = true;
    mWarn("retention:%" PRId32 ", no db exist, set all finished:%s", id, dbname);
  }

  if (allFinished) {
    mInfo("retention:%d, all finished", id);
    pIter = NULL;
    while (1) {
      SRetentionDetailObj *pDetail = NULL;
      pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION_DETAIL, pIter, (void **)&pDetail);
      if (pIter == NULL) break;

      if (pDetail->id == id) {
        SSdbRaw *pCommitRaw = mndRetentionDetailActionEncode(pDetail);
        if (pCommitRaw == NULL) {
          mndTransDrop(pTrans);
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          if (terrno != 0) code = terrno;
          TAOS_RETURN(code);
        }
        if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
          mError("retention:%d, trans:%d, failed to append commit log since %s", pDetail->id, pTrans->id,
                 tstrerror(code));
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pDetail);
          mndTransDrop(pTrans);
          TAOS_RETURN(code);
        }
        if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pDetail);
          mndTransDrop(pTrans);
          TAOS_RETURN(code);
        }
        mInfo("retention:%d, add drop compactdetail action", pDetail->compactDetailId);
      }

      sdbRelease(pMnode->pSdb, pDetail);
    }

    SRetentionObj *pObj = mndAcquireRetention(pMnode, id);
    if (pObj == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    SSdbRaw *pCommitRaw = mndRetentionActionEncode(pObj);
    mndReleaseRetention(pMnode, pObj);
    if (pCommitRaw == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
      mError("retention:%d, trans:%d, failed to append commit log since %s", id, pTrans->id, tstrerror(code));
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
      mError("retention:%d, trans:%d, failed to append commit log since %s", id, pTrans->id, tstrerror(code));
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    mInfo("retention:%d, add drop compact action", pObj->id);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("retention:%d, trans:%d, failed to prepare since %s", id, pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

static void mndRetentionPullup(SMnode *pMnode) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SArray *pArray = taosArrayInit(sdbGetSize(pSdb, SDB_RETENTION), sizeof(int32_t));
  if (pArray == NULL) return;

  void *pIter = NULL;
  while (1) {
    SRetentionObj *pObj = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_RETENTION, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    if (taosArrayPush(pArray, &pObj->id) == NULL) {
      mError("failed to push retention id:%d into array, but continue pull up", pObj->id);
    }
    sdbRelease(pSdb, pObj);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    int32_t *pId = taosArrayGet(pArray, i);
    mInfo("begin to pull up retention:%d", *pId);
    SRetentionObj *pObj = mndAcquireRetention(pMnode, *pId);
    if (pObj != NULL) {
      mInfo("retention:%d, begin to pull up", pObj->id);
      mndRetentionSendProgressReq(pMnode, pObj);
      if ((code = mndSaveRetentionProgress(pMnode, pObj->id)) != 0) {
        mError("retention:%d, failed to save retention progress since %s", pObj->id, tstrerror(code));
      }
      mndReleaseRetention(pMnode, pObj);
    }
  }
  taosArrayDestroy(pArray);
}

static int32_t mndTrimDbDispatchAudit(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow *tw) {
  int64_t tss = taosGetTimestampMs();
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) {
    return 0;
  }

  if (tsAuditLevel < AUDIT_LEVEL_CLUSTER) {
    return 0;
  }

  SName   name = {0};
  int32_t sqlLen = 0;
  char    sql[256] = {0};
  char    skeyStr[40] = {0};
  char    ekeyStr[40] = {0};
  char   *pDbName = pDb->name;

  if (tNameFromString(&name, pDb->name, T_NAME_ACCT | T_NAME_DB) == 0) {
    pDbName = name.dbname;
  }

  if (taosFormatUtcTime(skeyStr, sizeof(skeyStr), tw->skey, pDb->cfg.precision) == 0 &&
      taosFormatUtcTime(ekeyStr, sizeof(ekeyStr), tw->ekey, pDb->cfg.precision) == 0) {
    sqlLen = tsnprintf(sql, sizeof(sql), "trim db %s start with '%s' end with '%s'", pDbName, skeyStr, ekeyStr);
  } else {
    sqlLen = tsnprintf(sql, sizeof(sql), "trim db %s start with %" PRIi64 " end with %" PRIi64, pDbName, tw->skey,
                       tw->ekey);
  }

  int64_t tse = taosGetTimestampMs();
  double  duration = (double)(tse - tss);
  duration = duration / 1000;
  auditRecord(NULL, pMnode->clusterId, "autoTrimDB", name.dbname, "", sql, sqlLen, duration, 0);

  return 0;
}

extern int32_t mndTrimDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw, SArray *vgroupIds,
                         ETsdbOpType type, ETriggerType triggerType);
static int32_t mndTrimDbDispatch(SRpcMsg *pReq) {
  int32_t    code = 0, lino = 0;
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int64_t    curSec = taosGetTimestampMs() / 1000;
  STrimDbReq trimReq = {
      .tw.skey = INT64_MIN, .tw.ekey = curSec, .optrType = TSDB_OPTR_NORMAL, .triggerType = TSDB_TRIGGER_AUTO};

  void   *pIter = NULL;
  SDbObj *pDb = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb))) {
    if (pDb->cfg.isMount) {
      sdbRelease(pSdb, pDb);
      continue;
    }

    (void)snprintf(trimReq.db, sizeof(trimReq.db), "%s", pDb->name);

    if ((code = mndTrimDb(pMnode, pReq, pDb, trimReq.tw, trimReq.vgroupIds, trimReq.optrType, trimReq.triggerType)) ==
        0) {
      mInfo("db:%s, start to auto trim, optr:%u, tw:%" PRId64 ",%" PRId64, trimReq.db, trimReq.optrType,
            trimReq.tw.skey, trimReq.tw.ekey);
    } else {
      mError("db:%s, failed to auto trim since %s", pDb->name, tstrerror(code));
      sdbRelease(pSdb, pDb);
      continue;
    }

    TAOS_UNUSED(mndTrimDbDispatchAudit(pMnode, pReq, pDb, &trimReq.tw));

    sdbRelease(pSdb, pDb);
  }
_exit:
  return code;
}

static int32_t mndProcessQueryRetentionTimer(SRpcMsg *pReq) {
  mTrace("start to process query trim timer");
  mndRetentionPullup(pReq->info.node);
  return 0;
}

static int32_t mndProcessTrimDbTimer(SRpcMsg *pReq) {
  mTrace("start to process trim db timer");
  return mndTrimDbDispatch(pReq);
}
