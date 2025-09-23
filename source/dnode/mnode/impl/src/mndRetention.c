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

static int32_t mndProcessTrimTimer(SRpcMsg *pReq);
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
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_TRIM, mndProcessKillTrimReq);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_TRIM_PROGRESS_RSP, mndProcessQueryTrimRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TRIM_DB_TIMER, mndProcessTrimTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_TRIM_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_RETENTION,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndCompactActionEncode,  // reuse compact encode/decode
      .decodeFp = (SdbDecodeFp)mndCompactActionDecode,
      .insertFp = (SdbInsertFp)mndCompactActionInsert,  // reuse compact insert/update/delete
      .updateFp = (SdbUpdateFp)mndCompactActionUpdate,
      .deleteFp = (SdbDeleteFp)mndCompactActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRetention(SMnode *pMnode) { mDebug("mnd retention cleanup"); }

void tFreeRetentionObj(SRetentionObj *pObj) {}

int32_t tSerializeSRetentionObj(void *buf, int32_t bufLen, const SRetentionObj *pObj) {
  return tSerializeSCompactObj(buf, bufLen, (const SCompactObj *)pObj);
}

int32_t tDeserializeSRetentionObj(void *buf, int32_t bufLen, SRetentionObj *pObj) {
  return tDeserializeSCompactObj(buf, bufLen, (SCompactObj *)pObj);
}

SRetentionObj *mndAcquireRetention(SMnode *pMnode, int32_t id) {
  SSdb          *pSdb = pMnode->pSdb;
  SRetentionObj *pObj = sdbAcquire(pSdb, SDB_RETENTION, &id);
  if (pObj == NULL && (terrno != TSDB_CODE_SDB_OBJ_NOT_THERE && terrno != TSDB_CODE_SDB_OBJ_CREATING &&
                       terrno != TSDB_CODE_SDB_OBJ_DROPPING)) {
    terrno = TSDB_CODE_APP_ERROR;
    mError("retention:%" PRId64 ", failed to acquire retention since %s", id, terrstr());
  }
  return pObj;
}

void mndReleaseRetention(SMnode *pMnode, SRetentionObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

int32_t mndRetentionGetDbName(SMnode *pMnode, int32_t id, char *dbname, int32_t len) {
  int32_t        code = 0;
  SRetentionObj *pObj = mndAcquireRetention(pMnode, id);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pObj->dbname, len);
  mndReleaseRetention(pMnode, pObj);
  TAOS_RETURN(code);
}

int32_t mndAddRetentionToTrans(SMnode *pMnode, STrans *pTrans, SRetentionObj *pObj, SDbObj *pDb, SCompactDbRsp *rsp) {
  int32_t code = 0;
  pObj->id = tGenIdPI32();

  tstrncpy(pObj->dbname, pDb->name, sizeof(pObj->dbname));

  pObj->startTime = taosGetTimestampMs();

  SSdbRaw *pVgRaw = mndCompactActionEncode(pObj);
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
  int32_t        code = 0;
  int32_t        lino = 0;

  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep &&
        ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_RETENTION, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pObj->dbname)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pObj->dbname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pObj->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->startTime, false), pObj, &lino, _OVER);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

_OVER:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
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

  mInfo("vgId:%d, build kill retention vnode config req", pVgroup->vgId);
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

  mTrace("vgId:%d, build compact vnode config req, contLen:%d", pVgroup->vgId, contLen);
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

  SSdbRaw *pCommitRaw = mndCompactActionEncode(pObj);
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
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
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

      /*
      SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
        mndTransDrop(pTrans);
        return -1;
      }
      sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
      */
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

int32_t mndProcessKillTrimReq(SRpcMsg *pReq) {
  int32_t           code = 0;
  int32_t           lino = 0;
  SKillTrimReq req = {0};

  if ((code = tDeserializeSKillCompactReq(pReq->pCont, pReq->contLen, &req)) != 0) {
    TAOS_RETURN(code);
  }

  mInfo("start to kill trim:%" PRId32, req.id);

  SMnode        *pMnode = pReq->info.node;
  SRetentionObj *pObj = mndAcquireCompact(pMnode, req.id);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_INVALID_COMPACT_ID;
    tFreeSKillCompactReq(&req);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_TRIM_DB), &lino, _OVER);

  TAOS_CHECK_GOTO(mndKillRetention(pMnode, pReq, pObj), &lino, _OVER);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  char    obj[TSDB_INT32_ID_LEN] = {0};
  int32_t nBytes = snprintf(obj, sizeof(obj), "%d", pObj->id);
  if ((uint32_t)nBytes < sizeof(obj)) {
    auditRecord(pReq, pMnode->clusterId, "killTrim", pObj->dbname, obj, req.sql, req.sqlLen);
  } else {
    mError("trim:%" PRId32 " failed to audit since %s", pObj->id, tstrerror(TSDB_CODE_OUT_OF_RANGE));
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to kill trim %" PRId32 " since %s", req.id, terrstr());
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

int32_t mndProcessQueryTrimRsp(SRpcMsg *pReq) {
  int32_t                  code = 0;
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

  char dbname[TSDB_TABLE_FNAME_LEN] = {0};
  TAOS_CHECK_RETURN(mndRetentionGetDbName(pMnode, id, dbname, TSDB_TABLE_FNAME_LEN));

  if (!mndDbIsExist(pMnode, dbname)) {
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

  if (!mndDbIsExist(pMnode, dbname)) {
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
        SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
        if (pCommitRaw == NULL) {
          mndTransDrop(pTrans);
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          if (terrno != 0) code = terrno;
          TAOS_RETURN(code);
        }
        if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
          mError("retention:%d, trans:%d, failed to append commit log since %s", pDetail->id, pTrans->id, tstrerror(code));
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
    SSdbRaw *pCommitRaw = mndCompactActionEncode(pObj);
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
    int32_t       *pId = taosArrayGet(pArray, i);
    mInfo("begin to pull up retention:%d", *pId);
    SRetentionObj *pObj = mndAcquireCompact(pMnode, *pId);
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

static int32_t mndProcessTrimTimer(SRpcMsg *pReq) {
#ifdef TD_ENTERPRISE
  mTrace("start to process trim timer");
  mndRetentionPullup(pReq->info.node);
#endif
  return 0;
}