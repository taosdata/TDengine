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
#include "mndScan.h"
#include "audit.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndScan.h"
#include "mndScanDetail.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#define MND_SCAN_VER_NUMBER 1
#define MND_SCAN_ID_LEN     11

static int32_t  mndProcessScanTimer(SRpcMsg *pReq);
static SSdbRaw *mndScanActionEncode(SScanObj *pScan);
static SSdbRow *mndScanActionDecode(SSdbRaw *pRaw);
static int32_t  mndScanActionInsert(SSdb *pSdb, SScanObj *pScan);
static int32_t  mndScanActionUpdate(SSdb *pSdb, SScanObj *pOldScan, SScanObj *pNewScan);
static int32_t  mndScanActionDelete(SSdb *pSdb, SScanObj *pScan);
static int32_t  mndRetrieveScan(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndProcessKillScanReq(SRpcMsg *pReq);
static int32_t  mndProcessQueryScanRsp(SRpcMsg *pReq);

int32_t mndInitScan(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SCAN, mndRetrieveScan);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_SCAN, mndProcessKillScanReq);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_SCAN_PROGRESS_RSP, mndProcessQueryScanRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_SCAN_TIMER, mndProcessScanTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_SCAN_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_SCAN,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndScanActionEncode,
      .decodeFp = (SdbDecodeFp)mndScanActionDecode,
      .insertFp = (SdbInsertFp)mndScanActionInsert,
      .updateFp = (SdbUpdateFp)mndScanActionUpdate,
      .deleteFp = (SdbDeleteFp)mndScanActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupScan(SMnode *pMnode) { mDebug("mnd scan cleanup"); }

void tFreeScanObj(SScanObj *pScan) {}

static int32_t tSerializeSScanObj(void *buf, int32_t bufLen, const SScanObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->scanId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbname));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSScanObj(void *buf, int32_t bufLen, SScanObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->scanId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbname));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static SSdbRaw *mndScanActionEncode(SScanObj *pScan) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSScanObj(NULL, 0, pScan);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_SCAN, MND_SCAN_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSScanObj(buf, tlen, pScan);
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
    mError("scan:%" PRId32 ", failed to encode to raw:%p since %s", pScan->scanId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("scan:%" PRId32 ", encode to raw:%p, row:%p", pScan->scanId, pRaw, pScan);
  return pRaw;
}

static SSdbRow *mndScanActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdbRow  *pRow = NULL;
  SScanObj *pScan = NULL;
  void     *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_SCAN_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("scan read invalid ver, data ver: %d, curr ver: %d", sver, MND_SCAN_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SScanObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pScan = sdbGetRowObj(pRow);
  if (pScan == NULL) {
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

  if ((terrno = tDeserializeSScanObj(buf, tlen, pScan)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("scan:%" PRId32 ", failed to decode from raw:%p since %s", pScan->scanId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("scan:%" PRId32 ", decode from raw:%p, row:%p", pScan->scanId, pRaw, pScan);
  return pRow;
}

static int32_t mndScanActionInsert(SSdb *pSdb, SScanObj *pScan) {
  mTrace("scan:%" PRId32 ", perform insert action", pScan->scanId);
  return 0;
}

static int32_t mndScanActionDelete(SSdb *pSdb, SScanObj *pScan) {
  mTrace("scan:%" PRId32 ", perform delete action", pScan->scanId);
  tFreeScanObj(pScan);
  return 0;
}

static int32_t mndScanActionUpdate(SSdb *pSdb, SScanObj *pOldScan, SScanObj *pNewScan) {
  mTrace("scan:%" PRId32 ", perform update action, old row:%p new row:%p", pOldScan->scanId, pOldScan, pNewScan);

  return 0;
}

static SScanObj *mndAcquireScan(SMnode *pMnode, int64_t scanId) {
  SSdb     *pSdb = pMnode->pSdb;
  SScanObj *pScan = sdbAcquire(pSdb, SDB_SCAN, &scanId);
  if (pScan == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pScan;
}

static void mndReleaseScan(SMnode *pMnode, SScanObj *pScan) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pScan);
  pScan = NULL;
}

static int32_t mndScanGetDbName(SMnode *pMnode, int32_t scanId, char *dbname, int32_t len) {
  int32_t   code = 0;
  SScanObj *pScan = mndAcquireScan(pMnode, scanId);
  if (pScan == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pScan->dbname, len);
  mndReleaseScan(pMnode, pScan);
  TAOS_RETURN(code);
}

// scan db
int32_t mndAddScanToTran(SMnode *pMnode, STrans *pTrans, SScanObj *pScan, SDbObj *pDb, SScanDbRsp *rsp) {
  int32_t code = 0;
  pScan->scanId = tGenIdPI32();

  tstrncpy(pScan->dbname, pDb->name, sizeof(pScan->dbname));

  pScan->startTime = taosGetTimestampMs();

  SSdbRaw *pVgRaw = mndScanActionEncode(pScan);
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

  rsp->scanId = pScan->scanId;

  return 0;
}

// retrieve scan
static int32_t mndRetrieveScan(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SScanObj *pScan = NULL;
  char     *sep = NULL;
  SDbObj   *pDb = NULL;
  int32_t   code = 0;
  int32_t   lino = 0;

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
    pShow->pIter = sdbFetch(pSdb, SDB_SCAN, pShow->pIter, (void **)&pScan);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pScan->scanId, false), pScan, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pScan->dbname)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pScan->dbname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pScan->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pScan, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pScan->startTime, false), pScan, &lino,
                        _OVER);

    numOfRows++;
    sdbRelease(pSdb, pScan);
  }

_OVER:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}

// kill scan
static void *mndBuildKillScanReq(SMnode *pMnode, SVgObj *pVgroup, int32_t *pContLen, int32_t scanId, int32_t dnodeid) {
  SVKillScanReq req = {0};
  req.scanId = scanId;
  req.vgId = pVgroup->vgId;
  req.dnodeId = dnodeid;
  terrno = 0;

  mInfo("vgId:%d, build scan vnode config req", pVgroup->vgId);
  int32_t contLen = tSerializeSVKillScanReq(NULL, 0, &req);
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

  mTrace("vgId:%d, build scan vnode config req, contLen:%d", pVgroup->vgId, contLen);
  int32_t ret = 0;
  if ((ret = tSerializeSVKillScanReq((char *)pReq + sizeof(SMsgHead), contLen, &req)) < 0) {
    terrno = ret;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddKillScanAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, int32_t scanId, int32_t dnodeid) {
  int32_t      code = 0;
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeid);
  if (pDnode == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildKillScanReq(pMnode, pVgroup, &contLen, scanId, dnodeid);
  if (pReq == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_KILL_SCAN;

  mTrace("trans:%d, kill scan msg len:%d", pTrans->id, contLen);

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  return 0;
}

static int32_t mndKillScan(SMnode *pMnode, SRpcMsg *pReq, SScanObj *pScan) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "kill-scan");
  if (pTrans == NULL) {
    mError("scan:%" PRId32 ", failed to drop since %s", pScan->scanId, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, used to kill scan:%" PRId32, pTrans->id, pScan->scanId);

  mndTransSetDbName(pTrans, pScan->dbname, NULL);

  SSdbRaw *pCommitRaw = mndScanActionEncode(pScan);
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
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == pScan->scanId) {
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

      if ((code = mndAddKillScanAction(pMnode, pTrans, pVgroup, pScan->scanId, pDetail->dnodeId)) != 0) {
        mError("trans:%d, failed to append redo action since %s", pTrans->id, terrstr());
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }

      mndReleaseVgroup(pMnode, pVgroup);

      /*
      SSdbRaw *pCommitRaw = mndScanDetailActionEncode(pDetail);
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

static int32_t mndProcessKillScanReq(SRpcMsg *pReq) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SKillScanReq killScanReq = {0};

  if ((code = tDeserializeSKillScanReq(pReq->pCont, pReq->contLen, &killScanReq)) != 0) {
    TAOS_RETURN(code);
  }

  mInfo("start to kill scan:%" PRId32, killScanReq.scanId);

  SMnode   *pMnode = pReq->info.node;
  SScanObj *pScan = mndAcquireScan(pMnode, killScanReq.scanId);
  if (pScan == NULL) {
    code = TSDB_CODE_MND_INVALID_SCAN_ID;
    tFreeSKillScanReq(&killScanReq);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SCAN_DB), &lino, _OVER);

  TAOS_CHECK_GOTO(mndKillScan(pMnode, pReq, pScan), &lino, _OVER);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  char    obj[TSDB_INT32_ID_LEN] = {0};
  int32_t nBytes = snprintf(obj, sizeof(obj), "%d", pScan->scanId);
  if ((uint32_t)nBytes < sizeof(obj)) {
    auditRecord(pReq, pMnode->clusterId, "killScan", pScan->dbname, obj, killScanReq.sql, killScanReq.sqlLen);
  } else {
    mError("scan:%" PRId32 " failed to audit since %s", pScan->scanId, tstrerror(TSDB_CODE_OUT_OF_RANGE));
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to kill scan %" PRId32 " since %s", killScanReq.scanId, terrstr());
  }

  tFreeSKillScanReq(&killScanReq);
  mndReleaseScan(pMnode, pScan);

  TAOS_RETURN(code);
}

// update progress
static int32_t mndUpdateScanProgress(SMnode *pMnode, SRpcMsg *pReq, int32_t scanId, SQueryScanProgressRsp *rsp) {
  int32_t code = 0;

  void *pIter = NULL;
  while (1) {
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == scanId && pDetail->vgId == rsp->vgId && pDetail->dnodeId == rsp->dnodeId) {
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

  return TSDB_CODE_MND_SCAN_DETAIL_NOT_EXIST;
}

static int32_t mndProcessQueryScanRsp(SRpcMsg *pReq) {
  int32_t               code = 0;
  SQueryScanProgressRsp req = {0};
  if (pReq->code != 0) {
    mError("received wrong scan response, req code is %s", tstrerror(pReq->code));
    TAOS_RETURN(pReq->code);
  }
  code = tDeserializeSQueryScanProgressRsp(pReq->pCont, pReq->contLen, &req);
  if (code != 0) {
    mError("failed to deserialize vnode-query-scan-progress-rsp, ret:%d, pCont:%p, len:%d", code, pReq->pCont,
           pReq->contLen);
    TAOS_RETURN(code);
  }

  mDebug("scan:%d, receive query response, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", req.scanId, req.vgId,
         req.dnodeId, req.numberFileset, req.finished);

  SMnode *pMnode = pReq->info.node;

  code = mndUpdateScanProgress(pMnode, pReq, req.scanId, &req);
  if (code != 0) {
    mError("scan:%d, failed to update progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", req.scanId,
           req.vgId, req.dnodeId, req.numberFileset, req.finished);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

// timer
static void mndScanSendProgressReq(SMnode *pMnode, SScanObj *pScan) {
  void *pIter = NULL;

  while (1) {
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == pScan->scanId) {
      SEpSet epSet = {0};

      SDnodeObj *pDnode = mndAcquireDnode(pMnode, pDetail->dnodeId);
      if (pDnode == NULL) break;
      if (addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port) != 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }
      mndReleaseDnode(pMnode, pDnode);

      SQueryScanProgressReq req;
      req.scanId = pDetail->scanId;
      req.vgId = pDetail->vgId;
      req.dnodeId = pDetail->dnodeId;

      int32_t contLen = tSerializeSQueryScanProgressReq(NULL, 0, &req);
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

      if (tSerializeSQueryScanProgressReq((char *)pHead + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req) <= 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }

      SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_SCAN_PROGRESS, .contLen = contLen};

      rpcMsg.pCont = pHead;

      char    detail[1024] = {0};
      int32_t len = tsnprintf(detail, sizeof(detail), "msgType:%s numOfEps:%d inUse:%d",
                              TMSG_INFO(TDMT_VND_QUERY_SCAN_PROGRESS), epSet.numOfEps, epSet.inUse);
      for (int32_t i = 0; i < epSet.numOfEps; ++i) {
        len += tsnprintf(detail + len, sizeof(detail) - len, " ep:%d-%s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
      }

      mDebug("scan:%d, send update progress msg to %s", pDetail->scanId, detail);

      if (tmsgSendReq(&epSet, &rpcMsg) < 0) {
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }
}

static int32_t mndSaveScanProgress(SMnode *pMnode, int32_t scanId) {
  int32_t code = 0;
  bool    needSave = false;
  void   *pIter = NULL;
  while (1) {
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == scanId) {
      mDebug(
          "scan:%d, check save progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->scanId, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      // these 2 number will jump back after dnode restart, so < is not used here
      if (pDetail->numberFileset != pDetail->newNumberFileset || pDetail->finished != pDetail->newFinished)
        needSave = true;
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  char dbname[TSDB_TABLE_FNAME_LEN] = {0};
  TAOS_CHECK_RETURN(mndScanGetDbName(pMnode, scanId, dbname, TSDB_TABLE_FNAME_LEN));

  if (!mndDbIsExist(pMnode, dbname)) {
    needSave = true;
    mWarn("scan:%" PRId32 ", no db exist, set needSave:%s", scanId, dbname);
  }

  if (!needSave) {
    mDebug("scan:%" PRId32 ", no need to save", scanId);
    TAOS_RETURN(code);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, NULL, "update-scan-progress");
  if (pTrans == NULL) {
    mError("trans:%" PRId32 ", failed to create since %s", pTrans->id, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("scan:%d, trans:%d, used to update scan progress.", scanId, pTrans->id);

  mndTransSetDbName(pTrans, dbname, NULL);

  pIter = NULL;
  while (1) {
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == scanId) {
      mInfo(
          "scan:%d, trans:%d, check scan progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->scanId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      pDetail->numberFileset = pDetail->newNumberFileset;
      pDetail->finished = pDetail->newFinished;

      SSdbRaw *pCommitRaw = mndScanDetailActionEncode(pDetail);
      if (pCommitRaw == NULL) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }
      if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        mError("scan:%d, trans:%d, failed to append commit log since %s", pDetail->scanId, pTrans->id, terrstr());
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
    SScanDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->scanId == scanId) {
      mInfo("scan:%d, trans:%d, check scan finished, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d",
            pDetail->scanId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished);

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
    mWarn("scan:%" PRId32 ", no db exist, set all finished:%s", scanId, dbname);
  }

  if (allFinished) {
    mInfo("scan:%d, all finished", scanId);
    pIter = NULL;
    while (1) {
      SScanDetailObj *pDetail = NULL;
      pIter = sdbFetch(pMnode->pSdb, SDB_SCAN_DETAIL, pIter, (void **)&pDetail);
      if (pIter == NULL) break;

      if (pDetail->scanId == scanId) {
        SSdbRaw *pCommitRaw = mndScanDetailActionEncode(pDetail);
        if (pCommitRaw == NULL) {
          mndTransDrop(pTrans);
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          if (terrno != 0) code = terrno;
          TAOS_RETURN(code);
        }
        if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
          mError("scan:%d, trans:%d, failed to append commit log since %s", pDetail->scanId, pTrans->id, terrstr());
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
        mInfo("scan:%d, add drop scandetail action", pDetail->scanDetailId);
      }

      sdbRelease(pMnode->pSdb, pDetail);
    }

    SScanObj *pScan = mndAcquireScan(pMnode, scanId);
    if (pScan == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    SSdbRaw *pCommitRaw = mndScanActionEncode(pScan);
    mndReleaseScan(pMnode, pScan);
    if (pCommitRaw == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
      mError("scan:%d, trans:%d, failed to append commit log since %s", scanId, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
      mError("scan:%d, trans:%d, failed to append commit log since %s", scanId, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    mInfo("scan:%d, add drop scan action", pScan->scanId);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("scan:%d, trans:%d, failed to prepare since %s", scanId, pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

static void mndScanPullup(SMnode *pMnode) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SArray *pArray = taosArrayInit(sdbGetSize(pSdb, SDB_SCAN), sizeof(int32_t));
  if (pArray == NULL) return;

  void *pIter = NULL;
  while (1) {
    SScanObj *pScan = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN, pIter, (void **)&pScan);
    if (pIter == NULL) break;
    if (taosArrayPush(pArray, &pScan->scanId) == NULL) {
      mError("failed to push scan id:%d into array, but continue pull up", pScan->scanId);
    }
    sdbRelease(pSdb, pScan);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    mInfo("begin to pull up");
    int32_t  *pScanId = taosArrayGet(pArray, i);
    SScanObj *pScan = mndAcquireScan(pMnode, *pScanId);
    if (pScan != NULL) {
      mInfo("scan:%d, begin to pull up", pScan->scanId);
      mndScanSendProgressReq(pMnode, pScan);
      if ((code = mndSaveScanProgress(pMnode, pScan->scanId)) != 0) {
        mError("scan:%d, failed to save scan progress since %s", pScan->scanId, tstrerror(code));
      }
      mndReleaseScan(pMnode, pScan);
    }
  }
  taosArrayDestroy(pArray);
}

static int32_t mndScanDispatchAudit(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow *tw) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) {
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
    sqlLen = tsnprintf(sql, sizeof(sql), "scan db %s start with '%s' end with '%s'", pDbName, skeyStr, ekeyStr);
  } else {
    sqlLen =
        tsnprintf(sql, sizeof(sql), "scan db %s start with %" PRIi64 " end with %" PRIi64, pDbName, tw->skey, tw->ekey);
  }
  auditRecord(NULL, pMnode->clusterId, "autoScanDB", name.dbname, "", sql, sqlLen);

  return 0;
}

extern int32_t mndScanDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw, SArray *vgroupIds, bool metaOnly);
static int32_t mndScanDispatch(SRpcMsg *pReq) {
  int32_t code = 0;
#if 0
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int64_t curMs = taosGetTimestampMs();
  int64_t curMin = curMs / 60000LL;

  void   *pIter = NULL;
  SDbObj *pDb = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb))) {
    if (pDb->cfg.scanInterval <= 0) {
      mDebug("db:%p,%s, scan interval is %dm, skip", pDb, pDb->name, pDb->cfg.scanInterval);
      sdbRelease(pSdb, pDb);
      continue;
    }

    if (pDb->cfg.isMount) {
      sdbRelease(pSdb, pDb);
      continue;
    }

    // daysToKeep2 would be altered
    if (pDb->cfg.scanEndTime && (pDb->cfg.scanEndTime <= -pDb->cfg.daysToKeep2)) {
      mWarn("db:%p,%s, scan end time:%dm <= -keep2:%dm , skip", pDb, pDb->name, pDb->cfg.scanEndTime,
            -pDb->cfg.daysToKeep2);
      sdbRelease(pSdb, pDb);
      continue;
    }

    int64_t scanStartTime = pDb->cfg.scanStartTime ? pDb->cfg.scanStartTime : -pDb->cfg.daysToKeep2;
    int64_t scanEndTime = pDb->cfg.scanEndTime ? pDb->cfg.scanEndTime : -pDb->cfg.daysPerFile;

    if (scanStartTime >= scanEndTime) {
      mDebug("db:%p,%s, scan start time:%" PRIi64 "m >= end time:%" PRIi64 "m, skip", pDb, pDb->name, scanStartTime,
             scanEndTime);
      sdbRelease(pSdb, pDb);
      continue;
    }

    int64_t remainder = ((curMin - (int64_t)pDb->cfg.scanTimeOffset * 60LL) % pDb->cfg.scanInterval);
    if (remainder != 0) {
      mDebug("db:%p,%s, current time:%" PRIi64 "m is not divisible by scan interval:%dm, offset:%" PRIi8
             "h, remainder:%" PRIi64 "m, skip",
             pDb, pDb->name, curMin, pDb->cfg.scanInterval, pDb->cfg.scanTimeOffset, remainder);
      sdbRelease(pSdb, pDb);
      continue;
    }

    if ((pDb->scanStartTime / 60000LL) == curMin) {
      mDebug("db:%p:%s, scan has already been dispatched at %" PRIi64 "m(%" PRIi64 "ms), skip", pDb, pDb->name, curMin,
             pDb->scanStartTime);
      sdbRelease(pSdb, pDb);
      continue;
    }

    STimeWindow tw = {
        .skey = convertTimePrecision(curMs + scanStartTime * 60000LL, TSDB_TIME_PRECISION_MILLI, pDb->cfg.precision),
        .ekey = convertTimePrecision(curMs + scanEndTime * 60000LL, TSDB_TIME_PRECISION_MILLI, pDb->cfg.precision)};

    if ((code = mndScanDb(pMnode, NULL, pDb, tw, NULL, false)) == 0) {
      mInfo("db:%p,%s, succeed to dispatch scan with range:[%" PRIi64 ",%" PRIi64 "], interval:%dm, start:%" PRIi64
            "m, end:%" PRIi64 "m, offset:%" PRIi8 "h",
            pDb, pDb->name, tw.skey, tw.ekey, pDb->cfg.scanInterval, scanStartTime, scanEndTime,
            pDb->cfg.scanTimeOffset);
    } else {
      mWarn("db:%p,%s, failed to dispatch scan with range:[%" PRIi64 ",%" PRIi64 "], interval:%dm, start:%" PRIi64
            "m, end:%" PRIi64 "m, offset:%" PRIi8 "h, since %s",
            pDb, pDb->name, tw.skey, tw.ekey, pDb->cfg.scanInterval, scanStartTime, scanEndTime,
            pDb->cfg.scanTimeOffset, tstrerror(code));
    }

    TAOS_UNUSED(mndScanDispatchAudit(pMnode, pReq, pDb, &tw));

    sdbRelease(pSdb, pDb);
  }
#endif
  return 0;
}

static int32_t mndProcessScanTimer(SRpcMsg *pReq) {
  mTrace("start to process scan timer");
  mndScanPullup(pReq->info.node);
  TAOS_UNUSED(mndScanDispatch(pReq));
}

int32_t mndProcessScanDbReq(SRpcMsg *pReq) {
  // TODO
  return TSDB_CODE_OPS_NOT_SUPPORT;
}