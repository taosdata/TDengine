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
#include "mndUser.h"
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
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->dbUid));

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
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->dbUid));
  } else {
    pObj->dbUid = 0;
  }

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

static int32_t mndScanGetDbInfo(SMnode *pMnode, int32_t scanId, char *dbname, int32_t len, int64_t *dbUid) {
  int32_t   code = 0;
  SScanObj *pScan = mndAcquireScan(pMnode, scanId);
  if (pScan == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pScan->dbname, len);
  if (dbUid) *dbUid = pScan->dbUid;
  mndReleaseScan(pMnode, pScan);
  TAOS_RETURN(code);
}

// scan db
int32_t mndAddScanToTran(SMnode *pMnode, STrans *pTrans, SScanObj *pScan, SDbObj *pDb, SScanDbRsp *rsp) {
  int32_t code = 0;
  pScan->scanId = tGenIdPI32();

  tstrncpy(pScan->dbname, pDb->name, sizeof(pScan->dbname));
  pScan->dbUid = pDb->uid;

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
  SUserObj *pUser = NULL;
  SDbObj   *pIterDb = NULL;
  char      objFName[TSDB_OBJ_FNAME_LEN + 1] = {0};
  bool      showAll = false, showIter = false;
  int64_t   dbUid = 0;

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

  MND_SHOW_CHECK_OBJ_PRIVILEGE_ALL(pReq->info.conn.user, PRIV_SHOW_SCANS, PRIV_OBJ_DB, 0, _OVER);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SCAN, pShow->pIter, (void **)&pScan);
    if (pShow->pIter == NULL) break;

    MND_SHOW_CHECK_DB_PRIVILEGE(pDb, pScan->dbname, pScan, MND_OPER_SHOW_SCANS, _OVER);

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
  if (pUser) mndReleaseUser(pMnode, pUser);
  mndReleaseDb(pMnode, pDb);
  if (code != 0) {
    mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  pShow->numOfRows += numOfRows;
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
    taosMemoryFreeClear(pReq);
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

#if 0
  char    obj[TSDB_INT32_ID_LEN] = {0};
  int32_t nBytes = snprintf(obj, sizeof(obj), "%d", pScan->scanId);
  if ((uint32_t)nBytes < sizeof(obj)) {
    auditRecord(pReq, pMnode->clusterId, "killScan", pScan->dbname, obj, killScanReq.sql, killScanReq.sqlLen);
  } else {
    mError("scan:%" PRId32 " failed to audit since %s", pScan->scanId, tstrerror(TSDB_CODE_OUT_OF_RANGE));
  }
#endif
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

  char    dbname[TSDB_TABLE_FNAME_LEN] = {0};
  int64_t dbUid = 0;
  TAOS_CHECK_RETURN(mndScanGetDbInfo(pMnode, scanId, dbname, TSDB_TABLE_FNAME_LEN, &dbUid));

  if (!mndDbIsExist(pMnode, dbname, dbUid)) {
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

  if (!mndDbIsExist(pMnode, dbname, dbUid)) {
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

static int32_t mndBuildScanDbRsp(SScanDbRsp *pScanRsp, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc) {
  int32_t code = 0;
  int32_t rspLen = tSerializeSScanDbRsp(NULL, 0, pScanRsp);
  void   *pRsp = NULL;
  if (useRpcMalloc) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryMalloc(rspLen);
  }

  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  (void)tSerializeSScanDbRsp(pRsp, rspLen, pScanRsp);
  *pRspLen = rspLen;
  *ppRsp = pRsp;
  TAOS_RETURN(code);
}

static int32_t mndSetScanDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t scanTs) {
  int32_t code = 0;
  SDbObj  dbObj = {0};
  memcpy(&dbObj, pDb, sizeof(SDbObj));
  dbObj.scanStartTime = scanTs;

  SSdbRaw *pCommitRaw = mndDbActionEncode(&dbObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }
  TAOS_RETURN(code);
}

static void *mndBuildScanVnodeReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen, int64_t scanTs,
                                  STimeWindow tw) {
  SScanVnodeReq scanReq = {0};
  scanReq.dbUid = pDb->uid;
  scanReq.scanStartTime = scanTs;
  scanReq.tw = tw;
  tstrncpy(scanReq.db, pDb->name, TSDB_DB_FNAME_LEN);

  mInfo("vgId:%d, build scan vnode config req", pVgroup->vgId);
  int32_t contLen = tSerializeSScanVnodeReq(NULL, 0, &scanReq);
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

  if (tSerializeSScanVnodeReq((char *)pReq + sizeof(SMsgHead), contLen, &scanReq) < 0) {
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndBuildScanVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int64_t scanTs,
                                        STimeWindow tw) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildScanVnodeReq(pMnode, pDb, pVgroup, &contLen, scanTs, tw);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_SCAN;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

extern int32_t mndAddScanDetailToTran(SMnode *pMnode, STrans *pTrans, SScanObj *pScan, SVgObj *pVgroup,
                                      SVnodeGid *pVgid, int32_t index);

static int32_t mndSetScanDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, int64_t scanTs, STimeWindow tw,
                                       SArray *vgroupIds, SScanDbRsp *pScanRsp) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  SScanObj scan;
  if ((code = mndAddScanToTran(pMnode, pTrans, &scan, pDb, pScanRsp)) != 0) {
    TAOS_RETURN(code);
  }

  int32_t j = 0;
  int32_t numOfVgroups = taosArrayGetSize(vgroupIds);
  if (numOfVgroups > 0) {
    for (int32_t i = 0; i < numOfVgroups; i++) {
      int64_t vgId = *(int64_t *)taosArrayGet(vgroupIds, i);
      SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);

      if (pVgroup == NULL) {
        mError("db:%s, vgroup:%" PRId64 " not exist", pDb->name, vgId);
        TAOS_RETURN(TSDB_CODE_MND_VGROUP_NOT_EXIST);
      } else if (pVgroup->dbUid != pDb->uid) {
        mError("db:%s, vgroup:%" PRId64 " not belong to db:%s", pDb->name, vgId, pDb->name);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(TSDB_CODE_MND_VGROUP_NOT_EXIST);
      }
      sdbRelease(pSdb, pVgroup);
    }

    for (int32_t i = 0; i < numOfVgroups; i++) {
      int64_t vgId = *(int64_t *)taosArrayGet(vgroupIds, i);
      SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);

      if ((code = mndBuildScanVgroupAction(pMnode, pTrans, pDb, pVgroup, scanTs, tw)) != 0) {
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }

      for (int32_t i = 0; i < pVgroup->replica; i++) {
        SVnodeGid *gid = &pVgroup->vnodeGid[i];
        if ((code = mndAddScanDetailToTran(pMnode, pTrans, &scan, pVgroup, gid, j)) != 0) {
          sdbRelease(pSdb, pVgroup);
          TAOS_RETURN(code);
        }
        j++;
      }
      sdbRelease(pSdb, pVgroup);
    }
  } else {
    while (1) {
      SVgObj *pVgroup = NULL;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;

      if (pVgroup->dbUid == pDb->uid) {
        if ((code = mndBuildScanVgroupAction(pMnode, pTrans, pDb, pVgroup, scanTs, tw)) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pVgroup);
          TAOS_RETURN(code);
        }

        for (int32_t i = 0; i < pVgroup->replica; i++) {
          SVnodeGid *gid = &pVgroup->vnodeGid[i];
          if ((code = mndAddScanDetailToTran(pMnode, pTrans, &scan, pVgroup, gid, j)) != 0) {
            sdbCancelFetch(pSdb, pIter);
            sdbRelease(pSdb, pVgroup);
            TAOS_RETURN(code);
          }
          j++;
        }
      }

      sdbRelease(pSdb, pVgroup);
    }
  }

  TAOS_RETURN(code);
}

static int32_t mndScanDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw, SArray *vgroupIds) {
  int32_t    code = 0;
  int32_t    lino;
  SScanDbRsp scanRsp = {0};

  bool  isExist = false;
  void *pIter = NULL;
  while (1) {
    SScanObj *pScan = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SCAN, pIter, (void **)&pScan);
    if (pIter == NULL) break;

    if (strcmp(pScan->dbname, pDb->name) == 0) {
      isExist = true;
    }
    sdbRelease(pMnode->pSdb, pScan);
  }
  if (isExist) {
    mInfo("scan db:%s already exist", pDb->name);

    if (pReq) {
      int32_t rspLen = 0;
      void   *pRsp = NULL;
      scanRsp.scanId = 0;
      scanRsp.bAccepted = false;
      code = mndBuildScanDbRsp(&scanRsp, &rspLen, &pRsp, true);
      TSDB_CHECK_CODE(code, lino, _OVER);

      pReq->info.rsp = pRsp;
      pReq->info.rspLen = rspLen;
    }

    return TSDB_CODE_MND_SCAN_ALREADY_EXIST;
  }

  int64_t scanTs = taosGetTimestampMs();
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "scan-db");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to scan db:%s", pTrans->id, pDb->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  code = mndTransCheckConflict(pMnode, pTrans);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndSetScanDbCommitLogs(pMnode, pTrans, pDb, scanTs);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndSetScanDbRedoActions(pMnode, pTrans, pDb, scanTs, tw, vgroupIds, &scanRsp);
  TSDB_CHECK_CODE(code, lino, _OVER);

  if (pReq) {
    int32_t rspLen = 0;
    void   *pRsp = NULL;
    scanRsp.bAccepted = true;
    code = mndBuildScanDbRsp(&scanRsp, &rspLen, &pRsp, false);
    TSDB_CHECK_CODE(code, lino, _OVER);
    mndTransSetRpcRsp(pTrans, pRsp, rspLen);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessScanTimer(SRpcMsg *pReq) {
  mTrace("start to process scan timer");
  mndScanPullup(pReq->info.node);
  return 0;
}

int32_t mndProcessScanDbReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  SScanDbReq scanReq = {0};

  if (tDeserializeSScanDbReq(pReq->pCont, pReq->contLen, &scanReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to scan", scanReq.db);

  pDb = mndAcquireDb(pMnode, scanReq.db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SCAN_DB, pDb), NULL, _OVER);

  code = mndScanDb(pMnode, pReq, pDb, scanReq.timeRange, scanReq.vgroupIds);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process scan db req since %s", scanReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSScanDbReq(&scanReq);
  TAOS_RETURN(code);
}