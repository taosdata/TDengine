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
#include "mndCompact.h"
#include "mndTrans.h"
#include "mndShow.h"
#include "mndDb.h"
#include "mndCompactDetail.h"
#include "mndVgroup.h"
#include "tmsgcb.h"
#include "mndDnode.h"
#include "tmisce.h"
#include "audit.h"
#include "mndPrivilege.h"
#include "mndTrans.h"

#define MND_COMPACT_VER_NUMBER 1
#define MND_COMPACT_ID_LEN 11

static int32_t mndProcessCompactTimer(SRpcMsg *pReq);

int32_t mndInitCompact(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_COMPACT, mndRetrieveCompact);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_COMPACT, mndProcessKillCompactReq);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_COMPACT_PROGRESS_RSP, mndProcessQueryCompactRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_COMPACT_TIMER, mndProcessCompactTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_COMPACT_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_COMPACT,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndCompactActionEncode,
      .decodeFp = (SdbDecodeFp)mndCompactActionDecode,
      .insertFp = (SdbInsertFp)mndCompactActionInsert,
      .updateFp = (SdbUpdateFp)mndCompactActionUpdate,
      .deleteFp = (SdbDeleteFp)mndCompactActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCompact(SMnode *pMnode) {
  mDebug("mnd compact cleanup");
}

void tFreeCompactObj(SCompactObj *pCompact) {
}

int32_t tSerializeSCompactObj(void *buf, int32_t bufLen, const SCompactObj *pObj) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pObj->compactId) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->dbname) < 0) return -1;
  if (tEncodeI64(&encoder, pObj->startTime) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactObj(void *buf, int32_t bufLen, SCompactObj *pObj) {
  int8_t ex = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pObj->compactId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->dbname) < 0) return -1;
  if (tDecodeI64(&decoder, &pObj->startTime) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

SSdbRaw *mndCompactActionEncode(SCompactObj *pCompact) {
  terrno = TSDB_CODE_SUCCESS;

  void *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSCompactObj(NULL, 0, pCompact);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_COMPACT, MND_COMPACT_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSCompactObj(buf, tlen, pCompact);
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
    mError("compact:%" PRId32 ", failed to encode to raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("compact:%" PRId32 ", encode to raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRaw;
}

SSdbRow *mndCompactActionDecode(SSdbRaw *pRaw) {
  SSdbRow       *pRow = NULL;
  SCompactObj   *pCompact = NULL;
  void          *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_COMPACT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("compact read invalid ver, data ver: %d, curr ver: %d", sver, MND_COMPACT_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SCompactObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pCompact = sdbGetRowObj(pRow);
  if (pCompact == NULL) {
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

  if (tDeserializeSCompactObj(buf, tlen, pCompact) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  //taosInitRWLatch(&pView->lock);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("compact:%" PRId32 ", failed to decode from raw:%p since %s", pCompact->compactId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("compact:%" PRId32 ", decode from raw:%p, row:%p", pCompact->compactId, pRaw, pCompact);
  return pRow;
}

int32_t mndCompactActionInsert(SSdb *pSdb, SCompactObj *pCompact) {
  mTrace("compact:%" PRId32 ", perform insert action", pCompact->compactId);
  return 0;
}

int32_t mndCompactActionDelete(SSdb *pSdb, SCompactObj *pCompact) {
  mTrace("compact:%" PRId32 ", perform insert action", pCompact->compactId);
  tFreeCompactObj(pCompact);
  return 0;
}

int32_t mndCompactActionUpdate(SSdb *pSdb, SCompactObj *pOldCompact, SCompactObj *pNewCompact) {
  mTrace("compact:%" PRId32 ", perform update action, old row:%p new row:%p", 
          pOldCompact->compactId, pOldCompact, pNewCompact);

  return 0;
}

SCompactObj *mndAcquireCompact(SMnode *pMnode, int64_t compactId) {
  SSdb       *pSdb = pMnode->pSdb;
  SCompactObj   *pCompact = sdbAcquire(pSdb, SDB_COMPACT, &compactId);
  if (pCompact == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pCompact;
}

void mndReleaseCompact(SMnode *pMnode, SCompactObj *pCompact) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pCompact);
}

//compact db
int32_t mndAddCompactToTran(SMnode *pMnode, STrans *pTrans, SCompactObj* pCompact, SDbObj *pDb, SCompactDbRsp *rsp){
  pCompact->compactId = tGenIdPI32();

  strcpy(pCompact->dbname, pDb->name);

  pCompact->startTime = taosGetTimestampMs();

  SSdbRaw *pVgRaw = mndCompactActionEncode(pCompact);
  if (pVgRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pVgRaw) != 0) {
    sdbFreeRaw(pVgRaw);
    return -1;
  }
  (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);

  rsp->compactId = pCompact->compactId;

  return 0;
}

//retrieve compact
int32_t mndRetrieveCompact(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SCompactObj   *pCompact = NULL;
  char       *sep = NULL;
  SDbObj     *pDb = NULL;
  
  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep && ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_COMPACT, pShow->pIter, (void **)&pCompact);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pCompact->compactId, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pCompact->dbname)) {
      SName name = {0};
      tNameFromString(&name, pCompact->dbname, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      strncpy(varDataVal(tmpBuf), pCompact->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pCompact->startTime, false);

    numOfRows++;
    sdbRelease(pSdb, pCompact);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}

//kill compact
static void *mndBuildKillCompactReq(SMnode *pMnode, SVgObj *pVgroup, int32_t *pContLen, 
                                    int32_t compactId, int32_t dnodeid) {
  SVKillCompactReq req = {0};
  req.compactId = compactId;
  req.vgId = pVgroup->vgId;
  req.dnodeId = dnodeid;

  mInfo("vgId:%d, build compact vnode config req", pVgroup->vgId);
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

  tSerializeSVKillCompactReq((char *)pReq + sizeof(SMsgHead), contLen, &req);
  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddKillCompactAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, 
                                        int32_t compactId, int32_t dnodeid) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeid);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildKillCompactReq(pMnode, pVgroup, &contLen, compactId, dnodeid);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_KILL_COMPACT;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndKillCompact(SMnode *pMnode, SRpcMsg *pReq, SCompactObj *pCompact) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "kill-compact");
  if (pTrans == NULL) {
    mError("compact:%" PRId32 ", failed to drop since %s" , pCompact->compactId, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to kill compact:%" PRId32, pTrans->id, pCompact->compactId);

  SSdbRaw *pCommitRaw = mndCompactActionEncode(pCompact);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  void   *pIter = NULL;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->compactId == pCompact->compactId) {
      SVgObj *pVgroup = mndAcquireVgroup(pMnode, pDetail->vgId);
      if(pVgroup == NULL){
        mError("trans:%d, failed to append redo action since %s", pTrans->id, terrstr());
        mndTransDrop(pTrans);
        return -1;
      }

      if(mndAddKillCompactAction(pMnode, pTrans, pVgroup, pCompact->compactId, pDetail->dnodeId) != 0){
        mError("trans:%d, failed to append redo action since %s", pTrans->id, terrstr());
        mndTransDrop(pTrans);
        return -1;
      }

      mndReleaseVgroup(pMnode, pVgroup);

      /*
      SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
        mndTransDrop(pTrans);
        return -1;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
      */
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

int32_t mndProcessKillCompactReq(SRpcMsg *pReq){
  SKillCompactReq killCompactReq = {0};
  if (tDeserializeSKillCompactReq(pReq->pCont, pReq->contLen, &killCompactReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("start to kill compact:%" PRId32, killCompactReq.compactId);

  SMnode        *pMnode = pReq->info.node;
  int32_t       code = -1;
  SCompactObj   *pCompact = mndAcquireCompact(pMnode, killCompactReq.compactId);
  if(pCompact == NULL){
    terrno = TSDB_CODE_MND_INVALID_COMPACT_ID;
    tFreeSKillCompactReq(&killCompactReq);
    return -1;
  }

  if (0 != mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_COMPACT_DB)) {
    goto _OVER;
  }

  if (mndKillCompact(pMnode, pReq, pCompact) < 0) {
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[MND_COMPACT_ID_LEN] = {0};
  sprintf(obj, "%d", pCompact->compactId);

  auditRecord(pReq, pMnode->clusterId, "killCompact", pCompact->dbname, obj, killCompactReq.sql, killCompactReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to kill compact %" PRId32 " since %s", killCompactReq.compactId, terrstr());
  }

  tFreeSKillCompactReq(&killCompactReq);
  sdbRelease(pMnode->pSdb, pCompact);

  return code;
}

//update progress
static int32_t mndUpdateCompactProgress(SMnode *pMnode, SRpcMsg *pReq, int32_t compactId, SQueryCompactProgressRsp* rsp) {
  void* pIter = NULL;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->compactId == compactId && pDetail->vgId == rsp->vgId && pDetail->dnodeId == rsp->dnodeId) {
      pDetail->newNumberFileset = rsp->numberFileset;
      pDetail->newFinished = rsp->finished;

      sdbRelease(pMnode->pSdb, pDetail);

      return 0;
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  return -1;
}

int32_t mndProcessQueryCompactRsp(SRpcMsg *pReq){
  SQueryCompactProgressRsp req = {0};
  if (tDeserializeSQueryCompactProgressRsp(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mDebug("compact:%d, receive query response, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", 
        req.compactId, req.vgId, req.dnodeId, req.numberFileset, req.finished);

  SMnode        *pMnode = pReq->info.node;
  int32_t       code = -1;
  

  if(mndUpdateCompactProgress(pMnode, pReq, req.compactId, &req) != 0){
    mError("compact:%d, failed to update progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", 
        req.compactId, req.vgId, req.dnodeId, req.numberFileset, req.finished);
    return -1;
  }

  return 0;
}

//timer
void mndCompactSendProgressReq(SMnode *pMnode, SCompactObj *pCompact){
  void   *pIter = NULL;

  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->compactId == pCompact->compactId) {
      SEpSet epSet = {0};

      SDnodeObj       *pDnode = mndAcquireDnode(pMnode, pDetail->dnodeId);
      if(pDnode == NULL) break;
      addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port);
      mndReleaseDnode(pMnode, pDnode);

      SQueryCompactProgressReq req;
      req.compactId = pDetail->compactId;
      req.vgId = pDetail->vgId;
      req.dnodeId = pDetail->dnodeId;

      int32_t contLen = tSerializeSQueryCompactProgressReq(NULL, 0, &req);
      if (contLen < 0) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        continue;
      }

      contLen += sizeof(SMsgHead);

      SMsgHead *pHead = rpcMallocCont(contLen);
      if (pHead == NULL) {
        sdbCancelFetch(pMnode->pSdb, pDetail);
        sdbRelease(pMnode->pSdb, pDetail);
        continue;
      } 

      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(pDetail->vgId);

      tSerializeSQueryCompactProgressReq((char *)pHead + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req);

      SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_COMPACT_PROGRESS, 
                        .contLen = contLen};
      
      //rpcMsg.pCont = rpcMallocCont(contLen);
      //if (rpcMsg.pCont == NULL) {
      //  return;
      //}

      //memcpy(rpcMsg.pCont, pHead, contLen);

      rpcMsg.pCont = pHead;

      char    detail[1024] = {0};
      int32_t len = snprintf(detail, sizeof(detail), "msgType:%s numOfEps:%d inUse:%d", TMSG_INFO(TDMT_VND_QUERY_COMPACT_PROGRESS),
                            epSet.numOfEps, epSet.inUse);
      for (int32_t i = 0; i < epSet.numOfEps; ++i) {
        len += snprintf(detail + len, sizeof(detail) - len, " ep:%d-%s:%u", i, epSet.eps[i].fqdn,
                        epSet.eps[i].port);
      }

      mDebug("compact:%d, send update progress msg to %s", pDetail->compactId, detail);

      tmsgSendReq(&epSet, &rpcMsg);
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }
}

static int32_t mndSaveCompactProgress(SMnode *pMnode, int32_t compactId) {
  bool needSave = false;
  void* pIter = NULL;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->compactId == compactId) {
      mDebug("compact:%d, check save progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
            "newNumberFileset:%d, newFinished:%d", 
            pDetail->compactId, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
            pDetail->newNumberFileset, pDetail->newFinished);

      //these 2 number will jump back after dnode restart, so < is not used here
      if(pDetail->numberFileset != pDetail->newNumberFileset || pDetail->finished != pDetail->newFinished)
        needSave = true;
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if(!needSave) {
    mDebug("compact:%" PRId32 ", no need to save" , compactId);
    return 0;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-compact-progress");
  if (pTrans == NULL) {
    mError("trans:%" PRId32 ", failed to create since %s" , pTrans->id, terrstr());
    return -1;
  }
  mInfo("compact:%d, trans:%d, used to update compact progress.", compactId, pTrans->id);

  SCompactObj   *pCompact = mndAcquireCompact(pMnode, compactId);

  pIter = NULL;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->compactId == compactId) {
      mInfo("compact:%d, trans:%d, check compact progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
            "newNumberFileset:%d, newFinished:%d", 
            pDetail->compactId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
            pDetail->newNumberFileset, pDetail->newFinished);

      pDetail->numberFileset = pDetail->newNumberFileset;
      pDetail->finished = pDetail->newFinished;
   
      SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        mError("compact:%d, trans:%d, failed to append commit log since %s", pDetail->compactId, pTrans->id, terrstr());
        mndTransDrop(pTrans);
        return -1;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  bool allFinished = true;
  while (1) {
    SCompactDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if(pDetail->compactId == compactId){
      mInfo("compact:%d, trans:%d, check compact finished, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", 
        pDetail->compactId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished);

      if(pDetail->numberFileset == -1 && pDetail->finished == -1){
        allFinished = false;
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
      if (pDetail->numberFileset != -1 && pDetail->finished != -1 &&
          pDetail->numberFileset != pDetail->finished) {
        allFinished = false;
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if(allFinished){
    mInfo("compact:%d, all finished", pCompact->compactId);
    while (1) {
      SCompactDetailObj *pDetail = NULL;
      pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT_DETAIL, pIter, (void **)&pDetail);
      if (pIter == NULL) break;

      if (pDetail->compactId == pCompact->compactId) {    
        SSdbRaw *pCommitRaw = mndCompactDetailActionEncode(pDetail);
        if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
          mError("compact:%d, trans:%d, failed to append commit log since %s", pDetail->compactId, pTrans->id, terrstr());
          mndTransDrop(pTrans);
          return -1;
        }
        (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
      }

      sdbRelease(pMnode->pSdb, pDetail);
    }

    SSdbRaw *pCommitRaw = mndCompactActionEncode(pCompact);
    if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
      mError("compact:%d, trans:%d, failed to append commit log since %s", compactId, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      return -1;
    }
    (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("compact:%d, trans:%d, failed to prepare since %s", compactId, pTrans->id, terrstr());
    mndTransDrop(pTrans);
    sdbRelease(pMnode->pSdb, pCompact);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pCompact);
  mndTransDrop(pTrans);
  return 0;
}

void mndCompactPullup(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  SArray *pArray = taosArrayInit(sdbGetSize(pSdb, SDB_COMPACT), sizeof(int32_t));
  if (pArray == NULL) return;

  void *pIter = NULL;
  while (1) {
    SCompactObj *pCompact = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_COMPACT, pIter, (void **)&pCompact);
    if (pIter == NULL) break;
    taosArrayPush(pArray, &pCompact->compactId);
    sdbRelease(pSdb, pCompact);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    mInfo("begin to pull up");
    int32_t *pCompactId = taosArrayGet(pArray, i);
    SCompactObj  *pCompact = mndAcquireCompact(pMnode, *pCompactId);
    if (pCompact != NULL) {
      mInfo("compact:%d, begin to pull up", pCompact->compactId);
      mndCompactSendProgressReq(pMnode, pCompact);
      mndSaveCompactProgress(pMnode, pCompact->compactId);
    }
    mndReleaseCompact(pMnode, pCompact);
  }
  taosArrayDestroy(pArray);
}

static int32_t mndProcessCompactTimer(SRpcMsg *pReq) {
  mTrace("start to process compact timer");
  mndCompactPullup(pReq->info.node);
  return 0;
}
