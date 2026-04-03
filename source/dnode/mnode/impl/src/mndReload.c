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

#include "mndReload.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmsgcb.h"
#include "ttime.h"
#include "tuuid.h"

#define MND_RELOAD_VER_NUMBER 1

static int32_t   mndProcessReloadLastCacheReq(SRpcMsg *pReq);
static int32_t   mndProcessDropReloadReq(SRpcMsg *pReq);
static int32_t   mndRetrieveReload(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static SSdbRaw  *mndReloadActionEncode(SReloadObj *pReload);
static SSdbRow  *mndReloadActionDecode(SSdbRaw *pRaw);
static int32_t   mndReloadActionInsert(SSdb *pSdb, SReloadObj *pReload);
static int32_t   mndReloadActionDelete(SSdb *pSdb, SReloadObj *pReload);
static int32_t   mndReloadActionUpdate(SSdb *pSdb, SReloadObj *pOld, SReloadObj *pNew);

int32_t mndInitReload(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RELOAD, mndRetrieveReload);
  mndSetMsgHandle(pMnode, TDMT_MND_RELOAD_LAST_CACHE,          mndProcessReloadLastCacheReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_RELOAD,                mndProcessDropReloadReq);
  mndSetMsgHandle(pMnode, TDMT_VND_RELOAD_LAST_CACHE_RSP,      mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_CANCEL_LAST_CACHE_RELOAD_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType  = SDB_RELOAD,
      .keyType  = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndReloadActionEncode,
      .decodeFp = (SdbDecodeFp)mndReloadActionDecode,
      .insertFp = (SdbInsertFp)mndReloadActionInsert,
      .updateFp = (SdbUpdateFp)mndReloadActionUpdate,
      .deleteFp = (SdbDeleteFp)mndReloadActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupReload(SMnode *pMnode) { mDebug("mnd reload cleanup"); }

static void tFreeReloadObj(SReloadObj *pReload) {}

static int32_t tSerializeSReloadObj(void *buf, int32_t bufLen, const SReloadObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->reloadUid));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->cacheType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->scopeType));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->tableName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->colName));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->dbUid));
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

static int32_t tDeserializeSReloadObj(void *buf, int32_t bufLen, SReloadObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->reloadUid));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->cacheType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->scopeType));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->tableName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->colName));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static SSdbRaw *mndReloadActionEncode(SReloadObj *pReload) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSReloadObj(NULL, 0, pReload);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_RELOAD, MND_RELOAD_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSReloadObj(buf, tlen, pReload);
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
    mError("reload:%" PRId64 ", failed to encode to raw:%p since %s", pReload->reloadUid, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("reload:%" PRId64 ", encode to raw:%p, row:%p", pReload->reloadUid, pRaw, pReload);
  return pRaw;
}

static SSdbRow *mndReloadActionDecode(SSdbRaw *pRaw) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSdbRow    *pRow = NULL;
  SReloadObj *pReload = NULL;
  void       *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto OVER;

  if (sver != MND_RELOAD_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("reload read invalid ver, data ver: %d, curr ver: %d", sver, MND_RELOAD_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SReloadObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pReload = sdbGetRowObj(pRow);
  if (pReload == NULL) {
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

  if ((terrno = tDeserializeSReloadObj(buf, tlen, pReload)) < 0) goto OVER;

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("reload:%" PRId64 ", failed to decode from raw:%p since %s",
           pReload ? pReload->reloadUid : -1, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("reload:%" PRId64 ", decode from raw:%p, row:%p", pReload->reloadUid, pRaw, pReload);
  return pRow;
}

static int32_t mndReloadActionInsert(SSdb *pSdb, SReloadObj *pReload) {
  mTrace("reload:%" PRId64 ", perform insert action", pReload->reloadUid);
  return 0;
}

static int32_t mndReloadActionDelete(SSdb *pSdb, SReloadObj *pReload) {
  mTrace("reload:%" PRId64 ", perform delete action", pReload->reloadUid);
  tFreeReloadObj(pReload);
  return 0;
}

static int32_t mndReloadActionUpdate(SSdb *pSdb, SReloadObj *pOld, SReloadObj *pNew) {
  mTrace("reload:%" PRId64 ", perform update action, old row:%p new row:%p", pOld->reloadUid, pOld, pNew);
  return 0;
}

static SReloadObj *mndAcquireReload(SMnode *pMnode, int64_t reloadUid) {
  SSdb       *pSdb = pMnode->pSdb;
  SReloadObj *pReload = sdbAcquire(pSdb, SDB_RELOAD, &reloadUid);
  if (pReload == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pReload;
}

static void mndReleaseReload(SMnode *pMnode, SReloadObj *pReload) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pReload);
}


static void *mndBuildReloadLastCacheReq(SMnode *pMnode, SVgObj *pVgroup, int32_t *pContLen,
                                         int64_t reloadUid, int8_t cacheType) {
  SVReloadLastCacheReq req = {
      .reloadUid = reloadUid,
      .dbUid     = pVgroup->dbUid,
      .suid      = 0,
      .uid       = 0,
      .cid       = -1,
      .cacheType = cacheType,
  };

  int32_t contLen = tSerializeSVReloadLastCacheReq(NULL, 0, &req);
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
  pHead->vgId    = htonl(pVgroup->vgId);

  if (tSerializeSVReloadLastCacheReq((char *)pReq + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFreeClear(pReq);
    return NULL;
  }

  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddReloadVgroupAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup,
                                         int64_t reloadUid, int8_t cacheType) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildReloadLastCacheReq(pMnode, pVgroup, &contLen, reloadUid, cacheType);
  if (pReq == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont   = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_RELOAD_LAST_CACHE;

  mTrace("trans:%d, add reload-last-cache action for vgId:%d", pTrans->id, pVgroup->vgId);

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }
  return 0;
}

static int32_t mndAddReloadToTran(SMnode *pMnode, STrans *pTrans, SReloadObj *pReload, SDbObj *pDb,
                                   SMndReloadLastCacheRsp *pRsp) {
  int32_t code = 0;

  pReload->reloadUid = tGenIdPI64();
  pReload->dbUid     = pDb->uid;
  pReload->startTime = taosGetTimestampMs();
  tstrncpy(pReload->dbName, pDb->name, sizeof(pReload->dbName));

  SSdbRaw *pRaw = mndReloadActionEncode(pReload);
  if (pRaw == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    sdbFreeRaw(pRaw);
    TAOS_RETURN(code);
  }
  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_READY)) != 0) {
    TAOS_RETURN(code);
  }


  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SVgObj *pVgroup = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup)) != NULL) {
    if (pVgroup->dbUid == pDb->uid) {
      if ((code = mndAddReloadVgroupAction(pMnode, pTrans, pVgroup, pReload->reloadUid, pReload->cacheType)) != 0) {
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
    }
    sdbRelease(pSdb, pVgroup);
  }

  pRsp->reloadUid = pReload->reloadUid;
  return 0;
}

static int32_t mndRetrieveReload(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SReloadObj *pReload = NULL;
  int32_t     code = 0;
  int32_t     lino = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_RELOAD, pShow->pIter, (void **)&pReload);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    char             tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pReload->reloadUid, false), pReload, &lino,
                        _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    memset(tmpBuf, 0, sizeof(tmpBuf));
    SName name = {0};
    if (tNameFromString(&name, pReload->dbName, T_NAME_ACCT | T_NAME_DB) == 0) {
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pReload->dbName, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pReload, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pReload->cacheType, false), pReload, &lino,
                        _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pReload->scopeType, false), pReload, &lino,
                        _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    memset(tmpBuf, 0, sizeof(tmpBuf));
    tstrncpy(varDataVal(tmpBuf), pReload->tableName, TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pReload, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    memset(tmpBuf, 0, sizeof(tmpBuf));
    tstrncpy(varDataVal(tmpBuf), pReload->colName, TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pReload, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pReload->startTime, false), pReload, &lino,
                        _OVER);

    numOfRows++;
    sdbRelease(pSdb, pReload);
  }

_OVER:
  if (code != 0) {
    mError("failed to retrieve reload at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  pShow->numOfRows += numOfRows;
  return numOfRows;
}


static void *mndBuildCancelReloadReq(SVgObj *pVgroup, int32_t *pContLen, int64_t reloadUid) {
  SVCancelLastCacheReloadReq req = {.reloadUid = reloadUid};

  int32_t contLen = tSerializeSVCancelLastCacheReloadReq(NULL, 0, &req);
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
  pHead->vgId    = htonl(pVgroup->vgId);

  if (tSerializeSVCancelLastCacheReloadReq((char *)pReq + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFreeClear(pReq);
    return NULL;
  }

  *pContLen = contLen;
  return pReq;
}

static int32_t mndDropReload(SMnode *pMnode, SRpcMsg *pReq, SReloadObj *pReload) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "drop-reload");
  if (pTrans == NULL) {
    mError("reload:%" PRId64 ", failed to create drop trans since %s", pReload->reloadUid, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, used to drop reload:%" PRId64, pTrans->id, pReload->reloadUid);

  mndTransSetDbName(pTrans, pReload->dbName, NULL);

  SSdbRaw *pCommitRaw = mndReloadActionEncode(pReload);
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
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SVgObj *pVgroup = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup)) != NULL) {
    if (pVgroup->dbUid == pReload->dbUid) {
      int32_t      contLen = 0;
      void        *pCont = mndBuildCancelReloadReq(pVgroup, &contLen, pReload->reloadUid);
      if (pCont == NULL) {
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        mndTransDrop(pTrans);
        code = TSDB_CODE_OUT_OF_MEMORY;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }

      STransAction action = {0};
      action.epSet   = mndGetVgroupEpset(pMnode, pVgroup);
      action.pCont   = pCont;
      action.contLen = contLen;
      action.msgType = TDMT_VND_CANCEL_LAST_CACHE_RELOAD;

      if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
        taosMemoryFree(pCont);
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }
    }
    sdbRelease(pSdb, pVgroup);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropReloadReq(SRpcMsg *pReq) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SDropReloadReq   dropReq = {0};

  if ((code = tDeserializeSDropReloadReq(pReq->pCont, pReq->contLen, &dropReq)) != 0) {
    TAOS_RETURN(code);
  }

  mInfo("start to drop reload:%" PRId64, dropReq.reloadUid);

  SMnode     *pMnode = pReq->info.node;
  SReloadObj *pReload = mndAcquireReload(pMnode, dropReq.reloadUid);
  if (pReload == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndDropReload(pMnode, pReq, pReload), &lino, _OVER);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  mndReleaseReload(pMnode, pReload);
  TAOS_RETURN(code);
}

static int32_t mndProcessReloadLastCacheReq(SRpcMsg *pReq) {
  int32_t                code = 0;
  int32_t                lino = 0;
  SMnode                *pMnode = pReq->info.node;
  SMndReloadLastCacheReq req = {0};
  SDbObj                *pDb = NULL;
  STrans                *pTrans = NULL;

  if ((code = tDeserializeSMndReloadLastCacheReq(pReq->pCont, pReq->contLen, &req)) != 0) {
    mError("failed to deserialize SMndReloadLastCacheReq, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  mInfo("db:%s, start to reload last cache, cacheType:%d scopeType:%d", req.dbName, req.cacheType, req.scopeType);

  pDb = mndAcquireDb(pMnode, req.dbName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "reload-last-cache");
  if (pTrans == NULL) {
    mError("db:%s, failed to create reload trans since %s", req.dbName, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to reload last cache for db:%s", pTrans->id, req.dbName);

  mndTransSetDbName(pTrans, req.dbName, NULL);

  SReloadObj             reloadObj = {0};
  SMndReloadLastCacheRsp rsp = {0};

  reloadObj.cacheType = req.cacheType;
  reloadObj.scopeType = req.scopeType;
  tstrncpy(reloadObj.tableName, req.tableName, sizeof(reloadObj.tableName));
  tstrncpy(reloadObj.colName,   req.colName,   sizeof(reloadObj.colName));

  TAOS_CHECK_GOTO(mndAddReloadToTran(pMnode, pTrans, &reloadObj, pDb, &rsp), &lino, _OVER);

  // Serialize response and attach to transaction so the framework delivers it to the client
  int32_t rspLen = tSerializeSMndReloadLastCacheRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    code = rspLen;
    goto _OVER;
  }
  void *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  if ((code = tSerializeSMndReloadLastCacheRsp(pRsp, rspLen, &rsp)) < 0) {
    taosMemoryFree(pRsp);
    goto _OVER;
  }
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to reload last cache since %s", req.dbName, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
