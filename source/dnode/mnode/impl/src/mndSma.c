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

#define _DEFAULT_SOURCE
#include "mndSma.h"
#include "functionMgt.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndIndex.h"
#include "mndIndexComm.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define TSDB_SMA_VER_NUMBER   1
#define TSDB_SMA_RESERVE_SIZE 64

static SSdbRaw *mndSmaActionEncode(SSmaObj *pSma);
static SSdbRow *mndSmaActionDecode(SSdbRaw *pRaw);
static int32_t  mndSmaActionInsert(SSdb *pSdb, SSmaObj *pSma);
static int32_t  mndSmaActionDelete(SSdb *pSdb, SSmaObj *pSpSmatb);
static int32_t  mndSmaActionUpdate(SSdb *pSdb, SSmaObj *pOld, SSmaObj *pNew);
static int32_t  mndProcessDropSmaReq(SRpcMsg *pReq);
static int32_t  mndProcessGetSmaReq(SRpcMsg *pReq);
static int32_t  mndProcessGetTbSmaReq(SRpcMsg *pReq);
static int32_t  mndRetrieveSma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

static int32_t mndProcessCreateTSMAReq(SRpcMsg *pReq);
static int32_t mndProcessDropTSMAReq(SRpcMsg *pReq);

// sma and tag index comm func
static int32_t mndProcessDropIdxReq(SRpcMsg *pReq);
static int32_t mndRetrieveIdx(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveIdx(SMnode *pMnode, void *pIter);

static int32_t mndRetrieveTSMA(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTSMA(SMnode *pMnode, void *pIter);
static int32_t mndProcessGetTbTSMAReq(SRpcMsg *pReq);

typedef struct SCreateTSMACxt {
  SMnode        *pMnode;
  const SRpcMsg *pRpcReq;
  union {
    const SMCreateSmaReq *pCreateSmaReq;
    const SMDropSmaReq   *pDropSmaReq;
  };
  SDbObj             *pDb;
  SStbObj            *pSrcStb;
  SSmaObj            *pSma;
  const SSmaObj      *pBaseSma;
  const char         *streamName;
  const char         *targetStbFullName;
  SNodeList          *pProjects;
} SCreateTSMACxt;

int32_t mndInitSma(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_SMA,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndSmaActionEncode,
      .decodeFp = (SdbDecodeFp)mndSmaActionDecode,
      .insertFp = (SdbInsertFp)mndSmaActionInsert,
      .updateFp = (SdbUpdateFp)mndSmaActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSmaActionDelete,
  };

//  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_SMA, mndProcessCreateSmaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_SMA, mndProcessDropIdxReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_INDEX, mndProcessGetSmaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_TABLE_INDEX, mndProcessGetTbSmaReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_INDEX, mndRetrieveIdx);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_INDEX, mndCancelRetrieveIdx);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TSMA, mndProcessCreateTSMAReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TSMA, mndProcessDropTSMAReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_TABLE_TSMA, mndProcessGetTbTSMAReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_TSMA, mndProcessGetTbTSMAReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TSMAS, mndRetrieveTSMA);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TSMAS, mndCancelRetrieveTSMA);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSma(SMnode *pMnode) {}

static SSdbRaw *mndSmaActionEncode(SSmaObj *pSma) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t size =
      sizeof(SSmaObj) + pSma->exprLen + pSma->tagsFilterLen + pSma->sqlLen + pSma->astLen + TSDB_SMA_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_SMA, TSDB_SMA_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pSma->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pSma->stb, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pSma->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pSma->dstTbName, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->uid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->stbUid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->dbUid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->dstTbUid, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->intervalUnit, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->slidingUnit, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->timezone, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->dstVgId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->interval, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->offset, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->sliding, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->exprLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->tagsFilterLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->sqlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->astLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->version, _OVER)

  if (pSma->exprLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->expr, pSma->exprLen, _OVER)
  }
  if (pSma->tagsFilterLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->tagsFilter, pSma->tagsFilterLen, _OVER)
  }
  if (pSma->sqlLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->sql, pSma->sqlLen, _OVER)
  }
  if (pSma->astLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->ast, pSma->astLen, _OVER)
  }
  SDB_SET_BINARY(pRaw, dataPos, pSma->baseSmaName, TSDB_TABLE_FNAME_LEN, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_SMA_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("sma:%s, failed to encode to raw:%p since %s", pSma->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("sma:%s, encode to raw:%p, row:%p", pSma->name, pRaw, pSma);
  return pRaw;
}

static SSdbRow *mndSmaActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SSmaObj *pSma = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_SMA_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SSmaObj));
  if (pRow == NULL) goto _OVER;

  pSma = sdbGetRowObj(pRow);
  if (pSma == NULL) goto _OVER;

  int32_t dataPos = 0;

  SDB_GET_BINARY(pRaw, dataPos, pSma->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pSma->stb, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pSma->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pSma->dstTbName, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->uid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->stbUid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->dbUid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->dstTbUid, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->intervalUnit, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->slidingUnit, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->timezone, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->dstVgId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->interval, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->offset, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->sliding, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->exprLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->tagsFilterLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->sqlLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->astLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->version, _OVER)

  if (pSma->exprLen > 0) {
    pSma->expr = taosMemoryCalloc(pSma->exprLen, 1);
    if (pSma->expr == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->expr, pSma->exprLen, _OVER)
  }

  if (pSma->tagsFilterLen > 0) {
    pSma->tagsFilter = taosMemoryCalloc(pSma->tagsFilterLen, 1);
    if (pSma->tagsFilter == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->tagsFilter, pSma->tagsFilterLen, _OVER)
  }

  if (pSma->sqlLen > 0) {
    pSma->sql = taosMemoryCalloc(pSma->sqlLen, 1);
    if (pSma->sql == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->sql, pSma->sqlLen, _OVER)
  }

  if (pSma->astLen > 0) {
    pSma->ast = taosMemoryCalloc(pSma->astLen, 1);
    if (pSma->ast == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->ast, pSma->astLen, _OVER)
  }
  SDB_GET_BINARY(pRaw, dataPos, pSma->baseSmaName, TSDB_TABLE_FNAME_LEN, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_SMA_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    if (pSma != NULL) {
      mError("sma:%s, failed to decode from raw:%p since %s", pSma->name, pRaw, terrstr());
      taosMemoryFreeClear(pSma->expr);
      taosMemoryFreeClear(pSma->tagsFilter);
      taosMemoryFreeClear(pSma->sql);
      taosMemoryFreeClear(pSma->ast);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("sma:%s, decode from raw:%p, row:%p", pSma->name, pRaw, pSma);
  return pRow;
}

static int32_t mndSmaActionInsert(SSdb *pSdb, SSmaObj *pSma) {
  mTrace("sma:%s, perform insert action, row:%p", pSma->name, pSma);
  return 0;
}

static int32_t mndSmaActionDelete(SSdb *pSdb, SSmaObj *pSma) {
  mTrace("sma:%s, perform delete action, row:%p", pSma->name, pSma);
  taosMemoryFreeClear(pSma->tagsFilter);
  taosMemoryFreeClear(pSma->expr);
  taosMemoryFreeClear(pSma->sql);
  taosMemoryFreeClear(pSma->ast);
  return 0;
}

static int32_t mndSmaActionUpdate(SSdb *pSdb, SSmaObj *pOld, SSmaObj *pNew) {
  mTrace("sma:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  return 0;
}

SSmaObj *mndAcquireSma(SMnode *pMnode, char *smaName) {
  SSdb    *pSdb = pMnode->pSdb;
  SSmaObj *pSma = sdbAcquire(pSdb, SDB_SMA, smaName);
  if (pSma == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_SMA_NOT_EXIST;
  }
  return pSma;
}

void mndReleaseSma(SMnode *pMnode, SSmaObj *pSma) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSma);
}

static int32_t mndSetCreateSmaRedoLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndSmaActionEncode(pSma);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));

  TAOS_RETURN(code);
}

static int32_t mndSetCreateSmaUndoLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndSmaActionEncode(pSma);
  if (!pUndoRaw) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateSmaCommitLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSmaActionEncode(pSma);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetUpdateSmaStbCommitLogs(SMnode *pMnode, STrans *pTrans, SStbObj *pStb) {
  int32_t code = 0;
  SStbObj stbObj = {0};
  taosRLockLatch(&pStb->lock);
  memcpy(&stbObj, pStb, sizeof(SStbObj));
  taosRUnLockLatch(&pStb->lock);
  stbObj.numOfColumns = 0;
  stbObj.pColumns = NULL;
  stbObj.numOfTags = 0;
  stbObj.pTags = NULL;
  stbObj.numOfFuncs = 0;
  stbObj.pFuncs = NULL;
  stbObj.updateTime = taosGetTimestampMs();
  stbObj.lock = 0;
  stbObj.smaVer++;

  SSdbRaw *pCommitRaw = mndStbActionEncode(&stbObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndCheckCreateSmaReq(SMCreateSmaReq *pCreate) {
  int32_t code = TSDB_CODE_MND_INVALID_SMA_OPTION;
  if (pCreate->name[0] == 0) TAOS_RETURN(code);
  if (pCreate->stb[0] == 0) TAOS_RETURN(code);
  if (pCreate->igExists < 0 || pCreate->igExists > 1) TAOS_RETURN(code);
  if (pCreate->intervalUnit < 0) TAOS_RETURN(code);
  if (pCreate->slidingUnit < 0) TAOS_RETURN(code);
  if (pCreate->timezone < 0) TAOS_RETURN(code);
  if (pCreate->interval < 0) TAOS_RETURN(code);
  if (pCreate->offset < 0) TAOS_RETURN(code);
  if (pCreate->sliding < 0) TAOS_RETURN(code);
  if (pCreate->exprLen < 0) TAOS_RETURN(code);
  if (pCreate->tagsFilterLen < 0) TAOS_RETURN(code);
  if (pCreate->sqlLen < 0) TAOS_RETURN(code);
  if (pCreate->astLen < 0) TAOS_RETURN(code);
  if (pCreate->exprLen != 0 && strlen(pCreate->expr) + 1 != pCreate->exprLen) TAOS_RETURN(code);
  if (pCreate->tagsFilterLen != 0 && strlen(pCreate->tagsFilter) + 1 != pCreate->tagsFilterLen) TAOS_RETURN(code);
  if (pCreate->sqlLen != 0 && strlen(pCreate->sql) + 1 != pCreate->sqlLen) TAOS_RETURN(code);
  if (pCreate->astLen != 0 && strlen(pCreate->ast) + 1 != pCreate->astLen) TAOS_RETURN(code);

  SName smaName = {0};
  if (tNameFromString(&smaName, pCreate->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE) < 0) return -1;
  if (*(char *)tNameGetTableName(&smaName) == 0) return -1;

  code = 0;
  TAOS_RETURN(code);
}

static int32_t mndGetStreamNameFromSmaName(char *streamName, char *smaName) {
  SName   n;
  int32_t code = tNameFromString(&n, smaName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  snprintf(streamName, TSDB_TABLE_FNAME_LEN, "%d.%s", n.acctId, n.tname);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndSetDropSmaRedoLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndSmaActionEncode(pSma);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return 0;
}

static int32_t mndSetDropSmaCommitLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSmaActionEncode(pSma);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));

  return 0;
}

static int32_t mndSetDropSmaVgroupRedoLogs(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup) {
  int32_t  code = 0;
  SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pVgRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPING));

  return 0;
}

static int32_t mndSetDropSmaVgroupCommitLogs(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup) {
  int32_t  code = 0;
  SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pVgRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED));

  return 0;
}

static int32_t mndSetDropSmaVgroupRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t    code = 0;
  SVnodeGid *pVgid = pVgroup->vnodeGid + 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildDropVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_VNODE;
  action.acceptableCode = TSDB_CODE_VND_NOT_EXIST;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndDropSma(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SSmaObj *pSma) {
  int32_t     code = -1;
  SVgObj     *pVgroup = NULL;
  SStbObj    *pStb = NULL;
  STrans     *pTrans = NULL;
  SStreamObj *pStream = NULL;

  pVgroup = mndAcquireVgroup(pMnode, pSma->dstVgId);
  if (pVgroup == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, pSma->stb);
  if (pStb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-sma");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to drop sma:%s", pTrans->id, pSma->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);

  char streamName[TSDB_TABLE_FNAME_LEN] = {0};
  code = mndGetStreamNameFromSmaName(streamName, pSma->name);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }

  code = mndAcquireStream(pMnode, streamName, &pStream);
  if (pStream == NULL || pStream->pCreate->streamId != pSma->uid || code != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    goto _OVER;
  } else {
    // drop stream
    if ((code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED)) < 0) {
      mError("stream:%s, failed to drop log since %s", pStream->pCreate->name, tstrerror(code));
      sdbRelease(pMnode->pSdb, pStream);
      goto _OVER;
    }
  }
  TAOS_CHECK_GOTO(mndSetDropSmaRedoLogs(pMnode, pTrans, pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupRedoLogs(pMnode, pTrans, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaCommitLogs(pMnode, pTrans, pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupCommitLogs(pMnode, pTrans, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateSmaStbCommitLogs(pMnode, pTrans, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupRedoActions(pMnode, pTrans, pDb, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  mndReleaseStream(pMnode, pStream);
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseStb(pMnode, pStb);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropSmaReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SSmaObj     *pSma = NULL;
  SMDropSmaReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropSmaReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("sma:%s, start to drop", dropReq.name);

  SSIdx idx = {0};
  if ((code = mndAcquireGlobalIdx(pMnode, dropReq.name, SDB_SMA, &idx)) == 0) {
    pSma = idx.pIdx;
  } else {
    goto _OVER;
  }
  if (pSma == NULL) {
    if (dropReq.igNotExists) {
      mInfo("sma:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto _OVER;
    } else {
      code = TSDB_CODE_MND_SMA_NOT_EXIST;
      goto _OVER;
    }
  }

  SName name = {0};
  code = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb), NULL, _OVER);

  code = mndDropSma(pMnode, pReq, pDb, pSma);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("sma:%s, failed to drop since %s", dropReq.name, tstrerror(code));
  }

  mndReleaseSma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

static int32_t mndGetSma(SMnode *pMnode, SUserIndexReq *indexReq, SUserIndexRsp *rsp, bool *exist) {
  int32_t  code = -1;
  SSmaObj *pSma = NULL;

  SSIdx idx = {0};
  if (0 == mndAcquireGlobalIdx(pMnode, indexReq->indexFName, SDB_SMA, &idx)) {
    pSma = idx.pIdx;
  } else {
    *exist = false;
    return 0;
  }

  if (pSma == NULL) {
    *exist = false;
    return 0;
  }

  memcpy(rsp->dbFName, pSma->db, sizeof(pSma->db));
  memcpy(rsp->tblFName, pSma->stb, sizeof(pSma->stb));
  tstrncpy(rsp->indexType, TSDB_INDEX_TYPE_SMA, TSDB_INDEX_TYPE_LEN);

  SNodeList *pList = NULL;
  int32_t    extOffset = 0;
  code = nodesStringToList(pSma->expr, &pList);
  if (0 == code) {
    SNode *node = NULL;
    FOREACH(node, pList) {
      SFunctionNode *pFunc = (SFunctionNode *)node;
      extOffset += tsnprintf(rsp->indexExts + extOffset, sizeof(rsp->indexExts) - extOffset - 1, "%s%s",
                             (extOffset ? "," : ""), pFunc->functionName);
    }

    *exist = true;
  }

  mndReleaseSma(pMnode, pSma);
  return code;
}

int32_t mndGetTableSma(SMnode *pMnode, char *tbFName, STableIndexRsp *rsp, bool *exist) {
  int32_t         code = 0;
  SSmaObj        *pSma = NULL;
  SSdb           *pSdb = pMnode->pSdb;
  void           *pIter = NULL;
  STableIndexInfo info;

  SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
  if (NULL == pStb) {
    *exist = false;
    return TSDB_CODE_SUCCESS;
  }

  tstrncpy(rsp->dbFName, pStb->db, TSDB_DB_FNAME_LEN);
  tstrncpy(rsp->tbName, pStb->name + strlen(pStb->db) + 1, TSDB_TABLE_NAME_LEN);
  rsp->suid = pStb->uid;
  rsp->version = pStb->smaVer;
  mndReleaseStb(pMnode, pStb);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    if (pSma->stb[0] != tbFName[0] || strcmp(pSma->stb, tbFName)) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    info.intervalUnit = pSma->intervalUnit;
    info.slidingUnit = pSma->slidingUnit;
    info.interval = pSma->interval;
    info.offset = pSma->offset;
    info.sliding = pSma->sliding;
    info.dstTbUid = pSma->dstTbUid;
    info.dstVgId = pSma->dstVgId;

    SVgObj *pVg = mndAcquireVgroup(pMnode, pSma->dstVgId);
    if (pVg == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      return code;
    }
    info.epSet = mndGetVgroupEpset(pMnode, pVg);

    info.expr = taosMemoryMalloc(pSma->exprLen + 1);
    if (info.expr == NULL) {
      code = terrno;
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      return code;
    }

    memcpy(info.expr, pSma->expr, pSma->exprLen);
    info.expr[pSma->exprLen] = 0;

    if (NULL == taosArrayPush(rsp->pIndex, &info)) {
      code = terrno;
      taosMemoryFree(info.expr);
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      return code;
    }

    rsp->indexSize += sizeof(info) + pSma->exprLen + 1;
    *exist = true;

    sdbRelease(pSdb, pSma);
  }

  TAOS_RETURN(code);
}

static int32_t mndProcessGetSmaReq(SRpcMsg *pReq) {
  SUserIndexReq indexReq = {0};
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SUserIndexRsp rsp = {0};
  bool          exist = false;

  TAOS_CHECK_GOTO(tDeserializeSUserIndexReq(pReq->pCont, pReq->contLen, &indexReq), NULL, _OVER);

  code = mndGetSma(pMnode, &indexReq, &rsp, &exist);
  if (code) {
    goto _OVER;
  }

  if (!exist) {
    // TODO GET INDEX FROM FULLTEXT
    code = TSDB_CODE_MND_DB_INDEX_NOT_EXIST;
  } else {
    int32_t contLen = tSerializeSUserIndexRsp(NULL, 0, &rsp);
    void   *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      code = terrno;
      goto _OVER;
    }

    contLen = tSerializeSUserIndexRsp(pRsp, contLen, &rsp);
    if (contLen < 0) {
      code = terrno;
      goto _OVER;
    }

    pReq->info.rsp = pRsp;
    pReq->info.rspLen = contLen;

    code = 0;
  }

_OVER:
  if (code != 0) {
    mError("failed to get index %s since %s", indexReq.indexFName, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t mndProcessGetTbSmaReq(SRpcMsg *pReq) {
  STableIndexReq indexReq = {0};
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  STableIndexRsp rsp = {0};
  bool           exist = false;

  TAOS_CHECK_GOTO(tDeserializeSTableIndexReq(pReq->pCont, pReq->contLen, &indexReq), NULL, _OVER);

  rsp.pIndex = taosArrayInit(10, sizeof(STableIndexInfo));
  if (NULL == rsp.pIndex) {
    code = terrno;
    goto _OVER;
  }

  code = mndGetTableSma(pMnode, indexReq.tbFName, &rsp, &exist);
  if (code) {
    goto _OVER;
  }

  if (!exist) {
    code = TSDB_CODE_MND_DB_INDEX_NOT_EXIST;
  } else {
    int32_t contLen = tSerializeSTableIndexRsp(NULL, 0, &rsp);
    void   *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      code = terrno;
      goto _OVER;
    }

    contLen = tSerializeSTableIndexRsp(pRsp, contLen, &rsp);
    if (contLen < 0) {
      code = terrno;
      goto _OVER;
    }

    pReq->info.rsp = pRsp;
    pReq->info.rspLen = contLen;

    code = 0;
  }

_OVER:
  if (code != 0) {
    mError("failed to get table index %s since %s", indexReq.tbFName, tstrerror(code));
  }

  tFreeSerializeSTableIndexRsp(&rsp);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveSma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SSmaObj *pSma = NULL;
  int32_t  cols = 0;
  int32_t  code = 0;

  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return 0;
  }
  SSmaAndTagIter *pIter = pShow->pIter;
  while (numOfRows < rows) {
    pIter->pSmaIter = sdbFetch(pSdb, SDB_SMA, pIter->pSmaIter, (void **)&pSma);
    if (pIter->pSmaIter == NULL) break;

    if (NULL != pDb && pSma->dbUid != pDb->uid) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    cols = 0;

    SName smaName = {0};
    SName stbName = {0};
    char  n2[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char  n3[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    code = tNameFromString(&smaName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    char n1[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(n1, (char *)tNameGetTableName(&smaName));
      STR_TO_VARSTR(n2, (char *)mndGetDbStr(pSma->db));
      code = tNameFromString(&stbName, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }
    SColumnInfoData *pColInfo = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(n3, (char *)tNameGetTableName(&stbName));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n1, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n2, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n3, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pSma->dstVgId, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pSma->createdTime, false);
    }

    char col[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(col, (char *)"");

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)col, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

      char tag[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(tag, (char *)"sma_index");
      code = colDataSetVal(pColInfo, numOfRows, (const char *)tag, false);
    }

    numOfRows++;
    sdbRelease(pSdb, pSma);
    if (TSDB_CODE_SUCCESS != code) {
      sdbCancelFetch(pMnode->pSdb, pIter->pSmaIter);
      numOfRows = -1;
      break;
    }
  }

  mndReleaseDb(pMnode, pDb);
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

// sma and tag index comm func
static int32_t mndProcessDropIdxReq(SRpcMsg *pReq) {
  int ret = mndProcessDropSmaReq(pReq);
  if (ret == TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST || ret == TSDB_CODE_MND_SMA_NOT_EXIST) {
    terrno = 0;
    ret = mndProcessDropTagIdxReq(pReq);
  }
  return ret;
}

static int32_t mndRetrieveIdx(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  if (pShow->pIter == NULL) {
    pShow->pIter = taosMemoryCalloc(1, sizeof(SSmaAndTagIter));
  }
  if (!pShow->pIter) {
    return terrno;
  }
  int32_t read = mndRetrieveSma(pReq, pShow, pBlock, rows);
  if (read < rows) {
    read += mndRetrieveTagIdx(pReq, pShow, pBlock, rows - read);
  }
  // no more to read
  if (read < rows) {
    taosMemoryFree(pShow->pIter);
    pShow->pIter = NULL;
  }
  return read;
}
static void mndCancelRetrieveIdx(SMnode *pMnode, void *pIter) {
  SSmaAndTagIter *p = pIter;
  if (p != NULL) {
    SSdb *pSdb = pMnode->pSdb;
    sdbCancelFetchByType(pSdb, p->pSmaIter, SDB_SMA);
    sdbCancelFetchByType(pSdb, p->pIdxIter, SDB_IDX);
  }
  taosMemoryFree(p);
}

static void initSMAObj(SCreateTSMACxt *pCxt) {
  memcpy(pCxt->pSma->name, pCxt->pCreateSmaReq->name, TSDB_TABLE_FNAME_LEN);
  memcpy(pCxt->pSma->stb, pCxt->pCreateSmaReq->stb, TSDB_TABLE_FNAME_LEN);
  memcpy(pCxt->pSma->db, pCxt->pDb->name, TSDB_DB_FNAME_LEN);
  if (pCxt->pBaseSma) memcpy(pCxt->pSma->baseSmaName, pCxt->pBaseSma->name, TSDB_TABLE_FNAME_LEN);
  pCxt->pSma->createdTime = taosGetTimestampMs();
  pCxt->pSma->uid = mndGenerateUid(pCxt->pCreateSmaReq->name, TSDB_TABLE_FNAME_LEN);

  memcpy(pCxt->pSma->dstTbName, pCxt->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  pCxt->pSma->dstTbUid = 0;  // not used
  pCxt->pSma->stbUid = pCxt->pSrcStb ? pCxt->pSrcStb->uid : pCxt->pCreateSmaReq->normSourceTbUid;
  pCxt->pSma->dbUid = pCxt->pDb->uid;
  pCxt->pSma->interval = pCxt->pCreateSmaReq->interval;
  pCxt->pSma->intervalUnit = pCxt->pCreateSmaReq->intervalUnit;
  //  pCxt->pSma->timezone = taosGetLocalTimezoneOffset();
  pCxt->pSma->version = 1;

  pCxt->pSma->exprLen = pCxt->pCreateSmaReq->exprLen;
  pCxt->pSma->sqlLen = pCxt->pCreateSmaReq->sqlLen;
  pCxt->pSma->astLen = pCxt->pCreateSmaReq->astLen;
  pCxt->pSma->expr = pCxt->pCreateSmaReq->expr;
  pCxt->pSma->sql = pCxt->pCreateSmaReq->sql;
  pCxt->pSma->ast = pCxt->pCreateSmaReq->ast;
}

static int32_t mndSetUpdateDbTsmaVersionPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOld);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) {
    sdbFreeRaw(pRedoRaw);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY));
}

static int32_t mndSetUpdateDbTsmaVersionCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNew);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
}

static int32_t mndCreateTSMATxnPrepare(SCreateTSMACxt *pCxt) {
  int32_t      code = -1;
  STransAction createStreamRedoAction = {0};
  STransAction createStreamUndoAction = {0};
  STransAction dropStbUndoAction = {0};
  SMDropStbReq dropStbReq = {0};
  STrans *pTrans = mndTransCreate(pCxt->pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_TSMA, pCxt->pRpcReq, "create-tsma");
  if (!pTrans) {
    code = terrno;
    goto _OVER;
  }
  mndTransSetDbName(pTrans, pCxt->pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pCxt->pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to create tsma:%s", pTrans->id, pCxt->pCreateSmaReq->name);

  mndGetMnodeEpSet(pCxt->pMnode, &createStreamRedoAction.epSet);
  createStreamRedoAction.acceptableCode = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
  createStreamRedoAction.msgType = TDMT_MND_CREATE_STREAM;
  createStreamRedoAction.contLen = pCxt->pCreateSmaReq->streamReqLen;
  createStreamRedoAction.pCont = taosMemoryCalloc(1, createStreamRedoAction.contLen);
  memcpy(createStreamRedoAction.pCont, pCxt->pCreateSmaReq->createStreamReq, createStreamRedoAction.contLen);

  createStreamUndoAction.epSet = createStreamRedoAction.epSet;
  createStreamUndoAction.acceptableCode = TSDB_CODE_MND_STREAM_NOT_EXIST;
  createStreamUndoAction.msgType = TDMT_MND_DROP_STREAM;
  createStreamUndoAction.contLen = pCxt->pCreateSmaReq->dropStreamReqLen;
  createStreamUndoAction.pCont = taosMemoryCalloc(1, createStreamUndoAction.contLen);
  memcpy(createStreamUndoAction.pCont, pCxt->pCreateSmaReq->dropStreamReq, createStreamUndoAction.contLen);

  dropStbReq.igNotExists = true;
  tstrncpy(dropStbReq.name, pCxt->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  dropStbUndoAction.epSet = createStreamRedoAction.epSet;
  dropStbUndoAction.acceptableCode = TSDB_CODE_MND_STB_NOT_EXIST;
  dropStbUndoAction.retryCode = TSDB_CODE_MND_STREAM_MUST_BE_DELETED;
  dropStbUndoAction.msgType = TDMT_MND_STB_DROP;
  dropStbUndoAction.contLen = tSerializeSMDropStbReq(0, 0, &dropStbReq);
  dropStbUndoAction.pCont = taosMemoryCalloc(1, dropStbUndoAction.contLen);
  if (!dropStbUndoAction.pCont) {
    code = terrno;
    goto _OVER;
  }
  if (dropStbUndoAction.contLen !=
      tSerializeSMDropStbReq(dropStbUndoAction.pCont, dropStbUndoAction.contLen, &dropStbReq)) {
    mError("sma: %s, failed to create due to drop stb req encode failure", pCxt->pCreateSmaReq->name);
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  SDbObj newDb = {0};
  memcpy(&newDb, pCxt->pDb, sizeof(SDbObj));
  newDb.tsmaVersion++;
  TAOS_CHECK_GOTO(mndSetCreateSmaRedoLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSmaUndoLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSmaCommitLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendRedoAction(pTrans, &createStreamRedoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendUndoAction(pTrans, &createStreamUndoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendUndoAction(pTrans, &dropStbUndoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionPrepareLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionCommitLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pCxt->pMnode, pTrans), NULL, _OVER);

  code = TSDB_CODE_SUCCESS;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndCreateTSMA(SCreateTSMACxt *pCxt) {
  int32_t            code = 0;
  SSmaObj            sma = {0};

  pCxt->pSma = &sma;
  initSMAObj(pCxt);

  SNodeList *pProjects = NULL;
  code = nodesStringToList(pCxt->pCreateSmaReq->expr, &pProjects);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  pCxt->pProjects = pProjects;


  if (TSDB_CODE_SUCCESS != (code = mndCreateTSMATxnPrepare(pCxt))) {
    goto _OVER;
  } else {
    mInfo("sma:%s, uid:%" PRIi64 " create on stb:%" PRIi64 " dstTb:%s dstVg:%d", pCxt->pCreateSmaReq->name, sma.uid,
          sma.stbUid, sma.dstTbName, sma.dstVgId);
    code = 0;
  }

_OVER:
  if (pProjects) nodesDestroyList(pProjects);
  pCxt->pProjects = NULL;
  TAOS_RETURN(code);
}

static int32_t mndTSMAGenerateOutputName(const char *tsmaName, char *streamName, char *targetStbName) {
  SName   smaName;
  int32_t code = tNameFromString(&smaName, tsmaName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  snprintf(streamName, TSDB_TABLE_FNAME_LEN, "%d.%s", smaName.acctId, smaName.tname);
  snprintf(targetStbName, TSDB_TABLE_FNAME_LEN, "%s" TSMA_RES_STB_POSTFIX, tsmaName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessCreateTSMAReq(SRpcMsg *pReq) {
#ifdef WINDOWS
  TAOS_RETURN(TSDB_CODE_MND_INVALID_PLATFORM);
#endif
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SDbObj        *pDb = NULL;
  SStbObj       *pStb = NULL;
  SSmaObj       *pSma = NULL;
  SSmaObj       *pBaseTsma = NULL;
  SStreamObj    *pStream = NULL;
  int64_t        mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMCreateSmaReq createReq = {0};

  if (sdbGetSize(pMnode->pSdb, SDB_SMA) >= tsMaxTsmaNum) {
    code = TSDB_CODE_MND_MAX_TSMA_NUM_EXCEEDED;
    goto _OVER;
  }

  if (tDeserializeSMCreateSmaReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("start to create tsma: %s", createReq.name);
  if ((code = mndCheckCreateSmaReq(&createReq)) != 0) goto _OVER;

  if (createReq.normSourceTbUid == 0) {
    pStb = mndAcquireStb(pMnode, createReq.stb);
    if (!pStb && !createReq.recursiveTsma) {
      mError("tsma:%s, failed to create since stb:%s not exist", createReq.name, createReq.stb);
      code = TSDB_CODE_MND_STB_NOT_EXIST;
      goto _OVER;
    }
  }

  char streamName[TSDB_TABLE_FNAME_LEN] = {0};
  char streamTargetStbFullName[TSDB_TABLE_FNAME_LEN] = {0};
  code = mndTSMAGenerateOutputName(createReq.name, streamName, streamTargetStbFullName);
  if (TSDB_CODE_SUCCESS != code) {
    mInfo("tsma:%s, faield to generate name", createReq.name);
    goto _OVER;
  }

  pSma = sdbAcquire(pMnode->pSdb, SDB_SMA, createReq.name);
  if (pSma && createReq.igExists) {
    mInfo("tsma:%s, already exists in sma:%s, ignore exist is set", createReq.name, pSma->name);
    code = 0;
    goto _OVER;
  }

  if (pSma) {
    code = TSDB_CODE_MND_SMA_ALREADY_EXIST;
    goto _OVER;
  }

  SStbObj *pTargetStb = mndAcquireStb(pMnode, streamTargetStbFullName);
  if (pTargetStb) {
    code = TSDB_CODE_TDB_STB_ALREADY_EXIST;
    mError("tsma: %s, failed to create since output stable already exists: %s", createReq.name,
           streamTargetStbFullName);
    goto _OVER;
  }

  code = mndAcquireStream(pMnode, streamName, &pStream);
  if (pStream != NULL || code != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    mError("tsma:%s, failed to create since stream:%s already exist", createReq.name, streamName);
    code = TSDB_CODE_MND_SMA_ALREADY_EXIST;
    goto _OVER;
  }

  SName name = {0};
  code = tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb), NULL, _OVER);

  if (createReq.recursiveTsma) {
    pBaseTsma = sdbAcquire(pMnode->pSdb, SDB_SMA, createReq.baseTsmaName);
    if (!pBaseTsma) {
      mError("base tsma: %s not found when creating recursive tsma", createReq.baseTsmaName);
      code = TSDB_CODE_MND_SMA_NOT_EXIST;
      goto _OVER;
    }
    if (!pStb) {
      createReq.normSourceTbUid = pBaseTsma->stbUid;
    }
  }

  SCreateTSMACxt cxt = {
      .pMnode = pMnode,
      .pCreateSmaReq = &createReq,
      .streamName = streamName,
      .targetStbFullName = streamTargetStbFullName,
      .pDb = pDb,
      .pRpcReq = pReq,
      .pSma = NULL,
      .pBaseSma = pBaseTsma,
      .pSrcStb = pStb,
  };

  code = mndCreateTSMA(&cxt);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("tsma:%s, failed to create since %s", createReq.name, tstrerror(code));
  }

  if (pStb) mndReleaseStb(pMnode, pStb);
  if (pBaseTsma) mndReleaseSma(pMnode, pBaseTsma);
  mndReleaseSma(pMnode, pSma);
  mndReleaseStream(pMnode, pStream);
  mndReleaseDb(pMnode, pDb);
  tFreeSMCreateSmaReq(&createReq);

  TAOS_RETURN(code);
}

static int32_t mndDropTSMA(SCreateTSMACxt *pCxt) {
  int32_t      code = -1;
  STransAction dropStreamRedoAction = {0};
  STrans      *pTrans = mndTransCreate(pCxt->pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_TSMA, pCxt->pRpcReq, "drop-tsma");
  if (!pTrans) {
    code = terrno;
    goto _OVER;
  }
  mndTransSetDbName(pTrans, pCxt->pDb->name, NULL);
  if (mndTransCheckConflict(pCxt->pMnode, pTrans) != 0) goto _OVER;
  mndTransSetSerial(pTrans);
  mndGetMnodeEpSet(pCxt->pMnode, &dropStreamRedoAction.epSet);
  dropStreamRedoAction.acceptableCode = TSDB_CODE_MND_STREAM_NOT_EXIST;
  dropStreamRedoAction.msgType = TDMT_MND_DROP_STREAM;
  dropStreamRedoAction.contLen = pCxt->pDropSmaReq->dropStreamReqLen;
  dropStreamRedoAction.pCont = taosMemoryCalloc(1, dropStreamRedoAction.contLen);
  memcpy(dropStreamRedoAction.pCont, pCxt->pDropSmaReq->dropStreamReq, dropStreamRedoAction.contLen);

  // output stable is not dropped when dropping stream, dropping it when dropping tsma
  SMDropStbReq dropStbReq = {0};
  dropStbReq.igNotExists = false;
  tstrncpy(dropStbReq.name, pCxt->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  dropStbReq.sql = "drop";
  dropStbReq.sqlLen = 5;

  STransAction dropStbRedoAction = {0};
  mndGetMnodeEpSet(pCxt->pMnode, &dropStbRedoAction.epSet);
  dropStbRedoAction.acceptableCode = TSDB_CODE_MND_STB_NOT_EXIST;
  dropStbRedoAction.msgType = TDMT_MND_STB_DROP;
  dropStbRedoAction.contLen = tSerializeSMDropStbReq(0, 0, &dropStbReq);
  dropStbRedoAction.pCont = taosMemoryCalloc(1, dropStbRedoAction.contLen);
  if (!dropStbRedoAction.pCont) {
    code = terrno;
    goto _OVER;
  }
  if (dropStbRedoAction.contLen !=
      tSerializeSMDropStbReq(dropStbRedoAction.pCont, dropStbRedoAction.contLen, &dropStbReq)) {
    mError("tsma: %s, failedto drop due to drop stb req encode failure", pCxt->pDropSmaReq->name);
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  SDbObj newDb = {0};
  memcpy(&newDb, pCxt->pDb, sizeof(SDbObj));
  newDb.tsmaVersion++;
  TAOS_CHECK_GOTO(mndSetDropSmaRedoLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaCommitLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendRedoAction(pTrans, &dropStreamRedoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendRedoAction(pTrans, &dropStbRedoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionPrepareLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionCommitLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pCxt->pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_SUCCESS;
_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static bool hasRecursiveTsmasBasedOnMe(SMnode *pMnode, const SSmaObj *pSma) {
  SSmaObj *pSmaObj = NULL;
  void    *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSmaObj);
    if (pIter == NULL) break;
    if (0 == strncmp(pSmaObj->baseSmaName, pSma->name, TSDB_TABLE_FNAME_LEN)) {
      sdbRelease(pMnode->pSdb, pSmaObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      return true;
    }
    sdbRelease(pMnode->pSdb, pSmaObj);
  }
  return false;
}

static int32_t mndProcessDropTSMAReq(SRpcMsg *pReq) {
  int32_t      code = -1;
  SMDropSmaReq dropReq = {0};
  SSmaObj     *pSma = NULL;
  SDbObj      *pDb = NULL;
  SMnode      *pMnode = pReq->info.node;
  SStbObj     *pStb = NULL;
  if (tDeserializeSMDropSmaReq(pReq->pCont, pReq->contLen, &dropReq) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  char streamName[TSDB_TABLE_FNAME_LEN] = {0};
  char streamTargetStbFullName[TSDB_TABLE_FNAME_LEN] = {0};
  code = mndTSMAGenerateOutputName(dropReq.name, streamName, streamTargetStbFullName);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, streamTargetStbFullName);

  pSma = mndAcquireSma(pMnode, dropReq.name);
  if (!pSma && dropReq.igNotExists) {
    code = 0;
    goto _OVER;
  }
  if (!pSma) {
    code = TSDB_CODE_MND_SMA_NOT_EXIST;
    goto _OVER;
  }
  SName name = {0};
  code = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (!pDb) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb)) != 0) {
    goto _OVER;
  }

  if (hasRecursiveTsmasBasedOnMe(pMnode, pSma)) {
    code = TSDB_CODE_MND_INVALID_DROP_TSMA;
    goto _OVER;
  }

  SCreateTSMACxt cxt = {
      .pDb = pDb,
      .pMnode = pMnode,
      .pRpcReq = pReq,
      .pSma = pSma,
      .streamName = streamName,
      .targetStbFullName = streamTargetStbFullName,
      .pDropSmaReq = &dropReq,
  };

  code = mndDropTSMA(&cxt);

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_OVER:

  mndReleaseStb(pMnode, pStb);
  mndReleaseSma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveTSMA(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SDbObj          *pDb = NULL;
  int32_t          numOfRows = 0;
  SSmaObj         *pSma = NULL;
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = 0;
  SColumnInfoData *pColInfo;
  if (pShow->pIter == NULL) {
    pShow->pIter = taosMemoryCalloc(1, sizeof(SSmaAndTagIter));
  }
  if (!pShow->pIter) {
    return terrno;
  }
  if (pShow->db[0]) {
    pDb = mndAcquireDb(pMnode, pShow->db);
  }
  SSmaAndTagIter *pIter = pShow->pIter;
  while (numOfRows < rows) {
    pIter->pSmaIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter->pSmaIter, (void **)&pSma);
    if (pIter->pSmaIter == NULL) break;
    SDbObj *pSrcDb = mndAcquireDb(pMnode, pSma->db);

    if ((pDb && pSma->dbUid != pDb->uid) || !pSrcDb) {
      sdbRelease(pMnode->pSdb, pSma);
      if (pSrcDb) mndReleaseDb(pMnode, pSrcDb);
      continue;
    }

    int32_t cols = 0;
    SName   n = {0};

    code = tNameFromString(&n, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    char smaName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(smaName, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)smaName, false);
    }

    char db[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(db, (char *)mndGetDbStr(pSma->db));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)db, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = tNameFromString(&n, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }
    char srcTb[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(srcTb, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)srcTb, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)db, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = tNameFromString(&n, pSma->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }

    if (TSDB_CODE_SUCCESS == code) {
      char targetTb[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(targetTb, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)targetTb, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      // stream name
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)smaName, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)(&pSma->createdTime), false);
    }

    // interval
    char    interval[64 + VARSTR_HEADER_SIZE] = {0};
    int32_t len = 0;
    if (TSDB_CODE_SUCCESS == code) {
      if (!IS_CALENDAR_TIME_DURATION(pSma->intervalUnit)) {
        len = tsnprintf(interval + VARSTR_HEADER_SIZE, 64, "%" PRId64 "%c", pSma->interval,
                        getPrecisionUnit(pSrcDb->cfg.precision));
      } else {
        len = tsnprintf(interval + VARSTR_HEADER_SIZE, 64, "%" PRId64 "%c", pSma->interval, pSma->intervalUnit);
      }
      varDataSetLen(interval, len);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, interval, false);
    }

    char buf[TSDB_MAX_SAVED_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      // create sql
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      len = tsnprintf(buf + VARSTR_HEADER_SIZE, TSDB_MAX_SAVED_SQL_LEN, "%s", pSma->sql);
      varDataSetLen(buf, TMIN(len, TSDB_MAX_SAVED_SQL_LEN));
      code = colDataSetVal(pColInfo, numOfRows, buf, false);
    }

    // func list
    len = 0;
    SNode *pNode = NULL, *pFunc = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesStringToNode(pSma->ast, &pNode);
    }
    if (TSDB_CODE_SUCCESS == code) {
      char *start = buf + VARSTR_HEADER_SIZE;
      FOREACH(pFunc, ((SSelectStmt *)pNode)->pProjectionList) {
        if (nodeType(pFunc) == QUERY_NODE_FUNCTION) {
          SFunctionNode *pFuncNode = (SFunctionNode *)pFunc;
          if (!fmIsTSMASupportedFunc(pFuncNode->funcId)) continue;
          len += tsnprintf(start, TSDB_MAX_SAVED_SQL_LEN - len, "%s%s", start != buf + VARSTR_HEADER_SIZE ? "," : "",
                           ((SExprNode *)pFunc)->userAlias);
          if (len >= TSDB_MAX_SAVED_SQL_LEN) {
            len = TSDB_MAX_SAVED_SQL_LEN;
            break;
          }
          start = buf + VARSTR_HEADER_SIZE + len;
        }
      }
      nodesDestroyNode(pNode);
    }

    if (TSDB_CODE_SUCCESS == code) {
      varDataSetLen(buf, len);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, buf, false);
    }

    numOfRows++;
    mndReleaseSma(pMnode, pSma);
    mndReleaseDb(pMnode, pSrcDb);
    if (TSDB_CODE_SUCCESS != code) {
      sdbCancelFetch(pMnode->pSdb, pIter->pSmaIter);
      numOfRows = code;
      break;
    }
  }
  mndReleaseDb(pMnode, pDb);
  pShow->numOfRows += numOfRows;
  if (numOfRows < rows) {
    taosMemoryFree(pShow->pIter);
    pShow->pIter = NULL;
  }
  return numOfRows;
}

static void mndCancelRetrieveTSMA(SMnode *pMnode, void *pIter) {
  SSmaAndTagIter *p = pIter;
  if (p != NULL) {
    SSdb *pSdb = pMnode->pSdb;
    sdbCancelFetchByType(pSdb, p->pSmaIter, SDB_SMA);
  }
  taosMemoryFree(p);
}

int32_t dumpTSMAInfoFromSmaObj(const SSmaObj *pSma, const SStbObj *pDestStb, STableTSMAInfo *pInfo,
                               const SSmaObj *pBaseTsma) {
  int32_t code = 0;
  pInfo->interval = pSma->interval;
  pInfo->unit = pSma->intervalUnit;
  pInfo->tsmaId = pSma->uid;
  pInfo->version = pSma->version;
  pInfo->tsmaId = pSma->uid;
  pInfo->destTbUid = pDestStb->uid;
  SName sName = {0};
  code = tNameFromString(&sName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->name, sName.tname, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->targetDbFName, pSma->db, TSDB_DB_FNAME_LEN);
  code = tNameFromString(&sName, pSma->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->targetTb, sName.tname, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->dbFName, pSma->db, TSDB_DB_FNAME_LEN);
  code = tNameFromString(&sName, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->tb, sName.tname, TSDB_TABLE_NAME_LEN);
  pInfo->pFuncs = taosArrayInit(8, sizeof(STableTSMAFuncInfo));
  if (!pInfo->pFuncs) return TSDB_CODE_OUT_OF_MEMORY;

  SNode *pNode, *pFunc;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pBaseTsma ? pBaseTsma->ast : pSma->ast, &pNode)) {
    taosArrayDestroy(pInfo->pFuncs);
    pInfo->pFuncs = NULL;
    return TSDB_CODE_TSMA_INVALID_STAT;
  }
  if (pNode) {
    SSelectStmt *pSelect = (SSelectStmt *)pNode;
    FOREACH(pFunc, pSelect->pProjectionList) {
      STableTSMAFuncInfo funcInfo = {0};
      SFunctionNode     *pFuncNode = (SFunctionNode *)pFunc;
      if (!fmIsTSMASupportedFunc(pFuncNode->funcId)) continue;
      funcInfo.funcId = pFuncNode->funcId;
      funcInfo.colId = ((SColumnNode *)pFuncNode->pParameterList->pHead->pNode)->colId;
      if (!taosArrayPush(pInfo->pFuncs, &funcInfo)) {
        code = terrno;
        taosArrayDestroy(pInfo->pFuncs);
        nodesDestroyNode(pNode);
        return code;
      }
    }
    nodesDestroyNode(pNode);
  }
  pInfo->ast = taosStrdup(pSma->ast);
  if (!pInfo->ast) code = terrno;

  if (code == TSDB_CODE_SUCCESS && pDestStb->numOfTags > 0) {
    pInfo->pTags = taosArrayInit(pDestStb->numOfTags, sizeof(SSchema));
    if (!pInfo->pTags) {
      code = terrno;
    } else {
      for (int32_t i = 0; i < pDestStb->numOfTags; ++i) {
        if (NULL == taosArrayPush(pInfo->pTags, &pDestStb->pTags[i])) {
          code = terrno;
          break;
        }
      }
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    pInfo->pUsedCols = taosArrayInit(pDestStb->numOfColumns - 3, sizeof(SSchema));
    if (!pInfo->pUsedCols)
      code = terrno;
    else {
      // skip _wstart, _wend, _duration
      for (int32_t i = 1; i < pDestStb->numOfColumns - 2; ++i) {
        if (NULL == taosArrayPush(pInfo->pUsedCols, &pDestStb->pColumns[i])) {
          code = terrno;
          break;
        }
      }
    }
  }
  TAOS_RETURN(code);
}

// @note remember to mndReleaseSma(*ppOut)
static int32_t mndGetDeepestBaseForTsma(SMnode *pMnode, SSmaObj *pSma, SSmaObj **ppOut) {
  int32_t  code = 0;
  SSmaObj *pRecursiveTsma = NULL;
  if (pSma->baseSmaName[0]) {
    pRecursiveTsma = mndAcquireSma(pMnode, pSma->baseSmaName);
    if (!pRecursiveTsma) {
      mError("base tsma: %s for tsma: %s not found", pSma->baseSmaName, pSma->name);
      return TSDB_CODE_MND_SMA_NOT_EXIST;
    }
    while (pRecursiveTsma->baseSmaName[0]) {
      SSmaObj *pTmpSma = pRecursiveTsma;
      pRecursiveTsma = mndAcquireSma(pMnode, pTmpSma->baseSmaName);
      if (!pRecursiveTsma) {
        mError("base tsma: %s for tsma: %s not found", pTmpSma->baseSmaName, pTmpSma->name);
        mndReleaseSma(pMnode, pTmpSma);
        return TSDB_CODE_MND_SMA_NOT_EXIST;
      }
      mndReleaseSma(pMnode, pTmpSma);
    }
  }
  *ppOut = pRecursiveTsma;
  return code;
}

static int32_t mndGetTSMA(SMnode *pMnode, char *tsmaFName, STableTSMAInfoRsp *rsp, bool *exist) {
  int32_t  code = -1;
  SSmaObj *pSma = NULL;
  SSmaObj *pBaseTsma = NULL;
  SStbObj *pDstStb = NULL;

  pSma = sdbAcquire(pMnode->pSdb, SDB_SMA, tsmaFName);
  if (pSma) {
    pDstStb = mndAcquireStb(pMnode, pSma->dstTbName);
    if (!pDstStb) {
      sdbRelease(pMnode->pSdb, pSma);
      return TSDB_CODE_SUCCESS;
    }

    STableTSMAInfo *pTsma = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pTsma) {
      code = terrno;
      sdbRelease(pMnode->pSdb, pSma);
      mndReleaseStb(pMnode, pDstStb);
      TAOS_RETURN(code);
    }

    code = mndGetDeepestBaseForTsma(pMnode, pSma, &pBaseTsma);
    if (code == 0) {
      code = dumpTSMAInfoFromSmaObj(pSma, pDstStb, pTsma, pBaseTsma);
    }
    mndReleaseStb(pMnode, pDstStb);
    sdbRelease(pMnode->pSdb, pSma);
    if (pBaseTsma) mndReleaseSma(pMnode, pBaseTsma);
    if (terrno) {
      tFreeAndClearTableTSMAInfo(pTsma);
      TAOS_RETURN(code);
    }
    if (NULL == taosArrayPush(rsp->pTsmas, &pTsma)) {
      code = terrno;
      tFreeAndClearTableTSMAInfo(pTsma);
    }
    *exist = true;
  }
  TAOS_RETURN(code);
}

typedef bool (*tsmaFilter)(const SSmaObj *pSma, void *param);

static int32_t mndGetSomeTsmas(SMnode *pMnode, STableTSMAInfoRsp *pRsp, tsmaFilter filtered, void *param, bool *exist) {
  int32_t     code = 0;
  SSmaObj    *pSma = NULL;
  SSmaObj    *pBaseTsma = NULL;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SStreamObj *pStream = NULL;
  SStbObj    *pStb = NULL;
  bool        shouldRetry = false;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    if (filtered(pSma, param)) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    pStb = mndAcquireStb(pMnode, pSma->dstTbName);
    if (!pStb) {
      sdbRelease(pSdb, pSma);
      shouldRetry = true;
      continue;
    }

    SName smaName;
    char  streamName[TSDB_TABLE_FNAME_LEN] = {0};
    code = tNameFromString(&smaName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (TSDB_CODE_SUCCESS != code) {
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      TAOS_RETURN(code);
    }
    snprintf(streamName, TSDB_TABLE_FNAME_LEN, "%d.%s.%s", smaName.acctId, smaName.dbname, smaName.tname);
    pStream = NULL;

    code = mndAcquireStream(pMnode, streamName, &pStream);
    if (!pStream) {
      shouldRetry = true;
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      continue;
    }
    if (code != 0) {
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      TAOS_RETURN(code);
    }

    int64_t streamId = pStream->pCreate->streamId;
    mndReleaseStream(pMnode, pStream);

    STableTSMAInfo *pTsma = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pTsma) {
      code = terrno;
      mndReleaseStb(pMnode, pStb);
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    pTsma->streamAddr = taosMemoryCalloc(1, sizeof(SStreamTaskAddr));
    code = msmGetTriggerTaskAddr(pMnode, streamId, pTsma->streamAddr);
    if (code != 0) {
      shouldRetry = true;
      mndReleaseStb(pMnode, pStb);
      sdbRelease(pSdb, pSma);
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    pTsma->streamUid = streamId;

    code = mndGetDeepestBaseForTsma(pMnode, pSma, &pBaseTsma);
    if (code == 0) {
      code = dumpTSMAInfoFromSmaObj(pSma, pStb, pTsma, pBaseTsma);
    }
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pSdb, pSma);
    if (pBaseTsma) mndReleaseSma(pMnode, pBaseTsma);
    if (terrno) {
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    if (NULL == taosArrayPush(pRsp->pTsmas, &pTsma)) {
      code = terrno;
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    *exist = true;
  }
  if (shouldRetry) {
    return TSDB_CODE_NEED_RETRY;
  }
  return TSDB_CODE_SUCCESS;
}

static bool tsmaTbFilter(const SSmaObj *pSma, void *param) {
  const char *tbFName = param;
  return pSma->stb[0] != tbFName[0] || strcmp(pSma->stb, tbFName) != 0;
}

static int32_t mndGetTableTSMA(SMnode *pMnode, char *tbFName, STableTSMAInfoRsp *pRsp, bool *exist) {
  return mndGetSomeTsmas(pMnode, pRsp, tsmaTbFilter, tbFName, exist);
}

static bool tsmaDbFilter(const SSmaObj *pSma, void *param) {
  uint64_t *dbUid = param;
  return pSma->dbUid != *dbUid;
}

int32_t mndGetDbTsmas(SMnode *pMnode, const char *dbFName, uint64_t dbUid, STableTSMAInfoRsp *pRsp, bool *exist) {
  return mndGetSomeTsmas(pMnode, pRsp, tsmaDbFilter, &dbUid, exist);
}

static int32_t mndProcessGetTbTSMAReq(SRpcMsg *pReq) {
  STableTSMAInfoRsp rsp = {0};
  int32_t           code = -1;
  STableTSMAInfoReq tsmaReq = {0};
  bool              exist = false;
  SMnode           *pMnode = pReq->info.node;

  TAOS_CHECK_GOTO(tDeserializeTableTSMAInfoReq(pReq->pCont, pReq->contLen, &tsmaReq), NULL, _OVER);

  rsp.pTsmas = taosArrayInit(4, POINTER_BYTES);
  if (NULL == rsp.pTsmas) {
    code = terrno;
    goto _OVER;
  }

  if (tsmaReq.fetchingWithTsmaName) {
    code = mndGetTSMA(pMnode, tsmaReq.name, &rsp, &exist);
  } else {
    code = mndGetTableTSMA(pMnode, tsmaReq.name, &rsp, &exist);
    if (TSDB_CODE_NEED_RETRY == code) {
      code = TSDB_CODE_SUCCESS;
    }
  }
  if (code) {
    goto _OVER;
  }

  if (!exist) {
    code = TSDB_CODE_MND_SMA_NOT_EXIST;
  } else {
    int32_t contLen = tSerializeTableTSMAInfoRsp(NULL, 0, &rsp);
    void   *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      code = terrno;
      goto _OVER;
    }

    int32_t len = tSerializeTableTSMAInfoRsp(pRsp, contLen, &rsp);
    if (len < 0) {
      code = terrno;
      goto _OVER;
    }

    pReq->info.rsp = pRsp;
    pReq->info.rspLen = contLen;

    code = 0;
  }

_OVER:
  tFreeTableTSMAInfoRsp(&rsp);
  TAOS_RETURN(code);
}

static int32_t mkNonExistTSMAInfo(const STSMAVersion *pTsmaVer, STableTSMAInfo **ppTsma) {
  STableTSMAInfo *pInfo = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
  if (!pInfo) {
    return terrno;
  }
  pInfo->pFuncs = NULL;
  pInfo->tsmaId = pTsmaVer->tsmaId;
  tstrncpy(pInfo->dbFName, pTsmaVer->dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(pInfo->tb, pTsmaVer->tbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->name, pTsmaVer->name, TSDB_TABLE_NAME_LEN);
  pInfo->dbId = pTsmaVer->dbId;
  pInfo->ast = taosMemoryCalloc(1, 1);
  if (!pInfo->ast) {
    taosMemoryFree(pInfo);
    return terrno;
  }
  *ppTsma = pInfo;
  return TSDB_CODE_SUCCESS;
}

int32_t mndValidateTSMAInfo(SMnode *pMnode, STSMAVersion *pTsmaVersions, int32_t numOfTsmas, void **ppRsp,
                            int32_t *pRspLen) {
  int32_t         code = -1;
  STSMAHbRsp      hbRsp = {0};
  int32_t         rspLen = 0;
  void           *pRsp = NULL;
  char            tsmaFName[TSDB_TABLE_FNAME_LEN] = {0};
  STableTSMAInfo *pTsmaInfo = NULL;

  hbRsp.pTsmas = taosArrayInit(numOfTsmas, POINTER_BYTES);
  if (!hbRsp.pTsmas) {
    code = terrno;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < numOfTsmas; ++i) {
    STSMAVersion *pTsmaVer = &pTsmaVersions[i];
    pTsmaVer->dbId = be64toh(pTsmaVer->dbId);
    pTsmaVer->tsmaId = be64toh(pTsmaVer->tsmaId);
    pTsmaVer->version = ntohl(pTsmaVer->version);

    snprintf(tsmaFName, sizeof(tsmaFName), "%s.%s", pTsmaVer->dbFName, pTsmaVer->name);
    SSmaObj *pSma = mndAcquireSma(pMnode, tsmaFName);
    if (!pSma) {
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    }

    if (pSma->uid != pTsmaVer->tsmaId) {
      mDebug("tsma: %s.%" PRIx64 " tsmaId mismatch with current %" PRIx64, tsmaFName, pTsmaVer->tsmaId, pSma->uid);
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      mndReleaseSma(pMnode, pSma);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    } else if (pSma->version == pTsmaVer->version) {
      mndReleaseSma(pMnode, pSma);
      continue;
    }

    SStbObj *pDestStb = mndAcquireStb(pMnode, pSma->dstTbName);
    if (!pDestStb) {
      mInfo("tsma: %s.%" PRIx64 " dest stb: %s not found, maybe dropped", tsmaFName, pTsmaVer->tsmaId, pSma->dstTbName);
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      mndReleaseSma(pMnode, pSma);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    }

    // dump smaObj into rsp
    STableTSMAInfo *pInfo = NULL;
    pInfo = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pInfo) {
      code = terrno;
      mndReleaseSma(pMnode, pSma);
      mndReleaseStb(pMnode, pDestStb);
      goto _OVER;
    }

    SSmaObj *pBaseSma = NULL;
    code = mndGetDeepestBaseForTsma(pMnode, pSma, &pBaseSma);
    if (code == 0) code = dumpTSMAInfoFromSmaObj(pSma, pDestStb, pInfo, pBaseSma);

    mndReleaseStb(pMnode, pDestStb);
    mndReleaseSma(pMnode, pSma);
    if (pBaseSma) mndReleaseSma(pMnode, pBaseSma);
    if (terrno) {
      tFreeAndClearTableTSMAInfo(pInfo);
      goto _OVER;
    }

    if (NULL == taosArrayPush(hbRsp.pTsmas, pInfo)) {
      code = terrno;
      tFreeAndClearTableTSMAInfo(pInfo);
      goto _OVER;
    }
  }

  rspLen = tSerializeTSMAHbRsp(NULL, 0, &hbRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _OVER;
  }

  pRsp = taosMemoryMalloc(rspLen);
  if (!pRsp) {
    code = terrno;
    rspLen = 0;
    goto _OVER;
  }

  rspLen = tSerializeTSMAHbRsp(pRsp, rspLen, &hbRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _OVER;
  }
  code = 0;
_OVER:
  tFreeTSMAHbRsp(&hbRsp);
  *ppRsp = pRsp;
  *pRspLen = rspLen;
  TAOS_RETURN(code);
}

int32_t mndDropTSMAsByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SSmaObj *pSma = NULL;
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    if (pSma->dbUid == pDb->uid) {
      if ((code = mndSetDropSmaCommitLogs(pMnode, pTrans, pSma)) != 0) {
        sdbRelease(pSdb, pSma);
        sdbCancelFetch(pSdb, pSma);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pSma);
  }

  TAOS_RETURN(code);
}