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
#include "mndStb.h"
#include "audit.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndIndex.h"
#include "mndIndexComm.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndPerfSchema.h"
#include "mndPrivilege.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define STB_VER_NUMBER   2
#define STB_RESERVE_SIZE 64

static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw);
static int32_t  mndStbActionInsert(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionDelete(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionUpdate(SSdb *pSdb, SStbObj *pOld, SStbObj *pNew);
static int32_t  mndProcessTtlTimer(SRpcMsg *pReq);
static int32_t  mndProcessTrimDbTimer(SRpcMsg *pReq);
static int32_t  mndProcessS3MigrateDbTimer(SRpcMsg *pReq);
static int32_t  mndProcessS3MigrateDbRsp(SRpcMsg *pReq);
static int32_t  mndProcessCreateStbReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterStbReq(SRpcMsg *pReq);
static int32_t  mndProcessDropStbReq(SRpcMsg *pReq);
static int32_t  mndProcessDropTtltbRsp(SRpcMsg *pReq);
static int32_t  mndProcessTrimDbRsp(SRpcMsg *pReq);
static int32_t  mndProcessTableMetaReq(SRpcMsg *pReq);
static int32_t  mndRetrieveStb(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndRetrieveStbCol(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextStb(SMnode *pMnode, void *pIter);
static int32_t  mndProcessTableCfgReq(SRpcMsg *pReq);
static int32_t  mndAlterStbImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                               void *alterOriData, int32_t alterOriDataLen);
static int32_t  mndAlterStbAndUpdateTagIdxImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                                              void *alterOriData, int32_t alterOriDataLen, const SMAlterStbReq *pAlter);

static int32_t mndProcessCreateIndexReq(SRpcMsg *pReq);
static int32_t mndProcessDropIndexReq(SRpcMsg *pReq);

static int32_t mndProcessDropStbReqFromMNode(SRpcMsg *pReq);
static int32_t mndProcessDropTbWithTsma(SRpcMsg *pReq);
static int32_t mndProcessFetchTtlExpiredTbs(SRpcMsg *pReq);

int32_t mndInitStb(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_STB,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndStbActionEncode,
      .decodeFp = (SdbDecodeFp)mndStbActionDecode,
      .insertFp = (SdbInsertFp)mndStbActionInsert,
      .updateFp = (SdbUpdateFp)mndStbActionUpdate,
      .deleteFp = (SdbDeleteFp)mndStbActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STB, mndProcessCreateStbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_STB, mndProcessAlterStbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STB, mndProcessDropStbReq);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TTL_TABLE_RSP, mndProcessDropTtltbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TRIM_RSP, mndProcessTrimDbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TABLE_META, mndProcessTableMetaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TTL_TIMER, mndProcessTtlTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_TRIM_DB_TIMER, mndProcessTrimDbTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_S3MIGRATE_RSP, mndProcessS3MigrateDbRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_S3MIGRATE_DB_TIMER, mndProcessS3MigrateDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_TABLE_CFG, mndProcessTableCfgReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STB_DROP, mndProcessDropStbReqFromMNode);
  mndSetMsgHandle(pMnode, TDMT_MND_STB_DROP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TB_WITH_TSMA, mndProcessDropTbWithTsma);
  mndSetMsgHandle(pMnode, TDMT_VND_FETCH_TTL_EXPIRED_TBS_RSP, mndProcessFetchTtlExpiredTbs);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TABLE_RSP, mndTransProcessRsp);
  //  mndSetMsgHandle(pMnode, TDMT_MND_SYSTABLE_RETRIEVE, mndProcessRetrieveStbReq);

  // mndSetMsgHandle(pMnode, TDMT_MND_CREATE_INDEX, mndProcessCreateIndexReq);
  // mndSetMsgHandle(pMnode, TDMT_MND_DROP_INDEX, mndProcessDropIndexReq);
  // mndSetMsgHandle(pMnode, TDMT_VND_CREATE_INDEX_RSP, mndTransProcessRsp);
  // mndSetMsgHandle(pMnode, TDMT_VND_DROP_INDEX_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STB, mndRetrieveStb);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STB, mndCancelGetNextStb);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_COL, mndRetrieveStbCol);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_COL, mndCancelGetNextStb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStb(SMnode *pMnode) {}

SSdbRaw *mndStbActionEncode(SStbObj *pStb) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t size = sizeof(SStbObj) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema) + pStb->commentLen +
                 pStb->ast1Len + pStb->ast2Len + pStb->numOfColumns * sizeof(SColCmpr) + STB_RESERVE_SIZE +
                 taosArrayGetSize(pStb->pFuncs) * TSDB_FUNC_NAME_LEN;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STB, STB_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pStb->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pStb->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->updateTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->uid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->dbUid, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->tagVer, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->colVer, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->smaVer, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->nextColId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->maxdelay[0], _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->maxdelay[1], _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->watermark[0], _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->watermark[1], _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->ttl, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfColumns, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfTags, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfFuncs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->commentLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->ast1Len, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->ast2Len, _OVER)

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pStb->pColumns[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pSchema->flags, _OVER)
    SDB_SET_INT16(pRaw, dataPos, pSchema->colId, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, _OVER)
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pStb->pTags[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pSchema->flags, _OVER)
    SDB_SET_INT16(pRaw, dataPos, pSchema->colId, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, _OVER)
  }

  for (int32_t i = 0; i < pStb->numOfFuncs; ++i) {
    char *func = taosArrayGet(pStb->pFuncs, i);
    SDB_SET_BINARY(pRaw, dataPos, func, TSDB_FUNC_NAME_LEN, _OVER)
  }

  if (pStb->commentLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pStb->comment, pStb->commentLen + 1, _OVER)
  }

  if (pStb->ast1Len > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pStb->pAst1, pStb->ast1Len, _OVER)
  }

  if (pStb->ast2Len > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pStb->pAst2, pStb->ast2Len, _OVER)
  }

  if (pStb->pCmpr != NULL) {
    for (int i = 0; i < pStb->numOfColumns; i++) {
      SColCmpr *p = &pStb->pCmpr[i];
      SDB_SET_INT16(pRaw, dataPos, p->id, _OVER)
      SDB_SET_INT32(pRaw, dataPos, p->alg, _OVER)
    }
  }
  SDB_SET_RESERVE(pRaw, dataPos, STB_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("stb:%s, failed to encode to raw:%p since %s", pStb->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("stb:%s, encode to raw:%p, row:%p", pStb->name, pRaw, pStb);
  return pRaw;
}

static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SStbObj *pStb = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver > STB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SStbObj));
  if (pRow == NULL) goto _OVER;

  pStb = sdbGetRowObj(pRow);
  if (pStb == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pStb->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pStb->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->updateTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->uid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->dbUid, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->tagVer, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->colVer, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->smaVer, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->nextColId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->maxdelay[0], _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->maxdelay[1], _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->watermark[0], _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->watermark[1], _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->ttl, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->numOfColumns, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->numOfTags, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->numOfFuncs, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->commentLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->ast1Len, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->ast2Len, _OVER)

  pStb->pColumns = taosMemoryCalloc(pStb->numOfColumns, sizeof(SSchema));
  pStb->pTags = taosMemoryCalloc(pStb->numOfTags, sizeof(SSchema));
  pStb->pFuncs = taosArrayInit(pStb->numOfFuncs, TSDB_FUNC_NAME_LEN);
  if (pStb->pColumns == NULL || pStb->pTags == NULL || pStb->pFuncs == NULL) {
    goto _OVER;
  }

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pStb->pColumns[i];
    SDB_GET_INT8(pRaw, dataPos, &pSchema->type, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &pSchema->flags, _OVER)
    SDB_GET_INT16(pRaw, dataPos, &pSchema->colId, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->bytes, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, _OVER)
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pStb->pTags[i];
    SDB_GET_INT8(pRaw, dataPos, &pSchema->type, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &pSchema->flags, _OVER)
    SDB_GET_INT16(pRaw, dataPos, &pSchema->colId, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->bytes, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, _OVER)
  }

  for (int32_t i = 0; i < pStb->numOfFuncs; ++i) {
    char funcName[TSDB_FUNC_NAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, funcName, TSDB_FUNC_NAME_LEN, _OVER)
    if (taosArrayPush(pStb->pFuncs, funcName) == NULL) goto _OVER;
  }

  if (pStb->commentLen > 0) {
    pStb->comment = taosMemoryCalloc(pStb->commentLen + 1, 1);
    if (pStb->comment == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pStb->comment, pStb->commentLen + 1, _OVER)
  }

  if (pStb->ast1Len > 0) {
    pStb->pAst1 = taosMemoryCalloc(pStb->ast1Len, 1);
    if (pStb->pAst1 == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pStb->pAst1, pStb->ast1Len, _OVER)
  }

  if (pStb->ast2Len > 0) {
    pStb->pAst2 = taosMemoryCalloc(pStb->ast2Len, 1);
    if (pStb->pAst2 == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pStb->pAst2, pStb->ast2Len, _OVER)
  }

  pStb->pCmpr = taosMemoryCalloc(pStb->numOfColumns, sizeof(SColCmpr));
  if (sver < STB_VER_NUMBER) {
    // compatible with old data, setup default compress value
    // impl later
    for (int i = 0; i < pStb->numOfColumns; i++) {
      SSchema  *pSchema = &pStb->pColumns[i];
      SColCmpr *pCmpr = &pStb->pCmpr[i];
      pCmpr->id = pSchema->colId;
      pCmpr->alg = createDefaultColCmprByType(pSchema->type);
    }
  } else {
    for (int i = 0; i < pStb->numOfColumns; i++) {
      SColCmpr *pCmpr = &pStb->pCmpr[i];
      SDB_GET_INT16(pRaw, dataPos, &pCmpr->id, _OVER)
      SDB_GET_INT32(pRaw, dataPos, (int32_t *)&pCmpr->alg, _OVER)  // compatiable
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, STB_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("stb:%s, failed to decode from raw:%p since %s", pStb == NULL ? "null" : pStb->name, pRaw, terrstr());
    if (pStb != NULL) {
      taosMemoryFreeClear(pStb->pColumns);
      taosMemoryFreeClear(pStb->pTags);
      taosMemoryFreeClear(pStb->comment);
      taosMemoryFree(pStb->pCmpr);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("stb:%s, decode from raw:%p, row:%p", pStb->name, pRaw, pStb);
  return pRow;
}

void mndFreeStb(SStbObj *pStb) {
  taosArrayDestroy(pStb->pFuncs);
  taosMemoryFreeClear(pStb->pColumns);
  taosMemoryFreeClear(pStb->pTags);
  taosMemoryFreeClear(pStb->comment);
  taosMemoryFreeClear(pStb->pAst1);
  taosMemoryFreeClear(pStb->pAst2);
  taosMemoryFreeClear(pStb->pCmpr);
}

static int32_t mndStbActionInsert(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform insert action, row:%p", pStb->name, pStb);
  return 0;
}

static int32_t mndStbActionDelete(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform delete action, row:%p", pStb->name, pStb);
  mndFreeStb(pStb);
  return 0;
}

static int32_t mndStbActionUpdate(SSdb *pSdb, SStbObj *pOld, SStbObj *pNew) {
  mTrace("stb:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  int32_t numOfColumns = pOld->numOfColumns;
  if (pOld->numOfColumns < pNew->numOfColumns) {
    void *pColumns = taosMemoryMalloc(pNew->numOfColumns * sizeof(SSchema));
    if (pColumns != NULL) {
      taosMemoryFree(pOld->pColumns);
      pOld->pColumns = pColumns;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  if (pOld->numOfTags < pNew->numOfTags) {
    void *pTags = taosMemoryMalloc(pNew->numOfTags * sizeof(SSchema));
    if (pTags != NULL) {
      taosMemoryFree(pOld->pTags);
      pOld->pTags = pTags;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  if (pOld->commentLen < pNew->commentLen && pNew->commentLen > 0) {
    void *comment = taosMemoryMalloc(pNew->commentLen + 1);
    if (comment != NULL) {
      taosMemoryFree(pOld->comment);
      pOld->comment = comment;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }
  pOld->commentLen = pNew->commentLen;

  if (pOld->ast1Len < pNew->ast1Len) {
    void *pAst1 = taosMemoryMalloc(pNew->ast1Len + 1);
    if (pAst1 != NULL) {
      taosMemoryFree(pOld->pAst1);
      pOld->pAst1 = pAst1;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  if (pOld->ast2Len < pNew->ast2Len) {
    void *pAst2 = taosMemoryMalloc(pNew->ast2Len + 1);
    if (pAst2 != NULL) {
      taosMemoryFree(pOld->pAst2);
      pOld->pAst2 = pAst2;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  pOld->updateTime = pNew->updateTime;
  pOld->tagVer = pNew->tagVer;
  pOld->colVer = pNew->colVer;
  pOld->smaVer = pNew->smaVer;
  pOld->nextColId = pNew->nextColId;
  pOld->ttl = pNew->ttl;
  if (pNew->numOfColumns > 0) {
    pOld->numOfColumns = pNew->numOfColumns;
    memcpy(pOld->pColumns, pNew->pColumns, pOld->numOfColumns * sizeof(SSchema));
  }
  if (pNew->numOfTags > 0) {
    pOld->numOfTags = pNew->numOfTags;
    memcpy(pOld->pTags, pNew->pTags, pOld->numOfTags * sizeof(SSchema));
  }
  if (pNew->commentLen > 0) {
    memcpy(pOld->comment, pNew->comment, pNew->commentLen + 1);
    pOld->commentLen = pNew->commentLen;
  }
  if (pNew->ast1Len != 0) {
    memcpy(pOld->pAst1, pNew->pAst1, pNew->ast1Len);
    pOld->ast1Len = pNew->ast1Len;
  }
  if (pNew->ast2Len != 0) {
    memcpy(pOld->pAst2, pNew->pAst2, pNew->ast2Len);
    pOld->ast2Len = pNew->ast2Len;
  }
  if (numOfColumns < pNew->numOfColumns) {
    taosMemoryFree(pOld->pCmpr);
    pOld->pCmpr = taosMemoryCalloc(pNew->numOfColumns, sizeof(SColCmpr));
    memcpy(pOld->pCmpr, pNew->pCmpr, pNew->numOfColumns * sizeof(SColCmpr));
  } else {
    memcpy(pOld->pCmpr, pNew->pCmpr, pNew->numOfColumns * sizeof(SColCmpr));
  }

  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SStbObj *mndAcquireStb(SMnode *pMnode, char *stbName) {
  SSdb    *pSdb = pMnode->pSdb;
  SStbObj *pStb = sdbAcquire(pSdb, SDB_STB, stbName);
  if (pStb == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_STB_NOT_EXIST;
  }
  return pStb;
}

void mndReleaseStb(SMnode *pMnode, SStbObj *pStb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStb);
}

SDbObj *mndAcquireDbByStb(SMnode *pMnode, const char *stbName) {
  SName name = {0};
  if ((terrno = tNameFromString(&name, stbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) return NULL;

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  if ((terrno = tNameGetFullDbName(&name, db)) != 0) return NULL;

  return mndAcquireDb(pMnode, db);
}

static FORCE_INLINE int32_t schemaExColIdCompare(const void *colId, const void *pSchema) {
  if (*(col_id_t *)colId < ((SSchema *)pSchema)->colId) {
    return -1;
  } else if (*(col_id_t *)colId > ((SSchema *)pSchema)->colId) {
    return 1;
  }
  return 0;
}

void *mndBuildVCreateStbReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, int32_t *pContLen, void *alterOriData,
                            int32_t alterOriDataLen) {
  SEncoder       encoder = {0};
  int32_t        contLen;
  SName          name = {0};
  SVCreateStbReq req = {0};

  if ((terrno = tNameFromString(&name, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    goto _err;
  }
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  if ((terrno = tNameGetFullDbName(&name, dbFName)) != 0) {
    goto _err;
  };

  req.name = (char *)tNameGetTableName(&name);
  req.suid = pStb->uid;
  req.rollup = pStb->ast1Len > 0 ? 1 : 0;
  req.alterOriData = alterOriData;
  req.alterOriDataLen = alterOriDataLen;
  req.source = pStb->source;
  // todo
  req.schemaRow.nCols = pStb->numOfColumns;
  req.schemaRow.version = pStb->colVer;
  req.schemaRow.pSchema = pStb->pColumns;
  req.schemaTag.nCols = pStb->numOfTags;
  req.schemaTag.version = pStb->tagVer;
  req.schemaTag.pSchema = pStb->pTags;

  req.colCmpred = 1;
  SColCmprWrapper *pCmpr = &req.colCmpr;
  pCmpr->version = pStb->colVer;
  pCmpr->nCols = pStb->numOfColumns;

  req.colCmpr.pColCmpr = taosMemoryCalloc(pCmpr->nCols, sizeof(SColCmpr));
  for (int32_t i = 0; i < pStb->numOfColumns; i++) {
    SColCmpr *p = &pCmpr->pColCmpr[i];
    p->alg = pStb->pCmpr[i].alg;
    p->id = pStb->pCmpr[i].id;
  }

  if (req.rollup) {
    req.rsmaParam.maxdelay[0] = pStb->maxdelay[0];
    req.rsmaParam.maxdelay[1] = pStb->maxdelay[1];
    req.rsmaParam.watermark[0] = pStb->watermark[0];
    req.rsmaParam.watermark[1] = pStb->watermark[1];
    if (pStb->ast1Len > 0) {
      if (mndConvertRsmaTask(&req.rsmaParam.qmsg[0], &req.rsmaParam.qmsgLen[0], pStb->pAst1, pStb->uid,
                             STREAM_TRIGGER_WINDOW_CLOSE, req.rsmaParam.watermark[0],
                             req.rsmaParam.deleteMark[0]) < 0) {
        goto _err;
      }
    }
    if (pStb->ast2Len > 0) {
      if (mndConvertRsmaTask(&req.rsmaParam.qmsg[1], &req.rsmaParam.qmsgLen[1], pStb->pAst2, pStb->uid,
                             STREAM_TRIGGER_WINDOW_CLOSE, req.rsmaParam.watermark[1],
                             req.rsmaParam.deleteMark[1]) < 0) {
        goto _err;
      }
    }
  }
  // get length
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateStbReq, &req, contLen, ret);
  if (ret < 0) {
    goto _err;
  }

  contLen += sizeof(SMsgHead);

  SMsgHead *pHead = taosMemoryCalloc(1, contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tEncoderInit(&encoder, pBuf, contLen - sizeof(SMsgHead));
  if (tEncodeSVCreateStbReq(&encoder, &req) < 0) {
    taosMemoryFreeClear(pHead);
    tEncoderClear(&encoder);
    goto _err;
  }
  tEncoderClear(&encoder);

  *pContLen = contLen;
  taosMemoryFreeClear(req.rsmaParam.qmsg[0]);
  taosMemoryFreeClear(req.rsmaParam.qmsg[1]);
  taosMemoryFreeClear(req.colCmpr.pColCmpr);
  return pHead;
_err:
  taosMemoryFreeClear(req.rsmaParam.qmsg[0]);
  taosMemoryFreeClear(req.rsmaParam.qmsg[1]);
  taosMemoryFreeClear(req.colCmpr.pColCmpr);
  return NULL;
}

static void *mndBuildVDropStbReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, int32_t *pContLen) {
  SName        name = {0};
  SVDropStbReq req = {0};
  int32_t      contLen = 0;
  int32_t      ret = 0;
  SMsgHead    *pHead = NULL;
  SEncoder     encoder = {0};

  if ((terrno = tNameFromString(&name, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return NULL;
  }

  req.name = (char *)tNameGetTableName(&name);
  req.suid = pStb->uid;

  tEncodeSize(tEncodeSVDropStbReq, &req, contLen, ret);
  if (ret < 0) return NULL;

  contLen += sizeof(SMsgHead);
  pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));

  tEncoderInit(&encoder, pBuf, contLen - sizeof(SMsgHead));
  terrno = tEncodeSVDropStbReq(&encoder, &req);
  tEncoderClear(&encoder);
  if (terrno != 0) return NULL;

  *pContLen = contLen;
  return pHead;
}

int32_t mndCheckCreateStbReq(SMCreateStbReq *pCreate) {
  int32_t code = 0;
  if (pCreate->igExists < 0 || pCreate->igExists > 1) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  if (pCreate->numOfColumns < TSDB_MIN_COLUMNS || pCreate->numOfTags + pCreate->numOfColumns > TSDB_MAX_COLUMNS) {
    code = TSDB_CODE_PAR_INVALID_COLUMNS_NUM;
    TAOS_RETURN(code);
  }

  if (pCreate->numOfTags <= 0 || pCreate->numOfTags > TSDB_MAX_TAGS) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  SField *pField = taosArrayGet(pCreate->pColumns, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    code = TSDB_CODE_PAR_INVALID_FIRST_COLUMN;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < pCreate->numOfColumns; ++i) {
    SFieldWithOptions *pField1 = taosArrayGet(pCreate->pColumns, i);
    if (pField1->type >= TSDB_DATA_TYPE_MAX) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
    if (pField1->bytes <= 0) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
    if (pField1->name[0] == 0) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
  }

  for (int32_t i = 0; i < pCreate->numOfTags; ++i) {
    SField *pField1 = taosArrayGet(pCreate->pTags, i);
    if (pField1->type >= TSDB_DATA_TYPE_MAX) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
    if (pField1->bytes <= 0) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
    if (pField1->name[0] == 0) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateStbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) {
    sdbFreeRaw(pRedoRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));

  TAOS_RETURN(code);
}

static int32_t mndSetCreateStbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetCreateStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, NULL, 0);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }

    STransAction action = {0};
    action.mTraceId = pTrans->mTraceId;
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_CREATE_STB;
    action.acceptableCode = TSDB_CODE_TDB_STB_ALREADY_EXIST;
    action.retryCode = TSDB_CODE_TDB_STB_NOT_EXIST;
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

int32_t mndSetForceDropCreateStbRedoActions(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SStbObj *pStb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t contLen;

  void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, NULL, 0);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_CREATE_STB;
  action.acceptableCode = TSDB_CODE_TDB_STB_ALREADY_EXIST;
  action.retryCode = TSDB_CODE_TDB_STB_NOT_EXIST;
  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateStbUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_RETURN(code);
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_STB;
    action.acceptableCode = TSDB_CODE_TDB_STB_NOT_EXIST;
    if ((code = mndTransAppendUndoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

static SSchema *mndFindStbColumns(const SStbObj *pStb, const char *colName) {
  for (int32_t col = 0; col < pStb->numOfColumns; ++col) {
    SSchema *pSchema = &pStb->pColumns[col];
    if (strncasecmp(pSchema->name, colName, TSDB_COL_NAME_LEN) == 0) {
      return pSchema;
    }
  }
  return NULL;
}

int32_t mndBuildStbFromReq(SMnode *pMnode, SStbObj *pDst, SMCreateStbReq *pCreate, SDbObj *pDb) {
  int32_t code = 0;
  memcpy(pDst->name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  memcpy(pDst->db, pDb->name, TSDB_DB_FNAME_LEN);
  pDst->createdTime = taosGetTimestampMs();
  pDst->updateTime = pDst->createdTime;
  pDst->uid = (pCreate->source == TD_REQ_FROM_TAOX_OLD || pCreate->source == TD_REQ_FROM_TAOX)
                  ? pCreate->suid
                  : mndGenerateUid(pCreate->name, TSDB_TABLE_FNAME_LEN);
  pDst->dbUid = pDb->uid;
  pDst->tagVer = 1;
  pDst->colVer = 1;
  pDst->smaVer = 1;
  pDst->nextColId = 1;
  pDst->maxdelay[0] = pCreate->delay1;
  pDst->maxdelay[1] = pCreate->delay2;
  pDst->watermark[0] = pCreate->watermark1;
  pDst->watermark[1] = pCreate->watermark2;
  pDst->ttl = pCreate->ttl;
  pDst->numOfColumns = pCreate->numOfColumns;
  pDst->numOfTags = pCreate->numOfTags;
  pDst->numOfFuncs = pCreate->numOfFuncs;
  pDst->commentLen = pCreate->commentLen;
  pDst->pFuncs = pCreate->pFuncs;
  pDst->source = pCreate->source;
  pCreate->pFuncs = NULL;

  if (pDst->commentLen > 0) {
    pDst->comment = taosMemoryCalloc(pDst->commentLen + 1, 1);
    if (pDst->comment == NULL) {
      code = terrno;
      TAOS_RETURN(code);
    }
    memcpy(pDst->comment, pCreate->pComment, pDst->commentLen + 1);
  }

  pDst->ast1Len = pCreate->ast1Len;
  if (pDst->ast1Len > 0) {
    pDst->pAst1 = taosMemoryCalloc(pDst->ast1Len, 1);
    if (pDst->pAst1 == NULL) {
      code = terrno;
      TAOS_RETURN(code);
    }
    memcpy(pDst->pAst1, pCreate->pAst1, pDst->ast1Len);
  }

  pDst->ast2Len = pCreate->ast2Len;
  if (pDst->ast2Len > 0) {
    pDst->pAst2 = taosMemoryCalloc(pDst->ast2Len, 1);
    if (pDst->pAst2 == NULL) {
      code = terrno;
      TAOS_RETURN(code);
    }
    memcpy(pDst->pAst2, pCreate->pAst2, pDst->ast2Len);
  }

  pDst->pColumns = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SSchema));
  pDst->pTags = taosMemoryCalloc(1, pDst->numOfTags * sizeof(SSchema));
  if (pDst->pColumns == NULL || pDst->pTags == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  if (pDst->nextColId < 0 || pDst->nextColId >= 0x7fff - pDst->numOfColumns - pDst->numOfTags) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < pDst->numOfColumns; ++i) {
    SFieldWithOptions *pField = taosArrayGet(pCreate->pColumns, i);
    SSchema           *pSchema = &pDst->pColumns[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    pSchema->flags = pField->flags;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = pDst->nextColId;
    pDst->nextColId++;
  }

  for (int32_t i = 0; i < pDst->numOfTags; ++i) {
    SField  *pField = taosArrayGet(pCreate->pTags, i);
    SSchema *pSchema = &pDst->pTags[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    if (i == 0) {
      SSCHMEA_SET_IDX_ON(pSchema);
    }
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = pDst->nextColId;
    pDst->nextColId++;
  }
  // set col compress
  pDst->pCmpr = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SCmprObj));
  for (int32_t i = 0; i < pDst->numOfColumns; i++) {
    SFieldWithOptions *pField = taosArrayGet(pCreate->pColumns, i);
    SSchema           *pSchema = &pDst->pColumns[i];

    SColCmpr *pColCmpr = &pDst->pCmpr[i];
    pColCmpr->id = pSchema->colId;
    pColCmpr->alg = pField->compress;
  }
  TAOS_RETURN(code);
}
static int32_t mndGenIdxNameForFirstTag(char *fullname, char *dbname, char *stbname, char *tagname) {
  SName name = {0};
  if ((terrno = tNameFromString(&name, stbname, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return -1;
  }
  return snprintf(fullname, TSDB_INDEX_FNAME_LEN, "%s.%s_%s", dbname, tagname, tNameGetTableName(&name));
}

static int32_t mndCreateStb(SMnode *pMnode, SRpcMsg *pReq, SMCreateStbReq *pCreate, SDbObj *pDb) {
  SStbObj stbObj = {0};
  int32_t code = -1;

  char fullIdxName[TSDB_INDEX_FNAME_LEN * 2] = {0};

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq, "create-stb");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to create stb:%s", pTrans->id, pCreate->name);
  TAOS_CHECK_GOTO(mndBuildStbFromReq(pMnode, &stbObj, pCreate, pDb), NULL, _OVER);

  SSchema *pSchema = &(stbObj.pTags[0]);
  if (mndGenIdxNameForFirstTag(fullIdxName, pDb->name, stbObj.name, pSchema->name) < 0) {
    goto _OVER;
  }
  SSIdx idx = {0};
  if (mndAcquireGlobalIdx(pMnode, fullIdxName, SDB_IDX, &idx) == 0 && idx.pIdx != NULL) {
    code = TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST;
    mndReleaseIdx(pMnode, idx.pIdx);
    goto _OVER;
  }

  SIdxObj idxObj = {0};
  memcpy(idxObj.name, fullIdxName, TSDB_INDEX_FNAME_LEN);
  memcpy(idxObj.stb, stbObj.name, TSDB_TABLE_FNAME_LEN);
  memcpy(idxObj.db, stbObj.db, TSDB_DB_FNAME_LEN);
  memcpy(idxObj.colName, pSchema->name, TSDB_COL_NAME_LEN);
  idxObj.createdTime = taosGetTimestampMs();
  idxObj.uid = mndGenerateUid(fullIdxName, strlen(fullIdxName));
  idxObj.stbUid = stbObj.uid;
  idxObj.dbUid = stbObj.dbUid;

  TAOS_CHECK_GOTO(mndSetCreateIdxCommitLogs(pMnode, pTrans, &idxObj), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  if (mndStbActionDelete(pMnode->pSdb, &stbObj) != 0) mError("failed to mndStbActionDelete");
  TAOS_RETURN(code);
}

int32_t mndAddStbToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  TAOS_CHECK_RETURN(mndTransCheckConflict(pMnode, pTrans));
  TAOS_CHECK_RETURN(mndSetCreateStbCommitLogs(pMnode, pTrans, pDb, pStb));
  TAOS_CHECK_RETURN(mndSetCreateStbRedoActions(pMnode, pTrans, pDb, pStb));
  TAOS_CHECK_RETURN(mndSetCreateStbUndoActions(pMnode, pTrans, pDb, pStb));
  return 0;
}

static int32_t mndProcessTtlTimer(SRpcMsg *pReq) {
  SMnode           *pMnode = pReq->info.node;
  SSdb             *pSdb = pMnode->pSdb;
  SVgObj           *pVgroup = NULL;
  void             *pIter = NULL;
  SVDropTtlTableReq ttlReq = {
      .timestampSec = taosGetTimestampSec(), .ttlDropMaxCount = tsTtlBatchDropNum, .nUids = 0, .pTbUids = NULL};
  int32_t reqLen = tSerializeSVDropTtlTableReq(NULL, 0, &ttlReq);
  int32_t contLen = reqLen + sizeof(SMsgHead);

  mDebug("start to process ttl timer");

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    int32_t   code = 0;
    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    if ((code = tSerializeSVDropTtlTableReq((char *)pHead + sizeof(SMsgHead), reqLen, &ttlReq)) < 0) {
      mError("vgId:%d, failed to serialize drop ttl table request since %s", pVgroup->vgId, tstrerror(code));
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    SRpcMsg rpcMsg = {
        .msgType = TDMT_VND_FETCH_TTL_EXPIRED_TBS, .pCont = pHead, .contLen = contLen, .info = pReq->info};
    SEpSet epSet = mndGetVgroupEpset(pMnode, pVgroup);
    code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, failed to send drop ttl table request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mDebug("vgId:%d, send drop ttl table request to vnode, time:%" PRId32, pVgroup->vgId, ttlReq.timestampSec);
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndProcessTrimDbTimer(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  SVgObj     *pVgroup = NULL;
  void       *pIter = NULL;
  SVTrimDbReq trimReq = {.timestamp = taosGetTimestampSec()};
  int32_t     reqLen = tSerializeSVTrimDbReq(NULL, 0, &trimReq);
  int32_t     contLen = reqLen + sizeof(SMsgHead);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    int32_t code = 0;

    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbCancelFetch(pSdb, pVgroup);
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    if ((code = tSerializeSVTrimDbReq((char *)pHead + sizeof(SMsgHead), reqLen, &trimReq)) < 0) {
      mError("vgId:%d, failed to serialize trim db request since %s", pVgroup->vgId, tstrerror(code));
    }

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_TRIM, .pCont = pHead, .contLen = contLen};
    SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
    code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, timer failed to send vnode-trim request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mInfo("vgId:%d, timer send vnode-trim request to vnode, time:%d", pVgroup->vgId, trimReq.timestamp);
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndProcessS3MigrateDbTimer(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  SVgObj          *pVgroup = NULL;
  void            *pIter = NULL;
  SVS3MigrateDbReq s3migrateReq = {.timestamp = taosGetTimestampSec()};
  int32_t          reqLen = tSerializeSVS3MigrateDbReq(NULL, 0, &s3migrateReq);
  int32_t          contLen = reqLen + sizeof(SMsgHead);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    int32_t code = 0;

    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    if ((code = tSerializeSVS3MigrateDbReq((char *)pHead + sizeof(SMsgHead), reqLen, &s3migrateReq)) < 0) {
      mError("vgId:%d, failed to serialize s3migrate db request since %s", pVgroup->vgId, tstrerror(code));
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_S3MIGRATE, .pCont = pHead, .contLen = contLen};
    SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
    code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, timer failed to send vnode-s3migrate request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mInfo("vgId:%d, timer send vnode-s3migrate request to vnode, time:%d", pVgroup->vgId, s3migrateReq.timestamp);
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndFindSuperTableTagIndex(const SStbObj *pStb, const char *tagName) {
  for (int32_t tag = 0; tag < pStb->numOfTags; tag++) {
    if (strcmp(pStb->pTags[tag].name, tagName) == 0) {
      return tag;
    }
  }

  return -1;
}

static int32_t mndFindSuperTableColumnIndex(const SStbObj *pStb, const char *colName) {
  for (int32_t col = 0; col < pStb->numOfColumns; col++) {
    if (strcmp(pStb->pColumns[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static bool mndValidateSchema(SSchema *pSchemas, int32_t nSchema, SArray *pFields, int32_t maxLen) {
  int32_t rowLen = 0;
  for (int32_t i = 0; i < nSchema; ++i) {
    rowLen += (pSchemas + i)->bytes;
  }

  int32_t nField = taosArrayGetSize(pFields);
  for (int32_t i = 0; i < nField; ++i) {
    rowLen += ((SField *)TARRAY_GET_ELEM(pFields, i))->bytes;
  }

  return rowLen <= maxLen;
}

static int32_t mndBuildStbFromAlter(SStbObj *pStb, SStbObj *pDst, SMCreateStbReq *createReq) {
  int32_t code = 0;
  taosRLockLatch(&pStb->lock);
  memcpy(pDst, pStb, sizeof(SStbObj));
  taosRUnLockLatch(&pStb->lock);

  pDst->source = createReq->source;
  pDst->updateTime = taosGetTimestampMs();
  pDst->numOfColumns = createReq->numOfColumns;
  pDst->numOfTags = createReq->numOfTags;
  pDst->pColumns = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SSchema));
  pDst->pTags = taosMemoryCalloc(1, pDst->numOfTags * sizeof(SSchema));
  pDst->pCmpr = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SColCmpr));

  if (pDst->pColumns == NULL || pDst->pTags == NULL || pDst->pCmpr == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  if (pDst->nextColId < 0 || pDst->nextColId >= 0x7fff - pDst->numOfColumns - pDst->numOfTags) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < pDst->numOfColumns; ++i) {
    SFieldWithOptions *pField = taosArrayGet(createReq->pColumns, i);
    SSchema           *pSchema = &pDst->pColumns[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    pSchema->flags = pField->flags;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    int32_t cIndex = mndFindSuperTableColumnIndex(pStb, pField->name);
    if (cIndex >= 0) {
      pSchema->colId = pStb->pColumns[cIndex].colId;
    } else {
      pSchema->colId = pDst->nextColId++;
    }
  }

  for (int32_t i = 0; i < pDst->numOfTags; ++i) {
    SField  *pField = taosArrayGet(createReq->pTags, i);
    SSchema *pSchema = &pDst->pTags[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    int32_t cIndex = mndFindSuperTableTagIndex(pStb, pField->name);
    if (cIndex >= 0) {
      pSchema->colId = pStb->pTags[cIndex].colId;
    } else {
      pSchema->colId = pDst->nextColId++;
    }
  }
  for (int32_t i = 0; i < pDst->numOfColumns; i++) {
    SColCmpr          *p = pDst->pCmpr + i;
    SFieldWithOptions *pField = taosArrayGet(createReq->pColumns, i);
    SSchema           *pSchema = &pDst->pColumns[i];
    p->id = pSchema->colId;
    if (pField->compress == 0) {
      p->alg = createDefaultColCmprByType(pSchema->type);
    } else {
      p->alg = pField->compress;
    }
  }
  pDst->tagVer = createReq->tagVer;
  pDst->colVer = createReq->colVer;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessCreateStbReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SStbObj       *pStb = NULL;
  SDbObj        *pDb = NULL;
  SMCreateStbReq createReq = {0};
  bool           isAlter = false;

  if (tDeserializeSMCreateStbReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("stb:%s, start to create", createReq.name);
  if (mndCheckCreateStbReq(&createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.name);
  if (pStb != NULL) {
    if (createReq.igExists) {
      if (createReq.source == TD_REQ_FROM_APP) {
        mInfo("stb:%s, already exist, ignore exist is set", createReq.name);
        code = 0;
        goto _OVER;
      } else if (pStb->uid != createReq.suid) {
        mInfo("stb:%s, alter table does not need to be done, because table is deleted", createReq.name);
        code = 0;
        goto _OVER;
      } else if (createReq.tagVer > 0 || createReq.colVer > 0) {
        int32_t tagDelta = createReq.tagVer - pStb->tagVer;
        int32_t colDelta = createReq.colVer - pStb->colVer;
        mInfo("stb:%s, already exist while create, input tagVer:%d colVer:%d, exist tagVer:%d colVer:%d",
              createReq.name, createReq.tagVer, createReq.colVer, pStb->tagVer, pStb->colVer);
        if (tagDelta <= 0 && colDelta <= 0) {
          mInfo("stb:%s, schema version is not incremented and nothing needs to be done", createReq.name);
          code = 0;
          goto _OVER;
        } else if ((tagDelta == 1 && colDelta == 0) || (tagDelta == 0 && colDelta == 1) ||
                   (pStb->colVer == 1 && createReq.colVer > 1) || (pStb->tagVer == 1 && createReq.tagVer > 1)) {
          isAlter = true;
          mInfo("stb:%s, schema version is only increased by 1 number, do alter operation", createReq.name);
        } else {
          mError("stb:%s, schema version increase more than 1 number, error is returned", createReq.name);
          code = TSDB_CODE_MND_INVALID_SCHEMA_VER;
          goto _OVER;
        }
      } else {
        mError("stb:%s, already exist while create, input tagVer:%d colVer:%d is invalid, origin tagVer:%d colVer:%d",
               createReq.name, createReq.tagVer, createReq.colVer, pStb->tagVer, pStb->colVer);
        code = TSDB_CODE_MND_INVALID_SCHEMA_VER;
        goto _OVER;
      }
    } else {
      code = TSDB_CODE_MND_STB_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STB_NOT_EXIST) {
    goto _OVER;
  } else if ((createReq.source == TD_REQ_FROM_TAOX_OLD || createReq.source == TD_REQ_FROM_TAOX) &&
             (createReq.tagVer != 1 || createReq.colVer != 1)) {
    mInfo("stb:%s, alter table does not need to be done, because table is deleted", createReq.name);
    code = 0;
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb)) != 0) {
    goto _OVER;
  }

  int32_t numOfStbs = -1;
  if ((code = mndGetNumOfStbs(pMnode, pDb->name, &numOfStbs)) != 0) {
    goto _OVER;
  }

  if (pDb->cfg.numOfStables == 1 && numOfStbs != 0) {
    code = TSDB_CODE_MND_SINGLE_STB_MODE_DB;
    goto _OVER;
  }

  if ((code = grantCheck(TSDB_GRANT_STABLE)) < 0) {
    goto _OVER;
  }

  if (isAlter) {
    bool    needRsp = false;
    SStbObj pDst = {0};
    if ((code = mndBuildStbFromAlter(pStb, &pDst, &createReq)) != 0) {
      taosMemoryFreeClear(pDst.pTags);
      taosMemoryFreeClear(pDst.pColumns);
      taosMemoryFreeClear(pDst.pCmpr);
      goto _OVER;
    }

    code = mndAlterStbImp(pMnode, pReq, pDb, &pDst, needRsp, NULL, 0);
    taosMemoryFreeClear(pDst.pTags);
    taosMemoryFreeClear(pDst.pColumns);
    taosMemoryFreeClear(pDst.pCmpr);
  } else {
    code = mndCreateStb(pMnode, pReq, &createReq, pDb);
  }
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName name = {0};
  TAOS_CHECK_RETURN(tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

  if (createReq.sql == NULL && createReq.sqlLen == 0) {
    char detail[1000] = {0};

    sprintf(detail, "dbname:%s, stable name:%s", name.dbname, name.tname);

    auditRecord(pReq, pMnode->clusterId, "createStb", name.dbname, name.tname, detail, strlen(detail));
  } else {
    auditRecord(pReq, pMnode->clusterId, "createStb", name.dbname, name.tname, createReq.sql, createReq.sqlLen);
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to create since %s", createReq.name, tstrerror(code));
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  tFreeSMCreateStbReq(&createReq);

  TAOS_RETURN(code);
}

static int32_t mndCheckAlterStbReq(SMAlterStbReq *pAlter) {
  int32_t code = 0;
  if (pAlter->commentLen >= 0) return 0;
  if (pAlter->ttl != 0) return 0;

  if (pAlter->numOfFields < 1 || pAlter->numOfFields != (int32_t)taosArrayGetSize(pAlter->pFields)) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < pAlter->numOfFields; ++i) {
    SField *pField = taosArrayGet(pAlter->pFields, i);
    if (pField->name[0] == 0) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndAllocStbSchemas(const SStbObj *pOld, SStbObj *pNew) {
  pNew->pTags = taosMemoryCalloc(pNew->numOfTags, sizeof(SSchema));
  pNew->pColumns = taosMemoryCalloc(pNew->numOfColumns, sizeof(SSchema));
  pNew->pCmpr = taosMemoryCalloc(pNew->numOfColumns, sizeof(SColCmpr));
  if (pNew->pTags == NULL || pNew->pColumns == NULL || pNew->pCmpr == NULL) {
    TAOS_RETURN(terrno);
  }

  memcpy(pNew->pColumns, pOld->pColumns, sizeof(SSchema) * pOld->numOfColumns);
  memcpy(pNew->pTags, pOld->pTags, sizeof(SSchema) * pOld->numOfTags);
  memcpy(pNew->pCmpr, pOld->pCmpr, sizeof(SColCmpr) * pOld->numOfColumns);

  TAOS_RETURN(0);
}

static int32_t mndUpdateStbCommentAndTTL(const SStbObj *pOld, SStbObj *pNew, char *pComment, int32_t commentLen,
                                         int32_t ttl) {
  int32_t code = 0;
  if (commentLen > 0) {
    pNew->commentLen = commentLen;
    pNew->comment = taosMemoryCalloc(1, commentLen + 1);
    if (pNew->comment == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    memcpy(pNew->comment, pComment, commentLen + 1);
  } else if (commentLen == 0) {
    pNew->commentLen = 0;
  } else {
  }

  if (ttl >= 0) {
    pNew->ttl = ttl;
  }

  if ((code = mndAllocStbSchemas(pOld, pNew)) != 0) {
    TAOS_RETURN(code);
  }
  TAOS_RETURN(code);
}

static int32_t mndAddSuperTableTag(const SStbObj *pOld, SStbObj *pNew, SArray *pFields, int32_t ntags) {
  int32_t code = 0;
  if (pOld->numOfTags + ntags > TSDB_MAX_TAGS) {
    code = TSDB_CODE_MND_TOO_MANY_TAGS;
    TAOS_RETURN(code);
  }

  if (pOld->numOfColumns + ntags + pOld->numOfTags > TSDB_MAX_COLUMNS) {
    code = TSDB_CODE_MND_TOO_MANY_COLUMNS;
    TAOS_RETURN(code);
  }

  if (!mndValidateSchema(pOld->pTags, pOld->numOfTags, pFields, TSDB_MAX_TAGS_LEN)) {
    code = TSDB_CODE_PAR_INVALID_TAGS_LENGTH;
    TAOS_RETURN(code);
  }

  pNew->numOfTags = pNew->numOfTags + ntags;
  if ((code = mndAllocStbSchemas(pOld, pNew)) != 0) {
    TAOS_RETURN(code);
  }

  if (pNew->nextColId < 0 || pNew->nextColId >= 0x7fff - ntags) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < ntags; i++) {
    SField *pField = taosArrayGet(pFields, i);
    if (mndFindSuperTableColumnIndex(pOld, pField->name) >= 0) {
      code = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
      TAOS_RETURN(code);
    }

    if (mndFindSuperTableTagIndex(pOld, pField->name) >= 0) {
      code = TSDB_CODE_MND_TAG_ALREADY_EXIST;
      TAOS_RETURN(code);
    }

    SSchema *pSchema = &pNew->pTags[pOld->numOfTags + i];
    pSchema->bytes = pField->bytes;
    pSchema->type = pField->type;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = pNew->nextColId;
    pNew->nextColId++;

    mInfo("stb:%s, start to add tag %s", pNew->name, pSchema->name);
  }

  pNew->tagVer++;
  TAOS_RETURN(code);
}

static int32_t mndCheckAlterColForTopic(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SMqTopicObj *pTopic = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) break;

    mInfo("topic:%s, check tag and column modifiable, stb:%s suid:%" PRId64 " colId:%d, subType:%d sql:%s",
          pTopic->name, stbFullName, suid, colId, pTopic->subType, pTopic->sql);
    if (pTopic->ast == NULL) {
      sdbRelease(pSdb, pTopic);
      continue;
    }

    SNode *pAst = NULL;
    if (nodesStringToNode(pTopic->ast, &pAst) != 0) {
      code = TSDB_CODE_MND_FIELD_CONFLICT_WITH_TOPIC;
      mError("topic:%s, create ast error", pTopic->name);
      sdbRelease(pSdb, pTopic);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    SNodeList *pNodeList = NULL;
    if ((code = nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList)) !=
        0) {
      sdbRelease(pSdb, pTopic);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;
      mInfo("topic:%s, check colId:%d tableId:%" PRId64 " ctbStbUid:%" PRId64, pTopic->name, pCol->colId, pCol->tableId,
            pTopic->ctbStbUid);

      if (pCol->tableId != suid && pTopic->ctbStbUid != suid) {
        mInfo("topic:%s, check colId:%d passed", pTopic->name, pCol->colId);
        goto NEXT;
      }
      if (pCol->colId > 0 && pCol->colId == colId) {
        code = TSDB_CODE_MND_FIELD_CONFLICT_WITH_TOPIC;
        mError("topic:%s, check colId:%d conflicted", pTopic->name, pCol->colId);
        nodesDestroyNode(pAst);
        nodesDestroyList(pNodeList);
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pTopic);
        TAOS_RETURN(code);
      }
      mInfo("topic:%s, check colId:%d passed", pTopic->name, pCol->colId);
    }

  NEXT:
    sdbRelease(pSdb, pTopic);
    nodesDestroyNode(pAst);
    nodesDestroyList(pNodeList);
  }
  TAOS_RETURN(code);
}

static int32_t mndCheckAlterColForStream(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    SNode *pAst = NULL;
    if (nodesStringToNode(pStream->ast, &pAst) != 0) {
      code = TSDB_CODE_MND_INVALID_STREAM_OPTION;
      mError("stream:%s, create ast error", pStream->name);
      sdbRelease(pSdb, pStream);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    SNodeList *pNodeList = NULL;
    if ((code = nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList)) !=
        0) {
      sdbRelease(pSdb, pStream);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;

      if (pCol->tableId != suid) {
        mInfo("stream:%s, check colId:%d passed", pStream->name, pCol->colId);
        goto NEXT;
      }
      if (pCol->colId > 0 && pCol->colId == colId) {
        code = TSDB_CODE_MND_STREAM_MUST_BE_DELETED;
        mError("stream:%s, check colId:%d conflicted", pStream->name, pCol->colId);
        nodesDestroyNode(pAst);
        nodesDestroyList(pNodeList);
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
      mInfo("stream:%s, check colId:%d passed", pStream->name, pCol->colId);
    }

  NEXT:
    sdbRelease(pSdb, pStream);
    nodesDestroyNode(pAst);
    nodesDestroyList(pNodeList);
  }
  TAOS_RETURN(code);
}

static int32_t mndCheckAlterColForTSma(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SSmaObj *pSma = NULL;
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    mInfo("tsma:%s, check tag and column modifiable, stb:%s suid:%" PRId64 " colId:%d, sql:%s", pSma->name, stbFullName,
          suid, colId, pSma->sql);

    SNode *pAst = NULL;
    if (nodesStringToNode(pSma->ast, &pAst) != 0) {
      code = TSDB_CODE_SDB_INVALID_DATA_CONTENT;
      mError("tsma:%s, check tag and column modifiable, stb:%s suid:%" PRId64 " colId:%d failed since parse AST err",
             pSma->name, stbFullName, suid, colId);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    SNodeList *pNodeList = NULL;
    if ((code = nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList)) !=
        0) {
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;
      mInfo("tsma:%s, check colId:%d tableId:%" PRId64, pSma->name, pCol->colId, pCol->tableId);

      if ((pCol->tableId != suid) && (pSma->stbUid != suid)) {
        mInfo("tsma:%s, check colId:%d passed", pSma->name, pCol->colId);
        goto NEXT;
      }
      if ((pCol->colId) > 0 && (pCol->colId == colId)) {
        code = TSDB_CODE_MND_FIELD_CONFLICT_WITH_TSMA;
        mError("tsma:%s, check colId:%d conflicted", pSma->name, pCol->colId);
        nodesDestroyNode(pAst);
        nodesDestroyList(pNodeList);
        sdbRelease(pSdb, pSma);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
      mInfo("tsma:%s, check colId:%d passed", pSma->name, pCol->colId);
    }

  NEXT:
    sdbRelease(pSdb, pSma);
    nodesDestroyNode(pAst);
    nodesDestroyList(pNodeList);
  }
  TAOS_RETURN(code);
}

int32_t mndCheckColAndTagModifiable(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId) {
  TAOS_CHECK_RETURN(mndCheckAlterColForTopic(pMnode, stbFullName, suid, colId));
  TAOS_CHECK_RETURN(mndCheckAlterColForStream(pMnode, stbFullName, suid, colId));
  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, stbFullName, suid, colId));
  TAOS_RETURN(0);
}

static int32_t mndDropSuperTableTag(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, const char *tagName) {
  int32_t code = 0;
  int32_t tag = mndFindSuperTableTagIndex(pOld, tagName);
  if (tag < 0) {
    code = TSDB_CODE_MND_TAG_NOT_EXIST;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pTags[tag].colId;
  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  memmove(pNew->pTags + tag, pNew->pTags + tag + 1, sizeof(SSchema) * (pNew->numOfTags - tag - 1));
  pNew->numOfTags--;

  pNew->tagVer++;

  // if (mndDropIndexByTag(pMnode, pOld, tagName) != 0) {
  //   return -1;
  // }
  mInfo("stb:%s, start to drop tag %s", pNew->name, tagName);
  TAOS_RETURN(code);
}

static int32_t mndAlterStbTagName(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, SArray *pFields) {
  int32_t code = 0;
  if ((int32_t)taosArrayGetSize(pFields) != 2) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  SField *pField0 = taosArrayGet(pFields, 0);
  SField *pField1 = taosArrayGet(pFields, 1);

  const char *oldTagName = pField0->name;
  const char *newTagName = pField1->name;

  int32_t tag = mndFindSuperTableTagIndex(pOld, oldTagName);
  if (tag < 0) {
    code = TSDB_CODE_MND_TAG_NOT_EXIST;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pTags[tag].colId;
  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  if (mndFindSuperTableTagIndex(pOld, newTagName) >= 0) {
    code = TSDB_CODE_MND_TAG_ALREADY_EXIST;
    TAOS_RETURN(code);
  }

  if (mndFindSuperTableColumnIndex(pOld, newTagName) >= 0) {
    code = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  SSchema *pSchema = (SSchema *)(pNew->pTags + tag);
  memcpy(pSchema->name, newTagName, TSDB_COL_NAME_LEN);

  pNew->tagVer++;
  mInfo("stb:%s, start to modify tag %s to %s", pNew->name, oldTagName, newTagName);
  TAOS_RETURN(code);
}

static int32_t mndAlterStbTagBytes(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, const SField *pField) {
  int32_t code = 0;
  int32_t tag = mndFindSuperTableTagIndex(pOld, pField->name);
  if (tag < 0) {
    code = TSDB_CODE_MND_TAG_NOT_EXIST;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pTags[tag].colId;
  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  uint32_t nLen = 0;
  for (int32_t i = 0; i < pOld->numOfTags; ++i) {
    nLen += (pOld->pTags[i].colId == colId) ? pField->bytes : pOld->pTags[i].bytes;
  }

  if (nLen > TSDB_MAX_TAGS_LEN) {
    code = TSDB_CODE_PAR_INVALID_TAGS_LENGTH;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  SSchema *pTag = pNew->pTags + tag;

  if (!(pTag->type == TSDB_DATA_TYPE_BINARY || pTag->type == TSDB_DATA_TYPE_VARBINARY ||
        pTag->type == TSDB_DATA_TYPE_NCHAR || pTag->type == TSDB_DATA_TYPE_GEOMETRY)) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  if (pField->bytes <= pTag->bytes) {
    code = TSDB_CODE_MND_INVALID_ROW_BYTES;
    TAOS_RETURN(code);
  }

  pTag->bytes = pField->bytes;
  pNew->tagVer++;

  mInfo("stb:%s, start to modify tag len %s to %d", pNew->name, pField->name, pField->bytes);
  TAOS_RETURN(code);
}

static int32_t mndUpdateSuperTableColumnCompress(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, SArray *pField,
                                                 int32_t nCols) {
  // if (pColCmpr == NULL || colName == NULL) return -1;

  if (taosArrayGetSize(pField) != nCols) return TSDB_CODE_FAILED;
  TAOS_FIELD *p = taosArrayGet(pField, 0);

  int32_t code = 0;
  int32_t idx = mndFindSuperTableColumnIndex(pOld, p->name);
  if (idx == -1) {
    code = TSDB_CODE_MND_COLUMN_NOT_EXIST;
    TAOS_RETURN(code);
  }
  SSchema *pTarget = &pOld->pColumns[idx];
  col_id_t colId = pTarget->colId;
  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));
  code = validColCmprByType(pTarget->type, p->bytes);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_RETURN(code);
  }

  int8_t updated = 0;
  for (int i = 0; i < pNew->numOfColumns; i++) {
    SColCmpr *pCmpr = &pNew->pCmpr[i];
    if (pCmpr->id == colId) {
      uint32_t dst = 0;
      updated = tUpdateCompress(pCmpr->alg, p->bytes, TSDB_COLVAL_COMPRESS_DISABLED, TSDB_COLVAL_LEVEL_DISABLED,
                                TSDB_COLVAL_LEVEL_MEDIUM, &dst);
      if (updated > 0) pCmpr->alg = dst;
      break;
    }
  }

  if (updated == 0) {
    code = TSDB_CODE_MND_COLUMN_COMPRESS_ALREADY_EXIST;
    TAOS_RETURN(code);
  } else if (updated == -1) {
    code = TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
    TAOS_RETURN(code);
  }

  pNew->colVer++;

  TAOS_RETURN(code);
}
static int32_t mndAddSuperTableColumn(const SStbObj *pOld, SStbObj *pNew, SArray *pFields, int32_t ncols,
                                      int8_t withCompress) {
  int32_t code = 0;
  if (pOld->numOfColumns + ncols + pOld->numOfTags > TSDB_MAX_COLUMNS) {
    code = TSDB_CODE_MND_TOO_MANY_COLUMNS;
    TAOS_RETURN(code);
  }

  if ((code = grantCheck(TSDB_GRANT_TIMESERIES)) != 0) {
    TAOS_RETURN(code);
  }

  if (!mndValidateSchema(pOld->pColumns, pOld->numOfColumns, pFields, TSDB_MAX_BYTES_PER_ROW)) {
    code = TSDB_CODE_PAR_INVALID_ROW_LENGTH;
    TAOS_RETURN(code);
  }

  pNew->numOfColumns = pNew->numOfColumns + ncols;

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  if (pNew->nextColId < 0 || pNew->nextColId >= 0x7fff - ncols) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (withCompress) {
      SFieldWithOptions *pField = taosArrayGet(pFields, i);
      if (mndFindSuperTableColumnIndex(pOld, pField->name) >= 0) {
        code = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
        TAOS_RETURN(code);
      }

      if (mndFindSuperTableTagIndex(pOld, pField->name) >= 0) {
        code = TSDB_CODE_MND_TAG_ALREADY_EXIST;
        TAOS_RETURN(code);
      }

      SSchema *pSchema = &pNew->pColumns[pOld->numOfColumns + i];
      pSchema->bytes = pField->bytes;
      pSchema->type = pField->type;
      memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
      pSchema->colId = pNew->nextColId;
      pNew->nextColId++;

      SColCmpr *pCmpr = &pNew->pCmpr[pOld->numOfColumns + i];
      pCmpr->id = pSchema->colId;
      pCmpr->alg = pField->compress;
      mInfo("stb:%s, start to add column %s", pNew->name, pSchema->name);
    } else {
      SField *pField = taosArrayGet(pFields, i);
      if (mndFindSuperTableColumnIndex(pOld, pField->name) >= 0) {
        code = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
        TAOS_RETURN(code);
      }

      if (mndFindSuperTableTagIndex(pOld, pField->name) >= 0) {
        code = TSDB_CODE_MND_TAG_ALREADY_EXIST;
        TAOS_RETURN(code);
      }

      SSchema *pSchema = &pNew->pColumns[pOld->numOfColumns + i];
      pSchema->bytes = pField->bytes;
      pSchema->type = pField->type;
      memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
      pSchema->colId = pNew->nextColId;
      pNew->nextColId++;

      SColCmpr *pCmpr = &pNew->pCmpr[pOld->numOfColumns + i];
      pCmpr->id = pSchema->colId;
      pCmpr->alg = createDefaultColCmprByType(pSchema->type);
      mInfo("stb:%s, start to add column %s", pNew->name, pSchema->name);
    }
  }

  pNew->colVer++;
  TAOS_RETURN(code);
}

static int32_t mndDropSuperTableColumn(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, const char *colName) {
  int32_t code = 0;
  int32_t col = mndFindSuperTableColumnIndex(pOld, colName);
  if (col < 0) {
    code = TSDB_CODE_MND_COLUMN_NOT_EXIST;
    TAOS_RETURN(code);
  }

  if (col == 0) {
    code = TSDB_CODE_MND_INVALID_STB_ALTER_OPTION;
    TAOS_RETURN(code);
  }

  if (pOld->numOfColumns == 2) {
    code = TSDB_CODE_PAR_INVALID_DROP_COL;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pColumns[col].colId;
  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  int32_t sz = pNew->numOfColumns - col - 1;
  memmove(pNew->pColumns + col, pNew->pColumns + col + 1, sizeof(SSchema) * sz);
  memmove(pNew->pCmpr + col, pNew->pCmpr + col + 1, sizeof(SColCmpr) * sz);
  pNew->numOfColumns--;

  pNew->colVer++;
  mInfo("stb:%s, start to drop col %s", pNew->name, colName);
  TAOS_RETURN(code);
}

static int32_t mndAlterStbColumnBytes(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, const SField *pField) {
  int32_t code = 0;
  int32_t col = mndFindSuperTableColumnIndex(pOld, pField->name);
  if (col < 0) {
    code = TSDB_CODE_MND_COLUMN_NOT_EXIST;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pColumns[col].colId;

  uint32_t nLen = 0;
  for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
    nLen += (pOld->pColumns[i].colId == colId) ? pField->bytes : pOld->pColumns[i].bytes;
  }

  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    code = TSDB_CODE_MND_INVALID_ROW_BYTES;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId));

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  SSchema *pCol = pNew->pColumns + col;
  if (!(pCol->type == TSDB_DATA_TYPE_BINARY || pCol->type == TSDB_DATA_TYPE_VARBINARY ||
        pCol->type == TSDB_DATA_TYPE_NCHAR || pCol->type == TSDB_DATA_TYPE_GEOMETRY)) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  if (pField->bytes <= pCol->bytes) {
    code = TSDB_CODE_MND_INVALID_ROW_BYTES;
    TAOS_RETURN(code);
  }

  pCol->bytes = pField->bytes;
  pNew->colVer++;

  mInfo("stb:%s, start to modify col len %s to %d", pNew->name, pField->name, pField->bytes);
  TAOS_RETURN(code);
}

static int32_t mndSetAlterStbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) {
    sdbFreeRaw(pRedoRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetAlterStbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetAlterStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb, void *alterOriData,
                                         int32_t alterOriDataLen) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, alterOriData, alterOriDataLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_ALTER_STB;
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetAlterStbRedoActions2(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb,
                                          void *alterOriData, int32_t alterOriDataLen) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, alterOriData, alterOriDataLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_CREATE_INDEX;
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}
static int32_t mndBuildStbSchemaImp(SDbObj *pDb, SStbObj *pStb, const char *tbName, STableMetaRsp *pRsp) {
  int32_t code = 0;
  taosRLockLatch(&pStb->lock);

  int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  pRsp->pSchemas = taosMemoryCalloc(totalCols, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    taosRUnLockLatch(&pStb->lock);
    code = terrno;
    TAOS_RETURN(code);
  }
  pRsp->pSchemaExt = taosMemoryCalloc(pStb->numOfColumns, sizeof(SSchemaExt));
  if (pRsp->pSchemaExt == NULL) {
    taosRUnLockLatch(&pStb->lock);
    code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(pRsp->dbFName, pStb->db, sizeof(pRsp->dbFName));
  tstrncpy(pRsp->tbName, tbName, sizeof(pRsp->tbName));
  tstrncpy(pRsp->stbName, tbName, sizeof(pRsp->stbName));
  pRsp->dbId = pDb->uid;
  pRsp->numOfTags = pStb->numOfTags;
  pRsp->numOfColumns = pStb->numOfColumns;
  pRsp->precision = pDb->cfg.precision;
  pRsp->tableType = TSDB_SUPER_TABLE;
  pRsp->sversion = pStb->colVer;
  pRsp->tversion = pStb->tagVer;
  pRsp->suid = pStb->uid;
  pRsp->tuid = pStb->uid;

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    SSchema *pSrcSchema = &pStb->pColumns[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->flags = pSrcSchema->flags;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i + pStb->numOfColumns];
    SSchema *pSrcSchema = &pStb->pTags[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->flags = pSrcSchema->flags;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }
  for (int32_t i = 0; i < pStb->numOfColumns; i++) {
    SColCmpr   *pCmpr = &pStb->pCmpr[i];
    SSchemaExt *pSchEx = &pRsp->pSchemaExt[i];
    pSchEx->colId = pCmpr->id;
    pSchEx->compress = pCmpr->alg;
  }

  taosRUnLockLatch(&pStb->lock);
  TAOS_RETURN(code);
}

static int32_t mndBuildStbCfgImp(SDbObj *pDb, SStbObj *pStb, const char *tbName, STableCfgRsp *pRsp) {
  int32_t code = 0;
  taosRLockLatch(&pStb->lock);

  int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  pRsp->pSchemas = taosMemoryCalloc(totalCols, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    taosRUnLockLatch(&pStb->lock);
    code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(pRsp->dbFName, pStb->db, sizeof(pRsp->dbFName));
  tstrncpy(pRsp->tbName, tbName, sizeof(pRsp->tbName));
  tstrncpy(pRsp->stbName, tbName, sizeof(pRsp->stbName));
  pRsp->numOfTags = pStb->numOfTags;
  pRsp->numOfColumns = pStb->numOfColumns;
  pRsp->tableType = TSDB_SUPER_TABLE;
  pRsp->delay1 = pStb->maxdelay[0];
  pRsp->delay2 = pStb->maxdelay[1];
  pRsp->watermark1 = pStb->watermark[0];
  pRsp->watermark2 = pStb->watermark[1];
  pRsp->ttl = pStb->ttl;
  pRsp->commentLen = pStb->commentLen;
  if (pStb->commentLen > 0) {
    pRsp->pComment = taosStrdup(pStb->comment);
  }

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    SSchema *pSrcSchema = &pStb->pColumns[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->flags = pSrcSchema->flags;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i + pStb->numOfColumns];
    SSchema *pSrcSchema = &pStb->pTags[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->flags = pSrcSchema->flags;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }

  if (pStb->numOfFuncs > 0) {
    pRsp->pFuncs = taosArrayDup(pStb->pFuncs, NULL);
  }

  pRsp->pSchemaExt = taosMemoryCalloc(pStb->numOfColumns, sizeof(SSchemaExt));
  for (int32_t i = 0; i < pStb->numOfColumns; i++) {
    SColCmpr *pCmpr = &pStb->pCmpr[i];

    SSchemaExt *pSchExt = &pRsp->pSchemaExt[i];
    pSchExt->colId = pCmpr->id;
    pSchExt->compress = pCmpr->alg;
  }

  taosRUnLockLatch(&pStb->lock);
  TAOS_RETURN(code);
}

static int32_t mndValidateStbVersion(SMnode *pMnode, SSTableVersion *pStbVer, bool *schema, bool *sma) {
  int32_t code = 0;
  char    tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(tbFName, sizeof(tbFName), "%s.%s", pStbVer->dbFName, pStbVer->stbName);

  SDbObj *pDb = mndAcquireDb(pMnode, pStbVer->dbFName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    TAOS_RETURN(code);
  }

  if (pDb->uid != pStbVer->dbId) {
    mndReleaseDb(pMnode, pDb);
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    TAOS_RETURN(code);
  }

  SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }

  taosRLockLatch(&pStb->lock);

  if (pStbVer->sversion != pStb->colVer || pStbVer->tversion != pStb->tagVer) {
    *schema = true;
  } else {
    *schema = false;
  }

  if (pStbVer->smaVer && pStbVer->smaVer != pStb->smaVer) {
    *sma = true;
  } else {
    *sma = false;
  }

  taosRUnLockLatch(&pStb->lock);

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndBuildStbSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  int32_t code = 0;
  char    tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(tbFName, sizeof(tbFName), "%s.%s", dbFName, tbName);

  SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    TAOS_RETURN(code);
  }

  SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }

  code = mndBuildStbSchemaImp(pDb, pStb, tbName, pRsp);
  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  TAOS_RETURN(code);
}

static int32_t mndBuildStbCfg(SMnode *pMnode, const char *dbFName, const char *tbName, STableCfgRsp *pRsp) {
  int32_t code = 0;
  char    tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(tbFName, sizeof(tbFName), "%s.%s", dbFName, tbName);

  SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    TAOS_RETURN(code);
  }

  SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }

  code = mndBuildStbCfgImp(pDb, pStb, tbName, pRsp);

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  TAOS_RETURN(code);
}

static int32_t mndBuildSMAlterStbRsp(SDbObj *pDb, SStbObj *pObj, void **pCont, int32_t *pLen) {
  int32_t       code = 0;
  SEncoder      ec = {0};
  uint32_t      contLen = 0;
  SMAlterStbRsp alterRsp = {0};
  SName         name = {0};
  TAOS_CHECK_RETURN(tNameFromString(&name, pObj->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

  alterRsp.pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == alterRsp.pMeta) {
    code = terrno;
    TAOS_RETURN(code);
  }

  code = mndBuildStbSchemaImp(pDb, pObj, name.tname, alterRsp.pMeta);
  if (code) {
    tFreeSMAlterStbRsp(&alterRsp);
    return code;
  }

  tEncodeSize(tEncodeSMAlterStbRsp, &alterRsp, contLen, code);
  if (code) {
    tFreeSMAlterStbRsp(&alterRsp);
    return code;
  }

  void *cont = taosMemoryMalloc(contLen);
  if (NULL == cont) {
    code = terrno;
    tFreeSMAlterStbRsp(&alterRsp);
    TAOS_RETURN(code);
  }
  tEncoderInit(&ec, cont, contLen);
  code = tEncodeSMAlterStbRsp(&ec, &alterRsp);
  tEncoderClear(&ec);

  tFreeSMAlterStbRsp(&alterRsp);

  if (code < 0) TAOS_RETURN(code);

  *pCont = cont;
  *pLen = contLen;

  TAOS_RETURN(code);
}

int32_t mndBuildSMCreateStbRsp(SMnode *pMnode, char *dbFName, char *stbFName, void **pCont, int32_t *pLen) {
  int32_t code = -1;
  SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
  if (NULL == pDb) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  SStbObj *pObj = mndAcquireStb(pMnode, stbFName);
  if (NULL == pObj) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  SEncoder       ec = {0};
  uint32_t       contLen = 0;
  SMCreateStbRsp stbRsp = {0};
  SName          name = {0};
  TAOS_CHECK_GOTO(tNameFromString(&name, pObj->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE), NULL, _OVER);

  stbRsp.pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == stbRsp.pMeta) {
    code = terrno;
    goto _OVER;
  }

  code = mndBuildStbSchemaImp(pDb, pObj, name.tname, stbRsp.pMeta);
  if (code) {
    tFreeSMCreateStbRsp(&stbRsp);
    goto _OVER;
  }

  tEncodeSize(tEncodeSMCreateStbRsp, &stbRsp, contLen, code);
  if (code) {
    tFreeSMCreateStbRsp(&stbRsp);
    goto _OVER;
  }

  void *cont = taosMemoryMalloc(contLen);
  if (NULL == cont) {
    code = terrno;
    tFreeSMCreateStbRsp(&stbRsp);
    goto _OVER;
  }
  tEncoderInit(&ec, cont, contLen);
  TAOS_CHECK_GOTO(tEncodeSMCreateStbRsp(&ec, &stbRsp), NULL, _OVER);
  tEncoderClear(&ec);

  tFreeSMCreateStbRsp(&stbRsp);

  *pCont = cont;
  *pLen = contLen;

  code = 0;

_OVER:
  if (pObj) {
    mndReleaseStb(pMnode, pObj);
  }

  if (pDb) {
    mndReleaseDb(pMnode, pDb);
  }

  TAOS_RETURN(code);
}

static int32_t mndAlterStbImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                              void *alterOriData, int32_t alterOriDataLen) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "alter-stb");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to alter stb:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  if (needRsp) {
    void   *pCont = NULL;
    int32_t contLen = 0;
    TAOS_CHECK_GOTO(mndBuildSMAlterStbRsp(pDb, pStb, &pCont, &contLen), NULL, _OVER);
    mndTransSetRpcRsp(pTrans, pCont, contLen);
  }

  TAOS_CHECK_GOTO(mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndAlterStbAndUpdateTagIdxImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                                             void *alterOriData, int32_t alterOriDataLen, const SMAlterStbReq *pAlter) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "alter-stb");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to alter stb:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);

  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  if (needRsp) {
    void   *pCont = NULL;
    int32_t contLen = 0;
    TAOS_CHECK_GOTO(mndBuildSMAlterStbRsp(pDb, pStb, &pCont, &contLen), NULL, _OVER);
    mndTransSetRpcRsp(pTrans, pCont, contLen);
  }

  if (pAlter->alterType == TSDB_ALTER_TABLE_DROP_TAG) {
    SIdxObj idxObj = {0};
    SField *pField0 = taosArrayGet(pAlter->pFields, 0);
    bool    exist = false;
    if (mndGetIdxsByTagName(pMnode, pStb, pField0->name, &idxObj) == 0) {
      exist = true;
    }
    TAOS_CHECK_GOTO(mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
    TAOS_CHECK_GOTO(mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);

    if (exist == true) {
      TAOS_CHECK_GOTO(mndSetDropIdxPrepareLogs(pMnode, pTrans, &idxObj), NULL, _OVER);
      TAOS_CHECK_GOTO(mndSetDropIdxCommitLogs(pMnode, pTrans, &idxObj), NULL, _OVER);
    }

    TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen), NULL, _OVER);
    TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  } else if (pAlter->alterType == TSDB_ALTER_TABLE_UPDATE_TAG_NAME) {
    SIdxObj     idxObj = {0};
    SField     *pField0 = taosArrayGet(pAlter->pFields, 0);
    SField     *pField1 = taosArrayGet(pAlter->pFields, 1);
    const char *oTagName = pField0->name;
    const char *nTagName = pField1->name;
    bool        exist = false;

    if (mndGetIdxsByTagName(pMnode, pStb, pField0->name, &idxObj) == 0) {
      exist = true;
    }

    TAOS_CHECK_GOTO(mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
    TAOS_CHECK_GOTO(mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);

    if (exist == true) {
      memcpy(idxObj.colName, nTagName, strlen(nTagName));
      idxObj.colName[strlen(nTagName)] = 0;
      TAOS_CHECK_GOTO(mndSetAlterIdxPrepareLogs(pMnode, pTrans, &idxObj), NULL, _OVER);
      TAOS_CHECK_GOTO(mndSetAlterIdxCommitLogs(pMnode, pTrans, &idxObj), NULL, _OVER);
    }

    TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen), NULL, _OVER);
    TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  }
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndAlterStb(SMnode *pMnode, SRpcMsg *pReq, const SMAlterStbReq *pAlter, SDbObj *pDb, SStbObj *pOld) {
  bool    needRsp = true;
  int32_t code = -1;
  SField *pField0 = NULL;

  SStbObj stbObj = {0};
  taosRLockLatch(&pOld->lock);
  memcpy(&stbObj, pOld, sizeof(SStbObj));
  taosRUnLockLatch(&pOld->lock);
  stbObj.pColumns = NULL;
  stbObj.pTags = NULL;
  stbObj.pFuncs = NULL;
  stbObj.pCmpr = NULL;
  stbObj.updateTime = taosGetTimestampMs();
  stbObj.lock = 0;
  bool updateTagIndex = false;
  switch (pAlter->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
      code = mndAddSuperTableTag(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_DROP_TAG:
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndDropSuperTableTag(pMnode, pOld, &stbObj, pField0->name);
      updateTagIndex = true;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
      code = mndAlterStbTagName(pMnode, pOld, &stbObj, pAlter->pFields);
      updateTagIndex = true;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndAlterStbTagBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      code = mndAddSuperTableColumn(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields, 0);
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndDropSuperTableColumn(pMnode, pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndAlterStbColumnBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      needRsp = false;
      code = mndUpdateStbCommentAndTTL(pOld, &stbObj, pAlter->comment, pAlter->commentLen, pAlter->ttl);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      code = mndUpdateSuperTableColumnCompress(pMnode, pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      code = mndAddSuperTableColumn(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields, 1);
      break;
    default:
      needRsp = false;
      terrno = TSDB_CODE_OPS_NOT_SUPPORT;
      break;
  }

  if (code != 0) goto _OVER;
  if (updateTagIndex == false) {
    code = mndAlterStbImp(pMnode, pReq, pDb, &stbObj, needRsp, pReq->pCont, pReq->contLen);
  } else {
    code = mndAlterStbAndUpdateTagIdxImp(pMnode, pReq, pDb, &stbObj, needRsp, pReq->pCont, pReq->contLen, pAlter);
  }

_OVER:
  taosMemoryFreeClear(stbObj.pTags);
  taosMemoryFreeClear(stbObj.pColumns);
  taosMemoryFreeClear(stbObj.pCmpr);
  if (pAlter->commentLen > 0) {
    taosMemoryFreeClear(stbObj.comment);
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessAlterStbReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SStbObj      *pStb = NULL;
  SMAlterStbReq alterReq = {0};

  if (tDeserializeSMAlterStbReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("stb:%s, start to alter", alterReq.name);
  if (mndCheckAlterStbReq(&alterReq) != 0) goto _OVER;

  pDb = mndAcquireDbByStb(pMnode, alterReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, alterReq.name);
  if (pStb == NULL) {
    code = TSDB_CODE_MND_STB_NOT_EXIST;
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb)) != 0) {
    goto _OVER;
  }

  code = mndAlterStb(pMnode, pReq, &alterReq, pDb, pStb);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName   name = {0};
  int32_t ret = 0;
  if ((ret = tNameFromString(&name, alterReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0)
    mError("stb:%s, failed to tNameFromString since %s", alterReq.name, tstrerror(ret));

  auditRecord(pReq, pMnode->clusterId, "alterStb", name.dbname, name.tname, alterReq.sql, alterReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to alter since %s", alterReq.name, tstrerror(code));
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  tFreeSMAltertbReq(&alterReq);

  TAOS_RETURN(code);
}

static int32_t mndSetDropStbPrepareLogs(SMnode *pMnode, STrans *pTrans, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) {
    sdbFreeRaw(pRedoRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  TAOS_RETURN(code);
}

static int32_t mndSetDropStbCommitLogs(SMnode *pMnode, STrans *pTrans, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));

  TAOS_RETURN(code);
}

static int32_t mndSetDropStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_RETURN(code);
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_STB;
    action.acceptableCode = TSDB_CODE_TDB_STB_NOT_EXIST;
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

static int32_t mndDropStb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-stb");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to drop stb:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  TAOS_CHECK_GOTO(mndSetDropStbPrepareLogs(pMnode, pTrans, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropStbCommitLogs(pMnode, pTrans, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropStbRedoActions(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDropIdxsByStb(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDropSmasByStb(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndUserRemoveStb(pMnode, pTrans, pStb->name), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndCheckDropStbForTopic(SMnode *pMnode, const char *stbFullName, int64_t suid) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SMqTopicObj *pTopic = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) break;

    if (pTopic->subType == TOPIC_SUB_TYPE__TABLE) {
      if (pTopic->stbUid == suid) {
        sdbRelease(pSdb, pTopic);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(-1);
      }
    }

    if (pTopic->ast == NULL) {
      sdbRelease(pSdb, pTopic);
      continue;
    }

    SNode *pAst = NULL;
    if (nodesStringToNode(pTopic->ast, &pAst) != 0) {
      code = TSDB_CODE_MND_INVALID_TOPIC_OPTION;
      mError("topic:%s, create ast error", pTopic->name);
      sdbRelease(pSdb, pTopic);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    SNodeList *pNodeList = NULL;
    if ((code = nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList)) !=
        0) {
      sdbRelease(pSdb, pTopic);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;

      if (pCol->tableId == suid) {
        sdbRelease(pSdb, pTopic);
        nodesDestroyNode(pAst);
        nodesDestroyList(pNodeList);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(-1);
      } else {
        goto NEXT;
      }
    }
  NEXT:
    sdbRelease(pSdb, pTopic);
    nodesDestroyNode(pAst);
    nodesDestroyList(pNodeList);
  }
  TAOS_RETURN(code);
}

static int32_t mndCheckDropStbForStream(SMnode *pMnode, const char *stbFullName, int64_t suid) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->targetStbUid == suid) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pStream);
      TAOS_RETURN(-1);
    }

    SNode *pAst = NULL;
    if (nodesStringToNode(pStream->ast, &pAst) != 0) {
      code = TSDB_CODE_MND_INVALID_STREAM_OPTION;
      mError("stream:%s, create ast error", pStream->name);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pStream);
      TAOS_RETURN(code);
    }

    SNodeList *pNodeList = NULL;
    if ((code = nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList)) !=
        0) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pStream);
      TAOS_RETURN(code);
    }
    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;

      if (pCol->tableId == suid) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pStream);
        nodesDestroyNode(pAst);
        nodesDestroyList(pNodeList);
        TAOS_RETURN(-1);
      } else {
        goto NEXT;
      }
    }
  NEXT:
    sdbRelease(pSdb, pStream);
    nodesDestroyNode(pAst);
    nodesDestroyList(pNodeList);
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessDropTtltbRsp(SRpcMsg *pRsp) { return 0; }
static int32_t mndProcessTrimDbRsp(SRpcMsg *pRsp) { return 0; }
static int32_t mndProcessS3MigrateDbRsp(SRpcMsg *pRsp) { return 0; }

static int32_t mndProcessDropStbReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SStbObj     *pStb = NULL;
  SMDropStbReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropStbReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("stb:%s, start to drop", dropReq.name);

  pStb = mndAcquireStb(pMnode, dropReq.name);
  if (pStb == NULL) {
    if (dropReq.igNotExists) {
      mInfo("stb:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto _OVER;
    } else {
      code = TSDB_CODE_MND_STB_NOT_EXIST;
      goto _OVER;
    }
  }

  if ((dropReq.source == TD_REQ_FROM_TAOX_OLD || dropReq.source == TD_REQ_FROM_TAOX) && pStb->uid != dropReq.suid) {
    code = 0;
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, dropReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb)) != 0) {
    goto _OVER;
  }

  if (mndCheckDropStbForTopic(pMnode, dropReq.name, pStb->uid) < 0) {
    code = TSDB_CODE_MND_TOPIC_MUST_BE_DELETED;
    goto _OVER;
  }

  if (mndCheckDropStbForStream(pMnode, dropReq.name, pStb->uid) < 0) {
    code = TSDB_CODE_MND_STREAM_MUST_BE_DELETED;
    goto _OVER;
  }

  code = mndDropStb(pMnode, pReq, pDb, pStb);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName   name = {0};
  int32_t ret = 0;
  if ((ret = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0)
    mError("stb:%s, failed to tNameFromString since %s", dropReq.name, tstrerror(ret));

  auditRecord(pReq, pMnode->clusterId, "dropStb", name.dbname, name.tname, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to drop since %s", dropReq.name, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  tFreeSMDropStbReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessTableMetaReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  STableInfoReq infoReq = {0};
  STableMetaRsp metaRsp = {0};
  SUserObj     *pUser = NULL;

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pUser);
  if (pUser == NULL) return 0;
  bool sysinfo = pUser->sysInfo;

  TAOS_CHECK_GOTO(tDeserializeSTableInfoReq(pReq->pCont, pReq->contLen, &infoReq), NULL, _OVER);

  if (0 == strcmp(infoReq.dbFName, TSDB_INFORMATION_SCHEMA_DB)) {
    mInfo("information_schema table:%s.%s, start to retrieve meta", infoReq.dbFName, infoReq.tbName);
    TAOS_CHECK_GOTO(mndBuildInsTableSchema(pMnode, infoReq.dbFName, infoReq.tbName, sysinfo, &metaRsp), NULL, _OVER);
  } else if (0 == strcmp(infoReq.dbFName, TSDB_PERFORMANCE_SCHEMA_DB)) {
    mInfo("performance_schema table:%s.%s, start to retrieve meta", infoReq.dbFName, infoReq.tbName);
    TAOS_CHECK_GOTO(mndBuildPerfsTableSchema(pMnode, infoReq.dbFName, infoReq.tbName, &metaRsp), NULL, _OVER);
  } else {
    mInfo("stb:%s.%s, start to retrieve meta", infoReq.dbFName, infoReq.tbName);
    TAOS_CHECK_GOTO(mndBuildStbSchema(pMnode, infoReq.dbFName, infoReq.tbName, &metaRsp), NULL, _OVER);
  }

  int32_t rspLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  if (rspLen < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }

  if ((rspLen = tSerializeSTableMetaRsp(pRsp, rspLen, &metaRsp)) < 0) {
    code = rspLen;
    goto _OVER;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;
  code = 0;

  mTrace("%s.%s, meta is retrieved", infoReq.dbFName, infoReq.tbName);

_OVER:
  if (code != 0) {
    mError("stb:%s.%s, failed to retrieve meta since %s", infoReq.dbFName, infoReq.tbName, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  tFreeSTableMetaRsp(&metaRsp);
  // TODO change to TAOS_RETURN
  return code;
}

static int32_t mndProcessTableCfgReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  STableCfgReq cfgReq = {0};
  STableCfgRsp cfgRsp = {0};

  TAOS_CHECK_GOTO(tDeserializeSTableCfgReq(pReq->pCont, pReq->contLen, &cfgReq), NULL, _OVER);

  char dbName[TSDB_DB_NAME_LEN] = {0};
  TAOS_CHECK_GOTO(mndExtractShortDbNameFromDbFullName(cfgReq.dbFName, dbName), NULL, _OVER);
  if (0 == strcmp(dbName, TSDB_INFORMATION_SCHEMA_DB)) {
    mInfo("information_schema table:%s.%s, start to retrieve cfg", cfgReq.dbFName, cfgReq.tbName);
    TAOS_CHECK_GOTO(mndBuildInsTableCfg(pMnode, cfgReq.dbFName, cfgReq.tbName, &cfgRsp), NULL, _OVER);
  } else if (0 == strcmp(dbName, TSDB_PERFORMANCE_SCHEMA_DB)) {
    mInfo("performance_schema table:%s.%s, start to retrieve cfg", cfgReq.dbFName, cfgReq.tbName);
    TAOS_CHECK_GOTO(mndBuildPerfsTableCfg(pMnode, cfgReq.dbFName, cfgReq.tbName, &cfgRsp), NULL, _OVER);
  } else {
    mInfo("stb:%s.%s, start to retrieve cfg", cfgReq.dbFName, cfgReq.tbName);
    TAOS_CHECK_GOTO(mndBuildStbCfg(pMnode, cfgReq.dbFName, cfgReq.tbName, &cfgRsp), NULL, _OVER);
  }

  int32_t rspLen = tSerializeSTableCfgRsp(NULL, 0, &cfgRsp);
  if (rspLen < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }

  if ((rspLen = tSerializeSTableCfgRsp(pRsp, rspLen, &cfgRsp)) < 0) {
    code = rspLen;
    goto _OVER;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;
  code = 0;

  mTrace("%s.%s, cfg is retrieved", cfgReq.dbFName, cfgReq.tbName);

_OVER:
  if (code != 0) {
    mError("stb:%s.%s, failed to retrieve cfg since %s", cfgReq.dbFName, cfgReq.tbName, tstrerror(code));
  }

  tFreeSTableCfgRsp(&cfgRsp);
  TAOS_RETURN(code);
}

int32_t mndValidateStbInfo(SMnode *pMnode, SSTableVersion *pStbVersions, int32_t numOfStbs, void **ppRsp,
                           int32_t *pRspLen) {
  int32_t   code = 0;
  SSTbHbRsp hbRsp = {0};
  hbRsp.pMetaRsp = taosArrayInit(numOfStbs, sizeof(STableMetaRsp));
  if (hbRsp.pMetaRsp == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  hbRsp.pIndexRsp = taosArrayInit(numOfStbs, sizeof(STableIndexRsp));
  if (NULL == hbRsp.pIndexRsp) {
    taosArrayDestroy(hbRsp.pMetaRsp);
    code = terrno;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < numOfStbs; ++i) {
    SSTableVersion *pStbVersion = &pStbVersions[i];
    pStbVersion->suid = be64toh(pStbVersion->suid);
    pStbVersion->sversion = ntohl(pStbVersion->sversion);
    pStbVersion->tversion = ntohl(pStbVersion->tversion);
    pStbVersion->smaVer = ntohl(pStbVersion->smaVer);

    bool    schema = false;
    bool    sma = false;
    int32_t code = mndValidateStbVersion(pMnode, pStbVersion, &schema, &sma);
    if (TSDB_CODE_SUCCESS != code) {
      STableMetaRsp metaRsp = {0};
      metaRsp.numOfColumns = -1;
      metaRsp.suid = pStbVersion->suid;
      tstrncpy(metaRsp.dbFName, pStbVersion->dbFName, sizeof(metaRsp.dbFName));
      tstrncpy(metaRsp.tbName, pStbVersion->stbName, sizeof(metaRsp.tbName));
      tstrncpy(metaRsp.stbName, pStbVersion->stbName, sizeof(metaRsp.stbName));
      if (taosArrayPush(hbRsp.pMetaRsp, &metaRsp) == NULL) {
        code = terrno;
        return code;
      }
      continue;
    }

    if (schema) {
      STableMetaRsp metaRsp = {0};
      mInfo("stb:%s.%s, start to retrieve meta", pStbVersion->dbFName, pStbVersion->stbName);
      if (mndBuildStbSchema(pMnode, pStbVersion->dbFName, pStbVersion->stbName, &metaRsp) != 0) {
        metaRsp.numOfColumns = -1;
        metaRsp.suid = pStbVersion->suid;
        tstrncpy(metaRsp.dbFName, pStbVersion->dbFName, sizeof(metaRsp.dbFName));
        tstrncpy(metaRsp.tbName, pStbVersion->stbName, sizeof(metaRsp.tbName));
        tstrncpy(metaRsp.stbName, pStbVersion->stbName, sizeof(metaRsp.stbName));
        if (taosArrayPush(hbRsp.pMetaRsp, &metaRsp) == NULL) {
          code = terrno;
          return code;
        }
        continue;
      }

      if (taosArrayPush(hbRsp.pMetaRsp, &metaRsp) == NULL) {
        code = terrno;
        return code;
      }
    }

    if (sma) {
      bool           exist = false;
      char           tbFName[TSDB_TABLE_FNAME_LEN];
      STableIndexRsp indexRsp = {0};
      indexRsp.pIndex = taosArrayInit(10, sizeof(STableIndexInfo));
      if (NULL == indexRsp.pIndex) {
        code = terrno;
        TAOS_RETURN(code);
      }

      sprintf(tbFName, "%s.%s", pStbVersion->dbFName, pStbVersion->stbName);
      int32_t code = mndGetTableSma(pMnode, tbFName, &indexRsp, &exist);
      if (code || !exist) {
        indexRsp.suid = pStbVersion->suid;
        indexRsp.version = -1;
        indexRsp.pIndex = NULL;
      }

      strcpy(indexRsp.dbFName, pStbVersion->dbFName);
      strcpy(indexRsp.tbName, pStbVersion->stbName);

      if (taosArrayPush(hbRsp.pIndexRsp, &indexRsp) == NULL) {
        code = terrno;
        return code;
      }
    }
  }

  int32_t rspLen = tSerializeSSTbHbRsp(NULL, 0, &hbRsp);
  if (rspLen < 0) {
    tFreeSSTbHbRsp(&hbRsp);
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  void *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    tFreeSSTbHbRsp(&hbRsp);
    code = terrno;
    TAOS_RETURN(code);
  }

  rspLen = tSerializeSSTbHbRsp(pRsp, rspLen, &hbRsp);
  tFreeSSTbHbRsp(&hbRsp);
  if (rspLen < 0) return rspLen;
  *ppRsp = pRsp;
  *pRspLen = rspLen;
  TAOS_RETURN(code);
}

int32_t mndGetNumOfStbs(SMnode *pMnode, char *dbName, int32_t *pNumOfStbs) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    TAOS_RETURN(code);
  }

  int32_t numOfStbs = 0;
  void   *pIter = NULL;
  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->dbUid == pDb->uid) {
      numOfStbs++;
    }

    sdbRelease(pSdb, pStb);
  }

  *pNumOfStbs = numOfStbs;
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndExtractDbNameFromStbFullName(const char *stbFullName, char *dst) {
  SName name = {0};
  TAOS_CHECK_RETURN(tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

  TAOS_CHECK_RETURN(tNameGetFullDbName(&name, dst));

  return 0;
}

int32_t mndExtractShortDbNameFromStbFullName(const char *stbFullName, char *dst) {
  SName name = {0};
  TAOS_CHECK_RETURN(tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

  TAOS_CHECK_RETURN(tNameGetDbName(&name, dst));

  return 0;
}

int32_t mndExtractShortDbNameFromDbFullName(const char *stbFullName, char *dst) {
  SName name = {0};
  TAOS_CHECK_RETURN(tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB));

  TAOS_CHECK_RETURN(tNameGetDbName(&name, dst));

  return 0;
}

void mndExtractTbNameFromStbFullName(const char *stbFullName, char *dst, int32_t dstSize) {
  int32_t pos = -1;
  int32_t num = 0;
  for (pos = 0; stbFullName[pos] != 0; ++pos) {
    if (stbFullName[pos] == TS_PATH_DELIMITER[0]) num++;
    if (num == 2) break;
  }

  if (num == 2) {
    tstrncpy(dst, stbFullName + pos + 1, dstSize);
  }
}

// static int32_t mndProcessRetrieveStbReq(SRpcMsg *pReq) {
//   SMnode    *pMnode = pReq->info.node;
//   SShowMgmt *pMgmt = &pMnode->showMgmt;
//   SShowObj  *pShow = NULL;
//   int32_t    rowsToRead = SHOW_STEP_SIZE;
//   int32_t    rowsRead = 0;
//
//   SRetrieveTableReq retrieveReq = {0};
//   if (tDeserializeSRetrieveTableReq(pReq->pCont, pReq->contLen, &retrieveReq) != 0) {
//     terrno = TSDB_CODE_INVALID_MSG;
//     return -1;
//   }
//
//   SMnode    *pMnode = pReq->info.node;
//   SSdb      *pSdb = pMnode->pSdb;
//   int32_t    numOfRows = 0;
//   SDbObj    *pDb = NULL;
//   ESdbStatus objStatus = 0;
//
//   SUserObj *pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
//   if (pUser == NULL) return 0;
//   bool sysinfo = pUser->sysInfo;
//
//   // Append the information_schema database into the result.
////  if (!pShow->sysDbRsp) {
////    SDbObj infoschemaDb = {0};
////    setInformationSchemaDbCfg(pMnode, &infoschemaDb);
////    size_t numOfTables = 0;
////    getVisibleInfosTablesNum(sysinfo, &numOfTables);
////    mndDumpDbInfoData(pMnode, pBlock, &infoschemaDb, pShow, numOfRows, numOfTables, true, 0, 1);
////
////    numOfRows += 1;
////
////    SDbObj perfschemaDb = {0};
////    setPerfSchemaDbCfg(pMnode, &perfschemaDb);
////    numOfTables = 0;
////    getPerfDbMeta(NULL, &numOfTables);
////    mndDumpDbInfoData(pMnode, pBlock, &perfschemaDb, pShow, numOfRows, numOfTables, true, 0, 1);
////
////    numOfRows += 1;
////    pShow->sysDbRsp = true;
////  }
//
//  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_COLS);
//  blockDataEnsureCapacity(p, rowsToRead);
//
//  size_t               size = 0;
//  const SSysTableMeta* pSysDbTableMeta = NULL;
//
//  getInfosDbMeta(&pSysDbTableMeta, &size);
//  p->info.rows = buildDbColsInfoBlock(sysinfo, p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB);
//
//  getPerfDbMeta(&pSysDbTableMeta, &size);
//  p->info.rows = buildDbColsInfoBlock(sysinfo, p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB);
//
//  blockDataDestroy(p);
//
//
//  while (numOfRows < rowsToRead) {
//    pShow->pIter = sdbFetchAll(pSdb, SDB_DB, pShow->pIter, (void **)&pDb, &objStatus, true);
//    if (pShow->pIter == NULL) break;
//    if (strncmp(retrieveReq.db, pDb->name, strlen(retrieveReq.db)) != 0){
//      continue;
//    }
//    if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_OR_WRITE_DB, pDb) != 0) {
//      continue;
//    }
//
//    while (numOfRows < rowsToRead) {
//      pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
//      if (pShow->pIter == NULL) break;
//
//      if (pDb != NULL && pStb->dbUid != pDb->uid) {
//        sdbRelease(pSdb, pStb);
//        continue;
//      }
//
//      cols = 0;
//
//      SName name = {0};
//      char  stbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
//      mndExtractTbNameFromStbFullName(pStb->name, &stbName[VARSTR_HEADER_SIZE], TSDB_TABLE_NAME_LEN);
//      varDataSetLen(stbName, strlen(&stbName[VARSTR_HEADER_SIZE]));
//
//      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)stbName, false);
//
//      char db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
//      tNameFromString(&name, pStb->db, T_NAME_ACCT | T_NAME_DB);
//      tNameGetDbName(&name, varDataVal(db));
//      varDataSetLen(db, strlen(varDataVal(db)));
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)db, false);
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->createdTime, false);
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->numOfColumns, false);
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->numOfTags, false);
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->updateTime, false);  // number of tables
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      if (pStb->commentLen > 0) {
//        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
//        STR_TO_VARSTR(comment, pStb->comment);
//        colDataSetVal(pColInfo, numOfRows, comment, false);
//      } else if (pStb->commentLen == 0) {
//        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
//        STR_TO_VARSTR(comment, "");
//        colDataSetVal(pColInfo, numOfRows, comment, false);
//      } else {
//        colDataSetNULL(pColInfo, numOfRows);
//      }
//
//      char watermark[64 + VARSTR_HEADER_SIZE] = {0};
//      sprintf(varDataVal(watermark), "%" PRId64 "a,%" PRId64 "a", pStb->watermark[0], pStb->watermark[1]);
//      varDataSetLen(watermark, strlen(varDataVal(watermark)));
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)watermark, false);
//
//      char maxDelay[64 + VARSTR_HEADER_SIZE] = {0};
//      sprintf(varDataVal(maxDelay), "%" PRId64 "a,%" PRId64 "a", pStb->maxdelay[0], pStb->maxdelay[1]);
//      varDataSetLen(maxDelay, strlen(varDataVal(maxDelay)));
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)maxDelay, false);
//
//      char    rollup[160 + VARSTR_HEADER_SIZE] = {0};
//      int32_t rollupNum = (int32_t)taosArrayGetSize(pStb->pFuncs);
//      char   *sep = ", ";
//      int32_t sepLen = strlen(sep);
//      int32_t rollupLen = sizeof(rollup) - VARSTR_HEADER_SIZE - 2;
//      for (int32_t i = 0; i < rollupNum; ++i) {
//        char *funcName = taosArrayGet(pStb->pFuncs, i);
//        if (i) {
//          strncat(varDataVal(rollup), sep, rollupLen);
//          rollupLen -= sepLen;
//        }
//        strncat(varDataVal(rollup), funcName, rollupLen);
//        rollupLen -= strlen(funcName);
//      }
//      varDataSetLen(rollup, strlen(varDataVal(rollup)));
//
//      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//      colDataSetVal(pColInfo, numOfRows, (const char *)rollup, false);
//
//      numOfRows++;
//      sdbRelease(pSdb, pStb);
//    }
//
//    if (pDb != NULL) {
//      mndReleaseDb(pMnode, pDb);
//    }
//
//    sdbRelease(pSdb, pDb);
//  }
//
//  pShow->numOfRows += numOfRows;
//  mndReleaseUser(pMnode, pUser);
//
//
//
//
//
//
//
//
//  ShowRetrieveFp retrieveFp = pMgmt->retrieveFps[pShow->type];
//  if (retrieveFp == NULL) {
//    mndReleaseShowObj(pShow, false);
//    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
//    mError("show:0x%" PRIx64 ", failed to retrieve data since %s", pShow->id, terrstr());
//    return -1;
//  }
//
//  mDebug("show:0x%" PRIx64 ", start retrieve data, type:%d", pShow->id, pShow->type);
//  if (retrieveReq.user[0] != 0) {
//    memcpy(pReq->info.conn.user, retrieveReq.user, TSDB_USER_LEN);
//  } else {
//    memcpy(pReq->info.conn.user, TSDB_DEFAULT_USER, strlen(TSDB_DEFAULT_USER) + 1);
//  }
//  if (retrieveReq.db[0] && mndCheckShowPrivilege(pMnode, pReq->info.conn.user, pShow->type, retrieveReq.db) != 0) {
//    return -1;
//  }
//
//  int32_t numOfCols = pShow->pMeta->numOfColumns;
//
//  SSDataBlock *pBlock = createDataBlock();
//  for (int32_t i = 0; i < numOfCols; ++i) {
//    SColumnInfoData idata = {0};
//
//    SSchema *p = &pShow->pMeta->pSchemas[i];
//
//    idata.info.bytes = p->bytes;
//    idata.info.type = p->type;
//    idata.info.colId = p->colId;
//    blockDataAppendColInfo(pBlock, &idata);
//  }
//
//  blockDataEnsureCapacity(pBlock, rowsToRead);
//
//  if (mndCheckRetrieveFinished(pShow)) {
//    mDebug("show:0x%" PRIx64 ", read finished, numOfRows:%d", pShow->id, pShow->numOfRows);
//    rowsRead = 0;
//  } else {
//    rowsRead = (*retrieveFp)(pReq, pShow, pBlock, rowsToRead);
//    if (rowsRead < 0) {
//      terrno = rowsRead;
//      mDebug("show:0x%" PRIx64 ", retrieve completed", pShow->id);
//      mndReleaseShowObj(pShow, true);
//      blockDataDestroy(pBlock);
//      return -1;
//    }
//
//    pBlock->info.rows = rowsRead;
//    mDebug("show:0x%" PRIx64 ", stop retrieve data, rowsRead:%d numOfRows:%d", pShow->id, rowsRead, pShow->numOfRows);
//  }
//
//  size = sizeof(SRetrieveMetaTableRsp) + sizeof(int32_t) + sizeof(SSysTableSchema) * pShow->pMeta->numOfColumns +
//         blockDataGetSize(pBlock) + blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock));
//
//  SRetrieveMetaTableRsp *pRsp = rpcMallocCont(size);
//  if (pRsp == NULL) {
//    mndReleaseShowObj(pShow, false);
//    terrno = TSDB_CODE_OUT_OF_MEMORY;
//    mError("show:0x%" PRIx64 ", failed to retrieve data since %s", pShow->id, terrstr());
//    blockDataDestroy(pBlock);
//    return -1;
//  }
//
//  pRsp->handle = htobe64(pShow->id);
//
//  if (rowsRead > 0) {
//    char    *pStart = pRsp->data;
//    SSchema *ps = pShow->pMeta->pSchemas;
//
//    *(int32_t *)pStart = htonl(pShow->pMeta->numOfColumns);
//    pStart += sizeof(int32_t);  // number of columns
//
//    for (int32_t i = 0; i < pShow->pMeta->numOfColumns; ++i) {
//      SSysTableSchema *pSchema = (SSysTableSchema *)pStart;
//      pSchema->bytes = htonl(ps[i].bytes);
//      pSchema->colId = htons(ps[i].colId);
//      pSchema->type = ps[i].type;
//
//      pStart += sizeof(SSysTableSchema);
//    }
//
//    int32_t len = blockEncode(pBlock, pStart, pShow->pMeta->numOfColumns);
//  }
//
//  pRsp->numOfRows = htonl(rowsRead);
//  pRsp->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond time precision
//  pReq->info.rsp = pRsp;
//  pReq->info.rspLen = size;
//
//  if (rowsRead == 0 || rowsRead < rowsToRead) {
//    pRsp->completed = 1;
//    mDebug("show:0x%" PRIx64 ", retrieve completed", pShow->id);
//    mndReleaseShowObj(pShow, true);
//  } else {
//    mDebug("show:0x%" PRIx64 ", retrieve not completed yet", pShow->id);
//    mndReleaseShowObj(pShow, false);
//  }
//
//  blockDataDestroy(pBlock);
//  return TSDB_CODE_SUCCESS;
//}

static int32_t mndRetrieveStb(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SStbObj *pStb = NULL;
  int32_t  cols = 0;
  int32_t  lino = 0;
  int32_t  code = 0;

  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return terrno;
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL && pStb->dbUid != pDb->uid) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    if (isTsmaResSTb(pStb->name)) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    cols = 0;

    SName name = {0};

    char stbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    mndExtractTbNameFromStbFullName(pStb->name, &stbName[VARSTR_HEADER_SIZE], TSDB_TABLE_NAME_LEN);
    varDataSetLen(stbName, strlen(&stbName[VARSTR_HEADER_SIZE]));
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)stbName, false), pStb, &lino, _ERROR);

    char db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    RETRIEVE_CHECK_GOTO(tNameFromString(&name, pStb->db, T_NAME_ACCT | T_NAME_DB), pStb, &lino, _ERROR);
    RETRIEVE_CHECK_GOTO(tNameGetDbName(&name, varDataVal(db)), pStb, &lino, _ERROR);
    varDataSetLen(db, strlen(varDataVal(db)));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)db, false), pStb, &lino, _ERROR);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->createdTime, false), pStb, &lino,
                        _ERROR);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->numOfColumns, false), pStb, &lino,
                        _ERROR);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->numOfTags, false), pStb, &lino, _ERROR);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->updateTime, false), pStb, &lino,
                        _ERROR);  // number of tables

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pStb->commentLen > 0) {
      char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, pStb->comment);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, comment, false), pStb, &lino, _ERROR);
    } else if (pStb->commentLen == 0) {
      char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, "");
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, comment, false), pStb, &lino, _ERROR);
    } else {
      colDataSetNULL(pColInfo, numOfRows);
    }

    char watermark[64 + VARSTR_HEADER_SIZE] = {0};
    sprintf(varDataVal(watermark), "%" PRId64 "a,%" PRId64 "a", pStb->watermark[0], pStb->watermark[1]);
    varDataSetLen(watermark, strlen(varDataVal(watermark)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)watermark, false), pStb, &lino, _ERROR);

    char maxDelay[64 + VARSTR_HEADER_SIZE] = {0};
    sprintf(varDataVal(maxDelay), "%" PRId64 "a,%" PRId64 "a", pStb->maxdelay[0], pStb->maxdelay[1]);
    varDataSetLen(maxDelay, strlen(varDataVal(maxDelay)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)maxDelay, false), pStb, &lino, _ERROR);

    char    rollup[160 + VARSTR_HEADER_SIZE] = {0};
    int32_t rollupNum = (int32_t)taosArrayGetSize(pStb->pFuncs);
    char   *sep = ", ";
    int32_t sepLen = strlen(sep);
    int32_t rollupLen = sizeof(rollup) - VARSTR_HEADER_SIZE - 2;
    for (int32_t i = 0; i < rollupNum; ++i) {
      char *funcName = taosArrayGet(pStb->pFuncs, i);
      if (i) {
        (void)strncat(varDataVal(rollup), sep, rollupLen);
        rollupLen -= sepLen;
      }
      (void)strncat(varDataVal(rollup), funcName, rollupLen);
      rollupLen -= strlen(funcName);
    }
    varDataSetLen(rollup, strlen(varDataVal(rollup)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)rollup, false), pStb, &lino, _ERROR);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pColInfo) {
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)(&pStb->uid), false), pStb, &lino, _ERROR);
    }

    numOfRows++;
    sdbRelease(pSdb, pStb);
  }

  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }

  goto _OVER;

_ERROR:
  mError("show:0x%" PRIx64 ", failed to retrieve data at %s:%d since %s", pShow->id, __FUNCTION__, lino,
         tstrerror(code));

_OVER:
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static int32_t buildDbColsInfoBlock(const SSDataBlock *p, const SSysTableMeta *pSysDbTableMeta, size_t size,
                                    const char *dbName, const char *tbName) {
  char    tName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  char    dName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  char    typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;
  int32_t lino = 0;
  int32_t code = 0;

  STR_TO_VARSTR(dName, dbName);
  STR_TO_VARSTR(typeName, "SYSTEM_TABLE");

  for (int32_t i = 0; i < size; ++i) {
    const SSysTableMeta *pm = &pSysDbTableMeta[i];
    //    if (pm->sysInfo) {
    //      continue;
    //    }
    if (tbName[0] && strncmp(tbName, pm->name, TSDB_TABLE_NAME_LEN) != 0) {
      continue;
    }

    STR_TO_VARSTR(tName, pm->name);

    for (int32_t j = 0; j < pm->colNum; j++) {
      // table name
      SColumnInfoData *pColInfoData = taosArrayGet(p->pDataBlock, 0);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, tName, false), &lino, _OVER);

      // database name
      pColInfoData = taosArrayGet(p->pDataBlock, 1);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, dName, false), &lino, _OVER);

      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, typeName, false), &lino, _OVER);

      // col name
      char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(colName, pm->schema[j].name);
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, colName, false), &lino, _OVER);

      // col type
      int8_t colType = pm->schema[j].type;
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      char colTypeStr[VARSTR_HEADER_SIZE + 32];
      int  colTypeLen = sprintf(varDataVal(colTypeStr), "%s", tDataTypes[colType].name);
      if (colType == TSDB_DATA_TYPE_VARCHAR) {
        colTypeLen +=
            sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)", (int32_t)(pm->schema[j].bytes - VARSTR_HEADER_SIZE));
      } else if (colType == TSDB_DATA_TYPE_NCHAR) {
        colTypeLen += sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)",
                              (int32_t)((pm->schema[j].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
      }
      varDataSetLen(colTypeStr, colTypeLen);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, (char *)colTypeStr, false), &lino, _OVER);

      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, (const char *)&pm->schema[j].bytes, false), &lino, _OVER);
      for (int32_t k = 6; k <= 8; ++k) {
        pColInfoData = taosArrayGet(p->pDataBlock, k);
        colDataSetNULL(pColInfoData, numOfRows);
      }

      numOfRows += 1;
    }
  }
_OVER:
  mError("failed at %s:%d since %s", __FUNCTION__, lino, tstrerror(code));
  return numOfRows;
}
#define BUILD_COL_FOR_INFO_DB 1
#define BUILD_COL_FOR_PERF_DB 1 << 1
#define BUILD_COL_FOR_USER_DB 1 << 2
#define BUILD_COL_FOR_ALL_DB  (BUILD_COL_FOR_INFO_DB | BUILD_COL_FOR_PERF_DB | BUILD_COL_FOR_USER_DB)

static int32_t buildSysDbColsInfo(SSDataBlock *p, int8_t buildWhichDBs, char *tb) {
  size_t               size = 0;
  const SSysTableMeta *pSysDbTableMeta = NULL;

  if (buildWhichDBs & BUILD_COL_FOR_INFO_DB) {
    getInfosDbMeta(&pSysDbTableMeta, &size);
    p->info.rows = buildDbColsInfoBlock(p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB, tb);
  }

  if (buildWhichDBs & BUILD_COL_FOR_PERF_DB) {
    getPerfDbMeta(&pSysDbTableMeta, &size);
    p->info.rows = buildDbColsInfoBlock(p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB, tb);
  }

  return p->info.rows;
}

static int8_t determineBuildColForWhichDBs(const char *db) {
  int8_t buildWhichDBs;
  if (!db[0])
    buildWhichDBs = BUILD_COL_FOR_ALL_DB;
  else {
    char *p = strchr(db, '.');
    if (p && strcmp(p + 1, TSDB_INFORMATION_SCHEMA_DB) == 0) {
      buildWhichDBs = BUILD_COL_FOR_INFO_DB;
    } else if (p && strcmp(p + 1, TSDB_PERFORMANCE_SCHEMA_DB) == 0) {
      buildWhichDBs = BUILD_COL_FOR_PERF_DB;
    } else {
      buildWhichDBs = BUILD_COL_FOR_USER_DB;
    }
  }
  return buildWhichDBs;
}

static int32_t mndRetrieveStbCol(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  uint8_t  buildWhichDBs;
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  SStbObj *pStb = NULL;
  int32_t  numOfRows = 0;
  int32_t  lino = 0;
  int32_t  code = 0;

  buildWhichDBs = determineBuildColForWhichDBs(pShow->db);

  if (!pShow->sysDbRsp) {
    numOfRows = buildSysDbColsInfo(pBlock, buildWhichDBs, pShow->filterTb);
    mDebug("mndRetrieveStbCol get system table cols, rows:%d, db:%s", numOfRows, pShow->db);
    pShow->sysDbRsp = true;
  }

  if (buildWhichDBs & BUILD_COL_FOR_USER_DB) {
    SDbObj *pDb = NULL;
    if (strlen(pShow->db) > 0) {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL && TSDB_CODE_MND_DB_NOT_EXIST != terrno && pBlock->info.rows == 0) return terrno;
    }

    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(typeName, "SUPER_TABLE");
    bool fetch = pShow->restore ? false : true;
    pShow->restore = false;
    while (numOfRows < rows) {
      if (fetch) {
        pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
        if (pShow->pIter == NULL) break;
      } else {
        fetch = true;
        void *pKey = taosHashGetKey(pShow->pIter, NULL);
        pStb = sdbAcquire(pSdb, SDB_STB, pKey);
        if (!pStb) continue;
      }

      if (pDb != NULL && pStb->dbUid != pDb->uid) {
        sdbRelease(pSdb, pStb);
        continue;
      }

      SName name = {0};
      char  stbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      mndExtractTbNameFromStbFullName(pStb->name, &stbName[VARSTR_HEADER_SIZE], TSDB_TABLE_NAME_LEN);
      if (pShow->filterTb[0] && strncmp(pShow->filterTb, &stbName[VARSTR_HEADER_SIZE], TSDB_TABLE_NAME_LEN) != 0) {
        sdbRelease(pSdb, pStb);
        continue;
      }

      if ((numOfRows + pStb->numOfColumns) > rows) {
        pShow->restore = true;
        if (numOfRows == 0) {
          mError("mndRetrieveStbCol failed to get stable cols since buf:%d less than result:%d, stable name:%s, db:%s",
                 rows, pStb->numOfColumns, pStb->name, pStb->db);
        }
        sdbRelease(pSdb, pStb);
        break;
      }

      varDataSetLen(stbName, strlen(&stbName[VARSTR_HEADER_SIZE]));

      mDebug("mndRetrieveStbCol get stable cols, stable name:%s, db:%s", pStb->name, pStb->db);

      char db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      RETRIEVE_CHECK_GOTO(tNameFromString(&name, pStb->db, T_NAME_ACCT | T_NAME_DB), pStb, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tNameGetDbName(&name, varDataVal(db)), pStb, &lino, _OVER);
      varDataSetLen(db, strlen(varDataVal(db)));

      for (int i = 0; i < pStb->numOfColumns; i++) {
        int32_t          cols = 0;
        SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)stbName, false), pStb, &lino, _OVER);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)db, false), pStb, &lino, _OVER);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, typeName, false), pStb, &lino, _OVER);

        // col name
        char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(colName, pStb->pColumns[i].name);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, colName, false), pStb, &lino, _OVER);

        // col type
        int8_t colType = pStb->pColumns[i].type;
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        char colTypeStr[VARSTR_HEADER_SIZE + 32];
        int  colTypeLen = sprintf(varDataVal(colTypeStr), "%s", tDataTypes[colType].name);
        if (colType == TSDB_DATA_TYPE_VARCHAR) {
          colTypeLen += sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)",
                                (int32_t)(pStb->pColumns[i].bytes - VARSTR_HEADER_SIZE));
        } else if (colType == TSDB_DATA_TYPE_NCHAR) {
          colTypeLen += sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)",
                                (int32_t)((pStb->pColumns[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
        }
        varDataSetLen(colTypeStr, colTypeLen);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (char *)colTypeStr, false), pStb, &lino, _OVER);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->pColumns[i].bytes, false), pStb,
                            &lino, _OVER);
        while (cols < pShow->numOfColumns) {
          pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
          colDataSetNULL(pColInfo, numOfRows);
        }
        numOfRows++;
      }

      sdbRelease(pSdb, pStb);
    }

    if (pDb != NULL) {
      mndReleaseDb(pMnode, pDb);
    }
  }

  mDebug("mndRetrieveStbCol success, rows:%d, pShow->numOfRows:%d", numOfRows, pShow->numOfRows);
  goto _OVER;

_ERROR:
  mError("failed to mndRetrieveStbCol, rows:%d, pShow->numOfRows:%d, at %s:%d since %s", numOfRows, pShow->numOfRows,
         __FUNCTION__, lino, tstrerror(code));

_OVER:
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STB);
}

const char *mndGetStbStr(const char *src) {
  char *posDb = strstr(src, TS_PATH_DELIMITER);
  if (posDb != NULL) ++posDb;
  if (posDb == NULL) return src;

  char *posStb = strstr(posDb, TS_PATH_DELIMITER);
  if (posStb != NULL) ++posStb;
  if (posStb == NULL) return posDb;
  return posStb;
}

static int32_t mndCheckIndexReq(SCreateTagIndexReq *pReq) {
  // impl
  return TSDB_CODE_SUCCESS;
}

/*int32_t mndAddIndexImpl(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp, void *sql,
                        int32_t len) {
  // impl later
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "create-stb-index");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to add index to stb:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  if (mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb) != 0) goto _OVER;
  if (mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb) != 0) goto _OVER;
  if (mndSetAlterStbRedoActions2(pMnode, pTrans, pDb, pStb, sql, len) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  return code;

_OVER:
  mndTransDrop(pTrans);
  return code;
}
static int32_t mndAddIndex(SMnode *pMnode, SRpcMsg *pReq, SCreateTagIndexReq *tagIdxReq, SDbObj *pDb, SStbObj *pOld) {
  bool    needRsp = true;
  int32_t code = -1;
  SField *pField0 = NULL;

  SStbObj  stbObj = {0};
  SStbObj *pNew = &stbObj;

  taosRLockLatch(&pOld->lock);
  memcpy(&stbObj, pOld, sizeof(SStbObj));
  taosRUnLockLatch(&pOld->lock);

  stbObj.pColumns = NULL;
  stbObj.pTags = NULL;
  stbObj.updateTime = taosGetTimestampMs();
  stbObj.lock = 0;

  int32_t tag = mndFindSuperTableTagIndex(pOld, tagIdxReq->colName);
  if (tag < 0) {
    terrno = TSDB_CODE_MND_TAG_NOT_EXIST;
    return -1;
  }
  col_id_t colId = pOld->pTags[tag].colId;
  if (mndCheckColAndTagModifiable(pMnode, pOld->name, pOld->uid, colId) != 0) {
    return -1;
  }
  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  SSchema *pTag = pNew->pTags + tag;
  if (IS_IDX_ON(pTag)) {
    terrno = TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST;
    return -1;
  } else {
    pTag->flags |= COL_IDX_ON;
  }
  pNew->tagVer++;

  code = mndAddIndexImpl(pMnode, pReq, pDb, pNew, needRsp, pReq->pCont, pReq->contLen);

  return code;
}
static int32_t mndProcessCreateIndexReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SDbObj            *pDb = NULL;
  SStbObj           *pStb = NULL;
  SCreateTagIndexReq tagIdxReq = {0};

  if (tDeserializeSCreateTagIdxReq(pReq->pCont, pReq->contLen, &tagIdxReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("stb:%s, start to alter", tagIdxReq.stbName);

  if (mndCheckIndexReq(&tagIdxReq) != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, tagIdxReq.dbFName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, tagIdxReq.stbName);
  if (pStb == NULL) {
    terrno = TSDB_CODE_MND_STB_NOT_EXIST;
    goto _OVER;
  }
  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb) != 0) {
    goto _OVER;
  }

  code = mndAddIndex(pMnode, pReq, &tagIdxReq, pDb, pStb);
  if (terrno == TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST || terrno == TSDB_CODE_MND_TAG_NOT_EXIST) {
    return terrno;
  } else {
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to create index since %s", tagIdxReq.stbName, terrstr());
  }
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  return code;
}
static int32_t mndProcessDropIndexReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SDbObj          *pDb = NULL;
  SStbObj         *pStb = NULL;
  SDropTagIndexReq dropReq = {0};
  if (tDeserializeSDropTagIdxReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }
  //
  return TSDB_CODE_SUCCESS;
_OVER:
  return code;
}*/

static int32_t mndProcessDropStbReqFromMNode(SRpcMsg *pReq) {
  int32_t code = mndProcessDropStbReq(pReq);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    pReq->info.rsp = rpcMallocCont(1);
    pReq->info.rspLen = 1;
    pReq->info.noResp = false;
    pReq->code = code;
  }
  return code;
}

typedef struct SVDropTbVgReqs {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
} SVDropTbVgReqs;

typedef struct SMDropTbDbInfo {
  SArray *dbVgInfos;
  int32_t hashPrefix;
  int32_t hashSuffix;
  int32_t hashMethod;
} SMDropTbDbInfo;

typedef struct SMDropTbTsmaInfo {
  char           tsmaResTbDbFName[TSDB_DB_FNAME_LEN];
  char           tsmaResTbNamePrefix[TSDB_TABLE_FNAME_LEN];
  int32_t        suid;
  SMDropTbDbInfo dbInfo;  // reference to DbInfo in pDbMap
} SMDropTbTsmaInfo;

typedef struct SMDropTbTsmaInfos {
  SArray *pTsmaInfos;  // SMDropTbTsmaInfo
} SMDropTbTsmaInfos;

typedef struct SMndDropTbsWithTsmaCtx {
  SHashObj *pTsmaMap;     // <suid, SMDropTbTsmaInfos>
  SHashObj *pDbMap;       // <dbuid, SMDropTbDbInfo>
  SHashObj *pVgMap;       // <vgId, SVDropTbVgReqs>
  SArray   *pResTbNames;  // SArray<char*>
} SMndDropTbsWithTsmaCtx;

static int32_t mndDropTbAddTsmaResTbsForSingleVg(SMnode *pMnode, SMndDropTbsWithTsmaCtx *pCtx, SArray *pTbs,
                                                 int32_t vgId);

static void mndDestroyDropTbsWithTsmaCtx(SMndDropTbsWithTsmaCtx *p) {
  if (!p) return;

  if (p->pDbMap) {
    void *pIter = taosHashIterate(p->pDbMap, NULL);
    while (pIter) {
      SMDropTbDbInfo *pInfo = pIter;
      taosArrayDestroy(pInfo->dbVgInfos);
      pIter = taosHashIterate(p->pDbMap, pIter);
    }
    taosHashCleanup(p->pDbMap);
  }
  if (p->pResTbNames) {
    taosArrayDestroyP(p->pResTbNames, taosMemoryFree);
  }
  if (p->pTsmaMap) {
    void *pIter = taosHashIterate(p->pTsmaMap, NULL);
    while (pIter) {
      SMDropTbTsmaInfos *pInfos = pIter;
      taosArrayDestroy(pInfos->pTsmaInfos);
      pIter = taosHashIterate(p->pTsmaMap, pIter);
    }
    taosHashCleanup(p->pTsmaMap);
  }

  if (p->pVgMap) {
    void *pIter = taosHashIterate(p->pVgMap, NULL);
    while (pIter) {
      SVDropTbVgReqs *pReqs = pIter;
      taosArrayDestroy(pReqs->req.pArray);
      pIter = taosHashIterate(p->pVgMap, pIter);
    }
    taosHashCleanup(p->pVgMap);
  }
  taosMemoryFree(p);
}

static int32_t mndInitDropTbsWithTsmaCtx(SMndDropTbsWithTsmaCtx **ppCtx) {
  int32_t                 code = 0;
  SMndDropTbsWithTsmaCtx *pCtx = taosMemoryCalloc(1, sizeof(SMndDropTbsWithTsmaCtx));
  if (!pCtx) return terrno;
  pCtx->pTsmaMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (!pCtx->pTsmaMap) {
    code = terrno;
    goto _end;
  }

  pCtx->pDbMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (!pCtx->pDbMap) {
    code = terrno;
    goto _end;
  }
  pCtx->pResTbNames = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);

  pCtx->pVgMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (!pCtx->pVgMap) {
    code = terrno;
    goto _end;
  }
  *ppCtx = pCtx;
_end:
  if (code) mndDestroyDropTbsWithTsmaCtx(pCtx);
  return code;
}

static void *mndBuildVDropTbsReq(SMnode *pMnode, const SVgroupInfo *pVgInfo, const SVDropTbBatchReq *pReq,
                                 int32_t *len) {
  int32_t   contLen = 0;
  int32_t   ret = 0;
  SMsgHead *pHead = NULL;
  SEncoder  encoder = {0};

  tEncodeSize(tEncodeSVDropTbBatchReq, pReq, contLen, ret);
  if (ret < 0) return NULL;

  contLen += sizeof(SMsgHead);
  pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgInfo->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));

  tEncoderInit(&encoder, pBuf, contLen - sizeof(SMsgHead));
  int32_t code = tEncodeSVDropTbBatchReq(&encoder, pReq);
  tEncoderClear(&encoder);
  if (code != 0) return NULL;

  *len = contLen;
  return pHead;
}

static int32_t mndSetDropTbsRedoActions(SMnode *pMnode, STrans *pTrans, const SVDropTbVgReqs *pVgReqs, void *pCont,
                                        int32_t contLen) {
  STransAction action = {0};
  action.epSet = pVgReqs->info.epSet;
  action.pCont = pCont;
  action.contLen = contLen;
  action.msgType = TDMT_VND_DROP_TABLE;
  action.acceptableCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
  return mndTransAppendRedoAction(pTrans, &action);
}

static int32_t mndCreateDropTbsTxnPrepare(SRpcMsg *pRsp, SMndDropTbsWithTsmaCtx *pCtx) {
  int32_t code = 0;
  SMnode *pMnode = pRsp->info.node;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pRsp, "drop-tbs");
  mndTransSetChangeless(pTrans);
  mndTransSetSerial(pTrans);
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  void *pIter = taosHashIterate(pCtx->pVgMap, NULL);
  while (pIter) {
    const SVDropTbVgReqs *pVgReqs = pIter;
    int32_t               len = 0;
    void                 *p = mndBuildVDropTbsReq(pMnode, &pVgReqs->info, &pVgReqs->req, &len);
    if (!p) {
      taosHashCancelIterate(pCtx->pVgMap, pIter);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _OVER;
    }
    if ((code = mndSetDropTbsRedoActions(pMnode, pTrans, pVgReqs, p, len)) != 0) {
      taosHashCancelIterate(pCtx->pVgMap, pIter);
      goto _OVER;
    }
    pIter = taosHashIterate(pCtx->pVgMap, pIter);
  }
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropTbWithTsma(SRpcMsg *pReq) {
  int32_t      code = -1;
  SMnode      *pMnode = pReq->info.node;
  SDbObj      *pDb = NULL;
  SStbObj     *pStb = NULL;
  SMDropTbsReq dropReq = {0};
  bool         locked = false;
  if (tDeserializeSMDropTbsReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  SMndDropTbsWithTsmaCtx *pCtx = NULL;
  code = mndInitDropTbsWithTsmaCtx(&pCtx);
  if (code) goto _OVER;
  for (int32_t i = 0; i < dropReq.pVgReqs->size; ++i) {
    SMDropTbReqsOnSingleVg *pReq = taosArrayGet(dropReq.pVgReqs, i);
    code = mndDropTbAddTsmaResTbsForSingleVg(pMnode, pCtx, pReq->pTbs, pReq->vgInfo.vgId);
    if (code) goto _OVER;
  }
  if (mndCreateDropTbsTxnPrepare(pReq, pCtx) == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }
_OVER:
  tFreeSMDropTbsReq(&dropReq);
  if (pCtx) mndDestroyDropTbsWithTsmaCtx(pCtx);
  TAOS_RETURN(code);
}

static int32_t mndDropTbAdd(SMnode *pMnode, SHashObj *pVgHashMap, const SVgroupInfo *pVgInfo, char *name, tb_uid_t suid,
                            bool ignoreNotExists) {
  SVDropTbReq req = {.name = name, .suid = suid, .igNotExists = ignoreNotExists};

  SVDropTbVgReqs *pReqs = taosHashGet(pVgHashMap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  SVDropTbVgReqs  reqs = {0};
  if (pReqs == NULL) {
    reqs.info = *pVgInfo;
    reqs.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
    if (reqs.req.pArray == NULL) {
      return terrno;
    }
    if (taosArrayPush(reqs.req.pArray, &req) == NULL) {
      return terrno;
    }
    if (taosHashPut(pVgHashMap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &reqs, sizeof(reqs)) != 0) {
      return terrno;
    }
  } else {
    if (taosArrayPush(pReqs->req.pArray, &req) == NULL) {
      return terrno;
    }
  }
  return 0;
}

int vgInfoCmp(const void *lp, const void *rp) {
  SVgroupInfo *pLeft = (SVgroupInfo *)lp;
  SVgroupInfo *pRight = (SVgroupInfo *)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

static int32_t mndGetDbVgInfoForTsma(SMnode *pMnode, const char *dbname, SMDropTbTsmaInfo *pInfo) {
  int32_t code = 0;
  SDbObj *pDb = mndAcquireDb(pMnode, dbname);
  if (!pDb) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _end;
  }

  pInfo->dbInfo.dbVgInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
  if (!pInfo->dbInfo.dbVgInfos) {
    code = terrno;
    goto _end;
  }
  mndBuildDBVgroupInfo(pDb, pMnode, pInfo->dbInfo.dbVgInfos);
  taosArraySort(pInfo->dbInfo.dbVgInfos, vgInfoCmp);

  pInfo->dbInfo.hashPrefix = pDb->cfg.hashPrefix;
  pInfo->dbInfo.hashSuffix = pDb->cfg.hashSuffix;
  pInfo->dbInfo.hashMethod = pDb->cfg.hashMethod;

_end:
  if (pDb) mndReleaseDb(pMnode, pDb);
  if (code && pInfo->dbInfo.dbVgInfos) {
    taosArrayDestroy(pInfo->dbInfo.dbVgInfos);
    pInfo->dbInfo.dbVgInfos = NULL;
  }
  TAOS_RETURN(code);
}

int32_t vgHashValCmp(const void *lp, const void *rp) {
  uint32_t    *key = (uint32_t *)lp;
  SVgroupInfo *pVg = (SVgroupInfo *)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}

static int32_t mndDropTbAddTsmaResTbsForSingleVg(SMnode *pMnode, SMndDropTbsWithTsmaCtx *pCtx, SArray *pTbs,
                                                 int32_t vgId) {
  int32_t code = 0;

  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);
  if (!pVgObj) {
    code = 0;
    goto _end;
  }
  SVgroupInfo vgInfo = {.hashBegin = pVgObj->hashBegin,
                        .hashEnd = pVgObj->hashEnd,
                        .numOfTable = pVgObj->numOfTables,
                        .vgId = pVgObj->vgId};
  vgInfo.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  mndReleaseVgroup(pMnode, pVgObj);

  // get all stb uids
  for (int32_t i = 0; i < pTbs->size; ++i) {
    const SVDropTbReq *pTb = taosArrayGet(pTbs, i);
    if (taosHashGet(pCtx->pTsmaMap, &pTb->suid, sizeof(pTb->suid))) {
    } else {
      SMDropTbTsmaInfos infos = {0};
      infos.pTsmaInfos = taosArrayInit(2, sizeof(SMDropTbTsmaInfo));
      if (!infos.pTsmaInfos) {
        code = terrno;
        goto _end;
      }
      if (taosHashPut(pCtx->pTsmaMap, &pTb->suid, sizeof(pTb->suid), &infos, sizeof(infos)) != 0) {
        code = terrno;
        goto _end;
      }
    }
  }

  void    *pIter = NULL;
  SSmaObj *pSma = NULL;
  char     buf[TSDB_TABLE_FNAME_LEN] = {0};
  // get used tsmas and it's dbs
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (!pIter) break;
    SMDropTbTsmaInfos *pInfos = taosHashGet(pCtx->pTsmaMap, &pSma->stbUid, sizeof(pSma->stbUid));
    if (pInfos) {
      SMDropTbTsmaInfo info = {0};
      int32_t          len = sprintf(buf, "%s", pSma->name);
      sprintf(info.tsmaResTbDbFName, "%s", pSma->db);
      snprintf(info.tsmaResTbNamePrefix, TSDB_TABLE_FNAME_LEN, "%s", buf);
      SMDropTbDbInfo *pDbInfo = taosHashGet(pCtx->pDbMap, pSma->db, TSDB_DB_FNAME_LEN);
      info.suid = pSma->dstTbUid;
      if (!pDbInfo) {
        code = mndGetDbVgInfoForTsma(pMnode, pSma->db, &info);
        if (code != TSDB_CODE_SUCCESS) {
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pSma);
          goto _end;
        }
        if (taosHashPut(pCtx->pDbMap, pSma->db, TSDB_DB_FNAME_LEN, &info.dbInfo, sizeof(SMDropTbDbInfo)) != 0) {
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pSma);
          goto _end;
        }
      } else {
        info.dbInfo = *pDbInfo;
      }
      if (taosArrayPush(pInfos->pTsmaInfos, &info) == NULL) {
        code = terrno;
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pSma);
        goto _end;
      }
    }
    sdbRelease(pMnode->pSdb, pSma);
  }

  // generate vg req map
  for (int32_t i = 0; i < pTbs->size; ++i) {
    SVDropTbReq *pTb = taosArrayGet(pTbs, i);
    TAOS_CHECK_GOTO(mndDropTbAdd(pMnode, pCtx->pVgMap, &vgInfo, pTb->name, pTb->suid, pTb->igNotExists), NULL, _end);

    SMDropTbTsmaInfos *pInfos = taosHashGet(pCtx->pTsmaMap, &pTb->suid, sizeof(pTb->suid));
    SArray            *pVgInfos = NULL;
    char               buf[TSDB_TABLE_FNAME_LEN + TSDB_TABLE_NAME_LEN + 1];
    char               resTbFullName[TSDB_TABLE_FNAME_LEN + 1] = {0};
    for (int32_t j = 0; j < pInfos->pTsmaInfos->size; ++j) {
      SMDropTbTsmaInfo *pInfo = taosArrayGet(pInfos->pTsmaInfos, j);
      int32_t           len = sprintf(buf, "%s_%s", pInfo->tsmaResTbNamePrefix, pTb->name);
      len = taosCreateMD5Hash(buf, len);
      len = snprintf(resTbFullName, TSDB_TABLE_FNAME_LEN + 1, "%s.%s", pInfo->tsmaResTbDbFName, buf);
      uint32_t hashVal = taosGetTbHashVal(resTbFullName, len, pInfo->dbInfo.hashMethod, pInfo->dbInfo.hashPrefix,
                                          pInfo->dbInfo.hashSuffix);
      const SVgroupInfo *pVgInfo = taosArraySearch(pInfo->dbInfo.dbVgInfos, &hashVal, vgHashValCmp, TD_EQ);
      void              *p = taosStrdup(resTbFullName + strlen(pInfo->tsmaResTbDbFName) + TSDB_NAME_DELIMITER_LEN);
      if (taosArrayPush(pCtx->pResTbNames, &p) == NULL) {
        code = terrno;
        goto _end;
      }
      TAOS_CHECK_GOTO(mndDropTbAdd(pMnode, pCtx->pVgMap, pVgInfo, p, pInfo->suid, true), NULL, _end);
    }
  }
_end:
  return code;
}

static int32_t mndProcessFetchTtlExpiredTbs(SRpcMsg *pRsp) {
  int32_t                 code = -1;
  SDecoder                decoder = {0};
  SMnode                 *pMnode = pRsp->info.node;
  SVFetchTtlExpiredTbsRsp rsp = {0};
  SMndDropTbsWithTsmaCtx *pCtx = NULL;
  if (pRsp->code != TSDB_CODE_SUCCESS) {
    code = pRsp->code;
    goto _end;
  }
  if (pRsp->contLen == 0) {
    code = 0;
    goto _end;
  }

  tDecoderInit(&decoder, pRsp->pCont, pRsp->contLen);
  code = tDecodeVFetchTtlExpiredTbsRsp(&decoder, &rsp);
  if (code) goto _end;

  code = mndInitDropTbsWithTsmaCtx(&pCtx);
  if (code) goto _end;

  code = mndDropTbAddTsmaResTbsForSingleVg(pMnode, pCtx, rsp.pExpiredTbs, rsp.vgId);
  if (code) goto _end;
  if (mndCreateDropTbsTxnPrepare(pRsp, pCtx) == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_end:
  if (pCtx) mndDestroyDropTbsWithTsmaCtx(pCtx);
  tDecoderClear(&decoder);
  tFreeFetchTtlExpiredTbsRsp(&rsp);
  TAOS_RETURN(code);
}
