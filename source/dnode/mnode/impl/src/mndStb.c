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
#include "mndRsma.h"
#include "mndSecurityPolicy.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndStream.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndTxn.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define STB_VER_SUPPORT_COMP    2
#define STB_VER_SUPPORT_VIRTUAL 3
#define STB_VER_SUPPORT_OWNER   4
#define STB_VER_SUPPORT_TXN     5
#define STB_VER_SUPPORT_INHERIT 6
#define STB_VER_NUMBER          STB_VER_SUPPORT_INHERIT
#define STB_RESERVE_SIZE        51

static int32_t  mndStbActionInsert(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionDelete(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionUpdate(SSdb *pSdb, SStbObj *pOld, SStbObj *pNew);
static int32_t  mndProcessTtlTimer(SRpcMsg *pReq);
// static int32_t  mndProcessTrimDbTimer(SRpcMsg *pReq);
static int32_t  mndProcessCreateStbReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterStbReq(SRpcMsg *pReq);
static int32_t  mndProcessDropStbReq(SRpcMsg *pReq);
static int32_t  mndMarkStbTxnDrop(SMnode *pMnode, SRpcMsg *pReq, SStbObj *pStb, SDbObj *pDb, txn_id_t txnId);
static int32_t  mndMarkStbTxnAlter(SMnode *pMnode, SRpcMsg *pReq, SStbObj *pStb, SDbObj *pDb, txn_id_t txnId,
                                   void *pReqData, int32_t reqDataLen);
static int32_t  mndApplyTxnAlterOpsToSchema(SMnode *pMnode, SArray *pAlterOps, SDbObj *pDb, SStbObj *pBaseStb,
                                            const char *tbName, STableMetaRsp *pRsp, bool refByStm);
static int32_t  mndProcessDropTtltbRsp(SRpcMsg *pReq);
static int32_t  mndProcessTrimDbRsp(SRpcMsg *pReq);
static int32_t  mndProcessTrimDbWalRsp(SRpcMsg *pReq);
static int32_t  mndProcessTableMetaReq(SRpcMsg *pReq);
static int32_t  mndRetrieveStb(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndRetrieveStbCol(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndRetrieveVstableInherits(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextStb(SMnode *pMnode, void *pIter);
static int32_t  mndProcessTableCfgReq(SRpcMsg *pReq);
static int32_t  mndAlterStbImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                               void *alterOriData, int32_t alterOriDataLen);
static int32_t  mndAlterStbAndUpdateTagIdxImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb, bool needRsp,
                                              void *alterOriData, int32_t alterOriDataLen, const SMAlterStbReq *pAlter);
static int32_t  mndAlterStbDropBaseOnImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb,
                                         SStbObj *pOld, void *alterOriData, int32_t alterOriDataLen);
static void     mndInvalidateParentHasChildrenCache(SMnode *pMnode, const int64_t *parentSuids, int8_t numParents);

static int32_t mndProcessCreateIndexReq(SRpcMsg *pReq);
static int32_t mndProcessDropIndexReq(SRpcMsg *pReq);

static int32_t mndProcessDropStbReqFromMNode(SRpcMsg *pReq);
static int32_t mndProcessDropTbWithTsma(SRpcMsg *pReq);
static int32_t mndProcessFetchTtlExpiredTbs(SRpcMsg *pReq);
static int32_t mndProcessAuditRecordRsp(SRpcMsg *pRsp);
static int32_t mndProcessGetVstLeavesReq(SRpcMsg *pReq);

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
  mndSetMsgHandle(pMnode, TDMT_VND_CHECK_HAS_CTB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TTL_TABLE_RSP, mndProcessDropTtltbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TRIM_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TRIM_WAL_RSP, mndProcessTrimDbWalRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_STB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TABLE_META, mndProcessTableMetaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TTL_TIMER, mndProcessTtlTimer);
  // mndSetMsgHandle(pMnode, TDMT_MND_TRIM_DB_TIMER, mndProcessTrimDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_TABLE_CFG, mndProcessTableCfgReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STB_DROP, mndProcessDropStbReqFromMNode);
  mndSetMsgHandle(pMnode, TDMT_MND_STB_DROP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TB_WITH_TSMA, mndProcessDropTbWithTsma);
  mndSetMsgHandle(pMnode, TDMT_VND_FETCH_TTL_EXPIRED_TBS_RSP, mndProcessFetchTtlExpiredTbs);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TABLE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_TABLE_RSP, mndTransProcessRsp);

  //  mndSetMsgHandle(pMnode, TDMT_MND_SYSTABLE_RETRIEVE, mndProcessRetrieveStbReq);

  // mndSetMsgHandle(pMnode, TDMT_MND_CREATE_INDEX, mndProcessCreateIndexReq);
  // mndSetMsgHandle(pMnode, TDMT_MND_DROP_INDEX, mndProcessDropIndexReq);
  // mndSetMsgHandle(pMnode, TDMT_VND_CREATE_INDEX_RSP, mndTransProcessRsp);
  // mndSetMsgHandle(pMnode, TDMT_VND_DROP_INDEX_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_AUDIT_RECORD_RSP, mndProcessAuditRecordRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_VST_LEAVES, mndProcessGetVstLeavesReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STB, mndRetrieveStb);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STB, mndCancelGetNextStb);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_COL, mndRetrieveStbCol);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_COL, mndCancelGetNextStb);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VSTABLE_INHERITS, mndRetrieveVstableInherits);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VSTABLE_INHERITS, mndCancelGetNextStb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStb(SMnode *pMnode) {}

SSdbRaw *mndStbActionEncode(SStbObj *pStb) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    hasTypeMod = false;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t size = sizeof(SStbObj) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema) + pStb->commentLen +
                 pStb->ast1Len + pStb->ast2Len + pStb->numOfColumns * sizeof(SColCmpr) + STB_RESERVE_SIZE +
                 taosArrayGetSize(pStb->pFuncs) * TSDB_FUNC_NAME_LEN + sizeof(int32_t) * pStb->numOfColumns +
                 sizeof(int8_t) + TSDB_MAX_VST_PARENTS * sizeof(int64_t) + 2 * sizeof(int16_t) +
                 pStb->txnAlterReqsLen;
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
    hasTypeMod = hasTypeMod || HAS_TYPE_MOD(pSchema);
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
  SDB_SET_INT64(pRaw, dataPos, pStb->keep, _OVER)

  if (hasTypeMod) {
    for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
      SDB_SET_INT32(pRaw, dataPos, pStb->pExtSchemas[i].typeMod, _OVER);
    }
  }

  SDB_SET_INT8(pRaw, dataPos, pStb->virtualStb, _OVER)
  // since 3.4.0.0 - STB_VER_SUPPORT_OWNER
  SDB_SET_BINARY(pRaw, dataPos, pStb->createUser, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->ownerId, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pStb->secureDelete, _OVER)
  SDB_SET_UINT32(pRaw, dataPos, pStb->flags, _OVER)
  // batch-meta-txn - STB_VER_SUPPORT_TXN
  SDB_SET_INT64(pRaw, dataPos, (int64_t)pStb->txnId, _OVER)
  if (pStb->txnId != 0) {
    SDB_SET_INT8(pRaw, dataPos, pStb->txnStatus, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pStb->txnAlterReqsLen, _OVER)
    if (pStb->txnAlterReqsLen > 0) {
      if (pStb->pTxnAlterReqs == NULL) {
        terrno = TSDB_CODE_INVALID_PARA;
        goto _OVER;
      }
      SDB_SET_BINARY(pRaw, dataPos, pStb->pTxnAlterReqs, pStb->txnAlterReqsLen, _OVER)
    }
  }

  // since 3.x.x - STB_VER_SUPPORT_INHERIT
  SDB_SET_INT8(pRaw, dataPos, pStb->numParents, _OVER)
  for (int32_t i = 0; i < TSDB_MAX_VST_PARENTS; ++i) {
    SDB_SET_INT64(pRaw, dataPos, pStb->parentSuids[i], _OVER)
  }
  SDB_SET_INT16(pRaw, dataPos, pStb->ownColStart, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pStb->ownTagStart, _OVER)
  // since 3.x.x - STB_VER_SUPPORT_HAS_CHILDREN_CACHE
  SDB_SET_INT8(pRaw, dataPos, pStb->hasChildren, _OVER)


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

SSdbRow *mndStbActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SStbObj *pStb = NULL;
  bool     hasExtSchemas = false;

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
    hasExtSchemas = hasExtSchemas || HAS_TYPE_MOD(pSchema);
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
  if (sver < STB_VER_SUPPORT_COMP) {
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
      SDB_GET_INT32(pRaw, dataPos, (int32_t *)&pCmpr->alg, _OVER)  // compatible
    }
  }
  SDB_GET_INT64(pRaw, dataPos, &pStb->keep, _OVER)

  // type mod
  if (hasExtSchemas) {
    pStb->pExtSchemas = taosMemoryCalloc(pStb->numOfColumns, sizeof(SExtSchema));
    if (!pStb->pExtSchemas) goto _OVER;
    for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
      SSchema *pSchema = &pStb->pColumns[i];
      SDB_GET_INT32(pRaw, dataPos, &pStb->pExtSchemas[i].typeMod, _OVER)
    }
  }

  if (sver < STB_VER_SUPPORT_VIRTUAL) {
    pStb->virtualStb = 0;
  } else {
    SDB_GET_INT8(pRaw, dataPos, &pStb->virtualStb, _OVER)
  }

  if (sver < STB_VER_SUPPORT_OWNER) {
    pStb->createUser[0] = 0;
  } else {
    SDB_GET_BINARY(pRaw, dataPos, pStb->createUser, TSDB_USER_LEN, _OVER)
    SDB_GET_INT64(pRaw, dataPos, &pStb->ownerId, _OVER)
  }

  if (dataPos + sizeof(int8_t) <= pRaw->dataLen) {
    SDB_GET_INT8(pRaw, dataPos, &pStb->secureDelete, _OVER)
  } else {
    pStb->secureDelete = 0;
  }

  if (dataPos + sizeof(uint32_t) <= pRaw->dataLen) {
    SDB_GET_UINT32(pRaw, dataPos, &pStb->flags, _OVER)
  } else {
    pStb->flags = 0;
  }

  // batch-meta-txn - STB_VER_SUPPORT_TXN
  if (sver >= STB_VER_SUPPORT_TXN) {
    SDB_GET_INT64(pRaw, dataPos, &pStb->txnId, _OVER)
    if (pStb->txnId != 0) {
      SDB_GET_INT8(pRaw, dataPos, &pStb->txnStatus, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &pStb->txnAlterReqsLen, _OVER)
      if (pStb->txnAlterReqsLen > 0) {
        pStb->pTxnAlterReqs = taosMemoryMalloc(pStb->txnAlterReqsLen);
        if (pStb->pTxnAlterReqs == NULL) goto _OVER;
        SDB_GET_BINARY(pRaw, dataPos, pStb->pTxnAlterReqs, pStb->txnAlterReqsLen, _OVER)
      } else {
        pStb->pTxnAlterReqs = NULL;
      }
    }
  } else {
    pStb->txnId = 0;
    pStb->txnStatus = 0;
    pStb->txnAlterReqsLen = 0;
    pStb->pTxnAlterReqs = NULL;
  }

  // since 3.x.x - STB_VER_SUPPORT_INHERIT
  if (sver >= STB_VER_SUPPORT_INHERIT) {
    SDB_GET_INT8(pRaw, dataPos, &pStb->numParents, _OVER)
    for (int32_t i = 0; i < TSDB_MAX_VST_PARENTS; ++i) {
      SDB_GET_INT64(pRaw, dataPos, &pStb->parentSuids[i], _OVER)
    }
    SDB_GET_INT16(pRaw, dataPos, &pStb->ownColStart, _OVER)
    SDB_GET_INT16(pRaw, dataPos, &pStb->ownTagStart, _OVER)
    // since 3.x.x - STB_VER_SUPPORT_HAS_CHILDREN_CACHE
    // Skip persisted hasChildren (may be stale after restart); always recompute.
    if (dataPos + sizeof(int8_t) <= pRaw->dataLen) {
      int8_t dummy;
      SDB_GET_INT8(pRaw, dataPos, &dummy, _OVER)
    }
    pStb->hasChildren = -1;  // unknown, will be computed on demand
  } else {
    pStb->numParents = 0;
    memset(pStb->parentSuids, 0, sizeof(pStb->parentSuids));
    pStb->ownColStart = 0;
    pStb->ownTagStart = 0;
    pStb->hasChildren = 0;  // no inheritance, so no children
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
      taosMemoryFreeClear(pStb->pExtSchemas);
      taosMemoryFreeClear(pStb->pTxnAlterReqs);
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
  taosMemoryFreeClear(pStb->pExtSchemas);
  taosMemoryFreeClear(pStb->pTxnAlterReqs);
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
  terrno = 0;
  mTrace("stb:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  int32_t numOfColumns = pOld->numOfColumns;
  if (pOld->numOfColumns < pNew->numOfColumns) {
    void *pColumns = taosMemoryMalloc(pNew->numOfColumns * sizeof(SSchema));
    if (pColumns == NULL) {
      goto END;
    }
    taosMemoryFree(pOld->pColumns);
    pOld->pColumns = pColumns;
  }

  if (pOld->numOfTags < pNew->numOfTags) {
    void *pTags = taosMemoryMalloc(pNew->numOfTags * sizeof(SSchema));
    if (pTags == NULL) {
      goto END;
    }
    taosMemoryFree(pOld->pTags);
    pOld->pTags = pTags;
  }

  if (pOld->commentLen < pNew->commentLen && pNew->commentLen > 0) {
    void *comment = taosMemoryMalloc(pNew->commentLen + 1);
    if (comment == NULL) {
      goto END;
    }
    taosMemoryFree(pOld->comment);
    pOld->comment = comment;
  }
  pOld->commentLen = pNew->commentLen;

  if (pOld->ast1Len < pNew->ast1Len) {
    void *pAst1 = taosMemoryMalloc(pNew->ast1Len + 1);
    if (pAst1 == NULL) {
      goto END;
    }
    taosMemoryFree(pOld->pAst1);
    pOld->pAst1 = pAst1;
  }

  if (pOld->ast2Len < pNew->ast2Len) {
    void *pAst2 = taosMemoryMalloc(pNew->ast2Len + 1);
    if (pAst2 == NULL) {
      goto END;
    }
    taosMemoryFree(pOld->pAst2);
    pOld->pAst2 = pAst2;
  }

  pOld->updateTime = pNew->updateTime;
  pOld->tagVer = pNew->tagVer;
  pOld->colVer = pNew->colVer;
  pOld->smaVer = pNew->smaVer;
  pOld->nextColId = pNew->nextColId;
  pOld->ttl = pNew->ttl;
  pOld->keep = pNew->keep;
  pOld->ownerId = pNew->ownerId;
  pOld->secureDelete = pNew->secureDelete;
  pOld->flags = pNew->flags;
  pOld->txnId = pNew->txnId;
  pOld->txnStatus = pNew->txnStatus;

  // Update txn ALTER request chain
  if (pNew->txnAlterReqsLen > 0 && pNew->pTxnAlterReqs != NULL) {
    taosMemoryFreeClear(pOld->pTxnAlterReqs);
    pOld->pTxnAlterReqs = taosMemoryMalloc(pNew->txnAlterReqsLen);
    if (pOld->pTxnAlterReqs == NULL) {
      pOld->txnAlterReqsLen = 0;
      goto END;
    }
    memcpy(pOld->pTxnAlterReqs, pNew->pTxnAlterReqs, pNew->txnAlterReqsLen);
    pOld->txnAlterReqsLen = pNew->txnAlterReqsLen;
  } else {
    taosMemoryFreeClear(pOld->pTxnAlterReqs);
    pOld->txnAlterReqsLen = 0;
  }

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
    if (pOld->pCmpr == NULL){
      goto END;
    }
    memcpy(pOld->pCmpr, pNew->pCmpr, pNew->numOfColumns * sizeof(SColCmpr));
  } else {
    memcpy(pOld->pCmpr, pNew->pCmpr, pNew->numOfColumns * sizeof(SColCmpr));
  }

  if (pNew->pExtSchemas) {
    taosMemoryFreeClear(pOld->pExtSchemas);
    pOld->pExtSchemas = taosMemoryCalloc(pNew->numOfColumns, sizeof(SExtSchema));
    if (pOld->pExtSchemas == NULL){
      goto END;
    }
    memcpy(pOld->pExtSchemas, pNew->pExtSchemas, pNew->numOfColumns * sizeof(SExtSchema));
  }

  // VST inheritance fields
  pOld->numParents = pNew->numParents;
  memcpy(pOld->parentSuids, pNew->parentSuids, sizeof(pNew->parentSuids));
  pOld->ownColStart = pNew->ownColStart;
  pOld->ownTagStart = pNew->ownTagStart;
  pOld->hasChildren = pNew->hasChildren;

END:
  taosWUnLockLatch(&pOld->lock);
  return terrno;
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

// VST inheritance utility: check if a VST is a parent of any other VST.
// Uses in-memory cache (hasChildren field) to avoid repeated full scans.
// Caller must hold a reference to pParent (via mndAcquireStb / sdbFetch).
bool mndStbHasChildren(SMnode *pMnode, SStbObj *pParent) {
  if (pParent->hasChildren == 1) return true;
  if (pParent->hasChildren == 0) return false;
  // hasChildren == -1: unknown, perform full scan

  SSdb    *pSdb = pMnode->pSdb;
  void    *pIter = NULL;
  SStbObj *pStb = NULL;
  bool     found = false;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;
    for (int8_t i = 0; i < pStb->numParents; ++i) {
      if (pStb->parentSuids[i] == pParent->uid) {
        found = true;
        break;
      }
    }
    sdbRelease(pSdb, pStb);
    if (found) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }

  // atomic store: caller may hold pParent->lock as read (e.g. mndBuildStbSchemaImp),
  // so we must not try to acquire a write lock here. The transition is one-way
  // (-1 → 0/1) and both concurrent storers compute the same value, so a plain
  // atomic store is correct without holding the write lock.
  atomic_store_8(&pParent->hasChildren, found ? 1 : 0);
  return found;
}

// VST inheritance utility: resolve parent suids -> parent full names in one SDB scan.
// The mnode SStbObj only stores parentSuids[]; the vnode WAL entry (and the TMQ meta
// derived from it) needs parent *names* to be replayable, including across clusters
// where the source suids are meaningless. Fills pFNames[i] for each parentSuids[i];
// any suid not found is left as an empty string (caller tolerates absence).
static void mndResolveParentNames(SMnode *pMnode, const int64_t *parentSuids, int8_t numParents,
                                  char pFNames[][TSDB_TABLE_FNAME_LEN]) {
  if (numParents <= 0) return;
  SSdb    *pSdb = pMnode->pSdb;
  void    *pIter = NULL;
  SStbObj *pStb = NULL;
  int8_t   resolved = 0;
  while (resolved < numParents) {
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;
    for (int8_t i = 0; i < numParents; ++i) {
      if (pFNames[i][0] == '\0' && pStb->uid == parentSuids[i]) {
        tstrncpy(pFNames[i], pStb->name, TSDB_TABLE_FNAME_LEN);
        ++resolved;
      }
    }
    sdbRelease(pSdb, pStb);
  }
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
}

// VST inheritance utility: DAG cycle detection
// Returns true if adding parentSuids as parents to childSuid creates a cycle.
// Uses a dynamic visited set/queue so we do not silently truncate large DAGs.
bool mndCheckCyclicInherit(SMnode *pMnode, int64_t childSuid, int64_t *parentSuids, int8_t numParents) {
  bool      hasCycle = false;
  SArray   *queue = taosArrayInit(16, sizeof(int64_t));
  SHashObj *visited = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  // uid → {numParents, parentSuids[]} adjacency map, built in one O(N) scan
  SHashObj *pAdjMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (queue == NULL || visited == NULL || pAdjMap == NULL) {
    mError("cycle-check: out of memory; conservatively reporting cycle");
    hasCycle = true;
    goto _exit;
  }

  // Build uid → parentSuids[] map in one scan: O(N)
  {
    SSdb    *pSdb = pMnode->pSdb;
    void    *pIter = NULL;
    SStbObj *pStb = NULL;
    while ((pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb))) {
      if (pStb->numParents > 0) {
        struct { int8_t n; int64_t s[TSDB_MAX_VST_PARENTS]; } e;
        e.n = pStb->numParents;
        memcpy(e.s, pStb->parentSuids, sizeof(e.s));
        if (taosHashPut(pAdjMap, &pStb->uid, sizeof(int64_t), &e, sizeof(e)) != 0) {
          // A dropped adjacency edge would make the BFS below miss a real parent link
          // and could let a cycle slip through. Fail safe: conservatively report a
          // cycle rather than silently building an incomplete graph.
          mError("cycle-check: out of memory building adjacency map; conservatively reporting cycle");
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          hasCycle = true;
          goto _exit;
        }
      }
      sdbRelease(pSdb, pStb);
    }
  }

  for (int8_t i = 0; i < numParents; ++i) {
    if (parentSuids[i] == childSuid) {
      hasCycle = true;
      goto _exit;
    }
    if (taosHashGet(visited, &parentSuids[i], sizeof(int64_t)) == NULL) {
      if (taosArrayPush(queue, &parentSuids[i]) == NULL ||
          taosHashPut(visited, &parentSuids[i], sizeof(int64_t), &parentSuids[i], sizeof(int64_t)) != 0) {
        mError("cycle-check: out of memory; conservatively reporting cycle");
        hasCycle = true;
        goto _exit;
      }
    }
  }

  // BFS using O(1) hash lookup per node instead of O(N) sdbFetch scan
  size_t head = 0;
  while (head < taosArrayGetSize(queue)) {
    int64_t curSuid = *(int64_t *)taosArrayGet(queue, head++);
    if (curSuid == childSuid) {
      hasCycle = true;
      goto _exit;
    }

    struct { int8_t n; int64_t s[TSDB_MAX_VST_PARENTS]; } *pEntry =
        taosHashGet(pAdjMap, &curSuid, sizeof(int64_t));
    if (pEntry == NULL) continue;

    for (int8_t j = 0; j < pEntry->n; ++j) {
      int64_t nxt = pEntry->s[j];
      if (nxt == childSuid) {
        hasCycle = true;
        goto _exit;
      }
      if (taosHashGet(visited, &nxt, sizeof(int64_t)) == NULL) {
        if (taosArrayPush(queue, &nxt) == NULL ||
            taosHashPut(visited, &nxt, sizeof(int64_t), &nxt, sizeof(int64_t)) != 0) {
          mError("cycle-check: out of memory; conservatively reporting cycle");
          hasCycle = true;
          goto _exit;
        }
      }
    }
  }

_exit:
  if (queue) taosArrayDestroy(queue);
  if (visited) taosHashCleanup(visited);
  if (pAdjMap) taosHashCleanup(pAdjMap);
  return hasCycle;
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
                            int32_t alterOriDataLen, txn_id_t wireTxnId) {
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
  req.virtualStb = pStb->virtualStb;
  req.secureDelete = pStb->secureDelete;
  req.securityLevel = pStb->securityLevel;
  req.txnId = wireTxnId;  // batch-meta-txn: VNode marks STB as PRE_CREATE/PRE_ALTER

  // todo
  req.schemaRow.nCols = pStb->numOfColumns;
  req.schemaRow.version = pStb->colVer;
  req.schemaRow.pSchema = pStb->pColumns;
  req.schemaTag.nCols = pStb->numOfTags;
  req.schemaTag.version = pStb->tagVer;
  req.schemaTag.pSchema = pStb->pTags;

  req.colCmpred = 1;
  SColCmprWrapper *pCmpr = &req.colCmpr;
  req.keep = pStb->keep;
  pCmpr->version = pStb->colVer;
  pCmpr->nCols = pStb->numOfColumns;

  req.colCmpr.pColCmpr = taosMemoryCalloc(pCmpr->nCols, sizeof(SColCmpr));
  for (int32_t i = 0; i < pStb->numOfColumns; i++) {
    SColCmpr *p = &pCmpr->pColCmpr[i];
    p->alg = pStb->pCmpr[i].alg;
    p->id = pStb->pCmpr[i].id;
  }

  req.pExtSchemas = pStb->pExtSchemas; // only reference to it.

  // VST inheritance: carry parent suids + resolved parent full names so the WAL
  // entry (and TMQ meta derived from it) can emit a replayable BASE ON clause.
  req.numParents = pStb->numParents;
  req.ownColStart = pStb->ownColStart;
  req.ownTagStart = pStb->ownTagStart;
  if (req.numParents > 0) {
    memcpy(req.parentSuids, pStb->parentSuids, sizeof(int64_t) * req.numParents);
    mndResolveParentNames(pMnode, req.parentSuids, req.numParents, req.parentStbFNames);
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
  taosMemoryFreeClear(req.rsmaParam.name);
  taosMemoryFreeClear(req.rsmaParam.funcColIds);
  taosMemoryFreeClear(req.rsmaParam.funcIds);
  taosMemoryFreeClear(req.colCmpr.pColCmpr);
  return pHead;
_err:
  taosMemoryFreeClear(req.rsmaParam.name);
  taosMemoryFreeClear(req.rsmaParam.funcColIds);
  taosMemoryFreeClear(req.rsmaParam.funcIds);
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
  req.txnId = pStb->txnId;  // batch-meta-txn: pass txnId for VNode PRE_DROP marking

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
  int32_t code = tEncodeSVDropStbReq(&encoder, &req);
  tEncoderClear(&encoder);
  if (code != 0) {
    terrno = code;
    return NULL;
  }

  *pContLen = contLen;
  return pHead;
}

int32_t mndCheckCreateStbReq(SMCreateStbReq *pCreate) {
  int32_t code = 0;
  if (pCreate->igExists < 0 || pCreate->igExists > 1) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  if (pCreate->virtualStb != 0 && pCreate->virtualStb != 1) {
    code = TSDB_CODE_MND_INVALID_STB_OPTION;
    TAOS_RETURN(code);
  }

  int32_t maxColumns = pCreate->virtualStb ? TSDB_MAX_COLUMNS : TSDB_MAX_COLUMNS_NON_VIRTUAL;
  if (pCreate->numOfColumns < TSDB_MIN_COLUMNS || pCreate->numOfTags + pCreate->numOfColumns > maxColumns) {
    code = TSDB_CODE_PAR_INVALID_COLUMNS_NUM;
    TAOS_RETURN(code);
  }

  // numOfTags == 0 is allowed for VST with BASE ON (tags will be inherited from parents)
  bool noOwnTags = (pCreate->numOfTags == 0 && pCreate->numParents > 0 && pCreate->virtualStb);
  mInfo("stb:%s, check: numOfTags=%d numParents=%d virtualStb=%d noOwnTags=%d",
        pCreate->name, pCreate->numOfTags, pCreate->numParents, pCreate->virtualStb, (int)noOwnTags);
  if ((!noOwnTags && pCreate->numOfTags <= 0) || pCreate->numOfTags > TSDB_MAX_TAGS) {
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

int32_t mndSetCreateStbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, add stb to commit log", pTrans->id);
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

// Build VCT check actions (group 1) for specified parent suids.
// Sends TDMT_VND_CHECK_HAS_CTB to ALL vgroups in the DB for each parent.
// If any vgroup has a child table for a parent suid, the transaction fails with ROLLBACK.
static int32_t mndSetCheckHasCtbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb,
                                            int64_t *parentSuids, int8_t numParents) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  for (int8_t p = 0; p < numParents; ++p) {
    SVCheckHasCtbReq checkReq = {.suid = parentSuids[p]};
    int32_t          reqLen = tSerializeSVCheckHasCtbReq(NULL, 0, &checkReq);
    if (reqLen < 0) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    int32_t contLen = reqLen + sizeof(SMsgHead);

    SVgObj *pVgroup = NULL;
    void   *pIter = NULL;
    while (1) {
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;
      if (!mndVgroupInDb(pVgroup, pDb->uid)) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }

      SMsgHead *pHead = taosMemoryCalloc(1, contLen);
      if (pHead == NULL) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(terrno);
      }
      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(pVgroup->vgId);
      void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
      if (tSerializeSVCheckHasCtbReq(pBuf, reqLen, &checkReq) < 0) {
        taosMemoryFree(pHead);
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }

      STransAction action = {0};
      action.mTraceId = pTrans->mTraceId;
      action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
      action.pCont = pHead;
      action.contLen = contLen;
      action.msgType = TDMT_VND_CHECK_HAS_CTB;
      if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
        taosMemoryFree(pHead);
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }
      sdbRelease(pSdb, pVgroup);
    }
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;
  int32_t groupId = (pTrans->exec == TRN_EXEC_GROUP_PARALLEL) ? 2 : 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, NULL, 0, pStb->txnId);
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
    action.groupId = groupId;
    mInfo("trans:%d, add create stb to redo action", pTrans->id);
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

  void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, NULL, 0, pStb->txnId);
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
  action.groupId = pVgroup->vgId;
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
    mInfo("trans:%d, add drop stb to undo action", pTrans->id);
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
    if (taosStrncasecmp(pSchema->name, colName, TSDB_COL_NAME_LEN) == 0) {
      return pSchema;
    }
  }
  return NULL;
}

int32_t mndBuildStbFromReq(SMnode *pMnode, SStbObj *pDst, SMCreateStbReq *pCreate, SDbObj *pDb) {
  int32_t code = 0;
  bool    hasTypeMods = false;
  memcpy(pDst->name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  memcpy(pDst->db, pDb->name, TSDB_DB_FNAME_LEN);
  pDst->createdTime = taosGetTimestampMs();
  pDst->updateTime = pDst->createdTime;
  pDst->uid = (pCreate->source == TD_REQ_FROM_TAOX_OLD || pCreate->source == TD_REQ_FROM_TAOX ||
               pCreate->source == TD_REQ_FROM_SML || pCreate->suid != 0)
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
  pDst->keep = pCreate->keep;
  pDst->virtualStb = pCreate->virtualStb;
  pDst->secureDelete = pCreate->secureDelete;
  pDst->txnId = pCreate->txnId;  // batch-meta-txn: mark STB as txn-owned (invisible to other sessions)
  pDst->txnStatus = (pCreate->txnId != 0) ? META_TXN_PRE_CREATE : META_TXN_NORMAL;
  pCreate->pFuncs = NULL;

  // VST inheritance
  pDst->numParents = pCreate->numParents;
  pDst->ownColStart = pCreate->ownColStart;
  pDst->ownTagStart = pCreate->ownTagStart;
  if (pDst->numParents > 0) {
    for (int32_t i = 0; i < pDst->numParents; ++i) {
      SStbObj *pParentStb = mndAcquireStb(pMnode, pCreate->parentStbFNames[i]);
      if (pParentStb == NULL) {
        code = TSDB_CODE_MND_STB_NOT_EXIST;
        TAOS_RETURN(code);
      }
      pDst->parentSuids[i] = pParentStb->uid;
      mndReleaseStb(pMnode, pParentStb);
    }
  }

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
  // numOfTags may be 0 for VST with BASE ON (tags inherited from parents after merge)
  pDst->pTags = (pDst->numOfTags > 0) ? taosMemoryCalloc(pDst->numOfTags, sizeof(SSchema)) : NULL;
  if (pDst->pColumns == NULL || (pDst->numOfTags > 0 && pDst->pTags == NULL)) {
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
    hasTypeMods = hasTypeMods || HAS_TYPE_MOD(pSchema);
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
    if (pField->compress != 0) {
      code = validColCmprByType(pSchema->type, pField->compress);
      if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
      }
    }
    pColCmpr->alg = pField->compress;
  }

  if (hasTypeMods) {
    pDst->pExtSchemas = taosMemoryCalloc(pDst->numOfColumns, sizeof(SExtSchema));
    if (!pDst->pExtSchemas) {
      code = terrno;
      TAOS_RETURN(code);
    }
    for (int32_t i = 0; i < pDst->numOfColumns; ++i) {
      SFieldWithOptions * pField = taosArrayGet(pCreate->pColumns, i);
      pDst->pExtSchemas[i].typeMod = pField->typeMod;
    }
  }
  TAOS_RETURN(code);
}
int32_t mndGenIdxNameForFirstTag(char *fullname, char *dbname, char *stbname, char *tagname) {
  SName name = {0};
  if ((terrno = tNameFromString(&name, stbname, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return -1;
  }
  return snprintf(fullname, TSDB_INDEX_FNAME_LEN, "%s.%s_%s", dbname, tagname, tNameGetTableName(&name));
}

static int32_t mndCreateStb(SMnode *pMnode, SRpcMsg *pReq, SMCreateStbReq *pCreate, SDbObj *pDb, SUserObj *pOperUser) {
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
  memcpy(stbObj.createUser, pOperUser->name, TSDB_USER_LEN);
  stbObj.ownerId = pOperUser->uid;

  // Merge parent columns/tags into child schema during CREATE with BASE ON
  if (stbObj.numParents > 0) {
    int32_t addCols = 0, addTags = 0;
    SStbObj *pParents[TSDB_MAX_VST_PARENTS] = {0};
    for (int8_t i = 0; i < stbObj.numParents; ++i) {
      pParents[i] = mndAcquireStb(pMnode, pCreate->parentStbFNames[i]);
      if (!pParents[i]) { code = TSDB_CODE_MND_STB_NOT_EXIST; goto _OVER; }
      addCols += (pParents[i]->numOfColumns > 1) ? (pParents[i]->numOfColumns - 1) : 0;
      addTags += pParents[i]->numOfTags;
    }

    // Conflict detection: all parents' columns/tags (except ts) and child-own columns/tags must be unique.
    // Use a hash set keyed by name -> source string ("parent:NAME" or "child").
    SHashObj *pNames = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (pNames == NULL) {
      for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }
    for (int8_t p = 0; p < stbObj.numParents; ++p) {
      const char *pname = pCreate->parentStbFNames[p];
      // parent columns (skip ts at index 0)
      for (int32_t c = 1; c < pParents[p]->numOfColumns; ++c) {
        const char *nm = pParents[p]->pColumns[c].name;
        char       *prev = (char *)taosHashGet(pNames, nm, strlen(nm) + 1);
        if (prev != NULL) {
          mError("stb:%s, column '%s' conflicts between '%s' and parent '%s'", pCreate->name, nm, prev, pname);
          taosHashCleanup(pNames);
          for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
          code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
          goto _OVER;
        }
        if (taosHashPut(pNames, nm, strlen(nm) + 1, (void *)pname, strlen(pname) + 1) != 0) {
          taosHashCleanup(pNames);
          for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
          code = terrno;
          goto _OVER;
        }
      }
      for (int32_t t = 0; t < pParents[p]->numOfTags; ++t) {
        const char *nm = pParents[p]->pTags[t].name;
        char       *prev = (char *)taosHashGet(pNames, nm, strlen(nm) + 1);
        if (prev != NULL) {
          mError("stb:%s, tag '%s' conflicts between '%s' and parent '%s'", pCreate->name, nm, prev, pname);
          taosHashCleanup(pNames);
          for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
          code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
          goto _OVER;
        }
        if (taosHashPut(pNames, nm, strlen(nm) + 1, (void *)pname, strlen(pname) + 1) != 0) {
          taosHashCleanup(pNames);
          for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
          code = terrno;
          goto _OVER;
        }
      }
    }
    // child own (skip ts at index 0)
    const char *childTag = "child";
    for (int32_t i = 1; i < stbObj.numOfColumns; ++i) {
      const char *nm = stbObj.pColumns[i].name;
      char       *prev = (char *)taosHashGet(pNames, nm, strlen(nm) + 1);
      if (prev != NULL) {
        mError("stb:%s, column '%s' conflicts between 'child' and parent '%s'", pCreate->name, nm, prev);
        taosHashCleanup(pNames);
        for (int8_t i2 = 0; i2 < stbObj.numParents; ++i2) mndReleaseStb(pMnode, pParents[i2]);
        code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
        goto _OVER;
      }
      if (taosHashPut(pNames, nm, strlen(nm) + 1, (void *)childTag, strlen(childTag) + 1) != 0) {
        taosHashCleanup(pNames);
        for (int8_t i2 = 0; i2 < stbObj.numParents; ++i2) mndReleaseStb(pMnode, pParents[i2]);
        code = terrno;
        goto _OVER;
      }
    }
    for (int32_t i = 0; i < stbObj.numOfTags; ++i) {
      const char *nm = stbObj.pTags[i].name;
      char       *prev = (char *)taosHashGet(pNames, nm, strlen(nm) + 1);
      if (prev != NULL) {
        mError("stb:%s, tag '%s' conflicts between 'child' and parent '%s'", pCreate->name, nm, prev);
        taosHashCleanup(pNames);
        for (int8_t i2 = 0; i2 < stbObj.numParents; ++i2) mndReleaseStb(pMnode, pParents[i2]);
        code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
        goto _OVER;
      }
      if (taosHashPut(pNames, nm, strlen(nm) + 1, (void *)childTag, strlen(childTag) + 1) != 0) {
        taosHashCleanup(pNames);
        for (int8_t i2 = 0; i2 < stbObj.numParents; ++i2) mndReleaseStb(pMnode, pParents[i2]);
        code = terrno;
        goto _OVER;
      }
    }
    taosHashCleanup(pNames);

    int32_t ownNumCols = stbObj.numOfColumns;
    int32_t ownNumTags = stbObj.numOfTags;
    int32_t mergedNumCols = 1 + addCols + (ownNumCols - 1);  // ts + parent_cols + own_cols(no ts)
    int32_t mergedNumTags = addTags + ownNumTags;
    if (mergedNumCols > TSDB_MAX_COLUMNS || mergedNumTags > TSDB_MAX_TAGS) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      mError("stb:%s, BASE ON merge exceeds schema limit: cols=%d (max %d), tags=%d (max %d)",
             stbObj.name, mergedNumCols, TSDB_MAX_COLUMNS, mergedNumTags, TSDB_MAX_TAGS);
      for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
      goto _OVER;
    }

    SSchema *mergedCols = taosMemoryCalloc(mergedNumCols, sizeof(SSchema));
    SSchema *mergedTags = taosMemoryCalloc(mergedNumTags, sizeof(SSchema));
    SColCmpr *mergedCmpr = taosMemoryCalloc(mergedNumCols, sizeof(SColCmpr));
    if (!mergedCols || !mergedTags || !mergedCmpr) {
      taosMemoryFree(mergedCols); taosMemoryFree(mergedTags); taosMemoryFree(mergedCmpr);
      for (int8_t i = 0; i < stbObj.numParents; ++i) mndReleaseStb(pMnode, pParents[i]);
      code = terrno; goto _OVER;
    }

    // [0] = ts from child
    int32_t dst = 0;
    mergedCols[0] = stbObj.pColumns[0];
    mergedCmpr[0] = stbObj.pCmpr[0];
    dst = 1;

    col_id_t nextId = stbObj.nextColId;
    // Append parent columns (skip ts at index 0)
    for (int8_t p = 0; p < stbObj.numParents; ++p) {
      for (int32_t c = 1; c < pParents[p]->numOfColumns; ++c) {
        mergedCols[dst] = pParents[p]->pColumns[c];
        mergedCols[dst].colId = nextId++;
        SColCmpr cmpr = {.id = mergedCols[dst].colId,
                         .alg = createDefaultColCmprByType(mergedCols[dst].type)};
        mergedCmpr[dst] = cmpr;
        dst++;
      }
    }
    int16_t newOwnColStart = (int16_t)dst;
    // Append own columns (skip ts at index 0)
    for (int32_t i = 1; i < ownNumCols; ++i) {
      mergedCols[dst] = stbObj.pColumns[i];
      mergedCols[dst].colId = nextId++;
      mergedCmpr[dst] = stbObj.pCmpr[i];
      mergedCmpr[dst].id = mergedCols[dst].colId;
      dst++;
    }

    // Tags: [parent_tags][own_tags]
    dst = 0;
    for (int8_t p = 0; p < stbObj.numParents; ++p) {
      for (int32_t t = 0; t < pParents[p]->numOfTags; ++t) {
        mergedTags[dst] = pParents[p]->pTags[t];
        mergedTags[dst].colId = nextId++;
        dst++;
      }
    }
    int16_t newOwnTagStart = (int16_t)dst;
    for (int32_t i = 0; i < ownNumTags; ++i) {
      mergedTags[dst] = stbObj.pTags[i];
      mergedTags[dst].colId = nextId++;
      dst++;
    }

    for (int8_t i = 0; i < stbObj.numParents; ++i) {
      mndReleaseStb(pMnode, pParents[i]);
    }
    mndInvalidateParentHasChildrenCache(pMnode, stbObj.parentSuids, stbObj.numParents);

    taosMemoryFree(stbObj.pColumns);
    taosMemoryFree(stbObj.pTags);
    taosMemoryFree(stbObj.pCmpr);
    stbObj.pColumns = mergedCols;
    stbObj.pTags = mergedTags;
    stbObj.pCmpr = mergedCmpr;
    stbObj.numOfColumns = mergedNumCols;
    stbObj.numOfTags = mergedNumTags;
    stbObj.ownColStart = newOwnColStart;
    stbObj.ownTagStart = newOwnTagStart;
    stbObj.nextColId = nextId;

    mInfo("stb:%s, merged %d parent(s) during create, cols %d->%d, tags %d->%d, ownColStart=%d, ownTagStart=%d",
          pCreate->name, stbObj.numParents, ownNumCols, mergedNumCols, ownNumTags, mergedNumTags,
          newOwnColStart, newOwnTagStart);
  }

#ifdef TD_ENTERPRISE
  // MAC: reject CREATE STABLE if user.maxSecLevel < db.securityLevel (NRU: low-priv user
  // should not create objects in high-level DBs; in practice, USE DB already blocks this)
  // Only enforced when MAC is explicitly activated cluster-wide.
  // Trusted subjects (PRIV_SECURITY_POLICY_ALTER, directly or via any role that carries that
  // privilege; when MAC is mandatory the holder must have maxSecLevel=4) are exempt.
  bool hasMacLabelPriv = mndUserHasMacLabelPriv(pMnode, pOperUser);
  if (pMnode->macActive == MAC_MODE_MANDATORY && !hasMacLabelPriv && pOperUser->maxSecLevel < pDb->cfg.securityLevel) {
    code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
    mError("stb:%s, failed to create, user %s maxSecLevel(%d) < db securityLevel(%d)",
           pCreate->name, pOperUser->user, pOperUser->maxSecLevel, pDb->cfg.securityLevel);
    goto _OVER;
  }

  // MAC: STB default securityLevel = max(creator.maxSecLevel, db.securityLevel)
  // If the CREATE request specifies a securityLevel AND user has PRIV_SECURITY_POLICY_ALTER, honor it.
  // (check both direct priv and role inheritance: SYSSEC role carries PRIV_SECURITY_POLICY_ALTER)
  // Per FS §4.2.1.4: specifying securityLevel > 0 without PRIV_SECURITY_POLICY_ALTER is rejected;
  //                  securityLevel == 0 is always allowed (equivalent to default).
  if (pCreate->securityLevel > 0 && !hasMacLabelPriv) {
    code = TSDB_CODE_MND_NO_RIGHTS;
    mError("stb:%s, failed to create, user %s lacks PRIV_SECURITY_POLICY_ALTER to set security_level > 0",
           pCreate->name, pOperUser->user);
    goto _OVER;
  }
  if (pCreate->securityLevel > 0 && hasMacLabelPriv) {
    // MAC must be active to set stb security_level > 0; before activation only user levels can be set.
    if (pMnode->macActive != MAC_MODE_MANDATORY) {
      code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
      mError("stb:%s, failed to create, cannot set security_level > 0 before MAC is activated", pCreate->name);
      goto _OVER;
    }
    if (pCreate->securityLevel < pDb->cfg.securityLevel) {
      code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
      mError("stb:%s, failed to create, requested securityLevel(%d) < db securityLevel(%d)", pCreate->name,
             pCreate->securityLevel, pDb->cfg.securityLevel);
      goto _OVER;
    }
    stbObj.securityLevel = (uint8_t)pCreate->securityLevel;
  } else if (pCreate->securityLevel == 0) {
    // Explicitly specified as 0: no extra privilege or MAC precondition is required.
    stbObj.securityLevel = 0;
  } else if (pMnode->macActive == MAC_MODE_MANDATORY) {
    // MAC active: STB inherits max(creator.maxSecLevel, db.securityLevel)
    uint8_t userMax = pOperUser->maxSecLevel;
    uint8_t dbLevel = pDb->cfg.securityLevel;
    stbObj.securityLevel = (userMax > dbLevel) ? userMax : dbLevel;
  } else {
    // MAC not active: default security_level = 0
    stbObj.securityLevel = 0;
  }
#endif

  SSchema *pSchema = &(stbObj.pTags[0]);
  if (mndGenIdxNameForFirstTag(fullIdxName, pDb->name, stbObj.name, pSchema->name) < 0) {
    code = terrno;
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
  memcpy(idxObj.createUser, pOperUser->name, TSDB_USER_LEN);
  idxObj.ownerId = pOperUser->uid;
  idxObj.createdTime = taosGetTimestampMs();
  idxObj.uid = mndGenerateUid(fullIdxName, strlen(fullIdxName));
  idxObj.stbUid = stbObj.uid;
  idxObj.dbUid = stbObj.dbUid;

  TAOS_CHECK_GOTO(mndSetCreateIdxCommitLogs(pMnode, pTrans, &idxObj), NULL, _OVER);

  // If inherited VST, use SERIAL execution: check actions first, then DDL actions
  if (stbObj.numParents > 0) {
    mndTransSetSerial(pTrans);
    TAOS_CHECK_GOTO(mndSetCheckHasCtbRedoActions(pMnode, pTrans, pDb,
                    stbObj.parentSuids, stbObj.numParents), NULL, _OVER);
  }

  TAOS_CHECK_GOTO(mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  if (mndStbActionDelete(pMnode->pSdb, &stbObj) != 0) mError("failed to mndStbActionDelete");
  TAOS_RETURN(code);
}

typedef struct {
  const char *name;
  uint8_t     type;
  int32_t     bytes;
  uint32_t    alg;
} AuditColumnDef;

// column is consistent with vnodePrepareRow process in vnodeSvr.c
static const AuditColumnDef audit_columns[] = {
    {"ts", TSDB_DATA_TYPE_TIMESTAMP, 8, 0x2000102},
    {"details", TSDB_DATA_TYPE_VARCHAR, 50000 + VARSTR_HEADER_SIZE, 0xFF000302},
    {"user_name", TSDB_DATA_TYPE_VARCHAR, 25 + VARSTR_HEADER_SIZE, 0xFF000302},
    {"operation", TSDB_DATA_TYPE_VARCHAR, 20 + VARSTR_HEADER_SIZE, 0xFF000302},
    {"db", TSDB_DATA_TYPE_VARCHAR, TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE, 0xFF000302},
    {"resource", TSDB_DATA_TYPE_VARCHAR, TSDB_STREAM_NAME_LEN + VARSTR_HEADER_SIZE, 0xFF000302},
    {"client_address", TSDB_DATA_TYPE_VARCHAR, AUDIT_CLIENT_ADD_LEN + VARSTR_HEADER_SIZE, 0xFF000302},
    {"duration", TSDB_DATA_TYPE_DOUBLE, 8, 0x5000102},
    {"affected_rows", TSDB_DATA_TYPE_UBIGINT, 8, 0x1000102}};

static int32_t mndBuildAuditStb(SMnode *pMnode, SStbObj *pDst, SDbObj *pDb) {
  int32_t code = 0;
  char   *name = AUDIT_STABLE_NAME;
  (void)tsnprintf(pDst->name, TSDB_TABLE_FNAME_LEN, "%s.%s", pDb->name, name);
  memcpy(pDst->db, pDb->name, TSDB_DB_FNAME_LEN);
  pDst->createdTime = taosGetTimestampMs();
  pDst->updateTime = pDst->createdTime;
  pDst->uid = mndGenerateUid(pDst->name, strlen(pDst->name));
  pDst->dbUid = pDb->uid;
  pDst->tagVer = 1;
  pDst->colVer = 1;
  pDst->smaVer = 1;
  pDst->nextColId = 1;
  pDst->maxdelay[0] = -1;
  pDst->maxdelay[1] = -1;
  pDst->watermark[0] = 5000;
  pDst->watermark[1] = 5000;
  pDst->ttl = 0;
  pDst->keep = -1;
  pDst->source = 0;
  pDst->virtualStb = 0;
  pDst->numOfColumns = sizeof(audit_columns) / sizeof(AuditColumnDef);
  pDst->numOfTags = 1;
  pDst->numOfFuncs = 0;
  pDst->commentLen = -1;
  pDst->pFuncs = NULL;

  pDst->ast1Len = 0;
  pDst->ast2Len = 0;

  pDst->pColumns = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SSchema));
  pDst->pTags = taosMemoryCalloc(1, pDst->numOfTags * sizeof(SSchema));
  pDst->pCmpr = taosMemoryCalloc(1, pDst->numOfColumns * sizeof(SCmprObj));
  if (pDst->pColumns == NULL || pDst->pTags == NULL || pDst->pCmpr == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  if (pDst->nextColId < 0 || pDst->nextColId >= 0x7fff - pDst->numOfColumns - pDst->numOfTags) {
    code = TSDB_CODE_OUT_OF_RANGE;
    TAOS_RETURN(code);
  }

  SSchema *pSchema = NULL;
  for (int32_t i = 0; i < sizeof(audit_columns) / sizeof(AuditColumnDef); ++i) {
    pSchema = &pDst->pColumns[pDst->nextColId - 1];
    pSchema->type = audit_columns[i].type;
    pSchema->bytes = audit_columns[i].bytes;
    pSchema->flags = 1;
    tstrncpy(pSchema->name, audit_columns[i].name, TSDB_COL_NAME_LEN);
    pSchema->colId = pDst->nextColId;
    // hasTypeMods = hasTypeMods || HAS_TYPE_MOD(pSchema);
    SColCmpr *pColCmpr = &pDst->pCmpr[pDst->nextColId - 1];
    pColCmpr->id = pSchema->colId;
    pColCmpr->alg = audit_columns[i].alg;
    pDst->nextColId++;
  }

  // tag
  pSchema = &pDst->pTags[0];
  pSchema->type = TSDB_DATA_TYPE_VARCHAR;
  pSchema->bytes = 64 + VARSTR_HEADER_SIZE;
  SSCHMEA_SET_IDX_ON(pSchema);
  tstrncpy(pSchema->name, "cluster_id", TSDB_COL_NAME_LEN);
  pSchema->colId = pDst->nextColId;
  pDst->nextColId++;

  /*
    if (hasTypeMods) {
      pDst->pExtSchemas = taosMemoryCalloc(pDst->numOfColumns, sizeof(SExtSchema));
      if (!pDst->pExtSchemas) {
        code = terrno;
        TAOS_RETURN(code);
      }
      for (int32_t i = 0; i < pDst->numOfColumns; ++i) {
        SFieldWithOptions *pField = taosArrayGet(pCreate->pColumns, i);
        pDst->pExtSchemas[i].typeMod = pField->typeMod;
      }
    }
  */
  TAOS_RETURN(code);
}

static int32_t mndSetCreateAuditStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb,
                                               SVgObj *pVgroup) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t contLen;

  if (pVgroup == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    TAOS_RETURN(code);
  }

  void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, NULL, 0, pStb->txnId);
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
  mInfo("trans:%d, add create stb to redo action", pTrans->id);
  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

// Note: pVgroup is expected to point to the first element of a vgroup array (e.g. pVgroups at the call site).
// Only the first element is used for creating the audit super table.
int32_t mndCreateAuditStb(SMnode *pMnode, SDbObj *pDb, SUserObj *pOperUser, STrans *pTrans, SVgObj *pVgroup) {
  SStbObj stbObj = {0};
  int32_t code = -1;

  char fullIdxName[TSDB_INDEX_FNAME_LEN * 2] = {0};

  TAOS_CHECK_GOTO(mndBuildAuditStb(pMnode, &stbObj, pDb), NULL, _OVER);
  memcpy(stbObj.createUser, pOperUser->name, TSDB_USER_LEN);
  stbObj.ownerId = pOperUser->uid;

  SSchema *pSchema = &(stbObj.pTags[0]);
  if (mndGenIdxNameForFirstTag(fullIdxName, pDb->name, stbObj.name, pSchema->name) < 0) {
    code = terrno;
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
  memcpy(idxObj.createUser, pOperUser->name, TSDB_USER_LEN);
  idxObj.ownerId = pOperUser->uid;
  idxObj.createdTime = taosGetTimestampMs();
  idxObj.uid = mndGenerateUid(fullIdxName, strlen(fullIdxName));
  idxObj.stbUid = stbObj.uid;
  idxObj.dbUid = stbObj.dbUid;

  mndTransSetDbName(pTrans, pDb->name, stbObj.name);
  TAOS_CHECK_RETURN(mndTransCheckConflict(pMnode, pTrans));
  TAOS_CHECK_GOTO(mndSetCreateIdxCommitLogs(pMnode, pTrans, &idxObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateStbCommitLogs(pMnode, pTrans, pDb, &stbObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateAuditStbRedoActions(pMnode, pTrans, pDb, &stbObj, pVgroup), NULL, _OVER);

  code = 0;

_OVER:
  if (mndStbActionDelete(pMnode->pSdb, &stbObj) != 0) mError("failed to mndStbActionDelete");
  TAOS_RETURN(code);
}

static int32_t mndProcessAuditRecordRsp(SRpcMsg *pRsp) {
  int32_t code = 0;

  if (pRsp == NULL) {
    mError("audit record rsp, null response message");
    return -1;
  }

  if (pRsp->code != 0) {
    mError("audit record rsp failed, code:%d", pRsp->code);
    return pRsp->code;
  }

  SMnode *pMnode = pRsp->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  (void)pMnode;  // currently unused, kept for potential future use
  (void)pSdb;    // currently unused, kept for potential future use

  mDebug("audit record rsp succeeded, code:%d", pRsp->code);

  // no need to implement this rsp, since we do not care about the result of audit record insertion

  return code;
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

    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

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

#if 0
static int32_t mndProcessTrimDbTimer(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  SVgObj     *pVgroup = NULL;
  void       *pIter = NULL;
  SVTrimDbReq trimReq = {0};
  trimReq.compactStartTime = taosGetTimestampMs();
  trimReq.tw.skey = INT64_MIN;
  trimReq.tw.ekey = trimReq.compactStartTime;
  trimReq.compactId = 0;  // TODO: use the real value
  trimReq.metaOnly = 0;
  trimReq.triggerType = TSDB_TRIGGER_AUTO;
  int32_t reqLen = tSerializeSVTrimDbReq(NULL, 0, &trimReq);
  int32_t contLen = reqLen + sizeof(SMsgHead);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t code = 0;

    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbCancelFetch(pSdb, pVgroup);
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    trimReq.dbUid = pVgroup->dbUid;
    (void)snprintf(trimReq.db, sizeof(trimReq.db), "%s", pVgroup->dbName);
    if ((code = tSerializeSVTrimDbReq((char *)pHead + sizeof(SMsgHead), reqLen, &trimReq)) < 0) {
      mError("vgId:%d, failed to serialize trim db request since %s", pVgroup->vgId, tstrerror(code));
    }

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_TRIM, .pCont = pHead, .contLen = contLen};
    SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
    code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, timer failed to send vnode-trim request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mInfo("vgId:%d, timer send vnode-trim request to vnode, time:%" PRIi64 " ms", pVgroup->vgId, trimReq.tw.ekey);
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}
#endif

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
  pDst->pExtSchemas = taosMemoryCalloc(pDst->numOfColumns, sizeof(SExtSchema));

  if (pDst->pColumns == NULL || pDst->pTags == NULL || pDst->pCmpr == NULL || pDst->pExtSchemas == NULL) {
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
      code = validColCmprByType(pSchema->type, pField->compress);
      if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
      }
      p->alg = pField->compress;
    }
    if (pField->flags & COL_HAS_TYPE_MOD) {
      pDst->pExtSchemas[i].typeMod = pField->typeMod;
    }
  }
  pDst->tagVer = createReq->tagVer;
  pDst->colVer = createReq->colVer;
  return TSDB_CODE_SUCCESS;
}

// used for tmq_get_json_meta to build alter msg
static void buildAlterMsg(SStbObj *pStb, SStbObj *pDst, void** pAlterBuf, int32_t* len){
  SMAlterStbReq alterReq = {0};
  alterReq.pFields = taosArrayInit(2, sizeof(SField));
  if (NULL == alterReq.pFields) {
    mError("failed to init alter fields array");
    goto END;
  }
  tstrncpy(alterReq.name, pStb->name, TSDB_TABLE_FNAME_LEN);
  for (int32_t i = 0; i < pDst->numOfColumns && taosArrayGetSize(alterReq.pFields) == 0; ++i) {
    SSchema           *pSchema = &pDst->pColumns[i];
    int32_t cIndex = mndFindSuperTableColumnIndex(pStb, pSchema->name);
    if (cIndex >= 0 && pSchema->bytes == pStb->pColumns[cIndex].bytes) {
      continue;
    }
    if (cIndex < 0) {
      alterReq.alterType = TSDB_ALTER_TABLE_ADD_COLUMN;
    } else if (pSchema->bytes > pStb->pColumns[cIndex].bytes){
      alterReq.alterType = TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES;
    }
    SField *pAlterField = taosArrayReserve(alterReq.pFields, 1);
    pAlterField->type = pSchema->type;
    pAlterField->bytes = pSchema->bytes;
    tstrncpy(pAlterField->name, pSchema->name, TSDB_COL_NAME_LEN);
    mDebug("alter column name:%s, type:%d, bytes:%d", pAlterField->name, pAlterField->type, pAlterField->bytes);
  }

  for (int32_t i = 0; i < pDst->numOfTags && taosArrayGetSize(alterReq.pFields) == 0; ++i) {
    SSchema *pSchema = &pDst->pTags[i];
    int32_t cIndex = mndFindSuperTableTagIndex(pStb, pSchema->name);
    if (cIndex >= 0 && pSchema->bytes == pStb->pTags[cIndex].bytes) {
      continue;
    }
    if (cIndex < 0) {
      alterReq.alterType = TSDB_ALTER_TABLE_ADD_TAG;
    } else if (pSchema->bytes > pStb->pTags[cIndex].bytes){
      alterReq.alterType = TSDB_ALTER_TABLE_UPDATE_TAG_BYTES;
    }
    SField *pAlterField = taosArrayReserve(alterReq.pFields, 1);
    pAlterField->type = pSchema->type;
    pAlterField->bytes = pSchema->bytes;
    tstrncpy(pAlterField->name, pSchema->name, TSDB_COL_NAME_LEN);
    mDebug("alter tag name:%s, type:%d, bytes:%d", pAlterField->name, pAlterField->type, pAlterField->bytes);
  }
  alterReq.numOfFields = taosArrayGetSize(alterReq.pFields);
  if (alterReq.numOfFields == 0) {
    mError("no valid alter field found");
    goto END;
  }

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &alterReq);
  if (contLen <= 0) {
    mError("failed to get alter stb req len");
    goto END;
  }
  void*   buf = taosMemoryMalloc(contLen);
  if (buf == NULL) {
    mError("failed to malloc alter stb req buf");
    goto END;
  }
  int32_t code = tSerializeSMAlterStbReq(buf, contLen, &alterReq);
  if (code <= TSDB_CODE_SUCCESS) {
    mError("failed to serialize alter stb req %d", code);
    taosMemoryFreeClear(buf);
    goto END;
  }
  *pAlterBuf = buf;
  *len = contLen;
END:
  taosArrayDestroy(alterReq.pFields);
}

static int32_t mndProcessCreateStbReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SStbObj       *pStb = NULL;
  SDbObj        *pDb = NULL;
  SUserObj      *pOperUser = NULL;
  SMCreateStbReq createReq = {0};
  bool           isAlter = false;
  SHashObj      *pHash = NULL;
  int64_t        tss = taosGetTimestampMs();

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
  } else if ((createReq.source == TD_REQ_FROM_TAOX_OLD || createReq.source == TD_REQ_FROM_TAOX || createReq.source == TD_REQ_FROM_SML) &&
             (createReq.tagVer != 1 || createReq.colVer != 1)) {
    mInfo("stb:%s, alter table does not need to be done, because table is deleted", createReq.name);
    code = 0;
    goto _OVER;
  }

  pHash = taosHashInit(createReq.numOfColumns + createReq.numOfTags, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                       false, HASH_NO_LOCK);
  if (pHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  for (int32_t i = 0; i < createReq.numOfColumns; ++i) {
    SFieldWithOptions *pField = taosArrayGet(createReq.pColumns, i);
    if ((code = taosHashPut(pHash, pField->name, strlen(pField->name), NULL, 0)) != 0) {
      if (code == TSDB_CODE_DUP_KEY) {
        code = TSDB_CODE_TSC_DUP_COL_NAMES;
      }
      goto _OVER;
    }
  }

  for (int32_t i = 0; i < createReq.numOfTags; ++i) {
    SField *pField = taosArrayGet(createReq.pTags, i);
    if ((code = taosHashPut(pHash, pField->name, strlen(pField->name), NULL, 0)) != 0) {
      if (code == TSDB_CODE_DUP_KEY) {
        code = TSDB_CODE_TSC_DUP_COL_NAMES;
      }
      goto _OVER;
    }
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  if ((code = mndAcquireUser(pMnode, (RPC_MSG_USER(pReq)), &pOperUser))) {
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pDb))) {
    goto _OVER;
  }

  if ((code =
           mndCheckDbPrivilegeByNameRecF(pMnode, pOperUser, pDb->cfg.isAudit ? PRIV_AUDIT_TBL_CREATE : PRIV_TBL_CREATE,
                                         pDb->cfg.isAudit ? PRIV_OBJ_CLUSTER : PRIV_OBJ_DB, pDb->name, NULL))) {
    goto _OVER;
  }

  if (pDb->cfg.isMount) {
    code = TSDB_CODE_MND_MOUNT_OBJ_NOT_SUPPORT;
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

  // VST inheritance validation
  if (createReq.numParents > 0) {
    if (createReq.numParents > TSDB_MAX_VST_PARENTS) {
      code = TSDB_CODE_MND_VST_MAX_PARENTS_EXCEED;
      goto _OVER;
    }
    if (!createReq.virtualStb) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      goto _OVER;
    }

    int64_t parentSuids[TSDB_MAX_VST_PARENTS] = {0};
    for (int8_t i = 0; i < createReq.numParents; ++i) {
      SStbObj *pParentStb = mndAcquireStb(pMnode, createReq.parentStbFNames[i]);
      if (pParentStb == NULL) {
        code = TSDB_CODE_MND_STB_NOT_EXIST;
        goto _OVER;
      }
      if (!pParentStb->virtualStb) {
        mndReleaseStb(pMnode, pParentStb);
        code = TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL;
        goto _OVER;
      }
      // VCT check is done via TDMT_VND_CHECK_HAS_CTB in transaction group 1
      // Same DB check
      if (strncmp(pParentStb->db, pDb->name, TSDB_DB_FNAME_LEN) != 0) {
        mndReleaseStb(pMnode, pParentStb);
        code = TSDB_CODE_MND_VST_CROSS_DB;
        goto _OVER;
      }
      parentSuids[i] = pParentStb->uid;
      mndReleaseStb(pMnode, pParentStb);
    }

    // Cycle detection (use uid=0 for new table since it doesn't exist yet)
    int64_t newSuid = mndGenerateUid(createReq.name, TSDB_TABLE_FNAME_LEN);
    if (mndCheckCyclicInherit(pMnode, newSuid, parentSuids, createReq.numParents)) {
      code = TSDB_CODE_MND_VST_CIRCULAR_INHERIT;
      goto _OVER;
    }
  }

  if (isAlter) {
    bool    needRsp = false;
    SStbObj pDst = {0};
    if ((code = mndBuildStbFromAlter(pStb, &pDst, &createReq)) != 0) {
      taosMemoryFreeClear(pDst.pTags);
      taosMemoryFreeClear(pDst.pColumns);
      taosMemoryFreeClear(pDst.pCmpr);
      taosMemoryFreeClear(pDst.pExtSchemas);
      goto _OVER;
    }
    void* buf = NULL;
    int32_t contLen = 0;
    buildAlterMsg(pStb, &pDst, &buf, &contLen);
    code = mndAlterStbImp(pMnode, pReq, pDb, &pDst, needRsp, buf, contLen);
    taosMemoryFree(buf);
    taosMemoryFreeClear(pDst.pTags);
    taosMemoryFreeClear(pDst.pColumns);
    taosMemoryFreeClear(pDst.pCmpr);
    taosMemoryFreeClear(pDst.pExtSchemas);
  } else {
    // Batch meta txn: create STB immediately (undo-log model) so VNodes have schema
    // for same-txn child table creation. Shadow op tracks the STB for ROLLBACK undo.
    if (createReq.txnId != 0) {
      // Generate suid (parser does not set it; normal path uses mndBuildStbFromReq → mndGenerateUid)
      if (createReq.suid == 0) {
        createReq.suid = mndGenerateUid(createReq.name, TSDB_TABLE_FNAME_LEN);
      }
      // Track STB for undo at ROLLBACK (pReqData=NULL: not needed, name+uid suffice for DROP)
      code = mndTxnAddShadowOp(pMnode, createReq.txnId, MND_SHADOW_OP_CREATE_STB, createReq.name, createReq.suid,
                               pDb->name, NULL, 0);
      if (code != 0) {
        goto _OVER;
      }
      // Fall through to mndCreateStb (creates STB in SDB + distributes schema to VNodes)
    }
    code = mndCreateStb(pMnode, pReq, &createReq, pDb, pOperUser);
  }
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    SName name = {0};
    TAOS_CHECK_RETURN(tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    if (createReq.sql == NULL && createReq.sqlLen == 0) {
      char detail[1000] = {0};

      (void)snprintf(detail, sizeof(detail), "dbname:%s, stable name:%s", name.dbname, name.tname);

      auditRecord(pReq, pMnode->clusterId, "createStb", name.dbname, name.tname, detail, strlen(detail), duration, 0);
    } else {
      auditRecord(pReq, pMnode->clusterId, "createStb", name.dbname, name.tname, createReq.sql, createReq.sqlLen,
                  duration, 0);
    }
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to create since %s", createReq.name, tstrerror(code));
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSMCreateStbReq(&createReq);

  if (pHash != NULL) {
    taosHashCleanup(pHash);
  }

  TAOS_RETURN(code);
}

static int32_t mndCheckAlterStbReq(SMAlterStbReq *pAlter) {
  int32_t code = 0;
  if (pAlter->commentLen >= 0) return 0;
  if (pAlter->ttl != 0) return 0;
  if (pAlter->keep != -1) return 0;
  if (pAlter->secureDelete >= 0) return 0;

  // BASE ON alter types don't use pFields
  if (pAlter->alterType == TSDB_ALTER_TABLE_ADD_BASE_ON || pAlter->alterType == TSDB_ALTER_TABLE_DROP_BASE_ON) {
    if (pAlter->numParents < 1 || pAlter->numParents > TSDB_MAX_VST_PARENTS) {
      code = TSDB_CODE_MND_INVALID_STB_OPTION;
      TAOS_RETURN(code);
    }
    return 0;
  }

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
  if (pOld->pExtSchemas) {
    pNew->pExtSchemas = taosMemoryCalloc(pNew->numOfColumns, sizeof(SExtSchema));
    if (pNew->pExtSchemas == NULL) {
      TAOS_RETURN(terrno);
    }
    memcpy(pNew->pExtSchemas, pOld->pExtSchemas, sizeof(SExtSchema) * pOld->numOfColumns);
  }

  TAOS_RETURN(0);
}

static int32_t mndUpdateTableOptions(const SStbObj *pOld, SStbObj *pNew, char *pComment, int32_t commentLen,
                                     int32_t ttl, int64_t keep, int8_t secureDelete, int8_t securityLevel) {
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

  if (keep > 0) {
    pNew->keep = keep;
  }

  if (secureDelete >= 0) {
    pNew->secureDelete = secureDelete;
  }

  if (securityLevel >= 0 && (uint8_t)securityLevel != pOld->securityLevel) {
    pNew->securityLevel = (uint8_t)securityLevel;
    pNew->colVer++;  // bump version to invalidate client catalog cache only when changed
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

  int32_t maxColumns = pOld->virtualStb ? TSDB_MAX_COLUMNS : TSDB_MAX_COLUMNS_NON_VIRTUAL;
  if (pOld->numOfColumns + ntags + pOld->numOfTags > maxColumns) {
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
    if (pNew->nextColId > INT16_MAX) {
      code = TSDB_CODE_MND_EXCEED_MAX_COL_ID;
      TAOS_RETURN(code);
    }
    pSchema->colId = pNew->nextColId;
    pNew->nextColId++;

    mInfo("stb:%s, start to add tag %s", pNew->name, pSchema->name);
  }

  pNew->tagVer++;
  TAOS_RETURN(code);
}

static int32_t mndCheckAlterColForTSma(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId, bool isTag) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SSmaObj *pSma = NULL;
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    mInfo("tsma:%s, check tag and column modifiable, stb:%s suid:%" PRId64 " colId:%d, sql:%s", pSma->name, stbFullName,
          suid, colId, pSma->sql);

    if (isTag) {
      code = TSDB_CODE_MND_FIELD_CONFLICT_WITH_TSMA;
      mError("tsma:%s, check tag:%d conflicted", pSma->name, colId);
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

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

static int32_t mndDropSuperTableTag(SMnode *pMnode, const SStbObj *pOld, SStbObj *pNew, const char *tagName) {
  int32_t code = 0;
  if (pOld->numOfTags <= 1) {
    TAOS_CHECK_RETURN(TSDB_CODE_PAR_TOO_LESS_TAG_COLUMN);
  }

  int32_t tag = mndFindSuperTableTagIndex(pOld, tagName);
  if (tag < 0) {
    code = TSDB_CODE_MND_TAG_NOT_EXIST;
    TAOS_RETURN(code);
  }

  col_id_t colId = pOld->pTags[tag].colId;
  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, pOld->name, pOld->uid, colId, true));

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
  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, pOld->name, pOld->uid, colId, true));

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
  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, pOld->name, pOld->uid, colId, true));

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
static int32_t mndAddSuperTableColumn(const SStbObj *pOld, SStbObj *pNew, const SMAlterStbReq* pReq, int32_t ncols,
                                      int8_t withCompress) {
  int32_t code = 0;
  int32_t maxColumns = pOld->virtualStb ? TSDB_MAX_COLUMNS : TSDB_MAX_COLUMNS_NON_VIRTUAL;
  int32_t maxBytesPerRow = pOld->virtualStb ? TSDB_MAX_BYTES_PER_ROW_VIRTUAL : TSDB_MAX_BYTES_PER_ROW;
  if (pOld->numOfColumns + ncols + pOld->numOfTags > maxColumns) {
    code = TSDB_CODE_MND_TOO_MANY_COLUMNS;
    TAOS_RETURN(code);
  }

  if ((code = grantCheck(TSDB_GRANT_TIMESERIES)) != 0) {
    TAOS_RETURN(code);
  }

  if (!mndValidateSchema(pOld->pColumns, pOld->numOfColumns, pReq->pFields, maxBytesPerRow)) {
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
      SFieldWithOptions *pField = taosArrayGet(pReq->pFields, i);
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
      if (pNew->nextColId > INT16_MAX) {
        code = TSDB_CODE_MND_EXCEED_MAX_COL_ID;
        TAOS_RETURN(code);
      }
      pSchema->colId = pNew->nextColId;
      pNew->nextColId++;

      SColCmpr *pCmpr = &pNew->pCmpr[pOld->numOfColumns + i];
      pCmpr->id = pSchema->colId;
      code = validColCmprByType(pSchema->type, pField->compress);
      if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
      }
      pCmpr->alg = pField->compress;
      mInfo("stb:%s, start to add column %s", pNew->name, pSchema->name);
    } else {
      SField *pField = taosArrayGet(pReq->pFields, i);
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
      if (pNew->nextColId > INT16_MAX) {
        code = TSDB_CODE_MND_EXCEED_MAX_COL_ID;
        TAOS_RETURN(code);
      }
      pSchema->colId = pNew->nextColId;
      pNew->nextColId++;

      SColCmpr *pCmpr = &pNew->pCmpr[pOld->numOfColumns + i];
      pCmpr->id = pSchema->colId;
      pCmpr->alg = createDefaultColCmprByType(pSchema->type);
      mInfo("stb:%s, start to add column %s", pNew->name, pSchema->name);
    }
  }
  // 1. old schema already has extschemas
  // 2. new schema has extschemas
  if (pReq->pTypeMods || pOld->pExtSchemas) {
    if (!pNew->pExtSchemas) {
      // all ext schemas reset to zero
      pNew->pExtSchemas = taosMemoryCalloc(pNew->numOfColumns, sizeof(SExtSchema));
      if (!pNew->pExtSchemas) TAOS_RETURN(terrno);
    }
    if (pOld->pExtSchemas) {
      memcpy(pNew->pExtSchemas, pOld->pExtSchemas, pOld->numOfColumns * sizeof(SExtSchema));
    }
    if (taosArrayGetSize(pReq->pTypeMods) > 0) {
      // copy added column ext schema
      for (int32_t i = 0; i < ncols; ++i) {
        pNew->pColumns[pOld->numOfColumns + i].flags |= COL_HAS_TYPE_MOD;
        pNew->pExtSchemas[pOld->numOfColumns + i].typeMod = *(STypeMod *)taosArrayGet(pReq->pTypeMods, i);
      }
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
  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, pOld->name, pOld->uid, colId, false));

  TAOS_CHECK_RETURN(mndAllocStbSchemas(pOld, pNew));

  int32_t sz = pNew->numOfColumns - col - 1;
  memmove(pNew->pColumns + col, pNew->pColumns + col + 1, sizeof(SSchema) * sz);
  memmove(pNew->pCmpr + col, pNew->pCmpr + col + 1, sizeof(SColCmpr) * sz);
  if (pOld->pExtSchemas) {
    memmove(pNew->pExtSchemas + col, pNew->pExtSchemas + col + 1, sizeof(SExtSchema) * sz);
  }
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
  int32_t  maxBytesPerRow = pOld->virtualStb ? TSDB_MAX_BYTES_PER_ROW_VIRTUAL : TSDB_MAX_BYTES_PER_ROW;
  for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
    nLen += (pOld->pColumns[i].colId == colId) ? pField->bytes : pOld->pColumns[i].bytes;
  }

  if (nLen > maxBytesPerRow) {
    code = TSDB_CODE_MND_INVALID_ROW_BYTES;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndCheckAlterColForTSma(pMnode, pOld->name, pOld->uid, colId, false));

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
                                         int32_t alterOriDataLen, txn_id_t wireTxnId) {
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

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, alterOriData, alterOriDataLen, wireTxnId);
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
    // groupId=vgId: when this ALTER_STB is appended to the batch-txn commit STrans
    // (GROUP_PARALLEL) alongside this vgId's TDMT_VND_TXN_COMMIT (also groupId=vgId),
    // STrans serializes both in the same group — every ALTER_STB for the vnode is sent
    // and acked before its TXN_COMMIT, so no ALTER_STB can land in the WAL after commit
    // (see mndSetDropStbRedoActions for the matching DROP_STB rationale). The VST BASE-ON
    // paths use SERIAL, so per-vgId grouping is a no-op there.
    action.groupId = pVgroup->vgId;
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

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen, alterOriData, alterOriDataLen, pStb->txnId);
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

static int32_t mndBuildStbSchemaImp(SMnode *pMnode, SDbObj *pDb, SStbObj *pStb, const char *tbName, STableMetaRsp *pRsp, bool refByStm) {
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
  pRsp->numOfColRefs = 0;
  pRsp->pColRefs = NULL;
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
  pRsp->virtualStb = pStb->virtualStb;
  pRsp->ownerId = pStb->ownerId;
  pRsp->isAudit = pDb->cfg.isAudit ? 1 : 0;
  pRsp->secLvl = pStb->securityLevel;
  pRsp->secureDelete = pStb->secureDelete;
  pRsp->hasInheritors = (pMnode != NULL && pStb->virtualStb && mndStbHasChildren(pMnode, pStb)) ? 1 : 0;

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

  if (refByStm) {
    mndStreamUpdateTagsRefFlag(pMnode, pStb->uid, &pRsp->pSchemas[pStb->numOfColumns], pStb->numOfTags);
  }

  for (int32_t i = 0; i < pStb->numOfColumns; i++) {
    SColCmpr   *pCmpr = &pStb->pCmpr[i];
    SSchemaExt *pSchEx = &pRsp->pSchemaExt[i];
    pSchEx->colId = pCmpr->id;
    pSchEx->compress = pCmpr->alg;
    if (pStb->pExtSchemas) {
      pSchEx->typeMod = pStb->pExtSchemas[i].typeMod;
    }
  }

  taosRUnLockLatch(&pStb->lock);
  TAOS_RETURN(code);
}

static int32_t mndBuildStbCfgImp(SMnode *pMnode, SDbObj *pDb, SStbObj *pStb, const char *tbName, STableCfgRsp *pRsp) {
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
  pRsp->keep = pStb->keep;
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
    if (pStb->pExtSchemas) {
      pSchExt->typeMod = pStb->pExtSchemas[i].typeMod;
    }
  }
  pRsp->virtualStb = pStb->virtualStb;
  pRsp->pColRefs = NULL;
  pRsp->secureDelete = pStb->secureDelete;
  pRsp->securityLevel = pStb->securityLevel;

  // VST inheritance info
  pRsp->numParents = pStb->numParents;
  pRsp->ownColStart = pStb->ownColStart;
  pRsp->ownTagStart = pStb->ownTagStart;
  if (pStb->numParents > 0) {
    SSdb *pSdb = pMnode->pSdb;
    for (int8_t i = 0; i < pStb->numParents; ++i) {
      pRsp->parentStbNames[i][0] = '\0';
      void    *pIter2 = NULL;
      SStbObj *pParent = NULL;
      while (1) {
        pIter2 = sdbFetch(pSdb, SDB_STB, pIter2, (void **)&pParent);
        if (pIter2 == NULL) break;
        if (pParent->uid == pStb->parentSuids[i]) {
          mndExtractTbNameFromStbFullName(pParent->name, pRsp->parentStbNames[i], TSDB_TABLE_NAME_LEN);
          sdbRelease(pSdb, pParent);
          sdbCancelFetch(pSdb, pIter2);
          break;
        }
        sdbRelease(pSdb, pParent);
      }
    }
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

static int32_t mndBuildStbSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp, bool refByStm) {
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

  code = mndBuildStbSchemaImp(pMnode, pDb, pStb, tbName, pRsp, refByStm);
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

  code = mndBuildStbCfgImp(pMnode, pDb, pStb, tbName, pRsp);

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

  code = mndBuildStbSchemaImp(NULL, pDb, pObj, name.tname, alterRsp.pMeta, false);
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

  code = mndBuildStbSchemaImp(NULL, pDb, pObj, name.tname, stbRsp.pMeta, false);
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

  mInfo("trans:%d, used to alter stb:%s, alterOriDataLen:%d", pTrans->id, pStb->name, alterOriDataLen);
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
  TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen, pStb->txnId), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// ALTER ADD BASE ON uses ROLLBACK + SERIAL: check actions first, then DDL actions
static int32_t mndAlterStbAddBaseOnImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb,
                                       int64_t *newParentSuids, int8_t numNewParents,
                                       void *alterOriData, int32_t alterOriDataLen) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq, "alter-stb-add-baseon");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to alter stb add base on:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);

  // VCT check actions (executed first in serial order)
  TAOS_CHECK_GOTO(mndSetCheckHasCtbRedoActions(pMnode, pTrans, pDb,
                  newParentSuids, numNewParents), NULL, _OVER);

  // ALTER STB actions (executed after checks pass)
  void   *pCont = NULL;
  int32_t contLen = 0;
  TAOS_CHECK_GOTO(mndBuildSMAlterStbRsp(pDb, pStb, &pCont, &contLen), NULL, _OVER);
  mndTransSetRpcRsp(pTrans, pCont, contLen);

  TAOS_CHECK_GOTO(mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen, pStb->txnId), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndAlterStbDropBaseOnImp(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SStbObj *pStb,
                                        SStbObj *pOld, void *alterOriData, int32_t alterOriDataLen) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq, "alter-stb-drop-baseon");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to alter stb drop base on:%s", pTrans->id, pStb->name);
  mndTransSetDbName(pTrans, pDb->name, pStb->name);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);

  // VCT check actions (verify current VST doesn't have child tables)
  // Although DROP BASE ON may be allowed even with child VSTs,
  // we need to ensure VCT colRef mappings are updated if they exist.
  int64_t vstSuid = pStb->uid;
  TAOS_CHECK_GOTO(mndSetCheckHasCtbRedoActions(pMnode, pTrans, pDb, &vstSuid, 1), NULL, _OVER);

  // ALTER STB actions (executed after checks pass)
  void   *pCont = NULL;
  int32_t contLen = 0;
  TAOS_CHECK_GOTO(mndBuildSMAlterStbRsp(pDb, pStb, &pCont, &contLen), NULL, _OVER);
  mndTransSetRpcRsp(pTrans, pCont, contLen);

  TAOS_CHECK_GOTO(mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen, pStb->txnId), NULL, _OVER);
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

    TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen, pStb->txnId), NULL, _OVER);
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

    TAOS_CHECK_GOTO(mndSetAlterStbRedoActions(pMnode, pTrans, pDb, pStb, alterOriData, alterOriDataLen, pStb->txnId), NULL, _OVER);
    TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  }
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// ALTER STABLE ADD BASE ON - add parent VSTs to inheritance list
static int32_t mndAlterStbAddBaseOn(SMnode *pMnode, SStbObj *pOld, SStbObj *pNew, const SMAlterStbReq *pAlter,
                                    SDbObj *pDb) {
  int32_t code = 0;
  if (pAlter->numParents <= 0) {
    TAOS_RETURN(TSDB_CODE_MND_INVALID_STB_OPTION);
  }
  int8_t totalParents = pOld->numParents + pAlter->numParents;
  if (totalParents > TSDB_MAX_VST_PARENTS) {
    TAOS_RETURN(TSDB_CODE_MND_VST_MAX_PARENTS_EXCEED);
  }
  if (!pOld->virtualStb) {
    TAOS_RETURN(TSDB_CODE_MND_INVALID_STB_OPTION);
  }

  // Copy existing parent list
  pNew->numParents = pOld->numParents;
  memcpy(pNew->parentSuids, pOld->parentSuids, sizeof(pOld->parentSuids));

  // Validate and collect new parents
  SStbObj *pAddParents[TSDB_MAX_VST_PARENTS] = {0};
  int8_t   numAdd = pAlter->numParents;
  int32_t  addCols = 0, addTags = 0;

  for (int8_t i = 0; i < numAdd; ++i) {
    pAddParents[i] = mndAcquireStb(pMnode, (char *)pAlter->parentStbFNames[i]);
    if (pAddParents[i] == NULL) {
      code = TSDB_CODE_MND_STB_NOT_EXIST;
      goto _ADD_OVER;
    }
    // Check for duplicate: parent already in the current (or newly-added) list.
    // Return a dedicated code so callers can distinguish "already applied" from
    // "genuine schema conflict on the target" — the latter must not be silently swallowed.
    for (int8_t j = 0; j < pNew->numParents; ++j) {
      if (pNew->parentSuids[j] == pAddParents[i]->uid) {
        mInfo("stb:%s, parent '%s' already in inheritance list, returning ALREADY_INHERITED",
              pOld->name, pAlter->parentStbFNames[i]);
        code = TSDB_CODE_MND_VST_ALREADY_INHERITED;
        goto _ADD_OVER;
      }
    }
    if (!pAddParents[i]->virtualStb) {
      code = TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL;
      goto _ADD_OVER;
    }
    // VCT check is done via TDMT_VND_CHECK_HAS_CTB in transaction group 1
    if (strncmp(pAddParents[i]->db, pDb->name, TSDB_DB_FNAME_LEN) != 0) {
      code = TSDB_CODE_MND_VST_CROSS_DB;
      goto _ADD_OVER;
    }

    // Check column name conflicts with existing child schema (skip ts at index 0)
    for (int32_t c = 1; c < pAddParents[i]->numOfColumns; ++c) {
      if (mndFindSuperTableColumnIndex(pOld, pAddParents[i]->pColumns[c].name) >= 0 ||
          mndFindSuperTableTagIndex(pOld, pAddParents[i]->pColumns[c].name) >= 0) {
        mError("stb:%s, column '%s' conflicts between existing schema and parent '%s'", pOld->name,
               pAddParents[i]->pColumns[c].name, pAlter->parentStbFNames[i]);
        code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
        goto _ADD_OVER;
      }
      // Check against earlier newly-added parents
      for (int8_t pp = 0; pp < i; ++pp) {
        for (int32_t cc = 1; cc < pAddParents[pp]->numOfColumns; ++cc) {
          if (strcmp(pAddParents[pp]->pColumns[cc].name, pAddParents[i]->pColumns[c].name) == 0) {
            mError("stb:%s, column '%s' conflicts between parent '%s' and parent '%s'", pOld->name,
                   pAddParents[i]->pColumns[c].name, pAlter->parentStbFNames[pp], pAlter->parentStbFNames[i]);
            code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
            goto _ADD_OVER;
          }
        }
        for (int32_t tt = 0; tt < pAddParents[pp]->numOfTags; ++tt) {
          if (strcmp(pAddParents[pp]->pTags[tt].name, pAddParents[i]->pColumns[c].name) == 0) {
            mError("stb:%s, name '%s' conflicts between parent '%s' (tag) and parent '%s' (col)", pOld->name,
                   pAddParents[i]->pColumns[c].name, pAlter->parentStbFNames[pp], pAlter->parentStbFNames[i]);
            code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
            goto _ADD_OVER;
          }
        }
      }
    }
    for (int32_t t = 0; t < pAddParents[i]->numOfTags; ++t) {
      if (mndFindSuperTableColumnIndex(pOld, pAddParents[i]->pTags[t].name) >= 0 ||
          mndFindSuperTableTagIndex(pOld, pAddParents[i]->pTags[t].name) >= 0) {
        mError("stb:%s, tag '%s' conflicts between existing schema and parent '%s'", pOld->name,
               pAddParents[i]->pTags[t].name, pAlter->parentStbFNames[i]);
        code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
        goto _ADD_OVER;
      }
      for (int8_t pp = 0; pp < i; ++pp) {
        for (int32_t cc = 1; cc < pAddParents[pp]->numOfColumns; ++cc) {
          if (strcmp(pAddParents[pp]->pColumns[cc].name, pAddParents[i]->pTags[t].name) == 0) {
            mError("stb:%s, name '%s' conflicts between parent '%s' (col) and parent '%s' (tag)", pOld->name,
                   pAddParents[i]->pTags[t].name, pAlter->parentStbFNames[pp], pAlter->parentStbFNames[i]);
            code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
            goto _ADD_OVER;
          }
        }
        for (int32_t tt = 0; tt < pAddParents[pp]->numOfTags; ++tt) {
          if (strcmp(pAddParents[pp]->pTags[tt].name, pAddParents[i]->pTags[t].name) == 0) {
            mError("stb:%s, tag '%s' conflicts between parent '%s' and parent '%s'", pOld->name,
                   pAddParents[i]->pTags[t].name, pAlter->parentStbFNames[pp], pAlter->parentStbFNames[i]);
            code = TSDB_CODE_MND_VST_COL_NAME_CONFLICT;
            goto _ADD_OVER;
          }
        }
      }
    }

    // Skip ts column (index 0) from parent — child already has its own ts
    addCols += (pAddParents[i]->numOfColumns > 1) ? (pAddParents[i]->numOfColumns - 1) : 0;
    addTags += pAddParents[i]->numOfTags;

    pNew->parentSuids[pNew->numParents] = pAddParents[i]->uid;
    pNew->numParents++;
  }

  // Cycle detection
  if (mndCheckCyclicInherit(pMnode, pOld->uid, pNew->parentSuids, pNew->numParents)) {
    code = TSDB_CODE_MND_VST_CIRCULAR_INHERIT;
    goto _ADD_OVER;
  }

  // Merge new parent columns into schema
  {
    int16_t oldOwnColStart = pOld->ownColStart;
    int16_t oldOwnTagStart = pOld->ownTagStart;
    int32_t newNumCols = pOld->numOfColumns + addCols;
    int32_t newNumTags = pOld->numOfTags + addTags;

    pNew->numOfColumns = newNumCols;
    pNew->numOfTags = newNumTags;
    pNew->pColumns = taosMemoryCalloc(newNumCols, sizeof(SSchema));
    pNew->pTags = taosMemoryCalloc(newNumTags, sizeof(SSchema));
    pNew->pCmpr = taosMemoryCalloc(newNumCols, sizeof(SColCmpr));
    if (!pNew->pColumns || !pNew->pTags || !pNew->pCmpr) {
      code = terrno;
      goto _ADD_OVER;
    }
    if (pOld->pExtSchemas) {
      pNew->pExtSchemas = taosMemoryCalloc(newNumCols, sizeof(SExtSchema));
      if (!pNew->pExtSchemas) {
        code = terrno;
        goto _ADD_OVER;
      }
    }

    // Layout: [ts][old inherited cols][new parent cols][own cols (no ts)]
    // For standalone VST (ownColStart=0, numParents=0), ts is at index 0 and own cols start at 1.
    int16_t effectiveOwnColStart = (oldOwnColStart <= 1) ? 1 : oldOwnColStart;

    // Position 0: always ts column (never moves)
    int32_t dst = 0;
    pNew->pColumns[0] = pOld->pColumns[0];
    pNew->pCmpr[0] = pOld->pCmpr[0];
    if (pOld->pExtSchemas) pNew->pExtSchemas[0] = pOld->pExtSchemas[0];
    dst = 1;

    // Copy old inherited columns [1, effectiveOwnColStart)
    for (int32_t i = 1; i < effectiveOwnColStart; ++i) {
      pNew->pColumns[dst] = pOld->pColumns[i];
      pNew->pCmpr[dst] = pOld->pCmpr[i];
      if (pOld->pExtSchemas) pNew->pExtSchemas[dst] = pOld->pExtSchemas[i];
      dst++;
    }
    // Append new parent columns (skip ts col at index 0)
    col_id_t maxColId = 0;
    for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
      if (pOld->pColumns[i].colId > maxColId) maxColId = pOld->pColumns[i].colId;
    }
    for (int32_t i = 0; i < pOld->numOfTags; ++i) {
      if (pOld->pTags[i].colId > maxColId) maxColId = pOld->pTags[i].colId;
    }
    for (int8_t p = 0; p < numAdd; ++p) {
      for (int32_t c = 1; c < pAddParents[p]->numOfColumns; ++c) {
        pNew->pColumns[dst] = pAddParents[p]->pColumns[c];
        pNew->pColumns[dst].colId = ++maxColId;
        SColCmpr cmpr = {.id = pNew->pColumns[dst].colId,
                         .alg = createDefaultColCmprByType(pNew->pColumns[dst].type)};
        pNew->pCmpr[dst] = cmpr;
        if (pNew->pExtSchemas) memset(&pNew->pExtSchemas[dst], 0, sizeof(SExtSchema));
        dst++;
      }
    }
    // Copy own columns [effectiveOwnColStart, numOfColumns) — own non-ts columns
    for (int32_t i = effectiveOwnColStart; i < pOld->numOfColumns; ++i) {
      pNew->pColumns[dst] = pOld->pColumns[i];
      pNew->pCmpr[dst] = pOld->pCmpr[i];
      if (pOld->pExtSchemas) pNew->pExtSchemas[dst] = pOld->pExtSchemas[i];
      dst++;
    }

    // Tags: [old inherited tags][new parent tags][own tags]
    dst = 0;
    for (int32_t i = 0; i < oldOwnTagStart; ++i) {
      pNew->pTags[dst++] = pOld->pTags[i];
    }
    for (int8_t p = 0; p < numAdd; ++p) {
      for (int32_t t = 0; t < pAddParents[p]->numOfTags; ++t) {
        pNew->pTags[dst] = pAddParents[p]->pTags[t];
        pNew->pTags[dst].colId = ++maxColId;
        dst++;
      }
    }
    for (int32_t i = oldOwnTagStart; i < pOld->numOfTags; ++i) {
      pNew->pTags[dst++] = pOld->pTags[i];
    }

    pNew->ownColStart = effectiveOwnColStart + (int16_t)addCols;
    pNew->ownTagStart = oldOwnTagStart + (int16_t)addTags;
    pNew->colVer++;
    pNew->tagVer++;

    mInfo("stb:%s, added %d parent(s), cols %d->%d, tags %d->%d, ownColStart %d->%d, ownTagStart %d->%d",
          pOld->name, numAdd, pOld->numOfColumns, newNumCols, pOld->numOfTags, newNumTags,
          oldOwnColStart, pNew->ownColStart, oldOwnTagStart, pNew->ownTagStart);
  }

_ADD_OVER:
  for (int8_t r = 0; r < numAdd; ++r) {
    if (pAddParents[r]) mndReleaseStb(pMnode, pAddParents[r]);
  }
  TAOS_RETURN(code);
}

// ALTER STABLE DROP BASE ON - remove parent VSTs from inheritance list
static bool mndIsColFromParent(const SStbObj *pParent, const char *colName) {
  for (int32_t i = 0; i < pParent->numOfColumns; ++i) {
    if (strcmp(pParent->pColumns[i].name, colName) == 0) return true;
  }
  return false;
}

static bool mndIsTagFromParent(const SStbObj *pParent, const char *tagName) {
  for (int32_t i = 0; i < pParent->numOfTags; ++i) {
    if (strcmp(pParent->pTags[i].name, tagName) == 0) return true;
  }
  return false;
}

static bool mndContainsParentUid(const int64_t *pParentSuids, int8_t numParents, int64_t suid) {
  for (int8_t i = 0; i < numParents; ++i) {
    if (pParentSuids[i] == suid) {
      return true;
    }
  }
  return false;
}

// Invalidate hasChildren cache on parent STBs by their UIDs.
// Single-pass scan: O(numSTBs), sets all matching parents to -1 (unknown).
static void mndInvalidateParentHasChildrenCache(SMnode *pMnode, const int64_t *parentSuids, int8_t numParents) {
  if (numParents <= 0) return;

  SSdb *pSdb = pMnode->pSdb;
  bool  found[TSDB_MAX_VST_PARENTS] = {0};
  int8_t numFound = 0;
  void  *pIter = NULL;
  SStbObj *pStb = NULL;

  while (numFound < numParents) {
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;
    for (int8_t i = 0; i < numParents; ++i) {
      if (!found[i] && pStb->uid == parentSuids[i]) {
        atomic_store_8(&pStb->hasChildren, -1);
        found[i] = true;
        numFound++;
        break;
      }
    }
    sdbRelease(pSdb, pStb);
  }
  if (pIter) sdbCancelFetch(pSdb, pIter);
}

static int32_t mndAlterStbDropBaseOn(SMnode *pMnode, SStbObj *pOld, SStbObj *pNew, const SMAlterStbReq *pAlter) {
  int32_t code = 0;
  if (pAlter->numParents <= 0) {
    TAOS_RETURN(TSDB_CODE_MND_INVALID_STB_OPTION);
  }
  if (!pOld->virtualStb || pOld->numParents == 0) {
    TAOS_RETURN(TSDB_CODE_MND_INVALID_STB_OPTION);
  }

  // Collect parent STBs to drop (need their schemas to identify inherited columns)
  SStbObj *pDropParents[TSDB_MAX_VST_PARENTS] = {0};
  int8_t   numDrop = pAlter->numParents;

  for (int8_t i = 0; i < numDrop; ++i) {
    pDropParents[i] = mndAcquireStb(pMnode, (char *)pAlter->parentStbFNames[i]);
    if (pDropParents[i] == NULL) {
      for (int8_t r = 0; r < i; ++r) mndReleaseStb(pMnode, pDropParents[r]);
      TAOS_RETURN(TSDB_CODE_MND_STB_NOT_EXIST);
    }
  }

  // Copy existing parent list, then remove dropped parents
  pNew->numParents = pOld->numParents;
  memcpy(pNew->parentSuids, pOld->parentSuids, sizeof(pOld->parentSuids));

  for (int8_t i = 0; i < numDrop; ++i) {
    bool found = false;
    for (int8_t j = 0; j < pNew->numParents; ++j) {
      if (pNew->parentSuids[j] == pDropParents[i]->uid) {
        for (int8_t k = j; k < pNew->numParents - 1; ++k) {
          pNew->parentSuids[k] = pNew->parentSuids[k + 1];
        }
        pNew->parentSuids[pNew->numParents - 1] = 0;
        pNew->numParents--;
        found = true;
        break;
      }
    }
    if (!found) {
      for (int8_t r = 0; r < numDrop; ++r) mndReleaseStb(pMnode, pDropParents[r]);
      TAOS_RETURN(TSDB_CODE_MND_INVALID_STB_OPTION);
    }
  }

  // Acquire remaining (keep) parents to safely decide which inherited names still belong to a parent.
  // A column/tag is only dropped if it exists in dropped parents AND NOT in any remaining parent.
  SStbObj *pKeepParents[TSDB_MAX_VST_PARENTS] = {0};
  int8_t   numKeep = pNew->numParents;
  for (int8_t i = 0; i < numKeep; ++i) {
    char fname[TSDB_TABLE_FNAME_LEN] = {0};
    bool resolved = false;
    SSdb *pSdb = pMnode->pSdb;
    void *pIter = NULL;
    SStbObj *pStb = NULL;
    while (1) {
      pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
      if (pIter == NULL) break;
      if (pStb->uid == pNew->parentSuids[i]) {
        tstrncpy(fname, pStb->name, sizeof(fname));
        sdbRelease(pSdb, pStb);
        sdbCancelFetch(pSdb, pIter);
        resolved = true;
        break;
      }
      sdbRelease(pSdb, pStb);
    }
    if (resolved) {
      pKeepParents[i] = mndAcquireStb(pMnode, fname);
    }
  }

  // Build keep-flags for inherited columns [0, ownColStart) and tags [0, ownTagStart)
  int16_t oldOwnColStart = pOld->ownColStart;
  int16_t oldOwnTagStart = pOld->ownTagStart;

  bool *keepCol = taosMemoryCalloc(pOld->numOfColumns, sizeof(bool));
  bool *keepTag = taosMemoryCalloc(pOld->numOfTags, sizeof(bool));
  if (!keepCol || !keepTag) {
    taosMemoryFree(keepCol);
    taosMemoryFree(keepTag);
    for (int8_t r = 0; r < numDrop; ++r) mndReleaseStb(pMnode, pDropParents[r]);
    for (int8_t r = 0; r < numKeep; ++r) if (pKeepParents[r]) mndReleaseStb(pMnode, pKeepParents[r]);
    TAOS_RETURN(terrno);
  }

  // ts column (index 0) is always kept
  keepCol[0] = true;
  // Own columns/tags are always kept
  for (int32_t i = oldOwnColStart; i < pOld->numOfColumns; ++i) keepCol[i] = true;
  for (int32_t i = oldOwnTagStart; i < pOld->numOfTags; ++i) keepTag[i] = true;

  // Inherited columns [1, ownColStart): drop only if in a dropped parent AND not in any keep parent.
  for (int32_t i = 1; i < oldOwnColStart; ++i) {
    bool fromDropped = false;
    for (int8_t d = 0; d < numDrop; ++d) {
      if (mndIsColFromParent(pDropParents[d], pOld->pColumns[i].name)) {
        fromDropped = true;
        break;
      }
    }
    bool fromKeep = false;
    if (fromDropped) {
      for (int8_t k = 0; k < numKeep; ++k) {
        if (pKeepParents[k] != NULL &&
            (mndIsColFromParent(pKeepParents[k], pOld->pColumns[i].name) ||
             mndIsTagFromParent(pKeepParents[k], pOld->pColumns[i].name))) {
          fromKeep = true;
          break;
        }
      }
    }
    keepCol[i] = !fromDropped || fromKeep;
  }
  for (int32_t i = 0; i < oldOwnTagStart; ++i) {
    bool fromDropped = false;
    for (int8_t d = 0; d < numDrop; ++d) {
      if (mndIsTagFromParent(pDropParents[d], pOld->pTags[i].name)) {
        fromDropped = true;
        break;
      }
    }
    bool fromKeep = false;
    if (fromDropped) {
      for (int8_t k = 0; k < numKeep; ++k) {
        if (pKeepParents[k] != NULL &&
            (mndIsTagFromParent(pKeepParents[k], pOld->pTags[i].name) ||
             mndIsColFromParent(pKeepParents[k], pOld->pTags[i].name))) {
          fromKeep = true;
          break;
        }
      }
    }
    keepTag[i] = !fromDropped || fromKeep;
  }

  // Release parent refs
  for (int8_t r = 0; r < numDrop; ++r) mndReleaseStb(pMnode, pDropParents[r]);
  for (int8_t r = 0; r < numKeep; ++r) if (pKeepParents[r]) mndReleaseStb(pMnode, pKeepParents[r]);

  // Count surviving columns/tags
  int32_t newNumCols = 0, newInheritCols = 0;
  for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
    if (keepCol[i]) {
      if (i < oldOwnColStart) newInheritCols++;
      newNumCols++;
    }
  }
  int32_t newNumTags = 0, newInheritTags = 0;
  for (int32_t i = 0; i < pOld->numOfTags; ++i) {
    if (keepTag[i]) {
      if (i < oldOwnTagStart) newInheritTags++;
      newNumTags++;
    }
  }

  // Validate: ≥2 columns (TS + at least 1 more), ≥1 tag
  if (newNumCols < 2 || newNumTags < 1) {
    taosMemoryFree(keepCol);
    taosMemoryFree(keepTag);
    TAOS_RETURN(TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS);
  }

  // Allocate new schema arrays
  pNew->numOfColumns = newNumCols;
  pNew->numOfTags = newNumTags;
  pNew->pColumns = taosMemoryCalloc(newNumCols, sizeof(SSchema));
  pNew->pTags = taosMemoryCalloc(newNumTags, sizeof(SSchema));
  pNew->pCmpr = taosMemoryCalloc(newNumCols, sizeof(SColCmpr));
  if (!pNew->pColumns || !pNew->pTags || !pNew->pCmpr) {
    taosMemoryFree(keepCol);
    taosMemoryFree(keepTag);
    TAOS_RETURN(terrno);
  }
  if (pOld->pExtSchemas) {
    pNew->pExtSchemas = taosMemoryCalloc(newNumCols, sizeof(SExtSchema));
    if (!pNew->pExtSchemas) {
      taosMemoryFree(keepCol);
      taosMemoryFree(keepTag);
      TAOS_RETURN(terrno);
    }
  }

  // Copy surviving columns
  int32_t dst = 0;
  for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
    if (keepCol[i]) {
      pNew->pColumns[dst] = pOld->pColumns[i];
      pNew->pCmpr[dst] = pOld->pCmpr[i];
      if (pOld->pExtSchemas) pNew->pExtSchemas[dst] = pOld->pExtSchemas[i];
      dst++;
    }
  }

  // Copy surviving tags
  dst = 0;
  for (int32_t i = 0; i < pOld->numOfTags; ++i) {
    if (keepTag[i]) {
      pNew->pTags[dst] = pOld->pTags[i];
      dst++;
    }
  }

  pNew->ownColStart = (int16_t)newInheritCols;
  pNew->ownTagStart = (int16_t)newInheritTags;
  pNew->colVer++;
  pNew->tagVer++;

  taosMemoryFree(keepCol);
  taosMemoryFree(keepTag);

  mInfo("stb:%s, dropped %d parent(s), cols %d->%d, tags %d->%d, ownColStart %d->%d, ownTagStart %d->%d",
        pOld->name, numDrop, pOld->numOfColumns, newNumCols, pOld->numOfTags, newNumTags,
        oldOwnColStart, pNew->ownColStart, oldOwnTagStart, pNew->ownTagStart);

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
  stbObj.pExtSchemas = NULL;
  stbObj.updateTime = taosGetTimestampMs();
  stbObj.lock = 0;
  stbObj.virtualStb = pOld->virtualStb;
  // batch-meta-txn: propagate this ALTER's txnId so mndBuildVCreateStbReq (via
  // stbObj.txnId) forwards it to the vnode; without this the memcpy'd pOld->txnId
  // (typically 0, cleared once the prior CREATE txn committed) is used instead,
  // and the vnode-level ALTER_STB request silently carries no txnId.
  stbObj.txnId = pAlter->txnId;
  bool updateTagIndex = false;
  switch (pAlter->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot add tag: virtual stable has child inheritors", pOld->name);
        break;
      }
      code = mndAddSuperTableTag(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_DROP_TAG:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot drop tag: virtual stable has child inheritors", pOld->name);
        break;
      }
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndDropSuperTableTag(pMnode, pOld, &stbObj, pField0->name);
      updateTagIndex = true;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot rename tag: virtual stable has child inheritors", pOld->name);
        break;
      }
      code = mndAlterStbTagName(pMnode, pOld, &stbObj, pAlter->pFields);
      updateTagIndex = true;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot modify tag: virtual stable has child inheritors", pOld->name);
        break;
      }
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndAlterStbTagBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot add column: virtual stable has child inheritors", pOld->name);
        break;
      }
      code = mndAddSuperTableColumn(pOld, &stbObj, pAlter, pAlter->numOfFields, 0);
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      if (pOld->virtualStb && mndStbHasChildren(pMnode, pOld)) {
        code = TSDB_CODE_MND_VST_HAS_CHILDREN;
        mError("stb:%s, cannot drop column: virtual stable has child inheritors", pOld->name);
        break;
      }
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndDropSuperTableColumn(pMnode, pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      pField0 = taosArrayGet(pAlter->pFields, 0);
      code = mndAlterStbColumnBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      needRsp = false;
      code = mndUpdateTableOptions(pOld, &stbObj, pAlter->comment, pAlter->commentLen, pAlter->ttl, pAlter->keep,
                                   pAlter->secureDelete, pAlter->securityLevel);
#ifdef TD_ENTERPRISE
      // MAC: STB security_level must not be below DB security_level
      if (code == 0 && pAlter->securityLevel >= 0 && (uint8_t)pAlter->securityLevel < pDb->cfg.securityLevel) {
        mError("stb:%s, security_level %d below db security_level %d", pAlter->name, pAlter->securityLevel,
               pDb->cfg.securityLevel);
        code = TSDB_CODE_MAC_OBJ_LEVEL_BELOW_DB;
      }
#endif
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      code = mndUpdateSuperTableColumnCompress(pMnode, pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      code = mndAddSuperTableColumn(pOld, &stbObj, pAlter, pAlter->numOfFields, 1);
      break;
    case TSDB_ALTER_TABLE_ADD_BASE_ON:
      code = mndAlterStbAddBaseOn(pMnode, pOld, &stbObj, pAlter, pDb);
      break;
    case TSDB_ALTER_TABLE_DROP_BASE_ON:
      code = mndAlterStbDropBaseOn(pMnode, pOld, &stbObj, pAlter);
      break;
    default:
      needRsp = false;
      terrno = TSDB_CODE_OPS_NOT_SUPPORT;
      break;
  }

  if (code != 0) goto _OVER;
  if (pAlter->alterType == TSDB_ALTER_TABLE_ADD_BASE_ON) {
    // Only check newly added parents (starting at pOld->numParents)
    int8_t numNew = stbObj.numParents - pOld->numParents;
    code = mndAlterStbAddBaseOnImp(pMnode, pReq, pDb, &stbObj,
                                   &stbObj.parentSuids[pOld->numParents], numNew,
                                   pReq->pCont, pReq->contLen);
    if (code == 0 && numNew > 0) {
      mndInvalidateParentHasChildrenCache(pMnode, &stbObj.parentSuids[pOld->numParents], numNew);
    }
  } else if (pAlter->alterType == TSDB_ALTER_TABLE_DROP_BASE_ON) {
    // Check if this VST has any child VSTs before dropping inheritance.
    // If children exist, this change is a structural alteration and needs coordination.
    code = mndAlterStbDropBaseOnImp(pMnode, pReq, pDb, &stbObj, pOld,
                                    pReq->pCont, pReq->contLen);
    if (code == 0) {
      int64_t droppedParentSuids[TSDB_MAX_VST_PARENTS] = {0};
      int8_t  numDropped = 0;
      for (int8_t i = 0; i < pOld->numParents; ++i) {
        if (!mndContainsParentUid(stbObj.parentSuids, stbObj.numParents, pOld->parentSuids[i])) {
          droppedParentSuids[numDropped++] = pOld->parentSuids[i];
        }
      }
      if (numDropped > 0) {
        mndInvalidateParentHasChildrenCache(pMnode, droppedParentSuids, numDropped);
      }
    }
  } else if (updateTagIndex == false) {
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
  taosMemoryFreeClear(stbObj.pExtSchemas);
  TAOS_RETURN(code);
}

static int32_t mndProcessAlterStbReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SStbObj      *pStb = NULL;
  SUserObj     *pOperUser = NULL;
  SMAlterStbReq alterReq = {0};
  int64_t       tss = taosGetTimestampMs();

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
  if (pDb->cfg.isMount) {
    code = TSDB_CODE_MND_MOUNT_OBJ_NOT_SUPPORT;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, alterReq.name);
  if (pStb == NULL) {
    code = TSDB_CODE_MND_STB_NOT_EXIST;
    goto _OVER;
  }

  SName   name = {0};
  int32_t ret = 0;
  if ((ret = tNameFromString(&name, alterReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0)
    mError("stb:%s, failed to tNameFromString since %s", alterReq.name, tstrerror(ret));

  // if ((code = mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pDb)) != 0) {
  //   goto _OVER;
  // }
  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser), NULL, _OVER);

  // MAC: only superUser or user with PRIV_SECURITY_POLICY_ALTER can ALTER STABLE ... SECURITY_LEVEL
  // Check BEFORE general privilege check (holder may not have explicit ALTER grant)
  if (alterReq.securityLevel >= 0) {
#ifdef TD_ENTERPRISE
    // Virtual tables don't support security_level
    if (pStb->virtualStb) {
      mError("stb:%s, virtual table does not support ALTER SECURITY_LEVEL", alterReq.name);
      code = TSDB_CODE_PAR_INVALID_ALTER_TABLE;
      goto _OVER;
    }
    if (!mndUserHasMacLabelPriv(pMnode, pOperUser)) {
      mError("stb:%s, failed to alter security_level, user %s lacks PRIV_SECURITY_POLICY_ALTER", alterReq.name,
             pOperUser->user);
      code = TSDB_CODE_MND_NO_RIGHTS;
      goto _OVER;
    }
    // MAC must be active to set stb security_level > 0; before activation only user levels can be set.
    if (alterReq.securityLevel > 0 && pMnode->macActive != MAC_MODE_MANDATORY) {
      mError("stb:%s, failed to alter, cannot set security_level > 0 before MAC is activated", alterReq.name);
      code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
      goto _OVER;
    }
#endif
  } else {
    // Non-security_level ALTER requires normal DAC privilege checks
    TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pDb), NULL,
                    _OVER);
    TAOS_CHECK_GOTO(
        mndCheckDbPrivilegeByNameRecF(pMnode, pOperUser, PRIV_CM_ALTER, PRIV_OBJ_TBL, pDb->name, name.tname), NULL,
        _OVER);
  }

  // MAC clearance check: user.maxSecLevel must be >= stb.securityLevel to ALTER
  // Skip for security_level ALTER (SYSSEC manages security levels regardless of own level)
  if (alterReq.securityLevel < 0 && !pOperUser->superUser && pStb->securityLevel > 0 &&
      pOperUser->maxSecLevel < pStb->securityLevel) {
    mError("stb:%s, MAC access denied since user %s maxSecLevel(%d) < stb.securityLevel(%d) for ALTER",
           alterReq.name, pOperUser->user, pOperUser->maxSecLevel, pStb->securityLevel);
    code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
    goto _OVER;
  }

  // Batch meta txn: conflict detection — block if another txn owns this STB
  if (pStb->txnId != 0 && pStb->txnId != (txn_id_t)alterReq.txnId) {
    code = TSDB_CODE_TXN_RESOURCE_BUSY;
    goto _OVER;
  }
  if ((code = mndTxnCheckStbConflict(pMnode, alterReq.name, (txn_id_t)alterReq.txnId)) != 0) {
    goto _OVER;
  }

  // Batch meta txn: persist ALTER marker on SStbObj via Raft + memory shadow op.
  if (alterReq.txnId != 0) {
    // Validate the ALTER against the virtual current schema (base + prior in-txn ALTERs)
    {
      SStbObj  virtualStb = {0};
      SStbObj *pBase = pStb;
      SArray  *pPriorOps = NULL;
      int32_t  valCode = 0;

      // Get prior ALTER shadow ops for this STB in this txn
      valCode = mndTxnGetAlterOpsForStb(pMnode, (txn_id_t)alterReq.txnId, alterReq.name, &pPriorOps);
      if (valCode == 0 && pPriorOps != NULL && taosArrayGetSize(pPriorOps) > 0) {
        // Replay prior ops to build the virtual current schema
        taosRLockLatch(&pStb->lock);
        memcpy(&virtualStb, pStb, sizeof(SStbObj));
        virtualStb.pColumns = NULL;
        virtualStb.pTags = NULL;
        virtualStb.pFuncs = NULL;
        virtualStb.pCmpr = NULL;
        virtualStb.pExtSchemas = NULL;
        virtualStb.lock = 0;
        taosRUnLockLatch(&pStb->lock);
        valCode = mndAllocStbSchemas(pStb, &virtualStb);
        if (valCode != 0) {
          taosArrayDestroy(pPriorOps);
          code = valCode;
          goto _OVER;
        }

        int32_t numPrior = taosArrayGetSize(pPriorOps);
        for (int32_t i = 0; i < numPrior; i++) {
          SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pPriorOps, i);
          SMAlterStbReq priorReq = {0};
          if (tDeserializeSMAlterStbReq(pOp->pReqData, pOp->reqDataLen, &priorReq) != 0) {
            tFreeSMAltertbReq(&priorReq);
            continue;
          }
          SStbObj nextStb = {0};
          memcpy(&nextStb, &virtualStb, sizeof(SStbObj));
          nextStb.pColumns = NULL;
          nextStb.pTags = NULL;
          nextStb.pFuncs = NULL;
          nextStb.pCmpr = NULL;
          nextStb.pExtSchemas = NULL;
          nextStb.lock = 0;

          SField *pF = NULL;
          int32_t rc = 0;
          switch (priorReq.alterType) {
            case TSDB_ALTER_TABLE_ADD_TAG:
              rc = mndAddSuperTableTag(&virtualStb, &nextStb, priorReq.pFields, priorReq.numOfFields);
              break;
            case TSDB_ALTER_TABLE_DROP_TAG:
              pF = taosArrayGet(priorReq.pFields, 0);
              rc = mndDropSuperTableTag(pMnode, &virtualStb, &nextStb, pF->name);
              break;
            case TSDB_ALTER_TABLE_ADD_COLUMN:
              rc = mndAddSuperTableColumn(&virtualStb, &nextStb, &priorReq, priorReq.numOfFields, 0);
              break;
            case TSDB_ALTER_TABLE_DROP_COLUMN:
              pF = taosArrayGet(priorReq.pFields, 0);
              rc = mndDropSuperTableColumn(pMnode, &virtualStb, &nextStb, pF->name);
              break;
            case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
              pF = taosArrayGet(priorReq.pFields, 0);
              rc = mndAlterStbColumnBytes(pMnode, &virtualStb, &nextStb, pF);
              break;
            case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
              pF = taosArrayGet(priorReq.pFields, 0);
              rc = mndAlterStbTagBytes(pMnode, &virtualStb, &nextStb, pF);
              break;
            case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
              rc = mndAlterStbTagName(pMnode, &virtualStb, &nextStb, priorReq.pFields);
              break;
            case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
              rc = mndAddSuperTableColumn(&virtualStb, &nextStb, &priorReq, priorReq.numOfFields, 1);
              break;
            default:
              break;
          }
          tFreeSMAltertbReq(&priorReq);
          if (rc == 0) {
            taosMemoryFreeClear(virtualStb.pColumns);
            taosMemoryFreeClear(virtualStb.pTags);
            taosMemoryFreeClear(virtualStb.pCmpr);
            taosMemoryFreeClear(virtualStb.pExtSchemas);
            virtualStb = nextStb;
          } else {
            taosMemoryFreeClear(nextStb.pColumns);
            taosMemoryFreeClear(nextStb.pTags);
            taosMemoryFreeClear(nextStb.pCmpr);
            taosMemoryFreeClear(nextStb.pExtSchemas);
          }
        }
        pBase = &virtualStb;
      }
      taosArrayDestroy(pPriorOps);

      // Now validate the new ALTER against the virtual current schema
      SStbObj tmpStb = {0};
      memcpy(&tmpStb, pBase, sizeof(SStbObj));
      tmpStb.pColumns = NULL;
      tmpStb.pTags = NULL;
      tmpStb.pFuncs = NULL;
      tmpStb.pCmpr = NULL;
      tmpStb.pExtSchemas = NULL;
      tmpStb.lock = 0;

      SField *pField0 = NULL;
      switch (alterReq.alterType) {
        case TSDB_ALTER_TABLE_ADD_TAG:
          valCode = mndAddSuperTableTag(pBase, &tmpStb, alterReq.pFields, alterReq.numOfFields);
          break;
        case TSDB_ALTER_TABLE_DROP_TAG:
          pField0 = taosArrayGet(alterReq.pFields, 0);
          valCode = mndDropSuperTableTag(pMnode, pBase, &tmpStb, pField0->name);
          break;
        case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
          valCode = mndAlterStbTagName(pMnode, pBase, &tmpStb, alterReq.pFields);
          break;
        case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
          pField0 = taosArrayGet(alterReq.pFields, 0);
          valCode = mndAlterStbTagBytes(pMnode, pBase, &tmpStb, pField0);
          break;
        case TSDB_ALTER_TABLE_ADD_COLUMN:
          valCode = mndAddSuperTableColumn(pBase, &tmpStb, &alterReq, alterReq.numOfFields, 0);
          break;
        case TSDB_ALTER_TABLE_DROP_COLUMN:
          pField0 = taosArrayGet(alterReq.pFields, 0);
          valCode = mndDropSuperTableColumn(pMnode, pBase, &tmpStb, pField0->name);
          break;
        case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
          pField0 = taosArrayGet(alterReq.pFields, 0);
          valCode = mndAlterStbColumnBytes(pMnode, pBase, &tmpStb, pField0);
          break;
        case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
          valCode = mndUpdateSuperTableColumnCompress(pMnode, pBase, &tmpStb, alterReq.pFields, alterReq.numOfFields);
          break;
        case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
          valCode = mndAddSuperTableColumn(pBase, &tmpStb, &alterReq, alterReq.numOfFields, 1);
          break;
        case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
          valCode = mndUpdateTableOptions(pBase, &tmpStb, alterReq.comment, alterReq.commentLen, alterReq.ttl,
                                          alterReq.keep, alterReq.secureDelete, alterReq.securityLevel);
          break;
        default:
          break;
      }
      taosMemoryFreeClear(tmpStb.pColumns);
      taosMemoryFreeClear(tmpStb.pTags);
      taosMemoryFreeClear(tmpStb.pCmpr);
      taosMemoryFreeClear(tmpStb.pExtSchemas);
      if (alterReq.commentLen > 0) {
        taosMemoryFreeClear(tmpStb.comment);
      }

      // Clean up virtualStb if it was built
      if (pBase == &virtualStb) {
        taosMemoryFreeClear(virtualStb.pColumns);
        taosMemoryFreeClear(virtualStb.pTags);
        taosMemoryFreeClear(virtualStb.pCmpr);
        taosMemoryFreeClear(virtualStb.pExtSchemas);
      }

      if (valCode != 0) {
        code = valCode;
        goto _OVER;
      }
    }

    code = mndMarkStbTxnAlter(pMnode, pReq, pStb, pDb, (txn_id_t)alterReq.txnId, pReq->pCont, pReq->contLen);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  } else {
    code = mndAlterStb(pMnode, pReq, &alterReq, pDb, pStb);
    if (code == 0) {
      code = TSDB_CODE_ACTION_IN_PROGRESS;
    }
  }

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "alterStb", name.dbname, name.tname, alterReq.sql, alterReq.sqlLen, duration,
                0);
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to alter since %s", alterReq.name, tstrerror(code));
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pOperUser);
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
    // groupId=vgId so when this action is appended to the batch-txn commit STrans
    // alongside TDMT_VND_TXN_COMMIT (also groupId=vgId), STrans queues both in the
    // same per-VGroup group and dispatches them sequentially. Without this, the
    // TXN_COMMIT msg can race ahead of the DROP_STB on the destination VNode and
    // be observed as a no-op ("txn entry not found on commit"), leaving the STB
    // and cascade child markers stuck in PRE_DROP forever.
    action.groupId = pVgroup->vgId;
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
  TAOS_CHECK_GOTO(mndDropRsmaByStb(pMnode, pTrans, pDb, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndUserRemoveStb(pMnode, pTrans, pStb->name), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndTransDrop(pTrans);
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

    if (pStream->pCreate->outStbUid == suid) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pStream);
      TAOS_RETURN(-1);
    }

    sdbRelease(pSdb, pStream);
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessDropTtltbRsp(SRpcMsg *pRsp) { return 0; }
static int32_t mndProcessTrimDbRsp(SRpcMsg *pRsp) { return 0; }
static int32_t mndProcessTrimDbWalRsp(SRpcMsg *pRsp) { return 0; }
static int32_t mndProcessS3MigrateDbRsp(SRpcMsg *pRsp) { return 0; }

/**
 * Mark STB as PRE_DROP for txn crash recovery, and add memory shadow op.
 * Creates a mini-STrans with prepare-log to persist the marker in SDB via Raft.
 */
static int32_t mndMarkStbTxnDrop(SMnode *pMnode, SRpcMsg *pReq, SStbObj *pStb, SDbObj *pDb, txn_id_t txnId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "mark-stb-predrop")),
      code, lino, _exit, terrno);
  mndTransSetChangeless(pTrans);

  // Build marker SStbObj (shallow copy + override txn fields)
  SStbObj markerStb;
  taosRLockLatch(&pStb->lock);
  memcpy(&markerStb, pStb, sizeof(SStbObj));
  taosRUnLockLatch(&pStb->lock);
  markerStb.lock = 0;
  markerStb.txnId = txnId;
  // If STB was created in this same txn, use combined state; otherwise plain PRE_DROP
  markerStb.txnStatus = (markerStb.txnStatus == META_TXN_PRE_CREATE) ? META_TXN_PRE_CREATE_DROP : META_TXN_PRE_DROP;

  SSdbRaw *pRaw = mndStbActionEncode(&markerStb);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransAppendPrepareLog(pTrans, pRaw));

  // Broadcast the DROP to all vnodes in the DB immediately, with the real txnId, same as
  // CREATE/ALTER. vnodeProcessDropStbReq's txn path (metaDropSuperTable) handles both cases:
  // - STB pre-existing (NORMAL/PRE_ALTER): marks PRE_DROP, deferred — hides child tables from
  //   SHOW TABLES/VTABLES immediately, physically cascade-deletes when TXN_COMMIT arrives
  //   (vnodeTxnPromoteShadowEntries already does this generically for any tracked PRE_DROP uid).
  // - STB created in this same txn (PRE_CREATE_DROP): metaDropSuperTable recognizes the
  //   same-txn CREATE→DROP and physically deletes it right away — no need to wait for COMMIT.
  // Either way, no separate commit-time redo action is needed afterward.
  TAOS_CHECK_EXIT(mndSetDropStbRedoActions(pMnode, pTrans, pDb, &markerStb));

  // Add memory shadow op (for live COMMIT path — DROP only needs name, no reqData)
  code = mndTxnAddShadowOp(pMnode, txnId, MND_SHADOW_OP_DROP_STB, pStb->name, pStb->uid, pDb->name, NULL, 0);
  if (code != 0) goto _exit;

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * Append ALTER request data to SStbObj's txnAlterReqs chain for crash recovery.
 * Chain format: [count:int32] [len_0:int32][data_0] ... [len_N-1:int32][data_N-1]
 */
static int32_t mndStbBuildAlterChain(const SStbObj *pStb, const void *pReqData, int32_t reqDataLen,
                                     void **ppChain, int32_t *pChainLen) {
  int32_t oldLen = pStb->txnAlterReqsLen;
  int32_t oldCount = 0;
  if (pReqData == NULL || reqDataLen <= 0 || ppChain == NULL || pChainLen == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (oldLen < 0 || (oldLen > 0 && oldLen < (int32_t)sizeof(int32_t)) || (oldLen > 0 && pStb->pTxnAlterReqs == NULL)) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (oldLen >= (int32_t)sizeof(int32_t) && pStb->pTxnAlterReqs != NULL) {
    memcpy(&oldCount, pStb->pTxnAlterReqs, sizeof(int32_t));
  }

  if (oldCount >= TSDB_TXN_MAX_ALTER_PER_STB) {
    mError("stb:%s, ALTER chain count %d reached per-STB limit %d, please COMMIT and start a new transaction",
           pStb->name, oldCount, TSDB_TXN_MAX_ALTER_PER_STB);
    return TSDB_CODE_TXN_TOO_MANY_DDL_OPS;
  }

  if (oldLen > 0) {
    int32_t offset = (int32_t)sizeof(int32_t);
    for (int32_t i = 0; i < oldCount; ++i) {
      int32_t itemLen = 0;
      if (offset > oldLen - (int32_t)sizeof(int32_t)) {
        return TSDB_CODE_INVALID_MSG;
      }
      memcpy(&itemLen, (const char *)pStb->pTxnAlterReqs + offset, sizeof(int32_t));
      if (itemLen <= 0 || itemLen > oldLen - offset - (int32_t)sizeof(int32_t)) {
        return TSDB_CODE_INVALID_MSG;
      }
      offset += (int32_t)sizeof(int32_t) + itemLen;
    }
    if (offset != oldLen) {
      return TSDB_CODE_INVALID_MSG;
    }
  }

  int32_t newCount = oldCount + 1;
  int32_t headerLen = (oldLen > 0 ? oldLen : (int32_t)sizeof(int32_t));
  if (reqDataLen > INT32_MAX - headerLen - (int32_t)sizeof(int32_t)) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t newLen = headerLen + (int32_t)sizeof(int32_t) + reqDataLen;
  if (newLen < headerLen || newLen < reqDataLen) {
    return TSDB_CODE_INVALID_MSG;
  }

  void *pNew = taosMemoryMalloc(newLen);
  if (pNew == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  // Write count
  memcpy(pNew, &newCount, sizeof(int32_t));

  // Copy old entries (skip old count header)
  if (oldLen > (int32_t)sizeof(int32_t) && pStb->pTxnAlterReqs != NULL) {
    memcpy((char *)pNew + sizeof(int32_t),
           (char *)pStb->pTxnAlterReqs + sizeof(int32_t),
           oldLen - sizeof(int32_t));
  }

  // Append new entry: [len][data]
  int32_t offset = headerLen;
  memcpy((char *)pNew + offset, &reqDataLen, sizeof(int32_t));
  memcpy((char *)pNew + offset + sizeof(int32_t), pReqData, reqDataLen);

  *ppChain = pNew;
  *pChainLen = newLen;
  return 0;
}

/**
 * Mark STB with ALTER request data for txn crash recovery, and add memory shadow op.
 * Creates a mini-STrans with prepare-log to persist the marker in SDB via Raft.
 */
static int32_t mndMarkStbTxnAlter(SMnode *pMnode, SRpcMsg *pReq, SStbObj *pStb, SDbObj *pDb, txn_id_t txnId,
                                  void *pReqData, int32_t reqDataLen) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;
  void   *pNewChain = NULL;
  int32_t newChainLen = 0;

  // Build new ALTER chain: old chain + new entry
  TAOS_CHECK_EXIT(mndStbBuildAlterChain(pStb, pReqData, reqDataLen, &pNewChain, &newChainLen));

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "mark-stb-alter")),
      code, lino, _exit, terrno);
  mndTransSetChangeless(pTrans);

  // Build marker SStbObj (shallow copy + override txn fields)
  SStbObj markerStb;
  taosRLockLatch(&pStb->lock);
  memcpy(&markerStb, pStb, sizeof(SStbObj));
  taosRUnLockLatch(&pStb->lock);
  markerStb.lock = 0;
  markerStb.txnId = txnId;
  markerStb.pTxnAlterReqs = pNewChain;
  markerStb.txnAlterReqsLen = newChainLen;

  SSdbRaw *pRaw = mndStbActionEncode(&markerStb);
  // Null out so we don't accidentally use stale pointer
  markerStb.pTxnAlterReqs = NULL;
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransAppendPrepareLog(pTrans, pRaw));

  // Add memory shadow op with ALTER request data
  {
    void *pShadowData = taosMemoryMalloc(reqDataLen);
    if (pShadowData == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    memcpy(pShadowData, pReqData, reqDataLen);
    code = mndTxnAddShadowOp(pMnode, txnId, MND_SHADOW_OP_ALTER_STB, pStb->name, pStb->uid, pDb->name, pShadowData,
                             reqDataLen);
    if (code != 0) {
      taosMemoryFreeClear(pShadowData);
      goto _exit;
    }
  }

  // Build overlaid schema response so client can update pTxnTableMeta cache.
  // This ensures subsequent parser lookups within the same txn see the new schema.
  {
    SArray *pAlterOps = NULL;
    int32_t rspCode = mndTxnGetAlterOpsForStb(pMnode, txnId, pStb->name, &pAlterOps);
    if (rspCode == 0 && pAlterOps != NULL && taosArrayGetSize(pAlterOps) > 0) {
      SName tbName = {0};
      tNameFromString(&tbName, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

      STableMetaRsp metaRsp = {0};
      rspCode = mndApplyTxnAlterOpsToSchema(pMnode, pAlterOps, pDb, pStb, tbName.tname, &metaRsp, false);
      if (rspCode == 0) {
        SMAlterStbRsp alterRsp = {.pMeta = &metaRsp};
        SEncoder      ec = {0};
        uint32_t      contLen = 0;
        tEncodeSize(tEncodeSMAlterStbRsp, &alterRsp, contLen, rspCode);
        if (rspCode == 0) {
          void *pCont = taosMemoryMalloc(contLen);
          if (pCont != NULL) {
            tEncoderInit(&ec, pCont, contLen);
            if (tEncodeSMAlterStbRsp(&ec, &alterRsp) == 0) {
              mndTransSetRpcRsp(pTrans, pCont, contLen);
            } else {
              taosMemoryFree(pCont);
            }
            tEncoderClear(&ec);
          }
        }
      }
      tFreeSTableMetaRsp(&metaRsp);
    }
    taosArrayDestroy(pAlterOps);
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  taosMemoryFree(pNewChain);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * Append DROP STB actions to an existing Trans (used for txn commit/rollback).
 * Adds SDB prepare+commit logs + VNode DROP STB redo actions to pTrans, without creating a new Trans.
 */
int32_t mndAppendDropStbToTrans(SMnode *pMnode, STrans *pTrans, const char *stbName) {
  int32_t  code = 0;
  SStbObj *pStb = mndAcquireStb(pMnode, (char *)stbName);
  if (pStb == NULL) {
    mWarn("stb:%s, not found in SDB, skip drop", stbName);
    return TSDB_CODE_SUCCESS;  // idempotent
  }

  SDbObj *pDb = mndAcquireDbByStb(pMnode, stbName);
  if (pDb == NULL) {
    mndReleaseStb(pMnode, pStb);
    mWarn("stb:%s, db not found, skip drop", stbName);
    return TSDB_CODE_SUCCESS;  // idempotent
  }

  mInfo("stb:%s, appending drop actions to trans:%d", stbName, pTrans->id);

  code = mndSetDropStbPrepareLogs(pMnode, pTrans, pStb);
  if (code == 0) {
    code = mndSetDropStbCommitLogs(pMnode, pTrans, pStb);
  }
  // No VNode redo action needed here: the VNode already tracked this STB's PRE_DROP/PRE_CREATE
  // marker when the DROP (or CREATE) was originally dispatched mid-txn (mndMarkStbTxnDrop /
  // immediate CREATE dispatch), so vnodeProcessTxnCommitReq/vnodeProcessTxnRollbackReq's generic
  // shadow-entry promotion/undo (vnodeTxnPromoteShadowEntries / vnodeTxnUndoShadowEntries)
  // already performs the physical drop automatically once TXN_COMMIT/TXN_ROLLBACK arrives.
  if (code == 0) {
    code = mndDropIdxsByStb(pMnode, pTrans, pDb, pStb);
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  return code;
}

/**
 * Append ALTER STB actions to an existing Trans (used for txn commit).
 * Deserializes the SMAlterStbReq, builds modified SStbObj, adds SDB logs + VNode actions.
 */
/**
 * Append one ALTER STB operation to a commit trans.
 *
 * @param pAccumBase  Optional: previously-accumulated SStbObj from an earlier ALTER on the
 *                    same STB within this COMMIT.  When non-NULL, it is used as the schema
 *                    base instead of reading from SDB, so each subsequent ALTER in the same
 *                    txn builds on top of the previous one.  Caller retains ownership.
 * @param ppAccumResult Optional out-param: on success, receives a heap-allocated SStbObj
 *                    holding the schema after this ALTER (to be passed as pAccumBase to the
 *                    next ALTER on the same STB).  Caller must free via mndFreeStb() + taosMemoryFree().
 *                    Set to NULL on success when the ALTER was skipped (e.g. STB being dropped).
 *
 * pAst1/pAst2 are intentionally zeroed below: ALTER operations never touch AST fields, and
 * they must not be inherited into the accumulated schema (which is heap-allocated and freed
 * independently of SDB).  This allows callers to free the result with the standard mndFreeStb.
 */
int32_t mndAppendAlterStbToTrans(SMnode *pMnode, STrans *pTrans, void *pReqData, int32_t reqDataLen,
                                 SStbObj *pAccumBase, SStbObj **ppAccumResult) {
  int32_t       code = 0;
  SMAlterStbReq alterReq = {0};
  SStbObj      *pOld = NULL;
  bool          pOldFromSdb = false;  // whether pOld was acquired via mndAcquireStb
  SDbObj       *pDb = NULL;
  SStbObj       stbObj = {0};
  void         *pAlterCont = NULL;
  bool          stbBuilt = false;

  if (ppAccumResult != NULL) *ppAccumResult = NULL;

  if (tDeserializeSMAlterStbReq(pReqData, reqDataLen, &alterReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  // batch-meta-txn: this ALTER's real txnId, forwarded explicitly to mndSetAlterStbRedoActions
  // below so the vnode-alter-stb wire message is tagged for WAL/CDC atomic ordered delivery.
  // Kept on alterReq/alterOriData too (only consumed downstream by the client-side JSON-meta
  // builder in clientRawBlockJson.c, which ignores txnId — no need to clear it here).
  txn_id_t txnId = (txn_id_t)alterReq.txnId;

  if (pAccumBase != NULL) {
    // Use accumulated schema from previous ALTER on this STB in the same COMMIT.
    pOld = pAccumBase;
    pOldFromSdb = false;
  } else {
    pOld = mndAcquireStb(pMnode, alterReq.name);
    if (pOld == NULL) {
      code = TSDB_CODE_MND_STB_NOT_EXIST;
      goto _OVER;
    }
    pOldFromSdb = true;
  }
  pDb = mndAcquireDbByStb(pMnode, alterReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  mInfo("stb:%s, appending alter actions to trans:%d", alterReq.name, pTrans->id);

  // Build modified SStbObj (same logic as mndAlterStb)
  taosRLockLatch(&pOld->lock);
  memcpy(&stbObj, pOld, sizeof(SStbObj));
  taosRUnLockLatch(&pOld->lock);
  // Save txnStatus before clearing: used to detect whether a DROP will follow
  // in the same COMMIT trans (PRE_CREATE_DROP or PRE_DROP → skip entire ALTER).
  EMetaTxnStatus oldTxnStatus = stbObj.txnStatus;

  // For PRE_CREATE_DROP or PRE_DROP: the STB will be dropped in the same COMMIT trans.
  // The ALTER was already applied to SDB during the txn, so pOld already has the new
  // schema. Attempting to re-apply the schema modification (e.g. mndAddSuperTableColumn)
  // would fail with "column already exists". Since the DROP will clean up the STB from
  // both SDB and VNodes, skip the ENTIRE ALTER operation here.
  if (oldTxnStatus == META_TXN_PRE_CREATE_DROP || oldTxnStatus == META_TXN_PRE_DROP) {
    mInfo("stb:%s, skipping ALTER (txnStatus=%d, STB being dropped in same COMMIT trans:%d)", alterReq.name,
          (int)oldTxnStatus, pTrans->id);
    goto _OVER;  // code=0, clean success — DROP shadow op handles the cleanup
  }

  stbObj.pColumns = NULL;
  stbObj.pTags = NULL;
  stbObj.pFuncs = NULL;
  stbObj.pAst1 = NULL;  // not owned by accumulated schema; see mndAppendAlterStbToTrans doc
  stbObj.pAst2 = NULL;
  stbObj.pCmpr = NULL;
  stbObj.pExtSchemas = NULL;
  stbObj.pTxnAlterReqs = NULL;
  stbObj.txnAlterReqsLen = 0;
  stbObj.updateTime = taosGetTimestampMs();
  stbObj.lock = 0;
  stbObj.txnId = 0;  // COMMIT promotes STB: clear txnId so it becomes visible
  stbObj.txnStatus = META_TXN_NORMAL;  // Clear txn markers
  stbBuilt = true;

  SField *pField0 = NULL;
  switch (alterReq.alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
      code = mndAddSuperTableTag(pOld, &stbObj, alterReq.pFields, alterReq.numOfFields);
      break;
    case TSDB_ALTER_TABLE_DROP_TAG:
      pField0 = taosArrayGet(alterReq.pFields, 0);
      code = mndDropSuperTableTag(pMnode, pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
      code = mndAlterStbTagName(pMnode, pOld, &stbObj, alterReq.pFields);
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      pField0 = taosArrayGet(alterReq.pFields, 0);
      code = mndAlterStbTagBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      code = mndAddSuperTableColumn(pOld, &stbObj, &alterReq, alterReq.numOfFields, 0);
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      pField0 = taosArrayGet(alterReq.pFields, 0);
      code = mndDropSuperTableColumn(pMnode, pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      pField0 = taosArrayGet(alterReq.pFields, 0);
      code = mndAlterStbColumnBytes(pMnode, pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      code = mndUpdateTableOptions(pOld, &stbObj, alterReq.comment, alterReq.commentLen, alterReq.ttl, alterReq.keep,
                                   alterReq.secureDelete, alterReq.securityLevel);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      code = mndUpdateSuperTableColumnCompress(pMnode, pOld, &stbObj, alterReq.pFields, alterReq.numOfFields);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      code = mndAddSuperTableColumn(pOld, &stbObj, &alterReq, alterReq.numOfFields, 1);
      break;
    default:
      code = TSDB_CODE_OPS_NOT_SUPPORT;
      break;
  }
  if (code != 0) goto _OVER;

  // Re-serialize altered request for VNode redo actions
  int32_t newLen = tSerializeSMAlterStbReq(NULL, 0, &alterReq);
  pAlterCont = taosMemoryMalloc(newLen);
  if (pAlterCont == NULL) {
    code = terrno;
    goto _OVER;
  }
  if (tSerializeSMAlterStbReq(pAlterCont, newLen, &alterReq) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  // Add SDB logs and VNode actions to pTrans.
  // When the STB is about to be dropped in the same COMMIT trans (PRE_CREATE_DROP or
  // PRE_DROP), skip vnode-alter-stb redo actions. Sending vnode-alter-stb concurrently
  // with vnode-drop-stb causes a race: VNode uses separate queues for different message
  // types, so DROP can be processed before ALTER, leaving ALTER with "Stable not exists"
  // (TSDB_CODE_TDB_STB_NOT_EXIST). Since the DROP redo action will clean up the VNode
  // state anyway, the ALTER redo action is both unnecessary and harmful.
  bool skipVnodeAlter = (oldTxnStatus == META_TXN_PRE_CREATE_DROP || oldTxnStatus == META_TXN_PRE_DROP);
  code = mndSetAlterStbPrepareLogs(pMnode, pTrans, pDb, &stbObj);
  if (code == 0) code = mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, &stbObj);
  // stbObj.txnId stays 0 throughout (needed above so the SDB prepare/commit logs make the
  // STB visible on commit); the real batch txnId is passed explicitly here instead, so the
  // vnode-alter-stb wire message is tagged for WAL/CDC atomic ordered delivery.
  if (code == 0 && !skipVnodeAlter) {
    code = mndSetAlterStbRedoActions(pMnode, pTrans, pDb, &stbObj, pAlterCont, newLen, txnId);
  }

  // On success, hand the accumulated schema to the caller so subsequent ALTERs on the
  // same STB within the same COMMIT can build on top of it (avoiding lost-update).
  if (code == 0 && ppAccumResult != NULL && stbBuilt) {
    SStbObj *pResult = taosMemoryCalloc(1, sizeof(SStbObj));
    if (pResult == NULL) {
      code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    } else {
      // Shallow copy: transfer ownership of heap fields to pResult.
      memcpy(pResult, &stbObj, sizeof(SStbObj));
      stbObj.pColumns = NULL;  // prevent double-free at _OVER
      stbObj.pTags = NULL;
      stbObj.pCmpr = NULL;
      stbObj.comment = NULL;
      stbObj.pExtSchemas = NULL;
      *ppAccumResult = pResult;
    }
  }

_OVER:
  taosMemoryFree(pAlterCont);
  if (stbBuilt) {
    taosMemoryFreeClear(stbObj.pTags);
    taosMemoryFreeClear(stbObj.pColumns);
    taosMemoryFreeClear(stbObj.pCmpr);
    if (alterReq.commentLen > 0) taosMemoryFreeClear(stbObj.comment);
    taosMemoryFreeClear(stbObj.pExtSchemas);
  }
  if (pDb) mndReleaseDb(pMnode, pDb);
  if (pOldFromSdb && pOld) mndReleaseStb(pMnode, pOld);
  tFreeSMAltertbReq(&alterReq);
  return code;
}

static int32_t mndProcessDropStbReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SStbObj     *pStb = NULL;
  SUserObj    *pOperUser = NULL;
  SMDropStbReq dropReq = {0};
  int64_t      tss = taosGetTimestampMs();

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

  SName   name = {0};
  int32_t ret = 0;
  if ((ret = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0)
    mError("stb:%s, failed to tNameFromString since %s", dropReq.name, tstrerror(ret));

  // if ((code = mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pDb)) != 0) {
  //   goto _OVER;
  // }
  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pDb), NULL,
                  _OVER);
  TAOS_CHECK_GOTO(
      mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_DROP, PRIV_OBJ_TBL, pStb->ownerId, pDb->name, name.tname),
      NULL, _OVER);

  // MAC clearance check: user.maxSecLevel must be >= stb.securityLevel to DROP
  if (!pOperUser->superUser && pStb->securityLevel > 0 && pOperUser->maxSecLevel < pStb->securityLevel) {
    mError("stb:%s, MAC access denied since user %s maxSecLevel(%d) < stb.securityLevel(%d) for DROP",
           dropReq.name, pOperUser->user, pOperUser->maxSecLevel, pStb->securityLevel);
    code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
    goto _OVER;
  }

  if (pDb->cfg.isMount) {
    code = TSDB_CODE_MND_MOUNT_OBJ_NOT_SUPPORT;
    goto _OVER;
  }

  // VST inheritance: refuse DROP if this VST has child VSTs
  if (pStb->virtualStb && mndStbHasChildren(pMnode, pStb)) {
    code = TSDB_CODE_MND_VST_HAS_CHILDREN;
    goto _OVER;
  }

  // Batch meta txn: conflict detection — block if another txn owns this STB
  if (pStb->txnId != 0 && pStb->txnId != (txn_id_t)dropReq.txnId) {
    code = TSDB_CODE_TXN_RESOURCE_BUSY;
    goto _OVER;
  }
  // RYOW: same txn already marked this STB as dropped — return not-exist
  if (dropReq.txnId != 0 && pStb->txnId == (txn_id_t)dropReq.txnId &&
      (pStb->txnStatus == META_TXN_PRE_DROP || pStb->txnStatus == META_TXN_PRE_CREATE_DROP)) {
    if (dropReq.igNotExists) {
      code = 0;
    } else {
      code = TSDB_CODE_MND_STB_NOT_EXIST;
    }
    goto _OVER;
  }
  if ((code = mndTxnCheckStbConflict(pMnode, dropReq.name, (txn_id_t)dropReq.txnId)) != 0) {
    goto _OVER;
  }

  // Batch meta txn: persist PRE_DROP marker on SStbObj via Raft + memory shadow op.
  if (dropReq.txnId != 0) {
    code = mndMarkStbTxnDrop(pMnode, pReq, pStb, pDb, (txn_id_t)dropReq.txnId);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  } else {
    code = mndDropStb(pMnode, pReq, pDb, pStb);
    if (code == 0) {
      // VST inheritance: invalidate parent caches so they recompute hasChildren on next query
      mndInvalidateParentHasChildrenCache(pMnode, pStb->parentSuids, pStb->numParents);
      code = TSDB_CODE_ACTION_IN_PROGRESS;
    }
  }

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "dropStb", name.dbname, name.tname, dropReq.sql, dropReq.sqlLen, duration, 0);
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to drop since %s", dropReq.name, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSMDropStbReq(&dropReq);
  TAOS_RETURN(code);
}

/**
 * Apply ALTER STB shadow ops to rebuild a table meta response.
 * Clones the SStbObj from SDB, applies each ALTER op in sequence,
 * then rebuilds the metaRsp from the modified clone.
 *
 * @param pMnode    MNode
 * @param pAlterOps SArray of SMndShadowOp (ALTER ops only, in order)
 * @param pDb       Database object
 * @param pBaseStb  Base SStbObj from SDB
 * @param tbName    Table name (short name for response)
 * @param pRsp      Output metaRsp (existing content will be freed and rebuilt)
 * @param refByStm  Whether referenced by statement
 */
static int32_t mndApplyTxnAlterOpsToSchema(SMnode *pMnode, SArray *pAlterOps, SDbObj *pDb, SStbObj *pBaseStb,
                                           const char *tbName, STableMetaRsp *pRsp, bool refByStm) {
  int32_t code = 0;
  int32_t numOps = taosArrayGetSize(pAlterOps);

  // Deep clone the base SStbObj
  SStbObj current = {0};
  taosRLockLatch(&pBaseStb->lock);
  memcpy(&current, pBaseStb, sizeof(SStbObj));
  current.pColumns = NULL;
  current.pTags = NULL;
  current.pFuncs = NULL;
  current.pCmpr = NULL;
  current.pExtSchemas = NULL;
  current.lock = 0;
  taosRUnLockLatch(&pBaseStb->lock);

  // Allocate initial column/tag/cmpr arrays via mndAllocStbSchemas
  // (copies pBaseStb's columns/tags/cmpr to current)
  code = mndAllocStbSchemas(pBaseStb, &current);
  if (code != 0) goto _DONE;

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pAlterOps, i);

    SMAlterStbReq alterReq = {0};
    if (tDeserializeSMAlterStbReq(pOp->pReqData, pOp->reqDataLen, &alterReq) != 0) {
      code = TSDB_CODE_INVALID_MSG;
      tFreeSMAltertbReq(&alterReq);
      goto _DONE;
    }

    SStbObj next = {0};
    memcpy(&next, &current, sizeof(SStbObj));
    next.pColumns = NULL;
    next.pTags = NULL;
    next.pFuncs = NULL;
    next.pCmpr = NULL;
    next.pExtSchemas = NULL;
    next.lock = 0;
    next.updateTime = taosGetTimestampMs();

    SField *pField0 = NULL;
    switch (alterReq.alterType) {
      case TSDB_ALTER_TABLE_ADD_TAG:
        code = mndAddSuperTableTag(&current, &next, alterReq.pFields, alterReq.numOfFields);
        break;
      case TSDB_ALTER_TABLE_DROP_TAG:
        pField0 = taosArrayGet(alterReq.pFields, 0);
        code = mndDropSuperTableTag(pMnode, &current, &next, pField0->name);
        break;
      case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
        code = mndAlterStbTagName(pMnode, &current, &next, alterReq.pFields);
        break;
      case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
        pField0 = taosArrayGet(alterReq.pFields, 0);
        code = mndAlterStbTagBytes(pMnode, &current, &next, pField0);
        break;
      case TSDB_ALTER_TABLE_ADD_COLUMN:
        code = mndAddSuperTableColumn(&current, &next, &alterReq, alterReq.numOfFields, 0);
        break;
      case TSDB_ALTER_TABLE_DROP_COLUMN:
        pField0 = taosArrayGet(alterReq.pFields, 0);
        code = mndDropSuperTableColumn(pMnode, &current, &next, pField0->name);
        break;
      case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
        pField0 = taosArrayGet(alterReq.pFields, 0);
        code = mndAlterStbColumnBytes(pMnode, &current, &next, pField0);
        break;
      case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
        code = mndUpdateTableOptions(&current, &next, alterReq.comment, alterReq.commentLen, alterReq.ttl,
                                     alterReq.keep, alterReq.secureDelete, alterReq.securityLevel);
        break;
      case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
        code = mndUpdateSuperTableColumnCompress(pMnode, &current, &next, alterReq.pFields, alterReq.numOfFields);
        break;
      case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
        code = mndAddSuperTableColumn(&current, &next, &alterReq, alterReq.numOfFields, 1);
        break;
      default:
        code = TSDB_CODE_OPS_NOT_SUPPORT;
        break;
    }

    tFreeSMAltertbReq(&alterReq);

    if (code != 0) {
      taosMemoryFreeClear(next.pColumns);
      taosMemoryFreeClear(next.pTags);
      taosMemoryFreeClear(next.pCmpr);
      taosMemoryFreeClear(next.pExtSchemas);
      goto _DONE;
    }

    // Move to next iteration: free current arrays, adopt next's
    taosMemoryFreeClear(current.pColumns);
    taosMemoryFreeClear(current.pTags);
    taosMemoryFreeClear(current.pCmpr);
    taosMemoryFreeClear(current.pExtSchemas);
    current = next;
  }

  // Rebuild metaRsp from the modified clone
  tFreeSTableMetaRsp(pRsp);
  memset(pRsp, 0, sizeof(STableMetaRsp));
  code = mndBuildStbSchemaImp(pMnode, pDb, &current, tbName, pRsp, refByStm);

_DONE:
  taosMemoryFreeClear(current.pColumns);
  taosMemoryFreeClear(current.pTags);
  taosMemoryFreeClear(current.pCmpr);
  taosMemoryFreeClear(current.pExtSchemas);
  return code;
}

static int32_t mndProcessTableMetaReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  STableInfoReq infoReq = {0};
  STableMetaRsp metaRsp = {0};
  SUserObj     *pUser = NULL;

  code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pUser);
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
    // batch-meta-txn: hide PRE_CREATE/PRE_CREATE_DROP STBs from other sessions.
    // PRE_ALTER and PRE_DROP remain visible (redo-log model — schema unchanged until COMMIT).
    {
      char tbFName[TSDB_TABLE_FNAME_LEN] = {0};
      snprintf(tbFName, sizeof(tbFName), "%s.%s", infoReq.dbFName, infoReq.tbName);
      SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
      if (pStb != NULL && pStb->txnId != 0) {
        bool otherSession = (pStb->txnId != (txn_id_t)infoReq.txnId);
        bool isPreCreate = (pStb->txnStatus == META_TXN_PRE_CREATE || pStb->txnStatus == META_TXN_PRE_CREATE_DROP);
        bool isPreDrop = (pStb->txnStatus == META_TXN_PRE_DROP || pStb->txnStatus == META_TXN_PRE_CREATE_DROP);
        // Other sessions: PRE_CREATE tables are invisible (not yet committed).
        // Same session:   RYOW — table was already dropped in this txn, return not found.
        if ((otherSession && isPreCreate) || (!otherSession && isPreDrop)) {
          mInfo("stb:%s, txn %" PRIu64 " status=%d, requester txnId=%" PRId64 ", deny access (%s)", tbFName,
                pStb->txnId, pStb->txnStatus, infoReq.txnId, otherSession ? "hidden PRE_CREATE" : "RYOW PRE_DROP");
          mndReleaseStb(pMnode, pStb);
          code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
          goto _OVER;
        }
      }
      if (pStb) mndReleaseStb(pMnode, pStb);
    }
    TAOS_CHECK_GOTO(mndBuildStbSchema(pMnode, infoReq.dbFName, infoReq.tbName, &metaRsp, true), NULL, _OVER);

    // Same-txn ALTER visibility: overlay ALTER shadow ops from the active txn
    if (infoReq.txnId != 0) {
      SArray *pAlterOps = NULL;
      char    stbFName[TSDB_TABLE_FNAME_LEN] = {0};
      snprintf(stbFName, sizeof(stbFName), "%s.%s", infoReq.dbFName, infoReq.tbName);
      int32_t txnCode = mndTxnGetAlterOpsForStb(pMnode, (txn_id_t)infoReq.txnId, stbFName, &pAlterOps);
      if (txnCode == 0 && pAlterOps != NULL && taosArrayGetSize(pAlterOps) > 0) {
        SDbObj  *pDb = mndAcquireDb(pMnode, infoReq.dbFName);
        SStbObj *pStb = mndAcquireStb(pMnode, stbFName);
        if (pDb != NULL && pStb != NULL) {
          int32_t rc = mndApplyTxnAlterOpsToSchema(pMnode, pAlterOps, pDb, pStb, infoReq.tbName, &metaRsp, true);
          if (rc != 0) {
            mWarn("stb:%s, failed to overlay ALTER shadow ops for txn %" PRId64 ": %s", stbFName, infoReq.txnId,
                  tstrerror(rc));
          } else {
            mInfo("stb:%s, overlaid %d ALTER shadow ops for same-txn visibility (txn %" PRId64 ")", stbFName,
                  (int32_t)taosArrayGetSize(pAlterOps), infoReq.txnId);
          }
        }
        if (pStb) mndReleaseStb(pMnode, pStb);
        if (pDb) mndReleaseDb(pMnode, pDb);
      }
      taosArrayDestroy(pAlterOps);
    }
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
      if (mndBuildStbSchema(pMnode, pStbVersion->dbFName, pStbVersion->stbName, &metaRsp, false) != 0) {
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

      (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", pStbVersion->dbFName, pStbVersion->stbName);
      tstrncpy(indexRsp.dbFName, pStbVersion->dbFName, sizeof(indexRsp.dbFName));
      tstrncpy(indexRsp.tbName, pStbVersion->stbName, sizeof(indexRsp.tbName));

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

static int32_t mndRetrieveStb(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  SStbObj   *pStb = NULL;
  SUserObj  *pOperUser = NULL;
  SSHashObj *pUidNames = NULL;
  int32_t    cols = 0;
  int32_t    lino = 0;
  int32_t    code = 0;
  char       objFName[TSDB_OBJ_FNAME_LEN + 1] = {0};
  bool       showAll = false;

  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return terrno;
  }

  if ((code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser)) != 0) {
    goto _ERROR;
  }

  (void)snprintf(objFName, sizeof(objFName), "%d.*", pOperUser->acctId);
  showAll = (0 == mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_SHOW, PRIV_OBJ_TBL, pDb ? pDb->ownerId : 0,
                                           pDb ? pDb->name : objFName, "*"));
  showAll = showAll && (0 == mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                                       pDb ? pDb->name : objFName, false));

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL && pStb->dbUid != pDb->uid) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    if (pStb->txnId != 0) {
      if (pStb->txnStatus == META_TXN_PRE_CREATE) {
        if (pStb->txnId != (txn_id_t)pShow->txnId) {
          sdbRelease(pSdb, pStb);
          continue;
        }
      } else if (pStb->txnStatus == META_TXN_PRE_CREATE_DROP) {
        sdbRelease(pSdb, pStb);
        continue;
      } else if (pStb->txnStatus == META_TXN_PRE_DROP) {
        if (pStb->txnId == (txn_id_t)pShow->txnId) {
          sdbRelease(pSdb, pStb);
          continue;
        }
      }
    }

    if (isTsmaResSTb(pStb->name)) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    if (pOperUser->superUser == 0 && pMnode->macActive == MAC_MODE_MANDATORY && pStb->securityLevel > 0 &&
        pOperUser->maxSecLevel < pStb->securityLevel) {
      sdbRelease(pSdb, pStb);
      continue;
    }

#if 0
    if ((0 == pUser->superUser) && mndCheckStbPrivilege(pMnode, pUser, RPC_MSG_TOKEN(pReq), MND_OPER_SHOW_STB, pStb) != 0) {
      sdbRelease(pSdb, pStb);
      terrno = 0;
      continue;
    }
#endif
    cols = 0;

    SName name = {0};

    char stbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    mndExtractTbNameFromStbFullName(pStb->name, &stbName[VARSTR_HEADER_SIZE], TSDB_TABLE_NAME_LEN);

    if (!showAll && (mndCheckObjPrivilegeRecF(pMnode, pOperUser, PRIV_CM_SHOW, PRIV_OBJ_TBL, pStb->ownerId, pStb->db,
                                              &stbName[VARSTR_HEADER_SIZE]) ||
                     mndCheckDbPrivilegeByName(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB,
                                               pStb->db, false))) {
      sdbRelease(pSdb, pStb);
      terrno = 0;
      continue;
    }

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
    (void)snprintf(varDataVal(watermark), sizeof(watermark) - VARSTR_HEADER_SIZE, "%" PRId64 "a,%" PRId64 "a",
                   pStb->watermark[0], pStb->watermark[1]);
    varDataSetLen(watermark, strlen(varDataVal(watermark)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)watermark, false), pStb, &lino, _ERROR);

    char maxDelay[64 + VARSTR_HEADER_SIZE] = {0};
    (void)snprintf(varDataVal(maxDelay), sizeof(maxDelay) - VARSTR_HEADER_SIZE, "%" PRId64 "a,%" PRId64 "a",
                   pStb->maxdelay[0], pStb->maxdelay[1]);
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

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pColInfo) {
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)(&pStb->virtualStb), false), pStb, &lino, _ERROR);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pColInfo) {
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)(&pStb->keep), false), pStb, &lino, _ERROR);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pColInfo) {
      if (!pUidNames) {
        TAOS_CHECK_GOTO(mndBuildUidNamesHash(pMnode, &pUidNames), &lino, _OVER);
      }
      const char *ownerName = tSimpleHashGet(pUidNames, (const char *)&pStb->ownerId, sizeof(pStb->ownerId));
      char        owner[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(owner, ownerName ? ownerName : "[unknown]", sizeof(owner));
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)owner, false), pStb, &lino, _ERROR);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pColInfo) {
      uint8_t securityLevel = pStb->securityLevel;
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)(&securityLevel), false), pStb, &lino,
                          _ERROR);
    }

    numOfRows++;
    sdbRelease(pSdb, pStb);
  }

  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }
  if (pOperUser != NULL) {
    mndReleaseUser(pMnode, pOperUser);
  }

  goto _OVER;

_ERROR:
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }
  if (pOperUser != NULL) {
    mndReleaseUser(pMnode, pOperUser);
  }
  mError("show:0x%" PRIx64 ", failed to retrieve data at %s:%d since %s", pShow->id, __FUNCTION__, lino,
         tstrerror(code));

_OVER:
  pShow->numOfRows += numOfRows;
  tSimpleHashCleanup(pUidNames);
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
      int  colTypeLen =
          snprintf(varDataVal(colTypeStr), sizeof(colTypeStr) - VARSTR_HEADER_SIZE, "%s", tDataTypes[colType].name);
      if (colType == TSDB_DATA_TYPE_VARCHAR) {
        colTypeLen +=
            snprintf(varDataVal(colTypeStr) + colTypeLen, sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE, "(%d)",
                     (int32_t)(pm->schema[j].bytes - VARSTR_HEADER_SIZE));
      } else if (colType == TSDB_DATA_TYPE_NCHAR) {
        colTypeLen +=
            snprintf(varDataVal(colTypeStr) + colTypeLen, sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE, "(%d)",
                     (int32_t)((pm->schema[j].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
      }
      varDataSetLen(colTypeStr, colTypeLen);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, (char *)colTypeStr, false), &lino, _OVER);

      // col length
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      TAOS_CHECK_GOTO(colDataSetVal(pColInfoData, numOfRows, (const char *)&pm->schema[j].bytes, false), &lino, _OVER);

      // col precision, col scale, col nullable, col source
      for (int32_t k = 6; k <= 10; ++k) {
        pColInfoData = taosArrayGet(p->pDataBlock, k);
        colDataSetNULL(pColInfoData, numOfRows);
      }

      numOfRows += 1;
    }
  }
  return numOfRows;
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
        int  colTypeLen =
            snprintf(varDataVal(colTypeStr), sizeof(colTypeStr) - VARSTR_HEADER_SIZE, "%s", tDataTypes[colType].name);
        if (colType == TSDB_DATA_TYPE_VARCHAR) {
          colTypeLen +=
              snprintf(varDataVal(colTypeStr) + colTypeLen, sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE,
                       "(%d)", (int32_t)(pStb->pColumns[i].bytes - VARSTR_HEADER_SIZE));
        } else if (colType == TSDB_DATA_TYPE_NCHAR) {
          colTypeLen +=
              snprintf(varDataVal(colTypeStr) + colTypeLen, sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE,
                       "(%d)", (int32_t)((pStb->pColumns[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
        } else if (IS_DECIMAL_TYPE(colType)) {
          STypeMod typeMod = pStb->pExtSchemas[i].typeMod;
          uint8_t prec = 0, scale = 0;
          decimalFromTypeMod(typeMod, &prec, &scale);
          colTypeLen += snprintf(varDataVal(colTypeStr) + colTypeLen,
                                 sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE, "(%d,%d)", prec, scale);
        }
        varDataSetLen(colTypeStr, colTypeLen);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (char *)colTypeStr, false), pStb, &lino, _OVER);

        // col length
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->pColumns[i].bytes, false), pStb,
                            &lino, _OVER);
        
        // col precision, col scale, col nullable, col source
        for (int32_t j = 6; j <= 9; ++j) {
          pColInfo = taosArrayGet(pBlock->pDataBlock, j);
          colDataSetNULL(pColInfo, numOfRows);
        }

        // col id
        pColInfo = taosArrayGet(pBlock->pDataBlock, 10);
        RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->pColumns[i].colId, false), pStb,
                            &lino, _OVER);
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

// Retrieve inheritance relationships for ins_vstable_inherits system table
static int32_t mndRetrieveVstableInherits(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SStbObj *pStb = NULL;
  int32_t  code = 0;

  // Build uid -> shortName hash map once, avoiding O(C*P*N) inner sdbFetch loops.
  SHashObj *pNameMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pNameMap) {
    void    *pIter = NULL;
    SStbObj *pTmp = NULL;
    while ((pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pTmp))) {
      char shortName[TSDB_TABLE_NAME_LEN] = {0};
      mndExtractTbNameFromStbFullName(pTmp->name, shortName, TSDB_TABLE_NAME_LEN);
      taosHashPut(pNameMap, &pTmp->uid, sizeof(int64_t), shortName, TSDB_TABLE_NAME_LEN);
      sdbRelease(pSdb, pTmp);
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
    if (pShow->pIter == NULL) break;

    if (pStb->numParents <= 0) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    // Extract child db name and table name
    char childDbName[TSDB_DB_FNAME_LEN] = {0};
    char childStbName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractShortDbNameFromStbFullName(pStb->name, childDbName);
    mndExtractTbNameFromStbFullName(pStb->name, childStbName, TSDB_TABLE_NAME_LEN);

    for (int8_t i = 0; i < pStb->numParents && numOfRows < rows; ++i) {
      // Look up parent name from hash map: O(1) instead of O(N) sdbFetch scan
      char parentName[TSDB_TABLE_NAME_LEN] = {0};
      char *pName = pNameMap ? (char *)taosHashGet(pNameMap, &pStb->parentSuids[i], sizeof(int64_t)) : NULL;
      if (pName) {
        memcpy(parentName, pName, TSDB_TABLE_NAME_LEN);
      } else {
        // Fallback: linear scan when the hash map allocation OR an individual put
        // failed (a map miss must not silently emit a blank parent name).
        void    *pIter2 = NULL;
        SStbObj *pParent = NULL;
        while (1) {
          pIter2 = sdbFetch(pSdb, SDB_STB, pIter2, (void **)&pParent);
          if (pIter2 == NULL) break;
          if (pParent->uid == pStb->parentSuids[i]) {
            mndExtractTbNameFromStbFullName(pParent->name, parentName, TSDB_TABLE_NAME_LEN);
            sdbRelease(pSdb, pParent);
            sdbCancelFetch(pSdb, pIter2);
            break;
          }
          sdbRelease(pSdb, pParent);
        }
      }

      int32_t cols = 0;
      SColumnInfoData *pColInfo;
      char             buf[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

      // db_name
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      STR_TO_VARSTR(buf, childDbName);
      colDataSetVal(pColInfo, numOfRows, buf, false);

      // parent_stable_name
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      STR_TO_VARSTR(buf, parentName);
      colDataSetVal(pColInfo, numOfRows, buf, false);

      // parent_uid
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->parentSuids[i], false);

      // child_stable_name
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      STR_TO_VARSTR(buf, childStbName);
      colDataSetVal(pColInfo, numOfRows, buf, false);

      // child_uid
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->uid, false);

      // create_time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pStb->createdTime, false);

      numOfRows++;
    }
    sdbRelease(pSdb, pStb);
  }

  taosHashCleanup(pNameMap);
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STB);
}

static int32_t mndProcessGetVstLeavesReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  int32_t       code = TSDB_CODE_SUCCESS;
  SVstLeavesReq req = {0};
  SVstLeavesRsp rsp = {0};

  SArray       *queue = NULL;
  SHashObj     *seen = NULL;
  SHashObj     *nonLeaf = NULL;
  SArray       *allDescs = NULL;
  SArray       *leavesArr = NULL;
  SHashObj     *pChildMap = NULL;  // parentUid → SArray of child UIDs (reverse index)
  SHashObj     *pInfoMap = NULL;   // uid → SVstLeafInfo (name lookup)

  if (tDeserializeSVstLeavesReq(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  queue = taosArrayInit(16, sizeof(int64_t));
  seen = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  nonLeaf = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  allDescs = taosArrayInit(16, sizeof(int64_t));
  leavesArr = taosArrayInit(16, sizeof(SVstLeafInfo));
  pChildMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  pInfoMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (queue == NULL || seen == NULL || nonLeaf == NULL || allDescs == NULL || leavesArr == NULL ||
      pChildMap == NULL || pInfoMap == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  // Build reverse index and name map in one O(N) scan
  {
    void    *pIter = NULL;
    SStbObj *pStb = NULL;
    while ((pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb))) {
      // Name map: uid → SVstLeafInfo
      SVstLeafInfo info = {.suid = pStb->uid};
      mndExtractDbNameFromStbFullName(pStb->name, info.dbFName);
      mndExtractTbNameFromStbFullName(pStb->name, info.stbName, TSDB_TABLE_NAME_LEN);
      if (taosHashPut(pInfoMap, &pStb->uid, sizeof(int64_t), &info, sizeof(SVstLeafInfo)) != 0) {
        // A missing info entry would silently drop this table from the leaf set below.
        sdbRelease(pSdb, pStb);
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _OVER;
      }

      // Reverse index: parentUid → SArray of child UIDs
      for (int8_t i = 0; i < pStb->numParents; ++i) {
        int64_t puid = pStb->parentSuids[i];
        SArray **ppArr = (SArray **)taosHashGet(pChildMap, &puid, sizeof(int64_t));
        if (ppArr == NULL) {
          SArray *arr = taosArrayInit(4, sizeof(int64_t));
          if (arr == NULL) { sdbRelease(pSdb, pStb); code = TSDB_CODE_OUT_OF_MEMORY; goto _OVER; }
          if (taosHashPut(pChildMap, &puid, sizeof(int64_t), &arr, sizeof(SArray *)) != 0) {
            // Insert failed: arr is not reachable via pChildMap, so free it here to
            // avoid a leak, then fail fast rather than silently dropping the edge.
            taosArrayDestroy(arr);
            sdbRelease(pSdb, pStb);
            code = TSDB_CODE_OUT_OF_MEMORY;
            goto _OVER;
          }
          ppArr = (SArray **)taosHashGet(pChildMap, &puid, sizeof(int64_t));
        }
        if (ppArr == NULL || *ppArr == NULL || taosArrayPush(*ppArr, &pStb->uid) == NULL) {
          // A dropped parent→child edge would yield an incomplete leaf set.
          sdbRelease(pSdb, pStb);
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _OVER;
        }
      }

      sdbRelease(pSdb, pStb);
    }
  }

  if (taosArrayPush(queue, &req.suid) == NULL ||
      taosHashPut(seen, &req.suid, sizeof(int64_t), &req.suid, sizeof(int64_t)) != 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  // BFS down: find all descendants using O(1) hash lookup per step
  size_t qHead = 0;
  while (qHead < taosArrayGetSize(queue)) {
    int64_t curSuid = *(int64_t *)taosArrayGet(queue, qHead++);
    SArray **ppChildren = (SArray **)taosHashGet(pChildMap, &curSuid, sizeof(int64_t));
    if (ppChildren == NULL || *ppChildren == NULL) continue;

    size_t numChildren = taosArrayGetSize(*ppChildren);
    for (size_t c = 0; c < numChildren; ++c) {
      int64_t child = *(int64_t *)taosArrayGet(*ppChildren, c);
      taosHashPut(nonLeaf, &curSuid, sizeof(int64_t), &curSuid, sizeof(int64_t));
      if (taosHashGet(seen, &child, sizeof(int64_t)) == NULL) {
        if (taosArrayPush(allDescs, &child) == NULL ||
            taosArrayPush(queue, &child) == NULL ||
            taosHashPut(seen, &child, sizeof(int64_t), &child, sizeof(int64_t)) != 0) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _OVER;
        }
      }
    }
  }

  // Among descendants, find leaves using O(1) hash lookup for names
  size_t numDescs = taosArrayGetSize(allDescs);
  for (size_t d = 0; d < numDescs; ++d) {
    int64_t suid = *(int64_t *)taosArrayGet(allDescs, d);
    if (taosHashGet(nonLeaf, &suid, sizeof(int64_t)) != NULL) continue;
    SVstLeafInfo *pInfo = (SVstLeafInfo *)taosHashGet(pInfoMap, &suid, sizeof(int64_t));
    if (pInfo == NULL) continue;
    if (taosArrayPush(leavesArr, pInfo) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }
  }

  rsp.numLeaves = (int32_t)taosArrayGetSize(leavesArr);
  rsp.pLeaves = (rsp.numLeaves > 0) ? (SVstLeafInfo *)TARRAY_DATA(leavesArr) : NULL;

  int32_t rspLen = tSerializeSVstLeavesRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    code = rspLen;
    goto _OVER;
  }
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }
  if (tSerializeSVstLeavesRsp(pRsp, rspLen, &rsp) < 0) {
    rpcFreeCont(pRsp);
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;

_OVER:
  // Clean up SArray* values stored in pChildMap
  if (pChildMap) {
    void *pIt = NULL;
    while ((pIt = taosHashIterate(pChildMap, pIt)) != NULL) {
      SArray **ppArr = (SArray **)pIt;
      if (*ppArr) taosArrayDestroy(*ppArr);
    }
    taosHashCleanup(pChildMap);
  }
  if (pInfoMap) taosHashCleanup(pInfoMap);
  if (queue) taosArrayDestroy(queue);
  if (seen) taosHashCleanup(seen);
  if (nonLeaf) taosHashCleanup(nonLeaf);
  if (allDescs) taosArrayDestroy(allDescs);
  if (leavesArr) taosArrayDestroy(leavesArr);
  return code;
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
  if (mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_WRITE_DB, pDb) != 0) {
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
  SArray     *pBatchReqs;
  SVgroupInfo info;
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
  SHashObj *pVgMap;  // <vgId, SVDropTbVgReqs>
} SMndDropTbsWithTsmaCtx;

static int32_t mndDropTbForSingleVg(SMnode *pMnode, SMndDropTbsWithTsmaCtx *pCtx, SArray *pTbs, int32_t vgId);

static void destroySVDropTbBatchReqs(void *p);
static void mndDestroyDropTbsWithTsmaCtx(SMndDropTbsWithTsmaCtx *p) {
  if (!p) return;

  if (p->pVgMap) {
    void *pIter = taosHashIterate(p->pVgMap, NULL);
    while (pIter) {
      SVDropTbVgReqs *pReqs = pIter;
      taosArrayDestroyEx(pReqs->pBatchReqs, destroySVDropTbBatchReqs);
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
                                        int32_t contLen, tmsg_t msgType) {
  STransAction action = {0};
  action.epSet = pVgReqs->info.epSet;
  action.pCont = pCont;
  action.contLen = contLen;
  action.msgType = msgType;
  action.acceptableCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
  return mndTransAppendRedoAction(pTrans, &action);
}

static int32_t mndBuildDropTbRedoActions(SMnode *pMnode, STrans *pTrans, SHashObj *pVgMap, tmsg_t msgType) {
  int32_t code = 0;
  void   *pIter = taosHashIterate(pVgMap, NULL);
  while (pIter) {
    const SVDropTbVgReqs *pVgReqs = pIter;
    int32_t               len = 0;
    for (int32_t i = 0; i < taosArrayGetSize(pVgReqs->pBatchReqs) && code == TSDB_CODE_SUCCESS; ++i) {
      SVDropTbBatchReq *pBatchReq = taosArrayGet(pVgReqs->pBatchReqs, i);
      void             *p = mndBuildVDropTbsReq(pMnode, &pVgReqs->info, pBatchReq, &len);
      if (!p) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        break;
      }
      if ((code = mndSetDropTbsRedoActions(pMnode, pTrans, pVgReqs, p, len, msgType)) != 0) {
        break;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCancelIterate(pVgMap, pIter);
      break;
    }
    pIter = taosHashIterate(pVgMap, pIter);
  }
  return code;
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

  if ((code = mndBuildDropTbRedoActions(pMnode, pTrans, pCtx->pVgMap, TDMT_VND_DROP_TABLE)) != 0) goto _OVER;
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
    code = mndDropTbForSingleVg(pMnode, pCtx, pReq->pTbs, pReq->vgInfo.vgId);
    if (code) goto _OVER;
  }
  code = mndCreateDropTbsTxnPrepare(pReq, pCtx);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }
_OVER:
  tFreeSMDropTbsReq(&dropReq);
  if (pCtx) mndDestroyDropTbsWithTsmaCtx(pCtx);
  TAOS_RETURN(code);
}

static int32_t createDropTbBatchReq(const SVDropTbReq *pReq, SVDropTbBatchReq *pBatchReq) {
  pBatchReq->nReqs = 1;
  pBatchReq->pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
  if (!pBatchReq->pArray) return terrno;
  if (taosArrayPush(pBatchReq->pArray, pReq) == NULL) {
    taosArrayDestroy(pBatchReq->pArray);
    pBatchReq->pArray = NULL;
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static void destroySVDropTbBatchReqs(void *p) {
  SVDropTbBatchReq *pReq = p;
  taosArrayDestroy(pReq->pArray);
  pReq->pArray = NULL;
}

static int32_t mndDropTbAdd(SMnode *pMnode, SHashObj *pVgHashMap, const SVgroupInfo *pVgInfo, char *name, tb_uid_t suid,
                            bool ignoreNotExists) {
  SVDropTbReq req = {.name = name, .suid = suid, .igNotExists = ignoreNotExists, .uid = 0};

  SVDropTbVgReqs *pVgReqs = taosHashGet(pVgHashMap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  SVDropTbVgReqs  vgReqs = {0};
  if (pVgReqs == NULL) {
    vgReqs.info = *pVgInfo;
    vgReqs.pBatchReqs = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbBatchReq));
    if (!vgReqs.pBatchReqs) return terrno;
    SVDropTbBatchReq batchReq = {0};
    int32_t          code = createDropTbBatchReq(&req, &batchReq);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (taosArrayPush(vgReqs.pBatchReqs, &batchReq) == NULL) {
      taosArrayDestroy(batchReq.pArray);
      return terrno;
    }
    if (taosHashPut(pVgHashMap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &vgReqs, sizeof(vgReqs)) != 0) {
      taosArrayDestroyEx(vgReqs.pBatchReqs, destroySVDropTbBatchReqs);
      return terrno;
    }
  } else {
    SVDropTbBatchReq batchReq = {0};
    int32_t          code = createDropTbBatchReq(&req, &batchReq);
    if (TSDB_CODE_SUCCESS != code) return code;
    if (taosArrayPush(pVgReqs->pBatchReqs, &batchReq) == NULL) {
      taosArrayDestroy(batchReq.pArray);
      return terrno;
    }
  }
  return 0;
}

static int32_t mndDropTbForSingleVg(SMnode *pMnode, SMndDropTbsWithTsmaCtx *pCtx, SArray *pTbs, int32_t vgId) {
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

  for (int32_t i = 0; i < pTbs->size; ++i) {
    SVDropTbReq *pTb = taosArrayGet(pTbs, i);
    TAOS_CHECK_GOTO(mndDropTbAdd(pMnode, pCtx->pVgMap, &vgInfo, pTb->name, pTb->suid, pTb->igNotExists), NULL, _end);
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

  code = mndDropTbForSingleVg(pMnode, pCtx, rsp.pExpiredTbs, rsp.vgId);
  if (code) goto _end;
  code = mndCreateDropTbsTxnPrepare(pRsp, pCtx);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_end:
  if (pCtx) mndDestroyDropTbsWithTsmaCtx(pCtx);
  tDecoderClear(&decoder);
  tFreeFetchTtlExpiredTbsRsp(&rsp);
  TAOS_RETURN(code);
}
