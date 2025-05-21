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
#include "mndEncKey.h"
#include "mndTrans.h"
#include "mndShow.h"
#include "tmsgcb.h"
#include "tmisce.h"
#include "audit.h"
#include "mndPrivilege.h"
#include "mndTrans.h"

#define MND_ENC_KEY_VER_NUMBER 1

static int32_t mndProcessAKEncReq(SRpcMsg *pReq);

int32_t mndInitEncKey(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCKEY, mndRetrieveEncKey);
  mndSetMsgHandle(pMnode, TDMT_MND_AK_GEN, mndProcessAKGenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESTORE_DNODE, mndProcessAKEncReq);

  SSdbTable table = {
      .sdbType = SDB_ENC_KEY,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndEncKeyActionEncode,
      .decodeFp = (SdbDecodeFp)mndEncKeyActionDecode,
      .insertFp = (SdbInsertFp)mndEncKeyActionInsert,
      .updateFp = (SdbUpdateFp)mndEncKeyActionUpdate,
      .deleteFp = (SdbDeleteFp)mndEncKeyActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupEncKey(SMnode *pMnode) {
  mDebug("mnd EncKey cleanup");
}

int32_t tSerializeSEncKeyObj(void *buf, int32_t bufLen, const SEncKeyObj *pObj) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pObj->Id) < 0) return -1;
  if (tEncodeCStr(&encoder, pObj->key) < 0) return -1;
  if (tEncodeI64(&encoder, pObj->createTime) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSEncKeytObj(void *buf, int32_t bufLen, SEncKeyObj *pObj) {
  int8_t ex = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pObj->Id) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pObj->key) < 0) return -1;
  if (tDecodeI64(&decoder, &pObj->createTime) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

SSdbRaw *mndEncKeyActionEncode(SEncKeyObj *pEncKey) {
  terrno = TSDB_CODE_SUCCESS;

  void *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSEncKeyObj(NULL, 0, pEncKey);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_ENC_KEY, MND_ENC_KEY_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSEncKeyObj(buf, tlen, pEncKey);
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
    mError("enckey:%" PRId32 ", failed to encode to raw:%p since %s", pEncKey->Id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("enckey:%" PRId32 ", encode to raw:%p, row:%p", pEncKey->Id, pRaw, pEncKey);
  return pRaw;
}

SSdbRow *mndEncKeyActionDecode(SSdbRaw *pRaw) {
  SSdbRow       *pRow = NULL;
  SEncKeyObj    *pEncKey = NULL;
  void          *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_ENC_KEY_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("compact read invalid ver, data ver: %d, curr ver: %d", sver, MND_ENC_KEY_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SEncKeyObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pEncKey = sdbGetRowObj(pRow);
  if (pEncKey == NULL) {
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

  if (tDeserializeSEncKeytObj(buf, tlen, pEncKey) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  //taosInitRWLatch(&pView->lock);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("enckey:%" PRId32 ", failed to decode from raw:%p since %s", pEncKey->Id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("enckey:%" PRId32 ", decode from raw:%p, row:%p", pEncKey->Id, pRaw, pEncKey);
  return pRow;
}

int32_t mndEncKeyActionInsert(SSdb *pSdb, SEncKeyObj *pEncKey) {
  mTrace("enckey:%" PRId32 ", perform insert action", pEncKey->Id);
  return 0;
}

int32_t mndEncKeyActionDelete(SSdb *pSdb, SEncKeyObj *pEncKey) {
  mTrace("enckey:%" PRId32 ", perform insert action", pEncKey->Id);
  return 0;
}

int32_t mndEncKeyActionUpdate(SSdb *pSdb, SEncKeyObj *pOldEncKey, SEncKeyObj *pNewEncKey) {
  mTrace("enckey:%" PRId32 ", perform update action, old row:%p new row:%p", 
          pOldEncKey->Id, pOldEncKey, pNewEncKey);

  return 0;
}

//retrieve EncKey
int32_t mndRetrieveEncKey(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SEncKeyObj   *pEnckey = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ENC_KEY, pShow->pIter, (void **)&pEnckey);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pEnckey->Id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    strncpy(varDataVal(tmpBuf), pEnckey->key, 128);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pEnckey->createTime, false);

    numOfRows++;
    sdbRelease(pSdb, pEnckey);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

int32_t mndProcessAKGenReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  SAKGenReq akGenReq = {0};
  if (tDeserializeSAKGenReq(pReq->pCont, pReq->contLen, &akGenReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }
  
  mInfo("start to ak gen");

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "akgen");
  if (pTrans == NULL) {
    mError("trans:%" PRId32 ", failed to create since %s" , pTrans->id, terrstr());
    return -1;
  }

  int64_t t = taosGetTimestampMs();
  for(int32_t i = 0; i < akGenReq.count; i++){
    SEncKeyObj enckey;
    enckey.Id = i;
    enckey.createTime = t;
    sprintf(enckey.key, "ak-%d", i);

    SSdbRaw *pEncKey = mndEncKeyActionEncode(&enckey);
    if (pEncKey == NULL || mndTransAppendCommitlog(pTrans, pEncKey) != 0) {
      mError("enckey:%d, trans:%d, failed to append commit log since %s", enckey.Id, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      return -1;
    }
    (void)sdbSetRawStatus(pEncKey, SDB_STATUS_READY);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0; 
}

int32_t mndProcessAKEncReq(SRpcMsg *pReq) {
  int32_t code = -1;
  SMnode *pMnode = pReq->info.node;

  SRestoreDnodeReq restoreReq = {0};

  if (tDeserializeSRestoreDnodeReq(pReq->pCont, pReq->contLen, &restoreReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("dnode:%d, start to akenc, restore type:%d, %s, %s", restoreReq.dnodeId, restoreReq.restoreType, restoreReq.db,
        restoreReq.tb);

  code = 0;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to restore, restoreType:%d,  since %s", restoreReq.dnodeId, restoreReq.restoreType,
           terrstr());
  }

  tFreeSRestoreDnodeReq(&restoreReq);
  return code;
}