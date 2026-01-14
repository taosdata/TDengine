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

#define MND_ENCRYPT_ALGR_VER_NUMBER 1

#include "audit.h"
#include "mndEncryptAlgr.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "tencrypt.h"

static void tFreeEncryptAlgrObj(SEncryptAlgrObj *pEncryptAlgr) {}

static int32_t tSerializeSEncryptAlgrObj(void *buf, int32_t bufLen, const SEncryptAlgrObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->algorithm_id));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->desc));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pObj->type));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->source));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->ossl_algr_name));

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

static int32_t tDeserializeSEncryptAlgrObj(void *buf, int32_t bufLen, SEncryptAlgrObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->algorithm_id));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->desc));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pObj->type));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->source));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->ossl_algr_name));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static SSdbRaw *mndEncryptAlgrActionEncode(SEncryptAlgrObj *pEncryptAlgr) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSEncryptAlgrObj(NULL, 0, pEncryptAlgr);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_ENCRYPT_ALGORITHMS, MND_ENCRYPT_ALGR_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSEncryptAlgrObj(buf, tlen, pEncryptAlgr);
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
    mError("encrypt_algr:%" PRId32 ", failed to encode to raw:%p since %s", pEncryptAlgr->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("encrypt_algr:%" PRId32 ", encode to raw:%p, row:%p", pEncryptAlgr->id, pRaw, pEncryptAlgr);
  return pRaw;
}

SSdbRow *mndEncryptAlgrActionDecode(SSdbRaw *pRaw) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSdbRow     *pRow = NULL;
  SEncryptAlgrObj *pEncryptAlgr = NULL;
  void        *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_ENCRYPT_ALGR_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("encrypt_algr read invalid ver, data ver: %d, curr ver: %d", sver, MND_ENCRYPT_ALGR_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SEncryptAlgrObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pEncryptAlgr = sdbGetRowObj(pRow);
  if (pEncryptAlgr == NULL) {
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

  if ((terrno = tDeserializeSEncryptAlgrObj(buf, tlen, pEncryptAlgr)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("encrypt_algr:%" PRId32 ", failed to decode from raw:%p since %s", pEncryptAlgr->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("encrypt_algr:%" PRId32 ", decode from raw:%p, row:%p", pEncryptAlgr->id, pRaw, pEncryptAlgr);
  return pRow;
}

int32_t mndEncryptAlgrActionInsert(SSdb *pSdb, SEncryptAlgrObj *pEncryptAlgr) {
  mTrace("encrypt_algr:%" PRId32 ", perform insert action", pEncryptAlgr->id);
  return 0;
}

int32_t mndEncryptAlgrActionDelete(SSdb *pSdb, SEncryptAlgrObj *pEncryptAlgr) {
  mTrace("encrypt_algr:%" PRId32 ", perform delete action", pEncryptAlgr->id);
  tFreeEncryptAlgrObj(pEncryptAlgr);
  return 0;
}

int32_t mndEncryptAlgrActionUpdate(SSdb *pSdb, SEncryptAlgrObj *pOldEncryptAlgr, SEncryptAlgrObj *pNewEncryptAlgr) {
  mTrace("encrypt_algr:%" PRId32 ", perform update action, old row:%p new row:%p", pOldEncryptAlgr->id, pOldEncryptAlgr,
         pNewEncryptAlgr);

  return 0;
}

typedef void (*mndSetEncryptAlgrFp)(SEncryptAlgrObj *Obj);

static void mndSetSM4EncryptAlgr(SEncryptAlgrObj *Obj){
  Obj->id = 1;
  strncpy(Obj->algorithm_id, "SM4-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "SM4", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "SM4 symmetric encryption", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_algr_name, "SM4-CBC:SM4", TSDB_ENCRYPT_ALGR_NAME_LEN);
}

static void mndSetAESEncryptAlgr(SEncryptAlgrObj *Obj){
  Obj->id = 2;
  strncpy(Obj->algorithm_id, "AES-128-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "AES", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "AES symmetric encryption", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_algr_name, "AES-128-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
}

static void mndSetSM3EncryptAlgr(SEncryptAlgrObj *Obj) {
  Obj->id = 3;
  strncpy(Obj->algorithm_id, "SM3", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "SM3", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "SM3 digests", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__DIGEST;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_algr_name, "SM3", TSDB_ENCRYPT_ALGR_NAME_LEN);
}

static void mndSetSHAEncryptAlgr(SEncryptAlgrObj *Obj) {
  Obj->id = 4;
  strncpy(Obj->algorithm_id, "SHA-256", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "SHA-256", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "SHA2 digests", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__DIGEST;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_algr_name, "SHA-256", TSDB_ENCRYPT_ALGR_NAME_LEN);
}

static void mndSetSM2EncryptAlgr(SEncryptAlgrObj *Obj) {
  Obj->id = 5;
  strncpy(Obj->algorithm_id, "SM2", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "SM2", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "SM2 Asymmetric Cipher", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__ASYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
}

static void mndSetRSAEncryptAlgr(SEncryptAlgrObj *Obj) {
  Obj->id = 6;
  strncpy(Obj->algorithm_id, "RSA", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "RSA", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "RSA Asymmetric Cipher", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__ASYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
}

static SSdbRaw * mndCreateEncryptAlgrRaw(STrans *pTrans, SEncryptAlgrObj *Obj) {
  int32_t code = 0;

  SSdbRaw *pRaw = mndEncryptAlgrActionEncode(Obj);
  if (pRaw == NULL) {
    return NULL;
  }
  code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    terrno = code;
    return NULL;
  }

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    terrno = code;
    return NULL;
  }
  return pRaw;
}

int32_t mndSendCreateBuiltinReq(SMnode *pMnode) {
  int32_t code = 0;

  SRpcMsg rpcMsg = {.pCont = NULL,
                    .contLen = 0,
                    .msgType = TDMT_MND_BUILTIN_ENCRYPT_ALGR,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SEpSet  epSet = {0};

  mndGetMnodeEpSet(pMnode, &epSet);

  code = tmsgSendReq(&epSet, &rpcMsg);  // tmsgSendReq
  if (code != 0) {
    mError("failed to send builtin encrypt algr req, since %s", tstrerror(code));
  }
  return code;
}

static int32_t mndCreateBuiltinEncryptAlgr(SMnode *pMnode) {
  int32_t code = 0;
  STrans *pTrans = NULL;

  mndSetEncryptAlgrFp setFpArr[] = {mndSetSM4EncryptAlgr
#if defined(TD_ENTERPRISE) && defined(LINUX)
                                    ,
                                    mndSetAESEncryptAlgr,
                                    mndSetSM3EncryptAlgr,
                                    mndSetSHAEncryptAlgr,
                                    mndSetSM2EncryptAlgr,
                                    mndSetRSAEncryptAlgr
#endif
  };

  int32_t algrNum = sizeof(setFpArr) / sizeof(mndSetEncryptAlgrFp);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-enc-algr");
  if (pTrans == NULL) {
    mError("failed to create since %s",terrstr());
    code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to create default encrypt_algr", pTrans->id);

  for (int32_t i = 0; i < algrNum; i++) {
    SEncryptAlgrObj *Obj = taosMemoryMalloc(sizeof(SEncryptAlgrObj));
    if (Obj == NULL) {
      goto _OVER;
    }
    memset(Obj, 0, sizeof(SEncryptAlgrObj));
    setFpArr[i](Obj);
    SSdbRaw *pRaw = mndCreateEncryptAlgrRaw(pTrans, Obj);
    taosMemoryFree(Obj);
    if (pRaw == NULL) {
      mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
      goto _OVER;
    }
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto _OVER;
  }

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessBuiltinReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  if (!mndIsLeader(pMnode)) {
    return TSDB_CODE_SUCCESS;
  }

  return mndCreateBuiltinEncryptAlgr(pMnode);
}

static int32_t mndProcessBuiltinRsp(SRpcMsg *pRsp) {
  mInfo("builtin rsp");
  return 0;
}

static int32_t mndUpgradeBuiltinEncryptAlgr(SMnode *pMnode, int32_t version) {
  if (version >= TSDB_MNODE_BUILTIN_DATA_VERSION) return 0;

  return mndSendCreateBuiltinReq(pMnode);
}

#define SYMCBC "Symmetric Ciphers CBC mode"
#define ASYM       "Asymmetric Ciphers"
#define DIGEST     "Digests"
#define BUILTIN "build-in"
#define CUSTOMIZED "customized"
static int32_t mndRetrieveEncryptAlgr(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
  SMnode      *pMnode = pMsg->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t      numOfRows = 0;
  int32_t      cols = 0;
  SEncryptAlgrObj *pObj = NULL;

  char tmpBuf[1000] = {0};

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ENCRYPT_ALGORITHMS, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pObj->id, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->algorithm_id, TSDB_ENCRYPT_ALGR_NAME_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->name, TSDB_ENCRYPT_ALGR_NAME_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->desc, TSDB_ENCRYPT_ALGR_DESC_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if(pObj->type == ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS){
      tstrncpy(varDataVal(tmpBuf), SYMCBC, strlen(SYMCBC) + 1);
    } else if (pObj->type == ENCRYPT_ALGR_TYPE__DIGEST) {
      tstrncpy(varDataVal(tmpBuf), DIGEST, strlen(DIGEST) + 1);
    } else if (pObj->type == ENCRYPT_ALGR_TYPE__ASYMMETRIC_CIPHERS) {
      tstrncpy(varDataVal(tmpBuf), ASYM, strlen(ASYM) + 1);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    memset(tmpBuf, 0, 1000);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if(pObj->source == ENCRYPT_ALGR_SOURCE_BUILTIN){
      tstrncpy(varDataVal(tmpBuf), BUILTIN, strlen(BUILTIN) + 1);
    } else if (pObj->source == ENCRYPT_ALGR_SOURCE_CUSTOMIZED) {
      tstrncpy(varDataVal(tmpBuf), CUSTOMIZED, strlen(CUSTOMIZED) + 1);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->ossl_algr_name, TSDB_ENCRYPT_ALGR_NAME_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    sdbRelease(pSdb, pObj);
    numOfRows++;
  }

  pShow->numOfRows += numOfRows;

_OVER:
  if (code != 0) {
    mError("failed to retrieve encrypt_algr info at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelRetrieveEncryptAlgr(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ENCRYPT_ALGORITHMS);
}

static int32_t mndProcessCreateEncryptAlgrReq(SRpcMsg *pReq) {
  SMnode               *pMnode = pReq->info.node;
  int32_t               code = 0;
  int32_t               lino = 0;
  SCreateEncryptAlgrReq createReq = {0};
  STrans               *pTrans = NULL;

  int64_t tss = taosGetTimestampMs();

  if (tDeserializeSCreateEncryptAlgrReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("algr:%s, start to create, ossl_name:%s", createReq.algorithmId, createReq.osslAlgrName);

  // TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_USER), &lino, _OVER);

  SEncryptAlgrObj *exist = mndAcquireEncryptAlgrByAId(pMnode, createReq.algorithmId);
  if (exist != NULL) {
    mndReleaseEncryptAlgr(pMnode, exist);
    TAOS_CHECK_GOTO(TSDB_CODE_MNODE_ALGR_EXIST, &lino, _OVER);
  }

  SEncryptAlgrObj Obj = {0};
  int32_t         id = sdbGetMaxId(pMnode->pSdb, SDB_ENCRYPT_ALGORITHMS);
  if (id < 100) id = 101;
  Obj.id = id;
  strncpy(Obj.algorithm_id, createReq.algorithmId, TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj.name, createReq.name, TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj.desc, createReq.desc, TSDB_ENCRYPT_ALGR_DESC_LEN);
  if (strncmp(createReq.type, "Symmetric_Ciphers_CBC_mode", TSDB_ENCRYPT_ALGR_TYPE_LEN) == 0) {
    Obj.type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  } else if (strncmp(createReq.type, "Digests", TSDB_ENCRYPT_ALGR_TYPE_LEN) == 0) {
    Obj.type = ENCRYPT_ALGR_TYPE__DIGEST;
  } else if (strncmp(createReq.type, "Asymmetric_Ciphers", TSDB_ENCRYPT_ALGR_TYPE_LEN) == 0) {
    Obj.type = ENCRYPT_ALGR_TYPE__ASYMMETRIC_CIPHERS;
  } else {
    TAOS_CHECK_GOTO(TSDB_CODE_MNODE_INVALID_ENCRYPT_ALGR_TYPE, &lino, _OVER);
  }
  Obj.source = ENCRYPT_ALGR_SOURCE_CUSTOMIZED;
  strncpy(Obj.ossl_algr_name, createReq.osslAlgrName, TSDB_ENCRYPT_ALGR_NAME_LEN);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-enc-algr");
  if (pTrans == NULL) {
    mError("failed to create since %s", terrstr());
    code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to create encrypt_algr", pTrans->id);

  SSdbRaw *pRaw = mndCreateEncryptAlgrRaw(pTrans, &Obj);
  if (pRaw == NULL) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    goto _OVER;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto _OVER;
  }

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "createEncryptAlgr", "", createReq.algorithmId, createReq.sql,
                strlen(createReq.sql), duration, 0);
  }

  return code;
_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("algr:%s, failed to create at line %d since %s", createReq.algorithmId, lino, tstrerror(code));
  }

  tFreeSCreateEncryptAlgrReq(&createReq);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndDropEncryptAlgr(SMnode *pMnode, SRpcMsg *pReq, SEncryptAlgrObj *pAlgr) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-encrypt-algr");
  if (pTrans == NULL) {
    mError("algr:%s, failed to drop since %s", pAlgr->algorithm_id, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to drop algr:%s", pTrans->id, pAlgr->algorithm_id);

  SSdbRaw *pCommitRaw = mndEncryptAlgrActionEncode(pAlgr);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) < 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropEncryptAlgrReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = 0;
  int32_t             lino = 0;
  SEncryptAlgrObj    *pObj = NULL;
  SDropEncryptAlgrReq dropReq = {0};

  int64_t tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSDropEncryptAlgrReq(pReq->pCont, pReq->contLen, &dropReq), &lino, _OVER);

  mInfo("algr:%s, start to drop", dropReq.algorithmId);
  // TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_DROP_USER), &lino, _OVER);

  if (dropReq.algorithmId[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_ENCRYPT_ALGR_FORMAT, &lino, _OVER);
  }

  pObj = mndAcquireEncryptAlgrByAId(pMnode, dropReq.algorithmId);
  if (pObj == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MNODE_ENCRYPT_ALGR_NOT_EXIST, &lino, _OVER);
  }

  bool    exist = false;
  void   *pIter = NULL;
  SDbObj *pDb = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;

    if (pDb->cfg.encryptAlgorithm == pObj->id) {
      exist = true;
      sdbRelease(pMnode->pSdb, pDb);
      sdbCancelFetch(pMnode->pSdb, pIter);
      break;
    }

    sdbRelease(pMnode->pSdb, pDb);
  }

  if (exist) {
    TAOS_CHECK_GOTO(TSDB_CODE_MNODE_ENCRYPT_ALGR_IN_USE, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndDropEncryptAlgr(pMnode, pReq, pObj), &lino, _OVER);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "dropEncryptAlgr", "", dropReq.algorithmId, dropReq.sql, dropReq.sqlLen,
                duration, 0);
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("algr:%s, failed to drop at line %d since %s", dropReq.algorithmId, lino, tstrerror(code));
  }

  mndReleaseEncryptAlgr(pMnode, pObj);
  tFreeSDropEncryptAlgrReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveEncryptStatus(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char    tmpBuf[1000] = {0};

  // Only return data if not already retrieved
  if (pShow->numOfRows > 0) {
    return 0;
  }

  // Row 1: config encryption status
  if (numOfRows < rows) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), "config", 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), taosGetEncryptAlgoName(tsCfgAlgorithm), 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), (tsCfgKey[0] != '\0') ? "enabled" : "disabled", 16);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    numOfRows++;
  }

  // Row 2: metadata encryption status
  if (numOfRows < rows) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), "metadata", 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), taosGetEncryptAlgoName(tsMetaAlgorithm), 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), (tsMetaKey[0] != '\0') ? "enabled" : "disabled", 16);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    numOfRows++;
  }

  // Row 3: data encryption status
  if (numOfRows < rows) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), "data", 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    // Data key uses the master algorithm (usually SM4)
    tstrncpy(varDataVal(tmpBuf), taosGetEncryptAlgoName(tsEncryptAlgorithmType), 32);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), (tsDataKey[0] != '\0') ? "enabled" : "disabled", 16);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), &lino, _OVER);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;

_OVER:
  if (code != 0) {
    mError("failed to retrieve encrypt status at line %d since %s", lino, tstrerror(code));
  }
  return numOfRows;
}

int32_t mndInitEncryptAlgr(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ENCRYPT_ALGR, mndProcessCreateEncryptAlgrReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPT_ALGORITHMS, mndRetrieveEncryptAlgr);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPT_STATUS, mndRetrieveEncryptStatus);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPT_ALGORITHMS, mndCancelRetrieveEncryptAlgr);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ENCRYPT_ALGR, mndProcessDropEncryptAlgrReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BUILTIN_ENCRYPT_ALGR, mndProcessBuiltinReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BUILTIN_ENCRYPT_ALGR_RSP, mndProcessBuiltinRsp);

  SSdbTable table = {
      .sdbType = SDB_ENCRYPT_ALGORITHMS,
      .keyType = SDB_KEY_INT32,
      .upgradeFp = (SdbUpgradeFp)mndUpgradeBuiltinEncryptAlgr,
      .encodeFp = (SdbEncodeFp)mndEncryptAlgrActionEncode,
      .decodeFp = (SdbDecodeFp)mndEncryptAlgrActionDecode,
      .insertFp = (SdbInsertFp)mndEncryptAlgrActionInsert,
      .updateFp = (SdbUpdateFp)mndEncryptAlgrActionUpdate,
      .deleteFp = (SdbDeleteFp)mndEncryptAlgrActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupEncryptAlgr(SMnode *pMnode) { mDebug("mnd encrypt algorithms cleanup"); }

SEncryptAlgrObj *mndAcquireEncryptAlgrById(SMnode *pMnode, int64_t id) {
  SSdb        *pSdb = pMnode->pSdb;
  SEncryptAlgrObj *pAlgr = sdbAcquire(pSdb, SDB_ENCRYPT_ALGORITHMS, &id);
  if (pAlgr == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pAlgr;
}

void mndGetEncryptOsslAlgrNameById(SMnode *pMnode, int64_t id, char* out){
  SEncryptAlgrObj *obj = mndAcquireEncryptAlgrById(pMnode, id);
  if(obj != NULL){
    tstrncpy(out, obj->ossl_algr_name, TSDB_ENCRYPT_ALGR_NAME_LEN);
    mndReleaseEncryptAlgr(pMnode, obj);
  }
}

SEncryptAlgrObj *mndAcquireEncryptAlgrByAId(SMnode *pMnode, char* algorithm_id) {
  SSdb        *pSdb = pMnode->pSdb;
  SEncryptAlgrObj *pObj = NULL;
  void *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_ENCRYPT_ALGORITHMS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    if (strncasecmp(pObj->algorithm_id, algorithm_id, TSDB_ENCRYPT_ALGR_NAME_LEN) == 0) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }

    sdbRelease(pSdb, pObj);
  }
  return pObj;
}

void mndReleaseEncryptAlgr(SMnode *pMnode, SEncryptAlgrObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
  pObj = NULL;
}

