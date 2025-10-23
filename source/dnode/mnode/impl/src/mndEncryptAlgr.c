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

#include "mndEncryptAlgr.h"
#include "mndShow.h"
#include "mndTrans.h"

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
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->ossl_provider));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->ossl_algr_name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->ossl_provider_path));

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
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->ossl_provider));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->ossl_algr_name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->ossl_provider_path));

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

static void mndSetSM4EncryptAlgr(SEncryptAlgrObj *Obj){
  Obj->id = 1;
  strncpy(Obj->algorithm_id, "SM4-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "SM4", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "商密分组密码标准", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_provider, "default", TSDB_ENCRYPT_ALGR_PROVIDER_LEN);
  strncpy(Obj->ossl_algr_name, "SM4-CBC:SM4", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->ossl_provider_path, "", TSDB_ENCRYPT_ALGR_PROVIDER_PATH_LEN);
}

static void mndSetAESEncryptAlgr(SEncryptAlgrObj *Obj){
  Obj->id = 2;
  strncpy(Obj->algorithm_id, "AES-128-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "AES", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "AES symmetric encryption", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_provider, "default", TSDB_ENCRYPT_ALGR_PROVIDER_LEN);
  strncpy(Obj->ossl_algr_name, "AES-128-CBC", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->ossl_provider_path, "", TSDB_ENCRYPT_ALGR_PROVIDER_PATH_LEN);
}

static void mndSetTestEncryptAlgr(SEncryptAlgrObj *Obj){
  Obj->id = 3;
  strncpy(Obj->algorithm_id, "vigenere", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->name, "AES", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->desc, "AES symmetric encryption", TSDB_ENCRYPT_ALGR_DESC_LEN);
  Obj->type = ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS;
  Obj->source = ENCRYPT_ALGR_SOURCE_BUILTIN;
  strncpy(Obj->ossl_provider, "default", TSDB_ENCRYPT_ALGR_PROVIDER_LEN);
  strncpy(Obj->ossl_algr_name, "vigenere", TSDB_ENCRYPT_ALGR_NAME_LEN);
  strncpy(Obj->ossl_provider_path, "", TSDB_ENCRYPT_ALGR_PROVIDER_PATH_LEN);
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

#define ALGR_NUM 3
static int32_t  mndCreateBuiltinEncryptAlgr(SMnode *pMnode){
  int32_t code = 0;
  SArray* objArray = NULL;
  STrans* pTrans = NULL;
  SArray* rawArray = NULL;

  objArray = taosArrayInit(ALGR_NUM, sizeof(SEncryptAlgrObj));
  if (objArray == NULL) {
    return terrno;
  }
  for(int32_t i = 0; i < ALGR_NUM; i++){
    SEncryptAlgrObj *Obj = taosMemoryMalloc(sizeof(SEncryptAlgrObj));
    if(Obj == NULL){
      goto _OVER;
    }
    memset(Obj, 0, sizeof(SEncryptAlgrObj));
    taosArrayPush(objArray, Obj);
  }
  
  SEncryptAlgrObj *Obj1 = taosArrayGet(objArray, 0);
  mndSetSM4EncryptAlgr(Obj1);
  SEncryptAlgrObj *Obj2 = taosArrayGet(objArray, 1);
  mndSetAESEncryptAlgr(Obj2);
  SEncryptAlgrObj *Obj3 = taosArrayGet(objArray, 2);
  mndSetTestEncryptAlgr(Obj3);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-enc-algr");
  if (pTrans == NULL) {
    mError("failed to create since %s",terrstr());
    code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to create default encrypt_algr", pTrans->id);

  rawArray = taosArrayInit(ALGR_NUM, sizeof(SSdbRaw));
  if (rawArray == NULL) {
    goto _OVER;
  }

  for(int32_t i = 0; i < ALGR_NUM; i++){
    SEncryptAlgrObj *Obj = taosArrayGet(objArray, i);
    SSdbRaw *pRaw = mndCreateEncryptAlgrRaw(pTrans, Obj);
    if(pRaw == NULL){
      mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
      goto _OVER;
    }
    taosArrayPush(rawArray, pRaw);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto _OVER;
  }

  return code;

_OVER:  
  for(int32_t i = 0; i < ALGR_NUM; i++){
    SEncryptAlgrObj *Obj = taosArrayGet(objArray, i);
    if (Obj != NULL) taosMemoryFree(Obj);
    SSdbRaw *raw = taosArrayGet(rawArray, i);
    if (raw != NULL) sdbFreeRaw(raw);
  }
  taosArrayClear(rawArray);
  taosArrayClear(objArray);
  mndTransDrop(pTrans);
  return code;
}

#define SYMCBC "Symmetric Ciphers CBC mode"
#define BUILTIN "build-in"
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
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    memset(tmpBuf, 0, 1000);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if(pObj->source == ENCRYPT_ALGR_SOURCE_BUILTIN){
      tstrncpy(varDataVal(tmpBuf), BUILTIN, strlen(BUILTIN) + 1);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->ossl_provider, TSDB_ENCRYPT_ALGR_PROVIDER_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->ossl_algr_name, TSDB_ENCRYPT_ALGR_NAME_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pObj, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    tstrncpy(varDataVal(tmpBuf), pObj->ossl_provider_path, TSDB_ENCRYPT_ALGR_PROVIDER_PATH_LEN);
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

int32_t mndInitEncryptAlgr(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPT_ALGORITHMS, mndRetrieveEncryptAlgr);

  SSdbTable table = {
      .sdbType = SDB_ENCRYPT_ALGORITHMS,
      .keyType = SDB_KEY_INT32,
      .deployFp = (SdbDeployFp)mndCreateBuiltinEncryptAlgr,
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

    if(strncmp(pObj->algorithm_id, algorithm_id, TSDB_ENCRYPT_ALGR_NAME_LEN) == 0) {
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

