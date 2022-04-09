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
#include "mndFunc.h"
#include "mndAuth.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "mndUser.h"

#define SDB_FUNC_VER          1
#define SDB_FUNC_RESERVE_SIZE 64

static SSdbRaw *mndFuncActionEncode(SFuncObj *pFunc);
static SSdbRow *mndFuncActionDecode(SSdbRaw *pRaw);
static int32_t  mndFuncActionInsert(SSdb *pSdb, SFuncObj *pFunc);
static int32_t  mndFuncActionDelete(SSdb *pSdb, SFuncObj *pFunc);
static int32_t  mndFuncActionUpdate(SSdb *pSdb, SFuncObj *pOld, SFuncObj *pNew);
static int32_t  mndCreateFunc(SMnode *pMnode, SNodeMsg *pReq, SCreateFuncReq *pCreate);
static int32_t  mndDropFunc(SMnode *pMnode, SNodeMsg *pReq, SFuncObj *pFunc);
static int32_t  mndProcessCreateFuncReq(SNodeMsg *pReq);
static int32_t  mndProcessDropFuncReq(SNodeMsg *pReq);
static int32_t  mndProcessRetrieveFuncReq(SNodeMsg *pReq);
static int32_t  mndGetFuncMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveFuncs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextFunc(SMnode *pMnode, void *pIter);

int32_t mndInitFunc(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_FUNC,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndFuncActionEncode,
                     .decodeFp = (SdbDecodeFp)mndFuncActionDecode,
                     .insertFp = (SdbInsertFp)mndFuncActionInsert,
                     .updateFp = (SdbUpdateFp)mndFuncActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndFuncActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_FUNC, mndProcessCreateFuncReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_FUNC, mndProcessDropFuncReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_FUNC, mndProcessRetrieveFuncReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_FUNC, mndRetrieveFuncs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_FUNC, mndCancelGetNextFunc);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupFunc(SMnode *pMnode) {}

static SSdbRaw *mndFuncActionEncode(SFuncObj *pFunc) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = pFunc->commentSize + pFunc->codeSize + sizeof(SFuncObj) + SDB_FUNC_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_FUNC, SDB_FUNC_VER, size);
  if (pRaw == NULL) goto FUNC_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pFunc->name, TSDB_FUNC_NAME_LEN, FUNC_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pFunc->createdTime, FUNC_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pFunc->funcType, FUNC_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pFunc->scriptType, FUNC_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pFunc->align, FUNC_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pFunc->outputType, FUNC_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pFunc->outputLen, FUNC_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pFunc->bufSize, FUNC_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pFunc->signature, FUNC_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pFunc->commentSize, FUNC_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pFunc->codeSize, FUNC_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pFunc->pComment, pFunc->commentSize, FUNC_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pFunc->pCode, pFunc->codeSize, FUNC_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, SDB_FUNC_RESERVE_SIZE, FUNC_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, FUNC_ENCODE_OVER);

  terrno = 0;

FUNC_ENCODE_OVER:
  if (terrno != 0) {
    mError("func:%s, failed to encode to raw:%p since %s", pFunc->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("func:%s, encode to raw:%p, row:%p", pFunc->name, pRaw, pFunc);
  return pRaw;
}

static SSdbRow *mndFuncActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto FUNC_DECODE_OVER;

  if (sver != SDB_FUNC_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto FUNC_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SFuncObj));
  if (pRow == NULL) goto FUNC_DECODE_OVER;

  SFuncObj *pFunc = sdbGetRowObj(pRow);
  if (pFunc == NULL) goto FUNC_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pFunc->name, TSDB_FUNC_NAME_LEN, FUNC_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pFunc->createdTime, FUNC_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pFunc->funcType, FUNC_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pFunc->scriptType, FUNC_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pFunc->align, FUNC_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pFunc->outputType, FUNC_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pFunc->outputLen, FUNC_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pFunc->bufSize, FUNC_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pFunc->signature, FUNC_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pFunc->commentSize, FUNC_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pFunc->codeSize, FUNC_DECODE_OVER)

  pFunc->pComment = taosMemoryCalloc(1, pFunc->commentSize);
  pFunc->pCode = taosMemoryCalloc(1, pFunc->codeSize);
  if (pFunc->pComment == NULL || pFunc->pCode == NULL) {
    goto FUNC_DECODE_OVER;
  }

  SDB_GET_BINARY(pRaw, dataPos, pFunc->pComment, pFunc->commentSize, FUNC_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pFunc->pCode, pFunc->codeSize, FUNC_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, SDB_FUNC_RESERVE_SIZE, FUNC_DECODE_OVER)

  terrno = 0;

FUNC_DECODE_OVER:
  if (terrno != 0) {
    mError("func:%s, failed to decode from raw:%p since %s", pFunc->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("func:%s, decode from raw:%p, row:%p", pFunc->name, pRaw, pFunc);
  return pRow;
}

static int32_t mndFuncActionInsert(SSdb *pSdb, SFuncObj *pFunc) {
  mTrace("func:%s, perform insert action, row:%p", pFunc->name, pFunc);
  return 0;
}

static int32_t mndFuncActionDelete(SSdb *pSdb, SFuncObj *pFunc) {
  mTrace("func:%s, perform delete action, row:%p", pFunc->name, pFunc);
  taosMemoryFreeClear(pFunc->pCode);
  taosMemoryFreeClear(pFunc->pComment);
  return 0;
}

static int32_t mndFuncActionUpdate(SSdb *pSdb, SFuncObj *pOld, SFuncObj *pNew) {
  mTrace("func:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  return 0;
}

static SFuncObj *mndAcquireFunc(SMnode *pMnode, char *funcName) {
  SSdb     *pSdb = pMnode->pSdb;
  SFuncObj *pFunc = sdbAcquire(pSdb, SDB_FUNC, funcName);
  if (pFunc == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_FUNC_NOT_EXIST;
  }
  return pFunc;
}

static void mndReleaseFunc(SMnode *pMnode, SFuncObj *pFunc) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pFunc);
}

static int32_t mndCreateFunc(SMnode *pMnode, SNodeMsg *pReq, SCreateFuncReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SFuncObj func = {0};
  memcpy(func.name, pCreate->name, TSDB_FUNC_NAME_LEN);
  func.createdTime = taosGetTimestampMs();
  func.funcType = pCreate->funcType;
  func.scriptType = pCreate->scriptType;
  func.outputType = pCreate->outputType;
  func.outputLen = pCreate->outputLen;
  func.bufSize = pCreate->bufSize;
  func.signature = pCreate->signature;
  func.commentSize = pCreate->commentSize;
  func.codeSize = pCreate->codeSize;
  func.pComment = taosMemoryMalloc(func.commentSize);
  func.pCode = taosMemoryMalloc(func.codeSize);
  if (func.pCode == NULL || func.pCode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto CREATE_FUNC_OVER;
  }

  memcpy(func.pComment, pCreate->pComment, pCreate->commentSize);
  memcpy(func.pCode, pCreate->pCode, func.codeSize);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_FUNC, &pReq->rpcMsg);
  if (pTrans == NULL) goto CREATE_FUNC_OVER;

  mDebug("trans:%d, used to create func:%s", pTrans->id, pCreate->name);

  SSdbRaw *pRedoRaw = mndFuncActionEncode(&func);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) goto CREATE_FUNC_OVER;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) goto CREATE_FUNC_OVER;

  SSdbRaw *pUndoRaw = mndFuncActionEncode(&func);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) goto CREATE_FUNC_OVER;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) goto CREATE_FUNC_OVER;

  SSdbRaw *pCommitRaw = mndFuncActionEncode(&func);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) goto CREATE_FUNC_OVER;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) goto CREATE_FUNC_OVER;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto CREATE_FUNC_OVER;

  code = 0;

CREATE_FUNC_OVER:
  taosMemoryFree(func.pCode);
  taosMemoryFree(func.pComment);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndDropFunc(SMnode *pMnode, SNodeMsg *pReq, SFuncObj *pFunc) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_DROP_FUNC, &pReq->rpcMsg);
  if (pTrans == NULL) goto DROP_FUNC_OVER;

  mDebug("trans:%d, used to drop user:%s", pTrans->id, pFunc->name);

  SSdbRaw *pRedoRaw = mndFuncActionEncode(pFunc);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) goto DROP_FUNC_OVER;
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING);

  SSdbRaw *pUndoRaw = mndFuncActionEncode(pFunc);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) goto DROP_FUNC_OVER;
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  SSdbRaw *pCommitRaw = mndFuncActionEncode(pFunc);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) goto DROP_FUNC_OVER;
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) goto DROP_FUNC_OVER;

  code = 0;

DROP_FUNC_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateFuncReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SFuncObj      *pFunc = NULL;
  SCreateFuncReq createReq = {0};

  if (tDeserializeSCreateFuncReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_FUNC_OVER;
  }

  mDebug("func:%s, start to create", createReq.name);

  pFunc = mndAcquireFunc(pMnode, createReq.name);
  if (pFunc != NULL) {
    if (createReq.igExists) {
      mDebug("func:%s, already exist, ignore exist is set", createReq.name);
      code = 0;
      goto CREATE_FUNC_OVER;
    } else {
      terrno = TSDB_CODE_MND_FUNC_ALREADY_EXIST;
      goto CREATE_FUNC_OVER;
    }
  } else if (terrno == TSDB_CODE_MND_FUNC_ALREADY_EXIST) {
    goto CREATE_FUNC_OVER;
  }

  if (createReq.name[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_NAME;
    goto CREATE_FUNC_OVER;
  }

  if (createReq.commentSize <= 0 || createReq.commentSize > TSDB_FUNC_COMMENT_LEN) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_COMMENT;
    goto CREATE_FUNC_OVER;
  }

  if (createReq.codeSize <= 0 || createReq.codeSize > TSDB_FUNC_CODE_LEN) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_CODE;
    goto CREATE_FUNC_OVER;
  }

  if (createReq.pCode[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_CODE;
    goto CREATE_FUNC_OVER;
  }

  if (createReq.bufSize <= 0 || createReq.bufSize > TSDB_FUNC_BUF_SIZE) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_BUFSIZE;
    goto CREATE_FUNC_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto CREATE_FUNC_OVER;
  }

  if (mndCheckFuncAuth(pUser)) {
    goto CREATE_FUNC_OVER;
  }

  code = mndCreateFunc(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_FUNC_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("func:%s, failed to create since %s", createReq.name, terrstr());
  }

  mndReleaseFunc(pMnode, pFunc);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessDropFuncReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SFuncObj    *pFunc = NULL;
  SDropFuncReq dropReq = {0};

  if (tDeserializeSDropFuncReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_FUNC_OVER;
  }

  mDebug("func:%s, start to drop", dropReq.name);

  if (dropReq.name[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_NAME;
    goto DROP_FUNC_OVER;
  }

  pFunc = mndAcquireFunc(pMnode, dropReq.name);
  if (pFunc == NULL) {
    if (dropReq.igNotExists) {
      mDebug("func:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto DROP_FUNC_OVER;
    } else {
      terrno = TSDB_CODE_MND_FUNC_NOT_EXIST;
      goto DROP_FUNC_OVER;
    }
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto DROP_FUNC_OVER;
  }

  if (mndCheckFuncAuth(pUser)) {
    goto DROP_FUNC_OVER;
  }

  code = mndDropFunc(pMnode, pReq, pFunc);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_FUNC_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("func:%s, failed to drop since %s", dropReq.name, terrstr());
  }

  mndReleaseFunc(pMnode, pFunc);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessRetrieveFuncReq(SNodeMsg *pReq) {
  SMnode          *pMnode = pReq->pNode;
  int32_t          code = -1;
  SRetrieveFuncReq retrieveReq = {0};
  SRetrieveFuncRsp retrieveRsp = {0};

  if (tDeserializeSRetrieveFuncReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &retrieveReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto RETRIEVE_FUNC_OVER;
  }

  if (retrieveReq.numOfFuncs <= 0 || retrieveReq.numOfFuncs > TSDB_FUNC_MAX_RETRIEVE) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_RETRIEVE;
    goto RETRIEVE_FUNC_OVER;
  }

  retrieveRsp.numOfFuncs = retrieveReq.numOfFuncs;
  retrieveRsp.pFuncInfos = taosArrayInit(retrieveReq.numOfFuncs, sizeof(SFuncInfo));
  if (retrieveRsp.pFuncInfos == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto RETRIEVE_FUNC_OVER;
  }

  for (int32_t i = 0; i < retrieveReq.numOfFuncs; ++i) {
    char *funcName = taosArrayGet(retrieveReq.pFuncNames, i);

    SFuncObj *pFunc = mndAcquireFunc(pMnode, funcName);
    if (pFunc == NULL) {
      terrno = TSDB_CODE_MND_INVALID_FUNC;
      goto RETRIEVE_FUNC_OVER;
    }

    SFuncInfo funcInfo = {0};
    memcpy(funcInfo.name, pFunc->name, TSDB_FUNC_NAME_LEN);
    funcInfo.funcType = pFunc->funcType;
    funcInfo.scriptType = pFunc->scriptType;
    funcInfo.outputType = pFunc->outputType;
    funcInfo.outputLen = pFunc->outputLen;
    funcInfo.bufSize = pFunc->bufSize;
    funcInfo.signature = pFunc->signature;
    funcInfo.commentSize = pFunc->commentSize;
    funcInfo.codeSize = pFunc->codeSize;
    memcpy(funcInfo.pComment, pFunc->pComment, pFunc->commentSize);
    memcpy(funcInfo.pCode, pFunc->pCode, pFunc->codeSize);
    taosArrayPush(retrieveRsp.pFuncInfos, &funcInfo);
    mndReleaseFunc(pMnode, pFunc);
  }

  int32_t contLen = tSerializeSRetrieveFuncRsp(NULL, 0, &retrieveRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto RETRIEVE_FUNC_OVER;
  }

  tSerializeSRetrieveFuncRsp(pRsp, contLen, &retrieveRsp);

  pReq->pRsp = pRsp;
  pReq->rspLen = contLen;

  code = 0;

RETRIEVE_FUNC_OVER:
  taosArrayDestroy(retrieveReq.pFuncNames);
  taosArrayDestroy(retrieveRsp.pFuncInfos);

  return code;
}

static int32_t mndGetFuncMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_FUNC_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = PATH_MAX + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "comment");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "aggregate");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_TYPE_STR_MAX_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "outputtype");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "code_len");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "bufsize");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_FUNC);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static void *mnodeGenTypeStr(char *buf, int32_t buflen, uint8_t type, int16_t len) {
  char *msg = "unknown";
  if (type >= sizeof(tDataTypes) / sizeof(tDataTypes[0])) {
    return msg;
  }

  if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
    int32_t bytes = len > 0 ? (int32_t)(len - VARSTR_HEADER_SIZE) : len;

    snprintf(buf, buflen - 1, "%s(%d)", tDataTypes[type].name, type == TSDB_DATA_TYPE_NCHAR ? bytes / 4 : bytes);
    buf[buflen - 1] = 0;

    return buf;
  }

  return tDataTypes[type].name;
}

static int32_t mndRetrieveFuncs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode   *pMnode = pReq->pNode;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SFuncObj *pFunc = NULL;
  int32_t   cols = 0;
  char     *pWrite;
  char      buf[TSDB_TYPE_STR_MAX_LEN];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_FUNC, pShow->pIter, (void **)&pFunc);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pFunc->name, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pFunc->pComment, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->funcType == TSDB_FUNC_TYPE_AGGREGATE ? 1 : 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, mnodeGenTypeStr(buf, TSDB_TYPE_STR_MAX_LEN, pFunc->outputType, pFunc->outputLen),
                               pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pFunc->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->codeSize;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->bufSize;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pFunc);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextFunc(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}