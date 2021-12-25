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
#include "mndShow.h"
#include "mndSync.h"
#include "mndTrans.h"

#define SDB_FUNC_VER 1

static SSdbRaw *mndFuncActionEncode(SFuncObj *pFunc);
static SSdbRow *mndFuncActionDecode(SSdbRaw *pRaw);
static int32_t  mndFuncActionInsert(SSdb *pSdb, SFuncObj *pFunc);
static int32_t  mndFuncActionDelete(SSdb *pSdb, SFuncObj *pFunc);
static int32_t  mndFuncActionUpdate(SSdb *pSdb, SFuncObj *pOldFunc, SFuncObj *pNewFunc);
static int32_t  mndCreateFunc(SMnode *pMnode, SMnodeMsg *pMsg, SCreateFuncMsg *pCreate);
static int32_t  mndDropFunc(SMnode *pMnode, SMnodeMsg *pMsg, SFuncObj *pFunc);
static int32_t  mndProcessCreateFuncMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropFuncMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessRetrieveFuncMsg(SMnodeMsg *pMsg);
static int32_t  mndGetFuncMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveFuncs(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextFunc(SMnode *pMnode, void *pIter);

int32_t mndInitFunc(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_FUNC,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndFuncActionEncode,
                     .decodeFp = (SdbDecodeFp)mndFuncActionDecode,
                     .insertFp = (SdbInsertFp)mndFuncActionInsert,
                     .updateFp = (SdbUpdateFp)mndFuncActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndFuncActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_FUNCTION, mndProcessCreateFuncMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_FUNCTION, mndProcessDropFuncMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_FUNCTION, mndProcessRetrieveFuncMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_FUNCTION, mndGetFuncMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_FUNCTION, mndRetrieveFuncs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_FUNCTION, mndCancelGetNextFunc);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupFunc(SMnode *pMnode) {}

static SSdbRaw *mndFuncActionEncode(SFuncObj *pFunc) {
  int32_t  size = pFunc->commentSize + pFunc->codeSize + sizeof(SFuncObj);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_FUNC, SDB_FUNC_VER, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pFunc->name, TSDB_FUNC_NAME_LEN)
  SDB_SET_INT64(pRaw, dataPos, pFunc->createdTime)
  SDB_SET_INT8(pRaw, dataPos, pFunc->funcType)
  SDB_SET_INT8(pRaw, dataPos, pFunc->scriptType)
  SDB_SET_INT8(pRaw, dataPos, pFunc->align)
  SDB_SET_INT8(pRaw, dataPos, pFunc->outputType)
  SDB_SET_INT32(pRaw, dataPos, pFunc->outputLen)
  SDB_SET_INT32(pRaw, dataPos, pFunc->bufSize)
  SDB_SET_INT64(pRaw, dataPos, pFunc->sigature)
  SDB_SET_INT32(pRaw, dataPos, pFunc->commentSize)
  SDB_SET_INT32(pRaw, dataPos, pFunc->codeSize)
  SDB_SET_BINARY(pRaw, dataPos, pFunc->pComment, pFunc->commentSize)
  SDB_SET_BINARY(pRaw, dataPos, pFunc->pCode, pFunc->codeSize)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndFuncActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_FUNC_VER) {
    mError("failed to decode func since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  int32_t   size = sizeof(SFuncObj) + TSDB_FUNC_COMMENT_LEN + TSDB_FUNC_CODE_LEN;
  SSdbRow  *pRow = sdbAllocRow(size);
  SFuncObj *pFunc = sdbGetRowObj(pRow);
  if (pFunc == NULL) return NULL;
  char *tmp = (char *)pFunc + sizeof(SFuncObj);

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pFunc->name, TSDB_FUNC_NAME_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pFunc->createdTime)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pFunc->funcType)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pFunc->scriptType)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pFunc->align)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pFunc->outputType)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pFunc->outputLen)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pFunc->bufSize)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pFunc->sigature)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pFunc->commentSize)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pFunc->codeSize)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pFunc->pData, pFunc->commentSize + pFunc->codeSize)
  pFunc->pComment = pFunc->pData;
  pFunc->pCode = (pFunc->pData + pFunc->commentSize);

  return pRow;
}

static int32_t mndFuncActionInsert(SSdb *pSdb, SFuncObj *pFunc) {
  mTrace("func:%s, perform insert action", pFunc->name);
  return 0;
}

static int32_t mndFuncActionDelete(SSdb *pSdb, SFuncObj *pFunc) {
  mTrace("func:%s, perform delete action", pFunc->name);
  return 0;
}

static int32_t mndFuncActionUpdate(SSdb *pSdb, SFuncObj *pOldFunc, SFuncObj *pNewFunc) {
  mTrace("func:%s, perform update action", pOldFunc->name);
  return 0;
}

static int32_t mndCreateFunc(SMnode *pMnode, SMnodeMsg *pMsg, SCreateFuncMsg *pCreate) {
  SFuncObj *pFunc = calloc(1, sizeof(SFuncObj) + pCreate->commentSize + pCreate->codeSize);
  pFunc->createdTime = taosGetTimestampMs();
  pFunc->funcType = pCreate->funcType;
  pFunc->scriptType = pCreate->scriptType;
  pFunc->outputType = pCreate->outputType;
  pFunc->outputLen = pCreate->outputLen;
  pFunc->bufSize = pCreate->bufSize;
  pFunc->sigature = pCreate->sigature;
  pFunc->commentSize = pCreate->commentSize;
  pFunc->codeSize = pCreate->codeSize;
  pFunc->pComment = pFunc->pData;
  memcpy(pFunc->pComment, pCreate->pCont, pCreate->commentSize);
  pFunc->pCode = pFunc->pData + pCreate->commentSize;
  memcpy(pFunc->pCode, pCreate->pCont + pCreate->commentSize, pFunc->codeSize);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    free(pFunc);
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  mDebug("trans:%d, used to create func:%s", pTrans->id, pCreate->name);

  SSdbRaw *pRedoRaw = mndFuncActionEncode(pFunc);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    free(pFunc);
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING);

  SSdbRaw *pUndoRaw = mndFuncActionEncode(pFunc);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    free(pFunc);
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

  SSdbRaw *pCommitRaw = mndFuncActionEncode(pFunc);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    free(pFunc);
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  free(pFunc);
  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndDropFunc(SMnode *pMnode, SMnodeMsg *pMsg, SFuncObj *pFunc) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("func:%s, failed to drop since %s", pFunc->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop user:%s", pTrans->id, pFunc->name);

  SSdbRaw *pRedoRaw = mndFuncActionEncode(pFunc);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING);

  SSdbRaw *pUndoRaw = mndFuncActionEncode(pFunc);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  SSdbRaw *pCommitRaw = mndFuncActionEncode(pFunc);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateFuncMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;

  SCreateFuncMsg *pCreate = pMsg->rpcMsg.pCont;
  pCreate->outputLen = htonl(pCreate->outputLen);
  pCreate->bufSize = htonl(pCreate->bufSize);
  pCreate->sigature = htobe64(pCreate->sigature);
  pCreate->commentSize = htonl(pCreate->commentSize);
  pCreate->codeSize = htonl(pCreate->codeSize);

  mDebug("func:%s, start to create", pCreate->name);

  SFuncObj *pFunc = sdbAcquire(pMnode->pSdb, SDB_FUNC, pCreate->name);
  if (pFunc != NULL) {
    sdbRelease(pMnode->pSdb, pFunc);
    terrno = TSDB_CODE_MND_FUNC_ALREADY_EXIST;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  if (pCreate->name[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_NAME;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  if (pCreate->commentSize <= 0 || pCreate->commentSize > TSDB_FUNC_COMMENT_LEN) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_COMMENT;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  if (pCreate->codeSize <= 0 || pCreate->codeSize > TSDB_FUNC_CODE_LEN) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_CODE;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  if (pCreate->pCont[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_CODE;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  if (pCreate->bufSize < 0 || pCreate->bufSize > TSDB_FUNC_BUF_SIZE) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_BUFSIZE;
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  int32_t code = mndCreateFunc(pMnode, pMsg, pCreate);

  if (code != 0) {
    mError("func:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessDropFuncMsg(SMnodeMsg *pMsg) {
  SMnode       *pMnode = pMsg->pMnode;
  SDropFuncMsg *pDrop = pMsg->rpcMsg.pCont;

  mDebug("func:%s, start to drop", pDrop->name);

  if (pDrop->name[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_FUNC_NAME;
    mError("func:%s, failed to drop since %s", pDrop->name, terrstr());
    return -1;
  }

  SFuncObj *pFunc = sdbAcquire(pMnode->pSdb, SDB_FUNC, pDrop->name);
  if (pFunc == NULL) {
    terrno = TSDB_CODE_MND_FUNC_NOT_EXIST;
    mError("func:%s, failed to drop since %s", pDrop->name, terrstr());
    return -1;
  }

  int32_t code = mndDropFunc(pMnode, pMsg, pFunc);

  if (code != 0) {
    mError("func:%s, failed to drop since %s", pDrop->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessRetrieveFuncMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;

  SRetrieveFuncMsg *pRetrieve = pMsg->rpcMsg.pCont;
  pRetrieve->numOfFuncs = htonl(pRetrieve->numOfFuncs);

  int32_t size = sizeof(SRetrieveFuncRsp) + (sizeof(SFuncInfo) + TSDB_FUNC_CODE_LEN) * pRetrieve->numOfFuncs + 16384;

  SRetrieveFuncRsp *pRetrieveRsp = rpcMallocCont(size);
  pRetrieveRsp->numOfFuncs = htonl(pRetrieve->numOfFuncs);
  char *pOutput = pRetrieveRsp->pFuncInfos;

  for (int32_t i = 0; i < pRetrieve->numOfFuncs; ++i) {
    char funcName[TSDB_FUNC_NAME_LEN] = {0};
    memcpy(funcName, pRetrieve->pFuncNames + i * TSDB_FUNC_NAME_LEN, TSDB_FUNC_NAME_LEN);

    SFuncObj *pFunc = sdbAcquire(pMnode->pSdb, SDB_FUNC, funcName);
    if (pFunc == NULL) {
      terrno = TSDB_CODE_MND_INVALID_FUNC;
      mError("func:%s, failed to retrieve since %s", funcName, terrstr());
      return -1;
    }

    SFuncInfo *pFuncInfo = (SFuncInfo *)pOutput;

    strncpy(pFuncInfo->name, pFunc->name, TSDB_FUNC_NAME_LEN);
    pFuncInfo->funcType = pFunc->funcType;
    pFuncInfo->scriptType = pFunc->scriptType;
    pFuncInfo->outputType = pFunc->outputType;
    pFuncInfo->outputLen = htonl(pFunc->outputLen);
    pFuncInfo->bufSize = htonl(pFunc->bufSize);
    pFuncInfo->sigature = htobe64(pFunc->sigature);
    pFuncInfo->commentSize = htonl(pFunc->commentSize);
    pFuncInfo->codeSize = htonl(pFunc->codeSize);
    memcpy(pFuncInfo->pCont, pFunc->pCode, pFunc->commentSize + pFunc->codeSize);

    pOutput += sizeof(SFuncInfo) + pFunc->commentSize + pFunc->codeSize;
  }

  pMsg->pCont = pRetrieveRsp;
  pMsg->contLen = (int32_t)(pOutput - (char *)pRetrieveRsp);

  return 0;
}

static int32_t mndGetFuncMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_FUNC_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = PATH_MAX + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "comment");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "aggregate");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_TYPE_STR_MAX_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "outputtype");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "code_len");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "bufsize");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_FUNC);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, "show funcs");

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

static int32_t mndRetrieveFuncs(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode   *pMnode = pMsg->pMnode;
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