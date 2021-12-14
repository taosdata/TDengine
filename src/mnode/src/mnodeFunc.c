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
#include "os.h"
#include "trpc.h"
#include "tutil.h"
#include "tglobal.h"
#include "tgrant.h"
#include "tdataformat.h"
#include "tkey.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeUser.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeFunc.h"
#include "mnodeWrite.h"
#include "mnodeRead.h"
#include "mnodePeer.h"

int64_t        tsFuncRid = -1;
static void *  tsFuncSdb = NULL;
static int32_t tsFuncUpdateSize = 0;
static int32_t mnodeGetFuncMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveFuncs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeProcessRetrieveFuncImplMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessCreateFuncMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropFuncMsg(SMnodeMsg *pMsg);

static int32_t mnodeFuncActionDestroy(SSdbRow *pRow) {
  tfree(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionInsert(SSdbRow *pRow) {
  SFuncObj *pFunc = pRow->pObj;

  mTrace("func:%s, contLen: %d, insert into sdb", pFunc->name, pFunc->contLen);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionDelete(SSdbRow *pRow) {
  SFuncObj *pFunc = pRow->pObj;

  mTrace("func:%s, length: %d, delete from sdb", pFunc->name, pFunc->contLen);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionUpdate(SSdbRow *pRow) {
  SFuncObj *pFunc = pRow->pObj;

  SFuncObj *pSaved = mnodeGetFunc(pFunc->name);
  if (pFunc != pSaved) {
    memcpy(pSaved, pFunc, tsFuncUpdateSize);
    free(pFunc);
  }

  mnodeDecFuncRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionEncode(SSdbRow *pRow) {
  SFuncObj *pFunc = pRow->pObj;

  memcpy(pRow->rowData, pFunc, tsFuncUpdateSize);
  pRow->rowSize = tsFuncUpdateSize;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionDecode(SSdbRow *pRow) {
  SFuncObj *pFunc = (SFuncObj *)calloc(1, sizeof(SFuncObj));
  if (pFunc == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pFunc, pRow->rowData, tsFuncUpdateSize);
  pRow->pObj = pFunc;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFuncActionRestored() {
  int64_t numOfRows = sdbGetNumOfRows(tsFuncSdb);

  if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
    mInfo("dnode first deploy, func restored.");
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitFuncs() {
  SFuncObj tObj;
  tsFuncUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_FUNC,
    .name         = "funcs",
    .hashSessions = TSDB_DEFAULT_USERS_HASH_SIZE,
    .maxRowSize   = tsFuncUpdateSize,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_STRING,
    .fpInsert     = mnodeFuncActionInsert,
    .fpDelete     = mnodeFuncActionDelete,
    .fpUpdate     = mnodeFuncActionUpdate,
    .fpEncode     = mnodeFuncActionEncode,
    .fpDecode     = mnodeFuncActionDecode,
    .fpDestroy    = mnodeFuncActionDestroy,
    .fpRestored   = mnodeFuncActionRestored
  };

  tsFuncRid = sdbOpenTable(&desc);
  tsFuncSdb = sdbGetTableByRid(tsFuncRid);
  if (tsFuncSdb == NULL) {
    mError("table:%s, failed to create hash", desc.name);
    return -1;
  }

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_FUNCTION, mnodeProcessCreateFuncMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_FUNCTION, mnodeProcessDropFuncMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_RETRIEVE_FUNC, mnodeProcessRetrieveFuncImplMsg);

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_FUNCTION, mnodeGetFuncMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_FUNCTION, mnodeRetrieveFuncs);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_FUNCTION, mnodeCancelGetNextFunc);

  mDebug("table:%s, hash is created", desc.name);

  return 0;
}

void mnodeCleanupFuncs() {
  sdbCloseTable(tsFuncRid);
  tsFuncSdb = NULL;
}

SFuncObj *mnodeGetFunc(char *name) {
  return (SFuncObj *)sdbGetRow(tsFuncSdb, name);
}

void *mnodeGetNextFunc(void *pIter, SFuncObj **pFunc) {
  return sdbFetchRow(tsFuncSdb, pIter, (void **)pFunc);
}

void mnodeCancelGetNextFunc(void *pIter) {
 sdbFreeIter(tsFuncSdb, pIter);
}

void mnodeIncFuncRef(SFuncObj *pFunc) {
  sdbIncRef(tsFuncSdb, pFunc);
}

void mnodeDecFuncRef(SFuncObj *pFunc) {
  sdbDecRef(tsFuncSdb, pFunc);
}
/*
static int32_t mnodeUpdateFunc(SFuncObj *pFunc, void *pMsg) {
  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsFuncSdb,
    .pObj   = pFunc,
    .pMsg   = pMsg
  };

  int32_t code = sdbUpdateRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("func:%s, failed to alter by %s, reason:%s", pFunc->name, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("func:%s, is altered by %s", pFunc->name, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}
*/
int32_t mnodeCreateFunc(SAcctObj *pAcct, char *name, int32_t codeLen, char *codeScript, char *path, uint8_t outputType, int16_t outputLen, int32_t funcType, int32_t bufSize, SMnodeMsg *pMsg) {
#ifdef GRANT_CHECK_WRITE
  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_GRANT_EXPIRED;
  }
#endif

  if (!pMsg->pUser->writeAuth) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t code = acctCheck(pAcct, ACCT_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = grantCheck(TSDB_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (name[0] == 0) {
    return TSDB_CODE_MND_INVALID_FUNC_NAME;
  }

  if (codeScript[0] == 0) {
    return TSDB_CODE_MND_INVALID_FUNC_CODE;
  }

  if (codeLen < 0 || codeLen > TSDB_FUNC_CODE_LEN) {
    return TSDB_CODE_MND_INVALID_FUNC_CODE;
  }

  if (bufSize < 0 || bufSize > TSDB_FUNC_BUF_SIZE) {
    return TSDB_CODE_MND_INVALID_FUNC_BUFSIZE;
  }

  SFuncObj *pFunc = mnodeGetFunc(name);
  if (pFunc != NULL) {
    mDebug("func:%s, is already there", name);
    mnodeDecFuncRef(pFunc);
    return TSDB_CODE_MND_FUNC_ALREADY_EXIST;
  }

  pFunc = calloc(1, sizeof(SFuncObj));
  tstrncpy(pFunc->name, name, TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->path, path, tListLen(pFunc->path));
  memcpy(pFunc->cont, codeScript, codeLen);
  pFunc->contLen     = codeLen;
  pFunc->createdTime = taosGetTimestampMs();
  pFunc->resType     = outputType;
  pFunc->resBytes    = outputLen;
  pFunc->funcType    = funcType;
  pFunc->bufSize     = bufSize;
  pFunc->sig  = 0;
  pFunc->type = 1; //lua script, refactor

  SSdbRow row = {
    .type     = SDB_OPER_GLOBAL,
    .pTable   = tsFuncSdb,
    .pObj     = pFunc,
    .rowSize  = sizeof(SFuncObj),
    .pMsg     = pMsg
  };

  code = sdbInsertRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("func:%s, failed to create by %s, reason:%s", pFunc->name, mnodeGetUserFromMsg(pMsg), tstrerror(code));
    tfree(pFunc);
  } else {
    mLInfo("func:%s, is created by %s", pFunc->name, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeDropFunc(SFuncObj *pFunc, void *pMsg) {
  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsFuncSdb,
    .pObj   = pFunc,
    .pMsg   = pMsg
  };

  int32_t code = sdbDeleteRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("func:%s, failed to drop by %s, reason:%s", pFunc->name, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("func:%s, is dropped by %s", pFunc->name, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeGetFuncsNum() {
  return (int32_t)sdbGetNumOfRows(tsFuncSdb);
}

static int32_t mnodeGetFuncMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) {
    return TSDB_CODE_MND_NO_USER_FROM_CONN;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_FUNC_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = PATH_MAX + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "path");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "aggregate");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_TYPE_STR_MAX_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "outputtype");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "code_len");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "bufsize");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  strcpy(pMeta->tableFname, "show funcs");
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetFuncsNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mnodeDecUserRef(pUser);

  return 0;
}

static void* mnodeGenTypeStr(char *buf, int32_t buflen, uint8_t type, int16_t len) {
  char *msg = "unknown";
  if (type >= sizeof(tDataTypes)/sizeof(tDataTypes[0])) {
    return msg;
  }
  
  if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
    int32_t bytes = len > 0 ? (int)(len - VARSTR_HEADER_SIZE) : len;
    
    snprintf(buf, buflen - 1, "%s(%d)", tDataTypes[type].name, type == TSDB_DATA_TYPE_NCHAR ? bytes/4 : bytes);
    buf[buflen - 1] = 0;

    return buf;
  }
  
  return tDataTypes[type].name;
}

static int32_t mnodeRetrieveFuncs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SFuncObj *pFunc    = NULL;
  int32_t   cols      = 0;
  char     *pWrite;
  char      buf[TSDB_TYPE_STR_MAX_LEN];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextFunc(pShow->pIter, &pFunc);
    if (pFunc == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pFunc->name, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pFunc->path, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->funcType == TSDB_UDF_TYPE_AGGREGATE ? 1 : 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, mnodeGenTypeStr(buf, TSDB_TYPE_STR_MAX_LEN, pFunc->resType, pFunc->resBytes), pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pFunc->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->contLen;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pFunc->bufSize;
    cols++;

    numOfRows++;
    mnodeDecFuncRef(pFunc);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static int32_t mnodeProcessCreateFuncMsg(SMnodeMsg *pMsg) {
  SCreateFuncMsg *pCreate    = pMsg->rpcMsg.pCont;
  pCreate->codeLen       = htonl(pCreate->codeLen);
  pCreate->outputLen     = htons(pCreate->outputLen);
  pCreate->funcType      = htonl(pCreate->funcType);
  pCreate->bufSize       = htonl(pCreate->bufSize);

  return mnodeCreateFunc(pMsg->pUser->pAcct, pCreate->name, pCreate->codeLen, pCreate->code, pCreate->path, pCreate->outputType, pCreate->outputLen, pCreate->funcType, pCreate->bufSize, pMsg);
}

static int32_t mnodeProcessDropFuncMsg(SMnodeMsg *pMsg) {
  SDropFuncMsg *pDrop = pMsg->rpcMsg.pCont;

  SFuncObj *pFunc = mnodeGetFunc(pDrop->name);
  if (pFunc == NULL) {
    return TSDB_CODE_MND_INVALID_FUNC;
  }

  return mnodeDropFunc(pFunc, pMsg);
}

static int32_t mnodeProcessRetrieveFuncImplMsg(SMnodeMsg *pMsg) {
  SRetrieveFuncMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->num = htonl(pInfo->num);

  int32_t t = sizeof(SUdfFuncMsg) + (sizeof(SFunctionInfoMsg) + TSDB_FUNC_CODE_LEN) * pInfo->num + 16384;

  SUdfFuncMsg *pFuncMsg = rpcMallocCont(t);
  pFuncMsg->num = htonl(pInfo->num);
  char* pOutput = pFuncMsg->content;
  tstr* name = (tstr*) pInfo->name;

  for(int32_t i = 0; i < pInfo->num; ++i) {
    char buf[TSDB_FUNC_NAME_LEN] = {0};
    tstrncpy(buf, name->data, htons(name->len) + 1);

    SFuncObj* pFuncObj = mnodeGetFunc(buf);
    if (pFuncObj == NULL) {
      mError("function %s does not exist", buf);
      return TSDB_CODE_MND_INVALID_FUNC;
    }
    
    SFunctionInfoMsg* pFuncInfo = (SFunctionInfoMsg*) pOutput;

    strcpy(pFuncInfo->name, buf);
    pFuncInfo->len = htonl(pFuncObj->contLen);
    memcpy(pFuncInfo->content, pFuncObj->cont, pFuncObj->contLen);

    pFuncInfo->funcType = htonl(pFuncObj->funcType);
    pFuncInfo->resType = pFuncObj->resType;
    pFuncInfo->resBytes = htons(pFuncObj->resBytes);
    pFuncInfo->bufSize  = htonl(pFuncObj->bufSize);
    
    pOutput += sizeof(SFunctionInfoMsg) + pFuncObj->contLen;
    name =(tstr *)((char *)name + sizeof(*name) + htons(name->len));
  }

  pMsg->rpcRsp.rsp = pFuncMsg;
  pMsg->rpcRsp.len = (int32_t)(pOutput - (char*)pFuncMsg);
  return TSDB_CODE_SUCCESS;
}
