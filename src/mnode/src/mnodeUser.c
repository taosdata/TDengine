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
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeWrite.h"
#include "mnodePeer.h"

static void *  tsUserSdb = NULL;
static int32_t tsUserUpdateSize = 0;
static int32_t mnodeGetUserMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeProcessCreateUserMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessAlterUserMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropUserMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessAuthMsg(SMnodeMsg *pMsg);

static int32_t mnodeUserActionDestroy(SSdbOper *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeUserActionInsert(SSdbOper *pOper) {
  SUserObj *pUser = pOper->pObj;
  SAcctObj *pAcct = mnodeGetAcct(pUser->acct);

  if (pAcct != NULL) {
    mnodeAddUserToAcct(pAcct, pUser);
    mnodeDecAcctRef(pAcct);
  } else {
    mError("user:%s, acct:%s info not exist in sdb", pUser->user, pUser->acct);
    return TSDB_CODE_MND_INVALID_ACCT;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeUserActionDelete(SSdbOper *pOper) {
  SUserObj *pUser = pOper->pObj;
  SAcctObj *pAcct = mnodeGetAcct(pUser->acct);

  if (pAcct != NULL) {
    mnodeDropUserFromAcct(pAcct, pUser);
    mnodeDecAcctRef(pAcct);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeUserActionUpdate(SSdbOper *pOper) {
  SUserObj *pUser = pOper->pObj;
  SUserObj *pSaved = mnodeGetUser(pUser->user);
  if (pUser != pSaved) {
    memcpy(pSaved, pUser, tsUserUpdateSize);
    free(pUser);
  }
  mnodeDecUserRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeUserActionEncode(SSdbOper *pOper) {
  SUserObj *pUser = pOper->pObj;
  memcpy(pOper->rowData, pUser, tsUserUpdateSize);
  pOper->rowSize = tsUserUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeUserActionDecode(SSdbOper *pOper) {
  SUserObj *pUser = (SUserObj *)calloc(1, sizeof(SUserObj));
  if (pUser == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pUser, pOper->rowData, tsUserUpdateSize);
  pOper->pObj = pUser;
  return TSDB_CODE_SUCCESS;
}

static void mnodePrintUserAuth() {
  FILE *fp = fopen("auth.txt", "w");
  if (!fp) {
    mDebug("failed to auth.txt for write");
    return;
  }
  
  void *    pIter = NULL;
  SUserObj *pUser = NULL;

  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;

    char *base64 = base64_encode((const unsigned char *)pUser->pass, TSDB_KEY_LEN * 2);
    fprintf(fp, "user:%24s auth:%s\n", pUser->user, base64);
    free(base64);

    mnodeDecUserRef(pUser);
  }

  fflush(fp);
  sdbFreeIter(pIter);
  fclose(fp);
}

static int32_t mnodeUserActionRestored() {
  int32_t numOfRows = sdbGetNumOfRows(tsUserSdb);
  if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
    mInfo("dnode first deploy, create root user");
    SAcctObj *pAcct = mnodeGetAcct(TSDB_DEFAULT_USER);
    mnodeCreateUser(pAcct, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS, NULL);
    mnodeCreateUser(pAcct, "monitor", tsInternalPass, NULL);
    mnodeCreateUser(pAcct, "_"TSDB_DEFAULT_USER, tsInternalPass, NULL);
    mnodeDecAcctRef(pAcct);
  }

  if (tsPrintAuth != 0) {
    mInfo("print user auth, for -A parameter is set");
    mnodePrintUserAuth();
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitUsers() {
  SUserObj tObj;
  tsUserUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_USER,
    .tableName    = "users",
    .hashSessions = TSDB_DEFAULT_USERS_HASH_SIZE,
    .maxRowSize   = tsUserUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_STRING,
    .insertFp     = mnodeUserActionInsert,
    .deleteFp     = mnodeUserActionDelete,
    .updateFp     = mnodeUserActionUpdate,
    .encodeFp     = mnodeUserActionEncode,
    .decodeFp     = mnodeUserActionDecode,
    .destroyFp    = mnodeUserActionDestroy,
    .restoredFp   = mnodeUserActionRestored
  };

  tsUserSdb = sdbOpenTable(&tableDesc);
  if (tsUserSdb == NULL) {
    mError("table:%s, failed to create hash", tableDesc.tableName);
    return -1;
  }

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_USER, mnodeProcessCreateUserMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_USER, mnodeProcessAlterUserMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_USER, mnodeProcessDropUserMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_USER, mnodeGetUserMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_USER, mnodeRetrieveUsers);
  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_AUTH, mnodeProcessAuthMsg);
   
  mDebug("table:%s, hash is created", tableDesc.tableName);
  return 0;
}

void mnodeCleanupUsers() {
  sdbCloseTable(tsUserSdb);
  tsUserSdb = NULL;
}

SUserObj *mnodeGetUser(char *name) {
  return (SUserObj *)sdbGetRow(tsUserSdb, name);
}

void *mnodeGetNextUser(void *pIter, SUserObj **pUser) { 
  return sdbFetchRow(tsUserSdb, pIter, (void **)pUser); 
}

void mnodeIncUserRef(SUserObj *pUser) { 
  return sdbIncRef(tsUserSdb, pUser); 
}

void mnodeDecUserRef(SUserObj *pUser) { 
  return sdbDecRef(tsUserSdb, pUser); 
}

static int32_t mnodeUpdateUser(SUserObj *pUser, void *pMsg) {
  SSdbOper oper = {
    .type  = SDB_OPER_GLOBAL,
    .table = tsUserSdb,
    .pObj  = pUser,
    .pMsg  = pMsg
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("user:%s, is altered by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

int32_t mnodeCreateUser(SAcctObj *pAcct, char *name, char *pass, void *pMsg) {
  int32_t code = acctCheck(pAcct, ACCT_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (name[0] == 0) {
    return TSDB_CODE_MND_INVALID_USER_FORMAT;
  }

  if (pass[0] == 0) {
    return TSDB_CODE_MND_INVALID_PASS_FORMAT;
  }

  SUserObj *pUser = mnodeGetUser(name);
  if (pUser != NULL) {
    mDebug("user:%s, is already there", name);
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_USER_ALREADY_EXIST;
  }

  code = grantCheck(TSDB_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pUser = calloc(1, sizeof(SUserObj));
  tstrncpy(pUser->user, name, TSDB_USER_LEN);
  taosEncryptPass((uint8_t*) pass, strlen(pass), pUser->pass);
  strcpy(pUser->acct, pAcct->user);
  pUser->createdTime = taosGetTimestampMs();
  pUser->superAuth = 0;
  pUser->writeAuth = 1;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0 || strcmp(pUser->user, pUser->acct) == 0) {
    pUser->superAuth = 1;
  }

  SSdbOper oper = {
    .type    = SDB_OPER_GLOBAL,
    .table   = tsUserSdb,
    .pObj    = pUser,
    .rowSize = sizeof(SUserObj),
    .pMsg    = pMsg
  };

  code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
    tfree(pUser);
  } else {
    mLInfo("user:%s, is created by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeDropUser(SUserObj *pUser, void *pMsg) {
  SSdbOper oper = {
    .type  = SDB_OPER_GLOBAL,
    .table = tsUserSdb,
    .pObj  = pUser,
    .pMsg  = pMsg
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("user:%s, is dropped by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeGetUserMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) {
    return TSDB_CODE_MND_NO_USER_FROM_CONN;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "privilege");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "account");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  strcpy(pMeta->tableId, "show users");
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = pUser->pAcct->acctInfo.numOfUsers;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mnodeDecUserRef(pUser);
  return 0;
}

static int32_t mnodeRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  SUserObj *pUser    = NULL;
  int32_t  cols      = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextUser(pShow->pIter, &pUser);
    if (pUser == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->user, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pUser->superAuth) {
      const char *src = "super";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else if (pUser->writeAuth) {
      const char *src = "writable";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else {
      const char *src = "readable";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pUser->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->acct, pShow->bytes[cols]);
    cols++;

    numOfRows++;
    mnodeDecUserRef(pUser);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

SUserObj *mnodeGetUserFromConn(void *pConn) {
  SRpcConnInfo connInfo = {0};
  if (rpcGetConnInfo(pConn, &connInfo) == 0) {
    return mnodeGetUser(connInfo.user);
  } else {
    mError("can not get user from conn:%p", pConn);
    return NULL;
  }
}

char *mnodeGetUserFromMsg(void *pMsg) {
  SMnodeMsg *pMnodeMsg = pMsg;
  if (pMnodeMsg != NULL && pMnodeMsg->pUser != NULL) {
    return pMnodeMsg->pUser->user;
  } else {
    return "system";
  }
}

static int32_t mnodeProcessCreateUserMsg(SMnodeMsg *pMsg) {
  SUserObj *pOperUser = pMsg->pUser;
  
  if (pOperUser->superAuth) {
    SCreateUserMsg *pCreate = pMsg->rpcMsg.pCont;
    return mnodeCreateUser(pOperUser->pAcct, pCreate->user, pCreate->pass, pMsg);
  } else {
    mError("user:%s, no rights to create user", pOperUser->user);
    return TSDB_CODE_MND_NO_RIGHTS;
  }
}

static int32_t mnodeProcessAlterUserMsg(SMnodeMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;
  
  SAlterUserMsg *pAlter = pMsg->rpcMsg.pCont;
  SUserObj *pUser = mnodeGetUser(pAlter->user);
  if (pUser == NULL) {
    return TSDB_CODE_MND_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PASSWD) != 0) {
    bool hasRight = false;
    if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = true;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (hasRight) {
      memset(pUser->pass, 0, sizeof(pUser->pass));
      taosEncryptPass((uint8_t*)pAlter->pass, strlen(pAlter->pass), pUser->pass);
      code = mnodeUpdateUser(pUser, pMsg);
    } else {
      mError("user:%s, no rights to alter user", pOperUser->user);
      code = TSDB_CODE_MND_NO_RIGHTS;
    }
  } else if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
    bool hasRight = false;

    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = false;
    } else if (strcmp(pUser->user, pUser->acct) == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = false;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (pAlter->privilege == 1) { // super
      hasRight = false;
    }

    if (hasRight) {
      if (pAlter->privilege == 2) {  // read
        pUser->superAuth = 0;
        pUser->writeAuth = 0;
      }
      if (pAlter->privilege == 3) {  // write
        pUser->superAuth = 0;
        pUser->writeAuth = 1;
      }

      code = mnodeUpdateUser(pUser, pMsg);
    } else {
      mError("user:%s, no rights to alter user", pOperUser->user);
      code = TSDB_CODE_MND_NO_RIGHTS;
    }
  } else {
    mError("user:%s, no rights to alter user", pOperUser->user);
    code = TSDB_CODE_MND_NO_RIGHTS;
  }

  mnodeDecUserRef(pUser);
  return code;
}

static int32_t mnodeProcessDropUserMsg(SMnodeMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;

  SDropUserMsg *pDrop = pMsg->rpcMsg.pCont;
  SUserObj *pUser = mnodeGetUser(pDrop->user);
  if (pUser == NULL) {
    return TSDB_CODE_MND_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || strcmp(pUser->user, pUser->acct) == 0 ||
    (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  bool hasRight = false;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
    hasRight = false;
  } else if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
    hasRight = true;
  } else if (strcmp(pUser->user, pOperUser->user) == 0) {
    hasRight = false;
  } else if (pOperUser->superAuth) {
    if (strcmp(pOperUser->acct, pUser->acct) != 0) {
      hasRight = false;
    } else {
      hasRight = true;
    }
  }

  if (hasRight) {
    code = mnodeDropUser(pUser, pMsg);
  } else {
    code = TSDB_CODE_MND_NO_RIGHTS;
  }

  mnodeDecUserRef(pUser);
  return code;
}

void mnodeDropAllUsers(SAcctObj *pAcct)  {
  void *    pIter = NULL;
  int32_t   numOfUsers = 0;
  int32_t   acctNameLen = strlen(pAcct->user);
  SUserObj *pUser = NULL;

  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;

    if (strncmp(pUser->acct, pAcct->user, acctNameLen) == 0) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsUserSdb,
        .pObj = pUser,
      };
      sdbDeleteRow(&oper);
      numOfUsers++;
    }

    mnodeDecUserRef(pUser);
  }

  sdbFreeIter(pIter);

  mDebug("acct:%s, all users:%d is dropped from sdb", pAcct->user, numOfUsers);
}

int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (!sdbIsMaster()) {
    *secret = 0;
    mDebug("user:%s, failed to auth user, mnode is not master", user);
    return TSDB_CODE_APP_NOT_READY;
  }

  SUserObj *pUser = mnodeGetUser(user);
  if (pUser == NULL) {
    *secret = 0;
    mError("user:%s, failed to auth user, reason:%s", user, tstrerror(TSDB_CODE_MND_INVALID_USER));
    return TSDB_CODE_MND_INVALID_USER;
  } else {
    *spi = 1;
    *encrypt = 0;
    *ckey = 0;

    memcpy(secret, pUser->pass, TSDB_KEY_LEN);
    mnodeDecUserRef(pUser);
    mDebug("user:%s, auth info is returned", user);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessAuthMsg(SMnodeMsg *pMsg) {
  SAuthMsg *pAuthMsg = pMsg->rpcMsg.pCont;
  SAuthRsp *pAuthRsp = rpcMallocCont(sizeof(SAuthRsp));
  
  pMsg->rpcRsp.rsp = pAuthRsp;
  pMsg->rpcRsp.len = sizeof(SAuthRsp);
  
  return mnodeRetriveAuth(pAuthMsg->user, &pAuthRsp->spi, &pAuthRsp->encrypt, pAuthRsp->secret, pAuthRsp->ckey);
}
