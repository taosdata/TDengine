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
#include "ttime.h"
#include "tutil.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtAcct.h"
#include "tgrant.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"

void *  tsUserSdb = NULL;
static int32_t tsUserUpdateSize = 0;
static int32_t mgmtGetUserMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void mgmtProcessCreateUserMsg(SQueuedMsg *pMsg);
static void mgmtProcessAlterUserMsg(SQueuedMsg *pMsg);
static void mgmtProcessDropUserMsg(SQueuedMsg *pMsg);

static int32_t mgmtUserActionDestroy(SSdbOperDesc *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionInsert(SSdbOperDesc *pOper) {
  SUserObj *pUser = pOper->pObj;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);

  if (pAcct != NULL) {
    mgmtAddUserToAcct(pAcct, pUser);
  }
  else {
    mError("user:%s, acct:%s info not exist in sdb", pUser->user, pUser->acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionDelete(SSdbOperDesc *pOper) {
  SUserObj *pUser = pOper->pObj;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);

  if (pAcct != NULL) {
    mgmtDropUserFromAcct(pAcct, pUser);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionUpdate(SSdbOperDesc *pOper) {
  SUserObj *pUser = pOper->pObj;
  SUserObj *pSaved = mgmtGetUser(pUser->user);
  if (pUser != pSaved) {
    memcpy(pSaved, pUser, pOper->rowSize);
    free(pUser);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionEncode(SSdbOperDesc *pOper) {
  SUserObj *pUser = pOper->pObj;
  memcpy(pOper->rowData, pUser, tsUserUpdateSize);
  pOper->rowSize = tsUserUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionDecode(SSdbOperDesc *pOper) {
  SUserObj *pUser = (SUserObj *) calloc(1, sizeof(SUserObj));
  if (pUser == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pUser, pOper->rowData, tsUserUpdateSize);
  pOper->pObj = pUser;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtUserActionRestored() {
  if (dnodeIsFirstDeploy()) {
    SAcctObj *pAcct = mgmtGetAcct("root");
    mgmtCreateUser(pAcct, "root", "taosdata");
    mgmtCreateUser(pAcct, "monitor", tsInternalPass);
    mgmtCreateUser(pAcct, "_root", tsInternalPass);
    mgmtDecAcctRef(pAcct);
  }

  return 0;
}

int32_t mgmtInitUsers() {
  SUserObj tObj;
  tsUserUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_USER,
    .tableName    = "users",
    .hashSessions = TSDB_MAX_USERS,
    .maxRowSize   = tsUserUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_STRING,
    .insertFp     = mgmtUserActionInsert,
    .deleteFp     = mgmtUserActionDelete,
    .updateFp     = mgmtUserActionUpdate,
    .encodeFp     = mgmtUserActionEncode,
    .decodeFp     = mgmtUserActionDecode,
    .destroyFp    = mgmtUserActionDestroy,
    .restoredFp   = mgmtUserActionRestored
  };

  tsUserSdb = sdbOpenTable(&tableDesc);
  if (tsUserSdb == NULL) {
    mError("failed to init user data");
    return -1;
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_USER, mgmtProcessCreateUserMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_USER, mgmtProcessAlterUserMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_USER, mgmtProcessDropUserMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_USER, mgmtGetUserMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_USER, mgmtRetrieveUsers);
  
  mTrace("user data is initialized");
  return 0;
}

void mgmtCleanUpUsers() {
  sdbCloseTable(tsUserSdb);
}

SUserObj *mgmtGetUser(char *name) {
  return (SUserObj *)sdbGetRow(tsUserSdb, name);
}

void mgmtReleaseUser(SUserObj *pUser) { 
  return sdbDecRef(tsUserSdb, pUser); 
}

static int32_t mgmtUpdateUser(SUserObj *pUser) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsUserSdb,
    .pObj = pUser,
    .rowSize = tsUserUpdateSize
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

int32_t mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass) {
  int32_t code = acctCheck(pAcct, ACCT_GRANT_USER);
  if (code != 0) {
    return code;
  }

  if (name[0] == 0 || pass[0] == 0) {
    return TSDB_CODE_INVALID_MSG_CONTENT;
  }

  SUserObj *pUser = mgmtGetUser(name);
  if (pUser != NULL) {
    mTrace("user:%s is already there", name);
    mgmtReleaseUser(pUser);
    return TSDB_CODE_USER_ALREADY_EXIST;
  }

  code = grantCheck(TSDB_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pUser = calloc(1, sizeof(SUserObj));
  strcpy(pUser->user, name);
  taosEncryptPass((uint8_t*) pass, strlen(pass), pUser->pass);
  strcpy(pUser->acct, pAcct->user);
  pUser->createdTime = taosGetTimestampMs();
  pUser->superAuth = 0;
  pUser->writeAuth = 1;
  if (strcmp(pUser->user, "root") == 0 || strcmp(pUser->user, pUser->acct) == 0) {
    pUser->superAuth = 1;
  }

  SSdbOperDesc oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsUserSdb,
    .pObj = pUser,
    .rowSize = sizeof(SUserObj)
  };

  code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pUser);
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

static int32_t mgmtDropUser(SUserObj *pUser) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsUserSdb,
    .pObj = pUser
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

static int32_t mgmtGetUserMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) {
    return TSDB_CODE_INVALID_USER;
  }

  int32_t cols     = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 6;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "privilege");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
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

  mgmtReleaseUser(pUser);
  return 0;
}

static int32_t mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  SUserObj *pUser    = NULL;
  int32_t  cols      = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsUserSdb, pShow->pNode, (void **) &pUser);
    if (pUser == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pUser->user);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pUser->superAuth) {
      strcpy(pWrite, "super");
    } else if (pUser->writeAuth) {
      strcpy(pWrite, "write");
    } else {
      strcpy(pWrite, "read");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pUser->createdTime;
    cols++;

    numOfRows++;
    mgmtReleaseUser(pUser);
  }
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

SUserObj *mgmtGetUserFromConn(void *pConn, bool *usePublicIp) {
  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(pConn, &connInfo) == 0) {
    if (usePublicIp) {
      *usePublicIp = (connInfo.serverIp == tsPublicIpInt);
    }
    return mgmtGetUser(connInfo.user);
  }

  return NULL;
}

static void mgmtProcessCreateUserMsg(SQueuedMsg *pMsg) {
  int32_t code;
  SUserObj *pUser = pMsg->pUser;
  
  if (pUser->superAuth) {
    SCMCreateUserMsg *pCreate = pMsg->pCont;
    code = mgmtCreateUser(pUser->pAcct, pCreate->user, pCreate->pass);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is created by %s", pCreate->user, pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static void mgmtProcessAlterUserMsg(SQueuedMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;
  
  SCMAlterUserMsg *pAlter = pMsg->pCont;
  SUserObj *pUser = mgmtGetUser(pAlter->user);
  if (pUser == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_USER);
    return;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    mgmtReleaseUser(pUser);
    return;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PASSWD) != 0) {
    bool hasRight = false;
    if (strcmp(pOperUser->user, "root") == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = true;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, "root") == 0) {
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
      code = mgmtUpdateUser(pUser);
      mLPrint("user:%s password is altered by %s, result:%d", pUser->user, pOperUser->user, tstrerror(code));
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    mgmtSendSimpleResp(pMsg->thandle, code);
  } else if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
    bool hasRight = false;

    if (strcmp(pUser->user, "root") == 0) {
      hasRight = false;
    } else if (strcmp(pUser->user, pUser->acct) == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->user, "root") == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = false;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, "root") == 0) {
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

      code = mgmtUpdateUser(pUser);
      mLPrint("user:%s privilege is altered by %s, result:%d", pUser->user, pOperUser->user, tstrerror(code));
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    mgmtSendSimpleResp(pMsg->thandle, code);
  } else {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
  }

  mgmtReleaseUser(pUser);
}

static void mgmtProcessDropUserMsg(SQueuedMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;

  SCMDropUserMsg *pDrop = pMsg->pCont;
  SUserObj *pUser = mgmtGetUser(pDrop->user);
  if (pUser == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_USER);
    return ;
  }

  if (strcmp(pUser->user, "monitor") == 0 || strcmp(pUser->user, pUser->acct) == 0 ||
    (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    mgmtReleaseUser(pUser);
    return ;
  }

  bool hasRight = false;
  if (strcmp(pUser->user, "root") == 0) {
    hasRight = false;
  } else if (strcmp(pOperUser->user, "root") == 0) {
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
    code = mgmtDropUser(pUser);
    if (code == TSDB_CODE_SUCCESS) {
       mLPrint("user:%s is dropped by %s, result:%s", pUser->user, pOperUser->user, tstrerror(code));
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
  mgmtReleaseUser(pUser);
}

void  mgmtDropAllUsers(SAcctObj *pAcct)  {
  void *    pNode = NULL;
  void *    pLastNode = NULL;
  int32_t   numOfUsers = 0;
  int32_t   acctNameLen = strlen(pAcct->user);
  SUserObj *pUser = NULL;

  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(tsUserSdb, pNode, (void **)&pUser);
    if (pUser == NULL) break;

    if (strncmp(pUser->acct, pAcct->user, acctNameLen) == 0) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsUserSdb,
        .pObj = pUser,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfUsers++;
    }

    mgmtReleaseUser(pUser);
  }

  mTrace("acct:%s, all users:%d is dropped from sdb", pAcct->user, numOfUsers);
}