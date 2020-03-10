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
#include "tschemautil.h"
#include "ttime.h"
#include "mnode.h"


#include "mgmtMnode.h"
#include "mgmtShell.h"
#include "mgmtUser.h"

#include "mgmtAcct.h"
#include "mgmtGrant.h"
#include "mgmtTable.h"


void *tsUserSdb = NULL;
static int32_t tsUserUpdateSize = 0;

static int32_t   mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass);
static int32_t   mgmtDropUser(SAcctObj *pAcct, char *name);
static int32_t   mgmtUpdateUser(SUserObj *pUser);
static int32_t   mgmtGetUserMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
static int32_t   mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static SUserObj *mgmtGetUserFromConn(void *pConn);

static void mgmtProcessCreateUserMsg(SRpcMsg *rpcMsg);
static void mgmtProcessAlterUserMsg(SRpcMsg *rpcMsg);
static void mgmtProcessDropUserMsg(SRpcMsg *rpcMsg);

static void *(*mgmtUserActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionReset(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtUserAction(char action, void *row, char *str, int32_t size, int32_t *ssize);
static void  mgmtUserActionInit();

int32_t mgmtInitUsers() {
  void     *pNode     = NULL;
  SUserObj *pUser     = NULL;
  SAcctObj *pAcct     = NULL;
  int32_t  numOfUsers = 0;

  mgmtUserActionInit();

  SUserObj tObj;
  tsUserUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  tsUserSdb = sdbOpenTable(tsMaxUsers, tsUserUpdateSize, "user", SDB_KEYTYPE_STRING, tsMgmtDirectory, mgmtUserAction);
  if (tsUserSdb == NULL) {
    mError("failed to init user data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(tsUserSdb, pNode, (void **)&pUser);
    if (pUser == NULL) break;

    pUser->prev = NULL;
    pUser->next = NULL;

    pAcct = mgmtGetAcct(pUser->acct);
    mgmtAddUserIntoAcct(pAcct, pUser);

    numOfUsers++;
  }

  pAcct = mgmtGetAcct("root");
  mgmtCreateUser(pAcct, "root", "taosdata");
  mgmtCreateUser(pAcct, "monitor", tsInternalPass);
  mgmtCreateUser(pAcct, "_root", tsInternalPass);

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CREATE_USER, mgmtProcessCreateUserMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_ALTER_USER, mgmtProcessAlterUserMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_DROP_USER, mgmtProcessDropUserMsg);
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

static int32_t mgmtUpdateUser(SUserObj *pUser) {
  return sdbUpdateRow(tsUserSdb, pUser, 0, 1);
}

static int32_t mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass) {
  int32_t code = mgmtCheckUserLimit(pAcct);
  if (code != 0) {
    return code;
  }

  if (name[0] == 0 || pass[0] == 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  SUserObj *pUser = (SUserObj *)sdbGetRow(tsUserSdb, name);
  if (pUser != NULL) {
    mWarn("user:%s is already there", name);
    return TSDB_CODE_USER_ALREADY_EXIST;
  }

  code = mgmtCheckUserGrant();
  if (code != 0) {
    return code;
  }

  pUser = malloc(sizeof(SUserObj));
  memset(pUser, 0, sizeof(SUserObj));
  strcpy(pUser->user, name);
  taosEncryptPass((uint8_t*) pass, strlen(pass), pUser->pass);
  strcpy(pUser->acct, pAcct->user);
  pUser->createdTime = taosGetTimestampMs();
  pUser->superAuth = 0;
  pUser->writeAuth = 1;
  if (strcmp(pUser->user, "root") == 0 || strcmp(pUser->user, pUser->acct) == 0) {
    pUser->superAuth = 1;
  }

  code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(tsUserSdb, pUser, 0) < 0) {
    tfree(pUser);
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

static int32_t mgmtDropUser(SAcctObj *pAcct, char *name) {
  SUserObj *pUser;

  pUser = (SUserObj *)sdbGetRow(tsUserSdb, name);
  if (pUser == NULL) {
    mWarn("user:%s is not there", name);
    return TSDB_CODE_INVALID_USER;
  }

  if (strcmp(pAcct->user, pUser->acct) != 0) {
    return TSDB_CODE_NO_RIGHTS;
  }

  sdbDeleteRow(tsUserSdb, pUser);

  return 0;
}

static int32_t mgmtGetUserMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) {
    return TSDB_CODE_INVALID_USER;
  }

  int32_t cols     = 0;
  SSchema *pSchema = tsGetSchema(pMeta);

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
  strcpy(pSchema[cols].name, "created time");
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
  pShow->pNode = pUser->pAcct->pUser;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  SUserObj *pUser    = NULL;
  int32_t  cols      = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pUser = (SUserObj *)pShow->pNode;
    if (pUser == NULL) break;
    pShow->pNode = (void *)pUser->next;

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
  }
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mgmtUserActionInit() {
  mgmtUserActionFp[SDB_TYPE_INSERT]  = mgmtUserActionInsert;
  mgmtUserActionFp[SDB_TYPE_DELETE]  = mgmtUserActionDelete;
  mgmtUserActionFp[SDB_TYPE_UPDATE]  = mgmtUserActionUpdate;
  mgmtUserActionFp[SDB_TYPE_ENCODE]  = mgmtUserActionEncode;
  mgmtUserActionFp[SDB_TYPE_DECODE]  = mgmtUserActionDecode;
  mgmtUserActionFp[SDB_TYPE_RESET]   = mgmtUserActionReset;
  mgmtUserActionFp[SDB_TYPE_DESTROY] = mgmtUserActionDestroy;
}

static void *mgmtUserAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtUserActionFp[(uint8_t) action] != NULL) {
    return (*(mgmtUserActionFp[(uint8_t) action]))(row, str, size, ssize);
  }
  return NULL;
}

static void *mgmtUserActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *) row;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);

  pUser->pAcct = pAcct;
  mgmtAddUserIntoAcct(pAcct, pUser);

  return NULL;
}

static void *mgmtUserActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *) row;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);

  mgmtRemoveUserFromAcct(pAcct, pUser);

  return NULL;
}

static void *mgmtUserActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  return mgmtUserActionReset(row, str, size, ssize);
}

static void *mgmtUserActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *) row;

  if (size < tsUserUpdateSize) {
    *ssize = -1;
  } else {
    memcpy(str, pUser, tsUserUpdateSize);
    *ssize = tsUserUpdateSize;
  }

  return NULL;
}

static void *mgmtUserActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *) malloc(sizeof(SUserObj));
  if (pUser == NULL) return NULL;
  memset(pUser, 0, sizeof(SUserObj));

  memcpy(pUser, str, tsUserUpdateSize);

  return (void *)pUser;
}

static void *mgmtUserActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)row;

  memcpy(pUser, str, tsUserUpdateSize);

  return NULL;
}

static void *mgmtUserActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  tfree(row);

  return NULL;
}

static SUserObj *mgmtGetUserFromConn(void *pConn) {
  SRpcConnInfo connInfo;
  rpcGetConnInfo(pConn, &connInfo);

  return mgmtGetUser(connInfo.user);
}

static void mgmtProcessCreateUserMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  if (pUser->superAuth) {
    SCreateUserMsg *pCreate = rpcMsg->pCont;
    rpcRsp.code = mgmtCreateUser(pUser->pAcct, pCreate->user, pCreate->pass);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is created by %s", pCreate->user, pUser->user);
    }
  } else {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  }

  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessAlterUserMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SUserObj *pOperUser = mgmtGetUserFromConn(rpcMsg->handle);
  if (pOperUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SAlterUserMsg *pAlter = rpcMsg->pCont;
  SUserObj *pUser = mgmtGetUser(pAlter->user);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    rpcSendResponse(&rpcRsp);
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
      rpcRsp.code = mgmtUpdateUser(pUser);
      mLPrint("user:%s password is altered by %s, code:%d", pAlter->user, pUser->user, rpcRsp.code);
    } else {
      rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    }

    rpcSendResponse(&rpcRsp);
    return;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
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
      //if (pAlter->privilege == 1) {  // super
      //  pUser->superAuth = 1;
      //  pUser->writeAuth = 1;
      //}
      if (pAlter->privilege == 2) {  // read
        pUser->superAuth = 0;
        pUser->writeAuth = 0;
      }
      if (pAlter->privilege == 3) {  // write
        pUser->superAuth = 0;
        pUser->writeAuth = 1;
      }

      rpcRsp.code = mgmtUpdateUser(pUser);
      mLPrint("user:%s privilege is altered by %s, code:%d", pAlter->user, pUser->user, rpcRsp.code);
    } else {
      rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    }

    rpcSendResponse(&rpcRsp);
    return;
  }

  rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessDropUserMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SUserObj *pOperUser = mgmtGetUserFromConn(rpcMsg->handle);
  if (pOperUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return ;
  }

  SDropUserMsg *pDrop = rpcMsg->pCont;
  SUserObj *pUser = mgmtGetUser(pDrop->user);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return ;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    rpcSendResponse(&rpcRsp);
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
    if (strcmp(pUser->user, "root") == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
      hasRight = false;
    } else {
      hasRight = true;
    }
  }

  if (hasRight) {
    rpcRsp.code = mgmtDropUser(pUser->pAcct, pDrop->user);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is dropped by %s", pDrop->user, pUser->user);
    }
  } else {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  }

  rpcSendResponse(&rpcRsp);
}
