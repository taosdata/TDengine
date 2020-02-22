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
#include "tschemautil.h"
#include "ttime.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtUser.h"
#include "mgmtGrant.h"
#include "mgmtTable.h"

void *tsUserSdb = NULL;

void *(*mgmtUserActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionReset(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtUserActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

void mgmtUserActionInit() {
  mgmtUserActionFp[SDB_TYPE_INSERT]  = mgmtUserActionInsert;
  mgmtUserActionFp[SDB_TYPE_DELETE]  = mgmtUserActionDelete;
  mgmtUserActionFp[SDB_TYPE_UPDATE]  = mgmtUserActionUpdate;
  mgmtUserActionFp[SDB_TYPE_ENCODE]  = mgmtUserActionEncode;
  mgmtUserActionFp[SDB_TYPE_DECODE]  = mgmtUserActionDecode;
  mgmtUserActionFp[SDB_TYPE_RESET]   = mgmtUserActionReset;
  mgmtUserActionFp[SDB_TYPE_DESTROY] = mgmtUserActionDestroy;
}

void *mgmtUserAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtUserActionFp[(uint8_t) action] != NULL) {
    return (*(mgmtUserActionFp[(uint8_t) action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitUsers() {
  void     *pNode     = NULL;
  SUserObj *pUser     = NULL;
  SAcctObj *pAcct     = NULL;
  int32_t  numOfUsers = 0;

  mgmtUserActionInit();

  tsUserSdb = sdbOpenTable(tsMaxUsers, sizeof(SUserObj), "user", SDB_KEYTYPE_STRING, tsMgmtDirectory, mgmtUserAction);
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

  mTrace("user data is initialized");
  return 0;
}

SUserObj *mgmtGetUser(char *name) {
  return (SUserObj *)sdbGetRow(tsUserSdb, name);
}

int32_t mgmtUpdateUser(SUserObj *pUser) {
  return sdbUpdateRow(tsUserSdb, pUser, 0, 1);
}

int32_t mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass) {
  SUserObj *pUser;
  int32_t code;

  code = mgmtCheckUserLimit(pAcct);
  if (code != 0) {
    return code;
  }

  pUser = (SUserObj *)sdbGetRow(tsUserSdb, name);
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

int32_t mgmtDropUser(SAcctObj *pAcct, char *name) {
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

void mgmtCleanUpUsers() {
  sdbCloseTable(tsUserSdb);
}

int32_t mgmtGetUserMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
//  int32_t      cols = 0;
//  SSchema *pSchema = tsGetSchema(pMeta);
//
//  pShow->bytes[cols] = TSDB_USER_LEN;
//  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
//  strcpy(pSchema[cols].name, "name");
//  pSchema[cols].bytes = htons(pShow->bytes[cols]);
//  cols++;
//
//  pShow->bytes[cols] = 6;
//  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
//  strcpy(pSchema[cols].name, "privilege");
//  pSchema[cols].bytes = htons(pShow->bytes[cols]);
//  cols++;
//
//  pShow->bytes[cols] = 8;
//  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
//  strcpy(pSchema[cols].name, "created time");
//  pSchema[cols].bytes = htons(pShow->bytes[cols]);
//  cols++;
//
//  pMeta->numOfColumns = htons(cols);
//  pShow->numOfColumns = cols;
//
//  pShow->offset[0] = 0;
//  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
//
//  pShow->numOfRows = pConn->pAcct->acctInfo.numOfUsers;
//  pShow->pNode = pConn->pAcct->pUser;
//  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t       numOfRows = 0;
//  SUserObj *pUser = NULL;
//  char *    pWrite;
//  int32_t       cols = 0;
//
//  while (numOfRows < rows) {
//    pUser = (SUserObj *)pShow->pNode;
//    if (pUser == NULL) break;
//    pShow->pNode = (void *)pUser->next;
//
//    cols = 0;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    strcpy(pWrite, pUser->user);
//    cols++;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    if (pUser->superAuth) {
//      strcpy(pWrite, "super");
//    } else if (pUser->writeAuth) {
//      strcpy(pWrite, "write");
//    } else {
//      strcpy(pWrite, "read");
//    }
//    cols++;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    *(int64_t *)pWrite = pUser->createdTime;
//    cols++;
//
//    numOfRows++;
//  }
//  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void *mgmtUserActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);
  mgmtAddUserIntoAcct(pAcct, pUser);

  return NULL;
}

void *mgmtUserActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);
  mgmtRemoveUserFromAcct(pAcct, pUser);

  return NULL;
}

void *mgmtUserActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  return mgmtUserActionReset(row, str, size, ssize);
}

void *mgmtUserActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  int32_t       tsize = pUser->updateEnd - (char *)pUser;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pUser, tsize);
    *ssize = tsize;
  }
  return NULL;
}

void *mgmtUserActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)malloc(sizeof(SUserObj));
  if (pUser == NULL) return NULL;
  memset(pUser, 0, sizeof(SUserObj));

  int32_t tsize = pUser->updateEnd - (char *)pUser;
  memcpy(pUser, str, tsize);
  return (void *)pUser;
}

void *mgmtUserActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  int32_t       tsize = pUser->updateEnd - (char *)pUser;
  memcpy(pUser, str, tsize);

  return NULL;
}

void *mgmtUserActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  tfree(row);
  return NULL;
}

SUserObj *mgmtGetUserFromConn(void *pConn) {
  SRpcConnInfo connInfo;
  rpcGetConnInfo(pConn, &connInfo);
  return mgmtGetUser(connInfo.user);
}
