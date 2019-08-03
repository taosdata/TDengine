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

#include <arpa/inet.h>

#include "mgmt.h"
#include "tschemautil.h"

void *userSdb = NULL;
int   tsUserUpdateSize;

void *(*mgmtUserActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);
void *mgmtUserActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtUserActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtUserActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtUserActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtUserActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtUserActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtUserActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtUserActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtUserActionReset(void *row, char *str, int size, int *ssize);
void *mgmtUserActionDestroy(void *row, char *str, int size, int *ssize);

void mgmtUserActionInit() {
  mgmtUserActionFp[SDB_TYPE_INSERT] = mgmtUserActionInsert;
  mgmtUserActionFp[SDB_TYPE_DELETE] = mgmtUserActionDelete;
  mgmtUserActionFp[SDB_TYPE_UPDATE] = mgmtUserActionUpdate;
  mgmtUserActionFp[SDB_TYPE_ENCODE] = mgmtUserActionEncode;
  mgmtUserActionFp[SDB_TYPE_DECODE] = mgmtUserActionDecode;
  mgmtUserActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtUserActionBeforeBatchUpdate;
  mgmtUserActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtUserActionBatchUpdate;
  mgmtUserActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtUserActionAfterBatchUpdate;
  mgmtUserActionFp[SDB_TYPE_RESET] = mgmtUserActionReset;
  mgmtUserActionFp[SDB_TYPE_DESTROY] = mgmtUserActionDestroy;
}

void *mgmtUserAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtUserActionFp[action] != NULL) {
    return (*(mgmtUserActionFp[action]))(row, str, size, ssize);
  }
  return NULL;
}

int mgmtInitUsers() {
  void *    pNode = NULL;
  SUserObj *pUser = NULL;
  SAcctObj *pAcct = NULL;
  int       numOfUsers = 0;

  mgmtUserActionInit();

  userSdb = sdbOpenTable(tsMaxUsers, sizeof(SUserObj), "user", SDB_KEYTYPE_STRING, mgmtDirectory, mgmtUserAction);
  if (userSdb == NULL) {
    mError("failed to init user data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(userSdb, pNode, (void **)&pUser);
    if (pUser == NULL) break;

    pUser->prev = NULL;
    pUser->next = NULL;

    pAcct = &acctObj;
    mgmtAddUserIntoAcct(pAcct, pUser);

    numOfUsers++;
  }

  SUserObj tObj;
  tsUserUpdateSize = tObj.updateEnd - (char *)&tObj;

  mTrace("user data is initialized");
  return 0;
}

SUserObj *mgmtGetUser(char *name) { return (SUserObj *)sdbGetRow(userSdb, name); }

int mgmtUpdateUser(SUserObj *pUser) { return sdbUpdateRow(userSdb, pUser, 0, 1); }

int mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass) {
  SUserObj *pUser;

  int numOfUsers = sdbGetNumOfRows(userSdb);
  if (numOfUsers >= tsMaxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d", numOfUsers, tsMaxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }

  pUser = (SUserObj *)sdbGetRow(userSdb, name);
  if (pUser != NULL) {
    mWarn("user:%s is already there", name);
    return TSDB_CODE_USER_ALREADY_EXIST;
  }

  pUser = malloc(sizeof(SUserObj));
  memset(pUser, 0, sizeof(SUserObj));
  strcpy(pUser->user, name);
  taosEncryptPass((uint8_t *)pass, strlen(pass), pUser->pass);
  strcpy(pUser->acct, pAcct->user);
  pUser->createdTime = taosGetTimestampMs();
  pUser->superAuth = 0;
  pUser->writeAuth = 1;
  if (strcmp(pUser->user, "root") == 0 || strcmp(pUser->user, pUser->acct) == 0) {
    pUser->superAuth = 1;
  }

  int code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(userSdb, pUser, 0) < 0) {
    tfree(pUser);
    code = TSDB_CODE_SDB_ERROR;
  }

  //  mgmtAddUserIntoAcct(pAcct, pUser);

  return code;
}

int mgmtDropUser(SAcctObj *pAcct, char *name) {
  SUserObj *pUser;

  pUser = (SUserObj *)sdbGetRow(userSdb, name);
  if (pUser == NULL) {
    mWarn("user:%s is not there", name);
    return TSDB_CODE_INVALID_USER;
  }

  if (strcmp(pAcct->user, pUser->acct) != 0) return TSDB_CODE_NO_RIGHTS;

  //  mgmtRemoveUserFromAcct(pAcct, pUser);
  sdbDeleteRow(userSdb, pUser);

  return 0;
}

void mgmtCleanUpUsers() { sdbCloseTable(userSdb); }

int mgmtGetUserMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int      cols = 0;
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
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  //  pShow->numOfRows = sdbGetNumOfRows (userSdb);
  pShow->numOfRows = pConn->pAcct->acctInfo.numOfUsers;
  pShow->pNode = pConn->pAcct->pUser;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveUsers(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int       numOfRows = 0;
  SUserObj *pUser = NULL;
  char *    pWrite;
  int       cols = 0;

  while (numOfRows < rows) {
    //    pShow->pNode = sdbFetchRow(userSdb, pShow->pNode, (void **)&pUser);
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

void *mgmtUserActionInsert(void *row, char *str, int size, int *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  SAcctObj *pAcct = &acctObj;
  mgmtAddUserIntoAcct(pAcct, pUser);

  return NULL;
}
void *mgmtUserActionDelete(void *row, char *str, int size, int *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  SAcctObj *pAcct = &acctObj;
  mgmtRemoveUserFromAcct(pAcct, pUser);

  return NULL;
}
void *mgmtUserActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtUserActionReset(row, str, size, ssize);
}
void *mgmtUserActionEncode(void *row, char *str, int size, int *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  int       tsize = pUser->updateEnd - (char *)pUser;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pUser, tsize);
    *ssize = tsize;
  }
  return NULL;
}
void *mgmtUserActionDecode(void *row, char *str, int size, int *ssize) {
  SUserObj *pUser = (SUserObj *)malloc(sizeof(SUserObj));
  if (pUser == NULL) return NULL;
  memset(pUser, 0, sizeof(SUserObj));

  int tsize = pUser->updateEnd - (char *)pUser;
  memcpy(pUser, str, tsize);
  return (void *)pUser;
}
void *mgmtUserActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtUserActionBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtUserActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtUserActionReset(void *row, char *str, int size, int *ssize) {
  SUserObj *pUser = (SUserObj *)row;
  int       tsize = pUser->updateEnd - (char *)pUser;
  memcpy(pUser, str, tsize);

  return NULL;
}

void *mgmtUserActionDestroy(void *row, char *str, int size, int *ssize) {
  tfree(row);
  return NULL;
}
