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
#include "mgmt.h"

extern void *userSdb;
extern void *dbSdb;
SAcctObj     acctObj;

int mgmtInitAccts() { return 0; }

void mgmtCreateRootAcct() {}

SAcctObj *mgmtGetAcct(char *name) { return &acctObj; }

int mgmtCheckUserLimit(SAcctObj *pAcct) {
  int numOfUsers = sdbGetNumOfRows(userSdb);
  if (numOfUsers >= tsMaxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d", numOfUsers, tsMaxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return 0;
}

int mgmtCheckDbLimit(SAcctObj *pAcct) {
  int numOfDbs = sdbGetNumOfRows(dbSdb);
  if (numOfDbs >= tsMaxDbs) {
    mWarn("numOfDbs:%d, exceed tsMaxDbs:%d", numOfDbs, tsMaxDbs);
    return TSDB_CODE_TOO_MANY_DATABSES;
  }
  return 0;
}

int mgmtCheckMeterLimit(SAcctObj *pAcct) { return 0; }

int mgmtCheckUserGrant() { return 0; }

int mgmtCheckDbGrant() { return 0; }

int mgmtCheckMeterGrant() { return 0; }

void grantAddTimeSeries(uint32_t timeSeriesNum) {}

void mgmtCheckAcct() {
  SAcctObj *pAcct = &acctObj;
  pAcct->acctId = 0;
  strcpy(pAcct->user, "root");

  mgmtCreateUser(pAcct, "root", "taosdata");
  mgmtCreateUser(pAcct, "monitor", tsInternalPass);
  mgmtCreateUser(pAcct, "_root", tsInternalPass);
}

void mgmtCleanUpAccts() {}

int mgmtGetAcctMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) { return TSDB_CODE_OPS_NOT_SUPPORT; }

int mgmtRetrieveAccts(SShowObj *pShow, char *data, int rows, SConnObj *pConn) { return 0; }
