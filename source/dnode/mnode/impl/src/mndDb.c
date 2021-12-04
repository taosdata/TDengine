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
#include "mndDb.h"

static int32_t mnodeProcessUseMsg(SMnodeMsg *pMsg);

int32_t mndInitDb(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_USE_DB, mnodeProcessUseMsg);
  return 0;
}

void mndCleanupDb(SMnode *pMnode) {}

SDbObj *mndAcquireDb(SMnode *pMnode, char *db) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_DB, db);
}

void mndReleaseDb(SMnode *pMnode, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDb);
}

static int32_t mnodeProcessUseMsg(SMnodeMsg *pMsg) {
  SMnode    *pMnode = pMsg->pMnode;
  SUseDbMsg *pUse = pMsg->rpcMsg.pCont;

  strncpy(pMsg->db, pUse->db, TSDB_FULL_DB_NAME_LEN);

  SDbObj *pDb = mndAcquireDb(pMnode, pMsg->db);
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
    return 0;
  } else {
    mError("db:%s, failed to process use db msg since %s", pMsg->db, terrstr());
    return -1;
  }
}
