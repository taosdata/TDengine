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
#include "balance.h"
#include "balanceEngine.h"
#include "balanceScore.h"

void balanceInit() {
  balanceInitResourceFp          = balanceInitResource;
  balanceCleanUpResourceFp       = balanceCleanUpResource;
  balanceNotifyFp                = balanceNotify;
  balanceAllocVnodesFp           = balanceAllocVnodes;
  balanceSetDnodeRemoveStateFp   = balanceSetDnodeRemoveState;
  balanceSetDnodeUnRemoveStateFp = balanceSetDnodeUnRemoveState;
  balanceGetScoresMetaFp         = balanceGetScoresMeta;
  balanceRetrieveScoresFp        = balanceRetrieveScores;

  //TODO:create dnode
  int32_t numOfDnodes = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfDnodes <= 0) {
    pDnode->moduleStatus |= (1 << TSDB_MOD_MGMT);
  }

  //TODO:drop dnode
  pDnode->moduleStatus &= ~(1 << TSDB_MOD_MGMT);


  
}


int32_t mgmtGetScoresMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  if (mgmtGetScoresMetaFp) {
    SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
    if (pUser == NULL) return 0;
    if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;
    return mgmtGetScoresMetaFp(pMeta, pShow, pConn);
  } else {
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }
}

int32_t mgmtRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  if (mgmtRetrieveScoresFp) {
    return mgmtRetrieveScoresFp(pShow, data, rows, pConn);
  } else {
    return 0;
  }
}
