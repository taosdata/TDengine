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

#include "mnode.h"
#include "sdb.h"
#include "tschemautil.h"


int mgmtProcessCfgMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  int      code = 0;
  SCfgDnodeMsg *pCfg = (SCfgDnodeMsg *)pMsg;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CFG_MNODE_RSP);

  if (strcmp(pConn->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = sdbCfgNode(pMsg);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_MNODE_RSP, code);

  if (code == 0) mTrace("mnode:%s is configured by %s", pCfg->ip, pConn->pUser->user);

  return 0;
}

int mgmtProcessDropMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  SDropMnodeMsg *pDrop = (SDropMnodeMsg *)pMsg;
  int            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_MNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    // code = sdbRemovePeerByIp(inet_addr(pDrop->ip));
    SDnodeObj *pDnode = mgmtGetDnode(inet_addr(pDrop->ip));
    if (pDnode != NULL) {
      code = mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_MNODE_RSP, code);

  if (code == 0) {
    mLPrint("Mnode:%s is dropped by %s", pDrop->ip, pConn->pUser->user);
  }

  return 0;
}