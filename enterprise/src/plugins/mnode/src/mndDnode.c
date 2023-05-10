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

#include "mndDnode.h"
#include "mndTrans.h"

static int32_t mndProcessRestoreDnodeReqImpl(SRpcMsg *pReq){
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDnodeObj    *pDnode = NULL;
  SMnodeObj    *pMObj = NULL;
  SQnodeObj    *pQObj = NULL;
  SSnodeObj    *pSObj = NULL;
  SRestoreDnodeReq restoreReq = {0};

  if (tDeserializeSRestoreDnodeReq(pReq->pCont, pReq->contLen, &restoreReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  //mInfo("dnode:%d, start to restore, ep:%s:%d", restoreReq.dnodeId, restoreReq.fqdn, restoreReq.port);
  mInfo("dnode:%d, start to restore, restore type:%d", restoreReq.dnodeId, restoreReq.restoreType);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE) != 0) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, restoreReq.dnodeId);
  /*if (pDnode == NULL) {
    int32_t err = terrno;
    char    ep[TSDB_EP_LEN + 1] = {0};
    snprintf(ep, sizeof(ep), restoreReq.fqdn, restoreReq.port);
    pDnode = mndAcquireDnodeByEp(pMnode, ep);
    if (pDnode == NULL) {
      terrno = err;
      goto _OVER;
    }
  }
  */
  if (pDnode == NULL) {
    goto _OVER;
  }

  code = mndRestoreDnode(pMnode, pReq, pDnode, restoreReq.restoreType);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to restore, restoreType:%d,  since %s", 
                    restoreReq.dnodeId, restoreReq.restoreType, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  return code;
}