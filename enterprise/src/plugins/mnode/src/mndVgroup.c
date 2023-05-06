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

#include "mndVgroup.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndTrans.h"

extern int32_t mndAddVgroupBalanceToTrans(SMnode *pMnode, SVgObj *pVgroup, STrans *pTrans);

int32_t mndProcessVgroupBalanceLeaderMsgImp(SRpcMsg *pReq) {
  int32_t code = -1;
  
  SBalanceVgroupLeaderReq req = {0};
  if (tDeserializeSBalanceVgroupLeaderReq(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return code;
  }

  SMnode *pMnode = pReq->info.node;
  SSdb *pSdb = pMnode->pSdb;

  int32_t total = sdbGetSize(pSdb, SDB_VGROUP);
  if(total <= 0) {
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;
    return code;
  }
  
  STrans *pTrans = NULL;
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "bal-vg-leader");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to balance vgroup leader", pTrans->id);

  void *pIter = NULL;
  int32_t count = 0;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if(mndAddVgroupBalanceToTrans(pMnode, pVgroup, pTrans) == 0){
      count++;
    }

    sdbRelease(pSdb, pVgroup);
  }
  
  if(count == 0) {
    terrno = TSDB_CODE_TSC_INVALID_OPERATION;
    goto _OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

int32_t mndProcessSplitVgroupMsgImp(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = -1;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb = NULL;

  SSplitVgroupReq req = {0};
  if (tDeserializeSSplitVgroupReq(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("vgId:%d, start to split", req.vgId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SPLIT_VGROUP) != 0) {
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, req.vgId);
  if (pVgroup == NULL) goto _OVER;

  pDb = mndAcquireDb(pMnode, pVgroup->dbName);
  if (pDb == NULL) goto _OVER;

  code = mndSplitVgroup(pMnode, pReq, pDb, pVgroup);
  if (code != 0) {
    mError("vgId:%d, failed to start to split vgroup since %s, db:%s", pVgroup->vgId, terrstr(), pDb->name);
    goto _OVER;
  }

  mInfo("vgId:%d, split vgroup started successfully. db:%s", pVgroup->vgId, pDb->name);

_OVER:
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseDb(pMnode, pDb);
  return code;
}
