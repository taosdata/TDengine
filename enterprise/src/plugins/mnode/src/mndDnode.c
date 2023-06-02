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
#include "mndPrivilege.h"
#include "mndMnode.h"
#include "mndVgroup.h"
#include "mndDb.h"
#include "mndQnode.h"

int32_t mndRestoreDnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, int8_t restoreType) {
  int32_t  code = -1;
  STrans  *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "restore-dnode");
  if (pTrans == NULL) goto _OVER;

  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to restore dnode:%s", pTrans->id, pDnode->ep);

  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  bool needRestore = false;

  if(restoreType == RESTORE_TYPE__ALL || restoreType == RESTORE_TYPE__MNODE)
  {
    SMnodeObj *mnodeObj = mndAcquireMnode(pMnode, pDnode->id);
    if(mnodeObj == NULL){
      mError("trans:%d, no mnode exist on dnode:%s", pTrans->id, pDnode->ep);
      terrno = TSDB_CODE_MNODE_NOT_FOUND;
    }
    else{
      int32_t totalMnodes = sdbGetSize(pMnode->pSdb, SDB_MNODE);
      if (totalMnodes == 2) {
        mError("cant't restore mnode, since a mnode on it and replica is 2");
        terrno = TSDB_CODE_MNODE_ONLY_TWO_MNODE;

        mndReleaseMnode(pMnode, mnodeObj);
      }
      else{
        SMnodeObj newMnodeObj = {0};
        newMnodeObj.id = pDnode->id;
        newMnodeObj.createdTime = taosGetTimestampMs();
        newMnodeObj.updateTime = newMnodeObj.createdTime;
        newMnodeObj.role = TAOS_SYNC_ROLE_LEARNER;
        newMnodeObj.lastIndex = pMnode->applied;
        if (mndSetRestoreCreateMnodeRedoActions(pMnode, pTrans, pDnode, &newMnodeObj) != 0) goto _OVER;

        SMnodeObj mnodeLeaderObj = {0};
        mnodeLeaderObj.id = pDnode->id;
        mnodeLeaderObj.createdTime = taosGetTimestampMs();
        mnodeLeaderObj.updateTime = mnodeLeaderObj.createdTime;
        mnodeLeaderObj.role = TAOS_SYNC_ROLE_VOTER;
        mnodeLeaderObj.lastIndex = pMnode->applied + 1;
        if (mndSetRestoreAlterMnodeTypeRedoActions(pMnode, pTrans, pDnode, &mnodeLeaderObj) != 0) goto _OVER;

        if (mndSetCreateMnodeCommitLogs(pMnode, pTrans, &mnodeLeaderObj) != 0) goto _OVER;

        mndReleaseMnode(pMnode, mnodeObj);

        needRestore = true;
      }
    }
  }

  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL; 

  if(restoreType == RESTORE_TYPE__ALL || restoreType == RESTORE_TYPE__VNODE){
    while (1) {
      SVgObj *pVgroup = NULL;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;

      if (mndVgroupInDnode(pVgroup, pDnode->id)) {
        SDbObj *db = mndAcquireDb(pMnode, pVgroup->dbName);
        if(db == NULL){
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pVgroup);
          goto _OVER;
        }
        if (mndBuildRestoreAlterVgroupAction(pMnode, pTrans, db, pVgroup, pDnode) != 0) {
          sdbCancelFetch(pSdb, pIter);
          mndReleaseDb(pMnode, db);
          sdbRelease(pSdb, pVgroup);
          goto _OVER;
        }
        mndReleaseDb(pMnode, db);
        needRestore = true;
      }

      sdbRelease(pSdb, pVgroup);
    }
  }
  
  if(restoreType == RESTORE_TYPE__ALL || restoreType == RESTORE_TYPE__QNODE){
    SQnodeObj *pQnode = mndAcquireQnode(pMnode, pDnode->id);
    if(pQnode == NULL){
      terrno = TSDB_CODE_QNODE_NOT_FOUND;
      mError("trans:%d, no qnode exist on dnode:%s", pTrans->id, pDnode->ep);
    }
    else{
      if (mndSetCreateQnodeCommitLogs(pTrans, pQnode) != 0) goto _OVER;
      if (mndSetCreateQnodeRedoActions(pTrans, pDnode, pQnode) != 0) goto _OVER;

      mndReleaseQnode(pMnode, pQnode);

      needRestore = true;
    }
  }

  if(!needRestore) {
    if(restoreType == RESTORE_TYPE__ALL || restoreType == RESTORE_TYPE__VNODE) terrno = TSDB_CODE_MNODE_NO_NEED_RESTORE;
    goto _OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:

  mndTransDrop(pTrans);
  return code;
}

int32_t mndProcessRestoreDnodeReqImpl(SRpcMsg *pReq){
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
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DNODE) != 0) {
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

  if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    mError("dnode:%d, failed to restore since %s", pDnode->id, terrstr());
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