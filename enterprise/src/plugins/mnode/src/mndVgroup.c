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
#include "mndDnode.h"

extern int32_t mndAddVgroupBalanceToTrans(SMnode *pMnode, SVgObj *pVgroup, STrans *pTrans);
extern int32_t mndAddAlterVnodeConfigAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup);
extern int32_t mndCheckDnodeMemory(SMnode *pMnode, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pOldVgroup,
                                   SVgObj *pNewVgroup, SArray *pArray);
extern int32_t mndAddVnodeToVgroup(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SArray *pArray);
extern int32_t mndAddAlterVnodeReplicaAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId);
extern void *mndBuildAlterVnodeReplicaReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId,
                                          int32_t *pContLen);
extern int32_t mndRemoveVnodeFromVgroup(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SArray *pArray,
                                        SVnodeGid *pDelVgid);

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
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "bal-vg-leader");
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

int32_t mndAddAlterVnodeTypeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId) {
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) return -1;

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReplicaReq(pMnode, pDb, pVgroup, dnodeId, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_ALTER_VNODE_TYPE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_IS_VOTER;
  action.retryCode = TSDB_CODE_VND_NOT_CATCH_UP;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndBuildAlterVgroupActionImpl(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pVgroup,
                                  SArray *pArray) {
  SVgObj newVgroup = {0};
  memcpy(&newVgroup, pVgroup, sizeof(SVgObj));

  if (pVgroup->replica <= 0 || pVgroup->replica == pNewDb->cfg.replications) {
    if (mndAddAlterVnodeConfigAction(pMnode, pTrans, pNewDb, pVgroup) != 0) return -1;
    if (mndCheckDnodeMemory(pMnode, pOldDb, pNewDb, &newVgroup, pVgroup, pArray) != 0) return -1;
    return 0;
  }

  mndTransSetSerial(pTrans);

  if (newVgroup.replica == 1 && pNewDb->cfg.replications == 3) {
    mInfo("db:%s, vgId:%d, will add 2 vnodes, vn:0 dnode:%d", pVgroup->dbName, pVgroup->vgId,
          pVgroup->vnodeGid[0].dnodeId);

    //add second
    if (mndAddVnodeToVgroup(pMnode, pTrans, &newVgroup, pArray) != 0) return -1;

    //learner stage
    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[0].dnodeId) != 0)
      return -1;

    if (mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &newVgroup.vnodeGid[1]) != 0) return -1;

    //follower stage
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    if (mndAddAlterVnodeTypeAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[1].dnodeId) != 0)
      return -1;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[0].dnodeId) != 0)
      return -1;

    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup) != 0) return -1;

    //add third
    if (mndAddVnodeToVgroup(pMnode, pTrans, &newVgroup, pArray) != 0) return -1;

    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_VOTER;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[0].dnodeId) != 0)
      return -1;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[1].dnodeId) != 0)
      return -1;
    if (mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &newVgroup.vnodeGid[2]) != 0) return -1;

    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup) != 0) return -1;
  } else if (newVgroup.replica == 3 && pNewDb->cfg.replications == 1) {
    mInfo("db:%s, vgId:%d, will remove 2 vnodes, vn:0 dnode:%d vn:1 dnode:%d vn:2 dnode:%d", pVgroup->dbName,
          pVgroup->vgId, pVgroup->vnodeGid[0].dnodeId, pVgroup->vnodeGid[1].dnodeId, pVgroup->vnodeGid[2].dnodeId);

    SVnodeGid del1 = {0};
    SVnodeGid del2 = {0};
    if (mndRemoveVnodeFromVgroup(pMnode, pTrans, &newVgroup, pArray, &del1) != 0) return -1;
    if (mndAddDropVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &del1, true) != 0) return -1;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[0].dnodeId) != 0)
      return -1;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[1].dnodeId) != 0)
      return -1;
    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup) != 0) return -1;

    if (mndRemoveVnodeFromVgroup(pMnode, pTrans, &newVgroup, pArray, &del2) != 0) return -1;
    if (mndAddDropVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &del2, true) != 0) return -1;
    if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[0].dnodeId) != 0)
      return -1;
    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup) != 0) return -1;
  } else {
    return -1;
  }

  mndSortVnodeGid(&newVgroup);

  {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
      sdbFreeRaw(pVgRaw);
      return -1;
    }
    (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
  }

  return 0;
}