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

#ifndef _TD_MND_VGROUP_H_
#define _TD_MND_VGROUP_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t  mndInitVgroup(SMnode *pMnode);
void     mndCleanupVgroup(SMnode *pMnode);
SVgObj  *mndAcquireVgroup(SMnode *pMnode, int32_t vgId);
void     mndReleaseVgroup(SMnode *pMnode, SVgObj *pVgroup);
SSdbRaw *mndVgroupActionEncode(SVgObj *pVgroup);
SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw);
SEpSet   mndGetVgroupEpset(SMnode *pMnode, const SVgObj *pVgroup);
int32_t  mndGetVnodesNum(SMnode *pMnode, int32_t dnodeId);
void     mndSortVnodeGid(SVgObj *pVgroup);
int64_t  mndGetVnodesMemory(SMnode *pMnode, int32_t dnodeId);
int64_t  mndGetVgroupMemory(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup);

SArray *mndBuildDnodesArray(SMnode *, int32_t exceptDnodeId);
int32_t mndAllocSmaVgroup(SMnode *, SDbObj *pDb, SVgObj *pVgroup);
int32_t mndAllocVgroup(SMnode *, SDbObj *pDb, SVgObj **ppVgroups);
int32_t mndAddNewVgPrepareAction(SMnode *, STrans *pTrans, SVgObj *pVg);
int32_t mndAddCreateVnodeAction(SMnode *, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid);
int32_t mndAddAlterVnodeConfirmAction(SMnode *, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup);
int32_t mndAddAlterVnodeAction(SMnode *, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, tmsg_t msgType);
int32_t mndAddDropVnodeAction(SMnode *, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid, bool isRedo);
int32_t mndSetMoveVgroupsInfoToTrans(SMnode *, STrans *pTrans, int32_t dropDnodeId, bool force, bool unsafe);
int32_t mndBuildAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pVgroup,
                                  SArray *pArray);
int32_t mndBuildCompactVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int64_t compactTs,
                                    STimeWindow tw);
int32_t mndBuildRaftAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pVgroup,
                                  SArray *pArray);

void *mndBuildCreateVnodeReq(SMnode *, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen);
void *mndBuildDropVnodeReq(SMnode *, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen);
bool  mndVgroupInDb(SVgObj *pVgroup, int64_t dbUid);
bool  mndVgroupInDnode(SVgObj *pVgroup, int32_t dnodeId);
int32_t mndBuildRestoreAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *db, SVgObj *pVgroup, 
                                        SDnodeObj *pDnode);

int32_t mndSplitVgroup(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SVgObj *pVgroup);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_VGROUP_H_*/
