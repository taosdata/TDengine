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

#ifndef _TD_MND_ARBGROUP_H_
#define _TD_MND_ARBGROUP_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t    mndInitArbGroup(SMnode *pMnode);
void       mndCleanupArbGroup(SMnode *pMnode);
SArbGroup *mndAcquireArbGroup(SMnode *pMnode, int32_t vgId);
void       mndReleaseArbGroup(SMnode *pMnode, SArbGroup *pObj);
SSdbRaw   *mndArbGroupActionEncode(SArbGroup *pGroup);
SSdbRow   *mndArbGroupActionDecode(SSdbRaw *pRaw);

int32_t mndArbGroupInitFromVgObj(SVgObj *pVgObj, SArbGroup *outGroup);

int32_t mndSetCreateArbGroupRedoLogs(STrans *pTrans, SArbGroup *pGroup);
int32_t mndSetCreateArbGroupUndoLogs(STrans *pTrans, SArbGroup *pGroup);
int32_t mndSetCreateArbGroupCommitLogs(STrans *pTrans, SArbGroup *pGroup);

int32_t mndSetDropArbGroupPrepareLogs(STrans *pTrans, SArbGroup *pGroup);
int32_t mndSetDropArbGroupCommitLogs(STrans *pTrans, SArbGroup *pGroup);

bool mndUpdateArbGroupByHeartBeat(SArbGroup *pGroup, SVArbHbRspMember *pRspMember, int64_t nowMs, int32_t dnodeId,
                                  SArbGroup *pNewGroup);
bool mndUpdateArbGroupByCheckSync(SArbGroup *pGroup, int32_t vgId, char *member0Token, char *member1Token,
                                  bool newIsSync, SArbGroup *pNewGroup);
bool mndUpdateArbGroupBySetAssignedLeader(SArbGroup *pGroup, int32_t vgId, char *memberToken, int32_t errcode,
                                          SArbGroup *pNewGroup);

int32_t mndGetArbGroupSize(SMnode *pMnode);

typedef enum {
  CHECK_SYNC_NONE = 0,
  CHECK_SYNC_SET_ASSIGNED_LEADER = 1,
  CHECK_SYNC_CHECK_SYNC = 2,
  CHECK_SYNC_UPDATE = 3
} ECheckSyncOp;

void mndArbCheckSync(SArbGroup *pArbGroup, int64_t nowMs, ECheckSyncOp *pOp, SArbGroup *pNewGroup);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ARBGROUP_H_*/
