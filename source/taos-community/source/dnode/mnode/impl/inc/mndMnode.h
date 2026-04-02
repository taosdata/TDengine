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

#ifndef _TD_MND_MNODE_H_
#define _TD_MND_MNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t    mndInitMnode(SMnode *pMnode);
void       mndCleanupMnode(SMnode *pMnode);
SMnodeObj *mndAcquireMnode(SMnode *pMnode, int32_t mnodeId);
void       mndReleaseMnode(SMnode *pMnode, SMnodeObj *pObj);
bool       mndIsMnode(SMnode *pMnode, int32_t dnodeId);
void       mndGetMnodeEpSet(SMnode *pMnode, SEpSet *pEpSet);
int32_t    mndSetDropMnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj, bool force);
int32_t    mndSetRestoreCreateMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj);
int32_t    mndSetCreateMnodeCommitLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj);
int32_t    mndSetRestoreAlterMnodeTypeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj);
int32_t    mndSetRestoreCreateMnodeRedoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_MNODE_H_*/
