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

#ifndef _TD_MND_QNODE_H_
#define _TD_MND_QNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define QNODE_LOAD_VALUE(pQnode) (pQnode ? (pQnode->load.numOfQueryInQueue + pQnode->load.numOfFetchInQueue) : 0)

int32_t    mndInitQnode(SMnode *pMnode);
void       mndCleanupQnode(SMnode *pMnode);
SQnodeObj *mndAcquireQnode(SMnode *pMnode, int32_t qnodeId);
void       mndReleaseQnode(SMnode *pMnode, SQnodeObj *pObj);
int32_t    mndCreateQnodeList(SMnode *pMnode, SArray **pList, int32_t limit);
int32_t    mndSetDropQnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SQnodeObj *pObj, bool force);
bool       mndQnodeInDnode(SQnodeObj *pQnode, int32_t dnodeId);
int32_t    mndSetCreateQnodeCommitLogs(STrans *pTrans, SQnodeObj *pObj);
int32_t    mndSetCreateQnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SQnodeObj *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_QNODE_H_*/
