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

#ifndef _TD_MND_SNODE_H_
#define _TD_MND_SNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REPLACE_LEADER_ID(_obj, _o, _s) do {      \
    if ((_obj)->leadersId[0] == (_o)) {           \
      ((_obj)->leadersId[0] = (_s));              \
    } else if ((_obj)->leadersId[1] == (_o)) {    \
      ((_obj)->leadersId[1] = (_s));              \
    }                                             \
  } while (0)
  
int32_t    mndInitSnode(SMnode *pMnode);
void       mndCleanupSnode(SMnode *pMnode);
SSnodeObj *mndAcquireSnode(SMnode *pMnode, int32_t qnodeId);
void       mndReleaseSnode(SMnode *pMnode, SSnodeObj *pObj);
SEpSet     mndAcquireEpFromSnode(SMnode *pMnode, const SSnodeObj *pSnode);
int32_t    mndSetDropSnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SSnodeObj *pObj, bool force);
int32_t    mndDropSnodeImpl(SMnode *pMnode, SRpcMsg *pReq, SSnodeObj *pObj, STrans *pTrans);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SNODE_H_*/
