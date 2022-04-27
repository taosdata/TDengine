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
int32_t  mndAllocVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj **ppVgroups);
SEpSet   mndGetVgroupEpset(SMnode *pMnode, const SVgObj *pVgroup);
int32_t  mndGetVnodesNum(SMnode *pMnode, int32_t dnodeId);

void *mndBuildCreateVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen);
void *mndBuildDropVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_VGROUP_H_*/
