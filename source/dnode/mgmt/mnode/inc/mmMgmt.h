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

#ifndef _TD_DND_MNODE_MGMT_H_
#define _TD_DND_MNODE_MGMT_H_

#include "mmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

SMnode *mmAcquire(SDnode *pDnode);
void    mmRelease(SDnode *pDnode, SMnode *pMnode);
int32_t mmOpen(SDnode *pDnode, SMnodeOpt *pOption);
int32_t mmAlter(SDnode *pDnode, SMnodeOpt *pOption);
int32_t mmDrop(SDnode *pDnode);
int32_t mmBuildOptionFromReq(SDnode *pDnode, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_MGMT_H_*/