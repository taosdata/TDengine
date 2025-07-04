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

#ifndef _TD_MND_XNODE_H_
#define _TD_MND_XNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitXnode(SMnode* pMnode);
void    mndCleanupXnode(SMnode* pMnode);
void    mndRetrieveTasks(SMnode* pMnode, SArray* pFc, SArray* pAd);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ANODE_H_*/
