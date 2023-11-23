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

#ifndef _TD_MND_COMPACT_DETAIL_H_
#define _TD_MND_COMPACT_DETAIL_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitCompactDetail(SMnode *pMnode);
void    mndCleanupCompactDetail(SMnode *pMnode);

void    tFreeCompactDetailObj(SCompactDetailObj *pCompact);
int32_t tSerializeSCompactDetailObj(void *buf, int32_t bufLen, const SCompactDetailObj *pObj);
int32_t tDeserializeSCompactDetailObj(void *buf, int32_t bufLen, SCompactDetailObj *pObj);

SSdbRaw* mndCompactDetailActionEncode(SCompactDetailObj *pCompact);
SSdbRow* mndCompactDetailActionDecode(SSdbRaw *pRaw);

int32_t mndCompactDetailActionInsert(SSdb *pSdb, SCompactDetailObj *pCompact);
int32_t mndCompactDetailActionDelete(SSdb *pSdb, SCompactDetailObj *pCompact);
int32_t mndCompactDetailActionUpdate(SSdb *pSdb, SCompactDetailObj *pOldCompact, SCompactDetailObj *pNewCompact);

int32_t mndAddCompactDetailToTran(SMnode *pMnode, STrans *pTrans, SCompactObj* pCompact, SVgObj *pVgroup, 
                                    SVnodeGid *pVgid, int32_t index);

int32_t mndRetrieveCompactDetail(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_COMPACT_DETAIL_H_*/
