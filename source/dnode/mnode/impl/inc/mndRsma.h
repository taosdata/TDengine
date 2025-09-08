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

#ifndef _TD_MND_RSMA_H_
#define _TD_MND_RSMA_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t   mndInitRsma(SMnode *pMnode);
void      mndCleanupRsma(SMnode *pMnode);
SRsmaObj *mndAcquireRsma(SMnode *pMnode, char *name);
void      mndReleaseRsma(SMnode *pMnode, SRsmaObj *pSma);
void      mndRsmaFreeObj(SRsmaObj *pObj);
int32_t   mndDropRsmasByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t   mndDropRsmaByStb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb);
int32_t   mndValidateRsmaInfo(SMnode *pMnode, STSMAVersion *pTsmaVersions, int32_t numOfTsmas, void **ppRsp,
                              int32_t *pRspLen);
int32_t   mndDropRsmasByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t   mndGetDbRsmas(SMnode *pMnode, const char *dbFName, uint64_t dbUid, STableTSMAInfoRsp *pRsp, bool *exist);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SMA_H_*/
