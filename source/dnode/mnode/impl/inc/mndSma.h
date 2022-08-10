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

#ifndef _TD_MND_SMA_H_
#define _TD_MND_SMA_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t  mndInitSma(SMnode *pMnode);
void     mndCleanupSma(SMnode *pMnode);
SSmaObj *mndAcquireSma(SMnode *pMnode, char *smaName);
void     mndReleaseSma(SMnode *pMnode, SSmaObj *pSma);
int32_t  mndDropSmasByStb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb);
int32_t  mndDropSmasByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t  mndGetTableSma(SMnode *pMnode, char *tbFName, STableIndexRsp *rsp, bool *exist);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SMA_H_*/
