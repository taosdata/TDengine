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

#ifndef _TD_TRANSACTION_INT_H_
#define _TD_TRANSACTION_INT_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitTrans(SMnode *pMnode);
void    mndCleanupTrans(SMnode *pMnode);

STrans *trnCreate(ETrnPolicy policy, void *rpcHandle);
void    trnDrop(STrans *pTrans);
int32_t trnAppendRedoLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendUndoLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendCommitLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendRedoAction(STrans *pTrans, SEpSet *, void *pMsg);
int32_t trnAppendUndoAction(STrans *pTrans, SEpSet *, void *pMsg);

int32_t trnPrepare(STrans *pTrans, int32_t (*syncfp)(SSdbRaw *pRaw, void *pData));
int32_t trnApply(SSdbRaw *pRaw, void *pData, int32_t code);
int32_t trnExecute(int32_t tranId);

SSdbRaw *trnActionEncode(STrans *pTrans);
SSdbRow *trnActionDecode(SSdbRaw *pRaw);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSACTION_INT_H_*/
