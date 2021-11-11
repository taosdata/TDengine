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

#ifndef _TD_TRANSACTION_H_
#define _TD_TRANSACTION_H_

#include "sdb.h"
#include "taosmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STrans STrans;
typedef enum { TRN_POLICY_ROLLBACK = 1, TRN_POLICY_RETRY = 2 } ETrnPolicy;

int32_t trnInit();
void    trnCleanup();

STrans *trnCreate(ETrnPolicy);
void    trnDrop(STrans *pTrans);
void    trnSetRpcHandle(STrans *pTrans, void *rpcHandle);
int32_t trnAppendRedoLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendUndoLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendCommitLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t trnAppendRedoAction(STrans *pTrans, SEpSet *, void *pMsg);
int32_t trnAppendUndoAction(STrans *pTrans, SEpSet *, void *pMsg);

int32_t trnPrepare(STrans *pTrans, int32_t (*syncfp)(SSdbRaw *pRaw, void *pData));
int32_t trnApply(SSdbRaw *pRaw, void *pData, int32_t code);
int32_t trnExecute(int32_t tranId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSACTION_H_*/
