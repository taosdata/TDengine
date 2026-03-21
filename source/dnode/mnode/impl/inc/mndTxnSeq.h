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

#ifndef _TD_MND_USER_TXN_SEQ__H_
#define _TD_MND_USER_TXN_SEQ__H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define UTXN_ID_RANGE_STEP 100

int32_t     mndInitTxnSeq(SMnode *pMnode);
void        mndCleanupTxnSeq(SMnode *pMnode);
int32_t     mndAcquireTxnSeq(SMnode *pMnode, int32_t id, STxnSeqObj **ppObj);
void        mndReleaseTxnSeq(SMnode *pMnode, STxnSeqObj *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN_SEQ__H_*/
