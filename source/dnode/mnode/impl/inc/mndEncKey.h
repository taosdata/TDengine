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

#ifndef _TD_MND_ENC_KEY_H_
#define _TD_MND_ENC_KEY_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitEncKey(SMnode *pMnode);
void    mndCleanupEncKey(SMnode *pMnode);

int32_t tSerializeSEncKeyObj(void *buf, int32_t bufLen, const SEncKeyObj *pObj);
int32_t tDeserializeSEncKeyObj(void *buf, int32_t bufLen, SEncKeyObj *pObj);

SSdbRaw* mndEncKeyActionEncode(SEncKeyObj *pCompact);
SSdbRow* mndEncKeyActionDecode(SSdbRaw *pRaw);

int32_t mndEncKeyActionInsert(SSdb *pSdb, SEncKeyObj *pCompact);
int32_t mndEncKeyActionDelete(SSdb *pSdb, SEncKeyObj *pCompact);
int32_t mndEncKeyActionUpdate(SSdb *pSdb, SEncKeyObj *pOldCompact, SEncKeyObj *pNewCompact);

int32_t mndRetrieveEncKey(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

int32_t mndProcessAKGenReq(SRpcMsg *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ENC_KEY_H_*/
