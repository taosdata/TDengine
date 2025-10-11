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

#ifndef _TD_MND_COMPACT_H_
#define _TD_MND_COMPACT_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitCompact(SMnode *pMnode);
void    mndCleanupCompact(SMnode *pMnode);

void    tFreeCompactObj(SCompactObj *pCompact);
int32_t tSerializeSCompactObj(void *buf, int32_t bufLen, const SCompactObj *pObj);
int32_t tDeserializeSCompactObj(void *buf, int32_t bufLen, SCompactObj *pObj);

SSdbRaw* mndCompactActionEncode(SCompactObj *pCompact);
SSdbRow* mndCompactActionDecode(SSdbRaw *pRaw);

int32_t mndCompactActionInsert(SSdb *pSdb, SCompactObj *pCompact);
int32_t mndCompactActionDelete(SSdb *pSdb, SCompactObj *pCompact);
int32_t mndCompactActionUpdate(SSdb *pSdb, SCompactObj *pOldCompact, SCompactObj *pNewCompact);

int32_t mndAddCompactToTran(SMnode *pMnode, STrans *pTrans, SCompactObj* pCompact, SDbObj *pDb, SCompactDbRsp *rsp);

int32_t mndRetrieveCompact(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

int32_t mndProcessKillCompactReq(SRpcMsg *pReq);

int32_t mndProcessQueryCompactRsp(SRpcMsg *pReq);

SCompactObj *mndAcquireCompact(SMnode *pMnode, int64_t compactId);
void mndReleaseCompact(SMnode *pMnode, SCompactObj *pCompact);

void mndCompactSendProgressReq(SMnode *pMnode, SCompactObj *pCompact);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_COMPACT_H_*/
