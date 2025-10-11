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

#ifndef _TD_MND_RETENTION_H_
#define _TD_MND_RETENTION_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t  mndInitRetention(SMnode *pMnode);
void     mndCleanupRetention(SMnode *pMnode);
void     tFreeRetentionObj(SRetentionObj *pRetention);
int32_t  tSerializeSRetentionObj(void *buf, int32_t bufLen, const SRetentionObj *pObj);
int32_t  tDeserializeSRetentionObj(void *buf, int32_t bufLen, SRetentionObj *pObj);
SSdbRaw *mndRetentionActionEncode(SRetentionObj *pRetention);
SSdbRow *mndRetentionActionDecode(SSdbRaw *pRaw);
int32_t  mndRetentionActionInsert(SSdb *pSdb, SRetentionObj *pRetention);
int32_t  mndRetentionActionDelete(SSdb *pSdb, SRetentionObj *pRetention);
int32_t  mndRetentionActionUpdate(SSdb *pSdb, SRetentionObj *pOldRetention, SRetentionObj *pNewRetention);
int32_t mndAddRetentionToTrans(SMnode *pMnode, STrans *pTrans, SRetentionObj *pRetention, SDbObj *pDb, STrimDbRsp *rsp);
int32_t mndProcessKillRetentionReq(SRpcMsg *pReq);
int32_t mndProcessQueryRetentionRsp(SRpcMsg *pReq);

SRetentionObj *mndAcquireRetention(SMnode *pMnode, int32_t id);
void           mndReleaseRetention(SMnode *pMnode, SRetentionObj *pRetention);

void    mndRetentionSendProgressReq(SMnode *pMnode, SRetentionObj *pRetention);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_RETENTION_H_*/
