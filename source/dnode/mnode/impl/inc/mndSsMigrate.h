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

#ifndef _TD_MND_SSMIGRATE_H_
#define _TD_MND_SSMIGRATE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitSsMigrate(SMnode *pMnode);
void    mndCleanupSsMigrate(SMnode *pMnode);

void    tFreeSsMigrateObj(SSsMigrateObj *pSsMigrate);
int32_t tSerializeSSsMigrateObj(void *buf, int32_t bufLen, const SSsMigrateObj *pObj);
int32_t tDeserializeSSsMigrateObj(void *buf, int32_t bufLen, SSsMigrateObj *pObj);

SSdbRaw* mndSsMigrateActionEncode(SSsMigrateObj *pSsMigrate);
SSdbRow* mndSsMigrateActionDecode(SSdbRaw *pRaw);

int32_t mndSsMigrateActionInsert(SSdb *pSdb, SSsMigrateObj *pSsMigrate);
int32_t mndSsMigrateActionDelete(SSdb *pSdb, SSsMigrateObj *pSsMigrate);
int32_t mndSsMigrateActionUpdate(SSdb *pSdb, SSsMigrateObj *pOldSsMigrate, SSsMigrateObj *pNewSsMigrate);

int32_t mndAddSsMigrateToTran(SMnode *pMnode, STrans *pTrans, SSsMigrateObj* pSsMigrate, SDbObj *pDb);

int32_t mndRetrieveSsMigrate(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

int32_t mndProcessKillSsMigrateReq(SRpcMsg *pReq);

int32_t mndProcessQuerySsMigrateRsp(SRpcMsg *pReq);

SSsMigrateObj *mndAcquireSsMigrate(SMnode *pMnode, int64_t compactId);
void mndReleaseSsMigrate(SMnode *pMnode, SSsMigrateObj *pSsMigrate);

int32_t mndSsMigrateGetDbName(SMnode *pMnode, int32_t compactId, char *dbname, int32_t len);
void mndSsMigrateSendProgressReq(SMnode *pMnode, SSsMigrateObj *pSsMigrate);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SSMIGRATE_H_*/

