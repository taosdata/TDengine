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

#ifndef _TD_MND_S3MIGRATE_H_
#define _TD_MND_S3MIGRATE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitS3Migrate(SMnode *pMnode);
void    mndCleanupS3Migrate(SMnode *pMnode);

void    tFreeS3MigrateObj(SS3MigrateObj *pS3Migrate);
int32_t tSerializeSS3MigrateObj(void *buf, int32_t bufLen, const SS3MigrateObj *pObj);
int32_t tDeserializeSS3MigrateObj(void *buf, int32_t bufLen, SS3MigrateObj *pObj);

SSdbRaw* mndS3MigrateActionEncode(SS3MigrateObj *pS3Migrate);
SSdbRow* mndS3MigrateActionDecode(SSdbRaw *pRaw);

int32_t mndS3MigrateActionInsert(SSdb *pSdb, SS3MigrateObj *pS3Migrate);
int32_t mndS3MigrateActionDelete(SSdb *pSdb, SS3MigrateObj *pS3Migrate);
int32_t mndS3MigrateActionUpdate(SSdb *pSdb, SS3MigrateObj *pOldS3Migrate, SS3MigrateObj *pNewS3Migrate);

int32_t mndAddS3MigrateToTran(SMnode *pMnode, STrans *pTrans, SS3MigrateObj* pS3Migrate, SDbObj *pDb);

int32_t mndRetrieveS3Migrate(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

int32_t mndProcessKillS3MigrateReq(SRpcMsg *pReq);

int32_t mndProcessQueryS3MigrateRsp(SRpcMsg *pReq);

SS3MigrateObj *mndAcquireS3Migrate(SMnode *pMnode, int64_t compactId);
void mndReleaseS3Migrate(SMnode *pMnode, SS3MigrateObj *pS3Migrate);

int32_t mndS3MigrateGetDbName(SMnode *pMnode, int32_t compactId, char *dbname, int32_t len);
void mndS3MigrateSendProgressReq(SMnode *pMnode, SS3MigrateObj *pS3Migrate);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_S3MIGRATE_H_*/

