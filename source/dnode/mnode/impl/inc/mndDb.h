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

#ifndef _TD_MND_DATABASE_H_
#define _TD_MND_DATABASE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitDb(SMnode *pMnode);
void    mndCleanupDb(SMnode *pMnode);
SDbObj *mndAcquireDb(SMnode *pMnode, const char *db);
void    mndReleaseDb(SMnode *pMnode, SDbObj *pDb);
int32_t mndValidateDbInfo(SMnode *pMnode, SDbCacheInfo *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen);
int32_t mndExtractDbInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq);
int32_t mndCheckDbCfg(SMnode *pMnode, SDbCfg *pCfg);
int32_t mndCheckDbName(const char *dbName, SUserObj *pUser);
int32_t mndSetCreateDbPrepareAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndSetDropDbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndSetDropDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndSetDropDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
bool    mndIsDbReady(SMnode *pMnode, SDbObj *pDb);
void    mndBuildDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList);
bool    mndDbIsExist(SMnode *pMnode, const char *db);

SSdbRaw    *mndDbActionEncode(SDbObj *pDb);
const char *mndGetDbStr(const char *src);
const char *mndGetStableStr(const char *src);

int32_t mndProcessCompactDbReq(SRpcMsg *pReq);
int32_t mndCheckDbDnodeList(SMnode *pMnode, char *db, char *dnodeListStr, SArray *dnodeList);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DATABASE_H_*/
