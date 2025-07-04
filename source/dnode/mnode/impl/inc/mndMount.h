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

#ifndef _TD_MND_MOUNT_H_
#define _TD_MND_MOUNT_H_

#ifndef USE_MOUNT
#define USE_MOUNT
#endif
#ifdef USE_MOUNT

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitMount(SMnode *pMnode);
void    mndCleanupMount(SMnode *pMnode);
SDbObj *mndAcquireMount(SMnode *pMnode, const char *db);
void    mndReleaseMount(SMnode *pMnode, SDbObj *pDb);
int32_t mndValidateMountInfo(SMnode *pMnode, SDbCacheInfo *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen);
int32_t mndExtractMountInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq);
bool    mndIsMountReady(SMnode *pMnode, SDbObj *pDb);
bool    mndMountIsExist(SMnode *pMnode, const char *db);
void    mndBuildMountDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList);
void    mndMountFreeObj(SMountObj *pObj);
void    mndMountDestroyObj(SMountObj *pObj);
bool    mndHasMountOnDnode(SMnode *pMnode, int32_t dnodeId);

const char *mndGetMountStr(const char *src);

#ifdef __cplusplus
}
#endif

#endif

#endif /*_TD_MND_MOUNT_H_*/
