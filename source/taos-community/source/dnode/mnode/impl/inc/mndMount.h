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

#ifdef USE_MOUNT

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  SVgObj  vg;
  SDbObj *pDb;
  int32_t diskPrimary;
  int64_t committed;
  int64_t commitID;
  int64_t commitTerm;
  int64_t numOfSTables;
  int64_t numOfCTables;
  int64_t numOfNTables;
} SMountVgObj;

int32_t       mndInitMountLog(SMnode *pMnode);
void          mndCleanupMountLog(SMnode *pMnode);
SMountLogObj *mndAcquireMountLog(SMnode *pMnode);
void          mndReleaseMountLog(SMnode *pMnode, SMountLogObj *pObj);
int32_t       mndInitMount(SMnode *pMnode);
void          mndCleanupMount(SMnode *pMnode);
SMountObj    *mndAcquireMount(SMnode *pMnode, const char *mountName);
void          mndReleaseMount(SMnode *pMnode, SMountObj *pObj);
SSdbRaw      *mndMountActionEncode(SMountObj *pObj);
SSdbRow      *mndMountActionDecode(SSdbRaw *pRaw);
int32_t mndValidateMountInfo(SMnode *pMnode, SDbCacheInfo *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen);
int32_t mndExtractMountInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq);
bool    mndIsMountReady(SMnode *pMnode, SDbObj *pDb);
bool    mndMountIsExist(SMnode *pMnode, const char *db);
void    mndBuildMountDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList);
void    mndMountFreeObj(SMountObj *pObj);
void    mndMountDestroyObj(SMountObj *pObj);
bool    mndHasMountOnDnode(SMnode *pMnode, int32_t dnodeId);
int32_t mndBuildDropMountRsp(SMountObj *pObj, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc);
int32_t mndCreateMount(SMnode *pMnode, SRpcMsg *pReq, SMountInfo *pInfo, SUserObj *pUser);
int32_t mndDropMount(SMnode *pMnode, SRpcMsg *pReq, SMountObj *pObj);

#ifdef __cplusplus
}
#endif

#endif

#endif /*_TD_MND_MOUNT_H_*/
