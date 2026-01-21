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

#ifndef _TD_MND_USER_H_
#define _TD_MND_USER_H_

#include "mndInt.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif


int32_t mndInitUser(SMnode *pMnode);
void    mndCleanupUser(SMnode *pMnode);
int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser);
int32_t mndAcquireUserById(SMnode *pMnode, int64_t userId, SUserObj **ppUser);
int32_t mndBuildUidNamesHash(SMnode *pMnode, SSHashObj **ppHash);
void    mndReleaseUser(SMnode *pMnode, SUserObj *pUser);

int32_t mndEncryptPass(char *pass, const char* salt, int8_t *algo);

// for trans test
SSdbRaw *mndUserActionEncode(SUserObj *pUser);
int32_t  mndDupDbHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupKVHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupTableHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupTopicHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupRoleHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupPrivObjHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupPrivTblHash(SHashObj *pOld, SHashObj **ppNew, bool setUpdateTimeMax);
int32_t  mndMergePrivObjHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndMergePrivTblHash(SHashObj *pOld, SHashObj **ppNew, bool updateWithLatest);
int32_t  mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                 int32_t *pRspLen, int64_t ipWhiteListVer);
int32_t  mndPrincipalRemoveDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SSHashObj **ppUsers, SSHashObj **ppRoles);
int32_t  mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb);
int32_t  mndUserRemoveView(SMnode *pMnode, STrans *pTrans, char *view);
int32_t  mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic);
int32_t  mndUserDropRole(SMnode *pMnode, STrans *pTrans, SRoleObj *pObj);

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew);
void    mndUserFreeObj(SUserObj *pUser);

int64_t mndGetIpWhiteListVersion(SMnode *pMnode);
int32_t mndRefreshUserIpWhiteList(SMnode *pMnode);
int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser);

int64_t mndGetTimeWhiteListVersion(SMnode *pMnode);
int32_t mndRefreshUserDateTimeWhiteList(SMnode *pMnode);
int64_t mndGetUserTimeWhiteListVer(SMnode *pMnode, SUserObj *pUser);

int32_t mndGetAuditUser(SMnode *pMnode, char *user);
int32_t mndResetAuditLogUser(SMnode *pMnode, const char *user, bool isAdd);

void mndGetUserLoginInfo(const char *user, SLoginInfo *pLoginInfo);
void mndSetUserLoginInfo(const char *user, const SLoginInfo *pLoginInfo);
bool mndIsTotpEnabledUser(SUserObj *pUser);

int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser);
int32_t mndAlterUserFromRole(SRpcMsg *pReq, SUserObj *pOperUser, SAlterRoleReq *pAlterReq);

int32_t mndBuildSMCreateTotpSecretResp(STrans *pTrans, void **ppResp, int32_t *pRespLen);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_H_*/
