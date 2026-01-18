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

#ifndef _TD_MND_PRIVILEGE_H
#define _TD_MND_PRIVILEGE_H

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitPrivilege(SMnode *pMnode);
void    mndCleanupPrivilege(SMnode *pMnode);

int32_t mndCheckConnectPrivilege(SMnode *pMnode, SUserObj *pUser, const char* token, const SLoginInfo *pLoginInfo);
int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType);
int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, SDbObj *pDb);
int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, const char* token, EOperType operType, const char *dbname);
int32_t mndCheckStbPrivilege(SMnode *pMnode, SUserObj *pUser, const char* token, EOperType operType, SStbObj *pStb);
int32_t mndCheckViewPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, const char *pViewFName);
int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, SMqTopicObj *pTopic);
int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, const char* token, EShowType showType, const char *dbname);
int32_t mndCheckAlterUserPrivilege(SMnode* pMnode, const char *opUser, const char* opToken, SUserObj *pUser, SAlterUserReq *pAlter);
int32_t mndCheckTotpSecretPrivilege(SMnode* pMnode, const char *opUser, const char* opToken, SUserObj *pUser);
int32_t mndCheckTokenPrivilege(SMnode *pMnode, const char *opUser, const char *opToken, const char *user,
                               const char *token, EPrivType privType);

int32_t mndCheckSysObjPrivilege(SMnode *pMnode, SUserObj *pUser, const char *token, EPrivType privType,
                                EPrivObjType objType, int64_t ownerId, const char *objFName, const char *tbName);
int32_t mndCheckObjPrivilegeRec(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                int64_t ownerId, int32_t acctId, const char *objName, const char *tbName);
int32_t mndCheckObjPrivilegeRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                 int64_t ownerId, const char *objFName, const char *tbName);
int32_t mndCheckDbPrivilegeByNameRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                      const char *objFName, const char *tbName);

int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp);
int32_t mndSetUserIpWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp);
int32_t mndSetUserIpWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp);
int32_t mndEnableIpWhiteList(SMnode *pMnode);
int32_t mndFetchIpWhiteList(SIpWhiteList *ipList, char **buf);
int32_t mndEnableTimeWhiteList(SMnode *pMnode);
int32_t mndSetUserDateTimeWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SUserDateTimeWhiteList *pWhiteListRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_PRIVILEGE_H*/
