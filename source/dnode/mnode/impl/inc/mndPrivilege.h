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

bool mndMustChangePassword(SUserObj* pUser);
int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, EOperType operType);
int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb);
int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *dbname);
int32_t mndCheckStbPrivilege(SMnode *pMnode, SUserObj *pUser, EOperType operType, SStbObj *pStb);
int32_t mndCheckViewPrivilege(SMnode *pMnode, const char *user, EOperType operType, const char *pViewFName);
int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, EOperType operType, SMqTopicObj *pTopic);
int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, EShowType showType, const char *dbname);
int32_t mndCheckAlterUserPrivilege(SMnode* pMnode, const char *opUser, SUserObj *pUser, SAlterUserReq *pAlter);
int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp);
int32_t mndSetUserIpWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp);
int32_t mndSetUserIpWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp);
int32_t mndEnableIpWhiteList(SMnode *pMnode);
int32_t mndFetchIpWhiteList(SIpWhiteList *ipList, char **buf);
int32_t mndEnableTimeWhiteList(SMnode *pMnode);
int32_t mndSetUserDateTimeWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SUserDateTimeWhiteList *pWhiteListRsp);
int32_t mndCheckTokenPrivilege(SUserObj* pOperUser, const char *user);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_PRIVILEGE_H*/
