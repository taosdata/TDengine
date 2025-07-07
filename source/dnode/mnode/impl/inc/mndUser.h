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

enum {
  IP_WHITE_ADD,
  IP_WHITE_DROP,
};
int32_t mndInitUser(SMnode *pMnode);
void    mndCleanupUser(SMnode *pMnode);
int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser);
void    mndReleaseUser(SMnode *pMnode, SUserObj *pUser);
int32_t mndEncryptPass(char *pass, int8_t *algo);

// for trans test
SSdbRaw *mndUserActionEncode(SUserObj *pUser);
int32_t  mndDupDbHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupTableHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndDupTopicHash(SHashObj *pOld, SHashObj **ppNew);
int32_t  mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                 int32_t *pRspLen, int64_t ipWhiteListVer);
int32_t  mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, char *db);
int32_t  mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb);
int32_t  mndUserRemoveView(SMnode *pMnode, STrans *pTrans, char *view);
int32_t  mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic);

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew);
void    mndUserFreeObj(SUserObj *pUser);

int64_t mndGetIpWhiteVer(SMnode *pMnode);

int32_t mndUpdateIpWhiteForAllUser(SMnode *pMnode, char *user, char *fqdn, int8_t type, int8_t lock);

int32_t mndRefreshUserIpWhiteList(SMnode *pMnode);

int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_H_*/
