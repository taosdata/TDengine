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

int32_t   mndInitUser(SMnode *pMnode);
void      mndCleanupUser(SMnode *pMnode);
SUserObj *mndAcquireUser(SMnode *pMnode, const char *userName);
void      mndReleaseUser(SMnode *pMnode, SUserObj *pUser);

// for trans test
SSdbRaw  *mndUserActionEncode(SUserObj *pUser);
SHashObj *mndDupDbHash(SHashObj *pOld);
SHashObj *mndDupTableHash(SHashObj *pOld);
SHashObj *mndDupTopicHash(SHashObj *pOld);
int32_t   mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                  int32_t *pRspLen);
int32_t   mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, char *db);
int32_t   mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb);
int32_t   mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic);

int32_t   mndUserDupObj(SUserObj *pUser, SUserObj *pNew);
void      mndUserFreeObj(SUserObj *pUser);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_H_*/
