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

#ifndef _TD_MND_ROLE_H_
#define _TD_MND_ROLE_H_

#include "mndInt.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int64_t lastUpd;  // Last update time of role, unit is ms, default value is INT64_MAX. If user's
                    // lastRoleRetrieve time < lastUpd, need to retrieve role again.
  TdThreadRwlock rw;
} SRoleMgmt;

int32_t mndInitRole(SMnode *pMnode);
void    mndCleanupRole(SMnode *pMnode);
int32_t mndAcquireRole(SMnode *pMnode, const char *roleName, SRoleObj **ppRole);
void    mndReleaseRole(SMnode *pMnode, SRoleObj *pRole);
void    mndRoleFreeObj(SRoleObj *pObj);
int32_t mndRoleDupObj(SRoleObj *pOld, SRoleObj *pNew);
int32_t mndRoleDropParentUser(SMnode *pMnode, STrans *pTrans, SUserObj *pObj);
int32_t mndRoleGrantToUser(SMnode *pMnode, STrans *pTrans, SRoleObj *pRole, SUserObj *pUser);
int64_t mndGetRoleLastUpd();
void    mndSetRoleLastUpd(int64_t updateTime);
bool    mndNeedRetrieveRole(SUserObj *pUser);

SSdbRaw *mndRoleActionEncode(SRoleObj *pRole);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ROLE_H_*/
