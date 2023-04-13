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

#define _DEFAULT_SOURCE
#include "mndPrivilege.h"
#include "mndDb.h"
#include "mndUser.h"

#ifndef _PRIVILEGE
int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }
void    mndCleanupPrivilege(SMnode *pMnode) {}
int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, EOperType operType) { return 0; }
int32_t mndCheckAlterUserPrivilege(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter) { return 0; }
int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, EShowType showType, const char *dbname) { return 0; }
int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb) { return 0; }
int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *dbname) {
  return 0;
}
int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, EOperType operType, SMqTopicObj *pTopic) { return 0; }
int32_t mndCheckTopicPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *topicName) {
  return 0;
}
int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  memcpy(pRsp->user, pUser->user, TSDB_USER_LEN);
  pRsp->superAuth = 1;
  pRsp->enable = pUser->enable;
  pRsp->version = pUser->authVersion;
  return 0;
}
#endif