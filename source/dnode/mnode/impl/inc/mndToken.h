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

#ifndef _TD_MND_TOKEN_H_
#define _TD_MND_TOKEN_H_

#include "mndInt.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char name[TSDB_TOKEN_NAME_LEN];
  char user[TSDB_USER_LEN];
  int32_t expireTime; // in seconds
  int8_t enabled;
} SCachedTokenInfo;


int32_t mndInitToken(SMnode *pMnode);
void    mndCleanupToken(SMnode *pMnode);
int32_t mndAcquireToken(SMnode *pMnode, const char *token, STokenObj **ppToken);
void    mndReleaseToken(SMnode *pMnode, STokenObj *pToken);

int32_t mndDropTokensByUser(SMnode *pMnode, STrans* pTrans, const char* user);
void    mndDropCachedTokensByUser(const char *user);

// get the first active token for the user, return TSDB_CODE_MND_TOKEN_NOT_EXIST
// if no active token found, otherwise return 0.
// [token] should have enough space to hold the token string, i.e. TSDB_TOKEN_LEN.
int32_t mndGetUserActiveToken(const char* user, char* token);

int32_t mndTokenCacheRebuild(SMnode *pMnode);
SCachedTokenInfo* mndGetCachedTokenInfo(const char* token, SCachedTokenInfo* ti);

int32_t mndBuildSMCreateTokenResp(STrans *pTrans, void **ppResp, int32_t *pRespLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TOKEN_H_*/
