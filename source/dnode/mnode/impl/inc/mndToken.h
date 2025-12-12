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
  char user[TSDB_USER_LEN];
  int32_t expireTime; // in seconds
  int8_t enabled;
} SCachedTokenInfo;


int32_t mndInitToken(SMnode *pMnode);
void    mndCleanupToken(SMnode *pMnode);
int32_t mndAcquireToken(SMnode *pMnode, const char *token, STokenObj **ppToken);
void    mndReleaseToken(SMnode *pMnode, STokenObj *pToken);

int32_t mndTokenCacheRebuild(SMnode *pMnode);
char* mndTokenToUser(const char* token, char* user);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TOKEN_H_*/
