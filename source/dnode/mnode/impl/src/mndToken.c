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

#include "audit.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndToken.h"

#ifndef TD_ENTERPRISE

int32_t mndGetUserActiveToken(const char* user, char* token) {
  return TSDB_CODE_MND_TOKEN_NOT_EXIST;
}



int32_t mndTokenCacheRebuild(SMnode *pMnode) {
  return TSDB_CODE_SUCCESS;
}



SCachedTokenInfo* mndGetCachedTokenInfo(const char* token, SCachedTokenInfo* ti) {
  return NULL;
}



void mndDropCachedTokensByUser(const char* user) {
}



static int32_t mndProcessCreateTokenReq(SRpcMsg *pReq) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



static int32_t mndProcessAlterTokenReq(SRpcMsg *pReq) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



static int32_t mndProcessDropTokenReq(SRpcMsg *pReq) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



int32_t mndDropTokensByUser(SMnode *pMnode, STrans *pTrans, const char *user) {
  return TSDB_CODE_SUCCESS;
}



static int32_t mndRetrieveTokens(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  return TSDB_CODE_SUCCESS;
}



static void mndCancelGetNextToken(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_TOKEN);
}



int32_t mndInitToken(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOKEN, mndProcessCreateTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_TOKEN, mndProcessAlterTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOKEN, mndProcessDropTokenReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndRetrieveTokens);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndCancelGetNextToken);
  return TSDB_CODE_SUCCESS;
}



void mndCleanupToken(SMnode *pMnode) {
}



int32_t mndAcquireToken(SMnode *pMnode, const char *token, STokenObj **ppToken) {
  TAOS_RETURN(TSDB_CODE_MND_TOKEN_NOT_EXIST);
}



void mndReleaseToken(SMnode *pMnode, STokenObj *pToken) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pToken);
}



int32_t mndBuildSMCreateTokenResp(STrans *pTrans, void **ppResp, int32_t *pRespLen) {
  return 0;
}

#endif // TD_ENTERPRISE