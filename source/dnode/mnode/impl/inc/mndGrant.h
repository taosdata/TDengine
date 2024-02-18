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

#ifndef _TD_MND_GRANT_H_
#define _TD_MND_GRANT_H_

#ifdef __cplusplus
"C" {
#endif

#include "mndInt.h"

  int32_t mndInitGrant(SMnode * pMnode);
  void    mndCleanupGrant();
  void    grantParseParameter();
  void    grantReset(SMnode * pMnode, EGrantType grant, uint64_t value);
  void    grantAdd(EGrantType grant, uint64_t value);
  void    grantRestore(EGrantType grant, uint64_t value);

#ifdef TD_ENTERPRISE
  SSdbRaw *mndGrantActionEncode(SGrantLogObj * pGrant);
  SSdbRow *mndGrantActionDecode(SSdbRaw * pRaw);
  int32_t  mndGrantActionInsert(SSdb * pSdb, SGrantLogObj * pGrant);
  int32_t  mndGrantActionDelete(SSdb * pSdb, SGrantLogObj * pGrant);
  int32_t  mndGrantActionUpdate(SSdb * pSdb, SGrantLogObj * pOldGrant, SGrantLogObj * pNewGrant);

  int32_t grantAlterActiveCode(SMnode * pMnode, SGrantLogObj * pObj, const char *oldActive, const char *newActive,
                               char **mergeActive);

  int32_t mndProcessConfigGrantReq(SMnode * pMnode, SRpcMsg * pReq, SMCfgClusterReq * pCfg);
  int32_t mndProcessUpdGrantLog(SMnode * pMnode, SRpcMsg * pReq, SArray * pMachines, SGrantState * pState);

  int32_t       mndGrantGetLastState(SMnode * pMnode, SGrantState * pState);
  SGrantLogObj *mndAcquireGrant(SMnode * pMnode, void **ppIter);
  void          mndReleaseGrant(SMnode * pMnode, SGrantLogObj * pGrant, void *pIter);
#endif

#ifdef __cplusplus
}
#endif

#endif
