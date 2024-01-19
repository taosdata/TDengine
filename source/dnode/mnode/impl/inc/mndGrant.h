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
  void    mndCleanupGrant(SMnode * pMnode);
  void    grantParseParameter();
  void    grantReset(SMnode * pMnode, EGrantType grant, uint64_t value);
  void    grantAdd(EGrantType grant, uint64_t value);
  void    grantRestore(EGrantType grant, uint64_t value);

#ifdef TD_ENTERPRISE
  SSdbRaw *mndGrantActionEncode(SGrantObj * pGrant);
  SSdbRow *mndGrantActionDecode(SSdbRaw * pRaw);
  int32_t  mndGrantActionInsert(SSdb * pSdb, SGrantObj * pGrant);
  int32_t  mndGrantActionDelete(SSdb * pSdb, SGrantObj * pGrant);
  int32_t  mndGrantActionUpdate(SSdb * pSdb, SGrantObj * pOldGrant, SGrantObj * pNewGrant);

  int32_t grantAlterActiveCode(const char *active, char **newActive);

  int32_t mndProcessConfigGrantReq(SRpcMsg * pReq, SMCfgClusterReq * pCfg);
  int32_t mndProcessUpdMachineReq(SRpcMsg * pReq, SArray *pMachines);
  int32_t mndProcessUpdStateReq(SRpcMsg * pReq, SGrantState *pState);
#endif

#ifdef __cplusplus
}
#endif

#endif
