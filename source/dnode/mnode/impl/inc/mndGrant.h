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

  // void initDynGrantVersion(void);

  SGrantObj *mndAcquireGrant(SMnode * pMnode, int32_t id);
  void      mndReleaseGrant(SMnode * pMnode, SGrantObj *pGrant);

  SSdbRaw *mndGrantActionEncode(SGrantObj * pGrant);
  SSdbRow *mndGrantActionDecode(SSdbRaw * pRaw);
  int32_t  mndGrantActionInsert(SSdb * pSdb, SGrantObj * pGrant);
  int32_t  mndGrantActionDelete(SSdb * pSdb, SGrantObj * pGrant);
  int32_t  mndGrantActionUpdate(SSdb * pSdb, SGrantObj * pOldGrant, SGrantObj * pNewGrant);

  int32_t mndProcessUpdMachineReqImpl(void *pMachine, SRpcMsg *pReq);
  int32_t mndProcessUpdStateReqImpl(void *pState, SRpcMsg *pReq);
  int32_t mndProcessUpdActiveReqImpl(void *pActive, SRpcMsg *pReq);
  int32_t mndRetrieveGrantImpl(SRpcMsg * pReq, SShowObj * pShow, SSDataBlock * pBlock, int32_t rows);

#endif

#ifdef __cplusplus
}
#endif

#endif
