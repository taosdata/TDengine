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

#ifndef _TD_MND_EXT_SOURCE_H_
#define _TD_MND_EXT_SOURCE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// Community-visible: lifecycle
int32_t mndInitExtSource(SMnode *pMnode);
void    mndCleanupExtSource(SMnode *pMnode);

// Community-visible: message handlers (stubs in community, full impl in enterprise)
int32_t mndProcessCreateExtSourceReq(SRpcMsg *pReq);
int32_t mndProcessAlterExtSourceReq(SRpcMsg *pReq);
int32_t mndProcessDropExtSourceReq(SRpcMsg *pReq);
int32_t mndProcessRefreshExtSourceReq(SRpcMsg *pReq);
int32_t mndProcessGetExtSourceReq(SRpcMsg *pReq);

// Community-visible: system table retrieve (returns 0 rows in community)
int32_t mndRetrieveExtSources(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
void    mndCancelGetNextExtSource(SMnode *pMnode, void *pIter);

#ifdef TD_ENTERPRISE

// SDB action callbacks — implemented in enterprise mndExtSource.c
SSdbRaw *mndExtSourceActionEncode(SExtSourceObj *pSource);
SSdbRow *mndExtSourceActionDecode(SSdbRaw *pRaw);
int32_t  mndExtSourceActionInsert(SSdb *pSdb, SExtSourceObj *pSource);
int32_t  mndExtSourceActionDelete(SSdb *pSdb, SExtSourceObj *pSource);
int32_t  mndExtSourceActionUpdate(SSdb *pSdb, SExtSourceObj *pOld, SExtSourceObj *pNew);

// Acquire/release helpers
SExtSourceObj *mndAcquireExtSource(SMnode *pMnode, const char *sourceName);
void           mndReleaseExtSource(SMnode *pMnode, SExtSourceObj *pSource);

// Impl functions (called from community bridge functions)
int32_t mndProcessCreateExtSourceReqImpl(SCreateExtSourceReq *pCreateReq, SRpcMsg *pReq);
int32_t mndProcessAlterExtSourceReqImpl(SAlterExtSourceReq *pAlterReq, SRpcMsg *pReq);
int32_t mndProcessDropExtSourceReqImpl(SDropExtSourceReq *pDropReq, SRpcMsg *pReq);
int32_t mndProcessRefreshExtSourceReqImpl(SRefreshExtSourceReq *pRefreshReq, SRpcMsg *pReq);
int32_t mndProcessGetExtSourceReqImpl(SRpcMsg *pReq);
int32_t mndRetrieveExtSourcesImpl(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

// Heartbeat validation
int32_t mndValidateExtSourceInfo(SMnode *pMnode, SExtSourceVersion *pVersions, int32_t numOfSources,
                                  void **ppRsp, int32_t *pRspLen);

#endif /* TD_ENTERPRISE */

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_EXT_SOURCE_H_*/
