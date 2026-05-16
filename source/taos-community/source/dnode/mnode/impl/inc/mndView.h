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

#ifndef _TD_MND_VIEW_H_
#define _TD_MND_VIEW_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitView(SMnode *pMnode);
void    mndCleanupView(SMnode *pMnode);

int32_t mndProcessCreateViewReq(SRpcMsg *pReq);
int32_t mndProcessDropViewReq(SRpcMsg *pReq);
int32_t mndProcessGetViewMetaReq(SRpcMsg *pReq);


#ifdef TD_ENTERPRISE

void initDynViewVersion(void);

SViewObj *mndAcquireView(SMnode *pMnode, char *viewName);
void      mndReleaseView(SMnode *pMnode, SViewObj *pView);

SSdbRaw *mndViewActionEncode(SViewObj *pView);
SSdbRow *mndViewActionDecode(SSdbRaw *pRaw);
int32_t mndViewActionInsert(SSdb *pSdb, SViewObj *pView);
int32_t mndViewActionDelete(SSdb *pSdb, SViewObj *pView);
int32_t mndViewActionUpdate(SSdb *pSdb, SViewObj *pOldView, SViewObj *pNewView);

int32_t mndProcessCreateViewReqImpl(SCMCreateViewReq* pCreateView, SRpcMsg *pReq);
int32_t mndProcessDropViewReqImpl(SCMDropViewReq* pDropView, SRpcMsg *pReq);
int32_t mndProcessViewMetaReqImpl(SViewMetaReq* pMetaReq, SRpcMsg *pReq);
int32_t mndRetrieveViewImpl(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
int32_t mndDropViewByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndValidateDynViewVersion(SMnode *pMnode, SDynViewVersion* pReqVer, bool *needCheck, SDynViewVersion** ppRspVer);
int32_t mndValidateViewInfo(SMnode *pMnode, SViewVersion *pViewVersions, int32_t numOfViews, void **ppRsp,
                           int32_t *pRspLen);

#endif

int32_t mndRetrieveView(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
void mndCancelGetNextView(SMnode *pMnode, void *pIter);



#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_VIEW_H_*/
