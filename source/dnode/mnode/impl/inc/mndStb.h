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

#ifndef _TD_MND_STB_H_
#define _TD_MND_STB_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t  mndInitStb(SMnode *pMnode);
void     mndCleanupStb(SMnode *pMnode);
SStbObj *mndAcquireStb(SMnode *pMnode, char *stbName);
void     mndReleaseStb(SMnode *pMnode, SStbObj *pStb);
SSdbRaw *mndStbActionEncode(SStbObj *pStb);
int32_t  mndValidateStbInfo(SMnode *pMnode, SSTableVersion *pStbs, int32_t numOfStbs, void **ppRsp, int32_t *pRspLen);
int32_t  mndGetNumOfStbs(SMnode *pMnode, char *dbName, int32_t *pNumOfStbs);

int32_t mndCheckCreateStbReq(SMCreateStbReq *pCreate);
SDbObj *mndAcquireDbByStb(SMnode *pMnode, const char *stbName);
int32_t mndBuildStbFromReq(SMnode *pMnode, SStbObj *pDst, SMCreateStbReq *pCreate, SDbObj *pDb);
int32_t mndAddStbToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb);
void    mndFreeStb(SStbObj *pStb);
int32_t mndBuildSMCreateStbRsp(SMnode *pMnode, char *dbFName, char *stbFName, void **pCont, int32_t *pLen);

void mndExtractDbNameFromStbFullName(const char *stbFullName, char *dst);
void mndExtractShortDbNameFromStbFullName(const char *stbFullName, char *dst);
void mndExtractShortDbNameFromDbFullName(const char *stbFullName, char *dst);
void mndExtractTbNameFromStbFullName(const char *stbFullName, char *dst, int32_t dstSize);

const char *mndGetStbStr(const char *src);

int32_t mndAllocStbSchemas(const SStbObj *pOld, SStbObj *pNew);
int32_t mndCheckColAndTagModifiable(SMnode *pMnode, const char *stbFullName, int64_t suid, col_id_t colId);
void   *mndBuildVCreateStbReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, int32_t *pContLen, void *alterOriData,
                              int32_t alterOriDataLen);
int32_t mndSetForceDropCreateStbRedoActions(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SStbObj *pStb);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STB_H_*/
