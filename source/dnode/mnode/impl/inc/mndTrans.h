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

#ifndef _TD_MND_TRANS_H_
#define _TD_MND_TRANS_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  SEpSet  epSet;
  tmsg_t  msgType;
  int8_t  msgSent;
  int8_t  msgReceived;
  int32_t errCode;
  int32_t acceptableCode;
  int32_t contLen;
  void   *pCont;
} STransAction;

int32_t mndInitTrans(SMnode *pMnode);
void    mndCleanupTrans(SMnode *pMnode);

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnType type, const SRpcMsg *pReq);
void    mndTransDrop(STrans *pTrans);
int32_t mndTransAppendRedolog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction);
int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction);
void    mndTransSetRpcRsp(STrans *pTrans, void *pCont, int32_t contLen);
void    mndTransSetCb(STrans *pTrans, TransCbFp fp, void *param);
void    mndTransSetDbInfo(STrans *pTrans, SDbObj *pDb);

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans);
void    mndTransProcessRsp(SNodeMsg *pRsp);
void    mndTransPullup(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TRANS_H_*/
