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

typedef enum {
  TRANS_START_FUNC_TEST = 1,
  TRANS_STOP_FUNC_TEST = 2,
  TRANS_START_FUNC_MQ_REB = 3,
  TRANS_STOP_FUNC_MQ_REB = 4,
  TRANS_FUNC_RECOVER_STREAM_STEP_NEXT = 5,
} ETrnFunc;

typedef enum {
  TRANS_ACTION_NULL = 0,
  TRANS_ACTION_MSG = 1,
  TRANS_ACTION_RAW = 2,
} ETrnAct;

typedef struct {
  int32_t   id;
  int32_t   errCode;
  int32_t   acceptableCode;
  int32_t   retryCode;
  ETrnAct   actionType;
  ETrnStage stage;
  int8_t    reserved;
  int8_t    rawWritten;
  int8_t    msgSent;
  int8_t    msgReceived;
  tmsg_t    msgType;
  SEpSet    epSet;
  int32_t   contLen;
  void     *pCont;
  SSdbRaw  *pRaw;

  int64_t mTraceId;
} STransAction;

typedef void (*TransCbFp)(SMnode *pMnode, void *param, int32_t paramLen);

int32_t mndInitTrans(SMnode *pMnode);
void    mndCleanupTrans(SMnode *pMnode);
STrans *mndAcquireTrans(SMnode *pMnode, int32_t transId);
void    mndReleaseTrans(SMnode *pMnode, STrans *pTrans);

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnConflct conflict, const SRpcMsg *pReq,
                       const char *opername);
void    mndTransDrop(STrans *pTrans);

int32_t mndTransAppendPrepareLog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendRedolog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw);
int32_t mndTransAppendNullLog(STrans *pTrans);

int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction);
int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction);
void    mndTransSetRpcRsp(STrans *pTrans, void *pCont, int32_t contLen);
void    mndTransSetCb(STrans *pTrans, ETrnFunc startFunc, ETrnFunc stopFunc, void *param, int32_t paramLen);
void    mndTransSetDbName(STrans *pTrans, const char *dbname, const char *stbname);
void    mndTransSetSerial(STrans *pTrans);
void    mndTransSetParallel(STrans *pTrans);
void    mndTransSetOper(STrans *pTrans, EOperType oper);
int32_t mndTransCheckConflict(SMnode *pMnode, STrans *pTrans);
#ifndef BUILD_NO_CALL
static int32_t mndTrancCheckConflict(SMnode *pMnode, STrans *pTrans) {
    return mndTransCheckConflict(pMnode, pTrans);
}
#endif
int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans);
int32_t mndTransProcessRsp(SRpcMsg *pRsp);
void    mndTransPullup(SMnode *pMnode);
int32_t mndKillTrans(SMnode *pMnode, STrans *pTrans);
void    mndTransExecute(SMnode *pMnode, STrans *pTrans);
void    mndTransRefresh(SMnode *pMnode, STrans *pTrans);
int32_t mndSetRpcInfoForDbTrans(SMnode *pMnode, SRpcMsg *pMsg, EOperType oper, const char *dbname);

SSdbRaw *mndTransEncode(STrans *pTrans);
SSdbRow *mndTransDecode(SSdbRaw *pRaw);
void     mndTransDropData(STrans *pTrans);

bool mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TRANS_H_*/
