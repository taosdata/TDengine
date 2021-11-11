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
#include "trnInt.h"
#include "trpc.h"

void trnSetRpcHandle(STrans *pTrans, void *rpcHandle) {
  if (pTrans != NULL) {
    pTrans->rpcHandle = rpcHandle;
  }
}


int32_t trnPrepare(STrans *pTrans, int32_t (*syncfp)(SSdbRaw *pRaw, void *pData)) {
  if (syncfp == NULL) return -1;

  SSdbRaw *pRaw = trnActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("tranId:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }

  if (sdbWrite(pRaw) != 0) {
    mError("tranId:%d, failed to write trans since %s", pTrans->id, terrstr());
    return -1;
  }

  if ((*syncfp)(pRaw, pTrans->rpcHandle) != 0) {
    mError("tranId:%d, failed to sync trans since %s", pTrans->id, terrstr());
    return -1;
  }

  return 0;
}

static void trnSendRpcRsp(void *rpcHandle, int32_t code) {
  if (rpcHandle != NULL) {
    SRpcMsg rspMsg = {.handle = rpcHandle, .code = terrno};
    rpcSendResponse(&rspMsg);
  }
}

int32_t trnApply(SSdbRaw *pRaw, void *pData, int32_t code) {
  if (code != 0) {
    trnSendRpcRsp(pData, terrno);
    return 0;
  }

  if (sdbWrite(pRaw) != 0) {
    code = terrno;
    trnSendRpcRsp(pData, code);
    terrno = code;
    return -1;
  }

  return 0;
}

int32_t trnExecuteRedoLogs(STrans *pTrans) {return 0;}
int32_t trnExecuteUndoLogs(STrans *pTrans) {return 0;}
int32_t trnExecuteCommitLogs(STrans *pTrans) {return 0;}
int32_t trnExecuteRedoActions(STrans *pTrans) {return 0;}
int32_t trnExecuteUndoActions(STrans *pTrans) {return 0;}
static int32_t trnPerfomRollbackStage(STrans *pTrans) { return 0; }

int32_t trnExecute(int32_t tranId) {
  int32_t code = 0;

  STrans *pTrans = sdbAcquire(SDB_TRANS, &tranId);
  if (pTrans == NULL) {
    code = terrno;
    return code;
  }

  if (pTrans->stage == TRN_STAGE_PREPARE) {
    code = trnExecuteRedoLogs(pTrans);
    if (code == 0) {
      pTrans->stage = TRN_STAGE_EXECUTE;
    } else {
      pTrans->stage = TRN_STAGE_ROLLBACK;
    }
  }

  if (pTrans->stage == TRN_STAGE_EXECUTE) {
    code = trnExecuteRedoActions(pTrans);
    if (code == 0) {
      pTrans->stage = TRN_STAGE_COMMIT;
    } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      // do nothing
    } else {
      if (pTrans->policy == TRN_POLICY_RETRY) {
        pTrans->stage = TRN_STAGE_RETRY;
      } else {
        pTrans->stage = TRN_STAGE_ROLLBACK;
      }
    }
  }

  if (pTrans->stage == TRN_STAGE_COMMIT) {
    code = trnExecuteCommitLogs(pTrans);
    if (code == 0) {
      trnDrop(pTrans);
    }
  }

  if (pTrans->stage == TRN_STAGE_ROLLBACK) {
  }

  if (pTrans->stage == TRN_STAGE_RETRY) {
  }

  return 0;
}