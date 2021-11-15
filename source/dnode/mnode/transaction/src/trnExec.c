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

  if (sdbWrite(pData) != 0) {
    code = terrno;
    trnSendRpcRsp(pData, code);
    terrno = code;
    return -1;
  }

  return 0;
}

static int32_t trnExecuteArray(SArray *pArray) {
  for (int32_t index = 0; index < pArray->size; ++index) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, index);
    if (sdbWrite(pRaw) != 0) {
      return -1;
    }
  }

  return 0;
}

static int32_t trnExecuteRedoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->redoLogs); }

static int32_t trnExecuteUndoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->undoLogs); }

static int32_t trnExecuteCommitLogs(STrans *pTrans) { return trnExecuteArray(pTrans->commitLogs); }

static int32_t trnExecuteRedoActions(STrans *pTrans) { return trnExecuteArray(pTrans->redoActions); }

static int32_t trnExecuteUndoActions(STrans *pTrans) { return trnExecuteArray(pTrans->undoActions); }

static int32_t trnPerformPrepareStage(STrans *pTrans) {
  if (trnExecuteRedoLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformExecuteStage(STrans *pTrans) {
  int32_t code = trnExecuteRedoActions(pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_COMMIT;
    return 0;
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    return -1;
  } else {
    if (pTrans->policy == TRN_POLICY_RETRY) {
      pTrans->stage = TRN_STAGE_RETRY;
    } else {
      pTrans->stage = TRN_STAGE_ROLLBACK;
    }
    return 0;
  }
}

static int32_t trnPerformCommitStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformRollbackStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformRetryStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

int32_t trnExecute(int32_t tranId) {
  int32_t code = 0;

  STrans *pTrans = sdbAcquire(SDB_TRANS, &tranId);
  if (pTrans == NULL) {
    return -1;
  }

  if (pTrans->stage == TRN_STAGE_PREPARE) {
    if (trnPerformPrepareStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_EXECUTE) {
    if (trnPerformExecuteStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_COMMIT) {
    if (trnPerformCommitStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_ROLLBACK) {
    if (trnPerformRollbackStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_RETRY) {
    if (trnPerformRetryStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  sdbRelease(pTrans);
  return 0;
}