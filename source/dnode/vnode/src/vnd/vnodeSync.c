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
#include "sync.h"
#include "tq.h"
#include "tqCommon.h"
#include "tsdb.h"
#include "vnd.h"

#define BATCH_ENABLE 0

static inline bool vnodeIsMsgWeak(tmsg_t type) { return false; }

static inline void vnodeWaitBlockMsg(SVnode *pVnode, const SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, msg:%p wait block, type:%s sec:%d seq:%" PRId64, pVnode->config.vgId, pMsg,
          TMSG_INFO(pMsg->msgType), pVnode->blockSec, pVnode->blockSeq);
  tsem_wait(&pVnode->syncSem);
}

static inline void vnodePostBlockMsg(SVnode *pVnode, const SRpcMsg *pMsg) {
  if (vnodeIsMsgBlock(pMsg->msgType)) {
    const STraceId *trace = &pMsg->info.traceId;
    taosThreadMutexLock(&pVnode->lock);
    if (pVnode->blocked) {
      vGTrace("vgId:%d, msg:%p post block, type:%s sec:%d seq:%" PRId64, pVnode->config.vgId, pMsg,
              TMSG_INFO(pMsg->msgType), pVnode->blockSec, pVnode->blockSeq);
      pVnode->blocked = false;
      pVnode->blockSec = 0;
      pVnode->blockSeq = 0;
      tsem_post(&pVnode->syncSem);
    }
    taosThreadMutexUnlock(&pVnode->lock);
  }
}

void vnodeRedirectRpcMsg(SVnode *pVnode, SRpcMsg *pMsg, int32_t code) {
  SEpSet newEpSet = {0};
  syncGetRetryEpSet(pVnode->sync, &newEpSet);

  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, msg:%p is redirect since not leader, numOfEps:%d inUse:%d", pVnode->config.vgId, pMsg,
          newEpSet.numOfEps, newEpSet.inUse);
  for (int32_t i = 0; i < newEpSet.numOfEps; ++i) {
    vGTrace("vgId:%d, msg:%p redirect:%d ep:%s:%u", pVnode->config.vgId, pMsg, i, newEpSet.eps[i].fqdn,
            newEpSet.eps[i].port);
  }
  pMsg->info.hasEpSet = 1;

  if (code == 0) code = TSDB_CODE_SYN_NOT_LEADER;

  SRpcMsg rsp = {.code = code, .info = pMsg->info, .msgType = pMsg->msgType + 1};
  int32_t contLen = tSerializeSEpSet(NULL, 0, &newEpSet);

  rsp.pCont = rpcMallocCont(contLen);
  if (rsp.pCont == NULL) {
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    tSerializeSEpSet(rsp.pCont, contLen, &newEpSet);
    rsp.contLen = contLen;
  }

  tmsgSendRsp(&rsp);
}

static void inline vnodeHandleWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
  if (vnodeProcessWriteMsg(pVnode, pMsg, pMsg->info.conn.applyIndex, &rsp) < 0) {
    rsp.code = terrno;
    const STraceId *trace = &pMsg->info.traceId;
    vGError("vgId:%d, msg:%p failed to apply right now since %s", pVnode->config.vgId, pMsg, terrstr());
  }
  if (rsp.info.handle != NULL) {
    tmsgSendRsp(&rsp);
  } else {
    if (rsp.pCont) {
      rpcFreeCont(rsp.pCont);
    }
  }
}

static void vnodeHandleProposeError(SVnode *pVnode, SRpcMsg *pMsg, int32_t code) {
  if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_RESTORING) {
    vnodeRedirectRpcMsg(pVnode, pMsg, code);
  } else if (code == TSDB_CODE_MSG_PREPROCESSED) {
    SRpcMsg rsp = {.code = TSDB_CODE_SUCCESS, .info = pMsg->info};
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }
  } else {
    const STraceId *trace = &pMsg->info.traceId;
    vGError("vgId:%d, msg:%p failed to propose since %s, code:0x%x", pVnode->config.vgId, pMsg, tstrerror(code), code);
    SRpcMsg rsp = {.code = code, .info = pMsg->info};
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }
  }
}

static int32_t inline vnodeProposeMsg(SVnode *pVnode, SRpcMsg *pMsg, bool isWeak) {
  int64_t seq = 0;

  taosThreadMutexLock(&pVnode->lock);
  int32_t code = syncPropose(pVnode->sync, pMsg, isWeak, &seq);
  bool    wait = (code == 0 && vnodeIsMsgBlock(pMsg->msgType));
  if (wait) {
    ASSERT(!pVnode->blocked);
    pVnode->blocked = true;
    pVnode->blockSec = taosGetTimestampSec();
    pVnode->blockSeq = seq;
  }
  taosThreadMutexUnlock(&pVnode->lock);

  if (code > 0) {
    vnodeHandleWriteMsg(pVnode, pMsg);
  } else if (code < 0) {
    if (terrno != 0) code = terrno;
    vnodeHandleProposeError(pVnode, pMsg, code);
  }

  if (wait) vnodeWaitBlockMsg(pVnode, pMsg);
  return code;
}

void vnodeProposeCommitOnNeed(SVnode *pVnode, bool atExit) {
  if (!vnodeShouldCommit(pVnode, atExit)) {
    return;
  }

  int32_t   contLen = sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  pHead->contLen = contLen;
  pHead->vgId = pVnode->config.vgId;

  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_VND_COMMIT;
  rpcMsg.contLen = contLen;
  rpcMsg.pCont = pHead;
  rpcMsg.info.noResp = 1;

  vInfo("vgId:%d, propose vnode commit", pVnode->config.vgId);
  bool isWeak = false;

  if (!atExit) {
    if (vnodeProposeMsg(pVnode, &rpcMsg, isWeak) < 0) {
      vTrace("vgId:%d, failed to propose vnode commit since %s", pVnode->config.vgId, terrstr());
    }
    rpcFreeCont(rpcMsg.pCont);
    rpcMsg.pCont = NULL;
  } else {
    tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

#if BATCH_ENABLE

static void inline vnodeProposeBatchMsg(SVnode *pVnode, SRpcMsg **pMsgArr, bool *pIsWeakArr, int32_t *arrSize) {
  if (*arrSize <= 0) return;
  SRpcMsg *pLastMsg = pMsgArr[*arrSize - 1];

  taosThreadMutexLock(&pVnode->lock);
  int32_t code = syncProposeBatch(pVnode->sync, pMsgArr, pIsWeakArr, *arrSize);
  bool    wait = (code == 0 && vnodeIsBlockMsg(pLastMsg->msgType));
  if (wait) {
    ASSERT(!pVnode->blocked);
    pVnode->blocked = true;
  }
  taosThreadMutexUnlock(&pVnode->lock);

  if (code > 0) {
    for (int32_t i = 0; i < *arrSize; ++i) {
      vnodeHandleWriteMsg(pVnode, pMsgArr[i]);
    }
  } else if (code < 0) {
    if (terrno != 0) code = terrno;
    for (int32_t i = 0; i < *arrSize; ++i) {
      vnodeHandleProposeError(pVnode, pMsgArr[i], code);
    }
  }

  if (wait) vnodeWaitBlockMsg(pVnode, pLastMsg);
  pLastMsg = NULL;

  for (int32_t i = 0; i < *arrSize; ++i) {
    SRpcMsg        *pMsg = pMsgArr[i];
    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->config.vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }

  *arrSize = 0;
}

void vnodeProposeWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode   *pVnode = pInfo->ahandle;
  int32_t   vgId = pVnode->config.vgId;
  int32_t   code = 0;
  SRpcMsg  *pMsg = NULL;
  int32_t   arrayPos = 0;
  SRpcMsg **pMsgArr = taosMemoryCalloc(numOfMsgs, sizeof(SRpcMsg *));
  bool     *pIsWeakArr = taosMemoryCalloc(numOfMsgs, sizeof(bool));
  vTrace("vgId:%d, get %d msgs from vnode-write queue", vgId, numOfMsgs);

  for (int32_t msg = 0; msg < numOfMsgs; msg++) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    bool isWeak = vnodeIsMsgWeak(pMsg->msgType);
    bool isBlock = vnodeIsMsgBlock(pMsg->msgType);

    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p get from vnode-write queue, weak:%d block:%d msg:%d:%d pos:%d, handle:%p", vgId, pMsg,
            isWeak, isBlock, msg, numOfMsgs, arrayPos, pMsg->info.handle);

    if (!pVnode->restored) {
      vGWarn("vgId:%d, msg:%p failed to process since restore not finished, type:%s", vgId, pMsg,
             TMSG_INFO(pMsg->msgType));
      terrno = TSDB_CODE_SYN_RESTORING;
      vnodeHandleProposeError(pVnode, pMsg, TSDB_CODE_SYN_RESTORING);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    if (pMsgArr == NULL || pIsWeakArr == NULL) {
      vGError("vgId:%d, msg:%p failed to process since out of memory, type:%s", vgId, pMsg, TMSG_INFO(pMsg->msgType));
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      vnodeHandleProposeError(pVnode, pMsg, terrno);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    bool atExit = false;
    vnodeProposeCommitOnNeed(pVnode, atExit);

    code = vnodePreProcessWriteMsg(pVnode, pMsg);
    if (code != 0) {
      vGError("vgId:%d, msg:%p failed to pre-process since %s", vgId, pMsg, terrstr());
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    if (isBlock) {
      vnodeProposeBatchMsg(pVnode, pMsgArr, pIsWeakArr, &arrayPos);
    }

    pMsgArr[arrayPos] = pMsg;
    pIsWeakArr[arrayPos] = isWeak;
    arrayPos++;

    if (isBlock || msg == numOfMsgs - 1) {
      vnodeProposeBatchMsg(pVnode, pMsgArr, pIsWeakArr, &arrayPos);
    }
  }

  taosMemoryFree(pMsgArr);
  taosMemoryFree(pIsWeakArr);
}

#else

void vnodeProposeWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode  *pVnode = pInfo->ahandle;
  int32_t  vgId = pVnode->config.vgId;
  int32_t  code = 0;
  SRpcMsg *pMsg = NULL;
  vTrace("vgId:%d, get %d msgs from vnode-write queue", vgId, numOfMsgs);

  for (int32_t msg = 0; msg < numOfMsgs; msg++) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    bool isWeak = vnodeIsMsgWeak(pMsg->msgType);

    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p get from vnode-write queue, weak:%d block:%d msg:%d:%d, handle:%p", vgId, pMsg, isWeak,
            vnodeIsMsgBlock(pMsg->msgType), msg, numOfMsgs, pMsg->info.handle);

    if (!pVnode->restored) {
      vGWarn("vgId:%d, msg:%p failed to process since restore not finished, type:%s", vgId, pMsg,
             TMSG_INFO(pMsg->msgType));
      vnodeHandleProposeError(pVnode, pMsg, TSDB_CODE_SYN_RESTORING);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    bool atExit = false;
    vnodeProposeCommitOnNeed(pVnode, atExit);

    code = vnodePreProcessWriteMsg(pVnode, pMsg);
    if (code != 0) {
      if (code != TSDB_CODE_MSG_PREPROCESSED) {
        vGError("vgId:%d, msg:%p failed to pre-process since %s", vgId, pMsg, tstrerror(code));
        if (terrno != 0) code = terrno;
      }
      vnodeHandleProposeError(pVnode, pMsg, code);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    code = vnodeProposeMsg(pVnode, pMsg, isWeak);

    vGTrace("vgId:%d, msg:%p is freed, code:0x%x", vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

#endif

void vnodeApplyWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode  *pVnode = pInfo->ahandle;
  int32_t  vgId = pVnode->config.vgId;
  int32_t  code = 0;
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    const STraceId *trace = &pMsg->info.traceId;

    if (vnodeIsMsgBlock(pMsg->msgType)) {
      vGTrace("vgId:%d, msg:%p get from vnode-apply queue, type:%s handle:%p index:%" PRId64
              ", blocking msg obtained sec:%d seq:%" PRId64,
              vgId, pMsg, TMSG_INFO(pMsg->msgType), pMsg->info.handle, pMsg->info.conn.applyIndex, pVnode->blockSec,
              pVnode->blockSeq);
    } else {
      vGTrace("vgId:%d, msg:%p get from vnode-apply queue, type:%s handle:%p index:%" PRId64, vgId, pMsg,
              TMSG_INFO(pMsg->msgType), pMsg->info.handle, pMsg->info.conn.applyIndex);
    }

    SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
    if (rsp.code == 0) {
      if (vnodeProcessWriteMsg(pVnode, pMsg, pMsg->info.conn.applyIndex, &rsp) < 0) {
        rsp.code = terrno;
        vGError("vgId:%d, msg:%p failed to apply since %s, index:%" PRId64, vgId, pMsg, terrstr(),
                pMsg->info.conn.applyIndex);
      }
    }

    vnodePostBlockMsg(pVnode, pMsg);
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    } else {
      if (rsp.pCont) {
        rpcFreeCont(rsp.pCont);
      }
    }

    vGTrace("vgId:%d, msg:%p is freed, code:0x%x index:%" PRId64, vgId, pMsg, rsp.code, pMsg->info.conn.applyIndex);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

int32_t vnodeProcessSyncMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, sync msg:%p will be processed, type:%s", pVnode->config.vgId, pMsg, TMSG_INFO(pMsg->msgType));

  int32_t code = syncProcessMsg(pVnode->sync, pMsg);
  if (code != 0) {
    vGError("vgId:%d, failed to process sync msg:%p type:%s since %s", pVnode->config.vgId, pMsg,
            TMSG_INFO(pMsg->msgType), terrstr());
  }

  return code;
}

static int32_t vnodeSyncEqCtrlMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  if (pMsg == NULL || pMsg->pCont == NULL) {
    return -1;
  }

  if (msgcb == NULL || msgcb->putToQueueFp == NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    return -1;
  }

  int32_t code = tmsgPutToQueue(msgcb, SYNC_RD_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t vnodeSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  if (pMsg == NULL || pMsg->pCont == NULL) {
    return -1;
  }

  if (msgcb == NULL || msgcb->putToQueueFp == NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    return -1;
  }

  int32_t code = tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t vnodeSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = tmsgSendSyncReq(pEpSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t vnodeSyncGetSnapshotInfo(const SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  return vnodeGetSnapshot(pFsm->data, pSnapshot);
}

static int32_t vnodeSyncApplyMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, const SFsmCbMeta *pMeta) {
  SVnode *pVnode = pFsm->data;
  pMsg->info.conn.applyIndex = pMeta->index;
  pMsg->info.conn.applyTerm = pMeta->term;

  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, commit-cb is excuted, fsm:%p, index:%" PRId64 ", term:%" PRIu64 ", msg-index:%" PRId64
          ", weak:%d, code:%d, state:%d %s, type:%s code:0x%x",
          pVnode->config.vgId, pFsm, pMeta->index, pMeta->term, pMsg->info.conn.applyIndex, pMeta->isWeak, pMeta->code,
          pMeta->state, syncStr(pMeta->state), TMSG_INFO(pMsg->msgType), pMsg->code);

  return tmsgPutToQueue(&pVnode->msgCb, APPLY_QUEUE, pMsg);
}

static int32_t vnodeSyncCommitMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, SFsmCbMeta *pMeta) {
  if (pMsg->code == 0) {
    return vnodeSyncApplyMsg(pFsm, pMsg, pMeta);
  }

  const STraceId *trace = &pMsg->info.traceId;
  SVnode         *pVnode = pFsm->data;
  vnodePostBlockMsg(pVnode, pMsg);

  SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
  if (rsp.info.handle != NULL) {
    tmsgSendRsp(&rsp);
  }

  vGTrace("vgId:%d, msg:%p is freed, code:0x%x index:%" PRId64, TD_VID(pVnode), pMsg, rsp.code, pMeta->index);
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  return 0;
}

static int32_t vnodeSyncPreCommitMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, SFsmCbMeta *pMeta) {
  if (pMeta->isWeak == 1) {
    return vnodeSyncApplyMsg(pFsm, pMsg, pMeta);
  }
  return 0;
}

static SyncIndex vnodeSyncAppliedIndex(const SSyncFSM *pFSM) {
  SVnode *pVnode = pFSM->data;
  return atomic_load_64(&pVnode->state.applied);
}

static void vnodeSyncRollBackMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, SFsmCbMeta *pMeta) {
  SVnode *pVnode = pFsm->data;
  vTrace("vgId:%d, rollback-cb is excuted, fsm:%p, index:%" PRId64 ", weak:%d, code:%d, state:%d %s, type:%s",
         pVnode->config.vgId, pFsm, pMeta->index, pMeta->isWeak, pMeta->code, pMeta->state, syncStr(pMeta->state),
         TMSG_INFO(pMsg->msgType));
}

static int32_t vnodeSnapshotStartRead(const SSyncFSM *pFsm, void *pParam, void **ppReader) {
  SVnode *pVnode = pFsm->data;
  int32_t code = vnodeSnapReaderOpen(pVnode, (SSnapshotParam *)pParam, (SVSnapReader **)ppReader);
  return code;
}

static void vnodeSnapshotStopRead(const SSyncFSM *pFsm, void *pReader) {
  SVnode *pVnode = pFsm->data;
  vnodeSnapReaderClose(pReader);
}

static int32_t vnodeSnapshotDoRead(const SSyncFSM *pFsm, void *pReader, void **ppBuf, int32_t *len) {
  SVnode *pVnode = pFsm->data;
  int32_t code = vnodeSnapRead(pReader, (uint8_t **)ppBuf, len);
  return code;
}

static int32_t vnodeSnapshotStartWrite(const SSyncFSM *pFsm, void *pParam, void **ppWriter) {
  SVnode *pVnode = pFsm->data;

  do {
    int32_t itemSize = tmsgGetQueueSize(&pVnode->msgCb, pVnode->config.vgId, APPLY_QUEUE);
    if (itemSize == 0) {
      vInfo("vgId:%d, start write vnode snapshot since apply queue is empty", pVnode->config.vgId);
      break;
    } else {
      vInfo("vgId:%d, write vnode snapshot later since %d items in apply queue", pVnode->config.vgId, itemSize);
      taosMsleep(10);
    }
  } while (true);

  int32_t code = vnodeSnapWriterOpen(pVnode, (SSnapshotParam *)pParam, (SVSnapWriter **)ppWriter);
  return code;
}

static int32_t vnodeSnapshotStopWrite(const SSyncFSM *pFsm, void *pWriter, bool isApply, SSnapshot *pSnapshot) {
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, stop write vnode snapshot, apply:%d, index:%" PRId64 " term:%" PRIu64 " config:%" PRId64,
        pVnode->config.vgId, isApply, pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm, pSnapshot->lastConfigIndex);

  int32_t code = vnodeSnapWriterClose(pWriter, !isApply, pSnapshot);
  if (code != 0) {
    vError("vgId:%d, failed to finish applying vnode snapshot since %s, code:0x%x", pVnode->config.vgId, terrstr(),
           code);
  }
  return code;
}

static int32_t vnodeSnapshotDoWrite(const SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) {
  SVnode *pVnode = pFsm->data;
  vDebug("vgId:%d, continue write vnode snapshot, blockLen:%d", pVnode->config.vgId, len);
  int32_t code = vnodeSnapWrite(pWriter, pBuf, len);
  vDebug("vgId:%d, continue write vnode snapshot finished, blockLen:%d", pVnode->config.vgId, len);
  return code;
}

static void vnodeRestoreFinish(const SSyncFSM *pFsm, const SyncIndex commitIdx) {
  SVnode   *pVnode = pFsm->data;
  int32_t   vgId = pVnode->config.vgId;
  SyncIndex appliedIdx = -1;

  do {
    appliedIdx = vnodeSyncAppliedIndex(pFsm);
    ASSERT(appliedIdx <= commitIdx);
    if (appliedIdx == commitIdx) {
      vInfo("vgId:%d, no items to be applied, restore finish", pVnode->config.vgId);
      break;
    } else {
      vInfo("vgId:%d, restore not finish since %" PRId64 " items to be applied. commit-index:%" PRId64
            ", applied-index:%" PRId64,
            vgId, commitIdx - appliedIdx, commitIdx, appliedIdx);
      taosMsleep(10);
    }
  } while (true);

  ASSERT(commitIdx == vnodeSyncAppliedIndex(pFsm));
  walApplyVer(pVnode->pWal, commitIdx);
  pVnode->restored = true;

  SStreamMeta *pMeta = pVnode->pTq->pStreamMeta;
  streamMetaWLock(pMeta);

  if (pMeta->startInfo.tasksWillRestart) {
    vInfo("vgId:%d, sync restore finished, stream tasks will be launched by other thread", vgId);
    streamMetaWUnLock(pMeta);
    return;
  }

  if (vnodeIsRoleLeader(pVnode)) {
    // start to restore all stream tasks
    if (tsDisableStream) {
      vInfo("vgId:%d, sync restore finished, not launch stream tasks, since stream tasks are disabled", vgId);
    } else {
      vInfo("vgId:%d sync restore finished, start to launch stream task(s)", pVnode->config.vgId);
      int32_t numOfTasks = tqStreamTasksGetTotalNum(pMeta);
      if (numOfTasks > 0) {
        if (pMeta->startInfo.taskStarting == 1) {
          pMeta->startInfo.restartCount += 1;
          tqDebug("vgId:%d in start tasks procedure, inc restartCounter by 1, remaining restart:%d", vgId,
                  pMeta->startInfo.restartCount);
        } else {
          pMeta->startInfo.taskStarting = 1;

          streamMetaWUnLock(pMeta);
          tqStreamTaskStartAsync(pMeta, &pVnode->msgCb, false);
          return;
        }
      }
    }
  } else {
    vInfo("vgId:%d, sync restore finished, not launch stream tasks since not leader", vgId);
  }

  streamMetaWUnLock(pMeta);
}

static void vnodeBecomeFollower(const SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, become follower", pVnode->config.vgId);

  taosThreadMutexLock(&pVnode->lock);
  if (pVnode->blocked) {
    pVnode->blocked = false;
    vDebug("vgId:%d, become follower and post block", pVnode->config.vgId);
    tsem_post(&pVnode->syncSem);
  }
  taosThreadMutexUnlock(&pVnode->lock);

  if (pVnode->pTq) {
    tqUpdateNodeStage(pVnode->pTq, false);
    tqStopStreamTasksAsync(pVnode->pTq);
  }
}

static void vnodeBecomeLearner(const SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, become learner", pVnode->config.vgId);

  taosThreadMutexLock(&pVnode->lock);
  if (pVnode->blocked) {
    pVnode->blocked = false;
    vDebug("vgId:%d, become learner and post block", pVnode->config.vgId);
    tsem_post(&pVnode->syncSem);
  }
  taosThreadMutexUnlock(&pVnode->lock);
}

static void vnodeBecomeLeader(const SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;
  vDebug("vgId:%d, become leader", pVnode->config.vgId);
  if (pVnode->pTq) {
    tqUpdateNodeStage(pVnode->pTq, true);
  }
}

static bool vnodeApplyQueueEmpty(const SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;

  if (pVnode != NULL && pVnode->msgCb.qsizeFp != NULL) {
    int32_t itemSize = tmsgGetQueueSize(&pVnode->msgCb, pVnode->config.vgId, APPLY_QUEUE);
    return (itemSize == 0);
  } else {
    return true;
  }
}

static int32_t vnodeApplyQueueItems(const SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;

  if (pVnode != NULL && pVnode->msgCb.qsizeFp != NULL) {
    int32_t itemSize = tmsgGetQueueSize(&pVnode->msgCb, pVnode->config.vgId, APPLY_QUEUE);
    return itemSize;
  } else {
    return -1;
  }
}

static SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = vnodeSyncCommitMsg;
  pFsm->FpAppliedIndexCb = vnodeSyncAppliedIndex;
  pFsm->FpPreCommitCb = vnodeSyncPreCommitMsg;
  pFsm->FpRollBackCb = vnodeSyncRollBackMsg;
  pFsm->FpGetSnapshot = NULL;
  pFsm->FpGetSnapshotInfo = vnodeSyncGetSnapshotInfo;
  pFsm->FpRestoreFinishCb = vnodeRestoreFinish;
  pFsm->FpLeaderTransferCb = NULL;
  pFsm->FpApplyQueueEmptyCb = vnodeApplyQueueEmpty;
  pFsm->FpApplyQueueItems = vnodeApplyQueueItems;
  pFsm->FpBecomeLeaderCb = vnodeBecomeLeader;
  pFsm->FpBecomeFollowerCb = vnodeBecomeFollower;
  pFsm->FpBecomeLearnerCb = vnodeBecomeLearner;
  pFsm->FpReConfigCb = NULL;
  pFsm->FpSnapshotStartRead = vnodeSnapshotStartRead;
  pFsm->FpSnapshotStopRead = vnodeSnapshotStopRead;
  pFsm->FpSnapshotDoRead = vnodeSnapshotDoRead;
  pFsm->FpSnapshotStartWrite = vnodeSnapshotStartWrite;
  pFsm->FpSnapshotStopWrite = vnodeSnapshotStopWrite;
  pFsm->FpSnapshotDoWrite = vnodeSnapshotDoWrite;

  return pFsm;
}

int32_t vnodeSyncOpen(SVnode *pVnode, char *path, int32_t vnodeVersion) {
  SSyncInfo syncInfo = {
      .snapshotStrategy = SYNC_STRATEGY_WAL_FIRST,
      .batchSize = 1,
      .vgId = pVnode->config.vgId,
      .syncCfg = pVnode->config.syncCfg,
      .pWal = pVnode->pWal,
      .msgcb = &pVnode->msgCb,
      .syncSendMSg = vnodeSyncSendMsg,
      .syncEqMsg = vnodeSyncEqMsg,
      .syncEqCtrlMsg = vnodeSyncEqCtrlMsg,
      .pingMs = 5000,
      .electMs = 4000,
      .heartbeatMs = 700,
  };

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", path, TD_DIRSEP);
  syncInfo.pFsm = vnodeSyncMakeFsm(pVnode);

  SSyncCfg *pCfg = &syncInfo.syncCfg;
  vInfo("vgId:%d, start to open sync, replica:%d selfIndex:%d", pVnode->config.vgId, pCfg->replicaNum, pCfg->myIndex);
  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    vInfo("vgId:%d, index:%d ep:%s:%u dnode:%d cluster:%" PRId64, pVnode->config.vgId, i, pNode->nodeFqdn,
          pNode->nodePort, pNode->nodeId, pNode->clusterId);
  }

  pVnode->sync = syncOpen(&syncInfo, vnodeVersion);
  if (pVnode->sync <= 0) {
    vError("vgId:%d, failed to open sync since %s", pVnode->config.vgId, terrstr());
    return -1;
  }

  return 0;
}

int32_t vnodeSyncStart(SVnode *pVnode) {
  vInfo("vgId:%d, start sync", pVnode->config.vgId);
  if (syncStart(pVnode->sync) < 0) {
    vError("vgId:%d, failed to start sync subsystem since %s", pVnode->config.vgId, terrstr());
    return -1;
  }
  return 0;
}

void vnodeSyncPreClose(SVnode *pVnode) {
  vInfo("vgId:%d, sync pre close", pVnode->config.vgId);
  syncLeaderTransfer(pVnode->sync);
  syncPreStop(pVnode->sync);

  taosThreadMutexLock(&pVnode->lock);
  if (pVnode->blocked) {
    vInfo("vgId:%d, post block after close sync", pVnode->config.vgId);
    pVnode->blocked = false;
    tsem_post(&pVnode->syncSem);
  }
  taosThreadMutexUnlock(&pVnode->lock);
}

void vnodeSyncPostClose(SVnode *pVnode) {
  vInfo("vgId:%d, sync post close", pVnode->config.vgId);
  syncPostStop(pVnode->sync);
}

void vnodeSyncClose(SVnode *pVnode) {
  vInfo("vgId:%d, close sync", pVnode->config.vgId);
  syncStop(pVnode->sync);
}

void vnodeSyncCheckTimeout(SVnode *pVnode) {
  vTrace("vgId:%d, check sync timeout msg", pVnode->config.vgId);
  taosThreadMutexLock(&pVnode->lock);
  if (pVnode->blocked) {
    int32_t curSec = taosGetTimestampSec();
    int32_t delta = curSec - pVnode->blockSec;
    if (delta > VNODE_TIMEOUT_SEC) {
      vError("vgId:%d, failed to propose since timeout and post block, start:%d cur:%d delta:%d seq:%" PRId64,
             pVnode->config.vgId, pVnode->blockSec, curSec, delta, pVnode->blockSeq);
      if (syncSendTimeoutRsp(pVnode->sync, pVnode->blockSeq) != 0) {
#if 0
        SRpcMsg rpcMsg = {.code = TSDB_CODE_SYN_TIMEOUT, .info = pVnode->blockInfo};
        vError("send timeout response since its applyed, seq:%" PRId64 " handle:%p ahandle:%p", pVnode->blockSeq,
              rpcMsg.info.handle, rpcMsg.info.ahandle);
        rpcSendResponse(&rpcMsg);
#endif
      }
      pVnode->blocked = false;
      pVnode->blockSec = 0;
      pVnode->blockSeq = 0;
      tsem_post(&pVnode->syncSem);
    }
  }
  taosThreadMutexUnlock(&pVnode->lock);
}

bool vnodeIsRoleLeader(SVnode *pVnode) {
  SSyncState state = syncGetState(pVnode->sync);
  return state.state == TAOS_SYNC_STATE_LEADER;
}

bool vnodeIsLeader(SVnode *pVnode) {
  terrno = 0;
  SSyncState state = syncGetState(pVnode->sync);

  if (terrno != 0) {
    vInfo("vgId:%d, vnode is stopping", pVnode->config.vgId);
    return false;
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    vInfo("vgId:%d, vnode not leader, state:%s", pVnode->config.vgId, syncStr(state.state));
    return false;
  }

  if (!state.restored || !pVnode->restored) {
    terrno = TSDB_CODE_SYN_RESTORING;
    vInfo("vgId:%d, vnode not restored:%d:%d", pVnode->config.vgId, state.restored, pVnode->restored);
    return false;
  }

  return true;
}

int32_t vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnap) {
  int code = 0;
  pSnap->lastApplyIndex = pVnode->state.committed;
  pSnap->lastApplyTerm = pVnode->state.commitTerm;
  pSnap->lastConfigIndex = -1;
  pSnap->state = SYNC_FSM_STATE_COMPLETE;

  if (tsdbSnapGetFsState(pVnode) != TSDB_FS_STATE_NORMAL) {
    pSnap->state = SYNC_FSM_STATE_INCOMPLETE;
  }

  if (pSnap->type == TDMT_SYNC_PREP_SNAPSHOT || pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    code = tsdbSnapPrepDescription(pVnode, pSnap);
  }
  return code;
}
