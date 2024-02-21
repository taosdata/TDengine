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

#include "catalog.h"
#include "command.h"
#include "query.h"
#include "schInt.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

FORCE_INLINE SSchJob *schAcquireJob(int64_t refId) {
  qDebug("sch acquire jobId:0x%" PRIx64, refId);
  return (SSchJob *)taosAcquireRef(schMgmt.jobRef, refId);
}

FORCE_INLINE int32_t schReleaseJob(int64_t refId) {
  if (0 == refId) {
    return TSDB_CODE_SUCCESS;
  }

  qDebug("sch release jobId:0x%" PRIx64, refId);
  return taosReleaseRef(schMgmt.jobRef, refId);
}

char *schDumpEpSet(SEpSet *pEpSet) {
  if (NULL == pEpSet) {
    return NULL;
  }

  int32_t maxSize = 1024;
  char   *str = taosMemoryMalloc(maxSize);
  if (NULL == str) {
    return NULL;
  }

  int32_t n = 0;
  n += snprintf(str + n, maxSize - n, "numOfEps:%d, inUse:%d eps:", pEpSet->numOfEps, pEpSet->inUse);
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    SEp *pEp = &pEpSet->eps[i];
    n += snprintf(str + n, maxSize - n, "[%s:%d]", pEp->fqdn, pEp->port);
  }

  return str;
}

char *schGetOpStr(SCH_OP_TYPE type) {
  switch (type) {
    case SCH_OP_NULL:
      return "NULL";
    case SCH_OP_EXEC:
      return "EXEC";
    case SCH_OP_FETCH:
      return "FETCH";
    case SCH_OP_GET_STATUS:
      return "GET STATUS";
    default:
      return "UNKNOWN";
  }
}

void schFreeHbTrans(SSchHbTrans *pTrans) {
  rpcReleaseHandle((void *)pTrans->trans.pHandleId, TAOS_CONN_CLIENT);

  schFreeRpcCtx(&pTrans->rpcCtx);
}

void schCleanClusterHb(void *pTrans) {
  SCH_LOCK(SCH_WRITE, &schMgmt.hbLock);

  SSchHbTrans *hb = taosHashIterate(schMgmt.hbConnections, NULL);
  while (hb) {
    if (hb->trans.pTrans == pTrans) {
      SQueryNodeEpId *pEpId = taosHashGetKey(hb, NULL);
      schFreeHbTrans(hb);
      taosHashRemove(schMgmt.hbConnections, pEpId, sizeof(SQueryNodeEpId));
    }

    hb = taosHashIterate(schMgmt.hbConnections, hb);
  }

  SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);
}

int32_t schRemoveHbConnection(SSchJob *pJob, SSchTask *pTask, SQueryNodeEpId *epId) {
  int32_t code = 0;

  SCH_LOCK(SCH_WRITE, &schMgmt.hbLock);
  SSchHbTrans *hb = taosHashGet(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);
    SCH_TASK_ELOG("nodeId %d fqdn %s port %d not in hb connections", epId->nodeId, epId->ep.fqdn, epId->ep.port);
    return TSDB_CODE_SUCCESS;
  }

  int64_t taskNum = atomic_load_64(&hb->taskNum);
  if (taskNum <= 0) {
    schFreeHbTrans(hb);
    taosHashRemove(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId));
  }
  SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);

  return TSDB_CODE_SUCCESS;
}

int32_t schAddHbConnection(SSchJob *pJob, SSchTask *pTask, SQueryNodeEpId *epId, bool *exist) {
  int32_t     code = 0;
  SSchHbTrans hb = {0};

  hb.trans.pTrans = pJob->conn.pTrans;
  hb.taskNum = 1;

  SCH_ERR_RET(schMakeHbRpcCtx(pJob, pTask, &hb.rpcCtx));

  SCH_LOCK(SCH_WRITE, &schMgmt.hbLock);
  code = taosHashPut(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId), &hb, sizeof(SSchHbTrans));
  if (code) {
    SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);
    schFreeRpcCtx(&hb.rpcCtx);

    if (HASH_NODE_EXIST(code)) {
      *exist = true;
      return TSDB_CODE_SUCCESS;
    }

    qError("taosHashPut hb trans failed, nodeId:%d, fqdn:%s, port:%d", epId->nodeId, epId->ep.fqdn, epId->ep.port);
    SCH_ERR_RET(code);
  }

  SCH_UNLOCK(SCH_WRITE, &schMgmt.hbLock);

  return TSDB_CODE_SUCCESS;
}

int32_t schRegisterHbConnection(SSchJob *pJob, SSchTask *pTask, SQueryNodeEpId *pEpId) {
  SSchHbTrans *hb = NULL;

  while (true) {
    SCH_LOCK(SCH_READ, &schMgmt.hbLock);
    hb = taosHashGet(schMgmt.hbConnections, pEpId, sizeof(SQueryNodeEpId));
    if (NULL == hb) {
      bool exist = false;
      SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);
      SCH_ERR_RET(schAddHbConnection(pJob, pTask, pEpId, &exist));
      if (!exist) {
        SCH_RET(schBuildAndSendHbMsg(pEpId, NULL));
      }

      continue;
    }

    break;
  }

  atomic_add_fetch_64(&hb->taskNum, 1);

  SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);

  return TSDB_CODE_SUCCESS;
}

void schDeregisterTaskHb(SSchJob *pJob, SSchTask *pTask) {
  if (!pTask->registerdHb) {
    return;
  }

  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  SQueryNodeEpId  epId = {0};

  epId.nodeId = addr->nodeId;

  SEp *pEp = SCH_GET_CUR_EP(addr);
  strcpy(epId.ep.fqdn, pEp->fqdn);
  epId.ep.port = pEp->port;

  SCH_LOCK(SCH_READ, &schMgmt.hbLock);
  SSchHbTrans *hb = taosHashGet(schMgmt.hbConnections, &epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);
    SCH_TASK_WLOG("nodeId %d fqdn %s port %d not in hb connections", epId.nodeId, epId.ep.fqdn, epId.ep.port);
    return;
  }

  int64_t taskNum = atomic_sub_fetch_64(&hb->taskNum, 1);
  if (0 == taskNum) {
    SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);
    schRemoveHbConnection(pJob, pTask, &epId);
  } else {
    SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);
  }

  pTask->registerdHb = false;
}

int32_t schEnsureHbConnection(SSchJob *pJob, SSchTask *pTask) {
  if (!tsEnableQueryHb) {
    return TSDB_CODE_SUCCESS;
  }

  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  SQueryNodeEpId  epId = {0};

  epId.nodeId = addr->nodeId;

  SEp *pEp = SCH_GET_CUR_EP(addr);
  strcpy(epId.ep.fqdn, pEp->fqdn);
  epId.ep.port = pEp->port;

  SCH_ERR_RET(schRegisterHbConnection(pJob, pTask, &epId));

  pTask->registerdHb = true;

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateHbConnection(SQueryNodeEpId *epId, SSchTrans *trans) {
  int32_t      code = 0;
  SSchHbTrans *hb = NULL;

  SCH_LOCK(SCH_READ, &schMgmt.hbLock);
  hb = taosHashGet(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);
    qInfo("taosHashGet hb connection not exists, nodeId:%d, fqdn:%s, port:%d", epId->nodeId, epId->ep.fqdn,
          epId->ep.port);
    SCH_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  SCH_LOCK(SCH_WRITE, &hb->lock);
  memcpy(&hb->trans, trans, sizeof(*trans));
  SCH_UNLOCK(SCH_WRITE, &hb->lock);
  SCH_UNLOCK(SCH_READ, &schMgmt.hbLock);

  qDebug("hb connection updated, sId:0x%" PRIx64 ", nodeId:%d, fqdn:%s, port:%d, pTrans:%p, pHandle:%p", schMgmt.sId,
         epId->nodeId, epId->ep.fqdn, epId->ep.port, trans->pTrans, trans->pHandle);

  return TSDB_CODE_SUCCESS;
}

void schCloseJobRef(void) {
  if (!atomic_load_8((int8_t *)&schMgmt.exit)) {
    return;
  }

  if (schMgmt.jobRef >= 0) {
    taosCloseRef(schMgmt.jobRef);
    schMgmt.jobRef = -1;
  }
}

uint64_t schGenTaskId(void) { return atomic_add_fetch_64(&schMgmt.taskId, 1); }

#ifdef BUILD_NO_CALL
uint64_t schGenUUID(void) {
  static uint64_t hashId = 0;
  static int32_t  requestSerialId = 0;

  if (hashId == 0) {
    char    uid[64] = {0};
    int32_t code = taosGetSystemUUID(uid, tListLen(uid) - 1);
    if (code != TSDB_CODE_SUCCESS) {
      qError("Failed to get the system uid, reason:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t  ts = taosGetTimestampMs();
  uint64_t pid = taosGetPId();
  int32_t  val = atomic_add_fetch_32(&requestSerialId, 1);

  uint64_t id = ((hashId & 0x0FFF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
  return id;
}
#endif

void schFreeRpcCtxVal(const void *arg) {
  if (NULL == arg) {
    return;
  }

  SMsgSendInfo *pMsgSendInfo = (SMsgSendInfo *)arg;
  destroySendMsgInfo(pMsgSendInfo);
}

void schFreeRpcCtx(SRpcCtx *pCtx) {
  if (NULL == pCtx) {
    return;
  }
  void *pIter = taosHashIterate(pCtx->args, NULL);
  while (pIter) {
    SRpcCtxVal *ctxVal = (SRpcCtxVal *)pIter;

    (*pCtx->freeFunc)(ctxVal->val);

    pIter = taosHashIterate(pCtx->args, pIter);
  }

  taosHashCleanup(pCtx->args);

  if (pCtx->freeFunc) {
    (*pCtx->freeFunc)(pCtx->brokenVal.val);
  }
}

int32_t schGetTaskFromList(SHashObj *pTaskList, uint64_t taskId, SSchTask **pTask) {
  int32_t s = taosHashGetSize(pTaskList);
  if (s <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSchTask **task = taosHashGet(pTaskList, &taskId, sizeof(taskId));
  if (NULL == task || NULL == (*task)) {
    return TSDB_CODE_SUCCESS;
  }

  *pTask = *task;

  return TSDB_CODE_SUCCESS;
}
