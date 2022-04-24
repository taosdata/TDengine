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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmImp.h"

static void dmUpdateDnodeCfg(SDnode *pDnode, SDnodeCfg *pCfg) {
  if (pDnode->data.dnodeId == 0 || pDnode->data.clusterId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pDnode->data.latch);
    pDnode->data.dnodeId = pCfg->dnodeId;
    pDnode->data.clusterId = pCfg->clusterId;
    dmWriteEps(pDnode);
    taosWUnLockLatch(&pDnode->data.latch);
  }
}

static int32_t dmProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp) {
  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pDnode->data.dropped && pDnode->data.dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pDnode->data.dnodeId);
      pDnode->data.dropped = 1;
      dmWriteEps(pDnode);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen > 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pDnode->data.dnodeVer = statusRsp.dnodeVer;
      dmUpdateDnodeCfg(pDnode, &statusRsp.dnodeCfg);
      dmUpdateEps(pDnode, statusRsp.pDnodeEps);
    }
    rpcFreeCont(pRsp->pCont);
    tFreeSStatusRsp(&statusRsp);
  }

  return TSDB_CODE_SUCCESS;
}

void dmSendStatusReq(SDnode *pDnode) {
  SStatusReq req = {0};

  taosRLockLatch(&pDnode->data.latch);
  req.sver = tsVersion;
  req.dnodeVer = pDnode->data.dnodeVer;
  req.dnodeId = pDnode->data.dnodeId;
  req.clusterId = pDnode->data.clusterId;
  if (req.clusterId == 0) req.dnodeId = 0;
  req.rebootTime = pDnode->data.rebootTime;
  req.updateTime = pDnode->data.updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = pDnode->data.supportVnodes;
  tstrncpy(req.dnodeEp, pDnode->data.localEp, TSDB_EP_LEN);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezoneStr, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosRUnLockLatch(&pDnode->data.latch);

  SMonVloadInfo vinfo = {0};
  dmGetVnodeLoads(pDnode, &vinfo);
  req.pVloads = vinfo.pVloads;
  pDnode->data.unsyncedVgId = 0;
  pDnode->data.vndState = TAOS_SYNC_STATE_LEADER;
  for (int32_t i = 0; i < taosArrayGetSize(req.pVloads); ++i) {
    SVnodeLoad *pLoad = taosArrayGet(req.pVloads, i);
    if (pLoad->syncState != TAOS_SYNC_STATE_LEADER && pLoad->syncState != TAOS_SYNC_STATE_FOLLOWER) {
      pDnode->data.unsyncedVgId = pLoad->vgId;
      pDnode->data.vndState = pLoad->syncState;
    }
  }

  SMonMloadInfo minfo = {0};
  dmGetMnodeLoads(pDnode, &minfo);
  pDnode->data.isMnode = minfo.isMnode;
  pDnode->data.mndState = minfo.load.syncState;

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  tFreeSStatusReq(&req);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)0x9527};
  SRpcMsg rpcRsp = {0};

  dTrace("send req:%s to mnode, app:%p", TMSG_INFO(rpcMsg.msgType), rpcMsg.ahandle);
  dmSendToMnodeRecv(pDnode, &rpcMsg, &rpcRsp);
  dmProcessStatusRsp(pDnode, &rpcRsp);
}

int32_t dmProcessAuthRsp(SDnode *pDnode, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("auth rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessGrantRsp(SDnode *pDnode, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("grant rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessConfigReq(SDnode *pDnode, SNodeMsg *pMsg) {
  SRpcMsg       *pReq = &pMsg->rpcMsg;
  SDCfgDnodeReq *pCfg = pReq->pCont;
  dError("config req is received, but not supported yet");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t dmProcessCreateNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper != NULL) {
    dmReleaseWrapper(pWrapper);
    terrno = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    dError("failed to create node since %s", terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);
  pWrapper = &pDnode->wrappers[ntype];

  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
    return -1;
  }

  int32_t code = (*pWrapper->fp.createFp)(pWrapper, pMsg);
  if (code != 0) {
    dError("node:%s, failed to create since %s", pWrapper->name, terrstr());
  } else {
    dDebug("node:%s, has been created", pWrapper->name);
    (void)dmOpenNode(pWrapper);
    pWrapper->required = true;
    pWrapper->deployed = true;
    pWrapper->procType = pDnode->ptype;
  }

  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

int32_t dmProcessDropNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    dError("failed to drop node since %s", terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);

  int32_t code = (*pWrapper->fp.dropFp)(pWrapper, pMsg);
  if (code != 0) {
    dError("node:%s, failed to drop since %s", pWrapper->name, terrstr());
  } else {
    dDebug("node:%s, has been dropped", pWrapper->name);
    pWrapper->required = false;
    pWrapper->deployed = false;
  }

  dmReleaseWrapper(pWrapper);

  if (code == 0) {
    dmCloseNode(pWrapper);
    taosRemoveDir(pWrapper->path);
  }
  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

static void dmSetMgmtMsgHandle(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dmSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);

  // Requests handled by MNODE
  dmSetMsgHandle(pWrapper, TDMT_MND_GRANT_RSP, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dmSetMsgHandle(pWrapper, TDMT_MND_AUTH_RSP, dmProcessMgmtMsg, DEFAULT_HANDLE);
}

static int32_t dmStartMgmt(SMgmtWrapper *pWrapper) {
  if (dmStartStatusThread(pWrapper->pDnode) != 0) {
    return -1;
  }
  if (dmStartMonitorThread(pWrapper->pDnode) != 0) {
    return -1;
  }
  return 0;
}

static void dmStopMgmt(SMgmtWrapper *pWrapper) {
  dmStopMonitorThread(pWrapper->pDnode);
  dmStopStatusThread(pWrapper->pDnode);
}

static int32_t dmSpawnUdfd(SDnode *pDnode);

void dmUdfdExit(uv_process_t *process, int64_t exitStatus, int termSignal) {
  dInfo("udfd process exited with status %" PRId64 ", signal %d", exitStatus, termSignal);
  SDnode *pDnode = process->data;
  if (exitStatus == 0 && termSignal == 0 || atomic_load_32(&pDnode->udfdData.stopCalled)) {
    dInfo("udfd process exit due to SIGINT or dnode-mgmt called stop");
  } else {
    dInfo("udfd process restart");
    dmSpawnUdfd(pDnode);
  }
}

static int32_t dmSpawnUdfd(SDnode *pDnode) {
  dInfo("dnode start spawning udfd");
  uv_process_options_t options = {0};

  char path[PATH_MAX] = {0};
  if (tsProcPath == NULL) {
    path[0] = '.';
  } else {
    strncpy(path, tsProcPath, strlen(tsProcPath));
    taosDirName(path);
  }
  strcat(path, "/udfd");
  char* argsUdfd[] = {path, "-c", configDir, NULL};
  options.args = argsUdfd;
  options.file = path;

  options.exit_cb = dmUdfdExit;

  SUdfdData *pData = &pDnode->udfdData;
  uv_pipe_init(&pData->loop, &pData->ctrlPipe, 1);

  uv_stdio_container_t child_stdio[3];
  child_stdio[0].flags = UV_CREATE_PIPE | UV_READABLE_PIPE;
  child_stdio[0].data.stream = (uv_stream_t*) &pData->ctrlPipe;
  child_stdio[1].flags = UV_IGNORE;
  child_stdio[2].flags = UV_INHERIT_FD;
  child_stdio[2].data.fd = 2;
  options.stdio_count = 3;
  options.stdio = child_stdio;

  options.flags = UV_PROCESS_DETACHED;

  char dnodeIdEnvItem[32] = {0};
  char thrdPoolSizeEnvItem[32] = {0};
  snprintf(dnodeIdEnvItem, 32, "%s=%d", "DNODE_ID", pDnode->data.dnodeId);
  float numCpuCores = 4;
  taosGetCpuCores(&numCpuCores);
  snprintf(thrdPoolSizeEnvItem,32,  "%s=%d", "UV_THREADPOOL_SIZE", (int)numCpuCores*2);
  char* envUdfd[] = {dnodeIdEnvItem, thrdPoolSizeEnvItem, NULL};
  options.env = envUdfd;

  int err = uv_spawn(&pData->loop, &pData->process, &options);
  pData->process.data = (void*)pDnode;

  if (err != 0) {
    dError("can not spawn udfd. path: %s, error: %s", path, uv_strerror(err));
  }
  return err;
}

static void dmUdfdCloseWalkCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

static void dmUdfdStopAsyncCb(uv_async_t *async) {
  SDnode *pDnode = async->data;
  SUdfdData *pData = &pDnode->udfdData;
  uv_stop(&pData->loop);
}

static void dmWatchUdfd(void *args) {
  SDnode *pDnode = args;
  SUdfdData *pData = &pDnode->udfdData;
  uv_loop_init(&pData->loop);
  uv_async_init(&pData->loop, &pData->stopAsync, dmUdfdStopAsyncCb);
  pData->stopAsync.data = pDnode;
  int32_t err = dmSpawnUdfd(pDnode);
  atomic_store_32(&pData->spawnErr, err);
  uv_barrier_wait(&pData->barrier);
  uv_run(&pData->loop, UV_RUN_DEFAULT);
  uv_loop_close(&pData->loop);

  uv_walk(&pData->loop, dmUdfdCloseWalkCb, NULL);
  uv_run(&pData->loop, UV_RUN_DEFAULT);
  uv_loop_close(&pData->loop);
  return;
}

static int32_t dmStartUdfd(SDnode *pDnode) {
  SUdfdData *pData = &pDnode->udfdData;
  if (pData->startCalled) {
    dInfo("dnode-mgmt start udfd already called");
    return 0;
  }
  pData->startCalled = true;
  uv_barrier_init(&pData->barrier, 2);
  uv_thread_create(&pData->thread, dmWatchUdfd, pDnode);
  uv_barrier_wait(&pData->barrier);
  pData->needCleanUp = true;
  return pData->spawnErr;
}

static int32_t dmStopUdfd(SDnode *pDnode) {
  dInfo("dnode-mgmt to stop udfd. need cleanup: %d, spawn err: %d",
        pDnode->udfdData.needCleanUp, pDnode->udfdData.spawnErr);
  SUdfdData *pData = &pDnode->udfdData;
  if (!pData->needCleanUp || atomic_load_32(&pData->stopCalled)) {
    return 0;
  }
  atomic_store_32(&pData->stopCalled, 1);
  pData->needCleanUp = false;
  uv_barrier_destroy(&pData->barrier);
  uv_async_send(&pData->stopAsync);
  uv_thread_join(&pData->thread);

  return 0;
}

static int32_t dmInitMgmt(SMgmtWrapper *pWrapper) {
  dInfo("dnode-mgmt start to init");
  SDnode *pDnode = pWrapper->pDnode;

  pDnode->data.dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pDnode->data.dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadEps(pDnode) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pDnode->data.dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  if (dmStartWorker(pDnode) != 0) {
    return -1;
  }

  if (dmInitTrans(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    return -1;
  }
  dmReportStartup(pDnode, "dnode-transport", "initialized");

//  if (dmStartUdfd(pDnode) != 0) {
//    dError("failed to start udfd");
//  }

  dInfo("dnode-mgmt is initialized");
  return 0;
}

static void dmCleanupMgmt(SMgmtWrapper *pWrapper) {
  dInfo("dnode-mgmt start to clean up");
  SDnode *pDnode = pWrapper->pDnode;
  dmStopUdfd(pDnode);
  dmStopWorker(pDnode);

  taosWLockLatch(&pDnode->data.latch);
  if (pDnode->data.dnodeEps != NULL) {
    taosArrayDestroy(pDnode->data.dnodeEps);
    pDnode->data.dnodeEps = NULL;
  }
  if (pDnode->data.dnodeHash != NULL) {
    taosHashCleanup(pDnode->data.dnodeHash);
    pDnode->data.dnodeHash = NULL;
  }
  taosWUnLockLatch(&pDnode->data.latch);

  dmCleanupTrans(pDnode);
  dInfo("dnode-mgmt is cleaned up");
}

static int32_t dmRequireMgmt(SMgmtWrapper *pWrapper, bool *required) {
  *required = true;
  return 0;
}

void dmSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInitMgmt;
  mgmtFp.closeFp = dmCleanupMgmt;
  mgmtFp.startFp = dmStartMgmt;
  mgmtFp.stopFp = dmStopMgmt;
  mgmtFp.requiredFp = dmRequireMgmt;

  dmSetMgmtMsgHandle(pWrapper);
  pWrapper->name = "dnode";
  pWrapper->fp = mgmtFp;
}
