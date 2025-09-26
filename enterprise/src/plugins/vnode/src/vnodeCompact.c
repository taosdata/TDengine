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

#include "tsdb.h"
#include "vnd.h"

extern int32_t metaCompact(SMeta *pOldMeta, SMeta *pNewMeta, int64_t compactVersion);
extern int32_t metaOpenImpl(SVnode *pVnode, SMeta **ppMeta, const char *metaDir, int8_t rollback);
extern void    tsdbStopAllCompTask(STsdb *tsdb);
extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool ssMigrate);
extern int32_t tsdbCompMonitorGetInfo(STsdb *tsdb, SQueryCompactProgressRsp *rsp);
extern void    tsdbRemoveCompMonitorTask(STsdb *tsdb, SVATaskID *taskId);

static int32_t vnodeAsyncCompactMeta(SVnode *pVnode);

int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SCompactVnodeReq req = {0};

  int32_t code = tDeserializeSCompactVnodeReq(pReq, len, &req);
  if (code) return code;

  vInfo("vgId:%d, compact msg will be processed, db:%s dbUid:%" PRId64 " compactStartTime:%" PRId64 " metaOnly:%d",
        TD_VID(pVnode), req.db, req.dbUid, req.compactStartTime, req.metaOnly);

  if (req.metaOnly) {
    return vnodeAsyncCompactMeta(pVnode);
  } else {
    return tsdbAsyncCompact(pVnode->pTsdb, &req.tw, false);
  }
}

int32_t vnodeProcessKillCompactReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVKillCompactReq req = {0};

  vDebug("vgId:%d, kill compact msg will be processed, pReq:%p, len:%d", TD_VID(pVnode), pReq, len);
  int32_t code = tDeserializeSVKillCompactReq(pReq, len, &req);
  if (code) {
    return TSDB_CODE_INVALID_MSG;
  }
  vInfo("vgId:%d, kill compact msg will be processed, compactId:%d, dnodeId:%d, vgId:%d", TD_VID(pVnode), req.compactId,
        req.dnodeId, req.vgId);

  tsdbStopAllCompTask(pVnode->pTsdb);

  pRsp->msgType = TDMT_VND_KILL_COMPACT_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

int32_t vnodeQueryCompactProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  SQueryCompactProgressReq req = {0};

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;
  SQueryCompactProgressRsp rsp = {0};

  // deserialize request
  code = tDeserializeSQueryCompactProgressReq(pMsg->pCont, pMsg->contLen, &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // query compact progress
  rsp.dnodeId = req.dnodeId;
  TAOS_UNUSED(tsdbCompMonitorGetInfo(pVnode->pTsdb, &rsp));
  vInfo("update compact progress, compactId:%d vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", rsp.compactId,
        rsp.vgId, rsp.dnodeId, rsp.numberFileset, rsp.finished);
  rsp.compactId = req.compactId;

  // serialize response
  rspSize = tSerializeSQueryCompactProgressRsp(NULL, 0, &rsp);
  if (rspSize < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    vError("rpcMallocCont %d failed", rspSize);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  code = tSerializeSQueryCompactProgressRsp(pRsp, rspSize, &rsp);
  if (code < 0) {
    goto _exit;
  }
  code = 0;

_exit:
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_QUERY_COMPACT_PROGRESS_RSP;

  tmsgSendRsp(&rspMsg);

  return 0;
}

static int64_t vnodeGetCompatableVersion(SVnode *pVnode) {
  // TODO
  return INT64_MAX;
}

extern void vnodeGetMetaPath(SVnode *pVnode, const char *metaDir, char *fname);

static int32_t vnodeCompactMetaBegin(SVnode *pVnode) {
  int32_t code = TSDB_CODE_SUCCESS;

  // Sync Commit
  code = vnodeSyncCommit(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // TODO: make sure not doing snapshot

  // remove new dir if need
  char metaCompactDir[TSDB_FILENAME_LEN] = {0};
  vnodeGetMetaPath(pVnode, VNODE_META_TMP_DIR, metaCompactDir);
  taosRemoveDir(metaCompactDir);

  // Create and open the new meta
  code = metaOpenImpl(pVnode, &pVnode->pNewMeta, VNODE_META_TMP_DIR, 0);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  vInfo("vgId:%d, compact meta begin", TD_VID(pVnode));
  return 0;
}

static int32_t vnodeCompactMetaImpl(SVnode *pVnode) {
  // Begin transfer
  int32_t code = metaBegin(pVnode->pNewMeta, META_BEGIN_HEAP_NIL);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // Do transfer
  code = metaCompact(pVnode->pMeta, pVnode->pNewMeta, vnodeGetCompatableVersion(pVnode));
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // Commit transfer
  code = metaCommit(pVnode->pNewMeta, metaGetTxn(pVnode->pNewMeta));
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  code = metaFinishCommit(pVnode->pNewMeta, metaGetTxn(pVnode->pNewMeta));
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  code = metaBegin(pVnode->pNewMeta, META_BEGIN_HEAP_NIL);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }
  return code;
}

static int32_t vnodeCompactMetaCommit(SVnode *pVnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  char    metaDir[TSDB_FILENAME_LEN] = {0};
  char    metaCompactDir[TSDB_FILENAME_LEN] = {0};
  char    metaBackupDir[TSDB_FILENAME_LEN] = {0};

  vnodeGetMetaPath(pVnode, VNODE_META_DIR, metaDir);
  vnodeGetMetaPath(pVnode, VNODE_META_TMP_DIR, metaCompactDir);
  vnodeGetMetaPath(pVnode, VNODE_META_BACKUP_DIR, metaBackupDir);

  (void)taosThreadRwlockWrlock(&pVnode->metaRWLock);

  metaClose(&pVnode->pNewMeta);
  metaClose(&pVnode->pMeta);

  // Backup the meta dir
  code = taosRenameFile(metaDir, metaBackupDir);
  if (code) {
    (void)taosThreadRwlockUnlock(&pVnode->metaRWLock);
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // Rename the meta dir
  code = taosRenameFile(metaCompactDir, metaDir);
  if (code) {
    (void)taosThreadRwlockUnlock(&pVnode->metaRWLock);
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // Remove backup dir
  taosRemoveDir(metaBackupDir);

  // Open the meta
  code = metaOpen(pVnode, &pVnode->pMeta, 0);
  if (code) {
    (void)taosThreadRwlockUnlock(&pVnode->metaRWLock);
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  (void)taosThreadRwlockUnlock(&pVnode->metaRWLock);

  // Enable write
  code = vnodeBegin(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  vInfo("vgId:%d, compact meta commit", TD_VID(pVnode));
  return 0;
}

static void vnodeCompactMetaAbort(SVnode *pVnode) {
  if (pVnode->pNewMeta) {
    metaClose(&pVnode->pNewMeta);
  }

  // Enable write
  int32_t code = vnodeBegin(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
  }

  vInfo("vgId:%d, compact meta abort", TD_VID(pVnode));
}

static int32_t vnodeCompactMeta(void *arg) {
  SVnode *pVnode = (SVnode *)arg;
  // Begin
  int32_t code = 0;

  code = vnodeCompactMetaBegin(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    goto _exit;
  }

  // Do compact
  code = vnodeCompactMetaImpl(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    vnodeCompactMetaAbort(pVnode);
    goto _exit;
  }

  // Commit
  code = vnodeCompactMetaCommit(pVnode);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    goto _exit;
  }

_exit:
  tsdbRemoveCompMonitorTask(pVnode->pTsdb, &pVnode->metaCompactTask);
  return code;
}

extern int32_t tsdbAddCompMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t compactSize);

static int32_t vnodeAsyncCompactMetaImpl(SVnode *pVnode) {
  int32_t code = 0;
  STsdb  *pTsdb = pVnode->pTsdb;

  // Check background task
  if (pTsdb->bgTaskDisabled) {
    vInfo("vgId:%d, background task is disabled, skip compact", TD_VID(pVnode));
    return 0;
  }

  // Async schedule the task
  code = vnodeAsync(COMPACT_TASK_ASYNC, EVA_PRIORITY_HIGH, vnodeCompactMeta, NULL, pVnode, &pVnode->metaCompactTask);
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  } else {
    TAOS_UNUSED(tsdbAddCompMonitorTask(pTsdb, INT32_MIN, &pVnode->metaCompactTask, 0));
  }

  return 0;
}

static int32_t vnodeAsyncCompactMeta(SVnode *pVnode) {
  STsdb  *pTsdb = pVnode->pTsdb;
  int32_t code = 0;

  TAOS_UNUSED(taosThreadMutexLock(&pTsdb->mutex));
  code = vnodeAsyncCompactMetaImpl(pVnode);
  TAOS_UNUSED(taosThreadMutexUnlock(&pTsdb->mutex));
  if (code) {
    vError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    return code;
  }

  // Wait for the task
  vnodeAWait(&pVnode->metaCompactTask);
  return 0;
}