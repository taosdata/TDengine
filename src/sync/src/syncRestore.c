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
#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tsocket.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static void syncRemoveExtraFile(SSyncPeer *pPeer, int32_t sindex, int32_t eindex) {
  char       name[TSDB_FILENAME_LEN * 2] = {0};
  char       fname[TSDB_FILENAME_LEN * 3] = {0};
  uint32_t   magic;
  uint64_t   fversion;
  int64_t    size;
  uint32_t   index = sindex;
  SSyncNode *pNode = pPeer->pSyncNode;

  if (sindex < 0 || eindex < sindex) return;

  while (1) {
    name[0] = 0;
    magic = (*pNode->getFileInfo)(pNode->vgId, name, &index, eindex, &size, &fversion);
    if (magic == 0) break;

    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, name);
    (void)remove(fname);
    sInfo("%s, %s is removed for its extra", pPeer->id, fname);

    index++;
    if (index > eindex) break;
  }
}

static int32_t syncRestoreFile(SSyncPeer *pPeer, uint64_t *fversion) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  minfo; memset(&minfo, 0, sizeof(SFileInfo)); /* = {0}; */
  SFileInfo  sinfo; memset(&sinfo, 0, sizeof(SFileInfo)); /* = {0}; */
  SFileAck   fileAck = {0};
  int32_t    code = -1;
  char       name[TSDB_FILENAME_LEN * 2] = {0};
  uint32_t   pindex = 0;  // index in last restore
  bool       fileChanged = false;

  *fversion = 0;
  sinfo.index = 0;
  while (1) {
    // read file info
    int32_t ret = taosReadMsg(pPeer->syncFd, &(minfo), sizeof(minfo));
    if (ret < 0) {
      sError("%s, failed to read file info while restore file since %s", pPeer->id, strerror(errno));
      break;
    }

    // if no more file from master, break;
    if (minfo.name[0] == 0 || minfo.magic == 0) {
      sDebug("%s, no more files to restore", pPeer->id);

      // remove extra files after the current index
      syncRemoveExtraFile(pPeer, sinfo.index + 1, TAOS_SYNC_MAX_INDEX);
      code = 0;
      break;
    }

    // remove extra files on slave between the current and last index
    syncRemoveExtraFile(pPeer, pindex + 1, minfo.index - 1);
    pindex = minfo.index;

    // check the file info
    sinfo = minfo;
    sDebug("%s, get file:%s info size:%" PRId64, pPeer->id, minfo.name, minfo.size);
    sinfo.magic = (*pNode->getFileInfo)(pNode->vgId, sinfo.name, &sinfo.index, TAOS_SYNC_MAX_INDEX, &sinfo.size,
                                        &sinfo.fversion);

    // if file not there or magic is not the same, file shall be synced
    memset(&fileAck, 0, sizeof(fileAck));
    fileAck.sync = (sinfo.magic != minfo.magic || sinfo.name[0] == 0) ? 1 : 0;

    // send file ack
    ret = taosWriteMsg(pPeer->syncFd, &fileAck, sizeof(fileAck));
    if (ret < 0) {
      sError("%s, failed to write file:%s ack while restore file since %s", pPeer->id, minfo.name, strerror(errno));
      break;
    }

    // if sync is not required, continue
    if (fileAck.sync == 0) {
      sDebug("%s, %s is the same", pPeer->id, minfo.name);
      continue;
    }

    // if sync is required, open file, receive from master, and write to file
    // get the full path to file
    minfo.name[sizeof(minfo.name) - 1] = 0;
    snprintf(name, sizeof(name), "%s/%s", pNode->path, minfo.name);

    int32_t dfd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (dfd < 0) {
      sError("%s, failed to open file:%s while restore file since %s", pPeer->id, minfo.name, strerror(errno));
      break;
    }

    ret = taosCopyFds(pPeer->syncFd, dfd, minfo.size);
    fsync(dfd);
    close(dfd);
    if (ret < 0) {
      sError("%s, failed to copy file:%s while restore file since %s", pPeer->id, minfo.name, strerror(errno));
      break;
    }

    fileChanged = true;
    sDebug("%s, %s is received, size:%" PRId64, pPeer->id, minfo.name, minfo.size);
  }

  if (code == 0 && fileChanged) {
    // data file is changed, code shall be set to 1
    *fversion = minfo.fversion;
    code = 1;
    sDebug("%s, file changed after restore file, fver:%" PRIu64, pPeer->id, *fversion);
  }

  if (code < 0) {
    sError("%s, failed to restore %s since %s", pPeer->id, name, strerror(errno));
  }

  return code;
}

static int32_t syncRestoreWal(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int32_t    ret, code = -1;
  uint64_t   lastVer = 0;

  SWalHead *pHead = calloc(SYNC_MAX_SIZE, 1);  // size for one record
  if (pHead == NULL) return -1;

  while (1) {
    ret = taosReadMsg(pPeer->syncFd, pHead, sizeof(SWalHead));
    if (ret < 0) {
      sError("%s, failed to read walhead while restore wal since %s", pPeer->id, strerror(errno));
      break;
    }

    if (pHead->len == 0) {
      sDebug("%s, wal is synced over, last wver:%" PRIu64, pPeer->id, lastVer);
      code = 0;
      break;
    }  // wal sync over

    ret = taosReadMsg(pPeer->syncFd, pHead->cont, pHead->len);
    if (ret < 0) {
      sError("%s, failed to read walcont, len:%d while restore wal since %s", pPeer->id, pHead->len, strerror(errno));
      break;
    }

    sTrace("%s, restore a record, qtype:wal len:%d hver:%" PRIu64, pPeer->id, pHead->len, pHead->version);

    if (lastVer == pHead->version) {
      sError("%s, failed to restore record, same hver:%" PRIu64 ", wal sync failed" PRIu64, pPeer->id, lastVer);
      break;
    }
    lastVer = pHead->version;

    (*pNode->writeToCache)(pNode->vgId, pHead, TAOS_QTYPE_WAL, NULL);
  }

  if (code < 0) {
    sError("%s, failed to restore wal from syncFd:%d since %s", pPeer->id, pPeer->syncFd, strerror(errno));
  }

  free(pHead);
  return code;
}

static char *syncProcessOneBufferedFwd(SSyncPeer *pPeer, char *offset) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SWalHead * pHead = (SWalHead *)offset;

  (*pNode->writeToCache)(pNode->vgId, pHead, TAOS_QTYPE_FWD, NULL);
  offset += pHead->len + sizeof(SWalHead);

  return offset;
}

static int32_t syncProcessBufferedFwd(SSyncPeer *pPeer) {
  SSyncNode *  pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;
  int32_t      forwards = 0;

  sDebug("%s, number of buffered forwards:%d", pPeer->id, pRecv->forwards);

  char *offset = pRecv->buffer;
  while (forwards < pRecv->forwards) {
    offset = syncProcessOneBufferedFwd(pPeer, offset);
    forwards++;
  }

  pthread_mutex_lock(&pNode->mutex);

  while (forwards < pRecv->forwards && pRecv->code == 0) {
    offset = syncProcessOneBufferedFwd(pPeer, offset);
    forwards++;
  }

  nodeRole = TAOS_SYNC_ROLE_SLAVE;
  sDebug("%s, finish processing buffered fwds:%d", pPeer->id, forwards);

  pthread_mutex_unlock(&pNode->mutex);

  return pRecv->code;
}

int32_t syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead) {
  SSyncNode *  pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;
  int32_t len = pHead->len + sizeof(SWalHead);

  if (pRecv == NULL) {
    sError("%s, recv buffer is not create yet", pPeer->id);
    return -1;
  }

  if (pRecv->bufferSize - (pRecv->offset - pRecv->buffer) >= len) {
    memcpy(pRecv->offset, pHead, len);
    pRecv->offset += len;
    pRecv->forwards++;
    sTrace("%s, fwd is saved into queue, hver:%" PRIu64 " fwds:%d", pPeer->id, pHead->version, pRecv->forwards);
  } else {
    sError("%s, buffer size:%d is too small", pPeer->id, pRecv->bufferSize);
    pRecv->code = -1;  // set error code
  }

  return pRecv->code;
}

static void syncCloseRecvBuffer(SSyncNode *pNode) {
  if (pNode->pRecv) {
    tfree(pNode->pRecv->buffer);
  }

  tfree(pNode->pRecv);
}

static int32_t syncOpenRecvBuffer(SSyncNode *pNode) {
  syncCloseRecvBuffer(pNode);

  SRecvBuffer *pRecv = calloc(sizeof(SRecvBuffer), 1);
  if (pRecv == NULL) return -1;

  pRecv->bufferSize = SYNC_RECV_BUFFER_SIZE;
  pRecv->buffer = malloc(pRecv->bufferSize);
  if (pRecv->buffer == NULL) {
    free(pRecv);
    return -1;
  }

  pRecv->offset = pRecv->buffer;
  pRecv->forwards = 0;

  pNode->pRecv = pRecv;

  return 0;
}

static int32_t syncRestoreDataStepByStep(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  nodeSStatus = TAOS_SYNC_STATUS_FILE;
  uint64_t fversion = 0;

  sInfo("%s, start to restore, sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
  SFirstPktRsp firstPktRsp = {.sync = 1};
  if (taosWriteMsg(pPeer->syncFd, &firstPktRsp, sizeof(SFirstPktRsp)) < 0) {
    sError("%s, failed to send sync firstPkt rsp since %s", pPeer->id, strerror(errno));
    return -1;
  }

  sInfo("%s, start to restore file, set sstatus:%s", pPeer->id, syncStatus[nodeSStatus]);
  int32_t code = syncRestoreFile(pPeer, &fversion);
  if (code < 0) {
    sError("%s, failed to restore file", pPeer->id);
    return -1;
  }

  // if code > 0, data file is changed, notify app, and pass the version
  if (code > 0 && pNode->notifyFileSynced) {
    if ((*pNode->notifyFileSynced)(pNode->vgId, fversion) < 0) {
      sError("%s, app not in ready state", pPeer->id);
      return -1;
    }
  }

  nodeVersion = fversion;

  sInfo("%s, start to restore wal", pPeer->id);
  if (syncRestoreWal(pPeer) < 0) {
    sError("%s, failed to restore wal", pPeer->id);
    return -1;
  }

  nodeSStatus = TAOS_SYNC_STATUS_CACHE;
  sInfo("%s, start to insert buffered points, set sstatus:%s", pPeer->id, syncStatus[nodeSStatus]);
  if (syncProcessBufferedFwd(pPeer) < 0) {
    sError("%s, failed to insert buffered points", pPeer->id);
    return -1;
  }

  return 0;
}

void *syncRestoreData(void *param) {
  SSyncPeer *pPeer = param;
  SSyncNode *pNode = pPeer->pSyncNode;

  taosBlockSIGPIPE();
  __sync_fetch_and_add(&tsSyncNum, 1);

  (*pNode->notifyRole)(pNode->vgId, TAOS_SYNC_ROLE_SYNCING);

  if (syncOpenRecvBuffer(pNode) < 0) {
    sError("%s, failed to allocate recv buffer, restart connection", pPeer->id);
    syncRestartConnection(pPeer);
  } else {
    if (syncRestoreDataStepByStep(pPeer) == 0) {
      sInfo("%s, it is synced successfully", pPeer->id);
      nodeRole = TAOS_SYNC_ROLE_SLAVE;
      syncBroadcastStatus(pNode);
    } else {
      sError("%s, failed to restore data, restart connection", pPeer->id);
      nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
      syncRestartConnection(pPeer);
    }
  }

  (*pNode->notifyRole)(pNode->vgId, nodeRole);

  nodeSStatus = TAOS_SYNC_STATUS_INIT;
  sInfo("%s, sync over, set sstatus:%s", pPeer->id, syncStatus[nodeSStatus]);

  taosClose(pPeer->syncFd);
  syncCloseRecvBuffer(pNode);
  __sync_fetch_and_sub(&tsSyncNum, 1);
  syncDecPeerRef(pPeer);

  return NULL;
}
