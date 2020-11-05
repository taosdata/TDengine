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

#include "os.h"
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
    magic = (*pNode->getFileInfo)(pNode->ahandle, name, &index, eindex, &size, &fversion);
    if (magic == 0) break;

    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, name);
    (void)remove(fname);
    sDebug("%s, %s is removed", pPeer->id, fname);

    index++;
    if (index > eindex) break;
  }
}

static int syncRestoreFile(SSyncPeer *pPeer, uint64_t *fversion) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  minfo; memset(&minfo, 0, sizeof(minfo)); /* = {0}; */  // master file info
  SFileInfo  sinfo; memset(&sinfo, 0, sizeof(sinfo)); /* = {0}; */  // slave file info
  SFileAck   fileAck; 
  int        code = -1;
  char       name[TSDB_FILENAME_LEN * 2] = {0};
  uint32_t   pindex = 0;    // index in last restore
  bool       fileChanged = false;

  *fversion = 0;
  sinfo.index = 0;
  while (1) {
    // read file info
    int ret = taosReadMsg(pPeer->syncFd, &(minfo), sizeof(minfo));
    if (ret < 0) break;

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
    sDebug("%s, get file info:%s", pPeer->id, minfo.name);
    sinfo.magic = (*pNode->getFileInfo)(pNode->ahandle, sinfo.name, &sinfo.index, TAOS_SYNC_MAX_INDEX, &sinfo.size,
                                        &sinfo.fversion);

    // if file not there or magic is not the same, file shall be synced
    memset(&fileAck, 0, sizeof(fileAck));
    fileAck.sync = (sinfo.magic != minfo.magic || sinfo.name[0] == 0) ? 1 : 0;

    // send file ack
    ret = taosWriteMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret < 0) break;

    // if sync is not required, continue
    if (fileAck.sync == 0) {
      sDebug("%s, %s is the same", pPeer->id, minfo.name);
      continue;
    }

    // if sync is required, open file, receive from master, and write to file
    // get the full path to file
    minfo.name[sizeof(minfo.name) - 1] = 0;
    snprintf(name, sizeof(name), "%s/%s", pNode->path, minfo.name);

    int dfd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (dfd < 0) {
      sError("%s, failed to open file:%s", pPeer->id, name);
      break;
    }

    ret = taosCopyFds(pPeer->syncFd, dfd, minfo.size);
    fsync(dfd);
    close(dfd);
    if (ret < 0) break;

    fileChanged = true;
    sDebug("%s, %s is received, size:%" PRId64, pPeer->id, minfo.name, minfo.size);
  }

  if (code == 0 && fileChanged) {
    // data file is changed, code shall be set to 1
    *fversion = minfo.fversion;
    code = 1;
  }

  if (code < 0) {
    sError("%s, failed to restore %s(%s)", pPeer->id, name, strerror(errno));
  }

  return code;
}

static int syncRestoreWal(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int        ret, code = -1;

  void *buffer = calloc(1024000, 1);  // size for one record
  if (buffer == NULL) return -1;

  SWalHead *pHead = (SWalHead *)buffer;

  while (1) {
    ret = taosReadMsg(pPeer->syncFd, pHead, sizeof(SWalHead));
    if (ret < 0) break;

    if (pHead->len == 0) {
      code = 0;
      break;
    }  // wal sync over

    ret = taosReadMsg(pPeer->syncFd, pHead->cont, pHead->len);
    if (ret < 0) break;

    sDebug("%s, restore a record, ver:%" PRIu64, pPeer->id, pHead->version);
    (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_WAL, NULL);
  }

  if (code < 0) {
    sError("%s, failed to restore wal(%s)", pPeer->id, strerror(errno));
  }

  free(buffer);
  return code;
}

static char *syncProcessOneBufferedFwd(SSyncPeer *pPeer, char *offset) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SWalHead * pHead = (SWalHead *)offset;

  (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_FWD, NULL);
  offset += pHead->len + sizeof(SWalHead);

  return offset;
}

static int syncProcessBufferedFwd(SSyncPeer *pPeer) {
  SSyncNode *  pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;
  int          forwards = 0;

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

int syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead) {
  SSyncNode *  pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;

  if (pRecv == NULL) return -1;
  int len = pHead->len + sizeof(SWalHead);

  if (pRecv->bufferSize - (pRecv->offset - pRecv->buffer) >= len) {
    memcpy(pRecv->offset, pHead, len);
    pRecv->offset += len;
    pRecv->forwards++;
    sDebug("%s, fwd is saved into queue, ver:%" PRIu64 " fwds:%d", pPeer->id, pHead->version, pRecv->forwards);
  } else {
    sError("%s, buffer size:%d is too small", pPeer->id, pRecv->bufferSize);
    pRecv->code = -1;  // set error code
  }

  return pRecv->code;
}

static void syncCloseRecvBuffer(SSyncNode *pNode) {
  if (pNode->pRecv) {
    taosTFree(pNode->pRecv->buffer);
  }

  taosTFree(pNode->pRecv);
}

static int syncOpenRecvBuffer(SSyncNode *pNode) {
  syncCloseRecvBuffer(pNode);

  SRecvBuffer *pRecv = calloc(sizeof(SRecvBuffer), 1);
  if (pRecv == NULL) return -1;

  pRecv->bufferSize = 5000000;
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

static int syncRestoreDataStepByStep(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  nodeSStatus = TAOS_SYNC_STATUS_FILE;
  uint64_t fversion = 0;

  sDebug("%s, start to restore file", pPeer->id);
  int code = syncRestoreFile(pPeer, &fversion);
  if (code < 0) {
    sError("%s, failed to restore file", pPeer->id);
    return -1;
  }

  // if code > 0, data file is changed, notify app, and pass the version
  if (code > 0 && pNode->notifyFileSynced) {
    if ((*pNode->notifyFileSynced)(pNode->ahandle, fversion) < 0) {
      sError("%s, app not in ready state", pPeer->id);
      return -1;
    }
  }

  nodeVersion = fversion;

  sDebug("%s, start to restore wal", pPeer->id);
  if (syncRestoreWal(pPeer) < 0) {
    sError("%s, failed to restore wal", pPeer->id);
    return -1;
  }

  nodeSStatus = TAOS_SYNC_STATUS_CACHE;
  sDebug("%s, start to insert buffered points", pPeer->id);
  if (syncProcessBufferedFwd(pPeer) < 0) {
    sError("%s, failed to insert buffered points", pPeer->id);
    return -1;
  }

  return 0;
}

void *syncRestoreData(void *param) {
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncNode *pNode = pPeer->pSyncNode;

  taosBlockSIGPIPE();
  __sync_fetch_and_add(&tsSyncNum, 1);

  (*pNode->notifyRole)(pNode->ahandle, TAOS_SYNC_ROLE_SYNCING);

  if (syncOpenRecvBuffer(pNode) < 0) {
    sError("%s, failed to allocate recv buffer", pPeer->id);
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

  (*pNode->notifyRole)(pNode->ahandle, nodeRole);

  nodeSStatus = TAOS_SYNC_STATUS_INIT;
  taosClose(pPeer->syncFd);
  syncCloseRecvBuffer(pNode);
  __sync_fetch_and_sub(&tsSyncNum, 1);
  syncDecPeerRef(pPeer);

  return NULL;
}
