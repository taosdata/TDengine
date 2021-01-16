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
#include <sys/inotify.h>
#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "tutil.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tsocket.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static int32_t syncGetWalVersion(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersion)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sDebug("%s, vnode is commiting while retrieve, last wver:%" PRIu64, pPeer->id, pPeer->lastWalVer);
    return -1;
  }

  pPeer->lastWalVer = wver;
  return code;
}

static bool syncIsWalModified(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersion)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sDebug("%s, vnode is commiting while retrieve, last wver:%" PRIu64, pPeer->id, pPeer->lastWalVer);
    return true;
  }

  if (wver != pPeer->lastWalVer) {
    sDebug("%s, wal is modified while retrieve, wver:%" PRIu64 ", last:%" PRIu64, pPeer->id, wver, pPeer->lastWalVer);
    return true;
  }

  return false;
}

static int32_t syncGetFileVersion(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersion)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sDebug("%s, vnode is commiting while get fver for retrieve, last fver:%" PRIu64, pPeer->id, pPeer->lastFileVer);
    return -1;
  }

  pPeer->lastFileVer = fver;
  return code;
}

static bool syncAreFilesModified(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersion)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sDebug("%s, vnode is commiting while retrieve, last fver:%" PRIu64, pPeer->id, pPeer->lastFileVer);
    pPeer->fileChanged = 1;
    return true;
  }

  if (fver != pPeer->lastFileVer) {
    sDebug("%s, files are modified while retrieve, fver:%" PRIu64 ", last:%" PRIu64, pPeer->id, fver, pPeer->lastFileVer);
    pPeer->fileChanged = 1;
    return true;
  }

  pPeer->fileChanged = 0;
  return false;
}

static int32_t syncRetrieveFile(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  fileInfo; memset(&fileInfo, 0, sizeof(SFileInfo));
  SFileAck   fileAck; memset(&fileAck, 0, sizeof(SFileAck));
  int32_t    code = -1;
  char       name[TSDB_FILENAME_LEN * 2] = {0};

  if (syncGetFileVersion(pNode, pPeer) < 0) {
    pPeer->fileChanged = 1;
    return -1;
  }

  while (1) {
    // retrieve file info
    fileInfo.name[0] = 0;
    fileInfo.size = 0;
    fileInfo.magic = (*pNode->getFileInfo)(pNode->vgId, fileInfo.name, &fileInfo.index, TAOS_SYNC_MAX_INDEX,
                                           &fileInfo.size, &fileInfo.fversion);
    syncBuildFileInfo(&fileInfo, pNode->vgId);
    sDebug("%s, file:%s info is sent, index:%d size:%" PRId64 " fver:%" PRIu64 " magic:%u", pPeer->id, fileInfo.name,
           fileInfo.index, fileInfo.size, fileInfo.fversion, fileInfo.magic);

    // send the file info
    int32_t ret = taosWriteMsg(pPeer->syncFd, &(fileInfo), sizeof(SFileInfo));
    if (ret != sizeof(SFileInfo)) {
      code = -1;
      sError("%s, failed to write file:%s info while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    // if no file anymore, break
    if (fileInfo.magic == 0 || fileInfo.name[0] == 0) {
      code = 0;
      sDebug("%s, no more files to sync", pPeer->id);
      break;
    }

    // wait for the ack from peer
    ret = taosReadMsg(pPeer->syncFd, &fileAck, sizeof(SFileAck));
    if (ret != sizeof(SFileAck)) {
      code = -1;
      sError("%s, failed to read file:%s ack while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    ret = syncCheckHead((SSyncHead*)(&fileAck));
    if (ret != 0) {
      code = -1;
      sError("%s, failed to check file:%s ack while retrieve file since %s", pPeer->id, fileInfo.name, strerror(ret));
      break;
    }

    // set the peer sync version
    pPeer->sversion = fileInfo.fversion;

    // if sync is not required, continue
    if (fileAck.sync == 0) {
      fileInfo.index++;
      sDebug("%s, %s is the same, fver:%" PRIu64, pPeer->id, fileInfo.name, fileInfo.fversion);
      continue;
    } else {
      sDebug("%s, %s will be sent, fver:%" PRIu64, pPeer->id, fileInfo.name, fileInfo.fversion);
    }

    // get the full path to file
    snprintf(name, sizeof(name), "%s/%s", pNode->path, fileInfo.name);

    // send the file to peer
    int32_t sfd = open(name, O_RDONLY);
    if (sfd < 0) {
      code = -1;
      sError("%s, failed to open file:%s while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    ret = taosSendFile(pPeer->syncFd, sfd, NULL, fileInfo.size);
    close(sfd);
    if (ret < 0) {
      code = -1;
      sError("%s, failed to send file:%s while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    sDebug("%s, file:%s is sent, size:%" PRId64, pPeer->id, fileInfo.name, fileInfo.size);
    fileInfo.index++;

    // check if processed files are modified
    if (syncAreFilesModified(pNode, pPeer)) {
      code = -1;
      break;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    sError("%s, failed to retrieve file, code:0x%x", pPeer->id, code);
  }

  return code;
}

// if only a partial record is read out, upper layer will reload the file to get a complete record
static int32_t syncReadOneWalRecord(int32_t sfd, SWalHead *pHead) {
  int32_t ret = read(sfd, pHead, sizeof(SWalHead));
  if (ret < 0) {
    sError("sfd:%d, failed to read wal head since %s, ret:%d", sfd, strerror(errno), ret);
    return -1;
  }

  if (ret == 0) {
    sDebug("sfd:%d, read to the end of file, ret:%d", sfd, ret);
    return 0;
  }

  if (ret != sizeof(SWalHead)) {
    // file is not at end yet, it shall be reloaded
    sDebug("sfd:%d, a partial wal head is read out, ret:%d", sfd, ret);
    return 0;
  }

  assert(pHead->len <= TSDB_MAX_WAL_SIZE);

  ret = read(sfd, pHead->cont, pHead->len);
  if (ret < 0) {
    sError("sfd:%d, failed to read wal content since %s, ret:%d", sfd, strerror(errno), ret);
    return -1;
  }

  if (ret != pHead->len) {
    // file is not at end yet, it shall be reloaded
    sDebug("sfd:%d, a partial wal conetnt is read out, ret:%d", sfd, ret);
    return 0;
  }

  return sizeof(SWalHead) + pHead->len;
}

static int32_t syncRetrieveLastWal(SSyncPeer *pPeer, char *name, uint64_t fversion, int64_t offset) {
  int32_t sfd = open(name, O_RDONLY);
  if (sfd < 0) {
    sError("%s, failed to open wal:%s for retrieve since:%s", pPeer->id, name, tstrerror(errno));
    return -1;
  }

  int32_t code = taosLSeek(sfd, offset, SEEK_SET);
  if (code < 0) {
    sError("%s, failed to seek %" PRId64 " in wal:%s for retrieve since:%s", pPeer->id, offset, name, tstrerror(errno));
    close(sfd);
    return -1;
  }

  sDebug("%s, retrieve last wal:%s, offset:%" PRId64 " fver:%" PRIu64, pPeer->id, name, offset, fversion);

  SWalHead *pHead = malloc(SYNC_MAX_SIZE);
  int32_t   bytes = 0;

  while (1) {
    code = syncReadOneWalRecord(sfd, pHead);
    if (code < 0) {
      sError("%s, failed to read one record from wal:%s", pPeer->id, name);
      break;
    }

    if (code == 0) {
      code = bytes;
      sDebug("%s, read to the end of wal, bytes:%d", pPeer->id, bytes);
      break;
    }

    sDebug("%s, last wal is forwarded, hver:%" PRIu64, pPeer->id, pHead->version);

    int32_t wsize = code;
    int32_t ret = taosWriteMsg(pPeer->syncFd, pHead, wsize);
    if (ret != wsize) {
      code = -1;
      sError("%s, failed to forward wal since %s, hver:%" PRIu64, pPeer->id, strerror(errno), pHead->version);
      break;
    }

    pPeer->sversion = pHead->version;
    bytes += wsize;

    if (pHead->version >= fversion && fversion > 0) {
      code = 0;
      sDebug("%s, retrieve wal finished, hver:%" PRIu64 " fver:%" PRIu64, pPeer->id, pHead->version, fversion);
      break;
    }
  }

  free(pHead);
  close(sfd);

  return code;
}

static int32_t syncProcessLastWal(SSyncPeer *pPeer, char *wname, int64_t index) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int32_t    once = 0;  // last WAL has once ever been processed
  int64_t    offset = 0;
  uint64_t   fversion = 0;
  char       fname[TSDB_FILENAME_LEN * 2] = {0};  // full path to wal file

  // get full path to wal file
  snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);
  sDebug("%s, start to retrieve last wal:%s", pPeer->id, fname);

  while (1) {
    if (syncAreFilesModified(pNode, pPeer)) return -1;
    if (syncGetWalVersion(pNode, pPeer) < 0) return -1;

    int32_t bytes = syncRetrieveLastWal(pPeer, fname, fversion, offset);
    if (bytes < 0) {
      sDebug("%s, failed to retrieve last wal", pPeer->id);
      return bytes;
    }

    // check file changes
    bool walModified = syncIsWalModified(pNode, pPeer);

    // if file is not updated or updated once, set the fversion and sstatus
    if (!walModified || once) {
      if (fversion == 0) {
        pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;  // start to forward pkt
        fversion = nodeVersion;                   // must read data to fversion
        sDebug("%s, set sstatus:%s and fver:%" PRIu64, pPeer->id, syncStatus[pPeer->sstatus], fversion);
      }
    }

    // if all data up to fversion is read out, it is over
    if (pPeer->sversion >= fversion && fversion > 0) {
      sDebug("%s, data up to fver:%" PRIu64 " has been read out, bytes:%d sver:%" PRIu64, pPeer->id, fversion, bytes,
             pPeer->sversion);
      return 0;
    }

    // if all data are read out, and no update
    if (bytes == 0 && !walModified) {
      // wal not closed, it means some data not flushed to disk, wait for a while
      usleep(10000);
    }

    // if bytes > 0, file is updated, or fversion is not reached but file still open, read again
    once = 1;
    offset += bytes;
    sDebug("%s, continue retrieve last wal, bytes:%d offset:%" PRId64 " sver:%" PRIu64 " fver:%" PRIu64, pPeer->id,
           bytes, offset, pPeer->sversion, fversion);
  }

  return -1;
}

static int32_t syncRetrieveWal(SSyncPeer *pPeer) {
  SSyncNode * pNode = pPeer->pSyncNode;
  char        fname[TSDB_FILENAME_LEN * 3];
  char        wname[TSDB_FILENAME_LEN * 2];
  int32_t     size;
  int32_t     code = -1;
  int64_t     index = 0;

  while (1) {
    // retrieve wal info
    wname[0] = 0;
    code = (*pNode->getWalInfo)(pNode->vgId, wname, &index);
    if (code < 0) {
      sError("%s, failed to get wal info since:%s, code:0x%x", pPeer->id, strerror(errno), code);
      break;
    }

    if (wname[0] == 0) {  // no wal file
      code = 0;
      sDebug("%s, no wal file anymore", pPeer->id);
      break;
    }

    if (code == 0) {  // last wal
      code = syncProcessLastWal(pPeer, wname, index);
      break;
    }

    // get the full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);

    // send wal file, old wal file won't be modified, even remove is ok
    struct stat fstat;
    if (stat(fname, &fstat) < 0) {
      code = -1;
      sDebug("%s, failed to stat wal:%s for retrieve since %s, code:0x%x", pPeer->id, fname, strerror(errno), code);
      break;
    }

    size = fstat.st_size;
    sDebug("%s, retrieve wal:%s size:%d", pPeer->id, fname, size);

    int32_t sfd = open(fname, O_RDONLY);
    if (sfd < 0) {
      code = -1;
      sError("%s, failed to open wal:%s for retrieve since %s, code:0x%x", pPeer->id, fname, strerror(errno), code);
      break;
    }

    code = taosSendFile(pPeer->syncFd, sfd, NULL, size);
    close(sfd);
    if (code < 0) {
      sError("%s, failed to send wal:%s for retrieve since %s, code:0x%x", pPeer->id, fname, strerror(errno), code);
      break;
    }

    if (syncAreFilesModified(pNode, pPeer)) {
      code = -1;
      break;
    }
  }

  if (code == 0) {
    SWalHead walHead;
    memset(&walHead, 0, sizeof(walHead));
    if (taosWriteMsg(pPeer->syncFd, &walHead, sizeof(walHead)) == sizeof(walHead)) {
      pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;
      sInfo("%s, wal retrieve is finished, set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
    } else {
      sError("%s, failed to send last wal record since %s", pPeer->id, strerror(errno));
      code = -1;
    }
  } else {
    sError("%s, failed to send wal since %s, code:0x%x", pPeer->id, strerror(errno), code);
  }

  return code;
}

static int32_t syncRetrieveFirstPkt(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  SSyncMsg msg;
  syncBuildSyncDataMsg(&msg, pNode->vgId);

  if (taosWriteMsg(pPeer->syncFd, &msg, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sError("%s, failed to send sync-data msg since %s, tranId:%u", pPeer->id, strerror(errno), msg.tranId);
    return -1;
  }
  sDebug("%s, send sync-data msg to peer, tranId:%u", pPeer->id, msg.tranId);

  SSyncRsp rsp;
  if (taosReadMsg(pPeer->syncFd, &rsp, sizeof(SSyncRsp)) != sizeof(SSyncRsp)) {
    sError("%s, failed to read sync-data rsp since %s, tranId:%u", pPeer->id, strerror(errno), msg.tranId);
    return -1;
  }

  sDebug("%s, recv sync-data rsp from peer, tranId:%u rsp-tranId:%u", pPeer->id, msg.tranId, rsp.tranId);
  return 0;
}

static int32_t syncRetrieveDataStepByStep(SSyncPeer *pPeer) {
  sInfo("%s, start to retrieve, sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
  if (syncRetrieveFirstPkt(pPeer) < 0) {
    sError("%s, failed to start retrieve", pPeer->id);
    return -1;
  }

  pPeer->sversion = 0;
  pPeer->sstatus = TAOS_SYNC_STATUS_FILE;
  sInfo("%s, start to retrieve files, set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
  if (syncRetrieveFile(pPeer) != 0) {
    sError("%s, failed to retrieve files", pPeer->id);
    return -1;
  }

  // if no files are synced, there must be wal to sync, sversion must be larger than one
  if (pPeer->sversion == 0) pPeer->sversion = 1;

  sInfo("%s, start to retrieve wals", pPeer->id);
  int32_t code = syncRetrieveWal(pPeer);
  if (code != 0) {
    sError("%s, failed to retrieve wals, code:0x%x", pPeer->id, code);
    return -1;
  }

  return 0;
}

void *syncRetrieveData(void *param) {
  int64_t    rid = (int64_t)param;
  SSyncPeer *pPeer = syncAcquirePeer(rid);
  if (pPeer == NULL) {
    sError("failed to retrieve data, invalid peer rid:%" PRId64, rid);
    return NULL;
  }

  SSyncNode *pNode = pPeer->pSyncNode;

  taosBlockSIGPIPE();
  sInfo("%s, start to retrieve data, sstatus:%s, numOfRetrieves:%d", pPeer->id, syncStatus[pPeer->sstatus],
        pPeer->numOfRetrieves);

  if (pNode->notifyFlowCtrl) (*pNode->notifyFlowCtrl)(pNode->vgId, pPeer->numOfRetrieves);

  pPeer->syncFd = taosOpenTcpClientSocket(pPeer->ip, pPeer->port, 0);
  if (pPeer->syncFd < 0) {
    sError("%s, failed to open socket to sync", pPeer->id);
  } else {
    sInfo("%s, sync tcp is setup", pPeer->id);

    if (syncRetrieveDataStepByStep(pPeer) == 0) {
      sInfo("%s, sync retrieve process is successful", pPeer->id);
    } else {
      sError("%s, failed to retrieve data, restart connection", pPeer->id);
      syncRestartConnection(pPeer);
    }
  }

  if (pPeer->fileChanged) {
    pPeer->numOfRetrieves++;
  } else {
    pPeer->numOfRetrieves = 0;
    // if (pNode->notifyFlowCtrl) (*pNode->notifyFlowCtrl)(pNode->vgId, 0);
  }

  if (pNode->notifyFlowCtrl) (*pNode->notifyFlowCtrl)(pNode->vgId, 0);

  pPeer->fileChanged = 0;
  taosClose(pPeer->syncFd);

  // The ref is obtained in both the create thread and the current thread, so it is released twice
  sInfo("%s, sync retrieve data over, sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);

  syncReleasePeer(pPeer);
  syncReleasePeer(pPeer);

  return NULL;
}
