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
#include "tglobal.h"
#include "ttimer.h"
#include "tsocket.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static int32_t syncGetWalVersion(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersionFp)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sInfo("%s, vnode is commiting while retrieve, last wver:%" PRIu64, pPeer->id, pPeer->lastWalVer);
    return -1;
  }

  pPeer->lastWalVer = wver;
  return code;
}

static bool syncIsWalModified(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersionFp)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sInfo("%s, vnode is commiting while retrieve, last wver:%" PRIu64, pPeer->id, pPeer->lastWalVer);
    return true;
  }

  if (wver != pPeer->lastWalVer) {
    sInfo("%s, wal is modified while retrieve, wver:%" PRIu64 ", last:%" PRIu64, pPeer->id, wver, pPeer->lastWalVer);
    return true;
  }

  return false;
}

static int32_t syncGetFileVersion(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersionFp)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sInfo("%s, vnode is commiting while get fver for retrieve, last fver:%" PRIu64, pPeer->id, pPeer->lastFileVer);
    return -1;
  }

  pPeer->lastFileVer = fver;
  return code;
}

static bool syncAreFilesModified(SSyncNode *pNode, SSyncPeer *pPeer) {
  uint64_t fver, wver;
  int32_t  code = (*pNode->getVersionFp)(pNode->vgId, &fver, &wver);
  if (code != 0) {
    sInfo("%s, vnode is commiting while retrieve, last fver:%" PRIu64, pPeer->id, pPeer->lastFileVer);
    pPeer->fileChanged = 1;
    return true;
  }

  if (fver != pPeer->lastFileVer) {
    sInfo("%s, files are modified while retrieve, fver:%" PRIu64 ", last:%" PRIu64, pPeer->id, fver, pPeer->lastFileVer);
    pPeer->fileChanged = 1;
    return true;
  }

  pPeer->fileChanged = 0;
  return false;
}

static int32_t syncSendFileVersion(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  SFileVersion fileVersion;
  memset(&fileVersion, 0, sizeof(SFileVersion));
  syncBuildFileVersion(&fileVersion, pNode->vgId);

  uint64_t fver = pPeer->lastFileVer;
  fileVersion.fversion = htobe64(fver);
  int32_t ret = taosWriteMsg(pPeer->syncFd, &fileVersion, sizeof(SFileVersion));
  if (ret != sizeof(SFileVersion)) {
    sError("%s, failed to write fver:%" PRIu64 " since %s", pPeer->id, fver, strerror(errno));
    return -1;
  }

  SFileAck fileAck;
  memset(&fileAck, 0, sizeof(SFileAck));
  ret = taosReadMsg(pPeer->syncFd, &fileAck, sizeof(SFileAck));
  if (ret != sizeof(SFileAck)) {
    sError("%s, failed to read fver ack since %s", pPeer->id, strerror(errno));
    return -1;
  }

  // set the peer sync version
  pPeer->sversion = fver;

  return 0;
}

static int32_t syncRetrieveFile(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  if (syncGetFileVersion(pNode, pPeer) < 0) {
    pPeer->fileChanged = 1;
    return -1;
  }

  if (pNode->sendFileFp && (*pNode->sendFileFp)(pNode->pTsdb, pPeer->syncFd) != 0) {
    sError("%s, failed to retrieve file", pPeer->id);
    return -1;
  }

  if (syncSendFileVersion(pPeer) < 0) {
    return -1;
  }

  sInfo("%s, all files are retrieved", pPeer->id);
  return 0;
}

// if only a partial record is read out, upper layer will reload the file to get a complete record
static int32_t syncReadOneWalRecord(int32_t sfd, SWalHead *pHead) {
  int32_t ret = read(sfd, pHead, sizeof(SWalHead));
  if (ret < 0) {
    sError("sfd:%d, failed to read wal head since %s, ret:%d", sfd, strerror(errno), ret);
    return -1;
  }

  if (ret == 0) {
    sInfo("sfd:%d, read to the end of file, ret:%d", sfd, ret);
    return 0;
  }

  if (ret != sizeof(SWalHead)) {
    // file is not at end yet, it shall be reloaded
    sInfo("sfd:%d, a partial wal head is read out, ret:%d", sfd, ret);
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
    sInfo("sfd:%d, a partial wal conetnt is read out, ret:%d", sfd, ret);
    return 0;
  }

  return sizeof(SWalHead) + pHead->len;
}

static int64_t syncRetrieveLastWal(SSyncPeer *pPeer, char *name, uint64_t fversion, int64_t offset) {
  int32_t sfd = open(name, O_RDONLY | O_BINARY);
  if (sfd < 0) {
    sError("%s, failed to open wal:%s for retrieve since:%s", pPeer->id, name, tstrerror(errno));
    return -1;
  }

  int64_t code = taosLSeek(sfd, offset, SEEK_SET);
  if (code < 0) {
    sError("%s, failed to seek %" PRId64 " in wal:%s for retrieve since:%s", pPeer->id, offset, name, tstrerror(errno));
    close(sfd);
    return -1;
  }

  sInfo("%s, retrieve last wal:%s, offset:%" PRId64 " fver:%" PRIu64, pPeer->id, name, offset, fversion);

  SWalHead *pHead = malloc(SYNC_MAX_SIZE);
  int64_t   bytes = 0;

  while (1) {
    code = syncReadOneWalRecord(sfd, pHead);
    if (code < 0) {
      sError("%s, failed to read one record from wal:%s", pPeer->id, name);
      break;
    }

    if (code == 0) {
      code = bytes;
      sInfo("%s, read to the end of wal, bytes:%" PRId64, pPeer->id, bytes);
      break;
    }

    sTrace("%s, last wal is forwarded, hver:%" PRIu64, pPeer->id, pHead->version);

    int32_t wsize = (int32_t)code;
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
      sInfo("%s, retrieve wal finished, hver:%" PRIu64 " fver:%" PRIu64, pPeer->id, pHead->version, fversion);
      break;
    }
  }

  free(pHead);
  close(sfd);

  return code;
}

static int64_t syncProcessLastWal(SSyncPeer *pPeer, char *wname, int64_t index) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int32_t    once = 0;  // last WAL has once ever been processed
  int64_t    offset = 0;
  uint64_t   fversion = 0;
  char       fname[TSDB_FILENAME_LEN * 2] = {0};  // full path to wal file

  // get full path to wal file
  snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);
  sInfo("%s, start to retrieve last wal:%s", pPeer->id, fname);

  while (1) {
    if (syncAreFilesModified(pNode, pPeer)) return -1;
    if (syncGetWalVersion(pNode, pPeer) < 0) return -1;

    int64_t bytes = syncRetrieveLastWal(pPeer, fname, fversion, offset);
    if (bytes < 0) {
      sInfo("%s, failed to retrieve last wal, bytes:%" PRId64, pPeer->id, bytes);
      return bytes;
    }

    // check file changes
    bool walModified = syncIsWalModified(pNode, pPeer);

    // if file is not updated or updated once, set the fversion and sstatus
    if (!walModified || once) {
      if (fversion == 0) {
        pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;  // start to forward pkt
        fversion = nodeVersion;                   // must read data to fversion
        sInfo("%s, set sstatus:%s and fver:%" PRIu64, pPeer->id, syncStatus[pPeer->sstatus], fversion);
      }
    }

    // if all data up to fversion is read out, it is over
    if (pPeer->sversion >= fversion && fversion > 0) {
      sInfo("%s, data up to fver:%" PRIu64 " has been read out, bytes:%" PRId64 " sver:%" PRIu64, pPeer->id, fversion, bytes,
             pPeer->sversion);
      return 0;
    }

    // if all data are read out, and no update
    if (bytes == 0 && !walModified) {
      // wal not closed, it means some data not flushed to disk, wait for a while
      taosMsleep(10);
    }

    // if bytes > 0, file is updated, or fversion is not reached but file still open, read again
    once = 1;
    offset += bytes;
    sInfo("%s, continue retrieve last wal, bytes:%" PRId64 " offset:%" PRId64 " sver:%" PRIu64 " fver:%" PRIu64, pPeer->id,
           bytes, offset, pPeer->sversion, fversion);
  }

  return -1;
}

static int64_t syncRetrieveWal(SSyncPeer *pPeer) {
  SSyncNode * pNode = pPeer->pSyncNode;
  char        fname[TSDB_FILENAME_LEN * 3];
  char        wname[TSDB_FILENAME_LEN * 2];
  int32_t     size;
  int64_t     code = -1;
  int64_t     index = 0;

  while (1) {
    // retrieve wal info
    wname[0] = 0;
    code = (*pNode->getWalInfoFp)(pNode->vgId, wname, &index);
    if (code < 0) {
      sError("%s, failed to get wal info since:%s, code:0x%" PRIx64, pPeer->id, strerror(errno), code);
      break;
    }

    if (wname[0] == 0) {  // no wal file
      code = 0;
      sInfo("%s, no wal file anymore", pPeer->id);
      break;
    }

    if (code == 0) {  // last wal
      code = syncProcessLastWal(pPeer, wname, index);
      sInfo("%s, last wal processed, code:%" PRId64, pPeer->id, code);
      break;
    }

    // get the full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);

    // send wal file, old wal file won't be modified, even remove is ok
    struct stat fstat;
    if (stat(fname, &fstat) < 0) {
      code = -1;
      sInfo("%s, failed to stat wal:%s for retrieve since %s, code:0x%" PRIx64, pPeer->id, fname, strerror(errno), code);
      break;
    }

    size = fstat.st_size;
    sInfo("%s, retrieve wal:%s size:%d", pPeer->id, fname, size);

    int32_t sfd = open(fname, O_RDONLY | O_BINARY);
    if (sfd < 0) {
      code = -1;
      sError("%s, failed to open wal:%s for retrieve since %s, code:0x%" PRIx64, pPeer->id, fname, strerror(errno), code);
      break;
    }

    code = (int32_t)taosSendFile(pPeer->syncFd, sfd, NULL, size);
    close(sfd);
    if (code < 0) {
      sError("%s, failed to send wal:%s for retrieve since %s, code:0x%" PRIx64, pPeer->id, fname, strerror(errno), code);
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
    sError("%s, failed to send wal since %s, code:0x%" PRIx64, pPeer->id, strerror(errno), code);
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
  sInfo("%s, send sync-data msg to peer, tranId:%u", pPeer->id, msg.tranId);

  SSyncRsp rsp;
  if (taosReadMsg(pPeer->syncFd, &rsp, sizeof(SSyncRsp)) != sizeof(SSyncRsp)) {
    sError("%s, failed to read sync-data rsp since %s, tranId:%u", pPeer->id, strerror(errno), msg.tranId);
    return -1;
  }

  sInfo("%s, recv sync-data rsp from peer, tranId:%u rsp-tranId:%u", pPeer->id, msg.tranId, rsp.tranId);
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
  int64_t code = syncRetrieveWal(pPeer);
  if (code < 0) {
    sError("%s, failed to retrieve wals, code:0x%" PRIx64, pPeer->id, code);
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

  if (pNode->notifyFlowCtrlFp) (*pNode->notifyFlowCtrlFp)(pNode->vgId, pPeer->numOfRetrieves);

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
    // if (pNode->notifyFlowCtrlFp) (*pNode->notifyFlowCtrlFp)(pNode->vgId, 0);
  }

  if (pNode->notifyFlowCtrlFp) (*pNode->notifyFlowCtrlFp)(pNode->vgId, 0);

  pPeer->fileChanged = 0;
  taosCloseSocket(pPeer->syncFd);

  // The ref is obtained in both the create thread and the current thread, so it is released twice
  sInfo("%s, sync retrieve data over, sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);

  syncReleasePeer(pPeer);
  syncReleasePeer(pPeer);

  return NULL;
}
