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
#include "tlog.h"
#include "tutil.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tsocket.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static int32_t syncAreFilesModified(SSyncNode *pNode, SSyncPeer *pPeer) {
  if (pNode->getFileVersion == NULL) return 0;

  uint64_t fver = 0;
  int32_t  code = (*pNode->getFileVersion)(pNode->vgId, &fver);
  if (code != 0) {
    sInfo("%s, file are modified while retrieve, lastver:%" PRIu64, pPeer->id, pPeer->lastVer);
    return code;
  }

  if (fver != pPeer->lastVer) {
    sInfo("%s, file are modified while retrieve, fver:%" PRIu64 " lastver:%" PRIu64, pPeer->id, fver, pPeer->lastVer);
    return -1;
  }

  return 0;
}

static int32_t syncRetrieveFile(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  fileInfo = {0};
  SFileAck   fileAck = {0};
  int32_t    code = -1;
  char       name[TSDB_FILENAME_LEN * 2] = {0};

  if (pNode->getFileVersion) (*pNode->getFileVersion)(pNode->vgId, &pPeer->lastVer);

  while (1) {
    // retrieve file info
    fileInfo.name[0] = 0;
    fileInfo.magic = (*pNode->getFileInfo)(pNode->vgId, fileInfo.name, &fileInfo.index, TAOS_SYNC_MAX_INDEX,
                                           &fileInfo.size, &fileInfo.fversion);
    // fileInfo.size = htonl(size);
    sDebug("%s, file:%s info will be sent, size:%" PRId64, pPeer->id, fileInfo.name, fileInfo.size);

    // send the file info
    int32_t ret = taosWriteMsg(pPeer->syncFd, &(fileInfo), sizeof(fileInfo));
    if (ret < 0) {
      sError("%s, failed to write file:%s info while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    // if no file anymore, break
    if (fileInfo.magic == 0 || fileInfo.name[0] == 0) {
      sDebug("%s, no more files to sync", pPeer->id);
      code = 0;
      break;
    }

    // wait for the ack from peer
    ret = taosReadMsg(pPeer->syncFd, &fileAck, sizeof(fileAck));
    if (ret < 0) {
      sError("%s, failed to read file:%s ack while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    // set the peer sync version
    pPeer->sversion = fileInfo.fversion;

    // if sync is not required, continue
    if (fileAck.sync == 0) {
      fileInfo.index++;
      sDebug("%s, %s is the same", pPeer->id, fileInfo.name);
      continue;
    }

    // get the full path to file
    snprintf(name, sizeof(name), "%s/%s", pNode->path, fileInfo.name);

    // send the file to peer
    int32_t sfd = open(name, O_RDONLY);
    if (sfd < 0) {
      sError("%s, failed to open file:%s while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    ret = taosSendFile(pPeer->syncFd, sfd, NULL, fileInfo.size);
    close(sfd);
    if (ret < 0) {
      sError("%s, failed to send file:%s while retrieve file since %s", pPeer->id, fileInfo.name, strerror(errno));
      break;
    }

    sDebug("%s, file:%s is sent, size:%" PRId64, pPeer->id, fileInfo.name, fileInfo.size);
    fileInfo.index++;

    // check if processed files are modified
    if (syncAreFilesModified(pNode, pPeer) != 0) break;
  }

  if (code < 0) {
    sError("%s, failed to retrieve file", pPeer->id);
  }

  return code;
}

/* if only a partial record is read out, set the IN_MODIFY flag in event,
   so upper layer will reload the file to get a complete record */
static int32_t syncReadOneWalRecord(int32_t sfd, SWalHead *pHead, uint32_t *pEvent) {
  int32_t ret;

  ret = read(sfd, pHead, sizeof(SWalHead));
  if (ret < 0) return -1;
  if (ret == 0) return 0;

  if (ret != sizeof(SWalHead)) {
    // file is not at end yet, it shall be reloaded
    *pEvent = *pEvent | IN_MODIFY;
    return 0;
  }

  assert(pHead->len <= TSDB_MAX_WAL_SIZE);

  ret = read(sfd, pHead->cont, pHead->len);
  if (ret < 0) return -1;

  if (ret != pHead->len) {
    // file is not at end yet, it shall be reloaded
    *pEvent = *pEvent | IN_MODIFY;
    return 0;
  }

  return sizeof(SWalHead) + pHead->len;
}

static int32_t syncMonitorLastWal(SSyncPeer *pPeer, char *name) {
  pPeer->watchNum = 0;
  taosClose(pPeer->notifyFd);
  pPeer->notifyFd = inotify_init1(IN_NONBLOCK);
  if (pPeer->notifyFd < 0) {
    sError("%s, failed to init inotify since %s", pPeer->id, strerror(errno));
    return -1;
  }

  if (pPeer->watchFd == NULL) pPeer->watchFd = malloc(sizeof(int32_t) * tsMaxWatchFiles);
  if (pPeer->watchFd == NULL) {
    sError("%s, failed to allocate watchFd", pPeer->id);
    return -1;
  }

  memset(pPeer->watchFd, -1, sizeof(int32_t) * tsMaxWatchFiles);
  int32_t *wd = pPeer->watchFd;

  *wd = inotify_add_watch(pPeer->notifyFd, name, IN_MODIFY | IN_CLOSE_WRITE);
  if (*wd == -1) {
    sError("%s, failed to watch last wal since %s", pPeer->id, strerror(errno));
    return -1;
  }

  return 0;
}

static int32_t syncCheckLastWalChanges(SSyncPeer *pPeer, uint32_t *pEvent) {
  char    buf[2048];
  int32_t len = read(pPeer->notifyFd, buf, sizeof(buf));
  if (len < 0 && errno != EAGAIN) {
    sError("%s, failed to read notify FD since %s", pPeer->id, strerror(errno));
    return -1;
  }

  if (len == 0) return 0;

  struct inotify_event *event;
  for (char *ptr = buf; ptr < buf + len; ptr += sizeof(struct inotify_event) + event->len) {
    event = (struct inotify_event *)ptr;
    if (event->mask & IN_MODIFY) *pEvent = *pEvent | IN_MODIFY;
    if (event->mask & IN_CLOSE_WRITE) *pEvent = *pEvent | IN_CLOSE_WRITE;
  }

  if (pEvent != 0) sDebug("%s, last wal event:0x%x", pPeer->id, *pEvent);

  return 0;
}

static int32_t syncRetrieveLastWal(SSyncPeer *pPeer, char *name, uint64_t fversion, int64_t offset, uint32_t *pEvent) {
  SWalHead *pHead = malloc(SYNC_MAX_SIZE);
  int32_t   code = -1;
  int32_t   bytes = 0;
  int32_t   sfd;

  sfd = open(name, O_RDONLY);
  if (sfd < 0) {
    free(pHead);
    return -1;
  }

  (void)lseek(sfd, offset, SEEK_SET);
  sDebug("%s, retrieve last wal, offset:%" PRId64 " fver:%" PRIu64, pPeer->id, offset, fversion);

  while (1) {
    int32_t wsize = syncReadOneWalRecord(sfd, pHead, pEvent);
    if (wsize < 0) break;
    if (wsize == 0) {
      code = 0;
      break;
    }

    sTrace("%s, last wal is forwarded, hver:%" PRIu64, pPeer->id, pHead->version);
    int32_t ret = taosWriteMsg(pPeer->syncFd, pHead, wsize);
    if (ret != wsize) break;
    pPeer->sversion = pHead->version;

    bytes += wsize;

    if (pHead->version >= fversion && fversion > 0) {
      code = 0;
      bytes = 0;
      break;
    }
  }

  free(pHead);
  close(sfd);

  if (code == 0) return bytes;
  return -1;
}

static int32_t syncProcessLastWal(SSyncPeer *pPeer, char *wname, int64_t index) {
  SSyncNode *pNode = pPeer->pSyncNode;
  int32_t    code = -1;
  char       fname[TSDB_FILENAME_LEN * 2] = {0};  // full path to wal file

  if (syncAreFilesModified(pNode, pPeer) != 0) return -1;

  while (1) {
    int32_t  once = 0;  // last WAL has once ever been processed
    int64_t  offset = 0;
    uint64_t fversion = 0;
    uint32_t event = 0;

    // get full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);
    sDebug("%s, start to retrieve last wal:%s", pPeer->id, fname);

    // monitor last wal
    if (syncMonitorLastWal(pPeer, fname) < 0) break;

    while (1) {
      int32_t bytes = syncRetrieveLastWal(pPeer, fname, fversion, offset, &event);
      if (bytes < 0) break;

      // check file changes
      if (syncCheckLastWalChanges(pPeer, &event) < 0) break;

      // if file is not updated or updated once, set the fversion and sstatus
      if (((event & IN_MODIFY) == 0) || once) {
        if (fversion == 0) {
          pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;  // start to forward pkt
          sDebug("%s, fversion is 0 then set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
          fversion = nodeVersion;                   // must read data to fversion
        }
      }

      // if all data up to fversion is read out, it is over
      if (pPeer->sversion >= fversion && fversion > 0) {
        code = 0;
        sDebug("%s, data up to fver:%" PRIu64 " has been read out, bytes:%d", pPeer->id, fversion, bytes);
        break;
      }

      // if all data are read out, and no update
      if ((bytes == 0) && ((event & IN_MODIFY) == 0)) {
        // wal file is closed, break
        if (event & IN_CLOSE_WRITE) {
          code = 0;
          sDebug("%s, current wal is closed", pPeer->id);
          break;
        }

        // wal not closed, it means some data not flushed to disk, wait for a while
        usleep(10000);
      }

      // if bytes>0, file is updated, or fversion is not reached but file still open, read again
      once = 1;
      offset += bytes;
      sDebug("%s, retrieve last wal, bytes:%d", pPeer->id, bytes);
      event = event & (~IN_MODIFY);  // clear IN_MODIFY flag
    }

    if (code < 0) break;
    if (pPeer->sversion >= fversion && fversion > 0) break;

    index++;
    wname[0] = 0;
    code = (*pNode->getWalInfo)(pNode->vgId, wname, &index);
    if (code < 0) break;
    if (wname[0] == 0) {
      code = 0;
      break;
    }

    // current last wal is closed, there is a new one
    sDebug("%s, last wal is closed, try new one", pPeer->id);
  }

  taosClose(pPeer->notifyFd);

  return code;
}

static int32_t syncRetrieveWal(SSyncPeer *pPeer) {
  SSyncNode * pNode = pPeer->pSyncNode;
  char        fname[TSDB_FILENAME_LEN * 3];
  char        wname[TSDB_FILENAME_LEN * 2];
  int32_t     size;
  struct stat fstat;
  int32_t     code = -1;
  int64_t     index = 0;

  while (1) {
    // retrieve wal info
    wname[0] = 0;
    code = (*pNode->getWalInfo)(pNode->vgId, wname, &index);
    if (code < 0) break;  // error
    if (wname[0] == 0) {  // no wal file
      sDebug("%s, no wal file", pPeer->id);
      break;
    }

    if (code == 0) {  // last wal
      code = syncProcessLastWal(pPeer, wname, index);
      break;
    }

    // get the full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);

    // send wal file,
    // inotify is not required, old wal file won't be modified, even remove is ok
    if (stat(fname, &fstat) < 0) break;
    size = fstat.st_size;

    sDebug("%s, retrieve wal:%s size:%d", pPeer->id, fname, size);
    int32_t sfd = open(fname, O_RDONLY);
    if (sfd < 0) break;

    code = taosSendFile(pPeer->syncFd, sfd, NULL, size);
    close(sfd);
    if (code < 0) break;

    index++;

    if (syncAreFilesModified(pNode, pPeer) != 0) break;
  }

  if (code == 0) {
    pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;
    sInfo("%s, wal retrieve is finished, set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);

    SWalHead walHead;
    memset(&walHead, 0, sizeof(walHead));
    code = taosWriteMsg(pPeer->syncFd, &walHead, sizeof(walHead));
  } else {
    sError("%s, failed to send wal since %s", pPeer->id, strerror(errno));
  }

  return code;
}

static int32_t syncRetrieveFirstPkt(SSyncPeer *pPeer) {
  SSyncNode *pNode = pPeer->pSyncNode;

  SFirstPkt firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.syncHead.type = TAOS_SMSG_SYNC_DATA;
  firstPkt.syncHead.vgId = pNode->vgId;
  tstrncpy(firstPkt.fqdn, tsNodeFqdn, sizeof(firstPkt.fqdn));
  firstPkt.port = tsSyncPort;

  if (taosWriteMsg(pPeer->syncFd, &firstPkt, sizeof(firstPkt)) < 0) {
    sError("%s, failed to send sync firstPkt since %s", pPeer->id, strerror(errno));
    return -1;
  }

  SFirstPktRsp firstPktRsp;
  if (taosReadMsg(pPeer->syncFd, &firstPktRsp, sizeof(SFirstPktRsp)) < 0) {
    sError("%s, failed to read sync firstPkt rsp since %s", pPeer->id, strerror(errno));
    return -1;
  }

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
  if (syncRetrieveFile(pPeer) < 0) {
    sError("%s, failed to retrieve files", pPeer->id);
    return -1;
  }

  // if no files are synced, there must be wal to sync, sversion must be larger than one
  if (pPeer->sversion == 0) pPeer->sversion = 1;

  sInfo("%s, start to retrieve wals", pPeer->id);
  if (syncRetrieveWal(pPeer) < 0) {
    sError("%s, failed to retrieve wals", pPeer->id);
    return -1;
  }

  return 0;
}

void *syncRetrieveData(void *param) {
  SSyncPeer *pPeer = (SSyncPeer *)param;
  SSyncNode *pNode = pPeer->pSyncNode;
  taosBlockSIGPIPE();

  if (pNode->notifyFlowCtrl) (*pNode->notifyFlowCtrl)(pNode->vgId, pPeer->numOfRetrieves);

  pPeer->fileChanged = 0;
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
    if (pNode->notifyFlowCtrl) (*pNode->notifyFlowCtrl)(pNode->vgId, 0);
  }

  pPeer->fileChanged = 0;
  taosClose(pPeer->notifyFd);
  taosClose(pPeer->syncFd);
  syncDecPeerRef(pPeer);

  return NULL;
}
