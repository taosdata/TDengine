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

#include <stdint.h>
#include <stdbool.h>
#include <sys/inotify.h>
#include <unistd.h>
#include "os.h"
#include "tlog.h"
#include "tutil.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tsocket.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static int syncAddIntoWatchList(SSyncPeer *pPeer, char *name) 
{
  sDebug("%s, start to monitor:%s", pPeer->id, name);

  if (pPeer->notifyFd <=0) {
    pPeer->watchNum = 0;
    pPeer->notifyFd = inotify_init1(IN_NONBLOCK);
    if (pPeer->notifyFd < 0) {
      sError("%s, failed to init inotify(%s)", pPeer->id, strerror(errno));
      return -1;
    }

    if (pPeer->watchFd == NULL) pPeer->watchFd = malloc(sizeof(int)*tsMaxWatchFiles);
    if (pPeer->watchFd == NULL) {
      sError("%s, failed to allocate watchFd", pPeer->id);
      return -1;
    }

    memset(pPeer->watchFd, -1, sizeof(int)*tsMaxWatchFiles);
  }

  int *wd = pPeer->watchFd + pPeer->watchNum;

  if (*wd >= 0) {
    if (inotify_rm_watch(pPeer->notifyFd, *wd) < 0) {
      sError("%s, failed to remove wd:%d(%s)", pPeer->id, *wd, strerror(errno));
      return -1;
    }
  }

  *wd = inotify_add_watch(pPeer->notifyFd, name, IN_MODIFY);
  if (*wd == -1) {
    sError("%s, failed to add %s(%s)", pPeer->id, name, strerror(errno));
    return -1;
  }

  pPeer->watchNum++;
  pPeer->watchNum = (pPeer->watchNum +1) % tsMaxWatchFiles;

  return 0;
}

static int syncAreFilesModified(SSyncPeer *pPeer) 
{
  if (pPeer->notifyFd <=0) return 0;

  char buf[2048]; 
  int len = read(pPeer->notifyFd, buf, sizeof(buf));
  if (len <0 && errno != EAGAIN) {
    sError("%s, failed to read notify FD(%s)", pPeer->id, strerror(errno));    
    return -1;
  }
    
  int code = 0; 
  if (len >0) { 
    sDebug("%s, processed file is changed", pPeer->id);    
    code = 1;
  }

  return code;  
}

static int syncRetrieveFile(SSyncPeer *pPeer)
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  SFileInfo   fileInfo;
  SFileAck    fileAck;
  int         code = -1;
  char        name[TSDB_FILENAME_LEN * 2] = {0};

  memset(&fileInfo, 0, sizeof(fileInfo));
  memset(&fileAck, 0, sizeof(fileAck));

  while (1) {
    // retrieve file info
    fileInfo.name[0] = 0;
    fileInfo.magic = (*pNode->getFileInfo)(pNode->ahandle, fileInfo.name, &fileInfo.index, TAOS_SYNC_MAX_INDEX, &fileInfo.size, &fileInfo.fversion);   
    //fileInfo.size = htonl(size);

    // send the file info
    int32_t ret = taosWriteMsg(pPeer->syncFd, &(fileInfo), sizeof(fileInfo));
    if (ret < 0 ) break;

    // if no file anymore, break
    if (fileInfo.magic == 0 || fileInfo.name[0] == 0) { 
      sDebug("%s, no more files to sync", pPeer->id);    
      code = 0; break; 
    }

    // wait for the ack from peer
    ret = taosReadMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret <0)  break;

    // set the peer sync version
    pPeer->sversion = fileInfo.fversion;

    // get the full path to file
    snprintf(name, sizeof(name), "%s/%s", pNode->path, fileInfo.name);
    
    // add the file into watch list
    if ( syncAddIntoWatchList(pPeer, name) <0) break;

    // if sync is not required, continue
    if (fileAck.sync == 0) {
      fileInfo.index++; 
      sDebug("%s, %s is the same", pPeer->id, fileInfo.name);    
      continue; 
    }

    // send the file to peer
    int sfd = open(name, O_RDONLY);
    if ( sfd < 0 ) break;

    ret = tsendfile(pPeer->syncFd, sfd, NULL, fileInfo.size); 
    close(sfd); 
    if (ret <0) break;

    sDebug("%s, %s is sent, size:%d", pPeer->id, name, fileInfo.size);    
    fileInfo.index++; 

    // check if processed files are modified 
    if (syncAreFilesModified(pPeer) != 0) break;
  }

  if (code < 0) {
    sError("%s, failed to retrieve file(%s)", pPeer->id, strerror(errno));
  }

  return code;
}

/* if only a partial record is read out, set the IN_MODIFY flag in event,
   so upper layer will reload the file to get a complete record */
static int syncReadOneWalRecord(int sfd, SWalHead *pHead, uint32_t *pEvent) 
{ 
  int ret;

  ret = read(sfd, pHead, sizeof(SWalHead));
  if (ret < 0) return -1;
  if (ret == 0) return 0;

  if (ret != sizeof(SWalHead)) {
    // file is not at end yet, it shall be reloaded
    *pEvent = *pEvent | IN_MODIFY;
    return 0;
  }

  ret = read(sfd, pHead->cont, pHead->len);
  if (ret <0) return -1;

  if (ret != pHead->len) {
    // file is not at end yet, it shall be reloaded
    *pEvent = *pEvent | IN_MODIFY;
    return 0;
  }

  return sizeof(SWalHead) + pHead->len;
}    

static int syncMonitorLastWal(SSyncPeer *pPeer, char *name) 
{ 
  pPeer->watchNum = 0;
  tclose(pPeer->notifyFd);
  pPeer->notifyFd = inotify_init1(IN_NONBLOCK);
  if (pPeer->notifyFd < 0) {
    sError("%s, failed to init inotify(%s)", pPeer->id, strerror(errno));
    return -1;
  }

  if (pPeer->watchFd == NULL) pPeer->watchFd = malloc(sizeof(int)*tsMaxWatchFiles);
  if (pPeer->watchFd == NULL) {
    sError("%s, failed to allocate watchFd", pPeer->id);
    return -1;
  }

  memset(pPeer->watchFd, -1, sizeof(int)*tsMaxWatchFiles);
  int *wd = pPeer->watchFd;
 
  *wd = inotify_add_watch(pPeer->notifyFd, name, IN_MODIFY | IN_CLOSE_WRITE);
  if (*wd == -1) {
    sError("%s, failed to watch last wal(%s)", pPeer->id, strerror(errno));
    return -1;
  }

  return 0; 
}

static uint32_t syncCheckLastWalChanges(SSyncPeer *pPeer, uint32_t *pEvent) 
{
  char       buf[2048]; 
  int  len = read(pPeer->notifyFd, buf, sizeof(buf));
  if (len <0 && errno != EAGAIN) {
    sError("%s, failed to read notify FD(%s)", pPeer->id, strerror(errno));    
    return -1;
  }
    
  if (len == 0) return 0;

  struct inotify_event *event;
  for (char *ptr = buf; ptr < buf + len; ptr += sizeof(struct inotify_event) + event->len) {
    event = (struct inotify_event *) ptr;
    if (event->mask & IN_MODIFY) *pEvent = *pEvent | IN_MODIFY;
    if (event->mask & IN_CLOSE_WRITE) *pEvent = *pEvent | IN_CLOSE_WRITE;
  }

  if (pEvent != 0)
    sDebug("%s, last wal event:0x%x", pPeer->id, *pEvent);

  return 0;
}

static int syncRetrieveLastWal(SSyncPeer *pPeer, char *name, uint64_t fversion, int64_t offset, uint32_t *pEvent) 
{
  SWalHead  *pHead = (SWalHead *) malloc(640000);
  int        code = -1;
  int32_t    bytes = 0;
  int        sfd;

  sfd = open(name, O_RDONLY);
  if (sfd < 0) return -1;
  lseek(sfd, offset, SEEK_SET);
  sDebug("%s, retrieve last wal, offset:%" PRId64 " fversion:%" PRIu64, pPeer->id, offset, fversion);

  while (1) {
    int wsize = syncReadOneWalRecord(sfd, pHead, pEvent); 
    if (wsize <0) break;
    if (wsize == 0) { code = 0; break; }

    sDebug("%s, last wal is forwarded, ver:%" PRIu64, pPeer->id, pHead->version);
    int ret = taosWriteMsg(pPeer->syncFd, pHead, wsize);
    if ( ret != wsize ) break;
    pPeer->sversion = pHead->version;

    bytes += wsize;
 
    if (pHead->version >= fversion && fversion > 0) {
      code = 0; 
      bytes = 0; 
      break;
    }
  }

  free(pHead);
  tclose(sfd); 

  if (code == 0) return bytes;
  return -1;
}

static int syncProcessLastWal(SSyncPeer *pPeer, char *wname, uint32_t index) 
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  int         code = -1;
  char        fname[TSDB_FILENAME_LEN * 2];  // full path to wal file

  if (syncAreFilesModified(pPeer) != 0) return -1;

  while (1) {
    int32_t  once = 0; // last WAL has once ever been processed 
    int64_t  offset = 0;
    uint64_t fversion = 0;
    uint32_t event = 0;

    // get full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);
    sDebug("%s, start to retrieve last wal:%s", pPeer->id, fname);

    // monitor last wal
    if (syncMonitorLastWal(pPeer, fname) <0) break;

    while (1) {
      int32_t bytes = syncRetrieveLastWal(pPeer, fname, fversion, offset, &event);
      if (bytes < 0) break;

      // check file changes
      if (syncCheckLastWalChanges(pPeer, &event) <0) break;

      // if file is not updated or updated once, set the fversion and sstatus
      if (((event & IN_MODIFY) == 0) || once) {
        if (fversion == 0) {
          pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;  // start to forward pkt
          fversion = nodeVersion;    // must read data to fversion
        }
      }

      // if all data up to fversion is read out, it is over
      if (pPeer->sversion >= fversion && fversion > 0) {
        code = 0; 
        sDebug("%s, data up to fversion:%ld has been read out, bytes:%d", pPeer->id, fversion, bytes);
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
      event = event & (~IN_MODIFY); // clear IN_MODIFY flag 
    }

    if (code < 0) break;
    if (pPeer->sversion >= fversion && fversion > 0) break;  

    index++;  wname[0] = 0;
    code = (*pNode->getWalInfo)(pNode->ahandle, wname, &index);
    if ( code < 0) break;  
    if ( wname[0] == 0 ) {code = 0; break;}

    // current last wal is closed, there is a new one 
    sDebug("%s, last wal is closed, try new one", pPeer->id);
  }

  tclose(pPeer->notifyFd);

  return code;
}

static int syncRetrieveWal(SSyncPeer *pPeer)
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  char        fname[TSDB_FILENAME_LEN * 3];
  char        wname[TSDB_FILENAME_LEN * 2];
  int32_t     size;
  struct stat fstat;
  int         code = -1;
  uint32_t    index = 0;

  while (1) {
    // retrieve wal info
    wname[0] = 0;
    code = (*pNode->getWalInfo)(pNode->ahandle, wname, &index);   
    if (code < 0) break;  // error
    if (wname[0] == 0) {  // no wal file
      sDebug("%s, no wal file", pPeer->id);
      break;
    }    
      
    if (code == 0) {   // last wal 
      code = syncProcessLastWal(pPeer, wname, index);
      break;
    }

    // get the full path to wal file
    snprintf(fname, sizeof(fname), "%s/%s", pNode->path, wname);

    // send wal file, 
    // inotify is not required, old wal file won't be modified, even remove is ok
    if ( stat(fname, &fstat) < 0 ) break;
    size = fstat.st_size;

    sDebug("%s, retrieve wal:%s size:%d", pPeer->id, fname, size);    
    int sfd = open(fname, O_RDONLY);
    if (sfd < 0) break;

    code = tsendfile(pPeer->syncFd, sfd, NULL, size); 
    close(sfd); 
    if (code <0) break;

    index++; 

    if (syncAreFilesModified(pPeer) != 0) break; 
  }

  if (code == 0) {
    sDebug("%s, wal retrieve is finished", pPeer->id);    
    pPeer->sstatus = TAOS_SYNC_STATUS_CACHE;
    SWalHead walHead;
    memset(&walHead, 0, sizeof(walHead));
    code = taosWriteMsg(pPeer->syncFd, &walHead, sizeof(walHead));
  } else {
    sError("%s, failed to send wal(%s)", pPeer->id, strerror(errno));
  }

  return code;
}

static int syncRetrieveDataStepByStep(SSyncPeer *pPeer)
{
  SSyncNode  *pNode = pPeer->pSyncNode;

  SFirstPkt firstPkt;
  memset(&firstPkt, 0, sizeof(firstPkt));
  firstPkt.syncHead.type = TAOS_SMSG_SYNC_DATA;
  firstPkt.syncHead.vgId = pNode->vgId;
  tstrncpy(firstPkt.fqdn, tsNodeFqdn, sizeof(firstPkt.fqdn));
  firstPkt.port = tsSyncPort;

  if (write(pPeer->syncFd, (char *) &firstPkt, sizeof(firstPkt)) < 0) {
    sError("%s, failed to send syncCmd", pPeer->id);
    return -1;
  }

  pPeer->sversion = 0;  
  pPeer->sstatus = TAOS_SYNC_STATUS_FILE;
  sDebug("%s, start to retrieve file", pPeer->id);
  if (syncRetrieveFile(pPeer) < 0) {
    sError("%s, failed to retrieve file", pPeer->id);
    return -1;
  }

  // if no files are synced, there must be wal to sync, sversion must be larger than one
  if (pPeer->sversion == 0) 
    pPeer->sversion = 1;

  sDebug("%s, start to retrieve wal", pPeer->id);
  if (syncRetrieveWal(pPeer) < 0) {
    sError("%s, failed to retrieve wal", pPeer->id);
    return -1;
  }

  return 0;
}

void *syncRetrieveData(void *param)
{
  SSyncPeer   *pPeer = (SSyncPeer *)param;
  taosBlockSIGPIPE();

  pPeer->syncFd = taosOpenTcpClientSocket(pPeer->ip, pPeer->port, 0);
  if (pPeer->syncFd < 0) {
    sError("%s, failed to open socket to sync", pPeer->id);
  } else {
    sInfo("%s, sync tcp is setup", pPeer->id);
  
    if (syncRetrieveDataStepByStep(pPeer) == 0) {
      sDebug("%s, sync retrieve process is successful", pPeer->id);
    } else {
      sError("%s, failed to retrieve data, restart connection", pPeer->id);
      syncRestartConnection(pPeer);
    }
  }

  tclose(pPeer->notifyFd);
  tclose(pPeer->syncFd);
  syncDecPeerRef(pPeer);

  return NULL;
}
