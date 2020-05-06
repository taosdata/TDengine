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

static int syncRestoreFile(SSyncPeer *pPeer) 
{
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  minfo;   // master file info
  SFileInfo  sinfo;   // slave file info
  SFileAck   fileAck;
  int        code = -1;
  char       name[TSDB_FILENAME_LEN * 2] = {0};

  while (1) {
    // read file info
    int ret = taosReadMsg(pPeer->syncFd, &(minfo), sizeof(minfo));
    if (ret < 0 ) break;

    // if no more file, break;
    if (minfo.name[0] == 0 || minfo.magic == 0) {
      sTrace("%s, no more files to restore", pPeer->id);
      code = 0; 
      break;
    }
   
    fileAck.sync = 0;
    //minfo.index = htonl(minfo.index);
    //minfo.size = htonl(minfo.size);

    sTrace("%s, get file info:%s", pPeer->id, minfo.name);

    // check the file info
    strcpy(sinfo.name, minfo.name);
    sinfo.magic = (*pNode->getFileInfo)(pNode->ahandle, sinfo.name, &sinfo.index, &sinfo.size);

    // if file not there or magic is not the same, file shall be synced
    if (sinfo.magic != minfo.magic || sinfo.name[0] == 0) fileAck.sync =1;

    // send file ack
    ret = taosWriteMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret <0)  break;
 
    // if sync is not required, continue
    if (fileAck.sync == 0) {
      sTrace("%s, %s is the same", pPeer->id, minfo.name);
      continue;
    }

    // if sync is required, open file, receive from master, and write to file
    // get the full path to file
    sprintf(name, "%s/%s", pNode->path, minfo.name);

    int dfd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if ( dfd < 0 ) {
      sError("%s, failed to open file:%s", pPeer->id, name);
      break;
    }

    ret = taosCopyFds(pPeer->syncFd, dfd, minfo.size);
    close(dfd);
    if (ret<0) break;

    sTrace("%s, %s is received, size:%d", pPeer->id, minfo.name, minfo.size);
  }

  if (code < 0) {
    sError("%s, failed to restore %s(%s)", pPeer->id, name, strerror(errno));
  }

  return code;
}

static int syncRestoreWal(SSyncPeer *pPeer)
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  int         ret, code = -1;

  void *buffer = calloc(1024000, 1);  // size for one record
  if (buffer == NULL) return -1;

  SWalHead *pHead = (SWalHead *)buffer;

  while (1) {
    ret = taosReadMsg(pPeer->syncFd, pHead, sizeof(SWalHead));
    if (ret <0)  break;

    if (pHead->len == 0) {code = 0; break;}  // wal sync over
    
    ret = taosReadMsg(pPeer->syncFd, pHead->cont, pHead->len);
    if (ret <0)  break;

    sTrace("%s, restore a record, ver:%d", pPeer->id, pHead->version);
    (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_WAL);
  }

  if (code<0) {
    sError("%s, failed to restore wal(%s)", pPeer->id, strerror(errno));
  }

  free(buffer);
  return code;
}

static char *syncProcessOneBufferedFwd(SSyncPeer *pPeer, char *offset)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  SWalHead  *pHead = (SWalHead *) offset;

  (*pNode->writeToCache)(pNode->ahandle, pHead, TAOS_QTYPE_FWD);
  offset += pHead->len + sizeof(SWalHead);

  return offset;
}

static int syncProcessBufferedFwd(SSyncPeer *pPeer)
{
  SSyncNode   *pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;
  int          forwards = 0;

  sTrace("%s, number of buffered forwards:%d", pPeer->id, pRecv->forwards);

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
  sTrace("%s, finish processing buffered fwds:%d", pPeer->id, forwards);

  pthread_mutex_unlock(&pNode->mutex);

  return pRecv->code;
}

int syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead)
{ 
  SSyncNode   *pNode = pPeer->pSyncNode;
  SRecvBuffer *pRecv = pNode->pRecv;

  int len = pHead->len + sizeof(SWalHead);

  if (pRecv->bufferSize - (pRecv->offset - pRecv->buffer) >= len) {
    memcpy(pRecv->offset, pHead, len);
    pRecv->offset += len;
    pRecv->forwards++;
    sTrace("%s, fwd is saved into queue, ver:%d fwds:%d", 
           pPeer->id, pHead->version, pRecv->forwards);
  } else {
    sError("%s, buffer size:%d is too small", pRecv->bufferSize); 
    pRecv->code = -1;  // set error code
  }

  return pRecv->code;
}

static void syncCloseRecvBuffer(SRecvBuffer *pRecv)
{
  if (pRecv) {
    tfree(pRecv->buffer);
  }
}

static int syncOpenRecvBuffer(SSyncNode *pNode) 
{
  syncCloseRecvBuffer(pNode->pRecv);

  SRecvBuffer *pRecv = calloc(sizeof(SRecvBuffer), 1);
  if (pRecv == NULL) return -1;

  pRecv->bufferSize = 5000000;
  pRecv->buffer = malloc(pRecv->bufferSize);
  if (pRecv->buffer == NULL) return -1;

  pRecv->offset = pRecv->buffer;
  pRecv->forwards = 0;

  pNode->pRecv = pRecv;

  return 0;
}

static int syncRestoreDataStepByStep(SSyncPeer *pPeer)
{
  SSyncNode *pNode = pPeer->pSyncNode;
  nodeSStatus = TAOS_SYNC_STATUS_FILE;

  sTrace("%s, start to restore file", pPeer->id);
  if (syncRestoreFile(pPeer) < 0) {
    sError("%s, failed to restore file", pPeer->id);
    return -1;
  }

  sTrace("%s, start to restore wal", pPeer->id);
  if (syncRestoreWal(pPeer) < 0) {
    sError("%s, failed to restore wal", pPeer->id);
    return -1;
  }

  nodeSStatus = TAOS_SYNC_STATUS_CACHE;
  sTrace("%s, start to insert buffered points", pPeer->id);
  if (syncProcessBufferedFwd(pPeer) < 0) {
    sError("%s, failed to insert buffered points", pPeer->id);
    return -1;
  }

  return 0;
}

void *syncRestoreData(void *param)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncNode  *pNode = pPeer->pSyncNode;

  if (syncOpenRecvBuffer(pNode) < 0) {
    sError("%s, failed to allocate recv buffer", pPeer->id);
    tclose(pPeer->syncFd)
    return NULL;
  } 

  taosBlockSIGPIPE();
  __sync_fetch_and_add(&tsSyncNum, 1);

  if ( syncRestoreDataStepByStep(pPeer) == 0) {
    sPrint("%s, it is synced successfully", pPeer->id);
    nodeRole = TAOS_SYNC_ROLE_SLAVE;
    syncBroadcastStatus(pNode);
    (*pNode->notifyRole)(pNode->ahandle, nodeRole);
  } else {
    sError("%s, failed to restore data, restart connection", pPeer->id);
    nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
    syncRestartConnection(pPeer);
  }

  nodeSStatus = TAOS_SYNC_STATUS_INIT;
  tclose(pPeer->syncFd)
  syncCloseRecvBuffer(pNode->pRecv);

  __sync_fetch_and_sub(&tsSyncNum, 1);

  return NULL;
}

