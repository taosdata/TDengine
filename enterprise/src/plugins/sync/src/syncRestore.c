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
#include "os.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tsocket.h"
#include "tsync.h"
#include "syncInt.h"

static int syncRestoreFile(SSyncPeer *pPeer) 
{
  SSyncNode *pNode = pPeer->pSyncNode;
  SFileInfo  minfo, sinfo;
  SFileAck   fileAck;
  int        code = -1;

  while (1) {
    // read file info
    int ret = taosReadMsg(pPeer->syncFd, &(minfo), sizeof(minfo));
    if (ret < 0 ) break;

    // if no more file, break;
    if (minfo.name[0] == 0) {code = 0; break;}
   
    fileAck.sync = 0;
    minfo.index = htonl(minfo.index);
    minfo.size = htonl(minfo.size);

    // check the file info
    strcpy(sinfo.name, minfo.name);
    sinfo.magic = (*pNode->getFileInfo)(sinfo.name, &sinfo.index, &sinfo.size);

    // if file not there or magic is not the same, file shall be synced
    if (sinfo.magic != minfo.magic || sinfo.name[0] == 0) fileAck.sync =1;

    // send file ack
    ret = taosWriteMsg(pPeer->syncFd, &(fileAck), sizeof(fileAck));
    if (ret <0)  break;
 
    // if sync is not required, continue
    if (fileAck.sync == 0) continue;

    // if sync is requred, open file, receive from master, and write to file
    int dfd = open(sinfo.name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if ( dfd < 0 ) {
      dError("%s peer:%s, failed to open file:%s", pNode->label, pPeer->ipstr, minfo.name);
      break;
    }

    ret = taosCopyFds(pPeer->syncFd, dfd, minfo.size);
    close(dfd);
    if (ret<0) break;

    dTrace("%s peer:%s, %s is received, size:%d", pNode->label, pPeer->ipstr, minfo.name, minfo.size);
  }

  if (code<0) {
    dError("%s peer:%s, failed to recv %s, reason:%s", pNode->label, pPeer->ipstr, strerror(errno));
  }

  return code;
}

static int syncRestoreWal(SSyncPeer *pPeer)
{
  SSyncNode  *pNode = pPeer->pSyncNode;
  int         ret, code = -1;
  SWalHead    walHead;

  void *buffer = malloc(1024000);
  if (buffer == NULL) return -1;

  while (1) {
    ret = taosReadMsg(pPeer->syncFd, &(walHead), sizeof(walHead));
    if (ret <0)  break;

    if (walHead.len == 0) {code = 0; break;}  // wal sync over
    
    ret = taosReadMsg(pPeer->syncFd, buffer, walHead.len);
    if (ret <0)  break;

    (*pNode->writeToCache)(pNode->ahandle, walHead.version, walHead.cont, walHead.len);
  }

  if (code<0) {
    dError("%s peer:%s, failed to read WAL, reason:%s", pNode->label, pPeer->ipstr, strerror(errno));
  }

  free(buffer);
  return code;
}

static char *syncProcessOneBufferedFwd(SSyncNode *pNode, char *offset)
{
  SSyncHead *pHead = (SSyncHead *) offset;
  int        contLen = pHead->len;

  (*pNode->writeToCache)(pNode->ahandle, pHead->version, pHead->cont, pHead->len);
  offset += contLen + sizeof(SSyncHead);

  return offset;
}

static int syncProcessBufferedFwd(SSyncNode *pNode)
{
  SRecvBuffer *pRecv = pNode->pRecv;
  int          forwards = 0;
  char        *offset = NULL;

  offset = pRecv->buffer;
  while (forwards < pRecv->forwards) {
    offset = syncProcessOneBufferedFwd(pNode, offset);
    forwards++;
  }
  
  pthread_mutex_lock(&pNode->mutex);

  while (forwards < pRecv->forwards && pRecv->code == 0) {
    offset = syncProcessOneBufferedFwd(pNode, offset);
    forwards++;
  }

  pthread_mutex_unlock(&pNode->mutex);

  return pRecv->code;
}

int syncSaveIntoBuffer(SRecvBuffer *pRecv, SSyncHead *pHead)
{
  int contLen = pHead->len;

  if (pRecv->bufferSize - (pRecv->offset - pRecv->buffer) > contLen + 100) {
    memcpy(pRecv->offset, pHead, sizeof(SSyncHead));
    pRecv->offset += sizeof(SSyncHead);
    memcpy(pRecv->offset, pHead->cont, contLen);
    pRecv->offset += contLen;
    pRecv->forwards++;
  } else {
    pRecv->code = -1;  // set error code
  }

  return pRecv->code;
}

static void syncCloseRecvBuffer(SRecvBuffer *pRecv)
{
  if (pRecv) {
    free(pRecv->buffer);
  }
}

static int syncOpenRecvBuffer(SSyncNode *pNode) 
{
  syncCloseRecvBuffer(pNode->pRecv);

  SRecvBuffer *pRecv = calloc(sizeof(SRecvBuffer), 1);
  if (pRecv == NULL) return -1;

  pRecv->bufferSize = 1024000;
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

  dTrace("%s peer:%s, start to restore", pNode->label, pPeer->ipstr);

  pNode->status = TAOS_SYNC_STATUS_FILE;
  dTrace("%s peer:%s, start to restore file", pNode->label, pPeer->ipstr);
  if (syncRestoreFile(pPeer) < 0) {
    dError("%s peer:%s, failed to restore file", pNode->label, pPeer->ipstr);
    return -1;
  }

  dTrace("%s peer:%s, start to restore WAL", pNode->label, pPeer->ipstr);
  if (syncRestoreWal(pPeer) < 0) {
    dError("%s peer:%s, failed to restore WAL", pNode->label, pPeer->ipstr);
    return -1;
  }

  pNode->status = TAOS_SYNC_STATUS_CACHE;
  dTrace("%s peer:%s, start to insert buffered points", pNode->label, pPeer->ipstr);
  if (syncProcessBufferedFwd(pNode) < 0) {
    dError("%s peer:%s, failed to insert buffered points", pNode->label, pPeer->ipstr);
    return -1;
  }

  return 0;
}

void *syncRestoreData(void *param)
{
  SSyncPeer  *pPeer = (SSyncPeer *)param;
  SSyncNode  *pNode = pPeer->pSyncNode;

  if (syncOpenRecvBuffer(pNode) < 0) {
    dError("%s peer:%s, failed to allocate recv buffer", pNode->label, pPeer->ipstr);
    tclose(pPeer->syncFd)
    return NULL;
  } 

  taosBlockSIGPIPE();
  __sync_fetch_and_add(&tsSyncNum, 1);

  if ( syncRestoreDataStepByStep(pPeer) == 0) {
    dPrint("%s peer:%s, it is synced successfully", pNode->label, pPeer->ipstr);
    pNode->status = TAOS_SYNC_STATUS_SLAVE;
    syncBroadcastStatus(pNode);
    (*pNode->notifyStatus)(pNode->ahandle, pNode->status);
  } else {
    dError("%s peer:%s, failed to restore data, restart connection", pNode->label, pPeer->ipstr);
    pNode->status = TAOS_SYNC_STATUS_UNSYNCED;
    syncRestartConnection(pPeer);
  }

  tclose(pPeer->syncFd)
  syncCloseRecvBuffer(pNode->pRecv);

  __sync_fetch_and_sub(&tsSyncNum, 1);

  return NULL;
}

