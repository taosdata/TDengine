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

#include "tqInt.h"

//static
//read next version data
//
//send to fetch queue
//
//handle management message
//
static int tqProtoCheck(tmqMsgHead *pMsg) {
  return pMsg->protoVer == 0;
}

static int tqAckOneTopic(tqBufferHandle *bhandle, tmqOneAck *pAck, tqQueryMsg** ppQuery) {
  //clean old item and move forward
  int32_t consumeOffset = pAck->consumeOffset;
  int idx = consumeOffset % TQ_BUFFER_SIZE;
  ASSERT(bhandle->buffer[idx].content && bhandle->buffer[idx].executor);
  tfree(bhandle->buffer[idx].content);
  if( 1 /* TODO: need to launch new query */) {
    tqQueryMsg* pNewQuery = malloc(sizeof(tqQueryMsg));
    if(pNewQuery == NULL) {
      //TODO: memory insufficient
      return -1;
    }
    //TODO: lock executor
    pNewQuery->exec->executor = bhandle->buffer[idx].executor;
    //TODO: read from wal and assign to src
    pNewQuery->exec->src = 0;
    pNewQuery->exec->dest = &bhandle->buffer[idx];
    pNewQuery->next = *ppQuery;
    *ppQuery = pNewQuery;
  }
  return 0;
}

static int tqAck(tqGroupHandle* ghandle, tmqAcks* pAcks) {
  int32_t ackNum = pAcks->ackNum;
  tmqOneAck *acks = pAcks->acks;
  //double ptr for acks and list
  int i = 0;
  tqListHandle* node = ghandle->head;
  int ackCnt = 0;
  tqQueryMsg *pQuery = NULL;
  while(i < ackNum && node->next) {
    if(acks[i].topicId == node->next->bufHandle->topicId) {
      ackCnt++;
      tqAckOneTopic(node->next->bufHandle, &acks[i], &pQuery);
    } else if(acks[i].topicId < node->next->bufHandle->topicId) {
      i++;
    } else {
      node = node->next;
    }
  }
  if(pQuery) {
    //post message
  }
  return ackCnt;
}

static int tqCommitTCGroup(tqGroupHandle* handle) {
  //persist modification into disk
  return 0;
}

int tqCreateTCGroup(STQ *pTq, int64_t topicId, int64_t cgId, int64_t cId, tqGroupHandle** handle) {
  //create in disk
  return 0;
}

int tqOpenTCGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  //look up in disk
  //create
  //open
  return 0;
}

int tqCloseTCGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  return 0;
}

int tqDropTCGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  //delete from disk
  return 0;
}

static int tqFetch(tqGroupHandle* ghandle, void** msg) {
  tqListHandle* head = ghandle->head;
  tqListHandle* node = head;
  int totSize = 0;
  //TODO: make it a macro
  int sizeLimit = 4 * 1024;
  tmqMsgContent* buffer = malloc(sizeLimit);
  if(buffer == NULL) {
    //TODO:memory insufficient
    return -1;
  }
  //iterate the list to get msgs of all topics
  //until all topic iterated or msgs over sizeLimit
  while(node->next) {
    node = node->next;
    tqBufferHandle* bufHandle = node->bufHandle;
    int idx = bufHandle->nextConsumeOffset % TQ_BUFFER_SIZE;
    if(bufHandle->buffer[idx].content != NULL &&
        bufHandle->buffer[idx].offset == bufHandle->nextConsumeOffset
        ) {
      totSize += bufHandle->buffer[idx].size;
      if(totSize > sizeLimit) {
        void *ptr = realloc(buffer, totSize);
        if(ptr == NULL) {
          totSize -= bufHandle->buffer[idx].size;
          //TODO:memory insufficient
          //return msgs already copied
          break;
        }
      }
      *((int64_t*)buffer) = bufHandle->topicId;
      buffer = POINTER_SHIFT(buffer, sizeof(int64_t));
      *((int64_t*)buffer) = bufHandle->buffer[idx].size;
      buffer = POINTER_SHIFT(buffer, sizeof(int64_t));
      memcpy(buffer, bufHandle->buffer[idx].content, bufHandle->buffer[idx].size);
      buffer = POINTER_SHIFT(buffer, bufHandle->buffer[idx].size);
      if(totSize > sizeLimit) {
        break;
      }
    }
  }
  if(totSize == 0) {
    //no msg
    return -1;
  }

  return totSize;
}


tqGroupHandle* tqGetGroupHandle(STQ* pTq, int64_t cId) {
  return NULL;
}

int tqLaunchQuery(tqGroupHandle* ghandle) {
  return 0;
}

int tqSendLaunchQuery(STQ* pTq, int64_t topicId, int64_t cgId, void* query) {
  return 0;
}

/*int tqMoveOffsetToNext(tqGroupHandle* ghandle) {*/
  /*return 0;*/
/*}*/

int tqPushMsg(STQ* pTq , void* p, int64_t version) {
  //add reference
  //judge and launch new query
  return 0;
}

int tqCommit(STQ* pTq) {
  //do nothing
  return 0;
}

int tqConsume(STQ* pTq, tmqConsumeReq* pMsg) {
  if(!tqProtoCheck((tmqMsgHead *)pMsg)) {
    //proto version invalid
    return -1;
  }
  int64_t clientId = pMsg->head.clientId;
  tqGroupHandle *ghandle = tqGetGroupHandle(pTq, clientId);
  if(ghandle == NULL) {
    //client not connect
    return -1;
  }
  if(pMsg->acks.ackNum != 0) {
    if(tqAck(ghandle, &pMsg->acks) != 0) {
      //ack not success
      return -1;
    }
  }

  tmqConsumeRsp *pRsp = (tmqConsumeRsp*) pMsg;

  if(tqFetch(ghandle, (void**)&pRsp->msgs) < 0) {
    //fetch error
    return -1;
  }

  //judge and launch new query
  if(tqLaunchQuery(ghandle)) {
    //launch query error
    return -1;
  }
  return 0;
}


int tqSerializeGroupHandle(tqGroupHandle *gHandle, void** ppBytes, int32_t offset) {
  //calculate size
  int sz = tqGetGHandleSSize(gHandle);
  if(sz <= 0) {
    //TODO: err
    return -1;
  }
  void* ptr = realloc(*ppBytes, sz);
  if(ptr == NULL) {
    free(ppBytes);
    //TODO: memory err
    return -1;
  }
  *ppBytes = ptr;
  //do serialize
  *(int64_t*)ptr = gHandle->cId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = gHandle->cgId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = gHandle->topicNum;
  if(gHandle->topicNum > 0) {
    tqSerializeListHandle(gHandle->head, ppBytes, ptr - *ppBytes);
  }
  return 0;
}

int tqSerializeListHandle(tqListHandle *listHandle, void** ppBytes, int32_t offset) {
  void* ptr = POINTER_SHIFT(*ppBytes, offset);
  tqListHandle *node = listHandle;
  while(node->next) {
    node = node->next;
    offset = tqSerializeBufHandle(node->bufHandle, ppBytes, offset);
  }
  return offset;
}
int tqSerializeBufHandle(tqBufferHandle *bufHandle, void** ppBytes, int32_t offset) {
  void *ptr = POINTER_SHIFT(*ppBytes, offset);
  *(int64_t*)ptr = bufHandle->nextConsumeOffset;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = bufHandle->topicId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = bufHandle->head;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  *(int32_t*)ptr = bufHandle->tail;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for(int i = 0; i < TQ_BUFFER_SIZE; i++) {
    int sz = tqSerializeBufItem(&bufHandle->buffer[i], ppBytes, ptr - *ppBytes);
    ptr = POINTER_SHIFT(ptr, sz);
  }
  return ptr - *ppBytes;
}

int tqSerializeBufItem(tqBufferItem *bufItem, void** ppBytes, int32_t offset) {
  void *ptr = POINTER_SHIFT(*ppBytes, offset);
  //TODO: do we need serialize this? 
  return 0;
}

int tqDeserializeGroupHandle(const void* pBytes, tqGroupHandle **pGhandle) {
  return 0;
}
int tqDeserializeListHandle(const void* pBytes, tqListHandle **pListHandle) {
  return 0;
}
int tqDeserializeBufHandle(const void* pBytes, tqBufferHandle **pBufHandle) {
  return 0;
}
int tqDeserializeBufItem(const void* pBytes, tqBufferItem **pBufItem) {
  return 0;
}


int tqGetGHandleSSize(const tqGroupHandle *gHandle) {
  return 0;
}
int tqListHandleSSize(const tqListHandle *listHandle) {
  return 0;
}
int tqBufHandleSSize(const tqBufferHandle *bufHandle) {
  return 0;
}
int tqBufItemSSize(const tqBufferItem *bufItem) {
  return 0;
}
