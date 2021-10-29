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
static int tqProtoCheck(TmqMsgHead *pMsg) {
  return pMsg->protoVer == 0;
}

static int tqAckOneTopic(TqBufferHandle *bhandle, TmqOneAck *pAck, TqQueryMsg** ppQuery) {
  //clean old item and move forward
  int32_t consumeOffset = pAck->consumeOffset;
  int idx = consumeOffset % TQ_BUFFER_SIZE;
  ASSERT(bhandle->buffer[idx].content && bhandle->buffer[idx].executor);
  tfree(bhandle->buffer[idx].content);
  if( 1 /* TODO: need to launch new query */) {
    TqQueryMsg* pNewQuery = malloc(sizeof(TqQueryMsg));
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

static int tqAck(TqGroupHandle* ghandle, TmqAcks* pAcks) {
  int32_t ackNum = pAcks->ackNum;
  TmqOneAck *acks = pAcks->acks;
  //double ptr for acks and list
  int i = 0;
  TqListHandle* node = ghandle->head;
  int ackCnt = 0;
  TqQueryMsg *pQuery = NULL;
  while(i < ackNum && node->next) {
    if(acks[i].topicId == node->next->bufHandle.topicId) {
      ackCnt++;
      tqAckOneTopic(&node->next->bufHandle, &acks[i], &pQuery);
    } else if(acks[i].topicId < node->next->bufHandle.topicId) {
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

static int tqCommitTCGroup(TqGroupHandle* handle) {
  //persist modification into disk
  return 0;
}

int tqCreateTCGroup(STQ *pTq, int64_t topicId, int64_t cgId, int64_t cId, TqGroupHandle** handle) {
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

static int tqFetch(TqGroupHandle* ghandle, void** msg) {
  TqListHandle* head = ghandle->head;
  TqListHandle* node = head;
  int totSize = 0;
  //TODO: make it a macro
  int sizeLimit = 4 * 1024;
  TmqMsgContent* buffer = malloc(sizeLimit);
  if(buffer == NULL) {
    //TODO:memory insufficient
    return -1;
  }
  //iterate the list to get msgs of all topics
  //until all topic iterated or msgs over sizeLimit
  while(node->next) {
    node = node->next;
    TqBufferHandle* bufHandle = &node->bufHandle;
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
  return totSize;
}


TqGroupHandle* tqGetGroupHandle(STQ* pTq, int64_t cId) {
  return NULL;
}

int tqLaunchQuery(TqGroupHandle* ghandle) {
  return 0;
}

int tqSendLaunchQuery(TqGroupHandle* gHandle) {
  return 0;
}

/*int tqMoveOffsetToNext(TqGroupHandle* ghandle) {*/
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

int tqConsume(STQ* pTq, TmqConsumeReq* pMsg) {
  if(!tqProtoCheck((TmqMsgHead *)pMsg)) {
    //proto version invalid
    return -1;
  }
  int64_t clientId = pMsg->head.clientId;
  TqGroupHandle *ghandle = tqGetGroupHandle(pTq, clientId);
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

  TmqConsumeRsp *pRsp = (TmqConsumeRsp*) pMsg;

  if(tqFetch(ghandle, (void**)&pRsp->msgs) <= 0) {
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

int tqSerializeGroupHandle(TqGroupHandle *gHandle, void** ppBytes) {
  //calculate size
  int sz = tqGetGHandleSSize(gHandle);
  void* ptr = realloc(*ppBytes, sz);
  if(ptr == NULL) {
    free(ppBytes);
    //TODO: memory err
    return -1;
  }
  *ppBytes = ptr;
  //do serialization
  *(int64_t*)ptr = gHandle->cId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = gHandle->cgId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = gHandle->topicNum;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  if(gHandle->topicNum > 0) {
    tqSerializeListHandle(gHandle->head, ptr);
  }
  return 0;
}

void* tqSerializeListHandle(TqListHandle *listHandle, void* ptr) {
  TqListHandle *node = listHandle;
  ASSERT(node != NULL);
  while(node) {
    ptr = tqSerializeBufHandle(&node->bufHandle, ptr);
    node = node->next;
  }
  return ptr;
}

void* tqSerializeBufHandle(TqBufferHandle *bufHandle, void* ptr) {
  *(int64_t*)ptr = bufHandle->nextConsumeOffset;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = bufHandle->topicId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = bufHandle->head;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  *(int32_t*)ptr = bufHandle->tail;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for(int i = 0; i < TQ_BUFFER_SIZE; i++) {
    ptr = tqSerializeBufItem(&bufHandle->buffer[i], ptr);
  }
  return ptr;
}

void* tqSerializeBufItem(TqBufferItem *bufItem, void* ptr) {
  //TODO: do we need serialize this? 
  //mainly for executor
  return ptr;
}

const void* tqDeserializeGroupHandle(const void* pBytes, TqGroupHandle *gHandle) {
  const void* ptr = pBytes;
  gHandle->cId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  gHandle->cgId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  gHandle->ahandle = NULL;
  gHandle->topicNum = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  gHandle->head = NULL;
  TqListHandle *node = gHandle->head;
  for(int i = 0; i < gHandle->topicNum; i++) {
    if(gHandle->head == NULL) {
      if((node = malloc(sizeof(TqListHandle))) == NULL) {
        //TODO: error
        return NULL;
      }
      node->next= NULL;
      ptr = tqDeserializeBufHandle(ptr, &node->bufHandle); 
      gHandle->head = node;
    } else {
      node->next = malloc(sizeof(TqListHandle));
      if(node->next == NULL) {
        //TODO: error
        return NULL;
      }
      node->next->next = NULL;
      ptr = tqDeserializeBufHandle(ptr, &node->next->bufHandle);
      node = node->next;
    }
  }
  return ptr;
}

const void* tqDeserializeBufHandle(const void* pBytes, TqBufferHandle *bufHandle) {
  const void* ptr = pBytes;
  bufHandle->nextConsumeOffset = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  bufHandle->topicId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  bufHandle->head = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  bufHandle->tail = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for(int i = 0; i < TQ_BUFFER_SIZE; i++) {
    ptr = tqDeserializeBufItem(ptr, &bufHandle->buffer[i]);
  }
  return ptr;
}

const void* tqDeserializeBufItem(const void* pBytes, TqBufferItem *bufItem) {
  return pBytes;
}

//TODO: make this a macro
int tqGetGHandleSSize(const TqGroupHandle *gHandle) {
  return sizeof(int64_t) * 2
    + sizeof(int32_t)
    + gHandle->topicNum * tqBufHandleSSize();
}

//TODO: make this a macro
int tqBufHandleSSize() {
  return sizeof(int64_t) * 2
    + sizeof(int32_t) * 2
    + TQ_BUFFER_SIZE * tqBufItemSSize();
}

int tqBufItemSSize() {
  //TODO: do this need serialization?
  //mainly for executor
  return 0;
}
