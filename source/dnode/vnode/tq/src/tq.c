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
#include "tqMetaStore.h"

// static
// read next version data
//
// send to fetch queue
//
// handle management message
//

int tqGroupSSize(const STqGroup* pGroup);
int tqTopicSSize();
int tqItemSSize();

void* tqSerializeListHandle(STqList* listHandle, void* ptr);
void* tqSerializeTopic(STqTopic* pTopic, void* ptr);
void* tqSerializeItem(STqMsgItem* pItem, void* ptr);

const void* tqDeserializeTopic(const void* pBytes, STqTopic* pTopic);
const void* tqDeserializeItem(const void* pBytes, STqMsgItem* pItem);

STQ* tqOpen(const char* path, STqCfg* tqConfig, STqLogReader* tqLogReader, SMemAllocatorFactory* allocFac) {
  STQ* pTq = malloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->tqConfig = tqConfig;
  pTq->tqLogReader = tqLogReader;
  pTq->tqMemRef.pAlloctorFactory = allocFac;
  pTq->tqMemRef.pAllocator = allocFac->create(allocFac);
  if (pTq->tqMemRef.pAllocator == NULL) {
    // TODO: error code of buffer pool
  }
  pTq->tqMeta = tqStoreOpen(path, (FTqSerialize)tqSerializeGroup, (FTqDeserialize)tqDeserializeGroup, free, 0);
  if (pTq->tqMeta == NULL) {
    // TODO: free STQ
    return NULL;
  }
  return pTq;
}
void tqClose(STQ* pTq) {
  // TODO
}

static int tqProtoCheck(STqMsgHead* pMsg) { return pMsg->protoVer == 0; }

static int tqAckOneTopic(STqTopic* pTopic, STqOneAck* pAck, STqQueryMsg** ppQuery) {
  // clean old item and move forward
  int32_t consumeOffset = pAck->consumeOffset;
  int     idx = consumeOffset % TQ_BUFFER_SIZE;
  ASSERT(pTopic->buffer[idx].content && pTopic->buffer[idx].executor);
  tfree(pTopic->buffer[idx].content);
  if (1 /* TODO: need to launch new query */) {
    STqQueryMsg* pNewQuery = malloc(sizeof(STqQueryMsg));
    if (pNewQuery == NULL) {
      terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
      return -1;
    }
    // TODO: lock executor
    // TODO: read from wal and assign to src
    /*pNewQuery->exec->executor = pTopic->buffer[idx].executor;*/
    /*pNewQuery->exec->src = 0;*/
    /*pNewQuery->exec->dest = &pTopic->buffer[idx];*/
    /*pNewQuery->next = *ppQuery;*/
    /**ppQuery = pNewQuery;*/
  }
  return 0;
}

static int tqAck(STqGroup* pGroup, STqAcks* pAcks) {
  int32_t    ackNum = pAcks->ackNum;
  STqOneAck* acks = pAcks->acks;
  // double ptr for acks and list
  int          i = 0;
  STqList*     node = pGroup->head;
  int          ackCnt = 0;
  STqQueryMsg* pQuery = NULL;
  while (i < ackNum && node->next) {
    if (acks[i].topicId == node->next->topic.topicId) {
      ackCnt++;
      tqAckOneTopic(&node->next->topic, &acks[i], &pQuery);
    } else if (acks[i].topicId < node->next->topic.topicId) {
      i++;
    } else {
      node = node->next;
    }
  }
  if (pQuery) {
    // post message
  }
  return ackCnt;
}

static int tqCommitGroup(STqGroup* pGroup) {
  // persist modification into disk
  return 0;
}

int tqCreateGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId, STqGroup** ppGroup) {
  // create in disk
  STqGroup* pGroup = (STqGroup*)malloc(sizeof(STqGroup));
  if (pGroup == NULL) {
    // TODO
    return -1;
  }
  *ppGroup = pGroup;
  memset(pGroup, 0, sizeof(STqGroup));

  return 0;
}

STqGroup* tqOpenGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  STqGroup* pGroup = tqHandleGet(pTq->tqMeta, cId);
  if (pGroup == NULL) {
    int code = tqCreateGroup(pTq, topicId, cgId, cId, &pGroup);
    if (code < 0) {
      // TODO
      return NULL;
    }
    tqHandleMovePut(pTq->tqMeta, cId, pGroup);
  }
  ASSERT(pGroup);

  return pGroup;
}

int tqCloseGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  // TODO
  return 0;
}

int tqDropGroup(STQ* pTq, int64_t topicId, int64_t cgId, int64_t cId) {
  // delete from disk
  return 0;
}

static int tqFetch(STqGroup* pGroup, void** msg) {
  STqList* head = pGroup->head;
  STqList* node = head;
  int      totSize = 0;
  // TODO: make it a macro
  int            sizeLimit = 4 * 1024;
  STqMsgContent* buffer = malloc(sizeLimit);
  if (buffer == NULL) {
    // TODO:memory insufficient
    return -1;
  }
  // iterate the list to get msgs of all topics
  // until all topic iterated or msgs over sizeLimit
  while (node->next) {
    node = node->next;
    STqTopic* topicHandle = &node->topic;
    int       idx = topicHandle->nextConsumeOffset % TQ_BUFFER_SIZE;
    if (topicHandle->buffer[idx].content != NULL && topicHandle->buffer[idx].offset == topicHandle->nextConsumeOffset) {
      totSize += topicHandle->buffer[idx].size;
      if (totSize > sizeLimit) {
        void* ptr = realloc(buffer, totSize);
        if (ptr == NULL) {
          totSize -= topicHandle->buffer[idx].size;
          // TODO:memory insufficient
          // return msgs already copied
          break;
        }
      }
      *((int64_t*)buffer) = topicHandle->topicId;
      buffer = POINTER_SHIFT(buffer, sizeof(int64_t));
      *((int64_t*)buffer) = topicHandle->buffer[idx].size;
      buffer = POINTER_SHIFT(buffer, sizeof(int64_t));
      memcpy(buffer, topicHandle->buffer[idx].content, topicHandle->buffer[idx].size);
      buffer = POINTER_SHIFT(buffer, topicHandle->buffer[idx].size);
      if (totSize > sizeLimit) {
        break;
      }
    }
  }
  return totSize;
}

STqGroup* tqGetGroup(STQ* pTq, int64_t clientId) { return tqHandleGet(pTq->tqMeta, clientId); }

int tqSendLaunchQuery(STqMsgItem* bufItem, int64_t offset) {
  if (tqQueryExecuting(bufItem->status)) {
    return 0;
  }
  bufItem->status = 1;
  // load data from wal or buffer pool
  // put into exec
  // send exec into non blocking queue
  // when query finished, put into buffer pool
  return 0;
}

/*int tqMoveOffsetToNext(TqGroupHandle* gHandle) {*/
/*return 0;*/
/*}*/

int tqPushMsg(STQ* pTq, void* p, int64_t version) {
  // add reference
  // judge and launch new query
  return 0;
}

int tqCommit(STQ* pTq) {
  // do nothing
  return 0;
}

int tqBufferSetOffset(STqTopic* pTopic, int64_t offset) {
  int code;
  memset(pTopic->buffer, 0, sizeof(pTopic->buffer));
  // launch query
  for (int i = offset; i < offset + TQ_BUFFER_SIZE; i++) {
    int pos = i % TQ_BUFFER_SIZE;
    code = tqSendLaunchQuery(&pTopic->buffer[pos], offset);
    if (code < 0) {
      // TODO: error handling
    }
  }
  // set offset
  pTopic->nextConsumeOffset = offset;
  pTopic->floatingCursor = offset;
  return 0;
}

STqTopic* tqFindTopic(STqGroup* pGroup, int64_t topicId) {
  // TODO
  return NULL;
}

int tqSetCursor(STQ* pTq, STqSetCurReq* pMsg) {
  int       code;
  int64_t   clientId = pMsg->head.clientId;
  int64_t   topicId = pMsg->topicId;
  int64_t   offset = pMsg->offset;
  STqGroup* gHandle = tqGetGroup(pTq, clientId);
  if (gHandle == NULL) {
    // client not connect
    return -1;
  }
  STqTopic* topicHandle = tqFindTopic(gHandle, topicId);
  if (topicHandle == NULL) {
    return -1;
  }
  if (pMsg->offset == topicHandle->nextConsumeOffset) {
    return 0;
  }
  // TODO: check log last version

  code = tqBufferSetOffset(topicHandle, offset);
  if (code < 0) {
    // set error code
    return -1;
  }

  return 0;
}

int tqConsume(STQ* pTq, STqConsumeReq* pMsg) {
  int64_t   clientId = pMsg->head.clientId;
  STqGroup* pGroup = tqGetGroup(pTq, clientId);
  if (pGroup == NULL) {
    terrno = TSDB_CODE_TQ_GROUP_NOT_SET;
    return -1;
  }

  STqConsumeRsp* pRsp = (STqConsumeRsp*)pMsg;
  int            numOfMsgs = tqFetch(pGroup, (void**)&pRsp->msgs);
  if (numOfMsgs < 0) {
    return -1;
  }
  if (numOfMsgs == 0) {
    // most recent data has been fetched

    // enable timer for blocking wait
    // once new data written during wait time
    // launch query and response
  }

  // fetched a num of msgs, rpc response

  return 0;
}

#if 0
int tqConsume(STQ* pTq, STqConsumeReq* pMsg) {
  if (!tqProtoCheck((STqMsgHead*)pMsg)) {
    // proto version invalid
    return -1;
  }
  int64_t   clientId = pMsg->head.clientId;
  STqGroup* pGroup = tqGetGroup(pTq, clientId);
  if (pGroup == NULL) {
    // client not connect
    return -1;
  }
  if (pMsg->acks.ackNum != 0) {
    if (tqAck(pGroup, &pMsg->acks) != 0) {
      // ack not success
      return -1;
    }
  }

  STqConsumeRsp* pRsp = (STqConsumeRsp*)pMsg;

  if (tqFetch(pGroup, (void**)&pRsp->msgs) <= 0) {
    // fetch error
    return -1;
  }

  // judge and launch new query
  /*if (tqSendLaunchQuery(gHandle)) {*/
  // launch query error
  /*return -1;*/
  /*}*/
  return 0;
}
#endif

int tqSerializeGroup(const STqGroup* pGroup, STqSerializedHead** ppHead) {
  // calculate size
  int sz = tqGroupSSize(pGroup) + sizeof(STqSerializedHead);
  if (sz > (*ppHead)->ssize) {
    void* tmpPtr = realloc(*ppHead, sz);
    if (tmpPtr == NULL) {
      free(*ppHead);
      // TODO: memory err
      return -1;
    }
    *ppHead = tmpPtr;
    (*ppHead)->ssize = sz;
  }
  void* ptr = (*ppHead)->content;
  // do serialization
  *(int64_t*)ptr = pGroup->clientId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = pGroup->cgId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = pGroup->topicNum;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  if (pGroup->topicNum > 0) {
    tqSerializeListHandle(pGroup->head, ptr);
  }
  return 0;
}

void* tqSerializeListHandle(STqList* listHandle, void* ptr) {
  STqList* node = listHandle;
  ASSERT(node != NULL);
  while (node) {
    ptr = tqSerializeTopic(&node->topic, ptr);
    node = node->next;
  }
  return ptr;
}

void* tqSerializeTopic(STqTopic* pTopic, void* ptr) {
  *(int64_t*)ptr = pTopic->nextConsumeOffset;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = pTopic->topicId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int32_t*)ptr = pTopic->head;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  *(int32_t*)ptr = pTopic->tail;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
    ptr = tqSerializeItem(&pTopic->buffer[i], ptr);
  }
  return ptr;
}

void* tqSerializeItem(STqMsgItem* bufItem, void* ptr) {
  // TODO: do we need serialize this?
  // mainly for executor
  return ptr;
}

const void* tqDeserializeGroup(const STqSerializedHead* pHead, STqGroup** ppGroup) {
  STqGroup*   gHandle = *ppGroup;
  const void* ptr = pHead->content;
  gHandle->clientId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  gHandle->cgId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  gHandle->ahandle = NULL;
  gHandle->topicNum = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  gHandle->head = NULL;
  STqList* node = gHandle->head;
  for (int i = 0; i < gHandle->topicNum; i++) {
    if (gHandle->head == NULL) {
      if ((node = malloc(sizeof(STqList))) == NULL) {
        // TODO: error
        return NULL;
      }
      node->next = NULL;
      ptr = tqDeserializeTopic(ptr, &node->topic);
      gHandle->head = node;
    } else {
      node->next = malloc(sizeof(STqList));
      if (node->next == NULL) {
        // TODO: error
        return NULL;
      }
      node->next->next = NULL;
      ptr = tqDeserializeTopic(ptr, &node->next->topic);
      node = node->next;
    }
  }
  return ptr;
}

const void* tqDeserializeTopic(const void* pBytes, STqTopic* topic) {
  const void* ptr = pBytes;
  topic->nextConsumeOffset = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  topic->topicId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  topic->head = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  topic->tail = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
    ptr = tqDeserializeItem(ptr, &topic->buffer[i]);
  }
  return ptr;
}

const void* tqDeserializeItem(const void* pBytes, STqMsgItem* bufItem) { return pBytes; }

// TODO: make this a macro
int tqGroupSSize(const STqGroup* gHandle) {
  return sizeof(int64_t) * 2  // cId + cgId
         + sizeof(int32_t)    // topicNum
         + gHandle->topicNum * tqTopicSSize();
}

// TODO: make this a macro
int tqTopicSSize() {
  return sizeof(int64_t) * 2    // nextConsumeOffset + topicId
         + sizeof(int32_t) * 2  // head + tail
         + TQ_BUFFER_SIZE * tqItemSSize();
}

int tqItemSSize() {
  // TODO: do this need serialization?
  // mainly for executor
  return 0;
}
