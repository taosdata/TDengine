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

#ifndef _TD_TQ_H_
#define _TD_TQ_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tmqMsgHead {
  int32_t protoVer;
  int32_t msgType;
  int64_t cgId;
  int64_t clientId;
} tmqMsgHead;

typedef struct tmqOneAck {
  int64_t topicId;
  int64_t consumeOffset;
} tmqOneAck;

typedef struct tmqAcks {
  int32_t ackNum;
  //should be sorted
  tmqOneAck acks[];
} tmqAcks;

//TODO: put msgs into common
typedef struct tmqConnectReq {
  tmqMsgHead head;
  tmqAcks acks;
} tmqConnectReq;

typedef struct tmqConnectRsp {
  tmqMsgHead head;
  int8_t status;
} tmqConnectRsp;

typedef struct tmqDisconnectReq {
  tmqMsgHead head;
} tmqDisconnectReq;

typedef struct tmqDisconnectRsp {
  tmqMsgHead head;
  int8_t status;
} tmqDiconnectRsp;

typedef struct tmqConsumeReq {
  tmqMsgHead head;
  tmqAcks acks;
} tmqConsumeReq;

typedef struct tmqMsgContent {
  int64_t topicId;
  int64_t msgLen;
  char    msg[];
} tmqMsgContent;

typedef struct tmqConsumeRsp {
  tmqMsgHead    head;
  int64_t       bodySize;
  tmqMsgContent msgs[];
} tmqConsumeRsp;

typedef struct tmqMnodeSubscribeReq {
  tmqMsgHead head;
  int64_t topicLen;
  char topic[];
} tmqSubscribeReq;

typedef struct tmqMnodeSubscribeRsp {
  tmqMsgHead head;
  int64_t vgId;
  char ep[]; //TSDB_EP_LEN
} tmqSubscribeRsp;

typedef struct tmqHeartbeatReq {

} tmqHeartbeatReq;

typedef struct tmqHeartbeatRsp {

} tmqHeartbeatRsp;

typedef struct tqTopicVhandle {
  //name
  //
  //executor for filter
  //
  //callback for mnode
  //
} tqTopicVhandle;

typedef struct STQ {
  //the collection of group handle

} STQ;

#define TQ_BUFFER_SIZE 8

//TODO: define a serializer and deserializer
typedef struct tqBufferItem {
  int64_t offset;
  //executors are identical but not concurrent
  //so it must be a copy in each item
  void* executor;
  int64_t size;
  void* content;
} tqBufferItem;

typedef struct tqBufferHandle {
  //char* topic; //c style, end with '\0'
  //int64_t cgId;
  //void* ahandle;
  int64_t nextConsumeOffset;
  int64_t topicId;
  int32_t head;
  int32_t tail;
  tqBufferItem buffer[TQ_BUFFER_SIZE];
} tqBufferHandle;

typedef struct tqListHandle {
  tqBufferHandle bufHandle;
  struct tqListHandle* next;
} tqListHandle;

typedef struct tqGroupHandle {
  int64_t cId;
  int64_t cgId;
  void* ahandle;
  int32_t topicNum;
  tqListHandle *head; 
} tqGroupHandle;

typedef struct tqQueryExec {
  void* src;
  tqBufferItem* dest;
  void* executor;
} tqQueryExec;

typedef struct tqQueryMsg {
  tqQueryExec *exec;
  struct tqQueryMsg *next;
} tqQueryMsg;

//init in each vnode
STQ* tqInit(void* ref_func(void*), void* unref_func(void*));
void tqCleanUp(STQ*);

//void* will be replace by a msg type
int tqPushMsg(STQ*, void* msg, int64_t version);
int tqCommit(STQ*);

int tqConsume(STQ*, tmqConsumeReq*);

tqGroupHandle* tqGetGroupHandle(STQ*, int64_t cId);

int tqOpenTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqCloseTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqMoveOffsetToNext(tqGroupHandle*);
int tqResetOffset(STQ*, int64_t topicId, int64_t cgId, int64_t offset);
int tqRegisterContext(tqGroupHandle*, void*);
int tqLaunchQuery(tqGroupHandle*);
int tqSendLaunchQuery(tqGroupHandle*);

int tqSerializeGroupHandle(tqGroupHandle *gHandle, void** ppBytes);
void* tqSerializeListHandle(tqListHandle *listHandle, void* ptr);
void* tqSerializeBufHandle(tqBufferHandle *bufHandle, void* ptr);
void* tqSerializeBufItem(tqBufferItem *bufItem, void* ptr);

const void* tqDeserializeGroupHandle(const void* pBytes, tqGroupHandle *ghandle);
const void* tqDeserializeBufHandle(const void* pBytes, tqBufferHandle *bufHandle);
const void* tqDeserializeBufItem(const void* pBytes, tqBufferItem *bufItem);

int tqGetGHandleSSize(const tqGroupHandle *gHandle);
int tqBufHandleSSize();
int tqBufItemSSize();

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_H_*/
