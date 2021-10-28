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

typedef struct TmqMsgHead {
  int32_t protoVer;
  int32_t msgType;
  int64_t cgId;
  int64_t clientId;
} TmqMsgHead;

typedef struct TmqOneAck {
  int64_t topicId;
  int64_t consumeOffset;
} TmqOneAck;

typedef struct TmqAcks {
  int32_t ackNum;
  //should be sorted
  TmqOneAck acks[];
} TmqAcks;

//TODO: put msgs into common
typedef struct TmqConnectReq {
  TmqMsgHead head;
  TmqAcks acks;
} TmqConnectReq;

typedef struct TmqConnectRsp {
  TmqMsgHead head;
  int8_t status;
} TmqConnectRsp;

typedef struct TmqDisconnectReq {
  TmqMsgHead head;
} TmqDiscconectReq;

typedef struct TmqDisconnectRsp {
  TmqMsgHead head;
  int8_t status;
} TmqDisconnectRsp;

typedef struct TmqConsumeReq {
  TmqMsgHead head;
  TmqAcks acks;
} TmqConsumeReq;

typedef struct TmqMsgContent {
  int64_t topicId;
  int64_t msgLen;
  char    msg[];
} TmqMsgContent;

typedef struct TmqConsumeRsp {
  TmqMsgHead    head;
  int64_t       bodySize;
  TmqMsgContent msgs[];
} TmqConsumeRsp;

typedef struct TmqSubscribeReq {
  TmqMsgHead head;
  int64_t topicLen;
  char topic[];
} TmqSubscribeReq;

typedef struct tmqSubscribeRsp {
  TmqMsgHead head;
  int64_t vgId;
  char ep[]; //TSDB_EP_LEN
} TmqSubscribeRsp;

typedef struct TmqHeartbeatReq {

} TmqHeartbeatReq;

typedef struct TmqHeartbeatRsp {

} TmqHeartbeatRsp;

typedef struct TqTopicVhandle {
  //name
  //
  //executor for filter
  //
  //callback for mnode
  //
} TqTopicVhandle;

typedef struct STQ {
  //the collection of group handle

} STQ;

#define TQ_BUFFER_SIZE 8

//TODO: define a serializer and deserializer
typedef struct TqBufferItem {
  int64_t offset;
  //executors are identical but not concurrent
  //so it must be a copy in each item
  void* executor;
  int64_t size;
  void* content;
} TqBufferItem;

typedef struct TqBufferHandle {
  //char* topic; //c style, end with '\0'
  //int64_t cgId;
  //void* ahandle;
  int64_t nextConsumeOffset;
  int64_t topicId;
  int32_t head;
  int32_t tail;
  TqBufferItem buffer[TQ_BUFFER_SIZE];
} TqBufferHandle;

typedef struct TqListHandle {
  TqBufferHandle bufHandle;
  struct TqListHandle* next;
} TqListHandle;

typedef struct TqGroupHandle {
  int64_t cId;
  int64_t cgId;
  void* ahandle;
  int32_t topicNum;
  TqListHandle *head; 
} TqGroupHandle;

typedef struct TqQueryExec {
  void* src;
  TqBufferItem* dest;
  void* executor;
} TqQueryExec;

typedef struct TqQueryMsg {
  TqQueryExec *exec;
  struct TqQueryMsg *next;
} TqQueryMsg;

//init in each vnode
STQ* tqInit(void* ref_func(void*), void* unref_func(void*));
void tqCleanUp(STQ*);

//void* will be replace by a msg type
int tqPushMsg(STQ*, void* msg, int64_t version);
int tqCommit(STQ*);

int tqConsume(STQ*, TmqConsumeReq*);

TqGroupHandle* tqGetGroupHandle(STQ*, int64_t cId);

int tqOpenTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqCloseTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqMoveOffsetToNext(TqGroupHandle*);
int tqResetOffset(STQ*, int64_t topicId, int64_t cgId, int64_t offset);
int tqRegisterContext(TqGroupHandle*, void* ahandle);
int tqLaunchQuery(TqGroupHandle*);
int tqSendLaunchQuery(TqGroupHandle*);

int tqSerializeGroupHandle(TqGroupHandle *gHandle, void** ppBytes);
void* tqSerializeListHandle(TqListHandle *listHandle, void* ptr);
void* tqSerializeBufHandle(TqBufferHandle *bufHandle, void* ptr);
void* tqSerializeBufItem(TqBufferItem *bufItem, void* ptr);

const void* tqDeserializeGroupHandle(const void* pBytes, TqGroupHandle *ghandle);
const void* tqDeserializeBufHandle(const void* pBytes, TqBufferHandle *bufHandle);
const void* tqDeserializeBufItem(const void* pBytes, TqBufferItem *bufItem);

int tqGetGHandleSSize(const TqGroupHandle *gHandle);
int tqBufHandleSSize();
int tqBufItemSSize();

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_H_*/
