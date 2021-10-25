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
  int32_t headLen;
  int32_t protoVer;
  int64_t cgId;
  int64_t topicId;
  int64_t clientId;
  int32_t checksum;
  int32_t msgType;
} tmqMsgHead;

//TODO: put msgs into common
typedef struct tmqConnectReq {
  tmqMsgHead head;
} tmqConnectReq;

typedef struct tmqConnectResp {
  tmqMsgHead head;
  int8_t status;
} tmqConnectResp;

typedef struct tmqDisconnectReq {
  tmqMsgHead head;
} tmqDisconnectReq;

typedef struct tmqDisconnectResp {
  tmqMsgHead head;
  int8_t status;
} tmqDiconnectResp;

typedef struct tmqConsumeReq {
  tmqMsgHead head;
  int64_t commitOffset;
} tmqConsumeReq;

typedef struct tmqConsumeResp {
  tmqMsgHead head;
  char content[];
} tmqConsumeResp;

//
typedef struct tmqMnodeSubscribeReq {
  tmqMsgHead head;
  int64_t topicLen;
  char topic[];
} tmqSubscribeReq;

typedef struct tmqMnodeSubscribeResp {
  tmqMsgHead head;
  int64_t vgId;
  char ep[]; //TSDB_EP_LEN
} tmqSubscribeResp;

typedef struct tmqHeartbeatReq {

} tmqHeartbeatReq;

typedef struct tmqHeartbeatResp {

} tmqHeartbeatResp;

typedef struct tqTopicVhandle {
  //name
  //
  //executor for filter
  //
  //callback for mnode
  //
} tqTopicVhandle;

typedef struct STQ {
  //the set for topics
  //key=topicName: str
  //value=tqTopicVhandle

  //a map
  //key=<topic: str, cgId: int64_t>
  //value=consumeOffset: int64_t
} STQ;

#define TQ_BUFFER_SIZE 8

typedef struct tqBufferItem {
  int64_t offset;
  void* executor;
  void* content;
} tqBufferItem;

typedef struct tqGroupHandle {
  char* topic; //c style, end with '\0'
  int64_t cgId;
  void* ahandle;
  int64_t consumeOffset;
  int32_t head;
  int32_t tail;
  tqBufferItem buffer[TQ_BUFFER_SIZE];
} tqGroupHandle;

//init in each vnode
STQ* tqInit(void* ref_func(void*), void* unref_func(void*));
void tqCleanUp(STQ*);

//void* will be replace by a msg type
int tqPushMsg(STQ*, void* msg, int64_t version);
int tqCommit(STQ*);

//void* will be replace by a msg type
int tqHandleConsumeMsg(STQ*, tmqConsumeReq* msg);

tqGroupHandle* tqFindGHandleBycId(STQ*, int64_t cId);

int tqOpenTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqCloseTCGroup(STQ*, int64_t topicId, int64_t cgId, int64_t cId);
int tqMoveOffsetToNext(tqGroupHandle*);
int tqResetOffset(STQ*, int64_t topicId, int64_t cgId, int64_t offset);
int tqFetchMsg(tqGroupHandle*, void*);
int tqRegisterContext(tqGroupHandle*, void*);
int tqLaunchQuery(STQ*, int64_t topicId, int64_t cgId, void* query);
int tqSendLaunchQuery(STQ*, int64_t topicId, int64_t cgId, void* query);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_H_*/
