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

#ifndef _TD_SNODE_INT_H_
#define _TD_SNODE_INT_H_

#include "os.h"

#include "tlog.h"
#include "tmsg.h"
#include "tqueue.h"
#include "trpc.h"

#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  STREAM_STATUS__READY = 1,
  STREAM_STATUS__STOPPED,
  STREAM_STATUS__CREATING,
  STREAM_STATUS__STOPING,
  STREAM_STATUS__RESUMING,
  STREAM_STATUS__DELETING,
};

enum {
  STREAM_RUNNER__RUNNING = 1,
  STREAM_RUNNER__STOP,
};

typedef struct SSnode {
  SSnodeOpt cfg;
} SSnode;

typedef struct {
  int64_t streamId;
  int32_t IdxInLevel;
  int32_t level;
} SStreamInfo;

typedef struct {
  SStreamInfo meta;
  int8_t      status;
  void*       executor;
  STaosQueue* queue;
  void*       stateStore;
  // storage handle
} SStreamRunner;

typedef struct {
  SHashObj* pHash;
} SStreamMeta;

int32_t sndCreateStream();
int32_t sndDropStream();

int32_t sndStopStream();
int32_t sndResumeStream();

#ifdef __cplusplus
}
#endif

#endif /*_TD_SNODE_INT_H_*/
