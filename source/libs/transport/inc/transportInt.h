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

#ifndef _TD_TRANSPORT_INT_H_
#define _TD_TRANSPORT_INT_H_

#ifdef USE_UV
#include <uv.h>
#endif
#include "lz4.h"
#include "os.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include "tmsg.h"
#include "transLog.h"
#include "tref.h"
#include "trpc.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef USE_UV

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);
void* taosInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);

void taosCloseServer(void* arg);
void taosCloseClient(void* arg);

typedef struct {
  int      sessions;      // number of sessions allowed
  int      numOfThreads;  // number of threads to process incoming messages
  int      idleTime;      // milliseconds;
  uint16_t localPort;
  int8_t   connType;
  char     label[TSDB_LABEL_LEN];
  char     user[TSDB_UNI_LEN];  // meter ID

  void (*cfp)(void* parent, SRpcMsg*, SEpSet*);
  bool (*retry)(int32_t code, tmsg_t msgType);
  bool (*startTimer)(int32_t code, tmsg_t msgType);
  int index;

  void*         parent;
  void*         tcphandle;  // returned handle from TCP initialization
  int64_t       refId;
  TdThreadMutex mutex;
} SRpcInfo;

#endif  // USE_LIBUV

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSPORT_INT_H_*/
