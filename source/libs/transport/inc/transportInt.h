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
#include "rpcCache.h"
#include "rpcHead.h"
#include "rpcLog.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include "tidpool.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "ttimer.h"
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
  int64_t  index;
  char     label[TSDB_LABEL_LEN];

  char user[TSDB_UNI_LEN];         // meter ID
  char spi;                        // security parameter index
  char encrypt;                    // encrypt algorithm
  char secret[TSDB_PASSWORD_LEN];  // secret for the link
  char ckey[TSDB_PASSWORD_LEN];    // ciphering key

  void (*cfp)(void* parent, SRpcMsg*, SEpSet*);
  int (*afp)(void* parent, char* user, char* spi, char* encrypt, char* secret, char* ckey);
  bool (*pfp)(void* parent, tmsg_t msgType);
  void* (*mfp)(void* parent, tmsg_t msgType);
  bool (*efp)(void* parent, tmsg_t msgType);

  int32_t         refCount;
  void*           parent;
  void*           idPool;     // handle to ID pool
  void*           tmrCtrl;    // handle to timer
  SHashObj*       hash;       // handle returned by hash utility
  void*           tcphandle;  // returned handle from TCP initialization
  pthread_mutex_t mutex;
} SRpcInfo;

#endif  // USE_LIBUV

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSPORT_INT_H_*/
