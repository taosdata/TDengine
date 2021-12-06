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

#include <gtest/gtest.h>
#include "os.h"

#include "dnode.h"
#include "taosmsg.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tnote.h"
#include "trpc.h"
#include "tthread.h"
#include "ulog.h"
#include "tdataformat.h"

typedef struct {
  SDnode*    pDnode;
  pthread_t* threadId;
} SServer;

typedef struct {
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
  void*    clientRpc;
  SRpcMsg* pRsp;
  tsem_t   sem;
} SClient;

SServer* createServer(const char* path, const char* fqdn, uint16_t port);
void     dropServer(SServer* pServer);
SClient* createClient(const char* user, const char* pass, const char* fqdn, uint16_t port);
void     dropClient(SClient* pClient);
void     sendMsg(SClient* pClient, SRpcMsg* pMsg);
