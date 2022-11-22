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

#include "sut.h"

void* serverLoop(void* param) {
  TestServer* server = (TestServer*)param;
  server->runnning = false;

  if (dmInit() != 0) {
    return NULL;
  }

  server->runnning = true;
  if (dmRun() != 0) {
    return NULL;
  }

  dmCleanup();
  return NULL;
}

bool TestServer::Start() {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  taosThreadCreate(&threadId, &thAttr, serverLoop, this);
  taosThreadAttrDestroy(&thAttr);
  taosMsleep(2100);
  return runnning;
}

void TestServer::Stop() {
  dmStop();
  taosThreadJoin(threadId, NULL);
}
