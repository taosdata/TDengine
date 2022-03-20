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
  SDnode* pDnode = (SDnode*)param;
  dndRun(pDnode);
  return NULL;
}

SDnodeOpt TestServer::BuildOption(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
  SDnodeOpt option = {0};
  option.numOfSupportVnodes = 16;
  option.serverPort = port;
  strcpy(option.dataDir, path);
  snprintf(option.localEp, TSDB_EP_LEN, "%s:%u", fqdn, port);
  snprintf(option.localFqdn, TSDB_FQDN_LEN, "%s", fqdn);
  snprintf(option.firstEp, TSDB_EP_LEN, "%s", firstEp);
  return option;
}

bool TestServer::DoStart() {
  SDnodeOpt option = BuildOption(path, fqdn, port, firstEp);
  taosMkDir(path);

  pDnode = dndCreate(&option);
  if (pDnode == NULL) {
    return false;
  }

  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  taosThreadCreate(&threadId, &thAttr, serverLoop, pDnode);
  taosThreadAttrDestroy(&thAttr);
  taosMsleep(2100);
  return true;
}

void TestServer::Restart() {
  uInfo("start all server");
  Stop();
  DoStart();
  uInfo("all server is running");
}

bool TestServer::Start(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
  strcpy(this->path, path);
  strcpy(this->fqdn, fqdn);
  this->port = port;
  strcpy(this->firstEp, firstEp);

  taosRemoveDir(path);
  return DoStart();
}

void TestServer::Stop() {
  dndHandleEvent(pDnode, DND_EVENT_STOP);
  taosThreadJoin(threadId, NULL);

  if (pDnode != NULL) {
    dndClose(pDnode);
    pDnode = NULL;
  }
}
