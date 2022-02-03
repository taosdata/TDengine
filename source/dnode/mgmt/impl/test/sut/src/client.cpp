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

#include "tep.h"
#include "sut.h"

static void processClientRsp(void* parent, SRpcMsg* pRsp, SEpSet* pEpSet) {
  TestClient* client = (TestClient*)parent;
  client->SetRpcRsp(pRsp);
  uInfo("response:%s from dnode, code:0x%x", TMSG_INFO(pRsp->msgType), pRsp->code);
  tsem_post(client->GetSem());
}

void TestClient::SetRpcRsp(SRpcMsg* pRsp) { this->pRsp = pRsp; };

tsem_t* TestClient::GetSem() { return &sem; }

bool TestClient::Init(const char* user, const char* pass, const char* fqdn, uint16_t port) {
  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t*)pass, strlen(pass), secretEncrypt);

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = (char*)"DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processClientRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.user = (char*)user;
  rpcInit.ckey = (char*)"key";
  rpcInit.parent = this;
  rpcInit.secret = (char*)secretEncrypt;
  rpcInit.spi = 1;

  clientRpc = rpcOpen(&rpcInit);
  ASSERT(clientRpc);

  tsem_init(&sem, 0, 0);
  strcpy(this->fqdn, fqdn);
  this->port = port;

  return true;
}

void TestClient::Cleanup() {
  tsem_destroy(&sem);
  rpcClose(clientRpc);
}

SRpcMsg* TestClient::SendReq(SRpcMsg* pReq) {
  SEpSet epSet = {0};
  addEpIntoEpSet(&epSet, fqdn, port);
  rpcSendRequest(clientRpc, &epSet, pReq, NULL);
  tsem_wait(&sem);

  return pRsp;
}
