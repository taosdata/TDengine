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
#include "tdatablock.h"
#include "tmisce.h"
#include "tversion.h"

static void processClientRsp(void* parent, SRpcMsg* pRsp, SEpSet* pEpSet) {
  TestClient* client = (TestClient*)parent;
  client->SetRpcRsp(pRsp);
  uInfo("x response:%s from dnode, code:0x%x, msgSize: %d", TMSG_INFO(pRsp->msgType), pRsp->code, pRsp->contLen);
  tsem_post(client->GetSem());
}

void TestClient::SetRpcRsp(SRpcMsg* rsp) {
  if (this->pRsp) {
    taosMemoryFree(this->pRsp);
  }
  this->pRsp = (SRpcMsg*)taosMemoryCalloc(1, sizeof(SRpcMsg));
  this->pRsp->msgType = rsp->msgType;
  this->pRsp->code = rsp->code;
  this->pRsp->pCont = rsp->pCont;
  this->pRsp->contLen = rsp->contLen;
};

tsem_t* TestClient::GetSem() { return &sem; }

void TestClient::DoInit() {
  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t*)pass, strlen(pass), secretEncrypt);
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = (char*)"shell";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processClientRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.user = (char*)this->user;
  // rpcInit.ckey = (char*)"key";
  rpcInit.parent = this;
  // rpcInit.secret = (char*)secretEncrypt;
  // rpcInit.spi = 1;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  clientRpc = rpcOpen(&rpcInit);
  ASSERT(clientRpc);
  tsem_init(&this->sem, 0, 0);
}

bool TestClient::Init(const char* user, const char* pass) {
  strcpy(this->user, user);
  strcpy(this->pass, pass);
  this->pRsp = NULL;
  this->DoInit();
  return true;
}

void TestClient::Cleanup() {
  tsem_destroy(&sem);
  rpcClose(clientRpc);
}

void TestClient::Restart() {
  this->Cleanup();
  this->DoInit();
}

SRpcMsg* TestClient::SendReq(SRpcMsg* pReq) {
  SEpSet epSet = {0};
  addEpIntoEpSet(&epSet, tsLocalFqdn, tsServerPort);
  rpcSendRequest(clientRpc, &epSet, pReq, NULL);
  tsem_wait(&sem);
  uInfo("y response:%s from dnode, code:0x%x, msgSize: %d", TMSG_INFO(pRsp->msgType), pRsp->code, pRsp->contLen);

  return pRsp;
}
