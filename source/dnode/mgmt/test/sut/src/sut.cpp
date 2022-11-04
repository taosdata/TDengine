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

void Testbase::InitLog(const char* path) {
  dDebugFlag = 143;
  vDebugFlag = 0;
  mDebugFlag = 143;
  cDebugFlag = 0;
  jniDebugFlag = 0;
  tmrDebugFlag = 135;
  uDebugFlag = 135;
  rpcDebugFlag = 143;
  qDebugFlag = 0;
  wDebugFlag = 0;
  sDebugFlag = 0;
  tsdbDebugFlag = 0;
  tsLogEmbedded = 1;
  tsAsyncLog = 0;

  taosRemoveDir(path);
  taosMkDir(path);
  tstrncpy(tsLogDir, path, PATH_MAX);

  taosGetSystemInfo();
  tsRpcQueueMemoryAllowed = tsTotalMemoryKB * 0.1;
  if (taosInitLog("taosdlog", 1) != 0) {
    printf("failed to init log file\n");
  }
}

void Testbase::Init(const char* path, int16_t port) {
  osDefaultInit();
  tsServerPort = port;
  strcpy(tsLocalFqdn, "localhost");
  snprintf(tsLocalEp, TSDB_EP_LEN, "%s:%u", tsLocalFqdn, tsServerPort);
  strcpy(tsFirst, tsLocalEp);
  strcpy(tsDataDir, path);
  taosRemoveDir(path);
  taosMkDir(path);
  InitLog(TD_TMP_DIR_PATH "td");

  if (!server.Start()) {
    printf("failed to start server, exit\n");
    exit(0);
  };
  client.Init("root", "taosdata");
  showRsp = NULL;
}

void Testbase::Cleanup() {
  if (showRsp != NULL) {
    rpcFreeCont(showRsp);
    showRsp = NULL;
  }
  client.Cleanup();
  taosMsleep(10);
  server.Stop();
  dmCleanup();
}

void Testbase::Restart() {
  // server.Restart();
  client.Restart();
}

void Testbase::ServerStop() { server.Stop(); }
void Testbase::ServerStart() { server.Start(); }
void Testbase::ClientRestart() { client.Restart(); }

SRpcMsg* Testbase::SendReq(tmsg_t msgType, void* pCont, int32_t contLen) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = msgType;

  return client.SendReq(&rpcMsg);
}

int32_t Testbase::SendShowReq(int8_t showType, const char* tb, const char* db) {
  if (showRsp != NULL) {
    rpcFreeCont(showRsp);
    showRsp = NULL;
  }

  SRetrieveTableReq retrieveReq = {0};
  strcpy(retrieveReq.db, db);
  strcpy(retrieveReq.tb, tb);

  int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &retrieveReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSRetrieveTableReq(pReq, contLen, &retrieveReq);

  SRpcMsg* pRsp = SendReq(TDMT_MND_SYSTABLE_RETRIEVE, pReq, contLen);
  ASSERT(pRsp->pCont != nullptr);

  if (pRsp->contLen == 0) return -1;
  if (pRsp->code != 0) return -1;

  showRsp = (SRetrieveMetaTableRsp*)pRsp->pCont;
  showRsp->handle = htobe64(showRsp->handle);  // show Id
  showRsp->useconds = htobe64(showRsp->useconds);
  showRsp->numOfRows = htonl(showRsp->numOfRows);
  showRsp->compLen = htonl(showRsp->compLen);
  if (showRsp->numOfRows <= 0) return -1;

  return 0;
}

int32_t Testbase::GetShowRows() {
  if (showRsp != NULL) {
    return showRsp->numOfRows;
  } else {
    return 0;
  }
}
