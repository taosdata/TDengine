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
  if (taosInitLog("taosdlog", 1) != 0) {
    printf("failed to init log file\n");
  }
}

void Testbase::Init(const char* path, int16_t port) {
  dndInit();

  char fqdn[] = "localhost";
  char firstEp[TSDB_EP_LEN] = {0};
  snprintf(firstEp, TSDB_EP_LEN, "%s:%u", fqdn, port);

  InitLog("/tmp/td");
  server.Start(path, fqdn, port, firstEp);
  client.Init("root", "taosdata", fqdn, port);

  tFreeSTableMetaRsp(&metaRsp);
  showId = 0;
  pData = 0;
  pos = 0;
  pRetrieveRsp = NULL;
}

void Testbase::Cleanup() {
  tFreeSTableMetaRsp(&metaRsp);
  client.Cleanup();
  taosMsleep(10);
  server.Stop();
  dndCleanup();
}

void Testbase::Restart() {
  server.Restart();
  client.Restart();
}

void Testbase::ServerStop() { server.Stop(); }

void Testbase::ServerStart() { server.DoStart(); }
void Testbase::ClientRestart() { client.Restart(); }

SRpcMsg* Testbase::SendReq(tmsg_t msgType, void* pCont, int32_t contLen) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = msgType;

  return client.SendReq(&rpcMsg);
}

void Testbase::SendShowMetaReq(int8_t showType, const char* db) {
  SShowReq showReq = {0};
  showReq.type = showType;
  strcpy(showReq.db, db);

  int32_t contLen = tSerializeSShowReq(NULL, 0, &showReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSShowReq(pReq, contLen, &showReq);
  tFreeSShowReq(&showReq);

  SRpcMsg* pRsp = SendReq(TDMT_MND_SHOW, pReq, contLen);
  ASSERT(pRsp->pCont != nullptr);

  if (pRsp->contLen == 0) return;

  SShowRsp showRsp = {0};
  tDeserializeSShowRsp(pRsp->pCont, pRsp->contLen, &showRsp);
  tFreeSTableMetaRsp(&metaRsp);
  metaRsp = showRsp.tableMeta;
  showId = showRsp.showId;
}

int32_t Testbase::GetMetaColId(int32_t index) {
  SSchema* pSchema = &metaRsp.pSchemas[index];
  return pSchema->colId;
}

int8_t Testbase::GetMetaType(int32_t index) {
  SSchema* pSchema = &metaRsp.pSchemas[index];
  return pSchema->type;
}

int32_t Testbase::GetMetaBytes(int32_t index) {
  SSchema* pSchema = &metaRsp.pSchemas[index];
  return pSchema->bytes;
}

const char* Testbase::GetMetaName(int32_t index) {
  SSchema* pSchema = &metaRsp.pSchemas[index];
  return pSchema->name;
}

int32_t Testbase::GetMetaNum() { return metaRsp.numOfColumns; }

const char* Testbase::GetMetaTbName() { return metaRsp.tbName; }

void Testbase::SendShowRetrieveReq() {
  SRetrieveTableReq retrieveReq = {0};
  retrieveReq.showId = showId;
  retrieveReq.free = 0;

  int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &retrieveReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSRetrieveTableReq(pReq, contLen, &retrieveReq);

  SRpcMsg* pRsp = SendReq(TDMT_MND_SHOW_RETRIEVE, pReq, contLen);
  pRetrieveRsp = (SRetrieveTableRsp*)pRsp->pCont;
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  pRetrieveRsp->useconds = htobe64(pRetrieveRsp->useconds);
  pRetrieveRsp->compLen = htonl(pRetrieveRsp->compLen);

  pData = pRetrieveRsp->data;
  pos = 0;
}

const char* Testbase::GetShowName() { return metaRsp.tbName; }

int8_t Testbase::GetShowInt8() {
  int8_t data = *((int8_t*)(pData + pos));
  pos += sizeof(int8_t);
  return data;
}

int16_t Testbase::GetShowInt16() {
  int16_t data = *((int16_t*)(pData + pos));
  pos += sizeof(int16_t);
  return data;
}

int32_t Testbase::GetShowInt32() {
  int32_t data = *((int32_t*)(pData + pos));
  pos += sizeof(int32_t);
  return data;
}

int64_t Testbase::GetShowInt64() {
  int64_t data = *((int64_t*)(pData + pos));
  pos += sizeof(int64_t);
  return data;
}

int64_t Testbase::GetShowTimestamp() {
  int64_t data = *((int64_t*)(pData + pos));
  pos += sizeof(int64_t);
  return data;
}

const char* Testbase::GetShowBinary(int32_t len) {
  pos += sizeof(VarDataLenT);
  char* data = (char*)(pData + pos);
  pos += len;
  return data;
}

int32_t Testbase::GetShowRows() { return pRetrieveRsp->numOfRows; }

STableMetaRsp* Testbase::GetShowMeta() { return &metaRsp; }

SRetrieveTableRsp* Testbase::GetRetrieveRsp() { return pRetrieveRsp; }
