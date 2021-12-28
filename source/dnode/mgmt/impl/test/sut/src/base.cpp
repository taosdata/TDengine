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

#include "base.h"

void Testbase::InitLog(const char* path) {
  dDebugFlag = 207;
  vDebugFlag = 0;
  mDebugFlag = 207;
  cDebugFlag = 0;
  jniDebugFlag = 0;
  tmrDebugFlag = 0;
  uDebugFlag = 143;
  rpcDebugFlag = 0;
  odbcDebugFlag = 0;
  qDebugFlag = 0;
  wDebugFlag = 0;
  sDebugFlag = 0;
  tsdbDebugFlag = 0;
  cqDebugFlag = 0;
  tscEmbeddedInUtil = 1;
  tsAsyncLog = 0;

  taosRemoveDir(path);
  taosMkDir(path);

  char temp[PATH_MAX];
  snprintf(temp, PATH_MAX, "%s/taosdlog", path);
  if (taosInitLog(temp, tsNumOfLogLines, 1) != 0) {
    printf("failed to init log file\n");
  }
}

void Testbase::Init(const char* path, int16_t port) {
  char fqdn[] = "localhost";
  char firstEp[TSDB_EP_LEN] = {0};
  snprintf(firstEp, TSDB_EP_LEN, "%s:%u", fqdn, port);

  InitLog("/tmp/td");
  server.Start(path, fqdn, port, firstEp);
  client.Init("root", "taosdata", fqdn, port);
  taosMsleep(1100);
}

void Testbase::Cleanup() {
  server.Stop();
  client.Cleanup();
}

void Testbase::Restart() { server.Restart(); }

SRpcMsg* Testbase::SendMsg(tmsg_t msgType, void* pCont, int32_t contLen) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = msgType;

  return client.SendMsg(&rpcMsg);
}

void Testbase::SendShowMetaMsg(int8_t showType, const char* db) {
  int32_t   contLen = sizeof(SShowMsg);
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(contLen);
  pShow->type = showType;
  strcpy(pShow->db, db);

  SRpcMsg*  pMsg = SendMsg(TDMT_MND_SHOW, pShow, contLen);
  SShowRsp* pShowRsp = (SShowRsp*)pMsg->pCont;

  ASSERT(pShowRsp != nullptr);
  pShowRsp->showId = htonl(pShowRsp->showId);
  pMeta = &pShowRsp->tableMeta;
  pMeta->numOfTags = htonl(pMeta->numOfTags);
  pMeta->numOfColumns = htonl(pMeta->numOfColumns);
  pMeta->sversion = htonl(pMeta->sversion);
  pMeta->tversion = htonl(pMeta->tversion);
  pMeta->tuid = htobe64(pMeta->tuid);
  pMeta->suid = htobe64(pMeta->suid);

  showId = pShowRsp->showId;
}

int32_t Testbase::GetMetaColId(int32_t index) {
  SSchema* pSchema = &pMeta->pSchema[index];
  pSchema->colId = htonl(pSchema->colId);
  return pSchema->colId;
}

int8_t Testbase::GetMetaType(int32_t index) {
  SSchema* pSchema = &pMeta->pSchema[index];
  return pSchema->type;
}

int32_t Testbase::GetMetaBytes(int32_t index) {
  SSchema* pSchema = &pMeta->pSchema[index];
  pSchema->bytes = htonl(pSchema->bytes);
  return pSchema->bytes;
}

const char* Testbase::GetMetaName(int32_t index) {
  SSchema* pSchema = &pMeta->pSchema[index];
  return pSchema->name;
}

int32_t Testbase::GetMetaNum() { return pMeta->numOfColumns; }

const char* Testbase::GetMetaTbName() { return pMeta->tbFname; }

void Testbase::SendShowRetrieveMsg() {
  int32_t contLen = sizeof(SRetrieveTableMsg);

  SRetrieveTableMsg* pRetrieve = (SRetrieveTableMsg*)rpcMallocCont(contLen);
  pRetrieve->showId = htonl(showId);
  pRetrieve->free = 0;

  SRpcMsg* pMsg = SendMsg(TDMT_MND_SHOW_RETRIEVE, pRetrieve, contLen);
  pRetrieveRsp = (SRetrieveTableRsp*)pMsg->pCont;
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  pRetrieveRsp->useconds = htobe64(pRetrieveRsp->useconds);
  pRetrieveRsp->compLen = htonl(pRetrieveRsp->compLen);

  pData = pRetrieveRsp->data;
  pos = 0;
}

const char* Testbase::GetShowName() { return pMeta->tbFname; }

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

STableMetaMsg* Testbase::GetShowMeta() { return pMeta; }

SRetrieveTableRsp* Testbase::GetRetrieveRsp() { return pRetrieveRsp; }