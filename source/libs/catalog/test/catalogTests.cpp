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
#include <tglobal.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "catalog.h"
#include "tep.h"
#include "trpc.h"
#include "stub.h"
#include "addr_any.h"



namespace {

void ctgTestSetPrepareTableMeta();
void ctgTestSetPrepareCTableMeta();
void ctgTestSetPrepareSTableMeta();


int32_t ctgTestVgNum = 10;
int32_t ctgTestColNum = 2;
int32_t ctgTestTagNum = 1;
int32_t ctgTestSVersion = 1;
int32_t ctgTestTVersion = 1;

char *ctgTestClusterId = "cluster1";
char *ctgTestDbname = "1.db1";
char *ctgTestTablename = "table1";
char *ctgTestCTablename = "ctable1";
char *ctgTestSTablename = "stable1";


void sendCreateDbMsg(void *shandle, SEpSet *pEpSet) {
  SCreateDbMsg* pReq = (SCreateDbMsg*)rpcMallocCont(sizeof(SCreateDbMsg));
  strcpy(pReq->db, "1.db1");
  pReq->numOfVgroups = htonl(2);
  pReq->cacheBlockSize = htonl(16);
  pReq->totalBlocks = htonl(10);
  pReq->daysPerFile = htonl(10);
  pReq->daysToKeep0 = htonl(3650);
  pReq->daysToKeep1 = htonl(3650);
  pReq->daysToKeep2 = htonl(3650);
  pReq->minRows = htonl(100);
  pReq->maxRows = htonl(4096);
  pReq->commitTime = htonl(3600);
  pReq->fsyncPeriod = htonl(3000);
  pReq->walLevel = 1;
  pReq->precision = 0;
  pReq->compression = 2;
  pReq->replications = 1;
  pReq->quorum = 1;
  pReq->update = 0;
  pReq->cacheLastRow = 0;
  pReq->ignoreExist = 1;
  
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SCreateDbMsg);
  rpcMsg.msgType = TDMT_MND_CREATE_DB;

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(shandle, pEpSet, &rpcMsg, &rpcRsp);

  ASSERT_EQ(rpcRsp.code, 0);
}

void ctgTestPrepareDbVgroups(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SUseDbRsp *rspMsg = NULL; //todo

  pRsp->code =0;
  pRsp->contLen = sizeof(SUseDbRsp) + ctgTestVgNum * sizeof(SVgroupInfo);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (SUseDbRsp *)pRsp->pCont;
  strcpy(rspMsg->db, ctgTestDbname);
  rspMsg->vgVersion = htonl(1);
  rspMsg->vgNum = htonl(ctgTestVgNum);
  rspMsg->hashMethod = 0;

  SVgroupInfo *vg = NULL;
  uint32_t hashUnit = UINT32_MAX / ctgTestVgNum;
  for (int32_t i = 0; i < ctgTestVgNum; ++i) {
    vg = &rspMsg->vgroupInfo[i];

    vg->vgId = htonl(i + 1);
    vg->hashBegin = htonl(i * hashUnit);
    vg->hashEnd = htonl(hashUnit * (i + 1) - 1);
    vg->numOfEps = i % TSDB_MAX_REPLICA + 1;
    vg->inUse = i % vg->numOfEps;
    for (int32_t n = 0; n < vg->numOfEps; ++n) {
      SEpAddrMsg *addr = &vg->epAddr[n];
      strcpy(addr->fqdn, "a0");
      addr->port = htons(n + 22);
    }
  }

  vg->hashEnd = htonl(UINT32_MAX);

  return;
}




void ctgTestPrepareTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaMsg *rspMsg = NULL; //todo

  pRsp->code =0;
  pRsp->contLen = sizeof(STableMetaMsg) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaMsg *)pRsp->pCont;
  sprintf(rspMsg->tbFname, "%s.%s", ctgTestDbname, ctgTestTablename);
  rspMsg->numOfTags = 0;
  rspMsg->numOfColumns = htonl(ctgTestColNum);
  rspMsg->precision = 1;
  rspMsg->tableType = TSDB_NORMAL_TABLE;
  rspMsg->update = 1;
  rspMsg->sversion = htonl(ctgTestSVersion);
  rspMsg->tversion = htonl(ctgTestTVersion);
  rspMsg->suid = 0;
  rspMsg->tuid = htobe64(0x0000000000000001);
  rspMsg->vgId = htonl(8);

  SSchema *s = NULL;
  s = &rspMsg->pSchema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = htonl(0);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(1);
  s->bytes = htonl(4);
  strcpy(s->name, "col1");
  
  return;
}


void ctgTestPrepareCTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaMsg *rspMsg = NULL; //todo

  pRsp->code =0;
  pRsp->contLen = sizeof(STableMetaMsg) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaMsg *)pRsp->pCont;
  sprintf(rspMsg->tbFname, "%s.%s", ctgTestDbname, ctgTestCTablename);
  sprintf(rspMsg->stbFname, "%s.%s", ctgTestDbname, ctgTestSTablename);
  rspMsg->numOfTags = htonl(ctgTestTagNum);
  rspMsg->numOfColumns = htonl(ctgTestColNum);
  rspMsg->precision = 1;
  rspMsg->tableType = TSDB_CHILD_TABLE;
  rspMsg->update = 1;
  rspMsg->sversion = htonl(ctgTestSVersion);
  rspMsg->tversion = htonl(ctgTestTVersion);
  rspMsg->suid = htobe64(0x0000000000000002);
  rspMsg->tuid = htobe64(0x0000000000000003);
  rspMsg->vgId = htonl(9);

  SSchema *s = NULL;
  s = &rspMsg->pSchema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = htonl(0);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(1);
  s->bytes = htonl(4);
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = htonl(2);
  s->bytes = htonl(12);
  strcpy(s->name, "tag1s");

  
  return;
}


void ctgTestPrepareSTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaMsg *rspMsg = NULL; //todo

  pRsp->code =0;
  pRsp->contLen = sizeof(STableMetaMsg) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaMsg *)pRsp->pCont;
  sprintf(rspMsg->tbFname, "%s.%s", ctgTestDbname, ctgTestSTablename);
  sprintf(rspMsg->stbFname, "%s.%s", ctgTestDbname, ctgTestSTablename);
  rspMsg->numOfTags = htonl(ctgTestTagNum);
  rspMsg->numOfColumns = htonl(ctgTestColNum);
  rspMsg->precision = 1;
  rspMsg->tableType = TSDB_SUPER_TABLE;
  rspMsg->update = 1;
  rspMsg->sversion = htonl(ctgTestSVersion);
  rspMsg->tversion = htonl(ctgTestTVersion);
  rspMsg->suid = htobe64(0x0000000000000002);
  rspMsg->tuid = htobe64(0x0000000000000003);
  rspMsg->vgId = 0;

  SSchema *s = NULL;
  s = &rspMsg->pSchema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = htonl(0);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(1);
  s->bytes = htonl(4);
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = htonl(2);
  s->bytes = htonl(12);
  strcpy(s->name, "tag1s");

  
  return;
}

void ctgTestPrepareDbVgroupsAndNormalMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestPrepareDbVgroups(shandle, pEpSet, pMsg, pRsp);
  
  ctgTestSetPrepareTableMeta();
  
  return;
}


void ctgTestPrepareDbVgroupsAndChildMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestPrepareDbVgroups(shandle, pEpSet, pMsg, pRsp);
  
  ctgTestSetPrepareCTableMeta();
  
  return;
}

void ctgTestPrepareDbVgroupsAndSuperMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestPrepareDbVgroups(shandle, pEpSet, pMsg, pRsp);
  
  ctgTestSetPrepareSTableMeta();
  
  return;
}



void ctgTestSetPrepareDbVgroups() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroups);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroups);
    }
  }
}

void ctgTestSetPrepareTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareTableMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareTableMeta);
    }
  }
}

void ctgTestSetPrepareCTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareCTableMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareCTableMeta);
    }
  }
}

void ctgTestSetPrepareSTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareSTableMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareSTableMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndNormalMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndNormalMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndNormalMeta);
    }
  }
}


void ctgTestSetPrepareDbVgroupsAndChildMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndChildMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndChildMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndSuperMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndSuperMeta);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndSuperMeta);
    }
  }
}


}

TEST(tableMeta, normalTable) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo vgInfo = {0};

  ctgTestSetPrepareDbVgroups();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestTablename, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.numOfEps, 3);

  ctgTestSetPrepareTableMeta();

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  catalogDestroy();
}

TEST(tableMeta, childTableCase) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo vgInfo = {0};

  ctgTestSetPrepareDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestCTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestCTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestSTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  catalogDestroy();
}

TEST(tableMeta, superTableCase) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo vgInfo = {0};

  ctgTestSetPrepareDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestSTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  ctgTestSetPrepareCTableMeta();

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestCTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  tableMeta = NULL;
  code = catalogRenewAndGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestCTablename, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);



  catalogDestroy();
}

TEST(tableDistVgroup, normalTable) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndNormalMeta();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);


  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestTablename, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 8);
  ASSERT_EQ(vgInfo->numOfEps, 3);

  catalogDestroy();
}

TEST(tableDistVgroup, childTableCase) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestCTablename, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 9);
  ASSERT_EQ(vgInfo->numOfEps, 4);


  catalogDestroy();
}

TEST(tableDistVgroup, superTableCase) {
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  //sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, ctgTestSTablename, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 10);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 1);
  ASSERT_EQ(vgInfo->numOfEps, 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 1);
  ASSERT_EQ(vgInfo->vgId, 2);
  ASSERT_EQ(vgInfo->numOfEps, 2);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 2);
  ASSERT_EQ(vgInfo->vgId, 3);
  ASSERT_EQ(vgInfo->numOfEps, 3);


  catalogDestroy();
}



int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



