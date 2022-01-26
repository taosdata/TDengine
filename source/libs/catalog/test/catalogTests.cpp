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
#include "os.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"

#include "addr_any.h"
#include "catalog.h"
#include "stub.h"
#include "taos.h"
#include "tdef.h"
#include "tep.h"
#include "trpc.h"
#include "tvariant.h"

namespace {

extern "C" int32_t ctgGetTableMetaFromCache(struct SCatalog *pCatalog, const SName *pTableName, STableMeta **pTableMeta,
                                            int32_t *exist);
extern "C" int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output);

void ctgTestSetPrepareTableMeta();
void ctgTestSetPrepareCTableMeta();
void ctgTestSetPrepareSTableMeta();
void ctgTestSetPrepareMultiSTableMeta();

bool    ctgTestStop = false;
bool    ctgTestEnableSleep = false;
bool    ctgTestDeadLoop = false;
int32_t ctgTestPrintNum = 200000;
int32_t ctgTestMTRunSec = 30;

int32_t ctgTestCurrentVgVersion = 0;
int32_t ctgTestVgVersion = 1;
int32_t ctgTestVgNum = 10;
int32_t ctgTestColNum = 2;
int32_t ctgTestTagNum = 1;
int32_t ctgTestSVersion = 1;
int32_t ctgTestTVersion = 1;
int32_t ctgTestSuid = 2;
int64_t ctgTestDbId = 33;

uint64_t ctgTestClusterId = 0x1;
char    *ctgTestDbname = "1.db1";
char    *ctgTestTablename = "table1";
char    *ctgTestCTablename = "ctable1";
char    *ctgTestSTablename = "stable1";

void sendCreateDbMsg(void *shandle, SEpSet *pEpSet) {
  SCreateDbReq *pReq = (SCreateDbReq *)rpcMallocCont(sizeof(SCreateDbReq));
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
  rpcMsg.contLen = sizeof(SCreateDbReq);
  rpcMsg.msgType = TDMT_MND_CREATE_DB;

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(shandle, pEpSet, &rpcMsg, &rpcRsp);

  ASSERT_EQ(rpcRsp.code, 0);
}

void ctgTestInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;

  char temp[128] = {0};
  sprintf(temp, "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, tsNumOfLogLines, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

int32_t ctgTestGetVgNumFromVgVersion(int32_t vgVersion) {
  return ((vgVersion % 2) == 0) ? ctgTestVgNum - 2 : ctgTestVgNum;
}

void ctgTestBuildCTableMetaOutput(STableMetaOutput *output) {
  SName cn = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  SName sn = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(sn.dbname, "db1");
  strcpy(sn.tname, ctgTestSTablename);

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(&cn, tbFullName);

  SET_META_TYPE_BOTH_TABLE(output->metaType);

  strcpy(output->ctbFname, tbFullName);

  tNameExtractFullName(&cn, tbFullName);
  strcpy(output->tbFname, tbFullName);

  output->ctbMeta.vgId = 9;
  output->ctbMeta.tableType = TSDB_CHILD_TABLE;
  output->ctbMeta.uid = 3;
  output->ctbMeta.suid = 2;

  output->tbMeta = (STableMeta *)calloc(1, sizeof(STableMeta) + sizeof(SSchema) * (ctgTestColNum + ctgTestColNum));
  output->tbMeta->vgId = 9;
  output->tbMeta->tableType = TSDB_SUPER_TABLE;
  output->tbMeta->uid = 2;
  output->tbMeta->suid = 2;

  output->tbMeta->tableInfo.numOfColumns = ctgTestColNum;
  output->tbMeta->tableInfo.numOfTags = ctgTestTagNum;

  output->tbMeta->sversion = ctgTestSVersion;
  output->tbMeta->tversion = ctgTestTVersion;

  SSchema *s = NULL;
  s = &output->tbMeta->schema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &output->tbMeta->schema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1s");

  s = &output->tbMeta->schema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = 3;
  s->bytes = 12;
  strcpy(s->name, "tag1s");
}

void ctgTestBuildDBVgroup(SDBVgroupInfo *dbVgroup) {
  static int32_t vgVersion = ctgTestVgVersion + 1;
  int32_t        vgNum = 0;
  SVgroupInfo    vgInfo = {0};

  dbVgroup->vgVersion = vgVersion++;

  ctgTestCurrentVgVersion = dbVgroup->vgVersion;

  dbVgroup->hashMethod = 0;
  dbVgroup->dbId = ctgTestDbId;
  dbVgroup->vgInfo = taosHashInit(ctgTestVgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);

  vgNum = ctgTestGetVgNumFromVgVersion(dbVgroup->vgVersion);
  uint32_t hashUnit = UINT32_MAX / vgNum;

  for (int32_t i = 0; i < vgNum; ++i) {
    vgInfo.vgId = i + 1;
    vgInfo.hashBegin = i * hashUnit;
    vgInfo.hashEnd = hashUnit * (i + 1) - 1;
    vgInfo.epset.numOfEps = i % TSDB_MAX_REPLICA + 1;
    vgInfo.epset.inUse = i % vgInfo.epset.numOfEps;
    for (int32_t n = 0; n < vgInfo.epset.numOfEps; ++n) {
      SEp *addr = &vgInfo.epset.eps[n];
      strcpy(addr->fqdn, "a0");
      addr->port = htons(n + 22);
    }

    taosHashPut(dbVgroup->vgInfo, &vgInfo.vgId, sizeof(vgInfo.vgId), &vgInfo, sizeof(vgInfo));
  }
}

void ctgTestPrepareDbVgroups(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SUseDbRsp *rspMsg = NULL;  // todo

  pRsp->code = 0;
  pRsp->contLen = sizeof(SUseDbRsp) + ctgTestVgNum * sizeof(SVgroupInfo);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (SUseDbRsp *)pRsp->pCont;
  strcpy(rspMsg->db, ctgTestDbname);
  rspMsg->vgVersion = htonl(ctgTestVgVersion);
  ctgTestCurrentVgVersion = ctgTestVgVersion;
  rspMsg->vgNum = htonl(ctgTestVgNum);
  rspMsg->hashMethod = 0;
  rspMsg->uid = htobe64(ctgTestDbId);

  SVgroupInfo *vg = NULL;
  uint32_t     hashUnit = UINT32_MAX / ctgTestVgNum;
  for (int32_t i = 0; i < ctgTestVgNum; ++i) {
    vg = &rspMsg->vgroupInfo[i];

    vg->vgId = htonl(i + 1);
    vg->hashBegin = htonl(i * hashUnit);
    vg->hashEnd = htonl(hashUnit * (i + 1) - 1);
    vg->epset.numOfEps = i % TSDB_MAX_REPLICA + 1;
    vg->epset.inUse = i % vg->epset.numOfEps;
    for (int32_t n = 0; n < vg->epset.numOfEps; ++n) {
      SEp *addr = &vg->epset.eps[n];
      strcpy(addr->fqdn, "a0");
      addr->port = htons(n + 22);
    }
  }

  vg->hashEnd = htonl(UINT32_MAX);

  return;
}

void ctgTestPrepareTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp *rspMsg = NULL;  // todo

  pRsp->code = 0;
  pRsp->contLen = sizeof(STableMetaRsp) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaRsp *)pRsp->pCont;
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
  s->colId = htonl(1);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(2);
  s->bytes = htonl(4);
  strcpy(s->name, "col1");

  return;
}

void ctgTestPrepareCTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp *rspMsg = NULL;  // todo

  pRsp->code = 0;
  pRsp->contLen = sizeof(STableMetaRsp) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaRsp *)pRsp->pCont;
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
  s->colId = htonl(1);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(2);
  s->bytes = htonl(4);
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = htonl(3);
  s->bytes = htonl(12);
  strcpy(s->name, "tag1s");

  return;
}

void ctgTestPrepareSTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp *rspMsg = NULL;  // todo

  pRsp->code = 0;
  pRsp->contLen = sizeof(STableMetaRsp) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaRsp *)pRsp->pCont;
  sprintf(rspMsg->tbFname, "%s.%s", ctgTestDbname, ctgTestSTablename);
  sprintf(rspMsg->stbFname, "%s.%s", ctgTestDbname, ctgTestSTablename);
  rspMsg->numOfTags = htonl(ctgTestTagNum);
  rspMsg->numOfColumns = htonl(ctgTestColNum);
  rspMsg->precision = 1;
  rspMsg->tableType = TSDB_SUPER_TABLE;
  rspMsg->update = 1;
  rspMsg->sversion = htonl(ctgTestSVersion);
  rspMsg->tversion = htonl(ctgTestTVersion);
  rspMsg->suid = htobe64(ctgTestSuid);
  rspMsg->tuid = htobe64(ctgTestSuid);
  rspMsg->vgId = 0;

  SSchema *s = NULL;
  s = &rspMsg->pSchema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = htonl(1);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(2);
  s->bytes = htonl(4);
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = htonl(3);
  s->bytes = htonl(12);
  strcpy(s->name, "tag1s");

  return;
}

void ctgTestPrepareMultiSTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp *rspMsg = NULL;  // todo
  static int32_t idx = 1;

  pRsp->code = 0;
  pRsp->contLen = sizeof(STableMetaRsp) + (ctgTestColNum + ctgTestTagNum) * sizeof(SSchema);
  pRsp->pCont = calloc(1, pRsp->contLen);
  rspMsg = (STableMetaRsp *)pRsp->pCont;
  sprintf(rspMsg->tbFname, "%s.%s_%d", ctgTestDbname, ctgTestSTablename, idx);
  sprintf(rspMsg->stbFname, "%s.%s_%d", ctgTestDbname, ctgTestSTablename, idx);
  rspMsg->numOfTags = htonl(ctgTestTagNum);
  rspMsg->numOfColumns = htonl(ctgTestColNum);
  rspMsg->precision = 1;
  rspMsg->tableType = TSDB_SUPER_TABLE;
  rspMsg->update = 1;
  rspMsg->sversion = htonl(ctgTestSVersion);
  rspMsg->tversion = htonl(ctgTestTVersion);
  rspMsg->suid = htobe64(ctgTestSuid + idx);
  rspMsg->tuid = htobe64(ctgTestSuid + idx);
  rspMsg->vgId = 0;

  SSchema *s = NULL;
  s = &rspMsg->pSchema[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = htonl(1);
  s->bytes = htonl(8);
  strcpy(s->name, "ts");

  s = &rspMsg->pSchema[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = htonl(2);
  s->bytes = htonl(4);
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchema[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = htonl(3);
  s->bytes = htonl(12);
  strcpy(s->name, "tag1s");

  ++idx;

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

void ctgTestPrepareDbVgroupsAndMultiSuperMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestPrepareDbVgroups(shandle, pEpSet, pMsg, pRsp);

  ctgTestSetPrepareMultiSTableMeta();

  return;
}

void ctgTestSetPrepareDbVgroups() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroups);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroups);
    }
  }
}

void ctgTestSetPrepareTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareTableMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareTableMeta);
    }
  }
}

void ctgTestSetPrepareCTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareCTableMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareCTableMeta);
    }
  }
}

void ctgTestSetPrepareSTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareSTableMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareSTableMeta);
    }
  }
}

void ctgTestSetPrepareMultiSTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareMultiSTableMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareMultiSTableMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndNormalMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndNormalMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndNormalMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndChildMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndChildMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndChildMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndSuperMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndSuperMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndSuperMeta);
    }
  }
}

void ctgTestSetPrepareDbVgroupsAndMultiSuperMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestPrepareDbVgroupsAndMultiSuperMeta);
  {
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto &f : result) {
      stub.set(f.second, ctgTestPrepareDbVgroupsAndMultiSuperMeta);
    }
  }
}

}  // namespace

void *ctgTestGetDbVgroupThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  void            *mockPointer = (void *)0x1;
  SArray          *vgList = NULL;
  int32_t          n = 0;

  while (!ctgTestStop) {
    code = catalogGetDBVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, false, &vgList);
    if (code) {
      assert(0);
    }

    if (vgList) {
      taosArrayDestroy(vgList);
    }

    if (ctgTestEnableSleep) {
      usleep(rand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Get:%d\n", n);
    }
  }

  return NULL;
}

void *ctgTestSetDbVgroupThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  SDBVgroupInfo    dbVgroup = {0};
  int32_t          n = 0;

  while (!ctgTestStop) {
    ctgTestBuildDBVgroup(&dbVgroup);
    code = catalogUpdateDBVgroup(pCtg, ctgTestDbname, &dbVgroup);
    if (code) {
      assert(0);
    }

    if (ctgTestEnableSleep) {
      usleep(rand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Set:%d\n", n);
    }
  }

  return NULL;
}

void *ctgTestGetCtableMetaThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  int32_t          n = 0;
  STableMeta      *tbMeta = NULL;
  int32_t          exist = 0;

  SName cn = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  while (!ctgTestStop) {
    code = ctgGetTableMetaFromCache(pCtg, &cn, &tbMeta, &exist);
    if (code || 0 == exist) {
      assert(0);
    }

    tfree(tbMeta);

    if (ctgTestEnableSleep) {
      usleep(rand() % 5);
    }

    if (++n % ctgTestPrintNum == 0) {
      printf("Get:%d\n", n);
    }
  }

  return NULL;
}

void *ctgTestSetCtableMetaThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  SDBVgroupInfo    dbVgroup = {0};
  int32_t          n = 0;
  STableMetaOutput output = {0};

  ctgTestBuildCTableMetaOutput(&output);

  while (!ctgTestStop) {
    code = ctgUpdateTableMetaCache(pCtg, &output);
    if (code) {
      assert(0);
    }

    if (ctgTestEnableSleep) {
      usleep(rand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Set:%d\n", n);
    }
  }

  tfree(output.tbMeta);

  return NULL;
}

TEST(tableMeta, normalTable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestSetPrepareDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epset.numOfEps, 3);

  ctgTestSetPrepareTableMeta();

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
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
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  SDbVgVersion       *dbs = NULL;
  SSTableMetaVersion *stb = NULL;
  uint32_t            dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t             i = 0;
  while (i < 5) {
    ++i;
    code = catalogGetExpiredDBs(pCtg, &dbs, &dbNum);
    ASSERT_EQ(code, 0);
    code = catalogGetExpiredSTables(pCtg, &stb, &stbNum);
    ASSERT_EQ(code, 0);

    if (dbNum) {
      printf("got expired db,dbId:%" PRId64 "\n", dbs->dbId);
      free(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 "\n", stb->suid);
      free(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    sleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 0);

  catalogDestroy();
}

TEST(tableMeta, childTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestSetPrepareDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
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
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
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

  strcpy(n.tname, ctgTestSTablename);
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  SDbVgVersion       *dbs = NULL;
  SSTableMetaVersion *stb = NULL;
  uint32_t            dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t             i = 0;
  while (i < 5) {
    ++i;
    code = catalogGetExpiredDBs(pCtg, &dbs, &dbNum);
    ASSERT_EQ(code, 0);
    code = catalogGetExpiredSTables(pCtg, &stb, &stbNum);
    ASSERT_EQ(code, 0);

    if (dbNum) {
      printf("got expired db,dbId:%" PRId64 "\n", dbs->dbId);
      free(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 "\n", stb->suid);
      free(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    sleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 1);

  catalogDestroy();
}

TEST(tableMeta, superTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestSetPrepareDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  ctgTestSetPrepareCTableMeta();

  tableMeta = NULL;

  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
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
  code = catalogRenewAndGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  SDbVgVersion       *dbs = NULL;
  SSTableMetaVersion *stb = NULL;
  uint32_t            dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t             i = 0;
  while (i < 5) {
    ++i;
    code = catalogGetExpiredDBs(pCtg, &dbs, &dbNum);
    ASSERT_EQ(code, 0);
    code = catalogGetExpiredSTables(pCtg, &stb, &stbNum);
    ASSERT_EQ(code, 0);

    if (dbNum) {
      printf("got expired db,dbId:%" PRId64 "\n", dbs->dbId);
      free(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 "\n", stb->suid);
      free(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    sleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 1);

  catalogDestroy();
}

TEST(tableDistVgroup, normalTable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndNormalMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 8);
  ASSERT_EQ(vgInfo->epset.numOfEps, 3);

  catalogDestroy();
}

TEST(tableDistVgroup, childTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 9);
  ASSERT_EQ(vgInfo->epset.numOfEps, 4);

  catalogDestroy();
}

TEST(tableDistVgroup, superTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 10);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 1);
  ASSERT_EQ(vgInfo->epset.numOfEps, 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 1);
  ASSERT_EQ(vgInfo->vgId, 2);
  ASSERT_EQ(vgInfo->epset.numOfEps, 2);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 2);
  ASSERT_EQ(vgInfo->vgId, 3);
  ASSERT_EQ(vgInfo->epset.numOfEps, 3);

  catalogDestroy();
}

TEST(dbVgroup, getSetDbVgroupCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgroupInfo    dbVgroup = {0};
  SArray          *vgList = NULL;

  ctgTestSetPrepareDbVgroupsAndNormalMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetDBVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, false, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), ctgTestVgNum);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epset.numOfEps, 3);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  pvgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(pvgInfo->vgId, 8);
  ASSERT_EQ(pvgInfo->epset.numOfEps, 3);
  taosArrayDestroy(vgList);

  ctgTestBuildDBVgroup(&dbVgroup);
  code = catalogUpdateDBVgroup(pCtg, ctgTestDbname, &dbVgroup);
  ASSERT_EQ(code, 0);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 7);
  ASSERT_EQ(vgInfo.epset.numOfEps, 2);

  code = catalogGetTableDistVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  pvgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(pvgInfo->vgId, 8);
  ASSERT_EQ(pvgInfo->epset.numOfEps, 3);
  taosArrayDestroy(vgList);

  catalogDestroy();
}

TEST(multiThread, getSetDbVgroupCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgroupInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetPrepareDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1, thread2;
  pthread_create(&(thread1), &thattr, ctgTestSetDbVgroupThread, pCtg);

  sleep(1);
  pthread_create(&(thread1), &thattr, ctgTestGetDbVgroupThread, pCtg);

  while (true) {
    if (ctgTestDeadLoop) {
      sleep(1);
    } else {
      sleep(ctgTestMTRunSec);
      break;
    }
  }

  ctgTestStop = true;
  sleep(1);

  catalogDestroy();
}

TEST(multiThread, ctableMeta) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgroupInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;

  ctgTestSetPrepareDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1, thread2;
  pthread_create(&(thread1), &thattr, ctgTestSetCtableMetaThread, pCtg);
  sleep(1);
  pthread_create(&(thread1), &thattr, ctgTestGetCtableMetaThread, pCtg);

  while (true) {
    if (ctgTestDeadLoop) {
      sleep(1);
    } else {
      sleep(ctgTestMTRunSec);
      break;
    }
  }

  ctgTestStop = true;
  sleep(1);

  catalogDestroy();
}

TEST(rentTest, allRent) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgroupInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;
  SDbVgVersion       *dbs = NULL;
  SSTableMetaVersion *stable = NULL;
  uint32_t            num = 0;

  ctgTestSetPrepareDbVgroupsAndMultiSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {.type = TSDB_TABLE_NAME_T, .acctId = 1};
  strcpy(n.dbname, "db1");

  for (int32_t i = 1; i <= 10; ++i) {
    sprintf(n.tname, "%s_%d", ctgTestSTablename, i);

    STableMeta *tableMeta = NULL;
    code = catalogGetSTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(tableMeta->vgId, 0);
    ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
    ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
    ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
    ASSERT_EQ(tableMeta->uid, ctgTestSuid + i);
    ASSERT_EQ(tableMeta->suid, ctgTestSuid + i);
    ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
    ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
    ASSERT_EQ(tableMeta->tableInfo.precision, 1);
    ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

    code = catalogGetExpiredDBs(pCtg, &dbs, &num);
    ASSERT_EQ(code, 0);
    printf("%d - expired dbNum:%d\n", i, num);
    if (dbs) {
      printf("%d - expired dbId:%" PRId64 ", vgVersion:%d\n", i, dbs->dbId, dbs->vgVersion);
      free(dbs);
      dbs = NULL;
    }

    code = catalogGetExpiredSTables(pCtg, &stable, &num);
    ASSERT_EQ(code, 0);
    printf("%d - expired stableNum:%d\n", i, num);
    if (stable) {
      for (int32_t n = 0; n < num; ++n) {
        printf("suid:%" PRId64 ", sversion:%d, tversion:%d\n", stable[n].suid, stable[n].sversion, stable[n].tversion);
      }
      free(stable);
      stable = NULL;
    }
    printf("*************************************************\n");

    sleep(2);
  }

  catalogDestroy();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop