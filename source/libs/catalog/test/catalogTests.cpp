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
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#include <addr_any.h>

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif
#include "os.h"
#include "tglobal.h"
#include "catalog.h"
#include "catalogInt.h"
#include "stub.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "trpc.h"
#include "tvariant.h"

namespace {

extern "C" int32_t ctgdGetClusterCacheNum(struct SCatalog* pCatalog, int32_t type);
extern "C" int32_t ctgdEnableDebug(char *option);
extern "C" int32_t ctgdGetStatNum(char *option, void *res);

void ctgTestSetRspTableMeta();
void ctgTestSetRspCTableMeta();
void ctgTestSetRspSTableMeta();
void ctgTestSetRspMultiSTableMeta();

//extern "C" SCatalogMgmt gCtgMgmt;

enum {
  CTGT_RSP_VGINFO = 1,
  CTGT_RSP_TBMETA,
  CTGT_RSP_CTBMETA,
  CTGT_RSP_STBMETA,
  CTGT_RSP_MSTBMETA,
  CTGT_RSP_TBMETA_NOT_EXIST,
};

bool    ctgTestStop = false;
bool    ctgTestEnableSleep = false;
bool    ctgTestEnableLog = true;
bool    ctgTestDeadLoop = false;
int32_t ctgTestPrintNum = 10000;
int32_t ctgTestMTRunSec = 5;

int32_t  ctgTestCurrentVgVersion = 0;
int32_t  ctgTestVgVersion = 1;
int32_t  ctgTestVgNum = 10;
int32_t  ctgTestColNum = 2;
int32_t  ctgTestTagNum = 1;
int32_t  ctgTestSVersion = 1;
int32_t  ctgTestTVersion = 1;
int32_t  ctgTestSuid = 2;
uint64_t ctgTestDbId = 33;
uint64_t ctgTestNormalTblUid = 1;

uint64_t ctgTestClusterId = 0x1;
char    *ctgTestDbname = "1.db1";
char    *ctgTestTablename = "table1";
char    *ctgTestCTablename = "ctable1";
char    *ctgTestSTablename = "stable1";
char    *ctgTestCurrentCTableName = NULL;
char    *ctgTestCurrentTableName = NULL;
char    *ctgTestCurrentSTableName = NULL;

int32_t ctgTestRspFunc[100] = {0};
int32_t ctgTestRspIdx = 0;

void sendCreateDbMsg(void *shandle, SEpSet *pEpSet) {
  SCreateDbReq createReq = {0};
  strcpy(createReq.db, "1.db1");
  createReq.numOfVgroups = 2;
  createReq.buffer = -1;
  createReq.pageSize = -1;
  createReq.pages = -1;
  createReq.daysPerFile = 10;
  createReq.daysToKeep0 = 3650;
  createReq.daysToKeep1 = 3650;
  createReq.daysToKeep2 = 3650;
  createReq.minRows = 100;
  createReq.maxRows = 4096;
  createReq.fsyncPeriod = 3000;
  createReq.walLevel = 1;
  createReq.precision = 0;
  createReq.compression = 2;
  createReq.replications = 1;
  createReq.strict = 1;
  createReq.cacheLastRow = 0;
  createReq.ignoreExist = 1;

  int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSCreateDbReq(pReq, contLen, &createReq);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_CREATE_DB;

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(shandle, pEpSet, &rpcMsg, &rpcRsp);

  ASSERT_EQ(rpcRsp.code, 0);
}

void ctgTestInitLogFile() {
  if (!ctgTestEnableLog) {
    return;
  }
  
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, "/var/log/taos");

  ctgdEnableDebug("api");
  ctgdEnableDebug("meta");
  ctgdEnableDebug("cache");
  
  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

int32_t ctgTestGetVgNumFromVgVersion(int32_t vgVersion) {
  return ((vgVersion % 2) == 0) ? ctgTestVgNum - 2 : ctgTestVgNum;
}

void ctgTestBuildCTableMetaOutput(STableMetaOutput *output) {
  SName cn = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  SName sn = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(sn.dbname, "db1");
  strcpy(sn.tname, ctgTestSTablename);

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&cn, db);

  strcpy(output->dbFName, db);
  SET_META_TYPE_BOTH_TABLE(output->metaType);

  strcpy(output->ctbName, cn.tname);
  strcpy(output->tbName, sn.tname);

  output->ctbMeta.vgId = 9;
  output->ctbMeta.tableType = TSDB_CHILD_TABLE;
  output->ctbMeta.uid = 3;
  output->ctbMeta.suid = 2;

  output->tbMeta = (STableMeta *)taosMemoryCalloc(1, sizeof(STableMeta) + sizeof(SSchema) * (ctgTestColNum + ctgTestColNum));
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

void ctgTestBuildDBVgroup(SDBVgInfo **pdbVgroup) {
  static int32_t vgVersion = ctgTestVgVersion + 1;
  int32_t        vgNum = 0;
  SVgroupInfo    vgInfo = {0};
  SDBVgInfo *dbVgroup = (SDBVgInfo *)taosMemoryCalloc(1, sizeof(SDBVgInfo));

  dbVgroup->vgVersion = vgVersion++;

  ctgTestCurrentVgVersion = dbVgroup->vgVersion;

  dbVgroup->hashMethod = 0;
  dbVgroup->vgHash = taosHashInit(ctgTestVgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);

  vgNum = ctgTestGetVgNumFromVgVersion(dbVgroup->vgVersion);
  uint32_t hashUnit = UINT32_MAX / vgNum;

  for (int32_t i = 0; i < vgNum; ++i) {
    vgInfo.vgId = i + 1;
    vgInfo.hashBegin = i * hashUnit;
    vgInfo.hashEnd = hashUnit * (i + 1) - 1;
    vgInfo.epSet.numOfEps = i % TSDB_MAX_REPLICA + 1;
    vgInfo.epSet.inUse = i % vgInfo.epSet.numOfEps;
    for (int32_t n = 0; n < vgInfo.epSet.numOfEps; ++n) {
      SEp *addr = &vgInfo.epSet.eps[n];
      strcpy(addr->fqdn, "a0");
      addr->port = n + 22;
    }

    taosHashPut(dbVgroup->vgHash, &vgInfo.vgId, sizeof(vgInfo.vgId), &vgInfo, sizeof(vgInfo));
  }

  *pdbVgroup = dbVgroup;
}

void ctgTestBuildSTableMetaRsp(STableMetaRsp *rspMsg) {
  strcpy(rspMsg->dbFName, ctgTestDbname);
  sprintf(rspMsg->tbName, "%s", ctgTestSTablename);
  sprintf(rspMsg->stbName, "%s", ctgTestSTablename);
  rspMsg->numOfTags = ctgTestTagNum;
  rspMsg->numOfColumns = ctgTestColNum;
  rspMsg->precision = 1 + 1;
  rspMsg->tableType = TSDB_SUPER_TABLE;
  rspMsg->sversion = ctgTestSVersion + 1;
  rspMsg->tversion = ctgTestTVersion + 1;
  rspMsg->suid = ctgTestSuid + 1;
  rspMsg->tuid = ctgTestSuid + 1;
  rspMsg->vgId = 1;
  
  rspMsg->pSchemas = (SSchema *)taosMemoryCalloc(rspMsg->numOfTags + rspMsg->numOfColumns, sizeof(SSchema));

  SSchema *s = NULL;
  s = &rspMsg->pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &rspMsg->pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1s");

  s = &rspMsg->pSchemas[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = 3;
  s->bytes = 12 + 1;
  strcpy(s->name, "tag1s");

  return;
}

void ctgTestRspDbVgroups(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SUseDbRsp usedbRsp = {0};
  strcpy(usedbRsp.db, ctgTestDbname);
  usedbRsp.vgVersion = ctgTestVgVersion;
  ctgTestCurrentVgVersion = ctgTestVgVersion;
  usedbRsp.vgNum = ctgTestVgNum;
  usedbRsp.hashMethod = 0;
  usedbRsp.uid = ctgTestDbId;
  usedbRsp.pVgroupInfos = taosArrayInit(usedbRsp.vgNum, sizeof(SVgroupInfo));

  uint32_t hashUnit = UINT32_MAX / ctgTestVgNum;
  for (int32_t i = 0; i < ctgTestVgNum; ++i) {
    SVgroupInfo vg = {0};
    vg.vgId = i + 1;
    vg.hashBegin = i * hashUnit;
    vg.hashEnd = hashUnit * (i + 1) - 1;
    if (i == ctgTestVgNum - 1) {
      vg.hashEnd = htonl(UINT32_MAX);
    }

    vg.epSet.numOfEps = i % TSDB_MAX_REPLICA + 1;
    vg.epSet.inUse = i % vg.epSet.numOfEps;
    for (int32_t n = 0; n < vg.epSet.numOfEps; ++n) {
      SEp *addr = &vg.epSet.eps[n];
      strcpy(addr->fqdn, "a0");
      addr->port = n + 22;
    }

    taosArrayPush(usedbRsp.pVgroupInfos, &vg);
  }

  int32_t contLen = tSerializeSUseDbRsp(NULL, 0, &usedbRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSUseDbRsp(pReq, contLen, &usedbRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;
}

void ctgTestRspTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp metaRsp = {0};
  strcpy(metaRsp.dbFName, ctgTestDbname);
  strcpy(metaRsp.tbName, ctgTestTablename);
  metaRsp.numOfTags = 0;
  metaRsp.numOfColumns = ctgTestColNum;
  metaRsp.precision = 1;
  metaRsp.tableType = TSDB_NORMAL_TABLE;
  metaRsp.sversion = ctgTestSVersion;
  metaRsp.tversion = ctgTestTVersion;
  metaRsp.suid = 0;
  metaRsp.tuid = ctgTestNormalTblUid++;
  metaRsp.vgId = 8;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));

  SSchema *s = NULL;
  s = &metaRsp.pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &metaRsp.pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1");

  int32_t contLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableMetaRsp(pReq, contLen, &metaRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableMetaRsp(&metaRsp);
}

void ctgTestRspTableMetaNotExist(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  pRsp->code = CTG_ERR_CODE_TABLE_NOT_EXIST;
}


void ctgTestRspCTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp metaRsp = {0};
  strcpy(metaRsp.dbFName, ctgTestDbname);
  strcpy(metaRsp.tbName, ctgTestCurrentCTableName ? ctgTestCurrentCTableName : ctgTestCTablename);
  strcpy(metaRsp.stbName, ctgTestSTablename);
  metaRsp.numOfTags = ctgTestTagNum;
  metaRsp.numOfColumns = ctgTestColNum;
  metaRsp.precision = 1;
  metaRsp.tableType = TSDB_CHILD_TABLE;
  metaRsp.sversion = ctgTestSVersion;
  metaRsp.tversion = ctgTestTVersion;
  metaRsp.suid = 0x0000000000000002;
  metaRsp.tuid = 0x0000000000000003;
  metaRsp.vgId = 9;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));

  SSchema *s = NULL;
  s = &metaRsp.pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &metaRsp.pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1s");

  s = &metaRsp.pSchemas[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = 3;
  s->bytes = 12;
  strcpy(s->name, "tag1s");

  int32_t contLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableMetaRsp(pReq, contLen, &metaRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableMetaRsp(&metaRsp);
}

void ctgTestRspSTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  STableMetaRsp metaRsp = {0};
  strcpy(metaRsp.dbFName, ctgTestDbname);
  strcpy(metaRsp.tbName, ctgTestCurrentSTableName ? ctgTestCurrentSTableName : ctgTestSTablename);
  strcpy(metaRsp.stbName, ctgTestSTablename);
  metaRsp.numOfTags = ctgTestTagNum;
  metaRsp.numOfColumns = ctgTestColNum;
  metaRsp.precision = 1;
  metaRsp.tableType = TSDB_SUPER_TABLE;
  metaRsp.sversion = ctgTestSVersion;
  metaRsp.tversion = ctgTestTVersion;
  metaRsp.suid = ctgTestSuid;
  metaRsp.tuid = ctgTestSuid++;
  metaRsp.vgId = 0;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));

  SSchema *s = NULL;
  s = &metaRsp.pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &metaRsp.pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1s");

  s = &metaRsp.pSchemas[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = 3;
  s->bytes = 12;
  strcpy(s->name, "tag1s");

  int32_t contLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableMetaRsp(pReq, contLen, &metaRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableMetaRsp(&metaRsp);
}

void ctgTestRspMultiSTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  static int32_t idx = 1;

  STableMetaRsp metaRsp = {0};
  strcpy(metaRsp.dbFName, ctgTestDbname);
  sprintf(metaRsp.tbName, "%s_%d", ctgTestSTablename, idx);
  sprintf(metaRsp.stbName, "%s_%d", ctgTestSTablename, idx);
  metaRsp.numOfTags = ctgTestTagNum;
  metaRsp.numOfColumns = ctgTestColNum;
  metaRsp.precision = 1;
  metaRsp.tableType = TSDB_SUPER_TABLE;
  metaRsp.sversion = ctgTestSVersion;
  metaRsp.tversion = ctgTestTVersion;
  metaRsp.suid = ctgTestSuid + idx;
  metaRsp.tuid = ctgTestSuid + idx;
  metaRsp.vgId = 0;
  metaRsp.pSchemas = (SSchema *)taosMemoryMalloc((metaRsp.numOfTags + metaRsp.numOfColumns) * sizeof(SSchema));

  SSchema *s = NULL;
  s = &metaRsp.pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &metaRsp.pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1s");

  s = &metaRsp.pSchemas[2];
  s->type = TSDB_DATA_TYPE_BINARY;
  s->colId = 3;
  s->bytes = 12;
  strcpy(s->name, "tag1s");

  ++idx;

  int32_t contLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableMetaRsp(pReq, contLen, &metaRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableMetaRsp(&metaRsp);
}

void ctgTestRspByIdx(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (ctgTestRspFunc[ctgTestRspIdx]) {
    case CTGT_RSP_VGINFO:
      ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_TBMETA:
      ctgTestRspTableMeta(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_CTBMETA:
      ctgTestRspCTableMeta(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_STBMETA:
      ctgTestRspSTableMeta(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_MSTBMETA:
      ctgTestRspMultiSTableMeta(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_TBMETA_NOT_EXIST:
      ctgTestRspTableMetaNotExist(shandle, pEpSet, pMsg, pRsp);
      break;
    default:
      break;
  }

  ctgTestRspIdx++;

  return;
}

void ctgTestRspDbVgroupsAndNormalMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);

  ctgTestSetRspTableMeta();

  return;
}

void ctgTestRspDbVgroupsAndChildMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);

  ctgTestSetRspCTableMeta();

  return;
}

void ctgTestRspDbVgroupsAndSuperMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);

  ctgTestSetRspSTableMeta();

  return;
}

void ctgTestRspDbVgroupsAndMultiSuperMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);

  ctgTestSetRspMultiSTableMeta();

  return;
}

void ctgTestSetRspDbVgroups() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspDbVgroups);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroups);
    }
  }
}

void ctgTestSetRspTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspTableMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspTableMeta);
    }
  }
}

void ctgTestSetRspCTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspCTableMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspCTableMeta);
    }
  }
}

void ctgTestSetRspSTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspSTableMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspSTableMeta);
    }
  }
}

void ctgTestSetRspMultiSTableMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspMultiSTableMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspMultiSTableMeta);
    }
  }
}

void ctgTestSetRspByIdx() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspByIdx);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspByIdx);
    }
  }
}


void ctgTestSetRspDbVgroupsAndNormalMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspDbVgroupsAndNormalMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroupsAndNormalMeta);
    }
  }
}

void ctgTestSetRspDbVgroupsAndChildMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspDbVgroupsAndChildMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroupsAndChildMeta);
    }
  }
}

void ctgTestSetRspDbVgroupsAndSuperMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspDbVgroupsAndSuperMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroupsAndSuperMeta);
    }
  }
}

void ctgTestSetRspDbVgroupsAndMultiSuperMeta() {
  static Stub stub;
  stub.set(rpcSendRecv, ctgTestRspDbVgroupsAndMultiSuperMeta);
  {
#ifdef WINDOWS
    AddrAny any;
    std::map<std::string,void*> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroupsAndMultiSuperMeta);
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
    code = catalogGetDBVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, &vgList);
    if (code) {
      assert(0);
    }

    if (vgList) {
      taosArrayDestroy(vgList);
    }

    if (ctgTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Get:%d\n", n);
    }
  }

  return NULL;
}

void *ctgTestSetSameDbVgroupThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  SDBVgInfo       *dbVgroup = NULL;
  int32_t          n = 0;

  while (!ctgTestStop) {
    ctgTestBuildDBVgroup(&dbVgroup);
    code = catalogUpdateDBVgInfo(pCtg, ctgTestDbname, ctgTestDbId, dbVgroup);
    if (code) {
      assert(0);
    }

    if (ctgTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Set:%d\n", n);
    }
  }

  return NULL;
}

void *ctgTestSetDiffDbVgroupThread(void *param) {
  struct SCatalog *pCtg = (struct SCatalog *)param;
  int32_t          code = 0;
  SDBVgInfo    *dbVgroup = NULL;
  int32_t          n = 0;

  while (!ctgTestStop) {
    ctgTestBuildDBVgroup(&dbVgroup);
    code = catalogUpdateDBVgInfo(pCtg, ctgTestDbname, ctgTestDbId++, dbVgroup);
    if (code) {
      assert(0);
    }

    if (ctgTestEnableSleep) {
      taosUsleep(taosRand() % 5);
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
  bool             inCache = false;

  SName cn = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = &cn;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;

  while (!ctgTestStop) {
    code = ctgReadTbMetaFromCache(pCtg, &ctx, &tbMeta);
    if (code || !inCache) {
      assert(0);
    }

    taosMemoryFreeClear(tbMeta);

    if (ctgTestEnableSleep) {
      taosUsleep(taosRand() % 5);
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
  SDBVgInfo    dbVgroup = {0};
  int32_t          n = 0;
  STableMetaOutput *output = NULL;

  SCtgCacheOperation operation = {0};
  
  operation.opId = CTG_OP_UPDATE_TB_META;

  while (!ctgTestStop) {
    output = (STableMetaOutput *)taosMemoryMalloc(sizeof(STableMetaOutput));
    ctgTestBuildCTableMetaOutput(output);

    SCtgUpdateTblMsg *msg = (SCtgUpdateTblMsg *)taosMemoryMalloc(sizeof(SCtgUpdateTblMsg));
    msg->pCtg = pCtg;
    msg->output = output;
    operation.data = msg;

    code = ctgOpUpdateTbMeta(&operation);
    if (code) {
      assert(0);
    }

    if (ctgTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % ctgTestPrintNum == 0) {
      printf("Set:%d\n", n);
    }
  }

  return NULL;
}

#if 1


TEST(tableMeta, normalTable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM)) {
    taosMsleep(50);
  }
  
  ctgTestSetRspTableMeta();

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->uid, ctgTestNormalTblUid - 1);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  
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
      taosMemoryFree(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 ",dbFName:%s, stbName:%s\n", stb->suid, stb->dbFName, stb->stbName);
      taosMemoryFree(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    taosSsleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 0);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableMeta, childTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
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

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }


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

  taosMemoryFreeClear(tableMeta);

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
      taosMemoryFree(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 ",dbFName:%s, stbName:%s\n", stb->suid, stb->dbFName, stb->stbName);
      taosMemoryFree(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    taosSsleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 1);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableMeta, superTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  ctgTestSetRspCTableMeta();

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

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (2 != n) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  tableMeta = NULL;
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
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
      taosMemoryFree(dbs);
      dbs = NULL;
    } else {
      printf("no expired db\n");
    }

    if (stbNum) {
      printf("got expired stb,suid:%" PRId64 ",dbFName:%s, stbName:%s\n", stb->suid, stb->dbFName, stb->stbName);

      taosMemoryFree(stb);
      stb = NULL;
    } else {
      printf("no expired stb\n");
    }

    allDbNum += dbNum;
    allStbNum += stbNum;
    taosSsleep(2);
  }

  ASSERT_EQ(allDbNum, 1);
  ASSERT_EQ(allStbNum, 1);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableMeta, rmStbMeta) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  code = catalogRemoveStbMeta(pCtg, "1.db1", ctgTestDbId, ctgTestSTablename, ctgTestSuid - 1);
  ASSERT_EQ(code, 0);

  while (true) {
    int32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    int32_t m = ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM);
    if (n || m) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM), 0);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_NUM), 0);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_RENT_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM), 0);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableMeta, updateStbMeta) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  taosMemoryFreeClear(tableMeta);

  STableMetaRsp rsp = {0};
  ctgTestBuildSTableMetaRsp(&rsp);

  code = catalogUpdateSTableMeta(pCtg, &rsp);
  ASSERT_EQ(code, 0);
  taosMemoryFreeClear(rsp.pSchemas);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n != 3) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_DB_RENT_NUM), 1);
  ASSERT_EQ(ctgdGetClusterCacheNum(pCtg, CTG_DBG_STB_RENT_NUM), 1);

  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion + 1);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion + 1);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid + 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid + 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1 + 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt.stat, 0, sizeof(gCtgMgmt.stat));
}

TEST(refreshGetMeta, normal2normal) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_TBMETA;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->uid, ctgTestNormalTblUid - 1);  
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->uid, ctgTestNormalTblUid - 1);  
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(refreshGetMeta, normal2notexist) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_TBMETA_NOT_EXIST;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->uid, ctgTestNormalTblUid - 1);  
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, CTG_ERR_CODE_TABLE_NOT_EXIST);
  ASSERT_TRUE(tableMeta == NULL);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}


TEST(refreshGetMeta, normal2child) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_CTBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_STBMETA;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentCTableName = ctgTestTablename;
  ctgTestCurrentSTableName = ctgTestSTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->uid, ctgTestNormalTblUid - 1);  
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(refreshGetMeta, stable2child) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_CTBMETA;
  ctgTestRspFunc[4] = CTGT_RSP_STBMETA;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentSTableName = ctgTestTablename;
  ctgTestCurrentCTableName = ctgTestTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  ctgTestCurrentSTableName = ctgTestSTablename;
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(refreshGetMeta, stable2stable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[4] = CTGT_RSP_STBMETA;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentSTableName = ctgTestTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}


TEST(refreshGetMeta, child2stable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_CTBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[4] = CTGT_RSP_STBMETA;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentCTableName = ctgTestTablename;
  ctgTestCurrentSTableName = ctgTestSTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

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
  taosMemoryFreeClear(tableMeta);

  while (2 != ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  ctgTestCurrentSTableName = ctgTestTablename;
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid - 1);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}


TEST(tableDistVgroup, normalTable) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_VGINFO;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 8);
  ASSERT_EQ(vgInfo->epSet.numOfEps, 3);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableDistVgroup, childTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_CTBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_VGINFO;
  
  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 9);
  ASSERT_EQ(vgInfo->epSet.numOfEps, 4);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(tableDistVgroup, superTableCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo     *vgInfo = NULL;
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[2] = CTGT_RSP_STBMETA;
  ctgTestRspFunc[3] = CTGT_RSP_VGINFO;
  
  ctgTestSetRspByIdx();



  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 10);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(vgInfo->vgId, 1);
  ASSERT_EQ(vgInfo->epSet.numOfEps, 1);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 1);
  ASSERT_EQ(vgInfo->vgId, 2);
  ASSERT_EQ(vgInfo->epSet.numOfEps, 2);
  vgInfo = (SVgroupInfo *)taosArrayGet(vgList, 2);
  ASSERT_EQ(vgInfo->vgId, 3);
  ASSERT_EQ(vgInfo->epSet.numOfEps, 3);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(dbVgroup, getSetDbVgroupCase) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgInfo       *dbVgroup = NULL;
  SArray          *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;

  
  ctgTestSetRspByIdx();


  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetDBVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, ctgTestDbname, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), ctgTestVgNum);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  pvgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(pvgInfo->vgId, 8);
  ASSERT_EQ(pvgInfo->epSet.numOfEps, 3);
  taosArrayDestroy(vgList);

  ctgTestBuildDBVgroup(&dbVgroup);
  code = catalogUpdateDBVgInfo(pCtg, ctgTestDbname, ctgTestDbId, dbVgroup);
  ASSERT_EQ(code, 0);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.qDoneNum", (void *)&n);
    if (n != 3) {
      taosMsleep(50);
    } else {
      break;
    }
  }


  code = catalogGetTableHashVgroup(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 7);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 2);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, (const SEpSet *)mockPointer, &n, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), 1);
  pvgInfo = (SVgroupInfo *)taosArrayGet(vgList, 0);
  ASSERT_EQ(pvgInfo->vgId, 8);
  ASSERT_EQ(pvgInfo->epSet.numOfEps, 3);
  taosArrayDestroy(vgList);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(multiThread, getSetRmSameDbVgroup) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2;
  taosThreadCreate(&(thread1), &thattr, ctgTestSetSameDbVgroupThread, pCtg);

  taosSsleep(1);
  taosThreadCreate(&(thread2), &thattr, ctgTestGetDbVgroupThread, pCtg);

  while (true) {
    if (ctgTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(ctgTestMTRunSec);
      break;
    }
  }

  ctgTestStop = true;
  taosSsleep(1);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(multiThread, getSetRmDiffDbVgroup) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2;
  taosThreadCreate(&(thread1), &thattr, ctgTestSetDiffDbVgroupThread, pCtg);

  taosSsleep(1);
  taosThreadCreate(&(thread2), &thattr, ctgTestGetDbVgroupThread, pCtg);

  while (true) {
    if (ctgTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(ctgTestMTRunSec);
      break;
    }
  }

  ctgTestStop = true;
  taosSsleep(1);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(multiThread, ctableMeta) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2;
  taosThreadCreate(&(thread1), &thattr, ctgTestSetCtableMetaThread, pCtg);
  taosSsleep(1);
  taosThreadCreate(&(thread1), &thattr, ctgTestGetCtableMetaThread, pCtg);

  while (true) {
    if (ctgTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(ctgTestMTRunSec);
      break;
    }
  }

  ctgTestStop = true;
  taosSsleep(2);

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

TEST(rentTest, allRent) {
  struct SCatalog *pCtg = NULL;
  void            *mockPointer = (void *)0x1;
  SVgroupInfo      vgInfo = {0};
  SVgroupInfo     *pvgInfo = NULL;
  SDBVgInfo    dbVgroup = {0};
  SArray          *vgList = NULL;
  ctgTestStop = false;
  SDbVgVersion       *dbs = NULL;
  SSTableMetaVersion *stable = NULL;
  uint32_t            num = 0;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndMultiSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = { TSDB_TABLE_NAME_T, 1, {0}, {0} };
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

    while (ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM) < i) {
      taosMsleep(50);
    }

    code = catalogGetExpiredDBs(pCtg, &dbs, &num);
    ASSERT_EQ(code, 0);
    printf("%d - expired dbNum:%d\n", i, num);
    if (dbs) {
      printf("%d - expired dbId:%" PRId64 ", vgVersion:%d\n", i, dbs->dbId, dbs->vgVersion);
      taosMemoryFree(dbs);
      dbs = NULL;
    }

    code = catalogGetExpiredSTables(pCtg, &stable, &num);
    ASSERT_EQ(code, 0);
    printf("%d - expired stableNum:%d\n", i, num);
    if (stable) {
      for (int32_t n = 0; n < num; ++n) {
        printf("suid:%" PRId64 ", dbFName:%s, stbName:%s, sversion:%d, tversion:%d\n", stable[n].suid,
               stable[n].dbFName, stable[n].stbName, stable[n].sversion, stable[n].tversion);
      }
      taosMemoryFree(stable);
      stable = NULL;
    }
    printf("*************************************************\n");

    taosSsleep(2);
  }

  catalogDestroy();
  memset(&gCtgMgmt, 0, sizeof(gCtgMgmt));
}

#endif

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
