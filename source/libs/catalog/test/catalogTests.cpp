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
#include "catalog.h"
#include "catalogInt.h"
#include "os.h"
#include "stub.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "trpc.h"
#include "tvariant.h"
#include "ttimer.h"

namespace {

extern "C" int32_t ctgdGetClusterCacheNum(struct SCatalog *pCatalog, int32_t type);
extern "C" int32_t ctgdGetStatNum(char *option, void *res);

void ctgTestSetRspTableMeta();
void ctgTestSetRspCTableMeta();
void ctgTestSetRspSTableMeta();
void ctgTestSetRspMultiSTableMeta();

extern int32_t clientConnRefPool;

enum {
  CTGT_RSP_VGINFO = 1,
  CTGT_RSP_TBMETA,
  CTGT_RSP_CTBMETA,
  CTGT_RSP_STBMETA,
  CTGT_RSP_MSTBMETA,
  CTGT_RSP_INDEXINFO_E,
  CTGT_RSP_USERAUTH,
  CTGT_RSP_TBLCFG,
  CTGT_RSP_TBLINDEX,
  CTGT_RSP_DBCFG,
  CTGT_RSP_QNODELIST,
  CTGT_RSP_UDF,
  CTGT_RSP_SVRVER,
  CTGT_RSP_DNODElIST,
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
int32_t  ctgTestQnodeNum = 3;
int32_t  ctgTestIndexNum = 3;
int32_t  ctgTestFuncNum = 2;
int32_t  ctgTestFuncType = 3;
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
char    *ctgTestUsername = "user1";
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
  createReq.walFsyncPeriod = 3000;
  createReq.walLevel = 1;
  createReq.precision = 0;
  createReq.compression = 2;
  createReq.replications = 1;
  createReq.strict = 1;
  createReq.cacheLast = 0;
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
  tmrDebugFlag = 159;
  strcpy(tsLogDir, TD_LOG_DIR_PATH);

  ctgdEnableDebug("api", true);
  ctgdEnableDebug("meta", true);
  ctgdEnableDebug("cache", true);
  ctgdEnableDebug("lock", true);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

int32_t ctgTestGetVgNumFromVgVersion(int32_t vgVersion) {
  return ((vgVersion % 2) == 0) ? ctgTestVgNum - 2 : ctgTestVgNum;
}

void ctgTestBuildCTableMetaOutput(STableMetaOutput *output) {
  SName cn = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  SName sn = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
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

  output->tbMeta =
      (STableMeta *)taosMemoryCalloc(1, sizeof(STableMeta) + sizeof(SSchema) * (ctgTestColNum + ctgTestColNum));
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
  SDBVgInfo     *dbVgroup = (SDBVgInfo *)taosMemoryCalloc(1, sizeof(SDBVgInfo));

  dbVgroup->vgVersion = vgVersion++;

  ctgTestCurrentVgVersion = dbVgroup->vgVersion;

  dbVgroup->hashMethod = 0;
  dbVgroup->hashPrefix = 0;
  dbVgroup->hashSuffix = 0;
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
  rspMsg->suid = ctgTestSuid;
  rspMsg->tuid = ctgTestSuid;
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
  rpcFreeCont(pMsg->pCont);

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
    vg.numOfTable = i % 2;

    taosArrayPush(usedbRsp.pVgroupInfos, &vg);
  }

  int32_t contLen = tSerializeSUseDbRsp(NULL, 0, &usedbRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSUseDbRsp(pReq, contLen, &usedbRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  taosArrayDestroy(usedbRsp.pVgroupInfos);
}

void ctgTestRspTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

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
  rpcFreeCont(pMsg->pCont);

  pRsp->code = CTG_ERR_CODE_TABLE_NOT_EXIST;
}

void ctgTestRspCTableMeta(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

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
  rpcFreeCont(pMsg->pCont);

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
  metaRsp.tuid = ctgTestSuid + 1;
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
  rpcFreeCont(pMsg->pCont);

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

void ctgTestRspErrIndexInfo(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  pRsp->code = TSDB_CODE_MND_DB_INDEX_NOT_EXIST;
  pRsp->contLen = 0;
  pRsp->pCont = NULL;
}

void ctgTestRspUserAuth(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  SGetUserAuthRsp userRsp = {0};
  strcpy(userRsp.user, ctgTestUsername);
  userRsp.version = 1;
  userRsp.superAuth = 1;
  userRsp.enable = 1;

  int32_t contLen = tSerializeSGetUserAuthRsp(NULL, 0, &userRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSGetUserAuthRsp(pReq, contLen, &userRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;
}

void ctgTestRspTableCfg(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  static int32_t idx = 1;

  STableCfgRsp tblRsp = {0};
  strcpy(tblRsp.tbName, ctgTestTablename);
  tblRsp.numOfColumns = ctgTestColNum;

  tblRsp.pSchemas = (SSchema *)taosMemoryMalloc((tblRsp.numOfTags + tblRsp.numOfColumns) * sizeof(SSchema));

  SSchema *s = NULL;
  s = &tblRsp.pSchemas[0];
  s->type = TSDB_DATA_TYPE_TIMESTAMP;
  s->colId = 1;
  s->bytes = 8;
  strcpy(s->name, "ts");

  s = &tblRsp.pSchemas[1];
  s->type = TSDB_DATA_TYPE_INT;
  s->colId = 2;
  s->bytes = 4;
  strcpy(s->name, "col1");

  int32_t contLen = tSerializeSTableCfgRsp(NULL, 0, &tblRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableCfgRsp(pReq, contLen, &tblRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableCfgRsp(&tblRsp);
}

void ctgTestRspTableIndex(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  static int32_t idx = 1;

  STableIndexRsp tblRsp = {0};
  strcpy(tblRsp.tbName, ctgTestSTablename);

  tblRsp.pIndex = taosArrayInit(ctgTestIndexNum, sizeof(STableIndexInfo));

  STableIndexInfo info = {0};
  for (int32_t i = 0; i < ctgTestIndexNum; ++i) {
    info.interval = 1 + i;
    info.expr = (char *)taosMemoryCalloc(1, 10);
    taosArrayPush(tblRsp.pIndex, &info);
  }

  int32_t contLen = tSerializeSTableIndexRsp(NULL, 0, &tblRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSTableIndexRsp(pReq, contLen, &tblRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;

  tFreeSTableIndexRsp(&tblRsp);
}

void ctgTestRspDBCfg(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  static int32_t idx = 1;

  SDbCfgRsp dbRsp = {0};
  dbRsp.numOfVgroups = ctgTestVgNum;

  int32_t contLen = tSerializeSDbCfgRsp(NULL, 0, &dbRsp);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSDbCfgRsp(pReq, contLen, &dbRsp);

  pRsp->code = 0;
  pRsp->contLen = contLen;
  pRsp->pCont = pReq;
}

void ctgTestRspQnodeList(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  SQnodeListRsp qlistRsp = {0};
  qlistRsp.qnodeList = taosArrayInit(10, sizeof(SQueryNodeLoad));
  for (int32_t i = 0; i < ctgTestQnodeNum; ++i) {
    SQueryNodeLoad nodeLoad = {0};
    nodeLoad.addr.nodeId = i;

    (void)taosArrayPush(qlistRsp.qnodeList, &nodeLoad);
  }

  int32_t rspLen = tSerializeSQnodeListRsp(NULL, 0, &qlistRsp);
  void   *pReq = rpcMallocCont(rspLen);
  tSerializeSQnodeListRsp(pReq, rspLen, &qlistRsp);

  pRsp->code = 0;
  pRsp->contLen = rspLen;
  pRsp->pCont = pReq;

  tFreeSQnodeListRsp(&qlistRsp);
}

void ctgTestRspUdfInfo(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  SRetrieveFuncRsp funcRsp = {0};
  funcRsp.numOfFuncs = 1;
  funcRsp.pFuncInfos = taosArrayInit(1, sizeof(SFuncInfo));
  funcRsp.pFuncExtraInfos = taosArrayInit(1, sizeof(SFuncExtraInfo));
  SFuncInfo funcInfo = {0};
  strcpy(funcInfo.name, "func1");
  funcInfo.funcType = ctgTestFuncType;

  (void)taosArrayPush(funcRsp.pFuncInfos, &funcInfo);
  SFuncExtraInfo extraInfo = {0};
  extraInfo.funcVersion = 0;
  extraInfo.funcCreatedTime = taosGetTimestampMs();
  (void)taosArrayPush(funcRsp.pFuncExtraInfos, &extraInfo);
  
  int32_t rspLen = tSerializeSRetrieveFuncRsp(NULL, 0, &funcRsp);
  void   *pReq = rpcMallocCont(rspLen);
  tSerializeSRetrieveFuncRsp(pReq, rspLen, &funcRsp);

  pRsp->code = 0;
  pRsp->contLen = rspLen;
  pRsp->pCont = pReq;

  tFreeSRetrieveFuncRsp(&funcRsp);
}

void ctgTestRspSvrVer(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  SServerVerRsp verRsp = {0};
  strcpy(verRsp.ver, "1.0");

  int32_t rspLen = tSerializeSServerVerRsp(NULL, 0, &verRsp);
  void   *pReq = rpcMallocCont(rspLen);
  tSerializeSServerVerRsp(pReq, rspLen, &verRsp);

  pRsp->code = 0;
  pRsp->contLen = rspLen;
  pRsp->pCont = pReq;
}

void ctgTestRspDndeList(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  rpcFreeCont(pMsg->pCont);

  SDnodeListRsp dRsp = {0};
  dRsp.dnodeList = taosArrayInit(1, sizeof(SEpSet));
  SEpSet epSet = {0};
  epSet.numOfEps = 1;
  tstrncpy(epSet.eps[0].fqdn, "localhost", TSDB_FQDN_LEN);
  epSet.eps[0].port = 6030;

  (void)taosArrayPush(dRsp.dnodeList, &epSet);

  int32_t rspLen = tSerializeSDnodeListRsp(NULL, 0, &dRsp);
  void   *pReq = rpcMallocCont(rspLen);
  tSerializeSDnodeListRsp(pReq, rspLen, &dRsp);

  pRsp->code = 0;
  pRsp->contLen = rspLen;
  pRsp->pCont = pReq;

  tFreeSDnodeListRsp(&dRsp);
}

void ctgTestRspAuto(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (pMsg->msgType) {
    case TDMT_MND_USE_DB:
      ctgTestRspDbVgroups(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_VND_TABLE_CFG:
    case TDMT_MND_TABLE_CFG:
      ctgTestRspTableCfg(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_GET_TABLE_INDEX:
      ctgTestRspTableIndex(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_GET_DB_CFG:
      ctgTestRspDBCfg(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_QNODE_LIST:
      ctgTestRspQnodeList(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_RETRIEVE_FUNC:
      ctgTestRspUdfInfo(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_SERVER_VERSION:
      ctgTestRspSvrVer(shandle, pEpSet, pMsg, pRsp);
      break;
    case TDMT_MND_DNODE_LIST:
      ctgTestRspDndeList(shandle, pEpSet, pMsg, pRsp);
      break;
    default:
      break;
  }

  return;
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
    case CTGT_RSP_INDEXINFO_E:
      ctgTestRspErrIndexInfo(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_USERAUTH:
      ctgTestRspUserAuth(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_TBLCFG:
      ctgTestRspTableCfg(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_TBMETA_NOT_EXIST:
      ctgTestRspTableMetaNotExist(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_TBLINDEX:
      ctgTestRspTableIndex(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_DBCFG:
      ctgTestRspDBCfg(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_QNODELIST:
      ctgTestRspQnodeList(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_UDF:
      ctgTestRspUdfInfo(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_SVRVER:
      ctgTestRspSvrVer(shandle, pEpSet, pMsg, pRsp);
      break;
    case CTGT_RSP_DNODElIST:
      ctgTestRspDndeList(shandle, pEpSet, pMsg, pRsp);
      break;
    default:
      ctgTestRspAuto(shandle, pEpSet, pMsg, pRsp);
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
    AddrAny                       any;
    std::map<std::string, void *> result;
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
  struct SCatalog  *pCtg = (struct SCatalog *)param;
  int32_t           code = 0;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SArray           *vgList = NULL;
  int32_t           n = 0;

  while (!ctgTestStop) {
    code = catalogGetDBVgList(pCtg, mockPointer, ctgTestDbname, &vgList);
    if (code) {
      printf("code:%x\n", code);
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
  SDBVgInfo       *dbVgroup = NULL;
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

  SName cn = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(cn.dbname, "db1");
  strcpy(cn.tname, ctgTestCTablename);

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = &cn;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;

  while (!ctgTestStop) {
    code = ctgReadTbMetaFromCache(pCtg, &ctx, &tbMeta);
    if (code || NULL == tbMeta) {
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
  struct SCatalog  *pCtg = (struct SCatalog *)param;
  int32_t           code = 0;
  SDBVgInfo         dbVgroup = {0};
  int32_t           n = 0;
  STableMetaOutput *output = NULL;

  SCtgCacheOperation operation = {0};

  operation.opId = CTG_OP_UPDATE_TB_META;

  while (!ctgTestStop) {
    output = (STableMetaOutput *)taosMemoryMalloc(sizeof(STableMetaOutput));
    ctgTestBuildCTableMetaOutput(output);

    SCtgUpdateTbMetaMsg *msg = (SCtgUpdateTbMetaMsg *)taosMemoryMalloc(sizeof(SCtgUpdateTbMetaMsg));
    msg->pCtg = pCtg;
    msg->pMeta = output;
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

void ctgTestFetchRows(TAOS_RES *result, int32_t *rows) {
  TAOS_ROW    row;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    (*rows)++;
    memset(temp, 0, sizeof(temp));
    taos_print_row(temp, row, fields, num_fields);
    printf("\t[%s]\n", temp);
  }
}

void ctgTestExecQuery(TAOS *taos, char *sql, bool fetch, int32_t *rows) {
  TAOS_RES *result = taos_query(taos, sql);
  int       code = taos_errno(result);
  ASSERT_EQ(code, 0);

  if (fetch) {
    ctgTestFetchRows(result, rows);
  }

  taos_free_result(result);
}

TEST(tableMeta, normalTable) {
  struct SCatalog  *pCtg = NULL;
  SVgroupInfo       vgInfo = {0};
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n != 1) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  memset(&vgInfo, 0, sizeof(vgInfo));
  bool exists = false;
  code = catalogGetCachedTableHashVgroup(pCtg, &n, &vgInfo, &exists);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);
  ASSERT_EQ(exists, true);

  ctgTestSetRspTableMeta();

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  taosMemoryFree(tableMeta);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFree(tableMeta);

  tableMeta = NULL;
  catalogGetCachedTableMeta(pCtg, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 8);
  ASSERT_EQ(tableMeta->tableType, TSDB_NORMAL_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, 0);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  SDbCacheInfo   *dbs = NULL;
  SSTableVersion *stb = NULL;
  uint32_t        dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t         i = 0;
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
}

TEST(tableMeta, childTableCase) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFree(tableMeta);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFree(tableMeta);

  SDbCacheInfo   *dbs = NULL;
  SSTableVersion *stb = NULL;
  uint32_t        dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t         i = 0;
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
}

TEST(tableMeta, superTableCase) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  taosMemoryFree(tableMeta);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  tableMeta = NULL;
  code = catalogGetCachedSTableMeta(pCtg, &n, &tableMeta);
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
  taosMemoryFree(tableMeta);

  ctgTestSetRspCTableMeta();

  tableMeta = NULL;

  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFree(tableMeta);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (2 != n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  tableMeta = NULL;
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 9);
  ASSERT_EQ(tableMeta->tableType, TSDB_CHILD_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFree(tableMeta);

  SDbCacheInfo   *dbs = NULL;
  SSTableVersion *stb = NULL;
  uint32_t        dbNum = 0, stbNum = 0, allDbNum = 0, allStbNum = 0;
  int32_t         i = 0;
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
}

TEST(tableMeta, rmStbMeta) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  taosMemoryFree(tableMeta);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (0 == n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  code = catalogRemoveStbMeta(pCtg, "1.db1", ctgTestDbId, ctgTestSTablename, ctgTestSuid);
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
}

TEST(tableMeta, updateStbMeta) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);
  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  code = catalogUpdateTableMeta(pCtg, &rsp);
  ASSERT_EQ(code, 0);
  code = catalogAsyncUpdateTableMeta(pCtg, &rsp);
  ASSERT_EQ(code, 0);
  taosMemoryFreeClear(rsp.pSchemas);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
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

  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tableMeta->vgId, 0);
  ASSERT_EQ(tableMeta->tableType, TSDB_SUPER_TABLE);
  ASSERT_EQ(tableMeta->sversion, ctgTestSVersion + 1);
  ASSERT_EQ(tableMeta->tversion, ctgTestTVersion + 1);
  ASSERT_EQ(tableMeta->uid, ctgTestSuid);
  ASSERT_EQ(tableMeta->suid, ctgTestSuid);
  ASSERT_EQ(tableMeta->tableInfo.numOfColumns, ctgTestColNum);
  ASSERT_EQ(tableMeta->tableInfo.numOfTags, ctgTestTagNum);
  ASSERT_EQ(tableMeta->tableInfo.precision, 1 + 1);
  ASSERT_EQ(tableMeta->tableInfo.rowSize, 12);

  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
}

TEST(getIndexInfo, notExists) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_INDEXINFO_E;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SIndexInfo info;
  code = catalogGetIndexMeta(pCtg, mockPointer, "index1", &info);
  ASSERT_TRUE(code != 0);

  catalogDestroy();
}

TEST(refreshGetMeta, normal2normal) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
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
}

TEST(refreshGetMeta, normal2notexist) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
  ASSERT_EQ(code, CTG_ERR_CODE_TABLE_NOT_EXIST);
  ASSERT_TRUE(tableMeta == NULL);

  catalogDestroy();
}

TEST(refreshGetMeta, normal2child) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentCTableName = ctgTestTablename;
  ctgTestCurrentSTableName = ctgTestSTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
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
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(refreshGetMeta, stable2child) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentSTableName = ctgTestTablename;
  ctgTestCurrentCTableName = ctgTestTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  ctgTestCurrentSTableName = ctgTestSTablename;
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
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
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(refreshGetMeta, stable2stable) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentSTableName = ctgTestTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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
  taosMemoryFreeClear(tableMeta);

  while (0 == ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM)) {
    taosMsleep(50);
  }

  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
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
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(refreshGetMeta, child2stable) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  ctgTestCurrentCTableName = ctgTestTablename;
  ctgTestCurrentSTableName = ctgTestSTablename;

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  STableMeta *tableMeta = NULL;
  code = catalogGetTableMeta(pCtg, mockPointer, &n, &tableMeta);
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
  code = catalogRefreshGetTableMeta(pCtg, mockPointer, &n, &tableMeta, 0);
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
  taosMemoryFreeClear(tableMeta);

  catalogDestroy();
  ctgTestCurrentCTableName = NULL;
  ctgTestCurrentSTableName = NULL;
}

TEST(tableDistVgroup, normalTable) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo      *vgInfo = NULL;
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, &n, &vgList);
  ASSERT_TRUE(code != 0);

  catalogDestroy();
}

TEST(tableDistVgroup, childTableCase) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo      *vgInfo = NULL;
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestCTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, &n, &vgList);
  ASSERT_TRUE(code != 0);

  catalogDestroy();
}

TEST(tableDistVgroup, superTableCase) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo      *vgInfo = NULL;
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestSTablename);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, &n, &vgList);
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

  taosArrayDestroy(vgList);

  catalogDestroy();
}

TEST(dbVgroup, getSetDbVgroupCase) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SVgroupInfo      *pvgInfo = NULL;
  SDBVgInfo        *dbVgroup = NULL;
  SArray           *vgList = NULL;

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

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  code = catalogGetDBVgList(pCtg, mockPointer, ctgTestDbname, &vgList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize((const SArray *)vgList), ctgTestVgNum);

  taosArrayDestroy(vgList);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n > 0) {
      break;
    }
    taosMsleep(50);
  }

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 8);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 3);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, &n, &vgList);
  ASSERT_TRUE(code != 0);

  int32_t dbVer = 0;
  int64_t dbId = 0;
  int32_t tbNum = 0;
  int64_t stateTs = 0;
  code = catalogGetDBVgVersion(pCtg, ctgTestDbname, &dbVer, &dbId, &tbNum, &stateTs);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(dbVer, ctgTestVgVersion);
  ASSERT_EQ(dbId, ctgTestDbId);
  ASSERT_EQ(tbNum, ctgTestVgNum / 2);

  ctgTestBuildDBVgroup(&dbVgroup);
  code = catalogUpdateDBVgInfo(pCtg, ctgTestDbname, ctgTestDbId, dbVgroup);
  ASSERT_EQ(code, 0);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n != 3) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  code = catalogGetTableHashVgroup(pCtg, mockPointer, &n, &vgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(vgInfo.vgId, 7);
  ASSERT_EQ(vgInfo.epSet.numOfEps, 2);

  code = catalogGetTableDistVgInfo(pCtg, mockPointer, &n, &vgList);
  ASSERT_TRUE(code != 0);

  catalogDestroy();
}

TEST(multiThread, getSetRmSameDbVgroup) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SVgroupInfo      *pvgInfo = NULL;
  SDBVgInfo         dbVgroup = {0};
  SArray           *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
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
}

TEST(multiThread, getSetRmDiffDbVgroup) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SVgroupInfo      *pvgInfo = NULL;
  SDBVgInfo         dbVgroup = {0};
  SArray           *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroups();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
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
}

TEST(multiThread, ctableMeta) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SVgroupInfo      *pvgInfo = NULL;
  SDBVgInfo         dbVgroup = {0};
  SArray           *vgList = NULL;
  ctgTestStop = false;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndChildMeta();

  initQueryModuleMsgHandle();

  // sendCreateDbMsg(pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet);

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
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
}

TEST(rentTest, allRent) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;
  SVgroupInfo       vgInfo = {0};
  SVgroupInfo      *pvgInfo = NULL;
  SDBVgInfo         dbVgroup = {0};
  SArray           *vgList = NULL;
  ctgTestStop = false;
  SDbCacheInfo   *dbs = NULL;
  SSTableVersion *stable = NULL;
  uint32_t        num = 0;

  ctgTestInitLogFile();

  ctgTestSetRspDbVgroupsAndMultiSuperMeta();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");

  for (int32_t i = 1; i <= 10; ++i) {
    sprintf(n.tname, "%s_%d", ctgTestSTablename, i);

    STableMeta *tableMeta = NULL;
    code = catalogGetSTableMeta(pCtg, mockPointer, &n, &tableMeta);
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

    taosMemoryFree(tableMeta);

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
}

TEST(apiTest, catalogRefreshDBVgInfo_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  code = catalogRefreshDBVgInfo(pCtg, mockPointer, ctgTestDbname);
  ASSERT_EQ(code, 0);

  catalogDestroy();
}

TEST(apiTest, catalogChkAuth_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_USERAUTH;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SUserAuthInfo authInfo = {0};
  SUserAuthRes  authRes = {0};
  strcpy(authInfo.user, ctgTestUsername);
  toName(1, ctgTestDbname, ctgTestSTablename, &authInfo.tbName);
  authInfo.type = AUTH_TYPE_READ;
  bool exists = false;
  code = catalogChkAuthFromCache(pCtg, &authInfo, &authRes, &exists);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(exists, false);

  code = catalogChkAuth(pCtg, mockPointer, &authInfo, &authRes);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(authRes.pass[AUTH_RES_BASIC], true);

  while (true) {
    uint64_t n = 0;
    ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);
    if (n != 1) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  code = catalogChkAuthFromCache(pCtg, &authInfo, &authRes, &exists);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(authRes.pass[AUTH_RES_BASIC], true);
  ASSERT_EQ(exists, true);

  catalogDestroy();
}

TEST(apiTest, catalogRefreshGetTableCfg_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_VGINFO;
  ctgTestRspFunc[1] = CTGT_RSP_TBMETA;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  STableCfg *pCfg = NULL;

  code = catalogRefreshGetTableCfg(pCtg, mockPointer, &n, &pCfg);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(NULL != pCfg);
  ASSERT_EQ(pCfg->numOfColumns, ctgTestColNum);

  tFreeSTableCfgRsp((STableCfgRsp *)pCfg);
  taosMemoryFree(pCfg);

  catalogDestroy();
}

TEST(apiTest, catalogGetTableIndex_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_TBLINDEX;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);
  SArray *pRes = NULL;

  code = catalogGetTableIndex(pCtg, mockPointer, &n, &pRes);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(NULL != pRes);
  ASSERT_EQ(taosArrayGetSize(pRes), ctgTestIndexNum);

  taosArrayDestroyEx(pRes, tFreeSTableIndexInfo);

  catalogDestroy();
}

TEST(apiTest, catalogGetDBCfg_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_DBCFG;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SName n = {TSDB_TABLE_NAME_T, 1, {0}, {0}};
  strcpy(n.dbname, "db1");
  strcpy(n.tname, ctgTestTablename);

  SDbCfgInfo cfgInfo = {0};
  code = catalogGetDBCfg(pCtg, mockPointer, ctgTestDbname, &cfgInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(cfgInfo.numOfVgroups, ctgTestVgNum);

  catalogDestroy();
}

TEST(apiTest, catalogGetQnodeList_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_QNODELIST;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SArray *qnodeList = taosArrayInit(10, sizeof(SQueryNodeLoad));
  code = catalogGetQnodeList(pCtg, mockPointer, qnodeList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize(qnodeList), ctgTestQnodeNum);

  for (int32_t i = 0; i < ctgTestQnodeNum; ++i) {
    SQueryNodeLoad *pLoad = (SQueryNodeLoad *)taosArrayGet(qnodeList, i);
    ASSERT_EQ(pLoad->addr.nodeId, i);
  }

  catalogDestroy();
}

TEST(apiTest, catalogGetUdfInfo_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_UDF;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SFuncInfo funcInfo = {0};
  code = catalogGetUdfInfo(pCtg, mockPointer, "func1", &funcInfo);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(funcInfo.funcType, ctgTestFuncType);

  catalogDestroy();
}

TEST(apiTest, catalogGetServerVersion_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_SVRVER;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  char *ver = NULL;
  code = catalogGetServerVersion(pCtg, mockPointer, &ver);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(0 == strcmp(ver, "1.0"));

  catalogDestroy();
}

TEST(apiTest, catalogUpdateTableIndex_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_SVRVER;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  STableIndexRsp rsp = {0};
  strcpy(rsp.dbFName, ctgTestDbname);
  strcpy(rsp.tbName, ctgTestSTablename);
  rsp.suid = ctgTestSuid;
  rsp.version = 1;
  code = catalogUpdateTableIndex(pCtg, &rsp);
  ASSERT_EQ(code, 0);

  catalogDestroy();
}

TEST(apiTest, catalogGetDnodeList_test) {
  struct SCatalog  *pCtg = NULL;
  SRequestConnInfo  connInfo = {0};
  SRequestConnInfo *mockPointer = (SRequestConnInfo *)&connInfo;

  ctgTestInitLogFile();

  memset(ctgTestRspFunc, 0, sizeof(ctgTestRspFunc));
  ctgTestRspIdx = 0;
  ctgTestRspFunc[0] = CTGT_RSP_DNODElIST;

  ctgTestSetRspByIdx();

  initQueryModuleMsgHandle();

  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(ctgTestClusterId, &pCtg);
  ASSERT_EQ(code, 0);

  SArray *pList = NULL;
  code = catalogGetDnodeList(pCtg, mockPointer, &pList);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize(pList), 1);

  taosArrayDestroy(pList);

  catalogDestroy();
}

#ifdef INTEGRATION_TEST
TEST(intTest, autoCreateTableTest) {
  struct SCatalog *pCtg = NULL;

  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(NULL != taos);

  ctgdEnableDebug("api", true);
  ctgdEnableDebug("meta", true);
  ctgdEnableDebug("cache", true);
  ctgdEnableDebug("lock", true);

  ctgTestExecQuery(taos, "drop database if exists db1", false, NULL);
  ctgTestExecQuery(taos, "create database db1", false, NULL);
  ctgTestExecQuery(taos, "create stable db1.st1 (ts timestamp, f1 int) tags(tg1 int)", false, NULL);
  ctgTestExecQuery(taos, "insert into db1.tb1 using db1.st1 tags(1) values(now, 1)", false, NULL);

  ctgdGetOneHandle(&pCtg);

  while (true) {
    uint32_t n = ctgdGetClusterCacheNum(pCtg, CTG_DBG_META_NUM);
    if (2 != n) {
      taosMsleep(50);
    } else {
      break;
    }
  }

  uint64_t n = 0, m = 0;
  ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&n);

  ctgTestExecQuery(taos, "insert into db1.tb1 using db1.st1 tags(1) values(now, 2)", false, NULL);

  ctgTestExecQuery(taos, "insert into db1.tb1 values(now, 3)", false, NULL);

  taosMsleep(1000);
  ctgdGetStatNum("runtime.numOfOpDequeue", (void *)&m);

  ASSERT_EQ(n, m);

  ctgdEnableDebug("stopUpdate", true);
  ctgTestExecQuery(taos, "alter table db1.st1 add column f2 double", false, NULL);

  ctgdEnableDebug("stopUpdate", false);

  ctgTestExecQuery(taos, "insert into db1.tb1 (ts, f1) values(now, 4)", false, NULL);

  taos_close(taos);
}

#endif

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
