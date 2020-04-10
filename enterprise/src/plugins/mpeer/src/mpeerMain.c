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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tglobalcfg.h"
#include "tmodule.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "mnode.h"
#include "tbalance.h"
#include "tcluster.h"
#include "tgrant.h"
#include "vnode.h"
#include "mpeer.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "dnodeMClient.h"

static void   *tsMnodeSdb = NULL;
static int32_t tsMnodeUpdateSize = 0;
static int32_t mpeerCreateMnode(uint32_t ip);
static int32_t mpeerDropMnode(uint32_t ip);

static int32_t mpeerActionDestroy(SSdbOperDesc *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionInsert(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  SDnodeObj *pDnode = clusterGetDnode(pMnode->dnodeId);
  if (pDnode != NULL) {
    pMnode->privateIp = pDnode->privateIp;
    pDnode->publicIp = pDnode->publicIp;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionDelete(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  mTrace("mnode:%d, is dropped from sdb", pMnode->dnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionEncode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;

  if (pOper->maxRowSize < tsMnodeUpdateSize) {
    return -1;
  } else {
    memcpy(pOper->rowData, pMnode, tsMnodeUpdateSize);
    pOper->rowSize = tsMnodeUpdateSize;
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mpeerActionDecode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  if (pMnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pMnode, pOper->rowData, tsMnodeUpdateSize);
  pOper->pObj = pMnode;
  return TSDB_CODE_SUCCESS;
}

int32_t mpeerInit() {
  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "mnodes",
    .hashSessions = TSDB_MAX_MNODES,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_TYPE_AUTO,
    .insertFp     = mpeerActionInsert,
    .deleteFp     = mpeerActionDelete,
    .updateFp     = mpeerActionUpdate,
    .encodeFp     = mpeerActionEncode,
    .decodeFp     = mpeerActionDecode,
    .destroyFp    = mpeerActionDestroy,
  };

  tsMnodeSdb = sdbOpenTable(&tableDesc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  int32_t numOfRows = sdbGetNumOfRows(tsMnodeSdb);
  if (numOfRows > 1) {
    //TODO: init sync
  }

  mTrace("mnodes is initialized");
  return 0;
}

void mpeerCleanup() {
  sdbCloseTable(tsMnodeSdb);
}

bool mpeerInServerStatus() {}

bool mpeerIsMaster() {}

bool mgmtCheckRedirect(void *handle) {}

void mpeerGetPrivateIpList(SRpcIpSet *ipSet) {}

void mpeerGetPublicIpList(SRpcIpSet *ipSet) {}

// static void mpeerWorkAsMaster() {
//   sdbLPrint("dnode:%s start to work as master", tsPrivateIp);

//   pSelf->role = SDB_ROLE_MASTER;
//   pSelf->status = SDB_STATUS_SERVING;
//   sdbMaster = 1;
//   tsMpeerMasterStartTime = taosGetTimestampSec();

//   mpeerUpdateIpList();
//   (*sdbWorkAsMasterCallback)();
// }

// void sdbStopWorkingAsMaster() {
//   sdbLPrint("dnode:%s stop working as Master", tsPrivateIp);

//   pSelf->role = SDB_ROLE_UNDECIDED;
//   taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
//   sdbMaster = 0;

//   mpeerUpdateIpList();
// }
