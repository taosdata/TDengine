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
#include "tgrant.h"
#include "tbalance.h"
#include "tglobal.h"
#include "tconfig.h"
#include "tutil.h"
#include "tsocket.h"
#include "tbalance.h"
#include "tsync.h"
#include "tdataformat.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"
#include "mnodePeer.h"
#include "mnodeCluster.h"

int32_t tsAccessSquence = 0;
static void   *tsDnodeSdb = NULL;
static int32_t tsDnodeUpdateSize = 0;
extern void *  tsMnodeSdb;
extern void *  tsVgroupSdb;

static int32_t mnodeCreateDnode(char *ep, SMnodeMsg *pMsg);
static int32_t mnodeProcessCreateDnodeMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropDnodeMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessCfgDnodeMsg(SMnodeMsg *pMsg);
static void    mnodeProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) ;
static int32_t mnodeProcessDnodeStatusMsg(SMnodeMsg *rpcMsg);
static int32_t mnodeGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static char*   mnodeGetDnodeAlternativeRoleStr(int32_t alternativeRole);

static int32_t mnodeDnodeActionDestroy(SSdbOper *pOper) {
  taosTFree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionInsert(SSdbOper *pOper) {
  SDnodeObj *pDnode = pOper->pObj;
  if (pDnode->status != TAOS_DN_STATUS_DROPPING) {
    pDnode->status = TAOS_DN_STATUS_OFFLINE;
    pDnode->lastAccess = tsAccessSquence;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionDelete(SSdbOper *pOper) {
  SDnodeObj *pDnode = pOper->pObj;
 
  mnodeDropMnodeLocal(pDnode->dnodeId);
  balanceAsyncNotify();

  mDebug("dnode:%d, all vgroups is dropped from sdb", pDnode->dnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionUpdate(SSdbOper *pOper) {
  SDnodeObj *pNew = pOper->pObj;
  SDnodeObj *pDnode = mnodeGetDnode(pNew->dnodeId);
  if (pDnode != NULL && pNew != pDnode) {
    memcpy(pDnode, pNew, pOper->rowSize);
    free(pNew);
  }
  mnodeDecDnodeRef(pDnode);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionEncode(SSdbOper *pOper) {
  SDnodeObj *pDnode = pOper->pObj;
  memcpy(pOper->rowData, pDnode, tsDnodeUpdateSize);
  pOper->rowSize = tsDnodeUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionDecode(SSdbOper *pOper) {
  SDnodeObj *pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  if (pDnode == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pDnode, pOper->rowData, tsDnodeUpdateSize);
  pOper->pObj = pDnode;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDnodeActionRestored() {
  int32_t numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
    mInfo("dnode first deploy, create dnode:%s", tsLocalEp);
    mnodeCreateDnode(tsLocalEp, NULL);
    SDnodeObj *pDnode = mnodeGetDnodeByEp(tsLocalEp);
    if (pDnode != NULL) {
      mnodeAddMnode(pDnode->dnodeId);
      mnodeDecDnodeRef(pDnode);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitDnodes() {
  SDnodeObj tObj;
  tsDnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_DNODE,
    .tableName    = "dnodes",
    .hashSessions = TSDB_DEFAULT_DNODES_HASH_SIZE,
    .maxRowSize   = tsDnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_AUTO,
    .insertFp     = mnodeDnodeActionInsert,
    .deleteFp     = mnodeDnodeActionDelete,
    .updateFp     = mnodeDnodeActionUpdate,
    .encodeFp     = mnodeDnodeActionEncode,
    .decodeFp     = mnodeDnodeActionDecode,
    .destroyFp    = mnodeDnodeActionDestroy,
    .restoredFp   = mnodeDnodeActionRestored
  };

  tsDnodeSdb = sdbOpenTable(&tableDesc);
  if (tsDnodeSdb == NULL) {
    mError("failed to init dnodes data");
    return -1;
  }

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_DNODE, mnodeProcessCreateDnodeMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_DNODE, mnodeProcessDropDnodeMsg); 
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CONFIG_DNODE, mnodeProcessCfgDnodeMsg);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP, mnodeProcessCfgDnodeMsgRsp);
  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_STATUS, mnodeProcessDnodeStatusMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_MODULE, mnodeGetModuleMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_MODULE, mnodeRetrieveModules);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_CONFIGS, mnodeGetConfigMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_CONFIGS, mnodeRetrieveConfigs);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_VNODES, mnodeGetVnodeMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_VNODES, mnodeRetrieveVnodes);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_DNODE, mnodeGetDnodeMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_DNODE, mnodeRetrieveDnodes);
 
  mDebug("table:dnodes table is created");
  return 0;
}

void mnodeCleanupDnodes() {
  sdbCloseTable(tsDnodeSdb);
  tsDnodeSdb = NULL;
}

void *mnodeGetNextDnode(void *pIter, SDnodeObj **pDnode) { 
  return sdbFetchRow(tsDnodeSdb, pIter, (void **)pDnode); 
}

int32_t mnodeGetDnodesNum() {
  return sdbGetNumOfRows(tsDnodeSdb);
}

int32_t mnodeGetOnlinDnodesCpuCoreNum() {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;
  int32_t    cpuCores = 0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status != TAOS_DN_STATUS_OFFLINE) {
      cpuCores += pDnode->numOfCores;
    }
    mnodeDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);

  if (cpuCores < 2) cpuCores = 2;
  return cpuCores;
}

int32_t mnodeGetOnlineDnodesNum() {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;
  int32_t    onlineDnodes = 0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status != TAOS_DN_STATUS_OFFLINE) onlineDnodes++;
    mnodeDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);

  return onlineDnodes;
}

void *mnodeGetDnode(int32_t dnodeId) {
  return sdbGetRow(tsDnodeSdb, &dnodeId);
}

void *mnodeGetDnodeByEp(char *ep) {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (strcmp(ep, pDnode->dnodeEp) == 0) {
      sdbFreeIter(pIter);
      return pDnode;
    }
    mnodeDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);

  return NULL;
}

void mnodeIncDnodeRef(SDnodeObj *pDnode) {
  sdbIncRef(tsDnodeSdb, pDnode);
}

void mnodeDecDnodeRef(SDnodeObj *pDnode) {
  sdbDecRef(tsDnodeSdb, pDnode);
}

void mnodeUpdateDnode(SDnodeObj *pDnode) {
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnodeId:%d, failed update", pDnode->dnodeId);
  }
}

static int32_t mnodeProcessCfgDnodeMsg(SMnodeMsg *pMsg) {
  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    mError("failed to cfg dnode, no rights");
    return TSDB_CODE_MND_NO_RIGHTS;
  }
  
  SCMCfgDnodeMsg *pCmCfgDnode = pMsg->rpcMsg.pCont;
  if (pCmCfgDnode->ep[0] == 0) {
    tstrncpy(pCmCfgDnode->ep, tsLocalEp, TSDB_EP_LEN);
  }

  SDnodeObj *pDnode = mnodeGetDnodeByEp(pCmCfgDnode->ep);
  if (pDnode == NULL) {
    int32_t dnodeId = strtol(pCmCfgDnode->ep, NULL, 10);
    if (dnodeId <= 0 || dnodeId > 65536) {
      mError("failed to cfg dnode, invalid dnodeEp:%s", pCmCfgDnode->ep);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }

    pDnode = mnodeGetDnode(dnodeId);
    if (pDnode == NULL) {
      mError("failed to cfg dnode, invalid dnodeId:%d", dnodeId);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);

  if (strncasecmp(pCmCfgDnode->config, "balance", 7) == 0) {
    int32_t vnodeId = 0;
    int32_t dnodeId = 0;
    bool parseOk = taosCheckBalanceCfgOptions(pCmCfgDnode->config + 8, &vnodeId, &dnodeId);
    if (!parseOk) {
      mnodeDecDnodeRef(pDnode);
      return TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION;
    }

    int32_t code = balanceAlterDnode(pDnode, vnodeId, dnodeId);
    mnodeDecDnodeRef(pDnode);
    return code;
  } else {
    SMDCfgDnodeMsg *pMdCfgDnode = rpcMallocCont(sizeof(SMDCfgDnodeMsg));
    strcpy(pMdCfgDnode->ep, pCmCfgDnode->ep);
    strcpy(pMdCfgDnode->config, pCmCfgDnode->config);

    SRpcMsg rpcMdCfgDnodeMsg = {
      .ahandle = 0,
      .code = 0,
      .msgType = TSDB_MSG_TYPE_MD_CONFIG_DNODE,
      .pCont = pMdCfgDnode,
      .contLen = sizeof(SMDCfgDnodeMsg)
    };

    mInfo("dnode:%s, is configured by %s", pCmCfgDnode->ep, pMsg->pUser->user);
    dnodeSendMsgToDnode(&epSet, &rpcMdCfgDnodeMsg);
    mnodeDecDnodeRef(pDnode);
    return TSDB_CODE_SUCCESS;
  }
}

static void mnodeProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) {
  mInfo("cfg dnode rsp is received");
}

static bool mnodeCheckClusterCfgPara(const SClusterCfg *clusterCfg) {
  if (clusterCfg->numOfMnodes != htonl(tsNumOfMnodes)) {
    mError("\"numOfMnodes\"[%d - %d] cfg parameters inconsistent", clusterCfg->numOfMnodes, htonl(tsNumOfMnodes));
    return false;
  }  
  if (clusterCfg->enableBalance      != htonl(tsEnableBalance)) {
    mError("\"balance\"[%d - %d] cfg parameters inconsistent", clusterCfg->enableBalance, htonl(tsEnableBalance));
    return false;
  }
  if (clusterCfg->mnodeEqualVnodeNum != htonl(tsMnodeEqualVnodeNum)) {
    mError("\"mnodeEqualVnodeNum\"[%d - %d] cfg parameters inconsistent", clusterCfg->mnodeEqualVnodeNum, htonl(tsMnodeEqualVnodeNum));
    return false;
  }
  if (clusterCfg->offlineThreshold   != htonl(tsOfflineThreshold)) {
    mError("\"offlineThreshold\"[%d - %d] cfg parameters inconsistent", clusterCfg->offlineThreshold, htonl(tsOfflineThreshold));
    return false;
  }
  if (clusterCfg->statusInterval     != htonl(tsStatusInterval)) {
    mError("\"statusInterval\"[%d - %d] cfg parameters inconsistent", clusterCfg->statusInterval, htonl(tsStatusInterval));
    return false;
  }
  if (clusterCfg->maxtablesPerVnode  != htonl(tsMaxTablePerVnode)) {
    mError("\"maxTablesPerVnode\"[%d - %d] cfg parameters inconsistent", clusterCfg->maxtablesPerVnode, htonl(tsMaxTablePerVnode));
    return false;
  }
  if (clusterCfg->maxVgroupsPerDb    != htonl(tsMaxVgroupsPerDb)) {
    mError("\"maxVgroupsPerDb\"[%d - %d]  cfg parameters inconsistent", clusterCfg->maxVgroupsPerDb, htonl(tsMaxVgroupsPerDb));
    return false;
  }
  if (0 != strncasecmp(clusterCfg->arbitrator, tsArbitrator, strlen(tsArbitrator))) {
    mError("\"arbitrator\"[%s - %s]  cfg parameters inconsistent", clusterCfg->arbitrator, tsArbitrator);
    return false;
  }
  if (0 != strncasecmp(clusterCfg->timezone, tsTimezone, strlen(tsTimezone))) {
    mError("\"timezone\"[%s - %s]  cfg parameters inconsistent", clusterCfg->timezone, tsTimezone);
    return false;
  }
  if (0 != strncasecmp(clusterCfg->locale, tsLocale, strlen(tsLocale))) {
    mError("\"locale\"[%s - %s]  cfg parameters inconsistent", clusterCfg->locale, tsLocale);
    return false;
  }
  if (0 != strncasecmp(clusterCfg->charset, tsCharset, strlen(tsCharset))) {
    mError("\"charset\"[%s - %s] cfg parameters inconsistent.", clusterCfg->charset, tsCharset);
    return false;
  }
    
  return true;
}

static int32_t mnodeProcessDnodeStatusMsg(SMnodeMsg *pMsg) {
  SDMStatusMsg *pStatus = pMsg->rpcMsg.pCont;
  pStatus->dnodeId      = htonl(pStatus->dnodeId);
  pStatus->moduleStatus = htonl(pStatus->moduleStatus);
  pStatus->lastReboot   = htonl(pStatus->lastReboot);
  pStatus->numOfCores   = htons(pStatus->numOfCores);
  
  uint32_t version = htonl(pStatus->version);
  if (version != tsVersion) {
    mError("status msg version:%d not equal with mnode:%d", version, tsVersion);
    return TSDB_CODE_MND_INVALID_MSG_VERSION;
  }

  SDnodeObj *pDnode = NULL;
  if (pStatus->dnodeId == 0) {
    pDnode = mnodeGetDnodeByEp(pStatus->dnodeEp);
    if (pDnode == NULL) {
      mDebug("dnode %s not created", pStatus->dnodeEp);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  } else {
    pDnode = mnodeGetDnode(pStatus->dnodeId);
    if (pDnode == NULL) {
      mError("dnode id:%d, %s not exist", pStatus->dnodeId, pStatus->dnodeEp);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  pDnode->lastReboot       = pStatus->lastReboot;
  pDnode->numOfCores       = pStatus->numOfCores;
  pDnode->diskAvailable    = pStatus->diskAvailable;
  pDnode->alternativeRole  = pStatus->alternativeRole;
  pDnode->moduleStatus     = pStatus->moduleStatus;

  if (pStatus->dnodeId == 0) {
    mDebug("dnode:%d %s, first access, set clusterId %s", pDnode->dnodeId, pDnode->dnodeEp, mnodeGetClusterId());
  } else {
    if (strncmp(pStatus->clusterId, mnodeGetClusterId(), TSDB_CLUSTER_ID_LEN - 1) != 0) {
      mError("dnode:%d, input clusterId %s not match with exist %s", pDnode->dnodeId, pStatus->clusterId,
             mnodeGetClusterId());
      return TSDB_CODE_MND_INVALID_CLUSTER_ID;
    } else {
      mTrace("dnode:%d, status received, access times %d", pDnode->dnodeId, pDnode->lastAccess);
    }
  }

  int32_t openVnodes = htons(pStatus->openVnodes);
  int32_t contLen = sizeof(SDMStatusRsp) + openVnodes * sizeof(SDMVgroupAccess);
  SDMStatusRsp *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    mnodeDecDnodeRef(pDnode);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pRsp->dnodeCfg.dnodeId = htonl(pDnode->dnodeId);
  pRsp->dnodeCfg.moduleStatus = htonl((int32_t)pDnode->isMgmt);
  pRsp->dnodeCfg.numOfVnodes = htonl(openVnodes);
  tstrncpy(pRsp->dnodeCfg.clusterId, mnodeGetClusterId(), TSDB_CLUSTER_ID_LEN);
  SDMVgroupAccess *pAccess = (SDMVgroupAccess *)((char *)pRsp + sizeof(SDMStatusRsp));

  for (int32_t j = 0; j < openVnodes; ++j) {
    SVnodeLoad *pVload = &pStatus->load[j];
    pVload->vgId = htonl(pVload->vgId);
    pVload->cfgVersion = htonl(pVload->cfgVersion);

    SVgObj *pVgroup = mnodeGetVgroup(pVload->vgId);
    if (pVgroup == NULL) {
      SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);
      mInfo("dnode:%d, vgId:%d not exist in mnode, drop it", pDnode->dnodeId, pVload->vgId);
      mnodeSendDropVnodeMsg(pVload->vgId, &epSet, NULL);
    } else {
      mnodeUpdateVgroupStatus(pVgroup, pDnode, pVload);
      pAccess->vgId = htonl(pVload->vgId);
      pAccess->accessState = pVgroup->accessState;
      pAccess++;
      mnodeDecVgroupRef(pVgroup);
    }
  }

  if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
    // Verify whether the cluster parameters are consistent when status change from offline to ready
    bool ret = mnodeCheckClusterCfgPara(&(pStatus->clusterCfg));
    if (false == ret) {
      mnodeDecDnodeRef(pDnode);
      rpcFreeCont(pRsp);
      mError("dnode:%d, %s cluster cfg parameters inconsistent", pDnode->dnodeId, pStatus->dnodeEp);
      return TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT;
    }
    
    mDebug("dnode:%d, from offline to online", pDnode->dnodeId);
    pDnode->status = TAOS_DN_STATUS_READY;
    balanceSyncNotify();
    balanceAsyncNotify();
  }

  if (openVnodes != pDnode->openVnodes) {
    mnodeCheckUnCreatedVgroup(pDnode, pStatus->load, openVnodes);
  }

  pDnode->lastAccess = tsAccessSquence;

  //this func should be called after sdb replica changed
  mnodeGetMnodeInfos(&pRsp->mnodes);
  
  mnodeDecDnodeRef(pDnode);

  pMsg->rpcRsp.len = contLen;
  pMsg->rpcRsp.rsp =  pRsp;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeCreateDnode(char *ep, SMnodeMsg *pMsg) {
  int32_t grantCode = grantCheck(TSDB_GRANT_DNODE);
  if (grantCode != TSDB_CODE_SUCCESS) {
    return grantCode;
  }

  char dnodeEp[TSDB_EP_LEN] = {0};
  tstrncpy(dnodeEp, ep, TSDB_EP_LEN);
  strtrim(dnodeEp);

  char *temp = strchr(dnodeEp, ':');
  if (!temp) {
    int len = strlen(dnodeEp);
    if (dnodeEp[len - 1] == ';') dnodeEp[len - 1] = 0;
    len = strlen(dnodeEp);
    snprintf(dnodeEp + len, TSDB_EP_LEN - len, ":%d", tsServerPort);
  }
  ep = dnodeEp;

  SDnodeObj *pDnode = mnodeGetDnodeByEp(ep);
  if (pDnode != NULL) {
    mnodeDecDnodeRef(pDnode);
    mError("dnode:%d is already exist, %s:%d", pDnode->dnodeId, pDnode->dnodeFqdn, pDnode->dnodePort);
    return TSDB_CODE_MND_DNODE_ALREADY_EXIST;
  }

  pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->createdTime = taosGetTimestampMs();
  pDnode->status = TAOS_DN_STATUS_OFFLINE; 
  tstrncpy(pDnode->dnodeEp, ep, TSDB_EP_LEN);
  taosGetFqdnPortFromEp(ep, pDnode->dnodeFqdn, &pDnode->dnodePort);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode,
    .rowSize = sizeof(SDnodeObj),
    .pMsg = pMsg
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    int dnodeId = pDnode->dnodeId;
    taosTFree(pDnode);
    mError("failed to create dnode:%d, reason:%s", dnodeId, tstrerror(code));
  } else {
    mLInfo("dnode:%d is created", pDnode->dnodeId);
  }

  return code;
}

int32_t mnodeDropDnode(SDnodeObj *pDnode, void *pMsg) {
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode,
    .pMsg = pMsg
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop from cluster, result:%s", pDnode->dnodeId, tstrerror(code));
  } else {
    mLInfo("dnode:%d, is dropped from cluster", pDnode->dnodeId);
  }

  return code;
}

static int32_t mnodeDropDnodeByEp(char *ep, SMnodeMsg *pMsg) {
  SDnodeObj *pDnode = mnodeGetDnodeByEp(ep);
  if (pDnode == NULL) {
    int32_t dnodeId = (int32_t)strtol(ep, NULL, 10);
    pDnode = mnodeGetDnode(dnodeId);
    if (pDnode == NULL) {
      mError("dnode:%s, is not exist", ep);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  if (strcmp(pDnode->dnodeEp, mnodeGetMnodeMasterEp()) == 0) {
    mError("dnode:%d, can't drop dnode:%s which is master", pDnode->dnodeId, ep);
    mnodeDecDnodeRef(pDnode);
    return TSDB_CODE_MND_NO_REMOVE_MASTER;
  }

  mInfo("dnode:%d, start to drop it", pDnode->dnodeId);

  int32_t code = balanceDropDnode(pDnode);
  mnodeDecDnodeRef(pDnode);
  return code;
}

static int32_t mnodeProcessCreateDnodeMsg(SMnodeMsg *pMsg) {
  SCMCreateDnodeMsg *pCreate = pMsg->rpcMsg.pCont;

  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    return TSDB_CODE_MND_NO_RIGHTS;
  } else {
    return mnodeCreateDnode(pCreate->ep, pMsg);
  }
}

static int32_t mnodeProcessDropDnodeMsg(SMnodeMsg *pMsg) {
  SCMDropDnodeMsg *pDrop = pMsg->rpcMsg.pCont;

  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    return TSDB_CODE_MND_NO_RIGHTS;
  } else {
    return mnodeDropDnodeByEp(pDrop->ep, pMsg);
  }
}

static int32_t mnodeGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, TSDB_DEFAULT_USER) != 0) {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end_point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "cores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  mnodeDecUserRef(pUser);

  return 0;
}

static int32_t mnodeRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SDnodeObj *pDnode   = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDnode(pShow->pIter, &pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->dnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfCores;
    cols++;
    
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    char* status = mnodeGetDnodeStatusStr(pDnode->status);
    STR_TO_VARSTR(pWrite, status);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    char* role = mnodeGetDnodeAlternativeRoleStr(pDnode->alternativeRole);
    STR_TO_VARSTR(pWrite, role);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDnode->createdTime;
    cols++;

 
    numOfRows++;
    mnodeDecDnodeRef(pDnode);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool mnodeCheckModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  uint32_t status = pDnode->moduleStatus & (1 << moduleType);
  return status > 0;
}

static int32_t mnodeGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "module");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetDnodesNum() * TSDB_MOD_MAX;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;
  mnodeDecUserRef(pUser);

  return 0;
}

int32_t mnodeRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  char *  pWrite;

  while (numOfRows < rows) {
    SDnodeObj *pDnode = NULL;
    pShow->pIter = mnodeGetNextDnode(pShow->pIter, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      int32_t cols = 0;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDnode->dnodeId;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strncpy(pWrite, pDnode->dnodeEp, pShow->bytes[cols]-1);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      switch (moduleType) {
        case TSDB_MOD_MNODE:
          strcpy(pWrite, "mnode");
          break;
        case TSDB_MOD_HTTP:
          strcpy(pWrite, "http");
          break;
        case TSDB_MOD_MONITOR:
          strcpy(pWrite, "monitor");
          break;
        default:
          strcpy(pWrite, "unknown");
      }
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      bool enable = mnodeCheckModuleInDnode(pDnode, moduleType);
      strcpy(pWrite, enable ? "enable" : "disable");
      cols++;

      numOfRows++;
    }

    mnodeDecDnodeRef(pDnode);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool mnodeCheckConfigShow(SGlobalCfg *cfg) {
  if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW))
    return false;
  return true;
}

static int32_t mnodeGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_CFG_OPTION_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "config name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_CFG_VALUE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "config value");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 0;
  for (int32_t i = tsGlobalConfigNum - 1; i >= 0; --i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!mnodeCheckConfigShow(cfg)) continue;
    pShow->numOfRows++;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;
  mnodeDecUserRef(pUser);

  return 0;
}

static int32_t mnodeRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;

  for (int32_t i = tsGlobalConfigNum - 1; i >= 0 && numOfRows < rows; --i) {
    SGlobalCfg *cfg = tsGlobalConfig + i;
    if (!mnodeCheckConfigShow(cfg)) continue;

    char *pWrite;
    int32_t   cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    snprintf(pWrite, TSDB_CFG_OPTION_LEN, "%s", cfg->option);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    switch (cfg->valType) {
      case TAOS_CFG_VTYPE_INT16:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%d", *((int16_t *)cfg->ptr));
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_INT32:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%d", *((int32_t *)cfg->ptr));
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%f", *((float *)cfg->ptr));
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%s", (char *)cfg->ptr);
        numOfRows++;
        break;
      default:
        break;
    }
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static int32_t mnodeGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  SDnodeObj *pDnode = NULL;
  if (pShow->payloadLen > 0 ) {
    pDnode = mnodeGetDnodeByEp(pShow->payload);
  } else {
    void *pIter = mnodeGetNextDnode(NULL, (SDnodeObj **)&pDnode);
    sdbFreeIter(pIter);
  }

  if (pDnode != NULL) {
    pShow->numOfRows += pDnode->openVnodes;
    mnodeDecDnodeRef(pDnode);
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = pDnode;
  mnodeDecUserRef(pUser);

  return 0;
}

static int32_t mnodeRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;

  if (0 == rows) return 0;

  pDnode = (SDnodeObj *)(pShow->pIter);
  if (pDnode != NULL) {
    void *pIter = NULL;
    SVgObj *pVgroup;
    while (1) {
      pIter = mnodeGetNextVgroup(pIter, &pVgroup);
      if (pVgroup == NULL) break;

      for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
        SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
        if (pVgid->pDnode == pDnode) {
          cols = 0;

          pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
          *(uint32_t *)pWrite = pVgroup->vgId;
          cols++;

          pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
          strcpy(pWrite, mnodeGetMnodeRoleStr(pVgid->role));
          cols++;
        }
      }

      mnodeDecVgroupRef(pVgroup);
    }
    sdbFreeIter(pIter);
  } else {
    numOfRows = 0;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

char* mnodeGetDnodeStatusStr(int32_t dnodeStatus) {
  switch (dnodeStatus) {
    case TAOS_DN_STATUS_OFFLINE:   return "offline";
    case TAOS_DN_STATUS_DROPPING:  return "dropping";
    case TAOS_DN_STATUS_BALANCING: return "balancing";
    case TAOS_DN_STATUS_READY:     return "ready";
    default:                       return "undefined";
  }
}

static char* mnodeGetDnodeAlternativeRoleStr(int32_t alternativeRole) {
  switch (alternativeRole) {
    case TAOS_DN_ALTERNATIVE_ROLE_ANY: return "any";
    case TAOS_DN_ALTERNATIVE_ROLE_MNODE: return "mnode";
    case TAOS_DN_ALTERNATIVE_ROLE_VNODE: return "vnode";
    default:return "any";
  }
}

