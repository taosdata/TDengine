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
#include <arpa/inet.h>
#include <assert.h>
#include <unistd.h>

#include "dnodeSystem.h"
#include "dnodeMgmt.h"
#include "taosmsg.h"
#include "vnode.h"
#include "vnodeSystem.h"
#include "vnodeUtil.h"
#include "vnodeStatus.h"
#include "dnodeModule.h"

static void *tsDnodeMgmtServer = NULL;
static void *tsDnodeMgmtClient = NULL;
static SRpcIpSet tsMgmtIpList = {0};
static SRpcIpSet tsMgmtPublicIpList = {0};
static uint64_t tsCreatedTime = 0;
static char mgmtIpStr[TSDB_MAX_MGMT_IPS][20] = {0};

static bool dnodeSeekMgmtIp(FILE *fp);
static void dnodeSaveMgmtIp();
static void dnodeSendStatusMsgToMgmt(void *handle, void *tmrId);
static int32_t dnodeRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);

extern void grantSendMsgToMgmt();

void dnodeInitMgmtImp() {
  SRpcInit  rpcInit;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = tsMgmtShellPort;
  rpcInit.label        = "DND-mgmt-s";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessMsgFromMgmt;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.afp          = dnodeRetriveUserAuthInfo;

  tsDnodeMgmtServer = rpcOpen(&rpcInit);
  if (tsDnodeMgmtServer == NULL) {
    dError("failed to init connection to mgmt");
    return -1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = 0;
  rpcInit.label        = "DND-mgmt-c";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessMsgFromMgmt;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsDnodeMgmtClient = rpcOpen(&rpcInit);
  if (tsDnodeMgmtClient == NULL) {
    tscError("failed to init connection from mgmt");
    return -1;
  }

  taosTmrReset(dnodeSendStatusMsgToMgmt, 500, pObj, vnodeTmrCtrl, &tsStatusTimer);
}

void dnodeCleanUpMgmt() {
  if (tsDnodeMgmtServer) {
    rpcClose(tsDnodeMgmtServer);
    tsDnodeMgmtServer = NULL;
  }

  if (tsDnodeMgmtClient) {
    rpcClose(tsDnodeMgmtClient);
    tsDnodeMgmtClient = NULL;
  }
}

void dnodeSendMsgToMnodeImp(int8_t msgType, void *pCont, int32_t contLen) {
  rpcSendRequest(tsDnodeMgmtClient, &tsMgmtIpList, msgType, pCont, contLen;
}

void dnodeSendStatusMsgToMgmt(void *handle, void *tmrId) {
  taosTmrReset(dnodeSendStatusMsgToMgmt, tsStatusInterval * 1000, pObj, vnodeTmrCtrl, &tsStatusTimer);
  if (tsStatusTimer == NULL) {
    dError("Failed to start status timer");
    return;
  }

  int32_t contLen = tsOpenVnodes * sizeof(SVnodeLoad) + 160;
  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    dError("Failed to malloc status message");
    return;
  }

  pStatus->version = htonl(tsVersion);
  pStatus->publicIp = htonl(inet_addr(tsPublicIp));
  pStatus->lastReboot = htonl(tsRebootTime);
  pStatus->numOfCores = htons((uint16_t)tsNumOfCores);
  pStatus->alternativeRole = (uint8_t)tsAlternativeRole;
  pStatus->numOfTotalVnodes = htons((uint16_t)tsNumOfTotalVnodes);
  pStatus->diskAvailable = tsAvailDataDirGB;
  pStatus->openVnodes = htonl(tsOpenVnodes);

  SVnodeLoad *pLoad = (SVnodeLoad *)((char*)pStatus + sizeof(SStatusMsg));

  for (int32_t vnode = 0, count = 0; vnode <= tsMaxVnode; ++vnode) {
    if (vnodeList[vnode].cfg.maxSessions <= 0) continue;

    SVnodeObj *pVnode = vnodeList + vnode;
    pLoad->vnode = htonl(vnode);
    pLoad->vgId = htonl(pVnode->cfg.vgId);
    pLoad->status = (uint8_t)vnodeList[vnode].vnodeStatus;
    pLoad->syncStatus =(uint8_t)vnodeList[vnode].syncStatus;
    pLoad->accessState = (uint8_t)(pVnode->accessState);
    pLoad->totalStorage = htobe64(pVnode->vnodeStatistic.totalStorage);
    pLoad->compStorage = htobe64(pVnode->vnodeStatistic.compStorage);
    if (pVnode->vnodeStatus == TSDB_VN_STATUS_MASTER) {
      pLoad->pointsWritten = htobe64(pVnode->vnodeStatistic.pointsWritten);
    } else {
      pLoad->pointsWritten = htobe64(0);
    }
    pLoad++;

    if (++count >= tsOpenVnodes) {
      break;
    }
  }

  dnodeSentMsgToMgmt(TSDB_MSG_TYPE_STATUS, pStatus, pStatus, contLen);

  grantSendMsgToMgmt();
}

void dnodeProcessStatusRspImp(int8_t msgType, int8_t *pCont, int32_t contLen, void *pConn, int32_t code) {
  if (code != TSDB_CODE_REDIRECT && code != TSDB_CODE_SUCCESS) {
    dTrace("status is rejected by mgmt node, code:%d", code);
    return 0;
  }

  if (pCont == NULL || contLen == 0) {
    dTrace("status is invalid, cont is null");
    return;
  }

  SStatusRsp *pStatus = pCont;
  pStatus->ipList.port = htons(pStatus->ipList.port);
  if (pStatus->ipList.numOfIps <= 0) {
    dError("num of mgmt ips is:%d", mgmtIpList.numOfIps);
    return ;
  }

  for (int32_t i = 0; i < pStatus->ipList.numOfIps; ++i) {
    pStatus->ipList[i] = htonl(pStatus->ipList[i]);
  }

  if (memcmp(pStatus->ipList, tsMgmtIpList, sizeof(SRpcIpSet)) != 0) {
    dPrint("mgmt ip list is changed, numOfIps:%d inUse:%d", pStatus->ipList.numOfIps, pStatus->ipList.inUse);
    memcpy(pStatus->ipList, tsMgmtIpList, sizeof(SRpcIpSet));
    for (int32_t i = 0; i < pIpList->numOfIps; ++i) {
      dPrint("mgmt IP index:%d ip:%d:%s", i, pStatus->ipList.ip[i], taosIpStr(pStatus->ipList.ip[i]));
    }

    dnodeSaveMgmtIp();
  }

 SDnodeState *pState = (SDnodeState *)pMsg;

    uint32_t mgmtCreatedTime = htonl(pState->createdTime);
    if (mgmtCreatedTime > tsCreatedTime) {
      // tsCreatedTime is save at taos.cfg
      // and may be changed by user sometimes
      // so we delete this logic
      // if ( tsCreatedTime )
      //  dnodeResetSystem();
      tsCreatedTime = mgmtCreatedTime;
      dnodeSaveMgmtIp();
    }

    uint32_t status = htonl(pState->moduleStatus);
    if (status != tsModuleStatus) {
      dPrint("module status is received, old:%d, new:%d", tsModuleStatus, status);
      dnodeProcessModuleStatus(status);
    }



  SVnodeAccess *pAccess = NULL;
  while (pMsg < msg + msgLen) {
    pAccess = (SVnodeAccess *)pMsg;
    pAccess->vnode = htonl(pAccess->vnode);
    vnodeList[pAccess->vnode].accessState = pAccess->accessState;
    pMsg += sizeof(SVnodeAccess);
  }
  /* } */

  return 0;
}

static bool dnodeSeekMgmtIp(FILE *fp) {
  char * line, *option;
  size_t len;
  int32_t    olen;
  size_t seek_pos = -1;

  line = NULL;
  while (!feof(fp)) {
    size_t pos = ftell(fp);
    getline(&line, &len, fp);
    if (line == NULL) break;

    paGetToken(line, &option, &olen);
    if (olen == 0) {
      tfree(line);
      continue;
    }
    option[olen] = 0;

    if (strcmp(option, "mgmtIpCreateTime") == 0) {
      seek_pos = pos;
      tfree(line);
      continue;
    }

    tfree(line);
  }

  if (seek_pos == -1) {
    fseek(fp, 0, SEEK_END);
    return false;
  } else {
    fseek(fp, seek_pos, SEEK_SET);
    return true;
  }
}

void dnodeInitMgmtIpImp() {
  FILE *       fp;
  char         fn[128];
  SMgmtIpList *pIpList = &mgmtIpList;

  sprintf(fn, "%s/taos.cfg", configDir);
  fp = fopen(fn, "r");
  memset(pIpList, 0, sizeof(mgmtIpList));
  tsCreatedTime = 0;

  if (fp) {
    char * line, *option;
    int32_t    olen;
    size_t len;

    dnodeSeekMgmtIp(fp);

    line = NULL;
    while (!feof(fp)) {
      tfree(line);
      getline(&line, &len, fp);
      if (line == NULL) break;

      paGetToken(line, &option, &olen);
      if (olen == 0) continue;
      option[olen] = 0;

      char *rest = option + olen + 1;
      if (strcmp(option, "mgmtIpCreateTime") == 0) {
        sscanf(rest, "%" PRIu64, &tsCreatedTime);
      } else if (strcmp(option, "mgmtNumOfIps") == 0) {
        int32_t numOfIps = -1;
        sscanf(rest, "%d", &numOfIps);
        if (numOfIps >= 0 && numOfIps < TSDB_MAX_MGMT_IPS) {
          pIpList->numOfIps = numOfIps;
        } else {
          dError("num:%d of mgmtIps invalid", numOfIps);
        }
      } else if (strcmp(option, "mgmtIp") == 0) {
        int32_t  index = -1;
        char ipStr[20] = {0};
        sscanf(rest, "%d %s", &index, ipStr);
        uint32_t ip = inet_addr(ipStr);
        if (index >= 0 && index < TSDB_MAX_MGMT_IPS && ip != INADDR_NONE) {
          pIpList->ip[index] = ip;
        } else {
          dError("index:%d of mgmtIpList:%d:%s invalid", index, ip, ipStr);
        }
      }
    }
    tfree(line);
    fclose(fp);
  }

  bool ipListValid = true;
  if (pIpList->numOfIps == 0) {
    ipListValid = false;
  } else {
    for (int32_t i = 0; i < pIpList->numOfIps; ++i) {
      if (pIpList->ip[i] == 0) {
        ipListValid = false;
        break;
      }
    }
  }

  if (!ipListValid) {
    dPrint("read mgmt ipList from %s failed", fn);
    memset(pIpList, 0, sizeof(mgmtIpList));
    pIpList->numOfIps = 1;
    pIpList->ip[0] = inet_addr(tsMasterIp);
    if (tsSecondIp[0]) {
      pIpList->numOfIps = 3;
      pIpList->ip[1] = inet_addr(tsMasterIp);
      pIpList->ip[2] = inet_addr(tsSecondIp);
    }
  }

  for (int32_t i = 0; i < pIpList->numOfIps; ++i) {
    tinet_ntoa(mgmtIpStr[i], pIpList->ip[i]);
  }

  dPrint("%d mgmt IPs are configured:", pIpList->numOfIps);
  for (int32_t i = 0; i < pIpList->numOfIps; ++i) {
    dPrint("index:%d ip:%s", i, mgmtIpStr[i]);
  }

  if (pIpList->numOfIps >= 3) {
    strcpy(tsSecondIp, mgmtIpStr[2]);
  }

  if (pIpList->numOfIps >= 2) {
    strcpy(tsMasterIp, mgmtIpStr[1]);
  }
}

static void dnodeSaveMgmtIp() {
  FILE *fp;
  char  fn[128];

  sprintf(fn, "%s/taos.cfg", configDir);
  fp = fopen(fn, "r+");
  if (fp) {
    if (!dnodeSeekMgmtIp(fp)) {
      fprintf(fp, "\n##############################################################\n");
      fprintf(fp, "# The following parameters are the cache of management ip list\n");
    }

    fprintf(fp, "mgmtIpCreateTime %" PRIu64 "\n", tsCreatedTime);
    fprintf(fp, "mgmtNumOfIps     %d\n", mgmtIpList.numOfIps);
    for (int32_t i = 0; i < mgmtIpList.numOfIps; ++i) {
      char ipStr[20] = {0};
      tinet_ntoa(ipStr, mgmtIpList.ip[i]);
      fprintf(fp, "mgmtIp %d         %s\n", i, ipStr);
    }

    fclose(fp);
  } else {
    dError("failed to write file:%s", fn);
  }
}

static int32_t dnodeRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}
