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
#include "taosmsg.h"
#include "tlog.h"
#include "tsocket.h"
#include "tutil.h"
#include "dnodeSystem.h"
#include "dnodeMgmt.h"
#include "dnodeModule.h"

static void      *tsDnodeMgmtServer = NULL;
static void      *tsDnodeMgmtClient = NULL;
static SRpcIpSet tsDnodeMgmtIpList  = {0};
static uint64_t  tsCreatedTime      = 0;

static bool dnodeSeekMgmtIp(FILE *fp);
static void dnodeSaveMgmtIp();
static int32_t dnodeRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);

void (*mgmtUpdateModulesFp)(uint32_t status) = NULL;

uint32_t dnodeGetMgmtIp() {
  return tsDnodeMgmtIpList.ip[0];
}

int32_t dnodeInitMgmtImp() {
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

  return 0;
}

void dnodeCleanUpMgmtImp() {
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
  rpcSendRequest(tsDnodeMgmtClient, &tsDnodeMgmtIpList, msgType, pCont, contLen, NULL);
}

void dnodeSendRspToMnodeImp(void *handle, int32_t code, void *pCont, int contLen) {
  rpcSendResponse(handle, code, pCont, contLen);
}

void dnodeProcessStatusRspImp(void *pCont, int32_t contLen, int8_t msgType, void *pConn) {
  if (pCont == NULL || contLen == 0) {
    dTrace("status msg is invalid, cont is null");
    return;
  }

  SStatusRsp *pStatus = pCont;
  pStatus->code = htonl(pStatus->code);
  if (pStatus->code != TSDB_CODE_SUCCESS && pStatus->code != TSDB_CODE_REDIRECT) {
    dTrace("status msg is invalid, code:%d:%s", pStatus->code, tstrerror(pStatus->code));
    return;
  }

  pStatus->ipList.port = htons(pStatus->ipList.port);
  if (pStatus->ipList.numOfIps <= 0) {
    dError("num of mgmt ips is:%d", tsDnodeMgmtIpList.numOfIps);
    return;
  }

  dTrace("status msg is received, code:%d", pStatus->code);

  for (int32_t i = 0; i < pStatus->ipList.numOfIps; ++i) {
    pStatus->ipList.ip[i] = htonl(pStatus->ipList.ip[i]);
  }
  pStatus->ipList.port = htons(pStatus->ipList.port);

  if (memcmp(&(pStatus->ipList), &tsDnodeMgmtIpList, sizeof(SRpcIpSet)) != 0) {
    dPrint("mgmt ip list is changed, numOfIps:%d inUse:%d", pStatus->ipList.numOfIps, pStatus->ipList.inUse);
    tsDnodeMgmtIpList.numOfIps = pStatus->ipList.numOfIps;
    for (int32_t i = 0; i < pStatus->ipList.numOfIps; ++i) {
      tsDnodeMgmtIpList.ip[i] = pStatus->ipList.ip[i];
      dPrint("mgmt IP index:%d ip:%d:%s", i, tsDnodeMgmtIpList.ip[i], taosIpStr(tsDnodeMgmtIpList.ip[i]));
    }
    tsDnodeMgmtIpList.inUse = pStatus->ipList.inUse;
    tsDnodeMgmtIpList.port = pStatus->ipList.port;
    dnodeSaveMgmtIp();
  }

  SDnodeState *pState = &pStatus->dnodeState;
  pState->numOfVnodes = htonl(pState->numOfVnodes);
  pState->moduleStatus = htonl(pState->moduleStatus);
  pState->createdTime = htonl(pState->createdTime);

  if (pState->createdTime > tsCreatedTime) {
    // tsCreatedTime is save at taos.cfg and may be changed by user sometimes
    // so we delete this logic
    tsCreatedTime = pStatus->dnodeState.createdTime;
    dnodeSaveMgmtIp();
  }

  if (mgmtUpdateModulesFp) {
    (*mgmtUpdateModulesFp)(pState->moduleStatus);
  }

  for (int32_t i = 0; i < pState->numOfVnodes; ++i) {
    SVnodeAccess *pAccess = &(pStatus->vnodeAccess[i]);
    pAccess->vnode = htonl(pAccess->vnode);
    //TODO set vnode access state
    //dnodeSetVnodeState(pAccess->accessState);
  }
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
  SRpcIpSet    *pIpList = &tsDnodeMgmtIpList;

  sprintf(fn, "%s/taos.cfg", configDir);
  fp = fopen(fn, "r");
  memset(pIpList, 0, sizeof(SRpcIpSet));
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
          dError("index:%d of tsDnodeMgmtIpList:%d:%s invalid", index, ip, ipStr);
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
    memset(pIpList, 0, sizeof(tsDnodeMgmtIpList));
    pIpList->numOfIps = 1;
    pIpList->ip[0] = inet_addr(tsMasterIp);
    if (tsSecondIp[0]) {
      pIpList->numOfIps = 3;
      pIpList->ip[1] = inet_addr(tsMasterIp);
      pIpList->ip[2] = inet_addr(tsSecondIp);
    }
  }

  if (pIpList->numOfIps >= 2) {
    tinet_ntoa(tsSecondIp, pIpList->ip[1]);
  }

  if (pIpList->numOfIps >= 1) {
    tinet_ntoa(tsMasterIp, pIpList->ip[0]);
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
    fprintf(fp, "mgmtNumOfIps     %d\n", tsDnodeMgmtIpList.numOfIps);
    for (int32_t i = 0; i < tsDnodeMgmtIpList.numOfIps; ++i) {
      char ipStr[20] = {0};
      tinet_ntoa(ipStr, tsDnodeMgmtIpList.ip[i]);
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
