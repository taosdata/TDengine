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

extern SMgmtObj mgmtObj;
extern void*tsStatusTimer;
void *      pDnodeMgmtConn = NULL;
uint64_t    tsCreatedTime = 0;
SMgmtIpList mgmtIpList;
SMgmtIpList mgmtPublicIpList;
char        mgmtIpStr[TSDB_MAX_MGMT_IPS][20] = {0};

void vnodeSaveMgmtIp();
void grantSendMsgToMgmt();
void vnodeSendStatusMsgToMgmt(void *handle, void *tmrId);
int  vnodeProcessStatusRspMsg(char *msg, int msgLen, SMgmtObj *pObj);
int  vnodeProcessCreateMeterMsg(char *pMsg, int msgLen);
int  vnodeProcessCfgDnodeRequest(char *cont, int contLen, SMgmtObj *pMgmtObj);
void dnodeDistributeMsgFromMgmt(char *content, int msgLen, int msgType, SMgmtObj *pObj);


char *taosBuildRspMsgToMnodeWithSizeClusterImp(SMgmtObj *pObj, char type, int size) {
  return taosBuildRspMsgWithSize(pObj->thandle, type, size);
}

char *taosBuildReqMsgToMnodeWithSizeClusterImp(SMgmtObj *pObj, char type, int size) {
  return taosBuildReqMsgWithSize(pObj->thandle, type, size);
}

char *taosBuildRspMsgToMnodeClusterImp(SMgmtObj *pObj, char type) {
  return taosBuildRspMsgToMnodeWithSize(pObj, type, 256);
}

char *taosBuildReqMsgToMnodeClusterImp(SMgmtObj *pObj, char type) {
  return taosBuildReqMsgToMnodeWithSize(pObj, type, 256);
}

int taosSendSimpleRspToMnodeClusterImp(SMgmtObj *pObj, char rsptype, char code) {
  return taosSendSimpleRsp(pObj->thandle, rsptype, code);
}

int taosSendMsgToMnodeClusterImp(SMgmtObj *pObj, char *msg, int msgLen) {
  return taosSendMsgToPeer(pObj->thandle, msg, msgLen);
}

void *dnodeProcessMsgFromMgmtClusterImp(char *msg, void *ahandle, void *thandle) {
  SMgmtObj *pObj = (SMgmtObj *)ahandle;
  SIntMsg * pMsg = (SIntMsg *)msg;

  if (msg == NULL) {
    pObj->thandle = NULL;
    dError("connection to mgmt node is gone");
    pObj->mgmtIndex = (pObj->mgmtIndex + 1) % mgmtIpList.numOfIps;
    if (tsStatusTimer) taosTmrStop(tsStatusTimer);
    taosTmrReset(vnodeSendStatusMsgToMgmt, tsStatusInterval * 1000, pObj, vnodeTmrCtrl, &tsStatusTimer);
    pObj->status = 0;
    return NULL;
  }

  if (pObj != &mgmtObj) {
    dError("BUG!!! pObj:0x%0x, mgmtObj:0x%x", pObj, &mgmtObj);
    if (pMsg->msgType & 1) taosSendSimpleRsp(thandle, pMsg->msgType + 1, TSDB_CODE_OTHERS);
    return NULL;
  }

  pObj->status = 1;

  if (pMsg->msgType == TSDB_MSG_TYPE_STATUS_RSP) {
    vnodeProcessStatusRspMsg((char *) (pMsg->content), pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_CFG_PNODE) {
    vnodeProcessCfgDnodeRequest((char *)(pMsg->content), pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else {
    dnodeDistributeMsgFromMgmt((char *)(pMsg->content), pMsg->msgLen - sizeof(SIntMsg), pMsg->msgType, pObj);
  }

  return pObj;
}

int dnodeInitMgmtConnClusterImp() {
  SMgmtObj *pObj;
  SRpcInit  rpcInit;

  pObj = &mgmtObj;

  memset(pObj, 0, sizeof(SMgmtObj));
  strcpy(pObj->id, tsPrivateIp);
  pObj->sid = 1;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsPrivateIp;
  rpcInit.localPort = 0;
  rpcInit.label = "DND-mgmt";
  rpcInit.numOfThreads = 1;
  rpcInit.fp = dnodeProcessMsgFromMgmtClusterImp;
  rpcInit.bits = 4;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = 10;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_C();
  rpcInit.qhandle = dmQhandle;

  taosTmrReset(vnodeSendStatusMsgToMgmt, 500, pObj, vnodeTmrCtrl, &tsStatusTimer);

  pDnodeMgmtConn = taosOpenRpc(&rpcInit);

  if (pDnodeMgmtConn == NULL) {
    dError("failed to init connection to mgmt");
    return -1;
  }

  return 0;
}

void vnodeCleanUpMgmt() {
  taosTmrStopA(&tsStatusTimer);

  if (pDnodeMgmtConn) taosCloseRpc(pDnodeMgmtConn);
  pDnodeMgmtConn = NULL;
}

char *vnodeProcessOneBufferedCreateMsg(char *offset) {
  int msgLen;

  memcpy(&msgLen, offset, sizeof(msgLen));
  offset += sizeof(msgLen);

  vnodeProcessCreateMeterMsg(offset, msgLen);
  offset += msgLen;

  return offset;
}

int vnodeProcessBufferedCreateMsgs(int vnode) {
  STranQueue *pQueue;
  int         trans = 0;
  char *      offset = NULL;

  pQueue = (STranQueue *)vnodeList[vnode].pQueue;

  /*
   * This function is called infrequently, should focus on security, increase the scope of the lock
   */

  /*
  offset = pQueue->buffer;

  while (trans < pQueue->trans) {
    offset = vnodeProcessOneBufferedCreateMsg(offset);
    trans++;
  }
  */

  pthread_mutex_lock(&pQueue->qmutex);

  if (pQueue->buffer == NULL) {
    dError("vid:%d, failed to process buffered create msg for buffer is null", vnode);
    pthread_mutex_unlock(&pQueue->qmutex);
    return -1;
  }

  if (offset == NULL) offset = pQueue->buffer;
  while (trans < pQueue->trans) {
    offset = vnodeProcessOneBufferedCreateMsg(offset);
    trans++;
  }

  vnodeList[vnode].syncStatus = TSDB_VN_SYNC_STATUS_SYNC_FILE;
  pQueue->offset = pQueue->buffer;
  pQueue->trans = 0;

  pthread_mutex_unlock(&pQueue->qmutex);

  return 0;
}

int vnodeSaveCreateMsgIntoQueue(SVnodeObj *pVnode, char *pMsg, int msgLen) {
  int         code = 0;
  STranQueue *pQueue = (STranQueue *)pVnode->pQueue;

  pthread_mutex_lock(&pQueue->qmutex);

  if (pQueue->buffer == NULL) {
    dError("vid:%d, failed to save create msg into queue for buffer is null", pVnode->vnode);
    pthread_mutex_unlock(&pQueue->qmutex);
    return -1;
  }

  if (pVnode->syncStatus == TSDB_VN_SYNC_STATUS_SYNCING) {
    if (pQueue->bufferSize - (pQueue->offset - pQueue->buffer) < msgLen + 100) {
      dError("vid:%d, buffer size:%d is too small", pVnode->vnode, pQueue->bufferSize);
      vnodeCancelSync(pVnode->vnode);
    } else {
      memcpy(pQueue->offset, &msgLen, sizeof(msgLen));
      pQueue->offset += sizeof(msgLen);
      memcpy(pQueue->offset, pMsg, msgLen);
      pQueue->offset += msgLen;
      pQueue->trans++;

      dTrace("vid:%d, create req is queued", pVnode->vnode);
    }
  } else {
    code = vnodeProcessCreateMeterMsg(pMsg, msgLen);
  }

  pthread_mutex_unlock(&pQueue->qmutex);

  return code;
}

void vnodeSendStatusMsgToMgmt(void *handle, void *tmrId) {
  char *       pMsg, *pStart;
  int          msgLen;
  SStatusMsg * pStatus;
  SVnodeLoad * pLoad;
  SMgmtObj *   pObj = (SMgmtObj *)handle;
  SRpcConnInit connInit;
  uint8_t      code;

  taosTmrReset(vnodeSendStatusMsgToMgmt, tsStatusInterval * 1000, pObj, vnodeTmrCtrl, &tsStatusTimer);
  if (tsStatusTimer == NULL) {
    dError("Failed to start status timer");
  }

  if (pObj->thandle == NULL) {
    if (pDnodeMgmtConn == NULL) return;
    strcpy(pObj->id, tsPrivateIp);
    connInit.cid = 0;
    connInit.sid = 0;
    connInit.spi = 0;
    connInit.encrypt = 0;
    connInit.meterId = pObj->id;
    connInit.peerId = 0;
    connInit.shandle = pDnodeMgmtConn;
    connInit.ahandle = pObj;
    connInit.peerIp = mgmtIpStr[pObj->mgmtIndex];
    connInit.peerPort = tsMgmtVnodePort;

    dPrint("mgmt ip:%s is picked up", connInit.peerIp);
    pObj->thandle = taosOpenRpcConn(&connInit, &code);
  }

  pStart = taosBuildReqMsgWithSize(pObj->thandle, TSDB_MSG_TYPE_STATUS, tsOpenVnodes * sizeof(SVnodeLoad) + 160);
  if (pStart == NULL) return;
  pMsg = pStart;

  pStatus = (SStatusMsg *)pMsg;

  pStatus->version = htonl(tsVersion);
  pStatus->publicIp = htonl(inet_addr(tsPublicIp));
  pStatus->lastReboot = htonl(tsRebootTime);
  pStatus->numOfCores = htons((uint16_t)tsNumOfCores);
  pStatus->alternativeRole = (uint8_t)tsAlternativeRole;

  pStatus->numOfTotalVnodes = htons((uint16_t)tsNumOfTotalVnodes);
  pStatus->diskAvailable = tsAvailDataDirGB;

  pStatus->openVnodes = htonl(tsOpenVnodes);

  pMsg += sizeof(SStatusMsg);
  pLoad = (SVnodeLoad *)pMsg;

  for (int vnode = 0, count = 0; vnode <= tsMaxVnode; ++vnode) {
    if (vnodeList[vnode].cfg.maxSessions <= 0) continue;
    SVnodeObj *pVnode = vnodeList + vnode;
    pLoad->vnode = htonl(vnode);
    pLoad->vgId = htonl(pVnode->cfg.vgId);
    //int status = vnodeList[vnode].status > TSDB_STATUS_UNSYNCED) ? TSDB_STATUS_READY : TSDB_STATUS_UNSYNCED;
    //pLoad->status = (uint8_t)status;
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
    pMsg += sizeof(SVnodeLoad);

    if (++count >= tsOpenVnodes) {
      break;
    }
  }

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pObj->thandle, pStart, msgLen);

  grantSendMsgToMgmt();
}

int vnodeProcessStatusRspMsg(char *msg, int msgLen, SMgmtObj *pObj) {
  STaosRsp *pRsp;
  SIpList * pIpList;
  char *    pMsg;

  pRsp = (STaosRsp *)msg;
  pMsg = (char *)pRsp->more;

  if (pRsp->code != TSDB_CODE_REDIRECT && pRsp->code != 0) {
    dTrace("status is rejected by mgmt node, code:%d", pRsp->code);
    // taosCloseRpcConn(pObj->thandle);
    // pObj->thandle = NULL;
    return 0;
  }

  pIpList = (SIpList *)pMsg;
  mgmtIpList.numOfIps = pIpList->numOfIps;
  if (mgmtIpList.numOfIps <= 0) {
    dError("bug!!!, num of mgmt IPs is:%d", mgmtIpList.numOfIps);
    mgmtIpList.numOfIps = 1;
  }

  uint32_t oldMasterIp = mgmtIpList.ip[0];
  int      size = pIpList->numOfIps * 4;

  pMsg += sizeof(SIpList) + size;

  if (memcmp(pIpList->ip, mgmtIpList.ip, size) != 0) {
    dPrint("mgmt ip list is changed, numOfIps:%d", pIpList->numOfIps);
    for (int i = 0; i < pIpList->numOfIps; ++i) {
      tinet_ntoa(mgmtIpStr[i], pIpList->ip[i]);
      mgmtIpList.ip[i] = pIpList->ip[i];
      dPrint("mgmt IP index:%d ip:%s", i, mgmtIpStr[i]);
    }

    vnodeSaveMgmtIp();
  }

  pIpList = (SIpList *)pMsg;
  if (memcmp(pIpList->ip, mgmtPublicIpList.ip, size) != 0) {
    // Update public Ip address
    mgmtPublicIpList.numOfIps = pIpList->numOfIps;
    for (int i = 0; i < pIpList->numOfIps; ++i) {
      mgmtPublicIpList.ip[i] = pIpList->ip[i];
      dPrint("mgmt Public IP index:%d, ip:%s", i, taosIpStr(mgmtPublicIpList.ip[i]));
    }
  }

  if (pRsp->code == TSDB_CODE_REDIRECT) {
    if (oldMasterIp != mgmtIpList.ip[0]) {
      pObj->mgmtIndex = 0;
    } else {
      pObj->mgmtIndex = (pObj->mgmtIndex + 1) % mgmtIpList.numOfIps;
    }

    dTrace("redirected to different mgmt node");
    taosCloseRpcConn(pObj->thandle);
    pObj->thandle = NULL;

    // while cluster in unsynced state, all mgmt will return redirect msg
    // then the status msg will send too many times
    taosTmrReset(vnodeSendStatusMsgToMgmt, 1000, pObj, vnodeTmrCtrl, &tsStatusTimer);
    return 0;
  }

  pMsg += size + sizeof(SIpList);
  /* while ( pMsg < msg + msgLen ) { */
  if (*pMsg == TSDB_IE_TYPE_DNODE_STATE) {
    pMsg++;
    SDnodeState *pState = (SDnodeState *)pMsg;

    uint32_t mgmtCreatedTime = htonl(pState->createdTime);
    if (mgmtCreatedTime > tsCreatedTime) {
      // tsCreatedTime is save at taos.cfg
      // and may be changed by user sometimes
      // so we delete this logic
      // if ( tsCreatedTime )
      //  dnodeResetSystem();
      tsCreatedTime = mgmtCreatedTime;
      vnodeSaveMgmtIp();
    }

    uint32_t status = htonl(pState->moduleStatus);
    if (status != tsModuleStatus) {
      dPrint("module status is received, old:%d, new:%d", tsModuleStatus, status);
      dnodeProcessModuleStatus(status);
    }

    pMsg += sizeof(SDnodeState);
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

int vnodeRebuildCreateMsg(int vid, int sid, char *msg) {
  int         len;
  SMeterObj * pObj = vnodeList[vid].meterList[sid];
  SCreateMsg *pCreate;

  pCreate = (SCreateMsg *)msg;
  pCreate->vnode = htons(vid);
  pCreate->sid = htonl(sid);
  pCreate->numOfColumns = htons(pObj->numOfColumns);
  memcpy(pCreate->meterId, pObj->meterId, TSDB_METER_ID_LEN);
  pCreate->timeStamp = htobe64(pObj->timeStamp);
  pCreate->uid = pObj->uid;
  pCreate->sqlLen = htons(pObj->sqlLen);
  pCreate->sversion = htonl(pObj->sversion);

  /*
    SConnSec  *pConnSec;
    pConnSec = vnodeGetMeterSec(vid, sid);
    pCreate->spi = pConnSec->spi;
    pCreate->encrypt = pConnSec->encrypt;
    memcpy(pCreate->secret, pConnSec->secret, TSDB_KEY_LEN);
    memcpy(pCreate->cipheringKey, pConnSec->cipheringKey, TSDB_KEY_LEN);
  */
  assert((pObj->numOfColumns < TSDB_MAX_COLUMNS) && (pObj->numOfColumns > 0));
  for (int i = 0; i < pObj->numOfColumns; ++i) {
    pCreate->schema[i].type = pObj->schema[i].type;
    // strcpy(pCreate->schema[i].name, pObj->schema[i].name);
    pCreate->schema[i].bytes = htons(pObj->schema[i].bytes);
    pCreate->schema[i].colId = htons(pObj->schema[i].colId);
  }

  if (pObj->sqlLen) {
    char *sqlstr = ((char *)(pCreate->schema)) + pObj->numOfColumns * sizeof(SMColumn);
    strcpy(sqlstr, pObj->pSql);
  }

  len = sizeof(SCreateMsg) + pObj->numOfColumns * sizeof(SMColumn) + pObj->sqlLen;

  return len;
}

int vnodeRetrieveMissedCreateMsg(int vnode, int fd, uint64_t stime) {
  int        sid = 0;
  int        code = -1;
  uint32_t   len;
  SMeterObj *pObj;
  char *     msg;
  SVnodeObj *pVnode = vnodeList + vnode;
  int        writeLen;

  msg = (char *)malloc(1024 + TSDB_MAX_COLUMNS * sizeof(SSchema));

  dTrace("vid:%d, fd:%d start to retrieve missed create msg, stime:%" PRIu64, vnode, fd, stime);

  for (sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    pObj = pVnode->meterList[sid];

    if (pObj && !vnodeIsMeterState(pObj, TSDB_METER_STATE_DROPPED) && (pObj->timeStamp > stime)) {
      len = vnodeRebuildCreateMsg(vnode, sid, msg);
      writeLen = taosWriteMsg(fd, &len, sizeof(len));
      if (writeLen < 0) {
        dError("vid:%d, fd:%d failed to retrieve missed create msg len, writeLen:%d reason:%s", vnode, fd, writeLen, strerror(errno));
        goto _exit;
      }

      writeLen = taosWriteMsg(fd, msg, len);
      if (writeLen < 0) {
        dError("vid:%d, fd:%d failed to retrieve missed create msg, writeLen:%d reason:%s", vnode, fd, writeLen, strerror(errno));
        goto _exit;
      }

      dTrace("vid:%d sid:%d id:%s, meterObj is sent to peer, len:%d", vnode, sid, pObj->meterId, len);
    }
  }

  len = 0;
  writeLen = taosWriteMsg(fd, (char *)&len, sizeof(len));
  if (writeLen < 0) {
    dError("vid:%d, fd:%d failed to retrieve missed create msg end, writeLen:%d reason:%s", vnode, fd, writeLen, strerror(errno));
    goto _exit;
  }
  code = 0;

  dTrace("vid:%d, fd:%d retrieve missed create msg finished", vnode, fd);

_exit:
  free(msg);
  return code;
}

int vnodeRetrieveMissedRemoveMsg(int vid, int fd, uint64_t stime) {
  SMeterObj *pObj;
  int        sid, writeLen;
  SVnodeObj *pVnode = vnodeList + vid;
  int        oldVid = vid;

  dTrace("vid:%d, fd:%d start to retrieve missed remove msg", vid, fd);

  for (sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    pObj = pVnode->meterList[sid];

    if (pObj && (pObj->state == TSDB_METER_STATE_DROPPED) && (pObj->timeStamp > stime)) {
      writeLen = taosWriteMsg(fd, (char *)&vid, sizeof(vid));
      if (writeLen < 0) {
        dError("vid:%d, fd:%d failed to retrieve missed remove msg vid:%d, writeLen:%d reason:%s", vid, fd, vid, writeLen, strerror(errno));
        return -1;
      }

      writeLen = taosWriteMsg(fd, (char *)&sid, sizeof(sid));
      if (writeLen < 0) {
        dError("vid:%d, fd:%d failed to retrieve missed remove msg sid:%d, writeLen:%d reason:%s", vid, fd, sid, writeLen, strerror(errno));
        return -1;
      }
      dTrace("vid:%d sid:%d id:%s, removed meterObj is sent to peer", vid, sid, pObj->meterId);
    }
  }

  vid = -1;
  sid = -1;

  writeLen = taosWriteMsg(fd, (char *)&vid, sizeof(vid));
  if (writeLen < 0) {
    dError("vid:%d, fd:%d failed to retrieve missed remove msg vid:%d, writeLen:%d reason:%s", vid, fd, vid, writeLen, strerror(errno));
    return -1;
  }

  writeLen = taosWriteMsg(fd, (char *)&sid, sizeof(sid));
  if (writeLen < 0) {
    dError("vid:%d, fd:%d failed to retrieve missed remove msg sid:%d, writeLen:%d reason:%s", vid, fd, sid, writeLen, strerror(errno));
    return -1;
  }

  dTrace("vid:%d, fd:%d retrieve missed remove msg finished", oldVid, fd);

  return 0;
}

int vnodeRestoreMissedCreateMsg(int vnode, int fd) {
  char        msg[1024 + TSDB_MAX_COLUMNS * sizeof(SSchema)];
  uint32_t    len;
  SCreateMsg *pCreate;

  dTrace("vid:%d, fd:%d start to restore missed create msg", vnode, fd);

  while (1) {
    len = 0;

    int readLen = taosReadMsg(fd, &len, sizeof(len));
    if (readLen < 0) {
      dError("vid:%d, fd:%d failed to restore missed create msg len, readLen:%d reason:%s", vnode, fd, readLen, strerror(errno));
      return -1;
    }

    if (len == 0) {
      dTrace("vid:%d, fd:%d restore missed create msg len:%d, finished", vnode, fd, len);
      break;
    }

    readLen = taosReadMsg(fd, msg, len);
    if (readLen < 0) {
      dError("vid:%d, fd:%d failed to restore missed create msg, size:%d readLen:%d reason:%s",
              vnode, fd, len, readLen, strerror(errno));
      return -1;
    }

    pCreate = (SCreateMsg *)msg;
    pCreate->vnode = htons(vnode);

    dTrace("vid:%d, fd:%d missed create is restored, vnode:%d len:%d", vnode, fd, pCreate->vnode, len);

    vnodeProcessCreateMeterMsg(msg, len);
  }

  dTrace("vid:%d, fd:%d to restore missed create msg finished", vnode, fd);

  return 0;
}

int vnodeRestoreMissedRemoveMsg(int vnode, int fd) {
  int vid, sid;

  dTrace("vid:%d, fd:%d start to restore missed remove msg", vnode, fd);

  while (1) {
    int readLen = taosReadMsg(fd, &vid, sizeof(vid));
    if (readLen < 0) {
      dError("vid:%d, fd:%d failed to restore missed remove msg vid, size:%d read:%d reason:%s",
              vnode, fd, sizeof(vid), readLen, strerror(errno));
      return -1;
    }

    readLen = taosReadMsg(fd, &sid, sizeof(sid));
    if (readLen < 0) {
      dError("vid:%d, fd:%d failed to restore missed remove msg sid, size:%d read:%d reason:%s",
              vnode, fd, sizeof(sid), readLen, strerror(errno));
      return -1;
    }

    if (sid == -1) {
      dTrace("vid:%d, fd:%d restore missed remove msg sid:%d, finished", vnode, fd, sid);
      break;
    }

    if (vid == -1) {
      dTrace("vid:%d, fd:%d restore missed remove msg vid:%d, finished", vnode, fd, vid);
      break;
    }

    dTrace("vid:%d, fd:%d missed remove msg is restored, vid:%d sid:%d", vnode, fd, vid, sid);

    vnodeRemoveMeterObj(vid, sid);
  }

  dTrace("vid:%d, fd:%d restore missed remove msg finished", vnode, fd);

  return 0;
}

bool vnodeSeekMgmtIp(FILE *fp) {
  char * line, *option;
  size_t len;
  int    olen;
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

void dnodeInitMgmtIpClusterImp() {
  FILE *       fp;
  char         fn[128];
  SMgmtIpList *pIpList = &mgmtIpList;

  sprintf(fn, "%s/taos.cfg", configDir);
  fp = fopen(fn, "r");
  memset(pIpList, 0, sizeof(mgmtIpList));
  tsCreatedTime = 0;

  if (fp) {
    char * line, *option;
    int    olen;
    size_t len;

    vnodeSeekMgmtIp(fp);

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
        int numOfIps = -1;
        sscanf(rest, "%d", &numOfIps);
        if (numOfIps >= 0 && numOfIps < TSDB_MAX_MGMT_IPS) {
          pIpList->numOfIps = numOfIps;
        } else {
          dError("num:%d of mgmtIps invalid", numOfIps);
        }
      } else if (strcmp(option, "mgmtIp") == 0) {
        int  index = -1;
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
    for (int i = 0; i < pIpList->numOfIps; ++i) {
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

  for (int i = 0; i < pIpList->numOfIps; ++i) {
    tinet_ntoa(mgmtIpStr[i], pIpList->ip[i]);
  }

  dPrint("%d mgmt IPs are configured:", pIpList->numOfIps);
  for (int i = 0; i < pIpList->numOfIps; ++i) {
    dPrint("index:%d ip:%s", i, mgmtIpStr[i]);
  }

  if (pIpList->numOfIps >= 3) {
    strcpy(tsSecondIp, mgmtIpStr[2]);
  }

  if (pIpList->numOfIps >= 2) {
    strcpy(tsMasterIp, mgmtIpStr[1]);
  }
}

void vnodeSaveMgmtIp() {
  FILE *fp;
  char  fn[128];
  /* int         size; */

  /* size = sizeof(mgmtIpList); */

  sprintf(fn, "%s/taos.cfg", configDir);
  fp = fopen(fn, "r+");
  if (fp) {
    if (!vnodeSeekMgmtIp(fp)) {
      fprintf(fp, "\n##############################################################\n");
      fprintf(fp, "# The following parameters are the cache of management ip list\n");
    }

    fprintf(fp, "mgmtIpCreateTime %" PRIu64 "\n", tsCreatedTime);
    fprintf(fp, "mgmtNumOfIps     %d\n", mgmtIpList.numOfIps);
    for (int i = 0; i < mgmtIpList.numOfIps; ++i) {
      char ipStr[20] = {0};
      tinet_ntoa(ipStr, mgmtIpList.ip[i]);
      fprintf(fp, "mgmtIp %d         %s\n", i, ipStr);
    }

    fclose(fp);
  } else {
    dError("failed to write file:%s", fn);
  }
}
