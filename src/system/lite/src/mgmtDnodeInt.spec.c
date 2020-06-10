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
#include <endian.h>

#include "dnodeSystem.h"
#include "mgmt.h"
#include "tsched.h"
#include "tutil.h"
#include "vnode.h"
#include "tsystem.h"
#include "vnodeStatus.h"

extern void *dmQhandle;
void * mgmtStatusTimer = NULL;
void   mgmtProcessMsgFromDnode(char *content, int msgLen, int msgType, SDnodeObj *pObj);
void   vnodeProcessMsgFromMgmtSpec(SSchedMsg *sched);

char *taosBuildRspMsgToDnodeWithSize(SDnodeObj *pObj, char type, int size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}

char *taosBuildReqMsgToDnodeWithSize(SDnodeObj *pObj, char type, int size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}

char *taosBuildRspMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildRspMsgToDnodeWithSize(pObj, type, 256);
}

char *taosBuildReqMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildReqMsgToDnodeWithSize(pObj, type, 256);
}

int taosSendSimpleRspToDnode(SDnodeObj *pObj, char rsptype, char code) { return 0; }

int taosSendMsgToDnode(SDnodeObj *pObj, char *msg, int msgLen) {
  mTrace("msg:%s is sent to dnode", taosMsg[(uint8_t)(*(msg-1))]);

  /*
   * Lite version has no message header, so minus one
   */
  SSchedMsg schedMsg;
  schedMsg.tfp = NULL;
  schedMsg.fp = vnodeProcessMsgFromMgmtSpec;
  schedMsg.msg = msg - 1;
  schedMsg.ahandle = NULL;
  schedMsg.thandle = NULL;
  taosScheduleTask(dmQhandle, &schedMsg);

  return 0;
}

int mgmtInitDnodeInt() { return 0; }

void mgmtCleanUpDnodeInt() {}

void mgmtProcessDnodeStatus(void *handle, void *tmrId) {
  SDnodeObj *pObj = &dnodeObj;
  pObj->openVnodes = tsOpenVnodes;
  pObj->status = TSDB_DN_STATUS_READY;

  float memoryUsedMB = 0;
  taosGetSysMemory(&memoryUsedMB);
  pObj->diskAvailable = tsAvailDataDirGB;

  for (int vnode = 0; vnode < pObj->numOfVnodes; ++vnode) {
    SVnodeLoad *pVload = &(pObj->vload[vnode]);
    SVnodeObj * pVnode = vnodeList + vnode;

    // wait vnode dropped
    if (pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
      if (vnodeList[vnode].cfg.maxSessions <= 0) {
        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
        pVload->status = TSDB_VN_STATUS_OFFLINE;
        mPrint("dnode:%s, vid:%d, drop finished", taosIpStr(pObj->privateIp), vnode);
        taosTmrStart(mgmtMonitorDbDrop, 10000, NULL, mgmtTmr);
      }
    }

    if (vnodeList[vnode].cfg.maxSessions <= 0) {
      continue;
    }

    pVload->vnode = vnode;
    pVload->status = TSDB_VN_STATUS_MASTER;
    pVload->totalStorage = pVnode->vnodeStatistic.totalStorage;
    pVload->compStorage = pVnode->vnodeStatistic.compStorage;
    pVload->pointsWritten = pVnode->vnodeStatistic.pointsWritten;
    uint32_t vgId = pVnode->cfg.vgId;

    SVgObj *pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup == NULL) {
      mError("vgroup:%d is not there, but associated with vnode %d", vgId, vnode);
      pVload->dropStatus = TSDB_VN_DROP_STATUS_DROPPING;
      continue;
    }

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not belongs to any database, vnode:%d", vgId, vnode);
      continue;
    }

    if (pVload->vgId == 0 || pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
      mError("vid:%d, mgmt not exist, drop it", vnode);
      pVload->dropStatus = TSDB_VN_DROP_STATUS_DROPPING;
    }
  }

  taosTmrReset(mgmtProcessDnodeStatus, tsStatusInterval * 1000, NULL, mgmtTmr, &mgmtStatusTimer);
  if (mgmtStatusTimer == NULL) {
    mError("Failed to start status timer");
  }
}

void mgmtProcessMsgFromDnodeSpec(SSchedMsg *sched) {
  char  msgType = *sched->msg;
  char *content = sched->msg + 1;
  mTrace("msg:%s is received from dnode", taosMsg[(uint8_t)msgType]);

  mgmtProcessMsgFromDnode(content, 0, msgType, mgmtGetDnode(0));
  free(sched->msg);
}
