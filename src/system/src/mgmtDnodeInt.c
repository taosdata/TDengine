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
#pragma GCC diagnostic ignored "-Wpointer-sign"

int mgmtSendVPeersMsg(SVgObj *pVgroup, SDbObj *pDb);
char *mgmtBuildVpeersIe(char *pMsg, SVgObj *pVgroup, SDbObj *pDb);
char *mgmtBuildCreateMeterIe(STabObj *pMeter, char *pMsg, int vnode);

void vnodeProcessMsgFromMgmt(SSchedMsg *smsg);
void *rpcQhandle;

int mgmtSendMsgToDnode(char *msg) {
  mTrace("msg:%s is sent to dnode", taosMsg[*msg]);

  SSchedMsg schedMsg;
  schedMsg.fp = vnodeProcessMsgFromMgmt;
  schedMsg.msg = msg;
  schedMsg.ahandle = NULL;
  schedMsg.thandle = NULL;
  taosScheduleTask(rpcQhandle, &schedMsg);

  return 0;
}

int mgmtProcessMeterCfgMsg(unsigned char *cont) {
  char *        pMsg, *pStart;
  STabObj *     pMeter = NULL;
  SMeterCfgMsg *pCfg = (SMeterCfgMsg *)cont;
  SVgObj *      pVgroup;
  SDnodeObj *   pObj = &dnodeObj;

  int vnode = htonl(pCfg->vnode);
  int sid = htonl(pCfg->sid);

  pStart = (char *)malloc(64000);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_METER_CFG_RSP;
  pMsg = pStart + 1;

  if (vnode < pObj->numOfVnodes) {
    int vgId = pObj->vload[vnode].vgId;

    pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup) pMeter = pVgroup->meterList[sid];
  }

  if (pMeter) {
    *pMsg = 0;  // code
    pMsg++;
    pMsg = mgmtBuildCreateMeterIe(pMeter, pMsg, vnode);
  } else {
    mTrace("vnode:%d sid:%d, meter not there", vnode, sid);
    *pMsg = TSDB_CODE_INVALID_METER_ID;
    pMsg++;

    *(int32_t *)pMsg = htonl(vnode);
    pMsg += sizeof(int32_t);
    *(int32_t *)pMsg = htonl(sid);
    pMsg += sizeof(int32_t);
  }

  mgmtSendMsgToDnode(pStart);

  return 0;
}

int mgmtProcessVpeerCfgMsg(unsigned char *cont) {
  char *        pMsg, *pStart;
  SVpeerCfgMsg *pCfg = (SVpeerCfgMsg *)cont;
  SVgObj *      pVgroup = NULL;
  SDnodeObj *   pObj = &dnodeObj;

  int vnode = htonl(pCfg->vnode);

  pStart = (char *)malloc(256);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_VPEER_CFG_RSP;
  pMsg = pStart + 1;

  if (vnode < pObj->numOfVnodes) pVgroup = mgmtGetVgroup(pObj->vload[vnode].vgId);

  if (pVgroup) {
    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    *pMsg = 0;
    pMsg++;
    pMsg = mgmtBuildVpeersIe(pMsg, pVgroup, pDb);
  } else {
    mTrace("vnode:%d, no vgroup info, vgroup:%d", vnode, pObj->vload[vnode].vgId);
    *pMsg = TSDB_CODE_INVALID_VALUE;
    pMsg++;
    *(int32_t *)pMsg = htonl(vnode);
    pMsg += sizeof(int32_t);
  }

  return 0;
}

int mgmtProcessCreateRsp(unsigned char *msg) { return 0; }

int mgmtProcessFreeVnodeRsp(unsigned char *msg) { return 0; }

int mgmtProcessVPeersRsp(unsigned char *msg) {
  STaosRsp *pRsp = (STaosRsp *)msg;

  SDbObj *pDb = mgmtGetDb(pRsp->more);
  if (!pDb) {
    mError("db not find, code:%d", pRsp->code);
    return 0;
  }

  if (pDb->vgStatus != TSDB_VG_STATUS_IN_PROGRESS) {
    mTrace("db:%s vpeer rsp already disposed, code:%d", pRsp->more, pRsp->code);
    return 0;
  }

  if (pRsp->code == 0) {
    pDb->vgStatus = TSDB_VG_STATUS_READY;
    mTrace("db:%s vgroup is created in dnode", pRsp->more);
    return 0;
  }

  if (pRsp->code == TSDB_CODE_VG_COMMITLOG_INIT_FAILED) {
    pDb->vgStatus = TSDB_VG_STATUS_COMMITLOG_INIT_FAILED;
  } else {
    pDb->vgStatus = TSDB_VG_STATUS_INIT_FAILED;
  }
  mError("db:%s vgroup create failed, code:%d", pRsp->more, pRsp->code);

  return 0;
}

void mgmtProcessMsgFromVnode(SSchedMsg *sched) {
  char  msgType = *sched->msg;
  char *content = sched->msg + 1;

  mTrace("msg:%s is received from dnode", taosMsg[msgType]);

  if (msgType == TSDB_MSG_TYPE_METER_CFG) {
    mgmtProcessMeterCfgMsg(content);
  } else if (msgType == TSDB_MSG_TYPE_VPEER_CFG) {
    mgmtProcessVpeerCfgMsg(content);
  } else if (msgType == TSDB_MSG_TYPE_CREATE_RSP) {
    mgmtProcessCreateRsp(content);
  } else if (msgType == TSDB_MSG_TYPE_REMOVE_RSP) {
    // do nothing
  } else if (msgType == TSDB_MSG_TYPE_VPEERS_RSP) {
    mgmtProcessVPeersRsp(content);
  } else if (msgType == TSDB_MSG_TYPE_FREE_VNODE_RSP) {
    mgmtProcessFreeVnodeRsp(content);
  } else if (msgType == TSDB_MSG_TYPE_CFG_PNODE_RSP) {
    // do nothing;
  } else if (msgType == TSDB_MSG_TYPE_ALTER_STREAM_RSP) {
    // do nothing;
  } else {
    mError("%s from dnode is not processed", taosMsg[msgType]);
  }

  free(sched->msg);
}

char *mgmtBuildCreateMeterIe(STabObj *pMeter, char *pMsg, int vnode) {
  SCreateMsg *pCreateMeter;

  pCreateMeter = (SCreateMsg *)pMsg;
  pCreateMeter->vnode = htons(vnode);
  pCreateMeter->sid = htonl(pMeter->gid.sid);
  pCreateMeter->uid = pMeter->uid;
  memcpy(pCreateMeter->meterId, pMeter->meterId, TSDB_METER_ID_LEN);

  // pCreateMeter->lastCreate = htobe64(pVgroup->lastCreate);
  pCreateMeter->timeStamp = htobe64(pMeter->createdTime);
  /*
      pCreateMeter->spi = pSec->spi;
      pCreateMeter->encrypt = pSec->encrypt;
      memcpy(pCreateMeter->cipheringKey, pSec->cipheringKey, TSDB_KEY_LEN);
      memcpy(pCreateMeter->secret, pSec->secret, TSDB_KEY_LEN);
  */
  pCreateMeter->sversion = htonl(pMeter->sversion);
  pCreateMeter->numOfColumns = htons(pMeter->numOfColumns);
  SSchema *pSchema = mgmtGetMeterSchema(pMeter);

  for (int i = 0; i < pMeter->numOfColumns; ++i) {
    pCreateMeter->schema[i].type = pSchema[i].type;
    /* strcpy(pCreateMeter->schema[i].name, pSchema[i].name); */
    pCreateMeter->schema[i].bytes = htons(pSchema[i].bytes);
    pCreateMeter->schema[i].colId = htons(pSchema[i].colId);
  }

  pMsg = ((char *)(pCreateMeter->schema)) + pMeter->numOfColumns * sizeof(SMColumn);
  pCreateMeter->sqlLen = 0;

  if (pMeter->pSql) {
    int len = strlen(pMeter->pSql) + 1;
    pCreateMeter->sqlLen = htons(len);
    strcpy(pMsg, pMeter->pSql);
    pMsg += len;
  }

  return pMsg;
}

int mgmtSendCreateMsgToVnode(STabObj *pMeter, int vnode) {
  char *pMsg, *pStart;

  pStart = (char *)malloc(64000);
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_CREATE;
  pMsg = pStart + 1;

  pMsg = mgmtBuildCreateMeterIe(pMeter, pMsg, vnode);
  mgmtSendMsgToDnode(pStart);

  return 0;
}

int mgmtSendRemoveMeterMsgToVnode(STabObj *pMeter, int vnode) {
  SRemoveMeterMsg *pRemove;
  char *           pMsg, *pStart;

  pStart = (char *)malloc(1+sizeof(SRemoveMeterMsg));
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_REMOVE;
  pMsg = pStart + 1;

  pRemove = (SRemoveMeterMsg *)pMsg;
  pRemove->vnode = htons(vnode);
  pRemove->sid = htonl(pMeter->gid.sid);
  memcpy(pRemove->meterId, pMeter->meterId, TSDB_METER_ID_LEN);

  mgmtSendMsgToDnode(pStart);
  mTrace("vid:%d, send remove meter msg, sid:%d", vnode, pMeter->gid.sid);

  return 0;
}

int mgmtSendAlterStreamMsgToVnode(STabObj *pMeter, int vnode) {
  SAlterStreamMsg *pAlter;
  char *           pMsg, *pStart;

  pStart = (char *)malloc(128);
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_ALTER_STREAM;
  pMsg = pStart + 1;

  pAlter = (SAlterStreamMsg *)pMsg;
  pAlter->vnode = htons(vnode);
  pAlter->sid = htonl(pMeter->gid.sid);
  pAlter->uid = pMeter->uid;
  pAlter->status = pMeter->status;

  mgmtSendMsgToDnode(pStart);

  return 0;
}

char *mgmtBuildVpeersIe(char *pMsg, SVgObj *pVgroup, SDbObj *pDb) {
  SVPeersMsg *pVPeers = (SVPeersMsg *)pMsg;

  pVPeers->vnode = htonl(pVgroup->vnodeGid[0].vnode);

  pVPeers->cfg = pDb->cfg;
  SVnodeCfg *pCfg = &pVPeers->cfg;
  pCfg->vgId = htonl(pVgroup->vgId);
  pCfg->maxSessions = htonl(pCfg->maxSessions);
  pCfg->cacheBlockSize = htonl(pCfg->cacheBlockSize);
  pCfg->cacheNumOfBlocks.totalBlocks = htonl(pCfg->cacheNumOfBlocks.totalBlocks);
  pCfg->daysPerFile = htonl(pCfg->daysPerFile);
  pCfg->daysToKeep1 = htonl(pCfg->daysToKeep1);
  pCfg->daysToKeep2 = htonl(pCfg->daysToKeep2);
  pCfg->daysToKeep = htonl(pCfg->daysToKeep);
  pCfg->commitTime = htonl(pCfg->commitTime);
  pCfg->blocksPerMeter = htons(pCfg->blocksPerMeter);
  pCfg->replications = 1;
  pCfg->rowsInFileBlock = htonl(pCfg->rowsInFileBlock);

  return pMsg;
}

int mgmtSendVPeersMsg(SVgObj *pVgroup, SDbObj *pDb) {
  char *pMsg, *pStart;

  pStart = (char *)malloc(1024);
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_VPEERS;
  pMsg = pStart + 1;

  pMsg = mgmtBuildVpeersIe(pMsg, pVgroup, pDb);

  mgmtSendMsgToDnode(pStart);

  return 0;
}

int mgmtSendFreeVnodeMsg(int vnode) {
  SFreeVnodeMsg *pFreeVnode;
  char *         pMsg, *pStart;

  pStart = (char *)malloc(128);
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_FREE_VNODE;
  pMsg = pStart + 1;

  pFreeVnode = (SFreeVnodeMsg *)pMsg;
  pFreeVnode->vnode = htons(vnode);

  mgmtSendMsgToDnode(pStart);

  return 0;
}

int mgmtCfgDynamicOptions(SDnodeObj *pDnode, char *msg) { return 0; }
