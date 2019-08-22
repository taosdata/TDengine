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

#include <arpa/inet.h>
#include <assert.h>
#include <unistd.h>

#include "dnodeSystem.h"
#include "taosmsg.h"
#include "trpc.h"
#include "tsched.h"
#include "tsystem.h"
#include "vnode.h"
#include "vnodeMgmt.h"
#include "vnodeSystem.h"
#include "vnodeUtil.h"

int vnodeProcessVPeersMsg(char *msg);
int vnodeProcessCreateMeterMsg(char *pMsg);
int vnodeProcessFreeVnodeRequest(char *pMsg);
int vnodeProcessVPeerCfgRsp(char *msg);
int vnodeProcessMeterCfgRsp(char *msg);
int vnodeProcessAlterStreamRequest(char *pMsg);

void mgmtProcessMsgFromVnode(SSchedMsg *sched);
void *mgmtQhandle;

int vnodeSendMsgToMgmt(char *msg) {
  SSchedMsg schedMsg;
  schedMsg.fp = mgmtProcessMsgFromVnode;
  schedMsg.msg = msg;
  schedMsg.ahandle = NULL;
  schedMsg.thandle = NULL;
  taosScheduleTask(mgmtQhandle, &schedMsg);

  return 0;
}

void vnodeProcessMsgFromMgmt(SSchedMsg *sched) {
  char  msgType = *sched->msg;
  char *content = sched->msg + 1;

  dTrace("msg:%s is received from mgmt", taosMsg[msgType]);

  if (msgType == TSDB_MSG_TYPE_CREATE) {
    vnodeProcessCreateMeterRequest(content);
  } else if (msgType == TSDB_MSG_TYPE_VPEERS) {
    vnodeProcessVPeersMsg(content);
  } else if (msgType == TSDB_MSG_TYPE_VPEER_CFG_RSP) {
    vnodeProcessVPeerCfgRsp(content);
  } else if (msgType == TSDB_MSG_TYPE_METER_CFG_RSP) {
    vnodeProcessMeterCfgRsp(content);
  } else if (msgType == TSDB_MSG_TYPE_REMOVE) {
    vnodeProcessRemoveMeterRequest(content);
  } else if (msgType == TSDB_MSG_TYPE_FREE_VNODE) {
    vnodeProcessFreeVnodeRequest(content);
  } else if (msgType == TSDB_MSG_TYPE_ALTER_STREAM) {
    vnodeProcessAlterStreamRequest(content);
  } else {
    dError("%s is not processed", taosMsg[msgType]);
  }

  free(sched->msg);
}

int vnodeProcessMeterCfgRsp(char *pMsg) {
  int code = *pMsg;

  if (code == 0) {
    vnodeProcessCreateMeterMsg(pMsg + 1);
  } else {
    STaosRsp *pRsp;
    pRsp = (STaosRsp *)pMsg;
    int32_t *pint = (int32_t *)pRsp->more;
    int      vnode = htonl(*pint);
    int      sid = htonl(*(pint + 1));
    dError("vid:%d, sid:%d, meter is not configured, remove it", vnode, sid);
    int ret = vnodeRemoveMeterObj(vnode, sid);
    dTrace("vid:%d, sid:%d, meter delete ret:%d", vnode, sid, ret);
  }

  return 0;
}

int vnodeProcessCreateMeterRequest(char *pMsg) {
  SCreateMsg *pCreate;
  int         code = 0;
  int         vid;
  SVnodeObj * pVnode;
  char *      pStart;

  pCreate = (SCreateMsg *)pMsg;
  vid = htons(pCreate->vnode);

  if (vid >= TSDB_MAX_VNODES || vid < 0) {
    dError("vid:%d, vnode is out of range", vid);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _over;
  }

  pVnode = vnodeList + vid;
  if (pVnode->cfg.maxSessions <= 0) {
    dError("vid:%d, not activated", vid);
    code = TSDB_CODE_NOT_ACTIVE_SESSION;
    goto _over;
  }

  code = vnodeProcessCreateMeterMsg(pMsg);

_over:

  pStart = (char *)malloc(128);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_CREATE_RSP;
  pMsg = pStart + 1;

  *pMsg = code;
  vnodeSendMsgToMgmt(pStart);

  return code;
}

int vnodeProcessAlterStreamRequest(char *pMsg) {
  SAlterStreamMsg *pAlter;
  int              code = 0;
  int              vid, sid;
  SVnodeObj *      pVnode;
  char *           pStart;

  pAlter = (SAlterStreamMsg *)pMsg;
  vid = htons(pAlter->vnode);
  sid = htonl(pAlter->sid);

  if (vid >= TSDB_MAX_VNODES || vid < 0) {
    dError("vid:%d, vnode is out of range", vid);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _over;
  }

  pVnode = vnodeList + vid;
  if (pVnode->cfg.maxSessions <= 0 || pVnode->pCachePool == NULL) {
    dError("vid:%d is not activated yet", pAlter->vnode);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _over;
  }

  if (pAlter->sid >= pVnode->cfg.maxSessions || pAlter->sid < 0) {
    dError("vid:%d sid:%d uid:%ld, sid is out of range", pAlter->vnode, pAlter->sid, pAlter->uid);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _over;
  }

  SMeterObj *pMeterObj = vnodeList[vid].meterList[sid];
  if (pMeterObj == NULL || sid != pMeterObj->sid || vid != pMeterObj->vnode) {
    dError("vid:%d sid:%d, no active session", vid, sid);
    code = TSDB_CODE_NOT_ACTIVE_SESSION;
    goto _over;
  }

  pMeterObj->status = pAlter->status;
  if (pMeterObj->status == 1) {
    if (pAlter->stime > pMeterObj->lastKey)  // starting time can be specified
      pMeterObj->lastKey = pAlter->stime;
    vnodeCreateStream(pMeterObj);
  } else {
    vnodeRemoveStream(pMeterObj);
  }

  vnodeSaveMeterObjToFile(pMeterObj);

_over:
  pStart = (char *)malloc(128);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_ALTER_STREAM_RSP;
  pMsg = pStart + 1;

  *pMsg = code;
  vnodeSendMsgToMgmt(pStart);

  return code;
}

int vnodeProcessCreateMeterMsg(char *pMsg) {
  int         code;
  SMeterObj * pObj = NULL;
  SConnSec    connSec;
  SCreateMsg *pCreate = (SCreateMsg *)pMsg;

  pCreate->vnode = htons(pCreate->vnode);
  pCreate->sid = htonl(pCreate->sid);
  pCreate->lastCreate = htobe64(pCreate->lastCreate);
  pCreate->timeStamp = htobe64(pCreate->timeStamp);

  if (pCreate->vnode >= TSDB_MAX_VNODES || pCreate->vnode < 0) {
    dError("vid:%d is out of range", pCreate->vnode);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _create_over;
  }

  SVnodeObj *pVnode = vnodeList + pCreate->vnode;
  if (pVnode->pCachePool == NULL) {
    dError("vid:%d is not activated yet", pCreate->vnode);
    vnodeSendVpeerCfgMsg(pCreate->vnode);
    code = TSDB_CODE_NOT_ACTIVE_SESSION;
    goto _create_over;
  }

  if (pCreate->sid >= pVnode->cfg.maxSessions || pCreate->sid < 0) {
    dError("vid:%d sid:%d id:%s, sid is out of range", pCreate->vnode, pCreate->sid, pCreate->meterId);
    code = TSDB_CODE_INVALID_SESSION_ID;
    goto _create_over;
  }

  pCreate->numOfColumns = htons(pCreate->numOfColumns);
  if (pCreate->numOfColumns <= 0) {
    dTrace("vid:%d sid:%d id:%s, numOfColumns is out of range", pCreate->vnode, pCreate->sid, pCreate->meterId);
    code = TSDB_CODE_OTHERS;
    goto _create_over;
  }

  pCreate->sqlLen = htons(pCreate->sqlLen);
  pObj = (SMeterObj *)calloc(1, sizeof(SMeterObj) + pCreate->sqlLen + 1);
  if (pObj == NULL) {
    dError("vid:%d sid:%d id:%s, no memory to allocate meterObj", pCreate->vnode, pCreate->sid, pCreate->meterId);
    code = TSDB_CODE_NO_RESOURCE;
    goto _create_over;
  }

  /*
   * memory alignment may cause holes in SColumn struct which are not assigned any value
   * therefore, we could not use memcmp to compare whether two SColumns are equal or not.
   * So, we need to set the memory to 0 when allocating memory.
   */
  pObj->schema = (SColumn *)calloc(1, pCreate->numOfColumns * sizeof(SColumn));

  pObj->vnode = pCreate->vnode;
  pObj->sid = pCreate->sid;
  pObj->uid = pCreate->uid;
  memcpy(pObj->meterId, pCreate->meterId, TSDB_METER_ID_LEN);
  pObj->numOfColumns = pCreate->numOfColumns;
  pObj->timeStamp = pCreate->timeStamp;
  pObj->sversion = htonl(pCreate->sversion);
  pObj->maxBytes = 0;

  for (int i = 0; i < pObj->numOfColumns; ++i) {
    pObj->schema[i].type = pCreate->schema[i].type;
    pObj->schema[i].bytes = htons(pCreate->schema[i].bytes);
    pObj->schema[i].colId = htons(pCreate->schema[i].colId);
    pObj->bytesPerPoint += pObj->schema[i].bytes;
    if (pObj->maxBytes < pObj->schema[i].bytes) pObj->maxBytes = pObj->schema[i].bytes;
  }

  if (pCreate->sqlLen > 0) {
    pObj->sqlLen = pCreate->sqlLen;
    pObj->pSql = ((char *)pObj) + sizeof(SMeterObj);
    memcpy(pObj->pSql, (char *)pCreate->schema + pCreate->numOfColumns * sizeof(SMColumn), pCreate->sqlLen);
    pObj->pSql[pCreate->sqlLen] = 0;
  }

  pObj->pointsPerFileBlock = pVnode->cfg.rowsInFileBlock;

  if (sizeof(TSKEY) != pObj->schema[0].bytes) {
    dError("key length is not matched, required key length:%d", sizeof(TSKEY));
    code = TSDB_CODE_OTHERS;
    goto _create_over;
  }

  // security info shall be saved here
  connSec.spi = pCreate->spi;
  connSec.encrypt = pCreate->encrypt;
  memcpy(connSec.secret, pCreate->secret, TSDB_KEY_LEN);
  memcpy(connSec.cipheringKey, pCreate->cipheringKey, TSDB_KEY_LEN);

  code = vnodeCreateMeterObj(pObj, &connSec);

_create_over:
  if (code != TSDB_CODE_SUCCESS) {
    dTrace("vid:%d sid:%d id:%s, failed to create meterObj", pCreate->vnode, pCreate->sid, pCreate->meterId);
    tfree(pObj);
  }

  return code;
}

int vnodeProcessRemoveMeterRequest(char *pMsg) {
  SMeterObj *      pObj;
  SRemoveMeterMsg *pRemove;
  int              code = 0;
  char *           pStart;

  pRemove = (SRemoveMeterMsg *)pMsg;
  pRemove->vnode = htons(pRemove->vnode);
  pRemove->sid = htonl(pRemove->sid);

  if (pRemove->vnode < 0 || pRemove->vnode >= TSDB_MAX_VNODES) {
    dWarn("vid:%d sid:%d, already removed", pRemove->vnode, pRemove->sid);
    goto _remove_over;
  }

  if (vnodeList[pRemove->vnode].meterList == NULL) goto _remove_over;

  pObj = vnodeList[pRemove->vnode].meterList[pRemove->sid];
  if (pObj == NULL) goto _remove_over;

  if (memcmp(pObj->meterId, pRemove->meterId, TSDB_METER_ID_LEN) != 0) {
    dWarn("vid:%d sid:%d id:%s, remove ID:%s, meter ID not matched", pObj->vnode, pObj->sid, pObj->meterId,
          pRemove->meterId);
    goto _remove_over;
  }

  if (vnodeRemoveMeterObj(pRemove->vnode, pRemove->sid) == TSDB_CODE_ACTION_IN_PROGRESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    goto _remove_over;
  }

  dTrace("vid:%d sid:%d id:%s, meterObj is removed", pRemove->vnode, pRemove->sid, pRemove->meterId);

_remove_over:

  pStart = (char *)malloc(128);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_REMOVE_RSP;
  pMsg = pStart + 1;

  *pMsg = code;
  vnodeSendMsgToMgmt(pStart);

  return 0;
}

int vnodeProcessVPeerCfg(char *msg) {
  SVPeersMsg *pMsg = (SVPeersMsg *)msg;
  int         vnode;

  vnode = htonl(pMsg->vnode);
  if (vnode >= TSDB_MAX_VNODES) {
    dError("vid:%d, vnode is out of range", vnode);
    return -1;
  }

  if (vnodeList[vnode].status == TSDB_STATUS_CREATING) {
    dTrace("vid:%d, vnode is still under creating", vnode);
    return 0;
  }

  SVnodeCfg *pCfg = &pMsg->cfg;
  pCfg->vgId = htonl(pCfg->vgId);
  pCfg->maxSessions = htonl(pCfg->maxSessions);
  pCfg->cacheBlockSize = htonl(pCfg->cacheBlockSize);
  pCfg->cacheNumOfBlocks.totalBlocks = htonl(pCfg->cacheNumOfBlocks.totalBlocks);
  pCfg->daysPerFile = htonl(pCfg->daysPerFile);
  pCfg->daysToKeep1 = htonl(pCfg->daysToKeep1);
  pCfg->daysToKeep2 = htonl(pCfg->daysToKeep2);
  pCfg->daysToKeep = htonl(pCfg->daysToKeep);
  pCfg->commitTime = htonl(pCfg->commitTime);
  pCfg->blocksPerMeter = htons(pCfg->blocksPerMeter);
  pCfg->rowsInFileBlock = htonl(pCfg->rowsInFileBlock);

  dTrace("vid:%d, vgroup:%d, vpeer cfg received, sessions:%d, current session:%d", vnode, pCfg->vgId, pCfg->maxSessions,
         vnodeList[vnode].cfg.maxSessions);

  if (vnodeList[vnode].cfg.maxSessions == 0) {
    if (pCfg->maxSessions > 0) {
      return vnodeCreateVnode(vnode, pCfg, pMsg->vpeerDesc);
    }
  } else {
    if (pCfg->maxSessions <= 0) {
      vnodeRemoveVnode(vnode);
    }
  }

  return 0;
}

int vnodeProcessVPeerCfgRsp(char *msg) {
  STaosRsp *pRsp;

  pRsp = (STaosRsp *)msg;

  if (pRsp->code == 0) {
    vnodeProcessVPeerCfg(pRsp->more);
  } else {
    int32_t *pint = (int32_t *)pRsp->more;
    int      vnode = htonl(*pint);
    if (vnode < TSDB_MAX_VNODES && vnodeList[vnode].lastKey != 0) {
      dError("vnode:%d not configured, it shall be empty");
      vnodeRemoveVnode(vnode);
    } else {
      dTrace("vnode:%d is invalid", vnode);
    }
  }

  return 0;
}

int vnodeProcessVPeersMsg(char *msg) {
  int   code = 0;
  char *pStart, *pMsg;

  code = vnodeProcessVPeerCfg(msg);

  STaosRsp *  pRsp;
  SVPeersMsg *pVPeersMsg = (SVPeersMsg *)msg;

  pStart = (char *)malloc(128);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_VPEERS_RSP;
  pMsg = pStart + 1;

  pRsp = (STaosRsp *)pMsg;
  pRsp->code = code;
  memcpy(pRsp->more, pVPeersMsg->cfg.db, TSDB_DB_NAME_LEN);

  vnodeSendMsgToMgmt(pStart);

  return code;
}

int vnodeProcessFreeVnodeRequest(char *pMsg) {
  SFreeVnodeMsg *pFree;
  char *         pStart;

  pFree = (SFreeVnodeMsg *)pMsg;
  pFree->vnode = htons(pFree->vnode);

  if (pFree->vnode < 0 || pFree->vnode >= TSDB_MAX_VNODES) {
    dWarn("vid:%d out of range", pFree->vnode);
    return -1;
  }

  dTrace("vid:%d receive free vnode message", pFree->vnode);
  int32_t code = vnodeRemoveVnode(pFree->vnode);
  assert(code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS);

  pStart = (char *)malloc(128);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_FREE_VNODE_RSP;
  pMsg = pStart + 1;

  *pMsg = code;
  vnodeSendMsgToMgmt(pStart);

  return 0;
}

int vnodeSendVpeerCfgMsg(int vnode) {
  SVpeerCfgMsg *pCfg;
  char *        pStart, *pMsg;

  pStart = (char *)malloc(256);
  if (pStart == NULL) return -1;

  *pStart = TSDB_MSG_TYPE_VPEER_CFG;
  pMsg = pStart + 1;

  pCfg = (SVpeerCfgMsg *)pMsg;
  pCfg->vnode = htonl(vnode);
  pMsg += sizeof(SVpeerCfgMsg);

  vnodeSendMsgToMgmt(pStart);

  return 0;
}

int vnodeSendMeterCfgMsg(int vnode, int sid) {
  SMeterCfgMsg *pCfg;
  char *        pStart, *pMsg;

  pStart = (char *)malloc(256);
  if (pStart == NULL) return 0;

  *pStart = TSDB_MSG_TYPE_METER_CFG;
  pMsg = pStart + 1;

  pCfg = (SMeterCfgMsg *)pMsg;
  pCfg->vnode = htonl(vnode);
  pCfg->sid = htonl(sid);
  pMsg += sizeof(SMeterCfgMsg);

  vnodeSendMsgToMgmt(pStart);

  return 0;
}
