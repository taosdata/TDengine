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

#include "dnodeSystem.h"
#include "taosmsg.h"
#include "trpc.h"
#include "tsched.h"
#include "tsystem.h"
#include "vnode.h"
#include "vnodeMgmt.h"
#include "vnodeSystem.h"
#include "vnodeUtil.h"
#include "vnodeStatus.h"

SMgmtObj mgmtObj;
extern uint64_t tsCreatedTime;

int vnodeProcessVPeersMsg(char *msg, int msgLen, SMgmtObj *pMgmtObj);
int vnodeProcessCreateMeterMsg(char *pMsg, int msgLen);
int vnodeProcessFreeVnodeRequest(char *pMsg, int msgLen, SMgmtObj *pMgmtObj);
int vnodeProcessVPeerCfgRsp(char *msg, int msgLen, SMgmtObj *pMgmtObj);
int vnodeProcessMeterCfgRsp(char *msg, int msgLen, SMgmtObj *pMgmtObj);
int vnodeProcessCfgDnodeRequest(char *cont, int contLen, SMgmtObj *pMgmtObj);
int vnodeProcessAlterStreamRequest(char *pMsg, int msgLen, SMgmtObj *pObj);
void vnodeUpdateHeadFile(int vnode, int oldTables, int newTables);
void vnodeOpenVnode(int vnode);
void vnodeCleanUpOneVnode(int vnode);

int vnodeSaveCreateMsgIntoQueue(SVnodeObj *pVnode, char *pMsg, int msgLen);

char *taosBuildRspMsgToMnodeWithSize(SMgmtObj *pObj, char type, int size);
char *taosBuildReqMsgToMnodeWithSize(SMgmtObj *pObj, char type, int size);
char *taosBuildRspMsgToMnode(SMgmtObj *pObj, char type);
char *taosBuildReqMsgToMnode(SMgmtObj *pObj, char type);
int   taosSendSimpleRspToMnode(SMgmtObj *pObj, char rsptype, char code);
int   taosSendMsgToMnode(SMgmtObj *pObj, char *msg, int msgLen);

void vnodeProcessMsgFromMgmt(char *content, int msgLen, int msgType, SMgmtObj *pObj) {
  if (msgType == TSDB_MSG_TYPE_CREATE) {
    vnodeProcessCreateMeterRequest(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_VPEERS) {
    vnodeProcessVPeersMsg(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_VPEER_CFG_RSP) {
    vnodeProcessVPeerCfgRsp(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_METER_CFG_RSP) {
    vnodeProcessMeterCfgRsp(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_REMOVE) {
    vnodeProcessRemoveMeterRequest(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_FREE_VNODE) {
    vnodeProcessFreeVnodeRequest(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_CFG_PNODE) {
    vnodeProcessCfgDnodeRequest(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_ALTER_STREAM) {
    vnodeProcessAlterStreamRequest(content, msgLen, pObj);
  } else if (msgType == TSDB_MSG_TYPE_GRANT_RSP) {
    // do nothing
  } else {
    dError("%s is not processed", taosMsg[msgType]);
  }
}

int vnodeProcessMeterCfgRsp(char *pMsg, int msgLen, SMgmtObj *pObj) {
  int code = *pMsg;

  if (code == 0) {
    vnodeProcessCreateMeterMsg(pMsg + 1, msgLen - 1);
  } else {
    STaosRsp *pRsp;
    pRsp = (STaosRsp *)pMsg;
    int32_t *pint = (int32_t *)pRsp->more;
    int      vnode = htonl(*pint);
    int      sid = htonl(*(pint + 1));
    dError("vid:%d, sid:%d, code:%d, meter is not configured, remove it", vnode, sid, code);
    int ret = vnodeRemoveMeterObj(vnode, sid);
    dTrace("vid:%d, sid:%d, meter delete ret:%d", vnode, sid, ret);
  }

  return 0;
}

int vnodeProcessCreateMeterRequest(char *pMsg, int msgLen, SMgmtObj *pObj) {
  SCreateMsg *pCreate;
  int         code = 0;
  int         vid;
  SVnodeObj * pVnode;

  pCreate = (SCreateMsg *)pMsg;
  vid = htons(pCreate->vnode);

  if (vid >= TSDB_MAX_VNODES) {
    dError("vid:%d, vnode is out of range", vid);
    code = TSDB_CODE_INVALID_VNODE_ID;
    goto _over;
  }

  pVnode = vnodeList + vid;
  if (pVnode->cfg.maxSessions <= 0) {
    dError("vid:%d, not activated", vid);
    code = TSDB_CODE_NOT_ACTIVE_VNODE;
    goto _over;
  }

  if (pVnode->syncStatus == TSDB_VN_SYNC_STATUS_SYNCING) {
    code = vnodeSaveCreateMsgIntoQueue(pVnode, pMsg, msgLen);
    dTrace("vid:%d, create msg is saved into sync queue", vid);
  } else {
    code = vnodeProcessCreateMeterMsg(pMsg, msgLen);
  }

_over:
  taosSendSimpleRspToMnode(pObj, TSDB_MSG_TYPE_CREATE_RSP, code);

  return code;
}

int vnodeProcessAlterStreamRequest(char *pMsg, int msgLen, SMgmtObj *pObj) {
  SAlterStreamMsg *pAlter;
  int              code = 0;
  int              vid, sid;
  SVnodeObj *      pVnode;

  pAlter = (SAlterStreamMsg *)pMsg;
  vid = htons(pAlter->vnode);
  sid = htonl(pAlter->sid);

  if (vid >= TSDB_MAX_VNODES) {
    dError("vid:%d, vnode is out of range", vid);
    code = TSDB_CODE_INVALID_VNODE_ID;
    goto _over;
  }

  pVnode = vnodeList + vid;
  if (pVnode->cfg.maxSessions <= 0 || pVnode->pCachePool == NULL) {
    dError("vid:%d is not activated yet", pAlter->vnode);
    code = TSDB_CODE_NOT_ACTIVE_VNODE;
    goto _over;
  }

  if (pAlter->sid >= pVnode->cfg.maxSessions || pAlter->sid < 0) {
    dError("vid:%d sid:%d uid:%" PRIu64 ", sid is out of range", pAlter->vnode, pAlter->sid, pAlter->uid);
    code = TSDB_CODE_INVALID_TABLE_ID;
    goto _over;
  }

  SMeterObj *pMeterObj = vnodeList[vid].meterList[sid];
  if (pMeterObj == NULL || sid != pMeterObj->sid || vid != pMeterObj->vnode) {
    dError("vid:%d sid:%d, not active table", vid, sid);
    code = TSDB_CODE_NOT_ACTIVE_TABLE;
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
  taosSendSimpleRspToMnode(pObj, TSDB_MSG_TYPE_ALTER_STREAM_RSP, code);

  return code;
}

int vnodeProcessCreateMeterMsg(char *pMsg, int msgLen) {
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
    code = TSDB_CODE_INVALID_VNODE_ID;
    goto _create_over;
  }

  SVnodeObj *pVnode = vnodeList + pCreate->vnode;
  if (pVnode->pCachePool == NULL) {
    dError("vid:%d is not activated yet", pCreate->vnode);
    vnodeSendVpeerCfgMsg(pCreate->vnode);
    code = TSDB_CODE_NOT_ACTIVE_VNODE;
    goto _create_over;
  }

  if (pCreate->sid >= pVnode->cfg.maxSessions || pCreate->sid < 0) {
    dError("vid:%d sid:%d id:%s, sid is out of range", pCreate->vnode, pCreate->sid, pCreate->meterId);
    code = TSDB_CODE_INVALID_TABLE_ID;
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

int vnodeProcessRemoveMeterRequest(char *pMsg, int msgLen, SMgmtObj *pMgmtObj) {
  SMeterObj *      pObj;
  SRemoveMeterMsg *pRemove;
  int              code = 0;

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
  taosSendSimpleRspToMnode(pMgmtObj, TSDB_MSG_TYPE_REMOVE_RSP, code);
  return 0;
}

int vnodeProcessVPeerCfg(char *msg, int msgLen, SMgmtObj *pMgmtObj) {
  SVPeersMsg *pMsg = (SVPeersMsg *)msg;
  int         i, vnode;

  vnode = htonl(pMsg->vnode);
  if (vnode >= TSDB_MAX_VNODES) {
    dError("vid:%d, vnode is out of range", vnode);
    return -1;
  }

  if (vnodeList[vnode].vnodeStatus == TSDB_VN_STATUS_CREATING) {
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

  if (pCfg->replications > 0) {
    dPrint("vid:%d, vpeer cfg received, replica:%d session:%d, vnodeList replica:%d session:%d, acct:%s db:%s",
        vnode, pCfg->replications, pCfg->maxSessions, vnodeList[vnode].cfg.replications, vnodeList[vnode].cfg.maxSessions,
        pCfg->acct, pCfg->db);
    for (i = 0; i < pCfg->replications; ++i) {
      pMsg->vpeerDesc[i].vnode = htonl(pMsg->vpeerDesc[i].vnode);
      pMsg->vpeerDesc[i].ip = htonl(pMsg->vpeerDesc[i].ip);
      dPrint("vid:%d, vpeer:%d ip:0x%x vid:%d ", vnode, i, pMsg->vpeerDesc[i].ip, pMsg->vpeerDesc[i].vnode);
    }
  }

  if (vnodeList[vnode].cfg.maxSessions == 0) {
    dPrint("vid:%d, vnode is empty", vnode);
    if (pCfg->maxSessions > 0) {
      if (vnodeList[vnode].vnodeStatus == TSDB_VN_STATUS_OFFLINE) {
        dPrint("vid:%d, status:%s, start to create vnode", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
        return vnodeCreateVnode(vnode, pCfg, pMsg->vpeerDesc);
      } else {
        dPrint("vid:%d, status:%s, cannot preform create vnode operation", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
        return TSDB_CODE_INVALID_VNODE_STATUS;
      }
    }
  } else {
    dPrint("vid:%d, vnode is not empty", vnode);
    if (pCfg->maxSessions > 0) {
      if (vnodeList[vnode].vnodeStatus == TSDB_VN_STATUS_DELETING) {
        dPrint("vid:%d, status:%s, wait vnode delete finished", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
      } else {
        dPrint("vid:%d, status:%s, start to update vnode", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));

        if (pCfg->maxSessions != vnodeList[vnode].cfg.maxSessions) {
          vnodeCleanUpOneVnode(vnode);
        }

        vnodeConfigVPeers(vnode, pCfg->replications, pMsg->vpeerDesc);
        vnodeSaveVnodeCfg(vnode, pCfg, pMsg->vpeerDesc);

        /*
        if (pCfg->maxSessions != vnodeList[vnode].cfg.maxSessions) {
          vnodeUpdateHeadFile(vnode, vnodeList[vnode].cfg.maxSessions, pCfg->maxSessions);
          vnodeList[vnode].cfg.maxSessions = pCfg->maxSessions;
          vnodeOpenVnode(vnode);
        }
        */
      }
      return 0;
    } else {
      dPrint("vid:%d, status:%s, start to delete vnode", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
      vnodeRemoveVnode(vnode);
    }
  }

  return 0;
}

int vnodeProcessVPeerCfgRsp(char *msg, int msgLen, SMgmtObj *pMgmtObj) {
  STaosRsp *pRsp;

  pRsp = (STaosRsp *)msg;

  if (pRsp->code == 0) {
    vnodeProcessVPeerCfg(pRsp->more, msgLen - sizeof(STaosRsp), pMgmtObj);
  } else {
    int32_t *pint = (int32_t *)pRsp->more;
    int      vnode = htonl(*pint);
    if (vnode < TSDB_MAX_VNODES && vnodeList[vnode].lastKey != 0) {
      dError("vnode:%d not configured, it shall be empty, code:%d", vnode, pRsp->code);
      vnodeRemoveVnode(vnode);
    } else {
      dError("vnode:%d is invalid, code:%d", vnode, pRsp->code);
    }
  }

  return 0;
}

int vnodeProcessVPeersMsg(char *msg, int msgLen, SMgmtObj *pMgmtObj) {
  int code = 0;

  code = vnodeProcessVPeerCfg(msg, msgLen, pMgmtObj);

  char *      pStart;
  STaosRsp *  pRsp;
  SVPeersMsg *pVPeersMsg = (SVPeersMsg *)msg;

  pStart = taosBuildRspMsgToMnode(pMgmtObj, TSDB_MSG_TYPE_VPEERS_RSP);
  if (pStart == NULL) return -1;

  pRsp = (STaosRsp *)pStart;
  pRsp->code = code;
  memcpy(pRsp->more, pVPeersMsg->cfg.db, TSDB_DB_NAME_LEN);

  msgLen = sizeof(STaosRsp) + TSDB_DB_NAME_LEN;
  taosSendMsgToMnode(pMgmtObj, pStart, msgLen);

  return code;
}

int vnodeProcessFreeVnodeRequest(char *pMsg, int msgLen, SMgmtObj *pMgmtObj) {
  SFreeVnodeMsg *pFree;

  pFree = (SFreeVnodeMsg *)pMsg;
  pFree->vnode = htons(pFree->vnode);

  if (pFree->vnode < 0 || pFree->vnode >= TSDB_MAX_VNODES) {
    dWarn("vid:%d, out of range", pFree->vnode);
    return -1;
  }

  dTrace("vid:%d, receive free vnode message", pFree->vnode);
  int32_t code = vnodeRemoveVnode(pFree->vnode);
  assert(code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS);

  taosSendSimpleRspToMnode(pMgmtObj, TSDB_MSG_TYPE_FREE_VNODE_RSP, code);
  return 0;
}

int vnodeProcessCfgDnodeRequest(char *cont, int contLen, SMgmtObj *pMgmtObj) {
  SCfgMsg *pCfg = (SCfgMsg *)cont;

  int code = tsCfgDynamicOptions(pCfg->config);

  taosSendSimpleRspToMnode(pMgmtObj, TSDB_MSG_TYPE_CFG_PNODE_RSP, code);

  return 0;
}

void vnodeSendVpeerCfgMsg(int vnode) {
  char *        pMsg, *pStart;
  int           msgLen;
  SVpeerCfgMsg *pCfg;
  SMgmtObj *    pObj = &mgmtObj;

  pStart = taosBuildReqMsgToMnode(pObj, TSDB_MSG_TYPE_VPEER_CFG);
  if (pStart == NULL) return;
  pMsg = pStart;

  pCfg = (SVpeerCfgMsg *)pMsg;
  pCfg->vnode = htonl(vnode);
  pMsg += sizeof(SVpeerCfgMsg);

  msgLen = pMsg - pStart;
  taosSendMsgToMnode(pObj, pStart, msgLen);
}

int vnodeSendMeterCfgMsg(int vnode, int sid) {
  char *        pMsg, *pStart;
  int           msgLen;
  SMeterCfgMsg *pCfg;
  SMgmtObj *    pObj = &mgmtObj;

  pStart = taosBuildReqMsgToMnode(pObj, TSDB_MSG_TYPE_METER_CFG);
  if (pStart == NULL) return -1;
  pMsg = pStart;

  pCfg = (SMeterCfgMsg *)pMsg;
  pCfg->vnode = htonl(vnode);
  pCfg->sid = htonl(sid);
  pMsg += sizeof(SMeterCfgMsg);

  msgLen = pMsg - pStart;
  return taosSendMsgToMnode(pObj, pStart, msgLen);
}
