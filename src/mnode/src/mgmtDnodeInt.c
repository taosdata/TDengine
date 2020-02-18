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

#include "mnode.h"
#include "dnode.h"
#include "mgmtDnodeInt.h"
#include "mgmtBalance.h"
#include "mgmtDnode.h"
#include "mgmtDb.h"
#include "mgmtVgroup.h"
#include "mgmtTable.h"
#include "tutil.h"
#include "tstatus.h"
#include "tsystem.h"
#include "tsched.h"

#include "dnodeSystem.h"


#include "mgmtChildTable.h"
#include "mgmtNormalTable.h"
#include "mgmtStreamTable.h"

void  mgmtProcessMsgFromDnode(char *content, int msgLen, int msgType, SDnodeObj *pObj);
int   mgmtSendVPeersMsg(SVgObj *pVgroup);
char *mgmtBuildVpeersIe(char *pMsg, SVgObj *pVgroup, int vnode);
char *mgmtBuildCreateMeterIe(STabObj *pTable, char *pMsg, int vnode);


int mgmtProcessMeterCfgMsg(char *cont, int contLen, SDnodeObj *pObj) {
  char *        pMsg, *pStart;
  int           msgLen = 0;
  STabObj *     pTable = NULL;
  SMeterCfgMsg *pCfg = (SMeterCfgMsg *)cont;
  SVgObj *      pVgroup;

  if (!sdbMaster) {
    taosSendSimpleRspToDnode(pObj, TSDB_MSG_TYPE_TABLE_CFG_RSP, TSDB_CODE_REDIRECT);
    return 0;
  }

  int vnode = htonl(pCfg->vnode);
  int sid = htonl(pCfg->sid);

  pStart = taosBuildRspMsgToDnodeWithSize(pObj, TSDB_MSG_TYPE_TABLE_CFG_RSP, 64000);
  if (pStart == NULL) {
    taosSendSimpleRspToDnode(pObj, TSDB_MSG_TYPE_TABLE_CFG_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }

  pMsg = pStart;

  if (vnode < pObj->numOfVnodes) {
    int vgId = pObj->vload[vnode].vgId;

    pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup) pTable = pVgroup->meterList[sid];
  }

  if (pTable) {
    *pMsg = 0;  // code
    pMsg++;
    pMsg = mgmtBuildCreateMeterIe(pTable, pMsg, vnode);
  } else {
    mTrace("dnode:%s, vnode:%d sid:%d, meter not there", taosIpStr(pObj->privateIp), vnode, sid);
    *pMsg = TSDB_CODE_INVALID_METER_ID;
    pMsg++;

    *(int32_t *)pMsg = htonl(vnode);
    pMsg += sizeof(int32_t);
    *(int32_t *)pMsg = htonl(sid);
    pMsg += sizeof(int32_t);
  }

  msgLen = pMsg - pStart;
  taosSendMsgToDnode(pObj, pStart, msgLen);

  return 0;
}

int mgmtProcessVpeerCfgMsg(char *cont, int contLen, SDnodeObj *pObj) {
  char *        pMsg, *pStart;
  int           msgLen = 0;
  SVpeerCfgMsg *pCfg = (SVpeerCfgMsg *)cont;
  SVgObj *      pVgroup = NULL;

  if (!sdbMaster) {
    taosSendSimpleRspToDnode(pObj, TSDB_MSG_TYPE_VNODE_CFG_RSP, TSDB_CODE_REDIRECT);
    return 0;
  }

  int vnode = htonl(pCfg->vnode);

  pStart = taosBuildRspMsgToDnode(pObj, TSDB_MSG_TYPE_VNODE_CFG_RSP);
  if (pStart == NULL) {
    taosSendSimpleRspToDnode(pObj, TSDB_MSG_TYPE_VNODE_CFG_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }
  pMsg = pStart;

  if (vnode < pObj->numOfVnodes) pVgroup = mgmtGetVgroup(pObj->vload[vnode].vgId);

  if (pVgroup) {
    *pMsg = 0;
    pMsg++;
    pMsg = mgmtBuildVpeersIe(pMsg, pVgroup, vnode);
    mTrace("dnode:%s, vnode:%d, vgroup:%d, send create vnode msg, code:%d", taosIpStr(pObj->privateIp), vnode, pVgroup->vgId, *pMsg);
  } else {
    mTrace("dnode:%s, vnode:%d, no vgroup info, vgroup:%d", taosIpStr(pObj->privateIp), vnode, pObj->vload[vnode].vgId);
    *pMsg = TSDB_CODE_NOT_ACTIVE_VNODE;
    pMsg++;
    *(int32_t *)pMsg = htonl(vnode);
    pMsg += sizeof(int32_t);
  }

  msgLen = pMsg - pStart;
  taosSendMsgToDnode(pObj, pStart, msgLen);

  return 0;
}

int mgmtProcessCreateRsp(char *msg, int msgLen, SDnodeObj *pObj) { return 0; }

int mgmtProcessFreeVnodeRsp(char *msg, int msgLen, SDnodeObj *pObj) { return 0; }

int mgmtProcessVPeersRsp(char *msg, int msgLen, SDnodeObj *pObj) {
  STaosRsp *pRsp = (STaosRsp *)msg;

  if (!sdbMaster) {
    taosSendSimpleRspToDnode(pObj, TSDB_MSG_TYPE_DNODE_VPEERS_RSP, TSDB_CODE_REDIRECT);
    return 0;
  }

  SDbObj *pDb = mgmtGetDb(pRsp->more);
  if (!pDb) {
    mError("dnode:%s, db:%s not find, code:%d", taosIpStr(pObj->privateIp), pRsp->more, pRsp->code);
    return 0;
  }

  if (pDb->vgStatus != TSDB_VG_STATUS_IN_PROGRESS) {
    mTrace("dnode:%s, db:%s vpeer rsp already disposed, vgroup status:%s code:%d",
            taosIpStr(pObj->privateIp), pRsp->more, taosGetVgroupStatusStr(pDb->vgStatus), pRsp->code);
    return 0;
  }

  if (pRsp->code == TSDB_CODE_SUCCESS) {
    pDb->vgStatus = TSDB_VG_STATUS_READY;
    mTrace("dnode:%s, db:%s vgroup is created in dnode", taosIpStr(pObj->privateIp), pRsp->more);
    return 0;
  }

  pDb->vgStatus = pRsp->code;
  mError("dnode:%s, db:%s vgroup init failed, code:%d %s",
          taosIpStr(pObj->privateIp), pRsp->more, pRsp->code, taosGetVgroupStatusStr(pDb->vgStatus));

  return 0;
}

void mgmtProcessMsgFromDnode(char *content, int msgLen, int msgType, SDnodeObj *pObj) {
  if (msgType == TSDB_MSG_TYPE_TABLE_CFG) {
    mgmtProcessMeterCfgMsg(content, msgLen - sizeof(SIntMsg), pObj);
  } else if (msgType == TSDB_MSG_TYPE_VNODE_CFG) {
    mgmtProcessVpeerCfgMsg(content, msgLen - sizeof(SIntMsg), pObj);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE_RSP) {
    mgmtProcessCreateRsp(content, msgLen - sizeof(SIntMsg), pObj);
  } else if (msgType == TSDB_MSG_TYPE_REMOVE_RSP) {
    // do nothing
  } else if (msgType == TSDB_MSG_TYPE_DNODE_VPEERS_RSP) {
    mgmtProcessVPeersRsp(content, msgLen - sizeof(SIntMsg), pObj);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_FREE_VNODE_RSP) {
    mgmtProcessFreeVnodeRsp(content, msgLen - sizeof(SIntMsg), pObj);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_CFG_RSP) {
    // do nothing;
  } else if (msgType == TSDB_MSG_TYPE_ALTER_STREAM_RSP) {
    // do nothing;
  } else {
    mError("%s from dnode is not processed", taosMsg[msgType]);
  }
}

char *mgmtBuildCreateMeterIe(STabObj *pTable, char *pMsg, int vnode) {
  SCreateMsg *pCreateMeter;

  pCreateMeter = (SCreateMsg *)pMsg;
  pCreateMeter->vnode = htons(vnode);
  pCreateMeter->sid = htonl(pTable->gid.sid);
  pCreateMeter->uid = pTable->uid;
  memcpy(pCreateMeter->meterId, pTable->meterId, TSDB_TABLE_ID_LEN);

  // pCreateMeter->lastCreate = htobe64(pVgroup->lastCreate);
  pCreateMeter->timeStamp = htobe64(pTable->createdTime);
  /*
      pCreateMeter->spi = pSec->spi;
      pCreateMeter->encrypt = pSec->encrypt;
      memcpy(pCreateMeter->cipheringKey, pSec->cipheringKey, TSDB_KEY_LEN);
      memcpy(pCreateMeter->secret, pSec->secret, TSDB_KEY_LEN);
  */
  pCreateMeter->sversion = htonl(pTable->sversion);
  pCreateMeter->numOfColumns = htons(pTable->numOfColumns);
  SSchema *pSchema = mgmtGetTableSchema(pTable);

  for (int i = 0; i < pTable->numOfColumns; ++i) {
    pCreateMeter->schema[i].type = pSchema[i].type;
    /* strcpy(pCreateMeter->schema[i].name, pColumnModel[i].name); */
    pCreateMeter->schema[i].bytes = htons(pSchema[i].bytes);
    pCreateMeter->schema[i].colId = htons(pSchema[i].colId);
  }

  pMsg = ((char *)(pCreateMeter->schema)) + pTable->numOfColumns * sizeof(SMColumn);
  pCreateMeter->sqlLen = 0;

  if (pTable->pSql) {
    int len = strlen(pTable->pSql) + 1;
    pCreateMeter->sqlLen = htons(len);
    strcpy(pMsg, pTable->pSql);
    pMsg += len;
  }

  return pMsg;
}

int32_t mgmtSendCreateChildTableMsg(SChildTableObj *pTable, SVgObj *pVgroup, int32_t tagDataLen, int8_t *pTagData) {
  uint64_t timeStamp = taosGetTimestampMs();

  for (int32_t index = 0; index < pVgroup->numOfVnodes; ++index) {
    SDnodeObj *pObj = mgmtGetDnode(pVgroup->vnodeGid[index].ip);
    if (pObj == NULL) {
      continue;
    }

    int8_t *pStart = taosBuildReqMsgToDnodeWithSize(pObj, TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE, 64000);
    if (pStart == NULL) {
      continue;
    }

    int8_t *pMsg = mgmtBuildCreateChildTableMsg(pTable, pStart, pVgroup->vnodeGid[index].vnode, tagDataLen, pTagData);
    int32_t msgLen = pMsg - pStart;

    taosSendMsgToDnode(pObj, pStart, msgLen);
  }

  pVgroup->lastCreate = timeStamp;
  return 0;
}

int32_t mgmtSendCreateStreamTableMsg(SStreamTableObj *pTable, SVgObj *pVgroup) {
  uint64_t timeStamp = taosGetTimestampMs();

  for (int32_t index = 0; index < pVgroup->numOfVnodes; ++index) {
    SDnodeObj *pObj = mgmtGetDnode(pVgroup->vnodeGid[index].ip);
    if (pObj == NULL) {
      continue;
    }

    int8_t *pStart = taosBuildReqMsgToDnodeWithSize(pObj, TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE, 64000);
    if (pStart == NULL) {
      continue;
    }

    int8_t *pMsg = mgmtBuildCreateStreamTableMsg(pTable, pStart, pVgroup->vnodeGid[index].vnode);
    int32_t msgLen = pMsg - pStart;

    taosSendMsgToDnode(pObj, pStart, msgLen);
  }

  pVgroup->lastCreate = timeStamp;
  return 0;
}

int32_t mgmtSendCreateNormalTableMsg(SNormalTableObj *pTable, SVgObj *pVgroup) {
  uint64_t timeStamp = taosGetTimestampMs();

  for (int32_t index = 0; index < pVgroup->numOfVnodes; ++index) {
    SDnodeObj *pObj = mgmtGetDnode(pVgroup->vnodeGid[index].ip);
    if (pObj == NULL) {
      continue;
    }

    int8_t *pStart = taosBuildReqMsgToDnodeWithSize(pObj, TSDB_MSG_TYPE_DNODE_CREATE_CHILD_TABLE, 64000);
    if (pStart == NULL) {
      continue;
    }

    int8_t *pMsg = mgmtBuildCreateNormalTableMsg(pTable, pStart, pVgroup->vnodeGid[index].vnode);
    int32_t msgLen = pMsg - pStart;

    taosSendMsgToDnode(pObj, pStart, msgLen);
  }

  pVgroup->lastCreate = timeStamp;
  return 0;
}

int mgmtSendRemoveMeterMsgToDnode(STabObj *pTable, SVgObj *pVgroup) {
  SRemoveMeterMsg *pRemove;
  char *           pMsg, *pStart;
  int              i, msgLen = 0;
  SDnodeObj *      pObj;
  char             ipstr[20];
  uint64_t         timeStamp;

  timeStamp = taosGetTimestampMs();

  for (i = 0; i < pVgroup->numOfVnodes; ++i) {
    //if (pVgroup->vnodeGid[i].ip == 0) continue;

    pObj = mgmtGetDnode(pVgroup->vnodeGid[i].ip);
    if (pObj == NULL) continue;

    pStart = taosBuildReqMsgToDnode(pObj, TSDB_MSG_TYPE_DNODE_REMOVE_CHILD_TABLE);
    if (pStart == NULL) continue;
    pMsg = pStart;

    pRemove = (SRemoveMeterMsg *)pMsg;
    pRemove->vnode = htons(pVgroup->vnodeGid[i].vnode);
    pRemove->sid = htonl(pTable->gid.sid);
    memcpy(pRemove->meterId, pTable->meterId, TSDB_TABLE_ID_LEN);

    pMsg += sizeof(SRemoveMeterMsg);
    msgLen = pMsg - pStart;

    taosSendMsgToDnode(pObj, pStart, msgLen);

    tinet_ntoa(ipstr, pVgroup->vnodeGid[i].ip);
    mTrace("dnode:%s vid:%d, send remove meter msg, sid:%d status:%d", ipstr, pVgroup->vnodeGid[i].vnode,
           pTable->gid.sid, pObj->status);
  }

  pVgroup->lastRemove = timeStamp;

  return 0;
}

int mgmtSendAlterStreamMsgToDnode(STabObj *pTable, SVgObj *pVgroup) {
  SAlterStreamMsg *pAlter;
  char *           pMsg, *pStart;
  int              i, msgLen = 0;
  SDnodeObj *      pObj;

  for (i = 0; i < pVgroup->numOfVnodes; ++i) {
    if (pVgroup->vnodeGid[i].ip == 0) continue;

    pObj = mgmtGetDnode(pVgroup->vnodeGid[i].ip);
    if (pObj == NULL) continue;

    pStart = taosBuildReqMsgToDnode(pObj, TSDB_MSG_TYPE_ALTER_STREAM);
    if (pStart == NULL) continue;
    pMsg = pStart;

    pAlter = (SAlterStreamMsg *)pMsg;
    pAlter->vnode = htons(pVgroup->vnodeGid[i].vnode);
    pAlter->sid = htonl(pTable->gid.sid);
    pAlter->uid = pTable->uid;
    pAlter->status = pTable->status;

    pMsg += sizeof(SAlterStreamMsg);
    msgLen = pMsg - pStart;

    taosSendMsgToDnode(pObj, pStart, msgLen);
  }

  return 0;
}

char *mgmtBuildVpeersIe(char *pMsg, SVgObj *pVgroup, int vnode) {
  SVPeersMsg *pVPeers = (SVPeersMsg *)pMsg;
  SDbObj *    pDb;

  pDb = mgmtGetDb(pVgroup->dbName);
  pVPeers->vnode = htonl(vnode);

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
  pCfg->replications = (char)pVgroup->numOfVnodes;
  pCfg->rowsInFileBlock = htonl(pCfg->rowsInFileBlock);

  SVPeerDesc *vpeerDesc = pVPeers->vpeerDesc;

  pMsg = (char *)(pVPeers->vpeerDesc);

  for (int j = 0; j < pVgroup->numOfVnodes; ++j) {
    vpeerDesc[j].ip = htonl(pVgroup->vnodeGid[j].ip);
    vpeerDesc[j].vnode = htonl(pVgroup->vnodeGid[j].vnode);
    pMsg += sizeof(SVPeerDesc);
  }

  return pMsg;
}

int mgmtSendVPeersMsg(SVgObj *pVgroup) {
  SDnodeObj *pDnode;
  char *     pMsg, *pStart;
  int        msgLen = 0;

  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    pDnode = mgmtGetDnode(pVgroup->vnodeGid[i].ip);
    if (pDnode == NULL) {
      mError("dnode:%s not there", taosIpStr(pVgroup->vnodeGid[i].ip));
      continue;
    }

    pDnode->vload[pVgroup->vnodeGid[i].vnode].vgId = pVgroup->vgId;
    mgmtUpdateDnode(pDnode);

    if (pDnode->thandle && pVgroup->numOfVnodes >= 1) {
      pStart = taosBuildReqMsgToDnode(pDnode, TSDB_MSG_TYPE_DNODE_VPEERS);
      if (pStart == NULL) continue;
      pMsg = mgmtBuildVpeersIe(pStart, pVgroup, pVgroup->vnodeGid[i].vnode);
      msgLen = pMsg - pStart;

      taosSendMsgToDnode(pDnode, pStart, msgLen);
    }
  }

  return 0;
}

int mgmtSendOneFreeVnodeMsg(SVnodeGid *pVnodeGid) {
  SFreeVnodeMsg *pFreeVnode;
  char *         pMsg, *pStart;
  int            msgLen = 0;
  SDnodeObj *    pDnode;

  pDnode = mgmtGetDnode(pVnodeGid->ip);
  if (pDnode == NULL) {
    mError("dnode:%s not there", taosIpStr(pVnodeGid->ip));
    return -1;
  }

  if (pDnode->thandle == NULL) {
    mTrace("dnode:%s offline, failed to send Vpeer msg", taosIpStr(pVnodeGid->ip));
    return -1;
  }

  pStart = taosBuildReqMsgToDnode(pDnode, TSDB_MSG_TYPE_DNODE_FREE_VNODE);
  if (pStart == NULL) return -1;
  pMsg = pStart;

  pFreeVnode = (SFreeVnodeMsg *)pMsg;
  pFreeVnode->vnode = htons(pVnodeGid->vnode);

  pMsg += sizeof(SFreeVnodeMsg);

  msgLen = pMsg - pStart;
  taosSendMsgToDnode(pDnode, pStart, msgLen);

  return 0;
}

int mgmtSendFreeVnodeMsg(SVgObj *pVgroup) {
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    mgmtSendOneFreeVnodeMsg(pVgroup->vnodeGid + i);
  }

  return 0;
}

int mgmtCfgDynamicOptions(SDnodeObj *pDnode, char *msg) {
  char *option, *value;
  int   olen, valen;

  paGetToken(msg, &option, &olen);
  if (strncasecmp(option, "unremove", 8) == 0) {
    mgmtSetDnodeUnRemove(pDnode);
    return TSDB_CODE_SUCCESS;
  } else if (strncasecmp(option, "score", 5) == 0) {
    paGetToken(option + olen + 1, &value, &valen);
    if (valen > 0) {
      int score = atoi(value);
      mTrace("dnode:%s, custom score set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->customScore, score);
      pDnode->customScore = score;
      mgmtUpdateDnode(pDnode);
      mgmtStartBalanceTimer(15);
    }
    return TSDB_CODE_INVALID_SQL;
  } else if (strncasecmp(option, "bandwidth", 9) == 0) {
    paGetToken(msg, &value, &valen);
    if (valen > 0) {
      int bandwidthMb = atoi(value);
      if (bandwidthMb >= 0 && bandwidthMb < 10000000) {
        mTrace("dnode:%s, bandwidth(Mb) set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->bandwidthMb, bandwidthMb);
        pDnode->bandwidthMb = bandwidthMb;
        mgmtUpdateDnode(pDnode);
        return TSDB_CODE_SUCCESS;
      }
    }
    return TSDB_CODE_INVALID_SQL;
  }

  return -1;
}

int mgmtSendCfgDnodeMsg(char *cont) {
#ifdef CLUSTER
  char *     pMsg, *pStart;
  int        msgLen = 0;
#endif

  SDnodeObj *pDnode;
  SCfgMsg *  pCfg = (SCfgMsg *)cont;
  uint32_t   ip;

  ip = inet_addr(pCfg->ip);
  pDnode = mgmtGetDnode(ip);
  if (pDnode == NULL) {
    mError("dnode ip:%s not configured", pCfg->ip);
    return TSDB_CODE_NOT_CONFIGURED;
  }

  mTrace("dnode:%s, dynamic option received, content:%s", taosIpStr(pDnode->privateIp), pCfg->config);
  int code = mgmtCfgDynamicOptions(pDnode, pCfg->config);
  if (code != -1) {
    return code;
  }

#ifdef CLUSTER
  pStart = taosBuildReqMsg(pDnode->thandle, TSDB_MSG_TYPE_DNODE_CFG);
  if (pStart == NULL) return TSDB_CODE_NODE_OFFLINE;
  pMsg = pStart;

  memcpy(pMsg, cont, sizeof(SCfgMsg));
  pMsg += sizeof(SCfgMsg);

  msgLen = pMsg - pStart;
  taosSendMsgToDnode(pDnode, pStart, msgLen);
#else
  (void)tsCfgDynamicOptions(pCfg->config);
#endif
  return 0;
}


/*
 * functions for communicate between dnode and mnode
 */

extern void *dmQhandle;
void * mgmtStatusTimer = NULL;
void   mgmtProcessMsgFromDnode(char *content, int msgLen, int msgType, SDnodeObj *pObj);


char* taosBuildRspMsgToDnodeWithSizeImp(SDnodeObj *pObj, char type, int32_t size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}
char* (*taosBuildRspMsgToDnodeWithSize)(SDnodeObj *pObj, char type, int32_t size) = taosBuildRspMsgToDnodeWithSizeImp;

char* taosBuildReqMsgToDnodeWithSizeImp(SDnodeObj *pObj, char type, int32_t size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}
char* (*taosBuildReqMsgToDnodeWithSize)(SDnodeObj *pObj, char type, int32_t size) = taosBuildReqMsgToDnodeWithSizeImp;

char *taosBuildRspMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildRspMsgToDnodeWithSize(pObj, type, 256);
}

char *taosBuildReqMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildReqMsgToDnodeWithSize(pObj, type, 256);
}

int32_t taosSendSimpleRspToDnodeImp(SDnodeObj *pObj, char rsptype, char code) { return 0; }
int32_t (*taosSendSimpleRspToDnode)(SDnodeObj *pObj, char rsptype, char code) = taosSendSimpleRspToDnodeImp;

int32_t taosSendMsgToDnodeImp(SDnodeObj *pObj, char *msg, int32_t msgLen) {
  mTrace("msg:%s is sent to dnode", taosMsg[(uint8_t)(*(msg-1))]);

  /*
   * Lite version has no message header, so minus one
   */
  SSchedMsg schedMsg;
  schedMsg.fp = dnodeProcessMsgFromMgmtImp;
  schedMsg.msg = msg - 1;
  schedMsg.ahandle = NULL;
  schedMsg.thandle = NULL;
  taosScheduleTask(dmQhandle, &schedMsg);

  return 0;
}
int32_t (*taosSendMsgToDnode)(SDnodeObj *pObj, char *msg, int msgLen) = taosSendMsgToDnodeImp;


int32_t mgmtInitDnodeIntImp() { return 0; }
int32_t (*mgmtInitDnodeInt)() = mgmtInitDnodeIntImp;

void mgmtCleanUpDnodeIntImp() {}
void (*mgmtCleanUpDnodeInt)() = mgmtCleanUpDnodeIntImp;

void mgmtProcessDnodeStatusImp(void *handle, void *tmrId) {
/*
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
*/
}
void (*mgmtProcessDnodeStatus)(void *handle, void *tmrId) = mgmtProcessDnodeStatusImp;

void mgmtProcessMsgFromDnodeSpecImp(SSchedMsg *sched) {
  char  msgType = *sched->msg;
  char *content = sched->msg + 1;
  mTrace("msg:%s is received from dnode", taosMsg[(uint8_t)msgType]);

  mgmtProcessMsgFromDnode(content, 0, msgType, mgmtGetDnode(0));
  free(sched->msg);
}
void (*mgmtProcessMsgFromDnodeSpec)(SSchedMsg *sched) = mgmtProcessMsgFromDnodeSpecImp;
