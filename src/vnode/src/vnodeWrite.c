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
#include "taoserror.h"
#include "tqueue.h"
#include "trpc.h"
#include "tutil.h"
#include "tsdb.h"
#include "twal.h"
#include "tdataformat.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "vnodeLog.h"
#include "tcq.h"

static int32_t (*vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *, void *, SRspRet *);
static int32_t vnodeProcessSubmitMsg(SVnodeObj *pVnode, void *pMsg, SRspRet *);
static int32_t vnodeProcessCreateTableMsg(SVnodeObj *pVnode, void *pMsg, SRspRet *);
static int32_t vnodeProcessDropTableMsg(SVnodeObj *pVnode, void *pMsg, SRspRet *);
static int32_t vnodeProcessAlterTableMsg(SVnodeObj *pVnode, void *pMsg, SRspRet *);
static int32_t vnodeProcessDropStableMsg(SVnodeObj *pVnode, void *pMsg, SRspRet *);
static int32_t vnodeProcessUpdateTagValMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet);

void vnodeInitWriteFp(void) {
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_SUBMIT]          = vnodeProcessSubmitMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessCreateTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = vnodeProcessDropTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = vnodeProcessAlterTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = vnodeProcessDropStableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL]  = vnodeProcessUpdateTagValMsg;
}

int32_t vnodeProcessWrite(void *param1, int qtype, void *param2, void *item) {
  int32_t    code = 0;
  SVnodeObj *pVnode = (SVnodeObj *)param1;
  SWalHead  *pHead = param2;

  if (vnodeProcessWriteMsgFp[pHead->msgType] == NULL) 
    return TSDB_CODE_MSG_NOT_PROCESSED; 

  if (pHead->version == 0) { // from client or CQ 
    if (pVnode->status != TAOS_VN_STATUS_READY) 
      return TSDB_CODE_INVALID_VGROUP_ID;  // it may be in deleting or closing state

    if (pVnode->syncCfg.replica > 1 && pVnode->role != TAOS_SYNC_ROLE_MASTER)
      return TSDB_CODE_NOT_READY;

    // assign version
    pVnode->version++;
    pHead->version = pVnode->version;
  } else { // from wal or forward 
    // for data from WAL or forward, version may be smaller
    if (pHead->version <= pVnode->version) return 0;
  }

  pVnode->version = pHead->version;

  // write into WAL
  code = walWrite(pVnode->wal, pHead);
  if (code < 0) return code;

  // forward to peers, even it is WAL/FWD, it shall be called to update version in sync 
  int32_t syncCode = 0;
  syncCode = syncForwardToPeer(pVnode->sync, pHead, item, qtype);
  if (syncCode < 0) return syncCode;

  // write data locally 
  code = (*vnodeProcessWriteMsgFp[pHead->msgType])(pVnode, pHead->cont, item);
  if (code < 0) return code;

  return syncCode;
}

static int32_t vnodeProcessSubmitMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  int32_t code = 0;

  // save insert result into item

  vTrace("vgId:%d, submit msg is processed", pVnode->vgId);
  
  pRet->len = sizeof(SShellSubmitRspMsg);
  pRet->rsp = rpcMallocCont(pRet->len);
  SShellSubmitRspMsg *pRsp = pRet->rsp;
  code = tsdbInsertData(pVnode->tsdb, pCont, pRsp);
  pRsp->numOfFailedBlocks = 0; //TODO
  //pRet->len += pRsp->numOfFailedBlocks * sizeof(SShellSubmitRspBlock); //TODO
  pRsp->code              = 0;
  pRsp->numOfRows         = htonl(1);
  
  return code;
}

static int32_t vnodeProcessCreateTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {

  STableCfg *pCfg = tsdbCreateTableCfgFromMsg((SMDCreateTableMsg *)pCont);
  if (pCfg == NULL) return terrno;
  int32_t code = tsdbCreateTable(pVnode->tsdb, pCfg);

  tsdbClearTableCfg(pCfg);
  return code;
}

static int32_t vnodeProcessDropTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  SMDDropTableMsg *pTable = pCont;
  int32_t code = 0;

  vTrace("vgId:%d, table:%s, start to drop", pVnode->vgId, pTable->tableId);
  STableId tableId = {
    .uid = htobe64(pTable->uid),
    .tid = htonl(pTable->sid)
  };

  code = tsdbDropTable(pVnode->tsdb, tableId);

  return code;
}

static int32_t vnodeProcessAlterTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  STableCfg *pCfg = tsdbCreateTableCfgFromMsg((SMDCreateTableMsg *)pCont);
  if (pCfg == NULL) return terrno;
  int32_t code = tsdbAlterTable(pVnode->tsdb, pCfg);
  tsdbClearTableCfg(pCfg);
  return code;
}

static int32_t vnodeProcessDropStableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  SMDDropSTableMsg *pTable = pCont;
  int32_t code = 0;

  vTrace("vgId:%d, stable:%s, start to drop", pVnode->vgId, pTable->tableId);
  
  STableId stableId = {
    .uid = htobe64(pTable->uid),
    .tid = -1
  };

  code = tsdbDropTable(pVnode->tsdb, stableId);
  
  vTrace("vgId:%d, stable:%s, drop stable result:%s", pVnode->vgId, pTable->tableId, tstrerror(code));
 
  return code;
}

static int32_t vnodeProcessUpdateTagValMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  return tsdbUpdateTagValue(pVnode->tsdb, (SUpdateTableTagValMsg *)pCont);
}

int vnodeWriteToQueue(void *param, void *data, int type) {
  SVnodeObj *pVnode = param;
  SWalHead *pHead = data;

  int size = sizeof(SWalHead) + pHead->len;
  SWalHead *pWal = (SWalHead *)taosAllocateQitem(size);
  memcpy(pWal, pHead, size);

  atomic_add_fetch_32(&pVnode->refCount, 1);
  taosWriteQitem(pVnode->wqueue, type, pWal);

  return 0;
}

