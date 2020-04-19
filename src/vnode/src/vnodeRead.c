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
#include "tsdb.h"
#include "twal.h"
#include "tdataformat.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "vnodeLog.h"
#include "query.h"

static int32_t (*vnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *, void *pCont, int32_t contLen, SRspRet *pRet);
static int32_t  vnodeProcessQueryMsg(SVnodeObj *pVnode, void *pCont, int32_t contLen, SRspRet *pRet);
static int32_t  vnodeProcessRetrieveMsg(SVnodeObj *pVnode, void *pCont, int32_t contLen, SRspRet *pRet);

void vnodeInitReadFp(void) {
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_QUERY]    = vnodeProcessQueryMsg;
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_RETRIEVE] = vnodeProcessRetrieveMsg;
}

int32_t vnodeProcessRead(void *param, int msgType, void *pCont, int32_t contLen, SRspRet *ret) {
  SVnodeObj *pVnode = (SVnodeObj *)param;

  if (vnodeProcessReadMsgFp[msgType] == NULL) 
    return TSDB_CODE_MSG_NOT_PROCESSED; 

  if (pVnode->status == TAOS_VN_STATUS_DELETING || pVnode->status == TAOS_VN_STATUS_CLOSING) 
    return TSDB_CODE_NOT_ACTIVE_VNODE; 

  return (*vnodeProcessReadMsgFp[msgType])(pVnode, pCont, contLen, ret);
}

static int32_t vnodeProcessQueryMsg(SVnodeObj *pVnode, void *pCont, int32_t contLen, SRspRet *pRet) {
  SQueryTableMsg* pQueryTableMsg = (SQueryTableMsg*) pCont;
  memset(pRet, 0, sizeof(SRspRet));

  int32_t code = TSDB_CODE_SUCCESS;
  
  qinfo_t pQInfo = NULL;
  if (contLen != 0) {
    pRet->code = qCreateQueryInfo(pVnode->tsdb, pQueryTableMsg, &pQInfo);
  
    SQueryTableRsp *pRsp = (SQueryTableRsp *) rpcMallocCont(sizeof(SQueryTableRsp));
    pRsp->qhandle = htobe64((uint64_t) (pQInfo));
    pRsp->code = pRet->code;
     
    pRet->len = sizeof(SQueryTableRsp);
    pRet->rsp = pRsp;
    
    dTrace("pVnode:%p vgId:%d QInfo:%p, dnode query msg disposed", pVnode, pVnode->vgId, pQInfo);
  } else {
    pQInfo = pCont;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  qTableQuery(pQInfo); // do execute query
  
  return code;
}

static int32_t vnodeProcessRetrieveMsg(SVnodeObj *pVnode, void *pCont, int32_t contLen, SRspRet *pRet) {
  SRetrieveTableMsg *pRetrieve = pCont;
  void *pQInfo = (void*) htobe64(pRetrieve->qhandle);
  memset(pRet, 0, sizeof(SRspRet));

  int32_t code = TSDB_CODE_SUCCESS;

  dTrace("pVnode:%p vgId:%d QInfo:%p, retrieve msg is received", pVnode, pVnode->vgId, pQInfo);
  
  pRet->code = qRetrieveQueryResultInfo(pQInfo);
  if (pRet->code != TSDB_CODE_SUCCESS) {
    //TODO
    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
  } else {
    // todo check code and handle error in build result set
    pRet->code = qDumpRetrieveResult(pQInfo, (SRetrieveTableRsp **)&pRet->rsp, &pRet->len);

    if (qHasMoreResultsToRetrieve(pQInfo)) {
      pRet->qhandle = pQInfo;
      code = TSDB_CODE_ACTION_NEED_REPROCESSED;
    } else {  
      // no further execution invoked, release the ref to vnode
      qDestroyQueryInfo(pQInfo);
      vnodeRelease(pVnode);
    }
  }
  
  dTrace("pVnode:%p vgId:%d QInfo:%p, retrieve msg is disposed", pVnode, pVnode->vgId, pQInfo);
  return code;
}
