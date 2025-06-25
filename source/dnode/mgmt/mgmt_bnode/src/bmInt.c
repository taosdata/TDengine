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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "bmInt.h"
#include "tjson.h"

static int32_t bmRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void bmInitOption(SBnodeMgmt *pMgmt, SBnodeOpt *pOption) {
  pOption->msgCb = pMgmt->msgCb;
  pOption->dnodeId = pMgmt->pData->dnodeId;
}

static void bmClose(SBnodeMgmt *pMgmt) {
  if (pMgmt->pBnode != NULL) {
    // bmStopWorker(pMgmt);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }

  taosMemoryFree(pMgmt);
}

static int32_t bndOpenWrapper(SBnodeOpt *pOption, SBnode **pBnode) {
  int32_t code = bndOpen(pOption, pBnode);
  return code;
}

int32_t bmPutMsgToQueue(SBnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  int32_t  code;
  SRpcMsg *pMsg;

  code = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen, (void **)&pMsg);
  if (code) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code = terrno;
  }

  SBnode *pBnode = pMgmt->pBnode;
  if (pBnode == NULL) {
    code = terrno;
    dError("msg:%p failed to put into bnode queue since %s, type:%s qtype:%d len:%d", pMsg, tstrerror(code),
           TMSG_INFO(pMsg->msgType), qtype, pRpc->contLen);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code;
  }

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = SNODE_HANDLE;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case WRITE_QUEUE:
      // code = bmPutNodeMsgToWriteQueue(pMgmt, pMsg);
      // break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return code;
  }
  return code;
}

static int32_t bmDecodeFile(SJson *pJson, int32_t *proto) {
  int32_t code = 0;

  code = tjsonGetIntValue(pJson, "proto", proto);
  return code;
}

static int32_t bmReadFile(const char *path, const char *name, int32_t *proto) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  int32_t   nBytes = snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("file:%s not exist", file);
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    code = terrno;
    dError("failed to read file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (bmDecodeFile(pJson, proto) < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read bnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read bnode file:%s since %s", file, tstrerror(code));
  }
  return code;
}

static int32_t bmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t     code = 0;
  SBnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SBnodeMgmt));
  if (pMgmt == NULL) {
    return terrno;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)bmPutMsgToQueue;
  pMgmt->msgCb.mgmt = pMgmt;

  SBnodeOpt option = {0};
  bmInitOption(pMgmt, &option);

  code = bmReadFile(pInput->path, pInput->name, &option.proto);
  if (code != 0) {
    dError("failed to read bnode since %s", tstrerror(code));
    bmClose(pMgmt);
    return code;
  }

  code = bndOpenWrapper(&option, &pMgmt->pBnode);
  if (code != 0) {
    dError("failed to open bnode since %s", tstrerror(code));
    bmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("bnode-impl", "initialized");

  /*
  if ((code = bmStartWorker(pMgmt)) != 0) {
    dError("failed to start bnode worker since %s", tstrerror(code));
    bmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("bnode-worker", "initialized");
  */
  pOutput->pMgmt = pMgmt;
  return code;
}

static int32_t bmEncodeFile(SJson *pJson, bool deployed, int32_t proto) {
  if (tjsonAddDoubleToObject(pJson, "deployed", deployed) < 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  if (tjsonAddIntegerToObject(pJson, "proto", proto) < 0) {
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  return 0;
}

static int32_t bmWriteFile(const char *path, const char *name, bool deployed, int32_t proto) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};

  int32_t nBytes = snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  nBytes = snprintf(realfile, sizeof(realfile), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  if ((code = bmEncodeFile(pJson, deployed, proto)) != 0) goto _OVER;

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno;
    goto _OVER;
  }

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) {
    code = terrno;
    goto _OVER;
  }
  if (taosFsyncFile(pFile) < 0) {
    code = terrno;
    goto _OVER;
  }

  if (taosCloseFile(&pFile) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _OVER;
  }
  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), NULL, _OVER);

  dInfo("succeed to write file:%s, deloyed:%d", realfile, deployed);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to write file:%s since %s, deloyed:%d", realfile, tstrerror(code), deployed);
  }
  return code;
}

int32_t bmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t          code = 0;
  SDCreateBnodeReq createReq = {0};
  if (tDeserializeSMCreateBnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to create bnode since %s", tstrerror(code));

    tFreeSMCreateBnodeReq(&createReq);
    return code;
  }

  bool deployed = true;
  if ((code = bmWriteFile(pInput->path, pInput->name, deployed, createReq.bnodeProto)) != 0) {
    dError("failed to write bnode file since %s", tstrerror(code));

    tFreeSMCreateBnodeReq(&createReq);
    return code;
  }

  tFreeSMCreateBnodeReq(&createReq);

  return 0;
}

int32_t bmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t        code = 0;
  SDDropBnodeReq dropReq = {0};
  if (tDeserializeSMDropBnodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;

    return code;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop bnode since %s", tstrerror(code));

    tFreeSMDropBnodeReq(&dropReq);
    return code;
  }

  bool deployed = false;
  if ((code = dmWriteFile(pInput->path, pInput->name, deployed)) != 0) {
    dError("failed to write bnode file since %s", tstrerror(code));

    tFreeSMDropBnodeReq(&dropReq);
    return code;
  }

  tFreeSMDropBnodeReq(&dropReq);

  return 0;
}

SArray *bmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}

SMgmtFunc bmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = bmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)bmClose;
  mgmtFunc.createFp = (NodeCreateFp)bmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)bmProcessDropReq;
  mgmtFunc.requiredFp = bmRequire;
  mgmtFunc.getHandlesFp = bmGetMsgHandles;

  return mgmtFunc;
}
