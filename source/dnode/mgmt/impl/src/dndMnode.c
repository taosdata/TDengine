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
#include "dndMnode.h"
#include "dndDnode.h"
#include "dndTransport.h"

static int32_t dndInitMnodeReadWorker(SDnode *pDnode);
static int32_t dndInitMnodeWriteWorker(SDnode *pDnode);
static int32_t dndInitMnodeSyncWorker(SDnode *pDnode);
static int32_t dndInitMnodeMgmtWorker(SDnode *pDnode);
static void    dndCleanupMnodeReadWorker(SDnode *pDnode);
static void    dndCleanupMnodeWriteWorker(SDnode *pDnode);
static void    dndCleanupMnodeSyncWorker(SDnode *pDnode);
static void    dndCleanupMnodeMgmtWorker(SDnode *pDnode);
static int32_t dndAllocMnodeReadQueue(SDnode *pDnode);
static int32_t dndAllocMnodeWriteQueue(SDnode *pDnode);
static int32_t dndAllocMnodeApplyQueue(SDnode *pDnode);
static int32_t dndAllocMnodeSyncQueue(SDnode *pDnode);
static int32_t dndAllocMnodeMgmtQueue(SDnode *pDnode);
static void    dndFreeMnodeReadQueue(SDnode *pDnode);
static void    dndFreeMnodeWriteQueue(SDnode *pDnode);
static void    dndFreeMnodeApplyQueue(SDnode *pDnode);
static void    dndFreeMnodeSyncQueue(SDnode *pDnode);
static void    dndFreeMnodeMgmtQueue(SDnode *pDnode);

static void    dndProcessMnodeReadQueue(SDnode *pDnode, SMnodeMsg *pMsg);
static void    dndProcessMnodeWriteQueue(SDnode *pDnode, SMnodeMsg *pMsg);
static void    dndProcessMnodeApplyQueue(SDnode *pDnode, SMnodeMsg *pMsg);
static void    dndProcessMnodeSyncQueue(SDnode *pDnode, SMnodeMsg *pMsg);
static void    dndProcessMnodeMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg);
static int32_t dndWriteMnodeMsgToQueue(SMnode *pMnode, taos_queue pQueue, SRpcMsg *pRpcMsg);
void           dndProcessMnodeReadMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessMnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessMnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void           dndProcessMnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t dndPutMsgIntoMnodeApplyQueue(SDnode *pDnode, SMnodeMsg *pMsg);

static int32_t dndStartMnodeWorker(SDnode *pDnode);
static void    dndStopMnodeWorker(SDnode *pDnode);

static SMnode *dndAcquireMnode(SDnode *pDnode);
static void    dndReleaseMnode(SDnode *pDnode, SMnode *pMnode);

static int32_t dndReadMnodeFile(SDnode *pDnode);
static int32_t dndWriteMnodeFile(SDnode *pDnode);

static int32_t dndOpenMnode(SDnode *pDnode, SMnodeOpt *pOption);
static int32_t dndAlterMnode(SDnode *pDnode, SMnodeOpt *pOption);
static int32_t dndDropMnode(SDnode *pDnode);

static int32_t dndProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
static int32_t dndProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
static int32_t dndProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

static SMnode *dndAcquireMnode(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pMnode = pMgmt->pMnode;
  } else {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pMnode != NULL) {
    dTrace("acquire mnode, refCount:%d", refCount);
  }
  return pMnode;
}

static void dndReleaseMnode(SDnode *pDnode, SMnode *pMnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMnode != NULL) {
    refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pMnode != NULL) {
    dTrace("release mnode, refCount:%d", refCount);
  }
}

static int32_t dndReadMnodeFile(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  int32_t     code = TSDB_CODE_DND_MNODE_READ_FILE_ERROR;
  int32_t     len = 0;
  int32_t     maxLen = 4096;
  char       *content = calloc(1, maxLen + 1);
  cJSON      *root = NULL;

  FILE *fp = fopen(pMgmt->file, "r");
  if (fp == NULL) {
    dDebug("file %s not exist", pMgmt->file);
    code = 0;
    goto PRASE_MNODE_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_String) {
    dError("failed to read %s since deployed not found", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }
  pMgmt->deployed = atoi(deployed->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_String) {
    dError("failed to read %s since dropped not found", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }
  pMgmt->dropped = atoi(dropped->valuestring);

  cJSON *nodes = cJSON_GetObjectItem(root, "nodes");
  if (!nodes || nodes->type != cJSON_Array) {
    dError("failed to read %s since nodes not found", pMgmt->file);
    goto PRASE_MNODE_OVER;
  }

  pMgmt->replica = cJSON_GetArraySize(nodes);
  if (pMgmt->replica <= 0 || pMgmt->replica > TSDB_MAX_REPLICA) {
    dError("failed to read %s since nodes size %d invalid", pMgmt->file, pMgmt->replica);
    goto PRASE_MNODE_OVER;
  }

  for (int32_t i = 0; i < pMgmt->replica; ++i) {
    cJSON *node = cJSON_GetArrayItem(nodes, i);
    if (node == NULL) break;

    SReplica *pReplica = &pMgmt->replicas[i];

    cJSON *id = cJSON_GetObjectItem(node, "id");
    if (!id || id->type != cJSON_String || id->valuestring == NULL) {
      dError("failed to read %s since id not found", pMgmt->file);
      goto PRASE_MNODE_OVER;
    }
    pReplica->id = atoi(id->valuestring);

    cJSON *fqdn = cJSON_GetObjectItem(node, "fqdn");
    if (!fqdn || fqdn->type != cJSON_String || fqdn->valuestring == NULL) {
      dError("failed to read %s since fqdn not found", pMgmt->file);
      goto PRASE_MNODE_OVER;
    }
    tstrncpy(pReplica->fqdn, fqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *port = cJSON_GetObjectItem(node, "port");
    if (!port || port->type != cJSON_String || port->valuestring == NULL) {
      dError("failed to read %s since port not found", pMgmt->file);
      goto PRASE_MNODE_OVER;
    }
    pReplica->port = atoi(port->valuestring);
  }

  code = 0;
  dInfo("succcessed to read file %s", pMgmt->file);

PRASE_MNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  terrno = code;
  return code;
}

static int32_t dndWriteMnodeFile(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  char file[PATH_MAX + 20] = {0};
  snprintf(file, sizeof(file), "%s.bak", pMgmt->file);

  FILE *fp = fopen(file, "w");
  if (fp == NULL) {
    terrno = TSDB_CODE_DND_MNODE_WRITE_FILE_ERROR;
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 4096;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": \"%d\",\n", pMgmt->deployed);

  len += snprintf(content + len, maxLen - len, "  \"dropped\": \"%d\",\n", pMgmt->dropped);
  len += snprintf(content + len, maxLen - len, "  \"nodes\": [{\n");
  for (int32_t i = 0; i < pMgmt->replica; ++i) {
    SReplica *pReplica = &pMgmt->replicas[i];
    len += snprintf(content + len, maxLen - len, "    \"id\": \"%d\",\n", pReplica->id);
    len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\",\n", pReplica->fqdn);
    len += snprintf(content + len, maxLen - len, "    \"port\": \"%u\"\n", pReplica->port);
    if (i < pMgmt->replica - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);

  if (taosRenameFile(file, pMgmt->file) != 0) {
    terrno = TSDB_CODE_DND_MNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", pMgmt->file, terrstr());
    return -1;
  }

  dInfo("successed to write %s", pMgmt->file);
  return 0;
}

static int32_t dndStartMnodeWorker(SDnode *pDnode) {
  if (dndInitMnodeReadWorker(pDnode) != 0) {
    dError("failed to start mnode read worker since %s", terrstr());
    return -1;
  }

  if (dndInitMnodeWriteWorker(pDnode) != 0) {
    dError("failed to start mnode write worker since %s", terrstr());
    return -1;
  }

  if (dndInitMnodeSyncWorker(pDnode) != 0) {
    dError("failed to start mnode sync worker since %s", terrstr());
    return -1;
  }

  if (dndAllocMnodeReadQueue(pDnode) != 0) {
    dError("failed to alloc mnode read queue since %s", terrstr());
    return -1;
  }

  if (dndAllocMnodeWriteQueue(pDnode) != 0) {
    dError("failed to alloc mnode write queue since %s", terrstr());
    return -1;
  }

  if (dndAllocMnodeApplyQueue(pDnode) != 0) {
    dError("failed to alloc mnode apply queue since %s", terrstr());
    return -1;
  }

  if (dndAllocMnodeSyncQueue(pDnode) != 0) {
    dError("failed to alloc mnode sync queue since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopMnodeWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  pMgmt->pMnode = NULL;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 1) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pReadQ)) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pApplyQ)) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pWriteQ)) taosMsleep(10);
  while (!taosQueueEmpty(pMgmt->pSyncQ)) taosMsleep(10);

  dndCleanupMnodeReadWorker(pDnode);
  dndCleanupMnodeWriteWorker(pDnode);
  dndCleanupMnodeSyncWorker(pDnode);

  dndFreeMnodeReadQueue(pDnode);
  dndFreeMnodeWriteQueue(pDnode);
  dndFreeMnodeApplyQueue(pDnode);
  dndFreeMnodeSyncQueue(pDnode);
}

static bool dndNeedDeployMnode(SDnode *pDnode) {
  if (dndGetDnodeId(pDnode) > 0) {
    return false;
  }

  if (dndGetClusterId(pDnode) > 0) {
    return false;
  }

  if (strcmp(pDnode->opt.localEp, pDnode->opt.firstEp) != 0) {
    return false;
  }

  return true;
}

static void dndInitMnodeOption(SDnode *pDnode, SMnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendMsgToDnodeFp = dndSendMsgToDnode;
  pOption->sendMsgToMnodeFp = dndSendMsgToMnode;
  pOption->sendRedirectMsgFp = dndSendRedirectMsg;
  pOption->putMsgToApplyMsgFp = dndPutMsgIntoMnodeApplyQueue;
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);
}

static void dndBuildMnodeDeployOption(SDnode *pDnode, SMnodeOpt *pOption) {
  dndInitMnodeOption(pDnode, pOption);
  pOption->replica = 1;
  pOption->selfIndex = 0;
  SReplica *pReplica = &pOption->replicas[0];
  pReplica->id = 1;
  pReplica->port = pDnode->opt.serverPort;
  tstrncpy(pReplica->fqdn, pDnode->opt.localFqdn, TSDB_FQDN_LEN);

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

static void dndBuildMnodeOpenOption(SDnode *pDnode, SMnodeOpt *pOption) {
  dndInitMnodeOption(pDnode, pOption);
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pOption->selfIndex = pMgmt->selfIndex;
  pOption->replica = pMgmt->replica;
  memcpy(&pOption->replicas, pMgmt->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

static int32_t dndBuildMnodeOptionFromMsg(SDnode *pDnode, SMnodeOpt *pOption, SCreateMnodeMsg *pMsg) {
  dndInitMnodeOption(pDnode, pOption);
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);

  pOption->replica = pMsg->replica;
  pOption->selfIndex = -1;
  for (int32_t index = 0; index < pMsg->replica; ++index) {
    SReplica *pReplica = &pOption->replicas[index];
    pReplica->id = pMsg->replicas[index].id;
    pReplica->port = pMsg->replicas[index].port;
    tstrncpy(pReplica->fqdn, pMsg->replicas[index].fqdn, TSDB_FQDN_LEN);
    if (pReplica->id == pOption->dnodeId) {
      pOption->selfIndex = index;
    }
  }

  if (pOption->selfIndex == -1) {
    terrno = TSDB_CODE_DND_MNODE_ID_NOT_FOUND;
    dError("failed to build mnode options since %s", terrstr());
    return -1;
  }

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  return 0;
}

static int32_t dndOpenMnode(SDnode *pDnode, SMnodeOpt *pOption) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  int32_t code = dndStartMnodeWorker(pDnode);
  if (code != 0) {
    dError("failed to start mnode worker since %s", terrstr());
    return code;
  }

  SMnode *pMnode = mndOpen(pDnode->dir.mnode, pOption);
  if (pMnode == NULL) {
    dError("failed to open mnode since %s", terrstr());
    code = terrno;
    dndStopMnodeWorker(pDnode);
    terrno = code;
    return code;
  }

  if (dndWriteMnodeFile(pDnode) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    code = terrno;
    dndStopMnodeWorker(pDnode);
    mndClose(pMnode);
    mndDestroy(pDnode->dir.mnode);
    terrno = code;
    return code;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pMnode = pMnode;
  pMgmt->deployed = 1;
  taosWUnLockLatch(&pMgmt->latch);

  return 0;
}

static int32_t dndAlterMnode(SDnode *pDnode, SMnodeOpt *pOption) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL) {
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  if (mndAlter(pMnode, pOption) != 0) {
    dError("failed to alter mnode since %s", terrstr());
    dndReleaseMnode(pDnode, pMnode);
    return -1;
  }

  dndReleaseMnode(pDnode, pMnode);
  return 0;
}

static int32_t dndDropMnode(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL) {
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (dndWriteMnodeFile(pDnode) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    dndReleaseMnode(pDnode, pMnode);
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  dndStopMnodeWorker(pDnode);
  dndWriteMnodeFile(pDnode);
  mndClose(pMnode);
  mndDestroy(pDnode->dir.mnode);

  return 0;
}

static SCreateMnodeMsg *dndParseCreateMnodeMsg(SRpcMsg *pRpcMsg) {
  SCreateMnodeMsg *pMsg = pRpcMsg->pCont;
  pMsg->dnodeId = htonl(pMsg->dnodeId);
  for (int32_t i = 0; i < pMsg->replica; ++i) {
    pMsg->replicas[i].id = htonl(pMsg->replicas[i].id);
    pMsg->replicas[i].port = htons(pMsg->replicas[i].port);
  }

  return pMsg;
}

static int32_t dndProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  SCreateMnodeMsg *pMsg = dndParseCreateMnodeMsg(pRpcMsg->pCont);

  if (pMsg->dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_ID_INVALID;
    return -1;
  } else {
    SMnodeOpt option = {0};
    if (dndBuildMnodeOptionFromMsg(pDnode, &option, pMsg) != 0) {
      return -1;
    }

    return dndOpenMnode(pDnode, &option);
  }
}

static int32_t dndProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  SAlterMnodeMsg *pMsg = dndParseCreateMnodeMsg(pRpcMsg->pCont);

  if (pMsg->dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_ID_INVALID;
    return -1;
  } else {
    SMnodeOpt option = {0};
    if (dndBuildMnodeOptionFromMsg(pDnode, &option, pMsg) != 0) {
      return -1;
    }
    return dndAlterMnode(pDnode, &option);
  }
}

static int32_t dndProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  SDropMnodeMsg *pMsg = dndParseCreateMnodeMsg(pRpcMsg->pCont);

  if (pMsg->dnodeId != dndGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_ID_INVALID;
    return -1;
  } else {
    return dndDropMnode(pDnode);
  }
}

static void dndProcessMnodeMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_MNODE_IN:
      code = dndProcessCreateMnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_ALTER_MNODE_IN:
      code = dndProcessAlterMnodeReq(pDnode, pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_MNODE_IN:
      code = dndProcessDropMnodeReq(pDnode, pMsg);
      break;
    default:
      code = TSDB_CODE_MSG_NOT_PROCESSED;
      break;
  }

  SRpcMsg rsp = {.code = code, .handle = pMsg->handle};
  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dndProcessMnodeReadQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode != NULL) {
    mndProcessReadMsg(pMsg);
    dndReleaseMnode(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  mndCleanupMsg(pMsg);
}

static void dndProcessMnodeWriteQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode != NULL) {
    mndProcessWriteMsg(pMsg);
    dndReleaseMnode(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  mndCleanupMsg(pMsg);
}

static void dndProcessMnodeApplyQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode != NULL) {
    mndProcessApplyMsg(pMsg);
    dndReleaseMnode(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  mndCleanupMsg(pMsg);
}

static void dndProcessMnodeSyncQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode != NULL) {
    mndProcessSyncMsg(pMsg);
    dndReleaseMnode(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  mndCleanupMsg(pMsg);
}

static int32_t dndWriteMnodeMsgToQueue(SMnode *pMnode, taos_queue pQueue, SRpcMsg *pRpcMsg) {
  assert(pQueue);

  SMnodeMsg *pMsg = mndInitMsg(pMnode, pRpcMsg);
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosWriteQitem(pQueue, pMsg) != 0) {
    mndCleanupMsg(pMsg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

void dndProcessMnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pRpcMsg, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = dndAcquireMnode(pDnode);

  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg));
  if (pMsg == NULL || taosWriteQitem(pMgmt->pMgmtQ, pMsg) != 0) {
    SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = TSDB_CODE_OUT_OF_MEMORY};
    rpcSendResponse(&rsp);
    rpcFreeCont(pRpcMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

void dndProcessMnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL || dndWriteMnodeMsgToQueue(pMnode, pMgmt->pWriteQ, pMsg) != 0) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = terrno};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }

  dndReleaseMnode(pDnode, pMnode);
}

void dndProcessMnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL || dndWriteMnodeMsgToQueue(pMnode, pMgmt->pSyncQ, pMsg) != 0) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = terrno};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }

  dndReleaseMnode(pDnode, pMnode);
}

void dndProcessMnodeReadMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL || dndWriteMnodeMsgToQueue(pMnode, pMgmt->pSyncQ, pMsg) != 0) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = terrno};
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }

  dndReleaseMnode(pDnode, pMnode);
}

static int32_t dndPutMsgIntoMnodeApplyQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL) {
    return -1;
  }

  int32_t code = taosWriteQitem(pMgmt->pApplyQ, pMsg);
  dndReleaseMnode(pDnode, pMnode);
  return code;
}

static int32_t dndAllocMnodeMgmtQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->pMgmtQ = tWorkerAllocQueue(&pMgmt->mgmtPool, pDnode, (FProcessItem)dndProcessMnodeMgmtQueue);
  if (pMgmt->pMgmtQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

static void dndFreeMnodeMgmtQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerFreeQueue(&pMgmt->mgmtPool, pMgmt->pMgmtQ);
  pMgmt->pMgmtQ = NULL;
}

static int32_t dndInitMnodeMgmtWorker(SDnode *pDnode) {
  SMnodeMgmt  *pMgmt = &pDnode->mmgmt;
  SWorkerPool *pPool = &pMgmt->mgmtPool;
  pPool->name = "mnode-mgmt";
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("mnode mgmt worker is initialized");
  return 0;
}

static void dndCleanupMnodeMgmtWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerCleanup(&pMgmt->mgmtPool);
  dDebug("mnode mgmt worker is closed");
}

static int32_t dndAllocMnodeReadQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->pReadQ = tWorkerAllocQueue(&pMgmt->readPool, pDnode, (FProcessItem)dndProcessMnodeReadQueue);
  if (pMgmt->pReadQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeMnodeReadQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerFreeQueue(&pMgmt->readPool, pMgmt->pReadQ);
  pMgmt->pReadQ = NULL;
}

static int32_t dndInitMnodeReadWorker(SDnode *pDnode) {
  SMnodeMgmt  *pMgmt = &pDnode->mmgmt;
  SWorkerPool *pPool = &pMgmt->readPool;
  pPool->name = "mnode-read";
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("mnode read worker is initialized");
  return 0;
}

static void dndCleanupMnodeReadWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerCleanup(&pMgmt->readPool);
  dDebug("mnode read worker is closed");
}

static int32_t dndAllocMnodeWriteQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->pWriteQ = tWorkerAllocQueue(&pMgmt->writePool, pDnode, (FProcessItem)dndProcessMnodeWriteQueue);
  if (pMgmt->pWriteQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeMnodeWriteQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerFreeQueue(&pMgmt->writePool, pMgmt->pWriteQ);
  pMgmt->pWriteQ = NULL;
}

static int32_t dndAllocMnodeApplyQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->pApplyQ = tWorkerAllocQueue(&pMgmt->writePool, pDnode, (FProcessItem)dndProcessMnodeApplyQueue);
  if (pMgmt->pApplyQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeMnodeApplyQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerFreeQueue(&pMgmt->writePool, pMgmt->pApplyQ);
  pMgmt->pApplyQ = NULL;
}

static int32_t dndInitMnodeWriteWorker(SDnode *pDnode) {
  SMnodeMgmt  *pMgmt = &pDnode->mmgmt;
  SWorkerPool *pPool = &pMgmt->writePool;
  pPool->name = "mnode-write";
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("mnode write worker is initialized");
  return 0;
}

static void dndCleanupMnodeWriteWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerCleanup(&pMgmt->writePool);
  dDebug("mnode write worker is closed");
}

static int32_t dndAllocMnodeSyncQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->pSyncQ = tWorkerAllocQueue(&pMgmt->syncPool, pDnode, (FProcessItem)dndProcessMnodeSyncQueue);
  if (pMgmt->pSyncQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dndFreeMnodeSyncQueue(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerFreeQueue(&pMgmt->syncPool, pMgmt->pSyncQ);
  pMgmt->pSyncQ = NULL;
}

static int32_t dndInitMnodeSyncWorker(SDnode *pDnode) {
  SMnodeMgmt  *pMgmt = &pDnode->mmgmt;
  SWorkerPool *pPool = &pMgmt->syncPool;
  pPool->name = "mnode-sync";
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("mnode sync worker is initialized");
  return 0;
}

static void dndCleanupMnodeSyncWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  tWorkerCleanup(&pMgmt->syncPool);
  dDebug("mnode sync worker is closed");
}

int32_t dndInitMnode(SDnode *pDnode) {
  dInfo("dnode-mnode start to init");
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  taosInitRWLatch(&pMgmt->latch);

  if (dndInitMnodeMgmtWorker(pDnode) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/mnode.json", pDnode->dir.dnode);
  pMgmt->file = strdup(path);
  if (pMgmt->file == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dndReadMnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) {
    dInfo("mnode has been deployed and needs to be deleted");
    mndDestroy(pDnode->dir.mnode);
    return 0;
  }

  if (!pMgmt->deployed) {
    bool needDeploy = dndNeedDeployMnode(pDnode);
    if (!needDeploy) {
      dDebug("mnode does not need to be deployed");
      return 0;
    }

    dInfo("start to deploy mnode");
    SMnodeOpt option = {0};
    dndBuildMnodeDeployOption(pDnode, &option);
    return dndOpenMnode(pDnode, &option);
  } else {
    dInfo("start to open mnode");
    SMnodeOpt option = {0};
    dndBuildMnodeOpenOption(pDnode, &option);
    return dndOpenMnode(pDnode, &option);
  }
}

void dndCleanupMnode(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  dInfo("dnode-mnode start to clean up");
  dndStopMnodeWorker(pDnode);
  dndCleanupMnodeMgmtWorker(pDnode);
  tfree(pMgmt->file);
  dInfo("dnode-mnode is cleaned up");
}

int32_t dndGetUserAuthFromMnode(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = dndAcquireMnode(pDnode);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_APP_NOT_READY;
    dTrace("failed to get user auth since %s", terrstr());
    return -1;
  }

  int32_t code = mndRetriveAuth(pMnode, user, spi, encrypt, secret, ckey);
  dndReleaseMnode(pDnode, pMnode);
  return code;
}
