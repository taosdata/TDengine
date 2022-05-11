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
#include "dmInt.h"

static void dmPrintEps(SDnodeMgmt *pMgmt);
static bool dmIsEpChanged(SDnodeMgmt *pMgmt, int32_t dnodeId, const char *ep);
static void dmResetEps(SDnodeMgmt *pMgmt, SArray *dnodeEps);

static void dmGetDnodeEp(SDnodeMgmt *pMgmt, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  taosRLockLatch(&pMgmt->data.latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->data.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->ep.port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    }
  }

  taosRUnLockLatch(&pMgmt->data.latch);
}

int32_t dmReadEps(SDnodeMgmt *pMgmt) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t   len = 0;
  int32_t   maxLen = 256 * 1024;
  char     *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  pMgmt->data.dnodeEps = taosArrayInit(1, sizeof(SDnodeEp));
  if (pMgmt->data.dnodeEps == NULL) {
    dError("failed to calloc dnodeEp array since %s", strerror(errno));
    goto _OVER;
  }

  snprintf(file, sizeof(file), "%s%sdnode.json", pMgmt->path, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = 0;
    goto _OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto _OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto _OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s since dnodeId not found", file);
    goto _OVER;
  }
  pMgmt->data.dnodeId = dnodeId->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", file);
    goto _OVER;
  }
  pMgmt->data.clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", file);
    goto _OVER;
  }
  pMgmt->data.dropped = dropped->valueint;

  cJSON *dnodes = cJSON_GetObjectItem(root, "dnodes");
  if (!dnodes || dnodes->type != cJSON_Array) {
    dError("failed to read %s since dnodes not found", file);
    goto _OVER;
  }

  int32_t numOfDnodes = cJSON_GetArraySize(dnodes);
  if (numOfDnodes <= 0) {
    dError("failed to read %s since numOfDnodes:%d invalid", file, numOfDnodes);
    goto _OVER;
  }

  for (int32_t i = 0; i < numOfDnodes; ++i) {
    cJSON *node = cJSON_GetArrayItem(dnodes, i);
    if (node == NULL) break;

    SDnodeEp dnodeEp = {0};

    cJSON *did = cJSON_GetObjectItem(node, "id");
    if (!did || did->type != cJSON_Number) {
      dError("failed to read %s since dnodeId not found", file);
      goto _OVER;
    }

    dnodeEp.id = did->valueint;

    cJSON *dnodeFqdn = cJSON_GetObjectItem(node, "fqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s since dnodeFqdn not found", file);
      goto _OVER;
    }
    tstrncpy(dnodeEp.ep.fqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(node, "port");
    if (!dnodePort || dnodePort->type != cJSON_Number) {
      dError("failed to read %s since dnodePort not found", file);
      goto _OVER;
    }

    dnodeEp.ep.port = dnodePort->valueint;

    cJSON *isMnode = cJSON_GetObjectItem(node, "isMnode");
    if (!isMnode || isMnode->type != cJSON_Number) {
      dError("failed to read %s since isMnode not found", file);
      goto _OVER;
    }
    dnodeEp.isMnode = isMnode->valueint;

    taosArrayPush(pMgmt->data.dnodeEps, &dnodeEp);
  }

  code = 0;
  dDebug("succcessed to read file %s", file);
  dmPrintEps(pMgmt);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (taosArrayGetSize(pMgmt->data.dnodeEps) == 0) {
    SDnodeEp dnodeEp = {0};
    dnodeEp.isMnode = 1;
    taosGetFqdnPortFromEp(pMgmt->data.firstEp, &dnodeEp.ep);
    taosArrayPush(pMgmt->data.dnodeEps, &dnodeEp);
  }

  dmResetEps(pMgmt, pMgmt->data.dnodeEps);

  if (dmIsEpChanged(pMgmt, pMgmt->data.dnodeId, pMgmt->data.localEp)) {
    dError("localEp %s different with %s and need reconfigured", pMgmt->data.localEp, file);
    return -1;
  }

  terrno = code;
  return code;
}

int32_t dmWriteEps(SDnodeMgmt *pMgmt) {
  char file[PATH_MAX] = {0};
  char realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode.json.bak", pMgmt->path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%sdnode.json", pMgmt->path, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    dError("failed to write %s since %s", file, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 256 * 1024;
  char   *content = taosMemoryCalloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", pMgmt->data.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%" PRId64 "\",\n", pMgmt->data.clusterId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d,\n", pMgmt->data.dropped);
  len += snprintf(content + len, maxLen - len, "  \"dnodes\": [{\n");

  int32_t numOfEps = (int32_t)taosArrayGetSize(pMgmt->data.dnodeEps);
  for (int32_t i = 0; i < numOfEps; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pMgmt->data.dnodeEps, i);
    len += snprintf(content + len, maxLen - len, "    \"id\": %d,\n", pDnodeEp->id);
    len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\",\n", pDnodeEp->ep.fqdn);
    len += snprintf(content + len, maxLen - len, "    \"port\": %u,\n", pDnodeEp->ep.port);
    len += snprintf(content + len, maxLen - len, "    \"isMnode\": %d\n", pDnodeEp->isMnode);
    if (i < numOfEps - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(content);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  pMgmt->data.updateTime = taosGetTimestampMs();
  dDebug("successed to write %s", realfile);
  return 0;
}

void dmUpdateEps(SDnodeMgmt *pMgmt, SArray *eps) {
  int32_t numOfEps = taosArrayGetSize(eps);
  if (numOfEps <= 0) return;

  taosWLockLatch(&pMgmt->data.latch);

  int32_t numOfEpsOld = (int32_t)taosArrayGetSize(pMgmt->data.dnodeEps);
  if (numOfEps != numOfEpsOld) {
    dmResetEps(pMgmt, eps);
    dmWriteEps(pMgmt);
  } else {
    int32_t size = numOfEps * sizeof(SDnodeEp);
    if (memcmp(pMgmt->data.dnodeEps->pData, eps->pData, size) != 0) {
      dmResetEps(pMgmt, eps);
      dmWriteEps(pMgmt);
    }
  }

  taosWUnLockLatch(&pMgmt->data.latch);
}

static void dmResetEps(SDnodeMgmt *pMgmt, SArray *dnodeEps) {
  if (pMgmt->data.dnodeEps != dnodeEps) {
    SArray *tmp = pMgmt->data.dnodeEps;
    pMgmt->data.dnodeEps = taosArrayDup(dnodeEps);
    taosArrayDestroy(tmp);
  }

  pMgmt->data.mnodeEps.inUse = 0;
  pMgmt->data.mnodeEps.numOfEps = 0;

  int32_t mIndex = 0;
  int32_t numOfEps = (int32_t)taosArrayGetSize(dnodeEps);

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    pMgmt->data.mnodeEps.numOfEps++;

    pMgmt->data.mnodeEps.eps[mIndex] = pDnodeEp->ep;
    mIndex++;
  }

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    taosHashPut(pMgmt->data.dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
  }

  dmPrintEps(pMgmt);
}

static void dmPrintEps(SDnodeMgmt *pMgmt) {
  int32_t numOfEps = (int32_t)taosArrayGetSize(pMgmt->data.dnodeEps);
  dDebug("print dnode ep list, num:%d", numOfEps);
  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pEp = taosArrayGet(pMgmt->data.dnodeEps, i);
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->ep.fqdn, pEp->ep.port, pEp->isMnode);
  }
}

static bool dmIsEpChanged(SDnodeMgmt *pMgmt, int32_t dnodeId, const char *ep) {
  bool changed = false;
  if (dnodeId == 0) return changed;
  taosRLockLatch(&pMgmt->data.latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->data.dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1] = {0};
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    changed = (strcmp(ep, epstr) != 0);
    if (changed) {
      dError("dnode:%d, localEp %s different from %s", dnodeId, ep, epstr);
    }
  }

  taosRUnLockLatch(&pMgmt->data.latch);
  return changed;
}
