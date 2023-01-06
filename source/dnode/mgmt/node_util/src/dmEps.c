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
#include "dmUtil.h"
#include "tjson.h"
#include "tmisce.h"

static void dmPrintEps(SDnodeData *pData);
static bool dmIsEpChanged(SDnodeData *pData, int32_t dnodeId, const char *ep);
static void dmResetEps(SDnodeData *pData, SArray *dnodeEps);

static void dmGetDnodeEp(SDnodeData *pData, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  taosThreadRwlockRdlock(&pData->lock);

  SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
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

  taosThreadRwlockUnlock(&pData->lock);
}

int32_t dmReadEps(SDnodeData *pData) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t   len = 0;
  int32_t   maxLen = 256 * 1024;
  char     *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  pData->dnodeEps = taosArrayInit(1, sizeof(SDnodeEp));
  if (pData->dnodeEps == NULL) {
    dError("failed to calloc dnodeEp array since %s", strerror(errno));
    goto _OVER;
  }

  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
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
  pData->dnodeId = dnodeId->valueint;

  cJSON *dnodeVer = cJSON_GetObjectItem(root, "dnodeVer");
  if (!dnodeVer || dnodeVer->type != cJSON_String) {
    dError("failed to read %s since dnodeVer not found", file);
    goto _OVER;
  }
  pData->dnodeVer = atoll(dnodeVer->valuestring);

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", file);
    goto _OVER;
  }
  pData->clusterId = atoll(clusterId->valuestring);

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", file);
    goto _OVER;
  }
  pData->dropped = dropped->valueint;

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

    taosArrayPush(pData->dnodeEps, &dnodeEp);
  }

  code = 0;
  dDebug("succcessed to read file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (taosArrayGetSize(pData->dnodeEps) == 0) {
    SDnodeEp dnodeEp = {0};
    dnodeEp.isMnode = 1;
    taosGetFqdnPortFromEp(tsFirst, &dnodeEp.ep);
    taosArrayPush(pData->dnodeEps, &dnodeEp);
  }

  dDebug("reset dnode list on startup");
  dmResetEps(pData, pData->dnodeEps);

  if (dmIsEpChanged(pData, pData->dnodeId, tsLocalEp)) {
    dError("localEp %s different with %s and need reconfigured", tsLocalEp, file);
    return -1;
  }

  terrno = code;
  return code;
}

static int32_t dmEncodeEps(SJson *pJson, SDnodeData *pData) {
  if (tjsonAddDoubleToObject(pJson, "dnodeId", pData->dnodeId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "dnodeVer", pData->dnodeVer) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "clusterId", pData->clusterId) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "dropped", pData->dropped) < 0) return -1;

  SJson *dnodes = tjsonCreateArray();
  if (dnodes == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "dnodes", dnodes) < 0) return -1;

  int32_t numOfEps = (int32_t)taosArrayGetSize(pData->dnodeEps);
  for (int32_t i = 0; i < numOfEps; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, i);
    SJson    *dnode = tjsonCreateObject();
    if (dnode == NULL) return -1;

    if (tjsonAddDoubleToObject(dnode, "id", pDnodeEp->id) < 0) return -1;
    if (tjsonAddStringToObject(dnode, "fqdn", pDnodeEp->ep.fqdn) < 0) return -1;
    if (tjsonAddDoubleToObject(dnode, "port", pDnodeEp->ep.port) < 0) return -1;
    if (tjsonAddDoubleToObject(dnode, "isMnode", pDnodeEp->isMnode) < 0) return -1;
    if (tjsonAddItemToArray(dnodes, dnode) < 0) return -1;
  }

  return 0;
}

int32_t dmWriteEps(SDnodeData *pData) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json.bak", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) goto _OVER;

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (dmEncodeEps(pJson, pData) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  pData->updateTime = taosGetTimestampMs();
  dInfo("succeed to write dnode file:%s, dnodeVer:%" PRId64, realfile, pData->dnodeVer);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dInfo("succeed to write dnode file:%s since %s, dnodeVer:%" PRId64, realfile, terrstr(), pData->dnodeVer);
  }
  return code;
}

void dmUpdateEps(SDnodeData *pData, SArray *eps) {
  taosThreadRwlockWrlock(&pData->lock);
  dDebug("new dnode list get from mnode, dnodeVer:%" PRId64, pData->dnodeVer);
  dmResetEps(pData, eps);
  dmWriteEps(pData);
  taosThreadRwlockUnlock(&pData->lock);
}

static void dmResetEps(SDnodeData *pData, SArray *dnodeEps) {
  if (pData->dnodeEps != dnodeEps) {
    SArray *tmp = pData->dnodeEps;
    pData->dnodeEps = taosArrayDup(dnodeEps, NULL);
    taosArrayDestroy(tmp);
  }

  pData->mnodeEps.inUse = 0;
  pData->mnodeEps.numOfEps = 0;

  int32_t mIndex = 0;
  int32_t numOfEps = (int32_t)taosArrayGetSize(dnodeEps);

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    pData->mnodeEps.numOfEps++;

    pData->mnodeEps.eps[mIndex] = pDnodeEp->ep;
    mIndex++;
  }

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    taosHashPut(pData->dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
  }

  dmPrintEps(pData);
}

static void dmPrintEps(SDnodeData *pData) {
  int32_t numOfEps = (int32_t)taosArrayGetSize(pData->dnodeEps);
  dDebug("print dnode list, num:%d", numOfEps);
  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pEp = taosArrayGet(pData->dnodeEps, i);
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->ep.fqdn, pEp->ep.port, pEp->isMnode);
  }
}

static bool dmIsEpChanged(SDnodeData *pData, int32_t dnodeId, const char *ep) {
  bool changed = false;
  if (dnodeId == 0) return changed;
  taosThreadRwlockRdlock(&pData->lock);

  SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1] = {0};
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    changed = (strcmp(ep, epstr) != 0);
    if (changed) {
      dError("dnode:%d, localEp %s different from %s", dnodeId, ep, epstr);
    }
  }

  taosThreadRwlockUnlock(&pData->lock);
  return changed;
}

void dmGetMnodeEpSet(SDnodeData *pData, SEpSet *pEpSet) {
  taosThreadRwlockRdlock(&pData->lock);
  *pEpSet = pData->mnodeEps;
  taosThreadRwlockUnlock(&pData->lock);
}

void dmGetMnodeEpSetForRedirect(SDnodeData *pData, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dmGetMnodeEpSet(pData, pEpSet);
  dTrace("msg is redirected, handle:%p num:%d use:%d", pMsg->info.handle, pEpSet->numOfEps, pEpSet->inUse);
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dTrace("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
    if (strcmp(pEpSet->eps[i].fqdn, tsLocalFqdn) == 0 && pEpSet->eps[i].port == tsServerPort) {
      pEpSet->inUse = (i + 1) % pEpSet->numOfEps;
    }
  }
}

void dmSetMnodeEpSet(SDnodeData *pData, SEpSet *pEpSet) {
  if (memcmp(pEpSet, &pData->mnodeEps, sizeof(SEpSet)) == 0) return;
  taosThreadRwlockWrlock(&pData->lock);
  pData->mnodeEps = *pEpSet;
  taosThreadRwlockUnlock(&pData->lock);

  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }
}

int32_t dmUpdateDnodeInfo(void *data, int32_t *dnodeId, int64_t *clusterId, char *fqdn, uint16_t *port) {
  SDnodeData *pData = data;
  int32_t     ret = -1;
  taosThreadRwlockRdlock(&pData->lock);
  if (*dnodeId <= 0) {
    for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pData->dnodeEps); ++i) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, i);
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) == 0 && pDnodeEp->ep.port == *port) {
        dInfo("dnode:%s:%u, update dnodeId from %d to %d", fqdn, *port, *dnodeId, pDnodeEp->id);
        *dnodeId = pDnodeEp->id;
        *clusterId = pData->clusterId;
        ret = 0;
      }
    }
    if (ret != 0) {
      dInfo("dnode:%s:%u, failed to update dnodeId:%d", fqdn, *port, *dnodeId);
    }
  } else {
    SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, dnodeId, sizeof(int32_t));
    if (pDnodeEp) {
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) != 0) {
        dInfo("dnode:%d, update port from %s to %s", *dnodeId, fqdn, pDnodeEp->ep.fqdn);
        tstrncpy(fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
      }
      if (pDnodeEp->ep.port != *port) {
        dInfo("dnode:%d, update port from %u to %u", *dnodeId, *port, pDnodeEp->ep.port);
        *port = pDnodeEp->ep.port;
      }
      *clusterId = pData->clusterId;
      ret = 0;
    } else {
      dInfo("dnode:%d, failed to update dnode info", *dnodeId);
    }
  }
  taosThreadRwlockUnlock(&pData->lock);
  return ret;
}