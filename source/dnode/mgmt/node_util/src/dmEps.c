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

static int32_t dmDecodeEps(SJson *pJson, SDnodeData *pData) {
  int32_t code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "dnodeId", pData->dnodeId, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "dnodeVer", pData->dnodeVer, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "clusterId", pData->clusterId, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "dropped", pData->dropped, code);
  if (code < 0) return -1;

  SJson *dnodes = tjsonGetObjectItem(pJson, "dnodes");
  if (dnodes == NULL) return 0;
  int32_t numOfDnodes = tjsonGetArraySize(dnodes);

  for (int32_t i = 0; i < numOfDnodes; ++i) {
    SJson *dnode = tjsonGetArrayItem(dnodes, i);
    if (dnode == NULL) return -1;

    SDnodeEp dnodeEp = {0};
    tjsonGetInt32ValueFromDouble(dnode, "id", dnodeEp.id, code);
    if (code < 0) return -1;
    code = tjsonGetStringValue(dnode, "fqdn", dnodeEp.ep.fqdn);
    if (code < 0) return -1;
    tjsonGetUInt16ValueFromDouble(dnode, "port", dnodeEp.ep.port, code);
    if (code < 0) return -1;
    tjsonGetInt8ValueFromDouble(dnode, "isMnode", dnodeEp.isMnode, code);
    if (code < 0) return -1;

    if (taosArrayPush(pData->dnodeEps, &dnodeEp) == NULL) return -1;
  }

  return 0;
}

int32_t dmReadEps(SDnodeData *pData) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  pData->dnodeEps = taosArrayInit(1, sizeof(SDnodeEp));
  if (pData->dnodeEps == NULL) {
    dError("failed to calloc dnodeEp array since %s", strerror(errno));
    goto _OVER;
  }

  if (taosStatFile(file, NULL, NULL) < 0) {
    dInfo("dnode file:%s not exist", file);
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (dmDecodeEps(pJson, pData) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read mnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read dnode file:%s since %s", file, terrstr());
  }

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

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (dmEncodeEps(pJson, pData) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) goto _OVER;

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
    dError("failed to write dnode file:%s since %s, dnodeVer:%" PRId64, realfile, terrstr(), pData->dnodeVer);
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

void dmUpdateDnodeInfo(void *data, int32_t *did, int64_t *clusterId, char *fqdn, uint16_t *port) {
  SDnodeData *pData = data;
  int32_t     dnodeId = -1;
  if (did != NULL) dnodeId = *did;

  taosThreadRwlockRdlock(&pData->lock);

  if (pData->oldDnodeEps != NULL) {
    int32_t size = (int32_t)taosArrayGetSize(pData->oldDnodeEps);
    for (int32_t i = 0; i < size; ++i) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->oldDnodeEps, i);
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) == 0 && pDnodeEp->ep.port == *port) {
        dInfo("dnode:%d, update ep:%s:%u to %s:%u", dnodeId, fqdn, *port, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
        tstrncpy(fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
        *port = pDnodeEp->ep.port;
      }
    }
  }

  if (did != NULL && dnodeId <= 0) {
    int32_t size = (int32_t)taosArrayGetSize(pData->dnodeEps);
    for (int32_t i = 0; i < size; ++i) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, i);
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) == 0 && pDnodeEp->ep.port == *port) {
        dInfo("dnode:%s:%u, update dnodeId to dnode:%d", fqdn, *port, pDnodeEp->id);
        *did = pDnodeEp->id;
        if (clusterId != NULL) *clusterId = pData->clusterId;
      }
    }
  }

  if (dnodeId > 0) {
    SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
    if (pDnodeEp) {
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) != 0 || pDnodeEp->ep.port != *port) {
        dInfo("dnode:%d, update ep:%s:%u to %s:%u", dnodeId, fqdn, *port, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
        tstrncpy(fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
        *port = pDnodeEp->ep.port;
      }
      if (clusterId != NULL) *clusterId = pData->clusterId;
    }
  }

  taosThreadRwlockUnlock(&pData->lock);
}