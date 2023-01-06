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
#include "vmInt.h"
#include "tjson.h"

#define MAX_CONTENT_LEN 2 * 1024 * 1024

SVnodeObj **vmGetVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes) {
  taosThreadRwlockRdlock(&pMgmt->lock);

  int32_t     num = 0;
  int32_t     size = taosHashGetSize(pMgmt->hash);
  SVnodeObj **pVnodes = taosMemoryCalloc(size, sizeof(SVnodeObj *));

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      // dTrace("vgId:%d, acquire vnode list, ref:%d", pVnode->vgId, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->hash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->hash, pIter);
    }
  }

  taosThreadRwlockUnlock(&pMgmt->lock);
  *numOfVnodes = num;

  return pVnodes;
}

int32_t vmGetVnodeListFromFile(SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes) {
  int32_t      code = TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t      len = 0;
  int32_t      maxLen = MAX_CONTENT_LEN;
  char        *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON       *root = NULL;
  FILE        *fp = NULL;
  char         file[PATH_MAX] = {0};
  SWrapperCfg *pCfgs = NULL;
  TdFilePtr    pFile = NULL;

  snprintf(file, sizeof(file), "%s%svnodes.json", pMgmt->path, TD_DIRSEP);

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dInfo("file %s not exist", file);
    code = 0;
    goto _OVER;
  }

  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
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

  cJSON *vnodes = cJSON_GetObjectItem(root, "vnodes");
  if (!vnodes || vnodes->type != cJSON_Array) {
    dError("failed to read %s since vnodes not found", file);
    goto _OVER;
  }

  int32_t vnodesNum = cJSON_GetArraySize(vnodes);
  if (vnodesNum > 0) {
    pCfgs = taosMemoryCalloc(vnodesNum, sizeof(SWrapperCfg));
    if (pCfgs == NULL) {
      dError("failed to read %s since out of memory", file);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    for (int32_t i = 0; i < vnodesNum; ++i) {
      cJSON       *vnode = cJSON_GetArrayItem(vnodes, i);
      SWrapperCfg *pCfg = &pCfgs[i];

      cJSON *vgId = cJSON_GetObjectItem(vnode, "vgId");
      if (!vgId || vgId->type != cJSON_Number) {
        dError("failed to read %s since vgId not found", file);
        taosMemoryFree(pCfgs);
        goto _OVER;
      }
      pCfg->vgId = vgId->valueint;
      snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCfg->vgId);

      cJSON *dropped = cJSON_GetObjectItem(vnode, "dropped");
      if (!dropped || dropped->type != cJSON_Number) {
        dError("failed to read %s since dropped not found", file);
        taosMemoryFree(pCfgs);
        goto _OVER;
      }
      pCfg->dropped = dropped->valueint;

      cJSON *vgVersion = cJSON_GetObjectItem(vnode, "vgVersion");
      if (!vgVersion || vgVersion->type != cJSON_Number) {
        dError("failed to read %s since vgVersion not found", file);
        taosMemoryFree(pCfgs);
        goto _OVER;
      }
      pCfg->vgVersion = vgVersion->valueint;
    }

    *ppCfgs = pCfgs;
  }

  *numOfVnodes = vnodesNum;
  code = 0;
  dInfo("succcessed to read file %s, numOfVnodes:%d", file, vnodesNum);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

static int32_t vmEncodeVnodeList(SJson *pJson, SVnodeObj **ppVnodes, int32_t numOfVnodes) {
  SJson *vnodes = tjsonCreateArray();
  if (vnodes == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "vnodes", vnodes) < 0) return -1;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = ppVnodes[i];
    if (pVnode == NULL) continue;

    SJson *vnode = tjsonCreateObject();
    if (vnode == NULL) return -1;
    if (tjsonAddDoubleToObject(vnode, "vgId", pVnode->vgId) < 0) return -1;
    if (tjsonAddDoubleToObject(vnode, "dropped", pVnode->dropped) < 0) return -1;
    if (tjsonAddDoubleToObject(vnode, "vgVersion", pVnode->vgVersion) < 0) return -1;
    if (tjsonAddItemToArray(vnodes, vnode) < 0) return -1;
  }

  return 0;
}

int32_t vmWriteVnodeListToFile(SVnodeMgmt *pMgmt) {
  int32_t     code = -1;
  char       *buffer = NULL;
  SJson      *pJson = NULL;
  TdFilePtr   pFile = NULL;
  SVnodeObj **ppVnodes = NULL;
  char        file[PATH_MAX] = {0};
  char        realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%svnodes.json.bak", pMgmt->path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%svnodes.json", pMgmt->path, TD_DIRSEP);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) goto _OVER;

  int32_t numOfVnodes = 0;
  ppVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);
  if (ppVnodes == NULL) goto _OVER;

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (vmEncodeVnodeList(pJson, ppVnodes, numOfVnodes) != 0) goto _OVER;

  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;
  taosCloseFile(&pFile);

  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  dInfo("succeed to write vnodes file:%s, vnodes:%d", realfile, numOfVnodes);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (ppVnodes != NULL) {
    for (int32_t i = 0; i < numOfVnodes; ++i) {
      SVnodeObj *pVnode = ppVnodes[i];
      if (pVnode != NULL) {
        vmReleaseVnode(pMgmt, pVnode);
      }
    }
    taosMemoryFree(ppVnodes);
  }

  if (code != 0) {
    dError("failed to write vnodes file:%s since %s, vnodes:%d", realfile, terrstr(), numOfVnodes);
  }
  return code;
}