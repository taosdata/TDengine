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
#include "tjson.h"
#include "vmInt.h"

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

static int32_t vmDecodeVnodeList(SJson *pJson, SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes) {
  int32_t      code = -1;
  SWrapperCfg *pCfgs = NULL;
  *ppCfgs = NULL;

  SJson *vnodes = tjsonGetObjectItem(pJson, "vnodes");
  if (vnodes == NULL) return -1;

  int32_t vnodesNum = cJSON_GetArraySize(vnodes);
  if (vnodesNum > 0) {
    pCfgs = taosMemoryCalloc(vnodesNum, sizeof(SWrapperCfg));
    if (pCfgs == NULL) return -1;
  }

  for (int32_t i = 0; i < vnodesNum; ++i) {
    SJson *vnode = tjsonGetArrayItem(vnodes, i);
    if (vnode == NULL) goto _OVER;

    SWrapperCfg *pCfg = &pCfgs[i];
    tjsonGetInt32ValueFromDouble(vnode, "vgId", pCfg->vgId, code);
    if (code < 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "dropped", pCfg->dropped, code);
    if (code < 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "vgVersion", pCfg->vgVersion, code);
    if (code < 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "diskPrimary", pCfg->diskPrimary, code);
    if (code < 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "toVgId", pCfg->toVgId, code);
    if (code < 0) goto _OVER;

    snprintf(pCfg->path, sizeof(pCfg->path), "%s%svnode%d", pMgmt->path, TD_DIRSEP, pCfg->vgId);
  }

  code = 0;
  *ppCfgs = pCfgs;
  *numOfVnodes = vnodesNum;

_OVER:
  if (*ppCfgs == NULL) taosMemoryFree(pCfgs);
  return code;
}

int32_t vmGetVnodeListFromFile(SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes) {
  int32_t      code = -1;
  TdFilePtr    pFile = NULL;
  char        *pData = NULL;
  SJson       *pJson = NULL;
  char         file[PATH_MAX] = {0};
  SWrapperCfg *pCfgs = NULL;
  snprintf(file, sizeof(file), "%s%svnodes.json", pMgmt->path, TD_DIRSEP);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("vnode file:%s not exist", file);
    return 0;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open vnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat mnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read vnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (vmDecodeVnodeList(pJson, pMgmt, ppCfgs, numOfVnodes) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read vnode file %s", file);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read vnode file:%s since %s", file, terrstr());
  }
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
    if (tjsonAddDoubleToObject(vnode, "diskPrimary", pVnode->diskPrimary) < 0) return -1;
    if (pVnode->toVgId && tjsonAddDoubleToObject(vnode, "toVgId", pVnode->toVgId) < 0) return -1;
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
  snprintf(file, sizeof(file), "%s%svnodes_tmp.json", pMgmt->path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%svnodes.json", pMgmt->path, TD_DIRSEP);

  int32_t numOfVnodes = 0;
  ppVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);
  if (ppVnodes == NULL) goto _OVER;

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (vmEncodeVnodeList(pJson, ppVnodes, numOfVnodes) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

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
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write vnodes file:%s since %s, vnodes:%d", realfile, terrstr(), numOfVnodes);
  }
  return code;
}
