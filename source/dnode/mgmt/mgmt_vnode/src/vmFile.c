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

int32_t vmGetAllVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes) {
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  int32_t num = 0;
  int32_t size = taosHashGetSize(pMgmt->runngingHash);
  int32_t closedSize = taosHashGetSize(pMgmt->closedHash);
  size += closedSize;
  SVnodeObj **pVnodes = taosMemoryCalloc(size, sizeof(SVnodeObj *));
  if (pVnodes == NULL) {
    (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
    return terrno;
  }

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d,acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->runngingHash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->runngingHash, pIter);
    }
  }

  pIter = taosHashIterate(pMgmt->closedHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d, acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->closedHash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->closedHash, pIter);
    }
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
  *numOfVnodes = num;
  *ppVnodes = pVnodes;

  return 0;
}

int32_t vmGetAllVnodeListFromHashWithCreating(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes) {
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  int32_t num = 0;
  int32_t size = taosHashGetSize(pMgmt->runngingHash);
  int32_t creatingSize = taosHashGetSize(pMgmt->creatingHash);
  size += creatingSize;
  SVnodeObj **pVnodes = taosMemoryCalloc(size, sizeof(SVnodeObj *));
  if (pVnodes == NULL) {
    (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
    return terrno;
  }

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d,acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->runngingHash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->runngingHash, pIter);
    }
  }

  pIter = taosHashIterate(pMgmt->creatingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d, acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->creatingHash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->creatingHash, pIter);
    }
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  *numOfVnodes = num;
  *ppVnodes = pVnodes;

  return 0;
}

int32_t vmGetVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes) {
  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);

  int32_t     num = 0;
  int32_t     size = taosHashGetSize(pMgmt->runngingHash);
  SVnodeObj **pVnodes = taosMemoryCalloc(size, sizeof(SVnodeObj *));
  if (pVnodes == NULL) {
    (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
    return terrno;
  }

  void *pIter = taosHashIterate(pMgmt->runngingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    SVnodeObj  *pVnode = *ppVnode;
    if (pVnode && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
      dTrace("vgId:%d, acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
      pVnodes[num++] = (*ppVnode);
      pIter = taosHashIterate(pMgmt->runngingHash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->runngingHash, pIter);
    }
  }

  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
  *numOfVnodes = num;
  *ppVnodes = pVnodes;

  return 0;
}

static int32_t vmDecodeVnodeList(SJson *pJson, SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes) {
  int32_t      code = -1;
  SWrapperCfg *pCfgs = NULL;
  *ppCfgs = NULL;

  SJson *vnodes = tjsonGetObjectItem(pJson, "vnodes");
  if (vnodes == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

  int32_t vnodesNum = cJSON_GetArraySize(vnodes);
  if (vnodesNum > 0) {
    pCfgs = taosMemoryCalloc(vnodesNum, sizeof(SWrapperCfg));
    if (pCfgs == NULL) return terrno;
  }

  for (int32_t i = 0; i < vnodesNum; ++i) {
    SJson *vnode = tjsonGetArrayItem(vnodes, i);
    if (vnode == NULL) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _OVER;
    }

    SWrapperCfg *pCfg = &pCfgs[i];
    tjsonGetInt32ValueFromDouble(vnode, "vgId", pCfg->vgId, code);
    if (code != 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "dropped", pCfg->dropped, code);
    if (code != 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "vgVersion", pCfg->vgVersion, code);
    if (code != 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "diskPrimary", pCfg->diskPrimary, code);
    if (code != 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(vnode, "toVgId", pCfg->toVgId, code);
    if (code != 0) goto _OVER;

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
    code = terrno;
    dInfo("vnode file:%s not exist, reason:%s", file, tstrerror(code));
    code = 0;
    return code;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open vnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat vnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    code = terrno;
    dError("failed to read vnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (vmDecodeVnodeList(pJson, pMgmt, ppCfgs, numOfVnodes) < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read vnode file %s", file);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read vnode file:%s since %s", file, tstrerror(code));
  }
  return code;
}

static int32_t vmEncodeVnodeList(SJson *pJson, SVnodeObj **ppVnodes, int32_t numOfVnodes) {
  int32_t code = 0;
  SJson  *vnodes = tjsonCreateArray();
  if (vnodes == NULL) {
    return terrno;
  }
  if ((code = tjsonAddItemToObject(pJson, "vnodes", vnodes)) < 0) {
    tjsonDelete(vnodes);
    return code;
  };

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SVnodeObj *pVnode = ppVnodes[i];
    if (pVnode == NULL) continue;

    SJson *vnode = tjsonCreateObject();
    if (vnode == NULL) return terrno;
    if ((code = tjsonAddDoubleToObject(vnode, "vgId", pVnode->vgId)) < 0) return code;
    if ((code = tjsonAddDoubleToObject(vnode, "dropped", pVnode->dropped)) < 0) return code;
    if ((code = tjsonAddDoubleToObject(vnode, "vgVersion", pVnode->vgVersion)) < 0) return code;
    if ((code = tjsonAddDoubleToObject(vnode, "diskPrimary", pVnode->diskPrimary)) < 0) return code;
    if (pVnode->toVgId) {
      if ((code = tjsonAddDoubleToObject(vnode, "toVgId", pVnode->toVgId)) < 0) return code;
    }
    if ((code = tjsonAddItemToArray(vnodes, vnode)) < 0) return code;
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
  int32_t     lino = 0;
  int32_t     ret = -1;

  int32_t nBytes = snprintf(file, sizeof(file), "%s%svnodes_tmp.json", pMgmt->path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(file)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  nBytes = snprintf(realfile, sizeof(realfile), "%s%svnodes.json", pMgmt->path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(realfile)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t numOfVnodes = 0;
  TAOS_CHECK_GOTO(vmGetAllVnodeListFromHash(pMgmt, &numOfVnodes, &ppVnodes), &lino, _OVER);

  // terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(vmEncodeVnodeList(pJson, ppVnodes, numOfVnodes), &lino, _OVER);

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    lino = __LINE__;
    goto _OVER;
  }

  code = taosThreadMutexLock(&pMgmt->fileLock);
  if (code != 0) {
    lino = __LINE__;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _OVER1;
  }

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) {
    code = terrno;
    lino = __LINE__;
    goto _OVER1;
  }
  if (taosFsyncFile(pFile) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    lino = __LINE__;
    goto _OVER1;
  }

  code = taosCloseFile(&pFile);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    lino = __LINE__;
    goto _OVER1;
  }
  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), &lino, _OVER1);

  dInfo("succeed to write vnodes file:%s, vnodes:%d", realfile, numOfVnodes);

_OVER1:
  ret = taosThreadMutexUnlock(&pMgmt->fileLock);
  if (ret != 0) {
    dError("failed to unlock since %s", tstrerror(ret));
  }

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
    dError("failed to write vnodes file:%s at line:%d since %s, vnodes:%d", realfile, lino, tstrerror(code),
           numOfVnodes);
  }
  return code;
}

#ifdef USE_MOUNT
static int32_t vmDecodeMountList(SJson *pJson, SVnodeMgmt *pMgmt, SMountCfg **ppCfgs, int32_t *numOfMounts) {
  int32_t    code = 0, lino = 0;
  int32_t    mountsNum = 0;
  SMountCfg *pCfgs = NULL;
  SJson     *mounts = NULL;

  if (!(mounts = tjsonGetObjectItem(pJson, "mounts"))) {
    goto _exit;
  }
  if ((mountsNum = cJSON_GetArraySize(mounts)) > 0) {
    TSDB_CHECK_NULL((pCfgs = taosMemoryMalloc(mountsNum * sizeof(SMountCfg))), code, lino, _exit, terrno);
  }

  for (int32_t i = 0; i < mountsNum; ++i) {
    SJson *mount = tjsonGetArrayItem(mounts, i);
    TSDB_CHECK_NULL(mount, code, lino, _exit, TSDB_CODE_INVALID_JSON_FORMAT);
    SMountCfg *pCfg = &pCfgs[i];
    TAOS_CHECK_EXIT(tjsonGetBigIntValue(mount, "mountId", &pCfg->mountId));
    TAOS_CHECK_EXIT(tjsonGetStringValue2(mount, "name", pCfg->name, sizeof(pCfg->name)));
    TAOS_CHECK_EXIT(tjsonGetStringValue2(mount, "path", pCfg->path, sizeof(pCfg->path)));
  }
_exit:
  if (code) {
    dError("failed to decode mount list at line %d since %s", lino, tstrerror(code));
    if (pCfgs) {
      taosMemoryFree(pCfgs);
      pCfgs = NULL;
    }
  }
  *numOfMounts = mountsNum;
  *ppCfgs = pCfgs;
  return code;
}

int32_t vmGetMountListFromFile(SVnodeMgmt *pMgmt, SMountCfg **ppCfgs, int32_t *numOfMounts) {
  int32_t    code = 0, lino = 0;
  int64_t    size = 0;
  TdFilePtr  pFile = NULL;
  char      *pData = NULL;
  SJson     *pJson = NULL;
  char       file[PATH_MAX] = {0};
  SMountCfg *pCfgs = NULL;
  snprintf(file, sizeof(file), "%s%smounts.json", pMgmt->path, TD_DIRSEP);

  if (!taosCheckExistFile(file)) goto _exit;
  TAOS_CHECK_EXIT(taosStatFile(file, &size, NULL, NULL));
  TSDB_CHECK_NULL((pFile = taosOpenFile(file, TD_FILE_READ)), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((pData = taosMemoryMalloc(size + 1)), code, lino, _exit, terrno);
  if (taosReadFile(pFile, pData, size) != size) {
    TAOS_CHECK_EXIT(terrno);
  }
  pData[size] = '\0';
  TSDB_CHECK_NULL((pJson = tjsonParse(pData)), code, lino, _exit, TSDB_CODE_INVALID_JSON_FORMAT);
  TAOS_CHECK_EXIT(vmDecodeMountList(pJson, pMgmt, ppCfgs, numOfMounts));
  dInfo("succceed to read mounts file %s", file);
_exit:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (code != 0) {
    dError("failed to read mounts file:%s since %s", file, tstrerror(code));
  }
  return code;
}

static int32_t vmEncodeMountList(SVnodeMgmt *pMgmt, SJson *pJson) {
  int32_t    code = 0, lino = 0;
  SHashObj  *pTfsHash = pMgmt->mountTfsHash;
  int32_t    numOfMounts = 0;
  SJson     *mounts = NULL;
  SJson     *mount = NULL;
  SMountTfs *pTfs = NULL;

  if ((numOfMounts = taosHashGetSize(pTfsHash)) < 0) {
    goto _exit;
  }
  TSDB_CHECK_NULL((mounts = tjsonCreateArray()), code, lino, _exit, terrno);
  if ((code = tjsonAddItemToObject(pJson, "mounts", mounts))) {
    tjsonDelete(mounts);
    TAOS_CHECK_EXIT(code);
  }
  size_t keyLen = sizeof(int64_t);
  while ((pTfs = taosHashIterate(pTfsHash, pTfs))) {
    TSDB_CHECK_NULL((mount = tjsonCreateObject()), code, lino, _exit, terrno);
    if (!mount) TAOS_CHECK_EXIT(terrno);
    int64_t mountId = *(int64_t *)taosHashGetKey(pTfs, NULL);
    TAOS_CHECK_EXIT(tjsonAddIntegerToObject(mount, "mountId", mountId));
    TAOS_CHECK_EXIT(tjsonAddStringToObject(mount, "name", (*(SMountTfs **)pTfs)->name));
    TAOS_CHECK_EXIT(tjsonAddStringToObject(mount, "path", (*(SMountTfs **)pTfs)->path));
    TAOS_CHECK_EXIT(tjsonAddItemToArray(mounts, mount));
    mount = NULL;
  }
_exit:
  if (code != 0) {
    if (mount) tjsonDelete(mount);
    if (pTfs) taosHashCancelIterate(pTfsHash, pTfs);
    dError("failed to encode mount list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t vmWriteMountListToFile(SVnodeMgmt *pMgmt) {
  int32_t     code = 0, lino = 0, ret = 0;
  char       *buffer = NULL;
  SJson      *pJson = NULL;
  TdFilePtr   pFile = NULL;
  SVnodeObj **ppVnodes = NULL;
  char        file[PATH_MAX] = {0};
  char        realfile[PATH_MAX] = {0};
  bool        unlock = false;

  int32_t nBytes = snprintf(file, sizeof(file), "%s%smounts_tmp.json", pMgmt->path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(file)) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }
  nBytes = snprintf(realfile, sizeof(realfile), "%s%smounts.json", pMgmt->path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(realfile)) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_RANGE);
  }
  TSDB_CHECK_NULL((pJson = tjsonCreateObject()), code, lino, _exit, terrno);
  TAOS_CHECK_EXIT(vmEncodeMountList(pMgmt, pJson));
  TSDB_CHECK_NULL((buffer = tjsonToString(pJson)), code, lino, _exit, terrno);
  TAOS_CHECK_EXIT(taosThreadMutexLock(&pMgmt->fileLock));
  unlock = true;
  TSDB_CHECK_NULL((pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH)),
                  code, lino, _exit, terrno);

  int32_t len = strlen(buffer);
  if ((code = taosWriteFile(pFile, buffer, len)) <= 0) {
    TAOS_CHECK_EXIT(code);
  }
  TAOS_CHECK_EXIT(taosFsyncFile(pFile));
  TAOS_CHECK_EXIT(taosCloseFile(&pFile));
  TAOS_CHECK_EXIT(taosRenameFile(file, realfile));
  dInfo("succeed to write mounts file:%s", realfile);
_exit:
  if (unlock && (ret = taosThreadMutexUnlock(&pMgmt->fileLock))) {
    dError("failed to unlock at line %d when write mounts file since %s", __LINE__, tstrerror(ret));
  }
  if (pJson) tjsonDelete(pJson);
  if (buffer) taosMemoryFree(buffer);
  if (pFile) taosCloseFile(&pFile);
  if (code != 0) {
    dError("failed to write mounts file:%s at line:%d since %s", realfile, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t vmGetMountDisks(SVnodeMgmt *pMgmt, const char *mountPath, SArray **ppDisks) {
  int32_t   code = 0, lino = 0;
  SArray   *pDisks = NULL;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  int64_t   size = 0;
  int64_t   clusterId = 0, dropped = 0, encryptScope = 0;
  char      file[TSDB_MOUNT_FPATH_LEN] = {0};

  (void)snprintf(file, sizeof(file), "%s%s%s%sconfig%s%s", mountPath, TD_DIRSEP, dmNodeName(DNODE), TD_DIRSEP,
                 TD_DIRSEP, "local.json");
  TAOS_CHECK_EXIT(taosStatFile(file, &size, NULL, NULL));
  TSDB_CHECK_NULL((pFile = taosOpenFile(file, TD_FILE_READ)), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((content = taosMemoryMalloc(size + 1)), code, lino, _exit, terrno);
  if (taosReadFile(pFile, content, size) != size) {
    TAOS_CHECK_EXIT(terrno);
  }
  content[size] = '\0';
  pJson = tjsonParse(content);
  if (pJson == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  SJson *pConfigs = tjsonGetObjectItem(pJson, "configs");
  if (pConfigs == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  SJson *pDataDir = tjsonGetObjectItem(pConfigs, "dataDir");
  if (pDataDir == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  int32_t nDataDir = tjsonGetArraySize(pDataDir);
  if (!(pDisks = taosArrayInit_s(sizeof(SDiskCfg), nDataDir))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  for (int32_t i = 0; i < nDataDir; ++i) {
    char   dir[TSDB_MOUNT_PATH_LEN] = {0};
    SJson *pItem = tjsonGetArrayItem(pDataDir, i);
    if (pItem == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    code = tjsonGetStringValue(pItem, "dir", dir);
    if (code < 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    int32_t j = strlen(dir) - 1;
    while (j > 0 && (dir[j] == '/' || dir[j] == '\\')) {
      dir[j--] = '\0';  // remove trailing slashes
    }
    SJson *pLevel = tjsonGetObjectItem(pItem, "level");
    if (!pLevel) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    int32_t level = (int32_t)cJSON_GetNumberValue(pLevel);
    if (level < 0 || level >= TFS_MAX_TIERS) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    SJson *pPrimary = tjsonGetObjectItem(pItem, "primary");
    if (!pPrimary) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    int32_t primary = (int32_t)cJSON_GetNumberValue(pPrimary);
    if ((primary < 0 || primary > 1) || (primary == 1 && level != 0)) {
      dError("mount:%s, invalid primary disk, primary:%d, level:%d", mountPath, primary, level);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }
    int8_t    disable = (int8_t)cJSON_GetNumberValue(pLevel);
    SDiskCfg *pDisk = taosArrayGet(pDisks, i);
    pDisk->level = level;
    pDisk->primary = primary;
    pDisk->disable = disable;
    (void)snprintf(pDisk->dir, sizeof(pDisk->dir), "%s", dir);
  }
_exit:
  if (content != NULL) taosMemoryFreeClear(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (code != 0) {
    dError("failed to get mount disks at line %d since %s, path:%s", lino, tstrerror(code), mountPath);
    taosArrayDestroy(pDisks);
    pDisks = NULL;
  }
  *ppDisks = pDisks;
  TAOS_RETURN(code);
}

#endif