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

#include "vnodeInt.h"

#define VND_INFO_FNAME     "vnode.json"
#define VND_INFO_FNAME_TMP "vnode_tmp.json"

static int vnodeEncodeInfo(const SVnodeCfg *pCfg, uint8_t **ppData, int *len);
static int vnodeDecodeInfo(uint8_t *pData, int len, SVnodeCfg *pCfg);

const SVnodeCfg vnodeCfgDefault = {
    .wsize = 96 * 1024 * 1024, .ssize = 1 * 1024 * 1024, .lsize = 1024, .walCfg = {.level = TAOS_WAL_WRITE}};

int vnodeCheckCfg(const SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

int vnodeSaveCfg(const char *dir, const SVnodeCfg *pCfg) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  uint8_t  *data;
  int       len;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  // encode info
  data = NULL;
  len = 0;

  if (vnodeEncodeInfo(pCfg, &data, &len) < 0) {
    return -1;
  }

  // save info to a vnode_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, len) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFile) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFile);

  // free info binary
  taosMemoryFree(data);

  vInfo("vgId: %d vnode info is saved, fname: %s", pCfg->vgId, fname);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(data);
  return -1;
}

int vnodeCommitCfg(const char *dir) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  if (taosRenameFile(tfname, fname) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int vnodeLoadCfg(const char *dir) {
  // TODO
  return 0;
}

static int vnodeEncodeInfo(const SVnodeCfg *pCfg, uint8_t **ppData, int *len) {
  // TODO
  return 0;
}

static int vnodeDecodeInfo(uint8_t *pData, int len, SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

#if 1  //======================================================================
void vnodeOptionsCopy(SVnodeCfg *pDest, const SVnodeCfg *pSrc) {
  memcpy((void *)pDest, (void *)pSrc, sizeof(SVnodeCfg));
}

int vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName) {
  uint32_t hashValue = 0;

  switch (pVnodeOptions->hashMethod) {
    default:
      hashValue = MurmurHash3_32(tableFName, strlen(tableFName));
      break;
  }

    // TODO OPEN THIS !!!!!!!
#if 0
  if (hashValue < pVnodeOptions->hashBegin || hashValue > pVnodeOptions->hashEnd) {
    terrno = TSDB_CODE_VND_HASH_MISMATCH;
    return TSDB_CODE_VND_HASH_MISMATCH;
  }
#endif

  return TSDB_CODE_SUCCESS;
}

#endif