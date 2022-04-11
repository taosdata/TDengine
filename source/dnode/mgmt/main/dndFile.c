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
#include "dndInt.h"

#define MAXLEN 1024

int32_t dndReadFile(SMgmtWrapper *pWrapper, bool *pDeployed) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int64_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  cJSON    *root = NULL;
  char      file[PATH_MAX];
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s%s.json", pWrapper->path, TD_DIRSEP, pWrapper->name);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto _OVER;
  }

  len = taosReadFile(pFile, content, MAXLEN);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto _OVER;
  }

  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto _OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto _OVER;
  }
  *pDeployed = deployed->valueint != 0;

  dDebug("succcessed to read file %s, deployed:%d", file, *pDeployed);
  code = 0;

_OVER:
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

int32_t dndWriteFile(SMgmtWrapper *pWrapper, bool deployed) {
  int32_t   code = -1;
  int32_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s%s.json", pWrapper->path, TD_DIRSEP, pWrapper->name);
  snprintf(realfile, sizeof(realfile), "%s%s%s.json", pWrapper->path, TD_DIRSEP, pWrapper->name);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write %s since %s", file, terrstr());
    goto _OVER;
  }

  len += snprintf(content + len, MAXLEN - len, "{\n");
  len += snprintf(content + len, MAXLEN - len, "  \"deployed\": %d\n", deployed);
  len += snprintf(content + len, MAXLEN - len, "}\n");

  if (taosWriteFile(pFile, content, len) != len) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write file:%s since %s", file, terrstr());
    goto _OVER;
  }

  if (taosFsyncFile(pFile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fsync file:%s since %s", file, terrstr());
    goto _OVER;
  }

  taosCloseFile(&pFile);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d", realfile, deployed);
  code = 0;

_OVER:
  if (pFile != NULL) {
    taosCloseFile(&pFile);
  }

  return code;
}

TdFilePtr dndCheckRunning(const char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.running", dataDir, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s", filepath, terrstr());
    return NULL;
  }

  int32_t ret = taosLockFile(pFile);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s", filepath, terrstr());
    taosCloseFile(&pFile);
    return NULL;
  }

  dDebug("file:%s is locked", filepath);
  return pFile;
}

int32_t dndReadShmFile(SDnode *pDnode) {
  int32_t   code = -1;
  char      itemName[24] = {0};
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  cJSON    *root = NULL;
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s.shmfile", pDnode->dataDir, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, MAXLEN) > 0) {
    root = cJSON_Parse(content);
    if (root == NULL) {
      terrno = TSDB_CODE_INVALID_JSON_FORMAT;
      dError("failed to read %s since invalid json format", file);
      goto _OVER;
    }

    for (EDndType ntype = DNODE + 1; ntype < NODE_MAX; ++ntype) {
      snprintf(itemName, sizeof(itemName), "%s_shmid", dndNodeProcStr(ntype));
      cJSON *shmid = cJSON_GetObjectItem(root, itemName);
      if (shmid && shmid->type == cJSON_Number) {
        pDnode->wrappers[ntype].shm.id = shmid->valueint;
      }

      snprintf(itemName, sizeof(itemName), "%s_shmsize", dndNodeProcStr(ntype));
      cJSON *shmsize = cJSON_GetObjectItem(root, itemName);
      if (shmsize && shmsize->type == cJSON_Number) {
        pDnode->wrappers[ntype].shm.size = shmsize->valueint;
      }
    }
  }

  if (!tsMultiProcess || pDnode->ntype == DNODE || pDnode->ntype == NODE_MAX) {
    for (EDndType ntype = DNODE; ntype < NODE_MAX; ++ntype) {
      SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
      if (pWrapper->shm.id >= 0) {
        dDebug("shmid:%d, is closed, size:%d", pWrapper->shm.id, pWrapper->shm.size);
        taosDropShm(&pWrapper->shm);
      }
    }
  } else {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->ntype];
    if (taosAttachShm(&pWrapper->shm) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("shmid:%d, failed to attach shm since %s", pWrapper->shm.id, terrstr());
      goto _OVER;
    }
    dInfo("node:%s, shmid:%d is attached, size:%d", pWrapper->name, pWrapper->shm.id, pWrapper->shm.size);
  }

  dDebug("successed to load %s", file);
  code = 0;

_OVER:
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  return code;
}

int32_t dndWriteShmFile(SDnode *pDnode) {
  int32_t   code = -1;
  int32_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s.shmfile.bak", pDnode->dataDir, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%s.shmfile", pDnode->dataDir, TD_DIRSEP);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open file:%s since %s", file, terrstr());
    goto _OVER;
  }

  len += snprintf(content + len, MAXLEN - len, "{\n");
  for (EDndType ntype = DNODE + 1; ntype < NODE_MAX; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    len += snprintf(content + len, MAXLEN - len, "  \"%s_shmid\":%d,\n", dndNodeProcStr(ntype), pWrapper->shm.id);
    if (ntype == NODE_MAX - 1) {
      len += snprintf(content + len, MAXLEN - len, "  \"%s_shmsize\":%d\n", dndNodeProcStr(ntype), pWrapper->shm.size);
    } else {
      len += snprintf(content + len, MAXLEN - len, "  \"%s_shmsize\":%d,\n", dndNodeProcStr(ntype), pWrapper->shm.size);
    }
  }
  len += snprintf(content + len, MAXLEN - len, "}\n");

  if (taosWriteFile(pFile, content, len) != len) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write file:%s since %s", file, terrstr());
    goto _OVER;
  }

  if (taosFsyncFile(pFile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fsync file:%s since %s", file, terrstr());
    goto _OVER;
  }

  taosCloseFile(&pFile);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to rename %s to %s since %s", file, realfile, terrstr());
    return -1;
  }

  dInfo("successed to write %s", realfile);
  code = 0;

_OVER:
  if (pFile != NULL) {
    taosCloseFile(&pFile);
  }

  return code;
}
