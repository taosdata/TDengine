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
#include "dmInt.h"

#define MAXLEN 1024

int32_t dmReadFile(SMgmtWrapper *pWrapper, bool *pDeployed) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int64_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s%s.json", pWrapper->path, TD_DIRSEP, pWrapper->name);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    // dDebug("file %s not exist", file);
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

int32_t dmWriteFile(SMgmtWrapper *pWrapper, bool deployed) {
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

TdFilePtr dmCheckRunning(const char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.running", dataDir, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open file:%s since %s", filepath, terrstr());
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

int32_t dmReadShmFile(SMgmtWrapper *pWrapper) {
  int32_t   code = -1;
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  cJSON    *root = NULL;
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%sshmfile", pWrapper->path, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    // dDebug("node:%s, file %s not exist", pWrapper->name, file);
    code = 0;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, MAXLEN) > 0) {
    root = cJSON_Parse(content);
    if (root == NULL) {
      terrno = TSDB_CODE_INVALID_JSON_FORMAT;
      dError("node:%s, failed to read %s since invalid json format", pWrapper->name, file);
      goto _OVER;
    }

    cJSON *shmid = cJSON_GetObjectItem(root, "shmid");
    if (shmid && shmid->type == cJSON_Number) {
      pWrapper->procShm.id = shmid->valueint;
    }

    cJSON *shmsize = cJSON_GetObjectItem(root, "shmsize");
    if (shmsize && shmsize->type == cJSON_Number) {
      pWrapper->procShm.size = shmsize->valueint;
    }
  }

  if (!tsMultiProcess || pWrapper->pDnode->ntype == DNODE || pWrapper->pDnode->ntype == NODE_END) {
    if (pWrapper->procShm.id >= 0) {
      dDebug("node:%s, shmid:%d, is closed, size:%d", pWrapper->name, pWrapper->procShm.id, pWrapper->procShm.size);
      taosDropShm(&pWrapper->procShm);
    }
  } else {
    if (taosAttachShm(&pWrapper->procShm) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("shmid:%d, failed to attach shm since %s", pWrapper->procShm.id, terrstr());
      goto _OVER;
    }
    dInfo("node:%s, shmid:%d is attached, size:%d", pWrapper->name, pWrapper->procShm.id, pWrapper->procShm.size);
  }

  dDebug("node:%s, successed to load %s", pWrapper->name, file);
  code = 0;

_OVER:
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  return code;
}

int32_t dmWriteShmFile(SMgmtWrapper *pWrapper) {
  int32_t   code = -1;
  int32_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%sshmfile.bak", pWrapper->path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%sshmfile", pWrapper->path, TD_DIRSEP);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to open file:%s since %s", pWrapper->name, file, terrstr());
    goto _OVER;
  }

  len += snprintf(content + len, MAXLEN - len, "{\n");
  len += snprintf(content + len, MAXLEN - len, "  \"shmid\":%d,\n", pWrapper->procShm.id);
  len += snprintf(content + len, MAXLEN - len, "  \"shmsize\":%d\n", pWrapper->procShm.size);
  len += snprintf(content + len, MAXLEN - len, "}\n");

  if (taosWriteFile(pFile, content, len) != len) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to write file:%s since %s", pWrapper->name, file, terrstr());
    goto _OVER;
  }

  if (taosFsyncFile(pFile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to fsync file:%s since %s", pWrapper->name, file, terrstr());
    goto _OVER;
  }

  taosCloseFile(&pFile);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to rename %s to %s since %s", pWrapper->name, file, realfile, terrstr());
    return -1;
  }

  dInfo("node:%s, successed to write %s", pWrapper->name, realfile);
  code = 0;

_OVER:
  if (pFile != NULL) {
    taosCloseFile(&pFile);
  }

  return code;
}
