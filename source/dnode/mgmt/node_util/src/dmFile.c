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
#include "dmUtil.h"

#define MAXLEN 1024

int32_t dmReadFile(const char *path, const char *name, bool *pDeployed) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int64_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
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

int32_t dmWriteFile(const char *path, const char *name, bool deployed) {
  int32_t   code = -1;
  int32_t   len = 0;
  char      content[MAXLEN + 1] = {0};
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  snprintf(realfile, sizeof(realfile), "%s%s%s.json", path, TD_DIRSEP, name);

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

  dInfo("succeed to write %s, deployed:%d", realfile, deployed);
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

  int32_t retryTimes = 0;
  int32_t ret = 0;
  do {
    ret = taosLockFile(pFile);
    if (ret == 0) break;
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosMsleep(1000);
    retryTimes++;
    dError("failed to lock file:%s since %s, retryTimes:%d", filepath, terrstr(), retryTimes);
  } while (retryTimes < 12);

  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFile);
    return NULL;
  }

  terrno = 0;
  dDebug("lock file:%s to prevent repeated starts", filepath);
  return pFile;
}
