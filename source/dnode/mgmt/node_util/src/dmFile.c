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
#include "tjson.h"

#define MAXLEN 1024

static int32_t dmDecodeFile(SJson *pJson, bool *deployed) {
  int32_t code = 0;
  int32_t value = 0;

  tjsonGetInt32ValueFromDouble(pJson, "deployed", value, code);
  if (code < 0) return -1;

  *deployed = (value != 0);
  return code;
}

int32_t dmReadFile(const char *path, const char *name, bool *pDeployed) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("file:%s not exist", file);
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (dmDecodeFile(pJson, pDeployed) < 0) {
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
  return code;
}

static int32_t dmEncodeFile(SJson *pJson, bool deployed) {
  if (tjsonAddDoubleToObject(pJson, "deployed", deployed) < 0) return -1;
  return 0;
}

int32_t dmWriteFile(const char *path, const char *name, bool deployed) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  snprintf(realfile, sizeof(realfile), "%s%s%s.json", path, TD_DIRSEP, name);

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (dmEncodeFile(pJson, deployed) != 0) goto _OVER;
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
  dInfo("succeed to write file:%s, deloyed:%d", realfile, deployed);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write file:%s since %s, deloyed:%d", realfile, terrstr(), deployed);
  }
  return code;
}

TdFilePtr dmCheckRunning(const char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.running", dataDir, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC);
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
