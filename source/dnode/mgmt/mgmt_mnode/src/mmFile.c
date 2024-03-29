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
#include "mmInt.h"
#include "tjson.h"

static int32_t mmDecodeOption(SJson *pJson, SMnodeOpt *pOption) {
  int32_t code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "deployed", pOption->deploy, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "selfIndex", pOption->selfIndex, code);
  if (code < 0) return 0;
  tjsonGetInt32ValueFromDouble(pJson, "lastIndex", pOption->lastIndex, code);
  if (code < 0) return 0;

  SJson *replicas = tjsonGetObjectItem(pJson, "replicas");
  if (replicas == NULL) return 0;
  pOption->numOfTotalReplicas = tjsonGetArraySize(replicas);

  pOption->numOfReplicas = 0;

  for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
    SJson *replica = tjsonGetArrayItem(replicas, i);
    if (replica == NULL) return -1;

    SReplica *pReplica = pOption->replicas + i;
    tjsonGetInt32ValueFromDouble(replica, "id", pReplica->id, code);
    if (code < 0) return -1;
    code = tjsonGetStringValue(replica, "fqdn", pReplica->fqdn);
    if (code < 0) return -1;
    tjsonGetUInt16ValueFromDouble(replica, "port", pReplica->port, code);
    if (code < 0) return -1;
    tjsonGetInt32ValueFromDouble(replica, "role", pOption->nodeRoles[i], code);
    if (code < 0) return -1;
    if (pOption->nodeRoles[i] == TAOS_SYNC_ROLE_VOTER) {
      pOption->numOfReplicas++;
    }
  }

  for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
  }

  return 0;
}

int32_t mmReadFile(const char *path, SMnodeOpt *pOption) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%smnode.json", path, TD_DIRSEP);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("mnode file:%s not exist", file);
    return 0;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open mnode file:%s since %s", file, terrstr());
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
    dError("failed to read mnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (mmDecodeOption(pJson, pOption) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read mnode file %s", file);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read mnode file:%s since %s", file, terrstr());
  }
  return code;
}

static int32_t mmEncodeOption(SJson *pJson, const SMnodeOpt *pOption) {
  if (pOption->deploy && pOption->numOfTotalReplicas > 0) {
    if (tjsonAddDoubleToObject(pJson, "selfIndex", pOption->selfIndex) < 0) return -1;

    SJson *replicas = tjsonCreateArray();
    if (replicas == NULL) return -1;
    if (tjsonAddItemToObject(pJson, "replicas", replicas) < 0) return -1;

    for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
      SJson *replica = tjsonCreateObject();
      if (replica == NULL) return -1;

      const SReplica *pReplica = pOption->replicas + i;
      if (tjsonAddDoubleToObject(replica, "id", pReplica->id) < 0) return -1;
      if (tjsonAddStringToObject(replica, "fqdn", pReplica->fqdn) < 0) return -1;
      if (tjsonAddDoubleToObject(replica, "port", pReplica->port) < 0) return -1;
      if (tjsonAddDoubleToObject(replica, "role", pOption->nodeRoles[i]) < 0) return -1;
      if (tjsonAddItemToArray(replicas, replica) < 0) return -1;
    }
  }

  if (tjsonAddDoubleToObject(pJson, "lastIndex", pOption->lastIndex) < 0) return -1;

  if (tjsonAddDoubleToObject(pJson, "deployed", pOption->deploy) < 0) return -1;

  return 0;
}

int32_t mmWriteFile(const char *path, const SMnodeOpt *pOption) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%smnode.json.bak", path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%smnode.json", path, TD_DIRSEP);

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (mmEncodeOption(pJson, pOption) != 0) goto _OVER;
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
  dInfo("succeed to write mnode file:%s, deloyed:%d", realfile, pOption->deploy);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write mnode file:%s since %s, deloyed:%d", realfile, terrstr(), pOption->deploy);
  }
  return code;
}
