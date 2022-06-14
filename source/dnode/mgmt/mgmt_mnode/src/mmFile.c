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

int32_t mmReadFile(SMnodeMgmt *pMgmt, SReplica *pReplica, bool *pDeployed) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t   len = 0;
  int32_t   maxLen = 4096;
  char     *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%smnode.json", pMgmt->path, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = 0;
    goto _OVER;
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

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto _OVER;
  }
  *pDeployed = deployed->valueint;

  cJSON *id = cJSON_GetObjectItem(root, "id");
  if (id) {
    if (id->type != cJSON_Number) {
      dError("failed to read %s since id not found", file);
      goto _OVER;
    }
    if (pReplica) {
      pReplica->id = id->valueint;
    }
  }

  cJSON *fqdn = cJSON_GetObjectItem(root, "fqdn");
  if (fqdn) {
    if (fqdn->type != cJSON_String || fqdn->valuestring == NULL) {
      dError("failed to read %s since fqdn not found", file);
      goto _OVER;
    }
    if (pReplica) {
      tstrncpy(pReplica->fqdn, fqdn->valuestring, TSDB_FQDN_LEN);
    }
  }

  cJSON *port = cJSON_GetObjectItem(root, "port");
  if (port) {
    if (port->type != cJSON_Number) {
      dError("failed to read %s since port not found", file);
      goto _OVER;
    }
    if (pReplica) {
      pReplica->port = (uint16_t)port->valueint;
    }
  }

  code = 0;

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (code == 0) {
    dDebug("succcessed to read file %s, deployed:%d", file, *pDeployed);
  }

  terrno = code;
  return code;
}

int32_t mmWriteFile(SMnodeMgmt *pMgmt, const SReplica *pReplica, bool deployed) {
  char file[PATH_MAX] = {0};
  char realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%smnode.json.bak", pMgmt->path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%smnode.json", pMgmt->path, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 4096;
  char   *content = taosMemoryCalloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  if (pReplica != NULL && pReplica->id > 0) {
    len += snprintf(content + len, maxLen - len, "  \"id\": %d,\n", pReplica->id);
    len += snprintf(content + len, maxLen - len, "  \"fqdn\": \"%s\",\n", pReplica->fqdn);
    len += snprintf(content + len, maxLen - len, "  \"port\": %u\n,", pReplica->port);
  }
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d\n", deployed);
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(content);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dDebug("successed to write %s, deployed:%d", realfile, deployed);
  return 0;
}
