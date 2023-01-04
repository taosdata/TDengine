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

int32_t mmReadFile(const char *path, SMnodeOpt *pOption) {
  int32_t   code = TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t   len = 0;
  int32_t   maxLen = 4096;
  char     *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%smnode.json", path, TD_DIRSEP);
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
  pOption->deploy = deployed->valueint;

  cJSON *selfIndex = cJSON_GetObjectItem(root, "selfIndex");
  if (selfIndex) {
    if (selfIndex->type != cJSON_Number) {
      dError("failed to read %s since selfIndex not found", file);
      goto _OVER;
    }
    pOption->selfIndex = selfIndex->valueint;
  }

  cJSON *replicas = cJSON_GetObjectItem(root, "replicas");
  if (replicas) {
    if (replicas->type != cJSON_Array) {
      dError("failed to read %s since replicas not found", file);
      goto _OVER;
    }

    int32_t numOfReplicas = cJSON_GetArraySize(replicas);
    if (numOfReplicas <= 0) {
      dError("failed to read %s since numOfReplicas:%d invalid", file, numOfReplicas);
      goto _OVER;
    }
    pOption->numOfReplicas = numOfReplicas;

    for (int32_t i = 0; i < numOfReplicas; ++i) {
      SReplica *pReplica = pOption->replicas + i;

      cJSON *replica = cJSON_GetArrayItem(replicas, i);
      if (replica == NULL) break;

      cJSON *id = cJSON_GetObjectItem(replica, "id");
      if (id) {
        if (id->type != cJSON_Number) {
          dError("failed to read %s since id not found", file);
          goto _OVER;
        }
        if (pReplica) {
          pReplica->id = id->valueint;
        }
      }

      cJSON *fqdn = cJSON_GetObjectItem(replica, "fqdn");
      if (fqdn) {
        if (fqdn->type != cJSON_String || fqdn->valuestring == NULL) {
          dError("failed to read %s since fqdn not found", file);
          goto _OVER;
        }
        if (pReplica) {
          tstrncpy(pReplica->fqdn, fqdn->valuestring, TSDB_FQDN_LEN);
        }
      }

      cJSON *port = cJSON_GetObjectItem(replica, "port");
      if (port) {
        if (port->type != cJSON_Number) {
          dError("failed to read %s since port not found", file);
          goto _OVER;
        }
        if (pReplica) {
          pReplica->port = (uint16_t)port->valueint;
        }
      }
    }
  }

  code = 0;

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (code == 0) {
    dDebug("succcessed to read file %s, deployed:%d", file, pOption->deploy);
  }

  terrno = code;
  return code;
}

int32_t mmWriteFile(const char *path, const SMnodeOpt *pOption) {
  char file[PATH_MAX] = {0};
  char realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%smnode.json.bak", path, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%smnode.json", path, TD_DIRSEP);

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
  if (pOption->deploy && pOption->numOfReplicas > 0) {
    len += snprintf(content + len, maxLen - len, "  \"selfIndex\": %d,\n", pOption->selfIndex);
    len += snprintf(content + len, maxLen - len, "  \"replicas\": [{\n");

    for (int32_t i = 0; i < pOption->numOfReplicas; ++i) {
      const SReplica *pReplica = pOption->replicas + i;
      if (pReplica != NULL && pReplica->id > 0) {
        len += snprintf(content + len, maxLen - len, "    \"id\": %d,\n", pReplica->id);
        len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\",\n", pReplica->fqdn);
        len += snprintf(content + len, maxLen - len, "    \"port\": %u\n", pReplica->port);
      }
      if (i < pOption->numOfReplicas - 1) {
        len += snprintf(content + len, maxLen - len, "  },{\n");
      } else {
        len += snprintf(content + len, maxLen - len, "  }],\n");
      }
    }
  }
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d\n", pOption->deploy);
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

  dDebug("succeed to write %s, deployed:%d", realfile, pOption->deploy);
  return 0;
}
