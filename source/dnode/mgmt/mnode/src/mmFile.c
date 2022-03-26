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

int32_t mmReadFile(SMnodeMgmt *pMgmt, bool *pDeployed) {
  int32_t   code = TSDB_CODE_NODE_PARSE_FILE_ERROR;
  int32_t   len = 0;
  int32_t   maxLen = 4096;
  char     *content = taosMemoryCalloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX];
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%smnode.json", pMgmt->path, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_MNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_MNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_MNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_MNODE_OVER;
  }
  *pDeployed = deployed->valueint;

  cJSON *mnodes = cJSON_GetObjectItem(root, "mnodes");
  if (!mnodes || mnodes->type != cJSON_Array) {
    dError("failed to read %s since nodes not found", file);
    goto PRASE_MNODE_OVER;
  }

  pMgmt->replica = cJSON_GetArraySize(mnodes);
  if (pMgmt->replica <= 0 || pMgmt->replica > TSDB_MAX_REPLICA) {
    dError("failed to read %s since mnodes size %d invalid", file, pMgmt->replica);
    goto PRASE_MNODE_OVER;
  }

  for (int32_t i = 0; i < pMgmt->replica; ++i) {
    cJSON *node = cJSON_GetArrayItem(mnodes, i);
    if (node == NULL) break;

    SReplica *pReplica = &pMgmt->replicas[i];

    cJSON *id = cJSON_GetObjectItem(node, "id");
    if (!id || id->type != cJSON_Number) {
      dError("failed to read %s since id not found", file);
      goto PRASE_MNODE_OVER;
    }
    pReplica->id = id->valueint;

    cJSON *fqdn = cJSON_GetObjectItem(node, "fqdn");
    if (!fqdn || fqdn->type != cJSON_String || fqdn->valuestring == NULL) {
      dError("failed to read %s since fqdn not found", file);
      goto PRASE_MNODE_OVER;
    }
    tstrncpy(pReplica->fqdn, fqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *port = cJSON_GetObjectItem(node, "port");
    if (!port || port->type != cJSON_Number) {
      dError("failed to read %s since port not found", file);
      goto PRASE_MNODE_OVER;
    }
    pReplica->port = port->valueint;
  }

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d", file, *pDeployed);

PRASE_MNODE_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

int32_t mmWriteFile(SMnodeMgmt *pMgmt, bool deployed) {
  char file[PATH_MAX];
  snprintf(file, sizeof(file), "%s%smnode.json.bak", pMgmt->path, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);;
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 4096;
  char   *content = taosMemoryCalloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d,\n", deployed);
  len += snprintf(content + len, maxLen - len, "  \"mnodes\": [{\n");
  for (int32_t i = 0; i < pMgmt->replica; ++i) {
    SReplica *pReplica = &pMgmt->replicas[i];
    len += snprintf(content + len, maxLen - len, "    \"id\": %d,\n", pReplica->id);
    len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\",\n", pReplica->fqdn);
    len += snprintf(content + len, maxLen - len, "    \"port\": %u\n", pReplica->port);
    if (i < pMgmt->replica - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(content);

  char realfile[PATH_MAX];
  snprintf(realfile, sizeof(realfile), "%s%smnode.json", pMgmt->path, TD_DIRSEP);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);;
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d", realfile, deployed);
  return 0;
}
