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
#include "bmInt.h"

int32_t bmReadFile(SBnodeMgmt *pMgmt, bool *pDeployed) {
  int32_t   code = TSDB_CODE_DND_BNODE_READ_FILE_ERROR;
  int32_t   len = 0;
  int32_t   maxLen = 1024;
  char     *content = calloc(1, maxLen + 1);
  cJSON    *root = NULL;
  char      file[PATH_MAX];
  TdFilePtr pFile = NULL;

  snprintf(file, sizeof(file), "%s%sbnode.json", pMgmt->path, TD_DIRSEP);
  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    dDebug("file %s not exist", file);
    code = 0;
    goto PRASE_BNODE_OVER;
  }

  len = (int32_t)taosReadFile(pFile, content, maxLen);
  if (len <= 0) {
    dError("failed to read %s since content is null", file);
    goto PRASE_BNODE_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", file);
    goto PRASE_BNODE_OVER;
  }

  cJSON *deployed = cJSON_GetObjectItem(root, "deployed");
  if (!deployed || deployed->type != cJSON_Number) {
    dError("failed to read %s since deployed not found", file);
    goto PRASE_BNODE_OVER;
  }
  *pDeployed = deployed->valueint != 0;

  code = 0;
  dDebug("succcessed to read file %s, deployed:%d", file, *pDeployed);

PRASE_BNODE_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (pFile != NULL) taosCloseFile(&pFile);

  terrno = code;
  return code;
}

int32_t bmWriteFile(SBnodeMgmt *pMgmt, bool deployed) {
  char file[PATH_MAX];
  snprintf(file, sizeof(file), "%s%sbnode.json", pMgmt->path, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR;
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 1024;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"deployed\": %d\n", deployed);
  len += snprintf(content + len, maxLen - len, "}\n");

  taosWriteFile(pFile, content, len);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  free(content);

  char realfile[PATH_MAX];
  snprintf(realfile, sizeof(realfile), "%s%sbnode.json", pMgmt->path, TD_DIRSEP);

  if (taosRenameFile(file, realfile) != 0) {
    terrno = TSDB_CODE_DND_BNODE_WRITE_FILE_ERROR;
    dError("failed to rename %s since %s", file, terrstr());
    return -1;
  }

  dInfo("successed to write %s, deployed:%d", realfile, deployed);
  return 0;
}
