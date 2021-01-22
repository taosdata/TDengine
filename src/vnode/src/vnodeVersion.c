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

#define _DEFAULT_SOURCE
#include "os.h"
#include "cJSON.h"
#include "tglobal.h"
#include "vnodeVersion.h"

int32_t vnodeReadVersion(SVnodeObj *pVnode) {
  int32_t len = 0;
  int32_t maxLen = 100;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;

  terrno = TSDB_CODE_VND_INVALID_VRESION_FILE;
  char file[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(file, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);

  fp = fopen(file, "r");
  if (!fp) {
    if (errno != ENOENT) {
      vError("vgId:%d, failed to read %s, error:%s", pVnode->vgId, file, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_SUCCESS;
    }
    goto PARSE_VER_ERROR;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read %s, content is null", pVnode->vgId, file);
    goto PARSE_VER_ERROR;
  }

  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read %s, invalid json format", pVnode->vgId, file);
    goto PARSE_VER_ERROR;
  }

  cJSON *ver = cJSON_GetObjectItem(root, "version");
  if (!ver || ver->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, version not found", pVnode->vgId, file);
    goto PARSE_VER_ERROR;
  }
  pVnode->version = (uint64_t)ver->valueint;

  terrno = TSDB_CODE_SUCCESS;
  vInfo("vgId:%d, read %s successfully, fver:%" PRIu64, pVnode->vgId, file, pVnode->version);

PARSE_VER_ERROR:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  return terrno;
}

int32_t vnodeSaveVersion(SVnodeObj *pVnode) {
  char file[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(file, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    vError("vgId:%d, failed to write %s, reason:%s", pVnode->vgId, file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 100;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"version\": %" PRIu64 "\n", pVnode->fversion);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  terrno = 0;

  vInfo("vgId:%d, successed to write %s, fver:%" PRIu64, pVnode->vgId, file, pVnode->fversion);
  return TSDB_CODE_SUCCESS;
}