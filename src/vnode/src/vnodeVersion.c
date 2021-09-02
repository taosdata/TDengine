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
    vError("vgId:%d, failed to read %s, version invalid type", pVnode->vgId, file);
    goto PARSE_VER_ERROR;
  }
  pVnode->version = (uint64_t)ver->valueint;

  cJSON *off = cJSON_GetObjectItem(root, "offset");
  if (off) {
    if (off->type != cJSON_Number) {
      vError("vgId:%d, failed to read %s, offset invalid type", pVnode->vgId, file);
      goto PARSE_VER_ERROR;
    }
    pVnode->fOffset = (uint64_t)off->valueint;
    pVnode->offset = pVnode->fOffset;
  } else {
    pVnode->fOffset = 0;
    pVnode->offset = 0;
  }

  cJSON *startFileId = cJSON_GetObjectItem(root, "startFileId");
  if (startFileId && startFileId->type == cJSON_Number) {
    pVnode->startFileId = (int32_t)startFileId->valueint;
  } else {
    vError("vgId:%d, failed to read %s, invalid field startFileId", pVnode->vgId, file);
    pVnode->startFileId = -1;
  }

  cJSON *restoreFileId = cJSON_GetObjectItem(root, "restoreFileId");
  if (restoreFileId && restoreFileId->type == cJSON_Number) {
    pVnode->restoreFileId = (int32_t)restoreFileId->valueint;
  } else {
    vError("vgId:%d, failed to read %s, invalid field restoreFileId", pVnode->vgId, file);
    pVnode->restoreFileId = -1;
  }

  cJSON *writeFileId = cJSON_GetObjectItem(root, "writeFileId");
  if (writeFileId && writeFileId->type == cJSON_Number) {
    pVnode->writeFileId = (int32_t)writeFileId->valueint;
  } else {
    vError("vgId:%d, failed to read %s, invalid field writeFileId", pVnode->vgId, file);
    pVnode->writeFileId = -1;
  }

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
  len += snprintf(content + len, maxLen - len, "  \"version\": %" PRIu64 ",\n", pVnode->fversion);
  len += snprintf(content + len, maxLen - len, "  \"offset\": %" PRIu64 ",\n", pVnode->fOffset);
  len += snprintf(content + len, maxLen - len, "  \"startFileId\": %" PRId32 ",\n", pVnode->startFileId);
  len += snprintf(content + len, maxLen - len, "  \"restoreFileId\": %" PRId32 ",\n", pVnode->restoreFileId);
  len += snprintf(content + len, maxLen - len, "  \"writeFileId\": %" PRId32 "\n", pVnode->writeFileId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsync(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  vInfo("vgId:%d, successed to write %s, fver:%" PRIu64, pVnode->vgId, file, pVnode->fversion);
  return TSDB_CODE_SUCCESS;
}
