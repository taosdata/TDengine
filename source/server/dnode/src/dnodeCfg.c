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
#include "dnodeCfg.h"

static struct DnCfg {
  int32_t         dnodeId;
  int32_t         dropped;
  char            clusterId[TSDB_CLUSTER_ID_LEN];
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} tsDcfg;

static int32_t dnodeReadCfg() {
  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;

  fp = fopen(tsDcfg.file, "r");
  if (!fp) {
    dDebug("file %s not exist", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s since dnodeId not found", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }
  tsDcfg.dnodeId = (int32_t)dnodeId->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }
  tsDcfg.dropped = (int32_t)dropped->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", tsDcfg.file);
    goto PARSE_CFG_OVER;
  }
  tstrncpy(tsDcfg.clusterId, clusterId->valuestring, TSDB_CLUSTER_ID_LEN);

  dInfo("successed to read %s", tsDcfg.file);

PARSE_CFG_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  return 0;
}

static int32_t dnodeWriteCfg() {
  FILE *fp = fopen(tsDcfg.file, "w");
  if (!fp) {
    dError("failed to write %s since %s", tsDcfg.file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", tsDcfg.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d,\n", tsDcfg.dropped);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%s\"\n", tsDcfg.clusterId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", tsDcfg.file);
  return 0;
}

int32_t dnodeInitCfg() {
  tsDcfg.dnodeId = 0;
  tsDcfg.dropped = 0;
  tsDcfg.clusterId[0] = 0;
  snprintf(tsDcfg.file, sizeof(tsDcfg.file), "%s/dnodeCfg.json", tsDnodeDir);
  pthread_mutex_init(&tsDcfg.mutex, NULL);
  
  int32_t ret = dnodeReadCfg();
  if (ret == 0) {
    dInfo("dnode cfg is initialized");
  }

  if (tsDcfg.dropped) {
    dInfo("dnode is dropped and start to exit");
    return -1;
  }

  return ret;
}

void dnodeCleanupCfg() {
  pthread_mutex_destroy(&tsDcfg.mutex);
}

void dnodeUpdateCfg(SDnodeCfg *data) {
  if (tsDcfg.dnodeId != 0) return;

  pthread_mutex_lock(&tsDcfg.mutex);

  tsDcfg.dnodeId = data->dnodeId;
  tstrncpy(tsDcfg.clusterId, data->clusterId, TSDB_CLUSTER_ID_LEN);
  dInfo("dnodeId is set to %d, clusterId is set to %s", data->dnodeId, data->clusterId);

  dnodeWriteCfg();
  pthread_mutex_unlock(&tsDcfg.mutex);
}

void dnodeSetDropped() {
  pthread_mutex_lock(&tsDcfg.mutex);
  tsDcfg.dropped = 1;
  dnodeWriteCfg();
  pthread_mutex_unlock(&tsDcfg.mutex);
}

int32_t dnodeGetDnodeId() {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&tsDcfg.mutex);
  dnodeId = tsDcfg.dnodeId;
  pthread_mutex_unlock(&tsDcfg.mutex);
  return dnodeId;
}

void dnodeGetClusterId(char *clusterId) {
  pthread_mutex_lock(&tsDcfg.mutex);
  tstrncpy(clusterId, tsDcfg.clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&tsDcfg.mutex);
}

void dnodeGetCfg(int32_t *dnodeId, char *clusterId) {
  pthread_mutex_lock(&tsDcfg.mutex);
  *dnodeId = tsDcfg.dnodeId;
  tstrncpy(clusterId, tsDcfg.clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&tsDcfg.mutex);
}
