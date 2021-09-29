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
#include "dnodeCfg.h"

static int32_t dnodeReadCfg(DnCfg *cfg) {
  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;

  fp = fopen(cfg->file, "r");
  if (!fp) {
    dDebug("file %s not exist", cfg->file);
    goto PARSE_CFG_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", cfg->file);
    goto PARSE_CFG_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", cfg->file);
    goto PARSE_CFG_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s since dnodeId not found", cfg->file);
    goto PARSE_CFG_OVER;
  }
  cfg->dnodeId = (int32_t)dnodeId->valueint;

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    dError("failed to read %s since dropped not found", cfg->file);
    goto PARSE_CFG_OVER;
  }
  cfg->dropped = (int32_t)dropped->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s since clusterId not found", cfg->file);
    goto PARSE_CFG_OVER;
  }
  tstrncpy(cfg->clusterId, clusterId->valuestring, TSDB_CLUSTER_ID_LEN);

  dInfo("successed to read %s", cfg->file);

PARSE_CFG_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  return 0;
}

static int32_t dnodeWriteCfg(DnCfg *cfg) {
  FILE *fp = fopen(cfg->file, "w");
  if (!fp) {
    dError("failed to write %s since %s", cfg->file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", cfg->dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d,\n", cfg->dropped);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%s\"\n", cfg->clusterId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsync(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", cfg->file);
  return 0;
}

int32_t dnodeInitCfg(Dnode *dnode, DnCfg **out) {
  DnCfg* cfg = calloc(1, sizeof(DnCfg));
  if (cfg == NULL) return -1;

  cfg->dnode = dnode;
  cfg->dnodeId = 0;
  cfg->dropped = 0;
  cfg->clusterId[0] = 0;
  snprintf(cfg->file, sizeof(cfg->file), "%s/dnodeCfg.json", tsDnodeDir);
  pthread_mutex_init(&cfg->mutex, NULL);
  *out = cfg;

  int32_t ret = dnodeReadCfg(cfg);
  if (ret == 0) {
    dInfo("dnode cfg is initialized");
  }

  if (cfg->dropped) {
    dInfo("dnode is dropped and start to exit");
    return -1;
  }

  return ret;
}

void dnodeCleanupCfg(DnCfg **out) {
  DnCfg* cfg = *out;
  *out = NULL;

  pthread_mutex_destroy(&cfg->mutex);
  free(cfg);
}

void dnodeUpdateCfg(DnCfg *cfg, SDnodeCfg *data) {
  if (cfg == NULL || cfg->dnodeId == 0) return;

  pthread_mutex_lock(&cfg->mutex);

  cfg->dnodeId = data->dnodeId;
  tstrncpy(cfg->clusterId, data->clusterId, TSDB_CLUSTER_ID_LEN);
  dInfo("dnodeId is set to %d, clusterId is set to %s", cfg->dnodeId, cfg->clusterId);

  dnodeWriteCfg(cfg);
  pthread_mutex_unlock(&cfg->mutex);
}

void dnodeSetDropped(DnCfg *cfg) {
  pthread_mutex_lock(&cfg->mutex);
  cfg->dropped = 1;
  dnodeWriteCfg(cfg);
  pthread_mutex_unlock(&cfg->mutex);
}

int32_t dnodeGetDnodeId(DnCfg *cfg) {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&cfg->mutex);
  dnodeId = cfg->dnodeId;
  pthread_mutex_unlock(&cfg->mutex);
  return dnodeId;
}

void dnodeGetClusterId(DnCfg *cfg, char *clusterId) {
  pthread_mutex_lock(&cfg->mutex);
  tstrncpy(clusterId, cfg->clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&cfg->mutex);
}

void dnodeGetCfg(DnCfg *cfg, int32_t *dnodeId, char *clusterId) {
  pthread_mutex_lock(&cfg->mutex);
  *dnodeId = cfg->dnodeId;
  tstrncpy(clusterId, cfg->clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&cfg->mutex);
}
