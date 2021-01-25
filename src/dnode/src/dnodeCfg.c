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

static SDnodeCfg tsCfg = {0};
static pthread_mutex_t tsCfgMutex;

static int32_t dnodeReadCfg();
static int32_t dnodeWriteCfg();
static void    dnodeResetCfg(SDnodeCfg *cfg);
static void    dnodePrintCfg(SDnodeCfg *cfg);

int32_t dnodeInitCfg() {
  pthread_mutex_init(&tsCfgMutex, NULL);
  dnodeResetCfg(NULL);
  int32_t ret = dnodeReadCfg();
  if (ret == 0) {
    dInfo("dnode cfg is initialized");
  }
  return ret;
}

void dnodeCleanupCfg() { pthread_mutex_destroy(&tsCfgMutex); }

void dnodeUpdateCfg(SDnodeCfg *cfg) {
  if (tsCfg.dnodeId != 0) return;
  dnodeResetCfg(cfg);
}

int32_t dnodeGetDnodeId() {
  int32_t dnodeId = 0;
  pthread_mutex_lock(&tsCfgMutex);
  dnodeId = tsCfg.dnodeId;
  pthread_mutex_unlock(&tsCfgMutex);
  return dnodeId;
}

void dnodeGetClusterId(char *clusterId) {
  pthread_mutex_lock(&tsCfgMutex);
  tstrncpy(clusterId, tsCfg.clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&tsCfgMutex);
}

void dnodeGetCfg(int32_t *dnodeId, char *clusterId) {
  pthread_mutex_lock(&tsCfgMutex);
  *dnodeId = tsCfg.dnodeId;
  tstrncpy(clusterId, tsCfg.clusterId, TSDB_CLUSTER_ID_LEN);
  pthread_mutex_unlock(&tsCfgMutex);
}

static void dnodeResetCfg(SDnodeCfg *cfg) {
  if (cfg == NULL) return;
  if (cfg->dnodeId == 0) return;

  pthread_mutex_lock(&tsCfgMutex);
  tsCfg.dnodeId = cfg->dnodeId;
  tstrncpy(tsCfg.clusterId, cfg->clusterId, TSDB_CLUSTER_ID_LEN);
  dnodePrintCfg(cfg);
  dnodeWriteCfg();
  pthread_mutex_unlock(&tsCfgMutex);
}

static void dnodePrintCfg(SDnodeCfg *cfg) {
  dInfo("dnodeId is set to %d, clusterId is set to %s", cfg->dnodeId, cfg->clusterId);
}

static int32_t dnodeReadCfg() {
  int32_t   len = 0;
  int32_t   maxLen = 200;
  char *    content = calloc(1, maxLen + 1);
  cJSON *   root = NULL;
  FILE *    fp = NULL;
  SDnodeCfg cfg = {0};

  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeCfg.json", tsDnodeDir);

  fp = fopen(file, "r");
  if (!fp) {
    dDebug("failed to read %s, file not exist", file);
    goto PARSE_CFG_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s, content is null", file);
    goto PARSE_CFG_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s, invalid json format", file);
    goto PARSE_CFG_OVER;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s, dnodeId not found", file);
    goto PARSE_CFG_OVER;
  }
  cfg.dnodeId = (int32_t)dnodeId->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s, clusterId not found", file);
    goto PARSE_CFG_OVER;
  }
  tstrncpy(cfg.clusterId, clusterId->valuestring, TSDB_CLUSTER_ID_LEN);

  dInfo("read file %s successed", file);

PARSE_CFG_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  terrno = 0;

  dnodeResetCfg(&cfg);
  return 0;
}

static int32_t dnodeWriteCfg() {
  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeCfg.json", tsDnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s, reason:%s", file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", tsCfg.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%s\"\n", tsCfg.clusterId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  terrno = 0;

  dInfo("successed to write %s", file);
  return 0;
}
