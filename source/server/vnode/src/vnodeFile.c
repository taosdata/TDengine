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
#include "cJSON.h"
#include "vnodeFile.h"

int32_t vnodeReadCfg(int32_t vgId, SVnodeCfg *pCfg) {
  int32_t ret = TSDB_CODE_VND_APP_ERROR;
  int32_t len = 0;
  int     maxLen = 1000;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  char file[PATH_MAX + 30] = {0};
  sprintf(file, "%s/vnode%d/config.json", tsVnodeDir, vgId);

  fp = fopen(file, "r");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file:%s to read since %s", vgId, file, strerror(errno));
    ret = TAOS_SYSTEM_ERROR(errno);
    goto PARSE_VCFG_ERROR;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read %s since content is null", vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read %s since invalid json format", vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  cJSON *db = cJSON_GetObjectItem(root, "db");
  if (!db || db->type != cJSON_String || db->valuestring == NULL) {
    vError("vgId:%d, failed to read %s since db not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  tstrncpy(pCfg->db, db->valuestring, sizeof(pCfg->db));

  cJSON *dropped = cJSON_GetObjectItem(root, "dropped");
  if (!dropped || dropped->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since dropped not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->dropped = (int32_t)dropped->valueint;

  cJSON *quorum = cJSON_GetObjectItem(root, "quorum");
  if (!quorum || quorum->type != cJSON_Number) {
    vError("vgId: %d, failed to read %s, quorum not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->quorum = (int8_t)quorum->valueint;

  cJSON *cacheBlockSize = cJSON_GetObjectItem(root, "cacheBlockSize");
  if (!cacheBlockSize || cacheBlockSize->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since cacheBlockSize not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.cacheBlockSize = (int32_t)cacheBlockSize->valueint;

  cJSON *totalBlocks = cJSON_GetObjectItem(root, "totalBlocks");
  if (!totalBlocks || totalBlocks->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since totalBlocks not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.totalBlocks = (int32_t)totalBlocks->valueint;

  cJSON *daysPerFile = cJSON_GetObjectItem(root, "daysPerFile");
  if (!daysPerFile || daysPerFile->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since daysPerFile not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.daysPerFile = (int32_t)daysPerFile->valueint;

  cJSON *daysToKeep0 = cJSON_GetObjectItem(root, "daysToKeep0");
  if (!daysToKeep0 || daysToKeep0->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since daysToKeep0 not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.daysToKeep0 = (int32_t)daysToKeep0->valueint;

  cJSON *daysToKeep1 = cJSON_GetObjectItem(root, "daysToKeep1");
  if (!daysToKeep1 || daysToKeep1->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since daysToKeep1 not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.daysToKeep1 = (int32_t)daysToKeep1->valueint;

  cJSON *daysToKeep2 = cJSON_GetObjectItem(root, "daysToKeep2");
  if (!daysToKeep2 || daysToKeep2->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since daysToKeep2 not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.daysToKeep2 = (int32_t)daysToKeep2->valueint;

  cJSON *minRowsPerFileBlock = cJSON_GetObjectItem(root, "minRowsPerFileBlock");
  if (!minRowsPerFileBlock || minRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since minRowsPerFileBlock not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.minRowsPerFileBlock = (int32_t)minRowsPerFileBlock->valueint;

  cJSON *maxRowsPerFileBlock = cJSON_GetObjectItem(root, "maxRowsPerFileBlock");
  if (!maxRowsPerFileBlock || maxRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since maxRowsPerFileBlock not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.maxRowsPerFileBlock = (int32_t)maxRowsPerFileBlock->valueint;

  cJSON *precision = cJSON_GetObjectItem(root, "precision");
  if (!precision || precision->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since precision not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.precision = (int8_t)precision->valueint;

  cJSON *compression = cJSON_GetObjectItem(root, "compression");
  if (!compression || compression->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since compression not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.compression = (int8_t)compression->valueint;

  cJSON *update = cJSON_GetObjectItem(root, "update");
  if (!update || update->type != cJSON_Number) {
    vError("vgId: %d, failed to read %s since update not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.update = (int8_t)update->valueint;

  cJSON *cacheLastRow = cJSON_GetObjectItem(root, "cacheLastRow");
  if (!cacheLastRow || cacheLastRow->type != cJSON_Number) {
    vError("vgId: %d, failed to read %s since cacheLastRow not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->tsdb.cacheLastRow = (int8_t)cacheLastRow->valueint;

  cJSON *walLevel = cJSON_GetObjectItem(root, "walLevel");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since walLevel not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->wal.walLevel = (int8_t)walLevel->valueint;

  cJSON *fsyncPeriod = cJSON_GetObjectItem(root, "fsyncPeriod");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since fsyncPeriod not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->wal.fsyncPeriod = (int32_t)fsyncPeriod->valueint;

  cJSON *selfIndex = cJSON_GetObjectItem(root, "selfIndex");
  if (!selfIndex || selfIndex->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since selfIndex not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->sync.selfIndex = selfIndex->valueint;

  cJSON *replica = cJSON_GetObjectItem(root, "replica");
  if (!replica || replica->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s since replica not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  pCfg->sync.replica = replica->valueint;

  cJSON *nodes = cJSON_GetObjectItem(root, "nodes");
  if (!nodes || nodes->type != cJSON_Array) {
    vError("vgId:%d, failed to read %s, nodes not found", vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  int size = cJSON_GetArraySize(nodes);
  if (size != pCfg->sync.replica) {
    vError("vgId:%d, failed to read %s since nodes size not matched", vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodes, i);
    if (nodeInfo == NULL) continue;
    SNodeInfo *node = &pCfg->sync.nodeInfo[i];

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "id");
    if (!nodeId || nodeId->type != cJSON_Number) {
      vError("vgId:%d, failed to read %s since nodeId not found", vgId, file);
      goto PARSE_VCFG_ERROR;
    }
    node->nodeId = nodeId->valueint;

    cJSON *nodePort = cJSON_GetObjectItem(nodeInfo, "port");
    if (!nodePort || nodePort->type != cJSON_Number) {
      vError("vgId:%d, failed to read %s sincenodePort not found", vgId, file);
      goto PARSE_VCFG_ERROR;
    }
    node->nodePort = (uint16_t)nodePort->valueint;

    cJSON *nodeFqdn = cJSON_GetObjectItem(nodeInfo, "fqdn");
    if (!nodeFqdn || nodeFqdn->type != cJSON_String || nodeFqdn->valuestring == NULL) {
      vError("vgId:%d, failed to read %s since nodeFqdn not found", vgId, file);
      goto PARSE_VCFG_ERROR;
    }
    tstrncpy(node->nodeFqdn, nodeFqdn->valuestring, TSDB_FQDN_LEN);
  }

  ret = TSDB_CODE_SUCCESS;

PARSE_VCFG_ERROR:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  terrno = 0;
  return ret;
}

int32_t vnodeWriteCfg(int32_t vgId, SVnodeCfg *pCfg) {
  int32_t code = 0;
  char    file[PATH_MAX + 30] = {0};
  sprintf(file, "%s/vnode%d/config.json", tsVnodeDir, vgId);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    vError("vgId:%d, failed to write %s error:%s", vgId, file, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  int32_t len = 0;
  int32_t maxLen = 1000;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  // vnode
  len += snprintf(content + len, maxLen - len, "  \"vgId\": %d,\n", vgId);
  len += snprintf(content + len, maxLen - len, "  \"db\": \"%s\",\n", pCfg->db);
  len += snprintf(content + len, maxLen - len, "  \"dropped\": %d,\n", pCfg->dropped);
  len += snprintf(content + len, maxLen - len, "  \"quorum\": %d,\n", pCfg->quorum);
  // tsdb
  len += snprintf(content + len, maxLen - len, "  \"cacheBlockSize\": %d,\n", pCfg->tsdb.cacheBlockSize);
  len += snprintf(content + len, maxLen - len, "  \"totalBlocks\": %d,\n", pCfg->tsdb.totalBlocks);
  len += snprintf(content + len, maxLen - len, "  \"daysPerFile\": %d,\n", pCfg->tsdb.daysPerFile);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep0\": %d,\n", pCfg->tsdb.daysToKeep0);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep1\": %d,\n", pCfg->tsdb.daysToKeep1);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep2\": %d,\n", pCfg->tsdb.daysToKeep2);
  len += snprintf(content + len, maxLen - len, "  \"minRowsPerFileBlock\": %d,\n", pCfg->tsdb.minRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"maxRowsPerFileBlock\": %d,\n", pCfg->tsdb.maxRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"precision\": %d,\n", pCfg->tsdb.precision);
  len += snprintf(content + len, maxLen - len, "  \"compression\": %d,\n", pCfg->tsdb.compression);
  len += snprintf(content + len, maxLen - len, "  \"cacheLastRow\": %d,\n", pCfg->tsdb.cacheLastRow);
  len += snprintf(content + len, maxLen - len, "  \"update\": %d,\n", pCfg->tsdb.update);
  // wal
  len += snprintf(content + len, maxLen - len, "  \"walLevel\": %d,\n", pCfg->wal.walLevel);
  len += snprintf(content + len, maxLen - len, "  \"fsyncPeriod\": %d,\n", pCfg->wal.fsyncPeriod);
  // sync
  len += snprintf(content + len, maxLen - len, "  \"replica\": %d,\n", pCfg->sync.replica);
  len += snprintf(content + len, maxLen - len, "  \"selfIndex\": %d,\n", pCfg->sync.selfIndex);
  len += snprintf(content + len, maxLen - len, "  \"nodes\": [{\n");
  for (int32_t i = 0; i < pCfg->sync.replica; i++) {
    SNodeInfo *node = &pCfg->sync.nodeInfo[i];
    len += snprintf(content + len, maxLen - len, "    \"id\": %d,\n", node->nodeId);
    len += snprintf(content + len, maxLen - len, "    \"port\": %u,\n", node->nodePort);
    len += snprintf(content + len, maxLen - len, "    \"fqdn\": \"%s\"\n", node->nodeFqdn);
    if (i < pCfg->sync.replica - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);
  terrno = 0;

  vInfo("vgId:%d, successed to write %s", vgId, file);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeReadState(int32_t vgId, SSyncServerState *pState) {
  int32_t ret = TSDB_CODE_VND_APP_ERROR;
  int32_t len = 0;
  int32_t maxLen = 100;
  char   *content = calloc(1, maxLen + 1);
  cJSON  *root = NULL;
  FILE   *fp = NULL;

  char file[PATH_MAX + 30] = {0};
  sprintf(file, "%s/vnode%d/state.json", tsVnodeDir, vgId);

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read %s since content is null", vgId, file);
    goto PARSE_TERM_ERROR;
  }

  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read %s since invalid json format", vgId, file);
    goto PARSE_TERM_ERROR;
  }

  cJSON *term = cJSON_GetObjectItem(root, "term");
  if (!term || term->type != cJSON_String) {
    vError("vgId:%d, failed to read %s since term not found", vgId, file);
    goto PARSE_TERM_ERROR;
  }
  pState->term = atoll(term->valuestring);

  cJSON *voteFor = cJSON_GetObjectItem(root, "voteFor");
  if (!voteFor || voteFor->type != cJSON_String) {
    vError("vgId:%d, failed to read %s since voteFor not found", vgId, file);
    goto PARSE_TERM_ERROR;
  }
  pState->voteFor = atoi(voteFor->valuestring);

  vInfo("vgId:%d, read %s success, voteFor:%d, term:%" PRIu64, vgId, file, pState->voteFor, pState->term);

PARSE_TERM_ERROR:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  return ret;
}

int32_t vnodeSaveState(int32_t vgId, SSyncServerState *pState) {
  char file[PATH_MAX + 30] = {0};
  sprintf(file, "%s/vnode%d/state.json", tsVnodeDir, vgId);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    vError("vgId:%d, failed to write %s since %s", vgId, file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 100;
  char   *content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"term\": \"%" PRIu64 "\",\n", pState->term);
  len += snprintf(content + len, maxLen - len, "  \"voteFor\": \"%d\"\n", pState->voteFor);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  taosFsyncFile(fileno(fp));
  fclose(fp);
  free(content);

  vInfo("vgId:%d, write %s success, voteFor:%d, term:%" PRIu64, vgId, file, pState->voteFor, pState->term);
  return TSDB_CODE_SUCCESS;
}