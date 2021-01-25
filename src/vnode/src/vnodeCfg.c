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
#include "dnode.h"
#include "vnodeCfg.h"

static void vnodeLoadCfg(SVnodeObj *pVnode, SCreateVnodeMsg* vnodeMsg) {
  tstrncpy(pVnode->db, vnodeMsg->db, sizeof(pVnode->db));
  pVnode->dbCfgVersion = vnodeMsg->cfg.dbCfgVersion;
  pVnode->vgCfgVersion = vnodeMsg->cfg.vgCfgVersion;
  pVnode->tsdbCfg.cacheBlockSize = vnodeMsg->cfg.cacheBlockSize;
  pVnode->tsdbCfg.totalBlocks = vnodeMsg->cfg.totalBlocks;
  pVnode->tsdbCfg.daysPerFile = vnodeMsg->cfg.daysPerFile;
  pVnode->tsdbCfg.keep = vnodeMsg->cfg.daysToKeep;
  pVnode->tsdbCfg.keep1 = vnodeMsg->cfg.daysToKeep1;
  pVnode->tsdbCfg.keep2 = vnodeMsg->cfg.daysToKeep2;
  pVnode->tsdbCfg.minRowsPerFileBlock = vnodeMsg->cfg.minRowsPerFileBlock;
  pVnode->tsdbCfg.maxRowsPerFileBlock = vnodeMsg->cfg.maxRowsPerFileBlock;
  pVnode->tsdbCfg.precision = vnodeMsg->cfg.precision;
  pVnode->tsdbCfg.compression = vnodeMsg->cfg.compression;
  pVnode->tsdbCfg.cacheLastRow = vnodeMsg->cfg.cacheLastRow;
  pVnode->walCfg.walLevel = vnodeMsg->cfg.walLevel;
  pVnode->walCfg.fsyncPeriod = vnodeMsg->cfg.fsyncPeriod;
  pVnode->walCfg.keep = TAOS_WAL_NOT_KEEP;
  pVnode->syncCfg.replica = vnodeMsg->cfg.vgReplica;
  pVnode->syncCfg.quorum = vnodeMsg->cfg.quorum;
  pVnode->dbReplica = vnodeMsg->cfg.dbReplica;

  for (int i = 0; i < pVnode->syncCfg.replica; ++i) {
    SVnodeDesc *node = &vnodeMsg->nodes[i];
    pVnode->syncCfg.nodeInfo[i].nodeId = node->nodeId;
    taosGetFqdnPortFromEp(node->nodeEp, pVnode->syncCfg.nodeInfo[i].nodeFqdn, &pVnode->syncCfg.nodeInfo[i].nodePort);
    pVnode->syncCfg.nodeInfo[i].nodePort += TSDB_PORT_SYNC;
  }

  vInfo("vgId:%d, load vnode cfg successfully, replcia:%d", pVnode->vgId, pVnode->syncCfg.replica);
  for (int32_t i = 0; i < pVnode->syncCfg.replica; i++) {
    SNodeInfo *node = &pVnode->syncCfg.nodeInfo[i];
    vInfo("vgId:%d, dnode:%d, %s:%u", pVnode->vgId, node->nodeId, node->nodeFqdn, node->nodePort);
  }
}

int32_t vnodeReadCfg(SVnodeObj *pVnode) {
  int32_t ret = TSDB_CODE_VND_APP_ERROR;
  int32_t len = 0;
  int     maxLen = 1000;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;
  bool    nodeChanged = false;
  SCreateVnodeMsg vnodeMsg;

  char file[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(file, "%s/vnode%d/config.json", tsVnodeDir, pVnode->vgId);

  vnodeMsg.cfg.vgId = pVnode->vgId;

  fp = fopen(file, "r");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file:%s to read, error:%s", pVnode->vgId, file, strerror(errno));
    ret = TAOS_SYSTEM_ERROR(errno);
    goto PARSE_VCFG_ERROR;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read %s, content is null", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read %s, invalid json format", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  cJSON *db = cJSON_GetObjectItem(root, "db");
  if (!db || db->type != cJSON_String || db->valuestring == NULL) {
    vError("vgId:%d, failed to read %s, db not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  tstrncpy(vnodeMsg.db, db->valuestring, sizeof(vnodeMsg.db));

  cJSON *dbCfgVersion = cJSON_GetObjectItem(root, "cfgVersion");
  if (!dbCfgVersion || dbCfgVersion->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, cfgVersion not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.dbCfgVersion = (int32_t)dbCfgVersion->valueint;

  cJSON *vgCfgVersion = cJSON_GetObjectItem(root, "vgCfgVersion");
  if (!vgCfgVersion || vgCfgVersion->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, vgCfgVersion not found", pVnode->vgId, file);
    vnodeMsg.cfg.vgCfgVersion = 0;
  } else {
    vnodeMsg.cfg.vgCfgVersion = (int32_t)vgCfgVersion->valueint;
  }

  cJSON *cacheBlockSize = cJSON_GetObjectItem(root, "cacheBlockSize");
  if (!cacheBlockSize || cacheBlockSize->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, cacheBlockSize not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.cacheBlockSize = (int32_t)cacheBlockSize->valueint;

  cJSON *totalBlocks = cJSON_GetObjectItem(root, "totalBlocks");
  if (!totalBlocks || totalBlocks->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, totalBlocks not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.totalBlocks = (int32_t)totalBlocks->valueint;

  cJSON *daysPerFile = cJSON_GetObjectItem(root, "daysPerFile");
  if (!daysPerFile || daysPerFile->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, daysPerFile not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.daysPerFile = (int32_t)daysPerFile->valueint;

  cJSON *daysToKeep = cJSON_GetObjectItem(root, "daysToKeep");
  if (!daysToKeep || daysToKeep->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, daysToKeep not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.daysToKeep = (int32_t)daysToKeep->valueint;

  cJSON *daysToKeep1 = cJSON_GetObjectItem(root, "daysToKeep1");
  if (!daysToKeep1 || daysToKeep1->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, daysToKeep1 not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.daysToKeep1 = (int32_t)daysToKeep1->valueint;

  cJSON *daysToKeep2 = cJSON_GetObjectItem(root, "daysToKeep2");
  if (!daysToKeep2 || daysToKeep2->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, daysToKeep2 not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.daysToKeep2 = (int32_t)daysToKeep2->valueint;

  cJSON *minRowsPerFileBlock = cJSON_GetObjectItem(root, "minRowsPerFileBlock");
  if (!minRowsPerFileBlock || minRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, minRowsPerFileBlock not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.minRowsPerFileBlock = (int32_t)minRowsPerFileBlock->valueint;

  cJSON *maxRowsPerFileBlock = cJSON_GetObjectItem(root, "maxRowsPerFileBlock");
  if (!maxRowsPerFileBlock || maxRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, maxRowsPerFileBlock not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.maxRowsPerFileBlock = (int32_t)maxRowsPerFileBlock->valueint;

  cJSON *precision = cJSON_GetObjectItem(root, "precision");
  if (!precision || precision->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, precision not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.precision = (int8_t)precision->valueint;

  cJSON *compression = cJSON_GetObjectItem(root, "compression");
  if (!compression || compression->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, compression not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.compression = (int8_t)compression->valueint;

  cJSON *walLevel = cJSON_GetObjectItem(root, "walLevel");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, walLevel not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.walLevel = (int8_t)walLevel->valueint;

  cJSON *fsyncPeriod = cJSON_GetObjectItem(root, "fsync");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, fsyncPeriod not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.fsyncPeriod = (int32_t)fsyncPeriod->valueint;

  cJSON *wals = cJSON_GetObjectItem(root, "wals");
  if (!wals || wals->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, wals not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.wals = (int8_t)wals->valueint;

  cJSON *vgReplica = cJSON_GetObjectItem(root, "replica");
  if (!vgReplica || vgReplica->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, replica not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.vgReplica = (int8_t)vgReplica->valueint;

  cJSON *dbReplica = cJSON_GetObjectItem(root, "dbReplica");
  if (!dbReplica || dbReplica->type != cJSON_Number) {
    vError("vgId:%d, failed to read %s, dbReplica not found", pVnode->vgId, file);
    vnodeMsg.cfg.dbReplica = vnodeMsg.cfg.vgReplica;
    vnodeMsg.cfg.vgCfgVersion = 0;
  } else {
    vnodeMsg.cfg.dbReplica = (int8_t)dbReplica->valueint;
  }

  cJSON *quorum = cJSON_GetObjectItem(root, "quorum");
  if (!quorum || quorum->type != cJSON_Number) {
    vError("vgId: %d, failed to read %s, quorum not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }
  vnodeMsg.cfg.quorum = (int8_t)quorum->valueint;

  cJSON *cacheLastRow = cJSON_GetObjectItem(root, "cacheLastRow");
  if (!cacheLastRow || cacheLastRow->type != cJSON_Number) {
    vError("vgId: %d, failed to read %s, cacheLastRow not found", pVnode->vgId, file);
    vnodeMsg.cfg.cacheLastRow = 0;
    vnodeMsg.cfg.vgCfgVersion = 0;
  } else {
    vnodeMsg.cfg.cacheLastRow = (int8_t)cacheLastRow->valueint;
  }

  cJSON *nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    vError("vgId:%d, failed to read %s, nodeInfos not found", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != vnodeMsg.cfg.vgReplica) {
    vError("vgId:%d, failed to read %s, nodeInfos size not matched", pVnode->vgId, file);
    goto PARSE_VCFG_ERROR;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;
    SVnodeDesc *node = &vnodeMsg.nodes[i];

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      vError("vgId:%d, failed to read %s, nodeId not found", pVnode->vgId, file);
      goto PARSE_VCFG_ERROR;
    }
    node->nodeId = (int32_t)nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      vError("vgId:%d, failed to read %s, nodeFqdn not found", pVnode->vgId, file);
      goto PARSE_VCFG_ERROR;
    }
    tstrncpy(node->nodeEp, nodeEp->valuestring, TSDB_EP_LEN);

    bool changed = dnodeCheckEpChanged(node->nodeId, node->nodeEp);
    if (changed) nodeChanged = changed;
  }

  ret = TSDB_CODE_SUCCESS;

PARSE_VCFG_ERROR:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (nodeChanged) {
    vnodeWriteCfg(&vnodeMsg);
  }

  if (ret == TSDB_CODE_SUCCESS) {
    vnodeLoadCfg(pVnode, &vnodeMsg);
  }

  terrno = 0;
  return ret;
}

int32_t vnodeWriteCfg(SCreateVnodeMsg *pMsg) {
  char file[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(file, "%s/vnode%d/config.json", tsVnodeDir, pMsg->cfg.vgId);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    vError("vgId:%d, failed to write %s error:%s", pMsg->cfg.vgId, file, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  int32_t len = 0;
  int32_t maxLen = 1000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"db\": \"%s\",\n", pMsg->db);
  len += snprintf(content + len, maxLen - len, "  \"cfgVersion\": %d,\n", pMsg->cfg.dbCfgVersion);
  len += snprintf(content + len, maxLen - len, "  \"vgCfgVersion\": %d,\n", pMsg->cfg.vgCfgVersion);
  len += snprintf(content + len, maxLen - len, "  \"cacheBlockSize\": %d,\n", pMsg->cfg.cacheBlockSize);
  len += snprintf(content + len, maxLen - len, "  \"totalBlocks\": %d,\n", pMsg->cfg.totalBlocks);
  len += snprintf(content + len, maxLen - len, "  \"daysPerFile\": %d,\n", pMsg->cfg.daysPerFile);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep\": %d,\n", pMsg->cfg.daysToKeep);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep1\": %d,\n", pMsg->cfg.daysToKeep1);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep2\": %d,\n", pMsg->cfg.daysToKeep2);
  len += snprintf(content + len, maxLen - len, "  \"minRowsPerFileBlock\": %d,\n", pMsg->cfg.minRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"maxRowsPerFileBlock\": %d,\n", pMsg->cfg.maxRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"precision\": %d,\n", pMsg->cfg.precision);
  len += snprintf(content + len, maxLen - len, "  \"compression\": %d,\n", pMsg->cfg.compression);
  len += snprintf(content + len, maxLen - len, "  \"walLevel\": %d,\n", pMsg->cfg.walLevel);
  len += snprintf(content + len, maxLen - len, "  \"fsync\": %d,\n", pMsg->cfg.fsyncPeriod);
  len += snprintf(content + len, maxLen - len, "  \"replica\": %d,\n", pMsg->cfg.vgReplica);
  len += snprintf(content + len, maxLen - len, "  \"dbReplica\": %d,\n", pMsg->cfg.dbReplica);
  len += snprintf(content + len, maxLen - len, "  \"wals\": %d,\n", pMsg->cfg.wals);
  len += snprintf(content + len, maxLen - len, "  \"quorum\": %d,\n", pMsg->cfg.quorum);
  len += snprintf(content + len, maxLen - len, "  \"cacheLastRow\": %d,\n", pMsg->cfg.cacheLastRow);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < pMsg->cfg.vgReplica; i++) {
    SVnodeDesc *node = &pMsg->nodes[i];
    dnodeUpdateEp(node->nodeId, node->nodeEp, NULL, NULL);
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", node->nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", node->nodeEp);
    if (i < pMsg->cfg.vgReplica - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  terrno = 0;

  vInfo("vgId:%d, successed to write %s", pMsg->cfg.vgId, file);
  return TSDB_CODE_SUCCESS;
}
