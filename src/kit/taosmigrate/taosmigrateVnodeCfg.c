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

#include "taosmigrate.h"


static int32_t saveVnodeCfg(SVnodeObj *pVnode, char* cfgFile) 
{
  FILE *fp = fopen(cfgFile, "w");
  if (!fp) {
    printf("failed to open vnode cfg file for write, file:%s error:%s\n", cfgFile, strerror(errno));
    return errno;
  }

  int32_t len = 0;
  int32_t maxLen = 1000;
  char *  content = calloc(1, maxLen + 1);
  if (content == NULL) {
    fclose(fp);
    return -1;
  }

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"db\": \"%s\",\n", pVnode->db);
  len += snprintf(content + len, maxLen - len, "  \"cfgVersion\": %d,\n", pVnode->cfgVersion);
  len += snprintf(content + len, maxLen - len, "  \"cacheBlockSize\": %d,\n", pVnode->tsdbCfg.cacheBlockSize);
  len += snprintf(content + len, maxLen - len, "  \"totalBlocks\": %d,\n", pVnode->tsdbCfg.totalBlocks);
  len += snprintf(content + len, maxLen - len, "  \"daysPerFile\": %d,\n", pVnode->tsdbCfg.daysPerFile);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep\": %d,\n", pVnode->tsdbCfg.keep);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep1\": %d,\n", pVnode->tsdbCfg.keep1);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep2\": %d,\n", pVnode->tsdbCfg.keep2);
  len += snprintf(content + len, maxLen - len, "  \"minRowsPerFileBlock\": %d,\n", pVnode->tsdbCfg.minRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"maxRowsPerFileBlock\": %d,\n", pVnode->tsdbCfg.maxRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"precision\": %d,\n", pVnode->tsdbCfg.precision);
  len += snprintf(content + len, maxLen - len, "  \"compression\": %d,\n", pVnode->tsdbCfg.compression);
  len += snprintf(content + len, maxLen - len, "  \"walLevel\": %d,\n", pVnode->walCfg.walLevel);
  len += snprintf(content + len, maxLen - len, "  \"fsync\": %d,\n", pVnode->walCfg.fsyncPeriod);
  len += snprintf(content + len, maxLen - len, "  \"replica\": %d,\n", pVnode->syncCfg.replica);
  len += snprintf(content + len, maxLen - len, "  \"wals\": %d,\n", pVnode->walCfg.wals);
  len += snprintf(content + len, maxLen - len, "  \"quorum\": %d,\n", pVnode->syncCfg.quorum);

  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < pVnode->syncCfg.replica; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", pVnode->syncCfg.nodeInfo[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s:%d\"\n", pVnode->syncCfg.nodeInfo[i].nodeFqdn, pVnode->syncCfg.nodeInfo[i].nodePort);

    if (i < pVnode->syncCfg.replica - 1) {
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

  printf("mod vnode cfg %s successed\n", cfgFile);

  return 0;
}

static int32_t readVnodeCfg(SVnodeObj *pVnode, char* cfgFile) 
{
  cJSON  *root = NULL;
  char   *content = NULL;
  int     maxLen = 1000;
  int32_t ret = -1;

  FILE *fp = fopen(cfgFile, "r");
  if (!fp) {
    printf("failed to open vnode cfg file:%s to read, error:%s\n", cfgFile, strerror(errno));
    goto PARSE_OVER;
  }

  content = calloc(1, maxLen + 1);
  if (content == NULL) {
    goto PARSE_OVER;
  }
  
  int len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    printf("failed to read vnode cfg, content is null, error:%s\n", strerror(errno));
    goto PARSE_OVER;
  }

  content[maxLen] = (char)0;

  root = cJSON_Parse(content);
  if (root == NULL) {
    printf("failed to json parse %s, invalid json format\n", cfgFile);
    goto PARSE_OVER;
  }

  cJSON *db = cJSON_GetObjectItem(root, "db");
  if (!db || db->type != cJSON_String || db->valuestring == NULL) {
    printf("vgId:%d, failed to read vnode cfg, db not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  strcpy(pVnode->db, db->valuestring);
  
  cJSON *cfgVersion = cJSON_GetObjectItem(root, "cfgVersion");
  if (!cfgVersion || cfgVersion->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, cfgVersion not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->cfgVersion = cfgVersion->valueint;

  cJSON *cacheBlockSize = cJSON_GetObjectItem(root, "cacheBlockSize");
  if (!cacheBlockSize || cacheBlockSize->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, cacheBlockSize not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.cacheBlockSize = cacheBlockSize->valueint;

  cJSON *totalBlocks = cJSON_GetObjectItem(root, "totalBlocks");
  if (!totalBlocks || totalBlocks->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, totalBlocks not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.totalBlocks = totalBlocks->valueint;

  // cJSON *maxTables = cJSON_GetObjectItem(root, "maxTables");
  // if (!maxTables || maxTables->type != cJSON_Number) {
  //   printf("vgId:%d, failed to read vnode cfg, maxTables not found\n", pVnode->vgId);
  //   goto PARSE_OVER;
  // }
  // pVnode->tsdbCfg.maxTables = maxTables->valueint;

  cJSON *daysPerFile = cJSON_GetObjectItem(root, "daysPerFile");
  if (!daysPerFile || daysPerFile->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, daysPerFile not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.daysPerFile = daysPerFile->valueint;

  cJSON *daysToKeep = cJSON_GetObjectItem(root, "daysToKeep");
  if (!daysToKeep || daysToKeep->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, daysToKeep not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep = daysToKeep->valueint;

  cJSON *daysToKeep1 = cJSON_GetObjectItem(root, "daysToKeep1");
  if (!daysToKeep1 || daysToKeep1->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, daysToKeep1 not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep1 = daysToKeep1->valueint;

  cJSON *daysToKeep2 = cJSON_GetObjectItem(root, "daysToKeep2");
  if (!daysToKeep2 || daysToKeep2->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, daysToKeep2 not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep2 = daysToKeep2->valueint;

  cJSON *minRowsPerFileBlock = cJSON_GetObjectItem(root, "minRowsPerFileBlock");
  if (!minRowsPerFileBlock || minRowsPerFileBlock->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, minRowsPerFileBlock not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.minRowsPerFileBlock = minRowsPerFileBlock->valueint;

  cJSON *maxRowsPerFileBlock = cJSON_GetObjectItem(root, "maxRowsPerFileBlock");
  if (!maxRowsPerFileBlock || maxRowsPerFileBlock->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, maxRowsPerFileBlock not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.maxRowsPerFileBlock = maxRowsPerFileBlock->valueint;

  // cJSON *commitTime = cJSON_GetObjectItem(root, "commitTime");
  // if (!commitTime || commitTime->type != cJSON_Number) {
  //   printf("vgId:%d, failed to read vnode cfg, commitTime not found\n", pVnode->vgId);
  //   goto PARSE_OVER;
  // }
  // pVnode->tsdbCfg.commitTime = (int8_t)commitTime->valueint;

  cJSON *precision = cJSON_GetObjectItem(root, "precision");
  if (!precision || precision->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, precision not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.precision = (int8_t)precision->valueint;

  cJSON *compression = cJSON_GetObjectItem(root, "compression");
  if (!compression || compression->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, compression not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.compression = (int8_t)compression->valueint;

  cJSON *walLevel = cJSON_GetObjectItem(root, "walLevel");
  if (!walLevel || walLevel->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, walLevel not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.walLevel = (int8_t) walLevel->valueint;

  cJSON *fsyncPeriod = cJSON_GetObjectItem(root, "fsync");
  if (!fsyncPeriod || fsyncPeriod->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, fsyncPeriod not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.fsyncPeriod = fsyncPeriod->valueint;

  cJSON *wals = cJSON_GetObjectItem(root, "wals");
  if (!wals || wals->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, wals not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.wals = (int8_t)wals->valueint;
  pVnode->walCfg.keep = 0;

  cJSON *replica = cJSON_GetObjectItem(root, "replica");
  if (!replica || replica->type != cJSON_Number) {
    printf("vgId:%d, failed to read vnode cfg, replica not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->syncCfg.replica = (int8_t)replica->valueint;

  cJSON *quorum = cJSON_GetObjectItem(root, "quorum");
  if (!quorum || quorum->type != cJSON_Number) {
    printf("vgId: %d, failed to read vnode cfg, quorum not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->syncCfg.quorum = (int8_t)quorum->valueint;

  cJSON *nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    printf("vgId:%d, failed to read vnode cfg, nodeInfos not found\n", pVnode->vgId);
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != pVnode->syncCfg.replica) {
    printf("vgId:%d, failed to read vnode cfg, nodeInfos size not matched\n", pVnode->vgId);
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      printf("vgId:%d, failed to read vnode cfg, nodeId not found\n", pVnode->vgId);
      goto PARSE_OVER;
    }
    pVnode->syncCfg.nodeInfo[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      printf("vgId:%d, failed to read vnode cfg, nodeFqdn not found\n", pVnode->vgId);
      goto PARSE_OVER;
    }

    taosGetFqdnPortFromEp(nodeEp->valuestring, pVnode->syncCfg.nodeInfo[i].nodeFqdn, &pVnode->syncCfg.nodeInfo[i].nodePort);
    //pVnode->syncCfg.nodeInfo[i].nodePort += TSDB_PORT_SYNC;

    
    SdnodeIfo* pDnodeInfo = getDnodeInfo(pVnode->syncCfg.nodeInfo[i].nodeId);
    if (NULL == pDnodeInfo) {
      continue;
    }

    pVnode->syncCfg.nodeInfo[i].nodePort = pDnodeInfo->port;
    tstrncpy(pVnode->syncCfg.nodeInfo[i].nodeFqdn, pDnodeInfo->fqdn, TSDB_FQDN_LEN);    
  }

  ret = 0;
  //printf("read vnode cfg successfully, replcia:%d\n", pVnode->syncCfg.replica);
  //for (int32_t i = 0; i < pVnode->syncCfg.replica; i++) {
  //  printf("dnode:%d, %s:%d\n", pVnode->syncCfg.nodeInfo[i].nodeId, pVnode->syncCfg.nodeInfo[i].nodeFqdn, pVnode->syncCfg.nodeInfo[i].nodePort);
  //}

PARSE_OVER:
  taosTFree(content);
  cJSON_Delete(root);
  if (fp) fclose(fp);
  return ret;
}

static void modVnodeCfg(char* vnodeCfg)
{
  int32_t ret;
  SVnodeObj vnodeObj = {0};
  ret = readVnodeCfg(&vnodeObj, vnodeCfg);
  if (0 != ret) {
    printf("read vnode cfg %s fail!\n", vnodeCfg);
    return ;
  }
  
  (void)saveVnodeCfg(&vnodeObj, vnodeCfg);
  
  return ;
}

void modAllVnode(char *vnodeDir) 
{
  DIR *dir = opendir(vnodeDir);
  if (dir == NULL) return;

  char filename[1024];
  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
     
    if ((de->d_type & DT_DIR) && (strncmp(de->d_name, "vnode", 5) == 0)) {
      memset(filename, 0, 1024);
      snprintf(filename, 1023, "%s/%s/config.json", vnodeDir, de->d_name);
      modVnodeCfg(filename);
    }
  }

  closedir(dir);
}

