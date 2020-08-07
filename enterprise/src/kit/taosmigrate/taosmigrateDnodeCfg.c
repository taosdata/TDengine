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

//#include "dnodeInt.h"
//#include "dnodeMgmt.h"
//#include "dnodeVRead.h"
//#include "dnodeVWrite.h"
//#include "dnodeModule.h"

static SDMMnodeInfos tsDnodeIpInfos = {0};

static bool dnodeReadMnodeInfos(char* dnodeEpSet) {
  FILE *fp = fopen(dnodeEpSet, "r");
  if (!fp) {
    printf("failed to read mnodeEpSet.json, file not exist\n");
    return false;
  }

  bool  ret = false;
  int   maxLen = 2000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    printf("failed to read mnodeEpSet.json, content is null\n");
    return false;
  }

  content[len] = 0;
  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    printf("failed to read mnodeEpSet.json, invalid json format\n");
    goto PARSE_OVER;
  }

  cJSON* inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    printf("failed to read mnodeEpSet.json, inUse not found\n");
    goto PARSE_OVER;
  }
  tsDnodeIpInfos.inUse = inUse->valueint;

  cJSON* nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    printf("failed to read mnodeEpSet.json, nodeNum not found\n");
    goto PARSE_OVER;
  }
  tsDnodeIpInfos.nodeNum = nodeNum->valueint;

  cJSON* nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    printf("failed to read mnodeEpSet.json, nodeInfos not found\n");
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != tsDnodeIpInfos.nodeNum) {
    printf("failed to read mnodeEpSet.json, nodeInfos size not matched\n");
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON* nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      printf("failed to read mnodeEpSet.json, nodeId not found\n");
      goto PARSE_OVER;
    }
    tsDnodeIpInfos.nodeInfos[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      printf("failed to read mnodeEpSet.json, nodeName not found\n");
      goto PARSE_OVER;
    }
    strncpy(tsDnodeIpInfos.nodeInfos[i].nodeEp, nodeEp->valuestring, TSDB_EP_LEN);

    SdnodeIfo* pDnodeInfo = getDnodeInfo(tsDnodeIpInfos.nodeInfos[i].nodeId);
    if (NULL == pDnodeInfo) {
      continue;
    }

    tstrncpy(tsDnodeIpInfos.nodeInfos[i].nodeEp, pDnodeInfo->ep, TSDB_EP_LEN);    
 }

  ret = true;

  //printf("read mnode epSet successed, numOfEps:%d inUse:%d\n", tsDnodeIpInfos.nodeNum, tsDnodeIpInfos.inUse);
  //for (int32_t i = 0; i < tsDnodeIpInfos.nodeNum; i++) {
  //  printf("mnode:%d, %s\n", tsDnodeIpInfos.nodeInfos[i].nodeId, tsDnodeIpInfos.nodeInfos[i].nodeEp);
  //}

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}


static void dnodeSaveMnodeInfos(char* dnodeEpSet) {
  FILE *fp = fopen(dnodeEpSet, "w");
  if (!fp) return;

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", tsDnodeIpInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", tsDnodeIpInfos.nodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < tsDnodeIpInfos.nodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", tsDnodeIpInfos.nodeInfos[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", tsDnodeIpInfos.nodeInfos[i].nodeEp);
    if (i < tsDnodeIpInfos.nodeNum -1) {
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
  
  printf("mod mnode epSet successed\n");
}

void modDnodeEpSet(char* dnodeEpSet)
{
  (void)dnodeReadMnodeInfos(dnodeEpSet);
  dnodeSaveMnodeInfos(dnodeEpSet);
  return;
}


