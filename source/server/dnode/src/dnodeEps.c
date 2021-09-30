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
#include "dnodeEps.h"
#include "dnodeCfg.h"

static void dnodePrintEps(DnEps *eps) {
  dDebug("print dnodeEp, dnodeNum:%d", eps->dnodeNum);
  for (int32_t i = 0; i < eps->dnodeNum; i++) {
    SDnodeEp *ep = &eps->dnodeList[i];
    dDebug("dnode:%d, dnodeFqdn:%s dnodePort:%u", ep->dnodeId, ep->dnodeFqdn, ep->dnodePort);
  }
}

static void dnodeResetEps(DnEps *eps, SDnodeEps *data) {
  assert(data != NULL);

  if (data->dnodeNum > eps->dnodeNum) {
    SDnodeEp *tmp = calloc(data->dnodeNum, sizeof(SDnodeEp));
    if (tmp == NULL) return;

    tfree(eps->dnodeList);
    eps->dnodeList = tmp;
    eps->dnodeNum = data->dnodeNum;
    memcpy(eps->dnodeList, data->dnodeEps, eps->dnodeNum * sizeof(SDnodeEp));
    dnodePrintEps(eps);

    for (int32_t i = 0; i < eps->dnodeNum; ++i) {
      SDnodeEp *ep = &eps->dnodeList[i];
      taosHashPut(eps->dnodeHash, &ep->dnodeId, sizeof(int32_t), ep, sizeof(SDnodeEp));
    }
  }
}

static int32_t dnodeReadEps(DnEps *eps) {
  int32_t len = 0;
  int32_t maxLen = 30000;
  char *  content = calloc(1, maxLen + 1);
  cJSON * root = NULL;
  FILE *  fp = NULL;

  fp = fopen(eps->file, "r");
  if (!fp) {
    dDebug("file %s not exist", eps->file);
    goto PRASE_EPS_OVER;
  }

  len = (int32_t)fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s since content is null", eps->file);
    goto PRASE_EPS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s since invalid json format", eps->file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeNum = cJSON_GetObjectItem(root, "dnodeNum");
  if (!dnodeNum || dnodeNum->type != cJSON_Number) {
    dError("failed to read %s since dnodeNum not found", eps->file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s since dnodeInfos not found", eps->file);
    goto PRASE_EPS_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize != dnodeNum->valueint) {
    dError("failed to read %s since dnodeInfos size:%d not matched dnodeNum:%d", eps->file, dnodeInfosSize,
           (int32_t)dnodeNum->valueint);
    goto PRASE_EPS_OVER;
  }

  eps->dnodeNum = dnodeInfosSize;
  eps->dnodeList = calloc(dnodeInfosSize, sizeof(SDnodeEp));
  if (eps->dnodeList == NULL) {
    dError("failed to calloc dnodeEpList since %s", strerror(errno));
    goto PRASE_EPS_OVER;
  }

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *ep = &eps->dnodeList[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_Number) {
      dError("failed to read %s, dnodeId not found", eps->file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodeId = (int32_t)dnodeId->valueint;

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", eps->file);
      goto PRASE_EPS_OVER;
    }
    tstrncpy(ep->dnodeFqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_Number) {
      dError("failed to read %s, dnodePort not found", eps->file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodePort = (uint16_t)dnodePort->valueint;
  }

  dInfo("succcessed to read file %s", eps->file);
  dnodePrintEps(eps);

PRASE_EPS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);

  if (dnodeIsDnodeEpChanged(eps, eps->dnodeId, tsLocalEp)) {
    dError("dnode:%d, localEp different from %s dnodeEps.json and need reconfigured", eps->dnodeId, tsLocalEp);
    return -1;
  }

  terrno = 0;
  return 0;
}

static int32_t dnodeWriteEps(DnEps *eps) {
  FILE *fp = fopen(eps->file, "w");
  if (!fp) {
    dError("failed to write %s since %s", eps->file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeNum\": %d,\n", eps->dnodeNum);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < eps->dnodeNum; ++i) {
    SDnodeEp *ep = &eps->dnodeList[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": %d,\n", ep->dnodeId);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", ep->dnodeFqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": %u\n", ep->dnodePort);
    if (i < eps->dnodeNum - 1) {
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

  dInfo("successed to write %s", eps->file);
  return 0;
}

int32_t dnodeInitEps(Dnode *dnode, DnEps **out) {
  DnEps *eps = calloc(1, sizeof(DnEps));
  if (eps == NULL) return -1;

  eps->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (eps->dnodeHash == NULL) return -1;

  eps->dnode = dnode;
  eps->dnodeId = dnode->cfg->dnodeId;
  eps->dnodeNum = 0;
  snprintf(eps->file, sizeof(eps->file), "%s/dnodeEps.json", tsDnodeDir);
  pthread_mutex_init(&eps->mutex, NULL);
  *out = eps;

  int32_t ret = dnodeReadEps(eps);
  if (ret == 0) {
    dInfo("dnode eps is initialized");
  }

  return ret;
}

void dnodeCleanupEps(DnEps **out) {
  DnEps *eps = *out;
  *out = NULL;

  pthread_mutex_lock(&eps->mutex);

  if (eps->dnodeList != NULL) {
    free(eps->dnodeList);
    eps->dnodeList = NULL;
  }

  if (eps->dnodeHash) {
    taosHashCleanup(eps->dnodeHash);
    eps->dnodeHash = NULL;
  }

  eps->dnodeNum = 0;
  pthread_mutex_unlock(&eps->mutex);
  pthread_mutex_destroy(&eps->mutex);
  free(eps);
}

void dnodeUpdateEps(DnEps *eps, SDnodeEps *data) {
  if (data == NULL || data->dnodeNum <= 0) return;

  data->dnodeNum = htonl(data->dnodeNum);
  for (int32_t i = 0; i < data->dnodeNum; ++i) {
    data->dnodeEps[i].dnodeId = htonl(data->dnodeEps[i].dnodeId);
    data->dnodeEps[i].dnodePort = htons(data->dnodeEps[i].dnodePort);
  }

  pthread_mutex_lock(&eps->mutex);

  if (data->dnodeNum != eps->dnodeNum) {
    dnodeResetEps(eps, data);
    dnodeWriteEps(eps);
  } else {
    int32_t size = data->dnodeNum * sizeof(SDnodeEp);
    if (memcmp(eps->dnodeList, data->dnodeEps, size) != 0) {
      dnodeResetEps(eps, data);
      dnodeWriteEps(eps);
    }
  }

  pthread_mutex_unlock(&eps->mutex);
}

bool dnodeIsDnodeEpChanged(DnEps *eps, int32_t dnodeId, char *epstr) {
  bool changed = false;

  pthread_mutex_lock(&eps->mutex);

  SDnodeEp *ep = taosHashGet(eps->dnodeHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    char epSaved[TSDB_EP_LEN + 1];
    snprintf(epSaved, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
    changed = strcmp(epstr, epSaved) != 0;
    tstrncpy(epstr, epSaved, TSDB_EP_LEN);
  }

  pthread_mutex_unlock(&eps->mutex);

  return changed;
}

void dnodeGetDnodeEp(Dnode *dnode, int32_t dnodeId, char *epstr, char *fqdn, uint16_t *port) {
  assert(dnode != NULL);

  DnEps *eps = dnode->eps;
  pthread_mutex_lock(&eps->mutex);

  SDnodeEp *ep = taosHashGet(eps->dnodeHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    if (port) *port = ep->dnodePort;
    if (fqdn) tstrncpy(fqdn, ep->dnodeFqdn, TSDB_FQDN_LEN);
    if (epstr) snprintf(epstr, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
  }

  pthread_mutex_unlock(&eps->mutex);
}
