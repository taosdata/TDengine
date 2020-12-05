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
#include "hash.h"
#include "dnodeEps.h"

static SDnodeEps *tsEps = NULL;
static SHashObj * tsEpsHash = NULL;
static pthread_mutex_t tsEpsMutex;

static int32_t dnodeReadEps();
static int32_t dnodeWriteEps();
static void    dnodeResetEps(SDnodeEps *eps);
static void    dnodePrintEps(SDnodeEps *eps);

int32_t dnodeInitEps() {
  pthread_mutex_init(&tsEpsMutex, NULL);
  tsEpsHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  dnodeResetEps(NULL);
  int32_t ret = dnodeReadEps();
  if (ret == 0) {
    dInfo("dnode eps is initialized");
  }
  return ret;
}

void dnodeCleanupEps() {
  pthread_mutex_lock(&tsEpsMutex);
  if (tsEps) {
    free(tsEps);
    tsEps = NULL;
  }
  if (tsEpsHash) {
    taosHashCleanup(tsEpsHash);
    tsEpsHash = NULL;
  }
  pthread_mutex_unlock(&tsEpsMutex);
  pthread_mutex_destroy(&tsEpsMutex);
}

void dnodeUpdateEps(SDnodeEps *eps) {
  if (eps == NULL) return;

  eps->dnodeNum = htonl(eps->dnodeNum);
  for (int32_t i = 0; i < eps->dnodeNum; ++i) {
    eps->dnodeEps[i].dnodeId = htonl(eps->dnodeEps[i].dnodeId);
    eps->dnodeEps[i].dnodePort = htons(eps->dnodeEps[i].dnodePort);
  }

  pthread_mutex_lock(&tsEpsMutex);
  if (eps->dnodeNum != tsEps->dnodeNum) {
    dnodeResetEps(eps);
    dnodeWriteEps();
  } else {
    int32_t size = sizeof(SDnodeEps) + eps->dnodeNum * sizeof(SDnodeEp);
    if (memcmp(eps, tsEps, size) != 0) {
      dnodeResetEps(eps);
      dnodeWriteEps();
    }
  }
  pthread_mutex_unlock(&tsEpsMutex);
}

bool dnodeCheckEpChanged(int32_t dnodeId, char *epstr) {
  bool changed = false;
  pthread_mutex_lock(&tsEpsMutex);
  SDnodeEp *ep = taosHashGet(tsEpsHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    char epSaved[TSDB_EP_LEN + 1];
    snprintf(epSaved, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
    changed = strcmp(epstr, epSaved) != 0;
    tstrncpy(epstr, epSaved, TSDB_EP_LEN);
  }
  pthread_mutex_unlock(&tsEpsMutex);
  return changed;
}

void dnodeUpdateEp(int32_t dnodeId, char *epstr, char *fqdn, uint16_t *port) {
  pthread_mutex_lock(&tsEpsMutex);
  SDnodeEp *ep = taosHashGet(tsEpsHash, &dnodeId, sizeof(int32_t));
  if (ep != NULL) {
    if (port) *port = ep->dnodePort;
    if (fqdn) tstrncpy(fqdn, ep->dnodeFqdn, TSDB_FQDN_LEN);
    if (epstr) snprintf(epstr, TSDB_EP_LEN, "%s:%u", ep->dnodeFqdn, ep->dnodePort);
  }
  pthread_mutex_unlock(&tsEpsMutex);
}

static void dnodeResetEps(SDnodeEps *eps) {
  if (eps == NULL) {
    int32_t size = sizeof(SDnodeEps) + sizeof(SDnodeEp);
    if (tsEps == NULL) {
      tsEps = calloc(1, size);
    } else {
      tsEps->dnodeNum = 0;
    }
  } else {
    assert(tsEps);

    int32_t size = sizeof(SDnodeEps) + sizeof(SDnodeEp) * eps->dnodeNum;
    if (eps->dnodeNum > tsEps->dnodeNum) {
      tsEps = realloc(tsEps, size);
    }
    memcpy(tsEps, eps, size);
    dnodePrintEps(eps);
  }

  for (int32_t i = 0; i < tsEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsEps->dnodeEps[i];
    taosHashPut(tsEpsHash, &ep->dnodeId, sizeof(int32_t), ep, sizeof(SDnodeEp));
  }
}

static void dnodePrintEps(SDnodeEps *eps) {
  dDebug("print dnodeEp, dnodeNum:%d", eps->dnodeNum);
  for (int32_t i = 0; i < eps->dnodeNum; i++) {
    SDnodeEp *ep = &eps->dnodeEps[i];
    dDebug("dnodeId:%d, dnodeFqdn:%s dnodePort:%u", ep->dnodeId, ep->dnodeFqdn, ep->dnodePort);
  }
}

static int32_t dnodeReadEps() {
  int32_t    ret = -1;
  int32_t    len = 0;
  int32_t    maxLen = 30000;
  char *     content = calloc(1, maxLen + 1);
  cJSON *    root = NULL;
  FILE *     fp = NULL;
  SDnodeEps *eps = NULL;

  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeEps.json", tsDnodeDir);

  fp = fopen(file, "r");
  if (!fp) {
    dDebug("failed to read %s, file not exist", file);
    goto PRASE_EPS_OVER;
  }

  len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s, content is null", file);
    goto PRASE_EPS_OVER;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s, invalid json format", file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeNum = cJSON_GetObjectItem(root, "dnodeNum");
  if (!dnodeNum || dnodeNum->type != cJSON_Number) {
    dError("failed to read %s, dnodeNum not found", file);
    goto PRASE_EPS_OVER;
  }

  cJSON *dnodeInfos = cJSON_GetObjectItem(root, "dnodeInfos");
  if (!dnodeInfos || dnodeInfos->type != cJSON_Array) {
    dError("failed to read %s, dnodeInfos not found", file);
    goto PRASE_EPS_OVER;
  }

  int32_t dnodeInfosSize = cJSON_GetArraySize(dnodeInfos);
  if (dnodeInfosSize != dnodeNum->valueint) {
    dError("failed to read %s, dnodeInfos size:%d not matched dnodeNum:%d", file, dnodeInfosSize,
           (int32_t)dnodeNum->valueint);
    goto PRASE_EPS_OVER;
  }

  int32_t epsSize = sizeof(SDnodeEps) + dnodeInfosSize * sizeof(SDnodeEp);
  eps = calloc(1, epsSize);
  eps->dnodeNum = dnodeInfosSize;

  for (int32_t i = 0; i < dnodeInfosSize; ++i) {
    cJSON *dnodeInfo = cJSON_GetArrayItem(dnodeInfos, i);
    if (dnodeInfo == NULL) break;

    SDnodeEp *ep = &eps->dnodeEps[i];

    cJSON *dnodeId = cJSON_GetObjectItem(dnodeInfo, "dnodeId");
    if (!dnodeId || dnodeId->type != cJSON_Number) {
      dError("failed to read %s, dnodeId not found", file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodeId = dnodeId->valueint;

    cJSON *dnodeFqdn = cJSON_GetObjectItem(dnodeInfo, "dnodeFqdn");
    if (!dnodeFqdn || dnodeFqdn->type != cJSON_String || dnodeFqdn->valuestring == NULL) {
      dError("failed to read %s, dnodeFqdn not found", file);
      goto PRASE_EPS_OVER;
    }
    strncpy(ep->dnodeFqdn, dnodeFqdn->valuestring, TSDB_FQDN_LEN);

    cJSON *dnodePort = cJSON_GetObjectItem(dnodeInfo, "dnodePort");
    if (!dnodePort || dnodePort->type != cJSON_Number) {
      dError("failed to read %s, dnodePort not found", file);
      goto PRASE_EPS_OVER;
    }
    ep->dnodePort = (uint16_t)dnodePort->valueint;
  }

  ret = 0;

  dInfo("read file %s successed", file);
  dnodePrintEps(eps);

PRASE_EPS_OVER:
  if (content != NULL) free(content);
  if (root != NULL) cJSON_Delete(root);
  if (fp != NULL) fclose(fp);
  if (ret != 0) {
    if (eps) free(eps);
    eps = NULL;
  }

  dnodeResetEps(eps);
  if (eps) free(eps);

  dnodeUpdateEp(dnodeGetDnodeId(), tsLocalEp, tsLocalFqdn, &tsServerPort);

  terrno = 0;
  return 0;
}

static int32_t dnodeWriteEps() {
  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeEps.json", tsDnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s, reason:%s", file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 30000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeNum\": %d,\n", tsEps->dnodeNum);
  len += snprintf(content + len, maxLen - len, "  \"dnodeInfos\": [{\n");
  for (int32_t i = 0; i < tsEps->dnodeNum; ++i) {
    SDnodeEp *ep = &tsEps->dnodeEps[i];
    len += snprintf(content + len, maxLen - len, "    \"dnodeId\": %d,\n", ep->dnodeId);
    len += snprintf(content + len, maxLen - len, "    \"dnodeFqdn\": \"%s\",\n", ep->dnodeFqdn);
    len += snprintf(content + len, maxLen - len, "    \"dnodePort\": %u\n", ep->dnodePort);
    if (i < tsEps->dnodeNum - 1) {
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

  dInfo("successed to write %s", file);
  return 0;
}
