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
#include "tfunc.h"
#include <curl/curl.h>

#define TSDB_AFUNC_FUNC_PREFIX  "func="
#define TSDB_AFUNC_SPLIT        ","
#define TSDB_AFUNC_ROWS_SPLIT   "rows="

typedef struct {
  int64_t       ver;
  SHashObj     *hash;  // funcname -> SAFuncUrl
  TdThreadMutex lock;
} SAFuncMgmt;

static SAFuncMgmt tsFuncs = {0};

const char *taosFuncStr(EAFuncType type) {
  switch (type) {
    case AFUNC_TYPE_ANOMALY_WINDOW:
      return "anomaly_window";
    case AFUNC_TYPE_ANOMALY_DETECT:
      return "anomaly_detect";
    case AFUNC_TYPE_FORECAST:
      return "forecast";
    case AFUNC_TYPE_HISTORIC:
      return "historic";
    default:
      return "unknown";
  }
}

EAFuncType taosFuncInt(const char *name) {
  for (EAFuncType i = AFUNC_TYPE_START; i < AFUNC_TYPE_END; ++i) {
    if (strcasecmp(name, taosFuncStr(i)) == 0) {
      return i;
    }
  }

  return AFUNC_TYPE_START;
}

int32_t taosFuncInit() {
  if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
    uError("failed to init curl env");
    return -1;
  }

  tsFuncs.ver = 0;
  taosThreadMutexInit(&tsFuncs.lock, NULL);
  tsFuncs.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (tsFuncs.hash == NULL) {
    uError("failed to init tfunc hash");
    return -1;
  }

  uInfo("afunc env is initialized");
  return 0;
}

void taosFuncFreeHash(SHashObj *hash) {
  void *pIter = taosHashIterate(hash, NULL);
  while (pIter != NULL) {
    SAFuncUrl *pUrl = (SAFuncUrl *)pIter;
    taosMemoryFree(pUrl->url);
    pIter = taosHashIterate(hash, pIter);
  }
  taosHashCleanup(hash);
}

void taosFuncCleanup() {
  curl_global_cleanup();
  taosThreadMutexDestroy(&tsFuncs.lock);
  taosFuncFreeHash(tsFuncs.hash);
  tsFuncs.hash = NULL;
  uInfo("afunc env is cleaned up");
}

void taosFuncUpdate(int64_t newVer, SHashObj *pHash) {
  if (newVer > tsFuncs.ver) {
    taosThreadMutexLock(&tsFuncs.lock);
    SHashObj *hash = tsFuncs.hash;
    tsFuncs.ver = newVer;
    tsFuncs.hash = pHash;
    taosThreadMutexUnlock(&tsFuncs.lock);
    taosFuncFreeHash(hash);
  } else {
    taosFuncFreeHash(pHash);
  }
}

int32_t taosFuncGetName(const char *option, char *name, int32_t nameLen) {
  char *pos1 = strstr(option, TSDB_AFUNC_FUNC_PREFIX);
  char *pos2 = strstr(option, TSDB_AFUNC_SPLIT);
  if (pos1 != NULL) {
    if (nameLen > 0) {
      if (pos2 == NULL) {
        tstrncpy(name, pos1 + 5, nameLen);
      } else {
        tstrncpy(name, pos1, MIN((int32_t)(pos2 - pos1), nameLen));
      }
    }
    return 0;
  } else {
    return -1;
  }
}

int32_t taosFuncGetRows(const char *option, int32_t *rows) {
  char *pos1 = strstr(option, TSDB_AFUNC_ROWS_SPLIT);
  char *pos2 = strstr(option, TSDB_AFUNC_SPLIT);
  if (pos1 != NULL) {
    *rows = taosStr2Int32(pos1 + 5, NULL, 10);
    return 0;
  } else {
    return -1;
  }
}

int32_t taosFuncGetUrl(const char *funcName, EAFuncType type, char *url, int32_t urlLen) {
  int32_t code = 0;
  char    name[TSDB_FUNC_KEY_LEN] = {0};
  int32_t nameLen = snprintf(name, sizeof(name) - 1, "%s:%d", funcName, type);

  taosThreadMutexLock(&tsFuncs.lock);
  SAFuncUrl *pUrl = taosHashAcquire(tsFuncs.hash, name, nameLen);
  if (pUrl != NULL) {
    tstrncpy(url, pUrl->url, urlLen);
    uInfo("func:%s, type:%s, url:%s", funcName, taosFuncStr(type), url);
  } else {
    url[0] = 0;
    terrno = TSDB_CODE_MND_AFUNC_NOT_FOUND;
    code = terrno;
    uInfo("func:%s,type:%s, url not found", funcName, taosFuncStr(type));
  }
  taosThreadMutexUnlock(&tsFuncs.lock);

  return code;
}

int64_t taosFuncGetVersion() { return tsFuncs.ver; }

static size_t taosCurlWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SCurlResp *pRsp = userdata;
  pRsp->dataLen = (int64_t)contLen * (int64_t)nmemb;
  pRsp->data = taosMemoryMalloc(pRsp->dataLen + 1);

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data, pCont, pRsp->dataLen);
    pRsp->data[pRsp->dataLen] = 0;
    uInfo("curl resp is received, len:%" PRId64 ", cont:%s", pRsp->dataLen, pRsp->data);
    return pRsp->dataLen;
  } else {
    pRsp->dataLen = 0;
    uInfo("failed to malloc curl resp");
    return 0;
  }
}

int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp) {
  pRsp->data = NULL;
  pRsp->dataLen = 0;

#if 0
  return taosCurlTestStr(url, pRsp);
#else
  CURL    *curl = NULL;
  CURLcode code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 100);

  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return code;
#endif
}

int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp) {
  // curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "name=daniel&project=curl");
  return 0;
}

static int32_t taosCurlTestStr(const char *url, SCurlResp *pRsp) {
  const char *listStr =
      "{"
      "\"protocol\": 1,"
      "\"version\": 1,"
      "\"functions\": [{"
      "          \"name\": \"arima\","
      "          \"types\": [\"forecast\", \"anomaly_window\"]"
      "      },"
      "      {"
      "          \"name\": \"ai\","
      "          \"types\": [\"forecast\", \"historic\"]"
      "      }"
      "  ]"
      "}";

  const char *statusStr =
      "{"
      "  \"protocol\": 1,"
      "  \"status\": \"ready\""
      "}";

  if (strstr(url, "list") != NULL) {
    pRsp->dataLen = strlen(listStr);
    pRsp->data = taosMemoryCalloc(1, pRsp->dataLen + 1);
    strcpy(pRsp->data, listStr);
  } else {
    pRsp->dataLen = strlen(statusStr);
    pRsp->data = taosMemoryCalloc(1, pRsp->dataLen + 1);
    strcpy(pRsp->data, statusStr);
  }

  return 0;
}
