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
  // CURL_GLOBAL_NOTHING
  if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
    return -1;
  } else {
    uError("failed to init curl env");
    return 0;
  }
}

void taosFuncCleanup() { curl_global_cleanup(); }

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
