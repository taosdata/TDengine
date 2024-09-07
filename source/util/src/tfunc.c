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

int32_t taosFuncInit() { return curl_global_init(CURL_GLOBAL_ALL); }

void taosFuncCleanup() { curl_global_cleanup(); }

size_t write_data(void *ptr, size_t size, size_t nmemb, void *stream) {
  // int written = fwrite(ptr, size, nmemb, (FILE *)fp);
  // return written;
  return 0;
}

static int32_t taosTestStr(const char *url, char **ppCont, int32_t *pContLen) {
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
    *pContLen = strlen(listStr) + 1;
    *ppCont = taosMemoryMalloc(*pContLen);
    strcpy(*ppCont, listStr);
  } else {
    *pContLen = strlen(statusStr) + 1;
    *ppCont = taosMemoryMalloc(*pContLen);
    strcpy(*ppCont, statusStr);
  }

  return 0;
}

int32_t taosSendGetRequest(const char *url, char **ppCont, int32_t *pContLen) {
#if 1
  return taosTestStr(url, ppCont, pContLen);
#else
  CURL   *curl = NULL;
  int32_t code = 0;

  curl = curl_easy_init();  // 初始化
  if (curl == NULL) {
    return -1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  // curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "name=daniel&project=curl");
  code = curl_easy_perform(curl);

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return -code;
  return 0;
#endif
}

int32_t taosSendPostRequest(const char *url, char **ppCont, int32_t *pContLen) { return 0; }