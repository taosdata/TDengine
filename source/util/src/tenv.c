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
#include "tenv.h"
#include "tconfig.h"

static char toLowChar(char c) { return (c > 'Z' || c < 'A' ? c : (c - 'A' + 'a')); }

int32_t taosEnvNameToCfgName(const char *envNameStr, char *cfgNameStr, int32_t cfgNameMaxLen) {
  if (envNameStr == NULL || cfgNameStr == NULL) return -1;
  char *p = cfgNameStr;
  if (envNameStr[0] != 'T' || envNameStr[1] != 'A' || envNameStr[2] != 'O' || envNameStr[3] != 'S' ||
      envNameStr[4] != '_') {
    // if(p != envNameStr) strncpy(p, envNameStr, cfgNameMaxLen - 1);
    // p[cfgNameMaxLen - 1] = '\0';
    // return strlen(cfgNameStr);
    cfgNameStr[0] = '\0';
    return -1;
  }
  envNameStr += 5;
  if (*envNameStr != '\0') {
    *p = toLowChar(*envNameStr);
    p++;
    envNameStr++;
  }

  for (size_t i = 1; i < cfgNameMaxLen && *envNameStr != '\0'; i++) {
    if (*envNameStr == '_') {
      envNameStr++;
      *p = *envNameStr;
      if (*envNameStr == '\0') break;
    } else {
      *p = toLowChar(*envNameStr);
    }
    p++;
    envNameStr++;
  }

  *p = '\0';
  return strlen(cfgNameStr);
}

int32_t taosEnvToCfg(const char *envStr, char *cfgStr) {
  if (envStr == NULL || cfgStr == NULL) {
    return -1;
  }
  if (cfgStr != envStr) strcpy(cfgStr, envStr);
  char *p = strchr(cfgStr, '=');

  if (p != NULL) {
    char buf[CFG_NAME_MAX_LEN];
    if (*(p + 1) == '\'') {
      *(p + 1) = ' ';
      char *pEnd = &cfgStr[strlen(cfgStr) - 1];
      if (*pEnd == '\'') *pEnd = '\0';
    }
    *p = '\0';
    int32_t cfgNameLen = taosEnvNameToCfgName(cfgStr, buf, CFG_NAME_MAX_LEN);
    if (cfgNameLen > 0) {
      memcpy(cfgStr, buf, cfgNameLen);
      memset(&cfgStr[cfgNameLen], ' ', p - cfgStr - cfgNameLen + 1);
    } else {
      *cfgStr = '\0';
      return -1;
    }
  }
  return strlen(cfgStr);
}