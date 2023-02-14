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
#include "syncTest.h"

char *syncCfg2Str(SSyncCfg *pSyncCfg) {
  // cJSON *pJson = syncCfg2Json(pSyncCfg);
  // char  *serialized = cJSON_Print(pJson);
  // cJSON_Delete(pJson);
  // return serialized;
  return "";
}

int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  ASSERT(pRoot != NULL);

  // int32_t ret = syncCfgFromJson(pRoot, pSyncCfg);
  // ASSERT(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}
