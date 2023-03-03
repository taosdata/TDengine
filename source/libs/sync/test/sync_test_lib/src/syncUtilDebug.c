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

cJSON* syncUtilNodeInfo2Json(const SNodeInfo* p) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  cJSON_AddStringToObject(pRoot, "nodeFqdn", p->nodeFqdn);
  cJSON_AddNumberToObject(pRoot, "nodePort", p->nodePort);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SNodeInfo", pRoot);
  return pJson;
}

char* syncUtilRaftId2Str(const SRaftId* p) {
  // cJSON* pJson = syncUtilRaftId2Json(p);
  // char*  serialized = cJSON_Print(pJson);
  // cJSON_Delete(pJson);
  // return serialized;
  return "";
}
