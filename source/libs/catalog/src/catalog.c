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

#include "catalogInt.h"

SCatalogMgmt ctgMgmt = {0};


int32_t catalogInit(SCatalog *cfg) {
  ctgMgmt = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
}


struct SCatalog* catalogGetHandle(const char *clusterId) {
  return (struct SCatalog*) 0x1;
}

int32_t catalogGetAllMeta(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, const SCatalogReq* pMetaReq, SCatalogRsp* pMetaData) {
  return 0;
}
