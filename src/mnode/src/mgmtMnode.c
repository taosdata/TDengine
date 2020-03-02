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
#include "mgmtMnode.h"

int32_t mgmtGetMnodeMetaImp(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t (*mgmtGetMnodeMeta)(STableMeta *pMeta, SShowObj *pShow, void *pConn) = mgmtGetMnodeMetaImp;

int32_t mgmtRetrieveMnodesImp(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  return 0;
}

int32_t (*mgmtRetrieveMnodes)(SShowObj *pShow, char *data, int32_t rows, void *pConn) = mgmtRetrieveMnodesImp;
