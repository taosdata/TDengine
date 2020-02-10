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
 */#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "tast.h"

#ifndef TBASE_MNODE_UTIL_H
#define TBASE_MNODE_UTIL_H

bool mgmtTableCreateFromSuperTable(STabObj *pTableObj);
bool mgmtIsSuperTable(STabObj *pTableObj);
bool mgmtIsNormalTable(STabObj *pTableObj);

typedef struct SSyntaxTreeFilterSupporter {
  SSchema* pTagSchema;
  int32_t  numOfTags;
  int32_t  optr;
} SSyntaxTreeFilterSupporter;

char*   mgmtTableGetTag(STabObj* pMeter, int32_t col, SSchema* pTagColSchema);
int32_t mgmtGetTagsLength(STabObj* pMetric, int32_t col);
bool    mgmtCheckIsMonitorDB(char *db, char *monitordb);
int32_t mgmtCheckDBParams(SCreateDbMsg *pCreate);

#endif
