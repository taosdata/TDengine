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

#ifndef TDENGINE_MODULE_DNODE_GRANT_H
#define TDENGINE_MODULE_DNODE_GRANT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

void grantInit();

extern int32_t (*mgmtCheckUserGrantFp)();
extern int32_t (*mgmtCheckDbGrantFp)();
extern void    (*mgmtAddTimeSeriesFp)(uint32_t timeSeriesNum);
extern void    (*mgmtRestoreTimeSeriesFp)(uint32_t timeSeriesNum);
extern int32_t (*mgmtCheckTimeSeriesFp)(uint32_t timeseries);
extern bool    (*mgmtCheckExpiredFp)();
extern int32_t (*mgmtGetGrantsMetaFp)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
extern int32_t (*mgmtRetrieveGrantsFp)(SShowObj *pShow, char *data, int rows, void *pConn);
extern void    (*dnodeParseParameterKFp)();
extern void    (*mgmtUpdateGrantInfoFp)(void *pCont);

#ifdef __cplusplus
}
#endif

#endif
