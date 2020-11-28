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

#ifndef TDENGINE_MNODE_SHELL_H
#define TDENGINE_MNODE_SHELL_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnodeDef.h"

int32_t mnodeInitShow();
void    mnodeCleanUpShow();

typedef int32_t (*SShowMetaFp)(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
typedef void    (*SShowFreeIterFp)(void *pIter);
void mnodeAddShowMetaHandle(uint8_t showType, SShowMetaFp fp);
void mnodeAddShowRetrieveHandle(uint8_t showType, SShowRetrieveFp fp);
void mnodeAddShowFreeIterHandle(uint8_t msgType, SShowFreeIterFp fp);
void mnodeVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow);

#ifdef __cplusplus
}
#endif

#endif
