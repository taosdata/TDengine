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

#ifndef _TD_MND_INFO_SCHEMA_H_
#define _TD_MND_INFO_SCHEMA_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitInfos(SMnode *pMnode);
void    mndCleanupInfos(SMnode *pMnode);
int32_t mndBuildInsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, bool sysinfo,
                               STableMetaRsp *pRsp);
int32_t mndBuildInsTableCfg(SMnode *pMnode, const char *dbFName, const char *tbName, STableCfgRsp *pRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_INFO_SCHEMA_H_*/
