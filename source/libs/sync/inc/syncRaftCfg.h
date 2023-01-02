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

#ifndef _TD_LIBS_SYNC_RAFT_CFG_H
#define _TD_LIBS_SYNC_RAFT_CFG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

int32_t syncWriteCfgFile(SSyncNode *pNode);
int32_t syncReadCfgFile(SSyncNode *pNode);
int32_t syncAddCfgIndex(SSyncNode *pNode, SyncIndex cfgIndex);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_CFG_H*/
