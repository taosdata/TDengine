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

void dnodeGrantInit();
void grantActiveSystem(const char* cfgFile);
void grantSendMsgToMgmt();
void grantReset();
void grantUpdate(void *pGrant);
bool grantCheckExpired();
void grantRestoreTimeSeries(uint32_t timeseries);
void grantAddTimeSeries(uint32_t timeseries);
void grantResetCurStorage(uint64_t totalStorage);
int32_t grantCheckStorage();
int32_t grantCheckDatabases();
int32_t grantCheckUsers();
int32_t grantCheckAccts();
int32_t grantCheckDnodes();
int32_t grantCheckConns();
int32_t grantCheckStreams();
int32_t grantCheckCpuCores();
int32_t grantCheckQueryTime();
int32_t  grantCheckTimeSeries(uint32_t timeseries);

#ifdef __cplusplus
}
#endif

#endif
