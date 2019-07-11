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

#ifndef TDENGINE_TSYSTEM_H
#define TDENGINE_TSYSTEM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

bool taosGetSysMemory(float *memoryUsedMB);

bool taosGetProcMemory(float *memoryUsedMB);

bool taosGetDisk(float *diskUsedGB);

bool taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage);

bool taosGetBandSpeed(float *bandSpeedKb);

bool taosGetProcIO(float *readKB, float *writeKB);

void taosGetSystemInfo();

void taosKillSystem();

/*
 * transfer charset from non-standard format to standard format, in line with
 * requirements
 * of library of libiconv
 *
 * NOTE: user need to free the string
 */
char *taosCharsetReplace(char *charsetstr);

#ifdef __cplusplus
}
#endif

#endif
