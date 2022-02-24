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

#ifndef _TD_OS_ENV_H_
#define _TD_OS_ENV_H_

#include "osSysinfo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SOsEnv SOsEnv;

extern char configDir[];

void osInit();
void osUpdate();

bool   osLogSpaceAvailable();
int8_t osDaylight();

const char *osLogDir();
const char *osTempDir();
const char *osDataDir();
const char *osName();
const char *osTimezone();
const char *osLocale();
const char *osCharset();

void osSetLogDir(const char *logDir);
void osSetTempDir(const char *tempDir);
void osSetDataDir(const char *dataDir);
void osSetLogReservedSpace(float sizeInGB);
void osSetTempReservedSpace(float sizeInGB);
void osSetDataReservedSpace(float sizeInGB);
void osSetTimezone(const char *timezone);
bool osSetEnableCore(bool enable);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_ENV_H_*/