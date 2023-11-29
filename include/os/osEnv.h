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
#include "osTimezone.h"

#ifdef __cplusplus
extern "C" {
#endif

extern char            tsOsName[];
extern char            tsTimezoneStr[];
extern enum TdTimezone tsTimezone;
extern char            tsCharset[];
extern char            tsLocale[];
extern int8_t          tsDaylight;
extern bool            tsEnableCoreFile;
extern int64_t         tsPageSizeKB;
extern int64_t         tsOpenMax;
extern int64_t         tsStreamMax;
extern float           tsNumOfCores;
extern int64_t         tsTotalMemoryKB;
extern char           *tsProcPath;
extern char            tsSIMDEnable;
extern char            tsSSE42Enable;
extern char            tsAVXEnable;
extern char            tsAVX2Enable;
extern char            tsFMAEnable;
extern char 	       tsAVX512Enable;
extern char            tsTagFilterCache;

extern char configDir[];
extern char tsDataDir[];
extern char tsLogDir[];
extern char tsTempDir[];

extern SDiskSpace tsDataSpace;
extern SDiskSpace tsLogSpace;
extern SDiskSpace tsTempSpace;

void osDefaultInit();
void osUpdate();
void osCleanup();

bool osLogSpaceAvailable();
bool osDataSpaceAvailable();
bool osTempSpaceAvailable();

bool osLogSpaceSufficient();
bool osDataSpaceSufficient();
bool osTempSpaceSufficient();

void osSetTimezone(const char *timezone);
void osSetSystemLocale(const char *inLocale, const char *inCharSet);
void osSetProcPath(int32_t argc, char **argv);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_ENV_H_*/
