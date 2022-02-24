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

typedef struct SEnvVar {
  char       dataDir[PATH_MAX];
  char       logDir[PATH_MAX];
  char       tempDir[PATH_MAX];
  SDiskSpace dataSpace;
  SDiskSpace logSpace;
  SDiskSpace tempSpace;
  char       osName[16];
  char       timezone[TD_TIMEZONE_LEN];
  char       locale[TD_LOCALE_LEN];
  char       charset[TD_CHARSET_LEN];
  int8_t     daylight;
} SEnvVar;

extern char configDir[];

void     osInit();
SEnvVar *osEnv();
void     osUpdate();
bool     osLogSpaceAvailable();
char    *osLogDir();
char    *osTempDir();
char    *osDataDir();
char    *osName();
char *osTimezone();
int8_t   osDaylight();

void osSetTimezone(const char*timezone);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_ENV_H_*/