/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef PUB_H_
#define PUB_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdbool.h>
#include <ctype.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#ifdef WINDOWS
#define strcasecmp       _stricmp
#define strncasecmp      _strnicmp
#endif

//
//  --------------  define ------------------
//

// connect mode string
#define STR_NATIVE    "Native"
#define STR_WEBSOCKET "WebSocket"

#define DRIVER_OPT     "driver"
#define DRIVER_DESC    "Connect driver , value can be \"Native\" or \"WebSocket\", default is Native."

#define DSN_DESC       "The dsn to connect the cloud service."
#define OLD_DSN_DESC   "Alias for the -X/--dsn option."

#define DSN_NATIVE_CONFLICT "DSN option not support in native connection mode.\n"

// connect mode type define
#define CONN_MODE_INVALID   -1
#define CONN_MODE_NATIVE    0
#define CONN_MODE_WEBSOCKET 1
#define CONN_MODE_DEFAULT   CONN_MODE_NATIVE  // set default mode

// define error show module
#define INIT_PHASE "init"
#define TIP_ENGINE_ERR "Call engine failed."

// default port
#define DEFAULT_PORT_WS_LOCAL 6041
#define DEFAULT_PORT_WS_CLOUD 443
#define DEFAULT_PORT_NATIVE   6030


//
//  --------------  api ------------------
//

// get comn mode, if invalid argp then exit app
int8_t getConnMode(char *arg);

char* strToLowerCopy(const char *str);
int32_t parseDsn(char* dsn, char **host, char **port, char **user, char **pwd, char* error);

int32_t setConnMode(int8_t connMode, char *dsn, bool show);

uint16_t defaultPort(int8_t connMode, char *dsn);

// working connect mode
int8_t workingMode(int8_t connMode, char *dsn);

#ifdef __cplusplus
}
#endif

#endif // PUB_H_