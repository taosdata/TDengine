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

#ifndef TDENGINE_ODBC_H
#define TDENGINE_ODBC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "os.h"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include "tglobalcfg.h"

#if defined(_WIN32) || defined(_WIN64)
  #include <windowsx.h>
  #include <winuser.h>
  #define vsnprintf   _vsnprintf
  #define snprintf    _snprintf
  #define strcasecmp  _stricmp
  #define strncasecmp _strnicmp
  #define ODBC_INI "ODBC.INI"
#else
  #define INSTAPI
  #define ODBC_INI ".odbc.ini"
#endif

#include <odbcinst.h>

#undef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#undef max
#define max(a, b) ((a) < (b) ? (b) : (a))

#ifndef DRIVER_VER_INFO
#define DRIVER_VER_INFO version
#endif

#define MAXPATHLEN      (512)           /* Max path length */
#define MAXKEYLEN       (64)            /* Max keyword length */

#endif