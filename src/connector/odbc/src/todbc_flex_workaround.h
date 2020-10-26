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

#ifndef _todbc_flex_workaround_h_
#define _todbc_flex_workaround_h_

#ifdef _MSC_VER
#include <winsock2.h>
#include <windows.h>
#endif
#ifndef INT8_MIN
#include <stdint.h>
#endif
#include <sql.h>
#include <sqlext.h>

#endif // _todbc_flex_workaround_h_

