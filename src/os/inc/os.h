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

#ifndef TDENGINE_OS_H
#define TDENGINE_OS_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _TD_DARWIN_64
#include "os_darwin64.h"
#endif

#ifdef _TD_LINUX_64
#include "os_linux64.h"
#endif

#ifdef _TD_LINUX_32
#include "os_linux32.h"
#endif

#ifdef _TD_WINDOWS_64
#include "os_windows64.h"
#endif

#ifdef _TD_WINDOWS_32
#include "os_windows32.h"
#endif

#ifdef __cplusplus
}
#endif

#endif
