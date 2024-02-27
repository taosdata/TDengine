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

#include <stdio.h>

#ifndef TAOS_LOG_H
#define TAOS_LOG_H

//#define TAOS_LOG_ENABLE

#ifdef TAOS_LOG_ENABLE
#define TAOS_LOG(msg) printf("monitor_log %s %s %s %s %d %s\n", __DATE__, __TIME__, __FILE__, __FUNCTION__, __LINE__, msg);
#else
#define TAOS_LOG(msg)
#endif  // TAOS_LOG_ENABLE

#endif  // TAOS_LOG_H
