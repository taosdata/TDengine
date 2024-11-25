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

#ifndef TDENGINE_WRAPPER_H
#define TDENGINE_WRAPPER_H

#include "taos.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_DRIVER_NATIVE = 0,
  TAOS_DRIVER_WEBSOCKET = 1,
  TAOS_DRIVER_RESTFUL = 2,
} ETaosDriverType;

int32_t taosDriverInit(ETaosDriverType driverType);
void    taosDriverCleanup();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_WRAPPER_H
