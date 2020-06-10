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

#ifndef TDENGINE_VNODEDATAFILTERFUNC_H
#define TDENGINE_VNODEDATAFILTERFUNC_H

#ifdef __cplusplus
extern "C" {
#endif

#include "vnode.h"

__filter_func_t *vnodeGetRangeFilterFuncArray(int32_t type);

__filter_func_t *vnodeGetValueFilterFuncArray(int32_t type);

bool vnodeSupportPrefilter(int32_t type);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEDATAFILTERFUNC_H
