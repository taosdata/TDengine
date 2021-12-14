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

#ifndef _TD_UTIL_MACRO_H_
#define _TD_UTIL_MACRO_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// Module init/clear MACRO definitions
#define TD_MOD_UNINITIALIZED 0
#define TD_MOD_INITIALIZED 1

#define TD_MOD_UNCLEARD 0
#define TD_MOD_CLEARD 1

#define TD_DEF_MOD_INIT_FLAG(MOD) static int8_t MOD##InitFlag = TD_MOD_UNINITIALIZED
#define TD_DEF_MOD_CLEAR_FLAG(MOD) static int8_t MOD##ClearFlag = TD_MOD_UNCLEARD

#define TD_CHECK_AND_SET_MODE_INIT(MOD) \
  atomic_val_compare_exchange_8(&(MOD##InitFlag), TD_MOD_UNINITIALIZED, TD_MOD_INITIALIZED)

#define TD_CHECK_AND_SET_MOD_CLEAR(MOD) atomic_val_compare_exchange_8(&(MOD##ClearFlag), TD_MOD_UNCLEARD, TD_MOD_CLEARD)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_MACRO_H_*/