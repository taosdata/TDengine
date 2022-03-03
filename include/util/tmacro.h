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
#define TD_MOD_INITIALIZED   1

typedef int8_t td_mode_flag_t;

#define TD_CHECK_AND_SET_MODE_INIT(FLAG) atomic_val_compare_exchange_8((FLAG), TD_MOD_UNINITIALIZED, TD_MOD_INITIALIZED)
#define TD_CHECK_AND_SET_MOD_CLEAR(FLAG) atomic_val_compare_exchange_8((FLAG), TD_MOD_INITIALIZED, TD_MOD_UNINITIALIZED)

#define TD_IS_NULL(PTR) ((PTR) == NULL)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_MACRO_H_*/