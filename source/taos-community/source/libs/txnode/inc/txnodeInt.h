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

#ifndef _TD_TXNODE_INT_H_
#define _TD_TXNODE_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

// clang-format on
#if defined(_MSC_VER) && _MSC_VER < 1900 && !defined(bool)

#ifndef __cplusplus
#define bool char
#define true 1
#define false 0
#endif

#else

#ifndef __cplusplus
#include <stdbool.h>
#endif

#endif

#include <stddef.h>
#include <stdint.h>
#include "txnode.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_TXNODE_INT_H_*/
