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

#include <assert.h>

#ifndef TAOS_ASSERT_H
#define TAOS_ASSERT_H

#ifdef TAOS_ASSERT_ENABLE
#define TAOS_ASSERT(i) assert(i);
#else
#define TAOS_ASSERT(i)
#endif  // TAOS_TEST

#endif  // TAOS_ASSERT_H
