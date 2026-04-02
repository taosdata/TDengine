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

/**
 * @file taos_alloc.h
 * @brief memory management
 */

#ifndef TAOS_ALLOC_H
#define TAOS_ALLOC_H

#include <stdlib.h>
#include <string.h>

/**
 * @brief Redefine this macro if you wish to override it. The default value is malloc.
 */
#define taos_malloc malloc

/**
 * @brief Redefine this macro if you wish to override it. The default value is realloc.
 */
#define taos_realloc realloc

/**
 * @brief Redefine this macro if you wish to override it. The default value is strdup.
 */
#define taos_strdup strdup

/**
 * @brief Redefine this macro if you wish to override it. The default value is free.
 */
#define taos_free free

#endif  // TAOS_ALLOC_H
