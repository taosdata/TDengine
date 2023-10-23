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

#ifndef TAOS_STRING_BUILDER_I_H
#define TAOS_STRING_BUILDER_I_H

#include <stddef.h>

#include "taos_string_builder_t.h"

/**
 * API PRIVATE
 * @brief Constructor for taos_string_builder
 */
taos_string_builder_t *taos_string_builder_new(void);

/**
 * API PRIVATE
 * @brief Destroys a taos_string_builder*
 */
int taos_string_builder_destroy(taos_string_builder_t *self);

/**
 * API PRIVATE
 * @brief Adds a string
 */
int taos_string_builder_add_str(taos_string_builder_t *self, const char *str);

/**
 * API PRIVATE
 * @brief Adds a char
 */
int taos_string_builder_add_char(taos_string_builder_t *self, char c);

/**
 * API PRIVATE
 * @brief Clear the string
 */
int taos_string_builder_clear(taos_string_builder_t *self);

/**
 * API PRIVATE
 * @brief Remove data from the end
 */
int taos_string_buillder_truncate(taos_string_builder_t *self, size_t len);

/**
 * API PRIVATE
 * @brief Returns the length of the string
 */
size_t taos_string_builder_len(taos_string_builder_t *self);

/**
 * API PRIVATE
 * @brief Returns a copy of the string. The returned string must be deallocated when no longer needed.
 */
char *taos_string_builder_dump(taos_string_builder_t *self);

/**
 * API PRIVATE
 * @brief Getter for str member
 */
char *taos_string_builder_str(taos_string_builder_t *self);

#endif  // TAOS_STRING_BUILDER_I_H
