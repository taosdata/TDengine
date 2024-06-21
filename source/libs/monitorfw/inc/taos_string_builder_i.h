/**
 * Copyright 2019-2020 DigitalOcean Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
