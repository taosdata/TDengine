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

#ifndef TAOS_MAP_I_INCLUDED
#define TAOS_MAP_I_INCLUDED

#include "taos_map_t.h"

taos_map_t *taos_map_new(void);

int taos_map_set_free_value_fn(taos_map_t *self, taos_map_node_free_value_fn free_value_fn);

void *taos_map_get(taos_map_t *self, const char *key);

void *taos_map_get_withoutlock(taos_map_t *self, const char *key);

int taos_map_set(taos_map_t *self, const char *key, void *value);

int taos_map_delete(taos_map_t *self, const char *key);

int taos_map_destroy(taos_map_t *self);

size_t taos_map_size(taos_map_t *self);

taos_map_node_t *taos_map_node_new(const char *key, void *value, taos_map_node_free_value_fn free_value_fn);

#endif  // TAOS_MAP_I_INCLUDED
