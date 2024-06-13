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
