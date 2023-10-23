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

// Private
#include "taos_metric_t.h"

#ifndef TAOS_METRIC_I_INCLUDED
#define TAOS_METRIC_I_INCLUDED

/**
 * @brief API PRIVATE Returns a *taos_metric
 */
taos_metric_t *taos_metric_new(taos_metric_type_t type, const char *name, const char *help, size_t label_key_count,
                               const char **label_keys);

/**
 * @brief API PRIVATE Destroys a *taos_metric
 */
int taos_metric_destroy(taos_metric_t *self);

/**
 * @brief API PRIVATE takes a generic item, casts to a *taos_metric_t and destroys it
 */
int taos_metric_destroy_generic(void *item);

/**
 * @brief API Private takes a generic item, casts to a *taos_metric_t and destroys it. Discards any errors.
 */
void taos_metric_free_generic(void *item);

#endif  // TAOS_METRIC_I_INCLUDED
