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

#include "taos_metric_sample_t.h"
#include "taos_metric_t.h"

#ifndef TAOS_METRIC_SAMPLE_I_H
#define TAOS_METRIC_SAMPLE_I_H

/**
 * @brief API PRIVATE Return a taos_metric_sample_t*
 *
 * @param type The type of metric sample
 * @param l_value The entire left value of the metric e.g metric_name{foo="bar"}
 * @param r_value A double representing the value of the sample
 */
taos_metric_sample_t *taos_metric_sample_new(taos_metric_type_t type, const char *l_value, double r_value);

/**
 * @brief API PRIVATE Destroy the taos_metric_sample**
 */
int taos_metric_sample_destroy(taos_metric_sample_t *self);

/**
 * @brief API PRIVATE A taos_linked_list_free_item_fn to enable item destruction within a linked list's destructor
 */
int taos_metric_sample_destroy_generic(void *);

/**
 * @brief API PRIVATE A taos_linked_list_free_item_fn to enable item destruction within a linked list's destructor.
 *
 * This function ignores any errors.
 */
void taos_metric_sample_free_generic(void *gen);

#endif  // TAOS_METRIC_SAMPLE_I_H
