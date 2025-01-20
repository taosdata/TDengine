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
