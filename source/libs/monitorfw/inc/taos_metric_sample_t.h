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

#ifndef TAOS_METRIC_SAMPLE_T_H
#define TAOS_METRIC_SAMPLE_T_H

#include "taos_metric_sample.h"
#include "taos_metric_t.h"

#if !defined(WINDOWS)
#define C11_ATOMIC
#else
#define DOUBLE_ATOMIC
#endif 

#ifdef C11_ATOMIC
#include <stdatomic.h>
#endif

struct taos_metric_sample {
  taos_metric_type_t type; /**< type is the metric type for the sample */
  char *l_value;           /**< l_value is the full metric name and label set represeted as a string */
#ifdef C11_ATOMIC
  _Atomic double r_value;  /**< r_value is the value of the metric sample */
#else
#ifdef DOUBLE_ATOMIC
  double r_value; /**< r_value is the value of the metric sample */
#else
  int64_t r_value;
#endif
#endif  
};

#endif  // TAOS_METRIC_SAMPLE_T_H
