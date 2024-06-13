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
