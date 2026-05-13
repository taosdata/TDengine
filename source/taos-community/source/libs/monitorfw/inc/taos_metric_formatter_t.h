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

#ifndef TAOS_METRIC_FORMATTER_T_H
#define TAOS_METRIC_FORMATTER_T_H

#include "taos_string_builder_t.h"

typedef struct taos_metric_formatter {
  taos_string_builder_t *string_builder;
  taos_string_builder_t *err_builder;
} taos_metric_formatter_t;

#endif  // TAOS_METRIC_FORMATTER_T_H
