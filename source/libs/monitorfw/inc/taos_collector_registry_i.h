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

#include "taos_collector_registry_t.h"

#ifndef TAOS_COLLECTOR_REGISTRY_I_INCLUDED
#define TAOS_COLLECTOR_REGISTRY_I_INCLUDED

int taos_collector_registry_enable_custom_process_metrics(taos_collector_registry_t *self,
                                                          const char *process_limits_path,
                                                          const char *process_stats_path);

#endif  // TAOS_COLLECTOR_REGISTRY_I_INCLUDED
