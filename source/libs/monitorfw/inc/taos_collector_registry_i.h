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

#include "taos_collector_registry_t.h"

#ifndef TAOS_COLLECTOR_REGISTRY_I_INCLUDED
#define TAOS_COLLECTOR_REGISTRY_I_INCLUDED

int taos_collector_registry_enable_custom_process_metrics(taos_collector_registry_t *self,
                                                          const char *process_limits_path,
                                                          const char *process_stats_path);

#endif  // TAOS_COLLECTOR_REGISTRY_I_INCLUDED
