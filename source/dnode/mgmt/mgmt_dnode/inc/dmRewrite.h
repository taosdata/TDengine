/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef TDENGINE_DNODE_MGMT_DM_REWRITE_H_
#define TDENGINE_DNODE_MGMT_DM_REWRITE_H_

#include "os.h"
#include "tarray.h"
#include "tmsg.h"

enum { DM_REWRITE_ENDPOINTS_MAX_PORT = 65056 };

typedef enum {
  DM_REWRITE_ENDPOINTS_NONE = 0,
  DM_REWRITE_ENDPOINTS_PRECHECK,
  DM_REWRITE_ENDPOINTS_APPLY,
} EDmRewriteEndpointsMode;

typedef struct {
  SEp source;
  SEp target;
} SDmEndpointMapping;

typedef struct {
  EDmRewriteEndpointsMode mode;
  char precheckDir[PATH_MAX];
  SArray *pMappings;  // SDmEndpointMapping; precheck needs at least one entry
} SDmRewriteEndpointsOption;

// Both entries work on a stopped instance and take their mappings from
// `option->pMappings`. The precheck needs at least one mapping; apply ignores the
// field and rebuilds the mapping from the collected precheck files.
//
// "Stopped" is proven by locking `<dataDir>/.running`, the file the daemon
// creates and truncates on every start and never deletes, so the data directory
// must come from an instance that has been started at least once. A directory
// without that file fails the lock step and nothing is inspected or rewritten.
int32_t dmRewriteEndpointsPrecheck(const char *cfgDir,
                                  const SDmRewriteEndpointsOption *option);
int32_t dmRewriteEndpointsApply(const char *cfgDir,
                               const SDmRewriteEndpointsOption *option);
// Refuses to start while a rewrite backup is left behind.
int32_t dmRewriteEndpointsCheckLeftovers(const char *cfgDir);

#endif
