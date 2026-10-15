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

#ifndef _TD_TXNODE_INT_H_
#define _TD_TXNODE_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

// clang-format on
#if defined(_MSC_VER) && _MSC_VER < 1900 && !defined(bool)

#ifndef __cplusplus
#define bool char
#define true 1
#define false 0
#endif

#else

#ifndef __cplusplus
#include <stdbool.h>
#endif

#endif

#include <stddef.h>
#include <stdint.h>
#include "txnode.h"

#define XNODED_XNODE_DSN_PRIVATE_KEY_FILE "XNODED_XNODE_DSN_PRIVATE_KEY_FILE"
#define XNODED_PRIVATE_KEY_CONFIG_EXIT_CODE 78

int32_t txnodeBuildXnodedEnvironment(const char *const *inherited, int32_t inherited_count,
                                     const char *const *overrides, int32_t override_count, char ***environment);
void    txnodeFreeEnvironment(char **environment);
bool    txnodeShouldRestartXnoded(int64_t exitStatus, int32_t termSignal, bool isStopped);
int32_t txnodeParseXnodedPid(const char *buf, int64_t len, int32_t *pid);
int32_t txnodeReadXnodedPidFile(const char *path, int32_t *pid);
int32_t txnodeWriteXnodedPidFile(const char *path, int32_t pid);
bool    txnodeIsXnodedProcess(int32_t pid);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TXNODE_INT_H_*/
