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

#ifndef TD_DB_REST_H
#define TD_DB_REST_H

#include <stdbool.h>
#include <stdint.h>
#include "auth.h"
#include "machine.h"

#ifdef __cplusplus
extern "C" {
#endif


int32_t parseAuthQuota(const char *authQuotaStr, SAuthQuota *pAuthQuota);

int32_t queryAuthServer(const char *clusterId, SAuthQuota *pAuthQuota, bool *pEnableIsFalse);

int32_t queryAuthServerAll();

void updateAuthServer(const char *clusterId, SAuthReqData *pAuthReqData);

#ifdef __cplusplus
}
#endif

#endif  // TD_DB_REST_H
