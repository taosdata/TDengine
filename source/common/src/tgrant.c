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

#define _DEFAULT_SOURCE
#include "tgrant.h"

#ifndef _GRANT

int32_t grantCheck(EGrantType grant) { return TSDB_CODE_SUCCESS; }
int32_t grantCheckExpire(EGrantType grant) { return TSDB_CODE_SUCCESS; }

#ifdef TD_UNIQ_GRANT
int32_t grantCheckLE(EGrantType grant) { return TSDB_CODE_SUCCESS; }
#endif
#else
#ifdef TD_UNIQ_GRANT
int32_t grantCheckExpire(EGrantType grant) { return TSDB_CODE_SUCCESS; }
#endif
#endif